//! moonson — command-line entry point.
//!
//! M1 (step 1) adds WebSocket support to the scripting DSL: `websocket.connect`
//! returns a connection object with async `:send`, `:recv`, and `:close` methods,
//! so a scenario can hold a long-lived socket inside its coroutine — and mix it
//! with HTTP in the same virtual user.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use mlua::{Function, Lua, LuaSerdeExt, Table, Value};
use tokio_tungstenite::tungstenite::Message;

/// gRPC types generated from `proto/echo.proto` at build time (see build.rs).
mod echo {
    tonic::include_proto!("echo");
}

/// Upper bound for the latency histogram: 60 seconds, expressed in microseconds.
const MAX_LATENCY_US: u64 = 60_000_000;

/// The concrete WebSocket stream type `connect_async` yields (TCP, optionally
/// wrapped in TLS for `wss://`).
type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A live WebSocket connection, exposed to Luau as a userdata object. The stream
/// lives behind an async mutex so the `:send`/`:recv`/`:close` methods (which
/// only get shared access to `self`) can still drive it.
struct WsConn {
    stream: tokio::sync::Mutex<Option<WsStream>>,
    stats: Arc<Mutex<VuStats>>,
}

impl mlua::UserData for WsConn {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        // ws:send(text) — send a text frame.
        methods.add_async_method("send", |_, this, text: String| async move {
            let mut guard = this.stream.lock().await;
            let result = match guard.as_mut() {
                Some(stream) => stream.send(Message::Text(text)).await,
                None => return Err(mlua::Error::RuntimeError("websocket is closed".into())),
            };
            drop(guard);
            result.map_err(mlua::Error::external)?;
            this.stats.lock().unwrap().stream_sent += 1;
            Ok(())
        });

        // ws:recv() -> string | nil — wait for the next message; nil if closed.
        methods.add_async_method("recv", |_, this, ()| async move {
            let mut guard = this.stream.lock().await;
            let next = match guard.as_mut() {
                Some(stream) => stream.next().await,
                None => None,
            };
            drop(guard);
            match next {
                Some(Ok(message)) => {
                    this.stats.lock().unwrap().stream_recv += 1;
                    let text = match message {
                        Message::Text(t) => t.as_str().to_owned(),
                        Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                        _ => String::new(), // ping/pong/close control frames
                    };
                    Ok(Some(text))
                }
                Some(Err(e)) => Err(mlua::Error::external(e)),
                None => Ok(None),
            }
        });

        // ws:close() — finish the close handshake and DROP the socket so its file
        // descriptor is freed immediately (crucial when looping many connections).
        methods.add_async_method("close", |_, this, ()| async move {
            let mut guard = this.stream.lock().await;
            if let Some(mut stream) = guard.take() {
                let _ = stream.close(None).await;
            }
            Ok(())
        });
    }
}

/// A gRPC client connection to the echo service, exposed to Luau as a userdata
/// object with a `:unary(text)` method.
struct GrpcConn {
    client: echo::echo_client::EchoClient<tonic::transport::Channel>,
    stats: Arc<Mutex<VuStats>>,
}

impl mlua::UserData for GrpcConn {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        // grpc:unary(text) -> string — a single request/response call.
        methods.add_async_method("unary", |_, this, text: String| async move {
            // Clone the client (cheap; it shares the underlying HTTP/2 channel)
            // so we can call it without needing &mut on the shared userdata.
            let mut client = this.client.clone();
            let start = Instant::now();
            let outcome = client.unary(echo::EchoMessage { text }).await;
            let elapsed = start.elapsed();
            match outcome {
                Ok(response) => {
                    this.stats.lock().unwrap().record_request(elapsed, true);
                    Ok(response.into_inner().text)
                }
                Err(status) => {
                    this.stats.lock().unwrap().record_request(elapsed, false);
                    Err(mlua::Error::external(status))
                }
            }
        });

        // grpc:stream() -> a bidi stream object with :send/:recv/:close.
        methods.add_async_method("stream", |lua, this, ()| async move {
            let mut client = this.client.clone();
            // The outbound half is fed from an mpsc channel: `tx` (kept in the
            // stream object) pushes messages that tonic sends to the server.
            let (tx, rx) = futures_channel::mpsc::channel::<echo::EchoMessage>(128);
            let response = client.stream(rx).await.map_err(mlua::Error::external)?;
            lua.create_userdata(GrpcStream {
                tx,
                inbound: tokio::sync::Mutex::new(response.into_inner()),
                stats: this.stats.clone(),
            })
        });
    }
}

/// An open gRPC bidirectional stream, exposed to Luau with :send/:recv/:close.
struct GrpcStream {
    tx: futures_channel::mpsc::Sender<echo::EchoMessage>,
    inbound: tokio::sync::Mutex<tonic::Streaming<echo::EchoMessage>>,
    stats: Arc<Mutex<VuStats>>,
}

impl mlua::UserData for GrpcStream {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("send", |_, this, text: String| async move {
            let mut tx = this.tx.clone();
            tx.send(echo::EchoMessage { text })
                .await
                .map_err(mlua::Error::external)?;
            this.stats.lock().unwrap().stream_sent += 1;
            Ok(())
        });

        methods.add_async_method("recv", |_, this, ()| async move {
            let mut inbound = this.inbound.lock().await;
            let next = inbound.next().await;
            drop(inbound);
            match next {
                Some(Ok(message)) => {
                    this.stats.lock().unwrap().stream_recv += 1;
                    Ok(Some(message.text))
                }
                Some(Err(e)) => Err(mlua::Error::external(e)),
                None => Ok(None),
            }
        });

        methods.add_async_method("close", |_, this, ()| async move {
            // Closing the sender ends the outbound half; the server then ends the
            // inbound half in response.
            let mut tx = this.tx.clone();
            tx.close_channel();
            Ok(())
        });
    }
}

/// A dynamic gRPC client: a channel plus a descriptor pool loaded from a
/// `.proto` at run time, so scenarios can call arbitrary services.
struct DynGrpcClient {
    grpc: tonic::client::Grpc<tonic::transport::Channel>,
    pool: prost_reflect::DescriptorPool,
    stats: Arc<Mutex<VuStats>>,
}

impl mlua::UserData for DynGrpcClient {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        // client:unary("package.Service/Method", { field = value }) -> table
        methods.add_async_method(
            "unary",
            |lua, this, (method, message): (String, Value)| async move {
                let (service_name, method_name) = method.split_once('/').ok_or_else(|| {
                    mlua::Error::RuntimeError(format!(
                        "gRPC method must be \"Service/Method\", got '{method}'"
                    ))
                })?;
                let service = this.pool.get_service_by_name(service_name).ok_or_else(|| {
                    mlua::Error::RuntimeError(format!("unknown gRPC service '{service_name}'"))
                })?;
                let method_desc = service
                    .methods()
                    .find(|m| m.name() == method_name)
                    .ok_or_else(|| {
                        mlua::Error::RuntimeError(format!(
                            "unknown method '{method_name}' on '{service_name}'"
                        ))
                    })?;

                // Lua table -> JSON -> protobuf DynamicMessage (via the descriptor).
                let json = serde_json::to_value(&message).map_err(mlua::Error::external)?;
                let request_msg =
                    prost_reflect::DynamicMessage::deserialize(method_desc.input(), json)
                        .map_err(mlua::Error::external)?;

                let path = http::uri::PathAndQuery::from_maybe_shared(format!(
                    "/{service_name}/{method_name}"
                ))
                .map_err(mlua::Error::external)?;
                let codec = DynamicCodec {
                    output: method_desc.output(),
                };

                let mut client = this.grpc.clone();
                client.ready().await.map_err(mlua::Error::external)?;
                let start = Instant::now();
                let outcome = client
                    .unary(tonic::Request::new(request_msg), path, codec)
                    .await;
                let elapsed = start.elapsed();
                match outcome {
                    Ok(response) => {
                        this.stats.lock().unwrap().record_request(elapsed, true);
                        lua.to_value(&response.into_inner())
                    }
                    Err(status) => {
                        this.stats.lock().unwrap().record_request(elapsed, false);
                        Err(mlua::Error::external(status))
                    }
                }
            },
        );

        // client:stream("package.Service/Method") -> bidi stream object.
        methods.add_async_method("stream", |lua, this, method: String| async move {
            let (service_name, method_name) = method.split_once('/').ok_or_else(|| {
                mlua::Error::RuntimeError(format!(
                    "gRPC method must be \"Service/Method\", got '{method}'"
                ))
            })?;
            let service = this.pool.get_service_by_name(service_name).ok_or_else(|| {
                mlua::Error::RuntimeError(format!("unknown gRPC service '{service_name}'"))
            })?;
            let method_desc = service
                .methods()
                .find(|m| m.name() == method_name)
                .ok_or_else(|| {
                    mlua::Error::RuntimeError(format!(
                        "unknown method '{method_name}' on '{service_name}'"
                    ))
                })?;
            let path = http::uri::PathAndQuery::from_maybe_shared(format!(
                "/{service_name}/{method_name}"
            ))
            .map_err(mlua::Error::external)?;
            let codec = DynamicCodec {
                output: method_desc.output(),
            };
            let (tx, rx) = futures_channel::mpsc::channel::<prost_reflect::DynamicMessage>(128);
            let mut client = this.grpc.clone();
            client.ready().await.map_err(mlua::Error::external)?;
            let response = client
                .streaming(tonic::Request::new(rx), path, codec)
                .await
                .map_err(mlua::Error::external)?;
            lua.create_userdata(DynGrpcStream {
                tx,
                inbound: tokio::sync::Mutex::new(response.into_inner()),
                input: method_desc.input(),
                stats: this.stats.clone(),
            })
        });
    }
}

/// A gRPC bidirectional stream over a dynamic `.proto`, exposed to Luau with
/// :send/:recv/:close.
struct DynGrpcStream {
    tx: futures_channel::mpsc::Sender<prost_reflect::DynamicMessage>,
    inbound: tokio::sync::Mutex<tonic::Streaming<prost_reflect::DynamicMessage>>,
    input: prost_reflect::MessageDescriptor,
    stats: Arc<Mutex<VuStats>>,
}

impl mlua::UserData for DynGrpcStream {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("send", |_, this, message: Value| async move {
            let json = serde_json::to_value(&message).map_err(mlua::Error::external)?;
            let msg = prost_reflect::DynamicMessage::deserialize(this.input.clone(), json)
                .map_err(mlua::Error::external)?;
            let mut tx = this.tx.clone();
            tx.send(msg).await.map_err(mlua::Error::external)?;
            this.stats.lock().unwrap().stream_sent += 1;
            Ok(())
        });

        methods.add_async_method("recv", |lua, this, ()| async move {
            let mut inbound = this.inbound.lock().await;
            let next = inbound.next().await;
            drop(inbound);
            match next {
                Some(Ok(message)) => {
                    this.stats.lock().unwrap().stream_recv += 1;
                    let value = lua.to_value(&message)?;
                    Ok(Some(value))
                }
                Some(Err(status)) => Err(mlua::Error::external(status)),
                None => Ok(None),
            }
        });

        methods.add_async_method("close", |_, this, ()| async move {
            let mut tx = this.tx.clone();
            tx.close_channel();
            Ok(())
        });
    }
}

/// A tonic codec that carries prost-reflect `DynamicMessage`s, so calls whose
/// types are only known at run time can be encoded and decoded.
struct DynamicCodec {
    output: prost_reflect::MessageDescriptor,
}

impl tonic::codec::Codec for DynamicCodec {
    type Encode = prost_reflect::DynamicMessage;
    type Decode = prost_reflect::DynamicMessage;
    type Encoder = DynamicEncoder;
    type Decoder = DynamicDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        DynamicEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicDecoder {
            output: self.output.clone(),
        }
    }
}

struct DynamicEncoder;

impl tonic::codec::Encoder for DynamicEncoder {
    type Item = prost_reflect::DynamicMessage;
    type Error = tonic::Status;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        use bytes::BufMut;
        use prost::Message;
        dst.put_slice(&item.encode_to_vec());
        Ok(())
    }
}

struct DynamicDecoder {
    output: prost_reflect::MessageDescriptor,
}

impl tonic::codec::Decoder for DynamicDecoder {
    type Item = prost_reflect::DynamicMessage;
    type Error = tonic::Status;

    fn decode(
        &mut self,
        src: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        let message = prost_reflect::DynamicMessage::decode(self.output.clone(), src)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(Some(message))
    }
}

/// A CSV data set loaded once and shared by a worker's VUs. `:next()` hands out
/// rows round-robin (wrapping), so iterations get different data.
struct DataSet {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    cursor: std::sync::atomic::AtomicUsize,
}

impl mlua::UserData for DataSet {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        // data_set:next() -> table keyed by column name (nil if the file is empty).
        methods.add_method("next", |lua, this, ()| {
            if this.rows.is_empty() {
                return Ok(Value::Nil);
            }
            let index = this.cursor.fetch_add(1, Ordering::Relaxed) % this.rows.len();
            let row = lua.create_table()?;
            for (name, value) in this.headers.iter().zip(&this.rows[index]) {
                row.set(name.as_str(), value.as_str())?;
            }
            Ok(Value::Table(row))
        });
    }
}

#[derive(Parser)]
#[command(
    name = "moonson",
    version,
    about = "High-density load testing (walking skeleton)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Fire raw requests at a URL with N virtual users for a fixed duration.
    Load {
        /// Target URL to hit.
        #[arg(default_value = "https://httpbin.org/get")]
        url: String,
        /// Number of virtual users (concurrent workers).
        #[arg(long, default_value_t = 1)]
        vus: u32,
        /// How long to run: e.g. 500ms, 10s, 2m.
        #[arg(long, default_value = "5s")]
        duration: String,
        /// Per-request timeout: e.g. 5s, 500ms.
        #[arg(long, default_value = "30s")]
        timeout: String,
    },
    /// Run a Luau scenario across N virtual users for a fixed duration.
    Run {
        /// Path to a .luau scenario file.
        script: PathBuf,
        /// Base URL that scenario paths (e.g. "/get") are joined onto.
        #[arg(long, default_value = "https://httpbin.org")]
        base_url: String,
        /// Number of virtual users looping the scenario.
        #[arg(long, default_value_t = 1)]
        vus: u32,
        /// How long to run: e.g. 500ms, 10s, 2m.
        #[arg(long, default_value = "10s")]
        duration: String,
        /// Per-request timeout: e.g. 5s, 500ms.
        #[arg(long, default_value = "30s")]
        timeout: String,
        /// Ramp schedule, e.g. "30s:50,1m:50,10s:0" (overrides --vus/--duration).
        #[arg(long)]
        stages: Option<String>,
        /// Report format.
        #[arg(long, value_enum, default_value = "text")]
        out: OutputFormat,
    },
    /// Run a local WebSocket echo server (a reliable target for `run` scenarios).
    ServeEcho {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:9001")]
        addr: String,
    },
    /// Run a local gRPC echo server (a reliable target for `grpc.connect`).
    ServeGrpc {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
    /// Run a minimal fast HTTP server, used as the target for benchmarks.
    ServeBench {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:8080")]
        addr: String,
    },
    /// Parse a .proto file and print its services and methods.
    ProtoInfo {
        /// Path to a .proto file.
        proto: PathBuf,
    },
}

/// How to print the end-of-run report.
#[derive(Clone, Copy, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Load {
            url,
            vus,
            duration,
            timeout,
        } => {
            let duration = parse_duration(&duration)?;
            let timeout = parse_duration(&timeout)?;
            run_load(url, vus, duration, timeout).await
        }
        Command::Run {
            script,
            base_url,
            vus,
            duration,
            timeout,
            stages,
            out,
        } => {
            let duration = parse_duration(&duration)?;
            let timeout = parse_duration(&timeout)?;
            let stages = stages.map(|s| parse_stages(&s)).transpose()?;
            run_scenario(&script, base_url, vus, duration, timeout, stages, out).await
        }
        Command::ServeEcho { addr } => serve_echo(addr).await,
        Command::ServeGrpc { addr } => serve_grpc(addr).await,
        Command::ServeBench { addr } => serve_bench(addr).await,
        Command::ProtoInfo { proto } => proto_info(&proto),
    }
}

/// Per-virtual-user statistics. Each VU records into its own instance (so there
/// is no lock contention between VUs); we merge them into one at the end.
struct VuStats {
    /// Request latencies, in microseconds.
    latency: Histogram<u64>,
    /// Responses received (any HTTP status).
    ok: u64,
    /// Transport failures (no response: DNS, connection, TLS...).
    failed: u64,
    /// `check()` assertions that evaluated true / false.
    checks_passed: u64,
    checks_failed: u64,
    /// Streaming messages (WebSocket or gRPC) sent / received.
    stream_sent: u64,
    stream_recv: u64,
}

impl VuStats {
    fn new() -> Self {
        Self {
            latency: Histogram::new_with_bounds(1, MAX_LATENCY_US, 3)
                .expect("valid histogram bounds"),
            ok: 0,
            failed: 0,
            checks_passed: 0,
            checks_failed: 0,
            stream_sent: 0,
            stream_recv: 0,
        }
    }

    fn record_request(&mut self, elapsed: Duration, ok: bool) {
        let micros = (elapsed.as_micros() as u64).clamp(1, MAX_LATENCY_US);
        let _ = self.latency.record(micros);
        if ok {
            self.ok += 1;
        } else {
            self.failed += 1;
        }
    }

    fn record_check(&mut self, passed: bool) {
        if passed {
            self.checks_passed += 1;
        } else {
            self.checks_failed += 1;
        }
    }
}

/// Raw request loop (Step 2), behind the `load` subcommand.
async fn run_load(url: String, vus: u32, duration: Duration, timeout: Duration) -> Result<()> {
    println!("Running {vus} VU(s) against {url} for {duration:?}...");

    let ok = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .context("failed to build HTTP client")?;
    let deadline = Instant::now() + duration;

    let mut handles = Vec::with_capacity(vus as usize);
    for _ in 0..vus {
        let client = client.clone();
        let url = url.clone();
        let ok = ok.clone();
        let failed = failed.clone();
        handles.push(tokio::spawn(async move {
            while Instant::now() < deadline {
                match client.get(&url).send().await {
                    Ok(_response) => ok.fetch_add(1, Ordering::Relaxed),
                    Err(_error) => failed.fetch_add(1, Ordering::Relaxed),
                };
            }
        }));
    }
    for handle in handles {
        handle.await.context("a virtual user task panicked")?;
    }

    let ok = ok.load(Ordering::Relaxed);
    let failed = failed.load(Ordering::Relaxed);
    let secs = duration.as_secs_f64();
    let rps = if secs > 0.0 { ok as f64 / secs } else { 0.0 };
    println!("---");
    println!("requests: {ok}   errors: {failed}");
    println!("throughput: {rps:.0} req/s");
    Ok(())
}

/// Drive a Luau scenario across `vus` virtual users for `duration`, then report.
async fn run_scenario(
    path: &Path,
    base_url: String,
    vus: u32,
    duration: Duration,
    timeout: Duration,
    stages: Option<Vec<Stage>>,
    out: OutputFormat,
) -> Result<()> {
    let source = fs::read_to_string(path)
        .with_context(|| format!("cannot read scenario file {}", path.display()))?;

    // A ramp schedule (stages) or a constant number of VUs for a duration.
    let (max_vus, total_duration, schedule) = match stages {
        Some(stages) => {
            let max_vus = stages.iter().map(|s| s.target).max().unwrap_or(0);
            let total: Duration = stages.iter().map(|s| s.duration).sum();
            (max_vus, total, Some(Arc::new(stages)))
        }
        None => (vus, duration, None),
    };

    if matches!(out, OutputFormat::Text) {
        if schedule.is_some() {
            println!(
                "Running {} with up to {max_vus} VU(s) over {total_duration:?} (ramp)...",
                path.display()
            );
        } else {
            println!(
                "Running {} with {max_vus} VU(s) for {total_duration:?}...",
                path.display()
            );
        }
    }

    let start = Instant::now();
    let deadline = start + total_duration;

    // One worker thread per CPU (never more than there are VUs). Each worker owns
    // ONE Lua state and runs its slice of the VUs as coroutines on it, so a VU
    // costs a coroutine (a few KB), not a whole Lua state (~0.5 MB).
    let worker_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(max_vus.max(1) as usize);

    let mut worker_stats: Vec<Arc<Mutex<VuStats>>> = Vec::with_capacity(worker_count);
    let mut handles = Vec::with_capacity(worker_count);
    for w in 0..worker_count {
        // Give this worker a contiguous slice of VU ids.
        let first = (max_vus as usize * w / worker_count) as u32;
        let last = (max_vus as usize * (w + 1) / worker_count) as u32;
        let stats = Arc::new(Mutex::new(VuStats::new()));
        worker_stats.push(stats.clone());
        let source = source.clone();
        let base_url = base_url.clone();
        let schedule = schedule.clone();
        handles.push(std::thread::spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("failed to build worker runtime")?;
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let client = reqwest::Client::builder()
                    .timeout(timeout)
                    .build()
                    .context("failed to build HTTP client")?;
                let (lua, body) = build_scenario(&source, base_url, client, stats)?;
                let mut tasks = Vec::with_capacity((last - first) as usize);
                for vu_id in first..last {
                    let body = body.clone();
                    let schedule = schedule.clone();
                    tasks.push(tokio::task::spawn_local(async move {
                        while Instant::now() < deadline {
                            // Under a ramp, a VU generates load only while its id
                            // is below the current target; otherwise it idles.
                            let active = match &schedule {
                                Some(stages) => {
                                    (f64::from(vu_id)) < target_vus(stages, start.elapsed())
                                }
                                None => true,
                            };
                            if active {
                                let _: () = body.call_async(vu_id).await?;
                            } else {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                        Ok::<(), mlua::Error>(())
                    }));
                }
                for task in tasks {
                    task.await.context("a virtual user task panicked")??;
                }
                // Drop the scenario body before the Lua state it lives in.
                drop(body);
                drop(lua);
                Ok(())
            })
        }));
    }

    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("a worker thread panicked"))??;
    }

    // Merge every VU's histogram and counters into a single view.
    let mut latency =
        Histogram::<u64>::new_with_bounds(1, MAX_LATENCY_US, 3).expect("valid histogram bounds");
    let mut ok = 0u64;
    let mut failed = 0u64;
    let mut checks_passed = 0u64;
    let mut checks_failed = 0u64;
    let mut stream_sent = 0u64;
    let mut stream_recv = 0u64;
    for stats in &worker_stats {
        let stats = stats.lock().unwrap();
        latency
            .add(&stats.latency)
            .expect("histograms share bounds");
        ok += stats.ok;
        failed += stats.failed;
        checks_passed += stats.checks_passed;
        checks_failed += stats.checks_failed;
        stream_sent += stats.stream_sent;
        stream_recv += stats.stream_recv;
    }

    let total = ok + failed;
    let secs = total_duration.as_secs_f64();
    let rps = if secs > 0.0 { total as f64 / secs } else { 0.0 };
    let checks_total = checks_passed + checks_failed;
    let p50 = latency.value_at_quantile(0.50) as f64 / 1000.0;
    let p95 = latency.value_at_quantile(0.95) as f64 / 1000.0;
    let p99 = latency.value_at_quantile(0.99) as f64 / 1000.0;
    let max_ms = latency.max() as f64 / 1000.0;

    match out {
        OutputFormat::Json => {
            let report = serde_json::json!({
                "vus": max_vus,
                "duration_secs": secs,
                "requests": total,
                "ok": ok,
                "errors": failed,
                "throughput_rps": rps,
                "latency_ms": { "p50": p50, "p95": p95, "p99": p99, "max": max_ms },
                "checks": { "passed": checks_passed, "total": checks_total },
                "stream_messages": { "sent": stream_sent, "received": stream_recv },
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serialize report")
            );
        }
        OutputFormat::Text => {
            println!("---");
            println!("requests: {total}   ok: {ok}   errors: {failed}");
            println!("throughput: {rps:.0} req/s");
            if total > 0 {
                println!("latency (ms): p50 {p50:.1}  p95 {p95:.1}  p99 {p99:.1}  max {max_ms:.1}");
            }
            if checks_total > 0 {
                println!("checks: {checks_passed}/{checks_total} passed");
            }
            if stream_sent > 0 || stream_recv > 0 {
                println!("stream messages: sent {stream_sent}  received {stream_recv}");
            }
        }
    }
    Ok(())
}

/// Build a VU execution environment: a fresh Lua state with the `http`,
/// `websocket`, `grpc`, `think`, and `check` host functions registered and the
/// scenario loaded, returning the scenario body. One of these is shared by all
/// the VUs (coroutines) on a worker thread.
fn build_scenario(
    source: &str,
    base_url: String,
    client: reqwest::Client,
    stats: Arc<Mutex<VuStats>>,
) -> Result<(Lua, Function)> {
    let lua = Lua::new();

    // http.get(path[, opts]) and http.post(path[, opts]). Both return
    // { status = <number> }; opts may carry `json` and `headers`.
    let http = lua.create_table()?;
    for (name, method) in [
        ("get", reqwest::Method::GET),
        ("post", reqwest::Method::POST),
    ] {
        let client = client.clone();
        let base_url = base_url.clone();
        let stats = stats.clone();
        let function =
            lua.create_async_function(move |lua, (path, opts): (String, Option<Table>)| {
                let client = client.clone();
                let base_url = base_url.clone();
                let stats = stats.clone();
                let method = method.clone();
                async move {
                    perform_request(lua, client, base_url, stats, method, path, opts).await
                }
            })?;
        http.set(name, function)?;
    }
    lua.globals().set("http", http)?;

    // websocket.connect(path) -> connection object with :send/:recv/:close.
    let ws_base = to_ws_base(&base_url);
    let ws_stats = stats.clone();
    let websocket = lua.create_table()?;
    websocket.set(
        "connect",
        lua.create_async_function(move |lua, path: String| {
            let ws_base = ws_base.clone();
            let ws_stats = ws_stats.clone();
            async move {
                let url = if path.starts_with("ws://") || path.starts_with("wss://") {
                    path
                } else {
                    format!("{ws_base}{path}")
                };
                let (stream, _response) = tokio_tungstenite::connect_async(url)
                    .await
                    .map_err(mlua::Error::external)?;
                lua.create_userdata(WsConn {
                    stream: tokio::sync::Mutex::new(Some(stream)),
                    stats: ws_stats,
                })
            }
        })?,
    )?;
    lua.globals().set("websocket", websocket)?;

    // grpc.connect(url) -> connection object with a :unary(text) method.
    let grpc_stats = stats.clone();
    let grpc = lua.create_table()?;
    grpc.set(
        "connect",
        lua.create_async_function(move |lua, url: String| {
            let grpc_stats = grpc_stats.clone();
            async move {
                let client = echo::echo_client::EchoClient::connect(url)
                    .await
                    .map_err(mlua::Error::external)?;
                lua.create_userdata(GrpcConn {
                    client,
                    stats: grpc_stats,
                })
            }
        })?,
    )?;

    // grpc.dial(url, proto_path) -> dynamic client with :unary(method, table).
    let dial_stats = stats.clone();
    let dial = lua.create_async_function(move |lua, (url, proto): (String, String)| {
        let dial_stats = dial_stats.clone();
        async move {
            let channel = tonic::transport::Channel::from_shared(url)
                .map_err(mlua::Error::external)?
                .connect()
                .await
                .map_err(mlua::Error::external)?;
            let proto_path = std::path::PathBuf::from(proto);
            let include = proto_path
                .parent()
                .map(std::path::Path::to_path_buf)
                .unwrap_or_else(|| std::path::PathBuf::from("."));
            let descriptor_set = protox::compile([proto_path.as_path()], [include.as_path()])
                .map_err(mlua::Error::external)?;
            let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(descriptor_set)
                .map_err(mlua::Error::external)?;
            lua.create_userdata(DynGrpcClient {
                grpc: tonic::client::Grpc::new(channel),
                pool,
                stats: dial_stats,
            })
        }
    })?;
    grpc.set("dial", dial)?;

    lua.globals().set("grpc", grpc)?;

    // think(seconds): pause this VU without blocking the thread.
    let think = lua.create_async_function(|_, seconds: f64| async move {
        tokio::time::sleep(Duration::from_secs_f64(seconds.max(0.0))).await;
        Ok(())
    })?;
    lua.globals().set("think", think)?;

    // check(response, { name = boolean, ... }): tally each named assertion and
    // return whether all passed. The booleans are evaluated in Lua before the
    // call, so we just count them.
    let check_stats = stats.clone();
    let check = lua.create_function(move |_, (_response, checks): (Value, Table)| {
        let mut all_passed = true;
        for pair in checks.pairs::<String, bool>() {
            let (_name, passed) = pair?;
            check_stats.lock().unwrap().record_check(passed);
            if !passed {
                all_passed = false;
            }
        }
        Ok(all_passed)
    })?;
    lua.globals().set("check", check)?;

    // data.open("file.csv") -> data set with :next() returning the next row.
    let data = lua.create_table()?;
    data.set(
        "open",
        lua.create_function(|lua, path: String| {
            let mut reader = csv::Reader::from_path(&path).map_err(mlua::Error::external)?;
            let headers = reader
                .headers()
                .map_err(mlua::Error::external)?
                .iter()
                .map(String::from)
                .collect::<Vec<_>>();
            let mut rows = Vec::new();
            for record in reader.records() {
                let record = record.map_err(mlua::Error::external)?;
                rows.push(record.iter().map(String::from).collect::<Vec<_>>());
            }
            lua.create_userdata(DataSet {
                headers,
                rows,
                cursor: std::sync::atomic::AtomicUsize::new(0),
            })
        })?,
    )?;
    lua.globals().set("data", data)?;

    // scenario(name, body): stash the body so we can loop it after loading.
    let slot: Arc<Mutex<Option<Function>>> = Arc::new(Mutex::new(None));
    let store = slot.clone();
    lua.globals().set(
        "scenario",
        lua.create_function(move |_, (_name, body): (String, Function)| {
            *store.lock().unwrap() = Some(body);
            Ok(())
        })?,
    )?;

    lua.load(source)
        .exec()
        .context("error while loading scenario")?;
    let body = slot
        .lock()
        .unwrap()
        .take()
        .context("script defined no scenario; call scenario(name, function() ... end)")?;

    Ok((lua, body))
}

/// Send one HTTP request described by a scenario call, record it, and return a
/// `{ status }` table to the script.
async fn perform_request(
    lua: Lua,
    client: reqwest::Client,
    base_url: String,
    stats: Arc<Mutex<VuStats>>,
    method: reqwest::Method,
    path: String,
    opts: Option<Table>,
) -> mlua::Result<Table> {
    let url = format!("{base_url}{path}");
    let mut request = client.request(method, url);

    if let Some(opts) = opts {
        let json: Option<Value> = opts.get("json")?;
        if let Some(json) = json {
            request = request.json(&json);
        }
        let headers: Option<Table> = opts.get("headers")?;
        if let Some(headers) = headers {
            for pair in headers.pairs::<String, String>() {
                let (name, value) = pair?;
                request = request.header(name.as_str(), value.as_str());
            }
        }
    }

    let start = Instant::now();
    let outcome = request.send().await;
    let elapsed = start.elapsed();

    let result = lua.create_table()?;
    match outcome {
        Ok(response) => {
            let status = response.status().as_u16();
            stats.lock().unwrap().record_request(elapsed, true);
            result.set("status", status)?;
        }
        Err(_error) => {
            stats.lock().unwrap().record_request(elapsed, false);
            result.set("status", 0)?; // 0 = transport error, no response
        }
    }
    Ok(result)
}

/// A tiny local WebSocket echo server, so `run` scenarios have a reliable target
/// that does not depend on a public host being reachable.
async fn serve_echo(addr: String) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("cannot bind {addr}"))?;
    println!("echo server listening on http+ws://{addr} (Ctrl-C to stop)");
    loop {
        let (mut stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(error) => {
                eprintln!("serve-echo: accept error: {error}");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        tokio::spawn(async move {
            // Peek at the request without consuming it, so we can tell a
            // WebSocket upgrade apart from a plain HTTP request on the same port.
            let mut head = [0u8; 1024];
            let n = match stream.peek(&mut head).await {
                Ok(n) => n,
                Err(_) => return,
            };
            let is_ws = String::from_utf8_lossy(&head[..n])
                .to_ascii_lowercase()
                .contains("sec-websocket-key");

            if is_ws {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                while let Some(Ok(message)) = ws.next().await {
                    if message.is_close() {
                        break;
                    }
                    if (message.is_text() || message.is_binary()) && ws.send(message).await.is_err()
                    {
                        break;
                    }
                }
            } else {
                // Drain the request first: closing a socket that still has
                // unread bytes makes the OS send an RST, which the client sees as
                // an error. Then reply 200 OK and close cleanly.
                let mut sink = [0u8; 8192];
                let _ = stream.read(&mut sink).await;
                let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nok";
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
    }
}

/// The echo gRPC service used by `serve-grpc`.
struct EchoService;

#[tonic::async_trait]
impl echo::echo_server::Echo for EchoService {
    async fn unary(
        &self,
        request: tonic::Request<echo::EchoMessage>,
    ) -> Result<tonic::Response<echo::EchoMessage>, tonic::Status> {
        let text = request.into_inner().text;
        Ok(tonic::Response::new(echo::EchoMessage { text }))
    }

    type StreamStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<Item = Result<echo::EchoMessage, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

    async fn stream(
        &self,
        request: tonic::Request<tonic::Streaming<echo::EchoMessage>>,
    ) -> Result<tonic::Response<Self::StreamStream>, tonic::Status> {
        // Echo: forward every message from the inbound stream straight back out.
        let inbound = request.into_inner();
        let outbound: Self::StreamStream = Box::pin(inbound);
        Ok(tonic::Response::new(outbound))
    }
}

/// Run a local gRPC echo server (a reliable target for `grpc.connect`).
async fn serve_grpc(addr: String) -> Result<()> {
    let socket: std::net::SocketAddr = addr
        .parse()
        .with_context(|| format!("invalid address {addr}"))?;
    println!("gRPC echo server listening on http://{addr} (Ctrl-C to stop)");
    tonic::transport::Server::builder()
        .add_service(echo::echo_server::EchoServer::new(EchoService))
        .serve(socket)
        .await
        .context("gRPC server error")?;
    Ok(())
}

/// A minimal, fast, keep-alive HTTP server that answers every request with
/// `200 ok`. Used as the benchmark target so we measure the load *generators*,
/// not the target.
async fn serve_bench(addr: String) -> Result<()> {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::service::service_fn;
    use hyper::Response;
    use hyper_util::rt::TokioIo;

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("cannot bind {addr}"))?;
    println!("bench target listening on http://{addr} (Ctrl-C to stop)");
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(error) => {
                eprintln!("serve-bench: accept error: {error}");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        let io = TokioIo::new(stream);
        tokio::spawn(async move {
            let service = service_fn(|_req: hyper::Request<hyper::body::Incoming>| async {
                Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from_static(
                    b"ok",
                ))))
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await;
        });
    }
}

/// Compile a `.proto` file at runtime and print the services and methods it
/// declares. This is the parsing foundation for dynamic gRPC calls.
fn proto_info(path: &Path) -> Result<()> {
    let include = path.parent().unwrap_or_else(|| Path::new("."));
    let descriptor_set = protox::compile([path], [include])
        .with_context(|| format!("failed to compile {}", path.display()))?;
    let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(descriptor_set)
        .context("failed to build descriptor pool")?;

    for service in pool.services() {
        println!("service {}", service.full_name());
        for method in service.methods() {
            let kind = match (method.is_client_streaming(), method.is_server_streaming()) {
                (false, false) => "unary",
                (true, false) => "client-streaming",
                (false, true) => "server-streaming",
                (true, true) => "bidi-streaming",
            };
            println!(
                "  {} [{kind}]  {} -> {}",
                method.name(),
                method.input().full_name(),
                method.output().full_name()
            );
        }
    }
    Ok(())
}

/// Derive a WebSocket base URL from an HTTP one by swapping the scheme
/// (https -> wss, http -> ws).
fn to_ws_base(base_url: &str) -> String {
    if let Some(rest) = base_url.strip_prefix("https") {
        format!("wss{rest}")
    } else if let Some(rest) = base_url.strip_prefix("http") {
        format!("ws{rest}")
    } else {
        base_url.to_owned()
    }
}

/// One stage of a ramp schedule: move the active VU count to `target` over
/// `duration`, linearly from the previous stage's target.
struct Stage {
    duration: Duration,
    target: u32,
}

/// Parse a ramp schedule like "30s:50,1m:50,10s:0" into stages.
fn parse_stages(s: &str) -> Result<Vec<Stage>> {
    let mut stages = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        let (dur, target) = part
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("a stage must be \"<duration>:<vus>\", got '{part}'"))?;
        stages.push(Stage {
            duration: parse_duration(dur.trim())?,
            target: target
                .trim()
                .parse()
                .with_context(|| format!("invalid VU count in stage '{part}'"))?,
        });
    }
    if stages.is_empty() {
        bail!("no stages given");
    }
    Ok(stages)
}

/// The target active VU count at `elapsed` into the run, interpolating linearly
/// between stage targets (starting from 0).
fn target_vus(stages: &[Stage], elapsed: Duration) -> f64 {
    let mut prev_target = 0.0;
    let mut stage_start = Duration::ZERO;
    for stage in stages {
        let stage_end = stage_start + stage.duration;
        if elapsed < stage_end {
            let frac = if stage.duration.is_zero() {
                1.0
            } else {
                (elapsed - stage_start).as_secs_f64() / stage.duration.as_secs_f64()
            };
            return prev_target + (f64::from(stage.target) - prev_target) * frac;
        }
        prev_target = f64::from(stage.target);
        stage_start = stage_end;
    }
    prev_target
}

/// Turn a string like "10s" into a `Duration`. Supports `ms`, `s`, and `m`.
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let split = s
        .find(|c: char| c.is_ascii_alphabetic())
        .with_context(|| format!("missing time unit in '{s}' (e.g. 10s, 500ms, 2m)"))?;
    let (number, unit) = s.split_at(split);
    let value: u64 = number
        .parse()
        .with_context(|| format!("invalid number in '{s}'"))?;
    let duration = match unit {
        "ms" => Duration::from_millis(value),
        "s" => Duration::from_secs(value),
        "m" => Duration::from_secs(value * 60),
        other => bail!("unknown time unit '{other}' (use ms, s, or m)"),
    };
    Ok(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_seconds() {
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
    }

    #[test]
    fn parses_milliseconds() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
    }

    #[test]
    fn parses_minutes() {
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
    }

    #[test]
    fn rejects_missing_unit() {
        assert!(parse_duration("10").is_err());
    }

    #[test]
    fn rejects_unknown_unit() {
        assert!(parse_duration("10h").is_err());
    }

    #[test]
    fn parses_stages() {
        let stages = parse_stages("30s:50,1m:100").unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].duration, Duration::from_secs(30));
        assert_eq!(stages[0].target, 50);
        assert_eq!(stages[1].target, 100);
    }

    #[test]
    fn ramps_linearly_between_targets() {
        let stages = vec![Stage {
            duration: Duration::from_secs(10),
            target: 100,
        }];
        assert!(target_vus(&stages, Duration::from_secs(0)) < 1.0);
        assert!((target_vus(&stages, Duration::from_secs(5)) - 50.0).abs() < 1.0);
        // At/after the end, it holds the final target.
        assert!((target_vus(&stages, Duration::from_secs(10)) - 100.0).abs() < 1.0);
    }

    #[test]
    fn derives_ws_base_from_http_base() {
        assert_eq!(to_ws_base("https://example.com"), "wss://example.com");
        assert_eq!(to_ws_base("http://localhost:8080"), "ws://localhost:8080");
    }

    #[test]
    fn vustats_records_and_merges() {
        let mut a = VuStats::new();
        a.record_request(Duration::from_millis(10), true);
        a.record_request(Duration::from_millis(20), false);
        let mut b = VuStats::new();
        b.record_request(Duration::from_millis(30), true);

        a.latency.add(&b.latency).unwrap();
        assert_eq!(a.ok, 1);
        assert_eq!(a.failed, 1);
        assert_eq!(a.latency.len(), 3);
    }

    #[test]
    fn vustats_counts_checks() {
        let mut s = VuStats::new();
        s.record_check(true);
        s.record_check(false);
        s.record_check(true);
        assert_eq!(s.checks_passed, 2);
        assert_eq!(s.checks_failed, 1);
    }

    #[test]
    fn scenario_calls_http_get_for_each_call() {
        // Sync Rust <-> Luau round-trip (no async, no network).
        let lua = Lua::new();
        let calls = Arc::new(AtomicU64::new(0));

        let http = lua.create_table().unwrap();
        let counter = calls.clone();
        http.set(
            "get",
            lua.create_function(move |_, _url: String| {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
        lua.globals().set("http", http).unwrap();
        lua.globals()
            .set(
                "scenario",
                lua.create_function(|_, (_name, body): (String, Function)| {
                    let _: () = body.call(())?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();

        lua.load(
            r#"
            scenario("t", function()
              http.get("/a")
              http.get("/b")
            end)
            "#,
        )
        .exec()
        .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn async_host_function_runs_inside_a_coroutine() {
        // The async bridge, tested without network.
        let lua = Lua::new();
        let answer = lua
            .create_async_function(|_, ()| async move {
                tokio::task::yield_now().await;
                Ok(42_i64)
            })
            .unwrap();
        lua.globals().set("answer", answer).unwrap();

        let scenario: Function = lua
            .load(
                r#"
                return function()
                  return answer()
                end
                "#,
            )
            .eval()
            .unwrap();

        let result: i64 = scenario.call_async(()).await.unwrap();
        assert_eq!(result, 42);
    }
}
