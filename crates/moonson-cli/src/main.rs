//! moonson — command-line entry point.
//!
//! Step 4 makes the scripting bridge real. `http.get` is now an asynchronous
//! host function that performs an actual HTTP request, and the scenario body is
//! run as a coroutine (`call_async`) so it can await that request without
//! blocking the thread. This is the proof that the whole DSL model works.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use mlua::{Function, Lua};

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
    },
    /// Run a Luau scenario file once, performing real HTTP requests.
    Run {
        /// Path to a .luau scenario file.
        script: PathBuf,
        /// Base URL that scenario paths (e.g. "/get") are joined onto.
        #[arg(long, default_value = "https://httpbin.org")]
        base_url: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Load { url, vus, duration } => {
            let duration = parse_duration(&duration)?;
            run_load(url, vus, duration).await
        }
        Command::Run { script, base_url } => run_script(&script, base_url).await,
    }
}

/// Raw request loop (Step 2), behind the `load` subcommand.
async fn run_load(url: String, vus: u32, duration: Duration) -> Result<()> {
    println!("Running {vus} VU(s) against {url} for {duration:?}...");

    let ok = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let client = reqwest::Client::new();
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

/// Run a Luau scenario file once, performing real asynchronous HTTP requests.
///
/// Two things are new versus Step 3. First, `http.get` is now an async host
/// function (`create_async_function`) that actually sends the request and
/// returns a `{ status = ... }` table. Second, the scenario body runs with
/// `call_async` — as a coroutine — so when it calls `http.get` the coroutine
/// suspends and awaits the request instead of blocking the thread.
async fn run_script(path: &Path, base_url: String) -> Result<()> {
    let source = fs::read_to_string(path)
        .with_context(|| format!("cannot read scenario file {}", path.display()))?;

    let lua = Lua::new();
    let client = reqwest::Client::new();

    // http.get(path) -> { status = <number> }. A real async request; the base
    // URL is prepended so scenarios can use short paths like "/get".
    let http = lua.create_table()?;
    let get_client = client.clone();
    let get_base = base_url.clone();
    http.set(
        "get",
        lua.create_async_function(move |lua, path: String| {
            // These clones are moved into the future so it owns everything it
            // needs (it must be `'static`).
            let client = get_client.clone();
            let base = get_base.clone();
            async move {
                let url = format!("{base}{path}");
                let response = client
                    .get(&url)
                    .send()
                    .await
                    .map_err(mlua::Error::external)?;
                let result = lua.create_table()?;
                result.set("status", response.status().as_u16())?;
                Ok(result)
            }
        })?,
    )?;
    lua.globals().set("http", http)?;

    // scenario(name, body): remember the scenario so we can run it (async) after
    // the file finishes loading. We stash it in a shared slot because the host
    // function only gets shared access to its surroundings.
    let slot: Arc<Mutex<Option<(String, Function)>>> = Arc::new(Mutex::new(None));
    let store = slot.clone();
    lua.globals().set(
        "scenario",
        lua.create_function(move |_, (name, body): (String, Function)| {
            *store.lock().unwrap() = Some((name, body));
            Ok(())
        })?,
    )?;

    // Loading the file runs its top-level code, which calls scenario(...).
    lua.load(source.as_str())
        .exec()
        .with_context(|| format!("error while loading {}", path.display()))?;

    // Run the stored scenario as a coroutine, driving its async http.get calls.
    let scenario = slot.lock().unwrap().take();
    let (name, body) =
        scenario.context("script defined no scenario; call scenario(name, function() ... end)")?;
    println!("scenario \"{name}\" running against {base_url}...");
    let _: () = body.call_async(1).await?;
    println!("done.");
    Ok(())
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
    fn scenario_calls_http_get_for_each_call() {
        // Sync round-trip check (no async, no network): a script that calls
        // http.get twice should invoke our host function twice.
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
        // The crux of Step 4, tested without any network: an async host function
        // that yields and then returns a value, called from a Luau coroutine via
        // `call_async`.
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
