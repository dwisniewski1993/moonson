# Architecture

## Execution model: "script = runtime, plus a fast path"

A moonson scenario is **not** a static description compiled once. It is a real
program that runs once per iteration, per virtual user. When the script calls
`http.get(...)`, it calls into a Rust *host function* that does the actual async
I/O, then hands the result back to the script.

This is what makes streaming natural: `stream:recv()` simply suspends the
scenario's coroutine until the next message arrives, without blocking the OS
thread. It is the same model wrk (LuaJIT) and k6 (JavaScript) use — we use Rust +
Luau.

The trade-off: running the script on every request costs something. To protect
density (wedge #1), static/templated requests will eventually take a **fast
path** that skips the script VM. That optimisation is Milestone 3; we do not
build it until we can measure it.

## Layers

```
  CLI            parse args, load scenario, print the report
   |
  Script layer   Luau runtime (mlua); exposes http/ws/grpc host functions;
   |             runs each VU's scenario as an async coroutine
   |
  Protocol layer HTTP (reqwest/hyper), later WebSocket (tokio-tungstenite),
   |             later gRPC (tonic)
   |
  Core engine    virtual-user scheduler, iteration loop, timing, metrics
   |
  Tokio runtime  async I/O; a few OS threads drive thousands of VUs
```

## Crate layout (grows with the milestones)

We use a **Cargo workspace** — one repository containing several small crates —
so the pieces stay decoupled and compile independently.

| Crate            | Responsibility                            | Added in |
|------------------|-------------------------------------------|----------|
| `moonson-cli`    | binary: args, orchestration, reporting    | Step 0   |
| `moonson-script` | Luau runtime + host-function bindings     | Step 3   |
| `moonson-http`   | HTTP host functions                       | Step 4   |
| `moonson-core`   | scheduler, VU loop, metrics, report types | Step 5   |

Starting with one crate keeps things simple; we split pieces out as they earn a
boundary. See [`adr/0002`](adr/0002-cargo-workspace-layout.md).

## Key libraries

- **tokio** — async runtime. See [`adr/0003`](adr/0003-async-runtime-tokio.md).
- **mlua** (`luau` + `async` features) — embeds Luau and bridges its coroutines
  to Rust async. The heart of the design. See
  [`adr/0001`](adr/0001-luau-as-scripting-language.md).
- **reqwest** — HTTP client for the first milestone (simple, batteries-included;
  we may drop to raw `hyper` later for density).
- **hdrhistogram** — accurate latency percentiles (p95/p99) cheaply.
