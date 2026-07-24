# Roadmap

The plan is a series of small steps. Each ends with a program you can run and a
way to check it works. I write the code and tests for a step; you run it, confirm
it behaves, and we move on.

## Milestone 0 — walking skeleton (HTTP)

### Step 0 — Repo bootstrap + docs  ✅
Workspace, license, CI, and these documents. A binary that builds and prints a
banner.
**Check:** `cargo run -p moonson-cli` prints the moonson banner.

### Step 1 — First async HTTP GET  ✅
Replace the banner with one real request using Tokio + reqwest.
**Teaches:** dependencies, `async`/`await`, the `#[tokio::main]` entry point.
**Check:** `cargo run -p moonson-cli -- https://httpbin.org/get` prints `200`.

### Step 2 — Many virtual users for a duration  ✅
Spawn N async tasks that loop the request for D seconds; count them.
**Teaches:** `tokio::spawn`, `Arc`, atomic counters, `tokio::time`.
**Check:** `... --vus 50 --duration 5s <url>` prints total requests and req/s.

### Step 3 — Embed Luau
Load a `.luau` file, register a `scenario(name, fn)` function, run its body once
with a stub `http.get`.
**Teaches:** embedding a scripting VM, exposing Rust functions to a script.
**Check:** a script calling `http.get("/")` makes Rust log the call.

### Step 4 — The async bridge (the crux)
Make `http.get` a real async host function; run the scenario as a coroutine so
the script can `await` real HTTP without blocking. Proves the whole DSL model.
**Check:** the script performs a real GET and reads `r.status`; `check()` works.

### Step 5 — Scale + metrics (M0 done)
Run the scenario across N VUs for a duration; aggregate latency into a histogram;
print the report.
**Check:** `moonson run examples/smoke.luau --vus 100 --duration 10s` prints
requests, RPS, p50/p95/p99, and errors.

## After M0

- **M1 — WebSocket** (wedge #2)
- **M2 — gRPC streaming** (wedge #2)
- **M3 — Density benchmark & fast path** (wedge #1)
- **M4 — Observability, data feeds, ramp profiles**

We revisit and rewrite this file as we learn. That is expected, not a failure.
