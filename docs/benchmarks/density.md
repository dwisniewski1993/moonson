# Density benchmark: moonson vs k6

Wedge #1 is the claim that moonson can drive more load per machine than the
alternatives. This is where we test that honestly — including the real
possibility that, for plain HTTP, moonson is only *comparable* to k6 (both are
fast, and the work is I/O-bound). A comparable result is still useful: it tells
us the density advantage, if any, lives in memory-per-VU or in streaming, not in
raw HTTP requests/second.

## What we measure

- **Throughput** — requests/second each tool reports at a fixed VU count.
- **Memory** — peak RSS of the load-generator process, and RSS ÷ VUs.
- **Scaling** — how throughput and memory change as VUs grow.

## Caveats (read before trusting any number)

- **One machine.** Here the target and the load generator share CPU cores, and
  traffic goes over loopback (no real network). This is a *relative* signal, not
  an absolute one. A proper test puts target and generator on separate hosts.
- **The target must not be the bottleneck.** We use `moonson serve-bench`, a
  minimal keep-alive HTTP server that returns `ok`. If it saturates a core, you
  are measuring the target, not the generators — watch its CPU in Activity
  Monitor.
- **Use `--release`.** Debug builds are several times slower.
- **Warm up and repeat.** Run each a few times and take the median.

## Setup

Terminal 1 — the target:

```
cargo run --release -p moonson-cli -- serve-bench     # listens on 127.0.0.1:8080
```

Install k6 if needed: `brew install k6`.

## Run

Build moonson once so timing excludes the compiler:

```
cargo build --release -p moonson-cli
```

moonson (record the `throughput` line and the peak RSS):

```
/usr/bin/time -l ./target/release/moonson \
    run bench/bench.luau --base-url http://127.0.0.1:8080 --vus 50 --duration 20s
```

k6 (record `http_reqs` rate; keep VUs/duration identical in bench.js):

```
/usr/bin/time -l k6 run bench/bench.js
```

On macOS, `/usr/bin/time -l` prints "maximum resident set size" in bytes at the
end — that is peak RSS. Divide by the VU count for RSS/VU.

## Results (fill in)

| VUs  | tool    | req/s | peak RSS (MB) | RSS/VU (KB) |
|------|---------|-------|---------------|-------------|
| 50   | moonson |       |               |             |
| 50   | k6      |       |               |             |
| 200  | moonson |       |               |             |
| 200  | k6      |       |               |             |
| 1000 | moonson |       |               |             |
| 1000 | k6      |       |               |             |

## Interpreting the outcome

- **req/s similar:** expected for HTTP — the wedge is not raw throughput. Fine.
- **RSS/VU much lower for moonson:** that is the density story. Cheaper VUs mean
  more of them per box (and per dollar in the cloud).
- **moonson worse:** a signal to profile. The prime suspect is the one Lua state
  per VU — Milestone 3's "fast path" (skip the VM for static requests) and Lua
  state pooling target exactly this. That would be a genuinely important finding,
  not a failure.
