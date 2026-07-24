# Scope

## MVP: Milestone 0 — "walking skeleton (HTTP)"

The smallest program that proves the whole architecture end to end: a Luau
scenario, executed by the Rust engine, firing real HTTP requests concurrently
across many virtual users, producing a metrics report.

### In scope for M0

- Run a `.luau` scenario file from the CLI.
- HTTP GET/POST with headers and JSON bodies.
- A `check()` assertion helper and a `think()` sleep.
- N virtual users for a fixed duration.
- End-of-run report: total requests, requests/sec, latency p50/p95/p99, errors.

### Out of scope for M0 (comes later)

- WebSocket and gRPC (Milestones 1 and 2 — our wedges, but we build the
  foundation first).
- Distributed master/worker runs.
- Prometheus/Grafana export.
- Data feeds (CSV), pacing models, ramp-up profiles.
- Anything resembling a GUI.

### Definition of done for M0

`moonson run examples/smoke.luau --vus 100 --duration 10s` runs against a local
test server, does not crash, and prints a correct summary. The code is formatted
(`cargo fmt`), lint-clean (`cargo clippy`), and has tests for the metrics math
and the scenario loading.

## Later milestones (rough — will evolve)

- **M1 — WebSocket:** `websocket.connect`, send/recv in a scenario, mixed with
  HTTP. (Wedge #2.)
- **M2 — gRPC streaming:** unary + server/client/bidi streaming via `tonic`.
  (Wedge #2.)
- **M3 — Density pass:** benchmark VUs/core vs k6; optimise the hot path; add the
  "fast path" for static requests. (Wedge #1.)
- **M4 — Observability & ergonomics:** Prometheus export, data feeds, ramp-up
  profiles, nicer reports.

Distribution (master/workers) comes only after a single node is excellent.
