# Glossary

Short definitions of the load-testing terms used across these docs.

- **Load test** — pushing a controlled amount of traffic at a system to see how
  it behaves under expected load.
- **Stress test** — pushing *past* expected load to find the breaking point.
- **Soak test** — sustained load over a long time to find leaks and slow
  degradation.
- **Virtual user (VU)** — one simulated client. Many run at once. Density
  (wedge #1) is about how many VUs one machine can sustain.
- **Scenario** — the script describing what one VU does (log in, browse, ...).
- **Iteration** — one full run of a scenario by a VU. VUs usually loop
  iterations for the whole test duration.
- **Think time** — a deliberate pause inside a scenario, imitating a real user.
  `think(1)` = sleep 1 second.
- **Pacing** — controlling how often iterations start.
- **Open vs closed model** — *closed*: a fixed number of VUs, each starting a new
  iteration when the last finishes. *Open*: new iterations arrive at a set rate
  regardless of how many are in flight. M0 is closed.
- **RPS / throughput** — requests per second the tool generates.
- **Latency percentiles (p50/p95/p99)** — the response time under which 50 / 95 /
  99 percent of requests fall. p99 exposes the slow tail that averages hide.
- **Host function** — a Rust function exposed to the Luau script (e.g.
  `http.get`). The script calls it; Rust does the real work.
- **Coroutine** — a function that can pause ("yield") and resume later. Luau
  coroutines are how a scenario `await`s I/O without blocking a thread.
- **IR (intermediate representation)** — a normalised, protocol-agnostic form of
  a scenario the engine executes. Introduced when we need it, not before.
