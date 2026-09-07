# moonson

A modern, high-density **load testing tool**: a fast execution engine written in
**Rust**, driven by test scenarios written in **Luau** (a small, fast, typed
scripting language).

> **Status: early, but working.** HTTP, WebSocket, and gRPC (dynamic `.proto`,
> unary + streaming) load tests run today, and it's been benchmarked against k6
> (see [`docs/benchmarks/density.md`](docs/benchmarks/density.md)). Progress in
> [`docs/03-roadmap.md`](docs/03-roadmap.md).

## Why another load testing tool?

Most tools force a choice between *fast* and *easy to script*. moonson does not
try to beat everyone at everything — it bets on two specific strengths
("wedges"):

1. **Density** — as many virtual users per machine as possible, to cut the cost
   of generating load.
2. **Streaming protocols** — first-class WebSocket and gRPC streaming in the same
   scenario as HTTP, which today's tools handle awkwardly.

The reasoning behind each major decision lives in [`docs/adr/`](docs/adr/).

## What a test will look like

```lua
scenario("login_flow", function(vu)
  http.get("/login")
  local r = http.post("/auth", { json = { user = "test", pass = "123" } })
  check(r, { ["status is 200"] = r.status == 200 })
  think(1)
  http.get("/dashboard")
end)
```

```
moonson run examples/smoke.luau --vus 20 --duration 30s
```

WebSocket works too — a scenario can hold a long-lived socket and even mix it
with HTTP in the same virtual user:

```lua
scenario("mixed", function(vu)
  http.get("/login")
  local ws = websocket.connect("/stream")
  ws:send("hello")
  local reply = ws:recv()
  check(reply, { ["got a reply"] = reply ~= nil })
  ws:close()
end)
```

See [`examples/`](examples/) for runnable scenarios.

## Build & run

Install the Rust toolchain from https://rustup.rs, then:

```
cargo build                                          # compile
cargo run -p moonson-cli -- run examples/smoke.luau  # run a scenario
cargo run -p moonson-cli -- serve-echo               # local HTTP+WS echo for testing
```

## Documentation

| Doc | What's in it |
|-----|--------------|
| [`docs/00-vision.md`](docs/00-vision.md) | The problem and who it's for |
| [`docs/01-scope.md`](docs/01-scope.md) | What the MVP includes and excludes |
| [`docs/02-architecture.md`](docs/02-architecture.md) | How it is built |
| [`docs/03-roadmap.md`](docs/03-roadmap.md) | The step-by-step plan |
| [`docs/glossary.md`](docs/glossary.md) | Load-testing terms explained |
| [`docs/adr/`](docs/adr/) | Why we made each major decision |

## License

MIT — see [`LICENSE`](LICENSE).
