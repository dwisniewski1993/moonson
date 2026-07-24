# moonson

A modern, high-density **load testing tool**: a fast execution engine written in
**Rust**, driven by test scenarios written in **Luau** (a small, fast, typed
scripting language).

> **Status: pre-alpha.** Being built in the open, step by step. Not usable yet —
> follow [`docs/03-roadmap.md`](docs/03-roadmap.md).

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
moonson run examples/smoke.luau --vus 100 --duration 30s
```

(Neither works yet — that is the target we are building toward.)

## Build & run

Install the Rust toolchain from https://rustup.rs, then:

```
cargo build                  # compile
cargo run -p moonson-cli     # run the CLI (prints a banner for now)
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
