# Vision

## The problem

Load testing tools force a trade-off. The fast engines (Gatling, Goose) make you
write scenarios in a "serious" language (Scala, Rust). The easy ones (Locust,
Artillery) are limited by the speed of their scripting layer or a rigid config
format. k6 is the current sweet spot — a fast Go engine scripted in JavaScript —
and it is genuinely good.

We are **not** trying to beat k6 at everything. That is a losing fight. Instead
moonson picks two narrow strengths and goes deep.

## The two wedges

### 1. Density (virtual users per machine)

Cloud load generators cost money. If one moonson node drives meaningfully more
virtual users (VUs) than the alternatives, a test that needed 10 machines needs
3. That is concrete and measurable: **VUs per core, and VUs per dollar.**

We get there by combining a Rust + Tokio engine with **Luau**, whose virtual
machine is tiny (hundreds of KB), sandboxed per-VU, and bridges cleanly to async
Rust — no per-request cross-language marshalling, no GIL.

### 2. Streaming protocols

WebSocket and gRPC streaming are stateful and long-lived. Most tools bolt them
on: k6, for example, cannot cleanly mix its experimental WebSocket API and HTTP
in one script. moonson treats a scenario as an async coroutine, so waiting for
"the next WebSocket frame" or "the next gRPC message" is as natural as a `for`
loop — and HTTP, WS, and gRPC can share one virtual user.

## Non-goals (for now)

- Beating every tool on every protocol or feature.
- A GUI. moonson is tests-as-code, CLI-first.
- Enterprise production-readiness. **The first honest target is a tool good
  enough for hobbyists and small teams.**

## Who it's for

Developers, QA, and SREs comfortable with a CLI, who want a scriptable load
tester that scales well on one box and handles streaming protocols without
fighting the tool.

## Guiding principles

1. **A thin end-to-end slice beats a big unfinished layer.** We always keep a
   working program.
2. **Write down the "why."** Big decisions go in `docs/adr/`.
3. **Density is a number, not a vibe.** We benchmark against k6 and keep the
   result honest.
