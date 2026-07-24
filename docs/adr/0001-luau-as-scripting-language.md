# 0001 — Luau as the scripting language

**Status:** accepted

## Context

moonson's premise is a fast Rust engine driven by a friendly scripting language.
That language is the single most important technical decision, because it
constrains both wedges:

- **Density** — the per-VU memory and per-call overhead of the scripting VM
  directly caps how many VUs fit on a machine.
- **Streaming** — the language must express "wait for the next message"
  naturally, which means integrating with async Rust.

## Decision

Use **Luau** (Roblox's typed, sandboxed dialect of Lua) embedded via the
**`mlua`** crate, with its `luau` and `async` features. A scenario is a Luau
coroutine; I/O primitives (`http`, `ws`, `grpc`) are async Rust host functions
the coroutine awaits.

## Alternatives considered

- **Python / Ruby bridge (like Locust).** Rejected: calling a heavy interpreter
  per request across an FFI boundary, plus Python's GIL, destroys density — the
  exact problem we want to beat.
- **JavaScript via Boa / QuickJS.** Workable, but JS VMs are heavier per instance
  (worse density), and this is precisely k6's home turf — a bad place to compete
  head-on.
- **Rhai (native Rust scripting).** Pleasant and pure-Rust, but weaker async
  story, no type system, and a much smaller ecosystem than Lua.
- **YAML-only (like Artillery / Drill).** Rejected as the primary model: it
  cannot express the branching, correlation, and message loops streaming needs.
  It survives only as optional sugar that compiles down to Luau.

## Why Luau specifically

- Tiny VM (hundreds of KB), memory-optimised on 64-bit → good density.
- `mlua` bridges Lua coroutines to Rust async on any executor (Tokio) → natural
  streaming, no thread blocking.
- Gradual typing → safer, self-documenting scenarios; a real edge over k6's
  untyped scripts.
- Per-script sandbox (`safeenv`) → cheap isolation between VUs.
- Precedent: wrk (the fastest HTTP benchmarker) scripts in LuaJIT; the pattern is
  proven.

## Consequences

- **Good:** the architecture directly serves both wedges; streaming is easy;
  density has a real shot.
- **Cost:** Luau is less known than Python/JS — adoption friction. Mitigate with
  (a) Lua familiarity, (b) types + good docs, (c) an optional YAML front-end for
  simple cases, (d) scenario generation from OpenAPI/proto later.
- **Risk to watch:** per-request script overhead. We measure it in M3 and add a
  fast path for static requests if needed.
