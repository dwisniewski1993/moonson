# 0002 — Cargo workspace layout

**Status:** accepted

## Context

moonson will grow several distinct pieces: a CLI, an engine core, protocol
clients, and a scripting layer. We want them decoupled and independently
testable without splitting into separate repositories.

## Decision

Use a single **Cargo workspace**. Start with one crate (`moonson-cli`) and split
out `moonson-script`, `moonson-http`, and `moonson-core` as each earns a clear
boundary (Steps 3–5).

## Alternatives considered

- **One flat crate forever.** Simplest, but everything ends up coupled and the
  compile/test story gets muddy as protocols pile up.
- **Multiple repositories.** Overkill for a solo/hobby project; painful to change
  things that span crates.

## Consequences

- **Good:** clean boundaries, shared dependency versions via
  `[workspace.dependencies]`, faster incremental builds.
- **Cost:** a little upfront structure a beginner has to get used to. We add
  crates lazily to keep the learning curve gentle.
