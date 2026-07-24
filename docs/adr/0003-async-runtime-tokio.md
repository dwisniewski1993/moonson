# 0003 — Tokio as the async runtime

**Status:** accepted

## Context

A load generator is almost pure I/O: thousands of connections mostly waiting on
the network. That is the textbook case for async concurrency, and it needs a
runtime that scales to many concurrent tasks cheaply.

## Decision

Use **Tokio**, the de-facto async runtime for Rust.

## Alternatives considered

- **async-std.** Fine, but less momentum and a smaller ecosystem; the libraries
  we need (reqwest, tonic, tokio-tungstenite) target Tokio.
- **OS threads, one per VU.** Rejected: threads cost ~MBs of stack each; you
  cannot reach tens of thousands of VUs per node that way. Density dies.

## Consequences

- **Good:** millions of lightweight tasks; the whole protocol ecosystem we need
  is built on Tokio; `mlua`'s async feature plugs straight in.
- **Cost:** async Rust has a real learning curve (`Pin`, `Send` bounds,
  lifetimes in futures). Since the maintainer is new to Rust, the roadmap
  introduces async slowly, one concept per step.
