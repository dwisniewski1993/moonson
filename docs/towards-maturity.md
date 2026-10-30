# From hobby project to a product enterprises notice

The engine is proven: both wedges (density, streaming) are implemented and
measured. Maturity from here is **not more engine** — it is trust, adoption, and
proof at scale, roughly in that order. The thing to optimise for is one event:

> a load/performance engineer at a real company tries moonson unprompted, keeps
> it, and tells a colleague.

Everything below exists to make that happen. (Enterprise *procurement* — SSO,
SOC 2, contracts, sales — is a later, different game and pure cost until there is
demand; it is deliberately out of scope here.)

## Guiding principle

Lead with the wedges. Every phase should sharpen one sentence: *"the lightest,
fastest load generator that also does streaming well."* That is the only reason
someone switches away from k6, so never bury it.

## Phase 1 — Trustworthy OSS (a stranger can adopt it in 5 minutes)

Turn a clean repo into a tool people actually install and run.

- **Distribution:** GitHub Releases with prebuilt binaries (macOS + Linux,
  x86_64 + arm64), a Docker image, `cargo install`, maybe a Homebrew tap. CI
  builds and publishes them on tag.
- **CI hardening:** wire the built-in `serve-echo` / `serve-grpc` / `serve-bench`
  servers into integration tests for HTTP, WebSocket, and gRPC; keep
  fmt + clippy + test green on a matrix; pin an MSRV.
- **Release hygiene:** a CHANGELOG, a stated semver policy, and a README with a
  60-second quickstart and the k6 benchmark table up top.
- **Remove rough edges:** drop the legacy `load` subcommand; a scenario error
  should count-and-continue, not abort the whole run; add Prometheus `HELP`
  lines; friendlier CLI errors.

Done when: `brew install` / `docker run` / `cargo install`, then run a real test,
in under five minutes, straight from the README.

## Phase 2 — Usable for real tests, not demos

The features that make moonson *replace* someone's k6/Locust script.

- **Thresholds / pass-fail (highest ROI):** `--threshold "p95<200ms,errors<1%"`
  with a non-zero exit code on breach. This is the single feature that gets a
  load tool into CI gates — do it first.
- **Multiple scenarios + tags/groups**, and a per-tag (per-endpoint) metric
  breakdown in the report.
- **`setup()` / `teardown()` hooks**, environment variables and secrets into
  scenarios (`--env KEY=VALUE`).
- **HTTP realism:** redirects, a cookie jar, gzip, HTTP/2.
- **Reporting:** an HTML summary alongside the JSON we have, plus a ready
  GitHub Actions example.

Done when: someone can port a real k6 HTTP test and run it in CI with pass/fail.

## Phase 3 — Prove the wedge at scale (the headline differentiator)

Make the density/streaming story undeniable beyond one machine.

- **Distributed runs:** a coordinator plus workers (over gRPC or QUIC), scenario
  distribution, metric aggregation across nodes, one aggregate report. Start
  with a static worker list; Kubernetes-native later.
- **A minimal results store:** persist each run (SQLite/JSON) and a `compare`
  command for run-over-run regression detection. CLI-level is enough to start.
- **Re-benchmark at distributed scale** vs k6 with a reproducible methodology
  writeup, and publish it.

Done when: one coordinator drives N workers against a target and reports an
aggregate p99, and the per-VU memory advantage still holds across nodes.

## Phase 4 — Fit existing stacks (observability ecosystem)

Slot into what teams already run.

- Prometheus `HELP`/labels + a shippable Grafana dashboard JSON.
- Optional OTLP / Datadog output.
- Ready-made CI templates (GitHub, GitLab, Jenkins).

Done when: it plugs into existing monitoring with no custom glue.

## Cross-cutting: adoption (do this throughout, not at the end)

- **A benchmark blog post** with the honest k6 comparison (≈5x lighter, full
  methodology, caveats). This is the highest-leverage single act — it is how
  anyone finds the project at all. Pair it with Show HN / r/rust / the
  load-testing communities and an asciinema demo in the README.
- **Responsiveness:** fast issue triage, a CONTRIBUTING guide, good-first-issues,
  a Discussions or Discord space.
- **De-risk Luau** (the main adoption tax vs k6's JS/TS): an excellent scenario
  cookbook, editor type hints, and a "porting from k6" guide.

## What NOT to do yet

- SSO / RBAC / multi-tenancy / SOC 2: zero value before paying enterprise
  interest; pure cost.
- A hosted cloud product: only after single-node OSS has real adoption *and*
  distributed works.
- Protocols nobody asked for: add one only when a real user needs it.

## Sequencing and gating

Phase 1 gates everything — there is no adoption without a frictionless install.
Phase 2 next, because thresholds and ergonomics are what turn a demo into a
tool people keep. Build Phase 3 (distributed — the expensive one) **only once
Phases 1–2 have real users asking for scale**: that demand is the proof it is
worth the cost. Phase 4 lands whenever it unblocks a specific user.

The order is deliberately adoption-first, not feature-first: a tool nobody has
tried does not need to scale yet.
