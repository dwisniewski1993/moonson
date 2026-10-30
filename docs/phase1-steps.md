# Phase 1 — granular steps (trustworthy OSS)

Goal (from [`towards-maturity.md`](towards-maturity.md)): a stranger can install
moonson and run a real test in five minutes, and the repo looks maintained and
correct. Small steps, each shippable and verifiable. Rhythm as for M0–M4: code +
tests written for a step, you run and confirm, we commit per step.

## Step 1.1 — A scenario error must not abort the whole run
Today a single failed iteration (a Lua error, or a host error like a gRPC/WS
failure surfaced to the script) propagates up and kills the entire run. A real
tool has to survive a bad iteration.
- **Work:** catch per-iteration errors in the VU loop; record them (an
  `iteration_errors` counter in the report) and continue; only genuine setup
  failures (cannot build the Lua state or load the script) abort.
- **Done when:** a scenario that errors on some iterations still runs to the
  deadline and reports an error count instead of bailing out.

## Step 1.2 — Integration tests in CI
- **Work:** tests that start `serve-echo` / `serve-grpc` / `serve-bench`
  in-process and run a short scenario against each (HTTP, WebSocket, gRPC),
  asserting zero transport errors and the expected checks; run them in CI next
  to fmt/clippy/unit tests; pin an MSRV.
- **Done when:** CI runs end-to-end protocol tests on every push and they pass.

## Step 1.3 — Remove rough edges
- **Work:** drop the legacy `load` subcommand (superseded by `run` +
  `serve-bench`); add `# HELP` lines to the Prometheus output; turn `expect`s on
  user input into friendly errors; tidy `--help` text.
- **Done when:** no dead subcommand, `/metrics` is fully valid, and bad input
  prints a clear message rather than panicking.

## Step 1.4 — Release automation (binaries on tag)
- **Work:** a GitHub Actions release workflow that, on a `v*` tag,
  cross-compiles release binaries for macOS (x86_64 + arm64) and Linux
  (x86_64 + arm64) and attaches them to a GitHub Release; build and push a Docker
  image.
- **Done when:** pushing a tag produces a GitHub Release with downloadable
  binaries and a container image.

## Step 1.5 — Install paths
- **Work:** make `cargo install --git …` work (binary metadata), add a small
  `Dockerfile` and document `docker run …`; a Homebrew tap is optional and can
  come later.
- **Done when:** at least two of {prebuilt binary, `cargo install`, `docker run`}
  produce a working `moonson` with no toolchain fuss.

## Step 1.6 — README overhaul + CHANGELOG
- **Work:** a README whose first screen is what/why (the two wedges), a
  30-second quickstart using the real install commands from 1.4–1.5, the k6
  benchmark table, and links to `examples/`; add a demo recording; add a
  CHANGELOG and state the semver policy.
- **Done when:** a stranger can go from the README to a running test in about
  five minutes.

## Order and notes
1.1 → 1.3 are small local code/doc changes (fast). 1.2 locks correctness before
we ship anything. 1.4–1.5 are mostly CI and packaging (you run the pushes; we
read the Actions logs together). 1.6 is the shopfront and goes last, once the
install commands are real.
