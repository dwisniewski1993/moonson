# Contributing / working on moonson

## Prerequisites

Install Rust via https://rustup.rs. The `rust-toolchain.toml` file pins the
stable channel and pulls in `rustfmt` and `clippy` automatically.

## Everyday commands

```
cargo build                  # compile everything
cargo run -p moonson-cli     # run the CLI
cargo test                   # run all tests
cargo fmt --all              # auto-format
cargo clippy --all-targets   # lint
```

CI runs `fmt --check`, `clippy -D warnings`, `build`, and `test` on every push.
Keep those green.

## Commit style

We use [Conventional Commits](https://www.conventionalcommits.org): a short
prefix so history is scannable.

```
feat: add async http.get host function
fix: correct p99 calculation off-by-one
docs: expand the density rationale in the vision
chore: bump tokio to 1.40
```

## Branching

`main` always builds and runs. Do work on a branch (`feat/http-get`), open a PR,
let CI pass, merge. For a solo hobby project this is deliberately light process —
just enough to stay honest.

## Where decisions live

Anything non-obvious ("why Luau and not Python?") goes in an ADR under
`docs/adr/`. If you catch yourself explaining a choice twice, write the ADR.
