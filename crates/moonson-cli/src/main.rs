//! moonson — walking-skeleton entry point.
//!
//! Right now this only prints a banner. That is intentional: Step 0 is about
//! having a repo that *builds and runs*, nothing more. Step 1 replaces the body
//! of `main` with the first real feature — a single asynchronous HTTP request.

fn main() {
    // `env!("CARGO_PKG_VERSION")` is filled in by Cargo at compile time from the
    // `version` field in Cargo.toml. It is not a runtime lookup.
    println!(
        "moonson {} — high-density load testing (walking skeleton)",
        env!("CARGO_PKG_VERSION")
    );
    println!("No scenario engine yet. See docs/03-roadmap.md — Step 1 is next.");
}
