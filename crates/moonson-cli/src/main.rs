//! moonson — command-line entry point.
//!
//! Step 1: take a URL from the command line, send one asynchronous HTTP GET,
//! and print the response status. This is the smallest possible "real" request;
//! Step 2 will run it many times concurrently.

use std::env;

use anyhow::{Context, Result};

/// URL used when the user does not pass one on the command line.
const DEFAULT_URL: &str = "https://httpbin.org/get";

/// Pick the target URL from the process arguments.
///
/// Argument 0 is always the program's own name, so the URL — if given — is
/// argument 1. Pulling this into its own function keeps it easy to unit-test
/// without touching the network (see the tests at the bottom of the file).
fn pick_url<I: Iterator<Item = String>>(mut args: I) -> String {
    args.nth(1).unwrap_or_else(|| DEFAULT_URL.to_string())
}

// `#[tokio::main]` rewrites this async `main` into a normal synchronous `main`
// that (1) starts the Tokio async runtime and (2) runs our async code on it.
// It is what lets us use `.await` inside `main`.
//
// Returning `anyhow::Result<()>` means we can use the `?` operator: on error it
// returns early and Tokio prints the error before exiting with a non-zero code.
#[tokio::main]
async fn main() -> Result<()> {
    let url = pick_url(env::args());
    println!("GET {url}");

    // `reqwest::get` builds and sends the request. It is async, so `.await`
    // suspends `main` until the response headers arrive — without blocking the
    // OS thread, which is the whole point for a load generator. `?` bails out
    // with a helpful message if the request itself fails (bad URL, DNS, TLS...).
    let response = reqwest::get(url.as_str())
        .await
        .with_context(|| format!("request to {url} failed"))?;

    // `.status()` is the HTTP status code; `.as_u16()` turns it into a plain
    // number like 200 or 404.
    println!("status: {}", response.status().as_u16());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_first_argument_as_url() {
        let args = ["moonson", "https://example.com"].map(String::from);
        assert_eq!(pick_url(args.into_iter()), "https://example.com");
    }

    #[test]
    fn falls_back_to_default_when_no_argument() {
        let args = ["moonson"].map(String::from);
        assert_eq!(pick_url(args.into_iter()), DEFAULT_URL);
    }
}
