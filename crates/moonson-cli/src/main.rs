//! moonson — command-line entry point.
//!
//! Step 3 embeds the Luau scripting engine. The CLI now has two subcommands:
//! `load` runs the raw request loop from Step 2 (N VUs for a duration), and
//! `run` loads a `.luau` scenario file and executes it once. For now the
//! `http.get` that a scenario calls is a stub that only logs; Step 4 turns it
//! into a real asynchronous request.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use mlua::{Function, Lua};

#[derive(Parser)]
#[command(
    name = "moonson",
    version,
    about = "High-density load testing (walking skeleton)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Fire raw requests at a URL with N virtual users for a fixed duration.
    Load {
        /// Target URL to hit.
        #[arg(default_value = "https://httpbin.org/get")]
        url: String,
        /// Number of virtual users (concurrent workers).
        #[arg(long, default_value_t = 1)]
        vus: u32,
        /// How long to run: e.g. 500ms, 10s, 2m.
        #[arg(long, default_value = "5s")]
        duration: String,
    },
    /// Run a Luau scenario file once (I/O is still stubbed in this step).
    Run {
        /// Path to a .luau scenario file.
        script: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Load { url, vus, duration } => {
            let duration = parse_duration(&duration)?;
            run_load(url, vus, duration).await
        }
        Command::Run { script } => run_script(&script),
    }
}

/// Raw request loop (Step 2), now living behind the `load` subcommand.
async fn run_load(url: String, vus: u32, duration: Duration) -> Result<()> {
    println!("Running {vus} VU(s) against {url} for {duration:?}...");

    let ok = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let client = reqwest::Client::new();
    let deadline = Instant::now() + duration;

    let mut handles = Vec::with_capacity(vus as usize);
    for _ in 0..vus {
        let client = client.clone();
        let url = url.clone();
        let ok = ok.clone();
        let failed = failed.clone();
        handles.push(tokio::spawn(async move {
            while Instant::now() < deadline {
                match client.get(&url).send().await {
                    Ok(_response) => ok.fetch_add(1, Ordering::Relaxed),
                    Err(_error) => failed.fetch_add(1, Ordering::Relaxed),
                };
            }
        }));
    }
    for handle in handles {
        handle.await.context("a virtual user task panicked")?;
    }

    let ok = ok.load(Ordering::Relaxed);
    let failed = failed.load(Ordering::Relaxed);
    let secs = duration.as_secs_f64();
    let rps = if secs > 0.0 { ok as f64 / secs } else { 0.0 };
    println!("---");
    println!("requests: {ok}   errors: {failed}");
    println!("throughput: {rps:.0} req/s");
    Ok(())
}

/// Load a `.luau` file and run the scenario it defines, once.
///
/// This is the first time Rust and the script talk to each other. We expose two
/// things to the script — an `http` table with a `get` function, and a
/// `scenario(name, body)` function — then loading the file runs its top-level
/// code, which calls `scenario(...)`, which calls the body back.
fn run_script(path: &Path) -> Result<()> {
    let source = fs::read_to_string(path)
        .with_context(|| format!("cannot read scenario file {}", path.display()))?;

    let lua = Lua::new();

    // `http.get(url)` — a stub host function that only logs the call for now.
    let http = lua.create_table()?;
    http.set(
        "get",
        lua.create_function(|_, url: String| {
            println!("  http.get {url}");
            Ok(())
        })?,
    )?;
    lua.globals().set("http", http)?;

    // `scenario(name, body)` — register a scenario and run it once immediately,
    // passing a placeholder virtual-user id. Real scheduling arrives in Step 5.
    lua.globals().set(
        "scenario",
        lua.create_function(|_, (name, body): (String, Function)| {
            println!("scenario \"{name}\" running...");
            let _: () = body.call(1)?;
            Ok(())
        })?,
    )?;

    // Running the file triggers the `scenario(...)` call inside it.
    lua.load(source.as_str())
        .exec()
        .with_context(|| format!("error while running {}", path.display()))?;

    Ok(())
}

/// Turn a string like "10s" into a `Duration`. Supports `ms`, `s`, and `m`.
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let split = s
        .find(|c: char| c.is_ascii_alphabetic())
        .with_context(|| format!("missing time unit in '{s}' (e.g. 10s, 500ms, 2m)"))?;
    let (number, unit) = s.split_at(split);
    let value: u64 = number
        .parse()
        .with_context(|| format!("invalid number in '{s}'"))?;
    let duration = match unit {
        "ms" => Duration::from_millis(value),
        "s" => Duration::from_secs(value),
        "m" => Duration::from_secs(value * 60),
        other => bail!("unknown time unit '{other}' (use ms, s, or m)"),
    };
    Ok(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_seconds() {
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
    }

    #[test]
    fn parses_milliseconds() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
    }

    #[test]
    fn parses_minutes() {
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
    }

    #[test]
    fn rejects_missing_unit() {
        assert!(parse_duration("10").is_err());
    }

    #[test]
    fn rejects_unknown_unit() {
        assert!(parse_duration("10h").is_err());
    }

    #[test]
    fn scenario_calls_http_get_for_each_call() {
        // A self-contained check of the Rust <-> Luau round-trip: a script that
        // calls http.get twice should invoke our host function twice. No files,
        // no network.
        let lua = Lua::new();
        let calls = Arc::new(AtomicU64::new(0));

        let http = lua.create_table().unwrap();
        let counter = calls.clone();
        http.set(
            "get",
            lua.create_function(move |_, _url: String| {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
        lua.globals().set("http", http).unwrap();
        lua.globals()
            .set(
                "scenario",
                lua.create_function(|_, (_name, body): (String, Function)| {
                    let _: () = body.call(())?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();

        lua.load(
            r#"
            scenario("t", function()
              http.get("/a")
              http.get("/b")
            end)
            "#,
        )
        .exec()
        .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }
}
