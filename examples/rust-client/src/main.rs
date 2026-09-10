//! Minimal **native Rust client** example: a small CLI over
//! [`libfw_client::native::NativeClient`].
//!
//! It shows the three moving parts of embedding the Rust client in your own
//! program:
//!
//! * building a [`ClientConfig`] (the same knobs as the browser SDK),
//! * installing an event sink for progress / tuning updates, and
//! * calling the async transfer methods.
//!
//! Run it against any libfw server (e.g. the bundled axum example):
//!
//! ```text
//! # terminal 1 — a libfw server on :8080 with the dev token
//! cargo run -p axum-server -- dev-data 8080
//!
//! # terminal 2 — this example
//! cargo run -p rust-client -- ls
//! cargo run -p rust-client -- upload ./README.md docs/README.md
//! cargo run -p rust-client -- download docs/README.md ./downloaded.md
//! cargo run -p rust-client -- capabilities
//! ```
//!
//! `--url` / `--token` (or `LIBFW_URL` / `LIBFW_TOKEN`) point it at a server;
//! everything else is optional.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use libfw_client::native::{EventSink, NativeClient, NativeConfig, NativeEvent};
use libfw_client::{ClientConfig, CompressLevel};

const USAGE: &str = "\
libfw Rust client example

USAGE:
    rust-client [OPTIONS] <COMMAND> [ARGS]

COMMANDS:
    ls [DIR]                       List a remote directory (default: root)
    download <REMOTE> <DEST>       Download a file or a whole folder
    upload <LOCAL> [REMOTE]        Upload a file, or a folder tree
    capabilities                   Print the server's /capabilities payload

OPTIONS:
    --url <URL>                    Server base URL            (env LIBFW_URL)
    --token <TOKEN>                Bearer token               (env LIBFW_TOKEN)
    --concurrency <N>              Max concurrent files       (default 4)
    --window <N>                   Per-file in-flight window  (default 8/4)
    --chunk-size <BYTES>           Shared chunk size          (default 2 MiB)
    --max-retries <N>              Retries per block          (default 3)
    --timeout-ms <MS>              Stall timeout              (default 60000)
    --compress-level <POLICY>      auto|fast|balanced|max|N   (default balanced)
    --no-compress                  Disable zrip compression
    --auto-tune                    Adapt to the link via /capabilities
    --quiet, -q                    No progress output
    --help, -h                     Show this help
";

/// One parsed CLI invocation.
struct Options {
    base_url: String,
    token: String,
    command: Command,
    config: ClientConfig,
    quiet: bool,
    auto_tune: bool,
}

enum Command {
    List(String),
    Download { remote: String, dest: PathBuf },
    Upload { local: PathBuf, remote: String },
    Capabilities,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let opts = match Options::parse(args) {
        Ok(opts) => opts,
        Err(msg) => {
            if !msg.is_empty() {
                eprintln!("error: {msg}\n");
            }
            eprintln!("{USAGE}");
            return if msg.is_empty() {
                ExitCode::SUCCESS // --help
            } else {
                ExitCode::FAILURE
            };
        }
    };
    match run_async(opts).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run_async(opts: Options) -> Result<(), Box<dyn std::error::Error>> {
    let quiet = opts.quiet;
    let progress = Arc::new(Progress::default());
    let sink: EventSink = {
        let progress = progress.clone();
        Arc::new(move |event| progress.handle(event, quiet))
    };

    let mut config = opts.config.clone();
    config.auto_tune = opts.auto_tune;
    let native = NativeConfig::new(&opts.base_url, &opts.token)
        .with_client(config)
        .with_events(sink);
    let client = NativeClient::with_config(native);

    match opts.command {
        Command::Capabilities => {
            let caps = client.fetch_capabilities().await?;
            println!("protocol       : {}", caps.protocol);
            println!("compression    : {:?}", caps.compression.formats);
            println!(
                "zrip levels    : min={} default={} max={}",
                caps.compression.zrip_levels.min,
                caps.compression.zrip_levels.default,
                caps.compression.zrip_levels.max
            );
            println!(
                "concurrency    : min={} default={} max={}",
                caps.limits.concurrency.min,
                caps.limits.concurrency.default,
                caps.limits.concurrency.max
            );
            println!(
                "upload window  : min={} default={} max={}",
                caps.limits.upload_window.min,
                caps.limits.upload_window.default,
                caps.limits.upload_window.max
            );
            println!(
                "download window: min={} default={} max={}",
                caps.limits.download_window.min,
                caps.limits.download_window.default,
                caps.limits.download_window.max
            );
            println!(
                "chunk size     : min={} default={} max={}",
                caps.limits.chunk_size.min,
                caps.limits.chunk_size.default,
                caps.limits.chunk_size.max
            );
            println!("max upload size: {}", caps.limits.max_upload_size);
            println!("caps hash      : {}", caps.caps_hash());
        }
        Command::List(dir) => {
            let entries = client.list(&dir).await?;
            if entries.is_empty() {
                println!("(empty)");
            }
            for entry in entries {
                let kind = if entry.is_dir { "dir " } else { "file" };
                println!("{kind} {:>12}  {}", entry.size, entry.path);
            }
        }
        Command::Download { remote, dest } => {
            // `stat` answers 404 for a folder, so it doubles as the
            // "is this a folder?" probe.
            match client.stat(&remote).await {
                Ok(stat) => {
                    let bytes = client.download_file(&remote, &dest).await?;
                    println!(
                        "downloaded `{remote}` ({} bytes) → {} ({bytes} bytes this run)",
                        stat.size,
                        dest.display()
                    );
                }
                Err(libfw_client::LibfwError::Http { status: 404, .. }) => {
                    let bytes = client.download_folder(&remote, &dest).await?;
                    println!(
                        "downloaded folder `{remote}` → {} ({bytes} bytes)",
                        dest.display()
                    );
                }
                Err(e) => return Err(e.into()),
            }
        }
        Command::Upload { local, remote } => {
            if local.is_dir() {
                let bytes = client.upload_folder(&local, &remote).await?;
                println!("uploaded folder {} → `{remote}` ({bytes} bytes)", local.display());
            } else {
                let bytes = client.upload_file(&local, &remote).await?;
                println!("uploaded {} → `{remote}` ({bytes} bytes)", local.display());
            }
        }
    }

    // With `--auto-tune` the engine reports the parameters it settled on.
    if let Some(status) = client.tune_status() {
        println!(
            "tuning: {:?} concurrency={} window={}/{} chunk={} level={}",
            status.phase,
            status.params.concurrency,
            status.params.upload_window,
            status.params.download_window,
            status.params.chunk_size,
            status.params.compress_level
        );
    }
    Ok(())
}

/// Prints one progress line per file, throttled to whole percent steps.
#[derive(Default)]
struct Progress {
    last_pct: AtomicU64,
}

impl Progress {
    fn handle(&self, event: NativeEvent, quiet: bool) {
        if quiet {
            return;
        }
        match event {
            NativeEvent::FileStart { path, size } => {
                println!("→ {path} ({size} bytes)");
            }
            NativeEvent::Progress { done, total } => {
                let pct = if total == 0 { 100 } else { done * 100 / total };
                if self.last_pct.swap(pct, Ordering::Relaxed) != pct {
                    print!("\r  {pct:>3}% of {total} bytes");
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                }
            }
            NativeEvent::FileDone { bytes, .. } => {
                println!("\r✔ done ({bytes} bytes this run)               ");
            }
            NativeEvent::Tuning(event) => {
                println!(
                    "  tune: {:?} concurrency={} chunk={} mbps={:.1} rtt={:.0}ms",
                    event.phase,
                    event.params.concurrency,
                    event.params.chunk_size,
                    event.stats.mbps,
                    event.stats.rtt_ms
                );
            }
            NativeEvent::Log(msg) => eprintln!("  · {msg}"),
        }
    }
}

impl Options {
    fn parse(args: Vec<String>) -> Result<Options, String> {
        let mut base_url = std::env::var("LIBFW_URL").unwrap_or_default();
        let mut token = std::env::var("LIBFW_TOKEN").unwrap_or_default();
        let mut config = ClientConfig::default();
        let mut auto_tune = false;
        let mut quiet = false;
        let mut positional: Vec<String> = Vec::new();

        let mut i = 0usize;
        while i < args.len() {
            match args[i].as_str() {
                "--url" => base_url = value(&args, &mut i)?,
                "--token" => token = value(&args, &mut i)?,
                "--concurrency" => {
                    config.concurrency = value(&args, &mut i)?
                        .parse()
                        .map_err(|_| "bad --concurrency".to_string())?
                }
                "--window" => {
                    let n: usize = value(&args, &mut i)?
                        .parse()
                        .map_err(|_| "bad --window".to_string())?;
                    config.upload_window = n;
                    config.download_window = n;
                }
                "--chunk-size" => {
                    config.chunk_size = value(&args, &mut i)?
                        .parse()
                        .map_err(|_| "bad --chunk-size".to_string())?
                }
                "--max-retries" => {
                    config.max_retries = value(&args, &mut i)?
                        .parse()
                        .map_err(|_| "bad --max-retries".to_string())?
                }
                "--timeout-ms" => {
                    config.timeout_ms = value(&args, &mut i)?
                        .parse()
                        .map_err(|_| "bad --timeout-ms".to_string())?
                }
                "--compress-level" => config.compress_level = parse_level(&value(&args, &mut i)?)?,
                "--no-compress" => config.compress = false,
                "--auto-tune" => auto_tune = true,
                "--quiet" | "-q" => quiet = true,
                "--help" | "-h" => return Err(String::new()),
                other if other.starts_with('-') => return Err(format!("unknown flag `{other}`")),
                other => positional.push(other.to_string()),
            }
            i += 1;
        }

        if base_url.is_empty() {
            return Err("missing --url (or LIBFW_URL)".into());
        }
        if token.is_empty() {
            return Err("missing --token (or LIBFW_TOKEN)".into());
        }

        let mut positional = positional.into_iter();
        let name = positional.next().unwrap_or_default();
        let command = match name.as_str() {
            "ls" | "list" => Command::List(positional.next().unwrap_or_default()),
            "download" => Command::Download {
                remote: positional
                    .next()
                    .ok_or_else(|| "download requires <REMOTE> <DEST>".to_string())?,
                dest: PathBuf::from(
                    positional
                        .next()
                        .ok_or_else(|| "download requires <REMOTE> <DEST>".to_string())?,
                ),
            },
            "upload" => {
                let local = PathBuf::from(
                    positional
                        .next()
                        .ok_or_else(|| "upload requires <LOCAL> [REMOTE]".to_string())?,
                );
                let remote = positional.next().unwrap_or_else(|| {
                    local
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| "upload".to_string())
                });
                Command::Upload { local, remote }
            }
            "capabilities" | "caps" => Command::Capabilities,
            "" => return Err("missing command".into()),
            other => return Err(format!("unknown command `{other}`")),
        };

        Ok(Options {
            base_url,
            token,
            command,
            config,
            quiet,
            auto_tune,
        })
    }
}

/// Consume the next argument as a flag value.
fn value(args: &[String], i: &mut usize) -> Result<String, String> {
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| format!("missing value for `{}`", args[*i - 1]))
}

fn parse_level(s: &str) -> Result<CompressLevel, String> {
    match s.to_ascii_lowercase().as_str() {
        "auto" => Ok(CompressLevel::Auto),
        "fast" => Ok(CompressLevel::Fast),
        "balanced" => Ok(CompressLevel::Balanced),
        "max" => Ok(CompressLevel::Max),
        other => other
            .parse::<i32>()
            .map(CompressLevel::Fixed)
            .map_err(|_| format!("bad --compress-level `{s}`")),
    }
}
