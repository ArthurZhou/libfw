# Native Rust client

`libfw-client` is one crate with two faces:

- **Native** (`native::NativeClient`) — async `tokio` + `reqwest`, for
  binaries, services and CLIs on any non-`wasm32` target.
- **Browser** (`LibfwClient` via `wasm-bindgen`) — the engine behind
  `sdk/`. See [the SDK guide](../sdk/README.md).

Both speak the identical wire protocol and share `ClientConfig` and the
adaptive tuning engine. This guide covers the native side.

- [Install](#install)
- [Quick start](#quick-start)
- [API reference](#api-reference)
- [Configuration](#configuration)
- [Events](#events)
- [Resume](#resume)
- [Cancellation](#cancellation)
- [Adaptive tuning](#adaptive-tuning)
- [Errors](#errors)
- [Concurrency and sharing](#concurrency-and-sharing)
- [Example CLI](#example-cli)

## Install

```toml
[dependencies]
libfw-client = "0.4"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

`reqwest` (with `rustls-tls-native-roots`, HTTP/2) and `tokio` are pulled in
only for non-`wasm32` targets, so the same manifest still builds the WASM
engine with `wasm-pack`.

## Quick start

```rust,no_run
use libfw_client::{ClientConfig, native::NativeClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig {
        auto_tune: true,
        ..ClientConfig::default()
    };
    let client = NativeClient::new("http://127.0.0.1:8080", "dev-token", config);

    for entry in client.list("").await? {
        println!("{} {} {}", if entry.is_dir { "d" } else { "f" }, entry.size, entry.path);
    }

    let written = client.download_file("docs/plan.pdf", "./plan.pdf").await?;
    println!("wrote {written} bytes (0 = already complete)");

    client.upload_file("./plan.pdf".as_ref(), "archive/plan.pdf").await?;
    Ok(())
}
```

## API reference

### Construction

| Method | Description |
| --- | --- |
| `NativeClient::new(base_url, token, ClientConfig)` | client with an explicit config |
| `NativeClient::with_config(NativeConfig)` | full config including an event sink |
| `client.config() -> &ClientConfig` | the knobs in use |
| `client.capabilities() -> Option<Capabilities>` | last `/capabilities` loaded by the tuning engine |
| `client.fetch_capabilities() -> Result<Capabilities>` | network call to `GET /capabilities` |
| `client.tune_status() -> Option<NativeTuneStatus>` | `{ phase, params, stats, caps_hash }`, `None` when tuning is off |

### Files and folders

| Method | Returns | Notes |
| --- | --- | --- |
| `list(dir: &str)` | `Vec<RemoteFile>` | immediate children; `""` = root |
| `stat(remote)` | `RemoteStat { size, etag }` | a directory answers `404` |
| `download_file(remote, dest)` | bytes written this run | resumes; `0` when already complete |
| `download_folder(dir, dest_dir)` | bytes written | preserves the virtual structure under `dest_dir` |
| `upload_file(local: &Path, remote)` | payload bytes sent | resumes from the server's received ranges |
| `upload_files(Vec<(PathBuf, String)>)` | bytes sent | `concurrency` files in parallel |
| `upload_folder(local_dir, remote_dir)` | bytes sent | mirrors the tree; symlinks are skipped |

`download_folder` and `upload_folder` keep the layout: downloading `tree`
into `./out` writes `./out/tree/...`; uploading `./src` to `backup` writes
`backup/<relative path>`.

### Retry and resume internals

- Downloads `HEAD` the file first (authoritative ETag + size), then fetch the
  remaining bytes as bounded ranges. Anything above 512 KiB is fetched as
  chunks even at window 1, and the loop re-reads the tuned window / chunk
  size before every batch — a mid-file ramp applies to the file in flight.
- A `200`/`416` response while resuming means the remote changed or shrank;
  the client logs it and restarts from byte 0.
- Uploads probe the session, send only missing gaps, then commit.

## Configuration

`ClientConfig` mirrors the SDK options one-for-one:

| Field | Default | Meaning |
| --- | --- | --- |
| `concurrency` | 4 | max files transferred in parallel |
| `upload_window` | 8 | in-flight chunks per single-file upload |
| `download_window` | 4 | in-flight range GETs per single-file download |
| `compress` | `true` | negotiate zrip compression |
| `compress_level` | `CompressLevel::Balanced` | `Auto` / `Fast` / `Balanced` / `Max` / `Fixed(i32)` |
| `chunk_size` | 2 MiB | shared chunk size for uploads and download ranges |
| `max_retries` | 3 | retries per chunk/file |
| `base_retry_delay_ms` | 500 | initial exponential backoff |
| `max_retry_delay_ms` | 30 000 | backoff ceiling |
| `timeout_ms` | 60 000 | per-request stall timeout (not a whole-transfer deadline) |
| `auto_tune` | `false` | enable the adaptive ramp |
| `tune_ttl_ms` | 1 h | no effect natively (in-memory only, see below) |

```rust,no_run
use libfw_client::{ClientConfig, CompressLevel};

let config = ClientConfig {
    concurrency: 8,
    upload_window: 16,
    download_window: 8,
    compress_level: CompressLevel::Max,
    timeout_ms: 120_000,
    auto_tune: true,
    ..ClientConfig::default()
};
```

`NativeConfig` wraps that with the endpoint and an optional event sink:

```rust,no_run
use std::sync::Arc;
use libfw_client::{ClientConfig, native::{NativeClient, NativeConfig, NativeEvent}};

let client = NativeClient::with_config(
    NativeConfig::new("https://files.example.com", "token")
        .with_client(ClientConfig { auto_tune: true, ..ClientConfig::default() })
        .with_events(Arc::new(|event: NativeEvent| match event {
            NativeEvent::Progress { done, total } => eprintln!("{done}/{total}"),
            NativeEvent::Tuning(t) => eprintln!("tuning: {:?} {:?}", t.phase, t.params),
            _ => {}
        })),
);
```

`NativeConfig`'s `Debug` impl redacts the token.

## Events

`NativeEvent` is delivered through the sink passed to
`NativeConfig::with_events`:

| Variant | Fields |
| --- | --- |
| `FileStart` | `path`, `size` |
| `Progress` | `done`, `total` (cumulative for the current transfer) |
| `FileDone` | `path`, `bytes` |
| `Tuning(TuneEvent)` | `phase`, `params`, `stats` (only with `auto_tune`) |
| `Log(String)` | retries, resume decisions, restart notices |

The sink is `Arc<dyn Fn(NativeEvent) + Send + Sync>`, so it may be called from
any transfer task. Keep it fast and non-blocking (send to a channel if you
need to do IO).

## Resume

Download resume state is a JSON sidecar next to the target:

```
./plan.pdf
./plan.pdf.libfw-resume.json    { "etag": "\"1234-1710000000\"", "offset": 4194304, "size": 12345678 }
```

- The sidecar is written atomically (write temp + rename) at most every 4 MiB.
- On the next run the client `HEAD`s the file; if the ETag matches, it
  continues at `offset` with `If-Range`, otherwise it starts from 0.
- Deleting the sidecar forces a full re-download. Deleting the destination
  also starts from 0 (there is nothing to continue from).
- A completed download removes the sidecar.

Upload resume is server-side: the session id is the file's ETag (size +
mtime), so an interrupted upload resumes from the server's `.libfw-sess-*`
temp, and an edited file starts a new session. A **committed** upload drops
the sidecar, so re-uploading the same file re-sends it (positional writes stay
idempotent).

## Cancellation

There is no `pause()` / `cancel()` handle. Cancellation is structural — drop
the future, or bound it:

```rust,no_run
# use libfw_client::native::NativeClient;
# async fn f(client: &NativeClient) -> Result<(), Box<dyn std::error::Error>> {
use std::time::Duration;

let bytes = tokio::time::timeout(
    Duration::from_secs(30),
    client.download_file("big.bin", "./big.bin"),
)
.await??;

// or: tokio::select! { _ = shutdown.cancelled() => {}, r = transfer => { r?; } }
# Ok(())
# }
```

Both directions resume, so abandoning a transfer costs only the in-flight
blocks. Downloads may leave up to ~4 MiB unsaved since the last checkpoint;
uploads leave a resumable server temp.

## Adaptive tuning

With `auto_tune: true` the client fetches `/capabilities`, starts at the
advertised minimums, and ramps:

1. per-file window,
2. cross-file concurrency,

using 1-second EWMA RTT/throughput windows. The chunk size is **not**
ramped: it follows the measured throughput (~100 ms of it, rounded down to
64 KiB and clamped into the advertised range plus hard client caps). The zrip
level is a client policy from `compress_level`, never a ramp dimension.
Errors halve the parameters (`degraded`), which then hold for two stable
windows before settling.

State lifetime:

- **In memory**, for the lifetime of the client. A settle is reused by later
  transfers of the same client (a folder of files does not re-ramp per file)
  and dropped when a transfer fails.
- **Not persisted.** A new process re-measures the link. `tune_ttl_ms` has
  no effect here; the browser engine is the one that caches in
  `localStorage`.
- A server without `/capabilities` switches tuning off and uses the static
  configuration.

Read the live state:

```rust,no_run
# use libfw_client::native::NativeClient;
# fn f(client: &NativeClient) {
if let Some(status) = client.tune_status() {
    println!("{:?} {:?} {:?}", status.phase, status.params, status.stats);
}
# }
```

Very short transfers may end before a 1-second window closes, leaving
`ramping` at the advertised minimums. That is expected.

## Errors

Every fallible method returns `Result<T, LibfwError>`:

| Variant | Meaning |
| --- | --- |
| `Http { status, url }` | non-success HTTP status |
| `Network(String)` | transport failure |
| `Compress` / `Decompress` | codec failure |
| `Protocol(String)` | malformed peer data (bad JSON, unsafe path) |
| `Storage(String)` | local file IO failure |
| `Cancelled` | transfer cancelled |
| `Js(String)` | WASM-only, never produced natively |
| `Other(String)` | anything else |

`Http { status: 404 }` is also how `stat`/`list` report "no such file" and
how a pre-`/capabilities` server is detected.

## Concurrency and sharing

Every transfer method takes `&self`, so one client can be shared:

```rust,no_run
# use std::sync::Arc;
# use libfw_client::native::NativeClient;
# async fn f(client: NativeClient) -> Result<(), Box<dyn std::error::Error>> {
let client = Arc::new(client);
let a = { let c = client.clone(); tokio::spawn(async move { c.download_file("a.bin", "./a.bin").await }) };
let b = { let c = client.clone(); tokio::spawn(async move { c.download_file("b.bin", "./b.bin").await }) };
let _ = tokio::try_join!(a, b)?;
# Ok(())
# }
```

One client = one `reqwest` connection pool + one tuning engine. The pool is
sized from `concurrency`, so the same knob bounds total network parallelism.
`download_folder` / `upload_files` already parallelize internally with that
budget; use `Arc<NativeClient>` rather than one client per file to keep the
pool and the tuning state shared.

## Example CLI

`examples/rust-client` is a small runnable CLI (`ls`, `download`, `upload`,
`capabilities`). See [its README](../examples/rust-client/README.md).

```bash
cargo run -p axum-server -- dev-data 8080
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token \
    --auto-tune upload ./big.bin docs/big.bin
```
