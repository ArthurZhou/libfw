# libfw Rust client example

A minimal **native Rust** client: no browser, no WASM, no npm — just
`tokio` + `reqwest` speaking the same wire protocol as the browser SDK. It
exists to show the three things you need when embedding the client in your own
program:

* building a `ClientConfig` (the same knobs as the SDK options),
* installing an event sink for progress and tuning updates,
* calling the async transfer methods.

Full client reference: [docs/native-client.md](../../docs/native-client.md).

## Run it

Start a libfw server first — the bundled axum example is the quickest:

```bash
# terminal 1 — server on :8080, token `dev-token`
cargo run -p axum-server -- dev-data 8080

# terminal 2 — the CLI
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token ls
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token \
    upload ./Cargo.toml uploads/Cargo.toml
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token \
    download uploads/Cargo.toml ./Cargo.toml.bak
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token capabilities
```

The base URL and token are read from `LIBFW_URL` / `LIBFW_TOKEN`, or passed
with `--url` / `--token`.

## Commands

| Command | What it does |
| --- | --- |
| `ls [DIR]` | list a remote directory (`GET /dir/...`) |
| `download <REMOTE> <DEST>` | download one file, or a whole folder tree (`HEAD` 404 means folder) |
| `upload <LOCAL> [REMOTE]` | upload one file or a folder tree; `REMOTE` defaults to the local name |
| `capabilities` | print `/capabilities` (the contract the tuning engine negotiates) |

`download` and `upload` auto-detect files vs folders. Both print the number of
bytes moved **by this run** — `0` means everything was already in place.

## Options

```
--url <URL>                  Server base URL            (env LIBFW_URL)
--token <TOKEN>              Bearer token               (env LIBFW_TOKEN)
--concurrency <N>            Max concurrent files       (default 4)
--window <N>                 Per-file window, both directions
                             (default 8 upload / 4 download when unset)
--chunk-size <BYTES>         Shared chunk size          (default 2 MiB)
--max-retries <N>            Retries per block          (default 3)
--timeout-ms <MS>            Stall timeout              (default 60000)
--compress-level <POLICY>    auto|fast|balanced|max|N   (default balanced)
--no-compress                Disable zrip compression
--auto-tune                  Adapt to the link via /capabilities
--quiet, -q                  No progress output
--help, -h                   Usage
```

`--window` sets `upload_window` and `download_window` to the same value;
without it the library defaults apply (8 and 4).

## What the client does for you

* **Resumable downloads** — `HEAD` gives the authoritative size + ETag, a JSON
  sidecar next to the destination records progress, and only the remaining
  bytes are fetched. Re-running a finished download is a no-op.
* **Resumable uploads** — the tus-style session protocol: probe which ranges
  the server already holds, then send only the gaps.
* **Parallel transfer** — `--window` concurrent range GETs per file and
  `--concurrency` files in flight, so one file's throughput is bounded by the
  link rather than a single connection's `chunkSize / RTT`. Anything over
  512 KiB is chunked even at window 1, and the tuned window/chunk size is
  re-read before each batch, so a mid-file ramp applies to the file in
  flight.
* **Compression** — zrip (zstd) frames both ways. `--compress-level auto`
  benchmarks a real sample of the first uploaded file within the advertised
  range and picks the best bytes-saved-per-CPU level.
* **Adaptive tuning** — `--auto-tune` reads the server's limits, probes with
  the minimum configuration, then TCP-style ramps the per-file window and
  cross-file concurrency. The chunk size follows measured throughput, it is
  not a ramp dimension. A settle is reused by later transfers in the same
  client; the native client persists nothing, so a new process re-measures
  the link (the browser engine caches in `localStorage`; see the SDK's
  `tuneTtlMs`).

## Embedding it

```rust
use libfw_client::{ClientConfig, native::{NativeClient, NativeConfig, NativeEvent}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig { auto_tune: true, ..ClientConfig::default() };
    let client = NativeClient::with_config(
        NativeConfig::new("https://files.example.com", "your-token")
            .with_client(config)
            .with_events(std::sync::Arc::new(|event: NativeEvent| {
                if let NativeEvent::Progress { done, total } = event {
                    eprintln!("{done}/{total}");
                }
            })),
    );

    let bytes = client.download_file("reports/2026.pdf", "./2026.pdf").await?;
    println!("downloaded {bytes} bytes (0 = already complete)");

    client.upload_file("./2026.pdf".as_ref(), "archive/2026.pdf").await?;
    Ok(())
}
```

## Notes

* Downloads preserve the **virtual** path structure under `DEST`, exactly like
  the browser SDK: `download tree ./out` writes `./out/tree/...`.
* A committed upload removes the server-side range sidecar, so uploading the
  same file again re-sends it (positional writes keep that idempotent). Resume
  applies to *interrupted* uploads.
* Very short transfers may finish before a 1-second measurement window closes;
  the tuning phase then stays `ramping` at the advertised minimums. That is
  expected, not an error.
* There is no `pause()` / `cancel()` handle: cancellation is structural — drop
  the transfer future (or wrap it in `tokio::select!` /
  `tokio::time::timeout`). Both directions resume, so abandoning a transfer
  costs only the blocks in flight.
* A directory answers `404` to `HEAD`; that is how `download` distinguishes a
  folder from a file.

Implementation: [`crates/libfw-client/src/native/`](../../crates/libfw-client/src/native/).
Integration tests: `crates/libfw-client/tests/native_client.rs` (starts a real
libfw server over TCP).
