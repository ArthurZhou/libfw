# libfw Rust client example

A minimal **native Rust** client for libfw: no browser, no WASM, no npm — just
`tokio` + `reqwest` talking the same wire protocol as the browser SDK.

It exercises the three things you need when embedding the client in your own
program:

* building a `ClientConfig` (the same knobs as the SDK options),
* installing an event sink for progress / adaptive-tuning updates, and
* calling the async transfer methods.

## Run it

Start any libfw server first — the bundled axum example is the quickest:

```bash
# terminal 1 — server on :8080 with the dev token
cargo run -p axum-server -- dev-data 8080

# terminal 2 — this example (flags per call, or export LIBFW_URL/LIBFW_TOKEN)
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token ls
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token upload ./Cargo.toml uploads/Cargo.toml
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token download uploads/Cargo.toml ./Cargo.toml.bak
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token capabilities
```

The base URL and token are read from `LIBFW_URL` / `LIBFW_TOKEN`, or from
`--url` / `--token`.

## Commands

| Command | What it does |
| --- | --- |
| `ls [DIR]` | List a remote directory (`GET /dir/...`). |
| `download <REMOTE> <DEST>` | Download one file, or a whole folder tree (`stat` decides: a folder answers 404 to `HEAD`). |
| `upload <LOCAL> [REMOTE]` | Upload one file, or a whole folder tree (structure is mirrored). Returns the payload bytes actually sent — `0` when the server already held everything. |
| `capabilities` | Print the server's `/capabilities` payload (the contract the adaptive engine negotiates against). |

## Options

```
--url <URL>                  Server base URL            (env LIBFW_URL)
--token <TOKEN>              Bearer token               (env LIBFW_TOKEN)
--concurrency <N>            Max concurrent files       (default 4)
--window <N>                 Per-file in-flight window  (default 8 upload / 4 download)
--chunk-size <BYTES>         Shared chunk size          (default 2 MiB)
--max-retries <N>            Retries per block          (default 3)
--timeout-ms <MS>            Stall timeout              (default 60000)
--compress-level <POLICY>    auto|fast|balanced|max|N   (default balanced)
--no-compress                Disable zrip compression
--auto-tune                  Adapt to the link via /capabilities
--quiet, -q                  No progress output
```

## What the client does for you

* **Resumable downloads** — a `HEAD` gives the authoritative size + ETag, a
  JSON sidecar next to the destination remembers how far it got, and only the
  remaining byte range is fetched. Re-running a completed download is a no-op.
* **Resumable uploads** — the tus-style *session* protocol: the client probes
  the server for the ranges it already holds and re-sends only the gaps.
* **Parallel transfer** — `--window` concurrent `Range` GETs per file and
  `--concurrency` files in flight, so one file's throughput is bounded by the
  link rather than by a single connection's `chunkSize / RTT`. Anything over
  512 KiB is fetched as bounded ranges even at window 1, and the loop re-reads
  the tuned window / chunk size before every batch — a mid-file ramp applies to
  the download already in flight (one whole-file `206` could never be resumed,
  parallelised or tuned).
* **Compression** — zrip (zstd) frames on both directions; `--compress-level
  auto` measures a real sample of the first uploaded file against the
  advertised range and picks the best bytes-saved-vs-CPU trade-off.
* **Adaptive tuning** — with `--auto-tune` the engine reads the server's
  advertised limits, probes the link with a minimal configuration first, and
  then TCP-style ramps the per-file window and cross-file concurrency until it
  saturates. The chunk size is sized from the measured throughput (about
  100 ms of it, clamped into the advertised range), so a wider/faster link
  gets fewer, bigger requests. A settle is reused by later transfers **in the
  same client** (a folder of many files does not re-ramp per file); the native
  client persists nothing, so a new process re-measures the link from the
  advertised minimums (the browser engine caches its settle in `localStorage`,
  see the SDK's `tuneTtlMs`).

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

* Downloads preserve the **virtual** path structure below `DEST`, exactly like
  the browser SDK: `download tree ./out` writes `./out/tree/...`.
* A committed upload removes the server-side range sidecar, so uploading the
  same file again re-sends it (positional writes keep that idempotent). Resume
  applies to *interrupted* uploads.
* Very short transfers may finish before a 1-second measurement window closes,
  in which case the tuning phase stays `ramping` at the advertised minimums —
  that is expected, not an error.
* There is no `pause()`/`cancel()` handle: cancellation is structural, i.e.
  drop the transfer future (or wrap it in `tokio::select!` /
  `tokio::time::timeout`). Both directions resume, so abandoning a transfer
  only costs the blocks that were in flight.

See [`crates/libfw-client/src/native/`](../../crates/libfw-client/src/native/) for the
implementation and `crates/libfw-client/tests/native_client.rs` for an
integration suite that runs a real libfw server over TCP.
