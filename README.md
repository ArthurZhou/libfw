# libfw

Streaming file and folder transfer for Rust — a resumable HTTP transport with
parallel byte ranges, concurrent chunked uploads, per-block zstd compression
and adaptive tuning, plus official browser (WASM) and native Rust clients.

```text
            ┌──────────────── libfw-core (contracts) ─────────────────┐
            │ claims · verifier · validator · storage · ranges · codec │
            └───────────────┬───────────────────────┬─────────────────┘
                            │                       │
        ┌───────────────────▼──────────┐  ┌─────────▼──────────────────┐
        │ libfw-server (axum router)   │  │ libfw-client               │
        │ FsStorage · Range/ETag · zrip│  │ native (reqwest) + wasm    │
        └──────────────────────────────┘  └─────────┬──────────────────┘
                                                    │
                                          sdk/ (npm, ESM + WASM)
```

- **Resumable both ways.** Downloads continue from a validated ETag/offset;
  uploads resume from a shared server-side temp, re-sending only the gaps.
- **Parallel by request.** Downloads are N concurrent `Range` GETs, uploads
  are M concurrent chunk POSTs — a stalled request never blocks the rest.
- **Constant memory.** Bounded windows plus a 64 KiB read buffer on both
  sides; uploads commit with an atomic rename.
- **Compression on by default.** `zrip` (zstd) frames, negotiated per
  transfer, level clamped by the server and echoed back.
- **Pluggable authorization.** `TokenVerifier` + `Validator` traits; libfw
  never issues tokens.
- **Pluggable storage.** `StorageBackend` + `UploadSink`; `FsStorage` ships.
- **Optional path privacy.** `PathCodec` hides the real directory layout
  behind readable aliases or AES-256-GCM shadows.
- **Adaptive tuning.** A TCP-style ramp over real transfers, driven by a
  public `/capabilities` contract.

Browser support: Chromium-based browsersfor the File System Access API.
If no FSAPI support, in-browser client will download files in memory first, then zip it into one single file to download.

## Documentation

| Guide | English | 中文 |
| --- | --- | --- |
| Overview, quick start, feature map | this file | [README.zh-CN.md](./README.zh-CN.md) |
| **Authorization / custom verifiers** | [docs/authorization.md](./docs/authorization.md) | [docs/authorization.zh-CN.md](./docs/authorization.zh-CN.md) |
| Server integration | [docs/server-integration.md](./docs/server-integration.md) | [docs/server-integration.zh-CN.md](./docs/server-integration.zh-CN.md) |
| HTTP protocol reference | [docs/protocol.md](./docs/protocol.md) | [docs/protocol.zh-CN.md](./docs/protocol.zh-CN.md) |
| Native Rust client | [docs/native-client.md](./docs/native-client.md) | [docs/native-client.zh-CN.md](./docs/native-client.zh-CN.md) |
| Browser SDK | [sdk/README.md](./sdk/README.md) | — |
| Rust client CLI example | [examples/rust-client/README.md](./examples/rust-client/README.md) | [中文](./examples/rust-client/README.zh-CN.md) |
| axum demo server | [examples/axum-server/README.md](./examples/axum-server/README.md) | [中文](./examples/axum-server/README.zh-CN.md) |
| API reference | [docs.rs/libfw-core](https://docs.rs/libfw-core) · [libfw-server](https://docs.rs/libfw-server) · [libfw-client](https://docs.rs/libfw-client) | — |

The guides index lives in [docs/README.md](./docs/README.md).

## Repository layout

```text
crates/
  libfw-core/      claims, auth traits, storage traits, compression, ranges,
                   protocol constants, path codecs, capabilities
  libfw-server/    axum router + handlers, FsStorage, session temp handling
  libfw-client/    client engine: native (reqwest) + wasm (wasm-bindgen)
examples/
  axum-server/     full demo: API + embedded web UI at `/`
  actix-server/    minimal actix-web integration (API only)
  rust-client/     native CLI: ls / download / upload / capabilities
sdk/               libfw-client npm package (ESM + TS types + wasm)
docs/              guides (server, auth, protocol, native client)
tests/             Playwright e2e suites + Node unit tests
```

## Quick start

```bash
# 1. (optional, for the web UI) build the WASM engine once
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release

# 2. run the demo server: storage root `data`, port 8080, token `dev-token`
cargo run -p axum-server -- data 8080
```

Open <http://127.0.0.1:8080/> for the web UI (browse, upload, download files
and folders, live progress with pause/resume/cancel). The REST API works
without the UI build:

```bash
BASE=http://127.0.0.1:8080
TOKEN=dev-token

# capabilities (public, no token)
curl -s "$BASE/capabilities"

# upload a small file (meta = base64 of {"path","size"})
META=$(printf '{"path":"dir/a.txt","size":11}' | base64 -w0)
curl -X POST "$BASE/file/dir/a.txt" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-libfw-file-meta: $META" \
  --data-binary "hello world"

# range download
curl -H "Authorization: Bearer $TOKEN" -H 'Range: bytes=0-4' \
  "$BASE/file/dir/a.txt"

# listing
curl -H "Authorization: Bearer $TOKEN" "$BASE/dir/dir"
```

`dev-test.bat` (Windows) / `dev-test.sh` (Linux, macOS) run
`cargo test --workspace`, start the demo server and open the UI.

## Use it as a server

```rust
use std::sync::Arc;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_server::{router, FsStorage, ServerState};

#[derive(Clone)]
struct MyVerifier;

impl TokenVerifier for MyVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        // Replace with JWT / HMAC / introspection:
        // see docs/authorization.md
        if token.is_empty() {
            return Err(AuthError::MissingToken);
        }
        Ok(TokenClaims {
            sub: token.to_string(),
            exp: None,
            permissions: vec![Permission::Read, Permission::Write],
            allowed_paths: vec!["/".into()],
        })
    }
}

let state = Arc::new(
    ServerState::builder()
        .storage(FsStorage::new("/srv/files"))
        .verifier(MyVerifier)
        .validator(PathValidator::new())
        .build(),
);

// GET|HEAD|POST /file/{*path}, GET /dir[/{*path}], GET /capabilities
let app = router(state);
```

Mount it under a prefix with `.nest("/api", router(state))`. The builder also
takes `.path_codec(..)`, `.compression(..)`, `.max_upload_size(..)`,
`.limits(..)`, `.zrip_levels(..)`, `.cleanup_interval(..)` and
`.session_ttl(..)`. Full reference:
[docs/server-integration.md](./docs/server-integration.md).

Non-axum frameworks reuse `libfw-core` contracts plus the range/ETag helpers
exported by `libfw-server`; `examples/actix-server` is a complete actix-web
implementation.

## Authorization

The server extracts `Authorization: Bearer <token>`, asks your
`TokenVerifier` for `TokenClaims`, then asks a `Validator` whether those
claims may perform the action on the requested path.

```rust
pub struct TokenClaims {
    pub sub: String,                  // subject — also scopes upload sessions
    pub exp: Option<i64>,             // unix expiry, None = never
    pub permissions: Vec<Permission>, // Read | Write
    pub allowed_paths: Vec<String>,   // path prefixes on real storage paths
}
```

The bundled `PathValidator` requires an unexpired token, the permission the
action needs, and a path inside one of `allowed_paths`, matched on segment
boundaries (`/docs` matches `/docs/a.txt`, not `/docshop/x`). Install a
custom `Validator` for group ACLs, extension rules, read-only mounts, etc.

**The examples' fixed `dev-token` is a placeholder, not the API.** The API is
the `TokenVerifier` trait — implement it once for
[JWT, HMAC, JWKS or remote introspection](./docs/authorization.md). That guide
contains complete, copy-pasteable verifiers, custom validators, an
`/introspect` caching strategy, in-process integration tests and a shipping
checklist.

## Storage and shadow paths

`FsStorage` normalizes and validates paths (`..`, absolute paths, NUL and
symlink escapes are rejected), streams uploads into a temp file, and renames
atomically on commit. Session uploads write positionally into a shared temp
with a `.blocks` range sidecar, namespaced per token subject.

Implement `StorageBackend` to target S3, an object store or memory; the rest
of the server (ranges, ETag, compression) is unchanged. `UploadSink` has
graceful defaults: a backend that cannot report received ranges still works,
it just re-sends on resume.

If directory names are sensitive, install a `PathCodec`:
`MountPathCodec` for readable aliases, `EncryptedPathCodec` (feature
`path-encrypt`) for opaque AES-256-GCM shadows. Clients only ever see
shadows; authorization still runs on the decoded real path. Details:
[docs/server-integration.md](./docs/server-integration.md#path-translation-shadow-paths).

## Use it as a Rust client

The same crate that powers the browser SDK also works natively:

```toml
[dependencies]
libfw-client = "0.4"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use libfw_client::{ClientConfig, native::NativeClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig { auto_tune: true, ..ClientConfig::default() };
    let client = NativeClient::new("http://127.0.0.1:8080", "dev-token", config);

    for entry in client.list("").await? {
        println!("{} {}", if entry.is_dir { "dir " } else { "file" }, entry.path);
    }

    let bytes = client.download_file("docs/plan.pdf", "./plan.pdf").await?;
    println!("downloaded {bytes} bytes (0 = already complete)");

    client.upload_file("./plan.pdf".as_ref(), "archive/plan.pdf").await?;
    Ok(())
}
```

- Resumable downloads use a JSON sidecar (`<dest>.libfw-resume.json`) written
  atomically every ~4 MiB.
- Resumable uploads use the tus-style session protocol against the server.
- Tuning settles are shared in memory across transfers of the same client;
  nothing is persisted, so a new process re-measures the link.
- Cancellation is structural: drop the future or wrap it in
  `tokio::time::timeout`.
- One client = one connection pool + one tuning engine — share it as
  `Arc<NativeClient>`.

API reference: [docs/native-client.md](./docs/native-client.md). Runnable CLI:
[examples/rust-client](./examples/rust-client/README.md).

## Use it in the browser

```bash
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
npm --prefix sdk run build:umd   # optional UMD bundle
```

```js
import { LibfwClient } from 'libfw-client';

const client = new LibfwClient({
  baseUrl: '/',
  autoTune: true,
  onEvent: (e) => {
    if (e.type === 'progress') console.log(e.done, '/', e.total);
    if (e.type === 'tuning') console.log(e.phase, e.params, e.stats);
  },
});

await client.downloadFolder('your_token_here'); // pick a local folder
await client.upload('your_token_here', fileInput.files);
```

The SDK owns WASM instantiation, the File System Access API, IndexedDB resume
state, ZIP fallback for browsers without it, and download checkpointing
across a hard page refresh. Full option/API reference:
[sdk/README.md](./sdk/README.md).

## Protocol at a glance

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` / `HEAD` | `/file/{*path}` | download (`Range`, `ETag`, `If-Range`, zrip) / metadata |
| `POST` | `/file/{*path}` | upload chunk / session commit |
| `GET` | `/dir[/{*path}]` | JSON directory listing |
| `GET` | `/capabilities` | public tuning contract |

Key headers: `x-libfw-protocol`, `x-libfw-file-meta` (base64 JSON),
`x-libfw-offset`, `x-libfw-session`, `x-libfw-session-status`,
`x-libfw-final`, `x-libfw-compress`, `x-libfw-compress-level`.

Uploads are tus-style: probe which byte ranges the server already holds,
POST only the gaps (any order, positional writes), then commit with a size
and coverage check. Downloads are parallel `Range` GETs reordered in the
client, each retried independently.

Full details, including the session algorithm and status-code table:
[docs/protocol.md](./docs/protocol.md).

## Adaptive tuning

With `autoTune` / `auto_tune`, the client reads `/capabilities`, starts from
the advertised minimums and ramps the per-file window, then cross-file
concurrency, using 1-second EWMA RTT and throughput samples. The chunk size
follows the measured throughput (~100 ms of data, clamped into the advertised
range) rather than being a ramp dimension; the zrip level is a client policy.
Errors halve the parameters and require stable windows before settling.

The browser engine caches a settle in `localStorage` per origin and direction
(`tuneTtlMs`, default 1 hour, `0` disables). The native client keeps it in
memory only. The live state is available via `tuneStatus()` /
`tune_status()` and as `tuning` events.

## Building and docs

```bash
cargo build --workspace                       # native targets
cargo doc --no-deps --workspace --open        # rustdoc
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
npm --prefix sdk run build:umd                # optional UMD
```

All three published crates carry `[package.metadata.docs.rs]` with
`all-features = true`, and rustdoc builds warning-free, so
[docs.rs](https://docs.rs/libfw-core) renders the full API including the
feature-gated `EncryptedPathCodec`.

`libfw-client` is documented for a native target on docs.rs: the `native`
module is included, and the `wasm32`-only module's JavaScript face lives in
[sdk/README.md](./sdk/README.md) and `sdk/index.d.ts`.

## Testing

```bash
cargo test --workspace          # unit + integration (native), incl. a real
                                # server on an ephemeral port
npm run test:unit               # Node unit tests (ZIP writer, UTF-8 names)
```

Browser end-to-end suites live in `tests/e2e/` and run against a live demo
server with Playwright:

| Suite | Covers |
| --- | --- |
| `npm run e2e:matrix` | full upload/download matrix |
| `npm run e2e:resume` | abort-and-resume upload |
| `npm run e2e:resume-fs` | fs-mode download resume (OPFS-injected directory) |
| `npm run e2e:download` | parallel ranges, in-fetch progress, live stats |
| `npm run e2e:tune` | tuning panel and tuning events |
| `npm run e2e:tune-cache` | `localStorage` settle reuse, TTL expiry, `tuneTtlMs: 0` |
| `npm run e2e:refresh` | resume both directions across a hard page refresh |

```bash
# terminal 1
cargo run -p axum-server -- dev-data 8080
# terminal 2
npx playwright install chromium        # once
LIBFW_DATA=dev-data node tests/e2e/upload-matrix.cjs
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `BASE` | `http://127.0.0.1:8080` | demo server origin |
| `TOKEN` | `dev-token` | bearer token |
| `LIBFW_DATA` | OS temp dir | the **server's** storage root (resume suites inspect it) |
| `LIBFW_TMP` | OS temp dir | scratch directory for fixtures |
| `CHROME` / `BROWSER_CHANNEL` | Playwright's Chromium | explicit browser binary or channel |

## Versioning

- The wire protocol is versioned with `x-libfw-protocol: libfw/1`; mismatched
  clients fail fast with `426` instead of corrupting transfers.
- `/capabilities` advertises limits, not commands: clients clamp, the server
  does not enforce windows or chunk sizes, and `maxUploadSize` is always
  clamped down to what the server actually accepts.
- Feature flags: `libfw-core/path-encrypt` (re-exported as
  `libfw-server/path-encrypt`) enables `EncryptedPathCodec`.

## License

MIT
