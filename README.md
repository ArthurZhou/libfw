# libfw

A high-performance, low-memory **streaming file & folder transfer library** for
Rust, built as a Cargo workspace. It features resumable transfers
(`Range`/`ETag`/`If-Range`), automatic zstd compression (via
[`zrip`](https://crates.io/crates/zrip)), fine-grained bearer-token
authorization, and a browser SDK backed by a WASM engine.

```
crates/
  libfw-core/     shared contracts: claims, validator, storage, compression, ranges
  libfw-server/   embeddable axum handlers: routing, auth, Range/ETag, streaming I/O
  libfw-client/   WASM engine (wasm-bindgen) + JS SDK in sdk/
examples/
  axum-server/    runnable axum file server (the libfw integration example)
  actix-server/   runnable actix-web file server (same contracts, different framework)
  web/            HTML demo page for the browser SDK
sdk/              libfw-client npm package (ESM + TS types + wasm)
```



## Highlights

- **HTTP transport (robust on bad networks)**: the browser engine drives
  **all** control commands (listing, metadata) and data flow over plain HTTP
  — no WebSocket. Downloads use **parallel byte-range `Range` GETs** (one
  independent connection per range, tus-style) and uploads use **concurrent
  chunked POSTs** into a shared per-session temp with a final size-verified
  commit. Independent parallel streams mean a lost packet stalls only that
  one stream (which retries just its own bytes) instead of blocking a whole
  multiplexed WebSocket connection — this is what keeps transfers moving on
  lossy/unstable links. A WebSocket endpoint (`/ws`) remains on the server
  for raw clients and older builds.
- **Resumable**: the client persists `{ etag, offset, size }` in IndexedDB and
  re-validates against the server (source of truth) on every retry; uploads
  resume from a shared per-session temp (BitTorrent-style, only the missing
  blocks are re-sent).
- **Streaming & constant memory**: both sides use a bounded block window and a
  64 KiB sliding read buffer; the server writes uploads to a temp file and
  atomically renames on commit.
- **Compression**: `zrip` per-block compression, negotiated per transfer.
- **Fine-grained auth**: `Authorization: Bearer <token>` → verified claims →
  path-prefix + read/write permission validation (`401`/`403`). libfw never
  issues tokens.
- **Pluggable storage**: implement `StorageBackend` to target the local
  filesystem (shipped `FsStorage`), object storage, etc.

## Table of contents

- [Quick start: run a server](#quick-start-run-a-server)
- [Browser demo](#browser-demo)
- [Embedding in a Rust app](#embedding-in-a-rust-app)
- [Authorization](#authorization)
- [Storage backends](#storage-backends)
- [Browser SDK guide](#browser-sdk-guide)
- [HTTP transport](#http-transport)
- [HTTP protocol](#http-protocol)
- [Building from source](#building-from-source)
- [Testing](#testing)
- [License](#license)

## Quick start: run a server

```bash
# axum example (storage root `data`, port 8080)
cargo run -p axum-server -- data 8080

# or actix-web (port 8081)
cargo run -p libfw-actix-server -- data 8081
```

The dev servers accept the token `dev-token`:

```bash
# upload (streaming, with resume offset)
# `x-libfw-file-meta` is base64(JSON) — the value below decodes to {"path":"dir/a.txt","size":11}
curl -X POST -H "Authorization: Bearer dev-token" \
     -H 'x-libfw-file-meta: eyJwYXRoIjoiZGlyL2EudHh0Iiwic2l6ZSI6MTF9' \
     --data-binary "hello world" \
     http://127.0.0.1:8080/file/dir/a.txt

# download with a byte range
curl -H "Authorization: Bearer dev-token" -H "Range: bytes=0-4" \
     http://127.0.0.1:8080/file/dir/a.txt

# directory listing
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:8080/dir/dir
```

## Browser demo

A one-click dev script starts the axum server, serves the demo page, and opens
the browser:

```bash
# Windows
dev-test.bat

# Linux / macOS
./dev-test.sh
```

It runs `cargo test --workspace`, boots the API on `:8080` and a static server
for `examples/web/` on `:5173`. Override with `PORT_API`, `PORT_WEB` and
`TOKEN` env vars. The demo page (`examples/web/index.html`) can also be opened
manually from any static server once the SDK is built (see
[Building the SDK](#building-the-sdk)).

> The demo relies on the **File System Access API** (`showDirectoryPicker`,
> `createWritable`) and therefore needs a Chromium-based browser.

## Embedding in a Rust app

### axum

`libfw-server` ships a ready-made `Router`. Build a `ServerState`, mount it,
and go:

```rust,no_run
use std::sync::Arc;
use axum::Router;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_server::{router, FsStorage, ServerState};

// 1. Your token verifier: parse & verify bearer tokens into claims.
#[derive(Clone)]
struct MyVerifier;
impl TokenVerifier for MyVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        Ok(TokenClaims {
            sub: token.to_string(),
            exp: None,
            permissions: vec![Permission::Read, Permission::Write],
            allowed_paths: vec!["/".to_string()],
        })
    }
}

// 2. Assemble the state and mount the router.
let state = Arc::new(
    ServerState::builder()
        .storage(FsStorage::new("/srv/files"))
        .verifier(MyVerifier)
        .validator(PathValidator::new())
        // optional tweaks:
        // .compression(CompressionFormat::Zrip)
        // .max_upload_size(100 * 1024 * 1024 * 1024)
        .build(),
);

let app: Router = router(state);        // /file/{*path}, /dir/{*path}
```

`ServerState::builder()` requires `storage`, `verifier` and `validator`
(panics if missing) and defaults compression to `zrip` and the upload cap to
100 GiB.

To host libfw under a path prefix (e.g. `/api`), `nest` it in your own router:

```rust,no_run
let app = Router::new()
    .nest("/api", router(state))        // → /api/file/{*path}, /api/dir/{*path}
    .route("/", get(|| async { "hello" }));
```

### actix-web

libfw's core contracts are framework-agnostic, so actix-web is supported too
(the runnable `examples/actix-server` shows a full implementation):

```bash
cargo run -p libfw-actix-server -- data 8081
```

The example reuses `libfw_core` (`TokenVerifier`, `PathValidator`,
`StorageBackend`, compression) plus `libfw_server` helpers
(`FsStorage`, `ServerState`, `parse_range_header`, `content_range_value`, …)
to implement the same `/file/{path}` and `/dir/{path}` routes.

## Authorization

The server flow is: extract `Authorization: Bearer <token>` → verify it into
claims → validate the requested `path` + `action`. libfw never issues tokens —
it only parses and validates.

### Token claims

```rust
pub struct TokenClaims {
    pub sub: String,                      // subject (user / client)
    pub exp: Option<i64>,                 // unix expiry, None = never
    pub permissions: Vec<Permission>,     // Read | Write
    pub allowed_paths: Vec<String>,       // path prefixes the token may access
}
```

### Token verifier

Implement `TokenVerifier::verify(&self, token) -> Result<TokenClaims, AuthError>`.
This is where you plug in a JWT library or an external validation service:

- empty/malformed token → `AuthError::MissingToken`
- unverifiable token → `AuthError::Invalid(msg)`
- expired token → `AuthError::Expired`
- no permission for path/action → `AuthError::Forbidden`

### Path validation

The bundled `PathValidator` (an implementation of the `Validator` trait)
allows a request when all of these hold:

1. the token is not expired (`exp`),
2. it carries the `Permission` required by the action (`Read` for downloads,
   `Write` for uploads),
3. the requested path matches one of `allowed_paths`.

Paths are compared on a **segment boundary**: `allowed_paths = ["/docs"]`
matches `/docs`, `/docs/a.txt` and `/docs/` but **not** `/docshop/x`. The
root prefix `"/"` (or `""`) grants access to the whole tree; an empty
`allowed_paths` list denies everything. Set `PathValidator { raw_prefix_match:
true }` to fall back to raw string-prefix matching.

Need different rules (group-based ACLs, regex, per-file permissions)? Implement
the `Validator` trait yourself and pass it to `.validator(..)`.

### HTTP status mapping

| AuthError | Status |
| --------- | ------ |
| `MissingToken`, `Invalid`, `Expired` | `401 Unauthorized` |
| `Forbidden` | `403 Forbidden` |

## Storage backends

### Filesystem (`FsStorage`)

`FsStorage::new(root)` serves files under a directory. Uploads are streamed
into a temp file and **atomically renamed** on commit, so an aborted upload
never leaves a partial target behind. Paths are normalized and validated
(`..`/absolute/NUL are rejected) to prevent traversal.

### Custom backends

Implement the `StorageBackend` trait to target object storage, S3, an
in-memory fixture, etc. — the rest of the server (range handling, ETag,
compression) stays identical:

```rust,no_run
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    async fn file_meta(&self, path: &str) -> Result<Option<FileMeta>, StorageError>;
    async fn read_stream(&self, path: &str, range: RangeSpec)
        -> Result<Box<dyn Read + Send>, StorageError>;
    async fn write_stream(&self, path: &str, mode: WriteMode)
        -> Result<Box<dyn UploadSink>, StorageError>;
    async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, StorageError>;
    async fn mkdir_all(&self, path: &str) -> Result<(), StorageError>;
    async fn remove(&self, path: &str) -> Result<(), StorageError>;
}
```

`write_stream` receives a `WriteMode`:

- `Create` — fail with `AlreadyExists` if present,
- `Overwrite` — create or truncate,
- `Resume { offset }` — continue at `offset`, fail if the target isn't exactly
  `offset` bytes yet.

The returned `UploadSink` exposes `write(buf)`, `commit()` (atomic finalize,
returns `FileMeta`) and `abort()` (discard temp data).

## Browser SDK guide

The SDK (`sdk/`) is a zero-config ESM wrapper around the WASM engine. It owns
WASM instantiation, the File System Access API, IndexedDB resume state and the
`createWritable` byte sink — you only ever touch the `LibfwClient` class and
its `Promise`-based methods. Full API docs: [`sdk/README.md`](sdk/README.md).

### Building the SDK

```bash
# 1. Compile the WASM engine + generate the web glue (requires wasm-pack)
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release

# 2. (optional) bundle a UMD build
npm --prefix sdk run build:umd
```

### Constructor options

```js
const client = new LibfwClient({
  baseUrl: '/api',            // where libfw-server routes are mounted
  concurrency: 4,             // max parallel file transfers (default 4)
  uploadWindow: 8,            // in-flight chunks per single file upload (default 8;
                              // raise to reduce upload stutter on high-latency links)
  downloadWindow: 4,          // in-flight byte-range GETs per single file download
                              // (default 4; tus-style parallel download, so one file's
                              // throughput isn't limited by a single connection's RTT)
  downloadChunkSize: 256 * 1024, // byte range size for parallel downloads (default 256 KiB)
  compress: true,             // negotiate zrip compression (default true)
  chunkSize: 2 * 1024 * 1024, // upload chunk size (default 2 MiB)
  maxRetries: 3,              // retries per chunk/file (default 3)
  baseRetryDelayMs: 500,      // initial exponential backoff (default 500)
  maxRetryDelayMs: 30000,     // backoff ceiling (default 30 s)
  timeoutMs: 60000,           // per-request timeout (default 60 s)
  onEvent: (e) => {},         // progress / lifecycle listener
});
```

### Downloading

```js
// Download the whole server folder (empty dirPath = root) into a local
// directory chosen via showDirectoryPicker(). Structure is preserved.
const bytes = await client.downloadFolder('your_token_here');
const bytes = await client.downloadFolder('your_token_here', '/docs');
```

Bytes are streamed from the server, decompressed, and written with
`createWritable({ type: 'write', position, data })`. Because writables open
with `keepExistingData: true`, an interrupted download resumes exactly where
it stopped (`Range`/`If-Range` revalidation, IndexedDB-backed offsets).

**tus-style parallel download** (default on): a large file is fetched as
`downloadWindow` concurrent `Range` GETs, so a single file's throughput is
bounded by bandwidth instead of one connection's `chunkSize / RTT` — the
same bandwidth-delay-product fill that `uploadWindow` provides for uploads.
The engine reorders in-flight chunks in memory (worst case ≈
`downloadWindow × downloadChunkSize` bytes) so the SDK still receives bytes
strictly in order (append-mode writes, no `.crswap` churn). Each chunk is
retried independently, so a transient failure re-fetches only the lost part;
on resume the client first asks the server via `HEAD` (authoritative size +
ETag) and re-validates the persisted offset, then fetches only the chunks
after it.

### Uploading

```js
// From a FileList / File[] / <input type="file">
await client.upload('your_token_here', fileInput.files);

// From a whole local folder (showDirectoryPicker, structure mirrored)
await client.upload('your_token_here');

// From a precomputed plan (you then drive readFile yourself)
await client.upload('your_token_here', [
  { path: 'dir/a.txt', size: 11, mtime: 1710000000 },
]);
```

Each file is sliced into fixed-size chunks, each chunk compressed into one
zstd frame and POSTed with an absolute `x-libfw-offset` into a shared
per-session temp file. Up to `uploadWindow` chunks of one file are kept in
flight concurrently (independent of the cross-file `concurrency`), so a
high-latency link stays saturated.

Uploads are **tus-style verify-then-complete**: the server is the source of
truth — the client probes the byte ranges the server actually persisted, and
re-sends *only* the still-missing blocks. After each batch it re-probes and
fills any holes that per-request retries could not confirm (e.g. a response
lost after the server already wrote the data), and a failed commit triggers a
fresh probe + refill instead of failing the task. A final `x-libfw-final`
request verifies the merged size then renames the temp into place. Interrupted
uploads leave a resumable session temp on the server, which the server
periodically garbage-collects once it is older than the session TTL.

### Controls and state machine

```js
client.pause();   // downloading/uploading → paused
client.resume();  // paused → resumed (state revalidated first)
client.cancel();  // cancel the active transfer → failed

client.state();       // 'idle' | 'downloading' | 'uploading' | 'paused'
                      // | 'completed' | 'failed'
client.progress();    // 0..1
client.doneBytes();   // bytes transferred so far
client.totalBytes();  // total bytes to transfer
```

### Progress events

`options.onEvent` receives `{ type, path?, done?, total? }`:

- `fileStart` — `{ type, path, done: 0, total: size }`
- `fileCompleted` — `{ type, path }`
- `progress` — `{ type, done, total }`

### Errors

Every rejection is a `LibfwError` with a stable `code`:

`unknown` · `wasm` · `abort` · `unsupported` · `path` · `storage` · `idb` ·
`http` · `network` · `decompress` · `compress` · `protocol` · `cancelled`

```js
try {
  await client.downloadFolder(token);
} catch (err) {
  console.error(err.code, err.message); // e.g. "http", "http 404 for `/file/x`"
}
```

### Browser support

Downloading/uploading folders requires the File System Access API
(`showDirectoryPicker`), so Chromium-based browsers only. `downloadFolder`
throws `LibfwError` with code `unsupported` elsewhere.

## HTTP transport

The browser SDK/WASM engine performs **all** communication (control commands
and data) over plain HTTP at the routes below — no WebSocket. The design
follows the tus/download-manager model: **independent parallel streams** per
range/chunk, so a lost packet stalls only that one stream (which retries just
its own bytes) instead of blocking a whole multiplexed WebSocket connection.

### Downloads (tus-style parallel `Range` GETs)

1. The client `HEAD`s the file to learn the authoritative `ETag` + size
   (the server is the source of truth) and validates the persisted resume
   offset against that `ETag`.
2. The remaining bytes are fetched as `downloadWindow` concurrent
   `Range` GETs (default 4 × 256 KiB). Each chunk is retried **independently**
   — a transient failure re-fetches only that chunk, never the whole file.
3. Chunks are reordered in the engine and pushed to the SDK **strictly in
   order**; `Range`/`If-Range`/`416` give natural resume against the server
   `ETag`. `downloadWindow = 1` falls back to a sequential single-connection
   stream.

### Uploads (concurrent chunked POSTs + session commit)

1. The client probes the server (`x-libfw-session-status`) for the byte
   ranges it already holds in a shared per-session temp, and re-sends **only
   the missing chunks**, concurrently (`uploadWindow` in flight, default 8).
2. Each chunk carries its **absolute** `x-libfw-offset` (positional write, so
   chunks may arrive out of order) and a `201` response is its ack.
3. A single `x-libfw-final` commit validates the merged size against
   `meta.size` and atomically renames the temp into place. A rejected commit
   triggers a re-probe + refill, so a lost response that nevertheless landed
   server-side is never re-sent.

Both sides stay resumable: downloads by `{etag, offset}` and uploads via the
server's per-session temp (BitTorrent-style, only the missing parts are
re-transmitted).

### HTTP/3 & QUIC

The library does **not** implement QUIC itself — it has no need to. The
browser engine uses the standard `fetch`/`ReadableStream` APIs, so when the
server (or an edge/CDN in front of it) negotiates **HTTP/3**, every parallel
`Range` GET and chunked POST automatically rides an independent QUIC stream
with no head-of-line blocking. That is the single most effective upgrade for
lossy, high-latency networks, and it requires no client change.

The bundled example servers (`axum-server`, `actix-server`) serve HTTP/1.1.
To get HTTP/3 end-to-end, front them with a QUIC-capable reverse proxy
(Cloudflare, Caddy, nginx ≥ 1.25 with `http3 on;`, …) or an HTTP/3 load
balancer; `libfw` itself stays transport-agnostic.

## HTTP protocol

The HTTP routes are the transport the browser SDK uses (see
[HTTP transport](#http-transport) above). A WebSocket endpoint remains for
raw clients and older builds.

### Routes

| Method | Route          | Purpose |
| ------ | -------------- | ------- |
| GET    | `/ws`           | optional WebSocket control (kept for back-compat) |
| GET    | `/file/{*path}` | download (Range, ETag, If-Range, compression) |
| HEAD   | `/file/{*path}` | metadata only |
| POST   | `/file/{*path}` | streaming upload (headers below) |
| GET    | `/dir/{*path}`  | directory listing (JSON) |

All routes require `Authorization: Bearer <token>`.

### Downloads

- Plain `GET` returns `200` with the full body.
- `Range: bytes=…` returns `206 Partial Content` with `Content-Range` and an
  `ETag`; unsatisfiable ranges return `416` with `Content-Range: bytes */size`.
- `If-Range`/`If-None-Match` are honored: a matching `If-None-Match` → `304
  Not Modified`; a stale `If-Range` → full `200` body.
- Compression: send `Accept-Encoding: zrip` (or `x-libfw-compress: zrip`) to
  receive a zrip-compressed stream.

### Uploads

- `x-libfw-file-meta` — base64 of JSON `{ path, size, mtime, etag }` (required; encodes non-Latin-1 paths safely)
- `x-libfw-offset` — absent = create (`409` if exists), `0` = overwrite,
  `N > 0` = resume (size mismatch → `412`)
- `x-libfw-compress` — `zrip` when the body is compressed
- `x-libfw-session` — concurrent session id (the SDK sends one for every
  upload). Each chunk carries its ABSOLUTE `x-libfw-offset` and is written
  positionally into a shared per-session temp, so chunks can be pipelined
  out of order; `x-libfw-session-status` probes the already-received byte
  ranges, and `x-libfw-final: 1` commits (size-verified rename). Absent on a
  request → legacy sequential per-request upload.
- `HEAD /file/{*path}` is the tus-style metadata probe: the client reads the
  authoritative `ETag` + `Content-Length` to plan parallel downloads and to
  validate the persisted resume offset.

### Directory listing

`GET /dir/{*path}` returns a JSON array of entries:

```json
[
  { "path": "dir/a.txt", "is_dir": false, "size": 11, "mtime": 1710000000 },
  { "path": "dir/sub",   "is_dir": true,  "size": 0,  "mtime": 1710000001 }
]
```

### Status code reference

| Status | Meaning |
| ------ | ------- |
| `200` | full download / upload committed |
| `201` | upload created |
| `206` | partial content (Range fulfilled) |
| `304` | `If-None-Match` matched |
| `401` | missing / malformed / expired token |
| `403` | valid token, insufficient rights for path/action |
| `409` | upload with create semantics but target exists |
| `412` | resume offset mismatch (client resets and re-uploads) |
| `416` | unsatisfiable range |

## Building from source

```bash
# full workspace (native targets)
cargo build --workspace

# WASM engine for the browser SDK
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release

# UMD bundle of the SDK (requires rollup)
npm --prefix sdk run build:umd
```

## Testing

```bash
cargo test --workspace            # unit + integration tests (native)
wasm-pack test crates/libfw-client --node   # WASM-side tests (Node)
```

## License

MIT
