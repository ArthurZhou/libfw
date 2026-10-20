# Server integration

libfw splits into three published crates plus runnable examples:

| Crate | Role | Depends on |
| --- | --- | --- |
| `libfw-core` | contracts: claims, verifier/validator traits, storage traits, compression, ranges, protocol constants | — |
| `libfw-server` | axum router + handlers + `FsStorage` | `libfw-core` |
| `libfw-client` | browser engine (WASM) **and** native Rust client | `libfw-core` |

`libfw-server` is thin: it wires `libfw-core` contracts to axum. Any other
framework can implement the same routes with the core helpers.

- [axum](#axum)
- [Other frameworks](#other-frameworks)
- [ServerState reference](#serverstate-reference)
- [Storage backends](#storage-backends)
- [Path translation (shadow paths)](#path-translation-shadow-paths)
- [Capabilities and limits](#capabilities-and-limits)
- [Session cleanup](#session-cleanup)
- [Deployment](#deployment)
- [Examples](#examples)

## axum

```toml
[dependencies]
libfw-server = "0.4"
axum = "0.8"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust,no_run
use std::sync::Arc;
use axum::Router;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_server::{router, FsStorage, ServerState};

#[derive(Clone)]
struct MyVerifier;

impl TokenVerifier for MyVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        // Replace with JWT / HMAC / introspection — see docs/authorization.md
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(
        ServerState::builder()
            .storage(FsStorage::new("/srv/files"))
            .verifier(MyVerifier)
            .validator(PathValidator::new())
            .build(),
    );

    let app: Router = router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
```

`router(state)` returns a fully built `Router` with:

```
GET|HEAD  /file/{*path}
POST      /file/{*path}
GET       /dir
GET       /dir/{*path}
GET       /capabilities
```

Mount it under a prefix with `Router::nest`:

```rust,no_run
# use axum::Router;
# fn build() -> Router { Router::new() }
let app = Router::new()
    .nest("/api", router(state))          // → /api/file/..., /api/dir, /api/capabilities
    .route("/", axum::routing::get(|| async { "hello" }));
```

The path handed to your `Validator` is libfw-relative (`/file/x.txt` arrives
as `x.txt`), so a prefix never appears in authorization decisions.

Add middleware freely — the router is an ordinary axum `Router`:

```rust,no_run
# use axum::Router;
# let app: Router = Router::new();
use tower_http::trace::TraceLayer;

let app = app.layer(TraceLayer::new_for_http());
```

### Using the auth extractor in your own handlers

`BearerClaims` is a public `FromRequestParts` extractor backed by the same
verifier, so custom routes share the authentication logic:

```rust,no_run
use axum::extract::State;
use axum::Json;
use libfw_server::BearerClaims;

async fn whoami(BearerClaims(claims): BearerClaims) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "sub": claims.sub }))
}
```

## Other frameworks

`libfw-core` is framework-agnostic. If your framework is not axum, construct
`ServerState` directly and call the same helpers `libfw-server` uses:

| Need | Helper |
| --- | --- |
| parse `Range` | `libfw_server::parse_range_header` → `ParsedRange` |
| format `Content-Range` | `libfw_server::content_range_value`, `content_range_none_value` |
| ETag comparison | `etag_matches_if_none_match`, `if_range_matches` |
| path validation | `libfw_server::validate_rel_path` |
| shadow path handling | `ServerState::resolve_client_path` / `expose_path` |
| compression | `libfw_core::compressor_with_level`, `decompressor_with_limit` |
| header names | `HEADER_FILE_META`, `HEADER_SESSION`, … re-exported by `libfw-server` |

The bundled `examples/actix-server` is a complete actix-web implementation
of the same routes. It intentionally omits `/capabilities`, so adaptive
clients talking to it fall back to their static configuration.

If you implement the routes yourself, these invariants are required for
clients to work correctly:

1. `HEAD /file/{path}` returns `ETag` + an uncompressed `Content-Length`.
2. `GET` honors `Range`, `If-Range` and `If-None-Match`, and answers `416`
   with `Content-Range: bytes */<size>`.
3. `POST` accepts base64 `x-libfw-file-meta`, writes positionally for
   session uploads, and only commits on `x-libfw-final: 1`.
4. A session probe (`x-libfw-session-status: 1`) returns
   `{"ranges": [[start,end], …]}` in uncompressed offsets.
5. Ranges refer to uncompressed file offsets even when the body is
   compressed.

## ServerState reference

`ServerState::builder()` requires `storage`, `verifier` and `validator`;
`build()` panics when one is missing.

| Builder method | Default | Purpose |
| --- | --- | --- |
| `.storage(impl StorageBackend)` | required | file content backend |
| `.verifier(impl TokenVerifier)` | required | token → claims |
| `.validator(impl Validator)` | required | claims + path + action → allow/deny |
| `.path_codec(impl PathCodec)` | `IdentityPathCodec` | real ↔ shadow paths |
| `.compression(CompressionFormat)` | `Zrip` | download compression |
| `.max_upload_size(u64)` | 100 GiB | enforced upload cap |
| `.limits(Limits)` | built-in defaults | advertised tuning ranges |
| `.zrip_levels(ZripLevels)` | `None` | advertise/accept a level range |
| `.cleanup_interval(Duration)` | 1 h | stale session-temp sweep period (`ZERO` disables) |
| `.session_ttl(Duration)` | 24 h | max age of an abandoned session temp |

Public fields mirror the builder, so a built `ServerState` can be inspected
and reused:

```rust,no_run
# use std::sync::Arc;
# use libfw_server::ServerState;
# fn f(state: &Arc<ServerState>) {
let caps = state.capabilities();
println!("advertised upload cap: {} bytes", caps.limits.max_upload_size);
# }
```

Methods:

- `authorize(claims, path, action)` — run the validator directly.
- `resolve_client_path(claims, shadow, action)` — decode + authorize a
  client path; returns the real path or `PathResolveError`
  (`Invalid`/`Codec` → `400`, `Auth` → `401`/`403`).
- `expose_path(real)` — encode a real path for a response.
- `capabilities()` — the `/capabilities` payload.
- `spawn_stale_session_cleanup()` — start the sweeper task.

## Storage backends

### `FsStorage`

`FsStorage::new(root)` serves a local directory and is the reference
implementation:

- Paths are normalized and traversal is rejected (`..`, absolute paths, NUL).
- Every path component is checked against symlinks asynchronously, so a
  symlink planted inside the root cannot redirect a read or write outside it.
- Uploads stream into a temp file and are atomically renamed on commit.
- Session uploads write positionally into a shared temp plus a `.blocks`
  range sidecar; the first 16 hex chars of `SHA-256(sub)` are embedded in the
  name for per-user isolation.
- Range reads (`.blocks` parsing, `received_ranges`) are serialized per
  target with an internal lock registry, so concurrent chunks of different
  files never block each other while writers of the same file take turns.

### Custom backends

Implement `StorageBackend` to serve S3, an object store, memory, or a
database:

```rust,no_run
use async_trait::async_trait;
use std::io::Read;
use libfw_core::metadata::FileMeta;
use libfw_core::{DirEntry, RangeSpec, StorageBackend, StorageError, UploadSink, WriteMode};

struct MyBackend;

#[async_trait]
impl StorageBackend for MyBackend {
    async fn file_meta(&self, path: &str) -> Result<Option<FileMeta>, StorageError> { todo!() }
    async fn read_stream(&self, path: &str, range: RangeSpec)
        -> Result<Box<dyn Read + Send>, StorageError> { todo!() }
    async fn write_stream(&self, path: &str, mode: WriteMode)
        -> Result<Box<dyn UploadSink>, StorageError> { todo!() }
    async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, StorageError> { todo!() }
    async fn mkdir_all(&self, path: &str) -> Result<(), StorageError> { todo!() }
    async fn remove(&self, path: &str) -> Result<(), StorageError> { todo!() }
}
```

`write_stream` receives a `WriteMode`:

| Mode | Contract |
| --- | --- |
| `Create` | fail with `AlreadyExists` if the target exists |
| `Overwrite` | create or truncate |
| `Resume { offset }` | continue at `offset`; fail if the target is not exactly `offset` bytes |

`UploadSink` has two levels of support. Implement the minimum and the
concurrent session path degrades gracefully:

| Method | Required | Default behavior |
| --- | --- | --- |
| `write` | yes | — |
| `write_at(offset, buf)` | recommended | falls back to append |
| `received_ranges()` | recommended | returns `[]` → resume degrades to a full re-send |
| `len` | yes | — |
| `commit()` | yes | — |
| `abort()` | yes | — |

`StorageBackend::write_stream_session(path, session, owner, mode)` lets a
backend opt into the positional session protocol; the default forwards to
`write_stream`. `cleanup_stale_sessions(max_age)` defaults to removing
nothing.

If your backend cannot expose received ranges, commits are validated by
length only (the strongest available invariant), and resumes re-send the
whole file. That is still correct, just less efficient.

## Path translation (shadow paths)

By default, clients see real storage paths. A `PathCodec` hides the layout:

| Codec | Shadow looks like | Note |
| --- | --- | --- |
| `IdentityPathCodec` | the real path | default, zero cost |
| `MountPathCodec` | readable alias, e.g. `home/alice/x` | stable, human-readable namespaces |
| `EncryptedPathCodec` | `v1.<base64url>` opaque | AES-256-GCM, feature `path-encrypt` |

```rust,no_run
use libfw_core::pathmap::{MountPathCodec, PathCodec};

let codec = MountPathCodec::new(vec![
    ("home/alice".to_string(), "data/vol-3/alice".to_string()),
]);

assert_eq!(codec.encode("data/vol-3/alice/report.pdf"), "home/alice/report.pdf");
assert_eq!(codec.decode("home/alice/report.pdf").unwrap(), "data/vol-3/alice/report.pdf");
```

```rust,no_run
# #[cfg(feature = "path-encrypt")]
# fn f(key_hex: &str) -> Result<(), Box<dyn std::error::Error>> {
use libfw_server::{EncryptedPathCodec, ServerState};

let codec = EncryptedPathCodec::from_hex(key_hex)?; // 64 hex chars = 32 bytes
# let _ = codec;
# Ok(())
# }
```

Behavior:

- **Inbound** — `resolve_client_path` shape-validates the shadow, decodes it,
  then authorizes the **real** path. Token semantics never change.
- **Outbound** — listings and upload responses encode real paths back to
  shadows (`expose_path`), so a listed shadow can be used verbatim.
- **Hierarchical composition** — `{dirShadow}/literal/child` works: the
  longest decodable segment prefix is decoded and the rest is appended, then
  authorized. A directory shadow therefore covers its whole subtree without
  minting a shadow per file.
- **Tampering** — a shadow that no prefix decodes fails with `400`.
- **Determinism** — GCM shadows use a random nonce per encode; use
  `MountPathCodec` when you need stable, comparable names.
- **Root** — the canonical root (`""`) maps to itself, which is what the
  `GET /dir` route passes.

## Capabilities and limits

The `/capabilities` payload is the contract adaptive clients negotiate
against. `ServerState::capabilities()` builds it from your builder settings:

- `formats` reflects the configured download compression
  (`Zrip` → `["identity","zrip"]`, `None` → `["identity"]`).
- `limits` comes from `.limits(...)` or the built-in defaults.
- `maxUploadSize` is clamped to `min(advertised, enforced)` — the
  advertisement can never overstate what the server accepts.
- `zrip_levels` comes from `.zrip_levels(...)`; with `None` the level header
  is ignored and downloads use `ZRIP_DEFAULT_LEVEL` (1). Setting it makes the
  server clamp requested levels and echo the level it actually used.

What the server does **not** do: it does not enforce the advertised windows,
concurrency or chunk size. Those are client-side decisions inside the
advertised bounds. Keep that in mind when you advertise aggressive values —
a client with `autoTune: true` will ramp to them.

The payload is public by design (it is fetched before authentication), so
never put tenant, user or secret data in it.

## Session cleanup

Abandoned uploads leave `.libfw-sess-*` temps and `.blocks` sidecars behind.
Sweep them:

```rust,no_run
# use std::sync::Arc;
# use libfw_server::ServerState;
# fn f(state: &Arc<ServerState>) {
// uses cleanup_interval / session_ttl from the builder
let _handle = state.spawn_stale_session_cleanup();
# }
```

- `cleanup_interval(Duration::ZERO)` disables the sweeper and returns an
  already-finished task.
- The sweeper calls `StorageBackend::cleanup_stale_sessions(ttl)`; the count
  it logs is a number of **sessions**, not files.
- Custom backends that keep no long-lived temps can leave the default
  implementation and simply report `0`.

## Deployment

**Reverse proxy.** Disable response buffering for `/file/*`, preserve
`Range` and `If-Range`, and allow long request bodies for `POST /file/*`.
Classic nginx settings:

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_request_buffering off;   # stream uploads instead of spooling
    proxy_buffering off;           # stream downloads / range bodies
    client_max_body_size 0;        # let libfw enforce max_upload_size
    proxy_http_version 1.1;
}
```

**HTTP/3.** The library is transport-agnostic. Front it with a QUIC-capable
proxy/load balancer and every parallel `Range` GET / chunk POST rides its own
QUIC stream with no head-of-line blocking.

**Timeouts.** Keep idle/read timeouts above the client's `timeoutMs` (default
60 s) plus the configured chunk size worth of transfer time; otherwise the
proxy kills healthy slow links.

**CORS.** Only needed when the browser SDK is served from a different origin
than the API. Both example servers allow permissive CORS for development;
restrict it in production and allow the `Authorization`, `Range`,
`If-Range`, `If-None-Match`, `x-libfw-*` headers.

**Observability.** `libfw-server` emits `tracing` events for upload session
cleanup and storage failures; add `TraceLayer` from `tower-http` for request
spans. Do not log `Authorization` headers or raw tokens.

**Security checklist.**

- [ ] Replace the example fixed-token verifier (see
      [authorization](./authorization.md)).
- [ ] Set `.max_upload_size(...)` to what your disk/quota can absorb.
- [ ] Keep the storage root outside the web root; `FsStorage` rejects
      traversal but the directory itself should not be served by anything
      else.
- [ ] Restrict CORS to your own origins.
- [ ] Consider a `PathCodec` if directory names are sensitive.
- [ ] Set `session_ttl` to match how long clients may legitimately resume.

## Examples

```bash
# full axum server with an embedded web UI at /
cargo run -p axum-server -- data 8080

# minimal actix-web integration (API only, no /capabilities)
cargo run -p libfw-actix-server -- data 8081

# native Rust client
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token ls
```

The axum example reads these environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `LIBFW_PATH_KEY` | unset | 64-hex-char key → enable `EncryptedPathCodec` |
| `LIBFW_CLEANUP_INTERVAL` | `3600` | sweep period (`30s`, `5m`, `2h`, `1d`) |
| `LIBFW_SESSION_TTL` | `24h` | max age of an abandoned session temp |

It also serves `GET /health` (service name, version, protocol, storage root)
and `GET /sdk/*` from the repository's `sdk/` directory, which is how the web
UI loads the WASM engine.
