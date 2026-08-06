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

- **Resumable**: server answers `206 Partial Content` with `Content-Range` /
  `ETag`; supports `If-Range`, `If-None-Match` (→ `304`), `416` on
  unsatisfiable ranges and `412` on stale upload offsets. The client persists
  `{ etag, offset, size }` in IndexedDB and re-validates on every retry.
- **Streaming & constant memory**: both sides use a 64 KiB sliding window;
  the server writes uploads to a temp file and atomically renames on commit.
- **Compression**: `zrip` streaming compressor/decompressor, negotiated via
  `x-libfw-compress` / `Accept-Encoding: zrip`.
- **Fine-grained auth**: `Authorization: Bearer <token>` → verified claims →
  path-prefix + read/write permission validation (`401`/`403`). libfw never
  issues tokens.
- **Pluggable storage**: implement `StorageBackend` to target the local
  filesystem (shipped `FsStorage`), object storage, etc.

## Quick start (server)

```bash
# axum example (storage root `data`, port 8080)
cargo run -p axum-server -- data 8080

# or actix-web (port 8081)
cargo run -p libfw-actix-server -- data 8081
```

The dev servers accept the token `dev-token`:

```bash
# upload (streaming, with resume offset)
curl -X POST -H "Authorization: Bearer dev-token" \
     -H 'x-libfw-file-meta: {"path":"dir/a.txt","size":11}' \
     --data-binary "hello world" \
     http://127.0.0.1:8080/file/dir/a.txt

# download with a byte range
curl -H "Authorization: Bearer dev-token" -H "Range: bytes=0-4" \
     http://127.0.0.1:8080/file/dir/a.txt

# directory listing
curl -H "Authorization: Bearer dev-token" http://127.0.0.1:8080/dir/dir
```

### Embedding in axum

```rust,no_run
use std::sync::Arc;
use axum::Router;
use libfw_core::auth::{PathValidator, TokenVerifier, Validator};
use libfw_server::{router, ServerState};

let state = Arc::new(
    ServerState::builder()
        .storage(libfw_server::FsStorage::new("/srv/files"))
        .verifier(my_verifier)          // your TokenVerifier
        .validator(PathValidator::new())
        .build(),
);
let app: Router = router(state);        // /file/{*path}, /dir/{*path}
```

## Browser SDK

Build the WASM engine + web glue, then serve the demo:

```bash
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
# then open examples/web/index.html from any static server
```

```js
import { LibfwClient } from 'libfw-client';

const client = new LibfwClient({ baseUrl: '/api', concurrency: 4, compress: true });

await client.downloadFolder('your_token_here');              // showDirectoryPicker
await client.upload('your_token_here', fileInput.files);     // FileList upload
client.pause(); client.resume(); client.cancel();            // state machine control
```

The SDK owns WASM instantiation, the File System Access API, IndexedDB resume
state and `createWritable` writes. See [`sdk/README.md`](sdk/README.md).

## HTTP protocol

| Method | Route          | Purpose |
| ------ | -------------- | ------- |
| GET    | `/file/{*path}` | download (Range, ETag, If-Range, compression) |
| HEAD   | `/file/{*path}` | metadata only |
| POST   | `/file/{*path}` | streaming upload (headers below) |
| GET    | `/dir/{*path}`  | directory listing (JSON) |

Upload headers:

- `x-libfw-file-meta` — JSON `{ path, size, mtime, etag }` (required)
- `x-libfw-offset` — absent = create (`409` if exists), `0` = overwrite,
  `N > 0` = resume (mismatch → `412`)
- `x-libfw-compress` — `zrip` when the body is compressed

## Testing

```bash
cargo test --workspace            # 66 unit + integration tests (native)
wasm-pack test crates/libfw-client --node   # WASM-side tests (Node)
```

## License

MIT
