# libfw axum example server

A complete, runnable libfw deployment: the full HTTP API plus an embedded web
UI for browsing, uploading and downloading files and folders with live
progress, pause/resume/cancel and a transfer log.

```bash
cargo run -p axum-server -- <storage-dir> [port]
# defaults: storage ./data, port 8080, token dev-token
```

Then open <http://127.0.0.1:8080/>.

The UI needs the WASM engine built once:

```bash
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
```

Without `sdk/pkg` the page still renders and explains what to run; the REST
API works regardless.

## Routes

| Method | Route | Auth | Purpose |
| --- | --- | --- | --- |
| `GET` | `/` | no | embedded web UI (`index.html`, `include_str!`-embedded) |
| `GET` | `/health` | no | service name, version, protocol, storage root |
| `GET` | `/sdk/*` | no | the repository's `sdk/` directory (dev convenience) |
| `GET`/`HEAD` | `/file/{*path}` | yes | download (`Range`, `ETag`, `If-Range`, zrip) |
| `POST` | `/file/{*path}` | yes | upload (sequential or session chunks) |
| `GET` | `/dir[/{*path}]` | yes | JSON directory listing |
| `GET` | `/capabilities` | **no** | capability advertisement for adaptive clients |

All libfw routes require `Authorization: Bearer dev-token` (except
`/capabilities`). See [docs/protocol.md](../../docs/protocol.md) for the full
wire contract.

## Configuration

| Argument / variable | Default | Meaning |
| --- | --- | --- |
| `<storage-dir>` (argv) | `./data` | storage root (created if missing) |
| `<port>` (argv) | `8080` | listen port on `127.0.0.1` |
| `LIBFW_PATH_KEY` | unset | 64 hex chars → enable `EncryptedPathCodec` (AES-256-GCM shadow paths) |
| `LIBFW_CLEANUP_INTERVAL` | `3600` | stale session-temp sweep period (`30s`, `15m`, `2h`, `1d`) |
| `LIBFW_SESSION_TTL` | `24h` | max age of an abandoned `.libfw-sess-*` temp |

```bash
# encrypted shadow paths
LIBFW_PATH_KEY=$(openssl rand -hex 32) cargo run -p axum-server -- data 8080

# faster dev cleanup
LIBFW_CLEANUP_INTERVAL=60s LIBFW_SESSION_TTL=10m cargo run -p axum-server -- data 8080
```

## Try the API

```bash
BASE=http://127.0.0.1:8080
TOKEN=dev-token

curl -s "$BASE/health"
curl -s "$BASE/capabilities"

META=$(printf '{"path":"dir/a.txt","size":11}' | base64 -w0)
curl -X POST "$BASE/file/dir/a.txt" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-libfw-file-meta: $META" \
  --data-binary "hello world"

curl -H "Authorization: Bearer $TOKEN" -H 'Range: bytes=0-4' "$BASE/file/dir/a.txt"
curl -H "Authorization: Bearer $TOKEN" "$BASE/dir/dir"
```

## Notes

- The web UI is embedded with `include_str!("../index.html")`, so **rebuild
  the server after editing `index.html`**. The SDK is *not* embedded: it is
  served from the repository's `sdk/` directory at `/sdk`, which is why the
  page can pick up a freshly rebuilt WASM engine without recompiling.
- The server cannot be rebuilt while it is running on Windows (the executable
  is locked).
- The dev verifier accepts the literal token `dev-token` and grants full
  read/write. Replace it before exposing the server — see
  [docs/authorization.md](../../docs/authorization.md).
- CORS is permissive and `Cache-Control: no-store` is applied, both for
  development convenience; tighten them for production.
- Session temps are swept in the background, so a client that vanishes
  mid-upload does not leak temporary files forever.
