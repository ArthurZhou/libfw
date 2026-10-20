# HTTP protocol reference

The complete wire contract between a libfw client (browser SDK or native
Rust) and a libfw server. Everything is plain HTTP; there is no WebSocket
and no custom framing beyond chunk boundaries.

- Version: `libfw/1` (`x-libfw-protocol`)
- Base content type: `application/octet-stream`
- Paths in URLs are **canonical relative paths** (`dir/sub/file.txt`);
  leading `/`, `..`, `.` segments and NUL bytes are rejected.
- Paths may be *shadow paths* when the server installs a `PathCodec`; see
  [server integration](./server-integration.md#path-translation-shadow-paths).

## Routes

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/file/{*path}` | download (full or `Range`) |
| `HEAD` | `/file/{*path}` | metadata only (size + ETag) |
| `POST` | `/file/{*path}` | upload chunk / commit |
| `GET` | `/dir` | root listing |
| `GET` | `/dir/{*path}` | directory listing (JSON) |
| `GET` | `/capabilities` | capability advertisement (**public**) |

All routes except `/capabilities` require
`Authorization: Bearer <token>`. A malformed or missing header is `401`, a
policy denial is `403`.

Every route passes through the protocol-version middleware: if the request
carries `x-libfw-protocol` with an incompatible value, the server answers
`426 Upgrade Required` instead of silently misbehaving. Requests without the
header are accepted (plain `curl` / scripts).

## Headers

| Header | Direction | Meaning |
| --- | --- | --- |
| `Authorization` | → | `Bearer <token>` (required everywhere except `/capabilities`) |
| `x-libfw-protocol` | → | `libfw/1` version handshake |
| `Range` | → | `bytes=start-end` or `bytes=-suffix` |
| `If-Range` | → | ETag; if it does not match, the server ignores `Range` and sends `200` + full body |
| `If-None-Match` | → | ETag; on match the server answers `304` |
| `Accept-Encoding` | → | `zrip` to request compressed downloads |
| `x-libfw-compress` | ↔ | `zrip` / `identity`; on a download response it reports the format actually used |
| `x-libfw-compress-level` | ↔ | requested zrip level; the response echoes the level actually used |
| `x-libfw-file-meta` | → | base64(JSON `FileMeta`) for uploads |
| `x-libfw-offset` | → | absolute byte offset of this upload chunk |
| `x-libfw-session` | → | per-file upload session id |
| `x-libfw-session-status` | → | `1` turns the request into a resume probe (no body is written) |
| `x-libfw-final` | → | `1` marks the commit request |
| `ETag` | ← | opaque version id (derived from size + mtime) |
| `Content-Range` | ← | `bytes start-end/total` on `206`, `bytes */total` on `416` |
| `Accept-Ranges` | ← | always `bytes` |

### `FileMeta` (in `x-libfw-file-meta`)

The header value is **base64 of UTF-8 JSON**, not raw JSON: HTTP header
values cannot carry non-Latin-1 bytes, which would break CJK file names.

```json
{ "path": "dir/a.txt", "size": 11, "mtime": 1710000000, "etag": "11-1710000000" }
```

`path` is the virtual path, `size` the exact byte length that must be
committed, `mtime` unix seconds, `etag` optional (the server recomputes it
on commit). Helper functions live in `libfw_core::metadata`:
`encode_file_meta_header` / `decode_file_meta_header`.

## Downloads

### Plain download

```http
GET /file/docs/plan.pdf HTTP/1.1
Authorization: Bearer <token>
x-libfw-protocol: libfw/1
```

```http
HTTP/1.1 200 OK
Accept-Ranges: bytes
Content-Type: application/octet-stream
Content-Length: 1048576
ETag: "1048576-1710000000"
x-libfw-compress: identity
```

When compression is negotiated, the body is a stream of zrip (zstd) frames
and `Content-Length` is omitted (the compressed size is not known in
advance); the response reports `x-libfw-compress: zrip` and the level used.

### Range download

```http
GET /file/docs/plan.pdf HTTP/1.1
Range: bytes=0-262143
If-Range: "1048576-1710000000"
Accept-Encoding: zrip
x-libfw-compress-level: 1
```

```http
HTTP/1.1 206 Partial Content
Content-Range: bytes 0-262143/1048576
ETag: "1048576-1710000000"
x-libfw-compress: zrip
x-libfw-compress-level: 1
```

Rules:

- `Range` is applied to the **uncompressed** file offsets; compression wraps
  each requested range independently.
- `If-Range` that does not match the current ETag → the server ignores the
  range and returns the full body with `200`.
- Unsatisfiable ranges (start ≥ size, `bytes=-0`) → `416` with
  `Content-Range: bytes */<total>` and no body. An unsatisfiable range means
  the client's resume offset is stale: restart from byte 0.
- `If-None-Match` matching → `304` with no body.
- A directory answers `404` on `HEAD`/`GET`; use the listing route for
  directories.

### Compression levels

The server advertises a level range (`zripLevels`) and clamps any requested
level into it. `x-libfw-compress: zrip` is honored only when the server was
built with `CompressionFormat::Zrip` (the default). The echoed
`x-libfw-compress` / `x-libfw-compress-level` headers are authoritative —
never assume the level you asked for was used.

## Uploads

There are two upload shapes. Both start from `POST /file/{*path}` with
`x-libfw-file-meta` and both answer `201 Created` with:

```json
{ "file": { "path": "dir/a.txt", "size": 11, "mtime": 1710000000, "etag": "11-1710000000" } }
```

The returned `path` is already translated back to a **shadow path** when a
`PathCodec` is installed.

### Sequential upload (no session header)

`x-libfw-offset` selects the write mode:

| `x-libfw-offset` | Mode | On conflict |
| --- | --- | --- |
| absent | create | `409` if the target exists |
| `0` | overwrite | truncates |
| `N > 0` | resume | `412` if the target is not exactly `N` bytes |

The body is written to a temp file and atomically renamed on success. On any
failure the temp is discarded (`abort`), so a failed sequential upload never
leaves a partial target.

### Session upload (concurrent chunks)

This is what both bundled clients use. One logical upload = N chunk requests
+ 1 commit request, all carrying the same `x-libfw-session`.

```http
POST /file/big.bin HTTP/1.1
Authorization: Bearer <token>
x-libfw-session: 67108864-1710000000
x-libfw-offset: 2097152
x-libfw-file-meta: <base64 {"path":"big.bin","size":67108864,...}>
x-libfw-compress: zrip
Content-Type: application/octet-stream

<compressed bytes for [2097152, 4194304)>
```

```http
HTTP/1.1 201 Created
{ "file": { ... } }
```

Algorithm:

1. **Probe** — before sending anything, the client may ask what the server
   already holds:

   ```http
   POST /file/big.bin HTTP/1.1
   x-libfw-session: 67108864-1710000000
   x-libfw-session-status: 1
   x-libfw-file-meta: <...>
   ```

   ```json
   { "ranges": [[0, 1048576], [2097152, 4194304]] }
   ```

   Ranges are `[start, end)` in **uncompressed** file offsets. The probe
   writes nothing and does not finalize; it only opens (creating if
   necessary) the shared temp.

2. **Data chunks** — each carries its **absolute** `x-libfw-offset` and is
   written positionally, so chunks may arrive out of order and be pipelined.
   Retrying a chunk is idempotent.

3. **Commit** — a final request with `x-libfw-final: 1` (normally an empty
   body) verifies that the received ranges cover the whole file and that the
   length equals `meta.size`, then atomically renames the temp.

   Without `x-libfw-final`, the file is never committed: data chunk requests
   only leave a resumable temp behind.

4. **Abandonment** — a client that disappears leaves `.libfw-sess-*` temps
   (plus `.blocks` range sidecars) on the server. The server's sweeper
   removes them once they are older than the session TTL (tus `Expiration`;
   default 24 h, sweep interval 1 h). Active uploads keep their temp fresh,
   so the sweeper never interrupts a live transfer.

Commit failures worth handling explicitly:

| Response | Meaning | Correct reaction |
| --- | --- | --- |
| `400` "some blocks are missing" | a middle chunk never landed | re-probe, re-send the gaps, commit again |
| `400` "commit yields N bytes…" | size mismatch | re-probe; if the server has more than expected the file changed |
| `413` | `meta.size` or a chunk exceeds the server cap | stop; use a smaller file or a server with a larger cap |
| `412` | resume offset mismatch | restart from byte 0 |
| `409` | create semantics on an existing file | switch to overwrite (`x-libfw-offset: 0`) |

A request-body error (page refresh, dropped connection) is treated as a
transport event: the partially received blocks are **kept**. Other errors
(bad compression, size limits, disk) abort the temp.

### Session isolation

The server embeds a tag derived from `TokenClaims.sub` (the first 16 hex
chars of its SHA-256) into the session temp filename. Two different
authenticated subjects can therefore never collide on — or read — each
other's in-progress upload, even if one sends a session id observed from the
other.

### Session ids

`x-libfw-session` should be stable for **one file version** so a reloaded
page can find the same session again. Both clients use the file's ETag
(size + mtime) with quotes stripped, e.g. `67108864-1710000000`. A file
edited between attempts gets a new id and starts a fresh session.

## Directory listing

```http
GET /dir/docs HTTP/1.1
Authorization: Bearer <token>
```

```json
[
  { "path": "docs/deep", "is_dir": true,  "size": 0,  "mtime": 1710000000 },
  { "path": "docs/a.txt", "is_dir": false, "size": 11, "mtime": 1710000001 }
]
```

`GET /dir` (no path) lists the root. Entries carry full virtual paths
(translated to shadows when a codec is installed), so a listed path can be
used verbatim in a follow-up request.

## Capabilities

`GET /capabilities` is public (no bearer token) and returns the tuning
contract every adaptive client negotiates against:

```json
{
  "protocol": "libfw/1",
  "compression": {
    "formats": ["identity", "zrip"],
    "zripLevels": { "default": 1, "max": 4, "min": -8 }
  },
  "limits": {
    "concurrency":    { "default": 4, "max": 4, "min": 1 },
    "uploadWindow":   { "default": 8, "max": 8, "min": 1 },
    "downloadWindow": { "default": 4, "max": 4, "min": 1 },
    "chunkSize":      { "default": 2097152, "max": 8388608, "min": 262144 },
    "maxUploadSize": 21474836480,
    "maxRetries": 3,
    "timeoutMs": 60000
  }
}
```

Notes:

- **Advisory, not enforced.** The server does not hard-limit the windows,
  concurrency or chunk size it advertises; the client is expected to clamp
  them itself. The one exception is `maxUploadSize`, which
  `ServerState::capabilities()` clamps down to the limit the server actually
  enforces, so it can never be overstated.
- **`caps_hash`.** `Capabilities::caps_hash()` is
  `sha256-<hex>` over the canonical serde JSON. Field order is the declared
  struct order, so reordering fields changes the hash. Clients tag cached
  tuning results with this hash and discard them when it changes.
- **Ranges are `min..=max` with a `default`.** An inverted range must be
  treated as "use the default" (both `IntRange::clamp` and
  `ZripLevels::clamp_level` do).
- **Served without authentication**, so the payload must never contain
  tenant or user data.

## Status codes

| Status | Meaning |
| --- | --- |
| `200` | full download / probe response |
| `201` | upload chunk accepted or file committed |
| `206` | partial content |
| `304` | `If-None-Match` matched |
| `400` | malformed headers, invalid compressed stream, incomplete commit, invalid shadow path |
| `401` | missing / malformed / invalid / expired token |
| `403` | valid token, insufficient permission for the path or action |
| `404` | file or directory not found |
| `409` | create upload on an existing file |
| `412` | resume offset mismatch |
| `413` | upload exceeds `maxUploadSize` |
| `416` | range not satisfiable |
| `426` | incompatible `x-libfw-protocol` value |
| `500` | storage / IO failure |

## curl walkthrough

```bash
BASE=http://127.0.0.1:8080
TOKEN=dev-token

# capabilities (public)
curl -s "$BASE/capabilities" | jq .

# upload one small file sequentially
META=$(printf '{"path":"dir/a.txt","size":11}' | base64 -w0)
curl -sS -X POST "$BASE/file/dir/a.txt" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-libfw-file-meta: $META" \
  --data-binary "hello world" | jq .

# download a byte range
curl -sS -D- -o chunk.bin \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Range: bytes=0-4' \
  "$BASE/file/dir/a.txt"

# list a directory
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/dir/dir" | jq .

# session upload: probe, one chunk, commit
SESSION=a.txt-11
META=$(printf '{"path":"dir/b.txt","size":11}' | base64 -w0)
curl -sS -X POST "$BASE/file/dir/b.txt" \
  -H "Authorization: Bearer $TOKEN" -H "x-libfw-session: $SESSION" \
  -H "x-libfw-session-status: 1" -H "x-libfw-file-meta: $META"
curl -sS -X POST "$BASE/file/dir/b.txt" \
  -H "Authorization: Bearer $TOKEN" -H "x-libfw-session: $SESSION" \
  -H "x-libfw-offset: 0" -H "x-libfw-file-meta: $META" \
  --data-binary "hello world"
curl -sS -X POST "$BASE/file/dir/b.txt" \
  -H "Authorization: Bearer $TOKEN" -H "x-libfw-session: $SESSION" \
  -H "x-libfw-final: 1" -H "x-libfw-file-meta: $META"
```

`base64 -w0` is GNU coreutils syntax; on macOS use `| base64` (no wrapping).

## Transport notes

- **Parallelism is per request.** A download is `N` independent `Range`
  GETs, an upload is `M` independent chunk POSTs. A stalled request does not
  block the others, which is the point on lossy links.
- **HTTP/3 / QUIC** works without client changes: `fetch`/`reqwest` will use
  h3 when the server negotiates it, so each parallel request rides its own
  QUIC stream. The bundled examples serve HTTP/1.1; front them with a
  QUIC-capable proxy or load balancer to get h3.
- **Compression is per body.** Requests and responses are compressed
  independently; zrip frames are ~64 KiB, so memory stays constant
  regardless of file size.
