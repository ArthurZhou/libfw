# libfw-client SDK

The browser SDK for [libfw](../README.md): a zero-config wrapper around the
WASM engine, the File System Access API and IndexedDB.

## Usage

```js
import { LibfwClient } from 'libfw-client';

const client = new LibfwClient({
  baseUrl: '/',         // server origin (same-origin when empty)
  concurrency: 4,       // max parallel file transfers (independent HTTP streams)
  uploadWindow: 8,      // in-flight chunks per single file upload (raise to
                        // reduce upload stutter on high-latency links)
  downloadWindow: 4,    // parallel byte-range GETs per single file download
                        // (raise to reduce download stutter on high-latency links)
  compress: true,       // zrip per-block compression
  autoTune: true,       // adaptive tuning: probes /capabilities and ramps
                        // concurrency/windows/chunk sizes from real stats
  onEvent: (e) => {
    if (e.type === 'progress') updateProgressBar(e.done, e.total);
    else if (e.type === 'tuning') renderTuning(e.phase, e.params, e.stats);
    // other types: fileStart, fileCompleted, ...
  },
});

// Download a whole folder. Uses showDirectoryPicker when the File System
// Access API is available; otherwise the folder is zipped and saved via a
// traditional browser download — no feature detection needed by the caller.
await client.downloadFolder('your_token_here');

// Upload a FileList
const input = document.querySelector('input[type=file]');
await client.upload('your_token_here', input.files);

// Or upload a whole folder
await client.upload('your_token_here');

// Controls
client.pause();
client.resume();
client.cancel();
```

## How it works

- **HTTP transport, not WebSocket** — the engine drives all control commands
  (directory listing, metadata) and data over plain HTTP. This is what keeps
  transfers robust on lossy/unstable links: each transfer uses **independent
  parallel HTTP streams**, so a lost packet stalls only that one stream
  (which retries just its own bytes) instead of blocking a whole multiplexed
  WebSocket connection.
- **Downloads** — `downloadFolder(token, dirPath?)` / `downloadFile(token,
  filePath)` list the tree (for folders) and fetch each large file as
  `downloadWindow` concurrent `Range` GETs (tus-style parallel transfer, one
  independent connection per range). Each chunk is retried independently
  (only the lost part is re-fetched); the engine reorders the chunks in
  memory and pushes `Uint8Array`s to the SDK **strictly in order**.
  `Range`/`If-Range`/`416` give natural resume against the server ETag (the
  server is the source of truth). With the File System Access API the SDK
  streams chunks to disk via `fileHandle.createWritable()`; without it (or
  with `downloadMode: 'browser'`) it buffers the chunks and saves the result
  through a traditional browser download — a single file as-is, a folder
  packed into a `.zip`.
- **Uploads** — `upload(token, files?)` slices each file into chunks, reads
  them via `readFile`, compresses each into one zstd frame and POSTs the
  missing chunks concurrently (out of order) with `x-libfw-offset` into a
  shared per-session temp on the server (positional writes). A final
  `x-libfw-final` commit validates the size and atomically renames the temp
  into place. Only the chunks the server still misses are re-sent
  (`x-libfw-session-status` probe seeds resume), so interrupted uploads
  resume BitTorrent-style (only the broken/lost parts are re-transmitted).
- Resume state (`etag`, `offset`, `size`) is persisted per path in
  IndexedDB and re-validated on every retry.
- Pause/resume/cancel drive the WASM state machine
  (`idle → downloading/uploading → paused → resumed → completed/failed`).

## Build

```bash
# 1. Compile the WASM engine + generate the web glue (requires wasm-pack)
npm run build:wasm

# 2. (optional) bundle a UMD build
npm run build:umd
```

The resulting package contains:

```
pkg/                  wasm-pack output (wasm + wasm-bindgen web glue)
index.js              ESM SDK
zip.js                dependency-free ZIP writer (browser-download fallback)
index.d.ts            TypeScript types
dist/libfw-client.umd.js   UMD bundle (after build:umd)
```

## API

- `new LibfwClient(options?)`
  - `downloadWindow: number` (default `4`) — in-flight byte-range window per
    single file download (how many concurrent `Range` GETs); raise it on
    high-latency links. `1` disables parallelism (sequential downloads).
  - `downloadChunkSize: number` (default `262144`, 256 KiB) — byte range size
    for parallel downloads; the engine reorders in-flight chunks in memory
    (worst case ≈ `downloadWindow * downloadChunkSize` bytes) so the SDK
    still receives data in order.
  - `downloadMode: 'auto' | 'fs' | 'browser'` (default `'auto'`) — `'fs'`
    streams downloads through the File System Access API; `'browser'` buffers
    and triggers a traditional browser download (folders become `.zip`);
    `'auto'` uses `'fs'` when the API exists and falls back to `'browser'`.
  - `maxFallbackBytes: number` (default `536870912`, 512 MiB) — memory cap
    for the in-memory `'browser'` fallback. File sizes are pre-checked
    against it before buffering; a download that would exceed it rejects
    with a `too-large` `LibfwError` instead of risking an OOM. `0` disables.
  - `autoTune: boolean` (default `false`) — enable the adaptive tuning
    engine. The engine probes the server's `/capabilities` limits and
    TCP-style ramps concurrency / windows / chunk sizes (and the zrip
    level) from the advertised minimums using real transfer stats. When
    disabled, the configured static values are used as-is.
  - `tuneTtlMs: number` (default `3600000`, 1 h) — how long a settled
    tuning result is reused for the same server origin before re-ramping.
- `downloadFolder(token, dirPath?) → Promise<number>`
- `downloadFile(token, filePath) → Promise<number>`
- `upload(token, files?) → Promise<number>`
- `clearResumeStore(direction?) → Promise<number>`
- `pause()`, `resume()`, `cancel()`
- `state()`, `progress()`, `doneBytes()`, `totalBytes()`
- `tuneStatus() → { phase, params, stats, capsHash } | null` — live
  adaptive-tuning status. `phase` is `uninitialized | ramping | settled |
  degraded`; `params` is `{ concurrency, uploadWindow, downloadWindow,
  chunkSize, downloadChunkSize, compressLevel }`; `stats` is
  `{ rttMs, mbps }` (EWMA request RTT, last-window throughput). `null`
  until the WASM engine is initialised.
- Events: with `autoTune` enabled, `onEvent` additionally receives
  `{ type: 'tuning', phase, params, stats }` on every phase transition /
  window evaluation.
- Errors: every rejection is a `LibfwError` with a stable `code`.

See `index.d.ts` for the full type surface.
