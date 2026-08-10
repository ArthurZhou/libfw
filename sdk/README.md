# libfw-client SDK

The browser SDK for [libfw](../README.md): a zero-config wrapper around the
WASM engine, the File System Access API and IndexedDB.

## Usage

```js
import { LibfwClient } from 'libfw-client';

const client = new LibfwClient({
  baseUrl: '/',         // server origin; the engine derives ws(s)://host/ws
                        // from it (or set wsUrl explicitly)
  concurrency: 4,       // max parallel file transfers (one WS connection each)
  uploadWindow: 8,      // in-flight blocks per single file upload (raise to
                        // reduce upload stutter on high-latency links)
  downloadWindow: 4,    // in-flight blocks per single file download (raise to
                        // reduce download stutter on high-latency links)
  compress: true,       // zrip per-block compression
  onEvent: (e) => console.log(e), // { type: 'progress', done, total }
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

- **One WebSocket per transfer** — the engine talks to the server over
  `ws(s)://…/ws` for **all** control commands (handshake, directory listing,
  metadata) and data. Upload and download use the **same** block protocol:
  the sender pipelines fixed-size blocks with **no per-block ack** (they may
  travel out of order), and the receiver **verifies every block in real time**
  (CRC32 + length + bounds), marks bad blocks (`NAK`) and asks the sender to
  re-add them to its transfer queue; a wave boundary reconciles until every
  block is verified. `downloadWindow`/`uploadWindow` bound the in-flight
  blocks per wave (raise them on high-latency links).
- `downloadFolder(token, dirPath?)` / `downloadFile(token, filePath)` — the
  engine lists (for folders) and downloads each file as a receiver, reorders
  out-of-order blocks in memory and pushes `Uint8Array` chunks to the SDK
  strictly in order (append-mode `createWritable()`, no `.crswap` churn).
  With the File System Access API the SDK streams them to disk via
  `fileHandle.createWritable()`; without it (or with `downloadMode: 'browser'`)
  the SDK buffers the chunks and saves the result through a traditional
  browser download — a single file as-is, a folder packed into a `.zip`.
- `upload(token, files?)` — the engine slices each file into fixed-size
  blocks, reads them via `readFile`, compresses each block into one zstd
  frame, and sends them over the WebSocket as the sender. Only the blocks the
  server still misses are sent (`READY.received` seeds resume), so a
  high-latency link stays saturated and interrupted uploads resume
  BitTorrent-style (only the broken/lost parts are re-transmitted).
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
  - `downloadWindow: number` (default `4`) — in-flight blocks per single file
    download (how many blocks the server pipelines per wave before a
    reconciliation round); raise it on high-latency links.
  - `downloadChunkSize: number` (default `262144`, 256 KiB) — block size for
    downloads; the engine reorders out-of-order blocks in memory (worst case
    ≈ `downloadWindow * downloadChunkSize` bytes) so the SDK still receives
    data in order.
  - `wsUrl: string` — explicit WebSocket endpoint (`wss://host/ws`); when
    omitted it is derived from `baseUrl` (`http://h:8080` → `ws://h:8080/ws`,
    same-origin when empty).
  - `downloadMode: 'auto' | 'fs' | 'browser'` (default `'auto'`) — `'fs'`
    streams downloads through the File System Access API; `'browser'` buffers
    and triggers a traditional browser download (folders become `.zip`);
    `'auto'` uses `'fs'` when the API exists and falls back to `'browser'`.
  - `maxFallbackBytes: number` (default `536870912`, 512 MiB) — memory cap
    for the in-memory `'browser'` fallback. File sizes are pre-checked
    against it before buffering; a download that would exceed it rejects
    with a `too-large` `LibfwError` instead of risking an OOM. `0` disables.
- `downloadFolder(token, dirPath?) → Promise<number>`
- `downloadFile(token, filePath) → Promise<number>`
- `upload(token, files?) → Promise<number>`
- `clearResumeStore(direction?) → Promise<number>`
- `pause()`, `resume()`, `cancel()`
- `state()`, `progress()`, `doneBytes()`, `totalBytes()`
- Errors: every rejection is a `LibfwError` with a stable `code`.

See `index.d.ts` for the full type surface.
