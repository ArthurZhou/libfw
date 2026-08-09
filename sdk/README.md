# libfw-client SDK

The browser SDK for [libfw](../README.md): a zero-config wrapper around the
WASM engine, the File System Access API and IndexedDB.

## Usage

```js
import { LibfwClient } from 'libfw-client';

const client = new LibfwClient({
  baseUrl: '/api',      // where libfw-server routes are mounted
  concurrency: 4,       // max parallel file transfers
  compress: true,       // zrip streaming compression
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

- `downloadFolder(token, dirPath?)` / `downloadFile(token, filePath)` — the
  engine lists (for folders) and downloads each file with `Range`/`If-Range`
  resume, decompresses the zrip stream, and pushes `Uint8Array` chunks to the
  SDK. With the File System Access API the SDK streams them to disk via
  `fileHandle.createWritable()`; without it (or with `downloadMode: 'browser'`)
  the SDK buffers the chunks and saves the result through a traditional
  browser download — a single file as-is, a folder packed into a `.zip`.
- `upload(token, files?)` — the engine slices each file into fixed-size
  chunks, reads them via `readFile`, compresses each chunk into one zstd
  frame, and POSTs them with `x-libfw-offset` for server-side resume
  validation.
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
  - `downloadMode: 'auto' | 'fs' | 'browser'` (default `'auto'`) — `'fs'`
    streams downloads through the File System Access API; `'browser'` buffers
    and triggers a traditional browser download (folders become `.zip`);
    `'auto'` uses `'fs'` when the API exists and falls back to `'browser'`.
- `downloadFolder(token, dirPath?) → Promise<number>`
- `downloadFile(token, filePath) → Promise<number>`
- `upload(token, files?) → Promise<number>`
- `clearResumeStore(direction?) → Promise<number>`
- `pause()`, `resume()`, `cancel()`
- `state()`, `progress()`, `doneBytes()`, `totalBytes()`
- Errors: every rejection is a `LibfwError` with a stable `code`.

See `index.d.ts` for the full type surface.
