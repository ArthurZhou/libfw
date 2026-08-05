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

// Download a whole folder (showDirectoryPicker, preserves structure)
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

- `downloadFolder(token, dirPath?)` — the engine lists the folder on the
  server (recursively), then downloads each file with `Range`/`If-Range`
  resume, decompresses the zrip stream, and pushes `Uint8Array` chunks to
  the SDK, which writes them with `fileHandle.createWritable()`.
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
index.d.ts            TypeScript types
dist/libfw-client.umd.js   UMD bundle (after build:umd)
```

## API

- `new LibfwClient(options?)`
- `downloadFolder(token, dirPath?) → Promise<number>`
- `upload(token, files?) → Promise<number>`
- `pause()`, `resume()`, `cancel()`
- `state()`, `progress()`, `doneBytes()`, `totalBytes()`
- Errors: every rejection is a `LibfwError` with a stable `code`.

See `index.d.ts` for the full type surface.
