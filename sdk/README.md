# libfw-client (browser SDK)

The official browser client for [libfw](../README.md). It wraps the WASM
engine, the File System Access API and IndexedDB behind one small ESM class:
you call `upload` / `downloadFolder` / `downloadFile` with a bearer token.

The same protocol engine is available natively for non-browser programs —
see [docs/native-client.md](../docs/native-client.md).

- [Install](#install)
- [Quick start](#quick-start)
- [Options](#options)
- [API](#api)
- [Events](#events)
- [Errors](#errors)
- [How transfers work](#how-transfers-work)
- [Resume and page refresh](#resume-and-page-refresh)
- [Download modes and ZIP fallback](#download-modes-and-zip-fallback)
- [Adaptive tuning](#adaptive-tuning)
- [Building the package](#building-the-package)
- [Browser support and security](#browser-support-and-security)

## Install

```bash
npm install libfw-client
```

Or use it from source in this repository:

```bash
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
```

The package ships the ESM entry (`index.js`), TypeScript types
(`index.d.ts`), the dependency-free ZIP writer (`zip.js`) and the
`wasm-pack` output under `pkg/`. It does not bundle or fetch anything else.

## Quick start

```js
import { LibfwClient } from 'libfw-client';

const client = new LibfwClient({
  baseUrl: '/',          // where libfw-server is mounted (same origin by default)
  concurrency: 4,        // parallel files
  uploadWindow: 8,       // in-flight chunks per file upload
  downloadWindow: 4,     // parallel range GETs per file download
  compress: true,
  autoTune: true,
  onEvent: (e) => {
    if (e.type === 'progress') bar.value = e.done / e.total;
    if (e.type === 'tuning') console.log(e.phase, e.params, e.stats);
  },
});

// upload <input type="file" multiple>
await client.upload(token, document.querySelector('input[type=file]').files);

// upload a whole folder (showDirectoryPicker)
await client.upload(token);

// download a whole folder into a picked local directory
await client.downloadFolder(token);

// download a single file
await client.downloadFile(token, 'docs/plan.pdf');
```

`downloadFolder` / `downloadFile` / the argument-less `upload` call a
directory picker, so they must run from a user gesture (a click handler).

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `baseUrl` | `''` | origin/prefix where libfw is mounted; `''` = same origin |
| `concurrency` | `4` | max files transferred in parallel |
| `uploadWindow` | `8` | in-flight chunks per single-file upload |
| `downloadWindow` | `4` | in-flight `Range` GETs per single-file download (`1` = sequential) |
| `compress` | `true` | request/send zrip (zstd) compression |
| `compressLevel` | `'balanced'` | `'auto'` \| `'fast'` \| `'balanced'` \| `'max'` \| number; `'auto'` benchmarks uploads |
| `chunkSize` | `2097152` (2 MiB) | shared chunk size for uploads and download ranges |
| `maxRetries` | `3` | retries per chunk/file |
| `baseRetryDelayMs` | `500` | initial exponential backoff |
| `maxRetryDelayMs` | `30000` | backoff ceiling |
| `timeoutMs` | `60000` | per-request stall timeout |
| `autoTune` | `false` | enable the adaptive ramp |
| `tuneTtlMs` | `3600000` (1 h) | how long a cached settle stays usable; `0` disables caching |
| `wasmUrl` | auto | explicit URL of `libfw_client_bg.wasm` |
| `downloadMode` | `'auto'` | `'auto'` \| `'fs'` \| `'browser'` |
| `maxFallbackBytes` | `536870912` (512 MiB) | memory cap for the `'browser'` fallback; `0` disables |
| `onEvent` | — | progress/lifecycle/tuning listener |
| `directoryHandle` | — | pre-selected handle (or resolver) instead of `showDirectoryPicker` |
| `resolveDisplayName` | — | map a server path to the on-disk / archive name |
| `resolveUploadPath` | — | map an upload entry to its virtual server path |

Notes:

- `concurrency` is the cross-file fan-out. The engine sizes its shared
  in-flight request budget as `concurrency × max(uploadWindow,
  downloadWindow)`, so raising a window actually increases parallelism (it
  used to be capped by `concurrency` alone).
- `chunkSize` is used by both directions. The download path buffers in-flight
  chunks for reordering, so peak memory is roughly
  `downloadWindow × chunkSize`.
- `compressLevel` is a client *policy*, resolved against the server's
  advertised range. `'auto'` additionally micro-benchmarks the range on a
  real sample of the first uploaded file; downloads request the resolved
  level from the server.
- `tuneTtlMs` only matters with `autoTune`. It is ignored by the native
  client.

Example with a per-file virtual path:

```js
await client.upload(token, [
  { file, relPath: 'reports/2026/plan.pdf' },
]);
```

## API

### Transfers

| Method | Returns | Description |
| --- | --- | --- |
| `downloadFolder(token, dirPath?)` | bytes written | download a remote folder into a picked local folder (`''` = root) |
| `downloadFile(token, filePath, opts?)` | bytes written | download one file; `opts.fileName` overrides the leaf name |
| `upload(token, files?)` | bytes sent | `FileList`, `File[]`, `UploadFileEntry[]`, `UploadEntry[]`, or no argument = pick a folder |

### Controls and state

| Method | Description |
| --- | --- |
| `pause()` | pause the active transfer |
| `resume()` | resume it (state is revalidated first) |
| `cancel()` | cancel it → state `failed` |
| `state()` | `idle` \| `downloading` \| `uploading` \| `paused` \| `completed` \| `failed` |
| `progress()` | `0..1` |
| `doneBytes()` | bytes transferred so far |
| `totalBytes()` | total bytes to transfer |
| `tuneStatus()` | `{ phase, params, stats, capsHash }` or `null` |
| `clearResumeStore(direction?)` | remove persisted resume records (`'upload'` / `'download'`), returns the count |

`tuneStatus()` shape:

```js
{
  phase: 'settled',                 // uninitialized | ramping | settled | degraded
  params: {
    concurrency: 4,
    uploadWindow: 8,
    downloadWindow: 4,
    chunkSize: 4194304,
    compressLevel: 1,               // resolved policy (what downloads request)
  },
  stats: { rttMs: 12.4, mbps: 87.3 },
  capsHash: 'sha256-…',
}
```

## Events

`onEvent` receives one of:

| Event | Fields | When |
| --- | --- | --- |
| `fileStart` | `path`, `done: 0`, `total: size` | a file starts |
| `fileCompleted` | `path` | a file finishes |
| `progress` | `done`, `total` | byte progress (frequent — keep the handler cheap) |
| `tuning` | `phase`, `params`, `stats` | tuning phase/window changes (only with `autoTune`) |

`progress` is emitted per received body slice, not per chunk, so a large range
GET still animates. Downloads also emit monotonically increasing totals
across files.

## Errors

Every rejection is a `LibfwError` with a stable `code`:

| Code | Meaning |
| --- | --- |
| `unknown` | unclassified |
| `wasm` | engine initialization/panic surfaced |
| `abort` | `AbortController` fired |
| `unsupported` | a required browser API is missing |
| `path` | invalid/unsafe path |
| `storage` | local file write / seek failure |
| `idb` | IndexedDB failure |
| `http` | non-success HTTP status (`http 404 for \`/file/x\``) |
| `network` | transport failure |
| `decompress` | bad compressed stream |
| `compress` | compression failure |
| `protocol` | malformed server payload |
| `cancelled` | user cancelled |
| `too-large` | would exceed `maxFallbackBytes` |

```js
try {
  await client.downloadFolder(token);
} catch (err) {
  console.error(err.code, err.message);
}
```

## How transfers work

- **Plain HTTP, no WebSocket.** Control commands (listing, metadata) and data
  share the same transport, so proxies and CDNs behave predictably.
- **Downloads** — `HEAD` for the authoritative size/ETag, then parallel
  `Range` GETs (`downloadWindow`), reordered in the engine and streamed to
  disk in order. Each range retries independently; `Range`/`If-Range`/`416`
  give resume against the server ETag. Files at or below 512 KiB stay on the
  sequential path.
- **Uploads** — slice each file, compress each chunk into zrip frames, probe
  the server's received ranges (`x-libfw-session-status`), POST only the
  missing chunks with their absolute offset (out of order), then commit with
  `x-libfw-final: 1`. A commit that fails re-probes and refills rather than
  failing the task.
- **State machine** — `idle → downloading/uploading → paused → resumed →
  completed/failed`. `pause()` stops scheduling new work; in-flight requests
  are allowed to finish.

## Resume and page refresh

- Resume records (`etag`, `offset`, `size`) live in IndexedDB, keyed per
  path and direction, and are revalidated on every retry. `clearResumeStore`
  wipes them.
- **Download resume requires a real partial file**: `downloadMode: 'fs'`
  (or an injected `directoryHandle`). A hard refresh runs no cleanup code,
  so `createWritable()` would otherwise discard the `.crswap` and the target
  would be 0 bytes. The SDK therefore **checkpoints**: whenever the engine
  reports a durable offset (about every 4 MiB) it closes the writable and
  reopens it with `keepExistingData` + `seek`, so a reload resumes from the
  last checkpoint. Worst-case loss on a hard kill is one checkpoint
  interval. The in-memory `'browser'` fallback always restarts from 0.
- **Upload resume** works because the session temp lives on the server and
  the session id is the file's ETag (size + mtime): a reloaded page probes
  the same session and sends only the missing chunks. A request whose
  connection dies mid-body keeps the blocks already received.

## Download modes and ZIP fallback

| `downloadMode` | Behavior |
| --- | --- |
| `'fs'` | stream to disk via `showDirectoryPicker` / `createWritable`; folders are recreated; resumable |
| `'browser'` | buffer in memory, then trigger a normal browser download; folders become a `.zip`; not resumable |
| `'auto'` (default) | `'fs'` when the API exists, otherwise `'browser'` |

In `'browser'` mode each file is pre-checked against `maxFallbackBytes`
(512 MiB by default) and rejected with `too-large` rather than risking an
OOM.

The ZIP writer is dependency-free and emits UTF-8 entry names with the
general-purpose bit 11 (EFS) set, so CJK and other non-ASCII folder names
extract correctly on Windows, macOS and Linux. Illegal filesystem characters
are sanitized; `..` and drive-like segments are rejected.

## Adaptive tuning

With `autoTune: true` the engine fetches `/capabilities`, starts at the
advertised minimums and ramps the per-file window, then cross-file
concurrency, from 1-second EWMA measurements. The chunk size follows the
measured throughput (~100 ms of data, clamped into the advertised range); the
zrip level is a policy, never ramped. Errors halve the parameters and require
two stable windows before settling.

A settle is reused by later transfers in the same client **and** cached in
`localStorage`:

- Key: `libfw.tune.v1.<upload|download>.<origin>`.
- Tagged with the `/capabilities` hash; a changed advertisement invalidates it.
- Expires `tuneTtlMs` after the ramp settled (not after the last reuse), so a
  link measured long ago is re-measured even if it is used constantly.
- Discarded on transfer failure.
- Storage failures (private mode, quota) are swallowed: caching is an
  optimization and never fails a transfer.

`tuneTtlMs: 0` disables the cache entirely, so every transfer re-ramps.

A server without `/capabilities` (for example the actix example, which omits
the route) disables tuning and the configured static values are used as-is.

## Building the package

```bash
# from the repo root
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release

# optional UMD bundle (rollup)
npm --prefix sdk run build:umd
```

Layout produced:

```text
sdk/
  index.js              ESM SDK
  index.d.ts            TypeScript types
  zip.js                dependency-free ZIP writer
  pkg/                  wasm-pack output (wasm + wasm-bindgen glue)
  dist/                 UMD bundle (after build:umd)
```

The demo server serves this directory at `/sdk/*`, so the page can import
`/sdk/index.js` directly during development.

## Browser support and security

- **Chromium-based browsers** for anything that needs
  `showDirectoryPicker` / `createWritable` (folder download/upload,
  resumable downloads). `downloadMode: 'auto'` falls back to a plain browser
  download elsewhere; explicit `'fs'` calls reject with `unsupported`.
- **Tokens** are passed per call, not stored by the SDK. Do not embed
  long-lived tokens in public pages — issue short-lived ones from your
  backend and let the page fetch them. libfw never issues tokens; see
  [docs/authorization.md](../docs/authorization.md).
- **`/capabilities` is public** and must not contain sensitive data.
- The SDK writes to IndexedDB (resume records) and `localStorage` (tuning
  cache); both are per-origin. `clearResumeStore()` removes resume records,
  and `tuneTtlMs: 0` disables the tuning cache.
