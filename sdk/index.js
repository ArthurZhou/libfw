/**
 * libfw-client — browser SDK.
 *
 * A thin, dependency-free wrapper around the libfw WASM engine that owns:
 *  - WASM instantiation (via the wasm-bindgen `web` glue),
 *  - the File System Access API (`showDirectoryPicker`, `getFileHandle`,
 *    `createWritable`) for streaming downloads,
 *  - a traditional browser-download fallback when the File System Access API
 *    is unavailable: single files download directly, folders are packed into
 *    a `.zip` archive (see `downloadMode`),
 *  - IndexedDB resume-state persistence,
 *  - converting engine callbacks (`onWriteChunk`, `getFileList`, …) into
 *    real file I/O.
 *
 * Every method returns a `Promise`; no `WebAssembly` or raw memory APIs are
 * ever exposed to the caller.
 *
 * @module libfw-client
 */

import init, { LibfwClient as WasmEngine } from './pkg/libfw_client.js';
import { createZip } from './zip.js';

/** Database / store names used by the IndexedDB resume-state layer. */
const IDB_NAME = 'libfw';
const IDB_STORE = 'resume';

/**
 * Uniform error type thrown by every SDK operation.
 *
 * @example
 * try {
 *   await client.downloadFolder('token');
 * } catch (err) {
 *   console.error(err.code, err.message); // e.g. "http", "http 404 for `/file/x`"
 * }
 */
export class LibfwError extends Error {
  /**
   * @param {string} message human-readable description
   * @param {string} [code] machine-readable category
   */
  constructor(message, code = 'unknown') {
    super(message);
    this.name = 'LibfwError';
    this.code = code;
  }
}

/** Map an arbitrary rejection to a {@link LibfwError}. */
function toLibfwError(err) {
  if (err instanceof LibfwError) return err;
  if (err && typeof err === 'object' && err.isLibfwError) {
    return new LibfwError(String(err.message || err), 'wasm');
  }
  if (err && typeof err === 'object' && err.message) {
    return new LibfwError(String(err.message), err.name === 'AbortError' ? 'abort' : 'unknown');
  }
  return new LibfwError(String(err));
}

/**
 * IndexedDB-backed resume state (per virtual path).
 */
const Idb = {
  /** @returns {Promise<IDBDatabase>} */
  open() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(IDB_NAME, 1);
      req.onupgradeneeded = () => {
        if (!req.result.objectStoreNames.contains(IDB_STORE)) {
          req.result.createObjectStore(IDB_STORE);
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(new LibfwError(`indexeddb open: ${req.error}`, 'idb'));
    });
  },

  /**
   * @param {string} path virtual file path
   * @returns {Promise<object|null>} `{ etag, offset, size }` or `null`
   */
  async loadState(path) {
    try {
      const db = await Idb.open();
      return await new Promise((resolve, reject) => {
        const tx = db.transaction(IDB_STORE, 'readonly');
        const req = tx.objectStore(IDB_STORE).get(path);
        req.onsuccess = () => resolve(req.result ?? null);
        req.onerror = () => reject(new LibfwError(`idb get: ${req.error}`, 'idb'));
      });
    } catch (err) {
      if (err instanceof LibfwError) return null; // resume is best-effort
      throw err;
    }
  },

  /**
   * @param {string} path virtual file path
   * @param {object} state `{ etag, offset, size }`
   * @returns {Promise<void>}
   */
  async saveState(path, state) {
    const db = await Idb.open();
    await new Promise((resolve, reject) => {
      const tx = db.transaction(IDB_STORE, 'readwrite');
      tx.objectStore(IDB_STORE).put(state, path);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(new LibfwError(`idb put: ${tx.error}`, 'idb'));
    });
  },

  /**
   * Delete every key whose `direction:` prefix matches (e.g. all
   * `download:*` keys) while leaving the other direction intact.
   * @param {string} direction `'upload'` | `'download'`
   * @returns {Promise<number>} number of records removed
   */
  async clearDirection(direction) {
    const db = await Idb.open();
    const prefix = `${direction}:`;
    const keys = await new Promise((resolve, reject) => {
      const tx = db.transaction(IDB_STORE, 'readonly');
      const req = tx.objectStore(IDB_STORE).getAllKeys();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(new LibfwError(`idb keys: ${req.error}`, 'idb'));
    });
    const matches = keys.filter((key) => String(key).startsWith(prefix));
    if (matches.length === 0) return 0;
    await new Promise((resolve, reject) => {
      const tx = db.transaction(IDB_STORE, 'readwrite');
      const store = tx.objectStore(IDB_STORE);
      for (const key of matches) store.delete(key);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(new LibfwError(`idb clear direction: ${tx.error}`, 'idb'));
    });
    return matches.length;
  },

  /**
   * Wipe the whole resume store.
   * @returns {Promise<void>}
   */
  async clear() {
    const db = await Idb.open();
    await new Promise((resolve, reject) => {
      const tx = db.transaction(IDB_STORE, 'readwrite');
      tx.objectStore(IDB_STORE).clear();
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(new LibfwError(`idb clear: ${tx.error}`, 'idb'));
    });
  },
};

/** Split a POSIX virtual path into segments. */
function splitPath(path) {
  return String(path).split('/').filter((s) => s.length > 0);
}

/**
 * The high-level libfw client.
 *
 * @example
 * import { LibfwClient } from 'libfw-client';
 *
 * const client = new LibfwClient({ baseUrl: '/api', concurrency: 4, compress: true });
 * await client.downloadFolder('your_token_here');
 * await client.upload('your_token_here', fileInput.files);
 */

/**
 * `<script src>` of the bundle that is currently evaluating this module.
 *
 * `document.currentScript` is only non-null during a classic script's
 * synchronous evaluation, so it is captured here once, at load time, rather
 * than read later from an async callback (where it would already be `null`).
 * In ESM this is `null` (modules never set `currentScript`), which is
 * correct — the ESM path resolves the `.wasm` via `import.meta.url` instead.
 * @type {string|null}
 */
const BUNDLE_SCRIPT_SRC =
  typeof document !== 'undefined' && document.currentScript && document.currentScript.src
    ? document.currentScript.src
    : null;

export class LibfwClient {
  /**
   * @param {object} [options]
   * @param {string} [options.baseUrl=''] base URL the server is served from
   *        (same-origin when empty). The engine drives all control commands
   *        and data transfer over plain HTTP (parallel `Range` downloads,
   *        tus-style chunked uploads) — no WebSocket is used.
   * @param {number} [options.concurrency=4] global cap on concurrent
   *        in-flight HTTP transfers (files and per-file chunk/range windows
   *        combined). Every chunk upload and range download takes a shared
   *        permit, so this is what actually bounds the engine's network
   *        parallelism — set it to your server/browser connection budget
   *        (e.g. 1 = strictly one transfer at a time).
   * @param {number} [options.uploadWindow=8] per-file scheduling window for a
   *        single file's upload: keeps that file's read/compress pipeline
   *        full on high-latency links, but the TOTAL in-flight requests never
   *        exceed `concurrency`
   * @param {number} [options.downloadWindow=4] in-flight byte-range window
   *        for a single file's download. Large files are fetched as
   *        `downloadWindow` concurrent `Range` GETs (tus-style parallel
   *        transfer), so a single file's throughput is bounded by bandwidth
   *        instead of one connection's `chunkSize / RTT` on high-latency
   *        links. `1` disables parallelism (sequential downloads).
   * @param {number} [options.downloadChunkSize=262144] byte range size for
   *        parallel downloads (256 KiB default). Smaller than the upload
   *        chunk on purpose: the engine reorders in-flight chunks in memory
   *        (worst case ≈ `downloadWindow * downloadChunkSize` bytes) so the
   *        SDK still receives data strictly in order.
   * @param {boolean} [options.compress=true] negotiate zrip compression
   * @param {number} [options.chunkSize=2097152] upload chunk size in bytes
   *        (each chunk is split into many small ~64 KiB compressed frames, so
   *        any value works; larger = fewer, bigger POST requests — bounded
   *        only by the server's upload limit and client memory)
   * @param {number} [options.maxRetries=3] retries per chunk/file before failing
   * @param {number} [options.baseRetryDelayMs=500] initial backoff (ms)
   * @param {number} [options.maxRetryDelayMs=30000] backoff ceiling (ms)
   * @param {number} [options.timeoutMs=60000] per-read timeout (ms)
   * @param {string} [options.wasmUrl] explicit URL of `libfw_client_bg.wasm`;
   *        when omitted it is resolved automatically for both ESM and
   *        classic-`<script>`/UMD consumers (see {@link LibfwClient#_wasmUrl})
   * @param {'auto'|'fs'|'browser'} [options.downloadMode='auto'] how downloads
   *        reach the user's disk: `'fs'` uses the File System Access API
   *        (`showDirectoryPicker`); `'browser'` buffers each file and triggers
   *        a traditional browser download (folders are packed into a `.zip`);
   *        `'auto'` picks `'fs'` when the API exists, otherwise `'browser'`.
   * @param {number} [options.maxFallbackBytes=536870912] memory cap (bytes)
   *        for the in-memory `'browser'` download fallback. Each file's size
   *        (and the cumulative buffered total) is pre-checked against it
   *        before any bytes are buffered; a download that would exceed it is
   *        rejected with a `too-large` error instead of risking an OOM.
   *        `0` disables the limit.
   * @param {(event: {type: string, done: number, total: number, path?: string, error?: string}) => void} [options.onEvent]
   *        optional progress/state listener
   */
  constructor(options = {}) {
    this._options = {
      baseUrl: '',
      concurrency: 4,
      uploadWindow: 8,
      downloadWindow: 4,
      downloadChunkSize: 256 * 1024,
      compress: true,
      chunkSize: 2 * 1024 * 1024,
      maxRetries: 3,
      baseRetryDelayMs: 500,
      maxRetryDelayMs: 30000,
      timeoutMs: 60000,
      wasmUrl: null,
      downloadMode: 'auto',
      maxFallbackBytes: 512 * 1024 * 1024,
      onEvent: null,
      ...options,
    };
    /** @type {WasmEngine|null} */
    this._engine = null;
    /** @type {Promise<void>|null} */
    this._initPromise = null;
    /** @type {FileSystemDirectoryHandle|null} selected download directory */
    this._dirHandle = null;
    /** @type {Map<string, FileSystemFileHandle>} path → file handle */
    this._fileHandles = new Map();
    /** @type {Map<string, FileSystemWritableFileStream>} path → open writable stream */
    this._writables = new Map();
    /** @type {Map<string, File>} path → File (upload) */
    this._uploadFiles = new Map();
    /** @type {Array<{path:string,size:number,mtime:number}>} upload plan */
    this._uploadPlan = [];
    /**
     * Active browser-download fallback state, or `null`.
     * @type {{isFolder:boolean, buffers:Map<string,Uint8Array[]>, order:string[], sizes:Map<string,number>}|null}
     */
    this._fallback = null;
  }

  // ------------------------------------------------------------------ setup

  /**
   * Lazily initialise the WASM engine (idempotent).
   * @returns {Promise<WasmEngine>}
   * @private
   */
  async _ready() {
    if (this._engine) return this._engine;
    if (!this._initPromise) {
      // Always pass the .wasm location explicitly so the generated glue's
      // ESM-only `import.meta.url` fallback is never exercised — that
      // fallback throws when the SDK is bundled for a classic <script>/UMD
      // context. See _wasmUrl() for how the URL is resolved. The
      // `{ module_or_path }` object form is the glue's non-deprecated API.
      this._initPromise = init({ module_or_path: this._wasmUrl() }).catch((err) => {
        this._initPromise = null;
        throw toLibfwError(err);
      });
    }
    await this._initPromise;
    const engine = new WasmEngine({
      concurrency: this._options.concurrency,
      uploadWindow: this._options.uploadWindow,
      downloadWindow: this._options.downloadWindow,
      downloadChunkSize: this._options.downloadChunkSize,
      compress: this._options.compress,
      chunkSize: this._options.chunkSize,
      maxRetries: this._options.maxRetries,
      baseRetryDelayMs: this._options.baseRetryDelayMs,
      maxRetryDelayMs: this._options.maxRetryDelayMs,
      timeoutMs: this._options.timeoutMs,
    });
    engine.set_callbacks(this._makeCallbacks());
    this._engine = engine;
    return engine;
  }

  /**
   * Resolve the `.wasm` file URL without relying on `import.meta` (which is
   * ESM-only and a parse error in a classic `<script>`).
   *
   * Order: explicit `wasmUrl` option → classic-script `document.currentScript`
   * → ESM `import.meta.url`. The `wasmUrl` option is the escape hatch for
   * deployments where neither auto-detection applies.
   * @returns {string|URL}
   * @private
   */
  _wasmUrl() {
    if (this._options.wasmUrl) return this._options.wasmUrl;
    // Classic <script> / UMD: the bundle's own <script src> (captured at load
    // time in BUNDLE_SCRIPT_SRC) tells us where the sibling `.wasm` lives.
    if (BUNDLE_SCRIPT_SRC) {
      return new URL('libfw_client_bg.wasm', BUNDLE_SCRIPT_SRC);
    }
    // ESM: relative to this module.
    if (typeof import.meta !== 'undefined' && import.meta.url) {
      return new URL('./pkg/libfw_client_bg.wasm', import.meta.url);
    }
    // Last resort: same-origin relative path.
    return 'libfw_client_bg.wasm';
  }

  /**
   * Build the callbacks object handed to the WASM engine.
   * @returns {object}
   * @private
   */
  _makeCallbacks() {
    return {
      onFileStart: (path, size) => {
        if (this._fallback) {
          this._fallback.sizes.set(path, size);
          // Pre-check the in-memory cap BEFORE any bytes are buffered so an
          // oversized download is rejected instead of exhausting memory.
          const max = this._maxFallbackBytes();
          if (max > 0) {
            if (size > max) {
              throw new LibfwError(
                `file too large for browser download (${size} > ${max} bytes): ${path}`,
                'too-large'
              );
            }
            this._fallback.total += size;
            if (this._fallback.total > max) {
              throw new LibfwError(
                `browser download would buffer more than the ${max}-byte in-memory limit`,
                'too-large'
              );
            }
          }
        }
        this._emit({ type: 'fileStart', path, done: 0, total: size });
      },
      onWriteChunk: (path, offset, data) => this._onWriteChunk(path, offset, data),
      onFileCompleted: (path) => this._onFileCompleted(path),
      onProgress: (done, total) => this._emit({ type: 'progress', done, total }),
      loadState: (direction, path) => this._loadResumeState(direction, path),
      saveState: (direction, path, state) => {
        // The in-memory browser-download fallback never commits bytes to
        // disk, so a persisted download offset would be a phantom that
        // poisons a later FS-API resume. Skip persisting download state
        // while a fallback transfer is active.
        if (direction === 'download' && this._fallback) return Promise.resolve();
        return Idb.saveState(`${direction}:${path}`, state);
      },
      getFileList: () => this._getFileList(),
      readFile: (path, offset, length) => this._readFile(path, offset, length),
      log: (msg) => {
        if (typeof console !== 'undefined') console.debug(`[libfw] ${msg}`);
      },
    };
  }

  /** @private */
  _emit(event) {
    if (typeof this._options.onEvent === 'function') {
      try {
        this._options.onEvent(event);
      } catch {
        /* listener errors must not break transfers */
      }
    }
  }

  /**
   * Whether the File System Access API is available in this browser.
   * @returns {boolean}
   * @private
   */
  _supportsFsAccess() {
    return (
      typeof window !== 'undefined' &&
      typeof window.showDirectoryPicker === 'function' &&
      typeof FileSystemFileHandle !== 'undefined' &&
      typeof FileSystemDirectoryHandle !== 'undefined'
    );
  }

  /**
   * Resolve the effective download mode from the `downloadMode` option:
   * an explicit `'fs'`/`'browser'` wins; `'auto'` falls back to the browser
   * download when the File System Access API is missing.
   * @returns {'fs'|'browser'}
   * @private
   */
  _effectiveMode() {
    const mode = this._options.downloadMode || 'auto';
    if (mode === 'fs' || mode === 'browser') return mode;
    return this._supportsFsAccess() ? 'fs' : 'browser';
  }

  // ------------------------------------------------------------ downloads

  /**
   * Download a whole folder from the server.
   *
   * With the File System Access API available the folder is streamed into a
   * user-selected local directory (`showDirectoryPicker`), preserving the
   * structure through one `createWritable()` per file. Without FS API (or
   * with `downloadMode: 'browser'`) the folder is buffered in memory, packed
   * into a `.zip` and saved via a traditional browser download — no manual
   * feature detection needed by the caller.
   *
   * @param {string} token bearer token
   * @param {string} [dirPath=''] virtual server path to download (root by default)
   * @returns {Promise<number>} total bytes transferred
   * @throws {LibfwError}
   */
  async downloadFolder(token, dirPath = '') {
    const engine = await this._ready();
    if (this._effectiveMode() === 'browser') {
      return this._downloadViaBrowser(engine, token, dirPath, true);
    }
    this._dirHandle = await window.showDirectoryPicker();
    this._fileHandles.clear();
    try {
      return await engine.download_folder(this._options.baseUrl, token, dirPath);
    } catch (err) {
      throw toLibfwError(err);
    } finally {
      await this._flushWritables();
      await this._syncResumeOffsets();
    }
  }

  /**
   * Download a single file from the server at `filePath`.
   *
   * With the File System Access API available the file is streamed into the
   * directory chosen via `showDirectoryPicker()`. Without FS API (or with
   * `downloadMode: 'browser'`) the file is buffered and saved through a
   * traditional browser download.
   *
   * @param {string} token bearer token
   * @param {string} filePath virtual server path of the file to download
   * @returns {Promise<number>} total bytes transferred
   * @throws {LibfwError}
   */
  async downloadFile(token, filePath) {
    const engine = await this._ready();
    if (!filePath) throw new LibfwError('downloadFile requires a file path', 'path');
    if (this._effectiveMode() === 'browser') {
      return this._downloadViaBrowser(engine, token, filePath, false);
    }
    this._dirHandle = await window.showDirectoryPicker();
    this._fileHandles.clear();
    try {
      return await engine.download_file(this._options.baseUrl, token, filePath);
    } catch (err) {
      throw toLibfwError(err);
    } finally {
      await this._flushWritables();
      await this._syncResumeOffsets();
    }
  }

  /**
   * Buffer-chunk fallback download used when the File System Access API is
   * unavailable (or `downloadMode: 'browser'`).
   *
   * `onWriteChunk` chunks are collected per path in memory (the engine keeps
   * calling them in order). When the transfer finishes: a single file is
   * emitted as a `Blob` and saved via a normal browser download; a folder is
   * packed into a `.zip` (STORE method) and downloaded. Progress/state events
   * keep flowing as usual. Note this buffers the whole transfer in memory —
   * the cost of not having FS API to stream to disk.
   *
   * @param {WasmEngine} engine
   * @param {string} token
   * @param {string} path virtual path
   * @param {boolean} isFolder
   * @returns {Promise<number>} total bytes transferred
   * @private
   */
  async _downloadViaBrowser(engine, token, path, isFolder) {
    this._fallback = { isFolder, buffers: new Map(), order: [], sizes: new Map(), total: 0 };
    try {
      const total = isFolder
        ? await engine.download_folder(this._options.baseUrl, token, path)
        : await engine.download_file(this._options.baseUrl, token, path);
      const { buffers, order, sizes } = this._fallback;
      if (isFolder) {
        const entries = [];
        for (const p of order) {
          entries.push({
            name: this._safeEntryName(p),
            data: this._concatBuffers(buffers.get(p)?.chunks || []),
          });
        }
        // Include files that were announced but produced no bytes (empty).
        for (const p of sizes.keys()) {
          if (!buffers.has(p)) {
            entries.push({ name: this._safeEntryName(p), data: new Uint8Array(0) });
          }
        }
        this._triggerBrowserDownload(createZip(entries), this._archiveName(path));
      } else {
        const data = this._concatBuffers(buffers.get(path)?.chunks || []);
        this._triggerBrowserDownload(new Blob([data], { type: 'application/octet-stream' }), this._downloadName(path));
      }
      return total;
    } catch (err) {
      throw toLibfwError(err);
    } finally {
      this._fallback = null;
    }
  }

  /**
   * Concatenate buffered chunks into one `Uint8Array`.
   * @param {Uint8Array[]} bufs
   * @returns {Uint8Array}
   * @private
   */
  _concatBuffers(bufs) {
    if (bufs.length === 0) return new Uint8Array(0);
    if (bufs.length === 1) return bufs[0];
    const len = bufs.reduce((n, b) => n + b.length, 0);
    const out = new Uint8Array(len);
    let off = 0;
    for (const b of bufs) {
      out.set(b, off);
      off += b.length;
    }
    return out;
  }

  /**
   * Strip a leading `/` so an entry path is archive/OS friendly.
   * @param {string} path
   * @returns {string}
   * @private
   */
  _cleanPath(path) {
    return String(path).replace(/^\/+/, '');
  }

  /**
   * Validate a virtual path for use as a ZIP entry name, rejecting any
   * traversal (`..`), absolute/drive-letter prefixes or Windows-style
   * separators that could escape the archive on extraction (zip-slip).
   * @param {string} path
   * @returns {string}
   * @private
   */
  _safeEntryName(path) {
    const cleaned = this._cleanPath(path);
    const segs = String(cleaned).split('/');
    if (segs.some((seg) => seg === '..' || seg.includes('\\') || /^[a-zA-Z]:/.test(seg))) {
      throw new LibfwError(`unsafe path in download: ${path}`, 'path');
    }
    return cleaned;
  }

  /**
   * The configured in-memory cap for the browser-download fallback.
   * @returns {number} 0 disables the limit.
   * @private
   */
  _maxFallbackBytes() {
    const max = Number(this._options.maxFallbackBytes);
    return Number.isFinite(max) && max > 0 ? max : 0;
  }

  /**
   * Derive a safe file name from a virtual path.
   * @param {string} path
   * @returns {string}
   * @private
   */
  _downloadName(path) {
    const name = this._cleanPath(path).split('/').pop();
    return name || 'download';
  }

  /**
   * Derive the `.zip` archive name for a folder download.
   * @param {string} path
   * @returns {string}
   * @private
   */
  _archiveName(path) {
    const base = this._cleanPath(path).split('/').pop() || 'download';
    return `${base.replace(/[^\w.\- ]+/g, '_') || 'download'}.zip`;
  }

  /**
   * Trigger a traditional browser download via a temporary `<a download>`.
   * @param {Blob} blob
   * @param {string} filename
   * @returns {void}
   * @private
   */
  _triggerBrowserDownload(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    // Revoke once the download has had a chance to start.
    setTimeout(() => URL.revokeObjectURL(url), 60000);
  }

  /**
   * Stream a decompressed chunk to disk, keeping memory bounded regardless
   * of file size (no whole-file buffering).
   *
   * The destination writable is opened exactly once per file and written in
   * **append mode** (`writable.write(data)` without an explicit `position`).
   * The engine awaits this callback, so chunks for a file arrive strictly in
   * order, making append writes correct for both fresh and resumed
   * downloads. Crucially, this avoids per-write
   * `{ type: 'write', position }` calls, which in Chromium can spawn a fresh
   * `.crswap` swap file per write and leave the target file empty on close —
   * the single `createWritable()` + sequential writes + one `close()` below
   * commits the swap file atomically.
   * @param {string} path virtual path
   * @param {number} offset byte offset (informational; writes append)
   * @param {Uint8Array} data decompressed chunk
   * @returns {Promise<void>}
   * @private
   */
  async _onWriteChunk(path, offset, data) {
    if (this._fallback) {
      // Browser-download fallback: collect chunks in memory instead of
      // streaming to disk. Chunks arrive in order, so a plain append works.
      let buf = this._fallback.buffers.get(path);
      if (!buf) {
        buf = { chunks: [], len: 0 };
        this._fallback.buffers.set(path, buf);
        this._fallback.order.push(path);
      }
      // A chunk whose absolute offset is at or before the last one delivered
      // means the engine restarted this file (an internal retry re-delivers
      // the same byte range) — drop the partial buffer so the prefix is not
      // duplicated in the produced blob/zip.
      if (offset <= buf.len) {
        buf.chunks = [];
        buf.len = 0;
      }
      buf.chunks.push(data);
      buf.len += data.length;
      return;
    }
    let entry = this._writables.get(path);
    if (entry && offset <= entry.lastOffset) {
      // The engine restarted this file (a network/protocol error triggered
      // an internal retry that re-delivers the same byte range). Abort the
      // still-open writable — discarding its uncommitted swap file — and
      // reopen fresh, otherwise the re-delivered prefix would be appended a
      // second time and the committed file silently corrupted.
      this._writables.delete(path);
      try {
        await entry.writable.abort();
      } catch {
        /* best-effort discard */
      }
      entry = undefined;
    }
    if (!entry) {
      const { dir, name, handle } = await this._ensureFileHandle(path);
      this._fileHandles.set(path, handle);
      // A true resume (first chunk at offset > 0) keeps the existing prefix
      // on disk; a fresh download opens a truncating writable.
      const isResume = offset > 0;
      if (!isResume) {
        // Remove any orphaned `.crswap` left behind by a crashed/aborted run
        // so a stale swap file can never shadow the new write.
        await this._removeSwapFile(dir, name);
      }
      const writable = await handle.createWritable(
        isResume ? { keepExistingData: true } : undefined
      );
      entry = { writable, dir, name, lastOffset: offset };
      this._writables.set(path, entry);
    }
    entry.lastOffset = offset;
    await entry.writable.write(data);
  }

  /**
   * Close the destination writable once a file's transfer completes.
   * @param {string} path virtual path
   * @returns {Promise<void>}
   * @private
   */
  async _onFileCompleted(path) {
    if (this._fallback) {
      // Nothing is open to flush in browser-download mode.
      this._emit({ type: 'fileCompleted', path });
      return;
    }
    if (this._writables.has(path)) {
      await this._closeWritable(path);
    } else {
      // No bytes were written this run — either a zero-byte file or a file
      // already fully on disk from a resume. Materialize the target so an
      // empty file still appears on disk. `getFileHandle({ create: true })`
      // only creates a missing file, so an already-complete (resumed) file
      // is never truncated here.
      try {
        await this._ensureFileHandle(path);
      } catch {
        /* best-effort: never fail the transfer on materialization errors */
      }
    }
    this._emit({ type: 'fileCompleted', path });
  }

  /**
   * Close (and forget) a file's writable, atomically committing the swap
   * file to its final name. Best-effort so failure/abort never throws.
   * @param {string} path virtual path
   * @returns {Promise<void>}
   * @private
   */
  async _closeWritable(path) {
    const entry = this._writables.get(path);
    if (entry) {
      this._writables.delete(path);
      try {
        await entry.writable.close();
      } catch {
        /* best-effort flush on failure/abort */
      }
    }
  }

  /**
   * Resolve (and create, if needed) the file handle for a virtual path,
   * creating any parent directories along the way.
   * @param {string} path
   * @returns {Promise<{dir: FileSystemDirectoryHandle, name: string, handle: FileSystemFileHandle}>}
   * @private
   */
  async _ensureFileHandle(path) {
    const segments = splitPath(path);
    if (segments.length === 0) {
      throw new LibfwError(`invalid download path: ${path}`, 'path');
    }
    let dir = this._dirHandle;
    for (let i = 0; i < segments.length - 1; i += 1) {
      dir = await dir.getDirectoryHandle(segments[i], { create: true });
    }
    const name = segments[segments.length - 1];
    const handle = await dir.getFileHandle(name, { create: true });
    return { dir, name, handle };
  }

  /**
   * Delete a leftover Chromium swap file (`.<name>.crswap`) next to a file,
   * ignoring any error (no swap file, or permission denied).
   * @param {FileSystemDirectoryHandle} dir parent directory
   * @param {string} name target file name
   * @returns {Promise<void>}
   * @private
   */
  async _removeSwapFile(dir, name) {
    try {
      await dir.removeEntry(`.${name}.crswap`, { recursive: false });
    } catch {
      /* nothing to clean up */
    }
  }

  /**
   * Resolve a file handle for a virtual path WITHOUT creating it, so a
   * missing file returns `null` instead of throwing.
   * @param {string} path
   * @returns {Promise<FileSystemFileHandle|null>}
   * @private
   */
  async _resolveFileHandle(path) {
    if (!this._dirHandle) return null;
    try {
      const segments = splitPath(path);
      if (segments.length === 0) return null;
      let dir = this._dirHandle;
      for (let i = 0; i < segments.length - 1; i += 1) {
        dir = await dir.getDirectoryHandle(segments[i], { create: false });
      }
      return await dir.getFileHandle(segments[segments.length - 1], { create: false });
    } catch {
      return null; // missing dir or file, or no FS API
    }
  }

  /**
   * Load a persisted resume-state `{etag, offset}`, clamping the download
   * `offset` down to the ACTUAL number of bytes committed on disk.
   *
   * `createWritable()` only commits to the real file on `close()`, so a
   * download interrupted by a hard page kill leaves the persisted offset
   * AHEAD of the bytes actually on disk. Resuming from that stale offset
   * with `keepExistingData` would write past a prefix that isn't there,
   * leaving a silent gap in the file. Clamping to the real on-disk length
   * makes the resume pick up exactly where the file really ends.
   * @param {string} direction
   * @param {string} path
   * @returns {Promise<object|null>}
   * @private
   */
  async _loadResumeState(direction, path) {
    const state = await Idb.loadState(`${direction}:${path}`);
    if (!state) return null;
    // Only downloads that will append onto an on-disk file need the clamp;
    // uploads drive resume from the server's own received ranges.
    if (direction !== 'download') return state;
    const offset = Number(state.offset) || 0;
    if (offset <= 0) return state;
    const handle = await this._resolveFileHandle(path);
    if (!handle) {
      // No on-disk file to resume from: whatever offset was persisted has
      // nothing behind it. Restart from 0 instead of trusting a stale offset
      // that would make the engine skip the download entirely (producing an
      // empty/missing file while reporting "complete").
      return { ...state, offset: 0, size: 0 };
    }
    let diskLen = 0;
    try {
      diskLen = (await handle.getFile()).size;
    } catch {
      return state;
    }
    // Clamp to the bytes actually on disk. If nothing was committed (hard
    // kill before any close), resume from 0 so the whole file is fetched
    // again rather than leaving a gap.
    const clamped = Math.min(offset, diskLen);
    return { ...state, offset: clamped, size: clamped };
  }

  /**
   * Close all still-open writable streams (flush to disk). Called on
   * success, failure or cancellation of a transfer.
   * @returns {Promise<void>}
   * @private
   */
  async _flushWritables() {
    const pending = [...this._writables.entries()].map(async ([path, entry]) => {
      this._writables.delete(path);
      try {
        await entry.writable.close();
      } catch {
        /* best-effort flush */
      }
    });
    await Promise.allSettled(pending);
  }

  /**
   * Reconcile persisted download resume offsets with the bytes actually
   * committed to disk.
   *
   * `createWritable()` only commits to the real file on `close()`, so an
   * interrupted download's on-disk length can be ahead of (or behind) the
   * engine's periodically-saved offset. Overwriting each file's stored
   * offset with its real size keeps the append-based resume consistent:
   * the next transfer resumes exactly where the file on disk ends.
   * @returns {Promise<void>}
   * @private
   */
  async _syncResumeOffsets() {
    for (const [path, handle] of this._fileHandles) {
      try {
        const file = await handle.getFile();
        const size = file.size;
        const state = await Idb.loadState(`download:${path}`);
        if (state && typeof state.etag === 'string') {
          await Idb.saveState(`download:${path}`, { ...state, offset: size, size });
        }
      } catch {
        /* best-effort */
      }
    }
    this._fileHandles.clear();
  }

  // -------------------------------------------------------------- uploads

  /**
   * Upload files to the server.
   *
   * If `files` is omitted, `showDirectoryPicker()` is used to select a
   * local folder whose structure is mirrored on the server. Otherwise
   * `files` may be a `FileList`, an array of `File`s, or an array of
   * `{ path, size, mtime }` plan entries (when you want to drive reading
   * yourself).
   *
   * @param {string} token bearer token
   * @param {FileList|File[]|Array<{path:string,size:number,mtime:number}>} [files]
   * @returns {Promise<number>} total bytes uploaded
   * @throws {LibfwError}
   */
  async upload(token, files) {
    const engine = await this._ready();
    this._uploadFiles.clear();
    this._uploadPlan = [];

    if (files === undefined || files === null) {
      if (typeof window === 'undefined' || typeof window.showDirectoryPicker !== 'function') {
        throw new LibfwError('File System Access API is not available in this browser', 'unsupported');
      }
      const dir = await window.showDirectoryPicker();
      this._dirHandle = dir;
      this._uploadPlan = await this._collectDirectoryFiles(dir, '');
    } else {
      this._uploadPlan = await this._collectProvidedFiles(files);
    }

    try {
      return await engine.upload(this._options.baseUrl, token);
    } catch (err) {
      throw toLibfwError(err);
    }
  }

  /**
   * Walk a directory handle and build the upload plan.
   * @param {FileSystemDirectoryHandle} dir
   * @param {string} prefix virtual path prefix
   * @returns {Promise<Array<{path:string,size:number,mtime:number}>>}
   * @private
   */
  async _collectDirectoryFiles(dir, prefix) {
    const plan = [];
    for await (const [name, handle] of dir.entries()) {
      const path = prefix ? `${prefix}/${name}` : name;
      if (handle.kind === 'directory') {
        plan.push(...(await this._collectDirectoryFiles(handle, path)));
      } else {
        const file = await handle.getFile();
        this._uploadFiles.set(path, file);
        plan.push({ path, size: file.size, mtime: Math.floor(file.lastModified / 1000) });
      }
    }
    plan.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    return plan;
  }

  /**
   * Build the upload plan from a FileList / File[] / plan array.
   * @param {FileList|File[]|Array} files
   * @returns {Promise<Array<{path:string,size:number,mtime:number}>>}
   * @private
   */
  async _collectProvidedFiles(files) {
    if (Array.isArray(files) && files.length > 0 && typeof files[0] === 'object' && files[0] !== null && 'path' in files[0] && !(files[0] instanceof File)) {
      // Caller-supplied plan (no File objects → they must provide readFile).
      return files.map((f) => ({
        path: String(f.path),
        size: Number(f.size) || 0,
        mtime: Number(f.mtime) || 0,
      }));
    }
    const list = Array.from(files || []);
    const plan = [];
    for (const file of list) {
      if (!(file instanceof File)) continue;
      const path = file.webkitRelativePath || file.name;
      this._uploadFiles.set(path, file);
      plan.push({ path, size: file.size, mtime: Math.floor(file.lastModified / 1000) });
    }
    plan.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    return plan;
  }

  /**
   * Engine callback: current upload plan.
   * @returns {Promise<Array<{path:string,size:number,mtime:number}>>}
   * @private
   */
  async _getFileList() {
    return this._uploadPlan;
  }

  /**
   * Engine callback: read `length` bytes of an upload file at `offset`.
   * @param {string} path
   * @param {number} offset
   * @param {number} length
   * @returns {Promise<Uint8Array>}
   * @private
   */
  async _readFile(path, offset, length) {
    const file = this._uploadFiles.get(path);
    if (!file) {
      throw new LibfwError(`upload source not found: ${path}`, 'storage');
    }
    const blob = file.slice(offset, offset + length);
    const buffer = await blob.arrayBuffer();
    return new Uint8Array(buffer);
  }

  // ------------------------------------------------------------- controls

  /** Pause the active transfer (state → `paused`). */
  pause() {
    if (this._engine) this._engine.pause();
  }

  /** Resume a paused transfer. */
  resume() {
    if (this._engine) this._engine.resume();
  }

  /** Cancel the active transfer (state → `failed`). */
  cancel() {
    if (this._engine) this._engine.cancel();
  }

  /**
   * Current engine state: `idle | downloading | uploading | paused |
   * completed | failed`.
   * @returns {string}
   */
  state() {
    return this._engine ? this._engine.state() : 'idle';
  }

  /**
   * Progress in `[0, 1]`.
   * @returns {number}
   */
  progress() {
    return this._engine ? this._engine.progress() : 0;
  }

  /**
   * Bytes transferred so far.
   * @returns {number}
   */
  doneBytes() {
    return this._engine ? this._engine.done_bytes() : 0;
  }

  /**
   * Total bytes to transfer.
   * @returns {number}
   */
  totalBytes() {
    return this._engine ? this._engine.total_bytes() : 0;
  }

  // ------------------------------------------------------------- resume store

  /**
   * Delete persisted resume state (IndexedDB).
   *
   * Pass a direction to wipe only that transfer's state, leaving the other
   * direction intact — the targeted replacement for clearing the whole store
   * before every transfer:
   *
   * - `await client.clearResumeStore('download')` — drop all download state.
   * - `await client.clearResumeStore('upload')` — drop all upload state.
   * - `await client.clearResumeStore()` — wipe everything (whole-store clear).
   *
   * @param {'upload'|'download'} [direction] restrict to one direction
   * @returns {Promise<number>} number of records removed
   */
  async clearResumeStore(direction) {
    if (direction !== undefined && direction !== 'upload' && direction !== 'download') {
      throw new LibfwError(
        `clearResumeStore: expected 'upload' | 'download' | undefined, got ${JSON.stringify(direction)}`,
        'path'
      );
    }
    if (direction === undefined) {
      await Idb.clear();
      return 0;
    }
    return Idb.clearDirection(direction);
  }
}

export default LibfwClient;
