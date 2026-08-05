/**
 * libfw-client — browser SDK.
 *
 * A thin, dependency-free wrapper around the libfw WASM engine that owns:
 *  - WASM instantiation (via the wasm-bindgen `web` glue),
 *  - the File System Access API (`showDirectoryPicker`, `getFileHandle`,
 *    `createWritable`),
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
export class LibfwClient {
  /**
   * @param {object} [options]
   * @param {string} [options.baseUrl=''] base URL the server routes are mounted under
   * @param {number} [options.concurrency=4] max concurrent file transfers
   * @param {boolean} [options.compress=true] negotiate zrip compression
   * @param {number} [options.chunkSize=2097152] upload chunk size in bytes
   * @param {number} [options.maxRetries=3] retries per chunk/file before failing
   * @param {number} [options.baseRetryDelayMs=500] initial backoff (ms)
   * @param {number} [options.maxRetryDelayMs=30000] backoff ceiling (ms)
   * @param {number} [options.timeoutMs=60000] per-request timeout (ms)
   * @param {(event: {type: string, done: number, total: number, path?: string, error?: string}) => void} [options.onEvent]
   *        optional progress/state listener
   */
  constructor(options = {}) {
    this._options = {
      baseUrl: '',
      concurrency: 4,
      compress: true,
      chunkSize: 2 * 1024 * 1024,
      maxRetries: 3,
      baseRetryDelayMs: 500,
      maxRetryDelayMs: 30000,
      timeoutMs: 60000,
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
    /** @type {Map<string, FileSystemWritableFileStream>} path → writable stream */
    this._writables = new Map();
    /** @type {Map<string, File>} path → File (upload) */
    this._uploadFiles = new Map();
    /** @type {Array<{path:string,size:number,mtime:number}>} upload plan */
    this._uploadPlan = [];
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
      this._initPromise = init().catch((err) => {
        this._initPromise = null;
        throw toLibfwError(err);
      });
    }
    await this._initPromise;
    const engine = new WasmEngine({
      concurrency: this._options.concurrency,
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
   * Build the callbacks object handed to the WASM engine.
   * @returns {object}
   * @private
   */
  _makeCallbacks() {
    return {
      onFileStart: (path, size) => this._emit({ type: 'fileStart', path, done: 0, total: size }),
      onWriteChunk: (path, offset, data) => this._onWriteChunk(path, offset, data),
      onFileCompleted: (path) => this._emit({ type: 'fileCompleted', path }),
      onProgress: (done, total) => this._emit({ type: 'progress', done, total }),
      loadState: (path) => Idb.loadState(path),
      saveState: (path, state) => Idb.saveState(path, state),
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

  // ------------------------------------------------------------ downloads

  /**
   * Download a whole folder from the server into a local directory chosen
   * by the user via `showDirectoryPicker()`.
   *
   * Folder structure (including nested directories) is preserved; bytes
   * are streamed to disk through `createWritable({ type: 'write' })`.
   *
   * @param {string} token bearer token
   * @param {string} [dirPath=''] virtual server path to download (root by default)
   * @returns {Promise<number>} total bytes written
   * @throws {LibfwError}
   */
  async downloadFolder(token, dirPath = '') {
    const engine = await this._ready();
    if (typeof window === 'undefined' || typeof window.showDirectoryPicker !== 'function') {
      throw new LibfwError('File System Access API is not available in this browser', 'unsupported');
    }
    this._dirHandle = await window.showDirectoryPicker();
    this._fileHandles.clear();
    try {
      return await engine.download_folder(this._options.baseUrl, token, dirPath);
    } catch (err) {
      throw toLibfwError(err);
    } finally {
      await this._flushWritables();
    }
  }

  /**
   * @param {string} path virtual path
   * @param {number} offset byte offset
   * @param {Uint8Array} data decompressed chunk
   * @returns {Promise<void>}
   * @private
   */
  async _onWriteChunk(path, offset, data) {
    let writable = this._writables.get(path);
    if (!writable) {
      const handle = await this._ensureFileHandle(path);
      this._fileHandles.set(path, handle);
      // keepExistingData: true lets resumed downloads overwrite only the
      // tail without truncating the already-written prefix.
      writable = await handle.createWritable({ keepExistingData: true });
      this._writables.set(path, writable);
    }
    await writable.write({ type: 'write', position: offset, data });
  }

  /**
   * Resolve (and create, if needed) the file handle for a virtual path,
   * creating any parent directories along the way.
   * @param {string} path
   * @returns {Promise<FileSystemFileHandle>}
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
    return dir.getFileHandle(segments[segments.length - 1], { create: true });
  }

  /**
   * Close all open writable streams (flush to disk).
   * @returns {Promise<void>}
   * @private
   */
  async _flushWritables() {
    const pending = [...this._writables.values()].map((w) => w.close().catch(() => {}));
    this._writables.clear();
    await Promise.allSettled(pending);
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
}

export default LibfwClient;
