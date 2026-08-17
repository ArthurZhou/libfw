/**
 * Type definitions for `libfw-client` — the browser SDK.
 */

/** Machine-readable error categories. */
export type LibfwErrorCode =
  | 'unknown'
  | 'wasm'
  | 'abort'
  | 'unsupported'
  | 'path'
  | 'storage'
  | 'idb'
  | 'http'
  | 'network'
  | 'decompress'
  | 'compress'
  | 'protocol'
  | 'cancelled'
  | 'too-large';

/** Uniform error type thrown by every SDK operation. */
export declare class LibfwError extends Error {
  readonly name: 'LibfwError';
  readonly code: LibfwErrorCode;
  constructor(message: string, code?: LibfwErrorCode);
}

/** A file scheduled for upload. */
export interface UploadEntry {
  /** Virtual path (POSIX separators), e.g. `dir/sub/file.txt`. */
  path: string;
  /** Size in bytes. */
  size: number;
  /** Last-modified unix seconds. */
  mtime: number;
}

/** Adaptive-tuning parameters the engine is currently tuned to. */
export interface TuningParams {
  /** Cross-file transfer concurrency. */
  concurrency: number;
  /** In-flight chunks per single-file upload. */
  uploadWindow: number;
  /** In-flight byte-range GETs per single-file download. */
  downloadWindow: number;
  /** Upload chunk size in bytes. */
  chunkSize: number;
  /** Download byte-range size in bytes. */
  downloadChunkSize: number;
  /** zrip compression level (negative = faster, positive = smaller). */
  compressLevel: number;
}

/** Last-window transfer statistics reported by the tuning engine. */
export interface TuningStats {
  /** EWMA request round-trip time in milliseconds. */
  rttMs: number;
  /** Last-window throughput in megabits per second. */
  mbps: number;
}

/** Live adaptive-tuning status (see {@link LibfwClient.tuneStatus}). */
export interface TuneStatus {
  /** `uninitialized` until the first measurement window completes. */
  phase: 'uninitialized' | 'ramping' | 'settled' | 'degraded';
  params: TuningParams;
  stats: TuningStats;
  /** Hash of the server `/capabilities` payload the tuning is based on. */
  capsHash: string;
}

/** Progress / lifecycle event delivered via `options.onEvent`. */
export interface LibfwEvent {
  /** `fileStart`, `fileCompleted`, `progress`. */
  type: 'fileStart' | 'fileCompleted' | 'progress';
  /** Virtual path of the file involved (file events only). */
  path?: string;
  /** Bytes done (progress events). */
  done?: number;
  /** Total bytes (progress events). */
  total?: number;
}

/** Tuning event delivered via `options.onEvent` when `autoTune` is enabled. */
export interface LibfwTuningEvent {
  type: 'tuning';
  phase: TuneStatus['phase'];
  params: TuningParams;
  stats: TuningStats;
}

/** Options accepted by the {@link LibfwClient} constructor. */
export interface LibfwClientOptions {
  /**
   * Base URL the libfw server is served from (same-origin when empty). The
   * engine drives all control commands and data transfer over plain HTTP
   * (parallel `Range` downloads, tus-style chunked uploads) — no WebSocket
   * is used. Default `''`.
   */
  baseUrl?: string;
  /** Max concurrently-transferring files. Default `4`. */
  concurrency?: number;
  /**
   * In-flight chunk window for a single file's upload, independent of
   * `concurrency`. The missing chunks are POSTed concurrently (out of
   * order) into a shared per-session temp on the server; a higher value
   * keeps high-latency links saturated. Default `8`.
   */
  uploadWindow?: number;
  /**
   * In-flight byte-range window for a single file's download. Large files
   * are fetched as `downloadWindow` concurrent `Range` GETs (tus-style
   * parallel transfer), so a single file's throughput is bounded by
   * bandwidth instead of one connection's `chunkSize / RTT`. `1` disables
   * parallelism (sequential downloads). Default `4`.
   */
  downloadWindow?: number;
  /**
   * Byte range size for parallel downloads. The engine reorders in-flight
   * chunks in memory (worst case ≈ `downloadWindow * downloadChunkSize`
   * bytes) so the SDK still receives data strictly in order. Default
   * `262144` (256 KiB).
   */
  downloadChunkSize?: number;
  /** Negotiate zrip compression. Default `true`. */
  compress?: boolean;
  /** Upload chunk size in bytes. Default 2 MiB. */
  chunkSize?: number;
  /** Retries per chunk/file before failing. Default `3`. */
  maxRetries?: number;
  /** Initial exponential-backoff delay (ms). Default `500`. */
  baseRetryDelayMs?: number;
  /** Backoff ceiling (ms). Default `30000`. */
  maxRetryDelayMs?: number;
  /** Per-read timeout (ms). Default `60000`. */
  timeoutMs?: number;
  /**
   * Explicit URL of `libfw_client_bg.wasm`. When omitted it is resolved
   * automatically for both ESM and classic-`<script>`/UMD consumers.
   */
  wasmUrl?: string;
  /**
   * How downloads reach the user's disk.
   *
   * - `'fs'` — File System Access API (`showDirectoryPicker`), streaming to disk.
   * - `'browser'` — buffer each file, then trigger a traditional browser
   *   download; folders are packed into a `.zip` archive.
   * - `'auto'` (default) — `'fs'` when the API exists, else `'browser'`.
   */
  downloadMode?: 'auto' | 'fs' | 'browser';
  /**
   * Memory cap (bytes) for the in-memory `'browser'` download fallback.
   * File sizes are pre-checked against it before buffering; a download that
   * would exceed it is rejected with a `too-large` error. `0` disables the
   * limit. Default `536870912` (512 MiB).
   */
  maxFallbackBytes?: number;
  /**
   * Enable the adaptive tuning engine: the engine probes the server's
   * `/capabilities` limits and TCP-style ramps concurrency / windows /
   * chunk sizes (and the zrip level) from the advertised minimums using
   * real transfer stats. When disabled the configured static values are
   * used as-is. Default `false`.
   */
  autoTune?: boolean;
  /**
   * How long (ms) a settled tuning result is reused for the same server
   * origin before re-ramping. Default `3600000` (1 hour).
   */
  tuneTtlMs?: number;
  /** Optional progress/state listener. Tuning updates arrive as `{ type: 'tuning', phase, params, stats }`. */
  onEvent?: (event: LibfwEvent | LibfwTuningEvent) => void;
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
export declare class LibfwClient {
  constructor(options?: LibfwClientOptions);

  /**
   * Download a folder from the server into a user-selected local directory.
   *
   * Uses `window.showDirectoryPicker()`; nested directories are recreated
   * and bytes are streamed through `createWritable`.
   *
   * @param token bearer token
   * @param dirPath virtual server path to download (empty = root)
   * @returns total bytes written
   */
  downloadFolder(token: string, dirPath?: string): Promise<number>;

  /**
   * Download a single file from the server at `filePath` into a user-selected
   * local directory.
   *
   * @param token bearer token
   * @param filePath virtual server path of the file to download
   * @returns total bytes written
   */
  downloadFile(token: string, filePath: string): Promise<number>;

  /**
   * Upload files to the server.
   *
   * `files` may be a `FileList`, `File[]`, or `UploadEntry[]`. When omitted,
   * a local directory is selected with `showDirectoryPicker()`.
   *
   * @param token bearer token
   * @param files files (or plan entries) to upload
   * @returns total bytes uploaded
   */
  upload(
    token: string,
    files?: FileList | File[] | UploadEntry[],
  ): Promise<number>;

  /** Pause the active transfer. */
  pause(): void;

  /** Resume a paused transfer. */
  resume(): void;

  /** Cancel the active transfer. */
  cancel(): void;

  /**
   * Current engine state.
   * @returns `idle | downloading | uploading | paused | completed | failed`
   */
  state(): string;

  /** Progress in `[0, 1]`. */
  progress(): number;

  /** Bytes transferred so far. */
  doneBytes(): number;

  /** Total bytes to transfer. */
  totalBytes(): number;

  /**
   * Live adaptive-tuning status.
   *
   * @returns `{ phase, params, stats, capsHash }` — `phase` is
   *   `uninitialized | ramping | settled | degraded`; `params` holds the
   *   tuned `concurrency` / `uploadWindow` / `downloadWindow` / `chunkSize`
   *   / `downloadChunkSize` / `compressLevel`; `stats` is
   *   `{ rttMs, mbps }` (EWMA request RTT, last-window throughput).
   *   `null` until the WASM engine is initialised (or when `autoTune` is
   *   disabled, `phase` stays `uninitialized`).
   */
  tuneStatus(): TuneStatus | null;

  /**
   * Delete persisted resume state (IndexedDB).
   *
   * Pass a direction to wipe only that transfer's state (`'download'` or
   * `'upload'`); omit it to clear the whole store.
   *
   * @param direction restrict the wipe to one transfer direction
   * @returns number of records removed
   */
  clearResumeStore(direction?: 'upload' | 'download'): Promise<number>;
}

export default LibfwClient;
