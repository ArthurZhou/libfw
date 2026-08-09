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
  | 'cancelled';

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

/** Options accepted by the {@link LibfwClient} constructor. */
export interface LibfwClientOptions {
  /** Base URL the libfw server routes are mounted under. Default `''`. */
  baseUrl?: string;
  /** Max concurrent file transfers. Default `4`. */
  concurrency?: number;
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
  /** Per-request timeout (ms). Default `60000`. */
  timeoutMs?: number;
  /**
   * Explicit URL of `libfw_client_bg.wasm`. When omitted it is resolved
   * automatically for both ESM and classic-`<script>`/UMD consumers.
   */
  wasmUrl?: string;
  /** Optional progress/state listener. */
  onEvent?: (event: LibfwEvent) => void;
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
