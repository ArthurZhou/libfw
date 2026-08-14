//! Download scheduler: recursive folder listing, per-file streaming
//! downloads with Range/If-Range resume, exponential-backoff retries and
//! bounded-memory zrip decompression.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;

use js_sys::Reflect;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use web_sys::Response;

use futures::StreamExt;
use libfw_core::compress::{decompressor, CompressionFormat};
use libfw_core::storage::DirEntry;
use libfw_core::{HEADER_COMPRESS, MIN_PARALLEL_DOWNLOAD_BYTES};

use crate::config::ClientConfig;
use crate::error::LibfwError;
use crate::http::{auth_headers, dir_url, fetch, file_url, read_all, request, stream_body};
use crate::js::Callbacks;
use crate::plan::{total_bytes, FileEntry};
use crate::state::TaskControl;

/// A single file download outcome, used for resume-state bookkeeping.
struct DownloadOutcome {
    /// Final absolute size/offset observed from the server.
    size: u64,
}

/// Persist download progress roughly every this many bytes so an
/// interrupted transfer can resume from a recent offset instead of
/// restarting from byte 0.
const RESUME_SAVE_EVERY: u64 = 4 * 1024 * 1024;

/// List the immediate children of a virtual directory via `GET /dir/..`.
async fn list_dir(
    base_url: &str,
    token: &str,
    path: &str,
    timeout_ms: u32,
) -> Result<Vec<DirEntry>, LibfwError> {
    let headers = auth_headers(token, false)?;
    let url = dir_url(base_url, path);
    let req = request(&url, "GET", &headers, None)?;
    let resp = fetch(&req, timeout_ms).await?;
    let status = resp.status();
    if status != 200 {
        return Err(LibfwError::Http { status, url });
    }
    let body = read_all(&resp, timeout_ms).await?;
    serde_json::from_slice::<Vec<DirEntry>>(&body)
        .map_err(|e| LibfwError::Protocol(format!("bad listing JSON: {e}")))
}

/// Recursively collect every file under `path` (server-side walk).
///
/// Iterative with an explicit stack so deep directory trees neither blow
/// the compiler recursion limit nor the WASM call stack.
async fn collect_files(
    base_url: &str,
    token: &str,
    path: &str,
    timeout_ms: u32,
) -> Result<Vec<FileEntry>, LibfwError> {
    let mut out = Vec::new();
    let mut stack = vec![path.to_string()];
    while let Some(dir) = stack.pop() {
        for entry in list_dir(base_url, token, &dir, timeout_ms).await? {
            if entry.is_dir {
                stack.push(entry.path);
            } else {
                out.push(FileEntry {
                    path: entry.path,
                    size: entry.size,
                    mtime: entry.mtime,
                });
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// Sleep for `ms` milliseconds on the JS event loop.
async fn sleep_ms(ms: u32) {
    if ms == 0 {
        return;
    }
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let window = web_sys::window().expect("window");
        let f: &js_sys::Function = resolve.unchecked_ref();
        let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(f, ms as i32);
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

/// Download one file with resume + retry.
///
/// Routing (tus-style): a file with many remaining bytes uses the **parallel**
/// path — `download_window` concurrent byte-range GETs, so a single file's
/// throughput is bounded by bandwidth instead of one connection's
/// `chunk_size / RTT`. Small files and the tail of a large file stay on the
/// sequential single-connection path. Both paths resume from the persisted
/// contiguous offset and re-validate it against the server (which is the
/// source of truth for what exists).
async fn download_file(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<DownloadOutcome, LibfwError> {
    callbacks.on_file_start(&file.path, file.size)?;

    // 1. Load persisted resume state: { etag, offset }.
    let mut resume: Option<(String, u64)> = None;
    if let Some(state) = callbacks.load_state("download", &file.path).await? {
        let etag = Reflect::get(&state, &JsValue::from_str("etag"))
            .ok()
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        let offset = Reflect::get(&state, &JsValue::from_str("offset"))
            .ok()
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0) as u64;
        if !etag.is_empty() && offset > 0 {
            resume = Some((etag, offset));
        }
    }

    let mut offset = resume.as_ref().map(|(_, o)| *o).unwrap_or(0);
    let mut etag = resume.as_ref().map(|(e, _)| e.clone()).unwrap_or_default();
    let mut attempts = 0u32;

    // Nothing to transfer when the file is empty (`offset == size == 0`) or
    // already fully on disk (`offset == size`). Issuing a `Range` request for
    // either would get a 416 — an empty file would loop forever, and a
    // complete file would be needlessly re-downloaded. A stale offset past
    // the current size (the file shrank) restarts from byte 0 instead.
    match classify_download(file.size, offset) {
        DownloadDisposition::Restart => {
            offset = 0;
            etag = String::new();
        }
        DownloadDisposition::AlreadyDone => {
            // Credit the bytes already on disk so a resumed folder download
            // reports the true fraction (mirrors the upload path).
            if offset > 0 {
                control.add_progress(offset);
                control.report_progress_if(callbacks)?;
            }
            return finish_download(file, callbacks, etag, DownloadOutcome { size: file.size })
                .await;
        }
        DownloadDisposition::Transfer => {}
    }

    loop {
        control.wait_ready().await?;
        control.check()?;

        // 2. Parallel path for large remaining transfers (tus-style).
        if should_parallel(file.size, offset, config) {
            match download_file_parallel(
                base_url, token, file, &etag, offset, callbacks, control, config,
            )
            .await
            {
                // The parallel path learned the authoritative ETag via HEAD;
                // persist it so a later resume can validate its offset
                // against the real remote version.
                Ok((meta_etag, outcome)) => {
                    etag = meta_etag;
                    return finish_download(file, callbacks, etag, outcome).await;
                }
                // The server signalled the file changed / shrank mid-download
                // (a 416, or a full-body 200 despite If-Range): restart from
                // byte 0 with a clean slate.
                Err(e) if is_restart_err(&e) => {
                    offset = 0;
                    attempts = 0;
                    continue;
                }
                Err(e) => {
                    if attempts >= config.max_retries {
                        return Err(e);
                    }
                    attempts += 1;
                    callbacks.log(&format!(
                        "retrying `{}` (attempt {attempts}): {e}",
                        file.path
                    ));
                    sleep_ms(config.backoff_ms(attempts)).await;
                }
            }
            continue;
        }

        // 3. Sequential single-connection path (small files / tails).
        // Re-validate the offset on every (re)try via If-Range.
        let headers = auth_headers(token, config.compress)?;
        headers
            .set("Range", &format!("bytes={offset}-"))
            .map_err(|e| LibfwError::Js(format!("set Range failed: {e:?}")))?;
        if offset > 0 && !etag.is_empty() {
            headers
                .set("If-Range", &etag)
                .map_err(|e| LibfwError::Js(format!("set If-Range failed: {e:?}")))?;
        }
        let url = file_url(base_url, &file.path);
        let req = request(&url, "GET", &headers, None)?;

        match fetch(&req, config.timeout_ms).await {
            Ok(resp) => match resp.status() {
                200 => {
                    // Full content: the file changed (or first attempt).
                    etag = response_etag(&resp).unwrap_or(etag);
                    let outcome =
                        stream_download(&resp, file, callbacks, control, 0, &etag, config.timeout_ms)
                            .await?;
                    return finish_download(file, callbacks, etag, outcome).await;
                }
                206 => {
                    if etag.is_empty() {
                        etag = response_etag(&resp).unwrap_or_default();
                    }
                    let start = content_range_start(&resp).unwrap_or(offset);
                    let outcome = stream_download(
                        &resp,
                        file,
                        callbacks,
                        control,
                        start,
                        &etag,
                        config.timeout_ms,
                    )
                    .await?;
                    return finish_download(file, callbacks, etag, outcome).await;
                }
                416 => {
                    // Offset beyond EOF → the file shrank; restart cleanly.
                    offset = 0;
                    attempts = 0;
                    continue;
                }
                code => return Err(LibfwError::Http { status: code, url }),
            },
            Err(e) => {
                // Network failure → exponential backoff and retry.
                if attempts >= config.max_retries {
                    return Err(e);
                }
                attempts += 1;
                callbacks.log(&format!(
                    "retrying `{}` (attempt {attempts}): {e}",
                    file.path
                ));
                sleep_ms(config.backoff_ms(attempts)).await;
            }
        }
    }
}

/// Whether `file` should use the parallel (byte-range GET) download path.
///
/// Requires a per-file window > 1 and enough remaining bytes that the extra
/// per-request overhead pays off. Small files (and the tail of a large one)
/// stay on the sequential single-connection path.
fn should_parallel(size: u64, resume_offset: u64, config: &ClientConfig) -> bool {
    config.download_window > 1
        && size >= MIN_PARALLEL_DOWNLOAD_BYTES
        && size.saturating_sub(resume_offset) >= MIN_PARALLEL_DOWNLOAD_BYTES
}

/// A download error that means "the remote file changed / shrank — restart
/// from byte 0" (the parallel path surfaces it as a 416, or a full-body 200
/// despite `If-Range`).
fn is_restart_err(e: &LibfwError) -> bool {
    matches!(e, LibfwError::Http { status: 416 | 200, .. })
}

/// What a persisted resume offset means for a `size`-byte file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownloadDisposition {
    /// The offset is beyond EOF — the remote file shrank; restart from byte 0.
    Restart,
    /// The offset equals the file size — empty file, or already fully on disk.
    AlreadyDone,
    /// There are bytes left to fetch.
    Transfer,
}

/// Classify `offset` against a `size`-byte file.
fn classify_download(size: u64, offset: u64) -> DownloadDisposition {
    if offset > size {
        DownloadDisposition::Restart
    } else if offset == size {
        DownloadDisposition::AlreadyDone
    } else {
        DownloadDisposition::Transfer
    }
}

/// Fetch a file's authoritative `{ etag, size }` via `HEAD`.
///
/// The server is the source of truth (tus `HEAD` philosophy): the client
/// never trusts its own bookkeeping about the remote file — it asks. The
/// server returns `Content-Length` (no compression is negotiated on HEAD)
/// and `ETag`, which the client uses to validate the persisted resume
/// offset and to plan parallel chunks.
async fn fetch_meta(
    base_url: &str,
    token: &str,
    path: &str,
    timeout_ms: u32,
) -> Result<(String, u64), LibfwError> {
    let headers = auth_headers(token, false)?;
    let url = file_url(base_url, path);
    let req = request(&url, "HEAD", &headers, None)?;
    let resp = fetch(&req, timeout_ms).await?;
    let status = resp.status();
    if status != 200 {
        return Err(LibfwError::Http { status, url });
    }
    let etag = response_etag(&resp).unwrap_or_default();
    let size = content_length(&resp).unwrap_or(0);
    Ok((etag, size))
}

/// Fetch one byte range `[start, end)` as a single (decompressed) chunk,
/// with per-chunk exponential-backoff retries.
///
/// Only this chunk is retried — a transient failure never forces the whole
/// file (or the rest of the window) to restart, which is the tus
/// "retransmit only the broken part" principle. A 416 (file shrank) or a
/// full-body 200 (file changed despite `If-Range`) is NOT retried: it means
/// the caller must restart the whole file from byte 0.
async fn download_chunk_with_retry(
    base_url: &str,
    token: &str,
    path: &str,
    etag: &str,
    start: u64,
    end: u64,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<Vec<u8>, LibfwError> {
    let mut attempts = 0u32;
    loop {
        control.wait_ready().await?;
        control.check()?;
        match download_chunk_once(
            base_url, token, path, etag, start, end, config.compress, config.timeout_ms,
        )
        .await
        {
            Ok(data) => {
                if data.len() as u64 != end - start {
                    return Err(LibfwError::Protocol(format!(
                        "chunk {start}..{end} of `{path}` yielded {} bytes, expected {}",
                        data.len(),
                        end - start
                    )));
                }
                return Ok(data);
            }
            Err(e) if is_restart_err(&e) => return Err(e),
            Err(e) => {
                if attempts >= config.max_retries {
                    return Err(e);
                }
                attempts += 1;
                callbacks.log(&format!(
                    "retrying chunk {start}..{end} of `{path}` (attempt {attempts}): {e}"
                ));
                sleep_ms(config.backoff_ms(attempts)).await;
            }
        }
    }
}

/// A single `GET` with `Range: bytes=start-(end-1)`, decompressing the
/// response body into one `Vec<u8>`.
async fn download_chunk_once(
    base_url: &str,
    token: &str,
    path: &str,
    etag: &str,
    start: u64,
    end: u64,
    compress: bool,
    timeout_ms: u32,
) -> Result<Vec<u8>, LibfwError> {
    let headers = auth_headers(token, compress)?;
    let last = end.saturating_sub(1);
    headers
        .set("Range", &format!("bytes={start}-{last}"))
        .map_err(|e| LibfwError::Js(format!("set Range failed: {e:?}")))?;
    if !etag.is_empty() {
        headers
            .set("If-Range", etag)
            .map_err(|e| LibfwError::Js(format!("set If-Range failed: {e:?}")))?;
    }
    let url = file_url(base_url, path);
    let req = request(&url, "GET", &headers, None)?;
    let resp = fetch(&req, timeout_ms).await?;
    match resp.status() {
        206 => collect_chunk(&resp, timeout_ms).await,
        // Full body despite a Range + If-Range → the file changed; 416 → it
        // shrank. Both mean "restart from byte 0" (handled by the caller).
        code => Err(LibfwError::Http {
            status: code,
            url,
        }),
    }
}

/// Stream a `206` response body into one decompressed `Vec<u8>`.
async fn collect_chunk(resp: &Response, timeout_ms: u32) -> Result<Vec<u8>, LibfwError> {
    // Decide the wire format from the response header (robust against a
    // server that did not honour our Accept-Encoding).
    let format = resp
        .headers()
        .get(HEADER_COMPRESS)
        .ok()
        .flatten()
        .and_then(|v| CompressionFormat::parse_header(&v))
        .unwrap_or(CompressionFormat::None);

    let decomp = Rc::new(RefCell::new(decompressor(format)));
    let out = Rc::new(RefCell::new(Vec::new()));
    let collected = Rc::new(RefCell::new(Vec::new()));

    stream_body(
        resp,
        timeout_ms,
        {
            let decomp = decomp.clone();
            let out = out.clone();
            let collected = collected.clone();
            move |chunk| {
                let decomp = decomp.clone();
                let out = out.clone();
                let collected = collected.clone();
                async move {
                    decomp
                        .borrow_mut()
                        .decompress(&chunk, &mut out.borrow_mut())
                        .map_err(|e| LibfwError::Decompress(e.to_string()))?;
                    let data = std::mem::take(&mut *out.borrow_mut());
                    if !data.is_empty() {
                        collected.borrow_mut().extend_from_slice(&data);
                    }
                    Ok(())
                }
            }
        },
    )
    .await?;

    // Flush any final decompressed frames.
    decomp
        .borrow_mut()
        .finish(&mut out.borrow_mut())
        .map_err(|e| LibfwError::Decompress(e.to_string()))?;
    let tail = std::mem::take(&mut *out.borrow_mut());
    if !tail.is_empty() {
        collected.borrow_mut().extend_from_slice(&tail);
    }
    Ok(std::mem::take(&mut *collected.borrow_mut()))
}

/// Download a large file with the tus-style parallel path: `download_window`
/// concurrent byte-range GETs, reordered in memory and emitted to the SDK
/// **strictly in order** (so the SDK's append-mode writable stays correct and
/// the `.crswap` fix is preserved).
///
/// - **Server-authoritative**: starts with a `HEAD` to learn the real size
///   and ETag, validates the persisted resume offset against that ETag, and
///   only fetches the chunks after the contiguous resume point.
/// - **High-latency**: the concurrent window fills the bandwidth-delay
///   product, so a single file's throughput is bounded by bandwidth instead
///   of one connection's `chunk_size / RTT`.
/// - **Retransmission**: each chunk is retried independently (only the lost
///   part is re-fetched); a permanent failure fails the file but the
///   contiguous resume state stays persisted, so a later attempt continues
///   from where the disk actually ends.
async fn download_file_parallel(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    resume_etag: &str,
    resume_offset: u64,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<(String, DownloadOutcome), LibfwError> {
    let (meta_etag, size) = fetch_meta(base_url, token, &file.path, config.timeout_ms).await?;
    if size == 0 {
        return Err(LibfwError::Protocol(format!(
            "remote size of `{}` is 0; cannot plan parallel chunks",
            file.path
        )));
    }
    // Revalidate the persisted offset against the server's ETag (tus HEAD
    // philosophy): a changed file restarts from byte 0.
    let start = if !resume_etag.is_empty() && resume_etag != meta_etag {
        0
    } else {
        resume_offset.min(size)
    };

    if control.total_bytes() == 0 {
        control.set_total(size);
    }
    if start > 0 {
        // Seed progress with the contiguous bytes already on disk so a
        // resume reflects the true fraction (matches the upload path).
        control.add_progress(start);
        control.report_progress_if(callbacks)?;
    }

    let window = config.download_window.max(1);

    // Plan chunks from the resume point to EOF.
    let chunks = parallel_chunks(start, size, config.download_chunk_size);

    // Give the stream its own ETag copy so `meta_etag` stays free for the
    // return value (the stream borrows the closure until it is dropped).
    let stream_etag = meta_etag.clone();
    let mut stream = futures::stream::iter(chunks.into_iter().map(move |(s, e)| {
        let base_url = base_url.to_string();
        let token = token.to_string();
        let path = file.path.clone();
        let etag = stream_etag.clone();
        let callbacks = callbacks.clone();
        let control = control.clone();
        let config = config.clone();
        async move {
            let data = download_chunk_with_retry(
                &base_url, &token, &path, &etag, s, e, &callbacks, &control, &config,
            )
            .await?;
            Ok::<_, LibfwError>((s, data))
        }
    }))
    .buffer_unordered(window);

    // Reorder completed chunks so the SDK receives them in ascending order.
    // Worst-case memory = `window` in-flight chunks ≈ window * chunk_size.
    let mut pending: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut contiguous = start;
    let mut last_saved = 0u64;

    while let Some(res) = stream.next().await {
        control.wait_ready().await?;
        control.check()?;
        let (chunk_start, data) = res?;
        pending.insert(chunk_start, data);
        // Emit strictly in order (append-safe for the SDK's writable).
        while let Some(data) = pending.remove(&contiguous) {
            callbacks.on_write_chunk(&file.path, contiguous, &data).await?;
            let len = data.len() as u64;
            contiguous = contiguous.saturating_add(len);
            control.add_progress(len);
            control.report_progress_if(callbacks)?;
            // Persist the absolute contiguous offset periodically so a crash
            // mid-transfer can resume from disk's real end.
            if contiguous >= last_saved.saturating_add(RESUME_SAVE_EVERY) {
                last_saved = contiguous;
                let _ = callbacks
                    .save_state("download", &file.path, &resume_state_obj(&meta_etag, contiguous))
                    .await;
            }
        }
    }

    if contiguous != size {
        return Err(LibfwError::Protocol(format!(
            "download of `{}` stopped at {contiguous} bytes, expected {size}",
            file.path
        )));
    }
    Ok((meta_etag, DownloadOutcome { size: contiguous }))
}

/// The contiguous `[start, end)` chunks covering `[from, size)` at
/// `chunk_size`, starting at `from` (a resume offset).
fn parallel_chunks(from: u64, size: u64, chunk_size: u64) -> Vec<(u64, u64)> {
    // Defensive: a 0 chunk size (which config parsing prevents) falls back to
    // the default rather than degenerating into 1-byte chunks.
    let chunk_size = if chunk_size == 0 {
        libfw_core::DEFAULT_DOWNLOAD_CHUNK_SIZE
    } else {
        chunk_size
    };
    let mut chunks = Vec::new();
    let mut off = from.min(size);
    while off < size {
        let end = (off + chunk_size).min(size);
        chunks.push((off, end));
        off = end;
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_chunks_cover_file_from_resume() {
        assert_eq!(parallel_chunks(0, 10, 4), vec![(0, 4), (4, 8), (8, 10)]);
        assert_eq!(parallel_chunks(4, 10, 4), vec![(4, 8), (8, 10)]);
        // Resume at EOF → nothing left to fetch.
        assert!(parallel_chunks(10, 10, 4).is_empty());
        // Chunk size larger than the remainder is clamped.
        assert_eq!(parallel_chunks(8, 10, 4), vec![(8, 10)]);
    }

    #[test]
    fn parallel_chunks_zero_chunk_size_uses_one() {
        assert_eq!(parallel_chunks(0, 5, 0), vec![(0, 5)]);
    }

    #[test]
    fn should_parallel_requires_size_and_window() {
        let mut cfg = ClientConfig {
            download_window: 4,
            download_chunk_size: 256 * 1024,
            ..ClientConfig::default()
        };
        // Large file, fresh → parallel.
        assert!(should_parallel(10 * 1024 * 1024, 0, &cfg));
        // Window of 1 → sequential.
        cfg.download_window = 1;
        assert!(!should_parallel(10 * 1024 * 1024, 0, &cfg));
        cfg.download_window = 4;
        // Small file → sequential.
        assert!(!should_parallel(64 * 1024, 0, &cfg));
        // Large file, only a tiny tail left → sequential (avoid per-request
        // overhead on the last few bytes).
        assert!(!should_parallel(10 * 1024 * 1024, 10 * 1024 * 1024 - 1024, &cfg));
    }

    #[test]
    fn classify_download_disposition() {
        // Empty file: nothing to fetch.
        assert_eq!(classify_download(0, 0), DownloadDisposition::AlreadyDone);
        // Fresh download of a non-empty file.
        assert_eq!(classify_download(10, 0), DownloadDisposition::Transfer);
        // Mid-file resume.
        assert_eq!(classify_download(10, 4), DownloadDisposition::Transfer);
        // Fully downloaded: nothing left.
        assert_eq!(classify_download(10, 10), DownloadDisposition::AlreadyDone);
        // Stale offset beyond EOF (file shrank): restart.
        assert_eq!(classify_download(10, 11), DownloadDisposition::Restart);
    }
}

/// Stream a `200`/`206` response body, decompressing on the fly and
/// pushing chunks to JS. `start` is the byte offset the body begins at.
///
/// Progress is persisted to the resume store every [`RESUME_SAVE_EVERY`]
/// bytes (best-effort) so an interrupted transfer can resume from a recent
/// offset; `finish_download` persists the final state on success.
async fn stream_download(
    resp: &Response,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    start: u64,
    etag: &str,
    timeout_ms: u32,
) -> Result<DownloadOutcome, LibfwError> {
    // Decide the wire format from the response header (robust against a
    // server that did not honour our Accept-Encoding).
    let format = resp
        .headers()
        .get(HEADER_COMPRESS)
        .ok()
        .flatten()
        .and_then(|v| CompressionFormat::parse_header(&v))
        .unwrap_or(CompressionFormat::None);

    // If the total wasn't known up front (single-file download), derive it
    // from the server's `Content-Range`/`Content-Length` so the progress bar
    // has a real denominator instead of `0`.
    if control.total_bytes() == 0 {
        if let Some(total) = content_range_total(resp)
            .or_else(|| content_length(resp))
        {
            control.set_total(total.max(file.size));
        }
    }

    // A resumed download starts mid-file; seed progress with the prefix
    // already on disk so the bar reflects the true fraction (the parallel
    // path seeds it in `download_file_parallel`). Only the bytes read after
    // `start` are counted below.
    if start > 0 {
        control.add_progress(start);
        control.report_progress_if(callbacks)?;
    }

    // State is shared via `Rc` so the per-chunk `FnMut` callback can move
    // owned clones into its `async move` block (single-threaded WASM).
    let decomp = Rc::new(RefCell::new(decompressor(format)));
    let out = Rc::new(RefCell::new(Vec::new()));
    let file_offset = Rc::new(Cell::new(start));
    let last_saved = Rc::new(Cell::new(0u64));
    let callbacks = callbacks.clone();
    let control = control.clone();
    let path = file.path.clone();
    let etag = etag.to_string();
    let final_size = file.size;

    stream_body(
        resp,
        timeout_ms,
        |chunk| {
            let decomp = decomp.clone();
            let out = out.clone();
            let file_offset = file_offset.clone();
            let last_saved = last_saved.clone();
            let callbacks = callbacks.clone();
            let control = control.clone();
            let path = path.clone();
            let etag = etag.clone();
            async move {
                control.wait_ready().await?;
                control.check()?;
                decomp
                    .borrow_mut()
                    .decompress(&chunk, &mut out.borrow_mut())
                    .map_err(|e| LibfwError::Decompress(e.to_string()))?;
                let data = std::mem::take(&mut *out.borrow_mut());
                if !data.is_empty() {
                    let offset = file_offset.get();
                    callbacks.on_write_chunk(&path, offset, &data).await?;
                    file_offset.set(offset.saturating_add(data.len() as u64));
                    control.add_progress(data.len() as u64);
                    // Report smooth intermediate progress during a long
                    // single-file download (throttled to whole-percent
                    // boundaries; previously files sat at 0% → 100%).
                    control.report_progress_if(&callbacks)?;

                    // Persist an absolute resume offset every so often so a
                    // crash mid-transfer can continue instead of restarting.
                    let done = file_offset.get();
                    if done >= last_saved.get().saturating_add(RESUME_SAVE_EVERY) {
                        last_saved.set(done);
                        let _ = callbacks
                            .save_state("download", &path, &resume_state_obj(&etag, done))
                            .await;
                    }
                }
                Ok(())
            }
        },
    )
    .await?;

    // Flush any final decompressed frames.
    decomp
        .borrow_mut()
        .finish(&mut out.borrow_mut())
        .map_err(|e| LibfwError::Decompress(e.to_string()))?;
    let tail = std::mem::take(&mut *out.borrow_mut());
    if !tail.is_empty() {
        let offset = file_offset.get();
        callbacks.on_write_chunk(&path, offset, &tail).await?;
        file_offset.set(offset.saturating_add(tail.len() as u64));
        control.add_progress(tail.len() as u64);
        control.report_progress_if(&callbacks)?;
    }

    Ok(DownloadOutcome {
        size: final_size.max(file_offset.get()),
    })
}

/// Build a resume-state object for JS persistence.
fn resume_state_obj(etag: &str, offset: u64) -> JsValue {
    let state = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &state,
        &JsValue::from_str("etag"),
        &JsValue::from_str(etag),
    );
    let _ = js_sys::Reflect::set(
        &state,
        &JsValue::from_str("offset"),
        &JsValue::from_f64(offset as f64),
    );
    let _ = js_sys::Reflect::set(
        &state,
        &JsValue::from_str("size"),
        &JsValue::from_f64(offset as f64),
    );
    state.into()
}

/// Persist resume state (via the JS IndexedDB layer) and notify JS.
async fn finish_download(
    file: &FileEntry,
    callbacks: &Callbacks,
    etag: String,
    outcome: DownloadOutcome,
) -> Result<DownloadOutcome, LibfwError> {
    // Persist the ABSOLUTE end offset (`size`), not the per-request delta,
    // so a later resume re-requests the correct byte range.
    let offset = outcome.size;
    let size = outcome.size;
    let state = js_sys::Object::new();
    js_sys::Reflect::set(&state, &JsValue::from_str("etag"), &JsValue::from_str(&etag))
        .map_err(|e| LibfwError::Js(format!("state etag: {e:?}")))?;
    js_sys::Reflect::set(&state, &JsValue::from_str("offset"), &JsValue::from_f64(offset as f64))
        .map_err(|e| LibfwError::Js(format!("state offset: {e:?}")))?;
    js_sys::Reflect::set(&state, &JsValue::from_str("size"), &JsValue::from_f64(size as f64))
        .map_err(|e| LibfwError::Js(format!("state size: {e:?}")))?;
    callbacks.save_state("download", &file.path, &state).await?;
    callbacks.on_file_completed(&file.path).await?;
    Ok(outcome)
}

/// Download an entire folder (or the root when `path` is empty).
pub async fn download_folder(
    base_url: &str,
    token: &str,
    path: &str,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<u64, LibfwError> {
    let files = collect_files(base_url, token, path, config.timeout_ms).await?;
    let total = total_bytes(&files);
    control.set_total(total);
    callbacks.on_progress(0, total)?;

    let mut stream = futures::stream::iter(files.into_iter().map(|file| {
        let base_url = base_url.to_string();
        let token = token.to_string();
        let callbacks = callbacks.clone();
        let control = control.clone();
        let config = config.clone();
        async move { download_file(&base_url, &token, &file, &callbacks, &control, &config).await }
    }))
    .buffer_unordered(config.concurrency);

    while let Some(result) = stream.next().await {
        result?;
        // Report progress from the shared control block so pause/resume and
        // the onProgress events stay consistent (one source of truth).
        callbacks.on_progress(control.done_bytes(), control.total_bytes())?;
    }
    Ok(control.done_bytes())
}

/// Download a single file at `path` (size/etag discovered from the server).
pub async fn download_single(
    base_url: &str,
    token: &str,
    path: &str,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<u64, LibfwError> {
    // Discover the authoritative size/etag via HEAD (the server is the
    // source of truth), so a large single file can use the tus-style
    // parallel byte-range path instead of a single slow connection.
    let (_etag, size) = fetch_meta(base_url, token, path, config.timeout_ms).await?;
    let file = FileEntry {
        path: path.to_string(),
        size,
        mtime: 0,
    };
    let outcome = download_file(base_url, token, &file, callbacks, control, config).await?;
    // Return the ABSOLUTE byte count (the final offset), consistent with
    // `download_folder`, rather than this response's delta (which would be
    // misleading on a resumed download).
    Ok(outcome.size)
}

/// Read the ETag response header.
fn response_etag(resp: &Response) -> Option<String> {
    resp.headers().get("etag").ok().flatten()
}

/// Parse `Content-Range: bytes start-end/total` → `start`.
fn content_range_start(resp: &Response) -> Option<u64> {
    let value = resp.headers().get("content-range").ok().flatten()?;
    let after = value.split_whitespace().nth(1)?; // "start-end/total"
    let start = after.split('-').next()?;
    start.parse().ok()
}

/// Parse `Content-Range: bytes start-end/total` → `total` (the overall size).
fn content_range_total(resp: &Response) -> Option<u64> {
    let value = resp.headers().get("content-range").ok().flatten()?;
    let after = value.split_whitespace().nth(1)?; // "start-end/total"
    let total = after.split('/').nth(1)?;
    total.parse().ok()
}

/// Parse the `Content-Length` header.
fn content_length(resp: &Response) -> Option<u64> {
    resp.headers().get("content-length").ok().flatten()?.parse().ok()
}
