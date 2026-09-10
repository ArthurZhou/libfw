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
use crate::state::{Semaphore, TaskControl};
use crate::tune::{TuneHandle, now_ms};
use crate::upload::tune_tick;

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
    let headers = auth_headers(token, false, None)?;
    let url = dir_url(base_url, path);
    let (req, ctrl) = request(&url, "GET", &headers, None)?;
    let resp = fetch(&req, timeout_ms, &ctrl).await?;
    let status = resp.status();
    if status != 200 {
        return Err(LibfwError::Http { status, url });
    }
    let body = read_all(&resp, timeout_ms, &ctrl).await?;
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
        // No window (non-browser host): degrade to returning immediately
        // instead of panicking — the backoff is merely less patient.
        let Some(window) = web_sys::window() else { return };
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
// M3 tuning plumbing (tune handle + negotiated level) pushed this past the
// 7-arg lint; grouping them would churn every call site for no gain.
#[allow(clippy::too_many_arguments)]
async fn download_file(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
    tune: &TuneHandle,
    level: i32,
) -> Result<DownloadOutcome, LibfwError> {
    callbacks.on_file_start(&file.path, file.size)?;

    // 1. Load persisted resume state: { etag, offset }.
    let mut resume: Option<(String, u64)> = None;
    if let Some(state) = callbacks.load_state("download", &file.path).await? {
        let etag = Reflect::get(&state, &JsValue::from_str("etag"))
            .ok()
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        let offset = crate::js::safe_u64(
            &Reflect::get(&state, &JsValue::from_str("offset")).unwrap_or(JsValue::UNDEFINED),
            "offset",
        )?;
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

        // 2. Chunked path for large remaining transfers (tus-style): the loop
        //    inside re-reads the tuned window / chunk size before each batch.
        if should_chunked(file.size, offset) {
            match download_file_parallel(
                base_url, token, file, &etag, offset, callbacks, control, config, tune, level,
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
                    if tune.borrow().enabled() {
                        tune_tick(tune, control, control.done_bytes(), None, true);
                    }
                    sleep_ms(config.backoff_ms(attempts)).await;
                }
            }
            continue;
        }

        // 3. Sequential single-connection path (small files / tails).
        // Re-validate the offset on every (re)try via If-Range.
        let headers = auth_headers(token, config.compress, Some(level))?;
        headers
            .set("Range", &format!("bytes={offset}-"))
            .map_err(|e| LibfwError::Js(format!("set Range failed: {e:?}")))?;
        if offset > 0 && !etag.is_empty() {
            headers
                .set("If-Range", &etag)
                .map_err(|e| LibfwError::Js(format!("set If-Range failed: {e:?}")))?;
        }
        let url = file_url(base_url, &file.path);
        let (req, ctrl) = request(&url, "GET", &headers, None)?;

        let t0 = now_ms();
        match fetch(&req, config.timeout_ms, &ctrl).await {
            Ok(resp) => {
                let rtt = now_ms() - t0;
                match resp.status() {
                    200 => {
                        // Full content: the file changed (or first attempt).
                        etag = response_etag(&resp).unwrap_or(etag);
                        let outcome =
                            stream_download(&resp, file, callbacks, control, tune, 0, &etag, config.timeout_ms, &ctrl)
                                .await?;
                        if tune.borrow().enabled() {
                            tune_tick(tune, control, control.done_bytes(), Some(rtt), false);
                        }
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
                            tune,
                            start,
                            &etag,
                            config.timeout_ms,
                            &ctrl,
                        )
                        .await?;
                        if tune.borrow().enabled() {
                            tune_tick(tune, control, control.done_bytes(), Some(rtt), false);
                        }
                        return finish_download(file, callbacks, etag, outcome).await;
                    }
                    416 => {
                        // Offset beyond EOF → the file shrank; restart cleanly.
                        offset = 0;
                        attempts = 0;
                        continue;
                    }
                    code => return Err(LibfwError::Http { status: code, url }),
                }
            }
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
                if tune.borrow().enabled() {
                    tune_tick(tune, control, control.done_bytes(), None, true);
                }
                sleep_ms(config.backoff_ms(attempts)).await;
            }
        }
    }
}

/// Whether `file` should use the chunked byte-range download path.
///
/// Any transfer with enough remaining bytes to amortise the per-request
/// overhead uses it, even when the (tuned) window is 1: the loop re-reads
/// `download_window`/`chunk_size` before every batch, so a ramp that grows
/// them mid-file takes effect immediately. Small files and short tails stay
/// on the plain sequential path, where one request wins.
fn should_chunked(size: u64, resume_offset: u64) -> bool {
    size >= MIN_PARALLEL_DOWNLOAD_BYTES
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
    let headers = auth_headers(token, false, None)?;
    let url = file_url(base_url, path);
    let (req, ctrl) = request(&url, "HEAD", &headers, None)?;
    let resp = fetch(&req, timeout_ms, &ctrl).await?;
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
    level: i32,
) -> Result<(Vec<u8>, f64), LibfwError> {
    let mut attempts = 0u32;
    loop {
        control.wait_ready().await?;
        control.check()?;
        // Bytes this *attempt* has already reported as progress. A failed
        // attempt rolls them back (see below) so a retry cannot inflate the
        // bar, while a successful one leaves them counted — `collect_chunk`
        // reports as the body streams, which is what keeps the bar moving
        // while a range GET is still in flight.
        let counted = std::rc::Rc::new(std::cell::Cell::new(0u64));
        match download_chunk_once(
            base_url,
            token,
            path,
            etag,
            start,
            end,
            config.compress,
            Some(level),
            config.timeout_ms,
            control.semaphore(),
            control,
            callbacks,
            &counted,
        )
        .await
        {
            Ok((data, rtt)) => {
                if data.len() as u64 != end - start {
                    return Err(LibfwError::Protocol(format!(
                        "chunk {start}..{end} of `{path}` yielded {} bytes, expected {}",
                        data.len(),
                        end - start
                    )));
                }
                // Top up the (usually tiny) tail the streaming loop did not
                // report — the decompressor's final flush — so each successful
                // chunk contributes exactly its length and the bar reaches
                // 100 % without double counting.
                let reported = counted.get();
                let len = data.len() as u64;
                if reported < len {
                    control.add_progress(len - reported);
                    control.report_progress_if(callbacks)?;
                }
                return Ok((data, rtt));
            }
            Err(e) if is_restart_err(&e) => return Err(e),
            Err(e) => {
                // Undo the partial progress this attempt reported: those bytes
                // are not on disk (and will be re-fetched).
                let rollback = counted.get();
                if rollback > 0 {
                    control.subtract_progress(rollback);
                }
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
/// response body into one `Vec<u8>`. Returns the chunk plus its TTFB (ms)
/// for the tuning engine's RTT EWMA.
///
/// `semaphore` is the engine-wide in-flight HTTP pool (sized by
/// `concurrency`): every range GET takes a permit so `concurrency` bounds
/// the TOTAL number of parallel transfers, not just concurrent files.
async fn download_chunk_once(
    base_url: &str,
    token: &str,
    path: &str,
    etag: &str,
    start: u64,
    end: u64,
    compress: bool,
    level: Option<i32>,
    timeout_ms: u32,
    semaphore: &Semaphore,
    control: &TaskControl,
    callbacks: &Callbacks,
    counted: &std::rc::Rc<std::cell::Cell<u64>>,
) -> Result<(Vec<u8>, f64), LibfwError> {
    let headers = auth_headers(token, compress, level)?;
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
    let (req, ctrl) = request(&url, "GET", &headers, None)?;
    // Hold the permit for the whole request so the global cap is respected.
    let _permit = semaphore.acquire().await;
    let t0 = now_ms();
    let resp = fetch(&req, timeout_ms, &ctrl).await?;
    let rtt = now_ms() - t0;
    match resp.status() {
        206 => Ok((
            collect_chunk(&resp, timeout_ms, &ctrl, control, callbacks, counted).await?,
            rtt,
        )),
        // Full body despite a Range + If-Range → the file changed; 416 → it
        // shrank. Both mean "restart from byte 0" (handled by the caller).
        code => Err(LibfwError::Http {
            status: code,
            url,
        }),
    }
}

/// Stream a `206` response body into one decompressed `Vec<u8>`, reporting
/// progress **as the body arrives** (one event per decompressed slice).
///
/// Reporting per received slice — instead of only once the whole range has
/// been buffered — is what makes the bar move while a fetch is still in
/// flight: a single range GET can be many MiB, and on a slow link waiting for
/// it to complete looked like "progress is frozen". `counted` accumulates the
/// bytes this attempt reported so a failed attempt can roll them back.
async fn collect_chunk(
    resp: &Response,
    timeout_ms: u32,
    ctrl: &web_sys::AbortController,
    control: &TaskControl,
    callbacks: &Callbacks,
    counted: &std::rc::Rc<std::cell::Cell<u64>>,
) -> Result<Vec<u8>, LibfwError> {
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
    let collected = Rc::new(RefCell::new(Vec::new()));

    stream_body(
        resp,
        timeout_ms,
        ctrl,
        {
            let decomp = decomp.clone();
            let collected = collected.clone();
            let control = control.clone();
            let callbacks = callbacks.clone();
            let counted = counted.clone();
            move |chunk| {
                let decomp = decomp.clone();
                let collected = collected.clone();
                let control = control.clone();
                let callbacks = callbacks.clone();
                let counted = counted.clone();
                async move {
                    // Decompress straight into the collector — no intermediate
                    // buffer or extra copy per chunk.
                    let before = collected.borrow().len() as u64;
                    decomp
                        .borrow_mut()
                        .decompress(&chunk, &mut collected.borrow_mut())
                        .map_err(|e| LibfwError::Decompress(e.to_string()))?;
                    // Report the *decompressed* growth so the counter matches
                    // what lands on disk (a compressed body would otherwise
                    // scale progress by the compression ratio).
                    let delta = (collected.borrow().len() as u64).saturating_sub(before);
                    if delta > 0 {
                        counted.set(counted.get().saturating_add(delta));
                        control.add_progress(delta);
                        control.report_progress_if(&callbacks)?;
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
        .finish(&mut collected.borrow_mut())
        .map_err(|e| LibfwError::Decompress(e.to_string()))?;
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
    tune: &TuneHandle,
    level: i32,
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

    // Live, per-batch parameter reads: the tuning engine may have raised the
    // window or the chunk size since the previous batch, and a download must
    // act on that immediately (this is what makes "ramp up like TCP" visible
    // within a single file instead of only for the next transfer).
    let mut contiguous = start;
    let mut last_saved = 0u64;

    while contiguous < size {
        control.wait_ready().await?;
        control.check()?;
        let window = tune.borrow().params().download_window.max(1);
        let chunk_size = tune.borrow().params().chunk_size.max(1);
        // One batch = the next `window` chunks (never planning the whole file
        // up front, so the ramp can change the shape mid-transfer).
        let batch: Vec<(u64, u64)> = chunk_batch(contiguous, size, chunk_size, window);

        let results: Vec<Result<(u64, Vec<u8>, f64), LibfwError>> = {
            let etag = meta_etag.clone();
            let path = file.path.clone();
            futures::stream::iter(batch.iter().map(|&(s, e)| {
                let base_url = base_url.to_string();
                let token = token.to_string();
                let path = path.clone();
                let etag = etag.clone();
                let callbacks = callbacks.clone();
                let control = control.clone();
                let config = config.clone();
                async move {
                    let (data, rtt) = download_chunk_with_retry(
                        &base_url, &token, &path, &etag, s, e, &callbacks, &control, &config, level,
                    )
                    .await?;
                    Ok::<_, LibfwError>((s, data, rtt))
                }
            }))
            .buffer_unordered(window)
            .collect()
            .await
        };

        // A batch that lost a chunk permanently ends the file's attempt: feed
        // the engine so it shrinks the parameters, and let the caller decide
        // (retry / restart) with the contiguous offset already persisted.
        let mut ordered: BTreeMap<u64, (Vec<u8>, f64)> = BTreeMap::new();
        for result in results {
            match result {
                Ok((s, data, rtt)) => {
                    ordered.insert(s, (data, rtt));
                }
                Err(e) => {
                    if tune.borrow().enabled() {
                        tune_tick(tune, control, control.done_bytes(), None, true);
                    }
                    return Err(e);
                }
            }
        }

        // Emit strictly in order (append-safe for the SDK's writable).
        for (chunk_start, (data, rtt)) in ordered {
            if chunk_start != contiguous {
                return Err(LibfwError::Protocol(format!(
                    "download of `{}` lost its contiguous offset ({contiguous} != {chunk_start})",
                    file.path
                )));
            }
            callbacks.on_write_chunk(&file.path, contiguous, &data).await?;
            let len = data.len() as u64;
            contiguous = contiguous.saturating_add(len);
            // The bytes were already counted (and reported) while the chunk's
            // body streamed in — `collect_chunk` reports per read slice — so
            // progress stayed live during the fetch instead of jumping once
            // the whole range landed. Here we only persist the absolute
            // contiguous offset periodically, so a crash mid-transfer can
            // resume from disk's real end.
            if contiguous >= last_saved.saturating_add(RESUME_SAVE_EVERY) {
                last_saved = contiguous;
                let _ = callbacks
                    .save_state("download", &file.path, &resume_state_obj(&meta_etag, contiguous))
                    .await;
            }
            // Feed the tuning engine: one measurement per emitted chunk
            // (coalesced into 1 s windows by the engine, so this is cheap).
            if tune.borrow().enabled() {
                tune_tick(tune, control, control.done_bytes(), Some(rtt), false);
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

/// At most `window` consecutive `[start, end)` chunks covering `[from, size)`.
///
/// Computed on demand rather than planned up front so a long download can
/// re-read the tuned chunk size between batches, and so a 10 GiB file never
/// materialises its whole chunk list.
fn chunk_batch(from: u64, size: u64, chunk_size: u64, window: usize) -> Vec<(u64, u64)> {
    // Defensive: a 0 chunk size (which config parsing prevents) falls back to
    // the protocol default rather than degenerating into 1-byte chunks.
    let chunk_size = if chunk_size == 0 {
        libfw_core::CHUNK_SIZE
    } else {
        chunk_size
    };
    let mut out = Vec::with_capacity(window.min(1024));
    let mut offset = from.min(size);
    for _ in 0..window {
        if offset >= size {
            break;
        }
        let end = (offset + chunk_size).min(size);
        out.push((offset, end));
        offset = end;
    }
    out
}

/// The contiguous `[start, end)` chunks covering `[from, size)` at
/// `chunk_size`, starting at `from` (a resume offset).
fn parallel_chunks(from: u64, size: u64, chunk_size: u64) -> Vec<(u64, u64)> {
    chunk_batch(from, size, chunk_size, usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_batch_stops_at_the_window() {
        // At most `window` chunks, always starting exactly at `from`.
        assert_eq!(chunk_batch(0, 100, 10, 3), vec![(0, 10), (10, 20), (20, 30)]);
        // The last chunk of the file is clamped to `size`.
        assert_eq!(chunk_batch(95, 100, 10, 3), vec![(95, 100)]);
        // Starting at EOF yields nothing (no empty chunk).
        assert!(chunk_batch(100, 100, 10, 3).is_empty());
        // A window of 1 is the sequential case.
        assert_eq!(chunk_batch(0, 100, 10, 1), vec![(0, 10)]);
    }

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
    fn should_chunked_needs_a_large_remaining_transfer() {
        // A large file uses the chunked path even when the tuned window is 1:
        // the loop re-reads the window/chunk size before every batch, so the
        // ramp can widen it mid-file.
        assert!(should_chunked(10 * 1024 * 1024, 0));
        // Small file → one plain sequential request wins.
        assert!(!should_chunked(64 * 1024, 0));
        // Large file, only a tiny tail left → sequential (avoid per-request
        // overhead on the last few bytes).
        assert!(!should_chunked(10 * 1024 * 1024, 10 * 1024 * 1024 - 1024));
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
    tune: &TuneHandle,
    start: u64,
    etag: &str,
    timeout_ms: u32,
    ctrl: &web_sys::AbortController,
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
    let tune = tune.clone();
    let path = file.path.clone();
    let etag = etag.to_string();
    let final_size = file.size;

    stream_body(
        resp,
        timeout_ms,
        ctrl,
        |chunk| {
            let decomp = decomp.clone();
            let out = out.clone();
            let file_offset = file_offset.clone();
            let last_saved = last_saved.clone();
            let callbacks = callbacks.clone();
            let control = control.clone();
            let tune = tune.clone();
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

                    // Feed the tuning engine per read: a single-request
                    // download must still produce 1-second measurement
                    // windows, otherwise a slow link never ramps (and a short
                    // file never contributes to a settle).
                    // No fresh RTT sample here (one request = one TTFB).
                    if tune.borrow().enabled() {
                        tune_tick(&tune, &control, control.done_bytes(), None, false);
                    }

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
// M3 tuning plumbing (tune handle + negotiated level) pushed this past the
// 7-arg lint; grouping them would churn every call site for no gain.
#[allow(clippy::too_many_arguments)]
pub async fn download_folder(
    base_url: &str,
    token: &str,
    path: &str,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
    tune: &TuneHandle,
    level: i32,
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
        let tune = tune.clone();
        async move {
            download_file(
                &base_url, &token, &file, &callbacks, &control, &config, &tune, level,
            )
            .await
        }
    }))
    .buffer_unordered(tune.borrow().params().concurrency);

    while let Some(result) = stream.next().await {
        match result {
            Ok(_) => {}
            Err(e) => {
                if tune.borrow().enabled() {
                    tune_tick(tune, control, control.done_bytes(), None, true);
                }
                return Err(e);
            }
        }
        // Report progress from the shared control block so pause/resume and
        // the onProgress events stay consistent (one source of truth).
        callbacks.on_progress(control.done_bytes(), control.total_bytes())?;
    }
    Ok(control.done_bytes())
}

/// Download a single file at `path` (size/etag discovered from the server).
// M3 tuning plumbing (tune handle + negotiated level) pushed this past the
// 7-arg lint; grouping them would churn every call site for no gain.
#[allow(clippy::too_many_arguments)]
pub async fn download_single(
    base_url: &str,
    token: &str,
    path: &str,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
    tune: &TuneHandle,
    level: i32,
) -> Result<u64, LibfwError> {
    // Discover the authoritative size/etag via HEAD (the server is the
    // source of truth), so a large single file can use the tus-style
    // parallel byte-range path instead of a single slow connection.
    let (_etag, size) = fetch_meta(base_url, token, path, config.timeout_ms).await?;
    // Seed the total up front so every progress event for a single-file
    // download has a real denominator (the parallel/sequential paths also
    // set it, but the already-done/early paths and the first chunk would
    // otherwise report `bytes/0`).
    control.set_total(size);
    let file = FileEntry {
        path: path.to_string(),
        size,
        mtime: 0,
    };
    let outcome =
        download_file(base_url, token, &file, callbacks, control, config, tune, level).await?;
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
