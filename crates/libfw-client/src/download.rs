//! Download scheduler: recursive folder listing, per-file streaming
//! downloads with Range/If-Range resume, exponential-backoff retries and
//! bounded-memory zrip decompression.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use js_sys::Reflect;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use web_sys::Response;

use futures::StreamExt;
use libfw_core::compress::{decompressor, CompressionFormat};
use libfw_core::storage::DirEntry;
use libfw_core::HEADER_COMPRESS;

use crate::config::ClientConfig;
use crate::error::LibfwError;
use crate::http::{auth_headers, dir_url, fetch, file_url, read_all, request, stream_body};
use crate::js::Callbacks;
use crate::plan::{total_bytes, FileEntry};
use crate::state::TaskControl;

/// A single file download outcome, used for resume-state bookkeeping.
struct DownloadOutcome {
    /// Final size observed from the server.
    size: u64,
    /// Bytes actually written (decompressed).
    written: u64,
}

/// List the immediate children of a virtual directory via `GET /dir/..`.
async fn list_dir(base_url: &str, token: &str, path: &str) -> Result<Vec<DirEntry>, LibfwError> {
    let headers = auth_headers(token, false)?;
    let url = dir_url(base_url, path);
    let req = request(&url, "GET", &headers, None)?;
    let resp = fetch(&req).await?;
    let status = resp.status();
    if status != 200 {
        return Err(LibfwError::Http { status, url });
    }
    let body = read_all(&resp).await?;
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
) -> Result<Vec<FileEntry>, LibfwError> {
    let mut out = Vec::new();
    let mut stack = vec![path.to_string()];
    while let Some(dir) = stack.pop() {
        for entry in list_dir(base_url, token, &dir).await? {
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

    loop {
        control.wait_ready().await?;
        control.check()?;

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

        match fetch(&req).await {
            Ok(resp) => match resp.status() {
                200 => {
                    // Full content: the file changed (or first attempt).
                    etag = response_etag(&resp).unwrap_or(etag);
                    let outcome = stream_download(&resp, file, callbacks, control, 0).await?;
                    return finish_download(file, callbacks, etag, outcome).await;
                }
                206 => {
                    if etag.is_empty() {
                        etag = response_etag(&resp).unwrap_or_default();
                    }
                    let start = content_range_start(&resp).unwrap_or(offset);
                    let outcome = stream_download(&resp, file, callbacks, control, start).await?;
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

/// Stream a `200`/`206` response body, decompressing on the fly and
/// pushing chunks to JS. `start` is the byte offset the body begins at.
async fn stream_download(
    resp: &Response,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    start: u64,
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

    // State is shared via `Rc` so the per-chunk `FnMut` callback can move
    // owned clones into its `async move` block (single-threaded WASM).
    let decomp = Rc::new(RefCell::new(decompressor(format)));
    let out = Rc::new(RefCell::new(Vec::new()));
    let file_offset = Rc::new(Cell::new(start));
    let stream_written = Rc::new(Cell::new(0u64));
    let callbacks = callbacks.clone();
    let control = control.clone();
    let path = file.path.clone();
    let final_size = file.size;

    stream_body(resp, |chunk| {
        let decomp = decomp.clone();
        let out = out.clone();
        let file_offset = file_offset.clone();
        let stream_written = stream_written.clone();
        let callbacks = callbacks.clone();
        let control = control.clone();
        let path = path.clone();
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
                stream_written.set(stream_written.get().saturating_add(data.len() as u64));
                control.add_progress(data.len() as u64);
            }
            Ok(())
        }
    })
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
        stream_written.set(stream_written.get().saturating_add(tail.len() as u64));
        control.add_progress(tail.len() as u64);
    }

    let written_total = stream_written.get();
    Ok(DownloadOutcome {
        size: final_size.max(file_offset.get()),
        written: written_total,
    })
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
    let files = collect_files(base_url, token, path).await?;
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
    let file = FileEntry {
        path: path.to_string(),
        size: 0,
        mtime: 0,
    };
    let outcome = download_file(base_url, token, &file, callbacks, control, config).await?;
    Ok(outcome.written)
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
