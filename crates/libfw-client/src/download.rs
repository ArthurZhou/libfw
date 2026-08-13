//! Download scheduler over the WebSocket block transport.
//!
//! Download uses the **same** block protocol as upload: the server (sender)
//! pipelines fixed-size blocks without per-block acknowledgments, and the
//! browser (receiver) verifies every block (CRC32 + bounds) in real time,
//! marks bad blocks with `FRAME_NAK` and asks the sender to re-queue them.
//! A wave boundary reconciles: the receiver either completes (`FRAME_COMPLETE`)
//! or re-requests the missing blocks (`FRAME_REQ`).
//!
//! A folder download lists the tree over one short-lived control connection,
//! then transfers each file over its own connection (so `concurrency` files
//! run in parallel, exactly like the old HTTP path). The receiver reorders
//! out-of-order blocks in memory and hands them to the SDK **strictly in
//! order** so the append-mode writable stays correct.

use std::collections::BTreeMap;

use futures::StreamExt;
use libfw_core::compress::{CompressionFormat, decompressor};
use libfw_core::storage::DirEntry;
use libfw_core::ws::*;
use wasm_bindgen::{JsCast, JsValue};

use crate::config::ClientConfig;
use crate::error::LibfwError;
use crate::js::Callbacks;
use crate::plan::{total_bytes, FileEntry};
use crate::state::TaskControl;
use crate::ws::{parse_error, WsConnection, WsPool};

/// A single file download outcome, used for resume-state bookkeeping.
struct DownloadOutcome {
    /// Final absolute offset observed after a successful transfer.
    size: u64,
}

/// Persist download progress roughly every this many bytes so an
/// interrupted transfer can resume from a recent offset instead of
/// restarting from byte 0.
const RESUME_SAVE_EVERY: u64 = 4 * 1024 * 1024;

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

/// Recursively collect every file under `path` (server-side walk via WS).
///
/// Iterative with an explicit stack so deep directory trees neither blow
/// the compiler recursion limit nor the WASM call stack.
async fn collect_files(conn: &WsConnection, path: &str) -> Result<Vec<FileEntry>, LibfwError> {
    let mut out = Vec::new();
    let mut stack = vec![path.to_string()];
    while let Some(dir) = stack.pop() {
        let entries: Vec<DirEntry> = conn.list_dir(&dir).await?;
        for entry in entries {
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

/// Decompress one independent zrip frame into raw bytes.
fn decompress_block(data: &[u8]) -> Result<Vec<u8>, LibfwError> {
    let mut dec = decompressor(CompressionFormat::Zrip);
    let mut out = Vec::new();
    dec.decompress(data, &mut out)
        .map_err(|e| LibfwError::Decompress(e.to_string()))?;
    dec.finish(&mut out)
        .map_err(|e| LibfwError::Decompress(e.to_string()))?;
    Ok(out)
}

/// Download one file with resume + retry.
///
/// Each attempt checks a WebSocket connection out of the shared [`WsPool`]
/// (opening one if the pool is empty) and hands it back when the transfer
/// finishes, so a folder transfer reuses connections across files instead of
/// opening/closing one per file. Resume state `{etag, offset}` is validated
/// against the server's authoritative ETag/size in `download_once`: a changed
/// file restarts from byte 0.
async fn download_file(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
    pool: &WsPool,
) -> Result<DownloadOutcome, LibfwError> {
    callbacks.on_file_start(&file.path, file.size)?;

    // 1. Load persisted resume state: { etag, offset }.
    let mut resume: Option<(String, u64)> = None;
    if let Some(state) = callbacks.load_state("download", &file.path).await? {
        let etag = js_sys::Reflect::get(&state, &JsValue::from_str("etag"))
            .ok()
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        let offset = js_sys::Reflect::get(&state, &JsValue::from_str("offset"))
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

        let conn = match pool
            .checkout(base_url, token, config.timeout_ms, config.ws_url.as_deref())
            .await
        {
            Ok(c) => c,
            Err(e) => {
                if attempts >= config.max_retries {
                    return Err(e);
                }
                attempts += 1;
                callbacks.log(&format!(
                    "reconnecting for `{}` (attempt {attempts}): {e}",
                    file.path
                ));
                sleep_ms(config.backoff_ms(attempts)).await;
                continue;
            }
        };
        let result = download_once(&conn, file, &etag, offset, callbacks, control, config).await;
        match result {
            // Persist the AUTHORITATIVE ETag learned from the server; the
            // connection is still healthy so hand it back to the pool.
            Ok((meta_etag, outcome)) => {
                pool.checkin(conn);
                return finish_download(file, callbacks, meta_etag, outcome).await;
            }
            // The remote file changed / shrank → restart from byte 0. The
            // connection is fine; reuse it.
            Err(e) if is_restart_err(&e) => {
                pool.checkin(conn);
                offset = 0;
                attempts = 0;
                etag = String::new();
                continue;
            }
            Err(e) => {
                // Network/protocol error: the connection may be in a broken
                // state, so drop it rather than reuse it.
                drop(conn);
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

/// A download error that means "the remote file changed / shrank — restart
/// from byte 0".
fn is_restart_err(e: &LibfwError) -> bool {
    matches!(e, LibfwError::Http { status: 416 | 200, .. })
}

/// One download attempt over an open connection.
///
/// Sends `FRAME_START` (download), awaits `FRAME_READY` (authoritative
/// size/etag), validates the resume offset, then runs the receiver role.
/// Returns `(etag, outcome)` where `etag` is the server's authoritative one.
async fn download_once(
    conn: &WsConnection,
    file: &FileEntry,
    resume_etag: &str,
    offset: u64,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<(String, DownloadOutcome), LibfwError> {
    let start = StartRequest {
        kind: TransferKind::Download,
        path: file.path.clone(),
        size: 0,
        mtime: 0,
        etag: String::new(),
        compress: config.compress,
        mode: String::new(),
        offset,
        block_size: config.download_chunk_size,
        window: config.download_window.max(1) as u32,
    };
    conn.send(&control_frame(FRAME_START, &start))?;

    let ready = loop {
        control.check()?;
        let frame = conn.next().await?;
        match frame_type(&frame) {
            Some(FRAME_READY) => {
                break parse_control::<ReadyReply>(&frame, FRAME_READY)
                    .ok_or_else(|| LibfwError::Protocol("bad READY frame".into()))?;
            }
            Some(FRAME_ERROR) => {
                return Err(parse_error(&frame)
                    .unwrap_or_else(|| LibfwError::Protocol("download start failed".into())));
            }
            _ => {}
        }
    };

    // Revalidate the resume offset against the server (source of truth): a
    // changed file (ETag mismatch) or a shrunk file restarts from byte 0.
    if (!resume_etag.is_empty() && resume_etag != ready.etag) || offset > ready.size {
        return Err(LibfwError::Http {
            status: 416,
            url: file.path.clone(),
        });
    }

    if control.total_bytes() == 0 {
        control.set_total(ready.size.max(file.size));
    }
    if ready.offset > 0 {
        // Seed progress with the contiguous bytes already on disk so a
        // resume reflects the true fraction.
        control.add_progress(ready.offset);
        control.report_progress_if(callbacks)?;
    }

    let outcome = receive_download(conn, file, callbacks, control, &ready).await?;
    Ok((ready.etag.clone(), outcome))
}

/// The download receiver role: verify every block, reorder in memory, hand
/// bytes to the SDK in order, and drive retransmission of bad/missing blocks.
async fn receive_download(
    conn: &WsConnection,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    ready: &ReadyReply,
) -> Result<DownloadOutcome, LibfwError> {
    let start_off = ready.offset;
    let block_size = ready.block_size.max(1);
    let total_blocks = ready.total_blocks;
    let compress = ready.compress;

    let mut verified = BlockSet::new(total_blocks);
    // Reorder buffer keyed by block index → we can accept out-of-order blocks.
    let mut buffer: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    let mut contiguous_index: u32 = 0;
    let mut last_saved = 0u64;

    loop {
        control.wait_ready().await?;
        control.check()?;
        let frame = conn.next().await?;
        match frame_type(&frame) {
            Some(FRAME_BLOCK) => {
                let Some(block) = parse_block(&frame) else {
                    continue;
                };
                if block.index >= total_blocks || verified.contains(block.index) {
                    continue; // out of range or duplicate → idempotent no-op
                }
                // Real-time verification: CRC + length + bounds.
                let crc_ok = crc32(&block.data) == block.crc;
                let raw: Vec<u8> = if compress {
                    match decompress_block(&block.data) {
                        Ok(d) => d,
                        Err(_) => {
                            // Mark bad → sender re-adds to its queue.
                            conn.send(&nak_frame(block.index))?;
                            continue;
                        }
                    }
                } else {
                    block.data
                };
                let len_ok = !compress || raw.len() as u32 == block.raw_len;
                let abs = block_offset(block.index, block_size, start_off);
                let in_bounds = abs.saturating_add(raw.len() as u64) <= ready.size;
                if !(crc_ok && len_ok && in_bounds) {
                    conn.send(&nak_frame(block.index))?;
                    continue;
                }
                verified.insert(block.index);
                buffer.insert(block.index, raw);

                // Emit strictly in order (append-safe for the SDK writable).
                while let Some(data) = buffer.remove(&contiguous_index) {
                    let abs = block_offset(contiguous_index, block_size, start_off);
                    callbacks.on_write_chunk(&file.path, abs, &data).await?;
                    control.add_progress(data.len() as u64);
                    control.report_progress_if(callbacks)?;
                    contiguous_index += 1;
                    let abs_done = block_offset(contiguous_index, block_size, start_off);
                    if abs_done >= last_saved.saturating_add(RESUME_SAVE_EVERY) {
                        last_saved = abs_done;
                        let _ = callbacks
                            .save_state(
                                "download",
                                &file.path,
                                &resume_state_obj(&ready.etag, abs_done),
                            )
                            .await;
                    }
                }

                if verified.count() == total_blocks {
                    // All verified → flush any remaining (out-of-order) tail,
                    // then complete. The final size is the server's
                    // authoritative `ready.size` — NOT the flush return value,
                    // which is 0 when every block already arrived in order.
                    flush_remaining(
                        &mut buffer,
                        &mut contiguous_index,
                        file,
                        callbacks,
                        control,
                        block_size,
                        start_off,
                    )
                    .await?;
                    let final_size = ready.size;
                    conn.send(&control_frame(
                        FRAME_COMPLETE,
                        &CompleteMessage::ok(final_size),
                    ))?;
                    return Ok(DownloadOutcome { size: final_size });
                }
            }
            Some(FRAME_WAVE_DONE) => {
                // Reconciliation: everything verified → complete, else ask
                // the sender to re-send the missing blocks (its queue).
                let missing = verified.missing();
                if missing.is_empty() {
                    flush_remaining(
                        &mut buffer,
                        &mut contiguous_index,
                        file,
                        callbacks,
                        control,
                        block_size,
                        start_off,
                    )
                    .await?;
                    let final_size = ready.size;
                    conn.send(&control_frame(
                        FRAME_COMPLETE,
                        &CompleteMessage::ok(final_size),
                    ))?;
                    return Ok(DownloadOutcome { size: final_size });
                }
                conn.send(&req_frame(&missing))?;
            }
            Some(FRAME_COMPLETE) => {
                // The server (sender) never completes a download; if it does
                // something went wrong.
                return Err(LibfwError::Protocol(
                    "server ended the download stream unexpectedly".into(),
                ));
            }
            Some(FRAME_ERROR) => {
                return Err(parse_error(&frame)
                    .unwrap_or_else(|| LibfwError::Protocol("download error".into())));
            }
            _ => {}
        }
    }
}

/// Write every buffered (verified) block in ascending order — the tail after
/// the contiguous prefix — returning the final absolute size.
async fn flush_remaining(
    buffer: &mut BTreeMap<u32, Vec<u8>>,
    contiguous_index: &mut u32,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    block_size: u64,
    start_off: u64,
) -> Result<u64, LibfwError> {
    let mut last_abs = 0u64;
    let tail: Vec<(u32, Vec<u8>)> = std::mem::take(buffer).into_iter().collect();
    for (index, data) in tail {
        let abs = block_offset(index, block_size, start_off);
        callbacks.on_write_chunk(&file.path, abs, &data).await?;
        control.add_progress(data.len() as u64);
        control.report_progress_if(callbacks)?;
        last_abs = abs.saturating_add(data.len() as u64);
        *contiguous_index = index.saturating_add(1);
    }
    Ok(last_abs)
}

/// Build a resume-state object for JS persistence.
fn resume_state_obj(etag: &str, offset: u64) -> JsValue {
    let state = js_sys::Object::new();
    let _ = js_sys::Reflect::set(&state, &JsValue::from_str("etag"), &JsValue::from_str(etag));
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
    // Persist the ABSOLUTE end offset (`size`), not a per-request delta, so
    // a later resume re-requests the correct byte range.
    let offset = outcome.size;
    let size = outcome.size;
    let state = js_sys::Object::new();
    js_sys::Reflect::set(
        &state,
        &JsValue::from_str("etag"),
        &JsValue::from_str(&etag),
    )
    .map_err(|e| LibfwError::Js(format!("state etag: {e:?}")))?;
    js_sys::Reflect::set(
        &state,
        &JsValue::from_str("offset"),
        &JsValue::from_f64(offset as f64),
    )
    .map_err(|e| LibfwError::Js(format!("state offset: {e:?}")))?;
    js_sys::Reflect::set(
        &state,
        &JsValue::from_str("size"),
        &JsValue::from_f64(size as f64),
    )
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
    // One pool shared by the listing connection and every file transfer, so
    // the whole folder reuses a handful of WebSocket connections instead of
    // opening/closing one per file.
    let pool = WsPool::new();

    // 1. List the tree (reusing a pooled connection).
    let listing_conn = match pool
        .checkout(base_url, token, config.timeout_ms, config.ws_url.as_deref())
        .await
    {
        Ok(c) => c,
        Err(e) => return Err(e),
    };
    let files = match collect_files(&listing_conn, path).await {
        Ok(files) => files,
        Err(e) => {
            drop(listing_conn);
            return Err(e);
        }
    };
    pool.checkin(listing_conn);

    let total = total_bytes(&files);
    control.set_total(total);
    callbacks.on_progress(0, total)?;

    // 2. Transfer `concurrency` files at a time, each checking a connection
    //    out of the shared pool and handing it back when the file finishes.
    let mut stream = futures::stream::iter(files.into_iter().map(|file| {
        let base_url = base_url.to_string();
        let token = token.to_string();
        let callbacks = callbacks.clone();
        let control = control.clone();
        let config = config.clone();
        let pool = pool.clone();
        async move {
            download_file(
                &base_url, &token, &file, &callbacks, &control, &config, &pool,
            )
            .await
        }
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
    // A small shared pool (the metadata probe and the transfer may reuse the
    // same connection).
    let pool = WsPool::new();

    // Discover the authoritative size/etag over WS (server is the source of
    // truth) so `on_file_start` reports a real size.
    let meta_conn = match pool
        .checkout(base_url, token, config.timeout_ms, config.ws_url.as_deref())
        .await
    {
        Ok(c) => c,
        Err(e) => return Err(e),
    };
    let (_, size) = match meta_conn.file_meta(path).await {
        Ok(meta) => meta,
        Err(e) => {
            drop(meta_conn);
            return Err(e);
        }
    };
    pool.checkin(meta_conn);

    let file = FileEntry {
        path: path.to_string(),
        size,
        mtime: 0,
    };
    let outcome =
        download_file(base_url, token, &file, callbacks, control, config, &pool).await?;
    // Return the ABSOLUTE byte count (the final offset), consistent with
    // `download_folder`.
    Ok(outcome.size)
}
