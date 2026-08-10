//! Upload scheduler over the WebSocket block transport.
//!
//! Upload uses the **same** block protocol as download: the browser (sender)
//! pipelines fixed-size blocks without per-block acknowledgments, and the
//! server (receiver) verifies every block (CRC32 + bounds) in real time,
//! NAKs bad ones and asks the sender to re-add them to its transfer queue.
//! A wave boundary reconciles: the server either commits (`FRAME_COMPLETE`)
//! or re-requests the missing blocks (`FRAME_REQ`), which the sender re-adds
//! to its queue and re-sends.
//!
//! Uploads are resumable: the server seeds `FRAME_READY.received` with the
//! byte ranges it already holds (a shared per-session temp keyed by the file
//! ETag), so the client seeds progress and only retransmits the missing
//! blocks — BitTorrent-style "only the broken/lost parts".

use std::collections::VecDeque;

use futures::StreamExt;
use libfw_core::compress::{CompressionFormat, compressor};
use libfw_core::ws::*;
use wasm_bindgen::JsValue;

use wasm_bindgen::JsCast;

use crate::config::ClientConfig;
use crate::error::LibfwError;
use crate::js::Callbacks;
use crate::plan::{total_bytes, FileEntry};
use crate::state::TaskControl;
use crate::ws::{parse_error, WsConnection, WsPool};

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

/// Compress `data` into one independent zrip frame.
fn compress_block(data: &[u8]) -> Result<Vec<u8>, LibfwError> {
    let mut enc = compressor(CompressionFormat::Zrip)
        .map_err(|e| LibfwError::Compress(e.to_string()))?;
    let mut out = Vec::with_capacity(data.len());
    enc.compress(data, &mut out)
        .map_err(|e| LibfwError::Compress(e.to_string()))?;
    enc.finish(&mut out)
        .map_err(|e| LibfwError::Compress(e.to_string()))?;
    Ok(out)
}

/// Total number of bytes covered by a set of received byte ranges.
fn covered_bytes(received: &[[u64; 2]]) -> u64 {
    let mut total = 0u64;
    for [s, e] in received {
        total = total.saturating_add(e.saturating_sub(*s));
    }
    total
}

/// One upload attempt over an open connection: send `FRAME_START`, await
/// `FRAME_READY` (resume ranges), then run the sender role until the server
/// commits (`FRAME_COMPLETE`).
async fn upload_once(
    conn: &WsConnection,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<u64, LibfwError> {
    let start = StartRequest {
        kind: TransferKind::Upload,
        path: file.path.clone(),
        size: file.size,
        mtime: file.mtime,
        etag: file.to_meta().etag,
        compress: config.compress,
        mode: "overwrite".into(),
        offset: 0,
        block_size: config.chunk_size,
        window: config.upload_window.max(1) as u32,
    };
    conn.send(&control_frame(FRAME_START, &start))?;

    let ready = loop {
        let frame = conn.next().await?;
        match frame_type(&frame) {
            Some(FRAME_READY) => {
                break parse_control::<ReadyReply>(&frame, FRAME_READY)
                    .ok_or_else(|| LibfwError::Protocol("bad READY frame".into()))?;
            }
            Some(FRAME_ERROR) => {
                return Err(parse_error(&frame)
                    .unwrap_or_else(|| LibfwError::Protocol("upload start failed".into())));
            }
            _ => {}
        }
    };

    let block_size = if ready.block_size > 0 {
        ready.block_size
    } else {
        config.chunk_size.max(1)
    };
    let total_blocks = ready.total_blocks;

    // Seed the verified set + progress from what the server already holds.
    let mut verified = BlockSet::new(total_blocks);
    let received: Vec<(u64, u64)> = ready
        .received
        .iter()
        .map(|[s, e]| (*s, *e))
        .collect();
    verified.seed_from_ranges(block_size, &received);
    let initial_covered = covered_bytes(&ready.received).min(file.size);
    if initial_covered > 0 {
        control.add_progress(initial_covered);
        control.report_progress_if(callbacks)?;
    }

    // The transfer queue: only the blocks the server still misses. NAK/REQ
    // re-add bad blocks to this queue for retransmission.
    let mut queue: VecDeque<u32> = verified.missing().into_iter().collect();
    let window = config.upload_window.max(1);

    loop {
        control.wait_ready().await?;
        control.check()?;

        // 1. Pipeline one wave of blocks (no per-block ack; out of order OK).
        let mut sent = 0usize;
        while sent < window {
            let Some(idx) = queue.pop_front() else {
                break;
            };
            let (s, e) = block_bounds(idx, block_size, file.size);
            let len = e - s;
            let raw = callbacks.read_file(&file.path, s, len).await?;
            if raw.len() as u64 != len {
                return Err(LibfwError::Storage(format!(
                    "read {} of {} bytes for `{}`",
                    raw.len(),
                    len,
                    file.path
                )));
            }
            let raw_len = raw.len() as u32;
            let payload: Vec<u8> = if config.compress {
                compress_block(&raw)?
            } else {
                raw
            };
            let crc = crc32(&payload);
            conn.send(&block_frame(idx, crc, raw_len, &payload))?;
            control.add_progress(len);
            control.report_progress_if(callbacks)?;
            sent += 1;
        }

        // 2. Wave boundary: the receiver reconciles.
        conn.send(&wave_done_frame())?;

        // 3. Read events until the server asks for more (REQ) or finished
        //    (COMPLETE). NAKs re-queue immediately (实时核验 → 重传队列).
        loop {
            let frame = conn.next().await?;
            match frame_type(&frame) {
                Some(FRAME_NAK) => {
                    if let Some(idx) = parse_nak(&frame) {
                        queue.push_back(idx);
                    }
                    // Keep reading: the server may NAK several blocks.
                }
                Some(FRAME_REQ) => {
                    if let Some(indices) = parse_req(&frame) {
                        queue.extend(indices);
                    }
                    break; // next wave
                }
                Some(FRAME_COMPLETE) => {
                    let msg: CompleteMessage = parse_control(&frame, FRAME_COMPLETE)
                        .ok_or_else(|| LibfwError::Protocol("bad COMPLETE frame".into()))?;
                    if msg.ok {
                        return Ok(file.size.saturating_sub(initial_covered));
                    }
                    return Err(LibfwError::Protocol(
                        msg.error.unwrap_or_else(|| "upload failed".into()),
                    ));
                }
                Some(FRAME_ERROR) => {
                    return Err(parse_error(&frame)
                        .unwrap_or_else(|| LibfwError::Protocol("upload error".into())));
                }
                _ => {}
            }
        }
    }
}

/// Upload one file with the resumable WebSocket session protocol.
///
/// The connection is checked out of the shared [`WsPool`] (opening one when
/// the pool is empty) and handed back on success, so a multi-file upload
/// reuses connections instead of opening/closing one per file.
async fn upload_file(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
    pool: &WsPool,
) -> Result<u64, LibfwError> {
    callbacks.on_file_start(&file.path, file.size)?;

    let mut attempts = 0u32;
    let uploaded = loop {
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
        let result = upload_once(&conn, file, callbacks, control, config).await;

        match result {
            Ok(uploaded) => {
                // The connection is healthy; reuse it for the next file.
                pool.checkin(conn);
                callbacks
                    .save_state(
                        "upload",
                        &file.path,
                        &state_json(file.size, &file.to_meta().etag, file.size),
                    )
                    .await?;
                break uploaded;
            }
            Err(e) => {
                // Network/protocol error: drop the possibly-broken connection
                // rather than reuse it.
                drop(conn);
                if attempts >= config.max_retries {
                    return Err(e);
                }
                attempts += 1;
                callbacks.log(&format!(
                    "retrying upload of `{}` (attempt {attempts}): {e}",
                    file.path
                ));
                sleep_ms(config.backoff_ms(attempts)).await;
            }
        }
    };

    callbacks.on_file_completed(&file.path).await?;
    Ok(uploaded)
}

/// Build a resume-state object for JS persistence.
fn state_json(offset: u64, etag: &str, size: u64) -> JsValue {
    let state = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &state,
        &JsValue::from_str("offset"),
        &JsValue::from_f64(offset as f64),
    );
    let _ = js_sys::Reflect::set(&state, &JsValue::from_str("etag"), &JsValue::from_str(etag));
    let _ = js_sys::Reflect::set(&state, &JsValue::from_str("size"), &JsValue::from_f64(size as f64));
    state.into()
}

/// Upload every file reported by the JS `getFileList` callback.
pub async fn upload(
    base_url: &str,
    token: &str,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<u64, LibfwError> {
    let files = callbacks.file_list().await?;
    let total = total_bytes(&files);
    control.set_total(total);
    callbacks.on_progress(0, total)?;

    // One pool shared by all files: connections are checked out per file and
    // handed back, so `concurrency` connections are reused across the whole
    // upload instead of one open/close cycle per file.
    let pool = WsPool::new();
    let mut stream = futures::stream::iter(files.into_iter().map(|file| {
        let base_url = base_url.to_string();
        let token = token.to_string();
        let callbacks = callbacks.clone();
        let control = control.clone();
        let config = config.clone();
        let pool = pool.clone();
        async move {
            upload_file(
                &base_url, &token, &file, &callbacks, &control, &config, &pool,
            )
            .await
        }
    }))
    .buffer_unordered(config.concurrency);

    let mut done = 0u64;
    while let Some(result) = stream.next().await {
        done = done.saturating_add(result?);
        // Single source of truth for progress is the shared control block.
        // Clamp the done figure so a rare gap-fill re-send (which re-counts a
        // few bytes) can never show a bar past 100%.
        let total = control.total_bytes();
        let reported_done = if total == 0 {
            control.done_bytes()
        } else {
            control.done_bytes().min(total)
        };
        callbacks.on_progress(reported_done, total)?;
    }
    Ok(done)
}
