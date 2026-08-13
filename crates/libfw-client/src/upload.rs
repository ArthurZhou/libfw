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

/// File bytes newly CONFIRMED by the server between two wave reconciliations.
///
/// Each `FRAME_REQ` from the server lists every block it has **not** yet
/// CRC-verified. The blocks that were in the *previous* missing set but are
/// no longer listed are exactly the ones the server just verified, so folding
/// only those FILE bytes keeps upload progress tied to what the server has
/// actually received and written — not to what merely left the browser's
/// socket (`bufferedAmount` drain races the bar to ~100% on slow links).
fn confirmed_delta(
    prev_missing: &BlockSet,
    new_missing: &BlockSet,
    block_size: u64,
    file_size: u64,
) -> u64 {
    let mut total = 0u64;
    for i in 0..prev_missing.total() {
        if prev_missing.contains(i) && !new_missing.contains(i) {
            let (s, e) = block_bounds(i, block_size, file_size);
            total = total.saturating_add(e.saturating_sub(s));
        }
    }
    total
}

/// Roll the upload stall deadline forward when the connection made wire
/// progress since the last poll, returning whether it rolled.
///
/// `last_wire` is the transmitted-byte count observed on the **previous**
/// poll; `wire` is the current count; `last_activity` is the timestamp (ms
/// since epoch, e.g. `js_sys::Date::now()`) of the last observed activity.
/// The deadline rolls only when `wire` increased since that previous poll.
///
/// This must compare across polls, never two reads inside one poll: the
/// socket cannot drain between synchronous reads, so comparing those would
/// always be equal and the deadline would never roll — a slow link whose
/// wave takes longer than `timeout_ms` to drain would then be misread as a
/// stall and aborted with "ws read timed out" even though bytes are flowing.
fn roll_stall_on_wire(
    last_wire: &mut u64,
    last_activity: &mut f64,
    wire: u64,
    now: f64,
) -> bool {
    if wire > *last_wire {
        *last_wire = wire;
        *last_activity = now;
        true
    } else {
        false
    }
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
        control.check()?;
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

    // Upload progress is keyed to FILE bytes the server has CONFIRMED it
    // CRC-verified and written, not to bytes that merely left the browser's
    // socket. `send()` only queues into the WebSocket send buffer, and
    // counting by `bufferedAmount` drain races the bar to ~100% while a slow
    // link is still pushing bytes to the server. The server confirms a block
    // only by no longer listing it in its `FRAME_REQ` (or by `FRAME_COMPLETE`),
    // so we track the previously-missing set and fold in exactly the bytes
    // newly verified between wave reconciliations (see [`confirmed_delta`]).
    // `confirmed_bytes` is this file's total server-confirmed byte count
    // (resume ranges + deltas), used to force an exact 100% at `FRAME_COMPLETE`.
    let mut prev_missing = BlockSet::new(total_blocks);
    for i in 0..total_blocks {
        if !verified.contains(i) {
            prev_missing.insert(i);
        }
    }
    let mut confirmed_bytes = initial_covered;

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
            let frame = block_frame(idx, crc, raw_len, &payload);
            conn.send(&frame)?;
            sent += 1;
        }

        // 2. Wave boundary: the receiver reconciles.
        conn.send(&wave_done_frame())?;

        // 3. Poll for the server's response. The poll gives the JS event loop
        //    time to drain the socket and queue incoming frames; a stall guard
        //    replaces the old blocking `next()` timeout. Progress advances
        //    only when the server CONFIRMS bytes (see the FRAME_REQ branch
        //    below). NAKs re-queue immediately (实时核验 → 重传队列).
        //
        //    The stall deadline MUST roll with wire progress measured across
        //    polls (`last_wire`), not within a single poll: two reads of
        //    `transmitted_bytes()` inside one iteration always agree (nothing
        //    drains between synchronous reads), so comparing them could never
        //    refresh the deadline. With the deadline keyed to the PREVIOUS
        //    poll's value, a slow link whose wave takes longer than
        //    `timeout_ms` to drain keeps the timer rolling instead of being
        //    misread as a stall and aborted with "ws read timed out".
        let mut last_activity = js_sys::Date::now();
        let mut last_wire = conn.transmitted_bytes();
        loop {
            control.check()?;
            let wire_now = conn.transmitted_bytes();
            roll_stall_on_wire(
                &mut last_wire,
                &mut last_activity,
                wire_now,
                js_sys::Date::now(),
            );
            if let Some(frame) = conn.try_recv() {
                last_activity = js_sys::Date::now();
                match frame_type(&frame) {
                    Some(FRAME_NAK) => {
                        if let Some(idx) = parse_nak(&frame) {
                            queue.push_back(idx);
                        }
                        // Keep polling: the server may NAK several blocks.
                    }
                    Some(FRAME_REQ) => {
                        if let Some(indices) = parse_req(&frame) {
                            queue.extend(indices.iter().copied());
                            // The REQ lists every block the server still has
                            // not verified; the newly-confirmed set is the
                            // part of the previous missing set it no longer
                            // lists. Fold exactly those FILE bytes into the
                            // bar (server-confirmed progress).
                            let mut new_missing = BlockSet::new(total_blocks);
                            for i in &indices {
                                if *i < total_blocks {
                                    new_missing.insert(*i);
                                }
                            }
                            let delta = confirmed_delta(
                                &prev_missing,
                                &new_missing,
                                block_size,
                                file.size,
                            );
                            prev_missing = new_missing;
                            if delta > 0 {
                                confirmed_bytes = confirmed_bytes.saturating_add(delta);
                                control.add_progress(delta);
                                control.report_progress_if(callbacks)?;
                            }
                        }
                        break; // next wave
                    }
                    Some(FRAME_COMPLETE) => {
                        let msg: CompleteMessage = parse_control(&frame, FRAME_COMPLETE)
                            .ok_or_else(|| LibfwError::Protocol("bad COMPLETE frame".into()))?;
                        if msg.ok {
                            // The server confirms it holds every byte: fold
                            // any tail (blocks verified since the last REQ)
                            // and force an exact 100%.
                            let remaining = file.size.saturating_sub(confirmed_bytes);
                            if remaining > 0 {
                                control.add_progress(remaining);
                            }
                            control.report_progress_if(callbacks)?;
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
                continue;
            }
            // Stalled: no frames AND no wire progress for `timeout_ms`
            // (`0` disables the timeout, matching `with_timeout`).
            if config.timeout_ms > 0
                && js_sys::Date::now() - last_activity > config.timeout_ms as f64
            {
                return Err(LibfwError::Network("ws read timed out".into()));
            }
            // Yield to the JS event loop so the socket drains and incoming
            // messages queue up; 50 ms is a snappy, low-cost poll cadence.
            sleep_ms(50).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirmed_delta_counts_only_newly_verified_blocks() {
        // size 10, block 4 → blocks 0 (0..4), 1 (4..8), 2 (8..10).
        let bs = 4u64;
        let file_size = 10u64;

        let mut prev = BlockSet::new(3); // nothing confirmed yet → all missing
        for i in 0..3 {
            prev.insert(i);
        }
        // Server verifies block 1 only.
        let mut now = BlockSet::new(3);
        now.insert(0);
        now.insert(2);
        assert_eq!(confirmed_delta(&prev, &now, bs, file_size), 4); // block 1
        assert_eq!(confirmed_delta(&now, &prev, bs, file_size), 0); // reversed
    }

    #[test]
    fn confirmed_delta_is_monotonic_and_idempotent() {
        let bs = 4u64;
        let file_size = 10u64;
        let mut prev = BlockSet::new(3);
        for i in 0..3 {
            prev.insert(i);
        }
        // Wave 1: server verifies block 0 (4 bytes).
        let mut w1 = BlockSet::new(3);
        w1.insert(1);
        w1.insert(2);
        assert_eq!(confirmed_delta(&prev, &w1, bs, file_size), 4);
        // Wave 2: server verifies block 1 as well → only block 1 is new.
        let mut w2 = BlockSet::new(3);
        w2.insert(2);
        assert_eq!(confirmed_delta(&w1, &w2, bs, file_size), 4);
        // No further confirmation → no further progress.
        let mut w3 = BlockSet::new(3);
        w3.insert(2);
        assert_eq!(confirmed_delta(&w2, &w3, bs, file_size), 0);
    }

    #[test]
    fn confirmed_delta_counts_partial_last_block() {
        // size 10, block 4 → block 2 covers only 8..10 (2 bytes).
        let bs = 4u64;
        let file_size = 10u64;
        let mut prev = BlockSet::new(3);
        for i in 0..3 {
            prev.insert(i);
        }
        // Server verifies only the trailing partial block.
        let mut now = BlockSet::new(3);
        now.insert(0);
        now.insert(1);
        assert_eq!(confirmed_delta(&prev, &now, bs, file_size), 2);
    }

    #[test]
    fn stall_deadline_rolls_on_wire_progress_across_polls() {
        // Regression: the stall deadline must roll forward whenever the
        // socket keeps draining a slow wave, not only when a frame arrives.
        // Comparing two reads within a single poll would always be equal and
        // could never roll the deadline, falsely timing out a slow-but-
        // progressing upload ("ws read timed out" mid-transfer).
        let t0 = 1_000.0;
        let mut last_activity = t0;
        let mut last_wire = 0;

        // No wire progress yet → deadline stays put.
        assert!(!roll_stall_on_wire(&mut last_wire, &mut last_activity, 0, t0 + 10.0));
        assert_eq!(last_activity, t0);

        // Wire advances slowly (a long, slow wave draining) → the deadline
        // rolls forward on every poll, so the transfer is never misread as
        // a stall no matter how slow the drain is.
        assert!(roll_stall_on_wire(&mut last_wire, &mut last_activity, 4_000, t0 + 1_000.0));
        assert_eq!(last_activity, t0 + 1_000.0);
        assert!(roll_stall_on_wire(&mut last_wire, &mut last_activity, 9_000, t0 + 2_000.0));
        assert_eq!(last_activity, t0 + 2_000.0);

        // Wire stops advancing (wave fully drained, awaiting the server's
        // response) → no roll; a long silence here is a genuine stall.
        assert!(!roll_stall_on_wire(&mut last_wire, &mut last_activity, 9_000, t0 + 90_000.0));
        assert_eq!(last_activity, t0 + 2_000.0);
    }
}
