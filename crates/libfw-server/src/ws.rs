//! WebSocket handler: the unified block-transfer transport.
//!
//! Every transfer — **upload or download** — uses the identical block
//! protocol from [`libfw_core::ws`]:
//!
//! - The sender pipelines fixed-size blocks without waiting for a per-block
//!   acknowledgment (no-ack), so blocks may travel out of order.
//! - The receiver verifies **every** block in real time (CRC32 + bounds),
//!   marks bad blocks with `FRAME_NAK` and asks the sender to re-queue them.
//! - A wave boundary (`FRAME_WAVE_DONE`) triggers reconciliation: the
//!   receiver replies with `FRAME_REQ` (still-missing blocks) or
//!   `FRAME_COMPLETE` (everything verified). The sender re-adds requested
//!   blocks to its transfer queue and re-sends until the receiver is happy.
//!
//! All control commands (hello, directory listing, file metadata) travel over
//! the same WebSocket; there are no separate HTTP calls on the transfer path.
//! One connection may carry any number of transfers **sequentially** (the
//! browser client currently opens one connection per file, which lets
//! multiple files transfer concurrently across separate sockets).

use std::collections::VecDeque;
use std::io::Read;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use bytes::Bytes;
use futures::SinkExt;
use libfw_core::auth::{Action, AuthError};
use libfw_core::claims::TokenClaims;
use libfw_core::compress::{
    CompressionFormat, MAX_FRAME_OUTPUT, compressor, decompressor_with_limit,
};
use libfw_core::storage::WriteMode;
use libfw_core::ws::*;
use libfw_core::{RangeSpec, protocol_compatible};

use crate::{ServerState, validate_rel_path};

/// Default block size for a WebSocket download stream (256 KiB), kept small
/// so the browser's out-of-order reorder buffer stays modest.
const DEFAULT_DOWNLOAD_BLOCK: u64 = 256 * 1024;
/// Default in-flight blocks per wave when the client doesn't specify one
/// (bounds the receiver's buffering; each wave costs one RTT, so raise it on
/// high-latency links).
const SENDER_WINDOW: usize = 16;
/// Lower bound on a client-requested block size (kept low so small-block
/// transfers and tests keep working). Combined with the `MAX_BLOCKS` cap,
/// this still prevents a crafted `block_size: 1` on a huge file from forcing
/// an absurd block count.
const MIN_BLOCK_SIZE: u64 = 1024;
/// Upper bound on a client-requested block size (per-block memory is one
/// block per wave on the sender and receiver).
const MAX_BLOCK_SIZE: u64 = 16 * 1024 * 1024;
/// Upper bound on a client-requested in-flight window (bounds per-wave
/// memory: `window × block_size`).
const MAX_WINDOW: usize = 64;
/// Maximum number of blocks a single transfer may have. The download sender
/// eagerly materializes the full transfer queue (`VecDeque<u32>`, 4 bytes per
/// block), so this bounds that plan: 2²⁴ blocks ≈ 67 MiB worst case, which
/// covers ~4 TiB at the default 256 KiB block size.
const MAX_BLOCKS: u64 = 1 << 24;

/// axum route handler for `GET /ws`.
///
/// The upgrade is not gated by the `x-libfw-protocol` HTTP header (a browser
/// WebSocket handshake cannot carry it); instead the protocol version is
/// checked inside the `FRAME_HELLO` handshake.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
) -> Response {
    ws.on_upgrade(move |socket| run_socket(socket, state))
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

async fn run_socket(mut socket: WebSocket, state: Arc<ServerState>) {
    // 1. Handshake: HELLO (protocol + token) → HELLO_OK.
    let claims = match handshake(&mut socket, &state).await {
        Ok(claims) => claims,
        Err(err) => {
            let _ = send_frame(&mut socket, error_frame("handshake", &err)).await;
            let _ = socket.close().await;
            return;
        }
    };

    // 2. Control + any number of sequential transfers. All frames are binary
    //    (first byte = frame type); Text is accepted for robustness.
    loop {
        let msg = match socket.recv().await {
            Some(Ok(msg)) => msg,
            _ => break,
        };
        let frame: Vec<u8> = match msg {
            Message::Binary(data) => data.to_vec(),
            Message::Text(text) => text.as_bytes().to_vec(),
            Message::Close(_) => break,
            _ => continue,
        };
        match frame_type(&frame) {
            Some(FRAME_LIST_REQ) => {
                let reply = list_reply(&state, &claims, &frame).await;
                if send_frame(&mut socket, reply).await.is_err() {
                    break;
                }
            }
            Some(FRAME_META_REQ) => {
                let reply = meta_reply(&state, &claims, &frame).await;
                if send_frame(&mut socket, reply).await.is_err() {
                    break;
                }
            }
            Some(FRAME_START) => {
                let start: StartRequest = match parse_control(&frame, FRAME_START) {
                    Some(s) => s,
                    None => {
                        let _ = send_frame(
                            &mut socket,
                            error_frame("protocol", "malformed START"),
                        )
                        .await;
                        break;
                    }
                };
                match start.kind {
                    TransferKind::Download => {
                        run_download(&mut socket, &state, &claims, start).await;
                    }
                    TransferKind::Upload => {
                        run_upload(&mut socket, &state, &claims, start).await;
                    }
                }
                // Keep the connection open for further control/transfers.
            }
            Some(FRAME_COMPLETE) => {
                // A client abort before any transfer started.
                break;
            }
            // BLOCK/NAK/REQ outside an active transfer are out of protocol.
            _ => {}
        }
    }
}

/// Perform the `FRAME_HELLO` handshake and return the verified claims.
async fn handshake(socket: &mut WebSocket, state: &ServerState) -> Result<TokenClaims, String> {
    let msg = match socket.recv().await {
        Some(Ok(Message::Binary(data))) => data.to_vec(),
        Some(Ok(Message::Text(text))) => text.as_bytes().to_vec(),
        _ => return Err("expected FRAME_HELLO".into()),
    };
    let hello: Hello = parse_control(&msg, FRAME_HELLO).ok_or("expected FRAME_HELLO")?;
    if !protocol_compatible(&hello.protocol) {
        return Err(format!("unsupported protocol `{}`", hello.protocol));
    }
    let claims = state
        .verifier
        .verify(&hello.token)
        .map_err(|e| format!("authentication failed: {e}"))?;
    let ok = control_frame(FRAME_HELLO_OK, &serde_json::json!({ "ok": true }));
    send_frame(socket, ok).await.map_err(|_| "send failed".to_string())?;
    Ok(claims)
}

/// Send one raw frame over the socket.
async fn send_frame(socket: &mut WebSocket, frame: Vec<u8>) -> Result<(), ()> {
    socket
        .send(Message::Binary(Bytes::from(frame)))
        .await
        .map_err(|_| ())
}

/// Send a `FRAME_COMPLETE` message.
async fn send_complete(socket: &mut WebSocket, ok: bool, size: u64, err: Option<&str>) {
    let msg = CompleteMessage {
        ok,
        size,
        error: err.map(str::to_string),
    };
    let _ = send_frame(socket, control_frame(FRAME_COMPLETE, &msg)).await;
}

/// Build a `FRAME_ERROR` frame.
fn error_frame(code: &str, message: &str) -> Vec<u8> {
    control_frame(
        FRAME_ERROR,
        &ErrorMessage {
            code: code.to_string(),
            message: message.to_string(),
        },
    )
}

fn authorize(
    state: &ServerState,
    claims: &TokenClaims,
    path: &str,
    action: Action,
) -> Result<(), String> {
    state.authorize(claims, path, action).map_err(|err| match err {
        AuthError::Forbidden { path, action } => {
            format!("permission denied: {action} on `{path}`")
        }
        other => format!("unauthorized: {other}"),
    })
}

// ---------------------------------------------------------------------------
// Control: directory listing & metadata
// ---------------------------------------------------------------------------

async fn list_reply(state: &ServerState, claims: &TokenClaims, frame: &[u8]) -> Vec<u8> {
    #[derive(serde::Deserialize)]
    struct ListReq {
        #[serde(default)]
        path: String,
    }
    let req: ListReq = match serde_json::from_slice(frame_payload(frame)) {
        Ok(r) => r,
        Err(_) => return error_frame("protocol", "malformed LIST_REQ"),
    };
    let path = match validate_rel_path(&req.path) {
        Ok(p) => p,
        Err(e) => return error_frame("path", e),
    };
    if let Err(e) = authorize(state, claims, &path, Action::Read) {
        return error_frame("auth", &e);
    }
    match state.storage.list_dir(&path).await {
        Ok(entries) => control_frame(
            FRAME_LIST_REPLY,
            &serde_json::json!({ "path": req.path, "entries": entries }),
        ),
        Err(e) => error_frame("storage", &e.to_string()),
    }
}

async fn meta_reply(state: &ServerState, claims: &TokenClaims, frame: &[u8]) -> Vec<u8> {
    #[derive(serde::Deserialize)]
    struct MetaReq {
        #[serde(default)]
        path: String,
    }
    let req: MetaReq = match serde_json::from_slice(frame_payload(frame)) {
        Ok(r) => r,
        Err(_) => return error_frame("protocol", "malformed META_REQ"),
    };
    let path = match validate_rel_path(&req.path) {
        Ok(p) => p,
        Err(e) => return error_frame("path", e),
    };
    if let Err(e) = authorize(state, claims, &path, Action::Read) {
        return error_frame("auth", &e);
    }
    match state.storage.file_meta(&path).await {
        Ok(Some(meta)) => control_frame(
            FRAME_META_REPLY,
            &serde_json::json!({
                "path": meta.path,
                "size": meta.size,
                "mtime": meta.mtime,
                "etag": meta.etag,
            }),
        ),
        Ok(None) => error_frame("not_found", &format!("file not found: {path}")),
        Err(e) => error_frame("storage", &e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Download (server is the SENDER)
// ---------------------------------------------------------------------------

/// Serve one download stream: slice `[offset, size)` into blocks and push
/// them to the client (receiver), handling real-time NAK/REQ retransmission.
async fn run_download(
    socket: &mut WebSocket,
    state: &ServerState,
    claims: &TokenClaims,
    start: StartRequest,
) {
    let path = match validate_rel_path(&start.path) {
        Ok(p) => p,
        Err(e) => {
            let _ = send_frame(socket, error_frame("path", e)).await;
            return;
        }
    };
    if let Err(e) = authorize(state, claims, &path, Action::Read) {
        let _ = send_frame(socket, error_frame("auth", &e)).await;
        return;
    }
    let meta = match state.storage.file_meta(&path).await {
        Ok(Some(m)) => m,
        Ok(None) => {
            let _ = send_frame(socket, error_frame("not_found", &path)).await;
            return;
        }
        Err(e) => {
            let _ = send_frame(socket, error_frame("storage", &e.to_string())).await;
            return;
        }
    };

    // Clamp client-supplied sizing so a crafted START can't force an absurd
    // block count (block_size too small) or an unbounded in-flight wave
    // (window too large). The READY reply echoes the effective values, so a
    // legit client adapts to them automatically.
    let block_size = if (MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&start.block_size) {
        start.block_size
    } else {
        DEFAULT_DOWNLOAD_BLOCK
    };
    let window = if start.window > 0 {
        (start.window as usize).clamp(1, MAX_WINDOW)
    } else {
        SENDER_WINDOW
    };
    // Whether the server actually compresses (only when the client asked AND
    // the server's configured download compression is zrip).
    let compress = start.compress && state.compression == CompressionFormat::Zrip;
    let start_off = start.offset.min(meta.size);
    let total = meta.size - start_off;
    let total_blocks = block_count(total, block_size);
    // Bound the eagerly-allocated transfer queue (see `MAX_BLOCKS`); reject
    // instead of clamping so the client's block geometry stays what it asked
    // for whenever it is sane.
    if total_blocks as u64 > MAX_BLOCKS {
        let _ = send_frame(socket, error_frame("too_large", "transfer too large")).await;
        return;
    }

    let ready = ReadyReply {
        kind: TransferKind::Download,
        path: path.clone(),
        size: meta.size,
        mtime: meta.mtime,
        etag: meta.etag.clone(),
        compress,
        block_size,
        total_blocks,
        offset: start_off,
        received: Vec::new(),
    };
    if send_frame(socket, control_frame(FRAME_READY, &ready)).await.is_err() {
        return;
    }

    // Sender transfer queue: indices to send. NAK/REQ re-add to this queue.
    let mut queue: VecDeque<u32> = (0..total_blocks).collect();

    loop {
        // 1. Read one wave of blocks into memory, then send them.
        //
        // Each block is read with a FRESH reader opened at its absolute
        // offset and drained in a tight loop. This avoids a Windows/tokio
        // quirk where a long-lived `read_stream` reader returns early EOF
        // once its reads are interleaved with socket awaits — each block
        // read is independent, and memory stays bounded (one wave).
        let mut wave: Vec<(u32, Vec<u8>, u32, u32)> = Vec::new();
        let mut sent = 0usize;
        while sent < window {
            let Some(idx) = queue.pop_front() else {
                break;
            };
            let (start, end) = block_bounds(idx, block_size, total);
            let abs = block_offset(idx, block_size, start_off);
            let mut data = vec![0u8; (end - start) as usize];
            let mut reader = match state
                .storage
                .read_stream(&path, RangeSpec { start: abs, end: abs + (end - start) })
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    let _ = send_frame(socket, error_frame("storage", &e.to_string())).await;
                    return;
                }
            };
            if read_exact(&mut reader, &mut data).is_err() {
                let _ = send_frame(socket, error_frame("io", "read failed")).await;
                return;
            }
            let raw_len = data.len() as u32;
            let payload: Vec<u8> = if compress {
                match compress_frame(&data) {
                    Some(p) => p,
                    None => {
                        let _ =
                            send_frame(socket, error_frame("compress", "compress failed")).await;
                        return;
                    }
                }
            } else {
                data
            };
            let crc = crc32(&payload);
            wave.push((idx, payload, crc, raw_len));
            sent += 1;
        }

        // 2. Send the wave's blocks (no per-block ack).
        for (idx, payload, crc, raw_len) in wave {
            let frame = block_frame(idx, crc, raw_len, &payload);
            if send_frame(socket, frame).await.is_err() {
                return;
            }
        }

        // 3. Wave boundary: the receiver reconciles.
        if send_frame(socket, wave_done_frame()).await.is_err() {
            return;
        }

        // 4. Read events until the receiver asks for more (REQ) or is done
        //    (COMPLETE). NAKs re-queue immediately ("实时核验 → 重传队列").
        loop {
            let msg = match socket.recv().await {
                Some(Ok(msg)) => msg,
                _ => return, // client gone
            };
            let frame: Vec<u8> = match msg {
                Message::Binary(data) => data.to_vec(),
                Message::Text(text) => text.as_bytes().to_vec(),
                Message::Close(_) => return,
                _ => continue,
            };
            match frame_type(&frame) {
                Some(FRAME_NAK) => {
                    if let Some(index) = parse_nak(&frame) {
                        // Ignore out-of-range indices: `block_bounds` on such
                        // an index underflows and would allocate a huge
                        // buffer (crash/OOM) in the wave below.
                        if index < total_blocks {
                            queue.push_back(index);
                        }
                    }
                }
                Some(FRAME_REQ) => {
                    if let Some(indices) = parse_req(&frame) {
                        queue.extend(indices.into_iter().filter(|&i| i < total_blocks));
                    }
                    break; // next wave
                }
                Some(FRAME_COMPLETE) => return, // receiver finished → done
                _ => {}
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Upload (server is the RECEIVER)
// ---------------------------------------------------------------------------

/// Receive one upload stream: verify every block (CRC32 + bounds), write it
/// at its absolute offset into a shared session temp, NAK bad blocks and
/// commit once all blocks are verified.
async fn run_upload(
    socket: &mut WebSocket,
    state: &ServerState,
    claims: &TokenClaims,
    start: StartRequest,
) {
    let path = match validate_rel_path(&start.path) {
        Ok(p) => p,
        Err(e) => {
            let _ = send_frame(socket, error_frame("path", e)).await;
            return;
        }
    };
    if let Err(e) = authorize(state, claims, &path, Action::Write) {
        let _ = send_frame(socket, error_frame("auth", &e)).await;
        return;
    }
    if start.size > state.max_upload_size {
        let _ = send_frame(
            socket,
            error_frame(
                "too_large",
                &format!("upload exceeds limit of {} bytes", state.max_upload_size),
            ),
        )
        .await;
        return;
    }

    let block_size = if (MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&start.block_size) {
        start.block_size
    } else {
        libfw_core::CHUNK_SIZE
    };
    let total_blocks = block_count(start.size, block_size);
    // Bound the receiver's verified-set allocation (see `MAX_BLOCKS`).
    if total_blocks as u64 > MAX_BLOCKS {
        let _ = send_frame(socket, error_frame("too_large", "transfer too large")).await;
        return;
    }
    let mode = if start.mode.eq_ignore_ascii_case("create") {
        WriteMode::Create
    } else {
        WriteMode::Overwrite
    };
    // Deterministic session id from the ETag → an interrupted upload of the
    // same file version resumes the same shared temp on the server.
    let session = start.etag.trim_matches('"');

    let mut sink = match state.storage.write_stream_session(&path, session, mode).await {
        Ok(s) => s,
        Err(e) => {
            let _ = send_frame(socket, error_frame("storage", &e.to_string())).await;
            return;
        }
    };

    // Seed the verified set + resume ranges from what the server already
    // holds, so the client only retransmits the missing blocks.
    let received = sink.received_ranges().await.unwrap_or_default();
    let received_pairs: Vec<[u64; 2]> = received.iter().map(|r| [r.start, r.end]).collect();
    let received_slices: Vec<(u64, u64)> = received.iter().map(|r| (r.start, r.end)).collect();
    let mut verified = BlockSet::new(total_blocks);
    verified.seed_from_ranges(block_size, &received_slices);

    let ready = ReadyReply {
        kind: TransferKind::Upload,
        path: path.clone(),
        size: start.size,
        mtime: start.mtime,
        etag: start.etag.clone(),
        compress: start.compress,
        block_size,
        total_blocks,
        offset: 0,
        received: received_pairs,
    };
    if send_frame(socket, control_frame(FRAME_READY, &ready)).await.is_err() {
        return; // keep the session temp for a later resume
    }

    let compress = start.compress;
    loop {
        let msg = match socket.recv().await {
            Some(Ok(msg)) => msg,
            _ => {
                // Client gone: drop the sink WITHOUT abort so the session
                // temp survives for a resumable retry (tus expiration).
                return;
            }
        };
        let frame: Vec<u8> = match msg {
            Message::Binary(data) => data.to_vec(),
            Message::Text(text) => text.as_bytes().to_vec(),
            Message::Close(_) => {
                // Keep the session temp for a possible resume.
                return;
            }
            _ => continue,
        };
        match frame_type(&frame) {
            Some(FRAME_BLOCK) => {
                let Some(block) = parse_block(&frame) else {
                    continue;
                };
                // Already-verified (duplicate / re-sent) or out of range →
                // idempotent no-op.
                if block.index >= total_blocks || verified.contains(block.index) {
                    continue;
                }
                // Real-time verification: CRC + length + bounds.
                let crc_ok = crc32(&block.data) == block.crc;
                let raw: Vec<u8> = if compress {
                    match decompress_frame(&block.data) {
                        Ok(d) => d,
                        Err(_) => {
                            let _ = send_frame(socket, nak_frame(block.index)).await;
                            continue;
                        }
                    }
                } else {
                    block.data
                };
                let len_ok = !compress || raw.len() as u32 == block.raw_len;
                let abs = block_offset(block.index, block_size, 0);
                let end = abs.saturating_add(raw.len() as u64);
                let in_bounds = end <= start.size && end <= state.max_upload_size;
                if !(crc_ok && len_ok && in_bounds) {
                    // Mark bad → ask the sender to re-queue it.
                    let _ = send_frame(socket, nak_frame(block.index)).await;
                    continue;
                }
                if let Err(e) = sink.write_at(abs, &raw).await {
                    let _ = send_frame(
                        socket,
                        control_frame(
                            FRAME_COMPLETE,
                            &CompleteMessage::err(format!("write failed: {e}")),
                        ),
                    )
                    .await;
                    let _ = sink.abort().await;
                    return;
                }
                verified.insert(block.index);
            }
            Some(FRAME_WAVE_DONE) => {
                // Reconciliation: everything verified → commit, else ask the
                // sender to re-send the missing blocks.
                let missing = verified.missing();
                if missing.is_empty() {
                    let len = match sink.len().await {
                        Ok(l) => l,
                        Err(e) => {
                            let _ = send_complete(
                                socket,
                                false,
                                0,
                                Some(&format!("len failed: {e}")),
                            )
                            .await;
                            let _ = sink.abort().await;
                            return;
                        }
                    };
                    if len != start.size {
                        let _ = send_complete(socket, false, 0, Some("commit size mismatch")).await;
                        let _ = sink.abort().await;
                        return;
                    }
                    // `commit` consumes the sink; on failure there is nothing
                    // left to abort (temp is best-effort).
                    match sink.commit().await {
                        Ok(_) => {
                            let _ = send_complete(socket, true, start.size, None).await;
                            return;
                        }
                        Err(e) => {
                            let _ = send_complete(
                                socket,
                                false,
                                0,
                                Some(&format!("commit failed: {e}")),
                            )
                            .await;
                            return;
                        }
                    }
                } else {
                    let _ = send_frame(socket, req_frame(&missing)).await;
                }
            }
            Some(FRAME_COMPLETE) => {
                // A client abort mid-upload keeps the session temp.
                return;
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Read exactly `buf.len()` bytes (or fail on early EOF) from a reader.
fn read_exact(reader: &mut Box<dyn Read + Send>, buf: &mut [u8]) -> Result<(), ()> {
    let mut filled = 0usize;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => return Err(()),
            Ok(n) => filled += n,
            Err(_) => return Err(()),
        }
    }
    Ok(())
}

/// Compress `data` into one independent zrip frame.
fn compress_frame(data: &[u8]) -> Option<Vec<u8>> {
    let mut enc = compressor(CompressionFormat::Zrip).ok()?;
    let mut out = Vec::with_capacity(data.len());
    enc.compress(data, &mut out).ok()?;
    enc.finish(&mut out).ok()?;
    Some(out)
}

/// Decompress one independent zrip frame.
fn decompress_frame(data: &[u8]) -> Result<Vec<u8>, libfw_core::StorageError> {
    let mut dec = decompressor_with_limit(CompressionFormat::Zrip, MAX_FRAME_OUTPUT);
    let mut out: Vec<u8> = Vec::new();
    dec.decompress(data, &mut out)
        .map_err(|e| libfw_core::StorageError::Other(std::io::Error::other(format!("{e}"))))?;
    dec.finish(&mut out)
        .map_err(|e| libfw_core::StorageError::Other(std::io::Error::other(format!("{e}"))))?;
    Ok(out)
}
