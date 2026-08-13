//! End-to-end WebSocket integration tests: handshake, control (listing /
//! metadata), no-ack out-of-order block upload → download roundtrips,
//! NAK-based retransmission of bad blocks, multi-wave reconciliation and
//! resumable upload sessions.

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_core::metadata::etag_from_size_mtime;
use libfw_core::storage::{StorageBackend, WriteMode};
use libfw_core::ws::*;
use libfw_core::RangeSpec;
use libfw_server::{router, FsStorage, ServerState};
use tokio_tungstenite::tungstenite::Message;

/// A verifier that maps any token to full permissions.
#[derive(Clone)]
struct DevVerifier;
impl TokenVerifier for DevVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        Ok(TokenClaims {
            sub: token.to_string(),
            exp: None,
            permissions: vec![Permission::Read, Permission::Write],
            allowed_paths: vec!["/".to_string()],
        })
    }
}

/// Sanity check that `read_stream` really yields a full 40 KiB file in 1 KiB
/// reads (isolates the WS download reader from storage issues).
#[tokio::test]
async fn read_stream_yields_full_large_file() {
    use std::io::Read;
    let tmp = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(tmp.path());
    let data = vec![7u8; 40_000];
    let mut sink = storage
        .write_stream("big.bin", WriteMode::Overwrite)
        .await
        .unwrap();
    sink.write(&data).await.unwrap();
    sink.commit().await.unwrap();

    let mut reader = storage
        .read_stream("big.bin", RangeSpec { start: 0, end: 40_000 })
        .await
        .unwrap();
    let mut total = 0u64;
    let mut buf = vec![0u8; 1024];
    loop {
        let n = reader.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        total += n as u64;
    }
    assert_eq!(total, 40_000, "read_stream should yield the whole file");
}

/// Sanity check that out-of-order `write_at` fills the whole file (the WS
/// upload sends blocks in reverse order).
#[tokio::test]
async fn reverse_write_at_fills_full_file() {
    use std::io::Read;
    use libfw_core::ws::block_bounds;
    let tmp = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(tmp.path());
    let data: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    let mut sink = storage
        .write_stream("rev.bin", WriteMode::Overwrite)
        .await
        .unwrap();
    let total = block_count(data.len() as u64, 1024);
    for idx in (0..total).rev() {
        let (s, e) = block_bounds(idx, 1024, data.len() as u64);
        sink.write_at(s, &data[s as usize..e as usize]).await.unwrap();
    }
    sink.commit().await.unwrap();

    let meta = storage.file_meta("rev.bin").await.unwrap().unwrap();
    assert_eq!(meta.size, 40_000);
    let mut reader = storage
        .read_stream("rev.bin", RangeSpec { start: 0, end: 40_000 })
        .await
        .unwrap();
    let mut got = Vec::new();
    let mut buf = vec![0u8; 1024];
    loop {
        let n = reader.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, data, "reverse write_at should produce the full file");
}

/// Sanity check that a file uploaded over the WebSocket is complete on disk
/// (isolates the WS upload from the WS download).
#[tokio::test]
async fn ws_uploaded_file_readable_in_full() {
    use std::io::Read;
    let state = state();
    let mut ws = connect_ws(state.clone()).await;
    hello(&mut ws, "tok").await;
    let data: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    upload_file(&mut ws, "big.bin", &data, 1024, 0, false).await;

    let meta = state.storage.file_meta("big.bin").await.unwrap().unwrap();
    assert_eq!(meta.size, 40_000);
    let mut reader = state
        .storage
        .read_stream("big.bin", RangeSpec { start: 0, end: 40_000 })
        .await
        .unwrap();
    let mut got = Vec::new();
    let mut buf = vec![0u8; 1024];
    loop {
        let n = reader.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, data, "WS-uploaded file should be complete on disk");
}

type Ws = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

/// Build a router over a temp-dir storage backend.
fn state() -> Arc<ServerState> {
    Arc::new(
        ServerState::builder()
            .storage(FsStorage::new(tempfile::tempdir().unwrap().path()))
            .verifier(DevVerifier)
            .validator(PathValidator::new())
            .build(),
    )
}

/// Spawn the router on an ephemeral port and connect a WS client.
async fn connect_ws(state: Arc<ServerState>) -> Ws {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    let (ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .expect("ws connect");
    ws
}

/// Send one binary frame.
async fn send(ws: &mut Ws, frame: Vec<u8>) {
    ws.send(Message::Binary(frame)).await.expect("send");
}

/// Read the next binary frame, skipping control (ping/pong) frames.
async fn recv(ws: &mut Ws) -> Vec<u8> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Binary(b))) => return b.to_vec(),
            Some(Ok(_)) => continue,
            other => panic!("ws closed unexpectedly: {other:?}"),
        }
    }
}

/// Perform the HELLO handshake.
async fn hello(ws: &mut Ws, token: &str) {
    send(ws, control_frame(FRAME_HELLO, &Hello::new(token))).await;
    let f = recv(ws).await;
    assert_eq!(frame_type(&f), Some(FRAME_HELLO_OK), "expected HELLO_OK");
}

/// Upload a whole file using the no-ack out-of-order block protocol.
async fn upload_file(
    ws: &mut Ws,
    path: &str,
    data: &[u8],
    block_size: u64,
    mtime: u64,
    compress: bool,
) {
    let etag = etag_from_size_mtime(data.len() as u64, mtime);
    let start = StartRequest {
        kind: TransferKind::Upload,
        path: path.to_string(),
        size: data.len() as u64,
        mtime,
        etag,
        compress,
        mode: "overwrite".into(),
        offset: 0,
        block_size,
        window: 0,
    };
    send(ws, control_frame(FRAME_START, &start)).await;
    let ready = recv(ws).await;
    let _ready: ReadyReply = parse_control(&ready, FRAME_READY).unwrap();

    let total = block_count(data.len() as u64, block_size);
    // Deliberately send blocks in REVERSE order to prove out-of-order
    // (positional) writes work.
    for idx in (0..total).rev() {
        let (s, e) = block_bounds(idx, block_size, data.len() as u64);
        let payload = &data[s as usize..e as usize];
        let frame = if compress {
            let mut enc = libfw_core::compress::compressor(libfw_core::compress::CompressionFormat::Zrip)
                .unwrap();
            let mut out = Vec::new();
            enc.compress(payload, &mut out).unwrap();
            enc.finish(&mut out).unwrap();
            block_frame(idx, crc32(&out), payload.len() as u32, &out)
        } else {
            block_frame(idx, crc32(payload), payload.len() as u32, payload)
        };
        send(ws, frame).await;
    }
    send(ws, wave_done_frame()).await;
    let complete = recv(ws).await;
    if frame_type(&complete) != Some(FRAME_COMPLETE) {
        panic!(
            "upload expected COMPLETE, got type {:?} payload {}",
            frame_type(&complete),
            String::from_utf8_lossy(frame_payload(&complete))
        );
    }
    let msg: CompleteMessage = parse_control(&complete, FRAME_COMPLETE).unwrap();
    assert!(msg.ok, "upload should commit, got: {:?}", msg.error);
}

/// Download a whole file and return its bytes.
async fn download_file(ws: &mut Ws, path: &str, block_size: u64) -> Vec<u8> {
    let start = StartRequest {
        kind: TransferKind::Download,
        path: path.to_string(),
        size: 0,
        mtime: 0,
        etag: String::new(),
        compress: false,
        mode: String::new(),
        offset: 0,
        block_size,
        window: 0,
    };
    send(ws, control_frame(FRAME_START, &start)).await;
    let ready = recv(ws).await;
    let ready: ReadyReply = parse_control(&ready, FRAME_READY).unwrap();

    let total = ready.total_blocks as usize;
    let mut blocks: Vec<Option<Vec<u8>>> = vec![None; total];
    let mut got = Vec::new();
    loop {
        let f = recv(ws).await;
        match frame_type(&f) {
            Some(FRAME_BLOCK) => {
                let b = parse_block(&f).unwrap();
                assert_eq!(crc32(&b.data), b.crc, "download block failed CRC");
                assert!((b.index as usize) < total);
                if blocks[b.index as usize].is_none() {
                    blocks[b.index as usize] = Some(b.data);
                }
            }
            Some(FRAME_WAVE_DONE) => {
                let missing: Vec<u32> = (0..total as u32)
                    .filter(|i| blocks[*i as usize].is_none())
                    .collect();
                if missing.is_empty() {
                    send(ws, control_frame(FRAME_COMPLETE, &CompleteMessage::ok(ready.size)))
                        .await;
                    break;
                }
                send(ws, req_frame(&missing)).await;
            }
            Some(FRAME_COMPLETE) => {
                let msg: CompleteMessage = parse_control(&f, FRAME_COMPLETE).unwrap();
                assert!(msg.ok, "download failed: {:?}", msg.error);
                break;
            }
            other => {
                if other == Some(FRAME_ERROR) {
                    panic!(
                        "download got ERROR frame: {}",
                        String::from_utf8_lossy(frame_payload(&f))
                    );
                }
                panic!("unexpected frame type {other:?} during download")
            }
        }
    }
    for b in blocks {
        got.extend_from_slice(&b.expect("block present after completion"));
    }
    assert_eq!(got.len() as u64, ready.size);
    got
}

#[tokio::test]
async fn ws_hello_handshake() {
    let mut ws = connect_ws(state()).await;
    hello(&mut ws, "tok").await;
}

#[tokio::test]
async fn ws_upload_then_download_roundtrip() {
    let mut ws = connect_ws(state()).await;
    hello(&mut ws, "tok").await;

    let data: Vec<u8> = (0..10000u32).map(|i| (i % 251) as u8).collect();
    upload_file(&mut ws, "up.bin", &data, 4096, 0, false).await;
    let got = download_file(&mut ws, "up.bin", 4096).await;
    assert_eq!(got, data);
}

#[tokio::test]
async fn ws_roundtrip_multi_wave_large_file() {
    let mut ws = connect_ws(state()).await;
    hello(&mut ws, "tok").await;

    // 40 KiB at 1 KiB blocks = 40 blocks → more than one sender wave (16),
    // exercising the REQ-driven multi-wave reconciliation on download.
    let data: Vec<u8> = (0..40_000u32).map(|i| (i.wrapping_mul(7) % 256) as u8).collect();
    upload_file(&mut ws, "big.bin", &data, 1024, 0, false).await;
    let got = download_file(&mut ws, "big.bin", 1024).await;
    assert_eq!(got, data);
}

#[tokio::test]
async fn ws_roundtrip_compressed() {
    let mut ws = connect_ws(state()).await;
    hello(&mut ws, "tok").await;

    // Highly compressible data so the zrip frames actually shrink.
    let data: Vec<u8> = vec![b'A'; 20_000];
    upload_file(&mut ws, "z.bin", &data, 4096, 0, true).await;
    // Download compressed (client asks for compression).
    let start = StartRequest {
        kind: TransferKind::Download,
        path: "z.bin".into(),
        size: 0,
        mtime: 0,
        etag: String::new(),
        compress: true,
        mode: String::new(),
        offset: 0,
        block_size: 4096,
        window: 0,
    };
    send(&mut ws, control_frame(FRAME_START, &start)).await;
    let ready = recv(&mut ws).await;
    let ready: ReadyReply = parse_control(&ready, FRAME_READY).unwrap();
    assert!(ready.compress, "server should compress when requested");

    // Receive + decompress blocks.
    let total = ready.total_blocks as usize;
    let mut blocks: Vec<Option<Vec<u8>>> = vec![None; total];
    loop {
        let f = recv(&mut ws).await;
        match frame_type(&f) {
            Some(FRAME_BLOCK) => {
                let b = parse_block(&f).unwrap();
                assert_eq!(crc32(&b.data), b.crc);
                let mut dec =
                    libfw_core::compress::decompressor(libfw_core::compress::CompressionFormat::Zrip);
                let mut out = Vec::new();
                dec.decompress(&b.data, &mut out).unwrap();
                dec.finish(&mut out).unwrap();
                assert_eq!(out.len() as u32, b.raw_len);
                if blocks[b.index as usize].is_none() {
                    blocks[b.index as usize] = Some(out);
                }
            }
            Some(FRAME_WAVE_DONE) => {
                let missing: Vec<u32> = (0..total as u32)
                    .filter(|i| blocks[*i as usize].is_none())
                    .collect();
                if missing.is_empty() {
                    send(&mut ws, control_frame(FRAME_COMPLETE, &CompleteMessage::ok(ready.size)))
                        .await;
                    break;
                }
                send(&mut ws, req_frame(&missing)).await;
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }
    let mut got = Vec::new();
    for b in blocks {
        got.extend_from_slice(&b.unwrap());
    }
    assert_eq!(got, data);
}

#[tokio::test]
async fn ws_upload_bad_block_is_nakked_then_resends() {
    let mut ws = connect_ws(state()).await;
    hello(&mut ws, "tok").await;

    let data: Vec<u8> = vec![42u8; 8192]; // exactly 2 blocks at 4096
    let etag = etag_from_size_mtime(data.len() as u64, 0);
    let start = StartRequest {
        kind: TransferKind::Upload,
        path: "nak.bin".into(),
        size: data.len() as u64,
        mtime: 0,
        etag,
        compress: false,
        mode: "overwrite".into(),
        offset: 0,
        block_size: 4096,
        window: 0,
    };
    send(&mut ws, control_frame(FRAME_START, &start)).await;
    let ready = recv(&mut ws).await;
    let _ready: ReadyReply = parse_control(&ready, FRAME_READY).unwrap();

    // Send block 0 with a WRONG CRC → the server must NAK it immediately.
    let p0 = &data[0..4096];
    send(&mut ws, block_frame(0, 0xDEAD_BEEF, p0.len() as u32, p0)).await;
    let f = recv(&mut ws).await;
    assert_eq!(frame_type(&f), Some(FRAME_NAK), "expected NAK for bad block");
    assert_eq!(parse_nak(&f), Some(0));

    // Re-send block 0 correctly + block 1, then finish → commits.
    send(&mut ws, block_frame(0, crc32(p0), p0.len() as u32, p0)).await;
    let p1 = &data[4096..8192];
    send(&mut ws, block_frame(1, crc32(p1), p1.len() as u32, p1)).await;
    send(&mut ws, wave_done_frame()).await;
    let complete = recv(&mut ws).await;
    if frame_type(&complete) != Some(FRAME_COMPLETE) {
        panic!(
            "NAK upload expected COMPLETE, got type {:?} payload {}",
            frame_type(&complete),
            String::from_utf8_lossy(frame_payload(&complete))
        );
    }
    let msg: CompleteMessage = parse_control(&complete, FRAME_COMPLETE).unwrap_or_else(|| {
        panic!(
            "NAK upload COMPLETE did not parse; payload {}",
            String::from_utf8_lossy(frame_payload(&complete))
        )
    });
    assert!(msg.ok, "commit failed: {:?}", msg.error);

    // Roundtrip back to prove the file is intact.
    let got = download_file(&mut ws, "nak.bin", 4096).await;
    assert_eq!(got, data);
}

#[tokio::test]
async fn ws_upload_resume_reuses_partial_session() {
    let state = state();
    let data: Vec<u8> = (0..5000u32).map(|i| (i % 200) as u8).collect();
    let block_size = 4096u64;
    let etag = etag_from_size_mtime(data.len() as u64, 1);

    // 1. First connection: send only block 0, then disconnect WITHOUT
    //    WAVE_DONE — the server keeps the session temp for a resume.
    {
        let mut ws = connect_ws(state.clone()).await;
        hello(&mut ws, "tok").await;
        let start = StartRequest {
            kind: TransferKind::Upload,
            path: "resume.bin".into(),
            size: data.len() as u64,
            mtime: 1,
            etag: etag.clone(),
            compress: false,
            mode: "overwrite".into(),
            offset: 0,
            block_size,
            window: 0,
        };
        send(&mut ws, control_frame(FRAME_START, &start)).await;
        let ready = recv(&mut ws).await;
        let _ready: ReadyReply = parse_control(&ready, FRAME_READY).unwrap();
        let p0 = &data[0..4096];
        send(&mut ws, block_frame(0, crc32(p0), p0.len() as u32, p0)).await;
        // ws drops → connection closes → temp + range sidecar persist.
    }

    // 2. Reconnect with the same ETag → server reports block 0 as received.
    let mut ws = connect_ws(state).await;
    hello(&mut ws, "tok").await;
    let start = StartRequest {
        kind: TransferKind::Upload,
        path: "resume.bin".into(),
        size: data.len() as u64,
        mtime: 1,
        etag: etag.clone(),
        compress: false,
        mode: "overwrite".into(),
        offset: 0,
        block_size,
        window: 0,
    };
    send(&mut ws, control_frame(FRAME_START, &start)).await;
    let ready = recv(&mut ws).await;
    let ready: ReadyReply = parse_control(&ready, FRAME_READY).unwrap();
    assert_eq!(ready.received, vec![[0, 4096]], "resume ranges should include block 0");

    // 3. WAVE_DONE with nothing new → the server asks for block 1 only.
    send(&mut ws, wave_done_frame()).await;
    let f = recv(&mut ws).await;
    assert_eq!(frame_type(&f), Some(FRAME_REQ), "expected REQ for missing block");
    assert_eq!(parse_req(&f), Some(vec![1]));

    // 4. Send block 1 → finish → complete; then verify by downloading.
    let p1 = &data[4096..5000];
    send(&mut ws, block_frame(1, crc32(p1), p1.len() as u32, p1)).await;
    send(&mut ws, wave_done_frame()).await;
    let complete = recv(&mut ws).await;
    let msg: CompleteMessage = parse_control(&complete, FRAME_COMPLETE).unwrap();
    assert!(msg.ok, "resume commit failed: {:?}", msg.error);

    let got = download_file(&mut ws, "resume.bin", block_size).await;
    assert_eq!(got, data);
}

/// Download the tail of a file starting at `offset` (as the browser client
/// does after resuming an interrupted download that already has `offset`
/// bytes committed on disk), returning only the bytes from `offset` onward.
async fn download_file_resumed(ws: &mut Ws, path: &str, block_size: u64, offset: u64) -> Vec<u8> {
    let start = StartRequest {
        kind: TransferKind::Download,
        path: path.to_string(),
        size: 0,
        mtime: 0,
        etag: String::new(),
        compress: false,
        mode: String::new(),
        offset,
        block_size,
        window: 0,
    };
    send(ws, control_frame(FRAME_START, &start)).await;
    let ready = recv(ws).await;
    let ready: ReadyReply = parse_control(&ready, FRAME_READY).unwrap();
    // The server must echo the resume offset and index blocks from it.
    assert_eq!(ready.offset, offset, "server must honor the resume offset");

    let remaining = ready.size.saturating_sub(offset);
    let total_blocks = block_count(remaining, block_size);
    assert_eq!(ready.total_blocks, total_blocks, "tail block count mismatch");

    let mut blocks: Vec<Option<Vec<u8>>> = vec![None; total_blocks as usize];
    loop {
        let f = recv(ws).await;
        match frame_type(&f) {
            Some(FRAME_BLOCK) => {
                let b = parse_block(&f).unwrap();
                assert_eq!(crc32(&b.data), b.crc);
                assert!(b.index < total_blocks, "block index out of range");
                if blocks[b.index as usize].is_none() {
                    blocks[b.index as usize] = Some(b.data);
                }
            }
            Some(FRAME_WAVE_DONE) => {
                let missing: Vec<u32> = (0..total_blocks as u32)
                    .filter(|i| blocks[*i as usize].is_none())
                    .collect();
                if missing.is_empty() {
                    send(ws, control_frame(FRAME_COMPLETE, &CompleteMessage::ok(ready.size)))
                        .await;
                    break;
                }
                send(ws, req_frame(&missing)).await;
            }
            Some(FRAME_COMPLETE) => {
                let msg: CompleteMessage = parse_control(&f, FRAME_COMPLETE).unwrap();
                assert!(msg.ok, "download failed: {:?}", msg.error);
                break;
            }
            other => {
                if other == Some(FRAME_ERROR) {
                    panic!(
                        "download got ERROR frame: {}",
                        String::from_utf8_lossy(frame_payload(&f))
                    );
                }
                panic!("unexpected frame type {other:?} during resumed download")
            }
        }
    }
    let mut tail = Vec::new();
    for b in blocks {
        tail.extend_from_slice(&b.expect("block present after completion"));
    }
    assert_eq!(tail.len() as u64, remaining, "resumed tail length");
    tail
}

/// A download resumed from a non-zero offset must slice `[offset, size)` and
/// block-index from that offset, so a browser client appending the tail onto
/// its already-on-disk prefix reproduces the file exactly (no overlap/gap).
#[tokio::test]
async fn ws_download_resumes_from_nonzero_offset() {
    let state = state();
    let mut ws = connect_ws(state.clone()).await;
    hello(&mut ws, "tok").await;

    let data: Vec<u8> = (0..40_000u32).map(|i| (i.wrapping_mul(7) % 256) as u8).collect();
    let block_size = 1024u64;
    upload_file(&mut ws, "dl-resume.bin", &data, block_size, 0, false).await;

    // Resume mid-file (not block-aligned on purpose) — as if an interrupted
    // download had already committed the first 10_000 bytes on disk.
    let offset = 10_000u64;
    let tail = download_file_resumed(&mut ws, "dl-resume.bin", block_size, offset).await;
    assert_eq!(tail, data[offset as usize..]);

    // The full file is exactly prefix + tail (no gap, no overlap).
    let mut reassembled = data[..offset as usize].to_vec();
    reassembled.extend_from_slice(&tail);
    assert_eq!(reassembled, data);
}

#[tokio::test]
async fn ws_list_and_meta_control() {
    let mut ws = connect_ws(state()).await;
    hello(&mut ws, "tok").await;

    let data = b"control plane over ws".to_vec();
    upload_file(&mut ws, "dir/f.txt", &data, 4096, 0, false).await;

    // META_REQ → META_REPLY with size.
    send(&mut ws, control_frame(FRAME_META_REQ, &serde_json::json!({ "path": "dir/f.txt" })))
        .await;
    let f = recv(&mut ws).await;
    assert_eq!(frame_type(&f), Some(FRAME_META_REPLY));
    let text = std::str::from_utf8(frame_payload(&f)).unwrap();
    let v: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(v["size"].as_u64(), Some(data.len() as u64));

    // LIST_REQ root → contains the `dir` entry.
    send(&mut ws, control_frame(FRAME_LIST_REQ, &serde_json::json!({ "path": "" }))).await;
    let f = recv(&mut ws).await;
    assert_eq!(frame_type(&f), Some(FRAME_LIST_REPLY));
    let text = std::str::from_utf8(frame_payload(&f)).unwrap();
    assert!(text.contains("dir"), "listing should contain the dir entry: {text}");
}

#[tokio::test]
async fn ws_empty_file_roundtrip() {
    let mut ws = connect_ws(state()).await;
    hello(&mut ws, "tok").await;

    let data: Vec<u8> = Vec::new();
    upload_file(&mut ws, "empty.bin", &data, 4096, 0, false).await;
    let got = download_file(&mut ws, "empty.bin", 4096).await;
    assert!(got.is_empty());
}

#[tokio::test]
async fn ws_handshake_rejects_wrong_protocol() {
    let mut ws = connect_ws(state()).await;
    // Send a HELLO with an unsupported protocol version.
    send(&mut ws, control_frame(FRAME_HELLO, &Hello {
        protocol: "libfw/99".into(),
        token: "tok".into(),
    }))
    .await;
    let f = recv(&mut ws).await;
    assert_eq!(frame_type(&f), Some(FRAME_ERROR), "expected an error frame");
    assert_eq!(frame_type(&f), Some(FRAME_ERROR));
}
