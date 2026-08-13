//! WebSocket transport protocol shared by the server and the browser client.
//!
//! Both **upload and download use the exact same block-transfer engine** over
//! a single WebSocket connection per file:
//!
//! - The sender pipelines fixed-size blocks without waiting for a per-block
//!   acknowledgment, so blocks may be sent out of order and
//!   throughput is bounded by bandwidth instead of `block_size / RTT`.
//! - The receiver verifies **every** block in real time (CRC32 + bounds) and
//!   marks bad blocks with a [`FRAME_NAK`]; the sender re-adds those indices
//!   to its transfer queue.
//! - A wave boundary ([`FRAME_WAVE_DONE`]) triggers a reconciliation round:
//!   the receiver replies with a [`FRAME_REQ`] listing every block still not
//!   verified, which the sender re-queues and re-sends. This repeats until
//!   the receiver has verified all blocks, at which point it sends
//!   [`FRAME_COMPLETE`] (for downloads the client is the receiver and sends
//!   it; for uploads the server is the receiver, commits the file and sends
//!   it).
//!
//! All control commands (handshake, directory listing, metadata) travel over
//! the same WebSocket; nothing uses separate HTTP requests anymore on the
//! client transfer path.
//!
//! ## Frame layout
//!
//! Every frame is `[type: u8][payload: ...]`. Control frames carry a JSON
//! payload; data frames carry a compact binary payload (see the individual
//! builders/parsers below).

use serde::{Deserialize, Serialize};

use crate::constants::protocol_header_value;

// ---------------------------------------------------------------------------
// Frame type constants
// ---------------------------------------------------------------------------

/// Client → server handshake (`{"protocol","token"}`).
pub const FRAME_HELLO: u8 = 0x01;
/// Server → client handshake acknowledgment (`{"ok":true}`).
pub const FRAME_HELLO_OK: u8 = 0x02;
/// Client → server directory listing request (`{"path"}`).
pub const FRAME_LIST_REQ: u8 = 0x10;
/// Server → client directory listing reply (`{"path","entries":[...]}`).
pub const FRAME_LIST_REPLY: u8 = 0x11;
/// Client → server file metadata request (`{"path"}`).
pub const FRAME_META_REQ: u8 = 0x12;
/// Server → client file metadata reply (`{"path","size","mtime","etag"}`).
pub const FRAME_META_REPLY: u8 = 0x13;
/// Client → server transfer start (`StartRequest`).
pub const FRAME_START: u8 = 0x20;
/// Server → client transfer ready (`ReadyReply`).
pub const FRAME_READY: u8 = 0x21;
/// Block payload (binary): `[index:u32][crc:u32][raw_len:u32][data]`.
pub const FRAME_BLOCK: u8 = 0x30;
/// Receiver marks a block bad → sender re-queues it (binary `[index:u32]`).
pub const FRAME_NAK: u8 = 0x31;
/// Receiver asks the sender to re-send a set of blocks
/// (binary `[count:u32][index:u32 ...]`).
pub const FRAME_REQ: u8 = 0x32;
/// Sender finished a wave of blocks (empty payload).
pub const FRAME_WAVE_DONE: u8 = 0x33;
/// Receiver completed the transfer (`CompleteMessage`, JSON).
pub const FRAME_COMPLETE: u8 = 0x34;
/// Protocol error (`{"code","message"}`, JSON).
pub const FRAME_ERROR: u8 = 0xFF;

/// Read the frame type from the first byte.
pub fn frame_type(frame: &[u8]) -> Option<u8> {
    frame.first().copied()
}

/// Strip the type byte and return the payload slice.
pub fn frame_payload(frame: &[u8]) -> &[u8] {
    frame.get(1..).unwrap_or(&[])
}

// ---------------------------------------------------------------------------
// Control frames (JSON payload)
// ---------------------------------------------------------------------------

/// Build a control frame from a serializable JSON payload.
pub fn control_frame<T: Serialize>(kind: u8, payload: &T) -> Vec<u8> {
    let json = serde_json::to_vec(payload).unwrap_or_default();
    let mut out = Vec::with_capacity(1 + json.len());
    out.push(kind);
    out.extend_from_slice(&json);
    out
}

/// Parse a control frame's JSON payload into `T`.
pub fn parse_control<'a, T: Deserialize<'a>>(frame: &'a [u8], kind: u8) -> Option<T> {
    if frame.first() != Some(&kind) {
        return None;
    }
    serde_json::from_slice(frame.get(1..)?).ok()
}

/// The `FRAME_HELLO` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    /// Protocol handshake value (must equal [`protocol_header_value`]).
    pub protocol: String,
    /// Bearer token.
    pub token: String,
}

impl Hello {
    /// Build a well-formed hello payload.
    pub fn new(token: &str) -> Self {
        Hello {
            protocol: protocol_header_value().to_string(),
            token: token.to_string(),
        }
    }
}

/// Direction of a transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransferKind {
    Upload,
    Download,
}

/// The `FRAME_START` payload (client → server).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartRequest {
    /// Upload or download.
    pub kind: TransferKind,
    /// Virtual path.
    pub path: String,
    /// Declared size (upload; the download side ignores it).
    #[serde(default)]
    pub size: u64,
    /// Last-modified unix time (upload).
    #[serde(default)]
    pub mtime: u64,
    /// Deterministic content id (upload; server derives the session temp from
    /// it so an interrupted upload resumes the same shared temp).
    #[serde(default)]
    pub etag: String,
    /// Whether to compress each block payload.
    #[serde(default)]
    pub compress: bool,
    /// `"create"` | `"overwrite"` (upload only).
    #[serde(default)]
    pub mode: String,
    /// Resume offset (download only): the first block covers `offset..`.
    #[serde(default)]
    pub offset: u64,
    /// Block size the sender will use.
    #[serde(default)]
    pub block_size: u64,
    /// In-flight block window the sender should use (0 = server default).
    ///
    /// The download sender on the server pipelines up to this many blocks
    /// per wave before a reconciliation round, so the browser's
    /// `downloadWindow`/`uploadWindow` knobs stay meaningful.
    #[serde(default)]
    pub window: u32,
}

/// The `FRAME_READY` payload (server → client).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyReply {
    /// Upload or download.
    pub kind: TransferKind,
    /// Virtual path.
    pub path: String,
    /// Authoritative size (download: real file size; upload: declared size).
    #[serde(default)]
    pub size: u64,
    /// Last-modified unix time.
    #[serde(default)]
    pub mtime: u64,
    /// Authoritative ETag.
    #[serde(default)]
    pub etag: String,
    /// Whether this transfer is compressed.
    #[serde(default)]
    pub compress: bool,
    /// Block size in bytes.
    pub block_size: u64,
    /// Number of blocks in this transfer (indexes `0..total_blocks`).
    pub total_blocks: u32,
    /// Download only: absolute file offset the first block begins at.
    #[serde(default)]
    pub offset: u64,
    /// Upload only: byte ranges the server already holds (resume), as
    /// `[[start, end], ...]`.
    #[serde(default)]
    pub received: Vec<[u64; 2]>,
}

/// The `FRAME_COMPLETE` payload (receiver → sender).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteMessage {
    /// Whether the receiver accepted the whole transfer.
    pub ok: bool,
    /// Final size (download: bytes received; upload: committed size).
    #[serde(default)]
    pub size: u64,
    /// Human-readable error when `ok` is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl CompleteMessage {
    /// A successful completion.
    pub fn ok(size: u64) -> Self {
        CompleteMessage {
            ok: true,
            size,
            error: None,
        }
    }

    /// A failed completion.
    pub fn err(message: impl Into<String>) -> Self {
        CompleteMessage {
            ok: false,
            size: 0,
            error: Some(message.into()),
        }
    }
}

/// The `FRAME_ERROR` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessage {
    /// Machine-readable category.
    pub code: String,
    /// Human-readable detail.
    pub message: String,
}

// ---------------------------------------------------------------------------
// Block data frames (binary)
// ---------------------------------------------------------------------------

/// A decoded `FRAME_BLOCK` payload.
#[derive(Debug, Clone)]
pub struct Block {
    /// Zero-based block index.
    pub index: u32,
    /// CRC32 of the on-wire `data` payload.
    pub crc: u32,
    /// Decompressed length of `data` (== `data.len()` when uncompressed).
    pub raw_len: u32,
    /// The block bytes (possibly a compressed frame).
    pub data: Vec<u8>,
}

/// Build a `FRAME_BLOCK` frame.
pub fn block_frame(index: u32, crc: u32, raw_len: u32, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 12 + data.len());
    out.push(FRAME_BLOCK);
    out.extend_from_slice(&index.to_be_bytes());
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&raw_len.to_be_bytes());
    out.extend_from_slice(data);
    out
}

/// Parse a `FRAME_BLOCK` frame.
pub fn parse_block(frame: &[u8]) -> Option<Block> {
    if frame.first() != Some(&FRAME_BLOCK) || frame.len() < 13 {
        return None;
    }
    Some(Block {
        index: u32::from_be_bytes(frame[1..5].try_into().ok()?),
        crc: u32::from_be_bytes(frame[5..9].try_into().ok()?),
        raw_len: u32::from_be_bytes(frame[9..13].try_into().ok()?),
        data: frame[13..].to_vec(),
    })
}

/// Build a `FRAME_NAK` frame for `index`.
pub fn nak_frame(index: u32) -> Vec<u8> {
    let mut out = vec![FRAME_NAK];
    out.extend_from_slice(&index.to_be_bytes());
    out
}

/// Build a `FRAME_REQ` frame for a set of indices.
pub fn req_frame(indices: &[u32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + indices.len() * 4);
    out.push(FRAME_REQ);
    out.extend_from_slice(&(indices.len() as u32).to_be_bytes());
    for i in indices {
        out.extend_from_slice(&i.to_be_bytes());
    }
    out
}

/// Parse a `FRAME_REQ` frame into the requested indices.
pub fn parse_req(frame: &[u8]) -> Option<Vec<u32>> {
    if frame.first() != Some(&FRAME_REQ) || frame.len() < 5 {
        return None;
    }
    let count = u32::from_be_bytes(frame[1..5].try_into().ok()?) as usize;
    // A REQ frame carries exactly `count` 4-byte indices after the 5-byte
    // header. Validate the count against the ACTUAL payload length BEFORE
    // allocating: a malformed frame claiming billions of indices must not be
    // able to force a ~`count * 4` GiB pre-allocation (memory-exhaustion DoS,
    // reachable from both the server and the WASM client).
    if count > (frame.len() - 5) / 4 {
        return None;
    }
    let mut out = Vec::with_capacity(count);
    let mut off = 5usize;
    for _ in 0..count {
        out.push(u32::from_be_bytes(frame[off..off + 4].try_into().ok()?));
        off += 4;
    }
    Some(out)
}

/// Parse the block index out of a `FRAME_NAK` frame.
pub fn parse_nak(frame: &[u8]) -> Option<u32> {
    if frame.first() != Some(&FRAME_NAK) || frame.len() < 5 {
        return None;
    }
    Some(u32::from_be_bytes(frame[1..5].try_into().ok()?))
}

/// A `FRAME_WAVE_DONE` frame (empty payload).
pub fn wave_done_frame() -> Vec<u8> {
    vec![FRAME_WAVE_DONE]
}

// ---------------------------------------------------------------------------
// Checksums
// ---------------------------------------------------------------------------

/// CRC32 of a byte slice, used to verify every block in real time.
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

// ---------------------------------------------------------------------------
// Block math
// ---------------------------------------------------------------------------

/// The number of blocks covering `size` bytes at `block_size`.
pub fn block_count(size: u64, block_size: u64) -> u32 {
    let bs = block_size.max(1);
    if size == 0 {
        1
    } else {
        // Saturate instead of truncating: `div_ceil == 2³²` (e.g. 4 GiB at
        // 1-byte blocks) would otherwise wrap to 0 and silently produce an
        // empty, forever-pending transfer.
        size.div_ceil(bs).min(u32::MAX as u64) as u32
    }
}

/// The absolute `[start, end)` byte range of block `index`.
pub fn block_bounds(index: u32, block_size: u64, size: u64) -> (u64, u64) {
    let bs = block_size.max(1);
    let start = index as u64 * bs;
    // Out-of-range index: return an empty range rather than letting
    // `end - start` underflow (a caller doing `vec![0; end - start]` would
    // then try to allocate ~u64::MAX bytes and crash).
    if start >= size {
        return (size, size);
    }
    let end = start.saturating_add(bs).min(size);
    (start, end)
}

/// The absolute byte offset where block `index` of a transfer that begins at
/// file `offset` lands.
pub fn block_offset(index: u32, block_size: u64, offset: u64) -> u64 {
    offset.saturating_add(index as u64 * block_size)
}

// ---------------------------------------------------------------------------
// Verified-block set (receiver side)
// ---------------------------------------------------------------------------

/// A compact bitset tracking which blocks the receiver has verified good.
///
/// Used identically by the upload receiver (server) and the download
/// receiver (client) so both directions share the same "mark good / mark bad
/// / ask for missing" logic.
#[derive(Debug, Clone, Default)]
pub struct BlockSet {
    bits: Vec<u64>,
    total: u32,
    count: u32,
}

impl BlockSet {
    /// A fresh, empty set covering `total` blocks.
    pub fn new(total: u32) -> Self {
        BlockSet {
            bits: vec![0; (total as usize).div_ceil(64)],
            total,
            count: 0,
        }
    }

    /// Mark `index` as verified.
    pub fn insert(&mut self, index: u32) {
        if index >= self.total {
            return;
        }
        let word = (index / 64) as usize;
        let bit = 1u64 << (index % 64);
        if self.bits[word] & bit == 0 {
            self.bits[word] |= bit;
            self.count += 1;
        }
    }

    /// Whether `index` has been verified.
    pub fn contains(&self, index: u32) -> bool {
        index < self.total && (self.bits[(index / 64) as usize] & (1u64 << (index % 64))) != 0
    }

    /// Number of verified blocks.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Total blocks this set covers.
    pub fn total(&self) -> u32 {
        self.total
    }

    /// All indices still not verified, in ascending order.
    pub fn missing(&self) -> Vec<u32> {
        let mut out = Vec::new();
        for i in 0..self.total {
            if !self.contains(i) {
                out.push(i);
            }
        }
        out
    }

    /// Seed from previously-received byte ranges (resume): every block
    /// overlapping `[start, end)` is marked verified.
    pub fn seed_from_ranges(&mut self, block_size: u64, ranges: &[(u64, u64)]) {
        let bs = block_size.max(1);
        for &(start, end) in ranges {
            if end <= start {
                continue;
            }
            let first = (start / bs) as u32;
            let last = ((end - 1) / bs) as u32; // inclusive
            for i in first..=last {
                self.insert(i);
            }
        }
    }
}

/// The block indices of `size`-byte file at `block_size` that are NOT covered
/// by `received` ranges — the "transfer queue" a sender seeds on resume so it
/// only retransmits the broken/lost parts.
pub fn missing_blocks(size: u64, block_size: u64, received: &[(u64, u64)]) -> Vec<u32> {
    let total = block_count(size, block_size);
    let mut set = BlockSet::new(total);
    set.seed_from_ranges(block_size, received);
    set.missing()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_count_and_bounds() {
        assert_eq!(block_count(0, 4), 1);
        assert_eq!(block_count(10, 4), 3);
        assert_eq!(block_count(8, 4), 2);
        assert_eq!(block_bounds(1, 4, 10), (4, 8));
        assert_eq!(block_bounds(2, 4, 10), (8, 10));
        assert_eq!(block_offset(2, 4, 100), 108);
    }

    #[test]
    fn block_math_is_saturating_for_adversarial_inputs() {
        // Out-of-range index → empty range, never `end < start` (underflow).
        assert_eq!(block_bounds(u32::MAX, 1, 10), (10, 10));
        assert_eq!(block_bounds(5, 4, 10), (10, 10));
        // A block count that would exceed u32 saturates instead of wrapping
        // to 0 (which would silently make the transfer never complete).
        assert_eq!(block_count(u64::MAX, 1), u32::MAX);
        assert_eq!(block_count(4u64 * 1024 * 1024 * 1024, 1), u32::MAX);
    }

    #[test]
    fn crc_roundtrip_detects_corruption() {
        let data = b"hello libfw block";
        let crc = crc32(data);
        let mut bad = data.to_vec();
        bad[0] ^= 0xFF;
        assert_ne!(crc, crc32(&bad));
        assert_eq!(crc, crc32(data));
    }

    #[test]
    fn block_frame_roundtrip() {
        let data = vec![7u8; 100];
        let frame = block_frame(42, 12345, 100, &data);
        let parsed = parse_block(&frame).unwrap();
        assert_eq!(parsed.index, 42);
        assert_eq!(parsed.crc, 12345);
        assert_eq!(parsed.raw_len, 100);
        assert_eq!(parsed.data, data);
        assert_eq!(frame_type(&frame), Some(FRAME_BLOCK));
    }

    #[test]
    fn req_and_nak_roundtrip() {
        let req = req_frame(&[0, 3, 7, 9]);
        assert_eq!(parse_req(&req), Some(vec![0, 3, 7, 9]));
        let nak = nak_frame(5);
        assert_eq!(parse_nak(&nak), Some(5));
    }

    #[test]
    fn parse_req_rejects_oversized_count_before_allocating() {
        // A frame that CLAIMS more indices than its payload actually holds
        // must be rejected up front (not after `Vec::with_capacity(count)`),
        // so a crafted 9-byte frame cannot force a huge allocation.
        let mut frame = vec![FRAME_REQ];
        frame.extend_from_slice(&10_000_000u32.to_be_bytes());
        for i in 0..3u32 {
            frame.extend_from_slice(&i.to_be_bytes());
        }
        assert_eq!(parse_req(&frame), None);

        // A well-formed frame still parses exactly.
        let mut good = vec![FRAME_REQ];
        good.extend_from_slice(&2u32.to_be_bytes());
        good.extend_from_slice(&7u32.to_be_bytes());
        good.extend_from_slice(&9u32.to_be_bytes());
        assert_eq!(parse_req(&good), Some(vec![7, 9]));

        // Truncated/empty frames are rejected.
        assert_eq!(parse_req(&[FRAME_REQ]), None);
        assert_eq!(parse_req(&[FRAME_REQ, 0, 0, 0, 1]), None);
    }

    #[test]
    fn block_set_tracks_verified_and_missing() {
        let mut set = BlockSet::new(10);
        assert_eq!(set.total(), 10);
        set.insert(0);
        set.insert(3);
        set.insert(3); // idempotent
        assert_eq!(set.count(), 2);
        assert!(set.contains(0));
        assert!(!set.contains(1));
        assert_eq!(set.missing(), vec![1, 2, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn seed_from_ranges_marks_overlapping_blocks() {
        // size 10, block 4 → blocks 0 (0..4), 1 (4..8), 2 (8..10)
        let mut set = BlockSet::new(3);
        set.seed_from_ranges(4, &[(0, 4)]);
        assert!(set.contains(0));
        assert!(!set.contains(1));
        let mut set = BlockSet::new(3);
        set.seed_from_ranges(4, &[(4, 10)]);
        assert!(set.contains(1));
        assert!(set.contains(2));
        assert!(!set.contains(0));
    }

    #[test]
    fn missing_blocks_from_received_ranges() {
        // size 10, block 4 → blocks 0,1,2; received 0..4 → only 1,2 missing
        assert_eq!(missing_blocks(10, 4, &[(0, 4)]), vec![1, 2]);
        // nothing received → all missing
        assert_eq!(missing_blocks(10, 4, &[]), vec![0, 1, 2]);
        // everything received → none
        assert_eq!(missing_blocks(10, 4, &[(0, 10)]), Vec::<u32>::new());
    }

    #[test]
    fn control_frame_roundtrip() {
        let hello = Hello::new("tok");
        let frame = control_frame(FRAME_HELLO, &hello);
        assert_eq!(frame_type(&frame), Some(FRAME_HELLO));
        let parsed: Hello = parse_control(&frame, FRAME_HELLO).unwrap();
        assert_eq!(parsed.token, "tok");
        assert_eq!(parsed.protocol, protocol_header_value());
    }
}
