//! Streaming compression abstractions backed by [`zrip`] (zstd).
//!
//! # Design
//!
//! Both [`Compressor`] and [`Decompressor`] are *stream-oriented*: they
//! accept bounded input chunks and produce bounded output chunks, so the
//! heap footprint of processing a file does **not** grow with the file
//! size. The [`STREAM_BUF_SIZE`](crate::STREAM_BUF_SIZE) sliding window
//! keeps memory constant (~64 KiB typical, capped well below 4 MiB even
//! for adversarial frames).
//!
//! # Wire format
//!
//! The compressed body is a concatenation of independent zstd *frames*:
//! each [`Compressor::compress`] call emits one complete frame when the
//! next call arrives (or on [`Compressor::finish`]).
//!
//! The decompressor buffers compressed bytes until a complete frame is
//! present (frame boundaries are detected by a small built-in zstd frame
//! parser), then decodes exactly that frame with
//! [`zrip::decompress_with_limit`]. A frame may therefore span several
//! `decompress()` inputs, and one input may contain several frames.
//!
//! # Constant memory
//!
//! - **Compressor**: buffers at most one input chunk internally plus the
//!   zrip encoder workspace (~150 KiB).
//! - **Decompressor**: holds at most one *compressed* frame
//!   ([`MAX_PENDING_FRAME`]) and one *decompressed* frame
//!   ([`MAX_FRAME_OUTPUT`]) at a time.
//!
//! The server should feed [`Compressor::compress`] in
//! [`STREAM_BUF_SIZE`](crate::STREAM_BUF_SIZE) windows (64 KiB), keeping
//! frames ~64 KiB and the client's transient buffers small.

use std::io::Write;

use crate::error::{CompressError, DecompressError};
use crate::{CHUNK_SIZE, STREAM_BUF_SIZE};

/// Compression formats understood by libfw.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionFormat {
    /// No compression; the body passes through verbatim.
    #[serde(rename = "identity")]
    None,
    /// zstd via the [`zrip`] codec (levels -8..=4).
    Zrip,
}

impl CompressionFormat {
    /// Wire representation used in
    /// [`HEADER_COMPRESS`](crate::HEADER_COMPRESS) / `Content-Encoding`.
    pub fn as_str(self) -> &'static str {
        match self {
            CompressionFormat::None => "identity",
            CompressionFormat::Zrip => "zrip",
        }
    }

    /// Parses a header value (`identity`, `zrip`, …).
    pub fn parse_header(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "identity" | "none" => Some(CompressionFormat::None),
            "zrip" | "zstd" => Some(CompressionFormat::Zrip),
            _ => None,
        }
    }
}

/// Safety ceiling for the decompressed size of a single frame.
///
/// The protocol keeps frames ≤ [`CHUNK_SIZE`](crate::CHUNK_SIZE); this
/// limit is a guard against buggy or hostile peers.
pub const MAX_FRAME_OUTPUT: usize = CHUNK_SIZE as usize;

/// Maximum compressed bytes buffered while waiting for a frame boundary.
///
/// The worst-case incompressible [`CHUNK_SIZE`](crate::CHUNK_SIZE) chunk
/// compresses to ≈ chunk size + framing overhead, hence the slack.
pub const MAX_PENDING_FRAME: usize = CHUNK_SIZE as usize + STREAM_BUF_SIZE;

/// Default cap on the total bytes a single [`Decompressor::decompress`] call
/// may append to the output buffer.
///
/// A hostile peer could otherwise send many *small* frames in one network
/// chunk, each expanding to [`MAX_FRAME_OUTPUT`] — inflating memory by a
/// large factor before the caller drains the buffer. The default (a handful
/// of frames) keeps the transient peak bounded while comfortably allowing
/// legitimate coalesced reads (e.g. a browser delivering several 64 KiB
/// download frames at once).
pub const MAX_OUTPUT_PER_CALL: usize = MAX_FRAME_OUTPUT.saturating_mul(8);

/// Default zrip compression level (Fast strategy, good for network transfer).
pub const ZRIP_DEFAULT_LEVEL: i32 = 1;

/// Lowest zrip compression level (Fast strategy; least CPU, worst ratio).
pub const ZRIP_MIN_LEVEL: i32 = -8;

/// Highest zrip compression level (bounded memory, best ratio).
pub const ZRIP_MAX_LEVEL: i32 = 4;

/// Whether `level` is an addressable zrip compression level.
pub fn is_valid_zrip_level(level: i32) -> bool {
    (ZRIP_MIN_LEVEL..=ZRIP_MAX_LEVEL).contains(&level)
}

/// Clamp a requested zrip level into `min..=max`.
///
/// Servers must never trust a client-supplied level: out-of-range requests
/// are clamped (and the actual level echoed back on the response header),
/// keeping the wire format honest without hard-failing older clients.
pub fn negotiate_level(req: Option<i32>, min: i32, max: i32, default: i32) -> i32 {
    match req {
        None => default,
        Some(l) => l.clamp(min, max),
    }
}

/// Streaming compressor. Feed it bounded input chunks; it appends the
/// compressed bytes produced so far to the provided output buffer.
pub trait Compressor: Send {
    /// The format this compressor produces.
    fn format(&self) -> CompressionFormat;

    /// Compress `input`, appending compressed bytes to `out`.
    ///
    /// `input` should be kept ≤ [`CHUNK_SIZE`](crate::CHUNK_SIZE) to keep
    /// the output frames small and memory constant. When the next chunk
    /// arrives, the previous chunk's frame is finalized and emitted.
    fn compress(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CompressError>;

    /// Finalize the stream, appending the last frame (if any) to `out`.
    ///
    /// Must be called exactly once; afterwards the compressor is spent.
    fn finish(&mut self, out: &mut Vec<u8>) -> Result<(), CompressError>;
}

/// Streaming decompressor. Feed it arbitrary compressed bytes; decoded
/// output is appended to the provided buffer.
pub trait Decompressor: Send {
    /// The format this decompressor consumes.
    fn format(&self) -> CompressionFormat;

    /// Decompress `input`, appending decoded bytes to `out`.
    ///
    /// Input may split frames arbitrarily. `out` should be drained by the
    /// caller after each call to keep memory bounded.
    fn decompress(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), DecompressError>;

    /// Signal the end of the stream.
    ///
    /// Flushes any complete trailing frames and verifies the stream is
    /// well-formed (no truncated frames). Call exactly once.
    fn finish(&mut self, out: &mut Vec<u8>) -> Result<(), DecompressError>;
}

/// Construct a compressor for `format`.
pub fn compressor(format: CompressionFormat) -> Result<Box<dyn Compressor>, CompressError> {
    compressor_with_level(format, ZRIP_DEFAULT_LEVEL)
}

/// Construct a compressor for `format` at an explicit zrip level.
///
/// The level is only meaningful for [`CompressionFormat::Zrip`]; for
/// [`CompressionFormat::None`] the body passes through verbatim regardless
/// of `level`. Callers should pre-validate/clamp `level` with
/// [`negotiate_level`] before constructing (constructors still error on
/// out-of-range levels from zrip itself).
pub fn compressor_with_level(
    format: CompressionFormat,
    level: i32,
) -> Result<Box<dyn Compressor>, CompressError> {
    match format {
        CompressionFormat::None => Ok(Box::new(PassthroughCompressor)),
        CompressionFormat::Zrip => Ok(Box::new(ZripCompressor::new(level)?)),
    }
}

/// Construct a decompressor for `format`.
pub fn decompressor(format: CompressionFormat) -> Box<dyn Decompressor> {
    decompressor_with_limit(format, MAX_OUTPUT_PER_CALL)
}

/// Construct a decompressor for `format` with an explicit per-call output
/// budget (bytes a single [`Decompressor::decompress`] call may append).
///
/// Use a tight budget (e.g. [`MAX_FRAME_OUTPUT`]) on the server where the
/// peer is potentially hostile; use the default generous budget on the
/// client so coalesced multi-frame reads are never rejected.
pub fn decompressor_with_limit(
    format: CompressionFormat,
    max_output_per_call: usize,
) -> Box<dyn Decompressor> {
    match format {
        CompressionFormat::None => Box::new(PassthroughDecompressor),
        CompressionFormat::Zrip => Box::new(ZripDecompressor::with_max_output(max_output_per_call)),
    }
}

// ---------------------------------------------------------------------------
// Passthrough (identity)
// ---------------------------------------------------------------------------

struct PassthroughCompressor;

impl Compressor for PassthroughCompressor {
    fn format(&self) -> CompressionFormat {
        CompressionFormat::None
    }

    fn compress(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CompressError> {
        out.extend_from_slice(input);
        Ok(())
    }

    fn finish(&mut self, _out: &mut Vec<u8>) -> Result<(), CompressError> {
        Ok(())
    }
}

struct PassthroughDecompressor;

impl Decompressor for PassthroughDecompressor {
    fn format(&self) -> CompressionFormat {
        CompressionFormat::None
    }

    fn decompress(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), DecompressError> {
        out.extend_from_slice(input);
        Ok(())
    }

    fn finish(&mut self, _out: &mut Vec<u8>) -> Result<(), DecompressError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// zrip (zstd) implementation
// ---------------------------------------------------------------------------

/// zstd compressor emitting one independent frame per input chunk.
pub struct ZripCompressor {
    encoder: Option<zrip::FrameEncoder<Vec<u8>>>,
    /// Whether data has been written since the last frame boundary.
    dirty: bool,
}

impl ZripCompressor {
    /// Create a compressor at `level` (-8..=4; 0 = library default).
    pub fn new(level: i32) -> Result<Self, CompressError> {
        Ok(ZripCompressor {
            encoder: Some(zrip::FrameEncoder::new(Vec::new(), level)?),
            dirty: false,
        })
    }
}

impl Compressor for ZripCompressor {
    fn format(&self) -> CompressionFormat {
        CompressionFormat::Zrip
    }

    fn compress(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CompressError> {
        if input.is_empty() {
            return Ok(());
        }
        let encoder = self
            .encoder
            .as_mut()
            .ok_or_else(|| std::io::Error::other("compressor already finished"))?;
        // Finalize the previous chunk's frame and hand its compressed bytes back.
        if self.dirty {
            let finished = encoder.reset(Vec::new())?;
            out.extend_from_slice(&finished);
        }
        encoder.write_all(input)?;
        self.dirty = true;
        Ok(())
    }

    fn finish(&mut self, out: &mut Vec<u8>) -> Result<(), CompressError> {
        let encoder = self
            .encoder
            .take()
            .ok_or_else(|| std::io::Error::other("compressor already finished"))?;
        let tail = encoder.finish()?;
        out.extend_from_slice(&tail);
        self.dirty = false;
        Ok(())
    }
}

/// zstd decompressor reassembling frames across arbitrary chunk boundaries.
pub struct ZripDecompressor {
    /// Compressed bytes not yet turned into complete frames.
    pending: Vec<u8>,
    finished: bool,
    /// Hard cap on the total bytes appended to `out` during a single
    /// [`Decompressor::decompress`] / [`Decompressor::finish`] call.
    max_output_per_call: usize,
}

impl ZripDecompressor {
    /// Create a decompressor with the default per-call output budget
    /// ([`MAX_OUTPUT_PER_CALL`]).
    pub fn new() -> Self {
        ZripDecompressor::with_max_output(MAX_OUTPUT_PER_CALL)
    }

    /// Create a decompressor with an explicit per-call output budget.
    pub fn with_max_output(max_output_per_call: usize) -> Self {
        ZripDecompressor {
            pending: Vec::with_capacity(STREAM_BUF_SIZE),
            finished: false,
            max_output_per_call,
        }
    }

    /// Decode as many complete frames as `pending` holds, appending at most
    /// `max_add` bytes to `out` across the whole call.
    fn drain_frames(&mut self, out: &mut Vec<u8>, max_add: usize) -> Result<(), DecompressError> {
        let mut added = 0usize;
        loop {
            let boundary = frame_boundary(&self.pending);
            match boundary {
                FrameBoundary::Complete { len, content_size } => {
                    if content_size.is_some_and(|cs| cs > MAX_FRAME_OUTPUT as u64) {
                        return Err(DecompressError::TooLarge {
                            limit: MAX_FRAME_OUTPUT,
                        });
                    }
                    let decoded = {
                        let frame = &self.pending[..len];
                        zrip::decompress_with_limit(frame, MAX_FRAME_OUTPUT).map_err(|e| {
                            DecompressError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                e,
                            ))
                        })?
                    };
                    // Enforce the per-call budget *before* appending so the
                    // memory spike of a multi-frame bomb never materializes.
                    if added.saturating_add(decoded.len()) > max_add {
                        return Err(DecompressError::TooLarge { limit: max_add });
                    }
                    out.extend_from_slice(&decoded);
                    added = added.saturating_add(decoded.len());
                    self.pending.drain(..len);
                }
                FrameBoundary::Incomplete => return Ok(()),
                FrameBoundary::Invalid => {
                    return Err(DecompressError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid zstd frame data",
                    )))
                }
            }
        }
    }
}

impl Default for ZripDecompressor {
    fn default() -> Self {
        ZripDecompressor::new()
    }
}

impl Decompressor for ZripDecompressor {
    fn format(&self) -> CompressionFormat {
        CompressionFormat::Zrip
    }

    fn decompress(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), DecompressError> {
        if self.finished {
            return Err(DecompressError::Io(std::io::Error::other(
                "decompressor already finished",
            )));
        }
        if !input.is_empty() {
            self.pending.extend_from_slice(input);
        }
        self.drain_frames(out, self.max_output_per_call)?;
        if self.pending.len() > MAX_PENDING_FRAME {
            return Err(DecompressError::TooLarge {
                limit: MAX_PENDING_FRAME,
            });
        }
        Ok(())
    }

    fn finish(&mut self, out: &mut Vec<u8>) -> Result<(), DecompressError> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.drain_frames(out, self.max_output_per_call)?;
        if !self.pending.is_empty() {
            return Err(DecompressError::Truncated(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("{} trailing compressed bytes", self.pending.len()),
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Minimal zstd frame-boundary parser
// ---------------------------------------------------------------------------

const ZSTD_MAGIC: u32 = 0xFD2F_B528;
const SKIPPABLE_MASK: u32 = 0xFFFF_FFF0;
const SKIPPABLE_MAGIC: u32 = 0x184D_2A50;

enum FrameBoundary {
    /// A complete frame occupying `len` bytes, declaring `content_size`
    /// output bytes (`None` when the frame header does not state it).
    Complete { len: usize, content_size: Option<u64> },
    /// Need more bytes to know the boundary.
    Incomplete,
    /// Corrupt / unsupported.
    Invalid,
}

/// Determine the byte length of the next zstd frame in `buf` (if present).
fn frame_boundary(buf: &[u8]) -> FrameBoundary {
    if buf.len() < 4 {
        return FrameBoundary::Incomplete;
    }
    let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic == ZSTD_MAGIC {
        zstd_frame_boundary(buf)
    } else if (magic & SKIPPABLE_MASK) == SKIPPABLE_MAGIC {
        skippable_frame_boundary(buf)
    } else {
        FrameBoundary::Invalid
    }
}

/// Skippable frames: `magic(4) | skip_size u32(4) | payload(skip_size)`.
fn skippable_frame_boundary(buf: &[u8]) -> FrameBoundary {
    if buf.len() < 8 {
        return FrameBoundary::Incomplete;
    }
    let skip = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    let total = 8usize.saturating_add(skip);
    if buf.len() >= total {
        FrameBoundary::Complete {
            len: total,
            content_size: Some(0),
        }
    } else {
        FrameBoundary::Incomplete
    }
}

/// Walk the frame header + blocks of a standard zstd frame.
///
/// Layout: `magic(4) descriptor(1) [window(1)] [dict_id(0/1/2/4)]
/// [content_size(0/1/2/4/8)] block* checksum(0/4)`.
fn zstd_frame_boundary(buf: &[u8]) -> FrameBoundary {
    if buf.len() < 5 {
        return FrameBoundary::Incomplete;
    }
    let descriptor = buf[4];
    // Reserved bits (3..=4) must be zero.
    if descriptor & 0x18 != 0 {
        return FrameBoundary::Invalid;
    }
    let single_segment = descriptor & 0x20 != 0;
    let checksum = descriptor & 0x04 != 0;
    let dict_id_flag = descriptor & 0x03;
    let fcs_flag = (descriptor >> 6) & 0x03;

    let mut hdr_len = 5usize;
    if !single_segment {
        hdr_len += 1; // window descriptor
    }
    let dict_id_size = match dict_id_flag {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 4,
        _ => return FrameBoundary::Invalid,
    };
    hdr_len += dict_id_size;
    let fcs_size: usize = match fcs_flag {
        0 if single_segment => 1,
        0 => 0,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => return FrameBoundary::Invalid,
    };
    hdr_len += fcs_size;

    if buf.len() < hdr_len {
        return FrameBoundary::Incomplete;
    }
    let content_size = if fcs_size > 0 {
        let mut v = 0u64;
        for (i, &b) in buf[5..5 + fcs_size].iter().enumerate() {
            v |= (b as u64) << (8 * i);
        }
        Some(v)
    } else {
        None
    };

    // Walk blocks until the last-block flag.
    let mut off = hdr_len;
    loop {
        if buf.len() < off + 3 {
            return FrameBoundary::Incomplete;
        }
        // Standard zstd block header (as read by the reference decoder):
        // bit 0 = last_block, bits 1..=2 = block_type, bits 3..=23 = size.
        let block_header = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], 0]);
        let last = block_header & 0x01 != 0;
        let block_type = (block_header >> 1) & 0x03;
        let block_size = (block_header >> 3) as usize;
        if block_type == 3 {
            // Reserved block type.
            return FrameBoundary::Invalid;
        }
        off += 3 + block_size;
        if off > MAX_PENDING_FRAME {
            return FrameBoundary::Invalid;
        }
        if last {
            break;
        }
    }
    if checksum {
        off += 4;
    }
    if buf.len() < off {
        return FrameBoundary::Incomplete;
    }
    FrameBoundary::Complete {
        len: off,
        content_size,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_chunks(data: &[u8], feed: &[usize]) {
        let mut c = ZripCompressor::new(ZRIP_DEFAULT_LEVEL).unwrap();
        let mut compressed = Vec::new();
        let mut off = 0;
        for &n in feed {
            let end = (off + n).min(data.len());
            if end > off {
                c.compress(&data[off..end], &mut compressed).unwrap();
            }
            off = end;
        }
        c.finish(&mut compressed).unwrap();
        assert!(!compressed.is_empty());

        // Decode with arbitrarily split inputs, including mid-frame splits.
        let mut d = ZripDecompressor::new();
        let mut plain = Vec::new();
        let mut step = 1;
        let mut i = 0;
        while i < compressed.len() {
            let end = (i + step).min(compressed.len());
            d.decompress(&compressed[i..end], &mut plain).unwrap();
            i = end;
            step = step % 5 + 1; // 1..=5
        }
        d.finish(&mut plain).unwrap();
        assert_eq!(plain, data, "roundtrip mismatch with feed {feed:?}");
    }

    #[test]
    fn roundtrip_single_chunk() {
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        roundtrip_chunks(&data, &[data.len()]);
    }

    #[test]
    fn roundtrip_multi_chunk_64k_windows() {
        // Server-style: feed in 64 KiB sliding windows.
        let data: Vec<u8> = (0..300_000u32).map(|i| (i / 7) as u8).collect();
        let feed: Vec<usize> = std::iter::repeat(STREAM_BUF_SIZE).take(5).collect();
        roundtrip_chunks(&data, &feed);
    }

    #[test]
    fn roundtrip_highly_compressible() {
        let data = b"libfw streaming compression test. ".repeat(10_000);
        // Feed sizes cycling 1KiB..16KiB until the whole input is consumed.
        let mut feed = Vec::new();
        let mut consumed = 0;
        for (_, &size) in [1024usize, 2048, 4096, 8192, 16384].iter().cycle().enumerate() {
            feed.push(size);
            consumed += size;
            if consumed >= data.len() {
                break;
            }
        }
        roundtrip_chunks(&data, &feed);
    }

    #[test]
    fn roundtrip_empty_stream() {
        let mut c = ZripCompressor::new(ZRIP_DEFAULT_LEVEL).unwrap();
        let mut compressed = Vec::new();
        c.finish(&mut compressed).unwrap();

        let mut d = ZripDecompressor::new();
        let mut plain = Vec::new();
        d.decompress(&compressed, &mut plain).unwrap();
        d.finish(&mut plain).unwrap();
        assert!(plain.is_empty());
    }

    #[test]
    fn malformed_zstd_headers_are_rejected_without_panicking() {
        // Keep the parser defensive: malformed / hostile frame headers must
        // surface as Invalid instead of crashing the WASM guest with an
        // internal `unreachable!`.
        let bad = [0xFD, 0x2F, 0xB5, 0x28, 0xFF];
        assert!(matches!(frame_boundary(&bad), FrameBoundary::Invalid));

        let bad2 = [0xFD, 0x2F, 0xB5, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert!(matches!(frame_boundary(&bad2), FrameBoundary::Invalid));
    }

    #[test]
    fn empty_input_chunks_are_noops() {
        let mut c = ZripCompressor::new(ZRIP_DEFAULT_LEVEL).unwrap();
        let mut out = Vec::new();
        c.compress(&[], &mut out).unwrap();
        c.compress(b"hello", &mut out).unwrap();
        c.finish(&mut out).unwrap();

        let mut d = ZripDecompressor::new();
        let mut plain = Vec::new();
        d.decompress(&out, &mut plain).unwrap();
        d.finish(&mut plain).unwrap();
        assert_eq!(plain, b"hello");
    }

    #[test]
    fn format_header_roundtrip() {
        assert_eq!(CompressionFormat::parse_header("zrip"), Some(CompressionFormat::Zrip));
        assert_eq!(CompressionFormat::parse_header("ZSTD"), Some(CompressionFormat::Zrip));
        assert_eq!(CompressionFormat::parse_header("identity"), Some(CompressionFormat::None));
        assert_eq!(CompressionFormat::parse_header(""), Some(CompressionFormat::None));
        assert_eq!(CompressionFormat::parse_header("br"), None);
        assert_eq!(CompressionFormat::Zrip.as_str(), "zrip");
        assert_eq!(CompressionFormat::None.as_str(), "identity");
    }

    #[test]
    fn passthrough_roundtrip() {
        let mut c = compressor(CompressionFormat::None).unwrap();
        let mut d = decompressor(CompressionFormat::None);
        let mut compressed = Vec::new();
        let mut plain = Vec::new();
        c.compress(b"abc", &mut compressed).unwrap();
        c.finish(&mut compressed).unwrap();
        d.decompress(&compressed, &mut plain).unwrap();
        d.finish(&mut plain).unwrap();
        assert_eq!(plain, b"abc");
    }

    #[test]
    fn truncated_stream_is_detected() {
        let mut c = ZripCompressor::new(ZRIP_DEFAULT_LEVEL).unwrap();
        let mut compressed = Vec::new();
        c.compress(&vec![7u8; 5000], &mut compressed).unwrap();
        c.finish(&mut compressed).unwrap();
        compressed.truncate(compressed.len() - 1); // chop one byte

        let mut d = ZripDecompressor::new();
        let mut plain = Vec::new();
        d.decompress(&compressed, &mut plain).unwrap();
        assert!(matches!(d.finish(&mut plain), Err(DecompressError::Truncated(_))));
    }

    #[test]
    fn corrupt_stream_is_detected() {
        let mut d = ZripDecompressor::new();
        let mut plain = Vec::new();
        let err = d.decompress(b"this is not a zstd frame at all", &mut plain);
        assert!(err.is_err());
    }

    #[test]
    fn per_call_output_budget_rejects_multi_frame_bomb() {
        // A hostile peer delivers many small frames in ONE decompress() call;
        // the cumulative decoded output must trip the per-call budget instead
        // of ballooning memory.
        let mut compressed = Vec::new();
        for _ in 0..64 {
            let mut c = ZripCompressor::new(ZRIP_DEFAULT_LEVEL).unwrap();
            c.compress(&vec![7u8; STREAM_BUF_SIZE], &mut compressed)
                .unwrap();
            c.finish(&mut compressed).unwrap();
        }
        let mut d = ZripDecompressor::with_max_output(MAX_FRAME_OUTPUT);
        let mut plain = Vec::new();
        let err = d.decompress(&compressed, &mut plain);
        assert!(
            matches!(err, Err(DecompressError::TooLarge { .. })),
            "expected TooLarge, got {err:?}"
        );
        // The buffer must not have been inflated past the budget.
        assert!(plain.len() <= MAX_FRAME_OUTPUT);
    }

    #[test]
    fn generous_default_budget_allows_coalesced_frames() {
        // Several small frames in one call are fine under the default budget
        // (what a browser may hand the client's decompressor).
        let mut compressed = Vec::new();
        for _ in 0..8 {
            let mut c = ZripCompressor::new(ZRIP_DEFAULT_LEVEL).unwrap();
            c.compress(&vec![7u8; STREAM_BUF_SIZE], &mut compressed)
                .unwrap();
            c.finish(&mut compressed).unwrap();
        }
        let mut d = ZripDecompressor::new();
        let mut plain = Vec::new();
        d.decompress(&compressed, &mut plain).unwrap();
        d.finish(&mut plain).unwrap();
        assert_eq!(plain.len(), 8 * STREAM_BUF_SIZE);
    }

    #[test]
    fn zstd_compat_interop() {
        // Our frames are standard zstd: the reference `zstd` codec can
        // decode them, and we can decode its output.
        let data: Vec<u8> = (0..50_000u32).map(|i| (i % 31) as u8).collect();
        let mut c = ZripCompressor::new(ZRIP_DEFAULT_LEVEL).unwrap();
        let mut compressed = Vec::new();
        c.compress(&data, &mut compressed).unwrap();
        c.finish(&mut compressed).unwrap();
        let decoded = zstd::stream::decode_all(&compressed[..]).unwrap();
        assert_eq!(decoded, data);

        let zstd_enc = zstd::stream::encode_all(&data[..], 1).unwrap();
        let mut d = ZripDecompressor::new();
        let mut plain = Vec::new();
        d.decompress(&zstd_enc, &mut plain).unwrap();
        d.finish(&mut plain).unwrap();
        assert_eq!(plain, data);
    }
}
