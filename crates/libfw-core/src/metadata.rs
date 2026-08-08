//! Transfer metadata: file info, chunk plans and ETags.
//!
//! These structures are serialized to JSON and exchanged through
//! [`HEADER_FILE_META`](crate::HEADER_FILE_META) and the transfer
//! manifest so that server and client agree on chunk boundaries and can
//! validate resume offsets.

use serde::{Deserialize, Serialize};

use crate::CHUNK_SIZE;

/// Metadata about a single file involved in a transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMeta {
    /// Relative path (POSIX separators), e.g. `dir/sub/file.txt`.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Last-modified time as unix seconds.
    #[serde(default)]
    pub mtime: u64,
    /// Stable identifier for content — see [`etag_from_size_mtime`].
    #[serde(default)]
    pub etag: String,
}

impl FileMeta {
    /// Constructs a `FileMeta` and computes the ETag from size + mtime.
    pub fn new(path: impl Into<String>, size: u64, mtime: u64) -> Self {
        let path = path.into();
        let etag = etag_from_size_mtime(size, mtime);
        FileMeta {
            path,
            size,
            mtime,
            etag,
        }
    }
}

/// A contiguous byte range of a file: `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRange {
    /// First byte (inclusive).
    pub start: u64,
    /// One past the last byte (exclusive).
    pub end: u64,
}

impl ChunkRange {
    /// Number of bytes in this range.
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// True when the range covers zero bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One chunk of a file transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkMeta {
    /// Zero-based chunk index.
    pub index: u32,
    /// Byte offset of this chunk within the file.
    pub offset: u64,
    /// Byte length of this chunk.
    pub size: u64,
}

/// The full transfer plan for one file: chunk layout derived from size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferPlan {
    /// The file being transferred.
    pub file: FileMeta,
    /// Chunk size used to slice the file.
    pub chunk_size: u64,
    /// All chunks, in order.
    pub chunks: Vec<ChunkMeta>,
}

impl TransferPlan {
    /// Build a plan for `file` using the protocol default
    /// [`CHUNK_SIZE`](crate::CHUNK_SIZE).
    pub fn new(file: FileMeta) -> Self {
        TransferPlan::with_chunk_size(file, CHUNK_SIZE)
    }

    /// Build a plan for `file` with an explicit chunk size.
    ///
    /// The last chunk may be shorter than `chunk_size`.
    pub fn with_chunk_size(file: FileMeta, chunk_size: u64) -> Self {
        debug_assert!(chunk_size > 0);
        let mut chunks = Vec::new();
        let mut offset = 0u64;
        while offset < file.size {
            let len = chunk_size.min(file.size - offset);
            chunks.push(ChunkMeta {
                index: chunks.len() as u32,
                offset,
                size: len,
            });
            offset += len;
        }
        if file.size == 0 {
            chunks.push(ChunkMeta {
                index: 0,
                offset: 0,
                size: 0,
            });
        }
        TransferPlan {
            file,
            chunk_size,
            chunks,
        }
    }

    /// The total number of bytes covered by the plan.
    pub fn total_bytes(&self) -> u64 {
        self.chunks.iter().map(|c| c.size).sum()
    }
}

/// Compute a deterministic, content-independent ETag from size + mtime.
///
/// This is a *strong* ETag in the sense of being stable for a given file
/// version, but it does not read the file contents; two files with equal
/// size and mtime collide (acceptable for resume-validation purposes).
pub fn etag_from_size_mtime(size: u64, mtime: u64) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(size.to_le_bytes());
    h.update(mtime.to_le_bytes());
    let digest = h.finalize();
    format!("\"{}\"", hex(&digest[..8]))
}

/// Hex-encode a byte slice (lowercase, no prefix).
pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Serialize [`FileMeta`] for the [`HEADER_FILE_META`] header.
pub fn encode_file_meta(meta: &FileMeta) -> String {
    serde_json::to_string(meta).expect("FileMeta serializes")
}

/// Parse [`FileMeta`] from the [`HEADER_FILE_META`] header.
pub fn decode_file_meta(header: &str) -> Result<FileMeta, serde_json::Error> {
    serde_json::from_str(header)
}

/// Base64 alphabet (RFC 4648 §4, standard).
const B64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Base64-encode `input` (standard alphabet, `=` padding).
pub fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(B64_ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64_ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64_ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Base64-decode `input` (standard alphabet, `=`/whitespace tolerated).
pub fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(input.len().div_ceil(4) * 3);
    let mut buf = 0u32;
    let mut bits = 0u32;
    for c in input.chars() {
        if c == '=' || c.is_whitespace() {
            continue;
        }
        let val = match c {
            'A'..='Z' => c as u32 - 'A' as u32,
            'a'..='z' => c as u32 - 'a' as u32 + 26,
            '0'..='9' => c as u32 - '0' as u32 + 52,
            '+' => 62,
            '/' => 63,
            _ => return Err(format!("invalid base64 character: {c:?}")),
        };
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Ok(out)
}

/// Encode [`FileMeta`] for the [`HEADER_FILE_META`] header.
///
/// The raw JSON is base64-encoded so that non-Latin-1 virtual paths (e.g.
/// CJK filenames) survive HTTP header transport — browsers reject header
/// values containing characters outside ISO-8859-1.
pub fn encode_file_meta_header(meta: &FileMeta) -> String {
    base64_encode(encode_file_meta(meta).as_bytes())
}

/// Decode [`FileMeta`] from the [`HEADER_FILE_META`] header.
pub fn decode_file_meta_header(header: &str) -> Result<FileMeta, serde_json::Error> {
    let raw = base64_decode(header)
        .map_err(|e| serde_json::Error::io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
    decode_file_meta(&String::from_utf8_lossy(&raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_chunks_evenly() {
        let meta = FileMeta::new("/a/b.bin", 10, 0);
        let plan = TransferPlan::with_chunk_size(meta, 4);
        assert_eq!(plan.chunks.len(), 3);
        assert_eq!(
            plan.chunks.iter().map(|c| c.offset).collect::<Vec<_>>(),
            vec![0, 4, 8]
        );
        assert_eq!(
            plan.chunks.iter().map(|c| c.size).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
        assert_eq!(plan.total_bytes(), 10);
    }

    #[test]
    fn plan_exact_multiple() {
        let meta = FileMeta::new("/f", 8, 0);
        let plan = TransferPlan::with_chunk_size(meta, 4);
        assert_eq!(plan.chunks.len(), 2);
        assert_eq!(plan.total_bytes(), 8);
    }

    #[test]
    fn plan_empty_file_has_one_zero_chunk() {
        let meta = FileMeta::new("/empty", 0, 0);
        let plan = TransferPlan::new(meta);
        assert_eq!(plan.chunks.len(), 1);
        assert_eq!(plan.chunks[0].size, 0);
    }

    #[test]
    fn etag_is_stable_and_quoted() {
        let a = etag_from_size_mtime(100, 1_700_000_000);
        let b = etag_from_size_mtime(100, 1_700_000_000);
        let c = etag_from_size_mtime(101, 1_700_000_000);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.starts_with('"') && a.ends_with('"'));
    }

    #[test]
    fn file_meta_json_roundtrip() {
        let meta = FileMeta::new("dir/file.txt", 1234, 42);
        let encoded = encode_file_meta(&meta);
        assert!(encoded.contains("etag"));
        let decoded = decode_file_meta(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn base64_roundtrip_arbitrary_bytes() {
        for bytes in [
            b"".to_vec(),
            b"f".to_vec(),
            b"fo".to_vec(),
            b"foo".to_vec(),
            b"foob".to_vec(),
            vec![0u8, 1, 2, 3, 255, 128, 64],
            "中文文件名🚀".as_bytes().to_vec(),
        ] {
            let enc = base64_encode(&bytes);
            assert_eq!(base64_decode(&enc).unwrap(), bytes, "for {bytes:?}");
        }
    }

    #[test]
    fn base64_encode_known_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
    }

    #[test]
    fn file_meta_header_roundtrip_with_unicode_path() {
        // A CJK filename must survive the header-encoding roundtrip.
        let meta = FileMeta::new("目录/报告.txt", 999, 7);
        let encoded = encode_file_meta_header(&meta);
        // The base64 form is pure ASCII — safe as an HTTP header value.
        assert!(encoded.is_ascii());
        let decoded = decode_file_meta_header(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn decode_file_meta_header_rejects_garbage() {
        assert!(decode_file_meta_header("!!!not-base64!!!").is_err());
    }
}
