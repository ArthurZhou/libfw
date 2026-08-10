//! Protocol constants shared between server and client.

/// Fixed chunk size for fragmented transfers (2 MiB).
pub const CHUNK_SIZE: u64 = 2 * 1024 * 1024;

/// Constant sliding-window size for streaming (64 KiB).
pub const STREAM_BUF_SIZE: usize = 64 * 1024;

/// Maximum default concurrent connections in the WASM engine.
pub const DEFAULT_CONCURRENCY: usize = 4;

/// Default maximum retry count for a failed chunk before failing the task.
pub const MAX_RETRIES: u32 = 3;

/// Default cap for a single upload (100 GiB, configurable by the server).
pub const DEFAULT_MAX_UPLOAD_SIZE: u64 = 100 * 1024 * 1024 * 1024;

/// HTTP header advertising the compression algorithm on a body stream.
///
/// Value is a [`CompressionFormat`](crate::compress::CompressionFormat)
/// name (e.g. `zrip`). Mirrored by `Content-Encoding` when applicable.
pub const HEADER_COMPRESS: &str = "x-libfw-compress";

/// HTTP header carrying JSON-encoded [`FileMeta`](crate::metadata::FileMeta)
/// for an upload or download.
pub const HEADER_FILE_META: &str = "x-libfw-file-meta";

/// HTTP header carrying the byte offset to resume an interrupted upload.
pub const HEADER_OFFSET: &str = "x-libfw-offset";

/// HTTP header marking an upload request as the FINAL chunk of a file.
///
/// When present (value `1`/`true`), the server verifies that the resulting
/// file size equals the declared `x-libfw-file-meta` size before committing,
/// so a client cannot commit a truncated file. Absent (older clients) skips
/// the check — the header is optional and backward compatible.
pub const HEADER_FINAL: &str = "x-libfw-final";

/// HTTP header carrying a per-upload **session id**.
///
/// When present, the upload uses the concurrent "session" protocol: each
/// chunk carries its ABSOLUTE `x-libfw-offset` and is written into a shared
/// per-session temp file (positional writes), and the final
/// `x-libfw-final` request commits it. This lets a client pipeline many
/// chunks in flight (hiding round-trip latency) instead of serializing one
/// request per chunk. Absent → legacy per-request sequential upload.
pub const HEADER_SESSION: &str = "x-libfw-session";

/// HTTP header carrying the (optional) transfer version handshake.
pub const HEADER_PROTOCOL: &str = "x-libfw-protocol";

/// Protocol name used in the handshake header.
pub const PROTOCOL_NAME: &str = "libfw";

/// Protocol version used in the handshake header.
pub const PROTOCOL_VERSION: &str = "1";

/// The canonical `x-libfw-protocol` header value (e.g. `libfw/1`).
///
/// Both the server and the WASM client must agree on this exact value; the
/// server rejects (426) requests that explicitly advertise a different one.
/// Keep in sync with [`PROTOCOL_NAME`] / [`PROTOCOL_VERSION`].
pub fn protocol_header_value() -> &'static str {
    "libfw/1"
}

/// Whether a received `x-libfw-protocol` handshake value is compatible with
/// this build of the library.
pub fn protocol_compatible(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case(protocol_header_value())
}
