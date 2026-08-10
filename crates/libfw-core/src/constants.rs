//! Protocol constants shared between server and client.

/// Fixed chunk size for fragmented transfers (2 MiB).
pub const CHUNK_SIZE: u64 = 2 * 1024 * 1024;

/// Constant sliding-window size for streaming (64 KiB).
pub const STREAM_BUF_SIZE: usize = 64 * 1024;

/// Maximum default concurrent connections in the WASM engine.
pub const DEFAULT_CONCURRENCY: usize = 4;

/// Default in-flight chunk window for a single file's upload.
///
/// Kept independent of (and larger than) [`DEFAULT_CONCURRENCY`] so one file
/// keeps many chunks in flight on high-latency links — enough to fill the
/// bandwidth-delay product and avoid the "fill, drain, fill" stutter that a
/// tiny window (== concurrency) causes. It also sets an upper bound that
/// still works within browser per-origin connection limits (~6 for HTTP/1.1).
pub const DEFAULT_UPLOAD_WINDOW: usize = 8;

/// Default in-flight chunk window for a single file's **download**.
///
/// The tus-style parallel download fetches a large file as many concurrent
/// byte-range GETs, so a single file's throughput is bounded by bandwidth
/// instead of `chunk_size / RTT` (the same bandwidth-delay-product fill that
/// [`DEFAULT_UPLOAD_WINDOW`] provides for uploads). Independent of
/// `concurrency` (cross-file) and of the upload window. Kept modest because
/// the WASM engine buffers in-flight chunks in a reorder map so it can emit
/// them to the SDK strictly in order (append-mode writes); the buffer bound
/// is `window * download_chunk_size`.
pub const DEFAULT_DOWNLOAD_WINDOW: usize = 4;

/// Default chunk size for parallel (byte-range) downloads — 256 KiB.
///
/// Deliberately smaller than the 2 MiB upload chunk: the WASM engine holds
/// up to `download_window` of these in a reorder buffer while waiting for
/// them to arrive in order, so `window * chunk_size` (4 × 256 KiB = 1 MiB by
/// default) is the engine's worst-case extra allocation — comfortably under
/// the ~2 MiB per-file memory budget.
pub const DEFAULT_DOWNLOAD_CHUNK_SIZE: u64 = 256 * 1024;

/// A file with fewer remaining bytes than this stays on the sequential
/// single-connection download path; larger files use the parallel range-GET
/// path (which has per-request overhead that only pays off at size).
pub const MIN_PARALLEL_DOWNLOAD_BYTES: u64 = 512 * 1024;

/// Server-side TTL after which an unfinished "session" upload temp (and its
/// `.blocks` range sidecar) is considered stale and garbage-collected.
///
/// tus `Expiration`-style: a client that vanishes mid-upload would otherwise
/// leave its shared temp behind forever; the server sweeps temps older than
/// this. Uploads that are actively being written keep their temp fresh, so
/// this never interrupts an in-flight transfer.
pub const DEFAULT_SESSION_TTL: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

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
///
/// A session id should be **stable for a given file version** (derived from
/// the ETag) so an interrupted upload can be found again on resume: the
/// shared temp file is keyed by this id, and the server persists which byte
/// ranges have already been received (see [`HEADER_SESSION_STATUS`]).
pub const HEADER_SESSION: &str = "x-libfw-session";

/// HTTP header turning a session `POST` into a **status probe**.
///
/// When present (value `1`/`probe`) on a session request, the server does
/// NOT write the body. Instead it opens (creating if needed) the shared
/// per-session temp and replies with the JSON-encoded byte ranges already
/// received for that session: `{"ranges": [[start,end], ...]}`. The client
/// uses this to compute which blocks are still missing and re-send only
/// those — a BitTorrent-style "only retransmit the broken parts" resume.
///
/// Absent (older clients) keeps the previous behavior; legacy servers that
/// ignore this header simply write nothing for the empty probe body and
/// return an empty range set, which degrades to a full re-send (idempotent
/// positional writes make that correct too).
pub const HEADER_SESSION_STATUS: &str = "x-libfw-session-status";

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
