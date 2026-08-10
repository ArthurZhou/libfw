//! Storage abstraction for `libfw-server`.
//!
//! Implement [`StorageBackend`] to plug in any storage — a local
//! filesystem, object storage, in-memory fixtures, … — behind the same
//! streaming API. Streams are *pull/push* based so memory stays constant
//! regardless of file size.

use std::io::Read;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::StorageError;
use crate::metadata::FileMeta;
use crate::range::RangeSpec;

/// How an upload should open (or resume) its target stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Create the file; fail with `AlreadyExists` if present.
    Create,
    /// Create or truncate the file.
    Overwrite,
    /// Continue writing at `offset`; fail if the file is not exactly
    /// `offset` bytes yet.
    Resume { offset: u64 },
}

/// A directory listing entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirEntry {
    /// Full virtual path (relative to the mounted root).
    pub path: String,
    /// Whether this entry is a directory.
    pub is_dir: bool,
    /// Byte size (0 for directories).
    pub size: u64,
    /// Last-modified unix time.
    pub mtime: u64,
}

/// Streaming write handle returned by [`StorageBackend::write_stream`].
///
/// Data written goes to a temporary location (typically a temp file)
/// until [`UploadSink::commit`] atomically renames it into place; a
/// failed or aborted upload leaves no partial target behind.
#[async_trait]
pub trait UploadSink: Send {
    /// Append `buf` at the sink's current position.
    async fn write(&mut self, buf: &[u8]) -> Result<(), StorageError>;

    /// Write `buf` at an absolute `offset` within the destination.
    ///
    /// Sequential-only sinks may ignore `offset` and append instead (the
    /// default). Positional sinks (used by the concurrent "session" upload
    /// path) seek to `offset` first, so out-of-order chunks land in the
    /// right place.
    async fn write_at(&mut self, _offset: u64, buf: &[u8]) -> Result<(), StorageError> {
        self.write(buf).await
    }

    /// Current length of the destination, for size validation before commit.
    async fn len(&self) -> Result<u64, StorageError>;

    /// Finish the stream, finalize the destination and return its metadata.
    async fn commit(self: Box<Self>) -> Result<FileMeta, StorageError>;

    /// Discard the temporary data and clean up.
    async fn abort(self: Box<Self>) -> Result<(), StorageError>;
}

/// Pluggable storage backend for `libfw-server`.
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Return metadata for `path`, or `None` when it does not exist.
    async fn file_meta(&self, path: &str) -> Result<Option<FileMeta>, StorageError>;

    /// Open a read stream for `path` restricted to `range`.
    ///
    /// The returned reader yields exactly `range.len()` bytes on success.
    async fn read_stream(
        &self,
        path: &str,
        range: RangeSpec,
    ) -> Result<Box<dyn Read + Send>, StorageError>;

    /// Open a write stream for `path` according to `mode`.
    async fn write_stream(&self, path: &str, mode: WriteMode) -> Result<Box<dyn UploadSink>, StorageError>;

    /// Open a positional write stream for a **concurrent** "session" upload.
    ///
    /// All chunk requests for one file share the same `session` id and write
    /// into a single shared temp file at absolute offsets via
    /// [`UploadSink::write_at`]; only the final (commit) request renames it
    /// into place. The first request (which creates the temp) uses `mode`
    /// for the Create/Overwrite/Resume semantics; later requests ignore it.
    async fn write_stream_session(
        &self,
        path: &str,
        _session: &str,
        mode: WriteMode,
    ) -> Result<Box<dyn UploadSink>, StorageError> {
        // Default: single-request path is not concurrent; behave like a
        // normal `write_stream` for backends that don't opt into sessions.
        self.write_stream(path, mode).await
    }

    /// List the children of directory `path` (or the mount root when
    /// `path` is empty).
    async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, StorageError>;

    /// Recursively create `path` (and parents) as a directory.
    async fn mkdir_all(&self, path: &str) -> Result<(), StorageError>;

    /// Remove `path` (file, or directory recursively).
    async fn remove(&self, path: &str) -> Result<(), StorageError>;
}
