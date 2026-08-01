//! Error types for the libfw core contracts.

use std::io;

/// Error produced by [`Compressor`](crate::compress::Compressor)
/// implementations.
#[derive(Debug, thiserror::Error)]
pub enum CompressError {
    /// An invalid compression level was requested.
    #[error("invalid compression level: {0}")]
    InvalidLevel(i32),
    /// The underlying encoder failed.
    #[error("io error while compressing: {0}")]
    Io(#[from] io::Error),
    /// A zrip encoder error occurred.
    #[error("zrip compression error: {0}")]
    Zrip(#[from] zrip::CompressError),
}

/// Error produced by [`Decompressor`](crate::compress::Decompressor)
/// implementations.
#[derive(Debug, thiserror::Error)]
pub enum DecompressError {
    /// The stream ended before a complete frame was delivered.
    #[error("truncated compressed stream: {0}")]
    Truncated(io::Error),
    /// The underlying decoder failed.
    #[error("io error while decompressing: {0}")]
    Io(#[from] io::Error),
    /// A decoded frame exceeded the safety output limit.
    #[error("frame output exceeds safety limit of {limit} bytes")]
    TooLarge {
        /// The configured safety limit.
        limit: usize,
    },
}

/// Error produced by [`StorageBackend`](crate::storage::StorageBackend)
/// implementations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The requested path does not exist.
    #[error("path not found: {0}")]
    NotFound(String),
    /// The requested path already exists and must not be overwritten.
    #[error("path already exists: {0}")]
    AlreadyExists(String),
    /// The backend has insufficient capacity / the file is too large.
    #[error("file too large: {0}")]
    TooLarge(u64),
    /// A write failed mid-stream (e.g. resume offset mismatch).
    #[error("write failed at offset {offset}: {source}")]
    WriteFailed {
        /// Offset at which the write was attempted.
        offset: u64,
        /// Underlying cause.
        #[source]
        source: io::Error,
    },
    /// The backend is not read-only writable, etc.
    #[error("operation not supported: {0}")]
    Unsupported(&'static str),
    /// Any other backend failure.
    #[error("storage error: {0}")]
    Other(#[from] io::Error),
}

impl StorageError {
    /// Convenience constructor for a failed write.
    pub fn write_failed(offset: u64, source: io::Error) -> Self {
        StorageError::WriteFailed { offset, source }
    }
}
