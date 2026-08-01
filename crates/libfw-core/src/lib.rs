//! Shared data structures, traits, protocol constants and streaming
//! compression abstractions for the libfw transfer library.
//!
//! `libfw-core` is the foundation crate consumed by both
//! [`libfw-server`](https://docs.rs/libfw-server) (server routing /
//! middleware) and [`libfw-client`](https://docs.rs/libfw-client) (WASM
//! engine + JS SDK). It contains no I/O of its own: it defines the *contracts*.
//!
//! # Highlights
//!
//! - [`TokenClaims`], [`Action`] and the [`Validator`] trait for
//!   fine-grained bearer-token authorization.
//! - [`StorageBackend`] and [`UploadSink`] traits for pluggable storage.
//! - [`Compressor`] / [`Decompressor`] streaming traits backed by
//!   [`zrip`] (zstd) with constant-memory guarantees.
//! - Protocol constants: [`CHUNK_SIZE`], [`HEADER_COMPRESS`],
//!   [`HEADER_FILE_META`] and friends.
//! - Transfer metadata ([`FileMeta`], [`TransferPlan`], [`ChunkMeta`]).
//!
//! # Example
//!
//! ```no_run
//! use libfw_core::auth::{Action, PathValidator, Validator};
//! use libfw_core::claims::{TokenClaims, Permission};
//!
//! let claims = TokenClaims {
//!     sub: "user-42".into(),
//!     exp: None,
//!     permissions: vec![Permission::Read, Permission::Write],
//!     allowed_paths: vec!["/docs/".into()],
//! };
//! let validator = PathValidator::new();
//! assert!(validator.validate(&claims, "/docs/spec.pdf", Action::Read).is_ok());
//! ```

pub mod auth;
pub mod claims;
pub mod compress;
pub mod constants;
pub mod error;
pub mod metadata;
pub mod range;
pub mod storage;

pub use auth::{Action, AuthError, PathValidator, TokenVerifier, Validator};
pub use claims::{Permission, TokenClaims};
pub use compress::{CompressionFormat, Compressor, Decompressor};
pub use constants::*;
pub use error::{CompressError, DecompressError, StorageError};
pub use metadata::{ChunkMeta, FileMeta, TransferPlan};
pub use range::RangeSpec;
pub use storage::{DirEntry, StorageBackend, UploadSink, WriteMode};
