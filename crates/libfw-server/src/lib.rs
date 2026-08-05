//! Embeddable `libfw` server: axum routing, bearer-token authorization,
//! HTTP range handling and streaming upload/download.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use axum::Router;
//! use libfw_core::auth::{Action, PathValidator, TokenVerifier, Validator};
//! use libfw_core::claims::{Permission, TokenClaims};
//! use libfw_server::{router, ServerState};
//!
//! // 1. Token verifier: parse & verify bearer tokens into claims.
//! #[derive(Clone)]
//! struct MyVerifier;
//! impl TokenVerifier for MyVerifier {
//!     fn verify(&self, token: &str) -> Result<TokenClaims, libfw_core::auth::AuthError> {
//!         Ok(TokenClaims {
//!             sub: token.to_string(),
//!             exp: None,
//!             permissions: vec![Permission::Read, Permission::Write],
//!             allowed_paths: vec!["/".to_string()],
//!         })
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let state = Arc::new(ServerState::builder()
//!     .storage(libfw_server::FsStorage::new("/srv/files"))
//!     .verifier(MyVerifier)
//!     .validator(PathValidator::new())
//!     .build());
//!
//! let app: Router = router(state);
//! // ... serve `app` with your preferred hyper/tokio setup
//! # Ok(())
//! # }
//! ```

mod auth;
mod handlers;
mod http;
mod storage;

pub use auth::{AuthRejection, BearerClaims};
pub use http::{
    content_range_none_value, content_range_value, etag_matches_if_none_match, if_range_matches,
    parse_range_header, ParsedRange, RangeParseError,
};
pub use storage::{FsStorage, FsSink};

use std::sync::Arc;

use axum::Router;
use libfw_core::auth::{AuthError, TokenVerifier, Validator};
use libfw_core::compress::CompressionFormat;
use libfw_core::storage::StorageBackend;
use libfw_core::DEFAULT_MAX_UPLOAD_SIZE;
pub use libfw_core::{HEADER_COMPRESS, HEADER_FILE_META, HEADER_OFFSET};

/// Immutable server configuration shared by all handlers.
pub struct ServerState {
    /// The storage backend serving file content.
    pub storage: Arc<dyn StorageBackend>,
    /// Turns bearer tokens into claims.
    pub verifier: Arc<dyn TokenVerifier>,
    /// Decides whether claims may access a path.
    pub validator: Arc<dyn Validator>,
    /// Compression applied to downloads when the client asks for it.
    pub compression: CompressionFormat,
    /// Upper bound for a single upload body.
    pub max_upload_size: u64,
}

impl ServerState {
    /// Start building a server state.
    pub fn builder() -> ServerStateBuilder {
        ServerStateBuilder::default()
    }

    /// Check whether `claims` may perform `action` on `path`.
    pub fn authorize(
        &self,
        claims: &libfw_core::claims::TokenClaims,
        path: &str,
        action: libfw_core::auth::Action,
    ) -> Result<(), AuthError> {
        self.validator.validate(claims, path, action)
    }
}

/// Builder for [`ServerState`].
pub struct ServerStateBuilder {
    storage: Option<Arc<dyn StorageBackend>>,
    verifier: Option<Arc<dyn TokenVerifier>>,
    validator: Option<Arc<dyn Validator>>,
    compression: CompressionFormat,
    max_upload_size: u64,
}

impl Default for ServerStateBuilder {
    fn default() -> Self {
        ServerStateBuilder {
            storage: None,
            verifier: None,
            validator: None,
            compression: CompressionFormat::Zrip,
            max_upload_size: DEFAULT_MAX_UPLOAD_SIZE,
        }
    }
}

impl ServerStateBuilder {
    /// Required: the storage backend.
    pub fn storage(mut self, storage: impl StorageBackend) -> Self {
        self.storage = Some(Arc::new(storage));
        self
    }

    /// Required: the token verifier.
    pub fn verifier(mut self, verifier: impl TokenVerifier) -> Self {
        self.verifier = Some(Arc::new(verifier));
        self
    }

    /// Required: the path/permission validator.
    pub fn validator(mut self, validator: impl Validator) -> Self {
        self.validator = Some(Arc::new(validator));
        self
    }

    /// Compression for downloads (default: `Zrip`).
    pub fn compression(mut self, format: CompressionFormat) -> Self {
        self.compression = format;
        self
    }

    /// Maximum upload size in bytes (default: 100 GiB).
    pub fn max_upload_size(mut self, size: u64) -> Self {
        self.max_upload_size = size;
        self
    }

    /// Build the state, panicking if required fields are missing.
    pub fn build(self) -> ServerState {
        ServerState {
            storage: self.storage.expect("storage is required"),
            verifier: self.verifier.expect("verifier is required"),
            validator: self.validator.expect("validator is required"),
            compression: self.compression,
            max_upload_size: self.max_upload_size,
        }
    }
}

/// Build the axum router with the libfw routes mounted.
///
/// Routes:
/// - `GET  /file/{*path}` — download with Range / ETag / compression
/// - `HEAD /file/{*path}` — metadata only
/// - `POST /file/{*path}` — streaming upload (headers: `x-libfw-file-meta`,
///   optional `x-libfw-offset`, optional `x-libfw-compress`)
/// - `GET  /dir/{*path}`  — directory listing (JSON)
pub fn router(state: Arc<ServerState>) -> Router {
    use axum::routing::{get, post};

    Router::new()
        .route("/file/{*path}", get(handlers::download).head(handlers::head_file))
        .route("/file/{*path}", post(handlers::upload))
        .route("/dir/{*path}", get(handlers::list_dir))
        .with_state(state)
}

/// Normalize and validate a virtual path from the URL.
///
/// Rejects absolute paths, `..` segments, NUL bytes and empty segments.
pub fn validate_rel_path(path: &str) -> Result<String, &'static str> {
    if path.contains('\0') {
        return Err("path contains NUL byte");
    }
    if path.starts_with('/') {
        return Err("path must be relative");
    }
    let mut out = String::with_capacity(path.len());
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => return Err("path escapes the mount root"),
            seg => {
                if !out.is_empty() {
                    out.push('/');
                }
                out.push_str(seg);
            }
        }
    }
    Ok(out)
}
