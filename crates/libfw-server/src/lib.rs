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

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Router;
use libfw_core::auth::{AuthError, TokenVerifier, Validator};
use libfw_core::capabilities::{Capabilities, Limits, ZripLevels};
use libfw_core::claims::TokenClaims;
use libfw_core::compress::CompressionFormat;
use libfw_core::pathmap::{IdentityPathCodec, PathCodec, PathCodecError};
use libfw_core::storage::StorageBackend;
pub use libfw_core::pathmap::MountPathCodec;
#[cfg(feature = "path-encrypt")]
pub use libfw_core::pathmap::EncryptedPathCodec;
use libfw_core::{protocol_compatible, protocol_header_value, DEFAULT_MAX_UPLOAD_SIZE, HEADER_PROTOCOL};
pub use libfw_core::{
    HEADER_COMPRESS, HEADER_COMPRESS_LEVEL, HEADER_FILE_META, HEADER_FINAL, HEADER_OFFSET,
    HEADER_SESSION, HEADER_SESSION_STATUS,
};

/// Error resolving a client-supplied (shadow) path to a real storage path.
///
/// Returned by [`ServerState::resolve_client_path`]; map it to HTTP as:
/// - [`Invalid`](PathResolveError::Invalid) / [`Codec`](PathResolveError::Codec)
///   → `400 Bad Request` (malformed or tampered shadow path),
/// - [`Auth`](PathResolveError::Auth) → `401` / `403` as usual.
#[derive(Debug, thiserror::Error)]
pub enum PathResolveError {
    /// The shadow path failed basic shape validation (absolute, `..`,
    /// NUL byte, empty segments).
    #[error("invalid path: {0}")]
    Invalid(&'static str),
    /// The codec could not decode the shadow (unknown version, bad
    /// base64, tampered ciphertext, unmapped namespace).
    #[error("cannot decode path: {0}")]
    Codec(#[from] PathCodecError),
    /// The decoded real path is not authorized for this token.
    #[error(transparent)]
    Auth(#[from] AuthError),
}

/// Immutable server configuration shared by all handlers.
pub struct ServerState {
    /// The storage backend serving file content.
    pub storage: Arc<dyn StorageBackend>,
    /// Turns bearer tokens into claims.
    pub verifier: Arc<dyn TokenVerifier>,
    /// Decides whether claims may access a path.
    pub validator: Arc<dyn Validator>,
    /// Real storage paths ↔ client-visible shadow paths.
    ///
    /// Defaults to [`IdentityPathCodec`] (no translation).
    pub path_codec: Arc<dyn PathCodec>,
    /// Compression applied to downloads when the client asks for it.
    pub compression: CompressionFormat,
    /// Upper bound for a single upload body.
    pub max_upload_size: u64,
    /// Advertised tuning-parameter ranges; `None` exports the libfw
    /// built-in defaults (and enforces nothing beyond existing hard caps).
    pub limits: Option<Limits>,
    /// Advertised zrip level range; `None` keeps the legacy behavior:
    /// the level header is ignored and downloads always use
    /// [`ZRIP_DEFAULT_LEVEL`](libfw_core::ZRIP_DEFAULT_LEVEL).
    pub zrip_levels: Option<ZripLevels>,
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

impl ServerState {
    /// Resolve a client-supplied (shadow) path to the canonical real path.
    ///
    /// Pipeline: shape-validate the shadow → [`PathCodec::decode`] →
    /// shape-validate the decoded real path (defense in depth) →
    /// authorize `action` **against the real path**, so
    /// `allowed_paths` keeps its exact pre-translation semantics.
    ///
    /// The returned real path is safe to hand to the storage backend;
    /// it must never be echoed back to the client — responses go through
    /// [`ServerState::expose_path`].
    pub fn resolve_client_path(
        &self,
        claims: &TokenClaims,
        shadow: &str,
        action: libfw_core::auth::Action,
    ) -> Result<String, PathResolveError> {
        let shadow = validate_rel_path(shadow).map_err(PathResolveError::Invalid)?;
        // The canonical root (``) is not a shadow: it maps to itself
        // (the root handler `GET /dir` passes it directly). Everything
        // else must round-trip through the codec.
        let real = if shadow.is_empty() {
            shadow
        } else {
            self.path_codec.decode(&shadow)?
        };
        let real = validate_rel_path(&real).map_err(PathResolveError::Invalid)?;
        self.authorize(claims, &real, action)?;
        Ok(real)
    }

    /// Translate a real storage path for client consumption (shadow).
    pub fn expose_path(&self, real: &str) -> String {
        self.path_codec.encode(real)
    }
}

/// Builder for [`ServerState`].
pub struct ServerStateBuilder {
    storage: Option<Arc<dyn StorageBackend>>,
    verifier: Option<Arc<dyn TokenVerifier>>,
    validator: Option<Arc<dyn Validator>>,
    path_codec: Option<Arc<dyn PathCodec>>,
    compression: CompressionFormat,
    max_upload_size: u64,
    limits: Option<Limits>,
    zrip_levels: Option<ZripLevels>,
}

impl Default for ServerStateBuilder {
    fn default() -> Self {
        ServerStateBuilder {
            storage: None,
            verifier: None,
            validator: None,
            path_codec: None,
            compression: CompressionFormat::Zrip,
            max_upload_size: DEFAULT_MAX_UPLOAD_SIZE,
            limits: None,
            zrip_levels: None,
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

    /// Path translator between real storage paths and client-visible
    /// shadow paths (default: identity — no translation).
    ///
    /// Handlers resolve inbound shadow paths through
    /// [`ServerState::resolve_client_path`] (which also authorizes the
    /// real path) and encode outbound paths via
    /// [`ServerState::expose_path`]. See [`MountPathCodec`] for readable
    /// aliases and [`EncryptedPathCodec`] for opaque encrypted paths
    /// (`libfw-core/path-encrypt` feature).
    pub fn path_codec(mut self, codec: impl PathCodec) -> Self {
        self.path_codec = Some(Arc::new(codec));
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

    /// Tuning-parameter ranges advertised at `GET /capabilities`.
    ///
    /// `None` (default) exports the libfw built-in defaults. The server
    /// never hard-enforces these beyond existing caps (frame size, upload
    /// body limit); they are an advisory contract for adaptive clients.
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = Some(limits);
        self
    }

    /// Zrip level range for downloads: validates/clamps the
    /// `x-libfw-compress-level` request header and echoes the actual level.
    ///
    /// `None` (default) keeps the legacy behavior — the header is ignored
    /// and every zrip download uses [`ZRIP_DEFAULT_LEVEL`].
    pub fn zrip_levels(mut self, levels: ZripLevels) -> Self {
        self.zrip_levels = Some(levels);
        self
    }

    /// Build the state, panicking if required fields are missing.
    pub fn build(self) -> ServerState {
        ServerState {
            storage: self.storage.expect("storage is required"),
            verifier: self.verifier.expect("verifier is required"),
            validator: self.validator.expect("validator is required"),
            path_codec: self.path_codec.unwrap_or_else(|| Arc::new(IdentityPathCodec)),
            compression: self.compression,
            max_upload_size: self.max_upload_size,
            limits: self.limits,
            zrip_levels: self.zrip_levels,
        }
    }
}

impl ServerState {
    /// The capability advertisement served at `GET /capabilities`.
    ///
    /// Filters the compression formats by the configured download
    /// compression and overlays the optional builder overrides; unset
    /// dimensions fall back to the libfw built-in defaults.
    pub fn capabilities(&self) -> Capabilities {
        let mut caps = Capabilities::default();
        if let Some(limits) = &self.limits {
            caps.limits = limits.clone();
        }
        if let Some(levels) = &self.zrip_levels {
            caps.compression.zrip_levels = *levels;
        }
        caps.compression.formats = match self.compression {
            CompressionFormat::Zrip => {
                vec![CompressionFormat::None, CompressionFormat::Zrip]
            }
            CompressionFormat::None => vec![CompressionFormat::None],
        };
        caps
    }
}

/// Reject requests that explicitly advertise an incompatible protocol
/// version with `426 Upgrade Required`.
///
/// Requests *without* the handshake header are allowed, so raw HTTP clients
/// (curl, tests, older builds) keep working; the WASM/SDK client always
/// sends the header so it is guaranteed to be matched with this server.
async fn validate_protocol(req: Request, next: Next) -> Response {
    if let Some(value) = req
        .headers()
        .get(HEADER_PROTOCOL)
        .and_then(|v| v.to_str().ok())
    {
        if !protocol_compatible(value) {
            return (
                StatusCode::UPGRADE_REQUIRED,
                format!(
                    "unsupported protocol `{value}`; expected `{}`",
                    protocol_header_value()
                ),
            )
                .into_response();
        }
    }
    next.run(req).await
}

/// Build the axum router with the libfw routes mounted.
///
/// Routes:
/// - `GET  /file/{*path}` — download with Range / ETag / compression
/// - `HEAD /file/{*path}` — metadata only
/// - `POST /file/{*path}` — streaming upload (headers: `x-libfw-file-meta`,
///   optional `x-libfw-offset`, optional `x-libfw-compress`)
/// - `GET  /dir/{*path}`  — directory listing (JSON)
/// - `GET  /capabilities` — capability advertisement (JSON, **public**:
///   no auth — the payload is a non-sensitive contract for adaptive clients)
///
/// All routes first pass through [`validate_protocol`], which enforces the
/// `x-libfw-protocol` handshake shared with the WASM client.
pub fn router(state: Arc<ServerState>) -> Router {
    use axum::routing::{get, post};

    Router::new()
        .route("/file/{*path}", get(handlers::download).head(handlers::head_file))
        .route("/file/{*path}", post(handlers::upload))
        .route("/dir", get(handlers::list_dir_root))
        .route("/dir/{*path}", get(handlers::list_dir))
        .route("/capabilities", get(handlers::capabilities))
        .layer(axum::middleware::from_fn(validate_protocol))
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
