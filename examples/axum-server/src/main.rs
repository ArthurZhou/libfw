//! libfw integration example — axum file server.
//!
//! Run: `cargo run -p axum-server -- <storage-dir> [port] [--static <dir>]`
//!
//! API routes (all require `Authorization: Bearer <token>`, and validate the
//! `x-libfw-protocol` handshake shared with the WASM client):
//!   GET  /file/{*path}   download with Range/ETag/compression
//!   HEAD /file/{*path}   metadata only
//!   POST /file/{*path}   streaming upload
//!   GET  /dir/{*path}    directory listing (JSON)
//!
//! Extra routes:
//!   GET  /health         JSON service info (no auth)
//!   GET  /               same as /health
//!
//! Optional `--static <dir>` serves a static directory as a fallback, so the
//! web demo can be served from the same origin (e.g. `--static .` then open
//! `/examples/web/index.html`). The bundled `TokenVerifier` accepts the
//! literal token "dev-token".

use std::path::PathBuf;
use std::sync::Arc;

use axum::routing::get;
use axum::Json;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_server::{router, FsStorage, ServerState};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

/// A permissive token verifier for local development / demos.
///
/// In production, replace this with a JWT verifier or an external
/// validation service. libfw itself never issues tokens.
#[derive(Clone, Debug)]
struct DevTokenVerifier;

impl TokenVerifier for DevTokenVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        if token == "dev-token" {
            Ok(TokenClaims {
                sub: "dev-user".into(),
                exp: None,
                permissions: vec![Permission::Read, Permission::Write],
                allowed_paths: vec!["/".to_string()],
            })
        } else {
            Err(AuthError::Invalid("unknown token".into()))
        }
    }
}

/// Per-process info exposed by the `/health` endpoint.
#[derive(Clone)]
struct Health {
    storage_root: PathBuf,
    static_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_server=info,tower_http=info".into()),
        )
        .init();

    // Parse: <root> [port] [--static <dir>]  (all optional, in any order).
    let args: Vec<String> = std::env::args().collect();
    let mut root = PathBuf::from("./data");
    let mut port: u16 = 8080;
    let mut static_dir: Option<PathBuf> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--static" => {
                i += 1;
                static_dir = args.get(i).map(PathBuf::from);
            }
            "--port" => {
                i += 1;
                if let Some(p) = args.get(i).and_then(|s| s.parse().ok()) {
                    port = p;
                }
            }
            _ if args[i].parse::<u16>().is_ok() => {
                port = args[i].parse().unwrap_or(port);
            }
            _ if root == PathBuf::from("./data") => {
                root = PathBuf::from(&args[i]);
            }
            _ => {
                tracing::warn!("ignoring unknown argument: {}", args[i]);
            }
        }
        i += 1;
    }

    std::fs::create_dir_all(&root)?;
    let root = root.canonicalize().unwrap_or(root);

    let state = Arc::new(
        ServerState::builder()
            .storage(FsStorage::new(&root))
            .verifier(DevTokenVerifier)
            .validator(PathValidator::new())
            .build(),
    );
    let health = Arc::new(Health {
        storage_root: root.clone(),
        static_dir: static_dir.clone(),
    });

    // `GET /health` — service info (no auth required). Captures `health` by
    // value so it works on a state-less router.
    let health_route = {
        let health = health.clone();
        move || {
            let health = health.clone();
            async move {
                Json(serde_json::json!({
                    "service": "libfw-axum-server",
                    "status": "ok",
                    "version": env!("CARGO_PKG_VERSION"),
                    "protocol": libfw_core::protocol_header_value(),
                    "storage_root": health.storage_root.to_string_lossy(),
                    "static_dir": health.static_dir.as_ref().map(|d| d.to_string_lossy()),
                }))
            }
        }
    };

    // Permissive CORS for the dev demo: the HTML page may be served on a
    // different origin (e.g. :5173) than this API. Production deployments
    // should restrict this.
    let mut app = router(state)
        .route("/health", get(health_route))
        .layer(CorsLayer::permissive())
        .layer(tower_http::trace::TraceLayer::new_for_http());

    // Optionally serve a static directory (e.g. the repo root) as a
    // fallback so the web demo works from the same origin. API routes take
    // precedence over the fallback service.
    if let Some(dir) = &static_dir {
        let canonical = dir.canonicalize().unwrap_or_else(|_| dir.clone());
        tracing::info!("serving static files from {}", canonical.display());
        app = app.fallback_service(ServeDir::new(canonical));
    }

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    tracing::info!("libfw axum server listening on {addr}");
    tracing::info!("  storage root : {}", root.display());
    tracing::info!("  dev token    : `dev-token`");
    tracing::info!("  protocol     : {}", libfw_core::protocol_header_value());
    tracing::info!("  health       : http://{addr}/health");
    tracing::info!("  download     : GET/HEAD http://{addr}/file/{{*path}}");
    tracing::info!("  upload       : POST http://{addr}/file/{{*path}}");
    tracing::info!("  listing      : GET http://{addr}/dir/{{*path}}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
