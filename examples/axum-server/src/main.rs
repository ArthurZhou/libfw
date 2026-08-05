//! libfw integration example — axum file server.
//!
//! Run: `cargo run -p axum-server -- <storage-dir> [port]`
//!
//! Routes (all require `Authorization: Bearer <token>`):
//!   GET  /file/{*path}   download with Range/ETag/compression
//!   HEAD /file/{*path}   metadata only
//!   POST /file/{*path}   streaming upload
//!   GET  /dir/{*path}    directory listing (JSON)
//!
//! The bundled `TokenVerifier` accepts the literal token "dev-token".

use std::sync::Arc;

use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_server::{router, FsStorage, ServerState};
use tower_http::cors::CorsLayer;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_server=info,tower_http=info".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    let root = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "./data".to_string());
    let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8080);

    std::fs::create_dir_all(&root)?;

    let state = Arc::new(
        ServerState::builder()
            .storage(FsStorage::new(&root))
            .verifier(DevTokenVerifier)
            .validator(PathValidator::new())
            .build(),
    );

    // Permissive CORS for the dev demo: the HTML page is served on a
    // different port (e.g. :5173) than this API, so cross-origin requests
    // must be allowed. Production deployments should restrict this.
    let app = router(state)
        .layer(CorsLayer::permissive())
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    tracing::info!("libfw axum server listening on {addr}, storage root: {root}");
    tracing::info!("dev token: `dev-token`");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
