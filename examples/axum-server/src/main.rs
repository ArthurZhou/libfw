//! libfw integration example — axum file server with a built-in web UI.
//!
//! Serves the full libfw HTTP API (via `libfw_server::router`) plus an
//! embedded, dependency-free file-manager frontend at `/` — upload/download
//! files & folders, browse and navigate the storage tree, live progress with
//! pause/resume/cancel — all driven by the libfw browser SDK.
//!
//! Run: `cargo run -p axum-server -- <storage-dir> [port]`
//! Then open http://127.0.0.1:8080/ (token: `dev-token`).
//!
//! The frontend imports the SDK from `/sdk/index.js`, which the server
//! serves straight from the repository's `sdk/` directory, so the WASM
//! engine must be built first (once):
//!
//! ```bash
//! wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
//! ```
//!
//! Without `sdk/pkg` the page still renders and explains what to run; the
//! REST API itself works regardless.
//!
//! API routes (all require `Authorization: Bearer <token>`, and validate the
//! `x-libfw-protocol` handshake shared with the WASM client):
//!   GET  /file/{*path}   download with Range/ETag/compression
//!   HEAD /file/{*path}   metadata only
//!   POST /file/{*path}   streaming upload
//!   GET  /dir/{*path}    directory listing (JSON)
//!   GET  /capabilities   capability advertisement (public)
//!
//! Extra routes:
//!   GET  /           embedded web UI (no auth)
//!   GET  /health     JSON service info (no auth)
//!   GET  /sdk/*      the browser SDK + pkg (no auth, dev convenience)

use std::path::PathBuf;
use std::sync::Arc;

use axum::http::header::CACHE_CONTROL;
use axum::http::HeaderValue;
use axum::response::Html;
use axum::routing::get;
use axum::Json;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_server::{router, EncryptedPathCodec, FsStorage, ServerState};
use tower_http::cors::CorsLayer;
use tower_http::set_header::SetResponseHeaderLayer;
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
}

/// The embedded web UI (see `index.html`).
async fn index() -> Html<&'static str> {
    Html(include_str!("../index.html"))
}

/// Parse a duration from plain seconds (`300`) or a suffixed form
/// (`30s`, `15m`, `2h`, `1d`). Returns `None` on garbage input.
fn parse_duration(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    let (digits, mult) = match s.chars().last()? {
        's' => (&s[..s.len() - 1], 1u64),
        'm' => (&s[..s.len() - 1], 60),
        'h' => (&s[..s.len() - 1], 3600),
        'd' => (&s[..s.len() - 1], 86_400),
        '0'..='9' => (s, 1),
        _ => return None,
    };
    digits
        .parse::<u64>()
        .ok()
        .map(|n| std::time::Duration::from_secs(n.saturating_mul(mult)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_server=info,tower_http=info".into()),
        )
        .init();

    // Parse: <root> [port]  (both optional).
    let args: Vec<String> = std::env::args().collect();
    let mut root = PathBuf::from("./data");
    let mut port: u16 = 8080;
    for arg in args.iter().skip(1) {
        if let Ok(p) = arg.parse::<u16>() {
            port = p;
        } else {
            root = PathBuf::from(arg);
        }
    }

    std::fs::create_dir_all(&root)?;
    let root = root.canonicalize().unwrap_or(root);

    // tus-style expiry tuning (env-overridable).
    //   LIBFW_CLEANUP_INTERVAL — how often the sweeper runs (default 3600s)
    //   LIBFW_SESSION_TTL     — max age of an abandoned `.libfw-sess-*` temp
    //                           before it is removed (default 24h)
    // Both accept plain seconds ("300") or a suffix ("5m", "2h", "1d").
    let cleanup_interval = std::env::var("LIBFW_CLEANUP_INTERVAL")
        .ok()
        .and_then(|v| parse_duration(&v))
        .unwrap_or(std::time::Duration::from_secs(3600));
    let session_ttl = std::env::var("LIBFW_SESSION_TTL")
        .ok()
        .and_then(|v| parse_duration(&v))
        .unwrap_or(libfw_core::DEFAULT_SESSION_TTL);

    let state = Arc::new({
        let mut builder = ServerState::builder()
            .storage(FsStorage::new(&root))
            .verifier(DevTokenVerifier)
            .validator(PathValidator::new())
            .cleanup_interval(cleanup_interval)
            .session_ttl(session_ttl);
        if let Ok(key_hex) = std::env::var("LIBFW_PATH_KEY") {
            match EncryptedPathCodec::from_hex(&key_hex) {
                Ok(codec) => {
                    builder = builder.path_codec(codec);
                    tracing::info!("shadow paths: encrypted (LIBFW_PATH_KEY set)");
                }
                Err(err) => tracing::warn!(
                    "LIBFW_PATH_KEY ignored ({err}); falling back to identity paths"
                ),
            }
        }
        builder.build()
    });
    let health = Arc::new(Health {
        storage_root: root.clone(),
    });

    // tus-style expiry: sweep abandoned session-upload temps so a
    // client that vanished mid-upload never leaves its `.libfw-sess-*` temp
    // (or `.blocks` sidecar) behind forever. Interval and max age were set
    // on the instance above (env-overridable).
    tracing::info!(
        "session-temp cleanup: interval={cleanup_interval:?} ttl={session_ttl:?}"
    );
    state.spawn_stale_session_cleanup();

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
                }))
            }
        }
    };

    // Permissive CORS for the dev demo: the page can point its "server URL"
    // at a different origin (e.g. a remote libfw deployment). Production
    // deployments should restrict this.
    let app = router(state)
        .route("/", get(index))
        .route("/health", get(health_route))
        .nest_service(
            "/sdk",
            ServeDir::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../sdk")),
        )
        .layer(CorsLayer::permissive())
        .layer(SetResponseHeaderLayer::if_not_present(
            CACHE_CONTROL,
            HeaderValue::from_static("no-store, max-age=0"),
        ))
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    tracing::info!("libfw axum server listening on {addr}");
    tracing::info!("  storage root : {}", root.display());
    tracing::info!("  dev token    : `dev-token`");
    tracing::info!("  protocol     : {}", libfw_core::protocol_header_value());
    tracing::info!("  web UI       : http://{addr}/");
    tracing::info!("  health       : http://{addr}/health");
    tracing::info!("  transport    : HTTP (SDK/engine; parallel Range + chunked uploads)");
    tracing::info!("  download     : GET/HEAD http://{addr}/file/{{*path}}");
    tracing::info!("  upload       : POST http://{addr}/file/{{*path}}");
    tracing::info!("  listing      : GET http://{addr}/dir/{{*path}}");
    if !std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../sdk/pkg/libfw_client.js")).exists() {
        tracing::warn!("sdk/pkg missing — build the WASM engine with:");
        tracing::warn!("  wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release");
    }

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::parse_duration;
    use std::time::Duration;

    #[test]
    fn parses_plain_seconds() {
        assert_eq!(parse_duration("300"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("0"), Some(Duration::from_secs(0)));
    }

    #[test]
    fn parses_suffixed_durations() {
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("15m"), Some(Duration::from_secs(900)));
        assert_eq!(parse_duration("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_duration("1d"), Some(Duration::from_secs(86_400)));
        assert_eq!(parse_duration(" 5m "), Some(Duration::from_secs(300)));
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("abc"), None);
        assert_eq!(parse_duration("5x"), None);
        assert_eq!(parse_duration("-1"), None);
        assert_eq!(parse_duration("1.5h"), None);
    }
}
