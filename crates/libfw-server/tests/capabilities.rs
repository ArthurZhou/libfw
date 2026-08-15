//! Integration tests for `GET /capabilities` (public advertisement) and
//! the zrip download level negotiation (clamp + echo).

use std::sync::Arc;

use axum::body::Body;
use axum::http::header::{self, HeaderMap, HeaderValue};
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::capabilities::{IntRange, Limits, ZripLevels};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_core::compress::{CompressionFormat, decompressor};
use libfw_core::metadata::encode_file_meta_header;
use libfw_server::{
    router, FsStorage, ServerState, HEADER_COMPRESS, HEADER_COMPRESS_LEVEL, HEADER_FILE_META,
};
use serde_json::Value;
use tower::ServiceExt;

#[derive(Clone)]
struct DevVerifier;
impl TokenVerifier for DevVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        Ok(TokenClaims {
            sub: token.to_string(),
            exp: None,
            permissions: vec![Permission::Read, Permission::Write],
            allowed_paths: vec!["/".to_string()],
        })
    }
}

fn auth_header(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    headers
}

fn request(method: &str, uri: &str, headers: HeaderMap, body: Body) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers.iter() {
        builder = builder.header(name, value);
    }
    builder.body(body).unwrap()
}

async fn body_string(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn app_with(
    compression: CompressionFormat,
    limits: Option<Limits>,
    zrip_levels: Option<ZripLevels>,
) -> axum::Router {
    let mut b = ServerState::builder()
        .storage(FsStorage::new(tempfile::tempdir().unwrap().path()))
        .verifier(DevVerifier)
        .validator(PathValidator::new())
        .compression(compression);
    if let Some(l) = limits {
        b = b.limits(l);
    }
    if let Some(z) = zrip_levels {
        b = b.zrip_levels(z);
    }
    router(Arc::new(b.build()))
}

// ---------------------------------------------------------------------------
// /capabilities
// ---------------------------------------------------------------------------

#[tokio::test]
async fn capabilities_is_public_and_serves_defaults() {
    let app = app_with(CompressionFormat::Zrip, None, None);

    // No auth at all — must still be served (the whole point of it being
    // public: adaptive clients query it before authenticating).
    let resp = app
        .clone()
        .oneshot(request("GET", "/capabilities", HeaderMap::new(), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_str(&body_string(resp).await).unwrap();

    assert_eq!(json["protocol"], "libfw/1");
    assert_eq!(json["compression"]["formats"], serde_json::json!(["identity", "zrip"]));
    assert_eq!(json["compression"]["zripLevels"], serde_json::json!({"min": -8, "max": 4, "default": 1}));
    assert_eq!(json["limits"]["concurrency"], serde_json::json!({"min": 1, "max": 16, "default": 4}));
    assert_eq!(json["limits"]["uploadWindow"]["default"], 8);
    assert_eq!(json["limits"]["downloadWindow"]["default"], 4);
    assert_eq!(json["limits"]["chunkSize"]["default"], 2 * 1024 * 1024);
    assert_eq!(json["limits"]["downloadChunkSize"]["default"], 256 * 1024);
    assert_eq!(json["limits"]["maxUploadSize"], 100 * 1024 * 1024 * 1024u64);
    assert_eq!(json["limits"]["maxRetries"]["default"], 3);
    assert_eq!(json["limits"]["timeoutMs"]["default"], 600_000);

    // Authenticated request gets the identical payload.
    let resp2 = app
        .oneshot(request("GET", "/capabilities", auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let json2: Value = serde_json::from_str(&body_string(resp2).await).unwrap();
    assert_eq!(json2, json);
}

#[tokio::test]
async fn capabilities_reflects_builder_overrides() {
    let limits = Limits {
        concurrency: IntRange { min: 1, max: 4, default: 2 },
        upload_window: IntRange { min: 1, max: 2, default: 1 },
        ..Limits::default()
    };
    let levels = ZripLevels { min: -4, max: 0, default: -1 };
    let app = app_with(CompressionFormat::Zrip, Some(limits), Some(levels));

    let resp = app
        .oneshot(request("GET", "/capabilities", HeaderMap::new(), Body::empty()))
        .await
        .unwrap();
    let json: Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(json["limits"]["concurrency"], serde_json::json!({"min": 1, "max": 4, "default": 2}));
    assert_eq!(json["limits"]["uploadWindow"]["max"], 2);
    assert_eq!(json["compression"]["zripLevels"], serde_json::json!({"min": -4, "max": 0, "default": -1}));
}

#[tokio::test]
async fn capabilities_formats_follow_configured_compression() {
    // Server without zrip compression advertises identity only — clients
    // must not ask for zrip bodies it won't produce.
    let app = app_with(CompressionFormat::None, None, None);
    let resp = app
        .oneshot(request("GET", "/capabilities", HeaderMap::new(), Body::empty()))
        .await
        .unwrap();
    let json: Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(json["compression"]["formats"], serde_json::json!(["identity"]));
}

// ---------------------------------------------------------------------------
// Download level negotiation
// ---------------------------------------------------------------------------

/// Upload `data` at `path` (raw body, no upload compression), then request
/// a zrip download with an optional level header; returns (status, headers,
/// raw body bytes).
async fn upload_then_download(
    app: axum::Router,
    path: &str,
    data: &[u8],
    level_header: Option<i32>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let meta = libfw_core::metadata::FileMeta::new(path, data.len() as u64, 1_700_000_000);
    let mut headers = auth_header("tok");
    headers.insert(
        HEADER_FILE_META,
        HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap(),
    );
    let resp = app
        .clone()
        .oneshot(request("POST", &format!("/file/{path}"), headers, Body::from(data.to_vec())))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "upload failed: {}", body_string(resp).await);

    let mut dl_headers = auth_header("tok");
    dl_headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_static("zrip"));
    if let Some(level) = level_header {
        dl_headers.insert(HEADER_COMPRESS_LEVEL, HeaderValue::from(level));
    }
    let resp = app
        .oneshot(request("GET", &format!("/file/{path}"), dl_headers, Body::empty()))
        .await
        .unwrap();
    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, resp_headers, bytes)
}

fn decompress_zrip(body: &[u8]) -> Vec<u8> {
    let mut d = decompressor(CompressionFormat::Zrip);
    let mut out = Vec::new();
    d.decompress(body, &mut out).unwrap();
    d.finish(&mut out).unwrap();
    out
}

#[tokio::test]
async fn download_level_is_clamped_and_echoed() {
    let app = app_with(
        CompressionFormat::Zrip,
        None,
        Some(ZripLevels { min: -8, max: 4, default: 1 }),
    );
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();

    // Request above the advertised max → clamped to 4, echoed back.
    let (status, headers, body) = upload_then_download(app.clone(), "clamp-high.bin", &data, Some(99)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(HEADER_COMPRESS_LEVEL).unwrap(), "4");
    assert_eq!(decompress_zrip(&body), data);

    // Request below the advertised min → clamped to -8.
    let (_, headers, body) = upload_then_download(app.clone(), "clamp-low.bin", &data, Some(-99)).await;
    assert_eq!(headers.get(HEADER_COMPRESS_LEVEL).unwrap(), "-8");
    assert_eq!(decompress_zrip(&body), data);

    // In-range passes through.
    let (_, headers, body) = upload_then_download(app.clone(), "exact.bin", &data, Some(2)).await;
    assert_eq!(headers.get(HEADER_COMPRESS_LEVEL).unwrap(), "2");
    assert_eq!(decompress_zrip(&body), data);

    // No header → default, still echoed so the client learns it.
    let (_, headers, body) = upload_then_download(app, "default.bin", &data, None).await;
    assert_eq!(headers.get(HEADER_COMPRESS_LEVEL).unwrap(), "1");
    assert_eq!(decompress_zrip(&body), data);
}

#[tokio::test]
async fn download_level_legacy_mode_is_unchanged() {
    // Builder without zrip_levels: the header is ignored, nothing is
    // echoed, and the body is zrip at the library default — exactly the
    // pre-0.3.3 behavior, so old servers paired with new clients stay safe.
    let app = app_with(CompressionFormat::Zrip, None, None);
    let data: Vec<u8> = (0..50_000u32).map(|i| (i % 31) as u8).collect();
    let (status, headers, body) = upload_then_download(app, "legacy.bin", &data, Some(4)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get(HEADER_COMPRESS_LEVEL).is_none(), "legacy mode must not echo a level");
    assert_eq!(headers.get(HEADER_COMPRESS).unwrap(), "zrip");
    assert_eq!(decompress_zrip(&body), data);
}

#[tokio::test]
async fn download_identity_never_echoes_level() {
    // No Accept-Encoding: zrip → identity body; the level header is
    // irrelevant and must not be echoed even with zrip_levels configured.
    let app = app_with(
        CompressionFormat::Zrip,
        None,
        Some(ZripLevels { min: -8, max: 4, default: 1 }),
    );
    let data = b"plain identity body".repeat(10);
    let meta = libfw_core::metadata::FileMeta::new("id.bin", data.len() as u64, 1_700_000_000);
    let mut headers = auth_header("tok");
    headers.insert(
        HEADER_FILE_META,
        HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap(),
    );
    let resp = app
        .clone()
        .oneshot(request("POST", "/file/id.bin", headers, Body::from(data.to_vec())))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let mut dl = auth_header("tok");
    dl.insert(HEADER_COMPRESS_LEVEL, HeaderValue::from_static("3"));
    let resp = app
        .oneshot(request("GET", "/file/id.bin", dl, Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get(HEADER_COMPRESS).unwrap(), "identity");
    assert!(resp.headers().get(HEADER_COMPRESS_LEVEL).is_none());
    let bytes = resp.into_body().collect().await.unwrap().to_bytes().to_vec();
    assert_eq!(bytes, data);
}