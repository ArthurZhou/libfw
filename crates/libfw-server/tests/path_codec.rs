//! End-to-end tests for shadow-path translation (`PathCodec`).
//!
//! Covers the design's test plan:
//! - encrypted round-trip: shadow from `encode()` works in upload/download
//! - `list_dir` returns shadows that are directly usable in a follow-up
//!   transfer (the "link-level" contract)
//! - `allowed_paths` still authorizes the **real** path (token semantics
//!   unchanged), so a shadow of an unauthorized subtree stays forbidden
//! - tampered shadow → 400
//! - WebSocket upload accepts shadow paths and lands on the real path
//!
//! These tests only build when libfw-server's `path-encrypt` feature is on
//! (the encrypted codec is feature-gated in libfw-core).

#![cfg(feature = "path-encrypt")]

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_core::metadata::{FileMeta, encode_file_meta_header};
use libfw_core::pathmap::{EncryptedPathCodec, PathCodec};
use libfw_core::ws::{
    FRAME_COMPLETE, FRAME_HELLO, FRAME_HELLO_OK, FRAME_READY, FRAME_START, Hello, StartRequest,
    TransferKind, block_bounds, block_count, block_frame, control_frame, crc32, frame_payload,
    frame_type, parse_control, wave_done_frame,
};
use libfw_server::{FsStorage, ServerState, router, HEADER_FILE_META};
use serde::Deserialize;
use tower::ServiceExt;

const KEY: [u8; 32] = [7u8; 32];

/// State with an [`EncryptedPathCodec`] over a temp-dir backend.
fn state() -> Arc<ServerState> {
    Arc::new(
        ServerState::builder()
            .storage(FsStorage::new(tempfile::tempdir().unwrap().path()))
            .verifier(DevVerifier)
            .validator(PathValidator::new())
            .path_codec(EncryptedPathCodec::new(KEY))
            .build(),
    )
}

/// Full-access verifier (any path).
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

/// Read/write token confined to `/public/` — the same shape as the
/// `RestrictedVerifier` in `http_integration.rs`.
#[derive(Clone)]
struct RestrictedVerifier;
impl TokenVerifier for RestrictedVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        Ok(TokenClaims {
            sub: token.to_string(),
            exp: None,
            permissions: vec![Permission::Read, Permission::Write],
            allowed_paths: vec!["/public/".to_string()],
        })
    }
}

fn restricted_state() -> Arc<ServerState> {
    Arc::new(
        ServerState::builder()
            .storage(FsStorage::new(tempfile::tempdir().unwrap().path()))
            .verifier(RestrictedVerifier)
            .validator(PathValidator::new())
            .path_codec(EncryptedPathCodec::new(KEY))
            .build(),
    )
}

fn auth_header(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    headers
}

async fn body_string(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Build a `Request<Body>` from a `HeaderMap`.
fn request(method: &str, uri: &str, headers: HeaderMap, body: Body) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers.iter() {
        builder = builder.header(name, value);
    }
    builder.body(body).unwrap()
}

// ---------------------------------------------------------------------------
// HTTP: upload/download/list through shadows
// ---------------------------------------------------------------------------

/// Upload a real file through its shadow path; the shadow URL must be
/// usable verbatim (base64url is URI-safe, so no percent-encoding needed).
async fn upload_through_shadow(app: &Router, shadow: &str, data: &[u8]) {
    let meta = FileMeta::new("ignored/echo", data.len() as u64, 1_700_000_000);
    let mut headers = auth_header("tok");
    headers.insert(
        HEADER_FILE_META,
        HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap(),
    );
    let resp = app
        .clone()
        .oneshot(request("POST", &format!("/file/{shadow}"), headers, Body::from(data.to_vec())))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "body: {}",
        body_string(resp).await
    );
}

#[tokio::test]
async fn shadow_lists_and_transfers_roundtrip() {
    let state = state();
    let app = router(state.clone());
    let codec = EncryptedPathCodec::new(KEY);
    let data = b"shadow path round-trip payload".repeat(64);

    // 1. Upload through a shadow.
    let shadow = codec.encode("docs/a.txt");
    assert_ne!(shadow, "docs/a.txt", "encrypted shadow must be opaque");
    upload_through_shadow(&app, &shadow, &data).await;

    // Real path landed on disk, not the shadow.
    let meta = state.storage.file_meta("docs/a.txt").await.unwrap().unwrap();
    assert_eq!(meta.size, data.len() as u64);

    // 2. Download using the same shadow.
    let resp = app
        .clone()
        .oneshot(request("GET", &format!("/file/{shadow}"), auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await.into_bytes(), data);

    // 3. list_dir exposes shadows, and each listed shadow is usable for a
    //    direct download (the link-level contract). The directory itself is
    //    addressed by its own shadow, so nothing real ever crosses the wire.
    let docs_shadow = codec.encode("docs");
    let resp = app
        .clone()
        .oneshot(request("GET", &format!("/dir/{docs_shadow}"), auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    if resp.status() != StatusCode::OK {
        panic!("list failed: {}", resp.status());
    }
    let listing: DirList = serde_json::from_str(&body_string(resp).await).unwrap();
    let listed = listing
        .0
        .iter()
        .find(|e| codec.decode(&e.path).ok().as_deref() == Some("docs/a.txt"))
        .expect("real docs/a.txt should appear in the listing");
    let resp = app
        .clone()
        .oneshot(request("GET", &format!("/file/{}", listed.path), auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await.into_bytes(), data);
}

#[tokio::test]
async fn allowed_paths_apply_to_real_path_not_shadow() {
    let state = restricted_state();
    let app = router(state.clone());
    let codec = EncryptedPathCodec::new(KEY);

    // Inside the token's allowed subtree: works.
    let in_shadow = codec.encode("public/a.txt");
    upload_through_shadow(&app, &in_shadow, b"hello public").await;
    let resp = app
        .clone()
        .oneshot(request("GET", &format!("/file/{in_shadow}"), auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "allowed subtree must pass");

    // Outside the allowed subtree: the shadow decodes fine, but the real
    // path fails authorization → 403, exactly like the pre-translation
    // behavior for a real-path request.
    let out_shadow = codec.encode("etc/passwd");
    let resp = app
        .clone()
        .oneshot(request("GET", &format!("/file/{out_shadow}"), auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "real path outside allowed_paths must be rejected through a shadow"
    );
}

#[tokio::test]
async fn tampered_shadow_is_400() {
    let state = state();
    let app = router(state.clone());
    let codec = EncryptedPathCodec::new(KEY);

    let mut shadow = codec.encode("docs/a.txt").into_bytes();
    // Flip the last base64url char; odds are ~15/16 it stays a valid
    // base64url char, which is exactly the tamper case we want (a
    // structurally-valid but unauthentic shadow).
    let last = *shadow.last().unwrap();
    *shadow.last_mut().unwrap() = if last == b'A' { b'B' } else { b'A' };
    let tampered = String::from_utf8(shadow).unwrap();
    assert_ne!(tampered, codec.encode("docs/a.txt"), "tamper must change it");

    let resp = app
        .clone()
        .oneshot(request("GET", &format!("/file/{tampered}"), auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "GCM authentication failure must surface as 400"
    );
}

// ---------------------------------------------------------------------------
// WebSocket upload through a shadow
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct DirList(Vec<DirEntry>);

#[derive(Deserialize)]
struct DirEntry {
    path: String,
}

type Ws = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

async fn connect_ws(state: Arc<ServerState>) -> Ws {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    let (ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .expect("ws connect");
    ws
}

async fn send(ws: &mut Ws, frame: Vec<u8>) {
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(frame))
        .await
        .unwrap();
}

async fn recv(ws: &mut Ws) -> Vec<u8> {
    loop {
        match ws.next().await {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b))) => return b.to_vec(),
            Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(p))) => {
                ws.send(tokio_tungstenite::tungstenite::Message::Pong(p))
                    .await
                    .unwrap();
            }
            Some(Ok(_)) => continue,
            other => panic!("ws closed unexpectedly: {other:?}"),
        }
    }
}

#[tokio::test]
async fn ws_upload_accepts_shadow_path() {
    let state = state();
    let codec = EncryptedPathCodec::new(KEY);
    let shadow = codec.encode("media/clip.mp4");
    let mut ws = connect_ws(state.clone()).await;

    // Hello handshake.
    send(&mut ws, control_frame(FRAME_HELLO, &Hello::new("tok"))).await;
    let f = recv(&mut ws).await;
    assert_eq!(frame_type(&f), Some(FRAME_HELLO_OK));

    // Start an upload to the shadow path.
    let data: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
    let start = StartRequest {
        kind: TransferKind::Upload,
        path: shadow.clone(),
        size: data.len() as u64,
        mtime: 0,
        etag: String::new(),
        compress: false,
        mode: "overwrite".into(),
        offset: 0,
        block_size: 1024,
        window: 0,
    };
    send(&mut ws, control_frame(FRAME_START, &start)).await;
    let ready = recv(&mut ws).await;
    let _ready: libfw_core::ws::ReadyReply = parse_control(&ready, FRAME_READY).unwrap();

    // Send all blocks (in order is fine here), then wave-done.
    let total = block_count(data.len() as u64, 1024);
    for idx in 0..total {
        let (s, e) = block_bounds(idx, 1024, data.len() as u64);
        let payload = &data[s as usize..e as usize];
        send(
            &mut ws,
            block_frame(idx, crc32(payload), payload.len() as u32, payload),
        )
        .await;
    }
    send(&mut ws, wave_done_frame()).await;
    let complete = recv(&mut ws).await;
    assert_eq!(
        frame_type(&complete),
        Some(FRAME_COMPLETE),
        "payload: {}",
        String::from_utf8_lossy(frame_payload(&complete))
    );

    // The file landed on the REAL path, and only there.
    let meta = state
        .storage
        .file_meta("media/clip.mp4")
        .await
        .unwrap()
        .expect("real path must exist after WS upload through shadow");
    assert_eq!(meta.size, data.len() as u64);
    assert!(
        state.storage.file_meta(&shadow).await.unwrap().is_none(),
        "shadow must not be used as a storage path"
    );
}