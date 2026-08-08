//! End-to-end HTTP integration tests for the axum router: upload/download,
//! Range/ETag resume, conditional requests, compression, auth and listing.

use std::sync::Arc;

use axum::body::Body;
use axum::http::header::{self, HeaderMap, HeaderValue};
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_core::compress::{compressor, decompressor, CompressionFormat};
use libfw_core::metadata::encode_file_meta_header;
use libfw_server::{router, FsStorage, ServerState, HEADER_COMPRESS, HEADER_FILE_META, HEADER_OFFSET};
use tower::ServiceExt;

/// A verifier that maps the token to a subject with full permissions.
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

/// A verifier that only allows reads on `/public/`.
#[derive(Clone)]
struct RestrictedVerifier;
impl TokenVerifier for RestrictedVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        Ok(TokenClaims {
            sub: token.to_string(),
            exp: None,
            permissions: vec![Permission::Read],
            allowed_paths: vec!["/public/".to_string()],
        })
    }
}

fn app(verifier: impl TokenVerifier) -> axum::Router {
    let state = Arc::new(
        ServerState::builder()
            .storage(FsStorage::new(tempfile::tempdir().unwrap().path()))
            .verifier(verifier)
            .validator(PathValidator::new())
            .build(),
    );
    router(state)
}

fn auth_header(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    headers
}

/// Build a `Request<Body>` from a `HeaderMap`.
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

#[tokio::test]
async fn upload_then_download_roundtrip() {
    let app = app(DevVerifier);
    let data = b"hello libfw streaming transfer".repeat(100);

    // Upload (Create mode: no x-libfw-offset).
    let meta = libfw_core::metadata::FileMeta::new("a/b.txt", data.len() as u64, 1_700_000_000);
    let mut headers = auth_header("tok");
    headers.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    let resp = app
        .clone()
        .oneshot(request("POST", "/file/a/b.txt", headers, Body::from(data.to_vec())))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "body: {}", body_string(resp).await);

    // Download it back.
    let resp = app
        .oneshot(request("GET", "/file/a/b.txt", auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = body_string(resp).await.into_bytes();
    assert_eq!(got, data);
}

#[tokio::test]
async fn range_requests_return_206() {
    let app = app(DevVerifier);
    let data = b"0123456789".to_vec();
    let meta = libfw_core::metadata::FileMeta::new("r.bin", 10, 0);
    let mut headers = auth_header("tok");
    headers.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    app.clone()
        .oneshot(request("POST", "/file/r.bin", headers, Body::from(data.clone())))
        .await
        .unwrap();

    let mut headers = auth_header("tok");
    headers.insert(header::RANGE, HeaderValue::from_static("bytes=2-5"));
    let resp = app
        .oneshot(request("GET", "/file/r.bin", headers, Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(resp.headers().get(header::CONTENT_RANGE).unwrap(), "bytes 2-5/10");
    assert_eq!(resp.headers().get(header::ACCEPT_RANGES).unwrap(), "bytes");
    let got = body_string(resp).await.into_bytes();
    assert_eq!(got, b"2345");
}

#[tokio::test]
async fn if_none_match_returns_304() {
    let app = app(DevVerifier);
    let meta = libfw_core::metadata::FileMeta::new("e.txt", 3, 0);
    let mut headers = auth_header("tok");
    headers.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    let upload = app
        .clone()
        .oneshot(request("POST", "/file/e.txt", headers, Body::from(b"abc".to_vec())))
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::CREATED);

    // The server computes its own ETag from the real file (size + mtime);
    // use that for the conditional request.
    let upload_json: serde_json::Value =
        serde_json::from_str(&body_string(upload).await).expect("upload response is JSON");
    let etag = upload_json["file"]["etag"].as_str().expect("etag present").to_string();

    let mut headers = auth_header("tok");
    headers.insert(header::IF_NONE_MATCH, HeaderValue::from_str(&etag).unwrap());
    let resp = app
        .oneshot(request("GET", "/file/e.txt", headers, Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn missing_token_is_401() {
    let app = app(DevVerifier);
    let resp = app
        .oneshot(request("GET", "/file/x.txt", HeaderMap::new(), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn restricted_path_is_403() {
    let app = app(RestrictedVerifier);
    // Read allowed under /public/, denied elsewhere.
    let resp = app
        .clone()
        .oneshot(request("GET", "/file/private/secret.txt", auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Write always denied (Read-only token).
    let resp = app
        .oneshot(request("POST", "/file/public/x.txt", auth_header("tok"), Body::from(b"x".to_vec())))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn upload_then_list_dir() {
    let app = app(DevVerifier);
    let meta = libfw_core::metadata::FileMeta::new("sub/f.txt", 2, 0);
    let mut headers = auth_header("tok");
    headers.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    app.clone()
        .oneshot(request("POST", "/file/sub/f.txt", headers, Body::from(b"hi".to_vec())))
        .await
        .unwrap();

    let resp = app
        .oneshot(request("GET", "/dir/sub", auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("f.txt"), "listing: {body}");
}

#[tokio::test]
async fn compressed_upload_and_download_roundtrip() {
    let app = app(DevVerifier);
    let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();

    // Upload compressed: one zstd frame for the whole body.
    let mut enc = compressor(CompressionFormat::Zrip).unwrap();
    let mut payload = Vec::new();
    enc.compress(&data, &mut payload).unwrap();
    enc.finish(&mut payload).unwrap();

    let meta = libfw_core::metadata::FileMeta::new("c.bin", data.len() as u64, 0);
    let mut headers = auth_header("tok");
    headers.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    headers.insert(HEADER_COMPRESS, HeaderValue::from_static("zrip"));
    let resp = app
        .clone()
        .oneshot(request("POST", "/file/c.bin", headers, Body::from(payload)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Download with Accept-Encoding: zrip and decompress the response.
    let mut headers = auth_header("tok");
    headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_static("zrip"));
    let resp = app
        .oneshot(request("GET", "/file/c.bin", headers, Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get(HEADER_COMPRESS).unwrap(), "zrip");
    let compressed = resp.into_body().collect().await.unwrap().to_bytes();
    let mut dec = decompressor(CompressionFormat::Zrip);
    let mut plain = Vec::new();
    dec.decompress(&compressed, &mut plain).unwrap();
    dec.finish(&mut plain).unwrap();
    assert_eq!(plain, data);
}

#[tokio::test]
async fn browser_zstd_accept_encoding_does_not_trigger_zrip() {
    // Regression: a browser sends `Accept-Encoding: gzip, deflate, br,
    // zstd` automatically. This must NOT be interpreted as a request for
    // libfw's private zrip wire format (which a plain browser `fetch`
    // cannot decode) — the server must reply identity instead.
    let app = app(DevVerifier);
    let data = b"plain text body".to_vec();
    let meta = libfw_core::metadata::FileMeta::new("z.bin", data.len() as u64, 0);
    let mut headers = auth_header("tok");
    headers.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    app.clone()
        .oneshot(request("POST", "/file/z.bin", headers, Body::from(data.clone())))
        .await
        .unwrap();

    let mut headers = auth_header("tok");
    headers.insert(
        header::ACCEPT_ENCODING,
        HeaderValue::from_static("gzip, deflate, br, zstd"),
    );
    let resp = app
        .oneshot(request("GET", "/file/z.bin", headers, Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // No zrip — a plain consumer can read the body verbatim.
    assert_eq!(resp.headers().get(HEADER_COMPRESS).unwrap(), "identity");
    let got = body_string(resp).await.into_bytes();
    assert_eq!(got, data);
}

#[tokio::test]
async fn resume_upload_at_offset() {
    let app = app(DevVerifier);
    let meta = libfw_core::metadata::FileMeta::new("res.txt", 6, 0);

    // First chunk: offset 0 → overwrite mode.
    let mut headers = auth_header("tok");
    headers.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    headers.insert(HEADER_OFFSET, HeaderValue::from_static("0"));
    app.clone()
        .oneshot(request("POST", "/file/res.txt", headers.clone(), Body::from(b"ABCD".to_vec())))
        .await
        .unwrap();

    // Second chunk: offset 4 → resume mode appends.
    headers.insert(HEADER_OFFSET, HeaderValue::from_static("4"));
    let resp = app
        .clone()
        .oneshot(request("POST", "/file/res.txt", headers, Body::from(b"EF".to_vec())))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = app
        .oneshot(request("GET", "/file/res.txt", auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(body_string(resp).await, "ABCDEF");
}

#[tokio::test]
async fn bad_resume_offset_is_412() {
    let app = app(DevVerifier);
    let meta = libfw_core::metadata::FileMeta::new("bad.txt", 4, 0);
    let mut headers = auth_header("tok");
    headers.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    app.clone()
        .oneshot(request("POST", "/file/bad.txt", headers.clone(), Body::from(b"ABCD".to_vec())))
        .await
        .unwrap();

    // Resume at a wrong offset must fail with 412 so the client resets.
    headers.insert(HEADER_OFFSET, HeaderValue::from_static("9"));
    let resp = app
        .oneshot(request("POST", "/file/bad.txt", headers, Body::from(b"X".to_vec())))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
}

#[tokio::test]
async fn unsatisfiable_range_is_416() {
    let app = app(DevVerifier);
    let meta = libfw_core::metadata::FileMeta::new("s.bin", 5, 0);
    let mut headers = auth_header("tok");
    headers.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    app.clone()
        .oneshot(request("POST", "/file/s.bin", headers, Body::from(b"12345".to_vec())))
        .await
        .unwrap();

    let mut headers = auth_header("tok");
    headers.insert(header::RANGE, HeaderValue::from_static("bytes=100-"));
    let resp = app
        .oneshot(request("GET", "/file/s.bin", headers, Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(resp.headers().get(header::CONTENT_RANGE).unwrap(), "bytes */5");
}

#[tokio::test]
async fn path_traversal_is_rejected() {
    let app = app(DevVerifier);
    let resp = app
        .oneshot(request("GET", "/file/../secret", auth_header("tok"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn duplicate_upload_without_offset_conflicts() {
    let app = app(DevVerifier);
    let meta = libfw_core::metadata::FileMeta::new("dup.txt", 1, 0);
    let mut headers = auth_header("tok");
    headers.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    app.clone()
        .oneshot(request("POST", "/file/dup.txt", headers.clone(), Body::from(b"a".to_vec())))
        .await
        .unwrap();

    // Second Create-mode upload (no offset) → 409 Conflict.
    let resp = app
        .oneshot(request("POST", "/file/dup.txt", headers, Body::from(b"b".to_vec())))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}
