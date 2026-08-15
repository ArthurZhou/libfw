//! Regression probes for the upload "commit yields 0 bytes" bug and the
//! 2 MiB frame-boundary compression path.

use std::sync::Arc;

use axum::body::Body;
use axum::http::header::{self, HeaderMap, HeaderValue};
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_core::compress::{compressor, decompressor_with_limit, CompressionFormat, MAX_FRAME_OUTPUT};
use libfw_core::metadata::encode_file_meta_header;
use libfw_server::{
    router, FsStorage, ServerState, HEADER_COMPRESS, HEADER_FILE_META, HEADER_FINAL,
    HEADER_OFFSET, HEADER_SESSION, HEADER_SESSION_STATUS,
};
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

fn app() -> axum::Router {
    let state = Arc::new(
        ServerState::builder()
            .storage(FsStorage::new(tempfile::tempdir().unwrap().path()))
            .verifier(DevVerifier)
            .validator(PathValidator::new())
            .build(),
    );
    router(state)
}

fn auth_header() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer tok"));
    headers
}

fn request(method: &str, uri: &str, headers: HeaderMap, body: Body) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    for (n, v) in headers.iter() {
        b = b.header(n, v);
    }
    b.body(body).unwrap()
}

async fn status(resp: axum::response::Response) -> StatusCode {
    let code = resp.status();
    let _ = resp.into_body().collect().await;
    code
}

/// The server's exact decompression path for a single chunk.
#[test]
fn two_mib_frame_decompresses_with_max_frame_output_budget() {
    // A full 2 MiB chunk must decompress under the server's per-frame budget
    // (MAX_FRAME_OUTPUT == 2 MiB). An off-by-one here would reject every full
    // chunk and leave the session temp empty → "commit yields 0 bytes".
    let raw = vec![0xABu8; libfw_core::CHUNK_SIZE as usize];
    let mut enc = compressor(CompressionFormat::Zrip).unwrap();
    let mut payload = Vec::new();
    enc.compress(&raw, &mut payload).unwrap();
    enc.finish(&mut payload).unwrap();

    let mut dec = decompressor_with_limit(CompressionFormat::Zrip, MAX_FRAME_OUTPUT);
    let mut out = Vec::new();
    dec.decompress(&payload, &mut out).unwrap();
    dec.finish(&mut out).unwrap();
    assert_eq!(out.len(), raw.len(), "full 2 MiB chunk must round-trip");
    assert_eq!(out, raw);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_compressed_chunks_then_commit() {
    let app = app();
    let data: Vec<u8> = (0..(libfw_core::CHUNK_SIZE as usize * 3)).map(|i| (i % 251) as u8).collect();
    let meta = libfw_core::metadata::FileMeta::new("c.bin", data.len() as u64, 0);
    let session = "conc-sess";

    // Probe creates the shared temp.
    let mut hdr = auth_header();
    hdr.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    hdr.insert(HEADER_SESSION, HeaderValue::from_str(session).unwrap());
    hdr.insert(HEADER_OFFSET, HeaderValue::from_static("0"));
    hdr.insert(HEADER_SESSION_STATUS, HeaderValue::from_static("1"));
    let probe = app.clone().oneshot(request("POST", "/file/c.bin", hdr, Body::empty())).await.unwrap();
    assert_eq!(probe.status(), StatusCode::OK);

    // Compress each 2 MiB chunk and send concurrently.
    let chunk_size = libfw_core::CHUNK_SIZE as usize;
    let mut futs = Vec::new();
    for start in (0..data.len()).step_by(chunk_size) {
        let end = (start + chunk_size).min(data.len());
        let raw = data[start..end].to_vec();
        let mut enc = compressor(CompressionFormat::Zrip).unwrap();
        let mut payload = Vec::new();
        enc.compress(&raw, &mut payload).unwrap();
        enc.finish(&mut payload).unwrap();

        let mut hdr = auth_header();
        hdr.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
        hdr.insert(HEADER_SESSION, HeaderValue::from_str(session).unwrap());
        hdr.insert(HEADER_OFFSET, HeaderValue::from_str(&start.to_string()).unwrap());
        hdr.insert(HEADER_COMPRESS, HeaderValue::from_static("zrip"));
        let app = app.clone();
        futs.push(async move {
            let resp = app.oneshot(request("POST", "/file/c.bin", hdr, Body::from(payload))).await.unwrap();
            status(resp).await
        });
    }
    let codes = futures::future::join_all(futs).await;
    eprintln!("chunk codes: {codes:?}");

    let mut hdr = auth_header();
    hdr.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    hdr.insert(HEADER_SESSION, HeaderValue::from_str(session).unwrap());
    hdr.insert(HEADER_OFFSET, HeaderValue::from_str(&data.len().to_string()).unwrap());
    hdr.insert(HEADER_FINAL, HeaderValue::from_static("1"));
    let commit = app.clone().oneshot(request("POST", "/file/c.bin", hdr, Body::empty())).await.unwrap();
    assert_eq!(commit.status(), StatusCode::CREATED, "commit: {:?}", commit.into_body().collect().await.unwrap().to_bytes());
}
#[tokio::test(flavor = "multi_thread")]
async fn chunk_larger_than_frame_cap_commits_when_split_into_small_frames() {
    // Regression for the `chunkSize` config: a 4 MiB chunk (larger than the
    // server's MAX_FRAME_OUTPUT == 2 MiB) must still upload. The client splits
    // each chunk into ~64 KiB frames, so no single frame exceeds the cap and
    // the per-call decompression budget (MAX_OUTPUT_PER_CALL) is never hit.
    let app = app();
    let data: Vec<u8> = (0..(libfw_core::CHUNK_SIZE as usize * 2 + 12_345))
        .map(|i| (i % 251) as u8)
        .collect();
    let meta = libfw_core::metadata::FileMeta::new("big.bin", data.len() as u64, 0);
    let session = "big-frame-sess";

    // Compress the whole chunk the way the client does: many ~64 KiB frames.
    let mut enc = compressor(CompressionFormat::Zrip).unwrap();
    let mut payload = Vec::new();
    for w in data.chunks(libfw_core::STREAM_BUF_SIZE) {
        enc.compress(w, &mut payload).unwrap();
    }
    enc.finish(&mut payload).unwrap();
    assert!(!payload.is_empty(), "compressed payload must be non-empty");

    let mut hdr = auth_header();
    hdr.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    hdr.insert(HEADER_SESSION, HeaderValue::from_str(session).unwrap());
    hdr.insert(HEADER_OFFSET, HeaderValue::from_static("0"));
    hdr.insert(HEADER_COMPRESS, HeaderValue::from_static("zrip"));
    let resp = app
        .clone()
        .oneshot(request("POST", "/file/big.bin", hdr, Body::from(payload)))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "chunk larger than MAX_FRAME_OUTPUT must be accepted: {:?}",
        resp.into_body().collect().await.unwrap().to_bytes()
    );

    let mut hdr = auth_header();
    hdr.insert(HEADER_FILE_META, HeaderValue::from_str(&encode_file_meta_header(&meta)).unwrap());
    hdr.insert(HEADER_SESSION, HeaderValue::from_str(session).unwrap());
    hdr.insert(HEADER_OFFSET, HeaderValue::from_str(&data.len().to_string()).unwrap());
    hdr.insert(HEADER_FINAL, HeaderValue::from_static("1"));
    let commit = app
        .clone()
        .oneshot(request("POST", "/file/big.bin", hdr, Body::empty()))
        .await
        .unwrap();
    assert_eq!(commit.status(), StatusCode::CREATED, "commit: {:?}", commit.into_body().collect().await.unwrap().to_bytes());

    // The committed file must round-trip byte-for-byte.
    let resp = app
        .clone()
        .oneshot(request("GET", "/file/big.bin", auth_header(), Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(got.to_vec(), data, "downloaded content must match the source");
}

