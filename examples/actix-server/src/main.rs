//! libfw integration example — actix-web file server.
//!
//! Demonstrates that libfw's framework-agnostic core contracts (storage
//! backend, token validation, streaming compression) plug into actix-web
//! as easily as into axum.
//!
//! Run: `cargo run -p libfw-actix-server -- <storage-dir> [port]`
//! Then open http://127.0.0.1:8081/ for the transfer-status dashboard.
//!
//! Routes:
//!   GET  /                    demo dashboard (transfer status + config)
//!   GET  /capabilities        capability advertisement (public, JSON)
//!   GET  /file/{path}         download with Range/ETag/compression
//!   HEAD /file/{path}         metadata only
//!   POST /file/{path}         streaming upload
//!   GET  /dir/{path}          directory listing (JSON)
//!
//! The bundled `TokenVerifier` accepts the literal token "dev-token".

use std::io::Read;
use std::sync::Arc;

use actix_cors::Cors;
use actix_web::http::header::{self, HeaderMap};
use actix_web::web::{self, Bytes};
use actix_web::{App, HttpRequest, HttpResponse, HttpServer};
use futures::stream::{BoxStream, Stream, StreamExt};
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_core::compress::{decompressor, CompressionFormat};
use libfw_core::metadata::{decode_file_meta_header, FileMeta};
use libfw_core::storage::WriteMode;
use libfw_core::{RangeSpec, StorageError, STREAM_BUF_SIZE};
use libfw_server::{
    content_range_none_value, content_range_value, etag_matches_if_none_match, if_range_matches,
    parse_range_header, EncryptedPathCodec, FsStorage, ParsedRange, ServerState, HEADER_COMPRESS,
    HEADER_FILE_META, HEADER_FINAL, HEADER_OFFSET, HEADER_SESSION,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// A permissive token verifier for local development / demos.
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

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Extract + verify the bearer token, then authorize `action` on `path`.
fn authorize(
    req: &HttpRequest,
    state: &ServerState,
    path: &str,
    action: libfw_core::auth::Action,
) -> Result<TokenClaims, HttpResponse> {
    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| HttpResponse::Unauthorized().json(serde_json::json!({"error": "missing bearer token"})))?;
    let claims = state
        .verifier
        .verify(token.trim())
        .map_err(|e| HttpResponse::Unauthorized().json(serde_json::json!({"error": e.to_string()})))?;
    state
        .validator
        .validate(&claims, path, action)
        .map_err(|e| match e {
            AuthError::Forbidden { path, action } => HttpResponse::Forbidden().json(
                serde_json::json!({"error": format!("permission denied: {action} on `{path}`")}),
            ),
            other => HttpResponse::Unauthorized().json(serde_json::json!({"error": other.to_string()})),
        })?;
    Ok(claims)
}

/// Path validation shared by all handlers.
fn validate_path(raw: &str) -> Result<String, HttpResponse> {
    if raw.contains('\0') {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({"error": "path contains NUL"})));
    }
    if raw.starts_with('/') {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({"error": "path must be relative"})));
    }
    let mut out = String::with_capacity(raw.len());
    for seg in raw.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                return Err(HttpResponse::BadRequest().json(serde_json::json!({"error": "path escapes root"})))
            }
            s => {
                if !out.is_empty() {
                    out.push('/');
                }
                out.push_str(s);
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

/// Turn a blocking reader into an async byte stream (blocking reads run on
/// `spawn_blocking` so the runtime thread stays free).
fn reader_stream(reader: Box<dyn Read + Send>) -> BoxStream<'static, Result<Bytes, std::io::Error>> {
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    tokio::task::spawn_blocking(move || {
        let mut reader = reader;
        let mut buf = vec![0u8; STREAM_BUF_SIZE];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if tx.blocking_send(Ok(Bytes::copy_from_slice(&buf[..n]))).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                    break;
                }
            }
        }
    });
    ReceiverStream::new(rx).boxed()
}

/// Optionally wrap a byte stream through the zrip compressor.
///
/// The compressor is shared behind an `Arc<Mutex<_>>` because the map
/// closure (sync) and the finish step (async) both need it, and actix
/// requires `Send` response streams.
fn maybe_compress<S>(raw: S, format: CompressionFormat) -> BoxStream<'static, Result<Bytes, std::io::Error>>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    match format {
        CompressionFormat::None => raw.boxed(),
        CompressionFormat::Zrip => {
            let compressor = Arc::new(std::sync::Mutex::new(
                libfw_core::compress::compressor(CompressionFormat::Zrip).expect("zrip"),
            ));
            let map_c = compressor.clone();
            raw.map(move |item| {
                let mut out = Vec::new();
                match item {
                    Ok(chunk) => {
                        let mut enc = map_c.lock().unwrap();
                        enc.compress(&chunk, &mut out)
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                        Ok(Bytes::from(out))
                    }
                    Err(e) => Err(e),
                }
            })
            .chain(futures::stream::once(async move {
                let mut tail = Vec::new();
                let mut enc = compressor.lock().unwrap();
                enc.finish(&mut tail)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Ok(Bytes::from(tail))
            }))
            .boxed()
        }
    }
}

/// `GET /file/{path}` and `HEAD /file/{path}`.
async fn file_download(
    req: HttpRequest,
    state: web::Data<Arc<ServerState>>,
    path: web::Path<String>,
) -> HttpResponse {
    let path = match validate_path(path.as_str()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    if let Err(resp) = authorize(&req, &state, &path, libfw_core::auth::Action::Read) {
        return resp;
    }

    let meta = match state.storage.file_meta(&path).await {
        Ok(Some(m)) => m,
        Ok(None) => return HttpResponse::NotFound().body(path),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    // If-None-Match → 304.
    if let Some(v) = req
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        if etag_matches_if_none_match(v, &meta.etag) {
            return HttpResponse::NotModified().finish();
        }
    }

    // Range negotiation.
    let mut range = None;
    if let Some(raw) = req.headers().get(header::RANGE).and_then(|v| v.to_str().ok()) {
        match parse_range_header(raw) {
            Ok(r) => range = r,
            Err(_) => return HttpResponse::BadRequest().body("malformed range header"),
        }
    }
    if let Some(if_range) = req.headers().get(header::IF_RANGE).and_then(|v| v.to_str().ok()) {
        if !if_range_matches(if_range, &meta.etag) {
            range = None;
        }
    }

    let is_partial = range.is_some();
    let spec = match range {
        Some(ParsedRange::Bytes(r)) => match r.clamp(meta.size) {
            Some(s) => s,
            None => {
                let mut resp = HttpResponse::RangeNotSatisfiable().finish();
                resp.headers_mut().insert(
                    header::CONTENT_RANGE,
                    content_range_none_value(meta.size)
                        .parse()
                        .expect("header"),
                );
                return resp;
            }
        },
        Some(ParsedRange::Suffix(n)) => {
            if n == 0 || meta.size == 0 {
                let mut resp = HttpResponse::RangeNotSatisfiable().finish();
                resp.headers_mut().insert(
                    header::CONTENT_RANGE,
                    content_range_none_value(meta.size)
                        .parse()
                        .expect("header"),
                );
                return resp;
            }
            RangeSpec {
                start: meta.size.saturating_sub(n),
                end: meta.size,
            }
        }
        None => RangeSpec::full(meta.size),
    };

    // Compression: only when the client advertises support for `zrip`.
    let format = if state.compression == CompressionFormat::Zrip
        && req
            .headers()
            .get(header::ACCEPT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(|v| {
                v.split(',')
                    .any(|e| CompressionFormat::parse_header(e.trim()) == Some(CompressionFormat::Zrip))
            })
            .unwrap_or(false)
    {
        CompressionFormat::Zrip
    } else {
        CompressionFormat::None
    };

    let mut builder = if is_partial {
        HttpResponse::PartialContent()
    } else {
        HttpResponse::Ok()
    };
    builder
        .insert_header((header::ACCEPT_RANGES, "bytes"))
        .insert_header((header::CONTENT_TYPE, "application/octet-stream"))
        .insert_header((header::ETAG, meta.etag.clone()))
        .insert_header((HEADER_COMPRESS, format.as_str()));
    if is_partial {
        builder.insert_header((
            header::CONTENT_RANGE,
            content_range_value(&spec, meta.size),
        ));
    }
    if format == CompressionFormat::None {
        builder.insert_header((header::CONTENT_LENGTH, spec.len().to_string()));
    }

    if req.method() == actix_web::http::Method::HEAD {
        return builder.finish();
    }

    let reader = match state.storage.read_stream(&path, spec).await {
        Ok(r) => r,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };
    builder.streaming(maybe_compress(reader_stream(reader), format))
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

/// `POST /file/{path}` — streaming upload with offset resume + compression.
async fn file_upload(
    req: HttpRequest,
    state: web::Data<Arc<ServerState>>,
    path: web::Path<String>,
    body: web::Payload,
) -> HttpResponse {
    let path = match validate_path(path.as_str()) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let claims = match authorize(&req, &state, &path, libfw_core::auth::Action::Write) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    let headers: &HeaderMap = req.headers();
    let meta_header = match headers.get(HEADER_FILE_META).and_then(|v| v.to_str().ok()) {
        Some(h) => h,
        None => {
            return HttpResponse::BadRequest().body(format!("missing `{HEADER_FILE_META}` header"))
        }
    };
    let meta: FileMeta = match decode_file_meta_header(meta_header) {
        Ok(m) => m,
        Err(_) => return HttpResponse::BadRequest().body("invalid file meta"),
    };
    if meta.size > state.max_upload_size {
        return HttpResponse::PayloadTooLarge().body("upload too large");
    }

    let format = headers
        .get(HEADER_COMPRESS)
        .and_then(|v| v.to_str().ok())
        .and_then(CompressionFormat::parse_header)
        .unwrap_or(CompressionFormat::None);

    // `x-libfw-final` marks the request as the file's last chunk; only then
    // can the server verify the committed size matches `meta.size`.
    let final_chunk = headers
        .get(HEADER_FINAL)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == "1" || v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // Concurrent "session" upload path: chunks carry their ABSOLUTE offset
    // and are written into a shared per-session temp file (positional);
    // only the final request commits. Absent → legacy sequential upload.
    if let Some(session) = headers
        .get(HEADER_SESSION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        // Derive owner tag to isolate sessions per authenticated principal.
        let owner: String = {
            use sha2::Digest;
            let digest = sha2::Sha256::digest(claims.sub.as_bytes());
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut s = String::with_capacity(16);
            for &b in digest.iter().take(8) {
                s.push(HEX[(b >> 4) as usize] as char);
                s.push(HEX[(b & 0x0f) as usize] as char);
            }
            s
        };
        return upload_session_actix(
            &state,
            &path,
            &session,
            &owner,
            &meta,
            format,
            final_chunk,
            headers,
            body,
        )
        .await;
    }

    let mode = match headers.get(HEADER_OFFSET).and_then(|v| v.to_str().ok()) {
        None => WriteMode::Create,
        Some(off) if off.trim().parse::<u64>().map(|n| n == 0).unwrap_or(false) => {
            WriteMode::Overwrite
        }
        Some(off) => match off.trim().parse::<u64>() {
            Ok(n) => WriteMode::Resume { offset: n },
            Err(_) => return HttpResponse::BadRequest().body("invalid x-libfw-offset"),
        },
    };

    let mut sink = match state.storage.write_stream(&path, mode).await {
        Ok(s) => s,
        Err(StorageError::AlreadyExists(_)) => return HttpResponse::Conflict().body("exists"),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let mut decomp = decompressor(format);
    let mut out: Vec<u8> = Vec::new();
    let mut written = 0u64;
    let mut stream = body;

    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(_) => {
                let _ = sink.abort().await;
                return HttpResponse::BadRequest().body("body stream error");
            }
        };
        written += chunk.len() as u64;
        if written > state.max_upload_size {
            let _ = sink.abort().await;
            return HttpResponse::PayloadTooLarge().body("upload too large");
        }
        if let Err(e) = decomp.decompress(&chunk, &mut out) {
            let _ = sink.abort().await;
            return HttpResponse::BadRequest().body(format!("compressed stream invalid: {e}"));
        }
        let data = std::mem::take(&mut out);
        if !data.is_empty() && sink.write(&data).await.is_err() {
            let _ = sink.abort().await;
            return HttpResponse::InternalServerError().finish();
        }
    }
    if let Err(e) = decomp.finish(&mut out) {
        let _ = sink.abort().await;
        return HttpResponse::BadRequest().body(format!("compressed stream truncated: {e}"));
    }
    if !out.is_empty() && sink.write(&out).await.is_err() {
        let _ = sink.abort().await;
        return HttpResponse::InternalServerError().finish();
    }

    match sink.commit().await {
        Ok(committed) => HttpResponse::Created().json(serde_json::json!({ "file": committed })),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

/// Handle one request of the concurrent "session" upload protocol (mirrors
/// the axum handler). Data chunks write at their ABSOLUTE offset into a
/// shared per-session temp; the `x-libfw-final` request verifies the size
/// and commits.
async fn upload_session_actix(
    state: &ServerState,
    path: &str,
    session: &str,
    owner: &str,
    meta: &FileMeta,
    format: CompressionFormat,
    final_chunk: bool,
    headers: &HeaderMap,
    mut body: web::Payload,
) -> HttpResponse {
    let offset_hdr = headers.get(HEADER_OFFSET).and_then(|v| v.to_str().ok());
    let base_offset = match offset_hdr {
        None => 0u64,
        Some(off) => match off.trim().parse::<u64>() {
            Ok(n) => n,
            Err(_) => return HttpResponse::BadRequest().body("invalid x-libfw-offset"),
        },
    };
    let create_mode = if offset_hdr.is_none() {
        WriteMode::Create
    } else {
        WriteMode::Overwrite
    };

    let mut sink = match state.storage.write_stream_session(path, session, owner, create_mode).await {
        Ok(s) => s,
        Err(StorageError::AlreadyExists(_)) => return HttpResponse::Conflict().body("exists"),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let mut decomp = decompressor(format);
    let mut out: Vec<u8> = Vec::new();
    let mut written = 0u64;
    while let Some(chunk) = body.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(_) => {
                let _ = sink.abort().await;
                return HttpResponse::BadRequest().body("body stream error");
            }
        };
        if let Err(e) = decomp.decompress(&chunk, &mut out) {
            let _ = sink.abort().await;
            return HttpResponse::BadRequest().body(format!("compressed stream invalid: {e}"));
        }
        let data = std::mem::take(&mut out);
        if !data.is_empty() {
            let abs = base_offset.saturating_add(written);
            let end = abs.saturating_add(data.len() as u64);
            if end > state.max_upload_size {
                let _ = sink.abort().await;
                return HttpResponse::PayloadTooLarge().body("upload too large");
            }
            if end > meta.size {
                let _ = sink.abort().await;
                return HttpResponse::BadRequest().body("uploaded bytes exceed declared size");
            }
            if sink.write_at(abs, &data).await.is_err() {
                let _ = sink.abort().await;
                return HttpResponse::InternalServerError().finish();
            }
            written = written.saturating_add(data.len() as u64);
        }
    }
    if let Err(e) = decomp.finish(&mut out) {
        let _ = sink.abort().await;
        return HttpResponse::BadRequest().body(format!("compressed stream truncated: {e}"));
    }
    if !out.is_empty() {
        let abs = base_offset.saturating_add(written);
        let end = abs.saturating_add(out.len() as u64);
        if end > meta.size {
            let _ = sink.abort().await;
            return HttpResponse::BadRequest().body("uploaded bytes exceed declared size");
        }
        if sink.write_at(abs, &out).await.is_err() {
            let _ = sink.abort().await;
            return HttpResponse::InternalServerError().finish();
        }
    }

    if final_chunk {
        match sink.len().await {
            Ok(len) if len == meta.size => {}
            _ => {
                let _ = sink.abort().await;
                return HttpResponse::BadRequest().body("commit size mismatch");
            }
        }
        match sink.commit().await {
            Ok(committed) => {
                return HttpResponse::Created().json(serde_json::json!({ "file": committed }))
            }
            Err(_) => return HttpResponse::InternalServerError().finish(),
        }
    }
    HttpResponse::Created().json(serde_json::json!({ "file": meta }))
}

// ---------------------------------------------------------------------------
// Directory listing
// ---------------------------------------------------------------------------

/// `GET /dir/{path}` — JSON listing of immediate children.
async fn dir_list(
    req: HttpRequest,
    state: web::Data<Arc<ServerState>>,
    path: web::Path<String>,
) -> HttpResponse {
    dir_list_inner(&req, &state, path.as_str()).await
}

/// `GET /dir` — root listing (actix's `{path:.*}` does not match an empty
/// tail, so the root needs its own route).
async fn dir_list_root(req: HttpRequest, state: web::Data<Arc<ServerState>>) -> HttpResponse {
    dir_list_inner(&req, &state, "").await
}

async fn dir_list_inner(
    req: &HttpRequest,
    state: &web::Data<Arc<ServerState>>,
    path: &str,
) -> HttpResponse {
    let path = match validate_path(path) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    if let Err(resp) = authorize(req, state, &path, libfw_core::auth::Action::Read) {
        return resp;
    }
    match state.storage.list_dir(&path).await {
        Ok(entries) => HttpResponse::Ok().json(entries),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

/// `GET /capabilities` — the server's capability advertisement (public):
/// tuning-parameter ranges, zrip level range, compression formats and the
/// protocol version. The demo dashboard renders these as the transfer
/// configuration panel.
async fn capabilities(state: web::Data<Arc<ServerState>>) -> HttpResponse {
    HttpResponse::Ok().json(state.capabilities())
}

/// `GET /` — the embedded transfer-status dashboard (vanilla JS, no build
/// step): live upload/download progress, completion status and the
/// `/capabilities` configuration table.
async fn dashboard() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../index.html"))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let root = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "./data".to_string());
    let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8081);
    std::fs::create_dir_all(&root)?;

    let state = Arc::new({
        let mut builder = ServerState::builder()
            .storage(FsStorage::new(&root))
            .verifier(DevTokenVerifier)
            .validator(PathValidator::new());
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

    // tus-style expiry: sweep abandoned session-upload temps hourly so a
    // client that vanished mid-upload never leaves its `.libfw-sess-*` temp
    // (or `.blocks` sidecar) behind forever.
    {
        let storage = state.storage.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
            loop {
                tick.tick().await;
                match storage
                    .cleanup_stale_sessions(libfw_core::DEFAULT_SESSION_TTL)
                    .await
                {
                    Ok(n) if n > 0 => println!("cleaned {n} stale upload session temp(s)"),
                    _ => {}
                }
            }
        });
    }

    let addr = format!("127.0.0.1:{port}");
    println!("libfw actix-web server listening on {addr}, storage root: {root}");
    println!("dev token: `dev-token`");

    HttpServer::new(move || {
        App::new()
            // Permissive CORS for the dev demo (the dashboard is same-origin,
            // but a page served elsewhere may also call this API). Restrict
            // in production.
            .wrap(Cors::permissive())
            .app_data(web::Data::new(state.clone()))
            .route("/", web::get().to(dashboard))
            .route("/capabilities", web::get().to(capabilities))
            .route("/file/{path:.*}", web::get().to(file_download))
            .route("/file/{path:.*}", web::head().to(file_download))
            .route("/file/{path:.*}", web::post().to(file_upload))
            .route("/dir", web::get().to(dir_list_root))
            .route("/dir/{path:.*}", web::get().to(dir_list))
    })
    .bind(addr)?
    .run()
    .await
}
