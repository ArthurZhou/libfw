//! HTTP handlers: download (Range/ETag/compression), upload, listing.

use std::io::{self, Read};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures::stream::{BoxStream, Stream, StreamExt};
use libfw_core::auth::{Action, AuthError};
use libfw_core::capabilities::Capabilities;
use libfw_core::claims::TokenClaims;
use libfw_core::compress::{
    CompressionFormat, Compressor, MAX_OUTPUT_PER_CALL, ZRIP_DEFAULT_LEVEL,
    compressor_with_level, decompressor_with_limit, negotiate_level,
};
use libfw_core::metadata::{FileMeta, decode_file_meta_header};
use libfw_core::storage::{UploadSink, WriteMode};
use libfw_core::{RangeSpec, STREAM_BUF_SIZE, StorageError};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::auth::{AuthRejection, BearerClaims};
use crate::http::{
    ParsedRange, content_range_none_value, content_range_value, etag_matches_if_none_match,
    if_range_matches, parse_range_header,
};
use crate::{
    HEADER_COMPRESS, HEADER_COMPRESS_LEVEL, HEADER_FILE_META, HEADER_FINAL, HEADER_OFFSET,
    HEADER_SESSION, HEADER_SESSION_STATUS, ServerState, validate_rel_path,
};

/// Errors surfaced by handlers, mapped to HTTP responses.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ApiError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    #[allow(dead_code)] // part of the error API surface, mapped to 409
    Conflict(String),
    #[error("upload exceeds limit of {0} bytes")]
    PayloadTooLarge(u64),
    #[error("malformed range header")]
    RangeMalformed,
    #[error("range not satisfiable")]
    RangeUnsatisfiable(u64),
    #[error("not modified")]
    NotModified,
    #[error("authentication failed")]
    Auth(#[from] AuthRejection),
    #[error("storage error")]
    Storage(#[from] StorageError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            // Delegate to AuthRejection's own 401/403 conversion.
            ApiError::Auth(rej) => return rej.into_response(),
            // 416 must carry a `Content-Range: bytes */<total>` header and no body.
            ApiError::RangeUnsatisfiable(total) => {
                let mut r = Response::new(Body::empty());
                *r.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
                r.headers_mut().insert(
                    header::CONTENT_RANGE,
                    content_range_none_value(total).parse().unwrap(),
                );
                return r;
            }
            // 304 must not carry a body.
            ApiError::NotModified => {
                let mut r = Response::new(Body::empty());
                *r.status_mut() = StatusCode::NOT_MODIFIED;
                return r;
            }
            ApiError::Storage(StorageError::NotFound(p)) => {
                return (StatusCode::NOT_FOUND, p).into_response();
            }
            ApiError::Storage(StorageError::AlreadyExists(p)) => {
                return (StatusCode::CONFLICT, p).into_response();
            }
            ApiError::Storage(StorageError::TooLarge(_)) => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "upload exceeds configured limit".to_string(),
                )
                    .into_response();
            }
            // A resume offset that no longer matches tells the client to
            // reset its local progress (RFC 7232 / libfw protocol).
            ApiError::Storage(StorageError::WriteFailed { .. }) => {
                return (
                    StatusCode::PRECONDITION_FAILED,
                    "resume offset mismatch; reset client state".to_string(),
                )
                    .into_response();
            }
            ApiError::Storage(StorageError::Unsupported(msg)) => {
                return (StatusCode::BAD_REQUEST, msg.to_string()).into_response();
            }
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, msg),
            ApiError::PayloadTooLarge(limit) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("upload exceeds limit of {limit} bytes"),
            ),
            ApiError::RangeMalformed => (
                StatusCode::BAD_REQUEST,
                "malformed range header".to_string(),
            ),
            ApiError::Storage(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            ApiError::Io(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        }
        .into_response()
    }
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

/// Everything needed to build the download response.
struct DownloadPlan {
    status: StatusCode,
    headers: Vec<(&'static str, String)>,
    format: CompressionFormat,
    /// Zrip level the body is (or would be) compressed with.
    level: i32,
    reader: Option<Box<dyn Read + Send>>,
}

// ---------------------------------------------------------------------------
// Capabilities (public, no auth)
// ---------------------------------------------------------------------------

/// `GET /capabilities` — the server's capability advertisement.
///
/// Deliberately **public**: the payload contains no secrets and adaptive
/// clients need it before any authentication. Requests without credentials
/// (or without the protocol handshake) are served identically.
pub(crate) async fn capabilities(
    State(state): State<Arc<ServerState>>,
) -> Json<Capabilities> {
    Json(state.capabilities())
}

fn authorize_request(
    state: &ServerState,
    claims: &TokenClaims,
    path: &str,
    action: Action,
) -> Result<(), ApiError> {
    state
        .authorize(claims, path, action)
        .map_err(|err| match err {
            AuthError::Forbidden { path, action } => ApiError::Auth(AuthRejection::Forbidden {
                path,
                action: action.to_string(),
            }),
            other => ApiError::Auth(AuthRejection::Unauthorized(other.to_string())),
        })
}

async fn plan_download(
    state: &ServerState,
    path: &str,
    req_headers: &HeaderMap,
    with_reader: bool,
) -> Result<DownloadPlan, ApiError> {
    let path = validate_rel_path(path).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let meta = state
        .storage
        .file_meta(&path)
        .await?
        .ok_or_else(|| ApiError::NotFound(path.clone()))?;

    // If-None-Match → 304.
    if let Some(v) = req_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        if etag_matches_if_none_match(v, &meta.etag) {
            return Err(ApiError::NotModified);
        }
    }

    // Range negotiation.
    let mut range = None;
    if let Some(raw) = req_headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        range = parse_range_header(raw).map_err(|_| ApiError::RangeMalformed)?;
    }
    if let Some(if_range) = req_headers
        .get(header::IF_RANGE)
        .and_then(|v| v.to_str().ok())
    {
        if !if_range_matches(if_range, &meta.etag) {
            range = None; // ignore Range → full body
        }
    }

    let is_partial = range.is_some();
    let spec = match range {
        Some(ParsedRange::Bytes(r)) => r
            .clamp(meta.size)
            .ok_or(ApiError::RangeUnsatisfiable(meta.size))?,
        Some(ParsedRange::Suffix(n)) => {
            if n == 0 || meta.size == 0 {
                return Err(ApiError::RangeUnsatisfiable(meta.size));
            }
            RangeSpec {
                start: meta.size.saturating_sub(n),
                end: meta.size,
            }
        }
        None => RangeSpec::full(meta.size),
    };

    let format = negotiate_download_format(state, req_headers);
    let (level, echo_level) = negotiate_download_level(state, format, req_headers);
    let reader = if with_reader {
        Some(state.storage.read_stream(&path, spec).await?)
    } else {
        None
    };

    let mut headers = vec![
        (header::ACCEPT_RANGES.as_str(), "bytes".to_string()),
        (
            header::CONTENT_TYPE.as_str(),
            "application/octet-stream".to_string(),
        ),
        (header::ETAG.as_str(), meta.etag),
        (HEADER_COMPRESS, format.as_str().to_string()),
    ];
    if echo_level {
        headers.push((HEADER_COMPRESS_LEVEL, level.to_string()));
    }
    if is_partial {
        headers.push((
            header::CONTENT_RANGE.as_str(),
            content_range_value(&spec, meta.size),
        ));
    }
    if format == CompressionFormat::None {
        headers.push((header::CONTENT_LENGTH.as_str(), spec.len().to_string()));
    }

    Ok(DownloadPlan {
        status: if is_partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        },
        headers,
        format,
        level,
        reader,
    })
}

fn negotiate_download_format(state: &ServerState, req_headers: &HeaderMap) -> CompressionFormat {
    // Only an explicit `zrip` token in Accept-Encoding asks for libfw's
    // private wire format. A browser's standard `Accept-Encoding: … zstd`
    // must NOT be treated as a zrip request — that would send a body the
    // browser cannot decode (and would garble plain `fetch` consumers).
    let wants_zrip = req_headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(',').any(|e| e.trim().eq_ignore_ascii_case("zrip")))
        .unwrap_or(false);
    if wants_zrip && state.compression == CompressionFormat::Zrip {
        CompressionFormat::Zrip
    } else {
        CompressionFormat::None
    }
}

/// Resolve the zrip level for a download from the `x-libfw-compress-level`
/// request header, clamping it into the server's advertised range.
///
/// Returns `(level, echo)`: `echo` is true when the server actively
/// negotiated a level and the response must carry the actual level back on
/// [`HEADER_COMPRESS_LEVEL`]. When the builder left `zrip_levels` unset
/// (legacy mode) the header is ignored, downloads use the default level and
/// nothing is echoed — exactly the pre-0.3.3 behavior. Identity responses
/// never echo (there is no compression to negotiate).
fn negotiate_download_level(
    state: &ServerState,
    format: CompressionFormat,
    req_headers: &HeaderMap,
) -> (i32, bool) {
    let Some(levels) = state.zrip_levels else {
        return (ZRIP_DEFAULT_LEVEL, false);
    };
    if format != CompressionFormat::Zrip {
        return (ZRIP_DEFAULT_LEVEL, false);
    }
    let requested = req_headers
        .get(HEADER_COMPRESS_LEVEL)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<i32>().ok());
    let level = negotiate_level(requested, levels.min, levels.max, levels.default);
    (level, true)
}

pub(crate) async fn download(
    State(state): State<Arc<ServerState>>,
    Path(path): Path<String>,
    BearerClaims(claims): BearerClaims,
    req_headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize_request(&state, &claims, &path, Action::Read)?;
    let plan = plan_download(&state, &path, &req_headers, true).await?;

    let reader = plan.reader.expect("reader requested");
    let stream = body_stream(reader, plan.format, plan.level);
    let mut builder = Response::builder().status(plan.status);
    for (name, value) in plan.headers {
        builder = builder.header(name, value);
    }
    Ok(builder
        .body(Body::from_stream(stream))
        .expect("valid response"))
}

pub(crate) async fn head_file(
    State(state): State<Arc<ServerState>>,
    Path(path): Path<String>,
    BearerClaims(claims): BearerClaims,
    req_headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize_request(&state, &claims, &path, Action::Read)?;
    let plan = plan_download(&state, &path, &req_headers, false).await?;
    let mut builder = Response::builder().status(plan.status);
    for (name, value) in plan.headers {
        builder = builder.header(name, value);
    }
    Ok(builder.body(Body::empty()).expect("valid response"))
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct UploadOk {
    file: FileMeta,
}

/// Response body for a session status probe: the byte ranges already
/// received on the server, so the client can re-send only the missing gaps.
///
/// Serialized as `{"ranges": [[start, end], ...]}` with each range as a pair
/// of arrays (not objects) to keep the wire format compact and aligned with
/// the WASM client's parser.
#[derive(Serialize)]
struct SessionStatus {
    ranges: Vec<[u64; 2]>,
}

/// Write one decompressed batch to the sink, enforcing the server's
/// upload-size cap AND the client-declared `meta.size` bound.
///
/// The bound is computed as `resume_offset + appended_this_request` so a
/// malicious client can never grow a file beyond what it declared (which is
/// itself capped at `max_upload_size`). Counting *decompressed* bytes (not
/// compressed) is what actually protects disk usage.
async fn write_batch(
    sink: &mut Box<dyn UploadSink>,
    state: &ServerState,
    resume_offset: u64,
    meta_size: u64,
    appended: &mut u64,
    data: &[u8],
) -> Result<(), ApiError> {
    *appended = appended.saturating_add(data.len() as u64);
    let total = resume_offset.saturating_add(*appended);
    if total > state.max_upload_size {
        return Err(ApiError::PayloadTooLarge(state.max_upload_size));
    }
    if total > meta_size {
        return Err(ApiError::BadRequest(
            "uploaded bytes exceed the declared file size".into(),
        ));
    }
    sink.write(data).await?;
    Ok(())
}

/// Positional variant of [`write_batch`] for the concurrent session path:
/// `data` is written at its absolute offset (`base_offset + written`), so
/// chunks may arrive out of order and still land in the right place.
async fn write_at_batch(
    sink: &mut Box<dyn UploadSink>,
    state: &ServerState,
    base_offset: u64,
    meta_size: u64,
    written: &mut u64,
    data: &[u8],
) -> Result<(), ApiError> {
    let abs = base_offset.saturating_add(*written);
    let end = abs.saturating_add(data.len() as u64);
    if end > state.max_upload_size {
        return Err(ApiError::PayloadTooLarge(state.max_upload_size));
    }
    if end > meta_size {
        return Err(ApiError::BadRequest(
            "uploaded bytes exceed the declared file size".into(),
        ));
    }
    sink.write_at(abs, data).await?;
    *written = written.saturating_add(data.len() as u64);
    Ok(())
}

/// Handle one request of the concurrent "session" upload protocol.
///
/// Data-chunk requests write their body at the ABSOLUTE `x-libfw-offset`
/// into a shared per-session temp file and do not finalize; the
/// `x-libfw-final` (commit) request verifies the temp holds exactly
/// `meta.size` bytes, then atomically renames it into place.
/// Derive a short, stable owner tag from a token subject.
///
/// The tag is embedded in the session temp filename so that sessions belonging
/// to different authenticated principals never collide — even if a client
/// sends a session id it guessed or observed from another user. We use the
/// first 16 hex chars of the SHA-256 of `sub` (64 bits of collision
/// resistance, sufficient for this discrimination role).
fn owner_tag(sub: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(sub.as_bytes());
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(16);
    for &b in digest.iter().take(8) {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

async fn upload_session(
    state: &ServerState,
    path: &str,
    session: &str,
    claims: &TokenClaims,
    meta: &FileMeta,
    format: CompressionFormat,
    final_chunk: bool,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    // The first chunk (offset 0, or a Create chunk with no offset header)
    // creates the shared temp and selects Create/Overwrite; later chunks
    // reuse it and only supply their absolute offset.
    let offset_hdr = headers.get(HEADER_OFFSET).and_then(|v| v.to_str().ok());
    let base_offset = match offset_hdr {
        None => 0u64,
        Some(off) => off
            .trim()
            .parse::<u64>()
            .map_err(|_| ApiError::BadRequest(format!("invalid `{HEADER_OFFSET}`")))?,
    };
    let create_mode = if offset_hdr.is_none() {
        WriteMode::Create
    } else {
        WriteMode::Overwrite
    };

    let owner = owner_tag(&claims.sub);
    let mut sink = state
        .storage
        .write_stream_session(path, session, &owner, create_mode)
        .await?;

    // A status probe (`x-libfw-session-status`) asks "which byte ranges of
    // this session are already on disk?" without writing anything. The
    // client uses this after an interruption to re-send only the missing
    // blocks (BitTorrent-style resume). We drop the sink WITHOUT abort so
    // the shared temp and its sidecar stay intact for the following chunks.
    let status_probe = headers
        .get(HEADER_SESSION_STATUS)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("probe")
        })
        .unwrap_or(false);
    if status_probe {
        let ranges = sink.received_ranges().await?;
        drop(sink);
        let ranges = ranges
            .into_iter()
            .map(|r| [r.start, r.end])
            .collect::<Vec<[u64; 2]>>();
        return Ok((StatusCode::OK, Json(SessionStatus { ranges })).into_response());
    }

    // Per-frame safety is enforced by the decompressor itself (each frame is
    // capped at MAX_FRAME_OUTPUT); the per-CALL budget here is deliberately
    // generous so a body chunk carrying many small frames (e.g. a client that
    // slices each upload chunk into ~64 KiB frames to decouple frame size
    // from its configured `chunkSize`) is never rejected.
    let mut decomp = decompressor_with_limit(format, MAX_OUTPUT_PER_CALL);
    let mut out: Vec<u8> = Vec::new();
    let mut written = 0u64;
    let mut stream = body.into_data_stream();

    let write_result: Result<(), ApiError> = async {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| ApiError::Io(std::io::Error::other(e)))?;
            decomp
                .decompress(&chunk, &mut out)
                .map_err(|e| ApiError::BadRequest(format!("compressed stream invalid: {e}")))?;
            let data = std::mem::take(&mut out);
            if !data.is_empty() {
                write_at_batch(&mut sink, state, base_offset, meta.size, &mut written, &data)
                    .await?;
            }
        }
        decomp
            .finish(&mut out)
            .map_err(|e| ApiError::BadRequest(format!("compressed stream truncated: {e}")))?;
        if !out.is_empty() {
            write_at_batch(&mut sink, state, base_offset, meta.size, &mut written, &out).await?;
        }
        Ok(())
    }
    .await;

    if let Err(e) = write_result {
        let _ = sink.abort().await;
        return Err(e);
    }

    // Only the commit request finalizes: verify the shared temp holds the
    // exact declared size (all chunks present, no truncation), then rename.
    if final_chunk {
        let len = sink.len().await?;
        if len != meta.size {
            let _ = sink.abort().await;
            return Err(ApiError::BadRequest(format!(
                "commit yields {} bytes but the declared file size is {}",
                len, meta.size
            )));
        }
        // `len() == meta.size` is NOT enough: positional writes make a
        // missing MIDDLE chunk extend the temp to `meta.size` with a
        // zero-filled gap, which must never be committed as a complete file.
        // Reject a commit whose received ranges don't fully cover the file.
        // Sinks that don't track ranges return an empty list; for those we
        // fall back to the length-only check (backward compatibility).
        let received = sink.received_ranges().await?;
        if !received.is_empty() {
            let covered: u64 = received.iter().map(|r| r.end.saturating_sub(r.start)).sum();
            if covered != meta.size {
                let _ = sink.abort().await;
                return Err(ApiError::BadRequest(format!(
                    "commit covers {covered} bytes but the declared file size is {}; \
                     some blocks are missing",
                    meta.size
                )));
            }
        }
        let committed = sink.commit().await?;
        return Ok((StatusCode::CREATED, Json(UploadOk { file: committed })).into_response());
    }

    // Data chunk: keep the shared temp for subsequent requests.
    Ok((StatusCode::CREATED, Json(UploadOk { file: meta.clone() })).into_response())
}

pub(crate) async fn upload(
    State(state): State<Arc<ServerState>>,
    Path(path): Path<String>,
    BearerClaims(claims): BearerClaims,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    authorize_request(&state, &claims, &path, Action::Write)?;
    let path = validate_rel_path(path.as_str()).map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let meta_header = headers
        .get(HEADER_FILE_META)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::BadRequest(format!("missing `{HEADER_FILE_META}` header")))?;
    let meta: FileMeta = decode_file_meta_header(meta_header)
        .map_err(|e| ApiError::BadRequest(format!("invalid file meta: {e}")))?;
    if meta.size > state.max_upload_size {
        return Err(ApiError::PayloadTooLarge(state.max_upload_size));
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

    // Optional per-upload session id. When present the client pipelines many
    // chunks in flight, each carrying its ABSOLUTE `x-libfw-offset` and
    // written into a shared per-session temp file (positional writes); only
    // the `x-libfw-final` request commits. Absent → legacy sequential
    // per-request upload (Create/Overwrite/Resume) — fully backward
    // compatible with older clients.
    if let Some(session) = headers
        .get(HEADER_SESSION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        return upload_session(
            &state,
            &path,
            &session,
            &claims,
            &meta,
            format,
            final_chunk,
            headers,
            body,
        )
        .await;
    }

    // Absent offset → Create (409 if exists); `0` → Overwrite; `N>0` → Resume.
    let mode = match headers.get(HEADER_OFFSET).and_then(|v| v.to_str().ok()) {
        None => WriteMode::Create,
        Some(off) if off.trim().parse::<u64>().map(|n| n == 0).unwrap_or(false) => {
            WriteMode::Overwrite
        }
        Some(off) => {
            let offset = off
                .trim()
                .parse::<u64>()
                .map_err(|_| ApiError::BadRequest(format!("invalid `{HEADER_OFFSET}`")))?;
            WriteMode::Resume { offset }
        }
    };
    let resume_offset = match mode {
        WriteMode::Resume { offset } => offset,
        _ => 0,
    };

    let mut sink = state.storage.write_stream(&path, mode).await?;
    // Per-frame safety is enforced by the decompressor itself (each frame is
    // capped at MAX_FRAME_OUTPUT); the per-CALL budget here is deliberately
    // generous so a body chunk carrying many small frames (e.g. a client that
    // slices each upload chunk into ~64 KiB frames) is never rejected.
    let mut decomp = decompressor_with_limit(format, MAX_OUTPUT_PER_CALL);
    let mut out: Vec<u8> = Vec::new();
    let mut appended = 0u64;
    let mut stream = body.into_data_stream();

    // Run the streaming writes; on any failure abort the (temp) sink so no
    // partial target or orphan temp file is left behind.
    let write_result: Result<(), ApiError> = async {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| ApiError::Io(std::io::Error::other(e)))?;
            decomp
                .decompress(&chunk, &mut out)
                .map_err(|e| ApiError::BadRequest(format!("compressed stream invalid: {e}")))?;
            let data = std::mem::take(&mut out);
            if !data.is_empty() {
                write_batch(
                    &mut sink,
                    &state,
                    resume_offset,
                    meta.size,
                    &mut appended,
                    &data,
                )
                .await?;
            }
        }
        // Flush any final decompressed frames.
        decomp
            .finish(&mut out)
            .map_err(|e| ApiError::BadRequest(format!("compressed stream truncated: {e}")))?;
        if !out.is_empty() {
            write_batch(
                &mut sink,
                &state,
                resume_offset,
                meta.size,
                &mut appended,
                &out,
            )
            .await?;
        }
        Ok(())
    }
    .await;

    if let Err(e) = write_result {
        let _ = sink.abort().await;
        return Err(e);
    }

    // On the final chunk, the committed size must EXACTLY match the
    // client-declared `meta.size` (write_batch already rejects overruns;
    // this rejects undersized/truncated final bodies so a partial file can
    // never be committed as a complete one). Older clients that omit the
    // header keep the previous behavior.
    let final_size = resume_offset.saturating_add(appended);
    if final_chunk && final_size != meta.size {
        let _ = sink.abort().await;
        return Err(ApiError::BadRequest(format!(
            "final chunk yields {} bytes but the declared file size is {}",
            final_size, meta.size
        )));
    }

    let committed = sink.commit().await?;
    Ok((StatusCode::CREATED, Json(UploadOk { file: committed })).into_response())
}

// ---------------------------------------------------------------------------
// Directory listing
// ---------------------------------------------------------------------------

pub(crate) async fn list_dir(
    State(state): State<Arc<ServerState>>,
    Path(path): Path<String>,
    BearerClaims(claims): BearerClaims,
) -> Result<Response, ApiError> {
    list_dir_impl(&state, &claims, &path).await
}

/// `GET /dir` — list the mount root (the wildcard route `/dir/{*path}`
/// does not match the bare `/dir`, so it needs its own handler).
pub(crate) async fn list_dir_root(
    State(state): State<Arc<ServerState>>,
    BearerClaims(claims): BearerClaims,
) -> Result<Response, ApiError> {
    list_dir_impl(&state, &claims, "").await
}

async fn list_dir_impl(
    state: &ServerState,
    claims: &TokenClaims,
    path: &str,
) -> Result<Response, ApiError> {
    authorize_request(state, claims, path, Action::Read)?;
    let path = validate_rel_path(path).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let entries = state.storage.list_dir(&path).await?;
    Ok(Json(entries).into_response())
}

// ---------------------------------------------------------------------------
// Body stream helpers
// ---------------------------------------------------------------------------

/// Turn a blocking reader into an async byte stream (reads run on
/// `spawn_blocking` so the runtime thread stays free).
fn reader_stream(reader: Box<dyn Read + Send>) -> BoxStream<'static, Result<Bytes, io::Error>> {
    let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(4);
    tokio::task::spawn_blocking(move || {
        let mut reader = reader;
        let mut buf = vec![0u8; STREAM_BUF_SIZE];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if tx
                        .blocking_send(Ok(Bytes::copy_from_slice(&buf[..n])))
                        .is_err()
                    {
                        break; // consumer dropped
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

/// Wrap a byte stream through the streaming compressor.
struct CompressedStream<S> {
    inner: S,
    compressor: Box<dyn Compressor>,
    finished: bool,
}

impl<S> Stream for CompressedStream<S>
where
    S: Stream<Item = Result<Bytes, io::Error>> + Unpin,
{
    type Item = Result<Bytes, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    if !this.finished {
                        this.finished = true;
                        let mut tail = Vec::new();
                        match this.compressor.finish(&mut tail) {
                            Ok(()) => {
                                if tail.is_empty() {
                                    return Poll::Ready(None);
                                }
                                return Poll::Ready(Some(Ok(Bytes::from(tail))));
                            }
                            Err(e) => {
                                return Poll::Ready(Some(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    e,
                                ))));
                            }
                        }
                    }
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    let mut out = Vec::new();
                    match this.compressor.compress(&chunk, &mut out) {
                        Ok(()) => {
                            if out.is_empty() {
                                continue;
                            }
                            return Poll::Ready(Some(Ok(Bytes::from(out))));
                        }
                        Err(e) => {
                            return Poll::Ready(Some(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                e,
                            ))));
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}

fn body_stream(
    reader: Box<dyn Read + Send>,
    format: CompressionFormat,
    level: i32,
) -> BoxStream<'static, Result<Bytes, io::Error>> {
    let raw = reader_stream(reader);
    match format {
        CompressionFormat::None => raw,
        CompressionFormat::Zrip => {
            let compressor = compressor_with_level(CompressionFormat::Zrip, level)
                .expect("zrip compressor available");
            CompressedStream {
                inner: raw,
                compressor,
                finished: false,
            }
            .boxed()
        }
    }
}
