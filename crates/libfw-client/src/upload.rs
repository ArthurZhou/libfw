//! Upload scheduler: reads upload files via JS callbacks, slices them into
//! fixed-size chunks, compresses each chunk into one zstd frame and POSTs
//! them with `x-libfw-offset` so the server can resume/validate offsets.

use wasm_bindgen::JsValue;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

use futures::StreamExt;
use libfw_core::compress::{compressor, CompressionFormat};
use libfw_core::metadata::encode_file_meta_header;
use libfw_core::{
    HEADER_COMPRESS, HEADER_FILE_META, HEADER_FINAL, HEADER_OFFSET, HEADER_SESSION,
    HEADER_SESSION_STATUS,
};

use crate::config::ClientConfig;
use crate::error::LibfwError;
use crate::http::{auth_headers, fetch, file_url, read_all, request};
use crate::js::Callbacks;
use crate::plan::{chunk_bounds, total_bytes, FileEntry};
use crate::state::TaskControl;

/// Sleep for `ms` milliseconds on the JS event loop (shared with download).
pub(crate) async fn sleep_ms(ms: u32) {
    if ms == 0 {
        return;
    }
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let window = web_sys::window().expect("window");
        let f: &js_sys::Function = resolve.unchecked_ref();
        let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(f, ms as i32);
    });
    let _ = JsFuture::from(promise).await;
}

/// POST a single chunk (or the commit request) for a file.
///
/// `offset` is the ABSOLUTE byte offset of `body` in the final file.
/// When `session` is non-empty the request uses the concurrent "session"
/// protocol: the server writes the body at `offset` into a shared per-session
/// temp file and does NOT commit unless `final_chunk` is set. When `session`
/// is empty the legacy sequential protocol is used (offset = resume point,
/// commit on `final_chunk`).
async fn post_chunk(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    offset: u64,
    body: &[u8],
    compress: bool,
    timeout_ms: u32,
    final_chunk: bool,
    session: &str,
) -> Result<(), LibfwError> {
    let headers = auth_headers(token, false)?;
    headers
        .set(HEADER_OFFSET, &offset.to_string())
        .map_err(|e| LibfwError::Js(format!("set offset header failed: {e:?}")))?;
    headers
        .set(HEADER_FILE_META, &encode_file_meta_header(&file.to_meta()))
        .map_err(|e| LibfwError::Js(format!("set meta header failed: {e:?}")))?;
    if !session.is_empty() {
        headers
            .set(HEADER_SESSION, session)
            .map_err(|e| LibfwError::Js(format!("set session header failed: {e:?}")))?;
    }
    // Only advertise zrip when there is a body to compress (the commit
    // request carries an empty body and is always identity).
    if compress && !body.is_empty() {
        headers
            .set(HEADER_COMPRESS, "zrip")
            .map_err(|e| LibfwError::Js(format!("set compress header failed: {e:?}")))?;
    }
    // Mark the final chunk so the server can verify the committed size
    // matches the declared `meta.size` (and reject truncated uploads).
    if final_chunk {
        headers
            .set(HEADER_FINAL, "1")
            .map_err(|e| LibfwError::Js(format!("set final header failed: {e:?}")))?;
    }

    let url = file_url(base_url, &file.path);
    let body_value = js_sys::Uint8Array::from(body);
    let req = request(&url, "POST", &headers, Some(&body_value.into()))?;
    let resp = fetch(&req, timeout_ms).await?;
    let status = resp.status();
    if status == 201 {
        Ok(())
    } else {
        Err(LibfwError::Http { status, url })
    }
}

/// A deterministic, URL-safe session id for a file version.
///
/// Derived from the file's ETag (size + mtime) so an interrupted upload of
/// the *same file version* finds the same shared temp on the server and can
/// resume. A changed file produces a different ETag → a different session →
/// a fresh temp, which naturally invalidates stale partials. The ETag is a
/// quoted hex digest; stripping the quotes leaves only alphanumeric hex
/// chars, which the server allows in temp filenames.
fn session_id_for(file: &FileEntry) -> String {
    file.to_meta().etag.trim_matches('"').to_string()
}

/// Probe the server for the byte ranges already received for `session`.
///
/// Returns `Ok(Some(ranges))` when the server understood the probe (it
/// replies with `{"ranges": [[start, end], ...]}`). Returns `Ok(None)` when
/// the response has no `ranges` field — a legacy server that ignores the
/// probe header and simply echoes the file meta; the caller then treats the
/// session as empty (a full re-send, which is correct thanks to idempotent
/// positional writes).
async fn probe_session(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    session: &str,
    timeout_ms: u32,
) -> Result<Option<Vec<(u64, u64)>>, LibfwError> {
    let headers = auth_headers(token, false)?;
    headers
        .set(HEADER_OFFSET, "0")
        .map_err(|e| LibfwError::Js(format!("set offset header failed: {e:?}")))?;
    headers
        .set(HEADER_FILE_META, &encode_file_meta_header(&file.to_meta()))
        .map_err(|e| LibfwError::Js(format!("set meta header failed: {e:?}")))?;
    headers
        .set(HEADER_SESSION, session)
        .map_err(|e| LibfwError::Js(format!("set session header failed: {e:?}")))?;
    headers
        .set(HEADER_SESSION_STATUS, "1")
        .map_err(|e| LibfwError::Js(format!("set session-status header failed: {e:?}")))?;

    let url = file_url(base_url, &file.path);
    let req = request(&url, "POST", &headers, None)?;
    let resp = fetch(&req, timeout_ms).await?;
    let status = resp.status();
    if status != 200 && status != 201 {
        return Err(LibfwError::Http { status, url });
    }
    let body = read_all(&resp, timeout_ms).await?;
    #[derive(serde::Deserialize)]
    struct Ranges {
        #[serde(default)]
        ranges: Vec<[u64; 2]>,
    }
    let parsed: Ranges = serde_json::from_slice(&body)
        .map_err(|e| LibfwError::Protocol(format!("bad session-status JSON: {e}")))?;
    // Empty `ranges` could mean either "nothing received yet" (legit) or a
    // legacy server that echoed meta without a range list; in both cases the
    // caller treats it as "nothing received", which is safe.
    Ok(Some(
        parsed
            .ranges
            .into_iter()
            .map(|[s, e]| (s, e.max(s)))
            .collect(),
    ))
}

/// The sub-ranges of a whole file (aligned to `chunk_size` boundaries) not
/// covered by any `received` range.
///
/// Used after a probe to compute exactly which blocks are still missing, so
/// only the broken/lost parts get re-transmitted (tus-style resume).
fn aligned_missing(file: &FileEntry, chunk_size: u64, received: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut missing = Vec::new();
    for (start, end) in chunk_bounds(file, chunk_size, 0) {
        missing.extend(missing_ranges(start, end, received));
    }
    missing
}

/// The sub-ranges of `[start, end)` not covered by any `received` range.
///
/// Used after a probe to compute exactly which bytes are still missing, so
/// only the broken/lost parts get re-transmitted (BitTorrent-style resume).
fn missing_ranges(start: u64, end: u64, received: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut missing = vec![(start, end)];
    for range in received {
        let (rs0, re0) = *range;
        let rs = rs0.max(start);
        let re = re0.min(end).max(rs);
        if re <= rs {
            continue;
        }
        let mut next = Vec::with_capacity(missing.len() + 1);
        for (ms, me) in missing {
            if re <= ms || rs >= me {
                // No overlap with this received range.
                next.push((ms, me));
            } else {
                // Keep the parts outside [rs, re).
                if ms < rs {
                    next.push((ms, rs));
                }
                if re < me {
                    next.push((re, me));
                }
            }
        }
        missing = next;
    }
    missing
}

/// Read, compress and POST one chunk with per-chunk retry + backoff.
async fn upload_one_chunk(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
    start: u64,
    end: u64,
    session: &str,
) -> Result<u64, LibfwError> {
    control.wait_ready().await?;
    control.check()?;

    let len = end - start;
    let raw = callbacks.read_file(&file.path, start, len).await?;
    if raw.len() as u64 != len {
        return Err(LibfwError::Storage(format!(
            "read {} of {} bytes for `{}`",
            raw.len(),
            len,
            file.path
        )));
    }

    // Compress the chunk into a single independent zstd frame.
    let payload: Vec<u8> = if config.compress {
        let mut enc = compressor(CompressionFormat::Zrip)
            .map_err(|e| LibfwError::Compress(e.to_string()))?;
        let mut out = Vec::with_capacity(raw.len());
        enc.compress(&raw, &mut out)
            .map_err(|e| LibfwError::Compress(e.to_string()))?;
        enc.finish(&mut out)
            .map_err(|e| LibfwError::Compress(e.to_string()))?;
        out
    } else {
        raw
    };

    let mut attempts = 0u32;
    loop {
        control.wait_ready().await?;
        control.check()?;
        match post_chunk(
            base_url,
            token,
            file,
            start,
            &payload,
            config.compress,
            config.timeout_ms,
            false,
            session,
        )
        .await
        {
            Ok(()) => break,
            Err(e) => {
                if attempts >= config.max_retries {
                    return Err(e);
                }
                attempts += 1;
                callbacks.log(&format!(
                    "retrying chunk {start}..{end} of `{}` (attempt {attempts}): {e}",
                    file.path
                ));
                sleep_ms(config.backoff_ms(attempts)).await;
            }
        }
    }

    control.add_progress(len);
    // Report smooth intermediate progress during a long single-file upload
    // (throttled to whole-percent boundaries so a 200 MB file doesn't sit at
    // 0% until it jumps to 100%).
    control.report_progress_if(callbacks)?;
    Ok(len)
}

/// Send the final `x-libfw-final` commit request, with retry + backoff.
async fn commit_upload(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
    session: &str,
) -> Result<(), LibfwError> {
    let mut attempts = 0u32;
    loop {
        control.wait_ready().await?;
        control.check()?;
        match post_chunk(
            base_url,
            token,
            file,
            file.size,
            &[],
            false,
            config.timeout_ms,
            true,
            session,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempts >= config.max_retries {
                    return Err(e);
                }
                attempts += 1;
                callbacks.log(&format!(
                    "retrying commit for `{}` (attempt {attempts}): {e}",
                    file.path
                ));
                sleep_ms(config.backoff_ms(attempts)).await;
            }
        }
    }
}

/// Upload a whole file with the resumable, out-of-order "session" protocol.
///
/// This is the tus-style transfer path: each block is POSTed with its
/// ABSOLUTE `x-libfw-offset` into a shared per-session temp on the server
/// (positional writes), so blocks may be sent out of order and pipelined
/// with a bounded in-flight window — throughput is bounded by bandwidth
/// instead of `chunk_size / RTT`.
///
/// The server is the **source of truth**: before and after sending, the
/// client probes the byte ranges the server has actually persisted (tus's
/// `Upload-Offset` / `HEAD` philosophy) and re-sends *only* the still-missing
/// gaps — "verify-then-complete". Retransmission is self-healing: a lost
/// response that nonetheless landed server-side is detected by the next probe
/// (no wasted re-send), a block whose write was truly lost is re-sent, and a
/// failed commit triggers a fresh probe + refill instead of failing the task.
/// A final `x-libfw-final` commit request verifies the merged size then
/// renames the temp into place ("merge").
///
/// A legacy server that ignores the probe yields an empty range list → a
/// full re-send, which is correct thanks to idempotent positional writes.
async fn upload_session_resumable(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<u64, LibfwError> {
    let session = session_id_for(file);
    let window = config.upload_window.max(1);

    // Initial probe: bytes the server already holds (from a previous
    // interrupted attempt) are seeded into progress so a resume reflects the
    // true fraction. They are NOT counted in the returned `uploaded` figure,
    // which reports only what THIS session retained.
    let received = probe_session(base_url, token, file, &session, config.timeout_ms).await?
        .unwrap_or_default();
    let initial_covered = covered_bytes(&received).min(file.size);
    if initial_covered > 0 {
        control.add_progress(initial_covered);
        control.report_progress_if(callbacks)?;
    }

    let mut rounds = 0u32;
    loop {
        control.wait_ready().await?;
        control.check()?;

        // 1. Ask the server what it actually holds (authoritative).
        let received = probe_session(base_url, token, file, &session, config.timeout_ms).await?
            .unwrap_or_default();
        let missing = aligned_missing(file, config.chunk_size, &received);

        // 2. Everything present → commit. A commit failure (e.g. a size
        //    mismatch from a racing write) does NOT fail the task: we loop
        //    back, re-probe and re-fill the gaps, then retry the commit.
        if missing.is_empty() {
            match commit_upload(base_url, token, file, callbacks, control, config, &session)
                .await
            {
                Ok(()) => {
                    callbacks
                        .save_state(
                            "upload",
                            &file.path,
                            &state_json(file.size, &file.to_meta().etag, file.size),
                        )
                        .await?;
                    let final_covered = covered_bytes(&received).min(file.size);
                    return Ok(final_covered.saturating_sub(initial_covered));
                }
                Err(e) => {
                    if rounds >= config.max_retries {
                        return Err(e);
                    }
                    rounds += 1;
                    callbacks.log(&format!(
                        "commit failed for `{}`; re-verifying server state: {e}",
                        file.path
                    ));
                    continue;
                }
            }
        }

        // 3. Re-send ONLY the missing blocks, concurrently (out of order)
        //    with a bounded per-file window — independent of (and typically
        //    larger than) the cross-file `concurrency`, so one file keeps
        //    enough chunks in flight to fill the bandwidth-delay product on
        //    high-latency links. Per-block failures are collected, not fatal:
        //    the next probe decides what truly remains, so we only ever
        //    retransmit the broken/lost parts.
        let mut stream = futures::stream::iter(missing.into_iter().map(|(start, end)| {
            let base_url = base_url.to_string();
            let token = token.to_string();
            let file = file.clone();
            let callbacks = callbacks.clone();
            let control = control.clone();
            let config = config.clone();
            let session = session.clone();
            async move {
                upload_one_chunk(
                    &base_url, &token, &file, &callbacks, &control, &config, start, end, &session,
                )
                .await
            }
        }))
        .buffer_unordered(window);

        let mut first_error: Option<LibfwError> = None;
        while let Some(res) = stream.next().await {
            if let Err(e) = res {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        rounds += 1;
        if rounds > config.max_retries {
            // Bounded: give the server one final probe; if it still reports
            // gaps, surface the first underlying error (or a convergence
            // error). This mirrors tus's give-up path after repeated HEADs.
            let received = probe_session(base_url, token, file, &session, config.timeout_ms).await?
                .unwrap_or_default();
            if aligned_missing(file, config.chunk_size, &received).is_empty() {
                continue; // converged → loop top commits
            }
            return Err(first_error.unwrap_or_else(|| {
                LibfwError::Protocol(format!(
                    "upload of `{}` did not converge after {rounds} rounds",
                    file.path
                ))
            }));
        }
    }
}

/// Total number of bytes covered by a set of (possibly overlapping) received
/// byte ranges.
fn covered_bytes(received: &[(u64, u64)]) -> u64 {
    let mut total = 0u64;
    for (s, e) in received {
        if e > s {
            total = total.saturating_add(e - s);
        }
    }
    total
}

/// Upload one file with the resumable, out-of-order "session" protocol.
///
/// Every upload (fresh, overwrite or interrupted resume) goes through
/// [`upload_session_resumable`]: the server is probed for which byte ranges
/// it already holds, only the missing blocks are re-sent concurrently, and a
/// final commit merges them. An interrupted transfer simply leaves the
/// partially-received session temp on the server; the next attempt probes it
/// and retransmits only the broken/lost parts (BitTorrent-style), never the
/// whole file.
async fn upload_file(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<u64, LibfwError> {
    callbacks.on_file_start(&file.path, file.size)?;
    let uploaded =
        upload_session_resumable(base_url, token, file, callbacks, control, config).await?;
    callbacks.on_file_completed(&file.path).await?;
    Ok(uploaded)
}

/// Build a resume-state object for JS persistence.
fn state_json(offset: u64, etag: &str, size: u64) -> JsValue {
    let state = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &state,
        &JsValue::from_str("offset"),
        &JsValue::from_f64(offset as f64),
    );
    let _ = js_sys::Reflect::set(&state, &JsValue::from_str("etag"), &JsValue::from_str(etag));
    let _ = js_sys::Reflect::set(&state, &JsValue::from_str("size"), &JsValue::from_f64(size as f64));
    state.into()
}

/// Upload every file reported by the JS `getFileList` callback.
pub async fn upload(
    base_url: &str,
    token: &str,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<u64, LibfwError> {
    let files = callbacks.file_list().await?;
    let total = total_bytes(&files);
    control.set_total(total);
    callbacks.on_progress(0, total)?;

    let mut stream = futures::stream::iter(files.into_iter().map(|file| {
        let base_url = base_url.to_string();
        let token = token.to_string();
        let callbacks = callbacks.clone();
        let control = control.clone();
        let config = config.clone();
        async move { upload_file(&base_url, &token, &file, &callbacks, &control, &config).await }
    }))
    .buffer_unordered(config.concurrency);

    let mut done = 0u64;
    while let Some(result) = stream.next().await {
        done = done.saturating_add(result?);
        // Single source of truth for progress is the shared control block.
        // Clamp the done figure so a rare gap-fill re-send (which re-counts a
        // few bytes) can never show a bar past 100%.
        let total = control.total_bytes();
        let reported_done = if total == 0 {
            control.done_bytes()
        } else {
            control.done_bytes().min(total)
        };
        callbacks.on_progress(reported_done, total)?;
    }
    Ok(done)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_missing_covers_full_file_when_nothing_received() {
        let f = FileEntry {
            path: "f.bin".into(),
            size: 10,
            mtime: 1,
        };
        let missing = aligned_missing(&f, 4, &[]);
        assert_eq!(missing, vec![(0, 4), (4, 8), (8, 10)]);
    }

    #[test]
    fn aligned_missing_only_gaps() {
        let f = FileEntry {
            path: "f.bin".into(),
            size: 20,
            mtime: 1,
        };
        // Received [0,4) and [8,12) → only the two gaps remain, aligned.
        let missing = aligned_missing(&f, 4, &[(0, 4), (8, 12)]);
        assert_eq!(missing, vec![(4, 8), (12, 16), (16, 20)]);
    }

    #[test]
    fn aligned_missing_empty_when_fully_received() {
        let f = FileEntry {
            path: "f.bin".into(),
            size: 12,
            mtime: 1,
        };
        let missing = aligned_missing(&f, 4, &[(0, 12)]);
        assert!(missing.is_empty());
    }

    #[test]
    fn missing_ranges_none_when_fully_received() {
        let received = vec![(0, 100)];
        assert!(missing_ranges(0, 100, &received).is_empty());
    }

    #[test]
    fn missing_ranges_reports_only_gaps() {
        // Received [0,40) and [60,100); missing is the gap [40,60).
        let received = vec![(0, 40), (60, 100)];
        let missing = missing_ranges(0, 100, &received);
        assert_eq!(missing, vec![(40, 60)]);
    }

    #[test]
    fn missing_ranges_splits_partial_coverage() {
        // Desired [0,20) but only [8,16) received → [0,8) + [16,20) missing.
        let received = vec![(8, 16)];
        let missing = missing_ranges(0, 20, &received);
        assert_eq!(missing, vec![(0, 8), (16, 20)]);
    }

    #[test]
    fn missing_ranges_out_of_scope_received_ignored() {
        // Received ranges outside the desired window are ignored.
        let received = vec![(100, 200)];
        let missing = missing_ranges(0, 20, &received);
        assert_eq!(missing, vec![(0, 20)]);
    }

    #[test]
    fn session_id_is_stable_and_safe() {
        let a = FileEntry {
            path: "dir/f.bin".into(),
            size: 1024,
            mtime: 42,
        };
        let id = session_id_for(&a);
        // Deterministic: same file version → same id.
        let again = FileEntry {
            path: "dir/f.bin".into(),
            size: 1024,
            mtime: 42,
        };
        assert_eq!(session_id_for(&again), id);
        // Safe chars only (server allows [A-Za-z0-9_-]).
        assert!(id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        // Different file version → different id (fresh session temp).
        let changed = FileEntry {
            path: "dir/f.bin".into(),
            size: 2048,
            mtime: 42,
        };
        assert_ne!(session_id_for(&changed), id);
    }
}
