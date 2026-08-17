//! Upload scheduler: reads upload files via JS callbacks, slices them into
//! fixed-size chunks, compresses each chunk into one zstd frame and POSTs
//! them with `x-libfw-offset` so the server can resume/validate offsets.

use std::cell::Cell;
use std::rc::Rc;

use wasm_bindgen::JsValue;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

use futures::StreamExt;
use libfw_core::compress::{CompressionFormat, compressor_with_level};
use libfw_core::metadata::encode_file_meta_header;
use libfw_core::{
    HEADER_COMPRESS, HEADER_FILE_META, HEADER_FINAL, HEADER_OFFSET, HEADER_SESSION,
    HEADER_SESSION_STATUS,
};

use crate::config::ClientConfig;
use crate::error::LibfwError;
use crate::http::{auth_headers, fetch, file_url, read_all, request, xhr_post};
use crate::js::Callbacks;
use crate::plan::{chunk_bounds, total_bytes, FileEntry};
use crate::state::{Semaphore, TaskControl};
use crate::tune::{LEVEL_SAMPLE_SIZE, TuneEvent, TuneHandle, now_ms};

/// Feed one tuning sample and apply any emitted event (semaphore resize).
pub(crate) fn tune_tick(
    tune: &TuneHandle,
    control: &TaskControl,
    done_bytes: u64,
    rtt_ms: Option<f64>,
    error: bool,
) -> Option<TuneEvent> {
    let event = tune.borrow_mut().tick(now_ms(), done_bytes, rtt_ms, error);
    if let Some(ev) = &event {
        control.set_max_parallel(ev.params.concurrency);
    }
    event
}

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
///
/// `semaphore` is the engine-wide in-flight HTTP pool (sized by
/// `concurrency`): every data request takes a permit so `concurrency` bounds
/// the TOTAL number of parallel transfers, not just concurrent files.
///
/// `on_progress` (when set) is invoked on every XHR upload-progress tick with
/// `(loaded, total)` bytes of the body being sent, enabling wire-level
/// real-time progress reporting.
// clippy: this is the single choke point for every upload request; grouping
// the progress plumbing would churn every caller for no gain.
#[allow(clippy::too_many_arguments)]
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
    semaphore: &Semaphore,
    on_progress: Option<Rc<dyn Fn(u64, u64)>>,
) -> Result<(), LibfwError> {
    // Header pairs for the XHR upload path (XHR cannot consume a `Headers`).
    let mut headers: Vec<(String, String)> = vec![
        ("Authorization".to_string(), format!("Bearer {token}")),
        (
            libfw_core::HEADER_PROTOCOL.to_string(),
            libfw_core::protocol_header_value().to_string(),
        ),
    ];
    headers.push((HEADER_OFFSET.to_string(), offset.to_string()));
    headers.push((HEADER_FILE_META.to_string(), encode_file_meta_header(&file.to_meta())));
    if !session.is_empty() {
        headers.push((HEADER_SESSION.to_string(), session.to_string()));
    }
    // Only advertise zrip when there is a body to compress (the commit
    // request carries an empty body and is always identity).
    if compress && !body.is_empty() {
        headers.push((HEADER_COMPRESS.to_string(), "zrip".to_string()));
    }
    // Mark the final chunk so the server can verify the committed size
    // matches the declared `meta.size` (and reject truncated uploads).
    if final_chunk {
        headers.push((HEADER_FINAL.to_string(), "1".to_string()));
    }

    let url = file_url(base_url, &file.path);
    // Hold the permit for the whole request so the global cap is respected.
    let _permit = semaphore.acquire().await;
    // XHR-based upload with a NO-PROGRESS timeout: `fetch` cannot observe
    // upload progress, so a slow-but-active upload on a low-bandwidth link
    // (e.g. through a CF tunnel) would be killed by a wall clock. The XHR
    // path only aborts when nothing has moved for `timeout_ms`; the optional
    // `on_progress` callback turns the wire ticks into real-time progress.
    let promise = xhr_post(&url, &headers, body, timeout_ms, on_progress)?;
    let status = JsFuture::from(promise)
        .await
        .map_err(|e| LibfwError::Network(format!("upload request failed: {e:?}")))?
        .as_f64()
        .unwrap_or(0.0) as u16;
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
    let headers = auth_headers(token, false, None)?;
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
///
/// `level` is the negotiated zrip level (only used when `compress` is true).
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
    compress: bool,
    level: i32,
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

    // Compress the chunk into many small (~64 KiB) independent zstd frames
    // rather than one frame per chunk. The wire format is a concatenation of
    // frames, and the server caps each *frame* (MAX_FRAME_OUTPUT), so this
    // keeps the frame size independent of the configured `chunkSize` — a
    // larger `chunkSize` then just means more frames per request instead of a
    // frame the server would reject.
    let payload: Vec<u8> = if compress {
        let mut enc = compressor_with_level(CompressionFormat::Zrip, level)
            .map_err(|e| LibfwError::Compress(e.to_string()))?;
        let mut out = Vec::with_capacity(raw.len());
        for window in raw.chunks(libfw_core::STREAM_BUF_SIZE) {
            enc.compress(window, &mut out)
                .map_err(|e| LibfwError::Compress(e.to_string()))?;
        }
        enc.finish(&mut out)
            .map_err(|e| LibfwError::Compress(e.to_string()))?;
        out
    } else {
        raw
    };

    let mut attempts = 0u32;
    // Wire-level progress accounting for this chunk. `last_loaded` tracks how
    // many bytes of THIS payload have already been counted, and the closure
    // only counts the delta on every XHR progress tick, so:
    //  - a large chunk shows smooth, real-time progress instead of a jump,
    //  - a retried attempt never double-counts bytes it already reported
    //    (the counter only moves forward across attempts), and
    //  - the success path below reconciles any uncounted tail.
    let last_loaded = Rc::new(Cell::new(0u64));
    let on_progress = {
        let control = control.clone();
        let callbacks = callbacks.clone();
        let last_loaded = last_loaded.clone();
        Some(Rc::new(move |loaded: u64, _total: u64| {
            let prev = last_loaded.get();
            if loaded > prev {
                last_loaded.set(loaded);
                control.add_progress(loaded - prev);
                // Best-effort: a throwing JS progress handler must not abort
                // the transfer (the synchronous post-chunk report below is
                // still authoritative).
                let _ = control.report_progress_if(&callbacks);
            }
        }) as Rc<dyn Fn(u64, u64)>)
    };
    loop {
        control.wait_ready().await?;
        control.check()?;
        match post_chunk(
            base_url,
            token,
            file,
            start,
            &payload,
            compress,
            config.timeout_ms,
            false,
            session,
            control.semaphore(),
            on_progress.clone(),
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

    // Reconcile: XHR fires its last progress event slightly before the
    // request settles, so count the (usually tiny) uncounted tail — unless
    // the wire already counted the whole payload.
    let counted = last_loaded.get().min(len);
    if counted < len {
        control.add_progress(len - counted);
    }
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
            control.semaphore(),
            None, // the commit request carries no body, so no wire progress
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
/// The server is the **source of truth**, but only consulted when it must be
/// (download-style "no ack" happy path):
/// - One probe seeds progress from any partial the server already holds and
///   reports which blocks are still missing; round 0 reuses that result, so
///   there is no duplicate probe before the first send.
/// - The missing blocks are then POSTed concurrently; each `201` response is
///   that block's ack, so the client does NOT re-verify before committing —
///   it trusts the acks exactly as download trusts the bytes it receives.
/// - A single `x-libfw-final` commit validates the merged size against
///   `meta.size` and atomically renames the temp into place. The commit — not
///   a probe — is the authority: a chunk the acks missed (rare) surfaces
///   there as a rejection and triggers a re-probe + refill + retry, so
///   self-healing costs nothing on the happy path.
///
/// Retransmission is self-healing: a lost response that nonetheless landed
/// server-side is detected by the next probe (no wasted re-send), a block
/// whose write was truly lost is re-sent, and a failed commit triggers a
/// fresh probe + refill instead of failing the task.
///
/// A legacy server that ignores the probe yields an empty range list → a
/// full re-send, which is correct thanks to idempotent positional writes.
// M3 tuning plumbing (tune handle + negotiated level) pushed this past the
// 7-arg lint; grouping them would churn every call site for no gain.
#[allow(clippy::too_many_arguments)]
async fn upload_session_resumable(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
    tune: &TuneHandle,
    compress: bool,
    level: i32,
) -> Result<u64, LibfwError> {
    let session = session_id_for(file);

    // Initial probe: bytes the server already holds (from a previous
    // interrupted attempt) are seeded into progress so a resume reflects the
    // true fraction. They are NOT counted in the returned `uploaded` figure,
    // which reports only what THIS session retained.
    let mut received = probe_session(base_url, token, file, &session, config.timeout_ms).await?
        .unwrap_or_default();
    let initial_covered = covered_bytes(&received).min(file.size);
    if initial_covered > 0 {
        control.add_progress(initial_covered);
        control.report_progress_if(callbacks)?;
    }
    if initial_covered >= file.size {
        callbacks.log(&format!(
            "upload `{}` already fully present on the server; skipping re-send ({} bytes already covered)",
            file.path, initial_covered
        ));
    }

    let mut rounds = 0u32;
    let mut first_error: Option<LibfwError> = None;
    let uploaded = loop {
        control.wait_ready().await?;
        control.check()?;

        // Live parameter reads: the tuning engine may have raised/lowered
        // the window or chunk size since the last round.
        let window = tune.borrow().params().upload_window.max(1);
        let chunk_size = tune.borrow().params().chunk_size.max(1);

        // 1. Server is the source of truth for what it already holds. Round
        //    0 reuses the initial probe result — we never probe twice before
        //    the first send — and later rounds ask afresh.
        if rounds > 0 {
            received = probe_session(base_url, token, file, &session, config.timeout_ms).await?
                .unwrap_or_default();
        }
        let missing = aligned_missing(file, chunk_size, &received);

        // 2. Everything already present → commit directly (a resume whose
        //    partial covers the whole file, or a converged retry). A commit
        //    failure (e.g. a size mismatch from a racing write) does NOT fail
        //    the task: we loop back, re-probe and re-fill the gaps, then
        //    retry the commit.
        if missing.is_empty() {
            match commit_upload(base_url, token, file, callbacks, control, config, &session)
                .await
            {
                Ok(()) => break file.size.saturating_sub(initial_covered),
                Err(e) => {
                    if rounds >= config.max_retries {
                        return Err(e);
                    }
                    rounds += 1;
                    callbacks.log(&format!(
                        "commit failed for `{}`; re-verifying server state: {e}",
                        file.path
                    ));
                    first_error.get_or_insert(e);
                    continue;
                }
            }
        }

        // 3. Re-send ONLY the missing blocks, concurrently (out of order)
        //    with a bounded per-file window — independent of (and typically
        //    larger than) the cross-file `concurrency`, so one file keeps
        //    enough chunks in flight to fill the bandwidth-delay product on
        //    high-latency links. Per-block failures are collected, not fatal:
        //    each block's 201 response is its ack, and a rejected ack only
        //    retries that block.
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
                    &base_url, &token, &file, &callbacks, &control, &config, start, end,
                    &session, compress, level,
                )
                .await
            }
        }))
        .buffer_unordered(window);

        let mut round_errors = 0u32;
        while let Some(res) = stream.next().await {
            if let Err(e) = res {
                round_errors += 1;
                first_error.get_or_insert(e);
            }
        }

        // 4. No-ack happy path: commit directly instead of asking the server
        //    to re-verify first (mirroring download, which trusts the bytes it
        //    receives). The commit validates the merged size against
        //    `meta.size` — it, not a probe, is the authority — so a chunk the
        //    acks missed (rare) is caught here as a rejection and triggers a
        //    re-probe + refill on the next round.
        match commit_upload(base_url, token, file, callbacks, control, config, &session).await {
            Ok(()) => break file.size.saturating_sub(initial_covered),
            Err(e) => {
                rounds += 1;
                if rounds > config.max_retries {
                    // Bounded: one final probe decides whether we truly did
                    // not converge (surface the first underlying error) or
                    // merely hit a transient commit rejection (converged → one
                    // last commit). Mirrors tus's give-up after repeated HEADs.
                    let received = probe_session(
                        base_url,
                        token,
                        file,
                        &session,
                        config.timeout_ms,
                    )
                    .await?
                    .unwrap_or_default();
                    if aligned_missing(file, chunk_size, &received).is_empty() {
                        commit_upload(base_url, token, file, callbacks, control, config, &session)
                            .await?;
                        break file.size.saturating_sub(initial_covered);
                    }
                    return Err(first_error.unwrap_or_else(|| {
                        LibfwError::Protocol(format!(
                            "upload of `{}` did not converge after {rounds} rounds",
                            file.path
                        ))
                    }));
                }
                callbacks.log(&format!(
                    "commit failed for `{}`; re-verifying server state: {e}",
                    file.path
                ));
                first_error.get_or_insert(e);
            }
        }

        // Feed the tuning engine: one measurement per round (XHR cannot
        // expose TTFB, so no RTT sample), with this round's error count.
        if tune.borrow().enabled() {
            tune_tick(tune, control, control.done_bytes(), None, round_errors > 0);
        }
    };

    callbacks
        .save_state(
            "upload",
            &file.path,
            &state_json(file.size, &file.to_meta().etag, file.size),
        )
        .await?;
    Ok(uploaded)
}

/// Total number of bytes covered by a set of received byte ranges, treating
/// overlapping and adjacent ranges as one contiguous span.
///
/// The server merges ranges before replying, but a backend that doesn't may
/// return overlapping ranges; summing raw lengths would over-count (and in
/// turn over-seed progress / under-report the returned `uploaded` figure).
/// Merge first so the result is the true covered extent.
fn covered_bytes(received: &[(u64, u64)]) -> u64 {
    let mut ranges: Vec<(u64, u64)> = received
        .iter()
        .map(|&(s, e)| (s.min(e), s.max(e)))
        .filter(|&(s, e)| e > s)
        .collect();
    ranges.sort_by_key(|&(s, _)| s);
    let mut total = 0u64;
    let mut cur: Option<(u64, u64)> = None;
    for (s, e) in ranges {
        match cur {
            None => cur = Some((s, e)),
            Some((cs, ce)) => {
                if s <= ce {
                    cur = Some((cs, ce.max(e)));
                } else {
                    total = total.saturating_add(ce.saturating_sub(cs));
                    cur = Some((s, e));
                }
            }
        }
    }
    if let Some((cs, ce)) = cur {
        total = total.saturating_add(ce.saturating_sub(cs));
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
// M3 tuning plumbing (tune handle + negotiated level) pushed this past the
// 7-arg lint; grouping them would churn every call site for no gain.
#[allow(clippy::too_many_arguments)]
async fn upload_file(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
    tune: &TuneHandle,
    compress: bool,
    level: i32,
) -> Result<u64, LibfwError> {
    callbacks.on_file_start(&file.path, file.size)?;
    let uploaded =
        upload_session_resumable(base_url, token, file, callbacks, control, config, tune, compress, level)
            .await?;
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
    tune: &TuneHandle,
) -> Result<u64, LibfwError> {
    let files = callbacks.file_list().await?;
    let total = total_bytes(&files);
    control.set_total(total);
    callbacks.on_progress(0, total)?;

    // Resolve the compression level once per session: `Auto` micro-benchmarks
    // a 256 KiB sample of the first file against the advertised candidates.
    let caps = {
        let t = tune.borrow();
        t.caps().unwrap_or_default()
    };
    let (compress, level) = if config.compress {
        let enabled = {
            let t = tune.borrow();
            t.enabled()
        };
        let sample = if enabled {
            match files.first() {
                Some(f) if f.size > 0 => {
                    let len = (f.size as usize).min(LEVEL_SAMPLE_SIZE);
                    callbacks
                        .read_file(&f.path, 0, len as u64)
                        .await
                        .ok()
                        .filter(|b| !b.is_empty())
                }
                _ => None,
            }
        } else {
            None
        };
        let stats_mbps = {
            let t = tune.borrow();
            t.stats().mbps
        };
        let level = {
            let mut t = tune.borrow_mut();
            t.upload_compress_level(&caps, sample.as_deref(), stats_mbps)
        };
        (true, level)
    } else {
        (false, caps.compression.zrip_levels.default)
    };

    let mut stream = futures::stream::iter(files.into_iter().map(|file| {
        let base_url = base_url.to_string();
        let token = token.to_string();
        let callbacks = callbacks.clone();
        let control = control.clone();
        let config = config.clone();
        let tune = tune.clone();
        async move {
            upload_file(
                &base_url, &token, &file, &callbacks, &control, &config, &tune, compress, level,
            )
            .await
        }
    }))
    .buffer_unordered(tune.borrow().params().concurrency);

    let mut done = 0u64;
    while let Some(result) = stream.next().await {
        match result {
            Ok(bytes) => done = done.saturating_add(bytes),
            Err(e) => {
                if tune.borrow().enabled() {
                    tune_tick(tune, control, control.done_bytes(), None, true);
                }
                return Err(e);
            }
        }
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

    #[test]
    fn covered_bytes_merges_overlaps() {
        // Disjoint ranges sum normally.
        assert_eq!(covered_bytes(&[(0, 4), (8, 12)]), 8);
        // Overlapping ranges are coalesced, not double-counted.
        assert_eq!(covered_bytes(&[(0, 10), (5, 15)]), 15);
        // Adjacent ranges coalesce too.
        assert_eq!(covered_bytes(&[(0, 10), (10, 20)]), 20);
        // Fully covering the span collapses to one range.
        assert_eq!(covered_bytes(&[(0, 100), (40, 60), (60, 100)]), 100);
        // Empty ranges are ignored.
        assert_eq!(covered_bytes(&[(5, 5)]), 0);
    }
}
