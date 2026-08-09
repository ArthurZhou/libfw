//! Upload scheduler: reads upload files via JS callbacks, slices them into
//! fixed-size chunks, compresses each chunk into one zstd frame and POSTs
//! them with `x-libfw-offset` so the server can resume/validate offsets.

use js_sys::Reflect;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;

use futures::StreamExt;
use libfw_core::compress::{compressor, CompressionFormat};
use libfw_core::metadata::encode_file_meta_header;
use libfw_core::{HEADER_COMPRESS, HEADER_FILE_META, HEADER_FINAL, HEADER_OFFSET};

use crate::config::ClientConfig;
use crate::error::LibfwError;
use crate::http::{auth_headers, fetch, file_url, request};
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

/// POST a single chunk; returns the number of accepted bytes.
async fn post_chunk(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    offset: u64,
    body: &[u8],
    compress: bool,
    timeout_ms: u32,
    final_chunk: bool,
) -> Result<(), LibfwError> {
    let headers = auth_headers(token, false)?;
    headers
        .set(HEADER_OFFSET, &offset.to_string())
        .map_err(|e| LibfwError::Js(format!("set offset header failed: {e:?}")))?;
    headers
        .set(HEADER_FILE_META, &encode_file_meta_header(&file.to_meta()))
        .map_err(|e| LibfwError::Js(format!("set meta header failed: {e:?}")))?;
    if compress {
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

/// Upload one file chunk by chunk, with retry + resume support.
///
/// When the server answers `412 Precondition Failed` (the resume offset no
/// longer matches — e.g. the file was truncated), the persisted state is
/// cleared and the file is re-uploaded from byte 0 (bounded to one reset).
async fn upload_file(
    base_url: &str,
    token: &str,
    file: &FileEntry,
    callbacks: &Callbacks,
    control: &TaskControl,
    config: &ClientConfig,
) -> Result<u64, LibfwError> {
    callbacks.on_file_start(&file.path, file.size)?;

    let mut uploaded_total = 0u64;
    let mut restarted = false;

    'file: loop {
        // Load persisted resume state: { etag, offset }.
        let mut offset = 0u64;
        if let Some(state) = callbacks.load_state("upload", &file.path).await? {
            if let Some(o) = Reflect::get(&state, &JsValue::from_str("offset"))
                .ok()
                .and_then(|v| v.as_f64())
            {
                offset = o as u64;
            }
        }
        offset = offset.min(file.size);

        // Seed the shared progress with the prefix already on the server so
        // a pure resume reaches 100% at completion instead of showing only
        // this session's delta.
        let mut pass_added = offset;
        control.add_progress(offset);
        let mut pass_uploaded = 0u64;

        for (start, end) in chunk_bounds(file, config.chunk_size, offset) {
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
                    end == file.size,
                )
                .await
                {
                    Ok(()) => break,
                    Err(LibfwError::Http { status: 412, .. }) if !restarted => {
                        // Server says our offset is stale → wipe state and
                        // re-upload the whole file from byte 0. Undo this
                        // pass's progress (resume seed + already-posted
                        // bytes) so the file's bytes are never double-counted
                        // in the progress bar or the returned byte count.
                        restarted = true;
                        callbacks.log(&format!(
                            "server rejected offset {start} for `{}`; resetting",
                            file.path
                        ));
                        control.subtract_progress(pass_added);
                        let _ = callbacks
                            .save_state(
                                "upload",
                                &file.path,
                                &state_json(0, &file.to_meta().etag, file.size),
                            )
                            .await;
                        continue 'file;
                    }
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

            pass_uploaded = pass_uploaded.saturating_add(len);
            pass_added = pass_added.saturating_add(len);
            control.add_progress(len);

            // Persist progress so a crash/resume continues from here.
            callbacks
                .save_state(
                    "upload",
                    &file.path,
                    &state_json(end, &file.to_meta().etag, file.size),
                )
                .await?;
        }
        uploaded_total = uploaded_total.saturating_add(pass_uploaded);
        break 'file;
    }

    callbacks.on_file_completed(&file.path).await?;
    Ok(uploaded_total)
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
        callbacks.on_progress(control.done_bytes(), control.total_bytes())?;
    }
    Ok(done)
}
