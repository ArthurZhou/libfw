//! Native single-file download: resumable `Range` GETs, optional parallel
//! range window, zrip decompression and stall-based retries.
//!
//! Mirrors the browser engine's `download.rs` behaviour (`HEAD` metadata,
//! per-chunk `Range` + `If-Range`, in-order writes, restart when the remote
//! file changed) but writes to a local path with `tokio::fs`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use futures::StreamExt;
use libfw_core::compress::{CompressionFormat, decompressor};
use libfw_core::{HEADER_COMPRESS, MIN_PARALLEL_DOWNLOAD_BYTES};
use reqwest::header::{CONTENT_LENGTH, ETAG, IF_RANGE, RANGE};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::{NativeClient, NativeEvent, validate_relative};
use crate::LibfwError;
use crate::tune::{TransferKind, now_ms};

/// Persisted resume state for one destination file.
///
/// The browser engine keeps this in IndexedDB; natively a tiny JSON sidecar
/// next to the destination is enough. It is keyed by the server's ETag, so a
/// changed remote file (new ETag) transparently restarts from byte 0.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResumeState {
    etag: String,
    offset: u64,
}

/// Sidecar path for `dest` (`<dest>.libfw-resume.json`).
fn resume_path(dest: &Path) -> PathBuf {
    let mut name = dest.as_os_str().to_owned();
    name.push(".libfw-resume.json");
    PathBuf::from(name)
}

fn load_resume(dest: &Path) -> Option<ResumeState> {
    let text = std::fs::read_to_string(resume_path(dest)).ok()?;
    serde_json::from_str(&text).ok()
}

fn save_resume(dest: &Path, etag: &str, offset: u64) -> Result<(), LibfwError> {
    let state = ResumeState {
        etag: etag.to_string(),
        offset,
    };
    let text = serde_json::to_string(&state)
        .map_err(|e| LibfwError::Storage(format!("resume state: {e}")))?;
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| LibfwError::Storage(format!("create dir {}: {e}", parent.display())))?;
    }
    std::fs::write(resume_path(dest), text)
        .map_err(|e| LibfwError::Storage(format!("write resume state: {e}")))
}

/// Save the resume offset at most once per this many bytes (matches the
/// browser engine's periodic persistence).
const RESUME_SAVE_EVERY: u64 = 4 * 1024 * 1024;

/// The contiguous `[start, end)` ranges covering `[from, size)`.
fn parallel_chunks(from: u64, size: u64, chunk_size: u64) -> Vec<(u64, u64)> {
    let chunk_size = if chunk_size == 0 {
        libfw_core::CHUNK_SIZE
    } else {
        chunk_size
    };
    let mut chunks = Vec::new();
    let mut offset = from.min(size);
    while offset < size {
        let end = (offset + chunk_size).min(size);
        chunks.push((offset, end));
        offset = end;
    }
    chunks
}

/// Whether the chunked byte-range path should be used for this file.
///
/// Window-independent (mirrors the browser engine): any transfer with enough
/// remaining bytes to amortise the per-request overhead is chunked, *even at
/// window 1*, because the batch loop re-reads the live parameters between
/// batches — so a ramp that grows the window/chunk size mid-file takes effect
/// on the current download instead of only on the next one. Small files and
/// short tails stay on the streaming path, where one request wins.
fn should_chunked(size: u64, resume_offset: u64) -> bool {
    size >= MIN_PARALLEL_DOWNLOAD_BYTES
        && size.saturating_sub(resume_offset) >= MIN_PARALLEL_DOWNLOAD_BYTES
}

/// Bytes one batch may fetch: `window` chunks, never past the end.
fn batch_span(remaining: u64, chunk_size: u64, window: usize) -> u64 {
    let chunk_size = chunk_size.max(1);
    let window = window.max(1) as u64;
    chunk_size
        .saturating_mul(window)
        .max(chunk_size)
        .min(remaining.max(1))
}

impl NativeClient {
    /// Download `remote` into `dest`, resuming a previous partial download
    /// when the server's ETag still matches.
    ///
    /// Returns the number of bytes written **by this run** (0 when the file
    /// was already complete), so callers can tell a resume from a full fetch.
    /// Emits [`NativeEvent::FileStart`], [`NativeEvent::Progress`] (per file),
    /// [`NativeEvent::FileDone`] and — with adaptive tuning on —
    /// [`NativeEvent::Tuning`].
    pub async fn download_file(
        &self,
        remote: &str,
        dest: impl AsRef<Path>,
    ) -> Result<u64, LibfwError> {
        validate_relative(remote)?;
        let dest = dest.as_ref();
        let params = self.prepare_transfer(TransferKind::Download).await?;

        let mut attempt = self.download_once(remote, dest, params.compress_level, false);
        let mut outcome = attempt.await;
        if let Err(e) = &outcome {
            if super::is_restart_err(e) {
                self.log(format!(
                    "`{remote}` changed or shrank on the server; restarting from byte 0"
                ));
                attempt = self.download_once(remote, dest, params.compress_level, true);
                outcome = attempt.await;
            }
        }
        self.finish_transfer(outcome.is_ok());
        outcome
    }

    /// One download pass. `force_restart` ignores any persisted resume offset.
    async fn download_once(
        &self,
        remote: &str,
        dest: &Path,
        level: i32,
        force_restart: bool,
    ) -> Result<u64, LibfwError> {
        let (etag, size) = self.fetch_meta(remote).await?;

        // Resume only when the remote is the *same version* as the partial we
        // hold: a different ETag means the bytes on disk are unrelated.
        let resume = load_resume(dest).filter(|r| !force_restart && r.etag == etag);
        let local_len = std::fs::metadata(dest).map(|m| m.len()).unwrap_or(0);
        let start = match resume {
            Some(state) if state.offset <= size && local_len >= state.offset => state.offset,
            _ => 0,
        };
        if start > 0 {
            self.log(format!("resuming `{remote}` at byte {start} of {size}"));
        }

        self.emit(NativeEvent::FileStart {
            path: remote.to_string(),
            size,
        });

        if start == size {
            // Empty file, or already fully on disk.
            self.emit(NativeEvent::FileDone {
                path: remote.to_string(),
                bytes: 0,
            });
            return Ok(0);
        }

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| LibfwError::Storage(format!("create dir {}: {e}", parent.display())))?;
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(dest)
            .await
            .map_err(|e| LibfwError::Storage(format!("open {}: {e}", dest.display())))?;

        // Small files and short tails stream in one request; everything else
        // is fetched in bounded batches so the tuned window/chunk size can be
        // re-read mid-file (see `should_chunked`).
        let moved = if should_chunked(size, start) {
            let mut cursor = start;
            let mut moved = 0u64;
            while cursor < size {
                // Re-read the live parameters before every batch: the ramp may
                // have moved them while the previous batch was in flight.
                let window = self
                    .with_tune(|t| t.params().download_window.max(1))
                    .unwrap_or(self.config().download_window.max(1));
                let chunk_size = self
                    .with_tune(|t| t.params().chunk_size.max(1))
                    .unwrap_or(self.config().chunk_size.max(1));
                let end = cursor.saturating_add(batch_span(size - cursor, chunk_size, window));
                let got = self
                    .download_batch(
                        remote, &etag, &mut file, cursor, end, size, moved, window, chunk_size,
                        level, dest,
                    )
                    .await?;
                if got == 0 {
                    break;
                }
                cursor = cursor.saturating_add(got);
                moved = moved.saturating_add(got);
                // Persist progress once per batch (an atomic write, but not a
                // free one).
                let _ = save_resume(dest, &etag, cursor);
            }
            moved
        } else {
            self.download_sequential(remote, &etag, &mut file, start, size, level, dest)
                .await?
        };

        // A resumed file may still carry bytes beyond EOF (the remote shrank
        // but kept the same ETag — impossible in practice, cheap to enforce).
        file.set_len(size)
            .await
            .map_err(|e| LibfwError::Storage(format!("truncate {}: {e}", dest.display())))?;
        file.flush()
            .await
            .map_err(|e| LibfwError::Storage(format!("flush {}: {e}", dest.display())))?;

        let computed = start.saturating_add(moved);
        if computed != size {
            return Err(LibfwError::Protocol(format!(
                "download of `{remote}` stopped at {computed} bytes, expected {size}"
            )));
        }
        // Keep the completed offset so a repeated download is a no-op.
        let _ = save_resume(dest, &etag, size);
        self.emit(NativeEvent::FileDone {
            path: remote.to_string(),
            bytes: moved,
        });
        Ok(moved)
    }

    /// `HEAD /file/<path>` → `(etag, size)`.
    pub(super) async fn fetch_meta(&self, remote: &str) -> Result<(String, u64), LibfwError> {
        let url = self.file_url(remote);
        let resp = self
            .http()
            .head(&url)
            .headers(self.headers(false, None))
            .send()
            .await
            .map_err(|e| LibfwError::Network(e.to_string()))?;
        let status = resp.status().as_u16();
        if status != 200 {
            return Err(LibfwError::Http { status, url });
        }
        let etag = header_str(&resp, ETAG.as_str());
        let size = header_str(&resp, CONTENT_LENGTH.as_str())
            .parse::<u64>()
            .ok()
            .unwrap_or(0);
        Ok((etag, size))
    }

    /// One batch: `window` concurrent range GETs over `[from, to)`, written in
    /// order so the download can resume from a contiguous offset.
    ///
    /// `total` is the whole file size and `done_base` the bytes this run has
    /// already written, so progress stays meaningful across batches.
    #[allow(clippy::too_many_arguments)]
    async fn download_batch(
        &self,
        remote: &str,
        etag: &str,
        file: &mut tokio::fs::File,
        from: u64,
        to: u64,
        total: u64,
        done_base: u64,
        window: usize,
        chunk_size: u64,
        level: i32,
        dest: &Path,
    ) -> Result<u64, LibfwError> {
        let chunks = parallel_chunks(from, to, chunk_size);
        let remote_owned = remote.to_string();
        let etag_owned = etag.to_string();
        let mut stream = futures::stream::iter(chunks.into_iter().map(|(s, e)| {
            let remote = remote_owned.clone();
            let etag = etag_owned.clone();
            async move {
                let (data, rtt) = self
                    .get_range_with_retry(&remote, &etag, s, e, level)
                    .await?;
                Ok::<_, LibfwError>((s, data, rtt))
            }
        }))
        .buffer_unordered(window);

        let mut pending: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
        let mut cursor = from;
        let mut moved = 0u64;
        let mut last_saved = from;

        while let Some(result) = stream.next().await {
            let (chunk_start, data, rtt) = match result {
                Ok(part) => part,
                Err(e) => {
                    // Record the failure so the engine can shrink parameters,
                    // then surface it (the caller may restart the file).
                    self.tune_tick(done_base.saturating_add(moved), None, true);
                    return Err(e);
                }
            };
            pending.insert(chunk_start, data);
            // Flush everything that is now contiguous.
            while let Some(data) = pending.remove(&cursor) {
                file.seek(std::io::SeekFrom::Start(cursor))
                    .await
                    .map_err(|e| LibfwError::Storage(format!("seek: {e}")))?;
                file.write_all(&data)
                    .await
                    .map_err(|e| LibfwError::Storage(format!("write: {e}")))?;
                cursor = cursor.saturating_add(data.len() as u64);
                moved = moved.saturating_add(data.len() as u64);
                self.emit(NativeEvent::Progress {
                    done: done_base.saturating_add(moved),
                    total,
                });
                if cursor >= last_saved.saturating_add(RESUME_SAVE_EVERY) {
                    last_saved = cursor;
                    let _ = save_resume(dest, etag, cursor);
                }
                self.tune_tick(done_base.saturating_add(moved), Some(rtt), false);
            }
        }
        Ok(moved)
    }

    /// Single-connection download of `[start, size)`, streamed and
    /// decompressed incrementally so memory stays bounded.
    #[allow(clippy::too_many_arguments)]
    async fn download_sequential(
        &self,
        remote: &str,
        etag: &str,
        file: &mut tokio::fs::File,
        start: u64,
        size: u64,
        level: i32,
        dest: &Path,
    ) -> Result<u64, LibfwError> {
        let (resp, rtt) = self
            .range_request(remote, etag, start, size, level)
            .await?;
        let format = compression_format(&resp);
        let mut dec = decompressor(format);
        let mut cursor = start;
        let mut moved = 0u64;
        let mut last_saved = 0u64;
        let mut body = resp.bytes_stream();
        let mut out = Vec::new();

        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| LibfwError::Network(e.to_string()))?;
            out.clear();
            dec.decompress(&chunk, &mut out)
                .map_err(|e| LibfwError::Decompress(e.to_string()))?;
            if out.is_empty() {
                continue;
            }
            file.seek(std::io::SeekFrom::Start(cursor))
                .await
                .map_err(|e| LibfwError::Storage(format!("seek: {e}")))?;
            file.write_all(&out)
                .await
                .map_err(|e| LibfwError::Storage(format!("write: {e}")))?;
            cursor = cursor.saturating_add(out.len() as u64);
            moved = moved.saturating_add(out.len() as u64);
            if cursor >= last_saved.saturating_add(RESUME_SAVE_EVERY) {
                last_saved = cursor;
                let _ = save_resume(dest, etag, cursor);
            }
            self.emit(NativeEvent::Progress {
                done: moved,
                total: size,
            });
            self.tune_tick(moved, Some(rtt), false);
        }

        out.clear();
        dec.finish(&mut out)
            .map_err(|e| LibfwError::Decompress(e.to_string()))?;
        if !out.is_empty() {
            file.seek(std::io::SeekFrom::Start(cursor))
                .await
                .map_err(|e| LibfwError::Storage(format!("seek: {e}")))?;
            file.write_all(&out)
                .await
                .map_err(|e| LibfwError::Storage(format!("write: {e}")))?;
            moved = moved.saturating_add(out.len() as u64);
        }
        Ok(moved)
    }

    /// One range GET with per-chunk exponential-backoff retries.
    async fn get_range_with_retry(
        &self,
        remote: &str,
        etag: &str,
        start: u64,
        end: u64,
        level: i32,
    ) -> Result<(Vec<u8>, f64), LibfwError> {
        let mut attempt = 0u32;
        loop {
            match self.range_to_vec(remote, etag, start, end, level).await {
                Ok((data, rtt)) => {
                    if data.len() as u64 != end - start {
                        return Err(LibfwError::Protocol(format!(
                            "chunk {start}..{end} of `{remote}` yielded {} bytes, expected {}",
                            data.len(),
                            end - start
                        )));
                    }
                    return Ok((data, rtt));
                }
                // "File changed" — retrying is pointless; restart instead.
                Err(e) if super::is_restart_err(&e) => return Err(e),
                Err(e) => {
                    if attempt >= self.config().max_retries {
                        return Err(e);
                    }
                    attempt += 1;
                    self.log(format!(
                        "retrying chunk {start}..{end} of `{remote}` (attempt {attempt}): {e}"
                    ));
                    tokio::time::sleep(self.backoff(attempt)).await;
                }
            }
        }
    }

    /// `GET` one range and collect it into memory (parallel path).
    async fn range_to_vec(
        &self,
        remote: &str,
        etag: &str,
        start: u64,
        end: u64,
        level: i32,
    ) -> Result<(Vec<u8>, f64), LibfwError> {
        let (resp, rtt) = self.range_request(remote, etag, start, end, level).await?;
        let format = compression_format(&resp);
        let mut dec = decompressor(format);
        let body = resp
            .bytes()
            .await
            .map_err(|e| LibfwError::Network(e.to_string()))?;
        let mut out = Vec::new();
        dec.decompress(&body, &mut out)
            .map_err(|e| LibfwError::Decompress(e.to_string()))?;
        dec.finish(&mut out)
            .map_err(|e| LibfwError::Decompress(e.to_string()))?;
        Ok((out, rtt))
    }

    /// Issue a `Range` GET, returning the response (206 only) plus its TTFB.
    async fn range_request(
        &self,
        remote: &str,
        etag: &str,
        start: u64,
        end: u64,
        level: i32,
    ) -> Result<(reqwest::Response, f64), LibfwError> {
        let url = self.file_url(remote);
        let last = end.saturating_sub(1);
        let mut request = self
            .http()
            .get(&url)
            .headers(self.headers(self.config().compress, Some(level)))
            .header(RANGE, format!("bytes={start}-{last}"));
        if !etag.is_empty() {
            request = request.header(IF_RANGE, etag);
        }
        let t0 = now_ms();
        let resp = request
            .send()
            .await
            .map_err(|e| LibfwError::Network(e.to_string()))?;
        let rtt = now_ms() - t0;
        match resp.status() {
            StatusCode::PARTIAL_CONTENT => Ok((resp, rtt)),
            // 200 to a ranged request means the file changed under us; 416
            // means it shrank. Both are "restart from byte 0".
            other => Err(LibfwError::Http {
                status: other.as_u16(),
                url,
            }),
        }
    }
}

/// The wire compression format of a response.
fn compression_format(resp: &reqwest::Response) -> CompressionFormat {
    resp.headers()
        .get(HEADER_COMPRESS)
        .and_then(|v| v.to_str().ok())
        .and_then(CompressionFormat::parse_header)
        .unwrap_or(CompressionFormat::None)
}

/// Read a response header as a `String` (empty when absent/not ASCII).
fn header_str(resp: &reqwest::Response, name: &str) -> String {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_chunks_cover_the_tail_exactly() {
        assert_eq!(
            parallel_chunks(0, 10, 4),
            vec![(0, 4), (4, 8), (8, 10)]
        );
        assert_eq!(parallel_chunks(5, 10, 4), vec![(5, 9), (9, 10)]);
        assert!(parallel_chunks(10, 10, 4).is_empty());
        // A zero chunk size falls back to the protocol default.
        let chunks = parallel_chunks(0, 10, 0);
        assert_eq!(chunks, vec![(0, 10)]);
    }

    #[test]
    fn should_chunked_is_window_independent_and_ignores_small_tails() {
        let big = MIN_PARALLEL_DOWNLOAD_BYTES * 2;
        assert!(should_chunked(big, 0));
        // Window 1 is not known here on purpose: the batch loop re-reads the
        // live window per batch, so a minimal start still fetches bounded
        // chunks (regression: one whole-file 206 that could never be tuned).
        assert!(!should_chunked(1024, 0), "small files stay sequential");
        assert!(
            !should_chunked(MIN_PARALLEL_DOWNLOAD_BYTES, MIN_PARALLEL_DOWNLOAD_BYTES - 1),
            "the remaining tail must also be large"
        );
    }

    #[test]
    fn batch_span_is_bounded_and_never_zero() {
        // window × chunk, clamped to what is left.
        assert_eq!(batch_span(10 * 1024 * 1024, 256 * 1024, 4), 1024 * 1024);
        assert_eq!(batch_span(300 * 1024, 256 * 1024, 4), 300 * 1024);
        // Junk (window 0 / chunk 0) must still make progress.
        assert_eq!(batch_span(1024, 0, 0), 1);
    }

    #[test]
    fn resume_sidecar_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("file.bin");
        assert!(load_resume(&dest).is_none());
        save_resume(&dest, "\"abc\"", 1234).unwrap();
        let state = load_resume(&dest).unwrap();
        assert_eq!(state.etag, "\"abc\"");
        assert_eq!(state.offset, 1234);
        assert!(resume_path(&dest).exists());
    }
}
