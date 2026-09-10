//! Native single-file upload: the tus-style *session* protocol with
//! probe-based resume, pipelined chunks and zrip compression.
//!
//! Mirrors the browser engine's `upload.rs`: the server is the source of
//! truth (a probe reports which byte ranges it already holds), only the
//! missing blocks are re-sent, each block is a positional write into a shared
//! per-session temp, and a final commit validates coverage before atomically
//! renaming it into place.

use std::path::Path;
use std::time::UNIX_EPOCH;

use futures::StreamExt;
use libfw_core::metadata::{FileMeta, encode_file_meta_header};
use libfw_core::{
    HEADER_COMPRESS, HEADER_FILE_META, HEADER_FINAL, HEADER_OFFSET, HEADER_SESSION,
    HEADER_SESSION_STATUS,
};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::{NativeClient, NativeEvent, compress_chunk, session_id_for, validate_relative};
use crate::LibfwError;
use crate::plan::FileEntry;
use crate::tune::{CompressLevel, LEVEL_SAMPLE_SIZE, TransferKind};
use crate::upload::aligned_missing;

/// File metadata for a local file being uploaded as `remote`.
fn local_meta(local: &Path, remote: &str) -> Result<FileMeta, LibfwError> {
    let meta = std::fs::metadata(local)
        .map_err(|e| LibfwError::Storage(format!("stat {}: {e}", local.display())))?;
    if !meta.is_file() {
        return Err(LibfwError::Storage(format!(
            "{} is not a regular file",
            local.display()
        )));
    }
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Ok(FileMeta::new(remote, meta.len(), mtime))
}

/// Read `len` bytes at `start` from `path` (each concurrent block opens its
/// own handle so reads never interleave).
async fn read_slice(path: &Path, start: u64, len: u64) -> Result<Vec<u8>, LibfwError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| LibfwError::Storage(format!("open {}: {e}", path.display())))?;
    file.seek(std::io::SeekFrom::Start(start))
        .await
        .map_err(|e| LibfwError::Storage(format!("seek: {e}")))?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)
        .await
        .map_err(|e| LibfwError::Storage(format!("read {} bytes at {start}: {e}", len)))?;
    Ok(buf)
}

impl NativeClient {
    /// Upload `local` to the virtual path `remote`.
    ///
    /// Returns the payload bytes sent **by this run** (0 when the server
    /// already held the whole file). Resumes automatically: a probe reports
    /// the ranges the server still has, so an interrupted upload re-sends
    /// only the gaps.
    pub async fn upload_file(&self, local: &Path, remote: &str) -> Result<u64, LibfwError> {
        validate_relative(remote)?;
        // The upload path resolves its own zrip level below (`upload_level`,
        // which may micro-benchmark `Auto`), so the preamble is needed here
        // only to bootstrap the tuning engine for this direction.
        let _ = self.prepare_transfer(TransferKind::Upload).await?;
        let meta = local_meta(local, remote)?;
        let entry = FileEntry {
            path: remote.to_string(),
            size: meta.size,
            mtime: meta.mtime,
        };
        let session = session_id_for(&meta);
        let compress = self.config().compress;

        self.emit(NativeEvent::FileStart {
            path: remote.to_string(),
            size: meta.size,
        });

        // Resolve the zrip level: `Auto` micro-benchmarks a real sample of
        // this file (once per session) against the advertised range.
        let caps = self.capabilities().unwrap_or_default();
        let sample = if compress && self.config().compress_level == CompressLevel::Auto {
            let len = (meta.size).min(LEVEL_SAMPLE_SIZE as u64) as usize;
            if len == 0 {
                None
            } else {
                read_slice(local, 0, len as u64).await.ok()
            }
        } else {
            None
        };
        let mbps = self.with_tune(|t| t.stats().mbps).unwrap_or(0.0);
        let level = self.upload_level(&caps, sample.as_deref(), mbps);

        let outcome = self
            .upload_session(local, &entry, &meta, &session, compress, level)
            .await;
        self.finish_transfer(outcome.is_ok());

        let sent = outcome?;
        self.emit(NativeEvent::FileDone {
            path: remote.to_string(),
            bytes: sent,
        });
        Ok(sent)
    }

    /// Drive one file's session upload to completion.
    async fn upload_session(
        &self,
        local: &Path,
        entry: &FileEntry,
        meta: &FileMeta,
        session: &str,
        compress: bool,
        level: i32,
    ) -> Result<u64, LibfwError> {
        let size = meta.size;
        let mut sent = 0u64;
        let mut rounds = 0u32;
        let mut first_error: Option<LibfwError> = None;

        // Round 0 reuses the initial probe; later rounds re-probe so a lost
        // ack is detected instead of blindly re-sent.
        let mut received = self.probe_session(meta, session).await?;
        let covered: u64 = received
            .iter()
            .map(|(s, e)| e.saturating_sub(*s))
            .sum::<u64>()
            .min(size);
        if covered > 0 {
            self.log(format!(
                "upload `{}`: server already holds {covered} of {size} bytes; resuming",
                entry.path
            ));
        }

        loop {
            if rounds > 0 {
                received = self.probe_session(meta, session).await?;
            }
            let window = self
                .with_tune(|t| t.params().upload_window.max(1))
                .unwrap_or(self.config().upload_window.max(1));
            let chunk_size = self
                .with_tune(|t| t.params().chunk_size.max(1))
                .unwrap_or(self.config().chunk_size.max(1));
            let missing = aligned_missing(entry, chunk_size, &received);

            // Everything the server needs is present → commit (this is also
            // the converged-retry path).
            if missing.is_empty() {
                match self.commit(meta, session).await {
                    Ok(()) => {
                        // The success path skips the end-of-loop tick, so feed
                        // the engine here: an upload must contribute a
                        // measurement or the ramp never leaves the minimums.
                        self.tune_tick(sent, None, false);
                        return Ok(sent);
                    }
                    Err(e) => {
                        if rounds >= self.config().max_retries {
                            return Err(e);
                        }
                        rounds += 1;
                        self.log(format!(
                            "commit failed for `{}`; re-verifying server state: {e}",
                            entry.path
                        ));
                        first_error.get_or_insert(e);
                        continue;
                    }
                }
            }

            // Send only the missing blocks, pipelined up to `window`.
            let local_owned = local.to_path_buf();
            let meta_owned = meta.clone();
            let session_owned = session.to_string();
            let mut stream = futures::stream::iter(missing.into_iter().map(|(start, end)| {
                let local = local_owned.clone();
                let meta = meta_owned.clone();
                let session = session_owned.clone();
                async move { (start, self.post_block(&local, &meta, &session, start, end, compress, level).await) }
            }))
            .buffer_unordered(window);

            let mut round_errors = 0u32;
            while let Some((start, result)) = stream.next().await {
                match result {
                    Ok(sent_len) => {
                        sent = sent.saturating_add(sent_len);
                        self.emit(NativeEvent::Progress {
                            done: sent,
                            total: size,
                        });
                        self.tune_tick(sent, None, false);
                    }
                    Err(e) => {
                        round_errors += 1;
                        self.log(format!("block at {start} of `{}` failed: {e}", entry.path));
                        first_error.get_or_insert(e);
                    }
                }
            }

            // No-ack happy path: trust the 201 acks and commit; a rejection
            // re-probes and refills on the next round.
            match self.commit(meta, session).await {
                Ok(()) => {
                    self.tune_tick(sent, None, round_errors > 0);
                    return Ok(sent);
                }
                Err(e) => {
                    rounds += 1;
                    if rounds > self.config().max_retries {
                        // One last probe decides: converged (commit once more)
                        // or truly stuck (surface the first error).
                        let received = self.probe_session(meta, session).await?;
                        if aligned_missing(entry, chunk_size, &received).is_empty() {
                            self.commit(meta, session).await?;
                            return Ok(sent);
                        }
                        return Err(first_error.unwrap_or_else(|| {
                            LibfwError::Protocol(format!(
                                "upload of `{}` did not converge after {rounds} rounds",
                                entry.path
                            ))
                        }));
                    }
                    self.log(format!(
                        "commit failed for `{}`; re-verifying server state: {e}",
                        entry.path
                    ));
                    first_error.get_or_insert(e);
                }
            }

            self.tune_tick(sent, None, round_errors > 0);
        }
    }

    /// Probe the server for the ranges a session already holds.
    async fn probe_session(
        &self,
        meta: &FileMeta,
        session: &str,
    ) -> Result<Vec<(u64, u64)>, LibfwError> {
        let url = self.file_url(&meta.path);
        let resp = self
            .http()
            .post(&url)
            .headers(self.headers(false, None))
            .header(HEADER_OFFSET, "0")
            .header(HEADER_FILE_META, encode_file_meta_header(meta))
            .header(HEADER_SESSION, session)
            .header(HEADER_SESSION_STATUS, "1")
            .send()
            .await
            .map_err(|e| LibfwError::Network(e.to_string()))?;
        let status = resp.status().as_u16();
        if status != 200 && status != 201 {
            return Err(LibfwError::Http { status, url });
        }
        let body = resp
            .bytes()
            .await
            .map_err(|e| LibfwError::Network(e.to_string()))?;
        #[derive(Deserialize)]
        struct Ranges {
            #[serde(default)]
            ranges: Vec<[u64; 2]>,
        }
        // An empty range list means "nothing received" — or a legacy server
        // that echoed the meta without a range list; both are safe, because a
        // full re-send is idempotent under positional writes.
        let parsed: Ranges = serde_json::from_slice(&body)
            .map_err(|e| LibfwError::Protocol(format!("bad session-status JSON: {e}")))?;
        Ok(parsed
            .ranges
            .into_iter()
            .map(|[s, e]| (s, e.max(s)))
            .collect())
    }

    /// POST one block (with retries), returning the payload bytes sent.
    async fn post_block(
        &self,
        local: &Path,
        meta: &FileMeta,
        session: &str,
        start: u64,
        end: u64,
        compress: bool,
        level: i32,
    ) -> Result<u64, LibfwError> {
        let raw = read_slice(local, start, end - start).await?;
        // Compress into many ~64 KiB frames so no single frame can exceed the
        // server's per-frame cap, whatever `chunk_size` is.
        let payload = if compress {
            compress_chunk(&raw, level)?
        } else {
            raw
        };
        let len = payload.len() as u64;
        let url = self.file_url(&meta.path);
        let mut attempt = 0u32;
        loop {
            let mut request = self
                .http()
                .post(&url)
                .headers(self.headers(false, None))
                .header(HEADER_OFFSET, start.to_string())
                .header(HEADER_FILE_META, encode_file_meta_header(meta))
                .header(HEADER_SESSION, session)
                .body(payload.clone());
            if compress {
                request = request.header(HEADER_COMPRESS, "zrip");
            }
            match request.send().await {
                Ok(resp) if resp.status() == StatusCode::CREATED => return Ok(len),
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let err = LibfwError::Http { status, url: url.clone() };
                    if attempt >= self.config().max_retries {
                        return Err(err);
                    }
                    attempt += 1;
                    self.log(format!(
                        "retrying block {start}..{end} of `{}` (attempt {attempt}): {err}",
                        meta.path
                    ));
                    tokio::time::sleep(self.backoff(attempt)).await;
                }
                Err(e) => {
                    if attempt >= self.config().max_retries {
                        return Err(LibfwError::Network(e.to_string()));
                    }
                    attempt += 1;
                    self.log(format!(
                        "retrying block {start}..{end} of `{}` (attempt {attempt}): {e}",
                        meta.path
                    ));
                    tokio::time::sleep(self.backoff(attempt)).await;
                }
            }
        }
    }

    /// Commit the session (validates coverage, then renames into place).
    async fn commit(&self, meta: &FileMeta, session: &str) -> Result<(), LibfwError> {
        let url = self.file_url(&meta.path);
        let resp = self
            .http()
            .post(&url)
            .headers(self.headers(false, None))
            .header(HEADER_OFFSET, meta.size.to_string())
            .header(HEADER_FILE_META, encode_file_meta_header(meta))
            .header(HEADER_SESSION, session)
            .header(HEADER_FINAL, "1")
            .send()
            .await
            .map_err(|e| LibfwError::Network(e.to_string()))?;
        let status = resp.status();
        if status == StatusCode::CREATED {
            Ok(())
        } else {
            Err(LibfwError::Http {
                status: status.as_u16(),
                url,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_meta_rejects_missing_files() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.bin");
        assert!(local_meta(&missing, "nope.bin").is_err());
    }

    #[test]
    fn local_meta_computes_a_stable_session_etag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        std::fs::write(&path, b"hello").unwrap();
        let meta = local_meta(&path, "f.bin").unwrap();
        assert_eq!(meta.size, 5);
        assert_eq!(meta.path, "f.bin");
        // The session id is derived from size + mtime, so it is stable across
        // runs (which is what makes an interrupted upload resumable).
        let again = local_meta(&path, "f.bin").unwrap();
        assert_eq!(session_id_for(&meta), session_id_for(&again));
        assert!(!session_id_for(&meta).is_empty());
    }

    #[tokio::test]
    async fn read_slice_reads_the_requested_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        std::fs::write(&path, b"0123456789").unwrap();
        assert_eq!(read_slice(&path, 2, 4).await.unwrap(), b"2345");
        // Reading past EOF is an error, not silent truncation.
        assert!(read_slice(&path, 8, 4).await.is_err());
    }
}
