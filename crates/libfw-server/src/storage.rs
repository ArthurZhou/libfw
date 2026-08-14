//! Local filesystem storage backend for `libfw-server`.
//!
//! Writes go to a temporary file next to the destination and are
//! atomically renamed into place on [`UploadSink::commit`], so a failed or
//! aborted upload never leaves a partial target behind.

use std::io::{Read, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use libfw_core::metadata::{etag_from_size_mtime, ChunkRange, FileMeta};
use libfw_core::range::RangeSpec;
use libfw_core::storage::{DirEntry, StorageBackend, UploadSink, WriteMode};
use libfw_core::StorageError;

/// A [`StorageBackend`] rooted at a local directory.
///
/// Paths passed to the backend are treated as relative to `root`; any path
/// escaping the root (absolute, `..`, symlink-traversing) is rejected.
#[derive(Debug, Clone)]
pub struct FsStorage {
    root: PathBuf,
    /// Serializes read-merge-write of the per-session `.blocks` sidecars so
    /// concurrent chunk requests never clobber each other's received-range
    /// bookkeeping (a lost update here makes the commit coverage check reject
    /// a fully-written file).
    sidecar_lock: Arc<tokio::sync::Mutex<()>>,
}

impl FsStorage {
    /// Create a backend serving files under `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        FsStorage {
            root: root.into(),
            sidecar_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Resolve a virtual path against the root, rejecting traversal.
    ///
    /// Both textual `..` segments and symlinked path components are
    /// rejected so a read/write can never escape the mount root through a
    /// symlink planted inside it.
    fn resolve(&self, path: &str) -> Result<PathBuf, StorageError> {
        let rel = Path::new(path);
        if rel.is_absolute() {
            return Err(StorageError::Unsupported("absolute paths are not allowed"));
        }
        let mut joined = self.root.clone();
        for component in rel.components() {
            match component {
                Component::Normal(seg) => {
                    joined.push(seg);
                    // Reject any component that is itself a symlink.
                    match std::fs::symlink_metadata(&joined) {
                        Ok(m) if m.file_type().is_symlink() => {
                            return Err(StorageError::Unsupported(
                                "path must not traverse a symlink",
                            ))
                        }
                        Ok(_) => {}
                        // A not-yet-existing component is fine (e.g. a new
                        // upload target); it will be validated as it is
                        // created component by component.
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(StorageError::Other(e)),
                    }
                }
                Component::CurDir => {}
                _ => {
                    return Err(StorageError::Unsupported(
                        "path must not contain '..' or special components",
                    ))
                }
            }
        }
        Ok(joined)
    }
}

fn file_meta_at(rel: &str, meta: &std::fs::Metadata) -> FileMeta {
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    FileMeta {
        path: rel.to_string(),
        size: meta.len(),
        mtime,
        etag: etag_from_size_mtime(meta.len(), mtime),
    }
}

#[async_trait]
impl StorageBackend for FsStorage {
    async fn file_meta(&self, path: &str) -> Result<Option<FileMeta>, StorageError> {
        let full = self.resolve(path)?;
        let meta = match tokio::fs::metadata(&full).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(StorageError::Other(e)),
        };
        if meta.is_dir() {
            return Err(StorageError::Unsupported("path is a directory"));
        }
        Ok(Some(file_meta_at(path, &meta)))
    }

    async fn read_stream(
        &self,
        path: &str,
        range: RangeSpec,
    ) -> Result<Box<dyn Read + Send>, StorageError> {
        let full = self.resolve(path)?;
        let mut file = tokio::fs::File::open(&full)
            .await
            .map_err(|e| StorageError::Other(e))?;
        if range.start > 0 {
            tokio::io::AsyncSeekExt::seek(&mut file, SeekFrom::Start(range.start))
                .await
                .map_err(|e| StorageError::Other(e))?;
        }
        let std_file = file
            .try_into_std()
            .map_err(|e| StorageError::Other(std::io::Error::other(format!("{e:?}"))))?;
        // Restrict to exactly the requested range.
        let limited = std_file.take(range.len());
        Ok(Box::new(limited))
    }

    async fn write_stream(
        &self,
        path: &str,
        mode: WriteMode,
    ) -> Result<Box<dyn UploadSink>, StorageError> {
        let full = self.resolve(path)?;
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Other(e))?;
        }

        match mode {
            WriteMode::Create | WriteMode::Overwrite => {
                if mode == WriteMode::Create
                    && tokio::fs::try_exists(&full)
                        .await
                        .map_err(|e| StorageError::Other(e))?
                {
                    return Err(StorageError::AlreadyExists(path.to_string()));
                }
                // Write to a temp file, rename on commit.
                let tmp = temp_path_for(&full);
                let file = tokio::fs::File::create(&tmp)
                    .await
                    .map_err(|e| StorageError::Other(e))?;
                Ok(Box::new(FsSink {
                    file,
                    tmp: Some(tmp),
                    target: full,
                    rel: path.to_string(),
                    mode,
                    written: 0,
                    blocks_path: None,
                    ranges: Vec::new(),
                    sidecar_lock: self.sidecar_lock.clone(),
                }))
            }
            WriteMode::Resume { offset } => {
                let file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(&full)
                    .await
                    .map_err(|e| StorageError::Other(e))?;
                let current = file
                    .metadata()
                    .await
                    .map_err(|e| StorageError::Other(e))?
                    .len();
                if current != offset {
                    return Err(StorageError::write_failed(
                        offset,
                        std::io::Error::other(format!(
                            "existing file is {current} bytes, expected {offset}"
                        )),
                    ));
                }
                Ok(Box::new(FsSink {
                    file,
                    tmp: None,
                    target: full,
                    rel: path.to_string(),
                    mode,
                    written: offset,
                    blocks_path: None,
                    ranges: Vec::new(),
                    sidecar_lock: self.sidecar_lock.clone(),
                }))
            }
        }
    }

    async fn write_stream_session(
        &self,
        path: &str,
        session: &str,
        mode: WriteMode,
    ) -> Result<Box<dyn UploadSink>, StorageError> {
        let full = self.resolve(path)?;
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Other(e))?;
        }
        // The session string is embedded in a temp filename, so it must never
        // be able to inject path separators or `..` (a malicious client could
        // otherwise write outside the mount root). Restrict to safe chars.
        let safe: String = session
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let name = full
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "upload".to_string());
        let tmp = full.with_file_name(format!(".libfw-sess-{safe}-{name}"));

        let exists = tokio::fs::try_exists(&tmp)
            .await
            .map_err(|e| StorageError::Other(e))?;
        let file = if exists {
            // Subsequent chunk / resume of an in-flight session: open the
            // shared temp for positional (seek + write) access. `mode` is
            // ignored; the already-received ranges are reloaded from the
            // sidecar so the client can resume only the missing parts.
            tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&tmp)
                .await
                .map_err(|e| StorageError::Other(e))?
        } else {
            // First request for this session: create the shared temp.
            match mode {
                WriteMode::Create | WriteMode::Overwrite => {
                    if mode == WriteMode::Create
                        && tokio::fs::try_exists(&full)
                            .await
                            .map_err(|e| StorageError::Other(e))?
                    {
                        return Err(StorageError::AlreadyExists(path.to_string()));
                    }
                    tokio::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&tmp)
                        .await
                        .map_err(|e| StorageError::Other(e))?
                }
                // Resumable sessions are driven by the per-block probe (the
                // client asks "what ranges do you have?" and sends only the
                // gaps), not by a contiguous offset append, so a legacy
                // `Resume` mode is not applicable here.
                WriteMode::Resume { .. } => {
                    return Err(StorageError::Unsupported(
                        "session upload does not support contiguous resume; use the block probe".into(),
                    ))
                }
            }
        };
        // Load any already-received byte ranges from the sidecar so a pause /
        // resume only re-sends the missing blocks.
        let blocks_path = blocks_path_for(&tmp);
        // A freshly-created temp means nothing has been received yet: a stale
        // `.blocks` sidecar left behind by an earlier aborted attempt must not
        // make the probe report phantom ranges (the client would then skip
        // chunks and commit an empty / zero-filled file).
        if !exists {
            let _ = tokio::fs::remove_file(&blocks_path).await;
        }
        let ranges = if exists {
            read_ranges(&blocks_path).await
        } else {
            Vec::new()
        };
        Ok(Box::new(FsSink {
            file,
            tmp: Some(tmp),
            target: full,
            rel: path.to_string(),
            mode,
            written: 0,
            blocks_path: Some(blocks_path),
            ranges,
            sidecar_lock: self.sidecar_lock.clone(),
        }))
    }

    async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, StorageError> {
        let full = if path.is_empty() {
            self.root.clone()
        } else {
            self.resolve(path)?
        };
        let mut entries = Vec::new();
        let mut read_dir = tokio::fs::read_dir(&full)
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::NotFound(path.to_string())
                } else {
                    StorageError::Other(e)
                }
            })?;
        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| StorageError::Other(e))?
        {
            let name = entry.file_name().to_string_lossy().to_string();
            let rel = if path.is_empty() {
                name.clone()
            } else {
                format!("{path}/{name}")
            };
            let meta = entry
                .metadata()
                .await
                .map_err(|e| StorageError::Other(e))?;
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            entries.push(DirEntry {
                path: rel,
                is_dir: meta.is_dir(),
                size: if meta.is_dir() { 0 } else { meta.len() },
                mtime,
            });
        }
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(entries)
    }

    async fn mkdir_all(&self, path: &str) -> Result<(), StorageError> {
        let full = self.resolve(path)?;
        tokio::fs::create_dir_all(&full)
            .await
            .map_err(|e| StorageError::Other(e))?;
        Ok(())
    }

    async fn remove(&self, path: &str) -> Result<(), StorageError> {
        let full = self.resolve(path)?;
        // Use `symlink_metadata` (not `metadata`) so a symlink that appeared
        // since `resolve` checked is never followed — we refuse to remove
        // *through* it. (The check-then-use race cannot be fully closed on
        // all platforms without openat/O_NOFOLLOW; this narrows it for the
        // destructive operation.)
        let meta = tokio::fs::symlink_metadata(&full)
            .await
            .map_err(|e| StorageError::Other(e))?;
        if meta.file_type().is_symlink() {
            return Err(StorageError::Unsupported(
                "refusing to remove through a symlink",
            ));
        }
        if meta.is_dir() {
            tokio::fs::remove_dir_all(&full)
                .await
                .map_err(|e| StorageError::Other(e))
        } else {
            tokio::fs::remove_file(&full)
                .await
                .map_err(|e| StorageError::Other(e))
        }
    }

    async fn cleanup_stale_sessions(
        &self,
        max_age: std::time::Duration,
    ) -> Result<usize, StorageError> {
        // tus-style expiry: a client that vanishes mid-upload leaves its
        // `.libfw-sess-<id>-<name>` temp (and `.blocks` sidecar) behind. Walk
        // the root, removing the ones whose last write is older than
        // `max_age`. Only session temps are touched — never committed user
        // files — and symlinked directories are never followed.
        let deadline = SystemTime::now()
            .checked_sub(max_age)
            .unwrap_or(UNIX_EPOCH);
        let mut removed = 0usize;
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let mut rd = match tokio::fs::read_dir(&dir).await {
                Ok(rd) => rd,
                Err(_) => continue,
            };
            while let Ok(Some(entry)) = rd.next_entry().await {
                let ft = match entry.file_type().await {
                    Ok(ft) => ft,
                    Err(_) => continue,
                };
                if ft.is_dir() {
                    if !ft.is_symlink() {
                        stack.push(entry.path());
                    }
                    continue;
                }
                if ft.is_symlink() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if !name.starts_with(".libfw-sess-") {
                    continue;
                }
                let modified = entry
                    .metadata()
                    .await
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| UNIX_EPOCH + d)
                    .unwrap_or(UNIX_EPOCH);
                if modified < deadline {
                    let _ = tokio::fs::remove_file(entry.path()).await;
                    // Remove the parallel range sidecar (`<temp>.blocks`).
                    let mut sidecar = entry.path().as_os_str().to_owned();
                    sidecar.push(".blocks");
                    let _ = tokio::fs::remove_file(PathBuf::from(sidecar)).await;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }
}

/// A temporary path for a target, unique per attempt.
///
/// Uniqueness combines a monotonic counter with a timestamp so two
/// concurrent Create-mode uploads to the same target never collide on the
/// same temp file (which would otherwise interleave/truncate each other).
static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn temp_path_for(target: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let counter = TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let file_name = target
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "upload".to_string());
    let tmp_name = format!(".libfw-tmp-{file_name}-{nanos}-{counter}");
    target.with_file_name(tmp_name)
}

/// Streaming write handle for the filesystem backend.
pub struct FsSink {
    file: tokio::fs::File,
    tmp: Option<PathBuf>,
    target: PathBuf,
    /// The full virtual path (used for the committed `FileMeta`).
    rel: String,
    /// The mode the sink was opened with (used for the Create atomicity
    /// check at commit time).
    mode: WriteMode,
    written: u64,
    /// Optional sidecar path tracking received byte ranges for a resumable
    /// "session" upload (parallel to the session temp file). `None` for
    /// ordinary (non-session) sinks.
    blocks_path: Option<PathBuf>,
    /// In-memory copy of the received ranges (kept in sync with
    /// `blocks_path`).
    ranges: Vec<ChunkRange>,
    /// Shared lock serializing sidecar read-merge-write (see `FsStorage`).
    sidecar_lock: Arc<tokio::sync::Mutex<()>>,
}

/// Merge `new` (a `[start, end)` half-open range) into a sorted, disjoint
/// list of ranges, coalescing overlaps/adjacencies. Returns the updated list.
fn merge_range(ranges: &mut Vec<ChunkRange>, new: ChunkRange) {
    if new.is_empty() {
        return;
    }
    ranges.push(new);
    ranges.sort_by_key(|r| r.start);
    let mut merged: Vec<ChunkRange> = Vec::with_capacity(ranges.len());
    for r in ranges.drain(..) {
        if let Some(last) = merged.last_mut() {
            // Overlapping or adjacent ranges coalesce.
            if r.start <= last.end {
                if r.end > last.end {
                    last.end = r.end;
                }
                continue;
            }
        }
        merged.push(r);
    }
    *ranges = merged;
}

#[async_trait]
impl UploadSink for FsSink {
    async fn write(&mut self, buf: &[u8]) -> Result<(), StorageError> {
        use tokio::io::AsyncWriteExt;
        self.file
            .write_all(buf)
            .await
            .map_err(|e| StorageError::write_failed(self.written, e))?;
        self.written += buf.len() as u64;
        Ok(())
    }

    async fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), StorageError> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        self.file
            .seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| StorageError::write_failed(offset, e))?;
        self.file
            .write_all(buf)
            .await
            .map_err(|e| StorageError::write_failed(offset, e))?;
        // Track the highest extent written (used by `len`-style bookkeeping;
        // the real size for commit validation comes from file metadata).
        self.written = self.written.max(offset.saturating_add(buf.len() as u64));
        // For a resumable session sink, record the received byte range in the
        // sidecar so a later probe / resume knows this part is already on
        // disk and only missing gaps need to be re-sent.
        //
        // The read-merge-write is serialized under the storage-wide lock:
        // concurrent chunk requests each hold a *stale* in-memory `ranges`
        // copy loaded when their sink was opened, so a bare `persist_ranges`
        // would clobber other chunks' ranges (lost update) and make the
        // commit coverage check reject a fully-written file.
        if let Some(blocks) = self.blocks_path.clone() {
            let guard = self.sidecar_lock.lock().await;
            let mut current = read_ranges(&blocks).await;
            merge_range(&mut current, ChunkRange {
                start: offset,
                end: offset.saturating_add(buf.len() as u64),
            });
            persist_ranges(&blocks, &current).await?;
            self.ranges = current;
            drop(guard);
        }
        Ok(())
    }

    async fn received_ranges(&mut self) -> Result<Vec<ChunkRange>, StorageError> {
        Ok(self.ranges.clone())
    }

    async fn len(&self) -> Result<u64, StorageError> {
        self.file
            .metadata()
            .await
            .map(|m| m.len())
            .map_err(|e| StorageError::Other(e))
    }

    async fn commit(self: Box<Self>) -> Result<FileMeta, StorageError> {
        use tokio::io::AsyncWriteExt;
        let FsSink {
            mut file,
            tmp,
            target,
            rel,
            mode,
            blocks_path,
            ..
        } = *self;
        let _ = remove_blocks_sidecar(blocks_path.as_deref()).await;
        file.flush().await.map_err(|e| StorageError::Other(e))?;
        file.sync_all().await.map_err(|e| StorageError::Other(e))?;
        drop(file);
        if let Some(tmp) = tmp {
            // Create mode must never clobber a target that appeared while
            // we were streaming (closes the check-then-rename TOCTOU for
            // the common non-concurrent-writer case).
            if matches!(mode, WriteMode::Create)
                && tokio::fs::try_exists(&target)
                    .await
                    .map_err(|e| StorageError::Other(e))?
            {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err(StorageError::AlreadyExists(rel));
            }
            tokio::fs::rename(&tmp, &target)
                .await
                .map_err(|e| StorageError::Other(e))?;
            // Durability: fsync the parent directory so the rename itself
            // survives a crash.
            if let Some(parent) = target.parent() {
                if let Ok(d) = tokio::fs::File::open(parent).await {
                    let _ = d.sync_all().await;
                }
            }
        }
        let meta = tokio::fs::metadata(&target)
            .await
            .map_err(|e| StorageError::Other(e))?;
        Ok(file_meta_at(&rel, &meta))
    }

    async fn abort(self: Box<Self>) -> Result<(), StorageError> {
        let FsSink {
            file,
            tmp,
            blocks_path,
            ..
        } = *self;
        drop(file);
        if let Some(tmp) = tmp {
            let _ = tokio::fs::remove_file(&tmp).await;
        }
        let _ = remove_blocks_sidecar(blocks_path.as_deref()).await;
        Ok(())
    }
}

/// Sidecar filename for a session temp (e.g. `<temp>.blocks`).
fn blocks_path_for(tmp: &Path) -> PathBuf {
    let mut name = tmp.as_os_str().to_owned();
    name.push(".blocks");
    PathBuf::from(name)
}

/// Read the persisted received byte ranges for a session temp sidecar.
async fn read_ranges(blocks: &Path) -> Vec<ChunkRange> {
    match tokio::fs::read_to_string(blocks).await {
        Ok(text) => serde_json::from_str::<Vec<ChunkRange>>(&text).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Persist the received byte ranges to a session temp sidecar (best-effort).
async fn persist_ranges(blocks: &Path, ranges: &[ChunkRange]) -> Result<(), StorageError> {
    let text = serde_json::to_string(ranges).unwrap_or_else(|_| "[]".to_string());
    tokio::fs::write(blocks, text)
        .await
        .map_err(|e| StorageError::Other(e))
}

/// Remove a session temp sidecar (best-effort; missing file is fine).
async fn remove_blocks_sidecar(blocks: Option<&Path>) -> Result<(), StorageError> {
    if let Some(blocks) = blocks {
        let _ = tokio::fs::remove_file(blocks).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use libfw_core::storage::StorageBackend;

    /// Force a file's mtime (used to age a fake session temp).
    fn filetime_set(path: &Path, time: std::time::SystemTime) -> std::io::Result<()> {
        // Open writable: on Windows, setting file times through a read-only
        // handle is refused.
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        file.set_times(std::fs::FileTimes::new().set_modified(time))
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());

        let sink = storage
            .write_stream("a/b.txt", WriteMode::Create)
            .await
            .unwrap();
        let mut sink = sink;
        sink.write(b"hello ").await.unwrap();
        sink.write(b"world").await.unwrap();
        let meta = sink.commit().await.unwrap();
        assert_eq!(meta.size, 11);

        let got = storage.file_meta("a/b.txt").await.unwrap().unwrap();
        assert_eq!(got.size, 11);

        let mut reader = storage
            .read_stream("a/b.txt", RangeSpec::new(0, 11))
            .await
            .unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"hello world");
    }

    #[tokio::test]
    async fn create_rejects_existing() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        let sink = storage.write_stream("f.txt", WriteMode::Create).await.unwrap();
        let mut sink = sink;
        sink.write(b"x").await.unwrap();
        sink.commit().await.unwrap();

        let res = storage.write_stream("f.txt", WriteMode::Create).await;
        assert!(matches!(res, Err(StorageError::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn resume_writes_at_offset() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());

        let sink = storage.write_stream("f.txt", WriteMode::Create).await.unwrap();
        let mut sink = sink;
        sink.write(b"ABCD").await.unwrap();
        sink.commit().await.unwrap();

        let mut sink = storage
            .write_stream("f.txt", WriteMode::Resume { offset: 4 })
            .await
            .unwrap();
        sink.write(b"EF").await.unwrap();
        sink.commit().await.unwrap();

        let mut reader = storage
            .read_stream("f.txt", RangeSpec::full(6))
            .await
            .unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"ABCDEF");
    }

    #[tokio::test]
    async fn resume_offset_mismatch_fails() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        let sink = storage.write_stream("f.txt", WriteMode::Create).await.unwrap();
        let mut sink = sink;
        sink.write(b"AB").await.unwrap();
        sink.commit().await.unwrap();

        let res = storage.write_stream("f.txt", WriteMode::Resume { offset: 9 }).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn abort_leaves_no_partial_file() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        let sink = storage.write_stream("f.txt", WriteMode::Create).await.unwrap();
        let mut sink = sink;
        sink.write(b"partial").await.unwrap();
        sink.abort().await.unwrap();
        assert!(!dir.path().join("f.txt").exists());
    }

    #[tokio::test]
    async fn list_dir_and_remove() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/x.txt"), b"1").unwrap();
        std::fs::write(dir.path().join("a.txt"), b"22").unwrap();

        let entries = storage.list_dir("").await.unwrap();
        assert_eq!(entries.len(), 2);
        let names: Vec<_> = entries.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(names, vec!["a.txt", "sub"]);

        storage.remove("sub").await.unwrap();
        assert!(!dir.path().join("sub").exists());
    }

    #[tokio::test]
    async fn rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        assert!(storage.file_meta("../etc/passwd").await.is_err());
        assert!(storage.file_meta("/etc/passwd").await.is_err());
    }

    #[tokio::test]
    async fn cleanup_stale_sessions_removes_old_temps_only() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());

        // A stale session temp (very old mtime) + its range sidecar.
        let stale = dir.path().join(".libfw-sess-oldid-a.bin");
        std::fs::write(&stale, b"partial").unwrap();
        std::fs::write(dir.path().join(".libfw-sess-oldid-a.bin.blocks"), b"[[0,7]]").unwrap();
        // Age it well beyond the 1-hour TTL (7 days old). Windows/FAT can
        // clamp very old timestamps, so use a recent-but-stale date.
        let old = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(7 * 24 * 3600))
            .unwrap();
        assert!(
            filetime_set(&stale, old).is_ok(),
            "failed to age the stale session temp"
        );

        // A fresh session temp (now) must be kept.
        let fresh = dir.path().join(".libfw-sess-newid-a.bin");
        std::fs::write(&fresh, b"partial").unwrap();

        // A committed user file must never be touched.
        std::fs::write(dir.path().join("real.txt"), b"real").unwrap();

        let removed = storage
            .cleanup_stale_sessions(std::time::Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(removed, 1, "only the stale temp is removed");
        assert!(!stale.exists());
        assert!(!dir.path().join(".libfw-sess-oldid-a.bin.blocks").exists());
        assert!(fresh.exists(), "fresh temp survives");
        assert!(dir.path().join("real.txt").exists(), "user file survives");
    }

    #[tokio::test]
    async fn committed_meta_has_full_relative_path() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        let sink = storage
            .write_stream("deep/nested/f.txt", WriteMode::Create)
            .await
            .unwrap();
        let mut sink = sink;
        sink.write(b"hi").await.unwrap();
        let meta = sink.commit().await.unwrap();
        assert_eq!(meta.path, "deep/nested/f.txt");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlink_traversal() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());

        // A symlink planted inside the root pointing outside it must not let
        // reads/writes escape the mount root.
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret.txt"), b"top-secret").unwrap();
        symlink(outside.path(), dir.path().join("evil")).unwrap();

        assert!(storage.read_stream("evil/secret.txt", RangeSpec::full(10)).await.is_err());
        assert!(storage.file_meta("evil/secret.txt").await.is_err());
        assert!(storage.write_stream("evil/new.txt", WriteMode::Create).await.is_err());
    }
}
