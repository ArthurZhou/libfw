//! Local filesystem storage backend for `libfw-server`.
//!
//! Writes go to a temporary file next to the destination and are
//! atomically renamed into place on [`UploadSink::commit`], so a failed or
//! aborted upload never leaves a partial target behind.

use std::io::{Read, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use libfw_core::metadata::{etag_from_size_mtime, FileMeta};
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
}

impl FsStorage {
    /// Create a backend serving files under `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        FsStorage { root: root.into() }
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
                }))
            }
        }
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
        let meta = tokio::fs::metadata(&full)
            .await
            .map_err(|e| StorageError::Other(e))?;
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

    async fn commit(self: Box<Self>) -> Result<FileMeta, StorageError> {
        use tokio::io::AsyncWriteExt;
        let FsSink {
            mut file,
            tmp,
            target,
            rel,
            mode,
            ..
        } = *self;
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
        let FsSink { file, tmp, .. } = *self;
        drop(file);
        if let Some(tmp) = tmp {
            let _ = tokio::fs::remove_file(&tmp).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libfw_core::storage::StorageBackend;

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
