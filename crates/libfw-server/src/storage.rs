//! Local filesystem storage backend for `libfw-server`.
//!
//! Writes go to a temporary file and are atomically renamed into place on
//! [`UploadSink::commit`], so a failed or aborted upload never leaves a partial
//! target behind. The temp lives next to the destination by default and can be
//! moved to a dedicated directory with [`FsStorage::with_temp_dir`].
//!
//! Everything the backend creates for its own bookkeeping is named with the
//! reserved `.libfw-` prefix (`.libfw-sess-*` session temps, `.libfw-tmp-*`
//! one-shot temps, `.blocks` sidecars and their `.blocks.tmp` staging files).
//! Those names are rejected by [`FsStorage::resolve`] and hidden from
//! [`StorageBackend::list_dir`], so an in-flight upload never shows up in a
//! directory listing (or in a recursive folder download) as a phantom file.

use std::collections::HashMap;
use std::io::{Read, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::OwnedMutexGuard;

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
    /// Directory holding in-flight (uncommitted) upload temps.
    ///
    /// `None` (the default) keeps them next to their target — a same-directory
    /// rename is the cheapest atomic commit. `Some(dir)` moves every temp out
    /// of the served tree (see [`FsStorage::with_temp_dir`]), which keeps a
    /// directory listing, a folder download or an rsync of the root free of
    /// half-written uploads even if something goes wrong with filtering.
    temp_dir: Option<PathBuf>,
    /// Per-target locks (keyed by temp/target path). Concurrent chunk
    /// requests for *different* sessions/targets never serialize behind each
    /// other — only writers of the same target (who share a `.blocks`
    /// sidecar or an append-only resume file) take turns. Without this, a
    /// read-merge-write of the shared sidecar loses updates (making the
    /// commit coverage check reject a fully-written file).
    locks: Arc<std::sync::Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>>,
    /// Shared receive state per session temp.
    ///
    /// One entry per temp that has an open (or recently open) session sink.
    /// Every concurrent chunk request of the same session gets the *same*
    /// [`SessionState`], which is what lets those requests run in parallel
    /// while still agreeing on the received-range set: the ranges live here,
    /// behind a short critical section, instead of being loaded into each
    /// sink at open time.
    sessions: SessionRegistry,
}

/// The session registry type (also handed to [`FsSink`] so a finishing sink
/// can retire its own entry).
type SessionRegistry = Arc<std::sync::Mutex<HashMap<PathBuf, Arc<SessionState>>>>;

/// Receive bookkeeping shared by every concurrent request of one session.
///
/// Held behind an `Arc` in [`FsStorage::sessions`], so all chunk requests for a
/// temp see one authoritative range list.
pub(crate) struct SessionState {    /// The ranges received so far, guarded by a short-lived async mutex.
    ///
    /// The lock is taken only to merge a range and persist the sidecar —
    /// never for the duration of a request's body write, which is what used to
    /// serialize a whole upload to one block at a time.
    ranges: tokio::sync::Mutex<SessionRanges>,
}

/// The mutable half of [`SessionState`]: the range list plus its sidecar path.
struct SessionRanges {
    blocks_path: Option<PathBuf>,
    ranges: Vec<ChunkRange>,
}

impl std::fmt::Debug for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionState").finish_non_exhaustive()
    }
}

impl SessionState {
    /// Record `range` as received and persist the sidecar, in one short
    /// critical section.
    async fn record(&self, range: ChunkRange) -> Result<(), StorageError> {
        let mut guard = self.ranges.lock().await;
        merge_range(&mut guard.ranges, range);
        if let Some(blocks) = guard.blocks_path.clone() {
            persist_ranges(&blocks, &guard.ranges).await?;
        }
        Ok(())
    }

    /// A snapshot of the received ranges.
    async fn snapshot(&self) -> Vec<ChunkRange> {
        self.ranges.lock().await.ranges.clone()
    }

    /// The sidecar path this session persists to (`None` once retired).
    async fn blocks_path(&self) -> Option<PathBuf> {
        self.ranges.lock().await.blocks_path.clone()
    }
}

impl FsStorage {
    /// Create a backend serving files under `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        FsStorage {
            root: root.into(),
            temp_dir: None,
            locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Keep in-flight upload temps in `dir` instead of next to their target.
    ///
    /// Useful when the storage root is itself served by another process
    /// (rsync, `ls`, a media scanner) or lives on a filesystem where dotfiles
    /// are not hidden: nothing half-written ever appears inside it. Temps are
    /// namespaced by the SHA-free [`path_tag`] of the virtual path, so two
    /// same-named targets in different directories never collide.
    ///
    /// When `dir` is on a different filesystem than the root, `commit` falls
    /// back to "copy into a hidden sibling, then rename" — the target is still
    /// replaced atomically, at the cost of one extra copy of the file.
    ///
    /// The directory is created on demand; it must not be one of the served
    /// directories unless it is hidden (any path starting with `.libfw` is
    /// hidden automatically).
    pub fn with_temp_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.temp_dir = Some(dir.into());
        self
    }

    /// The configured temp directory, if any.
    pub fn temp_dir(&self) -> Option<&Path> {
        self.temp_dir.as_deref()
    }

    /// Directory that holds the temps of `target`.
    fn scratch_dir(&self, target: &Path) -> PathBuf {
        match &self.temp_dir {
            Some(dir) => dir.clone(),
            None => target
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| self.root.clone()),
        }
    }

    /// Unique temp path for a one-shot (non-session) upload of `target`.
    fn new_temp_path(&self, target: &Path, rel: &str) -> PathBuf {
        match &self.temp_dir {
            None => temp_path_for(target),
            Some(dir) => dir.join(format!(
                ".libfw-tmp-{}-{}-{}",
                path_tag(rel),
                file_name_of(target),
                unique_suffix()
            )),
        }
    }

    /// Deterministic temp path for a resumable session writing `target`.
    ///
    /// Every chunk of one session must find the *same* temp file, so this name
    /// is derived only from data the client echoes back on each request
    /// (`session`, `owner`, `rel`).
    fn session_temp_path(&self, target: &Path, rel: &str, owner: &str, session: &str) -> PathBuf {
        match &self.temp_dir {
            None => target.with_file_name(format!(
                ".libfw-sess-{owner}-{session}-{}",
                file_name_of(target)
            )),
            Some(dir) => dir.join(format!(
                ".libfw-sess-{owner}-{session}-{}-{}",
                path_tag(rel),
                file_name_of(target)
            )),
        }
    }

    /// Get (or create) the mutex guarding `path`.
    fn lock_for(&self, path: &Path) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.locks.lock().expect("lock registry poisoned");
        // Drop entries no sink holds anymore so the registry cannot grow
        // without bound across many short-lived sessions.
        if map.len() > 1024 {
            map.retain(|_, v| Arc::strong_count(v) > 1);
        }
        map.entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Resolve a virtual path against the root (pure text — no I/O).
    ///
    /// Rejects absolute paths, `..` segments and `CurDir` (`.`) other than
    /// the root-relative segments accepted by `Component::Normal`. Symlink
    /// checking is async and is done by the callers via
    /// [`FsStorage::check_symlinks`] when they need it (file/dir open paths
    /// that may traverse existing directory structure).
    fn resolve(&self, path: &str) -> Result<PathBuf, StorageError> {
        let rel = Path::new(path);
        if rel.is_absolute() {
            return Err(StorageError::Unsupported("absolute paths are not allowed"));
        }
        let mut joined = self.root.clone();
        for component in rel.components() {
            match component {
                Component::Normal(seg) => {
                    // The `.libfw-` namespace is the backend's own scratch
                    // space: hiding it from listings is not enough, a client
                    // must not be able to stat, download or delete an
                    // in-flight temp from inside the served tree.
                    if is_reserved_name(&seg.to_string_lossy()) {
                        return Err(StorageError::Unsupported(
                            "path uses the reserved `.libfw-` prefix",
                        ));
                    }
                    joined.push(seg);
                }
                Component::CurDir => {}
                _ => {
                    return Err(StorageError::Unsupported(
                        "path must not contain '..' or special components",
                    ))
                }
            }
        }
        // A user-configured temp directory may sit inside the root; it is
        // scratch space whatever it is called.
        if let Some(dir) = &self.temp_dir {
            if joined.starts_with(dir) {
                return Err(StorageError::Unsupported(
                    "path is inside the configured temporary directory",
                ));
            }
        }
        Ok(joined)
    }

    /// Walk every component of `full` between `self.root` and the leaf,
    /// rejecting any that is a symlink.
    ///
    /// Called asynchronously by the I/O methods that open existing paths so
    /// the symlink check never blocks a tokio worker thread. New (not yet
    /// existing) path tails are skipped — they cannot be symlinks.
    async fn check_symlinks(&self, full: &Path) -> Result<(), StorageError> {
        // Strip the root prefix to iterate only the virtual path components.
        let rel = match full.strip_prefix(&self.root) {
            Ok(r) => r,
            Err(_) => return Ok(()), // shouldn't happen; treat as safe
        };
        let mut probe = self.root.clone();
        for component in rel.components() {
            probe.push(component);
            match tokio::fs::symlink_metadata(&probe).await {
                Ok(m) if m.file_type().is_symlink() => {
                    return Err(StorageError::Unsupported(
                        "path must not traverse a symlink",
                    ))
                }
                Ok(_) => {}
                // Not-yet-existing component: fine (new upload target).
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(StorageError::Other(e)),
            }
        }
        Ok(())
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
        self.check_symlinks(&full).await?;
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
        self.check_symlinks(&full).await?;
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
        self.check_symlinks(&full).await?;
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
                tokio::fs::create_dir_all(self.scratch_dir(&full))
                    .await
                    .map_err(|e| StorageError::Other(e))?;
                let tmp = self.new_temp_path(&full, path);
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
                    shared: None,
                    target_guard: None,
                    registry: self.sessions.clone(),
                }))
            }
            WriteMode::Resume { offset } => {
                // Concurrent `Resume` writers of the same target would both
                // observe the same on-disk length and interleave their
                // appends. Hold the per-target lock for the sink's whole
                // lifetime so they take turns (the loser re-checks the
                // length only after the winner's commit/rejection).
                let target_guard = Some(self.lock_for(&full).lock_owned().await);
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
                    shared: None,
                    target_guard,
                    registry: self.sessions.clone(),
                }))
            }
        }
    }

    async fn write_stream_session(
        &self,
        path: &str,
        session: &str,
        owner: &str,
        mode: WriteMode,
    ) -> Result<Box<dyn UploadSink>, StorageError> {
        let full = self.resolve(path)?;
        self.check_symlinks(&full).await?;
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Other(e))?;
        }
        // Both the session id and owner tag are embedded in the temp filename,
        // so neither must inject path separators or `..`. Restrict to safe
        // chars (alphanumeric, `-`, `_`).
        let sanitize = |s: &str| -> String {
            s.chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect()
        };
        let safe_session = sanitize(session);
        // Embed the owner tag so that two authenticated users with the same
        // file path and an identical (guessed/observed) session id cannot
        // collide on the same temp file.  A short prefix of the owner hash
        // is enough for discrimination without making filenames unwieldy.
        let safe_owner = sanitize(&owner.chars().take(16).collect::<String>());
        tokio::fs::create_dir_all(self.scratch_dir(&full))
            .await
            .map_err(|e| StorageError::Other(e))?;
        let tmp = self.session_temp_path(&full, path, &safe_owner, &safe_session);

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
                // gaps), not by a contiguous offset append, so a
                // `WriteMode::Resume` request has no meaning here.
                WriteMode::Resume { .. } => {
                    return Err(StorageError::Unsupported(
                        "session upload does not support contiguous resume; use the block probe".into(),
                    ))
                }
            }
        };
        // Chunks of the same session share the temp and its sidecar. They no
        // longer serialize: every request gets the *same* [`SessionState`]
        // (found or created under the short per-target lock, which is released
        // before the body is written), and that shared state — not a
        // request-lifetime guard — keeps the received-range set coherent.
        //
        // Holding the per-target guard for the sink's whole lifetime (the
        // previous behaviour) made every concurrent block of one file queue
        // behind the others, so the client's in-flight window had no effect at
        // all: measured on a 24 MB/s + 300 ms link, 8 concurrent 2 MiB blocks
        // were delivered strictly one after another, 470 ms each.
        let blocks_path = blocks_path_for(&tmp);
        let session_state = {
            // The per-target lock serializes *creation* of a session's state
            // (two concurrent first requests must not each load the sidecar and
            // then overwrite the other's entry). It is released before the body
            // of this request is written, which is what keeps chunk writes
            // parallel.
            let _guard = self.lock_for(&tmp).lock_owned().await;
            // Fast path: an existing entry, found without any await.
            let existing = {
                let map = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
                map.get(&tmp).cloned()
            };
            match existing {
                Some(state) => state,
                None => {
                    // First request of this session: load whatever an earlier
                    // (interrupted) attempt left behind.
                    //
                    // A freshly-created temp means nothing has been received
                    // yet: a stale `.blocks` sidecar from an aborted attempt
                    // must not make the probe report phantom ranges (the client
                    // would then skip chunks and commit a zero-filled file).
                    if !exists {
                        let _ = tokio::fs::remove_file(&blocks_path).await;
                    }
                    let ranges = if exists {
                        read_ranges(&blocks_path).await?
                    } else {
                        Vec::new()
                    };
                    let state = Arc::new(SessionState {
                        ranges: tokio::sync::Mutex::new(SessionRanges {
                            blocks_path: Some(blocks_path),
                            ranges,
                        }),
                    });
                    // Insert only if nobody won the race while we read the
                    // sidecar (the per-target lock makes that impossible, but
                    // re-checking keeps the invariant local and cheap).
                    let mut map = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
                    map.entry(tmp.clone())
                        .or_insert_with(|| state.clone())
                        .clone()
                }
            }
        };
        Ok(Box::new(FsSink {
            file,
            tmp: Some(tmp),
            target: full,
            rel: path.to_string(),
            mode,
            written: 0,
            shared: Some(session_state),
            target_guard: None,
            registry: self.sessions.clone(),
        }))
    }

    async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, StorageError> {
        let full = if path.is_empty() {
            self.root.clone()
        } else {
            self.resolve(path)?
        };
        self.check_symlinks(&full).await?;
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
            // Never expose in-flight uploads: a `.libfw-sess-*` temp, its
            // `.blocks` sidecar or a `.libfw-tmp-*` one-shot temp is internal
            // bookkeeping, not a user file. Listing them made a directory view
            // (and a recursive folder download) show phantom entries whose
            // size changed as the upload progressed.
            if is_reserved_name(&name) {
                continue;
            }
            // A user-configured temp directory may sit inside the root under
            // an ordinary name; it is scratch space either way.
            if let Some(tmp_dir) = &self.temp_dir {
                if entry.path() == *tmp_dir {
                    continue;
                }
            }
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
        self.check_symlinks(&full).await?;
        tokio::fs::create_dir_all(&full)
            .await
            .map_err(|e| StorageError::Other(e))?;
        Ok(())
    }

    async fn remove(&self, path: &str) -> Result<(), StorageError> {
        let full = self.resolve(path)?;
        self.check_symlinks(&full).await?;
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
        // the root — plus the configured temp directory, which may live
        // outside it — removing the ones whose last write is older than
        // `max_age`. Only session temps are touched — never committed user
        // files — and symlinked directories are never followed.
        let deadline = SystemTime::now()
            .checked_sub(max_age)
            .unwrap_or(UNIX_EPOCH);
        let mut removed = 0usize;
        let mut stack = vec![self.root.clone()];
        if let Some(dir) = &self.temp_dir {
            if !dir.starts_with(&self.root) {
                stack.push(dir.clone());
            }
        }
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
                // On Linux/macOS `is_dir()` and `is_symlink()` are mutually
                // exclusive: a symlink to a directory reports
                // `is_symlink()=true`, `is_dir()=false`. The symlink check
                // below therefore rules out every symlinked directory
                // before the walk descends into it, and the two tests can
                // stay independent.
                if ft.is_symlink() {
                    // Never follow symlinks into directories.
                    continue;
                }
                if ft.is_dir() {
                    stack.push(entry.path());
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
                    // A sidecar is itself a `.libfw-sess-*` entry, so the walk
                    // reaches it too: it goes with its temp, and an orphaned
                    // one is still swept on its own pass. Only a temp counts,
                    // keeping the result a number of sessions.
                    let is_sidecar = name.ends_with(".blocks");
                    let mut sidecar = entry.path().as_os_str().to_owned();
                    sidecar.push(".blocks");
                    let _ = tokio::fs::remove_file(PathBuf::from(sidecar)).await;
                    if tokio::fs::remove_file(entry.path()).await.is_ok() && !is_sidecar {
                        removed += 1;
                    }
                }
            }
        }
        Ok(removed)
    }
}

/// Is `name` (one path segment) part of the backend's private scratch space?
///
/// Every temp, sidecar and staging file the backend creates starts with
/// `.libfw-`; the range sidecars (`.blocks`, `.blocks.tmp`) hang off such a
/// temp name, so the prefix check covers them too.
fn is_reserved_name(name: &str) -> bool {
    name.starts_with(".libfw-")
}

/// Run of the mill file name of a target path.
fn file_name_of(target: &Path) -> String {
    target
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "upload".to_string())
}

/// A unique-per-call suffix (timestamp + process-wide counter).
fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let counter = TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{nanos}-{counter}")
}

/// Stable 16-hex tag for a virtual path.
///
/// A shared temp directory holds temps for every directory of the root, so the
/// name alone no longer disambiguates `a/report.zip` from `b/report.zip`.
/// FNV-1a is tiny, dependency-free and — unlike `DefaultHasher` — stable across
/// processes and compiler versions, which the resumable-upload temp name
/// depends on (a resume in a fresh process must compute the same path).
fn path_tag(rel: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in rel.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// A temporary path for a target, unique per attempt, next to the target.
///
/// Uniqueness combines a monotonic counter with a timestamp so two
/// concurrent Create-mode uploads to the same target never collide on the
/// same temp file (which would otherwise interleave/truncate each other).
static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn temp_path_for(target: &Path) -> PathBuf {
    target.with_file_name(format!(
        ".libfw-tmp-{}-{}",
        file_name_of(target),
        unique_suffix()
    ))
}

/// Move `tmp` onto `target`, atomically.
///
/// A plain rename is atomic *within* a filesystem and replaces an existing
/// target in one step. With a temp directory on another device (the point of
/// [`FsStorage::with_temp_dir`] is often "put the scratch space on the fast
/// disk") the rename fails with `EXDEV`; then the bytes are copied into a
/// hidden sibling first and the sibling is renamed into place, so the visible
/// target still flips in one atomic step and is never observed half-written.
async fn move_into_place(tmp: &Path, target: &Path) -> std::io::Result<()> {
    match tokio::fs::rename(tmp, target).await {
        Ok(()) => Ok(()),
        Err(err) if is_cross_device(&err) => {
            let staged = temp_path_for(target);
            if let Err(copy_err) = tokio::fs::copy(tmp, &staged).await {
                let _ = tokio::fs::remove_file(&staged).await;
                return Err(copy_err);
            }
            match tokio::fs::rename(&staged, target).await {
                Ok(()) => {
                    let _ = tokio::fs::remove_file(tmp).await;
                    Ok(())
                }
                Err(rename_err) => {
                    let _ = tokio::fs::remove_file(&staged).await;
                    Err(rename_err)
                }
            }
        }
        Err(err) => Err(err),
    }
}

#[cfg(windows)]
const CROSS_DEVICE_ERRNO: i32 = 17; // ERROR_NOT_SAME_DEVICE
#[cfg(not(windows))]
const CROSS_DEVICE_ERRNO: i32 = 18; // EXDEV

/// Did the rename fail only because the two paths are on different devices?
///
/// Matched on the raw code rather than `ErrorKind::CrossesDevices` to stay
/// within the crate's minimum supported Rust version.
fn is_cross_device(err: &std::io::Error) -> bool {
    err.raw_os_error() == Some(CROSS_DEVICE_ERRNO)
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
    /// Shared receive state for a resumable "session" upload. `None` for
    /// ordinary (non-session) sinks.
    ///
    /// Shared with every other request of the same session, so concurrent
    /// chunk writes stay coherent WITHOUT serializing: each sink only takes
    /// the state's short lock to record its own range.
    shared: Option<Arc<SessionState>>,
    /// Per-target guard held for the sink's whole lifetime. `Some` only for
    /// `WriteMode::Resume`, which serializes concurrent appends to the same
    /// target (they share one file whose length they must agree on). `None`
    /// for session sinks (see `shared`) and for plain Create/Overwrite sinks,
    /// which write to a unique temp and rename on commit.
    ///
    /// Never read: the field exists to keep the guard (and therefore the lock)
    /// alive for the sink's lifetime, so it is released when the sink drops.
    #[allow(dead_code)]
    target_guard: Option<OwnedMutexGuard<()>>,
    /// The storage's session registry, so a finishing sink can retire its
    /// entry (see [`FsSink::forget_session`]).
    registry: SessionRegistry,
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
        // shared session state (which also persists the sidecar) so a later
        // probe / resume knows this part is already on disk and only missing
        // gaps need to be re-sent.
        //
        // The lock covers only the merge + sidecar write, not this request's
        // body: that is what lets concurrent blocks of one file proceed in
        // parallel while still agreeing on one range list.
        if let Some(session) = &self.shared {
            session
                .record(ChunkRange {
                    start: offset,
                    end: offset.saturating_add(buf.len() as u64),
                })
                .await?;
        }
        Ok(())
    }

    async fn received_ranges(&mut self) -> Result<Vec<ChunkRange>, StorageError> {
        match &self.shared {
            // Read from the *shared* state, not a per-sink snapshot: a probe
            // must report every range received by any concurrent request.
            Some(session) => Ok(session.snapshot().await),
            None => Ok(Vec::new()),
        }
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
            shared,
            registry,
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
                FsSink::forget_session(shared, &registry).await;
                return Err(StorageError::AlreadyExists(rel));
            }
            move_into_place(&tmp, &target)
                .await
                .map_err(|e| StorageError::Other(e))?;
            // Durability: fsync the parent directory so the rename itself
            // survives a crash.
            if let Some(parent) = target.parent() {
                if let Ok(d) = tokio::fs::File::open(parent).await {
                    let _ = d.sync_all().await;
                }
            }
            // The temp is gone, so the session's bookkeeping is meaningless:
            // dropping it also removes the sidecar.
            FsSink::forget_session(shared, &registry).await;
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
            shared,
            registry,
            ..
        } = *self;
        drop(file);
        if let Some(tmp) = tmp {
            let _ = tokio::fs::remove_file(&tmp).await;
            // An abort removes the shared temp, so the session cannot be
            // resumed: drop its state and sidecar too. Leaving the entry in
            // `FsStorage::sessions` would both leak memory and let a later
            // request see ranges for a file that no longer exists.
            FsSink::forget_session(shared, &registry).await;
        }
        Ok(())
    }
}

impl FsSink {
    /// Drop a session's shared state (and its sidecar) after the temp is gone.
    ///
    /// The registry entry is removed only if no other request still holds the
    /// state: an in-flight sibling sink must keep reporting the ranges it has
    /// already recorded, and it will call this itself when it finishes. The
    /// sidecar is deleted unconditionally (the temp it describes is gone), so a
    /// stale `.blocks` can never make a later probe report phantom ranges.
    async fn forget_session(state: Option<Arc<SessionState>>, registry: &SessionRegistry) {
        let Some(state) = state else { return };
        if let Some(blocks) = state.blocks_path().await {
            let _ = remove_blocks_sidecar(Some(&blocks)).await;
        }
        let mut map = registry.lock().unwrap_or_else(|e| e.into_inner());
        let path = map
            .iter()
            .find(|(_, v)| Arc::ptr_eq(v, &state))
            .map(|(k, _)| k.clone());
        if let Some(path) = path {
            // 1 = the map's own reference; anything more means a live request,
            // which will retire the entry itself when it finishes.
            if Arc::strong_count(&state) <= 2 {
                map.remove(&path);
            }
        }
    }
}

/// Sidecar filename for a session temp (e.g. `<temp>.blocks`).
fn blocks_path_for(tmp: &Path) -> PathBuf {
    let mut name = tmp.as_os_str().to_owned();
    name.push(".blocks");
    PathBuf::from(name)
}

/// Read the persisted received ranges for a session temp sidecar.
///
/// An invalid or tampered sidecar is a protocol error: the server must refuse
/// to continue instead of silently treating the upload as if it had no
/// received data and accidentally re-sending/overwriting valid chunks.
async fn read_ranges(blocks: &Path) -> Result<Vec<ChunkRange>, StorageError> {
    match tokio::fs::read_to_string(blocks).await {
        Ok(text) => serde_json::from_str::<Vec<ChunkRange>>(&text).map_err(|e| {
            // This message reaches clients as a 500 body, so it must not
            // disclose server-side filesystem paths.
            StorageError::Other(std::io::Error::other(format!(
                "corrupt session state: {e}"
            )))
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(StorageError::Other(e)),
    }
}

/// Persist the received byte ranges to a session temp sidecar.
///
/// Write-then-rename: [`read_ranges`] treats a malformed sidecar as fatal, so
/// a torn write (crash or error mid-update) must never be observable.
/// Same-directory rename is atomic on POSIX and replaces on Windows; the
/// `.tmp` sibling is itself a `.libfw-sess-*` file, so the stale-session
/// sweeper collects any leftovers.
async fn persist_ranges(blocks: &Path, ranges: &[ChunkRange]) -> Result<(), StorageError> {
    let text = serde_json::to_string(ranges).unwrap_or_else(|_| "[]".to_string());
    let mut tmp = blocks.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    tokio::fs::write(&tmp, text)
        .await
        .map_err(StorageError::Other)?;
    tokio::fs::rename(&tmp, blocks)
        .await
        .map_err(StorageError::Other)?;
    Ok(())
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
        let res = file.set_times(std::fs::FileTimes::new().set_modified(time));
        if res.is_ok() {
            return res;
        }
        // `File::set_times` goes through futimens(fd); hardened sandboxes
        // (seccomp filters that only allow the path-based form) reject it with
        // EPERM while utimensat(path) still works. Backdate by path instead so
        // the sweeper tests also run in those environments.
        path_utime(path, time).map_err(|_| res.unwrap_err())
    }

    /// Path-based `utimensat`: the fallback for [`filetime_set`].
    #[cfg(unix)]
    fn path_utime(path: &Path, time: std::time::SystemTime) -> std::io::Result<()> {
        use std::os::unix::ffi::OsStrExt;

        let since_epoch = time
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| std::io::Error::other(e))?;
        let ts = libc::timespec {
            tv_sec: since_epoch.as_secs() as libc::time_t,
            tv_nsec: since_epoch.subsec_nanos() as libc::c_long,
        };
        let times = [ts, ts]; // atime, mtime
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|e| std::io::Error::other(e))?;
        // SAFETY: `c_path` is a valid NUL-terminated path and `times` is a
        // two-element array, which is exactly what utimensat expects.
        let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    /// Non-Unix platforms never need the fallback.
    #[cfg(not(unix))]
    fn path_utime(_path: &Path, _time: std::time::SystemTime) -> std::io::Result<()> {
        Err(std::io::Error::other("no path-based utime on this platform"))
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
    async fn corrupt_session_sidecar_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        let tmp = dir.path().join(".libfw-sess-owner-session-f.txt");
        std::fs::write(&tmp, b"partial").unwrap();
        std::fs::write(tmp.with_extension("txt.blocks"), b"{not-json}").unwrap();

        let res = storage
            .write_stream_session("f.txt", "session", "owner", WriteMode::Create)
            .await;
        let err = match res {
            Ok(_) => panic!("malformed sidecar should not be accepted"),
            Err(e) => e,
        };
        // The message is surfaced to clients as a 500 body — it must not leak
        // the server-side storage root.
        let msg = err.to_string();
        assert!(msg.contains("corrupt session state"), "{msg}");
        assert!(
            !msg.contains(&dir.path().display().to_string()),
            "error leaked the server path: {msg}"
        );
    }

    #[tokio::test]
    async fn session_sidecar_is_written_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        let mut sink = storage
            .write_stream_session("f.txt", "session", "owner", WriteMode::Create)
            .await
            .unwrap();

        sink.write_at(4, b"abcd").await.unwrap();
        assert_eq!(
            sink.received_ranges().await.unwrap(),
            vec![ChunkRange { start: 4, end: 8 }]
        );
        sink.write_at(8, b"ef").await.unwrap();

        // The sidecar always holds complete, parseable JSON…
        let blocks = dir.path().join(".libfw-sess-owner-session-f.txt.blocks");
        let text = std::fs::read_to_string(&blocks).unwrap();
        let ranges: Vec<ChunkRange> = serde_json::from_str(&text).unwrap();
        assert_eq!(ranges, vec![ChunkRange { start: 4, end: 10 }]);
        // …and the write-then-rename leaves no temporary sibling behind.
        assert!(!dir
            .path()
            .join(".libfw-sess-owner-session-f.txt.blocks.tmp")
            .exists());
        sink.abort().await.unwrap();
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
    async fn cleanup_stale_sessions_sweeps_orphaned_sidecars_without_counting_them() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());

        // A sidecar whose temp is already gone (a crash between the two
        // writes): it must still be swept, or it leaks for the process
        // lifetime, but it is not a session in its own right.
        let orphan = dir.path().join(".libfw-sess-goneid-a.bin.blocks");
        std::fs::write(&orphan, b"[[0,7]]").unwrap();
        let old = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(7 * 24 * 3600))
            .unwrap();
        assert!(filetime_set(&orphan, old).is_ok(), "failed to age the orphan");

        // A fresh orphan must survive until it goes stale.
        let fresh_orphan = dir.path().join(".libfw-sess-newid-a.bin.blocks");
        std::fs::write(&fresh_orphan, b"[[0,7]]").unwrap();

        let removed = storage
            .cleanup_stale_sessions(std::time::Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(removed, 0, "an orphaned sidecar is not a session");
        assert!(!orphan.exists(), "the stale orphan is swept");
        assert!(fresh_orphan.exists(), "fresh orphan survives");
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

    /// An in-flight upload must be invisible: no `.libfw-sess-*` temp, no
    /// `.blocks` sidecar and no `.libfw-tmp-*` one-shot temp in a listing.
    #[tokio::test]
    async fn list_dir_hides_in_flight_uploads() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        std::fs::write(dir.path().join("real.txt"), b"done").unwrap();

        // Ordinary (one-shot) upload in flight.
        let mut plain = storage.write_stream("a.txt", WriteMode::Create).await.unwrap();
        plain.write(b"hal").await.unwrap();
        // Resumable session upload in flight (temp + `.blocks` sidecar).
        let mut session = storage
            .write_stream_session("b.txt", "sess-1", "owner-hash", WriteMode::Create)
            .await
            .unwrap();
        session.write_at(0, b"half").await.unwrap();

        let entries = storage.list_dir("").await.unwrap();
        let names: Vec<_> = entries.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(names, vec!["real.txt"], "in-flight uploads must not be listed");

        session.abort().await.unwrap();
        plain.commit().await.unwrap();
        let names: Vec<_> = storage
            .list_dir("")
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.path)
            .collect();
        assert_eq!(names, vec!["a.txt", "real.txt"]);
        // The aborted session left nothing behind either.
        assert!(!dir.path().join("b.txt").exists());
    }

    /// The reserved prefix is refused on direct access, not merely hidden.
    #[tokio::test]
    async fn reserved_prefix_cannot_be_read_or_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path());
        std::fs::write(dir.path().join(".libfw-sess-x-y-secret.txt"), b"s").unwrap();
        std::fs::write(dir.path().join(".libfw-tmp-a-1-2"), b"s").unwrap();

        for path in [".libfw-sess-x-y-secret.txt", ".libfw-tmp-a-1-2"] {
            assert!(
                storage.file_meta(path).await.is_err(),
                "{path} must not be stat-able"
            );
            assert!(
                storage.read_stream(path, RangeSpec::full(1)).await.is_err(),
                "{path} must not be downloadable"
            );
            assert!(
                storage.remove(path).await.is_err(),
                "{path} must not be removable through the API"
            );
        }
        // Traversal into such a directory is refused too.
        assert!(storage.list_dir(".libfw-sess-x").await.is_err());
    }

    /// With a temp directory the root stays clean for the whole upload, and
    /// the commit still lands atomically.
    #[tokio::test]
    async fn temp_dir_keeps_the_root_clean() {
        let root = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path()).with_temp_dir(scratch.path());
        std::fs::write(root.path().join("keep.txt"), b"keep").unwrap();

        let mut session = storage
            .write_stream_session("deep/dir/big.bin", "sess-9", "owner-hash", WriteMode::Create)
            .await
            .unwrap();
        session.write_at(0, b"0123456789").await.unwrap();
        session.write_at(10, b"abcdefghij").await.unwrap();

        // Nothing in the served tree except the pre-existing file (the
        // target's parent directories are created eagerly, as before).
        let names: Vec<String> = storage
            .list_dir("")
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.path)
            .collect();
        assert_eq!(names, vec!["deep", "keep.txt"]);
        assert!(
            storage.list_dir("deep/dir").await.unwrap().is_empty(),
            "no in-flight temp may leak into the listing"
        );
        // The temp (and its sidecar) live in the scratch directory.
        let staged: Vec<String> = std::fs::read_dir(scratch.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert!(staged.iter().any(|n| n.starts_with(".libfw-sess-") && !n.ends_with(".blocks")));
        assert!(staged.iter().any(|n| n.ends_with(".blocks")));

        let meta = session.commit().await.unwrap();
        assert_eq!(meta.path, "deep/dir/big.bin");
        assert_eq!(meta.size, 20);
        assert_eq!(
            std::fs::read(root.path().join("deep/dir/big.bin")).unwrap(),
            b"0123456789abcdefghij"
        );
        // Scratch is empty again (temp + sidecar consumed by the commit).
        assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    }

    /// Two same-named targets in different directories must not share a temp
    /// once temps live in one flat directory.
    #[tokio::test]
    async fn temp_dir_names_are_unique_per_target_path() {
        let root = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path()).with_temp_dir(scratch.path());

        let mut a = storage
            .write_stream_session("a/same.bin", "sess", "o", WriteMode::Create)
            .await
            .unwrap();
        let mut b = storage
            .write_stream_session("b/same.bin", "sess", "o", WriteMode::Create)
            .await
            .unwrap();
        a.write_at(0, b"aaaa").await.unwrap();
        b.write_at(0, b"bbbb").await.unwrap();
        a.commit().await.unwrap();
        b.commit().await.unwrap();

        assert_eq!(std::fs::read(root.path().join("a/same.bin")).unwrap(), b"aaaa");
        assert_eq!(std::fs::read(root.path().join("b/same.bin")).unwrap(), b"bbbb");
    }

    /// The sweeper must also reach temps parked outside the root.
    #[tokio::test]
    async fn cleanup_sweeps_the_configured_temp_dir() {
        let root = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path()).with_temp_dir(scratch.path());

        // `max_age == 0` means "everything written before now", which keeps
        // the test independent of being able to rewind an mtime.
        let a = scratch.path().join(".libfw-sess-o-s-a.txt");
        std::fs::write(&a, b"partial").unwrap();
        let b = scratch.path().join(".libfw-sess-o-s-b.txt");
        std::fs::write(&b, b"partial").unwrap();
        // …and one inside the root, to prove the walk still covers both.
        std::fs::create_dir_all(root.path().join("sub")).unwrap();
        let c = root.path().join("sub/.libfw-sess-o-s-c.txt");
        std::fs::write(&c, b"partial").unwrap();
        let keep = root.path().join("sub/user.txt");
        std::fs::write(&keep, b"mine").unwrap();

        let removed = storage
            .cleanup_stale_sessions(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(removed, 3, "both trees are swept, sidecars excluded");
        assert!(!a.exists() && !b.exists() && !c.exists());
        assert!(keep.exists(), "committed user files are never swept");
    }
}
