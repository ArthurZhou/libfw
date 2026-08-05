//! Transfer planning: flattening server listings and JS file lists into
//! ordered [`FileEntry`]s, and slicing them into chunks.

use js_sys::{Array, Reflect};
use wasm_bindgen::JsValue;

use crate::error::LibfwError;
use libfw_core::metadata::{etag_from_size_mtime, FileMeta, TransferPlan};

/// A file to transfer, identified by its virtual path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Virtual path relative to the mounted root (POSIX separators).
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Last-modified unix time.
    pub mtime: u64,
}

impl FileEntry {
    /// Build a [`FileMeta`] (computing the ETag from size + mtime).
    pub fn to_meta(&self) -> FileMeta {
        FileMeta {
            path: self.path.clone(),
            size: self.size,
            mtime: self.mtime,
            etag: etag_from_size_mtime(self.size, self.mtime),
        }
    }

    /// The transfer plan for this file at `chunk_size`.
    pub fn plan(&self, chunk_size: u64) -> TransferPlan {
        TransferPlan::with_chunk_size(self.to_meta(), chunk_size)
    }
}

/// Parse a JS array of `{ path, size, mtime }` objects.
pub fn parse_file_entries(value: &JsValue) -> Result<Vec<FileEntry>, LibfwError> {
    let arr = Array::from(value);
    let mut out = Vec::with_capacity(arr.length() as usize);
    for item in arr.iter() {
        let path = Reflect::get(&item, &JsValue::from_str("path"))
            .map_err(|e| LibfwError::Js(format!("missing `path`: {e:?}")))?
            .as_string()
            .ok_or_else(|| LibfwError::Js("`path` must be a string".into()))?;
        let size = Reflect::get(&item, &JsValue::from_str("size"))
            .ok()
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0) as u64;
        let mtime = Reflect::get(&item, &JsValue::from_str("mtime"))
            .ok()
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0) as u64;
        out.push(FileEntry { path, size, mtime });
    }
    Ok(out)
}

/// Total bytes of a file list (for progress reporting).
pub fn total_bytes(files: &[FileEntry]) -> u64 {
    files.iter().map(|f| f.size).sum()
}

/// The next chunk boundaries `[offset, end)` for `file` at `chunk_size`,
/// starting at `from` (a resume offset).
pub fn chunk_bounds(file: &FileEntry, chunk_size: u64, from: u64) -> Vec<(u64, u64)> {
    let mut bounds = Vec::new();
    let mut offset = from.min(file.size);
    while offset < file.size {
        let end = (offset + chunk_size).min(file.size);
        bounds.push((offset, end));
        offset = end;
    }
    bounds
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_bounds_cover_file_from_resume() {
        let f = FileEntry {
            path: "a.bin".into(),
            size: 10,
            mtime: 1,
        };
        let bounds = chunk_bounds(&f, 4, 0);
        assert_eq!(bounds, vec![(0, 4), (4, 8), (8, 10)]);

        let resumed = chunk_bounds(&f, 4, 4);
        assert_eq!(resumed, vec![(4, 8), (8, 10)]);

        let done = chunk_bounds(&f, 4, 10);
        assert!(done.is_empty());
    }

    #[test]
    fn plan_meta_has_etag() {
        let f = FileEntry {
            path: "x".into(),
            size: 5,
            mtime: 42,
        };
        let plan = f.plan(4);
        assert_eq!(plan.chunks.len(), 2);
        assert!(!plan.file.etag.is_empty());
        assert_eq!(plan.total_bytes(), 5);
    }

    #[test]
    #[cfg(target_arch = "wasm32")]
    fn parses_js_file_entries() {
        let arr = Array::new();
        let a = js_sys::Object::new();
        js_sys::Reflect::set(&a, &JsValue::from_str("path"), &JsValue::from_str("d/f.txt"))
            .unwrap();
        js_sys::Reflect::set(&a, &JsValue::from_str("size"), &JsValue::from_f64(12.0)).unwrap();
        arr.push(&a);
        let entries = parse_file_entries(&arr.into()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "d/f.txt");
        assert_eq!(entries[0].size, 12);
    }
}
