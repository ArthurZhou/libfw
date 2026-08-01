//! Byte-range representation shared by server and client.

use serde::{Deserialize, Serialize};

/// A byte range of a resource: `[start, end)`.
///
/// The transport layer uses *half-open* ranges internally; HTTP
/// `Content-Range` values are emitted as `start-end` with inclusive end
/// (see server helpers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeSpec {
    /// First byte (inclusive).
    pub start: u64,
    /// One past the last byte (exclusive).
    pub end: u64,
}

impl RangeSpec {
    /// Creates a range covering `len` bytes starting at `start`.
    pub fn new(start: u64, len: u64) -> Self {
        RangeSpec {
            start,
            end: start.saturating_add(len),
        }
    }

    /// A range covering the whole resource.
    pub fn full(len: u64) -> Self {
        RangeSpec { start: 0, end: len }
    }

    /// Number of bytes in the range.
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// True when the range is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clamp this range to `file_size` bytes.
    ///
    /// Returns `None` when the range lies completely beyond the file
    /// (HTTP 416 territory) or when `file_size` is zero.
    pub fn clamp(&self, file_size: u64) -> Option<RangeSpec> {
        if self.start >= file_size {
            return None;
        }
        Some(RangeSpec {
            start: self.start,
            end: self.end.min(file_size),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_range() {
        let r = RangeSpec::new(100, 50);
        assert_eq!((r.start, r.end, r.len()), (100, 150, 50));
    }

    #[test]
    fn clamp_limits_end() {
        let r = RangeSpec::new(0, 1000).clamp(100).unwrap();
        assert_eq!((r.start, r.end), (0, 100));
    }

    #[test]
    fn clamp_rejects_past_end() {
        assert!(RangeSpec::new(100, 10).clamp(99).is_none());
    }

    #[test]
    fn clamp_rejects_zero_file() {
        assert!(RangeSpec::full(0).clamp(0).is_none());
    }
}
