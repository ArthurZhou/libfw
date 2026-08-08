//! HTTP-range and ETag helpers (RFC 7233 / RFC 9110).

use std::str::FromStr;

use libfw_core::RangeSpec;

/// Errors from parsing a `Range` header.
#[derive(Debug, PartialEq, Eq)]
pub enum RangeParseError {
    /// The header is malformed or uses an unsupported unit.
    Malformed,
}

/// A parsed, still-unresolved range request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedRange {
    /// An explicit byte range `[start, end)` (end may be unbounded).
    Bytes(RangeSpec),
    /// The final `n` bytes of the resource (`bytes=-n`).
    Suffix(u64),
}

/// Parse a single-range `Range` header like `bytes=0-499`.
///
/// Returns:
/// - `Ok(Some(range))` for a single, well-formed `bytes=` range,
/// - `Ok(None)` when the header is absent, uses another unit, or requests
///   multiple ranges (which RFC 7233 §3.1 permits servers to ignore),
/// - `Err(Malformed)` for a malformed `bytes=` value.
pub fn parse_range_header(header: &str) -> Result<Option<ParsedRange>, RangeParseError> {
    let header = header.trim();
    if header.is_empty() {
        return Ok(None);
    }
    let Some((unit, spec)) = header.split_once('=') else {
        return Ok(None); // "other-units" — ignore per RFC 7233 §3.1
    };
    if !unit.trim().eq_ignore_ascii_case("bytes") {
        return Ok(None);
    }
    let spec = spec.trim();
    if spec.contains(',') {
        return Ok(None); // multi-range: may ignore
    }
    if let Some(suffix) = spec.strip_prefix('-') {
        let n = parse_u64(suffix)?;
        return Ok(Some(ParsedRange::Suffix(n)));
    }
    if let Some((start_s, end_s)) = spec.split_once('-') {
        let start = parse_u64(start_s)?;
        let end = if end_s.is_empty() {
            u64::MAX
        } else {
            // Inclusive → exclusive end. Guard against overflow on the
            // largest u64 value instead of panicking (debug) / wrapping.
            parse_u64(end_s)?
                .checked_add(1)
                .ok_or(RangeParseError::Malformed)?
        };
        if end <= start {
            return Err(RangeParseError::Malformed);
        }
        return Ok(Some(ParsedRange::Bytes(RangeSpec { start, end })));
    }
    Err(RangeParseError::Malformed)
}

fn parse_u64(s: &str) -> Result<u64, RangeParseError> {
    if s.is_empty() {
        return Err(RangeParseError::Malformed);
    }
    u64::from_str(s).map_err(|_| RangeParseError::Malformed)
}

/// `bytes start-end/total` (end inclusive) for a `Content-Range` header.
pub fn content_range_value(spec: &RangeSpec, total: u64) -> String {
    format!("bytes {}-{}/{}", spec.start, spec.end - 1, total)
}

/// `bytes */total` for a `416 Range Not Satisfiable` response.
pub fn content_range_none_value(total: u64) -> String {
    format!("bytes */{total}")
}

/// Check `If-None-Match` against the current ETag (RFC 9110 §13.1.2).
///
/// Supports `*` and comma-separated lists; `W/` weak prefixes compare
/// equal. Returns `true` when the request should be answered `304`.
pub fn etag_matches_if_none_match(if_none_match: &str, etag: &str) -> bool {
    let strong_etag = etag.strip_prefix("W/").unwrap_or(etag);
    if_none_match.split(',').any(|tag| {
        let tag = tag.trim().strip_prefix("W/").unwrap_or(tag.trim());
        tag == "*" || tag == strong_etag
    })
}

/// Check `If-Range` (strong ETag form only, RFC 9110 §13.1.5).
///
/// Returns `true` when the range should be honored.
pub fn if_range_matches(if_range: &str, etag: &str) -> bool {
    let candidate = if_range.trim();
    if let Some(tag) = candidate.strip_prefix('"') {
        // Strong comparison only — a `W/` If-Range never matches.
        return tag.trim_end_matches('"') == etag.trim_matches('"');
    }
    // HTTP-date form: not supported here — treat as no match.
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_closed_range() {
        let ParsedRange::Bytes(r) = parse_range_header("bytes=0-499").unwrap().unwrap() else {
            panic!("expected bytes range");
        };
        assert_eq!((r.start, r.end), (0, 500));
    }

    #[test]
    fn parses_open_ended_range() {
        let ParsedRange::Bytes(r) = parse_range_header("bytes=500-").unwrap().unwrap() else {
            panic!("expected bytes range");
        };
        assert_eq!((r.start, r.end), (500, u64::MAX));
    }

    #[test]
    fn parses_suffix_range() {
        let ParsedRange::Suffix(n) = parse_range_header("bytes=-500").unwrap().unwrap() else {
            panic!("expected suffix range");
        };
        assert_eq!(n, 500);
    }

    #[test]
    fn ignores_other_units_and_multi_ranges() {
        assert_eq!(parse_range_header("items=0-1").unwrap(), None);
        assert_eq!(parse_range_header("bytes=0-1,3-4").unwrap(), None);
        assert_eq!(parse_range_header("").unwrap(), None);
    }

    #[test]
    fn rejects_malformed() {
        assert_eq!(parse_range_header("bytes=abc"), Err(RangeParseError::Malformed));
        assert_eq!(parse_range_header("bytes=5-2"), Err(RangeParseError::Malformed));
        assert_eq!(parse_range_header("bytes=-"), Err(RangeParseError::Malformed));
    }

    #[test]
    fn rejects_overflowing_end_instead_of_panicking() {
        // u64::MAX + 1 must not overflow/panic; it is treated as malformed.
        assert_eq!(
            parse_range_header("bytes=0-18446744073709551615"),
            Err(RangeParseError::Malformed)
        );
    }

    #[test]
    fn if_none_match_matching() {
        assert!(etag_matches_if_none_match("\"abc\"", "\"abc\""));
        assert!(etag_matches_if_none_match("W/\"abc\"", "\"abc\""));
        assert!(etag_matches_if_none_match("*", "\"abc\""));
        assert!(etag_matches_if_none_match("\"x\", \"abc\"", "\"abc\""));
        assert!(!etag_matches_if_none_match("\"xyz\"", "\"abc\""));
    }

    #[test]
    fn if_range_matching() {
        assert!(if_range_matches("\"abc\"", "\"abc\""));
        assert!(!if_range_matches("W/\"abc\"", "\"abc\""));
        assert!(!if_range_matches("\"xyz\"", "\"abc\""));
        assert!(!if_range_matches("Wed, 01 Jan 2025 00:00:00 GMT", "\"abc\""));
    }
}
