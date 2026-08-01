// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Query model and composite-term encoding.
//!
//! Dictionary keys are FIELD-MAJOR (`key_layout=fid_v2`, the only layout):
//! `{field_id u16 BE}{token bytes}` — no separator, the fid is fixed-width
//! ([`write_composite`]/[`split_key`]). The historical composite-TERM byte
//! form `{token bytes}\x00{field_id u16 BE}` (a fixed 3-byte suffix, parsed
//! from the *end* of the key so a `\x00` inside the token is harmless)
//! survives as a live KEY FORM, not layout compat: bloom observe/probe are
//! pinned to it forever ([`bloom_canonical_key`] — group `.bf` continuity),
//! and merge partition sampling parses it
//! ([`write_composite_term`]).

/// Length of the composite-term suffix: `\x00` + `u16` big-endian field id.
pub(crate) const FIELD_SUFFIX_LEN: usize = 3;

/// The field id reserved as the *key marker* in `.vix` files: a composite
/// term `{path}\x00\xFF\xFF` records "this document has a non-null value at
/// `path`" (one key term per distinct path per doc). Key terms have no
/// fields-table entry.
pub(crate) const KEY_FIELD_ID: u16 = 0xFFFF;

/// Highest field id a real (value-indexed) field may take in a `.vix` file;
/// `0xFFFF` is reserved for [`KEY_FIELD_ID`]. Fields beyond this cap are not
/// term-indexed and land in `partial_fields` instead (never an error).
pub(crate) const MAX_REAL_FIELD_ID: u16 = 0xFFFE;

/// Build the composite key for `token` in field `field_id`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn composite_term(token: &[u8], field_id: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(token.len() + FIELD_SUFFIX_LEN);
    write_composite_term(&mut key, token, field_id);
    key
}

/// Write the composite key for `token` in field `field_id` into `out`
/// (clearing it first). Lets callers reuse a scratch buffer.
pub(crate) fn write_composite_term(out: &mut Vec<u8>, token: &[u8], field_id: u16) {
    out.clear();
    out.reserve(token.len() + FIELD_SUFFIX_LEN);
    out.extend_from_slice(token);
    out.push(0);
    out.extend_from_slice(&field_id.to_be_bytes());
}

/// Build the field-major dictionary key for `token` in field `field_id`
/// into `out` (clearing it first): `{fid u16 BE}{token}` — each field's keys
/// are contiguous (no separator: the fid is fixed-width), so probes and
/// walks touch only the cells intersecting that field's range. The numeric
/// tag (`\x01…`) and the key-term marker live inside the token, so every
/// key builds uniformly from `(token, fid)`.
pub(crate) fn write_composite(out: &mut Vec<u8>, token: &[u8], field_id: u16) {
    out.clear();
    out.reserve(token.len() + 2);
    out.extend_from_slice(&field_id.to_be_bytes());
    out.extend_from_slice(token);
}

/// Split a field-major dictionary key into `(token, field_id)` — the
/// inverse of [`write_composite`]. Returns `None` for keys too short to
/// carry the fid prefix (never produced by the writers).
pub(crate) fn split_key(key: &[u8]) -> Option<(&[u8], u16)> {
    if key.len() < 2 {
        return None;
    }
    Some((&key[2..], u16::from_be_bytes([key[0], key[1]])))
}

/// Rebuild the v1 (bloom-canonical) byte form of a dictionary key: the key
/// is split and re-suffixed to `{token}\x00{fid}`. Bloom observe/probe are
/// PINNED to this byte form forever — group `.bf` files accumulated under
/// it since v1 and must keep matching (a live key form, not layout compat).
pub(crate) fn bloom_canonical_key<'a>(key: &'a [u8], scratch: &'a mut Vec<u8>) -> &'a [u8] {
    let (token, fid) = (&key[2..], u16::from_be_bytes([key[0], key[1]]));
    write_composite_term(scratch, token, fid);
    scratch.as_slice()
}

/// A query over a `.vix` index, evaluated by
/// [`VixReader::eval`](crate::VixReader::eval) into a per-document bitmap.
///
/// Term-level variants operate on *tokens* (the part of a composite term
/// before the field-id suffix). Variants with `field: Option<String>` scan
/// every field when `field` is `None`. Referencing a field that is not
/// term-indexed in the file is an error — callers are expected to check
/// [`VixReader::field_id`](crate::VixReader::field_id) first and fall back to
/// a scan-time filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VixQuery {
    /// The exact token in the given field (one FST point lookup).
    Exact { field: String, token: Vec<u8> },
    /// Tokens starting with `prefix`, in one field or (if `None`) any field.
    Prefix {
        field: Option<String>,
        prefix: Vec<u8>,
    },
    /// Tokens containing `needle` as a substring.
    Contains {
        field: Option<String>,
        needle: Vec<u8>,
        case_insensitive: bool,
    },
    /// Tokens fully matching the regular expression (anchored, like tantivy's
    /// `RegexQuery`).
    Regex {
        field: Option<String>,
        pattern: String,
    },
    /// Tokens within the given Levenshtein `distance` (max 2) of `token`, in
    /// any field.
    Fuzzy { token: String, distance: u8 },
    /// The exact token in *any* field (match_all plain-token semantics).
    TokenAnyField { token: Vec<u8> },
    /// Documents that have a non-null value at the flattened `path`
    /// (key-existence terms).
    KeyExists { path: String },
    /// Every document.
    All,
    /// No document — the provably-empty query. Callers map a condition on a
    /// field the file provably does not carry (no key term ⇒ NULL in every
    /// row) to this instead of erroring. Evaluates as a term leaf matching
    /// nothing, so it composes exactly: `And` containing it is empty (with
    /// the usual short-circuit, zero postings IO), `Or` treats it as the
    /// identity, `Not` yields every document.
    Nothing,
    /// Bitmap AND of the sub-queries; an empty list evaluates to [`Self::All`].
    And(Vec<VixQuery>),
    /// Bitmap OR of the sub-queries; an empty list evaluates to no documents.
    Or(Vec<VixQuery>),
    /// Bitmap complement of the sub-query.
    Not(Box<VixQuery>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_roundtrip_and_ordering() {
        let mut key = Vec::new();
        write_composite(&mut key, b"hello", 258);
        assert_eq!(key, [&258u16.to_be_bytes()[..], b"hello"].concat());
        assert_eq!(split_key(&key), Some((&b"hello"[..], 258)));
        // field-major: ALL of field 1's keys sort before ANY of field 2's
        let mut a = Vec::new();
        let mut b = Vec::new();
        write_composite(&mut a, b"zzzz", 1);
        write_composite(&mut b, b"aaaa", 2);
        assert!(a < b, "keys must cluster by field id");
    }

    #[test]
    fn bloom_canonical_is_v1_form() {
        let mut key = Vec::new();
        write_composite(&mut key, b"tok", 7);
        let mut scratch = Vec::new();
        let canon = bloom_canonical_key(&key, &mut scratch);
        assert_eq!(canon, composite_term(b"tok", 7).as_slice());
        // numeric-tagged and key-term tokens convert uniformly too
        let mut tagged = Vec::new();
        write_composite(&mut tagged, b"\x01123", 9);
        let mut scratch = Vec::new();
        assert_eq!(
            bloom_canonical_key(&tagged, &mut scratch),
            composite_term(b"\x01123", 9).as_slice()
        );
    }

    #[test]
    fn composite_roundtrip() {
        let key = composite_term(b"hello", 258);
        assert_eq!(key, b"hello\x00\x01\x02");
    }

    #[test]
    fn composite_with_nul_in_token() {
        // NULs inside tokens are legal in the pinned bloom byte form; the
        // writer appends the 3-byte suffix regardless of token content.
        assert_eq!(composite_term(b"a\x00b", 1), b"a\x00b\x00\x00\x01");
    }

    #[test]
    fn write_composite_reuses_buffer() {
        let mut buf = vec![1, 2, 3];
        write_composite_term(&mut buf, b"x", 0xBEEF);
        assert_eq!(buf, b"x\x00\xBE\xEF");
    }
}
