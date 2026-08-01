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

//! Canonical value terms for JSON numbers and booleans.
//!
//! Numeric and boolean values are term-indexed by **value**, not by lexical
//! spelling: the canonical text of a JSON number is derived from its parsed
//! value under serde_json's classification order —
//!
//! - text that parses as `u64` → [`itoa`] decimal (`38`, `18446744073709551615`),
//! - text that parses as `i64` → [`itoa`] decimal (`-38`),
//! - everything else that parses as a **finite** `f64` → [`ryu`] shortest round-trip form (`38.0`,
//!   `1e-7`) — so `38.0`, `38.00` and `3.8e1` are ONE term,
//! - non-finite values (NaN/±Inf never appear in `_source`; an overflowing text like `1e999` parses
//!   to ±Inf) → no value term, matching the key-term policy (arrow-json serializes non-finite
//!   floats as JSON `null`).
//!
//! Booleans canonicalize to `true` / `false`.
//!
//! The token stored in the dictionary is the canonical text prefixed with
//! [`NUMERIC_TERM_TAG`] (`\x01`). The tag scopes lookups by value type:
//! string-shaped probes and scans (raw string equality, `str_match`
//! substring/regex walks, match_all token scans, `field_value_counts` value
//! enumeration) never see numeric terms, and numeric probes are built
//! explicitly via [`numeric_value_token`]. Without the tag, a query like
//! `str_field = '38'` would match rows whose JSON value is the *number* 38 —
//! rows the scan-side `json_get_str` projection maps to NULL. (A raw string
//! value that itself starts with byte `0x01` can still collide with a tagged
//! numeric term — a bounded, documented residual for adversarial
//! control-byte values.)
//!
//! Both writer derivations agree by construction: the column-driven path
//! canonicalizes the arrow value exactly as parsing its arrow-json `_source`
//! image back through serde_json would (integers are itoa either way; floats
//! round-trip through their shortest decimal form, so lexical-core's `1.0e20`
//! and ryu's `1e20` canonicalize identically; `Float32`/`Float16` values go
//! through their shortest *narrow* form first, mirroring arrow-json's
//! narrow-float encoders).

/// Leading tag byte of a numeric/boolean value-term token.
pub(crate) const NUMERIC_TERM_TAG: u8 = 0x01;

/// Build the dictionary token of a canonical numeric/boolean text:
/// [`NUMERIC_TERM_TAG`] followed by the text's bytes. Query mappings use this
/// to probe number/bool-typed equality; the composite field-id suffix is
/// appended by the query layer as for any token.
pub fn numeric_value_token(canonical_text: &str) -> Vec<u8> {
    let mut token = Vec::with_capacity(canonical_text.len() + 1);
    token.push(NUMERIC_TERM_TAG);
    token.extend_from_slice(canonical_text.as_bytes());
    token
}

/// Whether a dictionary token is a tagged numeric/boolean value term.
pub fn is_numeric_value_token(token: &[u8]) -> bool {
    token.first() == Some(&NUMERIC_TERM_TAG)
}

/// Canonical text of an `i64` value.
pub fn canonical_i64_text(value: i64) -> String {
    itoa::Buffer::new().format(value).to_string()
}

/// Canonical text of a `u64` value.
pub fn canonical_u64_text(value: u64) -> String {
    itoa::Buffer::new().format(value).to_string()
}

/// Canonical text of a finite `f64` value (ryu shortest round-trip form);
/// `None` for NaN/±Inf, which carry no value term.
pub fn canonical_f64_text(value: f64) -> Option<String> {
    value
        .is_finite()
        .then(|| ryu::Buffer::new().format_finite(value).to_string())
}

/// Canonical text of a finite `f32` value: the value is first rendered in
/// its shortest *f32* form (what arrow-json writes for a `Float32` column)
/// and that text is re-parsed as `f64` — the number a `_source` reader sees —
/// before the f64 canonicalization applies. `0.1f32` therefore canonicalizes
/// to `"0.1"`, not to the 17-digit image of `0.1f32 as f64`.
pub fn canonical_f32_text(value: f32) -> Option<String> {
    if !value.is_finite() {
        return None;
    }
    let mut narrow = ryu::Buffer::new();
    let text = narrow.format_finite(value);
    let widened: f64 = text
        .parse()
        .expect("shortest f32 form always parses as f64");
    canonical_f64_text(widened)
}

/// Canonical text of a boolean value.
pub fn canonical_bool_text(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

/// Canonical text of a [`serde_json::Number`], classified by VALUE exactly
/// like serde_json classifies number text: u64 first, then i64, then finite
/// f64 (see the module docs). `None` when no classification applies (an
/// overflowing text parsing to ±Inf).
pub fn canonical_number_text(number: &serde_json::Number) -> Option<String> {
    if let Some(value) = number.as_u64() {
        Some(canonical_u64_text(value))
    } else if let Some(value) = number.as_i64() {
        Some(canonical_i64_text(value))
    } else {
        number.as_f64().and_then(canonical_f64_text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_number_text_classifies_by_value() {
        let n = |text: &str| -> serde_json::Number { serde_json::from_str(text).unwrap() };
        // u64 / i64 route: itoa decimal
        assert_eq!(canonical_number_text(&n("38")).unwrap(), "38");
        assert_eq!(canonical_number_text(&n("-38")).unwrap(), "-38");
        assert_eq!(
            canonical_number_text(&n("18446744073709551615")).unwrap(),
            "18446744073709551615"
        );
        // f64 route: ryu shortest — spelling variants converge to one term
        assert_eq!(canonical_number_text(&n("38.0")).unwrap(), "38.0");
        assert_eq!(canonical_number_text(&n("38.00")).unwrap(), "38.0");
        assert_eq!(canonical_number_text(&n("3.8e1")).unwrap(), "38.0");
        assert_eq!(canonical_number_text(&n("1e20")).unwrap(), "1e20");
        assert_eq!(canonical_number_text(&n("-0.0")).unwrap(), "-0.0");
        // int-vs-float forms stay DISTINCT terms (queries probe the union)
        assert_ne!(
            canonical_number_text(&n("38")).unwrap(),
            canonical_number_text(&n("38.0")).unwrap()
        );
    }

    #[test]
    fn canonical_float_texts_match_source_round_trip() {
        // f64: canonical == ryu, and parsing any faithful spelling of the
        // value (e.g. lexical-core's exponent style) reproduces it
        assert_eq!(canonical_f64_text(38.0).unwrap(), "38.0");
        assert_eq!(canonical_f64_text(1e20).unwrap(), "1e20");
        assert_eq!(
            canonical_f64_text("1.0e20".parse().unwrap()).unwrap(),
            "1e20"
        );
        assert_eq!(canonical_f64_text(f64::NAN), None);
        assert_eq!(canonical_f64_text(f64::INFINITY), None);

        // f32 goes through its shortest narrow form: the value a _source
        // reader parses, NOT the widened 17-digit image
        assert_eq!(canonical_f32_text(0.1f32).unwrap(), "0.1");
        assert_eq!(canonical_f32_text(38.0f32).unwrap(), "38.0");
        assert_eq!(canonical_f32_text(f32::NAN), None);
    }

    #[test]
    fn numeric_tokens_are_tagged() {
        let token = numeric_value_token("38.0");
        assert_eq!(token[0], NUMERIC_TERM_TAG);
        assert_eq!(&token[1..], b"38.0");
        assert!(is_numeric_value_token(&token));
        assert!(!is_numeric_value_token(b"38.0"));
        assert!(!is_numeric_value_token(b""));
    }
}
