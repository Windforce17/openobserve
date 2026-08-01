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

//! THE full-text tokenizer of `.vix` files (`tokenizer = "o2-v2"`) — the
//! single source of truth for both the write side (term extraction) and the
//! search side (match_all query tokenization). Both must call
//! [`o2_tokenize`]; any second implementation reintroduces the silent
//! write/search mismatch this replaced.
//!
//! The behavior is a faithful port of the search pipeline in
//! `src/config/src/text_tokenizer/` (`O2Tokenizer` in `Search` collect
//! mode + `RemoveShortFilter` + tantivy `RemoveLongFilter` + `LowerCaser`):
//!
//! - ASCII alphanumeric runs are tokens, split at ANY non-ASCII-alphanumeric boundary (so "café" ->
//!   "caf" + "é", "用户admin登录" isolates "admin"),
//! - every non-ASCII **alphanumeric** char is its own token (per-char CJK),
//! - length filter in **bytes**, applied before lowercasing, mirroring the tantivy filters exactly:
//!   keep `min <= len` (`RemoveShortFilter`) and `len < max` (`RemoveLongFilter` is exclusive),
//!   with `min`/`max` clamped to at least 2/64 exactly like `o2_tokenizer_build` (`max(cfg,
//!   MIN_TOKEN_LENGTH)` / `max(cfg, MAX_TOKEN_LENGTH)`),
//! - lowercase the survivors.
//!
//! Deliberate deviations from the tantivy `O2Tokenizer`, identical on both
//! sides by construction since both call this function:
//!
//! - non-ASCII **non**-alphanumeric chars (emoji, unicode punctuation) never become tokens (the
//!   tantivy stream leaks the one directly following an ASCII run as a token),
//! - no camelCase handling: in `Search` mode the tantivy tokenizer emits only the root token, which
//!   equals the plain ASCII run, so queries line up; the `Ingest`-mode split tokens ("getUserName"
//!   -> "get"/"User"/"Name") are NOT indexed — the previous `.vix` tokenizer never split camelCase
//!   either.
//!
//! History: files stamped `tokenizer = "o2-v1"` were written by the removed
//! previous implementation (split on `!char::is_alphanumeric`, char-count
//! length filter), which silently missed non-ASCII match_all queries. There
//! is no legacy code path: the property mismatch makes the compaction merge
//! rebuild such files from `_source` (re-tokenizing with this function), so
//! they converge to `"o2-v2"` on their next compaction.

/// Byte-length floor mirrored from `o2_tokenizer_build`'s `MIN_TOKEN_LENGTH`
/// (the configured minimum is clamped to at least this).
const MIN_TOKEN_LENGTH: usize = 2;
/// Byte-length ceiling floor mirrored from `o2_tokenizer_build`'s
/// `MAX_TOKEN_LENGTH` (the configured maximum is clamped to at least this;
/// the limit itself is exclusive, like tantivy's `RemoveLongFilter`).
const MAX_TOKEN_LENGTH: usize = 64;

/// Tokenize `text` for full-text indexing **and** for match_all query terms
/// (see the [module docs](self) for the exact spec and its provenance).
///
/// `min_len`/`max_len` are byte lengths — pass the configured
/// `ZO_INVERTED_INDEX_MIN/MAX_TOKEN_LENGTH` values on both sides. They are
/// clamped to `>= 2`/`>= 64` (mirroring `o2_tokenizer_build`) and the
/// maximum is exclusive (mirroring tantivy's `RemoveLongFilter`). The
/// filter applies to the original bytes; lowercasing happens after, exactly
/// like the tantivy pipeline order.
pub fn o2_tokenize(text: &str, min_len: usize, max_len: usize) -> impl Iterator<Item = String> {
    let min = min_len.max(MIN_TOKEN_LENGTH);
    let max = max_len.max(MAX_TOKEN_LENGTH);
    let keep = |token: &str| token.len() >= min && token.len() < max;

    let mut tokens: Vec<String> = Vec::new();
    let mut run_start: Option<usize> = None;
    for (index, ch) in text.char_indices() {
        if ch.is_ascii_alphanumeric() {
            if run_start.is_none() {
                run_start = Some(index);
            }
            continue;
        }
        // any other char ends the current ASCII run
        if let Some(start) = run_start.take() {
            let token = &text[start..index];
            if keep(token) {
                tokens.push(token.to_lowercase());
            }
        }
        if !ch.is_ascii() && ch.is_alphanumeric() {
            // one token per non-ASCII alphanumeric char (2..=4 bytes)
            let token = &text[index..index + ch.len_utf8()];
            if keep(token) {
                tokens.push(token.to_lowercase());
            }
        }
    }
    if let Some(start) = run_start {
        let token = &text[start..];
        if keep(token) {
            tokens.push(token.to_lowercase());
        }
    }
    tokens.into_iter()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(text: &str) -> Vec<String> {
        o2_tokenize(text, 2, 64).collect()
    }

    // The expected values below cross-check the search pipeline: each case
    // is what `config::text_tokenizer::o2_collect_search_tokens` (the
    // tantivy `O2Tokenizer(Search)` + RemoveShort(2) + RemoveLong(64) +
    // LowerCaser stack) produces for the same input — except the documented
    // deviations, which are called out inline.

    #[test]
    fn ascii_splits_on_non_alphanumeric() {
        assert_eq!(
            tokens("Hello, happy tax-payer!"),
            vec!["hello", "happy", "tax", "payer"]
        );
        assert_eq!(tokens("192.168.1.80"), vec!["192", "168", "80"]);
    }

    #[test]
    fn lowercases() {
        assert_eq!(tokens("ERROR WaRn"), vec!["error", "warn"]);
    }

    #[test]
    fn accented_splits_ascii_runs_and_emits_char_tokens() {
        // matches o2_collect_search_tokens("café") == ["caf", "é"]:
        // the ASCII run ends at the non-ASCII char, which (being
        // alphanumeric and 2 bytes >= min 2) is a token of its own
        assert_eq!(tokens("café"), vec!["caf", "é"]);
        assert_eq!(tokens("café latte"), vec!["caf", "é", "latte"]);
    }

    #[test]
    fn cjk_per_char_and_embedded_ascii() {
        // matches o2_collect_search_tokens("用户admin登录")
        assert_eq!(
            tokens("用户admin登录"),
            vec!["用", "户", "admin", "登", "录"]
        );
        // a lone CJK char is 3 bytes >= min 2: kept (the byte filter is the
        // point — the old char-count filter dropped it)
        assert_eq!(tokens("中"), vec!["中"]);
        assert_eq!(tokens("size 中 large"), vec!["size", "中", "large"]);
        // Korean per char, like the tantivy pipeline
        assert_eq!(
            tokens("민족어대사전"),
            vec!["민", "족", "어", "대", "사", "전"]
        );
    }

    #[test]
    fn mixed_scripts() {
        // mirrors the O2Tokenizer test corpus (Search mode)
        assert_eq!(
            tokens("Hello世界こんにちは"),
            vec!["hello", "世", "界", "こ", "ん", "に", "ち", "は"]
        );
    }

    #[test]
    fn digits_and_alphanumeric_runs() {
        assert_eq!(tokens("123 456 789"), vec!["123", "456", "789"]);
        assert_eq!(tokens("test123 data456abc"), vec!["test123", "data456abc"]);
        // digits glued to CJK split exactly at the boundary
        assert_eq!(tokens("错误404页面"), vec!["错", "误", "404", "页", "面"]);
    }

    #[test]
    fn camel_case_is_one_run_like_search_mode() {
        // Search-mode tantivy emits only the root token — identical to the
        // plain run; the ingest-side splits are deliberately not produced.
        assert_eq!(tokens("getUserName"), vec!["getusername"]);
        assert_eq!(tokens("XMLHttpRequest"), vec!["xmlhttprequest"]);
    }

    #[test]
    fn emoji_and_unicode_punctuation_are_delimiters_not_tokens() {
        // DELIBERATE DEVIATION from the tantivy stream (which leaks a
        // non-ASCII delimiter directly after an ASCII run as a token):
        // non-alphanumeric chars never become tokens here.
        assert_eq!(tokens("go🚀now"), vec!["go", "now"]);
        assert_eq!(tokens("caf—bar"), vec!["caf", "bar"]);
        assert!(tokens("🚀🎉").is_empty());
    }

    #[test]
    fn length_filter_is_bytes_min_inclusive_max_exclusive() {
        // 1-byte ASCII tokens drop; 2-byte keep (RemoveShortFilter: >= min)
        assert_eq!(tokens("a bb c"), vec!["bb"]);
        // é is 1 char but 2 BYTES: kept
        assert_eq!(tokens("x é y"), vec!["é"]);
        // RemoveLongFilter is exclusive: a 64-byte token drops, 63 keeps
        let b63 = "y".repeat(63);
        let b64 = "z".repeat(64);
        assert_eq!(tokens(&format!("{b63} {b64}")), vec![b63]);
        // filtering happens on pre-lowercase bytes, like the pipeline order
        let cap63 = "Y".repeat(63);
        assert_eq!(tokens(&cap63), vec!["y".repeat(63)]);
    }

    #[test]
    fn clamps_limits_like_o2_tokenizer_build() {
        // min clamps up to 2 (max(cfg, MIN_TOKEN_LENGTH))
        let toks: Vec<String> = o2_tokenize("a bb", 0, 64).collect();
        assert_eq!(toks, vec!["bb"]);
        // max clamps up to 64 (max(cfg, MAX_TOKEN_LENGTH)): a 32-byte token
        // survives a configured max of 8
        let t32 = "q".repeat(32);
        let toks: Vec<String> = o2_tokenize(&t32, 2, 8).collect();
        assert_eq!(toks, vec![t32]);
        // a raised max is honored (65-byte token kept with max 128)
        let t65 = "r".repeat(65);
        let toks: Vec<String> = o2_tokenize(&t65, 2, 128).collect();
        assert_eq!(toks, vec![t65]);
        // ... and the exclusive bound still applies at the raised value
        let t128 = "s".repeat(128);
        assert!(o2_tokenize(&t128, 2, 128).next().is_none());
    }

    #[test]
    fn empty_and_punctuation_only() {
        assert!(tokens("").is_empty());
        assert!(tokens("!@#$%^&*()").is_empty());
        assert!(tokens("   \t\n  ").is_empty());
    }
}
