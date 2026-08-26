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

//! Error type of the `vortex_index` crate.
//!
//! Everything user-facing is wrapped in [`VixError`]; the public API surfaces
//! it through `anyhow::Result`, which it converts into automatically. Malformed
//! input (a corrupt `.vix` file, a bad query) must always produce an error and
//! never a panic.

use thiserror::Error;

/// Errors produced while building or querying a `.vix` index.
#[derive(Debug, Error)]
pub enum VixError {
    /// The container or one of its blobs cannot be decoded.
    #[error("malformed .vix data: {0}")]
    Malformed(String),
    /// The object's `version` property is not the one format version this
    /// crate understands (`version = "3"`, the format's single
    /// future-evolution discriminator), or the sidecar's `key_layout` is
    /// foreign.
    #[error("unsupported .vix format: {0}")]
    UnsupportedFormat(String),
    /// A query referenced a field that is not term-indexed in this file.
    #[error("field is not term-indexed in this file: {0:?}")]
    FieldNotIndexed(String),
    /// A column read referenced a column that is not in the column store.
    #[error("column not found in the column store: {0:?}")]
    ColumnNotFound(String),
    /// A term ordinal points outside the `terms` table.
    #[error("term ordinal {ordinal} out of range (term_count = {term_count})")]
    OrdinalOutOfRange { ordinal: u64, term_count: u64 },
    /// The query itself is invalid (bad regex, unsupported fuzzy distance, ...).
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    /// The writer was constructed or driven with unusable inputs.
    #[error("writer error: {0}")]
    Writer(String),
    /// Error bubbled up from the vortex engine.
    #[error("vortex error: {0}")]
    Vortex(#[from] vortex::error::VortexError),
    /// Error bubbled up from arrow.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Error bubbled up from tantivy-fst (dictionary build or lookup).
    #[error("fst error: {0}")]
    Fst(#[from] tantivy_fst::Error),
    /// Error while (de)serializing the JSON file properties.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// Error surfaced by a caller-supplied scan callback (streaming scans
    /// abort on the first callback failure and propagate it unchanged).
    #[error(transparent)]
    Callback(anyhow::Error),
}

/// Crate-internal result alias.
pub(crate) type Result<T, E = VixError> = std::result::Result<T, E>;
