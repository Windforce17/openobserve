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

//! The `.vix` core file format: inverted index + document store in
//! one puffin container — the `.vix` file *is* the data file.
//!
//! Blobs:
//! - `dict`:  one row per term row-group — `first_ordinal`, `term_min`, `term_max`, `fst`
//!   (tantivy-fst map of composite term -> local ordinal).
//! - `terms`: one row per composite term — `doc_count`, `postings` (delta + bitpacked doc ids).
//!   **Key terms** (`{path}\x00\xFF\xFF`, field id `0xFFFF` reserved as the key marker; real field
//!   ids cap at `0xFFFE`, overflowing fields degrade to `partial_fields`) record which docs have a
//!   non-null value at each flattened path, and **dense elision** writes an empty postings blob for
//!   any term whose `doc_count` equals the file's row count (the reader synthesizes the all-ones
//!   bitmap).
//! - `docs`:  one row per record — `_timestamp: i64`, one native column per column-store field, the
//!   caller-supplied `_source: utf8` (always) and `_original: utf8` (opt-in). Its columns are
//!   compressed with the BtrBlocks sampler *plus* the zstd/pco compact schemes
//!   (`_source`/`_original` JSON text lands on zstd), and its chunks — the decompression unit of a
//!   matched-row point read — are sized by the [`VixWriterOptions::docs_chunk_bytes`] byte budget,
//!   not the data row-group size.
//!
//! Composite terms encode `{token}\x00{field_id u16 BE}` so one FST serves
//! per-field exact/prefix/regex/fuzzy lookups, cross-field full-text scans
//! and key-existence lookups (see DESIGN.md §15 at the repository root of
//! this fork's parent directory).
//!
//! Entry points: [`VixWriter`] builds a file from arrow record batches
//! ([`VixWriter::new`] + [`VixWriter::push_batch_with_source`] /
//! [`VixWriter::push_docs_rows`]); [`VixReader`] opens the bytes and
//! evaluates [`VixQuery`]s into per-document
//! [`arrow::buffer::BooleanBuffer`] bitmaps.
//! [`VixReader::open_ranged`] / [`VixDocs::open_ranged`] open the same
//! structures over a [`VixRangeSource`] (an async `fetch(range)` closure over
//! an object store), turning query evaluation into a handful of small range
//! fetches instead of a whole-file download.
//!
//! The `version` puffin property is the format's one evolution
//! discriminator: readers require exactly the supported value (`"2"`) and
//! reject anything else at open with a clear error; every other unknown
//! property is ignored. Blobs are matched by `(blob_tag, type id)`; unknown blobs are
//! ignored, so the envelope tolerates additions.

pub mod bloom;
mod container;
mod dict_blocks;
mod docs;
mod error;
mod merge;
mod numeric;
pub mod postings;
mod query;
mod reader;
pub mod sbbf;
mod source;
mod spill;
#[doc(hidden)]
pub mod test_support;
mod tokenizer;
mod writer;

pub use container::VixOutput;
pub use docs::{ColumnBound, NumScalar, VixDocs};
pub use error::VixError;
pub use merge::DocIdMap;
pub use numeric::{
    canonical_bool_text, canonical_f32_text, canonical_f64_text, canonical_i64_text,
    canonical_number_text, canonical_u64_text, is_numeric_value_token, numeric_value_token,
};
pub use query::VixQuery;
pub use reader::{DocsDictChunk, FieldValueCounts, PlistCursor, TermVisitor, VixReader, ZoneChunk};
pub use source::{BytesRangeSource, VixRangeSource};
pub use tokenizer::o2_tokenize;
pub use writer::{
    DEFAULT_DOCS_CHUNK_BYTES, ID_COL_NAME, ORIGINAL_DATA_COL_NAME, SOURCE_COL_NAME,
    SOURCE_RENAMED_COL_NAME, TIMESTAMP_COL_NAME, VixWriter, VixWriterOptions, VixWriterStats,
    is_value_indexed_type,
};

#[cfg(test)]
mod tests;
