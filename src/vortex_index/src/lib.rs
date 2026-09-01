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

//! The core file format, version "3": ONE logical file = TWO objects
//! (DESIGN-V2.md §1/§5 — the sidecar split).
//!
//! - The DATA object (`.vix`): a puffin container holding the `docs` blob, the H2 per-column
//!   chunk-`stats` blob (splice-through-passthrough pruning metadata, H2/DESIGN §4) and the
//!   data-descriptive footer properties (`row_count`, `row_group_size`, `zone_map`, `row_order`,
//!   `oversize_skips`, and the `columns` field list with per-column present-row counts — field
//!   presence is data-descriptive and feeds pruning). Any docs scan needs nothing else.
//! - The INDEX sidecar (`.vxi`, same key with the extension swapped): a puffin container holding
//!   the `dict`/`dict_blocks`/`terms` blobs, the optional out-of-row `plist` region and the
//!   per-file `bloom` blob, plus the index-descriptive properties (`term_count`, `tokenizer`,
//!   `dict_layout`, `key_layout`, `plist_min_docs`, and the `fields`/`partial_fields` term-marking
//!   metadata). Index-off files (#40/#42 L0) have NO sidecar; a reader opened without one serves
//!   the existing "no usable index → filter-back scan" semantics.
//!
//! Blobs:
//! - `dict`/`dict_blocks`: the block dictionary — a restart-compressed block INDEX plus the raw
//!   concatenated prefix-compressed key blocks it addresses (readers range-fetch single blocks).
//! - `terms`: one row per composite term — `doc_count`, `postings` (delta + bitpacked doc ids).
//!   **Key terms** (field id `0xFFFF` reserved as the key marker; real field ids cap at `0xFFFE`,
//!   overflowing fields degrade to `partial_fields`) record which docs have a non-null value at
//!   each flattened path, and **dense elision** writes an empty postings blob for any term whose
//!   `doc_count` equals the file's row count (the reader synthesizes the all-ones bitmap).
//! - `docs`:  one row per record — `_timestamp: i64`, one native column per PRESENT field (v2
//!   all-present-columns, DESIGN §2), the caller-supplied `_source: utf8` (always) and `_original:
//!   utf8` (opt-in). Its columns are compressed with the BtrBlocks sampler *plus* the zstd/pco
//!   compact schemes (`_source`/`_original` JSON text lands on zstd), and its chunks — the
//!   decompression unit of a matched-row point read — are sized by the
//!   [`VixWriterOptions::docs_chunk_bytes`] byte budget, not the data row-group size.
//!
//! Composite dictionary keys are field-major `{field_id u16 BE}{token}`
//! (`key_layout = "fid_v2"`) so one dictionary serves per-field
//! exact/prefix/regex/fuzzy lookups, cross-field full-text scans and
//! key-existence lookups. The per-file bloom blob's hash form stays pinned
//! to v1 `{value}\0{fid BE}` (see [`bloom`]) so the group `.bf` assembly
//! remains a byte transpose.
//!
//! Entry points: [`VixWriter`] builds BOTH byte streams from arrow record
//! batches ([`VixWriter::new`] + [`VixWriter::push_batch_with_source`] /
//! [`VixWriter::push_docs_rows`]; [`VixWriter::finish`] returns
//! `(data_bytes, Option<index_bytes>)`); [`VixReader`] opens the pair
//! ([`VixReader::open_with_index`]) and evaluates [`VixQuery`]s into
//! per-document [`arrow::buffer::BooleanBuffer`] bitmaps.
//! [`VixReader::open_ranged_with_index`] / [`VixDocs::open_ranged`] open the
//! same structures over [`VixRangeSource`]s (an async `fetch(range)` closure
//! per object), turning query evaluation into a handful of small range
//! fetches instead of whole-object downloads — the sidecar source is only
//! touched by index reads, the data source only by docs reads.
//!
//! The `version` puffin property (on BOTH objects) is the format's one
//! evolution discriminator: readers require exactly the supported value
//! (`"3"`) and reject anything else at open with a clear error; every other
//! unknown property is ignored. Blobs are matched by `(blob_tag, type id)`;
//! unknown blobs are ignored, so the envelopes tolerate additions.

pub mod bloom;
mod clustered;
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
mod stats;
mod term_accumulator;
pub mod test_support;
mod tokenizer;
mod writer;

pub use container::{
    BloomEncodingCensus, DEFAULT_TAIL_FETCH_BYTES, RowOrder, VixOutput, ZoneEntry,
    region_row_ranges, set_tail_fetch_size,
};
pub use docs::{
    BoundValue, ColumnBound, DocsWidenPlan, EncodedDocsChunk, NumScalar, VixDocs, cmp_i128_vs_f64,
    cmp_num_vs_bound, docs_widen_plan,
};
pub use error::VixError;
pub use merge::DocIdMap;
pub use numeric::{
    canonical_bool_text, canonical_f32_text, canonical_f64_text, canonical_i64_text,
    canonical_number_text, canonical_u64_text, is_numeric_value_token, numeric_value_token,
};
pub use query::VixQuery;
pub use reader::{DocsDictChunk, FieldValueCounts, PlistCursor, TermVisitor, VixReader, ZoneChunk};
pub use source::{BytesRangeSource, VixRangeSource};
pub use stats::{
    ColumnChunkStat, ColumnChunkStats, DEFAULT_STATS_MAX_BYTES, DEFAULT_STATS_MIN_DENSITY,
    FileColumnStats, SpliceableStats, StatValue, validate_spliceable,
};
pub use tokenizer::o2_tokenize;
pub use writer::{
    BloomOnlyHasher, DEFAULT_DOCS_CHUNK_BYTES, DEFAULT_DOCS_CHUNK_MAX_ROWS, ID_COL_NAME,
    ORIGINAL_DATA_COL_NAME, RawValueSink, SOURCE_COL_NAME, SOURCE_RENAMED_COL_NAME,
    TIMESTAMP_COL_NAME, VixWriter, VixWriterOptions, VixWriterStats, docs_schema_mismatch_reason,
    is_value_indexed_type, resolve_auto_bloom_only,
};

#[cfg(test)]
mod tests;
