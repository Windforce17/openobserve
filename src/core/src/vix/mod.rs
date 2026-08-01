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

//! Write path of the `.vix` **core files** — the single object per data
//! unit holding the records (`docs` blob) together with the inverted index.
//!
//! [`core_writer`] builds core files at WAL→storage persist
//! (`write_core_file_from_tables`) and merges them at compaction
//! (`merge_core_files`: index merged straight from the inputs' term
//! dictionaries, with `merge_core_files_rebuild` — terms re-derived from
//! `_source` — as the fallback); [`source`] synthesizes the per-row
//! `_source` JSON.
//!
//! The legacy v1 sidecar-index builder (`create_vix_index`, which wrote a
//! separate `.vix` index object next to a parquet data file) was removed —
//! logs/traces are written as core files unconditionally, and no other
//! stream type builds an inverted index.

pub mod core_writer;
