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

//! Per-file collectors for the [`IndexOptimizeMode`] aggregation fast paths,
//! evaluated over the docs-blob columns of one `.vix` file.
//!
//! Every column access goes through the [`VixReader`] chokepoint
//! (`read_docs_column` / `read_docs_column_rows`) over the `docs` blob. The
//! collectors implement the aggregate contracts:
//!
//! - [`simple_select`]: the file's top-`limit` matched rows by `_timestamp`, as exact `(_timestamp,
//!   doc_id)` candidates for the global cross-file merge;
//! - [`simple_histogram`]: fixed-width `_timestamp` bucket counts (`num_buckets` long, zeros
//!   included), with `ts_offset` shifting the bucket origin;
//! - [`simple_multi_histogram`]: complete `(bucket, breakdown value, count)` rows within `[min,
//!   max)`;
//! - [`simple_top_n`]: complete bounded `GROUP BY field(s)` partial counts. Unsupported SQL values
//!   or exhausted budgets refuse the entire file to the exact scan, never a local top-k;
//! - [`simple_distinct`]: the first/last `limit` distinct values of a field over the matched rows,
//!   in byte order (= the old term-dictionary order).
//!
//! Unfiltered full-range single-field TopN/Distinct additionally try the term
//! dictionary before reading docs columns. Files without complete dictionary
//! counts or the required stored column use the precise DataFusion scan; this
//! module does not materialize `_source` as another aggregation engine.
//!
//! [`IndexOptimizeMode`]: config::meta::inverted_index::IndexOptimizeMode

use std::collections::{BTreeSet, HashSet};

use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray},
    buffer::BooleanBuffer,
    compute::cast,
    datatypes::DataType,
};
use config::{TIMESTAMP_COL_NAME, meta::inverted_index::MAX_SIMPLE_TOPN_FIELDS};
use hashbrown::HashMap;
use vortex_index::VixReader;

use super::result::MinMaxValue;

/// An expected per-file refusal, not an I/O failure. The caller must discard
/// this file's partial work and contribute it exactly once through DataFusion.
#[derive(Debug)]
pub(super) struct AggregateFallback(pub &'static str);

impl std::fmt::Display for AggregateFallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for AggregateFallback {}

/// Whether the file can serve reads of `name` through the docs-column
/// chokepoint (a `docs`-blob column). Files that predate a
/// `column_store_fields` setting lack the column and must fall back to a
/// scan. On a ranged reader resolving the docs schema may fetch the
/// docs-blob footer (hence fallible).
pub(super) fn docs_column_available(reader: &VixReader, name: &str) -> anyhow::Result<bool> {
    Ok(reader.docs_schema()?.field_with_name(name).is_ok())
}

/// The first non-`_timestamp` field of the optimize mode that this file
/// cannot serve from its docs columns (the file predates the
/// `column_store_fields` setting), if any — such a file falls back to the
/// scan path.
///
/// `_source` is treated as always-missing here even though it IS a docs
/// column: serving a group-by/distinct over it through the fast paths
/// would materialize the ENTIRE column (every row's full JSON) into one
/// arrow array (`read_docs_column`), silently — a multi-GB allocation and
/// an i32-offset-overflow hazard on big files. The scan branch streams it
/// chunk by chunk instead.
pub(super) fn missing_docs_column(
    reader: &VixReader,
    rule: &config::meta::inverted_index::IndexOptimizeMode,
) -> anyhow::Result<Option<String>> {
    for field in rule.referenced_fields() {
        if field == vortex_index::SOURCE_COL_NAME || !docs_column_available(reader, &field)? {
            return Ok(Some(field));
        }
    }
    Ok(None)
}

/// Read the `_timestamp` values of the given rows (ascending row order).
fn read_timestamps(reader: &VixReader, rows: Option<&[u64]>) -> anyhow::Result<Int64Array> {
    let column = match rows {
        Some(rows) => reader.read_docs_column_rows(TIMESTAMP_COL_NAME, rows)?,
        None => reader.read_docs_column(TIMESTAMP_COL_NAME)?,
    };
    let column = cast(&column, &DataType::Int64)
        .map_err(|e| anyhow::anyhow!("{TIMESTAMP_COL_NAME} is not an i64 column: {e}"))?;
    Ok(column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("{TIMESTAMP_COL_NAME} is not an i64 column"))?
        .clone())
}

/// Read one docs column of the given rows as strings (nulls preserved;
/// non-string stored types are stringified via an arrow cast, matching the
/// old collectors' `Key::{I64,U64,F64}::to_string` behavior).
fn read_column_strings(
    reader: &VixReader,
    name: &str,
    rows: Option<&[u64]>,
) -> anyhow::Result<StringArray> {
    let column: ArrayRef = match rows {
        Some(rows) => reader.read_docs_column_rows(name, rows)?,
        None => reader.read_docs_column(name)?,
    };
    let column = cast(&column, &DataType::Utf8)
        .map_err(|e| anyhow::anyhow!("column {name:?} cannot be read as strings: {e}"))?;
    Ok(column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("column {name:?} cannot be read as strings"))?
        .clone())
}

/// How a collector reads docs columns for the matched rows of a bitmap.
///
/// Docs chunks hold thousands of rows and are the decompression unit — a
/// point read decodes the whole chunk of its first selected row anyway, so
/// beyond a small selectivity the full column read filtered through the
/// bitmap is strictly cheaper than materializing an index list and taking
/// by indices.
enum RowAccess {
    /// Read whole columns; every row matches.
    AllRows,
    /// Read whole columns; visit only the bitmap's set positions.
    Filtered,
    /// Point-read the listed rows only (highly selective bitmaps).
    Rows(Vec<u64>),
}

/// Point reads win only for needle-grade selectivity (< ~2% of rows).
const POINT_READ_DENOMINATOR: usize = 50;

impl RowAccess {
    fn plan(bitmap: &BooleanBuffer) -> Self {
        let matched = bitmap.count_set_bits();
        if matched == bitmap.len() {
            RowAccess::AllRows
        } else if matched.saturating_mul(POINT_READ_DENOMINATOR) > bitmap.len() {
            RowAccess::Filtered
        } else {
            RowAccess::Rows(bitmap.set_indices().map(|i| i as u64).collect())
        }
    }

    /// The explicit row list for the column read, `None` for full reads.
    fn point_rows(&self) -> Option<&[u64]> {
        match self {
            RowAccess::Rows(rows) => Some(rows),
            _ => None,
        }
    }

    /// Visit the positions of the read-back arrays that belong to matched
    /// rows: every position for `AllRows`/`Rows` (the read already
    /// restricted them), the bitmap's set positions for `Filtered`.
    fn for_each_position(&self, bitmap: &BooleanBuffer, len: usize, mut f: impl FnMut(usize)) {
        match self {
            RowAccess::Filtered => bitmap.set_indices().for_each(&mut f),
            _ => (0..len).for_each(&mut f),
        }
    }
}

/// SimpleSelect: the file's top-`limit` matched rows ordered by `_timestamp`
/// (`ascend` = `ORDER BY _timestamp ASC`), as `(_timestamp, doc_id)` pairs,
/// timestamp-ordered best-first.
///
/// Sorted files (`row_order` ts_desc — every historical file): docs rows
/// are stored `ORDER BY _timestamp DESC`, so ascending doc ids are
/// descending timestamps and the best rows are the first (DESC) / last
/// (ASC) set bits. The exact per-candidate timestamps are read back for the
/// cross-file merge, and the final sort is on those timestamps, so a
/// not-perfectly-sorted file only costs accuracy at the truncation boundary
/// (as before).
///
/// #51c-c CONCAT-order files are NOT globally sorted — doc-id position says
/// nothing about recency, so the positional shortcut would return wrong
/// candidates (and, through the pruner merge, wrong query results). M4
/// (§6.2): a concat file with a PROVEN region decomposition narrows the
/// work piecewise — within a region rows ARE ts_desc, so each region's
/// best `limit` matched rows are its first (DESC) / last (ASC) set bits,
/// and only that candidate union's `_timestamp`s are read (≤ regions ×
/// limit values instead of every matched row) before the exact value-based
/// top-`limit`. A concat file WITHOUT proven regions reads every matched
/// row's `_timestamp` (the by-value fallback) — exact either way.
pub(super) fn simple_select(
    reader: &VixReader,
    bitmap: &BooleanBuffer,
    limit: usize,
    ascend: bool,
) -> anyhow::Result<Vec<(i64, u32)>> {
    let matched = bitmap.count_set_bits();
    let take = limit.min(matched);
    if take == 0 {
        return Ok(Vec::new());
    }
    if !reader.row_order().is_ts_desc() {
        if let Some(regions) = reader.ts_desc_row_ranges() {
            return simple_select_piecewise(reader, bitmap, take, ascend, &regions);
        }
        return simple_select_by_value(reader, bitmap, take, ascend);
    }
    // best rows: newest first for DESC = the first set bits (rows are stored
    // newest-first); oldest first for ASC = the last set bits
    let rows: Vec<u64> = if ascend {
        bitmap
            .set_indices()
            .skip(matched - take)
            .map(|i| i as u64)
            .collect()
    } else {
        bitmap.set_indices().take(take).map(|i| i as u64).collect()
    };
    let timestamps = read_timestamps(reader, Some(&rows))?;
    if timestamps.null_count() > 0 {
        return Err(anyhow::anyhow!(
            "missing {TIMESTAMP_COL_NAME} value in select candidates"
        ));
    }
    let mut candidates: Vec<(i64, u32)> = rows
        .iter()
        .enumerate()
        .map(|(i, &row)| (timestamps.value(i), row as u32))
        .collect();
    // timestamp-ordered best-first, as the pruner merge requires
    if ascend {
        candidates.sort_unstable();
    } else {
        candidates.sort_unstable_by(|a, b| b.cmp(a));
    }
    Ok(candidates)
}

/// [`simple_select`] for files whose stored order proves nothing (#51c-c
/// concat files without proven regions): read every matched row's
/// `_timestamp` and select the true top-`take` by VALUE (partial select,
/// then the best-first sort the pruner merge requires). Doc-id order never
/// enters the result.
fn simple_select_by_value(
    reader: &VixReader,
    bitmap: &BooleanBuffer,
    take: usize,
    ascend: bool,
) -> anyhow::Result<Vec<(i64, u32)>> {
    let rows: Vec<u64> = bitmap.set_indices().map(|i| i as u64).collect();
    select_top_by_value(reader, rows, take, ascend)
}

/// §6.2 M4: [`simple_select`] over a PROVEN region decomposition — each
/// region is internally ts_desc, so its best `take` matched rows are its
/// first (DESC) / last (ASC) set bits; the global exact top-`take` lives in
/// that candidate union (a region contributes a positional prefix/suffix of
/// its matched rows to any timestamp top-k). Only the candidates'
/// `_timestamp`s are read. Equal timestamps across the per-region cut may
/// resolve to different doc ids than the full by-value walk — an equally
/// correct tie subset (ORDER BY `_timestamp` constrains only timestamps).
fn simple_select_piecewise(
    reader: &VixReader,
    bitmap: &BooleanBuffer,
    take: usize,
    ascend: bool,
    regions: &[std::ops::Range<u64>],
) -> anyhow::Result<Vec<(i64, u32)>> {
    let mut candidates: Vec<u64> = Vec::new();
    for region in regions {
        let start = region.start as usize;
        let len = (region.end - region.start) as usize;
        let window = bitmap.slice(start, len);
        let matched = window.count_set_bits();
        if matched == 0 {
            continue;
        }
        let keep = take.min(matched);
        if ascend {
            // oldest = the region's LAST set bits
            candidates.extend(
                window
                    .set_indices()
                    .skip(matched - keep)
                    .map(|i| (start + i) as u64),
            );
        } else {
            // newest = the region's FIRST set bits
            candidates.extend(window.set_indices().take(keep).map(|i| (start + i) as u64));
        }
    }
    select_top_by_value(reader, candidates, take, ascend)
}

/// Exact top-`take` of `rows` by `_timestamp` VALUE (partial select, then
/// the best-first sort the pruner merge requires); equal timestamps prefer
/// the smaller doc id among the given rows.
fn select_top_by_value(
    reader: &VixReader,
    rows: Vec<u64>,
    take: usize,
    ascend: bool,
) -> anyhow::Result<Vec<(i64, u32)>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let timestamps = read_timestamps(reader, Some(&rows))?;
    if timestamps.null_count() > 0 {
        return Err(anyhow::anyhow!(
            "missing {TIMESTAMP_COL_NAME} value in select candidates"
        ));
    }
    let mut candidates: Vec<(i64, u32)> = rows
        .iter()
        .enumerate()
        .map(|(i, &row)| (timestamps.value(i), row as u32))
        .collect();
    // exact top-`take` by timestamp; equal timestamps prefer the smaller
    // doc id (deterministic; a tie subset is equally correct for ORDER BY
    // _timestamp, which constrains only the timestamp)
    let desc = |a: &(i64, u32), b: &(i64, u32)| b.0.cmp(&a.0).then(a.1.cmp(&b.1));
    let take = take.min(candidates.len());
    if take < candidates.len() {
        if ascend {
            candidates.select_nth_unstable(take - 1);
        } else {
            candidates.select_nth_unstable_by(take - 1, desc);
        }
        candidates.truncate(take);
    }
    if ascend {
        candidates.sort_unstable();
    } else {
        candidates.sort_unstable_by(desc);
    }
    Ok(candidates)
}

/// Return the one histogram bucket containing every row in `reader`, when
/// the zone table proves such a bucket exists. This is checked before a
/// condition's postings are opened: a fully covered file can then contribute
/// `count(condition)` directly instead of materializing row ids or reading
/// `_timestamp`.
pub(super) fn whole_file_histogram_bucket(
    reader: &VixReader,
    min_value: i64,
    bucket_width: u64,
    num_buckets: usize,
    ts_offset: i64,
) -> anyhow::Result<Option<usize>> {
    if num_buckets == 0 {
        return Ok(None);
    }
    let Some(chunks) = reader.zone_chunks() else {
        return Ok(None);
    };
    let Some(first) = chunks.first() else {
        return Ok(None);
    };
    let mut ts_min = first.ts_min;
    let mut ts_max = first.ts_max;
    for chunk in &chunks[1..] {
        ts_min = ts_min.min(chunk.ts_min);
        ts_max = ts_max.max(chunk.ts_max);
    }
    histogram_range_bucket(
        ts_min,
        ts_max,
        min_value,
        bucket_width,
        num_buckets,
        ts_offset,
    )
}

/// SimpleHistogram: count the matched rows into `num_buckets` fixed-width
/// `_timestamp` buckets starting at `min_value - ts_offset` (the old
/// collector shifted the range instead of the per-doc values). The returned
/// vector is always `num_buckets` long, zeros included.
pub(super) fn simple_histogram(
    reader: &VixReader,
    bitmap: &BooleanBuffer,
    min_value: i64,
    bucket_width: u64,
    num_buckets: usize,
    ts_offset: i64,
) -> anyhow::Result<Vec<u64>> {
    let mut counts = vec![0u64; num_buckets];
    if num_buckets == 0 || bitmap.count_set_bits() == 0 {
        return Ok(counts);
    }
    let width = i64::try_from(bucket_width.max(1))
        .map_err(|_| anyhow::anyhow!("histogram bucket width overflows i64: {bucket_width}"))?;
    let origin = min_value - ts_offset;

    // Zone-map fast path: a chunk whose whole `[ts_min, ts_max]` span lands in
    // one bucket contributes its matched-row count to that bucket without
    // decoding `_timestamp` — the bucket of every row in the chunk is that
    // bucket regardless of WHICH rows matched, so any partial bitmap folds
    // too. A chunk with no matched rows is skipped; only the chunks that
    // straddle a bucket edge are decoded, for their matched rows only.
    // (Docs are stored ORDER BY _timestamp DESC, so chunk time spans are
    // narrow and nearly all chunks fold; the fold itself only relies on the
    // chunk's own ts_min/ts_max, not on global sortedness.)
    if let Some(chunks) = reader.zone_chunks() {
        let mut boundary_rows: Vec<u64> = Vec::new();
        #[cfg(test)]
        let mut decoded_chunks = 0usize;
        for chunk in chunks {
            let start = chunk.row_offset as usize;
            let count = chunk.row_count as usize;
            let window = bitmap.slice(start, count);
            let matched = window.count_set_bits();
            if matched == 0 {
                continue; // no matched rows in this chunk
            }
            match chunk_single_bucket(chunk.ts_min, chunk.ts_max, origin, width, num_buckets) {
                Some(Some(bucket)) => {
                    counts[bucket] += matched as u64;
                    continue;
                }
                Some(None) => continue, // whole chunk outside the range
                None => {}              // straddles bucket edges: decode below
            }
            boundary_rows.extend(window.set_indices().map(|i| (start + i) as u64));
            #[cfg(test)]
            {
                decoded_chunks += 1;
            }
        }
        #[cfg(test)]
        tests::record_decoded_histogram_chunks(decoded_chunks);
        if !boundary_rows.is_empty() {
            let timestamps = read_timestamps(reader, Some(&boundary_rows))?;
            for i in 0..timestamps.len() {
                accumulate_bucket(&mut counts, &timestamps, i, origin, width, num_buckets);
            }
        }
        return Ok(counts);
    }

    // Decode path (files with no zone table): read `_timestamp` and bucket
    // each matched row.
    let access = RowAccess::plan(bitmap);
    let timestamps = read_timestamps(reader, access.point_rows())?;
    access.for_each_position(bitmap, timestamps.len(), |i| {
        accumulate_bucket(&mut counts, &timestamps, i, origin, width, num_buckets);
    });
    Ok(counts)
}

/// [`simple_histogram`] for a single dense out-of-row term, WITHOUT the
/// bitmap: each zone chunk's matched count is a skip-table rank diff
/// (`rank(chunk_end) - rank(chunk_start)`), single-bucket chunks fold that
/// count directly, and only bucket/window-straddling chunks decode their
/// touched groups (then their matched rows' `_timestamp`s). Correct
/// regardless of global `_timestamp` order — the fold relies only on
/// per-chunk bounds, exactly like the bitmap fold.
///
/// `window` is the query's `[start_time, end_time)` and must be clamped
/// EXPLICITLY: the grid's last bucket can overshoot the window (ceil
/// sizing), and the bitmap path clamps via the time-range AND — a
/// grid-drop-only ranked path would overcount the overshoot region
/// (caught by the dual-build equality test). Callers must have a zone
/// table (`reader.zone_chunks()` is Some).
pub(super) fn ranked_simple_histogram(
    reader: &VixReader,
    cursor: &vortex_index::PlistCursor,
    min_value: i64,
    bucket_width: u64,
    num_buckets: usize,
    ts_offset: i64,
    window: (i64, i64),
) -> anyhow::Result<Vec<u64>> {
    let mut counts = vec![0u64; num_buckets];
    if num_buckets == 0 || cursor.doc_count() == 0 {
        return Ok(counts);
    }
    let width = i64::try_from(bucket_width.max(1))
        .map_err(|_| anyhow::anyhow!("histogram bucket width overflows i64: {bucket_width}"))?;
    let origin = min_value - ts_offset;
    let (win_start, win_end) = window;
    let chunks = reader
        .zone_chunks()
        .ok_or_else(|| anyhow::anyhow!("ranked histogram requires a zone table"))?;

    let mut boundaries = Vec::with_capacity(chunks.len() + 1);
    boundaries.push(0);
    for chunk in chunks {
        boundaries.push(
            u32::try_from(chunk.row_offset + chunk.row_count)
                .map_err(|_| anyhow::anyhow!("chunk row end overflows u32"))?,
        );
    }
    // One batched ranged read for all distinct skip groups. Calling `rank`
    // per boundary would serialize one object-store request per chunk.
    let ranks = cursor.ranks(&boundaries)?;
    let mut boundary_rows: Vec<u64> = Vec::new();
    for (index, chunk) in chunks.iter().enumerate() {
        let start = boundaries[index];
        let end = boundaries[index + 1];
        let matched = ranks[index + 1].checked_sub(ranks[index]).ok_or_else(|| {
            anyhow::anyhow!("postings ranks decreased across chunk {start}..{end}")
        })?;
        if matched == 0 {
            continue;
        }
        // window first (inclusive chunk bounds vs half-open window)
        if chunk.ts_max < win_start || chunk.ts_min >= win_end {
            continue; // whole chunk outside the query window
        }
        let fully_in_window = chunk.ts_min >= win_start && chunk.ts_max < win_end;
        if fully_in_window {
            match chunk_single_bucket(chunk.ts_min, chunk.ts_max, origin, width, num_buckets) {
                Some(Some(bucket)) => {
                    counts[bucket] += matched;
                    continue;
                }
                Some(None) => continue, // whole chunk outside the grid
                None => {}              // straddles bucket edges: decode below
            }
        }
        // bucket-straddling or window-straddling: decode the chunk's rows
        cursor.for_each_in_range(start, end, |row| {
            boundary_rows.push(u64::from(row));
            Ok(())
        })?;
    }
    if !boundary_rows.is_empty() {
        let timestamps = read_timestamps(reader, Some(&boundary_rows))?;
        for i in 0..timestamps.len() {
            if timestamps.is_valid(i) {
                let ts = timestamps.value(i);
                if ts < win_start || ts >= win_end {
                    continue; // outside the query window (grid may overshoot)
                }
            }
            accumulate_bucket(&mut counts, &timestamps, i, origin, width, num_buckets);
        }
    }
    Ok(counts)
}

/// Windowed count for a single dense out-of-row term, without the bitmap:
/// chunks fully inside `[start_time, end_time)` contribute a rank diff,
/// disjoint chunks contribute nothing, and window-straddling chunks decode
/// their touched groups + those rows' `_timestamp`s. The exact equivalent of
/// `(condition bitmap & timestamp_range).count_set_bits()`.
pub(super) fn ranked_count_in_window(
    reader: &VixReader,
    cursor: &vortex_index::PlistCursor,
    start_time: i64,
    end_time: i64,
) -> anyhow::Result<u64> {
    if cursor.doc_count() == 0 || start_time >= end_time {
        return Ok(0);
    }
    let chunks = reader
        .zone_chunks()
        .ok_or_else(|| anyhow::anyhow!("ranked count requires a zone table"))?;
    let mut count = 0u64;
    let mut boundary_rows: Vec<u64> = Vec::new();
    let mut boundaries = Vec::with_capacity(chunks.len() + 1);
    boundaries.push(0);
    for chunk in chunks {
        boundaries.push(
            u32::try_from(chunk.row_offset + chunk.row_count)
                .map_err(|_| anyhow::anyhow!("chunk row end overflows u32"))?,
        );
    }
    let ranks = cursor.ranks(&boundaries)?;
    for (index, chunk) in chunks.iter().enumerate() {
        let start = boundaries[index];
        let end = boundaries[index + 1];
        let matched = ranks[index + 1].checked_sub(ranks[index]).ok_or_else(|| {
            anyhow::anyhow!("postings ranks decreased across chunk {start}..{end}")
        })?;
        if matched == 0 {
            continue;
        }
        // inclusive chunk bounds vs [start_time, end_time)
        if chunk.ts_min >= start_time && chunk.ts_max < end_time {
            count += matched;
        } else if chunk.ts_max < start_time || chunk.ts_min >= end_time {
            // disjoint
        } else {
            cursor.for_each_in_range(start, end, |row| {
                boundary_rows.push(u64::from(row));
                Ok(())
            })?;
        }
    }
    if !boundary_rows.is_empty() {
        let timestamps = read_timestamps(reader, Some(&boundary_rows))?;
        for i in 0..timestamps.len() {
            if timestamps.is_valid(i) {
                let ts = timestamps.value(i);
                if ts >= start_time && ts < end_time {
                    count += 1;
                }
            }
        }
    }
    Ok(count)
}

/// M16: the per-column chunk-stats table of `field`, validated to align
/// 1:1 with the zone table (the stats writer emits one row per zone entry;
/// anything else is untrustworthy — fail open). `None` = no zone table, no
/// stats blob, or no rows for this column (density-gated).
fn column_stats_table<'a>(
    reader: &'a VixReader,
    field: &str,
) -> Option<(
    &'a [vortex_index::ZoneChunk],
    &'a vortex_index::ColumnChunkStats,
)> {
    let zone = reader.zone_chunks()?;
    let table = reader.column_chunk_stats()?.columns.get(field)?;
    (table.chunks.len() == zone.len()).then_some((zone, table))
}

/// M16 count(field): the non-null count of one docs column over the file —
/// stats-answered wherever per-chunk presence counts exist, decode
/// elsewhere; EXACTLY equal to the full-decode count by construction
/// (stats are exact: immutable files, no deletes, splice-pinned).
///
/// - `bitmap` (conditioned evaluations): count the valid values over the matched rows — the normal
///   path, no stats shortcut (a subset of a chunk proves nothing).
/// - no bitmap + no `window` (condition-all, file fully inside the query range): the file-level
///   presence count from the `columns` property answers outright (v3 writers stamp it; merges sum
///   it); unknown (M1 entry) falls to the full decode.
/// - no bitmap + `window` (condition-all, straddling file): chunks fully inside the window
///   contribute their stats row's `present`; boundary chunks — and chunks without a stats row —
///   decode their rows (validity + `_timestamp` window check).
pub(super) fn count_field(
    reader: &VixReader,
    field: &str,
    window: Option<(i64, i64)>,
    bitmap: Option<&BooleanBuffer>,
) -> anyhow::Result<u64> {
    // conditioned: the bitmap already folds the window clamp
    if let Some(bitmap) = bitmap {
        if bitmap.count_set_bits() == 0 {
            return Ok(0);
        }
        let access = RowAccess::plan(bitmap);
        let column = match access.point_rows() {
            Some(rows) => reader.read_docs_column_rows(field, rows)?,
            None => reader.read_docs_column(field)?,
        };
        let mut count = 0u64;
        access.for_each_position(bitmap, column.len(), |i| {
            if column.is_valid(i) {
                count += 1;
            }
        });
        return Ok(count);
    }
    let Some((start, end)) = window else {
        // fully covered: the file-level presence count IS the answer
        if let Some((_, Some(present))) = reader
            .column_presence()
            .iter()
            .find(|(name, _)| name == field)
        {
            return Ok(*present);
        }
        // M1-era unknown presence: full decode
        let column = reader.read_docs_column(field)?;
        return Ok((column.len() - column.null_count()) as u64);
    };
    // straddling: fold covered chunks from stats, decode the rest
    let stats = column_stats_table(reader, field);
    let mut count = 0u64;
    let mut decode_rows: Vec<u64> = Vec::new();
    match reader.zone_chunks() {
        Some(chunks) => {
            for (index, chunk) in chunks.iter().enumerate() {
                if chunk.ts_max < start || chunk.ts_min >= end {
                    continue; // whole chunk outside the window
                }
                let fully_inside = chunk.ts_min >= start && chunk.ts_max < end;
                if fully_inside
                    && let Some((_, table)) = &stats
                    && let Some(Some(stat)) = table.chunks.get(index)
                {
                    count += stat.present;
                    continue;
                }
                decode_rows.extend(chunk.row_offset..chunk.row_offset + chunk.row_count);
            }
        }
        None => {
            // no zone table (M1 files): decode everything
            decode_rows.extend(0..reader.row_count());
        }
    }
    if !decode_rows.is_empty() {
        let column = reader.read_docs_column_rows(field, &decode_rows)?;
        let timestamps = read_timestamps(reader, Some(&decode_rows))?;
        for i in 0..column.len() {
            if column.is_valid(i) && timestamps.is_valid(i) {
                let ts = timestamps.value(i);
                if ts >= start && ts < end {
                    count += 1;
                }
            }
        }
    }
    Ok(count)
}

/// M16: the numeric family of one docs column for the min/max arm —
/// `None` for non-numeric stored types (string stats are prefix-bounded
/// and must NEVER answer; the caller degrades the file to the scan branch).
pub(super) fn min_max_family(
    reader: &VixReader,
    field: &str,
) -> anyhow::Result<Option<MinMaxFamily>> {
    let schema = reader.docs_schema()?;
    let Ok(field) = schema.field_with_name(field) else {
        return Ok(None);
    };
    Ok(match field.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Some(MinMaxFamily::I64)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Some(MinMaxFamily::U64)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Some(MinMaxFamily::F64),
        _ => None,
    })
}

/// The numeric family a min/max evaluation folds in (the docs column's
/// stored family; the stats table's tag must agree).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum MinMaxFamily {
    I64,
    U64,
    F64,
}

impl MinMaxFamily {
    fn stats_tag(&self) -> &'static str {
        match self {
            MinMaxFamily::I64 => "i64",
            MinMaxFamily::U64 => "u64",
            MinMaxFamily::F64 => "f64",
        }
    }

    /// The stats bound as a fold value; `None` for cross-tag values
    /// (defensive — the tag gate upstream should prevent it) and NaN.
    fn stat_value(&self, value: &vortex_index::StatValue) -> Option<MinMaxValue> {
        use vortex_index::StatValue;
        match (self, value) {
            (MinMaxFamily::I64, StatValue::I64(v)) => Some(MinMaxValue::I64(*v)),
            (MinMaxFamily::U64, StatValue::U64(v)) => Some(MinMaxValue::U64(*v)),
            (MinMaxFamily::F64, StatValue::F64(v)) => (!v.is_nan()).then_some(MinMaxValue::F64(*v)),
            _ => None,
        }
    }

    /// Cast one decoded arrow column to the family's widest type for the
    /// per-row fold.
    fn cast_column(&self, column: &ArrayRef) -> anyhow::Result<ArrayRef> {
        let target = match self {
            MinMaxFamily::I64 => DataType::Int64,
            MinMaxFamily::U64 => DataType::UInt64,
            MinMaxFamily::F64 => DataType::Float64,
        };
        cast(column, &target).map_err(|e| anyhow::anyhow!("column cast for min/max: {e}"))
    }

    /// Row `i` of a family-cast column as a fold value (`None` = null, or a
    /// NaN float — excluded from folds exactly like the stats builder).
    fn row_value(&self, column: &ArrayRef, i: usize) -> Option<MinMaxValue> {
        if column.is_null(i) {
            return None;
        }
        match self {
            MinMaxFamily::I64 => column
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| MinMaxValue::I64(a.value(i))),
            MinMaxFamily::U64 => column
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .map(|a| MinMaxValue::U64(a.value(i))),
            MinMaxFamily::F64 => column
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .and_then(|a| {
                    let v = a.value(i);
                    (!v.is_nan()).then_some(MinMaxValue::F64(v))
                }),
        }
    }
}

/// M16 min/max(field) for NUMERIC columns — per-chunk EXACT min/max stats
/// fold for covered chunks, decode for boundary/stats-less chunks;
/// `None` result = no non-null (non-NaN) value matched. Same
/// window/bitmap contract as [`count_field`]. String columns never reach
/// here (the caller gates on [`min_max_family`]).
pub(super) fn min_max_field(
    reader: &VixReader,
    field: &str,
    family: MinMaxFamily,
    is_max: bool,
    window: Option<(i64, i64)>,
    bitmap: Option<&BooleanBuffer>,
) -> anyhow::Result<Option<MinMaxValue>> {
    let mut best: Option<MinMaxValue> = None;
    let fold = |value: MinMaxValue, best: &mut Option<MinMaxValue>| {
        *best = Some(match best.take() {
            Some(current) => current.fold(value, is_max),
            None => value,
        });
    };
    // conditioned: decode the matched rows (a chunk subset proves nothing)
    if let Some(bitmap) = bitmap {
        if bitmap.count_set_bits() == 0 {
            return Ok(None);
        }
        let access = RowAccess::plan(bitmap);
        let column = match access.point_rows() {
            Some(rows) => reader.read_docs_column_rows(field, rows)?,
            None => reader.read_docs_column(field)?,
        };
        let column = family.cast_column(&column)?;
        access.for_each_position(bitmap, column.len(), |i| {
            if let Some(value) = family.row_value(&column, i) {
                fold(value, &mut best);
            }
        });
        return Ok(best);
    }
    // condition-all: stats fold + boundary/unknown decode. `_timestamp`'s
    // stats ARE the zone table (per-chunk exact inclusive min/max of the
    // stored values; the stats blob deliberately excludes the column).
    let ts_zone_served = field == TIMESTAMP_COL_NAME && family == MinMaxFamily::I64;
    let stats =
        column_stats_table(reader, field).filter(|(_, table)| table.tag == family.stats_tag());
    let mut decode_rows: Vec<u64> = Vec::new();
    match reader.zone_chunks() {
        Some(chunks) => {
            for (index, chunk) in chunks.iter().enumerate() {
                if let Some((start, end)) = window
                    && (chunk.ts_max < start || chunk.ts_min >= end)
                {
                    continue; // whole chunk outside the window
                }
                let fully_inside =
                    window.is_none_or(|(start, end)| chunk.ts_min >= start && chunk.ts_max < end);
                if fully_inside {
                    if ts_zone_served {
                        fold(
                            MinMaxValue::I64(if is_max { chunk.ts_max } else { chunk.ts_min }),
                            &mut best,
                        );
                        continue;
                    }
                    if let Some((_, table)) = &stats
                        && let Some(Some(stat)) = table.chunks.get(index)
                    {
                        if stat.present == 0 {
                            continue; // all-null chunk contributes nothing
                        }
                        let bound = if is_max { &stat.max } else { &stat.min };
                        if let Some(value) = bound.as_ref().and_then(|b| family.stat_value(b)) {
                            fold(value, &mut best);
                            continue;
                        }
                        // present values but no usable bound: decode the chunk
                    }
                }
                decode_rows.extend(chunk.row_offset..chunk.row_offset + chunk.row_count);
            }
        }
        None => decode_rows.extend(0..reader.row_count()),
    }
    if !decode_rows.is_empty() {
        let column = reader.read_docs_column_rows(field, &decode_rows)?;
        let column = family.cast_column(&column)?;
        let timestamps = match window {
            Some(_) => Some(read_timestamps(reader, Some(&decode_rows))?),
            None => None,
        };
        for i in 0..column.len() {
            if let Some((start, end)) = window {
                let ts = timestamps.as_ref().expect("read above");
                if ts.is_null(i) {
                    continue;
                }
                let t = ts.value(i);
                if t < start || t >= end {
                    continue;
                }
            }
            if let Some(value) = family.row_value(&column, i) {
                fold(value, &mut best);
            }
        }
    }
    Ok(best)
}

/// M16 chunk-decidable equality (DESIGN §4): the EXACT match bitmap of a
/// single numeric equality/IN conjunct, decided per chunk from the stats
/// table wherever possible and decoded elsewhere — the serve for
/// count-shaped aggregates whose conjunct the term index cannot answer
/// (#40/#42 index-off files, partial fields).
///
/// Per chunk: `present == 0` ⇒ no row matches (null-rejecting predicate);
/// every value outside `[min, max]` ⇒ no row matches; `present ==
/// row_count && min == max == v` for some probed value ⇒ EVERY row matches
/// (no nulls, all values equal v); anything else decodes the chunk and
/// compares per row. `Ok(None)` = no basis (no zone table, non-matching
/// type families, unparseable literals): the caller keeps today's path.
///
/// STRICT family gate: `Int` literals serve int-family columns, `Float`
/// literals float-family columns — cross-family coercions (the engine's
/// json_get semantics) never take this arm.
pub(super) fn stats_eq_bitmap(
    reader: &VixReader,
    field: &str,
    values: &[String],
    kind: crate::index::NumericKind,
) -> anyhow::Result<Option<BooleanBuffer>> {
    use arrow::array::BooleanBufferBuilder;

    if values.is_empty() {
        return Ok(None);
    }
    let Some(chunks) = reader.zone_chunks() else {
        return Ok(None); // no chunk geometry: nothing to decide with
    };
    let Some(family) = min_max_family(reader, field)? else {
        return Ok(None);
    };
    // strict kind<->family pairing (no cross-family coercion here)
    let family_ok = match kind {
        crate::index::NumericKind::Int => {
            matches!(family, MinMaxFamily::I64 | MinMaxFamily::U64)
        }
        crate::index::NumericKind::Float => matches!(family, MinMaxFamily::F64),
        crate::index::NumericKind::Bool => false,
    };
    if !family_ok {
        return Ok(None);
    }
    // parse the probed literals into the column family; any unparseable
    // text refuses the arm (never guess normalization)
    let mut probes: Vec<MinMaxValue> = Vec::with_capacity(values.len());
    for text in values {
        let parsed = match family {
            MinMaxFamily::I64 => text.parse::<i64>().ok().map(MinMaxValue::I64),
            MinMaxFamily::U64 => text.parse::<u64>().ok().map(MinMaxValue::U64),
            MinMaxFamily::F64 => text
                .parse::<f64>()
                .ok()
                .filter(|v| v.is_finite())
                .map(MinMaxValue::F64),
        };
        match parsed {
            Some(value) => probes.push(value),
            None => return Ok(None),
        }
    }
    let stats =
        column_stats_table(reader, field).filter(|(_, table)| table.tag == family.stats_tag());
    let len = reader.row_count() as usize;
    let mut builder = BooleanBufferBuilder::new(len);
    builder.append_n(len, false);
    let mut decode_rows: Vec<u64> = Vec::new();
    let eq = |a: &MinMaxValue, b: &MinMaxValue| -> bool {
        matches!(a.cmp_exact(b), Some(std::cmp::Ordering::Equal))
    };
    for (index, chunk) in chunks.iter().enumerate() {
        let stat = stats
            .as_ref()
            .and_then(|(_, table)| table.chunks.get(index))
            .and_then(|row| row.as_ref());
        let verdict = match stat {
            Some(stat) if stat.present == 0 => Some(false), // no values: no matches
            Some(stat) => {
                let min = stat.min.as_ref().and_then(|b| family.stat_value(b));
                let max = stat.max.as_ref().and_then(|b| family.stat_value(b));
                match (min, max) {
                    (Some(min), Some(max)) => {
                        let all_outside = probes.iter().all(|p| {
                            matches!(p.cmp_exact(&min), Some(std::cmp::Ordering::Less))
                                || matches!(p.cmp_exact(&max), Some(std::cmp::Ordering::Greater))
                        });
                        if all_outside {
                            Some(false)
                        } else if stat.present == chunk.row_count
                            && eq(&min, &max)
                            && probes.iter().any(|p| eq(p, &min))
                        {
                            Some(true) // no nulls, every value == a probe
                        } else {
                            None // inconclusive: decode
                        }
                    }
                    _ => None, // missing bound: decode
                }
            }
            None => None, // no stats row: decode
        };
        match verdict {
            Some(false) => {}
            Some(true) => {
                for row in chunk.row_offset..chunk.row_offset + chunk.row_count {
                    builder.set_bit(row as usize, true);
                }
            }
            None => decode_rows.extend(chunk.row_offset..chunk.row_offset + chunk.row_count),
        }
    }
    if !decode_rows.is_empty() {
        let column = reader.read_docs_column_rows(field, &decode_rows)?;
        let column = family.cast_column(&column)?;
        for (i, &row) in decode_rows.iter().enumerate() {
            if let Some(value) = family.row_value(&column, i)
                && probes.iter().any(|p| eq(p, &value))
            {
                builder.set_bit(row as usize, true);
            }
        }
    }
    Ok(Some(builder.finish()))
}

/// Return the fixed histogram bucket containing `timestamp`, or `None` when
/// the value falls outside the grid. This is the shared per-row rule for VIX
/// data and segment-WAL frames.
pub fn histogram_bucket(
    timestamp: i64,
    min_value: i64,
    bucket_width: u64,
    num_buckets: usize,
    ts_offset: i64,
) -> anyhow::Result<Option<usize>> {
    if num_buckets == 0 {
        return Ok(None);
    }
    let width = i64::try_from(bucket_width.max(1))
        .map_err(|_| anyhow::anyhow!("histogram bucket width overflows i64: {bucket_width}"))?;
    let origin = min_value
        .checked_sub(ts_offset)
        .ok_or_else(|| anyhow::anyhow!("histogram bucket origin overflows i64"))?;
    let Some(offset) = timestamp.checked_sub(origin) else {
        return Ok(None);
    };
    if offset < 0 {
        return Ok(None);
    }
    let bucket = (offset / width) as usize;
    Ok((bucket < num_buckets).then_some(bucket))
}

/// Return one in-grid bucket only when every timestamp in the inclusive
/// `[ts_min, ts_max]` range is guaranteed to land in it. `None` means the
/// range is outside the grid or crosses a bucket boundary and must be
/// inspected row-by-row.
pub fn histogram_range_bucket(
    ts_min: i64,
    ts_max: i64,
    min_value: i64,
    bucket_width: u64,
    num_buckets: usize,
    ts_offset: i64,
) -> anyhow::Result<Option<usize>> {
    if num_buckets == 0 || ts_min > ts_max {
        return Ok(None);
    }
    let width = i64::try_from(bucket_width.max(1))
        .map_err(|_| anyhow::anyhow!("histogram bucket width overflows i64: {bucket_width}"))?;
    let origin = min_value
        .checked_sub(ts_offset)
        .ok_or_else(|| anyhow::anyhow!("histogram bucket origin overflows i64"))?;
    Ok(chunk_single_bucket(ts_min, ts_max, origin, width, num_buckets).flatten())
}

/// Add row `i` of `timestamps` to its histogram bucket (the exact per-row
/// rule shared by the decode path and the zone-map boundary decode): drop
/// nulls, drop `ts < origin`, drop buckets `>= num_buckets`.
fn accumulate_bucket(
    counts: &mut [u64],
    timestamps: &Int64Array,
    i: usize,
    origin: i64,
    width: i64,
    num_buckets: usize,
) {
    if timestamps.is_null(i) {
        return;
    }
    let Some(offset) = timestamps.value(i).checked_sub(origin) else {
        return;
    };
    if offset < 0 {
        return;
    }
    let bucket = (offset / width) as usize;
    if bucket < num_buckets {
        counts[bucket] += 1;
    }
}

/// The extended bucket ordinal of `ts` (floor division of `ts - origin` by
/// `width`, allowing negatives), or `None` on i64 overflow (`ts - origin`
/// out of range) so the caller decodes the chunk rather than mis-bucket it.
fn extended_bucket(ts: i64, origin: i64, width: i64) -> Option<i64> {
    Some(ts.checked_sub(origin)?.div_euclid(width))
}

/// Verdict for folding a whole chunk spanning `[ts_min, ts_max]` into the
/// histogram without decoding:
/// - `Some(Some(bucket))` — every row lands in in-range `bucket` (add `row_count`),
/// - `Some(None)` — the whole chunk is out of range (contributes nothing),
/// - `None` — the chunk straddles ≥2 buckets and must be decoded.
///
/// Sound because both bounds fall in the same extended bucket ⇒ every row
/// (its `_timestamp` is within `[ts_min, ts_max]`) does too, and the extended
/// bucket matches the per-row rule (`0 <= (ts-origin)/width < num_buckets`).
fn chunk_single_bucket(
    ts_min: i64,
    ts_max: i64,
    origin: i64,
    width: i64,
    num_buckets: usize,
) -> Option<Option<usize>> {
    let e_min = extended_bucket(ts_min, origin, width)?;
    let e_max = extended_bucket(ts_max, origin, width)?;
    if e_min != e_max {
        return None;
    }
    if e_min >= 0 && (e_min as u128) < num_buckets as u128 {
        Some(Some(e_min as usize))
    } else {
        Some(None)
    }
}

/// Exact bounded histogram partials. NULL groups and unsupported stored types
/// refuse to the scan because the public String wire key cannot represent them.
pub(super) fn simple_multi_histogram(
    reader: &VixReader,
    bitmap: &BooleanBuffer,
    min_value: i64,
    max_value: i64,
    bucket_width: u64,
    ts_offset: i64,
    breakdown_field: &str,
) -> anyhow::Result<Vec<(i64, String, u64)>> {
    if bitmap.count_set_bits() == 0 || min_value >= max_value {
        return Ok(Vec::new());
    }
    let width = i64::try_from(bucket_width)
        .ok()
        .filter(|width| *width > 0)
        .ok_or(AggregateFallback("unsupported histogram width"))?;
    let raw_min = min_value
        .checked_sub(ts_offset)
        .ok_or(AggregateFallback("histogram grid overflow"))?;
    let raw_max = max_value
        .checked_sub(ts_offset)
        .ok_or(AggregateFallback("histogram grid overflow"))?;
    dict_multi_histogram(
        reader,
        bitmap,
        min_value,
        raw_min,
        raw_max,
        width,
        breakdown_field,
        DEFAULT_DICT_MULTI_HISTOGRAM_LIMITS,
    )
}

#[derive(Clone, Copy)]
struct DictMultiHistogramLimits {
    max_values: usize,
    max_value_bytes: usize,
    max_groups: usize,
}

const DEFAULT_DICT_MULTI_HISTOGRAM_LIMITS: DictMultiHistogramLimits = DictMultiHistogramLimits {
    max_values: 16 * 1024,
    max_value_bytes: 8 * 1024 * 1024,
    max_groups: 256 * 1024,
};

/// Only active chunks and bounded owned keys are retained. Count codes locally,
/// then intern each referenced string once; account output copies before adding
/// a new (bucket,value) key as those copies coexist during result construction.
struct GroupCounts {
    limits: DictMultiHistogramLimits,
    value_ids: HashMap<String, usize>,
    counts: HashMap<(i64, usize), u64>,
    value_bytes: usize,
    output_bytes: usize,
}

impl GroupCounts {
    fn new(limits: DictMultiHistogramLimits) -> Self {
        Self {
            limits,
            value_ids: HashMap::new(),
            counts: HashMap::new(),
            value_bytes: 0,
            output_bytes: 0,
        }
    }

    fn add(&mut self, bucket: i64, value: &str, count: u64) -> anyhow::Result<()> {
        let id = if let Some(id) = self.value_ids.get(value) {
            *id
        } else {
            if self.counts.len() >= self.limits.max_groups
                || value.len()
                    > self
                        .limits
                        .max_value_bytes
                        .saturating_sub(self.output_bytes)
            {
                return Err(AggregateFallback("aggregate group budget exceeded").into());
            }
            if self.value_ids.len() >= self.limits.max_values
                || value.len() > self.limits.max_value_bytes.saturating_sub(self.value_bytes)
            {
                return Err(AggregateFallback("aggregate value budget exceeded").into());
            }
            let id = self.value_ids.len();
            self.value_ids.insert(value.to_owned(), id);
            self.value_bytes += value.len();
            id
        };
        if let Some(total) = self.counts.get_mut(&(bucket, id)) {
            *total = total
                .checked_add(count)
                .ok_or(AggregateFallback("aggregate count overflow"))?;
        } else {
            if self.counts.len() >= self.limits.max_groups
                || value.len()
                    > self
                        .limits
                        .max_value_bytes
                        .saturating_sub(self.output_bytes)
            {
                return Err(AggregateFallback("aggregate group budget exceeded").into());
            }
            self.output_bytes += value.len();
            self.counts.insert((bucket, id), count);
        }
        Ok(())
    }

    fn finish(self) -> Vec<(i64, String, u64)> {
        let mut values = vec![String::new(); self.value_ids.len()];
        for (value, id) in self.value_ids {
            values[id] = value;
        }
        let mut rows: Vec<_> = self
            .counts
            .into_iter()
            .map(|((bucket, id), count)| (bucket, values[id].clone(), count))
            .collect();
        rows.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| a.1.cmp(&b.1))
        });
        rows
    }
}

/// Keep the selection itself chunk-local, including on legacy unzoned files.
/// Density is evaluated per touched chunk, so clustered matches retain pruning.
fn visit_selected_dict_chunks(
    reader: &VixReader,
    field: &str,
    bitmap: &BooleanBuffer,
    window: Option<(i64, i64)>,
    visitor: &mut dyn FnMut(vortex_index::DocsDictBatch, Option<&[u64]>) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let schema = reader.docs_schema()?;
    let data_type = schema
        .field_with_name(field)
        .map_err(|_| AggregateFallback("missing aggregate docs column"))?
        .data_type();
    if !matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Err(AggregateFallback("unsupported aggregate value type").into());
    }
    let mut visit_range = |range: std::ops::Range<u64>| -> anyhow::Result<()> {
        const CHUNK_ROWS: u64 = 65_536;
        let mut start = range.start;
        while start < range.end {
            vortex_index::check_read_cancelled()?;
            let end = range.end.min(start.saturating_add(CHUNK_ROWS));
            let selected = bitmap.slice(start as usize, (end - start) as usize);
            let matched = selected.count_set_bits();
            if matched > 0 {
                if matched.saturating_mul(POINT_READ_DENOMINATOR) <= selected.len() {
                    let rows: Vec<u64> = selected.set_indices().map(|i| start + i as u64).collect();
                    let values = reader.read_docs_column_rows(field, &rows)?;
                    let timestamps = if window.is_some() {
                        Some(read_timestamps(reader, Some(&rows))?)
                    } else {
                        None
                    };
                    let batch = vortex_index::DocsDictBatch {
                        row_offset: start,
                        timestamps,
                        codes: arrow::array::UInt64Array::from_iter_values(0..rows.len() as u64),
                        values,
                    };
                    visitor(batch, Some(&rows))?;
                } else {
                    reader.visit_docs_dict_chunks(
                        field,
                        start..end,
                        window.is_some(),
                        &mut |batch| visitor(batch, None),
                    )?;
                }
            }
            start = end;
        }
        Ok(())
    };
    if let Some(zones) = reader.zone_chunks() {
        let mut pending: Option<std::ops::Range<u64>> = None;
        for zone in zones {
            let range = zone.row_offset..zone.row_offset + u64::from(zone.row_count);
            if window.is_some_and(|(min, max)| zone.ts_max < min || zone.ts_min >= max)
                || bitmap
                    .slice(range.start as usize, (range.end - range.start) as usize)
                    .count_set_bits()
                    == 0
            {
                if let Some(range) = pending.take() {
                    visit_range(range)?;
                }
                continue;
            }
            match pending.as_mut() {
                Some(previous) if previous.end == range.start => previous.end = range.end,
                _ => {
                    if let Some(previous) = pending.take() {
                        visit_range(previous)?;
                    }
                    pending = Some(range);
                }
            }
        }
        if let Some(range) = pending {
            visit_range(range)?;
        }
    } else {
        visit_range(0..reader.row_count())?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn dict_multi_histogram(
    reader: &VixReader,
    bitmap: &BooleanBuffer,
    min_value: i64,
    raw_min: i64,
    raw_max: i64,
    width: i64,
    breakdown_field: &str,
    limits: DictMultiHistogramLimits,
) -> anyhow::Result<Vec<(i64, String, u64)>> {
    let mut counts = GroupCounts::new(limits);
    visit_selected_dict_chunks(
        reader,
        breakdown_field,
        bitmap,
        Some((raw_min, raw_max)),
        &mut |chunk, point_rows| {
            fold_dict_chunk(
                &mut counts,
                chunk,
                bitmap,
                point_rows,
                Some((min_value, raw_min, raw_max, width)),
            )
        },
    )?;
    Ok(counts.finish())
}

fn fold_dict_chunk(
    counts: &mut GroupCounts,
    chunk: vortex_index::DocsDictBatch,
    bitmap: &BooleanBuffer,
    point_rows: Option<&[u64]>,
    grid: Option<(i64, i64, i64, i64)>,
) -> anyhow::Result<()> {
    if chunk.values.len() > counts.limits.max_values
        || chunk.values.get_array_memory_size() > counts.limits.max_value_bytes
    {
        return Err(AggregateFallback("aggregate chunk dictionary budget exceeded").into());
    }
    let values = cast(&chunk.values, &DataType::Utf8)?;
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(AggregateFallback("unsupported aggregate dictionary type"))?;
    let mut local: HashMap<(i64, usize), u64> = HashMap::new();
    for i in 0..chunk.codes.len() {
        let row = point_rows.map_or(chunk.row_offset as usize + i, |rows| rows[i] as usize);
        if !bitmap.value(row) {
            continue;
        }
        let bucket = if let Some((min, raw_min, raw_max, width)) = grid {
            let timestamps = chunk
                .timestamps
                .as_ref()
                .ok_or(AggregateFallback("missing aggregate timestamps"))?;
            if timestamps.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            if ts < raw_min || ts >= raw_max {
                continue;
            }
            min.checked_add(
                ts.checked_sub(raw_min)
                    .ok_or(AggregateFallback("histogram grid overflow"))?
                    / width
                    * width,
            )
            .ok_or(AggregateFallback("histogram grid overflow"))?
        } else {
            0
        };
        if chunk.codes.is_null(i) {
            return Err(AggregateFallback("NULL aggregate group requires scan").into());
        }
        let code = usize::try_from(chunk.codes.value(i))?;
        if code >= values.len() {
            anyhow::bail!("aggregate dictionary code out of bounds");
        }
        if values.is_null(code) {
            return Err(AggregateFallback("NULL aggregate group requires scan").into());
        }
        if let Some(total) = local.get_mut(&(bucket, code)) {
            *total += 1;
        } else {
            if local.len() >= counts.limits.max_groups {
                return Err(AggregateFallback("aggregate chunk group budget exceeded").into());
            }
            local.insert((bucket, code), 1);
        }
    }
    for ((bucket, code), count) in local {
        counts.add(bucket, values.value(code), count)?;
    }
    Ok(())
}
#[derive(Hash)]
struct BorrowedGroup<'a>(&'a [&'a str]);

impl hashbrown::Equivalent<Vec<String>> for BorrowedGroup<'_> {
    fn equivalent(&self, key: &Vec<String>) -> bool {
        self.0.len() == key.len() && self.0.iter().zip(key).all(|(a, b)| *a == b)
    }
}

/// Return every bounded group, never a file-local top-k. The leader owns ranking.
pub(super) fn simple_top_n(
    reader: &VixReader,
    bitmap: &BooleanBuffer,
    fields: &[String],
    _limit: usize,
    _ascend: bool,
) -> anyhow::Result<Vec<(Vec<String>, u64)>> {
    if fields.is_empty() || fields.len() > MAX_SIMPLE_TOPN_FIELDS {
        return Err(AggregateFallback("unsupported aggregate grouping fields").into());
    }
    if bitmap.count_set_bits() == 0 {
        return Ok(Vec::new());
    }
    if fields.len() == 1 {
        let counts = dict_group_counts(reader, &fields[0], bitmap)?;
        return Ok(counts
            .into_iter()
            .map(|(value, count)| (vec![value], count))
            .collect());
    }
    // Tuple keys have no code-level common dictionary. Point-read bounded
    // chunks, retaining only complete bounded tuple counts across chunks.
    let limits = top_n_limits();
    for field in fields {
        let schema = reader.docs_schema()?;
        if !matches!(
            schema
                .field_with_name(field)
                .map_err(|_| AggregateFallback("missing aggregate docs column"))?
                .data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Err(AggregateFallback("unsupported aggregate value type").into());
        }
    }
    let mut counts: HashMap<Vec<String>, u64> = HashMap::new();
    let mut bytes = 0usize;
    for start in (0..bitmap.len()).step_by(65_536) {
        vortex_index::check_read_cancelled()?;
        let selected = bitmap.slice(start, (bitmap.len() - start).min(65_536));
        let rows: Vec<_> = selected.set_indices().map(|i| (start + i) as u64).collect();
        if rows.is_empty() {
            continue;
        }
        let columns = fields
            .iter()
            .map(|field| read_column_strings(reader, field, Some(&rows)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        for i in 0..rows.len() {
            if columns.iter().any(|column| column.is_null(i)) {
                return Err(AggregateFallback("NULL aggregate group requires scan").into());
            }
            let mut key = [""; MAX_SIMPLE_TOPN_FIELDS];
            for (slot, column) in key.iter_mut().zip(&columns) {
                *slot = column.value(i);
            }
            let borrowed = BorrowedGroup(&key[..fields.len()]);
            if let Some(total) = counts.get_mut(&borrowed) {
                *total += 1;
            } else {
                let key_bytes: usize = borrowed.0.iter().map(|value| value.len()).sum();
                if counts.len() >= limits.max_groups
                    || key_bytes > limits.max_value_bytes.saturating_sub(bytes)
                {
                    return Err(AggregateFallback("aggregate group budget exceeded").into());
                }
                bytes += key_bytes;
                counts.insert(
                    borrowed.0.iter().map(|value| (*value).to_owned()).collect(),
                    1,
                );
            }
        }
    }
    Ok(counts.into_iter().collect())
}

fn dict_group_counts(
    reader: &VixReader,
    field: &str,
    bitmap: &BooleanBuffer,
) -> anyhow::Result<HashMap<String, u64>> {
    let mut counts = GroupCounts::new(top_n_limits());
    visit_selected_dict_chunks(reader, field, bitmap, None, &mut |chunk, rows| {
        fold_dict_chunk(&mut counts, chunk, bitmap, rows, None)
    })?;
    Ok(counts
        .finish()
        .into_iter()
        .map(|(_, value, count)| (value, count))
        .collect())
}

fn top_n_limits() -> DictMultiHistogramLimits {
    let cap = config::get_config()
        .limit
        .inverted_index_topn_max_group_num
        .max(1)
        .min(DEFAULT_DICT_MULTI_HISTOGRAM_LIMITS.max_groups);
    DictMultiHistogramLimits {
        max_values: cap,
        max_groups: cap,
        ..DEFAULT_DICT_MULTI_HISTOGRAM_LIMITS
    }
}

/// Exact ordered DISTINCT candidates. A value omitted from a file's first/last
/// `limit` has `limit` better distinct values in that same file, so cannot enter
/// the global first/last `limit`. This proof applies only to the same raw-string
/// total order, not COUNT-based TopN. NULL and unsupported types require scan.
pub(super) fn simple_distinct(
    reader: &VixReader,
    bitmap: &BooleanBuffer,
    field: &str,
    limit: usize,
    ascend: bool,
) -> anyhow::Result<HashSet<String>> {
    if bitmap.count_set_bits() == 0 || limit == 0 {
        return Ok(HashSet::new());
    }
    let mut distinct = OrderedDistinct::new(limit, ascend)?;
    visit_selected_dict_chunks(reader, field, bitmap, None, &mut |chunk, point_rows| {
        if chunk.values.get_array_memory_size() > top_n_limits().max_value_bytes {
            return Err(AggregateFallback("distinct dictionary byte budget exceeded").into());
        }
        let values = cast(&chunk.values, &DataType::Utf8)?;
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(AggregateFallback("unsupported distinct dictionary type"))?;
        for i in 0..chunk.codes.len() {
            let row = point_rows.map_or(chunk.row_offset as usize + i, |rows| rows[i] as usize);
            if !bitmap.value(row) {
                continue;
            }
            if chunk.codes.is_null(i) {
                return Err(AggregateFallback("NULL distinct value requires scan").into());
            }
            let code = usize::try_from(chunk.codes.value(i))?;
            if code >= values.len() {
                anyhow::bail!("distinct dictionary code out of bounds");
            }
            if values.is_null(code) {
                return Err(AggregateFallback("NULL distinct value requires scan").into());
            }
            distinct.insert(values.value(code))?;
        }
        Ok(())
    })?;
    Ok(distinct.values.into_iter().collect())
}

struct OrderedDistinct {
    values: BTreeSet<String>,
    bytes: usize,
    limit: usize,
    ascend: bool,
    max_bytes: usize,
}

impl OrderedDistinct {
    fn new(limit: usize, ascend: bool) -> anyhow::Result<Self> {
        let limits = top_n_limits();
        if limit > limits.max_groups {
            return Err(AggregateFallback("distinct candidate budget exceeded").into());
        }
        Ok(Self {
            values: BTreeSet::new(),
            bytes: 0,
            limit,
            ascend,
            max_bytes: limits.max_value_bytes,
        })
    }

    fn insert(&mut self, value: &str) -> anyhow::Result<()> {
        if self.limit == 0 || self.values.contains(value) {
            return Ok(());
        }
        if self.values.len() == self.limit {
            let worst = if self.ascend {
                self.values.last()
            } else {
                self.values.first()
            }
            .expect("nonzero full candidate set");
            if (self.ascend && value >= worst.as_str()) || (!self.ascend && value <= worst.as_str())
            {
                return Ok(());
            }
            // Release the old owned key before allocating its replacement.
            let removed = if self.ascend {
                self.values.pop_last()
            } else {
                self.values.pop_first()
            }
            .expect("nonzero full candidate set");
            self.bytes -= removed.len();
            drop(removed);
        }
        if value.len() > self.max_bytes.saturating_sub(self.bytes) {
            return Err(AggregateFallback("distinct candidate byte budget exceeded").into());
        }
        self.bytes += value.len();
        self.values.insert(value.to_owned());
        Ok(())
    }
}

/// One file's TopN groups: `(group key, count)` with one key entry per
/// group field.
type TopNGroups = Vec<(Vec<String>, u64)>;

/// Unfiltered full-range SimpleTopN over one field, served straight from the
/// term dictionary (`VixReader::field_value_counts`): every raw term of the
/// field is one group with its exact `doc_count` — no postings and no docs
/// columns. `Ok(None)` when this file cannot prove exact per-value counts
/// (fts-marked / partial / non-string-typed / empty-string values, or a
/// pre-core file); the caller falls back to docs columns or the precise scan.
pub(super) fn unfiltered_top_n(
    reader: &VixReader,
    field: &str,
    _limit: usize,
    _ascend: bool,
) -> anyhow::Result<Option<TopNGroups>> {
    let Some(counts) = complete_value_counts(reader, field)? else {
        return Ok(None);
    };
    if counts.iter().map(|(_, count)| *count).sum::<u64>() != reader.row_count() {
        return Err(AggregateFallback("NULL or missing aggregate values require scan").into());
    }
    Ok(Some(
        counts
            .into_iter()
            .map(|(value, count)| (vec![value], count))
            .collect(),
    ))
}

/// Bounded complete raw-string counts; never confuse a top-k witness with a
/// complete dictionary. Reader reconciliation establishes raw-index coverage.
fn complete_value_counts(
    reader: &VixReader,
    field: &str,
) -> anyhow::Result<Option<Vec<(String, u64)>>> {
    if reader.field_oversize_skips(field) != 0 {
        return Ok(None);
    }
    let limits = top_n_limits();
    let Some((counts, truncated)) = reader.field_value_top_k(field, limits.max_values, false)?
    else {
        return Ok(None);
    };
    if truncated {
        return Err(AggregateFallback("aggregate dictionary group budget exceeded").into());
    }
    let mut bytes = 0usize;
    let mut result = Vec::with_capacity(counts.len());
    for (value, count) in counts {
        if value.len() > limits.max_value_bytes.saturating_sub(bytes) {
            return Err(AggregateFallback("aggregate dictionary byte budget exceeded").into());
        }
        bytes += value.len();
        let value = String::from_utf8(value)
            .map_err(|_| AggregateFallback("aggregate dictionary is not raw UTF8"))?;
        result.push((value, count));
    }
    Ok(Some(result))
}

/// Exact shortcut for a sole positive IN on this grouping field. Unlike TopN,
/// NULL/absent values are safe here: IN rejects them. Select the globally
/// requested literals from complete counts, not each file's local winners.
#[allow(clippy::too_many_arguments)]
pub(super) fn single_bucket_value_counts(
    reader: &VixReader,
    field: &str,
    values: &[String],
    file_range: (i64, i64),
    query_range: (i64, i64),
    min_value: i64,
    max_value: i64,
    bucket_width: u64,
    ts_offset: i64,
) -> anyhow::Result<Option<Vec<(i64, String, u64)>>> {
    if file_range.0 > file_range.1
        || file_range.0 < query_range.0
        || file_range.1 >= query_range.1
        || min_value >= max_value
        || bucket_width == 0
    {
        return Ok(None);
    }
    let Some(span) = max_value
        .checked_sub(min_value)
        .and_then(|v| u64::try_from(v).ok())
    else {
        return Ok(None);
    };
    let Ok(num_buckets) = usize::try_from(span.div_ceil(bucket_width)) else {
        return Ok(None);
    };
    let Some(bucket) = histogram_range_bucket(
        file_range.0,
        file_range.1,
        min_value,
        bucket_width,
        num_buckets,
        ts_offset,
    )?
    else {
        return Ok(None);
    };
    let Some(label) = i64::try_from(bucket_width)
        .ok()
        .and_then(|width| width.checked_mul(bucket as i64))
        .and_then(|offset| min_value.checked_add(offset))
    else {
        return Ok(None);
    };
    let limits = top_n_limits();
    if values.len() > limits.max_values {
        return Ok(None);
    }
    let mut literal_bytes = 0usize;
    for value in values {
        if value.len() > limits.max_value_bytes.saturating_sub(literal_bytes) {
            return Ok(None);
        }
        literal_bytes += value.len();
    }
    let Some(counts) = complete_value_counts(reader, field)? else {
        return Ok(None);
    };
    let selected: HashSet<&str> = values.iter().map(String::as_str).collect();
    Ok(Some(
        counts
            .into_iter()
            .filter(|(value, count)| *count > 0 && selected.contains(value.as_str()))
            .map(|(value, count)| (label, value, count))
            .collect(),
    ))
}

/// FILTERED single-field SimpleTopN served from the term dictionary +
/// postings: per raw string value of `field`, the count of its documents
/// inside `bitmap` (this file's condition∧time row set). This is the fast
/// serve for files that predate the field's `column_store_fields` entry —
/// the whole pre-setting history — replacing the MissingColumn scan
/// fallback with one SIMD postings pass over the field. `Ok(None)` under
/// exactly [`unfiltered_top_n`]'s eligibility contract.
pub(super) fn filtered_top_n(
    reader: &VixReader,
    bitmap: &BooleanBuffer,
    field: &str,
    _limit: usize,
    _ascend: bool,
) -> anyhow::Result<Option<TopNGroups>> {
    let cap = top_n_limits().max_values;
    if reader.field_oversize_skips(field) != 0 {
        return Ok(None);
    }
    let Some(counts) = reader.field_value_counts_filtered(field, bitmap, cap)? else {
        return Ok(None);
    };
    if counts.iter().map(|(_, count)| *count).sum::<u64>() != bitmap.count_set_bits() as u64 {
        return Err(AggregateFallback("NULL or missing aggregate values require scan").into());
    }
    let mut bytes = 0usize;
    let mut top = Vec::new();
    for (value, count) in counts.into_iter().filter(|(_, count)| *count > 0) {
        if value.len() > top_n_limits().max_value_bytes.saturating_sub(bytes) {
            return Err(AggregateFallback("aggregate dictionary byte budget exceeded").into());
        }
        bytes += value.len();
        top.push((
            vec![
                String::from_utf8(value)
                    .map_err(|_| AggregateFallback("aggregate dictionary is not raw UTF8"))?,
            ],
            count,
        ));
    }
    Ok(Some(top))
}

/// FILTERED SimpleDistinct twin of [`filtered_top_n`]: the field's values
/// with at least one document inside `bitmap`, in ascending byte order, so
/// the first/last `limit` are the answer. `Ok(None)` exactly like
/// [`unfiltered_top_n`].
pub(super) fn filtered_distinct(
    reader: &VixReader,
    bitmap: &BooleanBuffer,
    field: &str,
    limit: usize,
    ascend: bool,
) -> anyhow::Result<Option<HashSet<String>>> {
    if bitmap.count_set_bits() == 0 || limit == 0 {
        return Ok(Some(HashSet::new()));
    }
    let mut selected = OrderedDistinct::new(limit, ascend)?;
    if reader.field_oversize_skips(field) != 0 {
        return Ok(None);
    }
    let Some(counts) =
        reader.field_value_counts_filtered(field, bitmap, top_n_limits().max_values)?
    else {
        return Ok(None);
    };
    if counts.iter().map(|(_, count)| *count).sum::<u64>() != bitmap.count_set_bits() as u64 {
        return Err(AggregateFallback("NULL or missing distinct values require scan").into());
    }
    for (value, count) in counts {
        if count == 0 {
            continue;
        }
        let value = std::str::from_utf8(&value)
            .map_err(|_| AggregateFallback("distinct dictionary is not raw UTF8"))?;
        selected.insert(value)?;
    }
    Ok(Some(selected.values.into_iter().collect()))
}

/// Unfiltered full-range SimpleDistinct served straight from the term
/// dictionary: the field's raw terms come back in ascending byte order (the
/// old term-dictionary order), so the first/last `limit` are the answer.
/// `Ok(None)` exactly like [`unfiltered_top_n`].
pub(super) fn unfiltered_distinct(
    reader: &VixReader,
    field: &str,
    limit: usize,
    ascend: bool,
) -> anyhow::Result<Option<HashSet<String>>> {
    if limit == 0 {
        return Ok(Some(HashSet::new()));
    }
    let mut selected = OrderedDistinct::new(limit, ascend)?;
    if !reader.has_index() || reader.field_oversize_skips(field) != 0 {
        return Ok(None);
    }
    let Some(values) = reader.field_value_head(field, limit, !ascend)? else {
        return Ok(None);
    };
    // Head/tail proves complete raw-string term coverage, but the key term
    // counts only non-NULL values. Missing rows would require a NULL wire key.
    if reader.count(&vortex_index::VixQuery::KeyExists {
        path: field.to_owned(),
    })? != reader.row_count()
    {
        return Err(AggregateFallback("NULL or missing distinct values require scan").into());
    }
    for value in values {
        selected.insert(
            std::str::from_utf8(&value)
                .map_err(|_| AggregateFallback("distinct dictionary is not raw UTF8"))?,
        )?;
    }
    Ok(Some(selected.values.into_iter().collect()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Float64Array, RecordBatch},
        datatypes::{Field, Schema},
    };
    use vortex_index::{VixWriter, VixWriterOptions};

    use super::*;

    thread_local! {
        /// Number of docs chunks [`simple_histogram`] decoded on this thread
        /// since the last reset — the perf hook proving the zone-map fast
        /// path decodes only the boundary chunks.
        static DECODED_HISTOGRAM_CHUNKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// Called by [`simple_histogram`] (test builds only) once per boundary
    /// chunk it decodes.
    pub(super) fn record_decoded_histogram_chunks(n: usize) {
        DECODED_HISTOGRAM_CHUNKS.with(|c| c.set(c.get() + n));
    }

    fn reset_decoded_histogram_chunks() {
        DECODED_HISTOGRAM_CHUNKS.with(|c| c.set(0));
    }

    fn decoded_histogram_chunks() -> usize {
        DECODED_HISTOGRAM_CHUNKS.with(|c| c.get())
    }

    /// Build a core file: 8 docs ordered `_timestamp` DESC with columns
    /// `level` (utf8), `service` (utf8), `code` (i64) and `ratio` (f64) as
    /// column-store fields. `http.status` lives only inside `_source`.
    fn build_reader() -> VixReader {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("service", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
            Field::new("ratio", DataType::Float64, true),
        ]));
        let opts = VixWriterOptions {
            row_group_size: 4,
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        // rows newest-first: ts 107..100
        let ts: Vec<i64> = (0..8).map(|i| 107 - i).collect();
        let levels = vec![
            Some("error"),
            Some("info"),
            Some("error"),
            Some("info"),
            Some("info"),
            None,
            Some("info"),
            Some("error"),
        ];
        let services = vec![
            Some("a"),
            Some("a"),
            Some("b"),
            Some("a"),
            Some("b"),
            Some("a"),
            Some("a"),
            Some("c"),
        ];
        let codes: Vec<Option<i64>> = vec![
            Some(200),
            Some(500),
            Some(200),
            Some(404),
            Some(200),
            Some(200),
            None,
            Some(500),
        ];
        let ratios: Vec<Option<f64>> = vec![
            Some(0.5),
            Some(1.5),
            Some(0.5),
            None,
            Some(2.0),
            Some(0.5),
            Some(0.5),
            Some(1.5),
        ];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(StringArray::from(levels.clone())),
                Arc::new(StringArray::from(services.clone())),
                Arc::new(Int64Array::from(codes.clone())),
                Arc::new(Float64Array::from(ratios.clone())),
            ],
        )
        .unwrap();
        let sources: Vec<String> = (0..8)
            .map(|i| {
                format!(
                    r#"{{"_timestamp":{},"service":"{}","http.status":"x"}}"#,
                    ts[i],
                    services[i].unwrap_or("-")
                )
            })
            .collect();
        let sources = StringArray::from(sources);
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        {
            let (data, index) = writer.finish().unwrap();
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap()
        }
    }

    fn all_set(len: usize) -> BooleanBuffer {
        BooleanBuffer::new_set(len)
    }

    fn rowwise_multi_histogram_reference(
        reader: &VixReader,
        bitmap: &BooleanBuffer,
        min_value: i64,
        max_value: i64,
        bucket_width: u64,
        ts_offset: i64,
        breakdown_field: &str,
    ) -> Vec<(i64, String, u64)> {
        let timestamps = read_timestamps(reader, None).unwrap();
        let values = read_column_strings(reader, breakdown_field, None).unwrap();
        let mut counts: HashMap<(i64, String), u64> = HashMap::new();
        let raw_min = min_value - ts_offset;
        let raw_max = max_value - ts_offset;
        for row in bitmap.set_indices() {
            let ts = timestamps.value(row);
            if ts < raw_min || ts >= raw_max {
                continue;
            }
            assert!(
                !values.is_null(row),
                "reference requires non-NULL selected values"
            );
            let bucket = min_value + (ts - raw_min) / bucket_width as i64 * bucket_width as i64;
            *counts
                .entry((bucket, values.value(row).to_owned()))
                .or_default() += 1;
        }
        let mut rows: Vec<_> = counts
            .into_iter()
            .map(|((bucket, value), count)| (bucket, value, count))
            .collect();
        rows.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| a.1.cmp(&b.1))
        });
        rows
    }

    #[test]
    fn test_single_bucket_counts_select_global_literals_and_reject_boundaries() {
        let reader = build_reader();
        let values = vec!["info".to_owned(), "absent".to_owned(), "info".to_owned()];
        // The file contains a NULL level, which this positive IN excludes.
        assert_eq!(
            single_bucket_value_counts(
                &reader,
                "level",
                &values,
                (100, 107),
                (100, 108),
                99,
                109,
                10,
                0
            )
            .unwrap(),
            Some(vec![(99, "info".to_owned(), 4)])
        );
        // The requested key is not the file's local top-one.
        assert_eq!(
            single_bucket_value_counts(
                &reader,
                "service",
                &["c".to_owned()],
                (100, 107),
                (100, 108),
                100,
                108,
                8,
                0
            )
            .unwrap(),
            Some(vec![(100, "c".to_owned(), 1)])
        );
        for (file, query, min, max, width) in [
            ((100, 107), (101, 108), 100, 108, 8), // partial start
            ((100, 107), (100, 107), 100, 107, 8), // exclusive end
            ((100, 107), (100, 108), 100, 108, 4), // actual bucket crossing
        ] {
            assert!(
                single_bucket_value_counts(
                    &reader, "level", &values, file, query, min, max, width, 0
                )
                .unwrap()
                .is_none()
            );
        }
        assert!(
            single_bucket_value_counts(
                &reader,
                "ratio",
                &values,
                (100, 107),
                (100, 108),
                100,
                108,
                8,
                0
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn test_metadata_counts_refuse_oversize_but_docs_keep_the_group() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                max_raw_term_len: 8,
                ..Default::default()
            },
            false,
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![101, 100])),
                Arc::new(StringArray::from(vec!["oversized", "short"])),
            ],
        )
        .unwrap();
        writer
            .push_batch_with_source(
                &batch,
                &StringArray::from(vec![
                    r#"{"_timestamp":101,"value":"oversized"}"#,
                    r#"{"_timestamp":100,"value":"short"}"#,
                ]),
                None,
            )
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        let reader =
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap();
        for value in ["short", "oversized"] {
            assert!(
                single_bucket_value_counts(
                    &reader,
                    "value",
                    &[value.to_owned()],
                    (100, 101),
                    (100, 102),
                    100,
                    102,
                    2,
                    0
                )
                .unwrap()
                .is_none()
            );
        }
        assert!(
            unfiltered_top_n(&reader, "value", 1, false)
                .unwrap()
                .is_none()
        );
        let groups = simple_top_n(&reader, &all_set(2), &["value".to_owned()], 1, false).unwrap();
        assert_eq!(
            groups.into_iter().collect::<HashMap<_, _>>(),
            HashMap::from_iter([
                (vec!["oversized".to_owned()], 1),
                (vec!["short".to_owned()], 1),
            ])
        );
    }

    #[test]
    fn test_null_and_missing_groups_require_precise_scan() {
        let reader = build_reader();
        for field in ["level", "absent"] {
            assert!(
                simple_multi_histogram(&reader, &all_set(8), 100, 108, 8, 0, field)
                    .unwrap_err()
                    .is::<AggregateFallback>()
            );
            assert!(
                simple_top_n(&reader, &all_set(8), &[field.to_owned()], 1, false)
                    .unwrap_err()
                    .is::<AggregateFallback>()
            );
        }
        // A bitmap excluding NULL is safe; the selected rows, not schema
        // nullability, establish whether the String wire result is exact.
        let selected = BooleanBuffer::from_iter((0..8).map(|i| i != 5));
        let counts = filtered_top_n(&reader, &selected, "level", 1, false)
            .unwrap()
            .unwrap();
        assert_eq!(
            counts.into_iter().collect::<HashMap<_, _>>(),
            HashMap::from_iter([(vec!["info".to_owned()], 4), (vec!["error".to_owned()], 3)])
        );
        assert!(
            filtered_top_n(&reader, &all_set(8), "level", 1, false)
                .unwrap_err()
                .is::<AggregateFallback>()
        );
    }

    #[test]
    fn test_distributed_histogram_cap_counterexample_keeps_shared_winner() {
        let cap = config::get_config().limit.query_default_limit.max(1) as usize;
        // Each file has cap private winners at11 and a shared runner-up at10.
        // Local top-cap drops shared everywhere, although globally it wins20.
        let private_count = cap.min(DEFAULT_DICT_MULTI_HISTOGRAM_LIMITS.max_values - 1);
        let mut merged: HashMap<String, u64> = HashMap::new();
        for file in 0..2 {
            let mut values = Vec::new();
            for key in 0..private_count {
                values.extend(std::iter::repeat_n(format!("private-{file}-{key}"), 11));
            }
            values.extend(std::iter::repeat_n("shared".to_owned(), 10));
            let ts = vec![100; values.len()];
            let borrowed: Vec<_> = values.iter().map(|value| Some(value.as_str())).collect();
            let (reader, _) = zoned_and_decode(&ts, &borrowed);
            let rows =
                simple_multi_histogram(&reader, &all_set(ts.len()), 100, 101, 1, 0, "bd").unwrap();
            for (_, value, count) in rows {
                *merged.entry(value).or_default() += count;
            }
        }
        assert_eq!(merged["shared"], 20);
        assert!(
            merged
                .iter()
                .all(|(key, count)| key == "shared" || *count == 11)
        );
        assert_eq!(
            merged.into_iter().max_by_key(|(_, count)| *count).unwrap(),
            ("shared".to_owned(), 20)
        );
    }

    #[test]
    fn test_dense_all_but_one_clustered_and_sparse_histograms_are_exact() {
        let ts: Vec<_> = (0..8192).map(|i| 50_000 - i).collect();
        let values: Vec<_> = (0..ts.len())
            .map(|i| Some(["a", "b", "c", "d", "e", "f"][i % 6]))
            .collect();
        let (zoned, unzoned) = zoned_and_decode(&ts, &values);
        for bitmap in [
            BooleanBuffer::from_iter((0..ts.len()).map(|i| i != 37)),
            BooleanBuffer::from_iter((0..ts.len()).map(|i| i < 4096)),
            BooleanBuffer::from_iter((0..ts.len()).map(|i| i % 2 == 0)),
            BooleanBuffer::from_iter((0..ts.len()).map(|i| i == 0 || i == 4097)),
        ] {
            for reader in [&zoned, &unzoned] {
                let expected = rowwise_multi_histogram_reference(
                    reader, &bitmap, 42_000, 50_001, 100, 0, "bd",
                );
                assert_eq!(
                    simple_multi_histogram(reader, &bitmap, 42_000, 50_001, 100, 0, "bd").unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn test_docs_column_available() {
        let reader = build_reader();
        assert!(docs_column_available(&reader, "_timestamp").unwrap());
        assert!(docs_column_available(&reader, "level").unwrap());
        assert!(docs_column_available(&reader, "code").unwrap());
        // exists only inside _source: not a docs column
        assert!(!docs_column_available(&reader, "http.status").unwrap());
        assert!(!docs_column_available(&reader, "missing_field").unwrap());
    }

    #[test]
    fn test_missing_docs_column_per_mode() {
        use config::meta::inverted_index::IndexOptimizeMode;

        let reader = build_reader();
        // modes reading only _timestamp never report a missing column
        assert_eq!(
            missing_docs_column(&reader, &IndexOptimizeMode::SimpleCount).unwrap(),
            None
        );
        assert_eq!(
            missing_docs_column(&reader, &IndexOptimizeMode::SimpleSelect(10, false)).unwrap(),
            None
        );
        assert_eq!(
            missing_docs_column(&reader, &IndexOptimizeMode::SimpleHistogram(0, 10, 5, 0)).unwrap(),
            None
        );
        // modes over present docs columns pass
        assert_eq!(
            missing_docs_column(
                &reader,
                &IndexOptimizeMode::SimpleTopN(vec!["service".to_string()], 10, false)
            )
            .unwrap(),
            None
        );
        assert_eq!(
            missing_docs_column(
                &reader,
                &IndexOptimizeMode::SimpleDistinct("level".to_string(), 10, true)
            )
            .unwrap(),
            None
        );
        // a field the docs blob lacks (older file / never a cs field) is
        // reported: the file falls back to the scan path
        assert_eq!(
            missing_docs_column(
                &reader,
                &IndexOptimizeMode::SimpleTopN(
                    vec!["service".to_string(), "http.status".to_string()],
                    10,
                    false
                )
            )
            .unwrap(),
            Some("http.status".to_string())
        );
        assert_eq!(
            missing_docs_column(
                &reader,
                &IndexOptimizeMode::SimpleDistinct("http.status".to_string(), 10, true)
            )
            .unwrap(),
            Some("http.status".to_string())
        );
        assert_eq!(
            missing_docs_column(
                &reader,
                &IndexOptimizeMode::SimpleMultiHistogram(0, 10, 1, 0, "http.status".to_string())
            )
            .unwrap(),
            Some("http.status".to_string())
        );
    }

    #[test]
    fn test_simple_select_desc_and_asc() {
        let reader = build_reader();
        // rows are ts 107..100 (docs 0..7)
        let bitmap = all_set(8);
        // DESC: newest 3
        let candidates = simple_select(&reader, &bitmap, 3, false).unwrap();
        assert_eq!(candidates, vec![(107, 0), (106, 1), (105, 2)]);
        // ASC: oldest 3, best (smallest ts) first
        let candidates = simple_select(&reader, &bitmap, 3, true).unwrap();
        assert_eq!(candidates, vec![(100, 7), (101, 6), (102, 5)]);
    }

    /// #51c-c: over a CONCAT-order file the positional shortcut is a lie —
    /// the newest matched rows are NOT the first set bits. `simple_select`
    /// must return the true value-based top-K (ASC and DESC, bitmap
    /// respected, ties on the smaller doc id), or the pruner merge feeds
    /// wrong candidates into `ORDER BY _timestamp LIMIT n` answers.
    #[test]
    fn test_simple_select_concat_order_file_selects_by_value() {
        // two DESC runs back-to-back: [300, 250, 200] then [320, 270, 220]
        let ts: Vec<i64> = vec![300, 250, 200, 320, 270, 220];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_timestamp",
            DataType::Int64,
            false,
        )]));
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                concat_row_order: true,
                ..Default::default()
            },
            false,
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(ts.clone()))],
        )
        .unwrap();
        let sources: Vec<String> = ts
            .iter()
            .map(|t| format!(r#"{{"_timestamp":{t}}}"#))
            .collect();
        let sources = StringArray::from(sources);
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        let reader = {
            let (data, index) = writer.finish().unwrap();
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap()
        };
        assert!(
            !reader.row_order().is_ts_desc(),
            "the file under test must be concat-order"
        );

        // full bitmap: true top-3 DESC crosses both runs (320, 300, 270) —
        // the positional shortcut would have said (300, 250, 200)
        let bitmap = all_set(6);
        let candidates = simple_select(&reader, &bitmap, 3, false).unwrap();
        assert_eq!(candidates, vec![(320, 3), (300, 0), (270, 4)]);
        // ASC: true oldest 3 (200, 220, 250) — positional said (320,270,220)
        let candidates = simple_select(&reader, &bitmap, 3, true).unwrap();
        assert_eq!(candidates, vec![(200, 2), (220, 5), (250, 1)]);

        // bitmap respected: only docs 0, 2, 5 (ts 300, 200, 220) match
        let bitmap = BooleanBuffer::from_iter([true, false, true, false, false, true]);
        let candidates = simple_select(&reader, &bitmap, 2, false).unwrap();
        assert_eq!(candidates, vec![(300, 0), (220, 5)]);
        let candidates = simple_select(&reader, &bitmap, 2, true).unwrap();
        assert_eq!(candidates, vec![(200, 2), (220, 5)]);
        // limit >= matches returns them all, still value-ordered
        let candidates = simple_select(&reader, &bitmap, 10, false).unwrap();
        assert_eq!(candidates, vec![(300, 0), (220, 5), (200, 2)]);
        // empty bitmap
        let empty = BooleanBuffer::new_unset(6);
        assert!(simple_select(&reader, &empty, 5, false).unwrap().is_empty());
    }

    #[test]
    fn test_simple_select_respects_bitmap_and_limit() {
        let reader = build_reader();
        // match docs 1, 4, 6 (ts 106, 103, 101)
        let bitmap =
            BooleanBuffer::from_iter([false, true, false, false, true, false, true, false]);
        let candidates = simple_select(&reader, &bitmap, 2, false).unwrap();
        assert_eq!(candidates, vec![(106, 1), (103, 4)]);
        let candidates = simple_select(&reader, &bitmap, 2, true).unwrap();
        assert_eq!(candidates, vec![(101, 6), (103, 4)]);
        // limit larger than matches returns them all
        let candidates = simple_select(&reader, &bitmap, 10, false).unwrap();
        assert_eq!(candidates.len(), 3);
        // empty bitmap
        let empty = BooleanBuffer::new_unset(8);
        assert!(simple_select(&reader, &empty, 5, false).unwrap().is_empty());
    }

    #[test]
    fn test_simple_histogram_bucket_math() {
        let reader = build_reader();
        let bitmap = all_set(8);
        // buckets of width 2 from ts 100: [100,102) [102,104) [104,106) [106,108)
        let counts = simple_histogram(&reader, &bitmap, 100, 2, 4, 0).unwrap();
        assert_eq!(counts, vec![2, 2, 2, 2]);
        // narrower range drops out-of-range rows, zeros included
        let counts = simple_histogram(&reader, &bitmap, 104, 2, 4, 0).unwrap();
        assert_eq!(counts, vec![2, 2, 0, 0]);
    }
    #[test]
    fn test_whole_file_histogram_bucket_requires_one_in_range_bucket() {
        let reader = build_reader(); // timestamps 100..=107 across two zone chunks
        assert_eq!(
            whole_file_histogram_bucket(&reader, 100, 10, 2, 0).unwrap(),
            Some(0)
        );
        assert_eq!(
            whole_file_histogram_bucket(&reader, 102, 10, 2, 2).unwrap(),
            Some(0),
            "timezone offset keeps the same absolute bucket origin"
        );
        assert_eq!(
            whole_file_histogram_bucket(&reader, 100, 4, 2, 0).unwrap(),
            None,
            "a file crossing a bucket edge cannot use one count"
        );
        assert_eq!(
            whole_file_histogram_bucket(&reader, 200, 10, 2, 0).unwrap(),
            None,
            "an out-of-grid file contributes no in-range bucket"
        );
    }

    #[test]
    fn test_simple_histogram_ts_offset_shifts_origin() {
        let reader = build_reader();
        let bitmap = all_set(8);
        // origin = min_value - ts_offset = 102 - 2 = 100: same buckets as
        // min_value=100 without offset
        let counts = simple_histogram(&reader, &bitmap, 102, 2, 4, 2).unwrap();
        assert_eq!(counts, vec![2, 2, 2, 2]);
        // matches the old collector: shifting the range down by the offset
        let baseline = simple_histogram(&reader, &bitmap, 100, 2, 4, 0).unwrap();
        assert_eq!(counts, baseline);
    }

    #[test]
    fn test_simple_histogram_respects_bitmap() {
        let reader = build_reader();
        // only docs 0..2 (ts 107, 106, 105)
        let bitmap =
            BooleanBuffer::from_iter([true, true, true, false, false, false, false, false]);
        let counts = simple_histogram(&reader, &bitmap, 100, 4, 2, 0).unwrap();
        assert_eq!(counts, vec![0, 3]);
    }

    #[test]
    fn test_simple_multi_histogram_groups_and_bounds() {
        let reader = build_reader();
        let bitmap = BooleanBuffer::from_iter((0..8).map(|i| i != 5));
        // width 4 over [100, 108): buckets 100 (ts 100..103) and 104 (ts 104..107)
        let rows = simple_multi_histogram(&reader, &bitmap, 100, 108, 4, 0, "level").unwrap();
        // bucket 100: docs 4..7 = info(103), null(102), info(101), error(100)
        // bucket 104: docs 0..3 = error(107), info(106), error(105), info(104)
        assert_eq!(
            rows,
            vec![
                (100, "info".to_string(), 2),
                (100, "error".to_string(), 1),
                (104, "error".to_string(), 2),
                (104, "info".to_string(), 2),
            ]
        );
        // [min, max) is half-open: max=104 drops the second bucket
        let rows = simple_multi_histogram(&reader, &bitmap, 100, 104, 4, 0, "level").unwrap();
        assert_eq!(
            rows,
            vec![(100, "info".to_string(), 2), (100, "error".to_string(), 1),]
        );
    }

    #[test]
    fn test_simple_multi_histogram_ts_offset() {
        let reader = build_reader();
        let bitmap = BooleanBuffer::from_iter((0..8).map(|i| i != 5));
        // local range [102, 110) with offset 2 = raw range [100, 108); keys
        // come back in local space
        let rows = simple_multi_histogram(&reader, &bitmap, 102, 110, 4, 2, "level").unwrap();
        assert_eq!(
            rows,
            vec![
                (102, "info".to_string(), 2),
                (102, "error".to_string(), 1),
                (106, "error".to_string(), 2),
                (106, "info".to_string(), 2),
            ]
        );
    }

    #[test]
    fn test_simple_multi_histogram_numeric_breakdown() {
        let reader = build_reader();
        let bitmap = all_set(8);
        let error = simple_multi_histogram(&reader, &bitmap, 100, 108, 8, 0, "code").unwrap_err();
        assert!(error.is::<AggregateFallback>());
    }

    #[test]
    fn test_simple_top_n_single_field() {
        let reader = build_reader();
        let bitmap = all_set(8);
        let mut groups =
            simple_top_n(&reader, &bitmap, &["service".to_string()], 10, false).unwrap();
        groups.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        assert_eq!(
            groups,
            vec![
                (vec!["a".to_string()], 5),
                (vec!["b".to_string()], 2),
                (vec!["c".to_string()], 1),
            ]
        );
    }

    #[test]
    fn test_simple_top_n_multi_field_requires_null_scan() {
        let reader = build_reader();
        let bitmap = all_set(8);
        let error = simple_top_n(
            &reader,
            &bitmap,
            &["level".to_string(), "service".to_string()],
            10,
            false,
        )
        .unwrap_err();
        assert!(error.is::<AggregateFallback>());
        let bitmap = BooleanBuffer::from_iter((0..8).map(|i| i != 5));
        let groups = simple_top_n(
            &reader,
            &bitmap,
            &["level".to_string(), "service".to_string()],
            1,
            false,
        )
        .unwrap();
        assert_eq!(groups.iter().map(|(_, count)| *count).sum::<u64>(), 7);
        assert_eq!(groups.len(), 5);
    }

    #[test]
    fn test_simple_top_n_refuses_numeric_type_uncertainty() {
        let reader = build_reader();
        let bitmap = BooleanBuffer::from_iter([true, true, true, true, false, false, false, false]);
        for field in ["code", "ratio"] {
            assert!(
                simple_top_n(&reader, &bitmap, &[field.to_owned()], 10, false)
                    .unwrap_err()
                    .is::<AggregateFallback>()
            );
        }
    }

    #[test]
    fn test_simple_top_n_field_count_bounds() {
        let reader = build_reader();
        let bitmap = all_set(8);
        assert!(simple_top_n(&reader, &bitmap, &[], 10, false).is_err());
        let too_many: Vec<String> = (0..5).map(|i| format!("f{i}")).collect();
        assert!(simple_top_n(&reader, &bitmap, &too_many, 10, false).is_err());
    }

    #[test]
    fn test_simple_distinct_ordering() {
        let reader = build_reader();
        let bitmap = all_set(8);
        // services sorted: a, b, c
        let values = simple_distinct(&reader, &bitmap, "service", 2, true).unwrap();
        assert_eq!(values, HashSet::from(["a".to_string(), "b".to_string()]));
        let values = simple_distinct(&reader, &bitmap, "service", 2, false).unwrap();
        assert_eq!(values, HashSet::from(["b".to_string(), "c".to_string()]));
        // limit above the distinct count returns everything
        let values = simple_distinct(&reader, &bitmap, "service", 10, true).unwrap();
        assert_eq!(
            values,
            HashSet::from(["a".to_owned(), "b".to_owned(), "c".to_owned()])
        );
    }

    #[test]
    fn test_simple_distinct_respects_bitmap_and_nulls() {
        let reader = build_reader();
        // docs 3..6: services a, b, a; levels info, info, null, info
        let bitmap = BooleanBuffer::from_iter([false, false, false, true, true, true, true, false]);
        let values = simple_distinct(&reader, &bitmap, "service", 10, true).unwrap();
        assert_eq!(values, HashSet::from(["a".to_string(), "b".to_string()]));
        // NULL cannot be encoded in the String result, even after enough
        // ordered candidates have already been found.
        assert!(
            simple_distinct(&reader, &bitmap, "level", 1, true)
                .unwrap_err()
                .is::<AggregateFallback>()
        );
    }

    #[test]
    fn test_distinct_null_type_and_missing_refusals() {
        let reader = build_reader();
        for field in ["level", "absent", "code", "ratio"] {
            assert!(
                simple_distinct(&reader, &all_set(8), field, 1, false)
                    .unwrap_err()
                    .is::<AggregateFallback>()
            );
        }
        for field in ["level", "absent"] {
            assert!(
                unfiltered_distinct(&reader, field, 1, false)
                    .unwrap_err()
                    .is::<AggregateFallback>()
            );
            assert!(
                filtered_distinct(&reader, &all_set(8), field, 1, false)
                    .unwrap_err()
                    .is::<AggregateFallback>()
            );
        }
        let no_null = BooleanBuffer::from_iter((0..8).map(|i| i != 5));
        assert_eq!(
            filtered_distinct(&reader, &no_null, "level", 1, true).unwrap(),
            Some(HashSet::from(["error".to_owned()]))
        );
        assert_eq!(
            simple_distinct(&reader, &no_null, "level", 1, false).unwrap(),
            HashSet::from(["info".to_owned()])
        );
    }

    #[test]
    fn test_ordered_distinct_candidates_preserve_global_order_across_files() {
        let files = [vec!["z", "z", "é", "b", ""], vec!["中", "z", "b", "b", "a"]];
        let all: BTreeSet<_> = files.iter().flatten().copied().collect();
        for ascend in [true, false] {
            let mut candidates = BTreeSet::new();
            for values in &files {
                let ts = vec![100; values.len()];
                let strings: Vec<_> = values.iter().copied().map(Some).collect();
                let (reader, _) = zoned_and_decode(&ts, &strings);
                candidates
                    .extend(simple_distinct(&reader, &all_set(ts.len()), "bd", 2, ascend).unwrap());
            }
            let got: Vec<_> = if ascend {
                candidates.into_iter().take(2).collect()
            } else {
                candidates.into_iter().rev().take(2).collect()
            };
            let expected: Vec<_> = if ascend {
                all.iter().take(2).map(|v| (*v).to_owned()).collect()
            } else {
                all.iter().rev().take(2).map(|v| (*v).to_owned()).collect()
            };
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn test_distinct_budget_refuses_needed_keys_instead_of_approximating() {
        let mut distinct = OrderedDistinct::new(1, true).unwrap();
        distinct.max_bytes = 2;
        distinct.insert("b").unwrap();
        // Worse keys can be omitted by the ordering proof, regardless of size.
        distinct.insert("zzzz").unwrap();
        // A better key cannot be omitted merely because it exceeds the budget.
        assert!(
            distinct
                .insert("aaaa")
                .unwrap_err()
                .is::<AggregateFallback>()
        );
        let reader = build_reader();
        assert!(
            simple_distinct(
                &reader,
                &all_set(8),
                "service",
                top_n_limits().max_groups + 1,
                true
            )
            .unwrap_err()
            .is::<AggregateFallback>()
        );
    }

    /// IS NOT NULL evaluates through the key terms: doc 5 has a null
    /// `level`, so its key term is absent for that doc.
    #[test]
    fn test_is_not_null_condition_over_key_terms() {
        let reader = build_reader();
        let condition = crate::index::IndexCondition {
            conditions: vec![crate::index::Condition::IsNotNull("level".to_string())],
        };
        let (query, has_skipped) = condition
            .to_vix_query(
                "test",
                &|field| super::super::field_capability("test", &reader, field),
                &super::super::index_match_all_tokens,
            )
            .unwrap();
        assert!(!has_skipped);
        let bitmap = reader.eval(&query).unwrap();
        assert_eq!(bitmap.count_set_bits(), 7);
        assert!(!bitmap.value(5), "doc 5 has a null level");
        // count() answers from the key term doc count without postings
        assert_eq!(reader.count(&query).unwrap(), 7);
        // a path no document carries matches nothing (still not a skip)
        let condition = crate::index::IndexCondition {
            conditions: vec![crate::index::Condition::IsNotNull(
                "missing_field".to_string(),
            )],
        };
        let (query, has_skipped) = condition
            .to_vix_query(
                "test",
                &|field| super::super::field_capability("test", &reader, field),
                &super::super::index_match_all_tokens,
            )
            .unwrap();
        assert!(!has_skipped);
        assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 0);
    }

    /// A field entirely absent from the file (no key term): every
    /// index-servable condition shape maps to `VixQuery::Nothing` and
    /// matches nothing — exactly, with no skip — so the caller eliminates
    /// the file instead of scanning it.
    #[test]
    fn test_absent_field_conditions_match_nothing() {
        let reader = build_reader();
        assert!(!reader.key_term_exists("client_id").unwrap());

        for condition in [
            crate::index::Condition::Equal("client_id".to_string(), "x".to_string()),
            crate::index::Condition::NotEqual("client_id".to_string(), "x".to_string()),
            crate::index::Condition::In("client_id".to_string(), vec!["x".to_string()], false),
            crate::index::Condition::StrMatch("client_id".to_string(), "x".to_string(), true),
        ] {
            let index_condition = crate::index::IndexCondition {
                conditions: vec![condition.clone()],
            };
            let (query, has_skipped) = index_condition
                .to_vix_query(
                    "test",
                    &|field| super::super::field_capability("test", &reader, field),
                    &super::super::index_match_all_tokens,
                )
                .unwrap_or_else(|e| panic!("{condition:?} must evaluate: {e}"));
            assert!(!has_skipped, "{condition:?} must not skip");
            assert_eq!(query, vortex_index::VixQuery::Nothing, "{condition:?}");
            assert_eq!(
                reader.eval(&query).unwrap().count_set_bits(),
                0,
                "{condition:?}"
            );
            assert_eq!(reader.count(&query).unwrap(), 0, "{condition:?}");
        }

        // IS NOT NULL on the absent field: KeyExists → all zeros (ungated)
        let index_condition = crate::index::IndexCondition {
            conditions: vec![crate::index::Condition::IsNotNull("client_id".to_string())],
        };
        let (query, has_skipped) = index_condition
            .to_vix_query(
                "test",
                &|field| super::super::field_capability("test", &reader, field),
                &super::super::index_match_all_tokens,
            )
            .unwrap();
        assert!(!has_skipped);
        assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 0);

        // absent + servable under one AND list: still exact, still empty
        let index_condition = crate::index::IndexCondition {
            conditions: vec![
                crate::index::Condition::Equal("client_id".to_string(), "x".to_string()),
                crate::index::Condition::Equal("service".to_string(), "a".to_string()),
            ],
        };
        let (query, has_skipped) = index_condition
            .to_vix_query(
                "test",
                &|field| super::super::field_capability("test", &reader, field),
                &super::super::index_match_all_tokens,
            )
            .unwrap();
        assert!(!has_skipped);
        assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 0);
        assert_eq!(reader.count(&query).unwrap(), 0);
    }

    /// Pilot fix B: the dictionary-only collectors agree with the docs-column
    /// collectors on every servable field, and refuse fields whose
    /// dictionary cannot prove exact per-value counts.
    #[test]
    fn test_unfiltered_collectors_match_docs_collectors() {
        let reader = build_reader();
        let bitmap = all_set(8);

        // service: term+cs, no nulls -> dict serves; counts match the docs path
        let dict = unfiltered_top_n(&reader, "service", 10, false)
            .unwrap()
            .expect("service must be dictionary-servable");
        let mut dict_sorted = dict.clone();
        dict_sorted.sort();
        let mut docs_sorted =
            simple_top_n(&reader, &bitmap, &["service".to_string()], 10, false).unwrap();
        docs_sorted.sort();
        assert_eq!(dict_sorted, docs_sorted);
        // the dictionary path is exact: a 5/2/1 split over 8 docs
        assert_eq!(
            dict_sorted,
            vec![
                (vec!["a".to_string()], 5),
                (vec!["b".to_string()], 2),
                (vec!["c".to_string()], 1),
            ]
        );

        // Key-term reconciliation does not account for the SQL NULL group.
        assert!(
            unfiltered_top_n(&reader, "level", 10, false)
                .unwrap_err()
                .is::<AggregateFallback>()
        );

        // distinct via the dictionary matches the docs path, asc and desc
        for ascend in [true, false] {
            assert_eq!(
                unfiltered_distinct(&reader, "service", 2, ascend)
                    .unwrap()
                    .expect("service must be dictionary-servable"),
                simple_distinct(&reader, &bitmap, "service", 2, ascend).unwrap()
            );
        }

        // numeric cs field: values are not in the dictionary -> refused
        assert!(
            unfiltered_top_n(&reader, "ratio", 10, false)
                .unwrap()
                .is_none()
        );
        assert!(
            unfiltered_distinct(&reader, "code", 10, true)
                .unwrap()
                .is_none()
        );
    }

    /// Row-access planning: full match reads whole columns, moderate
    /// selectivity reads whole columns filtered through the bitmap, and
    /// only needle-grade selectivity pays per-row point reads.
    #[test]
    fn test_row_access_plan() {
        let all = BooleanBuffer::new_set(200);
        assert!(matches!(RowAccess::plan(&all), RowAccess::AllRows));
        // 100 of 200 matched: full read + bitmap filter
        let half = BooleanBuffer::from_iter((0..200).map(|i| i % 2 == 0));
        assert!(matches!(RowAccess::plan(&half), RowAccess::Filtered));
        // 1 of 200 matched (0.5% < 2%): point read
        let needle = BooleanBuffer::from_iter((0..200).map(|i| i == 42));
        match RowAccess::plan(&needle) {
            RowAccess::Rows(rows) => assert_eq!(rows, vec![42]),
            other => panic!(
                "expected point reads, got {}",
                match other {
                    RowAccess::AllRows => "AllRows",
                    RowAccess::Filtered => "Filtered",
                    RowAccess::Rows(_) => unreachable!(),
                }
            ),
        }
        // filtered visiting hits exactly the set positions
        let mut seen = Vec::new();
        RowAccess::Filtered.for_each_position(&half, 200, |i| seen.push(i));
        assert_eq!(seen, (0..200).filter(|i| i % 2 == 0).collect::<Vec<_>>());
    }

    /// Code grouping agrees with exact selected raw-string counts.
    #[test]
    fn test_dict_group_counts_matches_string_path() {
        let reader = build_reader();
        let full = all_set(8);
        // 4 of 8 rows: Filtered access (50% > the point-read threshold)
        let partial =
            BooleanBuffer::from_iter([true, false, true, true, false, false, true, false]);
        for (bitmap, name) in [(&full, "full"), (&partial, "partial")] {
            for column in ["service"] {
                let dict = dict_group_counts(&reader, column, bitmap).unwrap();
                // string-path reference counts
                let values = read_column_strings(&reader, column, None).unwrap();
                let mut expected: HashMap<String, u64> = HashMap::new();
                for i in 0..values.len() {
                    if bitmap.value(i) && !values.is_null(i) {
                        *expected.entry(values.value(i).to_string()).or_insert(0) += 1;
                    }
                }
                assert_eq!(dict, expected, "column {column}, bitmap {name}");
            }
        }
        // and the public collectors serve identical results through it
        let groups = simple_top_n(&reader, &partial, &["service".to_string()], 10, false).unwrap();
        let mut totals: HashMap<String, u64> = HashMap::new();
        for (key, count) in groups {
            *totals.entry(key[0].clone()).or_insert(0) += count;
        }
        assert_eq!(
            totals,
            HashMap::from_iter([("a".to_string(), 3), ("b".to_string(), 1)])
        );
        assert_eq!(
            simple_distinct(&reader, &partial, "service", 10, true).unwrap(),
            HashSet::from(["a".to_string(), "b".to_string()])
        );
    }

    /// The count fast path regression: match bitmaps and doc counts line up
    /// with the docs columns the collectors read.
    #[test]
    fn test_count_matches_bitmap() {
        let reader = build_reader();
        let (query, _) = crate::index::IndexCondition {
            conditions: vec![crate::index::Condition::Equal(
                "level".to_string(),
                "error".to_string(),
            )],
        }
        .to_vix_query(
            "test",
            &|field| super::super::field_capability("test", &reader, field),
            &super::super::index_match_all_tokens,
        )
        .unwrap();
        assert_eq!(reader.count(&query).unwrap(), 3);
        let bitmap = reader.eval(&query).unwrap();
        assert_eq!(bitmap.count_set_bits(), 3);
        // and the collectors see the same rows
        let candidates = simple_select(&reader, &bitmap, 10, false).unwrap();
        assert_eq!(candidates.len(), 3);
        assert_eq!(candidates[0], (107, 0));
    }

    // ------------------------------------------------------------------
    // Zone-map fast-path differential + perf tests.
    // ------------------------------------------------------------------

    /// Build a multi-chunk file over `ts` (one row per element, in the given
    /// order) with a `bd` utf8 column-store breakdown field. `docs_chunk_bytes
    /// = 1` blocks the zone table at the 64-row floor, so any dataset over ~64
    /// rows spans several chunks.
    fn build_zone_file(ts: &[i64], bd: &[Option<&str>]) -> (Vec<u8>, Option<Vec<u8>>) {
        build_zone_file_with_chunk_bytes(ts, bd, 1)
    }

    fn build_zone_file_with_chunk_bytes(
        ts: &[i64],
        bd: &[Option<&str>],
        docs_chunk_bytes: usize,
    ) -> (Vec<u8>, Option<Vec<u8>>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("bd", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            docs_chunk_bytes,
            ..Default::default()
        };
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(bd.to_vec())),
            ],
        )
        .unwrap();
        let sources: Vec<String> = (0..ts.len())
            .map(|i| match bd[i] {
                Some(v) => format!(r#"{{"_timestamp":{},"bd":"{v}"}}"#, ts[i]),
                None => format!(r#"{{"_timestamp":{}}}"#, ts[i]),
            })
            .collect();
        let sources = StringArray::from_iter_values(sources.iter().map(String::as_str));
        let mut writer = VixWriter::new(&schema, opts, false);
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        writer.finish().unwrap()
    }

    fn build_numeric_zone_reader(ts: &[i64], values: &[Option<i64>]) -> VixReader {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.to_vec())) as ArrayRef,
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap();
        let sources =
            StringArray::from_iter_values(ts.iter().map(|ts| format!(r#"{{"_timestamp":{ts}}}"#)));
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                docs_chunk_bytes: 1,
                ..Default::default()
            },
            false,
        );
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from)).unwrap()
    }

    /// The zone-map reader and the decode-path reader (zone table stripped)
    /// over the same bytes.
    fn zoned_and_decode(ts: &[i64], bd: &[Option<&str>]) -> (VixReader, VixReader) {
        let (bytes, index) = build_zone_file(ts, bd);
        let index = index.map(bytes::Bytes::from);
        let zoned =
            VixReader::open_with_index(bytes::Bytes::from(bytes.clone()), index.clone()).unwrap();
        let decode = VixReader::open_with_index(
            bytes::Bytes::from(
                vortex_index::test_support::strip_zone_map_property(&bytes).unwrap(),
            ),
            index,
        )
        .unwrap();
        assert!(zoned.zone_chunks().is_some());
        assert!(decode.zone_chunks().is_none());
        (zoned, decode)
    }

    /// Brute-force histogram reference straight off the row timestamps.
    fn brute_histogram(
        ts: &[i64],
        bitmap: &BooleanBuffer,
        min_value: i64,
        width: i64,
        num_buckets: usize,
        ts_offset: i64,
    ) -> Vec<u64> {
        let origin = min_value - ts_offset;
        let mut counts = vec![0u64; num_buckets];
        for (i, &t) in ts.iter().enumerate() {
            if !bitmap.value(i) {
                continue;
            }
            if let Some(off) = t.checked_sub(origin)
                && off >= 0
            {
                let b = (off / width) as usize;
                if b < num_buckets {
                    counts[b] += 1;
                }
            }
        }
        counts
    }

    /// A few bitmaps of decreasing density over `rows` rows.
    fn test_bitmaps(rows: usize) -> Vec<(&'static str, BooleanBuffer)> {
        vec![
            ("all", BooleanBuffer::new_set(rows)),
            (
                "even",
                BooleanBuffer::from_iter((0..rows).map(|i| i % 2 == 0)),
            ),
            (
                "sparse",
                BooleanBuffer::from_iter((0..rows).map(|i| i % 37 == 0)),
            ),
            (
                "window",
                BooleanBuffer::from_iter((0..rows).map(|i| (50..200).contains(&i))),
            ),
        ]
    }

    /// Differential: zone-map `simple_histogram` ≡ decode path ≡ brute force,
    /// over sorted / piecewise / adversarial distributions, filtered and
    /// unfiltered, across bucket widths (incl. narrower than a chunk) and
    /// offsets.
    #[test]
    fn test_simple_histogram_zone_matches_decode() {
        let rows = 300usize;
        let sorted: Vec<i64> = (0..rows as i64).map(|i| 100_000 - i).collect();
        let piecewise: Vec<i64> = (0..150)
            .map(|i| 100_000 - i)
            .chain((0..150).map(|i| 100_075 - i))
            .collect();
        let mut lcg = 0x1234_5678_9ABC_DEF0u64;
        let adversarial: Vec<i64> = (0..rows)
            .map(|_| {
                lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1);
                100_000 - ((lcg >> 40) % 400) as i64
            })
            .collect();
        let bd: Vec<Option<&str>> = (0..rows).map(|_| Some("x")).collect();

        for (name, ts) in [
            ("sorted", &sorted),
            ("piecewise", &piecewise),
            ("adversarial", &adversarial),
        ] {
            let (zoned, decode) = zoned_and_decode(ts, &bd);
            // widths: 1 (narrower than a 64-row chunk's span), 25, 10_000
            // (wider); offsets: 0 and a shift
            for &(min_value, width, num_buckets, ts_offset) in &[
                (99_700i64, 1u64, 400usize, 0i64),
                (99_700, 25, 20, 0),
                (99_700, 25, 20, 7),
                (0, 10_000, 12, 0),
                (99_800, 50, 3, 0), // truncated range (drops out-of-range rows)
            ] {
                let width_i = width as i64;
                for (bname, bitmap) in test_bitmaps(ts.len()) {
                    let want =
                        brute_histogram(ts, &bitmap, min_value, width_i, num_buckets, ts_offset);
                    let z =
                        simple_histogram(&zoned, &bitmap, min_value, width, num_buckets, ts_offset)
                            .unwrap();
                    let d = simple_histogram(
                        &decode,
                        &bitmap,
                        min_value,
                        width,
                        num_buckets,
                        ts_offset,
                    )
                    .unwrap();
                    assert_eq!(z, want, "{name}/{bname} zone width={width}");
                    assert_eq!(d, want, "{name}/{bname} decode width={width}");
                }
            }
        }
    }

    /// Differential: zone-map `simple_multi_histogram` ≡ decode path, over the
    /// same distributions, filtered and unfiltered, with null breakdown rows.
    #[test]
    fn test_simple_multi_histogram_zone_matches_decode() {
        let rows = 300usize;
        let sorted: Vec<i64> = (0..rows as i64).map(|i| 100_000 - i).collect();
        let mut lcg = 0xF00D_BABE_0000_0001u64;
        let adversarial: Vec<i64> = (0..rows)
            .map(|_| {
                lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1);
                100_000 - ((lcg >> 40) % 400) as i64
            })
            .collect();
        let breakdowns = ["a", "b", "c"];
        let bd: Vec<Option<&str>> = (0..rows).map(|i| Some(breakdowns[i % 3])).collect();

        for (name, ts) in [("sorted", &sorted), ("adversarial", &adversarial)] {
            let (zoned, decode) = zoned_and_decode(ts, &bd);
            for &(min_value, max_value, width, ts_offset) in &[
                (99_700i64, 100_001i64, 25u64, 0i64),
                (99_700, 100_001, 1, 0), // narrow buckets
                (99_800, 100_001, 50, 9),
            ] {
                for (bname, bitmap) in test_bitmaps(ts.len()) {
                    let mut z = simple_multi_histogram(
                        &zoned, &bitmap, min_value, max_value, width, ts_offset, "bd",
                    )
                    .unwrap();
                    let mut d = simple_multi_histogram(
                        &decode, &bitmap, min_value, max_value, width, ts_offset, "bd",
                    )
                    .unwrap();
                    z.sort();
                    d.sort();
                    assert_eq!(z, d, "{name}/{bname} multi width={width}");
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn assert_multi_matches_rowwise(
        reader: &VixReader,
        bitmap: &BooleanBuffer,
        min_value: i64,
        max_value: i64,
        bucket_width: u64,
        ts_offset: i64,
        field: &str,
        _expect_dict: bool,
        label: &str,
    ) -> Vec<(i64, String, u64)> {
        // Both dense masks and sparse point reads must retain complete counts.
        let width = i64::try_from(bucket_width).unwrap();
        let raw_min = min_value - ts_offset;
        let raw_max = max_value - ts_offset;
        assert!(
            reader
                .zone_chunks()
                .unwrap()
                .iter()
                .all(|chunk| chunk.ts_min >= raw_min && chunk.ts_max < raw_max),
            "{label} must exercise the fully-covered zone path"
        );
        let dict = dict_multi_histogram(
            reader,
            bitmap,
            min_value,
            raw_min,
            raw_max,
            width,
            field,
            DEFAULT_DICT_MULTI_HISTOGRAM_LIMITS,
        )
        .unwrap();
        let rowwise = rowwise_multi_histogram_reference(
            reader,
            bitmap,
            min_value,
            max_value,
            bucket_width,
            ts_offset,
            field,
        );
        assert_eq!(dict, rowwise, "{label}: dictionary vs row-wise");
        let optimized = simple_multi_histogram(
            reader,
            bitmap,
            min_value,
            max_value,
            bucket_width,
            ts_offset,
            field,
        )
        .unwrap();
        assert_eq!(optimized, rowwise, "{label}: public path vs row-wise");
        optimized
    }

    /// Differential for the production path: multiple per-chunk string
    /// dictionaries, null rows, exact lower/upper-adjacent timestamps and
    /// internal bucket edges. ALL exercises dictionary codes; a dense
    /// filtered bitmap proves the exact zone-aware row-wise fallback.
    #[test]
    fn test_dict_multi_histogram_string_matches_rowwise() {
        // Exceed the writer's 65,536-row physical chunk ceiling: Vortex may
        // coalesce tiny logical chunks up to ~1 MiB, so a 320-row fixture is
        // not sufficient to prove cross-dictionary row-offset handling.
        const ROWS: usize = 70_000;
        const RAW_MIN: i64 = 10_000;
        const RAW_MAX: i64 = RAW_MIN + ROWS as i64;
        const OFFSET: i64 = 7;
        const WIDTH: u64 = 4_096;
        let ts: Vec<i64> = (RAW_MIN..RAW_MAX).rev().collect();
        let terms = ["aa", "bb", "cc", "dd", "ee", "ff", "gg"];
        let breakdown: Vec<Option<&str>> = (0..ROWS)
            .map(|i| Some(terms[((i / 64) * 3 + i) % terms.len()]))
            .collect();
        let (data, index) = build_zone_file(&ts, &breakdown);
        let data = bytes::Bytes::from(data);
        let reader =
            VixReader::open_with_index(data.clone(), index.map(bytes::Bytes::from)).unwrap();
        let data_only = VixReader::open_with_index(data, None).unwrap();
        assert!(
            reader.read_docs_column_dict("bd").unwrap().len() > 1,
            "fixture must contain multiple dictionaries"
        );

        for (label, bitmap, expect_dict) in [
            ("all", BooleanBuffer::new_set(ROWS), true),
            (
                "dense-filtered",
                BooleanBuffer::from_iter((0..ROWS).map(|i| i % 4 != 2)),
                false,
            ),
        ] {
            let rows = assert_multi_matches_rowwise(
                &reader,
                &bitmap,
                RAW_MIN + OFFSET,
                RAW_MAX + OFFSET,
                WIDTH,
                OFFSET,
                "bd",
                expect_dict,
                label,
            );
            assert!(
                rows.iter().any(|row| row.0 == RAW_MIN + OFFSET),
                "{label}: lower-bound timestamp must be retained"
            );
            let final_bucket = RAW_MIN + OFFSET + ((ROWS as i64 - 1) / WIDTH as i64) * WIDTH as i64;
            assert!(
                rows.iter().any(|row| row.0 == final_bucket),
                "{label}: upper-adjacent timestamp must reach the final bucket"
            );
            let expected = (0..ROWS)
                .filter(|&i| bitmap.value(i) && breakdown[i].is_some())
                .count() as u64;
            assert_eq!(
                rows.iter().map(|row| row.2).sum::<u64>(),
                expected,
                "{label}: every matched row contributes once"
            );
        }
        assert!(!data_only.has_index());
        assert_multi_matches_rowwise(
            &data_only,
            &BooleanBuffer::new_set(ROWS),
            RAW_MIN + OFFSET,
            RAW_MAX + OFFSET,
            WIDTH,
            OFFSET,
            "bd",
            true,
            "all-data-only",
        );
        let sparse = BooleanBuffer::from_iter((0..ROWS).map(|i| i == 0));
        assert_eq!(
            simple_multi_histogram(
                &reader,
                &sparse,
                RAW_MIN + OFFSET,
                RAW_MAX + OFFSET,
                WIDTH,
                OFFSET,
                "bd",
            )
            .unwrap(),
            rowwise_multi_histogram_reference(
                &reader,
                &sparse,
                RAW_MIN + OFFSET,
                RAW_MAX + OFFSET,
                WIDTH,
                OFFSET,
                "bd",
            ),
        );
    }

    #[test]
    fn test_dict_multi_histogram_metadata_caps_fall_back_exactly() {
        let reader = build_reader();
        let bitmap = BooleanBuffer::new_set(8);
        let cases = [
            (
                "distinct-values",
                DictMultiHistogramLimits {
                    max_values: 1,
                    ..DEFAULT_DICT_MULTI_HISTOGRAM_LIMITS
                },
            ),
            (
                "owned-value-bytes",
                DictMultiHistogramLimits {
                    max_value_bytes: 0,
                    ..DEFAULT_DICT_MULTI_HISTOGRAM_LIMITS
                },
            ),
            (
                "groups",
                DictMultiHistogramLimits {
                    max_groups: 1,
                    ..DEFAULT_DICT_MULTI_HISTOGRAM_LIMITS
                },
            ),
        ];
        for (label, limits) in cases {
            let dict = dict_multi_histogram(&reader, &bitmap, 100, 100, 108, 4, "service", limits)
                .unwrap_err();
            assert!(
                dict.is::<AggregateFallback>(),
                "{label}: must route directly to precise scan"
            );
        }
    }

    /// Unsupported typed groups must not silently stringify or omit NULLs.
    #[test]
    fn test_dict_multi_histogram_numeric_refuses() {
        let reader = build_numeric_zone_reader(&[100, 101], &[Some(200), None]);
        let error =
            simple_multi_histogram(&reader, &all_set(2), 100, 102, 1, 0, "code").unwrap_err();
        assert!(error.is::<AggregateFallback>());
    }

    /// Manual release-mode microbenchmark for the dense production shape.
    /// The file build and initial parity check are excluded from timings.
    #[test]
    #[ignore = "manual release-mode microbenchmark"]
    fn benchmark_dict_multi_histogram_against_rowwise() {
        const ROWS: usize = 262_144;
        const SAMPLES: usize = 7;
        const RAW_MIN: i64 = 1_800_000_000_000_000;
        const WIDTH: u64 = 30_000;
        let ts: Vec<i64> = (RAW_MIN..RAW_MIN + ROWS as i64).rev().collect();
        let terms = [
            "api", "auth", "billing", "cache", "catalog", "checkout", "cron", "edge", "email",
            "events", "gateway", "ingest", "jobs", "metrics", "search", "worker",
        ];
        let breakdown: Vec<Option<&str>> = (0..ROWS)
            .map(|i| Some(terms[((i / 4096) * 5 + i.wrapping_mul(13)) % terms.len()]))
            .collect();
        let (data, _index) = build_zone_file_with_chunk_bytes(&ts, &breakdown, 64 * 1024);
        let reader = VixReader::open_with_index(bytes::Bytes::from(data), None).unwrap();
        assert!(!reader.has_index(), "benchmark must exercise data-only ALL");
        let bitmap = BooleanBuffer::new_set(ROWS);
        let min_value = RAW_MIN;
        let max_value = RAW_MIN + ROWS as i64;
        let dict_chunks = reader.read_docs_column_dict("bd").unwrap().len();
        assert!(dict_chunks > 1, "benchmark requires multiple dictionaries");

        let optimized =
            simple_multi_histogram(&reader, &bitmap, min_value, max_value, WIDTH, 0, "bd").unwrap();
        let rowwise = rowwise_multi_histogram_reference(
            &reader, &bitmap, min_value, max_value, WIDTH, 0, "bd",
        );
        assert_eq!(optimized, rowwise, "benchmark parity");

        let mut optimized_samples = Vec::with_capacity(SAMPLES);
        let mut rowwise_samples = Vec::with_capacity(SAMPLES);
        for sample in 0..SAMPLES {
            if sample % 2 == 0 {
                let started = std::time::Instant::now();
                std::hint::black_box(
                    simple_multi_histogram(&reader, &bitmap, min_value, max_value, WIDTH, 0, "bd")
                        .unwrap(),
                );
                optimized_samples.push(started.elapsed());

                let started = std::time::Instant::now();
                std::hint::black_box(rowwise_multi_histogram_reference(
                    &reader, &bitmap, min_value, max_value, WIDTH, 0, "bd",
                ));
                rowwise_samples.push(started.elapsed());
            } else {
                let started = std::time::Instant::now();
                std::hint::black_box(rowwise_multi_histogram_reference(
                    &reader, &bitmap, min_value, max_value, WIDTH, 0, "bd",
                ));
                rowwise_samples.push(started.elapsed());

                let started = std::time::Instant::now();
                std::hint::black_box(
                    simple_multi_histogram(&reader, &bitmap, min_value, max_value, WIDTH, 0, "bd")
                        .unwrap(),
                );
                optimized_samples.push(started.elapsed());
            }
        }
        optimized_samples.sort_unstable();
        rowwise_samples.sort_unstable();
        let optimized_median = optimized_samples[SAMPLES / 2];
        let rowwise_median = rowwise_samples[SAMPLES / 2];
        println!(
            "simple_multi_histogram rows={ROWS} dict_chunks={dict_chunks} samples={SAMPLES} \
             optimized_median_us={} rowwise_median_us={} speedup={:.3} parity=true",
            optimized_median.as_micros(),
            rowwise_median.as_micros(),
            rowwise_median.as_secs_f64() / optimized_median.as_secs_f64(),
        );
    }

    /// Perf shape: an unfiltered histogram over a sorted file with buckets
    /// WIDER than a chunk's time span decodes only the chunks that straddle a
    /// bucket edge — interior chunks fold in from the zone table with no
    /// decode.
    #[test]
    fn test_zone_histogram_decodes_only_boundary_chunks() {
        let rows = 640usize; // 10 chunks of 64 rows
        let ts: Vec<i64> = (0..rows as i64).map(|i| 100_000 - i).collect(); // sorted DESC
        let bd: Vec<Option<&str>> = (0..rows).map(|_| Some("x")).collect();
        let (zoned, _decode) = zoned_and_decode(&ts, &bd);
        let chunks = zoned.zone_chunks().unwrap().len();
        assert!(chunks >= 8, "need several chunks, got {chunks}");

        // bucket width 200 over ts [99_361, 100_000]: each 64-row chunk spans
        // ~64 ts, far narrower than a bucket, so at most one bucket edge cuts
        // through any chunk
        let bitmap = BooleanBuffer::new_set(rows);
        reset_decoded_histogram_chunks();
        let hist = simple_histogram(&zoned, &bitmap, 99_000, 200, 10, 0).unwrap();
        let decoded = decoded_histogram_chunks();
        assert_eq!(hist.iter().sum::<u64>(), rows as u64, "all rows counted");
        // far fewer decodes than chunks: only bucket-straddling chunks
        assert!(
            decoded < chunks,
            "expected boundary-only decodes, decoded {decoded} of {chunks} chunks"
        );
        assert!(
            decoded <= 10,
            "at most one decode per bucket boundary, got {decoded}"
        );

        // correctness vs the decode path on the same query
        let hist_decode = simple_histogram(&_decode, &bitmap, 99_000, 200, 10, 0).unwrap();
        assert_eq!(hist, hist_decode);
    }

    /// A PARTIAL bitmap folds the same way: a chunk wholly inside one bucket
    /// contributes its matched count without decoding `_timestamp` — every
    /// row of the chunk is in that bucket regardless of which rows matched.
    /// (Before the relaxation any chunk with a partial bitmap decoded, which
    /// made filtered histograms pay the full timestamp-decode cost.)
    #[test]
    fn test_zone_histogram_partial_bitmap_folds_without_decode() {
        let rows = 640usize; // 10 chunks of 64 rows
        let ts: Vec<i64> = (0..rows as i64).map(|i| 100_000 - i).collect(); // sorted DESC
        let bd: Vec<Option<&str>> = (0..rows).map(|_| Some("x")).collect();
        let (zoned, decode) = zoned_and_decode(&ts, &bd);
        let chunks = zoned.zone_chunks().unwrap().len();
        assert!(chunks >= 8, "need several chunks, got {chunks}");

        // ~12% selectivity spread across every chunk — the filtered-histogram
        // shape that used to decode all of them
        let bitmap = BooleanBuffer::from_iter((0..rows).map(|i| i % 8 == 0));
        reset_decoded_histogram_chunks();
        let hist = simple_histogram(&zoned, &bitmap, 99_000, 200, 10, 0).unwrap();
        let decoded = decoded_histogram_chunks();
        assert_eq!(
            hist.iter().sum::<u64>(),
            (rows / 8) as u64,
            "all matched rows counted"
        );
        assert!(
            decoded <= 10,
            "single-bucket chunks must fold their matched count, got {decoded} decodes of {chunks} chunks"
        );

        // correctness vs the decode path on the same query
        let hist_decode = simple_histogram(&decode, &bitmap, 99_000, 200, 10, 0).unwrap();
        assert_eq!(hist, hist_decode);
    }
    // ---------------- M16: stats-answered aggregation arms ----------------

    const M16_ROWS: usize = 256; // 4 chunks x 64 rows at the chunk floor

    /// Multi-chunk M16 fixture. Columns:
    /// - `code`: i64, dense (per-chunk stats rows exist)
    /// - `ratio`: f64 with nulls (dense enough for stats)
    /// - `sparse`: i64 at ~2.7% density — BELOW the 10% stats threshold (file-level presence only,
    ///   no chunk rows)
    /// - `void`: i64, all NULL
    /// - `name`: utf8 (string stats are prefix bounds — never answers)
    /// - `gate`: i64 with the §4 verdict shapes per chunk — all-7s / mixed 7-and-8 / all-9s /
    ///   7s-with-nulls
    ///
    /// `with_stats = false` raises the density threshold above 1.0 so NO
    /// column gets chunk rows (the M1-era no-stats shape).
    fn m16_fixture(with_stats: bool) -> (VixReader, M16Data) {
        let ts: Vec<i64> = (0..M16_ROWS as i64).map(|i| 100_000 - i).collect();
        let code: Vec<Option<i64>> = (0..M16_ROWS).map(|i| Some((i as i64) % 97)).collect();
        let ratio: Vec<Option<f64>> = (0..M16_ROWS)
            .map(|i| (i % 8 != 7).then(|| i as f64 * 0.5 - 30.0))
            .collect();
        let sparse: Vec<Option<i64>> = (0..M16_ROWS)
            .map(|i| (i % 40 == 0).then(|| i as i64 * 3))
            .collect();
        let void: Vec<Option<i64>> = vec![None; M16_ROWS];
        let name: Vec<Option<String>> = (0..M16_ROWS)
            .map(|i| Some(format!("n-{}", i % 5)))
            .collect();
        let gate: Vec<Option<i64>> = (0..M16_ROWS)
            .map(|i| match i / 64 {
                0 => Some(7),
                1 => Some(if i % 2 == 0 { 7 } else { 8 }),
                2 => Some(9),
                _ => (i % 4 != 0).then_some(7),
            })
            .collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("sparse", DataType::Int64, true),
            Field::new("void", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("gate", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(Int64Array::from(code.clone())),
                Arc::new(Float64Array::from(ratio.clone())),
                Arc::new(Int64Array::from(sparse.clone())),
                Arc::new(Int64Array::from(void.clone())),
                Arc::new(StringArray::from(
                    name.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(gate.clone())),
            ],
        )
        .unwrap();
        let sources = StringArray::from(vec!["{}"; M16_ROWS]);
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                docs_chunk_bytes: 1, // 64-row chunk floor
                stats_min_density: if with_stats { 0.0 } else { 2.0 },
                ..Default::default()
            },
            false,
        );
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        let reader = {
            let (data, index) = writer.finish().unwrap();
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap()
        };
        assert_eq!(reader.zone_chunks().unwrap().len(), 4);
        if with_stats {
            assert!(
                column_stats_table(&reader, "code").is_some(),
                "dense column must carry chunk stats"
            );
            assert!(
                column_stats_table(&reader, "sparse").is_none(),
                "a ~2.7%-density column must stay below the stats threshold"
            );
        } else {
            assert!(
                reader.column_chunk_stats().is_none()
                    || column_stats_table(&reader, "code").is_none(),
                "the no-stats fixture must carry no usable chunk rows"
            );
        }
        (
            reader,
            M16Data {
                ts,
                code,
                ratio,
                sparse,
                void,
                gate,
            },
        )
    }

    struct M16Data {
        ts: Vec<i64>,
        code: Vec<Option<i64>>,
        ratio: Vec<Option<f64>>,
        sparse: Vec<Option<i64>>,
        void: Vec<Option<i64>>,
        gate: Vec<Option<i64>>,
    }

    impl M16Data {
        /// The full-decode COUNT oracle: matched rows with a non-null value.
        fn count_oracle<T>(
            &self,
            values: &[Option<T>],
            window: Option<(i64, i64)>,
            matched: Option<&BooleanBuffer>,
        ) -> u64 {
            values
                .iter()
                .enumerate()
                .filter(|(i, v)| {
                    v.is_some()
                        && window.is_none_or(|(s, e)| self.ts[*i] >= s && self.ts[*i] < e)
                        && matched.is_none_or(|b| b.value(*i))
                })
                .count() as u64
        }

        /// The full-decode i64 MIN/MAX oracle.
        fn min_max_i64_oracle(
            &self,
            values: &[Option<i64>],
            is_max: bool,
            window: Option<(i64, i64)>,
            matched: Option<&BooleanBuffer>,
        ) -> Option<MinMaxValue> {
            let it = values.iter().enumerate().filter_map(|(i, v)| {
                let v = (*v)?;
                (window.is_none_or(|(s, e)| self.ts[i] >= s && self.ts[i] < e)
                    && matched.is_none_or(|b| b.value(i)))
                .then_some(v)
            });
            (if is_max { it.max() } else { it.min() }).map(MinMaxValue::I64)
        }

        /// The full-decode f64 MIN/MAX oracle (NaN-free fixture).
        fn min_max_f64_oracle(
            &self,
            is_max: bool,
            window: Option<(i64, i64)>,
            matched: Option<&BooleanBuffer>,
        ) -> Option<MinMaxValue> {
            let mut best: Option<f64> = None;
            for (i, v) in self.ratio.iter().enumerate() {
                let Some(v) = *v else { continue };
                if !(window.is_none_or(|(s, e)| self.ts[i] >= s && self.ts[i] < e)
                    && matched.is_none_or(|b| b.value(i)))
                {
                    continue;
                }
                best = Some(match best {
                    Some(b) => {
                        if is_max {
                            b.max(v)
                        } else {
                            b.min(v)
                        }
                    }
                    None => v,
                });
            }
            best.map(MinMaxValue::F64)
        }
    }

    /// bitmap matching every third row (a representative condition)
    fn every_third(len: usize) -> BooleanBuffer {
        use arrow::array::BooleanBufferBuilder;
        let mut builder = BooleanBufferBuilder::new(len);
        builder.append_n(len, false);
        for i in (0..len).step_by(3) {
            builder.set_bit(i, true);
        }
        builder.finish()
    }

    /// M16 pin: count_field == full-decode oracle across every arm — the
    /// presence-count answer, the straddling stats fold, sparse (no chunk
    /// rows), the all-null column, the conditioned bitmap path, and the
    /// no-stats (M1-era) file.
    #[test]
    fn m16_count_field_matches_full_decode() {
        for with_stats in [true, false] {
            let (reader, data) = m16_fixture(with_stats);
            let bitmap = every_third(M16_ROWS);
            // ts desc from 100_000: a window straddling chunks 1 and 2
            // (cuts mid-chunk on both edges) and one fully-outside window
            let straddle = Some((100_000 - 170, 100_000 - 90 + 1));
            let outside = Some((1, 2));
            for (field, values) in [
                ("code", &data.code),
                ("sparse", &data.sparse),
                ("void", &data.void),
                ("gate", &data.gate),
            ] {
                // fully covered (no window)
                assert_eq!(
                    count_field(&reader, field, None, None).unwrap(),
                    data.count_oracle(values, None, None),
                    "{field} full, with_stats={with_stats}"
                );
                // straddling window: covered chunks fold, boundaries decode
                assert_eq!(
                    count_field(&reader, field, straddle, None).unwrap(),
                    data.count_oracle(values, straddle, None),
                    "{field} straddle, with_stats={with_stats}"
                );
                // fully outside window
                assert_eq!(count_field(&reader, field, outside, None).unwrap(), 0);
                // conditioned bitmap
                assert_eq!(
                    count_field(&reader, field, None, Some(&bitmap)).unwrap(),
                    data.count_oracle(values, None, Some(&bitmap)),
                    "{field} bitmap, with_stats={with_stats}"
                );
            }
            // f64 column too
            assert_eq!(
                count_field(&reader, "ratio", straddle, None).unwrap(),
                data.count_oracle(&data.ratio, straddle, None)
            );
        }
    }

    /// M16 pin: min_max_field == full-decode oracle — numeric i64/f64 min
    /// and max, `_timestamp` (zone-served), sparse and all-null columns,
    /// straddling windows, conditioned bitmaps, and the no-stats file;
    /// string columns are refused by the family gate.
    #[test]
    fn m16_min_max_matches_full_decode() {
        for with_stats in [true, false] {
            let (reader, data) = m16_fixture(with_stats);
            let bitmap = every_third(M16_ROWS);
            let straddle = Some((100_000 - 170, 100_000 - 90 + 1));
            for is_max in [false, true] {
                for (field, values) in [
                    ("code", &data.code),
                    ("sparse", &data.sparse),
                    ("void", &data.void),
                ] {
                    let family = min_max_family(&reader, field).unwrap().unwrap();
                    assert_eq!(family, MinMaxFamily::I64);
                    for window in [None, straddle] {
                        assert_eq!(
                            min_max_field(&reader, field, family, is_max, window, None).unwrap(),
                            data.min_max_i64_oracle(values, is_max, window, None),
                            "{field} is_max={is_max} window={window:?} stats={with_stats}"
                        );
                    }
                    assert_eq!(
                        min_max_field(&reader, field, family, is_max, None, Some(&bitmap)).unwrap(),
                        data.min_max_i64_oracle(values, is_max, None, Some(&bitmap)),
                        "{field} bitmap is_max={is_max} stats={with_stats}"
                    );
                }
                // f64 column
                let family = min_max_family(&reader, "ratio").unwrap().unwrap();
                assert_eq!(family, MinMaxFamily::F64);
                for window in [None, straddle] {
                    assert_eq!(
                        min_max_field(&reader, "ratio", family, is_max, window, None).unwrap(),
                        data.min_max_f64_oracle(is_max, window, None),
                        "ratio is_max={is_max} window={window:?} stats={with_stats}"
                    );
                }
                // _timestamp: the zone table IS its stats
                let family = min_max_family(&reader, "_timestamp").unwrap().unwrap();
                let ts_values: Vec<Option<i64>> = data.ts.iter().map(|t| Some(*t)).collect();
                for window in [None, straddle] {
                    assert_eq!(
                        min_max_field(&reader, "_timestamp", family, is_max, window, None).unwrap(),
                        data.min_max_i64_oracle(&ts_values, is_max, window, None),
                        "_timestamp is_max={is_max} window={window:?}"
                    );
                }
            }
            // strings never answer: the family gate refuses
            assert_eq!(min_max_family(&reader, "name").unwrap(), None);
            // absent column: family gate refuses too (callers decide the
            // columns-complete zero shortcut)
            assert_eq!(min_max_family(&reader, "nope").unwrap(), None);
        }
    }

    /// M16 §4 pin: stats_eq_bitmap == per-row compare oracle over the
    /// verdict shapes (all-match without decode, no-match, inconclusive,
    /// null-bearing all-equal), multi-value IN, and the refusal gates
    /// (string column, cross-family kind, unparseable literal, no zone).
    #[test]
    fn m16_stats_eq_bitmap_matches_per_row_compare() {
        use crate::index::NumericKind;

        for with_stats in [true, false] {
            let (reader, data) = m16_fixture(with_stats);
            let oracle = |values: &[Option<i64>], probes: &[i64]| -> Vec<usize> {
                values
                    .iter()
                    .enumerate()
                    .filter(|(_, v)| v.is_some_and(|v| probes.contains(&v)))
                    .map(|(i, _)| i)
                    .collect()
            };
            let run = |field: &str, texts: &[&str], kind: NumericKind| {
                stats_eq_bitmap(
                    &reader,
                    field,
                    &texts.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                    kind,
                )
                .unwrap()
            };
            for (texts, probes) in [
                (vec!["7"], vec![7i64]), // all-match chunk 0, mixed 1, none 2, nulls 3
                (vec!["9"], vec![9i64]), // only chunk 2
                (vec!["8"], vec![8i64]), // only the mixed chunk
                (vec!["12345"], vec![12345i64]), // matches nothing
                (vec!["7", "9"], vec![7i64, 9]), // IN list
            ] {
                let bitmap = run("gate", &texts, NumericKind::Int)
                    .unwrap_or_else(|| panic!("gate arm must engage for {texts:?}"));
                assert_eq!(bitmap.len(), M16_ROWS);
                let got: Vec<usize> = bitmap.set_indices().collect();
                assert_eq!(
                    got,
                    oracle(&data.gate, &probes),
                    "probes={texts:?} stats={with_stats}"
                );
            }
            // sparse column (no stats rows): still exact, all chunks decode
            let bitmap = run("sparse", &["120"], NumericKind::Int).unwrap();
            assert_eq!(
                bitmap.set_indices().collect::<Vec<_>>(),
                oracle(&data.sparse, &[120])
            );
            // refusals: string column, cross-family kind, unparseable text
            assert!(run("name", &["n-1"], NumericKind::Int).is_none());
            assert!(run("gate", &["7"], NumericKind::Float).is_none());
            assert!(run("ratio", &["1"], NumericKind::Int).is_none());
            assert!(run("gate", &["not-a-number"], NumericKind::Int).is_none());
            assert!(run("gate", &["7"], NumericKind::Bool).is_none());
            // float family with Float kind engages
            let probe_ratio = data.ratio[10].unwrap(); // 10*0.5-30 = -25.0
            let bitmap = run("ratio", &[&probe_ratio.to_string()], NumericKind::Float).unwrap();
            let want: Vec<usize> = data
                .ratio
                .iter()
                .enumerate()
                .filter(|(_, v)| **v == Some(probe_ratio))
                .map(|(i, _)| i)
                .collect();
            assert_eq!(bitmap.set_indices().collect::<Vec<_>>(), want);
        }
    }
}
