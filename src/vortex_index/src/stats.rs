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

//! O2-owned per-column chunk statistics of the `docs` blob (H2, DESIGN §4).
//!
//! Pruning metadata must SURVIVE the docs-chunk passthrough — vortex's own
//! statistics cannot (computing them canonicalizes the chunks being copied),
//! so the DATA object carries its own compact per-column table in a `stats`
//! blob, designed to be CONCATENATION-SPLICEABLE: merging files =
//! concatenating their per-chunk stats rows per column (union over column
//! sets; a column an input lacks contributes zero-presence entries), no
//! recompute, no decode.
//!
//! Shape: for each docs column (the reserved `_timestamp`/`_source`/
//! `_original` excluded — the zone table is `_timestamp`'s stats), one row
//! per zone-table entry, 1:1 aligned (entry `i` bounds exactly the rows of
//! zone entry `i`, so readers reuse the zone table's row-offset prefix sum).
//! A row is one of
//!
//! - UNKNOWN (`null`): spliced from an input that carried no stats for this column (fail-open: the
//!   chunk cannot be pruned on this column),
//! - `[present]`: the non-null value count only (zero-presence runs, and types without an ordered
//!   min/max encoding), or
//! - `[present, min, max]`: full (either bound may be `null`; string bounds are truncated to a
//!   bounded prefix — the min is a plain prefix, always `<=` the true minimum, and the max is
//!   prefix-INCREMENTED so it stays `>=` the true maximum).
//!
//! Pay-as-you-go (H2): a column's chunk rows are emitted only when its
//! presence DENSITY clears a threshold, and the whole blob is bounded by a
//! byte cap (densest columns win) — a 1,500-column sparse schema costs a
//! presence entry per column, not 1,500 stats tables. Every column always
//! gets a file-level presence count in the `columns` property regardless.

use std::collections::BTreeMap;

use arrow::{
    array::{Array, ArrayRef as ArrowArrayRef},
    datatypes::DataType,
    record_batch::RecordBatch,
};

use crate::error::{Result, VixError};

/// Default [`presence-density`] threshold below which a column gets NO
/// per-chunk stats rows (file-level presence only). Overridden by
/// `ZO_VIX_STATS_MIN_DENSITY` through the writer options.
pub const DEFAULT_STATS_MIN_DENSITY: f64 = 0.1;
/// Default byte cap of the serialized `stats` blob (densest columns are
/// kept first). Overridden by `ZO_VIX_STATS_MAX_BYTES` through the writer
/// options.
pub const DEFAULT_STATS_MAX_BYTES: usize = 1024 * 1024;
/// String min/max bounds are truncated to at most this many BYTES (on a
/// char boundary). Small on purpose: bounds prune, they never answer.
pub const STATS_STR_PREFIX_BYTES: usize = 32;
/// The in-flight accumulation guard: when the folder's estimated resident
/// stats bytes exceed `max_bytes ×` this factor, the sparsest tracked
/// column is evicted (its file-level presence count survives). Bounds
/// writer memory on pathological wide-dense corpora (H2/H3).
const INFLIGHT_BYTES_FACTOR: usize = 4;

/// One column's min/max value in the stats table.
#[derive(Debug, Clone, PartialEq)]
pub enum StatValue {
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
    Str(String),
}

impl StatValue {
    fn to_json(&self) -> serde_json::Value {
        match self {
            StatValue::I64(v) => serde_json::Value::from(*v),
            StatValue::U64(v) => serde_json::Value::from(*v),
            StatValue::F64(v) => serde_json::Value::from(*v),
            StatValue::Bool(v) => serde_json::Value::from(*v),
            StatValue::Str(v) => serde_json::Value::from(v.as_str()),
        }
    }

    fn from_json(tag: &str, value: &serde_json::Value) -> Option<StatValue> {
        if value.is_null() {
            return None;
        }
        match tag {
            TAG_I64 => value.as_i64().map(StatValue::I64),
            TAG_U64 => value.as_u64().map(StatValue::U64),
            TAG_F64 => value.as_f64().map(StatValue::F64),
            TAG_BOOL => value.as_bool().map(StatValue::Bool),
            TAG_STR => value.as_str().map(|s| StatValue::Str(s.to_string())),
            _ => None,
        }
    }
}

/// One chunk's stats row for one column (aligned with the zone table).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChunkStat {
    /// Non-null values in the chunk (arrow validity; NaN floats count as
    /// present here but never bound min/max).
    pub present: u64,
    pub min: Option<StatValue>,
    pub max: Option<StatValue>,
}

/// One column's stats: the value-encoding tag plus one row per zone entry
/// (`None` = unknown, spliced from a stats-less input).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChunkStats {
    /// Value-encoding tag: `"i64"`, `"u64"`, `"f64"`, `"bool"`, `"str"`, or
    /// `"none"` (presence-only types).
    pub tag: String,
    pub chunks: Vec<Option<ColumnChunkStat>>,
}

/// The parsed `stats` blob of one DATA object.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FileColumnStats {
    pub columns: BTreeMap<String, ColumnChunkStats>,
}

/// Everything a passthrough merge splices from one input, fetched by the
/// reader accessors: the file-level per-column presence counts (from the
/// `columns` property; `None` = unknown, pre-stats file) and the per-chunk
/// stats table (from the `stats` blob).
#[derive(Debug, Clone, Default)]
pub struct SpliceableStats {
    pub presence: Vec<(String, Option<u64>)>,
    pub chunks: FileColumnStats,
}

pub(crate) const TAG_I64: &str = "i64";
pub(crate) const TAG_U64: &str = "u64";
pub(crate) const TAG_F64: &str = "f64";
pub(crate) const TAG_BOOL: &str = "bool";
pub(crate) const TAG_STR: &str = "str";
pub(crate) const TAG_NONE: &str = "none";

/// The stats tag of an arrow docs-column type.
pub(crate) fn stats_tag(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => TAG_I64,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => TAG_U64,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => TAG_F64,
        DataType::Boolean => TAG_BOOL,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => TAG_STR,
        _ => TAG_NONE,
    }
}

/// Serialize the blob: `{"v":1,"cols":{name:{"t":tag,"c":[row,...]}}}` with
/// rows encoded `null` | `[present]` | `[present, min, max]`.
pub(crate) fn encode_stats_blob(stats: &FileColumnStats) -> Result<Vec<u8>> {
    let mut cols = serde_json::Map::with_capacity(stats.columns.len());
    for (name, column) in &stats.columns {
        cols.insert(name.clone(), encode_column(column));
    }
    let root = serde_json::json!({ "v": 1, "cols": cols });
    serde_json::to_vec(&root).map_err(|e| VixError::Writer(format!("encode stats blob: {e}")))
}

fn encode_column(column: &ColumnChunkStats) -> serde_json::Value {
    let rows: Vec<serde_json::Value> = column
        .chunks
        .iter()
        .map(|row| match row {
            None => serde_json::Value::Null,
            Some(stat) => {
                if stat.min.is_none() && stat.max.is_none() {
                    serde_json::json!([stat.present])
                } else {
                    serde_json::json!([
                        stat.present,
                        stat.min.as_ref().map(StatValue::to_json),
                        stat.max.as_ref().map(StatValue::to_json),
                    ])
                }
            }
        })
        .collect();
    serde_json::json!({ "t": column.tag, "c": rows })
}

/// Estimated serialized bytes of one column's table (the byte-cap unit).
fn encoded_column_size(name: &str, column: &ColumnChunkStats) -> usize {
    // cheap but honest: serialize the column fragment itself
    name.len() + 16 + serde_json::to_string(&encode_column(column)).map_or(0, |s| s.len())
}

/// Parse a `stats` blob. Unknown top-level keys and column tags are
/// tolerated (their rows read as unknown).
pub(crate) fn decode_stats_blob(bytes: &[u8]) -> Result<FileColumnStats> {
    let root: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| VixError::Malformed(format!("stats blob: {e}")))?;
    let mut columns = BTreeMap::new();
    let Some(cols) = root.get("cols").and_then(|v| v.as_object()) else {
        return Ok(FileColumnStats { columns });
    };
    for (name, value) in cols {
        let tag = value
            .get("t")
            .and_then(|t| t.as_str())
            .unwrap_or(TAG_NONE)
            .to_string();
        let rows = value
            .get("c")
            .and_then(|c| c.as_array())
            .cloned()
            .unwrap_or_default();
        let chunks: Vec<Option<ColumnChunkStat>> = rows
            .iter()
            .map(|row| {
                let parts = row.as_array()?;
                let present = parts.first().and_then(|p| p.as_u64())?;
                let min = parts.get(1).and_then(|v| StatValue::from_json(&tag, v));
                let max = parts.get(2).and_then(|v| StatValue::from_json(&tag, v));
                Some(ColumnChunkStat { present, min, max })
            })
            .collect();
        columns.insert(name.clone(), ColumnChunkStats { tag, chunks });
    }
    Ok(FileColumnStats { columns })
}

// ---------------------------------------------------------------------------
// String bound truncation
// ---------------------------------------------------------------------------

/// A LOWER bound of `value` within the prefix budget: the longest char-
/// boundary prefix. A prefix is always `<=` the full string.
fn str_lower_bound(value: &str) -> String {
    if value.len() <= STATS_STR_PREFIX_BYTES {
        return value.to_string();
    }
    let mut end = STATS_STR_PREFIX_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

/// An UPPER bound of `value` within the prefix budget: the value itself
/// when it fits, else its prefix with the last char incremented to the next
/// scalar (popping chars that cannot increment). `None` when no valid upper
/// bound exists within the budget (degenerate; the max is then unknown).
fn str_upper_bound(value: &str) -> Option<String> {
    if value.len() <= STATS_STR_PREFIX_BYTES {
        return Some(value.to_string());
    }
    let mut prefix = str_lower_bound(value);
    while let Some(last) = prefix.chars().next_back() {
        let last_len = last.len_utf8();
        // next scalar after `last` (skipping the surrogate gap)
        let mut code = last as u32 + 1;
        if (0xD800..=0xDFFF).contains(&code) {
            code = 0xE000;
        }
        if let Some(next) = char::from_u32(code) {
            prefix.truncate(prefix.len() - last_len);
            prefix.push(next);
            return Some(prefix);
        }
        // last char was char::MAX: drop it and increment the previous one
        prefix.truncate(prefix.len() - last_len);
    }
    None
}

// ---------------------------------------------------------------------------
// Fold-side accumulation
// ---------------------------------------------------------------------------

/// One column's open-window accumulator.
#[derive(Debug, Default)]
struct WindowAcc {
    present: u64,
    min_i: Option<i64>,
    max_i: Option<i64>,
    min_u: Option<u64>,
    max_u: Option<u64>,
    min_f: Option<f64>,
    max_f: Option<f64>,
    min_b: Option<bool>,
    max_b: Option<bool>,
    min_s: Option<String>,
    /// `Some(None)` = "an upper bound exists but could not be encoded"
    /// (degenerate increment): the window's max stays unknown.
    max_s: Option<Option<String>>,
}

impl WindowAcc {
    fn close(&mut self, tag: &str) -> ColumnChunkStat {
        let (min, max) = match tag {
            TAG_I64 => (
                self.min_i.take().map(StatValue::I64),
                self.max_i.take().map(StatValue::I64),
            ),
            TAG_U64 => (
                self.min_u.take().map(StatValue::U64),
                self.max_u.take().map(StatValue::U64),
            ),
            TAG_F64 => (
                self.min_f.take().map(StatValue::F64),
                self.max_f.take().map(StatValue::F64),
            ),
            TAG_BOOL => (
                self.min_b.take().map(StatValue::Bool),
                self.max_b.take().map(StatValue::Bool),
            ),
            TAG_STR => (
                self.min_s.take().map(StatValue::Str),
                self.max_s.take().flatten().map(StatValue::Str),
            ),
            _ => (None, None),
        };
        let present = self.present;
        self.present = 0;
        ColumnChunkStat { present, min, max }
    }
}

/// One tracked docs column of the folder.
struct TrackedColumn {
    name: String,
    tag: &'static str,
    /// Index of the column in the docs schema (fold addressing).
    field_index: usize,
    window: WindowAcc,
    chunks: Vec<Option<ColumnChunkStat>>,
    /// Running file-level presence count. Survives chunk-row eviction;
    /// `None` once poisoned by a splice from a presence-less input.
    present_total: Option<u64>,
    /// Rows covered by KNOWN (non-`None`) chunk entries and their presence
    /// sum — the density basis at finish.
    known_rows: u64,
    known_present: u64,
    /// The in-flight guard evicted this column's chunk rows: presence keeps
    /// counting, chunk stats are gone for this file.
    evicted: bool,
    /// Rough resident-bytes estimate of `chunks` (the eviction unit).
    resident_bytes: usize,
}

/// Streaming builder of the per-column chunk stats table, windowed EXACTLY
/// like the `_timestamp` zone table (the caller drives both from the same
/// row windows, so entry `i` of every column covers zone entry `i`'s rows).
pub(crate) struct ColumnStatsFolder {
    columns: Vec<TrackedColumn>,
    min_density: f64,
    max_bytes: usize,
    total_resident: usize,
}

impl ColumnStatsFolder {
    /// Track every non-reserved docs column of `docs_schema`.
    /// `reserved` names are skipped (`_timestamp`/`_source`/`_original`).
    pub(crate) fn new(
        docs_schema: &arrow::datatypes::Schema,
        reserved: &[&str],
        min_density: f64,
        max_bytes: usize,
    ) -> Self {
        let min_density = if min_density <= 0.0 {
            DEFAULT_STATS_MIN_DENSITY
        } else {
            min_density
        };
        let max_bytes = if max_bytes == 0 {
            DEFAULT_STATS_MAX_BYTES
        } else {
            max_bytes
        };
        let columns = docs_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !reserved.contains(&field.name().as_str()))
            .map(|(field_index, field)| TrackedColumn {
                name: field.name().clone(),
                tag: stats_tag(field.data_type()),
                field_index,
                window: WindowAcc::default(),
                chunks: Vec::new(),
                present_total: Some(0),
                known_rows: 0,
                known_present: 0,
                evicted: false,
                resident_bytes: 0,
            })
            .collect();
        Self {
            columns,
            min_density,
            max_bytes,
            total_resident: 0,
        }
    }

    /// Fold one row window `[offset, offset+len)` of a docs batch into the
    /// open window accumulators. The caller closes windows via
    /// [`Self::close_window`] at exactly the zone folder's boundaries.
    pub(crate) fn fold_window(&mut self, batch: &RecordBatch, offset: usize, len: usize) {
        for column in &mut self.columns {
            let array = batch.column(column.field_index);
            let slice = array.slice(offset, len);
            fold_array(&mut column.window, column.tag, &slice);
            if let Some(total) = column.present_total.as_mut() {
                *total += (slice.len() - slice.null_count()) as u64;
            }
        }
    }

    /// Close the open window across every column: one chunk row each,
    /// covering `rows` rows (the matching zone entry's row count).
    pub(crate) fn close_window(&mut self, rows: u64) {
        let mut added = 0usize;
        for column in &mut self.columns {
            let stat = column.window.close(column.tag);
            column.known_rows += rows;
            column.known_present += stat.present;
            if column.evicted {
                continue;
            }
            let cost = 12
                + match (&stat.min, &stat.max) {
                    (Some(StatValue::Str(a)), Some(StatValue::Str(b))) => a.len() + b.len() + 8,
                    (Some(_), Some(_)) => 24,
                    _ => 4,
                };
            column.resident_bytes += cost;
            added += cost;
            column.chunks.push(Some(stat));
        }
        self.total_resident += added;
        self.enforce_inflight_guard();
    }

    /// Splice one passthrough input's stats (aligned with the
    /// `zone_entries` the caller just appended to the zone table): a
    /// tracked column takes the input's rows verbatim when the input
    /// carries an ALIGNED, TAG-MATCHING table for it; a column ABSENT from
    /// the input's docs schema contributes zero-presence rows; anything
    /// else (stats-less column of a stats-bearing file: below the input's
    /// density threshold or byte cap) contributes UNKNOWN rows, and its
    /// file presence stays countable only through the input's presence
    /// list. The caller has validated blob presence and alignment
    /// (`validate_spliceable`).
    pub(crate) fn append_spliced(
        &mut self,
        entry_count: usize,
        entry_rows: u64,
        spliced: &SpliceableStats,
    ) {
        let mut added = 0usize;
        for column in &mut self.columns {
            // file-level presence: the input's count, or poison to unknown
            let input_presence = spliced
                .presence
                .iter()
                .find(|(name, _)| name == &column.name);
            match input_presence {
                // absent from the input's docs schema: zero rows there
                None => {}
                Some((_, Some(count))) => {
                    if let Some(total) = column.present_total.as_mut() {
                        *total += count;
                    }
                }
                Some((_, None)) => column.present_total = None,
            }

            let table = spliced.chunks.columns.get(&column.name);
            match table {
                Some(table) if table.tag == column.tag && table.chunks.len() == entry_count => {
                    for row in &table.chunks {
                        if let Some(stat) = row {
                            column.known_present += stat.present;
                        }
                        if !column.evicted {
                            let cost = 12
                                + match row {
                                    Some(ColumnChunkStat {
                                        min: Some(StatValue::Str(a)),
                                        max: Some(StatValue::Str(b)),
                                        ..
                                    }) => a.len() + b.len() + 8,
                                    Some(_) => 24,
                                    None => 2,
                                };
                            column.resident_bytes += cost;
                            added += cost;
                            column.chunks.push(row.clone());
                        }
                    }
                    // known-row accounting: count rows of the KNOWN entries
                    // only (unknown rows stay out of the density basis)
                    let known: usize = table.chunks.iter().filter(|row| row.is_some()).count();
                    if known == entry_count {
                        column.known_rows += entry_rows;
                    } else {
                        // approximate: unknown entries' rows are not
                        // separable without the zone table — attribute
                        // proportionally (density is a heuristic gate)
                        column.known_rows += entry_rows * known as u64 / entry_count.max(1) as u64;
                    }
                }
                Some(_) | None if input_presence.is_none() => {
                    // column absent from the input: its rows hold NO values
                    for _ in 0..entry_count {
                        if !column.evicted {
                            column.resident_bytes += 12;
                            added += 12;
                            column.chunks.push(Some(ColumnChunkStat {
                                present: 0,
                                min: None,
                                max: None,
                            }));
                        }
                    }
                    column.known_rows += entry_rows;
                }
                _ => {
                    // the input has the column but no (usable) chunk table:
                    // unknown rows — fail-open for pruning
                    for _ in 0..entry_count {
                        if !column.evicted {
                            column.resident_bytes += 2;
                            added += 2;
                            column.chunks.push(None);
                        }
                    }
                }
            }
        }
        self.total_resident += added;
        self.enforce_inflight_guard();
    }

    /// Evict the sparsest tracked column's chunk rows while the resident
    /// estimate exceeds the guard (presence counting survives).
    fn enforce_inflight_guard(&mut self) {
        let guard = self.max_bytes.saturating_mul(INFLIGHT_BYTES_FACTOR);
        while self.total_resident > guard {
            let Some(victim) = self
                .columns
                .iter_mut()
                .filter(|column| !column.evicted && !column.chunks.is_empty())
                .min_by_key(|column| column.known_present)
            else {
                return;
            };
            victim.evicted = true;
            self.total_resident = self.total_resident.saturating_sub(victim.resident_bytes);
            victim.resident_bytes = 0;
            victim.chunks = Vec::new();
        }
    }

    /// Close out: per-column file presence (for the `columns` property) and
    /// the serialized `stats` blob (`None` when no column qualified). The
    /// density threshold and the byte cap apply here; a column with any
    /// UNKNOWN rows is density-gated on its KNOWN rows only.
    pub(crate) fn finish(mut self) -> (Vec<(String, Option<u64>)>, Option<Vec<u8>>) {
        let presence: Vec<(String, Option<u64>)> = self
            .columns
            .iter()
            .map(|column| (column.name.clone(), column.present_total))
            .collect();

        // density gate
        let mut kept: Vec<(usize, f64)> = Vec::new();
        for (index, column) in self.columns.iter().enumerate() {
            if column.evicted || column.chunks.is_empty() {
                continue;
            }
            if column.chunks.iter().all(|row| row.is_none()) {
                continue; // nothing known: pure dead weight
            }
            if column.known_rows == 0 {
                continue;
            }
            let density = column.known_present as f64 / column.known_rows as f64;
            if density >= self.min_density {
                kept.push((index, density));
            }
        }
        // byte cap: densest first, deterministic ties by name
        kept.sort_by(|(ia, da), (ib, db)| {
            db.partial_cmp(da)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| self.columns[*ia].name.cmp(&self.columns[*ib].name))
        });
        let mut stats = FileColumnStats::default();
        let mut budget = self.max_bytes;
        for (index, _) in kept {
            let column = &mut self.columns[index];
            let table = ColumnChunkStats {
                tag: column.tag.to_string(),
                chunks: std::mem::take(&mut column.chunks),
            };
            let cost = encoded_column_size(&column.name, &table);
            if cost > budget {
                continue;
            }
            budget -= cost;
            stats.columns.insert(column.name.clone(), table);
        }
        // The blob is emitted even with ZERO qualifying columns: its
        // presence marks a stats-era file (spliceable — unknown-free
        // presence counts plus whatever tables fit), so all-sparse files
        // keep qualifying for the passthrough instead of decoding forever.
        (presence, encode_stats_blob(&stats).ok())
    }
}

/// Fold one arrow slice into a window accumulator.
fn fold_array(window: &mut WindowAcc, tag: &str, array: &ArrowArrayRef) {
    use arrow::array::{
        BooleanArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, LargeStringArray, StringArray, StringViewArray, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    };
    window.present += (array.len() - array.null_count()) as u64;
    match tag {
        TAG_I64 => {
            let mut fold = |value: i64| {
                window.min_i = Some(window.min_i.map_or(value, |m| m.min(value)));
                window.max_i = Some(window.max_i.map_or(value, |m| m.max(value)));
            };
            match array.data_type() {
                DataType::Int8 => {
                    let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
                    a.iter().flatten().for_each(|v| fold(v as i64));
                }
                DataType::Int16 => {
                    let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
                    a.iter().flatten().for_each(|v| fold(v as i64));
                }
                DataType::Int32 => {
                    let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
                    a.iter().flatten().for_each(|v| fold(v as i64));
                }
                DataType::Int64 => {
                    let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    a.iter().flatten().for_each(&mut fold);
                }
                _ => {}
            }
        }
        TAG_U64 => {
            let mut fold = |value: u64| {
                window.min_u = Some(window.min_u.map_or(value, |m| m.min(value)));
                window.max_u = Some(window.max_u.map_or(value, |m| m.max(value)));
            };
            match array.data_type() {
                DataType::UInt8 => {
                    let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                    a.iter().flatten().for_each(|v| fold(v as u64));
                }
                DataType::UInt16 => {
                    let a = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                    a.iter().flatten().for_each(|v| fold(v as u64));
                }
                DataType::UInt32 => {
                    let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                    a.iter().flatten().for_each(|v| fold(v as u64));
                }
                DataType::UInt64 => {
                    let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                    a.iter().flatten().for_each(&mut fold);
                }
                _ => {}
            }
        }
        TAG_F64 => {
            let mut fold = |value: f64| {
                if value.is_nan() {
                    return; // present, but never a bound
                }
                window.min_f = Some(window.min_f.map_or(value, |m| m.min(value)));
                window.max_f = Some(window.max_f.map_or(value, |m| m.max(value)));
            };
            match array.data_type() {
                DataType::Float16 => {
                    let a = array.as_any().downcast_ref::<Float16Array>().unwrap();
                    a.iter().flatten().for_each(|v| fold(v.to_f64()));
                }
                DataType::Float32 => {
                    let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
                    a.iter().flatten().for_each(|v| fold(v as f64));
                }
                DataType::Float64 => {
                    let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    a.iter().flatten().for_each(&mut fold);
                }
                _ => {}
            }
        }
        TAG_BOOL => {
            if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
                for value in a.iter().flatten() {
                    window.min_b = Some(window.min_b.map_or(value, |m| m & value));
                    window.max_b = Some(window.max_b.map_or(value, |m| m | value));
                }
            }
        }
        TAG_STR => {
            let mut fold = |value: &str| {
                let lower = str_lower_bound(value);
                match &mut window.min_s {
                    Some(current) => {
                        if lower.as_str() < current.as_str() {
                            *current = lower;
                        }
                    }
                    None => window.min_s = Some(lower),
                }
                let upper = str_upper_bound(value);
                match (&mut window.max_s, upper) {
                    (None, upper) => window.max_s = Some(upper),
                    (Some(None), _) => {} // max already unknowable
                    (Some(current @ Some(_)), Some(upper)) => {
                        if upper.as_str() > current.as_deref().unwrap() {
                            *current = Some(upper);
                        }
                    }
                    (Some(current @ Some(_)), None) => *current = None,
                }
            };
            match array.data_type() {
                DataType::Utf8 => {
                    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                    a.iter().flatten().for_each(&mut fold);
                }
                DataType::LargeUtf8 => {
                    let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    a.iter().flatten().for_each(&mut fold);
                }
                DataType::Utf8View => {
                    let a = array.as_any().downcast_ref::<StringViewArray>().unwrap();
                    a.iter().flatten().for_each(&mut fold);
                }
                _ => {}
            }
        }
        _ => {}
    }
}

/// Validate one input's spliceable stats against its zone-table entry
/// count: the blob must exist, every column table must be tag-consistent
/// and 1:1 aligned with the zone entries, and every presence count must be
/// known. `Err(reason)` disqualifies the input from the passthrough (it
/// decodes; fresh stats are computed) — this is what makes the v1
/// stats-loss regression structurally impossible: a chunk copy CANNOT
/// produce a stats-less output.
pub fn validate_spliceable(
    stats: &SpliceableStats,
    zone_entries: usize,
) -> std::result::Result<(), String> {
    for (name, count) in &stats.presence {
        if count.is_none() {
            return Err(format!(
                "column {name:?} has no file-level presence count (pre-stats file)"
            ));
        }
    }
    for (name, table) in &stats.chunks.columns {
        if table.chunks.len() != zone_entries {
            return Err(format!(
                "column {name:?} stats cover {} chunks but the zone table has {zone_entries}",
                table.chunks.len()
            ));
        }
    }
    Ok(())
}

/// Parse the `columns` property: a JSON array whose entries are either a
/// plain name (M1 files — presence unknown) or a `[name, present_rows]`
/// pair (stats-era files).
pub(crate) fn parse_columns_prop(raw: &str) -> Result<Vec<(String, Option<u64>)>> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| VixError::Malformed(format!("columns property: {e}")))?;
    let Some(entries) = value.as_array() else {
        return Err(VixError::Malformed(
            "columns property is not an array".to_string(),
        ));
    };
    entries
        .iter()
        .map(|entry| {
            if let Some(name) = entry.as_str() {
                return Ok((name.to_string(), None));
            }
            if let Some(pair) = entry.as_array()
                && let Some(name) = pair.first().and_then(|n| n.as_str())
            {
                return Ok((name.to_string(), pair.get(1).and_then(|c| c.as_u64())));
            }
            Err(VixError::Malformed(format!(
                "columns property entry is neither a name nor a [name, count] pair: {entry}"
            )))
        })
        .collect()
}

/// Serialize the `columns` property: `[name, count]` pairs when the count
/// is known, plain names otherwise.
pub(crate) fn encode_columns_prop(entries: &[(String, Option<u64>)]) -> Result<String> {
    let values: Vec<serde_json::Value> = entries
        .iter()
        .map(|(name, count)| match count {
            Some(count) => serde_json::json!([name, count]),
            None => serde_json::Value::from(name.as_str()),
        })
        .collect();
    serde_json::to_string(&values).map_err(|e| VixError::Writer(format!("columns property: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_bounds_are_valid_and_bounded() {
        // short strings pass through
        assert_eq!(str_lower_bound("abc"), "abc");
        assert_eq!(str_upper_bound("abc").as_deref(), Some("abc"));
        // long strings truncate; lower <= value <= upper
        let long = "z".repeat(100) + "tail";
        let lower = str_lower_bound(&long);
        let upper = str_upper_bound(&long).expect("upper bound");
        assert!(lower.len() <= STATS_STR_PREFIX_BYTES);
        assert!(upper.len() <= STATS_STR_PREFIX_BYTES + 4);
        assert!(lower.as_str() <= long.as_str());
        assert!(upper.as_str() >= long.as_str());
        // multi-byte boundary safety
        let unicode = "日本語のログメッセージが長い場合の切り詰め".repeat(3);
        let lower = str_lower_bound(&unicode);
        let upper = str_upper_bound(&unicode).expect("upper bound");
        assert!(lower.as_str() <= unicode.as_str());
        assert!(upper.as_str() >= unicode.as_str());
    }

    #[test]
    fn blob_roundtrip() {
        let mut stats = FileColumnStats::default();
        stats.columns.insert(
            "svc".to_string(),
            ColumnChunkStats {
                tag: TAG_STR.to_string(),
                chunks: vec![
                    Some(ColumnChunkStat {
                        present: 3,
                        min: Some(StatValue::Str("api".into())),
                        max: Some(StatValue::Str("web".into())),
                    }),
                    None,
                    Some(ColumnChunkStat {
                        present: 0,
                        min: None,
                        max: None,
                    }),
                ],
            },
        );
        stats.columns.insert(
            "code".to_string(),
            ColumnChunkStats {
                tag: TAG_I64.to_string(),
                chunks: vec![Some(ColumnChunkStat {
                    present: 2,
                    min: Some(StatValue::I64(-3)),
                    max: Some(StatValue::I64(500)),
                })],
            },
        );
        let bytes = encode_stats_blob(&stats).unwrap();
        let decoded = decode_stats_blob(&bytes).unwrap();
        assert_eq!(decoded, stats);
    }

    #[test]
    fn columns_prop_roundtrip_and_m1_compat() {
        let entries = vec![
            ("_timestamp".to_string(), Some(10u64)),
            ("svc".to_string(), Some(7)),
            ("legacy".to_string(), None),
        ];
        let raw = encode_columns_prop(&entries).unwrap();
        assert_eq!(parse_columns_prop(&raw).unwrap(), entries);
        // M1 plain-list form
        let legacy = parse_columns_prop(r#"["_timestamp","svc"]"#).unwrap();
        assert_eq!(
            legacy,
            vec![("_timestamp".to_string(), None), ("svc".to_string(), None)]
        );
    }
}
