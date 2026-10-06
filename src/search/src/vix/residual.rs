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

//! Residual filtering INSIDE the index evaluation.
//!
//! An aggregate fast path needs an EXACT per-file match set. When the index
//! can only produce a superset (`has_skipped`: a multi-word `match_all`
//! whose token AND is weaker than the SQL substring predicate, an equality
//! on a token-indexed field narrowed to its tokens, a conjunct the file
//! cannot serve), the whole file used to fall to the scan branch: a second
//! index pass plus a DataFusion scan that re-opens every file's docs
//! footer (2 round trips, MBs of layout) and decodes the predicate columns'
//! chunks for a handful of rows — 30–50 s of a cold 48 h histogram, every
//! time, because a superset result is never memoised as exact.
//!
//! Here the superset is refined in place: the candidate rows' predicate
//! columns are point-read through the reader that is already open
//! ([`VixReader::read_docs_columns_rows`], one chunk read per touched
//! chunk) and the WHOLE condition is evaluated on them with the same
//! physical expression the scan branch would apply
//! ([`IndexCondition::to_physical_expr`] — identical semantics by
//! construction). The result is the exact bitmap: the aggregate collectors
//! run on it unchanged, the per-file result memoises as exact, and the scan
//! branch never sees the file.
//!
//! Bounded on purpose: a superset that is most of the file (a dense token,
//! a value present in every row) would make the refinement a column scan of
//! the file under the index phase's concurrency, so above
//! [`RESIDUAL_MAX_ROWS`] candidates or [`RESIDUAL_MAX_CHUNKS`] touched docs
//! chunks the file keeps today's fallback. Only string-typed predicate
//! columns qualify: numeric drift rows carry scan-side coercions this path
//! does not reproduce.

use arrow::{
    array::{Array, BooleanArray, BooleanBufferBuilder},
    buffer::BooleanBuffer,
};
use arrow_schema::DataType;
use datafusion::{physical_plan::ColumnarValue, scalar::ScalarValue};
use vortex_index::VixReader;

use crate::index::{Condition, IndexCondition};

/// Most candidate rows a superset may hold to be refined in the index phase.
/// The production family this serves (`match_all(phrase) AND service AND
/// body = value` histograms) leaves a few rows per file; `.200` cold runs:
/// ~4 per file over 2,300 files per follower.
pub(super) const RESIDUAL_MAX_ROWS: usize = 4096;

/// Most docs chunks the candidates may touch: each one is a column read of
/// a few MB on merged files, in the one round trip the point read costs.
pub(super) const RESIDUAL_MAX_CHUNKS: usize = 8;

/// Why a superset was not refined; the caller falls back to the scan branch
/// exactly as before and the reason reaches the fast-path fallback counter.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Refusal {
    TooManyRows(usize),
    TooManyChunks(usize),
    /// A predicate column this file does not store as a docs column, or
    /// stores with a non-string type.
    UnsupportedColumn(String),
    /// A condition shape the physical rebuild cannot reproduce here.
    UnsupportedCondition,
    /// The full-text predicate has no column to evaluate against.
    NoFullTextColumn,
}

impl Refusal {
    pub(super) fn reason(&self) -> &'static str {
        match self {
            Refusal::TooManyRows(_) => "residual: too many candidate rows",
            Refusal::TooManyChunks(_) => "residual: too many candidate chunks",
            Refusal::UnsupportedColumn(_) => "residual: unsupported column",
            Refusal::UnsupportedCondition => "residual: unsupported condition",
            Refusal::NoFullTextColumn => "residual: no full-text column",
        }
    }
}

/// Refine the superset `bitmap` of `condition` on `reader` to the exact
/// match set, or say why not. `fts_fields` is the query's full-text scope
/// (the stream's full-text fields, exactly what the scan's `match_all`
/// rebuild would read); fields the file does not store are dropped from the
/// disjunction — a column absent from the file is NULL in every row, and
/// `IS NOT NULL AND ... LIKE` is false on NULL, so the disjunct contributes
/// nothing either way.
///
/// `Ok(Ok(exact))` is a bitmap of the reader's row count; an empty superset
/// is returned as-is (exact already). Errors are read/evaluation failures.
pub(super) fn refine_superset(
    reader: &VixReader,
    condition: &IndexCondition,
    fts_fields: &[String],
    bitmap: &BooleanBuffer,
) -> anyhow::Result<Result<BooleanBuffer, Refusal>> {
    refine_superset_within(
        reader,
        condition,
        fts_fields,
        bitmap,
        RESIDUAL_MAX_ROWS,
        RESIDUAL_MAX_CHUNKS,
    )
}

/// [`refine_superset`] with explicit candidate-row and touched-chunk caps.
pub(super) fn refine_superset_within(
    reader: &VixReader,
    condition: &IndexCondition,
    fts_fields: &[String],
    bitmap: &BooleanBuffer,
    max_rows: usize,
    max_chunks: usize,
) -> anyhow::Result<Result<BooleanBuffer, Refusal>> {
    let candidates = bitmap.count_set_bits();
    if candidates == 0 {
        return Ok(Ok(bitmap.clone()));
    }
    if candidates > max_rows {
        return Ok(Err(Refusal::TooManyRows(candidates)));
    }
    let rows: Vec<u64> = bitmap.set_indices().map(|row| row as u64).collect();
    let chunks = touched_chunks(reader, &rows);
    if chunks > max_chunks {
        return Ok(Err(Refusal::TooManyChunks(chunks)));
    }
    if !condition.conditions.iter().all(residual_supported_shape) {
        return Ok(Err(Refusal::UnsupportedCondition));
    }

    // the docs-blob footer — the one cost the scan branch paid per file
    // too — through a handle the cached reader does not retain (see
    // `VixReader::detached_docs`)
    let docs = reader.detached_docs();
    let schema = docs.schema()?;
    let present_fts: Vec<String> = fts_fields
        .iter()
        .filter(|field| schema.index_of(field).is_ok())
        .cloned()
        .collect();
    if condition.uses_full_text() && present_fts.is_empty() {
        return Ok(Err(Refusal::NoFullTextColumn));
    }
    let mut names: Vec<String> = condition
        .get_schema_fields(&present_fts)
        .into_iter()
        .collect();
    names.sort_unstable();
    if names.is_empty() {
        // a condition over no column (`All`) is exact by construction
        return Ok(Ok(bitmap.clone()));
    }
    for name in &names {
        match schema.index_of(name) {
            Ok(index) if is_string_type(schema.field(index).data_type()) => {}
            _ => return Ok(Err(Refusal::UnsupportedColumn(name.clone()))),
        }
    }
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();

    let mut exact = BooleanBufferBuilder::new(bitmap.len());
    exact.append_n(bitmap.len(), false);
    for window in rows.chunks(65_536) {
        let batch = docs.read_columns_rows(&name_refs, window)?;
        let expr = match condition.to_physical_expr(batch.schema().as_ref(), &present_fts) {
            Ok(expr) => expr,
            Err(_) => return Ok(Err(Refusal::UnsupportedCondition)),
        };
        match expr.evaluate(&batch)? {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                for &row in window {
                    exact.set_bit(row as usize, true);
                }
            }
            ColumnarValue::Scalar(_) => {}
            ColumnarValue::Array(array) => {
                let mask = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        anyhow::anyhow!("residual predicate did not evaluate to booleans")
                    })?;
                if mask.len() != window.len() {
                    return Err(anyhow::anyhow!(
                        "residual predicate mask has {} rows for {} candidates",
                        mask.len(),
                        window.len()
                    ));
                }
                for (i, &row) in window.iter().enumerate() {
                    if mask.is_valid(i) && mask.value(i) {
                        exact.set_bit(row as usize, true);
                    }
                }
            }
        }
    }
    Ok(Ok(exact.finish()))
}

/// Distinct docs chunks the sorted `rows` fall in (zone map when the file
/// has one, else the fixed row-group size; 1 when neither is known).
fn touched_chunks(reader: &VixReader, rows: &[u64]) -> usize {
    if let Some(chunks) = reader.zone_chunks() {
        let mut touched = 0usize;
        let mut next = 0usize;
        for chunk in chunks {
            let end = chunk.row_offset + chunk.row_count;
            let start = next;
            while next < rows.len() && rows[next] < end {
                next += 1;
            }
            if next > start {
                touched += 1;
            }
            if next == rows.len() {
                break;
            }
        }
        if next < rows.len() {
            // rows past the zone map's coverage: one more chunk's worth
            touched += 1;
        }
        return touched;
    }
    let group = reader.row_group_size() as u64;
    if group == 0 {
        return 1;
    }
    let mut touched = 0usize;
    let mut last = None;
    for &row in rows {
        let chunk = row / group;
        if last != Some(chunk) {
            touched += 1;
            last = Some(chunk);
        }
    }
    touched
}

/// Condition shapes whose physical rebuild over the file's own docs columns
/// matches the scan branch's semantics exactly. Numeric comparisons carry
/// the scan side's type coercions for drift rows and regexes have no
/// physical form; both keep the fallback.
fn residual_supported_shape(condition: &Condition) -> bool {
    match condition {
        Condition::Equal(..)
        | Condition::NotEqual(..)
        | Condition::StrMatch(..)
        | Condition::In(..)
        | Condition::IsNull(_)
        | Condition::IsNotNull(_)
        | Condition::MatchAll(_)
        | Condition::FuzzyMatchAll(..)
        | Condition::All() => true,
        Condition::NumericCmp(..) | Condition::Regex(..) => false,
        Condition::Or(left, right) | Condition::And(left, right) => {
            residual_supported_shape(left) && residual_supported_shape(right)
        }
        Condition::Not(inner) => residual_supported_shape(inner),
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}
