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

//! Per-row `_source` handling shared by every star-query surface.
//!
//! [`synthesize_source`] turns one flattened [`RecordBatch`] into the
//! per-row `_source` strings stored in a core `.vix` file: each row becomes
//! a single-level JSON object of the batch's columns. It is used
//! - at WAL→storage persist and compaction rebuilds (the core writer),
//! - at query time for data that predates the docs blob: memtable batches (`adapt_batch` on the
//!   ingester) and parquet files (WAL parquet plus pre-migration storage parquet) via
//!   [`SynthesizeSourceExpr`] / [`SourceSynthesizingExprAdapterFactory`], so a row-store-driven
//!   `SELECT *` sees a `_source` image for EVERY row regardless of where it lives.
//!
//! Rules (mirror what a search hit looks like, so `_source` can reconstruct
//! hits faithfully):
//! - **`_timestamp` is included** — search-hit objects carry it, and keeping it makes `_source` a
//!   complete record on its own,
//! - **`_o2_id` and `_original` are excluded** — `_o2_id` is an internal dedup handle (kept as a
//!   docs column instead) and `_original` has its own opt-in docs column,
//! - null values are omitted (flatten drops nulls; absent == null),
//! - keys are the batch's column names verbatim (already dotted — flatten keeps dots),
//! - values keep their JSON-native types: numbers as numbers, bools as bools, strings as strings.
//!
//! The implementation rides on `arrow-json`'s vectorized line-delimited
//! writer (no per-row `serde_json::Value` round trips) and splits the
//! output buffer on `\n` — JSON string escaping guarantees no literal
//! newline inside a serialized row.
//!
//! [`expand_star_source_hits`] is the response-side counterpart: it explodes
//! a hit's `_source` column back into the hit's own fields (physical column
//! values win on overlap), giving star queries per-record field sets without
//! ever enumerating the stream schema.

use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, StringArray},
    record_batch::RecordBatch,
};
use arrow_json::{LineDelimitedWriter, writer::WriterBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use config::{ID_COL_NAME, ORIGINAL_DATA_COL_NAME};
use datafusion::{
    common::{
        Result as DataFusionResult,
        tree_node::{Transformed, TransformedResult, TreeNode},
    },
    error::DataFusionError,
    physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
    },
    physical_plan::{ColumnarValue, PhysicalExpr, expressions::Column},
};
use vortex_index::SOURCE_COL_NAME;

/// Columns excluded from `_source` (see the module docs).
const EXCLUDED_COLS: [&str; 3] = [ID_COL_NAME, ORIGINAL_DATA_COL_NAME, SOURCE_COL_NAME];

/// Serialize every row of `batch` into its `_source` JSON object.
pub fn synthesize_source(batch: &RecordBatch) -> Result<StringArray, anyhow::Error> {
    let num_rows = batch.num_rows();
    let included: Vec<usize> = batch
        .schema_ref()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !EXCLUDED_COLS.contains(&field.name().as_str()))
        .map(|(index, _)| index)
        .collect();
    if included.is_empty() || num_rows == 0 {
        return Ok(StringArray::from_iter_values(std::iter::repeat_n(
            "{}", num_rows,
        )));
    }
    let projected = if included.len() == batch.num_columns() {
        batch.clone()
    } else {
        batch.project(&included)?
    };

    let mut buf = Vec::with_capacity(projected.get_array_memory_size() / 2);
    let mut writer: LineDelimitedWriter<_> = WriterBuilder::new()
        .with_explicit_nulls(false) // null == absent, like flatten
        .build(&mut buf);
    writer.write(&projected)?;
    writer.finish()?;
    drop(writer);

    let mut rows = Vec::with_capacity(num_rows);
    for line in buf.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue; // the trailing newline
        }
        rows.push(std::str::from_utf8(line)?);
    }
    if rows.len() != num_rows {
        return Err(anyhow::anyhow!(
            "_source synthesis produced {} rows for a {num_rows}-row batch",
            rows.len()
        ));
    }
    Ok(StringArray::from_iter_values(rows))
}

/// Explode each hit's `_source` JSON back into the hit itself.
///
/// The row-store-driven `SELECT *` (DESIGN §5) projects only
/// `_timestamp` + column-store columns + `_source` (+ explicitly requested
/// internal columns); the response layer then materializes every hit from
/// ITS OWN record here:
/// - the row's `_source` string is parsed and its fields become the hit (JSON `null` values are
///   dropped — they only appear for non-finite floats, whose `_source` image is null, matching the
///   `json_get_*` scan semantics),
/// - the row's OTHER columns (physical docs columns and explicitly referenced fields) overlay the
///   parsed object — the physical value wins on overlap (e.g. a column-store cell for a row whose
///   `_source` text drifted),
/// - the `_source` key itself never reaches the response.
///
/// Rows without a `_source` key (explicit-field queries, aggregations, or
/// legacy rows some scan could not synthesize) pass through untouched, so
/// the helper is safe to run over every response row.
pub fn expand_star_source_hits(rows: &mut [serde_json::Map<String, serde_json::Value>]) {
    for row in rows.iter_mut() {
        let Some(source_value) = row.remove(SOURCE_COL_NAME) else {
            // The expander runs over EVERY response's rows: aggregation and
            // explicit-field rows have no `_source` BY DESIGN and pass
            // through silently (they also carry no `_timestamp`). Only a
            // RECORD-shaped row missing `_source` is a degraded star hit
            // (observed live as `{"_timestamp"}`-only rows served from a
            // cold ingester's WAL window) — warn on exactly that.
            if let Some(ts) = row.get(config::TIMESTAMP_COL_NAME) {
                log::warn!(
                    "star hit carries no _source cell, keeping physical columns only (ts {ts:?})",
                );
            }
            continue;
        };
        let serde_json::Value::String(source) = source_value else {
            // defensive: a non-string `_source` cell (never produced by the
            // writers) — drop the key, keep the physical columns
            log::warn!(
                "star hit carries a non-string _source cell, keeping physical columns only (ts {:?})",
                row.get(config::TIMESTAMP_COL_NAME)
            );
            continue;
        };
        match serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&source) {
            Ok(parsed) => {
                let mut hit: serde_json::Map<String, serde_json::Value> =
                    parsed.into_iter().filter(|(_, v)| !v.is_null()).collect();
                // physical columns win on overlap
                for (key, value) in std::mem::take(row) {
                    hit.insert(key, value);
                }
                *row = hit;
            }
            Err(e) => {
                log::warn!("star hit carries unparsable _source, keeping physical columns: {e}");
            }
        }
    }
}

/// A [`PhysicalExpr`] producing the per-row `_source` JSON image of a file
/// that has no stored `_source` column: it serializes ALL of the file's own
/// (non-internal) columns per row, exactly like [`synthesize_source`] does
/// at persist time.
///
/// Its children are plain [`Column`] references into the physical file
/// schema, so DataFusion's projection machinery sees the dependencies and
/// reads those columns from the file.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SynthesizeSourceExpr {
    /// `Column` per serialized physical column (child expressions).
    columns: Vec<Arc<dyn PhysicalExpr>>,
    /// Field per serialized column (name + type for the synthesized batch).
    fields: Vec<Field>,
}

impl SynthesizeSourceExpr {
    /// Serialize every non-internal column of `physical_file_schema`.
    pub fn from_file_schema(physical_file_schema: &Schema) -> Self {
        let (columns, fields) = physical_file_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !EXCLUDED_COLS.contains(&field.name().as_str()))
            .map(|(index, field)| {
                (
                    Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                    field.as_ref().clone(),
                )
            })
            .collect();
        Self { columns, fields }
    }
}

impl Display for SynthesizeSourceExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "synthesize_source({} columns)", self.columns.len())
    }
}

impl PhysicalExpr for SynthesizeSourceExpr {
    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let num_rows = batch.num_rows();
        let arrays: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|column| column.evaluate(batch)?.into_array(num_rows))
            .collect::<DataFusionResult<_>>()?;
        let schema = Arc::new(Schema::new(self.fields.clone()));
        let projected = RecordBatch::try_new_with_options(
            schema,
            arrays,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        let source = synthesize_source(&projected)
            .map_err(|e| DataFusionError::Execution(format!("_source synthesis failed: {e}")))?;
        Ok(ColumnarValue::Array(Arc::new(source)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.columns.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            columns: children,
            fields: self.fields.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "synthesize_source")
    }
}

/// A [`PhysicalExprAdapterFactory`] that rewrites references to a `_source`
/// column MISSING from the scanned file into [`SynthesizeSourceExpr`] over
/// the file's own columns, then delegates to the default adapter for
/// everything else (missing columns → typed nulls, type casts, ...).
///
/// This is what makes a row-store-driven `SELECT *` complete over data that
/// predates the `.vix` docs blob: WAL parquet on ingesters and pre-migration
/// storage parquet carry the flattened record columns but no `_source`
/// column — instead of null-filling it (which would erase those records from
/// star hits), each row's `_source` image is synthesized from the row itself
/// at scan time, O(record) per row. Files that DO store `_source` (or scans
/// that never ask for it) are untouched.
#[derive(Debug)]
pub struct SourceSynthesizingExprAdapterFactory;

impl PhysicalExprAdapterFactory for SourceSynthesizingExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> DataFusionResult<Arc<dyn PhysicalExprAdapter>> {
        let default = DefaultPhysicalExprAdapterFactory.create(
            Arc::clone(&logical_file_schema),
            Arc::clone(&physical_file_schema),
        )?;
        Ok(Arc::new(SourceSynthesizingExprAdapter {
            logical_file_schema,
            physical_file_schema,
            default,
        }))
    }
}

#[derive(Debug)]
struct SourceSynthesizingExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    default: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for SourceSynthesizingExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let expr = if self.logical_file_schema.index_of(SOURCE_COL_NAME).is_ok()
            && self.physical_file_schema.index_of(SOURCE_COL_NAME).is_err()
        {
            let logical_type = self
                .logical_file_schema
                .field_with_name(SOURCE_COL_NAME)?
                .data_type()
                .clone();
            let physical_file_schema = Arc::clone(&self.physical_file_schema);
            expr.transform(|node| {
                if let Some(column) = node.downcast_ref::<Column>()
                    && column.name() == SOURCE_COL_NAME
                {
                    let synthesized: Arc<dyn PhysicalExpr> = Arc::new(
                        SynthesizeSourceExpr::from_file_schema(&physical_file_schema),
                    );
                    // the default adapter only casts Column nodes it rewrote
                    // itself, so match the logical type here
                    let synthesized = if logical_type == DataType::Utf8 {
                        synthesized
                    } else {
                        Arc::new(datafusion::physical_plan::expressions::CastExpr::new(
                            synthesized,
                            logical_type.clone(),
                            None,
                        ))
                    };
                    return Ok(Transformed::yes(synthesized));
                }
                Ok(Transformed::no(node))
            })
            .data()?
        } else {
            expr
        };
        self.default.rewrite(expr)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array};
    use config::TIMESTAMP_COL_NAME;
    use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
    use serde_json::json;

    use super::*;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("kubernetes.pod", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("ok", DataType::Boolean, true),
            Field::new(ID_COL_NAME, DataType::Utf8, true),
            Field::new(ORIGINAL_DATA_COL_NAME, DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1000, 1001, 1002])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("err \"quoted\"\nline"),
                    None,
                    Some(""),
                ])),
                Arc::new(StringArray::from(vec![Some("p1"), Some("p2"), None])),
                Arc::new(Int64Array::from(vec![Some(200), None, Some(500)])),
                Arc::new(Float64Array::from(vec![Some(0.5), Some(1.25), None])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(StringArray::from(vec!["id0", "id1", "id2"])),
                Arc::new(StringArray::from(vec!["raw0", "raw1", "raw2"])),
            ],
        )
        .unwrap()
    }

    fn parse(source: &StringArray, row: usize) -> serde_json::Value {
        serde_json::from_str(source.value(row)).unwrap()
    }

    #[test]
    fn test_synthesize_source_rules() {
        let source = synthesize_source(&test_batch()).unwrap();
        assert_eq!(source.len(), 3);

        // _timestamp included; _o2_id/_original excluded; nulls omitted;
        // native JSON types; dotted keys verbatim; strings with newlines and
        // quotes survive the line split (escaped inside the JSON)
        assert_eq!(
            parse(&source, 0),
            json!({
                "_timestamp": 1000,
                "log": "err \"quoted\"\nline",
                "kubernetes.pod": "p1",
                "code": 200,
                "ratio": 0.5,
                "ok": true,
            })
        );
        assert_eq!(
            parse(&source, 1),
            json!({
                "_timestamp": 1001,
                "kubernetes.pod": "p2",
                "ratio": 1.25,
                "ok": false,
            })
        );
        // the empty string is a value, not a null
        assert_eq!(
            parse(&source, 2),
            json!({
                "_timestamp": 1002,
                "log": "",
                "code": 500,
            })
        );
    }

    /// Adversarial-review probe (2026-07-23): `_source` fidelity for every
    /// arrow value shape the ingest/plan layers can realistically hand the
    /// move job.
    ///
    /// Verified exact round trips: u64 up to `u64::MAX` (schema inference
    /// produces UInt64 for values beyond i64), `i64::MIN`, unicode
    /// (CJK/emoji/U+2028), and all three arrow string flavors (Utf8 is
    /// covered by the rules test; LargeUtf8 and Utf8View here — DataFusion
    /// plans may deliver view arrays).
    ///
    /// REVIEW FINDINGS documented here:
    /// - Non-finite floats (NaN/±Inf — reachable via OTLP double attributes or VRL math) are
    ///   serialized by arrow-json as the JSON literal `null`: the value is unrecoverable from
    ///   `_source`. `_source` is authoritative, so the column-driven key-term writer treats
    ///   non-finite slots as null too (both derivations agree; see
    ///   `review_merge_paths_disagree_on_nan_inf_key_terms`) — the real NaN/Inf value survives only
    ///   in a docs cs column, when the field is column-stored.
    /// - A Timestamp-typed column (not produced by o2's log/trace inference today, but expressible
    ///   through schema evolution) morphs into an ISO STRING inside `_source` — a number-in,
    ///   string-out type change.
    #[test]
    fn review_synthesize_source_exotic_values() {
        use arrow::array::{
            LargeStringArray, StringViewArray, TimestampMicrosecondArray, UInt64Array,
        };
        use arrow_schema::TimeUnit;

        let unicode = "汉字🚀\u{2028}line";
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("f", DataType::Float64, true),
            Field::new("u", DataType::UInt64, true),
            Field::new("i", DataType::Int64, true),
            Field::new("big", DataType::LargeUtf8, true),
            Field::new("view", DataType::Utf8View, true),
            Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(1.5),
                ])),
                Arc::new(UInt64Array::from(vec![
                    Some(u64::MAX),
                    Some(i64::MAX as u64 + 1),
                    None,
                    Some(0),
                ])),
                Arc::new(Int64Array::from(vec![Some(i64::MIN), None, None, None])),
                Arc::new(LargeStringArray::from(vec![
                    Some(unicode),
                    None,
                    None,
                    None,
                ])),
                Arc::new(StringViewArray::from(vec![
                    Some("view-value"),
                    None,
                    None,
                    None,
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_704_067_200_000_000),
                    None,
                    None,
                    None,
                ])),
            ],
        )
        .unwrap();
        let source = synthesize_source(&batch).unwrap();

        let row0 = parse(&source, 0);
        // FINDING: NaN -> literal null (key present, value lost)
        assert_eq!(row0.get("f"), Some(&serde_json::Value::Null));
        assert_eq!(parse(&source, 1).get("f"), Some(&serde_json::Value::Null));
        assert_eq!(parse(&source, 2).get("f"), Some(&serde_json::Value::Null));
        assert_eq!(parse(&source, 3)["f"], json!(1.5));

        // u64 beyond i64 range round-trips exactly
        assert_eq!(row0["u"].as_u64(), Some(u64::MAX));
        assert_eq!(parse(&source, 1)["u"].as_u64(), Some(i64::MAX as u64 + 1));
        assert_eq!(row0["i"].as_i64(), Some(i64::MIN));

        // unicode and the non-Utf8 string flavors round-trip verbatim
        assert_eq!(row0["big"], json!(unicode));
        assert_eq!(row0["view"], json!("view-value"));

        // FINDING (latent): a Timestamp column becomes an ISO string
        let t = row0["t"].as_str().expect("timestamp serialized as string");
        assert!(t.starts_with("2024-01-01T00:00:00"), "got {t:?}");

        // nulls stay omitted for these types too
        let row2 = parse(&source, 2);
        assert!(row2.get("u").is_none());
        assert!(row2.get("big").is_none());
    }

    #[test]
    fn test_synthesize_source_empty_cases() {
        // zero rows
        let source = synthesize_source(&test_batch().slice(0, 0)).unwrap();
        assert_eq!(source.len(), 0);

        // a batch where every column is excluded degrades to "{}" rows
        let schema = Arc::new(Schema::new(vec![Field::new(
            ID_COL_NAME,
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef],
        )
        .unwrap();
        let source = synthesize_source(&batch).unwrap();
        assert_eq!(source.value(0), "{}");
        assert_eq!(source.value(1), "{}");
    }

    /// Hit explosion: `_source` fields materialize the hit, JSON nulls are
    /// dropped, physical columns win on overlap, `_source` never leaks, and
    /// rows without `_source` pass through untouched.
    #[test]
    fn test_expand_star_source_hits_precedence_and_parity() {
        let row = |v: serde_json::Value| match v {
            serde_json::Value::Object(m) => m,
            _ => unreachable!(),
        };
        let mut rows = vec![
            // physical `code` (column-store cell) wins over the drifted
            // `_source` text; `nan_field` null image is dropped; the rest of
            // the record comes from `_source`
            row(json!({
                "_timestamp": 1000i64,
                "code": 200,
                SOURCE_COL_NAME: "{\"_timestamp\":1000,\"code\":\"200-drifted\",\
                                  \"kubernetes.pod\":\"p1\",\"level\":\"info\",\
                                  \"nan_field\":null}",
            })),
            // no _source key (explicit-field query shape): untouched
            row(json!({"_timestamp": 2000i64, "level": "warn"})),
            // unparsable _source: physical columns kept, key dropped
            row(json!({"_timestamp": 3000i64, "code": 7, SOURCE_COL_NAME: "not-json"})),
        ];
        expand_star_source_hits(&mut rows);

        // parity: hit fields == record `_source` keys (minus nulls) ∪ physical cols
        assert_eq!(
            serde_json::Value::Object(rows[0].clone()),
            json!({
                "_timestamp": 1000i64,
                "code": 200,
                "kubernetes.pod": "p1",
                "level": "info",
            })
        );
        assert_eq!(
            serde_json::Value::Object(rows[1].clone()),
            json!({"_timestamp": 2000i64, "level": "warn"})
        );
        assert_eq!(
            serde_json::Value::Object(rows[2].clone()),
            json!({"_timestamp": 3000i64, "code": 7})
        );
    }

    /// The adapter rewrites a missing `_source` column into a synthesis over
    /// the file's own columns; evaluating the rewritten projection yields
    /// each row's full record image. Files that DO store `_source` keep the
    /// plain column reference (delegated to the default adapter).
    #[test]
    fn test_expr_adapter_synthesizes_missing_source() {
        let physical = test_batch();
        let physical_schema = physical.schema();
        // logical table schema: the row-store star shape over this file
        let logical_schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new(SOURCE_COL_NAME, DataType::Utf8, true),
        ]));

        let adapter = SourceSynthesizingExprAdapterFactory
            .create(Arc::clone(&logical_schema), physical_schema.clone())
            .unwrap();
        // the projection expression for the `_source` table column
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(SOURCE_COL_NAME, 1));
        let rewritten = adapter.rewrite(expr).unwrap();
        assert!(
            rewritten.downcast_ref::<SynthesizeSourceExpr>().is_some(),
            "expected SynthesizeSourceExpr, got {rewritten}"
        );

        let value = rewritten.evaluate(&physical).unwrap();
        let array = value.into_array(physical.num_rows()).unwrap();
        let source = array.as_any().downcast_ref::<StringArray>().unwrap();
        // identical to the persist-time synthesis of the same batch
        let expected = synthesize_source(&physical).unwrap();
        assert_eq!(source, &expected);
        // spot-check content: full record, internals excluded
        let row0 = parse(source, 0);
        assert_eq!(row0["kubernetes.pod"], json!("p1"));
        assert!(row0.get(ID_COL_NAME).is_none());

        // a file that stores `_source` keeps the plain column reference
        let physical_with_source = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new(SOURCE_COL_NAME, DataType::Utf8, true),
        ]));
        let adapter = SourceSynthesizingExprAdapterFactory
            .create(logical_schema, physical_with_source)
            .unwrap();
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(SOURCE_COL_NAME, 1));
        let rewritten = adapter.rewrite(expr).unwrap();
        assert!(
            rewritten.downcast_ref::<SynthesizeSourceExpr>().is_none(),
            "stored _source must stay a column reference"
        );

        // and a schema-only rewrite (no _source requested) matches the
        // default adapter byte-for-byte
        let logical_no_source = Arc::new(Schema::new(vec![Field::new(
            TIMESTAMP_COL_NAME,
            DataType::Int64,
            false,
        )]));
        let ours = SourceSynthesizingExprAdapterFactory
            .create(Arc::clone(&logical_no_source), physical.schema())
            .unwrap();
        let default = DefaultPhysicalExprAdapterFactory
            .create(logical_no_source, physical.schema())
            .unwrap();
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(TIMESTAMP_COL_NAME, 0));
        assert_eq!(
            ours.rewrite(Arc::clone(&expr)).unwrap().to_string(),
            default.rewrite(expr).unwrap().to_string()
        );
    }
}
