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

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{SchemaRef, SortOptions};
use async_trait::async_trait;
use config::{TIMESTAMP_COL_NAME, get_config};
use datafusion::{
    catalog::Session,
    common::{Constraints, Result},
    datasource::{MemTable, TableProvider},
    logical_expr::{Expr, TableType},
    physical_expr::{LexOrdering, PhysicalSortExpr},
    physical_plan::{ExecutionPlan, expressions::Column, sorts::sort::SortExec},
};

use crate::{
    datafusion::table_provider::helpers::{adapt_memtable_projection, apply_combined_filter},
    index::IndexCondition,
};

/// A memtable/immutable-WAL table: holds the group's RAW record batches
/// (the fields each record actually carries) and advertises the PLAN
/// schema. The raw->plan adaptation — null-padding, type casts, and
/// `_source` synthesis — happens per streamed batch inside `scan()`
/// ([`adapt_memtable_projection`]), so nothing pays for rows a query never
/// streams. The previous design adapted (and synthesized `_source` for)
/// EVERY batch eagerly at table build: each concurrent star query then
/// retained the memtable's whole JSON image — 12-24GB for a 6GB memtable —
/// which OOMKilled prod ingesters (2026-07-30).
#[derive(Debug)]
pub struct NewMemTable {
    mem_table: MemTable,
    plan_schema: SchemaRef,
    sorted_by_time: bool,
    index_condition: Option<IndexCondition>,
    fst_fields: Vec<String>,
    timestamp_filter: (i64, i64),
}

impl NewMemTable {
    /// `raw_schema`/`partitions`: the group's record batches exactly as the
    /// memtable holds them (present fields only, no `_source`).
    /// `plan_schema`: the table schema the query plan expects
    /// (`empty_exec.full_schema()` — may add `_source`, drop fields, or use
    /// evolved types); `scan()` adapts raw -> plan per streamed batch.
    pub fn try_new(
        raw_schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        plan_schema: SchemaRef,
        sorted_by_time: bool,
        index_condition: Option<IndexCondition>,
        fst_fields: Vec<String>,
        timestamp_filter: (i64, i64),
    ) -> Result<Self> {
        let mem = MemTable::try_new(raw_schema, partitions)?;
        Ok(Self {
            mem_table: mem,
            plan_schema,
            sorted_by_time,
            index_condition,
            fst_fields,
            timestamp_filter,
        })
    }
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{physical_plan::collect, prelude::SessionContext};

    use super::*;

    /// raw schema: what the memtable batches actually carry (no `_source`)
    fn raw_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("service", DataType::Utf8, true),
        ]))
    }

    /// plan schema: the star-rewritten table schema (`_source` present,
    /// `service` dropped by the plan, `level` type-evolved to Utf8View)
    fn plan_schema(source_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8View, true),
            Field::new(vortex_index::SOURCE_COL_NAME, source_type, true),
        ]))
    }

    fn raw_batch() -> RecordBatch {
        RecordBatch::try_new(
            raw_schema(),
            vec![
                Arc::new(Int64Array::from(vec![2000i64, 1000])),
                Arc::new(StringArray::from(vec![Some("error"), None])),
                Arc::new(StringArray::from(vec!["svc-a", "svc-b"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_try_new_single_empty_partition_succeeds() {
        let result = NewMemTable::try_new(
            raw_schema(),
            vec![vec![]],
            plan_schema(DataType::Utf8),
            false,
            None,
            vec![],
            (0, i64::MAX),
        );
        assert!(result.is_ok());
    }

    /// A star projection must materialize `_source` with EVERY raw field —
    /// including ones the PLAN schema dropped (`service`) — synthesized
    /// lazily per streamed batch. Regression guard for the 2026-07-30
    /// design: synthesizing after the plan projection silently loses fields
    /// (the memtable batches at scan time only carry the plan's columns),
    /// and synthesizing eagerly at table build retained the memtable's
    /// whole JSON image and OOMKilled prod ingesters.
    #[tokio::test]
    async fn star_projection_synthesizes_full_source_from_raw_batches() {
        let plan = plan_schema(DataType::Utf8);
        let table = NewMemTable::try_new(
            raw_schema(),
            vec![vec![raw_batch()]],
            Arc::clone(&plan),
            false,
            None,
            vec![],
            (0, i64::MAX),
        )
        .unwrap();

        // the row-store star projection: `_timestamp` + `_source` ONLY
        let projection = vec![
            plan.index_of("_timestamp").unwrap(),
            plan.index_of(vortex_index::SOURCE_COL_NAME).unwrap(),
        ];
        let ctx = SessionContext::new();
        let exec = table
            .scan(&ctx.state(), Some(&projection), &[], None)
            .await
            .unwrap();
        assert_eq!(
            exec.schema().fields().len(),
            2,
            "scan output must match the requested projection: {:?}",
            exec.schema()
        );
        let batches = collect(exec, ctx.task_ctx()).await.unwrap();
        let mut seen = 0;
        for b in &batches {
            let src = b
                .column_by_name(vortex_index::SOURCE_COL_NAME)
                .expect("_source projected");
            let src = src
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 _source");
            for i in 0..b.num_rows() {
                assert!(!src.is_null(i), "_source must be synthesized, not null");
                let v: serde_json::Value = serde_json::from_str(src.value(i)).unwrap();
                assert!(v.get("_timestamp").is_some(), "lost `_timestamp`: {v}");
                assert!(
                    v.get("service").is_some(),
                    "lost `service` (a raw field the plan dropped): {v}"
                );
                assert!(
                    v.get(vortex_index::SOURCE_COL_NAME).is_none(),
                    "`_source` must not nest itself: {v}"
                );
                seen += 1;
            }
        }
        assert_eq!(seen, 2, "both rows must come back");
    }

    /// The plan may type `_source` as Utf8View; `SynthesizeSourceExpr`
    /// yields Utf8, so the adaptation must cast — an uncast mismatch breaks
    /// IPC encoding ("Missing variadic count for Utf8View column", an
    /// e2e-only failure before this test existed). Raw `level` (Utf8) must
    /// likewise come back as the plan's Utf8View.
    #[tokio::test]
    async fn adaptation_casts_to_plan_types() {
        let plan = plan_schema(DataType::Utf8View);
        let table = NewMemTable::try_new(
            raw_schema(),
            vec![vec![raw_batch()]],
            Arc::clone(&plan),
            false,
            None,
            vec![],
            (0, i64::MAX),
        )
        .unwrap();
        let projection = vec![
            plan.index_of("_timestamp").unwrap(),
            plan.index_of("level").unwrap(),
            plan.index_of(vortex_index::SOURCE_COL_NAME).unwrap(),
        ];
        let ctx = SessionContext::new();
        let exec = table
            .scan(&ctx.state(), Some(&projection), &[], None)
            .await
            .unwrap();
        let batches = collect(exec, ctx.task_ctx()).await.unwrap();
        let mut rows = 0;
        for b in &batches {
            assert_eq!(
                b.schema()
                    .field_with_name(vortex_index::SOURCE_COL_NAME)
                    .unwrap()
                    .data_type(),
                &DataType::Utf8View,
            );
            assert_eq!(
                b.schema().field_with_name("level").unwrap().data_type(),
                &DataType::Utf8View,
            );
            rows += b.num_rows();
        }
        assert_eq!(rows, 2);
    }

    /// A plan field missing from the raw batches (schema evolution) comes
    /// back as typed NULLs, and a plain projection without `_source` never
    /// synthesizes anything.
    #[tokio::test]
    async fn missing_plan_field_null_pads() {
        let plan = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("brand_new_field", DataType::Utf8, true),
        ]));
        let table = NewMemTable::try_new(
            raw_schema(),
            vec![vec![raw_batch()]],
            Arc::clone(&plan),
            false,
            None,
            vec![],
            (0, i64::MAX),
        )
        .unwrap();
        let projection = vec![
            plan.index_of("level").unwrap(),
            plan.index_of("brand_new_field").unwrap(),
        ];
        let ctx = SessionContext::new();
        let exec = table
            .scan(&ctx.state(), Some(&projection), &[], None)
            .await
            .unwrap();
        let batches = collect(exec, ctx.task_ctx()).await.unwrap();
        let mut rows = 0;
        for b in &batches {
            let col = b.column_by_name("brand_new_field").unwrap();
            assert_eq!(col.null_count(), b.num_rows());
            rows += b.num_rows();
        }
        assert_eq!(rows, 2);
    }
}

#[async_trait]
impl TableProvider for NewMemTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.plan_schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.mem_table.constraints()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_schema = self.schema();
        // Plan-schema fields the scan must produce: the filter's columns,
        // `_timestamp` (for the timestamp filter), and the requested
        // projection.
        let mut needed: Vec<usize> = self
            .index_condition
            .as_ref()
            .map(|ic| ic.get_schema_projection(Arc::clone(&plan_schema), &self.fst_fields))
            .unwrap_or_default();
        if let Ok(timestamp_idx) = plan_schema.index_of(TIMESTAMP_COL_NAME)
            && !needed.contains(&timestamp_idx)
        {
            needed.push(timestamp_idx);
        }
        if let Some(v) = projection.as_ref() {
            needed.extend(v.iter().copied());
        }
        needed.sort();
        needed.dedup();

        // the requested projection, remapped to positions within `needed`
        // (the adapted output's column order) — the final narrowing that
        // `apply_combined_filter` applies after filtering
        let requested_in_needed = projection.as_ref().map(|p| {
            p.iter()
                .filter_map(|i| needed.iter().position(|f| f == i))
                .collect::<Vec<_>>()
        });

        // Raw-side projection: `_source` is synthesized from the record's
        // OTHER columns, so if it is needed the scan must read every raw
        // column or the JSON would silently lose fields (a star query
        // requests only `_timestamp` + `_source` + cs columns). Memtable
        // arrays are Arc-shared — widening copies nothing.
        let raw_schema = self.mem_table.schema();
        let needs_source = needed
            .iter()
            .any(|&i| plan_schema.field(i).name() == vortex_index::SOURCE_COL_NAME);
        let raw_projection: Vec<usize> = if needs_source {
            (0..raw_schema.fields().len()).collect()
        } else {
            needed
                .iter()
                .filter_map(|&i| raw_schema.index_of(plan_schema.field(i).name()).ok())
                .collect()
        };

        let memory_exec = self
            .mem_table
            .scan(state, Some(&raw_projection), filters, limit)
            .await?;

        // raw -> plan adaptation, per streamed batch (casts, null-padding,
        // lazy `_source` synthesis)
        let projection_exec = adapt_memtable_projection(&plan_schema, &needed, memory_exec)?;

        // if the index condition can remove filter, we can skip the config
        // feature_query_remove_filter_with_index
        let can_remove_filter = self
            .index_condition
            .as_ref()
            .map(|v| v.can_remove_filter())
            .unwrap_or(true);
        let index_condition =
            if can_remove_filter || get_config().common.feature_query_remove_filter_with_index {
                self.index_condition.as_ref()
            } else {
                None
            };
        let filter_exec = apply_combined_filter(
            index_condition,
            Some(self.timestamp_filter),
            &projection_exec.schema(),
            &self.fst_fields,
            projection_exec,
            requested_in_needed.as_ref(),
        )?;

        apply_sort(filter_exec, self.sorted_by_time)
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.mem_table.get_column_default(column)
    }
}

// create sort exec by _timestamp
fn wrap_sort(exec: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let column_timestamp = config::TIMESTAMP_COL_NAME.to_string();
    let index = exec.schema().index_of(&column_timestamp);
    (match index {
        Ok(index) => {
            let ordering = LexOrdering::new(vec![PhysicalSortExpr {
                expr: Arc::new(Column::new(&column_timestamp, index)),
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            }]);
            Arc::new(SortExec::new(ordering.unwrap(), exec))
        }
        Err(_) => exec,
    }) as _
}

fn apply_sort(
    exec_plan: Arc<dyn ExecutionPlan>,
    sorted_by_time: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(if sorted_by_time {
        wrap_sort(exec_plan)
    } else {
        exec_plan
    })
}
