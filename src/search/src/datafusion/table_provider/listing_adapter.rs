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

use arrow_schema::{SchemaRef, SortOptions};
use config::{TIMESTAMP_COL_NAME, get_config};
use datafusion::{
    catalog::{Session, TableProvider, memory::DataSourceExec},
    common::{ColumnStatistics, Result},
    datasource::{
        TableType,
        listing::{ListingTable, ListingTableConfig},
        physical_plan::{FileGroup, FileScanConfig},
    },
    execution::cache::cache_manager::FileStatisticsCache,
    logical_expr::TableProviderFilterPushDown,
    physical_expr::{LexOrdering, PhysicalSortExpr},
    physical_plan::{ExecutionPlan, expressions::Column, union::UnionExec},
    prelude::Expr,
};
use rayon::prelude::*;
use tonic::async_trait;

use crate::{
    datafusion::table_provider::helpers::{apply_combined_filter, generate_access_plan},
    index::IndexCondition,
};

#[derive(Debug)]
pub struct ListingTableAdapter {
    listing_table: ListingTable,
    trace_id: String,
    index_condition: Option<IndexCondition>,
    fst_fields: Vec<String>,
    timestamp_filter: Option<(i64, i64)>,
}

impl ListingTableAdapter {
    pub fn try_new(
        config: ListingTableConfig,
        trace_id: String,
        index_condition: Option<IndexCondition>,
        fst_fields: Vec<String>,
        timestamp_filter: Option<(i64, i64)>,
    ) -> Result<Self> {
        let listing_table = ListingTable::try_new(config)?;
        Ok(Self {
            listing_table,
            trace_id,
            index_condition,
            fst_fields,
            timestamp_filter,
        })
    }

    pub fn with_cache(mut self, cache: Option<Arc<dyn FileStatisticsCache>>) -> Self {
        self.listing_table = self.listing_table.with_cache(cache);
        self
    }
}

#[async_trait]
impl TableProvider for ListingTableAdapter {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.listing_table.schema())
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

        let Some(index_condition) = index_condition else {
            // nothing to re-apply: one branch over every file
            return match self
                .scan_branch(state, projection, filters, limit, None, None)
                .await?
            {
                Some(plan) => Ok(plan),
                None => empty_scan(self.schema(), projection),
            };
        };

        // PER-FILE fallback blast radius: files the index answered EXACTLY
        // carry a row selection whose rows already satisfy the condition —
        // they must not pay the re-applied filter (for schema-mixed fields
        // the re-check materializes `_source` per row: one partial file used
        // to poison a whole follower part, 60s vs 10s on a 12h histogram).
        // Only files WITHOUT an exact selection (partial fields, skipped
        // condition shapes, no index, eval errors) re-apply the condition —
        // their (superset) selections still prune via the access plan.
        let exact = self
            .scan_branch(state, projection, filters, limit, None, Some(true))
            .await?;
        let fallback = self
            .scan_branch(
                state,
                projection,
                filters,
                limit,
                Some(index_condition),
                Some(false),
            )
            .await?;
        match (exact, fallback) {
            (Some(exact), Some(fallback)) => {
                log::info!(
                    "[trace_id {}] [SCAN:NARROW] split scan: exact branch + fallback branch (re-applied filter on the fallback only)",
                    self.trace_id
                );
                Ok(UnionExec::try_new(vec![exact, fallback])?)
            }
            (Some(plan), None) | (None, Some(plan)) => Ok(plan),
            (None, None) => empty_scan(self.schema(), projection),
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.listing_table.supports_filters_pushdown(filters)
    }
}

impl ListingTableAdapter {
    /// One scan branch over the files matched by `keep_exact_selection`
    /// (`None` = every file): the pre-split body of `scan`, with the
    /// re-applied `index_condition` now an explicit parameter. Returns
    /// `None` when the branch matches no files at all.
    #[allow(clippy::too_many_arguments)]
    async fn scan_branch(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        index_condition: Option<&IndexCondition>,
        keep_exact_selection: Option<bool>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let (parquet_projection, filter_projection) =
            if index_condition.is_some() || self.timestamp_filter.is_some() {
                // get the projection for the filter
                let mut filter_projection = index_condition
                    .map(|ic| ic.get_schema_projection(self.schema(), &self.fst_fields))
                    .unwrap_or_default();

                // add _timestamp column if timestamp_filter is present
                if self.timestamp_filter.is_some()
                    && let Ok(timestamp_idx) = self.schema().index_of(TIMESTAMP_COL_NAME)
                    && !filter_projection.contains(&timestamp_idx)
                {
                    filter_projection.push(timestamp_idx);
                }

                // add requested projection columns
                if let Some(v) = projection.as_ref() {
                    filter_projection.extend(v.iter().copied());
                }
                filter_projection.sort();
                filter_projection.dedup();

                // regenerate the projection with the filter_projection
                let projection = projection.as_ref().map(|p| {
                    p.iter()
                        .filter_map(|i| filter_projection.iter().position(|f| f == i))
                        .collect::<Vec<_>>()
                });
                (Some(filter_projection), projection)
            } else {
                (projection.cloned(), None)
            };
        let parquet_projection = parquet_projection.as_ref();
        let filter_projection = filter_projection.as_ref();

        if let Some(projection) = parquet_projection {
            let schema = self.schema();
            let names: Vec<&str> = projection
                .iter()
                .map(|i| schema.field(*i).name().as_str())
                .collect();
            log::info!(
                "[trace_id {}] [SCAN:NARROW] provider scan columns: {names:?} (filter re-apply columns: {:?})",
                self.trace_id,
                filter_projection.map(|v| v.len()).unwrap_or(0)
            );
        }
        let parquet_exec = self
            .listing_table
            .scan(state, parquet_projection, filters, limit)
            .await?;

        let order_by_time_desc = !self.listing_table.options().file_sort_order.is_empty();
        let reverse = order_by_time_desc && parquet_exec.properties().output_ordering().is_none();
        let target_partitions = self.listing_table.options().target_partitions;
        let (parquet_exec, kept_files) = prepare_file_scan_groups(
            &self.trace_id,
            state,
            parquet_exec,
            reverse,
            target_partitions,
            keep_exact_selection,
        );
        if keep_exact_selection.is_some() && kept_files == Some(0) {
            return Ok(None);
        }

        let plan = apply_combined_filter(
            index_condition,
            self.timestamp_filter,
            &parquet_exec.schema(),
            &self.fst_fields,
            parquet_exec,
            filter_projection,
        )?;

        Ok(Some(plan))
    }
}

/// An empty result with the projected table schema (a branchless scan of an
/// empty file set).
fn empty_scan(
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = datafusion::common::project_schema(&schema, projection)?;
    Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
        schema,
    )))
}

/// Rebuild a `DataSourceExec`'s file groups for the scan: re-split (or
/// reverse) groups for `_timestamp` DESC output when `reverse` is set,
/// attach the per-file access plans (index row selections) via
/// [`generate_access_plan`], and repartition to `target_partitions`.
fn prepare_file_scan_groups(
    trace_id: &str,
    state: &dyn Session,
    plan: Arc<dyn ExecutionPlan>,
    reverse: bool,
    target_partitions: usize,
    keep_exact_selection: Option<bool>,
) -> (Arc<dyn ExecutionPlan>, Option<usize>) {
    if let Some(data_source_exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some(config) = data_source_exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
    {
        let mut file_groups = config.file_groups.clone();
        // branch split: keep only the files whose index selection exactness
        // matches (see ListingTableAdapter::scan)
        if let Some(want_exact) = keep_exact_selection {
            file_groups = file_groups
                .into_iter()
                .map(|group| {
                    FileGroup::new(
                        group
                            .into_inner()
                            .into_iter()
                            .filter(|file| {
                                super::super::storage::file_list::has_exact_scan_selection(
                                    file.path().as_ref(),
                                ) == want_exact
                            })
                            .collect(),
                    )
                })
                .filter(|group| !group.is_empty())
                .collect();
        }

        // `_source` is synthesized per row for files that do not store it
        // (WAL parquet, pre-migration storage parquet). The listing-collected
        // per-file statistics claim that column is ALL NULL (null_count ==
        // num_rows — the schema-evolution convention for a column missing
        // from the file), which lets the parquet opener constant-fold the
        // projected `_source` column to a NULL literal BEFORE the
        // synthesizing expr adapter runs — star hits then degrade to their
        // physical columns only (the prod "3-field hit" bug). The claim is
        // factually wrong for a synthesized column: downgrade it to unknown
        // so nothing folds on it. Files that really store `_source` (.vix)
        // go through VixCoreFormat, not this adapter path.
        let source_stats_idx = config
            .file_source()
            .table_schema()
            .table_schema()
            .index_of(vortex_index::SOURCE_COL_NAME)
            .ok();

        if reverse {
            let schema = config.file_source().table_schema().table_schema();
            match schema.index_of(TIMESTAMP_COL_NAME) {
                Ok(index) => {
                    let sort_order = LexOrdering::new(vec![PhysicalSortExpr {
                        expr: Arc::new(Column::new(TIMESTAMP_COL_NAME, index)),
                        options: SortOptions {
                            descending: true,
                            nulls_first: false,
                        },
                    }]);
                    if let Some(sort_order) = sort_order {
                        match FileScanConfig::split_groups_by_statistics_with_target_partitions(
                            schema,
                            &file_groups,
                            &sort_order,
                            target_partitions,
                        ) {
                            Ok(new_file_groups) => {
                                file_groups = new_file_groups;
                            }
                            Err(e) => {
                                log::warn!(
                                    "[trace_id {trace_id}] failed to split file groups by statistics: {e}, falling back to reversing file groups"
                                );
                                file_groups = file_groups
                                    .into_iter()
                                    .map(|file_group| {
                                        let mut files = file_group.into_inner();
                                        files.reverse();
                                        FileGroup::new(files)
                                    })
                                    .collect();
                            }
                        }
                    }
                }
                Err(_) => {
                    log::warn!(
                        "[trace_id {trace_id}] _timestamp column not found in schema, skipping split_groups_by_statistics"
                    );
                }
            }
        }

        let start = std::time::Instant::now();
        let new_file_groups: Vec<_> = file_groups
            .into_par_iter()
            .map(|file_group| {
                let group: Vec<_> = file_group
                    .into_inner()
                    .into_iter()
                    .map(|mut file| {
                        generate_access_plan(&mut file);
                        if let (Some(idx), Some(stats)) =
                            (source_stats_idx, file.statistics.as_ref())
                            && stats.column_statistics.len() > idx
                        {
                            let mut stats = stats.as_ref().clone();
                            stats.column_statistics[idx] = ColumnStatistics::new_unknown();
                            file.statistics = Some(Arc::new(stats));
                        }
                        file
                    })
                    .collect();
                // TODO: check if we need statistics for FileGroup
                // the statistics in FileGroup is used in ExecutionPlan::partition_statistics
                FileGroup::new(group)
            })
            .collect();

        let groups_len = new_file_groups.len();
        let max_group_len = new_file_groups.iter().map(|g| g.len()).max().unwrap_or(0);
        let files_nums = new_file_groups.iter().map(|g| g.len()).sum::<usize>();

        log::info!(
            "[trace_id {trace_id}] listing table adapter, target_partitions: {target_partitions}, file groups: {groups_len}, max group len: {max_group_len}, total files: {files_nums}, took: {} ms",
            start.elapsed().as_millis() as usize,
        );

        let mut config = config.clone();
        config.file_groups = new_file_groups;
        let mut plan = Arc::new(DataSourceExec::new(Arc::new(config))) as Arc<dyn ExecutionPlan>;
        // skip repartitioning when `reverse` is true, becuase it is already have many groups
        if !reverse
            && let Ok(Some(repartition_plan)) =
                plan.repartitioned(target_partitions, state.config_options())
        {
            plan = repartition_plan;
        }
        return (plan, Some(files_nums));
    }
    // not a plain file scan: no per-file view, report the count as unknown
    (plan, None)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int64Array, StringArray},
        record_batch::RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use config::{TIMESTAMP_COL_NAME, meta::stream::FileKey};
    use datafusion::{physical_plan::collect, prelude::SessionContext};
    use parquet::arrow::ArrowWriter;
    use vortex_index::SOURCE_COL_NAME;

    use crate::{
        datafusion::exec::{TableBuilder, create_runtime_env, create_session_config},
        index::{Condition, IndexCondition},
    };

    /// Star hits served from WAL parquet must SYNTHESIZE `_source` from the
    /// file's own columns — never null-fill it. Regression test for the prod
    /// "3-field star hit" bug: `SELECT *` with an extracted index condition
    /// returned only `_timestamp` + the filter columns for rows in the WAL
    /// parquet window (the star projection carries `_source`, the file has
    /// no such column, and the null-filled cell made the response layer fall
    /// back to physical columns).
    #[tokio::test]
    async fn wal_parquet_star_synthesizes_source() {
        // a WAL parquet file: flattened record columns, no `_source`
        let file_schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("ua", DataType::Utf8, true),
            Field::new("xff", DataType::Utf8, true),
            Field::new("uri", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3000i64, 2000, 1000])),
                Arc::new(StringArray::from(vec!["chrome", "chrome", "safari"])),
                Arc::new(StringArray::from(vec!["1.2.3.4", "1.2.3.4", "5.6.7.8"])),
                Arc::new(StringArray::from(vec!["/a", "/b", "/c"])),
                Arc::new(Int64Array::from(vec![200i64, 301, 500])),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, file_schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let wal_dir = &config::get_config().common.data_wal_dir;
        let rel = "files/default/logs/star_source_test/0/2026/01/01/00/1.parquet";
        let disk_path = format!("{wal_dir}{rel}");
        std::fs::create_dir_all(std::path::Path::new(&disk_path).parent().unwrap()).unwrap();
        std::fs::write(&disk_path, &buf).unwrap();

        let mut file = FileKey::from_file_name(rel);
        file.meta.compressed_size = buf.len() as i64;
        file.meta.original_size = buf.len() as i64;
        file.meta.records = 3;
        file.meta.min_ts = 1000;
        file.meta.max_ts = 3000;

        // the table schema the search layer hands down: file columns plus
        // the `_source` field the star rewrite projects
        let mut fields: Vec<Field> = file_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(SOURCE_COL_NAME, DataType::Utf8, true));
        let table_schema = Arc::new(Schema::new(fields));

        // an extracted equality condition (the filter the index answered)
        let mut index_condition = IndexCondition::new();
        index_condition.add_condition(Condition::Equal("ua".to_string(), "chrome".to_string()));

        let session = config::meta::search::Session {
            id: "star-source-test".to_string(),
            storage_type: config::meta::search::StorageType::Wal,
            work_group: None,
            target_partitions: 2,
        };
        let tables = TableBuilder::new()
            .index_condition(Some(index_condition))
            .fst_fields(vec![])
            .build(session, vec![file], table_schema.clone())
            .await
            .unwrap();
        assert_eq!(tables.len(), 1);

        let runtime = create_runtime_env("star-source-test", 0).await.unwrap();
        let ctx = SessionContext::new_with_config_rt(
            create_session_config(false, 2).unwrap(),
            Arc::new(runtime),
        );

        // the row-store star projection: `_timestamp` + `_source`
        let projection = vec![
            table_schema.index_of(TIMESTAMP_COL_NAME).unwrap(),
            table_schema.index_of(SOURCE_COL_NAME).unwrap(),
        ];
        let plan = tables[0]
            .scan(&ctx.state(), Some(&projection), &[], None)
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "the extracted equality keeps the 2 chrome rows");
        for batch in &batches {
            let source = batch.column_by_name(SOURCE_COL_NAME).expect("_source col");
            let source = source
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 _source");
            for i in 0..batch.num_rows() {
                assert!(
                    !source.is_null(i),
                    "_source must be synthesized from the file's columns, not null-filled"
                );
                let row: serde_json::Value = serde_json::from_str(source.value(i)).unwrap();
                assert!(
                    row.get("uri").is_some() && row.get("status").is_some(),
                    "synthesized _source must carry ALL file columns, got: {row}"
                );
            }
        }
    }

    /// THE per-file fallback blast radius (the 60s-vs-10s prod flap): a file
    /// whose index selection is EXACT must not pay the re-applied condition,
    /// while a file WITHOUT one still does — in the same scan. The exact
    /// file's selection already encodes the predicate; the fallback file
    /// re-filters. Both contribute rows, and one partial file no longer
    /// forces the re-filter (and its `_source`-scale projection) onto every
    /// other file.
    #[tokio::test]
    async fn split_scan_refilters_only_files_without_exact_selection() {
        use config::meta::stream::{FileSelection, RowIdBitmap};

        let file_schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
        ]));
        let write_file = |rel: &str, svcs: [&str; 3]| {
            let batch = RecordBatch::try_new(
                file_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![3000i64, 2000, 1000])),
                    Arc::new(StringArray::from(svcs.to_vec())),
                ],
            )
            .unwrap();
            let mut buf = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, file_schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let wal_dir = &config::get_config().common.data_wal_dir;
            let disk_path = format!("{wal_dir}{rel}");
            std::fs::create_dir_all(std::path::Path::new(&disk_path).parent().unwrap()).unwrap();
            std::fs::write(&disk_path, &buf).unwrap();
            let mut file = FileKey::from_file_name(rel);
            file.meta.compressed_size = buf.len() as i64;
            file.meta.original_size = buf.len() as i64;
            file.meta.records = 3;
            file.meta.min_ts = 1000;
            file.meta.max_ts = 3000;
            file
        };

        // exact file: rows [nexus, other, nexus]; the index answered rows
        // {0, 2} EXACTLY — no re-filter may run on it (its selection is the
        // predicate)
        let mut exact_file = write_file(
            "files/default/logs/split_scan_test/0/2026/01/01/00/exact.parquet",
            ["nexus", "other", "nexus"],
        );
        exact_file.with_selection(
            FileSelection::Rows(Arc::new(RowIdBitmap::from_row_ids(3, [0u32, 2]))),
            None,
        );
        exact_file.selection_exact = true;

        // fallback file: no selection (the index skipped it) — the
        // re-applied condition must drop its non-matching rows
        let fallback_file = write_file(
            "files/default/logs/split_scan_test/0/2026/01/01/00/fallback.parquet",
            ["nexus", "other", "other"],
        );

        let mut index_condition = IndexCondition::new();
        index_condition.add_condition(Condition::Equal("svc".to_string(), "nexus".to_string()));

        let session = config::meta::search::Session {
            id: "split-scan-test".to_string(),
            storage_type: config::meta::search::StorageType::Wal,
            work_group: None,
            target_partitions: 2,
        };
        let tables = TableBuilder::new()
            .index_condition(Some(index_condition))
            .fst_fields(vec![])
            .build(
                session,
                vec![exact_file, fallback_file],
                file_schema.clone(),
            )
            .await
            .unwrap();
        assert_eq!(tables.len(), 1);

        let runtime = create_runtime_env("split-scan-test", 0).await.unwrap();
        let ctx = SessionContext::new_with_config_rt(
            create_session_config(false, 2).unwrap(),
            Arc::new(runtime),
        );

        let projection = vec![file_schema.index_of(TIMESTAMP_COL_NAME).unwrap()];
        let plan = tables[0]
            .scan(&ctx.state(), Some(&projection), &[], None)
            .await
            .unwrap();
        let display = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string();
        assert!(
            display.contains("UnionExec"),
            "exact + fallback files must split into a union: {display}"
        );

        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // exact file contributes its 2 selected rows (no re-filter), the
        // fallback file re-filters down to its 1 nexus row
        assert_eq!(rows, 3, "2 exact-selection rows + 1 re-filtered row");
    }
}
