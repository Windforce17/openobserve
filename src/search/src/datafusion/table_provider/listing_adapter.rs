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

use arrow_schema::SchemaRef;
use config::{TIMESTAMP_COL_NAME, get_config};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{ColumnStatistics, DataFusionError, Result},
    datasource::{
        TableType,
        listing::{ListingTable, ListingTableConfig},
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder},
        table_schema::TableSchema,
    },
    execution::cache::cache_manager::FileStatisticsCache,
    logical_expr::TableProviderFilterPushDown,
    physical_expr_adapter::PhysicalExprAdapterFactory,
    physical_plan::{ExecutionPlan, union::UnionExec},
    prelude::Expr,
};
use datafusion_datasource::compute_all_files_statistics;
use tonic::async_trait;

use crate::{
    datafusion::table_provider::helpers::{apply_combined_filter, generate_access_plan},
    index::IndexCondition,
};

pub struct ListingTableAdapter {
    listing_table: ListingTable,
    file_schema: SchemaRef,
    expr_adapter: Option<Arc<dyn PhysicalExprAdapterFactory>>,
    trace_id: String,
    index_condition: Option<IndexCondition>,
    fst_fields: Vec<String>,
    timestamp_filter: Option<(i64, i64)>,
}

impl std::fmt::Debug for ListingTableAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListingTableAdapter")
            .field("listing_table", &self.listing_table)
            .field("file_schema", &self.file_schema)
            .field("trace_id", &self.trace_id)
            .field("index_condition", &self.index_condition)
            .field("fst_fields", &self.fst_fields)
            .field("timestamp_filter", &self.timestamp_filter)
            .finish()
    }
}

impl ListingTableAdapter {
    pub fn try_new(
        config: ListingTableConfig,
        trace_id: String,
        index_condition: Option<IndexCondition>,
        fst_fields: Vec<String>,
        timestamp_filter: Option<(i64, i64)>,
        expr_adapter: Option<Arc<dyn PhysicalExprAdapterFactory>>,
    ) -> Result<Self> {
        let file_schema = config.file_schema.clone().ok_or_else(|| {
            DataFusionError::Internal("ListingTableAdapter requires a file schema".to_string())
        })?;
        let listing_table = ListingTable::try_new(config)?;
        Ok(Self {
            listing_table,
            file_schema,
            expr_adapter,
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

        // Enumerate once, with no raw-row LIMIT shortcut: membership, access
        // plans, residuals and ordered winners have not been established yet.
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let partition_filters = self
            .listing_table
            .supports_filters_pushdown(&filter_refs)?
            .into_iter()
            .zip(filters)
            .filter_map(|(pushdown, filter)| {
                matches!(pushdown, TableProviderFilterPushDown::Exact).then(|| filter.clone())
            })
            .collect::<Vec<_>>();
        let listed = self
            .listing_table
            .list_files_for_scan(state, &partition_filters, None)
            .await?;
        let source_stats_idx = self.schema().index_of(vortex_index::SOURCE_COL_NAME).ok();
        let mut exact_groups = Vec::new();
        let mut fallback_groups = Vec::new();
        let mut exact_files = 0;
        let mut fallback_files = 0;
        for group in listed.file_groups {
            let mut exact = Vec::new();
            let mut fallback = Vec::new();
            for mut file in group.into_inner() {
                let is_exact = generate_access_plan(&mut file);
                // A synthesized parquet _source is not all-NULL even when
                // its physical file schema lacks the column.
                if let (Some(idx), Some(stats)) = (source_stats_idx, &mut file.statistics)
                    && idx < stats.column_statistics.len()
                {
                    let stats = Arc::make_mut(stats);
                    stats.column_statistics[idx] = ColumnStatistics::new_unknown();
                }
                if index_condition.is_none() || is_exact {
                    exact.push(file);
                    exact_files += 1;
                } else {
                    fallback.push(file);
                    fallback_files += 1;
                }
            }
            if !exact.is_empty() {
                exact_groups.push(FileGroup::new(exact));
            }
            if !fallback.is_empty() {
                fallback_groups.push(FileGroup::new(fallback));
            }
        }
        let target = self.listing_table.options().target_partitions.max(1);
        let (exact_target, fallback_target) =
            allocate_branch_partitions(exact_files, fallback_files, target);
        // Access/residual/time filters run below the outer SQL limit. Passing
        // it to either unfiltered source could stop before a qualifying row.
        let scan_limit = if self.index_condition.is_none()
            && self.timestamp_filter.is_none()
            && filters.is_empty()
            && self.listing_table.options().file_sort_order.is_empty()
            && exact_groups.iter().flat_map(|g| g.iter()).all(|file| {
                file.statistics
                    .as_ref()
                    .is_some_and(|stats| stats.num_rows.is_exact() == Some(true))
            }) {
            limit
        } else {
            None
        };
        let mut plans = Vec::with_capacity(2);
        if exact_files > 0 {
            plans.push(
                self.scan_branch(
                    state,
                    projection,
                    scan_limit,
                    None,
                    exact_groups,
                    listed.grouped_by_partition,
                    exact_target,
                )
                .await?,
            );
        }
        if fallback_files > 0 {
            plans.push(
                self.scan_branch(
                    state,
                    projection,
                    None,
                    index_condition,
                    fallback_groups,
                    listed.grouped_by_partition,
                    fallback_target,
                )
                .await?,
            );
        }
        match plans.len() {
            0 => empty_scan(self.schema(), projection),
            1 => Ok(plans.pop().unwrap()),
            _ => Ok(UnionExec::try_new(plans)?),
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.listing_table.supports_filters_pushdown(filters)
    }
}

fn allocate_branch_partitions(
    exact_files: usize,
    fallback_files: usize,
    target_partitions: usize,
) -> (usize, usize) {
    let target = target_partitions.max(1);
    match (exact_files, fallback_files) {
        (0, 0) => (0, 0),
        (0, _) => (0, target),
        (_, 0) => (target, 0),
        _ if target == 1 => (1, 1),
        _ => {
            let total = exact_files as u128 + fallback_files as u128;
            let proportional = ((target as u128 * exact_files as u128) / total) as usize;
            let exact = proportional.clamp(1, target - 1);
            (exact, target - exact)
        }
    }
}

impl ListingTableAdapter {
    /// Construct one nonempty branch from its final membership and partition
    /// share. Listing, selection attachment and branch discovery are complete.
    #[allow(clippy::too_many_arguments)]
    async fn scan_branch(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        index_condition: Option<&IndexCondition>,
        file_groups: Vec<FileGroup>,
        grouped_by_partition: bool,
        target_partitions: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
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
                } else {
                    filter_projection.extend(0..self.schema().fields().len());
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
        let options = self.listing_table.options();
        let mut file_groups = if grouped_by_partition {
            file_groups
        } else {
            FileGroup::new(
                file_groups
                    .into_iter()
                    .flat_map(|g| g.into_inner())
                    .collect(),
            )
            .split_files(target_partitions)
        };
        let output_ordering = self
            .listing_table
            .try_create_output_ordering(state.execution_props(), &file_groups)?;
        let declared_order = !options.file_sort_order.is_empty();
        if (declared_order
            || state
                .config_options()
                .execution
                .split_file_groups_by_statistics)
            && let Some(ordering) = output_ordering.first()
        {
            match FileScanConfig::split_groups_by_statistics_with_target_partitions(
                &self.schema(),
                &file_groups,
                ordering,
                target_partitions,
            ) {
                Ok(groups) => file_groups = groups,
                Err(error) if declared_order => {
                    log::warn!(
                        "[trace_id {}] failed to split file groups by statistics: {error}; reversing file groups",
                        self.trace_id
                    );
                    file_groups = file_groups
                        .into_iter()
                        .map(|group| {
                            let mut files = group.into_inner();
                            files.reverse();
                            FileGroup::new(files)
                        })
                        .collect();
                }
                Err(error) => log::debug!("failed to split file groups by statistics: {error}"),
            }
        }
        let (file_groups, statistics) =
            compute_all_files_statistics(file_groups, self.schema(), options.collect_stat, false)?;
        let table_partition_cols = options
            .table_partition_cols
            .iter()
            .map(|(name, _)| {
                self.schema()
                    .field_with_name(name)
                    .map(|f| Arc::new(f.clone()))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let file_source = options.format.file_source(TableSchema::new(
            Arc::clone(&self.file_schema),
            table_partition_cols,
        ));
        let object_store_url = self
            .listing_table
            .table_paths()
            .first()
            .ok_or_else(|| {
                DataFusionError::Internal("listed files have no object store".to_string())
            })?
            .object_store();
        let conf = FileScanConfigBuilder::new(object_store_url, file_source)
            .with_file_groups(file_groups)
            .with_statistics(statistics)
            .with_constraints(
                self.listing_table
                    .constraints()
                    .cloned()
                    .unwrap_or_default(),
            )
            .with_projection_indices(parquet_projection.cloned())?
            .with_limit(limit)
            .with_output_ordering(output_ordering)
            .with_expr_adapter(self.expr_adapter.clone())
            .with_partitioned_by_file_group(grouped_by_partition)
            .build();
        let mut parquet_exec = options.format.create_physical_plan(state, conf).await?;
        if !declared_order
            && let Some(repartitioned) =
                parquet_exec.repartitioned(target_partitions, state.config_options())?
        {
            parquet_exec = repartitioned;
        }

        let plan = apply_combined_filter(
            index_condition,
            self.timestamp_filter,
            &parquet_exec.schema(),
            &self.fst_fields,
            parquet_exec,
            filter_projection,
        )?;

        Ok(plan)
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
        assert!(
            plan.properties().output_partitioning().partition_count() <= 2,
            "one nonempty branch must retain the table's full partition target"
        );
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
            Field::new("code", DataType::Int64, true),
        ]));
        let write_file = |rel: &str, svcs: [&str; 3]| {
            let batch = RecordBatch::try_new(
                file_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![3000i64, 2000, 1000])),
                    Arc::new(StringArray::from(svcs.to_vec())),
                    Arc::new(Int64Array::from(vec![30i64, 20, 10])),
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
        let mut fallback_file = write_file(
            "files/default/logs/split_scan_test/0/2026/01/01/00/fallback.parquet",
            ["other", "nexus", "other"],
        );
        // The object exists on the real WAL store, but its registration has
        // no trusted size/count. Listing must resolve its size rather than
        // silently dropping it before the footer/scan can produce row 2000.
        fallback_file.meta.compressed_size = 0;
        fallback_file.meta.records = 0;
        let mut partial_file = write_file(
            "files/default/logs/split_scan_test/0/2026/01/01/00/partial.parquet",
            ["nexus", "other", "nexus"],
        );
        partial_file.with_selection(
            FileSelection::Rows(Arc::new(RowIdBitmap::from_row_ids(3, [1u32, 2]))),
            None,
        );
        let empty_file = write_file(
            "files/default/logs/split_scan_test/0/2026/01/01/00/first.parquet",
            ["other", "other", "other"],
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
            .timestamp_filter((1000, 3000))
            .fst_fields(vec![])
            .build(
                session,
                vec![empty_file, exact_file, partial_file, fallback_file],
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

        // Reordered same-typed fields and duplicate output slots must not
        // inherit the sorted/deduplicated physical fetch projection.
        let projection = vec![2, 0, 2];
        let plan = tables[0]
            .scan(&ctx.state(), Some(&projection), &[], Some(1))
            .await
            .unwrap();

        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["code", TIMESTAMP_COL_NAME, "code"]
            );
            let cols = batch
                .columns()
                .iter()
                .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap())
                .collect::<Vec<_>>();
            for row in 0..batch.num_rows() {
                rows.push((cols[0].value(row), cols[1].value(row), cols[2].value(row)));
            }
        }
        rows.sort_unstable();
        assert_eq!(rows, vec![(10, 1000, 10), (10, 1000, 10), (20, 2000, 20)]);

        let plan = tables[0].scan(&ctx.state(), None, &[], None).await.unwrap();
        assert_eq!(
            plan.schema(),
            file_schema,
            "None projects every table column"
        );
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        crate::datafusion::storage::file_list::clear("split-scan-test");
    }
}
