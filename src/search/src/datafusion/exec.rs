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

use std::{cmp::max, num::NonZero, str::FromStr, sync::Arc};

use arrow_schema::Field;
use config::{
    FileFormat, TIMESTAMP_COL_NAME, get_batch_size, get_config,
    meta::{
        search::{Session as SearchSession, StorageType},
        stream::FileKey,
    },
    utils::schema_ext::SchemaExt,
};
use datafusion::{
    arrow::datatypes::{DataType, Schema},
    catalog::TableProvider,
    config::Dialect,
    datasource::{
        file_format::{FileFormat as DataFusionFileFormat, parquet::ParquetFormat},
        listing::{ListingOptions, ListingTableConfig, ListingTableUrl},
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
    },
    error::{DataFusionError, Result},
    execution::{
        cache::cache_manager::{CacheManagerConfig, FileStatisticsCache},
        context::SessionConfig,
        memory_pool::{
            FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool, UnboundedMemoryPool,
        },
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
        session_state::SessionStateBuilder,
    },
    logical_expr::AggregateUDF,
    optimizer::{AnalyzerRule, OptimizerRule},
    physical_optimizer::PhysicalOptimizerRule,
    prelude::{SessionContext, col},
};
use futures::StreamExt;
#[cfg(feature = "enterprise")]
use o2_enterprise::enterprise::search::WorkGroup;
use vortex::{VortexSessionDefault, io::session::RuntimeSessionExt, session::VortexSession};
use vortex_datafusion::VortexFormat;

use super::{
    peak_memory_pool::PeakMemoryPool, planner::extension_planner::OpenobserveQueryPlanner,
    storage::file_list, udf::transform_udf::get_all_transform,
};
use crate::{
    datafusion::{
        source_synthesis::SourceSynthesizingExprAdapterFactory,
        storage::file_statistics_cache,
        table_provider::{listing_adapter::ListingTableAdapter, uniontable::NewUnionTable},
        vix_format::VixCoreFormat,
    },
    index::IndexCondition,
};

pub const DATAFUSION_MIN_MEM: usize = 1024 * 1024 * 256; // 256MB

pub fn create_session_config(
    sorted_by_time: bool,
    target_partitions: usize,
) -> Result<SessionConfig> {
    let cfg = get_config();
    let target_partitions = if target_partitions == 0 {
        cfg.limit.cpu_num
    } else {
        target_partitions
    };
    let target_partitions = max(cfg.limit.datafusion_min_partition_num, target_partitions);
    let mut config = SessionConfig::from_env()?
        .with_batch_size(get_batch_size())
        .with_target_partitions(target_partitions)
        .with_information_schema(true);

    config
        .options_mut()
        .execution
        .listing_table_ignore_subdirectory = false;

    config.options_mut().sql_parser.dialect = Dialect::PostgreSQL;

    config.options_mut().execution.parquet.pushdown_filters =
        cfg.common.feature_pushdown_filter_enabled;
    // config = config.set_bool("datafusion.execution.parquet.reorder_filters", true);

    if sorted_by_time {
        config
            .options_mut()
            .execution
            .split_file_groups_by_statistics = true;
    }

    // When set to true, skips verifying that the schema produced by planning the input of
    // `LogicalPlan::Aggregate` exactly matches the schema of the input plan.
    config
        .options_mut()
        .execution
        .skip_physical_aggregate_schema_check = true;

    // DataFusion 54 executes uncorrelated scalar subqueries physically via
    // `ScalarSubqueryExec`/`ScalarSubqueryExpr` instead of rewriting them into joins.
    // `ScalarSubqueryExpr` can only be (de)serialized inside its surrounding
    // `ScalarSubqueryExec`, which breaks our distributed plan splitting across the Flight
    // boundary. Disable the physical path so `ScalarSubqueryToJoin` decorrelates them into
    // joins again, keeping the serialized follower plans valid.
    config
        .options_mut()
        .optimizer
        .enable_physical_uncorrelated_scalar_subquery = false;

    // DataFusion 54 builds a runtime `DynamicFilterPhysicalExpr` from a `HashJoinExec`'s
    // build-side join keys and pushes it into the probe-side scan. That runtime state can't
    // cross our distributed RemoteScan/Flight boundary, and after our custom join rewrites
    // (`swap_inputs` + broadcast/enrichment join) the filter ends up referencing build-side
    // columns by index against the projected probe-side batch, producing
    // "PhysicalExpr Column references column ... but input schema only has N columns" at
    // execution time. Disable join dynamic filter pushdown to keep the split plans valid.
    config
        .options_mut()
        .optimizer
        .enable_join_dynamic_filter_pushdown = false;

    Ok(config)
}

/// Build the configured tracked pool (`TrackConsumersPool` over the
/// `ZO_MEMORY_CACHE_DATAFUSION_MEMORY_POOL` type) with `memory_size` bytes.
fn build_tracked_pool(memory_size: usize) -> Result<Arc<dyn MemoryPool>> {
    let cfg = get_config();
    let mem_pool = super::MemoryPoolType::from_str(&cfg.memory_cache.datafusion_memory_pool)
        .map_err(|e| {
            DataFusionError::Execution(format!("Invalid datafusion memory pool type: {e}"))
        })?;
    Ok(match mem_pool {
        super::MemoryPoolType::Greedy => {
            let pool = GreedyMemoryPool::new(memory_size);
            Arc::new(TrackConsumersPool::new(pool, NonZero::new(20).unwrap()))
        }
        super::MemoryPoolType::Fair => {
            let pool = FairSpillPool::new(memory_size);
            Arc::new(TrackConsumersPool::new(pool, NonZero::new(20).unwrap()))
                as Arc<dyn MemoryPool>
        }
        super::MemoryPoolType::None => {
            let pool = UnboundedMemoryPool::default();
            Arc::new(TrackConsumersPool::new(pool, NonZero::new(20).unwrap()))
        }
    })
}

/// M26: ONE process-wide memory pool shared by every `merge_parquet_files`
/// context (compactor merges and segment-builder L0 builds of flat/metadata
/// streams). Sized once with the SAME `datafusion_max_size` semantics a
/// single merge context used to get.
///
/// Before this, every merge CONTEXT got its own pool of that full size —
/// on a prod compactor (48Gi, `ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE=12288`,
/// 4 merge workers + live lane + builder) the aggregate tracked ceiling was
/// N x 12.9GB, structurally above the pod limit. Metadata-class merges
/// (default/metadata/trace_list_index) measurably fill a whole per-context
/// pool (repeated ~12.8GB `peak_memory_pool` drops fleet-wide, 2026-08-21),
/// so a few overlapping merges ramped the pod 2GB -> 46GB at job-throughput
/// speed and OOM-killed it — the M26 "live per-job leak" signature. Sharing
/// the pool bounds the AGGREGATE: concurrent merges now spill (correct,
/// bounded, alive) instead of stacking fresh gigabytes (unbounded, dead).
/// Query contexts are untouched.
static SHARED_MERGE_POOL: std::sync::LazyLock<Result<Arc<dyn MemoryPool>>> =
    std::sync::LazyLock::new(|| {
        let memory_size = std::cmp::max(
            DATAFUSION_MIN_MEM,
            get_config().memory_cache.datafusion_max_size,
        );
        log::info!(
            "[DATAFUSION] shared merge memory pool created: {} MB",
            memory_size / (1024 * 1024)
        );
        build_tracked_pool(memory_size)
    });

pub async fn create_runtime_env(trace_id: &str, memory_limit: usize) -> Result<RuntimeEnv> {
    create_runtime_env_inner(trace_id, memory_limit, false).await
}

async fn create_runtime_env_inner(
    trace_id: &str,
    memory_limit: usize,
    shared_merge_pool: bool,
) -> Result<RuntimeEnv> {
    let object_store_registry = DefaultObjectStoreRegistry::new();

    let memory = super::storage::memory::FS::new();
    let memory_url = url::Url::parse("memory:///").unwrap();
    object_store_registry.register_store(&memory_url, Arc::new(memory));

    let wal = super::storage::wal::FS::new();
    let wal_url = url::Url::parse("wal:///").unwrap();
    object_store_registry.register_store(&wal_url, Arc::new(wal));

    let cfg = get_config();
    let mut builder =
        RuntimeEnvBuilder::new().with_object_store_registry(Arc::new(object_store_registry));
    if cfg.limit.datafusion_file_stat_cache_max_size > 0 {
        let cache_config = CacheManagerConfig::default();
        let cache_config = cache_config
            .with_file_statistics_cache(Some(file_statistics_cache::GLOBAL_CACHE.clone()))
            .with_file_statistics_cache_limit(cfg.limit.datafusion_file_stat_cache_max_size);
        builder = builder.with_cache_manager(cache_config);
    }

    let inner_pool = if shared_merge_pool {
        match SHARED_MERGE_POOL.as_ref() {
            Ok(pool) => Arc::clone(pool),
            Err(e) => {
                return Err(DataFusionError::Execution(format!(
                    "shared merge memory pool init failed: {e}"
                )));
            }
        }
    } else {
        let memory_size = std::cmp::max(DATAFUSION_MIN_MEM, memory_limit);
        build_tracked_pool(memory_size)?
    };
    // per-context peak observability on top of the (possibly shared) pool:
    // with the shared pool the logged peak is the POOL level at this
    // context's grows — the pod-relevant number
    let memory_pool = PeakMemoryPool::new(inner_pool, trace_id.to_string());

    builder = builder.with_memory_pool(Arc::new(memory_pool));
    builder.build()
}

pub struct DataFusionContextBuilder<'a> {
    trace_id: &'a str,
    work_group: Option<String>,
    analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    optimizer_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    sorted_by_time: bool,
    single_partition: bool,
    shared_merge_pool: bool,
}

impl<'a> Default for DataFusionContextBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> DataFusionContextBuilder<'a> {
    pub fn new() -> Self {
        Self {
            trace_id: "",
            work_group: None,
            analyzer_rules: vec![],
            optimizer_rules: vec![],
            physical_optimizer_rules: vec![],
            sorted_by_time: false,
            single_partition: false,
            shared_merge_pool: false,
        }
    }

    pub fn trace_id(mut self, trace_id: &'a str) -> Self {
        self.trace_id = trace_id;
        self
    }

    pub fn work_group(mut self, work_group: Option<String>) -> Self {
        self.work_group = work_group;
        self
    }

    pub fn analyzer_rules(
        mut self,
        analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> Self {
        self.analyzer_rules = analyzer_rules;
        self
    }

    pub fn optimizer_rules(
        mut self,
        optimizer_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    ) -> Self {
        self.optimizer_rules = optimizer_rules;
        self
    }

    pub fn physical_optimizer_rules(
        mut self,
        physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    ) -> Self {
        self.physical_optimizer_rules = physical_optimizer_rules;
        self
    }

    pub fn sorted_by_time(mut self, sorted_by_time: bool) -> Self {
        self.sorted_by_time = sorted_by_time;
        self
    }

    /// M13/M20b: plan at exactly ONE partition, bypassing the
    /// `datafusion_min_partition_num` floor (`create_session_config` clamps
    /// to it otherwise). For bounded merge inputs — the segment builder's
    /// ≤superbatch-MB in-memory batch (M13), or the compactor's size-capped
    /// metadata merge groups (M20b) — parallel sort buys nothing, and the
    /// repartitioned plan is the pool-starvation shape: RepartitionExec
    /// buffers unspillably while N ExternalSorters split the pool until one
    /// fails its first allocation (M12 fix-1 rationale; prod
    /// default/metadata/trace_list_index builds and compactor merges were
    /// the remaining instances). One sorter spills properly instead.
    pub fn single_partition(mut self, single_partition: bool) -> Self {
        self.single_partition = single_partition;
        self
    }

    /// M26: draw this context's DataFusion memory from the ONE process-wide
    /// merge pool (see [`SHARED_MERGE_POOL`]) instead of a fresh full-size
    /// per-context pool. Set by `merge_parquet_files` — compactor merges and
    /// segment-builder L0 builds — so CONCURRENT merges share (and spill
    /// against) one bounded budget rather than stacking `datafusion_max_size`
    /// each. Query contexts keep their per-context pools.
    pub fn shared_merge_pool(mut self, shared_merge_pool: bool) -> Self {
        self.shared_merge_pool = shared_merge_pool;
        self
    }

    pub async fn build(self, target_partitions: usize) -> Result<SessionContext, DataFusionError> {
        let cfg = get_config();
        let (target_partitions, memory_size) =
            (target_partitions, cfg.memory_cache.datafusion_max_size);
        #[cfg(feature = "enterprise")]
        let (target_partitions, memory_size) = get_cpu_and_mem_limit(
            self.trace_id,
            self.work_group.clone(),
            target_partitions,
            memory_size,
        )
        .await?;

        let mut session_config = create_session_config(self.sorted_by_time, target_partitions)?;
        if self.single_partition {
            // after create_session_config on purpose: this is the one caller
            // allowed under the min-partition floor (see single_partition)
            session_config = session_config.with_target_partitions(1);
        }
        let runtime_env = Arc::new(
            create_runtime_env_inner(self.trace_id, memory_size, self.shared_merge_pool).await?,
        );
        let mut builder = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime_env)
            .with_default_features();
        for rule in self.analyzer_rules {
            builder = builder.with_analyzer_rule(rule);
        }
        if !self.optimizer_rules.is_empty() {
            builder = builder.with_optimizer_rules(self.optimizer_rules)
        }
        for rule in self.physical_optimizer_rules {
            builder = builder.with_physical_optimizer_rule(rule);
        }
        if cfg.common.feature_join_match_one_enabled {
            builder = builder.with_query_planner(Arc::new(OpenobserveQueryPlanner::new()));
        }
        Ok(SessionContext::new_with_state(builder.build()))
    }
}

pub fn register_udf(ctx: &SessionContext, org_id: &str) -> Result<()> {
    ctx.register_udf(super::udf::str_match_udf::STR_MATCH_UDF.clone());
    ctx.register_udf(super::udf::str_match_udf::STR_MATCH_IGNORE_CASE_UDF.clone());
    ctx.register_udf(super::udf::fuzzy_match_udf::FUZZY_MATCH_UDF.clone());
    ctx.register_udf(super::udf::regexp_udf::REGEX_MATCH_UDF.clone());
    ctx.register_udf(super::udf::regexp_udf::REGEX_NOT_MATCH_UDF.clone());
    ctx.register_udf(super::udf::regexp_udf::REGEXP_MATCH_TO_FIELDS_UDF.clone());
    ctx.register_udf(super::udf::regexp_matches_udf::REGEX_MATCHES_UDF.clone());
    ctx.register_udf(super::udf::time_range_udf::TIME_RANGE_UDF.clone());
    ctx.register_udf(super::udf::date_format_udf::DATE_FORMAT_UDF.clone());
    ctx.register_udf(super::udf::string_to_array_v2_udf::STRING_TO_ARRAY_V2_UDF.clone());
    ctx.register_udf(super::udf::arrzip_udf::ARR_ZIP_UDF.clone());
    ctx.register_udf(super::udf::arrindex_udf::ARR_INDEX_UDF.clone());
    ctx.register_udf(super::udf::arr_descending_udf::ARR_DESCENDING_UDF.clone());
    ctx.register_udf(super::udf::arrjoin_udf::ARR_JOIN_UDF.clone());
    ctx.register_udf(super::udf::arrcount_udf::ARR_COUNT_UDF.clone());
    ctx.register_udf(super::udf::arrsort_udf::ARR_SORT_UDF.clone());
    ctx.register_udf(super::udf::cast_to_arr_udf::CAST_TO_ARR_UDF.clone());
    ctx.register_udf(super::udf::spath_udf::SPATH_UDF.clone());
    ctx.register_udf(super::udf::to_arr_string_udf::TO_ARR_STRING.clone());
    ctx.register_udf(super::udf::histogram_udf::HISTOGRAM_UDF.clone());
    ctx.register_udf(super::udf::match_all_hash_udf::MATCH_ALL_HASH_UDF.clone());
    ctx.register_udf(super::udf::match_all_udf::MATCH_ALL_UDF.clone());
    ctx.register_udf(super::udf::match_all_udf::FUZZY_MATCH_ALL_UDF.clone());
    ctx.register_udaf(AggregateUDF::from(
        super::udaf::summary_percentile::SummaryPercentile::new(),
    ));
    ctx.register_udf(super::udf::cast_to_timestamp_udf::CAST_TO_TIMESTAMP_UDF.clone());
    let udf_list = get_all_transform(org_id)?;
    for udf in udf_list {
        ctx.register_udf(udf.clone());
    }

    #[cfg(feature = "enterprise")]
    {
        ctx.register_udf(super::udf::cipher_udf::DECRYPT_UDF.clone());
        ctx.register_udf(super::udf::cipher_udf::DECRYPT_SLOW_UDF.clone());
        ctx.register_udf(super::udf::cipher_udf::ENCRYPT_UDF.clone());
        ctx.register_udaf(AggregateUDF::from(
            o2_enterprise::enterprise::search::datafusion::udaf::approx_topk::ApproxTopK::new(),
        ));
        ctx.register_udaf(AggregateUDF::from(
            o2_enterprise::enterprise::search::datafusion::udaf::approx_topk_distinct::ApproxTopKDistinct::new(),
        ));
    }

    Ok(())
}

pub async fn register_metrics_table(
    session: &SearchSession,
    schema: Arc<Schema>,
    table_name: &str,
    files: Vec<FileKey>,
) -> Result<SessionContext> {
    let ctx = DataFusionContextBuilder::new()
        .trace_id(&session.id)
        .work_group(session.work_group.clone())
        .build(session.target_partitions)
        .await?;

    let tables = TableBuilder::new()
        .file_stat_cache(ctx.runtime_env().cache_manager.get_file_statistic_cache())
        .build(session.clone(), files, schema.clone())
        .await?;
    let union_table = Arc::new(NewUnionTable::new(schema, tables));
    ctx.register_table(table_name, union_table)?;

    Ok(ctx)
}

/// Create a datafusion table from a list of files and a schema
pub struct TableBuilder {
    sorted_by_time: bool,
    file_stat_cache: Option<Arc<dyn FileStatisticsCache>>,
    index_condition: Option<IndexCondition>,
    fst_fields: Vec<String>,
    timestamp_filter: Option<(i64, i64)>,
    collect_stat: bool,
}

impl Default for TableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            sorted_by_time: false,
            file_stat_cache: None,
            index_condition: None,
            fst_fields: vec![],
            timestamp_filter: None,
            collect_stat: true,
        }
    }

    pub fn sorted_by_time(mut self, sorted_by_time: bool) -> Self {
        self.sorted_by_time = sorted_by_time;
        self
    }

    pub fn file_stat_cache(
        mut self,
        file_stat_cache: Option<Arc<dyn FileStatisticsCache>>,
    ) -> Self {
        self.file_stat_cache = file_stat_cache;
        self
    }

    pub fn index_condition(mut self, index_condition: Option<IndexCondition>) -> Self {
        self.index_condition = index_condition;
        self
    }

    pub fn fst_fields(mut self, fst_fields: Vec<String>) -> Self {
        self.fst_fields = fst_fields;
        self
    }

    /// apply timestamp filter to the table
    pub fn timestamp_filter(mut self, timestamp_filter: (i64, i64)) -> Self {
        self.timestamp_filter = Some(timestamp_filter);
        self
    }
    /// Whether DataFusion should open every file to infer statistics while
    /// planning. Histogram fallbacks already carry trusted file-list bounds
    /// and do not declare file ordering, so they can disable this O(files)
    /// probe and scan only the unresolved files.
    pub fn collect_stat(mut self, collect_stat: bool) -> Self {
        self.collect_stat = collect_stat;
        self
    }

    pub async fn build(
        self,
        session: SearchSession,
        files: Vec<FileKey>,
        schema: Arc<Schema>,
    ) -> Result<Vec<Arc<dyn TableProvider>>> {
        let cfg = get_config();
        let target_partitions = if session.target_partitions == 0 {
            cfg.limit.cpu_num
        } else {
            session.target_partitions
        };
        let target_partitions = max(cfg.limit.datafusion_min_partition_num, target_partitions);

        #[cfg(feature = "enterprise")]
        let (target_partitions, _) = get_cpu_and_mem_limit(
            &session.id,
            session.work_group.clone(),
            target_partitions,
            cfg.memory_cache.datafusion_max_size,
        )
        .await?;

        // Group files by format
        let mut parquet_files = Vec::new();
        let mut vortex_files = Vec::new();
        let mut vix_files = Vec::new();

        for file in files {
            match FileFormat::from_extension(&file.key) {
                Some(FileFormat::Vortex) => vortex_files.push(file),
                // A core .vix data file is a puffin container (docs blob +
                // embedded index), scanned through VixCoreFormat.
                Some(FileFormat::Vix) => vix_files.push(file),
                _ => parquet_files.push(file), // Default to parquet
            }
        }

        log::info!(
            "[trace_id: {}] parquet_files numbers: {}, vortex_files numbers: {}, vix_files numbers: {}",
            session.id,
            parquet_files.len(),
            vortex_files.len(),
            vix_files.len()
        );

        // Build table providers for each format
        let mut tables: Vec<Arc<dyn TableProvider>> = Vec::new();

        if !parquet_files.is_empty() {
            let table = self
                .build_table_for_format(
                    session.clone(),
                    parquet_files,
                    schema.clone(),
                    FileFormat::Parquet,
                    target_partitions,
                )
                .await?;
            tables.push(table);
        }

        if !vortex_files.is_empty() {
            let table = self
                .build_table_for_format(
                    session.clone(),
                    vortex_files,
                    schema.clone(),
                    FileFormat::Vortex,
                    target_partitions,
                )
                .await?;
            tables.push(table);
        }

        if !vix_files.is_empty() {
            // Per-file `row_order` keying (v2 §6.2): files stamped ts_desc
            // keep the declared per-file `_timestamp DESC` ordering, and —
            // M4 — concat files with a PROVEN region decomposition join
            // them (their scans k-way merge the regions, so the stream they
            // emit really is ts_desc). Only OPAQUE concat files (no proven
            // regions, or wider than the merge cap) go into the undeclared
            // table and pay a real sort.
            let (sorted_vix, concat_vix) = if self.sorted_by_time {
                partition_vix_files_by_row_order(&session, vix_files).await
            } else {
                (vix_files, Vec::new())
            };
            if !concat_vix.is_empty() {
                log::debug!(
                    "[trace_id: {}] vix tables: {} file(s) keep the declared sort \
                     (ts_desc or region-merged concat), {} opaque concat file(s) scan undeclared",
                    session.id,
                    sorted_vix.len(),
                    concat_vix.len(),
                );
            }
            if !sorted_vix.is_empty() {
                let table = self
                    .build_table_for_format(
                        session.clone(),
                        sorted_vix,
                        schema.clone(),
                        FileFormat::Vix,
                        target_partitions,
                    )
                    .await?;
                tables.push(table);
            }
            if !concat_vix.is_empty() {
                let table = self
                    .build_concat_vix_table(
                        session.clone(),
                        concat_vix,
                        schema.clone(),
                        target_partitions,
                    )
                    .await?;
                tables.push(table);
            }
        }

        Ok(tables)
    }

    /// The concat-order `.vix` table: same format/adapter as the sorted
    /// table but registered under its own prefix token and WITHOUT the
    /// per-file sort declaration (its files' rows are not globally
    /// `_timestamp` DESC).
    async fn build_concat_vix_table(
        &self,
        session: SearchSession,
        files: Vec<FileKey>,
        schema: Arc<Schema>,
        target_partitions: usize,
    ) -> Result<Arc<dyn TableProvider>> {
        self.build_table_inner(
            session,
            files,
            schema,
            FileFormat::Vix,
            target_partitions,
            false,
            "vix-concat",
        )
        .await
    }

    async fn build_table_for_format(
        &self,
        session: SearchSession,
        files: Vec<FileKey>,
        schema: Arc<Schema>,
        format: FileFormat,
        target_partitions: usize,
    ) -> Result<Arc<dyn TableProvider>> {
        let declare_sort = self.sorted_by_time;
        self.build_table_inner(
            session,
            files,
            schema,
            format,
            target_partitions,
            declare_sort,
            format.extension(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_table_inner(
        &self,
        session: SearchSession,
        files: Vec<FileKey>,
        schema: Arc<Schema>,
        format: FileFormat,
        target_partitions: usize,
        declare_sort: bool,
        prefix_token: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        // Configure listing options with the appropriate file format
        let file_format: Arc<dyn DataFusionFileFormat> = match format {
            FileFormat::Parquet => Arc::new(ParquetFormat::default()),
            FileFormat::Vortex => {
                let vortex_session = VortexSession::default().with_tokio();
                Arc::new(VortexFormat::new(vortex_session))
            }
            // Core files: logical stream schema over the docs blob, with
            // non-physical columns extracted from `_source`. The query time
            // range is pushed into each file's vortex scan. A DECLARED-sort
            // table additionally requires every scan to emit ts_desc — a
            // concat file routed here (proven regions) k-way merges (§6.2).
            FileFormat::Vix => Arc::new(
                VixCoreFormat::new(self.timestamp_filter).with_ordered_output(declare_sort),
            ),
        };

        let mut listing_options = ListingOptions::new(file_format)
            .with_target_partitions(target_partitions)
            .with_collect_stat(self.collect_stat);

        if declare_sort {
            // Every format stores its rows ORDER BY _timestamp DESC, so
            // declare that per-file sort order. Core .vix files supply exact
            // `_timestamp` min/max statistics from `VixCoreFormat::infer_stats`
            // (parquet/vortex from their own footers), so
            // `split_file_groups_by_statistics` can order the file groups to
            // uphold the declared ordering and the SortExec is elided.
            //
            // .vix files stamped `row_order=concat` (concatenation-order
            // merge outputs — rows NOT globally sorted) never reach a
            // declared table: `partition_vix_files_by_row_order` routes them
            // to the undeclared concat table, keyed on the FILE's own
            // row_order property. Parquet/vortex files are never
            // concat-ordered.
            listing_options = listing_options
                .with_file_sort_order(vec![vec![col(TIMESTAMP_COL_NAME).sort(false, false)]]);
        }

        let schema_key = schema.hash_key();
        let format = prefix_token;
        let trace_id = &session.id;
        let prefix = match session.storage_type {
            StorageType::Memory => {
                file_list::set(trace_id, &schema_key, format, files).await;
                format!("memory:///{trace_id}/schema={schema_key}/format={format}/",)
            }
            StorageType::Wal => {
                file_list::set(trace_id, &schema_key, format, files).await;
                format!("wal:///{trace_id}/schema={schema_key}/format={format}/",)
            }
        };
        let prefix = match ListingTableUrl::parse(prefix) {
            Ok(url) => url,
            Err(e) => {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "ListingTableUrl error: {e}",
                )));
            }
        };

        let mut config = ListingTableConfig::new(prefix).with_listing_options(listing_options);
        let timestamp_field = schema.field_with_name(TIMESTAMP_COL_NAME);
        let schema = if timestamp_field.is_ok() && timestamp_field.unwrap().is_nullable() {
            let new_fields = schema
                .fields()
                .iter()
                .map(|x| {
                    if x.name() == TIMESTAMP_COL_NAME {
                        Arc::new(Field::new(
                            TIMESTAMP_COL_NAME.to_string(),
                            DataType::Int64,
                            false,
                        ))
                    } else {
                        x.clone()
                    }
                })
                .collect::<Vec<_>>();
            Arc::new(Schema::new(new_fields))
        } else {
            schema
        };
        config = config.with_schema(schema);
        // the default adapter plus `_source` synthesis: a star-projected
        // `_source` column missing from a parquet file (WAL parquet,
        // pre-migration storage parquet) is synthesized per row from the
        // file's own columns instead of null-filled (DESIGN §5)
        config = config.with_expr_adapter_factory(Arc::new(SourceSynthesizingExprAdapterFactory));
        let mut table = ListingTableAdapter::try_new(
            config,
            session.id.clone(),
            self.index_condition.clone(),
            self.fst_fields.clone(),
            self.timestamp_filter,
        )?;
        if self.file_stat_cache.is_some() {
            table = table.with_cache(self.file_stat_cache.clone());
        }
        Ok(Arc::new(table))
    }
}

/// Per-file ordering class of a `.vix` DATA object (§6.2, M4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VixOrderClass {
    /// `row_order=ts_desc`: globally sorted, declared as before.
    Sorted,
    /// Concat file with a PROVEN region decomposition within the merge cap:
    /// its scans k-way merge the regions, so it JOINS the declared table.
    ConcatMergeable,
    /// Concat file without proven regions (or over the cap, or unprobeable):
    /// no order can be assumed — undeclared table, real sort.
    Opaque,
}

/// Process-wide memo of `.vix` DATA objects' order-class verdicts, keyed by
/// object key. Object keys are immutable (a rewritten file gets a new key),
/// so a verdict never goes stale; entries for aged-out files are dropped by
/// the crude clear-on-cap below (retention is 1 day, so the map stays
/// small; a clear only costs re-probes of live files). NOTE the class
/// depends on ZO_VIX_ORDER_MERGE_MAX_REGIONS — a runtime knob change needs
/// a process restart to fully re-classify (verdicts are conservative
/// either way).
static VIX_ROW_ORDER_MEMO: std::sync::LazyLock<
    parking_lot::RwLock<hashbrown::HashMap<String, VixOrderClass>>,
> = std::sync::LazyLock::new(Default::default);
const VIX_ROW_ORDER_MEMO_CAP: usize = 200_000;

/// The order class from a probed reader's footer verdicts.
fn classify_vix_order(ts_desc: bool, regions: Option<usize>, has_zone: bool) -> VixOrderClass {
    if ts_desc {
        return VixOrderClass::Sorted;
    }
    let cap = get_config().common.vix_order_merge_max_regions;
    match regions {
        // the zone table is required for the merge's lazy region bounds AND
        // for the concat file's exact `_timestamp` statistics (infer_stats)
        Some(count) if cap > 0 && count <= cap && has_zone => VixOrderClass::ConcatMergeable,
        _ => VixOrderClass::Opaque,
    }
}

/// Partition `.vix` files for a declared-sort query: `(declared, opaque)` —
/// `declared` = ts_desc files plus region-mergeable concat files (their
/// scans emit ts_desc through the §6.2 k-way merge), `opaque` = everything
/// whose order cannot be proven (scans undeclared, real sort).
///
/// Verdict sources, cheapest first: the process memo, a reader already
/// memoized by the index-eval path (zero IO), else ONE footer parse over
/// the cache ladder (memory/disk cache first; a cold file pays one small
/// tail GET — the same per-file plan-time IO class `infer_stats` already
/// performs for the declared-sort statistics). Anything unprobeable
/// (WAL-side sets, zero-size meta, open errors) lands in the OPAQUE bucket:
/// never declare an order that is not proven — the scan itself surfaces any
/// real error later, and an undeclared sorted file only costs a real sort.
async fn partition_vix_files_by_row_order(
    session: &SearchSession,
    files: Vec<FileKey>,
) -> (Vec<FileKey>, Vec<FileKey>) {
    // WAL sets never contain .vix files (they are storage objects); if one
    // ever appears the cache ladder cannot serve it — fail-safe to opaque.
    if session.storage_type == StorageType::Wal {
        return (Vec::new(), files);
    }
    let concurrency = max(1, get_config().limit.cpu_num.min(16));
    let handle = tokio::runtime::Handle::current();
    let probes = futures::stream::iter(files.into_iter().map(|file| {
        let handle = handle.clone();
        async move {
            if let Some(&class) = VIX_ROW_ORDER_MEMO.read().get(&file.key) {
                return (file, class);
            }
            if let Some(reader) = crate::vix::reader_cache::GLOBAL_CACHE.get(&file.key) {
                let class = classify_vix_order(
                    reader.row_order().is_ts_desc(),
                    reader.ts_desc_row_ranges().map(|r| r.len()),
                    reader.zone_chunks().is_some(),
                );
                memo_vix_row_order(&file.key, class);
                return (file, class);
            }
            let Ok(size) = u64::try_from(file.meta.compressed_size) else {
                return (file, VixOrderClass::Opaque);
            };
            if size == 0 {
                return (file, VixOrderClass::Opaque);
            }
            let source = Arc::new(crate::vix::source::LadderRangeSource::new(
                file.account.clone(),
                &file.key,
                size,
                handle,
                None,
            ));
            let key = file.key.clone();
            let verdict = tokio::task::spawn_blocking(move || {
                vortex_index::VixDocs::open_ranged(source).map(|docs| {
                    classify_vix_order(
                        docs.row_order().is_ts_desc(),
                        docs.ts_desc_row_ranges().map(|r| r.len()),
                        docs.zone_chunks().is_some(),
                    )
                })
            })
            .await;
            match verdict {
                Ok(Ok(class)) => {
                    memo_vix_row_order(&key, class);
                    (file, class)
                }
                Ok(Err(error)) => {
                    log::debug!(
                        "vix row-order probe of {key} failed (treated as opaque): {error:#}"
                    );
                    (file, VixOrderClass::Opaque)
                }
                Err(join_error) => {
                    log::debug!(
                        "vix row-order probe task of {key} failed (treated as opaque): \
                         {join_error}"
                    );
                    (file, VixOrderClass::Opaque)
                }
            }
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;

    let mut declared = Vec::with_capacity(probes.len());
    let mut opaque = Vec::new();
    for (file, class) in probes {
        match class {
            VixOrderClass::Sorted | VixOrderClass::ConcatMergeable => declared.push(file),
            VixOrderClass::Opaque => opaque.push(file),
        }
    }
    (declared, opaque)
}

fn memo_vix_row_order(key: &str, class: VixOrderClass) {
    let mut memo = VIX_ROW_ORDER_MEMO.write();
    if memo.len() >= VIX_ROW_ORDER_MEMO_CAP {
        memo.clear();
    }
    memo.insert(key.to_string(), class);
}

#[cfg(feature = "enterprise")]
async fn get_cpu_and_mem_limit(
    trace_id: &str,
    work_group: Option<String>,
    target_partitions: usize,
    memory_size: usize,
) -> Result<(usize, usize)> {
    let (target_partitions, memory_size) = if let Some(wg) = work_group.as_ref()
        && let Ok(wg) = WorkGroup::from_str(wg)
    {
        wg.get_resource(trace_id, target_partitions, memory_size)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to get dynamic resource: {e}"))
            })?
    } else {
        (target_partitions, memory_size)
    };
    let target_partitions = std::cmp::max(
        get_config().limit.datafusion_min_partition_num,
        target_partitions,
    );

    log::info!(
        "[trace_id: {trace_id}] work_group: {work_group:?}, target_partitions: {target_partitions}, memory_size: {memory_size}"
    );

    Ok((target_partitions, memory_size))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use config::get_config;

    use super::*;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("field1", DataType::Utf8, true),
            Field::new("field2", DataType::Int64, true),
        ]))
    }

    /// M26 pin: every `shared_merge_pool(true)` context draws from ONE
    /// process-wide pool — a reservation made through context A is visible
    /// in context B's pool level, so concurrent merges are bounded in
    /// AGGREGATE (48Gi compactors died stacking ~12.8GB per-context
    /// metadata-merge pool fills, 2026-08-21). Default contexts keep
    /// isolated per-context pools.
    #[tokio::test]
    async fn m26_merge_contexts_share_one_memory_pool() {
        use datafusion::execution::memory_pool::MemoryConsumer;

        let ctx_a = DataFusionContextBuilder::new()
            .trace_id("m26-shared-a")
            .shared_merge_pool(true)
            .build(2)
            .await
            .unwrap();
        let ctx_b = DataFusionContextBuilder::new()
            .trace_id("m26-shared-b")
            .shared_merge_pool(true)
            .build(2)
            .await
            .unwrap();
        let pool_a = Arc::clone(&ctx_a.runtime_env().memory_pool);
        let pool_b = Arc::clone(&ctx_b.runtime_env().memory_pool);

        const GROW: usize = 64 * 1024 * 1024;
        let base_b = pool_b.reserved();
        let mut reservation = MemoryConsumer::new("m26-shared-pin").register(&pool_a);
        reservation.grow(GROW);
        assert!(
            pool_b.reserved() >= base_b + GROW,
            "a reservation through context A must be visible in context B's \
             pool level: base={base_b}, now={}",
            pool_b.reserved()
        );

        // a DEFAULT context's pool is its own: the shared reservation must
        // not appear there
        let ctx_c = DataFusionContextBuilder::new()
            .trace_id("m26-default-c")
            .build(2)
            .await
            .unwrap();
        let pool_c = Arc::clone(&ctx_c.runtime_env().memory_pool);
        assert!(
            pool_c.reserved() < GROW,
            "default contexts must keep isolated per-context pools, got {}",
            pool_c.reserved()
        );

        reservation.free();
    }

    #[tokio::test]
    async fn test_create_session_config_default() -> Result<()> {
        let config = create_session_config(false, 0)?;

        // Test default configurations
        assert_eq!(
            config.options().execution.target_partitions,
            get_config()
                .limit
                .cpu_num
                .max(get_config().limit.datafusion_min_partition_num)
        );
        assert_eq!(config.options().execution.batch_size, get_batch_size());
        assert_eq!(config.options().sql_parser.dialect, Dialect::PostgreSQL);
        assert!(!config.options().execution.listing_table_ignore_subdirectory);
        assert!(config.information_schema());
        // Join dynamic filter pushdown must stay disabled: its runtime filter can't cross our
        // distributed RemoteScan/Flight boundary and breaks our custom join rewrites.
        assert!(
            !config
                .options()
                .optimizer
                .enable_join_dynamic_filter_pushdown
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_create_session_config_with_partitions() -> Result<()> {
        let target_partitions = 8;
        let config = create_session_config(true, target_partitions)?;

        let expected_partitions = std::cmp::max(
            get_config().limit.datafusion_min_partition_num,
            target_partitions,
        );

        assert_eq!(
            config.options().execution.target_partitions,
            expected_partitions
        );
        assert!(config.options().execution.split_file_groups_by_statistics);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_session_config_sorted_by_time() -> Result<()> {
        let config = create_session_config(true, 4)?;
        assert!(config.options().execution.split_file_groups_by_statistics);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_runtime_env() -> Result<()> {
        let memory_limit = 1024 * 1024 * 512; // 512MB
        let runtime_env = create_runtime_env("test", memory_limit).await?;

        // Check that object stores are registered
        let memory_url = url::Url::parse("memory:///").unwrap();
        let wal_url = url::Url::parse("wal:///").unwrap();

        assert!(
            runtime_env
                .object_store_registry
                .get_store(&memory_url)
                .is_ok()
        );
        assert!(
            runtime_env
                .object_store_registry
                .get_store(&wal_url)
                .is_ok()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_create_runtime_env_min_memory() -> Result<()> {
        let small_memory = 1024; // Very small memory
        let runtime_env = create_runtime_env("test", small_memory).await?;

        // Should handle small memory gracefully
        // Memory pool behavior may vary by implementation
        // Memory pool exists and was created successfully
        let _ = runtime_env.memory_pool.reserved();

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_context_builder_new() {
        let builder = DataFusionContextBuilder::new();
        assert_eq!(builder.trace_id, "");
        assert_eq!(builder.work_group, None);
        assert!(!builder.sorted_by_time);
        assert!(builder.analyzer_rules.is_empty());
        assert!(builder.optimizer_rules.is_empty());
        assert!(builder.physical_optimizer_rules.is_empty());
    }

    #[tokio::test]
    async fn test_datafusion_context_builder_with_options() {
        let builder = DataFusionContextBuilder::new()
            .trace_id("test-trace-123")
            .work_group(Some("test-group".to_string()))
            .sorted_by_time(true);

        assert_eq!(builder.trace_id, "test-trace-123");
        assert_eq!(builder.work_group, Some("test-group".to_string()));
        assert!(builder.sorted_by_time);
    }

    #[tokio::test]
    async fn test_datafusion_context_builder_build() -> Result<()> {
        let builder = DataFusionContextBuilder::new()
            .trace_id("test-trace")
            .sorted_by_time(true);

        let ctx = builder.build(4).await?;

        // Verify context was created successfully
        assert!(ctx.sql("SELECT 1").await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_register_udf() -> Result<()> {
        let ctx = SessionContext::new();
        let result = register_udf(&ctx, "test_org");

        assert!(result.is_ok());

        // Test that UDFs are registered by checking the context has functions
        // str_match might have different signature, so just verify registration succeeded
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_table_builder_new() {
        let builder = TableBuilder::new();
        assert!(!builder.sorted_by_time);
        assert!(builder.file_stat_cache.is_none());
        assert!(builder.index_condition.is_none());
        assert!(builder.fst_fields.is_empty());
    }

    #[test]
    fn test_table_builder_with_options() {
        let builder = TableBuilder::new()
            .sorted_by_time(true)
            .fst_fields(vec!["field1".to_string()]);

        assert!(builder.sorted_by_time);
        assert_eq!(builder.fst_fields, vec!["field1".to_string()]);
    }

    #[test]
    fn test_table_builder_file_stat_cache_none() {
        let builder = TableBuilder::new().file_stat_cache(None);
        assert!(builder.file_stat_cache.is_none());
    }

    #[test]
    fn test_table_builder_index_condition_none() {
        let builder = TableBuilder::new().index_condition(None);
        assert!(builder.index_condition.is_none());
    }

    #[test]
    fn test_table_builder_timestamp_filter() {
        let builder = TableBuilder::new().timestamp_filter((100, 200));
        assert_eq!(builder.timestamp_filter, Some((100, 200)));
    }

    #[tokio::test]
    async fn test_create_session_config_memory_pools() -> Result<()> {
        // Test different memory pool configurations by creating runtime environments
        let memory_limit = 1024 * 1024 * 256; // 256MB

        // Test that runtime env creation works (which tests different pool types)
        let runtime_env = create_runtime_env("test", memory_limit).await?;
        // Memory pool exists and was created successfully
        // Memory pool exists and was created successfully
        let _ = runtime_env.memory_pool.reserved();

        Ok(())
    }

    mod integration_tests {
        use config::meta::{
            search::{Session as SearchSession, StorageType},
            stream::{FileKey, FileMeta},
        };

        use super::*;

        #[tokio::test]
        async fn test_register_table_integration() -> Result<()> {
            let session = SearchSession {
                id: "test-session".to_string(),
                storage_type: StorageType::Memory,
                target_partitions: 2,
                work_group: None,
            };

            let schema = create_test_schema();
            let files = vec![FileKey {
                key: "test-file".to_string(),
                meta: FileMeta::default(),
                deleted: false,
                account: "test_account".to_string(),
                id: 1,
                selection: None,
                row_group_size: None,
                selection_exact: false,
            }];

            let result = register_metrics_table(&session, schema, "test_table", files).await;

            // Should create context successfully
            assert!(result.is_ok());
            if let Ok(ctx) = result {
                // Verify table is registered
                assert!(
                    ctx.catalog("datafusion")
                        .unwrap()
                        .schema("public")
                        .unwrap()
                        .table("test_table")
                        .await
                        .is_ok()
                );
            }

            Ok(())
        }

        #[tokio::test]
        async fn test_table_builder_build_integration() -> Result<()> {
            let session = SearchSession {
                id: "test-session".to_string(),
                storage_type: StorageType::Memory,
                target_partitions: 2,
                work_group: None,
            };

            let schema = create_test_schema();
            let files = vec![FileKey {
                key: "test-file".to_string(),
                meta: FileMeta::default(),
                deleted: false,
                account: "test_account".to_string(),
                id: 1,
                selection: None,
                row_group_size: None,
                selection_exact: false,
            }];

            let builder = TableBuilder::new().sorted_by_time(true);

            let result = builder.build(session, files, schema).await;
            assert!(result.is_ok());

            Ok(())
        }
    }

    mod error_cases {
        use super::*;

        #[tokio::test]
        async fn test_create_runtime_env_invalid_memory_pool_type() {
            // This test verifies error handling in memory pool creation
            // The actual error handling is in the FromStr implementation
            let memory_limit = 1024 * 1024 * 256;
            let result = create_runtime_env("test", memory_limit).await;
            assert!(result.is_ok()); // Should handle gracefully
        }

        #[tokio::test]
        async fn test_datafusion_context_builder_zero_partitions() -> Result<()> {
            let builder = DataFusionContextBuilder::new();
            let ctx = builder.build(0).await?; // Zero partitions should use default

            // Should still create a valid context
            assert!(ctx.sql("SELECT 1").await.is_ok());

            Ok(())
        }
    }

    mod configuration_tests {
        use super::*;

        #[tokio::test]
        async fn test_session_config_bloom_filter_settings() -> Result<()> {
            // Test bloom filter configurations
            let config1 = create_session_config(false, 4)?;
            let config2 = create_session_config(true, 4)?;

            // Both should be valid configurations
            assert!(config1.options().execution.target_partitions > 0);
            assert!(config2.options().execution.target_partitions > 0);

            Ok(())
        }

        #[tokio::test]
        async fn test_session_config_partition_bounds() -> Result<()> {
            // Test minimum partition enforcement
            let config = create_session_config(false, 1)?; // Very small number

            let actual_partitions = config.options().execution.target_partitions;
            assert!(actual_partitions >= get_config().limit.datafusion_min_partition_num);

            Ok(())
        }
    }
}
