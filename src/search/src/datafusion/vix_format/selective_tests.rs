// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::atomic::AtomicBool;

use datafusion::{
    datasource::{
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
    },
    execution::{runtime_env::RuntimeEnvBuilder, session_state::SessionStateBuilder},
    physical_plan::{collect, filter::FilterExec},
    prelude::{SessionConfig, SessionContext},
};
use futures::{TryStreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetResult, ListResult, MultipartUpload, ObjectStoreExt, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use vortex_index::{VixWriter, VixWriterOptions};

use super::*;

#[derive(Debug, Default)]
struct MeasuredStore {
    inner: InMemory,
    bytes: Arc<AtomicUsize>,
    full_reads: AtomicUsize,
    fail: AtomicBool,
}
impl fmt::Display for MeasuredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MeasuredStore")
    }
}
#[async_trait]
impl ObjectStore for MeasuredStore {
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if self.fail.load(Ordering::Relaxed) && !options.head {
            return Err(object_store::Error::Generic {
                store: "MeasuredStore",
                source: "injected range failure".into(),
            });
        }
        let head = options.head;
        let full = options.range.is_none() && !head;
        let mut result = self.inner.get_opts(path, options).await?;
        if !head {
            let object_store::GetResultPayload::Stream(stream) = result.payload else {
                unreachable!("InMemory always returns a stream")
            };
            let bytes = Arc::clone(&self.bytes);
            result.payload = object_store::GetResultPayload::Stream(
                stream
                    .inspect_ok(move |chunk| {
                        bytes.fetch_add(chunk.len(), Ordering::Relaxed);
                    })
                    .boxed(),
            );
            self.full_reads
                .fetch_add(usize::from(full), Ordering::Relaxed);
        }
        Ok(result)
    }
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn fixture(complete: bool) -> (bytes::Bytes, SchemaRef) {
    fixture_order(complete, false)
}

fn fixture_order(complete: bool, concat: bool) -> (bytes::Bytes, SchemaRef) {
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
    ]));
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            columns_complete: complete,
            docs_chunk_bytes: 64 * 1024,
            row_group_size: 500,
            concat_row_order: concat,
            ..Default::default()
        },
        false,
    );
    let mut state = 42u64;
    for base in (0..10_000).step_by(500) {
        let mut timestamps = Vec::new();
        let mut bodies = Vec::new();
        let mut messages = Vec::new();
        let mut sources = Vec::new();
        for row in base..base + 500 {
            let body = match row {
                0 => Some("aaa"),
                123 => Some("ENOSPC needle"),
                9999 => Some("zzz"),
                n if n % 1000 == 123 => Some("ENOSPC needle"),
                n if n % 997 == 0 => None,
                _ => Some("ordinary"),
            };
            let message = (row == 321).then_some("ENOSPC second");
            let padding: String = (0..512)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (b'a' + (state % 26) as u8) as char
                })
                .collect();
            let timestamp = if concat {
                10_000 - ((row + 5000) % 10_000)
            } else {
                10_000 - row
            };
            timestamps.push(timestamp);
            bodies.push(body);
            messages.push(message);
            sources.push(serde_json::json!({"_timestamp": timestamp, "body": body,
                "message": message, "padding": padding, "ghost": if complete { None } else { Some("hidden") }}).to_string());
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(StringArray::from(bodies)),
                Arc::new(StringArray::from(messages)),
            ],
        )
        .unwrap();
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
    }
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(SOURCE_COL_NAME, DataType::Utf8, true)));
    fields.push(Arc::new(Field::new("ghost", DataType::Utf8, true)));
    (
        bytes::Bytes::from(writer.finish().unwrap().0),
        Arc::new(Schema::new(fields)),
    )
}

fn equal_body(needle: &str) -> ColumnBound {
    ColumnBound {
        column: "body".into(),
        min: Some((BoundValue::Str(needle.into()), true)),
        max: Some((BoundValue::Str(needle.into()), true)),
    }
}

async fn scan_equality(
    data: bytes::Bytes,
    schema: SchemaRef,
    needle: &str,
    enabled: bool,
) -> (Vec<RecordBatch>, usize, usize) {
    let store = Arc::new(MeasuredStore::default());
    let path = Path::from("data/selective.vix");
    store.put(&path, data.into()).await.unwrap();
    let meta = store.head(&path).await.unwrap();
    let opener = VixCoreOpener {
        column_bounds: vec![equal_body(needle)],
        null_rejected_columns: vec![],
        late_source_predicate: None,
        selective_source_range_prepass: enabled,
        emit_ts_desc: false,
        batch_size: 1024,
        object_store: store.clone(),
        projected_schema: schema,
        timestamp_filter: None,
        memory_pool: None,
        conversion_width: None,
        metrics: ExecutionPlanMetricsSet::new(),
        partition: 0,
    };
    let mut file = PartitionedFile::new(path.to_string(), meta.size);
    file.object_meta = meta;
    let batches: Vec<RecordBatch> = opener
        .open(file)
        .unwrap()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    // The unchanged FilterExec must still run when the native prepass is broad.
    let batches = batches
        .into_iter()
        .map(|batch| {
            let body = batch
                .column_by_name("body")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let mask =
                arrow::array::BooleanArray::from_iter(body.iter().map(|v| v.map(|v| v == needle)));
            arrow::compute::filter_record_batch(&batch, &mask).unwrap()
        })
        .collect();
    (
        batches,
        store.bytes.load(Ordering::Relaxed),
        store.full_reads.load(Ordering::Relaxed),
    )
}

fn rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|column| {
                        arrow::util::display::array_value_to_string(
                            batch.column(column).as_ref(),
                            row,
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>()
                    .join(" | "),
            );
        }
    }
    rows
}

#[tokio::test(flavor = "multi_thread")]
async fn selective_source_range_zero_sparse_and_broad_match_legacy() {
    let (data, schema) = fixture(true);
    for needle in ["missing", "aaa", "ENOSPC needle", "ordinary"] {
        let (old, old_bytes, old_full) =
            scan_equality(data.clone(), schema.clone(), needle, false).await;
        let (new, new_bytes, new_full) =
            scan_equality(data.clone(), schema.clone(), needle, true).await;
        assert_eq!(rows(&old), rows(&new), "needle={needle}");
        assert_eq!(old_full, 1);
        assert_eq!(new_full, 0);
        if needle == "missing" {
            assert!(rows(&new).is_empty());
            assert!(
                new_bytes * 4 < old_bytes,
                "old={old_bytes}, new={new_bytes}"
            );
        }
        println!(
            "selective-source needle={needle:?} old_bytes={old_bytes} ranged_bytes={new_bytes}"
        );
    }
}

async fn context(data: bytes::Bytes, schema: SchemaRef) -> (SessionContext, Arc<MeasuredStore>) {
    context_order(data, schema, false).await
}

async fn context_order(
    data: bytes::Bytes,
    schema: SchemaRef,
    ordered: bool,
) -> (SessionContext, Arc<MeasuredStore>) {
    let store = Arc::new(MeasuredStore::default());
    store
        .put(&Path::from("data/test.vix"), data.into())
        .await
        .unwrap();
    let registry = DefaultObjectStoreRegistry::new();
    registry.register_store(&url::Url::parse("test:///").unwrap(), store.clone());
    let runtime = RuntimeEnvBuilder::new()
        .with_object_store_registry(Arc::new(registry))
        .build()
        .unwrap();
    let state = SessionStateBuilder::new()
        .with_config(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_collect_statistics(false),
        )
        .with_runtime_env(Arc::new(runtime))
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    let mut options = ListingOptions::new(Arc::new(
        VixCoreFormat::new(None).with_ordered_output(ordered),
    ));
    if ordered {
        options = options.with_file_sort_order(vec![vec![
            datafusion::prelude::col(TIMESTAMP_COL_NAME).sort(false, false),
        ]]);
    }
    let table = ListingTableConfig::new(ListingTableUrl::parse("test:///data/").unwrap())
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(table).unwrap()))
        .unwrap();
    (ctx, store)
}

async fn run(ctx: &SessionContext, sql: &str, enabled: bool) -> Vec<RecordBatch> {
    run_switches(ctx, sql, enabled, enabled).await
}

async fn run_switches(
    ctx: &SessionContext,
    sql: &str,
    ranged: bool,
    late: bool,
) -> Vec<RecordBatch> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let plan = inject_vix_scan_pruning_with_late_source(plan, late).unwrap();
    let plan = plan
        .transform_down(|node| {
            if let Some(exec) = node.downcast_ref::<DataSourceExec>()
                && let Some(conf) = exec.data_source().downcast_ref::<FileScanConfig>()
            {
                let source: &dyn std::any::Any = conf.file_source.as_ref();
                if let Some(source) = source.downcast_ref::<VixCoreSource>() {
                    let mut source = source.clone();
                    source.selective_source_range_prepass = ranged;
                    let mut conf = conf.clone();
                    conf.file_source = Arc::new(source);
                    return Ok(Transformed::yes(DataSourceExec::from_data_source(conf)));
                }
            }
            Ok(Transformed::no(node))
        })
        .unwrap()
        .data;
    collect(plan, ctx.task_ctx()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn late_source_sql_null_missing_in_order_limit_and_fulltext() {
    let (data, schema) = fixture(true);
    let (ctx, store) = context(data, schema).await;
    for condition in [
        "body = 'ENOSPC needle'",
        "body = 'ordinary'",
        "body LIKE '%missing%'",
        "body LIKE '%ENOSPC%' OR message LIKE '%ENOSPC%'",
        "body IN ('ENOSPC needle', 'aaa')",
        "body IS NULL",
        "ghost IS NULL",
        "ghost = 'hidden'",
        "body LIKE '%ordinary%'",
        "body NOT IN ('ordinary', NULL)",
        "body IS NULL OR message LIKE '%second%'",
    ] {
        for order in ["ASC", "DESC"] {
            let sql = format!(
                "SELECT _timestamp, _source FROM t WHERE {condition} ORDER BY _timestamp {order} LIMIT 7 OFFSET 1"
            );
            let old = run(&ctx, &sql, false).await;
            for (ranged, late) in [(true, false), (false, true), (true, true)] {
                let new = run_switches(&ctx, &sql, ranged, late).await;
                assert_eq!(
                    rows(&old),
                    rows(&new),
                    "ranged={ranged}, late={late}: {sql}"
                );
            }
        }
    }
    let sql = "SELECT _timestamp, _source FROM t WHERE body LIKE '%missing%'";
    store.bytes.store(0, Ordering::Relaxed);
    let old = run(&ctx, sql, false).await;
    let old_bytes = store.bytes.swap(0, Ordering::Relaxed);
    let new = run(&ctx, sql, true).await;
    let new_bytes = store.bytes.load(Ordering::Relaxed);
    assert_eq!(rows(&old), rows(&new));
    assert!(
        new_bytes * 4 < old_bytes,
        "old={old_bytes}, new={new_bytes}"
    );
    println!("late-source zero-match old_bytes={old_bytes} new_bytes={new_bytes}");
}

#[tokio::test(flavor = "multi_thread")]
async fn late_source_match_all_reconstruction_and_legacy_missing_column() {
    let (data, schema) = fixture(false);
    let schema = fulltext_schema(&schema);
    let (ctx, _) = context(data, schema.clone()).await;
    let sql =
        "SELECT _timestamp, _source FROM t WHERE ghost = 'hidden' ORDER BY _timestamp DESC LIMIT 3";
    assert_eq!(
        rows(&run(&ctx, sql, false).await),
        rows(&run(&ctx, sql, true).await)
    );
    let plan = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let expr = crate::index::Condition::MatchAll("ENOSPC".into())
        .to_physical_expr(&plan.schema(), &["body".into(), "message".into()])
        .unwrap();
    let plan: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(expr, plan).unwrap());
    let old = collect(
        inject_vix_scan_pruning_with_late_source(plan.clone(), false).unwrap(),
        ctx.task_ctx(),
    )
    .await
    .unwrap();
    let new = collect(
        inject_vix_scan_pruning_with_late_source(plan, true).unwrap(),
        ctx.task_ctx(),
    )
    .await
    .unwrap();
    assert_eq!(rows(&old), rows(&new));
    assert_eq!(rows(&new).len(), 11);
}

#[tokio::test(flavor = "multi_thread")]
async fn late_source_read_error_is_not_an_empty_result() {
    let (data, schema) = fixture(true);
    let (ctx, store) = context(data, schema).await;
    let plan = ctx
        .sql("SELECT _source FROM t WHERE body LIKE '%missing%'")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let plan = inject_vix_scan_pruning_with_late_source(plan, true).unwrap();
    store.fail.store(true, Ordering::Relaxed);
    let error = collect(plan, ctx.task_ctx()).await.unwrap_err();
    assert!(
        error.to_string().contains("injected range failure"),
        "{error}"
    );
}

#[test]
fn late_source_cancellation_is_propagated() {
    struct Cancelled;
    impl vortex_index::VixReadOperation for Cancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }
    let (data, schema) = fixture(true);
    let docs = VixDocs::open(data).unwrap();
    let expr: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_plan::expressions::IsNullExpr::new(Arc::new(Column::new("body", 1))),
    );
    let predicate = LateSourcePredicate::try_new(&expr, &schema)
        .unwrap()
        .unwrap();
    let error = vortex_index::with_read_operation(Arc::new(Cancelled), || predicate.select(&docs))
        .unwrap_err();
    assert!(error.chain().any(|cause| matches!(
        cause.downcast_ref::<vortex_index::VixError>(),
        Some(vortex_index::VixError::Cancelled)
    )));
}

#[tokio::test(flavor = "multi_thread")]
async fn late_source_dropped_consumer_releases_reservation() {
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    let (data, schema) = fixture(true);
    let store = Arc::new(MeasuredStore::default());
    let path = Path::from("data/cancel.vix");
    store.put(&path, data.into()).await.unwrap();
    let meta = store.head(&path).await.unwrap();
    let expr: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_plan::expressions::IsNullExpr::new(Arc::new(Column::new("body", 1))),
    );
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 * 1024 * 1024));
    let opener = VixCoreOpener {
        column_bounds: vec![],
        null_rejected_columns: vec![],
        late_source_predicate: LateSourcePredicate::try_new(&expr, &schema).unwrap(),
        selective_source_range_prepass: false,
        emit_ts_desc: false,
        batch_size: 1,
        object_store: store,
        projected_schema: schema,
        timestamp_filter: None,
        memory_pool: Some(pool.clone()),
        conversion_width: None,
        metrics: ExecutionPlanMetricsSet::new(),
        partition: 0,
    };
    let mut file = PartitionedFile::new(path.to_string(), meta.size);
    file.object_meta = meta;
    let mut stream = opener.open(file).unwrap().await.unwrap();
    assert!(stream.try_next().await.unwrap().is_some());
    assert!(pool.reserved() >= late_source::SELECTION_RESERVATION);
    drop(stream);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while pool.reserved() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled prepass must release its pool reservation");
}

#[test]
fn late_source_budget_and_source_dependency_fall_back() {
    let (data, schema) = fixture(true);
    let schema = fulltext_schema(&schema);
    let docs = VixDocs::open(data).unwrap();
    let expr = crate::index::Condition::MatchAll("ordinary".into())
        .to_physical_expr(&schema, &["body".into()])
        .unwrap();
    let predicate = LateSourcePredicate::try_new(&expr, &schema)
        .unwrap()
        .unwrap();
    assert!(
        predicate.select(&docs).unwrap().is_none(),
        "broad matches must discard the partial selection"
    );
    let source_expr: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_plan::expressions::IsNullExpr::new(Arc::new(Column::new(
            SOURCE_COL_NAME,
            schema.index_of(SOURCE_COL_NAME).unwrap(),
        ))),
    );
    assert!(
        LateSourcePredicate::try_new(&source_expr, &schema)
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn late_source_concat_merge_matches_full_sort_and_avoids_source_reads() {
    let (data, schema) = fixture_order(true, true);
    let docs = VixDocs::open(data.clone()).unwrap();
    assert!(!docs.row_order().is_ts_desc());
    let (baseline, _) = context(data.clone(), schema.clone()).await;
    let (ordered, store) = context_order(data, schema, true).await;
    for condition in [
        "body = 'ENOSPC needle'",
        "body LIKE '%ENOSPC%'",
        "body LIKE '%ordinary%'",
    ] {
        for suffix in [
            "ORDER BY _timestamp DESC",
            "ORDER BY _timestamp DESC LIMIT 7 OFFSET 2",
            "ORDER BY _timestamp ASC LIMIT 3 OFFSET 1",
        ] {
            let sql = format!("SELECT _timestamp, _source FROM t WHERE {condition} {suffix}");
            let expected = rows(&run(&baseline, &sql, false).await);
            assert!(!expected.is_empty(), "fixture must exercise {sql}");
            for enabled in [false, true] {
                assert_eq!(
                    expected,
                    rows(&run(&ordered, &sql, enabled).await),
                    "enabled={enabled}: {sql}"
                );
            }
        }
    }
    // Equality normally skips the generic prepass. The concat merge does not
    // run the native equality pass, so it must retain the generic one.
    let sql = "SELECT _timestamp, _source FROM t WHERE body = 'missing' ORDER BY _timestamp DESC";
    store.bytes.store(0, Ordering::Relaxed);
    let old = run(&ordered, sql, false).await;
    let old_bytes = store.bytes.swap(0, Ordering::Relaxed);
    let new = run(&ordered, sql, true).await;
    let new_bytes = store.bytes.load(Ordering::Relaxed);
    assert_eq!(rows(&old), rows(&new));
    assert!(
        new_bytes * 4 < old_bytes,
        "old={old_bytes}, new={new_bytes}"
    );
    println!("late-source concat zero-match old_bytes={old_bytes} new_bytes={new_bytes}");
}

#[tokio::test(flavor = "multi_thread")]
async fn late_source_type_change_preserves_cast_and_query_results() {
    let physical = Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("body", DataType::Int64, true),
    ]));
    let mut writer = VixWriter::new(
        &physical,
        VixWriterOptions {
            columns_complete: true,
            ..Default::default()
        },
        false,
    );
    let batch = RecordBatch::try_new(
        physical,
        vec![
            Arc::new(Int64Array::from_iter_values((1..=1000).rev())),
            Arc::new(Int64Array::from_iter_values(
                (0..1000).map(|row| if row == 123 { 123 } else { 0 }),
            )),
        ],
    )
    .unwrap();
    writer
        .push_batch_with_source(
            &batch,
            &StringArray::from_iter_values((0..1000).map(|row| format!("{{\"row\":{row}}}"))),
            None,
        )
        .unwrap();
    let data = bytes::Bytes::from(writer.finish().unwrap().0);
    let logical = Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
        Field::new(SOURCE_COL_NAME, DataType::Utf8, true),
    ]));
    let expr: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_plan::expressions::IsNullExpr::new(Arc::new(Column::new("body", 1))),
    );
    let predicate = LateSourcePredicate::try_new(&expr, &logical)
        .unwrap()
        .unwrap();
    assert!(
        predicate
            .select(&VixDocs::open(data.clone()).unwrap())
            .unwrap()
            .is_none(),
        "stored numeric -> registry string must retain the legacy cast path"
    );
    let (ctx, _) = context(data, logical).await;
    let sql = "SELECT _timestamp, body, _source FROM t WHERE body LIKE '%123%' ORDER BY _timestamp DESC LIMIT 2";
    let old = rows(&run(&ctx, sql, false).await);
    assert_eq!(old.len(), 1);
    assert_eq!(old, rows(&run(&ctx, sql, true).await));
}

#[tokio::test(flavor = "multi_thread")]
async fn late_source_admission_failure_releases_memory_before_io() {
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    let (data, schema) = fixture(true);
    let store = Arc::new(MeasuredStore::default());
    let path = Path::from("data/admission.vix");
    store.put(&path, data.into()).await.unwrap();
    let meta = store.head(&path).await.unwrap();
    let expr: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_plan::expressions::IsNullExpr::new(Arc::new(Column::new("body", 1))),
    );
    for enabled in [false, true] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let opener = VixCoreOpener {
            column_bounds: vec![equal_body("missing")],
            null_rejected_columns: vec![],
            late_source_predicate: if enabled {
                LateSourcePredicate::try_new(&expr, &schema).unwrap()
            } else {
                None
            },
            selective_source_range_prepass: enabled,
            emit_ts_desc: false,
            batch_size: 1,
            object_store: store.clone(),
            projected_schema: schema.clone(),
            timestamp_filter: None,
            memory_pool: Some(pool.clone()),
            conversion_width: None,
            metrics: ExecutionPlanMetricsSet::new(),
            partition: 0,
        };
        let mut file = PartitionedFile::new(path.to_string(), meta.size);
        file.object_meta = meta.clone();
        assert!(opener.open(file).unwrap().await.is_err());
        assert_eq!(pool.reserved(), 0);
        assert_eq!(store.bytes.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn late_source_sparse_selection_and_expression_budget() {
    let (data, schema) = fixture(true);
    let schema = fulltext_schema(&schema);
    let expr = crate::index::Condition::MatchAll("ENOSPC".into())
        .to_physical_expr(&schema, &["body".into(), "message".into()])
        .unwrap();
    let predicate = LateSourcePredicate::try_new(&expr, &schema)
        .unwrap()
        .unwrap();
    let selected = predicate
        .select(&VixDocs::open(data).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(selected.matched(), 11);
    assert!(
        selected.iter().any(|row| row > 8192),
        "selection must preserve row offsets across windows"
    );
    let mut oversized: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_plan::expressions::IsNullExpr::new(Arc::new(Column::new("body", 1))),
    );
    for _ in 0..128 {
        oversized = Arc::new(datafusion::physical_plan::expressions::NotExpr::new(
            oversized,
        ));
    }
    assert!(
        LateSourcePredicate::try_new(&oversized, &schema)
            .unwrap()
            .is_none()
    );
}

fn fulltext_schema(schema: &Schema) -> SchemaRef {
    // Condition::MatchAll constructs Utf8View literals, as in production plans.
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                if matches!(field.name().as_str(), "body" | "message") {
                    Arc::new(field.as_ref().clone().with_data_type(DataType::Utf8View))
                } else {
                    field.clone()
                }
            })
            .collect::<Vec<_>>(),
    ))
}
