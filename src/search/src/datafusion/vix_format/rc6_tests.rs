// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

use arrow::array::{Array, StringArray};
use datafusion::{
    datasource::{
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
    },
    execution::{runtime_env::RuntimeEnvBuilder, session_state::SessionStateBuilder},
    physical_plan::{
        collect,
        expressions::{BinaryExpr, Column, LikeExpr, Literal},
        filter::FilterExec,
    },
    prelude::{SessionConfig, SessionContext},
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use vortex_index::{VixWriter, VixWriterOptions};

use super::*;

fn string_file(values: Vec<Option<String>>, start: i64) -> bytes::Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..values.len()).map(|i| start + i as i64 + 1),
            )),
            Arc::new(StringArray::from_iter(values.iter().map(|v| v.as_deref()))),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            row_group_size: 32,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(
            &batch,
            &StringArray::from(vec!["{}"; batch.num_rows()]),
            None,
        )
        .unwrap();
    bytes::Bytes::from(writer.finish().unwrap().0)
}

fn numeric_file() -> bytes::Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("body", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![900001, 900002, 900003])),
            Arc::new(Int64Array::from(vec![Some(123), Some(-42), None])),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(
            &batch,
            &StringArray::from(vec![r#"{"body":123}"#, r#"{"body":-42}"#, "{}"]),
            None,
        )
        .unwrap();
    bytes::Bytes::from(writer.finish().unwrap().0)
}

async fn context(files: &[bytes::Bytes]) -> Result<SessionContext> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT_CONTEXT: AtomicUsize = AtomicUsize::new(0);
    let root = format!("rc6-data-{}/", NEXT_CONTEXT.fetch_add(1, Ordering::Relaxed));
    let store = Arc::new(InMemory::new());
    for (i, file) in files.iter().enumerate() {
        store
            .put(&Path::from(format!("{root}{i}.vix")), file.clone().into())
            .await?;
    }
    let registry = DefaultObjectStoreRegistry::new();
    registry.register_store(&url::Url::parse("test:///").unwrap(), store);
    let runtime = RuntimeEnvBuilder::new()
        .with_object_store_registry(Arc::new(registry))
        .build()?;
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(1))
        .with_runtime_env(Arc::new(runtime))
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let config = ListingTableConfig::new(ListingTableUrl::parse(format!("test:///{root}"))?)
        .with_listing_options(ListingOptions::new(Arc::new(VixCoreFormat::new(None))))
        .with_schema(schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(config)?))?;
    Ok(ctx)
}

fn scan_bounds(plan: &Arc<dyn ExecutionPlan>) -> Vec<ColumnBound> {
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some(conf) = exec.data_source().downcast_ref::<FileScanConfig>()
    {
        let source: &dyn std::any::Any = conf.file_source.as_ref();
        if let Some(source) = source.downcast_ref::<VixCoreSource>() {
            return source.column_bounds.clone();
        }
    }
    plan.children().into_iter().flat_map(scan_bounds).collect()
}

async fn row_ids(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> Result<Vec<i64>> {
    let batches = collect(plan, ctx.task_ctx()).await?;
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name(TIMESTAMP_COL_NAME)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        ids.extend(column.iter().flatten());
    }
    ids.sort_unstable();
    Ok(ids)
}

#[tokio::test]
async fn rc6_core_rows_equal_with_pruning_disabled() -> Result<()> {
    let prefixes = [
        "alpha",
        "beta",
        "你好",
        "😀",
        "a%b",
        "a_b",
        r"a\b",
        "Z",
        "\u{10ffff}",
        "\u{d7ff}",
    ];
    let mut files = Vec::new();
    let mut seed = 42_u64;
    for (i, prefix) in prefixes.into_iter().enumerate() {
        let values = (0..256)
            .map(|row| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                if row % 19 == 0 {
                    None
                } else {
                    Some(format!("{prefix}{}", seed % 32))
                }
            })
            .collect();
        files.push(string_file(values, (i * 256) as i64));
    }
    files.push(numeric_file());
    let ctx = context(&files).await?;
    let mut reduced = 0;
    for predicate in [
        "body = 'alpha1' OR body = 'alpha2'",
        "'alpha1' = body OR body = 'alpha2'",
        "body IN ('alpha1', 'alpha2')",
        "body LIKE 'alpha%'",
        "body LIKE '你好%'",
        "body LIKE '😀%'",
        r"body LIKE 'a\%b%'",
        r"body LIKE 'a\_b%'",
        r"body LIKE 'a\\b%'",
        r"body LIKE 'a\%b%' ESCAPE '\'",
        "body LIKE '\u{10ffff}%'",
        "body LIKE '\u{d7ff}%'",
        "body LIKE '12%'",
        "body = '123' OR body = '-42'",
        "body LIKE '%'",
        "body ILIKE 'ALPHA%'",
        "body NOT LIKE 'alpha%'",
        "body LIKE 'a_ph%'",
        "body = 'alpha1' OR body IS NULL",
        "body IN ('alpha1', NULL)",
        "body = 'alpha1' OR body = NULL",
        "body = 'alpha1' OR _timestamp = 900001",
        "CAST(_timestamp AS VARCHAR) LIKE '9%'",
        "body LIKE 'alpha%' AND (body = 'alpha1' OR body = 'alpha2')",
    ] {
        let sql = format!("SELECT _timestamp, body FROM t WHERE {predicate}");
        let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
        // Execution plans can contain one-shot repartition state. Build each
        // run independently, including shapes for which extraction is a no-op.
        let expected = row_ids(&ctx, plan).await?;
        let old = inject_vix_scan_pruning_with_options(
            ctx.sql(&sql).await?.create_physical_plan().await?,
            false,
            false,
        )?;
        let new = inject_vix_scan_pruning_with_options(
            ctx.sql(&sql).await?.create_physical_plan().await?,
            true,
            true,
        )?;
        assert_eq!(row_ids(&ctx, old.clone()).await?, expected, "legacy {sql}");
        let actual = row_ids(&ctx, new.clone()).await?;
        assert_eq!(actual, expected, "{sql}");
        let old_bounds = scan_bounds(&old);
        let new_bounds = scan_bounds(&new);
        let candidates = |bounds: &[ColumnBound]| {
            files
                .iter()
                .filter(|file| {
                    let docs = VixDocs::open((*file).clone()).unwrap();
                    file_provably_skippable(&docs, None, bounds, &[]).is_none()
                })
                .count()
        };
        if candidates(&new_bounds) < candidates(&old_bounds) {
            reduced += 1;
        }
    }
    assert!(
        reduced >= 6,
        "OR and prefix predicates must reduce candidate files: {reduced}"
    );
    // This DataFusion revision rejects non-backslash ESCAPE before a
    // physical LikeExpr exists. Never infer alternative escape semantics.
    let df = ctx
        .sql("SELECT _timestamp, body FROM t WHERE body LIKE 'a!_%' ESCAPE '!'")
        .await?;
    assert!(df.create_physical_plan().await.is_err());
    Ok(())
}

#[tokio::test]
async fn rc6_core_switches_and_unknown_or_arms() -> Result<()> {
    use datafusion::logical_expr::Operator;
    let ctx = context(&[string_file(vec![Some("abc".into())], 1)]).await?;
    // Build the filter directly, so SQL optimizer rewrites cannot hide
    // whether this extractor handles the OR/LIKE expression itself.
    let scan = ctx
        .sql("SELECT _timestamp, body FROM t")
        .await?
        .create_physical_plan()
        .await?;
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("body", 1));
    let literal = |s: &str| -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Utf8(Some(s.into()))))
    };
    let eq = |value: Arc<dyn PhysicalExpr>| -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(column.clone(), Operator::Eq, value))
    };
    let or: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        eq(literal("ab")),
        Operator::Or,
        eq(literal("ac")),
    ));
    let prefix: Arc<dyn PhysicalExpr> =
        Arc::new(LikeExpr::new(false, false, column, literal("ab%")));
    for (predicate, or_enabled, prefix_enabled, expected) in [
        (or.clone(), false, false, 0),
        (or.clone(), true, false, 1),
        (or, false, true, 0),
        (prefix.clone(), false, false, 0),
        (prefix.clone(), true, false, 0),
        (prefix, false, true, 1),
    ] {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, scan.clone())?);
        let plan = inject_vix_scan_pruning_with_options(plan, or_enabled, prefix_enabled)?;
        assert_eq!(scan_bounds(&plan).len(), expected);
    }
    Ok(())
}

#[test]
fn rc6_bounded_scan_propagates_consumer_cancellation() {
    let file = string_file((0..512).map(|i| Some(format!("alpha{i}"))).collect(), 0);
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let bounds = [ColumnBound {
        column: "body".into(),
        min: Some((BoundValue::Str("alpha".into()), true)),
        max: Some((BoundValue::Str("alphb".into()), false)),
    }];
    let mut calls = 0;
    let result = scan_core_docs(
        VixDocs::open(file.clone()).unwrap(),
        &schema,
        None,
        None,
        &bounds,
        32,
        &mut |_| {
            calls += 1;
            anyhow::bail!("rc6 test consumer cancelled")
        },
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("rc6 test consumer cancelled")
    );
    assert_eq!(calls, 1, "do not decode further chunks after cancellation");
    // Cancellation must not poison the immutable file or leave a resource
    // needed by another reader held. All storage is in memory.
    let mut rows = 0;
    scan_core_docs(
        VixDocs::open(file).unwrap(),
        &schema,
        None,
        None,
        &bounds,
        32,
        &mut |batch| {
            rows += batch.num_rows();
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(rows, 512);
}
