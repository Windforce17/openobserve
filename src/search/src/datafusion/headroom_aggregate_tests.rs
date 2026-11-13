// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    error::Result,
    execution::{
        TaskContext,
        context::SessionConfig,
        memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool},
        runtime_env::RuntimeEnvBuilder,
    },
    functions_aggregate::count::count_udaf,
    physical_expr::{aggregate::AggregateExprBuilder, expressions::col},
    physical_plan::{
        ExecutionPlan, SendableRecordBatchStream,
        aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
        collect, displayable,
        stream::RecordBatchStreamAdapter,
        streaming::{PartitionStream, StreamingTableExec},
    },
};

use super::spillable_reserve_pool::SpillableReservePool;

#[derive(Debug)]
struct GrowingOpenerSource {
    schema: SchemaRef,
}

impl PartitionStream for GrowingOpenerSource {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        let opener = MemoryConsumer::new("simulated VixCoreOpener").register(ctx.memory_pool());
        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter((0..128).map(move |batch| {
                // A bounded, identical allocation schedule in every run. Input
                // advances only when the real aggregate asks for another batch.
                if (20..28).contains(&batch) {
                    opener.try_grow(64 * 1024)?;
                }
                let keys = StringArray::from_iter_values((0..256).map(|row| {
                    let id = (batch * 256 + row) % 24_000;
                    format!("{id:08}-{}", "x".repeat(240))
                }));
                Ok(RecordBatch::try_new(schema.clone(), vec![Arc::new(keys)])?)
            })),
        ))
    }
}

async fn aggregate(pool: Arc<dyn MemoryPool>) -> Result<BTreeMap<String, i64>> {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
    let input: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
        schema.clone(),
        vec![Arc::new(GrowingOpenerSource {
            schema: schema.clone(),
        })],
        None,
        [],
        false,
        None,
    )?);
    let group_by = PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".into())]);
    let aggregates = vec![Arc::new(
        AggregateExprBuilder::new(count_udaf(), vec![col("key", &schema)?])
            .schema(schema.clone())
            .alias("count")
            .build()?,
    )];
    let partial: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
        AggregateMode::Partial,
        group_by.clone(),
        aggregates.clone(),
        vec![None],
        input,
        schema.clone(),
    )?);
    let final_agg: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        group_by,
        aggregates,
        vec![None],
        partial,
        schema,
    )?);
    let plan = displayable(final_agg.as_ref()).indent(true).to_string();
    assert!(
        plan.contains("mode=Partial") && plan.contains("mode=Final"),
        "{plan}"
    );
    let config = SessionConfig::new().with_batch_size(256).set_usize(
        "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
        usize::MAX,
    );
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;
    let ctx = Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime.clone()),
    );
    let result = tokio::time::timeout(Duration::from_secs(30), collect(final_agg.clone(), ctx))
        .await
        .expect("aggregation must finish or fail promptly");
    let metrics = datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(
        final_agg.as_ref(),
    )
    .indent(true)
    .to_string();
    eprintln!("HEADROOM_AGGREGATE {metrics}");
    drop(final_agg);
    assert_eq!(
        pool.reserved(),
        0,
        "success/error must release reservations"
    );
    assert_eq!(runtime.spilling_progress().active_files_count, 0);
    let mut output = BTreeMap::new();
    for batch in result? {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!keys.is_null(row));
            assert!(
                output
                    .insert(keys.value(row).to_string(), counts.value(row))
                    .is_none()
            );
        }
    }
    Ok(output)
}

#[tokio::test]
async fn headroom_real_partial_final_preserves_opener_progress() {
    // At this limit the unprotected partial state leaves less than one opener
    // increment; a 10% reserve makes Partial emit before reaching that state.
    const LIMIT: usize = 21 * 1024 * 1024 / 8;
    let expected = aggregate(Arc::new(UnboundedMemoryPool::default()))
        .await
        .unwrap();
    assert_eq!(expected.len(), 24_000);
    assert_eq!(expected.values().sum::<i64>(), 128 * 256);
    let baseline = aggregate(SpillableReservePool::wrap(
        "aggregate",
        LIMIT,
        0,
        Arc::new(GreedyMemoryPool::new(LIMIT)),
    ))
    .await;
    assert!(
        baseline
            .as_ref()
            .is_err_and(|error| error.to_string().contains("simulated VixCoreOpener")),
        "expected opener starvation, got error {:?}",
        baseline.as_ref().err()
    );
    for percent in [10, 15, 25] {
        let actual = aggregate(SpillableReservePool::wrap(
            "aggregate",
            LIMIT,
            percent,
            Arc::new(GreedyMemoryPool::new(LIMIT)),
        ))
        .await
        .unwrap();
        assert_eq!(actual, expected, "percent={percent}");
    }
}
