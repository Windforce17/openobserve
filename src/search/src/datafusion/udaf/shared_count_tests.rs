// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

use datafusion::{
    logical_expr::AggregateUDF,
    physical_expr::{aggregate::AggregateExprBuilder, expressions::Column},
    physical_expr_common::binary_map::OutputType,
};
use datafusion_functions_aggregate_common::aggregate::{
    count_distinct::{BytesDistinctCountAccumulator, BytesViewDistinctCountAccumulator},
    groups_accumulator::GroupsAccumulatorAdapter,
};

use super::*;

fn old(dt: &DataType) -> Box<dyn GroupsAccumulator> {
    let dt = dt.clone();
    Box::new(GroupsAccumulatorAdapter::new(move || {
        let acc: Box<dyn Accumulator> = match dt {
            DataType::Utf8 => Box::new(BytesDistinctCountAccumulator::<i32>::new(OutputType::Utf8)),
            DataType::LargeUtf8 => {
                Box::new(BytesDistinctCountAccumulator::<i64>::new(OutputType::Utf8))
            }
            DataType::Utf8View => {
                Box::new(BytesViewDistinctCountAccumulator::new(OutputType::Utf8View))
            }
            DataType::Binary => Box::new(BytesDistinctCountAccumulator::<i32>::new(
                OutputType::Binary,
            )),
            DataType::LargeBinary => Box::new(BytesDistinctCountAccumulator::<i64>::new(
                OutputType::Binary,
            )),
            DataType::BinaryView => Box::new(BytesViewDistinctCountAccumulator::new(
                OutputType::BinaryView,
            )),
            _ => unreachable!(),
        };
        Ok(acc)
    }))
}
fn types() -> [DataType; 6] {
    [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ]
}
fn values(dt: &DataType, round: usize) -> ArrayRef {
    let data: Vec<Option<String>> = (0..1009)
        .map(|i| {
            let k = (i * 7919 + round * 104729) % 97;
            if k.is_multiple_of(5) {
                None
            } else if k == 1 {
                Some(String::new())
            } else if k == 2 {
                Some("超长string".repeat(4096))
            } else {
                Some(format!("值-{k}-{}", i % 7))
            }
        })
        .collect();
    arrow::compute::cast(&(Arc::new(StringArray::from(data)) as ArrayRef), dt).unwrap()
}
fn check_equal(a: ArrayRef, b: ArrayRef) {
    assert_eq!(a.to_data(), b.to_data());
}

#[test]
fn shared_count_convert_to_state_matches_update_and_upstream_merge() -> Result<()> {
    for dt in types() {
        let input = values(&dt, 0);
        let groups: Vec<_> = (0..input.len()).map(|row| row % 17).collect();
        let filter = BooleanArray::from_iter((0..input.len()).map(|row| {
            if row % 11 == 0 {
                None
            } else {
                Some(row % 3 != 0)
            }
        }));
        for filter in [None, Some(&filter)] {
            let source = ByteDistinctGroups::new(dt.clone());
            let original_size = source.size();
            let state = source.convert_to_state(&[Arc::clone(&input)], filter)?;
            assert_eq!(state[0].len(), input.len());
            assert_eq!(
                source.size(),
                original_size,
                "conversion must not mutate the accumulator"
            );
            let mut updated = ByteDistinctGroups::new(dt.clone());
            updated.update_batch(&[Arc::clone(&input)], &groups, filter, 19)?;
            let expected = updated.evaluate(EmitTo::All)?;
            let mut merged = ByteDistinctGroups::new(dt.clone());
            merged.merge_batch(&state, &groups, 19)?;
            check_equal(Arc::clone(&expected), merged.evaluate(EmitTo::All)?);
            let mut upstream = old(&dt);
            upstream.merge_batch(&state, &groups, 19)?;
            check_equal(expected, upstream.evaluate(EmitTo::All)?);
        }
    }
    Ok(())
}

#[test]
fn shared_count_random_cross_implementation_and_emit_first() -> Result<()> {
    for dt in types() {
        let mut baseline = old(&dt);
        let mut candidate = ByteDistinctGroups::new(dt.clone());
        let groups: Vec<usize> = (0..1009).map(|i| (i * 13) % 79).collect();
        let filter = BooleanArray::from(
            (0..1009)
                .map(|i| if i % 17 == 0 { None } else { Some(i % 3 != 0) })
                .collect::<Vec<_>>(),
        );
        for round in 0..7 {
            let batch = vec![values(&dt, round)];
            baseline.update_batch(&batch, &groups, Some(&filter), 83)?;
            candidate.update_batch(&batch, &groups, Some(&filter), 83)?;
        }
        // NULLs, filtered groups and never-seen groups all retain zero counts.
        for n in [0, 13, 29, 41] {
            let old_state = baseline.state(EmitTo::First(n))?;
            let new_state = candidate.state(EmitTo::First(n))?;
            if n == 0 {
                assert!(old_state.is_empty());
                assert_eq!(new_state[0].len(), 0);
                continue;
            }
            assert_eq!(old_state[0].data_type(), new_state[0].data_type());
            let ids: Vec<usize> = (0..n).collect();
            let mut old_from_new = old(&dt);
            let mut new_from_old = ByteDistinctGroups::new(dt.clone());
            let mut all_old = old(&dt);
            for _ in 0..3 {
                // Repeated state merges must remain idempotent.
                old_from_new.merge_batch(&new_state, &ids, n)?;
                new_from_old.merge_batch(&old_state, &ids, n)?;
                all_old.merge_batch(&old_state, &ids, n)?;
            }
            // Exercise another serialization round, not merely evaluation.
            let roundtrip = new_from_old.state(EmitTo::All)?;
            new_from_old.merge_batch(&roundtrip, &ids, n)?;
            let expected = all_old.evaluate(EmitTo::All)?;
            check_equal(expected.clone(), old_from_new.evaluate(EmitTo::All)?);
            check_equal(expected, new_from_old.evaluate(EmitTo::All)?);
        }
        assert_eq!(candidate.size(), size_of::<ByteDistinctGroups>());
        // Reuse after all groups have been emitted.
        let batch = vec![values(&dt, 9)];
        baseline.update_batch(&batch, &groups, None, 83)?;
        candidate.update_batch(&batch, &groups, None, 83)?;
        check_equal(
            baseline.evaluate(EmitTo::First(17))?,
            candidate.evaluate(EmitTo::First(17))?,
        );
        check_equal(
            baseline.evaluate(EmitTo::All)?,
            candidate.evaluate(EmitTo::All)?,
        );
        assert_eq!(candidate.size(), size_of::<ByteDistinctGroups>());
    }
    Ok(())
}

#[test]
fn shared_count_large_group_budget_and_state_schema() -> Result<()> {
    for dt in types() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "value",
            dt.clone(),
            true,
        )]));
        let original = AggregateExprBuilder::new(
            datafusion::functions_aggregate::count::count_udaf(),
            vec![Arc::new(Column::new("value", 0))],
        )
        .schema(schema.clone())
        .alias("count(DISTINCT value)")
        .with_distinct(true)
        .build()?;
        let replacement = AggregateExprBuilder::new(
            Arc::new(AggregateUDF::from(SharedCount::default())),
            vec![Arc::new(Column::new("value", 0))],
        )
        .schema(schema)
        .alias("count(DISTINCT value)")
        .with_distinct(true)
        .build()?;
        assert_eq!(original.state_fields()?, replacement.state_fields()?);
        assert!(replacement.groups_accumulator_supported());
        let mut candidate = replacement.create_groups_accumulator()?;
        let batch = arrow::compute::cast(
            &(Arc::new(StringArray::from(vec![Some("value"); 16384])) as ArrayRef),
            &dt,
        )?;
        let ids: Vec<usize> = (0..16384).collect();
        candidate.update_batch(&[batch], &ids, None, 16384)?;
        assert!(
            candidate.size() < 2 * 1024 * 1024,
            "shared table must fit in 2 MiB, got {}",
            candidate.size()
        );
        let state = candidate.state(EmitTo::All)?;
        assert_eq!(
            state[0].data_type(),
            original.state_fields()?[0].data_type()
        );
    }
    Ok(())
}

#[tokio::test]
async fn shared_count_sql_differential_and_plain_count_identity() -> Result<()> {
    use datafusion::{datasource::MemTable, execution::context::SessionContext};
    for dt in types() {
        let vals = values(&dt, 0);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", dt, true),
        ]));
        let groups = Int64Array::from(
            (0..1009)
                .map(|i| {
                    if i % 11 == 0 {
                        None
                    } else {
                        Some((i % 29) as i64)
                    }
                })
                .collect::<Vec<_>>(),
        );
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(groups), vals],
        )?;
        let old = SessionContext::new();
        let new = crate::datafusion::exec::DataFusionContextBuilder::new()
            .build(4)
            .await?;
        for ctx in [&old, &new] {
            ctx.register_table(
                "t",
                Arc::new(MemTable::try_new(
                    schema.clone(),
                    vec![vec![batch.clone()], vec![batch.clone()]],
                )?),
            )?;
        }
        for sql in [
            "SELECT g, count(*), count(DISTINCT v), count(DISTINCT g) FROM t GROUP BY g ORDER BY g",
            "SELECT g, count(DISTINCT v) FILTER (WHERE g > 10), count(v) FROM t GROUP BY g ORDER BY g",
            "SELECT count(DISTINCT v), count(v), count(*) FROM t",
            "SELECT g, count(DISTINCT v) FROM t WHERE false GROUP BY g",
            "SELECT count(DISTINCT v), count(*) FROM t WHERE false",
        ] {
            let a = old.sql(sql).await?.collect().await?;
            let b = new.sql(sql).await?.collect().await?;
            let a = arrow::util::pretty::pretty_format_batches(&a)?.to_string();
            let b = arrow::util::pretty::pretty_format_batches(&b)?.to_string();
            assert_eq!(a, b, "{sql}");
        }
    }
    let schema = Arc::new(arrow::datatypes::Schema::empty());
    let expr = AggregateExprBuilder::new(
        Arc::new(AggregateUDF::from(SharedCount::default())),
        vec![datafusion::physical_expr::expressions::lit(1i64)],
    )
    .schema(schema)
    .alias("n")
    .build()?;
    assert!(
        crate::datafusion::optimizer::physical_optimizer::utils::is_count_rows_aggregate(&expr)
    );
    Ok(())
}

#[tokio::test]
async fn shared_count_skip_partial_distinct_and_plain_count() -> Result<()> {
    use datafusion::{
        datasource::MemTable,
        execution::context::{SessionConfig, SessionContext},
        physical_plan::{collect, display::DisplayableExecutionPlan},
    };

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
    ]));
    let batches = (0..8)
        .map(|batch| {
            arrow::record_batch::RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values((0..128).map(|row| row % 31))),
                    Arc::new(StringArray::from_iter((0..128).map(|row| {
                        (row % 7 != 0).then(|| format!("value-{}", (row + batch) % 43))
                    }))),
                ],
            )
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut outputs = Vec::new();
    for skip in [false, true] {
        let mut config = SessionConfig::new()
            .with_target_partitions(2)
            .with_batch_size(128);
        config
            .options_mut()
            .execution
            .skip_partial_aggregation_probe_rows_threshold = if skip { 0 } else { usize::MAX };
        config
            .options_mut()
            .execution
            .skip_partial_aggregation_probe_ratio_threshold = 0.0;
        let ctx = SessionContext::new_with_config(config);
        ctx.register_udaf(AggregateUDF::from(SharedCount::default()));
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(
                Arc::clone(&schema),
                vec![batches.clone(); 2],
            )?),
        )?;
        let plan = ctx.sql("SELECT g, count(*), count(v), count(DISTINCT v), count(DISTINCT v) FILTER (WHERE g > 10) FROM t GROUP BY g ORDER BY g")
            .await?.create_physical_plan().await?;
        let result = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        outputs.push(arrow::util::pretty::pretty_format_batches(&result)?.to_string());
        if skip {
            let mut skipped = 0;
            use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
            plan.apply(|node| {
                if let Some(metrics) = node.metrics() {
                    for metric in metrics.iter() {
                        if metric.value().name() == "skipped_aggregation_rows" {
                            skipped += metric.value().as_usize();
                        }
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(
                skipped > 0,
                "skip-partial path not exercised: {}",
                DisplayableExecutionPlan::with_metrics(plan.as_ref()).indent(true)
            );
        }
    }
    assert_eq!(outputs[0], outputs[1]);
    Ok(())
}

#[tokio::test]
async fn shared_count_wire_registry_old_new_and_drop() -> Result<()> {
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        execution::{
            context::SessionContext,
            memory_pool::{FairSpillPool, MemoryPool},
            runtime_env::RuntimeEnvBuilder,
            session_state::SessionStateBuilder,
        },
        physical_plan::{aggregates::AggregateExec, collect},
    };
    use datafusion_proto::bytes::{
        physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
    };
    use futures::StreamExt;

    use crate::datafusion::{
        distributed_plan::codec::get_physical_extension_codec, exec::DataFusionContextBuilder,
        table_provider::empty_table::NewEmptyTable,
    };
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("v", DataType::Utf8View, true),
    ]));
    let original = SessionContext::new();
    let replacement = DataFusionContextBuilder::new().build(4).await?;
    let codec = get_physical_extension_codec();
    for ctx in [&original, &replacement] {
        ctx.register_table(
            "t",
            Arc::new(NewEmptyTable::new("t", schema.clone()).with_partitions(4)),
        )?;
    }
    for sender in [&original, &replacement] {
        // DF56 rewrites mixed count + byte DISTINCT to a two-level GROUP BY.
        // A FILTER intentionally keeps DISTINCT so this tests registry decoding,
        // rather than an optimizer-eliminated accumulator (single_distinct_to_groupby).
        let plan = sender
            .sql("SELECT g, count(*), count(DISTINCT v) FILTER (WHERE g > 0) FROM t GROUP BY g")
            .await?
            .create_physical_plan()
            .await?;
        let bytes = physical_plan_to_bytes_with_extension_codec(plan, &codec)?;
        for (receiver, is_new) in [(&original, false), (&replacement, true)] {
            let decoded = physical_plan_from_bytes_with_extension_codec(
                &bytes,
                &receiver.task_ctx(),
                &codec,
            )?;
            let mut seen = 0;
            decoded.apply(|p| {
                if let Some(agg) = p.downcast_ref::<AggregateExec>() {
                    for expr in agg.aggr_expr() {
                        if expr.is_distinct() {
                            seen += 1;
                            assert_eq!(expr.groups_accumulator_supported(), is_new);
                            assert_eq!(expr.fun().name(), "count");
                        }
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(seen > 0);
        }
    }
    let pool = Arc::new(FairSpillPool::new(128 * 1024 * 1024));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build()?,
    );
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(replacement.state())
            .with_runtime_env(runtime)
            .build(),
    );
    let groups = Int64Array::from_iter_values(0..16384);
    let values: ArrayRef = Arc::new(StringViewArray::from_iter_values(
        (0..16384).map(|i| format!("long distinct value {i}")),
    ));
    let batch =
        arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![Arc::new(groups), values])?;
    ctx.register_table(
        "input",
        Arc::new(datafusion::datasource::MemTable::try_new(
            schema,
            vec![vec![batch; 4]; 4],
        )?),
    )?;
    for cancel in [false, true] {
        let plan = ctx
            .sql("SELECT g, count(*), count(DISTINCT v) FROM input GROUP BY g")
            .await?
            .create_physical_plan()
            .await?;
        if cancel {
            let mut stream =
                datafusion::physical_plan::execute_stream(plan.clone(), ctx.task_ctx())?;
            let _ = stream.next().await.transpose()?;
            drop(stream);
        } else {
            let _ = collect(plan.clone(), ctx.task_ctx()).await?;
        }
        drop(plan);
        // Repartition workers release their reservations asynchronously on abort.
        for _ in 0..200 {
            if pool.reserved() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(pool.reserved(), 0, "cancel={cancel}");
    }
    Ok(())
}

#[tokio::test]
async fn shared_count_keeps_native_single_distinct_rewrite() -> Result<()> {
    use datafusion::{datasource::MemTable, execution::context::SessionContext};
    let native = Count::new();
    let shared = SharedCount::default();
    for dt in [
        DataType::Utf8,
        DataType::Utf8View,
        DataType::LargeBinary,
        DataType::Int64,
        DataType::Float64,
    ] {
        for is_distinct in [false, true] {
            assert_eq!(
                shared
                    .groups_accumulator_supported_for_types(std::slice::from_ref(&dt), is_distinct),
                native
                    .groups_accumulator_supported_for_types(std::slice::from_ref(&dt), is_distinct),
                "{dt} distinct={is_distinct}"
            );
        }
    }
    assert_eq!(shared.distinct_handling(), native.distinct_handling());
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("v", DataType::Utf8, true),
    ]));
    let mut plans = Vec::new();
    for use_shared in [false, true] {
        let ctx = SessionContext::new();
        if use_shared {
            ctx.register_udaf(AggregateUDF::from(SharedCount::default()));
        }
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]])?),
        )?;
        let plan = ctx
            .sql("SELECT g, count(DISTINCT v) FROM t GROUP BY g")
            .await?
            .into_optimized_plan()?;
        plans.push(plan.display_indent().to_string());
    }
    assert_eq!(
        plans[0].matches("Aggregate:").count(),
        2,
        "native count must take the single-distinct rewrite:\n{}",
        plans[0]
    );
    assert_eq!(plans[0], plans[1]);
    Ok(())
}
