// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{sync::Arc, time::Duration};

use datafusion::{
    common::Result,
    execution::{
        context::SessionContext,
        memory_pool::{GreedyMemoryPool, MemoryPool},
        runtime_env::RuntimeEnvBuilder,
        session_state::SessionStateBuilder,
    },
    physical_plan::{collect, display::DisplayableExecutionPlan},
};

// DF 54's shared SpillPool can strand an unfinished file when concurrent
// writers replace current_write_file. Both the reader and distributor park.
// This exercises the *unmodified* count implementation, not SharedCount.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn repartition_spill_finishes_with_concurrent_writers() -> Result<()> {
    for attempt in 0..12 {
        let pool = Arc::new(GreedyMemoryPool::new(4 * 1024 * 1024));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build_arc()?;
        let cfg = super::exec::create_session_config(false, 2)?
            .with_target_partitions(2)
            .with_batch_size(64);
        let state = SessionStateBuilder::new()
            .with_config(cfg)
            .with_runtime_env(runtime.clone())
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.sql("CREATE TABLE t AS SELECT v % 13 AS g, CASE WHEN v % 29 = 0 THEN NULL ELSE md5(CAST(v % 337 AS VARCHAR)) END AS trace_id FROM generate_series(1, 40000) AS s(v)").await?.collect().await?;
        let plan = ctx
            .sql("SELECT g, trace_id, count(*) FROM t GROUP BY g, trace_id")
            .await?
            .create_physical_plan()
            .await?;
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            collect(plan.clone(), ctx.task_ctx()),
        )
        .await;
        let metrics = format!(
            "{}",
            DisplayableExecutionPlan::with_metrics(plan.as_ref()).indent(true)
        );
        drop(plan);
        for _ in 0..200 {
            if pool.reserved() == 0 && runtime.spilling_progress().active_files_count == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            pool.reserved(),
            0,
            "attempt {attempt}: cancelled pool leaked"
        );
        assert_eq!(runtime.spilling_progress().active_files_count, 0);
        let result = result.unwrap_or_else(|_| {
            panic!("attempt {attempt}: Repartition spill deadlocked\n{metrics}")
        });
        match result {
            Ok(batches) => assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4394),
            // Concurrent scheduling changes how much of this deliberately tiny
            // pool is available for sort headroom. Resource errors must remain
            // errors; completing OR failing promptly is the liveness contract.
            Err(datafusion::common::DataFusionError::ResourcesExhausted(_)) => {}
            Err(error) => return Err(error),
        }
        assert!(metrics.contains("spill_count="));
    }
    Ok(())
}
