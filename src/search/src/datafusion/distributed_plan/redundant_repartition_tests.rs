// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use datafusion::{
    execution::context::SessionConfig,
    physical_expr::{LexOrdering, PhysicalSortExpr},
    physical_plan::{
        ExecutionPlanProperties, Partitioning, displayable, expressions::col,
        limit::GlobalLimitExec, repartition::RepartitionExec, sorts::sort::SortExec,
    },
};
use hashbrown::HashMap;

use super::*;
use crate::datafusion::{
    distributed_plan::node::RemoteScanNodes,
    optimizer::physical_optimizer::remote_scan::RemoteScanRewriter,
    table_provider::empty_table::NewEmptyTable,
};

const TARGET_PARTITIONS: usize = 16;
const REMOTE_NODES: usize = 3;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn nodes(server: &Server) -> Arc<RemoteScanNodes> {
    let node = server.node(&[config::meta::cluster::Role::Querier], REMOTE_NODES);
    Arc::new(RemoteScanNodes::new(
        config::datafusion::request::Request {
            org_id: "rr-test".to_string(),
            timeout: 5,
            ..Default::default()
        },
        node.nodes.to_vec(),
        HashMap::from([(TableReference::bare("t"), vec![vec![1], vec![2], vec![3]])]),
        HashMap::new(),
        false,
        opentelemetry::Context::current(),
        None,
    ))
}

fn data(server: &Server, empty: bool) {
    *server.service.data.lock() = (0..REMOTE_NODES)
        .map(|node| {
            if empty || node == REMOTE_NODES - 1 {
                return vec![];
            }
            (0..3)
                .map(|batch| {
                    let start = i64::try_from(node * 100 + batch * 4).unwrap();
                    RecordBatch::try_new(
                        schema(),
                        vec![
                            Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(1)])),
                            Arc::new(Int64Array::from_iter_values(start..start + 4)),
                        ],
                    )
                    .unwrap()
                })
                .collect()
        })
        .collect();
}

async fn physical(sql: &str) -> Arc<dyn ExecutionPlan> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(TARGET_PARTITIONS),
    );
    ctx.register_table(
        "t",
        Arc::new(NewEmptyTable::new("t", schema()).with_partitions(TARGET_PARTITIONS)),
    )
    .unwrap();
    ctx.sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

fn rewrite(
    plan: Arc<dyn ExecutionPlan>,
    nodes: Arc<RemoteScanNodes>,
    enabled: bool,
) -> Arc<dyn ExecutionPlan> {
    rewrite_with_reduce(plan, nodes, enabled, false)
}

fn rewrite_with_reduce(
    plan: Arc<dyn ExecutionPlan>,
    nodes: Arc<RemoteScanNodes>,
    enabled: bool,
    reduce: bool,
) -> Arc<dyn ExecutionPlan> {
    let mut rewriter = RemoteScanRewriter::new(nodes);
    rewriter.skip_redundant_repartition = enabled;
    rewriter.partial_reduce_enabled = reduce;
    let rewritten = plan.rewrite(&mut rewriter).unwrap().data;
    assert!(rewriter.is_changed);
    rewritten
}

fn text(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(true).to_string()
}

fn assert_same_partitioning(left: &Arc<dyn ExecutionPlan>, right: &Arc<dyn ExecutionPlan>) {
    // UnknownPartitioning and UnKnownColumn deliberately compare unequal to themselves.
    // Compare metadata structure, not DataFusion's distribution-satisfaction equality.
    assert_eq!(
        format!("{:?}", left.output_partitioning()),
        format!("{:?}", right.output_partitioning())
    );
    assert_eq!(
        left.output_partitioning().partition_count(),
        right.output_partitioning().partition_count()
    );
}

// Count leader work only: the serialized child below RemoteScan runs on followers.
fn repartitions(plan: &Arc<dyn ExecutionPlan>) -> usize {
    if plan.is::<RemoteScanExec>() {
        return 0;
    }
    usize::from(plan.is::<RepartitionExec>())
        + plan.children().into_iter().map(repartitions).sum::<usize>()
}

fn remote_scans(plan: &Arc<dyn ExecutionPlan>) -> usize {
    if plan.is::<RemoteScanExec>() {
        assert_eq!(plan.output_partitioning().partition_count(), REMOTE_NODES);
        return 1;
    }
    plan.children().into_iter().map(remote_scans).sum()
}

async fn rows(plan: Arc<dyn ExecutionPlan>) -> Vec<String> {
    let batches = collect(plan.clone(), SessionContext::new().task_ctx())
        .await
        .unwrap();
    plan.apply(|node| {
        if let Some(remote) = node.downcast_ref::<RemoteScanExec>() {
            assert!(remote.partial_err.lock().is_empty());
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    let batch_refs = batches.iter().collect::<Vec<_>>();
    let mut rows = config::utils::arrow::record_batches_to_json_rows(&batch_refs)
        .unwrap()
        .into_iter()
        .map(|row| serde_json::to_string(&row).unwrap())
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn compare(sql: &str, expected_removed: usize, empty: bool) {
    compare_with_reduce(sql, expected_removed, empty, &[false, true]).await;
}

async fn compare_with_reduce(sql: &str, expected_removed: usize, empty: bool, reduces: &[bool]) {
    let server = Server::start().await;
    data(&server, empty);
    let local = SessionContext::new();
    let table =
        datafusion::datasource::MemTable::try_new(schema(), server.service.data.lock().clone())
            .unwrap();
    local.register_table("t", Arc::new(table)).unwrap();
    let oracle = rows(
        local
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap(),
    )
    .await;
    for &reduce in reduces {
        let source = physical(sql).await;
        let off = rewrite_with_reduce(source.clone(), nodes(&server), false, reduce);
        let on = rewrite_with_reduce(source, nodes(&server), true, reduce);
        let before = repartitions(&off);
        let after = repartitions(&on);
        assert_eq!(
            before - after,
            expected_removed,
            "off:\n{}on:\n{}",
            text(&off),
            text(&on)
        );
        assert_same_partitioning(&off, &on);
        let scans = remote_scans(&off);
        assert_eq!(scans, remote_scans(&on));
        if expected_removed == 0 {
            assert_eq!(text(&off), text(&on));
        }
        let expected = rows(off).await;
        let actual = rows(on).await;
        assert_eq!(expected, actual, "{sql}");
        assert_eq!(actual, oracle, "independent in-memory execution: {sql}");
        assert_eq!(server.take_requests().len(), scans * REMOTE_NODES * 2);
        println!(
            "RR_COUNT sql={sql:?} empty={empty} reduce={reduce} remote_nodes={REMOTE_NODES} target_partitions={TARGET_PARTITIONS} before={before} after={after} rows={}",
            actual.len()
        );
    }
    server.finish().await;
}

#[tokio::test]
async fn redundant_repartition_grouped_flight() {
    for empty in [false, true] {
        compare(
            "SELECT k, count(*) AS c, sum(v) AS total FROM t GROUP BY k",
            1,
            empty,
        )
        .await;
    }
}

#[tokio::test]
async fn redundant_repartition_ungrouped_flight() {
    for empty in [false, true] {
        compare("SELECT count(*) AS c, sum(v) AS total FROM t", 1, empty).await;
    }
}

#[tokio::test]
async fn redundant_repartition_limit_flight_keeps_legacy_plan() {
    compare("SELECT k, v FROM t ORDER BY v LIMIT 5", 0, false).await;
    compare(
        "SELECT k, count(*) AS c FROM t GROUP BY k ORDER BY c, k LIMIT 2",
        0,
        false,
    )
    .await;
}

#[tokio::test]
async fn redundant_repartition_sensitive_consumers_keep_legacy_plan() {
    let server = Server::start().await;
    for sql in [
        "SELECT sum(CAST(v AS DOUBLE)) FROM t",
        "SELECT k, array_agg(v) FROM t GROUP BY k",
        "SELECT first_value(v) FROM t",
        "SELECT row_number() OVER (), k, count(*) FROM t GROUP BY k",
        "SELECT k, v FROM t LIMIT 5 OFFSET 2",
    ] {
        for reduce in [false, true] {
            let source = physical(sql).await;
            let off = rewrite_with_reduce(source.clone(), nodes(&server), false, reduce);
            let on = rewrite_with_reduce(source, nodes(&server), true, reduce);
            assert_eq!(text(&off), text(&on), "{sql}");
            assert!(
                repartitions(&off) > 0,
                "exercise an actual RR boundary: {sql}"
            );
        }
    }
    server.finish().await;
    compare("SELECT sum(CAST(v AS DOUBLE)) FROM t", 0, false).await;
    compare_with_reduce("SELECT array_agg(v ORDER BY v) FROM t", 0, false, &[false]).await;
}

#[tokio::test]
async fn redundant_repartition_union_flight() {
    compare("SELECT k, count(*) AS c FROM t GROUP BY k UNION ALL SELECT k, count(*) AS c FROM t GROUP BY k", 2, false).await;
}

#[tokio::test]
async fn redundant_repartition_grouping_sets_flight() {
    for empty in [false, true] {
        compare_with_reduce(
            "SELECT k, count(*) AS c, sum(v) AS total FROM t GROUP BY GROUPING SETS ((k), ())",
            1,
            empty,
            &[false],
        )
        .await;
    }
}

#[tokio::test]
async fn redundant_repartition_grouping_sets_partial_reduce_keeps_legacy_error() {
    let server = Server::start().await;
    for empty in [false, true] {
        data(&server, empty);
        let source = physical(
            "SELECT k, count(*) AS c, sum(v) AS total FROM t GROUP BY GROUPING SETS ((k), ())",
        )
        .await;
        let off = rewrite_with_reduce(source.clone(), nodes(&server), false, true);
        let on = rewrite_with_reduce(source, nodes(&server), true, true);
        assert_eq!(text(&off), text(&on));
        for plan in [off, on] {
            let error = collect(plan, SessionContext::new().task_ctx())
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("Grouping sets must be expanded before PartialReduce aggregation"),
                "{error}"
            );
        }
    }
    server.finish().await;
}

#[tokio::test]
async fn redundant_repartition_disabled_baseline_flight() {
    let server = Server::start().await;
    for sql in [
        "SELECT k, count(*) AS c, sum(v) AS total FROM t GROUP BY k",
        "SELECT count(*) AS c, sum(v) AS total FROM t",
        "SELECT k, v FROM t ORDER BY v LIMIT 5",
        "SELECT k, count(*) AS c FROM t GROUP BY k UNION ALL SELECT k, count(*) AS c FROM t GROUP BY k",
        "SELECT k, count(*) AS c, sum(v) AS total FROM t GROUP BY GROUPING SETS ((k), ())",
    ] {
        for empty in [false, true] {
            data(&server, empty);
            for reduce in [false, true] {
                let plan = rewrite_with_reduce(physical(sql).await, nodes(&server), false, reduce);
                let plan_text = text(&plan);
                let result = if reduce && sql.contains("GROUPING SETS") {
                    let error = collect(plan, SessionContext::new().task_ctx())
                        .await
                        .unwrap_err()
                        .to_string();
                    assert!(error.contains("Grouping sets must be expanded before PartialReduce"));
                    vec!["baseline PartialReduce grouping sets error".to_string()]
                } else {
                    rows(plan).await
                };
                println!(
                    "RR_BASELINE {}",
                    serde_json::json!({
                        "sql": sql, "empty": empty, "reduce": reduce,
                        "plan": plan_text, "rows": result,
                    })
                );
            }
        }
    }
    server.finish().await;
}

fn input() -> Arc<dyn ExecutionPlan> {
    Arc::new(
        NewEmptyExec::new("t", schema(), None, &[], None, false, schema())
            .with_partitions(TARGET_PARTITIONS),
    )
}

fn hash(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        RepartitionExec::try_new(
            input,
            Partitioning::Hash(vec![col("k", &schema()).unwrap()], TARGET_PARTITIONS),
        )
        .unwrap(),
    )
}

// Construct the baseline boundary directly from f519e9c807's f_up branch.
fn legacy_boundary(
    parent: Arc<dyn ExecutionPlan>,
    nodes: &RemoteScanNodes,
) -> Arc<dyn ExecutionPlan> {
    let input = parent.children()[0].clone();
    let partitions = input.output_partitioning().partition_count();
    let remote = Arc::new(
        RemoteScanExec::new(input, nodes.get_remote_node(&TableReference::bare("t"))).unwrap(),
    );
    let rr = Arc::new(
        RepartitionExec::try_new(remote, Partitioning::RoundRobinBatch(partitions)).unwrap(),
    );
    parent
        .replace_children(
            vec![rr],
            datafusion::physical_plan::execution_plan::ReplaceChildrenOptions::new(
                datafusion::physical_plan::execution_plan::ChildrenPropertiesMode::Recompute,
            ),
        )
        .unwrap()
}

#[tokio::test]
async fn redundant_repartition_boundary_legacy_and_parallelism() {
    let server = Server::start().await;
    data(&server, false);
    for parent in [
        hash(input()),
        Arc::new(CoalescePartitionsExec::new(input())),
    ] {
        let expected = legacy_boundary(parent.clone(), &nodes(&server));
        let off = rewrite(parent.clone(), nodes(&server), false);
        assert_eq!(text(&off), text(&expected));
        let on = rewrite(parent, nodes(&server), true);
        assert_eq!(repartitions(&off), repartitions(&on) + 1);
        assert!(on.children()[0].is::<RemoteScanExec>());
        assert_same_partitioning(&off, &on);
        assert_eq!(remote_scans(&on), 1);
        if on.is::<RepartitionExec>() {
            assert_eq!(
                on.output_partitioning().partition_count(),
                TARGET_PARTITIONS
            );
        } else {
            assert_eq!(on.output_partitioning().partition_count(), 1);
        }
        let expected = rows(off).await;
        let actual = rows(on).await;
        assert_eq!(expected, actual);
        assert_eq!(actual.len(), 24);
    }
    server.finish().await;
}

#[tokio::test]
async fn redundant_repartition_round_robin_preserve_order_and_fetch_stay() {
    let server = Server::start().await;
    let sorted: Arc<dyn ExecutionPlan> = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new_default(
                col("k", &schema()).unwrap(),
            )])
            .unwrap(),
            input(),
        )
        .with_preserve_partitioning(true),
    );
    let preserving: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            sorted,
            Partitioning::Hash(vec![col("k", &schema()).unwrap()], TARGET_PARTITIONS),
        )
        .unwrap()
        .with_preserve_order(),
    );
    assert!(
        preserving
            .downcast_ref::<RepartitionExec>()
            .unwrap()
            .preserve_order()
    );
    let parents: Vec<Arc<dyn ExecutionPlan>> = vec![
        Arc::new(
            RepartitionExec::try_new(input(), Partitioning::RoundRobinBatch(TARGET_PARTITIONS))
                .unwrap(),
        ),
        preserving,
        Arc::new(CoalescePartitionsExec::new(input()).with_fetch(Some(5))),
    ];
    for parent in parents {
        let expected = legacy_boundary(parent.clone(), &nodes(&server));
        let off = rewrite(parent.clone(), nodes(&server), false);
        let on = rewrite(parent, nodes(&server), true);
        assert_eq!(text(&off), text(&expected));
        assert_eq!(text(&on), text(&expected));
    }
    server.finish().await;
}

#[tokio::test]
async fn redundant_repartition_limit_ancestor_and_sibling_are_conservative() {
    let server = Server::start().await;
    for fetch in [Some(5), None] {
        let limited: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(
            Arc::new(CoalescePartitionsExec::new(hash(input()))),
            2,
            fetch,
        ));
        let source = datafusion::physical_plan::union::UnionExec::try_new(vec![
            limited,
            Arc::new(CoalescePartitionsExec::new(hash(input()))),
        ])
        .unwrap();
        let off = rewrite(source.clone(), nodes(&server), false);
        let on = rewrite(source, nodes(&server), true);
        assert_eq!(text(off.children()[0]), text(on.children()[0]));
        assert_eq!(repartitions(&off), repartitions(&on) + 1);
    }
    server.finish().await;
}

#[test]
fn redundant_repartition_no_remote_nodes_keeps_legacy_plan() {
    let nodes = Arc::new(RemoteScanNodes::new(
        config::datafusion::request::Request::default(),
        vec![],
        HashMap::new(),
        HashMap::new(),
        false,
        opentelemetry::Context::current(),
        None,
    ));
    for parent in [
        hash(input()),
        Arc::new(CoalescePartitionsExec::new(input())),
    ] {
        let expected = legacy_boundary(parent.clone(), &nodes);
        let on = rewrite(parent, nodes.clone(), true);
        assert_eq!(text(&on), text(&expected));
    }
}
