// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Leader plans must pass DataFusion's final plan check.
//!
//! Since DataFusion 56 the physical planner runs `check_plan_tree_sanity` once
//! after every physical optimizer rule, including the rules OpenObserve appends
//! (`RemoteScanRule`, `LeaderIndexOptimizerRule`, ...). DataFusion 54 ran
//! `SanityCheckPlan` as the last default rule, before the appended ones, so a
//! leader plan whose operators declare weaker properties than they deliver
//! was never rejected. These tests plan the leader side exactly like
//! `run_datafusion` (`core::search::cluster::flight`) does — `Sql::new`,
//! `SearchContextBuilder` with a remote-scan context, `register_table`,
//! `create_physical_plan` — for every benchmark query shape and assert that
//! planning succeeds.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use config::{
    datafusion::request::Request,
    meta::{cluster::NodeInfo, stream::StreamType},
};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use hashbrown::HashMap;
use proto::cluster_rpc::SearchQuery;

use super::remote_scan::test_cluster_role;
use crate::{
    datafusion::{
        context::{SearchContextBuilder, register_table},
        distributed_plan::remote_scan_exec::RemoteScanExec,
        optimizer::{
            context::{PhysicalOptimizerContext, RemoteScanContext},
            create_physical_plan,
            logical_optimizer::fuse_approx_percentiles::FuseApproxPercentiles,
        },
        udaf::shared_percentile::NAME as SHARED_PERCENTILES_NAME,
    },
    sql::Sql,
};

const ORG: &str = "leader_plan_sanity";
const SCHEMA_TS: i64 = 1_752_660_674_351_000;
const END_TIME: i64 = 1_752_660_674_351_000;
const HOUR_MICROS: i64 = 3_600_000_000;

#[derive(Debug)]
struct TestNode {
    name: String,
    ingester: bool,
}

impl NodeInfo for TestNode {
    fn is_querier(&self) -> bool {
        !self.ingester
    }

    fn is_ingester(&self) -> bool {
        self.ingester
    }

    fn get_grpc_addr(&self) -> String {
        format!("{}:5081", self.name)
    }

    fn get_auth_token(&self) -> String {
        String::new()
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn is_local(&self) -> bool {
        false
    }
}

/// A cluster layout the leader can plan for.
struct Cluster {
    name: &'static str,
    queriers: usize,
    ingesters: usize,
    /// A querier in a multi-node deployment (dev/prod); `false` is the
    /// single all-roles node, which takes the single-node optimization.
    multi_node: bool,
}

const CLUSTERS: &[Cluster] = &[
    Cluster {
        name: "multi-node: 3 queriers + 1 ingester",
        queriers: 3,
        ingesters: 1,
        multi_node: true,
    },
    Cluster {
        name: "multi-node: 1 querier",
        queriers: 1,
        ingesters: 0,
        multi_node: true,
    },
    Cluster {
        name: "single node",
        queriers: 1,
        ingesters: 0,
        multi_node: false,
    },
];

/// Benchmark queries (`perf13_ab.py`) plus additional shapes production
/// traffic uses: top-N with every sort direction, OFFSET, aggregates with
/// ORDER BY/LIMIT/HAVING, DISTINCT, multiple COUNT(DISTINCT), approximate
/// aggregates, window functions, UNION ALL, joins and subqueries.
const QUERIES: &[(&str, &str)] = &[
    // benchmark set
    (
        "h_server",
        "SELECT histogram(_timestamp, '1 minute') AS m, count(*) AS c FROM \"default\" WHERE service_name = 'manus-node-server' GROUP BY m ORDER BY m",
    ),
    (
        "c_workers",
        "SELECT count(*) AS c FROM \"default\" WHERE service_name = 'manus-node-workers'",
    ),
    (
        "s_svc_strmatch_or",
        "SELECT count(*) AS c FROM \"default\" WHERE str_match(service_name, 'node') OR str_match(service_name, 'biz') OR str_match(service_name, 'worker')",
    ),
    (
        "s_svc_like_or",
        "SELECT count(*) AS c FROM \"default\" WHERE service_name LIKE '%server%' OR service_name LIKE '%gateway%'",
    ),
    (
        "s_svc_or_hist",
        "SELECT histogram(_timestamp, '1 minute') AS m, count(*) AS c FROM \"default\" WHERE str_match(service_name, 'node') OR str_match(service_name, 'worker') GROUP BY m ORDER BY m",
    ),
    (
        "s_body_or",
        "SELECT count(*) AS c FROM \"default\" WHERE str_match(body, 'timeout') OR str_match(body, 'refused')",
    ),
    (
        "w_top",
        "SELECT * FROM \"default\" ORDER BY _timestamp DESC LIMIT 100",
    ),
    (
        "w_server_top",
        "SELECT * FROM \"default\" WHERE service_name = 'manus-node-server' ORDER BY _timestamp DESC LIMIT 100",
    ),
    (
        "w_timeout_top",
        "SELECT * FROM \"default\" WHERE match_all('timeout') ORDER BY _timestamp DESC LIMIT 100",
    ),
    (
        "w_body_like",
        "SELECT count(*) AS c FROM \"default\" WHERE body LIKE '%perf10zz%'",
    ),
    (
        "m_server_err_top",
        "SELECT * FROM \"default\" WHERE service_name = 'manus-node-server' AND match_all('error') ORDER BY _timestamp DESC LIMIT 200",
    ),
    (
        "t2_narrow_top200",
        "SELECT _timestamp, service_name, level FROM \"default\" ORDER BY _timestamp DESC, service_name ASC, level ASC LIMIT 200",
    ),
    (
        "t2_trace_top200",
        "SELECT _timestamp, trace_id FROM \"default\" ORDER BY _timestamp DESC, trace_id ASC LIMIT 200",
    ),
    (
        "t2_body_length",
        "SELECT count(body) AS c, sum(length(body)) AS chars FROM \"default\"",
    ),
    (
        "t2_svc_length",
        "SELECT service_name, count(*) AS c, sum(length(body)) AS chars FROM \"default\" GROUP BY service_name ORDER BY service_name LIMIT 100000",
    ),
    (
        "t4_uri_like",
        "SELECT count(*) AS c FROM \"s3_access_logs\" WHERE request_uri LIKE '%cloud-computer%'",
    ),
    (
        "t3_task_narrow",
        "SELECT _timestamp, service_name, body FROM \"default\" WHERE str_match(body, 'task') ORDER BY _timestamp DESC LIMIT 200",
    ),
    (
        "o1_trace_d2",
        "SELECT trace_id AS t, count(*) AS n, count(DISTINCT service_name) AS svcs, count(DISTINCT level) AS lv FROM \"default\" GROUP BY trace_id ORDER BY n DESC, t ASC LIMIT 40",
    ),
    (
        "o1_svc_d1",
        "SELECT service_name AS s, count(DISTINCT trace_id) AS traces FROM \"default\" GROUP BY service_name ORDER BY traces DESC, s ASC LIMIT 40",
    ),
    (
        "apisix_oom",
        "SELECT client_ip AS ip, count(*) AS n, count(DISTINCT user_id) AS users, count(DISTINCT path) AS paths FROM \"apisix\" GROUP BY client_ip ORDER BY n DESC LIMIT 40",
    ),
    // top-N and plain scans
    (
        "top_asc",
        "SELECT * FROM \"default\" ORDER BY _timestamp ASC LIMIT 100",
    ),
    (
        "top_offset",
        "SELECT * FROM \"default\" ORDER BY _timestamp DESC LIMIT 100 OFFSET 200",
    ),
    (
        "top_non_time",
        "SELECT * FROM \"default\" ORDER BY status DESC LIMIT 50",
    ),
    (
        "top_expr",
        "SELECT _timestamp, body FROM \"default\" ORDER BY length(body) DESC LIMIT 20",
    ),
    (
        "scan_no_order",
        "SELECT * FROM \"default\" WHERE level = 'error' LIMIT 100",
    ),
    (
        "scan_no_limit",
        "SELECT _timestamp, body FROM \"default\" WHERE level = 'error'",
    ),
    (
        "top_regexp",
        "SELECT * FROM \"default\" WHERE re_match(body, 'time.?out') ORDER BY _timestamp DESC LIMIT 100",
    ),
    // aggregates
    ("agg_count_all", "SELECT count(*) FROM \"default\""),
    (
        "agg_min_max_ts",
        "SELECT min(_timestamp) AS a, max(_timestamp) AS b FROM \"default\"",
    ),
    (
        "agg_topk",
        "SELECT service_name, count(*) AS c FROM \"default\" GROUP BY service_name ORDER BY c DESC LIMIT 10",
    ),
    (
        "agg_multi_key",
        "SELECT service_name, level, count(*) AS c, avg(duration) AS d FROM \"default\" GROUP BY service_name, level ORDER BY service_name, level",
    ),
    (
        "agg_having",
        "SELECT service_name, count(*) AS c FROM \"default\" GROUP BY service_name HAVING count(*) > 10 ORDER BY c DESC",
    ),
    (
        "agg_case_rate",
        "SELECT histogram(_timestamp, '1 minute') AS minute, count(*) AS requests, SUM(CASE WHEN status IN (500, 502, 503, 504) THEN 1 ELSE 0 END) AS errors, 100.0 * SUM(CASE WHEN status IN (500, 502, 503, 504) THEN 1 ELSE 0 END) / count(*) AS error_percent FROM \"default\" WHERE service_name = 'manus-node-server' AND body = 'HTTP request' GROUP BY histogram(_timestamp, '1 minute') ORDER BY minute",
    ),
    (
        "agg_distinct",
        "SELECT DISTINCT service_name FROM \"default\" ORDER BY service_name LIMIT 100",
    ),
    (
        "agg_distinct_nolimit",
        "SELECT DISTINCT level FROM \"default\"",
    ),
    (
        "agg_count_distinct_single",
        "SELECT count(DISTINCT trace_id) FROM \"default\"",
    ),
    (
        "agg_approx",
        "SELECT service_name, approx_distinct(trace_id) AS t, approx_percentile_cont(duration, 0.99) AS p99 FROM \"default\" GROUP BY service_name ORDER BY t DESC LIMIT 20",
    ),
    (
        "percentiles_by_service",
        "SELECT service_name, approx_percentile_cont(duration, 0.5) AS p50, approx_percentile_cont(duration, 0.95) AS p95, approx_percentile_cont(duration, 0.99) AS p99 FROM \"default\" GROUP BY service_name ORDER BY p99 DESC LIMIT 20",
    ),
    (
        "percentiles_histogram",
        "SELECT histogram(_timestamp, '1 minute') AS ts, approx_percentile_cont(duration, 0.5) AS p50, approx_percentile_cont(duration, 0.99) AS p99 FROM \"default\" WHERE service_name = 'svc-1' GROUP BY ts ORDER BY ts",
    ),
    (
        "agg_offset",
        "SELECT service_name, count(*) AS c FROM \"default\" GROUP BY service_name ORDER BY c DESC LIMIT 10 OFFSET 10",
    ),
    (
        "agg_hist_top",
        "SELECT histogram(_timestamp) AS h, service_name, count(*) AS c FROM \"default\" GROUP BY h, service_name ORDER BY h DESC, c DESC LIMIT 100",
    ),
    // windows, unions, joins and subqueries
    (
        "window_row_number",
        "SELECT * FROM (SELECT service_name, _timestamp, row_number() OVER (PARTITION BY service_name ORDER BY _timestamp DESC) AS rn FROM \"default\") WHERE rn <= 3",
    ),
    (
        "cte_agg_order",
        "WITH s AS (SELECT service_name, count(*) AS c FROM \"default\" GROUP BY service_name) SELECT * FROM s ORDER BY c DESC LIMIT 5",
    ),
    (
        "nested_top",
        "SELECT * FROM (SELECT _timestamp, service_name FROM \"default\" ORDER BY _timestamp DESC LIMIT 1000) ORDER BY service_name LIMIT 10",
    ),
    (
        "union_all_top",
        "SELECT _timestamp, service_name FROM \"default\" WHERE level = 'error' UNION ALL SELECT _timestamp, service_name FROM \"default\" WHERE level = 'warn' ORDER BY _timestamp DESC LIMIT 100",
    ),
    (
        "union_all_agg",
        "SELECT 'a' AS k, count(*) AS c FROM \"default\" UNION ALL SELECT 'b' AS k, count(*) AS c FROM \"apisix\"",
    ),
    (
        "join_inner",
        "SELECT a.trace_id, a.service_name, b.path FROM \"default\" a JOIN \"apisix\" b ON a.trace_id = b.user_id ORDER BY a._timestamp DESC LIMIT 100",
    ),
    (
        "join_left_agg",
        "SELECT a.service_name, count(b.path) AS c FROM \"default\" a LEFT JOIN \"apisix\" b ON a.trace_id = b.user_id GROUP BY a.service_name ORDER BY c DESC LIMIT 10",
    ),
    (
        "in_subquery",
        "SELECT * FROM \"default\" WHERE trace_id IN (SELECT user_id FROM \"apisix\" WHERE status = 500) ORDER BY _timestamp DESC LIMIT 100",
    ),
    (
        "scalar_subquery",
        "SELECT count(*) FROM \"default\" WHERE duration > (SELECT avg(duration) FROM \"default\")",
    ),
    (
        "exists_subquery",
        "SELECT service_name FROM \"default\" a WHERE EXISTS (SELECT 1 FROM \"apisix\" b WHERE b.user_id = a.trace_id) LIMIT 10",
    ),
];

async fn init_streams() {
    infra::db_init().await.unwrap();
    let default_schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
        Field::new("duration", DataType::Float64, true),
    ]);
    let apisix_schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("client_ip", DataType::Utf8, true),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
    ]);
    let s3_schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("request_uri", DataType::Utf8, true),
        Field::new("key", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
    ]);
    for (stream, schema) in [
        ("default", &default_schema),
        ("apisix", &apisix_schema),
        ("s3_access_logs", &s3_schema),
    ] {
        infra::schema::merge(ORG, stream, StreamType::Logs, schema, Some(SCHEMA_TS))
            .await
            .unwrap();
    }
}

fn search_query(sql: &str, size: i32) -> SearchQuery {
    SearchQuery {
        sql: sql.to_string(),
        quick_mode: false,
        from: 0,
        size,
        start_time: END_TIME - HOUR_MICROS,
        end_time: END_TIME,
        track_total_hits: false,
        query_type: "UI".to_string(),
        uses_zo_fn: false,
        query_fn: "".to_string(),
        skip_wal: false,
        action_id: "".to_string(),
        histogram_interval: 0,
        timezone: None,
        sampling_ratio: None,
    }
}

fn cluster_nodes(cluster: &Cluster) -> Vec<Arc<dyn NodeInfo>> {
    (0..cluster.queriers + cluster.ingesters)
        .map(|i| {
            Arc::new(TestNode {
                name: format!("n{i}"),
                ingester: i >= cluster.queriers,
            }) as Arc<dyn NodeInfo>
        })
        .collect()
}

/// Plans `sql` on the leader exactly like `run_datafusion`.
async fn plan_leader(
    sql: &Sql,
    nodes: Vec<Arc<dyn NodeInfo>>,
    shared_percentiles: bool,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let node_count = nodes.len();
    let mut partitioned_file_lists = HashMap::new();
    for stream in sql.schemas.keys() {
        // every querier holds files, ingesters hold none
        let lists = (0..node_count)
            .map(|i| {
                if nodes[i].is_ingester() {
                    vec![]
                } else {
                    vec![i as i64 + 1]
                }
            })
            .collect::<Vec<_>>();
        partitioned_file_lists.insert(stream.clone(), lists);
    }
    let sql = Arc::new(sql.clone());
    let ctx = SearchContextBuilder::new()
        .target_partitions(8)
        .add_context(PhysicalOptimizerContext::RemoteScan(RemoteScanContext {
            nodes,
            partitioned_file_lists,
            partitioned_bloom_vers: HashMap::new(),
            context: opentelemetry::Context::current(),
            is_leader: false,
        }))
        .add_context(PhysicalOptimizerContext::StreamingAggregation(None))
        .add_context(PhysicalOptimizerContext::AggregateTopk)
        .build(&Request::default(), &sql)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("context: {e}")))?;
    if shared_percentiles {
        // ZO_FEATURE_QUERY_SHARED_PERCENTILES=true in dev/prod appends this as the last
        // logical rule (join_match_one is off); the global config is shared by every test.
        ctx.add_optimizer_rule(Arc::new(FuseApproxPercentiles::new()));
    }
    register_table(&ctx, &sql)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("register_table: {e}")))?;
    create_physical_plan(&ctx, &sql.sql).await
}

fn count_remote_scans(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let own = usize::from(plan.downcast_ref::<RemoteScanExec>().is_some());
    own + plan
        .children()
        .into_iter()
        .map(count_remote_scans)
        .sum::<usize>()
}

#[tokio::test]
async fn leader_plans_pass_the_final_plan_check() {
    init_streams().await;
    let mut failures = Vec::new();
    let mut planned = 0;
    let mut fused_percentile_plans = 0;
    for (name, sql_text) in QUERIES {
        let percentile_variants: &[bool] = if sql_text.contains("approx_percentile_cont") {
            &[false, true]
        } else {
            &[false]
        };
        for &shared_percentiles in percentile_variants {
            for size in [100, -1] {
                let sql = Sql::new(&search_query(sql_text, size), ORG, StreamType::Logs, None)
                    .await
                    .unwrap_or_else(|e| panic!("Sql::new failed for {name}: {e}"));
                for cluster in CLUSTERS {
                    let case = format!(
                        "{name} (size {size}, {}, shared_percentiles={shared_percentiles})",
                        cluster.name
                    );
                    test_cluster_role::set_multi_node(cluster.multi_node);
                    let planned_plan =
                        plan_leader(&sql, cluster_nodes(cluster), shared_percentiles).await;
                    test_cluster_role::set_multi_node(false);
                    match planned_plan {
                        Ok(plan) => {
                            planned += 1;
                            let shown = displayable(plan.as_ref()).indent(false).to_string();
                            assert!(
                                count_remote_scans(&plan) > 0,
                                "{case}: leader plan has no RemoteScanExec:\n{shown}"
                            );
                            if shown.contains(SHARED_PERCENTILES_NAME) {
                                fused_percentile_plans += 1;
                            }
                        }
                        Err(e) => failures.push(format!("{case}: {e}")),
                    }
                }
            }
        }
    }
    // the shared-percentile variant must really plan the fused aggregate
    assert!(
        fused_percentile_plans > 0,
        "no leader plan used {SHARED_PERCENTILES_NAME}"
    );
    assert!(
        failures.is_empty(),
        "{} of {} leader plans failed:\n{}",
        failures.len(),
        failures.len() + planned,
        failures.join("\n")
    );
}
