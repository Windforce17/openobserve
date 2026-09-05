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

use std::{collections::HashMap, sync::Arc};

use config::meta::inverted_index::IndexOptimizeMode;
use datafusion::{
    common::{
        Result,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor},
    },
    config::ConfigOptions,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan, aggregates::AggregateExec,
        sorts::sort_preserving_merge::SortPreservingMergeExec,
    },
    sql::TableReference,
};
use hashbrown::HashSet;
use parking_lot::Mutex;

mod count;
mod distinct;
mod histogram;
mod minmax;
mod select;
mod topn;
mod utils;

use crate::datafusion::{
    distributed_plan::{empty_exec::NewEmptyExec, remote_scan_exec::RemoteScanExec},
    optimizer::physical_optimizer::index_optimizer::{
        count::is_simple_count,
        distinct::is_simple_distinct,
        histogram::{is_simple_histogram, is_simple_multi_histogram},
        minmax::is_simple_min_max,
        select::is_simple_select,
        topn::is_simple_topn,
        utils::is_complex_plan,
    },
};

/// this use in query follower to generate [`IndexOptimizeMode`]
/// this is used for optimizer that do not need global information
/// NOTE: use this optimizer in follower only when all filter
/// can be extract to index condition(except _timestamp filter)
///
/// `index_fields` is the stream's `column_store_fields`;
/// `unfiltered_index_fields` are the term-indexed string fields additionally
/// eligible for single-field TopN/Distinct when the query has no condition
/// (served from the term dictionary alone — pilot fix B).
#[derive(Debug)]
pub struct FollowerIndexOptimizerRule {
    time_range: (i64, i64),
    index_fields: HashSet<String>,
    unfiltered_index_fields: HashSet<String>,
    index_optimizer_mode: Arc<Mutex<Option<IndexOptimizeMode>>>,
}

impl FollowerIndexOptimizerRule {
    pub fn new(
        time_range: (i64, i64),
        index_fields: HashSet<String>,
        unfiltered_index_fields: HashSet<String>,
        index_optimizer_mode: Arc<Mutex<Option<IndexOptimizeMode>>>,
    ) -> Self {
        Self {
            time_range,
            index_fields,
            unfiltered_index_fields,
            index_optimizer_mode,
        }
    }
}

impl PhysicalOptimizerRule for FollowerIndexOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config::get_config().common.inverted_index_enabled {
            return Ok(plan);
        }

        let mut rewriter = FollowerIndexOptimizer::new(
            self.time_range,
            self.index_fields.clone(),
            self.unfiltered_index_fields.clone(),
            self.index_optimizer_mode.clone(),
        );
        let plan = plan.rewrite(&mut rewriter)?.data;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "FollowerIndexOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

struct FollowerIndexOptimizer {
    time_range: (i64, i64),
    index_fields: HashSet<String>,
    unfiltered_index_fields: HashSet<String>,
    index_optimizer_mode: Arc<Mutex<Option<IndexOptimizeMode>>>,
}

impl FollowerIndexOptimizer {
    pub fn new(
        time_range: (i64, i64),
        index_fields: HashSet<String>,
        unfiltered_index_fields: HashSet<String>,
        index_optimizer_mode: Arc<Mutex<Option<IndexOptimizeMode>>>,
    ) -> Self {
        Self {
            time_range,
            index_fields,
            unfiltered_index_fields,
            index_optimizer_mode,
        }
    }
}

impl TreeNodeRewriter for FollowerIndexOptimizer {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        if is_complex_plan(&plan) {
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Stop));
        }

        if plan.downcast_ref::<SortPreservingMergeExec>().is_some() {
            // Check for SimpleSelect
            if let Some(index_optimize_mode) = is_simple_select(Arc::clone(&plan)) {
                *self.index_optimizer_mode.lock() = Some(index_optimize_mode);
                return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
            }

            // check if the query is simple topn or simple distinct
            if config::cluster::LOCAL_NODE.is_single_node() {
                if let Some(index_optimize_mode) = is_simple_topn(
                    Arc::clone(&plan),
                    self.index_fields.clone(),
                    self.unfiltered_index_fields.clone(),
                ) {
                    *self.index_optimizer_mode.lock() = Some(index_optimize_mode);
                    return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
                } else if let Some(index_optimize_mode) = is_simple_distinct(
                    Arc::clone(&plan),
                    self.index_fields.clone(),
                    self.unfiltered_index_fields.clone(),
                ) {
                    *self.index_optimizer_mode.lock() = Some(index_optimize_mode);
                    return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
                }
            }
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue));
        } else if plan.downcast_ref::<AggregateExec>().is_some() {
            // Check for SimpleCount / SimpleCountField (M16)
            if let Some(index_optimize_mode) =
                is_simple_count(Arc::clone(&plan), &self.index_fields)
            {
                *self.index_optimizer_mode.lock() = Some(index_optimize_mode);
                return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
            }
            // Check for SimpleMinMax (M16)
            if let Some(index_optimize_mode) =
                is_simple_min_max(Arc::clone(&plan), &self.index_fields)
            {
                *self.index_optimizer_mode.lock() = Some(index_optimize_mode);
                return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
            }
            // Check for SimpleHistogram
            if let Some(index_optimize_mode) =
                is_simple_histogram(Arc::clone(&plan), self.time_range)
            {
                *self.index_optimizer_mode.lock() = Some(index_optimize_mode);
                return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
            }
            // Check for SimpleMultiHistogram
            if let Some(index_optimize_mode) = is_simple_multi_histogram(
                Arc::clone(&plan),
                self.time_range,
                self.index_fields.clone(),
            ) {
                *self.index_optimizer_mode.lock() = Some(index_optimize_mode);
                return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
            }
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue));
        }
        Ok(Transformed::no(plan))
    }
}

/// this use in query leader to generate [`IndexOptimizeMode`]
/// this is used for optimizer that need global information
/// like order and limit
///
/// `index_fields` maps each table to its `column_store_fields`;
/// `unfiltered_index_fields` to the term-indexed string fields additionally
/// eligible for single-field TopN/Distinct when the query has no condition
/// (pilot fix B).
#[derive(Default, Debug)]
pub struct LeaderIndexOptimizerRule {
    index_fields: HashMap<TableReference, HashSet<String>>,
    unfiltered_index_fields: HashMap<TableReference, HashSet<String>>,
}

impl LeaderIndexOptimizerRule {
    pub fn new(
        index_fields: HashMap<TableReference, HashSet<String>>,
        unfiltered_index_fields: HashMap<TableReference, HashSet<String>>,
    ) -> Self {
        Self {
            index_fields,
            unfiltered_index_fields,
        }
    }
}

impl PhysicalOptimizerRule for LeaderIndexOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config::get_config().common.inverted_index_enabled {
            return Ok(plan);
        }

        let mut rewriter = LeaderIndexOptimizer::new(
            self.index_fields.clone(),
            self.unfiltered_index_fields.clone(),
        );
        let plan = plan.rewrite(&mut rewriter)?.data;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "LeaderIndexOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

struct LeaderIndexOptimizer {
    index_fields: HashMap<TableReference, HashSet<String>>,
    unfiltered_index_fields: HashMap<TableReference, HashSet<String>>,
}

impl LeaderIndexOptimizer {
    pub fn new(
        index_fields: HashMap<TableReference, HashSet<String>>,
        unfiltered_index_fields: HashMap<TableReference, HashSet<String>>,
    ) -> Self {
        Self {
            index_fields,
            unfiltered_index_fields,
        }
    }
}

impl TreeNodeRewriter for LeaderIndexOptimizer {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        if is_complex_plan(&plan) {
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Stop));
        }

        if plan.downcast_ref::<SortPreservingMergeExec>().is_some() {
            // Get the index fields of the underlying table
            let mut visitor = TableNameVisitor::new();
            plan.visit(&mut visitor)?;
            let Some(table_name) = visitor.table_name else {
                return Ok(Transformed::new(plan, false, TreeNodeRecursion::Stop));
            };
            let index_fields = self
                .index_fields
                .get(&table_name)
                .cloned()
                .unwrap_or(HashSet::new());
            let unfiltered_index_fields = self
                .unfiltered_index_fields
                .get(&table_name)
                .cloned()
                .unwrap_or(HashSet::new());

            // check if the query is simple topn or simple distinct
            if let Some(index_optimize_mode) = is_simple_topn(
                Arc::clone(&plan),
                index_fields.clone(),
                unfiltered_index_fields.clone(),
            ) {
                // Check for SimpleTopN
                let mut rewriter = IndexOptimizerRewrite::new(index_optimize_mode);
                let plan = plan.rewrite(&mut rewriter)?.data;
                return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
            } else if let Some(index_optimize_mode) = is_simple_distinct(
                Arc::clone(&plan),
                index_fields.clone(),
                unfiltered_index_fields.clone(),
            ) {
                // Check for SimpleDistinct
                let mut rewriter = IndexOptimizerRewrite::new(index_optimize_mode);
                let plan = plan.rewrite(&mut rewriter)?.data;
                return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
            }
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue));
        }
        Ok(Transformed::no(plan))
    }
}

#[derive(Debug)]
struct IndexOptimizerRewrite {
    index_optimizer_mode: IndexOptimizeMode,
}

impl IndexOptimizerRewrite {
    fn new(index_optimizer_mode: IndexOptimizeMode) -> Self {
        IndexOptimizerRewrite {
            index_optimizer_mode,
        }
    }
}

impl TreeNodeRewriter for IndexOptimizerRewrite {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, node: Arc<dyn ExecutionPlan>) -> Result<Transformed<Self::Node>> {
        if let Some(remote) = node.downcast_ref::<RemoteScanExec>() {
            let remote = Arc::new(
                remote
                    .clone()
                    .set_index_optimize_mode(self.index_optimizer_mode.clone()),
            ) as Arc<dyn ExecutionPlan>;
            return Ok(Transformed::new(remote, true, TreeNodeRecursion::Stop));
        }
        Ok(Transformed::no(node))
    }
}

// visit physical plan to get underlying table name
struct TableNameVisitor {
    table_name: Option<TableReference>,
}

impl TableNameVisitor {
    pub fn new() -> Self {
        Self { table_name: None }
    }
}

impl<'n> TreeNodeVisitor<'n> for TableNameVisitor {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        let name = node.name();
        if name == "NewEmptyExec" {
            let table = node.downcast_ref::<NewEmptyExec>().unwrap();
            self.table_name = Some(TableReference::from(table.name()));
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    }
}

/// Follower-fidelity extraction harness for the aggregation fast-path
/// detectors, shared by the detector unit tests and the vix end-to-end
/// differential tests.
///
/// It reproduces the production pipeline end to end: the LEADER plans `sql`
/// with the production custom logical rules that shape aggregate plans
/// ([`RewriteHistogram`] — resolving a 1-arg `histogram(_timestamp)` to a
/// concrete `date_bin` interval literal exactly like production: the preset
/// seconds when the request carries `histogram_interval` (the streaming
/// path pre-computes it from the FULL query range), the
/// `generate_histogram_interval` auto formula otherwise — and
/// [`AddSortAndLimitRule`], which production appends for default-limit
/// queries) plus the [`RemoteScanRule`] leader/follower split; the
/// RemoteScanExec child (the exact sub-plan the leader ships) is then
/// roundtripped through the flight proto codec (what flight.rs `do_get`
/// deserializes) and [`FollowerIndexOptimizerRule`] runs over the received
/// plan with the stream's `column_store_fields`, exactly like
/// `optimizer_physical_plan` on a querier.
#[cfg(test)]
pub(crate) mod test_harness {
    use std::sync::Arc;

    use arrow_schema::Schema;
    use config::meta::inverted_index::IndexOptimizeMode;
    use datafusion::{
        execution::{SessionStateBuilder, runtime_env::RuntimeEnvBuilder},
        physical_optimizer::PhysicalOptimizerRule,
        physical_plan::ExecutionPlan,
        prelude::{SessionConfig, SessionContext},
        sql::TableReference,
    };
    use datafusion_proto::bytes::{
        physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
    };
    use hashbrown::HashSet;
    use parking_lot::Mutex;

    use super::{FollowerIndexOptimizerRule, utils::tests::get_remote_scan};
    use crate::datafusion::{
        distributed_plan::codec::get_physical_extension_codec,
        optimizer::{
            logical_optimizer::{
                add_sort_and_limit::AddSortAndLimitRule, rewrite_histogram::RewriteHistogram,
            },
            physical_optimizer::remote_scan::RemoteScanRule,
        },
        table_provider::empty_table::NewEmptyTable,
        udf::histogram_udf,
    };

    /// Number of rows `AddSortAndLimitRule` is created with: production uses
    /// `query_default_limit + 5` when the request has no explicit LIMIT
    /// (the UI histogram request carries `size: -1`).
    const DEFAULT_LIMIT: usize = 1005;

    /// Plans `sql` against table `"t"` with the given schema and returns the
    /// [`IndexOptimizeMode`] a follower extracts from the shipped sub-plan.
    ///
    /// `preset_interval_secs` mirrors `query.histogram_interval`: pass the
    /// streaming path's pre-computed seconds, or 0 for the plain `_search`
    /// auto-interval resolution inside [`RewriteHistogram`].
    pub async fn follower_extracted_mode(
        sql: &str,
        schema: Arc<Schema>,
        time_range: (i64, i64),
        preset_interval_secs: i64,
        column_store_fields: HashSet<String>,
    ) -> Option<IndexOptimizeMode> {
        let mut file_id_lists = hashbrown::HashMap::new();
        file_id_lists.insert(TableReference::from("t"), vec![]);

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_default_features()
            .with_optimizer_rule(Arc::new(RewriteHistogram::new(
                time_range.0,
                time_range.1,
                preset_interval_secs,
                None,
            )))
            .with_optimizer_rule(Arc::new(AddSortAndLimitRule::new(DEFAULT_LIMIT, 0)))
            .with_physical_optimizer_rule(Arc::new(RemoteScanRule::new_test(file_id_lists, false)))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("t", Arc::clone(&schema)).with_partitions(12);
        ctx.register_table("t", Arc::new(provider)).unwrap();
        ctx.register_udf(histogram_udf::HISTOGRAM_UDF.clone());

        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let physical_plan = ctx.state().create_physical_plan(&plan).await.unwrap();

        // the sub-plan the leader ships to followers
        let remote_scans = get_remote_scan(physical_plan);
        assert_eq!(remote_scans.len(), 1, "expected one RemoteScanExec: {sql}");
        let shipped = Arc::clone(remote_scans[0].children()[0]);

        // proto roundtrip: exactly what flight.rs do_get receives
        let codec = get_physical_extension_codec();
        let bytes = physical_plan_to_bytes_with_extension_codec(shipped, &codec).unwrap();
        let follower_plan =
            physical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &codec).unwrap();

        let mode = Arc::new(Mutex::new(None));
        let rule = FollowerIndexOptimizerRule::new(
            time_range,
            column_store_fields,
            HashSet::new(),
            mode.clone(),
        );
        let _ = rule
            .optimize(follower_plan, ctx.state().config_options())
            .unwrap();
        mode.lock().clone()
    }
}

#[cfg(test)]
mod tests {

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        execution::{SessionStateBuilder, runtime_env::RuntimeEnvBuilder},
        physical_plan::ExecutionPlan,
        prelude::{SessionConfig, SessionContext},
    };

    use super::*;
    use crate::datafusion::{
        distributed_plan::node::RemoteScanNode,
        optimizer::physical_optimizer::{
            index_optimizer::utils::tests::get_remote_scan, remote_scan::RemoteScanRule,
        },
        table_provider::empty_table::NewEmptyTable,
    };

    #[test]
    fn test_table_name_visitor_extracts_table_name_from_new_empty_exec() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(NewEmptyExec::new(
            "my_table",
            schema.clone(),
            None,
            &[],
            None,
            false,
            schema.clone(),
        ));

        let mut visitor = TableNameVisitor::new();
        plan.visit(&mut visitor).unwrap();
        let table = visitor.table_name.expect("table name should be found");
        assert_eq!(
            table.to_string(),
            TableReference::from("my_table").to_string()
        );
    }

    #[test]
    fn test_index_optimizer_rewrite_transforms_remote_scan_exec() {
        // Build a minimal child plan and a default RemoteScanExec
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let child: Arc<dyn ExecutionPlan> = Arc::new(NewEmptyExec::new(
            "child",
            schema.clone(),
            None,
            &[],
            None,
            false,
            schema.clone(),
        ));

        let remote_node = RemoteScanNode::default();
        let remote = RemoteScanExec::new(Arc::clone(&child), remote_node)
            .expect("construct remote scan exec");
        let plan: Arc<dyn ExecutionPlan> = Arc::new(remote);

        // Apply rewrite with a concrete mode and assert it reports transformed=true
        let mode = IndexOptimizeMode::SimpleTopN(vec!["field".to_string()], 10, true);
        let mut rewriter = IndexOptimizerRewrite::new(mode.clone());
        let result = plan.rewrite(&mut rewriter).unwrap();
        assert!(result.transformed, "plan should be marked as transformed");
        assert_eq!(result.data.name(), "RemoteScanExec");
        let remote_scan = result.data.downcast_ref::<RemoteScanExec>().unwrap();
        assert_eq!(remote_scan.index_optimize_mode(), Some(mode));
    }

    #[tokio::test]
    async fn test_follower_rule_sets_mode_for_simple_select() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let index_optimizer_mode = Arc::new(Mutex::new(None));
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_physical_optimizer_rule(Arc::new(FollowerIndexOptimizerRule::new(
                (0, 0),
                HashSet::new(),
                HashSet::new(),
                index_optimizer_mode.clone(),
            )))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("t", schema.clone()).with_partitions(12);
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let sql = "SELECT * FROM t ORDER BY _timestamp DESC LIMIT 10";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let _plan = ctx.state().create_physical_plan(&plan).await.unwrap();

        assert_eq!(
            index_optimizer_mode.lock().clone(),
            Some(IndexOptimizeMode::SimpleSelect(10, false))
        );
    }

    #[tokio::test]
    async fn test_follower_rule_sets_mode_for_simple_count() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let index_optimizer_mode = Arc::new(Mutex::new(None));
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_physical_optimizer_rule(Arc::new(FollowerIndexOptimizerRule::new(
                (0, 0),
                HashSet::new(),
                HashSet::new(),
                index_optimizer_mode.clone(),
            )))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("t", schema.clone()).with_partitions(12);
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let sql = "SELECT count(*) FROM t";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let _plan = ctx.state().create_physical_plan(&plan).await.unwrap();

        assert_eq!(
            index_optimizer_mode.lock().clone(),
            Some(IndexOptimizeMode::SimpleCount)
        );
    }

    /// M16: the follower extracts count(field)/min-max from the SHIPPED
    /// sub-plan (post proto roundtrip — one Partial AggregateExec), with
    /// the eligibility and numeric gates applied.
    #[tokio::test]
    async fn test_follower_extracts_m16_modes() {
        use super::test_harness::follower_extracted_mode;

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let fields: HashSet<String> = ["code".to_string(), "name".to_string()]
            .into_iter()
            .collect();
        let time_range = (1_000i64, 2_000i64);
        let cases = vec![
            (
                "SELECT count(name) FROM t",
                Some(IndexOptimizeMode::SimpleCountField("name".to_string())),
            ),
            (
                "SELECT count(code) as c FROM t",
                Some(IndexOptimizeMode::SimpleCountField("code".to_string())),
            ),
            (
                "SELECT min(code) FROM t",
                Some(IndexOptimizeMode::SimpleMinMax("code".to_string(), false)),
            ),
            (
                "SELECT max(code) FROM t",
                Some(IndexOptimizeMode::SimpleMinMax("code".to_string(), true)),
            ),
            (
                "SELECT max(_timestamp) FROM t",
                Some(IndexOptimizeMode::SimpleMinMax(
                    "_timestamp".to_string(),
                    true,
                )),
            ),
            // strings are prefix-bounded: min/max never fast-paths them
            ("SELECT min(name) FROM t", None),
            // count(*) keeps its own mode
            (
                "SELECT count(*) FROM t",
                Some(IndexOptimizeMode::SimpleCount),
            ),
        ];
        for (sql, expected) in cases {
            let mode =
                follower_extracted_mode(sql, Arc::clone(&schema), time_range, 0, fields.clone())
                    .await;
            assert_eq!(mode, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn test_leader_rule_topn() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
        ]));
        let mut index_fields: HashMap<TableReference, HashSet<String>> = HashMap::new();
        index_fields.insert(
            TableReference::from("t"),
            HashSet::from(["name".to_string()]),
        );

        let mut file_id_lists = hashbrown::HashMap::new();
        file_id_lists.insert(TableReference::from("t"), vec![]);
        let remote_scan_rule = RemoteScanRule::new_test(file_id_lists, false);

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_physical_optimizer_rule(Arc::new(remote_scan_rule))
            .with_physical_optimizer_rule(Arc::new(LeaderIndexOptimizerRule::new(
                index_fields,
                HashMap::new(),
            )))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("t", schema.clone()).with_partitions(12);
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let sql = "select name, count(*) as cnt from t group by name order by cnt desc limit 10";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let plan = ctx.state().create_physical_plan(&plan).await.unwrap();

        let remote_scan = get_remote_scan(plan);
        assert_eq!(
            remote_scan[0].index_optimize_mode(),
            Some(IndexOptimizeMode::SimpleTopN(
                vec!["name".to_string()],
                10,
                false
            ))
        )
    }

    #[tokio::test]
    async fn test_leader_rule_topn_requires_column_store_field() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
        ]));
        // `name` is not in the stream's column_store_fields (the per-table
        // set is empty), so the group field is not fast-path eligible
        let mut index_fields: HashMap<TableReference, HashSet<String>> = HashMap::new();
        index_fields.insert(TableReference::from("t"), HashSet::new());

        let mut file_id_lists = hashbrown::HashMap::new();
        file_id_lists.insert(TableReference::from("t"), vec![]);
        let remote_scan_rule = RemoteScanRule::new_test(file_id_lists, false);

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_physical_optimizer_rule(Arc::new(remote_scan_rule))
            .with_physical_optimizer_rule(Arc::new(LeaderIndexOptimizerRule::new(
                index_fields,
                HashMap::new(),
            )))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("t", schema.clone()).with_partitions(12);
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let sql = "select name, count(*) as cnt from t group by name order by cnt desc limit 10";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let plan = ctx.state().create_physical_plan(&plan).await.unwrap();

        let remote_scan = get_remote_scan(plan);
        assert_eq!(remote_scan[0].index_optimize_mode(), None);
    }

    #[tokio::test]
    async fn test_leader_rule_distinct() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
        ]));
        let mut index_fields: HashMap<TableReference, HashSet<String>> = HashMap::new();
        index_fields.insert(
            TableReference::from("t"),
            HashSet::from(["name".to_string()]),
        );

        let mut file_id_lists = hashbrown::HashMap::new();
        file_id_lists.insert(TableReference::from("t"), vec![]);
        let remote_scan_rule = RemoteScanRule::new_test(file_id_lists, false);

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_physical_optimizer_rule(Arc::new(remote_scan_rule))
            .with_physical_optimizer_rule(Arc::new(LeaderIndexOptimizerRule::new(
                index_fields,
                HashMap::new(),
            )))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("t", schema.clone()).with_partitions(12);
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let sql = "select distinct name from t group by name order by name limit 10";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let plan = ctx.state().create_physical_plan(&plan).await.unwrap();

        let remote_scan = get_remote_scan(plan);
        assert_eq!(
            remote_scan[0].index_optimize_mode(),
            Some(IndexOptimizeMode::SimpleDistinct(
                "name".to_string(),
                10,
                true
            ))
        )
    }

    #[tokio::test]
    async fn test_leader_rule_subquery() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("kubernetes_namespace_name", DataType::Utf8, false),
            Field::new("kubernetes_container_name", DataType::Utf8, false),
            Field::new("log", DataType::Utf8, false),
        ]));
        let mut index_fields: HashMap<TableReference, HashSet<String>> = HashMap::new();
        index_fields.insert(
            TableReference::from("t"),
            HashSet::from(["kubernetes_namespace_name".to_string()]),
        );

        let mut file_id_lists = hashbrown::HashMap::new();
        file_id_lists.insert(TableReference::from("t"), vec![]);
        let remote_scan_rule = RemoteScanRule::new_test(file_id_lists, false);

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_physical_optimizer_rule(Arc::new(remote_scan_rule))
            .with_physical_optimizer_rule(Arc::new(LeaderIndexOptimizerRule::new(
                index_fields,
                HashMap::new(),
            )))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("t", schema.clone()).with_partitions(12);
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let sql = "select kubernetes_namespace_name,
                                      array_agg(distinct kubernetes_container_name) as container_name
                                    from t
                                    where log like '%zinc%'
                                    and kubernetes_namespace_name in (
                                        select distinct kubernetes_namespace_name
                                        from t
                                        order by kubernetes_namespace_name limit 10000)
                                    group by kubernetes_namespace_name
                                    order by kubernetes_namespace_name
                                    limit 10";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let plan = ctx.state().create_physical_plan(&plan).await.unwrap();

        let remote_scan = get_remote_scan(plan);
        assert_eq!(
            remote_scan[0].index_optimize_mode(),
            Some(IndexOptimizeMode::SimpleDistinct(
                "kubernetes_namespace_name".to_string(),
                10000,
                true
            ))
        )
    }
}
