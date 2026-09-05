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

use config::meta::inverted_index::IndexOptimizeMode;
use datafusion::{
    common::{
        Result,
        tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor},
    },
    physical_plan::{
        ExecutionPlan,
        aggregates::{AggregateExec, AggregateInputMode},
    },
};
use hashbrown::HashSet;

use crate::datafusion::optimizer::physical_optimizer::{
    index_optimizer::utils::{count_rows_input_is_original, is_complex_plan},
    utils::{count_column_aggregate, is_count_rows_aggregate},
};

#[rustfmt::skip]
/// check if the plan is like:
/// select count(*) from stream
/// or select count(*) as cnt from stream
/// example plan:
///   ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
///     GlobalLimitExec: skip=0, fetch=100
///       AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
///         CoalescePartitionsExec
///           AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
///             ProjectionExec: expr=[]
///               CoalesceBatchesExec: target_batch_size=8192
///                 FilterExec: _timestamp@0 >= 175256100000000 AND _timestamp@0 < 17525610000000000
///                   CooperativeExec
///                     NewEmptyExec: name="default"
///
/// M16: `select count(field)` over a bare column in `index_fields` (the
/// stream's fast-path-eligible fields) additionally resolves to
/// [`IndexOptimizeMode::SimpleCountField`] — the null-skipping per-column
/// count, stats-answered from per-chunk presence counts.
pub fn is_simple_count(
    plan: Arc<dyn ExecutionPlan>,
    index_fields: &HashSet<String>,
) -> Option<IndexOptimizeMode> {
    let mut visitor = SimpleCountVisitor::new(index_fields);
    let _ = plan.visit(&mut visitor);
    if visitor.failed || !visitor.raw_input_proven { None } else { visitor.mode }
}

struct SimpleCountVisitor<'a> {
    index_fields: &'a HashSet<String>,
    /// The mode every AggregateExec level agreed on so far.
    mode: Option<IndexOptimizeMode>,
    failed: bool,
    raw_input_proven: bool,
}

impl<'a> SimpleCountVisitor<'a> {
    pub fn new(index_fields: &'a HashSet<String>) -> Self {
        Self {
            index_fields,
            mode: None,
            failed: true, // no AggregateExec seen yet
            raw_input_proven: false,
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for SimpleCountVisitor<'_> {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
            let derived = if aggregate.group_expr().is_empty()
                && aggregate.aggr_expr().len() == 1
                && aggregate.filter_expr().iter().all(Option::is_none)
            {
                let expr = &aggregate.aggr_expr()[0];
                if is_count_rows_aggregate(expr) && count_rows_input_is_original(aggregate) {
                    Some(IndexOptimizeMode::SimpleCount)
                } else {
                    count_column_aggregate(expr)
                        .filter(|column| self.index_fields.contains(column))
                        .map(IndexOptimizeMode::SimpleCountField)
                }
            } else {
                None
            };
            // every AggregateExec level (Final + Partial) must derive the
            // SAME mode
            match derived {
                Some(mode) if self.mode.as_ref().is_none_or(|m| *m == mode) => {
                    self.mode = Some(mode);
                    self.failed = false;
                    self.raw_input_proven |=
                        aggregate.mode().input_mode() == AggregateInputMode::Raw;
                }
                _ => {
                    self.mode = None;
                    self.failed = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        } else if is_complex_plan(node) {
            // if encounter complex plan, stop visiting
            self.mode = None;
            self.failed = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{common::Result, prelude::SessionContext};

    use super::*;
    use crate::datafusion::table_provider::empty_table::NewEmptyTable;

    #[tokio::test]
    async fn test_is_simple_count() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ctx = SessionContext::new();
        let provider = NewEmptyTable::new("t", schema);
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let fields: HashSet<String> = ["name".to_string()].into_iter().collect();
        let cases = vec![
            (
                "SELECT count(*) from t",
                Some(IndexOptimizeMode::SimpleCount),
            ),
            (
                "SELECT count(*) as cnt from t",
                Some(IndexOptimizeMode::SimpleCount),
            ),
            (
                "SELECT count(_timestamp) from t",
                Some(IndexOptimizeMode::SimpleCount),
            ),
            (
                "SELECT count(_timestamp) as cnt from t",
                Some(IndexOptimizeMode::SimpleCount),
            ),
            ("SELECT name, count(*) as cnt from t group by name", None),
            // M16: count(field) over an eligible column
            (
                "SELECT count(name) from t",
                Some(IndexOptimizeMode::SimpleCountField("name".to_string())),
            ),
            (
                "SELECT count(name) as cnt from t",
                Some(IndexOptimizeMode::SimpleCountField("name".to_string())),
            ),
            // distinct is not the simple count shape
            ("SELECT count(distinct name) from t", None),
        ];

        for (sql, expected) in cases {
            let plan = ctx.state().create_logical_plan(sql).await?;
            let physical_plan = ctx.state().create_physical_plan(&plan).await?;

            assert_eq!(expected, is_simple_count(physical_plan, &fields), "{sql}");
        }

        // count(field) on a column OUTSIDE index_fields is refused
        let plan = ctx
            .state()
            .create_logical_plan("SELECT count(name) from t")
            .await?;
        let physical_plan = ctx.state().create_physical_plan(&plan).await?;
        assert_eq!(is_simple_count(physical_plan, &HashSet::new()), None);

        Ok(())
    }

    #[test]
    fn test_is_simple_count_returns_none_for_empty_exec() {
        use datafusion::physical_plan::empty::EmptyExec;
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "a",
            arrow_schema::DataType::Int32,
            false,
        )]));
        let plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
            Arc::new(EmptyExec::new(schema));
        assert!(is_simple_count(plan, &HashSet::new()).is_none());
    }
}
