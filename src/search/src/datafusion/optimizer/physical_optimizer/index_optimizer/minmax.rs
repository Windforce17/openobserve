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

use config::{TIMESTAMP_COL_NAME, meta::inverted_index::IndexOptimizeMode};
use datafusion::{
    arrow::datatypes::DataType,
    common::{
        Result,
        tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor},
    },
    physical_plan::{ExecutionPlan, aggregates::AggregateExec},
};
use hashbrown::HashSet;

use crate::datafusion::optimizer::physical_optimizer::{
    index_optimizer::utils::is_complex_plan, utils::min_max_column_aggregate,
};

/// M16: check if the plan is `select min(field) from stream` /
/// `select max(field) from stream` — one bare-column min/max aggregate, no
/// grouping — over a NUMERIC column (Int/UInt/Float families; `_timestamp`
/// included). Resolves to [`IndexOptimizeMode::SimpleMinMax`], which the
/// vix evaluation answers from per-chunk exact min/max stats where they
/// exist. String columns never qualify: their stats are conservative
/// PREFIX bounds — prune-only, never answers.
pub fn is_simple_min_max(
    plan: Arc<dyn ExecutionPlan>,
    index_fields: &HashSet<String>,
) -> Option<IndexOptimizeMode> {
    let mut visitor = SimpleMinMaxVisitor::new(index_fields);
    let _ = plan.visit(&mut visitor);
    if visitor.failed || !visitor.numeric_checked {
        None
    } else {
        visitor
            .mode
            .map(|(field, is_max)| IndexOptimizeMode::SimpleMinMax(field, is_max))
    }
}

struct SimpleMinMaxVisitor<'a> {
    index_fields: &'a HashSet<String>,
    /// The (field, is_max) every AggregateExec level agreed on so far.
    mode: Option<(String, bool)>,
    /// Whether some level's INPUT schema carried the column and proved it
    /// numeric (the Partial level does; the Final level's input holds the
    /// aggregate state column instead).
    numeric_checked: bool,
    failed: bool,
}

impl<'a> SimpleMinMaxVisitor<'a> {
    fn new(index_fields: &'a HashSet<String>) -> Self {
        Self {
            index_fields,
            mode: None,
            numeric_checked: false,
            failed: true, // no AggregateExec seen yet
        }
    }
}

fn is_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

impl<'n> TreeNodeVisitor<'n> for SimpleMinMaxVisitor<'_> {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
            let derived = if aggregate.group_expr().is_empty() && aggregate.aggr_expr().len() == 1
            {
                min_max_column_aggregate(&aggregate.aggr_expr()[0]).filter(|(field, _)| {
                    field == TIMESTAMP_COL_NAME || self.index_fields.contains(field)
                })
            } else {
                None
            };
            match derived {
                Some(mode) if self.mode.as_ref().is_none_or(|m| *m == mode) => {
                    // the numeric gate resolves at whichever level still has
                    // the raw column in its input schema (the Partial level)
                    if let Ok(field) = aggregate.input().schema().field_with_name(&mode.0) {
                        if is_numeric(field.data_type()) {
                            self.numeric_checked = true;
                        } else {
                            self.mode = None;
                            self.failed = true;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                    self.mode = Some(mode);
                    self.failed = false;
                }
                _ => {
                    self.mode = None;
                    self.failed = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        } else if is_complex_plan(node) {
            self.mode = None;
            self.failed = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::datafusion::table_provider::empty_table::NewEmptyTable;

    #[tokio::test]
    async fn test_is_simple_min_max() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ctx = SessionContext::new();
        let provider = NewEmptyTable::new("t", schema);
        ctx.register_table("t", Arc::new(provider)).unwrap();
        let fields: HashSet<String> = ["code", "ratio", "name"]
            .into_iter()
            .map(str::to_string)
            .collect();

        let cases = vec![
            (
                "SELECT min(code) from t",
                Some(IndexOptimizeMode::SimpleMinMax("code".into(), false)),
            ),
            (
                "SELECT max(code) as m from t",
                Some(IndexOptimizeMode::SimpleMinMax("code".into(), true)),
            ),
            (
                "SELECT max(ratio) from t",
                Some(IndexOptimizeMode::SimpleMinMax("ratio".into(), true)),
            ),
            (
                "SELECT min(_timestamp) from t",
                Some(IndexOptimizeMode::SimpleMinMax("_timestamp".into(), false)),
            ),
            // string column: prefix-bounded stats never answer
            ("SELECT min(name) from t", None),
            // two aggregates / grouping / distinct: not the simple shape
            ("SELECT min(code), max(code) from t", None),
            ("SELECT name, min(code) from t group by name", None),
            ("SELECT count(code) from t", None),
        ];
        for (sql, expected) in cases {
            let plan = ctx.state().create_logical_plan(sql).await?;
            let physical_plan = ctx.state().create_physical_plan(&plan).await?;
            assert_eq!(expected, is_simple_min_max(physical_plan, &fields), "{sql}");
        }

        // an eligible shape on a field OUTSIDE index_fields is refused
        let plan = ctx
            .state()
            .create_logical_plan("SELECT min(code) from t")
            .await?;
        let physical_plan = ctx.state().create_physical_plan(&plan).await?;
        assert_eq!(is_simple_min_max(physical_plan, &HashSet::new()), None);
        Ok(())
    }
}
