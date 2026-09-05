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

use arrow_schema::DataType;
use datafusion::{
    common::tree_node::TreeNode,
    physical_plan::{
        ExecutionPlan, PhysicalExpr,
        aggregates::{AggregateExec, AggregateInputMode},
        expressions::{Column, Literal},
        filter::FilterExec,
        projection::ProjectionExec,
    },
};

// check if the plan contains join, union, interleave, unnest, partial sort(use for streaming
// table), bounded window agg, window agg
pub fn is_complex_plan(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.exists(|plan| {
        Ok(plan.name() == "HashJoinExec"
            || plan.name() == "RecursiveQueryExec"
            || plan.name() == "UnionExec"
            || plan.name() == "InterleaveExec"
            || plan.name() == "UnnestExec"
            || plan.name() == "CrossJoinExec"
            || plan.name() == "NestedLoopJoinExec"
            || plan.name() == "SymmetricHashJoinExec"
            || plan.name() == "SortMergeJoinExec"
            || plan.name() == "PartialSortExec"
            || plan.name() == "BoundedWindowAggExec"
            || plan.name() == "WindowAggExec"
            || plan.children().len() > 1)
    })
    .unwrap_or(true)
}

/// A String aggregate wire key is safe only when it represents the same bare
/// logical string column all the way to the scan. Schema type alone is not a
/// proof: `CAST(n AS VARCHAR) AS n` also exposes a Utf8 column named `n`.
pub(super) fn raw_string_group_column<'a>(
    expr: &'a Arc<dyn PhysicalExpr>,
    input: &Arc<dyn ExecutionPlan>,
) -> Option<&'a str> {
    let column = expr.downcast_ref::<Column>()?;
    raw_column_at(input, column.index(), column.name(), |ty| {
        matches!(
            ty,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    })
    .then_some(column.name())
}

/// Prove a bare timestamp column still refers to the original Int64 scan input.
pub(super) fn raw_timestamp_column(
    expr: &Arc<dyn PhysicalExpr>,
    input: &Arc<dyn ExecutionPlan>,
) -> bool {
    let Some(column) = expr.downcast_ref::<Column>() else {
        return false;
    };
    column.name() == config::TIMESTAMP_COL_NAME
        && raw_column_at(input, column.index(), column.name(), |ty| {
            matches!(ty, DataType::Int64)
        })
}

/// Prove the raw input to COUNT(_timestamp) is the original timestamp, not a
/// same-name CASE/CAST projection. Partial-state combiners defer this proof;
/// their caller must also encounter a successfully proven raw-input stage.
pub(super) fn count_rows_input_is_original(aggregate: &AggregateExec) -> bool {
    if aggregate.mode().input_mode() != AggregateInputMode::Raw {
        return true;
    }
    let [expr] = aggregate.aggr_expr() else {
        return false;
    };
    let args = expr.expressions();
    let [arg] = args.as_slice() else {
        return false;
    };
    if let Some(literal) = arg.downcast_ref::<Literal>() {
        return !literal.value().is_null();
    }
    raw_timestamp_column(arg, aggregate.input())
}

fn raw_column_at(
    plan: &Arc<dyn ExecutionPlan>,
    index: usize,
    name: &str,
    accepts_type: fn(&DataType) -> bool,
) -> bool {
    let schema = plan.schema();
    let Some(field) = schema.fields().get(index) else {
        return false;
    };
    if field.name() != name || !accepts_type(field.data_type()) {
        return false;
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let Some(expr) = projection.expr().get(index) else {
            return false;
        };
        let Some(column) = expr.expr.downcast_ref::<Column>() else {
            return false;
        };
        return expr.alias == name
            && column.name() == name
            && raw_column_at(projection.input(), column.index(), name, accepts_type);
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        let source_index = match filter.projection() {
            Some(indices) => match indices.get(index) {
                Some(index) => *index,
                None => return false,
            },
            None => index,
        };
        return raw_column_at(filter.input(), source_index, name, accepts_type);
    }
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        let groups = aggregate.group_expr();
        if groups.groups().len() != 1 || groups.groups()[0].iter().any(|is_null| *is_null) {
            return false;
        }
        let Some((expr, alias)) = groups.expr().get(index) else {
            return false;
        };
        let Some(column) = expr.downcast_ref::<Column>() else {
            return false;
        };
        return alias == name
            && column.name() == name
            && raw_column_at(aggregate.input(), column.index(), name, accepts_type);
    }
    let children = plan.children();
    if children.is_empty() {
        // An unknown or computed leaf is not a logical scan identity proof.
        return matches!(
            plan.name(),
            "NewEmptyExec" | "DataSourceExec" | "MemoryExec"
        );
    }
    if children.len() != 1
        || !matches!(
            plan.name(),
            "CoalesceBatchesExec"
                | "CoalescePartitionsExec"
                | "RepartitionExec"
                | "SortExec"
                | "SortPreservingMergeExec"
                | "CooperativeExec"
                | "RemoteScanExec"
        )
        || schema.as_ref() != children[0].schema().as_ref()
    {
        return false;
    }
    raw_column_at(children[0], index, name, accepts_type)
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::{
            Result,
            tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor},
        },
        physical_plan::{
            ExecutionPlan,
            aggregates::{AggregateExec, AggregateMode},
            empty::EmptyExec,
        },
    };

    use super::*;
    use crate::datafusion::distributed_plan::remote_scan_exec::RemoteScanExec;

    fn empty_plan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        Arc::new(EmptyExec::new(schema))
    }

    #[test]
    fn test_is_complex_plan_empty_exec_returns_false() {
        let plan = empty_plan();
        assert!(!is_complex_plan(&plan));
    }

    #[tokio::test]
    async fn test_group_matchers_require_logical_raw_string_identity() -> Result<()> {
        use config::meta::inverted_index::IndexOptimizeMode;
        use datafusion::prelude::{SessionConfig, SessionContext};
        use hashbrown::HashSet;

        use crate::datafusion::{
            optimizer::physical_optimizer::index_optimizer::{
                distinct::is_simple_distinct, histogram::is_simple_multi_histogram,
                topn::is_simple_topn,
            },
            table_provider::empty_table::NewEmptyTable,
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("f", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        ctx.register_table(
            "t",
            Arc::new(NewEmptyTable::new("t", schema).with_partitions(2)),
        )?;
        for (group, source, eligible) in [
            ("f", "t", true),
            ("n", "t", false),
            ("CAST(f AS BIGINT)", "t", false),
            (
                "n",
                "(SELECT _timestamp, CAST(n AS VARCHAR) AS n FROM t) q",
                false,
            ),
            ("f", "(SELECT _timestamp, lower(f) AS f FROM t) q", false),
            ("f", "(SELECT _timestamp, f FROM t) q", true),
        ] {
            let fields = HashSet::from(["f".to_owned(), "n".to_owned()]);
            let sql = format!(
                "SELECT {group} AS k, count(*) AS cnt FROM {source} GROUP BY k ORDER BY cnt DESC LIMIT 2"
            );
            let logical = ctx.state().create_logical_plan(&sql).await?;
            let physical = ctx.state().create_physical_plan(&logical).await?;
            assert_eq!(
                is_simple_topn(physical, fields.clone(), fields.clone()),
                eligible.then(|| IndexOptimizeMode::SimpleTopN(vec!["f".to_owned()], 2, false)),
                "{sql}"
            );

            let sql =
                format!("SELECT {group} AS k FROM {source} GROUP BY k ORDER BY k ASC LIMIT 2");
            let logical = ctx.state().create_logical_plan(&sql).await?;
            let physical = ctx.state().create_physical_plan(&logical).await?;
            assert_eq!(
                is_simple_distinct(physical, fields.clone(), fields.clone()),
                eligible.then(|| IndexOptimizeMode::SimpleDistinct("f".to_owned(), 2, true)),
                "{sql}"
            );

            let sql = format!(
                "SELECT date_bin(INTERVAL '1 hour', to_timestamp_micros(_timestamp), TIMESTAMP '1970-01-01 00:00:00') AS b, {group} AS k, count(*) AS cnt FROM {source} GROUP BY b, k"
            );
            let logical = ctx.state().create_logical_plan(&sql).await?;
            let physical = ctx.state().create_physical_plan(&logical).await?;
            let partial = Arc::new(get_partial_aggregate_plan(physical).unwrap()) as _;
            assert_eq!(
                is_simple_multi_histogram(partial, (0, 3_600_000_000), fields),
                eligible.then(|| IndexOptimizeMode::SimpleMultiHistogram(
                    0,
                    3_600_000_000,
                    3_600_000_000,
                    0,
                    "f".to_owned()
                )),
                "{sql}"
            );
        }
        // Exercise the precomputed-column case explicitly: the group expression
        // is a bare Utf8 Column, but its source is a same-name numeric cast.
        for (sql, name, eligible) in [
            ("SELECT CAST(n AS VARCHAR) AS n FROM t", "n", false),
            ("SELECT lower(f) AS f FROM t", "f", false),
            ("SELECT f FROM t", "f", true),
        ] {
            let logical = ctx.state().create_logical_plan(sql).await?;
            let input = ctx.state().create_physical_plan(&logical).await?;
            let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(name, 0));
            assert!(matches!(
                input.schema().field(0).data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ));
            assert_eq!(
                raw_string_group_column(&column, &input),
                eligible.then_some(name),
                "{sql}"
            );
            let remote: Arc<dyn ExecutionPlan> =
                Arc::new(RemoteScanExec::new(input, Default::default())?);
            assert_eq!(
                raw_string_group_column(&column, &remote),
                eligible.then_some(name),
                "remote: {sql}"
            );
        }
        let logical = ctx
            .state()
            .create_logical_plan(
                "SELECT f FROM t GROUP BY f ORDER BY CAST(f AS BIGINT) ASC LIMIT 2",
            )
            .await?;
        let physical = ctx.state().create_physical_plan(&logical).await?;
        let fields = HashSet::from(["f".to_owned()]);
        assert_eq!(is_simple_distinct(physical, fields.clone(), fields), None);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_matchers_reject_filtered_and_cast_counts_with_colliding_alias() -> Result<()>
    {
        use datafusion::prelude::{SessionConfig, SessionContext};
        use hashbrown::HashSet;

        use crate::datafusion::{
            optimizer::physical_optimizer::index_optimizer::{
                count::is_simple_count,
                histogram::{is_simple_histogram, is_simple_multi_histogram},
                topn::is_simple_topn,
            },
            table_provider::empty_table::NewEmptyTable,
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, true),
            Field::new("f", DataType::Utf8, true),
        ]));
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        ctx.register_table(
            "t",
            Arc::new(NewEmptyTable::new("t", schema).with_partitions(2)),
        )?;
        let bin = "date_bin(INTERVAL '1 hour', to_timestamp_micros(_timestamp), TIMESTAMP '1970-01-01 00:00:00')";
        let fields = HashSet::from(["f".to_owned()]);
        for (count, source, eligible) in [
            ("COUNT(*)", "t", true),
            ("COUNT(1)", "t", true),
            ("COUNT(_timestamp)", "t", true),
            ("COUNT(_timestamp) FILTER (WHERE f = 'ok')", "t", false),
            ("COUNT(CAST(_timestamp AS TINYINT))", "t", false),
            ("COUNT(DISTINCT _timestamp)", "t", false),
            (
                "COUNT(_timestamp)",
                "(SELECT f, CASE WHEN f='ok' THEN _timestamp ELSE NULL END AS _timestamp FROM t) q",
                false,
            ),
            (
                "COUNT(_timestamp)",
                "(SELECT f, CAST(_timestamp AS TINYINT) AS _timestamp FROM t) q",
                false,
            ),
            ("COUNT(_timestamp)", "(SELECT f, _timestamp FROM t) q", true),
        ] {
            let sql = format!(
                "SELECT f AS key, {count} AS f FROM {source} GROUP BY key ORDER BY f DESC LIMIT 2"
            );
            let logical = ctx.state().create_logical_plan(&sql).await?;
            let physical = ctx.state().create_physical_plan(&logical).await?;
            assert_eq!(
                is_simple_topn(physical, fields.clone(), fields.clone()).is_some(),
                eligible,
                "{sql}"
            );

            let sql =
                format!("SELECT {bin} AS b, f AS key, {count} AS f FROM {source} GROUP BY 1, 2");
            let logical = ctx.state().create_logical_plan(&sql).await?;
            let physical = ctx.state().create_physical_plan(&logical).await?;
            let matched = get_partial_aggregate_plan(physical).and_then(|partial| {
                is_simple_multi_histogram(Arc::new(partial), (0, 3_600_000_000), fields.clone())
            });
            assert_eq!(matched.is_some(), eligible, "{sql}");

            let sql = format!("SELECT {bin} AS b, {count} AS f FROM {source} GROUP BY 1");
            let logical = ctx.state().create_logical_plan(&sql).await?;
            let physical = ctx.state().create_physical_plan(&logical).await?;
            let matched = get_partial_aggregate_plan(physical)
                .and_then(|partial| is_simple_histogram(Arc::new(partial), (0, 3_600_000_000)));
            assert_eq!(matched.is_some(), eligible, "{sql}");

            let sql = format!("SELECT {count} AS f FROM {source}");
            let logical = ctx.state().create_logical_plan(&sql).await?;
            let physical = ctx.state().create_physical_plan(&logical).await?;
            assert_eq!(
                is_simple_count(physical, &fields).is_some(),
                eligible,
                "{sql}"
            );
        }
        // COUNT(*) proves row counting independently of the histogram source.
        // A same-name, same-type timestamp projection must not borrow that proof,
        // either directly or beneath the supported fixed timezone offset.
        for (timestamp, eligible) in [
            ("_timestamp", true),
            ("CASE WHEN f = 'ok' THEN _timestamp ELSE NULL END", false),
            ("CAST(CAST(_timestamp AS TINYINT) AS BIGINT)", false),
        ] {
            let source = format!("(SELECT f, {timestamp} AS _timestamp FROM t) q");
            for timestamp_arg in ["_timestamp", "_timestamp + 3600000000"] {
                let bin = format!(
                    "date_bin(INTERVAL '1 hour', to_timestamp_micros({timestamp_arg}), TIMESTAMP '1970-01-01 00:00:00')"
                );
                let sql = format!(
                    "SELECT {bin} AS b, f AS key, COUNT(*) AS f FROM {source} GROUP BY 1, 2"
                );
                let logical = ctx.state().create_logical_plan(&sql).await?;
                let physical = ctx.state().create_physical_plan(&logical).await?;
                let partial =
                    get_partial_aggregate_plan(physical).expect("partial histogram aggregate");
                assert_eq!(
                    is_simple_multi_histogram(
                        Arc::new(partial),
                        (0, 3_600_000_000),
                        fields.clone()
                    )
                    .is_some(),
                    eligible,
                    "{sql}"
                );

                let sql = format!("SELECT {bin} AS b, COUNT(*) AS f FROM {source} GROUP BY 1");
                let logical = ctx.state().create_logical_plan(&sql).await?;
                let physical = ctx.state().create_physical_plan(&logical).await?;
                let partial =
                    get_partial_aggregate_plan(physical).expect("partial histogram aggregate");
                assert_eq!(
                    is_simple_histogram(Arc::new(partial), (0, 3_600_000_000)).is_some(),
                    eligible,
                    "{sql}"
                );
            }
        }
        // Combining partial states without any provable raw-input stage must
        // not authorize row counting solely from a final COUNT expression.
        let logical = ctx
            .state()
            .create_logical_plan("SELECT COUNT(_timestamp) FROM t")
            .await?;
        let physical = ctx.state().create_physical_plan(&logical).await?;
        assert!(is_simple_count(physical.clone(), &fields).is_some());
        let without_raw_stage = physical
            .transform_up(|node| {
                use datafusion::common::tree_node::Transformed;
                if node
                    .downcast_ref::<AggregateExec>()
                    .is_some_and(|aggregate| {
                        aggregate.mode().input_mode() == AggregateInputMode::Raw
                    })
                {
                    Ok(Transformed::yes(
                        Arc::new(EmptyExec::new(node.schema())) as Arc<dyn ExecutionPlan>
                    ))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;
        assert!(is_simple_count(without_raw_stage, &fields).is_none());
        Ok(())
    }

    // get the first final aggregate plan from bottom to top
    pub fn get_partial_aggregate_plan(plan: Arc<dyn ExecutionPlan>) -> Option<AggregateExec> {
        let mut visitor = AggregateVisitor::new();
        let _ = plan.visit(&mut visitor);
        let data = visitor.get_data();
        data.map(|v| v.downcast_ref::<AggregateExec>().unwrap().clone())
    }

    struct AggregateVisitor {
        data: Option<Arc<dyn ExecutionPlan>>,
    }

    impl AggregateVisitor {
        fn new() -> Self {
            Self { data: None }
        }

        fn get_data(&self) -> Option<&Arc<dyn ExecutionPlan>> {
            self.data.as_ref()
        }
    }

    impl Default for AggregateVisitor {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<'n> TreeNodeVisitor<'n> for AggregateVisitor {
        type Node = Arc<dyn ExecutionPlan>;

        fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
            if node.name() == "AggregateExec" {
                let agg = node.downcast_ref::<AggregateExec>().unwrap();
                if *agg.mode() == AggregateMode::Partial {
                    self.data = Some(node.clone());
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        }
    }

    pub fn get_remote_scan(plan: Arc<dyn ExecutionPlan>) -> Vec<Arc<RemoteScanExec>> {
        let mut visitor = RemoteScanVisitor::new();
        let _ = plan.visit(&mut visitor);
        visitor.get_data()
    }

    struct RemoteScanVisitor {
        data: Vec<Arc<RemoteScanExec>>,
    }

    impl RemoteScanVisitor {
        fn new() -> Self {
            Self { data: Vec::new() }
        }

        fn get_data(&self) -> Vec<Arc<RemoteScanExec>> {
            self.data.clone()
        }
    }

    impl Default for RemoteScanVisitor {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<'n> TreeNodeVisitor<'n> for RemoteScanVisitor {
        type Node = Arc<dyn ExecutionPlan>;

        fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
            if node.name() == "RemoteScanExec" {
                let remote_scan = node.downcast_ref::<RemoteScanExec>().unwrap();
                self.data.push(Arc::new(remote_scan.clone()));
            }
            Ok(TreeNodeRecursion::Continue)
        }
    }
}
