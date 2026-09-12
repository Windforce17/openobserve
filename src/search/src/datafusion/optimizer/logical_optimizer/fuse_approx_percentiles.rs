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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow_schema::DataType;
use datafusion::{
    common::{
        Column, Result, ScalarValue,
        tree_node::{Transformed, TreeNode},
    },
    functions::core::expr_fn::get_field,
    functions_aggregate::approx_percentile_cont::ApproxPercentileCont,
    logical_expr::{Aggregate, Expr, LogicalPlan, Projection, expr::AggregateFunction},
    optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder},
};
use datafusion_functions_aggregate_common::tdigest::DEFAULT_MAX_SIZE;

use crate::datafusion::udaf::shared_percentile;

/// Share a TDigest only after coercion, simplification and dead-output pruning.
/// The restoring projection is the original Aggregate's public output boundary.
#[derive(Debug, Default)]
pub struct FuseApproxPercentiles;

impl FuseApproxPercentiles {
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for FuseApproxPercentiles {
    fn name(&self) -> &str {
        "fuse_approx_percentiles"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return Ok(Transformed::no(plan));
        };
        fuse(aggregate, config)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Compatibility<'a> {
    input: &'a Expr,
    filter: &'a Option<Box<Expr>>,
    data_type: &'a DataType,
    max_centroids: usize,
}

struct DigestGroup<'a> {
    key: Compatibility<'a>,
    quantiles: Vec<f64>,
    alias: Option<String>,
}

fn fuse(aggregate: Aggregate, config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
    // Physical aliases do not preserve arbitrary field metadata. Do not pretend
    // otherwise by attaching the old logical schema to differently typed fields.
    if aggregate
        .schema
        .fields()
        .iter()
        .any(|field| !field.metadata().is_empty())
    {
        return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
    }
    let group_len = aggregate.group_expr_len()?;
    let mut groups: Vec<DigestGroup<'_>> = Vec::new();
    let mut by_key = HashMap::new();
    let mut slots = vec![None; aggregate.aggr_expr.len()];
    for (index, expr) in aggregate.aggr_expr.iter().enumerate() {
        let field = aggregate.schema.field(group_len + index);
        let Some((key, quantile)) = eligible(expr, field.data_type(), field.is_nullable())? else {
            continue;
        };
        let group_index = *by_key.entry(key).or_insert_with(|| {
            let index = groups.len();
            groups.push(DigestGroup {
                key,
                quantiles: Vec::new(),
                alias: None,
            });
            index
        });
        let quantiles = &mut groups[group_index].quantiles;
        slots[index] = Some((group_index, quantiles.len()));
        quantiles.push(quantile);
    }
    if !groups.iter().any(|group| group.quantiles.len() >= 2) {
        return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
    }

    let mut names: HashSet<String> = aggregate
        .schema
        .fields()
        .iter()
        .chain(aggregate.input.schema().fields().iter())
        .map(|field| field.name().clone())
        .collect();
    let mut new_aggr = Vec::with_capacity(aggregate.aggr_expr.len());
    // group_expr_len includes expanded grouping sets and their hidden grouping id.
    let mut restoring: Vec<Expr> = aggregate
        .schema
        .columns()
        .into_iter()
        .take(group_len)
        .map(Expr::Column)
        .collect();
    for (index, expr) in aggregate.aggr_expr.iter().enumerate() {
        let (qualifier, field) = aggregate.schema.qualified_field(group_len + index);
        let replacement = match slots[index] {
            Some((group_index, quantile_index)) if groups[group_index].quantiles.len() >= 2 => {
                let group = &mut groups[group_index];
                if group.alias.is_none() {
                    let alias = loop {
                        let name = config.alias_generator().next("__oo_shared_percentiles");
                        if names.insert(name.clone()) {
                            break name;
                        }
                    };
                    let function = shared_percentile::create_udaf(
                        group.quantiles.clone(),
                        group.key.max_centroids,
                    )?;
                    new_aggr.push(
                        Expr::AggregateFunction(AggregateFunction::new_udf(
                            function,
                            vec![group.key.input.clone()],
                            false,
                            group.key.filter.clone(),
                            vec![],
                            None,
                        ))
                        .alias(alias.clone()),
                    );
                    group.alias = Some(alias);
                }
                get_field(
                    Expr::Column(Column::from_name(group.alias.as_ref().unwrap())),
                    format!("q{quantile_index}"),
                )
                .alias_qualified(qualifier.cloned(), field.name())
            }
            _ => {
                new_aggr.push(expr.clone());
                Expr::Column(Column::new(qualifier.cloned(), field.name()))
            }
        };
        restoring.push(replacement);
    }
    let new_aggregate = Aggregate::try_new(
        Arc::clone(&aggregate.input),
        aggregate.group_expr.clone(),
        new_aggr,
    )?;
    let mut projection =
        Projection::try_new(restoring, Arc::new(LogicalPlan::Aggregate(new_aggregate)))?;
    // Derive real expression fields first. An unusual programmatic schema is
    // outside the supported scope; never use with_schema as a type/nullability cast.
    if projection.schema.iter().ne(aggregate.schema.iter())
        || projection.schema.metadata() != aggregate.schema.metadata()
    {
        return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
    }
    // The fields were checked above; preserve the original functional dependencies
    // too, including grouping-set information needed by outer optimizations.
    projection.schema = aggregate.schema;
    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
}

fn eligible<'a>(
    expr: &'a Expr,
    data_type: &'a DataType,
    nullable: bool,
) -> Result<Option<(Compatibility<'a>, f64)>> {
    let expr = match expr {
        Expr::Alias(alias) if alias.metadata.is_none() => alias.expr.as_ref(),
        expr => expr,
    };
    let Expr::AggregateFunction(function) = expr else {
        return Ok(None);
    };
    // Function names are not identities: custom UDAFs may shadow the builtin.
    if function
        .func
        .inner()
        .downcast_ref::<ApproxPercentileCont>()
        .is_none()
    {
        return Ok(None);
    }
    let params = &function.params;
    if params.distinct
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
        || !(2..=3).contains(&params.args.len())
        || !nullable
        || !matches!(
            data_type,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        )
        || params.args[0].is_volatile()
        || params
            .filter
            .as_ref()
            .is_some_and(|filter| filter.is_volatile())
    {
        return Ok(None);
    }
    // Preserve nonliteral and metadata-bearing arguments on the original validator
    // path, even when an upstream simplifier elected not to fold them.
    for arg in params
        .args
        .iter()
        .chain(params.filter.iter().map(Box::as_ref))
    {
        if arg.exists(|expr| {
            Ok(matches!(
                expr,
                Expr::Literal(_, Some(_))
                    | Expr::Alias(datafusion::logical_expr::expr::Alias {
                        metadata: Some(_),
                        ..
                    })
            ))
        })? {
            return Ok(None);
        }
    }
    let quantile = match &params.args[1] {
        Expr::Literal(ScalarValue::Float64(Some(value)), None) => *value,
        Expr::Literal(ScalarValue::Float32(Some(value)), None) => f64::from(*value),
        _ => return Ok(None),
    };
    if !(0.0..=1.0).contains(&quantile) {
        return Ok(None);
    }
    let max_centroids = if params.args.len() == 3 {
        let Some(size) = literal_centroids(&params.args[2]) else {
            return Ok(None);
        };
        size
    } else {
        DEFAULT_MAX_SIZE
    };
    Ok(Some((
        Compatibility {
            input: &params.args[0],
            filter: &params.filter,
            data_type,
            max_centroids,
        },
        quantile,
    )))
}

fn literal_centroids(expr: &Expr) -> Option<usize> {
    let Expr::Literal(value, None) = expr else {
        return None;
    };
    let value = match value {
        ScalarValue::UInt8(Some(value)) => usize::from(*value),
        ScalarValue::UInt16(Some(value)) => usize::from(*value),
        ScalarValue::UInt32(Some(value)) => usize::try_from(*value).ok()?,
        ScalarValue::UInt64(Some(value)) => usize::try_from(*value).ok()?,
        ScalarValue::Int8(Some(value)) => usize::try_from(*value).ok()?,
        ScalarValue::Int16(Some(value)) => usize::try_from(*value).ok()?,
        ScalarValue::Int32(Some(value)) => usize::try_from(*value).ok()?,
        ScalarValue::Int64(Some(value)) => usize::try_from(*value).ok()?,
        _ => return None,
    };
    // The builtin's unsigned zero edge remains its own behavior, not a new error.
    (value > 0).then_some(value)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Float32Array, Float64Array, StringArray},
        compute::concat_batches,
        record_batch::RecordBatch,
    };
    use arrow_schema::{Field, Schema};
    use datafusion::{
        common::tree_node::TreeNodeRecursion,
        datasource::MemTable,
        execution::{SessionStateBuilder, config::SessionConfig},
        prelude::SessionContext,
    };

    use super::*;
    use crate::{datafusion::optimizer::generate_optimizer_rules, sql::Sql};

    fn context(fused: bool, max_passes: usize) -> SessionContext {
        // Use the production rule sequence without mutating process-global env/config.
        let sql = Sql {
            metadata: Arc::new(crate::sql::SqlMetadata {
                sql: String::new(),
                is_complex: true,
                org_id: String::new(),
                stream_type: config::meta::stream::StreamType::Logs,
                stream_names: vec![],
                has_match_all: false,
                equal_items: Default::default(),
                columns: Default::default(),
                aliases: vec![],
                schemas: Default::default(),
                limit: config::QUERY_WITH_NO_LIMIT,
                offset: 0,
                group_by: vec![],
                order_by: vec![],
                histogram_interval: None,
                timezone: None,
                sorted_by_time: false,
                pagination: Default::default(),
            }),
            time_range: (0, 0),
            sampling_config: None,
        };
        let mut rules = generate_optimizer_rules(&sql, false);
        if fused {
            let position = rules
                .iter()
                .rposition(|rule| rule.name() == "optimize_projections")
                .unwrap();
            rules.insert(position + 1, Arc::new(FuseApproxPercentiles::new()));
        }
        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().optimizer.max_passes = max_passes;
        config.options_mut().optimizer.skip_failed_rules = false;
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_optimizer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, true),
            Field::new("v", DataType::Float64, true),
            Field::new("v32", DataType::Float32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    Some("b"),
                    Some("c"),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(3.0),
                    Some(7.0),
                    Some(10.0),
                    Some(20.0),
                    None,
                    Some(30.0),
                ])),
                Arc::new(Float32Array::from(vec![
                    Some(1.0),
                    Some(3.0),
                    Some(7.0),
                    Some(10.0),
                    Some(20.0),
                    None,
                    Some(30.0),
                ])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    async fn run(ctx: &SessionContext, sql: &str) -> Result<(RecordBatch, usize)> {
        let frame = ctx.sql(sql).await?;
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let plan = frame.clone().into_optimized_plan()?;
        let mut shared = 0;
        plan.apply(|plan| {
            if let LogicalPlan::Aggregate(aggregate) = plan {
                for expr in &aggregate.aggr_expr {
                    expr.apply(|expr| {
                        if matches!(expr, Expr::AggregateFunction(function)
                            if function.func.inner().downcast_ref::<shared_percentile::SharedPercentiles>().is_some())
                        {
                            shared += 1;
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let batches = frame.collect().await?;
        // Physical result fields, not just the logical relabelled schema, must match.
        for batch in &batches {
            assert_eq!(batch.schema(), schema, "{sql}");
        }
        Ok((concat_batches(&schema, &batches)?, shared))
    }

    async fn parity(sql: &str, shared: usize) -> RecordBatch {
        let (expected, _) = run(&context(false, 1), sql).await.unwrap();
        for max_passes in [1, 3] {
            let (actual, count) = run(&context(true, max_passes), sql).await.unwrap();
            assert_eq!(actual, expected, "{sql}; max_passes={max_passes}");
            assert_eq!(count, shared, "{sql}; max_passes={max_passes}");
        }
        expected
    }

    #[tokio::test]
    async fn aliases_order_duplicates_and_hidden_consumers() {
        let batch = parity(
            "SELECT g, approx_percentile_cont(v, 0.95) AS \"__oo_shared_percentiles_1\", \
             count(*) AS n, approx_percentile_cont(v, 0.5) AS median, \
             approx_percentile_cont(v, 0.95) AS duplicate, \
             approx_percentile_cont(v, 0.99) AS high FROM t GROUP BY g ORDER BY median DESC NULLS LAST",
            1,
        ).await;
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            [
                "g",
                "__oo_shared_percentiles_1",
                "n",
                "median",
                "duplicate",
                "high"
            ]
        );
        assert_eq!(batch.column(1), batch.column(4));
        parity(
            "SELECT g, approx_percentile_cont(v, 0.5) AS p FROM t GROUP BY g \
             HAVING approx_percentile_cont(v, 0.95) > 5 \
             ORDER BY approx_percentile_cont(v, 0.99) DESC",
            1,
        )
        .await;
        parity(
            "SELECT x.g, x.p95 - x.p50 AS spread FROM \
             (SELECT g, approx_percentile_cont(v, 0.5) AS p50, \
              approx_percentile_cont(v, 0.95) AS p95, approx_percentile_cont(v, 0.99) AS p99 \
              FROM t GROUP BY g) x ORDER BY x.g NULLS LAST",
            1,
        )
        .await;
        parity(
            "SELECT x.p50 FROM (SELECT approx_percentile_cont(v, 0.5) AS p50, \
             approx_percentile_cont(v, 0.95) AS p95, approx_percentile_cont(v, 0.99) AS p99 FROM t) x",
            0,
        ).await;
    }

    #[tokio::test]
    async fn filters_precision_and_coercions_are_isolated() {
        parity(
            "SELECT g, approx_percentile_cont(v, 0.5) FILTER (WHERE v < 15) AS p50, \
             approx_percentile_cont(v, 0.95, 100) FILTER (WHERE v < 15) AS p95, \
             approx_percentile_cont(v, 0.5) FILTER (WHERE v > 15) AS other_filter, \
             approx_percentile_cont(v, 0.5, 200) AS other_precision, \
             approx_percentile_cont(v32, 0.5) AS f50, approx_percentile_cont(v32, 0.95) AS f95 \
             FROM t GROUP BY g ORDER BY g NULLS LAST",
            2,
        )
        .await;
        parity(
            "SELECT approx_percentile_cont(v, 0.5) AS p50, \
             approx_percentile_cont(v, 0.95, 100) AS p95, \
             approx_percentile_cont(v, 0.5, 200) AS h50, \
             approx_percentile_cont(v, 0.95, 200) AS h95 FROM t",
            2,
        )
        .await;
    }

    #[tokio::test]
    async fn null_empty_and_grouping_set_outputs() {
        parity(
            "SELECT g, approx_percentile_cont(v, 0.5) AS p50, \
             approx_percentile_cont(v, 0.95) AS p95 FROM t \
             GROUP BY GROUPING SETS ((g), (), (g)) ORDER BY g NULLS LAST, p50 NULLS LAST",
            1,
        )
        .await;
        parity(
            "SELECT g, approx_percentile_cont(v, 0.5) FILTER (WHERE v > 100) AS p50, \
             approx_percentile_cont(v, 0.95) FILTER (WHERE v > 100) AS p95 \
             FROM t GROUP BY g ORDER BY g NULLS LAST",
            1,
        )
        .await;
        // A runtime-empty input still exercises aggregate evaluation, rather than
        // a statically false predicate being removed by PropagateEmptyRelation.
        let batch = parity(
            "SELECT approx_percentile_cont(v, 0.5) AS p50, \
             approx_percentile_cont(v, 0.95) AS p95 FROM t WHERE v > 100",
            1,
        )
        .await;
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(0).is_null(0) && batch.column(1).is_null(0));
    }

    #[tokio::test]
    async fn unsupported_calls_keep_original_results_and_errors() {
        parity(
            "SELECT approx_percentile_cont(DISTINCT v, 0.5) AS p50, \
             approx_percentile_cont(DISTINCT v, 0.95) AS p95 FROM t",
            0,
        )
        .await;
        parity(
            "SELECT approx_percentile_cont(0.5) WITHIN GROUP (ORDER BY v DESC) AS p50, \
             approx_percentile_cont(0.95) WITHIN GROUP (ORDER BY v DESC) AS p95 FROM t",
            0,
        )
        .await;
        parity(
            "SELECT v, approx_percentile_cont(v, 0.5) OVER () AS p50, \
             approx_percentile_cont(v, 0.95) OVER () AS p95 FROM t ORDER BY v NULLS LAST",
            0,
        )
        .await;
        for invalid in [
            "approx_percentile_cont(v, -0.1)",
            "approx_percentile_cont(v, 1.1)",
            "approx_percentile_cont(v, CAST('NaN' AS DOUBLE))",
            "approx_percentile_cont(v, NULL)",
            "approx_percentile_cont(v, v)",
            "approx_percentile_cont(v, 0.5, -1)",
            "approx_percentile_cont(v, 0.5, 0)",
            "approx_percentile_cont(v, 0.5, NULL)",
        ] {
            let sql = format!(
                "SELECT approx_percentile_cont(v, 0.5) AS p50, \
                approx_percentile_cont(v, 0.95) AS p95, {invalid} AS invalid FROM t"
            );
            let expected = run(&context(false, 1), &sql).await.unwrap_err();
            let actual = run(&context(true, 1), &sql).await.unwrap_err();
            assert_eq!(actual.to_string(), expected.to_string(), "{sql}");
        }
    }

    #[tokio::test]
    async fn same_name_custom_udaf_retains_its_behavior() {
        use datafusion::{
            functions_aggregate::approx_percentile_cont::ApproxPercentileAccumulator,
            logical_expr::{Volatility, create_udaf},
        };

        // This function intentionally ignores q and always returns the minimum.
        // Name-based fusion would silently replace that user-defined contract.
        let custom = create_udaf(
            "approx_percentile_cont",
            vec![DataType::Float64, DataType::Float64],
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            Arc::new(|_| {
                Ok(Box::new(ApproxPercentileAccumulator::new(
                    0.0,
                    DataType::Float64,
                )))
            }),
            Arc::new(vec![
                DataType::UInt64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
            ]),
        );
        let baseline = context(false, 1);
        let fused = context(true, 1);
        baseline.register_udaf(custom.clone());
        fused.register_udaf(custom);
        let sql = "SELECT approx_percentile_cont(v, 0.5) AS p50, \
                   approx_percentile_cont(v, 0.95) AS p95 FROM t";
        let (expected, _) = run(&baseline, sql).await.unwrap();
        let (actual, shared) = run(&fused, sql).await.unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.column(0), actual.column(1));
        assert_eq!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.0
        );
        assert_eq!(shared, 0);
    }
}
