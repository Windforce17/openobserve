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

use datafusion::{
    common::{Result, not_impl_err},
    execution::TaskContext,
    logical_expr::{AggregateUDF, ScalarUDF},
    physical_plan::ExecutionPlan,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::datafusion::udaf::shared_percentile::{NAME, SharedPercentiles};

#[cfg(feature = "enterprise")]
mod aggregate_topk_exec;
mod deduplication_exec;
mod empty_exec;
#[cfg(feature = "enterprise")]
mod enrichment_exec;
mod physical_plan_node;
#[cfg(feature = "enterprise")]
mod streaming_aggs_exec;
#[cfg(feature = "enterprise")]
mod tmp_exec;

pub fn get_physical_extension_codec() -> ComposedPhysicalExtensionCodec {
    ComposedPhysicalExtensionCodec {
        codecs: vec![Arc::new(
            physical_plan_node::PhysicalPlanNodePhysicalExtensionCodec {},
        )],
    }
}

/// A PhysicalExtensionCodec that tries one of multiple inner codecs
/// until one works
#[derive(Debug)]
pub struct ComposedPhysicalExtensionCodec {
    pub codecs: Vec<Arc<dyn PhysicalExtensionCodec>>,
}

impl PhysicalExtensionCodec for ComposedPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut last_err = None;
        for codec in &self.codecs {
            match codec.try_decode(buf, inputs, ctx) {
                Ok(plan) => return Ok(plan),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap())
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let mut last_err = None;
        for codec in &self.codecs {
            match codec.try_encode(node.clone(), buf) {
                Ok(_) => return Ok(()),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap())
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        let mut last_err = None;
        for codec in &self.codecs {
            match codec.try_decode_udf(name, buf) {
                Ok(plan) => return Ok(plan),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap())
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        let mut last_err = None;
        for codec in &self.codecs {
            match codec.try_encode_udf(_node, buf) {
                Ok(_) => return Ok(()),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap())
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        // The default subcodec hook succeeds without writing anything. Dispatch here
        // so it cannot swallow the configuration of a query-specific aggregate.
        if let Some(shared) = node.inner().downcast_ref::<SharedPercentiles>() {
            buf.extend_from_slice(&shared.encode_config()?);
        }
        Ok(())
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        if name == NAME {
            return Ok(Arc::new(AggregateUDF::from(
                SharedPercentiles::decode_config(buf)?,
            )));
        }
        not_impl_err!("PhysicalExtensionCodec is not provided for aggregate function {name}")
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            array::{Float64Array, Int32Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        },
        common::{ScalarValue, config::ConfigOptions},
        datasource::memory::MemorySourceConfig,
        functions::core::get_field,
        functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf,
        physical_expr::{
            ScalarFunctionExpr,
            aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
            expressions::{col, lit},
        },
        physical_plan::{
            PhysicalExpr,
            aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
            coalesce_partitions::CoalescePartitionsExec,
            collect,
            projection::ProjectionExec,
        },
        prelude::SessionContext,
    };
    use datafusion_proto::{
        bytes::{
            physical_plan_from_bytes_with_extension_codec,
            physical_plan_to_bytes_with_extension_codec,
        },
        protobuf::{
            PhysicalAggregateExprNode, PhysicalPlanNode,
            physical_aggregate_expr_node::AggregateFunction, physical_expr_node::ExprType,
            physical_plan_node::PhysicalPlanType,
        },
    };
    use prost::Message;

    use super::*;
    use crate::datafusion::udaf::{
        shared_percentile::create_udaf, summary_percentile::SummaryPercentile,
    };

    fn input() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int32, false),
            Field::new("v", DataType::Float64, true),
        ]));
        let partitions = [
            vec![Some(10.0), Some(100.0), None, Some(30.0), Some(300.0)],
            vec![Some(20.0), Some(200.0), None, Some(40.0), None],
        ]
        .into_iter()
        .map(|values| {
            Ok(vec![RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 1, 2])),
                    Arc::new(Float64Array::from(values)),
                ],
            )?])
        })
        .collect::<Result<Vec<_>>>()?;
        Ok(MemorySourceConfig::try_new_exec(&partitions, schema, None)?)
    }

    fn roundtrip(
        plan: Arc<dyn ExecutionPlan>,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let codec = get_physical_extension_codec();
        let schema = plan.schema();
        let bytes = physical_plan_to_bytes_with_extension_codec(plan, &codec)?;
        let decoded =
            physical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &codec)?;
        assert_eq!(schema, decoded.schema());
        Ok(decoded)
    }

    fn aggregate_pipeline(
        input: Arc<dyn ExecutionPlan>,
        aggregates: Vec<Arc<AggregateFunctionExpr>>,
        grouped: bool,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        let groups = PhysicalGroupBy::new_single(if grouped {
            vec![(col("g", &schema)?, "g".to_owned())]
        } else {
            vec![]
        });
        let filters = vec![None; aggregates.len()];
        let partial = roundtrip(
            Arc::new(AggregateExec::try_new(
                AggregateMode::Partial,
                groups.clone(),
                aggregates.clone(),
                filters.clone(),
                input,
                Arc::clone(&schema),
            )?),
            ctx,
        )?;
        // Both source partitions produce state for the same groups. Exercise a
        // regional partial-state merge as well as the final merge.
        let reduced = roundtrip(
            Arc::new(AggregateExec::try_new(
                AggregateMode::PartialReduce,
                groups.clone(),
                aggregates.clone(),
                filters.clone(),
                Arc::new(CoalescePartitionsExec::new(partial)),
                Arc::clone(&schema),
            )?),
            ctx,
        )?;
        roundtrip(
            Arc::new(AggregateExec::try_new(
                AggregateMode::Final,
                groups,
                aggregates,
                filters,
                reduced,
                schema,
            )?),
            ctx,
        )
    }

    fn rows(batches: &[RecordBatch]) -> Result<Vec<Vec<ScalarValue>>> {
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| ScalarValue::try_from_array(column, row))
                        .collect::<Result<Vec<_>>>()?,
                );
            }
        }
        rows.sort_by(|left, right| left.partial_cmp(right).unwrap());
        Ok(rows)
    }

    #[tokio::test]
    async fn configured_percentiles_roundtrip_partial_reduce_final_and_projection() -> Result<()> {
        let ctx = SessionContext::new();
        // Deliberately never register NAME: each instance must come from its
        // definition, not from a registry's single default configuration.
        for grouped in [false, true] {
            let input = input()?;
            let schema = input.schema();
            // The small precision forces compression, so losing it on the wire
            // changes the interior quantiles rather than only invisible state.
            let configs = [(vec![0.0, 0.5, 1.0], 100), (vec![1.0, 0.0, 0.7, 0.3], 2)];
            let mut shared = Vec::new();
            let mut independent = Vec::new();
            for (index, (quantiles, centroids)) in configs.iter().enumerate() {
                shared.push(Arc::new(
                    AggregateExprBuilder::new(
                        create_udaf(quantiles.clone(), *centroids)?,
                        vec![col("v", &schema)?],
                    )
                    .schema(Arc::clone(&schema))
                    .alias(format!("shared_{index}"))
                    .build()?,
                ));
                for (slot, quantile) in quantiles.iter().enumerate() {
                    independent.push(Arc::new(
                        AggregateExprBuilder::new(
                            approx_percentile_cont_udaf(),
                            vec![col("v", &schema)?, lit(*quantile), lit(*centroids as i64)],
                        )
                        .schema(Arc::clone(&schema))
                        .alias(format!("p{index}_{slot}"))
                        .build()?,
                    ));
                }
            }
            let shared_plan = aggregate_pipeline(Arc::clone(&input), shared, grouped, &ctx)?;
            let output_schema = shared_plan.schema();
            let mut projection: Vec<(Arc<dyn PhysicalExpr>, String)> = if grouped {
                vec![(col("g", &output_schema)?, "g".to_owned())]
            } else {
                vec![]
            };
            for (index, (quantiles, _)) in configs.iter().enumerate() {
                for slot in 0..quantiles.len() {
                    projection.push((
                        Arc::new(ScalarFunctionExpr::try_new(
                            get_field(),
                            vec![
                                col(&format!("shared_{index}"), &output_schema)?,
                                lit(format!("q{slot}")),
                            ],
                            &output_schema,
                            Arc::new(ConfigOptions::default()),
                        )?),
                        format!("p{index}_{slot}"),
                    ));
                }
            }
            let projected = roundtrip(
                Arc::new(ProjectionExec::try_new(projection, shared_plan)?),
                &ctx,
            )?;
            let oracle = aggregate_pipeline(input, independent, grouped, &ctx)?;
            let actual = rows(&collect(projected, ctx.task_ctx()).await?)?;
            let expected = rows(&collect(oracle, ctx.task_ctx()).await?)?;
            assert_eq!(actual, expected);
            // Concrete extrema and all-null groups guard against a vacuous
            // comparison of two empty/misrouted execution plans.
            let values = if grouped {
                vec![
                    vec![Some(10.0), Some(25.0), Some(40.0), Some(40.0), Some(10.0)],
                    vec![
                        Some(100.0),
                        Some(200.0),
                        Some(300.0),
                        Some(300.0),
                        Some(100.0),
                    ],
                    vec![None; 5],
                ]
            } else {
                vec![vec![
                    Some(10.0),
                    Some(40.0),
                    Some(300.0),
                    Some(300.0),
                    Some(10.0),
                ]]
            };
            let concrete: Vec<Vec<ScalarValue>> = values
                .into_iter()
                .enumerate()
                .map(|(index, values)| {
                    let mut row = if grouped {
                        vec![ScalarValue::Int32(Some(index as i32 + 1))]
                    } else {
                        vec![]
                    };
                    row.extend(values.into_iter().map(ScalarValue::Float64));
                    row
                })
                .collect();
            let extrema_and_median: Vec<_> = actual
                .iter()
                .map(|row| row[..5 + usize::from(grouped)].to_vec())
                .collect();
            assert_eq!(extrema_and_median, concrete);
        }
        Ok(())
    }

    fn first_aggregate(node: &mut PhysicalPlanNode) -> &mut PhysicalAggregateExprNode {
        let Some(PhysicalPlanType::Aggregate(aggregate)) = node.physical_plan_type.as_mut() else {
            panic!("expected physical aggregate");
        };
        let Some(ExprType::AggregateExpr(expr)) = aggregate.aggr_expr[0].expr_type.as_mut() else {
            panic!("expected aggregate expression");
        };
        expr
    }

    #[test]
    fn configured_percentiles_reject_missing_malformed_and_unknown_definitions() -> Result<()> {
        let ctx = SessionContext::new();
        let codec = get_physical_extension_codec();
        let input = input()?;
        let schema = input.schema();
        let aggregate = Arc::new(
            AggregateExprBuilder::new(create_udaf(vec![0.1, 0.9], 64)?, vec![col("v", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("percentiles")
                .build()?,
        );
        let plan = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![]),
            vec![aggregate],
            vec![None],
            input,
            schema,
        )?);
        let bytes = physical_plan_to_bytes_with_extension_codec(plan, &codec)?;
        let mut proto = PhysicalPlanNode::decode(bytes.as_ref()).unwrap();
        let valid = first_aggregate(&mut proto).fun_definition.clone().unwrap();
        let mut trailing = valid.clone();
        trailing.push(0);
        for payload in [
            None,
            Some(vec![]),
            Some(vec![255]),
            Some(valid[..valid.len() - 1].to_vec()),
            Some(trailing),
        ] {
            let mut invalid = proto.clone();
            first_aggregate(&mut invalid).fun_definition = payload;
            assert!(
                physical_plan_from_bytes_with_extension_codec(
                    &invalid.encode_to_vec(),
                    &ctx.task_ctx(),
                    &codec,
                )
                .is_err()
            );
        }
        // Near-matching names must not be accepted by prefix/suffix dispatch.
        first_aggregate(&mut proto).aggregate_function = Some(
            AggregateFunction::UserDefinedAggrFunction(format!("{NAME}_unknown")),
        );
        assert!(
            physical_plan_from_bytes_with_extension_codec(
                &proto.encode_to_vec(),
                &ctx.task_ctx(),
                &codec,
            )
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn existing_builtin_and_registered_custom_aggregates_roundtrip() -> Result<()> {
        let ctx = SessionContext::new();
        let summary = AggregateUDF::from(SummaryPercentile::new());
        let mut payload = Vec::new();
        get_physical_extension_codec().try_encode_udaf(&summary, &mut payload)?;
        assert!(payload.is_empty());
        ctx.register_udaf(summary);
        let plan = ctx
            .sql(
                "SELECT count(*) AS n, sum(v) AS total, \
                 summary_percentile(v, n, 0.5) AS median \
                 FROM (VALUES (CAST(10 AS DOUBLE), CAST(1 AS BIGINT)), \
                 (CAST(20 AS DOUBLE), CAST(3 AS BIGINT))) AS t(v, n)",
            )
            .await?
            .create_physical_plan()
            .await?;
        let decoded = roundtrip(Arc::clone(&plan), &ctx)?;
        let actual = rows(&collect(decoded, ctx.task_ctx()).await?)?;
        assert_eq!(actual, rows(&collect(plan, ctx.task_ctx()).await?)?);
        assert_eq!(
            actual,
            vec![vec![
                ScalarValue::Int64(Some(2)),
                ScalarValue::Float64(Some(30.0)),
                ScalarValue::Float64(Some(20.0)),
            ]]
        );
        Ok(())
    }
}
