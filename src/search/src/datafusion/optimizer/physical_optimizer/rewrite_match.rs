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

use arrow_schema::{DataType, Schema, SchemaRef};
use config::get_config;
use datafusion::{
    common::{
        Result, project_schema,
        tree_node::{
            Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
        },
    },
    config::ConfigOptions,
    error::DataFusionError,
    logical_expr::Operator,
    physical_expr::{
        PhysicalExpr, ScalarFunctionExpr,
        expressions::{Column, IsNotNullExpr, Literal},
    },
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan,
        expressions::{BinaryExpr, LikeExpr},
        filter::{FilterExec, FilterExecBuilder},
    },
    scalar::ScalarValue,
};

use crate::datafusion::{
    distributed_plan::empty_exec::NewEmptyExec,
    optimizer::physical_optimizer::utils::{
        disjunction, extract_column, extract_int64_literal, extract_string_literal,
    },
    udf::{
        fuzzy_match_udf,
        match_all_hash_udf::MATCH_ALL_HASH_UDF_NAME,
        match_all_udf::{FUZZY_MATCH_ALL_UDF_NAME, MATCH_ALL_UDF_NAME},
    },
};

/// Physical optimization rule that rewrites match_all() to LIKE expressions
#[derive(Default, Debug)]
pub struct RewriteMatchPhysical {
    fields: Vec<(String, DataType)>,
}

impl RewriteMatchPhysical {
    pub fn new(fields: Vec<(String, DataType)>) -> Self {
        Self { fields }
    }
}

impl PhysicalOptimizerRule for RewriteMatchPhysical {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut plan_rewriter = PlanRewriter::new(self.fields.clone());
        plan.rewrite(&mut plan_rewriter).data()
    }

    fn name(&self) -> &str {
        "RewriteMatchAllRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// Plan rewriter that applies expression rewriting to FilterExec predicates
#[derive(Debug, Clone)]
struct PlanRewriter {
    fields: Vec<(String, DataType)>,
}

impl PlanRewriter {
    fn new(fields: Vec<(String, DataType)>) -> Self {
        Self { fields }
    }
}

impl TreeNodeRewriter for PlanRewriter {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Some(filter) = plan.downcast_ref::<FilterExec>() {
            let predicate = filter.predicate();

            // 1. Check if the predicate contains any match_all functions
            if !has_match_all_function(predicate) {
                return Ok(Transformed::no(plan));
            }

            // 2. Rewrite the datasource projection to include FST fields
            let mut add_fst_fields_to_projection = AddFstFieldsToProjection::new(
                self.fields.iter().map(|f| f.0.clone()).collect(),
                filter.schema(),
            );
            let input = filter
                .input()
                .clone()
                .rewrite(&mut add_fst_fields_to_projection)?
                .data;

            // 3. Update column indices in the predicate to match the new schema
            let new_schema = input.schema();
            let mut column_index_rewriter = ColumnIndexRewriter::new(new_schema.clone());
            let predicate_with_updated_indices = predicate
                .clone()
                .rewrite(&mut column_index_rewriter)
                .data()?;

            // 4. Rewrite match_all/fuzzy_match_all
            let mut expr_rewriter = MatchAllRewriter::new(new_schema.clone(), self.fields.clone());
            let rewritten_predicate = predicate_with_updated_indices
                .rewrite(&mut expr_rewriter)
                .data()?;

            // 5. Create new filter with rewritten predicate
            let new_filter = FilterExecBuilder::new(rewritten_predicate, input)
                .apply_projection(add_fst_fields_to_projection.filter_projection.clone())?
                .with_fetch(filter.fetch())
                .build()?;
            return Ok(Transformed::yes(Arc::new(new_filter)));
        }
        Ok(Transformed::no(plan))
    }
}

// Rewriter for updating column indices when schema changes
#[derive(Debug, Clone)]
struct ColumnIndexRewriter {
    new_schema: SchemaRef,
}

impl ColumnIndexRewriter {
    fn new(new_schema: SchemaRef) -> Self {
        Self { new_schema }
    }
}

impl TreeNodeRewriter for ColumnIndexRewriter {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_up(&mut self, expr: Self::Node) -> Result<Transformed<Self::Node>> {
        // Check if this is a Column expression
        if let Some(column) = expr.downcast_ref::<Column>() {
            let field_name = column.name();
            // Find the new index in the new schema
            if let Ok(new_index) = self.new_schema.index_of(field_name) {
                // Only update if the index changed
                if new_index != column.index() {
                    let new_column = Arc::new(Column::new(field_name, new_index));
                    return Ok(Transformed::yes(new_column as Arc<dyn PhysicalExpr>));
                }
            }
        }
        Ok(Transformed::no(expr))
    }
}

// Rewriter for match_all() to LIKE expressions in physical plans
#[derive(Debug, Clone)]
pub struct MatchAllRewriter {
    schema: SchemaRef,
    fields: Vec<(String, DataType)>,
}

impl MatchAllRewriter {
    pub fn new(schema: SchemaRef, fields: Vec<(String, DataType)>) -> Self {
        Self { schema, fields }
    }
}

impl TreeNodeRewriter for MatchAllRewriter {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_up(&mut self, expr: Self::Node) -> Result<Transformed<Self::Node>> {
        if is_match_all_physical(&expr) {
            match rewrite_match_all_physical(&expr, self.schema.clone(), &self.fields) {
                Ok(new_expr) => Ok(Transformed::yes(new_expr)),
                Err(e) => Err(DataFusionError::Internal(format!(
                    "Failed to rewrite match_all: {e}",
                ))),
            }
        } else {
            Ok(Transformed::no(expr))
        }
    }
}

fn rewrite_match_all_physical(
    expr: &Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
    fields: &[(String, DataType)],
) -> Result<Arc<dyn PhysicalExpr>> {
    let scalar_fn = expr.downcast_ref::<ScalarFunctionExpr>().ok_or_else(|| {
        DataFusionError::Internal("Expected ScalarFunctionExpr for match_all".to_string())
    })?;

    let name = scalar_fn.name();
    let args = scalar_fn.args();

    let cfg = get_config();

    if name == MATCH_ALL_UDF_NAME {
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "match_all() expects 1 argument, got {}",
                args.len()
            )));
        }

        let item_expr = &args[0];
        let item = extract_string_literal(item_expr)?;

        let mut expr_list = Vec::with_capacity(fields.len());
        let item = item
            .trim_start_matches("re:") // regex
            .trim_start_matches('*') // contains
            .trim_end_matches('*') // prefix or contains
            .to_string(); // remove prefix and suffix *

        for (field, data_type) in fields.iter() {
            let term = if cfg.common.utf8_view_enabled {
                Arc::new(Literal::new(ScalarValue::Utf8View(Some(format!(
                    "%{item}%"
                )))))
            } else if data_type == &DataType::LargeUtf8 {
                Arc::new(Literal::new(ScalarValue::LargeUtf8(Some(format!(
                    "%{item}%"
                )))))
            } else {
                Arc::new(Literal::new(ScalarValue::Utf8(Some(format!("%{item}%")))))
            };

            let new_expr = create_like_expr_with_not_null_physical(schema.as_ref(), field, term);
            expr_list.push(new_expr);
        }

        if expr_list.is_empty() {
            return Err(DataFusionError::Internal(
                infra::errors::ErrorCodes::FullTextSearchFieldNotFound.to_string(),
            ));
        }

        Ok(disjunction(expr_list))
    } else if name == FUZZY_MATCH_ALL_UDF_NAME {
        if args.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "fuzzy_match_all() expects 2 arguments, got {}",
                args.len()
            )));
        }

        let item_expr = &args[0];
        let item = extract_column(item_expr)?;
        let distance_expr = &args[1];
        let distance = extract_int64_literal(distance_expr)?;
        let fuzzy_expr = Arc::new(fuzzy_match_udf::FUZZY_MATCH_UDF.clone());

        let mut expr_list = Vec::with_capacity(fields.len());
        for (field, data_type) in fields.iter() {
            let term = if cfg.common.utf8_view_enabled {
                Arc::new(Literal::new(ScalarValue::Utf8View(Some(format!(
                    "%{item}%"
                )))))
            } else if data_type == &DataType::LargeUtf8 {
                Arc::new(Literal::new(ScalarValue::LargeUtf8(Some(format!(
                    "%{item}%"
                )))))
            } else {
                Arc::new(Literal::new(ScalarValue::Utf8(Some(format!("%{item}%")))))
            };

            let new_expr = Arc::new(ScalarFunctionExpr::try_new(
                fuzzy_expr.clone(),
                vec![
                    Arc::new(Column::new(field, schema.index_of(field).unwrap())),
                    term,
                    Arc::new(Literal::new(ScalarValue::Int64(Some(distance)))),
                ],
                schema.as_ref(),
                Arc::new(ConfigOptions::default()),
            )?) as Arc<dyn PhysicalExpr>;
            expr_list.push(new_expr);
        }

        if expr_list.is_empty() {
            return Err(DataFusionError::Internal(
                infra::errors::ErrorCodes::FullTextSearchFieldNotFound.to_string(),
            ));
        }

        Ok(disjunction(expr_list))
    } else if name == MATCH_ALL_HASH_UDF_NAME {
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "match_all_hash() expects 1 argument, got {}",
                args.len()
            )));
        }

        let item_expr = &args[0];
        let item = extract_string_literal(item_expr)?;

        // Hash the input string using MD5 - search for just the hash value (no brackets)
        let digest = md5::compute(item.as_bytes());
        let hash_value = format!("{digest:x}");

        let mut expr_list = Vec::with_capacity(fields.len());

        for (field, data_type) in fields.iter() {
            let term = if cfg.common.utf8_view_enabled {
                Arc::new(Literal::new(ScalarValue::Utf8View(Some(format!(
                    "%{hash_value}%"
                )))))
            } else if data_type == &DataType::LargeUtf8 {
                Arc::new(Literal::new(ScalarValue::LargeUtf8(Some(format!(
                    "%{hash_value}%"
                )))))
            } else {
                Arc::new(Literal::new(ScalarValue::Utf8(Some(format!(
                    "%{hash_value}%"
                )))))
            };

            let new_expr = create_like_expr_with_not_null_physical(schema.as_ref(), field, term);
            expr_list.push(new_expr);
        }

        if expr_list.is_empty() {
            return Err(DataFusionError::Internal(
                infra::errors::ErrorCodes::FullTextSearchFieldNotFound.to_string(),
            ));
        }

        Ok(disjunction(expr_list))
    } else {
        Err(DataFusionError::Internal(format!(
            "Unexpected function name: {name}",
        )))
    }
}

// create like expr with not null physical
fn create_like_expr_with_not_null_physical(
    schema: &Schema,
    field: &str,
    pattern: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    let column_expr = Arc::new(Column::new(field, schema.index_of(field).unwrap()));

    let is_not_null = Arc::new(IsNotNullExpr::new(column_expr.clone()));

    let like_expr = Arc::new(LikeExpr::new(
        false, // negated
        true,  // case_insensitive
        column_expr,
        pattern,
    ));

    Arc::new(BinaryExpr::new(is_not_null, Operator::And, like_expr))
}

// check if the expr contains match_all function
fn has_match_all_function(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let mut found = false;
    let _ = expr.apply(&mut |expr| {
        if is_match_all_physical(expr) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}

// check if the expr is match_all function
fn is_match_all_physical(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if let Some(scalar_fn) = expr.downcast_ref::<ScalarFunctionExpr>() {
        let name = scalar_fn.name();
        name.to_lowercase() == MATCH_ALL_UDF_NAME
            || name == FUZZY_MATCH_ALL_UDF_NAME
            || name == MATCH_ALL_HASH_UDF_NAME
    } else {
        false
    }
}

struct AddFstFieldsToProjection {
    fst_fields: Vec<String>,
    filter_schema: SchemaRef,
    pub filter_projection: Option<Vec<usize>>,
}

impl AddFstFieldsToProjection {
    pub fn new(fst_fields: Vec<String>, filter_schema: SchemaRef) -> Self {
        Self {
            fst_fields,
            filter_schema,
            filter_projection: None,
        }
    }
}

impl TreeNodeRewriter for AddFstFieldsToProjection {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Some(empty_exec) = plan.downcast_ref::<NewEmptyExec>() {
            let schema = empty_exec.full_schema();
            // used for read field from parquet file, because match_all function do not include
            // the field in argument, so when we rewrite match_all function, we need to add
            // the field to the projection
            let mut parquet_projection = self
                .fst_fields
                .iter()
                .map(|f| schema.index_of(f).unwrap())
                .collect::<Vec<_>>();
            if let Some(projection) = empty_exec.projection() {
                parquet_projection.extend(projection.iter().copied());
            }
            parquet_projection.sort();
            parquet_projection.dedup();

            // Restore the ordered output schema, including repeated column slots.
            // Only the scan-input projection above is a set.
            let filter_projection = self
                .filter_schema
                .fields()
                .iter()
                .map(|f| schema.index_of(f.name()).unwrap())
                .filter_map(|i| parquet_projection.iter().position(|f| *f == i))
                .collect::<Vec<_>>();
            self.filter_projection = Some(filter_projection);

            // create new NewEmptyExec with new projection
            let projected_schema = project_schema(&schema, Some(&parquet_projection))?;
            let new_empty_exec = NewEmptyExec::new(
                empty_exec.name(),
                projected_schema,
                Some(&parquet_projection),
                empty_exec.filters(),
                empty_exec.limit(),
                empty_exec.sorted_by_time(),
                empty_exec.full_schema(),
            );

            return Ok(Transformed::new(
                Arc::new(new_empty_exec) as Self::Node,
                true,
                TreeNodeRecursion::Stop,
            ));
        }
        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int64Array, StringArray, StringViewArray},
        compute::concat_batches,
    };
    use arrow_schema::DataType;
    use config::get_config;
    use datafusion::{
        arrow::{
            datatypes::{Field, Schema},
            record_batch::RecordBatch,
        },
        assert_batches_eq,
        catalog::MemTable,
        datasource::memory::MemorySourceConfig,
        execution::{runtime_env::RuntimeEnvBuilder, session_state::SessionStateBuilder},
        physical_plan::{collect, projection::ProjectionExec},
        prelude::{SessionConfig, SessionContext},
    };

    use super::*;
    use crate::datafusion::{table_provider::empty_table::NewEmptyTable, udf::match_all_udf};

    #[tokio::test]
    async fn test_rewrite_match_physical() {
        let sqls = [(
            "select * from t where match_all('open')",
            vec![
                "+------------+-------------+-------------+",
                "| _timestamp | name        | log         |",
                "+------------+-------------+-------------+",
                "| 1          | open        | o2          |",
                "| 3          | openobserve | openobserve |",
                "+------------+-------------+-------------+",
            ],
        )];

        let schema = if get_config().common.utf8_view_enabled {
            Arc::new(Schema::new(vec![
                Field::new("_timestamp", DataType::Int64, false),
                Field::new("name", DataType::Utf8View, false),
                Field::new("log", DataType::Utf8View, false),
            ]))
        } else {
            Arc::new(Schema::new(vec![
                Field::new("_timestamp", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("log", DataType::Utf8, false),
            ]))
        };

        let name_array: Arc<dyn Array> = if get_config().common.utf8_view_enabled {
            Arc::new(StringViewArray::from(vec![
                "open",
                "observe",
                "openobserve",
                "OBserve",
                "oo",
            ]))
        } else {
            Arc::new(StringArray::from(vec![
                "open",
                "observe",
                "openobserve",
                "OBserve",
                "oo",
            ]))
        };

        let log_array: Arc<dyn Array> = if get_config().common.utf8_view_enabled {
            Arc::new(StringViewArray::from(vec![
                "o2",
                "obSERVE",
                "openobserve",
                "o2",
                "oo",
            ]))
        } else {
            Arc::new(StringArray::from(vec![
                "o2",
                "obSERVE",
                "openobserve",
                "o2",
                "oo",
            ]))
        };

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                name_array,
                log_array,
            ],
        )
        .unwrap();

        let fields = vec![
            ("name".to_string(), DataType::Utf8),
            ("log".to_string(), DataType::Utf8),
        ];

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_default_features()
            .with_physical_optimizer_rules(vec![Arc::new(RewriteMatchPhysical::new(
                fields.clone(),
            ))])
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();
        ctx.register_udf(match_all_udf::MATCH_ALL_UDF.clone());
        ctx.register_udf(match_all_udf::FUZZY_MATCH_ALL_UDF.clone());

        for item in sqls {
            let df = ctx.sql(item.0).await.unwrap();
            let data = df.collect().await.unwrap();
            assert_batches_eq!(item.1, &data);
        }
    }

    async fn case_projection_fixture() -> (
        SessionContext,
        RecordBatch,
        Arc<dyn ExecutionPlan>,
        Vec<(String, DataType)>,
    ) {
        let string_type = if get_config().common.utf8_view_enabled {
            DataType::Utf8View
        } else {
            DataType::Utf8
        };
        let strings = |values: Vec<&str>| -> Arc<dyn Array> {
            if string_type == DataType::Utf8View {
                Arc::new(StringViewArray::from(values))
            } else {
                Arc::new(StringArray::from(values))
            }
        };
        // The read schema puts body before container, unlike the SELECT list.
        // Row 1 can match only through message, which is absent from SELECT.
        // Widening inserts message before container, requiring the CASE
        // predicate's container index to move as well as its output position.
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("body", string_type.clone(), false),
            Field::new("message", string_type.clone(), false),
            Field::new("k8s.container.name", string_type.clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                strings(vec![
                    "message-only body",
                    "stream duration is zero",
                    "stream duration is zero",
                    "unmatched body",
                ]),
                strings(vec![
                    "stream duration is zero",
                    "second message",
                    "stream duration is zero",
                    "no matching phrase",
                ]),
                strings(vec!["llm-router", "llm-router", "api-worker", "llm-router"]),
            ],
        )
        .unwrap();
        let fields = vec![
            ("body".to_string(), string_type.clone()),
            ("message".to_string(), string_type),
        ];
        // Keep the normal physical optimizer, including projection pushdown.
        // RewriteMatchPhysical runs afterwards, before scan materialization.
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(NewEmptyTable::new("t", schema)))
            .unwrap();
        ctx.register_udf(match_all_udf::MATCH_ALL_UDF.clone());
        let plan = ctx
            .sql(
                r#"SELECT _timestamp, "k8s.container.name", body
                   FROM t
                   WHERE CASE
                       WHEN ("k8s.container.name" = 'llm-router'
                             AND match_all('stream duration is zero'))
                       THEN TRUE ELSE FALSE END"#,
            )
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        (ctx, batch, plan, fields)
    }

    async fn collect_rewritten_case(
        ctx: &SessionContext,
        batch: &RecordBatch,
        plan: Arc<dyn ExecutionPlan>,
        fields: Vec<(String, DataType)>,
    ) -> Vec<RecordBatch> {
        let plan = RewriteMatchPhysical::new(fields)
            .optimize(plan, &ConfigOptions::default())
            .unwrap();
        // Materialize only after rewriting: a MemTable at planning time would
        // bypass the NewEmptyExec branch that widens the input for FTS fields.
        let plan = plan
            .transform_up(|plan| {
                if let Some(scan) = plan.downcast_ref::<NewEmptyExec>() {
                    let input = MemorySourceConfig::try_new_exec(
                        &[vec![batch.clone()]],
                        scan.full_schema(),
                        scan.projection().cloned(),
                    )?;
                    Ok(Transformed::yes(input as Arc<dyn ExecutionPlan>))
                } else {
                    Ok(Transformed::no(plan))
                }
            })
            .unwrap()
            .data;
        collect(plan, ctx.task_ctx()).await.unwrap()
    }

    #[tokio::test]
    async fn test_rewrite_match_case_preserves_output_order() {
        let (ctx, batch, plan, fields) = case_projection_fixture().await;
        let data = collect_rewritten_case(&ctx, &batch, plan, fields).await;
        // Exact named values and row selection, not just the output schema:
        // message-only matching must retain row 1; rows 3 and 4 must be absent.
        let expected = batch.slice(0, 2).project(&[0, 3, 1]).unwrap();
        assert_eq!(data[0].schema(), expected.schema());
        assert_eq!(concat_batches(&expected.schema(), &data).unwrap(), expected);
    }

    #[tokio::test]
    async fn test_rewrite_match_case_preserves_repeated_output_slots() {
        let (ctx, batch, plan, fields) = case_projection_fixture().await;
        let mut case_filter = None;
        plan.apply(|node| {
            if let Some(filter) = node.downcast_ref::<FilterExec>()
                && has_match_all_function(filter.predicate())
            {
                case_filter = Some((filter.predicate().clone(), filter.input().clone()));
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        let (predicate, input) = case_filter.expect("planned CASE filter");
        let input_schema = input.schema();
        let timestamp = input_schema.index_of("_timestamp").unwrap();
        let body = input_schema.index_of("body").unwrap();
        let container = input_schema.index_of("k8s.container.name").unwrap();
        // SQL projection optimization can share a repeated SELECT expression.
        // FilterExec also legally supports repeated output positions, consumed
        // by positional parents. Build that equivalent duplicate-selection
        // plan explicitly so deduplication cannot be masked by the optimizer.
        // Slots are already in read order, isolating dedup from the swap bug.
        let filter = Arc::new(
            FilterExecBuilder::new(predicate, input)
                .apply_projection(Some(vec![timestamp, body, container, container]))
                .unwrap()
                .build()
                .unwrap(),
        );
        let expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("_timestamp", 0)), "_timestamp".into()),
            (Arc::new(Column::new("body", 1)), "body".into()),
            (
                Arc::new(Column::new("k8s.container.name", 2)),
                "first_container".into(),
            ),
            (
                Arc::new(Column::new("k8s.container.name", 3)),
                "second_container".into(),
            ),
        ];
        let plan = Arc::new(ProjectionExec::try_new(expressions, filter).unwrap());
        let expected_schema = plan.schema();
        let expected = RecordBatch::try_new(
            expected_schema.clone(),
            [0, 1, 3, 3]
                .into_iter()
                .map(|index| batch.column(index).slice(0, 2))
                .collect(),
        )
        .unwrap();
        let data = collect_rewritten_case(&ctx, &batch, plan, fields).await;
        assert_eq!(data[0].schema(), expected_schema);
        assert_eq!(concat_batches(&expected_schema, &data).unwrap(), expected);
    }
}
