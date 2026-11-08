// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Predicate-first source materialization. The original FilterExec remains
//! authoritative. Never move a LIMIT or change the stored row order here.

use anyhow::Context;
use arrow::array::BooleanArray;
use datafusion::{
    logical_expr::Operator,
    physical_expr::utils::{collect_columns, reassign_expr_columns},
    physical_plan::expressions as phys,
};

use super::*;

const MAX_MATCHES: usize = 65_536;
const WINDOW_ROWS: u64 = 8192;
// Sparse ids can occupy one roaring container each. Budget the vector, container
// headers and both bitmap copies, not only their serialized payload. Native
// decode memory still uses the reader's accounting; this is not an RSS bound.
pub(super) const SELECTION_RESERVATION: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(super) struct LateSourcePredicate {
    expr: Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
}

impl LateSourcePredicate {
    pub(super) fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    pub(super) fn try_new(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Option<Self>> {
        if !supported(expr, &mut 128) {
            return Ok(None);
        }
        let columns = collect_columns(expr);
        if columns.is_empty() || columns.len() > 32 {
            return Ok(None);
        }
        let mut indices = Vec::with_capacity(columns.len());
        for column in columns {
            if matches!(
                column.name(),
                SOURCE_COL_NAME | config::ORIGINAL_DATA_COL_NAME
            ) {
                return Ok(None);
            }
            indices.push(schema.index_of(column.name())?);
        }
        indices.sort_unstable();
        indices.dedup();
        let schema = Arc::new(schema.project(&indices)?);
        let expr = reassign_expr_columns(Arc::clone(expr), &schema)?;
        Ok(Some(Self { expr, schema }))
    }

    /// None means broad or source-dependent: use the unchanged streaming scan.
    /// Some(empty) proves no match. Errors and cancellation are never swallowed.
    pub(super) fn select(&self, docs: &VixDocs) -> anyhow::Result<Option<RowIdBitmap>> {
        vortex_index::check_read_cancelled()?;
        if docs.row_count() > u32::MAX as u64 {
            return Ok(None);
        }
        // A registry cast can fail on a row outside the timestamp window.
        // Do not introduce that failure by evaluating the cast earlier.
        if self.schema.fields().iter().any(|field| {
            docs.schema()
                .field_with_name(field.name())
                .is_ok_and(|stored| {
                    stored.data_type() != field.data_type()
                        && !(is_string(stored.data_type()) && is_string(field.data_type()))
                })
        }) {
            return Ok(None);
        }
        let plan = LogicalProjectionPlan::new(docs, &self.schema)?;
        if plan.physical_projection.iter().any(|name| {
            matches!(
                name.as_str(),
                SOURCE_COL_NAME | config::ORIGINAL_DATA_COL_NAME
            )
        }) {
            // Incomplete legacy files can hide a predicate column in _source.
            return Ok(None);
        }
        let budget = (docs.row_count() / 50).min(MAX_MATCHES as u64) as usize;
        let mut matches = Vec::with_capacity(budget);
        let mut broad = false;
        for start in (0..docs.row_count()).step_by(WINDOW_ROWS as usize) {
            vortex_index::check_read_cancelled()?;
            let mut row = start;
            docs.scan_docs_row_range(
                Some(&plan.physical_projection),
                start..(start + WINDOW_ROWS).min(docs.row_count()),
                &mut |batch| {
                    vortex_index::check_read_cancelled()?;
                    if broad {
                        return Ok(());
                    }
                    let batch = plan.project(&batch)?;
                    let mask = self
                        .expr
                        .evaluate(&batch)
                        .context("evaluate late-source predicate")?
                        .into_array(batch.num_rows())?;
                    let mask = mask
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| anyhow::anyhow!("late-source predicate is not Boolean"))?;
                    for index in 0..mask.len() {
                        if mask.is_valid(index) && mask.value(index) {
                            if matches.len() == budget {
                                broad = true;
                                break;
                            }
                            matches.push((row + index as u64) as u32);
                        }
                    }
                    row += batch.num_rows() as u64;
                    Ok(())
                },
            )
            .context("read late-source predicate columns")?;
            if broad {
                return Ok(None);
            }
        }
        Ok(Some(RowIdBitmap::from_row_ids(
            docs.row_count() as usize,
            matches,
        )))
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

// An allowlist prevents volatile UDFs, casts and arithmetic errors from being
// evaluated on rows that an upstream filter would otherwise discard. LIKE
// disjunctions are the existing match_all fallback, not a new token matcher.
fn supported(expr: &Arc<dyn PhysicalExpr>, remaining: &mut usize) -> bool {
    if *remaining == 0 {
        return false;
    }
    *remaining -= 1;
    let known = if let Some(binary) = expr.downcast_ref::<phys::BinaryExpr>() {
        matches!(
            binary.op(),
            Operator::And
                | Operator::Or
                | Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
        )
    } else {
        expr.downcast_ref::<Column>().is_some()
            || expr.downcast_ref::<Literal>().is_some()
            || expr.downcast_ref::<phys::LikeExpr>().is_some()
            || expr.downcast_ref::<phys::InListExpr>().is_some()
            || expr.downcast_ref::<phys::IsNullExpr>().is_some()
            || expr.downcast_ref::<phys::IsNotNullExpr>().is_some()
            || expr.downcast_ref::<phys::NotExpr>().is_some()
    };
    known
        && expr
            .children()
            .iter()
            .all(|child| supported(child, remaining))
}

#[cfg(test)]
mod expression_visit_tests {
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        execution::object_store::ObjectStoreUrl,
        physical_expr::expressions::{DynamicFilterPhysicalExpr, col, lit},
        physical_plan::execution_plan::plan_contains_expression_id,
    };

    use super::*;

    #[test]
    fn apply_expressions_reports_projection_and_late_dynamic_predicate() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("projected", DataType::Int64, false),
            Field::new("predicate_only", DataType::Int64, false),
        ]));
        let column = col("predicate_only", &schema)?;
        let dynamic: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![column], lit(true)));
        let id = dynamic.expression_id().expect("dynamic filter has an id");
        let mut source = VixCoreSource::new(TableSchema::from(Arc::clone(&schema)), None);
        // Any retained expression must be visible, including dynamic filters.
        // try_new currently declines dynamic inputs; inject one to pin the
        // traversal contract independently of the predicate admission policy.
        source.late_source_predicate = Some(LateSourcePredicate {
            expr: dynamic,
            schema: Arc::clone(&schema),
        });
        let mut columns = Vec::new();
        source.apply_expressions(&mut |root| {
            root.apply(|expr| {
                if let Some(column) = expr.downcast_ref::<Column>() {
                    columns.push(column.name().to_string());
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        assert!(columns.iter().any(|name| name == "projected"));
        assert!(
            columns
                .iter()
                .filter(|name| *name == "predicate_only")
                .count()
                >= 2
        );
        assert_eq!(
            source.apply_expressions(&mut |_| Ok(TreeNodeRecursion::Stop))?,
            TreeNodeRecursion::Stop
        );
        let conf = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
            .build();
        let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(conf);
        assert!(plan_contains_expression_id(&plan, id)?);
        Ok(())
    }
}
