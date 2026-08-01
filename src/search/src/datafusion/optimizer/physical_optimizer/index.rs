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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use config::text_tokenizer::o2_collect_search_tokens;
use datafusion::{
    arrow::datatypes::DataType,
    common::{
        Result,
        tree_node::{
            Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
        },
    },
    config::ConfigOptions,
    logical_expr::Operator,
    physical_expr::{ScalarFunctionExpr, conjunction, split_conjunction},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan, PhysicalExpr,
        expressions::{BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, NotExpr},
        filter::{FilterExec, FilterExecBuilder},
        limit::LocalLimitExec,
        projection::ProjectionExec,
    },
};
use hashbrown::HashMap;
use parking_lot::Mutex;

use crate::{
    datafusion::{
        distributed_plan::empty_exec::NewEmptyExec,
        optimizer::physical_optimizer::utils::{
            extract_string_literal, get_column_name, is_column, is_only_timestamp_filter, is_value,
        },
        udf::{
            MATCH_FIELD_IGNORE_CASE_UDF_NAME, MATCH_FIELD_UDF_NAME, STR_MATCH_UDF_IGNORE_CASE_NAME,
            STR_MATCH_UDF_NAME,
            match_all_udf::{FUZZY_MATCH_ALL_UDF_NAME, MATCH_ALL_UDF_NAME},
        },
    },
    index::{
        Condition, IndexCondition, normalize_numeric_literal, numeric_kind_of, try_physical_value,
    },
};

/// Index-eligible fields keyed by name, carrying their REGISTRY type: the
/// type decides which predicate shapes the term index can serve (string
/// fields keep today's rules; numeric/bool fields serve `=`/`!=`/`IN`/`IS
/// NOT NULL` with value-normalized literals — see
/// [`crate::index::NumericKind`]).
pub type IndexFields = HashMap<String, DataType>;

#[derive(Default, Debug)]
pub struct IndexRule {
    index_fields: IndexFields,
    index_condition: Arc<Mutex<Option<IndexCondition>>>,
    // this set to true when all filter can be extract to
    // index condition(except _timestamp filter)
    pub can_optimize: Arc<AtomicBool>,
}

impl IndexRule {
    pub fn new(
        index_fields: IndexFields,
        index_condition: Arc<Mutex<Option<IndexCondition>>>,
    ) -> Self {
        Self {
            index_fields,
            index_condition,
            can_optimize: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn can_optimize(&self) -> bool {
        self.can_optimize.load(Ordering::Relaxed)
    }
}

impl PhysicalOptimizerRule for IndexRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config::get_config().common.inverted_index_enabled {
            return Ok(plan);
        }

        let mut rewriter =
            IndexOptimizer::new(self.index_fields.clone(), self.index_condition.clone());
        let plan = plan.rewrite(&mut rewriter).data()?;

        // If no filter was found at all (e.g., SELECT count(*) FROM table),
        // and optimizer is enabled, we can still optimize
        if !rewriter.has_filter && rewriter.optimizer_enabled {
            rewriter.can_optimize = true;
        }

        // if all filter can be used in index, we can
        // use index optimizer rule to optimize the query
        if self.index_condition.lock().is_none() && rewriter.can_optimize {
            *self.index_condition.lock() = Some(IndexCondition {
                conditions: vec![Condition::All()],
            });
        }

        // set can_optimize to the index_rule
        self.can_optimize
            .store(rewriter.can_optimize, Ordering::Relaxed);

        Ok(plan)
    }

    fn name(&self) -> &str {
        "IndexConditionRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

struct IndexOptimizer {
    index_fields: IndexFields,
    index_condition: Arc<Mutex<Option<IndexCondition>>>,
    // set to true when the filter only have _timestamp filter
    can_optimize: bool,
    // set to true when the plan contains a FilterExec
    has_filter: bool,
    is_remove_filter: bool,
    optimizer_enabled: bool,
}

impl IndexOptimizer {
    pub fn new(
        index_fields: IndexFields,
        index_condition: Arc<Mutex<Option<IndexCondition>>>,
    ) -> Self {
        Self {
            index_fields,
            index_condition,
            can_optimize: false,
            has_filter: false,
            is_remove_filter: config::get_config()
                .common
                .feature_query_remove_filter_with_index,
            optimizer_enabled: config::get_config()
                .common
                .inverted_index_count_optimizer_enabled,
        }
    }

    #[cfg(test)]
    fn new_with_config(
        index_fields: IndexFields,
        index_condition: Arc<Mutex<Option<IndexCondition>>>,
        is_remove_filter: bool,
        optimizer_enabled: bool,
    ) -> Self {
        Self {
            index_fields,
            index_condition,
            can_optimize: false,
            has_filter: false,
            is_remove_filter,
            optimizer_enabled,
        }
    }
}

impl TreeNodeRewriter for IndexOptimizer {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Some(filter) = node.downcast_ref::<FilterExec>() {
            self.has_filter = true;
            let mut index_conditions = IndexCondition::new();
            let mut other_conditions = Vec::new();
            for expr in split_conjunction(filter.predicate()) {
                if is_expr_valid_for_index(expr, &self.index_fields) {
                    let condition = Condition::from_physical_expr(expr, &self.index_fields);
                    index_conditions.add_condition(condition);
                } else {
                    other_conditions.push(expr.clone());
                }
            }

            // check if we can remove the filter
            let is_remove_filter = self.is_remove_filter || index_conditions.can_remove_filter();

            // set the index condition
            if !index_conditions.is_empty() {
                *self.index_condition.lock() = Some(index_conditions);
            }

            if is_remove_filter {
                // if all filter can be used in index, we can
                // use index optimizer rule to optimize the query
                if self.optimizer_enabled
                    && is_only_timestamp_filter(&other_conditions.iter().collect::<Vec<_>>())
                {
                    self.can_optimize = true;
                }
                let plan = construct_filter_exec(filter, other_conditions)?;
                return Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop));
            } else {
                return Ok(Transformed::new(node, false, TreeNodeRecursion::Stop));
            }
        }
        Ok(Transformed::no(node))
    }
}

fn construct_filter_exec(
    filter: &FilterExec,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // The index-served conditions were just stripped from the predicate, but
    // the scan below still projects the columns only THEY referenced. That is
    // not merely a dead column: for files whose stored schema lacks it the
    // scan synthesizes the values by json-extracting `_source` — fetching and
    // parsing the raw row column for data nothing consumes (measured: a
    // schema-mixed term filter dragged ~1/3 of every object through the scan,
    // ~2s per file). Narrow the scan to what the remaining plan needs and
    // remap every surviving column index.
    let (child, exprs, out_projection) = narrow_scan_to_kept_columns(filter, exprs)?;
    if exprs.is_empty() {
        let mut plan = match &out_projection {
            Some(projection_indices) => {
                let filter_child_schema = child.schema();
                let proj_exprs = projection_indices
                    .iter()
                    .map(|p| {
                        let field = filter_child_schema.field(*p).clone();
                        (
                            Arc::new(Column::new(field.name(), *p)) as Arc<dyn PhysicalExpr>,
                            field.name().to_string(),
                        )
                    })
                    .collect::<Vec<_>>();
                Arc::new(ProjectionExec::try_new(proj_exprs, Arc::clone(&child))?)
            }
            None => Arc::clone(&child),
        };
        if let Some(fetch) = filter.fetch() {
            plan = Arc::new(LocalLimitExec::new(plan, fetch));
        }
        Ok(plan)
    } else {
        let plan = FilterExecBuilder::new(conjunction(exprs), child)
            .apply_projection_by_ref(out_projection.as_ref())?
            .with_fetch(filter.fetch())
            .build()?;
        Ok(Arc::new(plan))
    }
}

/// Narrow the filter's child scan ([`NewEmptyExec`]) to the columns still
/// consumed after index-served conditions were stripped: the filter's OUTPUT
/// columns plus everything the kept conditions reference. Returns the (new)
/// child, the kept conditions with column indices remapped to the narrowed
/// schema, and the filter's output projection remapped the same way.
///
/// Narrowing only happens when it is provably safe:
/// - the child is a `NewEmptyExec` (the pre-registration scan placeholder — its projection is what
///   the real table provider will scan);
/// - the filter HAS an output projection (`None` means the filter exposes its full input schema to
///   the plan above, so every column stays live).
///
/// Anything else passes through unchanged.
fn narrow_scan_to_kept_columns(
    filter: &FilterExec,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<(
    Arc<dyn ExecutionPlan>,
    Vec<Arc<dyn PhysicalExpr>>,
    Option<Arc<[usize]>>,
)> {
    let input = Arc::clone(filter.input());
    let out_projection = filter.projection().clone();
    let Some(empty) = input.downcast_ref::<NewEmptyExec>() else {
        log::info!(
            "[SCAN:NARROW] skipped: filter child is {}, not NewEmptyExec",
            input.name()
        );
        return Ok((input, exprs, out_projection));
    };
    let Some(out_proj) = out_projection else {
        log::info!(
            "[SCAN:NARROW] skipped: filter has no output projection (full schema flows up), scan {} keeps {} columns",
            empty.name(),
            empty.schema().fields().len()
        );
        return Ok((input, exprs, None));
    };

    // every input column the remaining plan still consumes
    let input_schema = empty.schema();
    let mut needed: std::collections::BTreeSet<usize> = out_proj.iter().copied().collect();
    for expr in &exprs {
        for column in datafusion::physical_expr::utils::collect_columns(expr) {
            needed.insert(column.index());
        }
    }
    if needed.len() >= input_schema.fields().len() {
        log::info!(
            "[SCAN:NARROW] skipped: all {} scan columns of {} still consumed",
            input_schema.fields().len(),
            empty.name()
        );
        return Ok((input, exprs, Some(out_proj)));
    }

    // keep list (sorted) and old->new index remap. Never narrow to ZERO
    // columns: a count-only shape (every consumed column stripped) keeps
    // `_timestamp` (always stored, 8B/row) so every downstream file format
    // sees a normal single-column scan instead of a row-count-only one.
    let mut needed = needed;
    if needed.is_empty() {
        let ts = input_schema
            .fields()
            .iter()
            .position(|f| f.name() == config::TIMESTAMP_COL_NAME)
            .unwrap_or(0);
        needed.insert(ts);
    }
    let keep: Vec<usize> = needed.into_iter().collect();
    let remap: HashMap<usize, usize> = keep
        .iter()
        .enumerate()
        .map(|(new_idx, old_idx)| (*old_idx, new_idx))
        .collect();

    let new_fields: Vec<_> = keep
        .iter()
        .map(|i| input_schema.field(*i).clone())
        .collect();
    let new_schema = Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
        new_fields,
        input_schema.metadata().clone(),
    ));
    // the scan projection indexes the FULL table schema; the current one is
    // positionally aligned with the scan's (projected) schema
    let new_scan_projection: Vec<usize> = match empty.projection() {
        Some(projection) => keep.iter().map(|i| projection[*i]).collect(),
        None => keep.clone(),
    };
    // pushed-down logical filters that reference a dropped column would make
    // the provider re-widen the scan — drop them (they are exactly the
    // index-served conditions being removed; pushdown filters are only ever
    // a pruning hint, never load-bearing for correctness)
    let kept_names: std::collections::HashSet<&str> = new_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let new_filters: Vec<datafusion::logical_expr::Expr> = empty
        .filters()
        .iter()
        .filter(|expr| {
            expr.column_refs()
                .iter()
                .all(|column| kept_names.contains(column.name.as_str()))
        })
        .cloned()
        .collect();

    log::info!(
        "[SCAN:NARROW] scan {}: {} -> {} columns (dropped: {})",
        empty.name(),
        input_schema.fields().len(),
        keep.len(),
        input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| !remap.contains_key(i))
            .map(|(_, f)| f.name().as_str())
            .collect::<Vec<_>>()
            .join(",")
    );
    let partitions = input.properties().output_partitioning().partition_count();
    let new_child: Arc<dyn ExecutionPlan> = Arc::new(
        NewEmptyExec::new(
            empty.name(),
            new_schema,
            Some(&new_scan_projection),
            &new_filters,
            empty.limit(),
            empty.sorted_by_time(),
            empty.full_schema(),
        )
        .with_partitions(partitions),
    );

    // remap the kept conditions and the filter's output projection
    let remapped_exprs = exprs
        .into_iter()
        .map(|expr| remap_expr_columns(expr, &remap))
        .collect::<Result<Vec<_>>>()?;
    let remapped_proj: Vec<usize> = out_proj.iter().map(|i| remap[i]).collect();
    Ok((new_child, remapped_exprs, Some(remapped_proj.into())))
}

/// Rewrite every [`Column`] in `expr` to its index in the narrowed schema.
fn remap_expr_columns(
    expr: Arc<dyn PhysicalExpr>,
    remap: &HashMap<usize, usize>,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.transform(|node| {
        if let Some(column) = node.downcast_ref::<Column>() {
            let new_index = *remap.get(&column.index()).ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(format!(
                    "narrowed scan lost column {} referenced by a kept condition",
                    column.name()
                ))
            })?;
            if new_index != column.index() {
                return Ok(Transformed::yes(
                    Arc::new(Column::new(column.name(), new_index)) as Arc<dyn PhysicalExpr>,
                ));
            }
        }
        Ok(Transformed::no(node))
    })
    .data()
}

// Check if the expression is valid for the index. `index_fields` carries the
// registry type per field: string fields accept every shape they did before;
// numeric/bool fields accept `=`/`!=`/`IN` only when every literal is
// value-servable ([`normalize_numeric_literal`]) — a fractional literal on
// an integer field, a non-finite float or a non-numeric text stays a plan
// filter — plus `IS NOT NULL` (key terms exist for every type). str_match /
// match_field remain string-field predicates: their substring/pattern
// semantics have no exact image over canonical numeric terms.
fn is_expr_valid_for_index(expr: &Arc<dyn PhysicalExpr>, index_fields: &IndexFields) -> bool {
    // literal servability for one comparison side against `column`
    let literal_ok = |column: &str, literal: &Arc<dyn PhysicalExpr>| -> bool {
        match index_fields.get(column).and_then(numeric_kind_of) {
            None => true, // string semantics: any literal text works
            Some(kind) => try_physical_value(literal)
                .is_some_and(|text| normalize_numeric_literal(kind, &text).is_some()),
        }
    };
    if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
        match expr.op() {
            Operator::Eq | Operator::NotEq => {
                let (column, literal) = if is_value(expr.left()) && is_column(expr.right()) {
                    (get_column_name(expr.right()), expr.left())
                } else if is_value(expr.right()) && is_column(expr.left()) {
                    (get_column_name(expr.left()), expr.right())
                } else {
                    return false;
                };

                if !index_fields.contains_key(column) || !literal_ok(column, literal) {
                    return false;
                }
            }
            Operator::And | Operator::Or => {
                return is_expr_valid_for_index(expr.left(), index_fields)
                    && is_expr_valid_for_index(expr.right(), index_fields);
            }
            _ => return false,
        }
    } else if let Some(expr) = expr.downcast_ref::<InListExpr>() {
        if !is_column(expr.expr()) {
            return false;
        }
        let column = get_column_name(expr.expr());
        if !index_fields.contains_key(column) {
            return false;
        }

        for value in expr.list() {
            if !is_value(value) || !literal_ok(column, value) {
                return false;
            }
        }
    } else if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
        let name = expr.name();
        return match name {
            MATCH_ALL_UDF_NAME => {
                expr.args().len() == 1
                    && extract_string_literal(&expr.args()[0])
                        .map(|s| !o2_collect_search_tokens(&s).is_empty())
                        .unwrap_or(false)
            }
            FUZZY_MATCH_ALL_UDF_NAME => expr.args().len() == 2,
            STR_MATCH_UDF_NAME
            | STR_MATCH_UDF_IGNORE_CASE_NAME
            | MATCH_FIELD_UDF_NAME
            | MATCH_FIELD_IGNORE_CASE_UDF_NAME => {
                expr.args().len() == 2
                    && index_fields
                        .get(get_column_name(&expr.args()[0]))
                        .is_some_and(|dt| numeric_kind_of(dt).is_none())
            }
            _ => false,
        };
    } else if let Some(expr) = expr.downcast_ref::<IsNotNullExpr>() {
        // `field IS NOT NULL` maps to the core-file key-existence terms,
        // which exist for columns of every type
        return is_column(expr.arg()) && index_fields.contains_key(get_column_name(expr.arg()));
    } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
        // `field IS NULL` is the exact key-term complement (writers omit
        // null values from _source and key terms) — same coverage
        return is_column(expr.arg()) && index_fields.contains_key(get_column_name(expr.arg()));
    } else if let Some(expr) = expr.downcast_ref::<NotExpr>() {
        return is_expr_valid_for_index(expr.arg(), index_fields);
    } else if is_column(expr) {
        // DataFusion simplifies `bool_field = true` to the BARE column (and
        // `= false` to `NOT bool_field`): a boolean-typed index field as a
        // predicate is exactly the true-valued probe
        return index_fields
            .get(get_column_name(expr))
            .is_some_and(|dt| matches!(dt, DataType::Boolean));
    } else {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, FieldRef, Schema};
    use datafusion::{
        catalog::MemTable,
        logical_expr::Operator,
        physical_expr::{
            PhysicalExpr,
            expressions::{BinaryExpr, Column, Literal},
        },
        physical_optimizer::PhysicalOptimizerRule,
        prelude::SessionContext,
        scalar::ScalarValue,
    };

    use super::{is_expr_valid_for_index, *};
    use crate::{
        datafusion::{
            optimizer::physical_optimizer::utils::is_only_timestamp_filter,
            udf::{
                match_all_udf::{self, MATCH_ALL_UDF},
                str_match_udf::{self, STR_MATCH_UDF},
            },
        },
        index::Condition,
    };

    fn eq(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, Operator::Eq, right))
    }

    fn ne(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, Operator::NotEq, right))
    }

    fn gt(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, Operator::Gt, right))
    }

    fn lt(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, Operator::Lt, right))
    }

    fn column(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, 0))
    }

    fn literal(value: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Utf8(Some(value.to_string()))))
    }

    fn and(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, Operator::And, right))
    }

    fn or(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, Operator::Or, right))
    }

    fn not(expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(NotExpr::new(expr))
    }

    fn match_all(lit: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(ScalarFunctionExpr::new(
            MATCH_ALL_UDF_NAME,
            Arc::new(MATCH_ALL_UDF.clone()),
            vec![literal(lit)],
            FieldRef::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(ConfigOptions::default()),
        ))
    }

    fn str_match(field: &str, lit: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(ScalarFunctionExpr::new(
            STR_MATCH_UDF_NAME,
            Arc::new(STR_MATCH_UDF.clone()),
            vec![column(field), literal(lit)],
            FieldRef::new(Field::new(field, DataType::Utf8, true)),
            Arc::new(ConfigOptions::default()),
        ))
    }

    fn in_list(field: &str, list: Vec<&str>) -> Arc<dyn PhysicalExpr> {
        Arc::new(
            InListExpr::try_new(
                column(field),
                list.iter().map(|lit| literal(lit)).collect(),
                false,
                &Schema::new(vec![Field::new(field, DataType::Utf8, true)]),
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_is_only_timestamp_filter() {
        // Create timestamp filter expressions
        let timestamp_col = column("_timestamp");
        let timestamp_literal = Arc::new(Literal::new(ScalarValue::Int64(Some(1234567890))));
        let timestamp_gt = gt(timestamp_col.clone(), timestamp_literal.clone());
        let timestamp_lt = lt(timestamp_col, timestamp_literal);

        let timestamp_filters = vec![&timestamp_gt, &timestamp_lt];
        assert!(is_only_timestamp_filter(&timestamp_filters));

        // Create non-timestamp filter
        let name_col = column("name");
        let name_literal = Arc::new(Literal::new(ScalarValue::Utf8(Some("test".to_string()))));
        let name_eq = eq(name_col, name_literal);

        let mixed_filters = vec![&timestamp_gt, &name_eq];
        assert!(!is_only_timestamp_filter(&mixed_filters));
    }

    /// FilterExec(predicate, projection) over a NewEmptyExec shaped like the
    /// prod traces scan: schema [_timestamp, duration, service.name], full
    /// table schema one column wider so scan-projection indices are distinct
    /// from field positions.
    fn narrowing_fixture(
        predicate: Arc<dyn PhysicalExpr>,
        projection: Option<Vec<usize>>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("duration", DataType::Int64, true),
            Field::new("service.name", DataType::Utf8, true),
        ]));
        let full_schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("duration", DataType::Int64, true),
            Field::new("service.name", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let svc = datafusion::logical_expr::Expr::Column(datafusion::common::Column::from_name(
            "service.name",
        ));
        let dur = datafusion::logical_expr::Expr::Column(datafusion::common::Column::from_name(
            "duration",
        ));
        use datafusion::prelude::lit;
        let filters = vec![svc.eq(lit("nexus-service")), dur.gt(lit(2000i64))];
        let child: Arc<dyn ExecutionPlan> = Arc::new(NewEmptyExec::new(
            "default",
            schema,
            Some(&vec![0usize, 1, 2]),
            &filters,
            None,
            false,
            full_schema,
        ));
        let mut builder = FilterExecBuilder::new(predicate, child);
        if let Some(projection) = projection {
            let projection: Arc<[usize]> = projection.into();
            builder = builder.apply_projection_by_ref(Some(&projection)).unwrap();
        }
        Arc::new(builder.build().unwrap())
    }

    fn svc_eq_nexus() -> Arc<dyn PhysicalExpr> {
        eq(
            Arc::new(Column::new("service.name", 2)),
            Arc::new(Literal::new(ScalarValue::Utf8(Some(
                "nexus-service".to_string(),
            )))),
        )
    }

    fn duration_gt_2000() -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("duration", 1)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(2000)))),
        ))
    }

    fn rewrite_with_index(
        plan: Arc<dyn ExecutionPlan>,
        indexed: &[&str],
    ) -> (Arc<dyn ExecutionPlan>, Option<IndexCondition>) {
        let index_condition = Arc::new(Mutex::new(None));
        let mut rewriter = IndexOptimizer::new_with_config(
            string_fields(indexed),
            index_condition.clone(),
            true,
            false,
        );
        let plan = plan.rewrite(&mut rewriter).unwrap().data;
        let condition = index_condition.lock().clone();
        (plan, condition)
    }

    /// THE prod regression (61s histogram): the index serves the term, the
    /// filter keeps only the numeric remainder — and the scan must STOP
    /// projecting the term column. Leaving it in makes files whose stored
    /// schema lacks the column synthesize it by json-extracting `_source`
    /// (~1/3 of every object fetched for a column nothing consumes).
    #[test]
    fn test_strip_narrows_scan_projection_for_mixed_predicate() {
        let predicate = Arc::new(BinaryExpr::new(
            svc_eq_nexus(),
            Operator::And,
            duration_gt_2000(),
        )) as Arc<dyn PhysicalExpr>;
        let plan = narrowing_fixture(predicate, Some(vec![0]));

        let (plan, condition) = rewrite_with_index(plan, &["service.name"]);

        let condition = condition.expect("term must move into the index condition");
        assert_eq!(
            condition.conditions,
            vec![Condition::Equal(
                "service.name".to_string(),
                "nexus-service".to_string()
            )]
        );

        let filter = plan
            .downcast_ref::<FilterExec>()
            .expect("the numeric remainder keeps a FilterExec");
        let predicate = format!("{}", filter.predicate());
        assert!(
            predicate.contains("duration@1 > 2000"),
            "kept condition must be remapped to the narrowed schema: {predicate}"
        );
        assert!(
            !predicate.contains("service.name"),
            "index-served condition must leave the plan filter: {predicate}"
        );
        assert_eq!(
            filter.schema().fields().len(),
            1,
            "the filter still outputs only _timestamp"
        );
        assert_eq!(filter.schema().field(0).name(), "_timestamp");

        let scan = filter
            .input()
            .downcast_ref::<NewEmptyExec>()
            .expect("the scan stays a NewEmptyExec");
        let names: Vec<_> = scan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            vec!["_timestamp", "duration"],
            "the term column must be gone from the scan"
        );
        assert_eq!(scan.projection(), Some(&vec![0usize, 1]));
        assert_eq!(
            scan.filters().len(),
            1,
            "the pushed-down term filter is dropped with its column"
        );
    }

    /// Every condition index-served: no filter remains, and the projection
    /// collapses the scan to exactly the consumed column.
    #[test]
    fn test_strip_all_conditions_projects_narrowed_scan() {
        let plan = narrowing_fixture(svc_eq_nexus(), Some(vec![0]));

        let (plan, condition) = rewrite_with_index(plan, &["service.name"]);
        assert!(condition.is_some());

        let projection = plan
            .downcast_ref::<ProjectionExec>()
            .expect("no remainder leaves a plain projection");
        assert_eq!(projection.schema().fields().len(), 1);
        assert_eq!(projection.schema().field(0).name(), "_timestamp");
        let scan = projection
            .input()
            .downcast_ref::<NewEmptyExec>()
            .expect("scan under the projection");
        assert_eq!(scan.schema().fields().len(), 1);
        assert_eq!(scan.schema().field(0).name(), "_timestamp");
        assert_eq!(scan.projection(), Some(&vec![0usize]));
    }

    /// A filter WITHOUT an output projection exposes its whole input schema
    /// upward — narrowing must not run (every column may be consumed above).
    #[test]
    fn test_no_filter_projection_keeps_scan_wide() {
        let predicate = Arc::new(BinaryExpr::new(
            svc_eq_nexus(),
            Operator::And,
            duration_gt_2000(),
        )) as Arc<dyn PhysicalExpr>;
        let plan = narrowing_fixture(predicate, None);

        let (plan, condition) = rewrite_with_index(plan, &["service.name"]);
        assert!(condition.is_some());

        let filter = plan.downcast_ref::<FilterExec>().expect("filter kept");
        let scan = filter
            .input()
            .downcast_ref::<NewEmptyExec>()
            .expect("scan unchanged");
        assert_eq!(
            scan.schema().fields().len(),
            3,
            "no narrowing without a filter output projection"
        );
        assert_eq!(scan.projection(), Some(&vec![0usize, 1, 2]));
    }

    /// A kept (non-index-servable) condition on the SAME column pins it: the
    /// column survives narrowing and the kept condition is remapped onto it.
    #[test]
    fn test_kept_condition_pins_shared_column() {
        // string > literal is not index-servable, so it stays in the filter
        let svc_gt = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("service.name", 2)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Utf8(Some("a".to_string())))),
        )) as Arc<dyn PhysicalExpr>;
        let predicate = Arc::new(BinaryExpr::new(svc_eq_nexus(), Operator::And, svc_gt))
            as Arc<dyn PhysicalExpr>;
        let plan = narrowing_fixture(predicate, Some(vec![0]));

        let (plan, condition) = rewrite_with_index(plan, &["service.name"]);
        assert!(condition.is_some());

        let filter = plan.downcast_ref::<FilterExec>().expect("filter kept");
        let predicate = format!("{}", filter.predicate());
        assert!(
            predicate.contains("service.name@1"),
            "shared column survives, remapped to the narrowed schema: {predicate}"
        );
        let scan = filter
            .input()
            .downcast_ref::<NewEmptyExec>()
            .expect("scan narrowed but keeps the pinned column");
        let names: Vec<_> = scan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec!["_timestamp", "service.name"]);
        assert_eq!(scan.projection(), Some(&vec![0usize, 2]));
    }

    fn string_fields(names: &[&str]) -> IndexFields {
        names
            .iter()
            .map(|name| (name.to_string(), DataType::Utf8))
            .collect()
    }

    #[test]
    fn test_is_expr_valid_for_index() {
        let index_fields = string_fields(&["name", "id"]);
        // PhysicalExpr, is_valid, Condition
        let case = vec![
            // name = 'test'
            (
                eq(column("name"), literal("test")),
                true,
                Some(Condition::Equal("name".to_string(), "test".to_string())),
            ),
            // name > 'test'
            (gt(column("name"), literal("test")), false, None),
            // name = 'bar' and match_all('error')
            (
                and(eq(column("name"), literal("bar")), match_all("error")),
                true,
                Some(Condition::And(
                    Box::new(Condition::Equal("name".to_string(), "bar".to_string())),
                    Box::new(Condition::MatchAll("error".to_string())),
                )),
            ),
            // name = 'bar' or match_all('error')
            (
                or(eq(column("name"), literal("bar")), match_all("error")),
                true,
                Some(Condition::Or(
                    Box::new(Condition::Equal("name".to_string(), "bar".to_string())),
                    Box::new(Condition::MatchAll("error".to_string())),
                )),
            ),
            // not(name = 'bar') and match_all('error') and str_match('name', 'test')
            (
                and(
                    not(eq(column("name"), literal("bar"))),
                    and(match_all("error"), str_match("name", "test")),
                ),
                true,
                Some(Condition::And(
                    Box::new(Condition::Not(Box::new(Condition::Equal(
                        "name".to_string(),
                        "bar".to_string(),
                    )))),
                    Box::new(Condition::And(
                        Box::new(Condition::MatchAll("error".to_string())),
                        Box::new(Condition::StrMatch(
                            "name".to_string(),
                            "test".to_string(),
                            true,
                        )),
                    )),
                )),
            ),
            // name != 'bar' and match_all('error') and str_match('name', 'test')
            (
                and(
                    ne(column("name"), literal("bar")),
                    and(match_all("error"), str_match("name", "test")),
                ),
                true,
                Some(Condition::And(
                    Box::new(Condition::NotEqual("name".to_string(), "bar".to_string())),
                    Box::new(Condition::And(
                        Box::new(Condition::MatchAll("error".to_string())),
                        Box::new(Condition::StrMatch(
                            "name".to_string(),
                            "test".to_string(),
                            true,
                        )),
                    )),
                )),
            ),
            // name in ('bar', 'test') and match_all('error')
            (
                and(in_list("name", vec!["bar", "test"]), match_all("error")),
                true,
                Some(Condition::And(
                    Box::new(Condition::In(
                        "name".to_string(),
                        vec!["bar".to_string(), "test".to_string()],
                        false,
                    )),
                    Box::new(Condition::MatchAll("error".to_string())),
                )),
            ),
            // match_all('c') should be invalid because tokens are empty
            (match_all("c"), false, None),
            // match_all('a') should be invalid because tokens are empty
            (match_all("a"), false, None),
            // status = 'test'
            (eq(column("status"), literal("test")), false, None),
            // name IS NOT NULL → key-existence lookup
            (
                Arc::new(IsNotNullExpr::new(column("name"))) as Arc<dyn PhysicalExpr>,
                true,
                Some(Condition::IsNotNull("name".to_string())),
            ),
            // name IS NULL → key-existence complement
            (
                Arc::new(IsNullExpr::new(column("name"))) as Arc<dyn PhysicalExpr>,
                true,
                Some(Condition::IsNull("name".to_string())),
            ),
            // name IS NOT NULL and match_all('error')
            (
                and(
                    Arc::new(IsNotNullExpr::new(column("name"))),
                    match_all("error"),
                ),
                true,
                Some(Condition::And(
                    Box::new(Condition::IsNotNull("name".to_string())),
                    Box::new(Condition::MatchAll("error".to_string())),
                )),
            ),
            // IS NOT NULL on a non-index field stays in the filter
            (
                Arc::new(IsNotNullExpr::new(column("status"))) as Arc<dyn PhysicalExpr>,
                false,
                None,
            ),
        ];

        for (expr, is_valid, condition) in case {
            if is_valid {
                assert!(is_expr_valid_for_index(&expr, &index_fields));
            } else {
                assert!(!is_expr_valid_for_index(&expr, &index_fields));
            }
            if let Some(condition) = condition {
                assert_eq!(
                    Condition::from_physical_expr(&expr, &index_fields),
                    condition
                );
            }
        }
    }

    #[test]
    fn test_is_expr_valid_for_index_numeric_fields() {
        use datafusion::scalar::ScalarValue;

        use crate::index::NumericKind;

        let index_fields: IndexFields = [
            ("code".to_string(), DataType::Int64),
            ("credit".to_string(), DataType::Float64),
            ("ok".to_string(), DataType::Boolean),
            ("name".to_string(), DataType::Utf8),
        ]
        .into_iter()
        .collect();
        let lit = |value: ScalarValue| -> Arc<dyn PhysicalExpr> { Arc::new(Literal::new(value)) };

        // servable numeric shapes extract as NumericCmp
        let expr = eq(column("code"), lit(ScalarValue::Int64(Some(38))));
        assert!(is_expr_valid_for_index(&expr, &index_fields));
        assert_eq!(
            Condition::from_physical_expr(&expr, &index_fields),
            Condition::NumericCmp("code".into(), vec!["38".into()], false, NumericKind::Int)
        );
        // an integral float literal on an int column is servable (the cast
        // comparison holds exactly for the integer value) ...
        let expr = eq(column("code"), lit(ScalarValue::Float64(Some(38.0))));
        assert!(is_expr_valid_for_index(&expr, &index_fields));
        // ... a fractional one is NOT (must stay a plan filter; the stored
        // JSON may drift, so "matches nothing" is not assumed)
        let expr = eq(column("code"), lit(ScalarValue::Float64(Some(38.5))));
        assert!(!is_expr_valid_for_index(&expr, &index_fields));
        // non-finite floats keep today's behavior: not extracted
        let expr = eq(column("credit"), lit(ScalarValue::Float64(Some(f64::NAN))));
        assert!(!is_expr_valid_for_index(&expr, &index_fields));
        let expr = eq(column("credit"), lit(ScalarValue::Float64(Some(38.5))));
        assert!(is_expr_valid_for_index(&expr, &index_fields));
        // a non-numeric text against a numeric field is unservable
        let expr = eq(column("credit"), literal("abc"));
        assert!(!is_expr_valid_for_index(&expr, &index_fields));
        // IN lists gate every literal
        let expr: Arc<dyn PhysicalExpr> = Arc::new(
            InListExpr::try_new(
                column("code"),
                vec![
                    lit(ScalarValue::Int64(Some(1))),
                    lit(ScalarValue::Int64(Some(2))),
                ],
                false,
                &Schema::new(vec![Field::new("code", DataType::Int64, true)]),
            )
            .unwrap(),
        );
        assert!(is_expr_valid_for_index(&expr, &index_fields));
        assert_eq!(
            Condition::from_physical_expr(&expr, &index_fields),
            Condition::NumericCmp(
                "code".into(),
                vec!["1".into(), "2".into()],
                false,
                NumericKind::Int
            )
        );
        // str_match has no exact image over canonical numeric terms:
        // string fields only
        assert!(is_expr_valid_for_index(
            &str_match("name", "test"),
            &index_fields
        ));
        assert!(!is_expr_valid_for_index(
            &str_match("code", "38"),
            &index_fields
        ));
        // IS NOT NULL rides on key terms, which every type gets
        let expr: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(column("credit")));
        assert!(is_expr_valid_for_index(&expr, &index_fields));
        // bool literals
        let expr = eq(column("ok"), lit(ScalarValue::Boolean(Some(true))));
        assert!(is_expr_valid_for_index(&expr, &index_fields));
        assert_eq!(
            Condition::from_physical_expr(&expr, &index_fields),
            Condition::NumericCmp("ok".into(), vec!["true".into()], false, NumericKind::Bool)
        );
    }

    #[test]
    fn test_index_rule_name_returns_expected() {
        let rule = IndexRule::new(IndexFields::new(), Arc::new(parking_lot::Mutex::new(None)));
        assert_eq!(rule.name(), "IndexConditionRule");
    }

    #[test]
    fn test_index_rule_schema_check_returns_true() {
        let rule = IndexRule::new(IndexFields::new(), Arc::new(parking_lot::Mutex::new(None)));
        assert!(rule.schema_check());
    }

    #[test]
    fn test_index_rule_can_optimize_initial_false() {
        let rule = IndexRule::new(IndexFields::new(), Arc::new(parking_lot::Mutex::new(None)));
        assert!(!rule.can_optimize());
    }

    #[tokio::test]
    async fn test_index_optimizer_optimizer_enabled() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["openobserve"])),
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(StringArray::from(vec!["success"])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        ctx.register_udf(match_all_udf::MATCH_ALL_UDF.clone());
        ctx.register_udf(str_match_udf::STR_MATCH_UDF.clone());
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // sql, can_optimizer, except_condition
        let cases = vec![
            (
                "SELECT count(*) from t where name = 'openobserve' and _timestamp > 1715395200000",
                true,
                Some(IndexCondition {
                    conditions: vec![Condition::Equal(
                        "name".to_string(),
                        "openobserve".to_string(),
                    )],
                }),
            ),
            (
                "SELECT count(*) from t where (name = 'openobserve' or match_all('error')) and _timestamp > 1715395200000",
                true,
                Some(IndexCondition {
                    conditions: vec![Condition::Or(
                        Box::new(Condition::Equal(
                            "name".to_string(),
                            "openobserve".to_string(),
                        )),
                        Box::new(Condition::MatchAll("error".to_string())),
                    )],
                }),
            ),
            (
                "SELECT count(*) from t where status = 'openobserve' or match_all('error') and _timestamp > 1715395200000",
                false,
                None,
            ),
        ];

        for (sql, can_optimizer, except_condition) in cases {
            let plan = ctx.state().create_logical_plan(sql).await.unwrap();
            let physical_plan = ctx.state().create_physical_plan(&plan).await.unwrap();
            let index_fields = string_fields(&["name", "id"]);
            let index_condition = Arc::new(Mutex::new(None));
            let is_remove_filter = true;
            let optimizer_enabled = true;
            let mut rewriter = IndexOptimizer::new_with_config(
                index_fields,
                index_condition.clone(),
                is_remove_filter,
                optimizer_enabled,
            );
            let _physical_plan = physical_plan.rewrite(&mut rewriter).unwrap().data;

            assert_eq!(index_condition.lock().clone(), except_condition);
            assert_eq!(rewriter.can_optimize, can_optimizer);
        }
    }

    /// End-to-end SQL extraction over REAL DataFusion literal coercion (the
    /// live problem-1 shape): numeric comparisons — including a string
    /// literal against a Float64 column, which DataFusion folds into a
    /// Float64 literal — become NumericCmp with value-normalized texts;
    /// numeric-looking literals on STRING fields keep string semantics; a
    /// fractional literal on an int field stays a plan filter.
    #[tokio::test]
    async fn test_index_optimizer_numeric_sql_extraction() {
        use crate::index::NumericKind;

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("code", DataType::Int64, false),
            Field::new("credit", DataType::Float64, false),
            Field::new("ok", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["38"])),
                Arc::new(Int64Array::from(vec![38])),
                Arc::new(arrow::array::Float64Array::from(vec![38.0])),
                Arc::new(arrow::array::BooleanArray::from(vec![true])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let index_fields: IndexFields = [
            ("name".to_string(), DataType::Utf8),
            ("code".to_string(), DataType::Int64),
            ("credit".to_string(), DataType::Float64),
            ("ok".to_string(), DataType::Boolean),
        ]
        .into_iter()
        .collect();

        let cases = vec![
            // the live problem-1 query shape: a STRING literal on a float
            // column — DataFusion coerces it to Float64(38.0)
            (
                "SELECT count(*) from t where credit = '38.0'",
                Some(IndexCondition {
                    conditions: vec![Condition::NumericCmp(
                        "credit".to_string(),
                        vec!["38.0".to_string()],
                        false,
                        NumericKind::Float,
                    )],
                }),
            ),
            (
                "SELECT count(*) from t where code = 38",
                Some(IndexCondition {
                    conditions: vec![Condition::NumericCmp(
                        "code".to_string(),
                        vec!["38".to_string()],
                        false,
                        NumericKind::Int,
                    )],
                }),
            ),
            // DataFusion rewrites small IN lists to OR chains at plan time;
            // each equality leaf expands independently (larger lists stay
            // InListExpr and take the multi-value NumericCmp arm)
            (
                "SELECT count(*) from t where code in (38, 40)",
                Some(IndexCondition {
                    conditions: vec![Condition::Or(
                        Box::new(Condition::NumericCmp(
                            "code".to_string(),
                            vec!["38".to_string()],
                            false,
                            NumericKind::Int,
                        )),
                        Box::new(Condition::NumericCmp(
                            "code".to_string(),
                            vec!["40".to_string()],
                            false,
                            NumericKind::Int,
                        )),
                    )],
                }),
            ),
            // DataFusion simplifies `ok = true` to the BARE column and
            // `ok = false` to `NOT ok` — both shapes extract
            (
                "SELECT count(*) from t where ok = true",
                Some(IndexCondition {
                    conditions: vec![Condition::NumericCmp(
                        "ok".to_string(),
                        vec!["true".to_string()],
                        false,
                        NumericKind::Bool,
                    )],
                }),
            ),
            (
                "SELECT count(*) from t where ok = false",
                Some(IndexCondition {
                    conditions: vec![Condition::Not(Box::new(Condition::NumericCmp(
                        "ok".to_string(),
                        vec!["true".to_string()],
                        false,
                        NumericKind::Bool,
                    )))],
                }),
            ),
            // numeric-looking literal on a STRING field keeps string
            // semantics (DataFusion casts the COLUMN, the literal text stays)
            (
                "SELECT count(*) from t where name = '38'",
                Some(IndexCondition {
                    conditions: vec![Condition::Equal("name".to_string(), "38".to_string())],
                }),
            ),
            // fractional literal on an int field: unservable, stays a filter
            ("SELECT count(*) from t where code = 38.5", None),
        ];

        for (sql, expected) in cases {
            let plan = ctx.state().create_logical_plan(sql).await.unwrap();
            let physical_plan = ctx.state().create_physical_plan(&plan).await.unwrap();
            let index_condition = Arc::new(Mutex::new(None));
            let mut rewriter = IndexOptimizer::new_with_config(
                index_fields.clone(),
                index_condition.clone(),
                false,
                false,
            );
            let _plan = physical_plan.rewrite(&mut rewriter).unwrap().data;
            assert_eq!(index_condition.lock().clone(), expected, "sql: {sql}");
        }
    }

    #[tokio::test]
    async fn test_index_optimizer_remove_filter_disabled() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["openobserve"])),
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(StringArray::from(vec!["success"])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        ctx.register_udf(match_all_udf::MATCH_ALL_UDF.clone());
        ctx.register_udf(str_match_udf::STR_MATCH_UDF.clone());
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // sql, can_optimizer, except_condition
        let cases = vec![
            (
                "SELECT count(*) from t where name = 'openobserve' and _timestamp > 1715395200000",
                true,
                Some(IndexCondition {
                    conditions: vec![Condition::Equal(
                        "name".to_string(),
                        "openobserve".to_string(),
                    )],
                }),
            ),
            (
                "SELECT count(*) from t where (name = 'openobserve' or match_all('error')) and _timestamp > 1715395200000",
                true,
                Some(IndexCondition {
                    conditions: vec![Condition::Or(
                        Box::new(Condition::Equal(
                            "name".to_string(),
                            "openobserve".to_string(),
                        )),
                        Box::new(Condition::MatchAll("error".to_string())),
                    )],
                }),
            ),
            (
                "SELECT count(*) from t where _timestamp > 1715395200000",
                true,
                None,
            ),
            (
                "SELECT count(*) from t where match_all('response node') and _timestamp > 1715395200000",
                false,
                Some(IndexCondition {
                    conditions: vec![Condition::MatchAll("response node".to_string())],
                }),
            ),
        ];

        for (sql, can_optimizer, except_condition) in cases {
            let plan = ctx.state().create_logical_plan(sql).await.unwrap();
            let physical_plan = ctx.state().create_physical_plan(&plan).await.unwrap();
            let index_fields = string_fields(&["name", "id"]);
            let index_condition = Arc::new(Mutex::new(None));
            let is_remove_filter = false;
            let optimizer_enabled = true;
            let mut rewriter = IndexOptimizer::new_with_config(
                index_fields,
                index_condition.clone(),
                is_remove_filter,
                optimizer_enabled,
            );
            let _physical_plan = physical_plan.rewrite(&mut rewriter).unwrap().data;

            assert_eq!(index_condition.lock().clone(), except_condition);
            assert_eq!(rewriter.can_optimize, can_optimizer);
        }
    }

    #[tokio::test]
    async fn test_index_optimizer_no_filter_count_star() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["openobserve"])),
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(StringArray::from(vec!["success"])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // sql, optimizer_enabled, can_optimize, expected_condition
        let cases = vec![
            // SELECT count(*) with no filter should be optimizable
            (
                "SELECT count(*) from t",
                true,
                true,
                Some(IndexCondition {
                    conditions: vec![Condition::All()],
                }),
            ),
            // SELECT count(*) with no filter but optimizer disabled
            ("SELECT count(*) from t", false, false, None),
        ];

        for (sql, optimizer_enabled, expected_can_optimize, expected_condition) in cases {
            let plan = ctx.state().create_logical_plan(sql).await.unwrap();
            let physical_plan = ctx.state().create_physical_plan(&plan).await.unwrap();
            let index_fields = string_fields(&["name", "id"]);
            let index_condition = Arc::new(Mutex::new(None));
            let mut rewriter = IndexOptimizer::new_with_config(
                index_fields,
                index_condition.clone(),
                false,
                optimizer_enabled,
            );
            let _physical_plan = physical_plan.rewrite(&mut rewriter).unwrap().data;

            // Apply the same post-rewrite logic as IndexRule::optimize
            if !rewriter.has_filter && rewriter.optimizer_enabled {
                rewriter.can_optimize = true;
            }
            if index_condition.lock().is_none() && rewriter.can_optimize {
                *index_condition.lock() = Some(IndexCondition {
                    conditions: vec![Condition::All()],
                });
            }

            assert_eq!(
                index_condition.lock().clone(),
                expected_condition,
                "Failed for sql: {}",
                sql
            );
            assert_eq!(
                rewriter.can_optimize, expected_can_optimize,
                "Failed for sql: {}",
                sql
            );
        }
    }
}

#[cfg(test)]
mod is_null_wire_path {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        physical_plan::{ExecutionPlan, displayable, filter::FilterExec},
        prelude::SessionContext,
    };

    use crate::datafusion::table_provider::empty_table::NewEmptyTable;

    /// Regression tests for the .37/.38 "IS NULL never extracts on prod"
    /// mystery. The plan machinery was correct all along (the miss shipped a
    /// stale image); these pin every layer IS NULL crosses so a real
    /// regression in any of them fails locally: the leader logical optimizer
    /// (SimplifyExpressions must not rewrite `IS NULL` on a nullable field),
    /// the physical planner (predicate stays `IsNullExpr`), the proto codec
    /// (round-trips as `IsNullExpr`), and the follower IndexRule (validity
    /// gate + condition builder extract `Condition::IsNull`).
    fn find_filter(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.downcast_ref::<FilterExec>().is_some() {
            return Some(Arc::clone(plan));
        }
        plan.children().iter().find_map(|child| find_filter(child))
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[tokio::test]
    async fn is_null_survives_logical_optimization() {
        let ctx = SessionContext::new();
        let provider = NewEmptyTable::new("t", test_schema()).with_partitions(2);
        ctx.register_table("t", Arc::new(provider)).unwrap();
        for (sql, kept) in [
            (
                "SELECT COUNT(*) FROM t WHERE name IS NULL",
                "t.name IS NULL",
            ),
            (
                "SELECT COUNT(*) FROM t WHERE name IS NOT NULL",
                "t.name IS NOT NULL",
            ),
        ] {
            let plan = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
            let text = plan.display_indent().to_string();
            assert!(
                text.contains(kept),
                "optimized logical plan for {sql} lost the predicate:\n{text}"
            );
        }
    }

    #[tokio::test]
    async fn is_null_extracts_after_proto_roundtrip() {
        let ctx = SessionContext::new();
        let provider = NewEmptyTable::new("t", test_schema()).with_partitions(2);
        ctx.register_table("t", Arc::new(provider)).unwrap();
        let df = ctx
            .sql("SELECT COUNT(*) FROM t WHERE name IS NULL")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        // freshly planned predicate is IsNullExpr (what the leader serializes)
        let filter = find_filter(&plan).expect("fresh plan has a FilterExec");
        let filter = filter.downcast_ref::<FilterExec>().unwrap();
        assert!(
            filter
                .predicate()
                .downcast_ref::<datafusion::physical_expr::expressions::IsNullExpr>()
                .is_some(),
            "fresh plan predicate is not IsNullExpr: {}",
            filter.predicate()
        );

        // the prod path: leader serializes, follower deserializes, THEN the
        // follower IndexRule (validity gate + condition builder) extracts
        let codec = crate::datafusion::distributed_plan::codec::get_physical_extension_codec();
        let bytes = datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec(
            Arc::clone(&plan),
            &codec,
        )
        .unwrap();
        let ctx2 = SessionContext::new();
        let back = datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(
            &bytes,
            &ctx2.task_ctx(),
            &codec,
        )
        .unwrap();
        let filter = find_filter(&back).expect("round-tripped plan has a FilterExec");
        let filter = filter.downcast_ref::<FilterExec>().unwrap();
        assert!(
            filter
                .predicate()
                .downcast_ref::<datafusion::physical_expr::expressions::IsNullExpr>()
                .is_some(),
            "round-tripped predicate is not IsNullExpr: {}",
            filter.predicate()
        );

        let mut index_fields: super::IndexFields = Default::default();
        index_fields.insert("name".to_string(), DataType::Utf8);
        let cond = Arc::new(parking_lot::Mutex::new(None));
        let rule = super::IndexRule::new(index_fields, Arc::clone(&cond));
        let optimized = datafusion::physical_optimizer::PhysicalOptimizerRule::optimize(
            &rule,
            back,
            ctx2.state().config_options(),
        )
        .unwrap();
        let extracted = cond.lock().clone();
        assert!(
            extracted.is_some(),
            "IS NULL must extract an index condition after the wire round-trip; plan:\n{}",
            displayable(optimized.as_ref()).indent(true)
        );
        assert_eq!(format!("{:?}", extracted.unwrap()), "name IS NULL");
    }
}
