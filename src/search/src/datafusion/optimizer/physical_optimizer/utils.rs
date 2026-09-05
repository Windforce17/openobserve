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

use config::{TIMESTAMP_COL_NAME, meta::inverted_index::UNKNOWN_NAME};
use datafusion::{
    common::{Result, tree_node::TreeNode},
    error::DataFusionError,
    functions_aggregate::count::Count,
    logical_expr::Operator,
    physical_expr::{
        PhysicalExpr,
        aggregate::AggregateFunctionExpr,
        expressions::{Column, Literal},
    },
    physical_plan::{
        ExecutionPlan,
        expressions::{BinaryExpr, CastExpr, lit},
    },
    scalar::ScalarValue,
};

pub fn is_aggregate_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.exists(|plan| Ok(plan.name() == "AggregateExec"))
        .unwrap_or(false)
}

pub fn extract_string_literal(expr: &Arc<dyn PhysicalExpr>) -> Result<String> {
    if let Some(literal) = expr.downcast_ref::<Literal>() {
        match literal.value() {
            ScalarValue::Utf8(Some(s)) => Ok(s.clone()),
            ScalarValue::Utf8View(Some(s)) => Ok(s.to_string()),
            ScalarValue::LargeUtf8(Some(s)) => Ok(s.clone()),
            _ => Err(DataFusionError::Internal(format!(
                "Expected string literal, got: {:?}",
                literal.value()
            ))),
        }
    } else {
        Err(DataFusionError::Internal(
            "Expected literal expression for string argument".to_string(),
        ))
    }
}

pub fn extract_column(expr: &Arc<dyn PhysicalExpr>) -> Result<Column> {
    if let Some(column) = expr.downcast_ref::<Column>() {
        Ok(column.clone())
    } else {
        Err(DataFusionError::Internal(
            "Expected column expression".to_string(),
        ))
    }
}

pub fn extract_int64_literal(expr: &Arc<dyn PhysicalExpr>) -> Result<i64> {
    if let Some(literal) = expr.downcast_ref::<Literal>() {
        match literal.value() {
            ScalarValue::Int64(Some(s)) => Ok(*s),
            _ => Err(DataFusionError::Internal(format!(
                "Expected int64 literal, got: {:?}",
                literal.value()
            ))),
        }
    } else {
        Err(DataFusionError::Internal(
            "Expected literal expression for int64 argument".to_string(),
        ))
    }
}

// combine all exprs with OR operator
pub fn disjunction(
    predicates: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
) -> Arc<dyn PhysicalExpr> {
    disjunction_opt(predicates).unwrap_or_else(|| lit(true))
}

fn disjunction_opt(
    predicates: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
) -> Option<Arc<dyn PhysicalExpr>> {
    predicates
        .into_iter()
        .fold(None, |acc, predicate| match acc {
            None => Some(predicate),
            Some(acc) => Some(Arc::new(BinaryExpr::new(acc, Operator::Or, predicate))),
        })
}

pub fn is_column(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if expr.downcast_ref::<Column>().is_some() {
        true
    } else if let Some(expr) = expr.downcast_ref::<CastExpr>() {
        is_column(expr.expr())
    } else {
        false
    }
}

pub fn get_column_name(expr: &Arc<dyn PhysicalExpr>) -> &str {
    if let Some(expr) = expr.downcast_ref::<Column>() {
        expr.name()
    } else if let Some(expr) = expr.downcast_ref::<CastExpr>() {
        get_column_name(expr.expr())
    } else {
        UNKNOWN_NAME
    }
}

/// M16: `Some(column)` when the aggregate is a plain `count(column)` over a
/// bare (possibly cast) column that is NOT `_timestamp` — the null-skipping
/// per-column count [`is_count_rows_aggregate`] deliberately excludes
/// (count(_timestamp) counts rows: `_timestamp` is never null).
pub fn count_column_aggregate(expr: &AggregateFunctionExpr) -> Option<String> {
    if !expr.fun().name().eq_ignore_ascii_case("count") || expr.is_distinct() {
        return None;
    }
    let args = expr.expressions();
    // a BARE column only (no cast: a safe cast can null out values and
    // change the count; the fast path must answer the stored column)
    if args.len() != 1 {
        return None;
    }
    let column = args[0].downcast_ref::<Column>()?.name();
    (column != TIMESTAMP_COL_NAME).then(|| column.to_string())
}

/// M16: `Some((column, is_max))` when the aggregate is a plain `min(column)`
/// / `max(column)` over a bare (possibly cast) column.
pub fn min_max_column_aggregate(expr: &AggregateFunctionExpr) -> Option<(String, bool)> {
    let fun = expr.fun().name();
    let is_max = if fun.eq_ignore_ascii_case("max") {
        true
    } else if fun.eq_ignore_ascii_case("min") {
        false
    } else {
        return None;
    };
    if expr.is_distinct() {
        return None;
    }
    let args = expr.expressions();
    // a BARE column only (no cast: the fast path answers the stored values)
    if args.len() != 1 {
        return None;
    }
    let column = args[0].downcast_ref::<Column>()?;
    Some((column.name().to_string(), is_max))
}

pub fn is_count_rows_aggregate(expr: &AggregateFunctionExpr) -> bool {
    // The display/output name is not an aggregate identity. In particular, an
    // alias named `count(Int64(1))` must not make another expression count rows.
    if expr.fun().inner().downcast_ref::<Count>().is_none() || expr.is_distinct() {
        return false;
    }
    let args = expr.expressions();
    let [arg] = args.as_slice() else {
        return false;
    };
    if let Some(literal) = arg.downcast_ref::<Literal>() {
        return !literal.value().is_null();
    }
    // A cast can fail or introduce NULLs, even when its input is _timestamp.
    arg.downcast_ref::<Column>()
        .is_some_and(|column| column.name() == TIMESTAMP_COL_NAME)
}

pub fn is_value(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<Literal>().is_some()
}

pub fn is_only_timestamp_filter(expr: &[&Arc<dyn PhysicalExpr>]) -> bool {
    expr.iter().all(|expr| is_timestamp_filter(expr))
}

fn is_timestamp_filter(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
        match expr.op() {
            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
                let column = if is_value(expr.left()) && is_column(expr.right()) {
                    get_column_name(expr.right())
                } else if is_value(expr.right()) && is_column(expr.left()) {
                    get_column_name(expr.left())
                } else {
                    return false;
                };

                if column != TIMESTAMP_COL_NAME {
                    return false;
                }
            }
            _ => return false,
        }
    } else {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use datafusion::{
        physical_expr::expressions::Column, physical_plan::expressions::Literal,
        scalar::ScalarValue,
    };

    use super::*;

    fn utf8_literal(s: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Utf8(Some(s.to_string()))))
    }

    fn int64_literal(n: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int64(Some(n))))
    }

    fn col_expr(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, 0))
    }

    #[test]
    fn test_count_rows_requires_count_identity_and_uncast_nonnull_argument() -> Result<()> {
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::{
            functions_aggregate::{count::count_udaf, sum::sum_udaf},
            physical_expr::aggregate::AggregateExprBuilder,
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            TIMESTAMP_COL_NAME,
            DataType::Int64,
            false,
        )]));
        let timestamp = col_expr(TIMESTAMP_COL_NAME);
        let cast: Arc<dyn PhysicalExpr> =
            Arc::new(CastExpr::new(timestamp.clone(), DataType::Int8, None));
        let null: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(None)));
        for (arg, distinct, expected) in [
            (int64_literal(1), false, true),
            (timestamp, false, true),
            (cast, false, false),
            (null, false, false),
            (int64_literal(1), true, false),
        ] {
            let aggregate = AggregateExprBuilder::new(count_udaf(), vec![arg])
                .schema(schema.clone())
                .alias("count(Int64(1))")
                .with_distinct(distinct)
                .build()?;
            assert_eq!(is_count_rows_aggregate(&aggregate), expected);
        }
        let sum = AggregateExprBuilder::new(sum_udaf(), vec![int64_literal(1)])
            .schema(schema)
            .alias("count(Int64(1))")
            .build()?;
        assert!(!is_count_rows_aggregate(&sum));
        Ok(())
    }

    #[test]
    fn test_extract_string_literal_utf8() {
        let expr = utf8_literal("hello");
        assert_eq!(extract_string_literal(&expr).unwrap(), "hello");
    }

    #[test]
    fn test_extract_string_literal_utf8view() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Utf8View(Some(
            "viewval".to_string(),
        ))));
        assert_eq!(extract_string_literal(&expr).unwrap(), "viewval");
    }

    #[test]
    fn test_extract_string_literal_wrong_type_returns_error() {
        let expr = int64_literal(42);
        assert!(extract_string_literal(&expr).is_err());
    }

    #[test]
    fn test_extract_string_literal_column_returns_error() {
        let expr = col_expr("mycol");
        assert!(extract_string_literal(&expr).is_err());
    }

    #[test]
    fn test_extract_int64_literal() {
        let expr = int64_literal(99);
        assert_eq!(extract_int64_literal(&expr).unwrap(), 99);
    }

    #[test]
    fn test_extract_int64_literal_wrong_type_returns_error() {
        let expr = utf8_literal("not_int");
        assert!(extract_int64_literal(&expr).is_err());
    }

    #[test]
    fn test_is_value_true_for_literal() {
        let expr = utf8_literal("x");
        assert!(is_value(&expr));
    }

    #[test]
    fn test_is_value_false_for_column() {
        let expr = col_expr("col");
        assert!(!is_value(&expr));
    }

    #[test]
    fn test_is_column_true() {
        let expr = col_expr("mycol");
        assert!(is_column(&expr));
    }

    #[test]
    fn test_is_column_false_for_literal() {
        let expr = utf8_literal("val");
        assert!(!is_column(&expr));
    }

    #[test]
    fn test_get_column_name() {
        let expr = col_expr("mycolname");
        assert_eq!(get_column_name(&expr), "mycolname");
    }

    #[test]
    fn test_get_column_name_unknown_for_literal() {
        let expr = utf8_literal("val");
        assert_eq!(get_column_name(&expr), UNKNOWN_NAME);
    }

    #[test]
    fn test_is_aggregate_exec_false_for_empty_exec() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let exec: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        assert!(!is_aggregate_exec(&exec));
    }

    #[test]
    fn test_extract_column_ok() {
        let expr = col_expr("mycol");
        let col = extract_column(&expr).unwrap();
        assert_eq!(col.name(), "mycol");
    }

    #[test]
    fn test_extract_column_err_for_literal() {
        let expr = utf8_literal("val");
        assert!(extract_column(&expr).is_err());
    }

    #[test]
    fn test_disjunction_empty_returns_true_literal() {
        let result = disjunction(vec![]);
        // empty → lit(true)
        let lit = result.downcast_ref::<datafusion::physical_plan::expressions::Literal>();
        assert!(lit.is_some());
    }

    #[test]
    fn test_disjunction_single_returns_same() {
        let expr = utf8_literal("x");
        let result = disjunction(vec![expr.clone()]);
        assert!(is_value(&result));
    }

    #[test]
    fn test_is_only_timestamp_filter_empty_slice() {
        // all() on empty = true
        assert!(is_only_timestamp_filter(&[]));
    }
}
