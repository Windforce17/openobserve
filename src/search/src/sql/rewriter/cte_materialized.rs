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

//! Plans every CTE inline, as DataFusion 54 did.
//!
//! DataFusion 56 plans `WITH c AS MATERIALIZED (...)` as a shared
//! `MaterializedCteExec`; DataFusion 54 ignored the hint. In DataFusion 56
//! `TopKAggregation` pushes an outer `ORDER BY ... LIMIT` into the shared
//! CTE body and returns wrong rows (apache/datafusion#27219, fix pending in
//! apache/datafusion#27230). Clearing the hint keeps the DataFusion 54 plan
//! and results for these queries.

use std::ops::ControlFlow;

use sqlparser::ast::{Query, VisitorMut};

pub struct InlineMaterializedCteVisitor;

impl VisitorMut for InlineMaterializedCteVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(with) = query.with.as_mut() {
            for cte in &mut with.cte_tables {
                cte.materialized = None;
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        config::Dialect,
        datasource::MemTable,
        prelude::{SessionConfig, SessionContext},
    };
    use sqlparser::{ast::VisitMut, dialect::PostgreSqlDialect, parser::Parser};

    use super::*;

    fn inline_ctes(sql: &str) -> String {
        let mut statement = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();
        let _ = statement.visit(&mut InlineMaterializedCteVisitor);
        statement.to_string()
    }

    #[test]
    fn clears_materialized_hints_at_every_query_depth() {
        let sql = "SELECT * FROM (WITH c AS MATERIALIZED (SELECT x FROM t GROUP BY x) \
                   SELECT x FROM c) s WHERE x IN \
                   (WITH d AS NOT MATERIALIZED (SELECT x FROM t) SELECT x FROM d)";
        let rewritten = inline_ctes(sql);
        assert!(
            !rewritten.contains("MATERIALIZED"),
            "hint survived: {rewritten}"
        );
        let expected = "SELECT * FROM (WITH c AS (SELECT x FROM t GROUP BY x) \
                        SELECT x FROM c) s WHERE x IN \
                        (WITH d AS (SELECT x FROM t) SELECT x FROM d)";
        // token spans differ between the two texts, so compare canonical SQL
        let canonical = Parser::parse_sql(&PostgreSqlDialect {}, expected)
            .unwrap()
            .pop()
            .unwrap()
            .to_string();
        assert_eq!(rewritten, canonical);
    }

    async fn context_with_t() -> SessionContext {
        let mut config = SessionConfig::new().with_target_partitions(4);
        config.options_mut().sql_parser.dialect = Dialect::PostgreSQL;
        let ctx = SessionContext::new_with_config(config);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(1..=200))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Vec<Vec<i64>> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut rows = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
                            assert!(!column.is_null(row));
                            column.value(row)
                        })
                        .collect(),
                );
            }
        }
        rows
    }

    async fn explain(ctx: &SessionContext, sql: &str) -> String {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string()
    }

    /// The apache/datafusion#27219 reproductions return the correct rows once
    /// the hint is cleared, and the rewritten plan no longer materializes.
    #[tokio::test]
    async fn materialized_cte_under_order_by_limit_returns_correct_rows() {
        let ctx = context_with_t().await;
        let cases: [(&str, Vec<Vec<i64>>); 2] = [
            (
                "SELECT * FROM (WITH c AS MATERIALIZED (SELECT x FROM t GROUP BY x) \
                 SELECT x FROM c WHERE x > 100) s ORDER BY x LIMIT 3",
                vec![vec![101], vec![102], vec![103]],
            ),
            (
                "SELECT * FROM (WITH c AS MATERIALIZED (SELECT x FROM t GROUP BY x) \
                 SELECT c.x, m.n FROM c CROSS JOIN (SELECT count(*) AS n FROM c) m) s \
                 ORDER BY x LIMIT 3",
                vec![vec![1, 200], vec![2, 200], vec![3, 200]],
            ),
        ];
        for (sql, expected) in cases {
            // the unrewritten query really takes the DataFusion 56 materialized path
            assert!(
                explain(&ctx, sql).await.contains("MaterializedCteExec"),
                "DataFusion no longer materializes: {sql}"
            );
            let rewritten = inline_ctes(sql);
            let plan = explain(&ctx, &rewritten).await;
            assert!(
                !plan.contains("MaterializedCte"),
                "rewritten plan still materializes:\n{plan}"
            );
            assert_eq!(rows(&ctx, &rewritten).await, expected, "{rewritten}");
        }
    }
}
