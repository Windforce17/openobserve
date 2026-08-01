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

use std::{ops::ControlFlow, sync::Arc};

use config::{
    ID_COL_NAME, ORIGINAL_DATA_COL_NAME, TIMESTAMP_COL_NAME, meta::sql::OrderBy,
    utils::sql::AGGREGATE_UDF_LIST,
};
use datafusion::common::TableReference;
use hashbrown::{HashMap, HashSet};
use infra::schema::SchemaCache;
use sqlparser::ast::{
    Expr, GroupByExpr, Ident, OrderByKind, Query, SelectItem, SetExpr, Value, ValueWithSpan,
    VisitMut, VisitorMut,
};
use vortex_index::SOURCE_COL_NAME;

use crate::{sql::visitor::utils::FieldNameVisitor, utils::trim_quotes};

/// visit a sql to get all columns
pub struct ColumnVisitor<'a> {
    pub columns: HashMap<TableReference, HashSet<String>>,
    pub columns_alias: HashSet<(String, String)>,
    pub schemas: &'a HashMap<TableReference, Arc<SchemaCache>>,
    pub group_by: Vec<String>,
    pub order_by: Vec<(String, OrderBy)>, // field_name, order_by
    pub offset: Option<i64>,
    pub limit: Option<i64>,
    pub is_wildcard: bool,
    pub is_distinct: bool,
    pub has_agg_function: bool,
    /// Bare identifiers referenced in a WHERE clause that resolve to NO
    /// stream schema field (and are not internal columns). In SQL a WHERE
    /// clause cannot reference SELECT aliases, so for plain single-stream
    /// statements these are definitively unknown fields — `Sql::new` turns
    /// them into a deterministic "field not found" error instead of letting
    /// DataFusion fail later against whatever the plan schema happens to be.
    pub where_unresolved_fields: HashSet<String>,
}

impl<'a> ColumnVisitor<'a> {
    pub fn new(schemas: &'a HashMap<TableReference, Arc<SchemaCache>>) -> Self {
        Self {
            columns: HashMap::new(),
            columns_alias: HashSet::new(),
            schemas,
            group_by: Vec::new(),
            order_by: Vec::new(),
            offset: None,
            limit: None,
            is_wildcard: false,
            is_distinct: false,
            has_agg_function: false,
            where_unresolved_fields: HashSet::new(),
        }
    }

    /// Record `ident` as a referenced column of every stream whose schema
    /// carries it. Mirrors DataFusion's identifier normalization: an exact
    /// match first; when the identifier is UNQUOTED and misses, its
    /// lowercase form (which is what DataFusion will actually look up).
    /// Returns the resolved field name, if any schema matched.
    fn resolve_column(&mut self, ident: &sqlparser::ast::Ident) -> Option<String> {
        let mut resolved = None;
        for (name, schema) in self.schemas.iter() {
            if schema.contains_field(&ident.value) {
                self.columns
                    .entry(name.clone())
                    .or_default()
                    .insert(ident.value.clone());
                resolved = Some(ident.value.clone());
            }
        }
        if resolved.is_none() && ident.quote_style.is_none() {
            let lower = ident.value.to_lowercase();
            if lower != ident.value {
                for (name, schema) in self.schemas.iter() {
                    if schema.contains_field(&lower) {
                        self.columns
                            .entry(name.clone())
                            .or_default()
                            .insert(lower.clone());
                        resolved = Some(lower.clone());
                    }
                }
            }
        }
        resolved
    }
}

impl VisitorMut for ColumnVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Identifier(ident) => {
                let ident = ident.clone();
                self.resolve_column(&ident);
            }
            Expr::CompoundIdentifier(idents) => {
                // check if table_name is in schemas, otherwise the table_name maybe is a alias
                let ident = idents.last().unwrap().clone();
                self.resolve_column(&ident);
            }
            Expr::Function(f)
                if AGGREGATE_UDF_LIST
                    .contains(&trim_quotes(&f.name.to_string().to_lowercase()).as_str()) =>
            {
                self.has_agg_function = true;
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(order_by) = query.order_by.as_mut()
            && let OrderByKind::Expressions(exprs) = &mut order_by.kind
        {
            for order in exprs.iter_mut() {
                let mut name_visitor = FieldNameVisitor::new();
                let _ = order.expr.visit(&mut name_visitor);
                if name_visitor.field_names.len() == 1 {
                    let expr_name = name_visitor.field_names.iter().next().unwrap().to_string();
                    self.order_by.push((
                        expr_name,
                        if order.options.asc.unwrap_or(true) {
                            OrderBy::Asc
                        } else {
                            OrderBy::Desc
                        },
                    ));
                }
            }
        }
        if let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() {
            for select_item in select.projection.iter_mut() {
                match select_item {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        self.columns_alias
                            .insert((expr.to_string(), alias.value.to_string()));
                    }
                    SelectItem::Wildcard(_) => {
                        self.is_wildcard = true;
                    }
                    _ => {}
                }
            }
            if let GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
                for expr in exprs.iter_mut() {
                    let mut name_visitor = FieldNameVisitor::new();
                    let _ = expr.visit(&mut name_visitor);
                    if name_visitor.field_names.len() == 1 {
                        let expr_name = name_visitor.field_names.iter().next().unwrap().to_string();
                        self.group_by.push(expr_name);
                    }
                }
            }
            if select.distinct.is_some() {
                self.is_distinct = true;
            }
            // WHERE-clause identifiers that no stream schema resolves
            // (exact match, or the lowercase form DataFusion would look up
            // for an unquoted identifier). Internal columns are exempt.
            if let Some(selection) = select.selection.as_mut() {
                let mut where_idents = WhereIdentVisitor::default();
                let _ = selection.visit(&mut where_idents);
                for ident in where_idents.idents {
                    if is_internal_column(&ident.value) {
                        continue;
                    }
                    let resolves =
                        self.schemas
                            .values()
                            .any(|schema| schema.contains_field(&ident.value))
                            || (ident.quote_style.is_none()
                                && self.schemas.values().any(|schema| {
                                    schema.contains_field(&ident.value.to_lowercase())
                                }));
                    if !resolves {
                        self.where_unresolved_fields.insert(ident.value);
                    }
                }
            }
        } else if let sqlparser::ast::SetExpr::SetOperation { left, right, .. } =
            query.body.as_mut()
            && (has_wildcard(left) || has_wildcard(right))
        {
            self.is_wildcard = true;
        }
        let mut has_limit = false;
        if let Some(limit_clause) = query.limit_clause.as_ref()
            && let sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. } = limit_clause
        {
            if let Some(limit) = limit.as_ref()
                && let Expr::Value(ValueWithSpan { value, span: _ }) = limit
                && let Value::Number(n, _) = value
                && let Ok(num) = n.to_string().parse::<i64>()
                && self.limit.is_none()
            {
                has_limit = true;
                self.limit = Some(num);
            }
            if let Some(offset) = offset.as_ref()
                && let Expr::Value(ValueWithSpan { value, span: _ }) = &offset.value
                && let Value::Number(n, _) = value
                && let Ok(num) = n.to_string().parse::<i64>()
                && self.offset.is_none()
            {
                self.offset = Some(num);
            }
        }
        if has_limit && self.offset.is_none() {
            self.offset = Some(0);
        }
        ControlFlow::Continue(())
    }
}

/// Internal columns a query may reference without them being registry
/// fields.
fn is_internal_column(name: &str) -> bool {
    name == TIMESTAMP_COL_NAME
        || name == ID_COL_NAME
        || name == ORIGINAL_DATA_COL_NAME
        || name == SOURCE_COL_NAME
}

/// Collects the bare identifiers of a WHERE clause (quote style preserved).
/// Compound identifiers are deliberately skipped: after the dotted-fields
/// rewrite they are table-qualified references whose resolution DataFusion
/// owns.
#[derive(Default)]
struct WhereIdentVisitor {
    idents: Vec<Ident>,
}

impl VisitorMut for WhereIdentVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Identifier(ident) = expr {
            self.idents.push(ident.clone());
        }
        ControlFlow::Continue(())
    }
}

fn has_wildcard(set: &SetExpr) -> bool {
    match set {
        SetExpr::Select(select) => {
            for item in select.projection.iter() {
                if let SelectItem::Wildcard(_) = item {
                    return true;
                }
            }
            false
        }
        SetExpr::SetOperation { left, right, .. } => has_wildcard(left) || has_wildcard(right),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use sqlparser::dialect::GenericDialect;

    use super::*;

    #[test]
    fn test_column_visitor() {
        let sql = "SELECT name, age, COUNT(*) FROM users WHERE status = 'active' GROUP BY name, age ORDER BY name";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let mut schemas = HashMap::new();
        let schema = Schema::new(vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("age", DataType::Int32, false)),
            Arc::new(Field::new("status", DataType::Utf8, false)),
        ]);
        schemas.insert(
            TableReference::from("users"),
            Arc::new(SchemaCache::new(schema)),
        );

        let mut column_visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut column_visitor);

        // Should extract columns, group by, order by, and detect aggregate function
        assert!(column_visitor.has_agg_function);
        assert_eq!(column_visitor.group_by, vec!["name", "age"]);
        assert_eq!(
            column_visitor.order_by,
            vec![("name".to_string(), OrderBy::Asc)]
        );
    }

    #[test]
    fn test_column_visitor_with_limit() {
        let sql = "SELECT name, age, COUNT(*) FROM users WHERE status in (select distinct status from users order by status limit 10) GROUP BY name, age ORDER BY name limit 1000";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let mut schemas = HashMap::new();
        let schema = Schema::new(vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("age", DataType::Int32, false)),
            Arc::new(Field::new("status", DataType::Utf8, false)),
        ]);
        schemas.insert(
            TableReference::from("users"),
            Arc::new(SchemaCache::new(schema)),
        );

        let mut column_visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut column_visitor);

        // Should extract limit
        assert_eq!(column_visitor.limit, Some(1000));
    }

    fn visit(sql: &str, schema_fields: &[(&str, DataType)]) -> ColumnVisitorResult {
        // PostgreSqlDialect matches Sql::new (quote-style semantics matter here)
        let mut statement =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, sql)
                .unwrap()
                .pop()
                .unwrap();
        let schema = Schema::new(
            schema_fields
                .iter()
                .map(|(name, data_type)| Arc::new(Field::new(*name, data_type.clone(), true)))
                .collect::<Vec<_>>(),
        );
        let mut schemas = HashMap::new();
        schemas.insert(
            TableReference::from("t"),
            Arc::new(SchemaCache::new(schema)),
        );
        let mut visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut visitor);
        ColumnVisitorResult {
            columns: visitor
                .columns
                .get(&TableReference::from("t"))
                .cloned()
                .unwrap_or_default(),
            where_unresolved: visitor.where_unresolved_fields,
        }
    }

    struct ColumnVisitorResult {
        columns: HashSet<String>,
        where_unresolved: HashSet<String>,
    }

    /// WHERE identifiers that no schema field resolves are reported for the
    /// deterministic "field not found" error; resolvable, internal, and
    /// non-WHERE identifiers are not.
    #[test]
    fn test_where_unresolved_fields() {
        let fields = [
            ("level", DataType::Utf8),
            ("k8s.container.name", DataType::Utf8),
        ];

        // unknown WHERE field is reported; the known one is not
        let r = visit(
            r#"SELECT * FROM t WHERE "k8s.container.name" = 'x' AND missing_field = 'y'"#,
            &fields,
        );
        assert_eq!(
            r.where_unresolved,
            HashSet::from(["missing_field".to_string()])
        );
        assert!(r.columns.contains("k8s.container.name"));

        // internal columns are exempt even though no registry carries them
        let r = visit(
            "SELECT * FROM t WHERE _timestamp > 1 AND _o2_id > 0",
            &fields,
        );
        assert!(r.where_unresolved.is_empty());

        // SELECT/ORDER BY identifiers (e.g. aliases) are never validated —
        // only the WHERE clause is definitive
        let r = visit(
            "SELECT level AS lvl FROM t WHERE level = 'info' ORDER BY lvl",
            &fields,
        );
        assert!(r.where_unresolved.is_empty());

        // function arguments inside WHERE are identifiers too
        let r = visit("SELECT * FROM t WHERE str_match(nope, 'x')", &fields);
        assert_eq!(r.where_unresolved, HashSet::from(["nope".to_string()]));
    }

    /// Identifier-case handling mirrors DataFusion: an UNQUOTED identifier
    /// falls back to its lowercase form (which is what DataFusion looks
    /// up), and the resolved lowercase name joins `columns` so the plan
    /// schema can bind it; a QUOTED identifier stays exact.
    #[test]
    fn test_where_field_case_normalization() {
        let fields = [("level", DataType::Utf8)];

        // unquoted mixed case resolves via the lowercase form
        let r = visit("SELECT * FROM t WHERE Level = 'x'", &fields);
        assert!(r.where_unresolved.is_empty());
        assert!(r.columns.contains("level"));

        // quoted mixed case is exact — unresolved
        let r = visit(r#"SELECT * FROM t WHERE "Level" = 'x'"#, &fields);
        assert_eq!(r.where_unresolved, HashSet::from(["Level".to_string()]));
    }

    #[test]
    fn test_column_visitor_with_wildcard() {
        let sql = "SELECT * FROM users union select * from users";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let mut schemas = HashMap::new();
        let schema = Schema::new(vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("age", DataType::Int32, false)),
            Arc::new(Field::new("status", DataType::Utf8, false)),
        ]);
        schemas.insert(
            TableReference::from("users"),
            Arc::new(SchemaCache::new(schema)),
        );

        let mut column_visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut column_visitor);

        // Should extract columns, group by, order by, and detect aggregate function
        assert!(column_visitor.is_wildcard);
    }

    fn make_schemas() -> HashMap<TableReference, Arc<SchemaCache>> {
        let mut schemas = HashMap::new();
        let schema = Schema::new(vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("age", DataType::Int32, false)),
            Arc::new(Field::new("status", DataType::Utf8, false)),
        ]);
        schemas.insert(
            TableReference::from("users"),
            Arc::new(SchemaCache::new(schema)),
        );
        schemas
    }

    #[test]
    fn test_column_visitor_direct_wildcard() {
        let sql = "SELECT * FROM users";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let schemas = make_schemas();
        let mut visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut visitor);

        assert!(visitor.is_wildcard);
    }

    #[test]
    fn test_column_visitor_is_distinct() {
        let sql = "SELECT DISTINCT name FROM users";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let schemas = make_schemas();
        let mut visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut visitor);

        assert!(visitor.is_distinct);
    }

    #[test]
    fn test_column_visitor_columns_alias() {
        let sql = "SELECT name AS n, age AS a FROM users";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let schemas = make_schemas();
        let mut visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut visitor);

        assert!(
            visitor
                .columns_alias
                .iter()
                .any(|(expr, alias)| expr == "name" && alias == "n")
        );
        assert!(
            visitor
                .columns_alias
                .iter()
                .any(|(expr, alias)| expr == "age" && alias == "a")
        );
    }

    #[test]
    fn test_column_visitor_limit_and_offset() {
        let sql = "SELECT * FROM users LIMIT 100 OFFSET 50";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let schemas = make_schemas();
        let mut visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut visitor);

        assert_eq!(visitor.limit, Some(100));
        assert_eq!(visitor.offset, Some(50));
    }

    #[test]
    fn test_column_visitor_order_by_desc() {
        let sql = "SELECT name FROM users ORDER BY age DESC";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let schemas = make_schemas();
        let mut visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut visitor);

        assert_eq!(visitor.order_by, vec![("age".to_string(), OrderBy::Desc)]);
    }

    #[test]
    fn test_column_visitor_no_tests_when_no_schema_match() {
        let sql = "SELECT unknown_col FROM users";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let schemas = make_schemas();
        let mut visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut visitor);

        // unknown_col not in schema → columns map empty
        let users_ref = TableReference::from("users");
        assert!(visitor.columns.get(&users_ref).is_none_or(|s| s.is_empty()));
    }

    #[test]
    fn test_column_visitor_compound_identifier() {
        // table.field style → CompoundIdentifier branch in pre_visit_expr
        let sql = "SELECT users.name, users.age FROM users";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let schemas = make_schemas();
        let mut visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut visitor);

        let users_ref = TableReference::from("users");
        let columns = visitor.columns.get(&users_ref).unwrap();
        assert!(columns.contains("name"));
        assert!(columns.contains("age"));
    }

    #[test]
    fn test_column_visitor_order_by_compound_expr_skipped() {
        // ORDER BY expression with multiple fields → field_names.len() != 1 → order_by skipped
        let sql = "SELECT name FROM users ORDER BY name, age";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let schemas = make_schemas();
        let mut visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut visitor);

        // Both single-field order_by expressions captured
        assert_eq!(visitor.order_by.len(), 2);
        assert!(
            visitor
                .order_by
                .iter()
                .any(|(f, _)| f == "name" || f == "age")
        );
    }

    #[test]
    fn test_column_visitor_limit_no_explicit_offset_defaults_to_zero() {
        // LIMIT with no OFFSET → offset defaults to 0
        let sql = "SELECT * FROM users LIMIT 5";
        let mut statement = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();

        let schemas = make_schemas();
        let mut visitor = ColumnVisitor::new(&schemas);
        let _ = statement.visit(&mut visitor);

        assert_eq!(visitor.limit, Some(5));
        assert_eq!(visitor.offset, Some(0));
    }
}
