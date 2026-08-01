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

//! Dotted-field identifier resolution (DESIGN.md §15.5/§15.6).
//!
//! Flattening keeps `.` in field names (`{"http":{"status":"500"}}` →
//! field `http.status`), but SQL parses an unquoted `http.status` as the
//! compound identifier `table http` . `column status`. This pre-pass rewrites
//! a [`Expr::CompoundIdentifier`] whose ROOT is not a table name / alias /
//! CTE of the query but whose dot-joined name IS a stream-schema field into
//! the equivalent quoted single identifier (`"http.status"`), everywhere an
//! expression can appear (SELECT / WHERE / GROUP BY / ORDER BY / HAVING …).
//!
//! Resolution rules:
//! - `a.b` where `a` is a table/alias/CTE → untouched (`table.column`).
//! - `a.b`/`a.b.c` where the joined name is a schema field → `"a.b"` / `"a.b.c"` (unquoted idents
//!   also match the field case-insensitively, mirroring DataFusion's ident normalization; the
//!   schema's spelling wins).
//! - `t.a.b` where `t` IS a table/alias and `a.b` is a schema field → `t."a.b"`.
//! - anything else → untouched.
//!
//! Queries against streams without dotted fields are never modified (the
//! rewriter is a no-op unless some schema field contains a `.`).

use std::{collections::HashSet, ops::ControlFlow, sync::Arc};

use datafusion::common::TableReference;
use hashbrown::HashMap;
use infra::schema::SchemaCache;
use sqlparser::ast::{
    Expr, Ident, ObjectName, Statement, TableFactor, Visit, VisitMut, Visitor, VisitorMut,
};

/// Rewrite unquoted dotted field references in `statement`. Entry point used
/// by `Sql::new_with_options`; see the module docs for the rules.
pub fn rewrite_dotted_fields(
    statement: &mut Statement,
    schemas: &HashMap<TableReference, Arc<SchemaCache>>,
) {
    // fast path: nothing to resolve when no stream has dotted fields
    let has_dotted_fields = schemas
        .values()
        .any(|schema| schema.fields_map().keys().any(|name| name.contains('.')));
    if !has_dotted_fields {
        return;
    }

    // pass 1: every name a compound-identifier root could legally refer to —
    // real table names (all qualifier parts), aliases, CTE names
    let mut collector = QualifierCollector::default();
    let _ = Visit::visit(&*statement, &mut collector);

    // pass 2: rewrite
    let mut visitor = DottedFieldsVisitor {
        schemas,
        qualifiers: collector.qualifiers,
    };
    let _ = VisitMut::visit(statement, &mut visitor);
}

/// Collects the lowercased set of table names, table aliases and CTE names
/// of the whole statement (joins, subqueries and CTE bodies included).
#[derive(Default)]
struct QualifierCollector {
    qualifiers: HashSet<String>,
}

impl QualifierCollector {
    fn add_ident(&mut self, ident: &Ident) {
        self.qualifiers.insert(ident.value.to_lowercase());
    }
}

impl Visitor for QualifierCollector {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        // every part: `FROM logs.stream` legitimizes both `logs.x` and
        // `stream.x` as table-qualified references
        for part in &relation.0 {
            if let Some(ident) = part.as_ident() {
                self.add_ident(ident);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        let alias = match table_factor {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::UNNEST { alias, .. }
            | TableFactor::NestedJoin { alias, .. } => alias.as_ref(),
            _ => None,
        };
        if let Some(alias) = alias {
            self.add_ident(&alias.name);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.add_ident(&cte.alias.name);
            }
        }
        ControlFlow::Continue(())
    }
}

struct DottedFieldsVisitor<'a> {
    schemas: &'a HashMap<TableReference, Arc<SchemaCache>>,
    qualifiers: HashSet<String>,
}

impl DottedFieldsVisitor<'_> {
    /// The schema field matching `name` — exact spelling first, then (for
    /// unquoted references, which DataFusion lowercases) case-insensitive.
    fn resolve_field(&self, name: &str, quoted: bool) -> Option<String> {
        for schema in self.schemas.values() {
            if schema.contains_field(name) {
                return Some(name.to_string());
            }
        }
        if quoted {
            return None;
        }
        let lower = name.to_lowercase();
        for schema in self.schemas.values() {
            if schema.contains_field(&lower) {
                return Some(lower);
            }
        }
        None
    }

    fn is_qualifier(&self, ident: &Ident) -> bool {
        self.qualifiers.contains(&ident.value.to_lowercase())
    }
}

/// Dot-join the identifier values (`[a, b, c]` → `a.b.c`); `quoted` reports
/// whether any part carried explicit quoting (exact spelling then).
fn joined_name(idents: &[Ident]) -> (String, bool) {
    let name = idents
        .iter()
        .map(|ident| ident.value.as_str())
        .collect::<Vec<_>>()
        .join(".");
    let quoted = idents.iter().any(|ident| ident.quote_style.is_some());
    (name, quoted)
}

impl VisitorMut for DottedFieldsVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::CompoundIdentifier(idents) = expr
            && idents.len() >= 2
        {
            if !self.is_qualifier(&idents[0]) {
                // root is no table: the whole dotted path may be one field
                let (name, quoted) = joined_name(idents);
                if let Some(field) = self.resolve_field(&name, quoted) {
                    *expr = Expr::Identifier(Ident::with_quote('"', field));
                }
            } else if idents.len() >= 3 {
                // root IS a table/alias: the remainder may be one field
                // (`t.a.b` → `t."a.b"`)
                let (name, quoted) = joined_name(&idents[1..]);
                if let Some(field) = self.resolve_field(&name, quoted) {
                    let root = idents[0].clone();
                    *expr = Expr::CompoundIdentifier(vec![root, Ident::with_quote('"', field)]);
                }
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use super::*;

    fn schemas_with(fields: &[&str]) -> HashMap<TableReference, Arc<SchemaCache>> {
        let schema = Schema::new(
            fields
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        );
        let mut schemas = HashMap::new();
        schemas.insert(
            TableReference::from("vixtest"),
            Arc::new(SchemaCache::new(schema)),
        );
        schemas
    }

    fn rewrite(sql: &str, fields: &[&str]) -> String {
        let mut statement = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();
        rewrite_dotted_fields(&mut statement, &schemas_with(fields));
        statement.to_string()
    }

    #[test]
    fn unquoted_dotted_field_in_where() {
        assert_eq!(
            rewrite(
                "SELECT * FROM vixtest WHERE http.status = 'x'",
                &["_timestamp", "http.status"],
            ),
            "SELECT * FROM vixtest WHERE \"http.status\" = 'x'"
        );
    }

    #[test]
    fn quoted_dotted_field_untouched() {
        assert_eq!(
            rewrite(
                "SELECT * FROM vixtest WHERE \"http.status\" = 'x'",
                &["_timestamp", "http.status"],
            ),
            "SELECT * FROM vixtest WHERE \"http.status\" = 'x'"
        );
    }

    #[test]
    fn table_qualified_column_still_resolves_as_table_column() {
        // `v.level` must stay table.column even though a field named
        // "v.level" could exist in another stream
        assert_eq!(
            rewrite(
                "SELECT v.level FROM vixtest AS v WHERE v.level = 'x'",
                &["level", "v.level", "http.status"],
            ),
            "SELECT v.level FROM vixtest AS v WHERE v.level = 'x'"
        );
        // and the real stream name works like an alias
        assert_eq!(
            rewrite(
                "SELECT vixtest.level FROM vixtest",
                &["level", "http.status"],
            ),
            "SELECT vixtest.level FROM vixtest"
        );
    }

    #[test]
    fn three_segment_field() {
        assert_eq!(
            rewrite(
                "SELECT a.b.c FROM vixtest WHERE a.b.c = 'x' GROUP BY a.b.c ORDER BY a.b.c",
                &["a.b.c", "http.status"],
            ),
            "SELECT \"a.b.c\" FROM vixtest WHERE \"a.b.c\" = 'x' GROUP BY \"a.b.c\" ORDER BY \"a.b.c\""
        );
    }

    #[test]
    fn qualified_dotted_field() {
        // alias root + dotted remainder that is a schema field
        assert_eq!(
            rewrite("SELECT v.http.status FROM vixtest AS v", &["http.status"],),
            "SELECT v.\"http.status\" FROM vixtest AS v"
        );
    }

    #[test]
    fn select_group_order_positions_covered() {
        assert_eq!(
            rewrite(
                "SELECT http.status, count(*) FROM vixtest WHERE http.status <> '' \
                 GROUP BY http.status HAVING count(*) > 1 ORDER BY http.status",
                &["http.status"],
            ),
            "SELECT \"http.status\", count(*) FROM vixtest WHERE \"http.status\" <> '' \
             GROUP BY \"http.status\" HAVING count(*) > 1 ORDER BY \"http.status\""
        );
    }

    #[test]
    fn unknown_dotted_name_untouched() {
        // no such field: keep the compound identifier (legacy behavior /
        // genuine table.column errors surface unchanged)
        assert_eq!(
            rewrite(
                "SELECT * FROM vixtest WHERE a.b = 'x'",
                &["_timestamp", "http.status"],
            ),
            "SELECT * FROM vixtest WHERE a.b = 'x'"
        );
    }

    #[test]
    fn no_dotted_schema_fields_is_a_noop() {
        // fast path: nothing rewritten (not even lowercase normalization)
        assert_eq!(
            rewrite(
                "SELECT * FROM vixtest WHERE A.B = 'x'",
                &["_timestamp", "level"],
            ),
            "SELECT * FROM vixtest WHERE A.B = 'x'"
        );
    }

    #[test]
    fn unquoted_reference_matches_case_insensitively() {
        // DataFusion lowercases unquoted idents; match the schema spelling
        assert_eq!(
            rewrite(
                "SELECT * FROM vixtest WHERE HTTP.Status = 'x'",
                &["http.status"],
            ),
            "SELECT * FROM vixtest WHERE \"http.status\" = 'x'"
        );
    }

    #[test]
    fn cte_and_subquery_qualifiers_respected() {
        // `c` is a CTE name: c.level stays table.column even though a field
        // "c.level" exists
        assert_eq!(
            rewrite(
                "WITH c AS (SELECT * FROM vixtest) SELECT c.level FROM c",
                &["level", "c.level"],
            ),
            "WITH c AS (SELECT * FROM vixtest) SELECT c.level FROM c"
        );
    }

    // ------------------------------------------------------------------
    // adversarial-review proving tests
    // ------------------------------------------------------------------

    /// A hostile FIELD NAME containing a double quote must round-trip: the
    /// rewritten statement is re-serialized (`Sql.sql = statement.to_string()`)
    /// and later re-parsed by DataFusion, so Ident Display escaping is
    /// load-bearing. sqlparser doubles the quote char — verify no SQL
    /// injection / parse break is possible through schema field names.
    #[test]
    fn review_field_name_with_embedded_quote_roundtrips() {
        // schema field: evil"x.y   (quote inside the first segment)
        let field = "evil\"x.y";
        // the querier must quote the segment to type it: "evil""x".y
        let sql = "SELECT * FROM vixtest WHERE \"evil\"\"x\".y = 'v'";
        let out = rewrite(sql, &["_timestamp", field]);
        // rewritten to the single quoted ident, with the inner quote doubled
        assert_eq!(out, "SELECT * FROM vixtest WHERE \"evil\"\"x.y\" = 'v'");
        // and the output re-parses cleanly to the same statement (no
        // injection, no truncation)
        let reparsed = Parser::parse_sql(&PostgreSqlDialect {}, &out)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(reparsed.to_string(), out);
        // the parsed comparison value is still the literal 'v' — nothing
        // escaped into a second expression
        assert!(out.ends_with("= 'v'"));
    }

    /// A field name that tries to smuggle an OR through the rewrite: the
    /// value is carried as one Ident, so operators inside the name stay
    /// inside the quotes.
    #[test]
    fn review_injection_shaped_field_name_stays_quoted() {
        let field = "a.b' OR '1'='1";
        // reference it with the exact quoting sqlparser needs
        let sql = "SELECT * FROM vixtest WHERE a.\"b' OR '1'='1\" = 'v'";
        let out = rewrite(sql, &[field]);
        assert_eq!(out, "SELECT * FROM vixtest WHERE \"a.b' OR '1'='1\" = 'v'");
        let reparsed = Parser::parse_sql(&PostgreSqlDialect {}, &out)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(reparsed.to_string(), out);
    }

    /// FINDING (low): qualifier collection is statement-global, not
    /// per-scope. An alias `http` in ANY branch of a UNION suppresses the
    /// rewrite of `http.status` in EVERY branch, so the first branch below
    /// fails to resolve the dotted field (DataFusion will then error with
    /// "table http not found" instead of reading the field). Documents the
    /// current, scope-insensitive behavior.
    #[test]
    fn review_union_alias_in_other_branch_suppresses_rewrite() {
        let out = rewrite(
            "SELECT http.status FROM vixtest UNION ALL SELECT x FROM other AS http",
            &["http.status"],
        );
        assert_eq!(
            out, "SELECT http.status FROM vixtest UNION ALL SELECT x FROM other AS http",
            "if this fails the rewriter became scope-aware - update the review report"
        );
        // control: without the aliased second branch the same reference
        // rewrites fine
        let out = rewrite("SELECT http.status FROM vixtest", &["http.status"]);
        assert_eq!(out, "SELECT \"http.status\" FROM vixtest");
    }

    /// A table alias equal to a dotted field's FIRST SEGMENT shadows the
    /// field (SQL resolution order: qualifier wins). `http.status` then
    /// resolves as alias `http`, column `status` — the field needs quoting.
    /// Correct per SQL semantics; pinned here so a future "fix" doesn't
    /// silently flip resolution.
    #[test]
    fn review_alias_matching_field_first_segment_shadows_field() {
        let out = rewrite(
            "SELECT http.status FROM vixtest AS http",
            &["http.status", "status"],
        );
        assert_eq!(out, "SELECT http.status FROM vixtest AS http");
        // quoted reference still reaches the field
        let out = rewrite(
            "SELECT \"http.status\" FROM vixtest AS http",
            &["http.status", "status"],
        );
        assert_eq!(out, "SELECT \"http.status\" FROM vixtest AS http");
    }

    /// No-op guarantee: a statement over a stream WITHOUT dotted fields is
    /// returned byte-identical (no re-quoting, no case normalization),
    /// including odd constructs.
    #[test]
    fn review_noop_without_dotted_fields_is_byte_identical() {
        for sql in [
            "SELECT a.b, C.D FROM vixtest WHERE x.y.z = 1 GROUP BY a.b",
            "WITH c AS (SELECT * FROM vixtest) SELECT c.level FROM c JOIN vixtest v ON v.id = c.id",
        ] {
            assert_eq!(rewrite(sql, &["_timestamp", "level", "id"]), sql);
        }
    }

    /// Dotted references inside function args, CASE, BETWEEN and IN lists
    /// are rewritten too (expression-position coverage).
    #[test]
    fn review_expression_positions_covered() {
        assert_eq!(
            rewrite(
                "SELECT count(http.status), CASE WHEN http.status = '500' THEN 1 ELSE 0 END \
                 FROM vixtest WHERE http.status IN ('200', '500') AND a.b BETWEEN 1 AND 2",
                &["http.status", "a.b"],
            ),
            "SELECT count(\"http.status\"), CASE WHEN \"http.status\" = '500' THEN 1 ELSE 0 END \
             FROM vixtest WHERE \"http.status\" IN ('200', '500') AND \"a.b\" BETWEEN 1 AND 2"
        );
    }
}
