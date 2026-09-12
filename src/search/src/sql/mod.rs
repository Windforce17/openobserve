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

pub mod histogram;
use std::sync::{Arc, LazyLock as Lazy};

use arrow_schema::{DataType, Field};
use config::{
    TIMESTAMP_COL_NAME,
    datafusion::request::Request,
    get_config,
    meta::{
        search::SearchEventType,
        sql::{OrderBy, TableReferenceExt, resolve_stream_names_with_type},
        stream::StreamType,
    },
    utils::{query_select_utils::replace_o2_custom_patterns, sql::is_complex_query_stmt},
};
use datafusion::{arrow::datatypes::Schema, common::TableReference};
use hashbrown::{HashMap, HashSet};
use infra::{
    errors::{Error, ErrorCodes},
    schema::{SchemaCache, unwrap_stream_settings},
};
use proto::cluster_rpc::SearchQuery;
use regex::Regex;
use sqlparser::{ast::VisitMut, dialect::PostgreSqlDialect, parser::Parser};

use crate::sql::{
    rewriter::{
        add_o2_id::AddO2IdVisitor, add_timestamp::AddTimestampVisitor,
        dotted_fields::rewrite_dotted_fields, match_all_raw::MatchAllRawVisitor,
        remove_dashboard_placeholder::RemoveDashboardAllVisitor,
        track_total_hits::TrackTotalHitsVisitor,
    },
    schema::{generate_schema_fields, generate_select_star_schema, has_original_column},
    visitor::{
        column::ColumnVisitor,
        histogram_interval::{HistogramIntervalVisitor, validate_and_adjust_histogram_interval},
        match_all::MatchVisitor,
        partition_column::PartitionColumnVisitor,
    },
};

pub mod rewriter;
pub mod schema;
pub mod visitor;

pub static RE_ONLY_SELECT: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)select[ ]+\*").unwrap());
pub static RE_SELECT_FROM: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)SELECT (.*) FROM").unwrap());

pub static RE_HISTOGRAM: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)histogram\(([^\)]*)\)").unwrap());

/// Immutable metadata shared by all executions of one prepared query.
#[derive(Clone, Debug)]
pub struct SqlMetadata {
    pub sql: String,
    pub is_complex: bool,
    pub org_id: String,
    pub stream_type: StreamType,
    pub stream_names: Vec<TableReference>,
    pub has_match_all: bool, // match_all, only for single stream
    pub equal_items: HashMap<TableReference, Vec<(String, String)>>, /* table_name ->
                              * [(field_name, value)] */
    pub columns: HashMap<TableReference, HashSet<String>>, // table_name -> [field_name]
    pub aliases: Vec<(String, String)>,                    // field_name, alias
    pub schemas: HashMap<TableReference, Arc<SchemaCache>>,
    pub limit: i64,
    pub offset: i64,
    pub group_by: Vec<String>,
    pub order_by: Vec<(String, OrderBy)>,
    pub histogram_interval: Option<i64>,
    pub timezone: Option<String>,
    pub sorted_by_time: bool, // if only order by _timestamp
    pub pagination: SqlPagination,
}

/// A shallow execution view; bounds and sampling belong to this execution.
#[derive(Clone, Debug)]
pub struct Sql {
    pub metadata: Arc<SqlMetadata>,
    pub time_range: (i64, i64),
    pub sampling_config: Option<proto::cluster_rpc::SamplingConfig>,
}

impl std::ops::Deref for Sql {
    type Target = SqlMetadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

/// Parsed pagination and time-axis facts used only to certify cache coverage.
/// Default is deliberately uncertified for metadata constructed without an AST.
#[derive(Clone, Debug, Default)]
pub struct SqlPagination {
    root_limit: Option<i64>,
    legacy_limit: Option<i64>,
    cache_safe: bool,
    timestamp_columns: Vec<String>,
    has_explicit_order: bool,
}

impl SqlPagination {
    fn from_statement(
        statement: &mut sqlparser::ast::Statement,
        legacy_limit: Option<i64>,
        order_by: &[(String, OrderBy)],
    ) -> Self {
        let mut visitor = PaginationVisitor {
            pagination: Self {
                legacy_limit,
                cache_safe: matches!(statement, sqlparser::ast::Statement::Query(_)),
                ..Default::default()
            },
            depth: 0,
            order_by,
        };
        let _ = statement.visit(&mut visitor);
        visitor.pagination
    }

    fn execution_limit(&self, requested: i64) -> i64 {
        if requested == -1 || requested == 0 {
            self.legacy_limit.unwrap_or(requested)
        } else {
            requested
        }
    }
}

struct PaginationVisitor<'a> {
    pagination: SqlPagination,
    depth: usize,
    order_by: &'a [(String, OrderBy)],
}

fn literal_row_count(expr: &sqlparser::ast::Expr) -> Option<i64> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(value, _),
            ..
        }) => value.parse::<i64>().ok().filter(|value| *value >= 0),
        _ => None,
    }
}

fn is_physical_timestamp(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr;
    let ident = match expr {
        Expr::Identifier(ident) => Some(ident),
        Expr::CompoundIdentifier(idents) => idents.last(),
        _ => None,
    };
    ident.is_some_and(|ident| {
        if ident.quote_style.is_some() {
            ident.value == TIMESTAMP_COL_NAME
        } else {
            ident.value.eq_ignore_ascii_case(TIMESTAMP_COL_NAME)
        }
    })
}

fn is_histogram_function(function: &sqlparser::ast::Function) -> bool {
    function
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("histogram"))
}

fn is_direct_cache_time(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    if is_physical_timestamp(expr) {
        return true;
    }
    matches!(expr,
        Expr::Function(function) if is_histogram_function(function)
            && matches!(&function.args,
                FunctionArguments::List(arguments)
                    if matches!(arguments.args.first(),
                        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(timestamp)))
                            if is_physical_timestamp(timestamp)))
    )
}

fn plain_cache_wildcard(options: &sqlparser::ast::WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
        && options.opt_alias.is_none()
}

fn cache_timestamp_columns(select: &sqlparser::ast::Select) -> Vec<String> {
    use sqlparser::ast::{SelectItem, SelectItemQualifiedWildcardKind};
    let mut columns = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::ExprWithAlias { expr, alias } if is_direct_cache_time(expr) => {
                columns.push(alias.value.clone());
            }
            SelectItem::UnnamedExpr(expr) if is_physical_timestamp(expr) => {
                columns.push(TIMESTAMP_COL_NAME.to_string());
            }
            SelectItem::Wildcard(options)
            | SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(_),
                options,
            ) if plain_cache_wildcard(options) => {
                columns.push(TIMESTAMP_COL_NAME.to_string());
            }
            _ => {}
        }
    }
    // A duplicate/overwritten output name is not a physical time-axis proof.
    columns.retain(|column| {
        select
            .projection
            .iter()
            .filter(|item| match item {
                SelectItem::ExprWithAlias { alias, .. } => alias.value == *column,
                SelectItem::UnnamedExpr(expr) => {
                    column == TIMESTAMP_COL_NAME && is_physical_timestamp(expr)
                }
                SelectItem::Wildcard(_)
                | SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(_),
                    _,
                ) => column == TIMESTAMP_COL_NAME,
                _ => false,
            })
            .count()
            == 1
    });
    columns
}

impl sqlparser::ast::VisitorMut for PaginationVisitor<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut sqlparser::ast::Query) -> std::ops::ControlFlow<()> {
        use sqlparser::ast::{
            Distinct, LimitClause, SelectItem, SelectItemQualifiedWildcardKind, SetExpr,
            TableFactor,
        };
        let mut cap = None;
        let mut safe = true;
        match &query.limit_clause {
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                if let Some(limit) = limit {
                    cap = literal_row_count(limit);
                    safe &= cap.is_some();
                }
                safe &= offset
                    .as_ref()
                    .is_none_or(|offset| literal_row_count(&offset.value) == Some(0));
                safe &= limit_by.is_empty();
            }
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                cap = literal_row_count(limit);
                safe &= cap.is_some() && literal_row_count(offset) == Some(0);
            }
            None => {}
        }
        // The pinned DataFusion planner rejects FETCH. Do not let a cache hit
        // hide its normal NotImplemented error, even for a literal quantity.
        safe &= query.fetch.is_none();
        if self.depth == 0 && query.with.is_none() {
            self.pagination.root_limit = cap;
            if let Some(order) = &query.order_by {
                use sqlparser::ast::{Expr, OrderByKind};
                self.pagination.has_explicit_order = true;
                // ColumnVisitor's referenced-field extraction is not an order
                // proof: negation, positional/qualified references and casts can
                // disagree with the direction consumed by cache sorting.
                safe &= order.interpolate.is_none();
                safe &= match &order.kind {
                    OrderByKind::Expressions(expressions) => {
                        expressions.first().is_some_and(|first| {
                            if let Expr::Identifier(ident) = &first.expr {
                                let direction = if first.options.asc.unwrap_or(true) {
                                    OrderBy::Asc
                                } else {
                                    OrderBy::Desc
                                };
                                first.with_fill.is_none()
                                    && self.order_by.first().is_some_and(|(field, order)| {
                                        field == &ident.value && *order == direction
                                    })
                            } else {
                                false
                            }
                        })
                    }
                    _ => false,
                };
            }
            if let SetExpr::Select(select) = query.body.as_ref() {
                safe &= select.top.is_none() && select.from.len() == 1;
                // DISTINCT ON can retain a representative outside a later
                // subrange while discarding a row inside it. Output timestamps
                // therefore cannot certify the whole requested range.
                safe &= !matches!(select.distinct, Some(Distinct::On(_)));
                // Plain DISTINCT is time-local only when its deduplication key
                // includes the physical timestamp, not merely a derived bucket.
                if matches!(select.distinct, Some(Distinct::Distinct)) {
                    safe &= select.projection.iter().any(|item| match item {
                        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                            is_physical_timestamp(expr)
                        }
                        SelectItem::Wildcard(options)
                        | SelectItem::QualifiedWildcard(
                            SelectItemQualifiedWildcardKind::ObjectName(_),
                            options,
                        ) => plain_cache_wildcard(options),
                        _ => false,
                    });
                }
                // Window values and QUALIFY selections depend on rows outside
                // a clipped subrange, even when their projected timestamp is physical.
                safe &= select.qualify.is_none() && select.named_window.is_empty();
                if let Some(source) = select.from.first() {
                    safe &= source.joins.is_empty()
                        && matches!(&source.relation, TableFactor::Table {
                            alias, args: None, version: None, sample: None,
                            json_path: None, with_ordinality: false, ..
                        } if alias.as_ref().is_none_or(|alias| alias.columns.is_empty()));
                }
                if safe {
                    self.pagination.timestamp_columns = cache_timestamp_columns(select);
                }
            } else {
                safe = false;
            }
        } else {
            // CTEs/subqueries/joins can synthesize an identically named time
            // field. Without lineage, their output cannot certify scan coverage.
            safe = false;
        }
        self.pagination.cache_safe &= safe;
        self.depth += 1;
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_query(
        &mut self,
        _query: &mut sqlparser::ast::Query,
    ) -> std::ops::ControlFlow<()> {
        self.depth -= 1;
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut sqlparser::ast::Expr) -> std::ops::ControlFlow<()> {
        use sqlparser::ast::{
            Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value, ValueWithSpan,
        };
        if let Expr::Function(function) = expr
            && function.over.is_some()
        {
            self.pagination.cache_safe = false;
        }
        if let Expr::Function(function) = expr
            && is_histogram_function(function)
        {
            if let FunctionArguments::List(arguments) = &function.args {
                self.pagination.cache_safe &= matches!(arguments.args.first(),
                    Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(timestamp)))
                        if is_physical_timestamp(timestamp));
                if let Some(timezone) = arguments.args.get(2) {
                    self.pagination.cache_safe &= matches!(timezone,
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan {
                            value: Value::SingleQuotedString(zone), ..
                        }))) if matches!(zone.as_str(), "UTC" | "Etc/UTC" | "Z" | "+00:00")
                    );
                }
            } else {
                self.pagination.cache_safe = false;
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// Retained only while cache preparation finishes rewriting the execution SQL.
pub struct SqlPreparation {
    statement: sqlparser::ast::Statement,
    source: String,
    stream_names: Vec<TableReference>,
    schemas: HashMap<TableReference, Arc<SchemaCache>>,
}

impl SqlPreparation {
    pub async fn new(
        query: &SearchQuery,
        org_id: &str,
        stream_type: StreamType,
    ) -> Result<Self, Error> {
        let source = replace_o2_custom_patterns(&query.sql).unwrap_or_else(|_| query.sql.clone());
        let stream_names = resolve_stream_names_with_type(&source)
            .map_err(|e| Error::ErrorCode(ErrorCodes::SearchSQLNotValid(e.to_string())))?;
        let mut schemas = HashMap::with_capacity(stream_names.len());
        for stream in &stream_names {
            let stream_name = stream.stream_name();
            let schema =
                infra::schema::get(org_id, &stream_name, stream.get_stream_type(stream_type))
                    .await
                    .unwrap_or_else(|_| Schema::empty());
            if schema.fields().is_empty() {
                return Err(Error::ErrorCode(ErrorCodes::SearchStreamNotFound(
                    stream_name,
                )));
            }
            schemas.insert(stream.clone(), Arc::new(SchemaCache::new(schema)));
        }
        let statement = Parser::parse_sql(&PostgreSqlDialect {}, &source)
            .map_err(|e| Error::ErrorCode(ErrorCodes::SearchSQLNotValid(e.to_string())))?
            .pop()
            .unwrap();
        Ok(Self {
            statement,
            source,
            stream_names,
            schemas,
        })
    }

    pub fn finish(
        &self,
        query: &SearchQuery,
        org_id: &str,
        stream_type: StreamType,
        search_event_type: Option<SearchEventType>,
        extract_patterns: bool,
    ) -> Result<Sql, Error> {
        Sql::from_statement(
            query,
            org_id,
            stream_type,
            search_event_type,
            extract_patterns,
            self.statement.clone(),
            self.stream_names.clone(),
            self.schemas.clone(),
        )
    }

    pub fn into_sql(
        self,
        query: &SearchQuery,
        org_id: &str,
        stream_type: StreamType,
        search_event_type: Option<SearchEventType>,
        extract_patterns: bool,
    ) -> Result<Sql, Error> {
        Sql::from_statement(
            query,
            org_id,
            stream_type,
            search_event_type,
            extract_patterns,
            self.statement,
            self.stream_names,
            self.schemas,
        )
    }

    /// Mirror only the cache's actual textual edits before collecting finalized
    /// metadata, retaining the parsed statement whenever the edit maps to its AST.
    #[allow(clippy::too_many_arguments)]
    pub fn finish_cache(
        mut self,
        query: &SearchQuery,
        org_id: &str,
        stream_type: StreamType,
        search_event_type: Option<SearchEventType>,
        histogram_replacement: Option<(&str, &str)>,
        add_timestamp: bool,
    ) -> Result<Sql, Error> {
        let mut reparse = add_timestamp;
        if let Some((before, after)) = histogram_replacement
            && !reparse
        {
            let replacement = Parser::new(&PostgreSqlDialect {})
                .try_with_sql(after)
                .and_then(|mut parser| parser.parse_expr());
            if let Ok(sqlparser::ast::Expr::Function(replacement)) = replacement {
                let mut matches = self.source.match_indices(before).peekable();
                let mut location = sqlparser::tokenizer::Location::new(1, 1);
                let mut locations = HashSet::new();
                for (offset, ch) in self.source.char_indices() {
                    if matches.peek().is_some_and(|(start, _)| *start == offset) {
                        locations.insert(location);
                        matches.next();
                    }
                    if ch == '\n' {
                        location.line += 1;
                        location.column = 1;
                    } else {
                        location.column += 1;
                    }
                }
                reparse = locations.is_empty();
                let mut visitor = CacheHistogramReplacement {
                    locations,
                    replacement,
                };
                let _ = self.statement.visit(&mut visitor);
                reparse |= !visitor.locations.is_empty();
            } else {
                reparse = true;
            }
        }
        if reparse {
            // The legacy textual edits can touch comments/literals, nested calls
            // or projection spelling that does not map to the retained AST.
            // Parse the exact final text in that case; never guess an edit or
            // suppress its syntax error. Loaded schemas are still reused, and
            // no execution delta repeats this preparation.
            let source =
                replace_o2_custom_patterns(&query.sql).unwrap_or_else(|_| query.sql.clone());
            self.statement = Parser::parse_sql(&PostgreSqlDialect {}, &source)
                .map_err(|e| Error::ErrorCode(ErrorCodes::SearchSQLNotValid(e.to_string())))?
                .pop()
                .unwrap();
        }
        self.into_sql(query, org_id, stream_type, search_event_type, false)
    }
}

struct CacheHistogramReplacement {
    locations: HashSet<sqlparser::tokenizer::Location>,
    replacement: sqlparser::ast::Function,
}

impl sqlparser::ast::VisitorMut for CacheHistogramReplacement {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut sqlparser::ast::Expr) -> std::ops::ControlFlow<()> {
        use sqlparser::ast::Spanned;
        let sqlparser::ast::Expr::Function(function) = expr else {
            return std::ops::ControlFlow::Continue(());
        };
        // Match original source positions, not AST equality: the cache replaces
        // identical textual occurrences, not every histogram call.
        if self.locations.remove(&function.span().start) {
            // Function spans exclude parentheses and can include FILTER/OVER.
            // Replace the call's name/arguments only; retain those suffixes.
            function.name = self.replacement.name.clone();
            function.args = self.replacement.args.clone();
        }
        std::ops::ControlFlow::Continue(())
    }
}

impl Sql {
    pub async fn new_from_req(req: &Request, query: &SearchQuery) -> Result<Sql, Error> {
        let search_event_type = req
            .search_event_type
            .as_ref()
            .and_then(|s| SearchEventType::try_from(s.as_str()).ok());
        Self::new(query, &req.org_id, req.stream_type, search_event_type).await
    }

    pub async fn new(
        query: &SearchQuery,
        org_id: &str,
        stream_type: StreamType,
        search_event_type: Option<SearchEventType>,
    ) -> Result<Sql, Error> {
        Self::new_with_options(query, org_id, stream_type, search_event_type, false).await
    }

    pub async fn new_with_options(
        query: &SearchQuery,
        org_id: &str,
        stream_type: StreamType,
        search_event_type: Option<SearchEventType>,
        extract_patterns: bool,
    ) -> Result<Sql, Error> {
        SqlPreparation::new(query, org_id, stream_type)
            .await?
            .into_sql(
                query,
                org_id,
                stream_type,
                search_event_type,
                extract_patterns,
            )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_statement(
        query: &SearchQuery,
        org_id: &str,
        stream_type: StreamType,
        search_event_type: Option<SearchEventType>,
        extract_patterns: bool,
        mut statement: sqlparser::ast::Statement,
        stream_names: Vec<TableReference>,
        total_schemas: HashMap<TableReference, Arc<SchemaCache>>,
    ) -> Result<Sql, Error> {
        let offset = query.from as i64;
        let mut limit = query.size as i64;

        //********************Change the sql start*********************************//
        // 2. rewrite track_total_hits
        if query.track_total_hits {
            let mut trace_total_hits_visitor = TrackTotalHitsVisitor::new();
            let _ = statement.visit(&mut trace_total_hits_visitor);
        }

        // 3. rewrite all filter that include DASHBOARD_ALL with true
        let mut remove_dashboard_all_visitor = RemoveDashboardAllVisitor::new();
        let _ = statement.visit(&mut remove_dashboard_all_visitor);

        // 4. rewrite match_all_raw and match_all_raw_ignore_case to match_all
        let mut match_all_raw_visitor = MatchAllRawVisitor::new();
        let _ = statement.visit(&mut match_all_raw_visitor);

        // 4b. resolve unquoted dotted field references (`http.status` ->
        // `"http.status"`) against the stream schemas, so the collectors
        // below and DataFusion see one identifier instead of table.column
        rewrite_dotted_fields(&mut statement, &total_schemas);
        //********************Change the sql end*********************************//

        // the statement's complexity gates row-store star expansion and
        // identifier validation below (rewrites past this point never
        // change complexity)
        let is_complex = is_complex_query_stmt(&statement);

        // 5. get column name, alias, group by, order by
        let mut column_visitor = ColumnVisitor::new(&total_schemas);
        let _ = statement.visit(&mut column_visitor);

        // 5b. deterministic identifier validation (plain single-stream
        // statements): a WHERE field is validated against the stream's
        // LATEST schema (the union view — never a time-range-selected
        // version), and the error carries a STABLE message instead of
        // enumerating whatever field list the plan schema happens to hold.
        if !is_complex
            && stream_names.len() == 1
            && !column_visitor.where_unresolved_fields.is_empty()
        {
            let mut missing = column_visitor
                .where_unresolved_fields
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            missing.sort_unstable();
            return Err(Error::ErrorCode(ErrorCodes::SearchFieldNotFound(format!(
                "{}. Field not found in stream schema.",
                missing.join(", ")
            ))));
        }

        let columns = column_visitor.columns.clone();
        let aliases = column_visitor
            .columns_alias
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let group_by = column_visitor.group_by;
        let mut order_by = column_visitor.order_by;

        // check if need sort by time
        if order_by.is_empty()
            && !query.track_total_hits
            && stream_names.len() == 1
            && group_by.is_empty()
            && !column_visitor.has_agg_function
            && !column_visitor.is_distinct
        {
            order_by.push((TIMESTAMP_COL_NAME.to_string(), OrderBy::Desc));
        }
        let need_sort_by_time = order_by.len() == 1
            && order_by[0].0 == TIMESTAMP_COL_NAME
            && order_by[0].1 == OrderBy::Desc;

        // check if need exact limit and offset
        if (limit == -1 || limit == 0)
            && let Some(n) = column_visitor.limit
        {
            limit = n;
        }
        let legacy_limit = column_visitor.limit;

        // 6. get match_all() value
        let mut match_visitor = MatchVisitor::new(&total_schemas);
        let _ = statement.visit(&mut match_visitor);

        // 7. check if have full text search filed in stream
        if match_visitor.has_match_all && !match_visitor.is_support_match_all {
            return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                "match_all() should directly apply to stream, FROM clause should not be join/subuqery/cte".to_string(),
            )));
        } else if match_visitor.match_all_wrong_streams {
            return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                "match_all() should only apply to the stream that have full text search fields"
                    .to_string(),
            )));
        }

        // 8. generate used schema
        let mut used_schemas = HashMap::with_capacity(total_schemas.len());
        if column_visitor.is_wildcard {
            let has_original_column = has_original_column(&columns);
            used_schemas = generate_select_star_schema(
                total_schemas,
                &columns,
                has_original_column,
                &search_event_type,
                match_visitor.has_match_all,
                stream_type,
                // row-store-driven star: plain single-stream statements
                // only — joins/subqueries/CTEs keep the (referenced-column
                // bounded, §9) registry expansion
                !is_complex && stream_names.len() == 1,
            );
        } else {
            for (stream, schema) in total_schemas.iter() {
                let columns = columns.get(stream).cloned().unwrap_or(Default::default());
                let fields = generate_schema_fields(columns, schema, match_visitor.has_match_all);
                let schema = Schema::new(fields).with_metadata(schema.schema().metadata().clone());
                used_schemas.insert(stream.clone(), Arc::new(SchemaCache::new(schema)));
            }
        }

        // 9. get partition column value
        let mut partition_column_visitor = PartitionColumnVisitor::new(&used_schemas);
        let _ = statement.visit(&mut partition_column_visitor);

        // 10. pick up histogram interval
        let mut histogram_interval_visitor =
            HistogramIntervalVisitor::new((query.start_time, query.end_time));
        let _ = statement.visit(&mut histogram_interval_visitor);
        if let Some(error) = histogram_interval_visitor.error {
            return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(error)));
        }
        let histogram_interval = histogram_interval_visitor
            .is_histogram
            .then(|| {
                if query.histogram_interval > 0 {
                    Some(validate_and_adjust_histogram_interval(
                        query.histogram_interval,
                        (query.start_time, query.end_time),
                    ))
                } else {
                    histogram_interval_visitor.interval
                }
            })
            .flatten();
        let timezone = query.timezone.clone();

        //********************Change the sql start*********************************//
        // 11. add _timestamp and _o2_id if need
        if !is_complex {
            let mut add_timestamp_visitor = AddTimestampVisitor::new();
            let _ = statement.visit(&mut add_timestamp_visitor);
            if o2_id_is_needed(&used_schemas, &search_event_type) {
                let mut add_o2_id_visitor = AddO2IdVisitor::new();
                let _ = statement.visit(&mut add_o2_id_visitor);
            }
        }
        //********************Change the sql end************************************//

        // 13. replace the Utf8 to Utf8View type
        let final_schemas = finalize_schemas(&used_schemas);
        let pagination = SqlPagination::from_statement(&mut statement, legacy_limit, &order_by);

        Ok(Sql {
            metadata: Arc::new(SqlMetadata {
                sql: statement.to_string(),
                is_complex,
                org_id: org_id.to_string(),
                stream_type,
                stream_names,
                has_match_all: match_visitor.has_match_all,
                equal_items: partition_column_visitor.equal_items,
                columns,
                aliases,
                schemas: final_schemas,
                limit,
                offset,
                group_by,
                order_by,
                histogram_interval,
                timezone,
                sorted_by_time: need_sort_by_time,
                pagination,
            }),
            time_range: (query.start_time, query.end_time),
            sampling_config: parse_sampling_config(
                query,
                histogram_interval,
                (query.start_time, query.end_time),
                extract_patterns,
            ),
        })
    }

    /// Bind the finalized root metadata without widening the scan to histogram
    /// buckets. The optimizer also uses this end for named-zone DST resolution.
    pub fn bind(&self, query: &SearchQuery) -> Self {
        let time_range = (query.start_time, query.end_time);
        let limit = self.pagination.execution_limit(query.size as i64);
        let offset = query.from as i64;
        let mut metadata = self.metadata.clone();
        if limit != self.limit || offset != self.offset {
            let metadata = Arc::make_mut(&mut metadata);
            metadata.limit = limit;
            metadata.offset = offset;
        }
        Self {
            metadata,
            time_range,
            sampling_config: parse_sampling_config(
                query,
                self.histogram_interval,
                time_range,
                false,
            ),
        }
    }

    /// Actual result cap for a zero-offset cacheable execution: `Some(-1)` is
    /// unlimited, `Some(n >= 0)` is a finite cap, and `None` is uncertified.
    /// A full page at this cap cannot prove complete time-range coverage.
    pub fn cache_coverage_limit(&self, ts_column: &str) -> Option<i64> {
        if !self.pagination.cache_safe
            || self.offset != 0
            || !self
                .pagination
                .timestamp_columns
                .iter()
                .any(|column| column == ts_column)
        {
            return None;
        }
        let default_cap = if self.limit > config::QUERY_WITH_NO_LIMIT && self.limit <= 0 {
            Some(i64::try_from(get_config().limit.query_default_limit).ok()?)
        } else {
            None
        };
        // AddSortAndLimit preserves an existing SQL Limit rather than
        // applying request.size again. HTTP's default-limit truncation is the
        // only additional cap when sql.limit is in its default range.
        let cap = self
            .pagination
            .root_limit
            .or_else(|| (self.limit > 0).then_some(self.limit))
            .or(default_cap);
        if cap.is_some()
            && !self.pagination.has_explicit_order
            && self.histogram_interval.is_none()
            && !(self.sorted_by_time && !self.is_complex)
        {
            // Unordered DISTINCT/GROUP BY results have no temporal prefix
            // guarantee. Unordered histograms are handled by the writer's
            // exhausted-only admission; ordinary rows have an injected DESC sort.
            return None;
        }
        Some(match (cap, default_cap) {
            (Some(cap), Some(default_cap)) => cap.min(default_cap),
            (Some(cap), None) => cap,
            (None, _) => -1,
        })
    }

    pub fn get_first_stream_key(&self) -> String {
        self.stream_names
            .first()
            .map(|s| {
                format!(
                    "{}/{}",
                    s.get_stream_type(self.stream_type),
                    s.stream_name()
                )
            })
            // For multi-stream / cross-index queries there is no single stream
            // name.  Fall back to the stream-type prefix so select_nodes always
            // receives a non-empty, deterministic key (rather than "" which
            // would silently select all nodes and defeat org/stream affinity).
            .unwrap_or_else(|| format!("{}/", self.stream_type))
    }
}

impl std::fmt::Display for Sql {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sql: {}, time_range: {:?}, stream: {}/{}/{:?}, has_match_all: {}, equal_items: {:?}, aliases: {:?}, limit: {}, offset: {}, group_by: {:?}, order_by: {:?}, histogram_interval: {:?}, sorted_by_time: {}, is_complex: {}",
            self.sql,
            self.time_range,
            self.org_id,
            self.stream_type,
            self.stream_names,
            self.has_match_all,
            self.equal_items,
            self.aliases,
            self.limit,
            self.offset,
            self.group_by,
            self.order_by,
            self.histogram_interval,
            self.sorted_by_time,
            self.is_complex,
        )
    }
}

fn o2_id_is_needed(
    schemas: &HashMap<TableReference, Arc<SchemaCache>>,
    search_event_type: &Option<SearchEventType>,
) -> bool {
    // avoid automatically adding _o2_id for pipeline queries
    !matches!(search_event_type, Some(SearchEventType::DerivedStream))
        && schemas.values().any(|schema| {
            let stream_setting = unwrap_stream_settings(schema.schema());
            stream_setting.is_some_and(|setting| setting.store_original_data)
        })
}

fn finalize_schemas(
    used_schemas: &HashMap<TableReference, Arc<SchemaCache>>,
) -> HashMap<TableReference, Arc<SchemaCache>> {
    let cfg = get_config();
    if cfg.common.utf8_view_enabled {
        let mut final_schemas = HashMap::with_capacity(used_schemas.len());
        for (stream, schema) in used_schemas.iter() {
            let mut fields = schema
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    if f.data_type() == &DataType::Utf8 || f.data_type() == &DataType::LargeUtf8 {
                        Arc::new(Field::new(f.name(), DataType::Utf8View, f.is_nullable()))
                    } else {
                        f.clone()
                    }
                })
                .collect::<Vec<_>>();
            fields.sort_by(|a, b| a.name().cmp(b.name()));
            let new_schema = Schema::new(fields).with_metadata(schema.schema().metadata().clone());
            final_schemas.insert(stream.clone(), Arc::new(SchemaCache::new(new_schema)));
        }
        final_schemas
    } else {
        let mut final_schemas = HashMap::with_capacity(used_schemas.len());
        // sort the schema fields by name
        for (stream, schema) in used_schemas.iter() {
            let mut fields = schema.schema().fields().to_vec();
            fields.sort_by(|a, b| a.name().cmp(b.name()));
            let new_schema = Schema::new(fields).with_metadata(schema.schema().metadata().clone());
            final_schemas.insert(stream.clone(), Arc::new(SchemaCache::new(new_schema)));
        }
        final_schemas
    }
}

/// Parse sampling configuration from SearchQuery
/// Converts sampling_ratio to SamplingConfig for internal use
fn parse_sampling_config(
    query: &proto::cluster_rpc::SearchQuery,
    _histogram_interval: Option<i64>,
    _time_range: (i64, i64),
    _extract_patterns: bool,
) -> Option<proto::cluster_rpc::SamplingConfig> {
    #[cfg(not(feature = "enterprise"))]
    {
        if query.sampling_ratio.is_some() {
            log::warn!(
                "[SAMPLING] Sampling is an enterprise feature. Queries will run without sampling. \
                    To enable sampling, please upgrade to OpenObserve Enterprise Edition."
            );
        }
        None
    }

    #[cfg(feature = "enterprise")]
    {
        o2_enterprise::enterprise::search::sampling::core::parse_sampling_config(
            query,
            _histogram_interval,
            _time_range,
            _extract_patterns,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_re_only_select_matches_star() {
        assert!(RE_ONLY_SELECT.is_match("select * from logs"));
        assert!(RE_ONLY_SELECT.is_match("SELECT * FROM logs"));
    }

    #[test]
    fn test_re_only_select_no_match_named_columns() {
        assert!(!RE_ONLY_SELECT.is_match("select a, b from logs"));
    }

    #[test]
    fn test_re_select_from_captures_columns() {
        let sql = "SELECT a, b FROM logs";
        assert!(RE_SELECT_FROM.is_match(sql));
        if let Some(caps) = RE_SELECT_FROM.captures(sql) {
            assert_eq!(caps.get(1).map(|m| m.as_str()), Some("a, b"));
        }
    }

    #[test]
    fn test_re_histogram_matches() {
        assert!(RE_HISTOGRAM.is_match("histogram(_timestamp, '1 hour')"));
        assert!(RE_HISTOGRAM.is_match("HISTOGRAM(ts,'5 minute')"));
    }

    #[test]
    fn test_re_histogram_no_match() {
        assert!(!RE_HISTOGRAM.is_match("count(_timestamp)"));
    }

    #[test]
    fn test_parse_sampling_config_returns_none_in_oss() {
        let query = SearchQuery::default();
        let result = parse_sampling_config(&query, None, (0, 0), false);
        assert!(result.is_none());
    }

    fn star_query(sql: &str) -> SearchQuery {
        SearchQuery {
            sql: sql.to_string(),
            from: 0,
            size: 100,
            ..Default::default()
        }
    }

    /// End-to-end `Sql::new` proof of the row-store star: with a WIDE
    /// registry (5k+ fields), the plan schema of a plain `SELECT *` stays
    /// the fixed physical set (`_timestamp` + referenced fields +
    /// `_source`) — planning cost flat in the registry width — while the
    /// same statement wrapped in a CTE keeps the registry expansion.
    #[tokio::test]
    async fn test_sql_new_star_schema_is_registry_width_independent() {
        let org = "row_store_star_test";
        infra::db_init().await.unwrap();
        let mut fields = vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("k8s.container.name", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
        ];
        for i in 0..5000 {
            fields.push(Field::new(
                format!("attr.field.{i:05}"),
                DataType::Utf8,
                true,
            ));
        }
        infra::schema::merge(
            org,
            "wide",
            StreamType::Logs,
            &Schema::new(fields),
            Some(1752660674351000),
        )
        .await
        .unwrap();

        let query = star_query(r#"SELECT * FROM "wide" WHERE "k8s.container.name" = 'x' LIMIT 10"#);
        let sql = Sql::new(&query, org, StreamType::Logs, None).await.unwrap();
        let schema = sql
            .schemas
            .get(&TableReference::bare("wide"))
            .expect("stream schema");
        let names: Vec<&str> = schema
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        // finalize_schemas sorts by name: _source, _timestamp, referenced
        assert_eq!(names, vec!["_source", "_timestamp", "k8s.container.name"]);

        // complex statements (CTE): §9 keeps the registry-star ONLY here,
        // BOUNDED by the statement's referenced columns — H4: plan cost
        // O(query), flat in the 5000-field registry width (the #45
        // multi-second-plan shape), quick-mode truncation deleted
        let query = star_query(
            r#"WITH f AS (SELECT * FROM "wide" WHERE "k8s.container.name" = 'x') SELECT * FROM f"#,
        );
        let sql = Sql::new(&query, org, StreamType::Logs, None).await.unwrap();
        let schema = sql
            .schemas
            .get(&TableReference::bare("wide"))
            .expect("stream schema");
        let names: Vec<&str> = schema
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
            vec!["_timestamp", "k8s.container.name"],
            "CTE star = referenced-column bound, never O(registry)"
        );
        assert!(!schema.contains_field(vortex_index::SOURCE_COL_NAME));
    }

    /// Deterministic identifier validation: an unknown WHERE field fails at
    /// parse time against the LATEST schema with a STABLE message — never
    /// DataFusion's "Valid fields are ..." enumeration of whatever plan
    /// schema a node happened to hold.
    #[tokio::test]
    async fn test_sql_new_unknown_where_field_fails_deterministically() {
        let org = "star_validation_test";
        infra::db_init().await.unwrap();
        infra::schema::merge(
            org,
            "logs",
            StreamType::Logs,
            &Schema::new(vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("level", DataType::Utf8, true),
            ]),
            Some(1752660674351000),
        )
        .await
        .unwrap();

        let query = star_query(r#"SELECT * FROM "logs" WHERE "k8s.container.name" = 'x' LIMIT 10"#);
        let mut messages = Vec::new();
        for _ in 0..3 {
            let err = Sql::new(&query, org, StreamType::Logs, None)
                .await
                .expect_err("unknown WHERE field must fail");
            messages.push(err.to_string());
        }
        assert!(
            messages[0].contains("k8s.container.name. Field not found in stream schema."),
            "unexpected message: {}",
            messages[0]
        );
        // identical across repeated identical queries
        assert_eq!(messages[0], messages[1]);
        assert_eq!(messages[1], messages[2]);

        // known fields (and internal columns) still parse
        let query = star_query(r#"SELECT * FROM "logs" WHERE level = 'x' AND _timestamp > 1"#);
        assert!(Sql::new(&query, org, StreamType::Logs, None).await.is_ok());
    }

    fn cache_preparation_fixture(query: &SearchQuery) -> SqlPreparation {
        let schema = Arc::new(SchemaCache::new(Schema::new(vec![Field::new(
            TIMESTAMP_COL_NAME,
            DataType::Int64,
            false,
        )])));
        SqlPreparation {
            statement: Parser::parse_sql(&PostgreSqlDialect {}, &query.sql)
                .unwrap()
                .pop()
                .unwrap(),
            source: query.sql.clone(),
            stream_names: vec![TableReference::bare("logs")],
            schemas: HashMap::from([(TableReference::bare("logs"), schema)]),
        }
    }

    fn cache_query_context(sql: &Sql, timestamps: &[i64]) -> datafusion::prelude::SessionContext {
        use datafusion::{
            arrow::{array::Int64Array, record_batch::RecordBatch},
            prelude::SessionContext,
        };

        use crate::datafusion::{optimizer::generate_optimizer_rules, udf::histogram_udf};

        let ctx = SessionContext::new();
        ctx.register_udf(histogram_udf::HISTOGRAM_UDF.clone());
        for rule in generate_optimizer_rules(sql, false) {
            ctx.add_optimizer_rule(rule);
        }
        // The execution's table registration is half-open, independently of
        // histogram alignment; retain boundary rows in the fixture to detect
        // accidentally reusing the root range.
        let values = timestamps
            .iter()
            .copied()
            .filter(|ts| *ts >= sql.time_range.0 && *ts < sql.time_range.1)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                TIMESTAMP_COL_NAME,
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        ctx.register_batch("logs", batch).unwrap();
        ctx
    }

    async fn execute_cache_histogram(
        sql: &Sql,
        timestamps: &[i64],
    ) -> Vec<config::utils::json::Map<String, config::utils::json::Value>> {
        let ctx = cache_query_context(sql, timestamps);
        let batches = ctx.sql(&sql.sql).await.unwrap().collect().await.unwrap();
        config::utils::arrow::record_batches_to_json_rows(&batches.iter().collect::<Vec<_>>())
            .unwrap()
    }

    #[tokio::test]
    async fn finalized_histogram_binds_disjoint_dst_deltas_and_inline_bounds() {
        let micros = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_micros()
        };
        let a = micros("2025-11-02T04:00:00Z");
        let b = micros("2025-11-03T04:00:00Z");
        let hour = 3_600_000_000;
        let query = SearchQuery {
            sql: format!(
                "SELECT histogram(_timestamp, '1 day') AS bucket, count(*) AS n FROM logs WHERE _timestamp != {a} AND _timestamp != {b} GROUP BY bucket ORDER BY bucket"
            ),
            start_time: a - 2 * hour,
            end_time: b + 2 * hour,
            timezone: Some("America/New_York".to_string()),
            size: -1,
            ..Default::default()
        };
        let mut final_query = query.clone();
        histogram::handle_histogram(&mut final_query.sql, (query.start_time, query.end_time), 0);
        let before = RE_HISTOGRAM.find(&query.sql).unwrap().as_str();
        let after = RE_HISTOGRAM.find(&final_query.sql).unwrap().as_str();
        let root = cache_preparation_fixture(&query)
            .finish_cache(
                &final_query,
                "cache_binding",
                StreamType::Logs,
                None,
                Some((before, after)),
                false,
            )
            .unwrap();
        let rows = [a, a + hour / 2, a + hour, b, b + hour / 2, b + hour];
        for start in [a, b] {
            let delta = SearchQuery {
                start_time: start,
                end_time: start + hour,
                ..final_query.clone()
            };
            let bound = root.bind(&delta);
            let fresh = cache_preparation_fixture(&delta)
                .into_sql(&delta, "cache_binding", StreamType::Logs, None, false)
                .unwrap();
            let result = execute_cache_histogram(&bound, &rows).await;
            assert_eq!(result, execute_cache_histogram(&fresh, &rows).await);
            assert_eq!(
                result,
                vec![
                    config::utils::json::json!({
                        "bucket": "2025-11-02T00:00:00", "n": 1
                    })
                    .as_object()
                    .unwrap()
                    .clone()
                ]
            );
        }
    }

    #[tokio::test]
    async fn finalized_auto_histogram_keeps_full_range_width() {
        let query = SearchQuery {
            sql: "SELECT histogram(_timestamp) AS bucket, count(*) AS n FROM logs GROUP BY bucket ORDER BY bucket".to_string(),
            start_time: 1_700_000_000_000_000,
            end_time: 1_700_086_400_000_000,
            size: -1,
            ..Default::default()
        };
        let mut final_query = query.clone();
        histogram::handle_histogram(&mut final_query.sql, (query.start_time, query.end_time), 0);
        let root = cache_preparation_fixture(&query)
            .finish_cache(
                &final_query,
                "auto_binding",
                StreamType::Logs,
                None,
                Some((
                    RE_HISTOGRAM.find(&query.sql).unwrap().as_str(),
                    RE_HISTOGRAM.find(&final_query.sql).unwrap().as_str(),
                )),
                false,
            )
            .unwrap();
        let delta = SearchQuery {
            start_time: query.start_time + 17_000_000,
            end_time: query.start_time + 137_000_000,
            ..final_query.clone()
        };
        let rows = [
            delta.start_time,
            delta.start_time + 30_000_000,
            delta.end_time,
        ];
        let fresh = cache_preparation_fixture(&delta)
            .into_sql(&delta, "auto_binding", StreamType::Logs, None, false)
            .unwrap();
        let actual = execute_cache_histogram(&root.bind(&delta), &rows).await;
        assert_eq!(actual, execute_cache_histogram(&fresh, &rows).await);
        assert_eq!(
            actual
                .iter()
                .map(|row| row["n"].as_u64().unwrap())
                .sum::<u64>(),
            2
        );
    }
    #[test]
    fn finalized_histogram_textual_rewrite_preserves_syntax_errors() {
        let query = SearchQuery {
            sql: "SELECT 'histogram(_timestamp)' AS text, histogram(_timestamp) AS bucket, count(*) FROM logs GROUP BY bucket".to_string(),
            start_time: 1_700_000_000_000_000,
            end_time: 1_700_086_400_000_000,
            ..Default::default()
        };
        let mut final_query = query.clone();
        histogram::handle_histogram(&mut final_query.sql, (query.start_time, query.end_time), 0);
        let result = cache_preparation_fixture(&query).finish_cache(
            &final_query,
            "cache_syntax",
            StreamType::Logs,
            None,
            Some((
                RE_HISTOGRAM.find(&query.sql).unwrap().as_str(),
                RE_HISTOGRAM.find(&final_query.sql).unwrap().as_str(),
            )),
            false,
        );
        assert!(matches!(
            result,
            Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(_)))
        ));
    }

    #[tokio::test]
    async fn cache_coverage_uses_executed_sql_limit_precedence() {
        let rows = (1..=10).collect::<Vec<i64>>();
        for (clause, size, expected) in [("LIMIT 3", 100, 3), ("LIMIT 7", 2, 7)] {
            let query = SearchQuery {
                sql: format!("SELECT _timestamp FROM logs ORDER BY _timestamp {clause}"),
                size,
                start_time: 0,
                end_time: 100,
                ..Default::default()
            };
            let sql = cache_preparation_fixture(&query)
                .into_sql(&query, "cache_cap", StreamType::Logs, None, false)
                .unwrap();
            let result = execute_cache_histogram(&sql, &rows).await;
            assert_eq!(result.len(), expected);
            assert_eq!(
                sql.cache_coverage_limit(TIMESTAMP_COL_NAME),
                Some(expected as i64)
            );
            assert_eq!(
                result.last().unwrap()[TIMESTAMP_COL_NAME].as_i64(),
                Some(expected as i64)
            );
        }
    }

    #[tokio::test]
    async fn cache_coverage_rebinds_changed_request_cap_and_offset() {
        let query = SearchQuery {
            sql: "SELECT _timestamp FROM logs ORDER BY _timestamp".to_string(),
            size: 5,
            start_time: 0,
            end_time: 100,
            ..Default::default()
        };
        let root = cache_preparation_fixture(&query)
            .into_sql(&query, "cache_rebind", StreamType::Logs, None, false)
            .unwrap();
        let rows = (1..=10).collect::<Vec<i64>>();
        let smaller = SearchQuery {
            size: 2,
            ..query.clone()
        };
        let bound = root.bind(&smaller);
        assert_eq!(bound.cache_coverage_limit(TIMESTAMP_COL_NAME), Some(2));
        let result = execute_cache_histogram(&bound, &rows).await;
        assert_eq!(
            result
                .iter()
                .map(|row| row[TIMESTAMP_COL_NAME].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let offset = SearchQuery { from: 1, ..smaller };
        let bound = root.bind(&offset);
        assert_eq!(bound.cache_coverage_limit(TIMESTAMP_COL_NAME), None);
        let result = execute_cache_histogram(&bound, &rows).await;
        assert_eq!(
            result
                .iter()
                .map(|row| row[TIMESTAMP_COL_NAME].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        // Binding never changes an already-running root execution.
        assert_eq!(execute_cache_histogram(&root, &rows).await.len(), 5);
    }

    #[test]
    fn cache_coverage_rejects_unproven_pagination_and_histogram_coordinates() {
        for text in [
            "SELECT _timestamp FROM logs LIMIT 3 OFFSET 1",
            "SELECT _timestamp FROM logs LIMIT 3 OFFSET $1",
            "SELECT _timestamp FROM logs LIMIT $1",
            "SELECT _timestamp FROM logs FETCH FIRST $1 ROWS ONLY",
            "SELECT _timestamp FROM logs ORDER BY _timestamp FETCH FIRST 3 ROWS WITH TIES",
            "SELECT count(*) FROM (SELECT _timestamp FROM logs LIMIT 3) q",
            "SELECT histogram(_timestamp, '1 day', 'America/New_York') FROM logs",
        ] {
            let query = SearchQuery {
                sql: text.to_string(),
                size: 100,
                ..Default::default()
            };
            let sql = cache_preparation_fixture(&query)
                .into_sql(&query, "cache_unsafe", StreamType::Logs, None, false)
                .unwrap();
            assert_eq!(sql.cache_coverage_limit(TIMESTAMP_COL_NAME), None, "{text}");
        }
        let query = SearchQuery {
            sql: "SELECT _timestamp FROM logs LIMIT 3 OFFSET 0".to_string(),
            size: 100,
            ..Default::default()
        };
        let sql = cache_preparation_fixture(&query)
            .into_sql(&query, "cache_zero_offset", StreamType::Logs, None, false)
            .unwrap();
        assert_eq!(sql.cache_coverage_limit(TIMESTAMP_COL_NAME), Some(3));
    }

    #[tokio::test]
    async fn cache_coverage_bypasses_transformed_time_without_changing_results() {
        let query = SearchQuery {
            sql: "SELECT _timestamp + 10 AS _timestamp FROM logs ORDER BY _timestamp".to_string(),
            size: 100,
            start_time: 0,
            end_time: 100,
            ..Default::default()
        };
        let sql = cache_preparation_fixture(&query)
            .into_sql(&query, "cache_transformed", StreamType::Logs, None, false)
            .unwrap();
        assert_eq!(sql.cache_coverage_limit(TIMESTAMP_COL_NAME), None);
        let result = execute_cache_histogram(&sql, &[1, 2]).await;
        assert_eq!(
            result
                .iter()
                .map(|row| row[TIMESTAMP_COL_NAME].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![11, 12]
        );
    }

    #[test]
    fn cache_coverage_proves_the_selected_physical_time_axis() {
        for (text, axis) in [
            ("SELECT _timestamp AS event_time FROM logs", "event_time"),
            (
                "SELECT DISTINCT _timestamp AS event_time FROM logs ORDER BY event_time",
                "event_time",
            ),
            (
                "SELECT l._timestamp AS event_time FROM logs l",
                "event_time",
            ),
            ("SELECT * FROM logs l", "_timestamp"),
            ("SELECT 1 AS field FROM logs", "_timestamp"),
            (
                "SELECT histogram(_timestamp, '1 hour') AS bucket, count(*) FROM logs GROUP BY bucket",
                "bucket",
            ),
        ] {
            let query = SearchQuery {
                sql: text.to_string(),
                size: 100,
                ..Default::default()
            };
            let sql = cache_preparation_fixture(&query)
                .into_sql(&query, "cache_axis", StreamType::Logs, None, false)
                .unwrap();
            assert_eq!(sql.cache_coverage_limit(axis), Some(100), "{text}");
            assert_eq!(sql.cache_coverage_limit("unrelated_output"), None);
        }
        for (text, axis) in [
            ("SELECT DISTINCT _timestamp AS ts FROM logs LIMIT 2", "ts"),
            (
                "SELECT _timestamp AS ts FROM logs GROUP BY ts LIMIT 2",
                "ts",
            ),
            (
                "SELECT DISTINCT histogram(_timestamp, '1 hour') AS bucket FROM logs",
                "bucket",
            ),
            (
                "SELECT histogram(_timestamp + 10, '1 hour') AS bucket FROM logs",
                "bucket",
            ),
            (
                "SELECT histogram(_timestamp, '1 hour') + INTERVAL '1 hour' AS bucket FROM logs",
                "bucket",
            ),
            (
                "SELECT histogram(_timestamp, '1 day', 'America/New_York') AS bucket FROM logs",
                "bucket",
            ),
            (
                "SELECT _timestamp FROM (SELECT _timestamp FROM logs) q",
                "_timestamp",
            ),
            (
                "WITH q AS (SELECT _timestamp FROM logs) SELECT _timestamp FROM q",
                "_timestamp",
            ),
            (
                "SELECT a._timestamp FROM logs a JOIN logs b ON a._timestamp = b._timestamp",
                "_timestamp",
            ),
            (
                "SELECT *, _timestamp + 10 AS _timestamp FROM logs",
                "_timestamp",
            ),
        ] {
            let query = SearchQuery {
                sql: text.to_string(),
                size: 100,
                ..Default::default()
            };
            let sql = cache_preparation_fixture(&query)
                .into_sql(&query, "cache_axis_unsafe", StreamType::Logs, None, false)
                .unwrap();
            assert_eq!(sql.cache_coverage_limit(axis), None, "{text}");
        }
    }

    #[tokio::test]
    async fn cache_coverage_rejects_cross_time_distinct_on_representatives() {
        let query = SearchQuery {
            sql: "SELECT DISTINCT ON (_timestamp % 2) _timestamp AS ts FROM logs".to_string(),
            size: 10,
            start_time: 0,
            end_time: 10,
            ..Default::default()
        };
        let sql = cache_preparation_fixture(&query)
            .into_sql(&query, "cache_distinct_on", StreamType::Logs, None, false)
            .unwrap();
        let result = execute_cache_histogram(&sql, &[1, 3]).await;
        assert_eq!(result.len(), 1);
        let omitted = if result[0]["ts"].as_i64().unwrap() == 1 {
            3
        } else {
            1
        };
        let narrow = SearchQuery {
            start_time: omitted,
            end_time: omitted + 1,
            ..query
        };
        let narrow_result = execute_cache_histogram(&sql.bind(&narrow), &[1, 3]).await;
        assert_eq!(
            narrow_result
                .iter()
                .map(|row| row["ts"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![omitted]
        );
        assert_eq!(sql.cache_coverage_limit("ts"), None);
    }

    #[tokio::test]
    async fn cache_coverage_rejects_window_values_that_change_in_subranges() {
        let query = SearchQuery {
            sql: "SELECT _timestamp AS ts, row_number() OVER (ORDER BY _timestamp) AS n FROM logs ORDER BY ts".to_string(),
            size: 10,
            start_time: 0,
            end_time: 10,
            ..Default::default()
        };
        let sql = cache_preparation_fixture(&query)
            .into_sql(&query, "cache_window", StreamType::Logs, None, false)
            .unwrap();
        let result = execute_cache_histogram(&sql, &[1, 3]).await;
        assert_eq!(
            result
                .iter()
                .map(|row| (row["ts"].as_i64().unwrap(), row["n"].as_i64().unwrap()))
                .collect::<Vec<_>>(),
            vec![(1, 1), (3, 2)]
        );
        let narrow = SearchQuery {
            start_time: 3,
            end_time: 4,
            ..query
        };
        let narrow_result = execute_cache_histogram(&sql.bind(&narrow), &[1, 3]).await;
        assert_eq!(
            narrow_result
                .iter()
                .map(|row| (row["ts"].as_i64().unwrap(), row["n"].as_i64().unwrap()))
                .collect::<Vec<_>>(),
            vec![(3, 1)]
        );
        assert_eq!(sql.cache_coverage_limit("ts"), None);
    }

    #[tokio::test]
    async fn cache_coverage_requires_actual_leading_order_metadata_agreement() {
        for (order, expected, certified) in [
            ("-_timestamp ASC", vec![40, 30], false),
            ("l._timestamp ASC", vec![10, 20], false),
            ("1 DESC", vec![40, 30], false),
            ("_timestamp DESC", vec![40, 30], true),
        ] {
            let query = SearchQuery {
                sql: format!("SELECT _timestamp FROM logs l ORDER BY {order} LIMIT 2"),
                size: 2,
                start_time: 0,
                end_time: 50,
                ..Default::default()
            };
            let sql = cache_preparation_fixture(&query)
                .into_sql(&query, "cache_ranking", StreamType::Logs, None, false)
                .unwrap();
            let result = execute_cache_histogram(&sql, &[10, 20, 30, 40]).await;
            assert_eq!(
                result
                    .iter()
                    .map(|row| row[TIMESTAMP_COL_NAME].as_i64().unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                sql.cache_coverage_limit(TIMESTAMP_COL_NAME),
                certified.then_some(2),
                "{order}"
            );
        }
    }

    #[tokio::test]
    async fn cache_coverage_rejects_fetch_without_hiding_planner_error() {
        let query = SearchQuery {
            sql: "SELECT _timestamp FROM logs ORDER BY _timestamp FETCH FIRST 4 ROWS ONLY"
                .to_string(),
            size: 100,
            start_time: 0,
            end_time: 100,
            ..Default::default()
        };
        let sql = cache_preparation_fixture(&query)
            .into_sql(&query, "cache_fetch", StreamType::Logs, None, false)
            .unwrap();
        assert_eq!(sql.cache_coverage_limit(TIMESTAMP_COL_NAME), None);
        let error = cache_query_context(&sql, &[1, 2, 3, 4, 5])
            .sql(&sql.sql)
            .await
            .err()
            .expect("FETCH must retain its planner error");
        assert!(matches!(
            error,
            datafusion::common::DataFusionError::NotImplemented(_)
        ));
    }
}
