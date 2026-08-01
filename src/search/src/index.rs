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
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use config::{
    get_config, meta::inverted_index::UNKNOWN_NAME, text_tokenizer::o2_collect_search_tokens,
};
use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    config::ConfigOptions,
    logical_expr::Operator,
    physical_expr::{ScalarFunctionExpr, conjunction},
    physical_plan::{
        PhysicalExpr,
        expressions::{
            BinaryExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, LikeExpr, Literal,
            NotExpr,
        },
    },
    scalar::ScalarValue,
};
use hashbrown::{HashMap, HashSet};
use vortex_index::{
    VixQuery, canonical_f64_text, canonical_i64_text, canonical_u64_text, numeric_value_token,
};

/// Historical prefix kept in `to_query()` cache keys for match_all
/// conditions — only a stable cache-key label (no `_all` field exists).
const MATCH_ALL_QUERY_PREFIX: &str = "_all";

use super::datafusion::udf::fuzzy_match_udf;
use crate::datafusion::udf::{
    MATCH_FIELD_IGNORE_CASE_UDF_NAME, MATCH_FIELD_UDF_NAME, STR_MATCH_UDF_IGNORE_CASE_NAME,
    STR_MATCH_UDF_NAME,
    match_all_udf::{FUZZY_MATCH_ALL_UDF_NAME, MATCH_ALL_UDF_NAME},
    str_match_udf,
};

/// Error marker: every AND condition was skipped, no per-file query could be
/// built (the caller keeps the file and re-applies the DataFusion filter).
/// A typed error so the per-file retry logic can tell this deterministic
/// outcome apart from transient IO failures.
#[derive(Debug)]
pub struct AllConditionsSkipped;

impl fmt::Display for AllConditionsSkipped {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "All AND conditions are failed to generate vix query")
    }
}

impl std::error::Error for AllConditionsSkipped {}

/// Per-file capability of a named field for term-index lookups, reported by
/// the closure [`IndexCondition::to_vix_query`] takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldCap {
    /// The field's raw whole values are term-indexed in this file
    /// (`VixReader::has_term_capability`): conditions on it map to index
    /// queries directly.
    Term,
    /// The file carries the field, but not as raw value terms — fts-only
    /// (tokens), column-store/numeric storage, or an internal column. The
    /// index cannot decide conditions on it: skip them and re-apply the
    /// DataFusion filter (`has_skipped`).
    FtsOnly,
    /// No document of this file carries the field at all (no key term in
    /// the dictionary): it is NULL in every row. Conditions that can never
    /// be TRUE on all-NULL input map to [`VixQuery::Nothing`] — an EXACT
    /// empty result that eliminates the file without a scan.
    Absent,
}

// note the condition in IndexCondition is connection by AND operator
#[derive(Default, Clone, Hash, Eq, PartialEq)]
pub struct IndexCondition {
    pub conditions: Vec<Condition>,
}

impl IndexCondition {
    pub fn new() -> Self {
        IndexCondition {
            conditions: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }

    pub fn add_condition(&mut self, condition: Condition) {
        self.conditions.push(condition);
    }
}

impl Debug for IndexCondition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_query())
    }
}

impl IndexCondition {
    // this only use for display the query
    pub fn to_query(&self) -> String {
        self.conditions
            .iter()
            .map(|condition| condition.to_query())
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    // get the vix query for the index condition
    // Returns (query, has_skipped):
    //   has_skipped = true means some conditions were skipped because the
    //   index of this file cannot decide them — the field is carried by the
    //   file but not raw-value term-indexed ([`FieldCap::FtsOnly`]: an fts
    //   field's values are token-indexed only, never whole values; numeric /
    //   column-store-only storage), or an absent field appears in a shape
    //   whose truth does not hinge on it alone (e.g. OR with a servable
    //   predicate). The caller must keep the DataFusion filter so that the
    //   skipped predicates are still evaluated.
    //
    //   A field reported [`FieldCap::Absent`] is NULL in every row of the
    //   file (no key term), so a condition that can never be TRUE on
    //   all-NULL input (SQL three-valued logic — Equal/In/StrMatch/Regex
    //   match nothing; NotEqual/NOT IN are UNKNOWN, not TRUE) maps to
    //   [`VixQuery::Nothing`]: an EXACT empty per-file result, NOT a skip —
    //   `has_skipped` stays false and the empty evaluation eliminates the
    //   file without a scan.
    //
    //   `field_cap` reports the per-file capability
    //   (`vix::field_capability`, backed by `VixReader::has_term_capability`
    //   and the `VixReader::key_term_exists` dictionary probe).
    //   `tokenize` turns a match_all value into index tokens — the canonical
    //   `vortex_index::o2_tokenize` (via `vix::index_match_all_tokens`), the
    //   same function the writer indexes with.
    pub fn to_vix_query(
        &self,
        trace_id: &str,
        field_cap: &dyn Fn(&str) -> FieldCap,
        tokenize: &dyn Fn(&str) -> Vec<String>,
    ) -> anyhow::Result<(VixQuery, bool)> {
        let mut has_skipped = false;
        let mut queries: Vec<VixQuery> = Vec::with_capacity(self.conditions.len());
        for condition in &self.conditions {
            // classify the fields this condition looks up in the per-file
            // term index
            let fields = condition.term_index_fields();
            let mut fts_only: Option<&String> = None;
            let mut absent: HashSet<String> = HashSet::new();
            for field in &fields {
                match field_cap(field) {
                    FieldCap::Term => {}
                    FieldCap::FtsOnly => {
                        fts_only = Some(field);
                        break;
                    }
                    FieldCap::Absent => {
                        absent.insert(field.clone());
                    }
                }
            }
            if let Some(missing) = fts_only {
                log::info!(
                    "[trace_id {trace_id}] to_vix_query: skipping condition, field {missing} is not term-indexed in this file"
                );
                has_skipped = true;
                continue;
            }
            if !absent.is_empty() {
                if condition.never_true_when_null(&absent) {
                    // no document of this file carries the absent field(s),
                    // so they are NULL in every row and the condition can
                    // never be TRUE: it matches nothing — exactly, so the
                    // file is eliminated instead of scanned
                    log::debug!(
                        "[trace_id {trace_id}] to_vix_query: condition on absent field(s) {absent:?} matches nothing in this file"
                    );
                    queries.push(VixQuery::Nothing);
                    continue;
                }
                // mixed shape (e.g. OR of an absent-field predicate with a
                // servable one, or a NOT over such a mix): the condition can
                // still be TRUE, keep today's skip + filter-back
                log::info!(
                    "[trace_id {trace_id}] to_vix_query: skipping condition, absent field(s) {absent:?} appear in a shape the index cannot decide"
                );
                has_skipped = true;
                continue;
            }
            match condition.to_vix_query(tokenize) {
                Ok(query) => {
                    queries.push(query);
                }
                Err(e) => {
                    log::info!(
                        "[trace_id {trace_id}] to_vix_query: skipping condition due to error: {e}"
                    );
                    has_skipped = true;
                }
            }
        }
        if queries.is_empty() {
            Err(anyhow::Error::new(AllConditionsSkipped))
        } else if queries.len() == 1 {
            Ok((queries.pop().unwrap(), has_skipped))
        } else {
            Ok((VixQuery::And(queries), has_skipped))
        }
    }

    /// Whether the condition touches a field for which the vix index skipped
    /// at least one value at build time. Term lookups on such fields may miss
    /// documents, so the whole file must fall back to a scan.
    ///
    /// `fts_fields` is the FILE's fts-marked field set: match_all consults
    /// only fts tokens, so it is tainted only by a partial field that is
    /// fts-marked in this file. Writers never partial-mark an fts field for
    /// oversize values (tokens are length-independent) — an fts field is
    /// partial only when its tokens are genuinely missing (field-id
    /// overflow, unindexable type drift, or a pre-token-fix build), and the
    /// merge fast path already refuses such inputs. Value-term lookups
    /// (equality/range/regex/...) keep the flat check: a partial mark always
    /// means their raw terms may miss documents.
    pub fn uses_partial_fields(
        &self,
        partial_fields: &std::collections::HashSet<String>,
        fts_fields: &std::collections::HashSet<String>,
    ) -> bool {
        !partial_fields.is_empty()
            && self
                .conditions
                .iter()
                .any(|condition| condition.uses_partial_fields(partial_fields, fts_fields))
    }

    // get the fields use for search in datafusion(for add filter back logical)
    pub fn get_schema_fields(&self, fst_fields: &[String]) -> HashSet<String> {
        self.conditions
            .iter()
            .fold(HashSet::new(), |mut acc, condition| {
                acc.extend(condition.get_schema_fields(fst_fields));
                acc
            })
    }

    pub fn get_schema_projection(&self, schema: SchemaRef, fst_fields: &[String]) -> Vec<usize> {
        let fields = self.get_schema_fields(fst_fields);
        let mut projection = Vec::with_capacity(fields.len());
        for field in fields.iter() {
            if let Ok(index) = schema.index_of(field) {
                projection.push(index);
            }
        }
        projection
    }

    pub fn to_physical_expr(
        &self,
        schema: &arrow_schema::Schema,
        fst_fields: &[String],
    ) -> Result<Arc<dyn PhysicalExpr>, anyhow::Error> {
        Ok(conjunction(
            self.conditions
                .iter()
                .map(|condition| condition.to_physical_expr(schema, fst_fields))
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }

    pub fn can_remove_filter(&self) -> bool {
        self.conditions
            .iter()
            .all(|condition| condition.can_remove_filter())
    }

    // use for check if the index condition is only
    // for the condition that query without filter
    pub fn is_condition_all(&self) -> bool {
        self.conditions.len() == 1 && matches!(self.conditions[0], Condition::All())
    }

    // use for the simple histogram RANK fast path: the single `field = value` term
    pub fn single_equal_term(&self) -> Option<(String, String)> {
        if self.conditions.len() == 1
            && let Condition::Equal(field, value) = &self.conditions[0]
        {
            Some((field.clone(), value.clone()))
        } else {
            None
        }
    }
}

/// How a numeric/bool-typed comparison probes the term index — decided by
/// the field's REGISTRY type at extraction, because the scan-side ground
/// truth differs per projection (`json_get_int` rejects floats and coerces
/// int-parseable strings; `json_get_float` coerces ints, floats and
/// f64-parseable strings; `json_get_bool` accepts booleans and
/// "true"/"false" strings).
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum NumericKind {
    /// Int/UInt registry types: probe the canonical integer term only —
    /// float-stored rows project to NULL under `json_get_int`, so their
    /// `x.0` terms must NOT match.
    Int,
    /// Float registry types: probe the union of the float (ryu) form and,
    /// for integral values, the integer (itoa) form — `json_get_float`
    /// coerces both JSON spellings to the same value.
    Float,
    /// Boolean registry type: probe "true"/"false".
    Bool,
}

/// The [`NumericKind`] of a registry field type; `None` = string semantics.
pub fn numeric_kind_of(data_type: &DataType) -> Option<NumericKind> {
    match data_type {
        DataType::Boolean => Some(NumericKind::Bool),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Some(NumericKind::Int),
        DataType::Float32 | DataType::Float64 => Some(NumericKind::Float),
        _ => None,
    }
}

// single condition
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum Condition {
    // field, value
    Equal(String, String),
    // field, value
    NotEqual(String, String),
    // field, value, case_sensitive
    StrMatch(String, String, bool),
    // field, values, negated
    In(String, Vec<String>, bool),
    // numeric/bool-typed comparison on a numeric/bool REGISTRY field:
    // field, normalized literal texts (one for =/!=, several for IN),
    // negated, kind. Probes the union of the canonical value-term forms
    // (tagged) plus the same texts as raw string terms (canonically-spelled
    // string-stored drift rows, which the scan-side json_get coercion also
    // matches).
    NumericCmp(String, Vec<String>, bool, NumericKind),
    // field, pattern
    Regex(String, String),
    // field: the flattened path has a (non-null) value — answered from the
    // core-file key terms as [`VixQuery::KeyExists`]
    IsNotNull(String),
    // field: the flattened path is NULL for the row — writers omit null
    // values from `_source` AND key terms (absent == null), so this is
    // EXACTLY the complement of the key term: [`VixQuery::Not(KeyExists)`].
    // A file lacking the field entirely matches EVERY row.
    IsNull(String),
    // term
    MatchAll(String),
    // term, distance
    FuzzyMatchAll(String, u8),
    All(),
    Or(Box<Condition>, Box<Condition>),
    And(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
}

impl Condition {
    // this only use for display the query
    pub fn to_query(&self) -> String {
        match self {
            Condition::Equal(field, value) => format!("{field}={value}"),
            Condition::NotEqual(field, value) => format!("{field}!={value}"),
            Condition::StrMatch(field, value, case_sensitive) => {
                if *case_sensitive {
                    format!("str_match({field}, '{value}')")
                } else {
                    format!("str_match_ignore_case({field}, '{value}')")
                }
            }
            Condition::In(field, values, negated) => {
                if *negated {
                    format!("{} NOT IN ({})", field, values.join(","))
                } else {
                    format!("{} IN ({})", field, values.join(","))
                }
            }
            // the `num:` prefix keeps cache keys distinct from a string
            // In/Equal over the same texts (different probe semantics)
            Condition::NumericCmp(field, values, negated, kind) => format!(
                "num:{field}{}({}) [{kind:?}]",
                if *negated { " NOT IN " } else { " IN " },
                values.join(",")
            ),
            Condition::Regex(field, value) => format!("{field}=~{value}"),
            Condition::IsNotNull(field) => format!("{field} IS NOT NULL"),
            Condition::IsNull(field) => format!("{field} IS NULL"),
            Condition::MatchAll(value) => {
                let tokens = o2_collect_search_tokens(value);
                format!("({MATCH_ALL_QUERY_PREFIX}:{value}):({tokens:?})")
            }
            Condition::FuzzyMatchAll(value, distance) => {
                format!("{MATCH_ALL_QUERY_PREFIX}:fuzzy({value}, {distance})")
            }
            Condition::All() => "ALL".to_string(),
            Condition::Or(left, right) => format!("({} OR {})", left.to_query(), right.to_query()),
            Condition::And(left, right) => {
                format!("({} AND {})", left.to_query(), right.to_query())
            }
            Condition::Not(condition) => format!("NOT({})", condition.to_query()),
        }
    }

    /// Translate one gated filter expression (see
    /// `is_expr_valid_for_index`) into a [`Condition`]. `index_fields` maps
    /// every index-eligible field to its REGISTRY type: comparisons on
    /// numeric/bool-typed fields become [`Condition::NumericCmp`] with
    /// value-normalized literals, everything else keeps string semantics.
    pub fn from_physical_expr(
        expr: &Arc<dyn PhysicalExpr>,
        index_fields: &HashMap<String, DataType>,
    ) -> Self {
        let numeric_kind = |field: &str| index_fields.get(field).and_then(numeric_kind_of);
        if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
            match expr.op() {
                Operator::Eq | Operator::NotEq => {
                    let (field, value) = if is_physical_value(expr.left())
                        && is_physical_column(expr.right())
                    {
                        (
                            get_physical_column_name(expr.right()).to_string(),
                            get_physical_value(expr.left()),
                        )
                    } else if is_physical_value(expr.right()) && is_physical_column(expr.left()) {
                        (
                            get_physical_column_name(expr.left()).to_string(),
                            get_physical_value(expr.right()),
                        )
                    } else {
                        unreachable!()
                    };

                    let negated = *expr.op() == Operator::NotEq;
                    if let Some(kind) = numeric_kind(&field) {
                        let value = normalize_numeric_literal(kind, &value)
                            .expect("literal gated by is_expr_valid_for_index");
                        Condition::NumericCmp(field, vec![value], negated, kind)
                    } else if negated {
                        Condition::NotEqual(field, value)
                    } else {
                        Condition::Equal(field, value)
                    }
                }
                Operator::And => Condition::And(
                    Box::new(Condition::from_physical_expr(expr.left(), index_fields)),
                    Box::new(Condition::from_physical_expr(expr.right(), index_fields)),
                ),
                Operator::Or => Condition::Or(
                    Box::new(Condition::from_physical_expr(expr.left(), index_fields)),
                    Box::new(Condition::from_physical_expr(expr.right(), index_fields)),
                ),
                _ => unreachable!(),
            }
        } else if let Some(expr) = expr.downcast_ref::<InListExpr>() {
            let field = get_physical_column_name(expr.expr()).to_string();
            let values: Vec<String> = expr.list().iter().map(get_physical_value).collect();
            if let Some(kind) = numeric_kind(&field) {
                let values = values
                    .iter()
                    .map(|value| {
                        normalize_numeric_literal(kind, value)
                            .expect("literals gated by is_expr_valid_for_index")
                    })
                    .collect();
                Condition::NumericCmp(field, values, expr.negated(), kind)
            } else {
                Condition::In(field, values, expr.negated())
            }
        } else if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
            let name = expr.name();
            match name {
                MATCH_ALL_UDF_NAME => Condition::MatchAll(get_physical_value(&expr.args()[0])),
                FUZZY_MATCH_ALL_UDF_NAME => {
                    let value = get_physical_value(&expr.args()[0]);
                    let distance = get_physical_value(&expr.args()[1]).parse().unwrap_or(1);
                    Condition::FuzzyMatchAll(value, distance)
                }
                STR_MATCH_UDF_NAME | MATCH_FIELD_UDF_NAME => {
                    let field = get_physical_column_name(&expr.args()[0]).to_string();
                    let value = get_physical_value(&expr.args()[1]);
                    Condition::StrMatch(field, value, true)
                }
                STR_MATCH_UDF_IGNORE_CASE_NAME | MATCH_FIELD_IGNORE_CASE_UDF_NAME => {
                    let field = get_physical_column_name(&expr.args()[0]).to_string();
                    let value = get_physical_value(&expr.args()[1]);
                    Condition::StrMatch(field, value, false)
                }
                _ => unreachable!(),
            }
        } else if let Some(expr) = expr.downcast_ref::<IsNotNullExpr>() {
            Condition::IsNotNull(get_physical_column_name(expr.arg()).to_string())
        } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
            Condition::IsNull(get_physical_column_name(expr.arg()).to_string())
        } else if let Some(expr) = expr.downcast_ref::<NotExpr>() {
            Condition::Not(Box::new(Condition::from_physical_expr(
                expr.arg(),
                index_fields,
            )))
        } else if is_physical_column(expr) {
            // a bare boolean column as a predicate: DataFusion's
            // simplification of `bool_field = true` (gated to Boolean index
            // fields by is_expr_valid_for_index)
            Condition::NumericCmp(
                get_physical_column_name(expr).to_string(),
                vec!["true".to_string()],
                false,
                NumericKind::Bool,
            )
        } else {
            unreachable!()
        }
    }

    /// Translate the condition into a [`VixQuery`]. The translation is
    /// field-agnostic: per-file field presence is checked by the caller via
    /// [`Condition::term_index_fields`] before this runs. `tokenize` turns
    /// match_all values into index tokens (the canonical
    /// `vortex_index::o2_tokenize`).
    ///
    /// SQL three-valued logic: `field != value` / `field NOT IN (..)` are
    /// false-or-null on NULL rows, so plain bitmap negation (which matches
    /// null rows) is wrong. On core files the key term is exactly
    /// "non-null", so negated conditions are rewritten to
    /// `And[Not(..), KeyExists(field)]` — exact, which is what keeps
    /// [`Condition::can_remove_filter`] true for them.
    pub fn to_vix_query(&self, tokenize: &dyn Fn(&str) -> Vec<String>) -> anyhow::Result<VixQuery> {
        Ok(match self {
            Condition::Equal(field, value) => VixQuery::Exact {
                field: field.clone(),
                token: value.clone().into_bytes(),
            },
            Condition::NotEqual(field, value) => VixQuery::And(vec![
                VixQuery::Not(Box::new(VixQuery::Exact {
                    field: field.clone(),
                    token: value.clone().into_bytes(),
                })),
                // SQL: `NULL != value` is NULL, not true — restrict the
                // negation to rows that HAVE the field (key term ⇔ non-null)
                VixQuery::KeyExists {
                    path: field.clone(),
                },
            ]),
            Condition::In(field, values, negated) => {
                let query = VixQuery::Or(
                    values
                        .iter()
                        .map(|value| VixQuery::Exact {
                            field: field.clone(),
                            token: value.clone().into_bytes(),
                        })
                        .collect(),
                );
                if *negated {
                    // same three-valued-logic guard as NotEqual
                    VixQuery::And(vec![
                        VixQuery::Not(Box::new(query)),
                        VixQuery::KeyExists {
                            path: field.clone(),
                        },
                    ])
                } else {
                    query
                }
            }
            // numeric/bool comparison: Or over every canonical form the
            // value can take — the TAGGED numeric terms plus the same texts
            // as raw string terms (canonically-spelled string drift). The
            // negated shape carries the NotEqual/NOT-IN KeyExists guard; it
            // additionally keeps the scan filter (`can_remove_filter` =
            // false) because rows whose stored value does not coerce under
            // the json_get projection (e.g. a non-numeric string in a float
            // field) are UNKNOWN in SQL but present in the bitmap.
            Condition::NumericCmp(field, values, negated, kind) => {
                let query = VixQuery::Or(
                    values
                        .iter()
                        .flat_map(|value| numeric_probe_tokens(*kind, value))
                        .map(|token| VixQuery::Exact {
                            field: field.clone(),
                            token,
                        })
                        .collect(),
                );
                if *negated {
                    VixQuery::And(vec![
                        VixQuery::Not(Box::new(query)),
                        VixQuery::KeyExists {
                            path: field.clone(),
                        },
                    ])
                } else {
                    query
                }
            }
            Condition::Regex(field, value) => VixQuery::Regex {
                field: Some(field.clone()),
                pattern: value.clone(),
            },
            // key terms mark every doc whose flattened record has a
            // (non-null) value at the path — exactly `field IS NOT NULL`
            // (flatten omits nulls, so present == not null)
            Condition::IsNull(field) => VixQuery::Not(Box::new(VixQuery::KeyExists {
                path: field.clone(),
            })),
            Condition::IsNotNull(field) => VixQuery::KeyExists {
                path: field.clone(),
            },
            Condition::StrMatch(field, value, case_sensitive) => VixQuery::Contains {
                field: Some(field.clone()),
                needle: value.clone().into_bytes(),
                case_insensitive: !case_sensitive,
            },
            Condition::MatchAll(value) => {
                if value.is_empty() || value == "*" {
                    VixQuery::All
                } else {
                    // tokens come out of the same canonical tokenizer the
                    // writer indexes with (already lowercased)
                    let mut tokens = tokenize(value);
                    let contains_search =
                        tokens.len() == 1 && value.starts_with("*") && value.ends_with("*");
                    let first_prefix = if value.starts_with("*") && !tokens.is_empty() {
                        Some(tokens.remove(0))
                    } else {
                        None
                    };
                    let last_prefix = if value.ends_with("*") {
                        tokens.pop()
                    } else {
                        None
                    };
                    let mut terms: Vec<VixQuery> = tokens
                        .into_iter()
                        .map(|value| VixQuery::TokenAnyField {
                            token: value.into_bytes(),
                        })
                        .collect();
                    if let Some(value) = first_prefix {
                        terms.push(if contains_search {
                            // `*foo*` — substring match over tokens
                            VixQuery::Contains {
                                field: None,
                                needle: value.into_bytes(),
                                case_insensitive: true,
                            }
                        } else {
                            // leading `*foo` — anchored regex over tokens
                            VixQuery::Regex {
                                field: None,
                                pattern: format!(".*{}", regex::escape(&value)),
                            }
                        });
                    }
                    if let Some(value) = last_prefix {
                        // trailing `foo*` — token prefix match
                        terms.push(VixQuery::Prefix {
                            field: None,
                            prefix: value.into_bytes(),
                        });
                    }
                    match terms.len() {
                        0 => {
                            return Err(anyhow::anyhow!(
                                "The value of match_all() function can't be empty"
                            ));
                        }
                        1 => terms.remove(0),
                        _ => VixQuery::And(terms),
                    }
                }
            }
            Condition::FuzzyMatchAll(value, distance) => {
                if value.is_empty() {
                    return Err(anyhow::anyhow!(
                        "The value of fuzzy_match_all() function can't be empty"
                    ));
                }
                VixQuery::Fuzzy {
                    token: value.clone(),
                    distance: (*distance).min(2),
                }
            }
            Condition::All() => VixQuery::All,
            Condition::Or(left, right) => VixQuery::Or(vec![
                left.to_vix_query(tokenize)?,
                right.to_vix_query(tokenize)?,
            ]),
            Condition::And(left, right) => VixQuery::And(vec![
                left.to_vix_query(tokenize)?,
                right.to_vix_query(tokenize)?,
            ]),
            // general negation gets NO KeyExists guard (the negated shape can
            // span several fields) — can_remove_filter() is false for it, so
            // the scan-side filter repairs the null-row semantics
            Condition::Not(condition) => VixQuery::Not(Box::new(condition.to_vix_query(tokenize)?)),
        })
    }

    /// Fields the condition looks up in the per-file term index. A file that
    /// is missing any of them cannot evaluate this condition (the condition
    /// is skipped and the DataFusion filter is added back). match_all-style
    /// conditions scan tokens across every field and need no specific one.
    pub fn term_index_fields(&self) -> HashSet<String> {
        let mut fields = HashSet::new();
        match self {
            Condition::Equal(field, _)
            | Condition::NotEqual(field, _)
            | Condition::In(field, ..)
            | Condition::NumericCmp(field, ..)
            | Condition::Regex(field, _)
            | Condition::StrMatch(field, ..) => {
                fields.insert(field.clone());
            }
            // key terms live outside the per-field value index: an absent
            // key term is a correct "no doc has this path", not a missing
            // field
            Condition::IsNotNull(_) | Condition::IsNull(_) => {}
            Condition::MatchAll(_) | Condition::FuzzyMatchAll(..) | Condition::All() => {}
            Condition::Or(left, right) | Condition::And(left, right) => {
                fields.extend(left.term_index_fields());
                fields.extend(right.term_index_fields());
            }
            Condition::Not(condition) => {
                fields.extend(condition.term_index_fields());
            }
        }
        fields
    }

    /// SQL three-valued logic over a file where every field in `null_fields`
    /// is NULL in every row (the file has no key term for it): whether the
    /// condition can never evaluate to TRUE on any row of such a file.
    ///
    /// Comparisons on NULL are UNKNOWN and UNKNOWN never selects a row, so
    /// Equal / In / NotEqual / NOT IN on a null field are never TRUE (NULL
    /// != 'x' is UNKNOWN, not TRUE); `str_match`/regex UDFs do not match
    /// null input either; `IS NOT NULL` is genuinely FALSE. `AND` is never
    /// TRUE when either side never is; `OR` needs both; `NOT c` is TRUE
    /// only where `c` is genuinely FALSE, so it defers to
    /// [`Self::never_false_when_null`]. Fields outside `null_fields` are
    /// unknown statically and contribute conservatively.
    ///
    /// `true` means the condition matches nothing in the file — the caller
    /// maps it to [`VixQuery::Nothing`], an exact empty result.
    fn never_true_when_null(&self, null_fields: &HashSet<String>) -> bool {
        match self {
            Condition::Equal(field, _)
            | Condition::NotEqual(field, _)
            | Condition::In(field, ..)
            | Condition::NumericCmp(field, ..)
            | Condition::StrMatch(field, ..)
            | Condition::Regex(field, _)
            | Condition::IsNotNull(field) => null_fields.contains(field),
            // IS NULL on an all-null field is genuinely TRUE for every row
            Condition::IsNull(_) => false,
            Condition::MatchAll(_) | Condition::FuzzyMatchAll(..) | Condition::All() => false,
            Condition::And(left, right) => {
                left.never_true_when_null(null_fields) || right.never_true_when_null(null_fields)
            }
            Condition::Or(left, right) => {
                left.never_true_when_null(null_fields) && right.never_true_when_null(null_fields)
            }
            Condition::Not(inner) => inner.never_false_when_null(null_fields),
        }
    }

    /// Companion of [`Self::never_true_when_null`]: whether the condition
    /// can never evaluate to genuinely FALSE (as opposed to UNKNOWN) on any
    /// row. Only SQL comparisons are provably UNKNOWN-everywhere on a null
    /// field; `str_match`/regex UDFs may return FALSE on null input and
    /// `IS NOT NULL` IS false, so they never qualify (conservative — a
    /// wrong `true` here would let `NOT(..)` wrongly claim never-TRUE).
    fn never_false_when_null(&self, null_fields: &HashSet<String>) -> bool {
        match self {
            // comparisons on NULL are UNKNOWN, never genuinely FALSE
            Condition::Equal(field, _) | Condition::NotEqual(field, _) => {
                null_fields.contains(field)
            }
            // `NULL IN (..)` / `NULL NOT IN (..)` are UNKNOWN — except the
            // degenerate empty list, which is vacuously FALSE
            Condition::In(field, values, _) => !values.is_empty() && null_fields.contains(field),
            // same comparison semantics as In (values non-empty by
            // construction; guarded anyway)
            Condition::NumericCmp(field, values, ..) => {
                !values.is_empty() && null_fields.contains(field)
            }
            // IS NULL on an all-null field is TRUE for every row — never FALSE
            Condition::IsNull(field) => null_fields.contains(field),
            Condition::StrMatch(..)
            | Condition::Regex(..)
            | Condition::IsNotNull(_)
            | Condition::MatchAll(_)
            | Condition::FuzzyMatchAll(..) => false,
            Condition::All() => true,
            // AND is FALSE iff either side is: never-false needs both
            Condition::And(left, right) => {
                left.never_false_when_null(null_fields) && right.never_false_when_null(null_fields)
            }
            // OR is FALSE iff both sides are: one never-false side suffices
            Condition::Or(left, right) => {
                left.never_false_when_null(null_fields) || right.never_false_when_null(null_fields)
            }
            Condition::Not(inner) => inner.never_true_when_null(null_fields),
        }
    }

    /// Whether evaluating this condition would consult a field with skipped
    /// terms. Any-field conditions (match_all/fuzzy) consult fts TOKENS
    /// only, so they are tainted exactly by partial fields that are
    /// fts-marked in this file — a partial mark on a non-fts field (an
    /// oversize raw value elsewhere in the schema) cannot hide a token.
    pub fn uses_partial_fields(
        &self,
        partial_fields: &std::collections::HashSet<String>,
        fts_fields: &std::collections::HashSet<String>,
    ) -> bool {
        match self {
            Condition::Equal(field, _)
            | Condition::NotEqual(field, _)
            | Condition::In(field, ..)
            | Condition::NumericCmp(field, ..)
            | Condition::Regex(field, _)
            | Condition::StrMatch(field, ..)
            // conservative: key terms are written independently of the
            // skipped values, but a partial-marked field is already degraded
            // in this file — scan it rather than reason about writer paths
            | Condition::IsNotNull(field)
            | Condition::IsNull(field) => partial_fields.contains(field),
            Condition::MatchAll(value) => {
                !(value.is_empty() || value == "*")
                    && partial_fields.iter().any(|f| fts_fields.contains(f))
            }
            Condition::FuzzyMatchAll(value, _) => {
                !value.is_empty() && partial_fields.iter().any(|f| fts_fields.contains(f))
            }
            Condition::All() => false,
            Condition::Or(left, right) | Condition::And(left, right) => {
                left.uses_partial_fields(partial_fields, fts_fields)
                    || right.uses_partial_fields(partial_fields, fts_fields)
            }
            Condition::Not(condition) => {
                condition.uses_partial_fields(partial_fields, fts_fields)
            }
        }
    }

    // get the fields use for search in datafusion(for add filter back logical)
    pub fn get_schema_fields(&self, fst_fields: &[String]) -> HashSet<String> {
        let mut fields = HashSet::new();
        match self {
            Condition::Equal(field, _)
            | Condition::NotEqual(field, _)
            | Condition::StrMatch(field, ..)
            | Condition::In(field, ..)
            | Condition::NumericCmp(field, ..)
            | Condition::Regex(field, _)
            | Condition::IsNotNull(field)
            | Condition::IsNull(field) => {
                fields.insert(field.clone());
            }
            Condition::MatchAll(_) | Condition::FuzzyMatchAll(..) => {
                fields.extend(fst_fields.iter().cloned());
            }
            Condition::All() => {}
            Condition::Or(left, right) | Condition::And(left, right) => {
                fields.extend(left.get_schema_fields(fst_fields));
                fields.extend(right.get_schema_fields(fst_fields));
            }
            Condition::Not(condition) => {
                fields.extend(condition.get_schema_fields(fst_fields));
            }
        }
        fields
    }

    pub fn to_physical_expr(
        &self,
        schema: &arrow_schema::Schema,
        fst_fields: &[String],
    ) -> Result<Arc<dyn PhysicalExpr>, anyhow::Error> {
        let cfg = get_config();
        match self {
            Condition::Equal(name, value) => {
                let index = schema.index_of(name).unwrap();
                let left = Arc::new(Column::new(name, index));
                let field = schema.field(index);
                let right = get_scalar_value(value, field.data_type())?;
                Ok(Arc::new(BinaryExpr::new(left, Operator::Eq, right)))
            }
            Condition::NotEqual(name, value) => {
                let index = schema.index_of(name).unwrap();
                let left = Arc::new(Column::new(name, index));
                let field = schema.field(index);
                let right = get_scalar_value(value, field.data_type())?;
                Ok(Arc::new(BinaryExpr::new(left, Operator::NotEq, right)))
            }
            Condition::StrMatch(name, value, case_sensitive) => {
                create_str_match_expr(schema, name, value, *case_sensitive)
            }
            Condition::In(name, values, negated) => {
                let index = schema.index_of(name).unwrap();
                let left = Arc::new(Column::new(name, index));
                let field = schema.field(index);
                let values: Vec<Arc<dyn PhysicalExpr>> = values
                    .iter()
                    .map(|value| get_scalar_value(value, field.data_type()).map(|v| v as _))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(InListExpr::try_new(
                    left, values, *negated, schema,
                )?))
            }
            // reconstructed with the scan schema's own type — the normalized
            // texts parse under every numeric/bool type they were built for
            Condition::NumericCmp(name, values, negated, _) => {
                let index = schema
                    .index_of(name)
                    .map_err(|e| anyhow::anyhow!("numeric field {name} not in schema: {e}"))?;
                let field = schema.field(index);
                let left = Arc::new(Column::new(name, index));
                if let [value] = values.as_slice()
                    && !negated
                {
                    let right = get_scalar_value(value, field.data_type())?;
                    return Ok(Arc::new(BinaryExpr::new(left, Operator::Eq, right)));
                }
                let values: Vec<Arc<dyn PhysicalExpr>> = values
                    .iter()
                    .map(|value| get_scalar_value(value, field.data_type()).map(|v| v as _))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(InListExpr::try_new(
                    left, values, *negated, schema,
                )?))
            }
            Condition::Regex(field, ..) => {
                // Regex conditions are extracted only on the promql path,
                // which never reconstructs a physical filter; can_remove_
                // filter() is false, so this must be an error, not a panic —
                // a crafted/mixed plan otherwise aborts the process.
                Err(anyhow::anyhow!(
                    "Condition::Regex({field}) cannot be reconstructed as a physical expression \
                     (regex conditions are only supported for promql)"
                ))?
            }
            Condition::IsNull(name) => {
                let index = schema.index_of(name)?;
                Ok(Arc::new(IsNullExpr::new(Arc::new(Column::new(
                    name, index,
                )))))
            }
            Condition::IsNotNull(name) => {
                let index = schema
                    .index_of(name)
                    .map_err(|e| anyhow::anyhow!("IS NOT NULL field {name} not in schema: {e}"))?;
                Ok(Arc::new(IsNotNullExpr::new(Arc::new(Column::new(
                    name, index,
                )))))
            }
            Condition::MatchAll(value) => {
                let value = value
                    .trim_start_matches("re:") // regex
                    .trim_start_matches('*') // contains
                    .trim_end_matches('*') // prefix or contains
                    .to_string();
                let term = if cfg.common.utf8_view_enabled {
                    Arc::new(Literal::new(ScalarValue::Utf8View(Some(format!(
                        "%{value}%"
                    )))))
                } else {
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(format!("%{value}%")))))
                };
                let mut expr_list: Vec<Arc<dyn PhysicalExpr>> =
                    Vec::with_capacity(fst_fields.len());
                for field in fst_fields.iter() {
                    let term = if !cfg.common.utf8_view_enabled
                        && let Some((_idx, schema_field)) = schema.column_with_name(field)
                        && schema_field.data_type() == &DataType::LargeUtf8
                    {
                        Arc::new(Literal::new(ScalarValue::LargeUtf8(Some(format!(
                            "%{value}%"
                        )))))
                    } else {
                        term.clone()
                    };
                    expr_list.push(create_like_expr_with_not_null(field, term, schema));
                }
                if expr_list.is_empty() {
                    return Err(anyhow::anyhow!(
                        "Using match_all() function in a stream that don't have full text search field"
                    )); // already check this in sql.rs
                }
                Ok(disjunction(expr_list))
            }
            Condition::FuzzyMatchAll(value, distance) => {
                let fuzzy_expr = Arc::new(fuzzy_match_udf::FUZZY_MATCH_UDF.clone());
                let term = if cfg.common.utf8_view_enabled {
                    Arc::new(Literal::new(ScalarValue::Utf8View(Some(value.to_string()))))
                } else {
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(value.to_string()))))
                };
                let distance = Arc::new(Literal::new(ScalarValue::Int64(Some(*distance as i64))));
                let mut expr_list: Vec<Arc<dyn PhysicalExpr>> =
                    Vec::with_capacity(fst_fields.len());
                for field in fst_fields.iter() {
                    let term = if !cfg.common.utf8_view_enabled
                        && let Some((_idx, schema_field)) = schema.column_with_name(field)
                        && schema_field.data_type() == &DataType::LargeUtf8
                    {
                        Arc::new(Literal::new(ScalarValue::LargeUtf8(Some(
                            value.to_string(),
                        ))))
                    } else {
                        term.clone()
                    };
                    let new_expr = Arc::new(ScalarFunctionExpr::try_new(
                        fuzzy_expr.clone(),
                        vec![
                            Arc::new(Column::new(field, schema.index_of(field).unwrap())),
                            term,
                            distance.clone(),
                        ],
                        schema,
                        Arc::new(ConfigOptions::default()),
                    )?);
                    expr_list.push(new_expr);
                }
                if expr_list.is_empty() {
                    return Err(anyhow::anyhow!(
                        "Using fuzzy_match_all() function in a stream that don't have full text search field"
                    )); // already check this in sql.rs
                }
                Ok(disjunction(expr_list))
            }
            Condition::All() => Ok(Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))),
            Condition::Or(left, right) => {
                let left = left.to_physical_expr(schema, fst_fields)?;
                let right = right.to_physical_expr(schema, fst_fields)?;
                Ok(Arc::new(BinaryExpr::new(left, Operator::Or, right)))
            }
            Condition::And(left, right) => {
                let left = left.to_physical_expr(schema, fst_fields)?;
                let right = right.to_physical_expr(schema, fst_fields)?;
                Ok(Arc::new(BinaryExpr::new(left, Operator::And, right)))
            }
            Condition::Not(condition) => {
                let expr = condition.to_physical_expr(schema, fst_fields)?;
                Ok(Arc::new(NotExpr::new(expr)))
            }
        }
    }

    pub fn can_remove_filter(&self) -> bool {
        match self {
            Condition::Equal(..) => true,
            // exact: to_vix_query conjoins KeyExists(field), so the bitmap
            // matches SQL three-valued logic (null rows excluded). Files
            // where the field is not term-indexed skip the condition and
            // get the filter re-applied instead.
            Condition::NotEqual(..) => true,
            Condition::StrMatch(..) => true,
            // negated In carries the same KeyExists conjunction as NotEqual
            Condition::In(..) => true,
            // positive numeric probes are exact (every probed form coerces
            // to the compared value under the scan's json_get projection);
            // the NEGATED shape keeps the filter — rows whose stored value
            // does not coerce (a non-numeric string under a float field)
            // are UNKNOWN in SQL but pass the bitmap's KeyExists guard
            Condition::NumericCmp(_, _, negated, _) => !negated,
            Condition::Regex(..) => false,
            // exact on core files (key term present == non-null); files without
            // key terms skip the condition and get the filter re-applied
            Condition::IsNotNull(..) => true,
            // exact: writers omit nulls from _source and key terms, so
            // no-key-term <=> NULL — the bitmap needs no re-filter
            Condition::IsNull(..) => true,
            Condition::MatchAll(v) => is_alphanumeric(v),
            Condition::FuzzyMatchAll(..) => false,
            Condition::All() => true,
            Condition::Or(left, right) => left.can_remove_filter() && right.can_remove_filter(),
            Condition::And(left, right) => left.can_remove_filter() && right.can_remove_filter(),
            // general negation gets no KeyExists guard in to_vix_query: its
            // bitmap wrongly matches null rows, keep the filter (SQL
            // three-valued logic)
            Condition::Not(..) => false,
        }
    }
}

// TODO: duplication with datafusion/optimizer/physical_optimizer/utils.rs
fn is_physical_column(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if expr.downcast_ref::<Column>().is_some() {
        true
    } else if let Some(expr) = expr.downcast_ref::<CastExpr>() {
        is_physical_column(expr.expr())
    } else {
        false
    }
}

// TODO: duplication with datafusion/optimizer/physical_optimizer/utils.rs
fn get_physical_column_name(expr: &Arc<dyn PhysicalExpr>) -> &str {
    if let Some(expr) = expr.downcast_ref::<Column>() {
        expr.name()
    } else if let Some(expr) = expr.downcast_ref::<CastExpr>() {
        get_physical_column_name(expr.expr())
    } else {
        UNKNOWN_NAME
    }
}

fn is_physical_value(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<Literal>().is_some()
}

fn get_physical_value(expr: &Arc<dyn PhysicalExpr>) -> String {
    match try_physical_value(expr) {
        Some(value) => value,
        None => unimplemented!("get_physical_value not support {:?}", expr),
    }
}

/// Total variant of [`get_physical_value`]: `None` for literal shapes with
/// no text image (used by the extraction GATE, which must reject instead of
/// panic).
pub(crate) fn try_physical_value(expr: &Arc<dyn PhysicalExpr>) -> Option<String> {
    let literal = expr.downcast_ref::<Literal>()?;
    Some(match literal.value() {
        ScalarValue::Boolean(Some(b)) => b.to_string(),
        ScalarValue::Int8(Some(i)) => i.to_string(),
        ScalarValue::Int16(Some(i)) => i.to_string(),
        ScalarValue::Int32(Some(i)) => i.to_string(),
        ScalarValue::Int64(Some(i)) => i.to_string(),
        ScalarValue::UInt8(Some(i)) => i.to_string(),
        ScalarValue::UInt16(Some(i)) => i.to_string(),
        ScalarValue::UInt32(Some(i)) => i.to_string(),
        ScalarValue::UInt64(Some(i)) => i.to_string(),
        ScalarValue::Float32(Some(f)) => f.to_string(),
        ScalarValue::Float64(Some(f)) => f.to_string(),
        ScalarValue::Utf8(Some(s)) => s.clone(),
        ScalarValue::LargeUtf8(Some(s)) => s.clone(),
        ScalarValue::Utf8View(Some(s)) => s.clone(),
        ScalarValue::Binary(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => return None,
    })
}

// combine all exprs with OR operator
fn disjunction(exprs: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
    if exprs.len() == 1 {
        exprs[0].clone()
    } else {
        // conjuction all expr in exprs
        let mut expr = exprs[0].clone();
        for e in exprs.into_iter().skip(1) {
            expr = Arc::new(BinaryExpr::new(expr, Operator::Or, e));
        }
        expr
    }
}

/// Normalize a comparison literal for a [`NumericKind`] field into the
/// canonical text stored in [`Condition::NumericCmp`]. `None` = the literal
/// cannot be served by the index for this kind and the predicate must stay a
/// plan filter:
///
/// - `Int`: integral values only. A fractional literal (`int_field = 38.5`) or one outside the
///   i64/u64 ranges is unservable — deliberately NOT mapped to "matches nothing", since the stored
///   JSON may drift (safer to scan than to guess);
/// - `Float`: any finite f64 (normalized to ryu shortest text);
/// - `Bool`: exactly `true`/`false`;
/// - text that does not parse as a number at all (schema-type vs stored-JSON drift can make
///   DataFusion hand us one) is unservable for every kind.
pub fn normalize_numeric_literal(kind: NumericKind, literal: &str) -> Option<String> {
    match kind {
        NumericKind::Bool => matches!(literal, "true" | "false").then(|| literal.to_string()),
        NumericKind::Int => {
            if let Ok(value) = literal.parse::<u64>() {
                return Some(canonical_u64_text(value));
            }
            if let Ok(value) = literal.parse::<i64>() {
                return Some(canonical_i64_text(value));
            }
            // an integral float spelling ("38.0", "1e3") still compares
            // equal to integer-projected rows after DataFusion's cast
            let value = literal.parse::<f64>().ok()?;
            integral_int_text(value)
        }
        NumericKind::Float => {
            let value = literal.parse::<f64>().ok()?;
            canonical_f64_text(value)
        }
    }
}

/// The canonical INTEGER text of an integral, in-range f64; `None` when the
/// value is fractional, non-finite or outside the u64/i64 ranges. Integral
/// f64 values inside those ranges convert exactly.
fn integral_int_text(value: f64) -> Option<String> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    if (0.0..18446744073709551616.0).contains(&value) {
        Some(canonical_u64_text(value as u64))
    } else if (-9223372036854775808.0..0.0).contains(&value) {
        Some(canonical_i64_text(value as i64))
    } else {
        None
    }
}

/// The dictionary tokens one normalized [`Condition::NumericCmp`] value
/// probes: every canonical form the same JSON value can take (int and float
/// spellings are distinct terms), each as the TAGGED numeric term and as the
/// raw string term — string-stored rows in canonical spelling coerce to the
/// same value under the scan's `json_get_*` projection, so they must match
/// too. `Int` deliberately probes no float form (`json_get_int` maps
/// float-stored rows to NULL).
fn numeric_probe_tokens(kind: NumericKind, value: &str) -> Vec<Vec<u8>> {
    let mut texts: Vec<String> = vec![value.to_string()];
    match kind {
        NumericKind::Bool | NumericKind::Int => {}
        NumericKind::Float => {
            if let Ok(parsed) = value.parse::<f64>() {
                if let Some(int_form) = integral_int_text(parsed) {
                    texts.push(int_form);
                }
                if parsed == 0.0 {
                    // ±0.0 compare equal but are distinct canonical terms
                    for zero in ["0.0", "-0.0", "0"] {
                        texts.push(zero.to_string());
                    }
                }
            }
        }
    }
    texts.sort_unstable();
    texts.dedup();
    let mut tokens = Vec::with_capacity(texts.len() * 2);
    for text in texts {
        tokens.push(numeric_value_token(&text));
        tokens.push(text.into_bytes());
    }
    tokens
}

fn get_scalar_value(value: &str, data_type: &DataType) -> Result<Arc<Literal>, anyhow::Error> {
    Ok(match data_type {
        DataType::Boolean => Arc::new(Literal::new(ScalarValue::Boolean(Some(value.parse()?)))),
        DataType::Int8 => Arc::new(Literal::new(ScalarValue::Int8(Some(value.parse()?)))),
        DataType::Int16 => Arc::new(Literal::new(ScalarValue::Int16(Some(value.parse()?)))),
        DataType::Int32 => Arc::new(Literal::new(ScalarValue::Int32(Some(value.parse()?)))),
        DataType::Int64 => Arc::new(Literal::new(ScalarValue::Int64(Some(value.parse()?)))),
        DataType::UInt8 => Arc::new(Literal::new(ScalarValue::UInt8(Some(value.parse()?)))),
        DataType::UInt16 => Arc::new(Literal::new(ScalarValue::UInt16(Some(value.parse()?)))),
        DataType::UInt32 => Arc::new(Literal::new(ScalarValue::UInt32(Some(value.parse()?)))),
        DataType::UInt64 => Arc::new(Literal::new(ScalarValue::UInt64(Some(value.parse()?)))),
        DataType::Float32 => Arc::new(Literal::new(ScalarValue::Float32(Some(value.parse()?)))),
        DataType::Float64 => Arc::new(Literal::new(ScalarValue::Float64(Some(value.parse()?)))),
        DataType::Utf8 => Arc::new(Literal::new(ScalarValue::Utf8(Some(value.to_string())))),
        DataType::LargeUtf8 => Arc::new(Literal::new(ScalarValue::LargeUtf8(Some(
            value.to_string(),
        )))),
        DataType::Utf8View => {
            Arc::new(Literal::new(ScalarValue::Utf8View(Some(value.to_string()))))
        }
        DataType::Binary => Arc::new(Literal::new(ScalarValue::Binary(Some(
            value.as_bytes().to_vec(),
        )))),
        _ => unimplemented!(),
    })
}

fn create_like_expr_with_not_null(
    field: &str,
    term: Arc<dyn PhysicalExpr>,
    schema: &arrow_schema::Schema,
) -> Arc<dyn PhysicalExpr> {
    let column = Arc::new(Column::new(field, schema.index_of(field).unwrap()));
    Arc::new(BinaryExpr::new(
        Arc::new(IsNotNullExpr::new(column.clone())),
        Operator::And,
        Arc::new(LikeExpr::new(false, true, column, term.clone())),
    ))
}

fn create_str_match_expr(
    schema: &arrow_schema::Schema,
    name: &str,
    value: &str,
    case_sensitive: bool,
) -> Result<Arc<dyn PhysicalExpr>, anyhow::Error> {
    let index = schema.index_of(name).unwrap();
    let field = schema.field(index);
    let col = Arc::new(Column::new(name, index));

    // if the field is Utf8View, we need to cast it to Utf8 for str_match udf
    let left: Arc<dyn PhysicalExpr> = if *field.data_type() == DataType::Utf8View {
        Arc::new(CastExpr::new(col, DataType::Utf8, None))
    } else {
        col
    };

    // if the field is Utf8View, we need to cast it to Utf8 for str_match udf
    let data_type = if *field.data_type() == DataType::Utf8View {
        DataType::Utf8
    } else {
        field.data_type().clone()
    };

    let right = get_scalar_value(value, &data_type)?;
    let udf = if case_sensitive {
        Arc::new(str_match_udf::STR_MATCH_UDF.clone())
    } else {
        Arc::new(str_match_udf::STR_MATCH_IGNORE_CASE_UDF.clone())
    };

    let udf_expr = Arc::new(ScalarFunctionExpr::try_new(
        udf.clone(),
        vec![left, right],
        schema,
        Arc::new(ConfigOptions::default()),
    )?);
    Ok(udf_expr)
}

fn is_alphanumeric(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_alphanumeric())
}

fn _is_blank_or_alphanumeric(s: &str) -> bool {
    s.chars()
        .all(|c| c.is_ascii_whitespace() || c.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_condition_term_index_fields_equal() {
        let condition = Condition::Equal("field1".to_string(), "value1".to_string());
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 1);
        assert!(fields.contains("field1"));
    }

    #[test]
    fn test_condition_term_index_fields_in() {
        let condition = Condition::In(
            "field2".to_string(),
            vec![
                "value1".to_string(),
                "value2".to_string(),
                "value3".to_string(),
            ],
            false,
        );
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 1);
        assert!(fields.contains("field2"));
    }

    #[test]
    fn test_condition_term_index_fields_regex() {
        let condition = Condition::Regex("field3".to_string(), "pattern.*".to_string());
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 1);
        assert!(fields.contains("field3"));
    }

    #[test]
    fn test_condition_term_index_fields_match_all() {
        // match_all scans tokens across every field: no specific field needed
        let condition = Condition::MatchAll("search_term".to_string());
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 0);
    }

    #[test]
    fn test_condition_term_index_fields_fuzzy_match_all() {
        // fuzzy_match_all scans tokens across every field: no specific field needed
        let condition = Condition::FuzzyMatchAll("search_term".to_string(), 2);
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 0);
    }

    #[test]
    fn test_condition_term_index_fields_all() {
        let condition = Condition::All();
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 0);
    }

    #[test]
    fn test_condition_term_index_fields_or_simple() {
        let left = Condition::Equal("field1".to_string(), "value1".to_string());
        let right = Condition::Equal("field2".to_string(), "value2".to_string());
        let condition = Condition::Or(Box::new(left), Box::new(right));
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 2);
        assert!(fields.contains("field1"));
        assert!(fields.contains("field2"));
    }

    #[test]
    fn test_condition_term_index_fields_and_simple() {
        let left = Condition::Equal("field1".to_string(), "value1".to_string());
        let right = Condition::In("field2".to_string(), vec!["value1".to_string()], false);
        let condition = Condition::And(Box::new(left), Box::new(right));
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 2);
        assert!(fields.contains("field1"));
        assert!(fields.contains("field2"));
    }

    #[test]
    fn test_condition_term_index_fields_or_with_overlap() {
        let left = Condition::Equal("field1".to_string(), "value1".to_string());
        let right = Condition::Equal("field1".to_string(), "value2".to_string());
        let condition = Condition::Or(Box::new(left), Box::new(right));
        let fields = condition.term_index_fields();

        // Should only have one field since both conditions use the same field
        assert_eq!(fields.len(), 1);
        assert!(fields.contains("field1"));
    }

    #[test]
    fn test_condition_term_index_fields_and_with_overlap() {
        let left = Condition::Equal("field1".to_string(), "value1".to_string());
        let right = Condition::Regex("field1".to_string(), "pattern.*".to_string());
        let condition = Condition::And(Box::new(left), Box::new(right));
        let fields = condition.term_index_fields();

        // Should only have one field since both conditions use the same field
        assert_eq!(fields.len(), 1);
        assert!(fields.contains("field1"));
    }

    #[test]
    fn test_condition_term_index_fields_nested_complex() {
        // Create a complex nested condition: (field1 = value1 OR field2 = value2) AND (field3 =
        // value3 OR match_all(term))
        let left_or = Condition::Or(
            Box::new(Condition::Equal("field1".to_string(), "value1".to_string())),
            Box::new(Condition::Equal("field2".to_string(), "value2".to_string())),
        );
        let right_or = Condition::Or(
            Box::new(Condition::Equal("field3".to_string(), "value3".to_string())),
            Box::new(Condition::MatchAll("search_term".to_string())),
        );
        let condition = Condition::And(Box::new(left_or), Box::new(right_or));
        let fields = condition.term_index_fields();

        // match_all contributes no specific field
        assert_eq!(fields.len(), 3);
        assert!(fields.contains("field1"));
        assert!(fields.contains("field2"));
        assert!(fields.contains("field3"));
    }

    #[test]
    fn test_condition_term_index_fields_all_types_mixed() {
        // Test with all different condition types mixed together
        let equal_cond = Condition::Equal("equal_field".to_string(), "value".to_string());
        let in_cond = Condition::In("in_field".to_string(), vec!["val1".to_string()], false);
        let regex_cond = Condition::Regex("regex_field".to_string(), "pattern.*".to_string());
        let match_all_cond = Condition::MatchAll("search_term".to_string());
        let fuzzy_match_cond = Condition::FuzzyMatchAll("fuzzy_term".to_string(), 1);
        let all_cond = Condition::All();

        // Create nested structure: ((equal OR in) AND (regex OR match_all)) OR (fuzzy_match_all AND
        // all)
        let left_or = Condition::Or(Box::new(equal_cond), Box::new(in_cond));
        let right_or = Condition::Or(Box::new(regex_cond), Box::new(match_all_cond));
        let left_and = Condition::And(Box::new(left_or), Box::new(right_or));
        let right_and = Condition::And(Box::new(fuzzy_match_cond), Box::new(all_cond));
        let condition = Condition::Or(Box::new(left_and), Box::new(right_and));

        let fields = condition.term_index_fields();

        // equal_field, in_field, regex_field (match_all and fuzzy_match_all
        // need no specific field)
        assert_eq!(fields.len(), 3);
        assert!(fields.contains("equal_field"));
        assert!(fields.contains("in_field"));
        assert!(fields.contains("regex_field"));
    }

    #[test]
    fn test_condition_term_index_fields_empty_field_names() {
        let condition = Condition::Equal("".to_string(), "value".to_string());
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 1);
        assert!(fields.contains(""));
    }

    #[test]
    fn test_condition_term_index_fields_special_characters() {
        let condition = Condition::Equal("field.with.dots".to_string(), "value".to_string());
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 1);
        assert!(fields.contains("field.with.dots"));
    }

    #[test]
    fn test_condition_term_index_fields_unicode_field_names() {
        let condition = Condition::Equal("поле".to_string(), "значение".to_string());
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 1);
        assert!(fields.contains("поле"));
    }

    // add some test for str_match
    #[test]
    fn test_str_match() {
        let condition = Condition::StrMatch("field1".to_string(), "value1".to_string(), true);
        let fields = condition.term_index_fields();

        assert_eq!(fields.len(), 1);
        assert!(fields.contains("field1"));
    }

    #[test]
    fn test_is_alphanumeric() {
        assert!(is_alphanumeric("123"));
        assert!(is_alphanumeric("123abc"));
        assert!(!is_alphanumeric("123 abc"));
        assert!(!is_alphanumeric("123 abc 123"));
    }

    #[test]
    fn test_is_blank_or_alphanumeric() {
        assert!(_is_blank_or_alphanumeric("123"));
        assert!(_is_blank_or_alphanumeric("123abc"));
        assert!(_is_blank_or_alphanumeric("123 abc"));
        assert!(_is_blank_or_alphanumeric("123 abc 123"));
    }

    #[test]
    fn test_index_condition_new() {
        let condition = IndexCondition::new();
        assert!(condition.conditions.is_empty());
    }

    #[test]
    fn test_index_condition_add_condition() {
        let mut index_condition = IndexCondition::new();
        let condition = Condition::Equal("field1".to_string(), "value1".to_string());

        index_condition.add_condition(condition.clone());

        assert_eq!(index_condition.conditions.len(), 1);
        assert!(matches!(
            index_condition.conditions[0],
            Condition::Equal(ref field, ref value) if field == "field1" && value == "value1"
        ));
    }

    #[test]
    fn test_index_condition_to_query() {
        let mut index_condition = IndexCondition::new();
        index_condition.add_condition(Condition::Equal("field1".to_string(), "value1".to_string()));
        index_condition.add_condition(Condition::Equal("field2".to_string(), "value2".to_string()));

        let query_string = index_condition.to_query();
        assert_eq!(query_string, "field1=value1 AND field2=value2");
    }

    #[test]
    fn test_index_condition_to_query_empty() {
        let index_condition = IndexCondition::new();
        let query_string = index_condition.to_query();
        assert_eq!(query_string, "");
    }

    #[test]
    fn test_index_condition_is_empty() {
        let mut index_condition = IndexCondition::new();
        assert!(index_condition.is_empty());

        index_condition.add_condition(Condition::Equal("field1".to_string(), "value1".to_string()));
        assert!(!index_condition.is_empty());
    }

    #[test]
    fn test_index_condition_is_condition_all() {
        let mut index_condition = IndexCondition::new();
        index_condition.add_condition(Condition::All());

        assert!(index_condition.is_condition_all());

        index_condition.add_condition(Condition::Equal("field1".to_string(), "value1".to_string()));
        assert!(!index_condition.is_condition_all());
    }

    #[test]
    fn test_condition_to_query_equal() {
        let condition = Condition::Equal("field1".to_string(), "value1".to_string());
        assert_eq!(condition.to_query(), "field1=value1");
    }

    #[test]
    fn test_condition_to_query_not_equal() {
        let condition = Condition::NotEqual("field1".to_string(), "value1".to_string());
        assert_eq!(condition.to_query(), "field1!=value1");
    }

    #[test]
    fn test_condition_to_query_str_match() {
        let condition = Condition::StrMatch("field1".to_string(), "value1".to_string(), true);
        assert_eq!(condition.to_query(), "str_match(field1, 'value1')");

        let condition = Condition::StrMatch("field1".to_string(), "value1".to_string(), false);
        assert_eq!(
            condition.to_query(),
            "str_match_ignore_case(field1, 'value1')"
        );
    }

    #[test]
    fn test_condition_to_query_in() {
        let condition = Condition::In(
            "field1".to_string(),
            vec!["value1".to_string(), "value2".to_string()],
            false,
        );
        assert_eq!(condition.to_query(), "field1 IN (value1,value2)");

        let condition = Condition::In("field1".to_string(), vec!["value1".to_string()], true);
        assert_eq!(condition.to_query(), "field1 NOT IN (value1)");
    }

    #[test]
    fn test_condition_to_query_regex() {
        let condition = Condition::Regex("field1".to_string(), "pattern.*".to_string());
        assert_eq!(condition.to_query(), "field1=~pattern.*");
    }

    #[test]
    fn test_condition_to_query_match_all() {
        let condition = Condition::MatchAll("search_term".to_string());
        assert_eq!(
            condition.to_query(),
            "(_all:search_term):([\"search\", \"term\"])"
        );
    }

    #[test]
    fn test_condition_to_query_fuzzy_match_all() {
        let condition = Condition::FuzzyMatchAll("search_term".to_string(), 2);
        assert_eq!(condition.to_query(), "_all:fuzzy(search_term, 2)");
    }

    #[test]
    fn test_condition_to_query_all() {
        let condition = Condition::All();
        assert_eq!(condition.to_query(), "ALL");
    }

    #[test]
    fn test_condition_to_query_or() {
        let left = Condition::Equal("field1".to_string(), "value1".to_string());
        let right = Condition::Equal("field2".to_string(), "value2".to_string());
        let condition = Condition::Or(Box::new(left), Box::new(right));
        assert_eq!(condition.to_query(), "(field1=value1 OR field2=value2)");
    }

    #[test]
    fn test_condition_to_query_and() {
        let left = Condition::Equal("field1".to_string(), "value1".to_string());
        let right = Condition::Equal("field2".to_string(), "value2".to_string());
        let condition = Condition::And(Box::new(left), Box::new(right));
        assert_eq!(condition.to_query(), "(field1=value1 AND field2=value2)");
    }

    #[test]
    fn test_condition_to_query_not() {
        let inner = Condition::Equal("field1".to_string(), "value1".to_string());
        let condition = Condition::Not(Box::new(inner));
        assert_eq!(condition.to_query(), "NOT(field1=value1)");
    }

    fn partial(fields: &[&str]) -> std::collections::HashSet<String> {
        fields.iter().map(|f| f.to_string()).collect()
    }

    #[test]
    fn test_uses_partial_fields_field_conditions() {
        let partial_fields = partial(&["log"]);
        let no_fts = partial(&[]);
        assert!(
            Condition::Equal("log".to_string(), "v".to_string())
                .uses_partial_fields(&partial_fields, &no_fts)
        );
        assert!(
            !Condition::Equal("level".to_string(), "v".to_string())
                .uses_partial_fields(&partial_fields, &no_fts)
        );
        assert!(
            Condition::StrMatch("log".to_string(), "v".to_string(), true)
                .uses_partial_fields(&partial_fields, &no_fts)
        );
        assert!(
            Condition::Not(Box::new(Condition::In(
                "log".to_string(),
                vec!["v".to_string()],
                false
            )))
            .uses_partial_fields(&partial_fields, &no_fts)
        );
    }

    #[test]
    fn test_uses_partial_fields_any_field_conditions() {
        // match_all/fuzzy consult fts TOKENS: only a partial field that is
        // fts-marked in the file can hide a token. "log" partial + fts:
        let partial_fields = partial(&["log"]);
        let fts = partial(&["log", "body"]);
        assert!(Condition::MatchAll("err".to_string()).uses_partial_fields(&partial_fields, &fts));
        assert!(
            Condition::FuzzyMatchAll("err".to_string(), 1)
                .uses_partial_fields(&partial_fields, &fts)
        );
        // trivially-true match_all never touches the term index
        assert!(!Condition::MatchAll("*".to_string()).uses_partial_fields(&partial_fields, &fts));
        assert!(!Condition::MatchAll(String::new()).uses_partial_fields(&partial_fields, &fts));
        assert!(!Condition::All().uses_partial_fields(&partial_fields, &fts));

        // a partial NON-fts field (oversize raw value elsewhere) cannot
        // taint token queries — the .13-era 5.2s match_all regression:
        // one such file forced a full row-store scan on every query
        let non_fts_partial = partial(&["params.payload"]);
        assert!(
            !Condition::MatchAll("err".to_string()).uses_partial_fields(&non_fts_partial, &fts)
        );
        assert!(
            !Condition::FuzzyMatchAll("err".to_string(), 1)
                .uses_partial_fields(&non_fts_partial, &fts)
        );
        // ...but value lookups on that field still fall back
        assert!(
            Condition::Equal("params.payload".to_string(), "v".to_string())
                .uses_partial_fields(&non_fts_partial, &fts)
        );
    }

    #[test]
    fn test_index_condition_uses_partial_fields() {
        let mut index_condition = IndexCondition::new();
        index_condition.add_condition(Condition::Equal("log".to_string(), "v".to_string()));

        let no_fts = partial(&[]);
        assert!(index_condition.uses_partial_fields(&partial(&["log"]), &no_fts));
        assert!(!index_condition.uses_partial_fields(&partial(&["level"]), &no_fts));
        // empty partial set: nothing to taint
        assert!(!index_condition.uses_partial_fields(&partial(&[]), &no_fts));
    }

    #[test]
    fn test_condition_can_remove_filter_equal() {
        let condition = Condition::Equal("field1".to_string(), "value1".to_string());
        assert!(condition.can_remove_filter());
    }

    #[test]
    fn test_condition_can_remove_filter_regex() {
        let condition = Condition::Regex("field1".to_string(), "pattern.*".to_string());
        assert!(!condition.can_remove_filter());
    }

    #[test]
    fn test_condition_can_remove_filter_match_all_alphanumeric() {
        let condition = Condition::MatchAll("test123".to_string());
        assert!(condition.can_remove_filter());
    }

    #[test]
    fn test_condition_can_remove_filter_match_all_non_alphanumeric() {
        let condition = Condition::MatchAll("test 123".to_string());
        assert!(!condition.can_remove_filter());
    }

    #[test]
    fn test_condition_can_remove_filter_fuzzy_match_all() {
        let condition = Condition::FuzzyMatchAll("test".to_string(), 2);
        assert!(!condition.can_remove_filter());
    }

    /// Negated conditions: NotEqual / negated-In stay removable (their vix
    /// query carries the KeyExists non-null guard), while a general NOT(..)
    /// gets no guard and must keep the scan-side filter.
    #[test]
    fn test_condition_can_remove_filter_negations() {
        assert!(Condition::NotEqual("f".into(), "v".into()).can_remove_filter());
        assert!(Condition::In("f".into(), vec!["v".into()], true).can_remove_filter());
        assert!(
            !Condition::Not(Box::new(Condition::Equal("f".into(), "v".into()))).can_remove_filter()
        );
        assert!(
            !Condition::Not(Box::new(Condition::StrMatch("f".into(), "v".into(), true)))
                .can_remove_filter()
        );
    }

    /// Regex conditions cannot be reconstructed as physical filters — that
    /// must be an error (a mixed plan would otherwise abort the process).
    #[test]
    fn test_regex_physical_expr_is_error_not_panic() {
        let schema =
            arrow_schema::Schema::new(vec![arrow_schema::Field::new("f", DataType::Utf8, true)]);
        let err = Condition::Regex("f".into(), "err.*".into())
            .to_physical_expr(&schema, &[])
            .expect_err("regex reconstruction must error");
        assert!(err.to_string().contains("promql"), "{err}");
    }

    #[test]
    fn test_condition_can_remove_filter_or() {
        let left = Condition::Equal("field1".to_string(), "value1".to_string());
        let right = Condition::Equal("field2".to_string(), "value2".to_string());
        let condition = Condition::Or(Box::new(left), Box::new(right));
        assert!(condition.can_remove_filter());

        let left = Condition::Equal("field1".to_string(), "value1".to_string());
        let right = Condition::Regex("field2".to_string(), "pattern.*".to_string());
        let condition = Condition::Or(Box::new(left), Box::new(right));
        assert!(!condition.can_remove_filter());
    }

    #[test]
    fn test_disjunction_single() {
        use datafusion::{physical_expr::expressions::Literal, scalar::ScalarValue};

        let expr = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        let result = disjunction(vec![expr.clone()]);
        assert_eq!(result.as_ref() as *const _, expr.as_ref() as *const _);
    }

    #[test]
    fn test_get_scalar_value_boolean() {
        let result = get_scalar_value("true", &DataType::Boolean);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_scalar_value_int64() {
        let result = get_scalar_value("123", &DataType::Int64);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_scalar_value_utf8() {
        let result = get_scalar_value("test", &DataType::Utf8);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_scalar_value_error() {
        let result = get_scalar_value("not_a_number", &DataType::Int64);
        assert!(result.is_err());
    }

    /// A per-file capability check over a fixed list of term-indexed
    /// fields; everything else reports [`FieldCap::FtsOnly`] (carried by the
    /// file but not raw-value servable — the skip + filter-back path, the
    /// exact behavior of the old boolean closure).
    fn indexed(fields: &'static [&'static str]) -> impl Fn(&str) -> FieldCap {
        move |name: &str| {
            if fields.contains(&name) {
                FieldCap::Term
            } else {
                FieldCap::FtsOnly
            }
        }
    }

    /// A per-file capability check over a fixed list of term-indexed
    /// fields; everything else reports [`FieldCap::Absent`] (no document of
    /// the file carries the field — provably NULL everywhere).
    fn absent_unless(fields: &'static [&'static str]) -> impl Fn(&str) -> FieldCap {
        move |name: &str| {
            if fields.contains(&name) {
                FieldCap::Term
            } else {
                FieldCap::Absent
            }
        }
    }

    /// Stand-in for the per-file match_all tokenizer in tests.
    fn tok(value: &str) -> Vec<String> {
        o2_collect_search_tokens(value)
    }

    fn exact(field: &str, token: &str) -> VixQuery {
        VixQuery::Exact {
            field: field.to_string(),
            token: token.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_to_vix_query_all_fields_present() {
        // File has both A and B.
        let mut cond = IndexCondition::new();
        cond.add_condition(Condition::Equal("A".into(), "a".into()));
        cond.add_condition(Condition::Equal("B".into(), "b".into()));

        let (query, has_skipped) = cond
            .to_vix_query("test", &indexed(&["A", "B"]), &tok)
            .expect("query build should succeed");
        assert!(!has_skipped, "no field is missing, should not skip");
        assert_eq!(query, VixQuery::And(vec![exact("A", "a"), exact("B", "b")]));
    }

    #[test]
    fn test_is_not_null_condition_maps_to_key_exists() {
        // IS NOT NULL → KeyExists over the flattened path
        assert_eq!(
            Condition::IsNotNull("http.status".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::KeyExists {
                path: "http.status".to_string()
            }
        );
        // the field is looked up in the key terms, not the value term index:
        // per-file term coverage must not gate it
        assert!(
            Condition::IsNotNull("f".into())
                .term_index_fields()
                .is_empty()
        );
        // exact on core files, so the scan-side filter can be dropped
        assert!(Condition::IsNotNull("f".into()).can_remove_filter());
        // the filter-back projection must include the field
        assert_eq!(
            Condition::IsNotNull("f".into()).get_schema_fields(&[]),
            HashSet::from(["f".to_string()])
        );
        assert_eq!(
            Condition::IsNotNull("f".into()).to_query(),
            "f IS NOT NULL".to_string()
        );
    }

    #[test]
    fn test_to_vix_query_is_not_null_builds_key_exists() {
        // IS NOT NULL evaluates through key terms even when the field has no
        // value terms in the file (an absent key term is a correct
        // all-zeros, not a missing field) — per-file term coverage must not
        // gate it
        let mut lone = IndexCondition::new();
        lone.add_condition(Condition::IsNotNull("B".into()));
        let (query, has_skipped) = lone.to_vix_query("test", &indexed(&["A"]), &tok).unwrap();
        assert!(!has_skipped);
        assert_eq!(
            query,
            VixQuery::KeyExists {
                path: "B".to_string()
            }
        );
    }

    #[test]
    fn test_is_not_null_physical_expr_roundtrip() {
        use datafusion::physical_expr::expressions::IsNotNullExpr;

        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_timestamp", DataType::Int64, false),
            arrow_schema::Field::new("f", DataType::Utf8, true),
        ]);
        let expr: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(Arc::new(Column::new(
            "f",
            schema.index_of("f").unwrap(),
        ))));

        // physical → condition
        let condition = Condition::from_physical_expr(
            &expr,
            &HashMap::from([("f".to_string(), DataType::Utf8)]),
        );
        assert_eq!(condition, Condition::IsNotNull("f".to_string()));

        // condition → physical (the add-filter-back reconstruction)
        let rebuilt = condition.to_physical_expr(&schema, &[]).unwrap();
        assert_eq!(format!("{rebuilt}"), format!("{expr}"));

        // IS NULL round-trips the same way and maps to the key-term
        // complement (exact: writers omit nulls from _source AND key terms)
        let null_expr: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(Arc::new(Column::new(
            "f",
            schema.index_of("f").unwrap(),
        ))));
        let null_condition = Condition::from_physical_expr(
            &null_expr,
            &HashMap::from([("f".to_string(), DataType::Utf8)]),
        );
        assert_eq!(null_condition, Condition::IsNull("f".to_string()));
        let rebuilt = null_condition.to_physical_expr(&schema, &[]).unwrap();
        assert_eq!(format!("{rebuilt}"), format!("{null_expr}"));
        assert!(
            null_condition.can_remove_filter(),
            "IS NULL bitmap is exact"
        );
        assert_eq!(
            null_condition.to_vix_query(&|_| vec![]).unwrap(),
            VixQuery::Not(Box::new(VixQuery::KeyExists {
                path: "f".to_string()
            }))
        );
        // three-valued logic over a file where `f` is all-null: IS NULL is
        // genuinely TRUE for every row — it must NEVER prune to Nothing
        // (the 2026-07-26 "IS NULL on absent field returns no rows"
        // suspicion), and it is never FALSE (so NOT(f IS NULL) prunes)
        let nulls = HashSet::from(["f".to_string()]);
        assert!(!null_condition.never_true_when_null(&nulls));
        assert!(null_condition.never_false_when_null(&nulls));
        assert!(
            Condition::Not(Box::new(null_condition.clone())).never_true_when_null(&nulls),
            "NOT(f IS NULL) on an all-null file matches nothing"
        );

        // a field missing from the schema is an error, not a panic
        let missing = Condition::IsNotNull("missing".to_string());
        assert!(missing.to_physical_expr(&schema, &[]).is_err());
    }

    #[test]
    fn test_to_vix_query_missing_field_is_skipped() {
        // File only indexes A; condition references B (a field added after
        // this file was written, so it has no index data here).
        let mut cond = IndexCondition::new();
        cond.add_condition(Condition::Equal("A".into(), "a".into()));
        cond.add_condition(Condition::Equal("B".into(), "b".into()));

        let (query, has_skipped) = cond
            .to_vix_query("test", &indexed(&["A"]), &tok)
            .expect("query build should succeed even when a field is missing");
        assert!(has_skipped, "missing field B should be reported as skipped");
        // Only A should be referenced; B was dropped.
        assert_eq!(query, exact("A", "a"));
    }

    #[test]
    fn test_to_vix_query_all_fields_missing_returns_error() {
        // File indexes none of the referenced fields.
        // Should return an error so the caller falls back to parquet scan.
        let mut cond = IndexCondition::new();
        cond.add_condition(Condition::Equal("A".into(), "a".into()));
        cond.add_condition(Condition::Equal("B".into(), "b".into()));

        let result = cond.to_vix_query("test", &indexed(&["other_field"]), &tok);
        assert!(
            result.is_err(),
            "should return error when all fields are missing"
        );
    }

    #[test]
    fn test_to_vix_query_single_condition_missing_field() {
        // Single condition whose field is missing — should return an error.
        let mut cond = IndexCondition::new();
        cond.add_condition(Condition::Equal("B".into(), "b".into()));

        let result = cond.to_vix_query("test", &indexed(&["A"]), &tok);
        assert!(
            result.is_err(),
            "should return error when the only field is missing"
        );
    }

    #[test]
    fn test_to_vix_query_missing_field_inside_nested_condition() {
        // A missing field anywhere inside a nested condition skips the
        // whole top-level condition.
        let mut cond = IndexCondition::new();
        cond.add_condition(Condition::Or(
            Box::new(Condition::Equal("A".into(), "a".into())),
            Box::new(Condition::Equal("B".into(), "b".into())),
        ));
        cond.add_condition(Condition::Equal("A".into(), "a2".into()));

        let (query, has_skipped) = cond.to_vix_query("test", &indexed(&["A"]), &tok).unwrap();
        assert!(has_skipped);
        assert_eq!(query, exact("A", "a2"));
    }

    /// Absent fields (no document of the file carries the key): a condition
    /// that can never be TRUE on all-NULL input becomes [`VixQuery::Nothing`]
    /// — an exact empty result, never a skip. This is the fix for the
    /// absent-field full-scan cascade: the lone `Equal` used to leave
    /// `queries` empty (AllConditionsSkipped → file fully scanned).
    #[test]
    fn test_to_vix_query_absent_field_becomes_nothing() {
        // lone Equal on an absent field: evaluates (to nothing), no skip
        let mut lone = IndexCondition::new();
        lone.add_condition(Condition::Equal("B".into(), "b".into()));
        let (query, has_skipped) = lone
            .to_vix_query("test", &absent_unless(&["A"]), &tok)
            .expect("absent-field conditions must evaluate, not error");
        assert!(!has_skipped, "an absent field is exact, not a skip");
        assert_eq!(query, VixQuery::Nothing);

        // the other never-TRUE-on-NULL single-leaf shapes follow suit:
        // SQL three-valued logic (NULL != 'x' is UNKNOWN, not TRUE)
        for condition in [
            Condition::NotEqual("B".into(), "b".into()),
            Condition::In("B".into(), vec!["b".into()], false),
            Condition::In("B".into(), vec!["b".into()], true),
            Condition::StrMatch("B".into(), "b".into(), true),
            Condition::Regex("B".into(), "b.*".into()),
            // NOT over a pure comparison on the absent field is UNKNOWN
            // everywhere — never TRUE (must NOT become Not(Nothing) = all)
            Condition::Not(Box::new(Condition::Equal("B".into(), "b".into()))),
            // AND with a servable predicate can still never be TRUE
            Condition::And(
                Box::new(Condition::Equal("B".into(), "b".into())),
                Box::new(Condition::Equal("A".into(), "a".into())),
            ),
        ] {
            let cond = IndexCondition {
                conditions: vec![condition.clone()],
            };
            let (query, has_skipped) = cond
                .to_vix_query("test", &absent_unless(&["A"]), &tok)
                .unwrap_or_else(|e| panic!("{condition:?} must evaluate: {e}"));
            assert!(!has_skipped, "{condition:?} must not skip");
            assert_eq!(query, VixQuery::Nothing, "{condition:?}");
        }
    }

    /// An AND-list mixing an absent-field condition with a servable one:
    /// the absent one contributes `Nothing`, the servable one its term
    /// query — exact, no skip (the empty AND removes the file).
    #[test]
    fn test_to_vix_query_and_of_absent_and_term_conditions() {
        let mut cond = IndexCondition::new();
        cond.add_condition(Condition::Equal("B".into(), "b".into()));
        cond.add_condition(Condition::Equal("A".into(), "a".into()));
        let (query, has_skipped) = cond
            .to_vix_query("test", &absent_unless(&["A"]), &tok)
            .unwrap();
        assert!(!has_skipped);
        assert_eq!(
            query,
            VixQuery::And(vec![VixQuery::Nothing, exact("A", "a")])
        );
    }

    /// Shapes whose truth does not hinge on the absent field alone keep
    /// today's skip + filter-back (mapping them to `Nothing` would lose
    /// rows the servable side matches).
    #[test]
    fn test_to_vix_query_absent_field_mixed_shapes_still_skip() {
        for condition in [
            // OR with a servable predicate can be TRUE on its other side
            Condition::Or(
                Box::new(Condition::Equal("B".into(), "b".into())),
                Box::new(Condition::Equal("A".into(), "a".into())),
            ),
            // NOT(absent AND servable) is TRUE where the servable side is
            // genuinely FALSE
            Condition::Not(Box::new(Condition::And(
                Box::new(Condition::Equal("B".into(), "b".into())),
                Box::new(Condition::Equal("A".into(), "a".into())),
            ))),
        ] {
            let lone = IndexCondition {
                conditions: vec![condition.clone()],
            };
            let result = lone.to_vix_query("test", &absent_unless(&["A"]), &tok);
            assert!(
                result.is_err(),
                "{condition:?} alone must skip (AllConditionsSkipped)"
            );

            let mut with_term = IndexCondition::new();
            with_term.add_condition(condition.clone());
            with_term.add_condition(Condition::Equal("A".into(), "a2".into()));
            let (query, has_skipped) = with_term
                .to_vix_query("test", &absent_unless(&["A"]), &tok)
                .unwrap();
            assert!(has_skipped, "{condition:?} must report the skip");
            assert_eq!(query, exact("A", "a2"));
        }
    }

    /// FtsOnly still wins over Absent for the same condition: the file
    /// carries the fts field's rows, so the condition must be re-checked by
    /// the scan even when another referenced field is absent.
    #[test]
    fn test_to_vix_query_fts_only_beats_absent_within_a_condition() {
        let caps = |name: &str| match name {
            "F" => FieldCap::FtsOnly,
            "B" => FieldCap::Absent,
            _ => FieldCap::Term,
        };
        let mut cond = IndexCondition::new();
        cond.add_condition(Condition::Or(
            Box::new(Condition::Equal("F".into(), "f".into())),
            Box::new(Condition::Equal("B".into(), "b".into())),
        ));
        assert!(cond.to_vix_query("test", &caps, &tok).is_err());

        cond.add_condition(Condition::Equal("A".into(), "a".into()));
        let (query, has_skipped) = cond.to_vix_query("test", &caps, &tok).unwrap();
        assert!(has_skipped);
        assert_eq!(query, exact("A", "a"));
    }

    #[test]
    fn test_condition_to_vix_query_equal_and_not_equal() {
        assert_eq!(
            Condition::Equal("f".into(), "v".into())
                .to_vix_query(&tok)
                .unwrap(),
            exact("f", "v")
        );
        // != carries the non-null guard (SQL three-valued logic)
        assert_eq!(
            Condition::NotEqual("f".into(), "v".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::And(vec![
                VixQuery::Not(Box::new(exact("f", "v"))),
                VixQuery::KeyExists {
                    path: "f".to_string()
                },
            ])
        );
    }

    #[test]
    fn test_condition_to_vix_query_in() {
        let cond = Condition::In("f".into(), vec!["a".into(), "b".into()], false);
        assert_eq!(
            cond.to_vix_query(&tok).unwrap(),
            VixQuery::Or(vec![exact("f", "a"), exact("f", "b")])
        );

        // NOT IN carries the non-null guard (SQL three-valued logic)
        let negated = Condition::In("f".into(), vec!["a".into()], true);
        assert_eq!(
            negated.to_vix_query(&tok).unwrap(),
            VixQuery::And(vec![
                VixQuery::Not(Box::new(VixQuery::Or(vec![exact("f", "a")]))),
                VixQuery::KeyExists {
                    path: "f".to_string()
                },
            ])
        );
    }

    #[test]
    fn test_condition_to_vix_query_str_match_and_regex() {
        assert_eq!(
            Condition::StrMatch("f".into(), "Err".into(), true)
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::Contains {
                field: Some("f".to_string()),
                needle: b"Err".to_vec(),
                case_insensitive: false,
            }
        );
        assert_eq!(
            Condition::StrMatch("f".into(), "Err".into(), false)
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::Contains {
                field: Some("f".to_string()),
                needle: b"Err".to_vec(),
                case_insensitive: true,
            }
        );
        assert_eq!(
            Condition::Regex("f".into(), "err.*".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::Regex {
                field: Some("f".to_string()),
                pattern: "err.*".to_string(),
            }
        );
    }

    #[test]
    fn test_condition_to_vix_query_fuzzy_clamps_distance() {
        assert_eq!(
            Condition::FuzzyMatchAll("test".into(), 1)
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::Fuzzy {
                token: "test".to_string(),
                distance: 1,
            }
        );
        // distances above 2 are clamped (levenshtein automaton limit)
        assert_eq!(
            Condition::FuzzyMatchAll("test".into(), 5)
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::Fuzzy {
                token: "test".to_string(),
                distance: 2,
            }
        );
        assert!(
            Condition::FuzzyMatchAll(String::new(), 1)
                .to_vix_query(&tok)
                .is_err()
        );
    }

    #[test]
    fn test_condition_to_vix_query_bool_combinators() {
        let a = Condition::Equal("a".into(), "1".into());
        let b = Condition::Equal("b".into(), "2".into());
        assert_eq!(
            Condition::And(Box::new(a.clone()), Box::new(b.clone()))
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::And(vec![exact("a", "1"), exact("b", "2")])
        );
        assert_eq!(
            Condition::Or(Box::new(a.clone()), Box::new(b.clone()))
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::Or(vec![exact("a", "1"), exact("b", "2")])
        );
        assert_eq!(
            Condition::Not(Box::new(a)).to_vix_query(&tok).unwrap(),
            VixQuery::Not(Box::new(exact("a", "1")))
        );
        assert_eq!(Condition::All().to_vix_query(&tok).unwrap(), VixQuery::All);
    }

    // MatchAll translation table:
    //   ""/"*"        -> All
    //   plain token   -> TokenAnyField (lowercased by the tokenizer)
    //   trailing `x*` -> Prefix
    //   `*x*`         -> Contains (case-insensitive)
    //   leading `*x`  -> Regex `.*x` (regex-escaped token)
    //   multi token   -> And of the per-token queries
    #[test]
    fn test_condition_to_vix_query_match_all_empty_and_star() {
        assert_eq!(
            Condition::MatchAll(String::new())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::All
        );
        assert_eq!(
            Condition::MatchAll("*".into()).to_vix_query(&tok).unwrap(),
            VixQuery::All
        );
    }

    #[test]
    fn test_condition_to_vix_query_match_all_plain_token() {
        assert_eq!(
            Condition::MatchAll("error".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::TokenAnyField {
                token: b"error".to_vec(),
            }
        );
        // the search tokenizer lowercases, matching build-time tokens
        assert_eq!(
            Condition::MatchAll("Error".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::TokenAnyField {
                token: b"error".to_vec(),
            }
        );
    }

    #[test]
    fn test_condition_to_vix_query_match_all_trailing_star() {
        assert_eq!(
            Condition::MatchAll("err*".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::Prefix {
                field: None,
                prefix: b"err".to_vec(),
            }
        );
    }

    #[test]
    fn test_condition_to_vix_query_match_all_contains() {
        assert_eq!(
            Condition::MatchAll("*err*".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::Contains {
                field: None,
                needle: b"err".to_vec(),
                case_insensitive: true,
            }
        );
    }

    #[test]
    fn test_condition_to_vix_query_match_all_leading_star() {
        assert_eq!(
            Condition::MatchAll("*err".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::Regex {
                field: None,
                pattern: ".*err".to_string(),
            }
        );
    }

    #[test]
    fn test_condition_to_vix_query_match_all_multi_token() {
        assert_eq!(
            Condition::MatchAll("search term".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::And(vec![
                VixQuery::TokenAnyField {
                    token: b"search".to_vec(),
                },
                VixQuery::TokenAnyField {
                    token: b"term".to_vec(),
                },
            ])
        );
        // trailing star applies to the last token only
        assert_eq!(
            Condition::MatchAll("search term*".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::And(vec![
                VixQuery::TokenAnyField {
                    token: b"search".to_vec(),
                },
                VixQuery::Prefix {
                    field: None,
                    prefix: b"term".to_vec(),
                },
            ])
        );
        // leading star applies to the first token only
        assert_eq!(
            Condition::MatchAll("*search term".into())
                .to_vix_query(&tok)
                .unwrap(),
            VixQuery::And(vec![
                VixQuery::TokenAnyField {
                    token: b"term".to_vec(),
                },
                VixQuery::Regex {
                    field: None,
                    pattern: ".*search".to_string(),
                },
            ])
        );
    }

    #[test]
    fn test_condition_to_vix_query_match_all_no_tokens_is_error() {
        // wildcards with no token content cannot be translated
        assert!(Condition::MatchAll("**".into()).to_vix_query(&tok).is_err());
    }

    // ---- numeric/bool comparisons (Condition::NumericCmp) ----

    fn probe(field: &str, token: Vec<u8>) -> VixQuery {
        VixQuery::Exact {
            field: field.to_string(),
            token,
        }
    }

    #[test]
    fn test_normalize_numeric_literal_per_kind() {
        use NumericKind::*;
        // Int: integral texts only, normalized through the value
        assert_eq!(normalize_numeric_literal(Int, "38"), Some("38".into()));
        assert_eq!(normalize_numeric_literal(Int, "+38"), Some("38".into()));
        assert_eq!(normalize_numeric_literal(Int, "-5"), Some("-5".into()));
        assert_eq!(normalize_numeric_literal(Int, "38.0"), Some("38".into()));
        assert_eq!(normalize_numeric_literal(Int, "1e3"), Some("1000".into()));
        assert_eq!(
            normalize_numeric_literal(Int, "18446744073709551615"),
            Some("18446744073709551615".into())
        );
        assert_eq!(normalize_numeric_literal(Int, "38.5"), None);
        assert_eq!(normalize_numeric_literal(Int, "abc"), None);
        assert_eq!(normalize_numeric_literal(Int, "NaN"), None);
        // Float: any finite f64, ryu shortest text
        assert_eq!(normalize_numeric_literal(Float, "38"), Some("38.0".into()));
        assert_eq!(
            normalize_numeric_literal(Float, "38.50"),
            Some("38.5".into())
        );
        assert_eq!(
            normalize_numeric_literal(Float, "1e20"),
            Some("1e20".into())
        );
        assert_eq!(normalize_numeric_literal(Float, "NaN"), None);
        assert_eq!(normalize_numeric_literal(Float, "inf"), None);
        assert_eq!(normalize_numeric_literal(Float, "abc"), None);
        // Bool: exactly true/false
        assert_eq!(normalize_numeric_literal(Bool, "true"), Some("true".into()));
        assert_eq!(
            normalize_numeric_literal(Bool, "false"),
            Some("false".into())
        );
        assert_eq!(normalize_numeric_literal(Bool, "TRUE"), None);
        assert_eq!(normalize_numeric_literal(Bool, "1"), None);
    }

    #[test]
    fn test_numeric_cmp_probes_the_union_of_forms() {
        // Float kind, integral value: tagged {ryu, itoa} + the same raw
        // spellings (string-stored drift rows coerce under json_get_float)
        let cond = Condition::NumericCmp(
            "credit".into(),
            vec!["38.0".into()],
            false,
            NumericKind::Float,
        );
        let query = cond.to_vix_query(&tok).unwrap();
        assert_eq!(
            query,
            VixQuery::Or(vec![
                probe("credit", numeric_value_token("38")),
                probe("credit", b"38".to_vec()),
                probe("credit", numeric_value_token("38.0")),
                probe("credit", b"38.0".to_vec()),
            ])
        );

        // Int kind: NO float form — json_get_int maps float-stored rows to
        // NULL, so probing "38.0" would over-match
        let cond = Condition::NumericCmp("code".into(), vec!["38".into()], false, NumericKind::Int);
        assert_eq!(
            cond.to_vix_query(&tok).unwrap(),
            VixQuery::Or(vec![
                probe("code", numeric_value_token("38")),
                probe("code", b"38".to_vec()),
            ])
        );

        // fractional float: no int form
        let cond = Condition::NumericCmp(
            "credit".into(),
            vec!["38.5".into()],
            false,
            NumericKind::Float,
        );
        assert_eq!(
            cond.to_vix_query(&tok).unwrap(),
            VixQuery::Or(vec![
                probe("credit", numeric_value_token("38.5")),
                probe("credit", b"38.5".to_vec()),
            ])
        );

        // zero probes both signed float forms
        let cond = Condition::NumericCmp(
            "credit".into(),
            vec!["0.0".into()],
            false,
            NumericKind::Float,
        );
        let VixQuery::Or(probes) = cond.to_vix_query(&tok).unwrap() else {
            panic!("expected an Or of probes");
        };
        for text in ["0", "0.0", "-0.0"] {
            assert!(
                probes.contains(&probe("credit", numeric_value_token(text))),
                "missing tagged zero form {text:?}"
            );
        }

        // bool
        let cond =
            Condition::NumericCmp("ok".into(), vec!["true".into()], false, NumericKind::Bool);
        assert_eq!(
            cond.to_vix_query(&tok).unwrap(),
            VixQuery::Or(vec![
                probe("ok", numeric_value_token("true")),
                probe("ok", b"true".to_vec()),
            ])
        );

        // negated: KeyExists-guarded negation (SQL three-valued logic)
        let cond = Condition::NumericCmp("code".into(), vec!["38".into()], true, NumericKind::Int);
        assert_eq!(
            cond.to_vix_query(&tok).unwrap(),
            VixQuery::And(vec![
                VixQuery::Not(Box::new(VixQuery::Or(vec![
                    probe("code", numeric_value_token("38")),
                    probe("code", b"38".to_vec()),
                ]))),
                VixQuery::KeyExists {
                    path: "code".into()
                },
            ])
        );
    }

    #[test]
    fn test_numeric_cmp_semantics_flags() {
        let positive =
            Condition::NumericCmp("code".into(), vec!["38".into()], false, NumericKind::Int);
        let negated =
            Condition::NumericCmp("code".into(), vec!["38".into()], true, NumericKind::Int);
        // positive probes are exact; the negated bitmap may include rows
        // whose stored value does not coerce (UNKNOWN in SQL) — keep the
        // scan filter for it
        assert!(positive.can_remove_filter());
        assert!(!negated.can_remove_filter());
        // three-valued logic on an all-NULL (absent) field: comparisons are
        // never TRUE and never genuinely FALSE
        let absent: HashSet<String> = ["code".to_string()].into_iter().collect();
        assert!(positive.never_true_when_null(&absent));
        assert!(negated.never_true_when_null(&absent));
        assert!(positive.never_false_when_null(&absent));
        // field bookkeeping matches the other per-field comparisons
        assert!(positive.term_index_fields().contains("code"));
        assert!(positive.get_schema_fields(&[]).contains("code"));
        let partial: std::collections::HashSet<String> = ["code".to_string()].into_iter().collect();
        assert!(positive.uses_partial_fields(&partial, &Default::default()));
    }

    #[test]
    fn test_numeric_cmp_physical_expr_reconstruction() {
        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("code", DataType::Int64, true),
            arrow_schema::Field::new("credit", DataType::Float64, true),
            arrow_schema::Field::new("ok", DataType::Boolean, true),
        ]);
        // single positive value -> plain equality on the schema's own type
        let cond = Condition::NumericCmp("code".into(), vec!["38".into()], false, NumericKind::Int);
        assert_eq!(
            format!("{}", cond.to_physical_expr(&schema, &[]).unwrap()),
            "code@0 = 38"
        );
        let cond = Condition::NumericCmp(
            "credit".into(),
            vec!["38.5".into()],
            false,
            NumericKind::Float,
        );
        assert_eq!(
            format!("{}", cond.to_physical_expr(&schema, &[]).unwrap()),
            "credit@1 = 38.5"
        );
        let cond =
            Condition::NumericCmp("ok".into(), vec!["true".into()], false, NumericKind::Bool);
        assert_eq!(
            format!("{}", cond.to_physical_expr(&schema, &[]).unwrap()),
            "ok@2 = true"
        );
        // negated / multi-value -> IN list (negated keeps the filter, so the
        // reconstruction must round-trip)
        let cond = Condition::NumericCmp(
            "code".into(),
            vec!["38".into(), "40".into()],
            true,
            NumericKind::Int,
        );
        let expr = format!("{}", cond.to_physical_expr(&schema, &[]).unwrap());
        assert!(expr.contains("code@0 NOT IN"), "got {expr}");
        // a field missing from the scan schema is an error, not a panic
        let cond = Condition::NumericCmp("gone".into(), vec!["38".into()], false, NumericKind::Int);
        assert!(cond.to_physical_expr(&schema, &[]).is_err());
    }

    #[test]
    fn test_from_physical_expr_builds_numeric_cmp_by_registry_type() {
        let fields: HashMap<String, DataType> = [
            ("code".to_string(), DataType::Int64),
            ("credit".to_string(), DataType::Float64),
            ("ok".to_string(), DataType::Boolean),
            ("svc".to_string(), DataType::Utf8),
        ]
        .into_iter()
        .collect();
        let eq = |field: &str, index: usize, value: ScalarValue| -> Arc<dyn PhysicalExpr> {
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new(field, index)),
                Operator::Eq,
                Arc::new(Literal::new(value)),
            ))
        };

        // numeric-typed comparisons become NumericCmp with normalized texts
        assert_eq!(
            Condition::from_physical_expr(&eq("code", 0, ScalarValue::Int64(Some(38))), &fields),
            Condition::NumericCmp("code".into(), vec!["38".into()], false, NumericKind::Int)
        );
        // DataFusion's f.to_string() renders 38.0 as "38"; normalization
        // restores the canonical float text and the probe union covers both
        // JSON forms
        assert_eq!(
            Condition::from_physical_expr(
                &eq("credit", 1, ScalarValue::Float64(Some(38.0))),
                &fields
            ),
            Condition::NumericCmp(
                "credit".into(),
                vec!["38.0".into()],
                false,
                NumericKind::Float
            )
        );
        assert_eq!(
            Condition::from_physical_expr(&eq("ok", 2, ScalarValue::Boolean(Some(true))), &fields),
            Condition::NumericCmp("ok".into(), vec!["true".into()], false, NumericKind::Bool)
        );
        // string fields keep string semantics
        assert_eq!(
            Condition::from_physical_expr(
                &eq("svc", 3, ScalarValue::Utf8(Some("38".into()))),
                &fields
            ),
            Condition::Equal("svc".into(), "38".into())
        );
    }
}
