// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Conservative, bounded recognition shared by the index and core scan.

use std::sync::Arc;

use anyhow::Context as _;
use datafusion::{
    arrow::datatypes::DataType,
    logical_expr::Operator,
    physical_plan::{
        PhysicalExpr,
        expressions::{BinaryExpr, Column, Literal},
    },
    scalar::ScalarValue,
};

const MAX_OR_TERMS: usize = 1024;
const MAX_LITERAL_BYTES: usize = 256 * 1024;
const MAX_PREFIX_BYTES: usize = 4096;

pub(crate) fn string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

pub(crate) fn string_literal(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s),
        _ => None,
    }
}

/// Recognize a complete OR of direct, same-column equalities. Never discard
/// an unknown arm: that would turn the candidate set into a subset.
/// Casts, NULLs and mixed literal types fail open. Work and scratch storage
/// have hard limits even for a left-deep expression. No values are copied.
pub(crate) fn equal_or_literals(
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<(&Column, Vec<&ScalarValue>)> {
    if expr.downcast_ref::<BinaryExpr>()?.op() != &Operator::Or {
        return None;
    }
    let mut stack = vec![expr];
    let mut column: Option<&Column> = None;
    let mut values: Vec<&ScalarValue> = Vec::new();
    let mut visited = 0;
    let mut bytes = 0;
    while let Some(expr) = stack.pop() {
        visited += 1;
        if visited > MAX_OR_TERMS * 2 - 1 {
            return None;
        }
        let binary = expr.downcast_ref::<BinaryExpr>()?;
        match binary.op() {
            Operator::Or => {
                stack.push(binary.right());
                stack.push(binary.left());
            }
            Operator::Eq => {
                let (col, lit) = if let (Some(col), Some(lit)) = (
                    binary.left().downcast_ref::<Column>(),
                    binary.right().downcast_ref::<Literal>(),
                ) {
                    (col, lit)
                } else {
                    (
                        binary.right().downcast_ref::<Column>()?,
                        binary.left().downcast_ref::<Literal>()?,
                    )
                };
                let value = lit.value();
                if value.is_null()
                    || column.is_some_and(|c| c.name() != col.name() || c.index() != col.index())
                    || values
                        .first()
                        .is_some_and(|v| v.data_type() != value.data_type())
                {
                    return None;
                }
                bytes += string_literal(value).map_or(0, str::len);
                if bytes > MAX_LITERAL_BYTES || values.len() == MAX_OR_TERMS {
                    return None;
                }
                column = Some(col);
                values.push(value);
            }
            _ => return None,
        }
    }
    Some((column?, values))
}

/// Decode only a non-empty literal followed by one or more '%' wildcards.
/// Physical LikeExpr uses Arrow's implicit backslash escape. DataFusion
/// rejects other ESCAPE characters before creating this expression. Escaped
/// '%', '_' and '\\' are literal bytes; unknown escapes deliberately fail open.
/// A trailing escape, an interior wildcard or '_' wildcard is not a prefix.
pub(crate) fn like_prefix(pattern: &str) -> Option<String> {
    if pattern.len() > MAX_PREFIX_BYTES {
        return None;
    }
    let mut chars = pattern.chars();
    let mut prefix = String::new();
    while let Some(ch) = chars.next() {
        match ch {
            '%' => return (!prefix.is_empty() && chars.all(|c| c == '%')).then_some(prefix),
            '_' => return None,
            '\\' => match chars.next()? {
                escaped @ ('%' | '_' | '\\') => prefix.push(escaped),
                _ => return None,
            },
            ch => prefix.push(ch),
        }
    }
    None
}

/// Exclusive UTF-8 upper bound of a prefix. Increment a Unicode scalar,
/// not a byte, and skip the surrogate range. UTF-8 preserves scalar order.
/// An all-U+10FFFF prefix has no finite upper bound; its lower bound is safe.
pub(crate) fn prefix_upper_bound(prefix: &str) -> Option<String> {
    for (offset, ch) in prefix.char_indices().rev() {
        let next = match ch {
            '\u{10ffff}' => continue,
            '\u{d7ff}' => '\u{e000}',
            _ => char::from_u32(u32::from(ch) + 1)?,
        };
        let mut upper = prefix[..offset].to_string();
        upper.push(next);
        return Some(upper);
    }
    None
}

/// Rebuild LIKE without treating literal wildcard or escape characters as
/// operators. This is also used when a file cannot serve a prefix index.
pub(crate) fn prefix_pattern(prefix: &str) -> String {
    let mut pattern = String::with_capacity(prefix.len() + 1);
    for ch in prefix.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(ch);
    }
    pattern.push('%');
    pattern
}

/// Numbers/bools can be indexed before the writer casts the docs column to
/// strings. FTS emits no tokens for them; raw terms use numeric tags. A string
/// docs schema alone is therefore insufficient. Refuse predicates that could
/// match their text, including float specials, instead of trusting that schema.
fn may_match_scalar_text(value: &str, prefix: bool) -> bool {
    if value.starts_with(|ch: char| ch.is_ascii_digit() || matches!(ch, '+' | '-' | '.')) {
        return true;
    }
    let lower = value.to_ascii_lowercase();
    ["true", "false", "nan", "inf", "infinity"]
        .iter()
        .any(|scalar| {
            if prefix {
                scalar.starts_with(&lower)
            } else {
                *scalar == lower
            }
        })
}

/// Apply only to newly extracted paths. No row/column decoding: the detached
/// footer supplies the actual stored type and is released after this check.
/// Older files without a type proof, oversize omissions, and numeric drift
/// all fail open. Legacy IN/equality behavior is intentionally unchanged.
pub(crate) fn file_candidates_safe(
    condition: &super::IndexCondition,
    reader: &vortex_index::VixReader,
) -> anyhow::Result<bool> {
    use super::Condition;

    fn check(
        condition: &Condition,
        reader: &vortex_index::VixReader,
        docs: &vortex_index::DocsPointReader<'_>,
    ) -> anyhow::Result<bool> {
        vortex_index::check_read_cancelled()?;
        let field = match condition {
            Condition::NormalizedOr(inner) => {
                let Condition::In(field, values, false) = inner.as_ref() else {
                    return Ok(false);
                };
                if values
                    .iter()
                    .any(|value| may_match_scalar_text(value, false))
                {
                    return Ok(false);
                }
                field
            }
            Condition::Prefix(field, prefix) => {
                if may_match_scalar_text(prefix, true) {
                    return Ok(false);
                }
                field
            }
            Condition::And(items) | Condition::Or(items) => {
                for item in items {
                    if !check(item, reader, docs)? {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            Condition::Not(inner) => return check(inner, reader, docs),
            _ => return Ok(true),
        };
        if reader.field_oversize_skips(field) != 0 || reader.partial_fields().contains(field) {
            return Ok(false);
        }
        let schema = docs
            .schema()
            .with_context(|| format!("cannot prove stored string type for {field:?}"))?;
        Ok(schema
            .field_with_name(field)
            .is_ok_and(|f| string_type(f.data_type())))
    }

    let docs = reader.detached_docs();
    for conjunct in &condition.conditions {
        if !check(conjunct, reader, &docs)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests;
