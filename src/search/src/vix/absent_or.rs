// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use vortex_index::VixReader;

use super::{FieldCap, field_capability};
use crate::index::{Condition, IndexCondition};

// Numeric/bool terms are tagged, whereas Equal/In probe raw string terms.
// A stored scalar can be cast to strings by the scan. Without a docs-type
// proof, keep scalar spellings on that scan path rather than introducing a
// new raw-term lookup that would miss e.g. a stored integer queried as "7".
fn non_scalar_literal(value: &str) -> bool {
    !value.eq_ignore_ascii_case("true")
        && !value.eq_ignore_ascii_case("false")
        && value.parse::<f64>().is_err()
}

/// Simplify a positive equality OR only when this file proves every omitted
/// field is NULL and every remaining arm has complete raw-value terms. This is
/// a WHERE truth-set equivalence, NOT a general Boolean rewrite: NULL = x is
/// UNKNOWN, so replacing it with FALSE underneath NOT would be incorrect.
/// No metadata is fetched, and the original condition still identifies caches.
pub(super) fn simplify(
    reader: &VixReader,
    condition: &IndexCondition,
    enabled: bool,
) -> Option<IndexCondition> {
    if !enabled || !reader.has_index() || !reader.columns_complete() {
        return None;
    }
    let mut replacement = None;
    for (index, conjunct) in condition.conditions.iter().enumerate() {
        let Condition::Or(arms) = conjunct else {
            continue;
        };
        // Bound scratch work; unsupported/nested shapes retain the old route.
        if arms.len() < 2 || arms.len() > 1024 {
            continue;
        }
        let mut kept = Vec::with_capacity(arms.len());
        let mut supported = true;
        for arm in arms {
            let field = match arm {
                Condition::Equal(field, value) if non_scalar_literal(value) => field,
                Condition::In(field, values, false)
                    if !values.is_empty()
                        && values.iter().all(|value| non_scalar_literal(value)) =>
                {
                    field
                }
                _ => {
                    supported = false;
                    break;
                }
            };
            if matches!(
                field.as_str(),
                "" | "_timestamp" | "_source" | "_original" | "_id"
            ) || reader.field_oversize_skips(field) != 0
            {
                supported = false;
                break;
            }
            match field_capability(reader, field) {
                FieldCap::Absent => {}
                FieldCap::Term => kept.push(arm.clone()),
                // A physical column without usable terms is NOT absent. In
                // particular, bloom-only, token-only and partial fields stay
                // on the existing fallback path, even alongside an absent arm.
                _ => {
                    supported = false;
                    break;
                }
            }
        }
        // An all-absent OR already becomes Nothing in the legacy converter.
        if !supported || kept.is_empty() || kept.len() == arms.len() {
            continue;
        }
        let rewritten = if kept.len() == 1 {
            kept.pop().unwrap()
        } else {
            Condition::Or(kept)
        };
        replacement
            .get_or_insert_with(|| condition.clone())
            .conditions[index] = rewritten;
    }
    replacement
}

#[cfg(test)]
mod tests;
