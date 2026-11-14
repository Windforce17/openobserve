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

use std::collections::HashSet;

use arrow::{
    array::{Array, AsArray},
    datatypes::{
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
    record_batch::RecordBatch,
};
use arrow_schema::DataType;
use config::{metrics::QUERY_STAR_HITS_BATCHES_TOTAL, utils::arrow::record_batches_to_json_rows};
use serde_json::{Map, Value};
use vortex_index::SOURCE_COL_NAME;

use super::{StarExpandStats, expand_star_source, expand_star_source_hits};

fn supports_direct_star_hits(batch: &RecordBatch) -> bool {
    let mut names = HashSet::with_capacity(batch.num_columns());
    batch.schema_ref().fields().iter().all(|field| {
        names.insert(field.name())
            && matches!(
                field.data_type(),
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
            )
    })
}

fn direct_string(array: &dyn Array, row: usize) -> Option<&str> {
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(row)),
        DataType::Utf8View => Some(array.as_string_view().value(row)),
        _ => None,
    }
}

fn direct_value(array: &dyn Array, row: usize) -> Value {
    match array.data_type() {
        DataType::Boolean => Value::from(array.as_boolean().value(row)),
        DataType::Int8 => Value::from(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => Value::from(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => Value::from(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => Value::from(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => Value::from(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => Value::from(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => Value::from(array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => Value::from(array.as_primitive::<UInt64Type>().value(row)),
        _ => Value::from(direct_string(array, row).expect("validated direct star column")),
    }
}

/// Materialize star hits without serializing the batch to JSON first. Unsupported
/// types and duplicate physical names fall back as a whole batch. Expansion and
/// its diagnostics apply only to retained rows, just like truncate-then-expand.
pub fn record_batches_to_star_hits(
    batches: &[&RecordBatch],
    max_rows: Option<usize>,
) -> anyhow::Result<(Vec<Map<String, Value>>, StarExpandStats)> {
    let mut rows = Vec::new();
    let mut stats = StarExpandStats::default();
    for batch in batches {
        let retained = batch
            .num_rows()
            .min(max_rows.unwrap_or(usize::MAX).saturating_sub(rows.len()));
        if !supports_direct_star_hits(batch) {
            QUERY_STAR_HITS_BATCHES_TOTAL
                .with_label_values(&["fallback"])
                .inc();
            // Serialize even discarded rows to preserve writer errors after the limit.
            let mut fallback = record_batches_to_json_rows(&[batch])?;
            fallback.truncate(retained);
            let next = expand_star_source_hits(&mut fallback, true);
            stats.missing_source += next.missing_source;
            stats.non_string_source += next.non_string_source;
            stats.unparsable_source += next.unparsable_source;
            if stats.first_parse_error.is_none() {
                stats.first_parse_error = next.first_parse_error;
            }
            rows.extend(fallback);
            continue;
        }
        QUERY_STAR_HITS_BATCHES_TOTAL
            .with_label_values(&["direct"])
            .inc();
        for row_index in 0..retained {
            let mut row = Map::new();
            let mut source = None;
            for (field, array) in batch.schema_ref().fields().iter().zip(batch.columns()) {
                if array.data_type() == &DataType::Null || array.is_null(row_index) {
                    continue;
                }
                let value = if field.name() == SOURCE_COL_NAME {
                    source = direct_string(array.as_ref(), row_index);
                    // Preserve the old Map's insertion/removal order, including
                    // swap-remove when serde_json enables preserve_order.
                    Value::Null
                } else {
                    direct_value(array.as_ref(), row_index)
                };
                row.insert(field.name().clone(), value);
            }
            if row.remove(SOURCE_COL_NAME).is_none() {
                stats.missing_source += 1;
            } else if let Some(source) = source {
                expand_star_source(source, &mut row, &mut stats);
            } else {
                stats.non_string_source += 1;
            }
            rows.push(row);
        }
    }
    Ok((rows, stats))
}

#[cfg(test)]
#[path = "star_hits_tests.rs"]
mod tests;
