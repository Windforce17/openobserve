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
    borrow::{Borrow, Cow},
    io::{BufRead, Seek},
    sync::{Arc, LazyLock as Lazy},
};

use arrow_json::reader;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use regex::Regex;
use serde_json::{Map, Value};

use super::str::find;
use crate::{
    FxIndexMap, TIMESTAMP_COL_NAME,
    meta::{promql::HASH_LABEL, stream::StreamType},
};

const MAX_PARTITION_KEY_LENGTH: usize = 100;

static RE_CORRECT_STREAM_NAME: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^a-zA-Z0-9_:]+").unwrap());

pub fn infer_json_schema<R: BufRead>(
    reader: R,
    max_read_records: Option<usize>,
    stream_type: StreamType,
) -> Result<Schema, ArrowError> {
    let (schema, _) = reader::infer_json_schema(reader, max_read_records)?;
    Ok(fix_schema(schema, stream_type))
}

pub fn infer_json_schema_from_seekable<R: BufRead + Seek>(
    reader: R,
    max_read_records: Option<usize>,
    stream_type: StreamType,
) -> Result<Schema, ArrowError> {
    let (schema, _) = reader::infer_json_schema_from_seekable(reader, max_read_records)?;
    Ok(fix_schema(schema, stream_type))
}

pub fn infer_json_schema_from_map<I, V>(
    stream_name: &str,
    stream_type: impl Into<StreamType>,
    value_iter: I,
) -> Result<Schema, ArrowError>
where
    I: Iterator<Item = V>,
    V: Borrow<Map<String, Value>>,
{
    let mut fields = None;
    for value in value_iter {
        if fields.is_none() {
            fields = Some(FxIndexMap::with_capacity_and_hasher(
                value.borrow().len(),
                Default::default(),
            ));
        }
        infer_json_schema_from_object(stream_name, fields.as_mut().unwrap(), value.borrow())?;
    }
    let fields = fields.unwrap_or_default();
    let fields = fields
        .into_iter()
        .map(|(_, field)| field)
        .collect::<Vec<_>>();
    Ok(fix_schema(Schema::new(fields), stream_type.into()))
}

/// Infer one request's schema without widening a field after its first non-null
/// scalar value. The database's watched schema merge is still the authority:
/// this only makes a new field's registration candidate deterministic with
/// respect to input order. A concurrent writer may win the registration race,
/// in which case its type becomes canonical and the caller casts to it.
pub fn infer_json_schema_from_map_first_seen<I, V>(
    stream_name: &str,
    stream_type: impl Into<StreamType>,
    value_iter: I,
) -> Result<Schema, ArrowError>
where
    I: Iterator<Item = V>,
    V: Borrow<Map<String, Value>>,
{
    let mut fields = FxIndexMap::default();
    let mut reserved = false;
    for value in value_iter {
        if !reserved {
            fields.reserve(value.borrow().len());
            reserved = true;
        }
        for (key, value) in value.borrow() {
            if fields.contains_key(key) || value.is_null() {
                continue;
            }
            let data_type = match value {
                Value::String(_) => DataType::Utf8,
                Value::Number(v) => match number_data_type(v) {
                    Some(data_type) => data_type,
                    None => {
                        log::warn!(
                            "stream: {stream_name}, first non-null number for field {key:?} is outside i64/u64/f64; pinning it as string: {v:?}"
                        );
                        DataType::Utf8
                    }
                },
                Value::Bool(_) => DataType::Boolean,
                _ => {
                    return Err(ArrowError::SchemaError(format!(
                        "Cannot infer canonical schema for {stream_name}: field {key:?} has a non-scalar first value"
                    )));
                }
            };
            fields.insert(key.clone(), Field::new(key, data_type, true));
        }
    }
    Ok(fix_schema(
        Schema::new(fields.into_values().collect::<Vec<_>>()),
        stream_type.into(),
    ))
}

pub fn infer_json_schema_from_values<I, V>(
    stream_name: &str,
    stream_type: impl Into<StreamType>,
    value_iter: I,
) -> Result<Schema, ArrowError>
where
    I: Iterator<Item = V>,
    V: Borrow<Value>,
{
    let mut fields = None;
    for value in value_iter {
        match value.borrow() {
            Value::Object(v) => {
                if fields.is_none() {
                    fields = Some(FxIndexMap::with_capacity_and_hasher(
                        v.len(),
                        Default::default(),
                    ));
                }
                infer_json_schema_from_object(stream_name, fields.as_mut().unwrap(), v)?;
            }
            _ => {
                return Err(ArrowError::SchemaError(
                    "Cannot infer schema from non-object value".to_string(),
                ));
            }
        }
    }
    let fields = fields.unwrap_or_default();
    let fields = fields
        .into_iter()
        .map(|(_, field)| field)
        .collect::<Vec<_>>();
    Ok(fix_schema(Schema::new(fields), stream_type.into()))
}

fn infer_json_schema_from_object(
    stream_name: &str,
    fields: &mut FxIndexMap<String, Field>,
    value: &Map<String, Value>,
) -> Result<(), ArrowError> {
    // Records of one request usually repeat the key order of the first one,
    // so the field right after the previous match is compared by name before
    // falling back to a hash lookup. Either way the same entry is updated.
    let mut next = 0;
    for (key, value) in value.iter() {
        let data_type = match value {
            Value::String(_) => DataType::Utf8,
            Value::Number(v) => match number_data_type(v) {
                Some(data_type) => data_type,
                None => {
                    // For numbers that are too large to fit in i64/u64/f64,
                    // treat them as strings to preserve the data
                    log::warn!(
                        "stream: {stream_name}, Number value too large for standard numeric types, treating as string: {key:?} {v:?}"
                    );
                    DataType::Utf8
                }
            },
            Value::Bool(_) => DataType::Boolean,
            Value::Null => continue,
            _ => {
                return Err(ArrowError::SchemaError(
                    "Cannot infer schema from non-basic type value".to_string(),
                ));
            }
        };
        let index = match fields.get_index(next) {
            Some((name, _)) if name == key => Some(next),
            _ => fields.get_index_of(key.as_str()),
        };
        next = match index {
            Some(index) => {
                merge_data_type(&mut fields[index], key, data_type)?;
                index + 1
            }
            None => {
                fields.insert(key.clone(), Field::new(key, data_type, true));
                fields.len()
            }
        };
    }
    Ok(())
}

/// The inferred type of a JSON number; `None` when it fits none of i64, u64
/// and finite f64, which callers store as a string.
///
/// Equivalent to testing `is_i64()`, `is_u64()` and `is_f64()` in that order.
/// With serde_json's `arbitrary_precision` every `Number` is its decimal text
/// and each of those predicates parses it again, so classification dominated
/// schema inference. The two shapes that dominate ingest are decided from the
/// text alone: at most 18 digits always fit i64, and a plain decimal
/// (`-?d+.d+`, no exponent) shorter than 300 bytes is never an integer and
/// always parses to a finite f64. Every other text takes the parsing chain.
pub(crate) fn number_data_type(number: &serde_json::Number) -> Option<DataType> {
    let text = number.as_str();
    let unsigned = text.strip_prefix('-').unwrap_or(text).as_bytes();
    if !unsigned.is_empty() && unsigned.len() <= 18 && unsigned.iter().all(u8::is_ascii_digit) {
        return Some(DataType::Int64);
    }
    if text.len() < 300 && is_plain_decimal(unsigned) {
        return Some(DataType::Float64);
    }
    if number.is_i64() {
        Some(DataType::Int64)
    } else if number.is_u64() {
        Some(DataType::UInt64)
    } else if number.is_f64() {
        Some(DataType::Float64)
    } else {
        None
    }
}

/// `d+.d+` with ASCII digits only.
fn is_plain_decimal(text: &[u8]) -> bool {
    let Some(dot) = text.iter().position(|&b| b == b'.') else {
        return false;
    };
    let (integer, fraction) = (&text[..dot], &text[dot + 1..]);
    !integer.is_empty()
        && !fraction.is_empty()
        && integer.iter().all(u8::is_ascii_digit)
        && fraction.iter().all(u8::is_ascii_digit)
}

/// Widen an already inferred field for a new value type. The rules are the
/// request-level merge: strings absorb everything, floats absorb integers and
/// booleans, and a boolean yields to any other type.
fn merge_data_type(field: &mut Field, key: &str, data_type: DataType) -> Result<(), ArrowError> {
    let f_type = field.data_type();
    if f_type == &data_type {
        return Ok(());
    }
    let widen = match (f_type, &data_type) {
        (DataType::LargeUtf8, _) => false,
        (DataType::Utf8, DataType::LargeUtf8) => true,
        (DataType::Utf8, _) => false,
        (DataType::Float64, DataType::UInt64)
        | (DataType::Float64, DataType::Int64)
        | (DataType::Float64, DataType::Boolean) => false,
        (DataType::Int64, DataType::UInt64)
        | (DataType::Int64, DataType::Float64)
        | (DataType::Int64, DataType::Utf8)
        | (DataType::Int64, DataType::LargeUtf8) => true,
        (DataType::UInt64, DataType::Int64)
        | (DataType::UInt64, DataType::Boolean)
        | (DataType::Int64, DataType::Boolean) => false,
        (DataType::UInt64, DataType::Float64)
        | (DataType::UInt64, DataType::Utf8)
        | (DataType::UInt64, DataType::LargeUtf8) => true,
        (DataType::Float64, DataType::Utf8) | (DataType::Float64, DataType::LargeUtf8) => true,
        (DataType::Boolean, _) => true,
        _ => {
            return Err(ArrowError::SchemaError(format!(
                "Cannot infer schema from conflicting types: {f_type:?} and {data_type:?}"
            )));
        }
    };
    if widen {
        *field = Field::new(key, data_type, true);
    }
    Ok(())
}

/// Fix the schema to ensure that the start_time and end_time fields are always present with uint64
/// and the __hash__ field is always present with uint64
/// and that null fields are removed and sort the fields by name.
fn fix_schema(schema: Schema, stream_type: StreamType) -> Schema {
    let mut fields = if stream_type == StreamType::Traces {
        itertools::chain(
            schema.fields().into_iter().filter_map(|f| {
                if f.name() != "start_time"
                    && f.name() != "end_time"
                    && f.data_type() != &DataType::Null
                {
                    Some(f.to_owned())
                } else {
                    None
                }
            }),
            vec![
                Arc::new(Field::new("start_time", DataType::UInt64, true)),
                Arc::new(Field::new("end_time", DataType::UInt64, true)),
            ],
        )
        .collect::<Vec<_>>()
    } else {
        schema
            .fields()
            .into_iter()
            .filter_map(|f| {
                if f.data_type() == &DataType::Null {
                    None
                } else {
                    Some(f.to_owned())
                }
            })
            .collect::<Vec<_>>()
    };
    fields = fields
        .into_iter()
        .map(|x| {
            if x.name() == TIMESTAMP_COL_NAME {
                Arc::new(Field::new(
                    TIMESTAMP_COL_NAME.to_string(),
                    DataType::Int64,
                    false,
                ))
            } else if stream_type == StreamType::Metrics
                && x.data_type() == &DataType::Int64
                && x.name() == HASH_LABEL
            {
                Arc::new(Field::new(x.name().clone(), DataType::UInt64, false))
            } else {
                x
            }
        })
        .collect::<Vec<_>>();
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    Schema::new(fields)
}

// format partition key
pub fn format_partition_key(input: &str) -> String {
    let mut output = String::with_capacity(std::cmp::min(input.len(), MAX_PARTITION_KEY_LENGTH));
    for c in input.chars() {
        if output.len() > MAX_PARTITION_KEY_LENGTH {
            break;
        }
        if c.is_alphanumeric() || c == '=' || c == '-' || c == '_' {
            output.push(c);
        }
    }
    output
}

// format stream name
pub fn format_stream_name(stream_name: String) -> String {
    let replaced = RE_CORRECT_STREAM_NAME.replace_all(&stream_name, "_");

    // Check if any replacements were made
    match replaced {
        Cow::Borrowed(_) => {
            // No replacements were made - check if lowercasing is needed
            if crate::get_config().common.format_stream_name_to_lower
                && stream_name.chars().any(|c| c.is_ascii_uppercase())
            {
                // Has uppercase letters - must allocate and lowercase
                let mut owned = stream_name;
                owned.make_ascii_lowercase();
                owned
            } else {
                // No changes needed - return original string without reallocation
                stream_name
            }
        }
        Cow::Owned(mut owned) => {
            // Replacements were made - check if lowercasing is needed
            if crate::get_config().common.format_stream_name_to_lower
                && owned.chars().any(|c| c.is_ascii_uppercase())
            {
                // Has uppercase letters - lowercase the already-allocated string
                owned.make_ascii_lowercase();
            }
            owned
        }
    }
}

/// match a source is a needed file or not, return true if needed
pub fn filter_source_by_partition_key(source: &str, filters: &[(String, Vec<String>)]) -> bool {
    !filters.iter().any(|(k, v)| {
        let field = format_partition_key(&format!("{k}="));
        find(source, &format!("/{field}"))
            && !v.iter().any(|v| {
                let value = format_partition_key(&format!("{k}={v}"));
                find(source, &format!("/{value}/"))
            })
    })
}

/// compare two schemas, return true if they are equal
pub fn schema_eq(a: &Schema, b: &Schema) -> bool {
    // fast path
    if a.fields.eq(&b.fields) {
        return true;
    }
    // slow path, get all the filelds and sort them by name
    let mut a_fields = a.fields.iter().collect::<Vec<_>>();
    let mut b_fields = b.fields.iter().collect::<Vec<_>>();
    a_fields.sort_by(|a, b| a.name().cmp(b.name()));
    b_fields.sort_by(|a, b| a.name().cmp(b.name()));
    a_fields.eq(&b_fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_seen_schema_ignores_nulls_and_later_type_drift() {
        let first = serde_json::json!({"a": null, "b": "7"});
        let second = serde_json::json!({"a": 7, "b": 7});
        let third = serde_json::json!({"a": "later", "b": false});
        let records = [first, second, third];
        let schema = infer_json_schema_from_map_first_seen(
            "ordered",
            StreamType::Logs,
            records.iter().map(|value| value.as_object().unwrap()),
        )
        .unwrap();

        assert_eq!(
            schema.field_with_name("a").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("b").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_format_stream_name_no_changes() {
        // Test case: stream name that doesn't need any changes
        // This should return the original string without reallocation

        // Create a string with a known capacity to help verify the optimization
        let mut original = String::with_capacity(100);
        original.push_str("valid_stream_name_123");
        let original_capacity = original.capacity();
        let original_len = original.len();

        // Call the function - when no changes are needed, it should return the same String
        let result = format_stream_name(original);

        // Verify correctness
        assert_eq!(result, "valid_stream_name_123");

        // Verify that the result has the same capacity (optimization check)
        // When the optimization works, the function returns the original String,
        // so the capacity should be preserved
        // Note: This is a heuristic - if a new String was allocated, it might have
        // a different capacity. When the optimization works, we return the original
        // String which preserves its capacity.
        assert_eq!(
            result.capacity(),
            original_capacity,
            "Optimization check: When no changes are needed, format_stream_name should return the original String (same capacity = no reallocation)"
        );
        assert_eq!(result.len(), original_len);

        // Additional check: verify the string content matches exactly
        assert_eq!(result.as_str(), "valid_stream_name_123");
    }

    #[test]
    fn test_format_stream_name_with_replacements() {
        // Test case: stream name that needs character replacements
        let original = "stream-name with spaces!".to_string();
        let result = format_stream_name(original);

        // Should replace invalid characters with underscores
        assert_eq!(result, "stream_name_with_spaces_");
    }

    #[test]
    fn test_format_stream_name_with_lowercasing() {
        // This test depends on the config setting format_stream_name_to_lower
        // We'll test both scenarios
        let original_upper = "ValidStreamName".to_string();
        let result = format_stream_name(original_upper.clone());

        // The result depends on config, but should be consistent
        let cfg = crate::get_config();
        if cfg.common.format_stream_name_to_lower {
            assert_eq!(result, "validstreamname");
        } else {
            // If no lowercasing config, should return original (no replacements needed)
            assert_eq!(result, original_upper);
        }
    }

    #[test]
    fn test_format_stream_name_combined() {
        // Test case: stream name that needs both replacements and potentially lowercasing
        let original = "My-Stream Name!".to_string();
        let result = format_stream_name(original);

        // Should replace invalid characters
        assert!(result.contains("_"));
        assert!(!result.contains("-"));
        assert!(!result.contains(" "));
        assert!(!result.contains("!"));

        let cfg = crate::get_config();
        if cfg.common.format_stream_name_to_lower {
            assert_eq!(result, "my_stream_name_");
        } else {
            assert_eq!(result, "My_Stream_Name_");
        }
    }

    #[test]
    fn test_matches_by_partition_key_with_str() {
        let path = "files/default/logs/gke-fluentbit/2023/04/14/08/kuberneteshost=gke-dev1/kubernetesnamespacename=ziox-dev/7052558621820981249.parquet";
        let filters = vec![
            (vec![], true),
            (
                vec![("kuberneteshost".to_string(), vec!["gke-dev1".to_string()])],
                true,
            ),
            (
                vec![("kuberneteshost".to_string(), vec!["gke-dev2".to_string()])],
                false,
            ),
            (
                vec![(
                    "kuberneteshost".to_string(),
                    vec!["gke-dev1".to_string(), "gke-dev2".to_string()],
                )],
                true,
            ),
            (
                vec![("some_other_key".to_string(), vec!["no-matter".to_string()])],
                true,
            ),
            (
                vec![
                    ("kuberneteshost".to_string(), vec!["gke-dev1".to_string()]),
                    (
                        "kubernetesnamespacename".to_string(),
                        vec!["ziox-dev".to_string()],
                    ),
                ],
                true,
            ),
            (
                vec![
                    ("kuberneteshost".to_string(), vec!["gke-dev1".to_string()]),
                    (
                        "kubernetesnamespacename".to_string(),
                        vec!["abcdefg".to_string()],
                    ),
                ],
                false,
            ),
            (
                vec![
                    ("kuberneteshost".to_string(), vec!["gke-dev2".to_string()]),
                    (
                        "kubernetesnamespacename".to_string(),
                        vec!["ziox-dev".to_string()],
                    ),
                ],
                false,
            ),
            (
                vec![
                    ("kuberneteshost".to_string(), vec!["gke-dev2".to_string()]),
                    (
                        "kubernetesnamespacename".to_string(),
                        vec!["abcdefg".to_string()],
                    ),
                ],
                false,
            ),
            (
                vec![
                    (
                        "kuberneteshost".to_string(),
                        vec!["gke-dev1".to_string(), "gke-dev2".to_string()],
                    ),
                    (
                        "kubernetesnamespacename".to_string(),
                        vec!["ziox-dev".to_string()],
                    ),
                ],
                true,
            ),
            (
                vec![
                    (
                        "kuberneteshost".to_string(),
                        vec!["gke-dev1".to_string(), "gke-dev2".to_string()],
                    ),
                    (
                        "kubernetesnamespacename".to_string(),
                        vec!["abcdefg".to_string()],
                    ),
                ],
                false,
            ),
            (
                vec![
                    (
                        "kuberneteshost".to_string(),
                        vec!["gke-dev1".to_string(), "gke-dev2".to_string()],
                    ),
                    ("some_other_key".to_string(), vec!["no-matter".to_string()]),
                ],
                true,
            ),
        ];
        for (filter, expected) in filters {
            assert_eq!(filter_source_by_partition_key(path, &filter), expected);
        }
    }

    #[test]
    fn test_schema_eq() {
        let schema1 = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let schema2 = Schema::new(vec![
            Field::new("b", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
        ]);
        assert!(schema_eq(&schema1, &schema2));
    }

    #[test]
    fn test_schema_eq_with_metadata() {
        let schema1 = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());
        let schema2 = Schema::new_with_metadata(
            vec![
                Field::new("b", DataType::Int64, true),
                Field::new("a", DataType::Int64, true),
            ],
            metadata,
        );
        assert!(schema_eq(&schema1, &schema2));
    }

    #[test]
    fn test_schema_eq_with_different_order() {
        let schema1 = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let schema2 = Schema::new(vec![
            Field::new("b", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
        ]);
        assert!(schema_eq(&schema1, &schema2));
    }

    #[test]
    fn test_schema_eq_with_different_data_type() {
        let schema1 = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let schema2 = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]);
        assert!(!schema_eq(&schema1, &schema2));
    }

    #[test]
    fn test_format_partition_key_alphanumeric() {
        assert_eq!(format_partition_key("abc123"), "abc123");
        assert_eq!(format_partition_key("hello-world"), "hello-world");
        assert_eq!(format_partition_key("key=value"), "key=value");
        assert_eq!(format_partition_key("under_score"), "under_score");
    }

    #[test]
    fn test_format_partition_key_strips_special_chars() {
        assert_eq!(format_partition_key("hello world"), "helloworld");
        assert_eq!(format_partition_key("a/b/c"), "abc");
        assert_eq!(format_partition_key("foo:bar"), "foobar");
        assert_eq!(format_partition_key("a.b.c"), "abc");
        assert_eq!(format_partition_key(""), "");
    }

    #[test]
    fn test_format_partition_key_truncates_at_max_length() {
        let long = "a".repeat(300);
        let result = format_partition_key(&long);
        // loop checks > MAX_PARTITION_KEY_LENGTH before push, so max result is MAX+1
        assert!(result.len() <= super::MAX_PARTITION_KEY_LENGTH + 1);
        assert!(result.len() < 200);
    }

    #[test]
    fn test_infer_json_schema_from_values_non_object_returns_error() {
        use serde_json::Value;
        // Passing a non-Object value (Array) should hit the `_` branch → error
        let vals = vec![Value::Array(vec![Value::from(1), Value::from(2)])];
        let result = infer_json_schema_from_values("test_stream", StreamType::Logs, vals.iter());
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("non-object"), "unexpected error: {msg}");
    }

    #[test]
    fn test_infer_json_schema_from_values_object_with_array_field_returns_error() {
        use serde_json::{Map, Value};
        // An object containing an array field hits `_` in infer_json_schema_from_object
        let mut obj = Map::new();
        obj.insert("items".to_string(), Value::Array(vec![Value::from(1)]));
        let vals = vec![Value::Object(obj)];
        let result = infer_json_schema_from_values("test_stream", StreamType::Logs, vals.iter());
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("non-basic type"), "unexpected error: {msg}");
    }

    #[test]
    fn test_filter_source_no_filters_returns_true() {
        assert!(filter_source_by_partition_key(
            "/org/logs/stream/year=2024/month=01/",
            &[]
        ));
    }

    #[test]
    fn test_filter_source_matching_value_returns_true() {
        let filters = vec![("year".to_string(), vec!["2024".to_string()])];
        assert!(filter_source_by_partition_key(
            "/org/logs/stream/year=2024/month=01/",
            &filters
        ));
    }

    #[test]
    fn test_filter_source_non_matching_value_returns_false() {
        let filters = vec![("year".to_string(), vec!["2023".to_string()])];
        assert!(!filter_source_by_partition_key(
            "/org/logs/stream/year=2024/month=01/",
            &filters
        ));
    }

    #[test]
    fn test_filter_source_key_absent_returns_true() {
        let filters = vec![("region".to_string(), vec!["us-east".to_string()])];
        assert!(filter_source_by_partition_key(
            "/org/logs/stream/year=2024/",
            &filters
        ));
    }

    // Differential tests against the inference before the number fast path
    // and the key-order cursor, kept verbatim as the reference.
    fn legacy_number_type(number: &serde_json::Number) -> Option<DataType> {
        if number.is_i64() {
            Some(DataType::Int64)
        } else if number.is_u64() {
            Some(DataType::UInt64)
        } else if number.is_f64() {
            Some(DataType::Float64)
        } else {
            None
        }
    }

    fn legacy_convert_data_type(
        fields: &mut FxIndexMap<String, Field>,
        key: &str,
        data_type: DataType,
    ) -> Result<(), ArrowError> {
        let Some(f) = fields.get(key) else {
            fields.insert(key.to_string(), Field::new(key, data_type, true));
            return Ok(());
        };
        let f_type = f.data_type();
        if f_type == &data_type {
            return Ok(());
        }
        match (f_type, &data_type) {
            (DataType::LargeUtf8, _) => {}
            (DataType::Utf8, DataType::LargeUtf8) => {
                fields.insert(key.to_string(), Field::new(key, data_type, true));
            }
            (DataType::Utf8, _) => {}
            (DataType::Float64, DataType::UInt64)
            | (DataType::Float64, DataType::Int64)
            | (DataType::Float64, DataType::Boolean) => {}
            (DataType::Int64, DataType::UInt64)
            | (DataType::Int64, DataType::Float64)
            | (DataType::Int64, DataType::Utf8)
            | (DataType::Int64, DataType::LargeUtf8) => {
                fields.insert(key.to_string(), Field::new(key, data_type, true));
            }
            (DataType::UInt64, DataType::Int64)
            | (DataType::UInt64, DataType::Boolean)
            | (DataType::Int64, DataType::Boolean) => {}
            (DataType::UInt64, DataType::Float64)
            | (DataType::UInt64, DataType::Utf8)
            | (DataType::UInt64, DataType::LargeUtf8) => {
                fields.insert(key.to_string(), Field::new(key, data_type, true));
            }
            (DataType::Float64, DataType::Utf8) | (DataType::Float64, DataType::LargeUtf8) => {
                fields.insert(key.to_string(), Field::new(key, data_type, true));
            }
            (DataType::Boolean, _) => {
                fields.insert(key.to_string(), Field::new(key, data_type, true));
            }
            _ => {
                return Err(ArrowError::SchemaError(format!(
                    "Cannot infer schema from conflicting types: {f_type:?} and {data_type:?}"
                )));
            }
        }
        Ok(())
    }

    fn legacy_infer(records: &[Map<String, Value>]) -> Result<Schema, ArrowError> {
        let mut fields: FxIndexMap<String, Field> = FxIndexMap::default();
        for record in records {
            for (key, value) in record {
                let data_type = match value {
                    Value::String(_) => DataType::Utf8,
                    Value::Number(v) => legacy_number_type(v).unwrap_or(DataType::Utf8),
                    Value::Bool(_) => DataType::Boolean,
                    Value::Null => continue,
                    _ => {
                        return Err(ArrowError::SchemaError(
                            "Cannot infer schema from non-basic type value".to_string(),
                        ));
                    }
                };
                legacy_convert_data_type(&mut fields, key, data_type)?;
            }
        }
        Ok(fix_schema(
            Schema::new(fields.into_values().collect::<Vec<_>>()),
            StreamType::Logs,
        ))
    }

    /// xorshift64*: deterministic, so a failure reproduces from the seed.
    struct TestRng(u64);

    impl TestRng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        fn digit(&mut self) -> char {
            char::from(b'0' + self.below(10) as u8)
        }
    }

    /// A valid JSON number text around every classification boundary: digit
    /// counts near 18/19/20 and near 300, optional fraction and exponent.
    fn random_json_number(rng: &mut TestRng) -> String {
        let mut text = String::new();
        if rng.below(3) == 0 {
            text.push('-');
        }
        let int_len = match rng.below(4) {
            0 => 1 + rng.below(3),
            1 => 16 + rng.below(6),
            2 => 1 + rng.below(30),
            _ => 280 + rng.below(40),
        };
        if int_len == 1 {
            text.push(rng.digit());
        } else {
            // JSON forbids leading zeros on multi-digit integers
            text.push(char::from(b'1' + rng.below(9) as u8));
            for _ in 1..int_len {
                text.push(rng.digit());
            }
        }
        if rng.below(2) == 0 {
            text.push('.');
            for _ in 0..1 + rng.below(20) {
                text.push(rng.digit());
            }
        }
        if rng.below(4) == 0 {
            text.push(if rng.below(2) == 0 { 'e' } else { 'E' });
            match rng.below(3) {
                0 => text.push('-'),
                1 => text.push('+'),
                _ => {}
            }
            text.push_str(&rng.below(400).to_string());
        }
        text
    }

    fn parse_number(text: &str) -> serde_json::Number {
        match serde_json::from_str::<Value>(text) {
            Ok(Value::Number(number)) => number,
            other => panic!("{text:?} is not a JSON number: {other:?}"),
        }
    }

    #[test]
    fn number_data_type_matches_parse_chain_on_boundaries() {
        let decimal_299 = format!("{}.5", "9".repeat(297));
        let decimal_300 = format!("{}.5", "9".repeat(298));
        let integer_310 = "1".repeat(310);
        let texts = [
            "0",
            "-0",
            "7",
            "0.0",
            "-0.0",
            "999999999999999999",
            "-999999999999999999",
            "1000000000000000000",
            "9223372036854775807",
            "9223372036854775808",
            "-9223372036854775808",
            "-9223372036854775809",
            "18446744073709551615",
            "18446744073709551616",
            "-18446744073709551616",
            "1e308",
            "1e309",
            "-1e309",
            "1E2",
            "1.5e-400",
            "1.7976931348623157e308",
            "1.7976931348623159e308",
            decimal_299.as_str(),
            decimal_300.as_str(),
            integer_310.as_str(),
        ];
        for text in texts {
            let number = parse_number(text);
            assert_eq!(
                number_data_type(&number),
                legacy_number_type(&number),
                "{text}"
            );
        }
        let constructed = [
            serde_json::Number::from(i64::MIN),
            serde_json::Number::from(i64::MAX),
            serde_json::Number::from(u64::MAX),
            serde_json::Number::from_f64(f64::MAX).unwrap(),
            serde_json::Number::from_f64(5e-324).unwrap(),
            serde_json::Number::from_f64(1e300).unwrap(),
            serde_json::Number::from_f64(-0.25).unwrap(),
        ];
        for number in constructed {
            assert_eq!(
                number_data_type(&number),
                legacy_number_type(&number),
                "{}",
                number.as_str()
            );
        }
    }

    #[test]
    fn number_data_type_matches_parse_chain_on_random_texts() {
        let mut rng = TestRng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..50_000 {
            let text = random_json_number(&mut rng);
            let number = parse_number(&text);
            assert_eq!(
                number_data_type(&number),
                legacy_number_type(&number),
                "{text}"
            );
        }
    }

    #[test]
    fn infer_from_map_matches_legacy_on_random_batches() {
        const KEYS: [&str; 12] = [
            "_timestamp",
            "a",
            "b",
            "c",
            "host",
            "level",
            "msg",
            "n",
            "x",
            "y",
            "z",
            "start_time",
        ];
        let mut rng = TestRng(0xD1B5_4A32_D192_ED03);
        for batch_no in 0..3_000 {
            let records = (0..1 + rng.below(40))
                .map(|_| {
                    let mut keys: Vec<&str> =
                        KEYS.iter().copied().filter(|_| rng.below(5) != 0).collect();
                    if rng.below(5) == 0 {
                        // rotate and swap so some records break the shared order
                        let len = keys.len();
                        if len > 1 {
                            keys.rotate_left(rng.below(len as u64) as usize);
                            keys.swap(0, len - 1);
                        }
                    }
                    keys.into_iter()
                        .map(|key| {
                            let value = match rng.below(100) {
                                0..=24 => Value::String(format!("s{}", rng.below(100))),
                                25..=49 => {
                                    Value::Number(parse_number(&rng.below(1000).to_string()))
                                }
                                50..=59 => {
                                    Value::Number(parse_number(&random_json_number(&mut rng)))
                                }
                                60..=69 => Value::Number(parse_number(&format!(
                                    "{}.{}",
                                    rng.below(100),
                                    rng.below(100)
                                ))),
                                70..=79 => Value::Bool(rng.below(2) == 0),
                                80..=98 => Value::Null,
                                _ => serde_json::json!([1]),
                            };
                            (key.to_string(), value)
                        })
                        .collect::<Map<String, Value>>()
                })
                .collect::<Vec<_>>();
            let new = infer_json_schema_from_map("s", StreamType::Logs, records.iter());
            let old = legacy_infer(&records);
            match (new, old) {
                (Ok(new), Ok(old)) => assert_eq!(new, old, "batch {batch_no}"),
                (Err(new), Err(old)) => {
                    assert_eq!(new.to_string(), old.to_string(), "batch {batch_no}")
                }
                (new, old) => panic!("batch {batch_no}: new {new:?} vs old {old:?}"),
            }
        }
    }
}
