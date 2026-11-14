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

use std::sync::{Arc, LazyLock as Lazy};

use arrow::array::{Array, ArrayRef, GenericStringBuilder, ListBuilder, OffsetSizeTrait};
use arrow_schema::Field;
use datafusion::{
    arrow::datatypes::DataType::{self, *},
    common::{ScalarValue, exec_err},
    error::{DataFusionError, Result},
    logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

use super::regexp_udf::{StringArrayRef, cached_regex};
use crate::datafusion::udf::REGEX_MATCHES_UDF_NAME;

/// Implementation of regexp_matches
pub static REGEX_MATCHES_UDF: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(RegexpMatchesFunc::default()));

/// # `re_matches` User-Defined Function (UDF)
///
/// This UDF extracts all substrings from a string column that match a given regular expression
/// pattern. It is designed to work with the DataFusion query engine and supports both scalar and
/// array inputs.
///
/// ## Purpose
///
/// The `re_matches` UDF allows users to extract all matches of a regular expression from a string
/// column.
///
/// ## Function Signature
///
/// The UDF supports the following signatures:
///
/// - `re_matches(string_column, regex_pattern)`
///
/// - `string_column`: The input column containing strings to search.
/// - `regex_pattern`: The regular expression pattern to match.
///
/// ## Return Type
///
/// - The function returns a `List` of strings for each input row. If no matches are found, an empty
///   list is returned.
/// - If the input string or pattern is null, the function returns null for that row.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct RegexpMatchesFunc {
    signature: Signature,
}

impl Default for RegexpMatchesFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpMatchesFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Utf8, Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpMatchesFunc {
    fn name(&self) -> &str {
        REGEX_MATCHES_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(match &arg_types[0] {
            Null => Null,
            Utf8View => List(Arc::new(Field::new("item", Utf8, true))),
            other => List(Arc::new(Field::new("item", other.clone(), true))),
        })
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        regexp_matches_columnar(&args.args)
    }
}

fn regexp_matches_columnar(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let len = args.iter().fold(None, |acc, arg| match arg {
        ColumnarValue::Scalar(_) => acc,
        ColumnarValue::Array(array) => Some(array.len()),
    });
    let values = args[0].clone().into_array(len.unwrap_or(1))?;
    let result = match values.data_type() {
        Utf8 | Utf8View => regexp_matches_impl::<i32>(&values, &args[1])?,
        LargeUtf8 => regexp_matches_impl::<i64>(&values, &args[1])?,
        other => return exec_err!("unsupported data type {other:?} for function regexp_matches"),
    };
    if len.is_none() {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

pub fn regexp_matches<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    regexp_matches_impl::<T>(&args[0], &ColumnarValue::Array(Arc::clone(&args[1])))
}

fn regexp_matches_impl<T: OffsetSizeTrait>(
    array: &ArrayRef,
    pattern: &ColumnarValue,
) -> Result<ArrayRef> {
    let values = StringArrayRef::new(array)?;
    let pattern_array = match pattern {
        ColumnarValue::Array(array) => Some(StringArrayRef::new(array)?),
        _ => None,
    };
    let (constant, single_pattern) = match pattern {
        ColumnarValue::Scalar(ScalarValue::Utf8(value))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(value))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(value)) => {
            (Some(value.as_deref()), array.len() == 1)
        }
        ColumnarValue::Array(array) if array.len() == 1 => {
            (Some(Some(pattern_array.as_ref().unwrap().value(0))), true)
        }
        ColumnarValue::Array(_) => (None, false),
        other => return exec_err!("unsupported pattern {other:?} for function regexp_matches"),
    };
    let compile = |pattern: &str| {
        cached_regex(pattern)
            .map_err(|e| DataFusionError::Execution(format!("Invalid regex pattern: {e}")))
    };
    // Preserve the baseline's single-element behavior, including eager validation
    // and the empty-string payload of a null scalar pattern. For larger batches,
    // compile lazily: all-null or empty inputs must not validate an unused pattern.
    let mut scalar_regex = if single_pattern {
        Some(compile(constant.flatten().unwrap_or(""))?)
    } else {
        None
    };

    let mut list_builder = ListBuilder::new(GenericStringBuilder::<T>::new());

    for i in 0..array.len() {
        let row_pattern = match constant {
            Some(value) => value,
            None => {
                let patterns = pattern_array.as_ref().unwrap();
                (!patterns.is_null(i)).then(|| patterns.value(i))
            }
        };
        if values.is_null(i) || (!single_pattern && row_pattern.is_none()) {
            list_builder.append(false);
            continue;
        }

        let value = values.value(i);
        let re = if constant.is_some() {
            if scalar_regex.is_none() {
                scalar_regex = Some(compile(row_pattern.unwrap_or(""))?);
            }
            Arc::clone(scalar_regex.as_ref().unwrap())
        } else {
            compile(row_pattern.unwrap())?
        };

        let mut has_match = false;

        for cap in re.captures_iter(value) {
            // EXPLANATION OF REGEX CAPTURE GROUPS:
            // In a regex pattern like: '"value":(("([^"]+)")|(\d+))'
            // The groups are numbered from outside to inside:
            // Group 0: Always the entire match (e.g., "value":"hello")
            // Group 1: First explicit group (e.g., "hello")
            // Group 2+: Nested groups (e.g., hello - without quotes)
            //
            // Example:
            // Pattern: '"value":(("([^"]+)")|(\d+))'
            // Input:   '{"value":"hello"}'
            // Group 0: "value":"hello"    (full match)
            // Group 1: "hello"            (outer capture)
            // Group 2: "hello"            (middle capture)
            // Group 3: hello              (innermost capture - what we want!)

            // Try innermost capture group first, then work outwards
            let matched_value = (2..=cap.len())  // Start from 2 because: Group 0 is full match, Group 1 is first explicit group
                .rev()  // Reverse the range to start from last group (innermost) to first group
                .find_map(|i| cap.get(i))  // Try to get each group, returns None if group didn't match
                .or_else(|| cap.get(1))    // If no inner groups matched, try the first explicit group
                .or_else(|| cap.get(0))    // If no groups matched at all, use the entire match
                .map(|m| m.as_str()); // Convert the match to a string slice

            if let Some(matched) = matched_value {
                // Clean up the matched value:
                // If the value is wrapped in quotes (like "hello"), remove them
                // If it's not (like 123), keep as is
                let cleaned = if matched.starts_with('"') && matched.ends_with('"') {
                    // Remove the surrounding quotes
                    &matched[1..matched.len() - 1]
                } else {
                    matched
                };
                list_builder.values().append_value(cleaned);
                has_match = true;
            }
        }

        list_builder.append(has_match);
    }

    Ok(Arc::new(list_builder.finish()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{RecordBatch, StringArray};
    use arrow_schema::Schema;
    use datafusion::{
        assert_batches_eq, common::cast::as_generic_string_array, datasource::MemTable,
        logical_expr::ScalarUDFImpl, prelude::SessionContext,
    };
    use regex::Regex;

    use super::*;

    fn legacy_regexp_matches<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
        let values = as_generic_string_array::<T>(&args[0])?;
        let regex = as_generic_string_array::<T>(&args[1])?;

        // Scalar is a single regex pattern
        // Check if the regex pattern is a scalar
        let is_scalar_pattern = regex.len() == 1;

        // Precompile the regex if it's scalar
        let scalar_regex =
            if is_scalar_pattern {
                Some(Regex::new(regex.value(0)).map_err(|e| {
                    DataFusionError::Execution(format!("Invalid regex pattern: {e}"))
                })?)
            } else {
                None
            };

        let mut list_builder = ListBuilder::new(GenericStringBuilder::<T>::new());

        for i in 0..values.len() {
            if values.is_null(i) || (!is_scalar_pattern && regex.is_null(i)) {
                // Append NULL for this row
                list_builder.append(false);
                continue;
            }

            let value = values.value(i);

            let re = if is_scalar_pattern {
                scalar_regex.clone().unwrap()
            } else {
                Regex::new(regex.value(i)).map_err(|e| {
                    DataFusionError::Execution(format!("Invalid regex pattern: {e}"))
                })?
            };

            let mut has_match = false;

            for cap in re.captures_iter(value) {
                // EXPLANATION OF REGEX CAPTURE GROUPS:
                // In a regex pattern like: '"value":(("([^"]+)")|(\d+))'
                // The groups are numbered from outside to inside:
                // Group 0: Always the entire match (e.g., "value":"hello")
                // Group 1: First explicit group (e.g., "hello")
                // Group 2+: Nested groups (e.g., hello - without quotes)
                //
                // Example:
                // Pattern: '"value":(("([^"]+)")|(\d+))'
                // Input:   '{"value":"hello"}'
                // Group 0: "value":"hello"    (full match)
                // Group 1: "hello"            (outer capture)
                // Group 2: "hello"            (middle capture)
                // Group 3: hello              (innermost capture - what we want!)

                // Try innermost capture group first, then work outwards
                let matched_value = (2..=cap.len())  // Start from 2 because: Group 0 is full match, Group 1 is first explicit group
                    .rev()  // Reverse the range to start from last group (innermost) to first group
                    .find_map(|i| cap.get(i))  // Try to get each group, returns None if group didn't match
                    .or_else(|| cap.get(1))    // If no inner groups matched, try the first explicit group
                    .or_else(|| cap.get(0))    // If no groups matched at all, use the entire match
                    .map(|m| m.as_str()); // Convert the match to a string slice

                if let Some(matched) = matched_value {
                    // Clean up the matched value:
                    // If the value is wrapped in quotes (like "hello"), remove them
                    // If it's not (like 123), keep as is
                    let cleaned = if matched.starts_with('"') && matched.ends_with('"') {
                        // Remove the surrounding quotes
                        &matched[1..matched.len() - 1]
                    } else {
                        matched
                    };
                    list_builder.values().append_value(cleaned);
                    has_match = true;
                }
            }

            list_builder.append(has_match);
        }

        Ok(Arc::new(list_builder.finish()))
    }

    #[test]
    fn test_regexp_matches_func_name() {
        let func = RegexpMatchesFunc::new();
        assert_eq!(func.name(), REGEX_MATCHES_UDF_NAME);
    }

    #[test]
    fn test_regexp_matches_direct_finds_matches() {
        let values = StringArray::from(vec!["abc123def456"]);
        let pattern = StringArray::from(vec!["(\\d+)"]);
        let args: Vec<ArrayRef> = vec![Arc::new(values), Arc::new(pattern)];
        let result = regexp_matches::<i32>(&args).unwrap();
        use arrow::array::{Array, ListArray};
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(!list.is_null(0));
    }

    #[test]
    fn test_regexp_matches_direct_no_match_returns_null_row() {
        let values = StringArray::from(vec!["no digits here"]);
        let pattern = StringArray::from(vec!["(\\d+)"]);
        let args: Vec<ArrayRef> = vec![Arc::new(values), Arc::new(pattern)];
        let result = regexp_matches::<i32>(&args).unwrap();
        use arrow::array::{Array, ListArray};
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.is_null(0));
    }

    #[tokio::test]
    async fn test_re_matches_extract_all_numbers_as_scalar() {
        let sql = "SELECT re_matches(log, '(\\d+)') AS matches FROM t";

        // Define schema
        let schema = Arc::new(Schema::new(vec![Field::new("log", DataType::Utf8, false)]));

        // Define data
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "abc123def456",
                "no match here",
                "789ghi123",
            ]))],
        )
        .unwrap();

        // Create a session context
        let ctx = SessionContext::new();
        ctx.register_udf(REGEX_MATCHES_UDF.clone());

        // Register the table
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // Execute SQL
        let df = ctx.sql(sql).await.unwrap();
        let results = df.collect().await.unwrap();

        // Expected output
        let expected = [
            "+------------+",
            "| matches    |",
            "+------------+",
            "| [123, 456] |",
            "|            |",
            "| [789, 123] |",
            "+------------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_re_matches_regex_as_array() {
        let sql = "SELECT re_matches(log, regex) AS matches FROM t";

        // Define schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("log", DataType::Utf8, true), // Allow NULLs in `log`
            Field::new("regex", DataType::Utf8, true), // Allow NULLs in `regex`
        ]));

        // Define data
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("abc123def456"),  // Normal case: numbers in the string
                    Some("no match here"), // No match case
                    Some("789ghi123"),     // Multiple matches
                    None,                  // NULL log
                    Some(""),              // Empty string
                ])),
                Arc::new(StringArray::from(vec![
                    Some("\\d+"),     // Match any digits
                    Some("no match"), // Match literal "no match"
                    Some("ghi"),      // Match substring "ghi"
                    Some("\\w+"),     // Match any word (NULL log should return NULL)
                    None,             // NULL regex
                ])),
            ],
        )
        .unwrap();

        // Create a session context
        let ctx = SessionContext::new();
        ctx.register_udf(REGEX_MATCHES_UDF.clone());

        // Register the table
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // Execute SQL
        let df = ctx.sql(sql).await.unwrap();
        let results = df.collect().await.unwrap();

        // Expected output
        let expected = [
            "+------------+",
            "| matches    |",
            "+------------+",
            "| [123, 456] |", // Matches "\\d+" for "abc123def456"
            "| [no match] |", // Matches "no match" for "no match here"
            "| [ghi]      |", // Matches "ghi" for "789ghi123"
            "|            |", // NULL log should return NULL
            "|            |", // Empty string, no match
            "+------------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_re_matches_extract_json_values() {
        // Use a named capture group and reference it in the SQL
        let sql = "SELECT re_matches(json_data, '\"bt\":\"([^\"]+)\"') AS matches FROM t";

        // Define schema
        let schema = Arc::new(Schema::new(vec![Field::new(
            "json_data",
            DataType::Utf8,
            false,
        )]));

        // Define test data
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                r#"[{"bt":"login_benefit","cl":0},{"bt":"FDFS","cl":0},{"bt":"fdfsFirst","cl":0}]"#,
                r#"{"bt":"single_value"}"#,
                r#"{"other":"no_match"}"#,
                r#"[{"bt":"value1"},{"bt":"value2"}]"#,
            ]))],
        )
        .unwrap();

        // Create a session context
        let ctx = SessionContext::new();
        ctx.register_udf(REGEX_MATCHES_UDF.clone());

        // Register the table
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // Execute SQL
        let df = ctx.sql(sql).await.unwrap();
        let results = df.collect().await.unwrap();

        // Expected output
        let expected = [
            "+----------------------------------+",
            "| matches                          |",
            "+----------------------------------+",
            "| [login_benefit, FDFS, fdfsFirst] |",
            "| [single_value]                   |",
            "|                                  |",
            "| [value1, value2]                 |",
            "+----------------------------------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_re_matches_optional_capture_groups() {
        let sql = "SELECT re_matches(text, '\"value\":(\"([^\"]+)\"|\\d+)') AS matches FROM t";

        // Define schema
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));

        // Define test data with mixed formats
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                r#"{"value":"quoted"}"#,          // Value in quotes
                r#"{"value":123}"#,               // Numeric value without quotes
                r#"{"value":"abc","value":456}"#, // Mixed in same string
            ]))],
        )
        .unwrap();

        // Create a session context
        let ctx = SessionContext::new();
        ctx.register_udf(REGEX_MATCHES_UDF.clone());

        // Register the table
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // Execute SQL
        let df = ctx.sql(sql).await.unwrap();
        let results = df.collect().await.unwrap();

        // Expected output
        let expected = [
            "+------------+",
            "| matches    |",
            "+------------+",
            "| [quoted]   |", // Should capture the inner value
            "| [123]      |", // Should capture the number
            "| [abc, 456] |", // Should capture both values
            "+------------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    fn assert_matches_equal(actual: Result<ArrayRef>, expected: Result<ArrayRef>) {
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => assert_eq!(actual.to_data(), expected.to_data()),
            (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
            (actual, expected) => panic!("result mismatch: {actual:?} vs {expected:?}"),
        }
    }

    #[test]
    fn regex_matches_differential_scalar_patterns() {
        use arrow::array::{LargeStringArray, StringViewArray};
        let batches = vec![
            vec![],
            vec![None],
            vec![Some("")],
            vec![Some("abc42")],
            vec![None, None],
            vec![
                None,
                Some(""),
                Some("abc42def13"),
                Some("中文é🙂"),
                Some("\"quoted\""),
            ],
        ];
        for rows in batches {
            for pattern in [
                None,
                Some(""),
                Some("[0-9]+"),
                Some("["),
                Some(r"\:"),
                Some("中文|é|🙂"),
                Some(r#"("([^"]+)"|\d+)"#),
            ] {
                let arrays: Vec<ArrayRef> = vec![
                    Arc::new(StringArray::from(rows.clone())),
                    Arc::new(LargeStringArray::from(rows.clone())),
                    Arc::new(StringViewArray::from(rows.clone())),
                ];
                for (index, array) in arrays.into_iter().enumerate() {
                    let scalar = if index == 1 {
                        ScalarValue::LargeUtf8(pattern.map(str::to_owned))
                    } else {
                        ScalarValue::Utf8(pattern.map(str::to_owned))
                    };
                    let expanded = scalar.to_array_of_size(rows.len()).unwrap();
                    let oracle_values: ArrayRef = if index == 1 {
                        Arc::clone(&array)
                    } else {
                        Arc::new(StringArray::from(rows.clone()))
                    };
                    let old_args = [oracle_values, expanded];
                    let expected = if index == 1 {
                        legacy_regexp_matches::<i64>(&old_args)
                    } else {
                        legacy_regexp_matches::<i32>(&old_args)
                    };
                    let actual = regexp_matches_columnar(&[
                        ColumnarValue::Array(array),
                        ColumnarValue::Scalar(scalar),
                    ])
                    .and_then(|value| value.into_array(rows.len()));
                    assert_matches_equal(actual, expected);
                }
            }
        }
    }

    #[test]
    fn regex_matches_differential_array_patterns() {
        use arrow::array::{LargeStringArray, StringViewArray};
        let rows = vec![
            Some("abc123"),
            None,
            Some("中文é"),
            Some(""),
            Some("no match"),
        ];
        for patterns in [
            vec![
                Some("[0-9]+"),
                Some("["),
                Some("(中文)(é)"),
                None,
                Some("[0-9]+"),
            ],
            vec![Some("[")],
            vec![None],
            vec![Some("")],
            vec![Some("["), None, None, None, None],
        ] {
            let utf8: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(rows.clone())),
                Arc::new(StringArray::from(patterns.clone())),
            ];
            let large: Vec<ArrayRef> = vec![
                Arc::new(LargeStringArray::from(rows.clone())),
                Arc::new(LargeStringArray::from(patterns.clone())),
            ];
            let view: Vec<ArrayRef> = vec![
                Arc::new(StringViewArray::from(rows.clone())),
                Arc::new(StringViewArray::from(patterns)),
            ];
            assert_matches_equal(
                regexp_matches::<i32>(&utf8),
                legacy_regexp_matches::<i32>(&utf8),
            );
            assert_matches_equal(
                regexp_matches::<i64>(&large),
                legacy_regexp_matches::<i64>(&large),
            );
            assert_matches_equal(
                regexp_matches::<i32>(&view),
                legacy_regexp_matches::<i32>(&utf8),
            );
        }
    }

    #[test]
    fn regex_matches_scalar_null_and_invalid_compatibility() {
        for row in [None, Some(""), Some("abc42")] {
            for pattern in [None, Some(""), Some("[0-9]+"), Some("[")] {
                let args = [
                    ColumnarValue::Scalar(ScalarValue::Utf8(row.map(str::to_owned))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(pattern.map(str::to_owned))),
                ];
                let arrays: Vec<_> = args
                    .iter()
                    .map(|arg| arg.clone().into_array(1).unwrap())
                    .collect();
                let expected = legacy_regexp_matches::<i32>(&arrays);
                let actual = regexp_matches_columnar(&args).and_then(|value| value.into_array(1));
                assert_matches_equal(actual, expected);
            }
        }
    }

    #[test]
    fn regex_matches_constant_pattern_compiles_once_across_batches() {
        use super::super::regexp_udf::REGEX_COMPILATIONS;
        let before = REGEX_COMPILATIONS.get();
        for _ in 0..5 {
            let args = [
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    Some("extract777"),
                    None,
                    Some("extract42"),
                ]))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("extract([0-9]+)".into()))),
            ];
            regexp_matches_columnar(&args).unwrap();
        }
        assert_eq!(REGEX_COMPILATIONS.get() - before, 1);
    }

    #[test]
    #[ignore = "manual microbenchmark: 100 batches of 8192 rows"]
    fn regex_microbench_100_batches() {
        use std::{hint::black_box, time::Instant};

        use super::super::regexp_udf::{
            regex_match_expr_impl, tests::legacy_regex_match_expr_impl,
        };
        let values: ArrayRef =
            Arc::new(StringArray::from(vec!["log error42: request failed"; 8192]));
        let pattern = ScalarValue::Utf8(Some("err(or)?[0-9]+".into()));
        let args = [
            ColumnarValue::Array(Arc::clone(&values)),
            ColumnarValue::Scalar(pattern.clone()),
        ];
        for matches in [true, false] {
            let new = regex_match_expr_impl(matches);
            let old = legacy_regex_match_expr_impl(matches);
            new(&args).unwrap();
            for (label, implementation) in [("old", old), ("new", new)] {
                let start = Instant::now();
                for _ in 0..100 {
                    black_box(implementation(black_box(&args)).unwrap());
                }
                println!(
                    "regex_microbench match={matches} {label}: {:?}",
                    start.elapsed()
                );
            }
        }
        regexp_matches_columnar(&args).unwrap();
        let start = Instant::now();
        for _ in 0..100 {
            // The baseline UDF expands a scalar pattern for every batch.
            let old_args = [Arc::clone(&values), pattern.to_array_of_size(8192).unwrap()];
            black_box(legacy_regexp_matches::<i32>(black_box(&old_args)).unwrap());
        }
        println!("regex_microbench re_matches old: {:?}", start.elapsed());
        let start = Instant::now();
        for _ in 0..100 {
            black_box(regexp_matches_columnar(black_box(&args)).unwrap());
        }
        println!("regex_microbench re_matches new: {:?}", start.elapsed());
    }
}
