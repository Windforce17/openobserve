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

use datafusion::{
    arrow::{
        array::{ArrayRef, BooleanArray},
        datatypes::DataType,
    },
    common::cast::{as_large_string_array, as_string_array, as_string_view_array},
    error::{DataFusionError, Result},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    scalar::ScalarValue,
    sql::sqlparser::parser::ParserError,
};

/// Implementation of str_match
pub static STR_MATCH_UDF: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(StrMatchUdf::new()));

/// Implementation of str_match_ignore_case
pub static STR_MATCH_IGNORE_CASE_UDF: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(StrMatchIgnoreCaseUdf::new()));

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct StrMatchUdf {
    signature: Signature,
}

impl StrMatchUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for StrMatchUdf {
    fn name(&self) -> &str {
        super::STR_MATCH_UDF_NAME
    }

    fn aliases(&self) -> &[String] {
        static ALIASES: Lazy<Vec<String>> =
            Lazy::new(|| vec![super::MATCH_FIELD_UDF_NAME.to_string()]);
        &ALIASES
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        str_match_impl(&args.args, false)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct StrMatchIgnoreCaseUdf {
    signature: Signature,
}

impl StrMatchIgnoreCaseUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for StrMatchIgnoreCaseUdf {
    fn name(&self) -> &str {
        super::STR_MATCH_UDF_IGNORE_CASE_NAME
    }

    fn aliases(&self) -> &[String] {
        static ALIASES: Lazy<Vec<String>> =
            Lazy::new(|| vec![super::MATCH_FIELD_IGNORE_CASE_UDF_NAME.to_string()]);
        &ALIASES
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        str_match_impl(&args.args, true)
    }
}

fn str_match_impl(args: &[ColumnarValue], case_insensitive: bool) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::SQL(
            Box::new(ParserError::ParserError(
                "str_match UDF expects two string".to_string(),
            )),
            None,
        ));
    }

    let ColumnarValue::Array(haystack) = &args[0] else {
        return Err(DataFusionError::SQL(
            Box::new(ParserError::ParserError(
                "Invalid argument types[haystack] to str_match function".to_string(),
            )),
            None,
        ));
    };
    let ColumnarValue::Scalar(needle) = &args[1] else {
        return Err(DataFusionError::SQL(
            Box::new(ParserError::ParserError(
                "Invalid argument types[needle] to str_match function".to_string(),
            )),
            None,
        ));
    };
    let mut needle = match needle {
        ScalarValue::Utf8(v) => v,
        ScalarValue::Utf8View(v) => v,
        ScalarValue::LargeUtf8(v) => v,
        _ => {
            return Err(DataFusionError::SQL(
                Box::new(ParserError::ParserError(
                    "Invalid argument types[needle] to str_match function".to_string(),
                )),
                None,
            ));
        }
    }
    .as_ref()
    .ok_or_else(|| {
        DataFusionError::SQL(
            Box::new(ParserError::ParserError(
                "Invalid argument types[needle] to str_match function".to_string(),
            )),
            None,
        )
    })?
    .to_string();

    // pre-compute the needle
    if case_insensitive {
        needle.make_ascii_lowercase();
    };

    let mem_finder = memchr::memmem::Finder::new(needle.as_bytes());

    let array = match haystack.data_type() {
        DataType::Utf8 => match_strings(
            as_string_array(haystack)?.iter(),
            &mem_finder,
            case_insensitive,
        ),
        DataType::LargeUtf8 => match_strings(
            as_large_string_array(haystack)?.iter(),
            &mem_finder,
            case_insensitive,
        ),
        DataType::Utf8View => match_strings(
            as_string_view_array(haystack)?.iter(),
            &mem_finder,
            case_insensitive,
        ),
        data_type => {
            return Err(DataFusionError::Execution(format!(
                "str_match haystack must be Utf8, LargeUtf8 or Utf8View, got {data_type}"
            )));
        }
    };
    Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
}

fn match_strings<'a>(
    haystacks: impl Iterator<Item = Option<&'a str>>,
    finder: &memchr::memmem::Finder<'_>,
    case_insensitive: bool,
) -> BooleanArray {
    haystacks
        .map(|haystack| {
            haystack.map(|haystack| {
                if case_insensitive {
                    finder.find(haystack.to_lowercase().as_bytes()).is_some()
                } else {
                    finder.find(haystack.as_bytes()).is_some()
                }
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "str_match_legacy_test.rs"]
mod legacy;

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;
    use datafusion::{
        arrow::{
            array::Int64Array,
            datatypes::{Field, Schema},
            record_batch::RecordBatch,
        },
        datasource::MemTable,
        prelude::SessionContext,
    };

    use super::*;

    #[test]
    fn test_str_match_udf_name() {
        let udf = StrMatchUdf::new();
        assert_eq!(udf.name(), super::super::STR_MATCH_UDF_NAME);
    }

    #[test]
    fn test_str_match_udf_return_type_is_boolean() {
        let udf = StrMatchUdf::new();
        let rt = udf.return_type(&[]).unwrap();
        assert_eq!(rt, DataType::Boolean);
    }

    #[test]
    fn test_str_match_ignore_case_udf_name() {
        let udf = StrMatchIgnoreCaseUdf::new();
        assert_eq!(udf.name(), super::super::STR_MATCH_UDF_IGNORE_CASE_NAME);
    }

    #[test]
    fn test_str_match_ignore_case_return_type_is_boolean() {
        let udf = StrMatchIgnoreCaseUdf::new();
        let rt = udf.return_type(&[]).unwrap();
        assert_eq!(rt, DataType::Boolean);
    }

    #[test]
    fn test_str_match_udf_aliases_include_match_field() {
        let udf = StrMatchUdf::new();
        let aliases = udf.aliases();
        assert!(
            aliases
                .iter()
                .any(|a| a == super::super::MATCH_FIELD_UDF_NAME)
        );
    }

    #[tokio::test]
    async fn test_str_match_udf() {
        let sql = vec![
            "select * from t where str_match(log, 'es') and str_match_ignore_case(city, 'be')",
            "select * from t where str_match(log, 'es') and str_match_ignore_case(city, 'BE')",
            "select * from t where str_match(log, 'es') and str_match_ignore_case(city, '')",
            "select * from t where match_field(log, 'es') and match_field_ignore_case(city, 'be')",
            "select * from t where match_field(log, 'es') and match_field_ignore_case(city, 'BE')",
            "select * from t where match_field(log, 'es') and match_field_ignore_case(city, '')",
        ];

        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("log", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new("city", DataType::Utf8, false),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["this", "is", "a", "test"])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "New York",
                    "Pune",
                    "San Francisco",
                    "Beijing",
                ])),
            ],
        )
        .unwrap();

        // declare a new context. In spark API, this corresponds to a new spark
        // SQLsession
        let ctx = SessionContext::new();
        ctx.register_udf(STR_MATCH_UDF.clone());
        ctx.register_udf(STR_MATCH_IGNORE_CASE_UDF.clone());

        // declare a table in memory. In spark API, this corresponds to
        // createDataFrame(...).
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        for sql in sql {
            let df = ctx.sql(sql).await.unwrap();
            let result = df.collect().await.unwrap();
            let count = result.iter().map(|batch| batch.num_rows()).sum::<usize>();
            assert!(count > 0);
        }
    }

    #[test]
    fn test_str_match_impl_wrong_arg_count_errors() {
        let result = str_match_impl(&[], false);
        assert!(result.is_err());
    }

    #[test]
    fn test_str_match_impl_direct_match() {
        use datafusion::common::ScalarValue;
        let haystack = Arc::new(StringArray::from(vec!["hello world", "foo bar"]));
        let args = [
            ColumnarValue::Array(haystack),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("world".to_string()))),
        ];
        let result = str_match_impl(&args, false).unwrap();
        if let ColumnarValue::Array(out) = result {
            use arrow::array::{Array, BooleanArray};
            let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(out.value(0));
            assert!(!out.value(1));
        } else {
            panic!("expected array result");
        }
    }

    #[test]
    fn test_str_match_impl_case_insensitive() {
        use datafusion::common::ScalarValue;
        let haystack = Arc::new(StringArray::from(vec!["Hello World"]));
        let args = [
            ColumnarValue::Array(haystack),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("world".to_string()))),
        ];
        let result = str_match_impl(&args, true).unwrap();
        if let ColumnarValue::Array(out) = result {
            use arrow::array::{Array, BooleanArray};
            let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(out.value(0));
        } else {
            panic!("expected array result");
        }
    }
    fn invoke_real(args: Vec<ColumnarValue>, ci: bool) -> Result<ColumnarValue> {
        use datafusion::common::config::ConfigOptions;
        let udf = if ci {
            &*STR_MATCH_IGNORE_CASE_UDF
        } else {
            &*STR_MATCH_UDF
        };
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, a)| Arc::new(Field::new(format!("arg{i}"), a.data_type(), true)))
            .collect();
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 8,
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    #[test]
    fn test_str_match_native_string_types_and_slices() {
        use arrow::array::StringViewArray;
        let values = vec![
            Some("outside-before"),
            None,
            Some(""),
            Some("Hello WORLD"),
            Some("äÄÉéßİΣσ中文"),
            Some("123456789012"),
            Some("1234567890123"),
            Some("long repeated needle utf8 世界"),
            Some("outside-after"),
        ];
        let view: ArrayRef = Arc::new(StringViewArray::from(values.clone()));
        for haystack_type in [DataType::Utf8View, DataType::Utf8, DataType::LargeUtf8] {
            let full = arrow::compute::cast(&view, &haystack_type).unwrap();
            for sliced in [false, true] {
                let (haystack, inputs) = if sliced {
                    (full.slice(1, 7), &values[1..8])
                } else {
                    (full.clone(), &values[..])
                };
                for pattern in [
                    "",
                    "Hello",
                    "WORLD",
                    "Ä",
                    "ä",
                    "é",
                    "i\u{307}",
                    "世界",
                    "123456789012",
                    "1234567890123",
                    "missing",
                ] {
                    for ci in [false, true] {
                        let mut needle = pattern.to_owned();
                        if ci {
                            needle.make_ascii_lowercase();
                        }
                        let expected = BooleanArray::from(
                            inputs
                                .iter()
                                .map(|v| {
                                    v.map(|v| {
                                        if ci {
                                            v.to_lowercase().contains(&needle)
                                        } else {
                                            v.contains(&needle)
                                        }
                                    })
                                })
                                .collect::<Vec<_>>(),
                        );
                        for scalar in [
                            ScalarValue::Utf8(Some(pattern.into())),
                            ScalarValue::Utf8View(Some(pattern.into())),
                            ScalarValue::LargeUtf8(Some(pattern.into())),
                        ] {
                            let result = invoke_real(
                                vec![
                                    ColumnarValue::Array(haystack.clone()),
                                    ColumnarValue::Scalar(scalar),
                                ],
                                ci,
                            )
                            .unwrap();
                            let ColumnarValue::Array(result) = result else {
                                panic!("expected BooleanArray");
                            };
                            assert_eq!(
                                result.as_any().downcast_ref::<BooleanArray>().unwrap(),
                                &expected,
                                "{haystack_type:?} sliced={sliced} pattern={pattern:?} ci={ci}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_str_match_keeps_null_and_scalar_argument_errors() {
        use arrow::array::StringViewArray;
        let array: ArrayRef = Arc::new(StringViewArray::from(vec![None, Some("x")]));
        for ci in [false, true] {
            for scalar in [
                ScalarValue::Utf8(None),
                ScalarValue::Utf8View(None),
                ScalarValue::LargeUtf8(None),
            ] {
                assert!(
                    invoke_real(
                        vec![
                            ColumnarValue::Array(array.clone()),
                            ColumnarValue::Scalar(scalar)
                        ],
                        ci
                    )
                    .unwrap_err()
                    .to_string()
                    .contains("types[needle]")
                );
            }
            for scalar in [
                ScalarValue::Utf8(Some("x".into())),
                ScalarValue::Utf8View(Some("x".into())),
                ScalarValue::LargeUtf8(Some("x".into())),
                ScalarValue::Utf8(None),
            ] {
                assert!(
                    invoke_real(
                        vec![
                            ColumnarValue::Scalar(scalar),
                            ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".into())))
                        ],
                        ci
                    )
                    .unwrap_err()
                    .to_string()
                    .contains("types[haystack]")
                );
            }
            assert!(
                invoke_real(
                    vec![
                        ColumnarValue::Array(array.clone()),
                        ColumnarValue::Array(array.clone())
                    ],
                    ci
                )
                .unwrap_err()
                .to_string()
                .contains("types[needle]")
            );
        }
    }

    #[tokio::test]
    async fn test_str_match_sql_keeps_wire_cast_and_aliases() {
        use arrow::array::StringViewArray;
        use datafusion::physical_plan::displayable;
        let ctx = SessionContext::new();
        ctx.register_udf(STR_MATCH_UDF.clone());
        ctx.register_udf(STR_MATCH_IGNORE_CASE_UDF.clone());
        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8View, true),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringViewArray::from(vec![
                    None,
                    Some(""),
                    Some("Hello WORLD 世界"),
                    Some("hello world"),
                    Some("Ä ä İ"),
                ])),
                Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "native_view",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        for function in [
            "str_match",
            "str_match_ignore_case",
            "match_field",
            "match_field_ignore_case",
        ] {
            for needle in ["", "Hello", "WORLD", "Ä", "世界"] {
                let sql = format!(
                    "SELECT id FROM native_view WHERE {function}(body, '{needle}') ORDER BY id"
                );
                let df = ctx.sql(&sql).await.unwrap();
                let plan = df.create_physical_plan().await.unwrap();
                let display = displayable(plan.as_ref()).indent(true).to_string();
                assert!(
                    display.to_uppercase().contains("CAST("),
                    "wire-visible cast must remain: {display}"
                );
                let native = datafusion::physical_plan::collect(plan, ctx.task_ctx())
                    .await
                    .unwrap();
                let old = ctx.sql(&format!("SELECT id FROM native_view WHERE {function}(CAST(body AS VARCHAR), '{needle}') ORDER BY id")).await.unwrap().collect().await.unwrap();
                let ids = |batches: &[RecordBatch]| {
                    batches
                        .iter()
                        .flat_map(|b| {
                            b.column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values()
                                .to_vec()
                        })
                        .collect::<Vec<_>>()
                };
                assert_eq!(ids(&native), ids(&old), "{sql}");
            }
        }
    }

    #[test]
    fn test_str_match_native_index_expr_and_wire_roundtrip() {
        use arrow::array::StringViewArray;
        use datafusion::physical_plan::{ExecutionPlan, empty::EmptyExec, filter::FilterExec};
        use datafusion_proto::bytes::{
            physical_plan_from_bytes_with_extension_codec,
            physical_plan_to_bytes_with_extension_codec,
        };

        use crate::index::Condition;
        let codec = crate::datafusion::distributed_plan::codec::get_physical_extension_codec();
        let ctx = SessionContext::new();
        ctx.register_udf(STR_MATCH_UDF.clone());
        ctx.register_udf(STR_MATCH_IGNORE_CASE_UDF.clone());
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![
            None,
            Some(""),
            Some("HELLO world"),
            Some("hello world"),
            Some("long string with 世界"),
        ]));
        for data_type in [DataType::Utf8View, DataType::Utf8, DataType::LargeUtf8] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "body",
                data_type.clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![arrow::compute::cast(&view, &data_type).unwrap()],
            )
            .unwrap();
            for ci in [false, true] {
                let expression = Condition::StrMatch("body".into(), "HELLO".into(), !ci)
                    .to_physical_expr(&schema, &[])
                    .unwrap();
                assert!(!expression.to_string().to_uppercase().contains("CAST("));
                let expected = expression
                    .evaluate(&batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap();
                let plan: Arc<dyn ExecutionPlan> = Arc::new(
                    FilterExec::try_new(expression, Arc::new(EmptyExec::new(schema.clone())))
                        .unwrap(),
                );
                let bytes = physical_plan_to_bytes_with_extension_codec(plan, &codec).unwrap();
                let decoded =
                    physical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &codec)
                        .unwrap();
                let decoded = decoded.downcast_ref::<FilterExec>().unwrap();
                let actual = decoded
                    .predicate()
                    .evaluate(&batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap();
                assert_eq!(actual.to_data(), expected.to_data());
            }
        }
        let schema = Schema::new(vec![Field::new("body", DataType::Int64, false)]);
        assert!(
            Condition::StrMatch("body".into(), "1".into(), true)
                .to_physical_expr(&schema, &[])
                .is_err()
        );
        assert!(
            Condition::StrMatch("missing".into(), "1".into(), true)
                .to_physical_expr(&schema, &[])
                .is_err()
        );
    }
    #[tokio::test]
    async fn test_str_match_old_new_wire_plans_are_identical_and_cross_execute() {
        use arrow::array::StringViewArray;
        use datafusion::physical_plan::{ExecutionPlan, filter::FilterExec};
        use datafusion_proto::bytes::{
            physical_plan_from_bytes_with_extension_codec,
            physical_plan_to_bytes_with_extension_codec,
        };

        use crate::datafusion::table_provider::empty_table::NewEmptyTable;

        fn predicate(
            plan: &Arc<dyn ExecutionPlan>,
        ) -> Arc<dyn datafusion::physical_expr::PhysicalExpr> {
            if let Some(filter) = plan.downcast_ref::<FilterExec>() {
                return filter.predicate().clone();
            }
            assert_eq!(plan.children().len(), 1, "expected unary SQL filter plan");
            predicate(plan.children()[0])
        }

        assert_eq!(STR_MATCH_UDF.signature(), legacy::STR_MATCH_UDF.signature());
        assert_eq!(
            STR_MATCH_IGNORE_CASE_UDF.signature(),
            legacy::STR_MATCH_IGNORE_CASE_UDF.signature()
        );
        let codec = crate::datafusion::distributed_plan::codec::get_physical_extension_codec();
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![
            None,
            Some(""),
            Some("Hello WORLD 世界"),
            Some("hello world"),
            Some("Ä ä İ"),
            Some("1234567890123"),
        ]));
        for ty in [DataType::Utf8View, DataType::Utf8, DataType::LargeUtf8] {
            let schema = Arc::new(Schema::new(vec![Field::new("body", ty.clone(), true)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![arrow::compute::cast(&view, &ty).unwrap()],
            )
            .unwrap();
            let contexts = [SessionContext::new(), SessionContext::new()];
            contexts[0].register_udf(legacy::STR_MATCH_UDF.clone());
            contexts[0].register_udf(legacy::STR_MATCH_IGNORE_CASE_UDF.clone());
            contexts[1].register_udf(STR_MATCH_UDF.clone());
            contexts[1].register_udf(STR_MATCH_IGNORE_CASE_UDF.clone());
            for ctx in &contexts {
                ctx.register_table("wire", Arc::new(NewEmptyTable::new("wire", schema.clone())))
                    .unwrap();
            }
            for name in [
                "str_match",
                "str_match_ignore_case",
                "match_field",
                "match_field_ignore_case",
            ] {
                for needle in ["", "Hello", "WORLD", "Ä", "世界", "missing"] {
                    let mut encoded = Vec::new();
                    let mut expected = None;
                    for ctx in &contexts {
                        let plan = ctx
                            .sql(&format!(
                                "SELECT * FROM wire WHERE {name}(body, '{needle}')"
                            ))
                            .await
                            .unwrap()
                            .create_physical_plan()
                            .await
                            .unwrap();
                        let expression = predicate(&plan);
                        if ty != DataType::Utf8 {
                            assert!(expression.to_string().to_uppercase().contains("CAST("));
                        }
                        let result = expression
                            .evaluate(&batch)
                            .unwrap()
                            .into_array(batch.num_rows())
                            .unwrap();
                        if let Some(ref previous) = expected {
                            assert_eq!(&result.to_data(), previous);
                        } else {
                            expected = Some(result.to_data());
                        }
                        encoded.push(
                            physical_plan_to_bytes_with_extension_codec(plan, &codec).unwrap(),
                        );
                    }
                    assert_eq!(encoded[0], encoded[1], "{ty:?} {name} {needle:?}");
                    for bytes in &encoded {
                        for ctx in &contexts {
                            let decoded = physical_plan_from_bytes_with_extension_codec(
                                bytes,
                                &ctx.task_ctx(),
                                &codec,
                            )
                            .unwrap();
                            let result = predicate(&decoded)
                                .evaluate(&batch)
                                .unwrap()
                                .into_array(batch.num_rows())
                                .unwrap();
                            assert_eq!(Some(result.to_data()), expected);
                        }
                    }
                }
            }
        }
    }
}
