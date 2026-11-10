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
    common::cast::as_string_array,
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
        crate::datafusion::udf::STR_MATCH_UDF_NAME
    }

    fn aliases(&self) -> &[String] {
        static ALIASES: Lazy<Vec<String>> =
            Lazy::new(|| vec![crate::datafusion::udf::MATCH_FIELD_UDF_NAME.to_string()]);
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
        crate::datafusion::udf::STR_MATCH_UDF_IGNORE_CASE_NAME
    }

    fn aliases(&self) -> &[String] {
        static ALIASES: Lazy<Vec<String>> = Lazy::new(|| {
            vec![crate::datafusion::udf::MATCH_FIELD_IGNORE_CASE_UDF_NAME.to_string()]
        });
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

    // 1. cast both arguments to be aligned with the signature
    let ColumnarValue::Array(haystack) = &args[0] else {
        return Err(DataFusionError::SQL(
            Box::new(ParserError::ParserError(
                "Invalid argument types[haystack] to str_match function".to_string(),
            )),
            None,
        ));
    };
    let haystack = as_string_array(&haystack)?;
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

    // 2. perform the computation
    let array = haystack
        .iter()
        .map(|haystack| {
            haystack.map(|haystack| {
                if case_insensitive {
                    mem_finder
                        .find(haystack.to_lowercase().as_bytes())
                        .is_some()
                } else {
                    mem_finder.find(haystack.as_bytes()).is_some()
                }
            })
        })
        .collect::<BooleanArray>();

    // `Ok` because no error occurred during the calculation
    // `Arc` because arrays are immutable, thread-safe, trait objects.
    Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
}
