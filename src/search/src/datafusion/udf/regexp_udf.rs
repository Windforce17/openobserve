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

use arrow_schema::{Field, FieldRef, Fields};
use datafusion::{
    arrow::{
        array::{
            Array, ArrayData, ArrayRef, BooleanArray, LargeStringArray, StringArray,
            StringViewArray, StructArray, as_large_list_array, as_list_array,
        },
        datatypes::DataType,
    },
    common::cast::as_generic_string_array,
    error::{DataFusionError, Result},
    functions::regex::regexpmatch::regexp_match,
    logical_expr::{
        ReturnFieldArgs, ScalarFunctionArgs, ScalarFunctionImplementation, ScalarUDF,
        ScalarUDFImpl, Signature, TypeSignature::Exact, Volatility,
    },
    physical_plan::ColumnarValue,
    prelude::create_udf,
    scalar::ScalarValue,
};
use hashlink::LinkedHashMap;
use parking_lot::Mutex;

const REGEX_CACHE_CAPACITY: usize = 256;
static REGEX_CACHE: Lazy<Mutex<RegexCache>> = Lazy::new(|| Mutex::new(RegexCache::default()));

#[derive(Default)]
struct RegexCache {
    entries: LinkedHashMap<String, Arc<regex::Regex>>,
}

impl RegexCache {
    fn get_or_compile(
        &mut self,
        pattern: &str,
    ) -> std::result::Result<Arc<regex::Regex>, regex::Error> {
        if let Some(regex) = self.entries.to_back(pattern) {
            return Ok(Arc::clone(regex));
        }
        #[cfg(test)]
        REGEX_COMPILATIONS.with(|count| count.set(count.get().checked_add(1).unwrap()));
        let regex = Arc::new(regex::Regex::new(pattern)?);
        if self.entries.len() == REGEX_CACHE_CAPACITY {
            self.entries.pop_front();
        }
        self.entries.insert(pattern.to_owned(), Arc::clone(&regex));
        Ok(regex)
    }
}

#[cfg(test)]
thread_local! {
    pub(super) static REGEX_COMPILATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) fn cached_regex(pattern: &str) -> std::result::Result<Arc<regex::Regex>, regex::Error> {
    // Serializing cache misses also prevents concurrent batches from compiling the same key.
    // Matching happens after releasing this lock; the cache owns at most 256 compiled patterns.
    REGEX_CACHE.lock().get_or_compile(pattern)
}

pub(super) enum StringArrayRef<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> StringArrayRef<'a> {
    pub(super) fn new(array: &'a ArrayRef) -> Result<Self> {
        match array.data_type() {
            DataType::Utf8 => Ok(Self::Utf8(array.as_any().downcast_ref().unwrap())),
            DataType::LargeUtf8 => Ok(Self::LargeUtf8(array.as_any().downcast_ref().unwrap())),
            DataType::Utf8View => Ok(Self::Utf8View(array.as_any().downcast_ref().unwrap())),
            other => Err(DataFusionError::Execution(format!(
                "unsupported string data type {other:?}"
            ))),
        }
    }

    pub(super) fn value(&self, index: usize) -> &'a str {
        match self {
            Self::Utf8(array) => array.value(index),
            Self::LargeUtf8(array) => array.value(index),
            Self::Utf8View(array) => array.value(index),
        }
    }

    pub(super) fn is_null(&self, index: usize) -> bool {
        match self {
            Self::Utf8(array) => array.is_null(index),
            Self::LargeUtf8(array) => array.is_null(index),
            Self::Utf8View(array) => array.is_null(index),
        }
    }
}

/// Implementation of regexp_match
pub static REGEX_MATCH_UDF: Lazy<ScalarUDF> = Lazy::new(|| {
    create_udf(
        super::REGEX_MATCH_UDF_NAME,
        // takes two arguments: regex, pattern
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Stable,
        regex_match_expr_impl(true),
    )
});

/// Implementation of regexp_not_match
pub static REGEX_NOT_MATCH_UDF: Lazy<ScalarUDF> = Lazy::new(|| {
    create_udf(
        super::REGEX_NOT_MATCH_UDF_NAME,
        // takes two arguments: regex, pattern
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Stable,
        regex_match_expr_impl(false),
    )
});

/// Implementation of regexp_match_to_fields
pub static REGEXP_MATCH_TO_FIELDS_UDF: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(RegxpMatchToFields::new()));

/// Given a column containing string values and a single regex pattern,
/// `regex_match_expr` determines which values satisfy the pattern and which do
/// not.
///
/// If `matches` is true then this expression will filter values that do not
/// satisfy the regex (equivalent to `col ~= /pattern/`). If `matches` is
/// `false` then the expression will filter values that *do* match the regex,
/// which is equivalent to `col !~ /pattern/`.
///
/// This UDF is designed to support the regex operator that can be pushed down
/// via the InfluxRPC API.
pub fn regex_match_expr_impl(matches: bool) -> ScalarFunctionImplementation {
    // N.B., this function does not utilise the Arrow regexp compute
    // kernel because in order to act as a filter it needs to return a
    // boolean array of comparison results, not an array of strings as
    // the regex compute kernel does and it needs to implement the
    // regexp syntax for influxrpc.

    let func = move |args: &[ColumnarValue]| {
        assert_eq!(args.len(), 2); // only works over a single column and pattern at a time.

        let pattern = match &args[1] {
            // second arg was array (not constant)
            ColumnarValue::Array(_) => {
                return Err(DataFusionError::NotImplemented(format!(
                    "regex_match({matches}) with non scalar patterns not yet implemented"
                )));
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(pattern)) => pattern,
            ColumnarValue::Scalar(arg) => {
                return Err(DataFusionError::Plan(format!(
                    "Expected string pattern to regex_match({matches}), got: {arg:?}"
                )));
            }
        };

        let pattern = pattern.as_ref().ok_or_else(|| {
            DataFusionError::NotImplemented(
                "NULL patterns not supported in regex_match".to_string(),
            )
        })?;

        // Attempt to make the pattern compatible with what is accepted by
        // the golang regexp library which is different than Rust's regexp
        let pattern = clean_non_meta_escapes(pattern);

        let pattern = cached_regex(&pattern)
            .map_err(|e| DataFusionError::Plan(format!("error compiling regex pattern: {e}")))?;

        match &args[0] {
            ColumnarValue::Array(arr) => {
                let values = StringArrayRef::new(arr)?;
                let results = (0..arr.len())
                    .map(|index| {
                        (!values.is_null(index))
                            .then(|| pattern.is_match(values.value(index)) == matches)
                    })
                    .collect::<BooleanArray>();

                Ok(ColumnarValue::Array(Arc::new(results) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(row)) => {
                let res = row.as_ref().map(|v| pattern.is_match(v) == matches);
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(res)))
            }
            ColumnarValue::Scalar(v) => Err(DataFusionError::Plan(format!(
                "regex_match({matches}) expected first argument to be utf8, got ('{v}')"
            ))),
        }
    };

    Arc::new(func)
}

fn is_valid_character_after_escape(c: char) -> bool {
    // same list as https://docs.rs/regex-syntax/0.6.25/src/regex_syntax/ast/parse.rs.html#1445-1538
    match c {
        '0'..='7' => true,
        '8'..='9' => true,
        'x' | 'u' | 'U' => true,
        'p' | 'P' => true,
        'd' | 's' | 'w' | 'D' | 'S' | 'W' => true,
        _ => regex_syntax::is_meta_character(c),
    }
}

/// Removes all `/` patterns that the rust regex library would reject
/// and rewrites them to their unescaped form.
///
/// For example, `\:` is rewritten to `:` as `\:` is not a valid
/// escape sequence in the `regexp` crate but is valid in golang's
/// regexp implementation.
///
/// This is done for compatibility purposes so that the regular
/// expression matching in Rust more closely follows the matching in
/// golang, used by the influx storage rpc.
///
/// See <https://github.com/rust-lang/regex/issues/501> for more details
fn clean_non_meta_escapes(pattern: &str) -> String {
    if pattern.is_empty() {
        return pattern.to_string();
    }

    #[derive(Clone, Copy, Debug)]
    enum SlashState {
        No,
        Single,
        Double,
    }

    let mut next_state = SlashState::No;

    let next_chars = pattern
        .chars()
        .map(Some)
        .skip(1)
        .chain(std::iter::once(None));

    // emit char based on previous
    let new_pattern: String = pattern
        .chars()
        .zip(next_chars)
        .filter_map(|(c, next_char)| {
            let cur_state = next_state;
            next_state = match (c, cur_state) {
                ('\\', SlashState::No) => SlashState::Single,
                ('\\', SlashState::Single) => SlashState::Double,
                ('\\', SlashState::Double) => SlashState::Single,
                _ => SlashState::No,
            };

            // Decide to emit `c` or not
            match (cur_state, c, next_char) {
                (SlashState::No, '\\', Some(next_char))
                | (SlashState::Double, '\\', Some(next_char))
                    if !is_valid_character_after_escape(next_char) =>
                {
                    None
                }
                _ => Some(c),
            }
        })
        .collect();

    new_pattern
}

/// A customized implementation of REGEXP_MATCH() that further process the results
/// from DataFusion regexp_match() function. Instead of returning all found matches
/// in a single column, this function unpack the found matches to match Named Capturing
/// Groups parsed from regex patterns from the second function arguments. If successful,
/// the function returns a key_value pair struct where keys are field names and values
/// are found field values.
///
/// The number of arguments and data types of arguments are implemented the same as
/// the native REGEXP_MATCH() function. They can be used the same way, but to get results
/// back in different formats.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct RegxpMatchToFields {
    signature: Signature,
    aliases: Vec<String>,
}

impl RegxpMatchToFields {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![DataType::Utf8, DataType::Utf8]),
                    Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                    Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                    Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["regexp_match_to_fields".to_string()],
        }
    }
}

impl ScalarUDFImpl for RegxpMatchToFields {
    fn name(&self) -> &str {
        "regexp_match_to_fields"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        unreachable!() // since return_type_from_args is implemented
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 2 {
            return Err(DataFusionError::Execution(
                "regexp_match_to_fields function requires 2 arguments, haystack & pattern, of strings".to_string()
            ));
        }
        let regexp_pattern = match &args.scalar_arguments[1] {
            Some(ScalarValue::Utf8(Some(arg2))) => arg2.to_string().replace('"', ""),
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "The second argument for regexp_match_to_fields needs to be a string, but got {}",
                    args.arg_fields[1]
                )));
            }
        };
        let ret_type = &args.arg_fields[0];
        let fields = regex_pattern_to_fields(&regexp_pattern, ret_type.data_type())?;
        Ok(Arc::new(Field::new(
            "regexp_match_to_fields",
            DataType::Struct(Fields::from_iter(fields)),
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // 1. Get result from datafusion native regexp_match() function
        let len = args
            .args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(arr) => Some(arr.len()),
            });

        let inferred_length = len.unwrap_or(0);
        let args_array = args
            .args
            .iter()
            .map(|arg| arg.clone().into_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        let result = match &args_array[0].data_type() {
            DataType::Utf8 => regexp_match(&args_array)?,
            DataType::LargeUtf8 => regexp_match(&args_array)?,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported data type {other:?} for function regexp_match"
                )));
            }
        };

        let (ret_data_type, regexp_pattern) = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))) => {
                (DataType::Utf8, pattern.to_string().replace('"', ""))
            }
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(pattern))) => {
                (DataType::LargeUtf8, pattern.to_string().replace('"', ""))
            }
            _ => {
                return Err(DataFusionError::Execution("regexp_match_to_fields function requires 2 arguments, haystack & pattern, of strings".to_string()));
            }
        };

        // 2. Unpack result and argument to construct returning struct
        let fields = regex_pattern_to_fields(&regexp_pattern, &ret_data_type)?;

        // 3. Build returning struct
        let mut struct_builder = ArrayData::builder(DataType::Struct(fields.into()));
        match ret_data_type {
            // Result is a single column of ListArray of StringArray.
            // Get the first value of ListArray and iterate the StringArray
            // and build individual StringArrays
            DataType::Utf8 => {
                let result_string_arr = as_list_array(&result).value(0);
                let result_string_arr_internal =
                    as_generic_string_array::<i32>(&result_string_arr)?;

                for v in result_string_arr_internal {
                    let arr = StringArray::from(vec![v]);
                    struct_builder = struct_builder.len(arr.len()).add_child_data(arr.to_data());
                }
            }
            DataType::LargeUtf8 => {
                let result_string_arr = as_large_list_array(&result).value(0);
                let result_string_arr_internal =
                    as_generic_string_array::<i64>(&result_string_arr)?;

                for v in result_string_arr_internal {
                    let arr = StringArray::from(vec![v]);
                    struct_builder = struct_builder.len(arr.len()).add_child_data(arr.to_data());
                }
            }
            _ => unreachable!(), // since checked above
        }

        let Ok(struct_array_data) = struct_builder.build() else {
            return Err(DataFusionError::Execution("regexp_match_to_fields failed to pack result to fields. Named Capturing groups are required in regexp pattern".to_string()));
        };

        let struct_array = StructArray::from(struct_array_data);
        Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(
            struct_array,
        ))))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Parsing field names from given regex pattern by using Named Capturing Groups
/// Error if Named Capturing Groups not used in regex pattern.
fn regex_pattern_to_fields(pattern: &str, ret_type: &DataType) -> Result<Vec<Field>> {
    let mut field_names = vec![];
    let re = regex::Regex::new(r"\?<([^>']+)>|\?P<([^>']+)").unwrap();
    for (_, [field_name]) in re.captures_iter(pattern).map(|cap| cap.extract()) {
        field_names.push(field_name);
    }

    if field_names.is_empty() {
        Err(DataFusionError::Execution("Named Capturing Groups must be used to assign field names for regexp_match_to_fields function".to_string()))
    } else {
        Ok(field_names
            .into_iter()
            .map(|field_name| Field::new(field_name, ret_type.to_owned(), false))
            .collect())
    }
}

#[cfg(test)]
pub(super) mod tests {
    use datafusion::{
        arrow::{array::Int64Array, datatypes::Schema, record_batch::RecordBatch},
        assert_batches_eq,
        datasource::MemTable,
        prelude::SessionContext,
    };

    use super::*;

    #[tokio::test]
    async fn test_regex_udf() {
        let sql = "select * from t where re_match(log, '(err|panic)') and re_not_match(data_center, '(SF|Beijing)')";
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("log", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new("data_center", DataType::Utf8, false),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "2010-03-14 - err",
                    "2014-10-14, panic",
                    "c",
                    "d",
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["NY", "Pune", "SF", "Beijing"])),
            ],
        )
        .unwrap();

        // declare a new context. In spark API, this corresponds to a new spark
        // SQLsession
        let ctx = SessionContext::new();
        ctx.register_udf(REGEX_MATCH_UDF.clone());
        ctx.register_udf(REGEX_NOT_MATCH_UDF.clone());

        // declare a table in memory. In spark API, this corresponds to
        // createDataFrame(...).
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let df = ctx.sql(sql).await.unwrap();
        let result = df.collect().await.unwrap();
        let count = result.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(count, 2);
    }

    #[test]
    fn parse_escape() {
        assert!(is_valid_character_after_escape('0'));
        assert!(is_valid_character_after_escape('8'));
        assert!(is_valid_character_after_escape('x'));
        assert!(is_valid_character_after_escape('p'));
        assert!(is_valid_character_after_escape('d'));
        assert!(!is_valid_character_after_escape('a'));
    }

    #[test]
    fn test_clean_non_meta_escapes_empty() {
        assert_eq!(clean_non_meta_escapes(""), "");
    }

    #[test]
    fn test_clean_non_meta_escapes_valid_escapes_unchanged() {
        // \d, \w, \s are valid — kept as-is
        assert_eq!(clean_non_meta_escapes(r"\d+"), r"\d+");
        assert_eq!(clean_non_meta_escapes(r"\w"), r"\w");
        assert_eq!(clean_non_meta_escapes(r"\s"), r"\s");
    }

    #[test]
    fn test_clean_non_meta_escapes_removes_invalid_backslash() {
        // \: is invalid in Rust regex — backslash stripped, : kept
        assert_eq!(clean_non_meta_escapes(r"\:"), ":");
        // \- is valid (- is meta in regex_syntax 0.8+) — kept as-is
        assert_eq!(clean_non_meta_escapes(r"\-"), r"\-");
    }

    #[test]
    fn test_clean_non_meta_escapes_no_backslash() {
        let s = "hello world";
        assert_eq!(clean_non_meta_escapes(s), s);
    }

    #[test]
    fn test_clean_non_meta_escapes_double_backslash_kept() {
        // \\ is a valid escape (literal backslash) — both chars kept
        assert_eq!(clean_non_meta_escapes(r"\\"), r"\\");
    }

    #[tokio::test]
    async fn test_regexp_match_to_fields_udf() {
        let log_line = r#"2024-02-29 00:15:30 15.128.22.213 GET /Administradores_Elina/service-worker.js - 443"#;

        let schema = Arc::new(Schema::new(vec![Field::new("log", DataType::Utf8, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![log_line]))],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let my_udf = ScalarUDF::from(RegxpMatchToFields::new());
        ctx.register_udf(my_udf);

        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let sqls = [
            (
                r#"select regexp_match_to_fields(log, '(?P<timestamp>[^\s]+ [^\s]+) (?P<client_ip>[^\s]+) (?P<http_method>[^\s]+) (?P<requested_path>[^\s]+) (?P<placeholder1>[^\s]+) (?P<server_port>[^\s]+)') as subquery from t"#,
                vec![
                    "+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                    "| subquery                                                                                                                                                                  |",
                    "+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                    "| {timestamp: 2024-02-29 00:15:30, client_ip: 15.128.22.213, http_method: GET, requested_path: /Administradores_Elina/service-worker.js, placeholder1: -, server_port: 443} |",
                    "+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                ],
            ),
            (
                r#"select regexp_match_to_fields(log, '([^\s]+ [^\s]+) ([^\s]+) ([^\s]+)') as subquery from t"#,
                vec![],
            ),
        ];
        let test1 = ctx.sql(sqls[0].0).await.unwrap().collect().await.unwrap();
        assert_batches_eq!(sqls[0].1, &test1);

        let test2 = ctx.sql(sqls[1].0).await;
        assert!(test2.is_err());
    }

    #[test]
    fn test_regex_pattern_to_fields_with_named_groups() {
        let result =
            regex_pattern_to_fields(r"(?P<host>[^\s]+) (?P<method>[^\s]+)", &DataType::Utf8)
                .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name(), "host");
        assert_eq!(result[1].name(), "method");
    }

    #[test]
    fn test_regex_pattern_to_fields_no_named_groups_errors() {
        let result = regex_pattern_to_fields(r"([^\s]+) ([^\s]+)", &DataType::Utf8);
        assert!(result.is_err());
    }

    #[test]
    fn test_regex_pattern_to_fields_empty_pattern_errors() {
        let result = regex_pattern_to_fields("", &DataType::Utf8);
        assert!(result.is_err());
    }

    pub(in crate::datafusion::udf) fn legacy_regex_match_expr_impl(
        matches: bool,
    ) -> ScalarFunctionImplementation {
        let func = move |args: &[ColumnarValue]| {
            assert_eq!(args.len(), 2); // only works over a single column and pattern at a time.

            let pattern = match &args[1] {
                ColumnarValue::Array(_) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "regex_match({matches}) with non scalar patterns not yet implemented"
                    )));
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(pattern)) => pattern,
                ColumnarValue::Scalar(arg) => {
                    return Err(DataFusionError::Plan(format!(
                        "Expected string pattern to regex_match({matches}), got: {arg:?}"
                    )));
                }
            };

            let pattern = pattern.as_ref().ok_or_else(|| {
                DataFusionError::NotImplemented(
                    "NULL patterns not supported in regex_match".to_string(),
                )
            })?;

            let pattern = clean_non_meta_escapes(pattern);

            let pattern = regex::Regex::new(&pattern).map_err(|e| {
                DataFusionError::Plan(format!("error compiling regex pattern: {e}"))
            })?;

            match &args[0] {
                ColumnarValue::Array(arr) => {
                    let results = datafusion::arrow::array::as_string_array(arr)
                        .iter()
                        .map(|row| row.map(|v| pattern.is_match(v) == matches))
                        .collect::<BooleanArray>();

                    Ok(ColumnarValue::Array(Arc::new(results) as ArrayRef))
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(row)) => {
                    let res = row.as_ref().map(|v| pattern.is_match(v) == matches);
                    Ok(ColumnarValue::Scalar(ScalarValue::Boolean(res)))
                }
                ColumnarValue::Scalar(v) => Err(DataFusionError::Plan(format!(
                    "regex_match({matches}) expected first argument to be utf8, got ('{v}')"
                ))),
            }
        };

        Arc::new(func)
    }

    #[test]
    fn regex_cache_is_bounded_and_refreshes_lru() {
        let mut cache = RegexCache::default();
        let first = cache.get_or_compile("cache_first").unwrap();
        for index in 1..REGEX_CACHE_CAPACITY {
            cache.get_or_compile(&format!("cache_{index}")).unwrap();
        }
        assert!(Arc::ptr_eq(
            &first,
            &cache.get_or_compile("cache_first").unwrap()
        ));
        cache.get_or_compile("cache_overflow").unwrap();
        assert_eq!(cache.entries.len(), REGEX_CACHE_CAPACITY);
        assert!(!cache.entries.contains_key("cache_1"));
        assert!(cache.entries.contains_key("cache_first"));
        for index in 0..REGEX_CACHE_CAPACITY * 2 {
            cache.get_or_compile(&format!("extra_{index}")).unwrap();
            assert_eq!(cache.entries.len(), REGEX_CACHE_CAPACITY);
        }
        assert!(!cache.entries.contains_key("cache_first"));
        assert!(cache.get_or_compile("[").is_err());
        assert_eq!(cache.entries.len(), REGEX_CACHE_CAPACITY);
    }

    #[test]
    fn regex_cache_concurrent_misses_compile_once() {
        let cache = Arc::new(Mutex::new(RegexCache::default()));
        let compiled: usize = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let cache = Arc::clone(&cache);
                    scope.spawn(move || {
                        cache.lock().get_or_compile("concurrent_pattern").unwrap();
                        REGEX_COMPILATIONS.get()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .sum()
        });
        assert_eq!(compiled, 1);
    }

    #[test]
    fn regex_constant_pattern_compiles_once_across_batches() {
        let before = REGEX_COMPILATIONS.get();
        for _ in 0..5 {
            for matches in [true, false] {
                let args = [
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![
                        Some("cache:batch_42"),
                        None,
                        Some(""),
                    ]))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(r"cache\:batch_\d+".into()))),
                ];
                regex_match_expr_impl(matches)(&args).unwrap();
            }
        }
        assert_eq!(REGEX_COMPILATIONS.get() - before, 1);
        let before = REGEX_COMPILATIONS.get();
        cached_regex(r"cache:batch_\d+").unwrap();
        assert_eq!(REGEX_COMPILATIONS.get(), before);
    }

    #[test]
    fn regex_boolean_differential_strings_nulls_and_errors() {
        let rows = vec![
            None,
            Some(""),
            Some("error:42"),
            Some("中文é🙂"),
            Some("panic\\path"),
        ];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(rows.clone())),
            Arc::new(LargeStringArray::from(rows.clone())),
            Arc::new(StringViewArray::from(rows.clone())),
        ];
        for matches in [true, false] {
            for pattern in [
                Some(""),
                Some(r"\:"),
                Some(r"(err|panic)"),
                Some("中文|é|🙂"),
                Some("["),
                None,
            ] {
                let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(pattern.map(str::to_owned)));
                let expected = legacy_regex_match_expr_impl(matches)(&[
                    ColumnarValue::Array(Arc::clone(&arrays[0])),
                    pattern.clone(),
                ]);
                for array in &arrays {
                    let actual = regex_match_expr_impl(matches)(&[
                        ColumnarValue::Array(Arc::clone(array)),
                        pattern.clone(),
                    ]);
                    match (&actual, &expected) {
                        (Ok(ColumnarValue::Array(actual)), Ok(ColumnarValue::Array(expected))) => {
                            assert_eq!(actual.to_data(), expected.to_data())
                        }
                        (Err(actual), Err(expected)) => {
                            assert_eq!(actual.to_string(), expected.to_string())
                        }
                        _ => panic!("result mismatch: {actual:?} vs {expected:?}"),
                    }
                }
                for row in &rows {
                    let args = [
                        ColumnarValue::Scalar(ScalarValue::Utf8(row.map(str::to_owned))),
                        pattern.clone(),
                    ];
                    assert_eq!(
                        format!("{:?}", regex_match_expr_impl(matches)(&args)),
                        format!("{:?}", legacy_regex_match_expr_impl(matches)(&args)),
                    );
                }
            }
            let args = [
                ColumnarValue::Array(Arc::clone(&arrays[0])),
                ColumnarValue::Array(Arc::clone(&arrays[0])),
            ];
            assert_eq!(
                regex_match_expr_impl(matches)(&args)
                    .unwrap_err()
                    .to_string(),
                legacy_regex_match_expr_impl(matches)(&args)
                    .unwrap_err()
                    .to_string(),
            );
        }
    }

    #[tokio::test]
    async fn regex_planner_coercion_and_physical_roundtrip() {
        use datafusion::physical_plan::collect;
        use datafusion_proto::bytes::{
            physical_plan_from_bytes_with_extension_codec,
            physical_plan_to_bytes_with_extension_codec,
        };

        use crate::datafusion::{
            distributed_plan::codec::get_physical_extension_codec,
            udf::regexp_matches_udf::REGEX_MATCHES_UDF,
        };
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let rows = vec![Some("error42"), None, Some("中文")];
            let array: ArrayRef = match data_type {
                DataType::Utf8 => Arc::new(StringArray::from(rows)),
                DataType::LargeUtf8 => Arc::new(LargeStringArray::from(rows)),
                _ => Arc::new(StringViewArray::from(rows)),
            };
            let schema = Arc::new(Schema::new(vec![Field::new("s", data_type.clone(), true)]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
            let ctx = SessionContext::new();
            ctx.register_udf(REGEX_MATCH_UDF.clone());
            ctx.register_udf(REGEX_NOT_MATCH_UDF.clone());
            ctx.register_udf(REGEX_MATCHES_UDF.clone());
            ctx.register_table(
                "t",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();
            let plan = ctx.sql("SELECT re_match(s, 'err'), re_not_match(s, 'err'), re_matches(s, '[0-9]+') FROM t")
                .await.unwrap().create_physical_plan().await.unwrap();
            let display = datafusion::physical_plan::displayable(plan.as_ref())
                .indent(true)
                .to_string();
            println!("{data_type:?}: {display}");
            if data_type != DataType::Utf8 {
                assert!(display.contains("CAST(s@0 AS Utf8)"), "{display}");
            }
            let codec = get_physical_extension_codec();
            let bytes =
                physical_plan_to_bytes_with_extension_codec(Arc::clone(&plan), &codec).unwrap();
            let restored =
                physical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &codec)
                    .unwrap();
            assert_eq!(plan.schema(), restored.schema());
            let expected = collect(plan, ctx.task_ctx()).await.unwrap();
            let actual = collect(restored, ctx.task_ctx()).await.unwrap();
            assert_eq!(actual, expected);
            assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        }
    }
}
