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

use serde_json::value::{Map, Value};

/// Nested-key separator: nested objects flatten to dotted paths
/// (`{"http":{"status":..}}` -> `http.status`, DESIGN §15.5).
const KEY_SEPARATOR_DOT: &str = ".";
const TOKEN_NUMBER: &str = "$serde_json::private::Number";

#[inline]
pub fn flatten(to_flatten: Value) -> Result<Value, anyhow::Error> {
    flatten_with_level(to_flatten, 0)
}

/// Flattens the provided JSON object (`current`).
///
/// Flattened keys keep literal `.` characters (dotted paths such as OTLP's
/// `service.name` stay dotted; nested objects flatten with a `.` separator —
/// the core-file behavior). All other illegal characters are sanitized.
///
/// It will return an error if flattening the object would make two keys to be
/// the same, overwriting a value. It will also return an error if the JSON
/// value passed it's not an object.
///
/// # Errors
/// Will return `Err` if `to_flatten` it's not an object, or if flattening the
/// object would result in two or more keys colliding.
pub fn flatten_with_level(to_flatten: Value, max_level: u32) -> Result<Value, anyhow::Error> {
    // quick check to see if we have an object`
    let to_flatten = match to_flatten {
        Value::Object(v) => {
            if v.is_empty() || !v.iter().any(|(_k, v)| v.is_object() || v.is_array()) {
                if v.iter().all(|(k, _v)| check_key(k)) {
                    return Ok(Value::Object(v));
                }
                let mut formatted_map = Map::<String, Value>::with_capacity(v.len());
                for (mut k, v) in v.into_iter() {
                    format_key(&mut k);
                    formatted_map.insert(k, v);
                }
                return Ok(Value::Object(formatted_map));
            }
            Value::Object(v)
        }
        _ => {
            return Err(anyhow::anyhow!("flatten value must be an object"));
        }
    };

    let mut flat = Map::<String, Value>::new();
    flatten_value(to_flatten, "".to_owned(), max_level, 0, &mut flat).map(|_x| Value::Object(flat))
}

/// Flattens the passed JSON value (`current`), whose path is `parent_key` and
/// its 0-based depth is `depth`.  The result is stored in the JSON object
/// `flattened`.
fn flatten_value(
    current: Value,
    parent_key: String,
    max_level: u32,
    depth: u32,
    flattened: &mut Map<String, Value>,
) -> Result<(), anyhow::Error> {
    match current {
        Value::Object(map) => {
            flatten_object(map, &parent_key, max_level, depth, flattened)?;
        }
        Value::Array(arr) => {
            flatten_array(arr, &parent_key, max_level, depth, flattened)?;
        }
        Value::Null => {
            // we don't need to store null values
        }
        _ => {
            flattened.insert(parent_key, current);
        }
    }
    Ok(())
}

/// Flattens the passed object (`current`), whose path is `parent_key` and its
/// 0-based depth is `depth`.  The result is stored in the JSON object
/// `flattened`.
fn flatten_object(
    current: Map<String, Value>,
    parent_key: &str,
    max_level: u32,
    depth: u32,
    flattened: &mut Map<String, Value>,
) -> Result<(), anyhow::Error> {
    if current.is_empty() {
        return Ok(());
    }
    if max_level > 0 && depth >= max_level {
        let v = Value::String(Value::Object(current).to_string());
        flatten_value(v, parent_key.to_string(), max_level, depth, flattened)?;
        return Ok(());
    }
    for (mut k, mut v) in current.into_iter() {
        let parent_key = if k == TOKEN_NUMBER {
            v = if let Some(Some(n)) = v
                .as_str()
                .and_then(|v| v.parse::<f64>().ok().map(super::json::Number::from_f64))
            {
                Value::Number(n)
            } else {
                Value::Null
            };
            parent_key.to_string()
        } else {
            format_key(&mut k);
            if depth > 0 {
                format!("{parent_key}{KEY_SEPARATOR_DOT}{k}")
            } else {
                k
            }
        };
        flatten_value(v, parent_key, max_level, depth + 1, flattened)?;
    }
    Ok(())
}

/// Flattens the passed array (`current`), whose path is `parent_key` and its
/// 0-based depth is `depth`.  The result is stored in the JSON object
/// `flattened`.
fn flatten_array(
    current: Vec<Value>,
    parent_key: &str,
    max_level: u32,
    depth: u32,
    flattened: &mut Map<String, Value>,
) -> Result<(), anyhow::Error> {
    if current.is_empty() {
        return Ok(());
    }
    let v = Value::String(Value::Array(current.to_vec()).to_string());
    flatten_value(v, parent_key.to_string(), max_level, depth, flattened)?;
    Ok(())
}

/// We need every character in the key to be lowercase alphanumeric,
/// underscore or a literal `.` (dotted field names are first-class in
/// core files).
pub fn format_key(key: &mut String) {
    format_key_with(key, true)
}

/// [`format_key`] with an explicit dot policy. `keep_dots = false` is the
/// historical underscore behavior (`a.b` -> `a_b`), kept only for
/// [`format_label_name`] (Prometheus label naming).
fn format_key_with(key: &mut String, keep_dots: bool) {
    if check_key_with(key, keep_dots) {
        return;
    }

    let bytes = unsafe { key.as_bytes_mut() };
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i].is_ascii() {
            if bytes[i].is_ascii_uppercase() {
                bytes[i] = bytes[i].to_ascii_lowercase();
            } else if !bytes[i].is_ascii_lowercase()
                && !bytes[i].is_ascii_digit()
                && bytes[i] != b'_'
                && !(keep_dots && bytes[i] == b'.')
            {
                bytes[i] = b'_';
            }
        } else {
            *key = key
                .chars()
                .map(|c| {
                    if c.is_lowercase() || c.is_numeric() || (keep_dots && c == '.') {
                        c
                    } else if c.is_uppercase() {
                        c.to_lowercase().next().unwrap()
                    } else {
                        '_'
                    }
                })
                .collect();
            return;
        }
        i += 1;
    }
}

/// Metrics label names never keep dots (Prometheus label naming) — the one
/// remaining user of the underscore key mode.
pub fn format_label_name(label_name: &str) -> String {
    let mut key = label_name.to_string();
    format_key_with(&mut key, false);
    key
}

/// Reserved alias pairs `(dotted, canonical)` resolved after flattening.
///
/// Producers emit ECS-style dotted trace context — either literal body keys
/// (`"trace.id"`) or nested objects (`{"trace":{"id":..}}`, which flattening
/// turns into `trace.id`) — splitting the SAME identifier across two field
/// names. For exactly these pairs the underscore form is canonical (it is
/// what the OTLP log path mints from the protocol fields); every other field
/// keeps the dotted canon. Future reserved aliases are one line here.
pub const RESERVED_FIELD_ALIASES: [(&str, &str); 2] =
    [("trace.id", "trace_id"), ("span.id", "span_id")];

/// Canonicalize the [`RESERVED_FIELD_ALIASES`] on a flattened record: rename
/// the dotted key to its canonical underscore key when the latter is absent;
/// when both are present drop the dotted key — the canonical key wins
/// regardless of value equality. Records without the dotted keys are
/// untouched. Applied by the logs ingest funnel (`write_logs`) AFTER
/// flattening; traces ingest already uses the protocol fields and metrics
/// are left alone.
pub fn canonicalize_reserved_aliases(record: &mut Map<String, Value>) {
    for (dotted, canonical) in RESERVED_FIELD_ALIASES {
        if let Some(value) = record.remove(dotted)
            && !record.contains_key(canonical)
        {
            record.insert(canonical.to_string(), value);
        }
    }
}

#[inline]
fn check_key(key: &str) -> bool {
    check_key_with(key, true)
}

fn check_key_with(key: &str, keep_dots: bool) -> bool {
    key.chars()
        .all(|c| c.is_lowercase() || c.is_numeric() || c == '_' || (keep_dots && c == '.'))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_check_key_lowercase() {
        assert!(check_key_with("hello", false));
    }

    #[test]
    fn test_check_key_numeric() {
        assert!(check_key_with("123", false));
    }

    #[test]
    fn test_check_key_underscore() {
        assert!(check_key_with("my_key", false));
    }

    #[test]
    fn test_check_key_mixed_case() {
        assert!(!check_key_with("Hello_World", false));
    }

    #[test]
    fn test_check_key_special_characters() {
        assert!(!check_key_with("key!", false));
    }

    #[test]
    fn test_check_key_dots_follow_the_mode() {
        // label mode: a dot is an illegal character
        assert!(!check_key_with("service.name", false));
        // key mode: dots are first-class
        assert!(check_key_with("service.name", true));
        assert!(check_key("service.name"));
        // other illegal characters still fail in both modes
        assert!(!check_key_with("service.Name", true));
        assert!(!check_key_with("service name", true));
    }

    #[test]
    fn object_with_plain_values() {
        let obj = json!({"int": 1, "float": 2.0, "str": "a", "bool": true, "null": null});
        assert_eq!(obj, flatten(obj.clone()).unwrap());
    }

    #[test]
    fn object_with_plain_values_with_format_key() {
        let obj = json!({"int": 1, "float": 2.0, "str": "a", "bool": true, "null": null});
        let obj2 = json!({"int": 1, "Float": 2.0, "str": "a", "bool": true, "null": null});
        assert_eq!(obj, flatten(obj2).unwrap());
    }

    /// Ensures that when using `ArrayFormatting::Plain` both arrays and objects
    /// are formatted properly.
    #[test]
    fn array_formatting_plain() {
        let obj = json!({"s": {"a": [1, 2.0, "b", null, true]}});
        assert_eq!(
            flatten(obj).unwrap(),
            json!({
                format!("s{KEY_SEPARATOR_DOT}a"): "[1,2.0,\"b\",null,true]",
            })
        );
    }

    #[test]
    fn nested_single_key_value() {
        let obj = json!({"key": "value", "nested_key": {"key": "value"}});
        assert_eq!(
            flatten(obj).unwrap(),
            json!({"key": "value", "nested_key.key": "value"}),
        );
    }

    #[test]
    fn nested_multiple_key_value() {
        let obj = json!({"key": "value", "nested_key": {"key1": "value1", "key2": "value2"}});
        assert_eq!(
            flatten(obj).unwrap(),
            json!({"key": "value", "nested_key.key1": "value1", "nested_key.key2": "value2"}),
        );
    }

    #[test]
    fn complex_nested_struct() {
        let obj = json!({
            "simple_key": "simple_value",
            "key": [
                "value1",
                {"key": "value2"},
                {"nested_array": [
                    "nested1",
                    "nested2",
                    ["nested3", "nested4"]
                ]}
            ]
        });
        assert_eq!(
            flatten(obj).unwrap(),
            json!({"simple_key": "simple_value", "key": "[\"value1\",{\"key\":\"value2\"},{\"nested_array\":[\"nested1\",\"nested2\",[\"nested3\",\"nested4\"]]}]"}),
        );
    }

    // #[test]
    // fn overlapping_after_flattening_array() {
    //     let obj = json!({"key": ["value1", "value2"], "key_0": "Oopsy"});
    //     let res = flatten(&obj);
    //     assert!(res.is_err());
    //     match res {
    //         Err(err) => assert!(err.to_string().contains("key_0")),
    //         Ok(_) => panic!("This should have failed"),
    //     }
    // }

    /// Ensure that empty arrays are not present in the result
    #[test]
    fn empty_array() {
        let obj = json!({"key": []});
        assert_eq!(flatten(obj).unwrap(), json!({}));
    }

    /// Ensure that empty objects are not present in the result
    #[test]
    fn empty_object() {
        let obj = json!({"key": {}});
        assert_eq!(flatten(obj).unwrap(), json!({}));
    }

    #[test]
    fn empty_top_object() {
        let obj = json!({});
        assert_eq!(flatten(obj).unwrap(), json!({}));
    }

    /// Ensure that if all the end values of the JSON object are either `[]` or
    /// `{}` the flattened resulting object it's empty.
    #[test]
    fn empty_complex_object() {
        let obj = json!({"key": {"key2": {}, "key3": [[], {}, {"k": {}, "q": []}]}});
        assert_eq!(
            flatten(obj).unwrap(),
            json!({"key.key3": "[[],{},{\"k\":{},\"q\":[]}]"})
        );
    }

    #[test]
    fn nested_object_with_empty_array_and_string() {
        let obj = json!({"key": {"key2": [], "key3": "a"}});
        assert_eq!(flatten(obj).unwrap(), json!({"key.key3": "a"}));
    }

    #[test]
    fn nested_object_with_empty_object_and_string() {
        let obj = json!({"key": {"key2": {}, "key3": "a"}});
        assert_eq!(flatten(obj).unwrap(), json!({"key.key3": "a"}));
    }

    #[test]
    fn empty_string_as_key() {
        let obj = json!({"key": {"": "a"}});
        assert_eq!(flatten(obj).unwrap(), json!({"key.": "a"}));
    }

    #[test]
    fn empty_string_as_key_multiple_times() {
        let obj = json!({"key": {"": {"": {"": "a"}}}});
        assert_eq!(flatten(obj).unwrap(), json!({"key...": "a"}));
    }

    /// Flattening only makes sense for objects. Passing something else must
    /// return an informative error.
    #[test]
    fn first_level_must_be_an_object() {
        let integer = json!(3);
        let string = json!("");
        let boolean = json!(false);
        let null = json!(null);
        let array = json!([1, 2, 3]);

        for j in [integer, string, boolean, null, array].into_iter() {
            let res = flatten(j);
            match res {
                Err(_) => {} // Good
                Ok(_) => panic!("This should have failed"),
            }
        }
    }

    #[test]
    fn complex_array() {
        let obj = json!({"a": [1, [2, [3, 4], 5], 6]});
        assert_eq!(flatten(obj).unwrap(), json!({"a": "[1,[2,[3,4],5],6]"}));
    }

    #[test]
    fn complex_key_format() {
        let data = [
            (
                json!({"key": "value", "nested_key": {"key": "value", "foo": "bar"}}),
                json!({"key": "value", "nested_key.key": "value", "nested_key.foo": "bar"}),
            ),
            (
                json!({"key+bar": "value", "@nested_key": {"#key": "value", "&Foo": "Bar"}}),
                json!({"key_bar": "value", "_nested_key._key": "value", "_nested_key._foo": "Bar"}),
            ),
            (
                json!({"a": {"A.1": [1, [3, 4], 5], "A_2": 6}}),
                // dots are kept (label names are the only underscore mode,
                // covered by test_format_key_dot_modes)
                json!({"a.a.1": "[1,[3,4],5]", "a.a_2": 6}),
            ),
        ];
        for (input, expected) in data.into_iter() {
            assert_eq!(flatten(input).unwrap(), expected);
        }
    }

    #[test]
    fn test_flatten_json_complex() {
        let input = json!({
            "firstName": "John",
            "lastName": "Doe",
            "age": 25,
            "address": {
                "streetAddress": "123 Main St",
                "city": "Anytown",
                "state": "CA",
                "postalCode": "12345"
            },
            "phoneNumbers": [
                {
                    "type": "home",
                    "number": "555-555-1234"
                },
                {
                    "type": "work",
                    "number": "555-555-5678"
                }
            ]
        });

        let output = flatten(input).unwrap();

        // Check all fields except phonenumbers
        assert_eq!(output["firstname"], "John");
        assert_eq!(output["lastname"], "Doe");
        assert_eq!(output["age"], 25);
        assert_eq!(output["address.streetaddress"], "123 Main St");
        assert_eq!(output["address.city"], "Anytown");
        assert_eq!(output["address.state"], "CA");
        assert_eq!(output["address.postalcode"], "12345");

        // Parse and compare phonenumbers JSON to handle key ordering
        let phonenumbers_str = output["phonenumbers"].as_str().unwrap();
        let phonenumbers: serde_json::Value = serde_json::from_str(phonenumbers_str).unwrap();
        let expected_phonenumbers = json!([
            {"type": "home", "number": "555-555-1234"},
            {"type": "work", "number": "555-555-5678"}
        ]);
        assert_eq!(phonenumbers, expected_phonenumbers);
    }

    fn compare_flattened_json(actual: &serde_json::Value, expected: &serde_json::Value) {
        // Helper to compare JSON values, parsing embedded JSON strings
        let actual_obj = actual.as_object().unwrap();
        let expected_obj = expected.as_object().unwrap();

        assert_eq!(
            actual_obj.len(),
            expected_obj.len(),
            "Different number of fields"
        );

        for (key, expected_val) in expected_obj {
            let actual_val = actual_obj
                .get(key)
                .unwrap_or_else(|| panic!("Missing key: {}", key));

            if let (Some(actual_str), Some(expected_str)) =
                (actual_val.as_str(), expected_val.as_str())
            {
                // Both are strings, try to parse as JSON
                if let (Ok(actual_json), Ok(expected_json)) = (
                    serde_json::from_str::<serde_json::Value>(actual_str),
                    serde_json::from_str::<serde_json::Value>(expected_str),
                ) {
                    // Both strings contain valid JSON, compare them structurally
                    assert_eq!(actual_json, expected_json, "JSON mismatch for key: {}", key);
                } else {
                    // Not JSON or parse failed, compare as strings
                    assert_eq!(actual_str, expected_str, "String mismatch for key: {}", key);
                }
            } else {
                // Non-string values, compare directly
                assert_eq!(actual_val, expected_val, "Value mismatch for key: {}", key);
            }
        }
    }

    #[test]
    fn test_flatten_with_level() {
        let input = json!({
            "firstName": "John",
            "lastName": "Doe",
            "age": 25,
            "info": {
                "address": {
                    "streetAddress": "123 Main St",
                    "city": "Anytown",
                    "state": "CA",
                    "postalCode": "12345",
                    "phoneNumbers": {
                        "type": "home",
                        "number": "555-555-1234"
                    }
                },
                "phoneNumbers": [
                    {
                        "type": "home",
                        "number": "555-555-1234"
                    },
                    {
                        "type": "work",
                        "number": "555-555-5678"
                    }
                ]
            }
        });

        let expected_output_level0 = json!({
            "firstname": "John",
            "lastname": "Doe",
            "age": 25,
            "info.address.streetaddress": "123 Main St",
            "info.address.city": "Anytown",
            "info.address.state": "CA",
            "info.address.postalcode": "12345",
            "info.address.phonenumbers.number": "555-555-1234",
            "info.address.phonenumbers.type": "home",
            "info.phonenumbers": "[{\"number\":\"555-555-1234\",\"type\":\"home\"},{\"number\":\"555-555-5678\",\"type\":\"work\"}]"
        });
        let expected_output_level1 = json!({
            "firstname": "John",
            "lastname": "Doe",
            "age": 25,
            "info": "{\"address\":{\"city\":\"Anytown\",\"phoneNumbers\":{\"number\":\"555-555-1234\",\"type\":\"home\"},\"postalCode\":\"12345\",\"state\":\"CA\",\"streetAddress\":\"123 Main St\"},\"phoneNumbers\":[{\"number\":\"555-555-1234\",\"type\":\"home\"},{\"number\":\"555-555-5678\",\"type\":\"work\"}]}"
        });
        let expected_output_level2 = json!({
            "firstname": "John",
            "lastname": "Doe",
            "age": 25,
            "info.address": "{\"city\":\"Anytown\",\"phoneNumbers\":{\"number\":\"555-555-1234\",\"type\":\"home\"},\"postalCode\":\"12345\",\"state\":\"CA\",\"streetAddress\":\"123 Main St\"}",
            "info.phonenumbers": "[{\"number\":\"555-555-1234\",\"type\":\"home\"},{\"number\":\"555-555-5678\",\"type\":\"work\"}]"
        });
        let expected_output_level3 = json!({
            "firstname": "John",
            "lastname": "Doe",
            "age": 25,
            "info.address.streetaddress": "123 Main St",
            "info.address.city": "Anytown",
            "info.address.state": "CA",
            "info.address.postalcode": "12345",
            "info.address.phonenumbers": "{\"number\":\"555-555-1234\",\"type\":\"home\"}",
            "info.phonenumbers": "[{\"number\":\"555-555-1234\",\"type\":\"home\"},{\"number\":\"555-555-5678\",\"type\":\"work\"}]"
        });
        let expected_output_level4 = json!({
            "firstname": "John",
            "lastname": "Doe",
            "age": 25,
            "info.address.streetaddress": "123 Main St",
            "info.address.city": "Anytown",
            "info.address.state": "CA",
            "info.address.postalcode": "12345",
            "info.address.phonenumbers.number": "555-555-1234",
            "info.address.phonenumbers.type": "home",
            "info.phonenumbers": "[{\"number\":\"555-555-1234\",\"type\":\"home\"},{\"number\":\"555-555-5678\",\"type\":\"work\"}]"
        });

        let output = flatten_with_level(input.clone(), 0).unwrap();
        compare_flattened_json(&output, &expected_output_level0);
        let output = flatten_with_level(input.clone(), 1).unwrap();
        compare_flattened_json(&output, &expected_output_level1);
        let output = flatten_with_level(input.clone(), 2).unwrap();
        compare_flattened_json(&output, &expected_output_level2);
        let output = flatten_with_level(input.clone(), 3).unwrap();
        compare_flattened_json(&output, &expected_output_level3);
        let output = flatten_with_level(input.clone(), 4).unwrap();
        compare_flattened_json(&output, &expected_output_level4);
        let output = flatten_with_level(input, 5).unwrap();
        compare_flattened_json(&output, &expected_output_level4);
    }

    #[test]
    fn test_form_keys() {
        let test_cases = [
            "already_formatted_key_123",
            "simple",
            "HelloWorld",
            "UPPERCASE",
            "hello!world@123#",
            "test.key-here",
            "Mixed_Case_123!@#",
            "camelCaseTest",
            "hello世界",
            "café_là",
            "",
            "!@#$%^",
            "____",
            "Hello世界World",
            "test!!!test",
            "test123test456",
            "123test",
        ];
        let expected = [
            "already_formatted_key_123",
            "simple",
            "helloworld",
            "uppercase",
            "hello_world_123_",
            // dots are kept (the underscore mode exists only for label
            // names, covered by test_format_key_dot_modes)
            "test.key_here",
            "mixed_case_123___",
            "camelcasetest",
            "hello__",
            "café_là",
            "",
            "______",
            "____",
            "hello__world",
            "test___test",
            "test123test456",
            "123test",
        ];
        for (input, expected) in test_cases.iter().zip(expected) {
            let mut key = input.to_string();
            format_key(&mut key);
            assert_eq!(key, expected);
        }
    }

    #[test]
    fn test_format_label_name() {
        assert_eq!(format_label_name("HelloWorld"), "helloworld");
        assert_eq!(format_label_name("already_fine"), "already_fine");
        // label names are always dot-free (Prometheus label naming)
        assert_eq!(format_label_name("test.key"), "test_key");
        assert_eq!(format_label_name("UPPER_CASE"), "upper_case");
        assert_eq!(format_label_name(""), "");
    }

    #[test]
    fn test_format_key_dot_modes() {
        // (input, label mode, key mode) — dots survive only in the key
        // mode; all other sanitization matches in both modes
        let cases = [
            ("service.name", "service_name", "service.name"),
            ("k8s.Pod.Name", "k8s_pod_name", "k8s.pod.name"),
            ("with space.dot", "with_space_dot", "with_space.dot"),
            // non-ascii replacement path
            ("café.Là", "café_là", "café.là"),
            ("hello世界.x", "hello___x", "hello__.x"),
        ];
        for (input, label, key_mode) in cases {
            let mut key = input.to_string();
            format_key_with(&mut key, false);
            assert_eq!(key, label, "label mode for {input:?}");

            let mut key = input.to_string();
            format_key_with(&mut key, true);
            assert_eq!(key, key_mode, "key mode for {input:?}");
        }
    }

    #[test]
    fn test_flatten_keeps_dots() {
        let input = json!({
            "service.name": "api",
            "outer": {"inner.leaf": 1},
            "Plain": true,
        });

        // dots survive, nested objects flatten to dotted paths, and all
        // other sanitization is unchanged
        assert_eq!(
            flatten(input).unwrap(),
            json!({"service.name": "api", "outer.inner.leaf": 1, "plain": true})
        );
    }

    /// Flatten, then canonicalize the reserved aliases — the exact two-step
    /// sequence the logs ingest funnel runs on every record.
    fn flatten_and_canonicalize(input: serde_json::Value) -> serde_json::Value {
        let mut record = match flatten(input).unwrap() {
            Value::Object(map) => map,
            _ => unreachable!("flatten always returns an object"),
        };
        canonicalize_reserved_aliases(&mut record);
        Value::Object(record)
    }

    #[test]
    fn test_canonicalize_reserved_aliases_literal_dotted() {
        // literal dotted body keys are renamed to the canonical fields
        let input = json!({"log": "a", "trace.id": "t1", "span.id": "s1"});
        assert_eq!(
            flatten_and_canonicalize(input),
            json!({"log": "a", "trace_id": "t1", "span_id": "s1"})
        );
    }

    #[test]
    fn test_canonicalize_reserved_aliases_nested_object() {
        // nested trace context flattens to the dotted keys, which are then
        // renamed; sibling paths under the same object keep the dotted canon
        let input = json!({
            "log": "a",
            "trace": {"id": "t1", "flags": 1},
            "span": {"id": "s1"},
        });
        assert_eq!(
            flatten_and_canonicalize(input),
            json!({"log": "a", "trace_id": "t1", "trace.flags": 1, "span_id": "s1"})
        );
    }

    #[test]
    fn test_canonicalize_reserved_aliases_both_present_underscore_wins() {
        // when both forms are present the dotted key is dropped, regardless
        // of value equality (literal and nested shapes behave the same)
        for input in [
            json!({"trace.id": "loser", "trace_id": "winner", "span.id": "loser", "span_id": "winner"}),
            json!({"trace": {"id": "loser"}, "trace_id": "winner", "span": {"id": "loser"}, "span_id": "winner"}),
        ] {
            assert_eq!(
                flatten_and_canonicalize(input),
                json!({"trace_id": "winner", "span_id": "winner"})
            );
        }
    }

    #[test]
    fn test_canonicalize_reserved_aliases_only_underscore_is_noop() {
        let input = json!({"log": "a", "trace_id": "t1", "span_id": "s1"});
        assert_eq!(
            flatten_and_canonicalize(input.clone()),
            input,
            "canonical-only records must pass through unchanged"
        );
    }

    #[test]
    fn test_canonicalize_reserved_aliases_neither_is_noop() {
        // no reserved keys at all — including other dotted fields, which keep
        // the dotted canon untouched
        let input = json!({"log": "a", "service.name": "api", "trace.flags": 1});
        assert_eq!(flatten_and_canonicalize(input.clone()), input);
    }
}
