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

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use config::{
    ID_COL_NAME, ORIGINAL_DATA_COL_NAME, TIMESTAMP_COL_NAME,
    meta::{search::SearchEventType, sql::TableReferenceExt, stream::StreamType},
};
use datafusion::{arrow::datatypes::Schema, common::TableReference};
use hashbrown::{HashMap, HashSet};
use infra::schema::{
    SchemaCache, get_stream_setting_fts_fields,
    unwrap_stream_settings,
};
use vortex_index::SOURCE_COL_NAME;

pub fn generate_select_star_schema(
    schemas: HashMap<TableReference, Arc<SchemaCache>>,
    columns: &HashMap<TableReference, HashSet<String>>,
    has_original_column: HashMap<TableReference, bool>,
    search_event_type: &Option<SearchEventType>,
    need_fst_fields: bool,
    sql_stream_type: StreamType,
    row_store_eligible: bool,
) -> HashMap<TableReference, Arc<SchemaCache>> {
    let mut used_schemas = HashMap::new();
    for (name, schema) in schemas {
        let has_original_column = *has_original_column.get(&name).unwrap_or(&false);

        // Row-store-driven star (DESIGN §2.1): for a plain single-stream
        // `SELECT *` over a logs/traces stream, the star never enumerates
        // the schema registry — each hit is materialized from its own
        // record's `_source` at the response layer. Plan cost is
        // O(query + settings), flat in the registry width.
        if row_store_eligible
            && matches!(
                name.get_stream_type(sql_stream_type),
                StreamType::Logs | StreamType::Traces
            )
        {
            let fields = generate_row_store_star_fields(
                &schema,
                columns.get(&name),
                has_original_column,
                need_fst_fields,
            );
            let schema = Arc::new(SchemaCache::new(
                Schema::new(fields).with_metadata(schema.schema().metadata().clone()),
            ));
            used_schemas.insert(name, schema);
            continue;
        }

        // don't automatically skip _original for scheduled pipeline searches
        let skip_original_column = !has_original_column
            && !matches!(search_event_type, Some(SearchEventType::DerivedStream))
            && schema.contains_field(ORIGINAL_DATA_COL_NAME);

        // §9 (v2): registry-star survives ONLY for CTE/join/subquery
        // shapes, BOUNDED by the statement's referenced columns (H4: plan
        // cost O(query), never O(registry width)). quick_mode's arbitrary
        // first-N truncation is deleted. Fail-open: a table whose
        // referenced-column set is EMPTY (e.g. the inner star of
        // `SELECT * FROM (SELECT * FROM t)`) keeps the full registry
        // expansion — bounding it to zero columns would break the query.
        let referenced = columns.get(&name).filter(|cols| !cols.is_empty());
        let fields = match referenced {
            Some(cols) => {
                let mut fields =
                    generate_schema_fields(cols.clone(), &schema, need_fst_fields);
                // `_original` rides only when the statement references it
                // or a scheduled pipeline needs it (never by default)
                if !skip_original_column
                    && !fields.iter().any(|f| f.name() == ORIGINAL_DATA_COL_NAME)
                    && let Some(field) = schema.field_with_name(ORIGINAL_DATA_COL_NAME)
                {
                    fields.push(field.clone());
                }
                if skip_original_column && !need_fst_fields {
                    fields.retain(|field| field.name() != ORIGINAL_DATA_COL_NAME);
                }
                fields
            }
            None => {
                // full registry expansion, minus the `_original` skip
                let mut fields = schema.schema().fields().iter().cloned().collect::<Vec<_>>();
                if skip_original_column && !need_fst_fields {
                    fields.retain(|field| field.name() != ORIGINAL_DATA_COL_NAME);
                }
                fields
            }
        };
        let schema = Arc::new(SchemaCache::new(
            Schema::new(fields).with_metadata(schema.schema().metadata().clone()),
        ));
        used_schemas.insert(name, schema);
    }
    used_schemas
}

/// The physical projection of a row-store-driven `SELECT *` (DESIGN §5):
/// `_timestamp` + the stream's column-store columns + `_source` (+ internal
/// columns when applicable) + the identifiers the query itself references —
/// NEVER the registry field list. Hits are materialized per record from
/// `_source` by [`crate::datafusion::source_synthesis::expand_star_source_hits`],
/// with the physical columns taking precedence, so a matched record always
/// returns ITS OWN fields no matter how wide (or stale) the registry is.
pub fn generate_row_store_star_fields(
    schema: &SchemaCache,
    columns: Option<&HashSet<String>>,
    has_original_column: bool,
    need_fst_fields: bool,
) -> Vec<FieldRef> {
    let stream_settings = unwrap_stream_settings(schema.schema());
    let mut fields: Vec<FieldRef> = Vec::new();
    let mut names: HashSet<String> = HashSet::new();
    let push = |field_name: &str, fields: &mut Vec<FieldRef>, names: &mut HashSet<String>| {
        if !names.contains(field_name)
            && let Some(field) = schema.field_with_name(field_name)
        {
            names.insert(field_name.to_string());
            fields.push(field.clone());
        }
    };

    // `_timestamp` always; `_o2_id` whenever the stream carries it (a star
    // hit exposes it today, and the docs blob stores it as a native column)
    push(TIMESTAMP_COL_NAME, &mut fields, &mut names);
    push(ID_COL_NAME, &mut fields, &mut names);
    // `_original` only when the query references it explicitly
    if has_original_column {
        push(ORIGINAL_DATA_COL_NAME, &mut fields, &mut names);
    }
    // (v2 all-present-columns: `_source` alone is authoritative for the
    // star image — every field's value lives in it, and every present field
    // is also a native column; no settings-driven column overlay exists.)
    // fields the statement references (WHERE/ORDER BY/GROUP BY — already
    // registry-resolved by ColumnVisitor), so the plan can bind them
    if let Some(columns) = columns {
        for column in columns {
            push(column, &mut fields, &mut names);
        }
    }
    // match_all needs the full-text fields bound in the plan
    if need_fst_fields {
        for field in get_stream_setting_fts_fields(&stream_settings) {
            push(&field, &mut fields, &mut names);
        }
    }
    // the record itself
    fields.push(Arc::new(Field::new(SOURCE_COL_NAME, DataType::Utf8, true)));
    fields
}

// add field from full text search
pub fn generate_schema_fields(
    columns: HashSet<String>,
    schema: &SchemaCache,
    has_match_all: bool,
) -> Vec<FieldRef> {
    let mut columns = columns;

    // 1. add timestamp field
    if !columns.contains(TIMESTAMP_COL_NAME) {
        columns.insert(TIMESTAMP_COL_NAME.to_string());
    }

    // 2. check _o2_id
    if !columns.contains(ID_COL_NAME) {
        columns.insert(ID_COL_NAME.to_string());
    }

    // 3. add field from full text search
    if has_match_all {
        let stream_settings = infra::schema::unwrap_stream_settings(schema.schema());
        let fts_fields = get_stream_setting_fts_fields(&stream_settings);
        for fts_field in fts_fields {
            if schema.field_with_name(&fts_field).is_none() {
                continue;
            }
            columns.insert(fts_field);
        }
    }

    // 4. generate fields
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        if let Some(field) = schema.field_with_name(&column) {
            fields.push(field.clone());
        }
    }
    fields
}

// check if has original column in sql
pub fn has_original_column(
    columns: &HashMap<TableReference, HashSet<String>>,
) -> HashMap<TableReference, bool> {
    let mut has_original_column = HashMap::with_capacity(columns.len());
    for (name, column) in columns.iter() {
        if column.contains(ORIGINAL_DATA_COL_NAME) {
            has_original_column.insert(name.clone(), true);
        } else {
            has_original_column.insert(name.clone(), false);
        }
    }
    has_original_column
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use config::{ID_COL_NAME, ORIGINAL_DATA_COL_NAME, TIMESTAMP_COL_NAME};
    use datafusion::common::TableReference;
    use hashbrown::{HashMap, HashSet};

    use super::*;

    #[test]
    fn test_has_original_column_with_original() {
        let mut columns = HashMap::new();
        let mut table_columns = HashSet::new();
        table_columns.insert(ORIGINAL_DATA_COL_NAME.to_string());
        table_columns.insert("other_field".to_string());

        let table_ref = TableReference::bare("test_table");
        columns.insert(table_ref.clone(), table_columns);

        let result = has_original_column(&columns);

        assert_eq!(result.len(), 1);
        assert_eq!(result.get(&table_ref), Some(&true));
    }

    #[test]
    fn test_has_original_column_without_original() {
        let mut columns = HashMap::new();
        let mut table_columns = HashSet::new();
        table_columns.insert("field1".to_string());
        table_columns.insert("field2".to_string());

        let table_ref = TableReference::bare("test_table");
        columns.insert(table_ref.clone(), table_columns);

        let result = has_original_column(&columns);

        assert_eq!(result.len(), 1);
        assert_eq!(result.get(&table_ref), Some(&false));
    }

    #[test]
    fn test_has_original_column_multiple_tables() {
        let mut columns = HashMap::new();

        // Table 1 - has original
        let mut table1_columns = HashSet::new();
        table1_columns.insert(ORIGINAL_DATA_COL_NAME.to_string());
        table1_columns.insert("field1".to_string());
        let table1_ref = TableReference::bare("table1");
        columns.insert(table1_ref.clone(), table1_columns);

        // Table 2 - no original
        let mut table2_columns = HashSet::new();
        table2_columns.insert("field2".to_string());
        table2_columns.insert("field3".to_string());
        let table2_ref = TableReference::bare("table2");
        columns.insert(table2_ref.clone(), table2_columns);

        let result = has_original_column(&columns);

        assert_eq!(result.len(), 2);
        assert_eq!(result.get(&table1_ref), Some(&true));
        assert_eq!(result.get(&table2_ref), Some(&false));
    }

    #[test]
    fn test_has_original_column_empty_input() {
        let columns = HashMap::new();
        let result = has_original_column(&columns);
        assert!(result.is_empty());
    }

    #[test]
    fn test_has_original_column_empty_table_columns() {
        let mut columns = HashMap::new();
        let table_columns = HashSet::new();
        let table_ref = TableReference::bare("empty_table");
        columns.insert(table_ref.clone(), table_columns);

        let result = has_original_column(&columns);

        assert_eq!(result.len(), 1);
        assert_eq!(result.get(&table_ref), Some(&false));
    }

    #[test]
    fn test_generate_schema_fields_basic() {
        let fields = vec![
            Arc::new(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)),
            Arc::new(Field::new(ID_COL_NAME, DataType::Utf8, false)),
            Arc::new(Field::new("field1", DataType::Utf8, true)),
            Arc::new(Field::new("field2", DataType::Int32, true)),
        ];
        let schema_cache = SchemaCache::new(Schema::new(fields));

        let mut columns = HashSet::new();
        columns.insert("field1".to_string());

        let result = generate_schema_fields(columns, &schema_cache, false);

        // Should include timestamp, ID, and requested field
        let field_names: HashSet<String> = result.iter().map(|f| f.name().to_string()).collect();
        assert!(field_names.contains(TIMESTAMP_COL_NAME));
        assert!(field_names.contains(ID_COL_NAME));
        assert!(field_names.contains("field1"));
    }

    #[test]
    fn test_generate_schema_fields_missing_timestamp_and_id() {
        let fields = vec![
            Arc::new(Field::new("field1", DataType::Utf8, true)),
            Arc::new(Field::new("field2", DataType::Int32, true)),
            Arc::new(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)),
            Arc::new(Field::new(ID_COL_NAME, DataType::Utf8, false)),
        ];
        let schema = Schema::new(fields);
        let schema_cache = SchemaCache::new(schema);

        let mut columns = HashSet::new();
        columns.insert("field1".to_string());
        // Note: not including timestamp or ID in columns

        let result = generate_schema_fields(columns, &schema_cache, false);

        // Should automatically include timestamp and ID
        let field_names: HashSet<String> = result.iter().map(|f| f.name().to_string()).collect();
        assert!(field_names.contains(TIMESTAMP_COL_NAME));
        assert!(field_names.contains(ID_COL_NAME));
        assert!(field_names.contains("field1"));
    }

    #[test]
    fn test_generate_schema_fields_nonexistent_field() {
        let fields = vec![
            Arc::new(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)),
            Arc::new(Field::new(ID_COL_NAME, DataType::Utf8, false)),
            Arc::new(Field::new("existing_field", DataType::Utf8, true)),
        ];
        let schema_cache = SchemaCache::new(Schema::new(fields));

        let mut columns = HashSet::new();
        columns.insert("existing_field".to_string());
        columns.insert("nonexistent_field".to_string());

        let result = generate_schema_fields(columns, &schema_cache, false);

        // Should only include fields that exist in schema
        let field_names: HashSet<String> = result.iter().map(|f| f.name().to_string()).collect();
        assert!(field_names.contains("existing_field"));
        assert!(!field_names.contains("nonexistent_field"));
    }

    // Test constants and field name validation

    #[test]
    fn test_table_reference_creation() {
        let table_ref1 = TableReference::bare("test_table");
        let table_ref2 = TableReference::bare("test_table");

        // Test that table references with same name are equal
        assert_eq!(table_ref1, table_ref2);

        let table_ref3 = TableReference::bare("different_table");
        assert_ne!(table_ref1, table_ref3);
    }

    fn wide_registry(width: usize) -> SchemaCache {
        let mut fields = vec![
            Arc::new(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)),
            Arc::new(Field::new(ID_COL_NAME, DataType::Int64, true)),
            Arc::new(Field::new(ORIGINAL_DATA_COL_NAME, DataType::Utf8, true)),
            Arc::new(Field::new("k8s.container.name", DataType::Utf8, true)),
            Arc::new(Field::new("svc", DataType::Utf8, true)),
            Arc::new(Field::new("code", DataType::Int64, true)),
            Arc::new(Field::new("log", DataType::Utf8, true)),
        ];
        for i in 0..width {
            fields.push(Arc::new(Field::new(
                format!("attr.field.{i:05}"),
                DataType::Utf8,
                true,
            )));
        }
        let metadata = std::collections::HashMap::from([(
            "settings".to_string(),
            r#"{"column_store_fields":["svc","code"],"full_text_search_keys":["log"]}"#.to_string(),
        )]);
        SchemaCache::new(Schema::new(fields).with_metadata(metadata))
    }

    /// Row-store star planning is O(query + settings): the SAME projection
    /// comes out of a 10-field and a 5000-field registry — the star never
    /// enumerates registry fields (the live 2,366-field truncation shape).
    #[test]
    fn test_row_store_star_fields_flat_in_registry_width() {
        let columns = HashSet::from(["k8s.container.name".to_string()]);
        let narrow =
            generate_row_store_star_fields(&wide_registry(10), Some(&columns), false, false);
        let wide =
            generate_row_store_star_fields(&wide_registry(5000), Some(&columns), false, false);

        let names = |fields: &[FieldRef]| {
            fields
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>()
        };
        // identical projection regardless of registry width
        assert_eq!(names(&narrow), names(&wide));
        // _timestamp + _o2_id + referenced + _source: no settings-driven
        // column overlay exists since v2 all-present-columns — `_source`
        // alone is authoritative for the star image
        let mut sorted = names(&wide);
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            [
                ID_COL_NAME,
                vortex_index::SOURCE_COL_NAME,
                TIMESTAMP_COL_NAME,
                "k8s.container.name",
            ]
            .map(String::from)
        );

        // match_all pulls the fts fields in; explicit `_original` reference
        // pulls that column in — still O(query), not O(registry)
        let full = generate_row_store_star_fields(&wide_registry(5000), Some(&columns), true, true);
        let full = names(&full);
        assert!(full.contains(&"log".to_string()));
        assert!(full.contains(&ORIGINAL_DATA_COL_NAME.to_string()));
        assert_eq!(full.len(), 6);
    }

    /// `generate_select_star_schema` routes plain single-stream logs/traces
    /// star queries onto the row-store projection — quick mode (which used
    /// to TRUNCATE wide star queries to its first-N field subset) never
    /// applies there; every other shape keeps the registry expansion.
    #[test]
    fn test_select_star_schema_row_store_vs_registry_expansion() {
        let width = 2366; // the live stream's registry width
        let columns_set = HashSet::from(["k8s.container.name".to_string()]);
        let make_input = || {
            let mut schemas = HashMap::new();
            schemas.insert(
                TableReference::bare("default"),
                Arc::new(wide_registry(width)),
            );
            let mut columns = HashMap::new();
            columns.insert(TableReference::bare("default"), columns_set.clone());
            let mut has_original = HashMap::new();
            has_original.insert(TableReference::bare("default"), false);
            (schemas, columns, has_original)
        };

        // row-store eligible: tiny projection ending in _source
        let (schemas, columns, has_original) = make_input();
        let used = generate_select_star_schema(
            schemas,
            &columns,
            has_original,
            &None,
            false,
            StreamType::Logs,
            true,
        );
        let schema = used.get(&TableReference::bare("default")).unwrap();
        let names: Vec<&str> = schema
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(names.contains(&vortex_index::SOURCE_COL_NAME));
        assert_eq!(names.len(), 4, "star projection must not scale: {names:?}");
        // stream settings metadata survives (o2_id/settings consumers)
        assert!(schema.schema().metadata().contains_key("settings"));

        // not row-store eligible (join/subquery/CTE shapes): the §9
        // registry-star BOUNDED by the referenced columns — never the
        // arbitrary quick-mode first-N truncation, never O(registry)
        let (schemas, columns, has_original) = make_input();
        let used = generate_select_star_schema(
            schemas,
            &columns,
            has_original,
            &None,
            false,
            StreamType::Logs,
            false,
        );
        let schema = used.get(&TableReference::bare("default")).unwrap();
        let mut names: Vec<&str> = schema
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec![ID_COL_NAME, TIMESTAMP_COL_NAME, "k8s.container.name"],
            "CTE/join star = referenced columns + internals, O(query) not O(registry)"
        );
        assert!(!schema.contains_field(vortex_index::SOURCE_COL_NAME));

        // fail-open: an EMPTY referenced set keeps the full registry
        // expansion (the inner star of a nested `SELECT *` must not
        // collapse to zero columns), `_original` auto-skip still applied
        let (schemas, _, has_original) = make_input();
        let empty_columns: HashMap<TableReference, HashSet<String>> = HashMap::new();
        let used = generate_select_star_schema(
            schemas,
            &empty_columns,
            has_original,
            &None,
            false,
            StreamType::Logs,
            false,
        );
        let schema = used.get(&TableReference::bare("default")).unwrap();
        assert!(schema.schema().fields().len() > 400);
        assert!(!schema.contains_field(ORIGINAL_DATA_COL_NAME));
    }

    #[test]
    fn test_generate_schema_fields_has_match_all_true() {
        // has_match_all=true enters the FTS branch even when FTS fields list is empty
        let fields = vec![
            Arc::new(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)),
            Arc::new(Field::new(ID_COL_NAME, DataType::Utf8, false)),
            Arc::new(Field::new("field1", DataType::Utf8, true)),
        ];
        let schema_cache = SchemaCache::new(Schema::new(fields));

        let mut columns = HashSet::new();
        columns.insert("field1".to_string());

        let result = generate_schema_fields(columns, &schema_cache, true);
        let names: HashSet<_> = result.iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(TIMESTAMP_COL_NAME));
        assert!(names.contains(ID_COL_NAME));
        assert!(names.contains("field1"));
    }
}
