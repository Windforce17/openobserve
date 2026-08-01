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
    ID_COL_NAME, ORIGINAL_DATA_COL_NAME, TIMESTAMP_COL_NAME, get_config,
    meta::{search::SearchEventType, sql::TableReferenceExt, stream::StreamType},
};
use datafusion::{arrow::datatypes::Schema, common::TableReference};
use hashbrown::{HashMap, HashSet};
use infra::schema::{
    SchemaCache, get_stream_setting_column_store_fields, get_stream_setting_fts_fields,
    unwrap_stream_settings,
};
use vortex_index::SOURCE_COL_NAME;

#[allow(clippy::too_many_arguments)]
pub fn generate_select_star_schema(
    schemas: HashMap<TableReference, Arc<SchemaCache>>,
    columns: &HashMap<TableReference, HashSet<String>>,
    has_original_column: HashMap<TableReference, bool>,
    quick_mode: bool,
    quick_mode_num_fields: usize,
    search_event_type: &Option<SearchEventType>,
    need_fst_fields: bool,
    sql_stream_type: StreamType,
    row_store_eligible: bool,
) -> HashMap<TableReference, Arc<SchemaCache>> {
    let mut used_schemas = HashMap::new();
    for (name, schema) in schemas {
        let stream_settings = unwrap_stream_settings(schema.schema());
        let has_original_column = *has_original_column.get(&name).unwrap_or(&false);

        // Row-store-driven star (DESIGN §5): for a plain single-stream
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

        let quick_mode = quick_mode && schema.schema().fields().len() > quick_mode_num_fields;
        // don't automatically skip _original for scheduled pipeline searches
        let skip_original_column = !has_original_column
            && !matches!(search_event_type, Some(SearchEventType::DerivedStream))
            && schema.contains_field(ORIGINAL_DATA_COL_NAME);
        if quick_mode || skip_original_column {
            let fields = if quick_mode {
                let columns = columns.get(&name).cloned();
                let fts_fields = get_stream_setting_fts_fields(&stream_settings);
                generate_quick_mode_fields(
                    schema.schema(),
                    columns,
                    &fts_fields,
                    skip_original_column,
                    need_fst_fields,
                )
            } else {
                // skip selecting "_original" column if `SELECT * ...`
                let mut fields = schema.schema().fields().iter().cloned().collect::<Vec<_>>();
                if !need_fst_fields {
                    fields.retain(|field| field.name() != ORIGINAL_DATA_COL_NAME);
                }
                fields
            };
            let schema = Arc::new(SchemaCache::new(
                Schema::new(fields).with_metadata(schema.schema().metadata().clone()),
            ));
            used_schemas.insert(name, schema);
        } else {
            used_schemas.insert(name, schema);
        }
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
    // the column-store columns: native docs columns, they overlay the
    // `_source` image in the response (authoritative for merged files)
    if let Some(settings) = stream_settings.as_ref() {
        for field in get_stream_setting_column_store_fields(settings) {
            push(&field, &mut fields, &mut names);
        }
    }
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

pub fn generate_quick_mode_fields(
    schema: &Schema,
    columns: Option<HashSet<String>>,
    fts_fields: &[String],
    skip_original_column: bool,
    need_fst_fields: bool,
) -> Vec<Arc<arrow_schema::Field>> {
    let cfg = get_config();
    let strategy = cfg.limit.quick_mode_strategy.to_lowercase();
    let schema_fields = schema.fields().iter().cloned().collect::<Vec<_>>();
    let mut fields = match strategy.as_str() {
        "last" => {
            let skip = std::cmp::max(0, schema_fields.len() - cfg.limit.quick_mode_num_fields);
            schema_fields.into_iter().skip(skip).collect()
        }
        "both" => {
            let need_num = std::cmp::min(schema_fields.len(), cfg.limit.quick_mode_num_fields);
            let mut inner_fields = schema_fields
                .iter()
                .take(need_num / 2)
                .cloned()
                .collect::<Vec<_>>();
            if schema_fields.len() > inner_fields.len() {
                let skip = std::cmp::max(0, schema_fields.len() + inner_fields.len() - need_num);
                inner_fields.extend(schema_fields.into_iter().skip(skip));
            }
            inner_fields
        }
        _ => {
            // default is first mode
            schema_fields
                .into_iter()
                .take(cfg.limit.quick_mode_num_fields)
                .collect()
        }
    };

    let mut fields_name = fields
        .iter()
        .map(|f| f.name().to_string())
        .collect::<HashSet<_>>();

    // check the internal columns excluded from `SELECT *`
    if cfg.common.feature_query_exclude_all && fields_name.contains(ORIGINAL_DATA_COL_NAME) {
        fields.retain(|field| field.name().ne(ORIGINAL_DATA_COL_NAME));
    }

    // check _timestamp column
    if !fields_name.contains(TIMESTAMP_COL_NAME)
        && let Ok(field) = schema.field_with_name(TIMESTAMP_COL_NAME)
    {
        fields.push(Arc::new(field.clone()));
        fields_name.insert(TIMESTAMP_COL_NAME.to_string());
    }
    // add the selected columns
    if let Some(columns) = columns {
        for column in columns {
            if !fields_name.contains(&column)
                && let Ok(field) = schema.field_with_name(&column)
            {
                fields.push(Arc::new(field.clone()));
                fields_name.insert(column.to_string());
            }
        }
    }
    // check fts fields
    if need_fst_fields {
        for field in fts_fields {
            if !fields_name.contains(field)
                && let Ok(field) = schema.field_with_name(field)
            {
                fields.push(Arc::new(field.clone()));
                fields_name.insert(field.to_string());
            }
        }
    }

    // check quick mode fields
    for field in config::QUICK_MODEL_FIELDS.iter() {
        if !fields_name.contains(field)
            && let Ok(field) = schema.field_with_name(field)
        {
            fields.push(Arc::new(field.clone()));
            fields_name.insert(field.to_string());
        }
    }

    // include gen AI fields for LLM streams
    if let Some(settings) = unwrap_stream_settings(schema)
        && settings.is_llm_stream
    {
        use config::meta::traces::{
            GEN_AI_SENTINEL_COLUMN, OPTIONAL_GEN_AI_FIELDS, OPTIONAL_LLM_FIELDS,
            REQUIRED_GEN_AI_FIELDS, REQUIRED_LLM_FIELDS,
        };

        let field_lists: &[&[&str]] = if schema.field_with_name(GEN_AI_SENTINEL_COLUMN).is_ok() {
            &[
                REQUIRED_GEN_AI_FIELDS,
                OPTIONAL_GEN_AI_FIELDS,
                &["trace_id", "gen_ai_conversation_id", "user_id"],
            ]
        } else {
            &[
                REQUIRED_LLM_FIELDS,
                OPTIONAL_LLM_FIELDS,
                &["trace_id", "llm_session_id", "llm_user_id"],
            ]
        };
        for list in field_lists {
            for field_name in *list {
                if !fields_name.contains(*field_name)
                    && let Ok(field) = schema.field_with_name(field_name)
                {
                    fields.push(Arc::new(field.clone()));
                    fields_name.insert(field_name.to_string());
                }
            }
        }
    }

    if !need_fst_fields && skip_original_column && fields_name.contains(ORIGINAL_DATA_COL_NAME) {
        fields.retain(|field| field.name() != ORIGINAL_DATA_COL_NAME);
    }
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
    fn test_generate_quick_mode_fields_first_strategy() {
        let fields = vec![
            Arc::new(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)),
            Arc::new(Field::new("field1", DataType::Utf8, true)),
            Arc::new(Field::new("field2", DataType::Int32, true)),
            Arc::new(Field::new("field3", DataType::Float64, true)),
            Arc::new(Field::new("field4", DataType::Boolean, true)),
        ];
        let schema = Schema::new(fields);

        // Mock config - default strategy is "first"
        let result = generate_quick_mode_fields(&schema, None, &[], false, false);

        // Should include timestamp and some fields (exact count depends on config)
        assert!(!result.is_empty());
        assert!(result.iter().any(|f| f.name() == TIMESTAMP_COL_NAME));
    }

    #[test]
    fn test_generate_quick_mode_fields_with_columns() {
        let fields = vec![
            Arc::new(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)),
            Arc::new(Field::new("field1", DataType::Utf8, true)),
            Arc::new(Field::new("field2", DataType::Int32, true)),
            Arc::new(Field::new("selected_field", DataType::Float64, true)),
        ];
        let schema = Schema::new(fields);

        let mut columns = HashSet::new();
        columns.insert("selected_field".to_string());

        let result = generate_quick_mode_fields(&schema, Some(columns), &[], false, false);

        // Should include timestamp and selected field
        assert!(result.iter().any(|f| f.name() == TIMESTAMP_COL_NAME));
        assert!(result.iter().any(|f| f.name() == "selected_field"));
    }

    #[test]
    fn test_generate_quick_mode_fields_skip_original() {
        let fields = vec![
            Arc::new(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)),
            Arc::new(Field::new(ORIGINAL_DATA_COL_NAME, DataType::Utf8, true)),
            Arc::new(Field::new("field1", DataType::Utf8, true)),
        ];
        let schema = Schema::new(fields);

        let result = generate_quick_mode_fields(
            &schema,
            None,
            &[],
            true, // skip_original_column = true
            false,
        );

        // Should not include original data column
        assert!(!result.iter().any(|f| f.name() == ORIGINAL_DATA_COL_NAME));
        assert!(result.iter().any(|f| f.name() == TIMESTAMP_COL_NAME));
    }

    #[test]
    fn test_generate_quick_mode_fields_with_fts_fields() {
        let fields = vec![
            Arc::new(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)),
            Arc::new(Field::new("fts_field1", DataType::Utf8, true)),
            Arc::new(Field::new("fts_field2", DataType::Utf8, true)),
            Arc::new(Field::new("normal_field", DataType::Int32, true)),
        ];
        let schema = Schema::new(fields);

        let fts_fields = vec!["fts_field1".to_string(), "fts_field2".to_string()];

        let result = generate_quick_mode_fields(
            &schema,
            None,
            &fts_fields,
            false,
            true, // need_fst_fields = true
        );

        // Should include FTS fields
        assert!(result.iter().any(|f| f.name() == "fts_field1"));
        assert!(result.iter().any(|f| f.name() == "fts_field2"));
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
        // _timestamp + _o2_id + cs (svc, code) + referenced + _source
        let mut sorted = names(&wide);
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            [
                ID_COL_NAME,
                vortex_index::SOURCE_COL_NAME,
                TIMESTAMP_COL_NAME,
                "code",
                "k8s.container.name",
                "svc",
            ]
            .map(String::from)
        );

        // match_all pulls the fts fields in; explicit `_original` reference
        // pulls that column in — still O(query), not O(registry)
        let full = generate_row_store_star_fields(&wide_registry(5000), Some(&columns), true, true);
        let full = names(&full);
        assert!(full.contains(&"log".to_string()));
        assert!(full.contains(&ORIGINAL_DATA_COL_NAME.to_string()));
        assert_eq!(full.len(), 8);
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

        // row-store eligible: tiny projection ending in _source, even with
        // quick mode forced on
        let (schemas, columns, has_original) = make_input();
        let used = generate_select_star_schema(
            schemas,
            &columns,
            has_original,
            true, // quick mode forced (the live default)
            500,
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
        assert_eq!(names.len(), 6, "star projection must not scale: {names:?}");
        // stream settings metadata survives (o2_id/settings consumers)
        assert!(schema.schema().metadata().contains_key("settings"));

        // not row-store eligible (join/subquery/CTE shapes): the legacy
        // registry expansion stays — quick mode truncates to its first-N
        let (schemas, columns, has_original) = make_input();
        let used = generate_select_star_schema(
            schemas,
            &columns,
            has_original,
            true,
            500,
            &None,
            false,
            StreamType::Logs,
            false,
        );
        let schema = used.get(&TableReference::bare("default")).unwrap();
        assert!(schema.schema().fields().len() > 400);
        assert!(!schema.contains_field(vortex_index::SOURCE_COL_NAME));
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
