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

use config::{
    ID_COL_NAME, ORIGINAL_DATA_COL_NAME, TIMESTAMP_COL_NAME,
    meta::{sql::TableReferenceExt, stream::StreamType},
};
use hashbrown::HashMap;
use infra::errors::{Error, ErrorCodes};

#[cfg(feature = "enterprise")]
pub use crate::service::authz::{StreamPermissionResourceType, check_stream_permissions};
use crate::service::search::sql::Sql;

// ============================================================================
// Query Validation Helpers
// ============================================================================

/// Extracts a boolean query parameter
/// Accepts "true" (case-insensitive) as true, anything else as false
/// Returns false if parameter is not present
pub fn get_bool_from_request(query: &HashMap<String, String>, param_name: &str) -> bool {
    query
        .get(param_name)
        .and_then(|v| v.to_lowercase().parse::<bool>().ok())
        .unwrap_or(false)
}

/// Validates query fields against the stream schema
/// Returns Ok(()) if validation passes, or error if fields are invalid
pub async fn validate_query_fields(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    sql: &str,
) -> Result<(), Error> {
    // Step 1: Parse SQL to get columns (lightweight parsing)
    let search_query = proto::cluster_rpc::SearchQuery {
        sql: sql.to_string(),
        ..Default::default()
    };

    let sql = Sql::new_with_options(&search_query, org_id, stream_type, None, false).await?;

    // Step 2: Resolve which stream and fields to validate.
    //
    // For multi-stream CTE queries sql.stream_names lists every real stream referenced
    // (e.g. ["default", "k8s_events"]). Each stream has its own set of fields in
    // sql.columns. Look up the entry matching stream_name so that fields belonging to
    // a different stream are not incorrectly checked against stream_name's schema.
    let (target_stream, target_fields) = sql
        .stream_names
        .iter()
        .find(|r| r.stream_name() == stream_name)
        .map(|r| {
            let fields = sql.columns.get(r).cloned().unwrap_or_default();
            (stream_name.to_string(), fields)
        })
        .unwrap_or_else(|| {
            // stream_name not found in sql.stream_names (aliased or unresolved table).
            // Fall back to original behaviour: all tracked columns against stream_name.
            let fields = sql.columns.values().flatten().cloned().collect();
            (stream_name.to_string(), fields)
        });

    // Step 3: Validate target stream's fields against its own schema.
    let schema = infra::schema::get(org_id, &target_stream, stream_type)
        .await
        .map_err(|_| Error::ErrorCode(ErrorCodes::SearchStreamNotFound(target_stream.clone())))?;

    for field in target_fields {
        if is_system_field(&field) {
            continue;
        }

        if schema.field_with_name(&field).is_err() {
            return Err(Error::ErrorCode(ErrorCodes::SearchFieldNotFound(format!(
                "{}. Field not found in stream schema.",
                field
            ))));
        }
    }

    Ok(())
}

/// Checks if a field is a system field that should always be allowed
fn is_system_field(field: &str) -> bool {
    field == TIMESTAMP_COL_NAME || field == ID_COL_NAME || field == ORIGINAL_DATA_COL_NAME
}

/// Guard that cancels an in-flight streaming search when the HTTP response is
/// dropped before the search finishes — i.e. the client disconnected (closed
/// the browser tab, navigated away, network dropped).
///
/// The guard is moved into the response stream's closure so it shares the
/// stream's lifetime. Call [`SearchStreamGuard::mark_finished`] once a terminal
/// event (`Done` / `Error` / `Cancelled`) has been produced; otherwise its
/// `Drop` assumes the stream was torn down early and cancels the query.
///
/// Cancellation is keyed by the parent `trace_id`; the query manager removes
/// every internal sub-query sharing that prefix, so multi-stream searches are
/// covered too. Cancellation requires the enterprise build — on the open-source
/// build the guard only logs the disconnect.
pub struct SearchStreamGuard {
    org_id: String,
    trace_id: String,
    finished: bool,
}

impl SearchStreamGuard {
    pub fn new(org_id: String, trace_id: String) -> Self {
        Self {
            org_id,
            trace_id,
            finished: false,
        }
    }

    /// Mark the search as completed so `Drop` does not cancel it.
    pub fn mark_finished(&mut self) {
        self.finished = true;
    }
}

impl Drop for SearchStreamGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // The response stream was dropped before a terminal event was produced:
        // the client is gone. Cancel the still-running query so we stop burning
        // resources on results nobody will read.
        #[cfg(feature = "enterprise")]
        {
            let org_id = std::mem::take(&mut self.org_id);
            let trace_id = std::mem::take(&mut self.trace_id);
            log::info!(
                "[trace_id {trace_id}] client disconnected before search finished, cancelling query"
            );
            tokio::spawn(async move {
                super::query_manager::cancel_query_internal(&org_id, &trace_id).await;
            });
        }
        #[cfg(not(feature = "enterprise"))]
        log::debug!(
            "[trace_id {}] client disconnected before search finished (org {}); \
             query cancellation requires the enterprise build",
            self.trace_id,
            self.org_id
        );
    }
}

#[cfg(test)]
mod tests {
    use hashbrown::HashMap;

    use super::*;

    #[test]
    fn test_search_stream_guard_mark_finished() {
        let mut guard = SearchStreamGuard::new("org".to_string(), "trace".to_string());
        assert!(!guard.finished);
        guard.mark_finished();
        assert!(guard.finished);
        // A finished guard must not attempt cancellation when dropped.
        drop(guard);
    }

    #[test]
    fn test_get_bool_from_request() {
        let mut params = HashMap::new();

        // Test "true"
        params.insert("validate".to_string(), "true".to_string());
        assert!(get_bool_from_request(&params, "validate"));

        // Test "True" (case insensitive)
        params.insert("validate".to_string(), "True".to_string());
        assert!(get_bool_from_request(&params, "validate"));

        // Test "false"
        params.insert("validate".to_string(), "false".to_string());
        assert!(!get_bool_from_request(&params, "validate"));

        // Test "False" (case insensitive)
        params.insert("validate".to_string(), "False".to_string());
        assert!(!get_bool_from_request(&params, "validate"));

        // Test invalid value (treated as false)
        params.insert("validate".to_string(), "1".to_string());
        assert!(!get_bool_from_request(&params, "validate"));

        // Test missing parameter
        params.clear();
        assert!(!get_bool_from_request(&params, "validate"));
    }

    #[test]
    fn test_is_system_field() {
        // Test all system fields using constants
        assert!(is_system_field(config::TIMESTAMP_COL_NAME)); // "_timestamp"
        assert!(is_system_field(config::ID_COL_NAME)); // "_o2_id"
        assert!(is_system_field(config::ORIGINAL_DATA_COL_NAME)); // "_original"

        // Test non-system fields
        assert!(!is_system_field("_all"));
        assert!(!is_system_field("_all_values"));
        assert!(!is_system_field("custom_field"));
        assert!(!is_system_field("user_id"));
        assert!(!is_system_field("message")); // MESSAGE_COL_NAME is not a system field
        assert!(!is_system_field(""));
    }
}

// ============================================================================
// Query Field Validation Tests - Edge Cases Testing
// ============================================================================

#[cfg(test)]
mod validate_query_edge_cases {
    //! Edge case tests for query field validation against the stream schema.
    //!
    //! **Validation Strategy:** Based on DataFusion CLI testing,
    //! OpenObserve implements STRICT validation that aligns with DataFusion's behavior:
    //! - All fields must exist in the stream's schema
    //! - Validation is per-table in JOIN queries
    //! - Applies to all query types: SELECT, JOIN, subquery, CTE, UNION

    use super::*;

    mod helpers {
        use arrow_schema::{DataType, Field, Schema};
        use config::meta::stream::{StreamSettings, StreamType};
        use infra::schema::{STREAM_SCHEMAS_LATEST, STREAM_SETTINGS, SchemaCache};

        /// Test context holding stream configurations
        pub(super) struct TestContext {
            pub(super) org_id: String,
            pub(super) stream_type: StreamType,
            pub(super) cache_key_oly: String,
            pub(super) cache_key_test1: String,
        }

        impl TestContext {
            pub(super) async fn cleanup(&self) {
                {
                    let mut w = STREAM_SCHEMAS_LATEST.write().await;
                    w.remove(&self.cache_key_oly);
                    w.remove(&self.cache_key_test1);
                }
                {
                    let mut w = STREAM_SETTINGS.write().await;
                    w.remove(&self.cache_key_oly);
                    w.remove(&self.cache_key_test1);
                }
            }
        }

        /// Helper to create realistic schema for Olympic data streams
        pub(super) fn create_olympic_schema() -> Schema {
            Schema::new(vec![
                Field::new(config::TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("body", DataType::Utf8, true),
                Field::new("bronze_medals", DataType::Int64, true),
                Field::new("continent", DataType::Utf8, true),
                Field::new("flag_url", DataType::Utf8, true),
                Field::new("gold_medals", DataType::Int64, true),
                Field::new("id", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("rank", DataType::Int64, true),
                Field::new("total_medals", DataType::Int64, true),
                Field::new("unique_id", DataType::Utf8, true),
            ])
        }

        /// Initialize test context with schemas and default settings
        pub(super) async fn init_test_context(test_name: &str) -> TestContext {
            let org_id = format!("test_org_{}", test_name);
            let stream_type = StreamType::Logs;

            let cache_key_oly = format!("{}/{}/{}", org_id, stream_type, "oly");
            let cache_key_test1 = format!("{}/{}/{}", org_id, stream_type, "test1");

            // Setup schemas
            {
                let mut w = STREAM_SCHEMAS_LATEST.write().await;
                w.insert(
                    cache_key_oly.clone(),
                    SchemaCache::new(create_olympic_schema()),
                );
                w.insert(
                    cache_key_test1.clone(),
                    SchemaCache::new(create_olympic_schema()),
                );
            }

            // Setup settings
            {
                let mut w = STREAM_SETTINGS.write().await;
                w.insert(cache_key_oly.clone(), StreamSettings::default());
                w.insert(cache_key_test1.clone(), StreamSettings::default());

                // Update atomic cache
                let mut atomic_cache = hashbrown::HashMap::new();
                atomic_cache.insert(cache_key_oly.clone(), StreamSettings::default());
                atomic_cache.insert(cache_key_test1.clone(), StreamSettings::default());
                infra::schema::set_stream_settings_atomic(atomic_cache);
            }

            TestContext {
                org_id,
                stream_type,
                cache_key_oly,
                cache_key_test1,
            }
        }
    }

    use helpers::*;

    #[tokio::test]
    async fn test_basic_select_field_in_schema() {
        let ctx = init_test_context("basic_in_schema").await;

        let sql = r#"SELECT continent FROM "oly""#;
        let result = validate_query_fields(&ctx.org_id, "oly", ctx.stream_type, sql).await;

        ctx.cleanup().await;

        assert!(result.is_ok(), "Should pass when field is in the schema");
    }

    #[tokio::test]
    async fn test_field_not_in_schema_rejected() {
        // When the validated stream is not referenced by the SQL, validation
        // falls back to checking every tracked column against that stream's
        // schema — a column belonging to another stream must be rejected.
        let ctx = init_test_context("field_not_in_schema").await;

        // "continent" exists in oly's schema, but validating against "test1"
        // after removing "continent" from test1's schema must fail.
        {
            use arrow_schema::{DataType, Field, Schema};
            use infra::schema::{STREAM_SCHEMAS_LATEST, SchemaCache};
            let slim_schema = Schema::new(vec![
                Field::new(config::TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]);
            let mut w = STREAM_SCHEMAS_LATEST.write().await;
            w.insert(ctx.cache_key_test1.clone(), SchemaCache::new(slim_schema));
        }

        let sql = r#"SELECT continent FROM "oly""#;
        let result = validate_query_fields(&ctx.org_id, "test1", ctx.stream_type, sql).await;

        ctx.cleanup().await;

        assert!(
            result.is_err(),
            "Should fail when field is not in the validated stream's schema"
        );
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("continent") && err_msg.contains("not found"),
            "Expected field-not-found error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_join_fields_in_both_schemas() {
        let ctx = init_test_context("join_in_schemas").await;

        let sql = r#"SELECT a.continent, b.name
FROM "oly" AS a
JOIN "test1" AS b ON a.unique_id = b.unique_id"#;
        let result_a = validate_query_fields(&ctx.org_id, "oly", ctx.stream_type, sql).await;
        let result_b = validate_query_fields(&ctx.org_id, "test1", ctx.stream_type, sql).await;

        ctx.cleanup().await;

        assert!(result_a.is_ok(), "oly fields are all in schema");
        assert!(result_b.is_ok(), "test1 fields are all in schema");
    }

    #[tokio::test]
    async fn test_cte_union_fields_in_schema() {
        let ctx = init_test_context("cte_union_in_schema").await;

        let sql = r#"WITH base AS (
    SELECT continent, total_medals FROM "oly"
)
SELECT continent, total_medals FROM base
UNION ALL
SELECT continent, total_medals FROM "test1""#;
        let result = validate_query_fields(&ctx.org_id, "oly", ctx.stream_type, sql).await;

        ctx.cleanup().await;

        assert!(result.is_ok(), "CTE/UNION fields are all in schema");
    }

    #[tokio::test]
    async fn test_system_fields_always_allowed() {
        let ctx = init_test_context("system_fields").await;

        // _o2_id and _original are not part of the cached schema but must be
        // accepted as system fields
        let sql = r#"SELECT _timestamp, _o2_id, _original FROM "oly""#;
        let result = validate_query_fields(&ctx.org_id, "oly", ctx.stream_type, sql).await;

        ctx.cleanup().await;

        assert!(result.is_ok(), "system fields must always be allowed");
    }
}

// ============================================================================
// Regression test: CTE multi-stream field validation
//
// Bug: validate_query_fields flattened ALL streams' columns and checked them
// against ONE stream's schema. Fields from stream B (e.g. body_type in
// k8s_events) were rejected when validating stream A (default), producing a
// false SearchFieldNotFound error.
//
// This test FAILS on the unfixed code and PASSES after the fix.
// ============================================================================
#[cfg(test)]
mod test_cte_multi_stream_regression {
    use arrow_schema::{DataType, Field, Schema};
    use config::meta::stream::{StreamSettings, StreamType};
    use infra::schema::{STREAM_SCHEMAS_LATEST, STREAM_SETTINGS, SchemaCache};

    use super::*;

    #[tokio::test]
    async fn test_cte_join_cross_stream_field_not_rejected() {
        let org_id = "test_cte_regression";
        let st = StreamType::Logs;

        // default stream: pod/container logs — does NOT have body_type
        let default_schema = Schema::new(vec![
            Field::new(config::TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("k8s_cluster", DataType::Utf8, true),
            Field::new("k8s_namespace_name", DataType::Utf8, true),
            Field::new("k8s_pod_name", DataType::Utf8, true),
            Field::new("severity", DataType::Utf8, true),
        ]);

        // k8s_events stream: has body_type — the field the old code falsely rejected
        let k8s_events_schema = Schema::new(vec![
            Field::new(config::TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("body_type", DataType::Utf8, true),
            Field::new("event_name", DataType::Utf8, true),
            Field::new("k8s_cluster", DataType::Utf8, true),
            Field::new("k8s_namespace_name", DataType::Utf8, true),
        ]);

        let key_default = format!("{org_id}/{st}/default");
        let key_k8s = format!("{org_id}/{st}/k8s_events");

        {
            let mut w = STREAM_SCHEMAS_LATEST.write().await;
            w.insert(key_default.clone(), SchemaCache::new(default_schema));
            w.insert(key_k8s.clone(), SchemaCache::new(k8s_events_schema));
        }
        {
            let mut w = STREAM_SETTINGS.write().await;
            w.insert(key_default.clone(), StreamSettings::default());
            w.insert(key_k8s.clone(), StreamSettings::default());
            let mut atomic = hashbrown::HashMap::new();
            atomic.insert(key_default.clone(), StreamSettings::default());
            atomic.insert(key_k8s.clone(), StreamSettings::default());
            infra::schema::set_stream_settings_atomic(atomic);
        }

        // Exact query shape that triggered the bug in alerts validation
        let sql = r#"
WITH pod_logs AS (
    SELECT DISTINCT k8s_pod_name, k8s_namespace_name, k8s_cluster
    FROM "default"
    WHERE severity = '0'
),
k8s_events_agg AS (
    SELECT e.k8s_cluster, e.k8s_namespace_name, e.event_name, e.body_type
    FROM "k8s_events" e
    INNER JOIN pod_logs p
        ON e.k8s_cluster = p.k8s_cluster
        AND e.k8s_namespace_name = p.k8s_namespace_name
)
SELECT k8s_cluster, k8s_namespace_name, event_name, body_type
FROM k8s_events_agg"#;

        let result = validate_query_fields(org_id, "default", st, sql).await;

        {
            let mut w = STREAM_SCHEMAS_LATEST.write().await;
            w.remove(&key_default);
            w.remove(&key_k8s);
        }
        {
            let mut w = STREAM_SETTINGS.write().await;
            w.remove(&key_default);
            w.remove(&key_k8s);
        }

        // OLD code: body_type (k8s_events field) checked against default schema → Err
        // FIXED:    only default's fields validated against default schema → Ok
        assert!(
            result.is_ok(),
            "body_type belongs to k8s_events, not default — must not be rejected \
             when validating the default stream. Got: {result:?}"
        );
    }
}
