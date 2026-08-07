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

// macOS ld cannot encode compact-unwind offsets once __eh_frame exceeds 16MB;
// harmless for a binary this size (only slows panic unwinding), so silence it.
#![allow(linker_messages)]

#[cfg(test)]
mod tests {
    use core::time;
    use std::{
        env, fs,
        net::SocketAddr,
        str,
        sync::{Arc, Once},
        thread,
    };

    use arrow_flight::flight_service_server::FlightServiceServer;
    use axum::{
        Router,
        body::Body,
        http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header},
    };
    use bytes::{Bytes, BytesMut};
    use chrono::{Duration, Utc};
    use config::{
        get_config,
        meta::{
            alerts::{Operator, QueryCondition, TriggerCondition, alert::Alert},
            dashboards::{Dashboard, v5},
            pipeline::{
                Pipeline,
                components::{DerivedStream, PipelineSource},
            },
            stream::StreamType,
            triggers::{ScheduledTriggerData, Trigger, TriggerModule, TriggerStatus},
        },
        utils::{
            enrichment_local_cache::{
                get_key, get_metadata_content, get_metadata_path, get_table_dir, get_table_path,
            },
            json,
        },
    };
    use infra::schema::{STREAM_SCHEMAS, STREAM_SCHEMAS_LATEST, STREAM_SETTINGS};
    use openobserve::{
        common::{
            infra::config::ENRICHMENT_TABLES,
            meta::{ingestion::IngestionResponse, user::UserList},
        },
        handler::{
            grpc::{auth::check_auth, flight::FlightServiceImpl},
            http::{
                self,
                models::{
                    alerts::responses::{GetAlertResponseBody, ListAlertsResponseBody},
                    destinations::{Destination, DestinationType},
                },
                router::{basic_routes, config_routes, service_routes},
            },
        },
        migration,
        service::{
            alerts::scheduler::handlers::handle_triggers,
            enrichment::storage::{Values, local},
            search::SEARCH_SERVER,
        },
    };
    use prost::Message;
    use proto::{cluster_rpc::search_server::SearchServer, prometheus_rpc};
    use serde_json::json;
    use tonic::codec::CompressionEncoding;
    use tower::ServiceExt;

    static START: Once = Once::new();

    fn setup() -> (&'static str, &'static str) {
        START.call_once(|| unsafe {
            env::set_var("ZO_ROOT_USER_EMAIL", "root@example.com");
            env::set_var("ZO_ROOT_USER_PASSWORD", "Complexpass#123");
            env::set_var("ZO_LOCAL_MODE", "true");
            env::set_var("ZO_MAX_FILE_SIZE_ON_DISK", "1");
            env::set_var("ZO_FILE_PUSH_INTERVAL", "1");
            // Move WAL files to storage quickly so tests exercise the
            // storage + .vix index path instead of only the WAL path.
            env::set_var("ZO_MAX_FILE_RETENTION_TIME", "1");
            // Under ZO_INGEST_SEGMENT_MODE (set by the caller, not here)
            // acks are ack-on-append and visibility waits for the next
            // segment flush: run the flusher at the validation floor so the
            // suite's ingest->query gaps comfortably cover the lag without a
            // per-request sync knob (removed 2026-07-31 — one segment per
            // request is a pathological file shape). Inert when segment mode
            // is off.
            env::set_var("ZO_SEGMENT_FLUSH_INTERVAL_MS", "50");
            // The suite's storage-path assertions want segments converted to
            // L0 .vix files promptly; the production claim gate (wait for a
            // full batch so per-stream files come out batch-sized) would
            // idle the tiny test flows for its full wait. Inert when segment
            // mode is off.
            env::set_var("ZO_SEGMENT_BUILD_MAX_WAIT_SECS", "0");
            // The alert-destination tests use a dummy loopback host; the SSRF
            // guard must not reject it in this trusted test environment.
            env::set_var("ZO_SSRF_ALLOW_LOOPBACK", "true");
            env::set_var("ZO_PAYLOAD_LIMIT", "209715200");
            env::set_var("ZO_JSON_LIMIT", "209715200");
            env::set_var("ZO_RESULT_CACHE_ENABLED", "false");
            env::set_var("ZO_PRINT_KEY_SQL", "true");
            env::set_var("ZO_SMTP_ENABLED", "true");
            env::set_var("ZO_CREATE_ORG_THROUGH_INGESTION", "true");

            env_logger::init_from_env(
                env_logger::Env::new().default_filter_or(&get_config().log.level),
            );

            log::info!("setup Invoked");
        });
        (
            "Authorization",
            "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=",
        )
    }

    /// Initialize test router with service and basic routes
    fn init_test_router() -> Router {
        Router::new()
            .merge(basic_routes())
            .nest("/config", config_routes())
            .nest("/api", service_routes())
    }

    /// Make a test request and return the response
    /// Segment-mode acks are ack-on-append: read-after-ingest visibility
    /// waits for the next segment flush (harness floor 50ms), so strictly
    /// back-to-back ingest->search assertions poll briefly instead of
    /// assuming memtable-instant reads. Legacy mode satisfies `is_ready` on
    /// the first attempt, making the loop free. Returns the LAST response
    /// either way — the caller's assertion still runs (and prints it) on
    /// timeout.
    async fn search_json_eventually(
        app: &Router,
        org: &str,
        headers: &HeaderMap,
        body_str: &str,
        is_ready: impl Fn(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        let mut last = serde_json::Value::Null;
        for _ in 0..50 {
            let (status, body) = make_request(
                app,
                Method::POST,
                &format!("/api/{org}/_search"),
                Some(headers.clone()),
                Some(body_str.to_string()),
            )
            .await;
            assert!(
                status.is_success(),
                "search failed: {}",
                String::from_utf8_lossy(&body)
            );
            last = serde_json::from_slice(&body).expect("search response must be JSON");
            if is_ready(&last) {
                return last;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        last
    }

    /// The compactor commit fence (heartbeat-from-claim, 2026-07-31)
    /// refuses to commit a job this node does not OWN — tests invoking
    /// merge_by_stream directly must claim exactly like run_merge does.
    async fn claim_job_for_merge(job_id: i64) {
        let claimed =
            infra::file_list::get_pending_jobs(&config::cluster::LOCAL_NODE.uuid, 20, true, 0)
                .await
                .unwrap();
        assert!(
            claimed.iter().any(|j| j.id == job_id),
            "job {job_id} must be claimable by this node: {claimed:?}"
        );
    }

    async fn make_request(
        app: &Router,
        method: Method,
        uri: &str,
        headers: Option<HeaderMap>,
        body: Option<String>,
    ) -> (StatusCode, Bytes) {
        let mut req_builder = Request::builder().method(method).uri(uri);

        if let Some(hdrs) = headers {
            for (key, value) in hdrs.iter() {
                req_builder = req_builder.header(key, value);
            }
        }

        let req = if let Some(body_str) = body {
            req_builder
                .body(Body::from(body_str))
                .expect("Failed to build request")
        } else {
            req_builder
                .body(Body::empty())
                .expect("Failed to build request")
        };

        let response = app
            .clone()
            .oneshot(req)
            .await
            .expect("Failed to execute request");

        let status = response.status();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("Failed to read response body");

        (status, body_bytes)
    }

    /// Helper to create headers with auth
    fn auth_headers(auth: (&str, &str)) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(auth.1).expect("Invalid auth header"),
        );
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers
    }

    async fn init_grpc_server() -> Result<(), anyhow::Error> {
        let cfg = get_config();
        let ip = if !cfg.grpc.addr.is_empty() {
            cfg.grpc.addr.clone()
        } else {
            "0.0.0.0".to_string()
        };
        let gaddr: SocketAddr = format!("{}:{}", ip, cfg.grpc.port).parse()?;
        let search_svc = SearchServer::new(SEARCH_SERVER.clone())
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);
        let flight_svc = FlightServiceServer::new(FlightServiceImpl)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);

        log::info!("starting gRPC server at {}", gaddr);
        tonic::transport::Server::builder()
            .layer(tonic::service::InterceptorLayer::new(check_auth))
            .add_service(search_svc)
            .add_service(flight_svc)
            .serve(gaddr)
            .await
            .expect("gRPC server init failed");
        Ok(())
    }

    async fn e2e_100_tear_down() {
        log::info!("Tear Down Invoked");
        fs::remove_dir_all("./data").expect("Delete local dir failed");
    }

    /// Helper function to flush memtable to ensure proper test isolation
    /// This prevents memtable overflow errors when running sequential ingestion tests
    async fn flush_memtable() {
        if let Err(e) = ingester::flush_all().await {
            log::warn!("Failed to flush memtable: {}", e);
        }
    }

    /// Cleanup any leftover state from previous test runs/retries.
    /// Order matters: alerts first (they reference destinations), then destinations, then
    /// templates. This ensures idempotency when tests are retried.
    async fn cleanup_previous_test_state() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);

        // 1. Delete alerts first (they reference destinations)
        // List alerts and delete by ID
        let (status, body) = make_request(
            &app,
            Method::GET,
            "/api/v2/e2e/alerts?stream_type=logs",
            Some(headers.clone()),
            None,
        )
        .await;

        if status.is_success()
            && let Ok(alerts) = serde_json::from_slice::<serde_json::Value>(&body)
            && let Some(list) = alerts.get("list").and_then(|l| l.as_array())
        {
            let alert_names = ["alertChk", "sns_test_alert", "multirange_alert"];
            for alert in list {
                if let Some(name) = alert.get("name").and_then(|n| n.as_str())
                    && alert_names.contains(&name)
                    && let Some(id) = alert.get("alert_id").and_then(|id| id.as_str())
                {
                    let _ = make_request(
                        &app,
                        Method::DELETE,
                        &format!("/api/v2/e2e/alerts/{}?type=logs", id),
                        Some(headers.clone()),
                        None,
                    )
                    .await;
                    log::info!("Cleanup: deleted alert {}", name);
                }
            }
        }

        // 2. Delete destinations (they reference templates)
        for dest in ["slack", "email", "sns_alert"] {
            let _ = make_request(
                &app,
                Method::DELETE,
                &format!("/api/e2e/alerts/destinations/{}", dest),
                Some(headers.clone()),
                None,
            )
            .await;
        }

        // 3. Delete templates last
        for template in ["slackTemplate", "email_template", "snsTemplate"] {
            let _ = make_request(
                &app,
                Method::DELETE,
                &format!("/api/e2e/alerts/templates/{}", template),
                Some(headers.clone()),
                None,
            )
            .await;
        }

        // 4. Delete user if exists
        let _ = make_request(
            &app,
            Method::DELETE,
            "/api/e2e/users/nonadmin@example.com",
            Some(headers.clone()),
            None,
        )
        .await;

        log::info!("Cleanup: finished cleaning up previous test state");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn e2e_test() {
        // make sure data dir is deleted before we run integration tests
        fs::remove_dir_all("./data")
            .unwrap_or_else(|e| log::info!("Error deleting local dir: {}", e));

        setup();

        // start gRPC server
        tokio::task::spawn(async move {
            init_grpc_server()
                .await
                .expect("router gRPC server init failed");
        });

        // register node
        openobserve::common::infra::cluster::register_and_keep_alive()
            .await
            .unwrap();
        // init config
        config::init().await.unwrap();
        // init infra
        migration::init_db().await.unwrap();
        // ensure database tables are created
        infra::db::create_table().await.unwrap();
        // db migration steps, since it's separated out
        infra::table::migrate().await.unwrap();
        infra::init().await.unwrap();
        openobserve::service::bootstrap::init().await.unwrap();
        // ingester init
        ingester::init().await.unwrap();
        // init job
        openobserve::job::init().await.unwrap();

        // Wait for async initialization tasks (like default user creation) to complete
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Clean up any leftover state from previous test runs/retries
        // This ensures tests are idempotent
        cleanup_previous_test_state().await;

        for _i in 0..3 {
            e2e_1_post_bulk().await;
        }

        // Flush memtable after bulk ingestion to ensure proper test isolation
        // This prevents memtable overflow when subsequent tests try to ingest data
        flush_memtable().await;

        // ingest
        e2e_post_json().await;
        e2e_post_multi().await;
        e2e_trace_context_canonicalization().await;
        e2e_post_trace().await;
        e2e_post_metrics().await;
        e2e_post_hec().await;
        // e2e_post_kinesis_data().await;

        // streams
        e2e_get_stream().await;
        e2e_get_stream_schema().await;
        e2e_get_org_summary().await;
        e2e_post_stream_settings().await;
        e2e_get_es_settings().await;

        // functions
        e2e_post_function().await;
        e2e_list_functions().await;
        e2e_delete_function().await;

        // search
        e2e_search().await;
        e2e_search_around().await;

        // users
        e2e_post_user().await;
        e2e_update_user().await;
        e2e_update_user_with_empty().await;
        e2e_add_user_to_org().await;
        e2e_add_root_user_to_org().await;
        e2e_list_users().await;
        e2e_get_organizations().await;
        e2e_get_user_passcode().await;
        e2e_update_user_passcode().await;
        e2e_user_authentication().await;
        e2e_user_authentication_with_error().await;

        // dashboards
        {
            let board = e2e_create_dashboard().await;
            let list = e2e_list_dashboards().await;
            assert_eq!(list[0], board);

            let mut v5_board = board.v5.unwrap();
            v5_board.title = "e2e test".to_owned();
            v5_board.description = "Logs flow downstream".to_owned();

            let board = e2e_update_dashboard(v5_board, board.hash).await;
            assert_eq!(
                e2e_get_dashboard(&board.v5.as_ref().unwrap().dashboard_id).await,
                board
            );
            e2e_delete_dashboard(&board.v5.unwrap().dashboard_id).await;
            assert!(e2e_list_dashboards().await.is_empty());
        }

        // alert
        e2e_post_alert_template().await;
        e2e_get_alert_template().await;
        e2e_list_alert_template().await;
        e2e_post_alert_destination().await;
        e2e_get_alert_destination().await;
        e2e_list_alert_destinations().await;
        e2e_post_alert_multirange().await;
        e2e_delete_alert_multirange().await;
        e2e_post_alert().await;
        e2e_get_alert().await;
        e2e_handle_alert_after_destination_retries().await;
        e2e_handle_alert_after_evaluation_retries().await;
        e2e_handle_alert_reached_max_retries().await;
        e2e_list_alerts().await;
        e2e_list_real_time_alerts().await;
        e2e_delete_alert().await;
        e2e_delete_alert_destination().await;
        e2e_delete_alert_template().await;

        // Email-specific alert tests
        e2e_post_alert_email_template().await;
        e2e_get_alert_email_template().await;
        e2e_post_alert_email_destination().await;
        e2e_get_alert_email_destination().await;
        e2e_post_alert_email_destination_should_fail().await;

        e2e_delete_alert_email_destination().await;
        e2e_delete_alert_email_template().await;

        // Clean-up user here after email destinations deleted
        e2e_delete_user().await;

        // SNS-specific alert tests
        // Set up templates
        e2e_post_alert_template().await;
        e2e_post_sns_alert_template().await;

        // SNS destination tests
        e2e_post_sns_alert_destination().await;
        e2e_get_sns_alert_destination().await;
        e2e_list_alert_destinations_with_sns().await;
        e2e_update_sns_alert_destination().await;

        // Create and test alert with SNS destination
        e2e_post_alert_with_sns_destination().await;

        // Cleanup
        e2e_delete_alert_with_sns_destination().await;
        e2e_delete_sns_alert_destination().await;

        // derived streams
        e2e_create_test_pipeline().await;
        e2e_handle_derived_stream_success().await;
        e2e_handle_derived_stream_pipeline_not_found().await;
        e2e_handle_derived_stream_max_retries().await;
        test_derived_stream_invalid_timerange_delay_scenario().await;
        test_derived_stream_invalid_timerange_with_cron_frequency().await;
        e2e_handle_derived_stream_evaluation_failure().await;
        e2e_cleanup_test_pipeline().await;

        // enrichment table
        test_enrichment_table_integration().await;
        test_enrichment_table_local_all_sequential().await;

        // backfill jobs
        test_backfill_job_list_and_delete().await;
        test_backfill_job_get_nonexistent().await;
        test_backfill_job_delete_by_pipeline().await;
        test_backfill_job_enable_disable().await;

        // vix index end-to-end (self-contained stream; runs last so its
        // ingest/flush timing cannot shift the window-sensitive scheduler
        // tests above)
        e2e_vix_index_search().await;

        // single-file healing compaction (self-contained stream; after the
        // vix step for the same window-timing reason)
        e2e_single_file_healing_compaction().await;

        // others
        e2e_health_check().await;
        e2e_config().await;
        e2e_100_tear_down().await;

        // clear
        e2e_delete_stream().await;
    }

    async fn e2e_1_post_bulk() {
        let auth = setup();
        let path = "./tests/input.json";
        let body_str = fs::read_to_string(path).expect("Read file failed");
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/_bulk", "e2e"),
            Some(headers),
            Some(body_str),
        )
        .await;
        if !status.is_success() {
            let body_str = String::from_utf8_lossy(&body);
            panic!(
                "e2e_1_post_bulk failed with status {}: {}",
                status, body_str
            );
        }
    }

    async fn e2e_post_json() {
        let auth = setup();

        let app = init_test_router();
        let headers = auth_headers(auth);

        // timestamp in past
        let body_str = "[{\"Year\": 1896, \"City\": \"Athens\", \"Sport\": \"Aquatics\", \"Discipline\": \"Swimming\", \"Athlete\": \"HERSCHMANN, Otto\", \"Country\": \"AUT\", \"Gender\": \"Men\", \"Event\": \"100M Freestyle\", \"Medal\": \"Silver\", \"Season\": \"summer\",\"_timestamp\":1665136888163792}]";
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/{}/_json", "e2e", "olympics_schema"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
        let res: IngestionResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(res.code, 200);
        assert_eq!(res.status.len(), 1);
        assert_eq!(res.status[0].status.successful, 0);
        assert_eq!(res.status[0].status.failed, 1);
        assert!(res.status[0].status.error.contains("Too old data"));
        assert!(
            res.status[0]
                .status
                .error
                .contains("ZO_INGEST_ALLOWED_UPTO=")
        );

        // timestamp in future
        let body_str = "[{\"Year\": 1896, \"City\": \"Athens\", \"Sport\": \"Aquatics\", \"Discipline\": \"Swimming\", \"Athlete\": \"HERSCHMANN, Otto\", \"Country\": \"AUT\", \"Gender\": \"Men\", \"Event\": \"100M Freestyle\", \"Medal\": \"Silver\", \"Season\": \"summer\",\"_timestamp\":9999999999999999}]";
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/{}/_json", "e2e", "olympics_schema"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
        let res: IngestionResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(res.code, 200);
        assert_eq!(res.status.len(), 1);
        assert_eq!(res.status[0].status.successful, 0);
        assert_eq!(res.status[0].status.failed, 1);
        assert!(res.status[0].status.error.contains("Too far data"));
        assert!(
            res.status[0]
                .status
                .error
                .contains("ZO_INGEST_ALLOWED_IN_FUTURE=")
        );

        // timestamp not present
        let body_str = "[{\"Year\": 1896, \"City\": \"Athens\", \"Sport\": \"Aquatics\", \"Discipline\": \"Swimming\", \"Athlete\": \"HERSCHMANN, Otto\", \"Country\": \"AUT\", \"Gender\": \"Men\", \"Event\": \"100M Freestyle\", \"Medal\": \"Silver\", \"Season\": \"summer\"}]";
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/{}/_json", "e2e", "olympics_schema"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
        let res: IngestionResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(res.code, 200);
        assert_eq!(res.status.len(), 1);
        assert_eq!(res.status[0].status.successful, 1);
        assert_eq!(res.status[0].status.failed, 0);

        // timestamp just right
        let ts = chrono::Utc::now().timestamp_micros();
        let body_str = format!(
            "[{{\"Year\": 1896, \"City\": \"Athens\", \"Sport\": \"Aquatics\", \"Discipline\": \"Swimming\", \"Athlete\": \"HERSCHMANN, Otto\", \"Country\": \"AUT\", \"Gender\": \"Men\", \"Event\": \"100M Freestyle\", \"Medal\": \"Silver\", \"Season\": \"summer\",\"_timestamp\":{ts}}}]"
        );
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/{}/_json", "e2e", "olympics_schema"),
            Some(headers.clone()),
            Some(body_str),
        )
        .await;
        assert!(status.is_success());
        let res: IngestionResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(res.code, 200);
        assert_eq!(res.status.len(), 1);
        assert_eq!(res.status[0].status.successful, 1);
        assert_eq!(res.status[0].status.failed, 0);
    }

    async fn e2e_post_hec() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);

        // test case : missing index in metadata
        let body_str = "{\"event\":\"hello\"}";
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/_hec", "e2e"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());

        // test case : valid payload
        let body_str = "{\"event\":\"hello\",\"index\":\"hec_test\"}";
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/_hec", "e2e"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());

        // test case : json event
        let body_str =
            "{\"event\":{\"log\":\"hello\",\"severity\":\"info\"},\"index\":\"hec_test\"}";
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/_hec", "e2e"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());

        // test case : ndjson
        let body_str = r#"
                { "index": "hec_test", "event": "test log", "time": 1749113798091 }
                { "index": "hec_test", "event": {"log":"test log","severity":"info"}, "fields": {"cluster":"c1", "namespace":"n1"} }
                { "index": "hec_test", "event": {"log":"test log","severity":"info"}, "source" : "e2e_test", "fields": {"cluster":"c1", "namespace":"n1"}}
            "#;
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/_hec", "e2e"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_multi() {
        let auth = setup();
        let body_str = "{\"Year\": 1896, \"City\": \"Athens\", \"Sport\": \"Aquatics\", \"Discipline\": \"Swimming\", \"Athlete\": \"HERSCHMANN, Otto\", \"Country\": \"AUT\", \"Gender\": \"Men\", \"Event\": \"100M Freestyle\", \"Medal\": \"Silver\", \"Season\": \"summer\",\"_timestamp\":1665136888163792}";
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/{}/_multi", "e2e", "olympics_schema"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    /// Ingest-path proof of the reserved trace-context canonicalization
    /// (`flatten::canonicalize_reserved_aliases`, applied in the write_logs
    /// funnel): a record carrying NESTED trace context (`{"trace":{"id":..}}`
    /// — the shape collector-side OTTL misses) plus a literal dotted span
    /// alias stores the canonical `trace_id`/`span_id` and no dotted
    /// `trace.id`/`span.id`; when both forms arrive, the canonical field
    /// wins; unrelated dotted fields keep the dotted canon.
    async fn e2e_trace_context_canonicalization() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);

        let now = Utc::now().timestamp_micros();
        let records = serde_json::json!([
            {
                "_timestamp": now,
                "log": "canon nested",
                "trace": {"id": "tid-nested-1"},
                "span.id": "sid-literal-1",
                "service.name": "canon-svc",
            },
            {
                "_timestamp": now - 1_000,
                "log": "canon conflict",
                "trace.id": "loser",
                "trace_id": "winner",
            },
        ]);
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/{}/_json", "e2e", "trace_ctx_canon"),
            Some(headers.clone()),
            Some(records.to_string()),
        )
        .await;
        assert!(
            status.is_success(),
            "trace_ctx_canon ingest failed: {}",
            String::from_utf8_lossy(&body)
        );

        // The stream schema is inferred from the canonicalized records
        // (canonicalization runs before check_for_schema), so it must know
        // the canonical fields and never the dotted aliases.
        let (status, body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/streams/{}/schema", "e2e", "trace_ctx_canon"),
            Some(headers.clone()),
            None,
        )
        .await;
        assert!(
            status.is_success(),
            "trace_ctx_canon schema fetch failed: {}",
            String::from_utf8_lossy(&body)
        );
        let stream: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let fields: Vec<&str> = stream["schema"]
            .as_array()
            .expect("stream schema must be an array")
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        for canonical in ["trace_id", "span_id", "service.name"] {
            assert!(
                fields.contains(&canonical),
                "schema must contain {canonical:?}: {fields:?}"
            );
        }
        for dotted in ["trace.id", "span.id"] {
            assert!(
                !fields.contains(&dotted),
                "schema must not learn the dotted alias {dotted:?}: {fields:?}"
            );
        }

        // The stored records: hits carry only the canonical fields.
        let body_str = serde_json::json!({
            "query": {
                "sql": "select * from trace_ctx_canon",
                "from": 0,
                "size": 10,
                "start_time": now - 3_600_000_000i64,
                "end_time": now + 3_600_000_000i64,
            }
        })
        .to_string();
        let res = search_json_eventually(&app, "e2e", &headers, &body_str, |r| {
            r["hits"].as_array().map(|h| h.len()) == Some(2)
        })
        .await;
        let hits = res["hits"].as_array().cloned().unwrap_or_default();
        assert_eq!(hits.len(), 2, "expected both canon records: {res}");
        for hit in &hits {
            let obj = hit.as_object().unwrap();
            assert!(
                !obj.contains_key("trace.id") && !obj.contains_key("span.id"),
                "stored record must not carry a dotted trace-context field: {hit}"
            );
            match obj.get("log").and_then(|v| v.as_str()) {
                Some("canon nested") => {
                    assert_eq!(
                        obj.get("trace_id").and_then(|v| v.as_str()),
                        Some("tid-nested-1"),
                        "{hit}"
                    );
                    assert_eq!(
                        obj.get("span_id").and_then(|v| v.as_str()),
                        Some("sid-literal-1"),
                        "{hit}"
                    );
                    assert_eq!(
                        obj.get("service.name").and_then(|v| v.as_str()),
                        Some("canon-svc"),
                        "{hit}"
                    );
                }
                Some("canon conflict") => {
                    // both forms arrived: the canonical field wins
                    assert_eq!(
                        obj.get("trace_id").and_then(|v| v.as_str()),
                        Some("winner"),
                        "{hit}"
                    );
                }
                other => panic!("unexpected hit log value {other:?}: {hit}"),
            }
        }
    }

    async fn e2e_get_stream() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/streams", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_get_stream_schema() {
        let auth = setup();
        let one_sec = time::Duration::from_secs(2);
        thread::sleep(one_sec);
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/streams/{}/schema", "e2e", "olympics_schema"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_stream_settings() {
        let auth = setup();
        let body_str = r#"{"partition_keys":{"add":[{"field":"test_key"}],"remove":[]}, "full_text_search_keys":{"add":["city"],"remove":[]}}"#;
        // app
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::PUT,
            &format!("/api/{}/streams/{}/settings", "e2e", "olympics_schema"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_delete_stream() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/{}/streams/{}", "e2e", "olympics_schema"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_get_es_settings() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_function() {
        let auth = setup();
        let body_str = r#"{
            "name": "e2etestfn",
            "function":".sqNew,err = .Year*.Year \n .",
            "params":"row"
        }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/functions", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        if !status.is_success() {
            let body_str = String::from_utf8_lossy(&body);
            println!("e2e_post_function response status: {status:?}");
            println!("e2e_post_function response body: {body_str:?}");

            // If function already exists, that's OK for our test
            if status == StatusCode::BAD_REQUEST && body_str.contains("Function already exist") {
                println!("Function already exists, continuing with test");
                return;
            }

            panic!("e2e_post_function failed with status: {status:?}");
        }
        assert!(status.is_success());
    }

    async fn e2e_list_functions() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/functions", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_delete_function() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/{}/functions/{}", "e2e", "e2etestfn"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_search() {
        let auth = setup();
        let body_str = r#"{
            "query": {
                "sql": "select * from olympics_schema",
                "from": 0,
                "size": 100,
                "start_time": 1714857600000,
                "end_time": 1714944000000
            }
        }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/_search", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_search_around() {
        let auth = setup();

        let app = init_test_router();
        let ts = chrono::Utc::now().timestamp_micros();

        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!(
                "/api/{}/{}/_around?key={}&size=10",
                "e2e", "olympics_schema", ts
            ),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    /// Recursively collect files under `dir` whose path ends with `ext` and
    /// contains `needle`.
    fn find_files_with_ext(dir: &std::path::Path, ext: &str, needle: &str, out: &mut Vec<String>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    find_files_with_ext(&path, ext, needle, out);
                } else if let Some(p) = path.to_str()
                    && p.ends_with(ext)
                    && p.contains(needle)
                {
                    out.push(p.to_string());
                }
            }
        }
    }

    /// End-to-end check of the `.vix` index + core files: ingest a
    /// dedicated stream, wait for the WAL→storage move job
    /// (ZO_FILE_PUSH_INTERVAL=1) to write the core `.vix` object
    /// (logs/traces are always core files: records + index in ONE object,
    /// no parquet data file and no sibling index), then verify that term
    /// (equality on a non-FTS field), full-text (match_all), and count
    /// queries return the expected rows through the core-file scan path.
    /// A second stream (vixtest_dotted) checks the dotted-field roundtrip:
    /// nested ingest keeps `.` in field names, unquoted `http.status`
    /// resolves to the field, and hits materialize it from `_source`.
    async fn e2e_vix_index_search() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);

        // 10 records: 3 "error" rows carry a unique token in `log` (a default
        // FTS field), 7 "info" rows do not.
        let now = Utc::now().timestamp_micros();
        let mut records = Vec::new();
        for i in 0..10i64 {
            let (level, log) = if i < 3 {
                ("error", format!("request failed vixsmoketoken42 case {i}"))
            } else {
                ("info", format!("request ok case {i}"))
            };
            let mut record = serde_json::json!({
                "_timestamp": now - i * 1_000,
                "level": level,
                "service": format!("svc-{}", i % 2),
                "log": log,
            });
            if level == "error" {
                // only error rows carry err_code: the 7 info rows leave it
                // absent, which the index must treat as NULL (no key term)
                record["err_code"] = serde_json::json!("E42");
            }
            records.push(record);
        }
        let body_str = serde_json::to_string(&records).unwrap();
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/{}/_json", "e2e", "vixtest"),
            Some(headers.clone()),
            Some(body_str),
        )
        .await;
        assert!(
            status.is_success(),
            "vixtest ingest failed: {}",
            String::from_utf8_lossy(&body)
        );

        // Push the memtable to WAL, then wait for the move job to build the
        // index. The .vix object appearing in the local object store proves
        // the write path ran and the data is served from storage.
        flush_memtable().await;
        let mut vix_files = Vec::new();
        for _ in 0..60 {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            vix_files.clear();
            find_files_with_ext(
                std::path::Path::new("./data"),
                ".vix",
                "vixtest",
                &mut vix_files,
            );
            if !vix_files.is_empty() {
                break;
            }
        }
        assert!(
            !vix_files.is_empty(),
            "no .vix index file was created for stream vixtest within 30s"
        );

        let start_time = now - 3_600_000_000i64;
        let end_time = now + 3_600_000_000i64;
        let run_query = |sql: &str| {
            let body_str = serde_json::json!({
                "query": {
                    "sql": sql,
                    "from": 0,
                    "size": 100,
                    "start_time": start_time,
                    "end_time": end_time
                }
            })
            .to_string();
            let headers = headers.clone();
            let app = &app;
            async move {
                let (status, body) = make_request(
                    app,
                    Method::POST,
                    &format!("/api/{}/_search", "e2e"),
                    Some(headers),
                    Some(body_str),
                )
                .await;
                assert!(
                    status.is_success(),
                    "search failed: {}",
                    String::from_utf8_lossy(&body)
                );
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()
            }
        };

        // Term index: equality on a plain string field (not FTS-listed).
        let res = run_query("select * from vixtest where level = 'error'").await;
        assert_eq!(
            res["hits"].as_array().map(|h| h.len()),
            Some(3),
            "equality on term-indexed field returned wrong rows: {res}"
        );

        // Full-text index: match_all over tokenized FTS fields.
        let res = run_query("select * from vixtest where match_all('vixsmoketoken42')").await;
        assert_eq!(
            res["hits"].as_array().map(|h| h.len()),
            Some(3),
            "match_all returned wrong rows: {res}"
        );

        // Count fast path (SimpleCount over the index).
        let res = run_query("select count(*) as cnt from vixtest where level = 'info'").await;
        let cnt = res["hits"][0]["cnt"].as_i64().or_else(|| {
            res["hits"][0]["cnt"]
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
        });
        assert_eq!(cnt, Some(7), "count query returned wrong value: {res}");

        // IS NULL rides the key-term index (Condition::IsNull = exact
        // complement of KeyExists): rows where err_code is absent are the 7
        // info rows. Covers the SimpleCount shape...
        let res = run_query("select count(*) as cnt from vixtest where err_code is null").await;
        let cnt = res["hits"][0]["cnt"].as_i64().or_else(|| {
            res["hits"][0]["cnt"]
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
        });
        assert_eq!(cnt, Some(7), "IS NULL count returned wrong value: {res}");

        // ...the SimpleSelect (star + ORDER BY _timestamp + LIMIT) shape...
        let res = run_query(
            "select * from vixtest where err_code is null order by _timestamp desc limit 5",
        )
        .await;
        let hits = res["hits"].as_array().cloned().unwrap_or_default();
        assert_eq!(hits.len(), 5, "IS NULL select returned wrong rows: {res}");
        for hit in &hits {
            assert!(
                hit.get("err_code").is_none_or(|v| v.is_null()),
                "IS NULL select returned a row with err_code set: {hit}"
            );
        }

        // ...and the complement stays exact.
        let res = run_query("select count(*) as cnt from vixtest where err_code is not null").await;
        let cnt = res["hits"][0]["cnt"].as_i64().or_else(|| {
            res["hits"][0]["cnt"]
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
        });
        assert_eq!(
            cnt,
            Some(3),
            "IS NOT NULL count returned wrong value: {res}"
        );

        // Histogram fast path (SimpleHistogram over the index), in the shape
        // the UI issues it: histogram(_timestamp) + count(*) grouped by the
        // bucket. Bucket counts must sum to the 3 first-batch error rows.
        let res = run_query(
            "select histogram(_timestamp, '30 second') as zo_sql_key, count(*) as zo_sql_num \
             from vixtest where level = 'error' group by zo_sql_key order by zo_sql_key",
        )
        .await;
        let hits = res["hits"].as_array().cloned().unwrap_or_default();
        let total: i64 = hits
            .iter()
            .map(|hit| {
                hit["zo_sql_num"]
                    .as_i64()
                    .or_else(|| {
                        hit["zo_sql_num"]
                            .as_str()
                            .and_then(|s| s.parse::<i64>().ok())
                    })
                    .unwrap_or_default()
            })
            .sum();
        assert_eq!(
            total, 3,
            "first-batch histogram bucket counts must sum to the 3 error rows: {res}"
        );

        // -----------------------------------------------------------------
        // core-file shape: exactly ONE object per file unit. Everything
        // in the vixtest storage prefix is a `.vix` core file (no parquet /
        // vortex data files) and there is NO sibling index object.
        // -----------------------------------------------------------------
        let mut stream_objects = Vec::new();
        find_files_with_ext(
            std::path::Path::new("./data/openobserve/stream/files/e2e/logs/vixtest"),
            "",
            "",
            &mut stream_objects,
        );
        assert!(
            !stream_objects.is_empty(),
            "no storage objects found for stream vixtest"
        );
        for object in &stream_objects {
            assert!(
                object.ends_with(".vix"),
                "vixtest must be stored as core .vix objects only, found: {object}"
            );
        }
        let mut sibling_indexes = Vec::new();
        find_files_with_ext(
            std::path::Path::new("./data/openobserve/stream/files/e2e/index"),
            "",
            "vixtest",
            &mut sibling_indexes,
        );
        assert!(
            sibling_indexes.is_empty(),
            "core files must not have sibling index objects, found: {sibling_indexes:?}"
        );

        // -----------------------------------------------------------------
        // Dotted-field roundtrip on a second stream: nested ingest keeps `.`
        // in flattened field names; equality on the dotted field works both
        // quoted and unquoted; hits carry the dotted field from `_source`.
        // -----------------------------------------------------------------
        let mut records = Vec::new();
        for i in 0..4i64 {
            let status = if i < 2 { "500" } else { "200" };
            records.push(serde_json::json!({
                "_timestamp": now - i * 1_000,
                "log": format!("nested case {i}"),
                "http": { "status": status },
            }));
        }
        // a record without the `http` key: its flattened record carries no
        // `http.status` path, so the key-existence term skips it (IS NOT NULL)
        records.push(serde_json::json!({
            "_timestamp": now - 4_000,
            "log": "nested case 4 no http",
        }));
        let body_str = serde_json::to_string(&records).unwrap();
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/{}/_json", "e2e", "vixtest_dotted"),
            Some(headers.clone()),
            Some(body_str),
        )
        .await;
        assert!(
            status.is_success(),
            "vixtest_dotted ingest failed: {}",
            String::from_utf8_lossy(&body)
        );

        flush_memtable().await;
        let mut v2_files = Vec::new();
        for _ in 0..60 {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            v2_files.clear();
            find_files_with_ext(
                std::path::Path::new("./data/openobserve/stream/files/e2e/logs/vixtest_dotted"),
                ".vix",
                "",
                &mut v2_files,
            );
            if !v2_files.is_empty() {
                break;
            }
        }
        assert!(
            !v2_files.is_empty(),
            "no core .vix file was created for stream vixtest_dotted within 60s"
        );

        for sql in [
            // quoted dotted identifier works natively
            "select * from vixtest_dotted where \"http.status\" = '500'",
            // unquoted dotted identifier resolves to the field (§15.5)
            "select * from vixtest_dotted where http.status = '500'",
        ] {
            let res = run_query(sql).await;
            let hits = res["hits"].as_array().cloned().unwrap_or_default();
            assert_eq!(
                hits.len(),
                2,
                "dotted-field query {sql:?} wrong rows: {res}"
            );
            for hit in &hits {
                assert_eq!(
                    hit["http.status"].as_str(),
                    Some("500"),
                    "hit is missing the dotted field extracted from _source: {hit}"
                );
                assert!(
                    hit["log"]
                        .as_str()
                        .is_some_and(|log| log.starts_with("nested case")),
                    "hit is missing the log field: {hit}"
                );
            }
        }

        // IS NOT NULL answers from the core-file key-existence terms
        // (VixQuery::KeyExists): 4 of the 5 records carry the `http.status`
        // path, the no-http record does not.
        for sql in [
            "select count(*) as cnt from vixtest_dotted where \"http.status\" is not null",
            "select count(*) as cnt from vixtest_dotted where http.status is not null",
        ] {
            let res = run_query(sql).await;
            let cnt = res["hits"][0]["cnt"].as_i64().or_else(|| {
                res["hits"][0]["cnt"]
                    .as_str()
                    .and_then(|s| s.parse::<i64>().ok())
            });
            assert_eq!(
                cnt,
                Some(4),
                "IS NOT NULL query {sql:?} returned wrong count: {res}"
            );
        }

        // and the second stream also stores exactly one object per file unit
        let mut v2_objects = Vec::new();
        find_files_with_ext(
            std::path::Path::new("./data/openobserve/stream/files/e2e/logs/vixtest_dotted"),
            "",
            "",
            &mut v2_objects,
        );
        for object in &v2_objects {
            assert!(
                object.ends_with(".vix"),
                "vixtest_dotted must be stored as core .vix objects only, found: {object}"
            );
        }
        let mut v2_siblings = Vec::new();
        find_files_with_ext(
            std::path::Path::new("./data/openobserve/stream/files/e2e/index"),
            "",
            "vixtest_dotted",
            &mut v2_siblings,
        );
        assert!(
            v2_siblings.is_empty(),
            "core files must not have sibling index objects, found: {v2_siblings:?}"
        );

        // -----------------------------------------------------------------
        // Aggregation fast paths (P3b): add "service" to column_store_fields,
        // ingest a second batch (its files carry the service docs column and
        // serve the fast paths), then assert CORRECT results for a
        // TopN-shaped and a histogram-shaped query. Correctness is the
        // assertion — fast path or fallback are both acceptable (first-batch
        // files lack the service docs column, and the per-file capability
        // probe routes them to the DataFusion branch).
        // -----------------------------------------------------------------
        let body_str = r#"{"column_store_fields":{"add":["service"],"remove":[]}}"#;
        let (status, body) = make_request(
            &app,
            Method::PUT,
            &format!("/api/{}/streams/{}/settings", "e2e", "vixtest"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        assert!(
            status.is_success(),
            "vixtest settings update failed: {}",
            String::from_utf8_lossy(&body)
        );

        // second batch: 8 records newer than the settings change.
        // service totals across BOTH batches: svc-0 = 5+2 = 7, svc-2 = 6,
        // svc-1 = 5; level "error" totals: 3 + 2 = 5.
        let now2 = Utc::now().timestamp_micros();
        let mut records = Vec::new();
        for i in 0..8i64 {
            let service = if i < 6 { "svc-2" } else { "svc-0" };
            let level = if i < 2 { "error" } else { "info" };
            records.push(serde_json::json!({
                "_timestamp": now2 + i * 1_000,
                "level": level,
                "service": service,
                "log": format!("second batch case {i}"),
            }));
        }
        let body_str = serde_json::to_string(&records).unwrap();
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/{}/_json", "e2e", "vixtest"),
            Some(headers.clone()),
            Some(body_str),
        )
        .await;
        assert!(
            status.is_success(),
            "vixtest second ingest failed: {}",
            String::from_utf8_lossy(&body)
        );

        // count only the vixtest stream directory (vixtest_dotted is a sibling)
        let vixtest_dir = std::path::Path::new("./data/openobserve/stream/files/e2e/logs/vixtest");
        let mut batch2_files = Vec::new();
        find_files_with_ext(vixtest_dir, ".vix", "", &mut batch2_files);
        let prev_vix_files = batch2_files.len();
        flush_memtable().await;
        for _ in 0..60 {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            batch2_files.clear();
            find_files_with_ext(vixtest_dir, ".vix", "", &mut batch2_files);
            if batch2_files.len() > prev_vix_files {
                break;
            }
        }
        assert!(
            batch2_files.len() > prev_vix_files,
            "no new .vix file appeared for the second vixtest ingest within 60s"
        );

        // TopN-shaped query: group by the column-store field, order by count.
        // Retry briefly: the freshly moved file needs its file_list row.
        let topn_sql = "select service, count(*) as cnt from vixtest group by service order by cnt desc limit 10";
        let expected_topn: Vec<(&str, i64)> = vec![("svc-0", 7), ("svc-2", 6), ("svc-1", 5)];
        let mut got_topn: Vec<(String, i64)> = Vec::new();
        for _ in 0..15 {
            let res = run_query(topn_sql).await;
            got_topn = res["hits"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|hit| {
                    (
                        hit["service"].as_str().unwrap_or_default().to_string(),
                        hit["cnt"]
                            .as_i64()
                            .or_else(|| hit["cnt"].as_str().and_then(|s| s.parse::<i64>().ok()))
                            .unwrap_or_default(),
                    )
                })
                .collect();
            if got_topn.len() == expected_topn.len()
                && got_topn
                    .iter()
                    .zip(expected_topn.iter())
                    .all(|(g, e)| g.0 == e.0 && g.1 == e.1)
            {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        assert_eq!(
            got_topn
                .iter()
                .map(|(s, c)| (s.as_str(), *c))
                .collect::<Vec<_>>(),
            expected_topn,
            "topn query returned wrong groups/counts"
        );

        // Unfiltered TopN/Distinct over `level` — a term-indexed field that
        // is NOT column-stored (pilot fix B: served from the term dictionary
        // alone on index-eligible files, scan fallback elsewhere; correctness
        // is the assertion). Totals across both batches: info 13, error 5.
        let res = run_query(
            "select level, count(*) as cnt from vixtest group by level order by cnt desc limit 10",
        )
        .await;
        let got: Vec<(String, i64)> = res["hits"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|hit| {
                (
                    hit["level"].as_str().unwrap_or_default().to_string(),
                    hit["cnt"]
                        .as_i64()
                        .or_else(|| hit["cnt"].as_str().and_then(|s| s.parse::<i64>().ok()))
                        .unwrap_or_default(),
                )
            })
            .collect();
        assert_eq!(
            got.iter()
                .map(|(s, c)| (s.as_str(), *c))
                .collect::<Vec<_>>(),
            vec![("info", 13), ("error", 5)],
            "term-only topn query returned wrong groups/counts: {res}"
        );
        let res = run_query("select distinct level from vixtest order by level asc limit 10").await;
        let values: Vec<String> = res["hits"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|hit| hit["level"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            values,
            vec!["error", "info"],
            "term-only distinct query returned wrong values: {res}"
        );

        // Histogram-shaped query with a term filter: bucket counts over the
        // filtered rows must sum to the exact number of error records.
        let histogram_sql = "select histogram(_timestamp, '30 second') as zo_sql_key, count(*) as zo_sql_num \
             from vixtest where level = 'error' group by zo_sql_key order by zo_sql_key";
        let res = run_query(histogram_sql).await;
        let hits = res["hits"].as_array().cloned().unwrap_or_default();
        assert!(
            !hits.is_empty(),
            "histogram query returned no buckets: {res}"
        );
        let total: i64 = hits
            .iter()
            .map(|hit| {
                hit["zo_sql_num"]
                    .as_i64()
                    .or_else(|| {
                        hit["zo_sql_num"]
                            .as_str()
                            .and_then(|s| s.parse::<i64>().ok())
                    })
                    .unwrap_or_default()
            })
            .sum();
        assert_eq!(
            total, 5,
            "histogram bucket counts must sum to the 5 error rows: {res}"
        );
        for hit in &hits {
            assert!(
                !hit["zo_sql_key"].is_null(),
                "histogram bucket key missing: {hit}"
            );
        }

        // Distinct-shaped queries (SimpleDistinct): values from BOTH batches
        // (svc-1 exists only in the first batch, which serves it through the
        // scan fallback; svc-2 only in the second, served from the index).
        let res =
            run_query("select distinct service from vixtest order by service asc limit 10").await;
        let values: Vec<String> = res["hits"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|hit| hit["service"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            values,
            vec!["svc-0", "svc-1", "svc-2"],
            "distinct asc query returned wrong values: {res}"
        );
        let res =
            run_query("select distinct service from vixtest order by service desc limit 2").await;
        let values: Vec<String> = res["hits"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|hit| hit["service"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            values,
            vec!["svc-2", "svc-1"],
            "distinct desc/limit query returned wrong values: {res}"
        );

        // SimpleSelect-shaped queries (ORDER BY _timestamp DESC LIMIT n):
        // the bare shape returns the 5 newest rows overall...
        let res = run_query("select * from vixtest order by _timestamp desc limit 5").await;
        let logs: Vec<String> = res["hits"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|hit| hit["log"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            logs,
            (3..8i64)
                .rev()
                .map(|i| format!("second batch case {i}"))
                .collect::<Vec<_>>(),
            "select+sort+limit query returned wrong rows: {res}"
        );
        // ...and the filtered shape drives the vix SimpleSelect fast path
        // (per-file candidates + global top-N merge across files from both
        // batches: 2 error rows in the second batch, 3 in the first).
        let res = run_query(
            "select * from vixtest where level = 'error' order by _timestamp desc limit 5",
        )
        .await;
        let logs: Vec<String> = res["hits"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|hit| hit["log"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            logs,
            vec![
                "second batch case 1".to_string(),
                "second batch case 0".to_string(),
                "request failed vixsmoketoken42 case 0".to_string(),
                "request failed vixsmoketoken42 case 1".to_string(),
                "request failed vixsmoketoken42 case 2".to_string(),
            ],
            "filtered select+sort+limit query returned wrong rows: {res}"
        );
    }

    /// Single-file healing rebuild through the REAL compaction job flow
    /// (`merge_by_stream` + the merge worker + the file_list commit): a
    /// settled hour partition holding exactly ONE core file — a shape merge
    /// grouping could never touch — is probed against the current stream
    /// settings. A CURRENT file is a NO-OP (job completes, file untouched,
    /// nothing written); after a `column_store_fields` settings change the
    /// file is REBUILT through the normal merge commit (input replaced by
    /// one healed output, query results identical); a further job over the
    /// healed file is a no-op again (healing converges).
    async fn e2e_single_file_healing_compaction() {
        use chrono::TimeZone;
        use config::utils::time::hour_micros;
        use openobserve::service::compact::{merge::merge_by_stream, worker::MergeWorker};

        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);

        // One batch into a fresh stream, anchored mid-hour TWO hours ago:
        // the hour is settled (non-incremental) and its partition ends up
        // with exactly one .vix file.
        let now = Utc::now().timestamp_micros();
        let two_hours_ago = now - 2 * hour_micros(1);
        let anchor = two_hours_ago - (two_hours_ago % hour_micros(1)) + hour_micros(1) / 2;
        let mut records = Vec::new();
        for i in 0..6i64 {
            let (level, log) = if i < 2 {
                ("error", format!("healme healtoken77 case {i}"))
            } else {
                ("info", format!("healme ok case {i}"))
            };
            records.push(serde_json::json!({
                "_timestamp": anchor + i * 1_000,
                "level": level,
                "service": format!("svc-{}", i % 2),
                "log": log,
            }));
        }
        let body_str = serde_json::to_string(&records).unwrap();
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/{}/_json", "e2e", "healtest"),
            Some(headers.clone()),
            Some(body_str),
        )
        .await;
        assert!(
            status.is_success(),
            "healtest ingest failed: {}",
            String::from_utf8_lossy(&body)
        );

        flush_memtable().await;
        let healtest_dir =
            std::path::Path::new("./data/openobserve/stream/files/e2e/logs/healtest");
        let mut disk_objects = Vec::new();
        for _ in 0..60 {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            disk_objects.clear();
            find_files_with_ext(healtest_dir, ".vix", "", &mut disk_objects);
            if !disk_objects.is_empty() {
                break;
            }
        }
        assert_eq!(
            disk_objects.len(),
            1,
            "healtest must settle to exactly ONE .vix file, got {disk_objects:?}"
        );

        // the compactor's view of the hour partition
        let hour = Utc
            .timestamp_nanos(anchor * 1000)
            .format("%Y/%m/%d/%H")
            .to_string();
        let query_partition = || async {
            openobserve::service::file_list::query_for_merge(
                "e2e",
                StreamType::Logs,
                "healtest",
                &hour,
                &hour,
                false,
            )
            .await
            .unwrap()
        };
        let mut before = Vec::new();
        for _ in 0..30 {
            before = query_partition().await;
            if !before.is_empty() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        assert_eq!(
            before.len(),
            1,
            "file_list must hold exactly the one healtest file"
        );
        let original_key = before[0].key.clone();

        // query battery (deterministic order); snapshots must be identical
        // across the heal
        let start_time = anchor - 3_600_000_000i64;
        let end_time = now + 3_600_000_000i64;
        let queries = [
            "select * from healtest where match_all('healtoken77') order by _timestamp desc",
            "select * from healtest where level = 'error' order by _timestamp desc",
            "select service, count(*) as cnt from healtest group by service order by service asc",
        ];
        let snapshot = || {
            let headers = headers.clone();
            let app = &app;
            async move {
                let mut out = Vec::new();
                for sql in queries {
                    let body_str = serde_json::json!({
                        "query": {
                            "sql": sql,
                            "from": 0,
                            "size": 100,
                            "start_time": start_time,
                            "end_time": end_time
                        }
                    })
                    .to_string();
                    let (status, body) = make_request(
                        app,
                        Method::POST,
                        &format!("/api/{}/_search", "e2e"),
                        Some(headers.clone()),
                        Some(body_str),
                    )
                    .await;
                    assert!(
                        status.is_success(),
                        "search failed: {}",
                        String::from_utf8_lossy(&body)
                    );
                    let res = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
                    out.push(res["hits"].clone());
                }
                out
            }
        };
        let baseline = snapshot().await;
        assert_eq!(
            baseline[0].as_array().map(|h| h.len()),
            Some(2),
            "match_all baseline: {baseline:?}"
        );
        assert_eq!(
            baseline[1].as_array().map(|h| h.len()),
            Some(2),
            "equality baseline: {baseline:?}"
        );
        assert_eq!(
            baseline[2].as_array().map(|h| h.len()),
            Some(2),
            "group-by baseline: {baseline:?}"
        );

        let mut worker = MergeWorker::new(1);
        worker.run().unwrap();
        let offset = anchor - (anchor % hour_micros(1));

        // ---- Leg A: a CURRENT single file is a NO-OP --------------------
        let job_id = infra::file_list::add_job("e2e", StreamType::Logs, "healtest", offset)
            .await
            .unwrap();
        claim_job_for_merge(job_id).await;
        merge_by_stream(
            worker.tx(),
            "e2e",
            StreamType::Logs,
            "healtest",
            job_id,
            offset,
        )
        .await
        .unwrap();
        let after_noop = query_partition().await;
        assert_eq!(after_noop.len(), 1, "no-op leg: still one file");
        assert_eq!(
            after_noop[0].key, original_key,
            "a current single file must stay untouched (same key)"
        );
        disk_objects.clear();
        find_files_with_ext(healtest_dir, ".vix", "", &mut disk_objects);
        assert_eq!(
            disk_objects.len(),
            1,
            "the no-op must not write any object: {disk_objects:?}"
        );

        // ---- Leg B: settings gain a cs field -> healing rebuild ---------
        let body_str = r#"{"column_store_fields":{"add":["service"],"remove":[]}}"#;
        let (status, body) = make_request(
            &app,
            Method::PUT,
            &format!("/api/{}/streams/{}/settings", "e2e", "healtest"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        assert!(
            status.is_success(),
            "healtest settings update failed: {}",
            String::from_utf8_lossy(&body)
        );
        // wait until the compactor's own settings read sees the new field
        let mut settings_visible = false;
        for _ in 0..60 {
            let latest_schema = infra::schema::get("e2e", "healtest", StreamType::Logs)
                .await
                .unwrap();
            let settings = infra::schema::unwrap_stream_settings(&latest_schema);
            if settings.is_some_and(|s| s.column_store_fields.iter().any(|f| f == "service")) {
                settings_visible = true;
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
        assert!(
            settings_visible,
            "column_store_fields change never reached the schema cache"
        );

        let job_id = infra::file_list::add_job("e2e", StreamType::Logs, "healtest", offset)
            .await
            .unwrap();
        claim_job_for_merge(job_id).await;
        merge_by_stream(
            worker.tx(),
            "e2e",
            StreamType::Logs,
            "healtest",
            job_id,
            offset,
        )
        .await
        .unwrap();
        let after_heal = query_partition().await;
        assert_eq!(
            after_heal.len(),
            1,
            "healing must land exactly one output file"
        );
        let healed_key = after_heal[0].key.clone();
        assert_ne!(
            healed_key, original_key,
            "the healing rebuild must REPLACE the input file"
        );
        assert_eq!(after_heal[0].meta.records, 6, "all rows preserved");
        disk_objects.clear();
        find_files_with_ext(healtest_dir, ".vix", "", &mut disk_objects);
        assert!(
            disk_objects
                .iter()
                .any(|p| p.ends_with(healed_key.rsplit('/').next().unwrap_or_default())),
            "the healed object must exist in storage: {disk_objects:?}"
        );

        // identical results over the healed file
        let after = snapshot().await;
        assert_eq!(
            baseline, after,
            "query results must be identical across the heal"
        );

        // ---- Leg C: healing converges — the healed file is a no-op ------
        let job_id = infra::file_list::add_job("e2e", StreamType::Logs, "healtest", offset)
            .await
            .unwrap();
        claim_job_for_merge(job_id).await;
        merge_by_stream(
            worker.tx(),
            "e2e",
            StreamType::Logs,
            "healtest",
            job_id,
            offset,
        )
        .await
        .unwrap();
        let after_second = query_partition().await;
        assert_eq!(after_second.len(), 1, "converged leg: still one file");
        assert_eq!(
            after_second[0].key, healed_key,
            "the healed file must classify current (no rebuild loop)"
        );
    }

    async fn e2e_list_users() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/users", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
        let body_str = String::from_utf8_lossy(&body);
        println!("list users resp: {body_str:?}");
        // deserialize the body into UserList
        let user_list: UserList = serde_json::from_str(&body_str).unwrap();
        assert!(!user_list.data.is_empty());
        assert!(
            user_list
                .data
                .iter()
                .any(|user| user.email == "admin@example.com")
        );
        assert!(
            user_list
                .data
                .iter()
                .any(|user| user.email == "nonadmin@example.com")
        );
    }

    async fn e2e_get_organizations() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) =
            make_request(&app, Method::GET, "/api/organizations", Some(headers), None).await;
        assert!(status.is_success());
    }

    async fn e2e_get_user_passcode() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/passcode", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_update_user_passcode() {
        let auth = setup();
        let body_str = "";
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::PUT,
            &format!("/api/{}/passcode", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_user_authentication() {
        let _auth = setup();
        let body_str = r#"{
                                "name": "root@example.com",
                                "password": "Complexpass#123"
                            }"#;
        let app = init_test_router();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let (status, _body) = make_request(
            &app,
            Method::POST,
            "/auth/login",
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_user_authentication_with_error() {
        let _auth = setup();
        let body_str = r#"{
                                "name": "root2@example.com",
                                "password": "Complexpass#123"
                            }"#;
        let app = init_test_router();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let (status, _body) = make_request(
            &app,
            Method::POST,
            "/auth/login",
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(!status.is_success());
    }

    async fn e2e_post_user() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);

        // Delete user if it exists from previous test runs to avoid conflicts
        let _ = make_request(
            &app,
            Method::DELETE,
            &format!("/api/{}/users/{}", "e2e", "nonadmin@example.com"),
            Some(headers.clone()),
            None,
        )
        .await;

        let body_str = r#"{
                                "email": "nonadmin@example.com",
                                "password": "Abcd12345!",
                                "role": "admin"
                            }"#;
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/users", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        // print the body from resp
        println!("post user status: {status:?}");
        println!("post user body: {}", String::from_utf8_lossy(&body));
        assert!(status.is_success());
    }

    async fn e2e_update_user() {
        let auth = setup();
        let body_str = r#"{
                                "email": "nonadmin@example.com",
                                "new_password": "Newpass12!",
                                "change_password": true
                            }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::PUT,
            &format!("/api/{}/users/{}", "e2e", "nonadmin@example.com"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_update_user_with_empty() {
        let auth = setup();
        let body_str = r#"{}"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::PUT,
            &format!("/api/{}/users/{}", "e2e", "nonadmin@example.com"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(!status.is_success());
    }

    async fn e2e_add_root_user_to_org() {
        let auth = setup();
        let body_str = r#"{
            "role":"member"
        }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/users/{}", "e2e", "root@example.com"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        // It should fail as root user cannot be added to an organization
        assert!(!status.is_success());
    }

    async fn e2e_add_user_to_org() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);

        // Check if admin@example.com already exists from initialization
        let (status, body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/users", "e2e"),
            Some(headers.clone()),
            None,
        )
        .await;
        assert!(status.is_success());
        let body_str = String::from_utf8_lossy(&body);
        let user_list: UserList = serde_json::from_str(&body_str).unwrap();
        let admin_exists = user_list
            .data
            .iter()
            .any(|user| user.email == "admin@example.com");

        if !admin_exists {
            // Test that adding user to org fails when user doesn't exist
            let body_str = r#"{
                "role":"admin"
            }"#;
            let (status, _body) = make_request(
                &app,
                Method::POST,
                &format!("/api/{}/users/{}", "e2e", "admin@example.com"),
                Some(headers.clone()),
                Some(body_str.to_string()),
            )
            .await;
            // Should fail as the user still does not exist
            assert!(status.is_client_error());

            // Add the user
            let body_str = r#"{
                "email": "admin@example.com",
                "password": "Abcd12345!",
                "role": "admin"
            }"#;

            let (status, _body) = make_request(
                &app,
                Method::POST,
                &format!("/api/{}/users", "e2e"),
                Some(headers.clone()),
                Some(body_str.to_string()),
            )
            .await;
            assert!(status.is_success());
        }

        // Role in the default organization
        let body_str = r#"{
            "role":"admin"
        }"#;

        // Add the user to the default organization with role admin
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/users/{}", "default", "admin@example.com"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        println!(
            "add user to default org body: {}",
            String::from_utf8_lossy(&body)
        );
        // Accept both success (user added) and conflict (user already in org) as valid outcomes
        assert!(status.is_success() || status.as_u16() == 409);
    }

    async fn e2e_delete_user() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/{}/users/{}", "e2e", "nonadmin@example.com"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_create_dashboard() -> Dashboard {
        let auth = setup();
        let body_str = r##"{"version":5,"title":"b2","dashboardId":"","description":"desc2","role":"","owner":"root@example.com","created":"2023-03-30T07:49:41.744+00:00","tabs":[{"tabId":"tab1","name":"Main","panels":[{"id":"Panel_ID7857010","type":"bar","title":"p5","description":"sample config blah blah blah","config":{"show_legends":true},"queryType":"sql","queries":[{"query":"SELECT histogram(_timestamp) as \"x_axis_1\", count(kubernetes_host) as \"y_axis_1\" FROM \"default\" GROUP BY \"x_axis_1\" ORDER BY \"x_axis_1\"","customQuery":false,"fields":{"stream":"default","stream_type":"logs","x":[{"label":"Timestamp","alias":"x_axis_1","column":"_timestamp","color":null,"aggregationFunction":"histogram"}],"y":[{"label":"Kubernetes Host","alias":"y_axis_1","column":"kubernetes_host","color":"#5960b2","aggregationFunction":"count"}],"filter":{"filterType":"group","logicalOperator":"AND","conditions":[]}},"config":{"promql_legend":""}}],"layout":{"x":0,"y":0,"w":12,"h":13,"i":1}}]}]}"##;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (_status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/dashboards", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;

        let body_str = String::from_utf8_lossy(&body);
        let result: Dashboard = json::from_slice(&body)
            .unwrap_or_else(|e| panic!("Failed to deserialize dashboard: {e}\nBody: {body_str}"));
        result
    }

    async fn e2e_list_dashboards() -> Vec<Dashboard> {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (_status, body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/dashboards", "e2e"),
            Some(headers),
            None,
        )
        .await;

        // Try to parse the response body as a list of dashboards.
        let mut body_json: json::Value = json::from_slice(&body).unwrap();
        let list_json = body_json
            .as_object_mut()
            .unwrap()
            .remove("dashboards")
            .unwrap();
        let dashboards: Vec<Dashboard> = json::from_value(list_json).unwrap();

        dashboards
    }

    async fn e2e_update_dashboard(dashboard: v5::Dashboard, hash: String) -> Dashboard {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (_status, body) = make_request(
            &app,
            Method::PUT,
            &format!(
                "/api/{}/dashboards/{}?hash={}",
                "e2e", dashboard.dashboard_id, hash
            ),
            Some(headers),
            Some(json::to_string(&dashboard).unwrap()),
        )
        .await;

        json::from_slice(&body).unwrap()
    }

    async fn e2e_get_dashboard(dashboard_id: &str) -> Dashboard {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (_status, body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/dashboards/{dashboard_id}", "e2e"),
            Some(headers),
            None,
        )
        .await;

        json::from_slice(&body).unwrap()
    }

    async fn e2e_delete_dashboard(dashboard_id: &str) {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/{}/dashboards/{dashboard_id}", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_trace() {
        let auth = setup();
        let path = "./tests/trace_input.json";
        let body_str = fs::read_to_string(path).expect("Read file failed");

        // app
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/traces", "e2e"),
            Some(headers),
            Some(body_str),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_metrics() {
        let auth = setup();

        let loc_label: Vec<prometheus_rpc::Label> = vec![
            prometheus_rpc::Label {
                name: "__name__".to_string(),
                value: "grafana_api_dashboard_save_milliseconds_count".to_string(),
            },
            prometheus_rpc::Label {
                name: "cluster".to_string(),
                value: "prom-k8s".to_string(),
            },
            prometheus_rpc::Label {
                name: "__replica__".to_string(),
                value: "prom-k8s-0".to_string(),
            },
        ];

        let mut loc_samples: Vec<prometheus_rpc::Sample> = vec![];

        for i in 1..2 {
            loc_samples.push(prometheus_rpc::Sample {
                value: i as f64,
                timestamp: Utc::now().timestamp_micros(),
            });
        }
        loc_samples.push(prometheus_rpc::Sample {
            value: f64::NEG_INFINITY,
            timestamp: Utc::now().timestamp_micros(),
        });
        loc_samples.push(prometheus_rpc::Sample {
            value: f64::INFINITY,
            timestamp: Utc::now().timestamp_micros(),
        });

        loc_samples.push(prometheus_rpc::Sample {
            value: f64::NAN,
            timestamp: Utc::now().timestamp_micros(),
        });
        let loc_exemp: Vec<prometheus_rpc::Exemplar> = vec![];
        let loc_hist: Vec<prometheus_rpc::Histogram> = vec![];

        let ts = prometheus_rpc::TimeSeries {
            labels: loc_label,
            samples: loc_samples,
            exemplars: loc_exemp,
            histograms: loc_hist,
        };

        let metadata: Vec<prometheus_rpc::MetricMetadata> = vec![];
        let wr_req: prometheus_rpc::WriteRequest = prometheus_rpc::WriteRequest {
            timeseries: vec![ts],
            metadata,
        };
        let mut out = BytesMut::with_capacity(wr_req.encoded_len());
        wr_req.encode(&mut out).expect("Out of memory");
        let data: Bytes = out.into();
        let body = snap::raw::Encoder::new()
            .compress_vec(&data)
            .expect("Out of memory");

        // app
        let app = init_test_router();
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Prometheus-Remote-Write-Version",
            HeaderValue::from_static("0.1.0"),
        );
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("snappy"));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-protobuf"),
        );
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(auth.1).expect("Invalid auth header"),
        );

        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("/api/{}/prometheus/api/v1/write", "e2e"));
        let mut req_builder = req;
        for (key, value) in headers.iter() {
            req_builder = req_builder.header(key, value);
        }
        let req = req_builder
            .body(Body::from(body))
            .expect("Failed to build request");

        let response = app
            .clone()
            .oneshot(req)
            .await
            .expect("Failed to execute request");
        let status = response.status();
        assert!(
            status.is_success(),
            "Prometheus write failed with status: {}. This may indicate memtable overflow or resource constraints.",
            status
        );
    }

    async fn e2e_get_org_summary() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/summary", "e2e"),
            Some(headers),
            None,
        )
        .await;
        log::info!("{:?}", status);
        assert!(status.is_success());
    }

    async fn e2e_post_alert_template() {
        let auth = setup();
        let body_str = r#"{"name":"slackTemplate","body":"{\"text\":\"For stream {stream_name} of organization {org_name} alert {alert_name} of type {alert_type} is active app_name {app_name}\"}"}"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/alerts/templates", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        let text = String::from_utf8_lossy(&body).to_string();
        println!("e2e_post_alert_template: status: {status:?}, text: {text:?}");
        assert!(status.is_success());
    }

    async fn e2e_get_alert_template() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/alerts/templates/{}", "e2e", "slackTemplate"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_delete_alert_template() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/{}/alerts/templates/{}", "e2e", "slackTemplate"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_alert_email_template() {
        let auth = setup();
        let body_str = r#"{"name":"email_template","body":"This is email for {alert_name}.","type":"email","title":"Email Subject"}"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/alerts/templates", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_get_alert_email_template() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/alerts/templates/{}", "e2e", "email_template"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_delete_alert_email_template() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/{}/alerts/templates/{}", "e2e", "email_template"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_list_alert_template() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/alerts/templates", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_alert_destination() {
        let auth = setup();
        let body_str = r#"{
                "name": "slack",
                "url": "https://dummy/alert",
                "method": "post",
                "template": "slackTemplate",
                "headers":{
                    "x_org_id":"Test_header"
                }
            }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/alerts/destinations", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(
            status.is_success(),
            "post alert destination failed: {}",
            String::from_utf8_lossy(&body)
        );
    }

    async fn e2e_get_alert_destination() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/alerts/destinations/{}", "e2e", "slack"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_delete_alert_destination() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/{}/alerts/destinations/{}", "e2e", "slack"),
            Some(headers),
            None,
        )
        .await;
        log::info!("{:?}", status);
        assert!(status.is_success());
    }

    async fn e2e_post_alert_email_destination() {
        let auth = setup();
        let body_str = r#"{"url":"","method":"post","skip_tls_verify":false,"template":"email_template","headers":{},"name":"email","type":"email","emails":["nonadmin@example.com"]}"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/alerts/destinations", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_alert_email_destination_should_fail() {
        let auth = setup();
        let body_str = r#"{"url":"","method":"post","skip_tls_verify":false,"template":"email_template","headers":{},"name":"email_fail","type":"email","emails":["nonadmin2@example.com"]}"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/alerts/destinations", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_client_error());
    }

    async fn e2e_get_alert_email_destination() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/alerts/destinations/{}", "e2e", "email"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_delete_alert_email_destination() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/{}/alerts/destinations/{}", "e2e", "email"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_list_alert_destinations() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/alerts/destinations", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_sns_alert_template() {
        let auth = setup();
        let body_str = r#"{
            "name": "snsTemplate",
            "body": "{\"default\": \"SNS alert {alert_name} triggered for {stream_name} in {org_name}\"}"
        }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/alerts/templates", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_sns_alert_destination() {
        let auth = setup();
        let body_str = r#"{
            "name": "sns_alert",
            "type": "sns",
            "sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:MyTopic",
            "aws_region": "us-east-1",
            "template": "snsTemplate"
        }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/alerts/destinations", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_get_sns_alert_destination() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/alerts/destinations/{}", "e2e", "sns_alert"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());

        // Optionally, deserialize and check the response body
        let destination: Destination = serde_json::from_slice(&body).unwrap();
        assert_eq!(destination.destination_type, DestinationType::Sns);
        assert_eq!(
            destination.sns_topic_arn,
            Some("arn:aws:sns:us-east-1:123456789012:MyTopic".to_string())
        );
        assert_eq!(destination.aws_region, Some("us-east-1".to_string()));
    }

    async fn e2e_list_alert_destinations_with_sns() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) = make_request(
            &app,
            Method::GET,
            &format!("/api/{}/alerts/destinations", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());

        // Optionally, deserialize and check the response body
        let destinations: Vec<Destination> = serde_json::from_slice(&body).unwrap();
        assert!(
            destinations
                .iter()
                .any(|d| d.destination_type == DestinationType::Sns)
        );
    }

    async fn e2e_update_sns_alert_destination() {
        let auth = setup();
        let body_str = r#"{
            "name": "sns_alert",
            "type": "sns",
            "sns_topic_arn": "arn:aws:sns:us-west-2:123456789012:UpdatedTopic",
            "aws_region": "us-west-2",
            "template": "snsTemplate"
        }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::PUT,
            &format!("/api/{}/alerts/destinations/{}", "e2e", "sns_alert"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_delete_sns_alert_destination() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/{}/alerts/destinations/{}", "e2e", "sns_alert"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_alert_with_sns_destination() {
        let auth = setup();
        let body_str = r#"{
            "name": "sns_test_alert",
            "stream_type": "logs",
            "stream_name": "olympics_schema",
            "is_real_time": false,
            "query_condition": {
                "conditions": [{
                    "column": "level",
                    "operator": "=",
                    "value": "error"
                }]
            },
            "trigger_condition": {
                "period": 5,
                "threshold": 1,
                "silence": 10
            },
            "destinations": ["sns_alert"],
            "context_attributes": {
                "app_name": "TestApp"
            }
        }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/v2/{}/alerts", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_delete_alert_with_sns_destination() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);

        let alert = openobserve::service::db::alerts::alert::get_by_name(
            "e2e",
            config::meta::stream::StreamType::Logs,
            "olympics_schema",
            "sns_test_alert",
        )
        .await;
        assert!(alert.is_ok());
        let alert = alert.unwrap();
        assert!(alert.is_some());
        let alert = alert.unwrap();
        let id = alert.id;
        assert!(id.is_some());
        let id = id.unwrap();
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/v2/{}/alerts/{}", "e2e", id),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_post_alert_multirange() {
        let auth = setup();
        let body_str = r#"{
                                "name": "alert_multi_range",
                                "stream_type": "logs",
                                "stream_name": "olympics_schema",
                                "is_real_time": false,
                                "query_condition": {
                                    "conditions": [{
                                        "column": "country",
                                        "operator": "=",
                                        "value": "USA"
                                    }],
                                    "multi_time_range": [{
                                        "offSet": "1440m"
                                    }]
                                },
                                "trigger_condition": {
                                    "period": 5,
                                    "threshold": 1,
                                    "silence": 0,
                                    "frequency": 1
                                },
                                "destinations": ["slack"],
                                "context_attributes":{
                                    "app_name":"App1"
                                }
                            }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/v2/{}/alerts", "e2e"),
            Some(headers),
            Some(body_str.to_string()),
        )
        .await;
        assert!(status.is_success());

        // Get the alert with the same stream name
        let alert = openobserve::service::db::alerts::alert::get_by_name(
            "e2e",
            config::meta::stream::StreamType::Logs,
            "olympics_schema",
            "alert_multi_range",
        )
        .await;
        assert!(alert.is_ok());
        let alert = alert.unwrap();
        assert!(alert.is_some());
        let alert = alert.unwrap();
        assert_eq!(alert.stream_type, config::meta::stream::StreamType::Logs);
        assert_eq!(alert.stream_name, "olympics_schema");
        assert_eq!(alert.name, "alert_multi_range");
        let id = alert.id;
        assert!(id.is_some());
        let id = id.unwrap();
        // Check the trigger
        let trigger = openobserve::service::db::scheduler::exists(
            "e2e",
            config::meta::triggers::TriggerModule::Alert,
            &id.to_string(),
        )
        .await;
        assert!(trigger);
    }

    async fn e2e_delete_alert_multirange() {
        let auth = setup();
        let app = init_test_router();

        // Get the alert with the same stream name
        let alert = openobserve::service::db::alerts::alert::get_by_name(
            "e2e",
            config::meta::stream::StreamType::Logs,
            "olympics_schema",
            "alert_multi_range",
        )
        .await;
        assert!(alert.is_ok());
        let alert = alert.unwrap();
        assert!(alert.is_some());
        let alert = alert.unwrap();
        let id = alert.id;
        assert!(id.is_some());
        let id = id.unwrap();

        // Use the v2 api to delete the alert
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/v2/{}/alerts/{}", "e2e", id),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());

        let trigger = openobserve::service::db::scheduler::exists(
            "e2e",
            config::meta::triggers::TriggerModule::Alert,
            &id.to_string(),
        )
        .await;
        assert!(!trigger);
    }

    async fn e2e_post_alert() {
        let auth = setup();
        let body_str = r#"{
                                "name": "alertChk",
                                "stream_type": "logs",
                                "stream_name": "olympics_schema",
                                "is_real_time": false,
                                "enabled": true,
                                "query_condition": {
                                    "conditions": [{
                                        "column": "country",
                                        "operator": "NotContains",
                                        "value": "AUT"
                                    }]
                                },
                                "trigger_condition": {
                                    "period": 60,
                                    "threshold": 1,
                                    "silence": 0,
                                    "frequency": 60,
                                    "operator": ">="
                                },
                                "destinations": ["slack"],
                                "context_attributes":{
                                    "app_name":"App1"
                                }
                            }"#;
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            &format!("/api/v2/{}/alerts", "e2e"),
            Some(headers.clone()),
            Some(body_str.to_string()),
        )
        .await;
        println!("{:?}", status);
        assert!(status.is_success());

        // Get the alert list
        let (status, body) = make_request(
            &app,
            Method::GET,
            &format!("/api/v2/{}/alerts", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
        let alert_list_response: ListAlertsResponseBody = serde_json::from_slice(&body).unwrap();
        assert!(!alert_list_response.list.is_empty());
        let alert = alert_list_response
            .list
            .iter()
            .find(|a| a.name == "alertChk");
        assert!(alert.is_some());
        let alert = alert.unwrap();
        assert_eq!(alert.name, "alertChk");
        assert!(alert.enabled);
        let id = alert.alert_id;
        let id = id.to_string();

        // Check the trigger
        let trigger = openobserve::service::db::scheduler::exists(
            "e2e",
            config::meta::triggers::TriggerModule::Alert,
            &id.to_string(),
        )
        .await;
        assert!(trigger);
    }

    async fn e2e_get_alert() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);

        // Get the alert list
        let (status, body) = make_request(
            &app,
            Method::GET,
            &format!("/api/v2/{}/alerts", "e2e"),
            Some(headers.clone()),
            None,
        )
        .await;
        assert!(status.is_success());
        let alert_list_response: ListAlertsResponseBody = serde_json::from_slice(&body).unwrap();
        assert!(!alert_list_response.list.is_empty());
        let alert = alert_list_response
            .list
            .iter()
            .find(|a| a.name == "alertChk");
        assert!(alert.is_some());
        let alert = alert.unwrap();
        assert_eq!(alert.name, "alertChk");
        assert!(alert.enabled);
        let id = alert.alert_id;
        let id = id.to_string();

        let (status, body) = make_request(
            &app,
            Method::GET,
            &format!("/api/v2/{}/alerts/{}", "e2e", id),
            Some(headers),
            None,
        )
        .await;
        log::info!("{:?}", status);
        assert!(status.is_success());
        let alert_response: GetAlertResponseBody = serde_json::from_slice(&body).unwrap();
        assert_eq!(alert_response.0.name, "alertChk");
        assert_eq!(
            alert_response.0.stream_type,
            openobserve::handler::http::models::alerts::StreamType::Logs
        );
        assert_eq!(alert_response.0.stream_name, "olympics_schema");
        assert!(alert_response.0.enabled);
    }

    async fn e2e_handle_alert_after_destination_retries() {
        let alert = openobserve::service::db::alerts::alert::get_by_name(
            "e2e",
            config::meta::stream::StreamType::Logs,
            "olympics_schema",
            "alertChk",
        )
        .await;
        assert!(alert.is_ok());
        let alert = alert.unwrap();
        assert!(alert.is_some());
        let alert = alert.unwrap();
        let id = alert.id;
        assert!(id.is_some());
        let id = id.unwrap();

        let now = Utc::now().timestamp_micros();
        let mins_3_later = now
            + Duration::try_minutes(3)
                .unwrap()
                .num_microseconds()
                .unwrap();
        let trigger = Trigger {
            id: 1,
            org: "e2e".to_string(),
            module: config::meta::triggers::TriggerModule::Alert,
            module_key: id.to_string(),
            start_time: Some(now),
            end_time: Some(mins_3_later),
            next_run_at: now,
            is_realtime: false,
            is_silenced: false,
            status: config::meta::triggers::TriggerStatus::Processing,
            retries: 2,
            data: "{}".to_string(),
        };

        let trace_id = "test_trace_id";
        let res = handle_triggers(trace_id, trigger).await;
        // This alert has an invalid destination, but handle_triggers should succeed.
        // Note: May get partial results if files are cleaned up during test execution.
        if let Err(ref e) = res {
            let err_msg = e.to_string();
            // Accept partial response errors due to file cleanup race conditions in tests
            if !err_msg.contains("Partial response") && !err_msg.contains("parquet file not found")
            {
                panic!("handle_triggers failed unexpectedly: {:?}", res);
            }
        }

        let trigger = openobserve::service::db::scheduler::get(
            "e2e",
            config::meta::triggers::TriggerModule::Alert,
            &id.to_string(),
        )
        .await;
        assert!(trigger.is_ok());
        let trigger = trigger.unwrap();
        assert!(trigger.next_run_at > now && trigger.retries == 0);
    }

    async fn e2e_handle_alert_reached_max_retries() {
        let now = Utc::now().timestamp_micros();
        let mins_3_later = now
            + Duration::try_minutes(3)
                .unwrap()
                .num_microseconds()
                .unwrap();
        let alert = openobserve::service::db::alerts::alert::get_by_name(
            "e2e",
            config::meta::stream::StreamType::Logs,
            "olympics_schema",
            "alertChk",
        )
        .await;
        assert!(alert.is_ok());
        let alert = alert.unwrap();
        assert!(alert.is_some());
        let alert = alert.unwrap();
        let id = alert.id;
        assert!(id.is_some());
        let id = id.unwrap();
        let trigger = Trigger {
            id: 1,
            org: "e2e".to_string(),
            module: config::meta::triggers::TriggerModule::Alert,
            module_key: id.to_string(),
            start_time: Some(now),
            end_time: Some(mins_3_later),
            next_run_at: now,
            is_realtime: false,
            is_silenced: false,
            status: config::meta::triggers::TriggerStatus::Processing,
            retries: 3,
            data: "{}".to_string(),
        };

        let trace_id = "test_trace_id";
        let res = handle_triggers(trace_id, trigger).await;
        // This alert has an invalid destination
        assert!(res.is_ok());

        let trigger = openobserve::service::db::scheduler::get(
            "e2e",
            config::meta::triggers::TriggerModule::Alert,
            &id.to_string(),
        )
        .await;
        assert!(trigger.is_ok());
        let trigger = trigger.unwrap();
        assert!(trigger.next_run_at > now && trigger.retries == 0);
    }

    async fn e2e_handle_alert_after_evaluation_retries() {
        let mut alert: Alert = Default::default();
        alert.name = "test_alert_wrong_sql".to_string();
        alert.stream_type = "logs".into();
        alert.stream_name = "olympics_schema".to_string();
        alert.is_real_time = false;
        alert.enabled = true;
        alert.query_condition = QueryCondition {
            query_type: "sql".into(),
            conditions: None,
            sql: Some("SELEC country FROM \"olympics_schema\"".to_string()),
            ..Default::default()
        };
        alert.trigger_condition = TriggerCondition {
            period: 60,
            threshold: 1,
            silence: 0,
            frequency: 3600,
            operator: Operator::GreaterThanEquals,
            ..Default::default()
        };
        alert.destinations = vec!["slack".to_string()];

        let res = openobserve::service::db::alerts::alert::set("e2e", alert, true).await;
        assert!(res.is_ok());
        let alert = res.unwrap();
        let id = alert.id;
        assert!(id.is_some());
        let id = id.unwrap();

        let now = Utc::now().timestamp_micros();
        let mins_3_later = now
            + Duration::try_minutes(3)
                .unwrap()
                .num_microseconds()
                .unwrap();
        let trigger = Trigger {
            id: 1,
            org: "e2e".to_string(),
            module: config::meta::triggers::TriggerModule::Alert,
            module_key: id.to_string(),
            start_time: Some(now),
            end_time: Some(mins_3_later),
            next_run_at: now,
            is_realtime: false,
            is_silenced: false,
            status: config::meta::triggers::TriggerStatus::Processing,
            retries: 2,
            data: "{}".to_string(),
        };

        let trace_id = "test_trace_id";
        let res = handle_triggers(trace_id, trigger).await;
        // In case of alert evaluation errors, this error is returned
        assert!(res.is_err());

        let trigger = openobserve::service::db::scheduler::get(
            "e2e",
            config::meta::triggers::TriggerModule::Alert,
            &id.to_string(),
        )
        .await;
        assert!(trigger.is_ok());
        let trigger = trigger.unwrap();
        assert!(trigger.next_run_at > now && trigger.retries == 0);

        let res = openobserve::service::db::alerts::alert::delete_by_name(
            "e2e",
            config::meta::stream::StreamType::Logs,
            "olympics_schema",
            "test_alert_wrong_sql",
        )
        .await;
        assert!(res.is_ok());
    }

    async fn e2e_delete_alert() {
        let auth = setup();
        let app = init_test_router();

        let alert = openobserve::service::db::alerts::alert::get_by_name(
            "e2e",
            config::meta::stream::StreamType::Logs,
            "olympics_schema",
            "alertChk",
        )
        .await;
        assert!(alert.is_ok());
        let alert = alert.unwrap();
        assert!(alert.is_some());
        let alert = alert.unwrap();
        let id = alert.id;
        assert!(id.is_some());
        let id = id.unwrap();

        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::DELETE,
            &format!("/api/v2/{}/alerts/{}", "e2e", id),
            Some(headers),
            None,
        )
        .await;
        log::info!("{:?}", status);
        assert!(status.is_success());

        let trigger = openobserve::service::db::scheduler::exists(
            "e2e",
            config::meta::triggers::TriggerModule::Alert,
            &id.to_string(),
        )
        .await;
        assert!(!trigger);
    }

    async fn e2e_list_alerts() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/v2/{}/alerts", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_list_real_time_alerts() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::GET,
            &format!("/api/v2/{}/alerts", "e2e"),
            Some(headers),
            None,
        )
        .await;
        assert!(status.is_success());
    }

    async fn e2e_health_check() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) =
            make_request(&app, Method::GET, "/healthz", Some(headers), None).await;
        assert!(status.is_success());
    }

    async fn e2e_config() {
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, _body) = make_request(&app, Method::GET, "/config", Some(headers), None).await;
        assert!(status.is_success());
    }

    // Helper function to create pipeline via API
    async fn e2e_post_pipeline(pipeline_data: Pipeline) {
        let auth = setup();
        let body_str = serde_json::to_string(&pipeline_data).unwrap();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) = make_request(
            &app,
            Method::POST,
            &format!("/api/{}/pipelines", "e2e"),
            Some(headers),
            Some(body_str),
        )
        .await;
        if !status.is_success() {
            println!("Response body: {}", String::from_utf8_lossy(&body));
            panic!("Failed to create pipeline");
        }
    }

    // Derived Stream Integration Tests
    async fn e2e_create_test_pipeline() {
        // Create a test pipeline with derived stream for testing
        let pipeline_data = Pipeline {
            id: "test_derived_stream_pipeline".to_string(),
            version: 1,
            enabled: true,
            org: "e2e".to_string(),
            name: "test_derived_stream".to_string(),
            description: "Test pipeline for derived stream integration tests".to_string(),
            source: PipelineSource::Scheduled(DerivedStream {
                org_id: "e2e".to_string(),
                stream_type: StreamType::Logs,
                query_condition: QueryCondition {
                    query_type: config::meta::alerts::QueryType::SQL,
                    sql: Some("SELECT _timestamp FROM \"olympics_schema\"".to_string()),
                    ..Default::default()
                },
                trigger_condition: TriggerCondition {
                    period: 5,      // 5 minutes
                    frequency: 300, // 5 minutes in seconds
                    ..Default::default()
                },
                tz_offset: 0,
                start_at: None,
                delay: None,
            }),
            kind: Default::default(),
            nodes: vec![
                // Source node (query node for scheduled pipeline)
                config::meta::pipeline::components::Node::new(
                    "source-node-1".to_string(),
                    config::meta::pipeline::components::NodeData::Query(DerivedStream {
                        org_id: "e2e".to_string(),
                        stream_type: StreamType::Logs,
                        query_condition: QueryCondition {
                            query_type: config::meta::alerts::QueryType::SQL,
                            sql: Some("SELECT _timestamp FROM \"olympics_schema\"".to_string()),
                            ..Default::default()
                        },
                        trigger_condition: TriggerCondition {
                            period: 5,
                            frequency: 300,
                            ..Default::default()
                        },
                        tz_offset: 0,
                        start_at: None,
                        delay: None,
                    }),
                    100.0,
                    50.0,
                    "input".to_string(),
                ),
                // Destination node (output stream)
                config::meta::pipeline::components::Node::new(
                    "dest-node-1".to_string(),
                    config::meta::pipeline::components::NodeData::Stream(
                        config::meta::stream::StreamParams {
                            org_id: "e2e".to_string().into(),
                            stream_name: "test_derived_output_stream".to_string().into(),
                            stream_type: StreamType::Logs,
                        },
                    ),
                    100.0,
                    200.0,
                    "output".to_string(),
                ),
            ],
            edges: vec![config::meta::pipeline::components::Edge::new(
                "source-node-1".to_string(),
                "dest-node-1".to_string(),
            )],
        };

        // Save pipeline using API call
        e2e_post_pipeline(pipeline_data).await;
    }

    async fn e2e_handle_derived_stream_success() {
        // list the pipelines and choose the first one using API
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) =
            make_request(&app, Method::GET, "/api/e2e/pipelines", Some(headers), None).await;
        assert!(status.is_success());
        let pipeline_list: openobserve::handler::http::models::pipelines::PipelineList =
            json::from_slice(&body).unwrap();
        let pipeline = pipeline_list.list.first();
        assert!(pipeline.is_some());
        let pipeline = pipeline.unwrap();

        let now = Utc::now().timestamp_micros();
        let mins_5_later = now
            + Duration::try_minutes(5)
                .unwrap()
                .num_microseconds()
                .unwrap();
        let module_key = format!("logs/e2e/test_derived_stream/{}", pipeline.id);
        println!(
            "e2e handle derived stream success module_key: {}, where pipeline name is {}",
            module_key, pipeline.name
        );

        let trigger = Trigger {
            id: 1,
            org: "e2e".to_string(),
            module: TriggerModule::DerivedStream,
            module_key: module_key.clone(),
            start_time: Some(now),
            end_time: Some(mins_5_later),
            next_run_at: now,
            is_realtime: false,
            is_silenced: false,
            status: config::meta::triggers::TriggerStatus::Processing,
            retries: 0,
            data: "{}".to_string(),
        };

        let trace_id = "test_derived_stream_trace_id";
        let res = handle_triggers(trace_id, trigger).await;
        // Should succeed even with empty data
        assert!(res.is_ok());

        // Verify trigger was updated - retry with exponential backoff since trigger processing
        // is async and involves batch updates that may not flush immediately in CI
        let mut attempts = 0;
        let max_attempts = 20;
        let mut delay_ms = 100u64;
        let mut trigger_updated = false;

        while attempts < max_attempts {
            let trigger = openobserve::service::db::scheduler::get(
                "e2e",
                TriggerModule::DerivedStream,
                &module_key,
            )
            .await;

            if let Ok(trigger) = trigger
                && let Ok(scheduled_trigger_data) =
                    serde_json::from_str::<ScheduledTriggerData>(&trigger.data)
                && scheduled_trigger_data.period_end_time.is_some()
            {
                assert!(scheduled_trigger_data.period_end_time.unwrap() > 0);
                assert!(trigger.status == TriggerStatus::Waiting);
                assert!(trigger.next_run_at > now && trigger.retries == 0);
                trigger_updated = true;
                break;
            }

            attempts += 1;
            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            // Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1000ms (capped)
            delay_ms = std::cmp::min(delay_ms * 2, 1000);
        }

        assert!(
            trigger_updated,
            "Trigger was not updated after {max_attempts} attempts"
        );
    }

    async fn e2e_handle_derived_stream_pipeline_not_found() {
        let now = Utc::now().timestamp_micros();
        let mins_5_later = now
            + Duration::try_minutes(5)
                .unwrap()
                .num_microseconds()
                .unwrap();
        let module_key = "logs/e2e/nonexistent_pipeline/invalid_id".to_string();

        let trigger = Trigger {
            id: 2,
            org: "e2e".to_string(),
            module: TriggerModule::DerivedStream,
            module_key,
            start_time: Some(now),
            end_time: Some(mins_5_later),
            next_run_at: now,
            is_realtime: false,
            is_silenced: false,
            status: config::meta::triggers::TriggerStatus::Processing,
            retries: 0,
            data: "{}".to_string(),
        };

        let trace_id = "test_derived_stream_not_found_trace_id";
        let res = handle_triggers(trace_id, trigger).await;
        // Should fail with pipeline not found error
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Pipeline associated with trigger not found")
        );
    }

    async fn e2e_handle_derived_stream_max_retries() {
        // list pipelines using API
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) =
            make_request(&app, Method::GET, "/api/e2e/pipelines", Some(headers), None).await;
        assert!(status.is_success());
        let pipeline_list: openobserve::handler::http::models::pipelines::PipelineList =
            json::from_slice(&body).unwrap();
        let pipelines = pipeline_list.list.first();
        assert!(pipelines.is_some());
        let pipeline = pipelines.unwrap();

        let now = Utc::now().timestamp_micros();
        let mins_5_later = now
            + Duration::try_minutes(5)
                .unwrap()
                .num_microseconds()
                .unwrap();
        let module_key = format!("logs/e2e/test_derived_stream/{}", pipeline.id);

        let trigger = Trigger {
            id: 3,
            org: "e2e".to_string(),
            module: TriggerModule::DerivedStream,
            module_key: module_key.clone(),
            start_time: Some(now),
            end_time: Some(mins_5_later),
            next_run_at: now,
            is_realtime: false,
            is_silenced: false,
            status: config::meta::triggers::TriggerStatus::Processing,
            retries: 5, // Max retries reached
            data: "{}".to_string(),
        };

        let trace_id = "test_derived_stream_max_retries_trace_id";
        let res = handle_triggers(trace_id, trigger).await;
        // Should succeed but skip to next run due to max retries
        assert!(res.is_ok());

        // Verify trigger was updated with next run time and retries reset
        let trigger = openobserve::service::db::scheduler::get(
            "e2e",
            TriggerModule::DerivedStream,
            &module_key,
        )
        .await;
        assert!(trigger.is_ok());
        let trigger = trigger.unwrap();
        assert!(trigger.next_run_at > now && trigger.retries == 0);
    }

    async fn e2e_handle_derived_stream_evaluation_failure() {
        // Create a pipeline that queries a non-existent stream to cause evaluation failure.
        // evaluate_scheduled() returns Err when the stream is not found in schema cache,
        // which should increment the trigger's retries counter.
        let pipeline_data = Pipeline {
            id: "test_derived_stream_pipeline_invalid".to_string(),
            version: 1,
            enabled: true,
            org: "e2e".to_string(),
            name: "test_derived_stream_invalid".to_string(),
            description: "Test pipeline with non-existent stream to cause evaluation failure"
                .to_string(),
            source: PipelineSource::Scheduled(DerivedStream {
                org_id: "e2e".to_string(),
                stream_type: StreamType::Logs,
                query_condition: QueryCondition {
                    query_type: config::meta::alerts::QueryType::SQL,
                    sql: Some("SELECT * FROM \"nonexistent_stream_xyz_abc_test\"".to_string()),
                    ..Default::default()
                },
                trigger_condition: TriggerCondition {
                    period: 5,
                    frequency: 300,
                    ..Default::default()
                },
                tz_offset: 0,
                start_at: None,
                delay: None,
            }),
            kind: Default::default(),
            nodes: vec![
                // Source node (query node for scheduled pipeline with non-existent stream)
                config::meta::pipeline::components::Node::new(
                    "source-node-2".to_string(),
                    config::meta::pipeline::components::NodeData::Query(DerivedStream {
                        org_id: "e2e".to_string(),
                        stream_type: StreamType::Logs,
                        query_condition: QueryCondition {
                            query_type: config::meta::alerts::QueryType::SQL,
                            sql: Some(
                                "SELECT * FROM \"nonexistent_stream_xyz_abc_test\"".to_string(),
                            ),
                            ..Default::default()
                        },
                        trigger_condition: TriggerCondition {
                            period: 5,
                            frequency: 300,
                            ..Default::default()
                        },
                        tz_offset: 0,
                        start_at: None,
                        delay: None,
                    }),
                    150.0,
                    50.0,
                    "input".to_string(),
                ),
                // Destination node (output stream)
                config::meta::pipeline::components::Node::new(
                    "dest-node-2".to_string(),
                    config::meta::pipeline::components::NodeData::Stream(
                        config::meta::stream::StreamParams {
                            org_id: "e2e".to_string().into(),
                            stream_name: "test_invalid_pipeline_output".to_string().into(),
                            stream_type: StreamType::Logs,
                        },
                    ),
                    150.0,
                    200.0,
                    "output".to_string(),
                ),
            ],
            edges: vec![config::meta::pipeline::components::Edge::new(
                "source-node-2".to_string(),
                "dest-node-2".to_string(),
            )],
        };

        // Save pipeline directly to DB (bypassing API validation) to simulate a pipeline
        // with an invalid query that was saved before validation was added, or to test
        // what happens at evaluation time when the stream does not exist.
        openobserve::service::db::pipeline::set(&pipeline_data)
            .await
            .expect("Failed to set pipeline in DB");
        // Create the scheduler trigger directly with needs_validated=false so the
        // non-existent stream does not fail the pre-save test run.
        let derived_stream = match &pipeline_data.source {
            PipelineSource::Scheduled(ds) => ds.clone(),
            _ => panic!("Expected scheduled pipeline"),
        };
        openobserve::service::alerts::derived_streams::save(
            derived_stream,
            &pipeline_data.name,
            &pipeline_data.id,
            false,
        )
        .await
        .expect("Failed to save derived stream trigger");

        let pipeline = get_pipeline_from_api(pipeline_data.name.as_str()).await;

        let now = Utc::now().timestamp_micros();
        let mins_5_later = now
            + Duration::try_minutes(5)
                .unwrap()
                .num_microseconds()
                .unwrap();
        let module_key = format!("logs/e2e/test_derived_stream_invalid/{}", pipeline.id);

        let trigger = Trigger {
            id: 4,
            org: "e2e".to_string(),
            module: TriggerModule::DerivedStream,
            module_key: module_key.clone(),
            start_time: Some(now),
            end_time: Some(mins_5_later),
            next_run_at: now,
            is_realtime: false,
            is_silenced: false,
            status: config::meta::triggers::TriggerStatus::Processing,
            retries: 0,
            data: "{}".to_string(),
        };

        let trace_id = "test_derived_stream_eval_failure_trace_id";
        let _ = handle_triggers(trace_id, trigger).await;
        // Should succeed (handler handles errors gracefully) but increment retries
        // Verify trigger retries were incremented
        let trigger = openobserve::service::db::scheduler::get(
            "e2e",
            TriggerModule::DerivedStream,
            &module_key,
        )
        .await;
        assert!(trigger.is_ok());
        let trigger = trigger.unwrap();
        assert!(trigger.retries > 0);

        // Clean up the invalid pipeline
        let _ = openobserve::service::db::pipeline::delete(&pipeline.id).await;
    }

    // Test to handle case where pipeline triggers for invalid timerange where start time
    // is greater than end time because of the delay feature which is turned on after alert
    // is already created and is running for some time.

    async fn test_derived_stream_invalid_timerange_delay_scenario() {
        // Create a pipeline with derived stream that has delay configured
        let pipeline_data = Pipeline {
            id: "test_invalid_timerange_pipeline".to_string(),
            version: 1,
            enabled: true,
            org: "e2e".to_string(),
            name: "test_invalid_timerange_pipeline".to_string(),
            description: "Test pipeline for invalid timerange delay scenario".to_string(),
            source: PipelineSource::Scheduled(DerivedStream {
                org_id: "e2e".to_string(),
                stream_type: StreamType::Logs,
                query_condition: QueryCondition {
                    query_type: config::meta::alerts::QueryType::SQL,
                    sql: Some("SELECT _timestamp, city FROM \"olympics_schema\"".to_string()),
                    ..Default::default()
                },
                trigger_condition: TriggerCondition {
                    period: 5,      // 5 minutes
                    frequency: 300, // 5 minutes in seconds
                    ..Default::default()
                },
                tz_offset: 0,
                start_at: None,
                delay: Some(10), // 10 minutes delay
            }),
            kind: Default::default(),
            nodes: vec![
                // Source node (query node for scheduled pipeline)
                config::meta::pipeline::components::Node::new(
                    "source-node-1".to_string(),
                    config::meta::pipeline::components::NodeData::Query(DerivedStream {
                        org_id: "e2e".to_string(),
                        stream_type: StreamType::Logs,
                        query_condition: QueryCondition {
                            query_type: config::meta::alerts::QueryType::SQL,
                            sql: Some(
                                "SELECT _timestamp, city FROM \"olympics_schema\"".to_string(),
                            ),
                            ..Default::default()
                        },
                        trigger_condition: TriggerCondition {
                            period: 5,
                            frequency: 300,
                            ..Default::default()
                        },
                        tz_offset: 0,
                        start_at: None,
                        delay: Some(10), // 10 minutes delay
                    }),
                    100.0,
                    50.0,
                    "input".to_string(),
                ),
                // Destination node (output stream)
                config::meta::pipeline::components::Node::new(
                    "dest-node-1".to_string(),
                    config::meta::pipeline::components::NodeData::Stream(
                        config::meta::stream::StreamParams {
                            org_id: "e2e".to_string().into(),
                            stream_name: "derived_stream".to_string().into(),
                            stream_type: StreamType::Logs,
                        },
                    ),
                    100.0,
                    200.0,
                    "output".to_string(),
                ),
            ],
            edges: vec![config::meta::pipeline::components::Edge::new(
                "source-node-1".to_string(),
                "dest-node-1".to_string(),
            )],
        };

        // Create the pipeline
        e2e_post_pipeline(pipeline_data.clone()).await;
        let pipeline = get_pipeline_from_api(pipeline_data.name.as_str()).await;

        // Create a trigger that will cause invalid timerange scenario
        // Simulate a scenario where the trigger was created before delay was enabled
        // and now the start time is greater than end time due to delay
        let current_time = Utc::now().timestamp_micros();
        // 5 minutes is period of the pipeline and 1 min is delay in processing the pipeline
        let period_micros = Duration::try_minutes(6)
            .unwrap()
            .num_microseconds()
            .unwrap();
        let timeout = Duration::try_minutes(2)
            .unwrap()
            .num_microseconds()
            .unwrap();

        // Create trigger with start time that will be greater than end time after delay
        let trigger_start_time = current_time - period_micros; // 5 minutes ago

        let trigger = Trigger {
            id: 1,
            org: "e2e".to_string(),
            module: TriggerModule::DerivedStream,
            module_key: format!("logs/e2e/test_invalid_timerange_pipeline/{}", pipeline.id),
            next_run_at: current_time,
            start_time: Some(current_time),
            end_time: Some(current_time + timeout),
            is_realtime: false,
            is_silenced: false,
            status: config::meta::triggers::TriggerStatus::Processing,
            retries: 0,
            data: serde_json::json!({
                "period_end_time": trigger_start_time // This will cause start > end
            })
            .to_string(),
        };

        // Process the trigger - this should handle invalid timerange gracefully
        let trace_id = "test_invalid_timerange_trace_id";
        let result = handle_triggers(trace_id, trigger).await;

        // Should not return an error, but should handle invalid timerange gracefully
        assert!(result.is_ok());

        // Get the trigger from the database
        let trigger = openobserve::service::db::scheduler::get(
            "e2e",
            TriggerModule::DerivedStream,
            &format!("logs/e2e/test_invalid_timerange_pipeline/{}", pipeline.id),
        )
        .await;
        assert!(trigger.is_ok());
        let trigger = trigger.unwrap();
        // Next run at should be greater than current time
        assert!(trigger.next_run_at > current_time);

        // And last period end time should not change
        assert_eq!(
            trigger.data,
            serde_json::json!({
                "period_end_time": trigger_start_time
            })
            .to_string()
        );

        // Clean up
        let _ = openobserve::service::db::pipeline::delete(&pipeline.id).await;
        // Also delete the trigger job from scheduled jobs table
        let _ = openobserve::service::db::scheduler::delete(
            "e2e",
            TriggerModule::DerivedStream,
            &format!("logs/e2e/test_invalid_timerange_pipeline/{}", pipeline.id),
        )
        .await;
    }

    async fn test_derived_stream_invalid_timerange_with_cron_frequency() {
        let auth = setup();
        let app = init_test_router();

        // Create a pipeline with derived stream using cron frequency
        let pipeline_data = Pipeline {
            id: "test_cron_invalid_timerange_pipeline".to_string(),
            version: 1,
            enabled: true,
            org: "e2e".to_string(),
            name: "test_cron_invalid_timerange_pipeline".to_string(),
            description: "Test pipeline for invalid timerange with cron frequency".to_string(),
            source: PipelineSource::Scheduled(DerivedStream {
                org_id: "e2e".to_string(),
                stream_type: StreamType::Logs,
                query_condition: QueryCondition {
                    query_type: config::meta::alerts::QueryType::SQL,
                    sql: Some("SELECT _timestamp, city FROM \"olympics_schema\"".to_string()),
                    ..Default::default()
                },
                trigger_condition: TriggerCondition {
                    period: 5,                         // 5 minutes
                    frequency: 0,                      // 0 for cron frequency
                    cron: "0 */5 * * * *".to_string(), // Every 5 minutes
                    frequency_type: config::meta::alerts::FrequencyType::Cron,
                    ..Default::default()
                },
                tz_offset: 0,
                start_at: None,
                delay: Some(10), // 10 minutes delay
            }),
            kind: Default::default(),
            nodes: vec![
                // Source node (query node for scheduled pipeline)
                config::meta::pipeline::components::Node::new(
                    "source-node-1".to_string(),
                    config::meta::pipeline::components::NodeData::Query(DerivedStream {
                        org_id: "e2e".to_string(),
                        stream_type: StreamType::Logs,
                        query_condition: QueryCondition {
                            query_type: config::meta::alerts::QueryType::SQL,
                            sql: Some(
                                "SELECT _timestamp, city FROM \"olympics_schema\"".to_string(),
                            ),
                            ..Default::default()
                        },
                        trigger_condition: TriggerCondition {
                            period: 5,
                            frequency: 0,
                            cron: "0 */5 * * * *".to_string(),
                            frequency_type: config::meta::alerts::FrequencyType::Cron,
                            ..Default::default()
                        },
                        tz_offset: 0,
                        start_at: None,
                        delay: Some(10), // 10 minutes delay
                    }),
                    100.0,
                    50.0,
                    "input".to_string(),
                ),
                // Destination node (output stream)
                config::meta::pipeline::components::Node::new(
                    "dest-node-1".to_string(),
                    config::meta::pipeline::components::NodeData::Stream(
                        config::meta::stream::StreamParams {
                            org_id: "e2e".to_string().into(),
                            stream_name: "derived_stream".to_string().into(),
                            stream_type: StreamType::Logs,
                        },
                    ),
                    100.0,
                    200.0,
                    "output".to_string(),
                ),
            ],
            edges: vec![config::meta::pipeline::components::Edge::new(
                "source-node-1".to_string(),
                "dest-node-1".to_string(),
            )],
        };

        // Create the pipeline
        let headers = auth_headers(auth);
        let (status, _body) = make_request(
            &app,
            Method::POST,
            "/api/e2e/pipelines",
            Some(headers),
            Some(serde_json::to_string(&pipeline_data).unwrap()),
        )
        .await;
        println!("test derived stream invalid timerange with cron frequency status: {status:?}");
        assert!(status.is_success());

        let pipeline = get_pipeline_from_api(pipeline_data.name.as_str()).await;

        // Create trigger that will cause invalid timerange with cron frequency
        let current_time = Utc::now().timestamp_micros();
        let dur_20_mins = Duration::try_minutes(20)
            .unwrap()
            .num_microseconds()
            .unwrap();
        let period_micros = Duration::try_minutes(5)
            .unwrap()
            .num_microseconds()
            .unwrap();

        // Say, this trigger was last run at 15 mins ago
        let last_end_time = current_time - dur_20_mins;
        // Current end time needs to be aligned (as this is cron frequency)
        let current_next_run_time =
            TriggerCondition::align_time(last_end_time + period_micros, 0, Some(300), None); // This will cause start > end
        let timeout = Duration::try_minutes(2)
            .unwrap()
            .num_microseconds()
            .unwrap();

        let trigger = Trigger {
            id: 3,
            org: "e2e".to_string(),
            module: TriggerModule::DerivedStream,
            module_key: format!(
                "logs/e2e/test_cron_invalid_timerange_pipeline/{}",
                pipeline.id
            ),
            next_run_at: current_next_run_time,
            start_time: Some(current_time),
            end_time: Some(current_time + timeout), // end < start
            is_realtime: false,
            is_silenced: false,
            status: config::meta::triggers::TriggerStatus::Processing,
            retries: 0,
            data: serde_json::json!({
                "period_end_time": last_end_time // This will cause start > end
            })
            .to_string(),
        };

        // Process the trigger
        let trace_id = "test_cron_invalid_timerange_trace_id";
        let result = handle_triggers(trace_id, trigger).await;

        // Should handle invalid timerange with cron frequency gracefully
        assert!(result.is_ok());

        // Get the trigger from the database
        let trigger = openobserve::service::db::scheduler::get(
            "e2e",
            TriggerModule::DerivedStream,
            &format!(
                "logs/e2e/test_cron_invalid_timerange_pipeline/{}",
                pipeline.id
            ),
        )
        .await;
        assert!(trigger.is_ok());
        let trigger = trigger.unwrap();
        // Next run at should be greater than current time
        assert!(trigger.next_run_at > current_next_run_time);
        // Next run at should be less than current time, because the frequency is 5 mins
        assert!(trigger.next_run_at < current_time);
        assert_eq!(
            trigger.data,
            serde_json::json!({
                "period_end_time": last_end_time
            })
            .to_string()
        );

        // Clean up
        let _ = openobserve::service::db::pipeline::delete(&pipeline.id).await;
        // Also delete the trigger job from scheduled jobs table
        let _ = openobserve::service::db::scheduler::delete(
            "e2e",
            TriggerModule::DerivedStream,
            &format!(
                "logs/e2e/test_cron_invalid_timerange_pipeline/{}",
                pipeline.id
            ),
        )
        .await;
    }

    async fn e2e_cleanup_test_pipeline() {
        // list the pipelines and choose the first one using API
        let auth = setup();
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) =
            make_request(&app, Method::GET, "/api/e2e/pipelines", Some(headers), None).await;
        assert!(status.is_success());
        let pipeline_list: openobserve::handler::http::models::pipelines::PipelineList =
            json::from_slice(&body).unwrap();
        let pipeline = pipeline_list.list.first();
        assert!(pipeline.is_some());
        let pipeline = pipeline.unwrap();

        // Clean up test pipelines
        let _ = openobserve::service::db::pipeline::delete(&pipeline.id).await;
    }

    async fn get_pipeline_from_api(pipeline_name: &str) -> http::models::pipelines::Pipeline {
        let auth = setup();
        // Check if pipeline was saved successfully by doing a list using API
        let app = init_test_router();
        let headers = auth_headers(auth);
        let (status, body) =
            make_request(&app, Method::GET, "/api/e2e/pipelines", Some(headers), None).await;
        assert!(status.is_success(), "Failed to list pipelines");
        let pipeline_response: openobserve::handler::http::models::pipelines::PipelineList =
            json::from_slice(&body).unwrap();
        // Get the pipeline that matches the pipeline name
        let pipeline = pipeline_response
            .list
            .iter()
            .find(|p| p.name == pipeline_name);
        assert!(pipeline.is_some(), "Pipeline not found");
        let pipeline = pipeline.unwrap();
        pipeline.clone()
    }

    // ==================== ENRICHMENT TABLE TESTS ====================

    async fn e2e_save_enrichment_data_new_table() {
        let _auth = setup();
        let org_id = "e2e";
        let table_name = "test_enrichment_table";

        // Create test data
        let mut payload = Vec::new();
        let mut record1 = json::Map::new();
        record1.insert("name".to_string(), json::Value::String("John".to_string()));
        record1.insert("age".to_string(), json::Value::String("25".to_string()));
        record1.insert(
            "city".to_string(),
            json::Value::String("New York".to_string()),
        );
        payload.push(record1);

        let mut record2 = json::Map::new();
        record2.insert("name".to_string(), json::Value::String("Jane".to_string()));
        record2.insert("age".to_string(), json::Value::String("30".to_string()));
        record2.insert(
            "city".to_string(),
            json::Value::String("Los Angeles".to_string()),
        );
        payload.push(record2);

        // Call save_enrichment_data
        let result = openobserve::service::enrichment_table::save_enrichment_data(
            org_id, table_name, payload, false, // append_data = false
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.status().is_success());

        // Verify schema was created in database
        let schema_exists = openobserve::service::schema::stream_schema_exists(
            org_id,
            table_name,
            config::meta::stream::StreamType::EnrichmentTables,
            &mut std::collections::HashMap::new(),
        )
        .await;

        println!("schema_exists: {schema_exists:?}");

        assert!(schema_exists.has_fields);

        // Verify schema cache was updated
        let schema_key = format!(
            "{}/{}/{}",
            org_id,
            config::meta::stream::StreamType::EnrichmentTables,
            table_name
        );
        let stream_schemas = STREAM_SCHEMAS.read().await;
        assert!(stream_schemas.contains_key(&schema_key));
        drop(stream_schemas);

        // Verify latest schema cache was updated
        let stream_schemas_latest = STREAM_SCHEMAS_LATEST.read().await;
        assert!(stream_schemas_latest.contains_key(&schema_key));
        drop(stream_schemas_latest);

        // Verify stream settings cache was updated
        let stream_settings = STREAM_SETTINGS.read().await;
        assert!(stream_settings.contains_key(&schema_key));
        drop(stream_settings);

        // Get the meta table stats for enrichment table
        let meta_table_stats =
            openobserve::service::db::enrichment_table::get_meta_table_stats(org_id, table_name)
                .await;
        assert!(meta_table_stats.is_some());
        let meta_table_stats = meta_table_stats.unwrap();
        assert_ne!(meta_table_stats.size, 0);
        assert_ne!(meta_table_stats.start_time, 0);

        // Check get_enrichment_table function, it should return same data
        let data =
            openobserve::service::enrichment::get_enrichment_table(org_id, table_name, false).await;
        assert!(data.is_ok());
        let data = data.unwrap();
        assert!(data.len() == 2);
        println!("save enrichment data new tabledata: {data:?}");
        println!(
            "save enrichment data new table data[0]: {:?}",
            data[0].get("name").unwrap().to_string()
        );
        assert!(data[0].get("name").unwrap().to_string().eq("\"John\""));
        assert!(data[1].get("name").unwrap().to_string().eq("\"Jane\""));
        assert!(data[0].get("age").unwrap().to_string().eq("\"25\""));
        assert!(data[1].get("age").unwrap().to_string().eq("\"30\""));
        assert!(data[0].get("city").unwrap().to_string().eq("\"New York\""));
        assert!(
            data[1]
                .get("city")
                .unwrap()
                .to_string()
                .eq("\"Los Angeles\"")
        );

        // wait for 1 second
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Check the ENRICHMENT_TABLES cache to check if the table is created
        let enrichment_tables = ENRICHMENT_TABLES.clone();
        println!("save enrichment data new table enrichment_tables: {enrichment_tables:?}");
        assert!(enrichment_tables.contains_key(&schema_key));
        assert!(enrichment_tables.get(&schema_key).unwrap().data.len() == 2);

        drop(enrichment_tables);

        println!("save enrichment data new table meta_table_stats: {meta_table_stats:?}");
        // Also, it should store the cache in the disk
        check_enrichment_table_local_disk_cache(org_id, table_name, meta_table_stats.end_time)
            .await;
        // Clean up
        e2e_cleanup_enrichment_table(org_id, table_name).await;
    }

    async fn e2e_cleanup_enrichment_table(org_id: &str, stream_name: &str) {
        // Clean up the enrichment table and its schema
        openobserve::service::enrichment_table::delete_enrichment_table(
            org_id,
            stream_name,
            config::meta::stream::StreamType::EnrichmentTables,
            (0, chrono::Utc::now().timestamp_micros()),
        )
        .await;

        // Verify schema caches are cleaned up
        let schema_key = format!(
            "{}/{}/{}",
            org_id,
            config::meta::stream::StreamType::EnrichmentTables,
            stream_name
        );

        // Check that schema caches are cleared
        let stream_schemas = STREAM_SCHEMAS.read().await;
        assert!(!stream_schemas.contains_key(&schema_key));
        drop(stream_schemas);

        let stream_schemas_latest = STREAM_SCHEMAS_LATEST.read().await;
        assert!(!stream_schemas_latest.contains_key(&schema_key));
        drop(stream_schemas_latest);

        let stream_settings = STREAM_SETTINGS.read().await;
        assert!(!stream_settings.contains_key(&schema_key));
        drop(stream_settings);

        // wait for 2 seconds
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Check the ENRICHMENT_TABLES cache to check if the table is deleted
        let enrichment_tables = ENRICHMENT_TABLES.clone();
        println!("enrichment data cleanup enrichment_tables: {enrichment_tables:?}");
        assert!(!enrichment_tables.contains_key(&schema_key));
        drop(enrichment_tables);

        // Check the local disk cache to check if the table is deleted
        check_enrichment_table_local_disk_cache_deleted(org_id, stream_name).await;
    }

    async fn e2e_save_enrichment_data_append_mode() {
        let _auth = setup();
        let org_id = "e2e";
        let table_name = "test_enrichment_table_append";

        // First, create initial data
        let mut initial_payload = Vec::new();
        let mut record1 = json::Map::new();
        record1.insert("name".to_string(), json::Value::String("John".to_string()));
        record1.insert("age".to_string(), json::Value::String("25".to_string()));
        record1.insert(
            "city".to_string(),
            json::Value::String("New York".to_string()),
        );
        initial_payload.push(record1);

        let result1 = openobserve::service::enrichment_table::save_enrichment_data(
            org_id,
            table_name,
            initial_payload,
            false, // append_data = false
        )
        .await;
        assert!(result1.is_ok());

        // Get the meta table stats for enrichment table
        let meta_table_stats_first =
            openobserve::service::db::enrichment_table::get_meta_table_stats(org_id, table_name)
                .await;
        assert!(meta_table_stats_first.is_some());
        let meta_table_stats_first = meta_table_stats_first.unwrap();
        assert_ne!(meta_table_stats_first.size, 0);
        assert_ne!(meta_table_stats_first.start_time, 0);
        let schema_key = format!(
            "{}/{}/{}",
            org_id,
            config::meta::stream::StreamType::EnrichmentTables,
            table_name
        );

        // Wait for 2 seconds
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Check the ENRICHMENT_TABLES cache to check if the table is created
        let enrichment_tables = ENRICHMENT_TABLES.clone();
        assert!(enrichment_tables.contains_key(&schema_key));
        assert!(enrichment_tables.get(&schema_key).unwrap().data.len() == 1);
        drop(enrichment_tables);

        println!(
            "save enrichment data append mode check_enrichment_table_local_disk_cache meta_table_stats_first.start_time 1: {:?}",
            meta_table_stats_first.start_time
        );
        // Check the local disk cache to check if the table is created
        check_enrichment_table_local_disk_cache(
            org_id,
            table_name,
            meta_table_stats_first.end_time,
        )
        .await;

        // Now append more data
        let mut append_payload = Vec::new();
        let mut record2 = json::Map::new();
        record2.insert("name".to_string(), json::Value::String("Jane".to_string()));
        record2.insert("age".to_string(), json::Value::String("30".to_string()));
        record2.insert(
            "city".to_string(),
            json::Value::String("Los Angeles".to_string()),
        );
        append_payload.push(record2);

        let result2 = openobserve::service::enrichment_table::save_enrichment_data(
            org_id,
            table_name,
            append_payload,
            true, // append_data = true
        )
        .await;
        assert!(result2.is_ok());

        // Verify schema still exists and is valid
        let schema_exists = openobserve::service::schema::stream_schema_exists(
            org_id,
            table_name,
            config::meta::stream::StreamType::EnrichmentTables,
            &mut std::collections::HashMap::new(),
        )
        .await;

        assert!(schema_exists.has_fields);

        // Get the meta table stats for enrichment table
        let meta_table_stats_second =
            openobserve::service::db::enrichment_table::get_meta_table_stats(org_id, table_name)
                .await;
        assert!(meta_table_stats_second.is_some());
        let meta_table_stats_second = meta_table_stats_second.unwrap();
        assert_ne!(meta_table_stats_second.size, 0);
        assert_ne!(meta_table_stats_second.start_time, 0);
        assert_eq!(
            meta_table_stats_second.start_time,
            meta_table_stats_first.start_time
        );
        assert!(meta_table_stats_second.end_time > meta_table_stats_first.end_time);

        // Wait for 2 seconds
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Check the ENRICHMENT_TABLES cache to check if the table is created
        let enrichment_tables = ENRICHMENT_TABLES.clone();
        assert!(enrichment_tables.contains_key(&schema_key));
        assert!(enrichment_tables.get(&schema_key).unwrap().data.len() == 2);
        drop(enrichment_tables);
        println!(
            "save enrichment data append mode check_enrichment_table_local_disk_cache meta_table_stats_second.start_time 2: {:?}",
            meta_table_stats_second.start_time
        );

        // Check the local disk cache to check if the table is created
        check_enrichment_table_local_disk_cache(
            org_id,
            table_name,
            meta_table_stats_second.end_time,
        )
        .await;

        // Clean up
        e2e_cleanup_enrichment_table(org_id, table_name).await;
    }

    async fn e2e_save_enrichment_data_schema_evolution() {
        let _auth = setup();
        let org_id = "e2e";
        let table_name = "test_enrichment_table_evolution";

        // First, create initial data with basic fields
        let mut initial_payload = Vec::new();
        let mut record1 = json::Map::new();
        record1.insert("name".to_string(), json::Value::String("John".to_string()));
        record1.insert("age".to_string(), json::Value::String("25".to_string()));
        initial_payload.push(record1);

        let result1 = openobserve::service::enrichment_table::save_enrichment_data(
            org_id,
            table_name,
            initial_payload,
            false, // append_data = false
        )
        .await;
        assert!(result1.is_ok());

        let schema_key = format!(
            "{}/{}/{}",
            org_id,
            config::meta::stream::StreamType::EnrichmentTables,
            table_name
        );

        // wait for 2 seconds
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Check the ENRICHMENT_TABLES cache to check if the table is created
        let enrichment_tables = ENRICHMENT_TABLES.clone();
        assert!(enrichment_tables.contains_key(&schema_key));
        assert!(enrichment_tables.get(&schema_key).unwrap().data.len() == 1);
        drop(enrichment_tables);

        // Now append data with additional fields (schema evolution)
        let mut append_payload = Vec::new();
        let mut record2 = json::Map::new();
        record2.insert("name".to_string(), json::Value::String("Jane".to_string()));
        record2.insert("age".to_string(), json::Value::String("30".to_string()));
        record2.insert(
            "city".to_string(),
            json::Value::String("Los Angeles".to_string()),
        ); // New field
        record2.insert(
            "country".to_string(),
            json::Value::String("USA".to_string()),
        ); // New field
        append_payload.push(record2);

        let result2 = openobserve::service::enrichment_table::save_enrichment_data(
            org_id,
            table_name,
            append_payload,
            true, // append_data = true
        )
        .await;
        assert!(result2.is_ok());
        let result = result2.unwrap();
        assert!(!result.status().is_success());
        // wait for 2 seconds
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Clean up
        e2e_cleanup_enrichment_table(org_id, table_name).await;
    }

    // Test runner function that calls all enrichment table tests
    async fn test_enrichment_table_integration() {
        e2e_save_enrichment_data_new_table().await;
        e2e_save_enrichment_data_append_mode().await;
        e2e_save_enrichment_data_schema_evolution().await;
    }

    async fn check_enrichment_table_local_disk_cache(
        org_id: &str,
        table_name: &str,
        updated_at: i64,
    ) {
        let key = get_key(org_id, table_name);
        let table_dir = get_table_dir(&key);
        let file_path = get_table_path(table_dir.to_str().unwrap(), updated_at);
        assert!(file_path.exists());
    }

    async fn check_enrichment_table_local_disk_cache_deleted(org_id: &str, table_name: &str) {
        let key = get_key(org_id, table_name);
        let table_dir = get_table_dir(&key);
        assert!(!table_dir.exists());
    }

    // Helper function to setup schema for enrichment tables in tests
    async fn setup_enrichment_table_schema(org_id: &str, table_name: &str) {
        use arrow::datatypes::{DataType, Field, Schema};
        use config::meta::stream::StreamType;

        // Create a simple schema that matches our test data
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("data", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
            Field::new("nested", DataType::Utf8, true),
            Field::new("special_chars", DataType::Utf8, true),
        ]);

        // Use infra::schema::merge to create/update the schema in the database
        let result = infra::schema::merge(
            org_id,
            table_name,
            StreamType::EnrichmentTables,
            &schema,
            None,
        )
        .await;

        // Ignore error if schema already exists
        match result {
            Ok(_) => {}
            Err(e) => {
                log::debug!("Schema setup for {}/{}: {:?}", org_id, table_name, e);
            }
        }
    }

    async fn test_enrichment_table_local_all_sequential() {
        // Run all tests sequentially to avoid directory conflicts
        test_store_and_retrieve().await;
        test_store_multiple_versions().await;
        test_retrieve_nonexistent_table().await;
        test_delete().await;
        test_delete_nonexistent_table().await;
        test_get_last_updated_at().await;
        test_store_data_if_needed().await;
        test_store_data_if_needed_background().await;
        test_metadata_persistence().await;
        test_large_data_handling().await;
        test_error_handling().await;
    }

    async fn test_store_and_retrieve() {
        let org_id = "test_org";
        let table_name = "test_table";
        let test_data = vec![
            json!({"id": 1, "name": "Alice", "age": 25}),
            json!({"id": 2, "name": "Bob", "age": 30}),
        ];
        let updated_at = 1640995200;

        // Setup schema before storing data
        setup_enrichment_table_schema(org_id, table_name).await;

        // Test store function
        let test_data = Values::Json(Arc::new(test_data));
        let result = local::store(org_id, table_name, test_data, updated_at).await;
        println!("Store result: {result:?}");
        assert!(result.is_ok(), "Store should succeed");

        // Verify file was created
        let key = get_key(org_id, table_name);
        let table_dir = get_table_dir(&key);
        let file_path = get_table_path(table_dir.to_str().unwrap(), updated_at);
        assert!(file_path.exists(), "Data file should exist");

        // Verify metadata was created
        let metadata_path = get_metadata_path();
        assert!(metadata_path.exists(), "Metadata file should exist");

        // Test retrieve function
        let retrieved_data = local::retrieve(org_id, table_name).await;
        assert!(retrieved_data.is_ok(), "Retrieve should succeed");

        let retrieved_data = retrieved_data.unwrap().to_json().unwrap();
        assert_eq!(retrieved_data.len(), 2, "Should retrieve 2 records");
        assert_eq!(
            retrieved_data[0]["id"],
            json!(1),
            "First record should match"
        );
        assert_eq!(
            retrieved_data[1]["id"],
            json!(2),
            "Second record should match"
        );
    }

    async fn test_store_multiple_versions() {
        let org_id = "test_org";
        let table_name = "versioned_table";

        // Setup schema before storing data
        setup_enrichment_table_schema(org_id, table_name).await;

        // Store first version
        let data_v1 = Values::Json(Arc::new(vec![json!({"id": 1, "version": "v1"})]));
        let updated_at_v1 = 1640995200;
        let result = local::store(org_id, table_name, data_v1, updated_at_v1).await;
        assert!(result.is_ok(), "First store should succeed");

        // Store second version
        let data_v2 = Values::Json(Arc::new(vec![json!({"id": 2, "version": "v2"})]));
        let updated_at_v2 = 1640995300;
        let result = local::store(org_id, table_name, data_v2, updated_at_v2).await;
        assert!(result.is_ok(), "Second store should succeed");

        // Retrieve should get both versions
        let retrieved_data = local::retrieve(org_id, table_name).await.unwrap();
        assert_eq!(retrieved_data.len(), 2, "Should retrieve both versions");
    }

    async fn test_retrieve_nonexistent_table() {
        let result = local::retrieve("nonexistent_org", "nonexistent_table").await;
        assert!(
            result.is_err(),
            "Should return an error for nonexistent table"
        );
    }

    async fn test_delete() {
        let org_id = "test_org";
        let table_name = "delete_test_table";
        let test_data = Values::Json(Arc::new(vec![json!({"id": 1, "data": "test"})]));
        let updated_at = 1640995200;

        // Setup schema before storing data
        setup_enrichment_table_schema(org_id, table_name).await;

        // First store some data
        let result = local::store(org_id, table_name, test_data, updated_at).await;
        assert!(result.is_ok(), "Store should succeed");

        // Verify data exists
        let key = get_key(org_id, table_name);
        let table_dir = get_table_dir(&key);
        assert!(table_dir.exists(), "Table directory should exist");

        // Test delete function
        let result = local::delete(org_id, table_name).await;
        assert!(result.is_ok(), "Delete should succeed");

        // Verify directory was removed
        assert!(!table_dir.exists(), "Table directory should be removed");

        // Verify metadata was updated
        let metadata_content = get_metadata_content().await.unwrap();
        assert!(
            !metadata_content.contains_key(&key),
            "Key should be removed from metadata"
        );
    }

    async fn test_delete_nonexistent_table() {
        // Delete non-existent table should succeed gracefully
        let result = local::delete("nonexistent_org", "nonexistent_table").await;
        assert!(
            result.is_ok(),
            "Delete should handle nonexistent table gracefully"
        );
    }

    async fn test_get_last_updated_at() {
        let org_id = "test_org";
        let table_name = "timestamp_test_table";
        let test_data = Values::Json(Arc::new(vec![json!({"id": 1, "data": "test"})]));
        let updated_at = 1640995200;

        // Test getting timestamp before storing data
        let result = local::get_last_updated_at(org_id, table_name).await;
        assert!(result.is_ok(), "Get last updated should succeed");
        assert_eq!(result.unwrap(), 0, "Should return 0 for non-existent table");

        // Setup schema before storing data
        setup_enrichment_table_schema(org_id, table_name).await;

        // Store data
        local::store(org_id, table_name, test_data, updated_at)
            .await
            .unwrap();

        // Test getting timestamp after storing data
        let result = local::get_last_updated_at(org_id, table_name).await;
        assert!(result.is_ok(), "Get last updated should succeed");
        assert_eq!(
            result.unwrap(),
            updated_at,
            "Should return correct timestamp"
        );
    }

    async fn test_store_data_if_needed() {
        let org_id = "test_org";
        let table_name = "conditional_test_table";
        let test_data_v1 = Values::Json(Arc::new(vec![json!({"id": 1, "version": "v1"})]));
        let test_data_v2 = Values::Json(Arc::new(vec![json!({"id": 1, "version": "v2"})]));
        let updated_at_v1 = 1640995200;
        let updated_at_v2 = 1640995300;

        // Setup schema before storing data
        setup_enrichment_table_schema(org_id, table_name).await;

        // Store initial data
        let result =
            local::store_data_if_needed(org_id, table_name, test_data_v1, updated_at_v1).await;
        assert!(result.is_ok(), "Initial store should succeed");

        // Verify data was stored
        let retrieved_data = local::retrieve(org_id, table_name)
            .await
            .unwrap()
            .to_json()
            .unwrap();
        assert_eq!(
            retrieved_data[0]["version"],
            json!("v1"),
            "Should have v1 data"
        );

        // Try to store older data (should be ignored)
        let old_updated_at = 1640995100;
        let result =
            local::store_data_if_needed(org_id, table_name, test_data_v2.clone(), old_updated_at)
                .await;
        assert!(
            result.is_ok(),
            "Store with old timestamp should succeed but not update"
        );

        // Verify data wasn't changed
        let retrieved_data = local::retrieve(org_id, table_name)
            .await
            .unwrap()
            .to_json()
            .unwrap();
        assert_eq!(
            retrieved_data[0]["version"],
            json!("v1"),
            "Should still have v1 data"
        );

        // Store newer data (should update)
        let result =
            local::store_data_if_needed(org_id, table_name, test_data_v2, updated_at_v2).await;
        assert!(result.is_ok(), "Store with newer timestamp should succeed");

        // Verify data was updated
        let retrieved_data = local::retrieve(org_id, table_name)
            .await
            .unwrap()
            .to_json()
            .unwrap();
        assert_eq!(
            retrieved_data[0]["version"],
            json!("v2"),
            "Should now have v2 data"
        );
    }

    async fn test_store_data_if_needed_background() {
        let org_id = "test_org";
        let table_name = "background_test_table";
        let test_data = Values::Json(Arc::new(vec![json!({"id": 1, "data": "background"})]));
        let updated_at = 1640995200;

        // Setup schema before storing data
        setup_enrichment_table_schema(org_id, table_name).await;

        // Test background store
        let result =
            local::store_data_if_needed_background(org_id, table_name, test_data, updated_at).await;
        assert!(result.is_ok(), "Background store should succeed");

        // Wait a bit for background task to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify data was eventually stored
        let retrieved_data = local::retrieve(org_id, table_name)
            .await
            .unwrap()
            .to_json()
            .unwrap();
        assert_eq!(retrieved_data.len(), 1, "Should have stored 1 record");
        assert_eq!(
            retrieved_data[0]["data"],
            json!("background"),
            "Should have correct data"
        );
    }

    async fn test_metadata_persistence() {
        let org_id = "test_org";
        let table_name = "metadata_test_table";
        let test_data = Values::Json(Arc::new(vec![json!({"id": 1, "data": "test"})]));
        let updated_at = 1640995200;

        // Setup schema before storing data
        setup_enrichment_table_schema(org_id, table_name).await;

        // Store data
        local::store(org_id, table_name, test_data, updated_at)
            .await
            .unwrap();

        // Manually read metadata file
        let metadata_path = get_metadata_path();
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await.unwrap();
        let metadata: std::collections::HashMap<String, i64> =
            serde_json::from_str(&metadata_content).unwrap();

        let key = get_key(org_id, table_name);
        assert!(
            metadata.contains_key(&key),
            "Metadata should contain the key"
        );
        assert_eq!(
            metadata[&key], updated_at,
            "Metadata should have correct timestamp"
        );
    }

    async fn test_large_data_handling() {
        let org_id = "test_org";
        let table_name = "large_data_table";

        // Setup schema before storing data
        setup_enrichment_table_schema(org_id, table_name).await;

        // Create large dataset (1000 records)
        let large_data: Vec<_> = (0..1000)
            .map(|i| {
                json!({
                    "id": i,
                    "name": format!("name_{}", i),
                    "data": "x".repeat(100), // 100 characters per record
                    "nested": {
                        "field1": i * 2,
                        "field2": format!("nested_value_{}", i)
                    }
                })
            })
            .collect();

        let updated_at = 1640995200;

        // Test storing large data
        let large_data = Values::Json(Arc::new(large_data));
        let result = local::store(org_id, table_name, large_data, updated_at).await;
        assert!(result.is_ok(), "Should handle large data successfully");

        // Test retrieving large data
        let retrieved_data = local::retrieve(org_id, table_name)
            .await
            .unwrap()
            .to_json()
            .unwrap();
        assert_eq!(
            retrieved_data.len(),
            1000,
            "Should retrieve all 1000 records"
        );
        assert_eq!(
            retrieved_data[0]["id"],
            json!(0),
            "First record should be correct"
        );
        assert_eq!(
            retrieved_data[999]["id"],
            json!(999),
            "Last record should be correct"
        );
    }

    async fn test_error_handling() {
        // Test with invalid JSON data
        let org_id = "test_org";
        let table_name = "error_test_table";

        // Setup schema before storing data
        setup_enrichment_table_schema(org_id, table_name).await;

        // Create data that will serialize fine but test edge cases
        let test_data = Values::Json(Arc::new(vec![
            json!({"id": 1, "data": null}),
            json!({"id": 2, "special_chars": "!@#$%^&*()"}),
        ]));
        let updated_at = 1640995200;

        let result = local::store(org_id, table_name, test_data, updated_at).await;
        assert!(
            result.is_ok(),
            "Should handle special characters and null values"
        );

        let retrieved_data = local::retrieve(org_id, table_name)
            .await
            .unwrap()
            .to_json()
            .unwrap();
        assert_eq!(retrieved_data.len(), 2, "Should retrieve both records");
        assert!(
            retrieved_data[0]["data"].is_null(),
            "Should preserve null values"
        );
    }

    // ========================================================================
    // Backfill Jobs Integration Tests
    // ========================================================================

    async fn test_backfill_job_list_and_delete() {
        use openobserve::service::alerts::backfill::{delete_backfill_job, list_backfill_jobs};

        // Test listing backfill jobs
        let org_id = "e2e";
        let jobs_result = list_backfill_jobs(org_id).await;
        assert!(
            jobs_result.is_ok(),
            "Should successfully list backfill jobs"
        );

        let jobs = jobs_result.unwrap();
        // If there are any jobs, test delete functionality
        if let Some(first_job) = jobs.first() {
            let job_id = first_job.job_id.clone();
            let delete_result = delete_backfill_job(org_id, &job_id).await;
            // Delete may fail if job is in progress, which is acceptable
            match delete_result {
                Ok(_) => log::info!("Successfully deleted backfill job: {}", job_id),
                Err(e) => log::warn!("Could not delete backfill job (may be in progress): {}", e),
            }
        }
    }

    async fn test_backfill_job_get_nonexistent() {
        use openobserve::service::alerts::backfill::get_backfill_job;

        // Test getting a non-existent job
        let org_id = "e2e";
        let fake_job_id = "nonexistent_job_12345";
        let result = get_backfill_job(org_id, fake_job_id).await;
        assert!(result.is_err(), "Should fail to get non-existent job");
    }

    async fn test_backfill_job_delete_by_pipeline() {
        use openobserve::service::alerts::backfill::delete_backfill_jobs_by_pipeline;

        // Test deleting jobs by pipeline
        let org_id = "e2e";
        let pipeline_id = "test_nonexistent_pipeline";

        // Should succeed even if no jobs exist for this pipeline
        let result = delete_backfill_jobs_by_pipeline(org_id, pipeline_id).await;
        assert!(
            result.is_ok(),
            "Should successfully delete jobs by pipeline (even if none exist)"
        );
    }

    async fn test_backfill_job_enable_disable() {
        use openobserve::service::alerts::backfill::{enable_backfill_job, list_backfill_jobs};

        // Test enable/disable on existing jobs
        let org_id = "e2e";
        let jobs_result = list_backfill_jobs(org_id).await;

        if let Ok(jobs) = jobs_result
            && let Some(first_job) = jobs.first()
        {
            let job_id = first_job.job_id.clone();

            // Try to disable
            let disable_result = enable_backfill_job(org_id, &job_id, false).await;
            match disable_result {
                Ok(_) => {
                    log::info!("Successfully disabled backfill job: {}", job_id);

                    // Try to enable back
                    let enable_result = enable_backfill_job(org_id, &job_id, true).await;
                    if enable_result.is_ok() {
                        log::info!("Successfully re-enabled backfill job: {}", job_id);
                    }
                }
                Err(e) => log::warn!("Could not modify job state: {}", e),
            }
        }
    }
}
