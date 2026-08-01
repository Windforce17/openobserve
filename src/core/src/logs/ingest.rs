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
    collections::{HashMap, HashSet},
    io::{BufRead, Cursor, Read},
};

use axum::http;
use chrono::Utc;
#[cfg(feature = "cloud")]
use config::meta::self_reporting::usage::is_reserved_self_reporting_stream;
use config::{
    ID_COL_NAME, ORIGINAL_DATA_COL_NAME, TIMESTAMP_COL_NAME,
    meta::{
        self_reporting::usage::UsageType,
        stream::{StreamParams, StreamType},
    },
    metrics,
    utils::{
        flatten,
        json::{self, estimate_json_bytes},
        time::{now_micros, parse_timestamp_micro_from_value},
    },
};
use flate2::read::GzDecoder;
use infra::{
    errors::{Error, Result},
    schema::get_flatten_level,
};
#[cfg(feature = "vectorscan")]
use o2_enterprise::enterprise::re_patterns::get_pattern_manager;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    metrics::v1::metric::Data,
};
use prost::Message;
use serde_json::json;

use super::{bulk::TS_PARSE_FAILED, ingestion_log_enabled, log_failed_record};
use crate::{
    common::meta::ingestion::{
        AWSRecordType, BulkResponse, GCPIngestionResponse, IngestUser, IngestionData,
        IngestionDataIter, IngestionError, IngestionRequest, IngestionResponse, IngestionStatus,
        IngestionValueType, KinesisFHIngestionResponse, StreamStatus,
    },
    service::{
        format_stream_name, get_formatted_stream_name,
        ingestion::check_ingestion_allowed,
        logs::bulk::TRANSFORM_FAILED,
        schema::{get_future_discard_error, get_upto_discard_error},
    },
};

type LogDataByStream = HashMap<String, (Vec<(i64, json::Map<String, json::Value>)>, Option<usize>)>;

struct FinalizeRecordContext<'a> {
    stream_name: &'a str,
    org_id: &'a str,
    flatten_level: u32,
    min_ts: i64,
    max_ts: i64,
    streams_need_original_map: &'a HashMap<String, bool>,
    need_usage_report: bool,
    log_ingestion_errors: bool,
    stream_status: &'a mut StreamStatus,
    json_data_by_stream: &'a mut LogDataByStream,
    /// `Some` for the bulk shape: a record rejected before the write claims a
    /// bulk item here, so the response still carries one item per action
    pre_write_bulk: &'a mut Option<BulkResponse>,
}

/// Claim a bulk item for a record rejected BEFORE the write. The ES bulk
/// contract is one response item per action: counting the rejection only on
/// the aggregate status left the items array short and `errors: false`, so
/// positional clients (Filebeat/Logstash) never saw the loss.
fn pre_write_bulk_item(
    pre_write_bulk: &mut Option<BulkResponse>,
    stream_name: &str,
    doc_id: Option<String>,
    err_type: &str,
    error: &str,
) {
    if let Some(bulk) = pre_write_bulk.as_mut() {
        bulk.errors = true;
        super::bulk::add_record_status(
            stream_name.to_string(),
            doc_id,
            "".to_string(),
            None,
            bulk,
            Some(err_type.to_string()),
            Some(error.to_string()),
        );
    }
}

pub async fn ingest(
    thread_id: usize,
    org_id: &str,
    in_stream_name: &str,
    in_req: IngestionRequest,
    user: IngestUser,
    extend_json: Option<&HashMap<String, serde_json::Value>>,
    is_derived: bool,
) -> Result<IngestionResponse> {
    let start = std::time::Instant::now();
    let started_at: i64 = Utc::now().timestamp_micros();
    let cfg = config::get_config();
    let need_usage_report = in_req.should_report_usage();
    let log_ingestion_errors = ingestion_log_enabled().await;
    #[cfg(feature = "vectorscan")]
    let pattern_manager = get_pattern_manager().await?;
    let stream_type = StreamType::Logs;

    // check stream
    let stream_name = if cfg.common.skip_formatting_stream_name {
        get_formatted_stream_name(StreamParams::new(org_id, in_stream_name, stream_type)).await?
    } else {
        format_stream_name(in_stream_name.to_string())
    };
    if stream_name.is_empty() {
        return Err(Error::IngestionError("Stream name is empty".to_string()));
    }

    // Block user ingestion into reserved self-reporting streams
    // (usage/stats/triggers/errors/...). The internal self-reporting job writes
    // these via `IngestionRequest::Usage` (for which `should_report_usage()` is
    // false → `need_usage_report == false`), so it is exempt; any other request
    // targeting a reserved stream is a user write and is rejected. Cloud-only:
    // OSS / self-hosted may legitimately use these stream names.
    #[cfg(feature = "cloud")]
    if need_usage_report && is_reserved_self_reporting_stream(&stream_name) {
        return Err(Error::IngestionError(format!(
            "stream '{stream_name}' is reserved and cannot be ingested into"
        )));
    }

    // check system resource
    check_ingestion_allowed(org_id, stream_type, Some(&stream_name)).await?;

    let now = now_micros();
    let min_ts = now - cfg.limit.ingest_allowed_upto_micro;
    let max_ts = now + cfg.limit.ingest_allowed_in_future_micro;

    let mut derived_streams = HashSet::new();
    if is_derived {
        derived_streams.insert(stream_name.to_string());
    }

    // Start retrieve associated pipeline and construct pipeline components
    let stream_param = StreamParams::new(org_id, &stream_name, stream_type);
    let executable_pipelines =
        crate::service::ingestion::get_stream_executable_pipelines(&stream_param).await;
    let mut stream_params = vec![stream_param];
    let mut pipeline_inputs = Vec::with_capacity(stream_params.len());
    let mut original_options = Vec::with_capacity(stream_params.len());
    // End pipeline params construction

    if !executable_pipelines.is_empty() {
        for exec_pl in &executable_pipelines {
            let pl_destinations = exec_pl.get_all_destination_streams();
            stream_params.extend(pl_destinations);
        }
    }

    // Start get streams that store the original record
    let mut streams_need_original_map: HashMap<String, bool> = HashMap::new();
    crate::service::ingestion::get_original_data_streams(
        &stream_params,
        &mut streams_need_original_map,
    )
    .await;
    // with pipeline, we need to store original if any of the destinations requires original
    let store_original_when_pipeline_exists =
        !executable_pipelines.is_empty() && streams_need_original_map.values().any(|val| *val);
    // End get streams that store the original record

    let flatten_level = get_flatten_level(org_id, &stream_name, stream_type).await;

    let json_req: Vec<json::Value>; // to hold json request because of borrow checker
    let (endpoint, usage_type, data) = match in_req {
        IngestionRequest::JSON(req) => {
            json_req = json::from_slice(&req).unwrap_or({
                let val: json::Value = json::from_slice(&req)?;
                vec![val]
            });
            (
                "/api/org/ingest/logs/_json",
                UsageType::Json,
                IngestionData::JSON(json_req),
            )
        }
        IngestionRequest::Multi(req) => (
            "/api/org/ingest/logs/_multi",
            UsageType::Multi,
            IngestionData::Multi(req),
        ),
        IngestionRequest::JsonValues(IngestionValueType::Bulk, logs) => (
            "/api/org/ingest/logs/_bulk",
            UsageType::Bulk,
            IngestionData::JSON(logs),
        ),
        IngestionRequest::JsonValues(IngestionValueType::Hec, logs) => (
            "/api/org/ingest/logs/_hec",
            UsageType::Hec,
            IngestionData::JSON(logs),
        ),
        IngestionRequest::JsonValues(IngestionValueType::Loki, logs) => (
            "/api/org/ingest/logs/_loki",
            UsageType::Loki,
            IngestionData::JSON(logs),
        ),
        IngestionRequest::GCP(req) => (
            "/api/org/ingest/logs/_gcs",
            UsageType::GCPSubscription,
            IngestionData::GCP(req),
        ),
        IngestionRequest::KinesisFH(req) => (
            "/api/org/ingest/logs/_kinesis",
            UsageType::KinesisFirehose,
            IngestionData::KinesisFH(req),
        ),
        IngestionRequest::RUM(req) => (
            "/api/org/ingest/logs/_rum",
            UsageType::RUM,
            IngestionData::Multi(req),
        ),
        IngestionRequest::Usage(req) => {
            json_req = json::from_slice(&req).unwrap_or({
                let val: json::Value = json::from_slice(&req)?;
                vec![val]
            });
            (
                "/api/org/ingest/logs/_usage",
                UsageType::Json,
                IngestionData::JSON(json_req),
            )
        }
    };

    let mut stream_status = StreamStatus::new(&stream_name);
    // bulk items for records rejected BEFORE the write (flatten failure, bad
    // timestamp, a pipeline batch error): the ES bulk contract is one item
    // per action, so these must claim items too — None for every other shape
    let mut pre_write_bulk: Option<BulkResponse> =
        (usage_type == UsageType::Bulk).then(|| BulkResponse {
            took: 0,
            errors: false,
            items: vec![],
        });
    let mut json_data_by_stream: LogDataByStream = HashMap::new();
    let mut size_by_stream = HashMap::new();
    for ret in data.iter() {
        let mut item = match ret {
            Ok(item) => item,
            Err(e) => {
                log::error!("IngestionError: {e:?}");
                return Err(Error::IngestionError(format!("Failed processing: {e:?}")));
            }
        };

        if let Some(extend) = extend_json.as_ref() {
            for (key, val) in extend.iter() {
                item[key] = val.clone();
            }
        }

        // store a copy of original data before it's being transformed and/or flattened, when
        // 1. original data is an object
        let original_data = if item.is_object() {
            // 2. current stream does not have pipeline
            if executable_pipelines.is_empty() {
                // current stream requires original
                streams_need_original_map
                    .get(&stream_name)
                    .is_some_and(|v| *v)
                    .then(|| item.to_string())
            } else {
                // 3. with pipeline, storing original as long as streams_need_original_set is not
                //    empty
                // because not sure the pipeline destinations
                store_original_when_pipeline_exists.then(|| item.to_string())
            }
        } else {
            None // `item` won't be flattened, no need to store original
        };

        // we report stream size before pushing data to pipeline
        // this is to capture the actual size of stream at the time of ingestion
        let size: &mut usize = size_by_stream.entry(stream_name.clone()).or_insert(0);
        *size += estimate_json_bytes(&item);

        if !executable_pipelines.is_empty() {
            // buffer the records, timestamp, and originals for pipeline batch processing
            pipeline_inputs.push(item);
            original_options.push(original_data);
        } else if !finalize_and_buffer_record(
            item,
            original_data,
            &mut FinalizeRecordContext {
                stream_name: &stream_name,
                org_id,
                flatten_level,
                min_ts,
                max_ts,
                streams_need_original_map: &streams_need_original_map,
                need_usage_report,
                log_ingestion_errors,
                stream_status: &mut stream_status,
                json_data_by_stream: &mut json_data_by_stream,
                pre_write_bulk: &mut pre_write_bulk,
            },
        ) {
            continue;
        }
        tokio::task::coop::consume_budget().await;
    }

    // batch process records through pipeline
    if !executable_pipelines.is_empty() {
        let records_count = pipeline_inputs.len();
        let mut evaluation_tasks = tokio::task::JoinSet::new();
        for exec_pl in &executable_pipelines {
            if exec_pl.kind == config::meta::pipeline::PipelineKind::Evaluation
                && exec_pl.contains_llm_evaluation_node()
            {
                let exec_pl = exec_pl.clone();
                let org_id = org_id.to_string();
                let stream_name = stream_name.clone();
                let records = pipeline_inputs.clone();
                evaluation_tasks.spawn(async move {
                    if let Err(e) = exec_pl
                        .process_batch(&org_id, records, Some(stream_name.clone()))
                        .await
                    {
                        log::error!(
                            "[Pipeline] evaluation pipeline for stream {org_id}/{stream_name}: Batch execution error: {e}.",
                        );
                    }
                });
                continue;
            }

            let pipeline_start = std::time::Instant::now();
            let pl_result = exec_pl
                .process_batch(org_id, pipeline_inputs.clone(), Some(stream_name.clone()))
                .await;
            if cfg.common.print_key_event {
                // Pipeline wall-time vs total ingest elapsed so far, to see the realtime
                // pipeline's share of ingestion latency at the ingest layer.
                log::info!(
                    "[Pipeline:Timing] ingest org={org_id} stream={stream_name} pipeline={} records={records_count} pipeline_ms={} ingest_elapsed_ms={}",
                    exec_pl.get_pipeline_name(),
                    pipeline_start.elapsed().as_millis(),
                    start.elapsed().as_millis(),
                );
            }
            match pl_result {
                Err(e) => {
                    log::error!(
                        "[Pipeline] for stream {org_id}/{stream_name}: Batch execution error: {e}.",
                    );
                    stream_status.status.failed += records_count as u32;
                    stream_status.status.error = format!("Pipeline batch execution error: {e}");
                    // the whole batch was rejected before the write: every
                    // record claims its bulk item so none vanishes silently
                    let error = format!("Pipeline batch execution error: {e}");
                    for _ in 0..records_count {
                        pre_write_bulk_item(
                            &mut pre_write_bulk,
                            &stream_name,
                            None,
                            super::bulk::PIPELINE_EXEC_FAILED,
                            &error,
                        );
                    }
                    metrics::INGEST_ERRORS
                        .with_label_values(&[
                            org_id,
                            StreamType::Logs.as_str(),
                            &stream_name,
                            TRANSFORM_FAILED,
                        ])
                        .inc();
                }
                Ok(pl_results) => {
                    let function_no = exec_pl.num_of_func();
                    for (stream_params, stream_pl_results) in pl_results {
                        if stream_params.stream_type != StreamType::Logs {
                            continue;
                        }

                        let destination_stream = stream_params.stream_name.to_string();
                        if !derived_streams.contains(&destination_stream) {
                            derived_streams.insert(destination_stream.clone());
                        }

                        if !streams_need_original_map.contains_key(&destination_stream) {
                            // a new dynamically created stream. need to check the map again
                            crate::service::ingestion::get_original_data_streams(
                                &[stream_params],
                                &mut streams_need_original_map,
                            )
                            .await;
                        }

                        for (idx, mut res) in stream_pl_results {
                            // handle timestamp
                            let timestamp = match handle_timestamp(&mut res, min_ts, max_ts) {
                                Ok(ts) => ts,
                                Err(e) => {
                                    stream_status.status.failed += 1;
                                    stream_status.status.error = e.to_string();
                                    metrics::INGEST_ERRORS
                                        .with_label_values(&[
                                            org_id,
                                            StreamType::Logs.as_str(),
                                            &stream_name,
                                            TS_PARSE_FAILED,
                                        ])
                                        .inc();
                                    log_failed_record(log_ingestion_errors, &res, &e.to_string());
                                    continue;
                                }
                            };

                            let original_size = estimate_json_bytes(&res);

                            // get json object
                            let mut local_val = match res.take() {
                                json::Value::Object(val) => val,
                                _ => unreachable!(),
                            };

                            // usize::MAX used as a flag when pipeline is applied with
                            // ResultArray vrl
                            //  - invalid original_data
                            // add `_original` and '_record_id` if required by StreamSettings
                            if idx != usize::MAX
                                && streams_need_original_map
                                    .get(&destination_stream)
                                    .is_some_and(|v| *v)
                                && original_options[idx].is_some()
                            {
                                local_val.insert(
                                    ORIGINAL_DATA_COL_NAME.to_string(),
                                    original_options[idx].clone().unwrap().into(),
                                );
                                let record_id = crate::service::ingestion::generate_record_id(
                                    org_id,
                                    &destination_stream,
                                    &StreamType::Logs,
                                );
                                local_val.insert(
                                    ID_COL_NAME.to_string(),
                                    json::Value::String(record_id.to_string()),
                                );
                            }

                            let (ts_data, fn_num) = json_data_by_stream
                                .entry(destination_stream.clone())
                                .or_insert_with(|| (Vec::new(), None));
                            ts_data.push((timestamp, local_val));
                            *fn_num = need_usage_report.then_some(function_no);

                            // Since we report the size for the original stream before the
                            // pipeline execution we need to
                            // skip reporting the actual size on disk.
                            if destination_stream.ne(&stream_name) {
                                let size = size_by_stream
                                    .entry(destination_stream.clone())
                                    .or_insert(0);
                                *size += original_size;
                            }

                            tokio::task::coop::consume_budget().await;
                        }
                    }
                }
            }
        } // for each pipeline

        while let Some(result) = evaluation_tasks.join_next().await {
            if let Err(e) = result {
                log::error!(
                    "[Pipeline] evaluation pipeline task for stream {org_id}/{stream_name} failed: {e}.",
                );
            }
        }

        // When only evaluation pipelines exist for this stream (no user pipeline
        // is responsible for writing to the source stream), preserve original
        // records by writing them back to the source stream.
        let has_user_pipeline = executable_pipelines
            .iter()
            .any(|p| p.kind == config::meta::pipeline::PipelineKind::User);
        let has_evaluation_pipeline = executable_pipelines
            .iter()
            .any(|p| p.kind == config::meta::pipeline::PipelineKind::Evaluation);
        log::debug!(
            "[LOGS] source preservation check stream={stream_name}, pipelines={}, has_user_pipeline={has_user_pipeline}, has_evaluation_pipeline={has_evaluation_pipeline}, source_buffered={}",
            executable_pipelines.len(),
            json_data_by_stream.contains_key(&stream_name)
        );
        if !has_user_pipeline && !json_data_by_stream.contains_key(&stream_name) {
            for (idx, item) in pipeline_inputs.iter().enumerate() {
                let _ = finalize_and_buffer_record(
                    item.clone(),
                    original_options[idx].clone(),
                    &mut FinalizeRecordContext {
                        stream_name: &stream_name,
                        org_id,
                        flatten_level,
                        min_ts,
                        max_ts,
                        streams_need_original_map: &streams_need_original_map,
                        need_usage_report,
                        log_ingestion_errors,
                        stream_status: &mut stream_status,
                        json_data_by_stream: &mut json_data_by_stream,
                        pre_write_bulk: &mut pre_write_bulk,
                    },
                );
            }
        }
    }

    // if no data is left to write, fast return — but never silently: every
    // record may already have been counted failed (bad timestamps, records
    // that would not flatten, a pipeline batch failure...)
    if json_data_by_stream.is_empty() {
        return Ok(empty_buffer_response(stream_status, pre_write_bulk.take()));
    }

    // drop memory-intensive variables
    drop(streams_need_original_map);
    drop(executable_pipelines);
    drop(original_options);

    #[cfg(feature = "vectorscan")]
    {
        for (stream, data) in json_data_by_stream.iter_mut() {
            match pattern_manager.process_at_ingestion(
                org_id,
                StreamType::Logs,
                stream,
                &mut data.0,
            ) {
                Ok(_) => {}
                Err(e) => {
                    log::error!(
                        "error in processing records for patterns for stream {stream} : {e}"
                    );
                }
            }
        }
    }

    #[allow(clippy::type_complexity)]
    let (metric_rpt_status_code, response_body, response_code, response_error): (
        &str,
        StreamStatus,
        u16,
        Option<String>,
    ) = {
        let mut status = if usage_type == UsageType::Bulk {
            // seeded with the items of records rejected before the write, so
            // the response still carries one item per action
            IngestionStatus::Bulk(pre_write_bulk.take().unwrap_or(BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            }))
        } else {
            IngestionStatus::Record(stream_status.status.clone())
        };
        let write_result = super::write_logs_by_stream(
            thread_id,
            org_id,
            &user.to_email(),
            (started_at, &start),
            usage_type,
            &mut status,
            json_data_by_stream,
            size_by_stream,
            derived_streams,
        )
        .await;
        match status {
            IngestionStatus::Record(status) => {
                stream_status.status = status;
            }
            IngestionStatus::Bulk(items) => {
                stream_status.items = items.items;
            }
        };
        build_ingestion_response(stream_status, write_result)
    };

    // update ingestion metrics
    let took_time = start.elapsed().as_secs_f64();
    // Bulk requests are counted by the bulk handler (bulk.rs) once per HTTP request.
    // Counting here would result in N increments (one per stream) plus 1 from bulk.rs.
    if !matches!(usage_type, UsageType::Bulk) {
        metrics::HTTP_RESPONSE_TIME
            .with_label_values(&[
                endpoint,
                metric_rpt_status_code,
                org_id,
                StreamType::Logs.as_str(),
                "",
                "",
            ])
            .observe(took_time);
        metrics::HTTP_INCOMING_REQUESTS
            .with_label_values(&[
                endpoint,
                metric_rpt_status_code,
                org_id,
                StreamType::Logs.as_str(),
                "",
                "",
            ])
            .inc();
    }

    Ok(IngestionResponse {
        code: response_code,
        status: vec![response_body],
        error: response_error,
    })
}

/// The response for a request none of whose records reached the write
/// buffer. An empty buffer is not automatically a clean 200: when records
/// were counted failed before buffering (bad timestamps, records that would
/// not flatten, a pipeline batch failure), the accumulated failure state is
/// reported on the response `error`. The code stays 200 — these records were
/// rejected as data, resending the same bytes cannot help.
fn empty_buffer_response(
    mut stream_status: StreamStatus,
    pre_write_bulk: Option<BulkResponse>,
) -> IngestionResponse {
    // the bulk shape carries its rejections as ITEMS too — one per action —
    // so a caller that walks the items array never sees it short
    if let Some(bulk) = pre_write_bulk {
        stream_status.items = bulk.items;
    }
    let error = (stream_status.status.failed > 0).then(|| stream_status.status.error.clone());
    IngestionResponse {
        code: http::StatusCode::OK.into(),
        status: vec![stream_status],
        error,
    }
}

/// Turn an accounted ingest into its response: the metric status label, the
/// per-stream status, the response code and the error message.
///
/// A stream whose durable write failed is NEVER a 2xx — the records are not
/// stored and the caller that acks must resend them. The one exception is a
/// pure PERMANENT rejection (e.g. every failing stream is being deleted):
/// the code stays 200 because retrying cannot help, but the error still says
/// what was dropped and why. `write_logs_by_stream` has already moved the
/// failed counts out of `successful` into `failed`, so the body reports the
/// loss too.
fn build_ingestion_response(
    stream_status: StreamStatus,
    write_result: Result<Vec<super::StreamWriteFailure>>,
) -> (&'static str, StreamStatus, u16, Option<String>) {
    match write_result {
        Ok(failures) if failures.is_empty() => {
            ("200", stream_status, http::StatusCode::OK.as_u16(), None)
        }
        Ok(failures) => {
            let error = super::write_failure_message(&failures);
            log::error!("Error while writing logs: {error}");
            let code = super::write_failure_status_code(&failures);
            let metric = if (200..300).contains(&code) {
                "200"
            } else {
                "500"
            };
            (metric, stream_status, code, Some(error))
        }
        Err(e) => {
            log::error!("Error while writing logs: {e}");
            let code = if matches!(e, Error::ResourceError(_)) {
                http::StatusCode::SERVICE_UNAVAILABLE.as_u16()
            } else {
                http::StatusCode::INTERNAL_SERVER_ERROR.as_u16()
            };
            ("500", stream_status, code, Some(e.to_string()))
        }
    }
}

/// Finalize a log record (flatten, resolve timestamp, add `_original` /
/// `_o2_id` if configured) and push it into `json_data_by_stream`.
///
/// Returns `true` on success, `false` when the record should be skipped
/// (the caller should `continue`).
fn finalize_and_buffer_record(
    item: json::Value,
    original_data: Option<String>,
    ctx: &mut FinalizeRecordContext<'_>,
) -> bool {
    let mut res = match flatten::flatten_with_level(item, ctx.flatten_level) {
        Ok(r) => r,
        Err(e) => {
            ctx.stream_status.status.failed += 1;
            ctx.stream_status.status.error = e.to_string();
            pre_write_bulk_item(
                ctx.pre_write_bulk,
                ctx.stream_name,
                None,
                super::bulk::TRANSFORM_FAILED,
                &e.to_string(),
            );
            log::error!("Record flattening error: {e}");
            return false;
        }
    };
    let timestamp = match handle_timestamp(&mut res, ctx.min_ts, ctx.max_ts) {
        Ok(ts) => ts,
        Err(e) => {
            ctx.stream_status.status.failed += 1;
            ctx.stream_status.status.error = e.to_string();
            // the flattened record is still here: keep its doc id on the item
            let doc_id = res
                .as_object()
                .and_then(|record| super::record_doc_id(record).ok().flatten());
            pre_write_bulk_item(
                ctx.pre_write_bulk,
                ctx.stream_name,
                doc_id,
                super::bulk::TS_PARSE_FAILED,
                &e.to_string(),
            );
            metrics::INGEST_ERRORS
                .with_label_values(&[
                    ctx.org_id,
                    StreamType::Logs.as_str(),
                    ctx.stream_name,
                    crate::service::logs::bulk::TS_PARSE_FAILED,
                ])
                .inc();
            log_failed_record(ctx.log_ingestion_errors, &res, &e.to_string());
            return false;
        }
    };
    let mut local_val = match res.take() {
        json::Value::Object(val) => val,
        _ => {
            ctx.stream_status.status.failed += 1;
            pre_write_bulk_item(
                ctx.pre_write_bulk,
                ctx.stream_name,
                None,
                super::bulk::DOC_NOT_AN_OBJECT,
                "the record is not a JSON object",
            );
            return false;
        }
    };
    if ctx
        .streams_need_original_map
        .get(ctx.stream_name)
        .is_some_and(|v| *v)
        && let Some(ref od) = original_data
    {
        local_val.insert(ORIGINAL_DATA_COL_NAME.to_string(), od.clone().into());
        let record_id = crate::service::ingestion::generate_record_id(
            ctx.org_id,
            ctx.stream_name,
            &StreamType::Logs,
        );
        local_val.insert(
            ID_COL_NAME.to_string(),
            json::Value::String(record_id.to_string()),
        );
    }
    match ctx.json_data_by_stream.get_mut(ctx.stream_name) {
        Some((ts_data, fn_num)) => {
            ts_data.push((timestamp, local_val));
            *fn_num = ctx.need_usage_report.then_some(0);
        }
        None => {
            ctx.json_data_by_stream.insert(
                ctx.stream_name.to_string(),
                (
                    vec![(timestamp, local_val)],
                    ctx.need_usage_report.then_some(0),
                ),
            );
        }
    };
    true
}

pub fn handle_timestamp(
    value: &mut json::Value,
    min_ts: i64,
    max_ts: i64,
) -> Result<i64, anyhow::Error> {
    let local_val = value
        .as_object_mut()
        .ok_or_else(|| anyhow::Error::msg("Value is not an object"))?;
    let (timestamp, has_valid_timestamp) = match local_val.get(TIMESTAMP_COL_NAME) {
        Some(v) => {
            if !v.is_null() {
                match parse_timestamp_micro_from_value(v) {
                    Ok(t) => t,
                    Err(_) => return Err(anyhow::Error::msg("Can't parse timestamp")),
                }
            } else {
                (Utc::now().timestamp_micros(), false)
            }
        }
        None => (Utc::now().timestamp_micros(), false),
    };
    // check ingestion time
    if timestamp < min_ts {
        return Err(get_upto_discard_error());
    }
    if timestamp > max_ts {
        return Err(get_future_discard_error());
    }
    if !has_valid_timestamp {
        local_val.insert(
            TIMESTAMP_COL_NAME.to_string(),
            json::Value::Number(timestamp.into()),
        );
    }
    Ok(timestamp)
}

impl Iterator for IngestionDataIter {
    type Item = Result<json::Value, IngestionError>;

    fn next(&mut self) -> Option<Result<json::Value, IngestionError>> {
        match self {
            IngestionDataIter::JSONIter(iter) => iter.next().map(Ok),
            IngestionDataIter::MultiIter(iter) => loop {
                match iter.next() {
                    Some(Ok(line)) if line.trim().is_empty() => {
                        // If the line is empty, just continue to the next iteration.
                        continue;
                    }
                    Some(Ok(line)) => {
                        // If the line is not empty, attempt to parse it as JSON.
                        return Some(json::from_str(&line).map_err(IngestionError::from));
                    }
                    Some(Err(e)) => {
                        // If there's an error reading the line, return it.
                        return Some(Err(IngestionError::from(e)));
                    }
                    None => {
                        // If there are no more lines, return None.
                        return None;
                    }
                }
            },
            IngestionDataIter::GCP(iter, err) => match err {
                Some(e) => Some(Err(IngestionError::GCPError(e.clone()))),
                None => iter.next().map(Ok),
            },
            IngestionDataIter::KinesisFH(iter, err) => match err {
                Some(e) => Some(Err(IngestionError::AWSError(e.clone()))),
                None => iter.next().map(Ok),
            },
        }
    }
}

impl IngestionData {
    pub fn iter(self) -> IngestionDataIter {
        match self {
            IngestionData::JSON(vec) => IngestionDataIter::JSONIter(vec.into_iter()),
            IngestionData::Multi(data) => {
                let cursor = Cursor::new(data);
                IngestionDataIter::MultiIter(std::io::BufReader::new(cursor).lines())
            }
            IngestionData::GCP(request) => {
                let data = &request.message.data;
                let request_id = &request.message.message_id;
                let req_timestamp = &request.message.publish_time;
                match decode_and_decompress_to_string(data) {
                    Ok(decompressed_data) => {
                        let value: json::Value = json::from_str(&decompressed_data).unwrap();
                        IngestionDataIter::GCP(vec![value].into_iter(), None)
                    }
                    Err(e) => IngestionDataIter::GCP(
                        vec![].into_iter(),
                        Some(GCPIngestionResponse {
                            request_id: request_id.to_string(),
                            error_message: Some(e.to_string()),
                            timestamp: req_timestamp.to_string(),
                        }),
                    ),
                }
            }
            IngestionData::KinesisFH(request) => {
                let mut events = Vec::with_capacity(request.records.len());
                let request_id = &request.request_id;
                let req_timestamp = request.timestamp.unwrap_or(Utc::now().timestamp_micros());

                for record in &request.records {
                    match decode_and_decompress_to_vec(&record.data) {
                        Err(err) => {
                            return IngestionDataIter::KinesisFH(
                                events.into_iter(),
                                Some(KinesisFHIngestionResponse {
                                    request_id: request_id.to_string(),
                                    error_message: Some(err.to_string()),
                                    timestamp: req_timestamp,
                                }),
                            );
                        }
                        Ok(decompressed_data) => {
                            match deserialize_aws_record_from_vec(decompressed_data, request_id) {
                                Ok(parsed_events) => events.extend(parsed_events),
                                Err(err) => {
                                    return IngestionDataIter::KinesisFH(
                                        events.into_iter(),
                                        Some(KinesisFHIngestionResponse {
                                            request_id: request_id.to_string(),
                                            error_message: Some(err.to_string()),
                                            timestamp: req_timestamp,
                                        }),
                                    );
                                }
                            }
                        }
                    }
                }
                IngestionDataIter::KinesisFH(events.into_iter(), None)
            }
        }
    }
}

// Protobufs are not valid UTF-8 strings, so we need to maintain them as byte arrays
pub fn decode_and_decompress_to_vec(
    encoded_data: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let decoded_data = config::utils::base64::decode_raw(encoded_data)?;
    let mut gz = GzDecoder::new(decoded_data.as_slice());
    let mut vec = Vec::new();
    match gz.read_to_end(&mut vec) {
        Ok(_) => Ok(vec),
        Err(_) => Ok(decoded_data),
    }
}

// Use this function when we know the data is JSON since it will be valid UTF-8
pub fn decode_and_decompress_to_string(
    encoded_data: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let decoded_data = config::utils::base64::decode_raw(encoded_data)?;
    let mut gz = GzDecoder::new(decoded_data.as_slice());
    let mut decompressed_data = String::new();
    match gz.read_to_string(&mut decompressed_data) {
        Ok(_) => Ok(decompressed_data),
        Err(_) => Ok(String::from_utf8(decoded_data)?),
    }
}

/// Calculate size of VarInt header from byte array
///
/// See https://protobuf.dev/programming-guides/encoding/#varints for more info
pub fn get_size_of_var_int_header(bytes: &[u8]) -> Option<usize> {
    for (i, &b) in bytes.iter().enumerate() {
        // if most significant bit is 0
        if b & 0x80 == 0 {
            return Some(i + 1);
        }
    }

    None
}

fn deserialize_aws_record_from_vec(data: Vec<u8>, request_id: &str) -> Result<Vec<json::Value>> {
    // If it's a protobuf, process it as an OpenTelemetry 1.0 metric
    if let Some(header) = get_size_of_var_int_header(&data)
        && let Ok(a) = ExportMetricsServiceRequest::decode(&mut Cursor::new(&data[header..]))
    {
        return construct_values_from_open_telemetry_v1_metric(a);
    }

    let mut events = vec![];
    let mut value;
    let data = String::from_utf8(data)?;

    // It's likely newline-delimited JSON objects
    for line in data.lines() {
        match json::from_str(line) {
            Ok(AWSRecordType::KinesisFHLogs(kfh_log_data)) => {
                for event in kfh_log_data.log_events.iter() {
                    value = json::to_value(event)?;
                    let local_val = value
                        .as_object_mut()
                        .ok_or(anyhow::anyhow!("Error to convert Value to object"))?;

                    local_val.insert("requestId".to_owned(), request_id.into());
                    local_val.insert(
                        "messageType".to_owned(),
                        kfh_log_data.message_type.clone().into(),
                    );
                    local_val.insert("owner".to_owned(), kfh_log_data.owner.clone().into());
                    local_val.insert("logGroup".to_owned(), kfh_log_data.log_group.clone().into());
                    local_val.insert(
                        "logStream".to_owned(),
                        kfh_log_data.log_stream.clone().into(),
                    );
                    local_val.insert(
                        "subscriptionFilters".to_owned(),
                        kfh_log_data.subscription_filters.clone().into(),
                    );

                    let local_msg = event.message.as_str().unwrap();

                    if local_msg.starts_with('{') && local_msg.ends_with('}') {
                        let result: Result<json::Value, json::Error> = json::from_str(local_msg);

                        match result {
                            Err(_e) => {
                                local_val.insert("message".to_owned(), event.message.clone());
                            }
                            Ok(message_val) => {
                                local_val.insert("message".to_owned(), message_val.clone());
                            }
                        }
                    } else {
                        local_val.insert("message".to_owned(), local_msg.into());
                    }

                    local_val.insert(TIMESTAMP_COL_NAME.to_string(), event.timestamp.into());

                    value = local_val.clone().into();
                    events.push(value);
                }
            }
            Ok(AWSRecordType::KinesisFHMetrics(kfh_metric_data)) => {
                // Parse "dimensions" and "values" fields from KinesisFHMetricData
                let values = json::to_value(kfh_metric_data.value.clone())?;
                let dimensions = kfh_metric_data.dimensions.clone();
                let timestamp = kfh_metric_data.timestamp;

                let mut parsed_metric_value = json::to_value(kfh_metric_data)?;
                let local_parsed_metric_value = parsed_metric_value.as_object_mut().ok_or(
                    anyhow::anyhow!("CloudWatch metrics failed to parse Metric Object"),
                )?;

                for (value_name, value_val) in values.as_object().ok_or(anyhow::anyhow!(
                    "CloudWatch metrics failed to Metric Value Object"
                ))? {
                    local_parsed_metric_value.insert(value_name.to_owned(), value_val.to_owned());
                }
                local_parsed_metric_value.remove("value");

                let metric_dimensions = dimensions
                    .as_object()
                    .ok_or(anyhow::anyhow!(
                        "CloudWatch metrics dimensions parsing failed"
                    ))?
                    .iter()
                    .map(|(k, v)| format!("{k}=[{v}]"))
                    .collect::<Vec<_>>()
                    .join(", ");

                local_parsed_metric_value
                    .insert("metric_dimensions".to_owned(), metric_dimensions.into());
                local_parsed_metric_value.remove("dimensions");

                local_parsed_metric_value.insert(TIMESTAMP_COL_NAME.to_string(), timestamp.into());
                local_parsed_metric_value.remove("timestamp");

                value = local_parsed_metric_value.clone().into();
                events.push(value);
            }
            _ => {
                value = json::from_str(line)?;
                events.push(value);
            }
        }
    }
    Ok(events)
}

/// Extract a resource ID from an Amazon Resource Number string
///
/// See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html for more information
/// on ARNs
fn extract_resource_id_from_amazon_resource_number(arn: &str) -> &str {
    // skip the "arn" through the "account-id"
    let mut iter = arn.split(':').skip(5);
    // store directly into static array to avoid allocating Vec since we know what we want
    let split = [iter.next(), iter.next()];

    // If ARN looks like "arn:partition:service:region:account-id:resource-type:resource-id"
    if let Some(resource_id) = split[1] {
        return resource_id;
    }

    // If ARN looks like "arn:partition:service:region:account-id:resource-type/resource-id"
    if let Some((_, resource_id)) = split[0].unwrap().split_once('/') {
        return resource_id;
    }

    // ARN looks like "arn:partition:service:region:account-id:resource-id"
    split[0].unwrap()
}

/// Get the StringValue pair from the nested open telemetry KeyValue struct, else return None if it
/// isn't a StringValue
fn get_tuple_from_open_telemetry_key_value(kv: KeyValue) -> Option<(String, String)> {
    if let Some(AnyValue {
        value: Some(Value::StringValue(s)),
    }) = kv.value
    {
        Some((kv.key, s))
    } else {
        None
    }
}

/// Convert an OpenTelemetry v1.0 formatted request into a vector of json values.
///
/// The values are formatted to look the same as the ones extracted from AWS JSON telemetry format
fn construct_values_from_open_telemetry_v1_metric(
    data: ExportMetricsServiceRequest,
) -> Result<Vec<json::Value>> {
    let mut events = Vec::new();

    for resource_metric in data.resource_metrics {
        if resource_metric.resource.is_none() {
            continue;
        }

        // Collect all resource key value attributes e.g. cloud account ID and region
        let resource_attributes: HashMap<_, _> = resource_metric
            .resource
            .unwrap()
            .attributes
            .into_iter()
            .filter_map(get_tuple_from_open_telemetry_key_value)
            .collect();

        for sm in resource_metric.scope_metrics {
            for m in sm.metrics {
                let summary = match m.data {
                    Some(Data::Summary(summary)) => summary,
                    _ => continue, // AWS docs state that type should always be Summary
                };

                for i_sum in summary.data_points {
                    let dimensions = i_sum
                        .attributes
                        .iter()
                        .find(|kv| kv.key == "Dimensions")
                        .cloned();

                    let summary_attributes: HashMap<_, _> = i_sum
                        .attributes
                        .into_iter()
                        .filter_map(get_tuple_from_open_telemetry_key_value)
                        .collect();

                    let resource_id = extract_resource_id_from_amazon_resource_number(
                        resource_attributes.get("aws.exporter.arn").unwrap(),
                    );

                    let mut mv = json!({
                        "metric_stream_name": resource_id,
                        "account_id": resource_attributes.get("cloud.account.id").unwrap(),
                        "region": resource_attributes.get("cloud.region").unwrap(),
                        "namespace": summary_attributes.get("Namespace").unwrap(),
                        "metric_name": summary_attributes.get("MetricName").unwrap(),
                        TIMESTAMP_COL_NAME: std::time::Duration::from_nanos(i_sum.time_unix_nano).as_millis(),
                        "unit": m.unit,
                        "count": i_sum.count,
                        "sum": i_sum.sum,
                    });
                    let metric_value = mv.as_object_mut().unwrap();

                    if let Some(dimensions) = dimensions {
                        let string = match dimensions.value {
                            Some(AnyValue {
                                value: Some(Value::KvlistValue(kv_list)),
                            }) => kv_list.values,
                            _ => Vec::new(),
                        }
                        .into_iter()
                        .filter_map(get_tuple_from_open_telemetry_key_value)
                        .map(|(k, v)| format!("{k}=[\"{v}\"]"))
                        .collect::<Vec<_>>()
                        .join(", ");
                        metric_value.insert("metric_dimensions".to_string(), string.into());
                    }

                    for q in i_sum.quantile_values {
                        match q.quantile {
                            // Min and max values are the observed values for 0.0 and 1.0 quantiles
                            0.0 => metric_value.insert("min".to_string(), q.value.into()),
                            1.0 => metric_value.insert("max".to_string(), q.value.into()),
                            // Insert the rest of the quantiles in a format similar to p99.9
                            _ => metric_value
                                .insert(format!("p{:.1}", q.quantile * 100.0), q.value.into()),
                        };
                    }

                    events.push(mv);
                }
            }
        }
    }

    Ok(events)
}

#[cfg(test)]
mod tests {
    use config::TIMESTAMP_COL_NAME;
    use serde_json::json;

    use super::{
        build_ingestion_response, decode_and_decompress_to_string, decode_and_decompress_to_vec,
        deserialize_aws_record_from_vec, empty_buffer_response,
        extract_resource_id_from_amazon_resource_number, get_size_of_var_int_header,
        get_tuple_from_open_telemetry_key_value, handle_timestamp, pre_write_bulk_item,
    };
    use crate::{
        common::meta::ingestion::{BulkResponse, StreamStatus},
        service::logs::StreamWriteFailure,
    };

    fn stream_status(name: &str, successful: u32, failed: u32) -> StreamStatus {
        let mut status = StreamStatus::new(name);
        status.status.successful = successful;
        status.status.failed = failed;
        status
    }

    /// A durable write acks with 200 and the successful count.
    #[test]
    fn test_ingestion_response_success() {
        let (metric, body, code, error) =
            build_ingestion_response(stream_status("logs", 3, 0), Ok(vec![]));
        assert_eq!(metric, "200");
        assert_eq!(code, 200);
        assert!(error.is_none());
        assert_eq!(body.status.successful, 3);
        assert_eq!(body.status.failed, 0);
    }

    /// A failed write is reported as a non-2xx AND as failed records: the
    /// shipper must not delete its copy. Records counted successful before
    /// the write is what made this a 200 with `successful: N`.
    #[test]
    fn test_ingestion_response_write_failure_is_not_2xx() {
        let failures = vec![StreamWriteFailure {
            stream_name: "logs".to_string(),
            records: 3,
            error: infra::errors::Error::IngestionError("wal write failed".to_string()),
            permanent_rejection: false,
        }];
        // write_logs_by_stream has moved the 3 records into `failed`
        let (metric, body, code, error) =
            build_ingestion_response(stream_status("logs", 0, 3), Ok(failures));

        assert_eq!(metric, "500");
        assert_eq!(code, 500);
        assert!(!(200..300).contains(&code));
        assert_eq!(body.status.successful, 0);
        assert_eq!(body.status.failed, 3);
        let error = error.expect("a write failure must report an error");
        assert!(error.contains("logs"), "{error}");
        assert!(error.contains("wal write failed"), "{error}");
    }

    /// Backpressure is retryable: 503, so the client backs off and resends.
    #[test]
    fn test_ingestion_response_backpressure_is_503() {
        let failures = vec![StreamWriteFailure {
            stream_name: "logs".to_string(),
            records: 5,
            error: infra::errors::Error::ResourceError("memtable is full".to_string()),
            permanent_rejection: false,
        }];
        let (_, _, code, _) = build_ingestion_response(stream_status("logs", 0, 5), Ok(failures));
        assert_eq!(code, 503);

        let (_, _, code, error) = build_ingestion_response(
            stream_status("logs", 0, 5),
            Err(infra::errors::Error::ResourceError(
                "memtable is full".to_string(),
            )),
        );
        assert_eq!(code, 503);
        assert!(error.is_some());
    }

    /// Multi-stream requests report every failing stream, not just the first.
    #[test]
    fn test_ingestion_response_reports_every_failed_stream() {
        let failures = vec![
            StreamWriteFailure {
                stream_name: "logs_a".to_string(),
                records: 2,
                error: infra::errors::Error::IngestionError("disk full".to_string()),
                permanent_rejection: false,
            },
            StreamWriteFailure {
                stream_name: "logs_b".to_string(),
                records: 4,
                error: infra::errors::Error::IngestionError("disk full".to_string()),
                permanent_rejection: false,
            },
        ];
        let (_, body, code, error) =
            build_ingestion_response(stream_status("logs_a", 0, 6), Ok(failures));
        assert_eq!(code, 500);
        assert_eq!(body.status.failed, 6);
        let error = error.unwrap();
        assert!(
            error.contains("logs_a") && error.contains("logs_b"),
            "{error}"
        );
    }

    /// A pure PERMANENT rejection (the stream is being deleted) keeps the
    /// request 200 — retrying cannot help — but the body must say what was
    /// dropped and why, never `error: null`.
    #[test]
    fn test_ingestion_response_permanent_rejection_is_200_with_report() {
        let failures = vec![StreamWriteFailure {
            stream_name: "logs".to_string(),
            records: 3,
            error: infra::errors::Error::IngestionError(
                "stream [logs] is being deleted".to_string(),
            ),
            permanent_rejection: true,
        }];
        let (metric, body, code, error) =
            build_ingestion_response(stream_status("logs", 0, 3), Ok(failures));
        assert_eq!(metric, "200");
        assert_eq!(code, 200);
        assert_eq!(body.status.failed, 3);
        let error = error.expect("a dropped stream must be reported");
        assert!(error.contains("being deleted"), "{error}");
    }

    /// The empty-buffer fast return must not turn an all-failed request into
    /// a silent 200/error:null: the accumulated failure state is reported.
    #[test]
    fn test_empty_buffer_response_reports_accumulated_failures() {
        // every record failed before buffering
        let mut status = stream_status("logs", 0, 4);
        status.status.error = "Too old data, only last 5 hours data can be ingested".to_string();
        let res = empty_buffer_response(status, None);
        assert_eq!(res.code, 200, "data rejections are permanent: stay 200");
        let error = res.error.expect("an all-failed request must report why");
        assert!(error.contains("Too old data"), "{error}");
        assert_eq!(res.status[0].status.failed, 4);

        // a genuinely empty request stays a clean 200
        let res = empty_buffer_response(stream_status("logs", 0, 0), None);
        assert_eq!(res.code, 200);
        assert!(res.error.is_none());
    }

    /// The bulk shape reports pre-write rejections as ITEMS: an all-rejected
    /// bulk request used to answer 200/errors:false with an EMPTY items
    /// array, so positional clients never saw the loss.
    #[test]
    fn test_empty_buffer_response_carries_bulk_items() {
        let mut status = stream_status("logs", 0, 2);
        status.status.error = "Too old data".to_string();

        let mut pre_write_bulk = Some(BulkResponse {
            took: 0,
            errors: false,
            items: vec![],
        });
        pre_write_bulk_item(
            &mut pre_write_bulk,
            "logs",
            Some("doc1".to_string()),
            crate::service::logs::bulk::TS_PARSE_FAILED,
            "Too old data",
        );
        pre_write_bulk_item(
            &mut pre_write_bulk,
            "logs",
            None,
            crate::service::logs::bulk::TRANSFORM_FAILED,
            "flatten failed",
        );
        assert!(pre_write_bulk.as_ref().unwrap().errors);

        let res = empty_buffer_response(status, pre_write_bulk);
        assert_eq!(res.code, 200);
        assert_eq!(
            res.status[0].items.len(),
            2,
            "one item per rejected action, none swallowed"
        );
        let first = res.status[0].items[0].values().next().unwrap();
        assert_eq!(first._id, "doc1");
        let err = first.error.as_ref().expect("the rejection is on the item");
        assert_eq!(err.err_type, crate::service::logs::bulk::TS_PARSE_FAILED);
    }

    #[test]
    fn test_handle_timestamp_valid_in_range() {
        // 2024-01-15 in microseconds
        let ts = 1_705_276_800_000_000i64;
        let mut val = json!({TIMESTAMP_COL_NAME: ts});
        let result = handle_timestamp(&mut val, 0, i64::MAX);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ts);
    }

    #[test]
    fn test_handle_timestamp_too_old() {
        let min_ts = 1_705_276_800_000_000i64;
        let old_ts = 1_000_000_000_000_000i64;
        let mut val = json!({TIMESTAMP_COL_NAME: old_ts});
        let result = handle_timestamp(&mut val, min_ts, i64::MAX);
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_timestamp_too_future() {
        let max_ts = 1_705_276_800_000_000i64;
        let future_ts = 2_000_000_000_000_000i64;
        let mut val = json!({TIMESTAMP_COL_NAME: future_ts});
        let result = handle_timestamp(&mut val, 0, max_ts);
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_timestamp_not_object() {
        let mut val = json!("not an object");
        let result = handle_timestamp(&mut val, 0, i64::MAX);
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_timestamp_null_timestamp_uses_now() {
        let mut val = json!({TIMESTAMP_COL_NAME: null});
        let before = chrono::Utc::now().timestamp_micros();
        let result = handle_timestamp(&mut val, 0, i64::MAX);
        let after = chrono::Utc::now().timestamp_micros();
        assert!(result.is_ok());
        let ts = result.unwrap();
        assert!(ts >= before && ts <= after);
    }

    #[test]
    fn test_handle_timestamp_missing_field_uses_now() {
        let mut val = json!({"message": "hello"});
        let before = chrono::Utc::now().timestamp_micros();
        let result = handle_timestamp(&mut val, 0, i64::MAX);
        let after = chrono::Utc::now().timestamp_micros();
        assert!(result.is_ok());
        let ts = result.unwrap();
        assert!(ts >= before && ts <= after);
        // field should be inserted
        assert!(val.get(TIMESTAMP_COL_NAME).is_some());
    }

    #[test]
    fn test_get_tuple_from_open_telemetry_key_value_string_value() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
        let kv = KeyValue {
            key: "my_key".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("my_val".to_string())),
            }),
            ..Default::default()
        };
        let result = get_tuple_from_open_telemetry_key_value(kv);
        assert_eq!(result, Some(("my_key".to_string(), "my_val".to_string())));
    }

    #[test]
    fn test_get_tuple_from_open_telemetry_key_value_non_string() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
        let kv = KeyValue {
            key: "my_key".to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(42)),
            }),
            ..Default::default()
        };
        let result = get_tuple_from_open_telemetry_key_value(kv);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_tuple_from_open_telemetry_key_value_no_value() {
        use opentelemetry_proto::tonic::common::v1::KeyValue;
        let kv = KeyValue {
            key: "my_key".to_string(),
            value: None,
            ..Default::default()
        };
        let result = get_tuple_from_open_telemetry_key_value(kv);
        assert_eq!(result, None);
    }

    #[test]
    fn test_decode_and_decompress_success_string() {
        let encoded_data = "H4sIAAAAAAAAADWO0QqCMBiFX2XsOkKJZHkXot5YQgpdhMTSPzfSTbaZhPjuzbTLj3M45xtxC1rTGvJPB9jHQXrOL2lyP4VZdoxDvMFyEKDmpJF9NVBTskTW2gaNrGMl+85mC2VGAW0X1P1Dl4p3hksR8caA0ti/Fb9e+AZhZhwxr5a64VbD0NaOuR5xPLJzycEh+81fbxa4JmjVQ6uejwIG5YuLGjGgjWFIPlFll7ig8zOKuAImNWzxVExfL8ipzewAAAA=";
        let expected = "{\"messageType\":\"CONTROL_MESSAGE\",\"owner\":\"CloudwatchLogs\",\"logGroup\":\"\",\"logStream\":\"\",\"subscriptionFilters\":[],\"logEvents\":[{\"id\":\"\",\"timestamp\":1680683189085,\"message\":\"CWL CONTROL MESSAGE: Checking health of destination Firehose.\"}]}";
        let result = decode_and_decompress_to_string(encoded_data)
            .expect("Failed to decode and decompress data");
        assert_eq!(result, expected);
    }

    #[test]
    fn test_decode_and_decompress_success_vec() {
        let encoded_data = "H4sIAAAAAAAAADWO0QqCMBiFX2XsOkKJZHkXot5YQgpdhMTSPzfSTbaZhPjuzbTLj3M45xtxC1rTGvJPB9jHQXrOL2lyP4VZdoxDvMFyEKDmpJF9NVBTskTW2gaNrGMl+85mC2VGAW0X1P1Dl4p3hksR8caA0ti/Fb9e+AZhZhwxr5a64VbD0NaOuR5xPLJzycEh+81fbxa4JmjVQ6uejwIG5YuLGjGgjWFIPlFll7ig8zOKuAImNWzxVExfL8ipzewAAAA=";
        let expected = vec![
            123, 34, 109, 101, 115, 115, 97, 103, 101, 84, 121, 112, 101, 34, 58, 34, 67, 79, 78,
            84, 82, 79, 76, 95, 77, 69, 83, 83, 65, 71, 69, 34, 44, 34, 111, 119, 110, 101, 114,
            34, 58, 34, 67, 108, 111, 117, 100, 119, 97, 116, 99, 104, 76, 111, 103, 115, 34, 44,
            34, 108, 111, 103, 71, 114, 111, 117, 112, 34, 58, 34, 34, 44, 34, 108, 111, 103, 83,
            116, 114, 101, 97, 109, 34, 58, 34, 34, 44, 34, 115, 117, 98, 115, 99, 114, 105, 112,
            116, 105, 111, 110, 70, 105, 108, 116, 101, 114, 115, 34, 58, 91, 93, 44, 34, 108, 111,
            103, 69, 118, 101, 110, 116, 115, 34, 58, 91, 123, 34, 105, 100, 34, 58, 34, 34, 44,
            34, 116, 105, 109, 101, 115, 116, 97, 109, 112, 34, 58, 49, 54, 56, 48, 54, 56, 51, 49,
            56, 57, 48, 56, 53, 44, 34, 109, 101, 115, 115, 97, 103, 101, 34, 58, 34, 67, 87, 76,
            32, 67, 79, 78, 84, 82, 79, 76, 32, 77, 69, 83, 83, 65, 71, 69, 58, 32, 67, 104, 101,
            99, 107, 105, 110, 103, 32, 104, 101, 97, 108, 116, 104, 32, 111, 102, 32, 100, 101,
            115, 116, 105, 110, 97, 116, 105, 111, 110, 32, 70, 105, 114, 101, 104, 111, 115, 101,
            46, 34, 125, 93, 125,
        ];
        let result = decode_and_decompress_to_vec(encoded_data)
            .expect("Failed to decode and decompress data");
        assert_eq!(result, expected);
    }

    #[test]
    fn test_decode_success_string() {
        let encoded_data = "eyJtZXNzYWdlIjoiMiAwNTg2OTQ4NTY0NzYgZW5pLTAzYzBmNWJhNzlhNjZlZjE3IDEwLjMuMTY2LjcxIDEwLjMuMTQxLjIwOSA0NDMgMzg2MzQgNiAxMDMgNDI5MjYgMTY4MDgzODU1NiAxNjgwODM4NTc4IEFDQ0VQVCBPSyJ9Cg==";
        let expected = "{\"message\":\"2 058694856476 eni-03c0f5ba79a66ef17 10.3.166.71 10.3.141.209 443 38634 6 103 42926 1680838556 1680838578 ACCEPT OK\"}\n";
        let result = decode_and_decompress_to_string(encoded_data).expect("Failed to decode data");
        assert_eq!(result, expected);
    }

    #[test]
    fn test_decode_success_vec() {
        let encoded_data = "eyJtZXNzYWdlIjoiMiAwNTg2OTQ4NTY0NzYgZW5pLTAzYzBmNWJhNzlhNjZlZjE3IDEwLjMuMTY2LjcxIDEwLjMuMTQxLjIwOSA0NDMgMzg2MzQgNiAxMDMgNDI5MjYgMTY4MDgzODU1NiAxNjgwODM4NTc4IEFDQ0VQVCBPSyJ9Cg==";
        let expected = vec![
            123, 34, 109, 101, 115, 115, 97, 103, 101, 34, 58, 34, 50, 32, 48, 53, 56, 54, 57, 52,
            56, 53, 54, 52, 55, 54, 32, 101, 110, 105, 45, 48, 51, 99, 48, 102, 53, 98, 97, 55, 57,
            97, 54, 54, 101, 102, 49, 55, 32, 49, 48, 46, 51, 46, 49, 54, 54, 46, 55, 49, 32, 49,
            48, 46, 51, 46, 49, 52, 49, 46, 50, 48, 57, 32, 52, 52, 51, 32, 51, 56, 54, 51, 52, 32,
            54, 32, 49, 48, 51, 32, 52, 50, 57, 50, 54, 32, 49, 54, 56, 48, 56, 51, 56, 53, 53, 54,
            32, 49, 54, 56, 48, 56, 51, 56, 53, 55, 56, 32, 65, 67, 67, 69, 80, 84, 32, 79, 75, 34,
            125, 10,
        ];
        let result = decode_and_decompress_to_vec(encoded_data).expect("Failed to decode data");
        assert_eq!(result, expected);
    }

    #[test]
    fn test_decode_and_decompress_invalid_base64_string() {
        let encoded_data = "H4sIAAAAAAAC/ytJLS4BAAxGw7gNAAA&"; // Invalid base64 string
        let result = decode_and_decompress_to_string(encoded_data);
        assert!(
            result.is_err(),
            "Expected an error due to invalid base64 input"
        );
    }

    #[test]
    fn test_decode_and_decompress_invalid_base64_vec() {
        let encoded_data = "H4sIAAAAAAAC/ytJLS4BAAxGw7gNAAA&"; // Invalid base64 string
        let result = decode_and_decompress_to_vec(encoded_data);
        assert!(
            result.is_err(),
            "Expected an error due to invalid base64 input"
        );
    }

    #[test]
    fn test_deserialize_from_str_metrics() {
        let encoded_data = "eyJtZXRyaWNfc3RyZWFtX25hbWUiOiJDdXN0b21QYXJ0aWFsLUJDbjVjQSIsImFjY291bnRfaWQiOiI3MzkxNDcyMjI5ODkiLCJyZWdpb24iOiJ1cy1lYXN0LTIiLCJuYW1lc3BhY2UiOiJBV1MvVXNhZ2UiLCJtZXRyaWNfbmFtZSI6IkNhbGxDb3VudCIsImRpbWVuc2lvbnMiOnsiQ2xhc3MiOiJOb25lIiwiUmVzb3VyY2UiOiJHZXRNZXRyaWNEYXRhIiwiU2VydmljZSI6IkNsb3VkV2F0Y2giLCJUeXBlIjoiQVBJIn0sInRpbWVzdGFtcCI6MTcxMzkwMjcwMDAwMCwidmFsdWUiOnsibWF4IjoxLjAsIm1pbiI6MS4wLCJzdW0iOjMuMCwiY291bnQiOjMuMH0sInVuaXQiOiJOb25lIn0KeyJtZXRyaWNfc3RyZWFtX25hbWUiOiJDdXN0b21QYXJ0aWFsLUJDbjVjQSIsImFjY291bnRfaWQiOiI3MzkxNDcyMjI5ODkiLCJyZWdpb24iOiJ1cy1lYXN0LTIiLCJuYW1lc3BhY2UiOiJBV1MvRmlyZWhvc2UiLCJtZXRyaWNfbmFtZSI6IktNU0tleUludmFsaWRTdGF0ZSIsImRpbWVuc2lvbnMiOnsiRGVsaXZlcnlTdHJlYW1OYW1lIjoiUFVULUhUUC1SZFFXOCJ9LCJ0aW1lc3RhbXAiOjE3MTM5MDI2NDAwMDAsInZhbHVlIjp7Im1heCI6MC4wLCJtaW4iOjAuMCwic3VtIjowLjAsImNvdW50Ijo2MC4wfSwidW5pdCI6IkNvdW50In0KeyJtZXRyaWNfc3RyZWFtX25hbWUiOiJDdXN0b21QYXJ0aWFsLUJDbjVjQSIsImFjY291bnRfaWQiOiI3MzkxNDcyMjI5ODkiLCJyZWdpb24iOiJ1cy1lYXN0LTIiLCJuYW1lc3BhY2UiOiJBV1MvRmlyZWhvc2UiLCJtZXRyaWNfbmFtZSI6IktNU0tleU5vdEZvdW5kIiwiZGltZW5zaW9ucyI6eyJEZWxpdmVyeVN0cmVhbU5hbWUiOiJQVVQtSFRQLVJkUVc4In0sInRpbWVzdGFtcCI6MTcxMzkwMjY0MDAwMCwidmFsdWUiOnsibWF4IjowLjAsIm1pbiI6MC4wLCJzdW0iOjAuMCwiY291bnQiOjYwLjB9LCJ1bml0IjoiQ291bnQifQo=";
        let decoded = decode_and_decompress_to_vec(encoded_data);
        assert!(decoded.is_ok());
        let decoded = decoded.unwrap();
        let request_id = "test_id".to_string();
        let result = deserialize_aws_record_from_vec(decoded, &request_id);
        assert!(result.is_ok());
        let value = result.unwrap();
        for val in value {
            assert_eq!(val.get("account_id").unwrap(), "739147222989");
        }
    }

    #[test]
    fn test_deserialize_from_str_logs() {
        let encoded_data = "eyJtZXNzYWdlVHlwZSI6IkRBVEFfTUVTU0FHRSIsIm93bmVyIjoiMTIzNDU2Nzg5MDEyIiwibG9nR3JvdXAiOiJsb2dfZ3JvdXBfbmFtZSIsImxvZ1N0cmVhbSI6ImxvZ19zdHJlYW1fbmFtZSIsInN1YnNjcmlwdGlvbkZpbHRlcnMiOlsic3Vic2NyaXB0aW9uX2ZpbHRlcl9uYW1lIl0sImxvZ0V2ZW50cyI6W3siaWQiOiIwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1IiwidGltZXN0YW1wIjoxNzEzOTgzNDQ2LCJtZXNzYWdlIjoibG9nbWVzc2FnZTEifSx7ImlkIjoiMDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NSIsInRpbWVzdGFtcCI6IDE3MTM5ODM0NDYsIm1lc3NhZ2UiOiJsb2dtZXNzYWdlMiJ9XX0=";
        let decoded = decode_and_decompress_to_vec(encoded_data);
        assert!(decoded.is_ok());
        let decoded = decoded.unwrap();
        let request_id = "test_id".to_string();
        let result = deserialize_aws_record_from_vec(decoded, &request_id);
        assert!(result.is_ok());
        let result = result.unwrap();
        for val in result {
            assert_eq!(val.get("owner").unwrap(), "123456789012");
        }
    }

    #[test]
    fn test_var_int_header_empty_array() {
        let bytes = [];
        assert_eq!(get_size_of_var_int_header(&bytes), None);
    }

    #[test]
    fn test_var_int_header_no_valid_bytes() {
        let bytes = [0xFF; 100];
        assert_eq!(get_size_of_var_int_header(&bytes), None);
    }

    #[test]
    fn test_var_int_header() {
        let bytes: Vec<_> = (0..=u8::MAX).rev().collect();
        assert_eq!(get_size_of_var_int_header(&bytes), Some(129));
    }

    #[test]
    fn extract_resource_id_with_colon() {
        let arn = "arn:partition:service:region:account-id:resource-type:resource-id";
        assert_eq!(
            extract_resource_id_from_amazon_resource_number(arn),
            "resource-id"
        );
    }

    #[test]
    fn extract_resource_id_with_slash() {
        let arn = "arn:partition:service:region:account-id:resource-type/resource-id";
        assert_eq!(
            extract_resource_id_from_amazon_resource_number(arn),
            "resource-id"
        );
    }

    #[test]
    fn extract_resource_id_without_resource_type() {
        let arn = "arn:partition:service:region:account-id:resource-id";
        assert_eq!(
            extract_resource_id_from_amazon_resource_number(arn),
            "resource-id"
        );
    }
}
