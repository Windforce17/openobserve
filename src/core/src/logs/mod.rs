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
    sync::Arc,
    time::Instant,
};

use arrow_schema::{DataType, Field};
use bulk::SCHEMA_CONFORMANCE_FAILED;
use config::{
    DISTINCT_FIELDS, META_ORG_ID, SIZE_IN_MB, TIMESTAMP_COL_NAME, get_config,
    meta::{
        alerts::alert::Alert,
        self_reporting::usage::{RequestStats, UsageType},
        stream::{StreamParams, StreamPartition, StreamType},
    },
    metrics,
    utils::{
        flatten,
        json::{Map, Value, estimate_json_bytes, get_string_value},
        schema_ext::SchemaExt,
        time::now_micros,
        util::DISTINCT_STREAM_PREFIX,
    },
};
use infra::{
    errors::{Error, Result},
    schema::{SchemaCache, get_partition_time_level},
};

#[cfg(feature = "cloud")]
use crate::service::stream::get_stream;
use crate::{
    common::meta::{ingestion::IngestionStatus, stream::SchemaRecords},
    service::{
        alerts::alert::AlertExt,
        db,
        ingestion::{TriggerAlertData, evaluate_trigger, get_write_partition_key, write_file},
        metadata::{MetadataItem, MetadataType, distinct_values::DvItem, write},
        schema::{
            CanonicalizationSummary, canonicalize_record_into, check_for_schema,
            stream_schema_exists,
        },
        self_reporting::report_request_usage_stats,
    },
};

pub mod bulk;
pub mod hec;
pub mod ingest;
pub mod loki;
pub mod otlp;

static BULK_OPERATORS: [&str; 3] = ["create", "index", "update"];

/// The user-supplied per-record document id (the bulk action metadata copies
/// its `_id` into the record).
pub(crate) const DOC_ID_COL_NAME: &str = "_id";

pub type O2IngestJsonData = (Vec<(i64, Map<String, Value>)>, Option<usize>);

/// A stream whose records never reached a durable write. Unless the loss is a
/// permanent rejection, the request that carried them MUST NOT be answered
/// with a 2xx: shippers (filebeat, vector, fluent-bit, the OTel collector,
/// Splunk forwarders) commit their read position on a 2xx and the records are
/// gone.
pub(crate) struct StreamWriteFailure {
    pub stream_name: String,
    pub records: u32,
    pub error: Error,
    /// a permanent rejection (e.g. the stream is being deleted): the drop is
    /// still reported on the response body, but retrying cannot help, so it
    /// does not drive a non-2xx on its own
    pub permanent_rejection: bool,
}

/// The HTTP status for a request whose stream writes failed: backpressure is
/// retryable and maps to 503 (every other ingest endpoint does the same);
/// when every failure is a permanent rejection (e.g. all target streams are
/// being deleted) the request stays 200 — retrying cannot help, the drop is
/// reported on the body — anything else is 500. Never a 2xx for a retryable
/// loss.
pub(crate) fn write_failure_status_code(failures: &[StreamWriteFailure]) -> u16 {
    if failures
        .iter()
        .any(|f| matches!(f.error, Error::ResourceError(_)))
    {
        503
    } else if !failures.is_empty() && failures.iter().all(|f| f.permanent_rejection) {
        200
    } else {
        500
    }
}

/// One message naming every stream that lost records, for the response
/// `error` field. Bounded by the number of streams in the request.
pub(crate) fn write_failure_message(failures: &[StreamWriteFailure]) -> String {
    failures
        .iter()
        .map(|f| {
            format!(
                "stream [{}]: {} record(s) not written: {}",
                f.stream_name, f.records, f.error
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// The `_id` of a record. It is user-supplied — a non-string form is a
/// per-record error, never a panic that unwinds the whole request.
pub(crate) fn record_doc_id(record: &Map<String, Value>) -> Result<Option<String>, String> {
    match record.get(DOC_ID_COL_NAME) {
        None => Ok(None),
        Some(Value::String(id)) => Ok(Some(id.to_owned())),
        Some(other) => Err(format!(
            "invalid {DOC_ID_COL_NAME}: expected a string, got {other}"
        )),
    }
}

/// Records buffered for one stream that are NOT durable yet. They are
/// accounted on the request status only once `write_file` has returned Ok:
/// counting them successful before the write is what let a failed write be
/// acked with `successful: N`.
enum PendingRecords {
    /// non-bulk statuses only carry counts
    Count(u32),
    /// bulk statuses report one item per record, in REQUEST order: a record
    /// that fails before the write claims its slot immediately, so positional
    /// clients (Filebeat/Logstash) never attribute an error to the wrong
    /// document
    Docs(Vec<BulkSlot>),
}

/// One bulk response item, held at its record's position in the request.
enum BulkSlot {
    /// buffered for the durable write; resolved by `commit` (success) or
    /// `fail` (write failure)
    Pending(Option<String>),
    /// failed before the write; emitted as-is, in place
    Failed {
        doc_id: Option<String>,
        original_record: Option<Value>,
        err_type: String,
        reason: String,
    },
}

impl PendingRecords {
    fn new(status: &IngestionStatus) -> Self {
        match status {
            IngestionStatus::Record(_) => Self::Count(0),
            IngestionStatus::Bulk(_) => Self::Docs(Vec::new()),
        }
    }

    /// Every record of `json_data`, for the failure paths that run before the
    /// per-record loop has accounted anything.
    fn all(status: &IngestionStatus, json_data: &[(i64, Map<String, Value>)]) -> Self {
        match status {
            IngestionStatus::Record(_) => Self::Count(json_data.len() as u32),
            IngestionStatus::Bulk(_) => Self::Docs(
                json_data
                    .iter()
                    .map(|(_, record)| BulkSlot::Pending(record_doc_id(record).ok().flatten()))
                    .collect(),
            ),
        }
    }

    fn push(&mut self, doc_id: Option<String>) {
        match self {
            Self::Count(count) => *count += 1,
            Self::Docs(slots) => slots.push(BulkSlot::Pending(doc_id)),
        }
    }

    /// Claim this record's slot with a failure that happened before the
    /// write, so the emitted items keep request order. Counting statuses
    /// account the failure directly on `status` instead — a count carries no
    /// order.
    fn push_failed(
        &mut self,
        doc_id: Option<String>,
        original_record: Option<Value>,
        err_type: &str,
        reason: &str,
    ) {
        if let Self::Docs(slots) = self {
            slots.push(BulkSlot::Failed {
                doc_id,
                original_record,
                err_type: err_type.to_string(),
                reason: reason.to_string(),
            });
        }
    }

    /// The records still waiting on the durable write. Slots that already
    /// failed are excluded: they were accounted when they failed.
    fn len(&self) -> u32 {
        match self {
            Self::Count(count) => *count,
            Self::Docs(slots) => slots
                .iter()
                .filter(|slot| matches!(slot, BulkSlot::Pending(_)))
                .count() as u32,
        }
    }

    fn into_slots(self) -> Vec<BulkSlot> {
        match self {
            Self::Count(count) => (0..count).map(|_| BulkSlot::Pending(None)).collect(),
            Self::Docs(slots) => slots,
        }
    }

    /// The write is durable: the pending records are successful. Records that
    /// failed before the write keep their failure, in place.
    fn commit(self, status: &mut IngestionStatus, stream_name: &str) {
        match status {
            IngestionStatus::Record(status) => status.successful += self.len(),
            IngestionStatus::Bulk(bulk_res) => {
                for slot in self.into_slots() {
                    match slot {
                        BulkSlot::Pending(doc_id) => bulk::add_record_status(
                            stream_name.to_string(),
                            doc_id,
                            "".to_string(),
                            None,
                            bulk_res,
                            None,
                            None,
                        ),
                        BulkSlot::Failed {
                            doc_id,
                            original_record,
                            err_type,
                            reason,
                        } => bulk::add_record_status(
                            stream_name.to_string(),
                            doc_id,
                            "".to_string(),
                            original_record,
                            bulk_res,
                            Some(err_type),
                            Some(reason),
                        ),
                    }
                }
            }
        }
    }

    /// The write never became durable: the pending records are failed with
    /// `err_type`/`error`, so whoever acks upstream resends them. Records
    /// that failed before the write keep their own failure, in place.
    fn fail(self, status: &mut IngestionStatus, stream_name: &str, error: &str, err_type: &str) {
        match status {
            IngestionStatus::Record(status) => {
                status.failed += self.len();
                status.error = error.to_string();
            }
            IngestionStatus::Bulk(bulk_res) => {
                bulk_res.errors = true;
                for slot in self.into_slots() {
                    match slot {
                        BulkSlot::Pending(doc_id) => bulk::add_record_status(
                            stream_name.to_string(),
                            doc_id,
                            "".to_string(),
                            None,
                            bulk_res,
                            Some(err_type.to_string()),
                            Some(error.to_string()),
                        ),
                        BulkSlot::Failed {
                            doc_id,
                            original_record,
                            err_type,
                            reason,
                        } => bulk::add_record_status(
                            stream_name.to_string(),
                            doc_id,
                            "".to_string(),
                            original_record,
                            bulk_res,
                            Some(err_type),
                            Some(reason),
                        ),
                    }
                }
            }
        }
    }
}

/// Report one record as failed — on the request status for counting shapes,
/// and at its own slot in `pending` for the bulk shape, so the emitted items
/// keep request order — and keep going: a bad record never fails its
/// neighbours.
#[allow(clippy::too_many_arguments)]
fn report_record_failure(
    status: &mut IngestionStatus,
    pending: &mut PendingRecords,
    org_id: &str,
    stream_name: &str,
    doc_id: Option<String>,
    record_val: &Map<String, Value>,
    error: &str,
    failure_type: &str,
    log_ingest_errors: bool,
) {
    metrics::INGEST_ERRORS
        .with_label_values(&[org_id, StreamType::Logs.as_str(), stream_name, failure_type])
        .inc();
    log_failed_record(log_ingest_errors, record_val, error);
    match status {
        IngestionStatus::Record(status) => {
            status.failed += 1;
            status.error = error.to_string();
        }
        IngestionStatus::Bulk(bulk_res) => {
            bulk_res.errors = true;
            pending.push_failed(
                doc_id,
                Some(Value::Object(record_val.clone())),
                failure_type,
                error,
            );
        }
    }
}

/// Rename a user field literally named `_source` to `_source_field`
/// (`vortex_index::SOURCE_RENAMED_COL_NAME`) — the logs counterpart of the
/// traces `attr_` reserved-name convention. `_source` is the reserved
/// serialized-record column of core `.vix` files: left alone, the move job
/// would exclude the field from `_source` synthesis and from the writer
/// schema, silently losing the value. Applied to every record in
/// `write_logs` BEFORE schema inference, so the stream schema only ever
/// learns the renamed field. Degenerate double naming (a record carrying
/// both `_source` and `_source_field`) keeps the `_source` value, which
/// overwrites the other — documented, pathological input.
fn rename_reserved_source_field(record: &mut Map<String, Value>) {
    if let Some(value) = record.remove(vortex_index::SOURCE_COL_NAME) {
        record.insert(vortex_index::SOURCE_RENAMED_COL_NAME.to_string(), value);
    }
}

fn parse_bulk_index(v: &Value) -> Option<(&str, &str, Option<&str>)> {
    let local_val = v.as_object()?;
    for action in BULK_OPERATORS {
        if let Some(val) = local_val.get(action) {
            let Some(local_val) = val.as_object() else {
                log::warn!("Invalid bulk index action: {action}");
                continue;
            };
            let Some(index) = local_val.get("_index").and_then(|v| v.as_str()) else {
                continue;
            };
            let doc_id = local_val.get("_id").and_then(|v| v.as_str());
            return Some((action, index, doc_id));
        };
    }
    None
}

pub fn cast_to_type(
    value: &mut Map<String, Value>,
    delta: Vec<Field>,
) -> Result<(), anyhow::Error> {
    let mut parse_error = String::new();
    for field in delta {
        let field_name = field.name().clone();
        let Some(val) = value.get(&field_name) else {
            continue;
        };
        if val.is_null() {
            value.insert(field_name, Value::Null);
            continue;
        }
        match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                if val.is_string() {
                    continue;
                }
                value.insert(field_name, Value::String(get_string_value(val)));
            }
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                let ret = match val {
                    Value::Number(_) => {
                        continue;
                    }
                    Value::String(v) => v.parse::<i64>().map_err(|e| e.to_string()),
                    Value::Bool(v) => Ok(if *v { 1 } else { 0 }),
                    _ => Err("".to_string()),
                };
                match ret {
                    Ok(val) => {
                        value.insert(field_name, Value::Number(val.into()));
                    }
                    Err(_) => set_parsing_error(&mut parse_error, &field),
                };
            }
            DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => {
                let ret = match val {
                    Value::Number(_) => {
                        continue;
                    }
                    Value::String(v) => v.parse::<u64>().map_err(|e| e.to_string()),
                    Value::Bool(v) => Ok(if *v { 1 } else { 0 }),
                    _ => Err("".to_string()),
                };
                match ret {
                    Ok(val) => {
                        value.insert(field_name, Value::Number(val.into()));
                    }
                    Err(_) => set_parsing_error(&mut parse_error, &field),
                };
            }
            DataType::Float64 | DataType::Float32 | DataType::Float16 => {
                let ret = match val {
                    Value::Number(_) => {
                        continue;
                    }
                    Value::String(v) => v.parse::<f64>().map_err(|e| e.to_string()),
                    Value::Bool(v) => Ok(if *v { 1.0 } else { 0.0 }),
                    _ => Err("".to_string()),
                };
                match ret {
                    Ok(val) => {
                        value.insert(
                            field_name,
                            Value::Number(serde_json::Number::from_f64(val).unwrap()),
                        );
                    }
                    Err(_) => set_parsing_error(&mut parse_error, &field),
                };
            }
            DataType::Boolean => {
                let ret = match val {
                    Value::Bool(_) => {
                        continue;
                    }
                    Value::Number(v) => Ok(v.as_f64().unwrap_or(0.0) > 0.0),
                    Value::String(v) => v.parse::<bool>().map_err(|e| e.to_string()),
                    _ => Err("".to_string()),
                };
                match ret {
                    Ok(val) => {
                        value.insert(field_name, Value::Bool(val));
                    }
                    Err(_) => set_parsing_error(&mut parse_error, &field),
                };
            }
            _ => set_parsing_error(&mut parse_error, &field),
        };
    }
    if !parse_error.is_empty() {
        Err(anyhow::Error::msg(parse_error))
    } else {
        Ok(())
    }
}

fn set_parsing_error(parse_error: &mut String, field: &Field) {
    parse_error.push_str(&format!(
        "Failed to cast {} to type {} ",
        field.name(),
        field.data_type()
    ));
}

/// Writes every stream of the request and returns the streams whose write
/// failed. A failing stream never short-circuits the remaining ones: each is
/// attempted, each failure is accounted on `status`, and the caller turns the
/// aggregate into a non-2xx response.
#[allow(clippy::too_many_arguments)]
async fn write_logs_by_stream(
    thread_id: usize,
    org_id: &str,
    user_email: &str,
    time_stats: (i64, &Instant), // started_at
    usage_type: UsageType,
    status: &mut IngestionStatus,
    json_data_by_stream: HashMap<String, O2IngestJsonData>,
    byte_size_by_stream: HashMap<String, usize>,
    derived_streams: HashSet<String>,
) -> Result<Vec<StreamWriteFailure>> {
    let mut write_failures = Vec::new();
    for (stream_name, (json_data, fn_num)) in json_data_by_stream {
        // check if we are allowed to ingest
        if db::compact::retention::is_deleting_stream(org_id, StreamType::Logs, &stream_name, None)
        {
            // Dropped on purpose, but never silently: the records are
            // reported failed AND the drop is surfaced as a failure so the
            // response `error` names it. It is a PERMANENT rejection —
            // retrying into a deleted stream cannot help, so it keeps the
            // request 2xx (see `write_failure_status_code`) while the body
            // says what was dropped and why.
            let error = format!("stream [{stream_name}] is being deleted");
            log::warn!("{error}");
            metrics::INGEST_ERRORS
                .with_label_values(&[
                    org_id,
                    StreamType::Logs.as_str(),
                    &stream_name,
                    bulk::STREAM_DELETING,
                ])
                .inc_by(json_data.len() as u64);
            let records = json_data.len() as u32;
            let pending = PendingRecords::all(status, &json_data);
            pending.fail(status, &stream_name, &error, bulk::STREAM_DELETING);
            write_failures.push(StreamWriteFailure {
                stream_name,
                records,
                error: Error::IngestionError(error),
                permanent_rejection: true,
            });
            continue; // skip
        }

        // for cloud, we want to sent event when user creates a new stream
        #[cfg(feature = "cloud")]
        if get_stream(org_id, &stream_name, StreamType::Logs)
            .await
            .is_none()
        {
            let org = match super::organization::get_org(org_id).await {
                None => {
                    let error = Error::Message(format!("org with id {org_id} not found in db"));
                    let records = json_data.len() as u32;
                    let pending = PendingRecords::all(status, &json_data);
                    pending.fail(status, &stream_name, &error.to_string(), bulk::WRITE_FAILED);
                    write_failures.push(StreamWriteFailure {
                        stream_name,
                        records,
                        error,
                        permanent_rejection: false,
                    });
                    continue; // attempt the remaining streams
                }
                Some(org) => org,
            };

            super::self_reporting::cloud_events::enqueue_cloud_event(
                super::self_reporting::cloud_events::CloudEvent {
                    org_id: org.identifier.clone(),
                    org_name: org.name.clone(),
                    org_type: org.org_type.clone(),
                    user: Some(user_email.to_string()),
                    event: super::self_reporting::cloud_events::EventType::StreamCreated,
                    subscription_type: None,
                    stream_name: Some(stream_name.clone()),
                },
            )
            .await;
        }

        // write json data by stream
        let records = json_data.len() as u32;
        let is_derived = derived_streams.contains(&stream_name);
        let mut req_stats = match write_logs(
            thread_id,
            org_id,
            &stream_name,
            status,
            json_data,
            is_derived,
        )
        .await
        {
            Ok(req_stats) => req_stats,
            Err(error) => {
                // `write_logs` has already accounted these records as
                // failed on `status`; the aggregate drives the status code.
                log::error!("[LOGS] write failed for stream {org_id}/{stream_name}: {error}");
                write_failures.push(StreamWriteFailure {
                    stream_name,
                    records,
                    error,
                    permanent_rejection: false,
                });
                continue; // attempt the remaining streams
            }
        };

        let time_took = time_stats.1.elapsed().as_secs_f64();
        req_stats.response_time = time_took;
        req_stats.user_email = if user_email.is_empty() {
            None
        } else {
            Some(user_email.to_string())
        };

        req_stats.dropped_records = match status {
            IngestionStatus::Record(s) => s.failed.into(),
            IngestionStatus::Bulk(s) => {
                if s.errors {
                    s.items
                        .iter()
                        .map(|i| {
                            i.values()
                                .map(|res| if res.error.is_some() { 1 } else { 0 })
                                .sum::<i64>()
                        })
                        .sum()
                } else {
                    0
                }
            }
        };

        if let Some(fns_length) = fn_num {
            // the issue here is req_stats.size calculates size after flattening and
            // adding _timestamp col etc ; which inflates the size compared to the actual
            // data sent by user. So when reporting we check if the calling function has provided us
            // an "actual" size of the input, and is so use that instead of the req_stats
            if let Some(size) = byte_size_by_stream.get(&stream_name) {
                // req_stats already divides the size in mb
                req_stats.size = *size as f64 / SIZE_IN_MB;
            }
            report_request_usage_stats(
                req_stats,
                org_id,
                &stream_name,
                StreamType::Logs,
                usage_type,
                fns_length as u16,
                time_stats.0,
            )
            .await;
        }
    }
    Ok(write_failures)
}

async fn write_logs(
    thread_id: usize,
    org_id: &str,
    stream_name: &str,
    status: &mut IngestionStatus,
    mut json_data: Vec<(i64, Map<String, Value>)>,
    is_derived: bool,
) -> Result<RequestStats> {
    if json_data.is_empty() {
        return Ok(RequestStats::default());
    }

    let cfg = get_config();
    let log_ingest_errors = ingestion_log_enabled().await;

    // Reserved-name guard + reserved-alias canonicalization: this is the
    // single funnel of every logs ingest path (json/multi/bulk/hec/loki/gcp/
    // kinesis/otlp, pipelines included), records arrive here already
    // flattened, and it runs before `check_for_schema`, so the schema never
    // learns the reserved `_source` name or a dotted trace-context alias
    // (`trace.id`/`span.id` — the canonical `trace_id`/`span_id` wins).
    //
    // `_timestamp` canonicalization (same funnel, same reason): the STORED
    // `_timestamp` field must be exactly the per-record partition timestamp
    // as an i64 — the value the hour key, the WAL meta and every downstream
    // range derive from. Paths that re-shape records after their timestamp
    // was fixed (e.g. an OTLP pipeline whose VRL re-types or drops
    // `_timestamp`) otherwise store a divergent form, and the WAL arrow
    // conversion coerces a non-integer form to a LITERAL 0 — the live
    // zero-min_ts files that wedged the compactor. Forcing the field here
    // makes partition key == stored value structurally.
    for (timestamp, record) in json_data.iter_mut() {
        rename_reserved_source_field(record);
        flatten::canonicalize_reserved_aliases(record);
        if record.get(TIMESTAMP_COL_NAME).and_then(Value::as_i64) != Some(*timestamp) {
            record.insert(
                TIMESTAMP_COL_NAME.to_string(),
                Value::Number((*timestamp).into()),
            );
        }
    }
    // get schema and stream settings
    let mut stream_schema_map: HashMap<String, SchemaCache> = HashMap::new();
    let stream_schema = stream_schema_exists(
        org_id,
        stream_name,
        StreamType::Logs,
        &mut stream_schema_map,
    )
    .await;

    let schema = match stream_schema_map.get(stream_name) {
        Some(schema) => schema.schema().clone(),
        None => {
            // nothing was written: account every record as failed before the
            // error propagates to the caller that acks
            let error = format!("Schema not found for stream: {stream_name}");
            let pending = PendingRecords::all(status, &json_data);
            pending.fail(status, stream_name, &error, bulk::WRITE_FAILED);
            return Err(Error::IngestionError(error));
        }
    };
    let stream_settings = infra::schema::unwrap_stream_settings(&schema).unwrap_or_default();

    let mut partition_keys: Vec<StreamPartition> = vec![];
    let partition_time_level = get_partition_time_level(StreamType::Logs);
    if stream_schema.has_partition_keys {
        partition_keys = stream_settings.partition_keys;
    }

    // Start get stream alerts
    let mut stream_alerts_map: HashMap<String, Vec<Alert>> = HashMap::new();
    crate::service::ingestion::get_stream_alerts(
        &[StreamParams {
            org_id: org_id.to_owned().into(),
            stream_name: stream_name.to_owned().into(),
            stream_type: StreamType::Logs,
        }],
        &mut stream_alerts_map,
    )
    .await;
    let cur_stream_alerts =
        stream_alerts_map.get(&format!("{}/{}/{}", org_id, StreamType::Logs, stream_name));
    let mut triggers: TriggerAlertData =
        Vec::with_capacity(cur_stream_alerts.map_or(0, |v| v.len()));
    let mut evaluated_alerts = HashSet::new();
    // End get stream alert

    // start check for schema
    let min_timestamp = json_data
        .iter()
        .map(|(ts, _)| *ts)
        .min()
        .unwrap_or_else(now_micros);
    let schema_check = check_for_schema(
        org_id,
        stream_name,
        StreamType::Logs,
        &mut stream_schema_map,
        json_data.iter().map(|(_, v)| v).collect(),
        min_timestamp,
        is_derived, // is_derived is true if the stream is derived
    )
    .await;
    let (schema_evolution, infer_schema) = match schema_check {
        Ok(res) => res,
        Err(e) => {
            // nothing was written: account every record as failed before the
            // error propagates to the caller that acks
            let pending = PendingRecords::all(status, &json_data);
            pending.fail(status, stream_name, &e.to_string(), bulk::WRITE_FAILED);
            return Err(e.into());
        }
    };

    // get schema
    let latest_schema = stream_schema_map
        .get(stream_name)
        .unwrap()
        .schema()
        .as_ref()
        .clone()
        .with_metadata(HashMap::new());
    let schema_key = latest_schema.hash_key();
    let canonical_schema = stream_schema_map.get(stream_name).unwrap();
    // use latest schema as schema key
    // use inferred schema as record schema
    let rec_schema = match infer_schema {
        // use latest_schema's datetype for record schema
        Some(schema) => Arc::new(schema.cloned_from(&latest_schema)),
        None => Arc::new(latest_schema),
    };

    let mut distinct_values = Vec::with_capacity(16);

    let mut write_buf: HashMap<String, SchemaRecords> = HashMap::new();
    // buffered but not durable: accounted on `status` only after `write_file`
    let mut pending = PendingRecords::new(status);
    let mut canonical_summary = CanonicalizationSummary::default();

    for (timestamp, mut record_val) in json_data {
        if cfg.common.ingest_canonical_schema {
            canonicalize_record_into(
                StreamType::Logs,
                canonical_schema,
                &mut record_val,
                &mut canonical_summary,
            );
        }
        let doc_id = match record_doc_id(&record_val) {
            Ok(doc_id) => doc_id,
            Err(e) => {
                report_record_failure(
                    status,
                    &mut pending,
                    org_id,
                    stream_name,
                    None,
                    &record_val,
                    &e,
                    bulk::DOC_ID_INVALID,
                    log_ingest_errors,
                );
                continue;
            }
        };

        // validate record
        if !cfg.common.ingest_canonical_schema
            && let Some(delta) = schema_evolution.types_delta.as_ref()
        {
            let ret_val = if !schema_evolution.is_schema_changed {
                cast_to_type(&mut record_val, delta.to_owned())
            } else {
                let local_delta = delta
                    .iter()
                    .filter_map(|x| {
                        if x.metadata().contains_key("zo_cast") {
                            Some(x.to_owned())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                if !local_delta.is_empty() {
                    cast_to_type(&mut record_val, local_delta)
                } else {
                    Ok(())
                }
            };
            if let Err(e) = ret_val {
                // update status(fail)
                report_record_failure(
                    status,
                    &mut pending,
                    org_id,
                    stream_name,
                    doc_id,
                    &record_val,
                    &e.to_string(),
                    SCHEMA_CONFORMANCE_FAILED,
                    log_ingest_errors,
                );
                continue;
            }
        }

        // start check for alert trigger
        if let Some(alerts) = cur_stream_alerts
            && triggers.len() < alerts.len()
        {
            let end_time = now_micros();
            for alert in alerts {
                let key = format!(
                    "{}/{}/{}/{}",
                    org_id,
                    StreamType::Logs,
                    alert.stream_name,
                    alert.get_unique_key()
                );
                // For one alert, only one trigger per request
                // Trigger for this alert is already added.
                if evaluated_alerts.contains(&key) {
                    continue;
                }
                match alert
                    .evaluate(Some(&record_val), (None, end_time), None)
                    .await
                {
                    Ok(trigger_results) if trigger_results.data.is_some() => {
                        triggers.push((alert.clone(), trigger_results.data.unwrap()));
                        evaluated_alerts.insert(key);
                    }
                    Ok(_) => {
                        // the data doesn't satisfy the alert condition
                    }
                    Err(e) => {
                        log::error!("[LOGS] Error while evaluating realtime alert: {e}");
                    }
                }
            }
        }
        // end check for alert triggers

        // get distinct_value items
        if stream_settings.enable_distinct_fields {
            let mut map = Map::new();
            for field in DISTINCT_FIELDS.iter().chain(
                stream_settings
                    .distinct_value_fields
                    .iter()
                    .map(|f| &f.name),
            ) {
                if let Some(val) = record_val.get(field) {
                    map.insert(field.clone(), val.clone());
                }
            }

            if !map.is_empty() {
                // add distinct values
                distinct_values.push(MetadataItem::DistinctValues(DvItem {
                    stream_type: StreamType::Logs,
                    stream_name: stream_name.to_string(),
                    value: map,
                }));
            }
        }

        // get hour key
        let hour_key = get_write_partition_key(
            timestamp,
            &partition_keys,
            partition_time_level,
            &record_val,
            Some(&schema_key),
        );

        let hour_buf = write_buf.entry(hour_key).or_insert_with(|| SchemaRecords {
            schema_key: schema_key.clone(),
            schema: rec_schema.clone(),
            records: vec![],
            records_size: 0,
        });
        let record_val = Value::Object(record_val);
        let record_size = estimate_json_bytes(&record_val);
        hour_buf.records.push(Arc::new(record_val));
        hour_buf.records_size += record_size;

        // buffered only — success is accounted after the durable write
        pending.push(doc_id);
    }

    canonical_summary.flush_metrics(StreamType::Logs);
    if canonical_summary.nulled > 0 {
        let failure = canonical_summary.first_failure.as_ref().unwrap();
        log::warn!(
            "[LOGS] canonical schema normalization for {org_id}/{stream_name}: converted {} value(s), nulled {} failed value(s); first failure field={:?}, source_type={}, target_type={}",
            canonical_summary.converted,
            canonical_summary.nulled,
            failure.field,
            failure.source_type,
            failure.target_type,
        );
    }

    // write data to wal
    let writer =
        ingester::get_writer(thread_id, org_id, StreamType::Logs.as_str(), stream_name).await;
    let req_stats = match write_file(
        &writer,
        org_id,
        stream_name,
        write_buf,
        !cfg.common.wal_fsync_disabled,
    )
    .await
    {
        Ok(req_stats) => req_stats,
        Err(e) => {
            // The records are NOT durable. They are reported failed and the
            // error propagates: a shipper that reads `successful` or a 2xx
            // commits its offset and the records are gone.
            metrics::INGEST_ERRORS
                .with_label_values(&[
                    org_id,
                    StreamType::Logs.as_str(),
                    stream_name,
                    bulk::WRITE_FAILED,
                ])
                .inc_by(pending.len() as u64);
            pending.fail(status, stream_name, &e.to_string(), bulk::WRITE_FAILED);
            return Err(e);
        }
    };

    // durable: only now are the buffered records successful
    pending.commit(status, stream_name);

    // send distinct_values
    if !distinct_values.is_empty()
        && !stream_name.starts_with(DISTINCT_STREAM_PREFIX)
        && stream_settings.enable_distinct_fields
        && let Err(e) = write(org_id, MetadataType::DistinctValues, distinct_values).await
    {
        log::error!("Error while writing distinct values: {e}");
    }

    // only one trigger per request
    if !triggers.is_empty() {
        tokio::spawn(async move { evaluate_trigger(triggers).await });
    }

    Ok(req_stats)
}

async fn ingestion_log_enabled() -> bool {
    if !get_config().common.ingestion_log_enabled {
        return false;
    }
    // the logging will be enabled through meta only
    db::organization::get_org_setting_toggle_ingestion_logs(META_ORG_ID)
        .await
        .unwrap_or(false)
}

fn log_failed_record<T: std::fmt::Debug>(enabled: bool, record: &T, error: &str) {
    if !enabled {
        return;
    }
    log::warn!("failed to process record with error {error} : {record:?} ");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::meta::ingestion::{BulkResponse, RecordStatus};

    fn bulk_status() -> IngestionStatus {
        IngestionStatus::Bulk(BulkResponse {
            took: 0,
            errors: false,
            items: vec![],
        })
    }

    fn failed_items(
        status: &IngestionStatus,
    ) -> Vec<&crate::common::meta::ingestion::BulkResponseItem> {
        match status {
            IngestionStatus::Bulk(bulk_res) => bulk_res
                .items
                .iter()
                .flat_map(|item| item.values())
                .filter(|res| res.error.is_some())
                .collect(),
            IngestionStatus::Record(_) => vec![],
        }
    }

    /// `_id` is user-supplied: a numeric or null form is a per-record error,
    /// never `as_str().unwrap()` unwinding the whole request.
    #[test]
    fn test_record_doc_id_non_string_is_an_error() {
        let doc_id = |v: Value| {
            let mut record = Map::new();
            record.insert(DOC_ID_COL_NAME.to_string(), v);
            record_doc_id(&record)
        };

        assert_eq!(doc_id(Value::from("abc")), Ok(Some("abc".to_string())));
        assert!(doc_id(serde_json::json!(42)).is_err());
        assert!(doc_id(Value::Null).is_err());
        assert!(doc_id(serde_json::json!({"nested": true})).is_err());
        assert!(doc_id(serde_json::json!([1, 2])).is_err());
        // absent `_id` is not an error, it is simply no doc id
        assert_eq!(record_doc_id(&Map::new()), Ok(None));
    }

    /// A record with a bad `_id` fails on its own; its neighbours are
    /// unaffected and still get written.
    #[test]
    fn test_report_record_failure_fails_only_that_record() {
        let mut status = IngestionStatus::Record(RecordStatus::default());
        let mut pending = PendingRecords::new(&status);

        // record 1: good
        pending.push(None);
        // record 2: numeric `_id`
        let mut bad = Map::new();
        bad.insert(DOC_ID_COL_NAME.to_string(), serde_json::json!(7));
        let e = record_doc_id(&bad).unwrap_err();
        report_record_failure(
            &mut status,
            &mut pending,
            "org",
            "stream",
            None,
            &bad,
            &e,
            bulk::DOC_ID_INVALID,
            false,
        );
        // record 3: good
        pending.push(Some("doc3".to_string()));

        pending.commit(&mut status, "stream");
        match status {
            IngestionStatus::Record(status) => {
                assert_eq!(status.successful, 2);
                assert_eq!(status.failed, 1);
                assert!(status.error.contains(DOC_ID_COL_NAME));
            }
            IngestionStatus::Bulk(_) => panic!("expected a record status"),
        }
    }

    /// THE ordering contract (the regression this fixes): bulk items are
    /// emitted in REQUEST order even though successes are only accounted
    /// after the durable write. For [ok, bad, ok], a positional ES client
    /// (Filebeat/Logstash) must see the failure at index 1 — not first, with
    /// both successes shifted behind it.
    #[test]
    fn test_bulk_items_keep_request_order() {
        let mut status = bulk_status();
        let mut pending = PendingRecords::new(&status);

        // record 1: ok — buffered for the write
        pending.push(Some("doc1".to_string()));
        // record 2: fails during the loop (numeric `_id`)
        let mut bad = Map::new();
        bad.insert(DOC_ID_COL_NAME.to_string(), serde_json::json!(7));
        let e = record_doc_id(&bad).unwrap_err();
        report_record_failure(
            &mut status,
            &mut pending,
            "org",
            "stream",
            Some("doc2".to_string()),
            &bad,
            &e,
            bulk::DOC_ID_INVALID,
            false,
        );
        // record 3: ok — buffered for the write
        pending.push(Some("doc3".to_string()));

        // the durable write succeeds
        pending.commit(&mut status, "stream");

        let IngestionStatus::Bulk(bulk_res) = status else {
            panic!("expected a bulk status");
        };
        assert!(bulk_res.errors);
        let items: Vec<_> = bulk_res
            .items
            .iter()
            .map(|item| item.values().next().unwrap())
            .collect();
        assert_eq!(items.len(), 3, "one item per record, in request order");
        assert_eq!(
            items.iter().map(|i| i._id.as_str()).collect::<Vec<_>>(),
            vec!["doc1", "doc2", "doc3"],
            "items must keep request order"
        );
        assert!(items[0].error.is_none());
        let failed = items[1].error.as_ref().expect("index 1 is the bad record");
        assert_eq!(failed.err_type, bulk::DOC_ID_INVALID);
        assert!(items[2].error.is_none());
    }

    /// Same ordering contract on the failure path: when the durable write
    /// fails, pending records become WRITE_FAILED items at their own
    /// positions, and a record that failed earlier keeps its original error.
    #[test]
    fn test_bulk_items_keep_request_order_on_write_failure() {
        let mut status = bulk_status();
        let mut pending = PendingRecords::new(&status);

        pending.push(Some("doc1".to_string()));
        let mut bad = Map::new();
        bad.insert(DOC_ID_COL_NAME.to_string(), serde_json::json!(7));
        let e = record_doc_id(&bad).unwrap_err();
        report_record_failure(
            &mut status,
            &mut pending,
            "org",
            "stream",
            Some("doc2".to_string()),
            &bad,
            &e,
            bulk::DOC_ID_INVALID,
            false,
        );
        pending.push(Some("doc3".to_string()));

        // only the two pending records count against the failed write
        assert_eq!(pending.len(), 2);
        pending.fail(
            &mut status,
            "stream",
            "wal write failed",
            bulk::WRITE_FAILED,
        );

        let IngestionStatus::Bulk(bulk_res) = status else {
            panic!("expected a bulk status");
        };
        let items: Vec<_> = bulk_res
            .items
            .iter()
            .map(|item| item.values().next().unwrap())
            .collect();
        assert_eq!(
            items.iter().map(|i| i._id.as_str()).collect::<Vec<_>>(),
            vec!["doc1", "doc2", "doc3"],
            "items must keep request order"
        );
        let err_types: Vec<_> = items
            .iter()
            .map(|i| i.error.as_ref().unwrap().err_type.as_str())
            .collect();
        assert_eq!(
            err_types,
            vec![bulk::WRITE_FAILED, bulk::DOC_ID_INVALID, bulk::WRITE_FAILED],
            "the mid-loop failure keeps its own error, in place"
        );
    }

    /// The write never became durable: the buffered records must be reported
    /// FAILED, not successful. A shipper deletes its copy on `successful`.
    #[test]
    fn test_pending_records_failed_write_is_not_successful() {
        let mut status = IngestionStatus::Record(RecordStatus::default());
        let mut pending = PendingRecords::new(&status);
        for _ in 0..3 {
            pending.push(None);
        }
        assert_eq!(pending.len(), 3);
        pending.fail(
            &mut status,
            "stream",
            "wal write failed: no space left",
            bulk::WRITE_FAILED,
        );

        match status {
            IngestionStatus::Record(status) => {
                assert_eq!(status.successful, 0);
                assert_eq!(status.failed, 3);
                assert!(status.error.contains("no space left"));
            }
            IngestionStatus::Bulk(_) => panic!("expected a record status"),
        }
    }

    /// Same contract for the bulk shape: every buffered record becomes a
    /// failed item and `errors` is set.
    #[test]
    fn test_pending_records_failed_write_marks_bulk_items() {
        let mut status = bulk_status();
        let mut pending = PendingRecords::new(&status);
        pending.push(Some("doc1".to_string()));
        pending.push(Some("doc2".to_string()));
        pending.fail(
            &mut status,
            "stream",
            "wal write failed",
            bulk::WRITE_FAILED,
        );

        let failed = failed_items(&status);
        assert_eq!(failed.len(), 2);
        for item in failed {
            let error = item.error.as_ref().unwrap();
            assert_eq!(error.err_type, bulk::WRITE_FAILED);
            assert!(error.reason.contains("wal write failed"));
        }
        match status {
            IngestionStatus::Bulk(bulk_res) => assert!(bulk_res.errors),
            IngestionStatus::Record(_) => panic!("expected a bulk status"),
        }
    }

    /// A durable write is the only thing that turns buffered records into
    /// successes.
    #[test]
    fn test_pending_records_commit_after_durable_write() {
        let mut status = IngestionStatus::Record(RecordStatus::default());
        let mut pending = PendingRecords::new(&status);
        pending.push(None);
        pending.push(None);
        pending.commit(&mut status, "stream");

        match status {
            IngestionStatus::Record(status) => {
                assert_eq!(status.successful, 2);
                assert_eq!(status.failed, 0);
            }
            IngestionStatus::Bulk(_) => panic!("expected a record status"),
        }
    }

    /// Pre-loop failures (schema lookup, schema evolution) account every
    /// record of the stream, so nothing is lost silently.
    #[test]
    fn test_pending_records_all_accounts_every_record() {
        let json_data: Vec<(i64, Map<String, Value>)> = (0..4)
            .map(|i| {
                let mut record = Map::new();
                record.insert(DOC_ID_COL_NAME.to_string(), Value::from(format!("d{i}")));
                (i, record)
            })
            .collect();

        let mut status = IngestionStatus::Record(RecordStatus::default());
        PendingRecords::all(&status, &json_data).fail(
            &mut status,
            "stream",
            "schema not found",
            bulk::WRITE_FAILED,
        );
        match status {
            IngestionStatus::Record(status) => assert_eq!(status.failed, 4),
            IngestionStatus::Bulk(_) => panic!("expected a record status"),
        }

        // the bulk shape keeps the doc ids so the client can match items
        let mut status = bulk_status();
        PendingRecords::all(&status, &json_data).fail(
            &mut status,
            "stream",
            "schema not found",
            bulk::WRITE_FAILED,
        );
        let failed = failed_items(&status);
        assert_eq!(failed.len(), 4);
        assert_eq!(failed[0]._id, "d0");
    }

    /// A stream that is being deleted drops its records but reports them
    /// truthfully: the items carry the STREAM_DELETING error type, and the
    /// aggregate is a PERMANENT rejection that keeps the request 200 while
    /// the response error says what was dropped and why.
    #[test]
    fn test_deleting_stream_is_reported_but_stays_200() {
        let json_data: Vec<(i64, Map<String, Value>)> = (0..2)
            .map(|i| {
                let mut record = Map::new();
                record.insert(DOC_ID_COL_NAME.to_string(), Value::from(format!("d{i}")));
                (i, record)
            })
            .collect();

        // the bulk items name the real reason, not a generic write failure
        let mut status = bulk_status();
        let error = "stream [logs] is being deleted";
        PendingRecords::all(&status, &json_data).fail(
            &mut status,
            "logs",
            error,
            bulk::STREAM_DELETING,
        );
        let failed = failed_items(&status);
        assert_eq!(failed.len(), 2);
        for item in &failed {
            let err = item.error.as_ref().unwrap();
            assert_eq!(err.err_type, bulk::STREAM_DELETING);
            assert!(err.reason.contains("being deleted"));
        }

        // a request whose only failures are permanent rejections stays 200,
        // and the message still names the drop
        let failures = vec![StreamWriteFailure {
            stream_name: "logs".to_string(),
            records: 2,
            error: Error::IngestionError(error.to_string()),
            permanent_rejection: true,
        }];
        assert_eq!(write_failure_status_code(&failures), 200);
        let message = write_failure_message(&failures);
        assert!(message.contains("logs") && message.contains("being deleted"));

        // mixed with a REAL write failure, the request must not be 2xx
        let failures = vec![
            StreamWriteFailure {
                stream_name: "logs".to_string(),
                records: 2,
                error: Error::IngestionError(error.to_string()),
                permanent_rejection: true,
            },
            StreamWriteFailure {
                stream_name: "other".to_string(),
                records: 1,
                error: Error::IngestionError("disk full".to_string()),
                permanent_rejection: false,
            },
        ];
        assert_eq!(write_failure_status_code(&failures), 500);

        // and backpressure still wins as 503
        let failures = vec![
            StreamWriteFailure {
                stream_name: "logs".to_string(),
                records: 2,
                error: Error::IngestionError(error.to_string()),
                permanent_rejection: true,
            },
            StreamWriteFailure {
                stream_name: "other".to_string(),
                records: 1,
                error: Error::ResourceError("memtable is full".to_string()),
                permanent_rejection: false,
            },
        ];
        assert_eq!(write_failure_status_code(&failures), 503);
    }

    /// Backpressure is retryable (503), any other write failure is a 500 —
    /// and never a 2xx.
    #[test]
    fn test_write_failure_status_code_and_message() {
        let failures = vec![StreamWriteFailure {
            stream_name: "logs_a".to_string(),
            records: 7,
            error: Error::IngestionError("disk full".to_string()),
            permanent_rejection: false,
        }];
        assert_eq!(write_failure_status_code(&failures), 500);

        let failures = vec![
            StreamWriteFailure {
                stream_name: "logs_a".to_string(),
                records: 7,
                error: Error::IngestionError("disk full".to_string()),
                permanent_rejection: false,
            },
            StreamWriteFailure {
                stream_name: "logs_b".to_string(),
                records: 2,
                error: Error::ResourceError("memtable is full".to_string()),
                permanent_rejection: false,
            },
        ];
        assert_eq!(write_failure_status_code(&failures), 503);

        // every failing stream is named: multi-stream requests report the
        // aggregate, they do not stop at the first failure
        let message = write_failure_message(&failures);
        assert!(message.contains("logs_a"), "{message}");
        assert!(message.contains("logs_b"), "{message}");
        assert!(message.contains('7') && message.contains('2'), "{message}");

        assert_eq!(write_failure_status_code(&[]), 500);
    }

    #[test]
    fn test_set_parsing_error() {
        let mut parse_error = String::new();
        set_parsing_error(&mut parse_error, &Field::new("test", DataType::Utf8, true));
        assert!(!parse_error.is_empty());
    }

    #[test]
    fn test_cast_to_type() {
        let mut local_val = Map::new();
        local_val.insert("test".to_string(), Value::from("test13212"));
        let delta = vec![Field::new("test", DataType::Utf8, true)];
        let ret_val = cast_to_type(&mut local_val, delta);
        assert!(ret_val.is_ok());
    }

    #[test]
    fn test_parse_bulk_index_index_action() {
        let v = serde_json::json!({"index": {"_index": "my-stream", "_id": "doc1"}});
        let result = parse_bulk_index(&v);
        assert!(result.is_some());
        let (action, index, doc_id) = result.unwrap();
        assert_eq!(action, "index");
        assert_eq!(index, "my-stream");
        assert_eq!(doc_id, Some("doc1"));
    }

    #[test]
    fn test_parse_bulk_index_create_action_no_doc_id() {
        let v = serde_json::json!({"create": {"_index": "my-stream"}});
        let result = parse_bulk_index(&v);
        assert!(result.is_some());
        let (action, index, doc_id) = result.unwrap();
        assert_eq!(action, "create");
        assert_eq!(index, "my-stream");
        assert!(doc_id.is_none());
    }

    #[test]
    fn test_parse_bulk_index_no_known_action_returns_none() {
        let v = serde_json::json!({"delete": {"_index": "my-stream"}});
        let result = parse_bulk_index(&v);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_bulk_index_missing_index_skips_action() {
        let v = serde_json::json!({"index": {"_id": "doc1"}});
        let result = parse_bulk_index(&v);
        assert!(result.is_none());
    }

    #[test]
    fn test_rename_reserved_source_field() {
        // a user `_source` field is renamed, everything else untouched
        let mut record = serde_json::json!({
            "_timestamp": 1,
            "_source": "USER-VALUE",
            "log": "keep",
        });
        rename_reserved_source_field(record.as_object_mut().unwrap());
        assert_eq!(
            record,
            serde_json::json!({
                "_timestamp": 1,
                "_source_field": "USER-VALUE",
                "log": "keep",
            })
        );

        // non-string values move too (the guard is type-agnostic)
        let mut record = serde_json::json!({"_source": {"nested": true}});
        rename_reserved_source_field(record.as_object_mut().unwrap());
        assert_eq!(
            record,
            serde_json::json!({"_source_field": {"nested": true}})
        );

        // no `_source`: a no-op
        let mut record = serde_json::json!({"log": "x"});
        rename_reserved_source_field(record.as_object_mut().unwrap());
        assert_eq!(record, serde_json::json!({"log": "x"}));

        // degenerate double naming: the `_source` value wins (documented)
        let mut record = serde_json::json!({"_source": "a", "_source_field": "b"});
        rename_reserved_source_field(record.as_object_mut().unwrap());
        assert_eq!(record, serde_json::json!({"_source_field": "a"}));
    }

    /// The write_logs funnel runs BOTH per-record guards on every flattened
    /// record: the reserved `_source` rename and the reserved trace-context
    /// alias canonicalization (dotted `trace.id`/`span.id` fold into the
    /// canonical `trace_id`/`span_id`; other dotted fields keep the dotted
    /// canon).
    #[test]
    fn test_write_logs_record_guards() {
        let flattened = flatten::flatten(serde_json::json!({
            "_timestamp": 1,
            "_source": "USER-VALUE",
            "log": "keep",
            "trace": {"id": "nested-tid"},
            "span.id": "literal-sid",
            "span_id": "winner-sid",
            "service.name": "svc",
        }))
        .unwrap();
        let mut record = match flattened {
            Value::Object(map) => map,
            _ => unreachable!(),
        };

        // the exact two-step sequence write_logs applies per record
        rename_reserved_source_field(&mut record);
        flatten::canonicalize_reserved_aliases(&mut record);

        assert_eq!(
            Value::Object(record),
            serde_json::json!({
                "_timestamp": 1,
                "_source_field": "USER-VALUE",
                "log": "keep",
                "trace_id": "nested-tid",
                "span_id": "winner-sid",
                "service.name": "svc",
            })
        );
    }

    /// The `_timestamp` canonicalization of the write_logs funnel (the third
    /// per-record guard): the STORED field must equal the per-record
    /// partition timestamp as an i64 — a path that re-shapes records after
    /// their timestamp was fixed (e.g. an OTLP pipeline whose VRL re-types or
    /// drops `_timestamp`) otherwise stores a divergent form, and the WAL
    /// arrow conversion coerces a non-integer form to a LITERAL 0 (the live
    /// zero-min_ts regression).
    #[test]
    fn test_write_logs_timestamp_canonicalization() {
        let partition_ts = 1_784_950_134_213_159i64;
        let canonicalize = |mut record: Map<String, Value>| {
            // the exact expression write_logs applies per record
            if record.get(TIMESTAMP_COL_NAME).and_then(Value::as_i64) != Some(partition_ts) {
                record.insert(
                    TIMESTAMP_COL_NAME.to_string(),
                    Value::Number(partition_ts.into()),
                );
            }
            record
        };
        let ts_of = |record: &Map<String, Value>| record.get(TIMESTAMP_COL_NAME).cloned();

        // already canonical: untouched
        let record = canonicalize(
            serde_json::json!({ "_timestamp": partition_ts, "log": "x" })
                .as_object()
                .unwrap()
                .clone(),
        );
        assert_eq!(ts_of(&record), Some(Value::Number(partition_ts.into())));

        // a VRL-re-typed string form is forced back to the partition i64
        // (stored as a string it would coerce to a LITERAL 0 in the WAL)
        let record = canonicalize(
            serde_json::json!({ "_timestamp": "2026-07-24T10:08:54Z", "log": "x" })
                .as_object()
                .unwrap()
                .clone(),
        );
        assert_eq!(ts_of(&record), Some(Value::Number(partition_ts.into())));

        // a dropped `_timestamp` is restored; a zeroed one is repaired
        for input in [
            serde_json::json!({ "log": "x" }),
            serde_json::json!({ "_timestamp": 0, "log": "x" }),
        ] {
            let record = canonicalize(input.as_object().unwrap().clone());
            assert_eq!(ts_of(&record), Some(Value::Number(partition_ts.into())));
        }
    }
}
