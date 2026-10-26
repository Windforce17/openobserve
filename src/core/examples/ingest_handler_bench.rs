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

//! Ingest handler benchmark: drives the real request handlers in-process —
//! `logs::ingest::ingest` for `_multi` / `_json`, `logs::otlp::handle_request`
//! (after the protobuf decode the HTTP handler does) and `traces::otlp_proto`
//! — in segment mode, exactly the prod write tail, and drains the segment
//! buffer after every request (the drain is not timed).
//!
//! Payloads are synthesized from a field-statistics file (field name,
//! presence ratio, dominant type, average string length — no prod values):
//! `.work/ingest-perf/corpus/field_stats.json`. Dotted names become nested
//! objects for the JSON endpoints (flatten joins with `.`), OTLP keys stay
//! dotted like the OTel SDKs send them.
//!
//! Reports per case: µs per record (handler wall time on a 1-worker runtime,
//! requests run sequentially), heap allocations and allocated bytes per
//! record, and rows that actually reached the segment buffer (must equal the
//! records sent, otherwise the run measured an error path).
//!
//! Env knobs:
//!   BENCH_FIELD_STATS  path to field_stats.json            (required)
//!   BENCH_CASES        comma list, default all:
//!                      multi_vpc,multi_apisix,multi_waf,json_apisix,otlp_logs,otlp_traces
//!   BENCH_REQUESTS     timed requests per case             (default 40)
//!   BENCH_WARMUP       untimed requests per case           (default 5)
//!   BENCH_SEED         RNG seed                            (default 1)
//!
//! Run:
//!   cargo run --release -p openobserve-core --example ingest_handler_bench

use std::{
    alloc::{GlobalAlloc, Layout},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use bytes::Bytes;
use opentelemetry_proto::tonic::{
    collector::{logs::v1::ExportLogsServiceRequest, trace::v1::ExportTraceServiceRequest},
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value as OtlpValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span::Event},
};
use prost::Message;
use serde_json::{Map, Value};

/// Counts every allocation on top of mimalloc, the prod allocator.
struct CountingAlloc;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { mimalloc::MiMalloc.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// xorshift64* — deterministic, dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn text(&mut self, len: usize) -> String {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-_/.: ";
        (0..len)
            .map(|_| ALPHABET[self.below(ALPHABET.len() as u64) as usize] as char)
            .collect()
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Str,
    Int,
    Float,
    Bool,
}

/// One field of a stream's statistics; `pool` holds the distinct string
/// values the generator draws from (prod values repeat heavily).
struct FieldSpec {
    name: String,
    presence: f64,
    kind: Kind,
    pool: Vec<String>,
}

/// Distinct values per string field: enough to defeat trivial caching,
/// small enough to keep the repetition that prod fields show.
const STRING_POOL_SIZE: usize = 64;

fn load_specs(stats: &Value, key: &str, rng: &mut Rng) -> anyhow::Result<Vec<FieldSpec>> {
    let fields = stats
        .get(key)
        .and_then(|v| v.get("fields"))
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("field stats file has no `{key}.fields` object"))?;
    let mut specs = Vec::with_capacity(fields.len());
    for (name, stat) in fields {
        if name == "_timestamp" {
            continue;
        }
        let presence = stat.get("presence").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let types = stat.get("types").and_then(|v| v.as_object());
        let dominant = types
            .and_then(|t| {
                t.iter()
                    .filter(|(k, _)| k.as_str() != "null")
                    .max_by_key(|(_, n)| n.as_u64().unwrap_or(0))
                    .map(|(k, _)| k.clone())
            })
            .unwrap_or_else(|| "str".to_string());
        let kind = match dominant.as_str() {
            "int" => Kind::Int,
            "float" => Kind::Float,
            "bool" => Kind::Bool,
            _ => Kind::Str,
        };
        let avg_len = stat
            .get("avg_str_len")
            .and_then(|v| v.as_f64())
            .unwrap_or(8.0)
            .round()
            .max(1.0) as usize;
        let pool = if kind == Kind::Str {
            (0..STRING_POOL_SIZE).map(|_| rng.text(avg_len)).collect()
        } else {
            Vec::new()
        };
        specs.push(FieldSpec {
            name: name.clone(),
            presence,
            kind,
            pool,
        });
    }
    // deterministic order regardless of the stats file's map order
    specs.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(specs)
}

fn json_value(spec: &FieldSpec, rng: &mut Rng) -> Value {
    match spec.kind {
        Kind::Str => Value::String(spec.pool[rng.below(spec.pool.len() as u64) as usize].clone()),
        Kind::Int => Value::from(rng.below(1 << 31)),
        Kind::Float => Value::from((rng.unit() * 1000.0 * 1000.0).round() / 1000.0),
        Kind::Bool => Value::Bool(rng.below(2) == 1),
    }
}

fn otlp_value(spec: &FieldSpec, rng: &mut Rng) -> AnyValue {
    let value = match spec.kind {
        Kind::Str => {
            OtlpValue::StringValue(spec.pool[rng.below(spec.pool.len() as u64) as usize].clone())
        }
        Kind::Int => OtlpValue::IntValue(rng.below(1 << 31) as i64),
        Kind::Float => OtlpValue::DoubleValue((rng.unit() * 1000.0 * 1000.0).round() / 1000.0),
        Kind::Bool => OtlpValue::BoolValue(rng.below(2) == 1),
    };
    AnyValue { value: Some(value) }
}

fn key_value(key: &str, value: AnyValue) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(value),
        ..Default::default()
    }
}

/// Attributes for the specs that pass their presence draw; `strip` bytes are
/// cut from each name (resource fields are stored with a `service_` prefix).
fn sampled_attributes(specs: &[&FieldSpec], strip: usize, rng: &mut Rng) -> Vec<KeyValue> {
    let mut attributes = Vec::with_capacity(specs.len());
    for spec in specs {
        if rng.unit() < spec.presence {
            let value = otlp_value(spec, rng);
            attributes.push(key_value(&spec.name[strip..], value));
        }
    }
    attributes
}

/// Insert `value` at the dotted `path`, creating nested objects; a path whose
/// prefix is already a leaf is skipped (the stats are flattened names).
fn insert_path(map: &mut Map<String, Value>, path: &str, value: Value) {
    let mut parts = path.split('.').peekable();
    let mut current = map;
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            current.entry(part.to_string()).or_insert(value);
            return;
        }
        let entry = current
            .entry(part.to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        current = match entry {
            Value::Object(inner) => inner,
            _ => return,
        };
    }
}

fn json_record(specs: &[FieldSpec], rng: &mut Rng) -> Value {
    let mut record = Map::new();
    for spec in specs {
        if rng.unit() < spec.presence {
            let value = json_value(spec, rng);
            insert_path(&mut record, &spec.name, value);
        }
    }
    Value::Object(record)
}

fn ndjson_payload(specs: &[FieldSpec], records: usize, rng: &mut Rng) -> anyhow::Result<Bytes> {
    let mut out = Vec::with_capacity(records * 512);
    for _ in 0..records {
        serde_json::to_writer(&mut out, &json_record(specs, rng))?;
        out.push(b'\n');
    }
    Ok(Bytes::from(out))
}

fn json_array_payload(specs: &[FieldSpec], records: usize, rng: &mut Rng) -> anyhow::Result<Bytes> {
    let values: Vec<Value> = (0..records).map(|_| json_record(specs, rng)).collect();
    Ok(Bytes::from(serde_json::to_vec(&values)?))
}

fn now_nanos() -> u64 {
    // a differential run (BENCH_DIGEST_DIR) pins "now" so OTLP timestamps,
    // and with them the written rows, are identical across binaries
    if let Some(fixed) = std::env::var("BENCH_FIXED_NOW_NANOS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        return fixed;
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Resource attributes of the `default` logs stream: the k8s/host/os keys the
/// collector's k8sattributes processor adds, plus `service.name`.
fn is_log_resource_field(name: &str) -> bool {
    name.starts_with("k8s.")
        || name.starts_with("host.")
        || name.starts_with("os.")
        || name == "k8s_cluster"
}

/// Fields the OTLP logs handler derives from LogRecord members, not attributes.
const LOG_RECORD_FIELDS: &[&str] = &[
    "body",
    "severity",
    "dropped_attributes_count",
    "trace_id",
    "span_id",
    "service_name",
];

const LOG_RECORDS_PER_RESOURCE: usize = 100;

fn otlp_logs_payload(specs: &[FieldSpec], records: usize, rng: &mut Rng) -> Bytes {
    let resource_specs: Vec<&FieldSpec> = specs
        .iter()
        .filter(|s| is_log_resource_field(&s.name))
        .collect();
    let attribute_specs: Vec<&FieldSpec> = specs
        .iter()
        .filter(|s| {
            !is_log_resource_field(&s.name) && !LOG_RECORD_FIELDS.contains(&s.name.as_str())
        })
        .collect();
    let body_spec = specs.iter().find(|s| s.name == "body");
    let service_spec = specs.iter().find(|s| s.name == "service_name");
    let now = now_nanos();
    let mut resource_logs = Vec::new();
    let mut remaining = records;
    while remaining > 0 {
        let batch = remaining.min(LOG_RECORDS_PER_RESOURCE);
        remaining -= batch;
        let mut attributes: Vec<KeyValue> = resource_specs
            .iter()
            .map(|s| key_value(&s.name, otlp_value(s, rng)))
            .collect();
        if let Some(spec) = service_spec {
            attributes.push(key_value("service.name", otlp_value(spec, rng)));
        }
        let log_records = (0..batch)
            .map(|i| LogRecord {
                time_unix_nano: now + i as u64,
                observed_time_unix_nano: now + i as u64,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: body_spec.map(|s| otlp_value(s, rng)),
                attributes: sampled_attributes(&attribute_specs, 0, rng),
                trace_id: rng.bytes(16),
                span_id: rng.bytes(8),
                ..Default::default()
            })
            .collect();
        resource_logs.push(ResourceLogs {
            resource: Some(Resource {
                attributes,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "bench".to_string(),
                    ..Default::default()
                }),
                log_records,
                ..Default::default()
            }],
            ..Default::default()
        });
    }
    Bytes::from(ExportLogsServiceRequest { resource_logs }.encode_to_vec())
}

/// Columns the traces handler derives from Span members; `service_*` fields
/// are resource attributes (stored with the `service_` prefix).
const SPAN_FIELDS: &[&str] = &[
    "duration",
    "end_time",
    "events",
    "flags",
    "links",
    "operation_name",
    "service_name",
    "span_duration_nano",
    "span_id",
    "span_kind",
    "span_status",
    "start_time",
    "status_code",
    "status_message",
    "trace_id",
    "reference.parent_span_id",
    "reference.parent_trace_id",
    "reference.ref_type",
];

const SPANS_PER_RESOURCE: usize = 50;

fn otlp_traces_payload(specs: &[FieldSpec], spans: usize, rng: &mut Rng) -> Bytes {
    let resource_specs: Vec<&FieldSpec> = specs
        .iter()
        .filter(|s| s.name.starts_with("service_") && !SPAN_FIELDS.contains(&s.name.as_str()))
        .collect();
    let attribute_specs: Vec<&FieldSpec> = specs
        .iter()
        .filter(|s| !s.name.starts_with("service_") && !SPAN_FIELDS.contains(&s.name.as_str()))
        .collect();
    let operation_spec = specs.iter().find(|s| s.name == "operation_name");
    let service_spec = specs.iter().find(|s| s.name == "service_name");
    let now = now_nanos();
    let mut resource_spans = Vec::new();
    let mut remaining = spans;
    while remaining > 0 {
        let batch = remaining.min(SPANS_PER_RESOURCE);
        remaining -= batch;
        let mut attributes = sampled_attributes(&resource_specs, "service_".len(), rng);
        if let Some(spec) = service_spec {
            attributes.push(key_value("service.name", otlp_value(spec, rng)));
        }
        let trace_id = rng.bytes(16);
        let spans = (0..batch)
            .map(|i| {
                let start = now - 5_000_000 + i as u64 * 1_000;
                Span {
                    trace_id: trace_id.clone(),
                    span_id: rng.bytes(8),
                    parent_span_id: if i == 0 { Vec::new() } else { rng.bytes(8) },
                    name: operation_spec
                        .map(|s| s.pool[rng.below(s.pool.len() as u64) as usize].clone())
                        .unwrap_or_else(|| "op".to_string()),
                    kind: 2,
                    start_time_unix_nano: start,
                    end_time_unix_nano: start + rng.below(2_000_000),
                    attributes: sampled_attributes(&attribute_specs, 0, rng),
                    events: if rng.below(4) == 0 {
                        vec![Event {
                            time_unix_nano: start + 10,
                            name: "event".to_string(),
                            ..Default::default()
                        }]
                    } else {
                        Vec::new()
                    },
                    status: Some(Status {
                        code: 0,
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            })
            .collect();
        resource_spans.push(ResourceSpans {
            resource: Some(Resource {
                attributes,
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "bench".to_string(),
                    ..Default::default()
                }),
                spans,
                ..Default::default()
            }],
            ..Default::default()
        });
    }
    Bytes::from(ExportTraceServiceRequest { resource_spans }.encode_to_vec())
}

enum Payload {
    Multi { stream: &'static str, body: Bytes },
    Json { stream: &'static str, body: Bytes },
    OtlpLogs(Bytes),
    OtlpTraces(Bytes),
}

struct Case {
    name: &'static str,
    records_per_request: usize,
    payloads: Vec<Payload>,
    /// `_multi`/`_json` records carry no `_timestamp`, so ingest stamps the
    /// wall clock: a digest must mask it to compare two runs
    mask_ts: bool,
}

/// Distinct payloads per case; requests cycle through them.
const PAYLOAD_VARIANTS: usize = 4;

fn build_case(name: &'static str, stats: &Value, rng: &mut Rng) -> anyhow::Result<Case> {
    // records per request follow the prod request shapes: Vector `_multi`
    // batches thousands of NDJSON lines, OTel collectors send a few hundred
    let (key, records_per_request) = match name {
        "multi_vpc" => ("logs/aws_vpc_flow_logs", 5000),
        "multi_apisix" => ("logs/apisix", 2000),
        "multi_waf" => ("logs/aws_waf_log", 1000),
        "json_apisix" => ("logs/apisix", 2000),
        "otlp_logs" => ("logs/default", 200),
        "otlp_traces" => ("traces/default", 200),
        other => anyhow::bail!("unknown case `{other}`"),
    };
    let specs = load_specs(stats, key, rng)?;
    let mut payloads = Vec::with_capacity(PAYLOAD_VARIANTS);
    for _ in 0..PAYLOAD_VARIANTS {
        let payload = match name {
            "multi_vpc" => Payload::Multi {
                stream: "bench_vpc",
                body: ndjson_payload(&specs, records_per_request, rng)?,
            },
            "multi_apisix" => Payload::Multi {
                stream: "bench_apisix",
                body: ndjson_payload(&specs, records_per_request, rng)?,
            },
            "multi_waf" => Payload::Multi {
                stream: "bench_waf",
                body: ndjson_payload(&specs, records_per_request, rng)?,
            },
            "json_apisix" => Payload::Json {
                stream: "bench_apisix_json",
                body: json_array_payload(&specs, records_per_request, rng)?,
            },
            "otlp_logs" => Payload::OtlpLogs(otlp_logs_payload(&specs, records_per_request, rng)),
            _ => Payload::OtlpTraces(otlp_traces_payload(&specs, records_per_request, rng)),
        };
        payloads.push(payload);
    }
    Ok(Case {
        name,
        records_per_request,
        payloads,
        mask_ts: name.starts_with("multi_") || name.starts_with("json_"),
    })
}

const BENCH_USER: &str = "bench@local.dev";

async fn send(payload: &Payload) -> anyhow::Result<()> {
    use openobserve_core::ingestion_types::{IngestUser, IngestionRequest};
    match payload {
        Payload::Multi { stream, body } => {
            openobserve_core::logs::ingest::ingest(
                0,
                "default",
                stream,
                IngestionRequest::Multi(body.clone()),
                IngestUser::User(BENCH_USER.to_string()),
                None,
                false,
            )
            .await
            .map_err(|e| anyhow::anyhow!("_multi ingest into {stream} failed: {e}"))?;
        }
        Payload::Json { stream, body } => {
            openobserve_core::logs::ingest::ingest(
                0,
                "default",
                stream,
                IngestionRequest::JSON(body.clone()),
                IngestUser::User(BENCH_USER.to_string()),
                None,
                false,
            )
            .await
            .map_err(|e| anyhow::anyhow!("_json ingest into {stream} failed: {e}"))?;
        }
        Payload::OtlpLogs(body) => {
            // the HTTP handler's decode is part of the request cost
            let request = ExportLogsServiceRequest::decode(body.clone())
                .map_err(|e| anyhow::anyhow!("decode bench OTLP logs payload: {e}"))?;
            openobserve_core::logs::otlp::handle_request(
                0,
                "default",
                request,
                None,
                BENCH_USER,
                config::meta::otlp::OtlpRequestType::HttpProtobuf,
            )
            .await
            .map_err(|e| anyhow::anyhow!("OTLP logs handle_request failed: {e}"))?;
        }
        Payload::OtlpTraces(body) => {
            openobserve_core::traces::otlp_proto(
                "default",
                body.clone(),
                None,
                IngestUser::User(BENCH_USER.to_string()),
            )
            .await
            .map_err(|e| anyhow::anyhow!("OTLP traces otlp_proto failed: {e}"))?;
        }
    }
    Ok(())
}

fn drain_rows(digest: Option<(&mut Vec<String>, bool)>) -> usize {
    let frames = segment_wal::global_buffer().drain();
    if let Some((out, mask_ts)) = digest {
        for frame in frames.iter() {
            out.push(canonical_frame(frame, mask_ts));
        }
    }
    frames.iter().map(|frame| frame.batch.num_rows()).sum()
}

/// One frame as text: header, schema and rows with columns in name order, so
/// two runs compare equal exactly when they wrote the same logical rows.
fn canonical_frame(frame: &segment_wal::SegmentFrame, mask_ts: bool) -> String {
    use std::{collections::BTreeMap, fmt::Write};

    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let batch = &frame.batch;
    let schema = batch.schema();
    let mut cols: Vec<usize> = (0..schema.fields().len()).collect();
    cols.sort_by(|a, b| schema.field(*a).name().cmp(schema.field(*b).name()));
    let mut out = String::new();
    let (min_ts, max_ts) = if mask_ts {
        (0, 0)
    } else {
        (frame.min_ts, frame.max_ts)
    };
    let _ = writeln!(
        out,
        "{}/{}/{} ts={min_ts}..{max_ts} rows={} schema_meta={:?}",
        frame.org,
        frame.stream_type,
        frame.stream,
        batch.num_rows(),
        schema.metadata().iter().collect::<BTreeMap<_, _>>()
    );
    for &c in &cols {
        let f = schema.field(c);
        let _ = write!(
            out,
            "{}:{}:{}:{:?};",
            f.name(),
            f.data_type(),
            f.is_nullable(),
            f.metadata().iter().collect::<BTreeMap<_, _>>()
        );
    }
    out.push('\n');
    let options = FormatOptions::default().with_null("\u{2205}");
    let formatters: Vec<Option<ArrayFormatter>> = cols
        .iter()
        .map(|&c| {
            if mask_ts && schema.field(c).name() == "_timestamp" {
                None
            } else {
                Some(
                    ArrayFormatter::try_new(batch.column(c).as_ref(), &options)
                        .expect("every written column type has an arrow display formatter"),
                )
            }
        })
        .collect();
    for row in 0..batch.num_rows() {
        for formatter in &formatters {
            match formatter {
                Some(f) => {
                    let _ = write!(out, "{}", f.value(row));
                }
                None => out.push('T'),
            }
            out.push('\u{1f}');
        }
        out.push('\n');
    }
    out
}

struct CaseResult {
    records: usize,
    rows: usize,
    elapsed: Duration,
    allocs: u64,
    alloc_bytes: u64,
}

async fn run_case(
    case: &Case,
    warmup: usize,
    requests: usize,
    mut digest: Option<&mut Vec<String>>,
) -> anyhow::Result<CaseResult> {
    for i in 0..warmup {
        send(&case.payloads[i % case.payloads.len()]).await?;
        drain_rows(None);
    }
    let mut result = CaseResult {
        records: 0,
        rows: 0,
        elapsed: Duration::ZERO,
        allocs: 0,
        alloc_bytes: 0,
    };
    for i in 0..requests {
        let payload = &case.payloads[i % case.payloads.len()];
        let allocs_before = ALLOCS.load(Ordering::Relaxed);
        let bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
        let start = Instant::now();
        send(payload).await?;
        result.elapsed += start.elapsed();
        result.allocs += ALLOCS.load(Ordering::Relaxed) - allocs_before;
        result.alloc_bytes += ALLOC_BYTES.load(Ordering::Relaxed) - bytes_before;
        result.records += case.records_per_request;
        result.rows += drain_rows(digest.as_deref_mut().map(|d| (d, case.mask_ts)));
    }
    Ok(result)
}

fn env_usize(name: &str, default: usize) -> anyhow::Result<usize> {
    match std::env::var(name) {
        Ok(v) => v
            .parse()
            .map_err(|e| anyhow::anyhow!("{name}={v} is not a non-negative integer: {e}")),
        Err(_) => Ok(default),
    }
}

/// Prod-equivalent ingest settings; set before the config is first read.
fn set_default_env(data_dir: &std::path::Path) {
    let defaults = [
        ("ZO_LOCAL_MODE", "true".to_string()),
        ("ZO_INGEST_SEGMENT_MODE", "true".to_string()),
        ("ZO_WAL_NARROW_SCHEMA", "true".to_string()),
        ("ZO_INGEST_FLATTEN_LEVEL", "0".to_string()),
        ("ZO_SEGMENT_BUFFER_MAX_MB", "8192".to_string()),
        ("ZO_MEMORY_CIRCUIT_BREAKER_ENABLED", "false".to_string()),
        ("ZO_DISK_CIRCUIT_BREAKER_ENABLED", "false".to_string()),
        ("ZO_USAGE_REPORTING_ENABLED", "false".to_string()),
        ("ZO_TELEMETRY", "false".to_string()),
        ("ZO_DATA_DIR", format!("{}/", data_dir.display())),
    ];
    for (key, value) in defaults {
        if std::env::var_os(key).is_none() {
            // SAFETY: single-threaded at this point; no other thread reads env.
            unsafe { std::env::set_var(key, value) };
        }
    }
}

fn main() -> anyhow::Result<()> {
    let stats_path = std::env::var("BENCH_FIELD_STATS")
        .map_err(|_| anyhow::anyhow!("BENCH_FIELD_STATS must point to field_stats.json"))?;
    let stats: Value = serde_json::from_slice(
        &std::fs::read(&stats_path).map_err(|e| anyhow::anyhow!("read {stats_path}: {e}"))?,
    )
    .map_err(|e| anyhow::anyhow!("parse {stats_path}: {e}"))?;
    let cases = std::env::var("BENCH_CASES").unwrap_or_else(|_| {
        "multi_vpc,multi_apisix,multi_waf,json_apisix,otlp_logs,otlp_traces".to_string()
    });
    let requests = env_usize("BENCH_REQUESTS", 40)?;
    let warmup = env_usize("BENCH_WARMUP", 5)?;
    let mut rng = Rng(env_usize("BENCH_SEED", 1)? as u64 | 1);
    let digest_dir = std::env::var("BENCH_DIGEST_DIR")
        .ok()
        .map(std::path::PathBuf::from);
    if let Some(dir) = &digest_dir {
        std::fs::create_dir_all(dir)
            .map_err(|e| anyhow::anyhow!("create {}: {e}", dir.display()))?;
    }

    let data_dir =
        std::env::temp_dir().join(format!("ingest_handler_bench_{}", std::process::id()));
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| anyhow::anyhow!("create {}: {e}", data_dir.display()))?;
    set_default_env(&data_dir);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("build tokio runtime: {e}"))?;
    let outcome = runtime.block_on(async {
        infra::db_init().await.map_err(|e| anyhow::anyhow!("infra::db_init: {e}"))?;
        infra::init().await.map_err(|e| anyhow::anyhow!("infra::init: {e}"))?;
        println!("case,records_per_req,requests,records,rows_written,us_per_record,allocs_per_record,alloc_bytes_per_record");
        for name in cases.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let name: &'static str = Box::leak(name.to_string().into_boxed_str());
            let case = build_case(name, &stats, &mut rng)?;
            let mut digest = digest_dir.as_ref().map(|_| Vec::new());
            let r = run_case(&case, warmup, requests, digest.as_mut()).await?;
            if let (Some(dir), Some(mut frames)) = (&digest_dir, digest) {
                // frame order follows hash-map iteration; the content must not
                frames.sort();
                let path = dir.join(format!("{}.txt", case.name));
                std::fs::write(&path, frames.concat())
                    .map_err(|e| anyhow::anyhow!("write {}: {e}", path.display()))?;
            }
            let records = r.records.max(1) as f64;
            println!(
                "{},{},{},{},{},{:.2},{:.1},{:.0}",
                case.name,
                case.records_per_request,
                requests,
                r.records,
                r.rows,
                r.elapsed.as_secs_f64() * 1e6 / records,
                r.allocs as f64 / records,
                r.alloc_bytes as f64 / records,
            );
            if r.rows != r.records {
                eprintln!(
                    "WARNING {}: {} rows reached the segment buffer for {} records sent",
                    case.name, r.rows, r.records
                );
            }
        }
        anyhow::Ok(())
    });
    if let Err(e) = std::fs::remove_dir_all(&data_dir) {
        eprintln!("cleanup {}: {e}", data_dir.display());
    }
    outcome
}
