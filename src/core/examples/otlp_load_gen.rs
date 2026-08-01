//! OTLP trace load generator for request-path profiling: builds prod-shaped
//! `ExportTraceServiceRequest` protobuf batches (same types the ingest
//! handler decodes) and POSTs them concurrently at a local obs.
//!
//! Env: GEN_URL (default http://127.0.0.1:15080/api/default/v1/traces),
//! GEN_AUTH (basic auth "user:pass"), GEN_SECONDS (60), GEN_CONCURRENCY (6),
//! GEN_SPANS_PER_BATCH (200), GEN_DISTINCT_BATCHES (64).

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span::SpanKind, status::StatusCode},
};
use prost::Message;

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
        self.next() % n
    }

    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| (self.next() & 0xFF) as u8).collect()
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn kv_int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
        ..Default::default()
    }
}

fn build_batch(rng: &mut Rng, spans_per_batch: usize, base_ns: u64) -> Vec<u8> {
    let service = format!("ai-generate-service-{}-otel", rng.below(40));
    let mut spans = Vec::with_capacity(spans_per_batch);
    let mut trace_id = rng.bytes(16);
    let mut left_in_trace = 4 + rng.below(12);
    for i in 0..spans_per_batch {
        if left_in_trace == 0 {
            trace_id = rng.bytes(16);
            left_in_trace = 4 + rng.below(12);
        }
        left_in_trace -= 1;
        let start = base_ns + i as u64 * 1000;
        let magnitude = 16 + rng.below(12);
        let dur_ns = 50_000 + rng.below(1 << magnitude);
        let is_db = rng.below(100) < 30;
        let mut attributes = vec![
            kv("service_env", "prod"),
            kv(
                "service_host.name",
                &format!("generate-service-{:x}", rng.below(1 << 40)),
            ),
            kv("service_pod_name", "local"),
            kv_int("flags", 1),
        ];
        if is_db {
            let table = format!("video_summarize_task_{}", rng.below(60));
            attributes.push(kv("db.system.name", "mysql"));
            attributes.push(kv("db.collection.name", &table));
            attributes.push(kv("db.operation.name", "select"));
            attributes.push(kv("db.query.summary", &format!("select {table}")));
            attributes.push(kv(
                "db.query.text",
                &format!(
                    "SELECT * FROM `{table}` WHERE (`{table}`.`user_id` = ? AND `{table}`.`status` IN (?,?,?)) ORDER BY `{table}`.`id` DESC LIMIT {}",
                    1 + rng.below(100)
                ),
            ));
            attributes.push(kv("db.rows_affected", &rng.below(50).to_string()));
        } else {
            attributes.push(kv(
                "http.method",
                if rng.below(100) < 60 { "GET" } else { "POST" },
            ));
            attributes.push(kv(
                "http.route",
                &format!(
                    "/api/v{}/items/{{id}}/part_{}",
                    1 + rng.below(3),
                    rng.below(120)
                ),
            ));
            attributes.push(kv("http.status_code", "200"));
            attributes.push(kv(
                "http.url",
                &format!(
                    "https://api.internal/api/v1/items/{}/part?req={:x}&page={}",
                    rng.below(1_000_000),
                    rng.below(u64::MAX),
                    rng.below(50)
                ),
            ));
        }
        spans.push(Span {
            trace_id: trace_id.clone(),
            span_id: rng.bytes(8),
            parent_span_id: if rng.below(100) < 80 {
                rng.bytes(8)
            } else {
                vec![]
            },
            name: format!("operation_stage_{}", rng.below(300)),
            kind: SpanKind::Client as i32,
            start_time_unix_nano: start,
            end_time_unix_nano: start + dur_ns,
            attributes,
            status: Some(Status {
                code: StatusCode::Unset as i32,
                message: String::new(),
            }),
            ..Default::default()
        });
    }
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", &service),
                    kv("service.version", "vprod-7.13.73"),
                    kv("telemetry.sdk.language", "python"),
                ],
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
        }],
    };
    request.encode_to_vec()
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let url = std::env::var("GEN_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:15080/api/default/v1/traces".to_string());
    let auth =
        std::env::var("GEN_AUTH").unwrap_or_else(|_| "root@bench.local:Bench12345!".to_string());
    let seconds: u64 = std::env::var("GEN_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let concurrency: usize = std::env::var("GEN_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6);
    let spans_per_batch: usize = std::env::var("GEN_SPANS_PER_BATCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let distinct: usize = std::env::var("GEN_DISTINCT_BATCHES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);

    let base_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
    let mut rng = Rng(0xC0FFEE1234567890);
    let batches: Arc<Vec<Vec<u8>>> = Arc::new(
        (0..distinct)
            .map(|i| build_batch(&mut rng, spans_per_batch, base_ns + i as u64 * 1_000_000))
            .collect(),
    );
    eprintln!(
        "prepared {} distinct batches of {} spans (~{} KB each)",
        distinct,
        spans_per_batch,
        batches[0].len() / 1024
    );

    let sent = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut tasks = Vec::new();
    for worker in 0..concurrency {
        let batches = Arc::clone(&batches);
        let sent = Arc::clone(&sent);
        let errors = Arc::clone(&errors);
        let url = url.clone();
        let auth = auth.clone();
        tasks.push(tokio::spawn(async move {
            let client = reqwest::Client::new();
            let (user, pass) = auth.split_once(':').unwrap();
            let mut i = worker;
            while Instant::now() < deadline {
                let body = batches[i % batches.len()].clone();
                i = i.wrapping_add(1);
                let res = client
                    .post(&url)
                    .basic_auth(user, Some(pass))
                    .header("Content-Type", "application/x-protobuf")
                    .body(body)
                    .send()
                    .await;
                match res {
                    Ok(r) if r.status().is_success() => {
                        sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(r) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        if errors.load(Ordering::Relaxed) < 3 {
                            eprintln!(
                                "HTTP {}: {}",
                                r.status(),
                                r.text().await.unwrap_or_default()
                            );
                        }
                    }
                    Err(e) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        if errors.load(Ordering::Relaxed) < 3 {
                            eprintln!("send error: {e}");
                        }
                    }
                }
            }
        }));
    }
    let start = Instant::now();
    for t in tasks {
        let _ = t.await;
    }
    let wall = start.elapsed().as_secs_f64();
    let ok = sent.load(Ordering::Relaxed);
    let err = errors.load(Ordering::Relaxed);
    println!(
        "GEN_DONE batches_ok={ok} errors={err} spans={} wall={wall:.1}s rate={:.0} spans/s",
        ok * spans_per_batch as u64,
        ok as f64 * spans_per_batch as f64 / wall
    );
    Ok(())
}
