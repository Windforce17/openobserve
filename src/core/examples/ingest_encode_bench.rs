//! Ingest-encode benchmark: drives `write_core_file_from_tables` — the exact
//! unit of work the ingester's WAL→storage move job runs per group of WAL
//! files — with prod-shaped synthetic trace spans (schema modeled on the live
//! `default` traces stream; settings match prod: fts=[], cs=[duration,
//! service_name, operation_name, span_status], bloom=[trace_id],
//! store_original=false).
//!
//! Env knobs:
//!   BENCH_ROWS    total spans to encode          (default 2_000_000)
//!   BENCH_TABLES  WAL-file-like table providers  (default 4)
//!   BENCH_REPEAT  encode passes over same input  (default 1)
//!
//! Run under perf for the breakdown:
//!   CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release \
//!       -p openobserve-core --example ingest_encode_bench
//!   perf record -F 397 -g --call-graph dwarf,8192 \
//!       ./target/release/examples/ingest_encode_bench

use std::{sync::Arc, time::Instant};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::{catalog::TableProvider, datasource::MemTable};

const TIMESTAMP_COL: &str = "_timestamp";
const BATCH_ROWS: usize = 8192;

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
        self.next() % n
    }

    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }

    fn hex(&mut self, chars: usize) -> String {
        let mut s = String::with_capacity(chars);
        for _ in 0..chars {
            s.push(char::from_digit((self.below(16)) as u32, 16).unwrap());
        }
        s
    }
}

struct Pools {
    services: Vec<String>,
    operations: Vec<String>,
    routes: Vec<String>,
    tables: Vec<String>,
    hosts: Vec<String>,
    versions: Vec<String>,
}

impl Pools {
    fn new(rng: &mut Rng) -> Self {
        let services: Vec<String> = (0..40)
            .map(|i| match i % 4 {
                0 => format!("ai-generate-service-{i}-otel"),
                1 => format!("monica-api-{i}"),
                2 => format!("task-worker-{i}"),
                _ => format!("gateway-edge-{i}"),
            })
            .collect();
        let tables: Vec<String> = (0..60)
            .map(|i| format!("video_summarize_task_{i}"))
            .collect();
        let operations: Vec<String> = (0..300)
            .map(|i| match i % 3 {
                0 => format!("select {}", tables[i % tables.len()]),
                1 => format!("POST /api/v1/resource_{i}"),
                _ => format!("process_task_stage_{i}"),
            })
            .collect();
        let routes: Vec<String> = (0..120)
            .map(|i| format!("/api/v{}/items/{{id}}/part_{i}", 1 + i % 3))
            .collect();
        let hosts: Vec<String> = (0..30)
            .map(|i| format!("svc-{i}.cgvgfzef2scj.us-east-1.internal:3306"))
            .collect();
        let versions: Vec<String> = (0..25)
            .map(|_| format!("vprod-7.{}.{}+{}", rng.below(20), rng.below(99), rng.hex(8)))
            .collect();
        Self {
            services,
            operations,
            routes,
            tables,
            hosts,
            versions,
        }
    }
}

fn traces_schema() -> Arc<Schema> {
    let utf8 = |name: &str| Field::new(name, DataType::Utf8, true);
    Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL, DataType::Int64, false),
        Field::new("start_time", DataType::Int64, true),
        Field::new("end_time", DataType::Int64, true),
        Field::new("duration", DataType::Int64, true),
        Field::new("flags", DataType::Int64, true),
        Field::new("status_code", DataType::Int64, true),
        utf8("trace_id"),
        utf8("span_id"),
        utf8("operation_name"),
        utf8("service_name"),
        utf8("service.name"),
        utf8("span_kind"),
        utf8("span_status"),
        utf8("status_message"),
        utf8("events"),
        utf8("links"),
        utf8("span_duration_nano"),
        utf8("service_env"),
        utf8("service_host.name"),
        utf8("service_pod_name"),
        utf8("service_service.version"),
        utf8("reference.parent_span_id"),
        utf8("reference.parent_trace_id"),
        utf8("reference.ref_type"),
        utf8("infer_service_name"),
        utf8("infer_service_system"),
        utf8("infer_service_type"),
        utf8("db.collection.name"),
        utf8("db.operation.name"),
        utf8("db.query.summary"),
        utf8("db.query.text"),
        utf8("db.rows_affected"),
        utf8("db.system.name"),
        utf8("http.method"),
        utf8("http.route"),
        utf8("http.status_code"),
        utf8("http.url"),
        utf8("server.address"),
        utf8("user_agent.original"),
    ]))
}

/// One batch of prod-shaped spans. Input arrives roughly time-ordered with
/// jitter, like real WAL data (the writer re-sorts DESC via DataFusion —
/// that cost is part of the move job and belongs in the measurement).
#[allow(clippy::too_many_lines)]
fn make_batch(
    schema: &Arc<Schema>,
    pools: &Pools,
    rng: &mut Rng,
    base_ts_us: i64,
    rows: usize,
) -> arrow::record_batch::RecordBatch {
    let mut ts = Vec::with_capacity(rows);
    let mut start = Vec::with_capacity(rows);
    let mut end = Vec::with_capacity(rows);
    let mut dur = Vec::with_capacity(rows);
    let mut flags = Vec::with_capacity(rows);
    let mut status_code: Vec<i64> = Vec::with_capacity(rows);
    let mut trace_id = Vec::with_capacity(rows);
    let mut span_id = Vec::with_capacity(rows);
    let mut op = Vec::with_capacity(rows);
    let mut svc = Vec::with_capacity(rows);
    let mut svc_dotted = Vec::with_capacity(rows);
    let mut kind = Vec::with_capacity(rows);
    let mut span_status = Vec::with_capacity(rows);
    let mut status_msg = Vec::with_capacity(rows);
    let mut events = Vec::with_capacity(rows);
    let mut links = Vec::with_capacity(rows);
    let mut dur_nano = Vec::with_capacity(rows);
    let mut env = Vec::with_capacity(rows);
    let mut host = Vec::with_capacity(rows);
    let mut pod = Vec::with_capacity(rows);
    let mut ver = Vec::with_capacity(rows);
    let mut parent_span: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut parent_trace: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut ref_type: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut infer_name: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut infer_sys: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut infer_type: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut db_coll: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut db_op: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut db_summary: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut db_text: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut db_rows: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut db_sys: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut http_method: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut http_route: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut http_status: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut http_url: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut server_addr: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut user_agent: Vec<Option<String>> = Vec::with_capacity(rows);

    let mut cur_trace = rng.hex(32);
    let mut spans_left_in_trace = 4 + rng.below(12);
    for i in 0..rows {
        if spans_left_in_trace == 0 {
            cur_trace = rng.hex(32);
            spans_left_in_trace = 4 + rng.below(12);
        }
        spans_left_in_trace -= 1;

        // ~1 span/µs stream position with ±30s jitter: roughly ordered, not
        // sorted (matches WAL reality).
        let t = base_ts_us + i as i64 + rng.below(60_000_000) as i64 - 30_000_000;
        let magnitude = 4 + rng.below(13); // log-ish spread: 50µs..~2m
        let d_us = 50 + rng.below(1 << magnitude) as i64;
        ts.push(t);
        start.push(t * 1000 + rng.below(1000) as i64);
        end.push(t * 1000 + d_us * 1000);
        dur.push(d_us);
        flags.push(1);
        let failed = rng.chance(3);
        status_code.push(if failed { 2 } else { 0 });
        trace_id.push(cur_trace.clone());
        span_id.push(rng.hex(16));
        let op_idx =
            (rng.below(pools.operations.len() as u64 * 3) as usize).min(pools.operations.len() - 1); // zipf-ish head reuse
        op.push(pools.operations[op_idx].clone());
        let svc_idx = rng.below(pools.services.len() as u64) as usize;
        svc.push(pools.services[svc_idx].clone());
        svc_dotted.push(pools.services[svc_idx].clone());
        kind.push(rng.below(5).to_string());
        span_status.push(if failed {
            "ERROR".to_string()
        } else {
            "UNSET".to_string()
        });
        status_msg.push(if failed {
            format!(
                "operation failed: upstream timeout after {}ms",
                rng.below(5000)
            )
        } else {
            String::new()
        });
        events.push(if rng.chance(8) {
            format!(
                "[{{\"name\":\"exception\",\"timestamp\":{},\"attributes\":{{\"exception.type\":\"TimeoutError\",\"exception.message\":\"deadline exceeded {}\"}}}}]",
                t, rng.below(10_000)
            )
        } else {
            "[]".to_string()
        });
        links.push("[]".to_string());
        dur_nano.push((d_us * 1000).to_string());
        env.push(if rng.chance(85) { "prod" } else { "dev" }.to_string());
        host.push(format!("generate-service-{}-{}", rng.hex(9), rng.hex(5)));
        pod.push("local".to_string());
        ver.push(pools.versions[rng.below(pools.versions.len() as u64) as usize].clone());

        let has_parent = rng.chance(80);
        parent_span.push(has_parent.then(|| rng.hex(16)));
        parent_trace.push(has_parent.then(|| cur_trace.clone()));
        ref_type.push(has_parent.then(|| "ChildOf".to_string()));

        let is_db = rng.chance(30);
        let is_http = !is_db && rng.chance(55);
        infer_name
            .push(is_db.then(|| pools.hosts[rng.below(pools.hosts.len() as u64) as usize].clone()));
        infer_sys.push(is_db.then(|| "mysql".to_string()));
        infer_type.push(is_db.then(|| "database".to_string()));

        if is_db {
            let tbl = &pools.tables[rng.below(pools.tables.len() as u64) as usize];
            db_coll.push(Some(tbl.clone()));
            db_op.push(Some("select".to_string()));
            db_summary.push(Some(format!("select {tbl}")));
            db_text.push(Some(format!(
                "SELECT * FROM `{tbl}` WHERE (`{tbl}`.`user_id` = ? AND `{tbl}`.`status` IN (?,?,?) AND `{tbl}`.`created_at` > ?) ORDER BY `{tbl}`.`id` DESC LIMIT {}",
                1 + rng.below(100)
            )));
            db_rows.push(Some(rng.below(50).to_string()));
            db_sys.push(Some("mysql".to_string()));
        } else {
            db_coll.push(None);
            db_op.push(None);
            db_summary.push(None);
            db_text.push(None);
            db_rows.push(None);
            db_sys.push(None);
        }

        if is_http {
            let route = &pools.routes[rng.below(pools.routes.len() as u64) as usize];
            http_method.push(Some(
                if rng.chance(60) { "GET" } else { "POST" }.to_string(),
            ));
            http_route.push(Some(route.clone()));
            http_status.push(Some(if failed { "500" } else { "200" }.to_string()));
            http_url.push(Some(format!(
                "https://api.internal{}?req={}&page={}",
                route.replace("{id}", &rng.below(1_000_000).to_string()),
                rng.hex(12),
                rng.below(50)
            )));
            server_addr.push(Some(
                pools.hosts[rng.below(pools.hosts.len() as u64) as usize].clone(),
            ));
            user_agent.push(Some(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36".to_string(),
            ));
        } else {
            http_method.push(None);
            http_route.push(None);
            http_status.push(None);
            http_url.push(None);
            server_addr.push(None);
            user_agent.push(None);
        }
    }

    let str_arr = |v: Vec<String>| Arc::new(StringArray::from(v)) as ArrayRef;
    let opt_arr = |v: Vec<Option<String>>| Arc::new(StringArray::from(v)) as ArrayRef;
    arrow::record_batch::RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(Int64Array::from(start)),
            Arc::new(Int64Array::from(end)),
            Arc::new(Int64Array::from(dur)),
            Arc::new(Int64Array::from(flags)),
            Arc::new(Int64Array::from(status_code)),
            str_arr(trace_id),
            str_arr(span_id),
            str_arr(op),
            str_arr(svc),
            str_arr(svc_dotted),
            str_arr(kind),
            str_arr(span_status),
            str_arr(status_msg),
            str_arr(events),
            str_arr(links),
            str_arr(dur_nano),
            str_arr(env),
            str_arr(host),
            str_arr(pod),
            str_arr(ver),
            opt_arr(parent_span),
            opt_arr(parent_trace),
            opt_arr(ref_type),
            opt_arr(infer_name),
            opt_arr(infer_sys),
            opt_arr(infer_type),
            opt_arr(db_coll),
            opt_arr(db_op),
            opt_arr(db_summary),
            opt_arr(db_text),
            opt_arr(db_rows),
            opt_arr(db_sys),
            opt_arr(http_method),
            opt_arr(http_route),
            opt_arr(http_status),
            opt_arr(http_url),
            opt_arr(server_addr),
            opt_arr(user_agent),
        ],
    )
    .expect("batch construction")
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let rows: usize = std::env::var("BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000_000);
    let n_tables: usize = std::env::var("BENCH_TABLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let repeat: usize = std::env::var("BENCH_REPEAT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    let schema = traces_schema();
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let pools = Pools::new(&mut rng);

    eprintln!("generating {rows} spans across {n_tables} tables ...");
    let gen_start = Instant::now();
    let base_ts_us = 1_785_138_000_000_000_i64; // fixed epoch µs, deterministic
    let rows_per_table = rows / n_tables;
    let mut tables: Vec<Arc<dyn TableProvider>> = Vec::with_capacity(n_tables);
    let mut input_bytes = 0usize;
    for t in 0..n_tables {
        let mut batches = Vec::new();
        let mut left = rows_per_table;
        let mut offset = 0usize;
        while left > 0 {
            let n = left.min(BATCH_ROWS);
            let batch = make_batch(
                &schema,
                &pools,
                &mut rng,
                base_ts_us + (t * rows_per_table + offset) as i64,
                n,
            );
            input_bytes += batch.get_array_memory_size();
            batches.push(batch);
            left -= n;
            offset += n;
        }
        tables.push(Arc::new(MemTable::try_new(schema.clone(), vec![batches])?));
    }
    eprintln!(
        "generated in {:.1}s, arrow memory {:.0} MB",
        gen_start.elapsed().as_secs_f64(),
        input_bytes as f64 / 1e6
    );

    // Prod `default` traces stream settings (fetched live 2026-07-27).
    let fts_fields: Vec<String> = vec![];
    let cs_fields: Vec<String> = ["duration", "service_name", "operation_name", "span_status"]
        .into_iter()
        .map(String::from)
        .collect();
    let bloom_fields: Vec<String> = vec!["trace_id".to_string()];

    for pass in 0..repeat {
        let encode_start = Instant::now();
        let result = openobserve_core::vix::core_writer::write_core_file_from_tables(
            &format!("bench-{pass}"),
            schema.clone(),
            tables.clone(),
            &fts_fields,
            &cs_fields,
            &bloom_fields,
            false,
            0,
        )
        .await?;
        let wall = encode_start.elapsed().as_secs_f64();
        let stats = &result.stats;
        eprintln!(
            "pass {pass}: {} rows in {wall:.2}s = {:.0} rows/s | out {:.1} MB (index {:.1} MB, docs {:.1} MB) | {} terms | {} docs batches",
            stats.row_count,
            stats.row_count as f64 / wall,
            result.data.len() as f64 / 1e6,
            stats.index_size as f64 / 1e6,
            stats.docs_size as f64 / 1e6,
            stats.term_count,
            result.docs_batches,
        );
    }
    Ok(())
}
