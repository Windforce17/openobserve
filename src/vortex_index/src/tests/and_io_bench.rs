// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Throwaway harness: reproduce the per-file IO shape of a prod-style
//! `match_all(...) AND service_name = ...` evaluation on a merged-size file
//! through a latency-simulating ranged source, so serial round trips
//! ("waves") and bytes can be compared before/after `eval_and` changes.
//!
//! Run: `VIX_BENCH_ROWS=1000000 cargo test -p vortex_index --release --lib
//! and_io_bench -- --ignored --nocapture`

use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use arrow::{
    array::{ArrayRef, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use bytes::Bytes;
use futures::future::BoxFuture;
use rand::{RngExt, SeedableRng, rngs::StdRng};

use super::{any_token, bits_to_set, exact};
use crate::{VixQuery, VixRangeSource, VixReader, VixWriter, VixWriterOptions};

/// One `fetch_many` call: which object, which ranges, and when it was
/// issued (ms since the last `reset`) — batches issued within one latency
/// of each other overlap, i.e. share a wave.
type Call = (&'static str, Vec<Range<u64>>, f64);

#[derive(Default)]
struct Counters {
    batches: AtomicUsize,
    ranges: AtomicUsize,
    bytes: AtomicU64,
    physical_bytes: AtomicU64,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    log: parking_lot::Mutex<Vec<Call>>,
    started: parking_lot::Mutex<Option<Instant>>,
}

impl Counters {
    fn snapshot(&self) -> (usize, usize, u64, u64, usize) {
        (
            self.batches.load(Ordering::Acquire),
            self.ranges.load(Ordering::Acquire),
            self.bytes.load(Ordering::Acquire),
            self.physical_bytes.load(Ordering::Acquire),
            self.max_in_flight.load(Ordering::Acquire),
        )
    }
    fn reset(&self) {
        self.batches.store(0, Ordering::Release);
        self.ranges.store(0, Ordering::Release);
        self.bytes.store(0, Ordering::Release);
        self.physical_bytes.store(0, Ordering::Release);
        self.max_in_flight.store(0, Ordering::Release);
        self.log.lock().clear();
        *self.started.lock() = Some(Instant::now());
    }
}

/// Every `fetch_many` call costs one simulated round trip; concurrent calls
/// overlap (each sleeps on its own thread), so wall time / latency ≈ the
/// number of dependent waves.
struct LatencySource {
    name: &'static str,
    bytes: Bytes,
    latency: Duration,
    counters: Arc<Counters>,
}

impl VixRangeSource for LatencySource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let many = self.fetch_many(vec![range]);
        Box::pin(async move { Ok(many.await?.remove(0)) })
    }

    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        // the search ladder's default policy: bridge gaps up to 1 MiB
        self.fetch_many_sparse(ranges, 1 << 20)
    }

    fn fetch_many_sparse(
        &self,
        ranges: Vec<Range<u64>>,
        max_gap: u64,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        let counters = Arc::clone(&self.counters);
        let issued_ms = counters
            .started
            .lock()
            .map(|s| s.elapsed().as_secs_f64() * 1e3)
            .unwrap_or(0.0);
        counters
            .log
            .lock()
            .push((self.name, ranges.clone(), issued_ms));
        counters.batches.fetch_add(1, Ordering::AcqRel);
        counters.ranges.fetch_add(ranges.len(), Ordering::AcqRel);
        counters.bytes.fetch_add(
            ranges.iter().map(|r| r.end - r.start).sum::<u64>(),
            Ordering::AcqRel,
        );
        // physical spans after gap coalescing (what the ladder would GET)
        let mut sorted: Vec<Range<u64>> =
            ranges.iter().filter(|r| !r.is_empty()).cloned().collect();
        sorted.sort_by_key(|r| r.start);
        let mut physical: Vec<Range<u64>> = Vec::new();
        for range in sorted {
            match physical.last_mut() {
                Some(last) if range.start <= last.end + max_gap => {
                    last.end = last.end.max(range.end);
                }
                _ => physical.push(range),
            }
        }
        counters.physical_bytes.fetch_add(
            physical.iter().map(|r| r.end - r.start).sum::<u64>(),
            Ordering::AcqRel,
        );
        let now = counters.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        counters.max_in_flight.fetch_max(now, Ordering::AcqRel);
        let out: Vec<Bytes> = ranges
            .iter()
            .map(|r| self.bytes.slice(r.start as usize..r.end as usize))
            .collect();
        let latency = self.latency;
        let (tx, rx) = futures::channel::oneshot::channel();
        std::thread::spawn(move || {
            std::thread::sleep(latency);
            counters.in_flight.fetch_sub(1, Ordering::AcqRel);
            let _ = tx.send(out);
        });
        Box::pin(async move { Ok(rx.await.expect("latency thread")) })
    }

    fn describe(&self) -> String {
        self.name.to_string()
    }
}

const TARGET_SERVICE: &str = "cfworkers-deploy-cloudrun-worker";

fn build_fixture(rows: usize) -> (Bytes, Bytes) {
    let dir = std::env::temp_dir().join("vix_and_io_bench");
    std::fs::create_dir_all(&dir).unwrap();
    let data_path = dir.join(format!("{rows}.vix"));
    let index_path = dir.join(format!("{rows}.vxi"));
    if let (Ok(data), Ok(index)) = (std::fs::read(&data_path), std::fs::read(&index_path)) {
        return (Bytes::from(data), Bytes::from(index));
    }
    let started = Instant::now();
    let mut rng = StdRng::seed_from_u64(0x5eed);
    let services: Vec<String> = (0..40)
        .map(|i| {
            if i == 5 {
                TARGET_SERVICE.to_string()
            } else {
                format!("svc-{i:02}-worker")
            }
        })
        .collect();
    // zipf-ish service mix; index 5 ≈ 3.9% of rows
    let weights: Vec<f64> = (0..40).map(|i| 1.0 / (i as f64 + 1.0)).collect();
    let total_weight: f64 = weights.iter().sum();
    let filler: Vec<String> = (0..3000).map(|i| format!("w{i:04}x")).collect();
    let pick_service = |rng: &mut StdRng| -> usize {
        let mut r = rng.random::<f64>() * total_weight;
        for (i, w) in weights.iter().enumerate() {
            if r < *w {
                return i;
            }
            r -= w;
        }
        39
    };
    let mut target_rows_left = 6usize;
    let mut rare_rows_left = 3usize;
    let base_ts = 1_790_000_000_000_000i64;
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
    ]));
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            fts_field_names: vec!["body".to_string(), "data".to_string()],
            postings_plist_min_docs: 8192,
            encode_threads: 4,
            ..Default::default()
        },
        false,
    );
    let batch_rows = 65_536;
    let mut emitted = 0usize;
    while emitted < rows {
        let n = batch_rows.min(rows - emitted);
        let mut ts = Vec::with_capacity(n);
        let mut svc = Vec::with_capacity(n);
        let mut body = Vec::with_capacity(n);
        let mut data = Vec::with_capacity(n);
        let mut source = Vec::with_capacity(n);
        for i in 0..n {
            let row = emitted + i;
            ts.push(base_ts - (row as i64) * 1_000);
            let s = pick_service(&mut rng);
            let mut b = String::new();
            let mut d = String::new();
            let is_target_row = s == 5 && target_rows_left > 0 && rng.random::<f64>() < 0.0005;
            let is_rare_row = s == 5 && rare_rows_left > 0 && rng.random::<f64>() < 0.0005;
            if is_target_row {
                target_rows_left -= 1;
                b.push_str("Sending deploy callback");
                d.push_str("server_status:DEPLOY_STATUS_SUCCESS callback=ok");
            } else if is_rare_row {
                rare_rows_left -= 1;
                b.push_str("failed to connect to buildkitd.sock retrying");
                d.push_str("stage=build");
            } else {
                let words = 6 + rng.random_range(0..6);
                for k in 0..words {
                    if k > 0 {
                        b.push(' ');
                    }
                    b.push_str(&filler[rng.random_range(0..filler.len())]);
                }
                if rng.random::<f64>() < 0.35 {
                    b.push_str(" server");
                }
                if rng.random::<f64>() < 0.40 {
                    b.push_str(" status");
                }
                if rng.random::<f64>() < 0.25 {
                    b.push_str(" deploy");
                }
                if rng.random::<f64>() < 0.30 {
                    b.push_str(" success");
                }
                d.push_str("k=");
                d.push_str(&filler[rng.random_range(0..filler.len())]);
                if rng.random::<f64>() < 0.10 {
                    d.push_str(" status=ok");
                }
                if rng.random::<f64>() < 0.05 {
                    d.push_str(" server=a");
                }
            }
            source.push(format!(
                "{{\"_timestamp\":{},\"service_name\":\"{}\",\"body\":\"{}\",\"data\":\"{}\"}}",
                ts[i], services[s], b, d
            ));
            svc.push(services[s].clone());
            body.push(b);
            data.push(d);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(svc)),
            Arc::new(StringArray::from(body)),
            Arc::new(StringArray::from(data)),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        writer
            .push_batch_with_source(&batch, &StringArray::from(source), None)
            .unwrap();
        emitted += n;
    }
    assert_eq!(target_rows_left, 0, "fixture must contain the target rows");
    let (data, index) = writer.finish().unwrap();
    let index = index.unwrap();
    std::fs::write(&data_path, &data).unwrap();
    std::fs::write(&index_path, &index).unwrap();
    eprintln!(
        "built fixture rows={rows} data={} MB index={} MB in {:?}",
        data.len() / (1 << 20),
        index.len() / (1 << 20),
        started.elapsed()
    );
    (Bytes::from(data), Bytes::from(index))
}

/// The stream's FTS field list (`VIX_BENCH_FTS=body,content,...` for a real
/// file; the fixture indexes `body` and `data`).
fn fulltext(query: VixQuery) -> VixQuery {
    let fields = std::env::var("VIX_BENCH_FTS").unwrap_or_else(|_| "body,data".to_string());
    VixQuery::FullText {
        fields: fields.split(',').map(str::to_string).collect(),
        query: Box::new(query),
    }
}

fn three_token_query() -> VixQuery {
    fulltext(VixQuery::And(vec![
        VixQuery::And(vec![
            any_token("deploy"),
            any_token("status"),
            any_token("success"),
        ]),
        exact("service_name", TARGET_SERVICE),
    ]))
}

fn rare_query() -> VixQuery {
    fulltext(VixQuery::And(vec![
        VixQuery::And(vec![any_token("buildkitd"), any_token("sock")]),
        exact("service_name", TARGET_SERVICE),
    ]))
}

fn absent_query() -> VixQuery {
    fulltext(VixQuery::And(vec![
        VixQuery::And(vec![any_token("status"), any_token("zzznotthere")]),
        exact("service_name", TARGET_SERVICE),
    ]))
}

/// What the scan branch pays per file for the residual filter: point-read
/// the candidate rows of the A48 shape (`VIX_BENCH_FILE`) through the
/// latency source and report batches / reads / bytes / waves for the
/// `body` column alone and for the scan's full re-apply projection.
#[test]
#[ignore = "diagnostic; run with VIX_BENCH_FILE set"]
fn candidate_row_point_read_cost() {
    let path = std::env::var("VIX_BENCH_FILE").expect("VIX_BENCH_FILE");
    let data = Bytes::from(std::fs::read(&path).unwrap());
    let index =
        Bytes::from(std::fs::read(std::path::Path::new(&path).with_extension("vxi")).unwrap());
    let latency = Duration::from_millis(1);
    let service = std::env::var("VIX_BENCH_SERVICE").unwrap_or_else(|_| TARGET_SERVICE.to_string());
    let match_all = std::env::var("VIX_BENCH_QUERY")
        .unwrap_or_else(|_| "server_status:DEPLOY_STATUS_SUCCESS".to_string());
    let tokens: Vec<VixQuery> = crate::o2_tokenize(&match_all, 2, 64)
        .map(|t| any_token(&t))
        .collect();
    let query = fulltext(VixQuery::And(vec![
        VixQuery::And(tokens),
        exact("service_name", &service),
    ]));
    let memory = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
    let rows: Vec<u64> = bits_to_set(&memory.eval(&query).unwrap())
        .into_iter()
        .map(u64::from)
        .collect();
    eprintln!(
        "rows={} chunks={:?} candidates={} {:?}",
        memory.row_count(),
        memory.zone_chunks().map(|z| z.len()),
        rows.len(),
        &rows[..rows.len().min(8)]
    );
    let projections: [&[&str]; 3] = [
        &["body"],
        &["_timestamp", "body", "service_name"],
        &[
            "_timestamp",
            "body",
            "content",
            "data",
            "error",
            "message",
            "service_name",
        ],
    ];
    for (label, selected) in [("1 row", &rows[..1]), ("all candidates", &rows[..])] {
        for projection in projections {
            let counters = Arc::new(Counters::default());
            let data_src = Arc::new(LatencySource {
                name: "data",
                bytes: data.clone(),
                latency,
                counters: Arc::clone(&counters),
            });
            let index_src = Arc::new(LatencySource {
                name: "index",
                bytes: index.clone(),
                latency,
                counters: Arc::clone(&counters),
            });
            let reader =
                VixReader::open_ranged_with_index_tail(data_src, Some(index_src), 768 * 1024)
                    .unwrap();
            counters.reset();
            let started = Instant::now();
            let batch = reader.read_docs_columns_rows(projection, selected).unwrap();
            let ms = started.elapsed().as_secs_f64() * 1e3;
            let (b, r, by, _pby, mp) = counters.snapshot();
            eprintln!(
                "{label:<15} cols={:<2} rows={:<4} batches={b:>3} reads={r:>3} bytes={by:>10} ({:.1} MB) waves={:.1} max_par={mp}",
                projection.len(),
                batch.num_rows(),
                by as f64 / 1e6,
                ms / latency.as_secs_f64() / 1e3,
            );
            if std::env::var("VIX_BENCH_LOG").is_ok() {
                for (i, (src, ranges, issued_ms)) in counters.log.lock().iter().enumerate() {
                    let sizes: Vec<String> = ranges
                        .iter()
                        .map(|r| format!("{}+{}", r.start, r.end - r.start))
                        .collect();
                    eprintln!(
                        "    batch {i:>2} @{issued_ms:>6.1}ms {src:<5} {}",
                        sizes.join(" ")
                    );
                }
            }
        }
    }
}

/// `VIX_BENCH_FILE=/path/to/file.vix` measures a real (e.g. prod) file and
/// its `.vxi` sidecar instead of the synthetic fixture; `VIX_BENCH_QUERY`
/// then supplies the match_all text and `VIX_BENCH_SERVICE` the
/// `service_name` value (defaults: the fixture's).
#[test]
#[ignore = "benchmark harness; run explicitly with --ignored --nocapture"]
fn and_io_bench() {
    let rows: usize = std::env::var("VIX_BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let latency = Duration::from_millis(
        std::env::var("VIX_BENCH_LATENCY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20),
    );
    let (data, index) = match std::env::var("VIX_BENCH_FILE") {
        Ok(path) => {
            let data = std::fs::read(&path).expect("VIX_BENCH_FILE readable");
            let sidecar = std::path::Path::new(&path).with_extension("vxi");
            let index = std::fs::read(&sidecar).expect("sidecar .vxi next to VIX_BENCH_FILE");
            (Bytes::from(data), Bytes::from(index))
        }
        Err(_) => build_fixture(rows),
    };
    let memory = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
    eprintln!(
        "rows={} terms={} data={} MB index={} MB latency={:?}",
        memory.row_count(),
        memory.term_count(),
        data.len() / (1 << 20),
        index.len() / (1 << 20),
        latency
    );
    for blob in puffin::reader::parse_puffin_footer_from_bytes(&index)
        .unwrap()
        .blobs
        .iter()
    {
        let range = blob.get_offset(None);
        eprintln!(
            "blob {:<14} {:>10}+{:<10}",
            blob.properties.get("blob_tag").cloned().unwrap_or_default(),
            range.start,
            range.end - range.start
        );
    }
    eprintln!(
        "{:<28} {:>7} {:>7} {:>10} {:>10} {:>8} {:>6} {:>6} {:>8}",
        "query", "batches", "ranges", "bytes", "physical", "ms", "waves", "maxpar", "hits"
    );
    let service = std::env::var("VIX_BENCH_SERVICE").unwrap_or_else(|_| TARGET_SERVICE.to_string());
    let match_all = std::env::var("VIX_BENCH_QUERY")
        .unwrap_or_else(|_| "server_status:DEPLOY_STATUS_SUCCESS".to_string());
    let tokens: Vec<VixQuery> = crate::o2_tokenize(&match_all, 2, 64)
        .map(|t| any_token(&t))
        .collect();
    let svc = exact("service_name", &service);
    // `body = 'Sending deploy callback'` as the query layer now shapes it on
    // an fts-only field: the body-scoped AND of the value's tokens
    let body_value =
        std::env::var("VIX_BENCH_BODY").unwrap_or_else(|_| "Sending deploy callback".to_string());
    let body_tokens: Vec<VixQuery> = crate::o2_tokenize(&body_value, 2, 64)
        .map(|t| any_token(&t))
        .collect();
    let body_superset = VixQuery::FullText {
        fields: vec!["body".to_string()],
        query: Box::new(VixQuery::And(body_tokens)),
    };
    for (name, query) in [
        (
            "match_all+svc+body (prod)",
            fulltext(VixQuery::And(vec![
                VixQuery::And(tokens.clone()),
                svc.clone(),
                body_superset.clone(),
            ])),
        ),
        (
            "match_all+svc",
            fulltext(VixQuery::And(vec![
                VixQuery::And(tokens.clone()),
                svc.clone(),
            ])),
        ),
        (
            "svc+body",
            fulltext(VixQuery::And(vec![svc.clone(), body_superset])),
        ),
        ("match_all only", fulltext(VixQuery::And(tokens.clone()))),
        ("svc only", fulltext(svc.clone())),
        ("3tok+svc", three_token_query()),
        ("rare 2tok+svc", rare_query()),
        ("absent tok+svc", absent_query()),
        ("deploy only", fulltext(any_token("deploy"))),
    ] {
        let expected = bits_to_set(&memory.eval(&query).unwrap());
        let counters = Arc::new(Counters::default());
        let data_src = Arc::new(LatencySource {
            name: "data",
            bytes: data.clone(),
            latency,
            counters: Arc::clone(&counters),
        });
        let index_src = Arc::new(LatencySource {
            name: "index",
            bytes: index.clone(),
            latency,
            counters: Arc::clone(&counters),
        });
        let started = Instant::now();
        // prod runs ZO_VIX_EAGER_TAIL_BYTES=768 KiB (ENGINE-BACKLOG .187)
        let reader =
            VixReader::open_ranged_with_index_tail(data_src, Some(index_src), 768 * 1024).unwrap();
        let open_ms = started.elapsed().as_secs_f64() * 1e3;
        let (b, r, by, pby, mp) = counters.snapshot();
        eprintln!(
            "{:<28} {:>7} {:>7} {:>10} {:>10} {:>8.1} {:>6.1} {:>6} {:>8}",
            format!("{name} [open]"),
            b,
            r,
            by,
            pby,
            open_ms,
            open_ms / latency.as_secs_f64() / 1e3,
            mp,
            ""
        );
        counters.reset();
        let owned_before = reader.memory_size();
        let started = Instant::now();
        let got = bits_to_set(&reader.eval(&query).unwrap());
        let eval_ms = started.elapsed().as_secs_f64() * 1e3;
        let owned_after = reader.memory_size();
        let (b, r, by, pby, mp) = counters.snapshot();
        eprintln!(
            "{:<28} {:>7} {:>7} {:>10} {:>10} {:>8.1} {:>6.1} {:>6} {:>8}  owned {} -> {} (+{} KB)",
            format!("{name} [eval]"),
            b,
            r,
            by,
            pby,
            eval_ms,
            eval_ms / latency.as_secs_f64() / 1e3,
            mp,
            got.len(),
            owned_before / 1024,
            owned_after / 1024,
            owned_after.saturating_sub(owned_before) / 1024
        );
        if std::env::var("VIX_BENCH_LOG").is_ok() {
            for (i, (src, ranges, issued_ms)) in counters.log.lock().iter().enumerate() {
                let sizes: Vec<String> = ranges
                    .iter()
                    .map(|r| format!("{}+{}", r.start, r.end - r.start))
                    .collect();
                eprintln!(
                    "    batch {i:>2} @{issued_ms:>6.1}ms {src:<5} {}",
                    sizes.join(" ")
                );
            }
        }
        assert_eq!(got, expected, "{name}: ranged result must match memory");
    }
}

/// Dump the shape of `VIX_BENCH_FILE`: field table, plist threshold,
/// the most frequent `service_name` values and the doc counts of the
/// tokens named in `VIX_BENCH_QUERY` / `VIX_BENCH_BODY` per FTS field —
/// what `and_io_bench` needs to pick a realistic query for a real file.
#[test]
#[ignore = "diagnostic; run with VIX_BENCH_FILE set"]
fn and_io_probe() {
    use std::collections::BTreeMap;

    let path = std::env::var("VIX_BENCH_FILE").expect("VIX_BENCH_FILE");
    let data = Bytes::from(std::fs::read(&path).unwrap());
    let index =
        Bytes::from(std::fs::read(std::path::Path::new(&path).with_extension("vxi")).unwrap());
    let reader = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
    eprintln!(
        "rows={} terms={} data={} MB index={} MB",
        reader.row_count(),
        reader.term_count(),
        data.len() / (1 << 20),
        index.len() / (1 << 20)
    );
    for blob in puffin::reader::parse_puffin_footer_from_bytes(&index)
        .unwrap()
        .blobs
        .iter()
    {
        let range = blob.get_offset(None);
        eprintln!(
            "blob {:<14} {:>10}+{:<10} {:?}",
            blob.properties.get("blob_tag").cloned().unwrap_or_default(),
            range.start,
            range.end - range.start,
            blob.properties
                .iter()
                .filter(|(k, _)| k.contains("plist") || k.contains("min_docs"))
                .collect::<Vec<_>>()
        );
    }
    let mut fts: Vec<&String> = reader.fts_fields().iter().collect();
    fts.sort();
    eprintln!("fts fields ({}): {:?}", fts.len(), fts);
    let entries = reader.field_entries();
    eprintln!("fields: {}", entries.len());
    let fid_name: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    let service_fid = fid_name.iter().position(|n| *n == "service_name");
    let watch: Vec<String> = {
        let q = std::env::var("VIX_BENCH_QUERY")
            .unwrap_or_else(|_| "server_status:DEPLOY_STATUS_SUCCESS".to_string());
        let b = std::env::var("VIX_BENCH_BODY")
            .unwrap_or_else(|_| "Sending deploy callback".to_string());
        crate::o2_tokenize(&format!("{q} {b}"), 2, 64).collect()
    };
    let mut services: Vec<(u64, String)> = Vec::new();
    let mut tokens: BTreeMap<String, Vec<(String, u64)>> = BTreeMap::new();
    let mut per_field: BTreeMap<u16, (u64, u64)> = BTreeMap::new();
    reader
        .for_each_term(&mut |key, doc_count, _ids| {
            let (token, fid) = crate::query::split_key(key).unwrap();
            let slot = per_field.entry(fid).or_default();
            slot.0 += 1;
            slot.1 += doc_count;
            if Some(fid as usize) == service_fid {
                services.push((doc_count, String::from_utf8_lossy(token).into_owned()));
            }
            if let Ok(text) = std::str::from_utf8(token)
                && watch.iter().any(|w| w == text)
            {
                tokens.entry(text.to_string()).or_default().push((
                    fid_name.get(fid as usize).map_or("?", |v| v).to_string(),
                    doc_count,
                ));
            }
            Ok(())
        })
        .unwrap();
    services.sort_unstable_by(|a, b| b.cmp(a));
    eprintln!("service_name values: {}", services.len());
    for (count, name) in services.iter().take(12) {
        eprintln!("  {count:>9} {name}");
    }
    let mid = services.len() / 2;
    if let Some((count, name)) = services.get(mid) {
        eprintln!("  median: {count} {name}");
    }
    for (token, fields) in &tokens {
        eprintln!("token {token:<12} {fields:?}");
    }
    let mut heavy: Vec<(u64, u16)> = per_field.iter().map(|(f, (_, d))| (*d, *f)).collect();
    heavy.sort_unstable_by(|a, b| b.cmp(a));
    for (docs, fid) in heavy.iter().take(10) {
        eprintln!(
            "field {:<24} terms={:<8} postings_docs={docs}",
            fid_name.get(*fid as usize).map_or("?", |v| v),
            per_field[fid].0
        );
    }
}

/// For every `<name>.vxi.tail` in `VIX_TAIL_DIR` (the last N bytes of a
/// sidecar, paired with the object's full size in `sample.txt` lines
/// `<key> <size>`), report how many trailing bytes a cold open needs
/// tail-resident: the puffin footer, the `dict` blob and the `terms` blob's
/// 256 KiB vortex footer window. Compare against `ZO_VIX_EAGER_TAIL_BYTES`.
#[test]
#[ignore = "diagnostic; run with VIX_TAIL_DIR set"]
fn eager_tail_probe() {
    let dir = std::env::var("VIX_TAIL_DIR").expect("VIX_TAIL_DIR");
    let sizes: std::collections::HashMap<String, u64> =
        std::fs::read_to_string(format!("{dir}/sample.txt"))
            .unwrap()
            .lines()
            .filter_map(|line| {
                let mut parts = line.split_whitespace();
                let key = parts.next()?;
                let size: u64 = parts.next()?.parse().ok()?;
                Some((key.rsplit('/').next()?.to_string(), size))
            })
            .collect();
    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".vxi.tail"))
        .collect();
    entries.sort_by_key(|e| e.file_name());
    eprintln!(
        "{:<30} {:>11} {:>8} {:>8} {:>8} {:>9}",
        "sidecar", "size", "footer", "dict", "fields", "need_tail"
    );
    for entry in entries {
        let name = entry
            .file_name()
            .to_string_lossy()
            .trim_end_matches(".tail")
            .to_string();
        let tail = std::fs::read(entry.path()).unwrap();
        let size = sizes[&name];
        let tail_start = size - tail.len() as u64;
        let meta = puffin::reader::parse_puffin_footer_from_bytes(&tail).unwrap();
        // blob offsets are absolute file offsets; the parser only needs the
        // suffix, so re-base nothing — just read the directory
        let blob = |tag: &str| {
            meta.blobs
                .iter()
                .find(|b| b.properties.get("blob_tag").is_some_and(|t| t == tag))
                .map(|b| b.get_offset(None))
        };
        let terms = blob(crate::container::BLOB_TAG_TERMS).expect("terms blob");
        let dict = blob("dict").expect("dict blob");
        let footer_len = size - dict.end;
        let fields = meta
            .properties
            .get("fields")
            .map(|f| f.matches("\"name\"").count())
            .unwrap_or(0);
        let need = size
            - (terms.end
                - crate::source::VORTEX_FOOTER_INITIAL_READ_BYTES.min(terms.end - terms.start));
        let _ = tail_start;
        eprintln!(
            "{name:<30} {size:>11} {footer_len:>8} {:>8} {fields:>8} {need:>9}{}",
            dict.end - dict.start,
            if need > 768 * 1024 { "  > 768 KiB" } else { "" }
        );
    }
}
