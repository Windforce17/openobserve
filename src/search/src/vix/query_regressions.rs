// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Evaluator refusal is not a SQL scan. These regressions separately exercise
//! direct dispatch, real file-list ownership, and final SQL aggregation of
//! index/scan/Segment-style partials. The native query smoke covers the complete
//! optimizer and storage/Segment-WAL execution pipeline.

use std::{collections::BTreeMap, ops::Range, sync::Arc};

use arrow::{
    array::{Array, Int64Array, RecordBatch, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
};
use bytes::Bytes;
use datafusion::{datasource::MemTable, prelude::SessionContext};
use futures::{FutureExt, future::BoxFuture};
use vortex_index::{VixRangeSource, VixReader, VixWriterOptions, test_support};

use super::*;
use crate::index::Condition;

type Groups = Vec<(i64, String, u64)>;

/// Records actual source requests, not decoded rows or guessed remote traffic.
struct ObservedSource {
    bytes: Bytes,
    reads: parking_lot::Mutex<Vec<Range<u64>>>,
}

impl ObservedSource {
    fn new(bytes: Bytes) -> Arc<Self> {
        Arc::new(Self {
            bytes,
            reads: parking_lot::Mutex::new(Vec::new()),
        })
    }
}

impl VixRangeSource for ObservedSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        self.reads.lock().push(range.clone());
        let result = if range.start <= range.end && range.end <= self.len() {
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        } else {
            Err(anyhow::anyhow!("out-of-bounds fixture read: {range:?}"))
        };
        async move { result }.boxed()
    }
}

/// `ObservedSource` with a simulated per-request latency and issue-time
/// log, so dependent round trips ("waves") of a whole evaluation can be
/// read off: batches issued within one latency of each other overlap.
struct LatencySource {
    bytes: Bytes,
    latency: std::time::Duration,
    started: std::time::Instant,
    log: parking_lot::Mutex<Vec<(f64, Vec<Range<u64>>)>>,
}

impl LatencySource {
    fn new(bytes: Bytes, latency: std::time::Duration) -> Arc<Self> {
        Arc::new(Self {
            bytes,
            latency,
            started: std::time::Instant::now(),
            log: parking_lot::Mutex::new(Vec::new()),
        })
    }

    fn bytes_read(&self) -> u64 {
        self.log
            .lock()
            .iter()
            .flat_map(|(_, ranges)| ranges.iter().map(|r| r.end - r.start))
            .sum()
    }
}

impl VixRangeSource for LatencySource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let many = self.fetch_many(vec![range]);
        async move { Ok(many.await?.remove(0)) }.boxed()
    }

    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        self.log
            .lock()
            .push((self.started.elapsed().as_secs_f64() * 1e3, ranges.clone()));
        let out: Vec<Bytes> = ranges
            .iter()
            .map(|r| self.bytes.slice(r.start as usize..r.end as usize))
            .collect();
        let latency = self.latency;
        let (tx, rx) = futures::channel::oneshot::channel();
        std::thread::spawn(move || {
            std::thread::sleep(latency);
            let _ = tx.send(out);
        });
        async move { Ok(rx.await.expect("latency thread")) }.boxed()
    }
}

/// Diagnostic: the aggregate fast path WITH in-index residual filtering on
/// a real file pair (`VIX_BENCH_FILE` + its `.vxi`), through a 1 ms
/// latency source. Prints the superset size, the exact count and, per
/// phase, the batches / bytes / dependent waves — the per-file cost model
/// a cold production query pays (`VIX_BENCH_QUERY`, `VIX_BENCH_SERVICE`,
/// `VIX_BENCH_BODY`, `VIX_BENCH_FTS` as in `and_io_bench`).
#[test]
#[ignore = "diagnostic; run with VIX_BENCH_FILE set"]
fn prod_file_residual_histogram_cost() {
    let path = std::env::var("VIX_BENCH_FILE").expect("VIX_BENCH_FILE");
    let data = Bytes::from(std::fs::read(&path).unwrap());
    let index =
        Bytes::from(std::fs::read(std::path::Path::new(&path).with_extension("vxi")).unwrap());
    let latency = std::time::Duration::from_millis(1);
    let service = std::env::var("VIX_BENCH_SERVICE")
        .unwrap_or_else(|_| "cfworkers-deploy-cloudrun-worker".to_string());
    let phrase = std::env::var("VIX_BENCH_QUERY")
        .unwrap_or_else(|_| "server_status:DEPLOY_STATUS_SUCCESS".to_string());
    let body =
        std::env::var("VIX_BENCH_BODY").unwrap_or_else(|_| "Sending deploy callback".to_string());
    let fts: Vec<String> = std::env::var("VIX_BENCH_FTS")
        .unwrap_or_else(|_| "body,content,data,error,message".to_string())
        .split(',')
        .map(str::to_string)
        .collect();
    let condition = IndexCondition {
        conditions: vec![
            Condition::MatchAll(phrase),
            Condition::Equal("service_name".to_string(), service),
            Condition::Equal("body".to_string(), body),
        ],
    };
    let memory = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
    let (ts_min, ts_max) = memory
        .zone_chunks()
        .map(|chunks| {
            chunks.iter().fold((i64::MAX, i64::MIN), |(lo, hi), c| {
                (lo.min(c.ts_min), hi.max(c.ts_max))
            })
        })
        .expect("prod files carry a zone map");
    let hour = 3_600_000_000u64;
    let min_value = ts_min - ts_min.rem_euclid(hour as i64);
    let buckets = ((ts_max - min_value) as u64 / hour + 1) as usize;
    let mode = IndexOptimizeMode::SimpleHistogram(min_value, hour, buckets, 0);
    let range = (min_value, min_value + (buckets as i64) * hour as i64);

    let open_and_eval = |rule: Option<IndexOptimizeMode>| {
        let data_src = LatencySource::new(data.clone(), latency);
        let index_src = LatencySource::new(index.clone(), latency);
        let reader = VixReader::open_ranged_with_index(
            data_src.clone() as Arc<dyn VixRangeSource>,
            Some(index_src.clone() as Arc<dyn VixRangeSource>),
        )
        .unwrap();
        let opened_at = data_src.started.elapsed().as_secs_f64() * 1e3;
        let open_batches = data_src.log.lock().len() + index_src.log.lock().len();
        let open_bytes = data_src.bytes_read() + index_src.bytes_read();
        let started = std::time::Instant::now();
        let result = evaluate_vix_index(
            "bench",
            &reader,
            &condition,
            rule,
            range,
            true,
            Some((ts_min, ts_max)),
            None,
            Some(&fts),
        );
        let eval_ms = started.elapsed().as_secs_f64() * 1e3;
        let total_batches = data_src.log.lock().len() + index_src.log.lock().len();
        let total_bytes = data_src.bytes_read() + index_src.bytes_read();
        eprintln!(
            "open: {open_batches} batches, {open_bytes} B, {opened_at:.1} ms | eval: {} batches, {} B, {eval_ms:.1} ms (~{:.1} waves) | reader owned {} KB, gate peak {} KB",
            total_batches - open_batches,
            total_bytes - open_bytes,
            eval_ms / latency.as_secs_f64() / 1e3,
            reader.memory_size() / 1024,
            reader.memory_peak() / 1024,
        );
        for (src, log) in [("data", &data_src.log), ("index", &index_src.log)] {
            for (issued, ranges) in log.lock().iter() {
                let sizes: Vec<String> = ranges
                    .iter()
                    .map(|r| format!("{}+{}", r.start, r.end - r.start))
                    .collect();
                eprintln!("    @{issued:>7.1}ms {src:<5} {}", sizes.join(" "));
            }
        }
        result
    };

    eprintln!("== row-id pass (superset bitmap) ==");
    match open_and_eval(None).unwrap() {
        RawVixResult::Bitmap {
            bitmap,
            has_skipped,
            ..
        } => eprintln!(
            "superset rows={} has_skipped={has_skipped}",
            bitmap.count_set_bits()
        ),
        other => eprintln!("unexpected {other:?}"),
    }
    eprintln!("== aggregate pass (residual-refined histogram) ==");
    match open_and_eval(Some(mode)).unwrap() {
        RawVixResult::Histogram {
            histogram,
            has_skipped,
        } => eprintln!(
            "exact rows={} has_skipped={has_skipped} buckets={buckets} non-empty={:?}",
            histogram.iter().sum::<u64>(),
            histogram
                .iter()
                .enumerate()
                .filter(|(_, c)| **c > 0)
                .collect::<Vec<_>>()
        ),
        other => eprintln!("unexpected {other:?}"),
    }
}

/// Fan-out against the REAL process evaluation gate (`ZO_VIX_EVAL_MAX_BYTES`
/// / `ZO_VIX_SEARCH_CONCURRENCY` as the test process sees them): as many
/// concurrent residual-refined histograms of the A48 shape over
/// `VIX_BENCH_FILE` as the gate admits for the declaration, each a fresh
/// ranged reader with its own 1 ms-latency source. Reports admitted
/// concurrency, wall, growth timeouts and refusals — the `.201` failure
/// mode (111 admitted, each growing past its lease into 500 ms waits)
/// shows up here as timeouts/refusals and a wall far above
/// `rounds × waves × latency`. `VIX_BENCH_FANOUT` overrides the count.
#[test]
#[ignore = "diagnostic; run with VIX_BENCH_FILE set"]
fn prod_file_residual_fanout_under_the_gate() {
    let path = std::env::var("VIX_BENCH_FILE").expect("VIX_BENCH_FILE");
    let data = Bytes::from(std::fs::read(&path).unwrap());
    let index =
        Bytes::from(std::fs::read(std::path::Path::new(&path).with_extension("vxi")).unwrap());
    let latency = std::time::Duration::from_millis(1);
    let service = std::env::var("VIX_BENCH_SERVICE")
        .unwrap_or_else(|_| "cfworkers-deploy-cloudrun-worker".to_string());
    let phrase = std::env::var("VIX_BENCH_QUERY")
        .unwrap_or_else(|_| "server_status:DEPLOY_STATUS_SUCCESS".to_string());
    let body =
        std::env::var("VIX_BENCH_BODY").unwrap_or_else(|_| "Sending deploy callback".to_string());
    let fts: Vec<String> = std::env::var("VIX_BENCH_FTS")
        .unwrap_or_else(|_| "body,content,data,error,message".to_string())
        .split(',')
        .map(str::to_string)
        .collect();
    let condition = Arc::new(IndexCondition {
        conditions: vec![
            Condition::MatchAll(phrase),
            Condition::Equal("service_name".to_string(), service),
            Condition::Equal("body".to_string(), body),
        ],
    });
    let memory = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
    let rows = memory.row_count() as i64;
    let (ts_min, ts_max) = memory
        .zone_chunks()
        .map(|chunks| {
            chunks.iter().fold((i64::MAX, i64::MIN), |(lo, hi), c| {
                (lo.min(c.ts_min), hi.max(c.ts_max))
            })
        })
        .expect("prod files carry a zone map");
    drop(memory);
    let hour = 3_600_000_000u64;
    let min_value = ts_min - ts_min.rem_euclid(hour as i64);
    let buckets = ((ts_max - min_value) as u64 / hour + 1) as usize;
    let mode = IndexOptimizeMode::SimpleHistogram(min_value, hour, buckets, 0);
    let range = (min_value, min_value + (buckets as i64) * hour as i64);
    let declared = evaluation_working_bytes(rows, Some(&mode), true, false, Some((ts_min, ts_max)));
    let budget = source::evaluation_byte_budget();
    let admissible = budget - budget / 8;
    let slots = config::get_config().limit.vix_search_concurrency.max(1);
    let fanout: usize = std::env::var("VIX_BENCH_FANOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or((admissible / declared).min(slots).max(1));
    eprintln!(
        "gate {} MiB (admissible {} MiB), slots {slots}, declared {} KiB -> fan-out {fanout}",
        budget >> 20,
        admissible >> 20,
        declared >> 10
    );
    let timeouts_before = config::metrics::VIX_EVAL_GROWTH_TIMEOUTS_TOTAL
        .with_label_values::<&str>(&[])
        .get();
    let fts = Arc::new(fts);
    let started = std::time::Instant::now();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let outcomes: Vec<(bool, bool, u128, usize)> = rt.block_on(async {
        let mut handles = Vec::with_capacity(fanout);
        for _ in 0..fanout {
            let (data, index, condition, fts, mode) = (
                data.clone(),
                index.clone(),
                Arc::clone(&condition),
                Arc::clone(&fts),
                mode.clone(),
            );
            handles.push(tokio::spawn(async move {
                let operation =
                    source::ReadOperation::new(Arc::new(source::FetchStats::default()), None);
                let queued = std::time::Instant::now();
                let permit = source::acquire_evaluation(&operation, declared)
                    .await
                    .unwrap();
                let waited = queued.elapsed().as_millis();
                let data_src = LatencySource::new(data, latency);
                let index_src = LatencySource::new(index, latency);
                let result = run_evaluation("fanout", &operation, permit, move || {
                    let reader = VixReader::open_ranged_with_index(
                        data_src as Arc<dyn VixRangeSource>,
                        Some(index_src as Arc<dyn VixRangeSource>),
                    )?;
                    let result = evaluate_vix_index(
                        "fanout",
                        &reader,
                        &condition,
                        Some(mode),
                        range,
                        true,
                        Some((ts_min, ts_max)),
                        None,
                        Some(&fts),
                    )?;
                    let exact = matches!(
                        result,
                        RawVixResult::Histogram {
                            has_skipped: false,
                            ..
                        }
                    );
                    anyhow::Ok((exact, reader.memory_peak()))
                })
                .await;
                match result {
                    Ok((exact, peak)) => (true, exact, waited, peak),
                    Err(error) => {
                        eprintln!("evaluation failed: {error:#}");
                        (false, false, waited, 0)
                    }
                }
            }));
        }
        let mut out = Vec::with_capacity(fanout);
        for handle in handles {
            out.push(handle.await.unwrap());
        }
        out
    });
    let wall = started.elapsed();
    let timeouts = config::metrics::VIX_EVAL_GROWTH_TIMEOUTS_TOTAL
        .with_label_values::<&str>(&[])
        .get()
        - timeouts_before;
    let ok = outcomes.iter().filter(|o| o.0).count();
    let exact = outcomes.iter().filter(|o| o.1).count();
    let max_wait = outcomes.iter().map(|o| o.2).max().unwrap_or(0);
    let max_peak = outcomes.iter().map(|o| o.3).max().unwrap_or(0);
    eprintln!(
        "fan-out {fanout}: ok {ok}, exact {exact}, wall {:.0} ms, max admission wait {max_wait} ms, growth timeouts {timeouts}, max reader peak {} KB",
        wall.as_secs_f64() * 1e3,
        max_peak / 1024
    );
    assert_eq!(ok, fanout, "every evaluation must complete");
    assert_eq!(exact, fanout, "every evaluation must be refined exactly");
    assert_eq!(timeouts, 0, "no evaluation may hit the growth wait");
}

fn selected(field: &str, values: &[&str]) -> IndexCondition {
    IndexCondition {
        conditions: vec![Condition::In(
            field.to_owned(),
            values.iter().map(|s| (*s).to_owned()).collect(),
            false,
        )],
    }
}

fn mode(field: &str, min: i64, max: i64, width: u64) -> IndexOptimizeMode {
    IndexOptimizeMode::SimpleMultiHistogram(min, max, width, 0, field.to_owned())
}

fn exact_groups(result: anyhow::Result<RawVixResult>) -> Groups {
    match result.expect("exact aggregate evaluation") {
        RawVixResult::MultiHistogram {
            mut rows,
            has_skipped,
        } => {
            assert!(!has_skipped, "weaker predicates cannot supply final counts");
            rows.sort();
            rows
        }
        _ => panic!("expected exact grouped counts, not a bitmap or scan refusal"),
    }
}

fn requires_scan(result: anyhow::Result<RawVixResult>) {
    match result {
        Ok(RawVixResult::PartialFields | RawVixResult::MissingColumn { .. }) => {}
        Err(error) => assert!(
            requires_exact_scan(&error) || error.is::<crate::index::AllConditionsSkipped>(),
            "expected a semantic scan refusal, not an unrelated error: {error:#}",
        ),
        _ => panic!("uncertain aggregate must refuse, never return incomplete counts or a bitmap"),
    }
}

/// Independent row-wise scan of the actual stored columns. SQL IN excludes
/// NULLs; no NULL group is silently removed from an ALL reference.
fn scan_selected(
    reader: &VixReader,
    field: &str,
    values: &[&str],
    range: (i64, i64),
    min: i64,
    width: i64,
    extra: Option<(&str, &str)>,
) -> Groups {
    let timestamps = reader.read_docs_column("_timestamp").unwrap();
    let timestamps = timestamps.as_any().downcast_ref::<Int64Array>().unwrap();
    let groups = reader.read_docs_column(field).unwrap();
    let groups = arrow::compute::cast(&groups, &DataType::Utf8).unwrap();
    let groups = groups.as_any().downcast_ref::<StringArray>().unwrap();
    let extra_values = extra.map(|(name, _)| {
        let column = reader.read_docs_column(name).unwrap();
        arrow::compute::cast(&column, &DataType::Utf8).unwrap()
    });
    let mut counts = BTreeMap::new();
    for row in 0..timestamps.len() {
        let timestamp = timestamps.value(row);
        if timestamp < range.0
            || timestamp >= range.1
            || groups.is_null(row)
            || !values.contains(&groups.value(row))
        {
            continue;
        }
        if let Some((_, wanted)) = extra {
            let column = extra_values
                .as_ref()
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            if column.is_null(row) || column.value(row) != wanted {
                continue;
            }
        }
        let bucket = min + (timestamp - min).div_euclid(width) * width;
        *counts
            .entry((bucket, groups.value(row).to_owned()))
            .or_insert(0) += 1;
    }
    counts
        .into_iter()
        .map(|((bucket, value), count)| (bucket, value, count))
        .collect()
}

#[test]
fn positive_in_single_bucket_uses_counts_without_docs_or_postings_io() {
    let (data, index) = ranged_parity_tests::build_parity_file_with_plist(1);
    let reference = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
    let expected = scan_selected(
        &reference,
        "svc",
        &["api", "db"],
        (997_000, 1_000_001),
        997_000,
        10_000,
        None,
    );

    // All postings are out-of-line in this fixture. Poison docs and plist,
    // preserving the terms blob's required doc_count metadata. A tail fetch
    // may include payload bytes during open; poisoning also catches cached
    // decode, which post-open source counters alone cannot detect.
    let mut poisoned_data = data.to_vec();
    poisoned_data[test_support::blob_byte_range(&data, "docs").unwrap()].fill(0);
    let mut poisoned_index = index.to_vec();
    let plist = test_support::blob_byte_range(&index, "plist").unwrap();
    poisoned_index[plist.clone()].fill(0);
    let forbidden = plist.start as u64..plist.end as u64;
    let data_source = ObservedSource::new(Bytes::from(poisoned_data));
    let index_source = ObservedSource::new(Bytes::from(poisoned_index));
    let reader =
        VixReader::open_ranged_with_index(data_source.clone(), Some(index_source.clone())).unwrap();
    data_source.reads.lock().clear();
    index_source.reads.lock().clear();
    let answer = exact_groups(evaluate_vix_index(
        "selected-counts-no-payload",
        &reader,
        &selected("svc", &["api", "missing", "api", "db"]),
        Some(mode("svc", 997_000, 1_000_001, 10_000)),
        (997_000, 1_000_001),
        true,
        Some((997_001, 1_000_000)),
        None,
        None,
    ));
    assert_eq!(answer, expected);
    assert!(
        data_source.reads.lock().is_empty(),
        "metadata answer must not fetch docs"
    );
    for read in index_source.reads.lock().iter() {
        assert!(
            read.end <= forbidden.start || read.start >= forbidden.end,
            "metadata answer fetched postings: {read:?}"
        );
    }
}

#[test]
fn cross_bucket_and_partial_windows_take_exact_row_path() {
    let (data, index) = ranged_parity_tests::build_parity_file();
    let reference = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
    for (query, min, max, width, covered, file_bounds) in [
        (
            (997_000, 1_000_001),
            997_000,
            1_000_001,
            1_000,
            true,
            Some((997_001, 1_000_000)),
        ),
        (
            (998_000, 999_501),
            998_000,
            1_000_000,
            1_000,
            false,
            Some((997_001, 1_000_000)),
        ),
        // No actual file bounds: a direct caller cannot authorize the
        // metadata shortcut merely by claiming file_in_range=true.
        ((997_000, 1_000_001), 997_000, 1_000_001, 10_000, true, None),
    ] {
        let data_source = ObservedSource::new(data.clone());
        let index_source = ObservedSource::new(index.clone());
        let reader =
            VixReader::open_ranged_with_index(data_source.clone(), Some(index_source)).unwrap();
        data_source.reads.lock().clear();
        let answer = exact_groups(evaluate_vix_index(
            "selected-counts-boundary",
            &reader,
            &selected("svc", &["api", "db"]),
            Some(mode("svc", min, max, width)),
            query,
            covered,
            file_bounds,
            None,
            None,
        ));
        assert_eq!(
            answer,
            scan_selected(
                &reference,
                "svc",
                &["api", "db"],
                query,
                min,
                width as i64,
                None
            )
        );
        assert!(
            !data_source.reads.lock().is_empty(),
            "non-metadata route must read the real docs object"
        );
    }
}

#[test]
fn extra_conjunct_cannot_reuse_whole_field_selected_counts() {
    let (data, index) = ranged_parity_tests::build_parity_file();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let mut condition = selected("svc", &["api", "db"]);
    condition
        .conditions
        .push(Condition::Equal("svc".to_owned(), "api".to_owned()));
    let range = (997_000, 1_000_001);
    let expected = scan_selected(
        &reader,
        "svc",
        &["api", "db"],
        range,
        997_000,
        10_000,
        Some(("svc", "api")),
    );
    let unfiltered = scan_selected(&reader, "svc", &["api", "db"], range, 997_000, 10_000, None);
    assert_ne!(
        expected, unfiltered,
        "fixture must distinguish selected counts from the complete predicate"
    );
    assert_eq!(
        exact_groups(evaluate_vix_index(
            "selected-counts-extra-predicate",
            &reader,
            &condition,
            Some(mode("svc", 997_000, 1_000_001, 10_000)),
            range,
            true,
            Some((997_001, 1_000_000)),
            None,
            None,
        )),
        expected
    );
}

#[test]
fn different_field_predicate_keeps_exact_groups_on_shifted_and_partial_grids() {
    let (data, index) = ranged_parity_tests::build_parity_file();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let mut condition = selected("svc", &["api"]);
    condition
        .conditions
        .push(Condition::IsNotNull("level".to_owned()));
    for (query, raw_min, raw_max, width, offset, covered) in [
        ((997_000, 1_000_001), 997_000, 1_000_001, 10_000, 17, true),
        ((997_000, 1_000_001), 997_000, 1_000_001, 1_000, 17, true),
        ((998_000, 999_501), 998_000, 1_000_000, 10_000, 17, false),
    ] {
        let mut expected = scan_selected(
            &reader,
            "level",
            &["info", "warn", "error"],
            query,
            raw_min,
            width as i64,
            Some(("svc", "api")),
        );
        for row in &mut expected {
            row.0 += offset;
        }
        assert_eq!(
            exact_groups(evaluate_vix_index(
                "different-field-shifted-grid",
                &reader,
                &condition,
                Some(IndexOptimizeMode::SimpleMultiHistogram(
                    raw_min + offset,
                    raw_max + offset,
                    width,
                    offset,
                    "level".to_owned(),
                )),
                query,
                covered,
                Some((997_001, 1_000_000)),
                None,
                None,
            )),
            expected,
        );
    }
}

#[test]
fn oversize_partial_and_non_string_groups_require_precise_scan() {
    let oversized = "x".repeat(VixWriterOptions::default().max_raw_term_len + 1);
    let reader = review_tests::svc_file(&[Some("short"), Some(&oversized), Some("short")]);
    assert!(reader.field_oversize_skips("svc") > 0);
    // Even the small literal must refuse on an incompletely indexed field;
    // the oversized literal must not turn into an empty bitmap contribution.
    for value in ["short", oversized.as_str()] {
        requires_scan(evaluate_vix_index(
            "oversize-selected-counts",
            &reader,
            &selected("svc", &[value]),
            Some(mode("svc", 990, 1_010, 20)),
            (990, 1_010),
            true,
            Some((998, 1_000)),
            None,
            None,
        ));
    }

    let (data, index) = ranged_parity_tests::build_parity_file();
    // A legacy partial-field declaration must win even though the docs
    // column and key-presence index are available. Keeping an exact
    // IS NOT NULL conjunct exercises the dangerous superset-bitmap route,
    // not merely the trivial all-conditions-skipped refusal.
    let partial_index = test_support::repack_with_partial_fields(&index, &["svc"]).unwrap();
    let partial =
        VixReader::open_with_index(data.clone(), Some(Bytes::from(partial_index))).unwrap();
    assert!(partial.partial_fields().contains("svc"));
    let mut condition = selected("svc", &["api"]);
    condition
        .conditions
        .push(Condition::IsNotNull("svc".to_owned()));
    requires_scan(evaluate_vix_index(
        "partial-selected-counts",
        &partial,
        &condition,
        Some(mode("svc", 997_000, 1_000_001, 10_000)),
        (997_000, 1_000_001),
        true,
        Some((997_001, 1_000_000)),
        None,
        None,
    ));

    let numeric = VixReader::open_with_index(data, Some(index)).unwrap();
    // The condition itself is exact, but stringify-and-group would hide
    // stored type uncertainty from SQL's final aggregate.
    requires_scan(evaluate_vix_index(
        "numeric-group-scan",
        &numeric,
        &selected("svc", &["api"]),
        Some(mode("code", 997_000, 1_000_001, 10_000)),
        (997_000, 1_000_001),
        true,
        Some((997_001, 1_000_000)),
        None,
        None,
    ));
}

async fn stored_reader(file: &FileKey) -> VixReader {
    let data = infra::storage::get(&file.account, &file.key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    VixReader::open(data).unwrap()
}

/// A real final SQL aggregate over partition-local partials, including a
/// separately produced Segment-style partition. This does not instantiate the
/// Segment-WAL manager or claim full native-query coverage.
async fn sum_partials(partitions: Vec<Groups>) -> Groups {
    let schema = Arc::new(Schema::new(vec![
        Field::new("bucket", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("count", DataType::UInt64, false),
    ]));
    let batches = partitions
        .into_iter()
        .map(|rows| {
            let (mut buckets, mut values, mut counts) = (Vec::new(), Vec::new(), Vec::new());
            for (bucket, value, count) in rows {
                buckets.push(bucket);
                values.push(value);
                counts.push(count);
            }
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(buckets)),
                        Arc::new(StringArray::from(values)),
                        Arc::new(UInt64Array::from(counts)),
                    ],
                )
                .unwrap(),
            ]
        })
        .collect();
    let ctx = SessionContext::new();
    ctx.register_table(
        "partials",
        Arc::new(MemTable::try_new(schema, batches).unwrap()),
    )
    .unwrap();
    let batches = ctx.sql("SELECT bucket, value, SUM(count) AS total FROM partials GROUP BY bucket, value ORDER BY bucket, value")
        .await.unwrap().collect().await.unwrap();
    let mut result = Vec::new();
    for batch in batches {
        let buckets = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = arrow::compute::cast(batch.column(1), &DataType::Utf8).unwrap();
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        let counts = arrow::compute::cast(batch.column(2), &DataType::UInt64).unwrap();
        let counts = counts.as_any().downcast_ref::<UInt64Array>().unwrap();
        for row in 0..batch.num_rows() {
            result.push((
                buckets.value(row),
                values.value(row).to_owned(),
                counts.value(row),
            ));
        }
    }
    result
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_index_scan_and_segment_partials_own_each_file_once() {
    let metadata = tests::store_core_file_with_rows(
        "files/org/logs/dispatch-ownership/2026/01/01/00/metadata.vix",
        1_009,
        10,
    )
    .await;
    let indexed = tests::store_core_file_with_rows(
        "files/org/logs/dispatch-ownership/2026/01/01/00/cross-bucket.vix",
        1_004,
        10,
    )
    .await;
    let mut fallback = tests::store_core_file_with_rows(
        "files/org/logs/dispatch-ownership/2026/01/01/00/indexless.vix",
        1_006,
        7,
    )
    .await;
    fallback.meta.index_size = 0;
    let segment = tests::store_core_file_with_rows(
        "files/org/logs/dispatch-ownership/2026/01/01/00/segment-partition.vix",
        1_003,
        4,
    )
    .await;
    let query_range = (990, 1_020);
    let query = Arc::new(crate::types::QueryParams {
        trace_id: "dispatch-ownership".to_owned(),
        org_id: "org".to_owned(),
        stream: datafusion::sql::TableReference::from("t"),
        stream_type: StreamType::Logs,
        stream_name: "t".to_owned(),
        time_range: query_range,
        work_group: None,
        use_inverted_index: true,
        full_text_fields: None,
    });
    let mut files = vec![metadata.clone(), indexed.clone(), fallback.clone()];
    let (_, add_filter_back, result) = vix_search(
        query,
        &mut files,
        Some(selected("level", &["info", "absent", "info"])),
        Some(mode("level", 990, 1_020, 10)),
    )
    .await
    .unwrap();
    assert!(add_filter_back);
    assert_eq!(
        files
            .iter()
            .map(|file| file.key.as_str())
            .collect::<Vec<_>>(),
        vec![fallback.key.as_str()]
    );
    assert!(
        files[0].selection.is_none(),
        "uncertain file must receive a complete scan, not an incomplete selection"
    );
    let indexed_rows = match result {
        MultiResult::MultiHistogram(rows) => rows,
        other => panic!("expected grouped index partials: {other:?}"),
    };
    assert_eq!(
        sum_partials(vec![indexed_rows.clone()]).await,
        vec![(990, "info".to_owned(), 5), (1_000, "info".to_owned(), 15)]
    );

    let fallback_reader = stored_reader(&files[0]).await;
    let segment_reader = stored_reader(&segment).await;
    let scan_rows = scan_selected(
        &fallback_reader,
        "level",
        &["info"],
        query_range,
        990,
        10,
        None,
    );
    let segment_rows = scan_selected(
        &segment_reader,
        "level",
        &["info"],
        query_range,
        990,
        10,
        None,
    );
    let merged = sum_partials(vec![indexed_rows, scan_rows, segment_rows]).await;
    // 10 metadata + 10 indexed + 7 fallback + 4 independently aggregated
    // Segment-style rows. Any file left in both ownership paths overcounts.
    assert_eq!(
        merged,
        vec![(990, "info".to_owned(), 5), (1_000, "info".to_owned(), 26)]
    );
    let mut scan_partitions = Vec::new();
    for file in [&metadata, &indexed, &fallback, &segment] {
        scan_partitions.push(scan_selected(
            &stored_reader(file).await,
            "level",
            &["info"],
            query_range,
            990,
            10,
            None,
        ));
    }
    assert_eq!(merged, sum_partials(scan_partitions).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_file_operation_is_not_a_scan_fallback_or_reader_poison() {
    let file = tests::store_core_file_with_rows(
        "files/org/logs/dispatch-cancel/2026/01/01/00/shared.vix",
        1_009,
        10,
    )
    .await;
    let stats = Arc::new(source::FetchStats::default());
    let cancelled = source::ReadOperation::new(stats.clone(), None);
    cancelled.cancel();
    let error = search_vix_index(
        "dispatch-cancelled",
        (990, 1_020),
        Some(selected("level", &["info"])),
        Some(mode("level", 990, 1_020, 10)),
        &file,
        VixReadMode::Ranged,
        SidecarAccess::check_only(false),
        &cancelled,
        None,
    )
    .await
    .unwrap_err();
    assert!(
        is_cancelled_read(&error),
        "cancellation must terminate, not produce skipped/no-match: {error:#}"
    );
    assert_eq!(stats.fetches.load(std::sync::atomic::Ordering::Relaxed), 0);

    let live = source::ReadOperation::new(Arc::new(source::FetchStats::default()), None);
    let (key, result, skipped) = search_vix_index(
        "dispatch-live",
        (990, 1_020),
        Some(selected("level", &["info"])),
        Some(mode("level", 990, 1_020, 10)),
        &file,
        VixReadMode::Ranged,
        SidecarAccess::check_only(false),
        &live,
        None,
    )
    .await
    .unwrap();
    assert_eq!(key, file.key);
    assert!(!skipped);
    match result {
        VixSearchResult::MultiHistogram(rows) => {
            assert_eq!(rows, vec![(1_000, "info".to_owned(), 10)])
        }
        other => panic!("new operation must still answer the same file: {other:?}"),
    }
}

/// Move only the Puffin footer behind a sparse, unreferenced gap. Blob offsets
/// remain unchanged, and the OS never materializes the logical object's hole.
async fn sparse_sidecar(key: &str) -> (FileKey, SparseFixtureCleanup) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = tests::store_core_file_with_rows(key, 1_009, 10).await;
    let sidecar = config::vix_sidecar_key(key, file.meta.index_generation).unwrap();
    let original = infra::storage::get(&file.account, &sidecar)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    // Puffin ends with payload-size:u32, flags:u32, magic:[u8;4], and its
    // JSON footer begins with another four-byte magic before the payload.
    let trailer = original.len() - 12;
    let payload = u32::from_le_bytes(original[trailer..trailer + 4].try_into().unwrap()) as usize;
    let footer_start = original.len() - (4 + payload + 12);
    let root = std::path::Path::new(&get_config().common.data_stream_dir).to_path_buf();
    let data_path = root.join(key);
    let index_path = root.join(&sidecar);
    let cleanup = SparseFixtureCleanup {
        key: key.to_owned(),
        data_path,
        index_path,
    };
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .open(&cleanup.index_path)
        .unwrap();
    let logical_size = (1u64 << 30) + 4096;
    output.set_len(footer_start as u64).unwrap();
    output.set_len(logical_size).unwrap();
    output
        .seek(SeekFrom::Start(
            logical_size - (original.len() - footer_start) as u64,
        ))
        .unwrap();
    output.write_all(&original[footer_start..]).unwrap();
    drop(output);
    file.meta.index_size = logical_size as i64;
    assert_eq!(
        infra::storage::head(&file.account, &sidecar)
            .await
            .unwrap()
            .size,
        logical_size
    );
    (file, cleanup)
}

struct SparseFixtureCleanup {
    key: String,
    data_path: std::path::PathBuf,
    index_path: std::path::PathBuf,
}

impl Drop for SparseFixtureCleanup {
    fn drop(&mut self) {
        reader_cache::GLOBAL_CACHE.remove(&self.key);
        let _ = std::fs::remove_file(&self.data_path);
        let _ = std::fs::remove_file(&self.index_path);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sparse_gib_sidecar_count_and_topn_keep_exact_optimized_dispatch() {
    use std::sync::atomic::Ordering;
    for topn in [false, true] {
        let key = format!(
            "files/org/logs/sparse-admission/2026/01/01/00/{}.vix",
            if topn { "topn" } else { "count" }
        );
        let (file, _cleanup) = sparse_sidecar(&key).await;
        let rule = if topn {
            IndexOptimizeMode::SimpleTopN(vec!["level".to_owned()], 10, false)
        } else {
            IndexOptimizeMode::SimpleCount
        };
        let condition = if topn {
            IndexCondition {
                conditions: vec![Condition::All()],
            }
        } else {
            selected("level", &["info"])
        };
        assert!(
            (file.meta.index_size as usize).saturating_mul(4) > source::evaluation_byte_budget(),
            "fixture must exceed the old whole-index gate"
        );
        let stats = Arc::new(source::FetchStats::default());
        let operation = source::ReadOperation::new(Arc::clone(&stats), None);
        let (_, answer, skipped) = search_vix_index(
            "sparse-admission",
            (990, 1_020),
            Some(condition.clone()),
            Some(rule.clone()),
            &file,
            VixReadMode::Ranged,
            SidecarAccess::check_only(false),
            &operation,
            None,
        )
        .await
        .unwrap();
        assert!(!skipped);
        if topn {
            assert!(matches!(answer, VixSearchResult::TopN(groups)
                if groups == vec![(vec!["info".to_owned()], 10)]));
        } else {
            assert!(matches!(answer, VixSearchResult::Count(10)));
        }
        let fetched = stats.bytes.load(Ordering::Relaxed);
        assert!(
            fetched > 0 && fetched < 2 * 1024 * 1024,
            "cold metadata answer must read only bounded ranges, read {fetched} bytes"
        );
        assert!(
            stats.physical_bytes.load(Ordering::Relaxed) < 2 * 1024 * 1024,
            "coalescing must not fill the sparse gap"
        );

        // Exercise the real top-level dispatcher too: answered files must be
        // removed from the scan list exactly once, not silently degraded.
        let query = Arc::new(crate::types::QueryParams {
            trace_id: "sparse-vix-search".to_owned(),
            org_id: "org".to_owned(),
            stream: datafusion::sql::TableReference::from("t"),
            stream_type: StreamType::Logs,
            stream_name: "t".to_owned(),
            time_range: (990, 1_020),
            work_group: None,
            use_inverted_index: true,
            full_text_fields: None,
        });
        let mut files = vec![file];
        let (_, add_filter_back, result) =
            vix_search(query, &mut files, Some(condition), Some(rule))
                .await
                .unwrap();
        assert!(!add_filter_back);
        assert!(
            files.is_empty(),
            "optimized answer must not also enter the scan path"
        );
        if topn {
            assert!(matches!(result, MultiResult::TopN(groups)
                if groups == vec![(vec!["info".to_owned()], 10)]));
        } else {
            assert!(matches!(result, MultiResult::Count(10)));
        }
    }
}

/// An unfiltered COUNT over a window-straddling file is answered from the
/// data object alone (row_count + zone table + boundary `_timestamp`
/// chunks): with the sidecar object physically ABSENT the evaluation still
/// returns the exact clamped count, and the file leaves the scan list.
/// Sidecar footer probes were the one remote read a warm count paid on
/// every follower whose disk cache lagged the freshest hour.
#[tokio::test(flavor = "multi_thread")]
async fn unfiltered_straddling_count_never_opens_the_sidecar() {
    use std::sync::atomic::Ordering;
    let key = "files/org/logs/data-only-count/2026/01/01/00/straddle.vix";
    // rows carry _timestamp 1_000..=1_009 (max_ts - i for i in 0..10)
    let file = tests::store_core_file_with_rows(key, 1_009, 10).await;
    let sidecar = config::vix_sidecar_key(key, file.meta.index_generation).unwrap();
    assert!(file.meta.index_size > 0, "fixture must advertise a sidecar");
    infra::storage::del(vec![(file.account.as_str(), sidecar.as_str())])
        .await
        .unwrap();
    assert!(
        infra::storage::head(&file.account, &sidecar).await.is_err(),
        "the sidecar must be gone so any probe fails loudly"
    );
    reader_cache::GLOBAL_CACHE.remove(key);

    let condition = IndexCondition {
        conditions: vec![Condition::All()],
    };
    // [1_004, 1_020) straddles the file's start: rows 1_004..=1_009 = 6
    let stats = Arc::new(source::FetchStats::default());
    let operation = source::ReadOperation::new(Arc::clone(&stats), None);
    let (_, answer, skipped) = search_vix_index(
        "data-only-count",
        (1_004, 1_020),
        Some(condition.clone()),
        Some(IndexOptimizeMode::SimpleCount),
        &file,
        VixReadMode::Ranged,
        SidecarAccess::check_only(false),
        &operation,
        None,
    )
    .await
    .unwrap();
    assert!(!skipped);
    assert!(matches!(answer, VixSearchResult::Count(6)), "{answer:?}");
    assert!(stats.bytes.load(Ordering::Relaxed) > 0);

    let query = Arc::new(crate::types::QueryParams {
        trace_id: "data-only-count-search".to_owned(),
        org_id: "org".to_owned(),
        stream: datafusion::sql::TableReference::from("t"),
        stream_type: StreamType::Logs,
        stream_name: "t".to_owned(),
        time_range: (1_004, 1_020),
        work_group: None,
        use_inverted_index: true,
        full_text_fields: None,
    });
    let mut files = vec![file.clone()];
    let (_, add_filter_back, result) = vix_search(
        query,
        &mut files,
        Some(condition),
        Some(IndexOptimizeMode::SimpleCount),
    )
    .await
    .unwrap();
    assert!(!add_filter_back);
    assert!(
        files.is_empty(),
        "the answered file must not also be scanned"
    );
    assert!(matches!(result, MultiResult::Count(6)), "{result:?}");
    let _ = infra::storage::del(vec![(file.account.as_str(), key)]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cached_sparse_object_refuses_its_real_owner_before_any_fetch() {
    use std::sync::atomic::Ordering;
    let (file, _cleanup) =
        sparse_sidecar("files/org/logs/sparse-admission/2026/01/01/00/cached-too-large.vix").await;
    let stats = Arc::new(source::FetchStats::default());
    let operation = source::ReadOperation::new(Arc::clone(&stats), None);
    let error = search_vix_index(
        "cached-sparse-refusal",
        (990, 1_020),
        Some(selected("level", &["info"])),
        Some(IndexOptimizeMode::SimpleCount),
        &file,
        VixReadMode::Cached,
        SidecarAccess::check_only(false),
        &operation,
        None,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .chain()
            .any(|cause| cause.is::<source::FetchBudgetExceeded>()),
        "whole-object admission must preserve the typed fallback marker: {error:#}"
    );
    assert_eq!(
        stats.fetches.load(Ordering::Relaxed),
        0,
        "reserve data plus sidecar ownership before loading even the first object"
    );
    assert!(
        !operation.is_cancelled(),
        "budget refusal is not query cancellation"
    );
}

fn full_text_scope_fixture() -> (Bytes, Bytes, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("active", DataType::Utf8, false),
        Field::new("historical", DataType::Utf8, false),
        Field::new("raw", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![100, 99, 98, 97, 96, 95, 94])),
            Arc::new(StringArray::from(vec![
                "needle hay",
                "plain",
                "alpha beta",
                "alpha gap beta",
                "beta alpha",
                "alpha",
                "plain",
            ])),
            Arc::new(StringArray::from(vec![
                "plain",
                "needle hay",
                "plain",
                "plain",
                "plain",
                "beta",
                "alpha beta",
            ])),
            Arc::new(StringArray::from(vec!["needle"; 7])),
        ],
    )
    .unwrap();
    let mut writer = vortex_index::VixWriter::new(
        &schema,
        VixWriterOptions {
            fts_field_names: vec!["active".into(), "historical".into()],
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; 7]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    (Bytes::from(data), Bytes::from(index.unwrap()), batch)
}

fn scoped_rows(
    reader: &VixReader,
    condition: &IndexCondition,
    fields: &[String],
) -> (Vec<usize>, bool) {
    match evaluate_vix_index(
        "active-fts-scope",
        reader,
        condition,
        None,
        (90, 110),
        true,
        Some((94, 100)),
        None,
        Some(fields),
    )
    .unwrap()
    {
        RawVixResult::Bitmap {
            bitmap,
            has_skipped,
            ..
        } => (bitmap.set_indices().collect(), has_skipped),
        _ => panic!("expected scoped candidate rows"),
    }
}

#[test]
fn active_full_text_scope_ignores_historical_fields_and_preserves_named_predicates() {
    let (data, index, _) = full_text_scope_fixture();
    // A partial historical field is irrelevant when the current query no
    // longer searches it. Exercise real ranged sources, not reader metadata
    // copied into a pretend source of query scope.
    let partial = test_support::repack_with_partial_fields(&index, &["historical"]).unwrap();
    let reader = VixReader::open_ranged_with_index(
        ObservedSource::new(data),
        Some(ObservedSource::new(Bytes::from(partial))),
    )
    .unwrap();
    let active = vec!["active".to_string()];
    let condition = IndexCondition {
        conditions: vec![Condition::MatchAll("needle".into())],
    };
    assert_eq!(scoped_rows(&reader, &condition, &active), (vec![0], false));
    assert_eq!(scoped_rows(&reader, &condition, &[]), (vec![], false));
    let mixed = IndexCondition {
        conditions: vec![Condition::Or(
            Box::new(Condition::MatchAll("needle".into())),
            Box::new(Condition::Equal("raw".into(), "needle".into())),
        )],
    };
    assert_eq!(
        scoped_rows(&reader, &mixed, &active),
        ((0..7).collect(), false)
    );
    assert_eq!(scoped_rows(&reader, &mixed, &[]), ((0..7).collect(), false));
    let negated = IndexCondition {
        conditions: vec![Condition::And(
            Box::new(Condition::Not(Box::new(Condition::MatchAll(
                "needle".into(),
            )))),
            Box::new(Condition::Equal("raw".into(), "needle".into())),
        )],
    };
    assert_eq!(
        scoped_rows(&reader, &negated, &active),
        ((1..7).collect(), true)
    );
    assert_eq!(
        scoped_rows(&reader, &negated, &[]),
        ((0..7).collect(), true)
    );
    requires_scan(evaluate_vix_index(
        "partial-active-fts",
        &reader,
        &condition,
        Some(IndexOptimizeMode::SimpleCount),
        (90, 110),
        true,
        Some((94, 100)),
        None,
        Some(&["historical".into()]),
    ));
}

#[test]
fn active_full_text_scope_requires_capability_or_exact_absence() {
    let (data, index, _) = full_text_scope_fixture();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let condition = IndexCondition {
        conditions: vec![Condition::MatchAll("needle".into())],
    };
    // FTS-only fields are valid token sources even though raw equality is
    // unservable. Raw-only fields cannot substitute their exact-value terms.
    assert!(!reader.has_term_capability("active"));
    assert!(reader.has_term_capability("raw"));
    assert_eq!(
        scoped_rows(&reader, &condition, &["active".into(), "absent".into()]),
        (vec![0], false),
    );
    assert_eq!(
        scoped_rows(&reader, &condition, &["absent".into()]),
        (vec![], false)
    );
    for fields in [None, Some(vec!["active".into(), "raw".into()])] {
        requires_scan(evaluate_vix_index(
            "unknown-active-fts",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleCount),
            (90, 110),
            true,
            Some((94, 100)),
            None,
            fields.as_deref(),
        ));
    }
    let partial = IndexCondition {
        conditions: vec![
            Condition::Equal("active".into(), "needle hay".into()),
            Condition::MatchAll("needle".into()),
        ],
    };
    assert_eq!(
        scoped_rows(&reader, &partial, &["active".into()]),
        (vec![0], true)
    );
}

#[test]
fn full_text_scope_separates_result_and_bitmap_cache_entries() {
    let (data, index, _) = full_text_scope_fixture();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let file = FileKey {
        key: "files/org/logs/fts-scope/2026/01/01/00/cache.vix".into(),
        meta: config::meta::stream::FileMeta {
            min_ts: 94,
            max_ts: 100,
            records: 7,
            index_size: 4096,
            ..Default::default()
        },
        ..Default::default()
    };
    let condition = IndexCondition {
        conditions: vec![Condition::MatchAll("needle".into())],
    };
    let active = vec!["active".to_string()];
    let historical = vec!["historical".to_string()];
    let cache = vix_result_cache::VixResultCache::new(8);
    for rule in [None, Some(IndexOptimizeMode::SimpleCount)] {
        let key = generate_cache_key(&condition, &rule, &file, None, Some(&active));
        cache.put(key.clone(), CacheEntry::Count(1));
        assert!(cache.get(&key, rule.as_ref()).is_some());
        for scope in [None, Some(&[][..]), Some(historical.as_slice())] {
            let other = generate_cache_key(&condition, &rule, &file, None, scope);
            assert!(cache.get(&other, rule.as_ref()).is_none());
        }
    }
    // Both fields carry the same token but select different documents.
    assert_eq!(scoped_rows(&reader, &condition, &active), (vec![0], false));
    assert_eq!(
        scoped_rows(&reader, &condition, &historical),
        (vec![1], false)
    );
}

/// Item 4 (2026-10-06): a multi-word `match_all` aggregate is a SUPERSET at
/// the token level (reordered, separated and cross-field tokens are only
/// candidates) and used to refuse the native aggregate for the scan
/// branch. It is now refined in the index phase: the candidates' full-text
/// columns are point-read and the same `LIKE` disjunction the scan applies
/// decides them, so the count and histogram are EXACT and the file never
/// reaches the scan. The expected rows come from DataFusion itself.
#[tokio::test(flavor = "multi_thread")]
async fn multiword_full_text_aggregates_are_refined_to_the_exact_residual_rows() {
    let (data, index, batch) = full_text_scope_fixture();
    let data_size = data.len() as i64;
    let index_size = index.len() as i64;
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let scope = vec!["active".to_string(), "historical".to_string()];
    let condition = IndexCondition {
        conditions: vec![Condition::MatchAll("alpha beta".into())],
    };
    let (candidates, skipped) = scoped_rows(&reader, &condition, &scope);
    assert!(skipped, "row-id searches keep the superset contract");
    // Reordered, separated and cross-field tokens are only candidates.
    assert_eq!(candidates, vec![2, 3, 4, 5, 6]);
    match evaluate_vix_index(
        "multiword-aggregate",
        &reader,
        &condition,
        Some(IndexOptimizeMode::SimpleCount),
        (90, 110),
        true,
        Some((94, 100)),
        None,
        Some(&scope),
    )
    .unwrap()
    {
        RawVixResult::Count { count, has_skipped } => {
            assert_eq!(count, 2, "rows 94 and 98 hold the phrase");
            assert!(!has_skipped);
        }
        other => panic!("expected an exact count, got {other:?}"),
    }
    match evaluate_vix_index(
        "multiword-aggregate",
        &reader,
        &condition,
        Some(IndexOptimizeMode::SimpleHistogram(90, 10, 2, 0)),
        (90, 110),
        true,
        Some((94, 100)),
        None,
        Some(&scope),
    )
    .unwrap()
    {
        RawVixResult::Histogram {
            histogram,
            has_skipped,
        } => {
            assert_eq!(histogram, vec![2, 0], "both phrase rows fall in [90, 100)");
            assert!(!has_skipped);
        }
        other => panic!("expected an exact histogram, got {other:?}"),
    }
    let mask = arrow::array::BooleanArray::from(
        (0..batch.num_rows())
            .map(|row| candidates.contains(&row))
            .collect::<Vec<_>>(),
    );
    let selected = arrow::compute::filter_record_batch(&batch, &mask).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table(
        "candidates",
        Arc::new(MemTable::try_new(selected.schema(), vec![vec![selected]]).unwrap()),
    )
    .unwrap();
    let batches = ctx.sql(
        "SELECT _timestamp FROM candidates WHERE active LIKE '%alpha beta%' OR historical LIKE '%alpha beta%' ORDER BY _timestamp",
    ).await.unwrap().collect().await.unwrap();
    let timestamps: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(timestamps, vec![94, 98]);

    // Real dispatch answers the file from the fast path — nothing left for
    // the scan, no filter added back — with DataFusion's own row set.
    let file = FileKey {
        key: "files/org/logs/fts-scope/2026/01/01/00/multiword.vix".into(),
        meta: config::meta::stream::FileMeta {
            min_ts: 94,
            max_ts: 100,
            records: 7,
            compressed_size: data_size,
            index_size,
            ..Default::default()
        },
        ..Default::default()
    };
    reader_cache::GLOBAL_CACHE
        .put(
            reader_cache::ReaderCacheKey::new(file.key.clone(), 0, file.meta.index_size),
            reader,
        )
        .unwrap();
    let params = |fields| {
        Arc::new(crate::types::QueryParams {
            trace_id: "multiword-scope-dispatch".into(),
            org_id: "org".into(),
            stream: datafusion::sql::TableReference::from("fts-scope"),
            stream_type: StreamType::Logs,
            stream_name: "fts-scope".into(),
            time_range: (90, 110),
            work_group: None,
            use_inverted_index: true,
            full_text_fields: Some(fields),
        })
    };
    for rule in [
        IndexOptimizeMode::SimpleCount,
        IndexOptimizeMode::SimpleHistogram(90, 10, 2, 0),
    ] {
        let mut files = vec![file.clone()];
        let (_, filter_back, answer) = vix_search(
            params(scope.clone()),
            &mut files,
            Some(condition.clone()),
            Some(rule),
        )
        .await
        .unwrap();
        assert!(!filter_back, "the refined aggregate is exact");
        assert!(files.is_empty(), "the file is answered, not scanned");
        match answer {
            MultiResult::Count(count) => assert_eq!(count, timestamps.len() as u64),
            MultiResult::Histogram(buckets) => {
                assert_eq!(buckets, vec![timestamps.len() as u64, 0])
            }
            other => panic!("unexpected aggregate answer: {other:?}"),
        }
    }
    // A positive control must reach evaluation, not pass the fallback
    // assertions through missing object metadata or unknown query scope.
    let token = IndexCondition {
        conditions: vec![Condition::MatchAll("needle".into())],
    };
    for (fields, expected) in [(vec!["active".to_string()], 1), (scope, 2)] {
        let mut files = vec![file.clone()];
        let (_, filter_back, answer) = vix_search(
            params(fields),
            &mut files,
            Some(token.clone()),
            Some(IndexOptimizeMode::SimpleCount),
        )
        .await
        .unwrap();
        assert!(!filter_back);
        assert!(files.is_empty());
        match answer {
            MultiResult::Count(count) => assert_eq!(count, expected),
            other => panic!("expected exact scoped token count: {other:?}"),
        }
    }
    reader_cache::GLOBAL_CACHE.remove(&file.key);
}

/// Diagnostic: the `str_match` shape on a REAL `apisix` pair re-indexed
/// with `request.body` as a FULL-TEXT field at the production token cap
/// (`VIX_BENCH_FILE` = the original `.vix`; its `request.uri` /
/// `request.body` / `_timestamp` columns are re-written into a fresh pair
/// with `fts_field_names = [request.body]`, `max_token_len = 64`). Runs
/// `str_match(request.uri, VIX_BENCH_POINT_VALUE) AND
/// str_match_ignore_case(request.body, VIX_BENCH_WALK_NEEDLE)` as a row-id
/// query (token superset, filter re-applied downstream) and as a count
/// (residual-refined, exact), plus the LONE body `str_match` — the case the
/// `.204` walk-vs-verify could not help — printing index batches / bytes /
/// waves per pass through a 20 ms latency source, and the exact rows from an
/// in-memory column scan for comparison — including how many true rows the
/// token superset MISSES (needles inside tokens the cap dropped: the
/// accepted inexactness of full-text search here).
#[test]
#[ignore = "diagnostic; run with VIX_BENCH_FILE set"]
fn prod_file_str_match_on_fts_body_cost() {
    use arrow::array::StringArray;

    let path = std::env::var("VIX_BENCH_FILE").expect("VIX_BENCH_FILE");
    let point_value =
        std::env::var("VIX_BENCH_POINT_VALUE").unwrap_or_else(|_| "thirdparty_webhook".into());
    let needle = std::env::var("VIX_BENCH_WALK_NEEDLE").unwrap_or_else(|_| "asagent1".into());
    let source_data = Bytes::from(std::fs::read(&path).unwrap());
    let source_index =
        Bytes::from(std::fs::read(std::path::Path::new(&path).with_extension("vxi")).unwrap());
    let source = VixReader::open_with_index(source_data, Some(source_index)).unwrap();
    let column = |name: &str| -> Arc<dyn Array> {
        let column = source.read_docs_column(name).unwrap();
        arrow::compute::cast(&column, &DataType::Utf8).unwrap()
    };
    let ts = source.read_docs_column("_timestamp").unwrap();
    let uri = column("request.uri");
    let body = column("request.body");
    let rows = ts.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("request.uri", DataType::Utf8, true),
        Field::new("request.body", DataType::Utf8, true),
    ]));
    let started = std::time::Instant::now();
    let mut writer = vortex_index::VixWriter::new(
        &schema,
        VixWriterOptions {
            fts_field_names: vec!["request.body".to_string()],
            max_token_len: 64,
            ..Default::default()
        },
        false,
    );
    let sources = StringArray::from(vec!["{}"; rows]);
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![ts, uri.clone(), body.clone()]).unwrap();
    writer
        .push_batch_with_source(&batch, &sources, None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    let (data, index) = (Bytes::from(data), Bytes::from(index.unwrap()));
    eprintln!(
        "re-indexed {rows} rows in {:.1} s: data {} MB, sidecar {} MB (source pair {} MB)",
        started.elapsed().as_secs_f64(),
        data.len() / (1 << 20),
        index.len() / (1 << 20),
        source.memory_size() / (1 << 20),
    );
    let memory = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
    assert!(memory.fts_fields().contains("request.body"));
    eprintln!(
        "source raw-value dictionary of request.body: {:?} bytes",
        source.field_dictionary_bytes("request.body"),
    );

    // ground truth from the columns
    let uri_values = uri.as_any().downcast_ref::<StringArray>().unwrap();
    let body_values = body.as_any().downcast_ref::<StringArray>().unwrap();
    let lower_needle = needle.to_lowercase();
    let truth = |want_uri: bool| -> Vec<usize> {
        (0..rows)
            .filter(|&i| {
                (!want_uri
                    || (uri_values.is_valid(i) && uri_values.value(i).contains(&point_value)))
                    && body_values.is_valid(i)
                    && body_values.value(i).to_lowercase().contains(&lower_needle)
            })
            .collect()
    };
    let (ts_min, ts_max) = memory
        .zone_chunks()
        .expect("zone table")
        .iter()
        .fold((i64::MAX, i64::MIN), |(lo, hi), c| {
            (lo.min(c.ts_min), hi.max(c.ts_max))
        });
    let range = (ts_min, ts_max + 1);
    let latency = std::time::Duration::from_millis(20);
    let run = |label: &str,
               condition: &IndexCondition,
               rule: Option<IndexOptimizeMode>,
               expect: &[usize]| {
        let data_src = LatencySource::new(data.clone(), latency);
        let index_src = LatencySource::new(index.clone(), latency);
        let reader = VixReader::open_ranged_with_index(
            data_src.clone() as Arc<dyn VixRangeSource>,
            Some(index_src.clone() as Arc<dyn VixRangeSource>),
        )
        .unwrap();
        let open_batches = data_src.log.lock().len() + index_src.log.lock().len();
        let open_bytes = data_src.bytes_read() + index_src.bytes_read();
        let started = std::time::Instant::now();
        let result = evaluate_vix_index(
            "bench", &reader, condition, rule, range, true, None, None, None,
        );
        let eval_ms = started.elapsed().as_secs_f64() * 1e3;
        let batches = data_src.log.lock().len() + index_src.log.lock().len() - open_batches;
        let bytes = data_src.bytes_read() + index_src.bytes_read() - open_bytes;
        let data_bytes = data_src.bytes_read();
        let outcome = match result {
            // an aggregate over a superset too wide to refine in the index
            // phase falls back to the scan branch — the designed refusal
            Err(error) => format!("fallback: {error}"),
            Ok(RawVixResult::Bitmap {
                bitmap,
                has_skipped,
                ..
            }) => {
                let got: Vec<usize> = bitmap.set_indices().collect();
                let found = expect.iter().filter(|r| got.contains(r)).count();
                format!(
                    "superset rows={} (covers {found} of {} true rows; {} missed inside dropped \
                     tokens) has_skipped={has_skipped}",
                    got.len(),
                    expect.len(),
                    expect.len() - found
                )
            }
            Ok(RawVixResult::Count { count, has_skipped }) => {
                format!(
                    "count={count} (exact {}) has_skipped={has_skipped}",
                    expect.len()
                )
            }
            Ok(other) => format!("unexpected {other:?}"),
        };
        eprintln!(
            "{label:<44} eval {batches:>3} batches {bytes:>11} B ({data_bytes:>9} B data) {eval_ms:>7.1} ms ~{:>4.1} waves | {outcome}",
            eval_ms / latency.as_secs_f64() / 1e3
        );
    };
    let and = IndexCondition {
        conditions: vec![
            Condition::StrMatch("request.uri".to_string(), point_value.clone(), true),
            Condition::StrMatch("request.body".to_string(), needle.clone(), false),
        ],
    };
    let lone = IndexCondition {
        conditions: vec![Condition::StrMatch(
            "request.body".to_string(),
            needle.clone(),
            false,
        )],
    };
    let both = truth(true);
    let body_only = truth(false);
    eprintln!(
        "ground truth: uri+body {} rows, body alone {} rows",
        both.len(),
        body_only.len()
    );
    run("uri AND body: row ids (SELECT)", &and, None, &both);
    run(
        "uri AND body: count (residual exact)",
        &and,
        Some(IndexOptimizeMode::SimpleCount),
        &both,
    );
    run("body alone: row ids (SELECT)", &lone, None, &body_only);
    run(
        "body alone: count (residual exact)",
        &lone,
        Some(IndexOptimizeMode::SimpleCount),
        &body_only,
    );
}
