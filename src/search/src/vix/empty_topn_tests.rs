// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, Mutex},
};

use arrow::{array::StringArray, compute::cast, datatypes::DataType};
use bytes::Bytes;
use config::meta::inverted_index::IndexOptimizeMode;
use futures::future::BoxFuture;
use serde_json::json;
use vortex_index::{VixRangeSource, VixReader};

use super::{
    RawVixResult, evaluate_vix_index_with_empty_topn,
    ranged_parity_tests::{build_parity_file, fingerprint},
};
use crate::index::{Condition, IndexCondition};

type Reads = Arc<Mutex<Vec<(&'static str, Range<u64>)>>>;

#[derive(Clone)]
struct CountingSource {
    bytes: Bytes,
    object: &'static str,
    reads: Reads,
}

impl CountingSource {
    fn read(&self, ranges: Vec<Range<u64>>, gap: u64) -> anyhow::Result<Vec<Bytes>> {
        let mut physical = ranges.clone();
        physical.sort_by_key(|range| range.start);
        let mut merged: Vec<Range<u64>> = Vec::new();
        for range in physical {
            if let Some(last) = merged.last_mut()
                && range.start <= last.end.saturating_add(gap.min(1024 * 1024))
            {
                last.end = last.end.max(range.end);
            } else {
                merged.push(range);
            }
        }
        self.reads
            .lock()
            .unwrap()
            .extend(merged.into_iter().map(|range| (self.object, range)));
        ranges
            .into_iter()
            .map(|range| {
                anyhow::ensure!(range.start <= range.end && range.end <= self.len());
                Ok(self.bytes.slice(range.start as usize..range.end as usize))
            })
            .collect()
    }
}

impl VixRangeSource for CountingSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let result = self.read(vec![range], 1024 * 1024);
        Box::pin(async move { Ok(result?.remove(0)) })
    }

    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        self.fetch_many_sparse(ranges, 1024 * 1024)
    }

    fn fetch_many_sparse(
        &self,
        ranges: Vec<Range<u64>>,
        gap: u64,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        let result = self.read(ranges, gap);
        Box::pin(async move { result })
    }
}

fn open(data: &Bytes, index: Option<&Bytes>) -> (VixReader, Reads) {
    let reads = Reads::default();
    let source = |bytes: &Bytes, object| {
        Arc::new(CountingSource {
            bytes: bytes.clone(),
            object,
            reads: reads.clone(),
        }) as Arc<dyn VixRangeSource>
    };
    let reader = VixReader::open_ranged_with_index_sidecar_first(
        source(data, "data"),
        index.map(|index| source(index, "index")),
        true,
    )
    .unwrap();
    (reader, reads)
}

fn eval(
    reader: &VixReader,
    conditions: Vec<Condition>,
    fields: &[&str],
    enabled: bool,
) -> anyhow::Result<RawVixResult> {
    evaluate_vix_index_with_empty_topn(
        "empty-topn-test",
        reader,
        &IndexCondition { conditions },
        Some(IndexOptimizeMode::SimpleTopN(
            fields.iter().map(|field| (*field).to_owned()).collect(),
            10,
            false,
        )),
        (0, i64::MAX),
        true,
        None,
        None,
        Some(&["level".to_owned()]),
        enabled,
    )
}

fn equality(value: &str) -> Vec<Condition> {
    vec![Condition::Equal("svc".into(), value.into())]
}

fn data_reads(reads: &Reads) -> usize {
    reads
        .lock()
        .unwrap()
        .iter()
        .filter(|(object, _)| *object == "data")
        .count()
}

fn sorted_reads(reads: &Reads) -> Vec<(&'static str, u64, u64)> {
    let mut reads: Vec<_> = reads
        .lock()
        .unwrap()
        .iter()
        .map(|(object, range)| (*object, range.start, range.end))
        .collect();
    reads.sort_unstable();
    reads
}

#[test]
fn empty_topn_exact_empty_avoids_data_reads() {
    let (data, index) = build_parity_file();
    for fields in [
        &["svc"][..],
        &["svc", "code"][..],
        &["ghost"][..],
        &["_source"][..],
    ] {
        let (reader, reads) = open(&data, Some(&index));
        assert_eq!(data_reads(&reads), 0);
        let result = eval(&reader, equality("no-such-service"), fields, true).unwrap();
        assert!(
            matches!(result, RawVixResult::TopN { ref groups, has_skipped: false } if groups.is_empty())
        );
        assert_eq!(data_reads(&reads), 0, "{fields:?}");
        let (old_reader, old_reads) = open(&data, Some(&index));
        let old = eval(&old_reader, equality("no-such-service"), fields, false).unwrap();
        assert!(data_reads(&old_reads) > 0 || fields == ["_source"]);
        if fields == ["svc"] || fields == ["svc", "code"] {
            assert_eq!(
                fingerprint(&result).as_bytes(),
                fingerprint(&old).as_bytes()
            );
        } else {
            assert!(matches!(
                old,
                RawVixResult::MissingColumn { .. } | RawVixResult::TopN { .. }
            ));
            // Scanning zero matching input rows also contributes no NULL group.
            let empty = arrow::buffer::BooleanBuffer::new_unset(reader.row_count() as usize);
            assert!(
                super::collect::simple_top_n(
                    &reader,
                    &empty,
                    &fields.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>(),
                    10,
                    false
                )
                .unwrap()
                .is_empty()
            );
        }
    }
}

#[test]
fn empty_topn_nonempty_preserves_results_reads_and_missing_column_fallback() {
    let (data, index) = build_parity_file();
    for fields in [
        &["svc"][..],
        &["level"][..],
        &["svc", "level"][..],
        &["svc", "code"][..],
        &["svc", "ghost"][..],
        &["_source"][..],
    ] {
        let mut results = Vec::new();
        let mut io = Vec::new();
        for enabled in [false, true] {
            let (reader, reads) = open(&data, Some(&index));
            results.push(match eval(&reader, equality("api"), fields, enabled) {
                Ok(result) => fingerprint(&result),
                Err(error) => format!("error:{error:#}"),
            });
            io.push(sorted_reads(&reads));
        }
        assert_eq!(results[0].as_bytes(), results[1].as_bytes(), "{fields:?}");
        // Missing-column fallback may now inspect the filter first, but never scans
        // a different set of rows. Servable nonempty groups read identical ranges.
        if !results[0].starts_with("missing-column:") {
            assert_eq!(io[0], io[1], "{fields:?}");
        }
    }
}

#[test]
fn empty_topn_inexact_and_partial_filters_keep_schema_first() {
    let (data, index) = build_parity_file();
    let partial = Bytes::from(
        vortex_index::test_support::repack_with_partial_fields(&index, &["svc"]).unwrap(),
    );
    let unservable = Bytes::from(
        vortex_index::test_support::repack_dropping_field_term_capability(&index, "svc").unwrap(),
    );
    let oversize = Bytes::from(
        vortex_index::test_support::repack_properties(&data, |properties| {
            let value = json!({"svc": 1}).to_string();
            if let Some(entry) = properties
                .iter_mut()
                .find(|(key, _)| key == "oversize_skips")
            {
                entry.1 = value;
            } else {
                properties.push(("oversize_skips".into(), value));
            }
            Ok(())
        })
        .unwrap(),
    );
    let oversize_index = Bytes::from(
        vortex_index::test_support::repack_properties(&index, |properties| {
            properties.retain(|(key, _)| key != "data_footer_v1");
            Ok(())
        })
        .unwrap(),
    );
    for (data, sidecar, conditions) in [
        (&data, Some(&partial), vec![Condition::IsNull("svc".into())]),
        (&data, Some(&partial), equality("no-such-service")),
        (
            &data,
            Some(&partial),
            vec![
                Condition::Equal("svc".into(), "api".into()),
                Condition::Equal("absent".into(), "value".into()),
            ],
        ),
        (&data, Some(&unservable), equality("no-such-service")),
        (
            &oversize,
            Some(&oversize_index),
            equality("no-such-service"),
        ),
        (&data, None, equality("no-such-service")),
        (
            &data,
            Some(&index),
            vec![Condition::Equal("level".into(), "no-such-token".into())],
        ),
        (
            &data,
            Some(&index),
            vec![Condition::MatchAll("no-such-token".into())],
        ),
        (&data, Some(&index), vec![Condition::All()]),
    ] {
        let mut results = Vec::new();
        let mut io = Vec::new();
        for enabled in [false, true] {
            let (reader, reads) = open(data, sidecar);
            if std::ptr::eq(data, &oversize) {
                assert_eq!(reader.field_oversize_skips("svc"), 1);
            }
            let result = eval(&reader, conditions.clone(), &["level", "ghost"], enabled);
            results.push(match result {
                Ok(result) => fingerprint(&result),
                Err(error) => format!("error:{error:#}"),
            });
            io.push(sorted_reads(&reads));
        }
        assert_eq!(results[0], results[1], "{conditions:?}");
        assert_eq!(io[0], io[1], "{conditions:?}");
        assert!(!results[1].starts_with("topn:"));
    }
}

#[test]
fn empty_topn_conjunction_and_time_clamp_match_old_path() {
    let (data, index) = build_parity_file();
    for conditions in [
        vec![
            Condition::Equal("svc".into(), "api".into()),
            Condition::Equal("svc".into(), "db".into()),
        ],
        vec![Condition::In(
            "svc".into(),
            vec!["api".into(), "db".into()],
            false,
        )],
        vec![Condition::IsNull("svc".into())],
    ] {
        for time_range in [(0, 2_000_000), (999_990, 999_998), (10, 20)] {
            let mut answers = Vec::new();
            for enabled in [false, true] {
                let (reader, _) = open(&data, Some(&index));
                answers.push(fingerprint(
                    &evaluate_vix_index_with_empty_topn(
                        "empty-topn-clamp",
                        &reader,
                        &IndexCondition {
                            conditions: conditions.clone(),
                        },
                        Some(IndexOptimizeMode::SimpleTopN(vec!["svc".into()], 10, false)),
                        time_range,
                        false,
                        None,
                        None,
                        None,
                        enabled,
                    )
                    .unwrap(),
                ));
            }
            assert_eq!(answers[0].as_bytes(), answers[1].as_bytes());
        }
    }
}

#[test]
fn empty_topn_bitmap_admission_is_scoped_and_refusal_is_not_empty() {
    struct Budget {
        limit: usize,
        seen: std::sync::atomic::AtomicUsize,
    }
    impl vortex_index::VixReadOperation for Budget {
        fn is_cancelled(&self) -> bool {
            false
        }
        fn check_memory(&self, bytes: usize) -> vortex_index::Result<()> {
            self.seen.store(bytes, std::sync::atomic::Ordering::Relaxed);
            if bytes > self.limit {
                return Err(vortex_index::VixError::Malformed(
                    "test read budget exceeded".into(),
                ));
            }
            Ok(())
        }
    }
    let (data, index) = build_parity_file();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let bitmap = arrow::buffer::BooleanBuffer::new_set(3000);
    let bytes = bitmap.inner().capacity();
    let base = reader.memory_size();
    let budget = Arc::new(Budget {
        limit: base + bytes,
        seen: Default::default(),
    });
    vortex_index::with_read_operation(budget.clone(), || {
        let retained = reader.retain_query_bitmap(bitmap.clone()).unwrap();
        reader.count(&vortex_index::VixQuery::All).unwrap();
        assert_eq!(
            budget.seen.load(std::sync::atomic::Ordering::Relaxed),
            base + bytes
        );
        assert_eq!(retained.as_ref(), &bitmap);
        drop(retained);
        reader.count(&vortex_index::VixQuery::All).unwrap();
        assert_eq!(budget.seen.load(std::sync::atomic::Ordering::Relaxed), base);
    });
    let too_small = Arc::new(Budget {
        limit: base + bytes - 1,
        seen: Default::default(),
    });
    vortex_index::with_read_operation(too_small, || {
        assert!(reader.retain_query_bitmap(bitmap).is_err());
        assert!(eval(&reader, equality("api"), &["svc"], true).is_err());
    });
}

#[test]
#[ignore = "requires OO_EMPTY_TOPN_SAMPLE_DIR containing local immutable vix/vxi pairs"]
fn empty_topn_real_samples_differential() -> anyhow::Result<()> {
    let dir = std::env::var("OO_EMPTY_TOPN_SAMPLE_DIR")?;
    let mut paths = std::fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "vix")
                && !path.file_name().unwrap().to_string_lossy().starts_with('.')
        })
        .collect::<Vec<_>>();
    paths.sort();
    anyhow::ensure!(paths.len() >= 18);
    let mut records = Vec::new();
    for path in paths.into_iter().take(18) {
        let data = Bytes::from(std::fs::read(&path)?);
        let index = Bytes::from(std::fs::read(path.with_extension("vxi"))?);
        let oracle = VixReader::open_with_index(data.clone(), Some(index.clone()))?;
        let services = cast(&oracle.read_column("service_name")?, &DataType::Utf8)?;
        let services = services.as_any().downcast_ref::<StringArray>().unwrap();
        let group = ["level", "status", "span_kind", "operation_name"]
            .into_iter()
            .find(|field| oracle.field_id(field).is_some() && oracle.has_column_store_field(field))
            .unwrap();
        let groups = cast(&oracle.read_column(group)?, &DataType::Utf8)?;
        let groups = groups.as_any().downcast_ref::<StringArray>().unwrap();
        let mut counts = BTreeMap::<String, usize>::new();
        for value in services.iter().flatten() {
            *counts.entry(value.to_owned()).or_default() += 1;
        }
        let present = [
            "manus-node-server",
            "vida-bizserver",
            "bot-framework-service",
        ]
        .into_iter()
        .find(|value| counts.contains_key(*value))
        .map(str::to_owned)
        .unwrap_or_else(|| {
            counts
                .iter()
                .max_by_key(|(_, count)| *count)
                .unwrap()
                .0
                .clone()
        });
        for needle in [&present, "perf10-absent-20261103"] {
            let mut expected = BTreeMap::<Vec<Option<String>>, u64>::new();
            for (service, group) in services.iter().zip(groups.iter()) {
                if service == Some(needle) {
                    *expected.entry(vec![group.map(str::to_owned)]).or_default() += 1;
                }
            }
            let expected = serde_json::to_vec(&expected.into_iter().collect::<Vec<_>>())?;
            let mut seen = Vec::new();
            for enabled in [false, true] {
                let (reader, reads) = open(&data, Some(&index));
                let result = evaluate_vix_index_with_empty_topn(
                    "empty-topn-samples",
                    &reader,
                    &IndexCondition {
                        conditions: vec![Condition::Equal(
                            "service_name".into(),
                            needle.to_owned(),
                        )],
                    },
                    Some(IndexOptimizeMode::SimpleTopN(
                        vec![group.to_owned()],
                        10,
                        false,
                    )),
                    (0, i64::MAX),
                    true,
                    None,
                    None,
                    None,
                    enabled,
                )?;
                let RawVixResult::TopN {
                    mut groups,
                    has_skipped: false,
                } = result
                else {
                    anyhow::bail!("unexpected result: {result:?}");
                };
                groups.sort();
                let answer = serde_json::to_vec(&groups)?;
                assert_eq!(answer, expected);
                let io = sorted_reads(&reads);
                if enabled && needle == "perf10-absent-20261103" {
                    assert_eq!(data_reads(&reads), 0);
                }
                records.push(json!({"sample":path.file_stem().unwrap().to_str(), "enabled":enabled, "class":if needle == present {"present"} else {"absent"}, "requests":io.len(), "bytes":io.iter().map(|(_, start, end)| end-start).sum::<u64>(), "answer":groups, "reads":io}));
                seen.push(io);
            }
            if needle == present {
                assert_eq!(seen[0], seen[1]);
            }
        }
        println!(
            "{}: old/new/oracle bytes match for present and absent TopN",
            path.display()
        );
    }
    if let Ok(path) = std::env::var("OO_EMPTY_TOPN_SAMPLE_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&records)?)?;
    }
    Ok(())
}

#[tokio::test]
async fn empty_topn_missing_group_scan_semantics() {
    use arrow::{
        array::{ArrayRef, Int64Array, RecordBatch},
        datatypes::{Field, Schema},
    };
    use datafusion::prelude::SessionContext;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("svc", DataType::Utf8, true),
            Field::new("ghost", DataType::Utf8, true),
            Field::new("_timestamp", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![Some("api"), Some("db"), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>; 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1000, 999, 998])) as ArrayRef,
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_batch("logs", batch).unwrap();
    for (needle, rows) in [("no-such-service", 0), ("api", 1)] {
        let batches = ctx
            .sql(&format!(
                "SELECT ghost, count(*) FROM logs WHERE svc = '{needle}' GROUP BY ghost"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            rows
        );
        if rows == 1 {
            assert_eq!(batches[0].column(0).null_count(), 1);
        }
    }
}

#[test]
fn empty_topn_runtime_switch_controls_schema_order() {
    const KEY: &str = "ZO_VIX_TOPN_EMPTY_FILTER_FIRST_ENABLED";
    const CHILD: &str = "OO_EMPTY_TOPN_RUNTIME_SWITCH_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let (data, index) = build_parity_file();
        let (reader, reads) = open(&data, Some(&index));
        let result = super::evaluate_vix_index(
            "empty-topn-runtime-switch",
            &reader,
            &IndexCondition {
                conditions: equality("no-such-service"),
            },
            Some(IndexOptimizeMode::SimpleTopN(vec!["svc".into()], 10, false)),
            (0, i64::MAX),
            true,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(
            matches!(result, RawVixResult::TopN { ref groups, has_skipped: false } if groups.is_empty())
        );
        assert_eq!(
            data_reads(&reads) == 0,
            std::env::var(KEY).unwrap() == "true"
        );
        return;
    }
    for enabled in [false, true] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "vix::empty_topn_tests::empty_topn_runtime_switch_controls_schema_order",
            ])
            .env(CHILD, "1")
            .env(KEY, enabled.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{enabled}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
