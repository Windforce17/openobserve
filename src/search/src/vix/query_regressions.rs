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
        )),
        expected
    );
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
        false,
        &cancelled,
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
        false,
        &live,
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
            false,
            &operation,
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
        false,
        &operation,
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
