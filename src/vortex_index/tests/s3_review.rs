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

//! S3-review evidence tests (read-only review of the ranged read path).
//!
//! These tests quantify the object-store request/byte profile of
//! [`VixReader::open_ranged`]. History: before lazy dictionary loading
//! (2026-07, the 200M-record benchmark cliff), every ranged open fetched the
//! ENTIRE dict blob — ~33 MiB per real benchmark file, ~22% of the object,
//! re-fetched on every reader-cache miss; ~26 GB per cold query over 1057
//! files. Today an open fetches only the tail + the small dictionary
//! DIRECTORY, and per-row-group FST cells load lazily:
//!
//! - `s3_review_open_is_directory_only_and_fsts_load_lazily` pins the new profile over a synthetic
//!   multi-row-group file;
//! - the real-file battery (`O2_S3_REVIEW_VIX_FILE=<path> cargo test -p vortex_index --test
//!   s3_review -- --ignored --nocapture`) prints per-operation request counts and byte volumes.

use std::{
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use vortex_index::{VixQuery, VixRangeSource, VixReader, VixWriter, VixWriterOptions};

/// A range source over in-memory bytes that counts every fetch, so tests can
/// assert exactly how many object-store GETs (and bytes) an operation costs.
struct CountingSource {
    data: Bytes,
    fetches: AtomicUsize,
    bytes: AtomicU64,
    log: Mutex<Vec<Range<u64>>>,
}

impl CountingSource {
    fn new(data: Bytes) -> Arc<Self> {
        Arc::new(Self {
            data,
            fetches: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
            log: Mutex::new(Vec::new()),
        })
    }

    fn fetch_count(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }

    fn byte_count(&self) -> u64 {
        self.bytes.load(Ordering::SeqCst)
    }

    fn take_log(&self) -> Vec<Range<u64>> {
        std::mem::take(&mut self.log.lock().unwrap())
    }
}

impl VixRangeSource for CountingSource {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        self.bytes
            .fetch_add(range.end - range.start, Ordering::SeqCst);
        self.log.lock().unwrap().push(range.clone());
        let out = if range.start <= range.end && range.end <= self.data.len() as u64 {
            Ok(self.data.slice(range.start as usize..range.end as usize))
        } else {
            Err(anyhow::anyhow!("range {range:?} out of bounds"))
        };
        async move { out }.boxed()
    }

    fn describe(&self) -> String {
        "s3-review counting source".to_string()
    }
}

/// Build a synthetic core file whose dictionary spans MANY row groups (tiny
/// `rg_term_bytes`) and comfortably exceeds the 64 KiB footer tail window,
/// so directory-vs-FST fetch behavior is observable.
fn build_multi_rg_file() -> (Bytes, Bytes) {
    let rows = 20_000usize;
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
    ]));
    let ts: Vec<i64> = (0..rows as i64).map(|i| 1_000_000 - i).collect();
    // one unique value per row: 20k raw terms of ~26 bytes each
    let svc: Vec<String> = (0..rows).map(|i| format!("svc-{i:08}-abcdefgh")).collect();
    let sources: Vec<String> = (0..rows)
        .map(|i| format!(r#"{{"_timestamp":{},"svc":"{}"}}"#, ts[i], svc[i]))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(
                svc.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let opts = VixWriterOptions {
        // ~26-byte terms with a 4 KiB row-group budget: ~150 terms per row
        // group, >100 row groups
        ..Default::default()
    };
    let mut writer = VixWriter::new(&schema, opts, false);
    writer
        .push_batch_with_source(
            &batch,
            &StringArray::from(sources.iter().map(String::as_str).collect::<Vec<_>>()),
            None,
        )
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    (
        Bytes::from(data),
        Bytes::from(index.expect("indexed fixture has a sidecar")),
    )
}

/// The post-fix open/eval profile: a ranged open loads only the small
/// dictionary DIRECTORY (never the FST cells), an exact-term probe loads
/// exactly the ONE row-group FST the directory prunes to, a second probe in
/// the same row group is FST-free, and loaded cells grow
/// `VixReader::memory_size` (external caches re-read it). A re-open over a
/// fresh source pays the same small open again — the parsed-reader cache
/// above this layer is the only persistence, by design.
#[test]
fn s3_review_open_is_directory_only_and_fsts_load_lazily() {
    let (data, index) = build_multi_rg_file();
    let file_len = (data.len() + index.len()) as u64;
    assert!(
        index.len() as u64 > 2 * 64 * 1024,
        "the sidecar must exceed the tail window for laziness to be observable"
    );

    let dsource = CountingSource::new(data.clone());
    let isource = CountingSource::new(index.clone());
    let fetch_count = || dsource.fetch_count() + isource.fetch_count();
    let byte_count = || dsource.byte_count() + isource.byte_count();
    let reader = VixReader::open_ranged_with_index(
        Arc::clone(&dsource) as Arc<dyn VixRangeSource>,
        Some(Arc::clone(&isource) as Arc<dyn VixRangeSource>),
    )
    .unwrap();
    // open is FOOTER-ONLY (one tail per object): the dictionary index
    // parses lazily on the first dictionary touch, blocks fetch per lookup
    let open_fetches = fetch_count();
    let open_bytes = byte_count();
    assert!(
        open_bytes < 2 * (64 * 1024) + 96 * 1024,
        "open must fetch only the two tail windows: {open_bytes} bytes"
    );
    assert!(
        reader.term_row_group_count() > 20,
        "fixture must span many dictionary blocks, got {}",
        reader.term_row_group_count()
    );
    let open_memory = reader.memory_size();
    dsource.take_log();
    isource.take_log();

    // Cold exact-term probe: the directory prunes to ONE row group; exactly
    // one FST cell (a few KiB) is fetched, never the whole dict column.
    let probe = |needle: &str| VixQuery::Exact {
        field: "svc".to_string(),
        token: needle.as_bytes().to_vec(),
    };
    let bitmap = reader.eval(&probe("svc-00010000-abcdefgh")).unwrap();
    assert_eq!(bitmap.count_set_bits(), 1);
    let cold_fetches = fetch_count() - open_fetches;
    let cold_bytes = byte_count() - open_bytes;
    // one FST cell + the terms-blob footer + one doc_count/postings chunk
    // (plus read coalescing) — bounded, and far below a whole-dict load
    // (asserted against the full walk below)
    assert!(
        cold_bytes < 512 * 1024,
        "a needle probe must load ~one FST cell + one postings chunk, moved {cold_bytes} bytes"
    );
    let after_first_probe = reader.memory_size();
    assert!(
        after_first_probe > open_memory,
        "a loaded FST cell must grow memory_size ({open_memory} -> {after_first_probe})"
    );

    // A second needle in the SAME row group: the FST is resident, only the
    // postings point read remains.
    let (f0, b0) = (fetch_count(), byte_count());
    let bitmap = reader.eval(&probe("svc-00010001-abcdefgh")).unwrap();
    assert_eq!(bitmap.count_set_bits(), 1);
    let hot_fetches = fetch_count() - f0;
    let hot_bytes = byte_count() - b0;
    assert_eq!(
        reader.memory_size(),
        after_first_probe,
        "a probe in a resident row group must not load another FST"
    );
    // (the memory_size equality above is the FST-free proof; the fetch is
    // the ~128 KiB postings chunk of the second needle)
    assert!(
        hot_fetches <= 1 && hot_bytes < 512 * 1024,
        "same-row-group probe must be FST-free: {hot_fetches} fetches / {hot_bytes} bytes"
    );

    // A needle in a DIFFERENT row group loads exactly one more cell.
    let (_f1, b1) = (fetch_count(), byte_count());
    let bitmap = reader.eval(&probe("svc-00019999-abcdefgh")).unwrap();
    assert_eq!(bitmap.count_set_bits(), 1);
    assert!(
        reader.memory_size() > after_first_probe,
        "a probe in a new row group must load its FST cell"
    );
    assert!(byte_count() - b1 < 256 * 1024);

    // Full-dictionary walk (Contains = scan_all_tokens): loads EVERY
    // remaining FST cell — the needle probes above must have cost a small
    // fraction of this (the lazy-loading win).
    let (_f2, b2) = (fetch_count(), byte_count());
    let ordinal_bitmap = reader
        .eval(&VixQuery::Contains {
            field: Some("svc".to_string()),
            needle: b"-00000042-".to_vec(),
            case_insensitive: false,
        })
        .unwrap();
    assert_eq!(ordinal_bitmap.count_set_bits(), 1);
    let full_walk_bytes = byte_count() - b2;
    let full_memory = reader.memory_size();
    assert!(full_memory > after_first_probe);
    // resident-FST accounting is the honest lazy metric (fetch bytes mix in
    // terms-blob reads and read coalescing): one probe loads ~1 cell of the
    // >100-cell dictionary
    let one_cell = after_first_probe - open_memory;
    let all_cells = full_memory - open_memory;
    assert!(
        one_cell * 10 <= all_cells,
        "one lazily loaded FST cell ({one_cell}B) must be a small fraction of the whole \
         dictionary ({all_cells}B resident after the full walk)"
    );

    // Re-open over fresh sources: the same small footer-only open (no
    // layer below the in-process reader cache persists parsed state).
    let dsource2 = CountingSource::new(data);
    let isource2 = CountingSource::new(index);
    let _reader2 = VixReader::open_ranged_with_index(
        Arc::clone(&dsource2) as Arc<dyn VixRangeSource>,
        Some(Arc::clone(&isource2) as Arc<dyn VixRangeSource>),
    )
    .unwrap();
    assert_eq!(dsource2.fetch_count() + isource2.fetch_count(), open_fetches);
    assert_eq!(dsource2.byte_count() + isource2.byte_count(), open_bytes);

    println!(
        "s3_review synthetic (lazy): file={file_len}B open={open_fetches} fetches/{open_bytes}B, \
         cold needle=+{cold_fetches}/{cold_bytes}B, same-RG needle=+{hot_fetches}/{hot_bytes}B, \
         full dict walk={full_walk_bytes}B, memory {open_memory}->{after_first_probe}->\
         {full_memory}B"
    );
}

/// Fetch profile of a REAL benchmark core file (run with
/// `O2_S3_REVIEW_VIX_FILE=/path/to/file.vix cargo test -p vortex_index
/// --test s3_review -- --ignored --nocapture`). Prints per-operation
/// object-store request counts and byte volumes; each fetch would be one S3
/// ranged GET on a cold cache.
#[test]
#[ignore = "needs O2_S3_REVIEW_VIX_FILE pointing at a real .vix file"]
fn s3_review_real_file_fetch_profile() {
    let path = std::env::var("O2_S3_REVIEW_VIX_FILE")
        .expect("set O2_S3_REVIEW_VIX_FILE to a real .vix file");
    let data = Bytes::from(std::fs::read(&path).unwrap());
    // the sidecar sits next to the data object (extension swapped), or
    // wherever O2_S3_REVIEW_VXI_FILE points; absent = index-off file
    let index_path = std::env::var("O2_S3_REVIEW_VXI_FILE")
        .unwrap_or_else(|_| path.trim_end_matches(".vix").to_string() + ".vxi");
    let index = std::fs::read(&index_path).ok().map(Bytes::from);
    let total_len = (data.len() + index.as_ref().map_or(0, |b| b.len())) as u64;
    let source = CountingSource::new(data);
    let index_source = index.map(CountingSource::new);

    let mut phase_start = (0usize, 0u64);
    let index_for_phase = index_source.clone();
    let mut phase = |label: &str, source: &CountingSource| {
        let (mut fetches, mut bytes) = (source.fetch_count(), source.byte_count());
        let mut ranges = source.take_log();
        if let Some(isrc) = &index_for_phase {
            fetches += isrc.fetch_count();
            bytes += isrc.byte_count();
            ranges.extend(isrc.take_log());
        }
        let (df, db) = (fetches - phase_start.0, bytes - phase_start.1);
        println!(
            "s3_review real: {label:<38} +{df:>3} fetches, +{db:>12} bytes ({:.2}% of file) {}",
            db as f64 / total_len as f64 * 100.0,
            if ranges.len() <= 6 {
                format!("{ranges:?}")
            } else {
                format!("[{} ranges]", ranges.len())
            }
        );
        phase_start = (fetches, bytes);
        (df, db)
    };

    let open_start = std::time::Instant::now();
    let reader = VixReader::open_ranged_with_index(
        Arc::clone(&source) as Arc<dyn VixRangeSource>,
        index_source
            .as_ref()
            .map(|s| Arc::clone(s) as Arc<dyn VixRangeSource>),
    )
    .unwrap();
    let open_elapsed = open_start.elapsed();
    println!(
        "s3_review real: file {} = {} bytes, {} rows, {} terms, {} term row-groups, \
         parsed reader memory {} bytes, open wall {:?}",
        path,
        total_len,
        reader.row_count(),
        reader.term_count(),
        reader.term_row_group_count(),
        reader.memory_size(),
        open_elapsed,
    );
    let (open_fetches, open_bytes) = phase("open (tail + dict directory)", &source);
    // Lazy dict loading: open fetches the tail plus the small directory
    // columns — NEVER the FST cells. (Pre-fix this was tail + the whole
    // ~33 MiB dict blob, the 200M-benchmark cold-query killer.)
    assert!(open_fetches <= 8, "open took {open_fetches} fetches");
    assert!(
        open_bytes < 4 * 1024 * 1024,
        "open must not fetch the whole dict: {open_bytes} bytes"
    );

    // needle-style probe FIRST (before any full-dictionary walk): the
    // directory prunes an exact key lookup to one row group — one FST cell
    // (or, on pre-lazy files, the one big fst chunk) + one doc_count point
    // read: the shape of every exact-term / count fast-path per-file eval.
    // `_timestamp` is never key-termed; default to the ubiquitous `level`
    // (override with O2_S3_REVIEW_NEEDLE_KEY for non-log files).
    let needle_key =
        std::env::var("O2_S3_REVIEW_NEEDLE_KEY").unwrap_or_else(|_| "level".to_string());
    let count = reader
        .count(&VixQuery::KeyExists {
            path: needle_key.clone(),
        })
        .unwrap();
    phase(
        &format!("count(KeyExists({needle_key:?}))={count}: needle"),
        &source,
    );

    let bitmap = reader
        .eval(&VixQuery::KeyExists {
            path: needle_key.clone(),
        })
        .unwrap();
    assert_eq!(bitmap.count_set_bits() as u64, count);
    phase(&format!("KeyExists({needle_key:?}) postings"), &source);

    // full-dictionary walk: loads EVERY remaining FST cell (inherent to
    // whole-key enumeration; per-cell on lazy-layout files, the one big
    // chunk on pre-lazy files)
    let keys = reader.keys_with_prefix("").unwrap();
    assert!(!keys.is_empty());
    phase("keys_with_prefix(\"\") FULL dict walk", &source);
    let dense_path = &keys.iter().max_by_key(|(_, count)| *count).unwrap().0;
    let bitmap = reader
        .eval(&VixQuery::KeyExists {
            path: dense_path.clone(),
        })
        .unwrap();
    assert!(bitmap.count_set_bits() > 0);
    phase(&format!("KeyExists({dense_path:?}) postings"), &source);

    // the plan-time stats profile (VixCoreFormat::infer_stats): first + last
    // row of `_timestamp`
    let rows = [0u64, reader.row_count() - 1];
    reader.read_docs_column_rows("_timestamp", &rows).unwrap();
    phase("_timestamp boundary rows (infer_stats)", &source);

    // a needle-style docs point read: 3 scattered `_source` rows
    let picks = [
        7u64.min(reader.row_count() - 1),
        reader.row_count() / 2,
        reader.row_count() - 3,
    ];
    reader.read_source(&picks).unwrap();
    phase("_source point read of 3 rows", &source);

    // partial-time-range profile: eval_bitmap ANDs a timestamp_range bitmap,
    // which reads the WHOLE `_timestamp` column of the docs blob
    reader.timestamp_range(0, i64::MAX).unwrap();
    let (_, ts_bytes) = phase("timestamp_range (whole ts column)", &source);
    assert!(ts_bytes > 0);

    // dict-only unfiltered TopN/Distinct source (pilot fix B): doc_count
    // point reads for every value of one field, no postings, no docs
    if let Some((field, _)) = keys
        .iter()
        .find(|(name, _)| reader.field_id(name).is_some())
    {
        let counts = reader.field_value_counts(field).unwrap();
        phase(
            &format!(
                "field_value_counts({field:?}) -> {:?} values",
                counts.map(|v| v.len())
            ),
            &source,
        );
    }

    let total_fetches =
        source.fetch_count() + index_source.as_ref().map_or(0, |s| s.fetch_count());
    let total_bytes = source.byte_count() + index_source.as_ref().map_or(0, |s| s.byte_count());
    println!(
        "s3_review real: TOTAL {total_fetches} fetches, {total_bytes} bytes = {:.2}% of the \
         {total_len}-byte pair",
        total_bytes as f64 / total_len as f64 * 100.0,
    );
}
