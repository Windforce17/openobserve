// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Real heap cost of the index-phase steps on a production file pair, under a
//! counting global allocator — what the admission gate SHOULD charge, next
//! to what it does charge (`metadata_memory_bound` = encoded × 64).
//!
//! `cargo run -p vortex_index --release --example residual_alloc_probe -- \
//!     /tmp/vixab/75117742571525980165575.vix [service_name] [match_all]`
//!
//! Steps, each reported as live-heap delta and live-heap PEAK over the step:
//! ranged open (puffin footers of both objects), row-id eval of the A48 shape
//! (dictionary + terms + plist), detached docs footer open (`schema()`), the
//! candidate rows' `body` column, the candidate rows' 7 full-text columns.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use vortex_index::{VixQuery, VixRangeSource, VixReader};

static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

struct Counting;

impl Counting {
    fn bump(delta: i64) {
        let live = LIVE.fetch_add(delta, Ordering::Relaxed) + delta;
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            Self::bump(layout.size() as i64);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            Self::bump(layout.size() as i64);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        Self::bump(-(layout.size() as i64));
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            Self::bump(new_size as i64 - layout.size() as i64);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Live-heap delta and peak-above-start of `work`, in KB.
fn measure<T>(label: &str, work: impl FnOnce() -> T) -> T {
    let start = LIVE.load(Ordering::Relaxed);
    PEAK.store(start, Ordering::Relaxed);
    let out = work();
    let end = LIVE.load(Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);
    println!(
        "{label:<44} retained {:>8} KB   peak {:>8} KB",
        (end - start) / 1024,
        (peak - start) / 1024
    );
    out
}

struct MemSource {
    bytes: Bytes,
    reads: parking_lot::Mutex<Vec<Range<u64>>>,
}

impl VixRangeSource for MemSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        self.reads.lock().push(range.clone());
        let out = self.bytes.slice(range.start as usize..range.end as usize);
        async move { Ok(out) }.boxed()
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .expect("path to a .vix (its .vxi must sit next to it)");
    let service = args
        .next()
        .unwrap_or_else(|| "cfworkers-deploy-cloudrun-worker".to_string());
    let phrase = args
        .next()
        .unwrap_or_else(|| "server_status:DEPLOY_STATUS_SUCCESS".to_string());
    let data = Bytes::from(std::fs::read(&path).unwrap());
    let index =
        Bytes::from(std::fs::read(std::path::Path::new(&path).with_extension("vxi")).unwrap());
    println!(
        "data {} MB, index {} MB",
        data.len() / (1 << 20),
        index.len() / (1 << 20)
    );
    for tag in ["docs", "stats"] {
        if let Ok(range) = vortex_index::test_support::blob_byte_range(&data, tag) {
            println!(
                "    data blob {tag:<6} {}..{} ({} KB; tail starts at {})",
                range.start,
                range.end,
                (range.end - range.start) / 1024,
                data.len().saturating_sub(128 * 1024)
            );
        }
    }
    let data_src = Arc::new(MemSource {
        bytes: data,
        reads: parking_lot::Mutex::new(Vec::new()),
    });
    let index_src = Arc::new(MemSource {
        bytes: index,
        reads: parking_lot::Mutex::new(Vec::new()),
    });

    let reader = measure("ranged open (both puffin footers)", || {
        VixReader::open_ranged_with_index(
            data_src.clone() as Arc<dyn VixRangeSource>,
            Some(index_src.clone() as Arc<dyn VixRangeSource>),
        )
        .unwrap()
    });
    println!(
        "    reader.memory_size {} KB, gate peak {} KB",
        reader.memory_size() / 1024,
        reader.memory_peak() / 1024
    );

    // the file's own full-text fields (the A48 scope on logs/default)
    let mut fts: Vec<String> = reader.fts_fields().iter().cloned().collect();
    fts.sort();
    println!("    fts fields {fts:?}");
    let mut rows: Vec<u64> = Vec::new();
    if !fts.is_empty() && reader.has_term_capability("service_name") {
        let tokens: Vec<VixQuery> = vortex_index::o2_tokenize(&phrase, 2, 64)
            .map(|t| VixQuery::TokenAnyField {
                token: t.into_bytes(),
            })
            .collect();
        let query = VixQuery::FullText {
            fields: fts.clone(),
            query: Box::new(VixQuery::And(vec![
                VixQuery::And(tokens),
                VixQuery::Exact {
                    field: "service_name".to_string(),
                    token: service.into_bytes(),
                },
            ])),
        };
        let bitmap = measure("row-id eval (dict + terms + plist)", || {
            reader.eval(&query).unwrap()
        });
        rows = bitmap.set_indices().map(|r| r as u64).collect();
        println!(
            "    candidates {} rows; reader.memory_size {} KB, gate peak {} KB",
            rows.len(),
            reader.memory_size() / 1024,
            reader.memory_peak() / 1024
        );
    }
    if rows.is_empty() {
        // no match (or no A48-shaped fields) in this file: sample 8 rows
        // spread over the file so the docs steps still measure a realistic
        // multi-chunk point read
        let n = reader.row_count();
        rows = (0..8).map(|i| i * n / 8 + 17).filter(|r| *r < n).collect();
        println!("    (probing 8 spread rows {rows:?})");
    }

    let stats_len = reader.stats_blob_len();
    let before = reader.memory_peak();
    measure("stats blob decode (column_chunk_stats)", || {
        reader.column_chunk_stats().map(|s| s.columns.len())
    });
    println!(
        "    stats blob {} KB; gate peak delta {} KB",
        stats_len.unwrap_or(0) / 1024,
        (reader.memory_peak().saturating_sub(before)) / 1024
    );

    let docs = reader.detached_docs();
    let before_reads = data_src.reads.lock().len();
    let schema = measure("detached docs footer open (schema)", || {
        docs.schema().unwrap()
    });
    let footer_reads: Vec<Range<u64>> = data_src.reads.lock()[before_reads..].to_vec();
    println!(
        "    columns {}, footer reads {:?} = {} KB; gate peak {} KB",
        schema.fields().len(),
        footer_reads
            .iter()
            .map(|r| r.end - r.start)
            .collect::<Vec<_>>(),
        footer_reads.iter().map(|r| r.end - r.start).sum::<u64>() / 1024,
        reader.memory_peak() / 1024
    );

    let before_reads = data_src.reads.lock().len();
    let string_cols: Vec<String> = schema
        .fields()
        .iter()
        .filter(|f| {
            matches!(
                f.data_type(),
                arrow::datatypes::DataType::Utf8
                    | arrow::datatypes::DataType::LargeUtf8
                    | arrow::datatypes::DataType::Utf8View
            )
        })
        .map(|f| f.name().clone())
        .filter(|n| n != "_source" && n != "_original")
        .collect();
    let first_fts = fts
        .first()
        .cloned()
        .or_else(|| string_cols.first().cloned())
        .expect("a string column");
    measure(&format!("candidate rows: {first_fts} column"), || {
        docs.read_columns_rows(&[first_fts.as_str()], &rows)
            .unwrap()
    });
    let reads: Vec<Range<u64>> = data_src.reads.lock()[before_reads..].to_vec();
    println!(
        "    {} reads, {} KB",
        reads.len(),
        reads.iter().map(|r| r.end - r.start).sum::<u64>() / 1024
    );

    let before_reads = data_src.reads.lock().len();
    let mut seven: Vec<&str> = vec!["_timestamp"];
    let wide: Vec<&str> = if fts.is_empty() {
        string_cols.iter().map(String::as_str).take(5).collect()
    } else {
        fts.iter().map(String::as_str).take(5).collect()
    };
    seven.extend(wide);
    if schema.index_of("service_name").is_ok() {
        seven.push("service_name");
    }
    measure("candidate rows: ts + fts columns", || {
        docs.read_columns_rows(&seven, &rows).unwrap()
    });
    let reads: Vec<Range<u64>> = data_src.reads.lock()[before_reads..].to_vec();
    println!(
        "    {} reads, {} KB; gate peak {} KB",
        reads.len(),
        reads.iter().map(|r| r.end - r.start).sum::<u64>() / 1024,
        reader.memory_peak() / 1024
    );
}
