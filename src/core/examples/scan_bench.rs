//! Micro-bench for the core-docs scan under an index row selection — the
//! mixed-predicate shape (`indexed term AND numeric data filter`) that a
//! histogram over `service_name = X AND duration > N` produces.
//!
//! The prod symptom (2026-08-01): 12h histogram with a ~10%-dense selection
//! took ~61s while the same window WITHOUT the numeric filter (index-only
//! SimpleHistogram) took ~1.3s. Suspects: selection forces chunk-granular
//! RANGED reads and `decode_threads = 0` (both tuned for needle selections).
//!
//! Usage:
//!   scan_bench <file.vix> [density]
//!
//! Runs, against one REAL file, every combination that matters:
//!   bytes-full      whole object in memory, NO selection, duration filter
//!   bytes-sel       whole object in memory, selection, duration filter
//!   ranged-sel-0t   ranged source (local, fetch-counted), selection, 0 threads  <- prod today
//!   ranged-sel-4t   ranged source, selection, decode_threads=4
//!   bytes-sel-4t    whole object, selection, decode_threads=4
//!   ranged-needle   ranged source, 0.01% selection (the shape ranged mode is FOR)
//!
//! Every variant projects [_timestamp, duration] and pushes duration > 2000,
//! like the prod query. Prints wall time, produced rows, and (ranged) the
//! fetch count/bytes so serial-fetch behavior is visible directly.

use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use futures::FutureExt;
use vortex_index::{ColumnBound, NumScalar, VixDocs, VixRangeSource};

/// Range source over an in-memory buffer that counts fetches — stands in for
/// the disk-cache/S3 ranged reads of prod (per-fetch latency is simulated by
/// count; the COUNT is what proves serial vs coalesced behavior).
struct CountingSource {
    data: bytes::Bytes,
    fetches: AtomicU64,
    bytes: AtomicU64,
}

impl VixRangeSource for CountingSource {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
    fn fetch(
        &self,
        range: Range<u64>,
    ) -> futures::future::BoxFuture<'static, anyhow::Result<Bytes>> {
        self.fetches.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(range.end - range.start, Ordering::Relaxed);
        let out = self.data.slice(range.start as usize..range.end as usize);
        futures::future::ready(Ok(out)).boxed()
    }
}

use bytes::Bytes;

fn selection(rows: u64, density: f64) -> Vec<u64> {
    let step = (1.0 / density).max(1.0) as u64;
    (0..rows).step_by(step as usize).collect()
}

#[allow(clippy::too_many_arguments)]
fn run(
    label: &str,
    docs: &VixDocs,
    rows: Option<Vec<u64>>,
    decode_threads: usize,
    fetches: Option<(&AtomicU64, &AtomicU64)>,
) -> anyhow::Result<()> {
    if let Some((f, b)) = fetches {
        f.store(0, Ordering::Relaxed);
        b.store(0, Ordering::Relaxed);
    }
    let selected = rows.as_ref().map(|r| r.len());
    let bounds = [ColumnBound {
        column: "duration".to_string(),
        min: Some((NumScalar::I64(2000), false)),
        max: None,
    }];
    let mut produced: u64 = 0;
    let mut batches: u64 = 0;
    let start = Instant::now();
    docs.scan_docs_opts(
        Some(&["_timestamp".to_string(), "duration".to_string()]),
        rows,
        None,
        &bounds,
        None,
        decode_threads,
        &mut |batch| {
            produced += batch.num_rows() as u64;
            batches += 1;
            Ok(())
        },
    )?;
    let wall = start.elapsed();
    let fetch_str = match fetches {
        Some((f, b)) => format!(
            " fetches={} fetched_mb={:.1}",
            f.load(Ordering::Relaxed),
            b.load(Ordering::Relaxed) as f64 / 1e6
        ),
        None => String::new(),
    };
    println!(
        "{label:<14} wall={:>7.2?} selected={:>9} produced={produced:>9} batches={batches}{fetch_str}",
        wall,
        selected
            .map(|s| s.to_string())
            .unwrap_or_else(|| "ALL".into()),
    );
    Ok(())
}

/// Like `run` but with an explicit projection (empty slice = ALL columns).
fn run_proj(
    label: &str,
    docs: &VixDocs,
    rows: Option<Vec<u64>>,
    cols: &[&str],
    fetches: Option<(&AtomicU64, &AtomicU64)>,
) -> anyhow::Result<()> {
    if let Some((f, b)) = fetches {
        f.store(0, Ordering::Relaxed);
        b.store(0, Ordering::Relaxed);
    }
    let bounds = [ColumnBound {
        column: "duration".to_string(),
        min: Some((NumScalar::I64(2000), false)),
        max: None,
    }];
    let projection: Option<Vec<String>> = if cols.is_empty() {
        None
    } else {
        Some(cols.iter().map(|c| c.to_string()).collect())
    };
    let mut produced: u64 = 0;
    let start = Instant::now();
    docs.scan_docs_opts(
        projection.as_deref(),
        rows,
        None,
        &bounds,
        None,
        0,
        &mut |batch| {
            produced += batch.num_rows() as u64;
            Ok(())
        },
    )?;
    let fetch_str = match fetches {
        Some((f, b)) => format!(
            " fetches={} fetched_mb={:.1}",
            f.load(Ordering::Relaxed),
            b.load(Ordering::Relaxed) as f64 / 1e6
        ),
        None => String::new(),
    };
    println!(
        "{label:<14} wall={:>7.2?} produced={produced:>9}{fetch_str}",
        start.elapsed()
    );
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("usage: scan_bench <file.vix> [density]");
    let density: f64 = args.next().map(|d| d.parse().unwrap()).unwrap_or(0.10);

    let data = Bytes::from(std::fs::read(&path)?);
    println!(
        "file={path} size={:.1}MB density={density}",
        data.len() as f64 / 1e6
    );

    let bytes_docs = VixDocs::open(data.clone())?;
    let rows_total = bytes_docs.row_count();
    println!("rows={rows_total}");
    let sel = selection(rows_total, density);
    let needle = selection(rows_total, 0.0001);

    // in-memory variants
    run("bytes-full", &bytes_docs, None, 0, None)?;
    run("bytes-sel", &bytes_docs, Some(sel.clone()), 0, None)?;
    run("bytes-sel-4t", &bytes_docs, Some(sel.clone()), 4, None)?;

    // ranged variants (fetch-counted)
    let source = Arc::new(CountingSource {
        data: data.clone(),
        fetches: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
    });
    let counters = (
        // SAFETY of the borrow: the Arc outlives every run() call below.
        unsafe { &*(&source.fetches as *const AtomicU64) },
        unsafe { &*(&source.bytes as *const AtomicU64) },
    );
    let open_start = Instant::now();
    let ranged_docs = VixDocs::open_ranged(source.clone() as Arc<dyn VixRangeSource>)?;
    println!(
        "open_ranged  wall={:>7.2?} fetches={} fetched_mb={:.1}",
        open_start.elapsed(),
        source.fetches.load(Ordering::Relaxed),
        source.bytes.load(Ordering::Relaxed) as f64 / 1e6
    );
    run(
        "ranged-sel-0t",
        &ranged_docs,
        Some(sel.clone()),
        0,
        Some(counters),
    )?;
    run_proj(
        "rng-3col",
        &ranged_docs,
        Some(sel.clone()),
        &["_timestamp", "duration", "service_name"],
        Some(counters),
    )?;
    run_proj(
        "rng-source",
        &ranged_docs,
        Some(sel.clone()),
        &["_timestamp", "duration", "_source"],
        Some(counters),
    )?;
    run_proj(
        "rng-allcols",
        &ranged_docs,
        Some(sel.clone()),
        &[],
        Some(counters),
    )?;
    run("ranged-sel-4t", &ranged_docs, Some(sel), 4, Some(counters))?;
    run(
        "ranged-needle",
        &ranged_docs,
        Some(needle),
        0,
        Some(counters),
    )?;
    run("ranged-full", &ranged_docs, None, 0, Some(counters))?;

    Ok(())
}
