// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

//! Cold-miss IO probe over a real `.vix`/`.vxi` pair: how many range reads
//! (round trips) and bytes a cold ranged open plus one needle-equality MISS
//! costs, per phase (probe / open / eval / timestamp_range). Diagnostic
//! only; run with `VIX_PAIR` set. `repack_pair` re-emits a pair's sidecar in
//! the current layout (DESIGN-COLD-MISS §5) so the probe can compare old vs
//! new layouts on the same data.

use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use futures::future::BoxFuture;
use parking_lot::Mutex;

use crate::{VixQuery, VixRangeSource, VixReader, bloom_probe::FileBloomProbe};

/// Counts round trips (`fetch` or one `fetch_many` batch), ranges and bytes
/// against a whole in-memory copy of the object.
struct CountingSource {
    name: String,
    data: Bytes,
    round_trips: AtomicUsize,
    ranges: Mutex<Vec<Range<u64>>>,
}

impl CountingSource {
    fn open(path: &Path) -> Arc<Self> {
        Arc::new(Self {
            name: path.display().to_string(),
            data: Bytes::from(std::fs::read(path).expect("pair member readable")),
            round_trips: AtomicUsize::new(0),
            ranges: Mutex::new(Vec::new()),
        })
    }

    fn snapshot(&self) -> (usize, usize, u64) {
        let ranges = self.ranges.lock();
        (
            self.round_trips.load(Ordering::SeqCst),
            ranges.len(),
            ranges.iter().map(|r| r.end - r.start).sum(),
        )
    }

    fn slice(&self, range: &Range<u64>) -> anyhow::Result<Bytes> {
        if range.start > range.end || range.end > self.len() {
            anyhow::bail!("range {range:?} out of bounds for {}", self.name);
        }
        self.ranges.lock().push(range.clone());
        Ok(self.data.slice(range.start as usize..range.end as usize))
    }
}

impl VixRangeSource for CountingSource {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        self.round_trips.fetch_add(1, Ordering::SeqCst);
        let out = self.slice(&range);
        Box::pin(async move { out })
    }

    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        self.round_trips.fetch_add(1, Ordering::SeqCst);
        let out: anyhow::Result<Vec<Bytes>> = ranges.iter().map(|r| self.slice(r)).collect();
        Box::pin(async move { out })
    }

    fn describe(&self) -> String {
        self.name.clone()
    }
}

struct Phase {
    data: (usize, usize, u64),
    index: (usize, usize, u64),
}

fn phase(data: &CountingSource, index: &CountingSource) -> Phase {
    Phase {
        data: data.snapshot(),
        index: index.snapshot(),
    }
}

fn report(label: &str, before: &Phase, after: &Phase) {
    let d = |a: (usize, usize, u64), b: (usize, usize, u64)| (a.0 - b.0, a.1 - b.1, a.2 - b.2);
    let data = d(after.data, before.data);
    let index = d(after.index, before.index);
    eprintln!(
        "{label:<16} data: {:>2} rt {:>3} ranges {:>9} B | sidecar: {:>2} rt {:>3} ranges {:>9} B | total {:>2} rt {:>9} B",
        data.0,
        data.1,
        data.2,
        index.0,
        index.1,
        index.2,
        data.0 + index.0,
        data.2 + index.2
    );
}

fn sidecar_path(path: &Path) -> PathBuf {
    std::env::var("VIX_INDEX")
        .map(PathBuf::from)
        .unwrap_or_else(|_| path.with_extension("vxi"))
}

/// `VIX_PAIR=/path/x.vix` (sibling `.vxi`, or `VIX_INDEX=/path/other.vxi`),
/// `VIX_FIELD` (default `call_uid`), `VIX_NEEDLE` (default an absent token),
/// `VIX_TAIL` sidecar eager-tail bytes (default `ZO_VIX_EAGER_TAIL_BYTES` or
/// the built-in), `VIX_SIDECAR_FIRST=1` for the S3 sidecar-first open,
/// `VIX_TS=min,max` micros for the timestamp_range phase (default the whole
/// range).
#[test]
#[ignore = "diagnostic; run with VIX_PAIR set"]
fn cold_miss_probe() {
    let path = PathBuf::from(std::env::var("VIX_PAIR").expect("VIX_PAIR"));
    let index_path = sidecar_path(&path);
    let field = std::env::var("VIX_FIELD").unwrap_or_else(|_| "call_uid".into());
    let needle =
        std::env::var("VIX_NEEDLE").unwrap_or_else(|_| "cold-miss-probe-absent-value".into());
    if let Ok(tail) = std::env::var("VIX_TAIL") {
        crate::container::set_tail_fetch_size(tail.parse().expect("VIX_TAIL bytes"));
    }
    let sidecar_first = std::env::var("VIX_SIDECAR_FIRST").is_ok_and(|v| v == "1");
    let data = CountingSource::open(&path);
    let index = CountingSource::open(&index_path);
    eprintln!(
        "pair {} data={} B sidecar={} ({} B) tail={} B field={field:?} needle={needle:?} \
         sidecar_first={sidecar_first}",
        path.display(),
        data.len(),
        index_path.display(),
        index.len(),
        crate::container::tail_fetch_size()
    );

    // stage 1b: sidecar-only bloom probe (what the pruner does per file)
    let p0 = phase(&data, &index);
    let probe_source: Arc<dyn VixRangeSource> = index.clone();
    let probe = FileBloomProbe::open_sidecar(probe_source).expect("probe open");
    let presence = probe.has_field(&field);
    let verdict = if probe.has_blob() {
        probe
            .probe(&field, &[needle.as_bytes()], true)
            .map(|v| format!("{v:?}"))
            .unwrap_or_else(|e| format!("err {e}"))
    } else {
        "<no bloom blob>".to_string()
    };
    let p1 = phase(&data, &index);
    report("bloom_probe", &p0, &p1);
    eprintln!(
        "    layout_gen={} columns_complete={} sections={} bf_fields={} presence(has_field)={presence:?} verdict={verdict}",
        probe.layout_gen(),
        probe.columns_complete(),
        probe.sections().len(),
        probe.bloomed_fields().count(),
    );

    // the vix search path: pair open, equality miss, timestamp range
    let data_source: Arc<dyn VixRangeSource> = data.clone();
    let index_source: Arc<dyn VixRangeSource> = index.clone();
    let reader = VixReader::open_ranged_with_index_mode(
        data_source,
        Some(index_source),
        crate::container::tail_fetch_size(),
        sidecar_first,
    )
    .expect("pair open");
    let p2 = phase(&data, &index);
    report("open", &p1, &p2);

    // the search layer maps a field the file provably lacks to `Nothing`
    let query = if reader.has_term_capability(&field) {
        VixQuery::Exact {
            field: field.clone(),
            token: needle.as_bytes().to_vec(),
        }
    } else {
        eprintln!("    field {field:?} has no term capability here -> Nothing");
        VixQuery::Nothing
    };
    let hits = reader.eval(&query).expect("eval");
    let p3 = phase(&data, &index);
    report("eval_exact", &p2, &p3);
    eprintln!("    hits={}", hits.count_set_bits());

    // the search fast path skips the clamp on an empty bitmap (S3)
    let (min, max) = std::env::var("VIX_TS")
        .ok()
        .and_then(|v| {
            let (a, b) = v.split_once(',')?;
            Some((a.parse().ok()?, b.parse().ok()?))
        })
        .unwrap_or((0, i64::MAX));
    if hits.count_set_bits() > 0 {
        let ts = reader.timestamp_range(min, max).expect("timestamp_range");
        eprintln!("    ts_rows={}", ts.count_set_bits());
    } else {
        eprintln!("    timestamp_range skipped: empty index bitmap");
    }
    let p4 = phase(&data, &index);
    report("timestamp_range", &p3, &p4);
    report("TOTAL", &p0, &p4);
    let (_, _, ib) = index.snapshot();
    let (_, _, db) = data.snapshot();
    eprintln!(
        "cold_miss_probe field={field} rt={} bytes={} KiB",
        data.round_trips.load(Ordering::SeqCst) + index.round_trips.load(Ordering::SeqCst),
        (ib + db) / 1024
    );
}

/// `VIX_PAIR=/path/x.vix` (sibling `.vxi`), `VIX_OUT=/path/new.vxi`;
/// `VIX_TAIL_ZSTD=0` for a plain tail (default compressed),
/// `VIX_AUTO_MIN_DISTINCT` (default 1024; 0 = no AUTO sections),
/// `VIX_AUTO_MAX_RATIO` (0.25), `VIX_AUTO_MAX_DATA_PCT` (2.0).
#[test]
#[ignore = "diagnostic; run with VIX_PAIR and VIX_OUT set"]
fn repack_pair() {
    let path = PathBuf::from(std::env::var("VIX_PAIR").expect("VIX_PAIR"));
    let out = PathBuf::from(std::env::var("VIX_OUT").expect("VIX_OUT"));
    let index_path = path.with_extension("vxi");
    let env_or = |name: &str, default: f64| -> f64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let opts = crate::SidecarRepackOptions {
        sidecar_tail_zstd: std::env::var("VIX_TAIL_ZSTD").map_or(true, |v| v != "0"),
        auto_bloom: crate::bloom::AutoBloomPolicy {
            min_distinct: env_or("VIX_AUTO_MIN_DISTINCT", 1024.0) as u64,
            max_ratio: env_or("VIX_AUTO_MAX_RATIO", 0.25),
            max_data_pct: env_or("VIX_AUTO_MAX_DATA_PCT", 2.0),
        },
        bloom_fpp: 0.001,
    };
    let data = CountingSource::open(&path);
    let index = CountingSource::open(&index_path);
    let started = std::time::Instant::now();
    let repack = crate::repack_sidecar(data.clone(), index.clone(), &opts).expect("repack");
    let took = started.elapsed();
    std::fs::write(&out, &repack.sidecar).expect("write repacked sidecar");
    let (d_rt, _, d_bytes) = data.snapshot();
    let (i_rt, _, i_bytes) = index.snapshot();
    eprintln!(
        "repack_pair {} -> {}: layout_gen {} -> {}, sidecar {} -> {} B, rows={} terms={} \
         added_sections={:?}, reads data {d_rt} rt/{d_bytes} B sidecar {i_rt} rt/{i_bytes} B, \
         took {took:?}",
        index_path.display(),
        out.display(),
        repack.from_layout_gen,
        crate::target_layout_gen(&opts),
        index.len(),
        repack.sidecar.len(),
        repack.row_count,
        repack.term_count,
        repack.added_sections,
    );
}
