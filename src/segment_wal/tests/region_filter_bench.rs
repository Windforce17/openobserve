// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use config::meta::stream::StreamType;
use segment_wal::format::{
    FrameInfo, SEGMENT_VERSION_V2, SegmentFrame, SegmentHeader, decode_segment_projected,
    decode_segment_projected_regions, encode_segment_version,
};

struct CountingAllocator;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn record_allocation() {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCATIONS.try_with(|n| n.set(n.get().checked_add(1).unwrap()));
    }
}

// SAFETY: every allocation operation delegates unchanged to System. The
// thread-local counters contain only integers and never allocate themselves.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn scan(bytes: &[u8], regions: bool) {
    let mut rows = 0;
    let wanted = |f: &FrameInfo| {
        f.org == "org" && f.stream_type == StreamType::Logs && f.stream == "stream0"
    };
    let deliver = |f: SegmentFrame| {
        rows += f.batch.num_rows();
        black_box(f);
        Ok(())
    };
    if regions {
        decode_segment_projected_regions(
            black_box(bytes),
            None,
            Some(&mut |r| {
                Ok(r.org == "org" && r.stream_type == StreamType::Logs && r.stream == "stream0")
            }),
            wanted,
            deliver,
        )
        .unwrap();
    } else {
        decode_segment_projected(black_box(bytes), None, wanted, deliver).unwrap();
    }
    assert_eq!(rows, 50 * 32);
}

fn measure(bytes: &[u8], regions: bool, iterations: u32) -> (Duration, u64) {
    ALLOCATIONS.set(0);
    TRACK.set(true);
    let started = Instant::now();
    for _ in 0..iterations {
        scan(bytes, regions);
    }
    let elapsed = started.elapsed();
    TRACK.set(false);
    (elapsed, ALLOCATIONS.get())
}

#[test]
#[ignore = "microbenchmark: run with --ignored --nocapture --test-threads=1"]
fn region_filter_microbench() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..32))]).unwrap();
    let mut frames = Vec::new();
    for index in 0..50 {
        for stream in 0..10 {
            frames.push(SegmentFrame {
                org: "org".to_string(),
                stream_type: StreamType::Logs,
                stream: format!("stream{stream}"),
                min_ts: index,
                max_ts: index,
                batch: batch.clone(),
            });
        }
    }
    let header = SegmentHeader {
        node_uuid: "bench".to_string(),
        seq: 1,
        created_at: 0,
    };
    let bytes = encode_segment_version(&header, &frames, SEGMENT_VERSION_V2).unwrap();
    for _ in 0..10 {
        scan(&bytes, false);
        scan(&bytes, true);
    }
    let iterations = 100;
    for trial in 0..5 {
        let (old, new) = if trial % 2 == 0 {
            (
                measure(&bytes, false, iterations),
                measure(&bytes, true, iterations),
            )
        } else {
            let new = measure(&bytes, true, iterations);
            (measure(&bytes, false, iterations), new)
        };
        println!(
            "trial={trial} iterations={iterations} bytes={} baseline_us_per_scan={:.3} region_us_per_scan={:.3} baseline_allocs_per_scan={} region_allocs_per_scan={}",
            bytes.len(),
            old.0.as_secs_f64() * 1e6 / f64::from(iterations),
            new.0.as_secs_f64() * 1e6 / f64::from(iterations),
            old.1 / u64::from(iterations),
            new.1 / u64::from(iterations),
        );
        assert_eq!(old.1 - new.1, 900 * u64::from(iterations));
    }
}
