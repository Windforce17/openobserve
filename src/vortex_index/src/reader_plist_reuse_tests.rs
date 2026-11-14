// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use futures::{FutureExt, future::BoxFuture};

use super::*;
use crate::source::{VixReadOperation, with_read_operation};

struct CoalescingSource {
    data: Bytes,
    reads: Mutex<Vec<Range<u64>>>,
    calls: Mutex<Vec<Vec<Range<u64>>>>,
}

impl VixRangeSource for CoalescingSource {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let fetch = self.fetch_many(vec![range]);
        async move { Ok(fetch.await?.remove(0)) }.boxed()
    }

    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        self.calls.lock().push(ranges.clone());
        let mut sorted = ranges.clone();
        sorted.sort_by_key(|range| range.start);
        let mut physical: Vec<Range<u64>> = Vec::new();
        for range in sorted {
            if let Some(last) = physical.last_mut()
                && range.start <= last.end.saturating_add(1024 * 1024)
            {
                last.end = last.end.max(range.end);
            } else {
                physical.push(range);
            }
        }
        self.reads.lock().extend(physical);
        let out = ranges
            .iter()
            .map(|range| self.data.slice(range.start as usize..range.end as usize))
            .collect();
        async move { Ok(out) }.boxed()
    }
}

fn fixture(
    ids: &[u32],
    enabled: bool,
    memory: Arc<ReaderMemory>,
) -> (PlistCursor, Arc<CoalescingSource>, Bytes) {
    let record = Bytes::from(postings::encode_record(ids).unwrap());
    let header_len = postings::record_header_len(&record).unwrap();
    let mut object = vec![0xA5; 123];
    object.extend_from_slice(&record);
    let source = Arc::new(CoalescingSource {
        data: Bytes::from(object),
        reads: Mutex::new(Vec::new()),
        calls: Mutex::new(Vec::new()),
    });
    let cursor = PlistCursor {
        record: PlistCursorRecord::Ranged {
            source: source.clone(),
            range: 123..123 + record.len() as u64,
            prefix: record.slice(..header_len),
            header_len,
            reuse: enabled.then(|| PlistCursorReuse::new(memory)),
        },
        doc_count: ids.len() as u64,
    };
    (cursor, source, record)
}

fn reuse(cursor: &PlistCursor) -> &PlistCursorReuse {
    let PlistCursorRecord::Ranged {
        reuse: Some(reuse), ..
    } = &cursor.record
    else {
        panic!("fixture must enable ranged reuse");
    };
    reuse
}

fn walk_bytes(cursor: &PlistCursor, start: u32, end: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    cursor
        .for_each_in_range(start, end, |id| {
            bytes.extend_from_slice(&id.to_le_bytes());
            Ok(())
        })
        .unwrap();
    bytes
}

#[test]
fn plist_reuse_preserves_gap_bytes_and_eliminates_bucket_rereads() {
    let ids: Vec<u32> = (0..50_000).map(|index| index * 3 + 2).collect();
    let memory = Arc::new(ReaderMemory::new());
    let base = memory.total.load(Ordering::Acquire);
    let (cached, source, record) = fixture(&ids, true, memory.clone());
    let (legacy, old_source, _) = fixture(&ids, false, Arc::new(ReaderMemory::new()));
    let targets = [0, 3_000, 30_000, 75_000, 120_000, u32::MAX];
    let expected: Vec<u64> = targets
        .iter()
        .map(|target| ids.partition_point(|id| id < target) as u64)
        .collect();
    assert_eq!(cached.ranks(&targets).unwrap(), expected);
    assert_eq!(legacy.ranks(&targets).unwrap(), expected);
    assert_eq!(*source.reads.lock(), *old_source.reads.lock());
    assert_eq!(source.reads.lock().len(), 1);

    let (range, bytes) = reuse(&cached).window.get().unwrap();
    assert_eq!(
        bytes.as_ref(),
        &record[(range.start - 123) as usize..(range.end - 123) as usize]
    );
    assert_ne!(bytes.as_ptr(), source.data[range.start as usize..].as_ptr());
    assert!(bytes.len() <= PLIST_CURSOR_REUSE_MAX_BYTES);
    assert!(memory.total.load(Ordering::Acquire) >= base + bytes.len());
    for (start, end) in [
        (30_000, 40_000),
        (30_001, 30_050),
        (60_000, 70_000),
        (60_000, 70_000),
        (0, 0),
        (90_000, 85_000),
    ] {
        assert_eq!(
            walk_bytes(&cached, start, end),
            walk_bytes(&legacy, start, end)
        );
    }
    assert_eq!(cached.ranks(&targets).unwrap(), expected);
    assert_eq!(
        source.reads.lock().len(),
        1,
        "bucket reads must reuse the rank envelope"
    );
    assert!(old_source.reads.lock().len() > 1);
    drop(cached);
    assert_eq!(memory.total.load(Ordering::Acquire), base);
}

#[test]
fn plist_reuse_large_rank_span_falls_back_to_identical_sparse_requests() {
    let ids: Vec<u32> = (0..1_600_000).map(|index| index * 1000 + 2).collect();
    let memory = Arc::new(ReaderMemory::new());
    let base = memory.total.load(Ordering::Acquire);
    let (cached, source, record) = fixture(&ids, true, memory.clone());
    let (legacy, old_source, _) = fixture(&ids, false, Arc::new(ReaderMemory::new()));
    assert!(record.len() > PLIST_CURSOR_REUSE_MAX_BYTES);
    let targets = [ids[1_100], ids[1_500_100]];
    assert_eq!(
        cached.ranks(&targets).unwrap(),
        legacy.ranks(&targets).unwrap()
    );
    assert!(reuse(&cached).window.get().is_none());
    let start = ids[1_100];
    let end = ids[1_105];
    assert_eq!(
        walk_bytes(&cached, start, end),
        walk_bytes(&legacy, start, end)
    );
    assert_eq!(*source.calls.lock(), *old_source.calls.lock());
    assert_eq!(memory.total.load(Ordering::Acquire), base);
}

struct Budget {
    limit: usize,
    calls: AtomicUsize,
}

impl VixReadOperation for Budget {
    fn is_cancelled(&self) -> bool {
        false
    }

    fn try_check_memory(&self, bytes: usize) -> Result<bool> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(bytes <= self.limit)
    }
}

#[test]
fn plist_reuse_budget_refusal_falls_back_without_leaking_a_reservation() {
    let ids: Vec<u32> = (0..50_000).map(|index| index * 3 + 2).collect();
    for admitted_fetch in [false, true] {
        let memory = Arc::new(ReaderMemory::new());
        let base = memory.total.load(Ordering::Acquire);
        let (cursor, source, record) = fixture(&ids, true, memory.clone());
        let (legacy, old_source, _) = fixture(&ids, false, Arc::new(ReaderMemory::new()));
        let budget = Arc::new(Budget {
            limit: base + if admitted_fetch { record.len() } else { 0 },
            calls: AtomicUsize::new(0),
        });
        let targets = [3_000, 120_000];
        assert_eq!(
            with_read_operation(budget.clone(), || cursor.ranks(&targets)).unwrap(),
            legacy.ranks(&targets).unwrap()
        );
        assert_eq!(
            budget.calls.load(Ordering::Acquire),
            if admitted_fetch { 2 } else { 1 }
        );
        assert!(reuse(&cursor).window.get().is_none());
        assert_eq!(memory.total.load(Ordering::Acquire), base);
        assert!(memory.peak() <= budget.limit);
        assert_eq!(
            walk_bytes(&cursor, 30_000, 40_000),
            walk_bytes(&legacy, 30_000, 40_000)
        );
        assert_eq!(*source.calls.lock(), *old_source.calls.lock());
    }
}

#[test]
fn plist_reuse_fixed_cap_is_inclusive_and_does_not_replace_a_window() {
    let memory = Arc::new(ReaderMemory::new());
    let base = memory.total.load(Ordering::Acquire);
    let source = CoalescingSource {
        data: Bytes::from(vec![0xA5; PLIST_CURSOR_REUSE_MAX_BYTES + 1]),
        reads: Mutex::new(Vec::new()),
        calls: Mutex::new(Vec::new()),
    };
    let cache = PlistCursorReuse::new(memory.clone());
    assert!(
        cache
            .fetch_ranks(
                &source,
                std::slice::from_ref(&(0..PLIST_CURSOR_REUSE_MAX_BYTES as u64 + 1)),
            )
            .unwrap()
            .is_none()
    );
    assert!(source.reads.lock().is_empty());
    assert_eq!(memory.total.load(Ordering::Acquire), base);
    let windows = cache
        .fetch_ranks(
            &source,
            std::slice::from_ref(&(0..PLIST_CURSOR_REUSE_MAX_BYTES as u64)),
        )
        .unwrap()
        .unwrap();
    assert_eq!(windows[0].len(), PLIST_CURSOR_REUSE_MAX_BYTES);
    let retained = memory.total.load(Ordering::Acquire) - base;
    let peak = memory.peak() - base;
    assert!(retained <= PLIST_CURSOR_REUSE_MAX_BYTES + 256);
    assert!(peak <= 2 * PLIST_CURSOR_REUSE_MAX_BYTES + 256);
    eprintln!("plist reuse cap: retained={retained} bytes, peak={peak} bytes");
    assert!(
        cache
            .fetch_ranks(&source, std::slice::from_ref(&(1..2)))
            .unwrap()
            .is_none()
    );
    assert_eq!(source.reads.lock().len(), 1);
    drop(cache);
    assert!(
        memory.total.load(Ordering::Acquire) > base,
        "returned slices must retain admission"
    );
    drop(windows);
    assert_eq!(memory.total.load(Ordering::Acquire), base);
}

#[test]
fn plist_reuse_concurrent_cursors_do_not_share_bytes_and_release_all_charges() {
    let memory = Arc::new(ReaderMemory::new());
    let base = memory.total.load(Ordering::Acquire);
    let barrier = Arc::new(std::sync::Barrier::new(4));
    std::thread::scope(|scope| {
        for step in [2, 3, 5, 7] {
            let memory = memory.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                let ids: Vec<u32> = (0..50_000).map(|index| index * step + 1).collect();
                let (cursor, source, _) = fixture(&ids, true, memory);
                let targets = [ids[1_001], ids[40_001]];
                assert_eq!(cursor.ranks(&targets).unwrap(), vec![1_001, 40_001]);
                barrier.wait();
                for index in [5_001, 10_001, 15_001, 30_001] {
                    let expected: Vec<u8> = ids[index..index + 500]
                        .iter()
                        .flat_map(|id| id.to_le_bytes())
                        .collect();
                    assert_eq!(walk_bytes(&cursor, ids[index], ids[index + 500]), expected);
                }
                assert_eq!(source.reads.lock().len(), 1);
            });
        }
    });
    assert_eq!(memory.total.load(Ordering::Acquire), base);
}

#[test]
fn plist_reuse_prefix_only_and_outside_window_preserve_legacy_results() {
    let ids: Vec<u32> = (0..50_000).map(|index| index * 3 + 2).collect();
    let (mut cursor, source, record) = fixture(&ids, true, Arc::new(ReaderMemory::new()));
    let (legacy, ..) = fixture(&ids, false, Arc::new(ReaderMemory::new()));
    if let PlistCursorRecord::Ranged { prefix, .. } = &mut cursor.record {
        *prefix = record.slice(..record.len() / 2);
    }
    assert_eq!(
        cursor.ranks(&[3_000, 30_000]).unwrap(),
        legacy.ranks(&[3_000, 30_000]).unwrap()
    );
    assert!(source.reads.lock().is_empty());
    assert!(reuse(&cursor).window.get().is_none());
    assert_eq!(
        cursor.ranks(&[90_000, 100_000]).unwrap(),
        legacy.ranks(&[90_000, 100_000]).unwrap()
    );
    assert_eq!(
        walk_bytes(&cursor, 135_000, 145_000),
        walk_bytes(&legacy, 135_000, 145_000)
    );
    assert_eq!(source.reads.lock().len(), 2);
}

#[test]
fn plist_reuse_failed_fetch_and_cancelled_admission_release_pending_memory() {
    struct FailingSource;
    impl VixRangeSource for FailingSource {
        fn len(&self) -> u64 {
            1024
        }

        fn fetch(&self, _: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
            async { Err(anyhow::anyhow!("injected IO failure")) }.boxed()
        }
    }

    struct CancelledAdmission;
    impl VixReadOperation for CancelledAdmission {
        fn is_cancelled(&self) -> bool {
            false
        }

        fn try_check_memory(&self, _: usize) -> Result<bool> {
            Err(VixError::Cancelled)
        }
    }

    let memory = Arc::new(ReaderMemory::new());
    let base = memory.total.load(Ordering::Acquire);
    let cache = PlistCursorReuse::new(memory.clone());
    assert!(matches!(
        with_read_operation(Arc::new(CancelledAdmission), || {
            cache.fetch_ranks(&FailingSource, std::slice::from_ref(&(0..1024)))
        }),
        Err(VixError::Cancelled)
    ));
    assert_eq!(memory.total.load(Ordering::Acquire), base);
    assert_eq!(memory.peak(), base);
    assert!(
        cache
            .fetch_ranks(&FailingSource, std::slice::from_ref(&(0..1024)))
            .unwrap_err()
            .to_string()
            .contains("injected IO failure")
    );
    assert!(cache.window.get().is_none());
    assert_eq!(memory.total.load(Ordering::Acquire), base);
}

#[test]
fn plist_reuse_racing_fills_keep_only_one_window_and_charge_live_slices() {
    struct RacingSource(Arc<std::sync::Barrier>);
    impl VixRangeSource for RacingSource {
        fn len(&self) -> u64 {
            1024
        }

        fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
            self.0.wait();
            async move { Ok(Bytes::from(vec![0xA5; (range.end - range.start) as usize])) }.boxed()
        }
    }

    let memory = Arc::new(ReaderMemory::new());
    let base = memory.total.load(Ordering::Acquire);
    let cache = PlistCursorReuse::new(memory.clone());
    let source = RacingSource(Arc::new(std::sync::Barrier::new(4)));
    let windows = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    cache
                        .fetch_ranks(&source, std::slice::from_ref(&(0..1024)))
                        .unwrap()
                        .unwrap()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(memory.total.load(Ordering::Acquire) >= base + 4 * 1024);
    for window in &windows {
        assert_eq!(window[0].as_ref(), &[0xA5; 1024]);
    }
    drop(windows);
    let retained = memory.total.load(Ordering::Acquire) - base;
    assert!((1024..2048).contains(&retained));
    assert_eq!(cache.window.get().unwrap().1.as_ref(), &[0xA5; 1024]);
    drop(cache);
    assert_eq!(memory.total.load(Ordering::Acquire), base);
}
