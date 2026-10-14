// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};

use super::*;
use crate::source::{RangedBlob, VixReadOperation, with_read_operation};

#[derive(Debug, thiserror::Error)]
#[error("test reader ownership admission refused")]
struct OwnershipDenied;

struct Budget {
    limit: usize,
    cancelled: AtomicBool,
    calls: AtomicUsize,
    peak: AtomicUsize,
}

impl Budget {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            cancelled: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })
    }
}

impl VixReadOperation for Budget {
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
    fn check_memory(&self, bytes: usize) -> Result<()> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.peak.fetch_max(bytes, Ordering::AcqRel);
        if bytes > self.limit {
            Err(VixError::Callback(OwnershipDenied.into()))
        } else {
            Ok(())
        }
    }
}

struct AdvertisedSource {
    size: u64,
    tail: Option<Bytes>,
    tail_reads: AtomicUsize,
    body_reads: AtomicUsize,
}

impl VixRangeSource for AdvertisedSource {
    fn len(&self) -> u64 {
        self.size
    }
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let result = if let Some(tail) = &self.tail
            && range == (self.size - tail.len() as u64..self.size)
        {
            self.tail_reads.fetch_add(1, Ordering::AcqRel);
            Ok(tail.clone())
        } else {
            self.body_reads.fetch_add(1, Ordering::AcqRel);
            Err(anyhow::anyhow!("unexpected metadata body read"))
        };
        futures::future::ready(result).boxed()
    }
}

fn small_reader() -> VixReader {
    use arrow::datatypes::Schema;
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let mut writer = crate::VixWriter::new(
        &schema,
        crate::VixWriterOptions {
            encode_threads: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}", "{}"]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    VixReader::open_with_index(data.into(), index.map(Bytes::from)).unwrap()
}

#[test]
fn cached_metadata_is_admitted_even_for_a_zero_io_count() {
    let reader = small_reader();
    let error = with_read_operation(Budget::new(reader.memory_size() - 1), || {
        reader.count(&crate::VixQuery::All)
    })
    .unwrap_err();
    assert!(error.chain().any(|cause| cause.is::<OwnershipDenied>()));
    // A refusing/cancelled creator operation never becomes reader state.
    assert_eq!(reader.count(&crate::VixQuery::All).unwrap(), 2);
}

#[test]
fn observer_registration_refusal_and_cancellation_are_observable() {
    struct Notifications(AtomicUsize);
    impl ReaderMemoryObserver for Notifications {
        fn memory_changed(&self, _: usize) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    let reader = small_reader();
    let refused = Arc::new(Notifications(AtomicUsize::new(0)));
    let observer: Arc<dyn ReaderMemoryObserver> = refused.clone();
    let before = reader.memory_size();
    let error = with_read_operation(Budget::new(before), || {
        reader.observe_memory(Arc::downgrade(&observer))
    })
    .unwrap_err();
    assert!(
        anyhow::Error::new(error)
            .chain()
            .any(|cause| cause.is::<OwnershipDenied>())
    );
    reader.memory.notify();
    assert_eq!(refused.0.load(Ordering::Acquire), 0);
    assert_eq!(reader.memory_size(), before);
    assert_eq!(reader.memory.total.load(Ordering::Acquire), before);

    // A failed operation is not retained, and a successful retry subscribes.
    reader.observe_memory(Arc::downgrade(&observer)).unwrap();
    assert_eq!(refused.0.load(Ordering::Acquire), 1);

    // Cancellation must also refuse registration when spare vector slots
    // mean no allocation growth is needed. The healthy observer survives.
    let cancelled = Arc::new(Notifications(AtomicUsize::new(0)));
    let cancelled_observer: Arc<dyn ReaderMemoryObserver> = cancelled.clone();
    let operation = Budget::new(usize::MAX);
    operation.cancelled.store(true, Ordering::Release);
    let before = reader.memory_size();
    let error = with_read_operation(operation, || {
        reader.observe_memory(Arc::downgrade(&cancelled_observer))
    })
    .unwrap_err();
    assert!(matches!(error, VixError::Cancelled));
    reader.memory.notify();
    assert_eq!(cancelled.0.load(Ordering::Acquire), 0);
    assert_eq!(refused.0.load(Ordering::Acquire), 2);
    assert_eq!(reader.memory_size(), before);
    assert_eq!(reader.memory.total.load(Ordering::Acquire), before);
}

#[test]
fn advertised_footer_and_directory_refuse_before_body_fetch() {
    let mut tail = vec![0u8; 64];
    let trailer = tail.len() - puffin::FOOTER_SIZE as usize;
    tail[trailer..trailer + 4].copy_from_slice(&(16u32 * 1024 * 1024).to_le_bytes());
    let magic_start = tail.len() - puffin::MAGIC_SIZE as usize;
    tail[magic_start..].copy_from_slice(&puffin::MAGIC);
    let source = Arc::new(AdvertisedSource {
        size: 1024 * 1024 * 1024,
        tail: Some(tail.into()),
        tail_reads: AtomicUsize::new(0),
        body_reads: AtomicUsize::new(0),
    });
    let dynamic: Arc<dyn VixRangeSource> = source.clone();
    let error = with_read_operation(Budget::new(1024 * 1024), || {
        crate::container::parse_container_ranged_with_tail(&dynamic, 64)
    })
    .err()
    .unwrap();
    assert!(
        anyhow::Error::new(error)
            .chain()
            .any(|cause| cause.is::<OwnershipDenied>())
    );
    assert_eq!(source.tail_reads.load(Ordering::Acquire), 1);
    assert_eq!(source.body_reads.load(Ordering::Acquire), 0);

    let mut reader = small_reader();
    let source = Arc::new(AdvertisedSource {
        size: 64 * 1024 * 1024,
        tail: None,
        tail_reads: AtomicUsize::new(0),
        body_reads: AtomicUsize::new(0),
    });
    reader.dict_blob = Some(BlobHandle::Ranged(RangedBlob::new(
        source.clone(),
        0..source.size,
    )));
    let error = with_read_operation(Budget::new(reader.memory_size() + 1024 * 1024), || {
        reader.dict_index()
    })
    .err()
    .unwrap();
    assert!(
        anyhow::Error::new(error)
            .chain()
            .any(|cause| cause.is::<OwnershipDenied>())
    );
    assert_eq!(source.body_reads.load(Ordering::Acquire), 0);
}

/// DESIGN-COLD-MISS §2 S1: footer admission is sized on the DECODED payload.
/// A zstd footer whose 2 MiB of properties compress to a few hundred bytes
/// must be refused under a budget that its encoded size alone would pass —
/// on the ranged path (`TailProbe`) and the in-memory one (`parse_container`)
/// alike — and admitted, parsing the full property, under a budget sized for
/// the decoded form.
#[test]
fn compressed_footer_admission_is_sized_on_the_decoded_payload() {
    let pad = "a".repeat(2 * 1024 * 1024);
    let properties = vec![
        ("version".to_string(), "3".to_string()),
        ("pad".to_string(), pad.clone()),
    ];
    let compressed = Bytes::from(
        crate::container::build_container_parts(properties.clone(), Vec::new(), true).unwrap(),
    );
    let plain = Bytes::from(
        crate::container::build_container_parts(properties, Vec::new(), false).unwrap(),
    );
    assert!(
        compressed.len() < 64 * 1024,
        "the padded footer must compress far below the budget: {} bytes",
        compressed.len()
    );
    assert!(plain.len() > 2 * 1024 * 1024);
    let decoded = puffin::reader::footer_payload_decoded_size(&compressed).unwrap();
    assert!(decoded > 2 * 1024 * 1024, "{decoded}");

    let denied = |error: VixError| {
        assert!(
            anyhow::Error::new(error)
                .chain()
                .any(|cause| cause.is::<OwnershipDenied>())
        );
    };
    // a budget the ENCODED bound (×8 of ~hundreds of bytes + 64 KiB) would
    // pass, the decoded bound (×8 of 2 MiB) cannot
    let tight = Budget::new(4 * 1024 * 1024);
    let source = crate::BytesRangeSource::new("compressed", compressed.clone());
    let dynamic: Arc<dyn VixRangeSource> = source.clone();
    denied(
        with_read_operation(tight.clone(), || {
            crate::container::parse_container_ranged_with_tail(&dynamic, 64)
        })
        .err()
        .unwrap(),
    );
    denied(
        with_read_operation(tight.clone(), || {
            crate::container::parse_container(&compressed)
        })
        .err()
        .unwrap(),
    );
    // the plain form of the same footer is refused under the same budget:
    // compression changed the bytes on the wire, not the admission
    let plain_source = crate::BytesRangeSource::new("plain", plain.clone());
    let plain_dynamic: Arc<dyn VixRangeSource> = plain_source;
    denied(
        with_read_operation(tight, || {
            crate::container::parse_container_ranged_with_tail(&plain_dynamic, 64)
        })
        .err()
        .unwrap(),
    );

    let roomy = Budget::new(64 * 1024 * 1024);
    let container = with_read_operation(roomy.clone(), || {
        crate::container::parse_container_ranged_with_tail(&dynamic, 64)
    })
    .unwrap();
    assert_eq!(container.properties.get("pad"), Some(&pad));
    let container =
        with_read_operation(roomy, || crate::container::parse_container(&compressed)).unwrap();
    assert_eq!(container.properties.get("pad"), Some(&pad));
}

#[test]
fn both_cold_envelopes_remain_admitted_until_reader_publication() {
    let bytes = Bytes::from(
        crate::container::build_container(vec![("version".into(), "3".into())], Vec::new())
            .unwrap(),
    );
    let source = crate::BytesRangeSource::new("envelope", bytes);
    let memory = Arc::new(ReaderMemory::new());
    let _scope = memory.enter();
    with_read_operation(Budget::new(100 * 1024), || {
        let first = crate::container::parse_container_ranged(&source).unwrap();
        let error = crate::container::parse_container_ranged(&source)
            .err()
            .unwrap();
        assert!(
            anyhow::Error::new(error)
                .chain()
                .any(|cause| cause.is::<OwnershipDenied>())
        );
        drop(first);
        let second = crate::container::parse_container_ranged(&source).unwrap();
        assert_eq!(
            second.properties.get("version").map(String::as_str),
            Some("3")
        );
    });
    assert_eq!(memory.total.load(Ordering::Acquire), memory.size());
}

#[test]
fn concurrent_pending_allocations_sum_and_failed_reservation_rolls_back() {
    let memory = Arc::new(ReaderMemory::new());
    let base = memory.size();
    let budget = Budget::new(base + 1000);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let worker_memory = memory.clone();
        let operation = budget.clone();
        scope.spawn(move || {
            with_read_operation(operation, || {
                let _pending = worker_memory.reserve(600).unwrap();
                ready_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
        });
        ready_rx.recv().unwrap();
        with_read_operation(budget.clone(), || {
            let error = memory.reserve(500).err().unwrap();
            assert!(
                anyhow::Error::new(error)
                    .chain()
                    .any(|cause| cause.is::<OwnershipDenied>())
            );
            let _pending = memory.reserve(400).unwrap();
        });
        release_tx.send(()).unwrap();
    });
    with_read_operation(budget, || {
        let _pending = memory.reserve(1000).unwrap();
    });
    assert_eq!(memory.total.load(Ordering::Acquire), base);
}

// Consecutive bucket positions create dense runs and then FIFO tombstones,
// deterministically exercising logical capacity loss and in-place rehash.
#[derive(Default)]
struct BucketHasher(u64);

impl std::hash::Hasher for BucketHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("dictionary cache keys are usize");
    }
    fn write_usize(&mut self, value: usize) {
        self.0 = value as u64;
    }
}

#[test]
fn fifo_eviction_reuses_owned_budget_instead_of_accumulating_history() {
    let reader = small_reader();
    let owned_limit = reader.memory_size() + 8 * 1024 * 1024;
    let budget = Budget::new(owned_limit);
    with_read_operation(budget, || {
        let block = Bytes::from(vec![7u8; 4096]);
        let mut cache = (
            HashMap::with_hasher(std::hash::BuildHasherDefault::<BucketHasher>::default()),
            std::collections::VecDeque::new(),
            0,
        );
        let mut saw_logical_allocation_decrease = false;
        let mut saw_rehash = false;
        for key in 0..8192 {
            let previous_capacity_bytes = hash_table_bytes::<(usize, Bytes)>(cache.0.capacity());
            let previous_owned = reader.memory_size();
            let previous_table_bytes = cache.2;
            reader
                .cache_dict_block(&mut cache, key, block.clone())
                .unwrap();
            if hash_table_bytes::<(usize, Bytes)>(cache.0.capacity()) < previous_capacity_bytes {
                saw_logical_allocation_decrease = true;
                // Same-sized FIFO payloads cannot release the still-live
                // table allocation merely by leaving tombstones behind.
                assert!(reader.memory_size() >= previous_owned);
            }
            if key > 1024
                && hash_table_bytes::<(usize, Bytes)>(cache.0.capacity()) > previous_capacity_bytes
                && cache.2 == previous_table_bytes
            {
                saw_rehash = true;
                // Reclaiming tombstones in place must not charge another
                // historical table allocation.
                assert_eq!(reader.memory_size(), previous_owned);
            }
            assert!(reader.memory_size() <= owned_limit);
        }
        assert!(
            saw_logical_allocation_decrease,
            "fixture must cross an inferred bucket-size boundary through tombstones"
        );
        assert!(
            saw_rehash,
            "fixture must recycle tombstones without growing the allocation"
        );
        // The contract is bounded ownership through 32 MiB of churn, not
        // an allocator-specific steady-state size or historical charge.
        assert!(reader.memory_size() <= owned_limit);
        assert!(!cache.0.contains_key(&2047));
        assert_eq!(cache.0.get(&8191).unwrap().as_ref(), block.as_ref());
    });
}

/// A cached dictionary block owns exactly its bytes: a block sliced out of a
/// coalesced run (or a whole file-cache object) is copied out so the FIFO
/// cache can never keep the run alive, while a uniquely owned exact-size
/// fetch is stored as-is. Re-inserting a present key hands back the
/// resident block, not the argument.
#[test]
fn cached_dictionary_block_never_aliases_its_fetch_owner() {
    let reader = small_reader();
    let budget = Budget::new(reader.memory_size() + 1024 * 1024);
    with_read_operation(budget, || {
        let mut cache = (
            HashMap::with_hasher(std::hash::BuildHasherDefault::<BucketHasher>::default()),
            std::collections::VecDeque::new(),
            0,
        );
        let run = Bytes::from(vec![7u8; 64 * 1024]);
        let run_span = run.as_ptr() as usize..run.as_ptr() as usize + run.len();
        let cached = reader
            .cache_dict_block(&mut cache, 0, run.slice(4096..8192))
            .unwrap();
        assert_eq!(cached.as_ref(), &run[4096..8192]);
        assert!(
            !run_span.contains(&(cached.as_ptr() as usize)),
            "a cached block must not alias the run it was sliced from"
        );

        let unique = Bytes::from(vec![9u8; 4096]);
        let unique_ptr = unique.as_ptr();
        let cached = reader.cache_dict_block(&mut cache, 4096, unique).unwrap();
        assert_eq!(
            cached.as_ptr(),
            unique_ptr,
            "a unique fetch is stored without a copy"
        );

        let again = reader
            .cache_dict_block(&mut cache, 4096, Bytes::from(vec![1u8; 4096]))
            .unwrap();
        assert_eq!(again.as_ptr(), unique_ptr);
        assert_eq!(cache.0.len(), 2);
    });
}

#[test]
fn dictionary_cache_growth_refuses_before_allocating() {
    let reader = small_reader();
    let before = reader.memory_size();
    let block = Bytes::from_static(b"dictionary block");
    let mut cache = reader.block_cache.lock();
    let error = with_read_operation(
        Budget::new(before + block.len() + RETAINED_BYTES_OVERHEAD),
        || reader.cache_dict_block(&mut cache, 0, block.clone()),
    )
    .unwrap_err();
    assert!(
        anyhow::Error::new(error)
            .chain()
            .any(|cause| cause.is::<OwnershipDenied>())
    );
    assert_eq!(cache.0.capacity(), 0);
    assert_eq!(cache.1.capacity(), 0);
    assert_eq!(reader.memory_size(), before);
    assert_eq!(reader.memory.total.load(Ordering::Acquire), before);
    reader
        .cache_dict_block(&mut cache, 0, block.clone())
        .unwrap();
    assert_eq!(cache.0.get(&0), Some(&block));
}

#[test]
fn native_open_admission_preserves_markers_and_stops_before_fetch() {
    use vortex::{buffer::Alignment, io::VortexReadAt};
    let source = Arc::new(AdvertisedSource {
        size: 4096,
        tail: None,
        tail_reads: AtomicUsize::new(0),
        body_reads: AtomicUsize::new(0),
    });
    let blob = RangedBlob::new(source.clone(), 0..source.size);
    let operation = Budget::new(0);
    let (read, _opening) = with_read_operation(operation.clone(), || blob.opening_read_at());
    // Native IO is polled outside the synchronous TLS scope.
    let error = futures::executor::block_on(read.read_at(0, 4096, Alignment::none())).unwrap_err();
    assert!(
        anyhow::Error::new(error)
            .chain()
            .any(|cause| cause.is::<OwnershipDenied>())
    );
    assert_eq!(source.body_reads.load(Ordering::Acquire), 0);
    operation.cancelled.store(true, Ordering::Release);
    let error = futures::executor::block_on(read.read_at(0, 4096, Alignment::none())).unwrap_err();
    assert!(
        anyhow::Error::new(error)
            .chain()
            .any(|cause| matches!(cause.downcast_ref::<VixError>(), Some(VixError::Cancelled)))
    );
    assert_eq!(source.body_reads.load(Ordering::Acquire), 0);
}

#[test]
fn closing_native_metadata_scope_never_charges_streamed_chunks_cumulatively() {
    use vortex::{buffer::Alignment, io::VortexReadAt};
    let source = crate::BytesRangeSource::new("chunks", Bytes::from_static(b"abcd"));
    let blob = RangedBlob::new(source, 0..4);
    let operation = Budget::new(128 * 1024);
    let (read, opening) = with_read_operation(operation.clone(), || blob.opening_read_at());
    let metadata = futures::executor::block_on(read.read_at(0, 4, Alignment::none())).unwrap();
    let pending = opening.finish();
    drop(metadata);
    drop(pending);
    let calls = operation.calls.load(Ordering::Acquire);
    for _ in 0..1000 {
        let chunk = futures::executor::block_on(read.read_at(0, 4, Alignment::none()))
            .unwrap()
            .unwrap_host();
        assert_eq!(chunk.as_ref(), b"abcd");
    }
    assert_eq!(operation.calls.load(Ordering::Acquire), calls);
}

#[test]
fn cached_native_footer_counts_ownership_without_retaining_creator_operation() {
    use arrow::{array::StructArray, datatypes::Schema};
    use vortex::{
        VortexSessionDefault,
        io::{
            runtime::{BlockingRuntime, single::SingleThreadRuntime},
            session::RuntimeSessionExt,
        },
        session::VortexSession,
    };

    struct RecordingSource {
        bytes: Bytes,
        reads: Mutex<Vec<Range<u64>>>,
    }
    impl VixRangeSource for RecordingSource {
        fn len(&self) -> u64 {
            self.bytes.len() as u64
        }
        fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
            self.reads.lock().push(range.clone());
            futures::future::ready(Ok(self
                .bytes
                .slice(range.start as usize..range.end as usize)))
            .boxed()
        }
    }

    const CHUNKS: usize = 64;
    const ROWS: usize = 256;
    let nested_fields: arrow::datatypes::Fields =
        vec![Field::new("value", DataType::Int64, false)].into();
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("nested", DataType::Struct(nested_fields.clone()), false),
    ]);
    let batches: Vec<_> = (0..CHUNKS)
        .map(|chunk| {
            RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (0..ROWS).map(|row| (chunk * ROWS + row) as i64),
                    )),
                    Arc::new(StructArray::new(
                        nested_fields.clone(),
                        vec![Arc::new(Int64Array::from(vec![chunk as i64; ROWS]))],
                        None,
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    let bytes = crate::container::write_vortex_blob(
        &schema,
        &batches,
        crate::container::addressable_strategy(),
        1,
    )
    .unwrap();
    let source = Arc::new(RecordingSource {
        bytes: bytes.into(),
        reads: Mutex::new(Vec::new()),
    });
    let ranged = RangedBlob::new(source.clone(), 0..source.len());
    let memory = Arc::new(ReaderMemory::new());
    ranged.track_memory(memory.clone());
    let baseline = memory.size();
    let blob = BlobHandle::Ranged(ranged);
    let operation = Budget::new(64 * 1024 * 1024);
    let creator = Arc::downgrade(&operation);
    let (layouts, footer_reads) = with_read_operation(operation.clone(), || {
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let opened = crate::container::open_blob(&runtime, &session, &blob).unwrap();
        let footer_reads = source.reads.lock().clone();
        let mut stack = vec![(opened.footer().layout().clone(), 0)];
        let mut layouts = Vec::new();
        let mut max_depth = 0;
        // Materialize every ViewedLayoutChildren entry and each nested
        // ChunkedLayout's offsets, not just the footer's root view.
        while let Some((layout, depth)) = stack.pop() {
            max_depth = max_depth.max(depth);
            layouts.push(Arc::downgrade(&layout));
            stack.extend(
                layout
                    .children()
                    .unwrap()
                    .into_iter()
                    .map(|child| (child, depth + 1)),
            );
        }
        assert!(
            max_depth >= 3,
            "fixture must contain nested chunked children"
        );
        assert!(
            layouts.len() > CHUNKS * 2,
            "fixture must populate many lazy child entries"
        );
        let rows: usize = opened
            .scan()
            .unwrap()
            .into_array_iter(&runtime)
            .unwrap()
            .map(|array| array.unwrap().len())
            .sum();
        assert_eq!(rows, CHUNKS * ROWS);
        (layouts, footer_reads)
    });
    drop(operation);
    assert!(
        creator.upgrade().is_none(),
        "encoded footer cache retained the creator operation"
    );
    assert!(
        layouts.iter().all(|layout| layout.upgrade().is_none()),
        "native layout children survived their operation"
    );
    let encoded_bytes: usize = footer_reads
        .iter()
        .map(|range| (range.end - range.start) as usize)
        .sum();
    assert!(encoded_bytes > 0);
    // The footer trim keeps only the CONSUMED suffix of what the open read
    // ([min(dtype/layout/stats/footer segment offset), blob end), floored
    // at the postscript-sized suffix), so the retained ownership may be
    // smaller than the fetched bytes — but it must be nonzero (encoded
    // footer counted) and bounded by the fetch plus the range directory.
    assert!(
        memory.size() > baseline,
        "encoded footer omitted from ownership"
    );
    assert!(
        memory.size() <= baseline + encoded_bytes + 4096,
        "retained cache must contain only encoded bytes and a bounded range directory"
    );
    assert_eq!(
        memory.total.load(Ordering::Acquire),
        memory.size(),
        "per-open workspace leaked beyond native file lifetime"
    );

    let retained = memory.size();
    source.reads.lock().clear();
    let error = with_read_operation(Budget::new(retained), || {
        crate::container::blob_arrow_schema_owned(&blob)
    })
    .err()
    .unwrap();
    assert!(
        anyhow::Error::new(error)
            .chain()
            .any(|cause| cause.is::<OwnershipDenied>())
    );
    assert!(
        source.reads.lock().is_empty(),
        "warm admission denial performed physical IO"
    );
    assert_eq!(memory.total.load(Ordering::Acquire), retained);
    with_read_operation(Budget::new(64 * 1024 * 1024), || {
        let batches =
            crate::container::scan_blob(&blob, None, crate::container::RowSelection::All).unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            CHUNKS * ROWS
        );
        let sum: i64 = batches
            .iter()
            .map(|batch| {
                let nested = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                nested
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .sum::<i64>()
            })
            .sum();
        assert_eq!(sum, (ROWS * CHUNKS * (CHUNKS - 1) / 2) as i64);
    });
    // The retained-footer contract: warm scans never re-fetch the RETAINED
    // window (the consumed suffix). Bytes the trim deliberately released
    // (the unused prefix of the initial window) may be re-fetched.
    let retained_window = match &blob {
        BlobHandle::Ranged(ranged) => ranged.retained_footer_window(),
        BlobHandle::Mem(_) => unreachable!("ranged blob"),
    };
    assert!(
        source
            .reads
            .lock()
            .iter()
            .all(|read| { read.end <= retained_window.start || retained_window.end <= read.start }),
        "warm scan fetched bytes already retained by the footer cache"
    );
    assert_eq!(
        memory.size(),
        retained,
        "successive scans grew cached metadata"
    );
    assert_eq!(memory.total.load(Ordering::Acquire), retained);
}
