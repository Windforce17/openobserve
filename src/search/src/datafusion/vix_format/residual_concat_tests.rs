// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use datafusion::execution::memory_pool::GreedyMemoryPool;
use futures::{TryStreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetResult, ListResult, MultipartUpload, ObjectStoreExt, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

use super::*;
use crate::datafusion::peak_memory_pool::PeakMemoryPool;

const MIB: usize = 1024 * 1024;

#[derive(Debug)]
struct MeasuredStore {
    inner: Arc<dyn ObjectStore>,
    reads: AtomicUsize,
    bytes: AtomicUsize,
    fault: AtomicUsize,
}

impl MeasuredStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            fault: AtomicUsize::new(0),
        }
    }
}

impl fmt::Display for MeasuredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MeasuredStore")
    }
}

#[async_trait]
impl ObjectStore for MeasuredStore {
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let head = options.head;
        if !head {
            assert!(
                options.range.is_some(),
                "a scan must not fetch a whole object"
            );
            match self.fault.load(Ordering::Relaxed) {
                1 => {
                    return Err(object_store::Error::Generic {
                        store: "MeasuredStore",
                        source: "injected read failure".into(),
                    });
                }
                2 => panic!("injected read panic"),
                _ => {}
            }
        }
        let result = self.inner.get_opts(path, options).await?;
        if !head {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(
                (result.range.end - result.range.start) as usize,
                Ordering::Relaxed,
            );
        }
        Ok(result)
    }
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

fn pool() -> Arc<PeakMemoryPool> {
    Arc::new(PeakMemoryPool::new(
        Arc::new(GreedyMemoryPool::new(2 * 1024 * MIB)),
        "residual-concat".into(),
    ))
}

fn opener(
    store: Arc<dyn ObjectStore>,
    schema: SchemaRef,
    pool: Arc<PeakMemoryPool>,
    ordered: bool,
) -> VixCoreOpener {
    VixCoreOpener {
        column_bounds: vec![],
        null_rejected_columns: vec![],
        late_source_predicate: None,
        selective_source_range_prepass: true,
        emit_ts_desc: ordered,
        batch_size: 64,
        object_store: store,
        projected_schema: schema,
        timestamp_filter: None,
        memory_pool: Some(pool),
        conversion_width: None,
        metrics: ExecutionPlanMetricsSet::new(),
        partition: 0,
    }
}

async fn wait_released(pool: &PeakMemoryPool) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while pool.reserved() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("scan work must drain and release its reservation");
}

async fn fixture(regions: usize) -> (Arc<dyn ObjectStore>, PartitionedFile, SchemaRef) {
    fixture_with_zone_rows(regions, 64).await
}

async fn fixture_with_zone_rows(
    regions: usize,
    zone_rows: usize,
) -> (Arc<dyn ObjectStore>, PartitionedFile, SchemaRef) {
    // Disjoint regions are stored oldest first, not in global timestamp order.
    let runs: Vec<_> = (0..regions).map(|i| (1000 + i as i64 * 100, 64)).collect();
    let data = tests::build_concat_core_file_with_chunk_rows(&runs, "candidate", zone_rows);
    let docs = VixDocs::open(data.clone()).unwrap();
    if zone_rows == 64 {
        let zones = docs.zone_chunks().unwrap();
        assert_eq!(zones.len(), regions);
        assert!(zones.iter().all(|zone| zone.row_count == 64));
    } else {
        assert_eq!(docs.zone_chunks().unwrap().len(), 1);
    }
    drop(docs);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("residual.vix");
    store.put(&path, data.into()).await.unwrap();
    let meta = store.head(&path).await.unwrap();
    let mut file = PartitionedFile::new(path.to_string(), meta.size);
    file.object_meta = meta;
    // A candidate selection forces the same ranged path as L10.
    file.extensions.insert(VixScanSelection {
        row_ids: Some(Arc::new(RowIdBitmap::all_rows((regions * 64) as u32))),
        native_predicate: Default::default(),
    });
    let schema = Arc::new(Schema::new(vec![Field::new(
        TIMESTAMP_COL_NAME,
        DataType::Int64,
        false,
    )]));
    (store, file, schema)
}

#[tokio::test(flavor = "multi_thread")]
async fn disjoint_regions_do_not_accumulate_reservations() {
    for regions in [1, 25, 50] {
        let (store, file, schema) = fixture(regions).await;
        let pool = pool();
        let scan = opener(store, schema, pool.clone(), true);
        let mut stream = scan.open(file).unwrap().await.unwrap();
        let mut rows = 0;
        while let Some(batch) = stream.try_next().await.unwrap() {
            rows += batch.num_rows();
            // The output channel and consumer can retain retired regions.
            // This bound stays independent of the total region count.
            assert!(pool.reserved() <= 80 * MIB, "{}", pool.reserved());
        }
        drop(stream);
        wait_released(&pool).await;
        assert_eq!(rows, regions * 64);
        assert!(pool.peak_memory() <= 80 * MIB, "{}", pool.peak_memory());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unaligned_zones_preserve_order_and_release_memory() {
    let (store, file, schema) = fixture_with_zone_rows(25, 0).await;
    let pool = pool();
    let scan = opener(store, schema, pool.clone(), true);
    let mut stream = scan.open(file).unwrap().await.unwrap();
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        values.extend_from_slice(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    let expected: Vec<i64> = (0..25)
        .rev()
        .flat_map(|region| (0..64).map(move |row| 1000 + region * 100 - row))
        .collect();
    assert_eq!(values, expected);
    drop(stream);
    wait_released(&pool).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_concat_stream_releases_after_started_work_drains() {
    let (store, file, schema) = fixture(25).await;
    let pool = pool();
    let scan = opener(store, schema, pool.clone(), true);
    let mut stream = scan.open(file).unwrap().await.unwrap();
    let batch = stream.try_next().await.unwrap().unwrap();
    assert!(pool.reserved() > 0);
    drop(batch);
    drop(stream);
    wait_released(&pool).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concat_open_error_releases_reservation() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("corrupt.vix");
    store
        .put(
            &path,
            bytes::Bytes::from_static(b"not a vortex file").into(),
        )
        .await
        .unwrap();
    let meta = store.head(&path).await.unwrap();
    let mut file = PartitionedFile::new(path.to_string(), meta.size);
    file.object_meta = meta;
    let schema = Arc::new(Schema::new(vec![Field::new(
        TIMESTAMP_COL_NAME,
        DataType::Int64,
        false,
    )]));
    let pool = pool();
    let scan = opener(store, schema, pool.clone(), true);
    let mut stream = scan.open(file).unwrap().await.unwrap();
    assert!(stream.try_next().await.is_err());
    drop(stream);
    wait_released(&pool).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concat_range_error_and_panic_release_reservation() {
    for fault in [1, 2] {
        let (store, file, schema) = fixture(25).await;
        let store = Arc::new(MeasuredStore::new(store));
        store.fault.store(fault, Ordering::Relaxed);
        let pool = pool();
        let scan = opener(store, schema, pool.clone(), true);
        let mut stream = scan.open(file).unwrap().await.unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(10), stream.try_next())
            .await
            .expect("read error or panic must reach the stream")
            .unwrap_err();
        let expected = if fault == 1 {
            "injected read"
        } else {
            "vix read cancelled"
        };
        assert!(error.to_string().contains(expected), "{error}");
        drop(stream);
        wait_released(&pool).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn downstream_slice_keeps_region_charge_after_stream_drop() {
    let (store, file, schema) = fixture(25).await;
    let pool = pool();
    let scan = opener(store, schema, pool.clone(), true);
    let mut stream = scan.open(file).unwrap().await.unwrap();
    let batch = stream.try_next().await.unwrap().unwrap();
    let slice = batch.slice(0, 1);
    drop(batch);
    drop(stream);
    // After the stream and the scan work are gone, the file workspace (the
    // opener estimate and reader high water) is released. Only the charge
    // of the retained slice stays.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while pool.reserved() >= MIB {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the file workspace must be released after the scan ends");
    assert!(pool.reserved() > 0, "the retained slice keeps its charge");
    assert_eq!(slice.num_rows(), 1);
    drop(slice);
    wait_released(&pool).await;
}

fn operation(pool: Arc<PeakMemoryPool>) -> Arc<CoreScanOperation> {
    let metrics = ExecutionPlanMetricsSet::new();
    let memory_pool: Arc<dyn MemoryPool> = pool;
    let reservation = MemoryConsumer::new("merge-lifecycle").register(&memory_pool);
    let reservation = Arc::new(parking_lot::Mutex::new(Some(reservation)));
    let live = Arc::new(AtomicUsize::new(0));
    Arc::new(CoreScanOperation {
        io: ReadOperation::new(Arc::new(FetchStats::default()), None),
        reservation: Arc::clone(&reservation),
        _workspace: Arc::new(FileWorkspace {
            reservation,
            live: Arc::clone(&live),
        }),
        reader_high_water: parking_lot::Mutex::new(0),
        options: parking_lot::Mutex::new(Default::default()),
        downstream_wait_ns: MetricBuilder::new(&metrics).counter("wait", 0),
        live_owned_bytes: live,
        peak_owned_bytes: AtomicUsize::new(0),
        peak_reserved_bytes: MetricBuilder::new(&metrics).counter("peak", 0),
        metrics,
        partition: 0,
        location: "merge-lifecycle".into(),
    })
}

#[test]
fn decoded_charge_follows_last_slice_not_region() {
    use vortex_index::VixReadOperation;
    let pool = pool();
    let op = operation(pool.clone());
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef,
    )])
    .unwrap();
    let bytes = batch_slice_memory_size(&batch).unwrap();
    let batch = op.own_merge_batch(batch, Arc::new(())).unwrap();
    assert_eq!(pool.reserved(), bytes);
    let retained = batch.slice(0, 1).column(0).clone();
    drop(batch);
    drop(op);
    assert_eq!(pool.reserved(), bytes);
    drop(retained);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn retained_output_releases_the_file_workspace_when_the_scan_ends() {
    use vortex_index::VixReadOperation;
    let pool = pool();
    let op = operation(pool.clone());
    // The reader high water stands in for the per-file workspace.
    op.check_memory(64 * MIB).unwrap();
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef,
    )])
    .unwrap();
    let bytes = batch_slice_memory_size(&batch).unwrap();
    let batch = op.own_merge_batch(batch, Arc::new(())).unwrap();
    assert_eq!(pool.reserved(), 64 * MIB + bytes);
    drop(op);
    assert_eq!(pool.reserved(), bytes, "only the output charge remains");
    drop(batch);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn exhausted_disjoint_regions_release_before_the_next_region_opens() {
    struct Region(Arc<AtomicUsize>);
    impl Drop for Region {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let runs: Vec<_> = (0..50).map(|i| (1000 + i * 100, 64)).collect();
    let docs = VixDocs::open(tests::build_concat_core_file_with_chunk_rows(
        &runs,
        "candidate",
        64,
    ))
    .unwrap();
    assert_eq!(docs.zone_chunks().unwrap().len(), 50);
    let live = Arc::new(AtomicUsize::new(0));
    let pool = pool();
    let op = operation(pool.clone());
    let schema = Arc::new(Schema::new(vec![Field::new(
        TIMESTAMP_COL_NAME,
        DataType::Int64,
        false,
    )]));
    let mut output = Vec::new();
    vortex_index::with_read_operation(op.clone(), || {
        scan_core_docs_merged(
            docs,
            &schema,
            None,
            None,
            &[],
            64,
            Some(&op),
            &mut || {
                assert_eq!(
                    live.fetch_add(1, Ordering::SeqCst),
                    0,
                    "previous region must be exhausted"
                );
                Ok(Region(live.clone()))
            },
            &mut |batch| {
                assert!(op.live_owned_bytes.load(Ordering::Relaxed) < 16 * 1024);
                output.extend_from_slice(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values(),
                );
                Ok(())
            },
        )
        .unwrap();
    });
    let expected: Vec<_> = (0..50)
        .rev()
        .flat_map(|i| (0..64).map(move |row| 1000 + i * 100 - row))
        .collect();
    assert_eq!(output, expected);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(op.live_owned_bytes.load(Ordering::Relaxed), 0);
    drop(op);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn projection_charges_only_new_buffers_and_keeps_input_owners() {
    use vortex_index::VixReadOperation;
    let pool = pool();
    let op = operation(pool.clone());
    let input = RecordBatch::try_from_iter([(
        "text",
        Arc::new(StringArray::from(vec!["a long non-inline string"; 100])) as ArrayRef,
    )])
    .unwrap();
    let input = op.own_merge_batch(input, Arc::new(())).unwrap();
    let before = pool.reserved();
    let identity = own_projection_batch(&input, input.slice(1, 10), &op).unwrap();
    assert_eq!(
        pool.reserved(),
        before,
        "identity projection must not charge the same values twice"
    );
    let view = arrow::compute::cast(input.column(0), &DataType::Utf8View).unwrap();
    let output = RecordBatch::try_from_iter([("view", view)]).unwrap();
    let output = own_projection_batch(&input, output, &op).unwrap();
    assert_eq!(
        pool.reserved() - before,
        100 * 16,
        "cast allocates view records but shares string values"
    );
    drop(input);
    drop(identity);
    drop(op);
    assert!(pool.reserved() > 0);
    drop(output);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn external_parent_is_shared_and_logical_slice_is_measured() {
    use vortex_index::VixReadOperation;
    let pool = pool();
    let op = operation(pool.clone());
    let original = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef,
    )])
    .unwrap();
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let external = own_output_batch(original, owner).unwrap().slice(0, 2);
    let pointer = external.column(0).to_data().buffers()[0].as_ptr();
    let expected = batch_slice_memory_size(&external).unwrap();
    assert!(expected < 8192, "charge the logical slice, not its parent");
    let batch = op.own_merge_batch(external, Arc::new(())).unwrap();
    assert_eq!(batch.column(0).to_data().buffers()[0].as_ptr(), pointer);
    assert_eq!(pool.reserved(), expected);
    assert!(
        weak.upgrade().is_some(),
        "admission must not copy external backing"
    );
    assert!(pool.reserved() >= 2 * std::mem::size_of::<i64>());
    drop(op);
    assert!(pool.reserved() > 0);
    let retained = batch.slice(0, 1).column(0).clone();
    drop(batch);
    assert_eq!(pool.reserved(), expected);
    drop(retained);
    assert_eq!(pool.reserved(), 0);
    assert!(weak.upgrade().is_none());
}

#[test]
fn view_values_are_included_in_logical_charge() {
    let value = "x".repeat(4096);
    let array = arrow::array::StringViewArray::from(vec![Some(value.as_str()); 10]);
    let batch = RecordBatch::try_from_iter([("value", Arc::new(array) as ArrayRef)]).unwrap();
    let full = batch_slice_memory_size(&batch).unwrap();
    let slice = batch_slice_memory_size(&batch.slice(0, 2)).unwrap();
    assert!(slice >= 2 * (4096 + 16));
    assert!(full >= 10 * (4096 + 16));
    assert!(slice < full);
}

#[test]
fn shared_view_backing_is_charged_once() {
    // Dictionary expansion produces many views over one shared value.
    let value = "x".repeat(4096);
    let mut builder = arrow::array::StringViewBuilder::new().with_deduplicate_strings();
    for _ in 0..1000 {
        builder.append_value(&value);
    }
    let array = builder.finish();
    let backing: usize = array.data_buffers().iter().map(|buffer| buffer.len()).sum();
    assert_eq!(backing, 4096, "the builder stores the value once");
    let batch = RecordBatch::try_from_iter([("value", Arc::new(array) as ArrayRef)]).unwrap();
    let bytes = batch_slice_memory_size(&batch).unwrap();
    assert!(bytes >= 1000 * 16 + 4096, "{bytes}");
    assert!(
        bytes < 64 * 1024,
        "repeated views must not multiply the backing: {bytes}"
    );
}

#[test]
fn nested_view_values_are_charged() {
    let values: Vec<String> = (0..10).map(|i| format!("{i}").repeat(4096)).collect();
    let child = arrow::array::StringViewArray::from_iter_values(values.iter());
    let array = arrow::array::StructArray::from(vec![(
        Arc::new(Field::new("text", DataType::Utf8View, false)),
        Arc::new(child) as ArrayRef,
    )]);
    let batch = RecordBatch::try_from_iter([("nested", Arc::new(array) as ArrayRef)]).unwrap();
    let bytes = batch_slice_memory_size(&batch).unwrap();
    assert!(
        bytes >= 10 * 4096,
        "nested out-of-line values must be charged: {bytes}"
    );
}

fn multi_batch_concat() -> bytes::Bytes {
    use vortex_index::{VixWriter, VixWriterOptions};
    let schema = Arc::new(Schema::new(vec![Field::new(
        TIMESTAMP_COL_NAME,
        DataType::Int64,
        false,
    )]));
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            concat_row_order: true,
            docs_chunk_bytes: MIB,
            docs_chunk_max_rows: 64,
            ..Default::default()
        },
        false,
    );
    let values: Vec<_> = (0..3)
        .flat_map(|region| (0..512).map(move |row| 1000 + region - row))
        .collect();
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.clone()))]).unwrap();
    let payload = "x".repeat(16 * 1024);
    let sources: Vec<_> = values
        .iter()
        .map(|ts| format!(r#"{{"_timestamp":{ts},"payload":"{payload}"}}"#))
        .collect();
    writer
        .push_batch_with_source(&batch, &StringArray::from(sources), None)
        .unwrap();
    bytes::Bytes::from(writer.finish().unwrap().0)
}

#[test]
fn merged_exit_paths_release_pool_and_join_blocked_producers() {
    use vortex_index::VixReadOperation;
    struct FreshSource(bytes::Bytes);
    impl vortex_index::VixRangeSource for FreshSource {
        fn len(&self) -> u64 {
            self.0.len() as u64
        }
        fn fetch(
            &self,
            range: std::ops::Range<u64>,
        ) -> futures::future::BoxFuture<'static, anyhow::Result<bytes::Bytes>> {
            let bytes =
                bytes::Bytes::copy_from_slice(&self.0[range.start as usize..range.end as usize]);
            futures::future::ready(Ok(bytes)).boxed()
        }
    }
    struct CountedOperation {
        inner: Arc<CoreScanOperation>,
        decoded: AtomicUsize,
        fail_decode: bool,
    }
    impl VixReadOperation for CountedOperation {
        fn is_cancelled(&self) -> bool {
            self.inner.is_cancelled()
        }
        fn check_memory(&self, bytes: usize) -> vortex_index::Result<()> {
            self.inner.check_memory(bytes)
        }
        fn reserve_io(&self, bytes: usize) -> vortex_index::Result<Option<Arc<dyn Send + Sync>>> {
            self.inner.reserve_io(bytes)
        }
        fn own_merge_batch(
            &self,
            batch: RecordBatch,
            region: Arc<dyn Send + Sync>,
        ) -> vortex_index::Result<RecordBatch> {
            let batch = self.inner.own_merge_batch(batch, region)?;
            let decoded = self.decoded.fetch_add(1, Ordering::Release) + 1;
            if self.fail_decode && decoded == 3 {
                return Err(vortex_index::VixError::Callback(anyhow::anyhow!(
                    "injected producer failure"
                )));
            }
            Ok(batch)
        }
    }
    let data = multi_batch_concat();
    for mode in [
        "normal",
        "limit",
        "panic",
        "error",
        "cancel",
        "admission",
        "decode",
    ] {
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
        let data = data.clone();
        std::thread::spawn(move || {
            let pool = pool();
            let op = operation(pool.clone());
            let counted = Arc::new(CountedOperation {
                inner: op.clone(),
                decoded: AtomicUsize::new(0),
                fail_decode: mode == "decode",
            });
            let mut cancellation = Some(op.io.owner_guard());
            let opened = AtomicUsize::new(0);
            let schema = Arc::new(Schema::new(vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new(SOURCE_COL_NAME, DataType::Utf8, true),
            ]));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                vortex_index::with_read_operation(counted.clone(), || {
                    scan_core_docs_merged(
                        VixDocs::open_ranged(Arc::new(FreshSource(data))).unwrap(),
                        &schema,
                        None,
                        None,
                        &[],
                        1,
                        Some(&op),
                        &mut || {
                            if opened.fetch_add(1, Ordering::AcqRel) == 1 && mode == "admission" {
                                anyhow::bail!("injected admission failure");
                            }
                            Ok(())
                        },
                        &mut |_| {
                            if matches!(mode, "normal" | "decode" | "admission") {
                                return Ok(());
                            }
                            // At least one producer has reached its third batch:
                            // cursor + channel slot + blocked send are all occupied.
                            let start = Instant::now();
                            while counted.decoded.load(Ordering::Acquire)
                                < opened.load(Ordering::Acquire) * 2 + 1
                            {
                                assert!(start.elapsed() < std::time::Duration::from_secs(5));
                                std::thread::yield_now();
                            }
                            std::thread::sleep(std::time::Duration::from_millis(10));
                            match mode {
                                "panic" => panic!("injected callback panic"),
                                "error" => anyhow::bail!("injected callback failure"),
                                "limit" => anyhow::bail!("consumer has reached LIMIT"),
                                "cancel" => {
                                    drop(cancellation.take());
                                    vortex_index::check_read_cancelled()?;
                                    unreachable!("operation must be cancelled");
                                }
                                _ => unreachable!(),
                            }
                        },
                    )
                })
            }));
            assert!(match result {
                Err(_) => mode == "panic",
                Ok(result) => result.is_ok() == (mode == "normal"),
            });
            assert!(
                *op.reader_high_water.lock() > 0,
                "producer must retain reader admission context"
            );
            assert_eq!(op.live_owned_bytes.load(Ordering::Relaxed), 0);
            drop(cancellation);
            drop(counted);
            drop(op);
            assert_eq!(pool.reserved(), 0);
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("exit must release receivers before joining producers");
    }
}
