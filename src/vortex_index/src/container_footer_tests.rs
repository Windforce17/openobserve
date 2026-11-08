// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::StringArray;
use futures::FutureExt;

use super::*;
use crate::source::{VixReadOperation, with_read_operation};

struct Operation {
    cancelled: AtomicBool,
    limit: AtomicUsize,
    last: AtomicUsize,
}
impl Operation {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            cancelled: AtomicBool::new(false),
            limit: AtomicUsize::new(usize::MAX),
            last: AtomicUsize::new(0),
        })
    }
}
impl VixReadOperation for Operation {
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
    fn check_memory(&self, bytes: usize) -> Result<()> {
        self.last.store(bytes, Ordering::Release);
        if bytes > self.limit.load(Ordering::Acquire) {
            return Err(VixError::Malformed("footer test budget denied".into()));
        }
        Ok(())
    }
}
struct Source {
    bytes: Bytes,
    reads: Arc<AtomicUsize>,
}
impl VixRangeSource for Source {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn fetch(&self, range: std::ops::Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        futures::future::ready(Ok(self
            .bytes
            .slice(range.start as usize..range.end as usize)))
        .boxed()
    }
}
fn fixture(columns: usize) -> (BlobHandle, Arc<AtomicUsize>) {
    let schema = Schema::new(
        (0..columns)
            .map(|i| Field::new(format!("col{i}"), DataType::Int64, true))
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        (0..columns)
            .map(|i| {
                Arc::new(Int64Array::from(vec![Some(i as i64), None, Some(7)])) as ArrowArrayRef
            })
            .collect(),
    )
    .unwrap();
    let bytes = write_vortex_blob(&schema, &[batch], addressable_strategy(), 1).unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(Source {
        bytes: bytes.into(),
        reads: Arc::clone(&reads),
    });
    let ranged = RangedBlob::new(source.clone(), 0..source.len());
    ranged.track_memory(Arc::new(crate::reader::ReaderMemory::new()));
    (BlobHandle::Ranged(ranged), reads)
}
fn reopen(blob: &BlobHandle) -> OpenedBlob {
    let mut setup = NativeReadSetup::new().unwrap();
    let runtime = SingleThreadRuntime::default();
    let session = setup.session(runtime.handle());
    let mut opened = open_blob(&runtime, &session, blob).unwrap();
    drop(setup.memory_scope.take());
    opened._runtime = Some(runtime);
    opened._setup = Some(setup);
    opened
}

#[test]
fn schema_footer_reuses_native_layout_in_same_operation() {
    let (blob, reads) = fixture(512);
    with_read_operation(Operation::new(), || {
        let (_, first) = blob_arrow_schema_owned(&blob).unwrap();
        let layout = Arc::clone(first.footer().layout());
        drop(first);
        let before = reads.load(Ordering::Acquire);
        let second = reopen(&blob);
        assert!(Arc::ptr_eq(&layout, second.footer().layout()));
        assert_eq!(reads.load(Ordering::Acquire), before);
        assert_eq!(second.row_count(), 3);
    });
}

#[test]
fn schema_footer_does_not_cross_operation_boundary() {
    let (blob, _) = fixture(3);
    let layout = with_read_operation(Operation::new(), || {
        let (_, first) = blob_arrow_schema_owned(&blob).unwrap();
        Arc::clone(first.footer().layout())
    });
    with_read_operation(Operation::new(), || {
        assert!(!Arc::ptr_eq(&layout, reopen(&blob).footer().layout()));
    });
}

#[test]
fn leaf_planning_reuses_footer_once_with_operation_admission() {
    let (blob, reads) = fixture(64);
    with_read_operation(Operation::new(), || {
        let before = native_open_counts();
        let leaves = blob_column_leaves(&blob, "col0").unwrap().unwrap();
        assert!(!leaves.is_empty());
        let planned = native_open_counts();
        assert_eq!(planned.1, before.1 + 1);
        let cached_reads = reads.load(Ordering::Acquire);
        let first = reopen(&blob);
        assert_eq!(native_open_counts().1, planned.1);
        assert_eq!(reads.load(Ordering::Acquire), cached_reads);
        let second = reopen(&blob);
        assert_eq!(native_open_counts().1, planned.1 + 1);
        assert!(!Arc::ptr_eq(
            first.footer().layout(),
            second.footer().layout()
        ));
    });
    // Neither the operation-local slot nor its metadata runtime can escape.
    assert!(NATIVE_SCHEMA_FOOTER.with(|slot| slot.borrow().is_none()));
}

#[test]
fn read_registry_keeps_relocated_stats_session() {
    use vortex::session::SessionExt;

    let setup = NativeReadSetup::new().unwrap();
    let first = setup.registries.session();
    let second = setup.registries.session();
    assert!(
        first
            .get_opt::<vortex::expr::stats::session::StatsSession>()
            .is_some()
    );
    assert!(
        second
            .get_opt::<vortex::expr::stats::session::StatsSession>()
            .is_some()
    );
    assert!(
        first
            .get_opt::<vortex::io::session::RuntimeSession>()
            .is_none()
    );
    assert!(
        second
            .get_opt::<vortex::io::session::RuntimeSession>()
            .is_none()
    );
}

#[test]
fn read_template_is_built_once_and_concurrent_reads_do_not_mutate_registries() {
    let (blob, _) = fixture(3);
    let blob = Arc::new(blob);
    let setup = NativeReadSetup::new().unwrap();
    let template = setup.registries.template;
    let arrays = template.arrays.registry().snapshot();
    let dtypes = template.dtypes.registry().snapshot();
    let layouts = template.layouts.registry().snapshot();
    let scalars = template.scalars.registry().snapshot();
    drop(setup);
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let blob = blob.clone();
            std::thread::spawn(move || {
                with_read_operation(Operation::new(), || {
                    let setup = NativeReadSetup::new().unwrap();
                    let (schema, file) = blob_arrow_schema_owned(&blob).unwrap();
                    assert_eq!(schema.fields().len(), 3);
                    assert_eq!(file.row_count(), 3);
                    let batches = scan_blob(&blob, Some(&["col0"]), RowSelection::All).unwrap();
                    assert_eq!(batches.len(), 1);
                    assert_eq!(
                        batches[0].column(0).to_data(),
                        Int64Array::from(vec![Some(0), None, Some(7)]).to_data()
                    );
                    std::ptr::from_ref(setup.registries.template) as usize
                })
            })
        })
        .collect();
    for thread in threads {
        assert_eq!(
            thread.join().unwrap(),
            std::ptr::from_ref(template) as usize
        );
    }
    assert_eq!(NATIVE_TEMPLATE_BUILDS.load(Ordering::Relaxed), 1);
    // Pointer equality, not just equal entries: no COW publication occurred.
    assert!(Arc::ptr_eq(&arrays, &template.arrays.registry().snapshot()));
    assert!(Arc::ptr_eq(&dtypes, &template.dtypes.registry().snapshot()));
    assert!(Arc::ptr_eq(
        &layouts,
        &template.layouts.registry().snapshot()
    ));
    assert!(Arc::ptr_eq(
        &scalars,
        &template.scalars.registry().snapshot()
    ));
}

#[test]
fn read_template_sessions_keep_runtime_memory_and_late_defaults_private() {
    use vortex::session::SessionExt;

    let setup = NativeReadSetup::new().unwrap();
    let first_runtime = SingleThreadRuntime::default();
    let second_runtime = SingleThreadRuntime::default();
    let first = setup.session(first_runtime.handle());
    let second = setup.session(second_runtime.handle());
    first.allow_unknown();
    assert!(first.allows_unknown());
    assert!(!second.allows_unknown());
    assert!(!std::ptr::eq(
        &*first.get::<vortex::io::session::RuntimeSession>(),
        &*second.get::<vortex::io::session::RuntimeSession>()
    ));
    assert!(!std::ptr::eq(
        &*first.get::<vortex::array::memory::MemorySession>(),
        &*second.get::<vortex::array::memory::MemorySession>()
    ));
    drop(second);
    drop(second_runtime);
    assert_eq!(first_runtime.block_on(async { 7 }), 7);
    let metadata = setup.registries.session();
    assert!(!metadata.allows_unknown());
    assert!(
        metadata
            .get_opt::<vortex::io::session::RuntimeSession>()
            .is_none()
    );
    assert!(
        metadata
            .get_opt::<vortex::array::memory::MemorySession>()
            .is_none()
    );
}

#[test]
fn read_template_preserves_operation_admission_and_releases_each_owner() {
    let first = Operation::new();
    let second = Operation::new();
    let memory = Arc::new(crate::reader::ReaderMemory::new());
    let other_memory = Arc::new(crate::reader::ReaderMemory::new());
    let baseline = with_read_operation(first.clone(), || {
        memory.reserve(0).unwrap();
        first.last.load(Ordering::Acquire)
    });
    let expected = baseline + native_registry_workspace().unwrap() + NATIVE_SESSION_WORKSPACE;
    with_read_operation(first.clone(), || {
        let _scope = crate::source::enter_reader_memory(Arc::clone(&memory));
        let setup = NativeReadSetup::new().unwrap();
        memory.reserve(0).unwrap();
        assert_eq!(first.last.load(Ordering::Acquire), expected);
        with_read_operation(second.clone(), || {
            let _scope = crate::source::enter_reader_memory(Arc::clone(&other_memory));
            let inner = NativeReadSetup::new().unwrap();
            assert!(!Arc::ptr_eq(
                &setup.registries.memory,
                &inner.registries.memory
            ));
            assert_eq!(second.last.load(Ordering::Acquire), expected);
            second.cancelled.store(true, Ordering::Release);
            assert!(matches!(NativeReadSetup::new(), Err(VixError::Cancelled)));
        });
        memory.reserve(0).unwrap();
        assert_eq!(first.last.load(Ordering::Acquire), expected);
        crate::check_read_cancelled().unwrap();
        drop(setup);
    });
    second.cancelled.store(false, Ordering::Release);
    for (operation, memory) in [(first, memory), (second, other_memory)] {
        with_read_operation(operation.clone(), || {
            memory.reserve(0).unwrap();
            assert_eq!(operation.last.load(Ordering::Acquire), baseline);
        });
    }
}

#[test]
fn read_template_cached_plugins_do_not_bypass_budget_or_cancellation() {
    drop(NativeReadSetup::new().unwrap());
    for cancelled in [false, true] {
        let operation = Operation::new();
        operation.cancelled.store(cancelled, Ordering::Release);
        if !cancelled {
            operation.limit.store(0, Ordering::Release);
        }
        with_read_operation(operation, || {
            let result = NativeReadSetup::new();
            if cancelled {
                assert!(matches!(result, Err(VixError::Cancelled)));
            } else {
                assert!(
                    matches!(result, Err(VixError::Malformed(message)) if message == "footer test budget denied")
                );
            }
            assert!(NATIVE_READ_REGISTRIES.with(|slot| slot.borrow().is_none()));
        });
    }
    with_read_operation(Operation::new(), || {
        assert!(NativeReadSetup::new().is_ok());
    });
}

#[test]
fn schema_footer_keeps_original_admission_until_consumed() {
    let (blob, _) = fixture(64);
    let BlobHandle::Ranged(ranged) = &blob else {
        unreachable!()
    };
    let memory = ranged.reader_memory();
    let operation = Operation::new();
    with_read_operation(operation.clone(), || {
        let (_, first) = blob_arrow_schema_owned(&blob).unwrap();
        memory.reserve(0).unwrap();
        let during = operation.last.load(Ordering::Acquire);
        drop(first);
        memory.reserve(0).unwrap();
        let after = operation.last.load(Ordering::Acquire);
        assert!(after >= during.saturating_sub(NATIVE_SESSION_WORKSPACE));
        let second = reopen(&blob);
        memory.reserve(0).unwrap();
        let consumed = operation.last.load(Ordering::Acquire);
        assert!(consumed >= after);
        drop(second);
        memory.reserve(0).unwrap();
        assert!(operation.last.load(Ordering::Acquire) < after);
    });
}

#[test]
fn schema_footer_is_consumed_once_and_not_retained_without_operation() {
    let (blob, _) = fixture(3);
    let (_, first) = blob_arrow_schema_owned(&blob).unwrap();
    assert!(!Arc::ptr_eq(
        first.footer().layout(),
        reopen(&blob).footer().layout()
    ));
    with_read_operation(Operation::new(), || {
        let (_, schema) = blob_arrow_schema_owned(&blob).unwrap();
        let second = reopen(&blob);
        assert!(Arc::ptr_eq(
            schema.footer().layout(),
            second.footer().layout()
        ));
        assert!(!Arc::ptr_eq(
            second.footer().layout(),
            reopen(&blob).footer().layout()
        ));
    });
}

#[test]
fn schema_footer_identity_rejects_new_generation_and_other_files() {
    let (blob, _) = fixture(3);
    let (other, _) = fixture(7);
    let BlobHandle::Ranged(ranged) = &blob else {
        unreachable!()
    };
    let generation = RangedBlob::new(Arc::clone(&ranged.source), ranged.range.clone());
    generation.track_memory(ranged.reader_memory());
    let generation = BlobHandle::Ranged(generation);
    with_read_operation(Operation::new(), || {
        let (_, schema) = blob_arrow_schema_owned(&blob).unwrap();
        assert!(!Arc::ptr_eq(
            schema.footer().layout(),
            reopen(&generation).footer().layout()
        ));
        let (_, schema) = blob_arrow_schema_owned(&blob).unwrap();
        assert_eq!(
            vortex::arrow::ArrowSession::default()
                .to_arrow_schema(reopen(&other).dtype())
                .unwrap()
                .fields()
                .len(),
            7
        );
        // Even a non-matching open must evict the old slot.
        assert!(!Arc::ptr_eq(
            schema.footer().layout(),
            reopen(&blob).footer().layout()
        ));
    });
}

#[test]
fn schema_footer_nested_scope_and_unwind_restore_outer_slot() {
    let (blob, _) = fixture(3);
    with_read_operation(Operation::new(), || {
        let (_, first) = blob_arrow_schema_owned(&blob).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_read_operation(Operation::new(), || {
                let (_, inner) = blob_arrow_schema_owned(&blob).unwrap();
                assert!(!Arc::ptr_eq(
                    first.footer().layout(),
                    inner.footer().layout()
                ));
                panic!("exercise scope restoration");
            });
        }));
        assert!(result.is_err());
        assert!(Arc::ptr_eq(
            first.footer().layout(),
            reopen(&blob).footer().layout()
        ));
    });
}

#[test]
fn schema_footer_cancelled_or_refused_open_never_reads_or_scans() {
    for cancel in [false, true] {
        let (blob, reads) = fixture(64);
        let operation = Operation::new();
        with_read_operation(operation.clone(), || {
            let (_, schema) = blob_arrow_schema_owned(&blob).unwrap();
            drop(schema);
            let setup = NativeReadSetup::new().unwrap();
            let runtime = SingleThreadRuntime::default();
            let session = setup.session(runtime.handle());
            let before = reads.load(Ordering::Acquire);
            if cancel {
                operation.cancelled.store(true, Ordering::Release);
            } else {
                operation.limit.store(0, Ordering::Release);
            }
            let error = open_blob(&runtime, &session, &blob).err().unwrap();
            if cancel {
                assert!(matches!(error, VixError::Cancelled));
            } else {
                assert!(error.to_string().contains("footer test budget denied"));
            }
            assert_eq!(reads.load(Ordering::Acquire), before);
        });
        let batches = with_read_operation(Operation::new(), || {
            scan_blob(&blob, None, RowSelection::All)
        })
        .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }
}

#[test]
fn schema_footer_replacement_and_scope_exit_release_original_memory() {
    let (first, _) = fixture(64);
    let (second, _) = fixture(3);
    let BlobHandle::Ranged(ranged) = &first else {
        unreachable!()
    };
    // Populate only the immutable encoded footer before taking the reference.
    drop(reopen(&first));
    let memory = ranged.reader_memory();
    let operation = Operation::new();
    with_read_operation(operation.clone(), || {
        memory.reserve(0).unwrap();
    });
    let reference = operation.last.load(Ordering::Acquire);
    with_read_operation(operation.clone(), || {
        let (_, opened) = blob_arrow_schema_owned(&first).unwrap();
        drop(opened);
        memory.reserve(0).unwrap();
        assert!(operation.last.load(Ordering::Acquire) > reference);
        let (_, opened) = blob_arrow_schema_owned(&second).unwrap();
        drop(opened);
        memory.reserve(0).unwrap();
        assert_eq!(operation.last.load(Ordering::Acquire), reference);
    });
    with_read_operation(operation.clone(), || {
        memory.reserve(0).unwrap();
    });
    assert_eq!(operation.last.load(Ordering::Acquire), reference);
}

#[test]
fn schema_footer_wide_scan_matches_reparsed_layout() {
    let (blob, _) = fixture(2048);
    let expected = scan_blob(&blob, None, RowSelection::All).unwrap();
    for threads in [0, 2] {
        with_read_operation(Operation::new(), || {
            let (_, opened) = blob_arrow_schema_owned(&blob).unwrap();
            drop(opened);
            let mut actual = Vec::new();
            scan_blob_streaming(
                &blob,
                None,
                RowSelection::All,
                None,
                None,
                threads,
                &mut |batch| {
                    actual.push(batch);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(actual, expected);
        });
    }
}

#[test]
fn schema_footer_concat_regions_match_reparsed_complete_results() {
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![
                500, 400, 300, 200, 450, 400, 350, 1000, 100,
            ])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("é"),
                Some("界"),
                Some("b"),
                None,
                Some("c"),
                Some("d"),
                Some("e"),
            ])),
        ],
    )
    .unwrap();
    let mut writer = crate::VixWriter::new(
        &schema,
        crate::VixWriterOptions {
            concat_row_order: true,
            docs_chunk_bytes: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; 9]), None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let read = |limit| {
        let docs = crate::VixDocs::open_ranged(Arc::new(Source {
            bytes: Bytes::from(data.clone()),
            reads: Arc::new(AtomicUsize::new(0)),
        }))
        .unwrap();
        assert_eq!(docs.ts_desc_row_ranges(), Some(vec![0..4, 4..7, 7..9]));
        let mut out = Vec::new();
        docs.scan_docs_ts_desc_merged(
            Some(&["_timestamp".to_string(), "tag".to_string()]),
            None,
            None,
            &[],
            limit,
            &mut || Ok(()),
            &mut |batch| {
                out.push(batch);
                Ok(())
            },
        )
        .unwrap();
        out
    };
    for limit in [None, Some(2), Some(5)] {
        let expected = read(limit);
        let actual = with_read_operation(Operation::new(), || read(limit));
        assert_eq!(actual, expected);
        assert_eq!(
            actual.iter().map(RecordBatch::num_rows).sum::<usize>(),
            limit.unwrap_or(9) as usize
        );
    }
}

#[test]
fn schema_footer_rebinds_io_and_preserves_small_initial_read_floor() {
    struct BindingSource {
        inner: Arc<dyn VixRangeSource>,
        bindings: Arc<AtomicUsize>,
    }
    impl VixRangeSource for BindingSource {
        fn len(&self) -> u64 {
            self.inner.len()
        }
        fn for_current_operation(&self) -> Option<Arc<dyn VixRangeSource>> {
            self.bindings.fetch_add(1, Ordering::AcqRel);
            Some(Arc::clone(&self.inner))
        }
        fn fetch(&self, _: std::ops::Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
            panic!("scan must bind a fresh IO source");
        }
    }
    let (blob, _) = fixture(512);
    let BlobHandle::Ranged(ranged) = blob else {
        unreachable!()
    };
    let bindings = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(BindingSource {
        inner: ranged.source,
        bindings: Arc::clone(&bindings),
    });
    let blob = BlobHandle::Ranged(RangedBlob::with_initial_read(source, ranged.range, 1));
    with_read_operation(Operation::new(), || {
        let (_, schema) = blob_arrow_schema_owned(&blob).unwrap();
        assert_eq!(bindings.load(Ordering::Acquire), 1);
        drop(schema);
        let result = scan_blob(&blob, Some(&["col0", "col511"]), RowSelection::All).unwrap();
        assert_eq!(bindings.load(Ordering::Acquire), 2);
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let values = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(511), None, Some(7)]
        );
    });
}

#[test]
fn write_policy_is_pinned_to_079_compatible_components() {
    use vortex::editions::{CORE_2025_10_0, ComponentKind, EditionSessionExt};
    let session = legacy_write_session(VortexSession::default()).unwrap();
    assert_eq!(session.enabled_editions().editions(), vec![CORE_2025_10_0]);
    let arrays: std::collections::HashSet<_> = session
        .enabled_component_ids(ComponentKind::Array)
        .into_iter()
        .collect();
    assert_eq!(arrays, *LEGACY_ARRAY_ENCODINGS);
    for id in ["vortex.pco", "vortex.zstd", "vortex.fsst", "vortex.runend"] {
        assert!(arrays.contains(&vortex::session::registry::Id::from(id)));
    }
    for id in ["vortex.slice", "vortex.shared", "vortex.filter"] {
        assert!(!arrays.contains(&vortex::session::registry::Id::from(id)));
    }
    let layouts = session.enabled_component_ids(ComponentKind::Layout);
    assert!(layouts.contains(&vortex::session::registry::Id::from("vortex.stats")));
    assert!(!layouts.contains(&vortex::session::registry::Id::from("vortex.zoned")));
}

#[test]
fn file_statistics_resolve_paths_after_nested_fields_and_literal_dots() {
    use vortex::{buffer::ByteBuffer, dtype::FieldPath};

    use crate::NumScalar;

    let nested_fields = vec![Arc::new(Field::new("number", DataType::Int64, false))];
    let nested = StructArray::new(
        nested_fields.clone().into(),
        vec![Arc::new(Int64Array::from(vec![100, 200, 300]))],
        Some(arrow::buffer::NullBuffer::from(vec![true, false, true])),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("nested", DataType::Struct(nested_fields.into()), true),
        Field::new("number", DataType::Int64, true),
        Field::new("nested.number", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(nested),
            Arc::new(Int64Array::from(vec![Some(-10), None, Some(30)])),
            Arc::new(Int64Array::from(vec![7, 8, 9])),
        ],
    )
    .unwrap();

    for legacy in [true, false] {
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let session = if legacy {
            legacy_write_session(session).unwrap()
        } else {
            // Deliberately write a modern fixture to exercise post-order stats,
            // not the production write policy (which must stay legacy-only).
            session
        };
        let dtype = session.arrow().from_arrow_schema(&schema).unwrap();
        let mut bytes = Vec::new();
        let options =
            VortexWriteOptions::new(session.clone()).with_strategy(addressable_strategy());
        // The modern session alone does not enable nested statistics: the writer
        // defaults to the legacy layout even when modern editions are enabled.
        let options = if legacy {
            options
        } else {
            options.include_nested_statistics()
        };
        let mut writer = options.blocking(&runtime).writer(&mut bytes, dtype);
        writer
            .push(
                session
                    .arrow()
                    .from_arrow_record_batch(batch.clone(), &schema)
                    .unwrap(),
            )
            .unwrap();
        writer.finish().unwrap();
        let file = session
            .open_options()
            .open_buffer(ByteBuffer::copy_from(&bytes))
            .unwrap();
        let paths = file.footer().statistics().unwrap().paths();
        let number_index = paths
            .iter()
            .position(|path| path == &FieldPath::from_name("number"))
            .unwrap();
        if legacy {
            assert_eq!(number_index, 1);
        } else {
            assert!(
                number_index > 1,
                "nested stats must not use schema ordinals"
            );
        }
        let blob = BlobHandle::Mem(Bytes::from(bytes));
        assert_eq!(
            blob_column_stats(&blob, "number").unwrap(),
            Some((NumScalar::I64(-10), NumScalar::I64(30)))
        );
        assert_eq!(
            blob_column_stats(&blob, "nested.number").unwrap(),
            Some((NumScalar::I64(7), NumScalar::I64(9)))
        );
        assert_eq!(blob_column_stats(&blob, "missing").unwrap(), None);
        assert_eq!(blob_column_stats(&blob, "nested").unwrap(), None);
    }
}
