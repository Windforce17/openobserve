// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Regression for encoded docs chunks whose wire descendants exceed u16 buffers.
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::{
    array::{Array, BinaryViewArray, BinaryViewBuilder, UInt32Array},
    buffer::Buffer,
    datatypes::{DataType, Field, Schema},
};
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayContext, ArrayRef, Canonical, IntoArray, VortexSessionExecute,
        arrays::{ChunkedArray, DictArray, PrimitiveArray, StructArray, VarBinViewArray},
        validity::Validity,
    },
    arrow::ArrowSessionExt,
    buffer::Buffer as VortexBuffer,
    file::OpenOptionsSessionExt,
    io::{
        runtime::{BlockingRuntime, single::SingleThreadRuntime},
        session::RuntimeSessionExt,
    },
    session::VortexSession,
};

use crate::flat::{bound_flat_buffers, fits_flat_buffer_limit, serialized_buffer_count};

fn fragmented_values(n: usize, nullable: bool) -> BinaryViewArray {
    let mut builder = BinaryViewBuilder::new();
    let mut state = 42u64;
    for i in 0..n {
        let mut value = vec![0u8; 13];
        for byte in &mut value {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        let block = builder.append_block(Buffer::from(value));
        builder.try_append_view(block, 0, 13).unwrap();
        if nullable && i % 31 == 0 {
            builder.append_null();
        }
    }
    builder.finish()
}

fn dictionary(values: &BinaryViewArray, session: &VortexSession) -> ArrayRef {
    let native = session
        .arrow()
        .from_arrow_array(Arc::new(values.clone()), values.null_count() > 0)
        .unwrap();
    let codes = PrimitiveArray::new(
        VortexBuffer::from((0..values.len() as u32).collect::<Vec<_>>()),
        Validity::NonNullable,
    )
    .into_array();
    DictArray::try_new(codes, native).unwrap().into_array()
}

fn serialize(array: &ArrayRef, session: &VortexSession) -> Vec<u8> {
    array
        .serialize(&ArrayContext::empty(), session, &Default::default())
        .unwrap()
        .iter()
        .flat_map(|b| b.iter().copied())
        .collect()
}

fn write_and_read(dict: ArrayRef, values: &BinaryViewArray, expected_fail_open: u64) -> ArrayRef {
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::BinaryView, values.null_count() > 0),
        Field::new("row", DataType::UInt32, false),
    ]));
    let rows = PrimitiveArray::new(
        VortexBuffer::from((0..values.len() as u32).collect::<Vec<_>>()),
        Validity::NonNullable,
    )
    .into_array();
    let root = StructArray::try_new(
        ["payload", "row"].into(),
        vec![dict, rows],
        values.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let fail_open = Arc::new(AtomicU64::new(0));
    let mut encoder = crate::container::DocsBlobEncoder::spawn(
        schema,
        65536,
        0,
        None,
        true,
        Arc::clone(&fail_open),
        0,
    )
    .unwrap();
    encoder.push_encoded(root).unwrap();
    let (sink, docs_len) = encoder.join().unwrap();
    let bytes = match sink {
        crate::container::ContainerSink::Mem(bytes) => bytes,
        _ => panic!("expected memory sink"),
    };
    let docs = bytes::Bytes::from(bytes).slice(4..4 + docs_len as usize);
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = session.open_options().open_buffer(docs).unwrap();
    let mut row = 0;
    let mut payload = None;
    for array in file.scan().unwrap().into_array_iter(&runtime).unwrap() {
        use vortex::array::arrays::{Struct, struct_::StructArraySlotsExt};
        let array = array.unwrap();
        let stored = array.as_::<Struct>().fields()[0].clone();
        assert!(fits_flat_buffer_limit(&stored, &session));
        payload.get_or_insert(stored);
        let arrow = session
            .arrow()
            .execute_arrow(array, None, &mut session.create_execution_ctx())
            .unwrap();
        let batch = arrow
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let binary = arrow::compute::cast(batch.column(0), &DataType::BinaryView).unwrap();
        let binary = binary.as_any().downcast_ref::<BinaryViewArray>().unwrap();
        let rows = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        for i in 0..binary.len() {
            assert_eq!(binary.is_null(i), values.is_null(row), "null at row {row}");
            if !binary.is_null(i) {
                assert_eq!(binary.value(i), values.value(row), "payload at row {row}");
            }
            assert_eq!(rows.value(i), row as u32, "row index at row {row}");
            row += 1;
        }
    }
    assert_eq!(row, values.len());
    assert_eq!(fail_open.load(Ordering::Relaxed), expected_fail_open);
    payload.unwrap()
}

#[test]
fn encoded_dict_over_u16_buffers_roundtrips() {
    let values = fragmented_values(65535, false);
    let session = VortexSession::default();
    let dict = dictionary(&values, &session);
    assert_eq!(dict.nbuffers_recursive(), 65537);
    assert_eq!(serialized_buffer_count(&dict, &session).unwrap(), 65537);
    let error = dict
        .serialize(&ArrayContext::empty(), &session, &Default::default())
        .unwrap_err()
        .to_string();
    assert!(error.contains("u16::MAX buffers: 65537"), "{error}");
    write_and_read(dict, &values, 1);
}

#[test]
fn nullable_dict_over_u16_buffers_roundtrips() {
    let values = fragmented_values(65535, true);
    let session = VortexSession::default();
    let dict = dictionary(&values, &session);
    assert_eq!(serialized_buffer_count(&dict, &session).unwrap(), 65538);
    write_and_read(dict, &values, 1);
}

#[test]
fn exact_wire_limit_still_passes_through_byte_identically() {
    let values = fragmented_values(65533, false);
    let session = VortexSession::default();
    let dict = dictionary(&values, &session);
    assert_eq!(
        serialized_buffer_count(&dict, &session).unwrap(),
        usize::from(u16::MAX)
    );
    assert!(fits_flat_buffer_limit(&dict, &session));
    let before = serialize(&dict, &session);
    let bounded = bound_flat_buffers(dict.clone(), &session).unwrap();
    assert_eq!(serialize(&bounded, &session), before);
    let stored = write_and_read(dict, &values, 0);
    assert_eq!(stored.encoding_id(), bounded.encoding_id());
    assert_eq!(serialize(&stored, &session), before);
}

#[test]
fn one_buffer_over_limit_is_rejected() {
    let session = VortexSession::default();
    let values = fragmented_values(65534, false);
    let dict = dictionary(&values, &session);
    assert_eq!(serialized_buffer_count(&dict, &session).unwrap(), 65536);
    assert!(!fits_flat_buffer_limit(&dict, &session));
}

#[test]
fn fully_utilized_fragments_need_more_than_canonical_compact() {
    let session = VortexSession::default();
    let values = fragmented_values(65535, true);
    let array = session
        .arrow()
        .from_arrow_array(Arc::new(values.clone()), values.null_count() > 0)
        .unwrap();
    let mut ctx = session.create_execution_ctx();
    let canonical = array.clone().execute::<VarBinViewArray>(&mut ctx).unwrap();
    assert_eq!(canonical.data_buffers().len(), 65535);
    let compacted = canonical.compact_buffers(&mut ctx).unwrap().into_array();
    assert!(!fits_flat_buffer_limit(&compacted, &session));
    let safe = bound_flat_buffers(array.clone(), &session).unwrap();
    assert!(fits_flat_buffer_limit(&safe, &session));
    // This directly exercises the post-compression backstop; it must not rely
    // on either the encoded-root test or the compressor reducing fragmentation.
    assert_values_equal(safe, array, &session);
}

#[test]
fn concat_values_accumulates_buffers_not_just_rows() {
    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();
    let mut chunks = Vec::new();
    for n in [1, 7, 31] {
        let values = fragmented_values(n, false);
        chunks.push(
            session
                .arrow()
                .from_arrow_array(Arc::new(values), false)
                .unwrap(),
        );
    }
    let dtype = chunks[0].dtype().clone();
    let values = ChunkedArray::try_new(chunks, dtype)
        .unwrap()
        .into_array()
        .execute::<Canonical>(&mut ctx)
        .unwrap()
        .into_array();
    assert_eq!(
        values
            .as_::<vortex::array::arrays::VarBinView>()
            .data_buffers()
            .len(),
        39
    );
    assert_eq!(serialized_buffer_count(&values, &session).unwrap(), 40);
    // Buffer concatenation retains fully used fragments; the dict parent then
    // needs another buffer for codes. This is a mechanism test, not an assertion
    // about which unknown production input produced 65535 fragments.
    let codes = PrimitiveArray::new(
        VortexBuffer::from((0..39u32).collect::<Vec<_>>()),
        Validity::NonNullable,
    )
    .into_array();
    let dict = DictArray::try_new(codes, values).unwrap().into_array();
    assert_eq!(serialized_buffer_count(&dict, &session).unwrap(), 41);
}

#[test]
fn repeated_children_are_counted_per_wire_occurrence() {
    let session = VortexSession::default();
    let values = fragmented_values(4, false);
    let dict = dictionary(&values, &session);
    let root = StructArray::try_new(
        ["a", "b"].into(),
        vec![dict.clone(), dict],
        4,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    assert_eq!(serialized_buffer_count(&root, &session).unwrap(), 12);
    assert!(!serialize(&root, &session).is_empty());
}

#[test]
#[ignore = "local CPU microbenchmark; run explicitly with --ignored --nocapture"]
fn serialized_count_overhead() {
    use std::{hint::black_box, time::Instant};
    let session = VortexSession::default();
    for n in [4, 1024, 65535] {
        let values = fragmented_values(n, false);
        let dict = dictionary(&values, &session);
        let iterations = if n < 65535 { 10_000 } else { 100 };
        let mut samples = Vec::new();
        for _ in 0..7 {
            let start = Instant::now();
            for _ in 0..iterations {
                black_box(serialized_buffer_count(black_box(&dict), &session).unwrap());
            }
            samples.push(start.elapsed().as_nanos() as f64 / iterations as f64);
        }
        samples.sort_by(f64::total_cmp);
        eprintln!(
            "wire_count data_buffers={n} iterations={iterations} median_ns={:.1}",
            samples[3]
        );
    }
}

#[test]
fn nested_over_limit_tree_is_materialized_recursively() {
    let session = VortexSession::default();
    let values = fragmented_values(32767, true);
    let array = session
        .arrow()
        .from_arrow_array(Arc::new(values.clone()), values.null_count() > 0)
        .unwrap();
    let root = StructArray::try_new(
        ["a", "b"].into(),
        vec![array.clone(), array],
        values.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    assert!(!fits_flat_buffer_limit(&root, &session));
    let safe = bound_flat_buffers(root.clone(), &session).unwrap();
    assert!(fits_flat_buffer_limit(&safe, &session));
    assert_values_equal(safe, root, &session);
}

fn assert_values_equal(actual: ArrayRef, expected: ArrayRef, session: &VortexSession) {
    let mut ctx = session.create_execution_ctx();
    let actual = actual.execute::<Canonical>(&mut ctx).unwrap().into_array();
    let expected = expected
        .execute::<Canonical>(&mut ctx)
        .unwrap()
        .into_array();
    assert_eq!(actual.dtype(), expected.dtype());
    assert_eq!(actual.len(), expected.len());
    for row in 0..actual.len() {
        assert_eq!(
            actual.execute_scalar(row, &mut ctx).unwrap(),
            expected.execute_scalar(row, &mut ctx).unwrap(),
            "row {row}"
        );
    }
}
