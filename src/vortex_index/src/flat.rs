// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Keep each flat message within Vortex's recursive u16 buffer address space.

use async_trait::async_trait;
use futures::StreamExt;
use vortex::{
    array::{
        ArrayRef, Canonical, IntoArray, VortexSessionExecute, builders::builder_with_capacity_in,
        session::ArraySessionExt,
    },
    compressor::BtrBlocksCompressorBuilder,
    error::{VortexResult, vortex_err},
    layout::{
        LayoutRef, LayoutStrategy, LayoutWriterContext,
        layouts::flat::writer::FlatLayoutStrategy,
        segments::SegmentSinkRef,
        sequence::{
            SendableSequentialStream, SequencePointer, SequentialStreamAdapter, SequentialStreamExt,
        },
    },
    session::VortexSession,
};

/// Count the WIRE tree, not `ArrayRef::nbuffers_recursive()`: edition serializers
/// may replace buffers/children. Count repeated children once per occurrence,
/// just like `ArraySerializationTree::nbuffers_recursive` in Vortex serde.
/// No payload bytes are copied and no flatbuffer is built.
pub(crate) fn serialized_buffer_count(
    array: &ArrayRef,
    session: &VortexSession,
) -> VortexResult<usize> {
    let serialization = session.array_serialize(array)?.ok_or_else(|| {
        vortex_err!(
            "Array {} does not support serialization",
            array.encoding_id()
        )
    })?;
    let mut count = serialization.buffers.len();
    for child in &serialization.children {
        count = count.saturating_add(serialized_buffer_count(child, session)?);
    }
    Ok(count)
}

pub(crate) fn fits_flat_buffer_limit(array: &ArrayRef, session: &VortexSession) -> bool {
    serialized_buffer_count(array, session).is_ok_and(|count| count <= usize::from(u16::MAX))
}

/// Last line of defence AFTER compression, including decoded/coalesced chunks
/// and first-encode/terms leaves. Ordinary chunks are returned byte-for-byte.
/// Canonicalize/compact alone is insufficient: fully utilized VarBinView buffers
/// are deliberately retained, even when their count cannot fit a flat message.
pub(crate) fn bound_flat_buffers(
    array: ArrayRef,
    session: &VortexSession,
) -> VortexResult<ArrayRef> {
    if serialized_buffer_count(&array, session)? <= usize::from(u16::MAX) {
        return Ok(array);
    }
    let mut ctx = session.create_execution_ctx();
    let canonical = array.execute::<Canonical>(&mut ctx)?.into_array();
    let mut builder = builder_with_capacity_in(canonical.dtype(), canonical.len(), ctx.allocator());
    // Scalar appends copy values into owned buffers (also for nested children),
    // unlike array appends which can keep the very fragments we must eliminate.
    for row in 0..canonical.len() {
        builder.append_scalar(&canonical.execute_scalar(row, &mut ctx)?)?;
    }
    let dense = builder.finish();
    let compressed = BtrBlocksCompressorBuilder::from_session(session)
        .with_compact()
        .build()
        .compress(&dense, &mut ctx)?;
    if fits_flat_buffer_limit(&compressed, session) {
        return Ok(compressed);
    }
    // A compressor is allowed to return an uncompressed tree or add children.
    // Never assume recompression fixed the count; the dense canonical form is
    // a valid legacy encoding too. Impossibly wide/large values fail closed.
    if fits_flat_buffer_limit(&dense, session) {
        return Ok(dense);
    }
    Err(vortex_err!(
        "Materialized array still exceeds the flat message buffer limit"
    ))
}

#[derive(Clone, Default)]
pub(crate) struct BufferBoundedFlatStrategy(FlatLayoutStrategy);

#[async_trait]
impl LayoutStrategy for BufferBoundedFlatStrategy {
    async fn write_stream(
        &self,
        ctx: LayoutWriterContext,
        segment_sink: SegmentSinkRef,
        stream: SendableSequentialStream,
        eof: SequencePointer,
        session: &VortexSession,
    ) -> VortexResult<LayoutRef> {
        let dtype = stream.dtype().clone();
        let session_copy = session.clone();
        let stream = stream.map(move |item| {
            let (id, array) = item?;
            Ok((id, bound_flat_buffers(array, &session_copy)?))
        });
        self.0
            .write_stream(
                ctx,
                segment_sink,
                SequentialStreamAdapter::new(dtype, stream).sendable(),
                eof,
                session,
            )
            .await
    }
}
