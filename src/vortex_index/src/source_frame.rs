// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use prost::Message;
use vortex::{
    array::{ArrayId, ArrayRef, Canonical, ExecutionCtx, IntoArray, VTable},
    encodings::zstd::Zstd,
    error::VortexResult,
    session::VortexSession,
};
use vortex_btrblocks::{
    ArrayAndStats, BtrBlocksCompressorBuilder, CascadingCompressor, CompressorContext, Scheme,
    SchemeExt, schemes::string::ZstdScheme,
};
use vortex_compressor::scheme::{CompressionEstimate, DeferredEstimate};

pub(crate) const DEFAULT_SOURCE_FRAME_ROWS: usize = 8192;
pub(crate) const SMALL_SOURCE_FRAME_ROWS: usize = 2048;

// Vortex exposes the frame protobuf but keeps its value count private.
// Read that existing field only; unknown fields are ignored by prost.
#[derive(Message)]
struct FrameValues {
    #[prost(uint64, tag = "2")]
    values: u64,
}

pub(crate) fn has_small_frames(chunk: &ArrayRef) -> bool {
    let Some(array) = chunk.as_opt::<Zstd>() else {
        return false;
    };
    if !array.dtype().is_utf8() || array.dtype().is_nullable() {
        return false;
    }
    let parts = array
        .data()
        .clone()
        .into_parts(vortex::array::validity::Validity::NonNullable);
    if parts.slice_start != 0
        || parts.slice_stop != parts.n_rows
        || parts.n_rows != chunk.len()
        || parts.frames.len() != parts.metadata.frames().len()
        || parts.frames.is_empty()
    {
        return false;
    }
    let mut total = 0u64;
    for frame in parts.metadata.frames() {
        let Ok(frame) = FrameValues::decode(frame.encode_to_vec().as_slice()) else {
            return false;
        };
        if frame.values == 0 || frame.values > SMALL_SOURCE_FRAME_ROWS as u64 {
            return false;
        }
        let Some(next) = total.checked_add(frame.values) else {
            return false;
        };
        total = next;
    }
    total == chunk.len() as u64
}

#[derive(Debug)]
struct SourceZstdScheme;

impl Scheme for SourceZstdScheme {
    fn scheme_name(&self) -> &'static str {
        "vortex.string.zstd"
    }

    fn matches(&self, canonical: &Canonical) -> bool {
        canonical.dtype().is_utf8()
    }

    fn produced_encodings(&self) -> Vec<ArrayId> {
        vec![Zstd.id()]
    }

    fn expected_compression_ratio(
        &self,
        _data: &ArrayAndStats,
        _compress_ctx: CompressorContext,
        _exec_ctx: &mut ExecutionCtx,
    ) -> CompressionEstimate {
        CompressionEstimate::Deferred(DeferredEstimate::Sample)
    }

    fn compress(
        &self,
        _compressor: &CascadingCompressor,
        data: &ArrayAndStats,
        _compress_ctx: CompressorContext,
        exec_ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let compacted = data
            .array_as_varbinview()
            .into_owned()
            .compact_buffers(exec_ctx)?;
        Ok(
            Zstd::from_var_bin_view_without_dict(&compacted, 3, SMALL_SOURCE_FRAME_ROWS, exec_ctx)?
                .into_array(),
        )
    }
}

pub(crate) fn compressor(session: &VortexSession) -> BtrBlocksCompressorBuilder {
    BtrBlocksCompressorBuilder::from_session(session)
        .with_compact()
        .exclude_schemes([SchemeExt::id(&ZstdScheme)])
        .with_new_scheme(&SourceZstdScheme)
}
