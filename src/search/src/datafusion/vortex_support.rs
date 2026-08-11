// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Open-source Vortex data-file support: the write path, the scan access plan,
//! and the dedicated runtime. These used to live in the enterprise crate.

use std::sync::{Arc, LazyLock};

use arrow_schema::Schema;
use config::meta::stream::RowIdBitmap;
use datafusion::{
    arrow::record_batch::RecordBatch,
    error::{DataFusionError, Result},
};
use vortex::{
    VortexSessionDefault,
    array::{ArrayRef, ExecutionCtx},
    arrow::{FromArrowArray, FromArrowType},
    compressor::{BtrBlocksCompressor, BtrBlocksCompressorBuilder},
    dtype::DType,
    error::VortexResult,
    file::{VortexWriteOptions, WriteStrategyBuilder},
    io::session::RuntimeSessionExt,
    layout::layouts::compressed::CompressorPlugin,
    scan::selection::Selection,
    session::VortexSession,
};
use vortex_datafusion::VortexAccessPlan;

/// Dedicated runtime for CPU-heavy Vortex encode/decode work, sized by
/// `ZO_VORTEX_THREAD_NUM` (0 = number of CPU cores).
pub static VORTEX_RUNTIME: LazyLock<VortexRuntime> = LazyLock::new(|| {
    let threads = config::get_config().limit.vortex_thread_num.max(1);
    VortexRuntime::new(threads)
});

pub struct VortexRuntime {
    runtime: tokio::runtime::Runtime,
}

impl VortexRuntime {
    fn new(threads: usize) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("vortex")
            .enable_all()
            .build()
            .expect("failed to build the vortex runtime");
        Self { runtime }
    }

    pub fn spawn_blocking<F, R>(&self, f: F) -> tokio::task::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.runtime.spawn_blocking(f)
    }

    /// Blocks the current (non-async) thread on `fut`. Must not be called from
    /// an async context; intended for use inside `spawn_blocking` closures.
    pub fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.runtime.handle().block_on(fut)
    }
}

/// String-friendly chunk compressor: the stock BtrBlocks sampling compressor,
/// whose scheme set already prefers FSST/dictionary encodings for utf8 columns.
#[derive(Clone)]
pub struct Utf8Compressor {
    inner: BtrBlocksCompressor,
}

impl Default for Utf8Compressor {
    fn default() -> Self {
        Self {
            inner: BtrBlocksCompressorBuilder::default().build(),
        }
    }
}

impl CompressorPlugin for Utf8Compressor {
    fn compress_chunk(&self, chunk: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        self.inner.compress(chunk, ctx)
    }
}

/// Convert a per-row match bitmap (from the inverted index) into a Vortex scan
/// access plan. Returns `None` when the bitmap selects everything, so a full
/// scan proceeds without the selection overhead.
///
/// Vortex's scan mask is roaring-backed, so the selection is handed over as a
/// roaring treemap directly — the previous `Buffer<u64>` index list cost
/// 8 bytes per matched row only for vortex to re-compress it internally.
pub fn generate_vortex_access_plan(row_ids: &RowIdBitmap) -> Option<VortexAccessPlan> {
    if row_ids.selects_all() {
        return None;
    }
    let rows = roaring::RoaringTreemap::from_sorted_iter(row_ids.iter().map(u64::from))
        .expect("RowIdBitmap iterates ascending");
    Some(VortexAccessPlan::default().with_selection(Selection::IncludeRoaring(rows)))
}

/// Drain `rx` into a single in-memory `.vortex` file. Mirrors `write_parquet`
/// in `merge/mod.rs`: consumes the record-batch channel, then propagates any
/// error from the producing `read_task`.
pub async fn write_vortex(
    schema: Arc<Schema>,
    mut rx: tokio::sync::mpsc::Receiver<RecordBatch>,
    read_task: tokio::task::JoinHandle<Result<()>>,
) -> Result<Vec<u8>> {
    let writer_task = VORTEX_RUNTIME.spawn_blocking(move || {
        VORTEX_RUNTIME.block_on(async move {
            let session = VortexSession::default().with_tokio();
            let dtype = DType::from_arrow(schema.as_ref());
            let strategy = WriteStrategyBuilder::default()
                .with_compressor(Utf8Compressor::default())
                .build();
            let mut buf = Vec::new();
            let mut writer = VortexWriteOptions::new(session.clone())
                .with_strategy(strategy)
                .writer(&mut buf, dtype);
            while let Some(batch) = rx.recv().await {
                let array: ArrayRef = ArrayRef::from_arrow(batch, false).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "failed to convert arrow batch to vortex array: {e}"
                    ))
                })?;
                writer
                    .push(array)
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("vortex push error: {e}")))?;
            }
            writer
                .finish()
                .await
                .map_err(|e| DataFusionError::Execution(format!("vortex finish error: {e}")))?;
            Ok::<Vec<u8>, DataFusionError>(buf)
        })
    });

    let buf = writer_task
        .await
        .map_err(|e| DataFusionError::Execution(format!("vortex runtime task failed: {e}")))??;
    read_task
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;
    Ok(buf)
}
