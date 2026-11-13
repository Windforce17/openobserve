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
    arrow::ArrowSessionExt,
    compressor::{BtrBlocksCompressor, BtrBlocksCompressorBuilder},
    editions::{CORE_2025_10_0, EditionSessionExt, EnabledEditions},
    error::VortexResult,
    file::{VortexWriteOptions, WriteStrategyBuilder},
    io::session::RuntimeSessionExt,
    layout::layouts::compressed::CompressorPlugin,
    scan::selection::Selection,
    session::VortexSession,
};
use vortex_datafusion::VortexAccessPlan;

/// Dedicated runtime for Vortex writer coordination and decode work, sized
/// by `ZO_VORTEX_THREAD_NUM`. Writer CPU leaves use the shared bounded VIX
/// executor instead of this runtime.
pub static VORTEX_RUNTIME: LazyLock<VortexRuntime> = LazyLock::new(|| {
    let cfg = config::get_config();
    let threads = cfg.limit.vortex_thread_num.max(1);
    let leaf_threads = (cfg.limit.cpu_num / config::cluster::cpu_role_divisor())
        .saturating_sub(2)
        .max(1);
    vortex_index::configure_shared_cpu_executor(leaf_threads);
    VortexRuntime::new(threads)
});

pub struct VortexRuntime {
    runtime: tokio::runtime::Runtime,
}

impl VortexRuntime {
    fn new(threads: usize) -> Self {
        let runtime = config::allocator_reclaim::configure_runtime(
            tokio::runtime::Builder::new_multi_thread(),
        )
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

impl Utf8Compressor {
    pub fn from_session(session: &VortexSession) -> Self {
        Self {
            inner: BtrBlocksCompressorBuilder::from_session(session).build(),
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

/// Pin every write path to the components understood by Vortex 0.79 readers.
/// Enabling core alone replaces only that family: clear default JSON editions
/// before constructing any compressor or write strategy.
pub(super) fn legacy_write_session(session: VortexSession) -> Result<VortexSession> {
    session.register(EnabledEditions::default());
    session.enable_edition(CORE_2025_10_0).map_err(|error| {
        DataFusionError::Execution(format!("enable Vortex CORE_2025_10_0 edition: {error}"))
    })?;
    Ok(session)
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
            let handle = vortex_index::shared_vortex_execution_handle().map_err(|error| {
                DataFusionError::Execution(format!("shared Vortex executor: {error}"))
            })?;
            let session = legacy_write_session(VortexSession::default().with_handle(handle))?;
            let dtype = session
                .arrow()
                .from_arrow_schema(schema.as_ref())
                .map_err(|e| DataFusionError::Execution(format!("Vortex schema import: {e}")))?;
            let strategy = WriteStrategyBuilder::from_session(&session)
                .with_compressor(Utf8Compressor::from_session(&session))
                .build();
            let mut buf = Vec::new();
            let mut writer = VortexWriteOptions::new(session.clone())
                .with_strategy(strategy)
                .writer(&mut buf, dtype);
            while let Some(batch) = rx.recv().await {
                let array: ArrayRef = session
                    .arrow()
                    .from_arrow_record_batch(batch, schema.as_ref())
                    .map_err(|e| {
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

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use vortex::{
        buffer::ByteBuffer, editions::ComponentKind, file::OpenOptionsSessionExt,
        layout::layouts::flat::Flat,
    };

    use super::*;

    #[test]
    fn write_session_enables_only_079_core_without_json() -> Result<()> {
        let session = legacy_write_session(VortexSession::default())?;
        assert_eq!(session.enabled_editions().editions(), vec![CORE_2025_10_0]);
        assert!(
            session
                .enabled_component_ids(ComponentKind::Array)
                .iter()
                .all(|id| !id.to_string().contains("json"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn search_writer_footer_uses_only_079_encodings() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("message", DataType::Utf8, true),
        ]));
        let rows = 8192;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows)),
                Arc::new(StringArray::from_iter((0..rows).map(|i| {
                    (i % 7 != 0).then_some("{\"message\":\"repeated JSON text\",\"number\":42}")
                }))),
            ],
        )?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let read_task = tokio::spawn(async move {
            tx.send(batch)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            Ok(())
        });
        let bytes = write_vortex(schema, rx, read_task).await?;
        let session = VortexSession::default().with_tokio();
        let file = session
            .open_options()
            .open_buffer(ByteBuffer::copy_from(&bytes))
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert_eq!(file.row_count(), rows as u64);
        // CORE_2025_10_0 is the 0.79 registry. Check actual footer contexts,
        // not only the configured compressor: JSON-looking input must not
        // introduce a newer JSON encoding via a default edition.
        let legacy = legacy_write_session(VortexSession::default())?;
        let arrays = legacy.enabled_component_ids(ComponentKind::Array);
        let layouts = legacy.enabled_component_ids(ComponentKind::Layout);
        let mut stack = vec![file.footer().layout().clone()];
        let mut flat_count = 0;
        while let Some(layout) = stack.pop() {
            assert!(
                layouts
                    .iter()
                    .any(|id| id.to_string() == layout.encoding_id().to_string())
            );
            if let Some(flat) = layout.as_opt::<Flat>() {
                flat_count += 1;
                for id in flat.array_ctx().ids() {
                    assert!(
                        arrays
                            .iter()
                            .any(|allowed| allowed.to_string() == id.to_string()),
                        "unsupported footer encoding: {id}"
                    );
                }
            }
            stack.extend(
                layout
                    .children()
                    .map_err(|error| DataFusionError::External(Box::new(error)))?,
            );
        }
        assert!(flat_count > 0);
        Ok(())
    }
}
