// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Ordered, admitted CPU leaves. The controller alone pulls the iterator and
//! invokes callbacks. In particular neither I/O nor a waiting controller is a
//! CPU-pool job. Unsupported trees retain the ordinary inline conversion path.

use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        mpsc::{Receiver, sync_channel},
    },
    time::Instant,
};

use arrow::{
    array::StructArray as ArrowStructArray,
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use parking_lot::Mutex;
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute,
        arrays::{
            Bool, BoolArray, Constant, Dict, DictArray, Filter, FilterArray, Null, NullArray,
            Primitive, PrimitiveArray, ScalarFn, Shared, Slice, Struct, StructArray, VarBin,
            VarBinArray, VarBinView, VarBinViewArray, bool::BoolArrayExt, dict::DictArraySlotsExt,
            filter::FilterArrayExt, primitive::PrimitiveArrayExt, scalar_fn::ScalarFnArrayExt,
            shared::SharedArrayExt, slice::SliceArrayExt, struct_::StructArrayExt,
            varbin::VarBinArrayExt, varbinview::VarBinViewArrayExt,
        },
        scalar_fn::fns::pack::Pack,
        validity::Validity,
    },
    arrow::ArrowSessionExt,
    buffer::{BitBuffer, Buffer, BufferMut, ByteBuffer},
    dtype::DType,
    encodings::{
        alp::{ALP, ALPRD},
        fastlanes::{BitPacked, Delta, FoR},
        fsst::{FSST, FSSTArrayExt},
        pco::Pco,
        runend::{RunEnd, RunEndArrayExt},
        sequence::{Sequence, SequenceData},
        sparse::{Sparse, SparseArraySlotsExt},
        zstd::{Zstd, ZstdDataParts, ZstdFrameMetadata},
    },
    error::VortexResult,
    mask::Mask,
    session::VortexSession,
};

use crate::{Result, VixError, source::VixReadOperation};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NativePredicateStrategy {
    #[default]
    BoundedPrepass,
    DirectResidual,
    NativeStringEq,
}

#[derive(Clone, Debug, Default)]
pub struct NativeScanOptions {
    pub predicate: NativePredicateStrategy,
    pub conversion: Option<Arc<ScanCpuBudget>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ScanCpuBudgetSnapshot {
    pub submitted: u64,
    pub completed: u64,
    pub inline_fallbacks: u64,
    pub peak_jobs: usize,
    pub peak_bytes: usize,
    pub queue_wait_ns: u64,
    pub conversion_wall_ns: u64,
    pub callback_wall_ns: u64,
}

#[derive(Debug, Default)]
struct BudgetState {
    jobs: usize,
    bytes: usize,
    metrics: ScanCpuBudgetSnapshot,
}

/// Query-local shared admission. Buffer bytes cover detached inputs, all
/// supported execution temporaries, and Arrow outputs simultaneously. Array
/// descriptors are separately bounded by the tree and in-flight count limits.
#[derive(Debug)]
pub struct ScanCpuBudget {
    max_in_flight: NonZeroUsize,
    max_live_bytes: NonZeroUsize,
    state: Mutex<BudgetState>,
}

impl ScanCpuBudget {
    pub fn new(max_in_flight: NonZeroUsize, max_live_bytes: NonZeroUsize) -> Self {
        Self {
            max_in_flight,
            max_live_bytes,
            state: Mutex::new(BudgetState::default()),
        }
    }

    pub fn snapshot(&self) -> ScanCpuBudgetSnapshot {
        self.state.lock().metrics
    }

    fn acquire(self: &Arc<Self>, bytes: usize) -> Option<Credit> {
        let mut state = self.state.lock();
        let next = state.bytes.checked_add(bytes)?;
        if state.jobs >= self.max_in_flight.get() || next > self.max_live_bytes.get() {
            return None;
        }
        state.bytes = next;
        state.jobs += 1;
        state.metrics.peak_jobs = state.metrics.peak_jobs.max(state.jobs);
        state.metrics.peak_bytes = state.metrics.peak_bytes.max(next);
        Some(Credit {
            budget: self.clone(),
            bytes,
        })
    }

    fn fallback(&self) {
        self.state.lock().metrics.inline_fallbacks += 1;
    }
}

struct Credit {
    budget: Arc<ScanCpuBudget>,
    bytes: usize,
}
impl Credit {
    fn grow(&mut self, extra: usize) -> bool {
        let mut state = self.budget.state.lock();
        let Some(bytes) = state.bytes.checked_add(extra) else {
            return false;
        };
        if bytes > self.budget.max_live_bytes.get() {
            return false;
        }
        state.bytes = bytes;
        self.bytes += extra;
        state.metrics.peak_bytes = state.metrics.peak_bytes.max(bytes);
        true
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock();
        state.jobs -= 1;
        state.bytes -= self.bytes;
    }
}

// Declaration order matters: release outputs and their backing before owners
// and credits. The reservation also retains the actual DataFusion consumer.
struct Envelope {
    result: Result<RecordBatch>,
    _operation: Arc<dyn VixReadOperation>,
    _owner: Box<dyn Send + Sync>,
    _credit: Credit,
}

#[derive(Default)]
struct Pending(VecDeque<Receiver<Envelope>>);
impl Pending {
    fn deliver(&mut self, on_batch: &mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()> {
        let receiver = self.0.pop_front().expect("pending conversion");
        let envelope = receiver.recv().map_err(|_| {
            VixError::Malformed("native conversion worker exited without a result".into())
        })?;
        if envelope._operation.is_cancelled() {
            return Err(VixError::Cancelled);
        }
        // Destructure rather than map the result: the owner and credit must
        // remain live for the entire callback, including callback unwinding.
        let Envelope {
            result,
            _operation,
            _owner,
            _credit,
        } = envelope;
        let owner: Arc<dyn Send + Sync> = Arc::from(_owner);
        let batch = _operation.own_conversion_output(result?, owner.clone())?;
        struct CallbackTimer(Arc<ScanCpuBudget>, Instant);
        impl Drop for CallbackTimer {
            fn drop(&mut self) {
                self.0.state.lock().metrics.callback_wall_ns += nanos(self.1);
            }
        }
        let _timer = CallbackTimer(_credit.budget.clone(), Instant::now());
        on_batch(batch)
    }
    fn flush(&mut self, on_batch: &mut dyn FnMut(RecordBatch) -> Result<()>) -> Result<()> {
        while !self.0.is_empty() {
            self.deliver(on_batch)?;
        }
        Ok(())
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        // Started computation is non-cancellable. Drain even on an iterator
        // error, callback error/panic, or cancellation, without more callbacks.
        for receiver in self.0.drain(..) {
            drop(receiver.recv());
        }
    }
}

fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn submit(
    pending: &mut Pending,
    operation: Arc<dyn VixReadOperation>,
    owner: Box<dyn Send + Sync>,
    credit: Credit,
    work: impl FnOnce() -> Result<RecordBatch> + Send + 'static,
) -> Result<()> {
    // Startup errors must not count a submission which never reached a pool.
    crate::cpu_executor::shared_vortex_execution_handle()?;
    let (sender, receiver) = sync_channel(1);
    let queued = Instant::now();
    credit.budget.state.lock().metrics.submitted += 1;
    crate::cpu_executor::submit_conversion(Box::new(move || {
        credit.budget.state.lock().metrics.queue_wait_ns += nanos(queued);
        let start = Instant::now();
        let result = if operation.is_cancelled() {
            drop(work);
            Err(VixError::Cancelled)
        } else {
            catch_unwind(AssertUnwindSafe(work)).unwrap_or_else(|_| {
                Err(VixError::Malformed(
                    "native conversion worker panicked".into(),
                ))
            })
        };
        {
            let mut state = credit.budget.state.lock();
            state.metrics.completed += 1;
            state.metrics.conversion_wall_ns += nanos(start);
        }
        let _ = sender.send(Envelope {
            result,
            _operation: operation,
            _owner: owner,
            _credit: credit,
        });
    }))?;
    pending.0.push_back(receiver);
    Ok(())
}

pub(crate) fn convert_chunks_ordered<I>(
    chunks: I,
    session: &VortexSession,
    target: &DataType,
    operation: Arc<dyn VixReadOperation>,
    on_batch: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()>
where
    I: Iterator<Item = VortexResult<ArrayRef>>,
{
    let options = operation.scan_options();
    let budget = options
        .conversion
        .filter(|_| operation.supports_conversion());
    let width = if budget.is_some() {
        crate::cpu_executor::shared_cpu_thread_count()?
    } else {
        1
    };
    let mut pending = Pending::default();
    for array in chunks {
        if operation.is_cancelled() {
            return Err(VixError::Cancelled);
        }
        let array = array?;
        let probe = budget
            .as_ref()
            .and_then(|_| codec_probe_size(&array, 0, &mut 0));
        let Some((budget, probe)) = budget.as_ref().zip(probe) else {
            pending.flush(on_batch)?;
            if let Some(budget) = &budget {
                budget.fallback();
            }
            on_batch(to_record_batch(session, array, target)?)?;
            continue;
        };
        while pending.0.len() >= width {
            pending.deliver(on_batch)?;
        }
        let mut credit = budget.acquire(probe);
        while credit.is_none() && !pending.0.is_empty() {
            pending.deliver(on_batch)?;
            credit = budget.acquire(probe);
        }
        let Some(mut credit) = credit else {
            budget.fallback();
            on_batch(to_record_batch(session, array, target)?)?;
            continue;
        };
        let owner = match operation.reserve_conversion(probe) {
            Ok(owner) => owner,
            Err(_) => {
                drop(credit);
                pending.flush(on_batch)?;
                budget.fallback();
                if operation.is_cancelled() {
                    return Err(VixError::Cancelled);
                }
                on_batch(to_record_batch(session, array, target)?)?;
                continue;
            }
        };
        let array = lower_pack(array)?;
        // Zstd exposes owned metadata parts, not borrowed frame metadata.
        // The small source-sized probe admits these clones BEFORE allocation.
        let mut plans = CodecPlans::collect(&array);
        let bound = conversion_bound(&array, target, &plans).map(|bound| bound.max(probe));
        let extra = bound.and_then(|bound| bound.checked_sub(probe));
        let payload_owner = grow_owned(&mut credit, extra, &operation, &mut pending, on_batch)?;
        let Some(payload_owner) = payload_owner else {
            drop(plans);
            drop(owner);
            drop(credit);
            pending.flush(on_batch)?;
            budget.fallback();
            on_batch(to_record_batch(session, array, target)?)?;
            continue;
        };
        // Original opaque backing stays in the existing serial reader scope
        // until final admission; this is not a process-RSS/source-cache bound.
        let prepared = detach_planned(&array, &mut session.create_execution_ctx(), &mut plans)?;
        drop(plans);
        // Dictionary Zstd values are decoded once during preparation, while
        // standalone Zstd columns keep their substantial decode in the leaf.
        let empty_plans = CodecPlans::default();
        let extra = arrow_bound(&prepared, target, true, &empty_plans).and_then(|full| {
            full.checked_sub(arrow_bound(&prepared, target, false, &empty_plans)?)
        });
        let arrow_owner = grow_owned(&mut credit, extra, &operation, &mut pending, on_batch)?;
        let Some(arrow_owner) = arrow_owner else {
            drop(prepared);
            drop(payload_owner);
            drop(owner);
            drop(credit);
            pending.flush(on_batch)?;
            budget.fallback();
            if operation.is_cancelled() {
                return Err(VixError::Cancelled);
            }
            // No detached output escapes an additional-admission refusal.
            on_batch(to_record_batch(session, array, target)?)?;
            continue;
        };
        drop(array);
        let owner: Box<dyn Send + Sync> = Box::new((owner, payload_owner, arrow_owner));
        let session = session.clone();
        let target = target.clone();
        submit(&mut pending, operation.clone(), owner, credit, move || {
            admitted_record_batch(&session, prepared, &target)
        })?;
    }
    pending.flush(on_batch)
}

fn grow_owned(
    credit: &mut Credit,
    extra: Option<usize>,
    operation: &Arc<dyn VixReadOperation>,
    pending: &mut Pending,
    on_batch: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<Option<Box<dyn Send + Sync>>> {
    let Some(extra) = extra else {
        return Ok(None);
    };
    while !credit.grow(extra) {
        if pending.0.is_empty() {
            return Ok(None);
        }
        pending.deliver(on_batch)?;
    }
    if operation.is_cancelled() {
        return Err(VixError::Cancelled);
    }
    if extra == 0 {
        return Ok(Some(Box::new(())));
    }
    Ok(operation.reserve_conversion(extra).ok())
}

fn to_record_batch(
    session: &VortexSession,
    array: ArrayRef,
    target: &DataType,
) -> Result<RecordBatch> {
    let mut ctx = session.create_execution_ctx();
    let target = Field::new("", target.clone(), array.dtype().is_nullable());
    let array = session
        .arrow()
        .execute_arrow(array, Some(&target), &mut ctx)?;
    let array = array
        .as_any()
        .downcast_ref::<ArrowStructArray>()
        .ok_or_else(|| VixError::Malformed("vortex scan did not produce a struct array".into()))?;
    Ok(RecordBatch::from(array))
}

fn admitted_record_batch(
    session: &VortexSession,
    array: ArrayRef,
    target: &DataType,
) -> Result<RecordBatch> {
    let field = Field::new("", target.clone(), array.dtype().is_nullable());
    let array = admitted_arrow(session, array, &field, &mut session.create_execution_ctx())?;
    let structure = array
        .as_any()
        .downcast_ref::<ArrowStructArray>()
        .ok_or_else(|| VixError::Malformed("native conversion expected a struct".into()))?;
    Ok(RecordBatch::from(structure))
}

fn admitted_arrow(
    session: &VortexSession,
    array: ArrayRef,
    field: &Field,
    ctx: &mut ExecutionCtx,
) -> Result<arrow::array::ArrayRef> {
    use arrow::array::types::{BinaryViewType, StringViewType};
    use vortex::arrow::byte_view::canonical_varbinview_to_arrow;
    match field.data_type() {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => {
            let canonical = array.execute::<VarBinViewArray>(ctx)?;
            // The detached backing is already fully reserved and transferred
            // with the output. Bypass optional compact_buffers rewriting:
            // its geometric block growth is not needed for correctness.
            let arrow = match canonical.dtype() {
                DType::Utf8(_) => canonical_varbinview_to_arrow::<StringViewType>(&canonical, ctx)?,
                DType::Binary(_) => {
                    canonical_varbinview_to_arrow::<BinaryViewType>(&canonical, ctx)?
                }
                _ => unreachable!("canonical byte-view dtype"),
            };
            if matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView) {
                return Ok(arrow);
            }
            Ok(arrow::compute::cast(&arrow, field.data_type())?)
        }
        DataType::Struct(fields) => {
            let structure = array.execute::<StructArray>(ctx)?;
            if fields.len() != structure.names().len() {
                return Err(VixError::Malformed(
                    "native struct field count changed".into(),
                ));
            }
            let mut columns = Vec::with_capacity(fields.len());
            for (child, field) in structure.iter_unmasked_fields().zip(fields) {
                columns.push(admitted_arrow(session, child.clone(), field, ctx)?);
            }
            let nulls = vortex::arrow::to_null_buffer(
                structure
                    .struct_validity()
                    .execute_mask(structure.len(), ctx)?,
            );
            Ok(Arc::new(ArrowStructArray::try_new_with_length(
                fields.clone(),
                columns,
                nulls,
                structure.len(),
            )?))
        }
        _ => Ok(session.arrow().execute_arrow(array, Some(field), ctx)?),
    }
}
// Allocation accounting follows Vortex 0.79, not a compressed-size ratio.
// Vortex BufferMut reserves requested bytes PLUS the preferred alignment,
// including hidden leading padding; this pinned default is 256, not Arrow's 64.
fn allocation(bytes: usize) -> Option<usize> {
    let capacity = bytes.checked_add(*vortex::buffer::Alignment::DEFAULT_ALIGNMENT)?;
    (capacity <= isize::MAX as usize).then_some(capacity)
}
fn add(total: &mut usize, bytes: usize) -> Option<()> {
    *total = total.checked_add(allocation(bytes)?)?;
    Some(())
}

/// Metadata cloned during a small admitted probe, then consumed by detachment.
/// Vec entries deliberately follow projection multiplicity, including aliases.
#[derive(Default)]
struct CodecPlans {
    zstd: Vec<(usize, ZstdDataParts)>,
}

fn raw_validity(array: &ArrayRef) -> Validity {
    match array.slots().first().and_then(Option::as_ref) {
        Some(child) => Validity::Array(child.clone()),
        None if array.dtype().is_nullable() => Validity::AllValid,
        None => Validity::NonNullable,
    }
}

fn zstd_parts(array: &ArrayRef) -> ZstdDataParts {
    array
        .as_::<Zstd>()
        .data()
        .clone()
        .into_parts(raw_validity(array))
}

impl CodecPlans {
    fn collect(array: &ArrayRef) -> Self {
        fn count(array: &ArrayRef) -> usize {
            usize::from(array.is::<Zstd>())
                + array.slots().iter().flatten().map(count).sum::<usize>()
        }
        fn visit(array: &ArrayRef, plans: &mut CodecPlans) {
            if array.is::<Zstd>() {
                plans.zstd.push((array.addr(), zstd_parts(array)));
            }
            for child in array.slots().iter().flatten() {
                visit(child, plans);
            }
        }
        let mut plans = Self {
            zstd: Vec::with_capacity(count(array)),
        };
        visit(array, &mut plans);
        plans
    }

    fn get(&self, array: &ArrayRef) -> Option<&ZstdDataParts> {
        self.zstd
            .iter()
            .find(|(id, _)| *id == array.addr())
            .map(|(_, parts)| parts)
    }

    fn take(&mut self, array: &ArrayRef) -> Option<ZstdDataParts> {
        let index = self.zstd.iter().position(|(id, _)| *id == array.addr())?;
        Some(self.zstd.swap_remove(index).1)
    }
}

fn codec_probe_size(array: &ArrayRef, depth: usize, nodes: &mut usize) -> Option<usize> {
    *nodes = nodes.checked_add(1)?;
    if depth > 64 || *nodes > 4096 {
        return None;
    }
    let pack = array
        .as_opt::<ScalarFn>()
        .is_some_and(|function| function.scalar_fn().as_opt::<Pack>().is_some());
    if !(numeric(array)
        || wrapped(array)
        || scalar_constant(array)
        || array.is::<Bool>()
        || array.is::<Null>()
        || array.is::<Struct>()
        || array.is::<Dict>()
        || array.is::<VarBin>()
        || array.is::<VarBinView>()
        || array.is::<FSST>()
        || array.is::<Zstd>()
        || array.is::<Shared>()
        || pack)
    {
        return None;
    }
    let mut bytes = 0usize;
    if depth == 0 {
        add(&mut bytes, size_of::<CodecPlans>() + size_of::<Envelope>())?;
    }
    // All generic visitors below the probe allocate descriptor Vecs. Charge
    // their exact length before calling them, including many-buffer views.
    if !array.is::<Constant>() && !array.is::<Sparse>() {
        add(
            &mut bytes,
            array.nbuffers().checked_mul(
                size_of::<ByteBuffer>() + size_of::<vortex::array::buffer::BufferHandle>(),
            )?,
        )?;
    }
    if pack {
        add(
            &mut bytes,
            array.slots().len().checked_mul(
                3 * size_of::<ArrayRef>()
                    + size_of::<DType>()
                    + size_of::<vortex::dtype::FieldDType>(),
            )?,
        )?;
        add(&mut bytes, descriptor::<Struct>())?;
    }
    if array.is::<Zstd>() {
        // ZstdData::validate enforces frames.len==metadata.frames.len.
        // nbuffers is frames.len plus zero or one dictionary; no visitor runs.
        add(
            &mut bytes,
            array
                .nbuffers()
                .checked_mul(size_of::<ByteBuffer>() + size_of::<ZstdFrameMetadata>())?,
        )?;
        add(&mut bytes, size_of::<(usize, ZstdDataParts)>())?;
    }
    for child in array.slots().iter().flatten() {
        bytes = bytes.checked_add(codec_probe_size(child, depth + 1, nodes)?)?;
    }
    Some(bytes)
}

fn zstd_uncompressed(parts: &ZstdDataParts) -> Option<usize> {
    parts.metadata.frames.iter().try_fold(0usize, |sum, frame| {
        sum.checked_add(usize::try_from(frame.uncompressed_size).ok()?)
    })
}

fn zstd_bound(parts: &ZstdDataParts) -> Option<usize> {
    let raw = zstd_uncompressed(parts)?;
    // Keep the public reconstruction on its single-segment branch.
    if raw > i32::MAX as usize || parts.frames.len() != parts.metadata.frames.len() {
        return None;
    }
    for (frame, metadata) in parts.frames.iter().zip(&parts.metadata.frames) {
        // Legacy and concatenated streams have different context allocations.
        if frame.get(..4)? != [0x28, 0xb5, 0x2f, 0xfd] {
            return None;
        }
        if zstd::zstd_safe::find_frame_compressed_size(frame.as_slice()).ok()? != frame.len() {
            return None;
        }
        if zstd::zstd_safe::get_frame_content_size(frame.as_slice()).ok()??
            != metadata.uncompressed_size
        {
            return None;
        }
    }
    let mut bytes = 0usize;
    // These allocation-free functions are the pinned C implementation's own
    // sizeof(DCtx) and sizeof(DDict)+copied_dictionary_bytes. Vortex uses bulk
    // one-shot decoding, not the separately allocated streaming window path.
    let context = unsafe { zstd::zstd_safe::zstd_sys::ZSTD_estimateDCtxSize() };
    add(&mut bytes, context)?;
    if let Some(dictionary) = &parts.dictionary {
        let dictionary = unsafe {
            zstd::zstd_safe::zstd_sys::ZSTD_estimateDDictSize(
                dictionary.len(),
                zstd::zstd_safe::zstd_sys::ZSTD_dictLoadMethod_e::ZSTD_dlm_byCopy,
            )
        };
        add(&mut bytes, dictionary)?;
    }
    add(&mut bytes, raw)?;
    // Each reconstructed value consumes at least its four-byte length prefix.
    // Do not trust metadata n_values as an allocation bound on corrupt frames.
    // Growing BufferMut views can retain old+new capacities during doubling.
    for _ in 0..3 {
        add(&mut bytes, (raw / 4).checked_mul(16)?)?;
    }
    add(&mut bytes, parts.n_rows.checked_mul(16)?)?; // nullable output views
    add(&mut bytes, parts.n_rows.checked_mul(8)?)?; // mask indices / primitive expansion
    add(&mut bytes, parts.n_rows.div_ceil(8))?;
    // Probe parts, rebuilt typed data and a sliced metadata copy coexist.
    for _ in 0..3 {
        add(
            &mut bytes,
            parts
                .frames
                .len()
                .checked_mul(size_of::<ByteBuffer>() + size_of::<ZstdFrameMetadata>())?,
        )?;
        add(&mut bytes, size_of::<ZstdDataParts>())?;
    }
    Some(bytes)
}

fn contains_zstd(array: &ArrayRef) -> bool {
    array.is::<Zstd>() || array.slots().iter().flatten().any(contains_zstd)
}

// ArrayInner's private layout is ArrayParts plus ArrayId and ArrayStats;
// sum their public layouts (and Arc counters/alignment) rather than assuming
// an unexplained per-row or per-node metadata multiplier.
fn descriptor<V: vortex::array::VTable>() -> usize {
    size_of::<vortex::array::ArrayParts<V>>()
        + size_of::<vortex::array::ArrayId>()
        + size_of::<vortex::array::stats::ArrayStats>()
        + size_of::<parking_lot::RwLock<vortex::array::stats::StatsSet>>()
        + 4 * size_of::<usize>()
        + 64
}

fn compressor_slot<T>(_: fn(&ArrayRef, &mut ExecutionCtx) -> VortexResult<T>) -> usize {
    size_of::<std::sync::OnceLock<T>>()
}

fn metadata_bound(array: &ArrayRef) -> Option<usize> {
    let input = if array.is::<FSST>() {
        descriptor::<FSST>()
    } else if array.is::<Zstd>() {
        descriptor::<Zstd>()
    } else if array.is::<Pco>() {
        descriptor::<Pco>()
    } else if array.is::<Sequence>() {
        descriptor::<Sequence>()
    } else if array.is::<Sparse>() {
        descriptor::<Sparse>()
    } else if array.is::<RunEnd>() {
        descriptor::<RunEnd>()
    } else if array.is::<Slice>() {
        descriptor::<Slice>()
    } else if array.is::<Filter>() {
        descriptor::<Filter>()
    } else if array.is::<Dict>() {
        descriptor::<Dict>()
    } else if array.is::<Struct>() {
        descriptor::<Struct>()
    } else if array.is::<VarBin>() {
        descriptor::<VarBin>()
    } else if array.is::<VarBinView>() {
        descriptor::<VarBinView>()
    } else if array.is::<Bool>() {
        descriptor::<Bool>()
    } else if array.is::<Null>() {
        descriptor::<Null>()
    } else if array.is::<BitPacked>() {
        descriptor::<BitPacked>()
    } else if array.is::<FoR>() {
        descriptor::<FoR>()
    } else if array.is::<Delta>() {
        descriptor::<Delta>()
    } else if array.is::<ALP>() {
        descriptor::<ALP>()
    } else if array.is::<ALPRD>() {
        descriptor::<ALPRD>()
    } else if array.is::<Constant>() {
        descriptor::<Constant>()
    } else {
        descriptor::<Primitive>()
    };
    let mut bytes = 0usize;
    add(&mut bytes, input)?;
    // Preparatory canonical array, detached canonical array and the string
    // canonical/take result coexist at their execution boundary.
    add(&mut bytes, descriptor::<Primitive>())?;
    add(&mut bytes, descriptor::<Primitive>())?;
    add(&mut bytes, descriptor::<VarBinView>())?;
    // Arrow conversion and the operation's zero-copy ownership wrapper.
    let arrow = size_of::<arrow::array::StringViewArray>()
        .max(size_of::<ArrowStructArray>())
        .max(size_of::<arrow::array::Int64Array>())
        .max(size_of::<arrow::array::StringArray>())
        .max(size_of::<arrow::array::LargeStringArray>())
        .max(size_of::<arrow::array::BooleanArray>())
        .max(size_of::<arrow::array::NullArray>());
    add(&mut bytes, arrow + 2 * size_of::<usize>())?;
    add(&mut bytes, arrow + 2 * size_of::<usize>())?;
    let slots = array.slots().len();
    // Constructor Vec, temporary Arc, owned slots, and owned dtype fields.
    for element in [
        size_of::<ArrayRef>(),
        size_of::<ArrayRef>(),
        size_of::<Option<ArrayRef>>(),
        size_of::<DType>(),
        size_of::<vortex::dtype::FieldDType>(),
    ] {
        add(&mut bytes, slots.checked_mul(element)?)?;
    }
    if let Some(structure) = array.as_opt::<Struct>() {
        for name in structure.names().iter() {
            add(&mut bytes, name.as_ref().len())?;
        }
        add(
            &mut bytes,
            slots.checked_mul(size_of::<vortex::dtype::FieldName>())?,
        )?;
        add(
            &mut bytes,
            size_of::<vortex::dtype::FieldNames>()
                + size_of::<Arc<[vortex::dtype::FieldDType]>>()
                + size_of::<
                    std::sync::OnceLock<
                        vortex::utils::aliases::hash_map::HashMap<vortex::dtype::FieldName, usize>,
                    >,
                >()
                + 2 * size_of::<usize>(),
        )?;
    }
    if array.is::<FSST>() {
        // FSSTSymbolTable is an Arc containing two typed buffers plus an
        // uninitialized compressor OnceLock. Inferring its return type does
        // not train a compressor or initialize its large lookup tables.
        add(
            &mut bytes,
            size_of::<Buffer<u64>>()
                + size_of::<Buffer<u8>>()
                + compressor_slot(vortex::encodings::fsst::fsst_train_compressor)
                + 2 * size_of::<usize>(),
        )?;
    }
    // Heap-buffer descriptor collections in Vortex and both Arrow wrappers.
    let buffers = array.nbuffers().checked_add(1)?;
    for element in [
        size_of::<ByteBuffer>(),
        size_of::<vortex::array::buffer::BufferHandle>(),
        size_of::<arrow::buffer::Buffer>(),
        size_of::<arrow::buffer::Buffer>(),
    ] {
        add(&mut bytes, buffers.checked_mul(element)?)?;
    }
    Some(bytes)
}

// Pco 1.0.2 reads mode then delta in LSB-first nibbles. Reject Dict,
// Lookback and Conv1 BEFORE its metadata parser can allocate their states.
fn pco_chunk_latents(header: &[u8], chunk: &[u8], ptype: vortex::dtype::PType) -> Option<usize> {
    if header != [4, 1] || !matches!(ptype.byte_width(), 2 | 4 | 8) {
        return None;
    }
    let first = *chunk.first()?;
    let (delta_byte, latents) = match first & 15 {
        0 => (0, 1),                                    // Classic
        1 if ptype.is_int() => (ptype.byte_width(), 2), // inline IntMult base
        _ => return None,
    };
    match *chunk.get(delta_byte)? >> 4 {
        0 => Some(latents),
        1 if *chunk.get(delta_byte + 1)? & 7 != 0 => Some(latents),
        _ => None,
    }
}

fn pco_bound(array: &ArrayRef) -> Option<usize> {
    let view = array.as_opt::<Pco>()?;
    let metadata = view.metadata();
    if metadata.header.as_slice() != [4, 1] {
        return None;
    }
    let width = match array.dtype().as_ptype() {
        vortex::dtype::PType::U16 | vortex::dtype::PType::I16 | vortex::dtype::PType::F16 => 2,
        vortex::dtype::PType::U32 | vortex::dtype::PType::I32 | vortex::dtype::PType::F32 => 4,
        vortex::dtype::PType::U64 | vortex::dtype::PType::I64 | vortex::dtype::PType::F64 => 8,
        _ => return None,
    };
    let mut pages = 0usize;
    let mut values = 0usize;
    for chunk in &metadata.chunks {
        pages = pages.checked_add(chunk.pages.len())?;
        for page in &chunk.pages {
            values = values.checked_add(usize::try_from(page.n_values).ok()?)?;
        }
    }
    if metadata.chunks.len().checked_add(pages)? != array.nbuffers()
        || array.len() > view.unsliced_n_rows()
        || values > view.unsliced_n_rows()
    {
        return None;
    }
    if let Some(validity) = array.slots().first().and_then(Option::as_ref)
        && validity.len() != view.unsliced_n_rows()
    {
        return None;
    }
    let mut latent_count = 1usize;
    for index in 0..metadata.chunks.len() {
        let buffer = <Pco as vortex::array::VTable>::buffer(array.as_::<Pco>(), index);
        if !buffer.is_on_host() {
            return None;
        }
        latent_count = latent_count.max(pco_chunk_latents(
            &metadata.header,
            buffer.as_host().as_slice(),
            array.dtype().as_ptype(),
        )?);
    }
    // Per-latent source bound: 16384 bins/ANS entries, 256-value scratch,
    // EOF-reader replacement overlap and at most seven delta state values:
    // 1,042,616 bytes at width8, rounded to1MiB. IntMult has two independent
    // streams; its join writes borrowed scratch into dst without allocation.
    let mut bytes = latent_count.checked_mul(1024 * 1024)?;
    let physical = array.len().max(values).checked_mul(width)?;
    // Full overlapping pages, with old+new BufferMut growth capacities.
    for _ in 0..3 {
        add(&mut bytes, physical)?;
    }
    add(&mut bytes, array.len().checked_mul(width)?)?; // nullable expansion
    add(&mut bytes, array.len().checked_mul(width)?)?; // detached output
    add(&mut bytes, metadata.header.len())?;
    add(
        &mut bytes,
        metadata
            .chunks
            .len()
            .checked_mul(size_of::<vortex::encodings::pco::PcoChunkInfo>())?,
    )?;
    add(
        &mut bytes,
        pages.checked_mul(size_of::<vortex::encodings::pco::PcoPageInfo>())?,
    )?;
    Some(bytes)
}
fn numeric(array: &ArrayRef) -> bool {
    matches!(array.dtype(), DType::Primitive(..))
        && (array.is::<Primitive>()
            || array.is::<Constant>()
            || array.is::<Dict>()
            || array.is::<BitPacked>()
            || array.is::<FoR>()
            || array.is::<Delta>()
            || array.is::<ALP>()
            || array.is::<ALPRD>()
            || array.is::<Pco>()
            || array.is::<Sequence>()
            || array.is::<Sparse>()
            || array.is::<RunEnd>())
}

fn wrapped(array: &ArrayRef) -> bool {
    (array.is::<Slice>() || array.is::<Filter>())
        && matches!(
            array.dtype(),
            DType::Primitive(..) | DType::Bool(_) | DType::Utf8(_) | DType::Binary(_)
        )
}

fn wrapper_bound(array: &ArrayRef) -> Option<usize> {
    let mut bytes = 0usize;
    if let Some(sequence) = array.as_opt::<Sequence>() {
        if !array.slots().is_empty()
            || sequence.ptype() != array.dtype().as_ptype()
            || sequence.multiplier().ptype() != sequence.ptype()
        {
            return None;
        }
        SequenceData::validate(
            sequence.base(),
            sequence.multiplier(),
            array.dtype(),
            array.len(),
        )
        .ok()?;
    }
    if array.is::<Sparse>() {
        if array.slots().len() != 3 || array.slots()[0].is_none() || array.slots()[1].is_none() {
            return None;
        }
        let sparse = array.as_::<Sparse>();
        if sparse.patch_indices().is_empty()
            || sparse.patch_indices().len() > array.len()
            || sparse.patch_indices().len() != sparse.patch_values().len()
            || !sparse.patch_indices().dtype().is_unsigned_int()
            || sparse.patch_indices().dtype().is_nullable()
            || sparse.patch_values().dtype() != array.dtype()
            || sparse.fill_scalar().dtype() != array.dtype()
            || sparse.offset().checked_add(array.len()).is_none()
        {
            return None;
        }
        match sparse.fill_scalar().value() {
            None => {}
            Some(vortex::scalar::ScalarValue::Primitive(value))
                if value.ptype() == array.dtype().as_ptype() => {}
            _ => return None,
        }
        add(&mut bytes, sparse.patch_indices().len().div_ceil(8))?;
    }
    if array.is::<RunEnd>() {
        if array.slots().len() != 2 || array.slots().iter().any(Option::is_none) {
            return None;
        }
        let runend = array.as_::<RunEnd>();
        if !runend.ends().dtype().is_unsigned_int()
            || runend.ends().dtype().is_nullable()
            || runend.ends().len() != runend.values().len()
            || runend.values().dtype() != array.dtype()
            || runend.offset().checked_add(array.len()).is_none()
        {
            return None;
        }
    }
    if array.is::<Slice>() {
        if array.slots().len() != 1 || array.slots()[0].is_none() {
            return None;
        }
        let slice = array.as_::<Slice>();
        let range = slice.slice_range();
        if range.start > range.end
            || range.end > slice.child().len()
            || range.end - range.start != array.len()
            || array.dtype() != slice.child().dtype()
        {
            return None;
        }
    }
    if array.is::<Filter>() {
        if array.slots().len() != 1 || array.slots()[0].is_none() {
            return None;
        }
        let filter = array.as_::<Filter>();
        let mask = filter.filter_mask();
        if mask.len() != filter.child().len()
            || mask.true_count() != array.len()
            || array.dtype() != filter.child().dtype()
        {
            return None;
        }
        if matches!(array.dtype(), DType::Utf8(_) | DType::Binary(_))
            && !(filter.child().is::<Zstd>()
                || filter.child().is::<VarBinView>()
                || filter.child().is::<Dict>())
        {
            // Other encoded string filter kernels allocate different heaps.
            return None;
        }
        if let Mask::Values(values) = mask {
            let bits = values.bit_buffer();
            if bits.offset().checked_add(bits.len())? > bits.inner().len().checked_mul(8)?
                || bits.true_count() != values.true_count()
            {
                return None;
            }
            // Fresh slices have capacity K; lazy indices also reserve K.
            add(
                &mut bytes,
                values.true_count().checked_mul(3 * size_of::<usize>())?,
            )?;
            add(&mut bytes, values.len().div_ceil(8))?;
            add(
                &mut bytes,
                size_of::<vortex::mask::MaskValues>() + 2 * size_of::<usize>(),
            )?;
        }
        add(&mut bytes, array.len().checked_mul(16)?)?;
        // Boolean filtering retains an extra u64 word, including for tiny K.
        let words = array.len().div_ceil(64).checked_add(1)?.checked_mul(8)?;
        add(&mut bytes, words)?;
        add(&mut bytes, words)?;
    }
    Some(bytes)
}

fn detached_filter_mask(mask: &Mask) -> Mask {
    match mask {
        Mask::AllTrue(rows) => Mask::AllTrue(*rows),
        Mask::AllFalse(rows) => Mask::AllFalse(*rows),
        Mask::Values(values) => {
            let mut slices = Vec::with_capacity(values.true_count());
            slices.extend(values.bit_buffer().set_slices());
            Mask::from_slices(values.len(), slices)
        }
    }
}

fn invalid_wrapper() -> VixError {
    VixError::Malformed("invalid bounded numeric wrapper".into())
}

fn prepared_run_ends_ok(ends: &ArrayRef, values: &ArrayRef, offset: usize, len: usize) -> bool {
    let Some(primitive) = ends.as_opt::<Primitive>() else {
        return false;
    };
    if !ends.dtype().is_unsigned_int() || ends.dtype().is_nullable() || ends.len() != values.len() {
        return false;
    }
    let Some(required) = offset.checked_add(len) else {
        return false;
    };
    if ends.is_empty() {
        return len == 0 && offset == 0;
    }
    let fits = vortex::array::match_each_unsigned_integer_ptype!(primitive.ptype(), |E| {
        E::try_from(offset).is_ok() && E::try_from(len).is_ok()
    });
    if !fits {
        return false;
    }
    let mut previous = None;
    for row in 0..ends.len() {
        let Some(end) = integer_at(ends, row) else {
            return false;
        };
        if end < offset || previous.is_some_and(|previous| previous >= end) {
            return false;
        }
        previous = Some(end);
    }
    previous.is_some_and(|last| last >= required)
}

fn prepare_wrapper(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    plans: &mut CodecPlans,
) -> Result<Option<ArrayRef>> {
    if array.is::<Sequence>() {
        // Metadata arithmetic was checked before admission; this is a fresh
        // exact-length output and need not be copied a second time.
        return Ok(Some(
            array.clone().execute::<PrimitiveArray>(ctx)?.into_array(),
        ));
    }
    if let Some(slice) = array.as_opt::<Slice>() {
        let child = detach_planned(slice.child(), ctx, plans)?;
        let sliced = child.slice(slice.slice_range().clone())?;
        return Ok(Some(match array.dtype() {
            DType::Primitive(..) => sliced.execute::<PrimitiveArray>(ctx)?.into_array(),
            DType::Bool(_) => sliced.execute::<BoolArray>(ctx)?.into_array(),
            _ => sliced,
        }));
    }
    if let Some(filter) = array.as_opt::<Filter>() {
        let child = detach_planned(filter.child(), ctx, plans)?;
        let mask = detached_filter_mask(filter.filter_mask());
        let prepared = match array.dtype() {
            DType::Primitive(..) => {
                let child = child.execute::<PrimitiveArray>(ctx)?.into_array();
                FilterArray::try_new(child, mask)?
                    .into_array()
                    .execute::<PrimitiveArray>(ctx)?
                    .into_array()
            }
            DType::Bool(_) => {
                let child = child.execute::<BoolArray>(ctx)?.into_array();
                FilterArray::try_new(child, mask)?
                    .into_array()
                    .execute::<BoolArray>(ctx)?
                    .into_array()
            }
            _ => {
                if let Some(dict) = child.as_opt::<Dict>() {
                    // Filter canonical codes, never compressed FSST values.
                    let codes = FilterArray::try_new(dict.codes().clone(), mask)?
                        .into_array()
                        .execute::<PrimitiveArray>(ctx)?
                        .into_array();
                    DictArray::try_new(codes, dict.values().clone())?.into_array()
                } else {
                    // Only Zstd/canonical views reach this branch, as checked
                    // by wrapper_bound. Preserve substantial Zstd CPU work.
                    FilterArray::try_new(child, mask)?.into_array()
                }
            }
        };
        return Ok(Some(prepared));
    }
    if let Some(runend) = array.as_opt::<RunEnd>() {
        let ends = detach_planned(runend.ends(), ctx, plans)?.execute::<PrimitiveArray>(ctx)?;
        let values = detach_planned(runend.values(), ctx, plans)?.execute::<PrimitiveArray>(ctx)?;
        if !prepared_run_ends_ok(ends.as_ref(), values.as_ref(), runend.offset(), array.len()) {
            return Err(invalid_wrapper());
        }
        let dense = vortex::encodings::runend::compress::runend_decode_primitive(
            ends,
            values,
            runend.offset(),
            array.len(),
            ctx,
        )?;
        return Ok(Some(dense.into_array()));
    }
    if let Some(sparse) = array.as_opt::<Sparse>() {
        let indices = detach_planned(sparse.patch_indices(), ctx, plans)?
            .execute::<PrimitiveArray>(ctx)?
            .into_array();
        let values = detach_planned(sparse.patch_values(), ctx, plans)?
            .execute::<PrimitiveArray>(ctx)?
            .into_array();
        let mut normalized = BufferMut::<u64>::with_capacity(indices.len());
        let mut previous = None;
        for row in 0..indices.len() {
            let index = integer_at(&indices, row).ok_or_else(invalid_wrapper)?;
            if previous.is_some_and(|previous| previous > index) {
                return Err(invalid_wrapper());
            }
            let local = index
                .checked_sub(sparse.offset())
                .ok_or_else(invalid_wrapper)?;
            if local >= array.len() {
                return Err(invalid_wrapper());
            }
            normalized.push(u64::try_from(local).map_err(|_| invalid_wrapper())?);
            previous = Some(index);
        }
        let indices = PrimitiveArray::new(normalized.freeze(), Validity::NonNullable).into_array();
        let prepared = Sparse::try_new(indices, values, array.len(), sparse.fill_scalar().clone())?
            .into_array();
        return Ok(Some(prepared.execute::<PrimitiveArray>(ctx)?.into_array()));
    }
    Ok(None)
}

fn scalar_constant(array: &ArrayRef) -> bool {
    array.is::<Constant>()
        && matches!(
            array.dtype(),
            DType::Utf8(_) | DType::Binary(_) | DType::Bool(_) | DType::Null
        )
}

fn constant_byte_len(array: &ArrayRef) -> Option<usize> {
    let constant = array.as_opt::<Constant>()?;
    match array.dtype() {
        DType::Utf8(_) => Some(
            constant
                .scalar()
                .as_utf8()
                .value()
                .map_or(0, |value| value.len()),
        ),
        DType::Binary(_) => Some(
            constant
                .scalar()
                .as_binary()
                .value()
                .map_or(0, |value| value.len()),
        ),
        _ => None,
    }
}

/// Only trees whose entire backing can be reconstructed are accepted. Shared
/// contributes its source, never its possibly opaque cached canonical result.
/// Unrecognized codec classes, non-Pack expressions and extensions stay inline.
fn tree_bound(
    array: &ArrayRef,
    depth: usize,
    nodes: &mut usize,
    plans: &CodecPlans,
) -> Option<usize> {
    *nodes = nodes.checked_add(1)?;
    if depth > 64 || *nodes > 4096 {
        return None;
    }
    if let Some(shared) = array.as_opt::<Shared>() {
        return tree_bound(shared.source(), depth + 1, nodes, plans);
    }
    if !(numeric(array)
        || wrapped(array)
        || scalar_constant(array)
        || array.is::<Bool>()
        || array.is::<Null>()
        || array.is::<Struct>()
        || array.is::<Dict>()
        || array.is::<VarBin>()
        || array.is::<VarBinView>()
        || array.is::<FSST>()
        || array.is::<Zstd>())
    {
        return None;
    }
    // Constant::buffer() serializes its scalar into a newly allocated protobuf
    // buffer. Never call generic buffer visitors for constants before admission.
    if !array.is::<Constant>()
        && !array.is::<Sparse>()
        && array
            .buffer_handles()
            .iter()
            .any(|buffer| !buffer.is_on_host())
    {
        return None;
    }
    let mut bytes = metadata_bound(array)?;
    if !array.is::<Constant>() && !array.is::<Sparse>() {
        for buffer in array.buffers() {
            add(&mut bytes, buffer.len())?;
        }
    }
    for child in array.slots().iter().flatten() {
        bytes = bytes.checked_add(tree_bound(child, depth + 1, nodes, plans)?)?;
    }
    if array.is::<Zstd>() {
        bytes = bytes.checked_add(zstd_bound(plans.get(array)?)?)?;
    }
    if array.is::<Pco>() {
        bytes = bytes.checked_add(pco_bound(array)?)?;
    }
    bytes = bytes.checked_add(wrapper_bound(array)?)?;
    let rows = array.len();
    if let Some(length) = constant_byte_len(array) {
        if length > u32::MAX as usize {
            return None;
        }
        // constant_canonical_byte_view copies the scalar heap ONCE (including
        // the length==12 case) and allocates one repeated 16-byte view per row.
        add(&mut bytes, length)?;
        add(&mut bytes, rows.checked_mul(16)?)?;
    }
    // Validity execution/combination can retain input, a selection mask, and an
    // output bitmap. Bitmap builders may first allocate byte-per-row values.
    add(&mut bytes, rows)?;
    add(&mut bytes, rows.div_ceil(8))?;
    add(&mut bytes, rows.div_ceil(8))?;
    if numeric(array) {
        // Numeric output, detached copy, and builder/cast workspace. Delta
        // produces its full padded deltas child before slicing; account that
        // physical length rather than the logical outer slice length.
        let physical = if array.is::<Delta>() {
            array.slots().get(1)?.as_ref()?.len().max(rows)
        } else {
            rows
        };
        for _ in 0..3 {
            add(&mut bytes, physical.checked_mul(8)?)?;
        }
        // FastLanes processes fixed 1024-element scratch vectors (stack).
        add(&mut bytes, 1024 * 8)?;
    }
    if array.is::<VarBin>() || array.is::<VarBinView>() || array.is::<Dict>() || array.is::<FSST>()
    {
        add(&mut bytes, rows.checked_mul(16)?)?; // canonical output views/take
        add(&mut bytes, rows.checked_add(1)?.checked_mul(8)?)?; // offset/length cast
    }
    if let Some(varbin) = array.as_opt::<VarBin>() {
        if varbin.bytes().len() > i32::MAX as usize {
            return None;
        }
        // VarBin canonicalization takes a mutable byte buffer after slicing;
        // into_mut may copy when the detached parent is still referenced.
        add(&mut bytes, varbin.bytes().len())?;
    }
    if let Some(fsst) = array.as_opt::<FSST>() {
        let expanded = fsst.codes_bytes().len().checked_mul(8)?;
        // Stay on build_views_single_buffer; the rolling path can split/copy.
        if expanded > i32::MAX as usize {
            return None;
        }
        add(&mut bytes, expanded.checked_add(7)?)?;
    }
    Some(bytes)
}

fn integer_at(array: &ArrayRef, row: usize) -> Option<usize> {
    let primitive = array.as_opt::<Primitive>()?;
    if !array.dtype().is_int() {
        return None;
    }
    vortex::array::match_each_integer_ptype!(primitive.ptype(), |P| {
        usize::try_from(*primitive.as_slice::<P>().get(row)?).ok()
    })
}

fn value_length(array: &ArrayRef, row: usize) -> Option<usize> {
    if let Some(slice) = array.as_opt::<Slice>() {
        return value_length(slice.child(), slice.slice_range().start.checked_add(row)?);
    }
    if let Some(filter) = array.as_opt::<Filter>() {
        return match filter.filter_mask().indices() {
            vortex::mask::AllOr::All => value_length(filter.child(), row),
            vortex::mask::AllOr::None => None,
            vortex::mask::AllOr::Some(indices) => value_length(filter.child(), *indices.get(row)?),
        };
    }
    if let Some(dict) = array.as_opt::<Dict>() {
        let code = integer_at(dict.codes(), row)?;
        // Null codes may contain arbitrary bits; take ignores their values.
        if code >= dict.values().len() {
            return Some(0);
        }
        return value_length(dict.values(), code);
    }
    if let Some(fsst) = array.as_opt::<FSST>() {
        return integer_at(fsst.uncompressed_lengths(), row);
    }
    if let Some(view) = array.as_opt::<VarBinView>() {
        return Some(view.views().get(row)?.len() as usize);
    }
    if let Some(varbin) = array.as_opt::<VarBin>() {
        return integer_at(varbin.offsets(), row + 1)?
            .checked_sub(integer_at(varbin.offsets(), row)?);
    }
    None
}

fn zstd_arrow_upper(array: &ArrayRef, plans: &CodecPlans) -> Option<usize> {
    if array.is::<Zstd>() {
        return match plans.get(array) {
            Some(parts) => zstd_uncompressed(parts),
            None => zstd_uncompressed(&zstd_parts(array)),
        };
    }
    if let Some(slice) = array.as_opt::<Slice>() {
        return zstd_arrow_upper(slice.child(), plans);
    }
    if let Some(filter) = array.as_opt::<Filter>() {
        return zstd_arrow_upper(filter.child(), plans);
    }
    if let Some(shared) = array.as_opt::<Shared>() {
        return zstd_arrow_upper(shared.source(), plans);
    }
    None
}

fn arrow_bound(
    array: &ArrayRef,
    target: &DataType,
    prepared: bool,
    plans: &CodecPlans,
) -> Option<usize> {
    if let Some(shared) = array.as_opt::<Shared>() {
        return arrow_bound(shared.source(), target, prepared, plans);
    }
    let mut bytes = 0usize;
    if let (Some(structure), DataType::Struct(fields)) = (array.as_opt::<Struct>(), target) {
        if fields.len() != structure.names().len() {
            return None;
        }
        for (child, field) in structure.iter_unmasked_fields().zip(fields) {
            // Ordinal traversal is essential: projection names may repeat.
            if !field.metadata().is_empty() {
                return None;
            }
            bytes = bytes.checked_add(arrow_bound(child, field.data_type(), prepared, plans)?)?;
        }
    } else {
        match (array.dtype(), target) {
            (DType::Utf8(_), DataType::Utf8View) | (DType::Binary(_), DataType::BinaryView) => {}
            (DType::Utf8(_), DataType::Utf8 | DataType::LargeUtf8)
            | (DType::Binary(_), DataType::Binary | DataType::LargeBinary) => {
                // Arrow view-to-offset casting duplicates repeated dictionary
                // values. Full dictionary heap size alone is NOT this bound.
                if prepared {
                    let total = if let Some(total) = zstd_arrow_upper(array, plans) {
                        // Slices/filters select without repeating values.
                        total
                    } else {
                        (0..array.len()).try_fold(0usize, |sum, row| {
                            sum.checked_add(value_length(array, row)?)
                        })?
                    };
                    add(&mut bytes, total)?;
                }
                add(&mut bytes, array.len().checked_add(1)?.checked_mul(8)?)?;
            }
            (
                DType::Primitive(..),
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64,
            ) => {
                add(&mut bytes, array.len().checked_mul(8)?)?;
            }
            (DType::Bool(_), DataType::Boolean) | (DType::Null, DataType::Null) => {}
            _ => return None,
        }
    }
    add(&mut bytes, array.len().div_ceil(8))?;
    Some(bytes)
}

fn conversion_bound(array: &ArrayRef, target: &DataType, plans: &CodecPlans) -> Option<usize> {
    tree_bound(array, 0, &mut 0, plans)?.checked_add(arrow_bound(array, target, false, plans)?)
}

// Descriptor-only lowering from vendored ScalarFnPackToStructRule.
fn lower_pack(array: ArrayRef) -> Result<ArrayRef> {
    if array.slots().len() > 4096 {
        return Ok(array);
    }
    if let Some(function) = array.as_opt::<ScalarFn>()
        && let Some(pack) = function.scalar_fn().as_opt::<Pack>()
    {
        let validity = if array.dtype().is_nullable() {
            Validity::AllValid
        } else {
            Validity::NonNullable
        };
        return Ok(StructArray::try_new(
            pack.names.clone(),
            function.children(),
            array.len(),
            validity,
        )?
        .into_array());
    }
    Ok(array)
}

fn copy(buffer: &ByteBuffer) -> ByteBuffer {
    ByteBuffer::copy_from(buffer.as_slice())
}
fn detach_validity(
    validity: Validity,
    ctx: &mut ExecutionCtx,
    plans: &mut CodecPlans,
) -> Result<Validity> {
    Ok(match validity {
        Validity::Array(array) => Validity::Array(detach_planned(&array, ctx, plans)?),
        other => other,
    })
}

fn valid_offsets(offsets: &ArrayRef, rows: usize, bytes: usize) -> bool {
    if rows.checked_add(1) != Some(offsets.len()) || offsets.dtype().is_nullable() {
        return false;
    }
    let mut previous = 0usize;
    for row in 0..offsets.len() {
        let Some(offset) = integer_at(offsets, row) else {
            return false;
        };
        if offset < previous || offset > bytes {
            return false;
        }
        previous = offset;
    }
    true
}

#[cfg(test)]
fn detach(array: &ArrayRef, ctx: &mut ExecutionCtx) -> Result<ArrayRef> {
    detach_planned(array, ctx, &mut CodecPlans::collect(array))
}

fn detach_planned(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    plans: &mut CodecPlans,
) -> Result<ArrayRef> {
    if let Some(shared) = array.as_opt::<Shared>() {
        return detach_planned(shared.source(), ctx, plans);
    }
    if let Some(prepared) = prepare_wrapper(array, ctx, plans)? {
        return Ok(prepared);
    }
    if array.is::<Zstd>() {
        let mut parts = plans
            .take(array)
            .ok_or_else(|| VixError::Malformed("missing admitted Zstd metadata".into()))?;
        parts.dictionary = parts.dictionary.as_ref().map(copy);
        for frame in &mut parts.frames {
            *frame = copy(frame);
        }
        let validity = detach_validity(parts.validity, ctx, plans)?;
        let range = parts.slice_start..parts.slice_stop;
        let data = vortex::encodings::zstd::ZstdData::new(
            parts.dictionary,
            parts.frames,
            parts.metadata,
            parts.n_rows,
        );
        return Ok(Zstd::try_new(array.dtype().clone(), data, validity)?
            .into_array()
            .slice(range)?);
    }
    if scalar_constant(array) {
        return match array.dtype() {
            // The upstream canonicalizer copies the scalar bytes once and
            // constructs fresh repeated views/bits; reuse those allocations.
            DType::Utf8(_) | DType::Binary(_) => {
                Ok(array.clone().execute::<VarBinViewArray>(ctx)?.into_array())
            }
            DType::Bool(_) => Ok(array.clone().execute::<BoolArray>(ctx)?.into_array()),
            DType::Null => Ok(NullArray::new(array.len()).into_array()),
            _ => unreachable!("supported scalar constant dtype"),
        };
    }
    if numeric(array) {
        let prepared = if array.slots().iter().any(Option::is_some) {
            let slots = array
                .slots()
                .iter()
                .map(|slot| {
                    slot.as_ref()
                        .map(|child| detach_planned(child, ctx, plans))
                        .transpose()
                })
                .collect::<Result<vortex::array::ArraySlots>>()?;
            // SAFETY: detachment changes only physical representation; all
            // child values, dtypes, lengths and slot presence are preserved.
            // This parent stays controller-local; only its fresh primitive
            // copy below enters the CPU pool.
            unsafe { array.clone().with_slots(slots)? }
        } else {
            array.clone()
        };
        let primitive = prepared.execute::<PrimitiveArray>(ctx)?;
        let validity = detach_validity(primitive.as_ref().validity()?, ctx, plans)?;
        return Ok(PrimitiveArray::from_byte_buffer(
            copy(primitive.buffer_handle().as_host()),
            primitive.ptype(),
            validity,
        )
        .into_array());
    }
    if let Some(structure) = array.as_opt::<Struct>() {
        let fields = structure
            .iter_unmasked_fields()
            .map(|field| detach_planned(field, ctx, plans))
            .collect::<Result<Vec<_>>>()?;
        return Ok(StructArray::try_new(
            structure.names().clone(),
            fields,
            array.len(),
            detach_validity(structure.struct_validity(), ctx, plans)?,
        )?
        .into_array());
    }
    if let Some(dict) = array.as_opt::<Dict>() {
        let codes = detach_planned(dict.codes(), ctx, plans)?
            .execute::<PrimitiveArray>(ctx)?
            .into_array();
        let mut values = detach_planned(dict.values(), ctx, plans)?;
        if contains_zstd(&values) {
            // Reuse small full-value dictionary decoding for exact Arrow
            // repetition accounting; standalone Zstd remains compressed.
            values = values.execute::<VarBinViewArray>(ctx)?.into_array();
        }
        return Ok(DictArray::try_new(codes, values)?.into_array());
    }
    if let Some(fsst) = array.as_opt::<FSST>() {
        let codes = detach_planned(&fsst.codes().into_array(), ctx, plans)?
            .as_::<VarBin>()
            .into_owned();
        let lengths = detach_planned(fsst.uncompressed_lengths(), ctx, plans)?
            .execute::<PrimitiveArray>(ctx)?
            .into_array();
        let primitive = lengths.as_::<Primitive>();
        let bound = fsst
            .codes_bytes()
            .len()
            .checked_mul(8)
            .ok_or_else(|| VixError::Malformed("FSST expansion overflow".into()))?;
        let sum = vortex::array::match_each_integer_ptype!(primitive.ptype(), |P| {
            primitive
                .as_slice::<P>()
                .iter()
                .try_fold(0usize, |sum, &length| {
                    usize::try_from(length)
                        .ok()
                        .and_then(|length| sum.checked_add(length))
                })
        });
        if sum.is_none_or(|sum| sum > bound) {
            return Err(VixError::Malformed(
                "FSST lengths exceed symbol expansion bound".into(),
            ));
        }
        return Ok(FSST::try_new(
            array.dtype().clone(),
            Buffer::copy_from(fsst.symbols().as_slice()),
            Buffer::copy_from(fsst.symbol_lengths().as_slice()),
            codes,
            lengths,
            ctx,
        )?
        .into_array());
    }
    if let Some(varbin) = array.as_opt::<VarBin>() {
        let offsets = detach_planned(varbin.offsets(), ctx, plans)?
            .execute::<PrimitiveArray>(ctx)?
            .into_array();
        if !valid_offsets(&offsets, array.len(), varbin.bytes().len()) {
            return Err(VixError::Malformed(
                "invalid native variable-binary offsets".into(),
            ));
        }
        return Ok(VarBinArray::try_new(
            offsets,
            copy(varbin.bytes()),
            array.dtype().clone(),
            detach_validity(varbin.varbin_validity(), ctx, plans)?,
        )?
        .into_array());
    }
    if let Some(view) = array.as_opt::<VarBinView>() {
        let buffers = view
            .data_buffers()
            .iter()
            .map(|buffer| copy(buffer.as_host()))
            .collect::<Vec<_>>();
        return Ok(VarBinViewArray::try_new(
            Buffer::copy_from(view.views()),
            buffers.into(),
            array.dtype().clone(),
            detach_validity(view.varbinview_validity(), ctx, plans)?,
        )?
        .into_array());
    }
    if let Some(boolean) = array.as_opt::<Bool>() {
        let bits: BitBuffer = boolean.to_bit_buffer().iter().collect();
        return Ok(BoolArray::new(
            bits,
            detach_validity(boolean.as_ref().validity()?, ctx, plans)?,
        )
        .into_array());
    }
    if array.is::<Null>() {
        return Ok(NullArray::new(array.len()).into_array());
    }
    Err(VixError::Malformed(
        "unsupported native conversion tree reached detachment".into(),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::Schema,
    };
    use vortex::{
        VortexSessionDefault,
        buffer::buffer,
        encodings::fsst::{fsst_compress, fsst_train_compressor},
    };

    use super::*;

    struct Owner(Arc<AtomicUsize>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct Operation {
        budget: Arc<ScanCpuBudget>,
        cancelled: AtomicBool,
        live: Arc<AtomicUsize>,
        reservations_left: AtomicUsize,
        output_owners: Mutex<Vec<Arc<dyn Send + Sync>>>,
    }
    impl Operation {
        fn new(bytes: usize) -> Arc<Self> {
            Arc::new(Self {
                budget: Arc::new(ScanCpuBudget::new(
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroUsize::new(bytes).unwrap(),
                )),
                cancelled: AtomicBool::new(false),
                live: Arc::new(AtomicUsize::new(0)),
                reservations_left: AtomicUsize::new(usize::MAX),
                output_owners: Mutex::new(Vec::new()),
            })
        }
    }
    impl VixReadOperation for Operation {
        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::SeqCst)
        }
        fn scan_options(&self) -> NativeScanOptions {
            NativeScanOptions {
                conversion: Some(self.budget.clone()),
                ..Default::default()
            }
        }
        fn supports_conversion(&self) -> bool {
            true
        }
        fn reserve_conversion(&self, _: usize) -> Result<Box<dyn Send + Sync>> {
            if self
                .reservations_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_err()
            {
                return Err(VixError::InvalidQuery("injected memory refusal".into()));
            }
            self.live.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Owner(self.live.clone())))
        }
        fn own_conversion_output(
            &self,
            batch: RecordBatch,
            owner: Arc<dyn Send + Sync>,
        ) -> Result<RecordBatch> {
            // This module tests the handoff boundary; Core's tests cover
            // attaching this owner to actual Arrow allocations and slices.
            self.output_owners.lock().push(owner);
            Ok(batch)
        }
    }

    fn batch(value: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap()
    }

    fn input(value: i64) -> ArrayRef {
        StructArray::new(
            ["value"].into(),
            vec![buffer![value].into_array()],
            1,
            Validity::NonNullable,
        )
        .into_array()
    }

    fn target() -> DataType {
        DataType::Struct(vec![Field::new("value", DataType::Int64, false)].into())
    }

    struct Backing {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for Backing {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Backing {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    #[test]
    fn detached_input_releases_opaque_backing_before_cpu_submission() {
        let dropped = Arc::new(AtomicBool::new(false));
        let bytes = bytes::Bytes::from_owner(Backing {
            bytes: vec![7; 1024 * 1024],
            dropped: dropped.clone(),
        })
        .slice(512..520);
        let original = PrimitiveArray::from_byte_buffer(
            bytes.into(),
            vortex::dtype::PType::U8,
            Validity::NonNullable,
        )
        .into_array();
        let session = VortexSession::default();
        let detached = detach(&original, &mut session.create_execution_ctx()).unwrap();
        drop(original);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(detached.as_::<Primitive>().as_slice::<u8>(), &[7; 8]);
    }

    #[test]
    fn constant_strings_binary_and_bool_detach_opaque_scalar_backing() {
        use vortex::{array::arrays::ConstantArray, dtype::Nullability, scalar::Scalar};
        let session = VortexSession::default();
        let dropped = Arc::new(AtomicBool::new(false));
        let bytes = bytes::Bytes::from_owner(Backing {
            bytes: vec![42; 1024 * 1024],
            dropped: dropped.clone(),
        })
        .slice(100..120);
        let binary = ConstantArray::new(
            Scalar::binary(ByteBuffer::from(bytes), Nullability::NonNullable),
            3,
        )
        .into_array();
        let original = StructArray::new(
            ["status", "binary", "flag"].into(),
            vec![
                ConstantArray::new("UNSET", 3).into_array(),
                binary,
                ConstantArray::new(true, 3).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .into_array();
        let target = DataType::Struct(
            vec![
                Field::new("status", DataType::Utf8, false),
                Field::new("binary", DataType::Binary, false),
                Field::new("flag", DataType::Boolean, false),
            ]
            .into(),
        );
        let operation = Operation::new(1024 * 1024);
        convert_chunks_ordered(
            [Ok(original)].into_iter(),
            &session,
            &target,
            operation.clone(),
            &mut |batch| {
                assert!(
                    dropped.load(Ordering::SeqCst),
                    "opaque scalar source crossed the CPU boundary"
                );
                let statuses = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let binaries = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .unwrap();
                let flags = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .unwrap();
                for row in 0..3 {
                    assert_eq!(statuses.value(row), "UNSET");
                    assert_eq!(binaries.value(row), &[42; 20]);
                    assert!(flags.value(row));
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(operation.budget.snapshot().submitted, 1);
        assert_eq!(operation.budget.snapshot().inline_fallbacks, 0);
        operation.output_owners.lock().clear();
        assert_eq!(operation.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn completion_order_does_not_change_callback_order_or_credit_lifetime() {
        let operation = Operation::new(1024);
        let mut pending = Pending::default();
        let mut senders = Vec::new();
        for _ in 0..2 {
            let (sender, receiver) = sync_channel(1);
            senders.push(sender);
            pending.0.push_back(receiver);
        }
        for index in [1, 0] {
            senders[index]
                .send(Envelope {
                    result: Ok(batch(index as i64)),
                    _operation: operation.clone(),
                    _owner: operation.reserve_conversion(128).unwrap(),
                    _credit: operation.budget.acquire(128).unwrap(),
                })
                .ok()
                .unwrap();
        }
        let mut values = Vec::new();
        pending
            .flush(&mut |batch| {
                assert!(operation.budget.state.lock().bytes >= 128);
                assert!(operation.live.load(Ordering::SeqCst) > 0);
                values.push(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(values, [0, 1]);
        assert_eq!(operation.budget.state.lock().bytes, 0);
        assert_eq!(operation.live.load(Ordering::SeqCst), 2);
        operation.output_owners.lock().clear();
        assert_eq!(operation.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn fsst_dictionary_full_values_and_duplicate_projection_match_inline() {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let values = VarBinArray::from(vec![
            "a repeated long string for fsst",
            "another different long string",
            "unreferenced dictionary value",
        ])
        .into_array();
        let compressor = fsst_train_compressor(&values, &mut ctx).unwrap();
        let fsst = fsst_compress(&values, &compressor, &mut ctx).unwrap();
        // Lengths can themselves be dictionary encoded. They must be
        // canonicalized once in preparation, before allocation sizing.
        let lengths = DictArray::new(
            buffer![0u32, 1, 2].into_array(),
            fsst.uncompressed_lengths().clone(),
        )
        .into_array();
        let values = FSST::try_new(
            fsst.dtype().clone(),
            fsst.symbols().clone(),
            fsst.symbol_lengths().clone(),
            fsst.codes(),
            lengths,
            &mut ctx,
        )
        .unwrap()
        .into_array();
        let values = vortex::array::arrays::SharedArray::new(values).into_array();
        let dict = DictArray::new(buffer![1u32, 0, 1, 0].into_array(), values).into_array();
        let array = StructArray::new(
            ["x", "x"].into(),
            vec![dict.clone(), dict],
            4,
            Validity::NonNullable,
        )
        .into_array();
        let target = DataType::Struct(
            vec![
                Field::new("x", DataType::Utf8, false),
                Field::new("x", DataType::Utf8, false),
            ]
            .into(),
        );
        let expected = to_record_batch(&session, array.clone(), &target).unwrap();
        let operation = Operation::new(16 * 1024 * 1024);
        let mut actual = None;
        convert_chunks_ordered(
            std::iter::once(Ok(array)),
            &session,
            &target,
            operation.clone(),
            &mut |batch| {
                actual = Some(batch);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(actual.unwrap(), expected);
        assert_eq!(operation.budget.snapshot().submitted, 1);
        assert_eq!(operation.budget.snapshot().inline_fallbacks, 0);
        operation.output_owners.lock().clear();
        assert_eq!(operation.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn alp_and_alprd_numeric_preparation_preserves_float_bits() {
        let session = VortexSession::default();
        let values = [1.25f64, 2.5, 1.2345678901234567, -100_000_000.0];
        let primitive = PrimitiveArray::new(Buffer::copy_from(values), Validity::NonNullable);
        let alp = vortex::encodings::alp::alp_encode(
            primitive.as_view(),
            None,
            &mut session.create_execution_ctx(),
        )
        .unwrap()
        .into_array();
        let rd = vortex::encodings::alp::RDEncoder::new(&values)
            .encode(primitive.as_view())
            .into_array();
        let input = StructArray::new(
            ["alp", "rd"].into(),
            vec![alp, rd],
            values.len(),
            Validity::NonNullable,
        )
        .into_array();
        let target = DataType::Struct(
            vec![
                Field::new("alp", DataType::Float64, false),
                Field::new("rd", DataType::Float64, false),
            ]
            .into(),
        );
        let operation = Operation::new(1024 * 1024);
        convert_chunks_ordered(
            [Ok(input)].into_iter(),
            &session,
            &target,
            operation.clone(),
            &mut |batch| {
                for column in batch.columns() {
                    let output = column
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .unwrap();
                    assert_eq!(
                        output
                            .values()
                            .iter()
                            .map(|v| v.to_bits())
                            .collect::<Vec<_>>(),
                        values.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
                    );
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(operation.budget.snapshot().submitted, 1);
        operation.output_owners.lock().clear();
        assert_eq!(operation.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn unknown_tree_and_low_budget_are_inline_without_output_handoff() {
        let session = VortexSession::default();
        let low = Operation::new(1);
        let mut observed = Vec::new();
        convert_chunks_ordered(
            [Ok(input(9))].into_iter(),
            &session,
            &target(),
            low.clone(),
            &mut |batch| {
                observed.push(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                );
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(observed, [9]);
        assert_eq!(low.budget.snapshot().submitted, 0);
        assert_eq!(low.budget.snapshot().inline_fallbacks, 1);
        assert!(low.output_owners.lock().is_empty());

        // Chunk-of-struct inversion remains an explicitly unsupported tree.
        let dtype = input(17).dtype().clone();
        let array = vortex::array::arrays::ChunkedArray::try_new(vec![input(17), input(18)], dtype)
            .unwrap()
            .into_array();
        let unsupported = Operation::new(1024 * 1024);
        convert_chunks_ordered(
            [Ok(array)].into_iter(),
            &session,
            &target(),
            unsupported.clone(),
            &mut |batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(column.values().as_ref(), &[17, 18]);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(unsupported.budget.snapshot().submitted, 0);
        assert_eq!(unsupported.budget.snapshot().inline_fallbacks, 1);
        assert!(unsupported.output_owners.lock().is_empty());
    }

    #[test]
    fn zstd_frames_detach_opaque_owners_and_preserve_null_rows() {
        let session = VortexSession::default();
        let values = VarBinViewArray::from_iter(
            [
                Some("first outlined string value"),
                None,
                Some("second outlined string value"),
                Some("unselected tail"),
            ],
            DType::Utf8(vortex::dtype::Nullability::Nullable),
        );
        let encoded = Zstd::from_var_bin_view_without_dict(
            &values,
            3,
            16,
            &mut session.create_execution_ctx(),
        )
        .unwrap()
        .into_array();
        let mut parts = zstd_parts(&encoded);
        let frame = &parts.frames[0];
        let mut backing = vec![0u8; 1024 * 1024];
        backing[100..100 + frame.len()].copy_from_slice(frame.as_slice());
        let dropped = Arc::new(AtomicBool::new(false));
        parts.frames[0] = bytes::Bytes::from_owner(Backing {
            bytes: backing,
            dropped: dropped.clone(),
        })
        .slice(100..100 + frame.len())
        .into();
        let data = vortex::encodings::zstd::ZstdData::new(
            parts.dictionary,
            parts.frames,
            parts.metadata,
            parts.n_rows,
        );
        let column = Zstd::try_new(encoded.dtype().clone(), data, parts.validity)
            .unwrap()
            .into_array();
        drop(encoded);
        let mask_dropped = Arc::new(AtomicBool::new(false));
        let mut mask_backing = vec![0u8; 1024 * 1024];
        mask_backing[100] = 0b0111;
        let mask_bytes = bytes::Bytes::from_owner(Backing {
            bytes: mask_backing,
            dropped: mask_dropped.clone(),
        })
        .slice(100..101);
        let bitmap = BitBuffer::new(ByteBuffer::from(mask_bytes), 4);
        let column = FilterArray::try_new(column, Mask::from(bitmap))
            .unwrap()
            .into_array();
        let array =
            StructArray::new(["value"].into(), vec![column], 3, Validity::NonNullable).into_array();
        let target = DataType::Struct(vec![Field::new("value", DataType::Utf8, true)].into());
        let operation = Operation::new(16 * 1024 * 1024);
        convert_chunks_ordered(
            [Ok(array)].into_iter(),
            &session,
            &target,
            operation.clone(),
            &mut |batch| {
                use arrow::array::Array;
                assert!(
                    dropped.load(Ordering::SeqCst),
                    "opaque compressed frame entered the worker"
                );
                assert!(
                    mask_dropped.load(Ordering::SeqCst),
                    "opaque filter bitmap entered the worker"
                );
                let output = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(output.value(0), "first outlined string value");
                assert!(output.is_null(1));
                assert_eq!(output.value(2), "second outlined string value");
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(operation.budget.snapshot().submitted, 1);
        operation.output_owners.lock().clear();
        assert_eq!(operation.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn pco_prefix_guard_rejects_unbounded_codec_states() {
        use vortex::dtype::PType;
        assert_eq!(pco_chunk_latents(&[4, 1], &[0], PType::I64), Some(1));
        assert_eq!(pco_chunk_latents(&[4, 1], &[0x10, 7], PType::I64), Some(1));
        assert_eq!(
            pco_chunk_latents(&[4, 1], &[4, 255, 255, 255, 255], PType::I64),
            None
        ); // dictionary
        assert_eq!(
            pco_chunk_latents(&[4, 1], &[0x20, 255, 255], PType::I64),
            None
        ); // lookback
        assert_eq!(pco_chunk_latents(&[4, 1], &[0x10, 0], PType::I64), None); // invalid order
        assert_eq!(pco_chunk_latents(&[5, 1], &[0], PType::I64), None); // unknown grammar
        let mut int_mult = [0u8; 10];
        int_mult[0] = 1;
        assert_eq!(pco_chunk_latents(&[4, 1], &int_mult, PType::I64), Some(2));
        int_mult[8] = 0x10;
        int_mult[9] = 7;
        assert_eq!(pco_chunk_latents(&[4, 1], &int_mult, PType::I64), Some(2));
        assert_eq!(pco_chunk_latents(&[4, 1], &int_mult, PType::F64), None);
        int_mult[8] = 0x20;
        assert_eq!(pco_chunk_latents(&[4, 1], &int_mult, PType::I64), None);
    }

    #[test]
    fn numeric_wrappers_prepare_bottom_up_and_preserve_leading_empty_run() {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let runs = RunEnd::try_new(
            buffer![0u32, 3].into_array(),
            buffer![1i64, 2].into_array(),
            &mut ctx,
        )
        .unwrap()
        .into_array();
        let sparse = Sparse::try_new(
            buffer![1u32].into_array(),
            buffer![9i64].into_array(),
            3,
            0i64.into(),
        )
        .unwrap()
        .into_array();
        let sequence =
            Sequence::try_new_typed(10i64, 2i64, vortex::dtype::Nullability::NonNullable, 3)
                .unwrap()
                .into_array();
        let original = StructArray::new(
            ["runs", "sparse", "sequence"].into(),
            vec![runs, sparse, sequence],
            3,
            Validity::NonNullable,
        )
        .into_array();
        let fields = vec![
            Field::new("runs", DataType::Int64, false),
            Field::new("sparse", DataType::Int64, false),
            Field::new("sequence", DataType::Int64, false),
        ];
        let target = DataType::Struct(fields.into());
        let operation = Operation::new(1024 * 1024);
        convert_chunks_ordered(
            [Ok(original)].into_iter(),
            &session,
            &target,
            operation.clone(),
            &mut |batch| {
                for (column, expected) in
                    batch
                        .columns()
                        .iter()
                        .zip([[2, 2, 2], [0, 9, 0], [10, 12, 14]])
                {
                    assert_eq!(
                        column
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values()
                            .as_ref(),
                        expected
                    );
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(operation.budget.snapshot().submitted, 1);
        operation.output_owners.lock().clear();
    }

    #[test]
    fn empty_all_null_run_end_is_not_rejected() {
        let session = VortexSession::default();
        let values = PrimitiveArray::new(buffer![0i64], Validity::AllInvalid).into_array();
        let runs = RunEnd::try_new(
            buffer![0u32].into_array(),
            values,
            &mut session.create_execution_ctx(),
        )
        .unwrap()
        .into_array();
        let array =
            StructArray::new(["value"].into(), vec![runs], 0, Validity::NonNullable).into_array();
        let target = DataType::Struct(vec![Field::new("value", DataType::Int64, true)].into());
        let operation = Operation::new(1024 * 1024);
        convert_chunks_ordered(
            [Ok(array)].into_iter(),
            &session,
            &target,
            operation.clone(),
            &mut |batch| {
                assert_eq!(batch.num_rows(), 0);
                assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(operation.budget.snapshot().submitted, 1);
        operation.output_owners.lock().clear();
    }

    #[test]
    fn final_arrow_admission_refusal_returns_to_uncharged_serial_path() {
        let session = VortexSession::default();
        let value = vortex::array::arrays::ConstantArray::new(
            "outlined value requiring an Arrow byte heap",
            3,
        )
        .into_array();
        let array =
            StructArray::new(["value"].into(), vec![value], 3, Validity::NonNullable).into_array();
        let target = DataType::Struct(vec![Field::new("value", DataType::Utf8, false)].into());
        let operation = Operation::new(1024 * 1024);
        operation.reservations_left.store(2, Ordering::SeqCst); // metadata and preparation only
        convert_chunks_ordered(
            [Ok(array)].into_iter(),
            &session,
            &target,
            operation.clone(),
            &mut |batch| {
                assert_eq!(operation.live.load(Ordering::SeqCst), 0);
                assert_eq!(operation.budget.state.lock().bytes, 0);
                assert_eq!(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(2),
                    "outlined value requiring an Arrow byte heap"
                );
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(operation.budget.snapshot().submitted, 0);
        assert_eq!(operation.budget.snapshot().inline_fallbacks, 1);
        assert!(operation.output_owners.lock().is_empty());
    }

    #[test]
    fn admitted_empty_struct_preserves_explicit_row_count() {
        let session = VortexSession::default();
        let array = StructArray::new(
            vortex::dtype::FieldNames::empty(),
            Vec::<ArrayRef>::new(),
            3,
            Validity::NonNullable,
        )
        .into_array();
        let target = DataType::Struct(Vec::<Field>::new().into());
        let operation = Operation::new(1024 * 1024);
        convert_chunks_ordered(
            [Ok(array)].into_iter(),
            &session,
            &target,
            operation.clone(),
            &mut |batch| {
                assert_eq!(batch.num_rows(), 3);
                assert_eq!(batch.num_columns(), 0);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(operation.budget.snapshot().submitted, 1);
        operation.output_owners.lock().clear();
    }

    #[test]
    fn started_cancelled_job_keeps_owner_until_it_exits() {
        let operation = Operation::new(1024);
        let (started_tx, started_rx) = sync_channel(1);
        let (finish_tx, finish_rx) = sync_channel(1);
        let mut pending = Pending::default();
        submit(
            &mut pending,
            operation.clone(),
            operation.reserve_conversion(128).unwrap(),
            operation.budget.acquire(128).unwrap(),
            move || {
                started_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
                Ok(batch(1))
            },
        )
        .unwrap();
        started_rx.recv().unwrap();
        operation.cancelled.store(true, Ordering::SeqCst);
        assert_eq!(operation.live.load(Ordering::SeqCst), 1);
        assert_eq!(operation.budget.state.lock().bytes, 128);
        finish_tx.send(()).unwrap();
        assert!(matches!(
            pending.flush(&mut |_| panic!("callback after cancellation")),
            Err(VixError::Cancelled)
        ));
        drop(pending);
        assert_eq!(operation.live.load(Ordering::SeqCst), 0);
        assert_eq!(operation.budget.state.lock().bytes, 0);
    }

    #[test]
    fn worker_panic_is_an_error_and_releases_reservations() {
        let operation = Operation::new(1024);
        let mut pending = Pending::default();
        submit(
            &mut pending,
            operation.clone(),
            operation.reserve_conversion(128).unwrap(),
            operation.budget.acquire(128).unwrap(),
            || panic!("injected leaf panic"),
        )
        .unwrap();
        assert!(matches!(
            pending.flush(&mut |_| panic!("callback after worker panic")),
            Err(VixError::Malformed(_))
        ));
        drop(pending);
        assert_eq!(operation.live.load(Ordering::SeqCst), 0);
        assert_eq!(operation.budget.state.lock().bytes, 0);
        assert_eq!(operation.budget.snapshot().completed, 1);
    }

    #[test]
    fn callback_failure_and_unwind_drain_remaining_leaves() {
        #[derive(Debug, thiserror::Error)]
        #[error("callback sentinel")]
        struct Sentinel;
        for panic_callback in [false, true] {
            let operation = Operation::new(1024);
            let mut pending = Pending::default();
            for value in 0..2 {
                submit(
                    &mut pending,
                    operation.clone(),
                    operation.reserve_conversion(128).unwrap(),
                    operation.budget.acquire(128).unwrap(),
                    move || Ok(batch(value)),
                )
                .unwrap();
            }
            let result = catch_unwind(AssertUnwindSafe(|| {
                pending.flush(&mut |_| {
                    assert!(operation.budget.state.lock().bytes >= 128);
                    if panic_callback {
                        panic!("injected callback panic");
                    }
                    Err(VixError::Callback(anyhow::Error::new(Sentinel)))
                })
            }));
            if panic_callback {
                assert!(result.is_err());
            } else {
                let Err(VixError::Callback(error)) = result.unwrap() else {
                    panic!("callback error identity lost");
                };
                assert!(error.downcast_ref::<Sentinel>().is_some());
            }
            drop(pending);
            assert_eq!(operation.budget.snapshot().completed, 2);
            assert_eq!(operation.budget.state.lock().bytes, 0);
            operation.output_owners.lock().clear();
            assert_eq!(operation.live.load(Ordering::SeqCst), 0);
        }
    }
}
