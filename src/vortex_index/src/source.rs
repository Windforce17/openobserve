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

//! Ranged access to a `.vix` container.
//!
//! [`VixRangeSource`] abstracts "fetch these bytes of one immutable object"
//! so the readers ([`crate::VixReader::open_ranged`],
//! [`crate::VixDocs::open_ranged`]) can evaluate queries over an object-store
//! file without downloading it: the puffin footer comes from a tail fetch,
//! the dictionary from one small fetch, and the `terms`/`docs` blobs are
//! opened lazily as Vortex files whose segment reads translate to
//! chunk-granular range fetches (coalesced by vortex's IO layer).
//!
//! [`BlobReadAt`] is the bridge into vortex: it implements
//! [`vortex::io::VortexReadAt`] over a byte *window* of the source (one
//! puffin blob), adding the blob's base offset to every read. We bridge at
//! the `VortexReadAt` level (not `SegmentSource`) so vortex keeps its own
//! request coalescing, alignment handling and footer machinery.

use std::{
    cell::{Cell, RefCell},
    fmt,
    ops::Range,
    sync::{Arc, OnceLock},
};

use bytes::{Bytes, BytesMut};
use futures::{FutureExt, future::BoxFuture};
use vortex::{
    array::buffer::BufferHandle,
    buffer::{Alignment, ByteBuffer},
    error::{VortexResult, vortex_err},
    file::{DeserializeStep, Footer},
    io::{CoalesceConfig, VortexReadAt},
    session::VortexSession,
};

use crate::error::{Result, VixError};

/// Cancellation state belonging to one operation, never to a cached reader.
pub trait VixReadOperation: Send + Sync {
    fn is_cancelled(&self) -> bool;

    /// Admit absolute reader-owned bytes (retained plus pending allocations).
    /// This is independent of physical IO admission and defaults to unrestricted.
    fn check_memory(&self, _owned_bytes: usize) -> Result<()> {
        Ok(())
    }

    /// Execution policy is private to this operation, never to a cached reader.
    fn scan_options(&self) -> crate::NativeScanOptions {
        crate::NativeScanOptions::default()
    }

    /// Only callers with strong allocation ownership may queue conversions.
    fn supports_conversion(&self) -> bool {
        false
    }

    /// Reserve an additional, independently owned allocation before creating it.
    /// The returned owner must remain alive until that allocation is released.
    fn reserve_conversion(&self, _bytes: usize) -> Result<Box<dyn Send + Sync>> {
        Err(VixError::Malformed(
            "operation does not own conversion memory".to_string(),
        ))
    }

    /// Transfer an admitted conversion's Arrow buffers to downstream owners
    /// before its native envelope releases credits. Never called for unknown
    /// inline backing trees.
    fn own_conversion_output(
        &self,
        _batch: arrow::record_batch::RecordBatch,
        _owner: Arc<dyn Send + Sync>,
    ) -> Result<arrow::record_batch::RecordBatch> {
        Err(VixError::Malformed(
            "operation does not own downstream conversion buffers".to_string(),
        ))
    }
}

thread_local! {
    static READ_OPERATION: RefCell<Option<Arc<dyn VixReadOperation>>> = const { RefCell::new(None) };
    /// Scan-local override of the vortex coalescing gap (`None` = the
    /// object-store default of 1 MiB).
    static COALESCE_DISTANCE: Cell<Option<u64>> = const { Cell::new(None) };
    static READER_MEMORY: RefCell<Option<Arc<crate::reader::ReaderMemory>>> = const { RefCell::new(None) };
    /// Windows prefetched for ONE ranged blob (identified by its footer
    /// state) by the operation running on this thread.
    static PREFETCHED: RefCell<Option<(usize, Arc<PrefetchedWindows>)>> = const { RefCell::new(None) };
}

/// Byte windows of one blob's object fetched ahead of the scans that need
/// them (the field-scoped prefetch bundle): absolute source offsets, kept
/// sorted and non-overlapping. The IO bridge serves any read lying fully
/// inside a window without touching the source. Operation-scoped — registered
/// for one blob through [`RangedBlob::prefetch_scope`], never reader state.
pub(crate) struct PrefetchedWindows {
    windows: parking_lot::RwLock<Vec<(Range<u64>, Bytes)>>,
}

impl PrefetchedWindows {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            windows: parking_lot::RwLock::new(Vec::new()),
        })
    }

    /// Register `bytes` as the content of `range`, merging with adjacent or
    /// overlapping windows so a coalesced read over neighbours stays covered.
    pub(crate) fn add(&self, range: Range<u64>, bytes: Bytes) {
        debug_assert_eq!(bytes.len() as u64, range.end - range.start);
        if range.is_empty() {
            return;
        }
        let mut windows = self.windows.write();
        let first = windows.partition_point(|(window, _)| window.end < range.start);
        let last = windows.partition_point(|(window, _)| window.start <= range.end);
        if first == last {
            windows.insert(first, (range, bytes));
            return;
        }
        // Every drained neighbour touches `range`, and neighbours are pairwise
        // disjoint, so in start order each part begins at or before the bytes
        // stitched so far — the union is one gap-free window.
        let mut parts: Vec<(Range<u64>, Bytes)> = windows.drain(first..last).collect();
        parts.push((range, bytes));
        parts.sort_by_key(|(window, _)| window.start);
        let start = parts[0].0.start;
        let end = parts
            .iter()
            .map(|(window, _)| window.end)
            .max()
            .expect("at least the new window");
        let mut merged = BytesMut::with_capacity((end - start) as usize);
        let mut offset = start;
        for (window, data) in parts {
            if window.end <= offset {
                continue;
            }
            debug_assert!(window.start <= offset, "prefetch windows must touch");
            merged.extend_from_slice(&data[(offset - window.start) as usize..]);
            offset = window.end;
        }
        debug_assert_eq!(offset, end);
        windows.insert(first, (start..end, merged.freeze()));
    }

    /// The bytes of `range` when a single window covers all of it.
    pub(crate) fn covering(&self, range: &Range<u64>) -> Option<Bytes> {
        let windows = self.windows.read();
        let index = windows
            .partition_point(|(window, _)| window.start <= range.start)
            .checked_sub(1)?;
        let (window, bytes) = &windows[index];
        (range.end <= window.end).then(|| {
            bytes.slice((range.start - window.start) as usize..(range.end - window.start) as usize)
        })
    }

    pub(crate) fn covers(&self, range: &Range<u64>) -> bool {
        range.is_empty() || self.covering(range).is_some()
    }
}

/// Restores the previously registered prefetch when dropped.
pub(crate) struct PrefetchScope(Option<(usize, Arc<PrefetchedWindows>)>);

impl Drop for PrefetchScope {
    fn drop(&mut self) {
        let inner = PREFETCHED.with(|slot| slot.replace(self.0.take()));
        drop(inner);
    }
}

/// Run synchronous work with operation-local cancellation, restoring the
/// previous scope on normal return, nested calls, and unwinding.
pub fn with_read_operation<T>(operation: Arc<dyn VixReadOperation>, work: impl FnOnce() -> T) -> T {
    struct Restore(Option<Arc<dyn VixReadOperation>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let inner = READ_OPERATION.with(|slot| slot.replace(self.0.take()));
            drop(inner);
        }
    }
    let _restore = Restore(READ_OPERATION.with(|slot| slot.replace(Some(operation))));
    let _native_scope = crate::container::enter_native_read_scope();
    work()
}

/// Count metadata is physically interleaved with large postings segments.
/// Coalesce adjacent count reads, but never pay for an unrequested gap.
/// The setting is captured by the ephemeral IO bridge, not cached footers.
pub(crate) fn with_exact_range_reads<T>(work: impl FnOnce() -> T) -> T {
    with_coalesce_distance(0, work)
}

/// Run `work` with vortex segment coalescing limited to gaps of at most
/// `distance` bytes: point reads of a few cells spread over a blob merge
/// each chunk's adjacent column segments into one request without
/// fetching the untouched chunks between them.
pub(crate) fn with_coalesce_distance<T>(distance: u64, work: impl FnOnce() -> T) -> T {
    struct Restore(Option<u64>);
    impl Drop for Restore {
        fn drop(&mut self) {
            COALESCE_DISTANCE.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(COALESCE_DISTANCE.with(|slot| slot.replace(Some(distance))));
    work()
}

pub(crate) fn current_read_operation() -> Option<Arc<dyn VixReadOperation>> {
    READ_OPERATION.with(|slot| slot.borrow().clone())
}

/// Check the current synchronous operation without modifying shared state.
pub fn check_read_cancelled() -> std::result::Result<(), VixError> {
    if current_read_operation().is_some_and(|op| op.is_cancelled()) {
        Err(VixError::Cancelled)
    } else {
        Ok(())
    }
}

/// Admit current/pending reader ownership before allocating. Preserve the
/// operation's typed error chain, with cancellation taking precedence.
pub fn check_read_memory(owned_bytes: usize) -> Result<()> {
    check_operation_memory(current_read_operation().as_deref(), owned_bytes)
}

pub(crate) fn check_operation_memory(
    operation: Option<&dyn VixReadOperation>,
    owned_bytes: usize,
) -> Result<()> {
    if let Some(operation) = operation {
        if operation.is_cancelled() {
            return Err(VixError::Cancelled);
        }
        let result = operation.check_memory(owned_bytes);
        if operation.is_cancelled() {
            return Err(VixError::Cancelled);
        }
        result?;
    }
    Ok(())
}

pub(crate) struct ReaderMemoryScope(Option<Arc<crate::reader::ReaderMemory>>);

impl Drop for ReaderMemoryScope {
    fn drop(&mut self) {
        READER_MEMORY.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

pub(crate) fn enter_reader_memory(memory: Arc<crate::reader::ReaderMemory>) -> ReaderMemoryScope {
    ReaderMemoryScope(READER_MEMORY.with(|slot| slot.replace(Some(memory))))
}

pub(crate) fn current_reader_memory_if_present() -> Option<Arc<crate::reader::ReaderMemory>> {
    READER_MEMORY.with(|slot| slot.borrow().clone())
}

pub(crate) fn current_reader_memory() -> Arc<crate::reader::ReaderMemory> {
    current_reader_memory_if_present()
        .unwrap_or_else(|| Arc::new(crate::reader::ReaderMemory::new()))
}

fn fetch_error(error: anyhow::Error) -> VixError {
    if check_read_cancelled().is_err()
        || matches!(error.downcast_ref::<VixError>(), Some(VixError::Cancelled))
    {
        VixError::Cancelled
    } else {
        // Keep the original typed IO error/cause rather than relabeling it as
        // corrupt immutable file contents.
        VixError::Callback(error)
    }
}

/// Detach a long-lived slice from any larger parent allocation. Unique IO
/// buffers transfer ownership without copying; shared slices copy only their
/// visible window. Shrinking removes unused capacity from retained owners.
pub(crate) fn compact_bytes(bytes: Bytes) -> Bytes {
    let mut bytes = Vec::<u8>::from(bytes);
    bytes.shrink_to_fit();
    Bytes::from(bytes)
}

/// A random-access byte source over one immutable `.vix` object.
///
/// Contract:
/// - `len()` is the exact object size in bytes; `fetch(range)` must return exactly `range.end -
///   range.start` bytes for any `range` within `0..len()`.
/// - The returned future must be **executor-agnostic**: it is polled on vortex's single-thread
///   executor (no tokio reactor). Implementations doing real IO should run the IO on their own
///   runtime and hand the result over a channel; in-memory implementations can return ready
///   futures.
/// The trivial in-memory [`VixRangeSource`]: ranges slice a resident
/// `Bytes`. Tests and benches use it to drive the ranged merge/read paths
/// over fabricated files; production sources fetch from the cache ladder
/// or the object store instead.
pub struct BytesRangeSource {
    pub name: String,
    pub data: Bytes,
}

impl BytesRangeSource {
    pub fn new(name: impl Into<String>, data: Bytes) -> Arc<dyn VixRangeSource> {
        Arc::new(Self {
            name: name.into(),
            data: compact_bytes(data),
        })
    }
}

impl VixRangeSource for BytesRangeSource {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let result = if range.end > self.data.len() as u64 || range.start > range.end {
            Err(anyhow::anyhow!(
                "range {range:?} out of bounds for {} ({} bytes)",
                self.name,
                self.data.len()
            ))
        } else {
            Ok(self.data.slice(range.start as usize..range.end as usize))
        };
        futures::future::ready(result).boxed()
    }

    fn describe(&self) -> String {
        self.name.clone()
    }

    fn retained_bytes(&self) -> usize {
        self.data.len() + self.name.capacity() + std::mem::size_of::<Self>()
    }

    fn resident(&self, range: Range<u64>) -> Option<Bytes> {
        (range.start <= range.end && range.end <= self.data.len() as u64)
            .then(|| self.data.slice(range.start as usize..range.end as usize))
    }
}

pub trait VixRangeSource: Send + Sync + 'static {
    /// Total object size in bytes.
    fn len(&self) -> u64;

    /// Bind ephemeral IO to the caller's operation. Cached sources must
    /// remain operation-independent; the returned source is scan-local.
    fn for_current_operation(&self) -> Option<Arc<dyn VixRangeSource>> {
        None
    }

    /// Heap ownership retained by this immutable source (not transient IO).
    fn retained_bytes(&self) -> usize {
        0
    }

    /// A replacement of this source that retains only `keep` (a blob's
    /// Vortex footer window), releasing eagerly retained data bytes.
    /// `None` (the default) when this source retains nothing releasable.
    /// Called when the owning reader is demoted to a metadata-only cache
    /// tier: released bytes are re-fetched through the normal ranged
    /// paths on first use.
    fn trim_retained_tail(&self, _keep: Range<u64>) -> Option<Arc<dyn VixRangeSource>> {
        None
    }

    /// Whether the object is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Fetch exactly the bytes of `range` (end-exclusive, within `0..len()`).
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>>;

    /// Fetch several ranges in ONE round trip where the backend supports it
    /// (the cache ladder / S3 issue one batched request). The default chains
    /// [`VixRangeSource::fetch`] sequentially — correct everywhere, batched
    /// nowhere. Results are positional.
    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        let futs: Vec<_> = ranges.into_iter().map(|r| self.fetch(r)).collect();
        Box::pin(async move {
            let mut out = Vec::with_capacity(futs.len());
            for fut in futs {
                out.push(fut.await?);
            }
            Ok(out)
        })
    }

    /// [`VixRangeSource::fetch_many`] for sparse windows: a coalescing
    /// backend must not merge two ranges across a gap wider than `max_gap`
    /// bytes. The default ignores the bound (correct, but a coalescing
    /// source then fetches the gaps too).
    fn fetch_many_sparse(
        &self,
        ranges: Vec<Range<u64>>,
        _max_gap: u64,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        self.fetch_many(ranges)
    }

    /// A short description of the object (e.g. its storage path), used in
    /// error messages.
    fn describe(&self) -> String {
        "<vix range source>".to_string()
    }

    /// Bytes of `range` this source already holds in memory (an eager tail
    /// probe, an in-memory object), so a planner can skip IO for them.
    /// Default: nothing resident.
    fn resident(&self, _range: Range<u64>) -> Option<Bytes> {
        None
    }
}

/// Block the current thread on one `fetch_many` and validate every returned
/// length (see [`block_fetch`]; same blocking-thread contract).
pub(crate) fn block_fetch_many(
    source: &dyn VixRangeSource,
    ranges: Vec<Range<u64>>,
) -> Result<Vec<Bytes>> {
    block_fetch_many_with(source, ranges, None)
}

/// [`block_fetch_many`] for SPARSE windows: the backend may still merge
/// neighbouring ranges into one physical read, but never across a gap wider
/// than `max_gap` bytes — skip-group windows selected inside a long postings
/// record must not silently re-fetch the record between them.
pub(crate) fn block_fetch_many_sparse(
    source: &dyn VixRangeSource,
    ranges: Vec<Range<u64>>,
    max_gap: u64,
) -> Result<Vec<Bytes>> {
    block_fetch_many_with(source, ranges, Some(max_gap))
}

fn block_fetch_many_with(
    source: &dyn VixRangeSource,
    ranges: Vec<Range<u64>>,
    max_gap: Option<u64>,
) -> Result<Vec<Bytes>> {
    check_read_cancelled()?;
    for range in &ranges {
        if range.start > range.end || range.end > source.len() {
            return Err(VixError::Malformed(format!(
                "range {}..{} out of bounds for {} ({} bytes)",
                range.start,
                range.end,
                source.describe(),
                source.len()
            )));
        }
    }
    let expected: Vec<usize> = ranges.iter().map(|r| (r.end - r.start) as usize).collect();
    let bound = source.for_current_operation();
    let source = bound.as_deref().unwrap_or(source);
    let fetch = match max_gap {
        Some(max_gap) => source.fetch_many_sparse(ranges, max_gap),
        None => source.fetch_many(ranges),
    };
    let all = futures::executor::block_on(fetch).map_err(fetch_error)?;
    check_read_cancelled()?;
    if all.len() != expected.len() {
        return Err(VixError::Malformed(format!(
            "batched fetch of {} returned {} ranges, expected {}",
            source.describe(),
            all.len(),
            expected.len()
        )));
    }
    for (bytes, expected) in all.iter().zip(&expected) {
        if bytes.len() != *expected {
            return Err(VixError::Malformed(format!(
                "batched fetch of {} returned {} bytes for a {expected}-byte range",
                source.describe(),
                bytes.len()
            )));
        }
    }
    Ok(all)
}

/// Block the current thread on two independent fetches of two objects
/// issued concurrently — ONE round trip for a data/sidecar tail pair — and
/// validate both lengths (same blocking-thread contract as [`block_fetch`]).
pub(crate) fn block_fetch_pair(
    first: &dyn VixRangeSource,
    first_range: Range<u64>,
    second: &dyn VixRangeSource,
    second_range: Range<u64>,
) -> Result<(Bytes, Bytes)> {
    let mut fetched = block_fetch_bundle(vec![
        (first, vec![first_range]),
        (second, vec![second_range]),
    ])?
    .into_iter();
    let mut next = || {
        fetched
            .next()
            .and_then(|mut batch| batch.pop())
            .expect("bundle validated two single-range batches")
    };
    Ok((next(), next()))
}

/// Concurrent range fetches one operation keeps in flight against one
/// object: the vortex IO bridge's `concurrency`, and the fan-out of the
/// scalar/bundle helpers below. Reads issued within one such wave cost one
/// round trip of latency together.
pub(crate) const FETCH_CONCURRENCY: usize = 8;

/// Fetch planned disjoint metadata windows concurrently without offering
/// their gaps to a downstream `fetch_many` coalescer.
pub(crate) fn block_fetch_separate(
    source: &dyn VixRangeSource,
    ranges: Vec<Range<u64>>,
) -> Result<Vec<Bytes>> {
    Ok(block_fetch_bundle(
        ranges
            .into_iter()
            .map(|range| (source, vec![range]))
            .collect(),
    )?
    .into_iter()
    .map(|mut batch| batch.remove(0))
    .collect())
}

/// Block the current thread on several `fetch_many` batches — each on its
/// own source, each free to be gap-coalesced by that source's backend, all
/// issued concurrently (up to [`FETCH_CONCURRENCY`] in flight) — so one
/// round trip carries a coalescable batch AND exact windows that must never
/// be offered to a coalescer (a single-range batch). Results are positional
/// per batch and every length is validated.
pub(crate) fn block_fetch_bundle(
    batches: Vec<(&dyn VixRangeSource, Vec<Range<u64>>)>,
) -> Result<Vec<Vec<Bytes>>> {
    use futures::{StreamExt, TryStreamExt};
    check_read_cancelled()?;
    for (source, ranges) in &batches {
        for range in ranges {
            if range.start > range.end || range.end > source.len() {
                return Err(VixError::Malformed(format!(
                    "range {}..{} out of bounds for {} ({} bytes)",
                    range.start,
                    range.end,
                    source.describe(),
                    source.len(),
                )));
            }
        }
    }
    let bound: Vec<Option<Arc<dyn VixRangeSource>>> = batches
        .iter()
        .map(|(source, _)| source.for_current_operation())
        .collect();
    let reads = futures::stream::iter(batches.into_iter().zip(&bound).map(
        |((source, ranges), bound)| async move {
            let source = bound.as_deref().unwrap_or(source);
            check_read_cancelled()?;
            let expected: Vec<usize> = ranges.iter().map(|r| (r.end - r.start) as usize).collect();
            let all = source.fetch_many(ranges).await.map_err(fetch_error)?;
            check_read_cancelled()?;
            if all.len() != expected.len() {
                return Err(VixError::Malformed(format!(
                    "batched fetch of {} returned {} ranges, expected {}",
                    source.describe(),
                    all.len(),
                    expected.len()
                )));
            }
            for (bytes, expected) in all.iter().zip(&expected) {
                if bytes.len() != *expected {
                    return Err(VixError::Malformed(format!(
                        "batched fetch of {} returned {} bytes for a {expected}-byte range",
                        source.describe(),
                        bytes.len()
                    )));
                }
            }
            Ok(all)
        },
    ))
    .buffered(FETCH_CONCURRENCY)
    .try_collect();
    futures::executor::block_on(reads)
}

/// Block the current thread on one `fetch` and validate the returned length.
///
/// Only used from the synchronous reader entry points, which by contract run
/// on blocking threads (never on an async executor).
pub(crate) fn block_fetch(source: &dyn VixRangeSource, range: Range<u64>) -> Result<Bytes> {
    check_read_cancelled()?;
    if range.start > range.end || range.end > source.len() {
        return Err(VixError::Malformed(format!(
            "range {}..{} out of bounds for {} ({} bytes)",
            range.start,
            range.end,
            source.describe(),
            source.len()
        )));
    }
    let expected = (range.end - range.start) as usize;
    let bound = source.for_current_operation();
    let source = bound.as_deref().unwrap_or(source);
    let bytes = futures::executor::block_on(source.fetch(range.clone())).map_err(fetch_error)?;
    check_read_cancelled()?;
    if bytes.len() != expected {
        return Err(VixError::Malformed(format!(
            "fetch {}..{} of {} returned {} bytes, expected {expected}",
            range.start,
            range.end,
            source.describe(),
            bytes.len()
        )));
    }
    Ok(bytes)
}

/// A byte window of a [`VixRangeSource`] (one puffin blob), lazily opened as
/// a Vortex file. Only immutable encoded footer ranges survive an open.
/// Native footer/layout objects belong to the operation that decodes them.
pub(crate) struct RangedBlob {
    pub source: Arc<dyn VixRangeSource>,
    /// Absolute byte range of the blob inside the source object.
    pub range: Range<u64>,
    footer: Arc<FooterState>,
}

struct FooterState {
    ranges: OnceLock<Vec<(Range<u64>, Bytes)>>,
    memory: OnceLock<Arc<crate::reader::ReaderMemory>>,
}

impl FooterState {
    fn retained_bytes(ranges: &Vec<(Range<u64>, Bytes)>) -> usize {
        ranges.capacity() * std::mem::size_of::<(Range<u64>, Bytes)>()
            + ranges
                .iter()
                .map(|(_, bytes)| bytes.len() + 4 * std::mem::size_of::<usize>())
                .sum::<usize>()
    }

    /// The retained footer window `[start, blob_end)`: the recorded
    /// ranges are one contiguous suffix (the open's initial read plus any
    /// `NeedMoreData` prefixes, trimmed to what the parse consumes). The
    /// window's length is exactly what a later open must pass to
    /// `VortexOpenOptions::with_initial_read_size` so its initial suffix
    /// read is served entirely from `ranges` — zero fetches, no
    /// `NeedMoreData` prefix. `None` when nothing is cached yet or the
    /// recorded ranges are not a contiguous suffix ending at the blob end
    /// (only contrived non-open recordings; callers then keep today's
    /// 256 KiB-window behaviour).
    fn window(&self, blob_end: u64) -> Option<Range<u64>> {
        let ranges = self.ranges.get()?;
        let mut start = blob_end;
        for (range, _) in ranges.iter().rev() {
            if range.end != start {
                return None;
            }
            start = range.start;
        }
        (start < blob_end).then(|| start..blob_end)
    }
}

/// Reuse every covered byte, including a request that only partly overlaps
/// the encoded footer. Missing windows alone reach the underlying source.
async fn fetch_footer_range(
    source: &dyn VixRangeSource,
    footer: &FooterState,
    range: Range<u64>,
    operation: Option<&dyn VixReadOperation>,
) -> VortexResult<Bytes> {
    let Some(ranges) = footer.ranges.get() else {
        return fetch_native_range(source, range, operation).await;
    };
    if let Some((cached, bytes)) = ranges
        .iter()
        .find(|(cached, _)| cached.start <= range.start && range.end <= cached.end)
    {
        return Ok(
            bytes.slice((range.start - cached.start) as usize..(range.end - cached.start) as usize)
        );
    }
    if !ranges
        .iter()
        .any(|(cached, _)| cached.start < range.end && range.start < cached.end)
    {
        return fetch_native_range(source, range, operation).await;
    }
    let mut bytes = BytesMut::with_capacity((range.end - range.start) as usize);
    let mut offset = range.start;
    for (cached, data) in ranges {
        if cached.end <= offset || cached.start >= range.end {
            continue;
        }
        if offset < cached.start {
            bytes.extend_from_slice(
                &fetch_native_range(source, offset..cached.start, operation).await?,
            );
            offset = cached.start;
        }
        let end = cached.end.min(range.end);
        bytes.extend_from_slice(
            &data[(offset - cached.start) as usize..(end - cached.start) as usize],
        );
        offset = end;
    }
    if offset < range.end {
        bytes.extend_from_slice(&fetch_native_range(source, offset..range.end, operation).await?);
    }
    Ok(bytes.freeze())
}

async fn fetch_native_range(
    source: &dyn VixRangeSource,
    range: Range<u64>,
    operation: Option<&dyn VixReadOperation>,
) -> VortexResult<Bytes> {
    if operation.is_some_and(|op| op.is_cancelled()) {
        return Err(vortex_err!(External: VixError::Cancelled));
    }
    let bytes = source
        .fetch(range.clone())
        .await
        .map_err(|error| vortex_err!(External: VixError::Callback(error)))?;
    if operation.is_some_and(|op| op.is_cancelled()) {
        return Err(vortex_err!(External: VixError::Cancelled));
    }
    if bytes.len() as u64 != range.end - range.start {
        return Err(vortex_err!(
            "fetch {range:?} of {} returned {} bytes",
            source.describe(),
            bytes.len()
        ));
    }
    Ok(bytes)
}

impl RangedBlob {
    pub fn new(source: Arc<dyn VixRangeSource>, range: Range<u64>) -> Self {
        Self {
            source,
            range,
            footer: Arc::new(FooterState {
                ranges: OnceLock::new(),
                memory: OnceLock::new(),
            }),
        }
    }

    /// Blob length in bytes.
    pub fn len(&self) -> u64 {
        self.range.end - self.range.start
    }

    /// Identity of this blob for operation-scoped prefetch registration:
    /// the footer state is unique per blob handle and stable for its life.
    fn prefetch_id(&self) -> usize {
        Arc::as_ptr(&self.footer) as *const () as usize
    }

    /// Register `windows` for reads of THIS blob on the current thread until
    /// the returned scope drops (nested scopes restore their predecessor).
    pub(crate) fn prefetch_scope(&self, windows: Arc<PrefetchedWindows>) -> PrefetchScope {
        PrefetchScope(PREFETCHED.with(|slot| slot.replace(Some((self.prefetch_id(), windows)))))
    }

    /// Whether an open already retained this blob's encoded Vortex footer.
    pub(crate) fn footer_cached(&self) -> bool {
        self.footer.ranges.get().is_some()
    }

    /// Absolute window of the initial Vortex footer read an open performs:
    /// [`VORTEX_FOOTER_INITIAL_READ_BYTES`] from the blob end, clamped to
    /// the blob — or, once an open retained this blob's footer, exactly the
    /// RETAINED suffix ([`FooterState::window`], at most the initial
    /// window). Planning (`prefetch_field_bundle`, tail-residency checks)
    /// must fetch what is actually retained: the retained state serves a
    /// later open with `with_initial_read_size(retained_len)`, so no path
    /// ever re-fetches the footer of an opened blob.
    pub(crate) fn footer_window(&self) -> Range<u64> {
        self.footer
            .window(self.range.end)
            .unwrap_or_else(|| self.initial_footer_window())
    }

    /// The uncached initial-read window: a suffix read of up to
    /// [`VORTEX_FOOTER_INITIAL_READ_BYTES`] bytes.
    fn initial_footer_window(&self) -> Range<u64> {
        self.range.end - self.len().min(VORTEX_FOOTER_INITIAL_READ_BYTES)..self.range.end
    }

    /// Byte length a later open passes to
    /// `VortexOpenOptions::with_initial_read_size` (vortex hard-floors it at
    /// `MAX_POSTSCRIPT_SIZE + EOF`): the retained window once cached, else
    /// [`VORTEX_FOOTER_INITIAL_READ_BYTES`].
    pub(crate) fn footer_initial_read_bytes(&self) -> u64 {
        self.footer
            .window(self.range.end)
            .map_or(VORTEX_FOOTER_INITIAL_READ_BYTES, |window| {
                window.end - window.start
            })
    }

    /// Absolute window a demoted reader must keep servable without IO:
    /// the RETAINED footer window of an opened blob (its exact bounds are
    /// published for tests and planning), else the postscript-sized
    /// eager-tail suffix. An opened blob's footer state serves this whole
    /// window with zero IO.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn retained_footer_window(&self) -> Range<u64> {
        self.footer.window(self.range.end).unwrap_or_else(|| {
            self.range.end - self.len().min(VORTEX_FOOTER_READ_BYTES)..self.range.end
        })
    }

    /// Demote the underlying source to metadata-only retention: keep the
    /// blob's Vortex footer window servable, release retained eager-tail
    /// data bytes. Returns the released bytes (0 when nothing changed).
    ///
    /// An OPENED blob keeps nothing in the source: the retained footer
    /// state serves the whole footer window (data bytes re-fetch on
    /// demand, the demote contract). An unopened blob keeps the
    /// postscript-sized suffix through the eager tail, exactly as before.
    pub(crate) fn trim_retained_tail(&mut self) -> usize {
        let keep = if self.footer_cached() {
            self.range.end..self.range.end
        } else {
            self.range.end - self.len().min(VORTEX_FOOTER_READ_BYTES)..self.range.end
        };
        let before = self.source.retained_bytes();
        let Some(trimmed) = self.source.trim_retained_tail(keep) else {
            return 0;
        };
        self.source = trimmed;
        before.saturating_sub(self.source.retained_bytes())
    }

    /// Whether the footer window is servable without IO: retained by an
    /// earlier open (the cached window always covers the initial read a
    /// later open performs — they are the same suffix), held by the source
    /// (eager tail), or prefetched on this thread.
    pub(crate) fn footer_resident(&self) -> bool {
        if self.footer_cached() {
            return true;
        }
        let window = self.initial_footer_window();
        if self.source.resident(window.clone()).is_some() {
            return true;
        }
        PREFETCHED.with(|slot| {
            slot.borrow()
                .as_ref()
                .is_some_and(|(id, windows)| *id == self.prefetch_id() && windows.covers(&window))
        })
    }

    /// Attached during reader construction, before the reader is shared.
    pub(crate) fn track_memory(&self, memory: Arc<crate::reader::ReaderMemory>) {
        if self.footer.memory.set(Arc::clone(&memory)).is_ok() {
            memory.add(std::mem::size_of::<FooterState>() + 2 * std::mem::size_of::<usize>());
            if let Some(ranges) = self.footer.ranges.get() {
                memory.add(FooterState::retained_bytes(ranges));
            }
        }
    }

    /// The vortex IO bridge over this window.
    pub fn read_at(&self) -> BlobReadAt {
        BlobReadAt {
            source: self
                .source
                .for_current_operation()
                .unwrap_or_else(|| Arc::clone(&self.source)),
            range: self.range.clone(),
            operation: current_read_operation(),
            footer: Arc::clone(&self.footer),
            coalesce_distance: COALESCE_DISTANCE.with(Cell::get),
            opening_memory: None,
            prefetched: PREFETCHED.with(|slot| {
                slot.borrow()
                    .as_ref()
                    .filter(|(id, _)| *id == self.prefetch_id())
                    .map(|(_, windows)| Arc::clone(windows))
            }),
        }
    }

    pub(crate) fn opening_read_at(&self) -> (BlobReadAt, Arc<OpeningMemory>) {
        let opening = Arc::new(OpeningMemory {
            memory: self.reader_memory(),
            footer: Arc::clone(&self.footer),
            blob_end: self.range.end,
            state: parking_lot::Mutex::new(OpeningState {
                active: true,
                pending: None,
                ranges: Vec::new(),
            }),
        });
        let mut read = self.read_at();
        read.opening_memory = Some(Arc::clone(&opening));
        (read, opening)
    }

    pub(crate) fn reader_memory(&self) -> Arc<crate::reader::ReaderMemory> {
        self.footer
            .memory
            .get()
            .cloned()
            .unwrap_or_else(current_reader_memory)
    }
}

impl fmt::Debug for RangedBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangedBlob")
            .field("source", &self.source.describe())
            .field("range", &self.range)
            .field("footer_cached", &self.footer.ranges.get().is_some())
            .finish()
    }
}

/// Captured only by an ephemeral native IO bridge. Disable immediately after
/// footer open so segment scans never turn historical IO into owned memory.
pub(crate) struct OpeningMemory {
    memory: Arc<crate::reader::ReaderMemory>,
    footer: Arc<FooterState>,
    /// Absolute end offset of the blob: the retained footer window is a
    /// suffix `[consumed_start, blob_end)`.
    blob_end: u64,
    state: parking_lot::Mutex<OpeningState>,
}

struct OpeningState {
    active: bool,
    // Drop encoded buffers before releasing their opening reservation.
    ranges: Vec<(Range<u64>, Bytes)>,
    pending: Option<crate::reader::PendingMemory>,
}

impl OpeningMemory {
    fn reserve(&self, length: usize, operation: Option<&dyn VixReadOperation>) -> Result<bool> {
        let mut state = self.state.lock();
        if !state.active {
            return Ok(false);
        }
        let pending = self
            .memory
            .reserve_with(crate::container::metadata_memory_bound(length), |owned| {
                check_operation_memory(operation, owned)
            })?;
        state.pending = Some(match state.pending.take() {
            Some(previous) => previous.merge(pending),
            None => pending,
        });
        Ok(true)
    }

    fn retain(&self, range: Range<u64>, bytes: Bytes) -> Bytes {
        if self.footer.ranges.get().is_some() {
            return bytes;
        }
        // Admission covers the input, compact copy, range directory and
        // native decoding workspace. Never pin a larger source owner.
        let bytes = compact_bytes(bytes);
        self.state.lock().ranges.push((range, bytes.clone()));
        bytes
    }

    pub(crate) fn finish(&self) -> Option<crate::reader::PendingMemory> {
        let mut state = self.state.lock();
        state.active = false;
        let mut ranges = std::mem::take(&mut state.ranges);
        if !ranges.is_empty() {
            ranges.sort_unstable_by_key(|(range, _)| range.start);
            // Trim the fetched window to the single suffix the Vortex
            // footer parse actually consumes: [consumed_start, blob_end)
            // with consumed_start = min(dtype?, layout, stats?, footer)
            // segment offset from the postscript, floored at the
            // postscript-sized suffix a Vortex open's initial read is
            // hard-floored to. The retained window then serves every later
            // open's initial read in full (see
            // [`RangedBlob::footer_initial_read_bytes`]) while the unused
            // prefix of the 256 KiB window is released. Any parse failure
            // keeps the whole recorded window — today's behaviour.
            let retained = Self::trim_to_consumed_suffix(&ranges, self.blob_end).unwrap_or(ranges);
            if self.footer.ranges.set(retained.clone()).is_ok()
                && let Some(memory) = self.footer.memory.get()
            {
                memory.add(FooterState::retained_bytes(&retained));
                memory.notify();
            }
        }
        state.pending.take()
    }

    /// Coalesce the open's recorded ranges into the single consumed suffix
    /// `[consumed_start, blob_end)`, `None` on any postscript parse error
    /// (the caller keeps the recorded window unchanged then). The recorded
    /// ranges are suffix-shaped (the open's initial read plus any
    /// `NeedMoreData` prefixes), so the trim only drops an unused PREFIX.
    fn trim_to_consumed_suffix(
        ranges: &[(Range<u64>, Bytes)],
        blob_end: u64,
    ) -> Option<Vec<(Range<u64>, Bytes)>> {
        let (tail_range, tail) = ranges.last()?;
        // The open must have read through the blob end (EOF marker).
        if tail_range.end != blob_end {
            return None;
        }
        let eof = vortex::file::EOF_SIZE;
        let tail = tail.as_ref();
        if tail.len() < eof {
            return None;
        }
        // EOF record: [version u16][ps_size u16][magic 4]; the postscript
        // flatbuffer sits directly before it.
        let ps_len = u16::from_le_bytes(
            tail[tail.len() - eof + 2..tail.len() - eof + 4]
                .try_into()
                .ok()?,
        ) as usize;
        if tail.len() < ps_len + eof {
            return None;
        }
        // Drive vortex's own deserializer over the postscript+EOF suffix:
        // the FIRST step either errors (corrupt postscript — keep the
        // window) or reports `NeedMoreData { offset }` — exactly the first
        // consumed byte (min of the dtype/layout/stats/footer segment
        // offsets, no segment parsing). `Done` cannot occur for a
        // well-formed blob (the segments precede the postscript).
        let ps_eof = ByteBuffer::copy_from(&tail[tail.len() - ps_len - eof..]);
        let mut deserializer =
            Footer::deserializer(ps_eof, VortexSession::empty()).with_size(blob_end);
        let consumed_start = match deserializer.deserialize() {
            Ok(DeserializeStep::NeedMoreData { offset, .. }) => offset,
            _ => return None,
        };
        // Floor at the postscript-sized suffix (vortex hard-floors an
        // open's initial read to MAX_POSTSCRIPT_SIZE + EOF) and clamp into
        // the recorded window: never retain bytes the open did not read.
        let floor = blob_end.saturating_sub(VORTEX_FOOTER_READ_BYTES);
        let window_start = ranges.first()?.0.start;
        let start = consumed_start.min(floor).max(window_start);
        if !(window_start <= start && start < blob_end) {
            return None;
        }
        // Copy the suffix out of the recorded windows (they may be several
        // adjacent ranges); a gap before `blob_end` keeps the full window.
        let mut suffix = BytesMut::with_capacity((blob_end - start) as usize);
        let mut offset = start;
        for (range, bytes) in ranges {
            if range.end <= offset {
                continue;
            }
            if range.start > offset {
                return None;
            }
            let end = range.end.min(blob_end);
            suffix.extend_from_slice(
                &bytes[(offset - range.start) as usize..(end - range.start) as usize],
            );
            offset = end;
        }
        if offset != blob_end {
            return None;
        }
        let bytes = compact_bytes(suffix.freeze());
        debug_assert_eq!(bytes.len() as u64, blob_end - start);
        Some(vec![(start..blob_end, bytes)])
    }
}

/// The postscript-sized suffix of a blob (its maximum postscript plus the
/// EOF marker): the minimum window a Vortex open reads (its hard floor on
/// `initial_read_size`) and — for a blob never opened — the window a
/// demoted reader keeps servable through the eager tail (see
/// [`RangedBlob::trim_retained_tail`]; an OPENED blob's retained footer
/// state supersedes the tail copy entirely).
pub(crate) const VORTEX_FOOTER_READ_BYTES: u64 =
    vortex::file::MAX_POSTSCRIPT_SIZE as u64 + vortex::file::EOF_SIZE as u64;

/// The initial footer read an open performs and the field-scoped prefetch
/// bundle fetches: one 256 KiB suffix read covers the postscript AND the
/// serialized layout of prod-sized terms/docs blobs, eliminating the
/// sequential `NeedMoreData` prefix read the 64 KiB postscript window paid
/// (39-167 KB on prod sidecars). Clamped to the blob length; a blob whose
/// whole footer window lies inside the eager tail still opens without IO.
/// Byte cost, not a format: only the FETCHED window grows — the retained
/// footer state stores just the consumed suffix of it (see
/// [`OpeningMemory::finish`]).
pub(crate) const VORTEX_FOOTER_INITIAL_READ_BYTES: u64 = 256 * 1024;

/// [`VortexReadAt`] over a byte window of a [`VixRangeSource`]: every read
/// adds the window base offset and goes through `fetch`.
#[derive(Clone)]
pub(crate) struct BlobReadAt {
    source: Arc<dyn VixRangeSource>,
    range: Range<u64>,
    operation: Option<Arc<dyn VixReadOperation>>,
    footer: Arc<FooterState>,
    coalesce_distance: Option<u64>,
    opening_memory: Option<Arc<OpeningMemory>>,
    /// Operation-scoped windows registered for this blob, consulted before
    /// the retained footer and the source.
    prefetched: Option<Arc<PrefetchedWindows>>,
}

impl VortexReadAt for BlobReadAt {
    fn concurrency(&self) -> usize {
        FETCH_CONCURRENCY
    }

    /// Count-only scans merge adjacent segments without fetching intervening
    /// postings; ordinary scans retain the object-storage coalescing policy.
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        let mut config = CoalesceConfig::object_storage();
        if let Some(distance) = self.coalesce_distance {
            config.distance = distance;
        }
        Some(config)
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let len = self.range.end - self.range.start;
        async move { Ok(len) }.boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let window = self.range.clone();
        let describe = self.source.describe();
        let Some(start) = window.start.checked_add(offset) else {
            return async { Err(vortex_err!("blob read offset overflow")) }.boxed();
        };
        let Some(end) = start.checked_add(length as u64) else {
            return async { Err(vortex_err!("blob read length overflow")) }.boxed();
        };
        if end > window.end {
            return async move {
                Err(vortex_err!(
                    "blob read {offset}..{} out of bounds for a {}-byte blob of {describe}",
                    offset + length as u64,
                    window.end - window.start
                ))
            }
            .boxed();
        }
        let operation = self.operation.clone();
        if operation.as_ref().is_some_and(|op| op.is_cancelled()) {
            return async { Err(vortex_err!(External: VixError::Cancelled)) }.boxed();
        }
        // Admission precedes even creation of the backend future: some sources
        // dispatch physical IO synchronously from fetch().
        let opening = match &self.opening_memory {
            Some(memory) => match memory.reserve(length, operation.as_deref()) {
                Ok(opening) => opening,
                Err(error) => return async move { Err(vortex_err!(External: error)) }.boxed(),
            },
            None => false,
        };
        let source = Arc::clone(&self.source);
        let footer = Arc::clone(&self.footer);
        let opening_memory = self.opening_memory.clone();
        let prefetched = self.prefetched.clone();
        async move {
            if operation.as_ref().is_some_and(|op| op.is_cancelled()) {
                return Err(vortex_err!(External: VixError::Cancelled));
            }
            let bytes = match prefetched
                .as_ref()
                .and_then(|windows| windows.covering(&(start..end)))
            {
                Some(bytes) => bytes,
                None => {
                    fetch_footer_range(source.as_ref(), &footer, start..end, operation.as_deref())
                        .await?
                }
            };
            if operation.as_ref().is_some_and(|op| op.is_cancelled()) {
                return Err(vortex_err!(External: VixError::Cancelled));
            }
            if bytes.len() != length {
                return Err(vortex_err!(
                    "fetch {start}..{end} of {describe} returned {} bytes, expected {length}",
                    bytes.len()
                ));
            }
            let bytes = if opening {
                opening_memory
                    .as_ref()
                    .expect("active opening")
                    .retain(start..end, bytes)
            } else {
                bytes
            };
            Ok(BufferHandle::new_host(
                ByteBuffer::from(bytes).aligned(alignment),
            ))
        }
        .boxed()
    }
}

// Shared cached state contains bytes only; operation-local bridges also
// cross native runtime threads.
fn _assert_send_sync() {
    fn assert<T: Send + Sync>() {}
    assert::<RangedBlob>();
    assert::<BlobReadAt>();
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[test]
    fn encoded_footer_ranges_fetch_only_missing_overlap_windows() {
        struct RecordingSource {
            reads: parking_lot::Mutex<Vec<Range<u64>>>,
        }
        impl VixRangeSource for RecordingSource {
            fn len(&self) -> u64 {
                10
            }
            fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
                self.reads.lock().push(range.clone());
                futures::future::ready(Ok(Bytes::from_static(b"0123456789")
                    .slice(range.start as usize..range.end as usize)))
                .boxed()
            }
        }
        let source = Arc::new(RecordingSource {
            reads: parking_lot::Mutex::new(Vec::new()),
        });
        let blob = RangedBlob::new(source.clone(), 0..10);
        let (read, opening) = blob.opening_read_at();
        for offset in [2, 6] {
            futures::executor::block_on(read.read_at(offset, 2, Alignment::none())).unwrap();
        }
        drop(opening.finish());
        source.reads.lock().clear();
        let read = blob.read_at();
        let bytes = futures::executor::block_on(read.read_at(0, 10, Alignment::none()))
            .unwrap()
            .unwrap_host();
        assert_eq!(bytes.as_ref(), b"0123456789");
        assert_eq!(*source.reads.lock(), vec![0..2, 4..6, 8..10]);
        source.reads.lock().clear();
        let bytes = futures::executor::block_on(read.read_at(2, 2, Alignment::none()))
            .unwrap()
            .unwrap_host();
        assert_eq!(bytes.as_ref(), b"23");
        assert!(source.reads.lock().is_empty());
    }

    struct Operation(AtomicBool);

    impl VixReadOperation for Operation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::Acquire)
        }
    }

    #[test]
    fn range_errors_preserve_original_marker_through_reader_and_vortex_bridges() {
        #[derive(Debug, thiserror::Error)]
        #[error("range denied by resource budget")]
        struct Denied;

        struct DeniedSource;
        impl VixRangeSource for DeniedSource {
            fn len(&self) -> u64 {
                4
            }

            fn fetch(&self, _: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
                futures::future::ready(Err(anyhow::Error::new(Denied))).boxed()
            }
        }

        let error = anyhow::Error::new(block_fetch(&DeniedSource, 0..4).unwrap_err());
        assert!(error.chain().any(|cause| cause.is::<Denied>()));
        let error = anyhow::Error::new(block_fetch_many(&DeniedSource, vec![0..4]).unwrap_err());
        assert!(error.chain().any(|cause| cause.is::<Denied>()));
        let error =
            anyhow::Error::new(block_fetch_separate(&DeniedSource, vec![0..1, 3..4]).unwrap_err());
        assert!(error.chain().any(|cause| cause.is::<Denied>()));

        let blob = RangedBlob::new(Arc::new(DeniedSource), 0..4);
        let error = futures::executor::block_on(blob.read_at().read_at(0, 4, Alignment::none()))
            .unwrap_err();
        let error = anyhow::Error::new(VixError::Vortex(error));
        assert!(error.chain().any(|cause| cause.is::<Denied>()));
    }

    #[test]
    fn read_operation_restores_nested_and_unwound_scopes() {
        let active = Arc::new(Operation(AtomicBool::new(false)));
        let cancelled = Arc::new(Operation(AtomicBool::new(true)));
        with_read_operation(active, || {
            assert!(check_read_cancelled().is_ok());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                with_read_operation(cancelled, || {
                    assert!(matches!(check_read_cancelled(), Err(VixError::Cancelled)));
                    panic!("unwind the inner operation");
                });
            }));
            assert!(result.is_err());
            assert!(check_read_cancelled().is_ok());
        });
        assert!(check_read_cancelled().is_ok());
    }

    #[test]
    fn ephemeral_io_carries_cancellation_without_poisoning_shared_blob() {
        let source = BytesRangeSource::new("shared", Bytes::from_static(b"abcd"));
        let blob = RangedBlob::new(source, 0..4);
        let operation = Arc::new(Operation(AtomicBool::new(false)));
        let read_at = with_read_operation(operation.clone(), || blob.read_at());
        let pending = read_at.read_at(0, 4, Alignment::none());
        operation.0.store(true, Ordering::Release);
        let error = futures::executor::block_on(pending).unwrap_err();
        let error = anyhow::Error::new(error);
        assert!(error.chain().any(|cause| {
            matches!(cause.downcast_ref::<VixError>(), Some(VixError::Cancelled))
        }));
        let fresh = blob.read_at();
        let bytes = futures::executor::block_on(fresh.read_at(0, 4, Alignment::none()))
            .unwrap()
            .unwrap_host();
        assert_eq!(bytes.as_ref(), b"abcd");
    }

    #[test]
    fn compact_retained_window_releases_the_parent_owner() {
        struct Owner {
            bytes: Vec<u8>,
            dropped: Arc<AtomicBool>,
        }
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }
        impl Drop for Owner {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::Release);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let original = Bytes::from_owner(Owner {
            bytes: vec![7; 1024 * 1024],
            dropped: Arc::clone(&dropped),
        });
        let retained = compact_bytes(original.slice(4096..4100));
        drop(original);
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(retained.as_ref(), &[7, 7, 7, 7]);
    }
}
