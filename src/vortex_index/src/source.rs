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
    io::{CoalesceConfig, VortexReadAt},
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
}

thread_local! {
    static READ_OPERATION: RefCell<Option<Arc<dyn VixReadOperation>>> = const { RefCell::new(None) };
    static EXACT_RANGES: Cell<bool> = const { Cell::new(false) };
    static READER_MEMORY: RefCell<Option<Arc<crate::reader::ReaderMemory>>> = const { RefCell::new(None) };
}

/// Run synchronous work with operation-local cancellation, restoring the
/// previous scope on normal return, nested calls, and unwinding.
pub fn with_read_operation<T>(operation: Arc<dyn VixReadOperation>, work: impl FnOnce() -> T) -> T {
    struct Restore(Option<Arc<dyn VixReadOperation>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            READ_OPERATION.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(READ_OPERATION.with(|slot| slot.replace(Some(operation))));
    work()
}

/// Count metadata is physically interleaved with large postings segments.
/// Coalesce adjacent count reads, but never pay for an unrequested gap.
/// The flag is captured by the ephemeral IO bridge, not cached footers.
pub(crate) fn with_exact_range_reads<T>(work: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            EXACT_RANGES.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(EXACT_RANGES.with(|slot| slot.replace(true)));
    work()
}

pub(crate) fn current_read_operation() -> Option<Arc<dyn VixReadOperation>> {
    READ_OPERATION.with(|slot| slot.borrow().clone())
}

/// Check the current synchronous operation without modifying shared state.
pub fn check_read_cancelled() -> std::result::Result<(), VixError> {
    if READ_OPERATION.with(|slot| slot.borrow().as_ref().is_some_and(|op| op.is_cancelled())) {
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

pub(crate) fn current_reader_memory() -> Arc<crate::reader::ReaderMemory> {
    READER_MEMORY
        .with(|slot| slot.borrow().clone())
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

    /// A short description of the object (e.g. its storage path), used in
    /// error messages.
    fn describe(&self) -> String {
        "<vix range source>".to_string()
    }
}

/// Block the current thread on one `fetch_many` and validate every returned
/// length (see [`block_fetch`]; same blocking-thread contract).
pub(crate) fn block_fetch_many(
    source: &dyn VixRangeSource,
    ranges: Vec<Range<u64>>,
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
    let all = futures::executor::block_on(source.fetch_many(ranges)).map_err(fetch_error)?;
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

/// Fetch planned disjoint metadata windows concurrently without offering
/// their gaps to a downstream `fetch_many` coalescer.
pub(crate) fn block_fetch_separate(
    source: &dyn VixRangeSource,
    ranges: Vec<Range<u64>>,
) -> Result<Vec<Bytes>> {
    use futures::{StreamExt, TryStreamExt};
    check_read_cancelled()?;
    for range in &ranges {
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
    let bound = source.for_current_operation();
    let source = bound.as_deref().unwrap_or(source);
    let reads = futures::stream::iter(ranges.into_iter().map(|range| async move {
        check_read_cancelled()?;
        let expected = (range.end - range.start) as usize;
        let bytes = source.fetch(range).await.map_err(fetch_error)?;
        check_read_cancelled()?;
        if bytes.len() != expected {
            return Err(VixError::Malformed(format!(
                "fetch of {} returned {} bytes, expected {expected}",
                source.describe(),
                bytes.len(),
            )));
        }
        Ok(bytes)
    }))
    .buffered(4)
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
            exact_ranges: EXACT_RANGES.with(Cell::get),
            opening_memory: None,
        }
    }

    pub(crate) fn opening_read_at(&self) -> (BlobReadAt, Arc<OpeningMemory>) {
        let opening = Arc::new(OpeningMemory {
            memory: self.reader_memory(),
            footer: Arc::clone(&self.footer),
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
            let retained = FooterState::retained_bytes(&ranges);
            if self.footer.ranges.set(ranges).is_ok()
                && let Some(memory) = self.footer.memory.get()
            {
                memory.add(retained);
                memory.notify();
            }
        }
        state.pending.take()
    }
}
/// [`VortexReadAt`] over a byte window of a [`VixRangeSource`]: every read
/// adds the window base offset and goes through `fetch`.
#[derive(Clone)]
pub(crate) struct BlobReadAt {
    source: Arc<dyn VixRangeSource>,
    range: Range<u64>,
    operation: Option<Arc<dyn VixReadOperation>>,
    footer: Arc<FooterState>,
    exact_ranges: bool,
    opening_memory: Option<Arc<OpeningMemory>>,
}

impl VortexReadAt for BlobReadAt {
    fn concurrency(&self) -> usize {
        8
    }

    /// Count-only scans merge adjacent segments without fetching intervening
    /// postings; ordinary scans retain the object-storage coalescing policy.
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        let mut config = CoalesceConfig::object_storage();
        if self.exact_ranges {
            config.distance = 0;
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
        async move {
            if operation.as_ref().is_some_and(|op| op.is_cancelled()) {
                return Err(vortex_err!(External: VixError::Cancelled));
            }
            let bytes =
                fetch_footer_range(source.as_ref(), &footer, start..end, operation.as_deref())
                    .await?;
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
