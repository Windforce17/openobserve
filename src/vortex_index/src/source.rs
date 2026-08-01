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
    fmt,
    ops::Range,
    sync::{Arc, OnceLock},
};

use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use vortex::{
    array::buffer::BufferHandle,
    buffer::{Alignment, ByteBuffer},
    error::{VortexResult, vortex_err},
    file::Footer,
    io::{CoalesceConfig, VortexReadAt},
};

use crate::error::{Result, VixError};

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
            data,
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
}

pub trait VixRangeSource: Send + Sync + 'static {
    /// Total object size in bytes.
    fn len(&self) -> u64;

    /// Whether the object is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Fetch exactly the bytes of `range` (end-exclusive, within `0..len()`).
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>>;

    /// A short description of the object (e.g. its storage path), used in
    /// error messages.
    fn describe(&self) -> String {
        "<vix range source>".to_string()
    }
}

/// Block the current thread on one `fetch` and validate the returned length.
///
/// Only used from the synchronous reader entry points, which by contract run
/// on blocking threads (never on an async executor).
pub(crate) fn block_fetch(source: &dyn VixRangeSource, range: Range<u64>) -> Result<Bytes> {
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
    let bytes = futures::executor::block_on(source.fetch(range.clone())).map_err(|e| {
        VixError::Malformed(format!(
            "fetch {}..{} of {}: {e:#}",
            range.start,
            range.end,
            source.describe()
        ))
    })?;
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
/// a Vortex file. The blob's Vortex [`Footer`] is cached after the first
/// open, so subsequent scans skip the footer fetch entirely and read only
/// the data segments they touch.
pub(crate) struct RangedBlob {
    pub source: Arc<dyn VixRangeSource>,
    /// Absolute byte range of the blob inside the source object.
    pub range: Range<u64>,
    footer: OnceLock<Footer>,
}

impl RangedBlob {
    pub fn new(source: Arc<dyn VixRangeSource>, range: Range<u64>) -> Self {
        Self {
            source,
            range,
            footer: OnceLock::new(),
        }
    }

    /// Blob length in bytes.
    pub fn len(&self) -> u64 {
        self.range.end - self.range.start
    }

    /// The cached Vortex footer of the blob, if a scan already parsed it.
    pub fn footer(&self) -> Option<Footer> {
        self.footer.get().cloned()
    }

    /// Cache the parsed Vortex footer (first writer wins).
    pub fn set_footer(&self, footer: Footer) {
        let _ = self.footer.set(footer);
    }

    /// The vortex IO bridge over this window.
    pub fn read_at(&self) -> BlobReadAt {
        BlobReadAt {
            source: Arc::clone(&self.source),
            range: self.range.clone(),
        }
    }
}

impl fmt::Debug for RangedBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangedBlob")
            .field("source", &self.source.describe())
            .field("range", &self.range)
            .field("footer_cached", &self.footer.get().is_some())
            .finish()
    }
}

/// [`VortexReadAt`] over a byte window of a [`VixRangeSource`]: every read
/// adds the window base offset and goes through `fetch`.
#[derive(Clone)]
pub(crate) struct BlobReadAt {
    source: Arc<dyn VixRangeSource>,
    range: Range<u64>,
}

impl VortexReadAt for BlobReadAt {
    fn concurrency(&self) -> usize {
        8
    }

    /// Let vortex merge gapped segment reads into few larger range fetches
    /// (the object-storage profile: ≤1 MiB gaps, ≤16 MiB spans).
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        Some(CoalesceConfig::object_storage())
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
        let start = window.start + offset;
        let end = start + length as u64;
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
        let fut = self.source.fetch(start..end);
        async move {
            let bytes = fut
                .await
                .map_err(|e| vortex_err!("fetch {start}..{end} of {describe}: {e:#}"))?;
            if bytes.len() != length {
                return Err(vortex_err!(
                    "fetch {start}..{end} of {describe} returned {} bytes, expected {length}",
                    bytes.len()
                ));
            }
            Ok(BufferHandle::new_host(
                ByteBuffer::from(bytes).aligned(alignment),
            ))
        }
        .boxed()
    }
}

// The reader caches `Footer`s and is shared across threads: everything a
// `RangedBlob` holds must stay Send + Sync.
fn _assert_send_sync() {
    fn assert<T: Send + Sync>() {}
    assert::<Footer>();
    assert::<RangedBlob>();
    assert::<BlobReadAt>();
}
