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

//! Cross-query cache of PARSED `.bf` footers.
//!
//! The pruner issues a `GetRange::Suffix(N)` to grab the footer (plus a
//! little trailing body) from object storage and parses it into a
//! [`BloomReader`]. A `.bf` is **write-once**: its path embeds a microsecond
//! `bloom_ver` and is never overwritten (a rebuild produces a new path; a
//! superseded `.bf` is deleted, not mutated), so a parsed footer stays valid
//! for the lifetime of the object.
//!
//! The cache holds the parsed reader, not the suffix bytes (DESIGN-COLD-MISS
//! §4 B1): a v2 footer is zstd-compressed and decoding + table building per
//! query per group is the cost worth skipping; the entry is accounted by the
//! reader's DECODED footprint ([`BloomReader::memory_size`]). LRU by bytes.
//!
//! Body block-rows a query needs are fetched on demand through the regular
//! `file_data` cache ladder (`memory → disk → remote`) via batched range
//! reads — there is no separate async whole-`.bf` backfill.

use std::sync::{Arc, LazyLock};

use hashlink::LruCache;
use parking_lot::Mutex;

use super::reader::BloomReader;

/// Global cache. Sized via [`config::Config::limit::bloom_footer_cache_max_size`]
/// (env: `ZO_BLOOM_FOOTER_CACHE_MAX_SIZE`).
pub static BLOOM_FOOTER_CACHE: LazyLock<BloomFooterCache> = LazyLock::new(|| {
    let max = config::get_config().limit.bloom_footer_cache_max_size;
    BloomFooterCache::new(max)
});

/// Path-keyed, byte-bounded LRU of parsed `.bf` footers.
pub struct BloomFooterCache {
    inner: Mutex<Inner>,
    max_bytes: usize,
}

struct Inner {
    lru: LruCache<String, (Arc<BloomReader>, usize)>,
    total: usize,
}

impl BloomFooterCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                lru: LruCache::new_unbounded(),
                total: 0,
            }),
            max_bytes,
        }
    }

    /// The parsed footer of `path`, if cached (a hit refreshes recency).
    pub fn get(&self, path: &str) -> Option<Arc<BloomReader>> {
        self.inner
            .lock()
            .lru
            .get(path)
            .map(|(reader, _)| Arc::clone(reader))
    }

    /// Insert; the first value for a `path` wins (a `.bf` is immutable, so a
    /// second parse of the same path is the same footer). An entry larger
    /// than the whole budget is not retained.
    pub fn put(&self, path: String, reader: Arc<BloomReader>) {
        let bytes = path.len() + reader.memory_size();
        if bytes > self.max_bytes {
            return;
        }
        let mut inner = self.inner.lock();
        if inner.lru.contains_key(&path) {
            return;
        }
        inner.lru.insert(path, (reader, bytes));
        inner.total += bytes;
        while inner.total > self.max_bytes {
            match inner.lru.remove_lru() {
                Some((_, (_, evicted))) => inner.total -= evicted,
                None => break,
            }
        }
    }

    /// Entries currently held.
    pub fn len(&self) -> usize {
        self.inner.lock().lru.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Accounted bytes currently held.
    pub fn bytes(&self) -> usize {
        self.inner.lock().total
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::writer::{BloomBuilder, BloomWriter},
        *,
    };

    fn reader(file_id: u64, values: usize) -> Arc<BloomReader> {
        let mut b = BloomBuilder::new();
        let i = b.begin_with_blocks(file_id, "trace_id", 8);
        for v in 0..values {
            b.insert(i, format!("v{v}").as_bytes());
        }
        let blob = BloomWriter::serialize(b.finish()).unwrap();
        Arc::new(BloomReader::parse(&blob).unwrap())
    }

    #[test]
    fn test_put_get_round_trip() {
        let c = BloomFooterCache::new(1024 * 1024);
        let path = "files/o/bloom/s_logs/2026/05/08/14/123456.bf".to_string();
        let parsed = reader(7, 3);
        c.put(path.clone(), Arc::clone(&parsed));
        let hit = c.get(&path).expect("hit");
        assert!(Arc::ptr_eq(&hit, &parsed));
        assert_eq!(c.len(), 1);
        assert_eq!(c.bytes(), path.len() + parsed.memory_size());
    }

    #[test]
    fn test_missing_returns_none() {
        let c = BloomFooterCache::new(1024 * 1024);
        assert!(c.get("nope").is_none());
        assert!(c.is_empty());
    }

    #[test]
    fn test_duplicate_put_keeps_the_first() {
        let c = BloomFooterCache::new(1024 * 1024);
        let first = reader(1, 1);
        c.put("k".to_string(), Arc::clone(&first));
        c.put("k".to_string(), reader(2, 1));
        assert!(Arc::ptr_eq(&c.get("k").unwrap(), &first));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn test_evicts_least_recently_used_under_pressure() {
        let one = reader(1, 1);
        let unit = "a".len() + one.memory_size();
        // room for exactly two entries
        let c = BloomFooterCache::new(unit * 2 + unit / 2);
        c.put("a".to_string(), one);
        c.put("b".to_string(), reader(2, 1));
        // touch `a` so `b` is the least recently used
        assert!(c.get("a").is_some());
        c.put("c".to_string(), reader(3, 1));
        assert!(c.get("a").is_some());
        assert!(c.get("b").is_none(), "LRU evicted");
        assert!(c.get("c").is_some());
        assert!(c.bytes() <= unit * 2 + unit / 2);
    }

    #[test]
    fn test_oversize_entry_is_not_retained() {
        let c = BloomFooterCache::new(16);
        c.put("a".to_string(), reader(1, 1));
        assert!(c.get("a").is_none());
        assert_eq!(c.bytes(), 0);
    }
}
