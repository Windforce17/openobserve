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

//! Memoize deterministic TopN refusals, not transient I/O or admission failures.
//! A hit always requests the complete DataFusion scan, never partial groups.

use std::sync::LazyLock;

use hashlink::LruCache;
use parking_lot::Mutex;

use super::collect::AggregateFallback;

const MAX_KEY_BYTES: usize = 4096;
const MAX_ENTRIES: usize = 65_536;

pub(super) static CACHE: LazyLock<RejectCache> =
    LazyLock::new(|| RejectCache::new(config::get_config().limit.vix_agg_reject_cache_entries));

pub(super) struct RejectCache {
    entries: Mutex<LruCache<Box<str>, &'static str>>,
    capacity: usize,
}

impl RejectCache {
    pub(super) fn new(capacity: usize) -> Self {
        let capacity = capacity.min(MAX_ENTRIES);
        Self {
            entries: Mutex::new(LruCache::new(capacity.max(1))),
            capacity,
        }
    }

    pub(super) fn get(&self, key: &str) -> Option<&'static str> {
        if self.capacity == 0 || key.len() > MAX_KEY_BYTES {
            return None;
        }
        self.entries.lock().get(key).copied()
    }

    pub(super) fn put(&self, key: String, error: &anyhow::Error) {
        if self.capacity == 0
            || key.len() > MAX_KEY_BYTES
            // anyhow contexts retain downcast identity without exposing it as
            // a separate std::error::Error in chain(). Check both interfaces.
            || error.is::<std::io::Error>()
            || error.is::<object_store::Error>()
            || error.is::<super::source::FetchBudgetExceeded>()
            || error.is::<super::source::EvaluationMemoryExhausted>()
            || error.is::<vortex_index::VixError>()
            || error.chain().any(|cause| {
                cause.is::<std::io::Error>()
                    || cause.is::<object_store::Error>()
                    || cause.is::<super::source::FetchBudgetExceeded>()
                    || cause.is::<super::source::EvaluationMemoryExhausted>()
            })
            || super::is_cancelled_read(error)
        {
            return;
        }
        // Whitelist decisions that depend only on immutable file contents,
        // selected rows, configured budgets and the versioned semantics.
        // Malformed data, missing objects, timeouts, memory admission and
        // cancellation must remain observable and retryable.
        // A fallback used only as context must not hide a transient root cause.
        let Some(fallback) = error.root_cause().downcast_ref::<AggregateFallback>() else {
            return;
        };
        if !matches!(
            fallback.0,
            "aggregate dictionary group budget exceeded"
                | "aggregate dictionary byte budget exceeded"
                | "aggregate chunk dictionary budget exceeded"
                | "aggregate chunk group budget exceeded"
                | "aggregate group budget exceeded"
                | "NULL or missing aggregate values require scan"
                | "NULL aggregate group requires scan"
        ) {
            return;
        }
        self.entries.lock().insert(key.into_boxed_str(), fallback.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refusal() -> anyhow::Error {
        AggregateFallback("aggregate dictionary group budget exceeded").into()
    }

    #[test]
    fn rc4_rejection_lru_is_bounded_and_refreshes_hits() {
        let cache = RejectCache::new(2);
        cache.put("a".into(), &refusal());
        cache.put("b".into(), &refusal());
        assert!(cache.get("a").is_some());
        cache.put("c".into(), &refusal());
        assert!(cache.get("b").is_none());
        assert!(cache.get("a").is_some());
        assert!(cache.get("c").is_some());
        assert_eq!(cache.entries.lock().len(), 2);
        cache.put("x".repeat(MAX_KEY_BYTES + 1), &refusal());
        assert_eq!(cache.entries.lock().len(), 2);
        let disabled = RejectCache::new(0);
        disabled.put("a".into(), &refusal());
        assert!(disabled.get("a").is_none());
        assert_eq!(RejectCache::new(usize::MAX).capacity, MAX_ENTRIES);
    }

    #[test]
    fn rc4_transient_and_cancelled_errors_are_never_cached() {
        let cache = RejectCache::new(8);
        for error in [
            anyhow::anyhow!("object-store timeout"),
            vortex_index::VixError::Cancelled.into(),
            AggregateFallback("unknown ranged object size").into(),
            AggregateFallback("aggregate count overflow").into(),
            std::io::Error::from(std::io::ErrorKind::TimedOut).into(),
            std::io::Error::from(std::io::ErrorKind::OutOfMemory).into(),
            anyhow::Error::from(vortex_index::VixError::Cancelled)
                .context(AggregateFallback("aggregate group budget exceeded")),
            anyhow::Error::from(std::io::Error::from(std::io::ErrorKind::OutOfMemory))
                .context(AggregateFallback("aggregate group budget exceeded")),
            refusal().context(std::io::Error::from(std::io::ErrorKind::OutOfMemory)),
        ] {
            cache.put("key".into(), &error);
            assert!(
                cache.get("key").is_none(),
                "cached transient error: {error:#}"
            );
        }
        cache.put("key".into(), &refusal().context("file generation 9"));
        assert!(cache.get("key").is_some());
    }
}
