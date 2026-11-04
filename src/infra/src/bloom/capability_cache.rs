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

//! Query-field facts that outlive the much larger parsed footer cache.
//! Keys include the storage account and immutable, versioned object path.
//! Eviction only loses an optimization: unknown facts require the normal probe.

use std::sync::LazyLock;

use hashbrown::{Equivalent, HashMap};
use parking_lot::Mutex;

use super::reader::BloomFieldCapability;

pub static BLOOM_CAPABILITY_CACHE: LazyLock<BloomCapabilityCache> = LazyLock::new(|| {
    let budget =
        (config::get_config().limit.bloom_footer_cache_max_size / 16).min(16 * 1024 * 1024);
    BloomCapabilityCache::new(budget)
});

type Key = (Box<str>, Box<str>, Box<str>);

#[derive(Hash)]
struct KeyRef<'a>(&'a str, &'a str, &'a str);

impl Equivalent<Key> for KeyRef<'_> {
    fn equivalent(&self, key: &Key) -> bool {
        self.0 == key.0.as_ref() && self.1 == key.1.as_ref() && self.2 == key.2.as_ref()
    }
}

pub struct BloomCapabilityCache {
    inner: Mutex<Inner>,
    max_bytes: usize,
}

struct Inner {
    entries: HashMap<Key, BloomFieldCapability>,
    key_bytes: usize,
}

impl BloomCapabilityCache {
    pub fn new(max_bytes: usize) -> Self {
        // Reserve at most half the budget for table storage. Never grow this
        // table during insertion, so table slack is included in the bound.
        let mut capacity = max_bytes / 512;
        let entries = loop {
            let table = HashMap::with_capacity(capacity);
            if table.allocation_size() <= max_bytes / 2 {
                break table;
            }
            capacity /= 2;
        };
        Self {
            inner: Mutex::new(Inner {
                entries,
                key_bytes: 0,
            }),
            max_bytes,
        }
    }

    pub fn get(&self, account: &str, path: &str, field: &str) -> Option<BloomFieldCapability> {
        self.inner
            .lock()
            .entries
            .get(&KeyRef(account, path, field))
            .copied()
    }

    /// The first fact wins for an immutable object. A full cache starts a new
    /// generation instead of keeping unbounded eviction metadata. Large keys
    /// are not retained. Both cases simply restore the normal footer path.
    pub fn put(&self, account: &str, path: &str, field: &str, facts: BloomFieldCapability) {
        let mut inner = self.inner.lock();
        if inner.entries.capacity() == 0
            || inner.entries.contains_key(&KeyRef(account, path, field))
        {
            return;
        }
        let key_bytes = account
            .len()
            .saturating_add(path.len())
            .saturating_add(field.len());
        let available = self.max_bytes - inner.entries.allocation_size();
        if key_bytes > available {
            return;
        }
        if inner.entries.len() == inner.entries.capacity()
            || inner.key_bytes > available - key_bytes
        {
            inner.entries.clear();
            inner.key_bytes = 0;
        }
        inner
            .entries
            .insert((account.into(), path.into(), field.into()), facts);
        inner.key_bytes += key_bytes;
    }

    /// Requested heap bytes, including empty hash-table slots and control
    /// bytes. Boxed strings have no spare capacity. Allocator overhead is not
    /// part of this logical budget, as with the existing footer cache.
    pub fn bytes(&self) -> usize {
        let inner = self.inner.lock();
        inner.entries.allocation_size() + inner.key_bytes
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bloom::{BloomBuilder, BloomFooterCache, BloomReader, BloomWriter};

    fn reader() -> Arc<BloomReader> {
        let mut b = BloomBuilder::new();
        b.begin_with_blocks(1, "trace_id", 8);
        Arc::new(BloomReader::parse(&BloomWriter::serialize(b.finish()).unwrap()).unwrap())
    }

    #[test]
    fn capability_cache_isolates_account_path_and_field() {
        let c = BloomCapabilityCache::new(4096);
        let r = reader();
        let no_info = r.field_capability("service_name", "composite");
        c.put("account-a", "v1.bf", "service_name", no_info);
        assert_eq!(c.get("account-a", "v1.bf", "service_name"), Some(no_info));
        assert!(c.get("account-b", "v1.bf", "service_name").is_none());
        assert!(c.get("account-a", "v2.bf", "service_name").is_none());
        assert!(c.get("account-a", "v1.bf", "trace_id").is_none());
        let before = c.bytes();
        c.put(
            "account-a",
            "v1.bf",
            "service_name",
            r.field_capability("trace_id", "composite"),
        );
        assert_eq!(c.bytes(), before);
        assert_eq!(c.get("account-a", "v1.bf", "service_name"), Some(no_info));
    }

    #[test]
    fn capability_cache_survives_footer_eviction_and_bounds_all_allocations() {
        let r = reader();
        let footers = BloomFooterCache::new("a".len() + r.memory_size());
        let c = BloomCapabilityCache::new(4096);
        let base = c.bytes();
        let facts = r.field_capability("service_name", "composite");
        c.put("account", "a", "service_name", facts);
        assert_eq!(c.bytes(), base + "accountaservice_name".len());
        footers.put("a".into(), Arc::clone(&r));
        footers.put("b".into(), Arc::clone(&r));
        assert!(footers.get("a").is_none());
        assert_eq!(c.get("account", "a", "service_name"), Some(facts));
        for i in 0..1000 {
            c.put("account", &format!("{i}.bf"), "service_name", facts);
            assert!(c.bytes() <= 4096);
        }
        assert!(c.get("account", "a", "service_name").is_none());
        let before = c.bytes();
        c.put("account", &"x".repeat(4096), "service_name", facts);
        assert_eq!(c.bytes(), before);
        let disabled = BloomCapabilityCache::new(0);
        disabled.put("", "a", "service_name", facts);
        assert_eq!(disabled.bytes(), 0);
        assert!(disabled.get("", "a", "service_name").is_none());
    }

    #[test]
    fn capability_cache_concurrent_repeated_puts_remain_bounded() {
        let c = Arc::new(BloomCapabilityCache::new(4096));
        let facts = reader().field_capability("service_name", "composite");
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let c = Arc::clone(&c);
                scope.spawn(move || {
                    for _ in 0..1000 {
                        c.put("account", "a", "service_name", facts);
                        assert_eq!(c.get("account", "a", "service_name"), Some(facts));
                    }
                });
            }
        });
        assert!(c.bytes() <= 4096);
    }
}
