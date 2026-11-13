//! Default-off compact reader representations. Settings are frozen at startup.
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{Arc, LazyLock, OnceLock, Weak},
};

use arrow_schema::{Field, Schema};
use parking_lot::Mutex;

#[path = "../../config/src/reader_compact.rs"]
mod config;
use config::ReaderCompactConfig;

static SETTINGS: OnceLock<ReaderCompactConfig> = OnceLock::new();

/// Validate and freeze environment settings before starting any service work.
/// Standalone engine users also initialize on their first reader open.
pub fn initialize_reader_compact() -> std::io::Result<()> {
    if SETTINGS.get().is_none() {
        let settings = ReaderCompactConfig::from_env()?;
        let _ = SETTINGS.set(settings);
    }
    Ok(())
}

pub(crate) fn settings() -> ReaderCompactConfig {
    // Direct low-level blob users retain the original representation unless
    // explicitly initialized. No environment parsing or fallback happens here.
    SETTINGS.get().copied().unwrap_or_default()
}

const SHARDS: usize = 16;
const PER_SHARD: usize = 4096;
#[derive(Default)]
struct Shard {
    entries: HashMap<u64, Vec<Weak<Field>>>,
    count: usize,
}
static FIELDS: LazyLock<[Mutex<Shard>; SHARDS]> =
    LazyLock::new(|| std::array::from_fn(|_| Mutex::new(Shard::default())));

// Arrow equality/hash omit IPC dictionary properties. Full serialization also
// preserves nested fields, nullability, metadata, and dictionary attributes.
fn key(field: &Field) -> Vec<u8> {
    serde_json::to_vec(field).expect("Field serialization is infallible")
}
fn intern(field: Arc<Field>) -> Arc<Field> {
    let bytes = key(&field);
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    let hash = h.finish();
    // Deduplication is optional: never wait while holding a potentially large
    // schema, and never acquire a second interner lock while holding this one.
    let Some(mut shard) = FIELDS[hash as usize % SHARDS].try_lock() else {
        return field;
    };
    if let Some(bucket) = shard.entries.get(&hash) {
        for weak in bucket {
            if let Some(existing) = weak.upgrade()
                && key(&existing) == bytes
            {
                return existing;
            }
        }
    }
    if shard.count >= PER_SHARD {
        shard.entries.retain(|_, bucket| {
            bucket.retain(|w| w.strong_count() > 0);
            !bucket.is_empty()
        });
        // At most PER_SHARD entries exist, so this sum cannot overflow.
        shard.count = shard.entries.values().map(Vec::len).sum();
    }
    if shard.count < PER_SHARD {
        shard
            .entries
            .entry(hash)
            .or_default()
            .push(Arc::downgrade(&field));
        shard.count += 1;
    }
    field
}
pub(crate) fn schema(schema: Schema) -> Schema {
    if !settings().intern_fields {
        return schema;
    }
    let fields = schema
        .fields()
        .iter()
        .cloned()
        .map(intern)
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Metadata};

    use super::*;
    #[test]
    fn field_intern_preserves_full_semantics() {
        let a = Arc::new(Field::new("compact_test", DataType::Utf8, true));
        let b = Arc::new(a.as_ref().clone());
        let x = intern(a.clone());
        let y = intern(b);
        assert!(Arc::ptr_eq(&x, &y));
        for other in [
            Field::new("compact_test2", DataType::Utf8, true),
            Field::new("compact_test", DataType::Int64, true),
            Field::new("compact_test", DataType::Utf8, false),
            Field::new("compact_test", DataType::Utf8, true)
                .with_metadata(Metadata::from_iter([("k", "v")])),
        ] {
            let z = intern(Arc::new(other));
            assert!(!Arc::ptr_eq(&x, &z));
        }
        #[allow(deprecated)]
        let dict = |id| {
            Arc::new(Field::new_dict(
                "compact_dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
                id,
                false,
            ))
        };
        let x = intern(dict(1));
        let y = intern(dict(2));
        assert_eq!(*x, *y, "Arrow equality intentionally ignores IPC id");
        assert!(!Arc::ptr_eq(&x, &y));
        let n1 = intern(Arc::new(Field::new(
            "nested",
            DataType::Struct(vec![x].into()),
            true,
        )));
        let n2 = intern(Arc::new(Field::new(
            "nested",
            DataType::Struct(vec![y].into()),
            true,
        )));
        assert!(!Arc::ptr_eq(&n1, &n2));
    }
    #[test]
    fn field_intern_is_weak() {
        let field = intern(Arc::new(Field::new("compact_weak", DataType::Utf8, true)));
        let weak = Arc::downgrade(&field);
        drop(field);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn field_intern_does_not_wait_on_a_busy_shard() {
        let field = Arc::new(Field::new("busy", DataType::Utf8, true));
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key(&field).hash(&mut h);
        let _guard = FIELDS[h.finish() as usize % SHARDS].lock();
        assert!(Arc::ptr_eq(&field, &intern(field.clone())));
    }
}
