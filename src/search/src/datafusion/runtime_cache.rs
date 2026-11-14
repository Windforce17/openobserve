// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{
    sync::{Arc, LazyLock, Weak},
    time::Duration,
};

use config::metrics;
use datafusion::{
    common::{HashMap, TableReference},
    execution::cache::{
        Cache, CacheEntryInfo, CacheKey, CacheValue,
        cache_manager::{CacheManagerConfig, FileMetadataCache, ListFilesCache},
        default_cache::DefaultCache,
    },
};
use parking_lot::Mutex;

const METADATA_LIMIT: usize = 4 * 1024 * 1024;
const LIST_LIMIT: usize = 64 * 1024;

#[derive(Default)]
struct Registry {
    metadata: Vec<Weak<FileMetadataCache>>,
    lists: Vec<Weak<ListFilesCache>>,
}

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(Default::default);

pub(super) fn configure(config: CacheManagerConfig, enabled: bool) -> CacheManagerConfig {
    if !enabled {
        return config;
    }
    let metadata: Arc<FileMetadataCache> = Arc::new(ObservedCache {
        inner: DefaultCache::new(METADATA_LIMIT).with_name("SmallRuntimeFileMetadataCache"),
        kind: "metadata",
    });
    let lists: Arc<ListFilesCache> = Arc::new(ObservedCache {
        inner: DefaultCache::new_with_ttl(LIST_LIMIT, config.list_files_cache_ttl)
            .with_name("SmallRuntimeListFilesCache"),
        kind: "list",
    });
    {
        let mut registry = REGISTRY.lock();
        registry.metadata.retain(|cache| cache.strong_count() > 0);
        registry.lists.retain(|cache| cache.strong_count() > 0);
        registry.metadata.push(Arc::downgrade(&metadata));
        registry.lists.push(Arc::downgrade(&lists));
    }
    config
        .with_file_metadata_cache(Some(metadata))
        .with_metadata_cache_limit(METADATA_LIMIT)
        .with_list_files_cache(Some(lists))
        .with_list_files_cache_limit(LIST_LIMIT)
}

fn entry_bytes<K: CacheKey, V: CacheValue>(cache: &dyn Cache<K, V>) -> anyhow::Result<usize> {
    cache
        .list_entries()
        .into_iter()
        .try_fold(0usize, |total, (key, info)| {
            total
                .checked_add(key.size())
                .and_then(|size| size.checked_add(info.size_bytes))
                .ok_or_else(|| anyhow::anyhow!("RuntimeEnv cache metric byte count overflow"))
        })
}

fn live_bytes<K: CacheKey, V: CacheValue>(
    caches: Vec<Weak<dyn Cache<K, V>>>,
) -> anyhow::Result<usize> {
    caches.into_iter().try_fold(0usize, |total, weak| {
        let size = match weak.upgrade() {
            Some(cache) => entry_bytes(cache.as_ref())?,
            None => 0,
        };
        total
            .checked_add(size)
            .ok_or_else(|| anyhow::anyhow!("RuntimeEnv cache metric total overflow"))
    })
}

/// Samples accounted entry bytes without retaining caches beyond collection.
/// Only opt-in caches are registered; the default path is not instrumented.
pub fn update_metrics() -> anyhow::Result<()> {
    let (metadata, lists) = {
        let mut registry = REGISTRY.lock();
        registry.metadata.retain(|cache| cache.strong_count() > 0);
        registry.lists.retain(|cache| cache.strong_count() > 0);
        (registry.metadata.clone(), registry.lists.clone())
    };
    for (kind, bytes) in [
        ("metadata", live_bytes(metadata)?),
        ("list", live_bytes(lists)?),
    ] {
        metrics::QUERY_RUNTIME_CACHE_USED_BYTES
            .with_label_values(&[kind])
            .set(i64::try_from(bytes)?);
    }
    Ok(())
}

struct ObservedCache<K: CacheKey, V: CacheValue> {
    inner: DefaultCache<K, V>,
    kind: &'static str,
}

impl<K: CacheKey, V: CacheValue> Cache<K, V> for ObservedCache<K, V> {
    fn get(&self, key: &K) -> Option<V> {
        let value = self.inner.get(key);
        let result = if value.is_some() { "hit" } else { "miss" };
        metrics::QUERY_RUNTIME_CACHE_REQUESTS_TOTAL
            .with_label_values(&[self.kind, result])
            .inc();
        value
    }

    fn put(&self, key: &K, value: V) -> Option<V> {
        self.inner.put(key, value)
    }

    fn remove(&self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }

    fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn clear(&self) {
        self.inner.clear();
    }

    fn name(&self) -> String {
        self.inner.name()
    }

    fn cache_limit(&self) -> usize {
        self.inner.cache_limit()
    }

    fn update_cache_limit(&self, limit: usize) {
        self.inner.update_cache_limit(limit);
    }

    fn cache_ttl(&self) -> Option<Duration> {
        self.inner.cache_ttl()
    }

    fn update_cache_ttl(&self, ttl: Option<Duration>) {
        self.inner.update_cache_ttl(ttl);
    }

    fn drop_table_entries(&self, table: &TableReference) -> datafusion::common::Result<()> {
        self.inner.drop_table_entries(table)
    }

    fn list_entries(&self) -> HashMap<K, CacheEntryInfo<V>> {
        self.inner.list_entries()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::cache::cache_manager::{
        CacheManager, CachedFileList, DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT,
        DEFAULT_METADATA_CACHE_LIMIT,
    };
    use object_store::{ObjectMeta, path::Path};

    use super::*;

    struct TestMetadata(usize);

    impl datafusion::execution::cache::cache_manager::FileMetadata for TestMetadata {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn memory_size(&self) -> usize {
            self.0
        }

        fn extra_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
    }

    #[test]
    fn small_runtime_metadata_evicts_and_rejects_oversized_entries() {
        use datafusion::execution::cache::cache_manager::CachedFileMetadataEntry;

        let manager =
            CacheManager::try_new(&configure(CacheManagerConfig::default(), true)).unwrap();
        let metadata = manager.get_file_metadata_cache();
        for i in 0..4 {
            let path = Path::from(format!("trace/schema=abc/file_{i}"));
            let meta = ObjectMeta {
                location: path.clone(),
                last_modified: chrono::DateTime::UNIX_EPOCH,
                size: 42,
                e_tag: None,
                version: None,
            };
            metadata.put(
                &path,
                CachedFileMetadataEntry::new(meta, Arc::new(TestMetadata(METADATA_LIMIT / 2))),
            );
            assert_eq!(metadata.len(), 1);
            assert!(metadata.get(&path).is_some());
            assert!(entry_bytes(metadata.as_ref()).unwrap() <= METADATA_LIMIT);
        }
        let path = Path::from("trace/schema=abc/file_3");
        let old = metadata.get(&path).unwrap();
        metadata.put(
            &path,
            CachedFileMetadataEntry::new(old.meta, Arc::new(TestMetadata(METADATA_LIMIT))),
        );
        assert!(metadata.is_empty());
        assert!(metadata.get(&path).is_none());
        assert_eq!(entry_bytes(metadata.as_ref()).unwrap(), 0);
    }

    #[test]
    fn disabled_runtime_caches_preserve_datafusion_defaults() {
        let config = configure(CacheManagerConfig::default(), false);
        assert!(config.file_metadata_cache.is_none());
        assert!(config.list_files_cache.is_none());
        assert_eq!(config.metadata_cache_limit, DEFAULT_METADATA_CACHE_LIMIT);
        assert_eq!(
            config.list_files_cache_limit,
            DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT
        );
        assert_eq!(config.list_files_cache_ttl, None);
        let manager = CacheManager::try_new(&config).unwrap();
        assert_eq!(
            manager.get_file_metadata_cache().cache_limit(),
            50 * 1024 * 1024
        );
        assert_eq!(
            manager.get_list_files_cache().unwrap().cache_limit(),
            1024 * 1024
        );
    }

    #[test]
    fn small_runtime_caches_are_bounded_and_not_shared() {
        let first = CacheManager::try_new(&configure(CacheManagerConfig::default(), true)).unwrap();
        let second =
            CacheManager::try_new(&configure(CacheManagerConfig::default(), true)).unwrap();
        let metadata = first.get_file_metadata_cache();
        assert_eq!(metadata.cache_limit(), METADATA_LIMIT);
        assert!(!Arc::ptr_eq(&metadata, &second.get_file_metadata_cache()));
        let lists = first.get_list_files_cache().unwrap();
        let other_lists = second.get_list_files_cache().unwrap();
        assert_eq!(lists.cache_limit(), LIST_LIMIT);
        assert!(!Arc::ptr_eq(&lists, &other_lists));
        let path = datafusion::execution::cache::TableScopedPath {
            table: None,
            path: Path::from("query/schema=abc/format=parquet"),
        };
        let file = ObjectMeta {
            location: Path::from("query/schema=abc/format=parquet/file"),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: 42,
            e_tag: None,
            version: None,
        };
        let value = CachedFileList::new(vec![file.clone()]);
        let expected_bytes = path.size().checked_add(value.size()).unwrap();
        lists.put(&path, value);
        assert_eq!(entry_bytes(lists.as_ref()).unwrap(), expected_bytes);
        assert!(lists.get(&path).is_some());
        assert!(other_lists.get(&path).is_none());
        let weak = Arc::downgrade(&lists);
        assert_eq!(live_bytes(vec![weak.clone()]).unwrap(), expected_bytes);

        for i in 0..1000 {
            let key = datafusion::execution::cache::TableScopedPath {
                table: None,
                path: Path::from(format!("query/path/{i}")),
            };
            lists.put(&key, CachedFileList::new(vec![file.clone()]));
        }
        assert!(entry_bytes(lists.as_ref()).unwrap() <= LIST_LIMIT);
        assert!(lists.len() < 1000);
        assert!(other_lists.is_empty());
        drop(lists);
        drop(first);
        assert!(weak.upgrade().is_none());
        assert_eq!(live_bytes(vec![weak]).unwrap(), 0);
    }

    #[test]
    fn runtime_cache_metrics_track_hits_and_live_usage() {
        const CHILD: &str = "OO_RUNTIME_CACHE_METRICS_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "datafusion::runtime_cache::tests::runtime_cache_metrics_track_hits_and_live_usage",
                    "--exact",
                    "--test-threads=1",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        let manager =
            CacheManager::try_new(&configure(CacheManagerConfig::default(), true)).unwrap();
        let lists = manager.get_list_files_cache().unwrap();
        let key = datafusion::execution::cache::TableScopedPath {
            table: None,
            path: Path::from("metrics/query"),
        };
        let hit = metrics::QUERY_RUNTIME_CACHE_REQUESTS_TOTAL.with_label_values(&["list", "hit"]);
        let miss = metrics::QUERY_RUNTIME_CACHE_REQUESTS_TOTAL.with_label_values(&["list", "miss"]);
        assert_eq!(hit.get(), 0);
        assert_eq!(miss.get(), 0);
        assert!(lists.get(&key).is_none());
        lists.put(
            &key,
            CachedFileList::new(vec![ObjectMeta {
                location: Path::from("metrics/query/file"),
                last_modified: chrono::DateTime::UNIX_EPOCH,
                size: 42,
                e_tag: None,
                version: None,
            }]),
        );
        assert!(lists.get(&key).is_some());
        assert_eq!(hit.get(), 1);
        assert_eq!(miss.get(), 1);
        update_metrics().unwrap();
        let used = metrics::QUERY_RUNTIME_CACHE_USED_BYTES.with_label_values(&["list"]);
        assert_eq!(
            used.get(),
            i64::try_from(entry_bytes(lists.as_ref()).unwrap()).unwrap()
        );
        assert!(used.get() > 0);
        drop(lists);
        drop(manager);
        update_metrics().unwrap();
        assert_eq!(used.get(), 0);
        assert!(REGISTRY.lock().metadata.is_empty());
        assert!(REGISTRY.lock().lists.is_empty());
    }

    #[tokio::test]
    async fn runtime_cache_population_parquet_and_vix() {
        use arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use datafusion::{
            datasource::{
                file_format::{FileFormat, parquet::ParquetFormat},
                listing::{ListingOptions, ListingTableConfig, ListingTableUrl},
                object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
            },
            execution::runtime_env::RuntimeEnvBuilder,
            prelude::{SessionConfig, SessionContext},
        };
        use object_store::{ObjectStoreExt, memory::InMemory};
        use parquet::arrow::ArrowWriter;
        use vortex_index::{VixWriter, VixWriterOptions};

        use crate::datafusion::{
            table_provider::listing_adapter::ListingTableAdapter, vix_format::VixCoreFormat,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 2, 1])),
                Arc::new(Int64Array::from(vec![30, 20, 10])),
            ],
        )
        .unwrap();
        let mut parquet = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut parquet, schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        writer
            .push_batch_with_source(
                &batch,
                &StringArray::from(vec![
                    r#"{"_timestamp":3,"value":30}"#,
                    r#"{"_timestamp":2,"value":20}"#,
                    r#"{"_timestamp":1,"value":10}"#,
                ]),
                None,
            )
            .unwrap();
        let vix = writer.finish().unwrap().0;

        for small in [false, true] {
            for is_vix in [false, true] {
                let extension = if is_vix { "vix" } else { "parquet" };
                let prefix = format!("query-{small}/schema=abc/format={extension}/");
                let store = Arc::new(InMemory::new());
                let bytes = if is_vix { vix.clone() } else { parquet.clone() };
                store
                    .put(
                        &Path::from(format!("{prefix}file.{extension}")),
                        bytes.into(),
                    )
                    .await
                    .unwrap();
                let registry = DefaultObjectStoreRegistry::new();
                registry.register_store(&url::Url::parse("test:///").unwrap(), store);
                let runtime = Arc::new(
                    RuntimeEnvBuilder::new()
                        .with_object_store_registry(Arc::new(registry))
                        .with_cache_manager(configure(CacheManagerConfig::default(), small))
                        .build()
                        .unwrap(),
                );
                let metadata = runtime.cache_manager.get_file_metadata_cache();
                let lists = runtime.cache_manager.get_list_files_cache().unwrap();
                assert!(metadata.is_empty());
                assert!(lists.is_empty());
                let ctx = SessionContext::new_with_config_rt(
                    SessionConfig::new().with_collect_statistics(true),
                    runtime,
                );
                let format: Arc<dyn FileFormat> = if is_vix {
                    Arc::new(VixCoreFormat::new(None))
                } else {
                    Arc::new(ParquetFormat::default())
                };
                let table = ListingTableAdapter::try_new(
                    ListingTableConfig::new(
                        ListingTableUrl::parse(format!("test:///{prefix}")).unwrap(),
                    )
                    .with_listing_options(ListingOptions::new(format))
                    .with_schema(schema.clone()),
                    format!("query-{small}"),
                    None,
                    vec![],
                    None,
                    None,
                )
                .unwrap();
                ctx.register_table("t", Arc::new(table)).unwrap();
                for _ in 0..2 {
                    let batches = ctx
                        .sql("SELECT value FROM t ORDER BY value")
                        .await
                        .unwrap()
                        .collect()
                        .await
                        .unwrap();
                    let values: Vec<_> = batches
                        .iter()
                        .flat_map(|batch| {
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values()
                                .iter()
                                .copied()
                        })
                        .collect();
                    assert_eq!(values, [10, 20, 30]);
                }
                assert_eq!(lists.len(), 1);
                assert!(entry_bytes(lists.as_ref()).unwrap() > 0);
                assert!(
                    lists
                        .list_entries()
                        .keys()
                        .all(|key| key.path.as_ref().contains("query-"))
                );
                if is_vix {
                    assert!(
                        metadata.is_empty(),
                        "VIX must not populate parquet metadata"
                    );
                } else {
                    assert_eq!(metadata.len(), 1);
                    assert!(entry_bytes(metadata.as_ref()).unwrap() > 0);
                    assert!(metadata.list_entries().values().any(|entry| entry.hits > 0));
                }
                println!(
                    "small={small} format={extension} list_entries={} list_bytes={} metadata_entries={} metadata_bytes={}",
                    lists.len(),
                    entry_bytes(lists.as_ref()).unwrap(),
                    metadata.len(),
                    entry_bytes(metadata.as_ref()).unwrap(),
                );
            }
        }
    }
}
