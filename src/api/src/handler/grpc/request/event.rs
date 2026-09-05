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

use std::ops::Range;

use anyhow::Result;
use config::{cluster::LOCAL_NODE, get_config, meta::stream::FileKey, metrics};
use infra::cache::file_data::{CacheType, TRACE_ID_FOR_CACHE_LATEST_FILE, disk, memory};
use opentelemetry::global;
use proto::cluster_rpc::{
    EmptyResponse, FileContent, FileContentResponse, FileList, SimpleFileList, event_server::Event,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, codegen::tokio_stream};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::handler::grpc::MetadataMap;

pub struct Eventer;

const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4MB chunks

#[tonic::async_trait]
impl Event for Eventer {
    type GetFilesStream = ReceiverStream<Result<FileContentResponse, Status>>;

    async fn send_file_list(
        &self,
        req: Request<FileList>,
    ) -> Result<Response<EmptyResponse>, Status> {
        let start = std::time::Instant::now();
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(req.metadata())));
        let _ = tracing::Span::current().set_parent(parent_cx);

        let req = req.get_ref();
        let grpc_addr = req.node_addr.clone();
        let put_items = req
            .items
            .iter()
            .filter(|v| !v.deleted)
            .map(FileKey::from)
            .collect::<Vec<_>>();
        let cfg = get_config();

        // Broadcast delivery can be out of order, so never upsert a put row
        // into the distributed querier's file-list cache: an older G1 event
        // could regress a newer G2 entry. Invalidate put rows so the next ID
        // lookup reloads authoritative DB metadata. Deleted rows deliberately
        // remain cached through the object-deletion grace: a leader may
        // already have snapshotted their IDs for an in-flight follower
        // request. In local mode LOCAL_CACHE is the SQLite authority itself,
        // so deleting from it would delete the real row.
        if LOCAL_NODE.is_querier() && !cfg.common.local_mode {
            for item in &put_items {
                infra::file_list::LOCAL_CACHE
                    .remove(&item.key)
                    .await
                    .map_err(|e| {
                        Status::internal(format!(
                            "failed to invalidate local file-list cache row {}: {e}",
                            item.key
                        ))
                    })?;
            }
        }
        if LOCAL_NODE.is_querier() {
            evict_stale_sidecar_caches(&put_items).await;
        }

        // cache latest files for querier
        if cfg.cache_latest_files.enabled && LOCAL_NODE.is_querier() {
            let files_to_download =
                collect_files_to_download(cfg.cache_latest_files.cache_parquet, &put_items);

            // Try batch download first
            if get_config().cache_latest_files.download_from_node {
                let mut failed_files = Vec::new();

                // Try batch download files
                if !files_to_download.is_empty() {
                    match crate::service::file_downloader::download_from_node(
                        &grpc_addr,
                        &files_to_download,
                    )
                    .await
                    {
                        Ok(failed) => failed_files = failed,
                        Err(e) => {
                            log::error!("[gRPC:Event] Failed to get files from notifier: {e}");
                            failed_files = files_to_download;
                        }
                    }
                }

                // Fallback to individual downloads for failed files
                for (id, account, file, size, ts) in failed_files {
                    if let Err(e) = crate::service::file_downloader::queue_download(
                        TRACE_ID_FOR_CACHE_LATEST_FILE.to_string(),
                        id,
                        account,
                        file,
                        size,
                        ts,
                        CacheType::Disk,
                    )
                    .await
                    {
                        log::error!("[gRPC:Event] Failed to cache file data: {e}");
                    }
                }
            } else {
                // Direct download when download_from_node_enabled is false
                for (id, account, file, size, ts) in files_to_download {
                    if let Err(e) = crate::service::file_downloader::queue_download(
                        TRACE_ID_FOR_CACHE_LATEST_FILE.to_string(),
                        id,
                        account,
                        file,
                        size,
                        ts,
                        CacheType::Disk,
                    )
                    .await
                    {
                        log::error!("[gRPC:Event] Failed to cache file data: {e}");
                    }
                }
            }

            // delete merge files
            if cfg.cache_latest_files.delete_merge_files && cfg.cache_latest_files.cache_parquet {
                let deleted_items = req
                    .items
                    .iter()
                    .filter(|v| v.deleted)
                    .map(FileKey::from)
                    .collect::<Vec<_>>();
                let del_items = merge_evict_keys(deleted_items.iter());
                infra::cache::file_data::delete::add(del_items);
            }
        }

        // metrics
        let time = start.elapsed().as_secs_f64();
        metrics::GRPC_RESPONSE_TIME
            .with_label_values(&["/event/send_file_list", "200", "", "", "", ""])
            .observe(time);
        metrics::GRPC_INCOMING_REQUESTS
            .with_label_values(&["/event/send_file_list", "200", "", "", "", ""])
            .inc();

        Ok(Response::new(EmptyResponse {}))
    }

    async fn get_files(
        &self,
        request: Request<SimpleFileList>,
    ) -> Result<Response<Self::GetFilesStream>, Status> {
        let file_list = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        // Spawn a task to handle the streaming
        tokio::spawn(async move {
            for path in file_list.files.iter() {
                if let Err(e) = handle_file_chunked(path, tx.clone()).await {
                    log::error!("[gRPC:Event] Failed to handle file {path}: {e}");
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// The download rows — `(id, account, key, size, max_ts)` — one broadcast
/// enqueues on a caching querier: each cacheable data file plus the immutable
/// `.vxi` sidecar named by its generation when `index_size > 0`.
/// `cache_parquet=false` enqueues nothing. Undersized rows
/// (`should_download`) and rows past the disk-cache max age skip whole-object
/// caching; a later query can fill the generation-addressed object on demand.
fn collect_files_to_download(
    cache_parquet: bool,
    put_items: &[FileKey],
) -> Vec<(i64, String, String, i64, i64)> {
    let mut files_to_download = Vec::new();
    if !cache_parquet {
        return files_to_download;
    }
    for item in put_items.iter() {
        if !crate::service::file_downloader::should_download(item.meta.records) {
            continue;
        }
        // files with data older than the cache max age should not be
        // cached, e.g. merged files from compaction of old partitions
        if crate::service::file_downloader::exceeds_cache_max_age(item.meta.max_ts, CacheType::Disk)
        {
            continue;
        }
        // cache the data file, and (v3 split) its `.vxi` index sidecar
        // when one exists
        files_to_download.push((
            item.id,
            item.account.clone(),
            item.key.clone(),
            item.meta.compressed_size,
            item.meta.max_ts,
        ));
        if item.meta.index_size > 0
            && let Some(sidecar) = config::vix_sidecar_key(&item.key, item.meta.index_generation)
        {
            files_to_download.push((
                item.id,
                item.account.clone(),
                sidecar,
                item.meta.index_size,
                item.meta.max_ts,
            ));
        }
    }
    files_to_download
}
/// Cache keys a broadcast's DELETED rows evict: each data key plus the
/// active immutable sidecar named by metadata when `index_size > 0`.
/// Generation zero derives the legacy canonical `.vxi` key; a positive
/// no-sidecar drop state queues only the data object.
fn merge_evict_keys<'a>(deleted_files: impl Iterator<Item = &'a FileKey>) -> Vec<String> {
    deleted_files
        .flat_map(|file| {
            let sidecar = (file.meta.index_size > 0)
                .then(|| config::vix_sidecar_key(&file.key, file.meta.index_generation))
                .flatten();
            std::iter::once(file.key.clone()).chain(sidecar)
        })
        .collect()
}

/// Validate cached bytes for the immutable sidecar named by each put event.
/// A size mismatch under that exact generation is corrupt/stale cache data;
/// other generations are different keys and remain available to in-flight
/// FileKey snapshots. Parsed-reader and result caches are purged by logical
/// data key so the broadcast releases every obsolete generation immediately;
/// readers already held by active queries remain alive through their `Arc`.
async fn evict_stale_sidecar_caches(put_items: &[FileKey]) {
    let mut core_keys: Vec<&str> = Vec::new();
    for item in put_items.iter() {
        if config::FileFormat::from_extension(&item.key) != Some(config::FileFormat::Vix) {
            continue;
        }
        core_keys.push(item.key.as_str());
        crate::service::search::vix::reader_cache::GLOBAL_CACHE.remove(&item.key);
        let Some(sidecar) = config::vix_sidecar_key(&item.key, item.meta.index_generation) else {
            continue;
        };
        let expected = item.meta.index_size;
        let disk_stale = disk::get_size(&sidecar)
            .await
            .is_some_and(|s| s as i64 != expected);
        let memory_stale = memory::get_size(&sidecar)
            .await
            .is_some_and(|s| s as i64 != expected);
        if disk_stale {
            let _ = disk::remove(&sidecar).await;
        }
        if memory_stale {
            let _ = memory::remove(&sidecar).await;
        }
        if disk_stale || memory_stale {
            log::info!(
                "[gRPC:Event] evicted stale sidecar cache of {} (row index_size {expected})",
                item.key
            );
        }
    }
    let purged = crate::service::search::vix::cache::GLOBAL_CACHE.remove_file_entries(core_keys);
    if purged > 0 {
        log::debug!("[gRPC:Event] purged {purged} vix result-cache entries for updated files");
    }
}

async fn handle_file_chunked(
    path: &str,
    tx: tokio::sync::mpsc::Sender<Result<FileContentResponse, Status>>,
) -> Result<(), Status> {
    let start = std::time::Instant::now();
    let filename = path.to_string();
    let mut offset = 0u64;
    let total_size = disk::get_size(path).await.unwrap_or(0) as u64;

    while offset < total_size {
        let chunk_size = std::cmp::min(CHUNK_SIZE as u64, total_size - offset);
        let chunk = match infra::cache::file_data::disk::get(
            path,
            Some(Range {
                start: offset,
                end: offset + chunk_size,
            }),
        )
        .await
        {
            Some(file_data) => file_data,
            None => {
                if let Err(e) = tx.send(Err(Status::not_found(path))).await {
                    log::error!("[gRPC:Event] Failed to send error: {e}");
                }
                return Err(Status::not_found(path));
            }
        };

        let response = FileContentResponse {
            entries: vec![FileContent {
                content: chunk.to_vec(),
                filename: filename.clone(),
            }],
        };

        if let Err(e) = tx.send(Ok(response)).await {
            log::error!("[gRPC:Event] Failed to send file chunk: {e}");
            return Err(Status::internal("Failed to send file chunk"));
        }

        offset += chunk_size;
    }

    log::info!(
        "[gRPC:Event] Send file: {}, total_size: {}, offset: {} took: {} ms",
        path,
        total_size,
        offset,
        start.elapsed().as_millis()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use proto::cluster_rpc::{FileKey, FileList, FileMeta};

    use super::*;

    /// A cached byte object is validated only against the exact immutable
    /// sidecar generation named by the broadcast metadata.
    #[tokio::test]
    async fn evict_stale_sidecar_caches_drops_size_mismatched_bytes() {
        if !get_config().disk_cache.enabled {
            return; // nothing to cover without a byte cache
        }
        let key = "files/e2e/logs/healcache/2026/08/17/00/sidecar_evict_test.vix";
        let generation = 17;
        let sidecar =
            config::vix_sidecar_key(key, generation).expect("core keys have sidecar keys");
        let old = bytes::Bytes::from_static(b"pre-heal sidecar bytes");

        // stale: the row now advertises a different sidecar size
        infra::cache::file_data::disk::set(&sidecar, old.clone())
            .await
            .unwrap();
        assert!(disk::exist(&sidecar).await, "seeded the old sidecar");
        let updated = config::meta::stream::FileKey::new(
            1,
            String::new(),
            key.to_string(),
            config::meta::stream::FileMeta {
                index_size: old.len() as i64 + 7,
                index_generation: generation,
                ..Default::default()
            },
            false,
        );
        evict_stale_sidecar_caches(std::slice::from_ref(&updated)).await;
        assert!(
            !disk::exist(&sidecar).await,
            "a size-mismatched cached sidecar must be evicted"
        );

        // current: a row whose index_size matches the cached bytes keeps them
        infra::cache::file_data::disk::set(&sidecar, old.clone())
            .await
            .unwrap();
        let current = config::meta::stream::FileKey::new(
            1,
            String::new(),
            key.to_string(),
            config::meta::stream::FileMeta {
                index_size: old.len() as i64,
                index_generation: generation,
                ..Default::default()
            },
            false,
        );
        evict_stale_sidecar_caches(std::slice::from_ref(&current)).await;
        assert!(
            disk::exist(&sidecar).await,
            "a size-matching cached sidecar must stay cached"
        );
        let _ = disk::remove(&sidecar).await;
    }

    /// Cache-latest enqueues the immutable sidecar key named by each FileKey
    /// snapshot. Equal-sized generations remain distinct; zero is legacy.
    #[test]
    fn m11_new_file_event_enqueues_data_and_sidecar() {
        let meta =
            |records: i64, index_size: i64, index_generation: i64| config::meta::stream::FileMeta {
                records,
                compressed_size: 4096,
                index_size,
                index_generation,
                ..Default::default()
            };
        let core = config::meta::stream::FileKey::new(
            7,
            "acct".to_string(),
            "files/org/logs/s1/2026/08/18/00/m11_new.vix".to_string(),
            meta(1000, 512, 73),
            false,
        );
        let rows = collect_files_to_download(true, std::slice::from_ref(&core));
        assert_eq!(rows.len(), 2, "data object + index sidecar");
        assert_eq!(rows[0].2, core.key);
        assert_eq!(rows[0].3, 4096, "data row sized by compressed_size");
        assert_eq!(
            rows[1].2,
            config::vix_sidecar_key(&core.key, core.meta.index_generation).unwrap(),
            "sidecar row derives the .vxi key"
        );
        assert_eq!(rows[1].2, "files/org/logs/s1/2026/08/18/00/m11_new.73.vxi");
        assert_eq!(
            rows[1].3, 512,
            "sidecar row sized by index_size (v2: exact object size)"
        );
        assert_eq!(rows[1].0, core.id, "sidecar keeps the data row's file id");

        let mut next = core.clone();
        next.meta.index_generation = 74;
        let next_rows = collect_files_to_download(true, std::slice::from_ref(&next));
        assert_ne!(
            rows[1].2, next_rows[1].2,
            "equal-sized generations must enqueue different immutable sidecars"
        );
        assert_eq!(
            next_rows[1].2,
            "files/org/logs/s1/2026/08/18/00/m11_new.74.vxi"
        );

        // A positive generation with index_size == 0 is an index-drop state:
        // cache only the data row because there is no active sidecar.
        let no_sidecar = config::meta::stream::FileKey::new(
            8,
            "acct".to_string(),
            "files/org/logs/s1/2026/08/18/00/m11_plain.vix".to_string(),
            meta(1000, 0, 75),
            false,
        );
        let rows = collect_files_to_download(true, std::slice::from_ref(&no_sidecar));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].2, no_sidecar.key);

        // legacy parquet rows never derive a sidecar even with index_size set
        let legacy = config::meta::stream::FileKey::new(
            9,
            "acct".to_string(),
            "files/org/logs/s1/2026/08/18/00/m11_legacy.parquet".to_string(),
            meta(1000, 512, 76),
            false,
        );
        let rows = collect_files_to_download(true, std::slice::from_ref(&legacy));
        assert_eq!(rows.len(), 1, "non-.vix keys have no derivable sidecar");

        // undersized rows skip whole (data AND sidecar)
        let tiny = config::meta::stream::FileKey::new(
            10,
            "acct".to_string(),
            "files/org/logs/s1/2026/08/18/00/m11_tiny.vix".to_string(),
            meta(5, 512, 77),
            false,
        );
        assert!(
            collect_files_to_download(true, std::slice::from_ref(&tiny)).is_empty(),
            "rows under file_download_min_records enqueue nothing"
        );
    }

    /// Deleted events evict the active immutable sidecar named by their
    /// metadata. Legacy generation zero keeps the canonical `.vxi` key, and
    /// a positive index-drop generation has no sidecar to evict.
    #[test]
    fn m11_merge_event_evicts_inputs_with_sidecars() {
        let deleted = [
            config::meta::stream::FileKey::new(
                1,
                String::new(),
                "files/org/a/1.vix".to_string(),
                config::meta::stream::FileMeta {
                    index_size: 100,
                    index_generation: 77,
                    ..Default::default()
                },
                true,
            ),
            config::meta::stream::FileKey::new(
                2,
                String::new(),
                "files/org/a/2.vix".to_string(),
                config::meta::stream::FileMeta {
                    index_size: 100,
                    ..Default::default()
                },
                true,
            ),
            config::meta::stream::FileKey::new(
                3,
                String::new(),
                "files/org/a/3.vix".to_string(),
                config::meta::stream::FileMeta {
                    index_generation: 88,
                    ..Default::default()
                },
                true,
            ),
            config::meta::stream::FileKey::new(
                4,
                String::new(),
                "files/org/a/4.parquet".to_string(),
                config::meta::stream::FileMeta {
                    index_size: 100,
                    index_generation: 99,
                    ..Default::default()
                },
                true,
            ),
        ];
        assert_eq!(
            merge_evict_keys(deleted.iter()),
            vec![
                "files/org/a/1.vix".to_string(),
                "files/org/a/1.77.vxi".to_string(),
                "files/org/a/2.vix".to_string(),
                "files/org/a/2.vxi".to_string(),
                "files/org/a/3.vix".to_string(),
                "files/org/a/4.parquet".to_string(),
            ]
        );
        assert!(
            merge_evict_keys(std::iter::empty::<&config::meta::stream::FileKey>()).is_empty(),
            "no deleted rows, nothing evicted"
        );
    }

    /// A heal publishes a new immutable generation. Broadcast handling may
    /// evict corrupt bytes under the new key, but must leave the old key and
    /// data bytes intact for queries holding the old FileKey snapshot.
    #[tokio::test]
    async fn m11_sidecar_only_heal_refreshes_sidecar_keeps_old_generation_and_data() {
        if !get_config().disk_cache.enabled {
            return;
        }
        let key = "files/e2e/logs/healcache/2026/08/18/00/m11_heal_keepdata.vix";
        let old_generation = 41;
        let new_generation = 42;
        let old_key = config::vix_sidecar_key(key, old_generation).unwrap();
        let new_key = config::vix_sidecar_key(key, new_generation).unwrap();
        let data = bytes::Bytes::from_static(b"data bytes the heal must not touch");
        let old_sidecar = bytes::Bytes::from_static(b"pre-heal sidecar bytes");
        infra::cache::file_data::disk::set(key, data.clone())
            .await
            .unwrap();
        infra::cache::file_data::disk::set(&old_key, old_sidecar.clone())
            .await
            .unwrap();
        // Simulate corrupt/partial cache data under the newly published key.
        infra::cache::file_data::disk::set(&new_key, old_sidecar.clone())
            .await
            .unwrap();

        let result_cache = &crate::service::search::vix::cache::GLOBAL_CACHE;
        let pre_heal_result_key = format!(
            "{key}|{old_generation}|{}|deadbeef_n_full",
            old_sidecar.len()
        );
        result_cache.put(
            pre_heal_result_key.clone(),
            crate::service::search::vix::cache::CacheEntry::NoMatch,
        );

        let healed = config::meta::stream::FileKey::new(
            1,
            String::new(),
            key.to_string(),
            config::meta::stream::FileMeta {
                records: 1000,
                compressed_size: data.len() as i64,
                index_size: old_sidecar.len() as i64 + 9,
                index_generation: new_generation,
                ..Default::default()
            },
            false,
        );

        evict_stale_sidecar_caches(std::slice::from_ref(&healed)).await;
        assert!(disk::exist(key).await, "data bytes remain cached");
        assert!(
            disk::exist(&old_key).await,
            "an in-flight old snapshot must retain its immutable sidecar"
        );
        assert!(
            !disk::exist(&new_key).await,
            "size-mismatched bytes under the new generation are evicted"
        );
        assert!(
            result_cache.get(&pre_heal_result_key, None).is_none(),
            "broadcast purge covers every result generation of the logical file"
        );

        let rows = collect_files_to_download(true, std::slice::from_ref(&healed));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].2, new_key);
        assert_eq!(rows[1].3, old_sidecar.len() as i64 + 9);
        assert!(
            merge_evict_keys(std::slice::from_ref(&healed).iter().filter(|v| v.deleted)).is_empty(),
            "a heal put-row must never reach the evict list"
        );

        let _ = disk::remove(key).await;
        let _ = disk::remove(&old_key).await;
        let _ = disk::remove(&new_key).await;
    }

    /// M11 (d): with the sub-flag off the collector enqueues nothing — the
    /// env-off escape hatch keeps pre-flip behavior byte-for-byte.
    #[test]
    fn m11_cache_parquet_off_enqueues_nothing() {
        let item = config::meta::stream::FileKey::new(
            7,
            "acct".to_string(),
            "files/org/logs/s1/2026/08/18/00/m11_off.vix".to_string(),
            config::meta::stream::FileMeta {
                records: 1000,
                compressed_size: 4096,
                index_size: 512,
                ..Default::default()
            },
            false,
        );
        assert!(collect_files_to_download(false, std::slice::from_ref(&item)).is_empty());
    }

    #[test]
    fn test_file_content_response_creation() {
        // Test creating a FileContentResponse
        let file_content = FileContent {
            content: b"test content".to_vec(),
            filename: "test.txt".to_string(),
        };

        let response = FileContentResponse {
            entries: vec![file_content.clone()],
        };

        assert_eq!(response.entries.len(), 1);
        assert_eq!(response.entries[0].content, b"test content");
        assert_eq!(response.entries[0].filename, "test.txt");
    }

    #[test]
    fn test_file_key_creation() {
        // Test creating FileKey directly
        let file_key = FileKey {
            id: 123,
            key: "test/file.parquet".to_string(),
            account: "test_account".to_string(),
            deleted: false,
            meta: Some(FileMeta {
                compressed_size: 1024,
                index_size: 512,
                max_ts: 1234567890,
                ..Default::default()
            }),
            segment_ids: None,
        };

        assert_eq!(file_key.id, 123);
        assert_eq!(file_key.key, "test/file.parquet");
        assert_eq!(file_key.account, "test_account");
    }

    #[test]
    fn test_filter_deleted_items() {
        // Test filtering deleted items from FileList
        let items = [
            FileKey {
                id: 1,
                key: "test/file1.parquet".to_string(),
                account: "test_account".to_string(),
                deleted: false,
                meta: Some(FileMeta {
                    compressed_size: 1024,
                    index_size: 512,
                    max_ts: 1234567890,
                    ..Default::default()
                }),
                segment_ids: None,
            },
            FileKey {
                id: 2,
                key: "test/file2.parquet".to_string(),
                account: "test_account".to_string(),
                deleted: true,
                meta: Some(FileMeta {
                    compressed_size: 2048,
                    index_size: 1024,
                    max_ts: 1234567891,
                    ..Default::default()
                }),
                segment_ids: None,
            },
            FileKey {
                id: 3,
                key: "test/file3.parquet".to_string(),
                account: "test_account".to_string(),
                deleted: false,
                meta: Some(FileMeta {
                    compressed_size: 3072,
                    index_size: 1536,
                    max_ts: 1234567892,
                    ..Default::default()
                }),
                segment_ids: None,
            },
        ];

        let non_deleted_items: Vec<&FileKey> = items.iter().filter(|v| !v.deleted).collect();

        assert_eq!(non_deleted_items.len(), 2);
        assert_eq!(non_deleted_items[0].id, 1);
        assert_eq!(non_deleted_items[1].id, 3);
    }

    #[test]
    fn test_chunk_size_calculation() {
        // Test chunk size calculation logic
        let total_size = 10000u64;
        let mut offset = 0u64;
        let chunk_size = 4096u64;

        let mut chunks = Vec::new();
        while offset < total_size {
            let current_chunk_size = std::cmp::min(chunk_size, total_size - offset);
            chunks.push(current_chunk_size);
            offset += current_chunk_size;
        }

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], 4096);
        assert_eq!(chunks[1], 4096);
        assert_eq!(chunks[2], 1808); // 10000 - 8192
        assert_eq!(offset, total_size);
    }

    #[test]
    fn test_range_creation() {
        // Test creating ranges for file reading
        let offset = 1024u64;
        let chunk_size = 512u64;
        let range = Range {
            start: offset,
            end: offset + chunk_size,
        };

        assert_eq!(range.start, 1024);
        assert_eq!(range.end, 1536);
        assert_eq!(range.end - range.start, 512);
    }

    #[test]
    fn test_file_meta_validation() {
        // Test FileMeta validation
        let valid_meta = FileMeta {
            compressed_size: 1024,
            index_size: 512,
            max_ts: 1234567890,
            ..Default::default()
        };

        assert!(valid_meta.compressed_size > 0);
        assert!(valid_meta.index_size > 0);
        assert!(valid_meta.max_ts > 0);

        // Test with zero values
        let zero_meta = FileMeta {
            compressed_size: 0,
            index_size: 0,
            max_ts: 0,
            ..Default::default()
        };

        assert_eq!(zero_meta.compressed_size, 0);
        assert_eq!(zero_meta.index_size, 0);
        assert_eq!(zero_meta.max_ts, 0);
    }

    #[test]
    fn test_cache_type_enum() {
        // Test CacheType enum values
        assert_eq!(CacheType::Disk as u32, 0);
        assert_eq!(CacheType::Memory as u32, 1);
    }

    #[test]
    fn test_metadata_map_creation() {
        // Test MetadataMap creation from tonic Request
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("test_key", "test_value".parse().unwrap());

        let request = Request::new(FileList {
            node_addr: "test_node".to_string(),
            items: vec![],
        });
        // Note: We can't easily test MetadataMap extraction without a real gRPC context
        // This test just ensures the type exists and can be referenced
        let _metadata_map = MetadataMap(&request.metadata().clone());
    }

    #[test]
    fn test_empty_response_creation() {
        // Test EmptyResponse creation
        let empty_response = EmptyResponse {};
        // EmptyResponse is a unit struct, so its size is 0
        assert_eq!(std::mem::size_of_val(&empty_response), 0);
    }

    #[test]
    fn test_file_download_batch_creation() {
        // Test creating file download batch
        let files_to_download = [
            (
                "file1".to_string(),
                "account1".to_string(),
                "key1".to_string(),
                1024,
                1234567890,
            ),
            (
                "file2".to_string(),
                "account2".to_string(),
                "key2".to_string(),
                2048,
                1234567891,
            ),
        ];

        assert_eq!(files_to_download.len(), 2);
        assert_eq!(files_to_download[0].0, "file1");
        assert_eq!(files_to_download[0].1, "account1");
        assert_eq!(files_to_download[0].2, "key1");
        assert_eq!(files_to_download[0].3, 1024);
        assert_eq!(files_to_download[0].4, 1234567890);
    }

    #[test]
    fn test_error_handling_patterns() {
        // Test common error handling patterns used in the code
        let result: Result<(), anyhow::Error> = Err(anyhow::anyhow!("test error"));

        match result {
            Ok(_) => panic!("Expected error"),
            Err(e) => {
                assert_eq!(e.to_string(), "test error");
            }
        }
    }

    #[test]
    fn test_logging_patterns() {
        // Test that logging patterns are consistent
        let path = "test/file.parquet";
        let total_size = 1024u64;
        let offset = 512u64;
        let elapsed_ms = 100u128;

        // This test just ensures the logging format is valid
        let log_message = format!(
            "[gRPC:Event] Send file: {path}, total_size: {total_size}, offset: {offset} took: {elapsed_ms} ms"
        );

        assert!(log_message.contains(path));
        assert!(log_message.contains(&total_size.to_string()));
        assert!(log_message.contains(&offset.to_string()));
        assert!(log_message.contains(&elapsed_ms.to_string()));
    }
}
