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

        // M3 sidecar refresh (sidecar-only heal, DESIGN-V2 §5): an updated
        // row keeps its data key but points at a REWRITTEN `.vxi` — evict
        // the stale local copies before the download block re-fetches.
        // Runs for EVERY querier — on-demand caches go stale exactly like
        // cache_latest ones.
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
                let del_items = merge_evict_keys(
                    req.items
                        .iter()
                        .filter(|v| v.deleted)
                        .map(|v| v.key.as_str()),
                );
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
/// enqueues on a caching querier (M11 default-on): each cacheable data file
/// PLUS its `.vxi` index sidecar when the row records one (v2 semantics:
/// `index_size` IS the sidecar object's exact size, `0` ⟺ no sidecar;
/// non-`.vix` keys derive no sidecar). `cache_parquet=false` enqueues
/// nothing. Undersized rows (`should_download`) and rows past the
/// disk-cache max age skip WHOLE — safe for sidecar-only heals because the
/// stale bytes were already evicted, so the next query fills on demand
/// instead of reading stale cache.
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
            && let Some(sidecar) = config::vix_sidecar_key(&item.key)
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

/// Cache keys a broadcast's DELETED rows evict (merge inputs the output
/// replaced, retention deletes): each data key plus its `.vxi` sidecar key
/// when derivable — a cheap no-op for keys never cached. Sidecar-only
/// heals broadcast their row with `deleted=false`, so a heal can NEVER
/// land here: the still-valid data bytes stay cached (M3 invariant, pinned
/// by `m11_sidecar_only_heal_refreshes_sidecar_keeps_data`).
fn merge_evict_keys<'a>(deleted_keys: impl Iterator<Item = &'a str>) -> Vec<String> {
    deleted_keys
        .flat_map(|k| std::iter::once(k.to_string()).chain(config::vix_sidecar_key(k)))
        .collect()
}

/// M3 sidecar-only heal (DESIGN-V2 §5): a file-update broadcast re-uses the
/// data key but points at a REWRITTEN `.vxi`, so locally cached sidecar
/// bytes whose size disagrees with the row's `index_size` are stale — evict
/// them from the disk + memory byte caches, and drop the memoized parsed
/// reader (keyed by the DATA key; it holds pre-heal index state). All cheap
/// no-ops for fresh adds: nothing is cached under a brand-new key.
/// Staleness until this fires is CORRECT by design (docs unchanged — the
/// old sidecar serves pre-heal results); this is the refresh.
///
/// M12: the per-file RESULT cache is purged too (`remove_file_entries`, one
/// pass per broadcast) — an answer-changing heal must never keep serving
/// pre-heal answers. The result-cache key also carries `index_size`, so
/// even a purge missed here (node down at broadcast time) cannot serve a
/// stale entry once the search path sees the healed row's meta; this sweep
/// is the immediacy path AND frees the dead entries' budget.
async fn evict_stale_sidecar_caches(put_items: &[FileKey]) {
    let mut core_keys: Vec<&str> = Vec::new();
    for item in put_items.iter() {
        let Some(sidecar) = config::vix_sidecar_key(&item.key) else {
            continue;
        };
        core_keys.push(item.key.as_str());
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
        crate::service::search::vix::reader_cache::GLOBAL_CACHE.remove(&item.key);
    }
    let purged =
        crate::service::search::vix::cache::GLOBAL_CACHE.remove_file_entries(core_keys);
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

    /// M3 sidecar-only heal: an update broadcast whose row `index_size`
    /// disagrees with the locally cached `.vxi` must EVICT the stale bytes;
    /// a matching size must leave them cached (fresh adds stay no-ops).
    #[tokio::test]
    async fn evict_stale_sidecar_caches_drops_size_mismatched_bytes() {
        if !get_config().disk_cache.enabled {
            return; // nothing to cover without a byte cache
        }
        let key = "files/e2e/logs/healcache/2026/08/17/00/sidecar_evict_test.vix";
        let sidecar = config::vix_sidecar_key(key).expect("core keys have sidecar keys");
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

    /// M11 (a): a new-file broadcast on a caching querier enqueues BOTH the
    /// `.vix` data object and its `.vxi` sidecar; `index_size` carries v2
    /// semantics (the sidecar's exact object size, 0 = no sidecar) and
    /// non-`.vix` keys never derive one.
    #[test]
    fn m11_new_file_event_enqueues_data_and_sidecar() {
        let meta = |records: i64, index_size: i64| config::meta::stream::FileMeta {
            records,
            compressed_size: 4096,
            index_size,
            ..Default::default()
        };
        let core = config::meta::stream::FileKey::new(
            7,
            "acct".to_string(),
            "files/org/logs/s1/2026/08/18/00/m11_new.vix".to_string(),
            meta(1000, 512),
            false,
        );
        let rows = collect_files_to_download(true, std::slice::from_ref(&core));
        assert_eq!(rows.len(), 2, "data object + index sidecar");
        assert_eq!(rows[0].2, core.key);
        assert_eq!(rows[0].3, 4096, "data row sized by compressed_size");
        assert_eq!(
            rows[1].2,
            config::vix_sidecar_key(&core.key).unwrap(),
            "sidecar row derives the .vxi key"
        );
        assert_eq!(
            rows[1].3, 512,
            "sidecar row sized by index_size (v2: exact object size)"
        );
        assert_eq!(rows[1].0, core.id, "sidecar keeps the data row's file id");

        // index_size == 0 ⟺ no sidecar exists: data row only
        let no_sidecar = config::meta::stream::FileKey::new(
            8,
            "acct".to_string(),
            "files/org/logs/s1/2026/08/18/00/m11_plain.vix".to_string(),
            meta(1000, 0),
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
            meta(1000, 512),
            false,
        );
        let rows = collect_files_to_download(true, std::slice::from_ref(&legacy));
        assert_eq!(rows.len(), 1, "non-.vix keys have no derivable sidecar");

        // undersized rows skip whole (data AND sidecar)
        let tiny = config::meta::stream::FileKey::new(
            10,
            "acct".to_string(),
            "files/org/logs/s1/2026/08/18/00/m11_tiny.vix".to_string(),
            meta(5, 512),
            false,
        );
        assert!(
            collect_files_to_download(true, std::slice::from_ref(&tiny)).is_empty(),
            "rows under file_download_min_records enqueue nothing"
        );
    }

    /// M11 (b): a merge broadcast's deleted rows evict the input data keys
    /// AND their `.vxi` sidecar keys; non-deleted rows never contribute.
    #[test]
    fn m11_merge_event_evicts_inputs_with_sidecars() {
        let keys = merge_evict_keys(
            ["files/org/a/1.vix", "files/org/a/2.parquet"]
                .iter()
                .copied(),
        );
        assert_eq!(
            keys,
            vec![
                "files/org/a/1.vix".to_string(),
                "files/org/a/1.vxi".to_string(),
                "files/org/a/2.parquet".to_string(),
            ],
            ".vix inputs evict both objects, legacy inputs just the data file"
        );
        assert!(
            merge_evict_keys(std::iter::empty()).is_empty(),
            "no deleted rows, nothing evicted"
        );
    }

    /// M11 (c) — THE flip-sensitive case: a sidecar-only heal broadcast
    /// (same data key, `deleted=false`, new `index_size`) must refresh the
    /// sidecar WITHOUT touching still-valid cached data bytes. Eviction
    /// hits only the stale `.vxi`; the re-enqueue lists both objects (the
    /// downloader's disk::exist check no-ops the data row); and because the
    /// heal row is a put, the merge-evict list can never contain it.
    #[tokio::test]
    async fn m11_sidecar_only_heal_refreshes_sidecar_keeps_data() {
        if !get_config().disk_cache.enabled {
            return; // nothing to cover without a byte cache
        }
        let key = "files/e2e/logs/healcache/2026/08/18/00/m11_heal_keepdata.vix";
        let sidecar = config::vix_sidecar_key(key).expect("core keys have sidecar keys");
        let data = bytes::Bytes::from_static(b"data bytes the heal must not touch");
        let old_sidecar = bytes::Bytes::from_static(b"pre-heal sidecar bytes");
        infra::cache::file_data::disk::set(key, data.clone())
            .await
            .unwrap();
        infra::cache::file_data::disk::set(&sidecar, old_sidecar.clone())
            .await
            .unwrap();
        // M12: memoized per-file RESULT entries (any condition, any pre-heal
        // index_size) must not survive the heal broadcast either
        let result_cache = &crate::service::search::vix::cache::GLOBAL_CACHE;
        let pre_heal_result_key = format!("{key}|{}|deadbeef_n_full", old_sidecar.len());
        result_cache.put(
            pre_heal_result_key.clone(),
            crate::service::search::vix::cache::CacheEntry::NoMatch,
        );

        // the heal broadcast: same data key, rewritten sidecar (new size)
        let healed = config::meta::stream::FileKey::new(
            1,
            String::new(),
            key.to_string(),
            config::meta::stream::FileMeta {
                records: 1000,
                compressed_size: data.len() as i64,
                index_size: old_sidecar.len() as i64 + 9,
                ..Default::default()
            },
            false,
        );

        evict_stale_sidecar_caches(std::slice::from_ref(&healed)).await;
        assert!(
            disk::exist(key).await,
            "sidecar-only heal must leave the cached DATA bytes untouched"
        );
        assert!(
            !disk::exist(&sidecar).await,
            "the stale sidecar bytes must be evicted"
        );
        assert!(
            result_cache.get(&pre_heal_result_key, None).is_none(),
            "M12: the heal broadcast must purge the file's result-cache entries"
        );

        // refresh: the caching block re-enqueues both objects, sidecar at
        // its NEW size — the data row is a downloader no-op (still cached)
        let rows = collect_files_to_download(true, std::slice::from_ref(&healed));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].2, sidecar);
        assert_eq!(rows[1].3, old_sidecar.len() as i64 + 9);

        // deleted=false ⟹ the delete_merge_files branch can never evict it
        let evicted = merge_evict_keys(
            std::slice::from_ref(&healed)
                .iter()
                .filter(|v| v.deleted)
                .map(|v| v.key.as_str()),
        );
        assert!(
            evicted.is_empty(),
            "a heal put-row must never reach the evict list"
        );

        let _ = disk::remove(key).await;
        let _ = disk::remove(&sidecar).await;
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
