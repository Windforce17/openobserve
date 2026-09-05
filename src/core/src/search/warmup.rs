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

//! Post-boot index warmup (#39 GAP 2). Queriers start cache-cold since the
//! Deployment migration (ephemeral volumes): the first touch of every file
//! pays S3 round trips that o2's weeks-warm caches never see (measured
//! 2026-08-11: cold needle 4.1s vs 0.57s, count24h 102s vs 51s). This task
//! runs once after the node comes ONLINE: it lists the last
//! `ZO_WARMUP_CACHE_HOURS` hours of files, keeps THIS node's
//! consistent-hash share — the same ring walk the query fan-out uses, so
//! each node warms exactly the files it will be asked to serve — and opens
//! each `.vix` through the normal ranged ladder, populating the disk and
//! memory caches with the footer tail and memoizing the parsed reader.
//! Best-effort: errors skip, the query path never waits on it.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use config::{cluster::LOCAL_NODE, get_config, meta::cluster::Role, utils::time::now_micros};
use futures::StreamExt;
use hashbrown::HashSet;
use infra::cluster::{get_cached_online_query_nodes, get_node_from_consistent_hash_within};

use crate::service::search as SearchService;

pub async fn warm_recent_indexes() {
    let cfg = get_config();
    let hours = cfg.limit.warmup_cache_hours;
    if hours == 0 {
        return;
    }
    let started = std::time::Instant::now();
    let end = now_micros();
    let min_ts = end.saturating_sub((hours as i64).saturating_mul(3_600_000_000));

    let Some(nodes) = get_cached_online_query_nodes(None).await else {
        log::warn!("[WARMUP] no online query nodes visible; skipping");
        return;
    };
    let allowed: HashSet<String> = nodes
        .iter()
        .filter(|n| n.is_querier())
        .map(|n| n.name.clone())
        .collect();
    if !allowed.contains(&LOCAL_NODE.name) {
        log::warn!("[WARMUP] this node is not in the querier ring yet; skipping");
        return;
    }

    // enumerate candidate files across every org/stream, newest first
    let mut files = Vec::new();
    let stream_types = [
        config::meta::stream::StreamType::Logs,
        config::meta::stream::StreamType::Metrics,
        config::meta::stream::StreamType::Traces,
    ];
    for org in crate::service::db::schema::list_organizations_from_cache().await {
        for stream_type in stream_types {
            for stream in
                crate::service::db::schema::list_streams_from_cache(&org, stream_type).await
            {
                match crate::service::file_list::query(
                    "warmup",
                    &org,
                    stream_type,
                    &stream,
                    infra::schema::get_partition_time_level(stream_type),
                    min_ts,
                    end,
                )
                .await
                {
                    Ok(mut keys) => files.append(&mut keys),
                    Err(e) => {
                        log::debug!("[WARMUP] file_list {org}/{stream_type}/{stream}: {e}");
                    }
                }
            }
        }
    }
    let candidates = files.len();

    // keep this node's ring share, newest first, bounded
    let mut own = Vec::new();
    for file in files {
        // no index, nothing to warm: column-store-only files (#40,
        // index-off stream types) and legacy parquet both stamp
        // index_size 0 — skipping them BEFORE the ring walk keeps the
        // bounded warm queue for files whose footer/dictionary actually
        // pays off.
        if file.meta.index_size == 0 {
            continue;
        }
        let owner = get_node_from_consistent_hash_within(
            &file.id.to_string(),
            &Role::Querier,
            None,
            &allowed,
        )
        .await;
        if owner.as_deref() == Some(LOCAL_NODE.name.as_str()) {
            own.push(file);
        }
    }
    own.sort_unstable_by_key(|f| std::cmp::Reverse(f.meta.max_ts));
    own.truncate(cfg.limit.warmup_cache_max_files);

    let warmed = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let own_len = own.len();
    futures::stream::iter(own)
        .for_each_concurrent(cfg.limit.warmup_cache_concurrency.max(1), |file| {
            let warmed = Arc::clone(&warmed);
            let skipped = Arc::clone(&skipped);
            let failed = Arc::clone(&failed);
            async move {
                match SearchService::vix::warm_file(
                    &file.account,
                    &file.key,
                    file.meta.compressed_size,
                    file.meta.index_size,
                    file.meta.index_generation,
                )
                .await
                {
                    Ok(true) => {
                        warmed.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(false) => {
                        skipped.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        log::debug!("[WARMUP] {}: {e:#}", file.key);
                    }
                }
            }
        })
        .await;

    log::info!(
        "[WARMUP] done: {} of {} candidate files were this node's share; warmed {}, skipped {}, failed {}, took {:.1}s (ZO_WARMUP_CACHE_HOURS={hours})",
        own_len,
        candidates,
        warmed.load(Ordering::Relaxed),
        skipped.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        started.elapsed().as_secs_f64(),
    );
}
