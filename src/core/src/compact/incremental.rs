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

//! Write-side trigger for incremental compaction of recent/open hours.
//!
//! Each ingester counts uploaded files independently for every
//! `(org, stream type, stream, data hour)`. Counters are local hints; the
//! durable job is deduplicated by `(stream, offsets)`.
//!
//! Normal counters are limited to the fixed four-hour domain
//! `[now_hour - 2h, now_hour + 1h]`, independent of the configured ingest
//! admission window. Older late-arriving L0 is covered by the 60-second
//! merge-debt sweep. Failed or in-flight counters survive window movement,
//! with a hard cap of eight counters per stream.

use std::sync::LazyLock;

use config::{
    RwAHashMap, get_config,
    meta::stream::StreamType,
    utils::time::{hour_micros, now_micros},
};
use hashbrown::HashMap;
use infra::cluster::get_cached_online_ingester_nodes;

const MAX_HOURS_PER_STREAM: usize = 8;
const HOURS_BEHIND: i64 = 2;
const HOURS_AHEAD: i64 = 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PendingHour {
    // `files` includes the immutable snapshot in `in_flight_files` while
    // `scheduling` is true. Arrivals during scheduling are therefore
    // `files - in_flight_files`.
    files: usize,
    in_flight_files: usize,
    scheduling: bool,
    // Durable local debt. Taking a candidate consumes it; arrivals may latch
    // it again while that candidate is being scheduled.
    schedule_required: bool,
}

impl PendingHour {
    fn outstanding_files(&self) -> usize {
        self.files.saturating_sub(self.in_flight_files)
    }
}

#[derive(Debug, Default)]
struct PendingStream {
    hours: HashMap<i64, PendingHour>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordOutcome {
    Recorded,
    IgnoredOutsideWindow,
    CapacityRefused,
}

static PENDING_FILES: LazyLock<RwAHashMap<String, PendingStream>> = LazyLock::new(Default::default);

fn in_recent_window(hour: i64, now_hour: i64) -> bool {
    let hour_size = hour_micros(1);
    hour >= now_hour - HOURS_BEHIND * hour_size && hour <= now_hour + HOURS_AHEAD * hour_size
}

async fn record_pending_file(
    pending: &RwAHashMap<String, PendingStream>,
    stream_key: &str,
    hour: i64,
    now_hour: i64,
    threshold: usize,
) -> RecordOutcome {
    let threshold = threshold.max(1);
    let mut streams = pending.write().await;
    if let Some(stream) = streams.get_mut(stream_key) {
        stream.hours.retain(|counter_hour, counter| {
            if counter.outstanding_files() >= threshold {
                counter.schedule_required = true;
            }
            in_recent_window(*counter_hour, now_hour)
                || counter.scheduling
                || counter.schedule_required
        });
    }
    if streams
        .get(stream_key)
        .is_some_and(|stream| stream.hours.is_empty())
    {
        streams.remove(stream_key);
    }

    if !in_recent_window(hour, now_hour) {
        return RecordOutcome::IgnoredOutsideWindow;
    }
    if !streams.contains_key(stream_key) {
        streams.insert(stream_key.to_owned(), PendingStream::default());
    }
    let stream = streams.get_mut(stream_key).expect("stream was inserted");
    if let Some(counter) = stream.hours.get_mut(&hour) {
        counter.files = counter.files.saturating_add(1);
        if counter.outstanding_files() >= threshold {
            counter.schedule_required = true;
        }
        return RecordOutcome::Recorded;
    }
    if stream.hours.len() >= MAX_HOURS_PER_STREAM {
        return RecordOutcome::CapacityRefused;
    }
    stream.hours.insert(
        hour,
        PendingHour {
            files: 1,
            schedule_required: threshold <= 1,
            ..Default::default()
        },
    );
    RecordOutcome::Recorded
}

async fn take_schedule_candidate(
    pending: &RwAHashMap<String, PendingStream>,
    stream_key: &str,
    threshold: usize,
) -> Option<i64> {
    let mut streams = pending.write().await;
    let stream = streams.get_mut(stream_key)?;
    let hour = stream
        .hours
        .iter()
        .filter(|(_, counter)| {
            !counter.scheduling && (counter.schedule_required || counter.files >= threshold.max(1))
        })
        .min_by_key(|(hour, counter)| (!counter.schedule_required, **hour))
        .map(|(hour, _)| *hour)?;
    let counter = stream.hours.get_mut(&hour).expect("candidate exists");
    counter.scheduling = true;
    counter.in_flight_files = counter.files;
    counter.schedule_required = false;
    Some(hour)
}

async fn finish_scheduling(
    pending: &RwAHashMap<String, PendingStream>,
    stream_key: &str,
    hour: i64,
    scheduled: bool,
    threshold: usize,
) {
    let mut streams = pending.write().await;
    let Some(stream) = streams.get_mut(stream_key) else {
        return;
    };
    let Some(counter) = stream.hours.get_mut(&hour) else {
        return;
    };
    if !scheduled {
        counter.in_flight_files = 0;
        counter.scheduling = false;
        counter.schedule_required = true;
        return;
    }

    counter.files = counter.files.saturating_sub(counter.in_flight_files);
    counter.in_flight_files = 0;
    counter.scheduling = false;
    counter.schedule_required |= counter.files >= threshold.max(1);
    if counter.files > 0 {
        return;
    }
    stream.hours.remove(&hour);
    if stream.hours.is_empty() {
        streams.remove(stream_key);
    }
}

/// Record one uploaded file and durably schedule its actual data hour once the
/// local threshold is reached.
pub async fn incr_pending_file(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    min_ts: i64,
) {
    let cfg = get_config();
    let ingester_num = get_cached_online_ingester_nodes()
        .await
        .map(|nodes| nodes.len())
        .unwrap_or(1)
        .max(1);
    let threshold =
        (cfg.compact.max_file_size / cfg.limit.max_file_size_in_memory.max(1) / ingester_num)
            .max(1)
            * 3;
    let hour_size = hour_micros(1);
    let requested_hour = min_ts.div_euclid(hour_size) * hour_size;
    let now = now_micros();
    let now_hour = now.div_euclid(hour_size) * hour_size;
    let stream_key = format!("{org_id}/{stream_type}/{stream_name}");

    let initial_outcome = record_pending_file(
        &PENDING_FILES,
        &stream_key,
        requested_hour,
        now_hour,
        threshold,
    )
    .await;
    let mut requested_needs_record = initial_outcome == RecordOutcome::CapacityRefused;
    if requested_needs_record {
        log::warn!(
            "[COMPACTOR:INCREMENTAL] bounded counter capacity reached for \
             [{org_id}/{stream_type}/{stream_name}]: all {MAX_HOURS_PER_STREAM} slots are \
             retained by recent, in-flight, or failed scheduling debt; hour {requested_hour} \
             is not counted yet and will be retried after each successful debt drain during \
             this arrival; if no slot opens, the 60-second merge-debt sweep is the fallback"
        );
    }

    loop {
        let Some(hour) = take_schedule_candidate(&PENDING_FILES, &stream_key, threshold).await
        else {
            break;
        };
        let result = infra::file_list::add_job(org_id, stream_type, stream_name, hour).await;
        let scheduled = result.is_ok();
        finish_scheduling(&PENDING_FILES, &stream_key, hour, scheduled, threshold).await;
        if let Err(e) = result {
            log::error!(
                "[COMPACTOR:INCREMENTAL] add_job failed for \
                 [{org_id}/{stream_type}/{stream_name}] hour {hour}: {e}; the counter is \
                 retained as schedule-required and will be retried on the next stream arrival"
            );
            break;
        }
        log::debug!(
            "[COMPACTOR:INCREMENTAL] enqueued incremental merge for \
             [{org_id}/{stream_type}/{stream_name}] hour {hour}"
        );
        if requested_needs_record {
            requested_needs_record = record_pending_file(
                &PENDING_FILES,
                &stream_key,
                requested_hour,
                now_hour,
                threshold,
            )
            .await
                == RecordOutcome::CapacityRefused;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM: &str = "org/logs/stream";
    const HOUR: i64 = 3_600_000_000;

    #[tokio::test]
    async fn recent_domain_tracks_exactly_four_hours() {
        let pending = RwAHashMap::default();
        let now = 100 * HOUR;
        for hour in 98..=101 {
            assert_eq!(
                record_pending_file(&pending, STREAM, hour * HOUR, now, 99).await,
                RecordOutcome::Recorded
            );
        }
        let streams = pending.read().await;
        let hours = &streams.get(STREAM).unwrap().hours;
        assert_eq!(hours.len(), 4);
        assert!(hours.keys().all(|hour| in_recent_window(*hour, now)));
    }

    #[tokio::test]
    async fn outside_domain_is_ignored_without_allocating() {
        let pending = RwAHashMap::default();
        let now = 100 * HOUR;
        assert_eq!(
            record_pending_file(&pending, STREAM, 97 * HOUR, now, 99).await,
            RecordOutcome::IgnoredOutsideWindow
        );
        assert_eq!(
            record_pending_file(&pending, STREAM, 102 * HOUR, now, 99).await,
            RecordOutcome::IgnoredOutsideWindow
        );
        assert!(pending.read().await.is_empty());
    }

    #[tokio::test]
    async fn lazy_prune_preserves_in_flight_and_schedule_debt() {
        let pending = RwAHashMap::default();
        let now = 100 * HOUR;
        record_pending_file(&pending, STREAM, 99 * HOUR, now, 1).await;
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 1).await,
            Some(99 * HOUR)
        );
        record_pending_file(&pending, STREAM, 100 * HOUR, now, 1).await;
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 1).await,
            Some(100 * HOUR)
        );
        finish_scheduling(&pending, STREAM, 100 * HOUR, false, 1).await;
        record_pending_file(&pending, STREAM, 500 * HOUR, 500 * HOUR, 99).await;

        let streams = pending.read().await;
        let hours = &streams.get(STREAM).unwrap().hours;
        assert!(hours.get(&(99 * HOUR)).unwrap().scheduling);
        assert!(hours.get(&(100 * HOUR)).unwrap().schedule_required);
        assert!(hours.contains_key(&(500 * HOUR)));
    }

    #[tokio::test]
    async fn failure_is_retried_on_next_outside_arrival() {
        let pending = RwAHashMap::default();
        let now = 100 * HOUR;
        record_pending_file(&pending, STREAM, 100 * HOUR, now, 1).await;
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 1).await,
            Some(100 * HOUR)
        );
        finish_scheduling(&pending, STREAM, 100 * HOUR, false, 1).await;
        assert_eq!(
            record_pending_file(&pending, STREAM, 50 * HOUR, now, usize::MAX).await,
            RecordOutcome::IgnoredOutsideWindow
        );
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, usize::MAX).await,
            Some(100 * HOUR)
        );
    }

    #[tokio::test]
    async fn failed_debt_cap_retries_refused_hour_after_drain() {
        let pending = RwAHashMap::default();
        let mut stream = PendingStream::default();
        for hour in 0..MAX_HOURS_PER_STREAM as i64 {
            stream.hours.insert(
                hour * HOUR,
                PendingHour {
                    files: 1,
                    schedule_required: true,
                    ..Default::default()
                },
            );
        }
        pending.write().await.insert(STREAM.to_owned(), stream);
        let requested_hour = 100 * HOUR;
        let threshold = 99;
        let mut needs_record =
            record_pending_file(&pending, STREAM, requested_hour, requested_hour, threshold).await
                == RecordOutcome::CapacityRefused;
        assert!(needs_record);
        let drained = take_schedule_candidate(&pending, STREAM, usize::MAX)
            .await
            .unwrap();
        finish_scheduling(&pending, STREAM, drained, true, threshold).await;
        if needs_record {
            needs_record =
                record_pending_file(&pending, STREAM, requested_hour, requested_hour, threshold)
                    .await
                    == RecordOutcome::CapacityRefused;
        }
        assert!(!needs_record);
        let streams = pending.read().await;
        let hours = &streams.get(STREAM).unwrap().hours;
        assert!(hours.contains_key(&requested_hour));
        assert!(hours.len() <= MAX_HOURS_PER_STREAM);
    }

    #[tokio::test]
    async fn alternating_hours_trigger_independently() {
        let pending = RwAHashMap::default();
        let now = 100 * HOUR;
        record_pending_file(&pending, STREAM, 99 * HOUR, now, 2).await;
        record_pending_file(&pending, STREAM, 100 * HOUR, now, 2).await;
        record_pending_file(&pending, STREAM, 99 * HOUR, now, 2).await;
        record_pending_file(&pending, STREAM, 100 * HOUR, now, 2).await;
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 2).await,
            Some(99 * HOUR)
        );
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 2).await,
            Some(100 * HOUR)
        );
    }

    #[tokio::test]
    async fn arrivals_during_scheduling_are_replayed_at_threshold() {
        let pending = RwAHashMap::default();
        let now = 100 * HOUR;
        record_pending_file(&pending, STREAM, 100 * HOUR, now, 1).await;
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 1).await,
            Some(100 * HOUR)
        );
        record_pending_file(&pending, STREAM, 100 * HOUR, now, 1).await;
        finish_scheduling(&pending, STREAM, 100 * HOUR, true, 1).await;
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 1).await,
            Some(100 * HOUR)
        );
        finish_scheduling(&pending, STREAM, 100 * HOUR, true, 1).await;
        assert!(pending.read().await.is_empty());
    }

    #[tokio::test]
    async fn lower_threshold_arrivals_latch_rerun_during_scheduling() {
        let pending = RwAHashMap::default();
        let now = 100 * HOUR;
        for _ in 0..12 {
            record_pending_file(&pending, STREAM, now, now, 12).await;
        }
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 12).await,
            Some(now)
        );

        for _ in 0..3 {
            record_pending_file(&pending, STREAM, now, now, 3).await;
        }
        finish_scheduling(&pending, STREAM, now, true, 12).await;

        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 12).await,
            Some(now)
        );
    }

    #[tokio::test]
    async fn lazy_window_moves_forward_and_backward() {
        let pending = RwAHashMap::default();
        let now = 100 * HOUR;
        for hour in 98..=101 {
            record_pending_file(&pending, STREAM, hour * HOUR, now, 99).await;
        }
        record_pending_file(&pending, STREAM, 103 * HOUR, 102 * HOUR, 99).await;
        {
            let streams = pending.read().await;
            let hours = &streams.get(STREAM).unwrap().hours;
            assert_eq!(hours.len(), 3);
            assert!(hours.contains_key(&(100 * HOUR)));
            assert!(hours.contains_key(&(101 * HOUR)));
            assert!(hours.contains_key(&(103 * HOUR)));
        }
        record_pending_file(&pending, STREAM, 99 * HOUR, 99 * HOUR, 99).await;
        let streams = pending.read().await;
        let hours = &streams.get(STREAM).unwrap().hours;
        assert_eq!(hours.len(), 2);
        assert!(hours.contains_key(&(99 * HOUR)));
        assert!(hours.contains_key(&(100 * HOUR)));
    }

    #[tokio::test]
    async fn threshold_crossing_survives_window_roll_before_take() {
        let pending = RwAHashMap::default();
        let old_now = 100 * HOUR;
        let old_hour = 98 * HOUR;
        record_pending_file(&pending, STREAM, old_hour, old_now, 2).await;
        record_pending_file(&pending, STREAM, old_hour, old_now, 2).await;
        let new_now = 101 * HOUR;
        record_pending_file(&pending, STREAM, 101 * HOUR, new_now, 99).await;
        assert!(
            pending
                .read()
                .await
                .get(STREAM)
                .unwrap()
                .hours
                .get(&old_hour)
                .unwrap()
                .schedule_required
        );
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 2).await,
            Some(old_hour)
        );
    }

    #[tokio::test]
    async fn threshold_decrease_preserves_ready_old_hour_during_prune() {
        let pending = RwAHashMap::default();
        let old_now = 100 * HOUR;
        let old_hour = 98 * HOUR;
        record_pending_file(&pending, STREAM, old_hour, old_now, 10).await;
        record_pending_file(&pending, STREAM, old_hour, old_now, 10).await;
        let new_now = 101 * HOUR;
        record_pending_file(&pending, STREAM, 101 * HOUR, new_now, 2).await;
        assert!(
            pending
                .read()
                .await
                .get(STREAM)
                .unwrap()
                .hours
                .get(&old_hour)
                .unwrap()
                .schedule_required
        );
        record_pending_file(&pending, STREAM, 102 * HOUR, new_now, 99).await;
        assert_eq!(
            take_schedule_candidate(&pending, STREAM, 2).await,
            Some(old_hour)
        );
    }
}
