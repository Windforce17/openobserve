// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::time::Duration;

use tokio::time::Instant;

use super::{Lazy, get_config};

// Restrict pacing to admitted workers. Foreground reads and compaction never
// inherit this scope. Range futures are polled within the same worker task.
tokio::task_local! { pub(super) static BACKGROUND: bool; }

const QUANTUM: usize = 64 * 1024;
// Tokio sleeps resolve to about 1 ms. A bucket that holds only one quantum
// therefore caps throughput near one quantum per timer tick (about 40 MB/s in
// the local download E2E) whatever the configured rate is. Holding 20 ms of
// the current rate spans many ticks, so the measured rate tracks the
// configured rate, and an idle period still grants at most a 20 ms burst.
const BURST_WINDOW: Duration = Duration::from_millis(20);
static QUERY_UNTIL: Lazy<parking_lot::Mutex<Instant>> =
    Lazy::new(|| parking_lot::Mutex::new(Instant::now()));
static LIMITER: Lazy<RateLimiter> = Lazy::new(RateLimiter::new);

pub(super) fn note_query_activity() {
    *QUERY_UNTIL.lock() = Instant::now() + Duration::from_secs(30);
}

struct Bucket {
    tokens: f64,
    updated: Instant,
    rate: u64,
}

fn burst_capacity(rate: u64) -> f64 {
    (rate as f64 * BURST_WINDOW.as_secs_f64()).max(QUANTUM as f64)
}

impl Bucket {
    fn refill(&mut self, now: Instant, rate: u64) {
        // Account elapsed time at the previous rate. A downgrade discards any
        // idle burst so foreground admission takes effect on the next quantum.
        self.tokens = (self.tokens
            + now.duration_since(self.updated).as_secs_f64() * self.rate as f64)
            .min(burst_capacity(rate));
        if rate < self.rate {
            self.tokens = 0.0;
        }
        self.updated = now;
        self.rate = rate;
    }
}

struct RateLimiter {
    bucket: tokio::sync::Mutex<Bucket>,
}
impl RateLimiter {
    fn new() -> Self {
        Self {
            bucket: tokio::sync::Mutex::new(Bucket {
                tokens: 0.0,
                updated: Instant::now(),
                rate: 0,
            }),
        }
    }

    async fn acquire(&self, mut bytes: usize, idle: u64, busy: u64, active: impl Fn() -> bool) {
        while bytes > 0 {
            let amount = bytes.min(QUANTUM);
            // Tokio Mutex is FIFO. Release it after every quantum, including
            // for large chunks, so a large response cannot monopolize the rate.
            let mut bucket = self.bucket.lock().await;
            loop {
                let rate = if active() { busy.min(idle) } else { idle }.max(1);
                bucket.refill(Instant::now(), rate);
                if bucket.tokens >= amount as f64 {
                    bucket.tokens -= amount as f64;
                    break;
                }
                let wait = Duration::from_secs_f64((amount as f64 - bucket.tokens) / rate as f64);
                tokio::time::sleep(wait.min(Duration::from_millis(100))).await;
            }
            drop(bucket);
            bytes -= amount;
        }
    }
}

pub(crate) async fn throttle(bytes: usize) {
    if !BACKGROUND.try_with(|enabled| *enabled).unwrap_or(false) {
        return;
    }
    let cfg = get_config();
    if !cfg.limit.file_download_rate_enabled {
        return;
    }
    LIMITER
        .acquire(
            bytes,
            cfg.limit.file_download_rate_bytes_per_sec,
            cfg.limit.file_download_query_rate_bytes_per_sec,
            || Instant::now() < *QUERY_UNTIL.lock(),
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn governor_downgrade_discards_idle_credit_and_bounds_burst() {
        let now = Instant::now();
        let mut bucket = Bucket {
            tokens: 0.0,
            updated: now,
            rate: 100_000,
        };
        bucket.refill(now + Duration::from_secs(100), 100_000);
        assert_eq!(bucket.tokens, QUANTUM as f64);
        bucket.refill(now + Duration::from_secs(100), 1000);
        assert_eq!(bucket.tokens, 0.0);
        bucket.refill(now + Duration::from_secs(101), 1000);
        assert_eq!(bucket.tokens, 1000.0);
    }

    #[test]
    fn governor_burst_spans_timer_ticks_at_high_rates() {
        let rate = 200 * 1024 * 1024;
        let now = Instant::now();
        let mut bucket = Bucket {
            tokens: 0.0,
            updated: now,
            rate,
        };
        // One 1 ms timer tick must grant more than one quantum at this rate.
        bucket.refill(now + Duration::from_millis(1), rate);
        assert!(bucket.tokens > QUANTUM as f64, "tokens {}", bucket.tokens);
        // An idle period is still bounded by the 20 ms burst window.
        bucket.refill(now + Duration::from_secs(10), rate);
        assert_eq!(bucket.tokens, burst_capacity(rate));
        assert_eq!(burst_capacity(1000), QUANTUM as f64);
    }

    #[tokio::test]
    async fn governor_throughput_tracks_configured_rate() {
        // 32 MiB at 400 MiB/s needs about 80 ms. The one-quantum bucket took
        // at least one timer tick per 64 KiB quantum, i.e. >= 512 ms.
        let rate = 400 * 1024 * 1024;
        let bytes = 32 * 1024 * 1024;
        let limiter = RateLimiter::new();
        let started = std::time::Instant::now();
        limiter.acquire(bytes, rate, rate, || false).await;
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(400),
            "32 MiB at 400 MiB/s took {elapsed:?}"
        );
        // Still paced: no more than the burst window can be skipped.
        let floor = Duration::from_secs_f64(bytes as f64 / rate as f64) - BURST_WINDOW;
        assert!(elapsed >= floor, "took {elapsed:?}, floor {floor:?}");
    }

    #[tokio::test]
    async fn governor_cancellation_releases_fair_waiter() {
        let limiter = std::sync::Arc::new(RateLimiter::new());
        let blocked = tokio::spawn({
            let limiter = limiter.clone();
            async move {
                limiter.acquire(QUANTUM, 1, 1, || true).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        blocked.abort();
        assert!(blocked.await.unwrap_err().is_cancelled());
        tokio::time::timeout(
            Duration::from_secs(1),
            limiter.acquire(1, 1_000_000, 1_000_000, || false),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn governor_foreground_scope_is_not_throttled() {
        tokio::time::timeout(Duration::from_millis(10), throttle(usize::MAX))
            .await
            .unwrap();
        note_query_activity();
        assert!(Instant::now() < *QUERY_UNTIL.lock());
    }
}
