// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Process-wide admission for canonicalization/compression, not ordered sink IO.

use std::sync::{Arc, OnceLock};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use vortex::error::{VortexResult, vortex_err};

#[path = "../../config/src/vix_merge_budget.rs"]
mod config;

pub(crate) const UNIT: u64 = 64 * 1024;
static SHARED: OnceLock<Result<Arc<MergeBudget>, String>> = OnceLock::new();

pub(crate) struct MergeBudget {
    semaphore: Arc<Semaphore>,
    units: u32,
}

impl MergeBudget {
    fn new(bytes: u64) -> Self {
        let units = (bytes / UNIT).clamp(1, u64::from(u32::MAX)) as u32;
        Self {
            semaphore: Arc::new(Semaphore::new(units as usize)),
            units,
        }
    }

    pub(crate) async fn acquire(&self, bytes: u64) -> VortexResult<OwnedSemaphorePermit> {
        // A request larger than the entire budget acquires every permit. It
        // cannot deadlock waiting for permits that do not exist, nor overlap
        // another admitted CPU task. Rounding always charges at least one unit.
        let units = bytes.div_ceil(UNIT).clamp(1, u64::from(self.units)) as u32;
        self.semaphore
            .clone()
            .acquire_many_owned(units)
            .await
            .map_err(|_| vortex_err!("shared merge in-flight budget closed"))
    }
}

pub(crate) fn shared() -> VortexResult<&'static Arc<MergeBudget>> {
    SHARED
        .get_or_init(|| {
            config::from_env()
                .map(|bytes| Arc::new(MergeBudget::new(bytes)))
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| vortex_err!("{error}"))
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    #[test]
    fn all_writers_share_one_process_budget() {
        let writers: Vec<_> = (0..5).map(|_| shared().unwrap().clone()).collect();
        for writer in &writers[1..] {
            assert!(Arc::ptr_eq(&writers[0], writer));
        }
    }

    #[tokio::test]
    async fn concurrent_writers_cannot_multiply_the_budget() {
        let budget = Arc::new(MergeBudget::new(4 * UNIT));
        let writer_a = budget.clone();
        let writer_b = budget.clone();
        let a = writer_a.acquire(3 * UNIT).await.unwrap();
        let mut b = Box::pin(writer_b.acquire(2 * UNIT));
        assert!(b.as_mut().now_or_never().is_none());
        drop(a);
        let b = b.await.unwrap();
        assert_eq!(budget.semaphore.available_permits(), 2);
        let a = writer_a.acquire(2 * UNIT).await.unwrap();
        assert_eq!(budget.semaphore.available_permits(), 0);
        drop((a, b));
        assert_eq!(budget.semaphore.available_permits(), 4);
    }

    #[tokio::test]
    async fn oversized_is_exclusive_and_cancelled_waiter_does_not_leak() {
        let budget = MergeBudget::new(4 * UNIT);
        let first = budget.acquire(1).await.unwrap();
        let mut cancelled = Box::pin(budget.acquire(u64::MAX));
        assert!(cancelled.as_mut().now_or_never().is_none());
        drop(cancelled);
        assert_eq!(budget.semaphore.available_permits(), 3);
        let mut oversized = Box::pin(budget.acquire(u64::MAX));
        assert!(oversized.as_mut().now_or_never().is_none());
        drop(first);
        let exclusive = oversized.await.unwrap();
        assert_eq!(budget.semaphore.available_permits(), 0);
        assert!(Box::pin(budget.acquire(1)).now_or_never().is_none());
        drop(exclusive);
        assert_eq!(budget.semaphore.available_permits(), 4);
    }
}
