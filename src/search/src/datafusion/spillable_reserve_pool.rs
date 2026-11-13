// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Query-only headroom for consumers that cannot respond to memory pressure.

use std::{fmt, sync::Arc};

use datafusion::{
    error::{DataFusionError, Result},
    execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
};
use parking_lot::Mutex;

#[derive(Debug, Default)]
struct Usage {
    total: usize,
    spillable: usize,
}

/// Admission is tentative until the inner pool accepts it. Never call another
/// pool while holding `usage`: nested local/global wrappers and tracking-pool
/// error reporting therefore cannot create a lock-order cycle.
#[derive(Debug)]
pub(super) struct SpillableReservePool {
    label: &'static str,
    limit: usize,
    spillable_limit: usize,
    usage: Mutex<Usage>,
    inner: Arc<dyn MemoryPool>,
}

impl SpillableReservePool {
    pub(super) fn wrap(
        label: &'static str,
        limit: usize,
        percent: u8,
        inner: Arc<dyn MemoryPool>,
    ) -> Arc<dyn MemoryPool> {
        // Return the identical Arc: the disabled path adds no accounting,
        // admission policy, synchronization or change to the underlying pool.
        if percent == 0 {
            return inner;
        }
        assert!(
            percent <= 50,
            "headroom percent must be validated at startup"
        );
        // The widened product is bounded by usize::MAX * 100, including on
        // 64-bit hosts. Round down once, not once per percentage point.
        let spillable_limit = usize::try_from((limit as u128) * (100 - u128::from(percent)) / 100)
            .expect("a percentage of a usize fits usize");
        Arc::new(Self {
            label,
            limit,
            spillable_limit,
            usage: Mutex::default(),
            inner,
        })
    }

    fn add(&self, reservation: &MemoryReservation, additional: usize, enforce: bool) -> Result<()> {
        let mut usage = self.usage.lock();
        let total = usage.total.checked_add(additional).ok_or_else(|| {
            DataFusionError::ResourcesExhausted(format!("{} accounting overflow", self.label))
        })?;
        let spillable = if reservation.consumer().can_spill() {
            let spillable = usage.spillable.checked_add(additional).ok_or_else(|| {
                DataFusionError::ResourcesExhausted(format!(
                    "{} spillable accounting overflow",
                    self.label
                ))
            })?;
            if enforce && spillable > self.spillable_limit {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "{} spillable reserve: cannot reserve {additional} additional bytes for {}: \
                     {} spillable bytes already reserved, spillable limit {}, total limit {}",
                    self.label,
                    reservation.consumer().name(),
                    usage.spillable,
                    self.spillable_limit,
                    self.limit,
                )));
            }
            spillable
        } else {
            usage.spillable
        };
        if enforce && total > self.limit {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "{} memory pool cannot reserve {additional} additional bytes: \
                 {} of {} bytes already reserved",
                self.label, usage.total, self.limit,
            )));
        }
        usage.total = total;
        usage.spillable = spillable;
        Ok(())
    }

    fn subtract(&self, reservation: &MemoryReservation, size: usize) {
        let mut usage = self.usage.lock();
        let total = usage
            .total
            .checked_sub(size)
            .expect("headroom total accounting underflow");
        let spillable = if reservation.consumer().can_spill() {
            usage
                .spillable
                .checked_sub(size)
                .expect("headroom spillable accounting underflow")
        } else {
            usage.spillable
        };
        usage.total = total;
        usage.spillable = spillable;
    }
}

impl MemoryPool for SpillableReservePool {
    fn name(&self) -> &str {
        self.label
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        // MemoryReservation::drop/free already shrank every reservation. split,
        // take and new_empty share the consumer; subtracting again would be wrong.
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        // grow is infallible with respect to policy, even beyond either limit.
        // An unrepresentable byte count is a programming error, never wrap it.
        self.add(reservation, additional, false)
            .expect("headroom infallible grow accounting overflow");
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.subtract(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.add(reservation, additional, true)?;
        if let Err(error) = self.inner.try_grow(reservation, additional) {
            self.subtract(reservation, additional);
            return Err(error);
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.usage.lock().total
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.limit)
    }
}

impl fmt::Display for SpillableReservePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (total, spillable) = {
            let usage = self.usage.lock();
            (usage.total, usage.spillable)
        };
        write!(
            f,
            "{}(used: {total}, spillable: {spillable}, spillable limit: {}, limit: {}, inner: {})",
            self.label, self.spillable_limit, self.limit, self.inner
        )
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use datafusion::execution::memory_pool::{
        FairSpillPool, GreedyMemoryPool, TrackConsumersPool, UnboundedMemoryPool,
    };

    use super::*;

    fn spill(pool: &Arc<dyn MemoryPool>) -> MemoryReservation {
        MemoryConsumer::new("spill")
            .with_can_spill(true)
            .register(pool)
    }

    #[test]
    fn headroom_caps_combined_spillable_and_allows_full_unspillable() {
        for inner in [
            Arc::new(GreedyMemoryPool::new(100)) as Arc<dyn MemoryPool>,
            Arc::new(FairSpillPool::new(100)),
            Arc::new(UnboundedMemoryPool::default()),
        ] {
            let pool = SpillableReservePool::wrap("test", 100, 15, inner);
            let a = spill(&pool);
            a.try_grow(60).unwrap();
            // Late registration must not permit aggregate overcommit in FairSpillPool.
            let b = spill(&pool);
            b.try_grow(25).unwrap();
            let error = b.try_grow(1).unwrap_err();
            assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
            assert!(error.to_string().contains("spillable reserve"));
            let opener = MemoryConsumer::new("opener").register(&pool);
            opener.try_grow(15).unwrap();
            assert!(opener.try_grow(1).is_err());
            assert_eq!(pool.reserved(), 100);
            drop((a, b));
            opener.try_grow(85).unwrap();
            assert_eq!(pool.reserved(), 100);
            drop(opener);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[test]
    fn headroom_infallible_grow_split_take_free_drop_unregister() {
        let tracked = Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(100),
            NonZeroUsize::new(20).unwrap(),
        ));
        let pool = SpillableReservePool::wrap("test", 100, 25, tracked.clone());
        let mut a = spill(&pool);
        a.grow(120);
        assert_eq!(pool.reserved(), 120);
        assert!(a.try_grow(1).is_err());
        a.shrink(50);
        let b = a.split(20);
        let c = a.take();
        let d = a.new_empty();
        d.try_grow(5).unwrap();
        assert_eq!(pool.reserved(), 75);
        assert_eq!(b.consumer().id(), d.consumer().id());
        drop(a);
        assert_eq!(b.free(), 20);
        assert_eq!(b.free(), 0);
        drop((b, c));
        assert_eq!(pool.reserved(), 5);
        assert_eq!(tracked.metrics().len(), 1);
        drop(d);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(tracked.reserved(), 0);
        assert!(tracked.metrics().is_empty());
        assert_eq!(
            pool.downcast_ref::<SpillableReservePool>()
                .unwrap()
                .usage
                .lock()
                .spillable,
            0
        );
    }

    #[test]
    fn headroom_rolls_back_inner_rejection_and_nested_global_cap() {
        let tracked: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(50));
        let pool = SpillableReservePool::wrap("test", 100, 15, tracked.clone());
        let a = spill(&pool);
        a.try_grow(50).unwrap();
        assert!(a.try_grow(10).is_err());
        assert_eq!(pool.reserved(), 50);
        a.free();
        a.try_grow(50).unwrap();
        drop(a);
        assert_eq!(pool.reserved(), 0);
        let global =
            SpillableReservePool::wrap("global", 100, 20, Arc::new(UnboundedMemoryPool::default()));
        let local_a = SpillableReservePool::wrap("local-a", 60, 20, global.clone());
        let local_b = SpillableReservePool::wrap("local-b", 100, 20, global.clone());
        let a = spill(&local_a);
        let b = spill(&local_b);
        a.try_grow(48).unwrap();
        assert!(
            a.try_grow(1)
                .unwrap_err()
                .to_string()
                .contains("local-a spillable reserve")
        );
        b.try_grow(32).unwrap();
        assert!(
            b.try_grow(1)
                .unwrap_err()
                .to_string()
                .contains("global spillable reserve")
        );
        assert_eq!(local_b.reserved(), 32);
        let opener = MemoryConsumer::new("opener").register(&local_b);
        opener.try_grow(20).unwrap();
        assert_eq!(global.reserved(), 100);
        drop((a, b, opener));
        assert_eq!(global.reserved(), 0);
        assert_eq!(local_a.reserved(), 0);
        assert_eq!(local_b.reserved(), 0);
    }

    #[test]
    fn headroom_off_preserves_identical_pool_and_operations() {
        for inner in [
            Arc::new(GreedyMemoryPool::new(100)) as Arc<dyn MemoryPool>,
            Arc::new(FairSpillPool::new(100)),
            Arc::new(UnboundedMemoryPool::default()),
        ] {
            let pool = SpillableReservePool::wrap("unused", 1, 0, inner.clone());
            assert!(Arc::ptr_eq(&pool, &inner));
            let a = spill(&pool);
            a.try_grow(100).unwrap();
            assert_eq!(inner.reserved(), 100);
            a.grow(1);
            a.shrink(1);
            assert_eq!(a.free(), 100);
            drop(a);
            assert_eq!(inner.reserved(), 0);
        }
    }

    #[test]
    fn headroom_rounding_and_checked_overflow() {
        let pool = SpillableReservePool::wrap(
            "rounding",
            101,
            15,
            Arc::new(UnboundedMemoryPool::default()),
        );
        let a = spill(&pool);
        a.try_grow(85).unwrap();
        assert!(a.try_grow(1).is_err());
        drop(a);
        let pool = SpillableReservePool::wrap(
            "overflow",
            usize::MAX,
            50,
            Arc::new(UnboundedMemoryPool::default()),
        );
        let a = MemoryConsumer::new("large").register(&pool);
        a.try_grow(usize::MAX).unwrap();
        assert!(a.try_grow(1).unwrap_err().to_string().contains("overflow"));
        assert_eq!(pool.reserved(), usize::MAX);
        drop(a);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn headroom_concurrent_admission_has_no_overcommit_or_leak() {
        let pool = SpillableReservePool::wrap(
            "concurrent",
            100,
            25,
            Arc::new(UnboundedMemoryPool::default()),
        );
        let barrier = Arc::new(std::sync::Barrier::new(8));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let pool = pool.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    let a = spill(&pool);
                    for _ in 0..1000 {
                        barrier.wait();
                        let accepted = a.try_grow(20).is_ok();
                        barrier.wait();
                        assert!(pool.reserved() <= 75);
                        barrier.wait();
                        if accepted {
                            a.free();
                        }
                    }
                });
            }
        });
        assert_eq!(pool.reserved(), 0);
    }
}
