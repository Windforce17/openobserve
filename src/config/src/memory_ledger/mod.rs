// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Stage 0 observation only: no query, admission or cache path calls reserve.
//! Materialized capacity remains unresident until a future, audited residency
//! protocol exists. Released materialized shadows remain pending conservatively;
//! stage 1 must add a sample fence before retiring any of those shadows.

pub mod metrics;
pub mod pressure;

use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use parking_lot::{Mutex, RwLock};
use pressure::{Decision, DecisionReason, PressureSnapshot};

use crate::{
    QueryMemoryLedgerConfig, QueryMemoryLedgerMode,
    utils::sysinfo::cgroup_memory::{self, MemorySample},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum MemoryDomain {
    Cache,
    DfExec,
    OtherExec,
    Background,
    Resident,
}

impl MemoryDomain {
    pub const ALL: [Self; 5] = [
        Self::Cache,
        Self::DfExec,
        Self::OtherExec,
        Self::Background,
        Self::Resident,
    ];
    pub fn label(self) -> &'static str {
        match self {
            Self::Cache => "cache",
            Self::DfExec => "df_exec",
            Self::OtherExec => "other_exec",
            Self::Background => "background",
            Self::Resident => "resident",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum AccountingError {
    Overflow,
    Underflow,
    DoubleRelease,
    OutOfOrderSample,
}

impl AccountingError {
    pub const ALL: [Self; 4] = [
        Self::Overflow,
        Self::Underflow,
        Self::DoubleRelease,
        Self::OutOfOrderSample,
    ];
    pub fn label(self) -> &'static str {
        match self {
            Self::Overflow => "overflow",
            Self::Underflow => "underflow",
            Self::DoubleRelease => "double_release",
            Self::OutOfOrderSample => "out_of_order_sample",
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DomainBytes {
    pub reserved: u64,
    pub materialized: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct LedgerState {
    pub domains: [DomainBytes; 5],
    pub owner_records: u64,
    pub pending_sample: u64,
    pub errors: [u64; 4],
    /// reserve, materialize, release (fixed order).
    pub transitions: [u64; 3],
}

impl LedgerState {
    fn unfulfilled(&self) -> Result<u64, AccountingError> {
        self.domains.iter().try_fold(0_u64, |sum, domain| {
            sum.checked_add(domain.reserved)
                .and_then(|sum| sum.checked_add(domain.materialized))
                .ok_or(AccountingError::Overflow)
        })
    }
}

pub struct NodeMemoryLedger {
    pub epoch: uuid::Uuid,
    pub config: QueryMemoryLedgerConfig,
    state: Mutex<LedgerState>,
    pressure: RwLock<PressureSnapshot>,
    last_error_log: Mutex<[Option<Instant>; 4]>,
}

impl NodeMemoryLedger {
    pub fn new(config: QueryMemoryLedgerConfig, sample: &MemorySample) -> Self {
        Self {
            epoch: *PROCESS_EPOCH.get_or_init(uuid::Uuid::new_v4),
            pressure: RwLock::new(PressureSnapshot::from_sample(sample, &config)),
            config,
            state: Mutex::new(LedgerState::default()),
            last_error_log: Mutex::new([None; 4]),
        }
    }

    pub fn state(&self) -> LedgerState {
        *self.state.lock()
    }
    pub fn pressure(&self) -> PressureSnapshot {
        self.pressure.read().clone()
    }

    pub fn update_sample(&self, sample: &MemorySample) {
        let snapshot = PressureSnapshot::from_sample(sample, &self.config);
        let mut pressure = self.pressure.write();
        if snapshot.sampled_at < pressure.sampled_at {
            drop(pressure);
            self.record_error(AccountingError::OutOfOrderSample);
            return;
        }
        *pressure = snapshot;
    }

    fn record_error(&self, error: AccountingError) {
        let index = error as usize;
        let mut state = self.state.lock();
        // Even the error counter must never wrap. At its representational limit
        // leave it unchanged and continue reporting the error through the log.
        if let Some(count) = state.errors[index].checked_add(1) {
            state.errors[index] = count;
        }
        drop(state);
        let now = Instant::now();
        let mut last = self.last_error_log.lock();
        let emit =
            last[index].is_none_or(|last| now.duration_since(last) >= Duration::from_secs(60));
        if emit {
            last[index] = Some(now);
        }
        drop(last);
        if emit {
            log::error!(
                "Memory ledger accounting error: reason={} epoch={}",
                error.label(),
                self.epoch
            );
        }
    }

    fn transaction(
        &self,
        change: impl FnOnce(&mut LedgerState) -> Result<(), AccountingError>,
    ) -> Result<(), AccountingError> {
        let mut state = self.state.lock();
        let mut next = *state;
        let result = change(&mut next).and_then(|()| {
            next.unfulfilled()?
                .checked_add(next.pending_sample)
                .ok_or(AccountingError::Overflow)?;
            Ok(())
        });
        if result.is_ok() {
            *state = next;
        }
        drop(state);
        if let Err(error) = result {
            self.record_error(error);
        }
        result
    }

    /// Logical test seam only: this is NOT an admission permission.
    pub fn reserve(
        self: &Arc<Self>,
        domain: MemoryDomain,
        bytes: u64,
    ) -> Result<Reservation, AccountingError> {
        self.transaction(|state| {
            state.domains[domain as usize].reserved = state.domains[domain as usize]
                .reserved
                .checked_add(bytes)
                .ok_or(AccountingError::Overflow)?;
            state.owner_records = state
                .owner_records
                .checked_add(1)
                .ok_or(AccountingError::Overflow)?;
            state.transitions[0] = state.transitions[0]
                .checked_add(1)
                .ok_or(AccountingError::Overflow)?;
            Ok(())
        })?;
        Ok(Reservation {
            ledger: Arc::clone(self),
            domain,
            reserved: bytes,
            materialized: 0,
            released: false,
        })
    }

    pub fn evaluate(&self, delta: u64) -> Decision {
        self.evaluate_at(delta, Instant::now())
    }

    pub fn evaluate_at(&self, delta: u64, now: Instant) -> Decision {
        let state = self.state();
        let snapshot = self.pressure();
        let Ok(unfulfilled) = state.unfulfilled() else {
            self.record_error(AccountingError::Overflow);
            return Decision {
                would_admit: false,
                reason: DecisionReason::ArithmeticOverflow,
                projected_anon: None,
                target: snapshot.target,
            };
        };
        let decision = pressure::evaluate(
            &snapshot,
            now,
            Duration::from_millis(self.config.sample_max_age_ms),
            unfulfilled,
            state.pending_sample,
            self.config.safety_bytes,
            delta,
        );
        if decision.reason == DecisionReason::ArithmeticOverflow {
            self.record_error(AccountingError::Overflow);
        }
        decision
    }
}

/// Non-cloneable owner skeleton. Drop does not panic, wait, collect or call caches.
/// Materialization moves capacity, never independently duplicates ownership.
pub struct Reservation {
    ledger: Arc<NodeMemoryLedger>,
    domain: MemoryDomain,
    reserved: u64,
    materialized: u64,
    released: bool,
}

impl Reservation {
    pub fn materialize(&mut self, bytes: u64) -> Result<(), AccountingError> {
        if self.released {
            self.ledger.record_error(AccountingError::DoubleRelease);
            return Err(AccountingError::DoubleRelease);
        }
        let result = self
            .reserved
            .checked_sub(bytes)
            .zip(self.materialized.checked_add(bytes));
        let Some((reserved, materialized)) = result else {
            self.ledger.record_error(AccountingError::Underflow);
            return Err(AccountingError::Underflow);
        };
        self.ledger.transaction(|state| {
            let domain = &mut state.domains[self.domain as usize];
            domain.reserved = domain
                .reserved
                .checked_sub(bytes)
                .ok_or(AccountingError::Underflow)?;
            domain.materialized = domain
                .materialized
                .checked_add(bytes)
                .ok_or(AccountingError::Overflow)?;
            state.transitions[1] = state.transitions[1]
                .checked_add(1)
                .ok_or(AccountingError::Overflow)?;
            Ok(())
        })?;
        self.reserved = reserved;
        self.materialized = materialized;
        Ok(())
    }

    pub fn release(&mut self) -> Result<(), AccountingError> {
        if self.released {
            self.ledger.record_error(AccountingError::DoubleRelease);
            return Err(AccountingError::DoubleRelease);
        }
        self.ledger.transaction(|state| {
            let domain = &mut state.domains[self.domain as usize];
            domain.reserved = domain
                .reserved
                .checked_sub(self.reserved)
                .ok_or(AccountingError::Underflow)?;
            domain.materialized = domain
                .materialized
                .checked_sub(self.materialized)
                .ok_or(AccountingError::Underflow)?;
            state.pending_sample = state
                .pending_sample
                .checked_add(self.materialized)
                .ok_or(AccountingError::Overflow)?;
            state.owner_records = state
                .owner_records
                .checked_sub(1)
                .ok_or(AccountingError::Underflow)?;
            state.transitions[2] = state.transitions[2]
                .checked_add(1)
                .ok_or(AccountingError::Overflow)?;
            Ok(())
        })?;
        self.released = true;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.release();
        }
    }
}

static PROCESS_EPOCH: OnceLock<uuid::Uuid> = OnceLock::new();

static LEDGER: OnceLock<Option<Arc<NodeMemoryLedger>>> = OnceLock::new();

pub fn global() -> Option<&'static Arc<NodeMemoryLedger>> {
    LEDGER.get().and_then(Option::as_ref)
}

/// The closure is never invoked in off mode, including at startup.
fn initialize_with(
    config: QueryMemoryLedgerConfig,
    sample: impl FnOnce() -> MemorySample,
) -> Option<Arc<NodeMemoryLedger>> {
    if config.mode == QueryMemoryLedgerMode::Off {
        return None;
    }
    Some(Arc::new(NodeMemoryLedger::new(config, &sample())))
}

pub fn init(config: QueryMemoryLedgerConfig) -> anyhow::Result<()> {
    config.validate()?;
    let process_epoch = *PROCESS_EPOCH.get_or_init(uuid::Uuid::new_v4);
    let mode = config.mode;
    let configured_target = config.anon_target_bytes;
    let ledger = initialize_with(config, cgroup_memory::read_observed);
    let (epoch, target, source) = match &ledger {
        Some(ledger) => (
            Some(ledger.epoch),
            ledger.pressure().target,
            ledger.config.target(ledger.pressure().limit).1,
        ),
        None => (
            Some(process_epoch),
            (configured_target != 0).then_some(configured_target),
            "off_no_sampling",
        ),
    };
    LEDGER.set(ledger).map_err(|_| {
        anyhow::anyhow!("Memory ledger startup configuration was already initialized")
    })?;
    log::info!(
        "Memory ledger: mode={mode:?} epoch={epoch:?} anon_target_bytes={target:?} source={source}; stage=0, business ownership uninstrumented, no enforcement"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::sysinfo::cgroup_memory::{MemoryObservation, MemoryUsage};

    pub(super) fn ledger() -> Arc<NodeMemoryLedger> {
        let cfg = QueryMemoryLedgerConfig {
            mode: QueryMemoryLedgerMode::Observe,
            anon_target_bytes: 100,
            safety_bytes: 1,
            sample_interval_ms: 200,
            sample_max_age_ms: 1000,
        };
        let sample = MemorySample {
            started_at: Instant::now(),
            usage: Some(MemoryUsage {
                current: 10,
                working_set: None,
                stat: vec![("anon", 10)],
                events: vec![],
            }),
            observation: MemoryObservation::default(),
        };
        Arc::new(NodeMemoryLedger::new(cfg, &sample))
    }

    #[test]
    fn off_does_not_call_sampler_or_create_ledger() {
        let cfg = QueryMemoryLedgerConfig::from_lookup(|_| Ok(None)).unwrap();
        assert!(initialize_with(cfg, || panic!("off must not sample")).is_none());
        let registry = prometheus::Registry::new();
        crate::metrics::register_memory_ledger_metrics(&registry).unwrap();
        assert!(registry.gather().is_empty());
        assert!(std::sync::LazyLock::get(&metrics::METRICS).is_none());
    }

    #[test]
    fn checked_reservation_materialization_and_double_release() {
        let ledger = ledger();
        let mut owner = ledger.reserve(MemoryDomain::Cache, 20).unwrap();
        owner.materialize(5).unwrap();
        assert_eq!(
            ledger.state().domains[0],
            DomainBytes {
                reserved: 15,
                materialized: 5
            }
        );
        assert_eq!(ledger.evaluate(0).projected_anon, Some(31));
        assert_eq!(owner.materialize(16), Err(AccountingError::Underflow));
        assert_eq!(
            ledger.state().errors[AccountingError::Underflow as usize],
            1
        );
        owner.release().unwrap();
        assert_eq!(ledger.state().domains[0], DomainBytes::default());
        assert_eq!(ledger.state().pending_sample, 5);
        assert_eq!(ledger.evaluate(0).projected_anon, Some(16));
        assert_eq!(owner.release(), Err(AccountingError::DoubleRelease));
        drop(owner);
        assert_eq!(
            ledger.state().errors[AccountingError::DoubleRelease as usize],
            1
        );
        assert_eq!(ledger.state().owner_records, 0);
        assert_eq!(ledger.state().transitions, [1, 1, 1]);
    }

    #[test]
    fn overflow_rolls_back_across_domains_and_records_error() {
        let ledger = ledger();
        let _owner = ledger.reserve(MemoryDomain::DfExec, u64::MAX).unwrap();
        assert!(matches!(
            ledger.reserve(MemoryDomain::Cache, 1),
            Err(AccountingError::Overflow)
        ));
        assert_eq!(ledger.state().domains[0].reserved, 0);
        assert_eq!(ledger.state().owner_records, 1);
        assert_eq!(ledger.state().errors[0], 1);
        assert_eq!(
            ledger.evaluate(0).reason,
            DecisionReason::ArithmeticOverflow
        );
        assert_eq!(ledger.state().errors[0], 2);
    }

    #[test]
    fn drop_and_concurrent_owners_return_unused_reservations() {
        let ledger = ledger();
        std::thread::scope(|scope| {
            for domain in MemoryDomain::ALL {
                let ledger = Arc::clone(&ledger);
                scope.spawn(move || {
                    for _ in 0..100 {
                        let _owner = ledger.reserve(domain, 1).unwrap();
                    }
                });
            }
        });
        assert_eq!(ledger.state().domains, [DomainBytes::default(); 5]);
        assert_eq!(ledger.state().owner_records, 0);
        assert_eq!(ledger.state().errors, [0; 4]);
    }

    #[test]
    fn older_sample_cannot_replace_newer_snapshot() {
        let ledger = ledger();
        let before = ledger.pressure();
        ledger.update_sample(&MemorySample {
            started_at: before.sampled_at - Duration::from_secs(1),
            usage: None,
            observation: MemoryObservation::default(),
        });
        assert_eq!(ledger.pressure().anon, before.anon);
        assert_eq!(ledger.state().errors[3], 1);
    }
}
