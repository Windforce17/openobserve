// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    collections::HashMap,
    sync::LazyLock,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use prometheus::{GaugeVec, IntCounterVec, Opts, Registry, core::Collector};

use super::{AccountingError, LedgerState, MemoryDomain, NodeMemoryLedger};
use crate::utils::sysinfo::cgroup_memory::MemoryLimit;

pub static METRICS: LazyLock<LedgerMetrics> =
    LazyLock::new(|| LedgerMetrics::new(crate::metrics::create_const_labels()));

pub struct LedgerMetrics {
    anon: GaugeVec,
    target: GaugeVec,
    projected: GaugeVec,
    headroom: GaugeVec,
    age: GaugeVec,
    valid: GaugeVec,
    owners: GaugeVec,
    records: GaugeVec,
    psi: GaugeVec,
    swap: GaugeVec,
    limits: GaugeVec,
    availability: GaugeVec,
    legacy: GaugeVec,
    transitions: IntCounterVec,
    errors: IntCounterVec,
    previous: Mutex<LedgerState>,
}

impl LedgerMetrics {
    pub fn new(labels: HashMap<String, String>) -> Self {
        let gauge = |name: &str, help: &str, dimensions: &[&str]| {
            GaugeVec::new(
                Opts::new(format!("zo_memory_ledger_{name}"), help).const_labels(labels.clone()),
                dimensions,
            )
            .expect("Static ledger metric schema")
        };
        let counter = |name: &str, help: &str, dimension| {
            IntCounterVec::new(
                Opts::new(format!("zo_memory_ledger_{name}"), help).const_labels(labels.clone()),
                &[dimension],
            )
            .expect("Static ledger metric schema")
        };
        Self {
            anon: gauge(
                "anon_bytes",
                "Last fresh cgroup anonymous usage; missing samples are omitted",
                &[],
            ),
            target: gauge(
                "anon_target_bytes",
                "Observation target capped by visible cgroup constraints, not a guarantee",
                &[],
            ),
            projected: gauge(
                "projected_anon_bytes",
                "Observed anon + unfulfilled + pending sample + safety, delta=0",
                &[],
            ),
            headroom: gauge(
                "headroom_bytes",
                "Signed target minus projected anon; negative means over target",
                &[],
            ),
            age: gauge(
                "sample_age_seconds",
                "Age since cgroup read start, using a monotonic clock",
                &[],
            ),
            valid: gauge(
                "sample_valid",
                "1 for fresh anon and usable target, 0 otherwise; does not certify optional telemetry or ownership coverage",
                &[],
            ),
            owners: gauge(
                "owner_bytes",
                "Instrumented unique logical owner bytes only; stage 0 has no business owners connected",
                &["domain", "state"],
            ),
            records: gauge(
                "owner_records",
                "Live instrumented owner handles; stage 0 does not cover business allocations",
                &[],
            ),
            psi: gauge(
                "psi",
                "Cgroup PSI averages in percent or cumulative stall seconds, distinguished by fixed kind",
                &["kind"],
            ),
            swap: gauge(
                "swap_bytes",
                "Observed cgroup swap current or finite max, missing and unlimited omitted",
                &["kind"],
            ),
            limits: gauge(
                "limit_bytes",
                "Finite local max/high or visible ancestor effective limit; missing and unlimited omitted",
                &["kind"],
            ),
            availability: gauge(
                "telemetry_available",
                "1 readable, 0 missing; unlimited is readable; ancestor_scope_known=0 means hidden ancestors may exist",
                &["kind"],
            ),
            legacy: gauge(
                "legacy_bytes",
                "Independent legacy accounting, not unique ownership; MUST NOT be added to owner_bytes",
                &["source"],
            ),
            transitions: counter(
                "transition_total",
                "Successful skeleton owner transitions",
                "transition",
            ),
            errors: counter(
                "accounting_errors_total",
                "Checked accounting failures; failed transactions do not change owner counts",
                "reason",
            ),
            previous: Mutex::new(LedgerState::default()),
        }
    }

    pub fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        for metric in [
            &self.anon,
            &self.target,
            &self.projected,
            &self.headroom,
            &self.age,
            &self.valid,
            &self.owners,
            &self.records,
            &self.psi,
            &self.swap,
            &self.limits,
            &self.availability,
            &self.legacy,
        ] {
            registry.register(Box::new(metric.clone()))?;
        }
        registry.register(Box::new(self.transitions.clone()))?;
        registry.register(Box::new(self.errors.clone()))?;
        Ok(())
    }

    pub fn publish(&self, ledger: &NodeMemoryLedger, now: Instant) {
        let snapshot = ledger.pressure();
        let age = snapshot.age(now);
        let fresh =
            age.is_some_and(|age| age <= Duration::from_millis(ledger.config.sample_max_age_ms));
        let valid = snapshot.is_valid(now, Duration::from_millis(ledger.config.sample_max_age_ms));
        set(&self.age, &[], age.map(|age| age.as_secs_f64()));
        set(&self.valid, &[], Some(u8::from(valid) as f64));
        set(
            &self.anon,
            &[],
            snapshot.anon.filter(|_| fresh).map(|v| v as f64),
        );
        set(
            &self.target,
            &[],
            snapshot.target.filter(|_| fresh).map(|v| v as f64),
        );
        let decision = ledger.evaluate_at(0, now);
        set(
            &self.projected,
            &[],
            decision.projected_anon.map(|v| v as f64),
        );
        set(
            &self.headroom,
            &[],
            decision
                .target
                .zip(decision.projected_anon)
                .and_then(|(target, projected)| {
                    target
                        .checked_sub(projected)
                        .map(|bytes| bytes as f64)
                        .or_else(|| projected.checked_sub(target).map(|bytes| -(bytes as f64)))
                }),
        );
        let observation = &snapshot.observation;
        for (kind, value) in [("max", observation.max), ("high", observation.high)] {
            set(
                &self.limits,
                &[kind],
                value
                    .and_then(MemoryLimit::bytes)
                    .filter(|_| fresh)
                    .map(|v| v as f64),
            );
            set(
                &self.availability,
                &[kind],
                Some(u8::from(fresh && value.is_some()) as f64),
            );
        }
        set(
            &self.limits,
            &["ancestor_effective"],
            observation
                .ancestor_limit
                .filter(|_| fresh)
                .map(|v| v as f64),
        );
        set(
            &self.limits,
            &["effective"],
            snapshot.limit.filter(|_| fresh).map(|v| v as f64),
        );
        set(
            &self.availability,
            &["ancestor_scope_known"],
            Some(u8::from(!observation.ancestor_scope_unknown && fresh) as f64),
        );
        for (prefix, psi) in [("some", observation.some), ("full", observation.full)] {
            let kinds = if prefix == "some" {
                ["some_avg10", "some_avg60", "some_total_seconds"]
            } else {
                ["full_avg10", "full_avg60", "full_total_seconds"]
            };
            for (kind, value) in kinds.into_iter().zip([
                psi.map(|p| p.avg10),
                psi.map(|p| p.avg60),
                psi.map(|p| p.total_usec as f64 / 1_000_000.0),
            ]) {
                set(&self.psi, &[kind], value.filter(|_| fresh));
            }
            set(
                &self.availability,
                &[prefix],
                Some(u8::from(fresh && psi.is_some()) as f64),
            );
        }
        set(
            &self.swap,
            &["current"],
            observation.swap_current.filter(|_| fresh).map(|v| v as f64),
        );
        set(
            &self.swap,
            &["max"],
            observation
                .swap_max
                .and_then(MemoryLimit::bytes)
                .filter(|_| fresh)
                .map(|v| v as f64),
        );
        for (kind, available) in [
            ("swap_current", observation.swap_current.is_some()),
            ("swap_max", observation.swap_max.is_some()),
        ] {
            set(
                &self.availability,
                &[kind],
                Some(u8::from(fresh && available) as f64),
            );
        }
        let state = ledger.state();
        for domain in MemoryDomain::ALL {
            let bytes = state.domains[domain as usize];
            set(
                &self.owners,
                &[domain.label(), "reserved"],
                Some(bytes.reserved as f64),
            );
            set(
                &self.owners,
                &[domain.label(), "materialized"],
                Some(bytes.materialized as f64),
            );
        }
        set(&self.records, &[], Some(state.owner_records as f64));
        let mut previous = self.previous.lock();
        for (index, transition) in ["reserve", "materialize", "release"]
            .into_iter()
            .enumerate()
        {
            if let Some(delta) = state.transitions[index].checked_sub(previous.transitions[index]) {
                self.transitions
                    .with_label_values(&[transition])
                    .inc_by(delta);
            }
        }
        for reason in AccountingError::ALL {
            let index = reason as usize;
            if let Some(delta) = state.errors[index].checked_sub(previous.errors[index]) {
                self.errors
                    .with_label_values(&[reason.label()])
                    .inc_by(delta);
            }
        }
        *previous = state;
    }

    pub fn publish_file_stat(&self, bytes: Option<u64>) {
        set(
            &self.legacy,
            &["file_stat"],
            bytes.map(|bytes| bytes as f64),
        );
    }

    /// Collect existing time-series without creating missing legacy children.
    /// These old gauges overlap and are never summed into the owner ledger.
    pub fn publish_legacy(&self) {
        use crate::metrics as old;
        let sources: [(&str, &dyn Collector); 6] = [
            (
                "df_shared_reserved",
                &*old::DATAFUSION_SHARED_RESERVED_BYTES,
            ),
            ("vix_eval", &*old::VIX_EVAL_MEMORY_BYTES),
            ("reader_cache", &*old::VIX_READER_CACHE_MEMORY_BYTES),
            ("vix_result_cache", &*old::VIX_RESULT_CACHE_MEMORY_USAGE),
            ("file_data_cache", &*old::QUERY_MEMORY_CACHE_USED_BYTES),
            ("bloom_footer", &*old::BLOOM_FOOTER_CACHE_BYTES),
        ];
        for (source, collector) in sources {
            let values: Vec<f64> = collector
                .collect()
                .iter()
                .flat_map(|family| family.get_metric())
                .filter(|metric| metric.gauge.is_some())
                .map(|metric| metric.get_gauge().value())
                .collect();
            set(
                &self.legacy,
                &[source],
                (!values.is_empty()).then(|| values.iter().sum()),
            );
        }
    }
}

fn set(metric: &GaugeVec, labels: &[&str], value: Option<f64>) {
    if let Some(value) = value {
        metric.with_label_values(labels).set(value);
    } else {
        let _ = metric.remove_label_values(labels);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_and_expired_samples_remove_values_and_set_valid_zero() {
        let ledger = super::super::tests::ledger();
        let metrics = LedgerMetrics::new(HashMap::new());
        let registry = Registry::new();
        metrics.register(&registry).unwrap();
        let now = ledger.pressure().sampled_at;
        metrics.publish(&ledger, now);
        assert_eq!(metrics.valid.with_label_values::<&str>(&[]).get(), 1.0);
        assert!(
            registry
                .gather()
                .iter()
                .any(|family| family.name() == "zo_memory_ledger_anon_bytes")
        );
        assert!(
            !registry
                .gather()
                .iter()
                .any(|family| family.name() == "zo_memory_ledger_swap_bytes")
        );
        metrics.publish(&ledger, now + Duration::from_secs(2));
        assert_eq!(metrics.valid.with_label_values::<&str>(&[]).get(), 0.0);
        assert!(
            !registry
                .gather()
                .iter()
                .any(|family| family.name() == "zo_memory_ledger_anon_bytes")
        );
        assert!(
            !registry
                .gather()
                .iter()
                .any(|family| family.name() == "zo_memory_ledger_projected_anon_bytes")
        );
    }

    #[test]
    fn fixed_labels_and_counter_deltas_are_not_double_counted() {
        let ledger = super::super::tests::ledger();
        let metrics = LedgerMetrics::new(HashMap::new());
        let mut owner = ledger.reserve(MemoryDomain::Cache, 1).unwrap();
        owner.release().unwrap();
        assert!(owner.release().is_err());
        metrics.publish(&ledger, Instant::now());
        metrics.publish(&ledger, Instant::now());
        assert_eq!(
            metrics.errors.with_label_values(&["double_release"]).get(),
            1
        );
        assert_eq!(metrics.transitions.with_label_values(&["reserve"]).get(), 1);
        assert_eq!(metrics.owners.collect()[0].get_metric().len(), 10);
    }
}
