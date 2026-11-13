// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::time::{Duration, Instant};

use crate::{
    QueryMemoryLedgerConfig,
    utils::sysinfo::cgroup_memory::{MemoryObservation, MemorySample},
};

#[derive(Debug, Clone)]
pub struct PressureSnapshot {
    pub sampled_at: Instant,
    pub anon: Option<u64>,
    pub file: Option<u64>,
    pub limit: Option<u64>,
    pub observation: MemoryObservation,
    /// True only for a readable anon and a finite, usable target at sample time.
    /// Optional PSI/swap/ancestor visibility are independent, not implied by this bit.
    pub valid: bool,
    pub target: Option<u64>,
}

impl PressureSnapshot {
    pub fn from_sample(sample: &MemorySample, cfg: &QueryMemoryLedgerConfig) -> Self {
        let stat = |key| {
            sample.usage.as_ref().and_then(|usage| {
                usage
                    .stat
                    .iter()
                    .find(|(name, _)| *name == key)
                    .map(|(_, value)| *value)
            })
        };
        let limit = sample.observation.effective_limit();
        let target = cfg.target(limit).0;
        let anon = stat("anon");
        Self {
            sampled_at: sample.started_at,
            anon,
            file: stat("file"),
            limit,
            observation: sample.observation.clone(),
            valid: anon.is_some() && target.is_some_and(|target| target > cfg.safety_bytes),
            target,
        }
    }

    pub fn age(&self, now: Instant) -> Option<Duration> {
        now.checked_duration_since(self.sampled_at)
    }

    pub fn is_valid(&self, now: Instant, max_age: Duration) -> bool {
        self.valid && self.age(now).is_some_and(|age| age <= max_age)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionReason {
    WithinTarget,
    TargetExceeded,
    TelemetryUnavailable,
    ArithmeticOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decision {
    pub would_admit: bool,
    pub reason: DecisionReason,
    pub projected_anon: Option<u64>,
    /// Signed in the exporter, but kept as two unsigned components internally.
    pub target: Option<u64>,
}

/// Observation only. Materialized bytes are deliberately NOT added to anon;
/// their unresident subset is included in unfulfilled, and frees never subtract anon.
pub fn evaluate(
    snapshot: &PressureSnapshot,
    now: Instant,
    max_age: Duration,
    unfulfilled: u64,
    pending_sample: u64,
    safety: u64,
    delta: u64,
) -> Decision {
    let blocked = |reason| Decision {
        would_admit: false,
        reason,
        projected_anon: None,
        target: snapshot.target,
    };
    if !snapshot.is_valid(now, max_age) {
        return blocked(DecisionReason::TelemetryUnavailable);
    }
    let projected = snapshot
        .anon
        .and_then(|anon| anon.checked_add(unfulfilled))
        .and_then(|bytes| bytes.checked_add(pending_sample))
        .and_then(|bytes| bytes.checked_add(safety))
        .and_then(|bytes| bytes.checked_add(delta));
    let Some(projected) = projected else {
        return blocked(DecisionReason::ArithmeticOverflow);
    };
    let would_admit = snapshot.target.is_some_and(|target| projected <= target);
    Decision {
        would_admit,
        reason: if would_admit {
            DecisionReason::WithinTarget
        } else {
            DecisionReason::TargetExceeded
        },
        projected_anon: Some(projected),
        target: snapshot.target,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn snapshot(now: Instant) -> PressureSnapshot {
        PressureSnapshot {
            sampled_at: now,
            anon: Some(10),
            file: Some(99),
            limit: Some(40),
            observation: MemoryObservation::default(),
            valid: true,
            target: Some(30),
        }
    }

    #[test]
    fn physical_formula_boundary_and_all_terms() {
        let now = Instant::now();
        let snapshot = snapshot(now);
        let decision = evaluate(&snapshot, now, Duration::from_secs(1), 5, 3, 2, 10);
        assert!(decision.would_admit);
        assert_eq!(decision.projected_anon, Some(30));
        assert_eq!(
            evaluate(&snapshot, now, Duration::from_secs(1), 5, 3, 2, 11).reason,
            DecisionReason::TargetExceeded
        );
        assert_eq!(
            evaluate(&snapshot, now, Duration::from_secs(1), u64::MAX, 0, 1, 0).reason,
            DecisionReason::ArithmeticOverflow
        );
        assert_eq!(
            evaluate(&snapshot, now, Duration::from_secs(1), 0, 0, 1, u64::MAX).reason,
            DecisionReason::ArithmeticOverflow
        );
    }

    #[test]
    fn stale_missing_and_future_samples_are_invalid() {
        let now = Instant::now();
        let mut snapshot = snapshot(now);
        let max_age = Duration::from_millis(1000);
        assert!(snapshot.is_valid(now + max_age, max_age));
        assert!(!snapshot.is_valid(now + max_age + Duration::from_nanos(1), max_age));
        assert_eq!(
            evaluate(
                &snapshot,
                now + max_age + Duration::from_nanos(1),
                max_age,
                0,
                0,
                1,
                0
            )
            .reason,
            DecisionReason::TelemetryUnavailable
        );
        assert!(!snapshot.is_valid(now - Duration::from_millis(1), max_age));
        snapshot.valid = false;
        assert!(!evaluate(&snapshot, now, max_age, 0, 0, 1, 0).would_admit);
    }
}
