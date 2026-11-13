// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only
//! Optional owner-thread collection after a complete VIX evaluation closure.
//!
//! The binary registers its allocator callback; this module never selects an
//! allocator for library users. Configuration is startup-only and defaults off.
//! ZO_VIX_EVAL_COLLECT_MODE=off|false|true, ZO_VIX_EVAL_COLLECT_INTERVAL_MS=1000.
//! Set interval=0 to collect after every task. Collection cannot reclaim escaped
//! results/cache owners or cross-thread frees that happen after this boundary.

use std::{
    cell::Cell,
    sync::OnceLock,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Off,
    Normal,
    Force,
}

#[derive(Clone, Copy, Debug)]
pub struct Settings {
    pub mode: Mode,
    pub interval: Duration,
}

impl Settings {
    pub fn parse(mode: &str, interval_ms: &str) -> Result<Self, String> {
        let mode = match mode {
            "off" => Mode::Off,
            "false" => Mode::Normal,
            "true" => Mode::Force,
            _ => return Err("ZO_VIX_EVAL_COLLECT_MODE must be off, false, or true".into()),
        };
        let ms: u64 = interval_ms
            .parse()
            .map_err(|_| "invalid ZO_VIX_EVAL_COLLECT_INTERVAL_MS")?;
        Ok(Self {
            mode,
            interval: Duration::from_millis(ms),
        })
    }
    pub fn from_env() -> Result<Self, String> {
        Self::parse(
            &crate::allocator_reclaim::startup_env("ZO_VIX_EVAL_COLLECT_MODE", "off")?,
            &crate::allocator_reclaim::startup_env("ZO_VIX_EVAL_COLLECT_INTERVAL_MS", "1000")?,
        )
    }
}

struct Policy {
    settings: Settings,
    collect: fn(bool),
}
static POLICY: OnceLock<Policy> = OnceLock::new();
thread_local! { static LAST: Cell<Option<Instant>> = const { Cell::new(None) }; }

/// Initialize once after the binary registers its allocator, if any.
pub fn initialize(settings: Settings) -> Result<(), &'static str> {
    let Some(collect) = crate::allocator_reclaim::collector() else {
        return Ok(());
    };
    POLICY
        .set(Policy { settings, collect })
        .map_err(|_| "VIX reclaim already initialized")
}

struct Guard<'a> {
    policy: Option<&'a Policy>,
}
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let Some(policy) = self.policy else {
            return;
        };
        if policy.settings.mode == Mode::Off {
            return;
        }
        // Set before calling the callback; no RefCell borrow or heap allocation
        // is held across allocator code. TLS destruction must never panic.
        let due = LAST
            .try_with(|last| {
                let now = Instant::now();
                if last
                    .get()
                    .is_some_and(|t| now.saturating_duration_since(t) < policy.settings.interval)
                {
                    false
                } else {
                    last.set(Some(now));
                    true
                }
            })
            .unwrap_or(false);
        if due {
            (policy.collect)(policy.settings.mode == Mode::Force);
        }
    }
}

/// The entire work closure (including its captures/permits/operation scopes)
/// must finish/drop before collection, on success, error, or unwind. A returned
/// value remains live. Queued tasks that never run do not collect on a waiter.
pub fn run<T>(work: impl FnOnce() -> T) -> T {
    run_with(POLICY.get(), work)
}
fn run_with<T>(policy: Option<&Policy>, work: impl FnOnce() -> T) -> T {
    let guard = Guard { policy };
    let result = work();
    drop(guard);
    result
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        panic::{AssertUnwindSafe, catch_unwind},
    };

    use super::*;
    thread_local! { static EVENTS: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) }; }
    struct Owner(&'static str);
    impl Drop for Owner {
        fn drop(&mut self) {
            EVENTS.with(|e| e.borrow_mut().push(self.0));
        }
    }
    fn collect(force: bool) {
        EVENTS.with(|e| e.borrow_mut().push(if force { "force" } else { "collect" }));
    }
    fn reset() {
        LAST.with(|v| v.set(None));
        EVENTS.with(|e| e.borrow_mut().clear());
    }
    fn events() -> Vec<&'static str> {
        EVENTS.with(|e| e.borrow().clone())
    }
    fn policy(mode: Mode, ms: u64) -> Policy {
        Policy {
            settings: Settings {
                mode,
                interval: Duration::from_millis(ms),
            },
            collect,
        }
    }
    #[test]
    fn eval_reclaim_capture_drops_before_collect_result_after() {
        reset();
        let p = policy(Mode::Force, 0);
        let owner = Owner("capture");
        let tid = std::thread::current().id();
        let result = run_with(Some(&p), move || {
            assert_eq!(tid, std::thread::current().id());
            drop(owner);
            Owner("result")
        });
        assert_eq!(events(), vec!["capture", "force"]);
        drop(result);
        assert_eq!(events(), vec!["capture", "force", "result"]);
    }
    #[test]
    fn eval_reclaim_error_and_panic_release_before_collect() {
        reset();
        let p = policy(Mode::Normal, 0);
        let owner = Owner("error_owner");
        let r: Result<(), ()> = run_with(Some(&p), move || {
            drop(owner);
            Err(())
        });
        assert!(r.is_err());
        assert_eq!(events(), vec!["error_owner", "collect"]);
        reset();
        let owner = Owner("panic_owner");
        let r = catch_unwind(AssertUnwindSafe(|| {
            run_with(Some(&p), move || {
                let _owner = owner;
                panic!("expected");
            })
        }));
        assert!(r.is_err());
        assert_eq!(events(), vec!["panic_owner", "collect"]);
    }
    #[test]
    fn eval_reclaim_disabled_and_interval_are_preserved() {
        reset();
        let p = policy(Mode::Off, 0);
        run_with(Some(&p), || ());
        run_with(None, || ());
        assert!(events().is_empty());
        let p = policy(Mode::Normal, 1000);
        run_with(Some(&p), || ());
        run_with(Some(&p), || ());
        assert_eq!(events(), vec!["collect"]);
        LAST.with(|v| v.set(Some(Instant::now() - Duration::from_secs(2))));
        run_with(Some(&p), || ());
        assert_eq!(events(), vec!["collect", "collect"]);
    }
    #[test]
    fn eval_reclaim_is_per_os_thread() {
        reset();
        let p = policy(Mode::Normal, 1000);
        run_with(Some(&p), || ());
        std::thread::spawn(|| {
            reset();
            let p = policy(Mode::Normal, 1000);
            run_with(Some(&p), || ());
            assert_eq!(events(), vec!["collect"]);
        })
        .join()
        .unwrap();
        run_with(Some(&p), || ());
        assert_eq!(events(), vec!["collect"]);
    }
    #[test]
    fn eval_reclaim_configuration_rejects_mistakes() {
        assert_eq!(Settings::parse("off", "1000").unwrap().mode, Mode::Off);
        assert_eq!(
            Settings::parse("true", "0").unwrap().interval,
            Duration::ZERO
        );
        assert!(Settings::parse("yes", "1000").is_err());
        assert!(Settings::parse("false", "-1").is_err());
    }
}
