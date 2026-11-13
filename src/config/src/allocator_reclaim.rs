// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only
//! Startup-only owner-thread collection on Tokio park. Zero disables collection.
//! This is an opportunity-based hook, not a timer: sleeping threads are not woken.

use std::{
    cell::Cell,
    sync::OnceLock,
    time::{Duration, Instant},
};

const INTERVAL_ENV: &str = "ZO_ALLOCATOR_PARK_COLLECT_INTERVAL_MS";
// An hour is already far beyond useful idle reclamation. Reject unit mistakes
// rather than silently retaining pages for days (or overflowing conversions).
const MAX_INTERVAL_MS: u64 = 3_600_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Settings {
    interval: Duration,
}

impl Settings {
    pub fn parse(value: &str) -> Result<Self, String> {
        let ms = value
            .parse::<u64>()
            .ok()
            .filter(|ms| *ms <= MAX_INTERVAL_MS)
            .ok_or_else(|| format!("{INTERVAL_ENV} must be an integer in 0..={MAX_INTERVAL_MS}"))?;
        Ok(Self {
            interval: Duration::from_millis(ms),
        })
    }

    pub fn from_env() -> Result<Self, String> {
        Self::parse(&startup_env(INTERVAL_ENV, "0")?)
    }

    pub fn enabled(self) -> bool {
        !self.interval.is_zero()
    }
}

/// Missing is a default, but non-Unicode values are configuration errors.
pub(crate) fn startup_env(name: &str, default: &str) -> Result<String, String> {
    match std::env::var(name) {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{name} must be valid Unicode")),
    }
}

static COLLECT: OnceLock<fn(bool)> = OnceLock::new();
static POLICY: OnceLock<Settings> = OnceLock::new();
thread_local! { static LAST: Cell<Option<Instant>> = const { Cell::new(None) }; }

/// Register the allocator exactly once, independently of the two policies.
/// The callback must not panic, allocate Rust objects, or re-enter collection.
/// Only the binary selecting the allocator should call this function.
pub fn register_collector(collect: fn(bool)) -> Result<(), &'static str> {
    COLLECT
        .set(collect)
        .map_err(|_| "allocator collector already registered")
}

pub(crate) fn collector() -> Option<fn(bool)> {
    COLLECT.get().copied()
}

pub fn initialize(settings: Settings) -> Result<(), &'static str> {
    POLICY
        .set(settings)
        .map_err(|_| "allocator park policy already initialized")
}

/// No locks, allocation, panicking TLS access, or waiting on initialization.
/// Record the timestamp before invoking allocator code and outside TLS access.
pub fn on_thread_park() {
    park_with(POLICY.get().copied(), collector());
}

fn park_with(settings: Option<Settings>, collect: Option<fn(bool)>) {
    let Some(settings) = settings.filter(|settings| settings.enabled()) else {
        return;
    };
    let Some(collect) = collect else { return };
    let due = LAST
        .try_with(|last| {
            let now = Instant::now();
            if last
                .get()
                .is_some_and(|at| now.saturating_duration_since(at) < settings.interval)
            {
                false
            } else {
                last.set(Some(now));
                true
            }
        })
        .unwrap_or(false);
    if due {
        collect(false);
    }
}

/// The main runtime predates .env/CLI parsing, so it needs a dormant hook.
/// After initialization, disabled builders get no hook at all. No env is read
/// here: runtime construction must not prematurely load startup configuration.
pub fn configure_runtime(mut builder: tokio::runtime::Builder) -> tokio::runtime::Builder {
    if POLICY.get().is_none_or(|settings| settings.enabled()) {
        builder.on_thread_park(on_thread_park);
    }
    builder
}

#[cfg(test)]
mod tests {
    use super::*;
    thread_local! { static CALLS: Cell<u64> = const { Cell::new(0) }; }

    fn collect(force: bool) {
        assert!(!force, "park must never force collection");
        CALLS.with(|calls| calls.set(calls.get().checked_add(1).unwrap()));
    }

    fn reset() {
        LAST.with(|last| last.set(None));
        CALLS.with(|calls| calls.set(0));
    }

    #[test]
    fn park_disabled_uninitialized_and_no_allocator_are_noops() {
        reset();
        let enabled = Settings::parse("1000").unwrap();
        park_with(None, Some(collect));
        park_with(Some(Settings::parse("0").unwrap()), Some(collect));
        park_with(Some(enabled), None);
        assert_eq!(CALLS.with(Cell::get), 0);
        assert!(LAST.with(Cell::get).is_none());
    }

    #[test]
    fn park_throttles_on_owner_thread_and_expires() {
        reset();
        let settings = Some(Settings::parse("1000").unwrap());
        park_with(settings, Some(collect));
        park_with(settings, Some(collect));
        assert_eq!(CALLS.with(Cell::get), 1);
        LAST.with(|last| last.set(Instant::now().checked_sub(Duration::from_secs(2))));
        park_with(settings, Some(collect));
        assert_eq!(CALLS.with(Cell::get), 2);
        std::thread::spawn(move || {
            park_with(settings, Some(collect));
            assert_eq!(CALLS.with(Cell::get), 1);
        })
        .join()
        .unwrap();
        park_with(settings, Some(collect));
        assert_eq!(CALLS.with(Cell::get), 2);
    }

    #[test]
    fn park_configuration_rejects_invalid_and_overflow_values() {
        assert!(!Settings::parse("0").unwrap().enabled());
        assert!(Settings::parse("1000").unwrap().enabled());
        assert!(Settings::parse("3600000").is_ok());
        for invalid in ["", "false", "-1", "1.5", "3600001", "18446744073709551616"] {
            assert!(Settings::parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn park_runtime_startup_and_throttle() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        static COLLECTS: AtomicUsize = AtomicUsize::new(0);
        static EXISTING_HOOK: AtomicUsize = AtomicUsize::new(0);
        static TOO_FAST: AtomicBool = AtomicBool::new(false);
        thread_local! { static CALLBACK_AT: Cell<Option<Instant>> = const { Cell::new(None) }; }
        fn record(force: bool) {
            assert!(!force);
            CALLBACK_AT.with(|at| {
                let now = Instant::now();
                if at
                    .replace(Some(now))
                    .is_some_and(|at| now.saturating_duration_since(at) < Duration::from_millis(39))
                {
                    TOO_FAST.store(true, Ordering::Relaxed);
                }
            });
            COLLECTS.fetch_add(1, Ordering::Relaxed);
        }
        let Ok(case) = std::env::var("ZO_RECLAIM_TEST_CASE") else {
            for kind in ["current", "multi"] {
                for case in [
                    "enabled",
                    "late",
                    "off",
                    "uninitialized",
                    "no-allocator",
                    "invalid",
                ] {
                    let output = std::process::Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "allocator_reclaim::tests::park_runtime_startup_and_throttle",
                            "--nocapture",
                        ])
                        .env("ZO_RECLAIM_TEST_CASE", case)
                        .env("ZO_RECLAIM_RUNTIME_KIND", kind)
                        .env(
                            INTERVAL_ENV,
                            if case == "invalid" {
                                "-1"
                            } else if case == "off" {
                                "0"
                            } else {
                                "40"
                            },
                        )
                        .env("ZO_VIX_EVAL_COLLECT_MODE", "off")
                        .env("ZO_VIX_EVAL_COLLECT_INTERVAL_MS", "100")
                        .output()
                        .unwrap();
                    assert!(
                        output.status.success(),
                        "{kind}/{case}: {} {}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
            }
            return;
        };
        if case == "invalid" {
            assert!(Settings::from_env().is_err());
            return;
        }
        let initialize_policies = || {
            if case != "no-allocator" {
                register_collector(record).unwrap();
            }
            crate::eval_reclaim::initialize(crate::eval_reclaim::Settings::from_env().unwrap())
                .unwrap();
            initialize(Settings::from_env().unwrap()).unwrap();
        };
        if case != "late" && case != "uninitialized" {
            initialize_policies();
        }
        let mut builder = if std::env::var("ZO_RECLAIM_RUNTIME_KIND").unwrap() == "multi" {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.worker_threads(1);
            builder
        } else {
            tokio::runtime::Builder::new_current_thread()
        };
        builder.on_thread_park(|| {
            EXISTING_HOOK.fetch_add(1, Ordering::Relaxed);
        });
        let runtime = configure_runtime(builder).enable_all().build().unwrap();
        let exercise = || {
            runtime.block_on(async {
                tokio::spawn(async {
                    for _ in 0..50 {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
                .unwrap();
            })
        };
        if case == "late" {
            exercise();
            assert_eq!(COLLECTS.load(Ordering::Relaxed), 0);
            initialize_policies();
        }
        exercise();
        drop(runtime);
        let calls = COLLECTS.load(Ordering::Relaxed);
        if case == "enabled" || case == "late" {
            assert!(
                calls >= 2,
                "hook must run again after the interval: {calls}"
            );
            assert!(
                !TOO_FAST.load(Ordering::Relaxed),
                "hook ignored thread-local throttle"
            );
        } else {
            assert_eq!(calls, 0);
        }
        if case == "off" {
            assert!(
                EXISTING_HOOK.load(Ordering::Relaxed) > 0,
                "off must not replace/install a hook"
            );
        }
    }
}
