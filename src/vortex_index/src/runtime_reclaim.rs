// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only
//! The embedding binary may supply an owner-thread park hook at startup.
//! No allocator/config dependency. Explicitly disabled runtimes get no hook;
//! runtimes built before initialization get a dormant, allocation-free hook.

use std::sync::OnceLock;

static PARK_HOOK: OnceLock<Option<fn()>> = OnceLock::new();

/// Startup-only. The supplied hook must not allocate, block, or panic.
/// None explicitly disables hooks for subsequently constructed runtimes.
pub fn register_park_hook(hook: Option<fn()>) -> Result<(), &'static str> {
    PARK_HOOK
        .set(hook)
        .map_err(|_| "VIX runtime park hook already registered")
}

fn on_thread_park() {
    if let Some(Some(hook)) = PARK_HOOK.get() {
        hook();
    }
}

pub(crate) fn configure_runtime(builder: tokio::runtime::Builder) -> tokio::runtime::Builder {
    configure_with(builder, PARK_HOOK.get().copied())
}

fn configure_with(
    mut builder: tokio::runtime::Builder,
    hook: Option<Option<fn()>>,
) -> tokio::runtime::Builder {
    match hook {
        Some(Some(hook)) => {
            builder.on_thread_park(hook);
        }
        None => {
            builder.on_thread_park(on_thread_park);
        }
        Some(None) => {}
    }
    builder
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn bridge_preserves_off_builder_and_forwards_enabled_hook() {
        static EXISTING: AtomicUsize = AtomicUsize::new(0);
        static FORWARDED: AtomicUsize = AtomicUsize::new(0);
        let mut off = tokio::runtime::Builder::new_current_thread();
        off.on_thread_park(|| {
            EXISTING.fetch_add(1, Ordering::Relaxed);
        });
        let off = configure_with(off, Some(None))
            .enable_all()
            .build()
            .unwrap();
        let early = configure_runtime(tokio::runtime::Builder::new_current_thread())
            .enable_all()
            .build()
            .unwrap();
        fn run(runtime: &tokio::runtime::Runtime) {
            runtime.block_on(async {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            });
        }
        run(&off);
        run(&early);
        assert!(EXISTING.load(Ordering::Relaxed) > 0);
        assert_eq!(FORWARDED.load(Ordering::Relaxed), 0);
        register_park_hook(Some(|| {
            FORWARDED.fetch_add(1, Ordering::Relaxed);
        }))
        .unwrap();
        run(&early);
        assert!(
            FORWARDED.load(Ordering::Relaxed) > 0,
            "early runtime must activate after initialization"
        );
        let before = FORWARDED.load(Ordering::Relaxed);
        let late = configure_runtime(tokio::runtime::Builder::new_current_thread())
            .enable_all()
            .build()
            .unwrap();
        run(&late);
        assert!(FORWARDED.load(Ordering::Relaxed) > before);
        assert!(register_park_hook(None).is_err());
    }
}
