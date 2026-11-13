// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Restart-stable query-pool headroom, validated before serving requests.

use std::{env, sync::OnceLock};

const ENV: &str = "ZO_DATAFUSION_UNSPILLABLE_RESERVE_PERCENT";
static PERCENT: OnceLock<Result<u8, String>> = OnceLock::new();

fn parse(value: Option<&str>) -> Result<u8, String> {
    let Some(value) = value else {
        return Ok(0);
    };
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("{ENV} must be an integer in 0..=50, got {value:?}"));
    }
    value
        .parse::<u8>()
        .ok()
        .filter(|p| *p <= 50)
        .ok_or_else(|| format!("{ENV} must be an integer in 0..=50, got {value:?}"))
}

/// Both errors and successful values are cached. Reload cannot silently change
/// the policy of existing pools. The server calls this after loading .env/CLI
/// config and propagates errors from run(), not from a panic-based initializer.
pub fn reserve_percent() -> anyhow::Result<u8> {
    PERCENT
        .get_or_init(|| match env::var(ENV) {
            Ok(value) => parse(Some(&value)),
            Err(env::VarError::NotPresent) => parse(None),
            Err(error) => Err(format!("{ENV}: {error}")),
        })
        .as_ref()
        .copied()
        .map_err(|error| anyhow::anyhow!(error.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headroom_defaults_off_and_accepts_inclusive_bounds() {
        assert_eq!(parse(None).unwrap(), 0);
        for p in 0..=50 {
            assert_eq!(parse(Some(&p.to_string())).unwrap(), p);
        }
    }

    #[test]
    fn headroom_rejects_invalid_startup_values() {
        for value in [
            "",
            "-1",
            "+1",
            "51",
            "256",
            "1.5",
            "true",
            " 10",
            "10 ",
            "18446744073709551616",
        ] {
            let error = parse(Some(value)).unwrap_err();
            assert!(error.contains(ENV), "{value}: {error}");
        }
    }

    #[test]
    fn headroom_startup_process_propagates_errors_and_caches_value() {
        const CHILD: &str = "OO_HEADROOM_CONFIG_TEST_CHILD";
        if let Ok(expected) = env::var(CHILD) {
            let actual = reserve_percent();
            if expected == "error" {
                assert!(actual.unwrap_err().to_string().contains(ENV));
                assert!(reserve_percent().is_err());
            } else {
                assert_eq!(actual.unwrap(), expected.parse::<u8>().unwrap());
                // The cached setting must not depend on future environment changes.
                // This child runs exactly one test, on one test thread.
                unsafe { env::set_var(ENV, "invalid-after-startup") };
                assert_eq!(reserve_percent().unwrap(), expected.parse::<u8>().unwrap());
            }
            return;
        }
        for (value, expected) in [
            (None, "0"),
            (Some("15"), "15"),
            (Some("51"), "error"),
            (Some("garbage"), "error"),
        ] {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child.args(["datafusion_headroom::tests::headroom_startup_process_propagates_errors_and_caches_value", "--exact", "--test-threads=1"]);
            child.env(CHILD, expected).env_remove(ENV);
            if let Some(value) = value {
                child.env(ENV, value);
            }
            assert!(child.status().unwrap().success(), "{value:?}");
        }
    }
}
