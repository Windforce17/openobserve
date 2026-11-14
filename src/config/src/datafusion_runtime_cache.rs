// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{env, sync::OnceLock};

const ENV: &str = "ZO_QUERY_SMALL_RUNTIME_CACHES";
static ENABLED: OnceLock<Result<bool, String>> = OnceLock::new();

fn parse(value: Option<&str>) -> Result<bool, String> {
    match value {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(value) => Err(format!("{ENV} must be true or false, got {value:?}")),
    }
}

/// Restart-only policy, validated by config initialization before serving requests.
pub fn enabled() -> anyhow::Result<bool> {
    ENABLED
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
    fn small_runtime_caches_default_off_and_strict_boolean() {
        assert!(!parse(None).unwrap());
        assert!(!parse(Some("false")).unwrap());
        assert!(parse(Some("true")).unwrap());
        for value in ["", "1", "0", "TRUE", "False", " true", "false ", "on"] {
            assert!(parse(Some(value)).unwrap_err().contains(ENV));
        }
    }

    #[test]
    fn small_runtime_caches_startup_value_and_error_are_cached() {
        const CHILD: &str = "OO_SMALL_RUNTIME_CACHES_TEST_CHILD";
        if let Ok(expected) = env::var(CHILD) {
            let actual = enabled();
            if expected == "error" {
                assert!(actual.unwrap_err().to_string().contains(ENV));
                assert!(enabled().is_err());
            } else {
                let expected = expected.parse::<bool>().unwrap();
                assert_eq!(actual.unwrap(), expected);
                // This subprocess runs exactly one test on a single test thread.
                unsafe { env::set_var(ENV, "invalid-after-startup") };
                assert_eq!(enabled().unwrap(), expected);
            }
            return;
        }
        for (value, expected) in [
            (None, "false"),
            (Some("false"), "false"),
            (Some("true"), "true"),
            (Some("garbage"), "error"),
            (Some("1"), "error"),
        ] {
            let mut child = std::process::Command::new(env::current_exe().unwrap());
            child.args([
                "datafusion_runtime_cache::tests::small_runtime_caches_startup_value_and_error_are_cached",
                "--exact",
                "--test-threads=1",
            ]);
            child.env(CHILD, expected).env_remove(ENV);
            if let Some(value) = value {
                child.env(ENV, value);
            }
            assert!(child.status().unwrap().success(), "{value:?}");
        }
    }

    #[tokio::test]
    async fn invalid_runtime_cache_setting_fails_config_initialization() {
        const CHILD: &str = "OO_INVALID_RUNTIME_CACHE_CONFIG_CHILD";
        if env::var_os(CHILD).is_some() {
            let error = crate::init().await.unwrap_err();
            assert!(error.to_string().contains(ENV));
            return;
        }
        let status = std::process::Command::new(env::current_exe().unwrap())
            .args([
                "datafusion_runtime_cache::tests::invalid_runtime_cache_setting_fails_config_initialization",
                "--exact",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .env(ENV, "invalid")
            .status()
            .unwrap();
        assert!(status.success());
    }
}
