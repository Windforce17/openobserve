// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Shared by startup validation and the standalone VIX writer (no crate dependencies).

use std::{env::VarError, io};

pub const ENV: &str = "ZO_VIX_MERGE_INFLIGHT_MB";
pub const DEFAULT_MB: u64 = 1024;
// Keep both byte conversion and Tokio's permit count safe, including on 32-bit hosts.
pub const MAX_MB: u64 = 1024 * 1024;

pub fn from_env() -> io::Result<u64> {
    parse(std::env::var(ENV))
}

fn parse(value: Result<String, VarError>) -> io::Result<u64> {
    let mb = match value {
        Err(VarError::NotPresent) => DEFAULT_MB,
        Ok(value) => value.parse::<u64>().map_err(|_| invalid())?,
        Err(VarError::NotUnicode(_)) => return Err(invalid()),
    };
    if !(1..=MAX_MB).contains(&mb) {
        return Err(invalid());
    }
    Ok(mb << 20)
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{ENV} must be an integer in 1..={MAX_MB} MiB (default {DEFAULT_MB})"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_and_boundaries() {
        assert_eq!(parse(Err(VarError::NotPresent)).unwrap(), 1024 << 20);
        for mb in [1, 512, DEFAULT_MB, MAX_MB] {
            assert_eq!(parse(Ok(mb.to_string())).unwrap(), mb << 20);
        }
    }

    #[test]
    fn invalid_values_fail_closed() {
        for value in [
            "",
            "0",
            "-1",
            "1.5",
            "foo",
            " 512",
            "1048577",
            "18446744073709551616",
        ] {
            assert!(parse(Ok(value.into())).is_err(), "accepted {value:?}");
        }
        assert!(parse(Err(VarError::NotUnicode("bad".into()))).is_err());
    }
}
