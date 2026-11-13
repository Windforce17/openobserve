// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Startup-only, default-off reader representation settings.
//! Kept dependency-free so the standalone VIX engine can share the parser.

use std::{env::VarError, io};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReaderCompactConfig {
    pub intern_fields: bool,
    pub lazy_dict: bool,
    pub compact_footer: bool,
}

impl ReaderCompactConfig {
    pub fn from_env() -> io::Result<Self> {
        Self::parse_with(|name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(VarError::NotPresent) => Ok(None),
            Err(VarError::NotUnicode(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must contain a Unicode boolean"),
            )),
        })
    }

    fn parse_with(mut read: impl FnMut(&str) -> io::Result<Option<String>>) -> io::Result<Self> {
        let mut flag = |name: &str| match read(name)?.as_deref() {
            None | Some("0" | "false") => Ok(false),
            Some("1" | "true") => Ok(true),
            Some(value) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name}={value:?}: expected 0, 1, false, or true"),
            )),
        };
        Ok(Self {
            intern_fields: flag("ZO_VIX_INTERN_FIELDS")?,
            lazy_dict: flag("ZO_VIX_LAZY_DICT")?,
            compact_footer: flag("ZO_VIX_COMPACT_FOOTER")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_config_defaults_off_and_accepts_original_boolean_spellings() {
        assert_eq!(
            ReaderCompactConfig::parse_with(|_| Ok(None)).unwrap(),
            ReaderCompactConfig::default()
        );
        for (value, enabled) in [("0", false), ("false", false), ("1", true), ("true", true)] {
            assert_eq!(
                ReaderCompactConfig::parse_with(|_| Ok(Some(value.into()))).unwrap(),
                ReaderCompactConfig {
                    intern_fields: enabled,
                    lazy_dict: enabled,
                    compact_footer: enabled,
                }
            );
        }
    }

    #[test]
    fn compact_config_rejects_invalid_values_for_each_switch() {
        for name in [
            "ZO_VIX_INTERN_FIELDS",
            "ZO_VIX_LAZY_DICT",
            "ZO_VIX_COMPACT_FOOTER",
        ] {
            for value in ["", "TRUE", "yes", "2", " true", "false "] {
                let error =
                    ReaderCompactConfig::parse_with(|key| Ok((key == name).then(|| value.into())))
                        .unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                assert!(error.to_string().contains(name));
            }
        }
        assert!(
            ReaderCompactConfig::parse_with(|_| Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "non-Unicode"
            )))
            .is_err()
        );
    }
}
