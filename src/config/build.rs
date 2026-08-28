// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    env,
    io::{Error, ErrorKind, Result},
    path::{Path, PathBuf},
    process::Command,
};

use chrono::{DateTime, SecondsFormat, Utc};

const BUILD_GIT_COMMIT_ENV: &str = "OPENOBSERVE_BUILD_GIT_COMMIT";
const BUILD_GIT_VERSION_ENV: &str = "OPENOBSERVE_BUILD_GIT_VERSION";

fn git_output(args: &[&str], description: &str) -> Result<String> {
    let output = Command::new("git").args(args).output().map_err(|error| {
        Error::new(
            error.kind(),
            format!("failed to run git while resolving {description}: {error}"),
        )
    })?;
    if !output.status.success() {
        return Err(Error::other(format!(
            "git {} failed while resolving {description}: status={} stderr={}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    let value = String::from_utf8(output.stdout).map_err(|error| {
        Error::new(
            ErrorKind::InvalidData,
            format!("git returned non-UTF-8 {description}: {error}"),
        )
    })?;
    let value = value.trim();
    if value.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("git returned an empty {description}"),
        ));
    }
    Ok(value.to_owned())
}

fn build_value(env_name: &str, git_args: &[&str], description: &str) -> Result<(String, bool)> {
    println!("cargo:rerun-if-env-changed={env_name}");
    let (value, from_git) = match env::var(env_name) {
        Ok(value) if !value.trim().is_empty() => Ok((value.trim().to_owned(), false)),
        Ok(_) => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("{env_name} is set but empty; expected {description}"),
        )),
        Err(env::VarError::NotPresent) => {
            git_output(git_args, description).map(|value| (value, true))
        }
        Err(env::VarError::NotUnicode(_)) => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("{env_name} is not valid UTF-8; expected {description}"),
        )),
    }?;
    if value.contains(['\r', '\n']) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("{env_name} contains a line break; expected one line of {description}"),
        ));
    }
    Ok((value, from_git))
}

fn git_path(args: &[&str], description: &str) -> Result<PathBuf> {
    let path = PathBuf::from(git_output(args, description)?);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn watch_git_path(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}

fn watch_git_metadata() -> Result<()> {
    let git_dir = git_path(&["rev-parse", "--git-dir"], "Git directory")?;
    let common_dir = git_path(&["rev-parse", "--git-common-dir"], "common Git directory")?;

    watch_git_path(&git_dir.join("HEAD"));
    watch_git_path(&common_dir.join("packed-refs"));
    watch_git_path(&common_dir.join("refs/tags"));

    let symbolic_ref = Command::new("git")
        .args(["symbolic-ref", "-q", "HEAD"])
        .output()
        .map_err(|error| {
            Error::new(
                error.kind(),
                format!("failed to run git while resolving the symbolic HEAD: {error}"),
            )
        })?;
    if symbolic_ref.status.success() {
        let reference = String::from_utf8(symbolic_ref.stdout).map_err(|error| {
            Error::new(
                ErrorKind::InvalidData,
                format!("git returned a non-UTF-8 symbolic HEAD: {error}"),
            )
        })?;
        watch_git_path(&common_dir.join(reference.trim()));
    } else if symbolic_ref.status.code() != Some(1) {
        return Err(Error::other(format!(
            "git symbolic-ref -q HEAD failed: status={} stderr={}",
            symbolic_ref.status,
            String::from_utf8_lossy(&symbolic_ref.stderr).trim()
        )));
    }

    Ok(())
}

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=build.rs");

    // build information
    let (git_tag, git_tag_from_git) = build_value(
        BUILD_GIT_VERSION_ENV,
        &["describe", "--tags", "--abbrev=0"],
        "Git version tag",
    )?;
    println!("cargo:rustc-env=GIT_VERSION={git_tag}");

    let (git_commit, git_commit_from_git) = build_value(
        BUILD_GIT_COMMIT_ENV,
        &["rev-parse", "HEAD"],
        "40-character Git commit SHA",
    )?;
    if git_commit.len() != 40
        || !git_commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "{BUILD_GIT_COMMIT_ENV} resolved to {git_commit:?}; expected a lowercase \
                 40-character hexadecimal Git SHA"
            ),
        ));
    }
    println!("cargo:rustc-env=GIT_COMMIT_HASH={git_commit}");

    if git_tag_from_git || git_commit_from_git {
        watch_git_metadata()?;
    }

    let now: DateTime<Utc> = Utc::now();
    let build_date = now.to_rfc3339_opts(SecondsFormat::Secs, true);
    println!("cargo:rustc-env=GIT_BUILD_DATE={build_date}");

    Ok(())
}
