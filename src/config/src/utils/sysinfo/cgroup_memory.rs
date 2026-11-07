// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq)]
pub struct MemoryUsage {
    pub current: u64,
    pub working_set: Option<u64>,
}

/// Read only the process's own memory cgroup, not a host/root fallback.
/// v2 working set is memory.current - inactive_file; v1 uses hierarchical
/// total_inactive_file. Missing stats must not masquerade as zero usage.
pub fn read() -> Option<MemoryUsage> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    let (dir, v2) = location(&membership, &mounts)?;
    let current = std::fs::read_to_string(dir.join(if v2 {
        "memory.current"
    } else {
        "memory.usage_in_bytes"
    }))
    .ok()?
    .trim()
    .parse::<u64>()
    .ok()?;
    let stat = std::fs::read_to_string(dir.join("memory.stat")).ok();
    Some(MemoryUsage {
        current,
        working_set: stat.as_deref().and_then(|s| working_set(current, s, v2)),
    })
}

fn working_set(current: u64, stat: &str, v2: bool) -> Option<u64> {
    let key = if v2 {
        "inactive_file"
    } else {
        "total_inactive_file"
    };
    stat.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        if fields.next()? != key {
            return None;
        }
        Some(current.saturating_sub(fields.next()?.parse::<u64>().ok()?))
    })
}

// mountinfo escapes spaces, tabs, line feeds and backslashes using octal.
fn unescape(path: &str) -> String {
    path.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}
fn location(membership: &str, mounts: &str) -> Option<(PathBuf, bool)> {
    for line in membership.lines() {
        let mut fields = line.splitn(3, ':');
        let _hierarchy = fields.next()?;
        let controllers = fields.next()?;
        let member = Path::new(fields.next()?);
        let v2 = controllers.is_empty();
        if !v2 && !controllers.split(',').any(|s| s == "memory") {
            continue;
        }
        // Refuse ambiguous paths instead of reading outside the cgroup mount.
        if !member.is_absolute()
            || member
                .components()
                .any(|c| c == std::path::Component::ParentDir)
        {
            continue;
        }
        for mount in mounts.lines() {
            let Some((left, right)) = mount.split_once(" - ") else {
                continue;
            };
            let before: Vec<_> = left.split_whitespace().collect();
            let after: Vec<_> = right.split_whitespace().collect();
            if before.len() < 5 || after.len() < 3 {
                continue;
            }
            let matches = if v2 {
                after[0] == "cgroup2"
            } else {
                after[0] == "cgroup" && after[2].split(',').any(|s| s == "memory")
            };
            if !matches {
                continue;
            }
            let root = unescape(before[3]);
            let Ok(relative) = member.strip_prefix(Path::new(&root)) else {
                continue;
            };
            return Some((Path::new(&unescape(before[4])).join(relative), v2));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resolves_v2_nested_and_namespace_root() {
        assert_eq!(
            location(
                "0::/pod/container\n",
                "1 2 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
            ),
            Some((PathBuf::from("/sys/fs/cgroup/pod/container"), true))
        );
        assert_eq!(
            location(
                "0::/\n",
                "1 2 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
            ),
            Some((PathBuf::from("/sys/fs/cgroup/"), true))
        );
        assert_eq!(
            location(
                "0::/pod/container\n",
                "1 2 0:1 /pod/container /cg rw - cgroup2 cgroup rw\n"
            ),
            Some((PathBuf::from("/cg/"), true))
        );
    }
    #[test]
    fn resolves_v1_memory_mount_not_cpu() {
        assert_eq!(
            location(
                "5:cpu:/pod\n4:memory:/pod\n",
                "1 2 0:1 / /cg/cpu rw - cgroup cgroup rw,cpu\n3 2 0:2 / /cg/memory rw - cgroup cgroup rw,memory\n"
            ),
            Some((PathBuf::from("/cg/memory/pod"), false))
        );
    }
    #[test]
    fn malformed_or_unrelated_cgroups_are_omitted() {
        assert_eq!(location("garbage", "garbage"), None);
        assert_eq!(
            location("0::/other\n", "1 2 0:1 /pod /cg rw - cgroup2 cgroup rw\n"),
            None
        );
        assert_eq!(
            location("0::/../other\n", "1 2 0:1 / /cg rw - cgroup2 cgroup rw\n"),
            None
        );
    }
    #[test]
    fn computes_working_set_and_does_not_invent_missing_stats() {
        assert_eq!(
            working_set(100, "anon 20\ninactive_file 30\n", true),
            Some(70)
        );
        assert_eq!(
            working_set(100, "inactive_file 10\ntotal_inactive_file 40\n", false),
            Some(60)
        );
        assert_eq!(working_set(100, "inactive_file 150\n", true), Some(0));
        assert_eq!(working_set(100, "anon 20\n", true), None);
        assert_eq!(working_set(100, "inactive_file x\n", true), None);
    }
    #[test]
    fn mountinfo_escapes_are_resolved() {
        assert_eq!(
            location("0::/\n", "1 2 0:1 / /cg\\040space rw - cgroup2 cgroup rw\n"),
            Some((PathBuf::from("/cg space/"), true))
        );
    }
}
