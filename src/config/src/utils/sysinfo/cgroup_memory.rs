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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryUsage {
    pub current: u64,
    pub working_set: Option<u64>,
    /// cgroup v2 `memory.stat` fields in [`STAT_KEYS`] that are present, in
    /// that order. Empty on v1 or when memory.stat is unreadable.
    pub stat: Vec<(&'static str, u64)>,
    /// cgroup v2 `memory.events` counters in [`EVENT_KEYS`] that are present.
    pub events: Vec<(&'static str, u64)>,
}

/// v2 memory.stat fields that split usage into anonymous, file (and how much
/// of it is dirty or under writeback), huge-page and kernel memory.
pub const STAT_KEYS: [&str; 11] = [
    "anon",
    "file",
    "active_file",
    "inactive_file",
    "file_dirty",
    "file_writeback",
    "anon_thp",
    "shmem",
    "slab",
    "sock",
    "kernel",
];

/// v2 memory.events counters: reclaim pressure (`high`, `max`) and OOM.
pub const EVENT_KEYS: [&str; 4] = ["high", "max", "oom", "oom_kill"];

/// Read only the process's own memory cgroup, not a host/root fallback.
/// v2 working set is memory.current - inactive_file; v1 uses hierarchical
/// total_inactive_file. Missing stats must not masquerade as zero usage.
pub fn read() -> Option<MemoryUsage> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    let (dir, v2) = location(&membership, &mounts)?;
    read_usage(&dir, v2)
}

fn read_usage(dir: &Path, v2: bool) -> Option<MemoryUsage> {
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
    let events = if v2 {
        std::fs::read_to_string(dir.join("memory.events")).ok()
    } else {
        None
    };
    Some(MemoryUsage {
        current,
        working_set: stat.as_deref().and_then(|s| working_set(current, s, v2)),
        stat: match (&stat, v2) {
            (Some(text), true) => pick(text, &STAT_KEYS),
            _ => Vec::new(),
        },
        events: events
            .as_deref()
            .map_or_else(Vec::new, |text| pick(text, &EVENT_KEYS)),
    })
}

/// `key value` lines restricted to `keys`, in `keys` order; unparseable or
/// absent keys are omitted rather than reported as zero.
fn pick(text: &str, keys: &[&'static str]) -> Vec<(&'static str, u64)> {
    keys.iter()
        .filter_map(|&key| {
            text.lines().find_map(|line| {
                let mut fields = line.split_whitespace();
                if fields.next()? != key {
                    return None;
                }
                Some((key, fields.next()?.parse::<u64>().ok()?))
            })
        })
        .collect()
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
    location_with_mount(membership, mounts).map(|(dir, v2, _)| (dir, v2))
}

fn location_with_mount(membership: &str, mounts: &str) -> Option<(PathBuf, bool, PathBuf)> {
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
            let mount = PathBuf::from(unescape(before[4]));
            return Some((mount.join(relative), v2, mount));
        }
    }
    None
}

/// `None` means unreadable/invalid, not zero or unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryLimit {
    Bytes(u64),
    Unlimited,
}

impl MemoryLimit {
    pub fn bytes(self) -> Option<u64> {
        match self {
            Self::Bytes(bytes) => Some(bytes),
            Self::Unlimited => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Psi {
    pub avg10: f64,
    pub avg60: f64,
    pub total_usec: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MemoryObservation {
    pub max: Option<MemoryLimit>,
    pub high: Option<MemoryLimit>,
    pub some: Option<Psi>,
    pub full: Option<Psi>,
    pub swap_current: Option<u64>,
    pub swap_max: Option<MemoryLimit>,
    /// Minimum visible ancestor headroom plus this cgroup's current usage.
    pub ancestor_limit: Option<u64>,
    /// Hidden namespace ancestors cannot be certified by mountinfo alone.
    pub ancestor_scope_unknown: bool,
}

impl MemoryObservation {
    pub fn effective_limit(&self) -> Option<u64> {
        [
            self.max.and_then(MemoryLimit::bytes),
            self.high.and_then(MemoryLimit::bytes),
            self.ancestor_limit,
        ]
        .into_iter()
        .flatten()
        .min()
    }
}

#[derive(Debug, Clone)]
pub struct MemorySample {
    /// Read start, not finish: slow IO must not rejuvenate an old sample.
    pub started_at: std::time::Instant,
    pub usage: Option<MemoryUsage>,
    pub observation: MemoryObservation,
}

/// Observe mode only. Legacy fields and extended fields share one location and
/// one memory.stat/events read; off calls `read` and never enters this function.
pub fn read_observed() -> MemorySample {
    let started_at = std::time::Instant::now();
    let located = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|membership| {
            let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
            location_with_mount(&membership, &mounts)
        });
    let Some((dir, v2, mount)) = located else {
        return MemorySample {
            started_at,
            usage: None,
            observation: MemoryObservation {
                ancestor_scope_unknown: true,
                ..Default::default()
            },
        };
    };
    let usage = read_usage(&dir, v2);
    let observation = observe_at(
        &dir,
        &mount,
        v2,
        usage.as_ref().map(|u| u.current),
        &|path| std::fs::read_to_string(path),
    );
    MemorySample {
        started_at,
        usage,
        observation,
    }
}

fn parse_limit(text: &str) -> Option<MemoryLimit> {
    match text.trim() {
        "max" => Some(MemoryLimit::Unlimited),
        value => value.parse::<u64>().ok().map(MemoryLimit::Bytes),
    }
}

fn parse_psi(text: &str, kind: &str) -> Option<Psi> {
    let line = text
        .lines()
        .find(|line| line.split_whitespace().next() == Some(kind))?;
    let value = |name: &str| {
        line.split_whitespace().skip(1).find_map(|field| {
            let (key, value) = field.split_once('=')?;
            (key == name).then_some(value)
        })
    };
    let average = |name: &str| {
        let value = value(name)?.parse::<f64>().ok()?;
        (value.is_finite() && (0.0..=100.0).contains(&value)).then_some(value)
    };
    Some(Psi {
        avg10: average("avg10")?,
        avg60: average("avg60")?,
        total_usec: value("total")?.parse().ok()?,
    })
}

fn observe_at(
    dir: &Path,
    mount: &Path,
    v2: bool,
    current: Option<u64>,
    read: &impl Fn(&Path) -> std::io::Result<String>,
) -> MemoryObservation {
    let text = |path: PathBuf| read(&path).ok();
    let limit = |path: PathBuf| text(path).as_deref().and_then(parse_limit);
    let max_name = if v2 {
        "memory.max"
    } else {
        "memory.limit_in_bytes"
    };
    let mut observation = MemoryObservation {
        max: limit(dir.join(max_name)),
        // Even '/' in mountinfo can be a cgroup namespace root.
        ancestor_scope_unknown: true,
        ..Default::default()
    };
    if v2 {
        observation.high = limit(dir.join("memory.high"));
        observation.swap_max = limit(dir.join("memory.swap.max"));
        observation.swap_current =
            text(dir.join("memory.swap.current")).and_then(|s| s.trim().parse().ok());
        if let Some(psi) = text(dir.join("memory.pressure")) {
            observation.some = parse_psi(&psi, "some");
            observation.full = parse_psi(&psi, "full");
        }
    }
    // Stop at the visible mount, never fall back to the host hierarchy.
    for ancestor in dir
        .ancestors()
        .skip(1)
        .take_while(|path| path.starts_with(mount))
    {
        let ancestor_current = text(ancestor.join(if v2 {
            "memory.current"
        } else {
            "memory.usage_in_bytes"
        }))
        .and_then(|s| s.trim().parse::<u64>().ok());
        for bound in [Some(max_name), v2.then_some("memory.high")]
            .into_iter()
            .flatten()
        {
            if let (Some(local), Some(used), Some(bound)) = (
                current,
                ancestor_current,
                limit(ancestor.join(bound)).and_then(MemoryLimit::bytes),
            ) {
                // Physical headroom is clamped at zero, NOT an owner-accounting refund.
                let headroom = bound.saturating_sub(used);
                if let Some(candidate) = local.checked_add(headroom) {
                    observation.ancestor_limit = Some(
                        observation
                            .ancestor_limit
                            .map_or(candidate, |old| old.min(candidate)),
                    );
                }
            }
        }
    }
    observation
}

#[cfg(test)]
mod observation_tests {
    use super::*;

    #[test]
    fn missing_and_permission_errors_are_not_zero() {
        for kind in [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied,
        ] {
            let observation =
                observe_at(Path::new("/cg/a"), Path::new("/cg"), true, Some(1), &|_| {
                    Err(std::io::Error::from(kind))
                });
            assert_eq!(observation.effective_limit(), None);
            assert_eq!(observation.swap_current, None);
            assert_eq!(observation.some, None);
            assert!(observation.ancestor_scope_unknown);
        }
    }

    #[test]
    fn max_literal_and_invalid_limits() {
        assert_eq!(parse_limit("max\n"), Some(MemoryLimit::Unlimited));
        assert_eq!(parse_limit("0"), Some(MemoryLimit::Bytes(0)));
        for value in ["", "-1", "bad", "18446744073709551616"] {
            assert_eq!(parse_limit(value), None);
        }
    }

    #[test]
    fn psi_fields_and_invalid_formats() {
        let text =
            "some avg10=1.25 avg60=0.50 avg300=0.10 total=42\nfull total=7 avg60=2 avg10=3\n";
        assert_eq!(
            parse_psi(text, "some"),
            Some(Psi {
                avg10: 1.25,
                avg60: 0.5,
                total_usec: 42
            })
        );
        assert_eq!(parse_psi(text, "full").unwrap().total_usec, 7);
        for text in [
            "some avg10=NaN avg60=0 total=1",
            "some avg10=101 avg60=0 total=1",
            "some avg10=0 total=1",
            "full avg10=0 avg60=0 total=1",
            "some avg10=0 avg60=0 total=-1",
        ] {
            assert_eq!(parse_psi(text, "some"), None);
        }
    }

    #[test]
    fn fixtures_cover_v2_ancestors_and_v1() {
        let fixture = tempfile::tempdir().unwrap();
        let mount = fixture.path();
        let dir = mount.join("child");
        std::fs::create_dir(&dir).unwrap();
        for (path, contents) in [
            (dir.join("memory.max"), "1000"),
            (dir.join("memory.high"), "max"),
            (dir.join("memory.swap.max"), "max"),
            (dir.join("memory.swap.current"), "12"),
            (mount.join("memory.max"), "900"),
            (mount.join("memory.current"), "800"),
        ] {
            std::fs::write(path, contents).unwrap();
        }
        let observation = observe_at(&dir, mount, true, Some(200), &|p| {
            std::fs::read_to_string(p)
        });
        assert_eq!(observation.ancestor_limit, Some(300));
        assert_eq!(observation.effective_limit(), Some(300));
        assert_eq!(observation.swap_max, Some(MemoryLimit::Unlimited));
        assert_eq!(observation.swap_current, Some(12));
        std::fs::write(dir.join("memory.limit_in_bytes"), "700").unwrap();
        std::fs::write(dir.join("memory.usage_in_bytes"), "200").unwrap();
        std::fs::write(dir.join("memory.stat"), "total_inactive_file 20\n").unwrap();
        let v1 = observe_at(&dir, mount, false, Some(200), &|p| {
            std::fs::read_to_string(p)
        });
        assert_eq!(v1.max, Some(MemoryLimit::Bytes(700)));
        assert_eq!(v1.high, None);
        assert_eq!(v1.some, None);
        assert_eq!(v1.swap_current, None);
        let usage = read_usage(&dir, false).unwrap();
        assert_eq!(usage.working_set, Some(180));
        assert!(usage.stat.is_empty());
        assert!(usage.events.is_empty());
    }
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
    fn picks_present_stat_and_event_fields_only() {
        assert_eq!(
            pick(
                "anon 20\nfile 30\nfile_dirty 7\nfile_writeback x\nanon_thp 0\n",
                &STAT_KEYS
            ),
            vec![
                ("anon", 20),
                ("file", 30),
                ("file_dirty", 7),
                ("anon_thp", 0)
            ]
        );
        assert_eq!(
            pick(
                "low 0\nhigh 3\nmax 9\noom 1\noom_kill 1\noom_group_kill 0\n",
                &EVENT_KEYS
            ),
            vec![("high", 3), ("max", 9), ("oom", 1), ("oom_kill", 1)]
        );
        assert!(pick("", &STAT_KEYS).is_empty());
    }
    #[test]
    fn mountinfo_escapes_are_resolved() {
        assert_eq!(
            location("0::/\n", "1 2 0:1 / /cg\\040space rw - cgroup2 cgroup rw\n"),
            Some((PathBuf::from("/cg space/"), true))
        );
    }
}
