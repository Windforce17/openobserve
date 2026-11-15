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

/// Input-byte budgets, not a promise about the size of a re-encoded output.
#[derive(Clone, Copy, Debug)]
pub struct MergeSizeLimits {
    pub original: i64,
    pub compressed: i64,
}

impl MergeSizeLimits {
    pub const fn new(original: i64, compressed: i64) -> Self {
        Self {
            original,
            compressed,
        }
    }

    pub const fn compressed_enabled(self) -> bool {
        self.compressed > 0
    }

    /// Either half-target makes a file final. Consequently any two candidates
    /// fit both budgets; the debt query must use the complement (AND).
    pub fn is_final(self, original: i64, compressed: i64) -> bool {
        original > self.original / 2
            || (self.compressed_enabled() && compressed > self.compressed / 2)
    }

    pub fn exceeds_plan(self, original: i64, compressed: i64) -> bool {
        original > self.original || (self.compressed_enabled() && compressed > self.compressed)
    }

    /// Preserve the legacy executor's additional compressed-byte guard when
    /// the new target is disabled, including unusual expanding encodings.
    pub fn exceeds_execution(self, original: i64, compressed: i64) -> bool {
        original > self.original || compressed > self.execution_compressed_limit()
    }

    pub fn execution_compressed_limit(self) -> i64 {
        if self.compressed_enabled() {
            self.compressed
        } else {
            self.original
        }
    }

    pub fn reached(self, original: i64, compressed: i64) -> bool {
        self.compressed_enabled() && (original >= self.original || compressed >= self.compressed)
    }

    pub fn sort_size(self, original: i64, compressed: i64) -> i64 {
        if self.compressed_enabled() {
            compressed
        } else {
            original
        }
    }

    pub fn candidate_compressed_limit(self) -> i64 {
        if self.compressed_enabled() {
            self.compressed / 2
        } else {
            i64::MAX
        }
    }

    pub fn query_limits(self, include_oversize: bool) -> (i64, i64) {
        if include_oversize {
            (i64::MAX, i64::MAX)
        } else {
            (
                self.original / 100 * 95 + self.original % 100 * 95 / 100,
                if self.compressed_enabled() {
                    self.compressed / 100 * 95 + self.compressed % 100 * 95 / 100
                } else {
                    i64::MAX
                },
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compressed_target_disabled_preserves_distinct_legacy_guards() {
        let limits = MergeSizeLimits::new(100, 0);
        for original in [0, 49, 50, 51, 100, 101] {
            for compressed in [0, 49, 50, 51, 100, 101, i64::MAX] {
                assert_eq!(limits.is_final(original, compressed), original > 50);
                assert_eq!(limits.exceeds_plan(original, compressed), original > 100);
                assert_eq!(
                    limits.exceeds_execution(original, compressed),
                    original > 100 || compressed > 100
                );
                assert!(!limits.reached(original, compressed));
                assert_eq!(limits.sort_size(original, compressed), original);
            }
        }
        assert_eq!(limits.candidate_compressed_limit(), i64::MAX);
        assert_eq!(limits.query_limits(false), (95, i64::MAX));
        assert_eq!(limits.query_limits(true), (i64::MAX, i64::MAX));
    }

    #[test]
    fn compressed_target_final_and_debt_are_complements() {
        let limits = MergeSizeLimits::new(100, 20);
        for original in 0..=101 {
            for compressed in 0..=21 {
                let candidate = original <= 50 && compressed <= limits.candidate_compressed_limit();
                assert_eq!(limits.is_final(original, compressed), !candidate);
                assert_eq!(
                    limits.exceeds_plan(original, compressed),
                    limits.exceeds_execution(original, compressed)
                );
                if candidate {
                    assert!(!limits.exceeds_plan(original * 2, compressed * 2));
                }
            }
        }
        assert_eq!(limits.query_limits(false), (95, 19));
        assert_eq!(limits.query_limits(true), (i64::MAX, i64::MAX));
    }
}
