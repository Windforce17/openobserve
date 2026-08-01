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

//! SBBF primitives — moved to `vortex_index::sbbf` so the `.vix` writer can
//! build per-file value blooms; re-exported here so the group `.bf`
//! machinery (and every existing caller) keeps its paths. The two crates
//! share one implementation, so per-file blob blocks transpose into group
//! `.bf` bodies byte-for-byte.

pub use vortex_index::sbbf::*;

#[cfg(test)]
mod tests {
    /// Hash parity with the project-wide gxhash util: the sbbf hash moved
    /// crates but MUST stay `config::utils::hash::sum64_bytes` forever —
    /// every persisted bloom depends on it.
    #[test]
    fn hash_matches_config_sum64_bytes() {
        for v in [b"".as_slice(), b"a", b"trace-12345", &[0u8, 255, 7, 42]] {
            assert_eq!(
                vortex_index::sbbf::hash_value(v),
                config::utils::hash::sum64_bytes(v),
                "sbbf hash drifted from config::utils::hash for {v:?}"
            );
        }
    }
}
