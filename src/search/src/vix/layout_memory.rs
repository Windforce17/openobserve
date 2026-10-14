// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! DESIGN-COLD-MISS §2 S3 transition aid: which `(stream, hour)` buckets
//! still hold sidecars WITHOUT an embedded data footer.
//!
//! A sidecar-first open of such a file learns that only after the sidecar
//! tail arrived and then pays the data tail as a second, SERIAL round trip;
//! the paired open issues both tails in one. Old hours are never repacked
//! unless something re-pends them, so without this memory every cold open
//! in the historical window would carry that extra depth for as long as the
//! window holds pre-property files. The querier therefore remembers, per
//! hour, what the last open found: an hour whose last file carried no footer
//! is opened the paired way (one wasted 64 KiB data GET per file if the
//! guess is stale — no extra depth), and is forgotten the moment one of its
//! files turns out to carry the footer (repack, re-merge). One file per hour
//! per pod pays the serial round trip; the rest of the hour does not.

use std::sync::LazyLock;

use hashlink::LruCache;
use parking_lot::Mutex;

/// Hours remembered as old-layout. ~80 B per entry; 65k hours ≈ 7 years of
/// one stream or a week of 400 streams — plenty, and the LRU bounds it.
const REMEMBERED_HOURS: usize = 65_536;

static OLD_LAYOUT_HOURS: LazyLock<Mutex<LruCache<String, ()>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(REMEMBERED_HOURS)));

/// The `(stream, hour)` bucket of a core file key
/// (`files/<org>/<type>/<stream>/<yyyy>/<mm>/<dd>/<hh>/<name>`), or `None`
/// for keys outside that shape (they open with the process-wide mode).
pub(crate) fn hour_bucket(file_key: &str) -> Option<String> {
    let (stream, date, _) = config::utils::parquet::parse_file_key_columns(file_key).ok()?;
    Some(format!("{stream}/{date}"))
}

/// Whether the hour's last opened sidecar lacked the embedded data footer.
pub(crate) fn hour_is_old_layout(hour: &str) -> bool {
    OLD_LAYOUT_HOURS.lock().get(hour).is_some()
}

/// Record what an open found: `old_layout` = the sidecar carried no
/// embedded data footer (`layout_gen() == 0`).
pub(crate) fn observe(hour: String, old_layout: bool) {
    let mut hours = OLD_LAYOUT_HOURS.lock();
    if old_layout {
        hours.insert(hour, ());
    } else {
        hours.remove(&hour);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hour_bucket_is_stream_and_hour() {
        assert_eq!(
            hour_bucket("files/o/logs/default/2026/10/14/07/7515392618021380096.vix").as_deref(),
            Some("o/logs/default/2026/10/14/07")
        );
        assert_eq!(hour_bucket("not/a/core/file"), None);
    }

    #[test]
    fn an_hour_is_old_until_one_file_carries_the_footer() {
        let hour = "o/logs/layout-memory-test/2026/10/14/08".to_string();
        assert!(!hour_is_old_layout(&hour));
        observe(hour.clone(), true);
        assert!(hour_is_old_layout(&hour));
        observe(hour.clone(), false);
        assert!(!hour_is_old_layout(&hour));
    }
}
