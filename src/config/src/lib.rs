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

#![feature(macro_metavar_expr_concat)]

pub mod allocator_reclaim;
pub mod axum;
pub mod cluster;
pub mod config;
pub mod config_path_manager;
pub mod datafusion;
pub mod datafusion_headroom;
pub mod datafusion_runtime_cache;
pub mod eval_reclaim;
pub mod heap_profile;
pub mod ider;
pub mod memory_ledger;
pub mod meta;
pub mod metrics;
pub mod prebuilt_loader;
pub mod reader_compact;
pub mod router;
pub mod search_admission;
pub mod stats;
pub mod text_tokenizer;
pub mod utils;

pub use config::*;

pub async fn init() -> Result<(), anyhow::Error> {
    datafusion_runtime_cache::enabled()?;
    // init ider
    ider::init();
    // init metrics
    metrics::init();

    // initialize chrome launch options, so that if chrome download is
    // needed, it will happen now and not during serving report API
    if cluster::LOCAL_NODE.is_alert_manager() {
        let _ = get_chrome_launch_options().await;
    }
    Ok(())
}
