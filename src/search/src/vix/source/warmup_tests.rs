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

use object_store::{
    memory::InMemory,
    throttle::{ThrottleConfig, ThrottledStore},
};

use super::*;
use crate::vix::{WarmFileOutcome, warm_file_outcome};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warm_file_cancel_releases_real_read_and_evaluation_permits() {
    assert!(config::is_local_disk_storage());
    let org = "warmup-cancel-fixture";
    let account = format!("{org}:default");
    let store = ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig {
            wait_get_per_call: Duration::from_secs(60),
            ..Default::default()
        },
    );
    infra::storage::add_account(org, Box::new(store)).await;
    let key = "files/warmup-cancel-fixture/logs/test/2026/01/01/00/slow.vix";
    let (data, index) = crate::vix::ranged_parity_tests::build_parity_file();
    let (size, index_size) = (data.len() as i64, index.len() as i64);
    infra::storage::put(&account, key, data).await.unwrap();
    infra::storage::put(&account, &config::vix_sidecar_key(key, 0).unwrap(), index)
        .await
        .unwrap();
    let permits = EVAL_COUNT.available_permits();
    let bytes = SEARCH_FETCH_BYTES.state.lock().used;
    let mut warm = Box::pin(warm_file_outcome(&account, key, size, index_size, 0));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(futures::poll!(&mut warm).is_pending());
            if SEARCH_FETCH_BYTES.state.lock().used > bytes {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("warmup must enter a real ranged read");
    assert_eq!(EVAL_COUNT.available_permits(), permits - 1);
    drop(warm);
    tokio::time::timeout(Duration::from_secs(5), async {
        while EVAL_COUNT.available_permits() != permits
            || SEARCH_FETCH_BYTES.state.lock().used != bytes
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("cancel must release evaluation and range-byte leases");
    assert!(
        crate::vix::reader_cache::GLOBAL_CACHE
            .peek(&crate::vix::reader_cache::ReaderCacheKey::new(
                key.to_owned(),
                0,
                index_size
            ))
            .is_none()
    );
}

#[tokio::test]
async fn warm_file_busy_does_not_queue_for_evaluation_slots() {
    let available = EVAL_COUNT.available_permits();
    let held = Arc::clone(&EVAL_COUNT)
        .acquire_many_owned(available as u32)
        .await
        .unwrap();
    assert_eq!(
        warm_file_outcome(
            "warmup-busy:default",
            "files/warmup-busy/logs/test/2026/01/01/00/busy.vix",
            100,
            100,
            0
        )
        .await
        .unwrap(),
        WarmFileOutcome::Busy
    );
    assert_eq!(EVAL_COUNT.available_permits(), 0);
    drop(held);
    assert_eq!(EVAL_COUNT.available_permits(), available);
}
