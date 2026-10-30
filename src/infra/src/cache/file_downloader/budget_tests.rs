// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use futures::poll;

use super::*;

fn fair_admission(bytes: usize) -> Arc<DownloadAdmission> {
    let mut admission = DownloadAdmission::new(bytes, 100);
    admission.fair = true;
    Arc::new(admission)
}

#[tokio::test]
async fn budget_fifo_large_and_small_cannot_starve() {
    let admission = fair_admission(100);
    let active = admission.reserve("active", 60).unwrap();
    active.activate().await.unwrap();
    let large = admission.reserve("large", 100).unwrap();
    let mut large_wait = Box::pin(large.activate());
    assert!(poll!(&mut large_wait).is_pending());
    let small = admission.reserve("small", 1).unwrap();
    let mut small_wait = Box::pin(small.activate());
    for _ in 0..100 {
        assert!(
            poll!(&mut small_wait).is_pending(),
            "small must not bypass the older large waiter"
        );
    }
    drop(active);
    large_wait.await.unwrap();
    assert_eq!(admission.state.lock().active_bytes, 100);
    assert!(poll!(&mut small_wait).is_pending());
    drop(large);
    small_wait.await.unwrap();
    assert_eq!(admission.state.lock().active_bytes, 1);
    // A later large request cannot bypass the earlier small request either.
    let later = admission.reserve("later", 100).unwrap();
    let mut later_wait = Box::pin(later.activate());
    assert!(poll!(&mut later_wait).is_pending());
    drop(small);
    later_wait.await.unwrap();
    drop(later);
    assert_eq!(admission.state.lock().active_bytes, 0);
    assert_eq!(admission.permits.available_permits(), 100);
}

#[tokio::test]
async fn budget_cancelled_head_removes_ticket_and_releases_dedupe() {
    let admission = fair_admission(100);
    let active = admission.reserve("active", 60).unwrap();
    active.activate().await.unwrap();
    let head = admission.reserve("head", 100).unwrap();
    let mut head_wait = Box::pin(head.activate());
    assert!(poll!(&mut head_wait).is_pending());
    let small = admission.reserve("small", 40).unwrap();
    let mut small_wait = Box::pin(small.activate());
    assert!(poll!(&mut small_wait).is_pending());
    drop(head_wait);
    drop(head);
    small_wait.await.unwrap();
    drop(admission.reserve("head", 100).unwrap());
    drop(active);
    drop(small);
    assert!(admission.state.lock().files.is_empty());
    assert_eq!(admission.permits.available_permits(), 100);
}

#[tokio::test]
async fn budget_close_wakes_waiters_and_refuses_new_keys() {
    let admission = fair_admission(100);
    let active = admission.reserve("active", 100).unwrap();
    active.activate().await.unwrap();
    let queued = admission.reserve("queued", 1).unwrap();
    let mut waiting = Box::pin(queued.activate());
    assert!(poll!(&mut waiting).is_pending());
    admission.close();
    assert_eq!(waiting.await, Err(DownloadRejection::Closed));
    assert!(matches!(
        admission.reserve("new", 1),
        Err(QueueDownloadOutcome::Rejected(DownloadRejection::Closed))
    ));
    drop(active);
    drop(queued);
    assert_eq!(admission.state.lock().active_bytes, 0);
    assert!(admission.state.lock().files.is_empty());
}

#[tokio::test]
async fn budget_separates_object_size_and_buffer_charge() {
    let admission = fair_admission(32);
    let large = admission
        .reserve_charged("large", 4096, 32, DownloadClass::QueryBulk, 0)
        .unwrap();
    large.activate().await.unwrap();
    assert_eq!(large.bytes, 4096);
    assert_eq!(admission.state.lock().active_bytes, 32);
    assert!(matches!(
        admission.reserve_charged("limited", 4096, 32, DownloadClass::QueryBulk, 4095),
        Err(QueueDownloadOutcome::Rejected(DownloadRejection::TooLarge))
    ));
    assert!(matches!(
        admission.reserve_charged("over-buffer", 4096, 33, DownloadClass::QueryBulk, 0),
        Err(QueueDownloadOutcome::Rejected(DownloadRejection::TooLarge))
    ));
    drop(large);
    assert_eq!(admission.state.lock().active_bytes, 0);
}

#[test]
fn budget_charge_matches_io_and_keeps_memory_peer_and_rollback_exact() {
    let size = 2 * 1024 * 1024 * 1024;
    let expected = crate::cache::disk_download_in_flight_bytes(size as u64) as usize;
    assert_eq!(
        admission_charge(size, file_data::CacheType::Disk, true, false),
        expected
    );
    assert_eq!(
        admission_charge(size, file_data::CacheType::Disk, false, false),
        size
    );
    assert_eq!(
        admission_charge(size, file_data::CacheType::Memory, true, false),
        size
    );
    assert_eq!(
        admission_charge(size, file_data::CacheType::Disk, true, true),
        size
    );
    assert_eq!(
        admission_charge(1, file_data::CacheType::Disk, true, false),
        1
    );
}

#[tokio::test]
async fn budget_repeated_activation_is_idempotent() {
    let admission = fair_admission(100);
    let item = admission.reserve("one", 100).unwrap();
    let (a, b) = tokio::join!(item.activate(), item.activate());
    a.unwrap();
    b.unwrap();
    assert_eq!(admission.state.lock().active_bytes, 100);
    drop(item);
    assert_eq!(admission.permits.available_permits(), 100);
}
