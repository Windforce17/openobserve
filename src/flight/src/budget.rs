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

//! Per-response admission for encoded Arrow IPC groups, independent of the execution pool.
//!
//! Reserve an upper bound BEFORE encoding. Reserving only after encoding leaves one unbounded
//! encoded group in every waiting producer. The permit follows the payload's `Bytes` ownership,
//! including clones, rather than being released when an item leaves the channel.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow_flight::{FlightData, error::FlightError};
use bytes::Bytes;
use config::metrics;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Eight of the existing 32 MiB IPC slice targets. This is per response, not per partition.
/// It limits prefetch without coupling independent follower streams needed by a leader merge.
pub const FLIGHT_DATA_BUDGET: usize = 256 * 1024 * 1024;

/// Control messages have a separate lane so an early partial-error message can precede a data
/// group that already owns the whole data budget. Produce at most one control message at a time;
/// oversized explain/metrics messages use the same exclusive-message rule as oversized data.
pub const FLIGHT_CONTROL_BUDGET: usize = 1024 * 1024;

static RETAINED: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct FlightDataBudget {
    limit: usize,
    semaphore: Arc<Semaphore>,
    retained: AtomicUsize,
    peak: AtomicUsize,
}

impl Default for FlightDataBudget {
    fn default() -> Self {
        Self::new(FLIGHT_DATA_BUDGET)
    }
}

impl FlightDataBudget {
    pub fn new(limit: usize) -> Self {
        assert!(limit > 0 && limit <= u32::MAX as usize);
        Self {
            limit,
            semaphore: Arc::new(Semaphore::new(limit)),
            retained: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    /// An oversized group acquires the entire response budget, and therefore can run only
    /// after all earlier groups have been consumed. It must not wait for impossible >limit permits.
    /// Call this after obtaining an input chunk: never hold a permit while polling the plan.
    pub async fn reserve(self: &Arc<Self>, upper_bound: usize) -> EncodingReservation {
        let charged = upper_bound.min(self.limit);
        let permit = Arc::clone(&self.semaphore)
            .acquire_many_owned(charged as u32)
            .await
            .expect("the per-response Flight semaphore is never closed");
        metrics::FLIGHT_ENCODE_RESERVED_BYTES.add(charged as i64);
        EncodingReservation {
            budget: Arc::clone(self),
            permit,
            charged,
            upper_bound,
        }
    }

    /// Payload lengths, not allocator capacity or the downstream tonic/prost/HTTP2 copies.
    pub fn retained_bytes(&self) -> usize {
        self.retained.load(Ordering::Relaxed)
    }

    pub fn peak_bytes(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
}

pub struct EncodingReservation {
    budget: Arc<FlightDataBudget>,
    permit: OwnedSemaphorePermit,
    charged: usize,
    upper_bound: usize,
}

impl EncodingReservation {
    /// Transfer the reservation to the actual bytes, releasing any pessimistic headroom.
    /// Keeping the group atomic also keeps dictionary updates contiguous on the wire.
    pub fn attach(mut self, mut group: Vec<FlightData>) -> Result<Vec<FlightData>, FlightError> {
        let bytes = group.iter().fold(0usize, |total, data| {
            total.saturating_add(flight_data_size(data))
        });
        if bytes > self.upper_bound {
            // A future Arrow encoding change must fail closed, not silently bypass admission.
            return Err(FlightError::protocol(format!(
                "Flight IPC size {bytes} exceeds encoding reservation bound {}",
                self.upper_bound
            )));
        }
        let charged = bytes.min(self.budget.limit);
        let excess = self.charged - charged;
        drop(self.permit.split(excess));
        metrics::FLIGHT_ENCODE_RESERVED_BYTES.sub(excess as i64);
        self.charged = charged;

        let local = self.budget.retained.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.budget.peak.fetch_max(local, Ordering::Relaxed);
        metrics::FLIGHT_ENCODED_BYTES.add(bytes as i64);
        let total = RETAINED.fetch_add(bytes, Ordering::Relaxed) + bytes;
        let old_peak = PEAK.fetch_max(total, Ordering::Relaxed);
        if total > old_peak {
            // Addition, not set: concurrent high-water updates cannot overwrite a newer peak.
            metrics::FLIGHT_ENCODED_PEAK_BYTES.add((total - old_peak) as i64);
        }
        let lease = Arc::new(EncodedLease {
            reservation: self,
            bytes,
        });
        for data in &mut group {
            for buffer in [
                &mut data.data_header,
                &mut data.data_body,
                &mut data.app_metadata,
            ] {
                if !buffer.is_empty() {
                    *buffer = Bytes::from_owner(LeasedBytes {
                        bytes: std::mem::take(buffer),
                        _lease: Arc::clone(&lease),
                    });
                }
            }
        }
        Ok(group)
    }
}

impl Drop for EncodingReservation {
    fn drop(&mut self) {
        metrics::FLIGHT_ENCODE_RESERVED_BYTES.sub(self.charged as i64);
    }
}

struct EncodedLease {
    reservation: EncodingReservation,
    bytes: usize,
}

impl Drop for EncodedLease {
    fn drop(&mut self) {
        self.reservation
            .budget
            .retained
            .fetch_sub(self.bytes, Ordering::Relaxed);
        RETAINED.fetch_sub(self.bytes, Ordering::Relaxed);
        metrics::FLIGHT_ENCODED_BYTES.sub(self.bytes as i64);
    }
}

struct LeasedBytes {
    bytes: Bytes,
    _lease: Arc<EncodedLease>,
}

impl AsRef<[u8]> for LeasedBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

pub fn flight_data_size(data: &FlightData) -> usize {
    data.data_header
        .len()
        .saturating_add(data.data_body.len())
        .saturating_add(data.app_metadata.len())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::FutureExt;

    use super::*;

    fn group(bytes: usize) -> Vec<FlightData> {
        vec![FlightData::new().with_data_body(vec![1; bytes])]
    }

    #[tokio::test]
    async fn clones_retain_admission_until_the_payload_is_dropped() {
        let budget = Arc::new(FlightDataBudget::new(100));
        let data = budget.reserve(100).await.attach(group(80)).unwrap();
        let cloned = data[0].data_body.clone();
        drop(data);
        assert_eq!(budget.retained_bytes(), 80);
        assert!(budget.reserve(21).now_or_never().is_none());
        drop(cloned);
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(budget.peak_bytes(), 80);
        let reservation = budget.reserve(100).await;
        drop(reservation);
        assert_eq!(budget.semaphore.available_permits(), 100);
    }

    #[tokio::test]
    async fn oversized_group_is_exclusive_and_makes_progress() {
        let budget = Arc::new(FlightDataBudget::new(100));
        let first = budget.reserve(30).await.attach(group(30)).unwrap();
        let waiting = budget.reserve(500);
        tokio::pin!(waiting);
        assert!(waiting.as_mut().now_or_never().is_none());
        drop(first);
        let large = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .attach(group(500))
            .unwrap();
        assert_eq!(budget.retained_bytes(), 500);
        assert!(budget.reserve(1).now_or_never().is_none());
        drop(large);
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(budget.semaphore.available_permits(), 100);
    }

    #[tokio::test]
    async fn error_and_cancelled_waiter_release_all_permits() {
        let budget = Arc::new(FlightDataBudget::new(100));
        assert!(budget.reserve(10).await.attach(group(11)).is_err());
        let data = budget.reserve(100).await.attach(group(100)).unwrap();
        assert!(budget.reserve(100).now_or_never().is_none());
        drop(data);
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(budget.semaphore.available_permits(), 100);
        drop(budget.reserve(100).await); // a cancelled FIFO waiter cannot strand permits
    }

    #[tokio::test]
    async fn panic_releases_encoding_reservation() {
        let budget = Arc::new(FlightDataBudget::new(100));
        let producer_budget = Arc::clone(&budget);
        let task = tokio::spawn(async move {
            let _reservation = producer_budget.reserve(100).await;
            panic!("encoder failed after admission");
        });
        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(budget.semaphore.available_permits(), 100);
        drop(budget.reserve(100).await);
    }
}
