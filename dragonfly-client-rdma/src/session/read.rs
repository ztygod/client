/*
 *     Copyright 2026 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Read admission, native-free completed leases and bounded completion tracking.

use crate::{Limits, SessionError};
use std::ops::Deref;
use std::sync::Arc;

pub(crate) const MAX_READ_WINDOW: usize = 64;
pub(crate) const MAX_READ_BATCH: usize = 65_536;

/// Bounds submitted Reads. Registered/leased buffers have separate pool limits.
///
/// ```no_run
/// use dragonfly_client_rdma::{ReadOptions, RemoteRegion, SessionError, SessionWorker};
/// # async fn example(worker: &mut SessionWorker, regions: Vec<RemoteRegion>) -> Result<(), SessionError> {
/// let mut stream = worker.read_stream(regions, ReadOptions::default()).await?;
/// while let Some(completed) = stream.next().await? {
///     // Consume or hand off this native-free lease, then drop it to return capacity.
///     std::hint::black_box((completed.index, completed.data.as_slice()));
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct ReadOptions {
    pub max_in_flight: usize,
    pub max_in_flight_bytes: usize,
}
impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            max_in_flight: 4,
            max_in_flight_bytes: 64 * 1024 * 1024,
        }
    }
}
impl ReadOptions {
    pub(crate) fn validate(&self, limits: &Limits) -> Result<(), SessionError> {
        if self.max_in_flight == 0
            || self.max_in_flight
                > MAX_READ_WINDOW
                    .min(limits.max_read_buffers)
                    .min(limits.completion_queue_size)
            || self.max_in_flight_bytes == 0
            || self.max_in_flight_bytes > limits.max_read_buffer_bytes
        {
            return Err(SessionError::InvalidOptions(
                "read window exceeds pool or CQ bounds",
            ));
        }
        Ok(())
    }
}

// The permit follows storage, including completed leases that outlive a Session.
// No native MR/context/Domain handle crosses the worker boundary.
pub(crate) struct ReadStorage {
    pub(crate) data: Box<[u8]>,
    pub(crate) _permit: crate::limits::Permit,
}

/// Immutable completed data. Clone/Send/Sync do not copy the payload. The owner
/// cannot repost this allocation until every clone is dropped. Holding leases
/// consumes pool capacity and applies backpressure to subsequent Reads.
#[derive(Clone)]
pub struct CompletedRead {
    storage: Arc<ReadStorage>,
    length: usize,
}
impl CompletedRead {
    #[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
    pub(crate) fn new(storage: Arc<ReadStorage>, length: usize) -> Result<Self, SessionError> {
        if length > storage.data.len() {
            return Err(SessionError::Protocol("read exceeds allocation"));
        }
        Ok(Self { storage, length })
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.storage.data[..self.length]
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}
impl Deref for CompletedRead {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl AsRef<[u8]> for CompletedRead {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl std::fmt::Debug for CompletedRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletedRead")
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

/// A batch completion; index refers to the original region list, not CQ order.
#[derive(Debug)]
pub struct ReadCompletion {
    pub index: usize,
    pub data: CompletedRead,
}

#[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
pub(crate) struct ReadEntry<T> {
    pub(crate) context: usize,
    pub(crate) length: usize,
    pub(crate) value: T,
}
#[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
pub(crate) struct ReadWindow<T> {
    pub(crate) entries: Vec<ReadEntry<T>>,
    bytes: usize,
    options: ReadOptions,
}
#[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
impl<T> ReadWindow<T> {
    pub(crate) fn new(options: ReadOptions) -> Result<Self, SessionError> {
        let mut window = Self {
            entries: Vec::new(),
            bytes: 0,
            options,
        };
        window.prepare(options)?;
        Ok(window)
    }
    pub(crate) fn prepare(&mut self, options: ReadOptions) -> Result<(), SessionError> {
        if !self.entries.is_empty() || self.bytes != 0 {
            return Err(SessionError::Protocol("cannot reset pending Read window"));
        }
        self.entries
            .try_reserve_exact(options.max_in_flight)
            .map_err(|_| SessionError::Allocation {
                bytes: options
                    .max_in_flight
                    .saturating_mul(std::mem::size_of::<ReadEntry<T>>()),
            })?;
        self.options = options;
        Ok(())
    }
    pub(crate) fn can_push(&self, length: usize) -> bool {
        self.entries.len() < self.options.max_in_flight
            && self
                .bytes
                .checked_add(length)
                .is_some_and(|n| n <= self.options.max_in_flight_bytes)
    }
    pub(crate) fn push(
        &mut self,
        context: usize,
        length: usize,
        value: T,
    ) -> Result<(), SessionError> {
        if context == 0 || self.entries.iter().any(|e| e.context == context) {
            return Err(SessionError::Protocol("duplicate read context"));
        }
        if !self.can_push(length) {
            return Err(SessionError::Protocol("read window overflow"));
        }
        self.bytes += length;
        self.entries.push(ReadEntry {
            context,
            length,
            value,
        });
        Ok(())
    }
    pub(crate) fn take(&mut self, context: usize) -> Result<ReadEntry<T>, SessionError> {
        let index = self
            .entries
            .iter()
            .position(|e| e.context == context)
            .ok_or(SessionError::Protocol("unknown read context"))?;
        let entry = self.entries.swap_remove(index);
        self.bytes -= entry.length;
        Ok(entry)
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceBudget;
    #[test]
    fn out_of_order_completions_refill_both_window_limits() {
        let mut window = ReadWindow::new(ReadOptions {
            max_in_flight: 3,
            max_in_flight_bytes: 10,
        })
        .unwrap();
        window.push(1, 4, "first").unwrap();
        window.push(2, 6, "second").unwrap();
        assert!(!window.can_push(1));
        assert_eq!(window.take(2).unwrap().value, "second");
        assert!(window.can_push(6));
        window.push(3, 3, "third").unwrap();
        window.push(4, 3, "fourth").unwrap();
        assert!(!window.can_push(0));
        assert!(window.push(4, 1, "duplicate").is_err());
        assert!(window.take(99).is_err());
        assert_eq!(window.take(1).unwrap().value, "first");
        assert_eq!(window.take(4).unwrap().value, "fourth");
        assert_eq!(window.take(3).unwrap().value, "third");
        assert_eq!(window.bytes, 0);
        assert!(window.is_empty());
    }
    #[test]
    fn completed_leases_hold_budget_across_threads_and_owner_shutdown() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CompletedRead>();
        let budget = ResourceBudget::new(Limits::default()).unwrap();
        let mut storage = Arc::new(ReadStorage {
            data: vec![7; 16].into_boxed_slice(),
            _permit: budget.reserve(16).unwrap(),
        });
        let lease = CompletedRead::new(Arc::clone(&storage), 8).unwrap();
        assert!(Arc::get_mut(&mut storage).is_none());
        assert_eq!(lease.as_slice().as_ptr(), storage.data.as_ptr());
        let other = lease.clone();
        drop(lease);
        drop(storage); // Simulate successful native close: data still belongs to leases.
        assert_eq!(budget.usage().registered_bytes, 16);
        std::thread::spawn(move || assert_eq!(&*other, &[7; 8]))
            .join()
            .unwrap();
        assert_eq!(budget.usage().registered_bytes, 0);
        assert_eq!(budget.usage().registered_regions, 0);
    }
    #[test]
    fn failed_completion_lookup_and_window_reset_retain_pending_owners() {
        struct Owner(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Owner {
            fn drop(&mut self) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut window = ReadWindow::new(ReadOptions::default()).unwrap();
        window.push(1, 8, Owner(Arc::clone(&dropped))).unwrap();
        assert!(window.take(99).is_err());
        assert!(window.prepare(ReadOptions::default()).is_err());
        assert_eq!(dropped.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(window.bytes, 8);
        drop(window); // Equivalent to releasing the retained bundle after EP close.
        assert_eq!(dropped.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
    #[test]
    fn window_options_reject_unbounded_count_and_bytes() {
        let limits = Limits::default();
        ReadOptions::default().validate(&limits).unwrap();
        for options in [
            ReadOptions {
                max_in_flight: 0,
                ..ReadOptions::default()
            },
            ReadOptions {
                max_in_flight: usize::MAX,
                ..ReadOptions::default()
            },
            ReadOptions {
                max_in_flight_bytes: 0,
                ..ReadOptions::default()
            },
            ReadOptions {
                max_in_flight_bytes: usize::MAX,
                ..ReadOptions::default()
            },
        ] {
            assert!(options.validate(&limits).is_err());
        }
    }
}
