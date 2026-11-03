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

//! Shared admission limits. Permits follow resources, including retained failures.

use crate::SessionError;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Upper bounds for a listener or worker and every session it owns.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Maximum live sessions, including bundles retained after close failure.
    pub max_sessions: usize,
    /// Maximum total registered allocation size across the shared budget.
    pub max_registered_bytes: usize,
    /// Also bound native registration metadata for many small exports.
    pub max_registered_regions: usize,
    /// Stop admitting sessions after this many unrecoverable cleanup failures.
    pub max_cleanup_failures: usize,
    pub max_transfer_size: usize,
    pub max_message_size: usize,
    pub completion_queue_size: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_sessions: 32,
            max_registered_bytes: 128 * 1024 * 1024,
            max_registered_regions: 1024,
            max_cleanup_failures: 1,
            max_transfer_size: 64 * 1024 * 1024,
            max_message_size: 4096,
            completion_queue_size: 64,
        }
    }
}
impl Limits {
    pub(crate) fn validate(&self) -> Result<(), SessionError> {
        if self.max_sessions == 0
            || self.max_registered_bytes == 0
            || self.max_registered_regions == 0
            || self.max_cleanup_failures == 0
            || self.max_transfer_size == 0
            || self.max_transfer_size > 64 * 1024 * 1024
            || self.max_message_size == 0
            || self.max_message_size > 64 * 1024
            || self.completion_queue_size == 0
        {
            return Err(SessionError::InvalidOptions("invalid resource limits"));
        }
        if self
            .max_message_size
            .checked_mul(2)
            .is_none_or(|n| n > self.max_registered_bytes)
        {
            return Err(SessionError::InvalidOptions(
                "budget must fit both control buffers",
            ));
        }
        Ok(())
    }
}

/// Shared counters, retained by resource permits even after an owner is dropped.
#[derive(Clone)]
pub struct ResourceBudget(Arc<BudgetInner>);
struct BudgetInner {
    limits: Limits,
    sessions: AtomicUsize,
    registered_bytes: AtomicUsize,
    registered_regions: AtomicUsize,
    cleanup_failures: AtomicUsize,
}
/// Best-effort snapshot of shared counters. Cleanup failures are cumulative.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceUsage {
    pub sessions: usize,
    pub registered_bytes: usize,
    pub registered_regions: usize,
    pub cleanup_failures: usize,
}
impl ResourceBudget {
    pub fn new(limits: Limits) -> Result<Self, SessionError> {
        limits.validate()?;
        Ok(Self(Arc::new(BudgetInner {
            limits,
            sessions: AtomicUsize::new(0),
            registered_bytes: AtomicUsize::new(0),
            registered_regions: AtomicUsize::new(0),
            cleanup_failures: AtomicUsize::new(0),
        })))
    }
    pub fn limits(&self) -> &Limits {
        &self.0.limits
    }
    pub fn usage(&self) -> ResourceUsage {
        ResourceUsage {
            sessions: self.0.sessions.load(Ordering::Relaxed),
            registered_bytes: self.0.registered_bytes.load(Ordering::Relaxed),
            registered_regions: self.0.registered_regions.load(Ordering::Relaxed),
            cleanup_failures: self.0.cleanup_failures.load(Ordering::Relaxed),
        }
    }
    #[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
    pub(crate) fn reserve(&self, bytes: usize) -> Result<Permit, SessionError> {
        let mut permit = self.acquire(bytes, false)?;
        let limit = self.0.limits.max_registered_regions;
        self.0
            .registered_regions
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(1).filter(|next| *next <= limit)
            })
            .map_err(|used| SessionError::ResourceLimit {
                resource: "registered regions",
                requested: 1,
                available: limit.saturating_sub(used),
            })?;
        permit.registration = true;
        Ok(permit)
    }
    #[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
    pub(crate) fn session(&self) -> Result<Permit, SessionError> {
        if self.0.cleanup_failures.load(Ordering::Acquire) >= self.0.limits.max_cleanup_failures {
            return Err(SessionError::ResourceLimit {
                resource: "cleanup failures",
                requested: 1,
                available: 0,
            });
        }
        self.acquire(1, true)
    }
    fn acquire(&self, amount: usize, session: bool) -> Result<Permit, SessionError> {
        let (counter, limit, resource) = if session {
            (&self.0.sessions, self.0.limits.max_sessions, "sessions")
        } else {
            (
                &self.0.registered_bytes,
                self.0.limits.max_registered_bytes,
                "registered bytes",
            )
        };
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount).filter(|next| *next <= limit)
            })
            .map_err(|used| SessionError::ResourceLimit {
                resource,
                requested: amount,
                available: limit.saturating_sub(used),
            })?;
        Ok(Permit {
            budget: self.clone(),
            amount,
            session,
            registration: false,
        })
    }
    #[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
    pub(crate) fn cleanup_failed(&self) {
        self.0.cleanup_failures.fetch_add(1, Ordering::Relaxed);
    }
}

/// Owned by the MR allocation or session bundle; only released after safe cleanup.
pub(crate) struct Permit {
    budget: ResourceBudget,
    amount: usize,
    session: bool,
    registration: bool,
}
impl Permit {
    #[cfg(all(target_os = "linux", feature = "rdma"))]
    pub(crate) fn cleanup_failed(&self) {
        self.budget.cleanup_failed();
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let counter = if self.session {
            &self.budget.0.sessions
        } else {
            &self.budget.0.registered_bytes
        };
        counter.fetch_sub(self.amount, Ordering::AcqRel);
        if self.registration {
            self.budget
                .0
                .registered_regions
                .fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_is_shared_and_released_by_the_last_owner() {
        let budget = ResourceBudget::new(Limits {
            max_sessions: 1,
            max_registered_bytes: 10,
            max_message_size: 1,
            ..Limits::default()
        })
        .unwrap();
        let session = budget.session().unwrap();
        assert!(budget.clone().session().is_err());
        let bytes = budget.reserve(8).unwrap();
        assert!(budget.reserve(3).is_err());
        assert!(budget.reserve(usize::MAX).is_err());
        assert_eq!(budget.usage().registered_bytes, 8);
        drop(bytes);
        drop(session);
        assert_eq!(budget.usage().sessions, 0);
        assert_eq!(budget.usage().registered_bytes, 0);
    }
    #[test]
    fn native_registration_count_and_cleanup_failures_stop_admission() {
        let budget = ResourceBudget::new(Limits {
            max_registered_regions: 1,
            ..Limits::default()
        })
        .unwrap();
        let permit = budget.reserve(1).unwrap();
        assert!(matches!(
            budget.reserve(1),
            Err(SessionError::ResourceLimit {
                resource: "registered regions",
                ..
            })
        ));
        assert_eq!(budget.usage().registered_bytes, 1);
        drop(permit);
        assert_eq!(budget.usage().registered_regions, 0);
        budget.cleanup_failed();
        assert!(matches!(
            budget.session(),
            Err(SessionError::ResourceLimit {
                resource: "cleanup failures",
                ..
            })
        ));
    }
    #[test]
    fn concurrent_admission_never_exceeds_the_limit() {
        let budget = ResourceBudget::new(Limits::default()).unwrap();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let b = budget.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        let _permit = b.reserve(1024).unwrap();
                        assert!(b.usage().registered_bytes <= b.limits().max_registered_bytes);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(budget.usage().registered_bytes, 0);
    }
}
