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

//! Export identities never repeat within a process, including across connections. Released identities
//! need no tombstones: every issued generation was inserted successfully.

use crate::{ResourceBudget, SessionError};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Application protocol identity. This is not an RDMA access key or revocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExportId {
    pub session: u64,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExportedRegion {
    pub id: ExportId,
    pub region: super::RemoteRegion,
}

/// Unpublished, budget-reserved allocation. Fill before exporting; dropping it
/// releases its reservation. It contains no native handles and may cross threads.
#[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
pub struct ExportBuffer {
    pub(crate) session: u64,
    pub(crate) data: Box<[u8]>,
    pub(crate) permit: crate::limits::Permit,
}
impl ExportBuffer {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
    #[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
    pub(crate) fn allocate(
        session: u64,
        length: usize,
        budget: &ResourceBudget,
    ) -> Result<Self, SessionError> {
        let permit = budget.reserve(length)?;
        let data = budget.take_export_buffer(length)?;
        Ok(Self {
            session,
            data,
            permit,
        })
    }
}

#[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
pub(crate) struct Exports<T> {
    session: u64,
    issued: u64,
    live: BTreeMap<u64, T>,
}
#[cfg_attr(not(all(target_os = "linux", feature = "rdma")), allow(dead_code))]
impl<T> Exports<T> {
    pub(crate) fn new() -> Result<Self, SessionError> {
        static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
        let session = NEXT_SESSION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| SessionError::Protocol("export session identity exhausted"))?;
        Ok(Self {
            session,
            issued: 0,
            live: BTreeMap::new(),
        })
    }
    pub(crate) fn session(&self) -> u64 {
        self.session
    }
    pub(crate) fn next_id(&self) -> Result<ExportId, SessionError> {
        Ok(ExportId {
            session: self.session,
            generation: self
                .issued
                .checked_add(1)
                .ok_or(SessionError::Protocol("export generation exhausted"))?,
        })
    }
    pub(crate) fn insert(&mut self, value: T) -> Result<ExportId, SessionError> {
        let id = self.next_id()?;
        self.live.insert(id.generation, value);
        self.issued = id.generation;
        Ok(id)
    }
    pub(crate) fn get_mut(&mut self, id: ExportId) -> Result<Option<&mut T>, SessionError> {
        if id.session != self.session || id.generation == 0 || id.generation > self.issued {
            return Err(SessionError::Protocol("unknown export identity"));
        }
        Ok(self.live.get_mut(&id.generation))
    }
    pub(crate) fn remove(&mut self, id: ExportId) {
        self.live.remove(&id.generation);
    }
    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.live.values_mut()
    }
    pub(crate) fn clear(&mut self) {
        self.live.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_and_cross_connection_releases_cannot_remove_a_new_export() {
        let mut exports = Exports::new().unwrap();
        let first = exports.insert(1).unwrap();
        exports.remove(first);
        let next = exports.insert(2).unwrap();
        assert!(exports.get_mut(first).unwrap().is_none());
        assert_eq!(exports.get_mut(next).unwrap(), Some(&mut 2));
        let mut other = Exports::new().unwrap();
        let foreign = other.insert(3).unwrap();
        assert!(exports.get_mut(foreign).is_err());
        assert!(exports
            .get_mut(ExportId {
                generation: next.generation + 1,
                ..next
            })
            .is_err());
        assert!(exports
            .get_mut(ExportId {
                generation: 0,
                ..next
            })
            .is_err());
    }
    #[test]
    fn repeated_release_tracking_and_cancelled_reservations_are_bounded() {
        let mut exports = Exports::new().unwrap();
        let budget = ResourceBudget::new(crate::Limits::default()).unwrap();
        for _ in 0..10_000 {
            let buffer = ExportBuffer::allocate(exports.session(), 32, &budget).unwrap();
            let id = exports.insert(buffer).unwrap();
            exports.remove(id);
        }
        assert!(exports.live.is_empty());
        assert_eq!(budget.usage().registered_bytes, 0);
        assert_eq!(budget.usage().registered_regions, 0);
        exports.issued = u64::MAX;
        assert!(exports.next_id().is_err());
    }
}
