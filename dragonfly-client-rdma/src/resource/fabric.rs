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

#[cfg(all(target_os = "linux", feature = "rdma"))]
use super::handle::{self, Api, Handle, NATIVE};
#[cfg(not(all(target_os = "linux", feature = "rdma")))]
use super::unavailable;
#[cfg(all(target_os = "linux", feature = "rdma"))]
use crate::{
    ffi::{raw::*, select_candidate, SelectedInfo},
    ProbeOptions,
};
use crate::{CompletionQueue, CqOptions, RdmaCapability, ResourceError};
#[cfg(all(target_os = "linux", feature = "rdma"))]
use std::rc::Rc;

/// An owned fabric. Open domains keep the underlying fabric alive.
pub struct Fabric {
    #[cfg(all(target_os = "linux", feature = "rdma"))]
    pub(crate) inner: handle::OwnedHandle,
    #[cfg(not(all(target_os = "linux", feature = "rdma")))]
    _not_send_sync: std::marker::PhantomData<std::rc::Rc<()>>,
}

/// An owned domain. Open completion queues keep the underlying domain alive.
pub struct Domain {
    #[cfg(all(target_os = "linux", feature = "rdma"))]
    pub(crate) inner: handle::OwnedHandle,
    #[cfg(not(all(target_os = "linux", feature = "rdma")))]
    _not_send_sync: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl Fabric {
    /// Re-queries and matches the entire candidate metadata snapshot before opening.
    ///
    /// Retains the selected fi_info, including provider-owned addressing details,
    /// for subsequent domain creation. A stale or edited candidate is rejected.
    pub fn open(candidate: &RdmaCapability) -> Result<Self, ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            open_fabric(candidate)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            let _ = candidate;
            Err(unavailable())
        }
    }

    pub fn open_domain(&self) -> Result<Domain, ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            open_domain(self)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            Err(unavailable())
        }
    }

    /// Closes once all domains have closed. On failure the handle remains owned
    /// and can be retried. Repeated successful closes are harmless.
    pub fn try_close(&mut self) -> Result<(), ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            handle::try_close(&mut self.inner)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            Err(unavailable())
        }
    }
}

impl Domain {
    pub fn open_completion_queue(
        &self,
        options: CqOptions,
    ) -> Result<CompletionQueue, ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            super::completion::open_cq(self, options)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            let _ = options;
            Err(unavailable())
        }
    }

    /// Fails with InUse while completion queues are live; preserves failed handles.
    pub fn try_close(&mut self) -> Result<(), ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            handle::try_close(&mut self.inner)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            Err(unavailable())
        }
    }
}

#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(super) fn open_fabric(candidate: &RdmaCapability) -> Result<Fabric, ResourceError> {
    ProbeOptions {
        provider: candidate.provider.clone(),
        domain: Some(candidate.domain.clone()),
    }
    .validate()?;
    from_info(select_candidate(candidate)?, NATIVE)
}

#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(super) fn from_info(info: SelectedInfo, api: Api) -> Result<Fabric, ResourceError> {
    let raw_info = info.as_ptr();
    let inner = Handle::create(None, Some(info), api, "Fabric", "fi_fabric", |result| {
        // SAFETY: info is the deep-copied, live query result, output is writable.
        unsafe { (api.fabric_open)(raw_info, result) }
    })?;
    Ok(Fabric { inner })
}

/// Connection queries carry addresses and accept requests carry a CM handle.
/// Keep their complete native info instead of reconstructing it from a snapshot.
#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(crate) fn fabric_from_info(info: SelectedInfo) -> Result<Fabric, ResourceError> {
    from_info(info, NATIVE)
}

#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(crate) fn domain_from_info(
    fabric: &Fabric,
    info: SelectedInfo,
) -> Result<Domain, ResourceError> {
    let raw = fabric.inner.as_ptr()?;
    let raw_info = info.as_ptr();
    let inner = Handle::create(
        Some(Rc::clone(&fabric.inner)),
        Some(info),
        NATIVE,
        "Domain",
        "fi_domain",
        |result| unsafe { d7y_rdma_domain_open(raw, raw_info, result) },
    )?;
    Ok(Domain { inner })
}

#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(super) fn open_domain(fabric: &Fabric) -> Result<Domain, ResourceError> {
    let raw = fabric.inner.as_ptr()?;
    let info = fabric
        .inner
        .info
        .as_ref()
        .expect("an open fabric retains the selected fi_info")
        .as_ptr();
    let api = fabric.inner.api;
    let inner = Handle::create(
        Some(Rc::clone(&fabric.inner)),
        None,
        api,
        "Domain",
        "fi_domain",
        |result| {
            // SAFETY: the parent and query result are retained for this call and the child.
            unsafe { (api.domain_open)(raw, info, result) }
        },
    )?;
    Ok(Domain { inner })
}
