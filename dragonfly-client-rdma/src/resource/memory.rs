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

//! Fixed-address allocations stay owned until MR deregistration succeeds.

use super::handle::{self, OwnedHandle};
use super::Domain;
use crate::ffi::raw::*;
use crate::{ResourceError, SessionError};
use std::ffi::c_void;
use std::rc::Rc;

pub(crate) struct RegisteredBuffer {
    inner: Option<BufferInner>,
}
struct BufferInner {
    mr: OwnedHandle,
    data: Box<[u8]>,
    _permit: crate::limits::Permit,
}

impl RegisteredBuffer {
    pub(crate) fn register(
        domain: &Domain,
        data: Vec<u8>,
        key: u64,
        remote_read: bool,
        budget: &crate::ResourceBudget,
    ) -> Result<Self, SessionError> {
        let permit = budget.reserve(data.len())?;
        let mut data = data.into_boxed_slice();
        let raw = domain.inner.as_ptr()?;
        let mr = handle::own(
            vec![Rc::clone(&domain.inner)],
            "MR",
            "fi_mr_reg",
            // SAFETY: data is fixed-address boxed storage. The returned MR
            // retains its Domain and allocation until deregistration succeeds.
            |out| unsafe {
                d7y_rdma_mr_register(
                    raw,
                    data.as_mut_ptr().cast(),
                    data.len(),
                    key,
                    i32::from(remote_read),
                    out,
                )
            },
        )?;
        Ok(Self {
            inner: Some(BufferInner {
                mr,
                data,
                _permit: permit,
            }),
        })
    }

    fn open(&self) -> Result<&BufferInner, ResourceError> {
        self.inner
            .as_ref()
            .ok_or(ResourceError::Closed { resource: "MR" })
    }
    pub(crate) fn data(&self) -> Result<&[u8], ResourceError> {
        Ok(&self.open()?.data)
    }
    pub(crate) fn data_mut(&mut self) -> Result<&mut [u8], ResourceError> {
        Ok(&mut self
            .inner
            .as_mut()
            .ok_or(ResourceError::Closed { resource: "MR" })?
            .data)
    }
    pub(crate) fn pointer(&mut self) -> Result<*mut c_void, ResourceError> {
        Ok(self.data_mut()?.as_mut_ptr().cast())
    }
    pub(crate) fn descriptor(&self) -> Result<*mut c_void, ResourceError> {
        // SAFETY: the live MR and its backing allocation are retained by self.
        Ok(unsafe { d7y_rdma_mr_descriptor(self.open()?.mr.as_ptr()?) })
    }
    pub(crate) fn key(&self) -> Result<u64, ResourceError> {
        // SAFETY: open() rejects closed MRs before reading provider metadata.
        Ok(unsafe { d7y_rdma_mr_key(self.open()?.mr.as_ptr()?) })
    }
    pub(crate) fn address(&self, mode: u64) -> Result<u64, ResourceError> {
        // SAFETY: the allocation stays fixed until deregistration succeeds.
        Ok(unsafe { d7y_rdma_mr_address(mode, self.data()?.as_ptr().cast()) })
    }
    pub(crate) fn try_close(&mut self) -> Result<(), ResourceError> {
        if let Some(inner) = self.inner.as_mut() {
            handle::try_close(&mut inner.mr)?;
        }
        self.inner.take();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fail_close_for_test(&mut self, fail: bool) {
        handle::fail_close_for_test(&mut self.inner.as_mut().unwrap().mr, fail);
    }
}

impl Drop for RegisteredBuffer {
    fn drop(&mut self) {
        if let Err(error) = self.try_close() {
            tracing::error!(%error, "MR deregistration failed; retaining registered allocation");
            // A live registration must never outlive its allocation. Keep both,
            // including their Domain dependency, on this exceptional path.
            if let Some(inner) = self.inner.as_ref() {
                inner._permit.cleanup_failed();
            }
            std::mem::forget(self.inner.take());
        }
    }
}
