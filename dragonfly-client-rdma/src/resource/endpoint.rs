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

//! CM resource wrappers. Endpoints retain every bound queue, not just the domain.

use super::handle::{self, OwnedHandle};
use super::{CompletionQueue, Domain, Fabric};
use crate::error::{check, check_diagnostic};
use crate::ffi::{raw::*, SelectedInfo};
use crate::SessionError;
use std::ffi::c_void;
use std::ptr;
use std::rc::Rc;

pub(crate) struct EventQueue {
    pub(crate) handle: OwnedHandle,
}

pub(crate) struct CmEvent {
    pub(crate) event: u32,
    pub(crate) fid: *mut c_void,
    pub(crate) info: Option<SelectedInfo>,
}

impl EventQueue {
    pub(crate) fn open(fabric: &Fabric) -> Result<Self, SessionError> {
        let raw = fabric.inner.as_ptr()?;
        let handle = handle::own(
            vec![Rc::clone(&fabric.inner)],
            "EQ",
            "fi_eq_open",
            |out| unsafe { d7y_rdma_eq_open(raw, out) },
        )?;
        Ok(Self { handle })
    }

    pub(crate) fn poll(&self) -> Result<Option<CmEvent>, SessionError> {
        let (mut event, mut provider_error) = (0, 0);
        let (mut fid, mut info) = (ptr::null_mut(), ptr::null_mut());
        let mut diagnostic = [0; 512];
        let code = unsafe {
            d7y_rdma_eq_poll(
                self.handle.as_ptr()?,
                &mut event,
                &mut fid,
                &mut info,
                &mut provider_error,
                diagnostic.as_mut_ptr(),
                diagnostic.len(),
            )
        };
        let info = (!info.is_null()).then(|| unsafe { SelectedInfo::from_raw(info) });
        check_diagnostic("fi_eq_read/readerr", code, provider_error, &diagnostic)?;
        Ok((code == 1).then_some(CmEvent { event, fid, info }))
    }
}

pub(crate) struct PassiveEndpoint {
    pub(crate) handle: OwnedHandle,
}

impl PassiveEndpoint {
    pub(crate) fn listen(
        fabric: &Fabric,
        eq: &EventQueue,
        info: &SelectedInfo,
    ) -> Result<Self, SessionError> {
        let raw = fabric.inner.as_ptr()?;
        let handle = handle::own(
            vec![Rc::clone(&fabric.inner), Rc::clone(&eq.handle)],
            "PassiveEndpoint",
            "fi_passive_ep",
            |out| unsafe { d7y_rdma_pep_open(raw, info.as_ptr(), out) },
        )?;
        check(
            "fi_pep_bind/listen",
            unsafe { d7y_rdma_pep_listen(handle.as_ptr()?, eq.handle.as_ptr()?) },
            0,
        )?;
        Ok(Self { handle })
    }

    pub(crate) fn reject(&self, info: &SelectedInfo) {
        if let Ok(raw) = self.handle.as_ptr() {
            let code = unsafe { d7y_rdma_pep_reject(raw, info.as_ptr()) };
            if code < 0 {
                tracing::warn!(code, "rejecting RDMA connection failed");
            }
        }
    }
}

pub(crate) struct Endpoint {
    pub(crate) handle: OwnedHandle,
}

impl Endpoint {
    pub(crate) fn open(
        domain: &Domain,
        eq: &EventQueue,
        tx: &CompletionQueue,
        rx: &CompletionQueue,
        info: &SelectedInfo,
    ) -> Result<Self, SessionError> {
        let raw = domain.inner.as_ptr()?;
        let dependencies = vec![
            Rc::clone(&domain.inner),
            Rc::clone(&eq.handle),
            Rc::clone(&tx.inner),
            Rc::clone(&rx.inner),
        ];
        let handle = handle::own(dependencies, "Endpoint", "fi_endpoint", |out| unsafe {
            d7y_rdma_ep_open(raw, info.as_ptr(), out)
        })?;
        Ok(Self { handle })
    }

    pub(crate) fn bind_enable(
        &self,
        eq: &EventQueue,
        tx: &CompletionQueue,
        rx: &CompletionQueue,
    ) -> Result<(), SessionError> {
        check(
            "fi_ep_bind/enable",
            unsafe {
                d7y_rdma_ep_bind_enable(
                    self.handle.as_ptr()?,
                    eq.handle.as_ptr()?,
                    tx.inner.as_ptr()?,
                    rx.inner.as_ptr()?,
                )
            },
            0,
        )
    }
}
