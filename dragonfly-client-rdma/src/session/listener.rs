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

use super::connection::Session;
use super::progress::Progress;
use super::*;
use crate::error::check;
use crate::ffi::query_session as query;
use crate::ffi::raw::*;
use crate::resource::endpoint::{EventQueue, PassiveEndpoint};
use crate::resource::handle;
use crate::{Fabric, ResourceBudget};
use std::rc::Rc;
use std::time::Instant;

/// One listening endpoint. Each accepted Session gets its own EQ and CQs.
pub struct Listener {
    pep: PassiveEndpoint,
    eq: EventQueue,
    fabric: Fabric,
    options: SessionOptions,
    budget: ResourceBudget,
}

impl Listener {
    pub fn bind(options: &SessionOptions) -> Result<Self, SessionError> {
        options.validate()?;
        Self::bind_with_budget(options, ResourceBudget::new(options.limits.clone())?)
    }

    pub fn bind_with_budget(
        options: &SessionOptions,
        budget: ResourceBudget,
    ) -> Result<Self, SessionError> {
        options.validate()?;
        Self::bind_native(options, budget)
    }

    pub(crate) fn bind_native(
        options: &SessionOptions,
        budget: ResourceBudget,
    ) -> Result<Self, SessionError> {
        let info = query(options, true)?;
        let fabric = crate::resource::fabric::fabric_from_info(info.duplicate()?)?;
        let eq = EventQueue::open(&fabric)?;
        let pep = PassiveEndpoint::listen(&fabric, &eq, &info)?;
        Ok(Self {
            pep,
            eq,
            fabric,
            options: options.clone(),
            budget,
        })
    }

    pub fn accept(&mut self) -> Result<Session, SessionError> {
        self.accept_with_timeout(self.options.timeout)
    }

    /// Bound idle listener waits separately from accepted session transactions.
    pub fn accept_with_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Session, SessionError> {
        if timeout.is_zero() || timeout > std::time::Duration::from_secs(600) {
            return Err(SessionError::InvalidOptions(
                "accept timeout must be in (0, 600] seconds",
            ));
        }
        let deadline = Instant::now() + timeout;
        let mut progress = Progress::default();
        loop {
            if let Some(event) = self.eq.poll()? {
                if unsafe { d7y_rdma_is_connreq(event.event) } == 0
                    || event.fid != self.pep.handle.as_ptr()?
                {
                    return Err(SessionError::UnexpectedEvent {
                        expected: "FI_CONNREQ on this listener",
                        event: event.event,
                    });
                }
                let info = event
                    .info
                    .ok_or(SessionError::Protocol("connection request has no fi_info"))?;
                let mut endpoint_opened = false;
                let result = (|| {
                    let permit = self.budget.session()?;
                    check(
                        "validate accepted fi_info",
                        unsafe { d7y_rdma_session_validate(info.as_ptr()) },
                        0,
                    )?;
                    let fabric = Fabric {
                        inner: Rc::clone(&self.fabric.inner),
                    };
                    // The domain and endpoint use this request's CM info.
                    let mut session = Session::build(
                        fabric,
                        &info,
                        self.options.timeout,
                        self.budget.clone(),
                        permit,
                        &mut endpoint_opened,
                    )?;
                    session.accept_connection(deadline)?;
                    Ok(session)
                })();
                // Once an endpoint owns the CM request, its close path owns
                // cleanup. Never reject a request after accept/EP teardown.
                if result.is_err() && !endpoint_opened {
                    self.pep.reject(&info);
                }
                return result;
            }
            progress.wait(deadline, "accept")?;
        }
    }

    /// May report InUse while accepted sessions still retain the fabric.
    pub fn try_close(&mut self) -> Result<(), SessionError> {
        handle::try_close(&mut self.pep.handle)?;
        handle::try_close(&mut self.eq.handle)?;
        self.fabric.try_close()?;
        Ok(())
    }
}
