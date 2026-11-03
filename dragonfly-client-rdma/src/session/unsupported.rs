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

use super::*;
use std::marker::PhantomData;
use std::rc::Rc;

pub struct Listener {
    _not_send_sync: PhantomData<Rc<()>>,
}
pub struct Session {
    _not_send_sync: PhantomData<Rc<()>>,
}
fn unavailable() -> SessionError {
    crate::ResourceError::Unavailable(if cfg!(target_os = "linux") {
        crate::UnavailableReason::FeatureDisabled
    } else {
        crate::UnavailableReason::UnsupportedPlatform
    })
    .into()
}
impl Listener {
    pub fn bind(options: &SessionOptions) -> Result<Self, SessionError> {
        options.validate()?;
        Err(unavailable())
    }
    pub fn bind_with_budget(
        options: &SessionOptions,
        _: crate::ResourceBudget,
    ) -> Result<Self, SessionError> {
        Self::bind(options)
    }
    pub fn accept_with_timeout(&mut self, _: std::time::Duration) -> Result<Session, SessionError> {
        Err(unavailable())
    }
    pub fn accept(&mut self) -> Result<Session, SessionError> {
        Err(unavailable())
    }
    pub fn try_close(&mut self) -> Result<(), SessionError> {
        Err(unavailable())
    }
}
impl Session {
    pub fn connect(options: &SessionOptions) -> Result<Self, SessionError> {
        options.validate()?;
        Err(unavailable())
    }
    pub fn connect_with_budget(
        options: &SessionOptions,
        _: crate::ResourceBudget,
    ) -> Result<Self, SessionError> {
        Self::connect(options)
    }
    pub fn transaction<T>(
        &mut self,
        _: impl FnOnce(&mut Self) -> Result<T, SessionError>,
    ) -> Result<T, SessionError> {
        Err(unavailable())
    }
    pub(crate) fn transaction_until<T>(
        &mut self,
        _: std::time::Instant,
        _: impl FnOnce(&mut Self) -> Result<T, SessionError>,
    ) -> Result<T, SessionError> {
        Err(unavailable())
    }
    pub fn send(&mut self, _: &[u8]) -> Result<(), SessionError> {
        Err(unavailable())
    }
    pub fn receive(&mut self) -> Result<Vec<u8>, SessionError> {
        Err(unavailable())
    }
    pub fn export(&mut self, _: Vec<u8>) -> Result<RemoteRegion, SessionError> {
        Err(unavailable())
    }
    pub fn reserve_export(&mut self, _: usize) -> Result<ExportBuffer, SessionError> {
        Err(unavailable())
    }
    pub fn export_buffer(&mut self, _: ExportBuffer) -> Result<ExportedRegion, SessionError> {
        Err(unavailable())
    }
    pub fn export_tracked(&mut self, _: Vec<u8>) -> Result<ExportedRegion, SessionError> {
        Err(unavailable())
    }
    pub fn resource_usage(&self) -> Result<crate::ResourceUsage, SessionError> {
        Err(unavailable())
    }
    /// Release a quiescent region on a supported platform.
    ///
    /// # Safety
    /// All peers must have finished accessing the export and must never reuse its
    /// address/key. See Session::release_export on Linux with the rdma feature.
    pub unsafe fn release_export(&mut self, _: ExportId) -> Result<bool, SessionError> {
        Err(unavailable())
    }
    pub fn read(&mut self, _: RemoteRegion) -> Result<Vec<u8>, SessionError> {
        Err(unavailable())
    }
    pub fn try_close(&mut self) -> Result<(), SessionError> {
        Err(unavailable())
    }
}
