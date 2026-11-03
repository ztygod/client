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

#[cfg(not(all(target_os = "linux", feature = "rdma")))]
use super::unavailable;
#[cfg(all(target_os = "linux", feature = "rdma"))]
use super::{
    handle::{self, Handle},
    CqOptions, Domain,
};
use crate::ResourceError;
#[cfg(all(target_os = "linux", feature = "rdma"))]
use std::rc::Rc;

#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(crate) mod operation {
    use crate::error::check_diagnostic;
    use crate::ffi::raw::*;
    use crate::resource::memory::RegisteredBuffer;
    use crate::{CompletionQueue, SessionError};
    use std::ffi::c_void;
    use std::ptr::{self, NonNull};

    #[derive(Clone, Copy)]
    pub(crate) enum Kind {
        Send = 0,
        Receive = 1,
        Read = 2,
    }

    impl Kind {
        pub(crate) fn name(self) -> &'static str {
            match self {
                Self::Send => "send",
                Self::Receive => "receive",
                Self::Read => "read",
            }
        }
    }

    // The provider may write to struct fi_context. Allocate it in C with the
    // correct alignment/layout, and keep its address stable until completion.
    pub(crate) struct Context(NonNull<c_void>);
    impl Context {
        pub(crate) fn new() -> Result<Self, SessionError> {
            NonNull::new(unsafe { d7y_rdma_context_alloc() })
                .map(Self)
                .ok_or_else(|| {
                    crate::ResourceError::Allocation {
                        resource: "operation context",
                    }
                    .into()
                })
        }
        pub(crate) fn pointer(&self) -> *mut c_void {
            self.0.as_ptr()
        }
    }
    impl Drop for Context {
        fn drop(&mut self) {
            unsafe { d7y_rdma_context_free(self.0.as_ptr()) };
        }
    }

    pub(crate) struct PendingOperation {
        pub(crate) buffer: RegisteredBuffer,
        pub(crate) context: Context,
        pub(crate) kind: Kind,
    }

    pub(crate) struct Completion {
        pub(crate) context: *mut c_void,
        pub(crate) length: usize,
        pub(crate) flags: u64,
    }

    impl CompletionQueue {
        pub(crate) fn poll(&self) -> Result<Option<Completion>, SessionError> {
            let mut context = ptr::null_mut();
            let (mut length, mut flags, mut provider_error) = (0, 0, 0);
            let mut diagnostic = [0; 512];
            let code = unsafe {
                d7y_rdma_cq_poll(
                    self.inner.as_ptr()?,
                    &mut context,
                    &mut length,
                    &mut flags,
                    &mut provider_error,
                    diagnostic.as_mut_ptr(),
                    diagnostic.len(),
                )
            };
            check_diagnostic("fi_cq_read/readerr", code, provider_error, &diagnostic)?;
            Ok((code == 1).then_some(Completion {
                context,
                length,
                flags,
            }))
        }
    }
}

/// An owned completion queue. No raw handle or polling API is exposed yet.
pub struct CompletionQueue {
    #[cfg(all(target_os = "linux", feature = "rdma"))]
    pub(crate) inner: handle::OwnedHandle,
    #[cfg(not(all(target_os = "linux", feature = "rdma")))]
    _not_send_sync: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl CompletionQueue {
    /// Reports native close failures and retains ownership for retry.
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
pub(super) fn open_cq(
    domain: &Domain,
    options: CqOptions,
) -> Result<CompletionQueue, ResourceError> {
    let raw = domain.inner.as_ptr()?;
    let api = domain.inner.api;
    let inner = Handle::create(
        Some(Rc::clone(&domain.inner)),
        None,
        api,
        "CQ",
        "fi_cq_open",
        |result| {
            // SAFETY: domain is live and retained by the resulting completion queue.
            unsafe { (api.cq_open)(raw, options.size, result) }
        },
    )?;
    Ok(CompletionQueue { inner })
}
