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

use super::{CompletionQueue, CqOptions, Domain, Fabric};
use crate::ffi::{error_message, select_candidate, SelectedInfo};
use crate::{ProbeOptions, RdmaCapability, ResourceError};
use std::ffi::{c_int, c_void};
use std::ptr::{self, NonNull};
use std::rc::Rc;

type OpenFabric = unsafe extern "C" fn(*const c_void, *mut *mut c_void) -> c_int;
type OpenDomain = unsafe extern "C" fn(*mut c_void, *const c_void, *mut *mut c_void) -> c_int;
type OpenCq = unsafe extern "C" fn(*mut c_void, usize, *mut *mut c_void) -> c_int;
type Close = unsafe extern "C" fn(*mut c_void) -> c_int;

unsafe extern "C" {
    fn d7y_rdma_fabric_open(info: *const c_void, result: *mut *mut c_void) -> c_int;
    fn d7y_rdma_domain_open(
        fabric: *mut c_void,
        info: *const c_void,
        result: *mut *mut c_void,
    ) -> c_int;
    fn d7y_rdma_cq_open(domain: *mut c_void, size: usize, result: *mut *mut c_void) -> c_int;
    fn d7y_rdma_close(resource: *mut c_void) -> c_int;
}

/// A private function table also lets tests inject allocation/close failures.
#[derive(Clone, Copy)]
struct Api {
    fabric_open: OpenFabric,
    domain_open: OpenDomain,
    cq_open: OpenCq,
    close: Close,
}

const NATIVE: Api = Api {
    fabric_open: d7y_rdma_fabric_open,
    domain_open: d7y_rdma_domain_open,
    cq_open: d7y_rdma_cq_open,
    close: d7y_rdma_close,
};

pub(super) type OwnedHandle = Rc<Handle>;

/// No Clone or raw-handle access is exposed to callers. A child owns a strong
/// parent reference, so even dropping public wrappers in the wrong order is safe.
pub(super) struct Handle {
    raw: Option<NonNull<c_void>>,
    parent: Option<OwnedHandle>,
    info: Option<SelectedInfo>,
    api: Api,
    kind: &'static str,
}

fn native_error(operation: &'static str, code: c_int) -> ResourceError {
    ResourceError::Libfabric {
        operation,
        code,
        message: error_message(code),
    }
}

impl Handle {
    fn create(
        parent: Option<OwnedHandle>,
        info: Option<SelectedInfo>,
        api: Api,
        kind: &'static str,
        operation: &'static str,
        open: impl FnOnce(*mut *mut c_void) -> c_int,
    ) -> Result<OwnedHandle, ResourceError> {
        let mut raw = ptr::null_mut();
        let code = open(&mut raw);
        if code != 0 {
            return Err(native_error(operation, code));
        }
        let raw = NonNull::new(raw).ok_or(crate::ProbeError::InvalidResult(
            "resource creation succeeded without returning a handle",
        ))?;
        Ok(Rc::new(Self {
            raw: Some(raw),
            parent,
            info,
            api,
            kind,
        }))
    }

    fn as_ptr(&self) -> Result<*mut c_void, ResourceError> {
        self.raw.map(NonNull::as_ptr).ok_or(ResourceError::Closed {
            resource: self.kind,
        })
    }

    fn close(&mut self) -> Result<(), ResourceError> {
        if let Some(raw) = self.raw {
            // SAFETY: this is the unique owner, and no dependent resource remains.
            let code = unsafe { (self.api.close)(raw.as_ptr()) };
            if code != 0 {
                return Err(native_error("fi_close", code));
            }
            self.raw = None;
            // Closed children must no longer keep parents busy.
            self.parent = None;
            self.info = None;
        }
        Ok(())
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            // Drop cannot report failure to the caller. Keep the dependency chain
            // alive if the provider fails to release a child; closing its parent
            // would violate libfabric's lifetime contract. No panic during unwind.
            tracing::error!(resource = self.kind, %error, "libfabric resource cleanup failed");
            if let Some(parent) = self.parent.take() {
                std::mem::forget(parent);
            }
        }
    }
}

pub(super) fn try_close(handle: &mut OwnedHandle) -> Result<(), ResourceError> {
    let kind = handle.kind;
    Rc::get_mut(handle)
        .ok_or(ResourceError::InUse { resource: kind })?
        .close()
}

pub(super) fn open_fabric(candidate: &RdmaCapability) -> Result<Fabric, ResourceError> {
    ProbeOptions {
        provider: candidate.provider.clone(),
        domain: Some(candidate.domain.clone()),
    }
    .validate()?;
    from_info(select_candidate(candidate)?, NATIVE)
}

fn from_info(info: SelectedInfo, api: Api) -> Result<Fabric, ResourceError> {
    let raw_info = info.as_ptr();
    let inner = Handle::create(None, Some(info), api, "Fabric", "fi_fabric", |result| {
        // SAFETY: info is the deep-copied, live query result, output is writable.
        unsafe { (api.fabric_open)(raw_info, result) }
    })?;
    Ok(Fabric { inner })
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProbeResult, RdmaResources, ResourceOptions};
    use std::cell::RefCell;

    fn tcp_candidate() -> RdmaCapability {
        let ProbeResult::Available(candidates) = crate::ffi::probe(&ProbeOptions {
            provider: "tcp".to_owned(),
            domain: None,
        })
        .unwrap() else {
            panic!("libfabric TCP provider is required for resource tests");
        };
        candidates.into_iter().next().unwrap()
    }

    #[test]
    fn real_provider_resources_close_and_reject_use_after_close() {
        let candidate = tcp_candidate();
        // The public transport is verbs only; internal tests exercise the same
        // ownership code with real TCP provider allocations, without RDMA hardware.
        for _ in 0..3 {
            let mut fabric = from_info(select_candidate(&candidate).unwrap(), NATIVE).unwrap();
            let mut domain = fabric.open_domain().unwrap();
            let mut tx = domain
                .open_completion_queue(CqOptions { size: 64 })
                .unwrap();
            let mut rx = domain
                .open_completion_queue(CqOptions { size: 64 })
                .unwrap();
            assert!(matches!(
                fabric.try_close(),
                Err(ResourceError::InUse { resource: "Fabric" })
            ));
            assert!(matches!(
                domain.try_close(),
                Err(ResourceError::InUse { resource: "Domain" })
            ));
            tx.try_close().unwrap();
            tx.try_close().unwrap();
            rx.try_close().unwrap();
            domain.try_close().unwrap();
            assert!(matches!(
                domain.open_completion_queue(CqOptions::default()),
                Err(ResourceError::Closed { resource: "Domain" })
            ));
            fabric.try_close().unwrap();
            assert!(matches!(
                fabric.open_domain(),
                Err(ResourceError::Closed { resource: "Fabric" })
            ));
        }
    }

    #[test]
    fn changed_candidate_is_rejected_before_allocating_resources() {
        let mut candidate = tcp_candidate();
        candidate.protocol_version ^= 1;
        assert!(matches!(
            select_candidate(&candidate),
            Err(ResourceError::CandidateUnavailable)
        ));
        let original = tcp_candidate();
        assert!(matches!(
            Fabric::open(&original),
            Err(ResourceError::Probe(_))
        ));
    }

    #[derive(Default)]
    struct State {
        calls: usize,
        fail_at: Option<usize>,
        close_calls: usize,
        fail_close_at: Option<usize>,
        live: Vec<usize>,
        events: Vec<String>,
    }

    thread_local! {
        static STATE: RefCell<State> = RefCell::new(State::default());
    }

    struct FakeHandle {
        id: usize,
        parent: Option<usize>,
    }

    unsafe fn allocate(parent: *mut c_void, result: *mut *mut c_void) -> c_int {
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.calls += 1;
            let id = state.calls;
            if state.fail_at == Some(id) {
                return -5;
            }
            // SAFETY: fake API receives a live handle allocated by this test.
            let parent = unsafe { parent.cast::<FakeHandle>().as_ref() }.map(|h| h.id);
            state.live.push(id);
            state.events.push(format!("open {id}"));
            let handle = Box::into_raw(Box::new(FakeHandle { id, parent }));
            // SAFETY: the caller supplies a writable output pointer.
            unsafe { *result = handle.cast() };
            0
        })
    }

    unsafe extern "C" fn fake_fabric(_: *const c_void, result: *mut *mut c_void) -> c_int {
        // SAFETY: forwards the output pointer supplied by Handle::create.
        unsafe { allocate(ptr::null_mut(), result) }
    }

    unsafe extern "C" fn fake_domain(
        parent: *mut c_void,
        _: *const c_void,
        result: *mut *mut c_void,
    ) -> c_int {
        // SAFETY: forwards a live fake fabric and writable output pointer.
        unsafe { allocate(parent, result) }
    }

    unsafe extern "C" fn fake_cq(parent: *mut c_void, _: usize, result: *mut *mut c_void) -> c_int {
        // SAFETY: forwards a live fake domain and writable output pointer.
        unsafe { allocate(parent, result) }
    }

    unsafe extern "C" fn fake_close(raw: *mut c_void) -> c_int {
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.close_calls += 1;
            if state.fail_close_at == Some(state.close_calls) {
                return -5;
            }
            // SAFETY: raw is uniquely owned and was allocated by the fake API.
            let handle = unsafe { Box::from_raw(raw.cast::<FakeHandle>()) };
            assert!(
                state.live.contains(&handle.id),
                "resource must be closed only once"
            );
            if let Some(parent) = handle.parent {
                assert!(state.live.contains(&parent), "parent must outlive child");
            }
            state.live.retain(|id| *id != handle.id);
            state.events.push(format!("close {}", handle.id));
            0
        })
    }

    const FAKE: Api = Api {
        fabric_open: fake_fabric,
        domain_open: fake_domain,
        cq_open: fake_cq,
        close: fake_close,
    };

    fn fake_fabric_owner(candidate: &RdmaCapability) -> Result<Fabric, ResourceError> {
        from_info(select_candidate(candidate)?, FAKE)
    }

    #[test]
    fn partial_construction_rolls_back_at_every_allocation_stage() {
        let candidate = tcp_candidate();
        for failure in 1..=4 {
            STATE.with(|s| {
                *s.borrow_mut() = State {
                    fail_at: Some(failure),
                    ..State::default()
                }
            });
            let result = fake_fabric_owner(&candidate)
                .and_then(|fabric| RdmaResources::from_fabric(fabric, ResourceOptions::default()));
            assert!(result.is_err());
            STATE.with(|s| {
                let s = s.borrow();
                assert!(
                    s.live.is_empty(),
                    "failed stage {failure} leaked a resource"
                );
                let expected: Vec<_> = (1..failure)
                    .map(|id| format!("open {id}"))
                    .chain((1..failure).rev().map(|id| format!("close {id}")))
                    .collect();
                assert_eq!(s.events, expected);
            });
        }
    }

    #[test]
    fn children_keep_dropped_parents_alive_until_the_last_queue_closes() {
        STATE.with(|s| *s.borrow_mut() = State::default());
        let fabric = fake_fabric_owner(&tcp_candidate()).unwrap();
        let domain = fabric.open_domain().unwrap();
        let tx = domain.open_completion_queue(CqOptions::default()).unwrap();
        let rx = domain.open_completion_queue(CqOptions::default()).unwrap();
        drop(fabric);
        drop(domain);
        STATE.with(|s| assert_eq!(s.borrow().live, [1, 2, 3, 4]));
        drop(tx);
        STATE.with(|s| assert_eq!(s.borrow().live, [1, 2, 4]));
        drop(rx);
        STATE.with(|s| {
            let s = s.borrow();
            assert!(s.live.is_empty());
            assert_eq!(
                s.events,
                [
                    "open 1", "open 2", "open 3", "open 4", "close 3", "close 4", "close 2",
                    "close 1"
                ]
            );
        });
    }

    #[test]
    fn explicit_close_can_retry_failure_without_double_close() {
        for failure in 1..=4 {
            STATE.with(|s| *s.borrow_mut() = State::default());
            let fabric = fake_fabric_owner(&tcp_candidate()).unwrap();
            let mut resources =
                RdmaResources::from_fabric(fabric, ResourceOptions::default()).unwrap();
            STATE.with(|s| s.borrow_mut().fail_close_at = Some(failure));
            assert!(matches!(
                resources.try_close(),
                Err(ResourceError::Libfabric {
                    operation: "fi_close",
                    ..
                })
            ));
            let expected_live: &[usize] = match failure {
                1 => &[1, 2, 3, 4],
                2 => &[1, 2, 4],
                3 => &[1, 2],
                _ => &[1],
            };
            STATE.with(|s| assert_eq!(s.borrow().live, expected_live));
            resources.try_close().unwrap();
            resources.try_close().unwrap();
            drop(resources);
            STATE.with(|s| {
                let s = s.borrow();
                assert!(s.live.is_empty());
                assert_eq!(
                    s.events,
                    [
                        "open 1", "open 2", "open 3", "open 4", "close 3", "close 4", "close 2",
                        "close 1"
                    ]
                );
            });
        }
    }

    #[test]
    fn panic_unwinding_releases_all_owned_resources() {
        STATE.with(|s| *s.borrow_mut() = State::default());
        let result = std::panic::catch_unwind(|| {
            let fabric = fake_fabric_owner(&tcp_candidate()).unwrap();
            let _resources =
                RdmaResources::from_fabric(fabric, ResourceOptions::default()).unwrap();
            panic!("simulate caller unwinding");
        });
        assert!(result.is_err());
        STATE.with(|s| {
            let s = s.borrow();
            assert!(s.live.is_empty());
            assert_eq!(&s.events[4..], ["close 3", "close 4", "close 2", "close 1"]);
        });
    }
}
