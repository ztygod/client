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

use std::ffi::{c_char, c_int, c_void};

/// Stable snapshot shared with c/probe.h; all pointers borrow from an InfoList.
#[repr(C)]
#[derive(Default)]
pub(crate) struct RawInfo {
    pub(super) provider: *const c_char,
    pub(super) domain: *const c_char,
    pub(super) device: *const c_char,
    pub(super) capabilities: u64,
    pub(super) mode: u64,
    pub(super) mr_mode: u64,
    pub(super) protocol: u32,
    pub(super) protocol_version: u32,
    pub(super) threading: u32,
    pub(super) control_progress: u32,
    pub(super) data_progress: u32,
    pub(super) messaging: u8,
    pub(super) rma: u8,
    pub(super) read: u8,
    pub(super) remote_read: u8,
    pub(super) remote_communication: u8,
}

/// Stable CQ batch ABI shared with c/transfer.h.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(crate) struct RawCompletion {
    pub(crate) context: *mut c_void,
    pub(crate) length: usize,
    pub(crate) flags: u64,
}

unsafe extern "C" {
    pub(crate) fn d7y_rdma_fabric_open(info: *const c_void, result: *mut *mut c_void) -> c_int;
    pub(crate) fn d7y_rdma_domain_open(
        fabric: *mut c_void,
        info: *const c_void,
        result: *mut *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_cq_open(
        domain: *mut c_void,
        size: usize,
        result: *mut *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_close(resource: *mut c_void) -> c_int;
    pub(crate) fn d7y_rdma_session_query(
        provider: *const c_char,
        domain: *const c_char,
        node: *const c_char,
        service: *const c_char,
        source: c_int,
        result: *mut *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_session_validate(info: *const c_void) -> c_int;
    pub(crate) fn d7y_rdma_session_max_size(info: *const c_void) -> usize;
    pub(crate) fn d7y_rdma_session_mr_mode(info: *const c_void) -> u64;
    pub(crate) fn d7y_rdma_session_dest(info: *const c_void) -> *mut c_void;
    pub(crate) fn d7y_rdma_eq_open(fabric: *mut c_void, result: *mut *mut c_void) -> c_int;
    pub(crate) fn d7y_rdma_pep_open(
        fabric: *mut c_void,
        info: *const c_void,
        result: *mut *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_pep_listen(pep: *mut c_void, eq: *mut c_void) -> c_int;
    pub(crate) fn d7y_rdma_ep_open(
        domain: *mut c_void,
        info: *const c_void,
        result: *mut *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_ep_bind_enable(
        ep: *mut c_void,
        eq: *mut c_void,
        tx: *mut c_void,
        rx: *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_ep_connect(ep: *mut c_void, address: *const c_void) -> c_int;
    pub(crate) fn d7y_rdma_ep_accept(ep: *mut c_void) -> c_int;
    pub(crate) fn d7y_rdma_ep_shutdown(ep: *mut c_void) -> c_int;
    pub(crate) fn d7y_rdma_pep_reject(pep: *mut c_void, info: *const c_void) -> c_int;
    pub(crate) fn d7y_rdma_eq_poll(
        eq: *mut c_void,
        event: *mut u32,
        fid: *mut *mut c_void,
        info: *mut *mut c_void,
        provider_error: *mut c_int,
        diagnostic: *mut c_char,
        diagnostic_size: usize,
    ) -> c_int;
    pub(crate) fn d7y_rdma_is_shutdown(event: u32) -> c_int;
    pub(crate) fn d7y_rdma_is_connected(event: u32) -> c_int;
    pub(crate) fn d7y_rdma_is_connreq(event: u32) -> c_int;
    pub(crate) fn d7y_rdma_mr_register(
        domain: *mut c_void,
        data: *mut c_void,
        size: usize,
        key: u64,
        remote_read: c_int,
        result: *mut *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_mr_descriptor(mr: *mut c_void) -> *mut c_void;
    pub(crate) fn d7y_rdma_mr_key(mr: *mut c_void) -> u64;
    pub(crate) fn d7y_rdma_mr_address(mode: u64, data: *const c_void) -> u64;
    pub(crate) fn d7y_rdma_context_alloc() -> *mut c_void;
    pub(crate) fn d7y_rdma_context_free(context: *mut c_void);
    pub(crate) fn d7y_rdma_post_send(
        ep: *mut c_void,
        data: *mut c_void,
        size: usize,
        desc: *mut c_void,
        context: *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_post_recv(
        ep: *mut c_void,
        data: *mut c_void,
        size: usize,
        desc: *mut c_void,
        context: *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_post_read(
        ep: *mut c_void,
        data: *mut c_void,
        size: usize,
        desc: *mut c_void,
        address: u64,
        key: u64,
        context: *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_cq_poll(
        cq: *mut c_void,
        context: *mut *mut c_void,
        length: *mut usize,
        flags: *mut u64,
        provider_error: *mut c_int,
        diagnostic: *mut c_char,
        diagnostic_size: usize,
    ) -> c_int;
    pub(crate) fn d7y_rdma_cq_poll_many(
        cq: *mut c_void,
        entries: *mut RawCompletion,
        capacity: usize,
        provider_error: *mut c_int,
        diagnostic: *mut c_char,
        diagnostic_size: usize,
    ) -> c_int;
    pub(crate) fn d7y_rdma_is_again(code: c_int) -> c_int;
    pub(crate) fn d7y_rdma_completion_matches(flags: u64, operation: c_int) -> c_int;
    pub(crate) fn d7y_rdma_probe(
        provider: *const c_char,
        domain: *const c_char,
        result: *mut *mut c_void,
    ) -> c_int;
    pub(crate) fn d7y_rdma_free_info(result: *mut c_void);
    pub(crate) fn d7y_rdma_next_info(info: *const c_void) -> *mut c_void;
    pub(crate) fn d7y_rdma_get_info(info: *const c_void, result: *mut RawInfo) -> c_int;
    pub(crate) fn d7y_rdma_error_string(code: c_int) -> *const c_char;
    pub(crate) fn d7y_rdma_dup_info(info: *const c_void) -> *mut c_void;
}
