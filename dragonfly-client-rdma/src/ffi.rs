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

use crate::{
    EndpointType, ProbeError, ProbeOptions, ProbeResult, RdmaCapability, ReadCapabilities,
    ResourceError, UnavailableReason,
};
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

/// Stable snapshot shared with c/probe.h; all pointers borrow from an InfoList.
#[repr(C)]
#[derive(Default)]
struct RawInfo {
    provider: *const c_char,
    domain: *const c_char,
    device: *const c_char,
    capabilities: u64,
    mode: u64,
    mr_mode: u64,
    protocol: u32,
    protocol_version: u32,
    threading: u32,
    control_progress: u32,
    data_progress: u32,
    messaging: u8,
    rma: u8,
    read: u8,
    remote_read: u8,
    remote_communication: u8,
}

unsafe extern "C" {
    fn d7y_rdma_probe(
        provider: *const c_char,
        domain: *const c_char,
        result: *mut *mut c_void,
    ) -> c_int;
    fn d7y_rdma_free_info(result: *mut c_void);
    fn d7y_rdma_next_info(info: *const c_void) -> *mut c_void;
    fn d7y_rdma_get_info(info: *const c_void, result: *mut RawInfo) -> c_int;
    fn d7y_rdma_error_string(code: c_int) -> *const c_char;
    fn d7y_rdma_dup_info(info: *const c_void) -> *mut c_void;
}

/// Owns the entire fi_info list, including when conversion exits early.
struct InfoList(*mut c_void);

impl Drop for InfoList {
    fn drop(&mut self) {
        // SAFETY: the shim returned this list and accepts NULL; only this guard frees it.
        unsafe { d7y_rdma_free_info(self.0) };
    }
}

fn libfabric_error(operation: &'static str, code: c_int) -> ProbeError {
    ProbeError::Libfabric {
        operation,
        code,
        message: error_message(code),
    }
}

pub(crate) fn error_message(code: c_int) -> String {
    // SAFETY: fi_strerror returns a library-owned NUL-terminated static string.
    unsafe {
        let message = d7y_rdma_error_string(code);
        if message.is_null() {
            "unknown libfabric error".to_owned()
        } else {
            CStr::from_ptr(message).to_string_lossy().into_owned()
        }
    }
}

/// Copies a nullable string while its parent fi_info list is still alive.
unsafe fn copy_string(value: *const c_char) -> Option<String> {
    if value.is_null() {
        None
    } else {
        // SAFETY: caller guarantees a live NUL-terminated string from the shim.
        Some(
            unsafe { CStr::from_ptr(value) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}

fn query(options: &ProbeOptions) -> Result<InfoList, ProbeError> {
    let to_c_string = |field, value: &str| {
        CString::new(value).map_err(|_| ProbeError::InvalidOptions {
            field,
            reason: "must contain no NUL",
        })
    };
    let provider = to_c_string("provider", &options.provider)?;
    let domain = options
        .domain
        .as_deref()
        .map(|domain| to_c_string("domain", domain))
        .transpose()?;
    let mut head = ptr::null_mut();
    // SAFETY: strings and the output pointer remain valid throughout the synchronous call.
    let code = unsafe {
        d7y_rdma_probe(
            provider.as_ptr(),
            domain
                .as_ref()
                .map_or(ptr::null(), |domain| domain.as_ptr()),
            &mut head,
        )
    };
    let list = InfoList(head);
    if code != 0 {
        return Err(libfabric_error("fi_getinfo", code));
    }

    Ok(list)
}

fn visit_candidates(
    options: &ProbeOptions,
    mut visit: impl FnMut(*const c_void, RdmaCapability) -> Result<(), ProbeError>,
) -> Result<(), ProbeError> {
    let list = query(options)?;
    let mut cursor = list.0;
    while !cursor.is_null() {
        let mut raw = RawInfo::default();
        // SAFETY: cursor points into the live list; raw matches the shim's C layout.
        let code = unsafe { d7y_rdma_get_info(cursor, &mut raw) };
        if code != 0 {
            return Err(libfabric_error("decode fi_info", code));
        }
        // SAFETY: cursor points into the list owned by the guard.
        let entry = cursor;
        cursor = unsafe { d7y_rdma_next_info(cursor) };

        // SAFETY: all strings are copied before list is dropped.
        let (provider, domain, device) = unsafe {
            (
                copy_string(raw.provider),
                copy_string(raw.domain),
                copy_string(raw.device),
            )
        };
        let provider = provider
            .filter(|value| !value.is_empty())
            .ok_or(ProbeError::InvalidResult("missing provider name"))?;
        let domain = domain
            .filter(|value| !value.is_empty())
            .ok_or(ProbeError::InvalidResult("missing domain name"))?;
        // Providers may return extra interfaces despite domain hints. Enforce selection here.
        if !provider.split(';').any(|name| name == options.provider)
            || options
                .domain
                .as_deref()
                .is_some_and(|requested| domain != requested)
        {
            continue;
        }
        let capabilities = ReadCapabilities {
            messaging: raw.messaging != 0,
            rma: raw.rma != 0,
            read: raw.read != 0,
            remote_read: raw.remote_read != 0,
            remote_communication: raw.remote_communication != 0,
        };
        if !capabilities.is_supported() {
            return Err(ProbeError::InvalidResult(
                "provider returned an interface without all requested capabilities",
            ));
        }

        visit(
            entry,
            RdmaCapability {
                provider,
                domain,
                device: device.filter(|value| !value.is_empty()),
                endpoint_type: EndpointType::Message,
                capabilities,
                raw_capabilities: raw.capabilities,
                mode: raw.mode,
                mr_mode: raw.mr_mode,
                protocol: raw.protocol,
                protocol_version: raw.protocol_version,
                threading: raw.threading,
                control_progress: raw.control_progress,
                data_progress: raw.data_progress,
            },
        )?;
    }
    Ok(())
}

pub(crate) fn probe(options: &ProbeOptions) -> Result<ProbeResult, ProbeError> {
    let mut candidates = Vec::new();
    visit_candidates(options, |_, candidate| {
        candidates.push(candidate);
        Ok(())
    })?;
    if candidates.is_empty() {
        Ok(ProbeResult::Unavailable(
            UnavailableReason::NoMatchingInterface,
        ))
    } else {
        Ok(ProbeResult::Available(candidates))
    }
}

/// Owns a single fi_dupinfo result, including addresses absent from the public snapshot.
pub(crate) struct SelectedInfo {
    list: InfoList,
}

impl SelectedInfo {
    pub(crate) fn as_ptr(&self) -> *const c_void {
        self.list.0
    }
}

pub(crate) fn select_candidate(candidate: &RdmaCapability) -> Result<SelectedInfo, ResourceError> {
    let options = ProbeOptions {
        provider: candidate.provider.clone(),
        domain: Some(candidate.domain.clone()),
    };
    let mut selected = None;
    visit_candidates(&options, |entry, actual| {
        if selected.is_none() && actual == *candidate {
            // SAFETY: entry belongs to the live query list. fi_dupinfo deep-copies one entry.
            let copy = unsafe { d7y_rdma_dup_info(entry) };
            if copy.is_null() {
                return Err(ProbeError::InvalidResult(
                    "fi_dupinfo failed to allocate candidate",
                ));
            }
            selected = Some(SelectedInfo {
                list: InfoList(copy),
            });
        }
        Ok(())
    })?;
    selected.ok_or(ResourceError::CandidateUnavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn software_provider_query_copies_metadata_before_freeing_the_list() {
        let ProbeResult::Available(candidates) = probe(&ProbeOptions {
            provider: "tcp".to_owned(),
            domain: None,
        })
        .unwrap() else {
            panic!("libfabric TCP provider is required for the hardware-independent FFI test");
        };
        assert!(!candidates.is_empty());
        for candidate in candidates {
            assert_eq!(candidate.provider, "tcp");
            assert!(!candidate.domain.is_empty());
            assert_eq!(candidate.endpoint_type, EndpointType::Message);
            assert!(candidate.capabilities.is_supported());
        }
    }

    #[test]
    fn missing_provider_is_an_unavailable_result() {
        assert_eq!(
            probe(&ProbeOptions {
                provider: "dragonfly-nonexistent-provider".to_owned(),
                domain: None,
            })
            .unwrap(),
            ProbeResult::Unavailable(UnavailableReason::NoMatchingInterface)
        );
    }

    #[test]
    fn domain_selection_is_enforced_when_the_provider_ignores_hints() {
        assert_eq!(
            probe(&ProbeOptions {
                provider: "tcp".to_owned(),
                domain: Some("dragonfly-nonexistent-rdma-domain".to_owned()),
            })
            .unwrap(),
            ProbeResult::Unavailable(UnavailableReason::NoMatchingInterface)
        );
    }
}
