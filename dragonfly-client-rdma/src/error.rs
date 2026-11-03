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

/// A libfabric failure with provider diagnostics copied before the next queue read.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("{operation} failed: {message} (fabric={code}, provider={provider_code})")]
pub struct FabricError {
    pub operation: &'static str,
    pub code: i32,
    pub provider_code: i32,
    pub message: String,
}

/// Discovery failures; a missing compatible interface is an unavailable result.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProbeError {
    #[error("invalid {field}: {reason}")]
    InvalidOptions {
        field: &'static str,
        reason: &'static str,
    },
    #[error(transparent)]
    Native(#[from] FabricError),
    #[error("invalid libfabric result: {0}")]
    InvalidResult(&'static str),
}

/// Resource creation and cleanup failures. Failed closes preserve ownership.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResourceError {
    #[error("RDMA unavailable: {0}")]
    Unavailable(crate::UnavailableReason),
    #[error("the candidate no longer matches local interfaces; run probe() again")]
    CandidateUnavailable,
    #[error(transparent)]
    Probe(#[from] ProbeError),
    #[error(transparent)]
    Native(#[from] FabricError),
    #[error("native allocation failed for {resource}")]
    Allocation { resource: &'static str },
    #[error("{resource} still has live child resources")]
    InUse { resource: &'static str },
    #[error("{resource} has already been closed")]
    Closed { resource: &'static str },
}

/// Transport errors. Recovering from failed I/O requires successful session close.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error(transparent)]
    Resource(#[from] ResourceError),
    #[error(transparent)]
    Native(#[from] FabricError),
    #[error("invalid session option: {0}")]
    InvalidOptions(&'static str),
    #[error("{0} timed out; close the session before reusing resources")]
    Timeout(&'static str),
    #[error("protocol error: {0}")]
    Protocol(&'static str),
    #[error("cannot {operation}: session is {state}")]
    InvalidState {
        operation: &'static str,
        state: &'static str,
    },
    #[error("unexpected CM event {event}; expected {expected}")]
    UnexpectedEvent { expected: &'static str, event: u32 },
    #[error("completion mismatch for {operation}: queue={queue}, flags={flags:#x}, context_matches={context_matches}")]
    CompletionMismatch {
        operation: &'static str,
        queue: &'static str,
        flags: u64,
        context_matches: bool,
    },
    #[error("peer disconnected")]
    Disconnected,
    #[error("{resource} limit exceeded (requested={requested}, available={available})")]
    ResourceLimit {
        resource: &'static str,
        requested: usize,
        available: usize,
    },
    #[error("allocation of {bytes} bytes failed")]
    Allocation { bytes: usize },
    #[error("RDMA worker stopped")]
    WorkerStopped,
    #[error("message length {actual} exceeds limit {limit}")]
    MessageTooLarge { actual: usize, limit: usize },
}

#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(crate) fn fabric_error(operation: &'static str, code: i32) -> FabricError {
    FabricError {
        operation,
        code,
        provider_code: 0,
        message: crate::ffi::error_message(code),
    }
}

#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(crate) fn check(
    operation: &'static str,
    code: i32,
    provider_code: i32,
) -> Result<(), SessionError> {
    if code < 0 {
        let mut error = fabric_error(operation, code);
        error.provider_code = provider_code;
        Err(error.into())
    } else {
        Ok(())
    }
}

#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(crate) fn check_diagnostic(
    operation: &'static str,
    code: i32,
    provider_code: i32,
    diagnostic: &[std::ffi::c_char],
) -> Result<(), SessionError> {
    if code >= 0 {
        return Ok(());
    }
    let mut error = fabric_error(operation, code);
    error.provider_code = provider_code;
    // The shim always NUL-terminates the caller-owned buffer.
    let bytes: Vec<u8> = diagnostic
        .iter()
        .take_while(|v| **v != 0)
        .map(|v| *v as u8)
        .collect();
    if !bytes.is_empty() {
        error.message = String::from_utf8_lossy(&bytes).into_owned();
    }
    Err(error.into())
}
