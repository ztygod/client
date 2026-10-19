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

/// Failure to validate options, query libfabric, or decode its result.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProbeError {
    #[error("invalid {field}: {reason}")]
    InvalidOptions {
        field: &'static str,
        reason: &'static str,
    },

    #[error("{operation} failed with libfabric code {code}: {message}")]
    Libfabric {
        operation: &'static str,
        code: i32,
        message: String,
    },

    #[error("invalid libfabric result: {0}")]
    InvalidResult(&'static str),
}

/// Failure to select, create or explicitly close a local libfabric resource.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResourceError {
    #[error("resource creation is unavailable: {0:?}")]
    Unavailable(crate::UnavailableReason),

    #[error("the candidate no longer matches local interfaces; run probe() again")]
    CandidateUnavailable,

    #[error(transparent)]
    Probe(#[from] ProbeError),

    #[error("{operation} failed with libfabric code {code}: {message}")]
    Libfabric {
        operation: &'static str,
        code: i32,
        message: String,
    },

    #[error("{resource} still has live child resources")]
    InUse { resource: &'static str },

    #[error("{resource} has already been closed")]
    Closed { resource: &'static str },
}
