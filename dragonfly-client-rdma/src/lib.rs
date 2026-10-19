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

//! Local discovery of verbs interfaces advertising message and RDMA Read support.
//!
//! A candidate is not proof of hardware offload or a working remote connection.
//! Fabric, Domain and completion queue ownership is managed through Rust RAII.
//! Peer-to-peer validation remains a separate step.

pub mod capability;
pub mod error;
pub mod resource;

#[cfg(all(target_os = "linux", feature = "rdma"))]
mod ffi;

pub use capability::{
    probe, EndpointType, ProbeOptions, ProbeResult, RdmaCapability, ReadCapabilities,
    UnavailableReason,
};
pub use error::{ProbeError, ResourceError};
pub use resource::{CompletionQueue, CqOptions, Domain, Fabric, RdmaResources, ResourceOptions};
