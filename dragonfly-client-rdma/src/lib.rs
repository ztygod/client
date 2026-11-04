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

//! Verbs discovery, owned native resources, reusable sessions and async workers.
//!
//! A candidate is not proof of hardware offload or a working remote connection.
//! Fabric, Domain and completion queue ownership is managed through Rust RAII.
//! The blocking Session API exchanges messages, reads registered memory, and
//! explicitly closes resources. Applications retain their own integrity checks. Target hardware still
//! requires validation with examples/verify_read_export_lifecycle.rs.

pub mod capability;
pub mod error;
pub mod limits;
pub mod resource;
pub mod session;
pub mod worker;

#[cfg(all(target_os = "linux", feature = "rdma"))]
mod ffi;

pub use capability::{
    probe, EndpointType, ProbeOptions, ProbeResult, RdmaCapability, ReadCapabilities,
    UnavailableReason,
};
pub use error::{FabricError, ProbeError, ResourceError, SessionError};
pub use limits::{Limits, ResourceBudget, ResourceUsage};
pub const MAX_TRANSFER_SIZE: usize = 64 * 1024 * 1024;
pub use resource::{CompletionQueue, CqOptions, Domain, Fabric, RdmaResources, ResourceOptions};
pub use session::{
    CompletedRead, ExportBuffer, ExportId, ExportedRegion, Listener, ReadCompletion, ReadOptions,
    RemoteRegion, Session, SessionOptions,
};
pub use worker::{ReadStream, SessionWorker};
