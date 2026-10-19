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

//! Local resource creation without endpoints, connections or registered memory.
//!
//! Children keep their parents alive with reference counting. Handles deliberately
//! implement neither Send nor Sync: provider threading requirements are not yet
//! integrated with Dragonfly's asynchronous execution model.

#[cfg(not(all(target_os = "linux", feature = "rdma")))]
use crate::UnavailableReason;
use crate::{RdmaCapability, ResourceError};

#[cfg(all(target_os = "linux", feature = "rdma"))]
mod native;

/// A polling CQ using FI_CQ_FORMAT_DATA and FI_WAIT_NONE.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CqOptions {
    /// Minimum entry count. Zero lets the provider choose its default.
    pub size: usize,
}

/// Independently configurable transmit and receive completion queues.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceOptions {
    pub tx_cq: CqOptions,
    pub rx_cq: CqOptions,
}

/// An owned fabric. Open domains keep the underlying fabric alive.
pub struct Fabric {
    #[cfg(all(target_os = "linux", feature = "rdma"))]
    inner: native::OwnedHandle,
    #[cfg(not(all(target_os = "linux", feature = "rdma")))]
    _not_send_sync: std::marker::PhantomData<std::rc::Rc<()>>,
}

/// An owned domain. Open completion queues keep the underlying domain alive.
pub struct Domain {
    #[cfg(all(target_os = "linux", feature = "rdma"))]
    inner: native::OwnedHandle,
    #[cfg(not(all(target_os = "linux", feature = "rdma")))]
    _not_send_sync: std::marker::PhantomData<std::rc::Rc<()>>,
}

/// An owned completion queue. No raw handle or polling API is exposed yet.
pub struct CompletionQueue {
    #[cfg(all(target_os = "linux", feature = "rdma"))]
    inner: native::OwnedHandle,
    #[cfg(not(all(target_os = "linux", feature = "rdma")))]
    _not_send_sync: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(not(all(target_os = "linux", feature = "rdma")))]
fn unavailable() -> ResourceError {
    ResourceError::Unavailable(if cfg!(target_os = "linux") {
        UnavailableReason::FeatureDisabled
    } else {
        UnavailableReason::UnsupportedPlatform
    })
}

impl Fabric {
    /// Re-queries and matches the entire candidate metadata snapshot before opening.
    ///
    /// Retains the selected fi_info, including provider-owned addressing details,
    /// for subsequent domain creation. A stale or edited candidate is rejected.
    pub fn open(candidate: &RdmaCapability) -> Result<Self, ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            native::open_fabric(candidate)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            let _ = candidate;
            Err(unavailable())
        }
    }

    pub fn open_domain(&self) -> Result<Domain, ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            native::open_domain(self)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            Err(unavailable())
        }
    }

    /// Closes once all domains have closed. On failure the handle remains owned
    /// and can be retried. Repeated successful closes are harmless.
    pub fn try_close(&mut self) -> Result<(), ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            native::try_close(&mut self.inner)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            Err(unavailable())
        }
    }
}

impl Domain {
    pub fn open_completion_queue(
        &self,
        options: CqOptions,
    ) -> Result<CompletionQueue, ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            native::open_cq(self, options)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            let _ = options;
            Err(unavailable())
        }
    }

    /// Fails with InUse while completion queues are live; preserves failed handles.
    pub fn try_close(&mut self) -> Result<(), ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            native::try_close(&mut self.inner)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            Err(unavailable())
        }
    }
}

impl CompletionQueue {
    /// Reports native close failures and retains ownership for retry.
    pub fn try_close(&mut self) -> Result<(), ResourceError> {
        #[cfg(all(target_os = "linux", feature = "rdma"))]
        {
            native::try_close(&mut self.inner)
        }
        #[cfg(not(all(target_os = "linux", feature = "rdma")))]
        {
            Err(unavailable())
        }
    }
}

/// A fabric, one domain, and separate transmit/receive completion queues.
///
/// Construction failures roll back previously opened resources through RAII.
/// Dropping this value releases CQs before Domain before Fabric. Successful
/// creation only validates local allocation, not a working RDMA connection.
pub struct RdmaResources {
    tx_cq: CompletionQueue,
    rx_cq: CompletionQueue,
    domain: Domain,
    fabric: Fabric,
}

impl RdmaResources {
    pub fn open(
        candidate: &RdmaCapability,
        options: ResourceOptions,
    ) -> Result<Self, ResourceError> {
        Self::from_fabric(Fabric::open(candidate)?, options)
    }

    fn from_fabric(fabric: Fabric, options: ResourceOptions) -> Result<Self, ResourceError> {
        let domain = fabric.open_domain()?;
        let tx_cq = domain.open_completion_queue(options.tx_cq)?;
        let rx_cq = domain.open_completion_queue(options.rx_cq)?;
        Ok(Self {
            tx_cq,
            rx_cq,
            domain,
            fabric,
        })
    }

    pub fn fabric(&self) -> &Fabric {
        &self.fabric
    }
    pub fn domain(&self) -> &Domain {
        &self.domain
    }
    pub fn tx_cq(&self) -> &CompletionQueue {
        &self.tx_cq
    }
    pub fn rx_cq(&self) -> &CompletionQueue {
        &self.rx_cq
    }

    /// Explicitly closes CQs, then Domain, then Fabric, stopping on error.
    /// Already closed resources are skipped on a subsequent retry.
    pub fn try_close(&mut self) -> Result<(), ResourceError> {
        self.tx_cq.try_close()?;
        self.rx_cq.try_close()?;
        self.domain.try_close()?;
        self.fabric.try_close()
    }
}

#[cfg(all(test, not(all(target_os = "linux", feature = "rdma"))))]
mod tests {
    use super::*;
    use crate::{EndpointType, ReadCapabilities};

    #[test]
    fn creation_reports_platform_or_feature_unavailability() {
        let candidate = RdmaCapability {
            provider: "verbs".to_owned(),
            domain: "mlx5_0".to_owned(),
            device: None,
            endpoint_type: EndpointType::Message,
            capabilities: ReadCapabilities {
                messaging: true,
                rma: true,
                read: true,
                remote_read: true,
                remote_communication: true,
            },
            raw_capabilities: 0,
            mode: 0,
            mr_mode: 0,
            protocol: 0,
            protocol_version: 0,
            threading: 0,
            control_progress: 0,
            data_progress: 0,
        };
        assert!(matches!(Fabric::open(&candidate), Err(error) if error == unavailable()));
        assert!(matches!(
            RdmaResources::open(&candidate, ResourceOptions::default()),
            Err(error) if error == unavailable()
        ));
    }
}
