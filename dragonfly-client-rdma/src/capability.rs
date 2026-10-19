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

use crate::ProbeError;

/// Local interface selection. Discovery currently targets verbs MSG endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeOptions {
    pub provider: String,
    /// A libfabric domain name, which is not necessarily the network device name.
    pub domain: Option<String>,
}

impl Default for ProbeOptions {
    fn default() -> Self {
        Self {
            provider: "verbs".to_owned(),
            domain: None,
        }
    }
}

impl ProbeOptions {
    pub(crate) fn validate(&self) -> Result<(), ProbeError> {
        for (field, value) in std::iter::once(("provider", self.provider.as_str()))
            .chain(self.domain.as_deref().map(|domain| ("domain", domain)))
        {
            if value.is_empty() || value.trim() != value || value.contains('\0') {
                return Err(ProbeError::InvalidOptions {
                    field,
                    reason: "must be nonempty, have no surrounding whitespace, and contain no NUL",
                });
            }
        }
        if self.provider != "verbs" {
            return Err(ProbeError::InvalidOptions {
                field: "provider",
                reason: "only the verbs provider with MSG endpoints is supported",
            });
        }
        Ok(())
    }
}

/// Endpoint type supported by this initial discovery implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointType {
    Message,
}

/// Capabilities required to serve as both a reader and a remote Read target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadCapabilities {
    pub messaging: bool,
    pub rma: bool,
    pub read: bool,
    pub remote_read: bool,
    pub remote_communication: bool,
}

impl ReadCapabilities {
    /// Whether all requirements of the initial transport are advertised.
    pub fn is_supported(&self) -> bool {
        self.messaging && self.rma && self.read && self.remote_read && self.remote_communication
    }
}

/// Owned metadata for one matching interface, not a validated transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RdmaCapability {
    pub provider: String,
    pub domain: String,
    pub device: Option<String>,
    pub endpoint_type: EndpointType,
    pub capabilities: ReadCapabilities,
    /// Raw libfabric capability flags, preserved for diagnostics.
    pub raw_capabilities: u64,
    /// Required application mode flags; resource creation must honor these.
    pub mode: u64,
    /// Required memory registration flags; discovery does not implement them.
    pub mr_mode: u64,
    /// Raw libfabric endpoint protocol and its version.
    pub protocol: u32,
    pub protocol_version: u32,
    /// Raw libfabric threading and progress requirements.
    pub threading: u32,
    pub control_progress: u32,
    pub data_progress: u32,
}

/// Expected reasons that local discovery cannot yield a candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnavailableReason {
    UnsupportedPlatform,
    FeatureDisabled,
    /// No interface matched the requested provider, domain, endpoint, and caps.
    NoMatchingInterface,
}

/// Discovery output. Available means candidates were found, not that RDMA works.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProbeResult {
    Available(Vec<RdmaCapability>),
    Unavailable(UnavailableReason),
}

/// Queries local verbs MSG interfaces without opening a connection or domain.
///
/// The result is a snapshot and does not reserve resources, verify a remote peer,
/// or prove hardware offload (verbs can also expose software RDMA devices).
pub fn probe(options: &ProbeOptions) -> Result<ProbeResult, ProbeError> {
    options.validate()?;

    #[cfg(not(target_os = "linux"))]
    {
        Ok(ProbeResult::Unavailable(
            UnavailableReason::UnsupportedPlatform,
        ))
    }

    #[cfg(all(target_os = "linux", not(feature = "rdma")))]
    {
        Ok(ProbeResult::Unavailable(UnavailableReason::FeatureDisabled))
    }

    #[cfg(all(target_os = "linux", feature = "rdma"))]
    {
        crate::ffi::probe(options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_rejects_invalid_selection_before_querying() {
        let test_cases = vec![
            ("", None, "provider"),
            ("verbs\0tcp", None, "provider"),
            (" verbs", None, "provider"),
            ("tcp", None, "provider"),
            ("verbs;ofi_rxm", None, "provider"),
            ("verbs", Some(""), "domain"),
            ("verbs", Some("mlx5_0\0"), "domain"),
            ("verbs", Some("mlx5_0 "), "domain"),
        ];
        for (provider, domain, field) in test_cases {
            let actual = probe(&ProbeOptions {
                provider: provider.to_owned(),
                domain: domain.map(str::to_owned),
            });
            assert!(
                matches!(actual, Err(ProbeError::InvalidOptions { field: actual, .. }) if actual == field)
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn probe_reports_unsupported_platform() {
        assert_eq!(
            probe(&ProbeOptions::default()).unwrap(),
            ProbeResult::Unavailable(UnavailableReason::UnsupportedPlatform)
        );
    }

    #[cfg(all(target_os = "linux", not(feature = "rdma")))]
    #[test]
    fn probe_reports_disabled_feature() {
        assert_eq!(
            probe(&ProbeOptions::default()).unwrap(),
            ProbeResult::Unavailable(UnavailableReason::FeatureDisabled)
        );
    }

    #[cfg(all(target_os = "linux", feature = "rdma"))]
    #[test]
    fn probe_reports_no_matching_domain() {
        assert_eq!(
            probe(&ProbeOptions {
                provider: "verbs".to_owned(),
                domain: Some("dragonfly-nonexistent-rdma-domain".to_owned()),
            })
            .unwrap(),
            ProbeResult::Unavailable(UnavailableReason::NoMatchingInterface)
        );
    }
}
