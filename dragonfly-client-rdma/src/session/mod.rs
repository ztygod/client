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

//! Blocking sessions. Native resources stay on the thread that creates them.
//! Failed or timed-out I/O retains its memory until endpoint close succeeds.

use crate::SessionError;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct SessionOptions {
    pub provider: String,
    pub domain: Option<String>,
    /// Local bind address for Listener; remote address for Session::connect.
    pub node: String,
    pub port: u16,
    /// Budget per connect, accept, or top-level transaction. Nested operations
    /// share the remaining transaction deadline. Synchronous native calls cannot
    /// be interrupted; numeric addresses avoid DNS resolution delays.
    pub timeout: Duration,
    /// Shared admission bounds; effective limits come from a supplied ResourceBudget.
    pub limits: crate::Limits,
}

impl SessionOptions {
    pub fn new(node: impl Into<String>, port: u16) -> Self {
        Self {
            provider: "verbs".into(),
            domain: None,
            node: node.into(),
            port,
            timeout: Duration::from_secs(30),
            limits: crate::Limits::default(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), SessionError> {
        if self.provider != "verbs" {
            return Err(SessionError::InvalidOptions("only verbs is supported"));
        }
        self.validate_common()
    }

    fn validate_common(&self) -> Result<(), SessionError> {
        if self.port == 0
            || self.node.is_empty()
            || self.node.trim() != self.node
            || self.node.contains('\0')
        {
            return Err(SessionError::InvalidOptions(
                "a nonempty node and nonzero port are required",
            ));
        }
        if self
            .domain
            .as_deref()
            .is_some_and(|v| v.is_empty() || v.trim() != v || v.contains('\0'))
        {
            return Err(SessionError::InvalidOptions("invalid domain"));
        }
        if self.timeout.is_zero() || self.timeout > Duration::from_secs(600) {
            return Err(SessionError::InvalidOptions(
                "timeout must be in (0, 600] seconds",
            ));
        }
        self.limits.validate()
    }
}

/// Wire-neutral remote registration metadata. Encode explicitly in the application protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteRegion {
    pub length: u64,
    pub address: u64,
    pub key: u64,
}
impl RemoteRegion {
    pub fn validate(&self, limit: usize) -> Result<usize, SessionError> {
        let length = usize::try_from(self.length)
            .map_err(|_| SessionError::Protocol("region length overflows usize"))?;
        if length == 0 || length > limit {
            return Err(SessionError::MessageTooLarge {
                actual: length,
                limit,
            });
        }
        self.address
            .checked_add(self.length)
            .ok_or(SessionError::Protocol("remote address overflow"))?;
        Ok(length)
    }
}

#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(crate) mod connection;
#[cfg(all(target_os = "linux", feature = "rdma"))]
pub(crate) mod listener;
#[cfg(any(test, all(target_os = "linux", feature = "rdma")))]
mod progress;
#[cfg(not(all(target_os = "linux", feature = "rdma")))]
mod unsupported;
#[cfg(all(target_os = "linux", feature = "rdma"))]
pub use connection::Session;
#[cfg(all(target_os = "linux", feature = "rdma"))]
pub use listener::Listener;
#[cfg(not(all(target_os = "linux", feature = "rdma")))]
pub use unsupported::{Listener, Session};

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_invalid_options_before_opening_native_resources() {
        for options in [
            SessionOptions::new("", 1),
            SessionOptions::new("localhost", 0),
            SessionOptions {
                provider: "tcp".into(),
                ..SessionOptions::new("localhost", 1)
            },
            SessionOptions {
                timeout: Duration::ZERO,
                ..SessionOptions::new("localhost", 1)
            },
        ] {
            assert!(matches!(
                Session::connect(&options),
                Err(SessionError::InvalidOptions(_))
            ));
            assert!(matches!(
                Listener::bind(&options),
                Err(SessionError::InvalidOptions(_))
            ));
        }
    }
}

#[cfg(test)]
mod region_tests {
    use super::*;
    #[test]
    fn remote_metadata_is_checked_before_native_access() {
        assert!(RemoteRegion {
            length: 1,
            address: u64::MAX,
            key: 0
        }
        .validate(64)
        .is_err());
        assert!(RemoteRegion {
            length: 0,
            address: 0,
            key: 0
        }
        .validate(64)
        .is_err());
        assert!(RemoteRegion {
            length: 65,
            address: 0,
            key: 0
        }
        .validate(64)
        .is_err());
    }
}
