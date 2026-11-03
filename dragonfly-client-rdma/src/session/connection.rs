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

use super::progress::Progress;
use super::{RemoteRegion, SessionOptions};
use crate::error::check;
use crate::ffi::{query_session as query, raw::*, SelectedInfo};
use crate::resource::completion::operation::{Context, Kind, PendingOperation};
use crate::resource::endpoint::{Endpoint, EventQueue};
use crate::resource::handle;
use crate::resource::memory::RegisteredBuffer;
use crate::{CompletionQueue, CqOptions, Domain, Fabric, ResourceBudget, SessionError};
use std::rc::Rc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Connecting,
    Connected,
    Failed,
    Closed,
}
impl State {
    fn name(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Failed => "failed",
            Self::Closed => "closed",
        }
    }
}

// An endpoint retains its queues. Keep the entire bundle on close failure so
// neither in-flight memory nor its registration can be released prematurely.
struct Resources {
    ep: Endpoint,
    pending: Option<PendingOperation>,
    exports: Vec<RegisteredBuffer>,
    send_buffer: Option<RegisteredBuffer>,
    receive_buffer: Option<RegisteredBuffer>,
    read_buffer: Option<RegisteredBuffer>,
    tx: CompletionQueue,
    rx: CompletionQueue,
    eq: EventQueue,
    domain: Domain,
    fabric: Option<Fabric>,
    budget: ResourceBudget,
    _session: crate::limits::Permit,
    mr_mode: u64,
    max_size: usize,
    next_key: u64,
}

/// Reusable blocking connection. Construct and use on one dedicated thread.
///
/// Any failed transaction poisons this session; only try_close is then allowed.
/// Exported regions remain registered until successful endpoint close.
pub struct Session {
    inner: Option<Box<Resources>>,
    state: State,
    timeout: Duration,
    deadline: Option<Instant>,
}
impl Session {
    /// Connect using a new resource budget. Use connect_with_budget to share limits.
    #[tracing::instrument(level = "debug", skip_all, fields(peer = %options.node, port = options.port))]
    pub fn connect(options: &SessionOptions) -> Result<Self, SessionError> {
        Self::connect_with_budget(options, ResourceBudget::new(options.limits.clone())?)
    }
    pub fn connect_with_budget(
        options: &SessionOptions,
        budget: ResourceBudget,
    ) -> Result<Self, SessionError> {
        options.validate()?;
        Self::connect_native(options, budget)
    }
    pub(crate) fn connect_native(
        options: &SessionOptions,
        budget: ResourceBudget,
    ) -> Result<Self, SessionError> {
        let deadline = Instant::now() + options.timeout;
        let permit = budget.session()?;
        let info = query(options, false)?;
        // SAFETY: info retains the native destination through fi_connect.
        let address = unsafe { d7y_rdma_session_dest(info.as_ptr()) };
        if address.is_null() {
            return Err(SessionError::Protocol("query returned no destination"));
        }
        let fabric = crate::resource::fabric::fabric_from_info(info.duplicate()?)?;
        let mut session = Self::build(fabric, &info, options.timeout, budget, permit, &mut false)?;
        check(
            "fi_connect",
            unsafe { d7y_rdma_ep_connect(session.resources()?.ep.handle.as_ptr()?, address) },
            0,
        )?;
        session.wait_connected(deadline)?;
        Ok(session)
    }
    pub(super) fn build(
        fabric: Fabric,
        info: &SelectedInfo,
        timeout: Duration,
        budget: ResourceBudget,
        permit: crate::limits::Permit,
        endpoint_opened: &mut bool,
    ) -> Result<Self, SessionError> {
        // SAFETY: info owns the complete query or CM result, alive throughout setup.
        check(
            "validate connection fi_info",
            unsafe { d7y_rdma_session_validate(info.as_ptr()) },
            0,
        )?;
        let max_size = unsafe { d7y_rdma_session_max_size(info.as_ptr()) };
        if max_size < budget.limits().max_message_size {
            return Err(SessionError::InvalidOptions(
                "control buffer exceeds provider message limit",
            ));
        }
        let mr_mode = unsafe { d7y_rdma_session_mr_mode(info.as_ptr()) };
        let domain = crate::resource::fabric::domain_from_info(&fabric, info.duplicate()?)?;
        let eq = EventQueue::open(&fabric)?;
        let options = CqOptions {
            size: budget.limits().completion_queue_size,
        };
        let tx = domain.open_completion_queue(options)?;
        let rx = domain.open_completion_queue(options)?;
        let ep = Endpoint::open(&domain, &eq, &tx, &rx, info)?;
        *endpoint_opened = true;
        let session = Self {
            inner: Some(Box::new(Resources {
                ep,
                pending: None,
                exports: Vec::new(),
                send_buffer: None,
                receive_buffer: None,
                read_buffer: None,
                tx,
                rx,
                eq,
                domain,
                fabric: Some(fabric),
                budget,
                _session: permit,
                mr_mode,
                max_size,
                next_key: 1,
            })),
            state: State::Connecting,
            timeout,
            deadline: None,
        };
        // Own the complete bundle before a native bind/enable can fail.
        let r = session.resources()?;
        r.ep.bind_enable(&r.eq, &r.tx, &r.rx)?;
        Ok(session)
    }
    fn invalid_state(&self, operation: &'static str) -> SessionError {
        SessionError::InvalidState {
            operation,
            state: self.state.name(),
        }
    }
    fn resources(&self) -> Result<&Resources, SessionError> {
        self.inner
            .as_deref()
            .ok_or_else(|| self.invalid_state("access resources"))
    }
    fn resources_mut(&mut self) -> Result<&mut Resources, SessionError> {
        let error = self.invalid_state("access resources");
        self.inner.as_deref_mut().ok_or(error)
    }
    pub(super) fn accept_connection(&mut self, deadline: Instant) -> Result<(), SessionError> {
        if Instant::now() >= deadline {
            return Err(SessionError::Timeout("accept"));
        }
        // SAFETY: the endpoint owns this FI_CONNREQ and all its bound resources.
        check(
            "fi_accept",
            unsafe { d7y_rdma_ep_accept(self.resources()?.ep.handle.as_ptr()?) },
            0,
        )?;
        self.wait_connected(deadline)
    }
    pub(super) fn wait_connected(&mut self, deadline: Instant) -> Result<(), SessionError> {
        let mut progress = Progress::default();
        loop {
            if Instant::now() >= deadline {
                return Err(SessionError::Timeout("connect"));
            }
            let r = self.resources()?;
            if let Some(event) = r.eq.poll()? {
                // SAFETY: event and endpoint belong to this connection's EQ.
                if event.fid != r.ep.handle.as_ptr()?
                    || unsafe { d7y_rdma_is_connected(event.event) } == 0
                {
                    return Err(SessionError::UnexpectedEvent {
                        expected: "FI_CONNECTED on this endpoint",
                        event: event.event,
                    });
                }
                self.state = State::Connected;
                return Ok(());
            }
            progress.wait(deadline, "connect")?;
        }
    }
    /// Apply one deadline across all messages, reads and caller work in a request.
    /// Native queries/registration calls cannot be interrupted; expiry is checked
    /// before posting and after each operation. An error requires explicit close.
    pub fn transaction<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<T, SessionError>,
    ) -> Result<T, SessionError> {
        self.transaction_until(Instant::now() + self.timeout, operation)
    }
    pub(crate) fn transaction_until<T>(
        &mut self,
        requested: Instant,
        operation: impl FnOnce(&mut Self) -> Result<T, SessionError>,
    ) -> Result<T, SessionError> {
        if self.state != State::Connected {
            return Err(self.invalid_state("start transaction"));
        }
        let outer = self.deadline.is_none();
        let previous = self.deadline;
        let deadline = previous.map_or(requested, |existing| existing.min(requested));
        self.deadline = Some(deadline);
        let mut result = if Instant::now() >= deadline {
            Err(SessionError::Timeout("transaction"))
        } else {
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(self)));
            let outcome = match outcome {
                Ok(value) => value,
                Err(payload) => {
                    self.state = State::Failed;
                    self.deadline = previous;
                    std::panic::resume_unwind(payload)
                }
            };
            outcome.and_then(|value| {
                if Instant::now() >= deadline {
                    Err(SessionError::Timeout("transaction"))
                } else {
                    Ok(value)
                }
            })
        };
        if result.is_ok() && self.state != State::Connected {
            result = Err(self.invalid_state("finish transaction"));
        }
        if result.is_err() {
            self.state = State::Failed;
        }
        if outer {
            self.deadline = None;
        } else {
            self.deadline = previous;
        }
        result
    }
    /// Send one bounded control message, reusing the registration after completion.
    pub fn send(&mut self, message: &[u8]) -> Result<(), SessionError> {
        self.transaction(|session| {
            let limit = session.resources()?.budget.limits().max_message_size;
            if message.is_empty() || message.len() > limit {
                return Err(SessionError::MessageTooLarge {
                    actual: message.len(),
                    limit,
                });
            }
            let mut buffer = match session.resources_mut()?.send_buffer.take() {
                Some(buffer) => buffer,
                None => session.register(zeroed(limit)?, false)?,
            };
            buffer.data_mut()?[..message.len()].copy_from_slice(message);
            let (buffer, _) = session.transfer(buffer, Kind::Send, 0, 0, message.len())?;
            session.resources_mut()?.send_buffer = Some(buffer);
            Ok(())
        })
    }
    /// Receive one control message. Returned bytes do not borrow registered memory.
    pub fn receive(&mut self) -> Result<Vec<u8>, SessionError> {
        self.transaction(|session| {
            let limit = session.resources()?.budget.limits().max_message_size;
            let buffer = match session.resources_mut()?.receive_buffer.take() {
                Some(buffer) => buffer,
                None => session.register(zeroed(limit)?, false)?,
            };
            let (buffer, length) = session.transfer(buffer, Kind::Receive, 0, 0, limit)?;
            let mut bytes = zeroed(length)?;
            bytes.copy_from_slice(&buffer.data()?[..length]);
            session.resources_mut()?.receive_buffer = Some(buffer);
            Ok(bytes)
        })
    }
    /// Export immutable bytes. The session retains the MR until endpoint close.
    /// Admission limits bound retained exports; no caller can recycle a live region.
    #[tracing::instrument(level = "debug", skip_all, fields(bytes = data.len()))]
    pub fn export(&mut self, data: Vec<u8>) -> Result<RemoteRegion, SessionError> {
        self.transaction(|session| {
            session.validate_length(data.len())?;
            let buffer = session.register(data, true)?;
            let region = RemoteRegion {
                length: buffer.data()?.len() as u64,
                address: buffer.address(session.resources()?.mr_mode)?,
                key: buffer.key()?,
            };
            session.resources_mut()?.exports.push(buffer);
            Ok(region)
        })
    }
    /// Read a region and return initialized bytes after its matching CQ completion.
    /// Address/key errors are reported by the provider and poison the connection.
    #[tracing::instrument(level = "debug", skip_all, fields(bytes = region.length))]
    pub fn read(&mut self, region: RemoteRegion) -> Result<Vec<u8>, SessionError> {
        self.transaction(|session| {
            let length = region.validate(session.resources()?.budget.limits().max_transfer_size)?;
            session.validate_length(length)?;
            let old = session.resources_mut()?.read_buffer.take();
            let buffer = match old {
                Some(buffer) if buffer.data()?.len() >= length => buffer,
                Some(mut buffer) => {
                    buffer.try_close()?;
                    session.register(zeroed(length)?, false)?
                }
                None => session.register(zeroed(length)?, false)?,
            };
            let (buffer, _) =
                session.transfer(buffer, Kind::Read, region.address, region.key, length)?;
            let mut bytes = zeroed(length)?;
            bytes.copy_from_slice(&buffer.data()?[..length]);
            session.resources_mut()?.read_buffer = Some(buffer);
            Ok(bytes)
        })
    }
    fn validate_length(&self, length: usize) -> Result<(), SessionError> {
        let r = self.resources()?;
        if length == 0 || length > r.max_size.min(r.budget.limits().max_transfer_size) {
            return Err(SessionError::MessageTooLarge {
                actual: length,
                limit: r.max_size.min(r.budget.limits().max_transfer_size),
            });
        }
        Ok(())
    }
    fn register(&mut self, data: Vec<u8>, remote: bool) -> Result<RegisteredBuffer, SessionError> {
        let r = self.resources_mut()?;
        let key = r.next_key;
        r.next_key = key
            .checked_add(1)
            .ok_or(SessionError::Protocol("MR key exhausted"))?;
        RegisteredBuffer::register(&r.domain, data, key, remote, &r.budget)
    }
    fn transfer(
        &mut self,
        buffer: RegisteredBuffer,
        kind: Kind,
        address: u64,
        key: u64,
        length: usize,
    ) -> Result<(RegisteredBuffer, usize), SessionError> {
        let r = self.inner.as_mut().ok_or(SessionError::InvalidState {
            operation: "transfer",
            state: "closed",
        })?;
        if r.pending.is_some() {
            return Err(SessionError::InvalidState {
                operation: "transfer",
                state: "operation pending",
            });
        }
        let context = Context::new()?;
        // Install ownership before posting. Neither timeout nor a native
        // error removes it; only completion or successful EP close does.
        r.pending = Some(PendingOperation {
            buffer,
            context,
            kind,
        });
        let deadline = self.deadline.ok_or(SessionError::InvalidState {
            operation: "transfer",
            state: "missing deadline",
        })?;
        let mut progress = Progress::default();
        loop {
            if Instant::now() >= deadline {
                return Err(SessionError::Timeout("submit transfer"));
            }
            let p = r.pending.as_mut().ok_or(SessionError::InvalidState {
                operation: "submit",
                state: "no pending operation",
            })?;
            let ep = r.ep.handle.as_ptr()?;
            let data = p.buffer.pointer()?;
            let size = length;
            let desc = p.buffer.descriptor()?;
            let context = p.context.pointer();
            // SAFETY: pending owns a fixed allocation, its live MR and stable
            // context. length never exceeds this allocation. Ownership remains
            // installed on submission error, timeout and every incomplete read.
            let code = unsafe {
                match kind {
                    Kind::Send => d7y_rdma_post_send(ep, data, size, desc, context),
                    Kind::Receive => d7y_rdma_post_recv(ep, data, size, desc, context),
                    Kind::Read => d7y_rdma_post_read(ep, data, size, desc, address, key, context),
                }
            };
            if code == 0 {
                break;
            }
            if unsafe { d7y_rdma_is_again(code) } == 0 {
                check("post transfer", code, 0)?;
            }
            // Poll both CQs to drive manual progress even when submission is
            // backpressured. Nothing has been submitted for this context yet.
            if r.tx.poll()?.is_some() || r.rx.poll()?.is_some() {
                return Err(SessionError::Protocol(
                    "unexpected completion before submission",
                ));
            }
            Self::check_peer(r)?;
            progress.wait(deadline, "submit transfer")?;
        }
        loop {
            for (is_rx, cq) in [(false, &r.tx), (true, &r.rx)] {
                if let Some(event) = cq.poll()? {
                    let p = r.pending.as_ref().ok_or(SessionError::InvalidState {
                        operation: "complete",
                        state: "no pending operation",
                    })?;
                    if event.context != p.context.pointer()
                        || is_rx != matches!(p.kind, Kind::Receive)
                        || unsafe { d7y_rdma_completion_matches(event.flags, p.kind as i32) } == 0
                    {
                        return Err(SessionError::CompletionMismatch {
                            operation: p.kind.name(),
                            queue: if is_rx { "receive" } else { "transmit" },
                            flags: event.flags,
                            context_matches: event.context == p.context.pointer(),
                        });
                    }
                    if is_rx && (event.length == 0 || event.length > length) {
                        return Err(SessionError::Protocol("received frame length mismatch"));
                    }
                    let completed_length = if is_rx { event.length } else { length };
                    let pending = r.pending.take().ok_or(SessionError::InvalidState {
                        operation: "complete",
                        state: "no pending operation",
                    })?;
                    return Ok((pending.buffer, completed_length));
                }
            }
            Self::check_peer(r)?;
            progress.wait(deadline, "transfer completion")?;
        }
    }

    fn check_peer(r: &Resources) -> Result<(), SessionError> {
        if let Some(event) = r.eq.poll()? {
            if unsafe { d7y_rdma_is_shutdown(event.event) } != 0 {
                return Err(SessionError::Disconnected);
            }
            return Err(SessionError::UnexpectedEvent {
                expected: "FI_SHUTDOWN",
                event: event.event,
            });
        }
        Ok(())
    }

    /// Close endpoint before memory and queues. Failures retain resources for retry.
    pub fn try_close(&mut self) -> Result<(), SessionError> {
        let Some(r) = self.inner.as_mut() else {
            return Ok(());
        };
        self.state = State::Failed;
        // shutdown alone is insufficient: providers may discard pending ops
        // without completions. Keep all allocations until close succeeds.
        if let Ok(ep) = r.ep.handle.as_ptr() {
            // SAFETY: shutdown only requests quiescence. All registered memory
            // and contexts remain owned until the following endpoint close.
            let code = unsafe { d7y_rdma_ep_shutdown(ep) };
            if code < 0 {
                tracing::debug!(code, "RDMA shutdown failed; attempting endpoint close");
            }
            // Drive manual progress and consume cancellation completions
            // while their contexts and allocations are still retained.
            // A CQ error must not prevent attempting endpoint close.
            for _ in 0..16 {
                let tx = r.tx.poll();
                let rx = r.rx.poll();
                if matches!(tx, Ok(None)) && matches!(rx, Ok(None)) {
                    break;
                }
                if let Err(error) = tx {
                    tracing::debug!(%error, "TX CQ during shutdown");
                }
                if let Err(error) = rx {
                    tracing::debug!(%error, "RX CQ during shutdown");
                }
            }
        }
        handle::try_close(&mut r.ep.handle)?;
        if let Some(p) = r.pending.as_mut() {
            p.buffer.try_close()?;
        }
        r.pending.take();
        for buffer in &mut r.exports {
            buffer.try_close()?;
        }
        r.exports.clear();
        for slot in [
            &mut r.send_buffer,
            &mut r.receive_buffer,
            &mut r.read_buffer,
        ] {
            if let Some(buffer) = slot.as_mut() {
                buffer.try_close()?;
            }
            slot.take();
        }
        r.tx.try_close()?;
        r.rx.try_close()?;
        handle::try_close(&mut r.eq.handle)?;
        r.domain.try_close()?;
        if let Some(fabric) = r.fabric.as_mut() {
            if Rc::strong_count(&fabric.inner) == 1 {
                fabric.try_close()?;
            }
        }
        // An accepted session may share Fabric with the Listener. Release
        // our reference, leaving final close to the last owner.
        r.fabric.take();
        self.inner.take();
        self.state = State::Closed;
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Err(error) = self.try_close() {
            tracing::error!(%error, "session close failed; retaining live resources and in-flight memory");
            if let Some(r) = self.inner.as_ref() {
                r.budget.cleanup_failed();
            }
            std::mem::forget(self.inner.take());
        }
    }
}

fn zeroed(length: usize) -> Result<Vec<u8>, SessionError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| SessionError::Allocation { bytes: length })?;
    bytes.resize(length, 0);
    Ok(bytes)
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
