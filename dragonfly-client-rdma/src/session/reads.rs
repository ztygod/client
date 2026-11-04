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

//! Owner-thread Read driver. Every posted buffer/context stays in Resources until
//! a matching completion, or successful endpoint close, permits releasing it.

use super::*;
use crate::resource::completion::operation::{Completion, CQ_BATCH_SIZE};
use crate::session::read::{ReadWindow, MAX_READ_BATCH};
use crate::{CompletedRead, ReadOptions};

pub(super) struct NativeRead {
    pub(super) buffer: RegisteredBuffer,
    pub(super) context: Context,
    region: RemoteRegion,
    index: usize,
    posted: bool,
}

impl Session {
    /// Read into a reusable registration and synchronously consume its bytes.
    /// The callback cannot retain the slice. Slow callbacks block owner progress;
    /// use read_lease or SessionWorker::read_stream for asynchronous consumers.
    pub fn read_with<T>(
        &mut self,
        region: RemoteRegion,
        consume: impl FnOnce(&[u8]) -> Result<T, SessionError>,
    ) -> Result<T, SessionError> {
        self.transaction(|session| {
            let data = session.read_lease(region)?;
            consume(data.as_slice())
        })
    }

    /// Completed, immutable payload with no copy and no native handles. Holding
    /// this lease reserves its pool slot/bytes until the last clone is dropped.
    pub fn read_lease(&mut self, region: RemoteRegion) -> Result<CompletedRead, SessionError> {
        let mut result = None;
        let options = ReadOptions {
            max_in_flight: 1,
            max_in_flight_bytes: region
                .length
                .try_into()
                .map_err(|_| SessionError::Protocol("region length overflows usize"))?,
        };
        self.read_many_with(std::slice::from_ref(&region), options, |_, data| {
            result = Some(data);
            Ok(())
        })?;
        result.ok_or(SessionError::Protocol("read completed without data"))
    }

    /// Read owned bytes for callers that need a copy independent of the pool.
    /// Existing behavior is preserved; read_with/read_lease avoid this copy.
    #[tracing::instrument(level = "debug", skip_all, fields(bytes = region.length))]
    pub fn read(&mut self, region: RemoteRegion) -> Result<Vec<u8>, SessionError> {
        self.read_with(region, |data| {
            let mut bytes = zeroed(data.len())?;
            bytes.copy_from_slice(data);
            Ok(bytes)
        })
    }

    /// Bounded, possibly out-of-order Reads of already negotiated regions. Each
    /// callback receives its input index and an immutable completed lease. Drop
    /// leases promptly to replenish pool capacity. Callback Cancelled drains all
    /// submitted Reads before returning Cancelled, keeping the session reusable.
    /// Other errors poison it and retain unconfirmed native operations.
    pub fn read_many_with(
        &mut self,
        regions: &[RemoteRegion],
        options: ReadOptions,
        consume: impl FnMut(usize, CompletedRead) -> Result<(), SessionError>,
    ) -> Result<(), SessionError> {
        self.read_many_until(
            Instant::now() + self.timeout,
            regions,
            options,
            consume,
            || false,
        )
    }

    pub(crate) fn read_many_until(
        &mut self,
        deadline: Instant,
        regions: &[RemoteRegion],
        options: ReadOptions,
        consume: impl FnMut(usize, CompletedRead) -> Result<(), SessionError>,
        cancelled: impl Fn() -> bool,
    ) -> Result<(), SessionError> {
        let drained_cancel = self.transaction_until(deadline, |session| {
            session.drive_reads(regions, options, consume, cancelled)
        })?;
        if drained_cancel {
            Err(SessionError::Cancelled)
        } else {
            Ok(())
        }
    }

    fn window(&self) -> Result<&ReadWindow<NativeRead>, SessionError> {
        self.resources()?
            .reads
            .as_ref()
            .ok_or(SessionError::Protocol("missing Read window"))
    }
    fn window_mut(&mut self) -> Result<&mut ReadWindow<NativeRead>, SessionError> {
        self.resources_mut()?
            .reads
            .as_mut()
            .ok_or(SessionError::Protocol("missing Read window"))
    }

    fn drive_reads(
        &mut self,
        regions: &[RemoteRegion],
        options: ReadOptions,
        mut consume: impl FnMut(usize, CompletedRead) -> Result<(), SessionError>,
        cancelled: impl Fn() -> bool,
    ) -> Result<bool, SessionError> {
        options.validate(self.resources()?.budget.limits())?;
        if regions.len() > MAX_READ_BATCH {
            return Err(SessionError::InvalidOptions(
                "Read batch exceeds 65536 regions",
            ));
        }
        // Validate the entire batch before the first post, including requests
        // larger than the byte window (which otherwise could never be admitted).
        for region in regions {
            let length = region.validate(options.max_in_flight_bytes)?;
            self.validate_length(length)?;
        }
        if self.resources()?.pending.is_some()
            || self
                .resources()?
                .reads
                .as_ref()
                .is_some_and(|w| !w.is_empty())
        {
            return Err(SessionError::Protocol(
                "another native operation is pending",
            ));
        }
        {
            let r = self.resources_mut()?;
            let additional = r
                .budget
                .limits()
                .max_read_buffers
                .saturating_sub(r.read_pool.len());
            r.read_pool
                .try_reserve(additional)
                .map_err(|_| SessionError::Allocation {
                    bytes: additional.saturating_mul(std::mem::size_of::<RegisteredBuffer>()),
                })?;
            let window = match r.spare_reads.take() {
                Some(mut window) => {
                    window.prepare(options)?;
                    window
                }
                None => ReadWindow::new(options)?,
            };
            r.reads = Some(window);
        }
        let deadline = self
            .deadline
            .ok_or(SessionError::Protocol("missing Read deadline"))?;
        let mut next = 0;
        let mut draining = false;
        let mut progress = Progress::default();
        let mut completions = [Completion::default(); CQ_BATCH_SIZE];
        loop {
            draining |= cancelled();
            if draining {
                // EAGAIN means this operation was not accepted. Its context and
                // allocation can return to the pool without a CQ completion.
                if let Some(context) = self
                    .window()?
                    .entries
                    .iter()
                    .find(|e| !e.value.posted)
                    .map(|e| e.context)
                {
                    let entry = self.window_mut()?.take(context)?;
                    self.resources_mut()?.read_pool.push(entry.value.buffer);
                }
            }
            if self.window()?.is_empty() && (draining || next == regions.len()) {
                let r = self.resources_mut()?;
                r.spare_reads = r.reads.take();
                return Ok(draining);
            }
            if Instant::now() >= deadline {
                return Err(SessionError::Timeout("Read pipeline"));
            }
            let mut advanced = false;
            loop {
                if draining {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(SessionError::Timeout("submit pipelined Read"));
                }
                let unposted = self.window()?.entries.iter().position(|e| !e.value.posted);
                if unposted.is_none() {
                    if next == regions.len() {
                        break;
                    }
                    let length = regions[next].length as usize; // validated above
                    if !self.window()?.can_push(length) {
                        break;
                    }
                    let buffer = match self.take_read_buffer(length) {
                        Ok(Some(buffer)) => buffer,
                        Ok(None) => break, // Leased buffers/pool pressure: drive progress.
                        Err(SessionError::ResourceLimit { .. }) if !self.window()?.is_empty() => {
                            break
                        }
                        Err(error) => return Err(error),
                    };
                    let context = match Context::new() {
                        Ok(context) => context,
                        Err(error) => {
                            self.resources_mut()?.read_pool.push(buffer);
                            return Err(error);
                        }
                    };
                    self.window_mut()?.push(
                        context.pointer() as usize,
                        length,
                        NativeRead {
                            buffer,
                            context,
                            region: regions[next],
                            index: next,
                            posted: false,
                        },
                    )?;
                }
                if Instant::now() >= deadline {
                    return Err(SessionError::Timeout("submit pipelined Read"));
                }
                let ep = self.resources()?.ep.handle.as_ptr()?;
                let entry = self
                    .window_mut()?
                    .entries
                    .iter_mut()
                    .find(|e| !e.value.posted)
                    .ok_or(SessionError::Protocol("missing unposted Read"))?;
                let read = &mut entry.value;
                // SAFETY: the window already owns this exclusively writable
                // allocation, MR and stable context; no lease exists before its
                // matching completion. Failed posts retain ownership in Resources.
                let code = unsafe {
                    d7y_rdma_post_read(
                        ep,
                        read.buffer.pointer()?,
                        entry.length,
                        read.buffer.descriptor()?,
                        read.region.address,
                        read.region.key,
                        read.context.pointer(),
                    )
                };
                if code == 0 {
                    read.posted = true;
                    next += 1;
                    advanced = true;
                } else if unsafe { d7y_rdma_is_again(code) } != 0 {
                    break;
                } else {
                    check("post pipelined Read", code, 0)?;
                }
            }
            // Drain both queues for manual-progress providers. Errors keep every
            // unconfirmed operation, even completions already removed in a batch.
            for is_rx in [false, true] {
                let count = {
                    let r = self.resources()?;
                    if is_rx {
                        r.rx.poll_many(&mut completions)?
                    } else {
                        r.tx.poll_many(&mut completions)?
                    }
                };
                advanced |= count > 0;
                for event in &completions[..count] {
                    let matched = self
                        .window()?
                        .entries
                        .iter()
                        .any(|e| e.context == event.context as usize && e.value.posted);
                    if is_rx
                        || !matched
                        || unsafe { d7y_rdma_completion_matches(event.flags, Kind::Read as i32) }
                            == 0
                    {
                        return Err(SessionError::CompletionMismatch {
                            operation: "read",
                            queue: if is_rx { "receive" } else { "transmit" },
                            flags: event.flags,
                            context_matches: matched,
                        });
                    }
                    let entry = self.window_mut()?.take(event.context as usize)?;
                    let lease = entry.value.buffer.completed(entry.length)?;
                    let index = entry.value.index;
                    self.resources_mut()?.read_pool.push(entry.value.buffer);
                    // entry.value.context drops only after confirmed completion.
                    draining |= cancelled();
                    if !draining {
                        match consume(index, lease) {
                            Ok(()) => {}
                            Err(SessionError::Cancelled) => draining = true,
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            if self.window()?.is_empty() && (draining || next == regions.len()) {
                let r = self.resources_mut()?;
                r.spare_reads = r.reads.take();
                return Ok(draining);
            }
            Self::check_peer(self.resources()?)?;
            if advanced {
                progress = Progress::default();
            } else {
                progress.wait(deadline, "Read progress/buffer return")?;
            }
        }
    }

    /// Idle registrations may be reused only when all completed leases dropped.
    fn take_read_buffer(
        &mut self,
        length: usize,
    ) -> Result<Option<RegisteredBuffer>, SessionError> {
        let r = self.resources_mut()?;
        let mut best = None;
        for (index, buffer) in r.read_pool.iter().enumerate() {
            if !buffer.is_leased() {
                let size = buffer.data()?.len();
                if size >= length && best.is_none_or(|(_, old_size)| size < old_size) {
                    best = Some((index, size));
                }
            }
        }
        if let Some((index, _)) = best {
            return Ok(Some(r.read_pool.swap_remove(index)));
        }
        let limits = r.budget.limits();
        let max_buffers = limits.max_read_buffers;
        let max_bytes = limits.max_read_buffer_bytes;
        let pending = r.reads.as_ref().map_or(0, |w| w.entries.len());
        // Keep other useful size classes until count/byte/shared-budget pressure
        // requires eviction. Growing one slot must not deregister the whole pool.
        while r.read_pool.len() + pending >= max_buffers
            || length > max_bytes.saturating_sub(r.read_pool_bytes)
        {
            if !evict_idle_read(r)? {
                return Ok(None);
            }
        }
        let permit = loop {
            match r.budget.reserve(length) {
                Ok(permit) => break permit,
                Err(error @ SessionError::ResourceLimit { .. }) => {
                    if evict_idle_read(r)? {
                        continue;
                    }
                    if r.read_pool.iter().any(RegisteredBuffer::is_leased) {
                        return Ok(None);
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        };
        let key = r.next_key;
        r.next_key = key
            .checked_add(1)
            .ok_or(SessionError::Protocol("MR key exhausted"))?;
        let buffer = RegisteredBuffer::register_reserved(
            &r.domain,
            zeroed(length)?.into_boxed_slice(),
            key,
            false,
            permit,
        )?;
        r.read_pool_bytes += length;
        Ok(Some(buffer))
    }
}

/// Pool entries are completed registrations; leased entries cannot be evicted.
fn evict_idle_read(r: &mut Resources) -> Result<bool, SessionError> {
    let Some(index) = r.read_pool.iter().position(|b| !b.is_leased()) else {
        return Ok(false);
    };
    let size = r.read_pool[index].data()?.len();
    r.read_pool[index].try_close()?;
    r.read_pool.swap_remove(index);
    r.read_pool_bytes -= size;
    Ok(true)
}
