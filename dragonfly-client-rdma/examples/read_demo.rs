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

//! Two-host verbs Read diagnostic with reusable sessions and budget-aware rotation.

mod support;
use dragonfly_client_rdma::{Listener, Session, SessionError, SessionOptions, MAX_TRANSFER_SIZE};
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;
use support::{checksum, pattern, verify, Message};

#[derive(Default)]
struct Report {
    bytes: u64,
    requests: usize,
    checksum: u64,
}
enum ServerStep {
    Verified(usize, u64),
    Rotate,
    Done,
}
enum ClientStep {
    Verified(u64),
    Rotate,
}

fn serve(
    session: &mut Session,
    release_exports: bool,
    can_rotate: bool,
) -> Result<ServerStep, SessionError> {
    session.transaction(|session| {
        let request = Message::decode(&session.receive()?)?;
        if request == Message::Done {
            session.send(&Message::Done.encode())?;
            return Ok(ServerStep::Done);
        }
        let Message::Request { length, seed } = request else {
            return Err(SessionError::Protocol("expected request"));
        };
        let length_usize = support::validate_length(length)?;
        // Reserve before generating bytes, while control buffers can still carry
        // the drain exchange. Do not swallow unrelated native/allocation errors.
        let mut buffer = match session.reserve_export(length_usize) {
            Ok(buffer) => buffer,
            Err(error @ SessionError::ResourceLimit { .. }) => {
                if !can_rotate {
                    session.send(&Message::Unavailable.encode())?;
                    return Err(error);
                }
                session.send(&Message::Rotate.encode())?;
                if Message::decode(&session.receive()?)? != Message::RotateAck {
                    return Err(SessionError::Protocol("expected rotation acknowledgement"));
                }
                return Ok(ServerStep::Rotate);
            }
            Err(error) => return Err(error),
        };
        for (i, byte) in buffer.as_mut_slice().iter_mut().enumerate() {
            *byte = pattern(i, seed);
        }
        let digest = checksum(buffer.as_slice());
        let export = session.export_buffer(buffer)?;
        session.send(&Message::Region { export, seed }.encode())?;
        let expected = Message::Release {
            id: export.id,
            length,
            checksum: digest,
        };
        if Message::decode(&session.receive()?)? != expected {
            return Err(SessionError::Protocol("release does not match export"));
        }
        if release_exports {
            // SAFETY: opt-in diagnostic for trusted peers only. Its matching
            // client waits for successful Read completion before RELEASE, never
            // pipelines reads, and discards the address/key after this request.
            // This is not a safe reclamation protocol for arbitrary peers.
            unsafe {
                session.release_export(export.id)?;
            }
        }
        session.send(
            &Message::Released {
                id: export.id,
                length,
                checksum: digest,
            }
            .encode(),
        )?;
        Ok(ServerStep::Verified(length_usize, digest))
    })
}

fn read_verify(
    session: &mut Session,
    length: usize,
    seed: u64,
) -> Result<ClientStep, SessionError> {
    session.transaction(|session| {
        session.send(
            &Message::Request {
                length: length as u64,
                seed,
            }
            .encode(),
        )?;
        let response = Message::decode(&session.receive()?)?;
        if response == Message::Rotate {
            session.send(&Message::RotateAck.encode())?;
            return Ok(ClientStep::Rotate);
        }
        if response == Message::Unavailable {
            return Err(SessionError::Protocol(
                "server export budget cannot admit request",
            ));
        }
        let Message::Region {
            export,
            seed: offered_seed,
        } = response
        else {
            return Err(SessionError::Protocol("expected region"));
        };
        if export.region.length != length as u64 || offered_seed != seed {
            return Err(SessionError::Protocol("region does not match request"));
        }
        let data = session.read(export.region)?;
        verify(&data, seed)?;
        let digest = checksum(&data);
        // No code path uses export.region after sending this promise. Read has
        // returned only after its matching successful CQ completion.
        session.send(
            &Message::Release {
                id: export.id,
                length: length as u64,
                checksum: digest,
            }
            .encode(),
        )?;
        let expected = Message::Released {
            id: export.id,
            length: length as u64,
            checksum: digest,
        };
        if Message::decode(&session.receive()?)? != expected {
            return Err(SessionError::Protocol(
                "expected matching release confirmation",
            ));
        }
        Ok(ClientStep::Verified(digest))
    })
}

fn server(options: &SessionOptions, release_exports: bool) -> Result<Report, SessionError> {
    let mut listener = Listener::bind(options)?;
    println!(
        "LISTENING {}:{} provider=verbs domain={} release_exports={release_exports}",
        options.node,
        options.port,
        options.domain.as_deref().unwrap_or("auto")
    );
    let result: Result<Report, SessionError> = (|| {
        let mut report = Report::default();
        loop {
            let mut session = listener.accept()?;
            let mut retained = false;
            let connection: Result<bool, SessionError> = (|| loop {
                match serve(&mut session, release_exports, retained)? {
                    ServerStep::Verified(bytes, digest) => {
                        report.requests += 1;
                        report.bytes += bytes as u64;
                        report.checksum = digest;
                        retained |= !release_exports;
                        let usage = session.resource_usage()?;
                        println!("REQUEST_VERIFIED request={} registered_bytes={} registered_regions={} cached_export_bytes={}",
                            report.requests, usage.registered_bytes, usage.registered_regions, usage.cached_export_bytes);
                    }
                    ServerStep::Rotate => break Ok(false),
                    ServerStep::Done => break Ok(true),
                }
            })();
            let cleanup = session.try_close();
            if let Err(error) = &cleanup {
                eprintln!("session cleanup: {error}");
            }
            let done = connection?;
            cleanup?;
            if done {
                return Ok(report);
            }
            println!("ROTATED; accepting replacement connection");
        }
    })();
    let cleanup = listener.try_close();
    if let Err(error) = &cleanup {
        eprintln!("listener cleanup: {error}");
    }
    let report = result?;
    cleanup?;
    Ok(report)
}

fn client(
    options: &SessionOptions,
    size: usize,
    seed: u64,
    requests: usize,
) -> Result<Report, SessionError> {
    let mut session = Session::connect(options)?;
    let result: Result<Report, SessionError> = (|| {
        let mut report = Report::default();
        let mut rotated_without_progress = false;
        while report.requests < requests {
            match read_verify(
                &mut session,
                size,
                seed.wrapping_add(report.requests as u64),
            )? {
                ClientStep::Verified(digest) => {
                    report.requests += 1;
                    report.bytes += size as u64;
                    report.checksum = digest;
                    rotated_without_progress = false;
                }
                ClientStep::Rotate => {
                    // A second rotation before any progress cannot reclaim this
                    // request's budget; fail instead of reconnecting forever.
                    if rotated_without_progress {
                        return Err(SessionError::Protocol("rotation made no progress"));
                    }
                    session.try_close()?;
                    session = Session::connect(options)?;
                    rotated_without_progress = true;
                }
            }
        }
        session.transaction(|session| {
            session.send(&Message::Done.encode())?;
            if Message::decode(&session.receive()?)? != Message::Done {
                return Err(SessionError::Protocol("expected final acknowledgement"));
            }
            Ok(())
        })?;
        Ok(report)
    })();
    let cleanup = session.try_close();
    if let Err(error) = &cleanup {
        eprintln!("session cleanup: {error}");
    }
    let report = result?;
    cleanup?;
    Ok(report)
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let Some(role) = args.next() else {
        return Err("expected server or client; use --help".into());
    };
    if role == "--help" || role == "-h" {
        println!("Usage: read_demo server|client --address IP [--port 7471] [--domain DOMAIN] [--timeout-secs 30]
\
                  Client: [--size 4194304] [--seed 42] [--requests 1]
\
                  Server: [--registered-bytes 134217728] [--registered-regions 1024] [--release-exports]
\
                  Default: retain exports and rotate before budget exhaustion.
\
                  --release-exports: trusted-peer early release with a bounded deregistered allocation cache.
\
                  Requires Linux, --features rdma, and working verbs interfaces on both hosts.");
        return Ok(());
    }
    if role != "server" && role != "client" {
        return Err("role must be server or client".into());
    }
    let mut options = SessionOptions::new("", 7471);
    let (mut size, mut seed, mut requests) = (4 * 1024 * 1024, 42u64, 1usize);
    let mut release_exports = false;
    let mut seen = std::collections::HashSet::new();
    while let Some(arg) = args.next() {
        if !seen.insert(arg.clone()) {
            return Err(format!("duplicate option: {arg}").into());
        }
        if arg == "--release-exports" && role == "server" {
            release_exports = true;
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| format!("{arg} requires a value"))?;
        match arg.as_str() {
            "--address" => options.node = value,
            "--port" => options.port = value.parse()?,
            "--domain" => options.domain = Some(value),
            "--timeout-secs" => options.timeout = Duration::from_secs(value.parse()?),
            "--size" if role == "client" => size = value.parse()?,
            "--seed" if role == "client" => seed = value.parse()?,
            "--requests" if role == "client" => requests = value.parse()?,
            "--registered-bytes" if role == "server" => {
                options.limits.max_registered_bytes = value.parse()?
            }
            "--registered-regions" if role == "server" => {
                options.limits.max_registered_regions = value.parse()?
            }
            _ => return Err(format!("unsupported option for {role}: {arg}").into()),
        }
    }
    if size == 0 || size > MAX_TRANSFER_SIZE || requests == 0 || requests > 1_000_000 {
        return Err("size must be in 1..=67108864; requests must be in 1..=1000000".into());
    }
    let report = if role == "server" {
        server(&options, release_exports)?
    } else {
        client(&options, size, seed, requests)?
    };
    println!(
        "VERIFIED requests={} bytes={} checksum={:016x}; resources closed",
        report.requests, report.bytes, report.checksum
    );
    Ok(())
}
fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("read_demo failed: {error}");
            ExitCode::FAILURE
        }
    }
}
