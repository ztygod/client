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

//! Two-host verbs Read prototype. See README for hardware and software setup.

mod support;
use dragonfly_client_rdma::{
    Listener, RemoteRegion, Session, SessionError, SessionOptions, MAX_TRANSFER_SIZE,
};
use support::{checksum, pattern, verify, Message};

#[derive(Debug)]
struct DemoReport {
    bytes: usize,
    checksum: u64,
}

fn serve(session: &mut Session) -> Result<DemoReport, SessionError> {
    session.transaction(|session| {
        let Message::Request { length, seed } = Message::decode(&session.receive()?)? else {
            return Err(SessionError::Protocol("expected request"));
        };
        let length_usize = support::validate_length(length)?;
        let data: Vec<_> = (0..length_usize).map(|i| pattern(i, seed)).collect();
        let digest = checksum(&data);
        let region = session.export(data)?;
        session.send(
            &Message::Region {
                length,
                address: region.address,
                key: region.key,
                seed,
            }
            .encode(),
        )?;
        if Message::decode(&session.receive()?)?
            != (Message::Verified {
                length,
                checksum: digest,
            })
        {
            return Err(SessionError::Protocol("verification mismatch"));
        }
        session.send(
            &Message::Finished {
                length,
                checksum: digest,
            }
            .encode(),
        )?;
        Ok(DemoReport {
            bytes: length_usize,
            checksum: digest,
        })
    })
}
fn read_verify(
    session: &mut Session,
    length: usize,
    seed: u64,
) -> Result<DemoReport, SessionError> {
    session.transaction(|session| {
        session.send(
            &Message::Request {
                length: length as u64,
                seed,
            }
            .encode(),
        )?;
        let Message::Region {
            length: offered,
            address,
            key,
            seed: offered_seed,
        } = Message::decode(&session.receive()?)?
        else {
            return Err(SessionError::Protocol("expected region"));
        };
        if offered != length as u64 || offered_seed != seed {
            return Err(SessionError::Protocol("region does not match request"));
        }
        let data = session.read(RemoteRegion {
            length: offered,
            address,
            key,
        })?;
        verify(&data, seed)?;
        let digest = checksum(&data);
        session.send(
            &Message::Verified {
                length: offered,
                checksum: digest,
            }
            .encode(),
        )?;
        if Message::decode(&session.receive()?)?
            != (Message::Finished {
                length: offered,
                checksum: digest,
            })
        {
            return Err(SessionError::Protocol("expected server confirmation"));
        }
        Ok(DemoReport {
            bytes: length,
            checksum: digest,
        })
    })
}

use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let Some(role) = args.next() else {
        return Err("expected server or client; use --help".into());
    };
    if role == "--help" || role == "-h" {
        println!("Usage: read_demo server|client --address IP [--port 7471] [--domain DOMAIN] [--timeout-secs 30] [--size 4194304] [--seed 42]\n\
                  --address is the local RDMA interface on server and the server RDMA address on client.\n\
                  --size and --seed apply to client. Size is in bytes, 1..=67108864.\n\
                  Requires Linux, --features rdma, and a working verbs provider on both hosts.");
        return Ok(());
    }
    if role != "server" && role != "client" {
        return Err("role must be server or client".into());
    }
    let mut options = SessionOptions::new("", 7471);
    let (mut size, mut seed) = (4 * 1024 * 1024, 42);
    let mut seen = std::collections::HashSet::new();
    while let Some(arg) = args.next() {
        if !seen.insert(arg.clone()) {
            return Err(format!("duplicate option: {arg}").into());
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
            _ => return Err(format!("unsupported option for {role}: {arg}").into()),
        }
    }
    if size == 0 || size > MAX_TRANSFER_SIZE {
        return Err("size must be in 1..=67108864 bytes".into());
    }
    let report = if role == "server" {
        let mut listener = Listener::bind(&options)?;
        println!(
            "LISTENING {}:{} provider=verbs domain={}",
            options.node,
            options.port,
            options.domain.as_deref().unwrap_or("auto")
        );
        let mut session = listener.accept()?;
        println!("CONNECTED; waiting for read request");
        let result = serve(&mut session);
        let cleanup = session.try_close();
        let listener_cleanup = listener.try_close();
        // Report the transfer error as well as any cleanup error.
        if let Err(error) = &cleanup {
            eprintln!("session cleanup: {error}");
        }
        if let Err(error) = &listener_cleanup {
            eprintln!("listener cleanup: {error}");
        }
        let report = result?;
        cleanup?;
        listener_cleanup?;
        report
    } else {
        let mut session = Session::connect(&options)?;
        println!("CONNECTED; requesting {size} bytes via RDMA Read");
        let result = read_verify(&mut session, size, seed);
        let cleanup = session.try_close();
        if let Err(error) = &cleanup {
            eprintln!("session cleanup: {error}");
        }
        let report = result?;
        cleanup?;
        report
    };
    println!(
        "VERIFIED bytes={} checksum={:016x}; resources closed",
        report.bytes, report.checksum
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
