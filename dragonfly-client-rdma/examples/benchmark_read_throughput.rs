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

//! Compare RDMA Read API throughput across two hosts.
//!
//! - Compare copy (owned Vec), borrow (synchronous borrowed consumption), window
//!   (bounded in-flight Reads), and stream (asynchronous completion leases) to
//!   observe the effects of payload copies and pipelining.
//! - Warm the buffer pool and verify every byte before timing. Timed iterations
//!   sample bytes and use black_box.
//! - Report batch throughput and registered/reserved resource counters. Payload
//!   size, iteration count, and window depth are configurable.
//! - Restart the server for each run. Exports remain registered until connection
//!   close; this benchmark does not exercise per-request release or rotation.
//! - Requires Linux, --features rdma, reachable trusted verbs peers, and release
//!   builds on both hosts. Results exclude storage and P99 latency measurements
//!   and do not independently establish hardware offload.

use dragonfly_client_rdma::{
    Listener, ReadOptions, RemoteRegion, SessionError, SessionOptions, SessionWorker,
    MAX_TRANSFER_SIZE,
};
use std::error::Error;
use std::process::ExitCode;
use std::time::Instant;

fn encode_region(region: RemoteRegion) -> Vec<u8> {
    [
        b"D7YB\0\x01\0\0".as_slice(),
        &region.length.to_be_bytes(),
        &region.address.to_be_bytes(),
        &region.key.to_be_bytes(),
    ]
    .concat()
}
fn decode_region(frame: &[u8]) -> Result<RemoteRegion, SessionError> {
    if frame.len() != 32 || &frame[..8] != b"D7YB\0\x01\0\0" {
        return Err(SessionError::Protocol("invalid benchmark region frame"));
    }
    let region = RemoteRegion {
        length: u64::from_be_bytes(frame[8..16].try_into().unwrap()),
        address: u64::from_be_bytes(frame[16..24].try_into().unwrap()),
        key: u64::from_be_bytes(frame[24..32].try_into().unwrap()),
    };
    region.validate(MAX_TRANSFER_SIZE)?;
    Ok(region)
}
fn verify(data: &[u8]) -> Result<(), SessionError> {
    if !data
        .iter()
        .enumerate()
        .all(|(i, byte)| *byte == (i % 251) as u8)
    {
        return Err(SessionError::Protocol("benchmark byte comparison failed"));
    }
    Ok(())
}
fn observe(data: &[u8]) -> u64 {
    std::hint::black_box(data);
    u64::from(data[0]) + u64::from(data[data.len() - 1])
}
fn serve(options: &SessionOptions) -> Result<(), SessionError> {
    let mut listener = Listener::bind(options)?;
    println!("LISTENING {}:{}", options.node, options.port);
    let mut session = listener.accept()?;
    let result = session.transaction(|session| {
        let frame = session.receive()?;
        if frame.len() != 16 || &frame[..8] != b"D7YB\0\x01\0\0" {
            return Err(SessionError::Protocol("invalid benchmark request"));
        }
        let length = u64::from_be_bytes(frame[8..].try_into().unwrap());
        let length = usize::try_from(length)
            .map_err(|_| SessionError::Protocol("benchmark size overflow"))?;
        if length == 0 || length > MAX_TRANSFER_SIZE {
            return Err(SessionError::InvalidOptions("invalid benchmark size"));
        }
        let mut buffer = session.reserve_export(length)?;
        for (i, byte) in buffer.as_mut_slice().iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }
        let export = session.export_buffer(buffer)?;
        session.send(&encode_region(export.region))?;
        // Receiving drives remote manual progress throughout the benchmark. The
        // caller supplies a per-benchmark timeout adequate for all iterations.
        if session.receive()? != b"D7YBdone" {
            return Err(SessionError::Protocol("expected benchmark completion"));
        }
        session.send(b"D7YBdone")
    });
    let cleanup = session.try_close();
    let listener_cleanup = listener.try_close();
    result?;
    cleanup?;
    listener_cleanup?;
    Ok(())
}

async fn measure(config: &Config) -> Result<(), SessionError> {
    let mut worker = SessionWorker::connect(config.options.clone(), 4).await?;
    let active_window = if config.mode == "copy" || config.mode == "borrow" {
        1
    } else {
        config.window
    };
    let options = ReadOptions {
        max_in_flight: active_window,
        max_in_flight_bytes: config.size * active_window,
    };
    let size = config.size;
    let result = async {
        let region = worker.execute(move |session| {
            let request = [b"D7YB\0\x01\0\0".as_slice(), &(size as u64).to_be_bytes()].concat();
            session.send(&request)?;
            let region = decode_region(&session.receive()?)?;
            if region.length != size as u64 { return Err(SessionError::Protocol("benchmark size mismatch")); }
            Ok(region)
        }).await?;
        // Warm every slot and compare every byte, outside the timed interval.
        worker.execute(move |session| {
            session.read_many_with(&vec![region; active_window * 2], options, |_, data| verify(&data))
        }).await?;
        let regions = vec![region; config.iterations];
        let started = Instant::now();
        let observed = match config.mode.as_str() {
            "copy" => worker.execute(move |session| {
                let mut observed = 0u64;
                for region in regions { observed = observed.wrapping_add(observe(&session.read(region)?)); }
                Ok(observed)
            }).await?,
            "borrow" => worker.execute(move |session| {
                let mut observed = 0u64;
                for region in regions { observed = observed.wrapping_add(session.read_with(region, |bytes| Ok(observe(bytes)))?); }
                Ok(observed)
            }).await?,
            "window" => worker.execute(move |session| {
                let mut observed = 0u64;
                session.read_many_with(&regions, options, |_, data| { observed = observed.wrapping_add(observe(&data)); Ok(()) })?;
                Ok(observed)
            }).await?,
            "stream" => {
                let mut stream = worker.read_stream(regions, options).await?;
                let mut observed = 0u64;
                let mut completed = 0;
                while let Some(completion) = stream.next().await? {
                    observed = observed.wrapping_add(observe(&completion.data));
                    completed += 1;
                }
                if completed != config.iterations { return Err(SessionError::Protocol("incomplete benchmark stream")); }
                observed
            }
            _ => return Err(SessionError::InvalidOptions("unknown benchmark mode")),
        };
        let elapsed = started.elapsed();
        let bytes = config.size as u64 * config.iterations as u64;
        let usage = worker.resource_usage();
        println!("RESULT mode={} window={} reads={} bytes={} seconds={:.6} MiB_per_second={:.2} registered_bytes={} registered_regions={} observed={}",
            config.mode, active_window, config.iterations, bytes, elapsed.as_secs_f64(),
            bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::EPSILON),
            usage.registered_bytes, usage.registered_regions, observed);
        worker.execute(|session| {
            session.send(b"D7YBdone")?;
            if session.receive()? != b"D7YBdone" { return Err(SessionError::Protocol("expected benchmark acknowledgement")); }
            Ok(())
        }).await
    }.await;
    let cleanup = worker.close().await;
    result?;
    cleanup?;
    Ok(())
}
struct Config {
    role: String,
    options: SessionOptions,
    mode: String,
    size: usize,
    iterations: usize,
    window: usize,
}
fn parse(args: impl Iterator<Item = String>) -> Result<Config, Box<dyn Error>> {
    let mut args = args;
    let role = args.next().ok_or("expected server or client; use --help")?;
    if role != "server" && role != "client" {
        return Err("expected server or client".into());
    }
    let mut config = Config {
        role,
        options: SessionOptions::new("", 7472),
        mode: "window".into(),
        size: 256 * 1024,
        iterations: 1024,
        window: 4,
    };
    config.options.timeout = std::time::Duration::from_secs(120);
    let mut seen = std::collections::HashSet::new();
    while let Some(arg) = args.next() {
        if !seen.insert(arg.clone()) {
            return Err(format!("duplicate option: {arg}").into());
        }
        let value = args
            .next()
            .ok_or_else(|| format!("{arg} requires a value"))?;
        match arg.as_str() {
            "--address" => config.options.node = value,
            "--port" => config.options.port = value.parse()?,
            "--domain" => config.options.domain = Some(value),
            "--timeout-secs" => {
                config.options.timeout = std::time::Duration::from_secs(value.parse()?)
            }
            "--mode" if config.role == "client" => config.mode = value,
            "--size" if config.role == "client" => config.size = value.parse()?,
            "--iterations" if config.role == "client" => config.iterations = value.parse()?,
            "--window" if config.role == "client" => config.window = value.parse()?,
            _ => return Err(format!("unsupported option: {arg}").into()),
        }
    }
    if !["copy", "borrow", "window", "stream"].contains(&config.mode.as_str())
        || config.size == 0
        || config.size > MAX_TRANSFER_SIZE
        || config.iterations == 0
        || config.iterations > 65536
        || config.window == 0
        || config.window > config.options.limits.max_read_buffers
    {
        return Err("invalid mode, size, iterations or window".into());
    }
    let active_window = if ["copy", "borrow"].contains(&config.mode.as_str()) {
        1
    } else {
        config.window
    };
    if config
        .size
        .checked_mul(active_window)
        .is_none_or(|n| n > config.options.limits.max_read_buffer_bytes)
    {
        return Err("size * active window exceeds Read pool byte limit".into());
    }
    Ok(config)
}
#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args()
        .nth(1)
        .is_some_and(|s| s == "--help" || s == "-h")
    {
        println!("Usage: benchmark_read_throughput server|client --address IP [--domain DOMAIN] [--port 7472] [--timeout-secs 120]\n\
                  Client: [--mode copy|borrow|window|stream] [--size 262144] [--iterations 1024] [--window 4]\n\
                  Restart the server for each run. Requires working verbs peers. Reports warm throughput; verify hardware and use release builds.");
        return ExitCode::SUCCESS;
    }
    let result: Result<(), Box<dyn Error>> = async {
        let config = parse(std::env::args().skip(1))?;
        if config.role == "server" {
            serve(&config.options)?;
        } else {
            measure(&config).await?;
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("benchmark_read_throughput failed: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn benchmark_wire_format_checks_identity_bounds_and_version() {
        let region = RemoteRegion {
            length: 4096,
            address: 123,
            key: 456,
        };
        assert_eq!(decode_region(&encode_region(region)).unwrap(), region);
        let mut bad = encode_region(region);
        bad[5] = 2;
        assert!(decode_region(&bad).is_err());
        assert!(decode_region(&encode_region(RemoteRegion {
            address: u64::MAX,
            ..region
        }))
        .is_err());
    }
    #[test]
    fn benchmark_rejects_unbounded_work_and_memory() {
        for args in [
            vec!["client", "--iterations", "65537"],
            vec!["client", "--window", "0"],
            vec!["client", "--size", "67108864", "--window", "8"],
        ] {
            assert!(parse(args.into_iter().map(str::to_owned)).is_err());
        }
        assert!(parse(
            ["client", "--mode", "copy", "--size", "67108864"]
                .into_iter()
                .map(str::to_owned)
        )
        .is_ok());
    }
}
