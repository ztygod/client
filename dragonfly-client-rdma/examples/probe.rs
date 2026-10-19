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

use dragonfly_client_rdma::{
    probe, ProbeOptions, ProbeResult, RdmaResources, ResourceOptions, UnavailableReason,
};
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut options = ProbeOptions::default();
    let mut open_resources = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--open-resources" => open_resources = true,
            "--provider" | "--domain" => {
                let Some(value) = args.next() else {
                    eprintln!("{arg} requires a value");
                    return ExitCode::from(2);
                };
                if arg == "--provider" {
                    options.provider = value;
                } else {
                    options.domain = Some(value);
                }
            }
            "-h" | "--help" => {
                println!("Usage: probe [--provider verbs] [--domain DOMAIN] [--open-resources]");
                return ExitCode::SUCCESS;
            }
            _ => {
                eprintln!("unknown argument: {arg}");
                return ExitCode::from(2);
            }
        }
    }

    match probe(&options) {
        Ok(ProbeResult::Available(candidates)) => {
            println!("Found {} matching interface(s)", candidates.len());
            println!("Candidates only: hardware offload and peer connectivity are not verified.");
            for candidate in candidates {
                println!("provider: {}", candidate.provider);
                println!("  domain: {}", candidate.domain);
                println!(
                    "  device: {}",
                    candidate.device.as_deref().unwrap_or("unknown")
                );
                println!("  endpoint: FI_EP_MSG");
                println!("  message / RMA / read / remote read / remote communication: supported");
                println!("  caps: {:#x}", candidate.raw_capabilities);
                println!("  required mode: {:#x}", candidate.mode);
                println!("  required mr_mode: {:#x}", candidate.mr_mode);
                println!(
                    "  protocol: {} (version {})",
                    candidate.protocol, candidate.protocol_version
                );
                println!("  threading: {}", candidate.threading);
                println!("  control progress: {}", candidate.control_progress);
                println!("  data progress: {}", candidate.data_progress);
                if open_resources {
                    match RdmaResources::open(&candidate, ResourceOptions::default()) {
                        Ok(mut resources) => {
                            println!("  opened Fabric, Domain, TX CQ and RX CQ");
                            if let Err(error) = resources.try_close() {
                                eprintln!("resource cleanup failed: {error}");
                                return ExitCode::FAILURE;
                            }
                            println!("  closed TX CQ, RX CQ, Domain and Fabric");
                        }
                        Err(error) => {
                            eprintln!("resource creation failed: {error}");
                            return ExitCode::FAILURE;
                        }
                    }
                }
            }
            ExitCode::SUCCESS
        }
        Ok(ProbeResult::Unavailable(reason)) => {
            let message = match reason {
                UnavailableReason::UnsupportedPlatform => "this probe implementation requires Linux",
                UnavailableReason::FeatureDisabled => "rebuild with --features rdma",
                UnavailableReason::NoMatchingInterface => {
                    "no interface matches the requested provider, domain, endpoint, and capabilities"
                }
            };
            println!("RDMA probe unavailable: {message}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("RDMA probe failed: {err}");
            ExitCode::FAILURE
        }
    }
}
