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

//! Build native RDMA dependencies when Cargo compiles this crate.
//!
//! With the rdma feature enabled for a Linux target, locate libfabric >= 1.17,
//! compile the C shim under c/, and emit link configuration for Rust FFI calls.
//! Default builds and non-Linux targets skip native setup and need no libfabric.

fn main() {
    // Rust calls these five shim modules through FFI. Rebuild when their C
    // sources or headers change.
    const SHIM_FILES: [&str; 5] = ["probe", "resource", "endpoint", "memory", "transfer"];
    for name in SHIM_FILES {
        println!("cargo:rerun-if-changed=c/{name}.c");
        println!("cargo:rerun-if-changed=c/{name}.h");
    }

    // The feature enables the optional cc/pkg-config build dependencies. Check
    // Cargo's target OS, rather than the build host OS, for cross-compilation.
    #[cfg(feature = "rdma")]
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        let mut pkg_config = pkg_config::Config::new();
        // Check the version and collect include paths and preprocessor defines.
        // Defer libfabric link directives until after the static shim is built.
        let library = pkg_config
            .atleast_version("1.17")
            .cargo_metadata(false)
            .probe("libfabric")
            .unwrap_or_else(|err| {
                panic!(
                    "the Linux rdma feature requires libfabric >= 1.17 and pkg-config; \
                     install libfabric-dev and pkg-config, or set PKG_CONFIG_PATH: {err}"
                )
            });

        // Compile the C shim into a static library for Rust extern declarations.
        // cc writes artifacts to Cargo's OUT_DIR and emits their link directives.
        let mut build = cc::Build::new();
        for name in SHIM_FILES {
            build.file(format!("c/{name}.c"));
        }
        // Use C11 and treat compiler warnings as errors to catch shim issues.
        build.std("c11").warnings(true).warnings_into_errors(true);
        // Honor pkg-config settings, including custom libfabric installations.
        for path in library.include_paths {
            build.include(path);
        }
        for (name, value) in library.defines {
            build.define(&name, value.as_deref());
        }
        build.compile("dragonfly_rdma_shim");

        // Query again to emit libfabric search paths and link directives.
        // GNU --as-needed requires libfabric after the static shim that references
        // it; otherwise the dependency may be discarded or symbols left unresolved.
        pkg_config
            .cargo_metadata(true)
            .probe("libfabric")
            .expect("libfabric was already located before compiling the shim");
    }
}
