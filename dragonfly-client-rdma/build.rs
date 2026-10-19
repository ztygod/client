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

fn main() {
    println!("cargo:rerun-if-changed=c/probe.c");
    println!("cargo:rerun-if-changed=c/probe.h");
    println!("cargo:rerun-if-changed=c/resource.c");
    println!("cargo:rerun-if-changed=c/resource.h");

    #[cfg(feature = "rdma")]
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        let mut pkg_config = pkg_config::Config::new();
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

        let mut build = cc::Build::new();
        build
            .file("c/probe.c")
            .file("c/resource.c")
            .std("c11")
            .warnings(true)
            .warnings_into_errors(true);
        for path in library.include_paths {
            build.include(path);
        }
        for (name, value) in library.defines {
            build.define(&name, value.as_deref());
        }
        build.compile("dragonfly_rdma_probe");

        // GNU --as-needed requires libfabric after the static shim that references it.
        pkg_config
            .cargo_metadata(true)
            .probe("libfabric")
            .expect("libfabric was already located before compiling the shim");
    }
}
