# Dragonfly Client RDMA

Local capability discovery and resource ownership for the initial
**verbs + FI_EP_MSG** transport.
The query requires `FI_MSG | FI_RMA | FI_READ | FI_REMOTE_READ | FI_REMOTE_COMM`.
It returns all matching interfaces with owned provider, domain and optional device
names, plus capability flags and the provider's mode, memory registration,
threading and progress requirements.

Discovery does not create a fabric, domain, endpoint or memory registration. A
candidate does not establish that resources can be opened, that the requirements
are implemented by Dragonfly, that hardware offload is present, or that another
peer is reachable. In particular, verbs can expose software RDMA devices.
Peer capability announcements, transfer selection and TCP fallback are separate
integration work. No existing downloader behavior is changed.

## Run

The default build needs no libfabric installation:

```bash
cargo run -p dragonfly-client-rdma --example probe
```

On Linux it reports `FeatureDisabled`. Other platforms, including macOS with
`--all-features`, report `UnsupportedPlatform` without compiling or linking C.

On Debian 12 / Ubuntu 24.04, install libfabric 1.17 or newer and a C compiler:

```bash
sudo apt-get install build-essential pkg-config libfabric-dev libfabric-bin
cargo run -p dragonfly-client-rdma --features rdma --example probe
cargo run -p dragonfly-client-rdma --features rdma --example probe -- --domain mlx5_0
```

`--provider` currently accepts only `verbs`. Domain names come from libfabric
and need not equal network device names. A missing match returns
`NoMatchingInterface`; this can mean a provider, domain, device, or capability
requirement did not match, not necessarily that no RDMA hardware exists.

The example exits successfully for an expected unavailable result. Invalid
arguments and query failures exit nonzero. Enabling `rdma` on Linux without the
development library produces a build-time dependency error, not an unavailable
device result. For custom installations, configure `PKG_CONFIG_PATH` and the
runtime library search path as appropriate.

## Create resources

On the Linux RDMA host, discover a candidate, open its local resources, and
explicitly close them to check provider cleanup errors:

```bash
cargo run -p dragonfly-client-rdma --features rdma --example probe -- --open-resources
```

The public API offers individual `Fabric`, `Domain`, and `CompletionQueue`
owners, or `RdmaResources` for a fabric, one domain, and separate TX/RX CQs:

```rust,no_run
use dragonfly_client_rdma::{probe, ProbeOptions, ProbeResult, RdmaResources, ResourceOptions};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
if let ProbeResult::Available(candidates) = probe(&ProbeOptions::default())? {
    let mut resources = RdmaResources::open(&candidates[0], ResourceOptions::default())?;
    // Future endpoints and memory registration will use these resources.
    resources.try_close()?;
}
# Ok(())
# }
```

`Fabric::open` re-queries the selected provider/domain and compares every public
candidate field. It rejects a changed or missing candidate with
`CandidateUnavailable`, and retains a deep copy of the matching `fi_info` rather
than reconstructing native attributes from Rust metadata. No fallback to another
provider/domain occurs. Multiple native entries may share the same public
snapshot; in that case the first matching entry is selected.

Each CQ retains its Domain, and each Domain retains its Fabric through `Rc`.
Dropping public parent wrappers early cannot close resources still used by
children. Partial construction and normal scope exit release CQs, then Domain,
then Fabric through RAII. `try_close()` rejects parents with live children and
preserves failed handles for retry. Already closed resources cannot create new
children; repeated successful closes are harmless.

Drop cannot return a provider close error. It logs failures through `tracing`,
does not panic, and retains the failed resource's parent chain instead of closing
parents underneath a live child. This exceptional path can leak native resources;
use `try_close()` when cleanup errors must be handled explicitly.

CQs use `FI_CQ_FORMAT_DATA` and `FI_WAIT_NONE`; `CqOptions::size` specifies a
minimum size, with zero selecting the provider default. Resources are deliberately
neither `Send` nor `Sync` until provider threading/progress requirements are
integrated. Creating these resources does not implement endpoint mode or memory
registration requirements, establish connections, or perform RDMA transfers.
Linux builds without the feature and non-Linux builds return an explicit
`ResourceError::Unavailable` from resource creation.

## Test

```bash
cargo test -p dragonfly-client-rdma
cargo test -p dragonfly-client-rdma --all-features
cargo clippy -p dragonfly-client-rdma --all-targets --all-features -- -D warnings
```

The Linux feature tests exercise the real C shim and libfabric using the TCP
provider, so they require a libfabric build with the TCP provider enabled and a
network interface advertising remote communication. Tests query metadata and
allocate/close real Fabric, Domain and CQ resources without creating endpoints
or connections. Injected allocation failures cover rollback at every stage, and
ownership tests cover early parent drops and retrying failed closes. This is an
internal test only; the public API rejects software TCP as an RDMA provider.
No RDMA hardware is required for these tests. Also compare the public probe with
`fi_info` on the eventual hardware test host:

```bash
fi_info -p verbs -t FI_EP_MSG -c 'FI_MSG|FI_RMA|FI_READ|FI_REMOTE_READ|FI_REMOTE_COMM' -v
```

The C query uses the permissive mode discovery of `fi_info`, records the returned
requirements, and frees all query resources before returning Rust-owned metadata.
Real verbs device discovery still needs validation on an RDMA host.

Native lifetime and CQ settings follow the libfabric manuals for
[Fabric](https://ofiwg.github.io/libfabric/main/man/fi_fabric.3.html),
[Domain](https://ofiwg.github.io/libfabric/main/man/fi_domain.3.html) and
[CQ](https://ofiwg.github.io/libfabric/v1.17.0/man/fi_cq.3.html).
