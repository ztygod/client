# Dragonfly Client RDMA

Libfabric discovery, owned native resources and a reusable blocking transport for
**verbs + FI_EP_MSG**. A bounded async worker keeps native handles on their owner
thread. The public API accepts only the verbs provider. Discovery alone does not
prove hardware offload, peer reachability, or application protocol compatibility.

## Build and probe

Default builds need no libfabric installation. Linux without the `rdma` feature
reports `FeatureDisabled`; non-Linux platforms report `UnsupportedPlatform`, even
with all features enabled. Native C is compiled only for Linux with `rdma`.

```bash
cargo run -p dragonfly-client-rdma --example probe
```

On the target Linux host, install libfabric >= 1.17 and a C compiler:

```bash
sudo apt-get install build-essential pkg-config libfabric-dev libfabric-bin
cargo run -p dragonfly-client-rdma --features rdma --example probe -- --domain mlx5_0
cargo run -p dragonfly-client-rdma --features rdma --example probe -- --open-resources
```

Domain names come from libfabric and need not equal network device names. The
query requires `FI_MSG | FI_RMA | FI_READ | FI_REMOTE_READ | FI_REMOTE_COMM` and
preserves mode, MR, protocol, threading and progress requirements. A missing
match is an expected unavailable result, not proof that no RDMA hardware exists.
An explicit Linux feature build without libfabric development files fails at
build time; configure `PKG_CONFIG_PATH` for a custom installation.

## Transport API

`Listener::bind/accept` and `Session::connect` establish connections. Native
objects are neither `Send` nor `Sync`; create and use them on one thread.

- `send` and `receive` exchange bounded application control messages.
- `export` registers immutable bytes and returns `RemoteRegion` metadata.
- `read` validates the region length and address range, waits for the matching
  Read completion, then returns owned bytes. The copy keeps thread-affine native
  registrations out of async consumers.
- `transaction` applies one deadline across all nested messages and reads in a
  request. A successful transaction leaves the connection reusable.
- Any failed transaction poisons the connection. Call `try_close`; subsequent
  data operations fail with the current state and attempted operation.

Applications define the control-message encoding and perform their own length,
identity, authorization and integrity checks. The core has no generated test data,
checksum policy, task IDs or Dragonfly piece protocol. The diagnostic wire format
and deterministic byte verification live under `examples/support/`.

Each call outside a transaction receives its own timeout. Connect and accept have
separate budgets. Synchronous provider queries and registration cannot be
interrupted; deadlines are checked before posting and when returning from a
transaction. Numeric addresses avoid DNS lookup delays. Progress polling briefly
spins, then yields and backs off to at most 50 microseconds. Both CQs and CM events
are driven for providers requiring manual progress. CQs remain `FI_WAIT_NONE`;
blocking CQ reads require a different, validated wait configuration.

`accept_with_timeout` overrides one idle accept wait without changing accepted
sessions' transaction timeout. Idle accept timeouts leave the listener reusable.
Timeouts must be positive and at most 600 seconds.

The connection query uses address-specific info and accept uses `FI_CONNREQ` info.
Implemented modes are `FI_CONTEXT`, `FI_LOCAL_MR`, `FI_RX_CQ_DATA`, and MR
local/virtual-address/allocated/provider-key modes, including returned legacy basic
registration. Unsupported requirements fail explicitly. Keys come from
`fi_mr_key`; remote addresses follow the selected MR mode.

## Async ownership and admission

`SessionWorker` creates its session on a dedicated OS thread. Submit a complete
request with `execute`; closures and owned results cross the boundary, native
handles do not. Its deadline includes queue wait. The queue is bounded to 1..=1024
jobs. Queue capacity bounds job count, not arbitrary memory captured by closures.

```rust,no_run
use dragonfly_client_rdma::{SessionOptions, SessionWorker};

# async fn example() -> Result<(), dragonfly_client_rdma::SessionError> {
let options = SessionOptions::new("192.0.2.10", 7471);
let mut worker = SessionWorker::connect(options, 8).await?;
let response = worker.execute(|session| {
    // The peer must implement this application's matching control protocol.
    session.send(b"request")?;
    session.receive()
}).await?;
worker.close().await?;
# let _ = response;
# Ok(())
# }
```

Dropping an awaiting future never releases in-flight provider memory. A queued
cancelled job is skipped; a running request finishes or times out before the next
job. Dropping the worker disconnects its queue without blocking the async runtime.
Use `close().await` to observe cleanup errors and retry a failed close. Successful
close stops the worker; that worker cannot be reused.

`Limits` bounds live sessions, registered bytes, registration count, control
message size, Read size and CQ capacity. `ResourceBudget` shares admission across
listeners/workers via `bind_with_budget` and `connect_with_budget`. Its effective
limits override the per-session limits. `usage()` provides registered bytes,
registration count, live sessions and cumulative retained cleanup failures.

Defaults are 32 sessions, 128 MiB registered memory, 1024 registrations, 4 KiB
control messages, 64 MiB Reads and 64 CQ entries. A retained cleanup failure stops
new session admission by default. Counters are a best-effort snapshot; they are
not yet exported to Dragonfly Prometheus metrics.

Control send/receive buffers retain their registrations between requests. Local
Read buffers reuse the largest successful allocation. There is one in-flight
operation per connection. Exported regions remain registered until endpoint close,
including after application acknowledgements: no lease expiry can recycle memory
while a peer may still access it. Byte and registration limits bound retained
exports. Sustained uploads need an application release protocol with safe MR
revocation, or connection rotation; cross-session registered-buffer pools and
concurrent operations are not implemented.

## Cleanup and errors

A child retains every parent/bound queue it needs. Explicit close orders Endpoint
before pending operations, exports and reusable buffers, then CQ/EQ, Domain and
Fabric. Dropping parent wrappers early cannot invalidate children. Partial setup
uses the same ownership rules. Failed closes preserve handles for retry; repeated
successful direct `try_close` calls are harmless.

Timeout/native failure retains the submitted buffer and native context until
Endpoint close succeeds. `Drop` logs errors and retains unsafe-to-release bundles,
including their admission permits. This exceptional path can leak native resources
but cannot silently release registered memory. Monitor usage and recreate the
service when retained resources cannot be recovered.

`FabricError` preserves operation, fabric code, provider code and diagnostic text.
CQ/EQ provider details are copied before another queue read can invalidate them.
Transport errors additionally identify unavailable platforms, bad options,
resource exhaustion, allocation failures, protocol errors, timeout and session
state. TCP fallback belongs around the complete piece transfer/write/verify
attempt in Dragonfly, not inside the native transport.

## Two-host diagnostic

Use two Linux hosts with compatible working verbs interfaces. Configure the
RoCEv2/InfiniBand link, routing and firewall first. Probe each host's local domain;
the domain names may differ. Use RDMA addresses rather than management addresses.

Host A:

```bash
cargo run -p dragonfly-client-rdma --features rdma --example read_demo -- \
  server --address 192.0.2.10 --port 7471 --domain mlx5_0 --timeout-secs 60
```

Host B:

```bash
cargo run -p dragonfly-client-rdma --features rdma --example read_demo -- \
  client --address 192.0.2.10 --port 7471 --domain mlx5_1 \
  --size 4194304 --seed 42 --timeout-secs 60
```

Both processes must print `VERIFIED` with matching byte count and checksum, close
resources and exit zero. Every byte is compared against deterministic data; FNV-1a
is only a diagnostic checksum. Test 1, 4096 and 4194304 bytes with different seeds.
This trusted-peer diagnostic is not authenticated or an application piece server.

## Tests and integration boundary

```bash
cargo test --locked -p dragonfly-client-rdma --all-targets
cargo test --locked -p dragonfly-client-rdma --all-features --all-targets
cargo clippy --locked -p dragonfly-client-rdma --all-targets --all-features -- -D warnings
```

Linux feature tests use real libfabric with its software TCP provider. They cover
repeated reads, reusable registrations, byte correctness, request cancellation,
transaction deadlines, admission, pending-I/O cleanup and retrying close failures.
Ownership tests inject allocation/close failures. These tests need the TCP
provider and an interface advertising remote communication, but no RDMA hardware.
The software provider is private to tests and is rejected by public constructors.

Dragonfly capability announcements, peer selection, Storage adapters, piece-level
TCP fallback and Prometheus integration remain application work. The current
`dragonfly-api` usage only carries TCP/QUIC address information; a versioned RDMA
capability advertisement must be agreed before automatic selection is connected.
Native software-provider validation does not establish real verbs hardware support
or performance benefits. Target Linux/RoCEv2/InfiniBand validation and TCP/RDMA
benchmarks remain required.

## Source layout

- `ffi/{raw,info}.rs`: declarations, query ownership and query conversion.
- `resource/{handle,fabric,endpoint,memory,completion}.rs`: native ownership and
  resource wrappers, mirrored by the C shim under `c/`.
- `session/{mod,listener,connection,progress,unsupported}.rs`: options, connection
  lifecycle, polling and unavailable-platform behavior.
- `limits.rs`: shared admission permits and counters; `worker.rs`: async boundary.
- `examples/read_demo.rs` and `examples/support/`: diagnostic application protocol.

Native lifetime, completion and error contracts follow the libfabric 1.17 manuals
for [Endpoints](https://ofiwg.github.io/libfabric/v1.17.0/man/fi_endpoint.3.html),
[MR](https://ofiwg.github.io/libfabric/v1.17.0/man/fi_mr.3.html),
[CQ](https://ofiwg.github.io/libfabric/v1.17.0/man/fi_cq.3.html) and
[EQ](https://ofiwg.github.io/libfabric/v1.17.0/man/fi_eq.3.html).
