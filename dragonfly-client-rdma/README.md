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
cargo run -p dragonfly-client-rdma --example check_rdma_capabilities
```

On the target Linux host, install libfabric >= 1.17 and a C compiler:

```bash
sudo apt-get install build-essential pkg-config libfabric-dev libfabric-bin
cargo run -p dragonfly-client-rdma --features rdma --example check_rdma_capabilities -- --domain mlx5_0
cargo run -p dragonfly-client-rdma --features rdma --example check_rdma_capabilities -- --open-resources
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
- `reserve_export` preallocates both control buffers and reserves the next export
  against the shared budget. Fill its `ExportBuffer`, then call `export_buffer` to
  publish it without a copy. `export_tracked` accepts an existing `Vec` instead.
  Both publication APIs return `ExportedRegion` with an application release ID.
- `read` preserves the compatible owned-`Vec` result, including its payload copy.
- `read_with` consumes a completed registration's slice directly on the owner
  thread; `read_lease` returns an immutable, native-free `CompletedRead` without a
  payload allocation/copy after pool warmup.
- `read_many_with` submits a bounded window of already negotiated regions and
  delivers `(input_index, CompletedRead)` in CQ completion order. All regions are
  validated before posting. These APIs check remote length/address bounds and
  require matching Read completions before exposing bytes.
- `transaction` applies one deadline across all nested messages and reads in a
  request. A successful transaction leaves the connection reusable.
- Failed transactions poison the connection. Call `try_close`; subsequent data
  operations fail with the current state and attempted operation. A typed Read
  cancellation that successfully drains submitted operations returns `Cancelled`
  outside its native transaction and leaves the connection reusable. Propagating
  it through an enclosing application transaction still fails that transaction.

Applications define the control-message encoding and perform their own length,
identity, authorization and integrity checks. The core has no generated test data,
checksum policy, task IDs or Dragonfly piece protocol. The diagnostic wire format
and deterministic byte verification live under `examples/read_export_protocol/`.

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

Control send/receive buffers retain their registrations between requests. Read
registrations are reused through a connection-local pool. Control transactions
remain serial; a Read batch may contain multiple in-flight operations. Export
lifetime management is described below.

## Read pipelines and completed-data ownership

`ReadOptions` defaults to 4 in-flight Reads and 64 MiB of submitted payloads. The
count cannot exceed 64, the connection's CQ capacity, or its Read pool capacity.
Each batch accepts at most 65,536 region descriptors; no new control protocol or
request multiplexing is implied. The application must negotiate metadata first
and keep each remote export valid until all of its Reads complete.

`Limits.max_read_buffers` / `max_read_buffer_bytes` bound each connection's full
registered Read working set, including in-flight and consumer-held allocations.
Defaults are 8 buffers and 64 MiB, additionally constrained by the shared
`ResourceBudget`. Window byte limits count posted/requested bytes; pool limits
count whole allocation sizes. Submission retries on `EAGAIN` keep driving both
CQs and CM events. The shim retrieves up to 16 CQ entries per driver poll and
routes completions by stable context, not submission order. Unknown contexts,
wrong queue/flags and provider errors fail the connection and retain every
unconfirmed operation until successful endpoint close. One transaction deadline
covers submission, buffer backpressure, polling and callbacks.

A `CompletedRead` is an immutable `Arc`-backed lease, automatically `Send + Sync`.
It contains bytes/admission ownership only, with no MR, Domain or operation
context. The owner retains native resources and cannot obtain mutable access or
repost the allocation while any lease clone exists. Short reads expose only the
completed length. Successfully closing the Session may deregister its MRs while
leases remain: their bytes stay readable, and their byte/registration permits
remain conservatively reserved until the last lease drops. Usage counters thus
include storage retained by consumers, not just currently open native MRs.

`read_with` runs the consumer on the owner thread and prevents slice references
from escaping. Use it for quick checks; a slow callback blocks progress. For
asynchronous storage/checksum consumers, use `SessionWorker::read_stream`. This
typed command delivers leases as soon as individual Reads finish while the owner
continues posting/polling others. Its completion queue's live payload count is
bounded by Read pool capacity: queued/held leases prevent reuse of their slots.
Drop each lease after consuming it. Holding all slots while waiting for additional
results applies backpressure and eventually exhausts the transaction deadline.
The stream's input index supports callers that need ordered writes.

```rust,no_run
use dragonfly_client_rdma::{ReadOptions, RemoteRegion, SessionError, SessionWorker};

async fn consume(worker: &mut SessionWorker, regions: Vec<RemoteRegion>) -> Result<(), SessionError> {
    let mut stream = worker.read_stream(regions, ReadOptions::default()).await?;
    while let Some(completed) = stream.next().await? {
        // Pass completed.data to a storage/checksum consumer. Holding it across
        // an await is safe; drop it after consumption to replenish the Read pool.
        std::hint::black_box((completed.index, completed.data.as_slice()));
    }
    Ok(())
}
```

`next().await` drains successful completions before reporting the final error once;
partial results do not imply a successful piece transfer. `finish().await` drains
remaining results and observes final errors. Dropping/cancelling a stream stops
new submissions; the owner drains accepted Reads before the next command. A
cancelled queued command skips native work. If drain times out or fails, all
unconfirmed buffers/contexts remain retained for connection-close cleanup. This
pipeline overlaps local consumption with Read I/O; arbitrary `execute` closures
and control messages remain serialized.


## Export admission, rotation and early release

The default safe behavior still retains exported regions until successful endpoint
close. A local Send completion or a timeout does not prove that a peer has stopped
reading a region. Existing `export` callers retain this behavior.

Use `reserve_export(length)` before generating/publishing the next payload. It
initializes both control buffers, atomically reserves byte/registration capacity
across the shared budget, and checks out an unpublished zeroed allocation. Fill
`as_mut_slice()`, then publish with `export_buffer(buffer)`. Dropping an unused
reservation returns its byte and registration permits. Reservations contain no
native handles, may cross threads, and are valid only for their originating
session. Consume or drop them before closing/rotating.

Once control buffers are initialized, an admission `ResourceLimit` does not poison
the session: the application can finish existing work, exchange a drain/rotate
message, successfully close the old connection, and reconnect before retrying the
unstarted request. Initial control-buffer setup failure requires closing/retrying;
no transfer has been published by that reservation. Do not propagate an expected
admission error out of an enclosing `transaction` if you intend to send a rotation
message: failed transactions still poison the session. A usage snapshot is not a
reservation, and raising limits only delays exhaustion. Refuse a request that
cannot fit even an empty connection instead of rotating indefinitely.

For trusted peers with a proven quiescence protocol, `unsafe release_export(id)`
allows early deregistration. `ExportId` contains a process-local session identity
and a monotonically increasing generation; IDs do not repeat within a process.
Only successfully published exports issue IDs. Duplicate releases return `false`
without storing unbounded tombstones; unknown/future/cross-session IDs are errors.
These IDs validate control messages, not native RDMA access. They are not a
hardware revocation mechanism or persistent identities across process restarts.

The caller's safety obligation is that all peers have completed every operation
against the region and will never use its old address/key again. A RELEASE sent
only after all successful Read completions and after prohibiting further Reads can
provide that promise in a trusted protocol. It cannot establish quiescence for an
arbitrary or buggy peer. Validate provider revocation and key-reuse behavior before
opting in; otherwise use connection rotation. Never release merely on TTL expiry.
A failed MR close retains the MR, data and permits, poisons data operations, and
allows release/connection-close retries. Only a successful deregistration returns
storage to the cache and returns registration capacity.

`ResourceBudget` shares a bounded cache of **unregistered allocations** across its
sessions/workers: defaults are 8 MiB and 8 buffers. Configure
`max_cached_export_bytes` / `max_cached_export_buffers`; zero disables retention.
Checkouts match exact lengths and clear previous contents before returning bytes
to the application. Each publication still performs a new MR registration and
uses the provider's returned key. Live registrations are never pooled. This saves
allocation/copying through `ExportBuffer`, not MR registration cost.
`clear_export_cache()` trims only unused allocations. `ResourceUsage` separately
reports cached bytes/buffers; registered bytes/count also include unpublished
reservations. Total retained cache capacity is shared, rather than multiplied by
the number of sessions. A persistent registered pool or hardware memory-window
revocation remains future work requiring provider-specific validation.

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
cargo run -p dragonfly-client-rdma --features rdma --example verify_read_export_lifecycle -- \
  server --address 192.0.2.10 --port 7471 --domain mlx5_0 --timeout-secs 60
```

Host B:

```bash
cargo run -p dragonfly-client-rdma --features rdma --example verify_read_export_lifecycle -- \
  client --address 192.0.2.10 --port 7471 --domain mlx5_1 \
  --size 4194304 --seed 42 --timeout-secs 60
```

Both processes must print `VERIFIED` with matching byte count and checksum, close
resources and exit zero. Every byte is compared against deterministic data; FNV-1a
is only a diagnostic checksum. Test 1, 4096 and 4194304 bytes with different seeds.
This diagnostic is not authenticated or an application piece server. Its version 2
wire format is incompatible with the original one-shot demo.

The server accepts repeated requests and rotates before exhausting either export
budget. To exercise rotation, set a small server budget and request many pieces:

```bash
# Host A: keeps the safe retain-until-close behavior.
cargo run -p dragonfly-client-rdma --features rdma --example verify_read_export_lifecycle -- \
  server --address 192.0.2.10 --registered-bytes 10485760
# Host B: 128 verified pieces, reconnecting when the server requests rotation.
cargo run -p dragonfly-client-rdma --features rdma --example verify_read_export_lifecycle -- \
  client --address 192.0.2.10 --size 4194304 --requests 128
```

For controlled trusted-peer testing, add `--release-exports` to Host A. Every
request then exchanges `RELEASE/RELEASED` containing the exact export identity,
length and diagnostic checksum. The client sends RELEASE only after the matching
Read completes and bytes verify, and never reuses its metadata. The server calls
the unsafe release interface only in this explicit mode. Logs show registered
bytes/regions and cached bytes after each request; the retained registered working
set should stay stable. Missing or mismatched releases fail the transaction and
retain exports for endpoint-close cleanup. Both modes refuse a request that cannot
fit an empty connection. The demo ends with a final Done acknowledgement and
closes all native resources.

## Throughput comparison

`benchmark_read_throughput` is a separate, trusted-peer two-host diagnostic. Restart its server
for every run, using release builds on both hosts:

```bash
# Host A
cargo run --release -p dragonfly-client-rdma --features rdma --example benchmark_read_throughput -- \
  server --address 192.0.2.10 --domain mlx5_0
# Host B; repeat with --mode copy, borrow, window and stream
cargo run --release -p dragonfly-client-rdma --features rdma --example benchmark_read_throughput -- \
  client --address 192.0.2.10 --domain mlx5_1 --mode window \
  --size 262144 --iterations 1024 --window 4
```

The modes compare owned-Vec copies, synchronous borrowed consumption, bounded
window callbacks, and asynchronous completion leases. All modes warm the active
pool slots and fully verify immutable server data before timing; timed iterations
sample bytes and use `black_box`, rather than running a full checksum. Results
report warm end-to-end batch throughput and registered/reserved working set.
This is not a storage benchmark, P99-latency measurement, or evidence of hardware
offload. Measure CPU and tail latency separately with the intended storage and
integrity pipeline; do not infer verbs gains from software TCP tests.

Compare windows 1/2/4/8 and payloads 64 KiB/256 KiB/1 MiB/4 MiB. Repeat runs and
keep host/domain settings consistent. `size * active_window` must fit the Read
pool byte limit; iterations are bounded to 1..=65,536. Copy/borrow use one active
Read. The default 120-second timeout covers the full benchmark request; increase
`--timeout-secs` on both hosts for longer runs (up to 600 seconds).

## Tests and integration boundary

```bash
cargo test --locked -p dragonfly-client-rdma --all-targets
cargo test --locked -p dragonfly-client-rdma --all-features --all-targets
cargo clippy --locked -p dragonfly-client-rdma --all-targets --all-features -- -D warnings
```

Linux feature tests use real libfabric with its software TCP provider. They cover
repeated reads, reusable registrations, byte correctness, request cancellation,
transaction deadlines, admission, pending-I/O cleanup and retrying close failures.
Export lifecycle tests additionally cover 128 release/reuse cycles under a small
budget, byte/registration-triggered rotation, stale and duplicate releases, failed
deregistration retries, and missing-release cleanup. Platform-independent tests
cover bounded/zeroed caches, cancelled reservations and versioned control frames.
Read pipeline tests cover byte-correct variable-size batches, input identity,
bounded registration reuse, cross-thread leases after native close, provider
failures with multiple operations retained, and stream cancellation followed by
connection reuse. Portable tests exercise out-of-order completion accounting,
window limits, lease admission lifetime and stream terminal errors.
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
- `session/exports.rs`: connection/generation identities and unpublished buffers.
- `session/read.rs`: native-free leases, options and bounded completion tracking.
- `session/reads.rs`: owner-thread Read pipeline and registered receive pool.
- `limits.rs`: shared admission, allocation cache and counters; `worker.rs`: async boundary.
- `examples/check_rdma_capabilities.rs`: local capability discovery and optional resource creation/close checks.
- `examples/verify_read_export_lifecycle.rs`: two-host byte verification, export release and connection rotation.
- `examples/read_export_protocol/`: lifecycle diagnostic wire format, data checks and protocol unit tests.
- `examples/benchmark_read_throughput.rs`: two-host throughput comparison across Read APIs.

Native lifetime, completion and error contracts follow the libfabric 1.17 manuals
for [Endpoints](https://ofiwg.github.io/libfabric/v1.17.0/man/fi_endpoint.3.html),
[MR](https://ofiwg.github.io/libfabric/v1.17.0/man/fi_mr.3.html),
[CQ](https://ofiwg.github.io/libfabric/v1.17.0/man/fi_cq.3.html) and
[EQ](https://ofiwg.github.io/libfabric/v1.17.0/man/fi_eq.3.html).
