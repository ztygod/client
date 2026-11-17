# RDMA validation record

Recorded on **2026-11-17**. This date records the developer's progress report;
individual test execution dates were not supplied. The results below are
developer-reported, not a newly executed test run or a complete raw-log archive.

## Implementation under review

The source reviewed for this record is `5cbdaf26c4cbbf138fbe0c404114e3b32429ce78`
(`feat: finish rdma`). This is a source reference, not confirmation that every
reported test ran against that exact revision.

- Rust transport crate with a C shim calling libfabric.
- Public transport: `verbs` provider, connection-oriented `FI_EP_MSG` endpoints.
- Application control messages: `fi_send` / `fi_recv`.
- Payload transfer: one-sided RDMA Read using `fi_read` and exported memory-region
  metadata. Read completion is checked before exposing completed data.
- Native session handles stay on their owner thread; an async worker provides a
  bounded submission boundary. Resource budgets limit sessions and registered/
  reserved allocations.
- Default exports remain retained until successful endpoint close. Early release
  is an explicit unsafe API requiring a valid trusted-peer quiescence protocol;
  its hardware/provider safety is not established by these reported tests.

See [README](README.md) for API, ownership, build and diagnostic instructions.
The crate is not yet integrated into Dragonfly's application piece-transfer path.
The diagnostic wire format does not establish Vortex compatibility.

## Reported environment and completed functional tests

| Layer | Reported configuration |
| --- | --- |
| Host | Mac, ARM64 |
| Virtualization | UTM |
| Guest | Ubuntu 24.04, ARM64 |
| RDMA device | Soft-RoCE (RXE), used through libfabric verbs |
| Physical RoCE / InfiniBand NICs | Not used |

| Scenario | Reported result | Scope |
| --- | --- | --- |
| Two processes inside one Ubuntu VM | RDMA Read passed: 16 consecutive 4 MiB reads, 64 MiB total, with data verification | Single-VM functional validation; 64 MiB is cumulative traffic, not one 64 MiB Read |
| Single-VM lifecycle testing | Connection rotation and resource cleanup completed | No raw resource-counter series or exact release-mode flags supplied |
| Two Ubuntu VMs | Bidirectional IP connectivity verified; cross-VM RDMA data transfer completed | Separate from the single-VM 4 MiB x 16 result; cross-VM sizes/counts were not supplied |

These results establish reported functional progress on virtualized RXE. They do
not establish physical NIC offload, real-fabric throughput/CPU benefits, complete
fault coverage, RISC-V64 execution, or Dragonfly end-to-end piece correctness.
No throughput, latency, CPU, speedup or complete-test-suite pass count is claimed
by this record.

The same progress and open design questions were posted in
[client#1926](https://github.com/dragonflyoss/client/issues/1926#issuecomment-6948343423).
That comment is a progress/discussion record, not raw test evidence or maintainer
approval of the protocol.

## Evidence to archive

For each completed or future run, retain:

- Exact tested Git commit and local changes, build profile/features, commands on
  both endpoints, execution date, and process exit codes.
- Guest kernel, Rust and libfabric versions, selected provider/domain, and RXE
  configuration. For hardware runs, also record NIC, driver, firmware and link.
- Whether endpoints are processes in one VM, separate VMs on one Mac, or separate
  physical hosts; record the applicable VM networking topology.
- Payload size, request count, seed/data-verification method, timeout, resource
  limits, and whether the diagnostic's `--release-exports` flag was enabled.
- Both endpoints' output, including verification, connection rotation, cleanup
  and resource counters where available. Record failures as well as successes.

Exact tested revisions, command lines, libfabric/kernel versions and raw logs
have not yet been attached to this document. Populate them from saved outputs or
an explicitly recorded rerun; example commands in README are not historical logs.

## Remaining validation and acceptance

1. **Application integration and protocol agreement.** Confirm Vortex adaptation
   or replacement, message semantics, version/old-peer compatibility and capability
   discovery with maintainers. Then validate ordinary, persistent and
   persistent-cache pieces through real Storage and the standard download path,
   including length/bounds/digest checks and complete-attempt TCP fallback.
2. **Business fault and compatibility tests.** Cover feature/device absence,
   incompatible capabilities, connection failure, transfer interruption, peer exit,
   cancellation, storage errors, late writes, terminal errors after the final data
   block, and bounded retries. Assert final bytes, metadata and notifications;
   preserve TCP/QUIC behavior.
3. **Physical hardware.** Validate at least one supported verbs configuration on
   real RoCEv2 or InfiniBand NICs, including concurrent transfer, timeout, teardown
   and cleanup. Keep those results separate from RXE results.
4. **Target architecture.** Confirm the project's RISC-V64 build/link/runtime
   acceptance scope and test it on the required environment. ARM64 VM results do
   not establish RISC-V64 support.
5. **TCP/RDMA comparison.** Use the same servers, data source, network conditions
   and integrity policy. Cover 64 KiB, 1 MiB, 4 MiB and 16 MiB pieces at 1, 8 and
   32 concurrent business requests, with at least three independent runs per
   combination (72 runs for both transports). Agree on a reference workload,
   such as 4 MiB at concurrency 32, before acceptance measurements. Record useful
   throughput, latency, both endpoints' CPU/resource use and run-to-run variation.
   Distinguish transport microbenchmarks from full verified Dragonfly downloads.
6. **Resource stability.** Exercise repeated downloads, connection reuse/rotation,
   slow consumers, resource pressure and shutdown. Check bounded resource usage
   and distinguish expected caches/held leases from cleanup failures.

Before concurrent benchmarks, account for memory admission: the default shared
registered/reserved budget is 128 MiB. If each of 32 active requests holds a whole
piece, 4 MiB pieces already require 128 MiB of payload capacity and 16 MiB pieces
require 512 MiB, before control buffers and other retained allocations. Record
budget settings, actual in-flight work, queuing and fallback counts; do not label
TCP fallback traffic as an RDMA performance result.
