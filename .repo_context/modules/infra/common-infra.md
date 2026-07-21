# Common Infrastructure

## Scope

- Paths:
  - `src/datasystem/common`
  - especially:
    - `rpc`
    - `shared_memory`
    - `kvstore`
    - `log`
    - `metrics`
    - `rdma`
    - `l2cache`
    - `device`
    - `os_transport_pipeline`
- Why this module exists:
  - provide the shared infrastructure layer that client, worker, and master all depend on;
  - centralize transport, storage backends, memory primitives, observability, and device-related helpers.
- Primary source files to verify against:
  - `src/datasystem/common/CMakeLists.txt`
  - `src/datasystem/common/rpc/CMakeLists.txt`
  - `src/datasystem/common/shared_memory/CMakeLists.txt`
  - `src/datasystem/common/kvstore/CMakeLists.txt`
  - `src/datasystem/common/kvstore/etcd/CMakeLists.txt`
  - `src/datasystem/common/kvstore/metastore/CMakeLists.txt`
  - `src/datasystem/common/kvstore/rocksdb/CMakeLists.txt`
  - `src/datasystem/common/log/CMakeLists.txt`
  - `src/datasystem/common/metrics/CMakeLists.txt`
  - `src/datasystem/common/rdma/CMakeLists.txt`
  - `src/datasystem/common/l2cache/CMakeLists.txt`
  - `src/datasystem/common/device/CMakeLists.txt`
  - `src/datasystem/common/os_transport_pipeline/CMakeLists.txt`

## Responsibilities

- Verified:
  - `src/datasystem/common` is the shared dependency root for both runtime-side and client-side code.
  - top-level common subdomains currently include:
    - auth and identity helpers: `ak_sk`, `iam`, `token`, `encrypt`
    - runtime plumbing: `rpc`, `eventloop`, `parallel`, `signal`, `flags`, `inject`, `util`
    - storage and memory: `shared_memory`, `kvstore`, `l2cache`, `object_cache`, `stream_cache`
    - observability: `log`, `metrics`, `perf`
    - transport and device paths: `rdma`, `device`, `os_transport_pipeline`
    - parallel task dispatch contract: `task_action` (subscribers wired today, not yet dispatched from production paths)
  - this layer is wide by design and should be split further when one subdomain becomes a frequent context target.
- Pending verification:
  - exact runtime ownership for each `common/*` component between client-only, worker-only, and truly shared callers;
  - which utility subdomains are stable enough to deserve their own dedicated module docs next.

## Stable Subdomains Worth Knowing

| Subdomain               | Verified role                                                                    | Build facts                                                                          |
| ----------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `rpc`                   | RPC plumbing plus code generation plugins and ZMQ-based transport implementation | builds rpc plugins, `common_rpc_zmq`, `common_rpc_zmq_client`, `rpc_stub_cache_mgr`  |
| `shared_memory`         | shared-memory allocator, arena, mmap abstractions, shared-disk detection         | builds `common_shared_memory` and `common_shm_unit_info`                             |
| `kvstore`               | metadata/backend storage families                                                | split into `etcd`, `metastore`, `rocksdb`                                            |
| `log`                   | logging, access recording, tracing, failure handling                             | builds `common_log`                                                                  |
| `metrics`               | resource metrics and exporters                                                   | builds `common_metrics` and exporter base                                            |
| `rdma`                  | fast transport wrappers and optional URMA/RDMA support                           | conditional build based on feature flags                                             |
| `l2cache`               | persistence and secondary-storage support                                        | includes OBS/SFS clients, distributed-disk slot client, and persistence API dispatch |
| `device`                | Ascend and optional Nvidia device support wrappers                               | builds `common_device` over backend-specific device libs                             |
| `os_transport_pipeline` | optional pipeline H2D transport path                                             | only built when `BUILD_PIPLN_H2D` is enabled                                         |
| `task_action`           | subscriber registry mirroring `HashRingEvent` migration/recovery/cleanup actions | builds `common_task_action`; callers use `datasystem::TaskActionRegistry`; subscribers are registered today by OC/SC metadata managers, `WorkerOcServiceClearDataFlow`, and `WorkerOCServer`, but `Dispatch` is not yet called by any production path (hash ring still drives execution via `HashRingEvent::NotifyAll`) |

Detailed follow-up docs now exist for:

- `modules/infra/l2cache/README.md`
- `modules/infra/l2cache/design.md`
- `modules/infra/l2cache/l2-cache-type.md`
- `modules/infra/slot/README.md`
- `modules/infra/slot/design.md`
- `modules/infra/logging/README.md`
- `modules/infra/logging/design.md`
- `modules/infra/logging/trace-and-context.md`
- `modules/infra/logging/access-recorder.md`
- `modules/infra/logging/log-lifecycle-and-rotation.md`
- `modules/infra/metrics/README.md`
- `modules/infra/metrics/design.md`
- `modules/infra/metrics/resource-collector.md`
- `modules/infra/metrics/exporters-and-buffering.md`
- `modules/infra/metrics/metric-families-and-registration.md`

## Storage And Metadata Backends

- Verified:
  - `kvstore` has three current backend families:
    - `etcd`
    - `metastore`
    - `rocksdb`
  - ETCD layer provides store, keep-alive, watch, health, and gRPC session pieces.
  - Metastore builds a lightweight ETCD-compatible shared service with KV, lease, watch, and maintenance service implementations.
  - RocksDB layer provides local persistent store and replica-related components.

## Transport And Memory Notes

- Verified:
  - `rpc` contains both generation tooling and the main ZMQ transport implementation.
  - selected ZMQ, message-queue, and URMA IO threads can be deprioritized together via `io_thread_nice`, which
    defaults to `0`; `0` skips `setpriority(2)`, while non-zero explicit values are applied in `ZmqServerImpl` proxy
    execution, `ZmqEpoll`, `MsgQueMgr`, and `UrmaManager` server event handling.
  - `shared_memory` contains allocator, jemalloc integration, arenas, shm units, and several mmap backends; when UB
    numa affinity is enabled it also records per-allocation NUMA ownership. Shared-memory pre-touch distribution is
    controlled by `shared_memory_distribution_policy` with values `none`, `interleave_all_numa`,
    `interleave_affinity_numa`; for interleave policies the implementation performs 1GB chunk round-robin across
    selected NUMA nodes, applies `SYS_mbind` with `MPOL_BIND` per chunk, and then page-touches each chunk. The
    distribution policy takes effect when `enable_urma=true` and
    `urma_register_whole_arena=true`. For `MemMmap`, `enable_huge_tlb=true` still selects explicit hugetlb mappings,
    while `enable_thp=true` leaves the process THP setting enabled and additionally applies `madvise(...,
MADV_HUGEPAGE)` to the shared-memory memfd mapping after `mmap` succeeds when the mapping is not using
    `MAP_HUGETLB`.
  - `rdma` always builds fast-transport wrapper pieces and conditionally adds URMA and RDMA implementations.
  - URMA write chunking is capped by the smaller of device capability and `urma_max_write_size_mb`; the flag defaults
    to `4` MB and is validated in the range `[1, 2048]` MB.
  - URMA send-side Jetty reuse is managed by a process-level send Jetty pool under `src/datasystem/common/rdma`.
    `urma_send_jetty_lane_pool_size` is the target active pool size and must be positive; timeout/error retirement is
    bounded by `urma_send_jetty_lane_refill_extra_size`, so the intended live-plus-retiring default cap is `200 + 200`.
    Every provider post first acquires a shared `UrmaJetty::PostPermit`. The Jetty uses one atomic gate word for
    `closing`, retire-finalizer arming/scheduling, and the active provider-call count: concurrent posts remain allowed,
    while retire closes admission and waits for already-admitted provider calls before `modify(ERROR)`.
    Pool detach and the pending-retire record are established before the finalizer is armed. The record preserves an
    early `FLUSH_ERR_DONE`; provider delete is admitted only after the per-Jetty flush notification moves the lifecycle
    to delete-ready. Jetty-level flush notifications are dispatched before request-level pipeline hooks so the sole
    notification cannot be consumed as an ordinary request completion. Modify/delete failure is quarantined and
    remains inside the configured live-resource bound. A quarantined Jetty keeps its registry identity reserved:
    flush carries only `local_id`, so registration rejects a different live wrapper with the same ID to prevent ABA.
    A send lane is leased once per logical transfer and shared by that transfer's chunk WR events; release or retirement
    happens only after the shared request lease is sealed and all associated events have completed, failed, or timed out.
    Worker-to-worker Batch Get is a narrower RPC-scoped exception: `BatchGetObjectRemoteImpl` attempts one shared-lane
    acquire before object processing. On success it passes the lane to ordinary and gather writes and seals it once
    after all sub-request WRs are created. When the acquire fails and transport fallback is enabled, the whole RPC is
    pinned to TCP before object processing; aggregate/GatherWrite and per-object URMA acquire/post are disabled, while
    the existing `TrackUrmaFallbackTcp` admission/accounting path remains in force. With fallback disabled, the original
    acquire error is returned (`K_TRY_AGAIN` for pool exhaustion). Object WR creation/provider-post failures use release
    cleanup in the shared-lease path. Same-Jetty concurrent post safety remains a provider contract; datasystem's gate
    specifically excludes post/modify and post/delete overlap.
  - URMA receive-side Jetty reuse is process-level: `UrmaResource::GetOrCreateSharedRecvJetty()` lazily creates the
    single RECV Jetty/JFR published by TCP handshake responses. Jetty role is immutable, so RECV async-event retirement
    does not enter the send pool or trigger send-pool refill. Shutdown closes Jetty admission before stopping the poll
    thread; non-converged provider resources are retained fail-closed rather than implicitly deleted.
  - when hetero is enabled, RDMA dependencies also pull in device and shared-memory related components.
  - HCCS RH2D is compiled only when `cann_hixl` is found and its detected HIXL version is `8.5.2` or newer. Older
    CANN/HIXL environments still build hetero and default ROCE paths, but `remote_h2d_link_type=HCCS` is not available
    because `hccs_transport.cpp` is not compiled and `ASCEND_HIXL_AVAILABLE` is not defined.
  - Ascend local Direct H2D supports opt-in descriptor parallelism through `DS_H2D_PARALLEL_WORKER_NUM`; the default
    value `1` keeps Direct serial. It preserves the synchronous `MGetH2D` contract and uses byte-balanced bounded ACL
    batches only when the configured worker count is greater than one and the estimated task count can cover all
    workers. The default aggregate size is 512 descriptors and requests below 24 MiB use serial Direct. A per-device
    bounded worker pool binds the device once per worker; caller-runs binds before its inline task. Both paths share one
    inflight limit, and a request drains all submitted tasks before returning the lowest-index failure. The default
    policy remains unchanged; focused mock-backed coverage lives in
    `tests/st/device/acl_resource_manager_fallback_test.cpp`.
  - FFTS and Huge FFTS H2D additionally support default-off object-level parallelism through
    `DS_H2D_FFTS_PARALLEL_WORKER_NUM` (default `1`) and `DS_H2D_FFTS_PARALLEL_MIN_BYTES` (default 24 MiB). Complete
    objects are byte-balanced by `AclParallelFftsExecutor` across independent copier instances; every instance owns its
    dispatcher, streams, notifies, and two device staging buffers. The executor validates the aggregate per-shard
    staging requirement against `DS_DEVICE_ACL_SIZE` before submitting work, drains all submitted shards before
    returning the first shard failure, and serializes calls so only one call can hold staging resources; insufficient
    work/resources uses serial FFTS. For object sizes `S[j]` and shard maxima `M[i]`, serial FFTS needs
    `2 * max(S[j])` device staging bytes while parallel FFTS needs `2 * sum(M[i])`; ordinary FFTS host staging remains
    `sum(S[j])` and is not multiplied by the worker count. Only when both directions use Huge FFTS is the caller's
    HugeTLB host buffer reused and the internal host pool skipped.
    Execution is observable through
    `TOTAL_H2D_PARALLEL_FFTS_MEMCPY`; configuration logging alone is not execution proof.
  - `os_transport_pipeline` is optional and only exists when pipeline H2D support is enabled.

## Observability Notes

- Verified:
  - `log` includes logging, access recording, tracing, and failure handling.
  - `metrics` includes exporter base, resource metric collection, and hard-disk exporter support.
  - `perf` is a separate common subdomain listed at top level and frequently appears in runtime loops and request paths.

## Review And Bugfix Notes

- Common change risks:
  - infra changes often have broad blast radius because both client and worker layers link here;
  - transport and shared-memory edits can affect correctness, performance, and deployment assumptions at the same time;
  - backend changes in `kvstore` can influence cluster coordination, metadata, and recovery behavior.
- Useful debugging orientation:
  - if a problem spans modules, check whether the common layer is the real coupling point before blaming business logic;
  - if a bug touches ETCD, Metastore, or RocksDB persistence, keep `common-infra` and `cluster-management` docs open together.

## Recommended Next Split

When this document gets too large, split it in this order:

1. `rpc-and-transport.md`
2. `shared-memory.md`
3. `kvstore-backends.md`
4. `device-and-rdma.md`
