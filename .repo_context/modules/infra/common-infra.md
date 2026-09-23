# Common Infrastructure

## Scope

- Paths:
  - `src/datasystem/common`
  - especially:
    - `rpc`
    - `shared_memory`
    - `memory`
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
  - `src/datasystem/common/memory/CMakeLists.txt`
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
    - storage and memory: `memory`, `shared_memory`, `kvstore`, `l2cache`, `object_cache`, `stream_cache`
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
| `memory`                | reusable process-memory diagnostics                                               | builds `jemalloc_stats_bvar`; roles link and own it explicitly                       |
| `kvstore`               | metadata/backend storage families                                                | split into `etcd`, `metastore`, `rocksdb`                                            |
| `log`                   | logging, access recording, tracing, failure handling                             | builds `common_log`                                                                  |
| `metrics`               | resource metrics and exporters                                                   | builds `common_metrics` and exporter base                                            |
| `rdma`                  | fast transport wrappers and optional URMA/RDMA support                           | conditional build based on feature flags                                             |
| `l2cache`               | persistence and secondary-storage support                                        | includes OBS/SFS clients, distributed-disk slot client, and persistence API dispatch |
| `device`                | Ascend and optional Nvidia device support wrappers                               | builds `common_device`; CUDA host registration is an always-buildable Nvidia helper  |
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
  - `memory/jemalloc_stats_bvar` reads process-level jemalloc stats through `mallctl` and exposes cached bvars. It does
    not use the prefixed allocator owned by `shared_memory`, does not create a collection thread or write files, and
    remains inactive unless an owning process explicitly constructs it.
  - `rpc` contains both generation tooling and the main ZMQ transport implementation.
  - `ScopedBthreadLocal<T>` exposes materializing access only through `Get()`, `operator->`, `operator*`, assignment, and conversion. Its non-materializing existing-value lookup is a private implementation helper used only by `Get()`; no production or test caller consumes a public `Peek()` API. This API contraction does not change key creation, per-execution-context allocation, locking, or value lifetime behavior.
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
    Client UB transport pools can set `ub_transport_arena_num` above `1` to split the single registered mapping into
    equal arenas. The requested pool size is rounded up so that each arena covers whole system pages, and the aligned
    size must remain within the 2GB client limit. When `enable_ub_numa_affinity=true` and whole-arena registration is
    enabled, the binding plan interleaves NUMA nodes from different chips before reusing another node on the same chip,
    while `ArenaGroup` round-robin allocation spreads buffers across those ranges; topology discovery and every NUMA
    bind are then required initialization steps. With affinity disabled, multi-arena allocation remains enabled but
    NUMA binding is skipped, so the default four-arena setting does not add a NUMA topology or `mbind` requirement.
  - `rdma` always builds fast-transport wrapper pieces and conditionally adds URMA and RDMA implementations.
  - Fast-transport `Event` completion waits use `bthread::Mutex` and `bthread::ConditionVariable`. A BRPC handler waiting for
    URMA or UCP completion therefore suspends its bthread instead of blocking the scheduler's worker pthread; completion
    notifications from ordinary polling pthreads remain supported.
  - URMA device selection (`UrmaManager::Init` in `src/datasystem/common/rdma/urma_manager.cpp`) collects an ordered
    candidate list via `UrmaGetEffectiveDevices`: the configured name (`DS_UB_DEV_NAME`, default `bonding_dev_0`) is
    tried first, then every remaining device whose name starts with `bonding`. `Init` tries each candidate through
    `UrmaGetDeviceByName` + `UrmaResource::Init`; because `UrmaResource::Init` starts with `Clear()`, a failed attempt
    is torn down before the next candidate. This lets a bare-metal worker start when its default bonding device
    (EID 0, from `DS_UB_DEV_EID` default `0`) is occupied by a container; previous behavior selected only the first
    `bonding*` device and failed startup when it was unavailable.
  - URMA write chunking is capped by the smaller of device capability and `urma_max_write_size_mb`; the flag defaults
    to `4` MB and is validated in the range `[1, 2048]` MB.
  - With UB NUMA affinity enabled, `ub_numa_src_chip_policy` selects the source-chip policy: `0` keeps round-robin
    behavior for ablation/rollback, while the default `1` keeps round-robin as the baseline and overrides a remote
    candidate only when the memory-affinity chip can absorb the estimated WR count for the logical write without
    becoming busier than that candidate. `ub_numa_rr_type` controls whether the decision is made per logical write or
    per post. A per-logical-write Gather advances the round-robin candidate once, then re-evaluates each WR's affinity
    from the byte-dominant source Chip across that WR's SGEs. Both policies use the existing per-chip `UrmaEvent`
    inflight-WR counters as relaxed feedback. If the absolute chip-1/chip-2 difference is strictly greater than
    `ub_numa_inflight_wr_diff_threshold` (default `15`), the
    lower-depth chip overrides all other decisions; `0` disables both depth correction and opportunistic affinity,
    preserving pure round-robin. No lock or reservation is added, so short concurrent overshoot is accepted.
    Workers publish the policy and threshold in `RegisterClientRspPb`; a Client freezes affinity, selection granularity,
    policy, and threshold from the first successful Worker registration and warns on later mismatches. A missing policy
    field from an old Worker has proto value `0`, preserving its round-robin behavior.
  - URMA Jetty modify/flush/delete work runs on the lazy `RetireJfs` thread pool with an internal concurrency of `4`.
    Resource shutdown drains and joins this non-droppable pool before releasing the Jetty registry and provider
    dependencies.
  - URMA send-side Jetty reuse is managed by a process-level send Jetty pool under `src/datasystem/common/rdma`.
    `urma_send_jetty_lane_pool_size` is the target active pool size and must be positive; explicit provider/error
    retirement is bounded by `urma_send_jetty_lane_refill_extra_size`, so the intended live-plus-retiring default cap
    is `200 + 200`. Each `UrmaConnection` snapshots `urma_send_lane_count_per_peer` at construction; its effective
    normal-phase cap is the smaller of that positive value and the process pool size (default `min(8, 200)`). This
    bounds one peer without adding a flag read to lane acquisition; HALF_OPEN probes remain limited to one lane.
    An upper-layer timeout deletes its business Event immediately and records the first timeout context
    on the RPC lane. Timeout and producer `Seal` form a two-sided handshake: whichever path observes both states calls
    force release synchronously, so an already-sealed lane does not wait for a timer and a still-producing lane cannot
    be reused early. Normal completion still releases the lane immediately. Force release is rejected for an unsealed
    lane or a lane that already requested retirement.
    Every provider post first acquires a shared `UrmaJetty::PostPermit`. The Jetty uses one atomic gate word for
    `closing`, retire-finalizer arming/scheduling, and the active provider-call count: concurrent posts remain allowed,
    while retire closes admission and waits for already-admitted provider calls before `modify(ERROR)`.
    Pool detach and the pending-retire record are established before the finalizer is armed. The record preserves an
    early `FLUSH_ERR_DONE`. Because that notification has no valid WR `user_ctx`, it is handled only by `local_id` and
    advances the pending-delete record directly; it never re-enters request Event handling or calls `urma_flush_jetty`.
    Jetty-level flush notifications are dispatched before request-level pipeline hooks so the sole notification cannot
    be consumed as an ordinary request completion. Modify/flush/delete failure is quarantined and
    remains inside the configured live-resource bound. A quarantined Jetty keeps its registry identity reserved:
    flush carries only `local_id`, so registration rejects a different live wrapper with the same ID to prevent ABA.
    A send lane is leased once per logical transfer and shared by that transfer's chunk WRs. Its request-generation
    floor snapshots the process-global monotonic 64-bit request ID before the lane creates WRs. After timeout force
    release and Jetty reuse, CQEs below the replacement lane's floor are classified as stale and cannot decrement that
    lane; remaining old WRs are tracked as per-Jetty orphan WRs for diagnostics. This fence relies on request IDs not
    wrapping. The compile-time Pipeline H2D path truncates request IDs and therefore disables timeout force release for
    the complete `BUILD_PIPLN_H2D` build, and disables generation checking on pipeline lanes, until it has a
    non-wrapping completion token. A stale fatal CQE still retires the physical Jetty because its provider status invalidates the
    reused transport object. Force release recovers logical pool availability but does not free provider SQ/JFS
    credits. `URMA_SEND_JETTY_ORPHAN_PRESSURE` reports when the per-Jetty orphan count exceeds the internal warning
    threshold of 16. The internal retire threshold is fixed at 32 against a JFS depth of 256; reaching it keeps the
    Jetty out of the reusable pool, installs normal pending-retire ownership, and queues provider
    modify/flush/delete work on the existing asynchronous finalizer. These thresholds are intentionally not exposed as
    runtime flags. A peer that retires eight send Jetties is circuit-broken to bound its pool damage. Connection
    stability checks retain the broken entry and request the existing single-flight handshake after cooldown.
    Same-incarnation replacements share the retire budget, in-flight cap and exponentially backed-off probe state
    (1 second initially, capped at 30 seconds). Only one lane is admitted while half-open; a completely successful
    lane resets the budget, while failed, cancelled or timed-out probes reopen it. A changed nonempty instance ID
    starts independent state. Old generations cannot admit new work or reset the new probe with late completions.
    Read/write/gather paths release connection-map accessors after copying shared ownership; replacement and removal
    do not clear live holders in place. RPC-shared lanes keep using their original connection generation.
    Last-holder destruction remains synchronous, outside the connection-map lock; `URMA_CONNECTION_CLEANUP`
    measures target/segment unimport cost in the existing perf framework. It is not claimed to be latency-free.
    Worker-to-worker Batch Get is a narrower RPC-scoped exception: `BatchGetObjectRemoteImpl` attempts one shared-lane
    acquire before object processing. On success it passes the lane to ordinary and gather writes and seals it once
    after all sub-request WRs are created. When the acquire fails and transport fallback is enabled, the whole RPC is
    pinned to TCP before object processing; aggregate/GatherWrite and per-object URMA acquire/post are disabled, while
    the existing `TrackUrmaFallbackTcp` admission/accounting path remains in force. With fallback disabled, the original
    acquire error is returned (`K_URMA_TRY_AGAIN` for pool exhaustion), distinct from application-level
    `K_TRY_AGAIN` so an SDK must not replay an already-saturated same-target request. Object WR
    creation/provider-post failures use release cleanup in the shared-lease path. Same-Jetty concurrent post safety remains a provider contract; datasystem's gate
    specifically excludes post/modify and post/delete overlap.
  - URMA receive-side Jetty reuse is process-level: `UrmaResource::GetOrCreateSharedRecvJetty()` lazily creates the
    single RECV Jetty/JFR published by TCP handshake responses. Jetty role is immutable, so RECV async-event retirement
    does not enter the send pool or trigger send-pool refill. Shutdown closes Jetty admission before stopping the poll
    thread; non-converged provider resources are retained fail-closed rather than implicitly deleted. If shutdown still
    has pending or quarantined Jetty resources, `UrmaResource` also retains their shared JFC/JFCE/context dependency
    closure until process exit, and `UrmaManager` skips `urma_uninit` plus dynamic-library unloading. This avoids an
    invalid partial teardown and the resulting expected provider error logs; fully converged shutdown keeps the normal
    explicit cleanup path.
  - URMA write failures preserve raw provider-post and completion status in `UrmaWriteFailure`. For client-side UB
    writes, the object-cache classifier treats raw status `4` as local-port-health query evidence rather than direct
    node-isolation evidence. CQE status `9` remains remote-peer ACK-timeout evidence; wait/RPC timeout evidence without
    a CQE remains `SUSPECT`, and resource-pressure failures remain non-isolating. Health-summary wire encoding maps
    the new CQE-9 reason to the existing hard-unavailable reason so
    rolling upgrades preserve the established enum range. A generic `K_URMA_ERROR` without raw provider/CQE evidence
    is not sufficient for hard isolation.
  - A timed-out URMA WRITE carrying a late-completion observer retains its complete `UrmaEvent` in the existing request
    map instead of copying request identity into a second tombstone object. The timeout transition is serialized with
    completion by the Event mutex and clears the strong `EventWaiter` reference before the foreground request returns;
    the Event already holds only a weak send-lane reference and never owns the payload. A late status-4 CQE consumes the
    original Event and notifies the still-live owner outside Event/retention locks. Retention is bounded to 1,024 Events
    and 3 seconds, with oldest-first capacity eviction, incremental poll-loop expiry, and explicit shutdown cleanup.
    Late status-4 query triggers are accepted only while the originating local sender remains live and open.
    Worker-to-worker late status-9 completions are attributed to the Event's remote endpoint and fenced by a per-peer
    recovery generation; that
    generation advances on successful peer recovery and trusted Worker-incarnation replacement. Reads, writes without
    a live observer, and WRs rejected before provider submission retain the original immediate deletion behavior.
  - Recovery probes preserve the raw `UrmaWriteFailure` through posting and CQE wait. With a local port monitor
    or remote verifier attached, E4/E9 and diagnostic probe completion trigger verification rather than deciding node
    admission. Only confirmed local facts or independent remote Query responses may isolate/recover that node.
    Unconverted peer paths retain their existing path-probe policy; one peer's failure must not be attributed to another.
  - The vendor-neutral contract is `common/object_cache/ub_port_health.h`; Provider returns `vector<UbPortStatus>`
    and Monitor validates/aggregates it into immutable snapshots and summaries. `UrmaManager` owns one shared
    `UbPortHealthMonitor` bound to its URMA context; Client/Worker consumers register weak observers. Client-side
    CQE status `4` only wakes an asynchronous `urma_user_ctl(BONDP_USER_CTL_QUERY_PORT_STATUS)` query. A failed or
    invalid query leaves admission unchanged; a valid all-BAD result rejects all Host object data APIs with
    `K_URMA_WORKER_UNAVAILABLE`. Once isolated, any GOOD port immediately reopens admission, but one-second polling
    continues until all ports are GOOD so a partial recovery can be observed and can regress to all-BAD safely.
    The Client request hot path reads one scalar atomic admission state updated by an existing weak Monitor observer,
    without copying the monitor `shared_ptr`, calling the provider, or taking the monitor lock. Repeated CQE triggers
    are coalesced before acquiring the monitor mutex. Monitor initialization failure follows the existing UB-runtime
    policy: required UB initialization returns the error, while an optional SHM endpoint logs the failure and continues
    with SHM/TCP. An unexpected missing monitor is rate-limited in logs while preserving the defined fail-open behavior.
    Shutdown prevents monitor recreation, joins outstanding queries/callbacks, clears published snapshot/admission
    state, and then tears down the URMA context. Restart collects fresh facts while preserving the previous health epoch
    fence. Shared Monitor startup still schedules the initial query needed by the routing health view.
    UB health synchronization reachable from brpc/bthread callbacks uses bthread-compatible primitives: Monitor and
    callback-drain mutex/CV pairs migrate together, registry/verifier/filter state retains one bthread write mutex,
    and `PeerUbAdmission` plus `UbHealthSummaryCache` retain read/write semantics with `bthread::RWLock`. Client sender
    admission linearizes its closing bit and active count through one CAS state word; only the bthread mutex/CV drain
    pair remains. Data-plane health callbacks acquire a counted lease under their lifecycle mutex, execute summary
    merge, verifier updates, hooks, and wakeups outside that mutex. Detach waits until every acquired lease exits.
    Other Provider/RPC and admission critical-section optimizations remain separate from this synchronization change.
    `UbHealthLeaseSync` keeps its `std::mutex`/`std::condition_variable` pair because only its dedicated native `Thread`
    owns that wait loop; changing its state machine is outside this synchronization commit.
    Pending refresh retains last-confirmed local admission. Query failures never invent all-down facts and pending
    refresh retries remain rate-limited. `ReadSummaryForQuery` reads cached facts and requests coalesced refresh
    without waiting for Provider; stale/in-flight replies are pending. `WorkerRouter`'s frozen DTO is declared in
    `client/object_cache/routing/ub_routing_health.h`; aggregate healthy count is derived, not stored separately.
    `WorkerSelfPortHealth` binds the context monitor without owning its lifetime. Query services read its cache
    and preserve pending state in the encoded response. Cache validity spans one provider interval plus one remote
    verification interval (two seconds), so a completed asynchronous refresh is observable at a one-second RPC cadence.
    The provider and isolated-peer query intervals remain one second; invalid/pending facts cannot change admission.
    Client read/write fault callbacks feed the shared `RemoteUbPortHealthVerifier`; passive all-BAD sidecars update
    `WorkerUbHealthRegistry` and request verification. While a Worker is already isolated, a same-incarnation,
    non-pending, non-all-BAD sidecar newer than the isolation fact also releases the Client write admission and cancels
    the scheduled verifier retry. A confirmed sidecar may share the cached pending fact's health epoch only when its
    port counts are identical. Direct recovery requires a business-response carrier from that Worker; otherwise the
    existing scheduled query remains the fallback, and no extra query is created for the passive summary. Passive write
    recovery preserves requester-local read-source evidence. Sidecar callbacks may overlap; registry incarnation and
    health-epoch fences reject
    stale observations before one complete state is published. The router Getter atomically
    aliases the frozen DTO from that immutable state for observed routable (`ACTIVE` or `LEAVING`) workers;
    topology reconciliation prunes departed workers and stale incarnations without creating UNKNOWN entries for
    workers the Client has never contacted. A verified write-target recovery advances its late-CQE generation fence.
    Due verification is ordered by deadline, with address only breaking ties. Client dispatch never waits for a batch;
    each completion frees one of four outstanding query slots and wakes reconcile. A CQE arriving during an in-flight
    query retains one rate-limited follow-up. Workers without the Query capability retry after 30 seconds rather than
    consuming a slot every second, and the first successful response after a retry compresses other isolated-peer
    deadlines to bound recovery spread. Other passive summary hints only accelerate peers already tracked by the
    verifier; they are not a discovery path. Shutdown drains at most the current four-query wave, whose RPC deadline is
    one second, before destroying endpoint dependencies. Registry publication COW-updates topology, health, verified
    admission, and the routing DTO together; an unchanged topology returns before those copies. Retired snapshots
    release after unlock.
    The private client admission observer is declared in `common/rdma/client_port_health_admission_observer.h`;
    moving this header does not change the atomic gate layout or observer lifetime.
    Native URMA port identity is the chip/die/port tuple. The provider sorts and validates these tuples before
    assigning process-local ordinal indexes; chip-local repeated port indexes are not duplicate physical ports.
    Routing transitions log only published before/after snapshots outside the writer lock, with hex-encoded identity
    prefixes. Batched observations log each worker's final published change, not unpublished intermediate values.
    Worker RemoteGet (single and batch) checks requester self admission before creating the remote RPC/receive path.
    Worker peer and migration paths reuse the same verifier type and a bounded shared query pool. Lease summaries
    carry only aggregate worker health, not a peer matrix; query results retain endpoint/incarnation/epoch fencing.
    Worker dispatch also refills on each completion without a batch future barrier. Callback leases pin the service
    across business work and active queries without holding the lifecycle mutex. Detach rejects new leases and drains
    existing users before service destruction; queued queries then return shutting-down without accessing the service.
    `tests/ut/common/rdma/ub_port_health_test.cpp` covers monitor state and lifecycle branches;
    `tests/ut/common/rdma/urma_port_status_provider_test.cpp` covers the mock ABI-to-provider path; and the mock-only
    Client CQE-4 case in `tests/st/client/object_cache/urma_object_client_test.cpp` covers asynchronous query, Host Get
    and Set rejection at 4/4 BAD, and reopening at 3/4 BAD.
  - when hetero is enabled, RDMA dependencies also pull in device and shared-memory related components.
  - CUDA host-memory registration lives under `common/device/nvidia` but remains independent of hetero GPU and Pipeline
    H2D build switches. It uses CUDA Runtime API declarations when toolkit headers are available and builds as a no-op
    registration helper when they are absent; runtime calls remain dynamically loaded rather than linked to libcudart.
  - HCCS RH2D is compiled only when `cann_hixl` is found and its detected HIXL version is `8.5.2` or newer. Older
    CANN/HIXL environments still build hetero and default ROCE paths, but `remote_h2d_link_type=HCCS` is not available
    because `hixl_transport.cpp` is not compiled and `ASCEND_HIXL_AVAILABLE` is not defined. In supported builds,
    `libdatasystem.so` and `libdatasystem_worker.so` contain only the versioned C ABI loader and opaque HIXL handles;
    `libds_hixl_plugin.so` is the leaf that links `libascendcl.so`, `libcann_hixl.so`, and `libmetadef.so`. The loader
    resolves that fixed-name plugin beside the calling core library, verifies its build-time SHA256, uses
    `RTLD_NOW | RTLD_LOCAL`, validates the v1 function table, and intentionally retains the mapping until process exit.
    Selecting HCCS without a usable plugin fails RemoteH2D initialization; it does not fall back to ROCE. Worker startup
    propagates this failure before allocator initialization, and client configuration preserves it for the first HCCS
    operation. The plugin boundary does not apply to `transfer_engine`.
  - `remote_h2d_link_type=HCCS` selects the HIXL transport. HIXL owns its protocol selection: without
    `HCCL_INTRA_ROCE_ENABLE`, Atlas A2 defaults to RoCE and Atlas A3 defaults to HCCS; setting the variable to `1`
    forces RoCE. Data system passes `remote_h2d_hccs_buffer_pool` through unchanged and does not adapt it to the
    selected protocol. The default `0:0` disables the relay pool and makes the Worker register source host memory;
    a positive `<count>:<size>` value enables the relay pool for either protocol.
  - HCCS RH2D pre-counts each scatter request's descriptors and reserves the active HIXL descriptor vector once so
    it does not grow or relocate while appending. There is no operator-facing descriptor cap: HIXL
    `TransferOpDesc` count is unbounded and HIXL splits the submission across the SQ queue depth internally, so
    the client submits the complete request in one `TransferSync`. Temporary device-memory registration budget
    exhaustion can still force an earlier flush, so pre-registered destination pools are required for the
    single-submit performance baseline.
  - Ascend local Direct H2D supports opt-in descriptor parallelism through `DS_H2D_PARALLEL_WORKER_NUM`; the default
    value `1` keeps Direct serial. It preserves the synchronous `MGetH2D` contract and uses byte-balanced bounded ACL
    batches only when the configured worker count is greater than one and the estimated task count can cover all
    workers. The default aggregate size is 512 descriptors and requests below 24 MiB use serial Direct. A per-device
    bounded worker pool binds the device once per worker; caller-runs binds before its inline task. Both paths share one
    inflight limit, and a request drains all submitted tasks before returning the lowest-index failure. The default
    policy remains unchanged; focused mock-backed coverage lives in
    `tests/st/device/acl_resource_manager_fallback_test.cpp`.
  - FFTS and Huge FFTS H2D additionally support object-level parallelism through
    `DS_H2D_FFTS_PARALLEL_WORKER_NUM` (default `4`) and `DS_H2D_FFTS_PARALLEL_MIN_BYTES` (default 128 MiB). Complete
    objects are byte-balanced by `AclParallelFftsExecutor` across independent copier instances; every active instance
    exclusively owns a control-resource bundle (dispatcher/context, two streams, and four notifies) plus two device
    staging buffers. Synchronized control bundles return atomically to a per-device cache, while staging buffers use a
    separate capacity-aware cache. The executor validates the aggregate per-shard
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
