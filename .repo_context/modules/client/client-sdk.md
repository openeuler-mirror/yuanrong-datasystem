# Client SDK

## Scope

- Paths:
  - `include/datasystem/*`
  - `src/datasystem/client`
  - `src/datasystem/pybind_api`
  - `python/yr/datasystem`
- Why this module exists:
  - provide the user-facing client APIs for KV, Object, Stream, hetero, and context operations;
  - connect SDK calls to worker/master services through shared memory, RPC, and optional device transfer paths;
  - expose the same core capabilities across C++ and Python.
- Primary source files to verify against:
  - `include/datasystem/datasystem.h`
  - `include/datasystem/kv_client.h`
  - `include/datasystem/object_client.h`
  - `include/datasystem/stream_client.h`
  - `include/datasystem/hetero_client.h`
  - `include/datasystem/context/context.h`
  - `include/datasystem/utils/connection.h`
  - `src/datasystem/client/CMakeLists.txt`
  - `src/datasystem/client/datasystem.cpp`
  - `src/datasystem/client/kv_cache/kv_client.cpp`
  - `src/datasystem/client/object_cache/object_client.cpp`
  - `src/datasystem/client/object_cache/object_client_impl.cpp`
  - `src/datasystem/client/transport/transport_layer.cpp`
  - `src/datasystem/client/transport/object_read/object_read_flow.cpp`
  - `src/datasystem/client/stream_cache/stream_client.cpp`
  - `src/datasystem/client/hetero_cache/hetero_client.cpp`
  - `src/datasystem/client/context/context.cpp`
  - `src/datasystem/client/service_discovery.cpp`
  - `src/datasystem/pybind_api/pybind_register_*.cpp`
  - `python/yr/datasystem/*.py`

## Responsibilities

- Verified:
  - `datasystem` shared library is built from `src/datasystem/client/*` and is the main user-facing client library.
  - `DsClient` is only a convenience aggregator. It constructs `KVClient`, `HeteroClient`, and `ObjectClient`, then initializes and shuts them down in order.
  - `ConnectOptions` is the common connection/auth/config carrier for C++ clients.
  - `ConnectOptions::enableLocalCache` defaults to `true`; setting it to `false` routes full-object `Get` through
    `TransportLayer`, which batches metadata queries by meta owner and reads successful keys independently, while the
    default path keeps the existing client-worker behavior.
  - KV and Object client code share the same deep backend implementation through `object_cache::ObjectClientImpl`.
  - General batch APIs accept up to 10,000 keys. `Exist` accepts up to 100,000 keys, so one query can cover the
    32,768 cache blocks required by a 1 Mi-token context with 32 tokens per key.
  - `KVClient::MGetH2D` requires every input key to be unique and returns `K_INVALID` before pipeline dispatch when
    duplicates are present. This constraint is specific to the KV pipeline H2D path, whose transfer state is tracked
    per input request; it does not change `HeteroClient::MGetH2D`.
  - `client::TransportLayer` provides worker-address-based `Get` plus transport-native `Create`/`Set` primitives. Its
    TCP Set path publishes an RPC payload, while its UB Set path writes the payload through URMA and publishes an empty
    payload, with bounded TCP fallback on UB write failure. `ObjectClientImpl::Put` uses these primitives for the
    non-SHM routed Set path and keeps one worker address fixed across Create, payload transfer, and Publish.
    Transport-owned BRPC channels use the SDK request and connection timeouts; foreground RPCs further clamp the
    per-call timeout to the remaining API deadline.
  - Client-direct Get preserves structured Provider UB failure details even when the data RPC fails. A hard Provider
    ERROR 4 immediately creates requester-local read-source admission evidence; a later request checks each endpoint
    group once, skips the quarantined source with `K_URMA_DATA_WORKER_UNAVAILABLE`, and continues with the next replica.
    Heartbeat health summaries share the same filter but are bound to the responding Worker endpoint and fenced by
    Worker incarnation plus monotonically increasing epoch before they can affect routing or replica admission. The
    requester tags local evidence with the latest trusted incarnation learned from topology membership or a validated
    heartbeat. Evidence learned before either source establishes the endpoint identity is unversioned and is cleared
    when the first trusted incarnation arrives. A matching incarnation never clears later hard local evidence; a
    different trusted incarnation clears evidence belonging to the old Worker process. Ordinary topology refresh and
    Global Fact lease expiry do not silently clear versioned local evidence. Global summary reads use a shared lock
    because Direct Read admission is a read-mostly foreground path.
  - Routed same-host Get uses one endpoint-scoped SHM session per target Worker. Object metadata, reference acquisition,
    and `DecreaseReference` use the client-facing `WorkerOCService`; only fd-session bootstrap and control
    (`GetSocketPath`, `RegisterClient`, `GetClientFd`, `DisconnectClient`) use `WorkerService`.
    `ShmTransporter` never falls back to `WorkerWorkerOCService.GetObjectRemote` for an SHM candidate. Each session owns
    its fd-passing socket and private `MmapManager`, while returned Buffers retain a session/mmap owner that releases the
    reference to the actual data Worker. Session failure closes the socket so Worker client-lost cleanup resolves any
    ambiguous Get-side reference increase before a new session is used. Target SHM capability is probed through that
    target Worker's `GetSocketPath` and `RegisterClient`; the initial bound Worker's `IsShmEnable()` is not a capability
    gate for another endpoint. If an SHM-candidate target does not publish an fd-passing endpoint, the read returns
    `K_NOT_SUPPORTED` without invoking WorkerOC Get or falling back to a Worker-to-Worker object RPC. Routed
    Create/MCreate uses a local payload buffer and never resolves a target Worker's fd
    through the initially bound Worker's fd channel or mmap namespace. If Worker allocation succeeds but local
    `ObjectBuffer` materialization fails, the transporter decreases every allocation returned by that Create/MCreate
    response before propagating the local error. `ObjectBuffer` tracks local allocation ownership independently from
    the Worker `shmId`, so routed payload buffers are freed locally while the `shmId` remains available for Worker
    reference release. Active sessions schedule a bounded
    `WorkerService.Heartbeat` through the process
    `TimerQueue` and existing release pool; this maintains the Worker liveness timestamp, removes expired fds from the
    session mmap table while live Buffers retain their mmap entry, and acknowledges those fds on the next heartbeat so
    the Worker can reuse them. Routed SHM Buffers use the target session's `RegisterClientRsp.lock_id` for their metadata
    latch rather than the SDK's initially bound Worker lock id. Transport selection/admission, session, fd-channel,
    auth, legacy reference state, and the mmap manager/table use bthread mutex/RWLock/condition-variable primitives
    because these paths can be entered from brpc/bthread execution contexts.
  - `client::TransportLayer` also provides internal same-worker `MCreate`/`MSet` primitives. TCP MCreate allocates local
    buffers and MSet sends one positional MultiPublish payload; UB MCreate uses one MultiCreate RPC, MSet pipelines
    non-blocking per-object URMA writes in bounded groups, and failed writes use bounded TCP payload fallback in the
    same MultiPublish RPC. With local
    cache disabled, public key/value `ObjectClientImpl::MSet` groups keys with the configured data-placement policy
    and sends each same-worker group through these primitives; with local cache enabled it preserves the legacy
    client-worker batch path.
  - With `enableLocalCache=false`, `ObjectClientImpl` initializes `client::Routing`; Set/MSet select workers through
    the per-client `ConnectOptions::dataPlacementPolicy`, which defaults to `PREFERRED_SAME_NODE`. The option does not
    change Get/MGet routing.
    Unavailable workers are excluded during bounded pre-Publish retries. With local cache enabled, both APIs preserve
    the legacy current-worker path and do not initialize the direct transport runtime.
  - Routed transport requests carry the gateway client id, token snapshot, thread tenant context, and shared transport
    `Signature`. Target workers authenticate routed Create, Publish, and cleanup requests by signature without requiring
    endpoint-local client registration. UB allocations are released asynchronously after the final Publish attempt, or
    synchronously after a local copy failure; shutdown drains the release queue before closing data-plane connections.
  - Public `ObjectBuffer` keeps transport-owned state opaque and exposes a status-returning `Create` factory; callers
    must pass state whose dynamic type is `ObjectBufferInfo`. Source-tree transport code uses
    `src/datasystem/client/transport/object_buffer_internal.h` for typed access, preventing installed SDK headers from
    depending on client transport or common object-cache implementation headers.
  - `ObjectClientImpl` owns the SDK routing lifecycle: after the initial worker is ready it creates `Routing`, passes the
    SDK host ID already resolved by `IServiceDiscovery`, performs a version-0 `GetHashRing` fetch, starts periodic
    versioned refresh, and stops routing before its transport resources. A direct local connection without service
    discovery may derive the SDK host ID from that local worker; a remote initial worker is never used as the SDK host ID.
    Each changed topology is first validated into a versioned `WorkerSnapshot`. All topology members are retained
    regardless of membership state; any malformed endpoint rejects the whole update, while an empty topology is a
    valid cleanup-all snapshot. The transport admission set is published before the new route becomes visible.
  - Routing owns lazy, endpoint-cached brpc channels only for versioned `GetHashRing` control requests. The channels use
    the SDK request/connect timeouts, disable brpc built-in retry and circuit breaking, and share the transport
    signature holder. Business Create/Get/Set RPCs remain owned by Transport. A later channel-unification change may
    share connection resources, but must preserve this ownership boundary and retry contract.
  - Routing uses closed token ranges (`lower_bound`) so a key hash exactly equal to a token selects that token owner,
    matching the server topology snapshot. Same-node worker addresses are sorted before hash-index selection, making
    placement deterministic across protobuf map iteration orders; this may redistribute same-node-preferred keys once
    at upgrade without changing the policy contract.
  - The Routing-owned `GetHashRing` control request is AK/SK signed. A matching topology version returns only the
    current version and `hash_ring_changed=false`; a mismatch returns one immutable ring snapshot together with its
    host-id map and current master address.
  - Stream uses its own `client::stream_cache::StreamClientImpl`.
  - `ListenWorker` closes request admission before invoking recovery callbacks for a changed `worker_start_id` or a
    worker-reported missing client. Recovery callbacks return `Status`; successful heartbeats do not reopen admission
    while mandatory client resources are still pending. Object/KV recovery separates one-shot worker registration from
    retryable decrease-ref and pipeline SHM mmap rebuild, while Stream clears producer/consumer and mmap state before
    reconnecting. During recovery, new requests fail with `K_RPC_UNAVAILABLE`.
  - Python bindings are not a separate reimplementation; they bind to C++ classes and helper types through `libds_client_py`.
  - `src/datasystem/client/cluster_query` is a dscli-only read facade with protobuf hidden behind its native
    boundary. It reads one explicitly selected ETCD or Coordinator backend, decodes raw facts locally, and
    projects node health, committed hash ranges, and key routes from one immutable `TopologySnapshot`. It is linked only
    into `libds_client_py`; it is not part of the public C++ `datasystem` SDK ABI or the Worker request path.
  - Python package `yr.datasystem` lazily exposes public SDK symbols, `DsTensorClient`, and optional transfer-engine
    bindings so importing `TransferEngine` alone does not eagerly load `libds_client_py` or its `libbrpc` dependency.
  - Client-direct pipeline RH2D serializes manager registration and response application, while independent worker
    batch-get RPCs fan out through the client-owned Get RPC pool after registration completes. The pool is passed
    explicitly into the direct round and is not additionally owned by `TransportLayer`. Each RPC owns its mutable
    request/response/payload state. A fallback payload is copied into a separate `UrmaManager` buffer instead of the
    active receive buffer, and the returned buffer owner retains both handles for external-stream source lifetime.
  - Pipeline RH2D request IDs are generated by `UrmaManager` as contiguous low-40-bit values and remain `uint64_t`
    through client/worker protobufs, chunk-manager maps, and MLCacheDirect send/receive/cancel calls. MLCacheDirect owns
    the transport-specific encoding into the completion context and restores the contiguous ID before completion
    dispatch. Its completion context uses a 60-bit request-ID field followed by one chunk-type bit and three chunk-ID
    bits. The client-worker shared-memory notification retains its explicit actual chunk size, while destination offsets
    follow MLCacheDirect's fixed 2 MiB chunk unit.
- Pending verification:
  - exact internal ownership split between `listen_worker.cpp`, `client_worker_common_api.cpp`, and `embedded_client_worker_api.cpp` for each API family;
  - whether Java and Go clients follow the same runtime layering closely enough to share one future module document.

## Public API Surface

- C++ aggregate:
  - `datasystem::DsClient`
  - obtains `KV()`, `Object()`, and `Hetero()`
- C++ direct clients:
  - `KVClient`
  - `PerfClient`
  - `ObjectClient`
  - `StreamClient`
  - `HeteroClient`
  - `Context`
- Shared config and utility surface:
  - `ConnectOptions`
  - `ServiceDiscovery`
  - `Status`
  - `Buffer`, `ReadOnlyBuffer`, stream producer/consumer types, hetero blob/future types

## Implementation Mapping

| Public surface | Main implementation path | Notes |
| --- | --- | --- |
| `DsClient` | `src/datasystem/client/datasystem.cpp` | convenience wrapper only |
| `KVClient` | `src/datasystem/client/kv_cache/kv_client.cpp` -> `object_cache::ObjectClientImpl` | KV create/set/get path is layered over object-cache client backend |
| `PerfClient` | `src/datasystem/client/perf_client/perf_client.cpp` | perf log reset/get helper for worker/client performance diagnostics |
| `ObjectClient` | `src/datasystem/client/object_cache/object_client.cpp` -> `object_cache::ObjectClientImpl` | object semantics are thin wrappers around shared implementation |
| direct object read | `src/datasystem/client/transport/transport_layer.cpp` -> `object_read/ObjectReadFlow` | groups keys by routed meta owner, then independently polls each key's returned data-worker locations through endpoint transporters |
| SDK object routing | `src/datasystem/client/object_cache/object_client_impl.cpp` -> `src/datasystem/client/routing/*` | `ObjectClientImpl` owns routing initialization, versioned hash-ring refresh, worker selection, failure-state updates, and shutdown; transport owns endpoint connection reuse and same-worker retries |
| `HeteroClient` | `src/datasystem/client/hetero_cache/hetero_client.cpp` plus object/device helpers | integrates D2H/H2D/D2D style operations |
| `StreamClient` | `src/datasystem/client/stream_cache/stream_client.cpp` -> `client::stream_cache::StreamClientImpl` | separate stream cache implementation family |
| `Context` | `src/datasystem/client/context/context.cpp` | thread-local trace and tenant context helpers |
| `IServiceDiscovery` / `ServiceDiscovery` / `CoordinatorServiceDiscovery` | `src/datasystem/client/service_discovery.cpp` | SDK worker selection for C++ callers using `ConnectOptions.serviceDiscovery`; both implementations accept an optional `clusterName`. ETCD discovery maps the logical membership table to the legacy physical prefix `/<clusterName>/datasystem/cluster` (or `/datasystem/cluster` when empty), while Coordinator discovery reads `/datasystem/<clusterName>/cluster` (or `/datasystem/cluster` when empty); `hostIdEnvName` is read from the process env first and then recovered from `<log_dir>/env`, and the resolved SDK host ID is exposed to the shared client backend for Routing and Transport locality. Public `CoordinatorServiceDiscovery::Init` initializes membership/random state and a temporary shared Coordinator proxy handle, publishing it only after the proxy's `Init` succeeds. The discovery object then retains that long-lived handle; repeated successful `Init` calls are idempotent, and all later worker snapshots reuse the same proxy and its fixed Coordinator address. Provider updates require reconstructing the discovery object or restarting the process. |
| `ICoordinatorDiscovery` | `include/datasystem/utils/coordinator_discovery.h` | Shared public candidate-provider contract used by SDK, Worker, and Coordinator startup. Proxy `Init` calls the provider once, requires a non-empty result, validates and caches only `front()`, and ignores remaining candidates. Every later RPC uses that cached address once. Changing the provider output or selected endpoint requires reconstructing the proxy or restarting the process, and multi-node Coordinator availability remains the responsibility of the Coordinator Raft layer. The fixed-address fallback uses internal `StaticCoordinatorDiscovery`. |
| `dscli query` | `cli/query.py` -> native query facade | Explicit backend, 5-second budget, local projection |

## Connection And Auth Model

- Verified in `ConnectOptions`:
  - direct worker address via `host` + `port`
  - connection and request timeout controls
  - token auth, curve key fields, AK/SK fields, tenant id
  - cross-node and exclusive connection toggles
  - `ConnectOptions::enableCrossNodeConnection` gates RUNTIME worker switching only (heartbeat-driven
    `SwitchWorkerHandle` installation, URMA data-plane failure callback, `SaveStandbyWorker` standby
    address retention, voluntary scale-down handling). It does NOT gate Init-stage connection
    establishment or `InitPreferredRemoteFallback`: under `PREFERRED_SAME_NODE` with the same-node
    worker unavailable, Init still admits a remote fallback worker and succeeds even when
    cross-node connection is disabled; only later runtime switch-over is blocked. The flag that
    prevents Init-stage remote fallback is `affinityPolicy = REQUIRED_SAME_NODE`, not
    `enableCrossNodeConnection`.
  - local-cache routing toggle; `enableLocalCache=false` routes Set according to
    `ConnectOptions::dataPlacementPolicy`
    and supports single- and multi-key full-object `Get`, with per-key partial results and without L2 loading or RH2D
  - remote H2D toggle
  - optional `IServiceDiscovery`; the public implementations are ETCD-backed `ServiceDiscovery` and coordinator-backed `CoordinatorServiceDiscovery`
  - fast transport shared-memory size
- Verified in `ObjectClientImpl` constructor:
  - when relevant fields are empty, some connection and auth options are loaded from environment variables such as:
    - `DATASYSTEM_HOST`
    - `DATASYSTEM_PORT`
    - `DATASYSTEM_CLIENT_PUBLIC_KEY`
    - `DATASYSTEM_CLIENT_PRIVATE_KEY`
    - `DATASYSTEM_SERVER_PUBLIC_KEY`
    - `DATASYSTEM_ACCESS_KEY`
    - `DATASYSTEM_SECRET_KEY`
    - `DATASYSTEM_TENANT_ID`
- Pending verification:
  - whether `StreamClientImpl` applies the same environment fallback behavior as `ObjectClientImpl`.

## Python Mapping

- Package entry:
  - `python/yr/datasystem/__init__.py`
- Python facade files:
  - `ds_client.py`
  - `kv_client.py`
  - `object_client.py`
  - `stream_client.py`
  - `hetero_client.py`
  - `ds_tensor_client.py`
  - `util.py`

### Verified Python layering

- `DsClient` in Python mirrors the C++ aggregate pattern by composing Python `KVClient`, `HeteroClient`, and `ObjectClient`.
- `KVClient`, `ObjectClient`, `StreamClient`, and `HeteroClient` wrap `yr.datasystem.lib.libds_client_py` objects.
- `libds_client_py` is populated by `src/datasystem/pybind_api/pybind_register*.cpp`.
- `pybind_register_cluster_query.cpp` exposes internal query functions and converts already-projected native results to
  Python dictionaries. `cli/query.py` owns only argument validation, JSON serialization, and exit codes.
- `DsTensorClient` is a Python-side convenience layer built on top of `HeteroClient` and tensor pointer extraction.
  The former page-attention-specific APIs and `PageAttnUtils` binding have been removed; callers use the remaining
  generic D2H, H2D, and D2D tensor operations.
- Python `HeteroClient` exposes synchronous `mget_h2d_from_multi_buffers` and `mset_d2h_from_multi_buffers` fast paths
  for callers that already own per-key device address and size arrays. The pybind boundary builds request-local
  `DeviceBlobList` descriptors directly from the nested Python lists, without first materializing nested C++ address
  and size vectors, and then reuses the existing C++ `MGetH2D` or `MSetD2H` implementation. Python `batch_is_exist`
  returns native integer indicators for batch consumers while the existing `exist` boolean contract remains unchanged;
  the public C++ API and descriptor ownership rules do not change.
- Python exposes both ETCD-backed `ServiceDiscovery` and coordinator-backed `CoordinatorServiceDiscovery`. When a
  `KVClient` is constructed with either discovery wrapper, the Python layer passes the native `IServiceDiscovery`
  object from the wrapper's public `native_discovery` property into the pybind `KVClient` constructor so the C++
  `ObjectClientImpl` owns initial worker selection and later failover rediscovery through
  `ConnectOptions.serviceDiscovery`. Python callers must call `service_discovery.init()` before constructing
  `KVClient`. Existing-client failover to another discovered Worker still follows the shared client contract: callers
  must set `enable_cross_node_connection=True` / `ConnectOptions::enableCrossNodeConnection=true`.
- Ascend `MSetD2H` can opt into bounded Direct descriptor parallelism with `DS_D2H_PARALLEL_WORKER_NUM>1`, or
  object-level FFTS parallelism with `DS_D2H_FFTS_PARALLEL_WORKER_NUM>1`; D2H uses its own `DS_D2H_PARALLEL_*` and
  `DS_D2H_FFTS_PARALLEL_*` namespaces so Set tuning does not change the H2D Get path. Both remain synchronous at the
  public API boundary and drain accepted tasks before returning. Parallel D2H FFTS keeps separate control and
  device-submit pools so each object shard preserves the existing device-to-host/host-to-host pipeline overlap.

### Verified Python/C++ differences to remember

- Python `Context` currently exposes `set_trace_id`, but not `SetTenantId`, even though C++ `Context` has both APIs.
- Python `KVClient` is backed by a pybind class named `KVClient` whose underlying C++ object is `ObjectClientImpl`.
  `DsClient`, `ObjectClient`, and `HeteroClient` may still resolve a service discovery object to a static worker address
  at the Python facade unless their wrappers explicitly pass native discovery through their own pybind constructors.
- Python package init only configures the bundled transfer-engine P2P DSO path when present; public SDK classes and
  transfer-engine bindings are loaded on first attribute access to keep TE-only imports isolated from `libbrpc`.
- Python wrappers raise exceptions on error instead of returning `Status` objects in the same way the C++ API does.

## Important Internal Neighbors

- Upstream callers:
  - user C++ applications
  - Python applications through `yr.datasystem`
  - tests under `tests/ut` and `tests/st`
- Downstream modules:
  - `src/datasystem/worker` services
  - `src/datasystem/master` metadata and coordination services
  - `src/datasystem/common/*` for RPC, shared memory, logging, metrics, rdma, kvstore, device helpers
  - `src/datasystem/protos`
  - optional `transfer_engine`

## Build And Packaging

- Main build definition:
  - `src/datasystem/client/CMakeLists.txt`
  - `bazel/BUILD.bazel`
- Notable facts:
  - client sources build both `datasystem_static` and `datasystem` shared library
  - `cluster_query_client` is a separate internal static target from `src/datasystem/client/cluster_query`; CMake and
    Bazel link it into `ds_client_py`/`libds_client_py` without appending its sources to `CLIENT_SRCS`.
  - `ds_router_client` is a separate client-facing library built from `router_client.cpp`
  - Python bindings are built from `src/datasystem/pybind_api` when Python API build is enabled; for Bazel wheel builds on `0.8.2`, `libds_client_py` must link from its own deps instead of `dynamic_deps = ["//:datasystem"]`, otherwise the installed wheel can fail at import time with unresolved Abseil log symbols.
  - transfer engine is only added from the root build when transfer-engine, hetero, and NPU-related conditions are satisfied
  - client transport sources are listed explicitly in both `src/datasystem/client/BUILD.bazel` and
    `src/datasystem/client/CMakeLists.txt`; transport buffer implementations likewise require synchronized Bazel and
    CMake source lists under `src/datasystem/common/object_cache`
  - Bazel target `//bazel:datasystem_sdk` packages a C++ SDK directory tree at `bazel-bin/bazel/datasystem_sdk/cpp` and also outputs `bazel-bin/bazel/datasystem_sdk.tar`; headers are under `cpp/include/datasystem/`, and the shared library is `lib/libdatasystem.so`

## Review And Bugfix Notes

- Common change risks:
  - `ConnectOptions` can affect multiple language bindings and shared backend initialization at once; `serviceDiscovery` is intentionally typed as `std::shared_ptr<IServiceDiscovery>` so SDK clients do not depend on the ETCD implementation;
  - `ObjectClientImpl` is shared by both KV and Object API families, so “KV-only” changes may regress object behavior;
  - direct-read metadata and replica retries share the caller's API deadline. The data phase first polls replicas from
    the fixed metadata location snapshot, but `K_NOT_READY` with `Worker endpoint is absent from latest transport
    snapshot` is treated as a stale topology/location signal: the reader tries remaining replicas, and
    `ObjectClientImpl::GetFromTransportLayer` forces the existing hash-ring refresher before it re-routes and re-queries
    metadata only for affected keys with deadline-bounded backoff. The transport round must still apply structured
    per-item results before deciding which keys are affected, so mixed batches do not turn object-level failures such as
    `K_NOT_FOUND` into stale-location retries.
  - Python-facing behavior can differ from C++ because pybind wrappers convert statuses into exceptions and sometimes rename methods;
  - context propagation changes can affect tracing and multi-tenant behavior across all client operations.
- Important invariants:
  - `DsClient` init order is KV -> Hetero -> Object; shutdown order is Object -> Hetero -> KV.
  - worker connectivity and auth material may come from explicit options or environment fallback in shared client backend code.
  - Worker FD integers are process-local identifiers, not stable SHM identities. After worker restart, the client must
    remove old mmap-table lookup entries and receive new SCM_RIGHTS FDs even when the new worker reuses the same integer.
    Existing Buffer and Stream page objects retain their old mapping through `shared_ptr<IMmapTableEntry>`; Object/KV
    Buffers additionally use `workerVersion` to reject cross-incarnation access as `K_BUFFER_DEPRECATED`.
  - If worker registration succeeds but mandatory SHM mmap rebuild fails, recovery retries only the rebuild stage for
    that worker incarnation. Re-registering the same client ID would be rejected by the worker client table. Object/KV
    recovery stage transitions are serialized by the bthread-friendly `shmRecoveryMutex_`; this lock is used only by
    background recovery and timeout callbacks, never by the foreground request hot path.
  - direct-read mode does not dynamically update AK/SK and does not load missing objects from L2; callers must recreate
    the client to change credentials for that mode.
  - direct-read endpoint entries use a TBB concurrent map under a lifecycle shared mutex, while each entry has its own
    mutex; different endpoints can initialize connections concurrently and the same endpoint is initialized once.
    After the first `WorkerSnapshot` is published, endpoints absent from the latest snapshot are rejected before cache
    lookup so delayed requests cannot recreate removed entries. A dedicated transport reconcile thread coalesces
    pending updates with latest-wins semantics, detaches absent entries under the lifecycle mutex, and closes their
    data planes after releasing that mutex. Shutdown stops and joins this reconcile thread before closing the manager.
    Worker incarnation changes that reuse the same endpoint are intentionally outside this mechanism.
  - transport RPC clients share a transport-owned `Signature` instance and sign each fully populated request immediately
    before sending it.
  - Set retries rebuild RPC or UB state once on the same worker inside `TransportLayer`. Cross-worker retry starts a
    new Create transaction, excludes `K_SCALE_DOWN` workers without poisoning their global health, and reports
    connection failures through `Routing::UpdateState`. A worker `K_NOT_READY` response safely reroutes the complete
    Create-to-Publish transaction. A Publish `K_RPC_UNAVAILABLE` is rerouted only when brpc reports a conservative
    connection-establishment failure with complete per-attempt diagnostics that proves the request was not sent;
    missing diagnostics, timeout, EOF, connection reset, and other ambiguous failures are never replayed on another
    worker. If a same-worker retry follows an ambiguous first Publish result, `TransportLayer` preserves that first
    result so a later connection-refused error cannot make replay appear safe.
  - `ObjectClientImpl::InitWithServiceDiscovery` keeps a per-Init local exclusion set: a worker
    whose Init attempt fails with `K_RPC_UNAVAILABLE` or `K_CLIENT_WORKER_DISCONNECT` (or
    `K_RPC_DEADLINE_EXCEEDED` under the zmq transport, where a post-TCP-probe RPC timeout is the
    only dead-worker signal) is skipped for the remainder of THAT Init call so the bounded retry
    switches to a different candidate instead of re-selecting a dead worker still shown READY
    inside the etcd lease window. The set is a function-local `unordered_set<HostPort>`, not a
    member, so it is cleared when Init returns (no cross-Init persistence, no permanent
    blacklist); a worker that restarts is selectable in the next Init call or via the runtime
    `RecoverPreferredLocalWorker` path. Under `REQUIRED_SAME_NODE` remote fallback is never
    admitted even when all same-node candidates are excluded.
  - Routed Set and MSet use the remaining SDK `ApiDeadline` for transport Create and Publish RPCs. brpc delivers the
    selected RPC timeout to the target worker, whose generated unary adapter initializes `reqTimeoutDuration` before
    entering the object-cache handler. Nested request contexts inherit that deadline, so metadata-owner retry loops and
    worker-to-master RPCs remain bounded by the caller's request budget. Connection-establishment failures still return
    immediately and follow bounded rerouting, while a slow but reachable worker can finish recovery work within that
    budget.
  - Worker metadata publication uses bounded per-owner attempts. Retryable Set failures re-resolve the metadata owner
    from the latest placement before the next attempt; MSet re-groups only unresolved keys and preserves successful
    owner groups. The MSet metadata route loop is bounded by both the API deadline and 10 attempts. MSet keys whose
    metadata was not created are never transitioned to the published state and are returned through
    `failed_object_keys` when either retry budget is exhausted. Seal metadata requests remain non-retryable after the
    master call is attempted; only route-resolution failures before that call may refresh the route and retry.
  - A worker absent from the latest transport snapshot returns `K_NOT_READY`, not object-level `K_NOT_FOUND`. Because
    no RPC was sent, routed Set and MSet may safely exclude that worker and rebuild the request on a current route.
    Routed direct Get uses the same narrow signal to recover from topology/metadata skew during scale or rolling
    changes; do not broaden this to all `K_NOT_READY` statuses, because SDK startup, shutdown, and non-snapshot
    readiness failures are different conditions.
  - Transport MSet preserves worker-reported partial failures and performs at most one same-worker UB recovery attempt.
    Routed `MultiCreateReqPb` and `MultiPublishReqPb` requests carry `is_routed=true`; target workers authenticate their
    signatures and tenant IDs without requiring the client to register separately on every metadata-owner worker.
    MultiCreate has no idempotency marker, so `K_RPC_UNAVAILABLE` is treated as an ambiguous allocation result: the
    transport state is torn down for the next request, but the current MultiCreate is not replayed. For MSet,
    `K_URMA_NEED_CONNECT` resets only the cached UB data plane and reuses the RPC client. A pre-Publish
    `K_RPC_UNAVAILABLE` may rebuild both RPC and data-plane state and retry once; after `InvokeMultiSet` starts, the same
    code is ambiguous and is not replayed. A dead UB connection is never converted into whole-batch TCP fallback. If the
    same-worker retry still returns `K_URMA_NEED_CONNECT`, `ObjectClientImpl` maps it to
    `SetFailureStage::TRANSFER`, allowing the routing layer to exclude that worker and reroute the group. Only
    per-object failures returned after `WritePayload` may use bounded TCP fallback: limiter admission sends that object
    as a TCP payload, while limiter rejection marks only that key failed and allows other objects to publish.
    `MultiPublishReqPb` has no retry marker, so ambiguous RPC failures are not replayed on the same or another worker.
    Pre-Publish rerouting recomputes a worker for every key and regroups the remaining batch instead of moving the whole
    group to the first key's fallback worker. UB writes are submitted and completed under lifecycle lock windows bounded
    by the smaller of 32 objects and the configured process send-lane pool; the MultiPublish RPC uses a separate lock
    window, allowing teardown to proceed between large-batch write groups without permitting the active connection to
    be closed during a write or publish operation. During rolling upgrade, workers must support routed MultiCreate and
    MultiPublish authentication before clients enable routed MSet traffic.
  - `TransportLayer::Get` owns `[TransportGet]` request boundaries and prints `ObjectReadResult::actualKind` as the
    successful request's SHM/UB/TCP transport. Redirect-follow events and successful transport requests use `INFO`;
    recoverable retry, fallback, degradation, metadata movement, and replica switching use `WARNING`;
    terminal route, metadata, data-plane, deadline, replica, missing-result, and materialization failures use `ERROR`.
    Data-plane dispatch logs each selected transporter at `INFO` before sending, and metadata warnings and errors are
    emitted for every occurrence. Other repeated degradation and per-key failure sites are sampled where needed, while
    normal route, metadata, replica, chunk, and payload details remain in `VLOG(1)`. Because `enableLocalCache=true`
    does not enter `TransportLayer::Get`, its gateway Get logging remains unchanged. Neither path remaps existing status
    codes; when every key fails, Get returns the first failure in input order. Partial success still returns `K_OK`.
    `MasterOCServiceImpl::QueryAndGet` logs authenticated handler entry at `INFO`, terminal handler failures at `ERROR`,
    and successful completion through `SLOW_LOG_IF_OR_VLOG` using the configured server process threshold.
  - when the existing client latency trace is enabled for a request, transport-layer Get contributes
    `client.process.direct_route`, `client.rpc.direct_query_and_get`, `client.rpc.direct_get_data`, and
    `client.process.direct_materialize` to the request `latencySummary`. The two RPC-class phases are parent-thread wall
    times, so they intentionally include connection setup, lifecycle-lock contention, task dispatch/wait, and bounded
    retries around the nested RPC or data-plane operation.
  - public full-object `Get` installs one API deadline before choosing the local-cache or direct-read path. A same-host
    SHM attempt and its TransportLayer fallback share that deadline instead of restarting the configured request timeout.
  - traced transport-layer Get data-plane attempts record connection acquisition or rebuild, entry-lock waits,
    RPC-client creation, URMA handshake/finalization, transfer, and retry preparation. If any recorded phase exceeds its
    client process or RPC slow threshold, one `[TransportGet] Phase latency` log prints the slow phase names and all
    recorded durations. Untraced requests do not construct the recorder, read the threshold config, or format the log.
  - `QueryAndGet` returns at most five copy locations per object. The primary address from object metadata is returned
    first, followed by non-primary locations, so replica retry always starts with the primary copy.
  - `tests/st/client/kv_cache/kv_client_transport_get_test.cpp` covers single-key and same-owner multi-key transport
    reads. It disables the local cache, applies the same deterministic hash rule in the SDK and worker processes, and
    resolves the metadata owner through the real SDK `Routing` path before asserting TCP or UB data transport.
  - `tests/st/client/kv_cache/kv_client_transport_set_test.cpp` covers the routed Set transaction over TCP or UB. It
    verifies successful data and metadata publication, complete transaction rerouting after a Publish-time scale-down
    or worker-not-ready response, Set and MSet rerouting from Create and Publish stages, and the rule that an ambiguous
    Publish connection failure is not replayed on another worker.
  - standby failover candidate order is randomized per switch attempt, so when one worker fails a batch of clients can spread across the remaining ready workers instead of stampeding to the first candidate in a shared list.
  - after a standby switch publishes the new current worker, cleanup of the previous worker's mmap fds captured at switch commit runs immediately when that worker API has no pending invocations; otherwise cleanup is deferred until its invocation count reaches zero. Cleanup removes only the captured fds, so mappings added for another worker before the deferred callback runs are preserved.
  - Python `DsTensorClient` depends on `HeteroClient`; tensor features are not an independent transport stack.
- Useful debug points:
  - `src/datasystem/client/object_cache/object_client_impl.cpp`
  - `src/datasystem/client/service_discovery.cpp`
  - `src/datasystem/pybind_api/pybind_register_*.cpp`

## Fast Verification

- Rebuild repository artifacts:
  - `bash build.sh`
- Rebuild tests:
  - `bash build.sh -t build`
- Run client-related C++ tests by label:
  - `bash build.sh -t run_cases -l ut`
  - `bash build.sh -t run_cases -l st`
- Narrow by test binary when iterating:
  - inspect `tests/ut/CMakeLists.txt` and `tests/st/CMakeLists.txt` for binaries such as `ds_ut`, `ds_ut_object`, `ds_st_object_cache`, `ds_st_kv_cache`
## Open Questions

- Should service discovery be documented as a C++-only advanced entrypoint for now, since Python constructors do not currently expose it directly?
- Should `DsTensorClient` live in this module document permanently, or split into a future hetero/transfer-engine focused document once that area is deepened?
