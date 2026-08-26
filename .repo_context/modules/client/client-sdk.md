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
  - Client-worker startup warmup reuses the real Set/Get path: 20 same-node 256 KiB objects followed by 80 one-byte
    meta-owner objects. The sequence remains serial because startup experiments found no tail-latency benefit from
    16-way warmup. Warmup shares one fixed 500 ms budget and passes its remaining time into Create, Publish, and Get, so
    a 20 ms business `requestTimeoutMs` cannot abort the initialization warmup. Runtime calls keep their original
    timeout because the override defaults to zero. Warmup runs for every transport mode and is independent of both
    URMA runtime enablement and the `enableCrossNodeConnection` failover option. The existing placement-policy split
    covers both `enableLocalCache=true` and `false`, and the meta-owner phase exercises the normal worker outbound path
    without a separate probe protocol. The same-node phase retains its 256 KiB data-plane probes when URMA is enabled;
    other transports use the one-byte connection probe so enabling their warmup does not reserve allocator arenas in
    low-capacity workers. Successfully created warmup objects are batch-deleted before initialization returns; their
    five-second TTL remains a cleanup fallback if that best-effort deletion fails.
  - `datasystem` shared library is built from `src/datasystem/client/*` and is the main user-facing client library.
  - `DsClient` is only a convenience aggregator. It constructs `KVClient`, `HeteroClient`, and `ObjectClient`, then initializes and shuts them down in order.
  - `ConnectOptions` is the common connection/auth/config carrier for C++ clients.
  - `ConnectOptions::enableLocalCache` defaults to `true`; setting it to `false` routes full-object `Get` through
    `TransportLayer`, which batches metadata queries by meta owner and reads successful keys independently, while the
    default path keeps the existing client-worker behavior.
  - KV and Object client code share the same deep backend implementation through `object_cache::ObjectClientImpl`.
  - In BRPC mode, every public `KVClient` operation establishes a `ScopedClientRequestContext` before validation,
    tracing, or backend calls. Standalone calls receive a fresh bthread-local context instead of inheriting pthread
    fallback trace/deadline state after M:N migration. The fresh context selectively preserves the caller-owned tenant
    id and trace prefix while resetting request-scoped trace ids, deadlines, latency, and auth state; calls made with an
    active request context preserve that context. When a standalone call actually tracks a client-to-worker transport,
    the scope publishes only its final transport kind to a separate bthread-local completed-call result so callers can
    query it after the temporary context exits; no other request state is copied back. A fallback Reset/Record clears
    this published result, which keeps non-KV client tracker behavior unchanged. The scope is a no-op in ZMQ mode. This
    boundary currently covers `KVClient` only; the other SDK client families remain unchanged.
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
    when the first trusted incarnation arrives. A different trusted incarnation clears evidence belonging to the old
    Worker process. For the same incarnation, repeated writable summaries preserve newer hard local evidence, while a
    validated global unavailable-to-writable epoch transition clears the matching client-local quarantine. Ordinary
    topology refresh and Global Fact lease expiry do not silently clear versioned local evidence. Global summary reads
    use a shared lock because Direct Read admission is a read-mostly foreground path. When heartbeat summaries are
    disabled or delayed, a quarantined Provider is recovered by the existing TransportLayer reconcile thread rather
    than business retries: after admission backoff expires, the Client sends an endpoint-scoped control RPC, capped at
    three seconds independently of the SDK request timeout, that pulls the Worker's current self summary. A non-writable
    summary keeps the endpoint quarantined and raises the next
    backoff. A writable summary permits the Worker to perform a dedicated one-byte Worker-to-Client UB WRITE into the
    Client's manager-owned probe segment. The recovery request creates that segment before handshake construction and
    serializes only its descriptor, so probe control cost does not scale with the Client's complete registered-segment
    table. Recovery commits only when that CQE succeeds and the endpoint, topology incarnation, and Client-local probe
    epoch still match. Healthy endpoints are not polled, and the foreground Direct Read path performs no additional RPC.
  - Routed same-host Get uses one endpoint-scoped SHM session per target Worker. Object metadata, reference acquisition,
    and `DecreaseReference` use the client-facing `WorkerOCService`; only fd-session bootstrap and control
    (`GetSocketPath`, `RegisterClient`, `GetClientFd`, `DisconnectClient`) use `WorkerService`.
    `ShmTransporter` never falls back to `WorkerWorkerOCService.GetObjectRemote` for an SHM candidate. Each session owns
    its fd-passing socket and private `MmapManager`, while returned Buffers retain a session/mmap owner that releases the
    reference to the actual data Worker. Session failure closes the socket so Worker client-lost cleanup resolves any
    ambiguous Get-side reference increase before a new session is used. Target SHM capability is probed through that
    target Worker's `GetSocketPath` and `RegisterClient`; the initial bound Worker's `IsShmEnable()` is not a capability
    gate for another endpoint. If an SHM-candidate target does not publish an fd-passing endpoint, or rejects
    `RegisterClient` because lossless ScaleIn has entered draining while the object still resides there, the direct
    read keeps the metadata-selected Worker and uses bounded transport fallback: UB first when URMA is enabled, then
    TCP. Only transport/capability failures advance the fallback chain; object, authentication, and application errors
    are returned unchanged. Every candidate shares the public Get deadline and is attempted at most once. The path
    covers topology propagation and selection/admission races after the proactive draining-state route takes effect;
    it never falls back to a Worker-to-Worker object RPC. Routed
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
  - Each non-embedded shared-memory mmap table lazily owns one serial background worker for CUDA host-memory
    registration. An mmap becomes usable before registration completes. KV `Get(..., ReadOnlyBuffer)` copies an
    affected SHM payload under its read latch into Buffer-owned pageable memory. KV `Create`/`MCreate` similarly expose
    Buffer-owned pageable memory while registration is pending; `Set`/`MSet` copy it back into the already allocated
    Worker SHM with a CPU copy before publishing, without waiting for registration. Plain `Get(..., Optional<Buffer>)`
    keeps its existing zero-copy behavior. Mmap-table shutdown drains pin work before entries can unpin and unmap.
  - Embedded mmap entries resolve allocator-owned worker fds to the allocator's existing address and borrow those fds;
    they do not close them. Non-embedded mmap entries instead own the SCM_RIGHTS fd copies received from the worker.
  - `client::TransportLayer` also provides internal same-worker `MCreate`/`MSet` primitives. TCP MCreate allocates local
    buffers and MSet sends one positional MultiPublish payload; UB MCreate uses one MultiCreate RPC, MSet pipelines
    non-blocking per-object URMA writes in bounded groups, and failed writes use bounded TCP payload fallback in the
    same MultiPublish RPC. With local
    cache disabled, public key/value `ObjectClientImpl::MSet` groups keys with the configured data-placement policy
    and sends each same-worker group through these primitives; with local cache enabled it preserves the legacy
    client-worker batch path.
  - Buffer-based `ObjectClientImpl::MSet` preserves the `ExistenceOpt` recorded by its matching `MCreate`. The MCreate
    existence check is an early allocation filter; the final MultiPublish carries NX so the Worker object table and
    metadata owner arbitrate concurrent writers atomically. NX-existing keys remain successful no-ops, while mixing
    non-placeholder Buffers with different existence options in one MSet is rejected before any publish.
  - With `enableLocalCache=false`, `ObjectClientImpl` initializes `client::Routing`; Set/MSet select workers through
    the per-client `ConnectOptions::dataPlacementPolicy`, which defaults to `PREFERRED_SAME_NODE`. Get/MGet always
    build metadata-owner transport requests independent of that write policy; the transport flow then reads
    metadata-selected replicas and may use same-host SHM. With `enableLocalCache=true`, Get/MGet stay on the bound
    Worker path even if routing was initialized for cross-node failover.
    Unavailable workers are excluded during bounded pre-Publish retries. With local cache enabled, both APIs preserve
    the legacy current-worker data path and do not initialize Routing. When URMA is enabled, they initialize the
    TransportLayer runtime only to share the same process-local UB sender admission and dedicated recovery probe used
    by routed writes. A raw provider/CQE status 4 from a legacy UB write therefore quarantines the sender before the
    next Create/MultiCreate and returns `K_URMA_WORKER_UNAVAILABLE` until the probe succeeds.
  - With `enableLocalCache=true` and `enableCrossNodeConnection=true`, `ObjectClientImpl` constructs its
    `TransportLayer` before client initialization completes even when the initially selected same-host Worker does not
    advertise URMA. A later same-host SHM-to-remote UB failover therefore reuses an already-published transport object
    and Routing's full Worker snapshot; the switch path does not lazily replace `transportLayer_`. This preserves the
    existing UB sender admission and reconcile-probe recovery path without adding synchronization to Set/Get. This
    eager construction is transport-neutral for a non-URMA initial Worker: `DataPlaneManager` initializes its generic
    lifecycle without activating the process-local UB runtime. For this compatibility path, later UB activation remains
    owned by a Worker handshake that advertises UB; client-direct pipeline initialization continues to request UB setup
    eagerly. The `TransportLayerOptions` default remains UB-eager, so routed clients with local cache disabled retain
    their existing initialization behavior.
  - Timed-out UB writes retain the transport Event, whose late-completion context holds only a weak reference to that
    originating TransportLayer's sender state plus its generation; the foreground waiter is detached at timeout. If a
    status-4 CQE arrives later, it quarantines that same Client sender and releases the retained Event. Shutdown or a
    completed recovery invalidates the generation, so an old CQE cannot quarantine a replacement sender and the
    retained Event cannot extend the Client or payload lifetime. Client write admission captures that generation under
    the sender-state lock but releases the lock before transport I/O; both synchronous and late failure reports validate
    the captured generation, so the URMA poller never waits behind a foreground completion wait. A stack-owned operation
    token tracks admitted UB Create/Set/MCreate/MSet work without holding the state lock. One atomic gate stores both
    the closing bit and active-token count, so shutdown closes new admission and drains existing tokens through one
    linearized state before destroying the data plane.
  - Client CQE-9 write-target isolation applies to UB writes in routed-only and local-cache modes. A quarantined bound
    Worker remains eligible when its same-host SHM capability keeps the Set off UB; a bound Worker without SHM
    capability joins the routing exclusion set and Set selects another eligible Worker. Request-local retry exclusions
    still override the same-host preference. The fault-free route path uses an atomic empty-observation fast path, and
    UB Set reads peer
    generations from an immutable topology cache instead of taking the admission write lock per request. Late CQE-9
    attribution runs on a dedicated executor, keeping its state-machine locks, formatting, and logging off the URMA
    polling thread.
  - Disabling local cache changes data placement, but the client identity and recovery lifecycle remain bound to the
    bootstrap Worker. Routed retry backoff checks that bound endpoint, and routed `Exist` performs the same check before
    dispatch because the operation can otherwise succeed entirely through another Worker. The check uses a 10 ms
    non-blocking TCP probe and classifies only explicit `ECONNREFUSED`, `ENOTCONN`, or `EHOSTDOWN` results as
    `K_RPC_PEER_DEAD`; timeouts and local probe failures remain unknown and do not poison the client. The successful
    Set/Get hot paths do not add an active probe; routed `Exist` pays one bounded probe to preserve the bound-client
    liveness contract. An explicit peer-dead result also wakes the existing HashRing refresher without changing the
    returned error, replaying the operation, or switching Workers speculatively. The eventual versioned ring remains
    authoritative for rebinding the client.
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
    Same-host `PRE_LEAVING` and `LEAVING` members remain admitted for reads but enter the non-SHM partition, so
    URMA-enabled Clients select UB directly and other Clients select TCP.
    A draining error observed from a stale SHM selection immediately removes that worker from the local SHM candidate
    set, preserving the fallback UB connection for subsequent Gets, and requests a hash-ring refresh even when the
    UB/TCP fallback succeeds. The request is admitted at most once per published transport snapshot, preventing
    concurrent Gets from continually extending the forced-refresh window. A later snapshot rebuilds the candidate set.
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
    host-id map and current master address. The initial fetch uses the configured SDK timeout. Periodic and forced
    refreshes rotate across the active Worker list, cap each RPC at 250 ms and each round at four endpoints, and keep
    probing after a reachable unchanged response so a lagging Worker cannot hide a newer ring published by another
    Worker. Metadata-owner-unavailable Set failures at either Create or Publish, and an explicitly dead bound Worker,
    open the existing six-second forced-refresh window; retries inside that window use a 250 ms interval. Force requests
    coalesce through the refresher deadline and never replay an ambiguous Create. Shutdown checks cancellation between
    probes, so it waits for at most one bounded refresh RPC.
  - A direct local-cache Get that observes `K_RPC_PEER_DEAD` submits one deduplicated switch task per concrete Worker API
    to the existing single-thread switch pool. Client shutdown first stops heartbeat producers, atomically closes that
    pool to new peer-dead submissions, and drains queued tasks before Worker APIs and pending-switch state are released.
  - Stream uses its own `client::stream_cache::StreamClientImpl`.
  - `ListenWorker` closes request admission before invoking recovery callbacks for a changed `worker_start_id` or a
    worker-reported missing client. Recovery callbacks return `Status`; successful heartbeats do not reopen admission
    while mandatory client resources are still pending. Object/KV recovery separates one-shot worker registration from
    retryable decrease-ref and pipeline SHM mmap rebuild, while Stream clears producer/consumer and mmap state before
    reconnecting. During recovery, new requests fail with `K_RPC_UNAVAILABLE`.
    For local-cache cross-node clients, a direct Get that receives `K_RPC_PEER_DEAD` from the currently bound Worker
    also queues a switch on the existing single-thread switch pool, deduplicated by the exact Worker API instance.
    The failed request still returns its original status; the background task retains and revalidates the exact Worker
    slot and API before switching, so a stale failure cannot move a client that has already rebound or suppress a
    peer-dead trigger from the replacement Worker. Submission failures clear the per-instance pending marker.
  - Python bindings are not a separate reimplementation; they bind to C++ classes and helper types through `libds_client_py`.
  - `src/datasystem/client/cluster_query` is a dscli-only read facade with protobuf hidden behind its native
    boundary. It reads one explicitly selected ETCD or Coordinator backend, decodes raw facts locally, and
    projects node health, committed hash ranges, and key routes from one immutable `TopologySnapshot`. It is linked only
    into `libds_client_py`; it is not part of the public C++ `datasystem` SDK ABI or the Worker request path.
  - Synchronous `HeteroClient::MSetD2H(..., outLocalSetKeys)` writes device-memory data to host objects and returns, in
    input order, keys that were absent from the connected worker and whose `MultiPublish` result was confirmed
    successful. Existing local keys are excluded, and ambiguous RPC failures do not report unconfirmed keys. The async
    and Python result surfaces are unchanged.
  - Python package `yr.datasystem` lazily exposes public SDK symbols, `DsTensorClient`, and optional transfer-engine
    bindings so importing `TransferEngine` alone does not eagerly load `libds_client_py` or its `libbrpc` dependency.
  - Client-direct pipeline RH2D serializes manager registration and response application, while independent worker
    batch-get RPCs fan out through the client-owned Get RPC pool after registration completes. The pool is passed
    explicitly into the direct round and is not additionally owned by `TransportLayer`. Each RPC owns its mutable
    request/response/payload state. A fallback payload is copied into a separate `UrmaManager` buffer instead of the
    active receive buffer, and the returned buffer owner retains both handles for external-stream source lifetime.
    Each worker batch also retains a data-plane lease through response application and the round-level wait, preventing
    the corresponding transporter from being reset between receiver preparation and transfer completion.
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

| `IServiceDiscovery` / `ServiceDiscovery` / `CoordinatorServiceDiscovery` | `src/datasystem/client/service_discovery.cpp` | SDK worker selection for C++ callers using `ConnectOptions.serviceDiscovery`; both implementations accept an optional `clusterName`. ETCD discovery maps the logical membership table to the legacy physical prefix `/<clusterName>/datasystem/cluster` (or `/datasystem/cluster` when empty), while Coordinator discovery reads `/datasystem/<clusterName>/cluster` (or `/datasystem/cluster` when empty); `hostIdEnvName` is read from the process env first and then recovered from `<log_dir>/env`, and the resolved SDK host ID is exposed to the shared client backend for Routing and Transport locality. Public `CoordinatorServiceDiscovery::Init` initializes membership/random state and a temporary shared Coordinator proxy handle, publishing it only after the proxy's `Init` succeeds. The discovery object then retains that long-lived handle; repeated successful `Init` calls are idempotent, and all later worker snapshots reuse the same proxy, which routes each Coordinator RPC to the current leader across the discovered candidate set. Provider updates require reconstructing the discovery object or restarting the process. |
| `ICoordinatorDiscovery` | `include/datasystem/utils/coordinator_discovery.h` | Shared public candidate-provider contract used by SDK, Worker, and Coordinator startup. Proxy `Init` calls the provider once, requires a non-empty result, validates each candidate via `HostPort::ParseString`, requires at least one valid candidate, and builds a `CoordinatorLeaderRouter` over all valid candidates. Every later RPC routes through the router to the current leader within a fixed deadline, refreshing candidates from the provider as needed; each attempt receives an equal share of the remaining deadline across distinct untried candidates so a dead cached leader cannot starve alternatives. A successful refresh starts a new candidate round under the same deadline and bounded retry waits, allowing a previously observed follower to be retried if it becomes leader during the call. The router caches the last observed leader identity and notifies subscribers on change. Changing the provider output requires reconstructing the proxy or restarting the process, and multi-node Coordinator leader availability remains the responsibility of the Coordinator Raft layer. The fixed-address fallback uses internal `StaticCoordinatorDiscovery`. |
| `dscli query` | `cli/query.py` -> native query facade | Explicit backend, 5-second budget, local projection |

## Connection And Auth Model

- Verified in `ConnectOptions`:
  - direct worker address via `host` + `port`
  - connection and request timeout controls; the explicit `GetSocketPath` / `RegisterClient` initialization RPCs derive
    their timeout from the current connection-attempt budget, while the long-lived channel default and runtime business
    RPCs use `requestTimeoutMs`
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
  - local-cache routing toggle and Set/MSet placement policy are independent fields on
    `ConnectOptions`. `enableLocalCache=false` routes Get/MGet through the transport layer
    (instead of the bound worker) and supports single- and multi-key full-object `Get` with per-key
    partial results and without RH2D (L2 loading follows the `Get` `queryL2Cache` parameter, default
    true). `dataPlacementPolicy` (default `PREFERRED_SAME_NODE`) controls Set/MSet placement and
    only takes effect when `enableLocalCache=false`; callers may pick `PREFERRED_META_OWNER` to route
    Set/MSet to the metadata owner. The two are intentionally independent: a caller may turn off local
    cache while keeping `PREFERRED_SAME_NODE`, or pair it with any other policy. The Python SDK exposes
    both as kwargs on `KVClient` and `DsClient`: `enable_local_cache=True` (default) and
    `data_placement_policy=DataPlacementPolicy.PREFERRED_SAME_NODE` (default). The pybind `KVClient`
    constructors receive both as trailing positional parameters
    (`py::arg("enableLocalCache") = true`, `py::arg("dataPlacementPolicy") = PREFERRED_SAME_NODE`)
    so existing positional callers remain compatible; `DataPlacementPolicy` is registered as a pybind
    enum (`ds.DataPlacementPolicy.PREFERRED_SAME_NODE / REQUIRED_SAME_NODE / PREFERRED_META_OWNER`).
    `HeteroClient` does not currently expose either kwarg; the underlying `ObjectClientImpl` still
    honors `ConnectOptions::enableLocalCache` for D2H/H2D paths, so a future `HeteroClient` kwarg can
    be added without changing the C++ side.
  - remote H2D toggle
  - optional `IServiceDiscovery`; the public implementations are ETCD-backed `ServiceDiscovery` and coordinator-backed `CoordinatorServiceDiscovery`
  - fast transport shared-memory size
  - routed single-key `Set(StringView)` copy tuning: `DATASYSTEM_SET_MEMCOPY_THREAD_NUM` selects 0 to 4 copy
    workers (default 4; 0 or 1 keeps serial copy), and `DATASYSTEM_SET_MEMCOPY_PARALLEL_THRESHOLD` sets the
    byte threshold for parallel copy (default 4 MiB). Each `ObjectClientImpl` owns a lazy 0-to-N pool; this tuning
    is limited to `ProcessTransportPut` and does not alter SHM Set, `Set(Buffer)`, MSet, or Get copy paths.
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
- Synchronous `MGetH2D` keeps the caller-owned `DeviceBlobList` input read-only and uses request-scoped non-owning
  `H2DObjectView` entries for local/remote source grouping; async calls retain their existing owning state copy.
  Same-node H2D prepares flat pointer/reference arrays without copying `DeviceBlobList::blobs`, while RH2D uses one
  flat backing allocation for all `P2pScatterEntry` destination pointers and sizes in a source group.
- Synchronous `MSetD2H` filters existing objects with request-scoped non-owning `D2HObjectView`/descriptor references
  and moves the selected Buffer owners instead of deep-copying `DeviceBlobList::blobs`; async calls build the same
  views from `AsyncMSetD2HState`'s owning copy. The D2H composer and ACL/CUDA resource managers consume these refs,
  while local/remote `MultiPublish` serializes protobuf `blob_sizes` directly from them with pre-reserved
  `RepeatedField` capacity. D2H does not initialize RH2D/HIXL configuration.
- Python exposes both ETCD-backed `ServiceDiscovery` and coordinator-backed `CoordinatorServiceDiscovery`. When a
  `KVClient` is constructed with either discovery wrapper, the Python layer passes the native `IServiceDiscovery`
  object from the wrapper's public `native_discovery` property into the pybind `KVClient` constructor so the C++
  `ObjectClientImpl` owns initial worker selection and later failover rediscovery through
  `ConnectOptions.serviceDiscovery`. Python callers must call `service_discovery.init()` before constructing
  `KVClient`. Existing-client failover to another discovered Worker still follows the shared client contract: callers
  must set `enable_cross_node_connection=True` / `ConnectOptions::enableCrossNodeConnection=true`.
- Ascend `MSetD2H` can opt into bounded Direct descriptor parallelism with `DS_D2H_PARALLEL_WORKER_NUM>1`.
  Object-level FFTS parallelism defaults to four workers for requests meeting `DS_D2H_FFTS_PARALLEL_MIN_BYTES`
  (default 48 MiB), and
  `DS_D2H_FFTS_PARALLEL_WORKER_NUM=1` restores serial execution. D2H uses its own `DS_D2H_PARALLEL_*` and
  `DS_D2H_FFTS_PARALLEL_*` namespaces so Set tuning does not change the H2D Get path. Both remain synchronous at the
  public API boundary and drain accepted tasks before returning. Parallel D2H FFTS keeps separate control and
  device-submit pools. After stream synchronization, dispatcher/context, two streams, and four notifies return as one
  control-resource bundle to a per-device cache; Huge FFTS device staging buffers use a separate capacity-aware cache.
  Ordinary D2H callback records are preallocated runtime userData with weak state references, so a failed stream
  synchronization can detach callback state without creating a shared-pointer cycle; a late callback safely no-ops.
  Huge FFTS directly targets the destination huge-page buffer and therefore skips callback/CV signaling and H2H future
  creation.

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

## Scale-Out Redirected Metadata Convergence

- Scope: `KVClient::Get` through the routed transport path when `enableLocalCache=false`; Set and ordinary data-plane
  admission are unchanged.
- Reproduction window: an old metadata owner redirects `QueryAndGet` to a newly added Worker after the server topology
  has changed but before the Client's `WorkerSnapshot` contains that endpoint. The normal RPC lookup returns the narrow
  stale-snapshot `K_NOT_READY` before the redirected metadata request is sent.
- Selected behavior: only a server-provided redirect whose topology version is strictly newer than the Client's
  atomically published redirect-admission snapshot may use a metadata-only RPC connection before snapshot convergence.
  Version zero and redirects at or behind the Client snapshot retain the normal admission rejection. The check runs
  before and after RPC-client creation so a concurrently published newer snapshot can revoke the exception. The
  redirect chain also rejects any next-hop topology version below the preceding hop, preventing a newer redirect from
  authorizing a later lookup against an older membership view. The stale-snapshot signal also requests an asynchronous
  `Routing::ForceRefresh`. Any prepared TCP or UB inline-data
  request is cleared before the redirected RPC, so the response resolves locations but does not transfer object bytes.
- Safety boundary: SHM registration, UB establishment, TCP/UB object reads, Set, and initial metadata-owner routing remain
  admitted by the latest transport snapshot. The metadata-only exception neither publishes the endpoint into the
  snapshot nor creates a transporter.
- Validation: first preserve a failing `ObjectMetadataClientTest` on the pre-fix branch under the URMA mock build, then
  make it pass and add invariant tests proving that old/zero-version redirects are rejected, concurrent snapshot
  publication revokes the exception, redirect chains cannot roll back topology versions, and the redirected RPC cache
  does not admit the endpoint's data plane.
  `KVClientTransportGetScaleOutRealUrmaTest.RedirectedMetadataOwnerPrecedesClientSnapshot` preserves the hardware-only
  scale-out timing window and skips under `USE_URMA_MOCK`.
- Rollback: revert the redirected metadata-only fallback. The previous fail-fast plus outer refresh behavior is restored;
  no persisted state or wire-format migration is involved.

## Review And Bugfix Notes

- Common change risks:
  - `ConnectOptions` can affect multiple language bindings and shared backend initialization at once; `serviceDiscovery` is intentionally typed as `std::shared_ptr<IServiceDiscovery>` so SDK clients do not depend on the ETCD implementation;
  - `ObjectClientImpl` is shared by both KV and Object API families, so “KV-only” changes may regress object behavior;
  - direct-read metadata and replica retries share the caller's API deadline. The data phase first polls replicas from
    the fixed metadata location snapshot, but `K_NOT_READY` with `Worker endpoint is absent from latest transport
    snapshot` are treated as stale topology/location signals. Metadata-owner connection, dispatch-deadline,
    peer-dead, client-disconnect, and owner-unavailable failures report the failed endpoint and return that stale
    signal immediately instead of retrying the fixed owner; the outer ObjectClient retry then rebuilds routing from the
    latest authoritative ring:
    the reader tries remaining replicas, and
    `ObjectClientImpl::GetFromTransportLayer` forces the existing hash-ring refresher before it re-routes and re-queries
    metadata only for affected keys with deadline-bounded backoff. Retry state is allocated only for affected keys;
    draining and stale-location budgets advance independently and never reset when the observed policy alternates.
    Concurrent force-refresh requests are coalesced by the refresher's atomic budget, and forced retries retain their
    250 ms minimum interval instead of letting request traffic wake the refresh loop continuously. The transport round
    must still apply structured per-item results before deciding which keys are affected, so mixed batches do not turn
    object-level failures such as `K_NOT_FOUND` into stale-location retries.
    A dead data replica is advanced within the current fixed-location round and does not trigger a metadata refresh.
  - Python-facing behavior can differ from C++ because pybind wrappers convert statuses into exceptions and sometimes rename methods;
  - context propagation changes can affect tracing and multi-tenant behavior across all client operations.
  - `tools/perf/cpu_spike_capture.sh` is an operator-side event trigger for transient client CPU spikes. It samples
    `/proc/<pid>/stat` at low cost and, only after consecutive high-CPU samples, stores a short `perf` recording plus
    process/thread/socket and optional caller-provided metric snapshots. It intentionally does not bulk-copy SDK logs;
    correlate logs afterward using the event timestamps.
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
  - direct-read mode does not dynamically update AK/SK; callers must recreate
    the client to change credentials for that mode. L2 loading in direct-read mode follows the `Get` `queryL2Cache`
    parameter (default true, honored since the ShmSession fd-passing Get change).
  - direct-read endpoint entries use a TBB concurrent map under a lifecycle shared mutex, while each entry has its own
    mutex; different endpoints can initialize connections concurrently and the same endpoint is initialized once.
    After the first `WorkerSnapshot` is published, endpoints absent from the latest snapshot are rejected before cache
    lookup so delayed requests cannot recreate removed entries. A dedicated transport reconcile thread coalesces
    pending updates with latest-wins semantics, detaches absent entries under the lifecycle mutex, and closes their
    data planes after releasing that mutex. Shutdown stops and joins this reconcile thread before closing the manager.
    Worker incarnation changes that reuse the same endpoint are intentionally outside this mechanism. The only
    admission exception is a metadata-only RPC to an owner explicitly returned by a server redirect: it triggers a
    forced ring refresh and clears any TCP/UB inline-data request before connecting. Data-plane creation, including
    SHM and UB, remains snapshot-gated until the refreshed snapshot admits that endpoint.
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
    normal route, metadata, replica, chunk, and payload details remain in `VLOG(1)`. With local cache and cross-node
    connection both enabled, routed same-host reads still use SHM while remote fallback enters `TransportLayer::Get`;
    local-only Get logging remains unchanged. Neither path remaps existing status codes; when every key fails, Get
    returns the first failure in input order. Partial success still returns `K_OK`.
    Same-host routed Get retains the selected worker's invocation guard through SHM response processing, so worker
    switch cleanup cannot unmap the old worker's shared memory while that read is still in flight.
    Client-side dynamic UB activation publishes readiness with release/acquire ordering only after `UrmaManager::Init`
    succeeds; transport selection remains fail-closed during initialization or after failure, without writing the
    process gflag from a heartbeat thread.
    `QueryAndGet` is a Client-to-Worker RPC. The metadata-owner Worker reads only resident local objects inline and
    resolves locations for misses through its existing metadata route; it does not pull, subscribe, create placeholders,
    or load L2 data for this fast path. Same-host requests carry the established SHM session identity, remote UB
    requests carry one preregistered buffer per input key. If SHM preparation fails while URMA is enabled, the client
    tries UB before degrading the whole owner group to TCP; when URMA is disabled, it degrades directly to TCP.
    Results remain positional, and absence of `data_result` means the existing replica-read phase must handle that key.
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
  - On a Worker-local miss, `QueryAndGet` uses side-effect-free `PureQueryMeta` and converts its result into a
    primary-first location list, appending the selected valid non-primary replica when one exists. The established
    `QueryMeta` subscription, payload, and location-update behavior remains unchanged.
  - `tests/st/client/kv_cache/kv_client_transport_get_test.cpp` covers single-key and same-owner multi-key transport
    reads. It disables the local cache, applies the same deterministic hash rule in the SDK and worker processes, and
    resolves the metadata owner through the real SDK `Routing` path before asserting TCP or UB data transport.
  - `tests/st/client/kv_cache/kv_client_transport_set_test.cpp` covers the routed Set transaction over TCP or UB. It
    verifies successful data and metadata publication, complete transaction rerouting after a Publish-time scale-down
    or worker-not-ready response, Set and MSet rerouting from Create and Publish stages, and the rule that an ambiguous
    Publish connection failure is not replayed on another worker.
  - A raw Client-to-Worker CQE status `9` quarantines only that routed Set/MSet write target. The current request keeps
    same-worker TCP fallback semantics; a failed request is rerouted only when Publish was not attempted or bRPC marks
    it definitely unsent. Get admission and the Client-local CQE-status-`4` sender circuit remain independent. The
    transport reconcile thread restores a quarantined target only after an exact Client-to-Worker UB WRITE probe and
    current topology-incarnation fencing succeed.
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
- Run the hardware-independent UB admission benchmark with the URMA mock build configuration:
  - `bazel run --define enable_urma_mock=true //tests/perf/client:peer_ub_admission_timeout_bench -- --threads=16 --reports-per-thread=30000`
  - the admission tool executes the real `PeerUbAdmission::ReportOutcome` state-machine path and reports its state,
    epoch, recovery-probe outcome, and process CPU time; it is not a bRPC or physical-UB CPU measurement.
## Open Questions

- Should service discovery be documented as a C++-only advanced entrypoint for now, since Python constructors do not currently expose it directly?
- Should `DsTensorClient` live in this module document permanently, or split into a future hetero/transfer-engine focused document once that area is deepened?
