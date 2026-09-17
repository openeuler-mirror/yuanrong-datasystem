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
  - `KVClientConfig` carries first-Init process settings. In addition to logging and monitoring, its
    `UrmaSendLaneCountPerPeer` Builder setter configures the Client's outbound per-peer URMA send-lane cap; the
    connection snapshots `min(configured cap, process lane-pool size)` when it is constructed.
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
  - `enable_ub_fault_isolation`, set through `KVClientConfig::Builder::UbFaultIsolationEnable` or the
    `DATASYSTEM_ENABLE_UB_FAULT_ISOLATION` environment variable, decides whether this Client SDK process applies UB
    port-health results as policy. The first Client `Init` of any kind resolves the value once and every later Client
    reuses it, so a later `Init` cannot flip a Client that is already serving; before that resolution the switch
    mirrors the flag. The default is enabled, and the switch never disables Worker-side UB isolation. When it is off,
    Client components
    are still created and still receive health summaries, but the results stay inert at five boundaries: local port
    admission always accepts, `UbHealthFilter` availability always accepts, the failure-report entries never quarantine
    the current request's Provider or write target, local UB writes arm no late-completion observer, and the routing
    snapshot publishes no port health, so scheduling keeps every UB path UNKNOWN. Recovery entries are no-ops as well,
    so no verification or recovery RPC is sent.
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
    Periodic session maintenance uses a dedicated client data-plane thread pool and never shares the asynchronous
    reference-release queue, so a release backlog cannot delay Worker liveness updates.
    Voluntary scale-down, restart, heartbeat failure, and other Worker cleanup paths retain the normal five-millisecond
    CUDA host-memory unregister interval. Normal Object/KV Client shutdown sets a Client-wide exit flag before SHM
    transport cleanup, so every entry owned by that Client skips the unregister interval even when a Buffer delays entry
    destruction.
    `ShmTransporter` never falls back to `WorkerWorkerOCService.GetObjectRemote` for an SHM candidate. Each session owns
    its fd-passing socket and private `MmapManager`, while returned Buffers retain a session/mmap owner that releases the
    reference to the actual data Worker. Session failure closes the socket so Worker client-lost cleanup resolves any
    ambiguous Get-side reference increase before a new session is used. Target SHM capability is probed through that
    target Worker's `GetSocketPath` and `RegisterClient`; the initial bound Worker's `IsShmEnable()` is not a capability
    gate for another endpoint. If an SHM-candidate target does not publish an fd-passing endpoint, or rejects
    `RegisterClient` because lossless ScaleIn has entered draining while the object still resides there, the direct
    read keeps the metadata-selected Worker and uses bounded transport fallback: UB first when URMA is enabled, then
    TCP. Only transport/capability failures advance the fallback chain; object, authentication, and application errors
    are returned unchanged. Every candidate shares the public Get deadline and is attempted at most once. A data Worker
    `K_NOT_FOUND` is terminal because it is returned after Worker-side retries are exhausted; among not-found statuses,
    only `K_WORKER_PULL_OBJECT_NOT_FOUND` remains eligible for Client replica retry. The path
    covers topology propagation and selection/admission races after the proactive draining-state route takes effect;
    it never falls back to a Worker-to-Worker object RPC. Routed
    Create/MCreate uses a local payload buffer and never resolves a target Worker's fd
    through the initially bound Worker's fd channel or mmap namespace. If Worker allocation succeeds but local
    `ObjectBuffer` materialization fails, the transporter decreases every allocation returned by that Create/MCreate
    response before propagating the local error. `ObjectBuffer` tracks local allocation ownership independently from
    the Worker `shmId`, so routed payload buffers are freed locally while the `shmId` remains available for Worker
    reference release. Active sessions schedule a bounded
    `WorkerService.Heartbeat` through the process
    `TimerQueue` and dedicated maintenance pool; this maintains the Worker liveness timestamp, removes expired fds from the
    session mmap table while live Buffers retain their mmap entry, and acknowledges those fds on the next heartbeat so
    the Worker can reuse them. Routed SHM Buffers use the target session's `RegisterClientRsp.lock_id` for their metadata
    latch rather than the SDK's initially bound Worker lock id. Transport selection/admission, session, fd-channel,
    auth, legacy reference state, and the mmap manager/table use bthread mutex/RWLock/condition-variable primitives
    because these paths can be entered from brpc/bthread execution contexts.
  - Non-embedded shared-memory mmap entries submit CUDA host-memory registration to one client-wide serial pin worker.
    Their last-reference deleter submits unregistration, unmapping, and final entry destruction to a separate
    client-wide serial unmap worker, so the thread releasing the entry does not wait for fragmented unregistration.
    Both workers share one operation mutex, so whole-mapping registration and unregistration cannot overlap, although
    the two queues do not provide global FIFO ordering. An mmap is usable immediately: KV `Create`/`MCreate` and both
    Buffer-returning `Get` variants expose the Worker SHM directly without waiting for registration or allocating a
    temporary Host buffer. Registration and unregistration divide each Worker mapping into fixed 64 MiB fragments
    (with a smaller tail fragment when needed) and wait 5 ms between fragments during normal Worker cleanup, including
    voluntary scale-down. Removing an mmap entry marks it retired before dropping the table reference, so a queued or
    running registration stops after any in-flight fragment returns and unregisters only its successfully registered
    prefix. Object/KV Client shutdown likewise stops registration after any in-flight fragment returns, skips all
    remaining registration fragments and their intervals, and skips the unregister interval. Client
    `DsCudaMemcpyAsync` splits H2D/D2H ranges at those planned fragment boundaries only when the Host pointer belongs to
    a Worker SHM mapping; other Host memory is submitted as one copy. The pin task retains the mmap entry, so shutdown
    cannot unpin or unmap it while registration is still running. Per-fragment register/unregister start and finish
    details are `VLOG(1)`; failures remain `ERROR`, while each whole Worker mapping emits `INFO` start/finish summaries
    with elapsed time and failure counts. A `DsCudaMemcpyAsync` crossing fragment boundaries emits one `VLOG(1)`
    summary. With VLOG disabled, operations exceeding 100 ms still emit thresholded `INFO` diagnostics:
    `CUDA_HOST_SLOW` measures a register/unregister callback, `CUDA_MEMCPY_SLOW` measures each memcpy callback,
    and `CUDA_MEMCPY_PREPARE_SLOW` measures registry lookup, split preparation, and the existing cross-fragment log.
    Each process independently rate-limits six categories: register, unregister, memcpy H2D/D2H, and prepare H2D/D2H.
    The first slow event is admitted immediately; later admissions are at least 10 seconds apart per category.
    The limiter uses `CudaSlowLogState` and `TryAcquireCudaSlowLog` in the existing
    `common/device/nvidia/cuda_host_memory.h/.cpp`. It uses relaxed atomics and one CAS attempt without a mutex or
    retry loop, only for slow events.
    Each admitted log includes `suppressed_count`; concurrent suppressions may be attributed to this or the next log.
    `suppressed_max_us` reports the best-effort maximum suppressed duration, excluding the admitted operation.
    It uses one relaxed atomic CAS attempt without retry; contention may underestimate the maximum. Count and maximum
    are independently exchanged with zero on admission and may be attributed to adjacent logs at concurrent boundaries.
    Suppressed events do not trigger a timer-based summary: they are reported only on a later admitted slow event.
    Admission does not bypass the existing INFO severity or request-sampling filters.
    Host callback timing starts after the start log; slow diagnostics include elapsed microseconds and an end Unix
    timestamp in microseconds captured before log emission. These are host-side durations, not GPU execution times;
    reconstructing the start from the end timestamp is approximate. Unregister reports size zero because its callback
    has no size argument. A callback exception makes its return-code field invalid. Diagnostics appear only after
    the call returns or throws, do not detect permanently stuck calls, and do not change CUDA synchronization.
    Fast fragments retain their existing VLOG level; whole-mapping summaries and error logs remain unchanged.
    Worker mapping publication copies live weak entries under a writer-only mutex and atomically publishes an
    immutable registry Snapshot. Concurrent `DsCudaMemcpyAsync` range lookups atomically retain and scan one Snapshot,
    so they neither block publication nor invoke a CUDA callback while holding an internal registry lock.
    CUDA-enabled applications must call `KVClient::RegisterCudaFuncs` before initializing any `KVClient`; the first
    valid process-wide callback table is frozen, and later registration attempts are ignored with a warning. All four
    callbacks (`hostRegister`, `hostUnregister`, `getErrorString`, and `memcpyAsync`) must be non-null for registration
    to be valid. Pin, unpin, and memcpy operations each use one synchronized callback snapshot without holding the
    publication lock while invoking application code.
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
    TransportLayer runtime to share one process-local UB port-health admission gate. A raw provider/CQE status 4 from
    a client UB write requests an asynchronous `urma_user_ctl` port query; it does not itself quarantine the sender.
    Only a valid all-BAD result closes the eight Host data entry paths: `Create`, `Put`, `Get`, buffer `Set`, buffer
    `MSet`, `MCreate`, key/value `MSet`, and `Publish`; rejection uses `K_URMA_WORKER_UNAVAILABLE`. This node-level gate
    intentionally applies before SHM/UB/TCP selection: after the client process confirms that every local UB port is
    BAD, it rejects all Host object data APIs rather than allowing a request that will later require the unavailable
    local UB endpoint. The foreground request that supplied CQE 4 keeps its existing fallback/result semantics because
    the port query is asynchronous; cleanup of that request uses TCP rather than the failed UB path.
  - With `enableLocalCache=true` and `enableCrossNodeConnection=true`, `ObjectClientImpl` constructs its
    `TransportLayer` before client initialization completes even when the initially selected same-host Worker does not
    advertise URMA. A later same-host SHM-to-remote UB failover therefore reuses an already-published transport object
    and Routing's full Worker snapshot; the switch path does not lazily replace `transportLayer_`. This preserves the
    existing UB sender admission path without adding locking to Set/Get. This
    eager construction is transport-neutral for a non-URMA initial Worker: `DataPlaneManager` initializes its generic
    lifecycle without activating the process-local UB runtime. For this compatibility path, later UB activation remains
    owned by a Worker handshake that advertises UB; client-direct pipeline initialization continues to request UB setup
    eagerly. The `TransportLayerOptions` default remains UB-eager, so routed clients with local cache disabled retain
    their existing initialization behavior.
  - Timed-out UB writes retain the transport Event, whose late-completion context holds only a weak reference to that
    originating TransportLayer's sender state; the foreground waiter is detached at timeout. If a
    status-4 CQE arrives later, it requests the same process-local port query and releases the retained Event. The
    shutdown gate prevents an old CQE from affecting a destroyed transport, and the
    retained Event cannot extend the Client or payload lifetime. Client write admission uses a CAS on one atomic gate
    containing both the closing bit and active-token count, so it holds no sender-state lock around transport I/O.
    A stack-owned operation token tracks admitted UB Create/Set/MCreate/MSet work; shutdown closes new operations in
    the same modification order and drains existing tokens through a bthread-compatible mutex/CV pair before
    destroying the data plane.
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
    Non-TCP Create/MultiCreate also carry client-generated allocation UUIDs that are reused by same-worker retries and
    transport fallback. The Worker uses each UUID as the `shmId`, so an ambiguous RPC result can be cleaned up without
    receiving the Create response. The allocation fields use numbers after the authentication fields so older Workers
    preserve the canonical serialization order used for signature verification. Ambiguous IDs are batched into bounded
    background DecreaseReference retries on a dedicated four-thread pool. Submission is non-blocking and drops cleanup
    when all four slots are occupied, incrementing `client_ambiguous_create_cleanup_dropped_total`; retries reuse the
    cached RPC client without repeated teardown. A
    later successful SHM/UB attempt with the same ID cancels that cleanup, while a TCP fallback keeps cleanup pending
    because it does not consume the Worker allocation. Worker-first rolling upgrade is required for immediate cleanup.
    The retry backoffs `{0, 100, 400}` create an approximately 500 ms scheduling window measured from cleanup-task
    start. Each cleanup RPC is capped at 500 ms so a failed Worker cannot extend Client shutdown by the normal request
    timeout; RPC execution and cleanup-pool scheduling are outside the scheduling-window bound. This is a best-effort
    window, not a protocol guarantee. If every DecreaseReference reaches the Worker before a delayed Create calls
    `AddShmUnit`, each removal is an idempotent no-op and the later reference is outside client cleanup coverage.
    Worker hard reclaim is a fallback only for references marked reclaimable, currently routed Create allocations for
    clients without a SHM session on that target Worker; SHM-enabled client references continue to use reconciliation.
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
    set and requests a hash-ring refresh even when the UB/TCP fallback succeeds. The SHM connection and its session
    share a monotonic draining marker, so concurrent session invalidation cannot erase the voluntary reason; the
    transition schedules at most one `DisconnectClient`, and later requests return the exact draining status instead
    of attempting `GetSocketPath`/`RegisterClient` again. Each endpoint still caches only one data transporter. The
    foreground request replaces the terminal SHM transporter with UB, establishes UB on demand, and then executes the
    fallback operation; there is no background UB prewarm or SHM/UB dual slot. `enableLocalCache=true` retains its
    direct-client drain behavior. The endpoint entry also keeps a monotonic SHM-draining bit under its existing
    `bthread::RWLock`. Once an explicit draining response sets it, a request carrying a previously cached SHM hint is
    rejected before the single UB/TCP slot is reset; RPC/data-plane teardown retains this bit, while authoritative
    snapshot removal deletes the whole entry. Thus neither foreground fallback nor connection rebuild can resurrect
    SHM for the leaving endpoint. A refresh request is admitted at most once per published transport snapshot,
    preventing concurrent Gets from continually extending the forced-refresh window. A later snapshot rebuilds the
    candidate set.
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
  - Lower-version routing responses are accepted when two different Workers report the same version and sorted ACTIVE
    address digest within the current or previous refresh round. The ACTIVE set must be nonempty but need not overlap
    known members. This cross-confirmation also accepts two Workers with the same stale view; refresh still needs
    reachable known responders and does not discover arbitrary replacement addresses.
  - GetHashRing requests carry the hostId content digest of the last successfully applied snapshot alongside the
    topology version. Either changing returns a complete same-snapshot ring/hostId payload, including successful empty
    maps. Same-version hostId updates refresh same-node placement and Transport SHM candidates without epoch reset.
    Publication failures retain the previous digest for retry. Legacy requests without a digest retain version-only
    behavior; old Workers cannot provide hostId-only updates.
  - Metadata-owner failure hooks admit at most one force-refresh request per owner per six seconds. Different owners
    have independent quotas; when the tracking map exceeds 64 entries, expired entries are reclaimed. A refresher
    result of false means its global wakeup was coalesced, not that the request had no effect: it can still extend the
    global retry deadline. The owner quota therefore measures attempts rather than new wakeups.
  - A direct local-cache Get that observes `K_RPC_PEER_DEAD` submits one deduplicated switch task per concrete Worker API
    to the existing single-thread switch pool. Client shutdown first stops heartbeat producers, atomically closes that
    pool to new peer-dead submissions, and drains queued tasks before Worker APIs and pending-switch state are released.
  - Stream uses its own `client::stream_cache::StreamClientImpl`.
  - `ListenWorker` closes request admission before invoking recovery callbacks for a changed `worker_start_id` or a
    worker-reported missing client. Recovery callbacks return `Status`; successful heartbeats do not reopen admission
    while mandatory client resources are still pending. Object/KV recovery separates one-shot worker registration from
    retryable decrease-ref and pipeline SHM mmap rebuild, while Stream clears producer/consumer and mmap state before
    reconnecting. During recovery, new requests fail with `K_RPC_UNAVAILABLE`.
    If an idempotent Object Get reaches a Worker whose in-memory client registration was lost, the remote Object client
    re-registers the existing client ID during recovery, refreshes the client ID in the request, and retries that Get within
    the existing retry path; non-idempotent Create/Publish requests are not replayed by this branch.
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

| `IServiceDiscovery` / `ServiceDiscovery` / `CoordinatorServiceDiscovery` | `src/datasystem/client/service_discovery.cpp` | SDK worker selection for C++ callers using `ConnectOptions.serviceDiscovery`; both implementations accept an optional `clusterName`. ETCD discovery maps the logical membership table to the legacy physical prefix `/<clusterName>/datasystem/cluster` (or `/datasystem/cluster` when empty), while Coordinator discovery reads `/datasystem/<clusterName>/cluster` (or `/datasystem/cluster` when empty); `hostIdEnvName` is read from the process env first and then recovered from `<log_dir>/env`, and the resolved SDK host ID is exposed to the shared client backend for Routing and Transport locality. Public `CoordinatorServiceDiscovery::Init` initializes membership/random state and a temporary shared Coordinator proxy handle, publishing it only after the proxy's `Init` succeeds. The discovery object then retains that long-lived handle; repeated successful `Init` calls are idempotent, and all later worker snapshots reuse the same proxy, which routes each Coordinator RPC to the current leader across the asynchronously refreshed candidate snapshot. |
| `ICoordinatorDiscovery` | `include/datasystem/utils/coordinator_discovery.h` | Shared public candidate-provider contract used by SDK, Worker, and Coordinator startup. Proxy `Init` calls the provider once, requires a non-empty result, validates and deduplicates candidates via `HostPort::ParseString`, then constructs `CoordinatorLeaderRouter` over a `CoordinatorDiscoveryCache` snapshot. Router retries and per-attempt RPC timeouts consume the caller's one absolute deadline, and it divides the remaining budget across untried candidates so one unavailable endpoint cannot starve the rest. A retryable completed candidate round requests an asynchronous Discovery refresh; the RPC thread reads snapshots only and never calls Discovery. Transport-only candidate exhaustion returns the last transport status without starting another round. `SERVING` and recovery-control `RECOVERING` responses publish fenced Leader identity through `CoordinatorServiceProxyBase`; ordinary `RECOVERING` responses keep retrying that Leader until its state changes or the deadline expires. The fixed-address fallback uses internal `StaticCoordinatorDiscovery`. |
| `dscli query` | `cli/query.py` -> native query facade | Explicit backend, 5-second budget, local projection |

`WorkerLeaderReconciler` uses `CoordinatorLeaderRouter` directly for the current Leader identity and one Leader-change
handler. Clearing the Router handler synchronously excludes later callback access.

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
- Public SDK classes and transfer-engine bindings are loaded on first attribute access to keep TE-only imports
  isolated from `libbrpc`.
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

## Scale-Out Location-Scoped Data-Plane Admission

- Scope: phase-two replica reads in routed `KVClient::Get` when `enableLocalCache=false`. The SDK sends `QueryAndGet` to
  an already admitted Worker; that Worker resolves metadata locally or through its metadata redirect path and returns
  the authoritative data locations. Set, initial metadata-owner routing, and the local-cache path are unchanged.
- Reproduction window: `QueryAndGet` returns a new Worker's data location from topology version Vnew while the SDK's
  atomically published `WorkerSnapshot` is still Vold and does not contain that Worker. Ordinary data-plane admission
  rejects the location with `K_NOT_READY` before RPC-channel or URMA-transporter creation.
- Provenance contract: the Master that actually reads each object's metadata loads one immutable membership snapshot,
  verifies that every returned primary/selected data endpoint belongs to that snapshot, and places its version on the
  per-result `QueryMetaInfoPb`. The Query Worker only propagates that evidence through the returned location; metadata-
  owner route and redirect versions are not data-endpoint membership evidence. An unavailable snapshot, an endpoint
  absent from it, or a missing field from an older peer yields zero and cannot authorize an exception.
- Selected behavior: `ReplicaReader` supplies this version only to its location-directed data read. If the endpoint is
  absent from the SDK snapshot, `DataPlaneManager` may create or reuse it only when the location version is strictly
  newer than the snapshot ring version. It revalidates the exceptional case after endpoint construction: success is
  retained if the new snapshot contains the endpoint, but a snapshot that has caught up without it revokes the
  exception. The entry records the exceptional location version under its concurrent-map accessor, so an older pending
  reconcile cannot tear it down after construction; a caught-up snapshot that still omits the endpoint may delete it.
  When post-build revalidation rejects the exception, the manager conditionally detaches the same entry unless a newer
  location evidence version has superseded it. Detach does not actively close shared transport state, so concurrent
  holders keep their existing lifetime while the manager releases its rejected strong reference. Ordinary callers
  remain protected by normal admission.
- Safety and performance boundary: ordinary `GetOrCreate`, Set/Delete, metadata-owner routing, reconciliation, endpoint
  deletion, and `enableLocalCache=true` retain their existing snapshot admission. `PureQueryMeta` adds one process-local
  immutable snapshot load per request and at most two indexed membership lookups per returned object; there is no
  topology RPC, cluster scan, new global lock, fixed wait, endpoint prewarming, Coordinator grace, or per-request heap
  container. The SDK's already-admitted steady-state data-plane path is unchanged.
- Compatibility and rollout: the protobuf changes are additive. Upgrade Workers before SDKs when the scale-out
  guarantee is required: old SDKs ignore the field; new SDKs talking to old Workers receive zero and fail closed.
- Validation anchors: `OCMetadataManagerTopologyTest` covers exact Master membership evidence and fail-closed stale
  locations; `DataPlaneManagerTest` covers newer/old/equal versions, build-time snapshot advance, and rejected-entry
  detachment;
  `ObjectMetadataClientTest` covers field propagation; `ReplicaReaderTest` covers unary and shared-endpoint batch reads;
  `WorkerOcServiceImplTest` covers direct and redirected Master-evidence propagation. A local-only Coordinator-backed scale-out ST pins
  the reader to its old snapshot, writes data on the new Worker, and verifies non-inline `QueryAndGet` followed by the
  new Worker's data RPC; it takes more than 8 seconds and is intentionally not committed. A real URMA load run is still
  required before claiming the 5 ms SLO.
- Rollback: revert the additive field propagation and location-directed admission entry points. No persisted state or
  Coordinator protocol migration is involved.

## Review And Bugfix Notes

- Common change risks:
  - `ConnectOptions` can affect multiple language bindings and shared backend initialization at once; `serviceDiscovery` is intentionally typed as `std::shared_ptr<IServiceDiscovery>` so SDK clients do not depend on the ETCD implementation;
  - `ObjectClientImpl` is shared by both KV and Object API families, so “KV-only” changes may regress object behavior;
  - direct-read metadata and replica retries share the caller's API deadline. The metadata phase queries the selected
    metadata-owner Worker first. Metadata-owner connection, dispatch-deadline, peer-dead, client-disconnect, and
    owner-unavailable failures report the failed endpoint and return a stale topology/location signal immediately
    instead of retrying that fixed owner. The outer `ObjectClientImpl::GetFromTransportLayer` retry then forces the
    existing hash-ring refresher before it re-routes and re-queries metadata only for affected keys with deadline-bounded
    backoff. The data phase polls replicas from one fixed metadata location snapshot: stale-snapshot errors and
    draining errors try the remaining replicas first, and a data-replica `K_RPC_PEER_DEAD` is treated the same way. If
    another listed replica succeeds, the current Get returns that value without refreshing metadata. If all listed
    replicas are exhausted after a stale/draining/dead-replica observation, the reader returns the recorded stale
    topology/location signal so the outer Get refreshes the hash ring and re-queries authoritative metadata. The first
    stale-location retry runs immediately after requesting the refresh; only persistent stale results use the 20 ms
    exponential backoff. The stale signal is internal to the retry loop: if the caller deadline expires before the next
    retry, whether during the read
    RPC itself or during refresh backoff, the public SDK result is rewritten to `K_RPC_DEADLINE_EXCEEDED`; if the stale
    refresh budget is exhausted while the API deadline is still alive, the public SDK result is rewritten to
    `K_RPC_UNAVAILABLE`. Both final statuses append the original
    stale/dead-replica diagnostic string. Retry state is allocated only for affected keys; draining and stale-location
    budgets advance independently and never reset when the observed policy alternates. Concurrent force-refresh requests
    are coalesced by the refresher's atomic budget, and forced retries retain their 250 ms minimum interval instead of
    letting request traffic wake the refresh loop continuously. The transport round must still apply structured per-item
    results before deciding which keys are affected, so mixed batches do not turn object-level failures such as
    `K_NOT_FOUND` into stale-location retries.
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
  - direct-read endpoint entries use a TBB concurrent map, while each entry protects its shared RPC client and single
    SHM/UB/TCP transporter slot with a `bthread::RWLock`; different endpoints can initialize connections concurrently.
    A transport-kind change resets that one slot while retaining the shared RPC client. Consequently, voluntary
    ScaleIn fallback performs UB establishment in the foreground. Ordinary transport changes may later rebuild SHM on
    demand, but an entry that has observed an explicit ScaleIn draining response rejects every stale SHM hint until the
    endpoint is removed from the authoritative snapshot.
    Ordinary endpoint admission loads one snapshot containing the live set, ring version, provisional flag and last
    confirmed-refresh time. Provisional snapshots allow unknown endpoints. A confirmed snapshot rejects absent
    endpoints before cache lookup while healthy. After 60 seconds without a confirmed refresh, the first absent-endpoint
    request may open one 120-second grace window; expiry restores rejection until another confirmed refresh.
    A matching-version `changed=false` response renews health without reconciliation. Grace state belongs to its
    snapshot generation, and the exceptional admission rechecks publication after granting grace.
    A dedicated transport reconcile thread coalesces
    pending updates with latest-wins semantics, erases absent entries through TBB map accessors, and closes their data
    planes afterward. Shutdown stops and joins this reconcile thread before closing the manager.
    Worker incarnation changes that reuse the same endpoint are intentionally outside this mechanism. A metadata-only
    RPC to an owner explicitly returned by a server redirect triggers a forced ring refresh and clears any TCP/UB
    inline-data request before connecting. Routed Get replica reads have the separate, version-fenced data-location
    exception described in `Scale-Out Location-Scoped Data-Plane Admission`; every other data-plane creation remains
    snapshot-gated.
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
    Routed two-step `Create(Buffer) -> Set/Seal` is pinned to the Create worker until Publish. If that worker rejects
    Publish with the side-effect-free `K_SCALE_DOWN` admission result, the SDK excludes it and reuses the one-step
    Create-copy-Publish flow on a remaining route, preserving mode, nested keys, TTL, existence, and seal semantics.
    Other Publish failures remain subject to the ambiguity rule and are not replayed across workers. The old transport
    allocation follows the existing release contract: non-owner buffers are scheduled for release by `TransportLayer`,
    while zero-copy send owners remain attached to the caller's Buffer so replay does not invalidate its data pointer.
    After the first `K_SCALE_DOWN` replay, that Buffer records the source as draining and retains its source mapping only
    as staging memory. Later `MemoryCopy`, `Publish`, `Seal`, and repeated MSet calls skip source liveness and run the same
    complete transaction on a current route; they never rebuild the removed source endpoint or rebind the caller's
    pointer, latch, and release owner.
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
  - An absent worker rejected by transport snapshot admission returns `K_NOT_READY`, not object-level `K_NOT_FOUND`. Because
    no RPC was sent, routed Set and MSet may safely exclude that worker and rebuild the request on a current route.
    Routed direct Get uses the same narrow signal to recover from topology/metadata skew during scale or rolling
    changes; do not broaden this to all `K_NOT_READY` statuses, because SDK startup, shutdown, and non-snapshot
    readiness failures are different conditions.
    `K_RPC_UNAVAILABLE` both invalidates the cached channel and permits the existing bounded, non-SHM metadata-owner
    read retry. These decisions are independent: a retry reconnects rather than reusing the failed channel. Dispatched
    SHM queries remain non-replayable.
  - Transport MSet preserves worker-reported partial failures and performs at most one same-worker UB recovery attempt.
    Routed `MultiCreateReqPb` and `MultiPublishReqPb` requests carry `is_routed=true`; target workers authenticate their
    signatures and tenant IDs without requiring the client to register separately on every metadata-owner worker.
    MultiCreate carries one positional allocation UUID per key. A first-attempt reservation `K_TRY_AGAIN` waits 1 ms
    within the API deadline and retries once on the same transport with the same UUIDs. `K_RPC_UNAVAILABLE` rebuilds the
    transport once and replays the request with the same UUIDs, preventing a second allocation for a duplicate attempt;
    a terminal ambiguous result schedules batched background DecreaseReference retries for those UUIDs. An explicit
    Worker application error is not ambiguous and does not schedule cleanup. For MSet,
    `K_URMA_NEED_CONNECT` resets only the cached UB data plane and reuses the RPC client. A pre-Publish
    `K_RPC_UNAVAILABLE` may rebuild both RPC and data-plane state and retry once; after `InvokeMultiSet` starts, the same
    code is ambiguous and is not replayed. A dead UB connection is never converted into whole-batch TCP fallback. If the
    same-worker retry still returns `K_URMA_NEED_CONNECT`, `ObjectClientImpl` maps it to
    `SetFailureStage::TRANSFER`, allowing the routing layer to exclude that worker and reroute the group. Only
    per-object failures returned after `WritePayload` may use bounded TCP fallback: limiter admission sends that object
    as a TCP payload, while limiter rejection marks only that key failed and allows other objects to publish.
    `MultiPublishReqPb` has no retry marker, so ambiguous RPC failures are not replayed on the same or another worker.
    The narrow exception is `K_SCALE_DOWN`, which the target returns before entering `MultiPublish`; the SDK safely
    replays each prepared two-step buffer through a complete transaction on a remaining route and retains the existing
    partial-success result contract.
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
  - `tests/st/client/kv_cache/kv_client_create_timeout_cleanup_test.cpp` uses a 1 GiB Worker SHM arena.
    The client-cleanup case delays 130 Worker responses after `AddShmUnit`, expects all 8 MiB Sets to time out,
    and keeps hard reclaim outside
    the test window, and verifies Worker reference metrics converge before a final Set. A second case blocks all four
    cleanup slots, verifies overflow increments the dropped-cleanup metric, and uses test-only eligibility injection to
    prove Worker hard reclaim returns the reference table to zero before the final Set. A third case pauses Create
    before `AddShmUnit`, observes all three client cleanup calls while the allocation is absent, then releases Create
    after the approximately 500 ms cleanup window. It verifies the late reference first appears and is ultimately
    removed by Worker hard reclaim rather than by client cleanup.
  - A raw Client-to-Worker CQE status `9` quarantines only that routed Set/MSet write target. The current request keeps
    same-worker TCP fallback semantics; a failed request is rerouted only when Publish was not attempted or bRPC marks
    it definitely unsent. Get admission and the Client-local CQE-status-`4` sender circuit remain independent. The
    transport reconcile thread restores a quarantined target only after an exact Client-to-Worker UB WRITE probe and
    current topology-incarnation fencing succeed.
  - standby failover candidate order is randomized per switch attempt, so when one worker fails a batch of clients can spread across the remaining ready workers instead of stampeding to the first candidate in a shared list.
  - preferred same-node Worker replacement moves the retired listener and mmap manager out while holding the switch
    mutex, then lets them destruct after releasing the mutex so listener shutdown never joins while holding the switch
    mutex.
  - after a standby switch publishes the new current worker, cleanup of the previous worker's mmap fds captured at switch commit runs immediately when that worker API has no pending invocations; otherwise cleanup is deferred until its invocation count reaches zero. Cleanup removes only the captured fds, so mappings added for another worker before the deferred callback runs are preserved.
  - Python `DsTensorClient` depends on `HeteroClient`; tensor features are not an independent transport stack.
- Useful debug points:
  - `src/datasystem/client/object_cache/object_client_impl.cpp`
  - `src/datasystem/client/service_discovery.cpp`
  - `src/datasystem/pybind_api/pybind_register_*.cpp`

## Transfer Engine Registration Contract

- The legacy `RegisterMemory(address, length)` API remains available and means that
  the remotely authorized logical range and the backend-registered physical backing
  are identical.
- `MemoryRegistration` plus `RegisterMemoryEx`/`BatchRegisterMemoryEx` separates a
  logical range from its caller-owned backing. The logical range must be fully
  contained in the backing; overlapping logical ranges and non-identical overlapping
  backings are rejected before backend registration. Registration and unregistration
  batches are capped at 4096 logical ranges. Backing-range planning and batch-internal
  logical-range overlap validation use ordered indexes, avoiding quadratic work within
  one request; batch removal builds hash indexes instead of repeatedly scanning the
  request under the memory-table mutex.
- `TransferEngineState` reference-counts exact backing ranges. One backing is sent to
  the data-plane backend once even when multiple logical ranges share it, and it is
  unregistered only after the last logical reference is removed. Read leases continue
  to protect logical ranges, not the unexposed gaps in a backing.
- HIXL CS mode defaults to `on` and route policy defaults to `roce`, making CS Device RoCE the fail-closed default.
  Runtimes without the HIXL client-server capability fail initialization instead of falling back. Operators can set
  CS mode to `auto` explicitly to restore capability-driven legacy fallback, or `off` to require legacy; they can set
  the route to `auto` explicitly to restore vendor route matching.
  `auto` and `hccs` policies require a 2 MiB-aligned backing base before calling HIXL. Explicit `roce` preserves
  byte-addressed legacy registration. Registration length and transfer address/length remain byte-granular.
- The HIXL backend uses `9.1.0+` as its fully supported baseline. Detected `8.5.2` through `9.0.x` retains a
  warning-backed legacy compatibility path that requires `TRANSFER_ENGINE_HIXL_CS_MODE=off`; lower or unknown versions
  disable the backend. The default CS mode remains `on` and fails closed when `CLIENT_SERVER_COMM` is unavailable;
  `auto` enables CS when the capability is reported and otherwise falls back to legacy. In CS mode an
  explicit `TRANSFER_ENGINE_HIXL_ROUTE=roce` injects the
  `roce:device` protocol filter and does not require `HCCL_INTRA_ROCE_ENABLE`. Legacy explicit RoCE still requires the
  HCCL switch. Both peers exchange and validate effective engine mode and route before HIXL Connect.
- `TRANSFER_ENGINE_HIXL_LOCAL_COMM_RES` optionally supplies a validated HIXL 1.3 JSON object for deployments that need
  explicit `net_instance_id` and endpoint lists. The core `hixl::Hixl` Engine and its AutoConnect capability probe are
  available from HIXL `9.1.0`. `TRANSFER_ENGINE_HIXL_AUTO_CONNECT=auto|on|off` defaults to auto mode; `off` retains
  explicit vendor Connect as the rollback path. AutoConnect does not bypass TE authorization or generation checks.
- Receiver-driven READ retries one `kNotReady` or `kRuntimeError` failure after releasing the old lease, clearing the
  route and generation cache, and rebuilding the full connection/authorization chain. Other errors are not retried.
- Transfer-engine control frames are capped at 4 MiB, control strings at 64 KiB, one READ batch at 4096 items, and
  active leased ranges at 65536. Socket operations use bounded connect/read/write waits. Server shutdown cancels queued
  and active sockets instead of draining slow connections.
- Receiver-driven read leases use non-sequential bearer tokens bound to requester host/port/device. Finalize closes new lease
  admission and waits for owner-side leases before clearing registration or finalizing HIXL; a 30-second wait expiry
  returns `kNotReady` so callers can retry without freeing HBM early. HIXL initialization rejects a read-lease TTL that
  does not exceed the transfer timeout by at least one second.
- Connection readiness state is capped at 4096 entries and cleared across engine incarnations. The opt-in environment
  diagnostic logs a fixed TransferEngine configuration allowlist once per process, never the complete process environment.
- The Python facade accepts the Mooncake-compatible four-argument initialization
  form with empty metadata or `P2PHANDSHAKE`, while retaining the three-argument
  YuanRong overload. `location` and `transport_hint` are compatibility arguments;
  `transport_hint` does not select HCCS versus RoCE.
- The integrated DataSystem wheel lazily exports `TransferEngine`, `MemoryRegistration`, `Result`, and `ErrorCode`
  from its optional `_transfer_engine` extension without loading the main client native library during parent import.
- Registration rollback failure moves the backend to a fail-closed degraded state;
  subsequent operations require `Finalize` and reinitialization.
- Source-of-truth files:
  - `transfer_engine/include/datasystem/transfer_engine/transfer_engine.h`
  - `transfer_engine/src/transfer_engine.cpp`
  - `transfer_engine/src/internal/memory/registered_memory_table.*`
  - `transfer_engine/src/internal/backend/ascend/hixl_config.*`
  - `transfer_engine/src/internal/backend/ascend/ascend_backend.*`
  - `transfer_engine/src/python/py_transfer_engine.cpp`

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

## Read-path transport failure handling

- `K_URMA_NEED_CONNECT` (1006) means the worker no longer recognizes the client's UB connection
  (`UrmaManager::CheckUrmaConnectionStable`). It is returned by the metadata `QueryAndGet` **precheck**, which guards
  only UB data requests and runs before the Worker writes inline data.
- Retry handling per path:
  - metadata `QueryAndGet`: `ObjectMetadataClient::PrepareQueryRetry` calls
    `DataPlaneManager::RebuildStaleUbDataPlane` with the serving transporter as an identity guard, then retries once
    over UB within the original request deadline. The existing per-endpoint rebuild slot provides single-flight;
    the replacement handshake runs without the entry lock and a short write lock publishes the completed UB plane.
    Concurrent stale responders wait with deadline-aware backoff. The rebuild owner claims the atomic slot under the
  endpoint read lock; waiters observe the occupied slot without contending for the write lock, and the final lease
  checks the same state before dispatch. A failed rebuild publishes the cooldown and detaches the known-stale UB
  transporter under one endpoint write lock before releasing the slot, then closes it outside the lock. If teardown
  replaces the endpoint entry during the lock-free handshake, publication returns `K_TRY_AGAIN`; the caller reacquires
  the current admitted entry and RPC client as part of the same request and continues within the original deadline.
  An older in-flight rebuild clears the cooldown only if the observed generation is unchanged, so persistent peer
  rejection cannot cause one handshake per waiting request.
  - replica / direct read: `DataPlaneExecutor::PrepareRetry` calls `ResetDataPlane` and retries.
  - writes: `TransportLayer::RebuildPlaneOnSetFailure` calls `ResetDataPlane` and retries.
- **Invariant: the read-path recovery gates must stay opt-in.** `GetOrCreate`, `AcquireDataPlaneLease` and
  `EnsureTransporterLocked` are shared by Set, Create, direct H2D leases and replica reads, and those callers treat
  connection errors as terminal. `ObjectMetadataClient` opts UB leases into the endpoint cooldown and rebuild-slot
  checks; all other callers retain the default behaviour.
- The 1006 branch releases the prepared UB receive buffers before waiting for or performing the handshake, then
  recreates them after recovery: the precheck rejects the request before the worker writes anything, so there is
  nothing to quarantine. Delayed release and TCP fallback are not used for that recovery attempt.
  An undispatched transporter-replacement race before or after the Worker 1006 retries UB with deadline-aware backoff
  instead of entering the generic inline-to-TCP fallback or consuming the one Worker reconnect attempt; a second Worker
  response carrying 1006 is still returned.
- The worker-entry precheck resolves a missing client-id key through the peer address
  (`fast_transport_manager_wrapper.cpp` passes `remote.request_address()` as `fallbackAddress`): a handshake that
  registered before the client id was known lives under the address. The fallback is consulted **only** when the
  request key has no connection at all, so an address-keyed hit never masks a stale instance id. A connection reached
  through the fallback reports `K_URMA_NEED_CONNECT` (never `K_URMA_TRY_AGAIN`) when it is circuit-broken, because
  `K_URMA_TRY_AGAIN` is outside both the worker-side remap and the read path's self-heal and would hard-fail a read.

## Standby connection drain gating

- After the preferred same-node worker recovers, the client keeps the old standby control connection until its data
  plane has been idle for `FLAGS_standby_drain_data_plane_quiet_ms` (default 30000). The gate exists because the
  standby worker can still be the **meta owner** of objects: metadata-owner reads are routed to it through per-address
  stubs and never touch `workerApi_[REMOTE]`, so an idle control connection says nothing about the data plane. Tearing
  the connection down makes the worker delete this client's URMA connection in `RefreshMeta`, and the next routed read
  fails the entry precheck with `K_URMA_NEED_CONNECT` (1006).
- Predicate: `ListenWorker::CanDisconnectStandby()`. Voluntary scale-down drains immediately (legacy behaviour);
  otherwise the registered drain handle decides. `FLAGS_standby_drain_data_plane_quiet_ms = 0` restores the legacy
  "drain as soon as the control connection looks idle" behaviour. The teardown path calls the same predicate again
  (`ShutdownStandbyConnection`) instead of restating its conditions: the teardown runs on the shared async-switch
  thread and can start seconds after the decision, during which read traffic can resume on the endpoint.
- Ordering: the teardown resets the client's own data-plane entry **before** sending `Disconnect`, so the client never
  keeps a transporter for a connection the peer is about to drop. A new SDK can recover the resulting 1006 from an
  old or new Worker without a protocol extension; old SDK behaviour is unchanged.
- Last-use recording is sampled: `MarkDataPlaneUse` refreshes `lastDataPlaneUseMs` at most once per
  `DATA_PLANE_USE_REFRESH_INTERVAL_MS` (100 ms) so the read hot path does not dirty the entry on every request, and it
  records the transporter kind that **actually served** the request, so a UB candidate served by the cached TCP
  fallback does not hold the drain back. `IsEndpointDataPlaneQuiet` discounts the sampling bound, so the effective wait
  is the configured window plus up to one interval and never shorter than it. The flag is a dynamic uint32 with no
  non-zero lower bound, so the compensation is unconditional.

## Read-path UB rebuild coordination

- `DataPlaneManager::AdmitUbRead` evaluates two read-path-private gates from a single entry lookup: the rebuild
  cooldown (`FLAGS_ub_rebuild_cooldown_ms`, default 1000; armed by any read-path UB handshake failure, not only 1006)
  and a per-endpoint rebuild slot. It returns `PROCEED` (a usable data plane), `REBUILD` (this request owns the slot and
  must release it through `DataPlaneManager::UbRebuildSlotGuard`), or `DEGRADE` (serve this request over TCP inline
  instead of queueing).
- The slot exists because a wave of readers can all pass the cooldown before the first handshake fails and arms it:
  without coordination every queued reader pays for its own doomed handshake. Exactly one request rebuilds; waiters
  never block and no lock is held across the RPC. Both gates stay private to the read path — writers, direct H2D
  leases and replica reads keep their `GetOrCreate` behaviour.
- A `REBUILD` verdict carries a release obligation: the caller must destroy its `DataPlaneManager::UbRebuildSlotGuard`
  when the handshake attempt ends. A slot that is never released leaves `ubRebuildSlotOwner` non-zero and silently
  degrades every later reader of that endpoint to TCP until the entry is reconciled or torn down. The guard is nested
  in `DataPlaneManager` and declared in `data_plane_manager.h` next to `AdmitUbRead` for that reason.
- Resuming UB after the cooldown elapses is part of the contract: the read path must not give UB up permanently.
- A 1006 returned by an established metadata request bypasses this initial-handshake cooldown: the existing endpoint
  rebuild slot elects one handshake outside the entry lock. Slot acquisition uses the endpoint read lock and an atomic
  compare-exchange; only final publication takes the write lock. Waiters therefore avoid write-lock contention while
  the owner is handshaking, observe the completed replacement before competing again, and the original request retries
  UB once.

## Open Questions

- Should service discovery be documented as a C++-only advanced entrypoint for now, since Python constructors do not currently expose it directly?
- Should `DsTensorClient` live in this module document permanently, or split into a future hetero/transfer-engine focused document once that area is deepened?

## Coordinator Set admission redirects

`ObjectClientImpl::ExecuteSetFlow` consumes typed pre-execution `WorkerRedirectPb` from Create / Publish. Each rejection carries at most three request-key-rotated candidates. Routing validates those candidates directly against its immutable ring, placement policy, health filters, and hard exclusions; it does not build an exclusion list for every other Worker. The SDK retains the request deadline and immediately isolates a rejecting Worker through the existing routing filter until the next ring update. Only typed Coordinator admission rejections expand the distinct-address budget; ordinary errors keep the existing bound. `ClientWorkerRemoteApi::Publish` preserves ambiguous earlier RPC failures so a later redirect cannot authorize unsafe replay. Transport UT covers candidate preference, cross-request isolation, retry budgets, and ambiguous Publish results.
