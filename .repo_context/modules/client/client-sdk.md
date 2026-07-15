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
  - `client::TransportLayer` provides worker-address-based `Get` plus transport-native `Create`/`Set` primitives. Its
    TCP Set path publishes an RPC payload, while its UB Set path writes the payload through URMA and publishes an empty
    payload, with bounded TCP fallback on UB write failure. `ObjectClientImpl::Put` uses these primitives for the
    non-SHM routed Set path and keeps one worker address fixed across Create, payload transfer, and Publish.
  - `client::TransportLayer` also provides internal same-worker `MCreate`/`MSet` primitives. TCP MCreate allocates local
    buffers and MSet sends one positional MultiPublish payload; UB MCreate uses one MultiCreate RPC, MSet pipelines
    non-blocking per-object URMA writes in bounded groups, and failed writes use bounded TCP payload fallback in the
    same MultiPublish RPC. With local
    cache disabled, public key/value `ObjectClientImpl::MSet` groups keys by metadata owner and sends each same-worker
    group through these primitives; with local cache enabled it preserves the legacy client-worker batch path.
  - With `enableLocalCache=false`, `ObjectClientImpl` initializes `client::Routing`; Set selects the key's metadata
    owner and key/value MSet groups keys by metadata owner with `HASH_RING_AFFINITY`. Unavailable workers are excluded
    during bounded pre-Publish retries. With local cache enabled, both APIs preserve the legacy current-worker path and
    do not initialize the direct transport runtime.
  - Routed transport requests carry the gateway client id, token snapshot, thread tenant context, and shared transport
    `Signature`. Target workers authenticate routed Create, Publish, and cleanup requests by signature without requiring
    endpoint-local client registration. UB allocations are released asynchronously after the final Publish attempt, or
    synchronously after a local copy failure; shutdown drains the release queue before closing data-plane connections.
  - Public `ObjectBuffer` keeps transport-owned state opaque and exposes a status-returning `Create` factory; callers
    must pass state whose dynamic type is `ObjectBufferInfo`. Source-tree transport code uses
    `src/datasystem/client/transport/object_buffer_internal.h` for typed access, preventing installed SDK headers from
    depending on client transport or common object-cache implementation headers.
  - `ObjectClientImpl` owns the SDK routing lifecycle: after the initial worker is ready it creates `Routing`, performs a
    version-0 `GetHashRing` fetch, starts periodic versioned refresh, and stops routing before its transport resources.
  - Routing refresh always uses the transport layer's cached endpoint RPC client. It does not add HashRing methods to
    the legacy local/remote worker API or embedded C wrapper, and the GetHashRing control request carries AK/SK fields.
    Embedded configuration and `UpdateAkSk` keep the legacy and transport signature holders synchronized.
  - Stream uses its own `client::stream_cache::StreamClientImpl`.
  - Python bindings are not a separate reimplementation; they bind to C++ classes and helper types through `libds_client_py`.
  - Python package `yr.datasystem` lazily exposes public SDK symbols, `DsTensorClient`, and optional transfer-engine
    bindings so importing `TransferEngine` alone does not eagerly load `libds_client_py` or its `libbrpc` dependency.
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
| `IServiceDiscovery` / `ServiceDiscovery` / `CoordinatorServiceDiscovery` | `src/datasystem/client/service_discovery.cpp` | SDK worker selection for C++ callers using `ConnectOptions.serviceDiscovery`; both implementations accept an optional `clusterName` and read `/datasystem/{clusterName}/cluster`, while an empty name reads `/datasystem/cluster`; `hostIdEnvName` is read from the process env first and then recovered from `<log_dir>/env` |

## Connection And Auth Model

- Verified in `ConnectOptions`:
  - direct worker address via `host` + `port`
  - connection and request timeout controls
  - token auth, curve key fields, AK/SK fields, tenant id
  - cross-node and exclusive connection toggles
  - local-cache routing toggle; `enableLocalCache=false` routes Set to the key's metadata owner and supports single-
    and multi-key full-object `Get`, with per-key partial results and without L2 loading or RH2D
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
- `DsTensorClient` is a Python-side convenience layer built on top of `HeteroClient`, tensor pointer extraction, and page-attention helpers.

### Verified Python/C++ differences to remember

- Python `Context` currently exposes `set_trace_id`, but not `SetTenantId`, even though C++ `Context` has both APIs.
- Python `KVClient` is backed by a pybind class named `KVClient` whose underlying C++ object is `ObjectClientImpl`.
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
  - direct-read metadata and replica retries share the caller's API deadline; the data phase reuses the fixed location
    snapshot and does not query metadata again between replica rounds;
  - Python-facing behavior can differ from C++ because pybind wrappers convert statuses into exceptions and sometimes rename methods;
  - context propagation changes can affect tracing and multi-tenant behavior across all client operations.
- Important invariants:
  - `DsClient` init order is KV -> Hetero -> Object; shutdown order is Object -> Hetero -> KV.
  - worker connectivity and auth material may come from explicit options or environment fallback in shared client backend code.
  - direct-read mode does not dynamically update AK/SK and does not load missing objects from L2; callers must recreate
    the client to change credentials for that mode.
  - direct-read endpoint entries use a TBB concurrent map under a lifecycle shared mutex, while each entry has its own
    mutex; different endpoints can initialize connections concurrently and the same endpoint is initialized once.
  - transport RPC clients share a transport-owned `Signature` instance and sign each fully populated request immediately
    before sending it.
  - Set retries rebuild RPC or UB state once on the same worker inside `TransportLayer`. Cross-worker retry starts a
    new Create transaction, excludes `K_SCALE_DOWN` workers without poisoning their global health, and reports
    connection failures through `Routing::UpdateState`. A Publish connection failure is treated as ambiguous and is
    never replayed on another worker.
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
  - transport-layer Get keeps flow-stage summaries plus endpoint, retry, redirect, and replica details in
    `[TransportGet]` debug logs. Like the gateway Get path, it does not remap existing status codes; when every key
    fails, it returns the first failure in input order without logging that user-visible error again. Transport-specific
    route, location, missing-result, and data-response errors use concise messages. Partial success still returns
    `K_OK`, and debug-only diagnostics avoid hot-path log volume.
  - `QueryAndGet` returns at most five copy locations per object. The primary address from object metadata is returned
    first, followed by non-primary locations, so replica retry always starts with the primary copy.
  - `tests/st/client/kv_cache/kv_client_transport_get_test.cpp` covers single-key and same-owner multi-key transport
    reads. It disables the local cache, applies the same deterministic hash rule in the SDK and worker processes, and
    resolves the metadata owner through the real SDK `Routing` path before asserting TCP or UB data transport.
  - `tests/st/client/kv_cache/kv_client_transport_set_test.cpp` covers the routed Set transaction over TCP or UB. It
    verifies successful data and metadata publication, complete transaction rerouting after a Publish-time scale-down
    response, and the rule that an ambiguous Publish connection failure is not replayed on another worker.
  - standby failover candidate order is randomized per switch attempt, so when one worker fails a batch of clients can spread across the remaining ready workers instead of stampeding to the first candidate in a shared list.
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
