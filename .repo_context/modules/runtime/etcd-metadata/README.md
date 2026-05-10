# ETCD Metadata

## Scope

- Path(s):
  - `third_party/protos/etcd`
  - `src/datasystem/common/kvstore/etcd`
  - `src/datasystem/common/kvstore/metastore`
  - integration from `src/datasystem/worker/cluster_manager` and `src/datasystem/worker/hash_ring`
- Why this module exists:
  - provide the ETCD v3 API surface used by the worker runtime;
  - wrap ETCD KV, watch, lease keepalive, transaction/CAS, health, authentication, and election RPCs behind project-local APIs;
  - provide an in-process ETCD-compatible Metastore server for deployments using `metastore_address`;
  - host the canonical cluster-membership and hash-ring metadata backend used by cluster management.
- Primary source files to verify against:
  - `third_party/protos/etcd/api/etcdserverpb/rpc.proto`
  - `third_party/protos/etcd/api/mvccpb/kv.proto`
  - `third_party/protos/etcd/v3election.proto`
  - `third_party/protos/etcd/v3lock.proto`
  - `src/datasystem/common/kvstore/etcd/etcd_store.cpp`
  - `src/datasystem/common/kvstore/etcd/etcd_watch.cpp`
  - `src/datasystem/common/kvstore/etcd/etcd_keep_alive.cpp`
  - `src/datasystem/common/kvstore/etcd/grpc_session.h`
  - `src/datasystem/common/kvstore/etcd/etcd_constants.h`
  - `src/datasystem/common/kvstore/metastore/*`

## Responsibilities

- Verified:
  - vendors ETCD v3.5.0 protobuf/gRPC API definitions under `third_party/protos/etcd`;
  - builds C++ ETCD proto/grpc libraries through Bazel targets and the CMake `etcdapi_proto` shared library;
  - maps project table names to ETCD key prefixes through `EtcdStore::CreateTable`;
  - performs normal KV operations: `Put`, `PutWithLeaseId`, `Get`, `RawGet`, `GetAll`, `PrefixSearch`, `RangeSearch`, `Delete`, and `BatchPut`;
  - implements CAS through ETCD transaction compare-and-put semantics in `EtcdStore::CAS` and `Transaction`;
  - owns lease-backed worker liveness writes through `EtcdStore::InitKeepAlive`, `RunKeepAliveTask`, `UpdateNodeState`, and `InformEtcdReconciliationDone`;
  - owns ETCD watch streams and local event compensation through `EtcdWatch`;
  - supports ETCD auth token acquisition/refresh and TLS channel creation;
  - exposes ETCD election helpers through `EtcdElector`;
  - implements a built-in ETCD-compatible Metastore server for KV, Lease, Watch, and Maintenance services.
- Pending verification:
  - all client/router-only uses of `common_etcd_client`;
  - production deployment guidance for choosing external ETCD vs built-in Metastore;
  - whether `v3lock.proto` has any runtime caller; no local wrapper was verified in the ETCD client path.

## Companion Docs

- Matching metadata JSON:
  - `.repo_context/modules/metadata/runtime.etcd-metadata.json`
- Matching `design.md`:
  - `.repo_context/modules/runtime/etcd-metadata/design.md`
- Cross-module DFX matrix:
  - `.repo_context/modules/runtime/cluster-management-dfx-matrix.md`
- Matching feature playbook:
  - `.repo_context/playbooks/features/runtime/etcd-metadata/implementation.md`
- Reason if either is intentionally omitted:
  - not omitted; this module is availability-sensitive and cross-module.

## Module Boundary Assessment

- Canonical module boundary:
  - external ETCD protocol definitions in `third_party/protos/etcd`;
  - project ETCD client wrappers in `src/datasystem/common/kvstore/etcd`;
  - ETCD-compatible in-process backend in `src/datasystem/common/kvstore/metastore`.
- Candidate sibling submodules considered:
  - `hash_ring` remains a sibling runtime module; it stores one hot serialized `HashRingPb` through this module but owns ring semantics.
  - `cluster_manager` remains a sibling runtime module; it owns worker lifecycle state and event interpretation.
  - object metadata tables under ETCD are runtime consumers, not owners of the ETCD client.
- Why Metastore is included here:
  - it registers the same ETCD gRPC services used by `EtcdStore` and is selected by `metastore_address` as an ETCD-compatible backend.
  - it is not just a storage library; lease expiration, watch events, transactions, and maintenance status all emulate ETCD semantics for local runtime use.

## Key Entry Points

- Proto services and message types:
  - `etcdserverpb.KV`: `Range`, `Put`, `DeleteRange`, `Txn`, `Compact`
  - `etcdserverpb.Watch`: streaming watch create/cancel/progress requests
  - `etcdserverpb.Lease`: `LeaseGrant`, `LeaseRevoke`, streaming `LeaseKeepAlive`, `LeaseTimeToLive`, `LeaseLeases`
  - `etcdserverpb.Maintenance`: `Status` and maintenance RPCs used by health checks
  - `v3electionpb.Election`: `Campaign`, `Leader`, `Observe`, `Resign`
  - `mvccpb.KeyValue` and `mvccpb.Event`: persistent key-value fields and PUT/DELETE watch events
- C++ runtime entrypoints:
  - `EtcdStore::CreateTable`
  - `EtcdStore::Put`, `PutWithLeaseId`, `BatchPut`
  - `EtcdStore::Get`, `RawGet`, `GetAll`, `PrefixSearch`, `RangeSearch`
  - `EtcdStore::Delete`
  - `EtcdStore::CAS`
  - `EtcdStore::InitKeepAlive`, `UpdateNodeState`, `InformEtcdReconciliationDone`
  - `EtcdStore::WatchEvents`, `InitWatch`, `WatchRun`, `ReInitWatch`
  - `EtcdStore::Writable`
  - `EtcdStore::Authenticate`
  - `EtcdHealth::CheckHealth`
  - `EtcdElector::Campaign`, `Leader`, `Observe`, `Resign`, `LeaseKeepAlive`
  - `MetaStoreServer::Start`, `Stop`
- Important constants:
  - `ETCD_RING_PREFIX`: `/datasystem/ring`
  - `ETCD_CLUSTER_TABLE`: `datasystem/cluster` without a leading slash because prefix is customized by cluster name
  - `ETCD_MASTER_ADDRESS_TABLE`: `/datasystem`
  - `ETCD_META_TABLE_PREFIX`, `ETCD_LOCATION_TABLE_PREFIX`, `ETCD_ASYNC_WORKER_OP_TABLE_PREFIX`, `ETCD_GLOBAL_CACHE_TABLE_PREFIX`
  - worker states in ETCD values: `ready`, `exiting`, `d_rst`

## Main Dependencies

- Upstream callers:
  - `EtcdClusterManager` creates tables, starts watches, initializes keepalive, consumes worker events, and calls node state updates.
  - `HashRing` reads/writes `/datasystem/ring` through CAS and responds to ring watch events.
  - object-cache metadata modules store metadata/location/global-cache records under ETCD metadata prefixes.
  - CLI and tests use ETCD store helpers for inspection and fault scenarios.
- Downstream dependencies:
  - external ETCD server when `etcd_address` is configured.
  - built-in Metastore server when `metastore_address` is configured.
  - gRPC, protobuf, access recorder, metrics vector, thread pools, timers, signal handling, secret manager, and TLS helpers.
- External contracts:
  - ETCD revision/version semantics drive CAS and watch filtering.
  - Lease expiration drives worker liveness deletion.
  - Watch delivery is treated as eventually reliable only with local compensation enabled by this module.

## Main Flows

### Proto And Build Surface

1. ETCD proto files are copied from ETCD release v3.5.0 under `third_party/protos/etcd`.
2. Bazel exposes `rpc_cc_proto`, `rpc_cc_grpc`, `kv_cc_proto`, and `v3election_cc_grpc`.
3. CMake builds `etcdapi_proto` from ETCD proto/grpc sources and links `common_etcd` / `common_etcd_client`.
4. Runtime code includes generated headers such as `etcd/api/etcdserverpb/rpc.grpc.pb.h`.

Important nuance:

- `v3lock.proto` is present, but the verified local wrapper covers election and does not expose a lock API in `src/datasystem/common/kvstore/etcd`.

### Table Prefixing And KV Access

1. `EtcdStore::CreateTable(tableName, tablePrefix)` records an in-memory mapping from table name to ETCD prefix.
2. When `cluster_name` is configured, normal table prefixes become `/<cluster_name><tablePrefix>`.
3. `ETCD_CLUSTER_TABLE` is special: it intentionally has no leading slash and relies on customized prefixing.
4. `Put`, `Get`, `Delete`, `PrefixSearch`, and `RangeSearch` compose `realKey = tablePrefix + "/" + key`.
5. `GetAll` and prefix/range queries strip table prefixes before returning caller-facing keys.

Important nuance:

- Table existence is checked only in the local `tableMap_`; creating a table does not create an ETCD namespace object.
- Other-AZ table prefixes are stored separately in `otherAzTableMap_` and used by multi-AZ watch and read paths.

### Transaction And CAS

1. `EtcdStore::CAS(table, key, processFunc, res)` resolves the real ETCD key and loops with randomized sleep.
2. It reads the current value and version using `Get`; missing key is treated as version `0`.
3. The caller-supplied process function returns a new value or `nullptr` when no write is needed.
4. `DoTransaction` creates a `Transaction`, compares the key version, and writes the new value through ETCD `Txn`.
5. `Transaction::Commit` returns `K_TRY_AGAIN` when ETCD comparison fails.

Important nuance:

- The comment says transaction comparison failure keeps retrying until success, but the current implementation increments `errorRetryNum` for every `DoTransaction` error, including `K_TRY_AGAIN`. High conflict can therefore exhaust `CAS_ERROR_MAX_RETRY_NUM` instead of retrying forever.
- `Transaction` uses a static shared KV `GrpcSession` initialized with `GetBackendAddress()`; backend address selection is global to the process.

### Lease Keepalive And Worker Liveness

1. `EtcdStore::InitKeepAlive(table, key, isRestart, isEtcdAvailableWhenStart)` prepares a `KeepAliveValue` with timestamp, state, and optional host id.
2. Initial states are `start`, `restart`, or `d_rst`; after the first successful write, later automatic writes use `recover`.
3. `RunKeepAliveTask` creates an ETCD lease with TTL `node_timeout_s`, creates `EtcdKeepAlive`, writes the cluster table key with that lease, then runs the keepalive loop.
4. `EtcdKeepAlive::Run` periodically sends `LeaseKeepAlive` requests after `LivenessHealthCheckEvent` notifications.
5. If keepalive fails longer than the lease-expiry threshold, the parent monitor either retries, emits a fake DELETE event, starts a death timer, or kills the worker when `auto_del_dead_node` and `node_dead_timeout_s` policy require it.
6. `UpdateNodeState` rewrites the local cluster-table value with the active lease.
7. `InformEtcdReconciliationDone` turns `restart` or `recover` into `ready` after reconciliation.

Important nuance:

- Writes to `ETCD_CLUSTER_TABLE` must be bound to a lease. `GrpcSession::SendRpc` explicitly rejects cluster-table `Put` requests whose lease is `0`.
- Keepalive failure can synthesize a local DELETE event even before a real ETCD watch event arrives, so cluster-manager deletion logic must treat event source carefully.

### Watch And Event Compensation

1. `EtcdStore::WatchEvents` converts table/key pairs to ETCD prefixes and starts `EtcdWatch`.
2. `EtcdWatch::CreateWatch` watches each prefix with `range_end = prefix + "\xFF"` and `start_revision + 1`.
3. The producer reads the ETCD watch stream and queues events.
4. The consumer filters stale events using local `keyVersion_`, runs the RocksDB cluster-info update callback, then calls the runtime event handler.
5. DELETE events under `ETCD_CLUSTER_TABLE` are ignored if the backend is not writable.
6. When the watch stream fails, `ReInitWatch` first calls `RetrieveEventActively`, then reopens the stream.
7. A passive compensation thread periodically prefix-searches ETCD and generates fake PUT/DELETE events for missed state.

Important nuance:

- Watch is not a pure subscription layer. It actively compares ETCD state against local `keyVersion_` and can synthesize events.
- Fake PUT events may be delayed when the observed version suggests an intermediate event may still arrive.

### Authentication, TLS, And Session Behavior

1. `GrpcSession::CreateSession` creates insecure channels by default.
2. If explicit client TLS material is supplied, it builds a TLS session from those values.
3. If `enable_etcd_auth=true`, it loads ETCD TLS material through `SecretManager` and uses `etcd_target_name_override` when configured.
4. `EtcdStore::Authenticate` sends ETCD `Authenticate`, stores the token, and starts `TokenRefreshLoop`.
5. RPCs add the auth token as request metadata named `token`.
6. `GrpcSession::SendRpc` records access metrics, applies deadlines/retry-on-unavailable behavior, and can cancel in-flight contexts on shutdown.

### Built-In Metastore Backend

1. `MetaStoreServer::Start(address)` registers KV, Lease, Watch, and Maintenance services on an insecure gRPC server.
2. `KVManager` stores keys in memory, increments a revision on writes, records a small per-key history ring, and supports range queries and transactions.
3. `LeaseManager` creates leases, tracks attached keys, and deletes keys when leases expire.
4. `WatchManager` registers watchers, sends historical events from `KVManager::RangeHistory`, and dispatches new put/delete notifications.
5. `MaintenanceServiceImpl` returns status using the current in-memory revision.

Important nuance:

- Metastore history keeps only `MAX_HISTORY_ENTRIES=5` per key/deleted key. It is not a persistent ETCD replacement unless surrounding deployment code provides durability elsewhere.

## Build And Test

- Build commands:
  - CMake: `common_etcd`, `common_etcd_client`, and `common_metastore` are pulled by higher-level build targets.
  - Bazel: `//src/datasystem/common/kvstore/etcd:etcd_store`
  - Bazel proto targets: `//third_party/protos/etcd:rpc_cc_grpc`, `//third_party/protos/etcd:rpc_cc_proto`, `//third_party/protos/etcd:v3election_cc_grpc`
- Representative tests:
  - `tests/st/common/kvstore/etcd_store_test.cpp`
  - `tests/st/common/kvstore/grpc_session_test.cpp`
  - `tests/ut/common/kvstore/metastore_server_test.cpp`
  - `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`
  - `tests/st/worker/object_cache/etcd_cluster_manager_test.cpp`

## Review And Bugfix Notes

- Common change risks:
  - changing ETCD table prefixes changes persisted key layout and watch routing;
  - changing CAS retry behavior directly affects hash-ring and cluster-manager scale operations;
  - changing keepalive state strings affects restart, reconciliation, voluntary exit, and passive deletion;
  - changing watch filtering or compensation can duplicate, suppress, or reorder cluster events;
  - changing Metastore transaction or history behavior can diverge from external ETCD behavior.
- Important invariants:
  - cluster-table worker keys must be lease-bound;
  - CAS compares ETCD key version unless the alternate value-based overload is used;
  - watch local `keyVersion_` must advance only after event handlers process the event;
  - DELETE cluster events are actionable only when the backend is writable;
  - `GetBackendAddress()` favors `metastore_address` when present.
- Observability or debugging hooks:
  - access recorder keys for ETCD Put/Get/GetAll/PrefixSearch/Delete/LeaseGrant/Authenticate;
  - `EtcdKeepAlive` and `EtcdWatch` logs;
  - inject points beginning with `EtcdWatch.*`, `EtcdStore.*`, `etcd.*`, `grpc_session.*`, and `worker.RunKeepAliveTask`.

## Current Design Weaknesses

- The ETCD wrapper is both a generic KV client and a runtime membership/watch/keepalive policy engine. This makes the module boundary blurry and pushes cluster-specific behavior into a common library.
- CAS is implemented as read-process-transaction over full values. Hot keys such as `/datasystem/ring` can create heavy ETCD load and high conflict rates during cluster scale or failure storms.
- CAS conflict handling is bounded by a generic error retry counter, so large writer fanout can surface as operation failure rather than backpressure-aware convergence.
- Watch compensation improves resilience but creates two event sources: ETCD stream events and synthesized fake events. This increases duplicate/order reasoning cost for cluster-manager and hash-ring handlers.
- Keepalive failure handling mixes transport diagnosis, local fake DELETE generation, retry, death timers, and `SIGKILL`; this couples ETCD network behavior to worker lifecycle policy.
- In-memory Metastore shares ETCD-compatible APIs but has limited history and no verified persistent durability in this layer, so behavior can differ from an external ETCD cluster under restart, compaction, or long watch gaps.
- Table prefix mappings live only in `EtcdStore` memory while data lives in ETCD/Metastore, so callers must recreate identical table mappings before accessing data.
- ETCD auth/TLS, router-client TLS, normal worker sessions, transaction static sessions, watch sessions, and keepalive sessions have different creation paths, which raises the risk of inconsistent backend or security behavior.

## Open Questions

- Whether `v3lock.proto` is intentionally vendored for future use or has callers outside the verified runtime path.
- Whether `Transaction` should own a non-static session per store/backend to avoid global lifecycle coupling.
- Whether hot runtime state should be split across multiple ETCD keys with narrower CAS domains.
- Whether Metastore should be documented and tested as a separate deployment mode with explicit durability and scale limits.
