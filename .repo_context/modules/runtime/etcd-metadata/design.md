# ETCD Metadata Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - current implementation
- Primary code paths:
  - `third_party/protos/etcd`
  - `src/datasystem/common/kvstore/etcd`
  - `src/datasystem/common/kvstore/metastore`
- Primary source-of-truth files:
  - `third_party/protos/etcd/api/etcdserverpb/rpc.proto`
  - `third_party/protos/etcd/api/mvccpb/kv.proto`
  - `src/datasystem/common/kvstore/etcd/etcd_store.cpp`
  - `src/datasystem/common/kvstore/etcd/etcd_watch.cpp`
  - `src/datasystem/common/kvstore/etcd/etcd_keep_alive.cpp`
  - `src/datasystem/common/kvstore/etcd/grpc_session.h`
  - `src/datasystem/common/kvstore/metastore/metastore_server.cpp`
  - `src/datasystem/common/kvstore/metastore/manager/kv_manager.cpp`
- Last verified against source:
  - `2026-05-08`
- Related context docs:
  - `.repo_context/modules/runtime/etcd-metadata/README.md`
  - `.repo_context/modules/runtime/hash-ring/README.md`
  - `.repo_context/modules/runtime/cluster-management.md`
- Related playbooks:
  - `.repo_context/playbooks/features/runtime/etcd-metadata/implementation.md`

## Purpose

- Why this design document exists:
  - capture the ETCD-compatible storage layer that cluster-manager and hash-ring depend on for membership, routing state, watch events, and failure detection.
- What problem this module solves:
  - stable project-local access to ETCD v3 APIs plus an optional in-process compatible backend.
- Who or what depends on this module:
  - `EtcdClusterManager`, `HashRing`, object-cache metadata managers, stream-cache metadata managers, replica/slot recovery paths, CLI tools, and ETCD failure tests.

## Goals

- Provide ETCD v3 protobuf/gRPC definitions to C++ runtime code.
- Hide direct ETCD RPC details behind `EtcdStore` and `GrpcSession`.
- Support cluster metadata writes, reads, range scans, CAS, watches, leases, and node liveness.
- Provide watch compensation so cluster state can converge after stream interruption.
- Provide an ETCD-compatible in-process Metastore mode.
- Support ETCD authentication and TLS when configured.

## Non-Goals

- It does not define the hash-ring state machine or worker lifecycle policy.
- It does not shard or redesign higher-level cluster metadata.
- It does not make Metastore a full persistent ETCD replacement in this layer.
- It does not expose every vendored ETCD service through local wrappers; for example, no local lock wrapper was verified.

## Architecture Overview

- High-level structure:
  - `third_party/protos/etcd` defines imported ETCD wire contracts.
  - `GrpcSession<T>` creates channels, applies auth metadata, sends synchronous or async RPCs, records access metrics, and cancels outstanding calls on shutdown.
  - `EtcdStore` implements the `KvStore` interface plus runtime-specific lease, watch, CAS, health, and auth helpers.
  - `EtcdWatch` manages watch stream lifecycle and missed-event compensation.
  - `EtcdKeepAlive` manages streaming `LeaseKeepAlive` for a single lease.
  - `EtcdElector` wraps ETCD election APIs.
  - `EtcdHealth` wraps Maintenance `Status`.
  - `MetaStoreServer` serves KV/Lease/Watch/Maintenance using in-memory managers.
- Key persistent or backend state:
  - ETCD key prefixes for cluster table, ring, master address, metadata tables, replica group, and slot recovery.
- Key in-memory state:
  - `EtcdStore::tableMap_`, `otherAzTableMap_`, auth token, lease id, keepalive value, timers, watch instance;
  - `EtcdWatch::keyVersion_`, prefix map, event queues;
  - Metastore `KVManager::data_`, revisions, lease maps, watch registrations, and limited history.

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| ETCD protos | vendored ETCD v3.5.0 API surface | `third_party/protos/etcd` | persistent wire contract |
| `GrpcSession<T>` | channel/session/RPC wrapper | `grpc_session.h` | TLS/auth/deadline/metrics/cancel |
| `EtcdStore` | project KV store and runtime ETCD policy | `etcd_store.*` | table mapping, CAS, watch, keepalive |
| `Transaction` | ETCD Txn helper | `etcd_store.cpp` | static KV session, compare/put/delete |
| `EtcdWatch` | watch stream and event compensation | `etcd_watch.*` | produces real and fake events |
| `EtcdKeepAlive` | streaming lease keepalive | `etcd_keep_alive.*` | lease renewal timing and timeout |
| `EtcdElector` | election wrapper | `etcd_elector.*` | campaign/leader/observe/resign |
| `EtcdHealth` | maintenance status wrapper | `etcd_health.*` | backend writability probe input |
| `MetaStoreServer` | built-in ETCD-compatible server | `metastore/*` | in-memory KV/lease/watch/maintenance |

## Data And State Model

- Table model:
  - callers create local table mappings; all operations then compose table prefix and caller key.
  - table names are project-local abstractions, not ETCD-created namespaces.
- Key-value model:
  - ETCD `KeyValue` supplies `key`, `value`, `create_revision`, `mod_revision`, `version`, and `lease`.
  - `RangeSearchResult` copies fields used by project code.
- Lease model:
  - worker membership rows in `ETCD_CLUSTER_TABLE` are lease-bound.
  - `KeepAliveValue` serializes as timestamp, state, and host id.
- Watch model:
  - real ETCD watch events are filtered by local mod revision.
  - compensation can synthesize PUT/DELETE events based on current prefix search results.
- CAS model:
  - process function transforms old value to new value.
  - ETCD transaction compares key version before writing the whole new value.
- Metastore model:
  - in-memory map keyed by ETCD key string;
  - monotonically increasing revision counter;
  - per-key history ring buffer of five entries;
  - lease manager deletes attached keys on expiration.

## Main Flows

### External ETCD Access

1. Caller constructs `EtcdStore(address)` or TLS-aware overload.
2. `EtcdStore::Init` creates KV and Lease sessions.
3. Caller creates required tables, installs event handlers, starts watches, and optionally starts keepalive.
4. Runtime operations call `GrpcSession::SendRpc` or `AsyncSendRpc`.
5. Shutdown cancels sessions, watch streams, keepalive streams, and background threads.

Failure-sensitive steps:

- Missing table mapping fails before contacting ETCD.
- `GrpcSession::SendRpc` converts transport failures to `K_RPC_UNAVAILABLE`; during terminal signal plus keepalive timeout it can return `K_RETRY_IF_LEAVING`.

### CAS Write

1. Resolve real ETCD key from table mapping.
2. Read current value/version with a bounded timeout.
3. Let the caller calculate a new serialized value.
4. Run ETCD `Txn` with `CompareKeyVersion(realKey, version)` and `Put(realKey, value)`.
5. Retry while error retry count is below `CAS_ERROR_MAX_RETRY_NUM`.

Failure-sensitive steps:

- Value serialization/deserialization work happens outside ETCD transaction.
- Multiple workers updating the same large value can repeatedly invalidate each other's version comparison.
- `newValue == nullptr` returns success without an ETCD write.

### Keepalive

1. Prepare `KeepAliveValue`.
2. Grant lease with TTL `node_timeout_s`.
3. Put the cluster-table worker row with the lease.
4. Start stream keepalive; renewal interval is derived from node timeout and capped.
5. On failure, retry lease recreation and consult backend writability.
6. If backend appears writable while local keepalive failed, synthesize DELETE and rely on cluster-manager/hash-ring deletion path.
7. Kill local process if failure duration exceeds policy and `auto_del_dead_node=true`.

Failure-sensitive steps:

- This path blends liveness evidence and lifecycle action; keepalive transport failure can lead to local deletion behavior.
- The lease id must be valid before `UpdateNodeState` or reconciliation writes.

### Watch Recovery

1. Start watch streams for all local and optional other-AZ prefixes.
2. Process real events in order from the stream queue.
3. Record processed key version after event handler returns.
4. Periodically prefix-search ETCD and compare current state against `keyVersion_`.
5. Generate fake PUT events for missing or newer keys and fake DELETE events for locally known keys missing in ETCD.
6. On watch stream failure, actively compensate before stream reinitialization.

Failure-sensitive steps:

- Fake events and real delayed events can both arrive; handlers must be idempotent or version-aware.
- Cluster-table DELETE events are suppressed when backend writability check fails.

### Metastore Request Handling

1. `MetaStoreServer` registers service implementations over in-memory managers.
2. KV Put/Delete/Txn update `KVManager`, revisions, lease bindings, and watch callbacks.
3. Watch service registers watchers and can replay limited historical events.
4. Lease expiration checks periodically delete lease-bound keys.
5. Maintenance status returns current in-memory revision.

Failure-sensitive steps:

- Only a small historical window is retained; long watch gaps can lose history.
- State is in process memory in this layer.

## External Interaction And Dependency Analysis

- `EtcdClusterManager`:
  - depends on cluster-table watches, ring watches, keepalive, node state updates, and backend writability.
  - failure impact: missed DELETE or duplicate fake DELETE can change passive scale-down timing.
- `HashRing`:
  - depends on CAS over `/datasystem/ring` and ring watch revisions.
  - failure impact: high CAS conflict delays initialization, add/remove node, voluntary scale-down, and self-healing.
- Metadata modules:
  - depend on ETCD metadata prefixes and batch writes.
  - failure impact: object route metadata may be present even when ring/membership state is changing.
- Metastore:
  - shares the same API surface but not necessarily the same operational properties as external ETCD.

## Configuration Model

| Config | Type | Effect | Risk if changed |
| --- | --- | --- | --- |
| `etcd_address` | string | external ETCD backend address | empty requires Metastore path or startup rejection |
| `metastore_address` | string | in-process ETCD-compatible backend address | selected by `GetBackendAddress` when present |
| `cluster_name` | string | prefixes ETCD table keys | changes key namespace |
| `other_cluster_names` | string/list | creates other-AZ prefixes | affects cross-AZ watch/read scope |
| `node_timeout_s` | uint32 | lease TTL and renewal timing | too small causes aggressive liveness loss |
| `node_dead_timeout_s` | uint32 | keepalive death policy | too small can kill workers during transient failures |
| `auto_del_dead_node` | bool | enables local kill after liveness timeout | false changes passive deletion behavior |
| `enable_etcd_auth` | bool | enables TLS/auth path | inconsistent cert config breaks ETCD access |
| `etcd_ca`, `etcd_cert`, `etcd_key`, `etcd_passphrase_path` | string | TLS material source | misconfiguration breaks sessions |
| `etcd_target_name_override` | string | TLS hostname override | mismatch breaks TLS validation |

## Availability, Reliability, And Resilience

- Availability model:
  - normal runtime relies on local watches plus local derived state, but durable mutation depends on ETCD-compatible backend availability.
- Reliability model:
  - CAS ensures single-key version safety, not multi-key cluster transaction safety.
  - leases provide liveness but are sensitive to network partition and timing configuration.
  - event compensation attempts to converge local event handlers after missed watch events.
- Resilience limits:
  - hot single-key writes amplify ETCD pressure.
  - watch compensation can duplicate work.
  - Metastore's in-memory history and storage limit failure recovery compared with a persistent ETCD cluster.

## Observability

- Main logs:
  - `EtcdStore::CAS` success/retry logs;
  - `EtcdKeepAlive` lease creation, retry, timeout, and shutdown logs;
  - `EtcdWatch` watch response, fake event, reinitialization, and queue-depth logs;
  - `MetaStoreServer` start/stop logs.
- Metrics:
  - `GrpcSession` records request status in `MetricsBlockingVector`.
  - access recorder maps selected ETCD RPC method names to ETCD request keys.
- Debug hooks:
  - `EtcdWatch.RetrieveEventPassively.*`
  - `EtcdStore.WatchRun.shutdown`
  - `EtcdStore.LaunchKeepAliveThreads.*`
  - `etcd.txn.commit`
  - `etcd.sendrpc`
  - `grpc_session.SendRpc.EtcdRequestFailed`

## Performance Characteristics

- Hot paths:
  - read and route paths use local state after watch updates, but writes go to ETCD.
  - cluster table keepalive writes happen for every worker.
  - ring/state-machine mutations often CAS full serialized values.
- Known expensive operations:
  - prefix scans during watch compensation;
  - large `HashRingPb` CAS read/serialize/write loops;
  - many concurrent lease keepalive streams;
  - repeated transaction conflicts under scale-out/scale-in.
- Scaling assumptions:
  - ETCD is used as a strongly coordinated metadata bus, not merely as a low-frequency configuration store.
  - current design does not shard high-conflict cluster state by worker, range, or task.

## Compatibility And Invariants

- ETCD proto files are a third-party wire-contract surface; changes require compatibility review.
- `ETCD_CLUSTER_TABLE` key format and lease binding are cluster-manager contracts.
- `ETCD_RING_PREFIX` is the hash-ring contract.
- Watch event handlers should tolerate duplicate or synthesized events.
- `KeepAliveValue` parse/string format is a compatibility surface for cluster-manager.
- Metastore behavior should stay close enough to ETCD for APIs used by the runtime, especially version, revision, lease, watch, and transaction semantics.

## Build, Test, And Verification

- Build entrypoints:
  - `third_party/protos/etcd/BUILD.bazel`
  - `cmake/external_libs/etcdapi.cmake`
  - `src/datasystem/common/kvstore/etcd/CMakeLists.txt`
  - `src/datasystem/common/kvstore/etcd/BUILD.bazel`
  - `src/datasystem/common/kvstore/metastore/CMakeLists.txt`
  - `src/datasystem/common/kvstore/metastore/BUILD.bazel`
- Representative tests:
  - `tests/st/common/kvstore/etcd_store_test.cpp`
  - `tests/st/common/kvstore/grpc_session_test.cpp`
  - `tests/ut/common/kvstore/metastore_server_test.cpp`
  - `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`
  - `tests/st/worker/object_cache/etcd_cluster_manager_test.cpp`
- Manual verification:
  - inspect key prefixes after `CreateTable`;
  - confirm cluster-table Put carries a nonzero lease;
  - force watch interruption and verify compensation emits expected fake events;
  - create CAS conflict and confirm bounded retry behavior;
  - restart ETCD or Metastore and verify keepalive and watch recovery.
