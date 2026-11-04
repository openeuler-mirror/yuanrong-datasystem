# ETCD Metadata Design

Object metadata persistence is local to RocksDB. Object metadata, locations, deletion records and pending Worker
operations do not use ETCD tables. Worker-to-Worker TCP migration retains its business protocol; the unused ETCD
queue protobuf field remains declared but is neither populated nor replayed. The built-in compatibility service and
its deployment parameters have been removed. External ETCD membership, leases, Watch, topology and slot coordination
remain supported.

## Document Metadata

- Status:
  - `active`
- Design scope:
  - current implementation
- Primary code paths:
  - `third_party/protos/etcd`
  - `src/datasystem/common/kvstore/etcd`
- Primary source-of-truth files:
  - `third_party/protos/etcd/api/etcdserverpb/rpc.proto`
  - `third_party/protos/etcd/api/mvccpb/kv.proto`
  - `src/datasystem/common/kvstore/etcd/etcd_store.cpp`
  - `src/datasystem/common/kvstore/etcd/etcd_watch.cpp`
  - `src/datasystem/common/kvstore/etcd/etcd_keep_alive.cpp`
  - `src/datasystem/common/kvstore/etcd/grpc_session.h`
- Last verified against source:
  - `2026-08-06`
- Related context docs:
  - `.repo_context/modules/runtime/etcd-metadata/README.md`
  - `.repo_context/modules/runtime/topology/README.md`
- Related playbooks:
  - `.repo_context/playbooks/features/runtime/etcd-metadata/implementation.md`

## Purpose

- Why this design document exists:
  - capture the ETCD-compatible storage layer that runtime topology depends on for membership, routing state, watch events, and failure detection.
- What problem this module solves:
  - stable project-local access to ETCD v3 APIs for external ETCD coordination.
- Who or what depends on this module:
  - `TopologyEngine`, replica recovery coordination, CLI tools, and ETCD failure tests.

## Goals

- Provide ETCD v3 protobuf/gRPC definitions to C++ runtime code.
- Hide direct ETCD RPC details behind `EtcdStore` and `GrpcSession`.
- Support cluster metadata writes, reads, range scans, CAS, watches, leases, and node liveness.
- Provide watch compensation so cluster state can converge after stream interruption.
- Support ETCD authentication and TLS when configured.

## Non-Goals

- It does not define the topology state machine or worker lifecycle policy.
- It does not shard or redesign higher-level cluster metadata.
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
- Key persistent or backend state:
  - ETCD key prefixes for membership, topology, operational coordination.
- Key in-memory state:
  - `EtcdStore::tableMap_`, auth token, lease id, keepalive value, monotonic first-publication fact, membership mutation
    mutex, timers, watch instance;
  - `EtcdWatch::keyVersion_`, prefix map, event queues;

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
  - Worker topology uses one Worker-owned Store and one stream carrying the union of Worker and Controller watch
    targets; Engine routes events to separate role dispatchers.
- CAS model:
  - process function transforms old value to new value.
  - ETCD transaction compares key version before writing the whole new value.

## Main Flows

### Object Cache Metadata Boundary

- `ObjectMetaStore` stores object metadata, locations, pending Worker operations and global-delete records only in local RocksDB. It has no ETCD backend, queues or table registration.
- Object Get requires metadata returned by the metadata owner. Startup loads enabled local RocksDB records directly and propagates storage errors; it does not choose a backend from a historical health marker. `rocksdb_write_mode=none` disables local object metadata persistence.
- Worker TCP migration persists received auxiliary tasks before acknowledgement and removes source tasks only for acknowledged, unchanged epochs. Task-only transfers require the receiver capability acknowledgement.
- Fast-restart metadata repair remains independent. External ETCD retains cluster coordination; the built-in Metastore service and its deployment options are removed.

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
3. Put the cluster-table worker row with the lease and mark the first-publication fact only after success. Initial
   `STARTING`/`RESTARTING` templates then become `RECOVERING` for later automatic lease recreation.
4. Start stream keepalive; renewal interval is `clamp(node_timeout_s * 1000 / 3, 100, 5000)` milliseconds.
5. On failure, retry lease recreation and consult backend writability.
6. If backend appears writable while local keepalive failed, synthesize DELETE and rely on topology deletion handling.
7. Kill the local process if failure duration exceeds `node_dead_timeout_s`.
8. Explicit READY/reconciliation, automatic recreation, and EXITING writes are serialized by the membership mutation
   mutex. The template remains RECOVERING after READY; the topology runtime owns the guarded recovery-to-READY closure.

Failure-sensitive steps:

- This path blends liveness evidence and lifecycle action; keepalive transport failure can lead to local deletion behavior.
- The lease id must be valid before `UpdateNodeState` or reconciliation writes.
- Failed membership writes do not advance the first-publication fact or a caller's lifecycle-admission gate.
- Membership mutation takes the store table shared mutex only in the outer-membership/inner-table direction; keepalive
  recovery may take `keepAliveLock_` before membership mutation, and the inverse nesting is forbidden.

### Watch Recovery

1. Start watch streams for configured local prefixes.
2. Process real events in order from the stream queue.
3. Record processed key version after event handler returns.
4. Periodically prefix-search ETCD and compare current state against `keyVersion_`.
5. Generate fake PUT events for missing or newer keys and fake DELETE events for locally known keys missing in ETCD.
6. On watch stream failure, actively compensate before stream reinitialization.

Failure-sensitive steps:

- Fake events and real delayed events can both arrive; handlers must be idempotent or version-aware.
- Cluster-table DELETE events are suppressed when backend writability check fails.

## External Interaction And Dependency Analysis

- `ClusterManager`:
  - depends on cluster-table watches, ring watches, keepalive, node state updates, and backend writability.
  - failure impact: missed DELETE or duplicate fake DELETE can change passive scale-down timing.
- `TopologyRepository`:
  - persists topology records using backend CAS and observes revisioned watch events.
  - failure impact: high CAS conflict delays initialization, add/remove node, voluntary scale-down, and self-healing.
- Object metadata consumers:
  - use local RocksDB persistence and Worker-to-Worker migration; object lookup does not fall back to ETCD.
  - shares the same API surface but not necessarily the same operational properties as external ETCD.

## Configuration Model

| Config | Type | Effect | Risk if changed |
| --- | --- | --- | --- |
| `cluster_name` | string | prefixes ETCD table keys | changes key namespace |
| `node_timeout_s` | uint32 | lease TTL and derived renewal timing | too small causes aggressive liveness loss |
| `node_dead_timeout_s` | uint32 | keepalive death policy | too small can kill workers during transient failures |
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

## Observability

- Main logs:
  - `EtcdStore::CAS` success/retry logs;
  - `EtcdKeepAlive` lease creation, retry, timeout, and shutdown logs;
  - `EtcdWatch` watch response, fake event, reinitialization, and queue-depth logs;
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
  - large `ClusterTopologyPb` CAS read/serialize/write loops;
  - many concurrent lease keepalive streams;
  - repeated transaction conflicts under scale-out/scale-in.
- Scaling assumptions:
  - ETCD is used as a strongly coordinated metadata bus, not merely as a low-frequency configuration store.
  - current design does not shard high-conflict cluster state by worker, range, or task.

## Compatibility And Invariants

- ETCD proto files are a third-party wire-contract surface; changes require compatibility review.
- `ETCD_CLUSTER_TABLE` key format and lease binding are topology membership contracts.
- The first successful membership publication is monotonic for one initialized keepalive owner and is independent of
  later lifecycle template transitions.
- A post-READY lease recreation must publish RECOVERING first; only the local admitted Worker may restore READY, and a
  later EXITING write must remain the final process-local lifecycle commit.
- `TopologyKeyHelper::TopologyKey` is the canonical topology key contract.
- Watch event handlers should tolerate duplicate or synthesized events.
- `WorkerServiceInfo` parse/string/proto format is a compatibility surface for topology membership.

## Build, Test, And Verification

- Build entrypoints:
  - `third_party/protos/etcd/BUILD.bazel`
  - `cmake/external_libs/etcdapi.cmake`
  - `src/datasystem/common/kvstore/etcd/CMakeLists.txt`
  - `src/datasystem/common/kvstore/etcd/BUILD.bazel`
- Representative tests:
  - `tests/st/common/kvstore/etcd_store_test.cpp`
  - `tests/st/common/kvstore/grpc_session_test.cpp`
  - `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`
  - `tests/ut/cluster/coordination_backend_contract_test.cpp`
- Manual verification:
  - inspect key prefixes after `CreateTable`;
  - confirm cluster-table Put carries a nonzero lease;
  - force watch interruption and verify compensation emits expected fake events;
  - create CAS conflict and confirm bounded retry behavior;
