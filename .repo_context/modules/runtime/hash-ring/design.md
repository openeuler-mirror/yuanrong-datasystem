# Hash Ring Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - current implementation
- Primary code paths:
  - `src/datasystem/worker/hash_ring`
  - `src/datasystem/protos/hash_ring.proto`
- Primary source-of-truth files:
  - `src/datasystem/worker/hash_ring/hash_ring.cpp`
  - `src/datasystem/worker/hash_ring/hash_ring_allocator.cpp`
  - `src/datasystem/worker/hash_ring/hash_ring_task_executor.cpp`
  - `src/datasystem/worker/hash_ring/hash_ring_health_check.cpp`
  - `src/datasystem/worker/hash_ring/read_hash_ring.cpp`
  - `src/datasystem/protos/hash_ring.proto`
- Last verified against source:
  - `2026-05-08`
- Related context docs:
  - `.repo_context/modules/runtime/hash-ring/README.md`
  - `.repo_context/modules/runtime/cluster-management.md`
- Related playbooks:
  - `.repo_context/playbooks/features/runtime/hash-ring/implementation.md`
- Related user-facing or internal docs:
  - `docs/source_zh_cn/design_document/cluster_management.md`

## Purpose

- Why this design document exists:
  - capture the current architecture and invariants of the ETCD-backed hash ring so future changes can be reviewed against real lifecycle behavior.
- What problem this module solves:
  - distributed-master metadata routing and topology change coordination across worker startup, scale-up, scale-down, failure, restart, and cross-AZ routing.
- Who or what depends on this module:
  - `EtcdClusterManager`, object-cache and stream-cache metadata managers, replica manager, worker object-cache service, client redirect behavior, and multi-AZ route lookup.

## Goals

- Route metadata keys to a worker UUID/address in distributed-master mode.
- Preserve enough persistent topology state in ETCD-compatible storage for workers to recover after restart.
- Serialize scale-up and scale-down migrations so metadata and data cleanup can complete before a worker becomes active or disappears.
- Allow passive deletion of failed workers when `auto_del_dead_node=true`.
- Allow voluntary scale-down to migrate metadata and data before worker exit.
- Support read-only routing against other AZ hash rings.

## Non-Goals

- It does not implement the ETCD client, watch transport, or lease keepalive layer.
- It does not own object/stream metadata migration internals; it only publishes event callbacks.
- It does not provide a sharded topology store; current implementation uses one hot ring key.
- It does not independently validate cluster liveness; failed workers come from `EtcdClusterManager`.

## Architecture Overview

- High-level structure:
  - `HashRing` owns local state, persistent ring mutations, routing APIs, and update handling.
  - `HashRingAllocator` is a pure-ish token and range-change algorithm helper over `HashRingPb`.
  - `HashRingTaskExecutor` turns `add_node_info` and `del_node_info` into asynchronous migration/recovery/cleanup work.
  - `HashRingHealthCheck` periodically retries stuck tasks and can repair selected abnormal protobuf states.
  - `ReadHashRing` is a protected `HashRing` subclass for read-only other-AZ rings.
  - `hash_ring_tools` maintains mapping helpers, JSON log formatting, and rolling-update map cleanup.
- Key persistent state:
  - serialized `HashRingPb` under `ETCD_RING_PREFIX` (`/datasystem/ring`) and, for multi-AZ, prefixed AZ ring paths.
- Key in-memory state:
  - `ringInfo_`, `tokenMap_`, `workerUuidHashMap_`, `workerUuid2AddrMap_`, `workerAddr2UuidMap_`, `relatedWorkerMap_`, `hashRange_`,
    atomic state flags, and async task IDs.

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| `HashRing` | state machine, ETCD CAS mutations, route lookup, event handling | `hash_ring.h`, `hash_ring.cpp` | owns local derived maps and delegates migrations |
| `HashRingAllocator` | token generation, add/remove range calculation | `hash_ring_allocator.*` | default four virtual nodes per worker |
| `HashRingTaskExecutor` | async scale-up/scale-down/voluntary migration work | `hash_ring_task_executor.*` | uses `HashRingEvent` subscribers and retries CAS marks |
| `HashRingHealthCheck` | stuck-state detection and optional self-healing | `hash_ring_health_check.*` | timing based on `node_dead_timeout_s` and `add_node_wait_time_s` |
| `ReadHashRing` | other-AZ read-side ring snapshot and route helper | `read_hash_ring.*` | updates from watch events without writing ring state |
| `hash_ring_tools` | ring map derivation, JSON logging, update-map cleanup | `hash_ring_tools.*` | `rolling_update_timeout_s` defined here |
| `HashRingPb` | persisted topology and task schema | `src/datasystem/protos/hash_ring.proto` | compatibility-sensitive |

## Data And State Model

- `WorkerPb.state`:
  - `INITIAL`: worker is known but has no tokens;
  - `JOINING`: tokens assigned and `add_node_info` migration pending;
  - `ACTIVE`: worker owns routing ranges;
  - `LEAVING`: voluntary scale-down has started.
- `HashRing::HashState`:
  - `NO_INIT`: centralized master path, no distributed ring routing;
  - `INIT`: waiting for first initialization or local join;
  - `PRE_RUNNING`: ring works but local node is not active yet;
  - `RUNNING`: local node is active in ring;
  - `PRE_LEAVING`: local node is leaving voluntarily;
  - `FAIL`: terminal local failure state.
- `add_node_info`:
  - keyed by destination worker; ranges tell source worker IDs to migrate metadata to the destination.
  - used for normal scale-up, voluntary scale-down metadata migration, and UUID restoration after rolling restart.
- `del_node_info`:
  - keyed by removed/failed worker; ranges tell recovery workers which ownership ranges to recover or clear.
- `key_with_worker_id_meta_map`:
  - maps removed or migrating worker UUIDs to substitute worker addresses for route compatibility.
- `update_worker_map`:
  - maps worker address to reusable UUID and timestamp during rolling update or voluntary scale-down UUID migration.
- Range convention:
  - normal range ownership is `(from, end]` with wraparound handling.
  - `from == end` is a point marker for UUID metadata migration, not an empty range.

## Main Flows

### Initial Cluster

1. Every worker inserts itself into `HashRingPb.workers` with `INITIAL` state during `InitRing`.
2. Periodic inspection waits for the configured delay, then a bounded set of candidate workers CAS-generates all tokens.
3. Token generation divides `UINT32_MAX` by total virtual-token count and marks all workers `ACTIVE`.
4. Watch events update local maps and publish `ClusterInitFinish`.

Failure-sensitive steps:

- CAS conflicts can delay init; only the first five sorted workers generate tokens, but all workers still watch and periodically inspect.
- Parse failure or missing ring can trigger self-healing only when `enable_hash_ring_self_healing=true`.

### Add Node

1. New worker starts as `INITIAL` with no tokens.
2. `GetAddingWorkers` refuses to add while another add/delete task is active unless `needForceJoin_` is set.
3. `HashRingAllocator::AddNode` splits the largest ownership ranges and records destination tokens.
4. `add_node_info` is written so source workers migrate ranges asynchronously.
5. Each source marks its range finished by CAS; when all are done, `add_node_info` clears and joining workers become active.

Failure-sensitive steps:

- If the source node dies while add-node work is pending, `UpdateHashWhenSrcWorkerFailed` marks ranges and writes recovery work to `del_node_info`.
- A joining node that dies requires special handling in `ProcessAddNodeInfoWhenJoiningNodeRemove`.

### Passive Failed-Node Removal

1. `EtcdClusterManager` classifies workers as failed based on cluster-table keepalive state.
2. `HashRing::RemoveWorkers` runs only if `EnableAutoDelDeadNode()` is true.
3. Process workers are chosen from related workers so not every worker writes the same deletion.
4. The selected process worker CAS-adds `del_node_info` and `key_with_worker_id_meta_map`.
5. `HashRingTaskExecutor` recovers metadata, clears data without metadata, and CAS-erases completed `del_node_info`.

Failure-sensitive steps:

- If the local worker removes itself passively, the code marks the process unhealthy and raises `SIGKILL` unless already voluntary leaving.
- Failed standby workers inside `del_node_info` are auto-cleaned by `CheckScaleDownInfoStandbyWorker`.

### Voluntary Scale Down

1. Worker marks `need_scale_down=true`.
2. Periodic inspection waits until the worker is primary for its DB when needed.
3. The worker transitions to `LEAVING` and writes `add_node_info` toward standby workers.
4. Metadata migration clears hash tokens and, after UUID migration, records substitution maps and `update_worker_map`.
5. Data migration starts after `DataMigrationReady`; `BeforeVoluntaryExit` runs before the worker is erased from the ring.

Failure-sensitive steps:

- If all running workers are being voluntarily removed, the code erases unfinished migrate-data workers and marks local shutdown done.
- If no standby worker exists for a voluntary node, the task can be abandoned and the node exits.

### Health Check And Self-Healing

1. The health-check thread records last observed ring value and revision.
2. If no ring change arrives for the retry window, it calls `RestoreScalingTaskIfNeeded`.
3. If no ring change arrives for the fix window, it runs policy checks.
4. With self-healing enabled, selected policies mutate and CAS-write a repaired ring.

Failure-sensitive steps:

- Auto-fix can erase `add_node_info`, `del_node_info`, or workers. Review any policy change as a data-recovery change.
- Stuck `JOINING`, `LEAVING`, or `INITIAL` workers without matching task records currently log manual-fix signals rather than full repair in all cases.

## External Interaction And Dependency Analysis

- ETCD store:
  - hot path for initialization, add, remove, voluntary scale-down, task completion, self-healing, and update-map cleanup.
  - failure impact: workers may keep stale local maps, restoration waits for health checks, and CAS loops can retry.
- `EtcdClusterManager`:
  - supplies watch events, failed-worker sets, DB primary location queries, cross-AZ rings, and route fallback.
  - failure impact: ring may not receive enough evidence to add/delete workers.
- Metadata managers:
  - implement the actual object/stream metadata movement.
  - failure impact: task completion is not marked, leaving `add_node_info` or `del_node_info` pending.
- Worker object-cache service:
  - gates voluntary exit and local clear-data tasks.
  - failure impact: voluntary scale-down can hang or abort.

## Configuration Model

| Config | Type | Default or source | Effect | Risk if changed |
| --- | --- | --- | --- | --- |
| `enable_distributed_master` | bool | `worker_oc_server.cpp`, default true | enables hash-ring route ownership | false bypasses ring routing and auto deletion |
| `auto_del_dead_node` | bool | declared in worker flags | enables passive failed-worker deletion | false leaves failed workers in ring until manual action |
| `add_node_wait_time_s` | uint32 | `worker_oc_server.cpp`, default 60 | delays first init/add-node attempts | low value increases CAS contention during startup |
| `node_dead_timeout_s` | uint32 | worker flags | health-check timing and failed-node policy input | small values can trigger aggressive deletion |
| `enable_hash_ring_self_healing` | bool | `worker_oc_server.cpp`, default false | permits auto repair of stuck ring states | can erase pending migration state |
| `rolling_update_timeout_s` | uint32 | `hash_ring_tools.cpp`, default 1800 | retention for `update_worker_map` | too small can break UUID reuse restoration |
| `etcd_address` | string | worker config | external ETCD backend | empty allowed only when metastore is used |
| `metastore_address` | string | worker config | built-in ETCD-compatible backend | takes precedence in hash-ring backend cleanup path |

## Availability, Reliability, And Resilience

- Availability model:
  - every worker has local derived route state, but the authoritative mutation point is the ETCD ring key.
  - watch events are primary; periodic inspection and health-check retry provide backup progress.
- Reliability model:
  - migration completion is recorded in ETCD only after event subscribers report success.
  - restart recovery replays existing `add_node_info` and `del_node_info`.
- Resilience limits:
  - many lifecycle transitions contend on one CAS key.
  - large cluster changes create large protobuf values, many event notifications, and repeated full-ring compare/write cycles.
  - if ETCD is unavailable and local ring is missing, distributed routing cannot safely initialize.

## Observability

- Main logs:
  - `HashRing finished init`, `Update ring of version`, `Ring summarize`, `Submit scale down task`, `Start migrate task`, `DoHealthCheck`.
- Metrics:
  - no hash-ring-specific metric family was verified in the hash_ring source.
- Debug hooks:
  - inject points throughout hash-ring code and tests;
  - JSON conversion utilities in `hash_ring_pb_utils.*`;
  - `worker_cli.cpp` ETCD ring read/write helpers.
- How to tell the module is healthy:
  - local `HashRing::IsWorkable()` is true;
  - `HashRingPb.cluster_has_init=true`;
  - no long-lived unexpected `add_node_info` or `del_node_info`;
  - active workers have UUIDs and tokens, and local maps can resolve UUID to address.

## Performance Characteristics

- Hot paths:
  - `GetMasterAddr` / `GetPrimaryWorkerUuid` read local maps and are cheap after update.
  - topology mutation paths are expensive because they parse, compare, mutate, serialize, and CAS the whole `HashRingPb`.
- Known expensive operations:
  - first init and scaling write full ring protobufs;
  - health check reads and may rewrite the full ring;
  - add/remove flows may fan out metadata migration events per changed range;
  - logs can split and print full ring JSON.
- Scaling assumptions:
  - the implementation bounds writer candidates in some places but does not shard the ring state.
  - the current single-key CAS design is not suitable for very large concurrent scale events without high conflict probability.

## Compatibility And Invariants

- `HashRingPb` field meanings are persistent compatibility surface.
- Worker address strings are keys in `HashRingPb.workers`; UUIDs are separate route identities.
- `update_worker_map` entries should not be removed while a worker exists with an empty UUID waiting for restoration.
- `key_with_worker_id_meta_map` must point substitute UUIDs at worker addresses that can be resolved by local maps or explicitly handled as removed workers.
- `HashRing::FAIL` is terminal for local state transitions.

## Build, Test, And Verification

- Build entrypoints:
  - `src/datasystem/worker/hash_ring/CMakeLists.txt`
  - `src/datasystem/worker/hash_ring/BUILD.bazel`
- Fast verification commands:
  - `./build.sh -t ut -- -R hash_ring_allocator_test`
  - `./build.sh -t ut -- -R hash_ring_health_check_test`
- Representative tests:
  - `tests/ut/worker/object_cache/hash_ring_allocator_test.cpp`
  - `tests/ut/worker/object_cache/hash_ring_health_check_test.cpp`
  - `tests/st/worker/object_cache/hash_ring_test.cpp`
  - `tests/st/client/kv_cache/kv_client_hashring_healing_test.cpp`

## Common Change Scenarios

### Adding A Ring Field

- Update `src/datasystem/protos/hash_ring.proto`.
- Audit JSON conversion, `MessageDifferencer` comparisons, self-healing, restart restoration, and tests that build literal JSON rings.
- Verify mixed-version and rolling-upgrade expectations before relying on the new field.

### Changing Scale-Up Or Scale-Down Behavior

- Start with `HashRing::AddNode`, `RemoveWorker`, `GenerateVoluntaryScaleDownChangingInfo`, and `HashRingTaskExecutor`.
- Confirm which event subscribers consume the changed ranges.
- Run allocator unit tests plus at least one system scale test for the affected path.

### Debugging A Stuck Ring

- Inspect `/datasystem/ring` as `HashRingPb`/JSON.
- Check whether the local node is `INIT`, `PRE_RUNNING`, `RUNNING`, `PRE_LEAVING`, or `FAIL`.
- Inspect `add_node_info`, `del_node_info`, worker states, `need_scale_down`, `key_with_worker_id_meta_map`, and `update_worker_map`.
- Look for `HashRingHealthCheck` logs and migration task logs with trace IDs.

## Pending Verification

- Full subscriber behavior for stream-cache migration and object-cache global-reference cleanup.
- Exact CTest target names for running only hash-ring system tests in the local build layout.

## Update Rules For This Document

- Update when `HashRingPb` changes.
- Update when a hash-ring state transition, migration task, ETCD key, or event contract changes.
- Update when cluster-manager watch semantics or failed-worker classification changes the ring lifecycle.
