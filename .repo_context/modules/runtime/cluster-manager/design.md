# Cluster Manager Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - current implementation
- Primary code paths:
  - `src/datasystem/worker/cluster_manager`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/object_cache/worker_oc_service_impl.cpp`
- Primary source-of-truth files:
  - `src/datasystem/worker/cluster_manager/cluster_manager.cpp`
  - `src/datasystem/worker/cluster_manager/cluster_manager.h`
  - `src/datasystem/worker/cluster_manager/cluster_node.h`
  - `src/datasystem/worker/cluster_manager/worker_health_check.cpp`
  - `src/datasystem/worker/cluster_event_type.h`
  - `src/datasystem/worker/hash_ring/hash_ring_event.h`
- Last verified against source:
  - `2026-05-08`
- Related context docs:
  - `.repo_context/modules/runtime/cluster-manager/README.md`
  - `.repo_context/modules/runtime/hash-ring/README.md`
  - `.repo_context/modules/runtime/etcd-metadata/README.md`
  - `.repo_context/modules/runtime/cluster-management.md`
- Related playbooks:
  - `.repo_context/playbooks/features/runtime/cluster-manager/implementation.md`

## Purpose

- Why this design document exists:
  - capture how worker membership, ETCD watches, hash-ring state, reconciliation, and route lookup are coordinated today.
- What problem this module solves:
  - turn ETCD-compatible membership and ring events into safe local routing and metadata lifecycle actions.
- Who or what depends on this module:
  - worker startup/shutdown, object-cache service, stream-cache service, slot recovery, notify-worker managers, worker remote APIs, and client metadata redirection.

## Goals

- Track active, timed-out, failed, exiting, restarted, and recovered workers.
- Initialize worker membership and hash-ring state from ETCD or degraded local snapshots.
- Keep cluster-node state aligned enough with hash-ring state to route requests and drive scale tasks.
- Notify metadata, slot-recovery, and worker cleanup subsystems when workers time out, recover, restart, or leave voluntarily.
- Provide route helpers for local metadata access.
- Gate worker readiness and ETCD state transitions through reconciliation and health probes.

## Non-Goals

- It does not implement ETCD RPC, watch, lease, or CAS mechanics.
- It does not calculate hash tokens or migration ranges.
- It does not execute metadata migration itself.
- It does not own metadata manager lifetime beyond routing/recovery notifications; local metadata managers are held by `MetadataManagerHolder`.
- It does not provide a formally isolated state-machine framework; current state transitions are distributed across event handlers and background loops.

## Architecture Overview

- High-level structure:
  - `ClusterManager` owns worker membership tables, event queue, background threads, hash-ring pointer, and route helpers.
  - `ClusterNode` stores membership event timestamp, lifecycle tag, and local state.
  - `worker_health_check` owns a process-global health flag and optional probe file.
  - `cluster_event_type.h` and `hash_ring_event.h` provide event-bus contracts to neighboring modules.
- Key persistent/backend state:
  - ETCD cluster-table lease keys;
  - ETCD hash-ring protobuf;
  - RocksDB cluster snapshots used during ETCD-down restart.
- Key in-memory state:
  - `clusterNodeTable_`, `orphanNodeTable_`;
  - priority event queue and temporary cluster-event cache;
  - `nodeTableCompletionTimer_`;
  - local `HashRing`;
  - `isLeaving_`, `isEtcdAvailableWhenStart_`, `workerWaitPost_`, and health probe state.

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| `ClusterManager` | lifecycle coordinator, event dispatch, route lookup | `cluster_manager.*` | central integration point |
| `ClusterNode` | local membership state wrapper | `cluster_node.h` | `ACTIVE`, `TIMEOUT`, `FAILED` plus ETCD lifecycle tag |
| event priority queue | decouple ETCD watch thread from heavy handlers | `cluster_manager.h/.cpp` | ring priority > cluster priority |
| node utility thread | event handling, demotion, hash-ring progress, sync | `StartNodeUtilThread` | overloaded loop |
| orphan monitor | cleanup nodes missing from hash ring/ETCD | `StartOrphanNodeMonitorThread` | per-orphan ETCD get |
| fake node repair | synthesize add/delete for ring workers absent from node table | `CompleteNodeTableWithFakeNode` | repairs full-cluster restart gaps |
| route helpers | map object keys to `MetaAddrInfo` via hash ring | route methods in header/cpp | uses hash ring and cluster-node state |
| health probe | process readiness state and optional file | `worker_health_check.*` | used by startup and fake-node scheduling |

## Data And State Model

- `ClusterNode::NodeState`:
  - `ACTIVE`: cluster-manager believes the worker has an active cluster-table row or has been added by a controlled fake event.
  - `TIMEOUT`: cluster-table DELETE was observed, but node-dead timeout has not elapsed.
  - `FAILED`: timeout elapsed or local voluntary/crash handling decided the worker is failed.
- `ClusterNode::additionEventType_`:
  - `start`: first start or fake node value;
  - `restart`: worker restarted while ETCD was available;
  - `recover`: worker recovered from network/ETCD interruption;
  - `ready`: reconciliation completed and worker can serve;
  - `exiting`: voluntary scale-down flow has started;
  - `d_rst`: downgrade restart when worker starts with ETCD unavailable.
- `ClusterInfo`:
  - startup transport object containing local ring data, local workers, ETCD revision, and backend availability.
- Route model:
  - in distributed mode, all object keys route by hashing the full key to a primary worker UUID via hash ring; worker UUID is an internal identity only, not embedded in object keys.
  - route batching caches hash-range decisions per call.

## Main Flows

### Initialization

1. `WorkerOCServer` validates ETCD/Metastore address and initializes `EtcdStore`.
2. It constructs `ClusterManager` before initializing services because services need route and lifecycle hooks.
3. `ClusterInfo` is loaded from ETCD/Metastore when backend health passes, otherwise from RocksDB and peer reconciliation.
4. `ClusterManager::Init` starts background threads before hash-ring initialization.
5. Startup cluster nodes are converted into queued fake PUT events so normal event handlers build local state.
6. Watches start from the startup revision.
7. Hash ring initializes from ETCD or degraded snapshot.
8. Local keepalive writes a lease-bound cluster-table row with `start`, `restart`, or `d_rst`.

Failure-sensitive steps:

- Background thread must be alive before watch/hash-ring interactions.
- `clusterInfo.localHashRing[0]` is assumed available in ETCD-down distributed startup.
- Timeout config validation prevents nonsensical liveness/demotion timing.

### Event Processing

1. Watch callback classifies events by key prefix.
2. Queue priority gives ring changes precedence over cluster changes.
3. Cluster events are cached while distributed hash ring is not workable.
4. Ring event updates the local hash ring.
5. Cluster event parses host address and lifecycle state, then handles local membership.

Failure-sensitive steps:

- Cluster event parsing depends on `KeepAliveValue` format even for fake node values.

### Passive Failure

1. DELETE event marks node `TIMEOUT`.
2. Object/stream subscribers receive timeout notification for metadata cleanup/recovery; metadata primary switching is removed.
3. Utility thread demotes to `FAILED` after timeout gap.
4. Failed node handling triggers metadata cleanup and slot recovery.
5. Hash ring receives failed-worker evidence and may remove the worker from routing.
6. Orphan monitor later erases stale node-table and API state.

Failure-sensitive steps:

- Metadata cleanup can be triggered before hash-ring removal fully drains.
- `HashRing::RemoveWorkers` is called periodically by every cluster-manager instance.

### Voluntary Exit

1. Server shutdown marks ETCD node state `exiting`.
2. Local cluster-manager sets `isLeaving_` and starts hash-ring voluntary scale-down.
3. Hash-ring events and worker service hooks wait for client/async-task readiness.
4. Cluster-table deletion of an exiting node is interpreted as success or crash based on hash-ring pre-leaving/leaving state.
5. Successful exit erases node-table/API state and metadata; crash-during-exit follows failed-node recovery.

Failure-sensitive steps:

- `exiting` is a persistent ETCD state string and must remain compatible with cluster-manager.
- Local worker deletion while voluntary exit is in progress is handled specially.

### Restart / Reconciliation

1. Restart writes `restart`, recovery writes `recover`, downgrade writes `d_rst`.
2. Current master triggers object/stream metadata reconciliation and node restart events.
3. Worker service counts reconciliation completions against expected hash-ring worker count.
4. Health probe is set only after reconciliation or a controlled give-up path.
5. ETCD state changes to `ready` after reconciliation completion.

Failure-sensitive steps:

- Restart recovery depends on hash-ring worker count.
- Give-up path can set health and ready after timeout even if not all reconciliation messages arrived.

### Route Lookup

1. Centralized mode returns configured `master_address`.
2. Distributed mode hashes the full object key to a primary worker UUID via hash ring.
3. Hash ring maps the owner UUID to worker address.
4. Connection check validates cluster-node state before returning.

Failure-sensitive steps:

- A worker can be in hash ring but not active/ready in cluster-node table.
- Route lookup now depends on hash-ring and cluster-node state.

## External Interaction And Dependency Analysis

- ETCD metadata:
  - supplies cluster-table watches, ring watches, keepalive state, and backend health.
  - failure impact: fake events and local snapshots try to preserve progress but increase state ambiguity.
- Hash ring:
  - supplies route ownership, worker UUID mapping, and voluntary/passive scale-down state.
  - failure impact: cluster-manager may cache cluster events until ring is workable.
- Object-cache metadata:
  - handles node timeout, restart, network recovery, metadata request, change primary, and failed-worker API cleanup.
- Stream-cache metadata:
  - handles clear/check metadata events and hash-ring recovery events.
- Local metadata holder:
  - `MetadataManagerHolder` owns the local object-cache and stream-cache metadata managers; it does not perform remote metadata ownership lookup.
- Slot recovery:
  - receives failed workers after demotion or crashed voluntary scale-down.
- Worker service/server:
  - gates health, readiness, client drain, async-task drain, and voluntary exit.

## Configuration Model

| Config | Type | Effect | Risk if changed |
| --- | --- | --- | --- |
| `enable_distributed_master` | bool | enables hash-ring route path | false bypasses many ring waits |
| `etcd_address` / `metastore_address` | string | backend for cluster metadata | backend health changes startup path |
| `cluster_name` | string | cluster/prefix namespace | namespace changes persisted key prefixes |
| `node_timeout_s` | uint32 | lease timeout and fake-node delay | too small creates fast timeout events |
| `node_dead_timeout_s` | uint32 | demotion from TIMEOUT to FAILED | must be greater than node timeout |
| `heartbeat_interval_ms` | int32 | lower bound for node timeout | must be less than node timeout in ms |
| `add_node_wait_time_s` | uint32 | affects startup table wait and hash-ring add timing | high values extend readiness wait |
| `auto_del_dead_node` | bool | allows local failed-worker deletion through hash ring | false leaves failed workers until manual action |
| `health_check_path` | string | optional liveness file path | readiness integrations depend on file state |

## Availability, Reliability, And Resilience

- Availability model:
  - local route reads use in-memory tables, but membership and ring updates depend on ETCD-compatible watch/keepalive.
  - degraded startup can use RocksDB snapshots only in distributed-master mode.
- Reliability model:
  - membership changes are eventually reflected through watch events, background loop demotion, and fake repair.
  - metadata and slot-recovery subsystems are notified through in-process event subscribers.
- Resilience limits:
  - many state copies must converge.
  - fake events repair some gaps but add ambiguity.
  - bounded queue and periodic loops can delay handling during event storms.

## Observability

- Main logs:
  - cluster info dump at startup;
  - `NodesToString`;
  - event queue enqueue/processing logs;
  - timeout demotion logs;
  - fake-node completion logs;
  - reconciliation and health-file logs.
- Debug hooks:
  - `ClusterManager.HandleNodeAdditionEvent.delay`
  - `ClusterManager.CheckWaitNodeTableComplete.*`
  - `ClusterManager.IfNeedTriggerReconciliation.noreconciliation`
  - `ClusterManager.GroupObjKeysByMasterHostPortWithStatus.PreFetchDestAddrFromAnywhere`
  - `WorkerOCServiceImpl.Reconciliation.*`
  - `worker.RunKeepAliveTask`
- How to tell the module is healthy:
  - local worker has `ready` state in ETCD cluster table;
  - health probe is set;
  - `clusterNodeTable_` includes all active hash-ring workers;
  - no long-lived fake node timers or orphan nodes;
  - route lookups return connected local nodes.

## Performance Characteristics

- Hot paths:
  - route lookup reads hash-ring maps and cluster-node tables.
  - background utility thread wakes every 100 ms when idle.
- Known expensive operations:
  - per-worker watch/keepalive/background loops;
  - periodic `GetFailedWorkers` table iteration and hash-ring inspection;
  - orphan cleanup ETCD `Get` per candidate worker;
  - group routing connection checks for many masters.
- Scaling assumptions:
  - cluster size is small enough that every worker can maintain full membership state and full ring snapshots.
  - large failure/scale storms are not isolated into sharded ownership domains.

## Compatibility And Invariants

- `KeepAliveValue` lifecycle state strings are compatibility surface.
- ETCD key prefixes for ring and cluster table must stay consistent with `EtcdStore::CreateTable` and event prefix matching.
- Hash-ring and node table are separate state machines; do not collapse them without a migration plan.
- `FAKE_NODE_EVENT_VALUE` is only safe because add/remove handlers guard fake vs real event values.
- `ClusterNode::TIMEOUT` must demote only after `node_dead_timeout_s - node_timeout_s`.
- `ready` should be written only after reconciliation/health readiness.

## Build, Test, And Verification

- Build entrypoints:
  - `src/datasystem/worker/cluster_manager/CMakeLists.txt`
  - `src/datasystem/worker/cluster_manager/BUILD.bazel`
- Representative tests:
  - `tests/st/worker/object_cache/cluster_manager_test.cpp`
  - `tests/st/client/kv_cache/kv_client_scale_test.cpp`
  - `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`
- Manual verification:
  - inspect ETCD cluster table for `start`/`restart`/`recover`/`ready`/`exiting`;
  - inspect `/datasystem/ring` for worker state and pending add/delete info;
  - compare node-table dump with hash-ring worker set;
  - force worker DELETE and verify TIMEOUT, FAILED, hash-ring removal, and cleanup events;
  - restart worker and verify reconciliation then `ready`.
