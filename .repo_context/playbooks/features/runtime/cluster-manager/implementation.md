# Cluster Manager Implementation Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - mixed
- Owning module or area:
  - `runtime.cluster-manager`
- Primary code paths:
  - `src/datasystem/worker/cluster_manager`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/object_cache/worker_oc_service_impl.cpp`
- Related module docs:
  - `.repo_context/modules/runtime/cluster-manager/README.md`
  - `.repo_context/modules/runtime/hash-ring/README.md`
  - `.repo_context/modules/runtime/etcd-metadata/README.md`
  - `.repo_context/modules/runtime/cluster-management.md`
- Related design docs:
  - `.repo_context/modules/runtime/cluster-manager/design.md`
- Related tests or validation entrypoints:
  - `tests/st/worker/object_cache/etcd_cluster_manager_test.cpp`
  - `tests/st/client/kv_cache/kv_client_scale_test.cpp`
  - `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`
- Last verified against source:
  - `2026-05-08`

## Purpose

- Why this playbook exists:
  - cluster-manager changes often alter worker lifecycle, hash-ring progress, metadata recovery, and route correctness at the same time.
- What change class it standardizes:
  - changes to ETCD cluster events, node addition/removal, restart reconciliation, passive scale-down, voluntary scale-down coordination, fake-node repair, routing, or health probes.
- What risks it is meant to reduce:
  - accidental metadata loss, stale routing, duplicate fake-event handling, delayed deletion, broken restart readiness, and uncontrolled ETCD/hash-ring pressure.

## When To Use This Playbook

- Use when:
  - modifying `src/datasystem/worker/cluster_manager/*`;
  - changing `ClusterNode` states or `KeepAliveValue` lifecycle tags;
  - changing worker startup/shutdown flow around `EtcdClusterManager`;
  - changing object/stream/replica/slot recovery subscribers to cluster events;
  - changing route helpers that use hash ring or replica-manager events.
- Do not use when:
  - changing only ETCD RPC internals with no lifecycle behavior; use ETCD metadata playbook.
  - changing only hash-ring range calculation; use hash-ring playbook.
- Escalate to design-first review when:
  - changing event ordering, queue priority, or cache-before-ring-ready behavior;
  - changing passive failure timing or cleanup semantics;
  - changing voluntary scale-down `exiting` behavior;
  - changing restart/reconciliation readiness gates;
  - changing fake-node repair behavior.

## Preconditions

- Required context to read first:
  - `.repo_context/modules/runtime/cluster-manager/README.md`
  - `.repo_context/modules/runtime/cluster-manager/design.md`
  - `.repo_context/modules/runtime/hash-ring/README.md`
  - `.repo_context/modules/runtime/etcd-metadata/README.md`
- Required source files to inspect first:
  - `etcd_cluster_manager.cpp`
  - `etcd_cluster_manager.h`
  - `cluster_node.h`
  - `worker_health_check.cpp`
  - `cluster_event_type.h`
  - `hash_ring_event.h`
- Required assumptions to verify before coding:
  - whether the worker is centralized or distributed master;
  - whether the change affects local AZ, other AZ, or both;
  - whether ETCD available and ETCD-down startup paths differ;
  - whether a node can be in a mismatched node-table/hash-ring state;
  - which event subscribers observe the change.

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| membership state | does `ACTIVE/TIMEOUT/FAILED` or lifecycle tag behavior change? | read-only or logging change | state transition, tag string, timeout change |
| event ordering | can cluster events process before ring readiness? | priority/cache unchanged | queue priority or temp cache changes |
| passive failure | can a node be removed earlier/later? | same timeout and demotion path | DELETE, demotion, cleanup, or slot recovery change |
| voluntary exit | does `exiting` handling change? | route-only change | worker erasure, metadata clearing, data migration gate |
| restart readiness | can `ready` be written at a different time? | same reconciliation gates | health probe or `InformEtcdReconciliationDone` change |
| cross-module events | are subscribers called differently? | subscriber set unchanged | new/removed event, changed argument semantics |
| scale pressure | does the background loop write or scan more? | same cadence | more ETCD gets/CAS, more fake events, more routing checks |

## Source Verification Checklist

- [ ] confirm which event type triggers the path: ring, cluster, replica, fake node, or watch compensation
- [ ] confirm local vs other-AZ dispatch
- [ ] confirm cluster-node state before and after the handler
- [ ] confirm hash-ring state before relying on route or failed-worker behavior
- [ ] confirm object/stream/replica/slot subscriber side effects
- [ ] confirm health probe and ETCD `ready` timing
- [ ] confirm restart and ETCD-down startup behavior
- [ ] confirm fake event guards still prevent removing real nodes

## Implementation Plan

1. Classify the change as startup, event dispatch, node addition, node deletion, restart, voluntary exit, routing, fake repair, or health.
2. Trace both local-AZ and other-AZ paths if the touched key comes from ETCD watch.
3. Trace both node-table and hash-ring state transitions.
4. Trace event subscribers and ensure changed arguments remain compatible.
5. Add targeted tests for state-machine behavior and system tests when ETCD/watch/hash-ring timing changes.
6. Update module context and cross-module docs if responsibilities or flows change.

## Guardrails

- Must preserve:
  - timeout config invariants;
  - ring event priority over cluster event processing during distributed startup;
  - lease-bound cluster-table membership rows;
  - health/ready only after reconciliation or controlled give-up path;
  - fake event protections.
- Must not change without explicit review:
  - `KeepAliveValue` state meanings;
  - `FAKE_NODE_EVENT_VALUE` behavior;
  - `HandleExitingNodeRemoveEvent`;
  - background utility loop cadence and extra ETCD/hash-ring writes;
  - route behavior for worker-id keys across AZ.
- Must verify in source before claiming:
  - exact subscriber behavior for object-cache, stream-cache, replica-manager, and slot-recovery;
  - whether ETCD-down startup path is covered;
  - whether centralized-master mode bypasses the distributed path.

## Validation Plan

- Fast checks:
  - targeted build for `cluster_manager` and `worker_health_check`.
- Representative tests:
  - `tests/st/worker/object_cache/etcd_cluster_manager_test.cpp`
  - `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`
  - affected scale/restart/voluntary-scale tests in `tests/st/client/kv_cache/kv_client_scale_test.cpp`
- Scenario checks:
  - first startup with multiple workers;
  - worker restart and reconciliation completion;
  - ETCD crash during restart;
  - node timeout to failed demotion;
  - passive scale-down during scale-up;
  - voluntary scale-down with `exiting`;
  - fake node table completion after full-cluster restart;
  - other-AZ ring and cluster event handling.

## Review Checklist

- [ ] node-table, ETCD cluster-table, and hash-ring state transitions are all described
- [ ] fake event and watch-compensation provenance is considered
- [ ] metadata/replica/slot recovery events remain compatible
- [ ] route lookup still checks connectivity and replica primary
- [ ] large-cluster ETCD/CAS/event pressure was considered
- [ ] context docs were updated if behavior changed

## Context Update Requirements

- Module docs to update:
  - `.repo_context/modules/runtime/cluster-manager/README.md`
- Design docs to update:
  - `.repo_context/modules/runtime/cluster-manager/design.md`
- Additional docs to update:
  - `.repo_context/modules/runtime/hash-ring/*` if hash-ring coordination changes
  - `.repo_context/modules/runtime/etcd-metadata/*` if ETCD watch/keepalive semantics change
  - `.repo_context/modules/runtime/cluster-management.md` if cross-module architecture changes
