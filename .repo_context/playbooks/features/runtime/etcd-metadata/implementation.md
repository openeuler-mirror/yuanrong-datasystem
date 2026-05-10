# ETCD Metadata Implementation Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - mixed
- Owning module or area:
  - `runtime.etcd-metadata`
- Primary code paths:
  - `third_party/protos/etcd`
  - `src/datasystem/common/kvstore/etcd`
  - `src/datasystem/common/kvstore/metastore`
- Related module docs:
  - `.repo_context/modules/runtime/etcd-metadata/README.md`
  - `.repo_context/modules/runtime/hash-ring/README.md`
  - `.repo_context/modules/runtime/cluster-management.md`
- Related design docs:
  - `.repo_context/modules/runtime/etcd-metadata/design.md`
- Related tests or validation entrypoints:
  - `tests/st/common/kvstore/etcd_store_test.cpp`
  - `tests/st/common/kvstore/grpc_session_test.cpp`
  - `tests/ut/common/kvstore/metastore_server_test.cpp`
  - `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`
  - `tests/st/worker/object_cache/etcd_cluster_manager_test.cpp`
- Last verified against source:
  - `2026-05-08`

## Purpose

- Why this playbook exists:
  - ETCD metadata changes can affect worker liveness, cluster membership, hash-ring routing, metadata durability, and scale behavior.
- What change class it standardizes:
  - changes to ETCD proto usage, `EtcdStore`, `GrpcSession`, CAS/transaction behavior, watch handling, keepalive, auth/TLS, or Metastore.
- What risks it is meant to reduce:
  - CAS storms, lost or duplicate events, accidental lease-less membership writes, inconsistent table prefixes, and divergence between external ETCD and Metastore behavior.

## When To Use This Playbook

- Use when:
  - modifying `src/datasystem/common/kvstore/etcd/*`;
  - modifying `src/datasystem/common/kvstore/metastore/*`;
  - changing ETCD proto build targets or generated API usage;
  - changing `ETCD_*` constants;
  - changing cluster-manager or hash-ring behavior that relies on watch, keepalive, or CAS semantics.
- Do not use when:
  - changing only hash-ring token allocation without ETCD access changes; use the hash-ring playbook.
- Escalate to design-first review when:
  - changing key prefixes or persisted table layout;
  - changing lease, keepalive, or local worker kill policy;
  - changing watch compensation or fake-event generation;
  - changing CAS retry behavior or transaction comparison target;
  - changing Metastore transaction/revision/watch semantics.

## Preconditions

- Required context to read first:
  - `.repo_context/modules/runtime/etcd-metadata/README.md`
  - `.repo_context/modules/runtime/etcd-metadata/design.md`
  - `.repo_context/modules/runtime/hash-ring/README.md`
- Required source files to inspect first:
  - `etcd_store.cpp`
  - `etcd_watch.cpp`
  - `etcd_keep_alive.cpp`
  - `grpc_session.h`
  - `etcd_constants.h`
  - relevant ETCD proto file in `third_party/protos/etcd`
  - Metastore manager/service file if using `metastore_address`
- Required assumptions to verify before coding:
  - whether the change affects external ETCD, built-in Metastore, or both;
  - whether callers need the same table prefixes and watch revisions;
  - whether cluster-table writes remain lease-bound;
  - whether event handlers can see duplicate or fake events;
  - whether the change increases ETCD transaction or prefix-scan frequency.

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| key layout | does a prefix or table constant change? | no persisted key change | new prefix, renamed table, cluster-name behavior change |
| CAS pressure | does writer fanout or retry cadence change? | same or fewer writes | more full-value transactions or more retries |
| liveness | can a worker be deleted or killed differently? | state string and lease policy unchanged | keepalive timeout, fake delete, or `SIGKILL` policy change |
| watch semantics | can events be duplicated, suppressed, or reordered? | handler contract unchanged | compensation/filter/revision changes |
| backend parity | does Metastore still match used ETCD semantics? | same KV/Txn/Watch behavior | revision, history, lease, or transaction differences |
| security | does auth/TLS path change? | same config and metadata behavior | new session creation path or token handling |

## Source Verification Checklist

- [ ] confirm real key prefix generation for every touched table
- [ ] confirm cluster-table writes carry a nonzero lease
- [ ] confirm CAS compares version or value intentionally
- [ ] confirm watch `keyVersion_` update order relative to event handler
- [ ] confirm fake events are version-aware and handler-safe
- [ ] confirm ETCD writability checks on DELETE paths
- [ ] confirm Metastore implements the same subset of ETCD behavior used by callers
- [ ] confirm shutdown cancels watch, keepalive, and outstanding RPCs

## Implementation Plan

1. Classify the change as proto/build, KV operation, CAS, watch, keepalive, auth/TLS, health, election, or Metastore.
2. Identify direct runtime consumers, especially cluster-manager and hash-ring.
3. Preserve existing table prefix and lease invariants unless the change is an explicit migration.
4. If changing CAS or watch behavior, reason about concurrent scale-up, scale-down, worker restart, and ETCD restart.
5. If changing Metastore, verify both external ETCD and Metastore tests or document the intentional difference.
6. Update module context when behavior, test paths, or operational limits change.

## Guardrails

- Must preserve:
  - ETCD cluster-table writes bound to leases;
  - compatibility of `KeepAliveValue` state parsing;
  - watch startup synchronization before callers proceed;
  - event compensation only after backend state can be read;
  - shutdown cancellation of streams and contexts.
- Must not change without explicit review:
  - `ETCD_RING_PREFIX` or `ETCD_CLUSTER_TABLE`;
  - CAS retry limits or transaction comparison target;
  - local fake DELETE generation policy;
  - worker kill behavior tied to keepalive failure;
  - Metastore revision/transaction behavior.
- Must verify in source before claiming:
  - exact ETCD service method used;
  - whether a code path uses external ETCD, Metastore, or a static transaction session;
  - whether the caller expects raw ETCD keys or table-relative keys.

## Validation Plan

- Fast checks:
  - targeted build for `common_etcd`, `common_metastore`, or nearest Bazel target.
- Representative tests:
  - `tests/st/common/kvstore/etcd_store_test.cpp`
  - `tests/st/common/kvstore/grpc_session_test.cpp`
  - `tests/ut/common/kvstore/metastore_server_test.cpp`
  - `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`
  - `tests/st/worker/object_cache/etcd_cluster_manager_test.cpp`
- Scenario checks:
  - CAS conflict on same key;
  - watch stream failure and compensation;
  - lease expiration and cluster-table DELETE;
  - ETCD restart during worker restart;
  - Metastore transaction with compare failure and success branches;
  - auth/TLS session creation if security path is touched.

## Review Checklist

- [ ] persisted key layout is unchanged or migration is explicit
- [ ] CAS conflict behavior is acceptable under scale/failure fanout
- [ ] watch handlers remain safe for fake or duplicate events
- [ ] keepalive state transitions preserve cluster-manager expectations
- [ ] Metastore and external ETCD behavior were compared for affected APIs
- [ ] context docs were updated if behavior changed

## Context Update Requirements

- Module docs to update:
  - `.repo_context/modules/runtime/etcd-metadata/README.md`
- Design docs to update:
  - `.repo_context/modules/runtime/etcd-metadata/design.md`
- Additional playbooks to update:
  - this file if workflow or risk gates change
