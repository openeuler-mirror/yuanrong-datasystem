# Slot Storage And Recovery Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - `current implementation`
- Primary code paths:
  - `src/datasystem/common/l2cache/slot_client/*`
  - `src/datasystem/worker/object_cache/slot_recovery/*`
  - `src/datasystem/worker/object_cache/slot_recovery_orchestrator.*`
  - `src/datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.cpp`
  - `src/datasystem/worker/object_cache/worker_oc_service_impl.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/protos/slot_recovery.proto`
- Primary source-of-truth files:
  - `src/datasystem/common/l2cache/slot_client/slot_client.*`
  - `src/datasystem/common/l2cache/slot_client/slot.*`
  - `src/datasystem/common/l2cache/slot_client/slot_manifest.*`
  - `src/datasystem/common/l2cache/slot_client/slot_index_codec.*`
  - `src/datasystem/common/l2cache/slot_client/slot_snapshot.*`
  - `src/datasystem/common/l2cache/slot_client/slot_writer.*`
  - `src/datasystem/common/l2cache/slot_client/slot_compactor.*`
  - `src/datasystem/common/l2cache/slot_client/slot_takeover_planner.*`
  - `src/datasystem/common/l2cache/slot_client/slot_file_util.*`
  - `src/datasystem/common/l2cache/slot_client/slot_internal_config.h`
  - `src/datasystem/worker/object_cache/slot_recovery/slot_recovery_manager.*`
  - `src/datasystem/worker/object_cache/slot_recovery/slot_recovery_store.*`
  - `src/datasystem/worker/object_cache/slot_recovery_orchestrator.*`
  - `src/datasystem/protos/slot_recovery.proto`
  - `tests/ut/common/l2cache/slot_store_test.cpp`
  - `tests/st/worker/object_cache/slot_end2end_test.cpp`
  - `tests/st/worker/object_cache/slot_recovery_manager_test.cpp`
- Last verified against source:
  - `2026-04-17`
- Related context docs:
  - `modules/infra/slot/README.md`
  - `modules/infra/l2cache/README.md`
  - `modules/infra/l2cache/l2-cache-type.md`
  - `modules/runtime/worker-runtime.md`
- Related playbooks:
  - `playbooks/features/infra/slot/implementation.md`
  - `playbooks/features/infra/l2cache/implementation.md`
- Related user-facing or internal docs:
  - `docs/source_zh_cn/best_practices/best_practices_for_reliabilty.md`
  - `slot/slot_final.md`
  - Note:
    - `slot/slot_final.md` is useful design background, but current code is the final source of truth.

## Purpose

- Why this design document exists:
  - give slot storage a standalone architecture reference instead of burying it under generic `l2cache` routing.
- What problem this module solves:
  - avoid object-per-file explosion on distributed disk by aggregating object versions into fixed hash slots with
    replayable metadata and takeover-friendly file layout.
- Who or what depends on this module:
  - `AggregatedPersistenceApi` when `l2_cache_type=distributed_disk`
  - worker startup repair
  - worker restart and failed-worker recovery
  - worker slot migration handoff

## Business And Scenario Overview

- Why this capability is needed:
  - distributed-disk persistence needs stronger local repair, takeover, and DFX semantics than OBS and SFS.
- Target users, callers, or operators:
  - worker-runtime developers, distributed-disk backend maintainers, and operators diagnosing restart or slot-handoff
    incidents.
- Typical usage time or trigger conditions:
  - every distributed-disk save/get/delete request;
  - worker startup when local slot directories may need repair;
  - scale-down, failed-worker recovery, or restart when slots must be preloaded or merged.
- Typical deployment or runtime environment:
  - worker processes sharing a distributed-disk root under `distributed_disk_path`, with ETCD-backed coordination for
    restart and failed-worker recovery.
- How the feature is expected to be used:
  - request paths persist data through `PersistenceApi` and `SlotClient`;
  - background compaction reduces stale file growth;
  - recovery flows replay or transfer slots into a new local worker namespace.
- What other functions or modules it may affect:
  - object-cache metadata recovery
  - worker migration RPC handling
  - worker startup and restart sequencing
  - common inject-driven test workflows
- Expected user or operator experience:
  - performance:
    - hot reads stay snapshot-first and only touch payload files after lookup, while writes amortize fsync cost through
      group commit for normal-sized payloads.
  - security:
    - the module assumes filesystem isolation and does not add its own authn/authz layer.
  - resilience:
    - torn tails, partial imports, unfinished compaction, and interrupted takeover are expected and recovered
      explicitly.
  - reliability:
    - replay, manifest recovery, import-batch fencing, and recovery-task replay preserve the visible object set after
      crash or worker handoff.
  - availability:
    - the system aims to restore service through local repair and worker-to-worker takeover rather than in-module
      replication.
  - privacy:
    - raw object payloads are written to disk; the slot layer does not redact or reinterpret them.

## Goals

- aggregate object versions into a bounded number of slots instead of exploding directory counts;
- keep a replayable, crash-tolerant persistence format for visible object state;
- support background compaction without stopping the whole worker;
- support takeover-based merge and preload between worker namespaces;
- provide enough DFX hooks to diagnose replay, compaction, takeover, and recovery issues from logs and tests.

## Non-Goals

- runtime-configurable slot counts; current source fixes the slot count at `128`
- in-module multi-replica storage or consensus ownership
- a generic queryable database layered on slot files
- a rich first-class metrics surface for every slot operation in the current implementation

## Scope

- In scope:
  - slot root-path construction and per-worker namespace ownership
  - slot manifest, index, data file, replay, writer, compaction, and takeover semantics
  - worker startup repair and ETCD-driven slot recovery integration
  - DFX hooks, tests, and invariants that protect slot persistence correctness
- Out of scope:
  - OBS and SFS object-per-version semantics
  - upstream async pipelines that decide when objects are spilled to L2
  - generalized cluster membership logic outside slot-recovery planning
- Important boundaries with neighboring modules:
  - `modules/infra/l2cache/*` owns backend selection and `PersistenceApi` dispatch.
  - `slot` owns the distributed-disk persistence format and recovery behavior once that backend is chosen.
  - worker object-cache services own metadata rebuild and recovery-task orchestration above the slot layer.

## Terminology

| Term | Meaning in this repository | Source or note |
| --- | --- | --- |
| `SlotClient` | distributed-disk router from object key to one local slot instance | `slot_client/slot_client.*` |
| `Slot` | one slot directory plus its full state machine | `slot_client/slot.*` |
| `SlotManifest` | durable record of current active, pending, obsolete, and transfer file sets | `slot_client/slot_manifest.*` |
| `SlotSnapshot` | in-memory visible view rebuilt from replay | `slot_client/slot_snapshot.*` |
| `SlotWriter` | active index/data writer with buffered group flush | `slot_client/slot_writer.*` |
| `SlotCompactor` | builder for compacted index/data artifacts | `slot_client/slot_compactor.*` |
| `SlotTakeoverPlanner` | durable planner for takeover file mapping and import log creation | `slot_client/slot_takeover_planner.*` |
| `SlotRecoveryOrchestrator` | worker-startup repair of local slot directories | `slot_recovery_orchestrator.*` |
| `SlotRecoveryManager` | ETCD-coordinated restart and failed-worker recovery executor | `slot_recovery_manager.*` |

## Current State And Design Choice Notes

- Current implementation or baseline behavior:
  - `SlotClient` hashes `objectKey` into `DISTRIBUTED_DISK_SLOT_NUM = 128` slots and lazily creates slot instances.
  - distributed-disk roots are built by `BuildSlotStoreRootForWorker(distributed_disk_path, cluster_name,
    SanitizeSlotWorkerNamespace(worker_address))`.
  - request-path `timeoutMs` exists on interfaces but is currently ignored by `SlotClient`.
  - `GetRequestSuccessRate()` currently returns an empty string for slot storage.
- Relevant constraints from current release or deployment:
  - one worker namespace owns one physical slot-home path at a time;
  - slot takeover relies on filesystem rename and durable import publication rather than replicated writes;
  - recovery coordination above local repair depends on worker services and ETCD-backed task state.
- Similar upstream, industry, or historical approaches considered:
  - current code keeps a log-structured local persistence model with replay and compaction instead of one-file-per-key
    storage.
- Selected approach and why:
  - fixed-slot aggregation reduces directory pressure and makes takeover cheaper than moving arbitrary object trees.
  - manifest-driven state transitions make compaction and takeover restartable.
  - replay plus import fencing keeps crash recovery localized to slot files instead of requiring a separate database.
- Known tradeoffs:
  - no in-module replica means availability depends on repair and takeover speed rather than instantaneous redundancy;
  - metrics are weaker than logs and tests for slot-specific DFX today;
  - slot-count changes are effectively format changes because every caller uses the same hash rule.

## Architecture Overview

- High-level structure:
  - `SlotClient` hashes requests to one `Slot`.
  - `Slot` owns manifest, replay snapshot, writer, compaction, takeover, and local repair for one slot directory.
  - `SlotIndexCodec` encodes CRC-protected `PUT`, `DELETE`, `IMPORT_BEGIN`, and `IMPORT_END` records.
  - `SlotRecoveryOrchestrator` repairs local slots at startup.
  - `SlotRecoveryManager` coordinates restart and failed-worker preload using `PreloadSlot()`.
- Key runtime roles or processes:
  - request threads calling save/get/delete
  - one background compact thread inside `SlotClient`
  - startup repair on the current worker namespace
  - ETCD-coordinated preload or merge after restart, failure, or migration
- Key persistent state, if any:
  - worker-scoped slot directories under `distributed_disk_path`
  - `manifest`
  - active, pending, and obsolete index and data files
  - takeover plans and import indexes
  - recovery-task state in the slot-recovery store
- Key control-flow or data-flow stages:
  - runtime bootstrap and manifest recovery
  - request-path save/get/delete
  - background compaction
  - takeover publish and source retirement
  - worker-level recovery callback into metadata rebuild

## Scenario Analysis

- Primary user or system scenarios:
  - a worker saves many object versions into one slot and later compacts stale records;
  - a restarted worker repairs unfinished local slot state before serving traffic;
  - a failed worker's successor preloads slot data and rebuilds metadata;
  - a migration target merges a source slot after successful handoff.
- Key interactions between actors and services:
  - request paths interact with `SlotClient` and local filesystem state
  - worker startup interacts with `SlotRecoveryOrchestrator`
  - worker recovery interacts with `SlotRecoveryManager`, ETCD task state, and metadata recovery
  - migration service interacts with `MergeSlot()`
- Normal path summary:
  - objects are written to rolling or dedicated data files, indexed by CRC-protected records, replayed into an in-memory
    snapshot, periodically compacted, and read back by snapshot lookup plus file read.
- Exceptional path summary:
  - torn index tails are truncated after the last valid record;
  - unfinished compaction or takeover is recovered from manifest state;
  - import records are ignored unless the corresponding batch is closed;
  - takeover returns `K_TRY_AGAIN` if transfer intent conflicts with compaction.

## Entry Points, External Interfaces, And Integration Points

- Public APIs:
  - `SlotClient::{Save,Get,GetWithoutVersion,Delete,PreloadSlot,MergeSlot,GetRequestSuccessRate}`
  - `BuildSlotStoreRootForWorker(...)`
- Internal service entrypoints:
  - `worker::WorkerOCServer::InitSlotWorkerNamespace()`
  - `worker::WorkerOCServer::InitSlotRecovery()`
  - `object_cache::SlotRecoveryManager::{ScheduleLocalPendingTasksFromStore,HandleLocalRestart,ExecuteRecoveryTask}`
  - `object_cache::WorkerOcServiceMigrateImpl`
- External protocols, schemas, or data formats:
  - manifest text file
  - binary index records with CRC protection
  - `slot_recovery.proto` tasks and incident state
- CLI commands, config flags, or environment variables:
  - `l2_cache_type`
  - `distributed_disk_path`
  - `distributed_disk_max_data_file_size_mb`
  - `distributed_disk_sync_interval_ms`
  - `distributed_disk_sync_batch_bytes`
  - `distributed_disk_compact_interval_s`
  - `cluster_name`
  - `worker_address`
- Background jobs, threads, or callbacks:
  - background compact loop inside `SlotClient`
  - preload callback used by `SlotRecoveryManager`
  - metadata rebuild callback after recovered payload enumeration
- Cross-module integration points:
  - `AggregatedPersistenceApi` forwards distributed-disk persistence calls into `SlotClient`
  - `MetaDataRecoveryManager` rebuilds metadata after `PreloadSlot()` enumeration
  - worker migration service finalizes slot handoff through `MergeSlot()`
- Upstream and downstream dependencies:
  - upstream: worker object-cache request, migration, and recovery code
  - downstream: local filesystem, slot recovery store, inject framework, and tests
- Backward-compatibility expectations for external consumers:
  - slot manifest and index semantics are operationally visible compatibility surfaces for existing persisted data.

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| `SlotClient` | hash-route requests and run background compaction | `slot_client/slot_client.*` | fixed `128` slots |
| `Slot` | one-slot persistence state machine | `slot_client/slot.*` | guarded by `Slot::mu_` |
| `SlotManifest` | durable phase and file-set state | `slot_client/slot_manifest.*` | text manifest |
| `SlotIndexCodec` | record encoding and torn-tail truncation | `slot_client/slot_index_codec.*` | `PUT`, `DELETE`, `IMPORT_BEGIN`, `IMPORT_END` |
| `SlotSnapshot` | visible object view after replay | `slot_client/slot_snapshot.*` | snapshot-first reads |
| `SlotWriter` | active file append and group commit | `slot_client/slot_writer.*` | interval or batch-size flush |
| `SlotCompactor` | compact artifact build and delta catch-up | `slot_client/slot_compactor.*` | preemptable by transfer |
| `SlotTakeoverPlanner` | target-side plan and import file creation | `slot_client/slot_takeover_planner.*` | durable takeover artifacts |
| `SlotRecoveryOrchestrator` | startup repair for current worker namespace | `slot_recovery_orchestrator.*` | local only |
| `SlotRecoveryManager` | restart and failed-worker recovery coordination | `slot_recovery_manager.*` | ETCD-backed task execution |

## Main Flows

### Bootstrap And Repair

1. `Slot::EnsureRuntimeReadyLocked()` rebuilds state from disk when cache state is stale.
2. Missing slot directories or manifests are bootstrapped locally.
3. `RecoverManifestIfNeeded()` resolves unfinished compaction or transfer states.
4. `SlotSnapshot::Replay()` rebuilds visible state from the active index.
5. `writer_.Init()` reopens the current active files.

Key files:

- `src/datasystem/common/l2cache/slot_client/slot.cpp`
- `src/datasystem/common/l2cache/slot_client/slot_manifest.cpp`
- `src/datasystem/common/l2cache/slot_client/slot_snapshot.cpp`
- `src/datasystem/common/l2cache/slot_client/slot_writer.cpp`

Failure-sensitive steps:

- missing or corrupt manifests fall back to local disk-layout bootstrap
- torn index tails are truncated after the last valid CRC-protected record

### Save, Get, And Delete

1. `SlotClient::Save()` hashes the key and delegates to one slot.
2. `Slot::Save()` validates writable manifest state, writes payload bytes, and appends a `PUT` record.
3. `Slot::Delete()` appends only a delete tombstone to the index.
4. `Slot::Get()` and `GetWithoutVersion()` resolve from `SlotSnapshot` before reading bytes from disk.

Key files:

- `src/datasystem/common/l2cache/slot_client/slot_client.cpp`
- `src/datasystem/common/l2cache/slot_client/slot.cpp`
- `src/datasystem/common/l2cache/slot_client/slot_index_codec.cpp`

Failure-sensitive steps:

- `Save()` requires a seekable payload stream to determine size
- large payloads bypass rolling files and use a dedicated file
- `timeoutMs` is currently ignored by the slot client path

### Compaction

1. `Slot::Compact()` repairs current state and captures a flushed baseline.
2. `SlotCompactor::BuildArtifacts()` copies visible records into a new compacted file set.
3. Delta records are replayed until cutover can safely happen.
4. The manifest moves through `COMPACT_COMMITTING` and then back to normal after swap.
5. Old files are marked obsolete and cleaned by continuation GC.

Key files:

- `src/datasystem/common/l2cache/slot_client/slot.cpp`
- `src/datasystem/common/l2cache/slot_client/slot_compactor.cpp`

Failure-sensitive steps:

- compaction exits with `K_TRY_AGAIN` when transfer intent preempts it
- compaction aborts if import records appear in the delta stream
- cutover refuses stale active-index assumptions

### Takeover, Preload, And Merge

1. `SlotClient::{MergeSlot,PreloadSlot}` route remote-worker operations into `Slot::Takeover()`.
2. takeover claims `transferIntentActive_` before it can proceed.
3. source home slot is renamed to a recovery path before target import publication.
4. `SlotTakeoverPlanner::BuildPlan()` creates file mappings, import index, and durable transfer plan.
5. imported records are published as one fenced import batch.
6. target returns to normal state and source recovery data is retired.
7. `PreloadSlot()` may use `PreloadLocal()` as a same-worker shortcut.

Key files:

- `src/datasystem/common/l2cache/slot_client/slot.cpp`
- `src/datasystem/common/l2cache/slot_client/slot_takeover_planner.cpp`

Failure-sensitive steps:

- partial import batches must not become visible on replay
- source retirement must not happen before imported data is durable on target
- compaction and takeover must not commit concurrently

### Worker Recovery Integration

1. startup sets the slot worker namespace only for `distributed_disk`.
2. startup repair runs `SlotRecoveryOrchestrator` on the local worker namespace.
3. restart scheduling reloads pending tasks from store and may run `HandleLocalRestart()`.
4. `ExecuteRecoveryTask()` calls `PreloadSlot()` with a callback that rebuilds local objects and metadata.
5. migration completion calls `MergeSlot()` after successful slot movement.

Key files:

- `src/datasystem/worker/worker_oc_server.cpp`
- `src/datasystem/worker/object_cache/worker_oc_service_impl.cpp`
- `src/datasystem/worker/object_cache/slot_recovery/slot_recovery_manager.cpp`
- `src/datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.cpp`

Failure-sensitive steps:

- wrong worker namespace derivation strands or cross-contaminates recovery state
- metadata rebuild order matters after recovered payload import

## Data And State Model

- Important in-memory state:
  - runtime manifest cache
  - replay snapshot
  - open writer state
  - `transferIntentActive_`
- Important on-disk or external state:
  - `manifest`
  - active, pending, and obsolete index/data files
  - takeover plan and import log
  - source recovery directories
  - ETCD-backed slot recovery task state
- Ownership and lifecycle rules:
  - one worker namespace owns one home slot path at a time
  - one `Slot` instance manages one local slot directory
  - takeover temporarily moves source data into a recovery path before target publication
- Concurrency or thread-affinity rules:
  - `Slot::mu_` serializes local slot state changes
  - `SlotClient::mu_` guards lazy slot construction only
  - background compaction runs concurrently with request paths but must yield to takeover intent
- Ordering requirements or invariants:
  - import records are replayed only from closed batches
  - `deleteAllVersion=true` maps to tombstone version `UINT64_MAX`
  - takeover must fence and rename source before target import publication
  - every caller must use the same fixed slot hash rule

## External Interaction And Dependency Analysis

- Dependency graph summary:
  - request paths -> `AggregatedPersistenceApi` -> `SlotClient` -> filesystem
  - worker recovery -> `SlotRecoveryManager` -> `PreloadSlot()` -> metadata recovery
- Critical upstream services or modules:
  - worker object-cache request pipeline
  - worker recovery and migration services
  - `PersistenceApi` backend factory
- Critical downstream services or modules:
  - filesystem operations for rename, fsync, and file enumeration
  - slot recovery store and ETCD-backed task state
  - metadata recovery manager
- Failure impact from each critical dependency:
  - filesystem errors can block writes, replay, compaction, or takeover
  - recovery-store errors can delay or skip restart and failed-worker recovery planning
  - metadata recovery errors can leave payloads recovered but worker metadata incomplete
- Version, protocol, or schema coupling:
  - slot manifest and index file format are compatibility surfaces
  - `slot_recovery.proto` shapes worker recovery task exchange
- Deployment or operations dependencies:
  - stable `distributed_disk_path`, `cluster_name`, and `worker_address`
  - shared access to distributed-disk roots by the relevant workers

## Open Source Software Selection And Dependency Record

| Dependency | Purpose in this module | Why selected | Alternatives considered | License | Version / upgrade strategy | Security / maintenance notes |
| --- | --- | --- | --- | --- | --- | --- |
| none unique to slot | slot relies on repository baseline C++/filesystem/protobuf stack | no slot-specific third-party package is selected in this layer | N/A | N/A | follow repository-wide dependency policy | review inherited filesystem and protobuf usage through normal repo maintenance |

## Configuration Model

| Config | Type | Default or source | Effect | Risk if changed |
| --- | --- | --- | --- | --- |
| `l2_cache_type` | string | `none` | must be `distributed_disk` for any slot path to exist | wrong value disables slot persistence and recovery |
| `distributed_disk_path` | string | empty by default | root used to build worker-scoped slot roots | wrong path strands or mixes slot data |
| `cluster_name` | string | empty -> `default` in path builder | cluster segment in slot root path | changing it redirects the worker to another slot tree |
| `worker_address` | string | runtime identity | sanitized into worker namespace | unstable values change ownership path |
| `distributed_disk_max_data_file_size_mb` | uint32 | `1024` | rolling-file size and dedicated-file threshold | too small increases file churn; too large slows compaction benefit |
| `distributed_disk_sync_interval_ms` | uint32 | `1000` | max buffered time before flush | larger values increase crash loss window |
| `distributed_disk_sync_batch_bytes` | uint64 | `33554432` | max buffered bytes before flush | larger values increase replay work and loss window |
| `distributed_disk_compact_interval_s` | uint32 | `3600` | background compaction cadence | too small increases IO churn; too large leaves stale files longer |

## Examples And Migration Notes

- Example API requests or responses to retain:
  - `PreloadSlot(sourceWorker, slotId, callback)` for recovery
  - `MergeSlot(sourceWorker, slotId)` for migration handoff
- Example configuration or deployment snippets to retain:
  - a worker-local namespace is derived from `distributed_disk_path`, `cluster_name`, and `worker_address`
- Example operator workflows or commands to retain:
  - inspect local slot roots when debugging replay or startup repair
  - correlate recovery logs with slot-recovery task state when restart recovery stalls
- Upgrade, rollout, or migration examples if behavior changes:
  - changes to manifest or index semantics should be treated as persisted-format rollouts, not as local refactors

## Availability

- Availability goals or service-level expectations:
  - keep distributed-disk persistence recoverable after worker restart or worker ownership transfer.
- Deployment topology and redundancy model:
  - one active worker namespace owns one slot-home path; takeover moves ownership to another worker namespace.
- Single-point-of-failure analysis:
  - there is no in-slot replica; underlying disk availability and successful takeover are critical.
- Failure domains and blast-radius limits:
  - damage is scoped per slot directory as much as possible through replay and manifest-local recovery.
- Health checks, readiness, and traffic removal conditions:
  - the slot layer itself has no dedicated readiness endpoint; worker readiness remains higher-level.
- Failover, switchover, or recovery entry conditions:
  - startup repair runs on process start;
  - restart and failed-worker recovery enter through `SlotRecoveryManager`;
  - migration switchover enters through `MergeSlot()`.
- Capacity reservation or headroom assumptions:
  - compaction and takeover assume enough disk headroom for pending files and moved source data during transition.

## Reliability

- Expected failure modes:
  - torn index tails
  - interrupted compaction
  - interrupted takeover after source rename or import begin
  - stale or wrong worker namespace root
  - metadata recovery failure after payload preload
- Data consistency, durability, or correctness requirements:
  - active index plus replay snapshot must reconstruct the visible object set;
  - import batches must be atomic from replay visibility perspective;
  - target publication must not expose incomplete takeover state.
- Retry, idempotency, deduplication, or exactly-once assumptions:
  - compaction and takeover are restartable through manifest recovery rather than exactly-once RPC guarantees;
  - replay deduplicates visibility by key/version semantics and delete watermarks.
- Partial-degradation behavior:
  - background compaction can yield while reads and writes continue;
  - local repair may proceed even when broader recovery planning is delayed.
- Recovery, replay, repair, or reconciliation strategy:
  - CRC-based index scan truncates torn tails;
  - manifest recovery resolves unfinished compaction or transfer;
  - `SlotRecoveryOrchestrator` repairs local slots at startup;
  - `SlotRecoveryManager` replays payloads and metadata on restart or failure recovery.
- RTO, RPO, MTTR, or equivalent recovery targets:
  - no explicit repo-level RTO or MTTR target is encoded in source;
  - crash-loss window is bounded by `distributed_disk_sync_interval_ms` and `distributed_disk_sync_batch_bytes`.
- What must remain true after failure:
  - only closed import batches are visible;
  - source and target ownership do not simultaneously publish conflicting slot state;
  - worker namespace isolation is preserved.

## Resilience

- Overload protection, rate limiting, or admission control:
  - no dedicated slot-layer rate limiter is implemented.
- Timeout, circuit-breaker, and backpressure strategy:
  - request-path timeout arguments exist but are currently ignored by the slot path;
  - compaction and takeover use `K_TRY_AGAIN` to avoid unsafe concurrent progress.
- Dependency failure handling:
  - filesystem and recovery-store errors surface as status failures and logs.
- Graceful-degradation behavior:
  - background compaction can skip or defer work when transfer intent is active;
  - local preload shortcut avoids unnecessary takeover for same-worker reads.
- Safe rollback, feature-gating, or kill-switch strategy:
  - slot behavior is gated by `l2_cache_type=distributed_disk`.
- Attack or fault scenarios the module should continue to tolerate:
  - torn-tail writes
  - interrupted compaction
  - interrupted takeover after durable plan creation
  - worker restart with pending local recovery tasks

## Security, Privacy, And Safety Constraints

- Authn/authz expectations:
  - none inside the slot layer; access control is delegated to process and filesystem environment.
- Input validation requirements:
  - correct worker namespace sanitization and stable slot ids are required for safe path derivation.
- Data sensitivity or privacy requirements:
  - object payloads stored on disk inherit sensitivity from higher layers; no redaction occurs in slot files.
- Secrets, credentials, or key-management requirements:
  - no slot-specific secret handling is implemented.
- Resource or isolation constraints:
  - slot roots must remain isolated per worker namespace to avoid cross-worker contamination.
- Unsafe changes to avoid:
  - changing hash rules, manifest semantics, or takeover ordering without compatibility review.
- Safety constraints for human, environment, or property impact:
  - none beyond standard data-integrity expectations in this repository.

## Observability

- Main logs:
  - `SlotClient` background compact logs
  - `Slot` save/get/delete/compact/takeover logs and warnings
  - `SlotRecoveryManager` planning and execution logs
- Metrics:
  - no dedicated slot metrics surface is exposed today
- Traces or correlation fields:
  - recovery paths use trace IDs through `Trace::Instance().SetTraceUUID()`
- Alerts or SLO-related signals:
  - none slot-specific in source; operators rely on logs and higher-level worker health signals
- Debug hooks or inspection commands:
  - inject points prefixed with `slotstore.`
  - inspect worker-scoped slot roots and manifest/index files during incident diagnosis
- How to tell the module is healthy:
  - startup repair completes without manifest or replay errors
  - compaction runs without repeated `K_TRY_AGAIN` or cutover failures
  - recovery incidents converge with completed slot counts and no failed slots

## Compatibility And Invariants

- External compatibility constraints:
  - existing slot directories, manifests, and indexes must remain replayable after compatible changes.
- Internal invariants:
  - writable steady state is `NORMAL/NONE/NONE/NONE`
  - only closed import batches are visible
  - source rename precedes target import publication during takeover
  - fixed slot hash rule uses `DISTRIBUTED_DISK_SLOT_NUM = 128`
- File format, wire format, or schema stability notes:
  - index record semantics and manifest field meanings are persisted-format contracts;
  - `slot_recovery.proto` shapes recovery coordination state.
- Ordering or naming stability notes:
  - worker-scoped roots, `slot_0000` directory naming, and `index_import_<txn>.log` / `takeover_<txn>.plan` patterns
    are part of the on-disk operational model.
- Upgrade, downgrade, or mixed-version constraints:
  - Pending verification:
    - explicit mixed-version upgrade guarantees are not documented in current source.

## Performance Characteristics

- Hot paths:
  - `Save()`
  - `Get()`
  - `Delete()`
- Known expensive operations:
  - compaction
  - takeover planning and file movement
  - large-payload dedicated file handling
  - full replay during repair
- Buffering, batching, or async behavior:
  - group commit flushes on time or byte thresholds
  - background compaction runs asynchronously
  - snapshot-first reads avoid holding the slot mutex during payload file read
- Resource limits or scaling assumptions:
  - slot count is fixed at `128`
  - disk headroom is needed for compact pending files and takeover staging

## Build, Test, And Verification

- Build entrypoints:
  - `src/datasystem/common/l2cache/CMakeLists.txt`
  - `src/datasystem/common/l2cache/BUILD.bazel`
- Fast verification commands:
  - build `common_slot_client` and touched worker object-cache targets
  - run focused slot-store and slot-recovery tests
- Representative unit, integration, and system tests:
  - `tests/ut/common/l2cache/slot_store_test.cpp`
  - `tests/ut/worker/object_cache/slot_worker_integration_test.cpp`
  - `tests/st/worker/object_cache/slot_end2end_test.cpp`
  - `tests/ut/worker/object_cache/slot_recovery_manager_test.cpp`
  - `tests/st/worker/object_cache/slot_recovery_manager_test.cpp`
- Recommended validation for risky changes:
  - replay and torn-tail recovery
  - compaction cutover and rollback
  - merge and preload takeover
  - worker restart and failed-worker recovery

## Self-Verification Cases

| ID | Test scenario | Purpose | Preconditions | Input / steps | Expected result |
| --- | --- | --- | --- | --- | --- |
| `slot-01` | torn index tail replay | verify crash-tolerant replay | slot files with valid records plus damaged tail | run repair or reopen slot | replay truncates tail and preserves last valid state |
| `slot-02` | compaction versus transfer intent | verify unsafe concurrent cutover is blocked | one slot compacting and transfer intent injected | trigger compaction and takeover overlap | compaction exits or retries safely without corrupting active state |
| `slot-03` | preload recovery after worker restart | verify payload and metadata rebuild | persisted slot data plus recovery task | run `HandleLocalRestart()` and recovery execution | payloads are preloaded and metadata recovery is invoked |

## Common Change Scenarios

### Adding A New Capability

- Recommended extension point:
  - choose the narrowest layer among `Slot`, `SlotWriter`, `SlotCompactor`, `SlotTakeoverPlanner`, or recovery manager.
- Required companion updates:
  - update this design doc, `slot/README.md`, and the slot feature playbook
  - update `l2cache/l2-cache-type.md` only if backend-facing semantics change
- Review checklist:
  - confirm persisted-format impact
  - confirm recovery ordering impact
  - confirm DFX hooks or tests still cover the new path

### Modifying Existing Behavior

- Likely breakpoints:
  - manifest transitions
  - replay semantics
  - source/target takeover ordering
  - worker namespace path derivation
- Compatibility checks:
  - existing slot files remain replayable
  - import visibility rules remain unchanged unless explicitly redesigned
- Observability checks:
  - relevant logs still identify slot id, worker namespace, and recovery phase
  - inject-driven tests still cover the changed failure point

### Debugging A Production Issue

- First files to inspect:
  - `slot.cpp`
  - `slot_manifest.cpp`
  - `slot_recovery_manager.cpp`
  - `worker_oc_server.cpp`
- First runtime evidence to inspect:
  - slot manifests and active indexes on disk
  - startup and recovery logs
  - slot-recovery incident or task state
- Common misleading symptoms:
  - metadata loss that is actually payload preload failure
  - compaction churn that is really takeover preemption
  - missing imported data caused by incomplete import batch publication

## Open Questions

- whether the slot layer should expose first-class metrics and health surfaces instead of relying on logs and tests;
- whether manifest or index evolution needs an explicit mixed-version rollout playbook in the future.
