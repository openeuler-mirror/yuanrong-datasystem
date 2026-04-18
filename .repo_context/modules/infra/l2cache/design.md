# L2 Cache And Secondary Storage Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - `current implementation`
- Primary code paths:
  - `src/datasystem/common/l2cache/l2_storage.*`
  - `src/datasystem/common/l2cache/persistence_api.*`
  - `src/datasystem/common/l2cache/object_persistence_api.*`
  - `src/datasystem/common/l2cache/aggregated_persistence_api.*`
  - `src/datasystem/common/l2cache/l2cache_client.*`
  - `src/datasystem/common/l2cache/obs_client/*`
  - `src/datasystem/common/l2cache/sfs_client/*`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Primary source-of-truth files:
  - same as the primary code paths above
- Last verified against source:
  - `2026-04-17`
- Related context docs:
  - `modules/infra/l2cache/README.md`
  - `modules/infra/l2cache/l2-cache-type.md`
  - `modules/infra/slot/README.md`
  - `modules/infra/slot/design.md`
  - `modules/runtime/worker-runtime.md`
- Related playbooks:
  - `playbooks/features/infra/l2cache/implementation.md`
  - `playbooks/features/infra/slot/implementation.md`
- Related user-facing or internal docs:
  - `slot/slot_final.md`
  - Note:
    - `slot/slot_final.md` is useful background, but current code is the final source of truth.

## Purpose

- Why this design document exists:
  - provide one architecture-level reference for `l2_cache_type` before readers dive into backend-specific details.
- What problem this module solves:
  - offer one shared persistence surface for worker L2 spill paths while allowing different storage backends with
    different semantics behind a parent routing layer.
- Who or what depends on this module:
  - worker object-cache persistence flows;
  - worker startup initialization;
  - backend-specific modules such as the standalone slot module.

## Business And Scenario Overview

- Why this capability is needed:
  - object cache needs an L2 persistence layer, but one storage model does not fit all deployments.
- Target users, callers, or operators:
  - worker runtime developers, storage-backend maintainers, and operators who configure secondary storage.
- Typical usage time or trigger conditions:
  - every L2 save/get/delete request;
  - worker startup when L2 is enabled;
  - slot migration or recovery when `l2_cache_type=distributed_disk`.
- Typical deployment or runtime environment:
  - workers running with either object-store backed persistence (`obs`), mounted shared storage (`sfs`), or local or
    shared disks configured as distributed-disk slot storage.
- What other functions or modules it may affect:
  - `worker_oc_server` startup;
  - object-cache metadata recovery;
  - migration RPC handling;
  - shared gflags validation.

## Goals

- keep caller-facing persistence APIs stable across multiple backends;
- separate backend selection from backend-specific implementation details;
- allow object-per-version backends and slot-aggregated backends to coexist behind one interface;
- support worker restart and failed-worker recovery only where the backend format can actually provide it.

## Non-Goals

- making every backend feature-equivalent;
- hiding all operational differences between OBS, SFS, and distributed-disk;
- providing a backend-agnostic success-rate or timeout implementation with identical semantics today.

## Scope

- In scope:
  - `l2_cache_type` selection and validation;
  - `PersistenceApi` factory and shared surface;
  - OBS and SFS object-per-version persistence;
  - parent-level integration of the distributed-disk slot backend.
- Out of scope:
  - the upstream async pipelines that decide when objects are persisted;
  - detailed slot manifest/index/compaction/recovery semantics;
  - detailed object-cache metadata semantics outside slot recovery callbacks;
  - deployment-specific cloud or filesystem provisioning.
- Important boundaries with neighboring modules:
  - `common/l2cache` owns backend choice and persistence implementation.
  - `modules/infra/slot/*` owns the distributed-disk slot persistence format, recovery, and DFX model as a standalone
    module context.
  - `worker/object_cache` owns restart and failed-worker recovery orchestration on top of the persistence API.
  - `common/util/gflag/*` owns flag declaration and validation.

## Terminology

| Term | Meaning in this repository | Source or note |
| --- | --- | --- |
| `l2_cache_type` | gflag selecting the active secondary-storage backend | `common_gflag_define.cpp`, `validator.h` |
| `PersistenceApi` | shared persistence interface plus backend factory | `persistence_api.h/.cpp` |
| `ObjectPersistenceApi` | object-per-version adapter for OBS and SFS | `object_persistence_api.h/.cpp` |
| `AggregatedPersistenceApi` | adapter from `PersistenceApi` to a `StorageClient` slot backend | `aggregated_persistence_api.h/.cpp` |
| `ObsClient` | OBS REST client used by the object-per-version path | `obs_client/*` |
| `SfsClient` | mounted-filesystem client used by the object-per-version path | `sfs_client/*` |
| `SlotClient` | distributed-disk router from object key to fixed slot | `slot_client/slot_client.*` |
| `SlotRecoveryManager` | worker recovery coordinator using `PreloadSlot()` | `slot_recovery_manager.*` |

## Current State And Design Choice Notes

- Current implementation or baseline behavior:
  - the repository currently supports `obs`, `sfs`, `distributed_disk`, and `none`.
  - `PersistenceApi::Create()` sends `distributed_disk` through `AggregatedPersistenceApi(SlotClient)` and routes the
    other supported backends through `ObjectPersistenceApi`.
  - OBS and SFS keep one encoded object directory plus per-version object/file storage.
  - distributed-disk keeps worker-scoped slot directories under `distributed_disk_path` and adds takeover plus recovery
    support.
- Selected approach and why:
  - one shared API keeps callsites stable while allowing the implementation split to follow the real storage model.
  - distributed-disk is intentionally separate because slot takeover and restart repair require different on-disk
    invariants than object-per-version storage.
- Known tradeoffs:
  - common API surface does not mean identical semantics: `MergeSlot()` and `PreloadSlot()` are only valid for
    distributed-disk.
  - `timeoutMs` is exposed on the API but currently ignored by `SlotClient`.
  - request success-rate reporting is richer on OBS than on distributed-disk.

## Architecture Overview

- High-level structure:
  - `GetCurrentStorageType()` and `IsSupportL2Storage()` interpret `l2_cache_type`.
  - `PersistenceApi::Create()` chooses the backend family.
  - `ObjectPersistenceApi` chooses `ObsClient` or `SfsClient`.
  - `AggregatedPersistenceApi` forwards persistence calls into `SlotClient`.
  - worker runtime layers only enable slot worker-namespace and slot recovery code for `distributed_disk`.
- Key runtime roles or processes:
  - request paths call `Save`, `Get`, `GetWithoutVersion`, and `Del`;
  - worker startup initializes persistence and possibly slot recovery;
  - migration service calls `MergeSlot`;
  - recovery manager calls `PreloadSlot`.
- Key persistent state, if any:
  - OBS objects under one bucket;
  - SFS files under `<sfs_path>/datasystem`;
  - distributed-disk slot store under
    `<distributed_disk_path>/datasystem/<cluster_name>/slot_store/<worker_namespace>/`.

## Scenario Analysis

- Primary user or system scenarios:
  - a worker persists one object version into OBS or SFS through the object-per-version path;
  - a worker persists many object versions into distributed-disk slots and later compacts them;
  - a restarted worker repairs its own distributed-disk slot directories;
  - a worker takes over slots from another worker during migration or failed-worker recovery.
- Normal path summary:
  - the worker selects one backend from `l2_cache_type`, initializes `PersistenceApi`, and persists or reads objects
    through the shared API.
- Exceptional path summary:
  - unsupported `l2_cache_type` behaves like no L2 storage;
  - distributed-disk recovery paths may reject or defer work when manifest state is mid-transfer or compaction;
  - OBS and SFS deliberately reject slot-specific operations as unsupported.

## Entry Points, External Interfaces, And Integration Points

- Public APIs:
  - `PersistenceApi::Create()`
  - `PersistenceApi::CreateShared()`
  - `PersistenceApi::{Init,Save,Get,GetWithoutVersion,Del,PreloadSlot,MergeSlot}`
  - `GetCurrentStorageType()`
  - `IsSupportL2Storage()`
- Internal service entrypoints:
  - `worker::WorkerOCServer::Init()`
  - `worker::WorkerOCServer::InitSlotWorkerNamespace()`
  - `worker::WorkerOCServer::InitSlotRecovery()`
  - `object_cache::SlotRecoveryManager::ExecuteRecoveryTask()`
  - `object_cache::WorkerOcServiceMigrateImpl`
- Config flags or environment variables:
  - shared:
    - `l2_cache_type`
    - `cluster_name`
    - `worker_address`
  - OBS:
    - `obs_access_key`
    - `obs_secret_key`
    - `obs_endpoint`
    - `obs_bucket`
    - `obs_https_enabled`
    - `enable_cloud_service_token_rotation`
  - SFS:
    - `sfs_path`
  - distributed-disk:
    - `distributed_disk_path`
    - `distributed_disk_max_data_file_size_mb`
    - `distributed_disk_sync_interval_ms`
    - `distributed_disk_sync_batch_bytes`
    - `distributed_disk_compact_interval_s`

## Backend Comparison

| Backend | Persistence model | Recovery model | Slot operations | Main risk surface |
| --- | --- | --- | --- | --- |
| `obs` | one encoded object key plus per-version object in OBS | per-object reads and deletes only | unsupported | auth, endpoint, multipart upload, request-rate visibility |
| `sfs` | one encoded object key plus per-version file under mounted SFS | per-object reads and deletes only | unsupported | mount correctness, rename atomicity, local directory scans |
| `distributed_disk` | fixed-slot aggregated index/data files under worker namespace | local repair plus worker-to-worker takeover | supported | manifest correctness, slot replay, recovery, path ownership |

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| `Validator::ValidateL2CacheType` | validate accepted backend names | `common/util/validator.h`, `common_gflags_validate.cpp` | source of allowed values |
| `l2_storage` helpers | interpret flag values into `L2StorageType` | `l2_storage.h/.cpp` | returns `NONE` for unsupported values |
| `PersistenceApi` | factory plus common interface | `persistence_api.h/.cpp` | shared entrypoint for callers |
| `ObjectPersistenceApi` | backend dispatch for OBS and SFS | `object_persistence_api.h/.cpp` | `MergeSlot` and `PreloadSlot` unsupported |
| `AggregatedPersistenceApi` | forwarder to slot backend | `aggregated_persistence_api.h/.cpp` | used only for distributed-disk |
| `ObsClient` | OBS REST implementation | `obs_client/*` | token rotation optional |
| `SfsClient` | local or mounted filesystem implementation | `sfs_client/*` | rename-on-publish |
| `SlotClient` | distributed-disk slot router | `slot_client/*` | fixed `128` slots |

## Critical Invariants

- `l2_cache_type=distributed_disk` must use `distributed_disk_path`, not `sfs_path`.
- `MergeSlot()` and `PreloadSlot()` must stay unsupported for OBS and SFS unless caller contracts change.
- distributed-disk slot ownership is worker-namespace scoped, so `worker_address` sanitization must stay consistent
  across startup, migration, and recovery.
- every caller must hash keys with the same fixed slot-count rule because the source currently hardcodes `128` slots.
- worker startup must not initialize slot recovery for non-distributed-disk backends.

## Failure And Recovery Model

- OBS and SFS:
  - rely on backend-specific object listing, upload, download, and delete behavior;
  - do not expose worker-level slot takeover or replay semantics through the shared API.
- distributed-disk:
  - keeps durable manifest plus index/data files per slot;
  - repairs unfinished local slot state on startup;
  - supports local restart and failed-worker recovery by replaying imported slot data through `PreloadSlot()`;
  - supports post-migration durable handoff through `MergeSlot()`.

## Review And Change Guardrails

- Changes usually need design-first review when:
  - adding a new `l2_cache_type`;
  - changing `PersistenceApi::Create()` dispatch rules;
  - changing path derivation for OBS, SFS, or distributed-disk;
  - changing slot ownership, takeover, or recovery semantics;
  - changing shared API semantics that differ by backend.
- Must verify in source before claiming:
  - accepted backend names and validation behavior;
  - whether a change is object-per-version or slot-aggregated;
  - which worker lifecycle path enables or skips slot recovery.

## Related Deep Dives

- `l2-cache-type.md`
  - brief backend summary, flags, and dispatch rules
- `../slot/design.md`
  - standalone slot design, including recovery, availability, reliability, resilience, and DFX

## Open Questions

- whether the shared async send/delete pipeline should get its own context doc under `l2cache/`.
- whether distributed-disk should expose a non-empty request success-rate summary through the shared API.
