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
  - `src/datasystem/common/l2cache/obs_client/*`
  - `src/datasystem/common/l2cache/sfs_client/*`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Primary source-of-truth files:
  - same as the primary code paths above
- Last verified against source:
  - `2026-11-03`
- Related context docs:
  - `modules/infra/l2cache/README.md`
  - `modules/infra/l2cache/l2-cache-type.md`
  - `modules/runtime/worker-runtime.md`
- Related playbooks:
  - `playbooks/features/infra/l2cache/implementation.md`
## Purpose

- Why this design document exists:
  - provide one architecture-level reference for `l2_cache_type` before readers dive into backend-specific details.
- What problem this module solves:
  - offer one shared persistence surface for worker L2 spill paths while allowing different storage backends with
    different semantics behind a parent routing layer.
- Who or what depends on this module:
  - worker object-cache persistence flows;
  - worker startup initialization;

## Business And Scenario Overview

- Why this capability is needed:
  - object cache needs an L2 persistence layer, but one storage model does not fit all deployments.
- Target users, callers, or operators:
  - worker runtime developers, storage-backend maintainers, and operators who configure secondary storage.
- Typical usage time or trigger conditions:
  - every L2 save/get/delete request;
  - worker startup when L2 is enabled;
- Typical deployment or runtime environment:
  - workers using OBS object storage or mounted SFS directories.
- What other functions or modules it may affect:
  - `worker_oc_server` startup;
  - object-cache metadata recovery;
  - migration RPC handling;
  - shared gflags validation.

## Goals

- keep caller-facing persistence APIs stable across multiple backends;
- separate backend selection from backend-specific implementation details;
- support worker restart and failed-worker recovery only where the backend format can actually provide it.

## Non-Goals

- making every backend feature-equivalent;
- providing a backend-agnostic success-rate or timeout implementation with identical semantics today.

## Scope

- In scope:
  - `l2_cache_type` selection and validation;
  - `PersistenceApi` factory and shared surface;
  - OBS and SFS object-per-version persistence;
- Out of scope:
  - the upstream async pipelines that decide when objects are persisted;
  - deployment-specific cloud or filesystem provisioning.
- Important boundaries with neighboring modules:
  - `common/l2cache` owns backend choice and persistence implementation.
  - `worker/object_cache` owns restart and failed-worker recovery orchestration on top of the persistence API.
  - `common/flags/*` owns flag declaration and validation.

## Terminology

| Term | Meaning in this repository | Source or note |
| --- | --- | --- |
| `l2_cache_type` | gflag selecting the active secondary-storage backend | `common_flag_define.cpp`, `validator.h` |
| `PersistenceApi` | shared persistence interface plus backend factory | `persistence_api.h/.cpp` |
| `ObjectPersistenceApi` | object-per-version adapter for OBS and SFS | `object_persistence_api.h/.cpp` |
| `ObsClient` | OBS REST client used by the object-per-version path | `obs_client/*` |
| `SfsClient` | mounted-filesystem client used by the object-per-version path | `sfs_client/*` |

## Current State And Design Choice Notes

- Current implementation or baseline behavior:
  - supported backend values are `obs`, `sfs`, and `none`.
  - OBS and SFS keep one encoded object directory plus per-version object/file storage.
- Selected approach and why:
  - one shared API keeps callsites stable while allowing the implementation split to follow the real storage model.
- Known tradeoffs:
  - OBS and SFS differ in timeout, listing, and request-rate reporting behavior.

## Architecture Overview

- High-level structure:
  - `GetCurrentStorageType()` and `IsSupportL2Storage()` interpret `l2_cache_type`.
  - `PersistenceApi::Create()` chooses the backend family.
  - `ObjectPersistenceApi` chooses `ObsClient` or `SfsClient`.
- Key runtime roles or processes:
  - request paths call `Save`, `Get`, `GetWithoutVersion`, and `Del`;
- Key persistent state, if any:
  - OBS objects under one bucket;
  - SFS files under `<sfs_path>/datasystem`;

## Scenario Analysis

- Primary user or system scenarios:
  - a worker persists one object version into OBS or SFS through the object-per-version path;
- Normal path summary:
  - the worker selects one backend from `l2_cache_type`, initializes `PersistenceApi`, and persists or reads objects
    through the shared API.
- Exceptional path summary:
  - invalid `l2_cache_type` values fail flag validation; direct helper lookup maps unrecognized values to `NONE`.

## Entry Points, External Interfaces, And Integration Points

- Public APIs:
  - `PersistenceApi::Create()`
  - `PersistenceApi::CreateShared()`
  - `PersistenceApi::{Init,Save,Get,GetWithoutVersion,Del}`
  - `GetCurrentStorageType()`
  - `IsSupportL2Storage()`
- Internal service entrypoints:
  - `worker::WorkerOCServer::Init()`
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

## Backend Comparison

| Backend | Persistence model | Recovery model | Main risk surface |
| --- | --- | --- | --- |
| `obs` | one encoded object key plus per-version object in OBS | per-object reads and deletes only | auth, endpoint, multipart upload, request-rate visibility |
| `sfs` | one encoded object key plus per-version file under mounted SFS | per-object reads and deletes only | mount correctness, rename atomicity, local directory scans |

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| `Validator::ValidateL2CacheType` | validate accepted backend names | `common/util/validator.h`, `common_flags_validate.cpp` | source of allowed values |
| `l2_storage` helpers | interpret flag values into `L2StorageType` | `l2_storage.h/.cpp` | returns `NONE` for unsupported values |
| `PersistenceApi` | factory plus common interface | `persistence_api.h/.cpp` | shared entrypoint for callers |
| `ObjectPersistenceApi` | OBS/SFS dispatch and version handling | `object_persistence_api.h/.cpp` | object-per-version layout |
| `ObsClient` | OBS REST implementation | `obs_client/*` | token rotation optional |
| `SfsClient` | local or mounted filesystem implementation | `sfs_client/*` | rename-on-publish |

## Critical Invariants

- Preserve encoded object paths and version fallback across Save/Get/List/Del.
- SFS publishes a completed upload by rename; incomplete files must remain distinguishable.
- Persistence errors must propagate to async-write retry and shutdown drain owners.

## Failure And Recovery Model

- OBS and SFS:
  - rely on backend-specific object listing, upload, download, and delete behavior;

## Review And Change Guardrails

- Changes usually need design-first review when:
  - adding a new `l2_cache_type`;
  - changing `PersistenceApi::Create()` dispatch rules;
  - changing shared API semantics that differ by backend.
- Must verify in source before claiming:
  - accepted backend names and validation behavior;

## Related Deep Dives

- `l2-cache-type.md`
  - brief backend summary, flags, and dispatch rules

## Open Questions

- whether the shared async send/delete pipeline should get its own context doc under `l2cache/`.

## Scale-In Compatibility

`OCMetadataManager::ProcessGiveUpPrimaryObject` no longer classifies every L2 primary as requiring an L2-specific
handoff. This affects OBS/SFS write-through and write-back modes as well: an eligible surviving copy receives the
primary role; without a copy, ordinary data migration applies and write-back modes request a completion wait.
The scale-in drain and migration owners must complete pending writes before source cleanup. Missing source payload
must remain a failure/retry rather than be counted as successful migration. Master classification tests cover response
selection only; runtime scale-in, write-back completion, restart, and data-read validation are separate evidence.
