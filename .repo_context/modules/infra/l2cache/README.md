# L2 Cache And Secondary Storage

## Scope

- Paths:
  - `src/datasystem/common/l2cache`
  - `src/datasystem/common/l2cache/obs_client`
  - `src/datasystem/common/l2cache/sfs_client`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Why this module exists:
  - provide backend selection and the shared persistence surface behind `l2_cache_type`;
  - keep object-per-version OBS/SFS and the distributed-disk slot backend behind one `PersistenceApi` family;
  - route readers from parent L2 semantics into the standalone slot module when distributed-disk internals matter.
- Primary source files to verify against:
  - `src/datasystem/common/l2cache/CMakeLists.txt`
  - `src/datasystem/common/l2cache/l2_storage.*`
  - `src/datasystem/common/l2cache/persistence_api.*`
  - `src/datasystem/common/l2cache/object_persistence_api.*`
  - `src/datasystem/common/l2cache/aggregated_persistence_api.*`
  - `src/datasystem/common/l2cache/sfs_client/sfs_client.*`
  - `src/datasystem/common/l2cache/obs_client/obs_client.*`
  - `src/datasystem/worker/worker_oc_server.cpp`

## Read Order Inside This Area

- If the question is about architecture boundaries, backend capability differences, worker integration, or what kind of
  change needs design-first treatment:
  - read `design.md`
- If the question is about backend selection, flags, `PersistenceApi` dispatch, or the differences between `obs`,
  `sfs`, and `distributed_disk`:
  - read `l2-cache-type.md`
- If the question is about distributed-disk slot internals, replay/repair, compaction, takeover, or restart recovery:
  - read `../slot/design.md`
- If the question is about how to implement or review an L2 or secondary-storage feature safely:
  - read `../../../playbooks/features/infra/l2cache/implementation.md`

## Responsibilities Overview

- Verified:
  - `l2_cache_type` currently accepts `obs`, `sfs`, `distributed_disk`, and `none`.
  - `PersistenceApi::Create()` dispatches `distributed_disk` to `AggregatedPersistenceApi(SlotClient)` and routes the
    other enabled backends through `ObjectPersistenceApi`.
  - OBS and SFS remain object-per-version backends and do not implement `MergeSlot` or `PreloadSlot`.
  - distributed-disk uses worker-scoped slot directories rooted under `distributed_disk_path`, not `sfs_path`.
  - worker restart and failed-worker slot recovery are only enabled when `FLAGS_l2_cache_type == "distributed_disk"`.
- Pending verification:
  - whether a dedicated, non-empty request success-rate report is planned for distributed-disk, because
    `SlotClient::GetRequestSuccessRate()` currently returns an empty string.

## Subdocuments

- `design.md`
  - architecture-level overview for backend selection, shared API surface, and parent-module boundaries
- `l2-cache-type.md`
  - backend selection, flags, dispatch rules, and the brief OBS/SFS versus distributed-disk comparison

## Related Sibling Module

- `../slot/README.md`
  - standalone distributed-disk slot module covering persistence format, recovery, reliability, availability, and DFX

## Matching Playbook

- `../../../playbooks/features/infra/l2cache/implementation.md`
  - feature workflow and risk gates for backend selection, `PersistenceApi`, and OBS/SFS or cross-backend changes
- `../../../playbooks/features/infra/slot/implementation.md`
  - feature workflow for slot manifest/index/compaction/takeover/recovery changes

## Main Components

| Component | Verified role | Details live primarily in |
| --- | --- | --- |
| `PersistenceApi` | shared persistence interface and backend factory | `l2-cache-type.md` |
| `ObjectPersistenceApi` | object-per-version persistence facade for OBS and SFS | `l2-cache-type.md` |
| `AggregatedPersistenceApi` | adapter from `PersistenceApi` into `StorageClient` slot backends | `l2-cache-type.md` |
| `ObsClient` | HTTP REST OBS client with AK/SK or token rotation auth | `l2-cache-type.md` |
| `SfsClient` | mounted-filesystem client using rename-based object publishing | `l2-cache-type.md` |
| `SlotClient` / `Slot` | distributed-disk slot router and per-slot storage manager | `../slot/design.md` |
| `SlotRecoveryOrchestrator` | worker-startup repair of the current worker namespace | `../slot/design.md` |
| `SlotRecoveryManager` | ETCD-coordinated failed-worker and restart recovery executor | `../slot/design.md` |

## Flags And Config Overview

- Backend selection:
  - `l2_cache_type`
- OBS:
  - `obs_access_key`
  - `obs_secret_key`
  - `obs_endpoint`
  - `obs_bucket`
  - `obs_https_enabled`
  - `enable_cloud_service_token_rotation`
- SFS:
  - `sfs_path`
- Distributed disk:
  - `distributed_disk_path`
  - `distributed_disk_max_data_file_size_mb`
  - `distributed_disk_sync_interval_ms`
  - `distributed_disk_sync_batch_bytes`
  - `distributed_disk_compact_interval_s`
  - `cluster_name`
  - `worker_address` through slot worker-namespace sanitization

## Cross-Module Coupling

- L2 cache depends on:
  - `common_util`
  - `common_log`
  - `common_inject`
  - `common_httpclient`
  - `common_metrics` for OBS response-code accounting
  - shared gflags under `src/datasystem/common/util/gflag/*`
- The standalone slot module is consumed by:
  - worker startup in `src/datasystem/worker/worker_oc_server.cpp`
  - worker restart and failed-worker recovery in `src/datasystem/worker/object_cache/slot_recovery/*`
  - worker migration service in `src/datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.cpp`

## Review And Bugfix Notes

- Common change risks:
  - changing `PersistenceApi::Create()` changes semantics for every worker path that spills to L2;
  - distributed-disk, OBS, and SFS share one API surface but do not share the same recovery guarantees or timeout
    behavior.
- Good first files when backend choice or L2 behavior looks wrong:
  - `src/datasystem/common/l2cache/persistence_api.cpp`
  - `src/datasystem/common/l2cache/object_persistence_api.cpp`
  - `src/datasystem/common/l2cache/aggregated_persistence_api.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
