# L2 Cache Type And Backend Selection

## Scope

- Paths:
  - `src/datasystem/common/l2cache/l2_storage.*`
  - `src/datasystem/common/l2cache/persistence_api.*`
  - `src/datasystem/common/l2cache/object_persistence_api.*`
  - `src/datasystem/common/l2cache/aggregated_persistence_api.*`
  - `src/datasystem/common/l2cache/l2cache_client.*`
  - `src/datasystem/common/l2cache/sfs_client/*`
  - `src/datasystem/common/l2cache/obs_client/*`
  - `src/datasystem/common/l2cache/slot_client/slot_client.*`
  - `src/datasystem/common/util/gflag/common_gflag_define.cpp`
  - `src/datasystem/common/util/validator.h`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Why this module exists:
  - define which secondary-storage backend is active for a worker;
  - keep backend-specific persistence details behind a single `PersistenceApi` interface;
  - make backend selection visible to worker startup, migration, and recovery paths.
- Primary source files to verify against:
  - `src/datasystem/common/l2cache/l2_storage.*`
  - `src/datasystem/common/l2cache/persistence_api.*`
  - `src/datasystem/common/l2cache/object_persistence_api.*`
  - `src/datasystem/common/l2cache/aggregated_persistence_api.*`
  - `src/datasystem/common/l2cache/sfs_client/sfs_client.*`
  - `src/datasystem/common/l2cache/obs_client/obs_client.*`
  - `src/datasystem/common/l2cache/slot_client/slot_client.*`
  - `src/datasystem/common/util/gflag/common_gflag_define.cpp`
  - `src/datasystem/common/util/gflag/common_gflags_validate.cpp`
  - `src/datasystem/common/util/validator.h`

## Responsibilities

- Verified:
  - `Validator::ValidateL2CacheType()` allows `obs`, `sfs`, `distributed_disk`, and `none`.
  - `GetCurrentStorageType()` maps `FLAGS_l2_cache_type` into `L2StorageType::{OBS,SFS,DISTRIBUTED_DISK}` and leaves
    `NONE` when the flag is unrecognized or disabled.
  - `IsSupportL2Storage()` returns true only for the three concrete backends, so worker startup skips persistence
    initialization for `none`.
  - `PersistenceApi::Create()` dispatches:
    - `distributed_disk` -> `AggregatedPersistenceApi(std::make_unique<SlotClient>(FLAGS_distributed_disk_path))`
    - otherwise -> `ObjectPersistenceApi`
  - `ObjectPersistenceApi::Init()` further dispatches inside the object-per-file path:
    - `obs` -> `ObsClient(FLAGS_obs_endpoint, FLAGS_obs_bucket)`
    - `sfs` -> `SfsClient(FLAGS_sfs_path)`
    - other values -> no-op init with an informational log
  - `ObjectPersistenceApi::{PreloadSlot,MergeSlot}` always return `K_NOT_SUPPORTED`.
- Pending verification:
  - whether any client-side code outside worker object-cache paths consumes `GetL2CacheRequestSuccessRate()`
    directly.

## Related Docs

- Module design:
  - `design.md`
- Slot module:
  - `../slot/README.md`
- Slot design:
  - `../slot/design.md`
- Matching playbook:
  - `../../../playbooks/features/infra/l2cache/implementation.md`
  - `../../../playbooks/features/infra/slot/implementation.md`

## Backend Summary

| Backend | Main implementation | Storage model | Slot takeover support | Key config |
| --- | --- | --- | --- | --- |
| `obs` | `ObjectPersistenceApi` + `ObsClient` | one encoded object key directory plus version object in one OBS bucket | no | `obs_access_key`, `obs_secret_key`, `obs_endpoint`, `obs_bucket`, `obs_https_enabled`, `enable_cloud_service_token_rotation` |
| `sfs` | `ObjectPersistenceApi` + `SfsClient` | one encoded object key directory plus version file under mounted `sfs_path` | no | `sfs_path` |
| `distributed_disk` | `AggregatedPersistenceApi` + `SlotClient` | worker-scoped slot directories with manifest/index/data files | yes | `distributed_disk_path`, `distributed_disk_*`, `cluster_name`, `worker_address` |

## Key Entry Points

- Public APIs:
  - `PersistenceApi::Create()`
  - `PersistenceApi::CreateShared()`
  - `PersistenceApi::{Init,Save,Get,GetWithoutVersion,Del,PreloadSlot,MergeSlot}`
  - `GetCurrentStorageType()`
  - `IsSupportL2Storage()`
- Internal services / executables:
  - `worker::WorkerOCServer::Init()` creates and initializes `persistenceApi_` when L2 storage is enabled
  - `object_cache::SlotRecoveryManager` only enables its feature gate for `distributed_disk`
  - `object_cache::WorkerOcServiceMigrateImpl` calls `MergeSlot()` after successful slot migration
- Config flags or environment variables:
  - shared selection flags:
    - `l2_cache_type`
    - `distributed_disk_path`
  - OBS-specific:
    - `obs_access_key`
    - `obs_secret_key`
    - `obs_endpoint`
    - `obs_bucket`
    - `obs_https_enabled`
    - `enable_cloud_service_token_rotation`
    - `TOKEN_ROTATION_CONFIG`
    - `/var/run/secrets/tokens/csms-token`
  - SFS-specific:
    - `sfs_path`

## Main Dependencies

- Upstream callers:
  - worker-side async or synchronous persistence paths through `PersistenceApi`
  - worker startup and shutdown flows
  - slot recovery manager and migration service for distributed-disk-only slot calls
- Downstream modules:
  - `ObsClient`
  - `SfsClient`
  - `SlotClient`
  - `SlotRecoveryManager`
- External dependencies:
  - `libcurl` for OBS REST traffic and `PersistenceApi::UrlEncode()`
  - mounted filesystem permissions for SFS and distributed-disk paths
  - ETCD only for the slot recovery coordination layer on top of distributed disk

## Object-Per-File Backends

- Shared object-per-file behavior:
  - `ObjectPersistenceApi::Save()` writes to `<encodedKey>/<version>`.
  - `PersistenceApi::UrlEncode()` uses `curl_easy_escape()` and replaces `%` with
    `L2CACHE_PERCENT_SIGN_ENCODE` to avoid ambiguous path handling.
  - `Get()` falls back to `GetWithoutVersion()` when the exact version is not found.
  - `Del()` lists all versions first and either preserves the newest version or deletes everything, depending on
    `deleteAllVersion`.
- SFS-specific verified facts:
  - `SfsClient` works under `<sfs_path>/datasystem`.
  - upload writes to `<target>_` first and renames to the final version path on success.
  - `List()` scans local directories; incomplete `_` files are hidden unless `listIncompleteVersions` is true.
  - `DeleteOne()` removes empty object directories after deleting the last version.
- OBS-specific verified facts:
  - `ObsClient` uses HTTP REST and chooses single-part upload up to 100 MB and multipart upload above that threshold.
  - `Delete()` batches at most 1000 object keys per request.
  - `obs_https_enabled=true` switches to virtual-hosted-style HTTPS URLs; `false` keeps path-style HTTP.
  - token rotation mode loads short-lived credentials via CCMS/IAM and refreshes them in a background thread.

## Distributed-Disk Differences

- Verified:
  - distributed-disk does not use `FLAGS_sfs_path`; `PersistenceApi::Create()` passes `FLAGS_distributed_disk_path`
    into `SlotClient`.
  - slot count is currently fixed at `DISTRIBUTED_DISK_SLOT_NUM = 128`; there is no runtime slot-count flag in source.
  - `SlotClient::{Save,Get,GetWithoutVersion,Delete}` accept `timeoutMs` in the interface but ignore it in the current
    implementation.
  - `SlotClient::GetRequestSuccessRate()` currently returns an empty string.
  - distributed-disk is the only backend that supports `MergeSlot()` and `PreloadSlot()`.

## Build And Test

- Build commands:
  - `cmake --build <build-dir> --target common_persistence_api`
  - `cmake --build <build-dir> --target common_slot_client`
  - Bazel targets follow `src/datasystem/common/l2cache/BUILD.bazel`
- Fast verification commands:
  - narrow builds for `common_persistence_api`, `common_sfs_client`, `common_obs`, or `common_slot_client`
  - run focused unit tests under `tests/ut/common/l2cache`, `tests/ut/common/sfs_client`, and
    `tests/ut/common/obs_client`
- Representative tests:
  - `tests/ut/common/l2cache/slot_store_test.cpp`
  - `tests/ut/worker/object_cache/slot_worker_integration_test.cpp`
  - `tests/st/worker/object_cache/slot_end2end_test.cpp`
  - `tests/st/common/sfs_client/sfs_persistence_api_test.cpp`
  - `tests/st/common/sfs_client/sfs_end2end_test.cpp`
  - `tests/st/common/obs_client/obs_persistence_api_test.cpp`

## Review And Bugfix Notes

- Common change risks:
  - changing dispatch in `PersistenceApi::Create()` changes worker behavior repo-wide;
  - changing encoding rules can silently orphan old OBS/SFS objects because list/get/delete must agree on the exact key;
  - changing distributed-disk root-path construction can make restart recovery look at the wrong namespace.
- Important invariants:
  - OBS and SFS are object-per-version backends; distributed-disk is slot-aggregated.
  - `MergeSlot()` and `PreloadSlot()` must remain distributed-disk-only unless the caller contracts change too.
  - worker startup should only initialize slot recovery when `l2_cache_type == "distributed_disk"`.
- Observability or debugging hooks:
  - `ObjectPersistenceApi`, `ObsClient`, `SfsClient`, and `SlotClient` all log backend-specific operations.
  - OBS request success rate is exposed through `ObsClient::GetRequestSuccessRate()`.

## Open Questions

- The current repo context does not yet document the separate async send/delete pipeline that feeds `PersistenceApi`.
- distributed-disk still lacks a non-empty success-rate string and dedicated slot metrics in the shared API surface.
