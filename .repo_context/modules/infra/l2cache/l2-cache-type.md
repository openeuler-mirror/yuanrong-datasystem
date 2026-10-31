# L2 Cache Type And Backend Selection

## Scope

- Paths:
  - `src/datasystem/common/l2cache/l2_storage.*`
  - `src/datasystem/common/l2cache/persistence_api.*`
  - `src/datasystem/common/l2cache/object_persistence_api.*`
  - `src/datasystem/common/l2cache/sfs_client/*`
  - `src/datasystem/common/l2cache/obs_client/*`
  - `src/datasystem/common/flags/common_flag_define.cpp`
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
  - `src/datasystem/common/l2cache/sfs_client/sfs_client.*`
  - `src/datasystem/common/l2cache/obs_client/obs_client.*`
  - `src/datasystem/common/flags/common_flag_define.cpp`
  - `src/datasystem/common/flags/common_flags_validate.cpp`
  - `src/datasystem/common/util/validator.h`

## Responsibilities

- Verified:
  - `l2_cache_type` accepts `obs`, `sfs`, and `none`.
  - `PersistenceApi::Create()` creates `ObjectPersistenceApi` for the supported backends.
  - `IsSupportL2Storage()` returns true only for the two concrete backends, so worker startup skips persistence
    initialization for `none`.
  - `ObjectPersistenceApi::Init()` further dispatches inside the object-per-file path:
    - `obs` -> `ObsClient(FLAGS_obs_endpoint, FLAGS_obs_bucket)`
    - `sfs` -> `SfsClient(FLAGS_sfs_path)`
    - other values -> no-op init with an informational log
- Pending verification:
  - whether any client-side code outside worker object-cache paths consumes `GetL2CacheRequestSuccessRate()`
    directly.

## Related Docs

- Module design:
  - `design.md`
- Matching playbook:
  - `../../../playbooks/features/infra/l2cache/implementation.md`

## Backend Summary

| Backend | Main implementation | Storage model | Key config |
| --- | --- | --- | --- |
| `obs` | `ObjectPersistenceApi` + `ObsClient` | one encoded object key directory plus version object in one OBS bucket | `obs_access_key`, `obs_secret_key`, `obs_endpoint`, `obs_bucket`, `obs_https_enabled`, `enable_cloud_service_token_rotation` |
| `sfs` | `ObjectPersistenceApi` + `SfsClient` | one encoded object key directory plus version file under mounted `sfs_path` | `sfs_path` |

## Key Entry Points

- Public APIs:
  - `PersistenceApi::Create()`
  - `PersistenceApi::CreateShared()`
  - `PersistenceApi::{Init,Save,Get,GetWithoutVersion,Del}`
  - `GetCurrentStorageType()`
  - `IsSupportL2Storage()`
- Internal services / executables:
  - `worker::WorkerOCServer::Init()` creates and initializes `persistenceApi_` when L2 storage is enabled
- Config flags or environment variables:
  - shared selection flags:
    - `l2_cache_type`
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
- Downstream modules:
  - `ObsClient`
  - `SfsClient`
- External dependencies:
  - `libcurl` for OBS REST traffic and `PersistenceApi::UrlEncode()`
  - mounted filesystem permissions for SFS

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

## Build And Test

- Build commands:
  - `cmake --build <build-dir> --target common_persistence_api`
  - Bazel targets follow `src/datasystem/common/l2cache/BUILD.bazel`
- Fast verification commands:
  - run focused unit tests under `tests/ut/common/l2cache`, `tests/ut/common/sfs_client`, and
    `tests/ut/common/obs_client`
- Representative tests:
  - `tests/st/common/sfs_client/sfs_persistence_api_test.cpp`
  - `tests/st/common/sfs_client/sfs_end2end_test.cpp`
  - `tests/st/common/obs_client/obs_persistence_api_test.cpp`

## Review And Bugfix Notes

- Common change risks:
  - changing dispatch in `PersistenceApi::Create()` changes worker behavior repo-wide;
  - changing encoding rules can silently orphan old OBS/SFS objects because list/get/delete must agree on the exact key;
- Important invariants:
  - OBS and SFS keep object-per-version paths and version-aware deletion.
- Observability or debugging hooks:
  - OBS request success rate is exposed through `ObsClient::GetRequestSuccessRate()`.

## Open Questions

- The current repo context does not yet document the separate async send/delete pipeline that feeds `PersistenceApi`.
