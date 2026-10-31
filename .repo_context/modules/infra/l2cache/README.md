# L2 Cache And Secondary Storage

## Scope

- Paths:
  - `src/datasystem/common/l2cache`
  - `src/datasystem/common/l2cache/obs_client`
  - `src/datasystem/common/l2cache/sfs_client`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Why this module exists:
  - provide backend selection and the shared persistence surface behind `l2_cache_type`;
- Primary source files to verify against:
  - `src/datasystem/common/l2cache/CMakeLists.txt`
  - `src/datasystem/common/l2cache/l2_storage.*`
  - `src/datasystem/common/l2cache/persistence_api.*`
  - `src/datasystem/common/l2cache/object_persistence_api.*`
  - `src/datasystem/common/l2cache/sfs_client/sfs_client.*`
  - `src/datasystem/common/l2cache/obs_client/obs_client.*`
  - `src/datasystem/worker/worker_oc_server.cpp`

## Read Order Inside This Area

- If the question is about architecture boundaries, backend capability differences, worker integration, or what kind of
  change needs design-first treatment:
  - read `design.md`
- If the question is about backend selection, flags, `PersistenceApi` dispatch, or the differences between `obs`,
  `sfs`, and `none`:
  - read `l2-cache-type.md`
- If the question is about how to implement or review an L2 or secondary-storage feature safely:
  - read `../../../playbooks/features/infra/l2cache/implementation.md`

## Responsibilities Overview

- Verified:
  - `l2_cache_type` accepts `obs`, `sfs`, and `none`.
  - `PersistenceApi::Create()` creates `ObjectPersistenceApi` for the supported backends.

## Subdocuments

- `design.md`
  - architecture-level overview for backend selection, shared API surface, and parent-module boundaries
- `l2-cache-type.md`

## Matching Playbook

- `../../../playbooks/features/infra/l2cache/implementation.md`
  - feature workflow and risk gates for backend selection, `PersistenceApi`, and OBS/SFS or cross-backend changes

## Main Components

| Component | Verified role | Details live primarily in |
| --- | --- | --- |
| `PersistenceApi` | shared persistence interface and backend factory | `l2-cache-type.md` |
| `ObjectPersistenceApi` | object-per-version persistence facade for OBS and SFS | `l2-cache-type.md` |
| `ObsClient` | HTTP REST OBS client with AK/SK or token rotation auth | `l2-cache-type.md` |
| `SfsClient` | mounted-filesystem client using rename-based object publishing | `l2-cache-type.md` |

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

## Cross-Module Coupling

- L2 cache depends on:
  - `common_util`
  - `common_log`
  - `common_inject`
  - `common_httpclient`
  - `common_metrics` for OBS response-code accounting
  - shared flags under `src/datasystem/common/flags/*`

## Review And Bugfix Notes

- Common change risks:
  - changing `PersistenceApi::Create()` changes semantics for every worker path that spills to L2;
- Good first files when backend choice or L2 behavior looks wrong:
  - `src/datasystem/common/l2cache/persistence_api.cpp`
  - `src/datasystem/common/l2cache/object_persistence_api.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
