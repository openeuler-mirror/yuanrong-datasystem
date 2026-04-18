# Slot Storage And Recovery

## Scope

- Paths:
  - `src/datasystem/common/l2cache/slot_client`
  - `src/datasystem/worker/object_cache/slot_recovery`
  - `src/datasystem/worker/object_cache/slot_recovery_orchestrator.*`
  - `src/datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.cpp`
  - `src/datasystem/worker/object_cache/worker_oc_service_impl.cpp`
  - `src/datasystem/protos/slot_recovery.proto`
- Why this module exists:
  - provide the distributed-disk storage engine behind `l2_cache_type=distributed_disk`;
  - own the slot manifest, index, data-file, compaction, takeover, and repair model;
  - connect disk-level slot takeover with worker restart and failed-worker recovery flows.
- Primary source files to verify against:
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

## Read Order Inside This Area

- If the question is about slot architecture, persistence format boundaries, recovery semantics, availability,
  reliability, or DFX expectations:
  - read `design.md`
- If the question is about backend selection or how slot relates to OBS/SFS and `PersistenceApi`:
  - read `../l2cache/l2-cache-type.md`
- If the question is about how to implement or review slot features safely:
  - read `../../../playbooks/features/infra/slot/implementation.md`

## Responsibilities Overview

- Verified:
  - `SlotClient` is the distributed-disk `StorageClient` implementation selected through
    `AggregatedPersistenceApi` when `l2_cache_type=distributed_disk`.
  - `Slot` owns one slot directory, manifest, replay snapshot, writer state, compaction, and takeover state machine.
  - slot persistence format includes CRC-protected index records, manifest-driven file-set transitions, and
    worker-scoped slot roots under `distributed_disk_path`.
  - worker startup repair, restart recovery, and failed-worker recovery all depend on slot-specific code paths.
  - slot migration handoff uses `MergeSlot()` and recovery uses `PreloadSlot()`.
- Pending verification:
  - whether a slot-specific metrics surface will be added beyond logs, inject points, and tests;
  - whether mixed-version upgrade constraints for slot manifest/index evolution need a dedicated rollout note.

## Companion Docs

- Matching `design.md`:
  - `design.md`
- Matching feature playbook:
  - `../../../playbooks/features/infra/slot/implementation.md`
- Reason if either is intentionally omitted:
  - none

## Key Entry Points

- Public APIs:
  - `datasystem::SlotClient::{Save,Get,GetWithoutVersion,Delete,PreloadSlot,MergeSlot}`
  - `datasystem::BuildSlotStoreRootForWorker(...)`
- Internal services / executables:
  - `worker::WorkerOCServer::InitSlotWorkerNamespace()`
  - `worker::WorkerOCServer::InitSlotRecovery()`
  - `object_cache::SlotRecoveryManager::{ScheduleLocalPendingTasksFromStore,HandleLocalRestart}`
  - `object_cache::WorkerOcServiceMigrateImpl`
- Config flags or environment variables:
  - `l2_cache_type`
  - `distributed_disk_path`
  - `distributed_disk_max_data_file_size_mb`
  - `distributed_disk_sync_interval_ms`
  - `distributed_disk_sync_batch_bytes`
  - `distributed_disk_compact_interval_s`
  - `cluster_name`
  - `worker_address`

## Main Dependencies

- Upstream callers:
  - `AggregatedPersistenceApi`
  - worker object-cache request paths through `PersistenceApi`
  - worker migration service
  - slot recovery manager
- Downstream modules:
  - local filesystem utilities under `slot_file_util.*`
  - worker object-cache metadata recovery callbacks
  - ETCD-backed slot recovery store
- External dependencies:
  - filesystem rename, fsync, and directory operations
  - injected fault hooks used in unit and system tests

## Build And Test

- Build commands:
  - `cmake --build <build-dir> --target common_slot_client`
  - `cmake --build <build-dir> --target common_persistence_api`
- Fast verification commands:
  - focused builds for `common_slot_client` plus touched worker object-cache targets
  - focused tests under `tests/ut/common/l2cache` and `tests/st/worker/object_cache`
- Representative tests:
  - `tests/ut/common/l2cache/slot_store_test.cpp`
  - `tests/ut/worker/object_cache/slot_worker_integration_test.cpp`
  - `tests/st/worker/object_cache/slot_end2end_test.cpp`
  - `tests/ut/worker/object_cache/slot_recovery_manager_test.cpp`
  - `tests/st/worker/object_cache/slot_recovery_manager_test.cpp`

## Review And Bugfix Notes

- Common change risks:
  - changing manifest fields or replay rules can strand persisted data after crash or restart;
  - changing worker-namespace path derivation can make recovery look at the wrong slot root;
  - changing takeover publication ordering can silently lose imported data on replay;
  - changing slot-count or hash rules can break ownership and data lookup compatibility.
- Important invariants:
  - distributed-disk slots use `distributed_disk_path`, not `sfs_path`;
  - slot count is fixed at `128`;
  - imported records become visible only through closed `IMPORT_BEGIN` / `IMPORT_END` batches;
  - source slot rename happens before target import publication during takeover.
- Observability or debugging hooks:
  - logs under `SlotClient`, `Slot`, and `SlotRecoveryManager`
  - inject points prefixed with `slotstore.`
  - focused tests covering replay, compaction, takeover, and recovery

## Open Questions

- whether slot-specific metrics should become first-class instead of staying log and inject driven;
- whether the current repo needs a dedicated mixed-version compatibility note for slot manifest/index evolution.
