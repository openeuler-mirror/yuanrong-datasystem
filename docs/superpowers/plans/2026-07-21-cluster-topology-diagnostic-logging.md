# Cluster Topology Diagnostic Logging Execution Plan

> Required flow: use Superpowers execution discipline, follow `.repo_context/playbooks/features/infra-engineering-workflow.md`, verify before completion, keep one commit for PR submission.

## Goal

Implement cluster topology diagnostic logs that let users search `CLUSTER_` history in the log platform, then narrow by `flow`, prefix trace, `epoch`, `operation_prefix`, and `task_prefix`. Keep topology behavior unchanged.

## Scope

Code:

- `src/datasystem/cluster/diagnostics/topology_diagnostic_log.{h,cpp}`
- `src/datasystem/protos/cluster_topology.proto`
- `src/datasystem/cluster/model/topology_types.h`
- `src/datasystem/cluster/repository/topology_repository_codec.cpp`
- `src/datasystem/cluster/control/topology_controller.{h,cpp}`
- `src/datasystem/cluster/control/topology_task_materializer.cpp`
- `src/datasystem/cluster/executor/topology_phase_callbacks.h`
- `src/datasystem/cluster/executor/topology_task_executor.cpp`
- `src/datasystem/cluster/runtime/topology_engine.cpp`
- `src/datasystem/cluster/runtime/topology_observer.cpp`
- `src/datasystem/cluster/coordination_backend/ds_coordination_backend.cpp`
- `src/datasystem/cluster/coordination_backend/etcd_coordination_backend.{h,cpp}`
- `src/datasystem/worker/worker_topology_phase_callbacks.{h,cpp}`
- CMake/Bazel build files for the touched cluster targets

Tests and docs:

- `tests/ut/cluster/topology_diagnostic_log_test.cpp`
- existing cluster topology schema/codec/controller/executor tests
- `docs/source_zh_cn/appendix/cluster_log_diagnosis_guide.md`
- `docs/source_zh_cn/appendix/index.md`
- `.repo_context/modules/infra/observability/*`
- overview and detailed design docs under `docs/superpowers/designs/`

Non-scope:

- no metadata-first scale-in behavior change;
- no master metadata manager logging refactor;
- no task janitor stale cleanup logging refactor;
- no failure missing/resolved or preempt/resume fine-grained logs;
- no backend keyspace, placement, task payload, retry policy, or callback order change.

## Tasks

- [x] Reproduce the existing diagnosis gap from the supplied sanitized scale-in log:
  - current logs show old scale-in batch epochs, repeated drain/runtime failures, empty trace slot, payload `cluster=`, `batch_type`, missing target/component/operation/flow duration.

- [x] Add diagnostics helper:
  - generate `cluster;<uuid_suffix>` using the same short UUID suffix shape as existing trace;
  - restore previous trace after scoped cluster logs;
  - format member states, lifecycle states, flows, callback phases, backend states, member lists, state counts, and short id prefixes.

- [x] Propagate active batch trace:
  - add `ChangeBatchPb.diagnostic_trace_id = 3`;
  - add `ActiveBatch::diagnosticTraceId`;
  - add `TopologyPhaseAction::diagnosticTraceId`;
  - encode/decode the field and preserve old-batch compatibility.

- [x] Normalize controller and materializer logs:
  - `CLUSTER_FLOW` start/wait/finish for bootstrap, scale-out, scale-in, failure, shutdown;
  - `CLUSTER_CHANGE` commit/read-back evidence;
  - `CLUSTER_MEMBERSHIP_OBSERVED` full member list;
  - `CLUSTER_TASK action=task_created`.

- [x] Normalize executor and callback logs:
  - executor notify/progress/recover inherits active batch trace;
  - scale-in callback logs `data_drain` and `metadata_handoff` with source/target/executor/operation/task/status;
  - failure callback step logs component, failed node, operation/task prefix, step and raw status.

- [x] Normalize backend/watch/runtime logs:
  - worker/controller/observer watch registration and resync use `flow=watch`;
  - worker backend access failure/recovered uses `flow=backend`;
  - availability transition uses `CLUSTER_DEGRADED`;
  - runtime operation failure uses structured `CLUSTER_RUNTIME_OPERATION_FAILED`;
  - coordination backend lease update and restart reconciliation emit `CLUSTER_MEMBERSHIP` / `CLUSTER_FLOW flow=restart`.

- [x] Add tests:
  - helper trace/member/lifecycle formatting;
  - proto schema contract;
  - repository codec round-trip;
  - controller new active batch trace;
  - executor trace propagation into callback action.

- [x] Add docs:
  - Chinese appendix guide with 4-node examples for multi-worker scale-out, scale-in, failure, backend degradation, process failure, bootstrap, restart, shutdown;
  - appendix index entry;
  - `.repo_context` observability pointers;
  - overview and detailed design aligned with current source.

## Validation Plan

Run locally when possible:

```bash
git diff --check
rg -n "CLUSTER_.*(trace[_]id=|severity=|cluster=|type_name=|batch_type|CLUSTER_SCALE_IN_DRAIN)" src/datasystem/cluster src/datasystem/worker/worker_topology_phase_callbacks.cpp src/datasystem/worker/worker_topology_phase_callbacks.h
rg -n "diagnostic_trace_id|diagnosticTraceId|ClusterTraceScope|BuildClusterDiagnosticTraceId|EnsureClusterDiagnosticTraceId" src/datasystem/protos/cluster_topology.proto src/datasystem/cluster tests/ut/cluster
rg -n "cluster_log_diagnosis_guide.md|集群日志定位指南" docs/source_zh_cn/appendix/index.md
```

Run on the remote yuanrong validation pool after pushing the single commit:

```bash
cmake --build <build_dir> --target cluster_topology_contract_ut
<build_dir>/tests/ut/cluster_topology_contract_ut --gtest_filter='TopologyDiagnosticLogTest.*:ClusterTopologySchemaTest.*:TopologyRepositoryCodecTest.*:TopologyControllerTest.*:TopologyTaskExecutorTest.*'
```

If the local macOS build script cannot source Linux `/etc/profile.d` files, record that limitation and rely on remote ds-test validation before creating PRs.

## PR Plan

- Keep exactly one commit.
- Push source branch only to fork `origin`.
- Create two GitCode PRs from the same branch:
  - base `master`;
  - base `0.9.1`.
- Use `.skills/ds-create-pr/` and the repository PR body template.
- After both PRs are created, comment `/retest` on each PR to trigger gates.
