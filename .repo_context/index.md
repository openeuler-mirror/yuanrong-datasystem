# Context Index

This is the main navigation page for `.repo_context/`.

Use it to jump from a question type to the smallest useful document instead of reading every module note.

## Read Order

1. `README.md`
2. `index.md`
3. `maintenance.md`
4. `generated/repo_index.md` when you need raw directory orientation
5. the most relevant module or playbook file

## Quick Routing By Intent

| If you need to... | Read first | Then verify against |
| --- | --- | --- |
| understand overall repo shape | `modules/overview/repository-overview.md` | `generated/repo_index.md` |
| find build, sanitizer, or coverage entrypoints | `modules/quality/build-test-debug.md` | `build.sh`, `CMakeLists.txt` |
| understand public SDK APIs and Python bindings | `modules/client/client-sdk.md` | `include/datasystem/*`, `src/datasystem/client`, `src/datasystem/pybind_api`, `python/yr/datasystem` |
| understand worker startup and runtime services | `modules/runtime/worker-runtime.md` | `src/datasystem/worker/*` |
| understand cluster metadata, ETCD, Metastore, and hash-ring coordination | `modules/runtime/cluster-management.md` | `docs/source_zh_cn/design_document/cluster_management.md`, `src/datasystem/worker/cluster_manager`, `src/datasystem/worker/hash_ring`, `cli/*.py` |
| understand shared infra used across modules | `modules/infra/common-infra.md` | `src/datasystem/common/*` |
| understand logging architecture before adding a feature | `modules/infra/logging/design.md` | `src/datasystem/common/log/*`, `src/datasystem/common/metrics/hard_disk_exporter/*` |
| understand trace ID generation or propagation | `modules/infra/logging/trace-and-context.md` | `src/datasystem/common/log/trace.*`, `src/datasystem/context/*` |
| understand access log, performance recording, or operation key mapping | `modules/infra/logging/access-recorder.md` | `src/datasystem/common/log/access_recorder.*`, `src/datasystem/common/log/access_point.def` |
| understand log startup, rotation, monitor flush, or crash logging | `modules/infra/logging/log-lifecycle-and-rotation.md` | `src/datasystem/common/log/logging.*`, `src/datasystem/common/log/log_manager.*`, `src/datasystem/common/log/failure_handler.*` |
| understand metrics architecture before adding a feature | `modules/infra/metrics/design.md` | `src/datasystem/common/metrics/*`, `src/datasystem/worker/worker_oc_server.cpp` |
| understand resource metric collection cadence or handler registration | `modules/infra/metrics/resource-collector.md` | `src/datasystem/common/metrics/res_metric_collector.*`, `src/datasystem/worker/worker_oc_server.cpp` |
| understand metric exporter buffering or hard-disk output | `modules/infra/metrics/exporters-and-buffering.md` | `src/datasystem/common/metrics/metrics_exporter.*`, `src/datasystem/common/metrics/hard_disk_exporter/*` |
| understand metric family definitions or runtime registration | `modules/infra/metrics/metric-families-and-registration.md` | `src/datasystem/common/metrics/res_metrics.def`, `src/datasystem/common/metrics/metrics_description.def`, `src/datasystem/worker/worker_oc_server.cpp` |
| diagnose a runtime, operations, or observability issue across modules | `modules/infra/observability/diagnosis-and-operations.md` | `src/datasystem/worker/*`, `src/datasystem/common/log/*`, `src/datasystem/common/metrics/*`, `tests/*` |
| map a symptom to the first logs, metrics, configs, and files to inspect | `modules/infra/observability/signal-map.md` | `src/datasystem/worker/*`, `src/datasystem/common/log/*`, `src/datasystem/common/metrics/*` |
| investigate a performance regression or latency issue | `modules/infra/observability/performance-troubleshooting.md` | `src/datasystem/worker/*`, `src/datasystem/common/log/*`, `src/datasystem/common/metrics/*`, `tests/perf/*` |
| understand readiness, liveness, and runtime health checks | `modules/infra/observability/runtime-health-and-runbook.md` | `src/datasystem/worker/worker_liveness_check.*`, `src/datasystem/worker/worker_main.cpp`, `src/datasystem/worker/worker_oc_server.cpp` |
| choose tests or reproduce a bug | `modules/quality/tests-and-reproduction.md` | `tests/*`, `build.sh` |
| design and implement a low-risk logging feature | `playbooks/features/infra/logging/implementation.md` | `modules/infra/logging/design.md`, `src/datasystem/common/log/*` |
| triage a production-style incident or collect first-pass evidence | `playbooks/operations/incident-triage.md` | `modules/infra/observability/diagnosis-and-operations.md`, `modules/quality/tests-and-reproduction.md` |
| run a structured performance investigation | `playbooks/operations/performance-investigation.md` | `modules/infra/observability/performance-troubleshooting.md`, `tests/perf/*` |
| decide what kind of context file to update | `playbooks/README.md` | `maintenance.md` |

## Quick Routing By Area

| Area | Primary doc | Typical code paths |
| --- | --- | --- |
| global repo map | `modules/overview/repository-overview.md` | `src/datasystem`, `include/datasystem`, `cli`, `tests`, `docs` |
| client/API surface | `modules/client/client-sdk.md` | `include/datasystem`, `src/datasystem/client`, `python/yr/datasystem` |
| worker runtime | `modules/runtime/worker-runtime.md` | `src/datasystem/worker` |
| cluster coordination | `modules/runtime/cluster-management.md` | `src/datasystem/worker/cluster_manager`, `src/datasystem/worker/hash_ring`, `cli/start.py`, `cli/up.py` |
| common infrastructure | `modules/infra/common-infra.md` | `src/datasystem/common` |
| logging area overview | `modules/infra/logging/README.md` | `src/datasystem/common/log` |
| logging design | `modules/infra/logging/design.md` | `src/datasystem/common/log/*`, `src/datasystem/common/metrics/hard_disk_exporter/*` |
| trace and context propagation | `modules/infra/logging/trace-and-context.md` | `src/datasystem/common/log/trace.*`, `src/datasystem/context/*` |
| access recorder and access logs | `modules/infra/logging/access-recorder.md` | `src/datasystem/common/log/access_recorder.*`, `src/datasystem/common/log/access_point.def` |
| log lifecycle and rotation | `modules/infra/logging/log-lifecycle-and-rotation.md` | `src/datasystem/common/log/logging.*`, `src/datasystem/common/log/log_manager.*`, `src/datasystem/common/log/failure_handler.*` |
| metrics area overview | `modules/infra/metrics/README.md` | `src/datasystem/common/metrics` |
| metrics design | `modules/infra/metrics/design.md` | `src/datasystem/common/metrics/*`, `src/datasystem/worker/worker_oc_server.cpp` |
| resource collector | `modules/infra/metrics/resource-collector.md` | `src/datasystem/common/metrics/res_metric_collector.*` |
| exporters and buffering | `modules/infra/metrics/exporters-and-buffering.md` | `src/datasystem/common/metrics/metrics_exporter.*`, `src/datasystem/common/metrics/hard_disk_exporter/*` |
| metric families and registration | `modules/infra/metrics/metric-families-and-registration.md` | `src/datasystem/common/metrics/res_metrics.def`, `src/datasystem/common/metrics/metrics_description.def`, `src/datasystem/worker/worker_oc_server.cpp` |
| observability and operations overview | `modules/infra/observability/README.md` | `src/datasystem/worker`, `src/datasystem/common/log`, `src/datasystem/common/metrics`, `tests` |
| diagnosis and operations | `modules/infra/observability/diagnosis-and-operations.md` | `src/datasystem/worker/*`, `src/datasystem/common/log/*`, `src/datasystem/common/metrics/*` |
| signal map | `modules/infra/observability/signal-map.md` | `src/datasystem/worker/*`, `src/datasystem/common/log/*`, `src/datasystem/common/metrics/*` |
| performance troubleshooting | `modules/infra/observability/performance-troubleshooting.md` | `src/datasystem/worker/*`, `src/datasystem/common/log/*`, `src/datasystem/common/metrics/*`, `tests/perf` |
| runtime health and runbook | `modules/infra/observability/runtime-health-and-runbook.md` | `src/datasystem/worker/worker_liveness_check.*`, `src/datasystem/worker/worker_main.cpp`, `src/datasystem/worker/worker_oc_server.cpp` |
| build/test/debug | `modules/quality/build-test-debug.md` | `build.sh`, `tests`, top-level CMake |
| test selection and repro | `modules/quality/tests-and-reproduction.md` | `tests/ut`, `tests/st` |

## When To Use `generated/repo_index.md`

Use `generated/repo_index.md` when:

- you know a path but need nearby files quickly;
- you need a fast coarse tree of a large area;
- the domain documents are not yet detailed enough.

Do not stop there if the question depends on behavior. Jump from the tree to source files.

## Current Gaps

- `modules/infra/common-infra.md` is still broad and should later be split further into `rpc`, `shared-memory`, `kvstore`, and `rdma-fast-transport`.
- `modules/client/client-sdk.md` should later split into `cpp-api`, `python-binding`, and `connect-options-auth`.
- `modules/runtime/worker-runtime.md` should later split into `startup-lifecycle`, `worker-service`, and `embedded-worker`.
