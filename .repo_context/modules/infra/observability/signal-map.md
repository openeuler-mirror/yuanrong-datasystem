# Signal Map

## Document Metadata

- Status:
  - `active`
- Doc type:
  - behavior note | routing reference
- Primary code paths:
  - `src/datasystem/worker/*`
  - `src/datasystem/common/log/*`
  - `src/datasystem/common/metrics/*`
  - `src/datasystem/worker/cluster_manager/*`
- Last verified against source:
  - `2026-04-13`
- Related design docs:
  - `.repo_context/modules/infra/observability/diagnosis-and-operations.md`
  - `.repo_context/modules/infra/logging/design.md`
  - `.repo_context/modules/infra/metrics/design.md`

## Scope

- Paths:
  - `src/datasystem/worker`
  - `src/datasystem/common/log`
  - `src/datasystem/common/metrics`
- Why this document exists:
  - route common symptoms to the first evidence sources, configs, and code entrypoints that usually shrink the problem fastest.

## Symptom Routing Table

| Symptom | First signals to inspect | Key config or files | First code entrypoints |
| --- | --- | --- | --- |
| worker process exits during startup | ordinary logs, crash logs | `log_dir`, metadata backend config | `src/datasystem/worker/worker_main.cpp`, `worker.cpp`, `worker_oc_server.cpp` |
| process is alive but not ready | readiness file, startup logs | `ready_check_path`, `worker_address`, `etcd_address`, `metastore_address` | `worker_oc_server.cpp`, `worker_runtime` docs |
| process was ready but later becomes unhealthy | liveness file, ordinary logs | `liveness_check_path`, `liveness_probe_timeout_s` | `src/datasystem/worker/worker_liveness_check.cpp` |
| request failures with little signal | ordinary logs, access logs, trace IDs | `log_monitor`, access-log files | `src/datasystem/common/log/access_recorder.cpp`, request entrypoints |
| requests succeed but monitor logs disappear | ordinary logs, resource monitor files, init logs | `log_monitor`, `log_monitor_exporter`, `log_dir` | `logging.cpp`, `res_metric_collector.cpp`, `hard_disk_exporter.cpp` |
| latency or throughput regression | ordinary logs, access logs, resource monitor files | queue-related runtime config, monitor interval, log flush settings | `worker_main.cpp`, `metrics_exporter.cpp`, `log_manager.cpp`, service thread pools |
| blank metric columns | resource monitor file, warning logs | `log_monitor`, registration coverage | `res_metric_collector.cpp`, `worker_oc_server.cpp`, `res_metrics.def` |
| routing or topology looks wrong after config change | ordinary logs, cluster-management evidence | `etcd_address`, `metastore_address`, cluster config | `cluster_manager/etcd_cluster_manager.cpp`, `hash_ring/hash_ring.cpp`, `worker_oc_server.cpp` |
| disk usage grows unexpectedly | ordinary logs, rotated monitor files | `max_log_size`, `max_log_file_num`, `log_retention_day`, `log_compress` | `log_manager.cpp`, `hard_disk_exporter.cpp` |

## Common Misreads

- Missing monitor files do not automatically mean the worker never started.
- A live PID does not automatically mean the worker is ready or healthy.
- One blank metric family often means missing handler registration, not total metrics failure.
- Crash evidence may only be in `container.log`, not ordinary severity files.

## Update Rules For This Document

- Keep this file short and routing-oriented.
- Update it whenever a recurring symptom gains a better first-check path or when signal semantics change.
