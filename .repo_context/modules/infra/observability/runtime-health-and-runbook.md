# Runtime Health And Runbook

## Document Metadata

- Status:
  - `active`
- Doc type:
  - behavior note | operator reference
- Primary code paths:
  - `src/datasystem/worker/worker_main.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/worker_liveness_check.cpp`
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
- Last verified against source:
  - `2026-04-13`
- Related design docs:
  - `.repo_context/modules/infra/observability/diagnosis-and-operations.md`
  - `.repo_context/modules/runtime/worker-runtime.md`

## Scope

- Paths:
  - `src/datasystem/worker/worker_main.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/worker_liveness_check.cpp`
- Why this document exists:
  - summarize the practical operator-facing health signals and the first runbook steps around startup, readiness, liveness, and shutdown.

## Health Levels

- `not_started`:
  - process failed before runtime stabilized; look at startup logs first.
- `started_not_ready`:
  - process exists but readiness has not been established.
- `ready_and_live`:
  - readiness exists and liveness file is fresh and successful.
- `ready_but_degraded`:
  - request path may still work, but monitor files, logs, or liveness checks show partial issues.
- `shutting_down`:
  - inspect `PreShutDown` and `ShutDown` evidence separately.

## First Operator Checks

1. Is the process running?
2. Does the readiness file exist where `ready_check_path` says it should?
3. Does the liveness file exist, stay fresh, and contain success?
4. Are ordinary logs updating under `log_dir`?
5. Are resource monitor files present when `log_monitor` is enabled?
6. Is metadata backend config (`etcd_address` or `metastore_address`) valid for this node?

## Common Operator Actions

- Startup failure:
  - inspect ordinary logs and crash output before retrying blindly.
- Readiness failure:
  - inspect worker startup config and service-init logs.
- Liveness failure:
  - inspect probe file freshness plus `WorkerLivenessCheck` evidence.
- Missing monitor output:
  - inspect `log_monitor`, exporter config, and log directory permissions.
- Planned shutdown:
  - confirm whether `PreShutDown` and `ShutDown` both completed cleanly.

## Update Rules For This Document

- Keep this file operator-facing and checklist-oriented.
- Update it when readiness, liveness, startup, or shutdown semantics change.
