# Metrics

## Scope

- Paths:
  - `src/datasystem/common/metrics`
  - `src/datasystem/common/metrics/hard_disk_exporter`
  - `src/datasystem/common/metrics/metrics_vector`
- Why this module exists:
  - provide the exporter abstraction and resource-metric collection pipeline used by observability paths;
  - collect periodic resource usage metrics from worker/master/runtime components and flush them to monitor logs.
- Primary source files to verify against:
  - `src/datasystem/common/metrics/CMakeLists.txt`
  - `src/datasystem/common/metrics/metrics_exporter.h`
  - `src/datasystem/common/metrics/metrics_exporter.cpp`
  - `src/datasystem/common/metrics/res_metric_collector.h`
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/CMakeLists.txt`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
  - `src/datasystem/common/metrics/res_metrics.def`
  - `src/datasystem/common/metrics/metrics_description.def`
  - `src/datasystem/common/metrics/res_metric_name.h`
  - `src/datasystem/worker/worker_oc_server.cpp`

## Read Order Inside This Area

- If the question is about overall architecture, extension points, or feature design:
  - read `design.md`
- If the question is about periodic collection, `RegisterCollectHandler`, collector startup, or blank metric columns:
  - read `resource-collector.md`
- If the question is about exporter buffering, flush threads, file output, or `HardDiskExporter` behavior:
  - read `exporters-and-buffering.md`
- If the question is about metric enum definitions, descriptions, or registration points:
  - read `metric-families-and-registration.md`

## Responsibilities Overview

- Verified:
  - `common_metrics` is built from `res_metric_collector.cpp` plus the shared exporter base.
  - `metrics_exporter_base` provides the asynchronous buffered exporter framework.
  - `HardDiskExporter` is the current concrete exporter used for resource-monitor logs and also by `AccessRecorder`.
  - `ResMetricCollector` is a singleton periodic collector that emits resource metrics when `log_monitor` is enabled.
  - metrics are not auto-discovered; they are registered explicitly through `RegisterCollectHandler`.
  - `worker_oc_server.cpp` is a major registration point for many resource metrics.
- Pending verification:
  - whether any additional registration points outside `WorkerOCServer` should get their own context note;
  - whether a non-harddisk exporter backend is planned but not yet implemented.

## Subdocuments

- `design.md`
  - architecture, extension points, compatibility constraints, and implementation guardrails
- `resource-collector.md`
  - collector singleton behavior, handler registration, init gating, and interval-driven sampling
- `exporters-and-buffering.md`
  - buffered exporter model, flush thread behavior, hard-disk output, and logging coupling
- `metric-families-and-registration.md`
  - enum definitions, descriptions, registration points, and review-sensitive alignment rules

## Main Components

| Component | Verified role | Details live primarily in |
| --- | --- | --- |
| `MetricsExporter` | abstract buffered exporter | `exporters-and-buffering.md` |
| `HardDiskExporter` | concrete file exporter | `exporters-and-buffering.md` |
| `ResMetricCollector` | singleton periodic collector | `resource-collector.md` |
| `res_metrics.def` | metric-name enumeration source | `metric-families-and-registration.md` |
| `metrics_description.def` | human-facing metric descriptions and units | `metric-families-and-registration.md` |
| `metrics_vector/*` | metric helper structures/vectors | this file plus future follow-up docs if needed |

## Flags And Config Overview

- Collector control flags:
  - `log_monitor`
  - `log_monitor_exporter`
  - `log_monitor_interval_ms`
- Exporter behavior also depends on logging-related flags:
  - `log_dir`
  - `logfile_mode`
  - `max_log_size`
  - `max_log_file_num`

## Cross-Module Coupling

- Metrics depends on:
  - `common_log`
  - `common_util`
  - common flags/gflags
- Metrics is consumed by:
  - worker runtime during resource registration and lifecycle
  - access logging through `HardDiskExporter`
- Important implication:
  - `metrics` and `logging` are not independent modules; `HardDiskExporter` is the bridge between them.

## Review And Bugfix Notes

- Common change risks:
  - adding a new metric family requires both registration and definition alignment;
  - missing handler registration does not fail hard, so metrics can silently degrade into blank columns plus one warning;
  - exporter changes affect both resource metrics and access-monitor logging.
- Good first files when metrics look wrong:
  - `res_metric_collector.cpp`
  - `hard_disk_exporter.cpp`
  - `worker_oc_server.cpp`
  - `res_metrics.def`
  - `metrics_description.def`

## Current Split Layout

- start with this file for ownership and routing;
- go to `design.md` before designing a new metrics feature or refactor;
- go to `resource-collector.md`, `exporters-and-buffering.md`, or `metric-families-and-registration.md` for behavior-level details.
