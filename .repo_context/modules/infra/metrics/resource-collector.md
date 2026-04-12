# Resource Collector

## Scope

- Paths:
  - `src/datasystem/common/metrics/res_metric_collector.h`
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
- Why this document exists:
  - explain how periodic metric collection is started, gated, and populated from registered handlers.

## Primary Source Files

- `src/datasystem/common/metrics/res_metric_collector.h`
- `src/datasystem/common/metrics/res_metric_collector.cpp`
- `src/datasystem/common/metrics/res_metric_name.h`
- `src/datasystem/worker/worker_oc_server.cpp`

## Responsibilities

- Verified:
  - `ResMetricCollector` is a singleton periodic collector.
  - collector interval defaults to `FLAGS_log_monitor_interval_ms`.
  - collector only initializes exporter infrastructure when `FLAGS_log_monitor` is true.
  - current supported exporter selection is `"harddisk"` via `FLAGS_log_monitor_exporter`.
  - `CollectMetrics()` iterates metric IDs from `ResMetricName::SHARED_MEMORY` up to `RES_METRICS_END - 1`.
  - if a metric has no registered handler, an empty field is emitted and a warning is logged once.
  - each handler returns a string payload, not a typed metric object.

## Registration Model

- Verified:
  - metrics are not auto-discovered.
  - producers register handlers explicitly through `RegisterCollectHandler`.
  - collector correctness therefore depends on initialization order and registration completeness, not just enum definitions.
- Practical effect:
  - a new metric family can compile cleanly yet still emit blanks until a handler is registered in the right runtime path.

## Lifecycle And Gating

- Startup is gated by:
  - `log_monitor`
  - `log_monitor_exporter`
  - exporter initialization success
- Runtime behavior:
  - registered handlers are polled periodically
  - returned strings are assembled into exporter output
  - missing handlers do not fail hard

## Common Failure Patterns

- Missing metric values:
  - check whether `log_monitor` is enabled
  - verify the handler was registered before collection starts
  - confirm the handler returns a non-empty payload
- Partial blank columns with only one warning:
  - likely missing `RegisterCollectHandler` coverage for one or more enum IDs
- Metrics stop after startup:
  - inspect collector thread lifecycle and exporter initialization path

## Bugfix And Review Notes

- Good first files when collection cadence or presence looks wrong:
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Common risks:
  - adding a new family without registration silently degrades observability;
  - changing collector loop timing can affect monitor file volume and downstream dashboards.
