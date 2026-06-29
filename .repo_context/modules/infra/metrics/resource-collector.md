# Resource Collector

## Document Metadata

- Status:
  - `active`
- Doc type:
  - behavior note | submodule reference
- Primary code paths:
  - `src/datasystem/common/metrics/res_metric_collector.h`
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Last verified against source:
  - `2026-06-26`
- Related design docs:
  - `.repo_context/modules/infra/metrics/design.md`
  - `.repo_context/modules/infra/metrics/metric-families-and-registration.md`
- Related tests:
  - `//tests/ut/worker:stream_usagemonitor_test`
  - `//tests/st/client/stream_cache:sc_metrics_test`
  - `//tests/st/client/kv_cache:kv_client_log_monitor_test`

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
- `src/datasystem/common/metrics/resource_json_schema.h`
- `src/datasystem/common/metrics/resource_json_schema.cpp`
- `src/datasystem/worker/worker_oc_server.cpp`
- `tests/ut/worker/BUILD.bazel`
- `tests/ut/common/metrics/resource_json_schema_test.cpp`
- `tests/ut/common/log/hard_disk_exporter_test.cpp`
- `tests/st/client/stream_cache/BUILD.bazel`
- `tests/st/client/kv_cache/BUILD.bazel`

## Responsibilities

- Verified:
  - `ResMetricCollector` is a singleton periodic collector.
  - collector interval defaults to `FLAGS_log_monitor_interval_ms`.
  - collector initializes exporter infrastructure when `FLAGS_log_monitor` or `FLAGS_json_log_monitor` is true.
  - after startup, each collection loop re-checks runtime `FLAGS_log_monitor || FLAGS_json_log_monitor`; `resource.log` writes require `log_monitor`, while `kv_resource.log` writes require `json_log_monitor`.
  - current supported exporter selection is `"harddisk"` via `FLAGS_log_monitor_exporter`.
  - `CollectMetrics()` iterates metric IDs from `ResMetricName::SHARED_MEMORY` up to `RES_METRICS_END - 1`.
  - if a metric has no registered handler, an empty field is emitted and a warning is logged once.
  - each handler returns a string payload, not a typed metric object.
- Verified (kv_resource.log JSON output):
  - the same `CollectMetrics()` loop can feed `resource.log` (text, via `HardDiskExporter`, gated by `log_monitor`) and `kv_resource.log` (JSON-Lines, via `JsonLinesExporter`, gated by `json_log_monitor`), sharing one call per handler to avoid resetting interval counters twice (e.g. `GetAndResetIntervalStats`).
  - handler results are cached once into a `std::vector<std::string>` indexed by `ResMetricName`; the text path joins them with `" | "` (unchanged), the JSON path serializes them via `BuildResourceJson`.
  - `resource_json_schema.h` is the single source of truth for the `ResMetricName -> (sub-field names, ods record mask, separator, group name)` mapping; `GetResourceFieldDesc` indexes `DESC_TABLE` by `static_cast<size_t>(name)`, bounded by `RES_METRICS_END`.
  - each kv_resource.log line is a pure JSON object `{"event":"resource_snapshot","version":"v0","pod_name":"...","cluster_name":"...","metrics":{...}}`; `pod_name`/`cluster_name` are JSON-escaped and prepended via the shared `WrapJsonWithPodCluster` helper (same path as kv_metrics.log).
  - ods-whitelisted groups/sub-fields are emitted; ods-dropped groups are omitted entirely; empty or malformed handler output emits an all-zero structure so the group key set is stable across lines.

## Registration Model

- Verified:
  - metrics are not auto-discovered.
  - producers register handlers explicitly through `RegisterCollectHandler`.
  - collector correctness therefore depends on initialization order and registration completeness, not just enum definitions.
- Practical effect:
  - a new metric family can compile cleanly yet still emit blanks until a handler is registered in the right runtime path.

## Lifecycle And Gating

- Startup is gated by:
  - `log_monitor` for `resource.log`
  - `json_log_monitor` for `kv_resource.log`
  - `log_monitor_exporter`
  - exporter initialization success
- Runtime behavior:
  - registered handlers are polled periodically while either `log_monitor=true` or `json_log_monitor=true`
  - returned strings are assembled into exporter output
  - missing handlers do not fail hard

## Compatibility And Change Notes

- Stability-sensitive behavior:
  - collector iteration follows `ResMetricName::SHARED_MEMORY` through `RES_METRICS_END - 1`, so family ordering is operationally visible;
  - initialization semantics depend on `log_monitor` and `log_monitor_exporter`;
  - missing handlers degrade to blank columns and one warning rather than hard failure;
  - `ThreadPoolUsage::ToString` field order now aligns with `res_metrics.def` sub-field names (IDLE_NUM/CURRENT_TOTAL_NUM/MAX_THREAD_NUM/WAITING_TASK_NUM/THREAD_POOL_USAGE); positions 1/3/4 were re-sampled to instant idle / max-thread / instant-waiting while the interval-sampling accumulators are unchanged. Downstream consumers keyed off the old position semantics need to sync.
- Safe change guidance:
  - keep registration timing aligned with runtime startup so handlers are in place before useful collection begins;
  - review blank-column behavior and warning semantics before changing missing-handler handling;
  - treat interval changes as operational behavior changes because they affect file volume and freshness.

## Verification Hints

- Fast source checks:
  - confirm init gating and exporter selection in `src/datasystem/common/metrics/res_metric_collector.cpp`;
  - confirm registration sites in `src/datasystem/worker/worker_oc_server.cpp`;
  - confirm family iteration boundaries in `src/datasystem/common/metrics/res_metric_name.h`.
- Fast validation targets:
  - `bazel test //tests/ut/worker:stream_usagemonitor_test`
  - `bazel test //tests/st/client/stream_cache:sc_metrics_test --test_tag_filters=manual`
- Manual validation:
  - enable monitor logging, register one known family, and confirm that expected values appear in resource output while an intentionally unregistered family yields a blank field plus one warning.

## Common Failure Patterns

- Missing metric values:
  - check whether `log_monitor` is enabled
  - verify the handler was registered before collection starts
  - confirm the handler returns a non-empty payload
- Partial blank columns with only one warning:
  - likely missing `RegisterCollectHandler` coverage for one or more enum IDs
- Metrics stop after startup:
  - inspect collector thread lifecycle, exporter initialization path, and runtime `log_monitor`

## Bugfix And Review Notes

- Good first files when collection cadence or presence looks wrong:
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Common risks:
  - adding a new family without registration silently degrades observability;
  - changing collector loop timing can affect monitor file volume and downstream dashboards.

## Update Rules For This Document

- Keep this file focused on collector lifecycle, init gating, registration behavior, and failure patterns rather than repeating full metrics architecture from `design.md`.
- Update this file when collector startup conditions, registration semantics, interval behavior, or verification entrypoints change.
- If a behavior claim is not confirmed in current source, narrow it or mark it as pending rather than generalizing it.
