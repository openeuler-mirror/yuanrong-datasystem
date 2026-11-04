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
- `src/datasystem/common/rpc/rpc_server.h`
- `src/datasystem/common/rpc/rpc_server.cpp`
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
  - collector initializes exporter infrastructure unconditionally; `resource.log` writes require `log_monitor`, while
    `kv_resource.log` JSON snapshots are always collected and written.
  - after startup, each collection loop runs unconditionally; `resource.log` writes re-check runtime
    `FLAGS_log_monitor`.
  - current supported exporter selection is `"harddisk"` via `FLAGS_log_monitor_exporter`.
  - `CollectMetrics()` iterates metric IDs from `ResMetricName::SHARED_MEMORY` up to `RES_METRICS_END - 1`.
  - unregistered or disabled groups are skipped before sampling and produce no field in either output.
  - each handler returns a string payload, not a typed metric object.
- Verified (kv_resource.log JSON output):
  - the same `CollectMetrics()` loop feeds gated text `resource.log` and always-on JSON-Lines `kv_resource.log`. It
    calls each handler once to avoid resetting interval counters twice, such as `GetAndResetIntervalStats`.
  - handler results are cached once into a `std::vector<std::string>` indexed by `ResMetricName`; text retains raw values in enum order separated by `" | "`, while JSON serializes the same samples via `BuildResourceJson`.
  - `resource_json_schema.h` is the single source of truth for the `ResMetricName -> (sub-field names, ods record mask, separator, group name)` mapping; `GetResourceFieldDesc` indexes `DESC_TABLE` by `static_cast<size_t>(name)`, bounded by `RES_METRICS_END`.
  - each kv_resource.log line is a pure JSON object `{"event":"resource_snapshot","version":"v0","pod_name":"...","cluster_name":"...","metrics":{...}}`; `pod_name`/`cluster_name` are JSON-escaped and prepended via the shared `WrapJsonWithPodCluster` helper (same path as kv_metrics.log).
  - ods-whitelisted groups/sub-fields are emitted for registered, enabled groups; ods-dropped groups are omitted from JSON. Empty or malformed samples retain the existing JSON all-zero fallback for enabled groups.
  - `enabled=false` suppresses sampling and the complete group in both resource logs; disabled groups reserve no text column. Enabled empty values and the original trailing separator remain.
  - normal `ObjectCopyWatermark::ToMetricsString()` output formats its five ratio fields with two fixed decimal places; JSON serialization preserves those numeric tokens, including trailing zeros.

## Registration Model

- Verified:
  - metrics are not auto-discovered.
  - producers register handlers explicitly through `RegisterCollectHandler(metricName, handler, enabled = true)`.
  - the handler and its shared collection/output flag are registered together before collection starts; existing callers default to enabled.
  - collector correctness therefore depends on initialization order and registration completeness, not just enum definitions.
- Practical effect:
  - a new metric family is emitted only after its enabled handler is registered in the right runtime path.

Worker registration determines these optional groups in both logs from the startup configuration:

| Group | Collection and output condition |
| --- | --- |
| `etcd_request_success_rate` | `!FLAGS_etcd_address.empty()` |
| `spill_hard_disk` | `WorkerOcSpill::Instance()->IsEnabled()` |
| `spill_io_stats` | `WorkerOcSpill::Instance()->IsEnabled()` |
| `object_copy_watermark` | `FLAGS_eviction_strategy != "clock"` |

These flags are captured as booleans at registration, rather than re-evaluated each collection cycle. The mixed
`oc_hit_num` group retains all existing fields, including `disk_hit_num`.

## Removed RPC Thread-Pool Metrics

- The obsolete eight OC/SC RPC service thread-pool groups have been removed from the enum, schema, descriptions and
  registration. They leave no columns in text output. `master_async_tasks_thread_pool` continues to describe its real
  DS asynchronous pool.
- The change removes the legacy interval-reporting path without adding replacement BRPC metrics or changing
  `GetRpcServicesSnapshot()` and the liveness-check policy.

## Lifecycle And Gating

- Startup is gated by:
  - `log_monitor` for `resource.log`
  - `log_monitor_exporter`
  - exporter initialization success
- Runtime behavior:
  - registered handlers are polled periodically; `log_monitor` gates only the `resource.log` text writes
  - returned strings are assembled into exporter output
  - groups without registered handlers are omitted

## Compatibility And Change Notes

- Stability-sensitive behavior:
  - optional groups are absent from both logs when disabled; consumers should treat absence as configuration-disabled rather than as a zero measurement;
  - text retains its seven logging prefix fields and original raw-value format, with no metric names, extra header or version field; omitting a group moves later columns forward, so consumers must account for the actual enabled groups;
  - the old RPC metric enum entries and text columns are deleted; remaining enum entries stay aligned with the schema table;
  - `scripts/generate_resource_report.py` reads current Worker raw-value columns using the group structure and sub-field counts, retains historical fixed-column parsing, and no longer models the removed RPC thread pools;
  - an enabled group remains present for zero values or malformed/empty handler results; the latter keep the existing all-zero fallback;
  - collector iteration follows `ResMetricName::SHARED_MEMORY` through `RES_METRICS_END - 1`, so family ordering is operationally visible;
  - initialization semantics depend on `log_monitor` and `log_monitor_exporter`;
  - `ThreadPoolUsage::ToString` field order now aligns with `res_metrics.def` sub-field names (IDLE_NUM/CURRENT_TOTAL_NUM/MAX_THREAD_NUM/WAITING_TASK_NUM/THREAD_POOL_USAGE); positions 1/3/4 were re-sampled to instant idle / max-thread / instant-waiting while the interval-sampling accumulators are unchanged. Downstream consumers keyed off the old position semantics need to sync.
- Safe change guidance:
  - keep registration timing aligned with runtime startup so handlers are in place before useful collection begins;
  - update text readers with format changes; disabled and unregistered groups do not reserve columns;
  - treat interval changes as operational behavior changes because they affect file volume and freshness.

## Verification Hints

- Fast source checks:
  - confirm init gating and exporter selection in `src/datasystem/common/metrics/res_metric_collector.cpp`;
  - confirm registration sites in `src/datasystem/worker/worker_oc_server.cpp`;
  - confirm family iteration boundaries in `src/datasystem/common/metrics/res_metric_name.h`.
- Fast validation targets:
  - CMake/CTest: `ctest -V -R '^KVClientLogMonitorTest\.TestObjectSize$' --output-on-failure`
  - `bazel test //tests/ut/common/log:hard_disk_exporter_test`
  - `bazel test //tests/ut/worker:stream_usagemonitor_test`
  - `bazel test //tests/st/client/stream_cache:sc_metrics_test --test_tag_filters=manual`
- Manual validation:
  - enable monitor logging and verify that registered, enabled groups appear once in both outputs while disabled and unregistered groups leave no column or JSON key.
  - existing ST readers obtain object size and copy watermarks from named JSON fields. Stream count and SC memory fields are outside the JSON whitelist, so stream fixtures map their configured text groups explicitly; enum values are not emitted column indexes.

## Common Failure Patterns

- Missing metric values:
  - check whether `log_monitor` is enabled
  - verify the handler was registered before collection starts
  - confirm the handler returns a non-empty payload
- Missing groups:
  - check whether the group has a registered, enabled handler; an enabled handler with malformed data retains JSON fallback and warning behavior
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
