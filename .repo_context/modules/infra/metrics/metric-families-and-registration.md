# Metric Families And Registration

## Document Metadata

- Status:
  - `active`
- Doc type:
  - behavior note | submodule reference
- Primary code paths:
  - `src/datasystem/common/metrics/res_metrics.def`
  - `src/datasystem/common/metrics/metrics_description.def`
  - `src/datasystem/common/metrics/res_metric_name.h`
  - `src/datasystem/common/metrics/metrics.h`
  - `src/datasystem/common/metrics/metrics.cpp`
  - `src/datasystem/common/metrics/kv_metrics.h`
  - `src/datasystem/common/metrics/kv_metrics.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/coordinator/coordinator_runtime.cpp`
  - `src/datasystem/coordinator/coordinator_service_impl.cpp`
  - `src/datasystem/coordinator/watch_dispatcher_impl.cpp`
- Last verified against source:
  - `2026-08-10`
- Related design docs:
  - `.repo_context/modules/infra/metrics/design.md`
  - `.repo_context/modules/infra/metrics/resource-collector.md`
- Related tests:
  - `//tests/ut/common/metrics:metrics_test`
  - `//tests/ut/coordinator:coordinator_service_impl_test`
  - `//tests/ut/worker:stream_usagemonitor_test`
  - `//tests/st/client/stream_cache:sc_metrics_test`
  - `//tests/st/client/kv_cache:kv_client_log_monitor_test`

## Scope

- Paths:
  - `src/datasystem/common/metrics/res_metrics.def`
  - `src/datasystem/common/metrics/metrics_description.def`
  - `src/datasystem/common/metrics/res_metric_name.h`
  - `src/datasystem/common/metrics/metrics.h`
  - `src/datasystem/common/metrics/metrics.cpp`
  - `src/datasystem/common/metrics/kv_metrics.h`
  - `src/datasystem/common/metrics/kv_metrics.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/coordinator/coordinator_runtime.cpp`
  - `src/datasystem/coordinator/coordinator_service_impl.cpp`
  - `src/datasystem/coordinator/watch_dispatcher_impl.cpp`
- Why this document exists:
  - explain how metric families are defined, documented, and connected to runtime registration points.

## Primary Source Files

- `src/datasystem/common/metrics/res_metrics.def`
- `src/datasystem/common/metrics/metrics_description.def`
- `src/datasystem/common/metrics/res_metric_name.h`
- `src/datasystem/common/metrics/metrics.h`
- `src/datasystem/common/metrics/metrics.cpp`
- `src/datasystem/common/metrics/kv_metrics.h`
- `src/datasystem/common/metrics/kv_metrics.cpp`
- `src/datasystem/worker/worker_oc_server.cpp`
- `src/datasystem/coordinator/coordinator_runtime.cpp`
- `src/datasystem/coordinator/coordinator_service_impl.cpp`
- `src/datasystem/coordinator/watch_dispatcher_impl.cpp`
- `tests/ut/common/metrics/metrics_test.cpp`
- `tests/ut/coordinator/coordinator_service_impl_test.cpp`
- `tests/ut/worker/BUILD.bazel`
- `tests/st/client/stream_cache/BUILD.bazel`
- `tests/st/client/kv_cache/BUILD.bazel`

## Metric Definitions

- Verified from `res_metrics.def` and `metrics_description.def`:
  - metrics are grouped by named families such as:
    - `SHARED_MEMORY`
    - `SPILL_HARD_DISK`
    - `ACTIVE_CLIENT_COUNT`
    - `OBJECT_COUNT`
    - `OBJECT_SIZE`
    - worker/master thread-pool families
    - `ETCD_QUEUE`
    - `ETCD_REQUEST_SUCCESS_RATE`
    - `OBS_REQUEST_SUCCESS_RATE`
    - stream families
    - `SHARED_DISK`
    - `SC_LOCAL_CACHE`
    - `OC_HIT_NUM`
  - descriptions file also records human meanings and units for many families.
- Review caution:
  - `res_metrics.def` explicitly warns not to change order, so enum-order stability matters.

## Major Registration Points

- Verified in `worker_oc_server.cpp`:
  - worker-side registration includes:
    - spill hard disk usage
    - shared memory usage
    - shared disk usage
    - worker object-cache service thread-pool usage
    - worker-worker object-cache service thread-pool usage
    - active client count
    - object count and object size
    - object cache hit stats
    - stream count
    - worker stream thread-pool usage
    - stream remote send success rate
    - stream local cache usage
  - master/coordination-side registration includes:
    - master-worker object-cache thread-pool usage
    - master object-cache thread-pool usage
    - ETCD queue usage
    - master async task thread-pool usage
    - master/worker stream service thread-pool usage
  - third-party/backend registration includes:
    - ETCD request success rate
    - OBS request success rate
  - Coordinator typed counters include:
    - one handler-dispatch request counter for each of the 12 `CoordinatorService` RPC methods;
    - successfully delivered watch-notification RPC batches;
    - successfully delivered watch events;
    - serialized `EventReqPb` bytes in successful business notifications, excluding transport framing.
  - Worker reachability probes use a separate RPC path and are excluded from all watch-notification counters.
  - The per-method request boundary is the first line of the shared service handler, before handler validation and serving gates. Requests rejected by the RPC framework before service dispatch are tracked only by framework-level metrics.
  - `CoordinatorRuntime` initializes the typed registry, calls `metrics::Tick()` outside its lifecycle mutex, and prints a final summary after service shutdown drains the metric producers.
  - typed counter throughput is derived from each summary's `delta` and top-level `interval_ms`: request/event TPS and byte B/s both use `delta * 1000 / interval_ms`.

## Alignment Rules

- A new resource-collector family requires all of:
  - enum entry in `res_metrics.def`
  - meaning/unit description in `metrics_description.def`
  - handler registration in runtime code
  - collector/exporter path already enabled for the target process
- A new typed metric requires all of:
  - append-only `KvMetricId` entry in `kv_metrics.h`;
  - matching same-index `MetricDesc` in `kv_metrics.cpp`;
  - producer-side `METRIC_*` update;
  - process lifecycle initialization plus periodic `Tick()`/final `PrintSummary()` when the process does not already drive typed metrics;
  - direct `common_metrics` dependency in both CMake and Bazel targets that include or invoke the API.
- Practical effect:
  - each mechanism's definition, semantics, producer, lifecycle, build dependencies, and tests must move together to avoid blank or misleading output.

## Compatibility And Change Notes

- Stability-sensitive behavior:
  - `res_metrics.def` explicitly marks family order as immutable for compatibility purposes;
  - `metrics_description.def` is part of the semantic contract for operators reading the output;
  - runtime registration in `worker_oc_server.cpp` must stay aligned with both definitions and descriptions;
  - typed metric IDs in `kv_metrics.h` and descriptors in `kv_metrics.cpp` are append-only and must remain in lockstep;
  - Coordinator request counters are incremented before validation/serving gates, while watch-notification batch, event, and serialized-byte counters are incremented only after a successful business notification RPC. The watch RPC histogram measures `HandleEvent` only, the failure counter follows its returned status, and the channel gauge is updated from `channels_.size()` while holding the channel-map write lock.
- Safe change guidance:
  - do not reorder existing families;
  - update definitions, descriptions, and registrations in the same change;
  - review whether downstream parsers, dashboards, or runbooks assume current order or family presence before changing them.

## Verification Hints

- Fast source checks:
  - for resource metrics, confirm family order, descriptions, and runtime registration in `res_metrics.def`, `metrics_description.def`, and `worker_oc_server.cpp`;
  - for typed metrics, confirm append-only ID/descriptor lockstep in `kv_metrics.h/.cpp`, producer updates, lifecycle driving, and CMake/Bazel direct dependencies;
  - for Coordinator counters, compare the 12 `CoordinatorService` methods in `coordinator.proto` with the 12 service-entry increments and verify that probe traffic bypasses notification counters.
- Fast validation targets:
  - `bazel test //tests/ut/common/metrics:metrics_test`
  - `bazel test //tests/ut/coordinator:coordinator_service_impl_test`
  - `bazel test //tests/ut/worker:stream_usagemonitor_test`
  - `bazel test //tests/st/client/stream_cache:sc_metrics_test --test_tag_filters=manual`
- Manual validation:
  - run one representative monitor scenario and confirm the new or changed family appears in the expected order with the intended meaning.

## Bugfix And Review Notes

- Good first files when one family is missing, misordered, or undocumented:
  - resource metrics: `res_metrics.def`, `metrics_description.def`, and `worker_oc_server.cpp`;
  - typed metrics: `kv_metrics.h`, `kv_metrics.cpp`, the producer callsite, and the owning process lifecycle.
- Common risks:
  - reordering enum definitions can corrupt the meaning of existing output columns;
  - registering a handler for the wrong family can produce plausible-looking but semantically wrong metrics.

## Update Rules For This Document

- Keep this file focused on family definitions, semantic descriptions, and runtime registration alignment instead of repeating full collector or exporter architecture from `design.md`.
- Update this file when family order, family meaning, description units, or major registration points change.
- If a registration claim depends on a callsite not yet verified, mark it as pending rather than treating it as generally true.
