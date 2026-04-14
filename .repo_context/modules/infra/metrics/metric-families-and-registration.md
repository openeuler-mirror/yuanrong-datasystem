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
  - `src/datasystem/worker/worker_oc_server.cpp`
- Last verified against source:
  - `2026-04-13`
- Related design docs:
  - `.repo_context/modules/infra/metrics/design.md`
  - `.repo_context/modules/infra/metrics/resource-collector.md`
- Related tests:
  - `//tests/ut/worker:stream_usagemonitor_test`
  - `//tests/st/client/stream_cache:sc_metrics_test`
  - `//tests/st/client/kv_cache:kv_client_log_monitor_test`

## Scope

- Paths:
  - `src/datasystem/common/metrics/res_metrics.def`
  - `src/datasystem/common/metrics/metrics_description.def`
  - `src/datasystem/common/metrics/res_metric_name.h`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Why this document exists:
  - explain how metric families are defined, documented, and connected to runtime registration points.

## Primary Source Files

- `src/datasystem/common/metrics/res_metrics.def`
- `src/datasystem/common/metrics/metrics_description.def`
- `src/datasystem/common/metrics/res_metric_name.h`
- `src/datasystem/worker/worker_oc_server.cpp`
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

## Alignment Rules

- A new metric family usually requires all of:
  - enum entry in `res_metrics.def`
  - meaning/unit description in `metrics_description.def`
  - handler registration in runtime code
  - collector/exporter path already enabled for the target process
- Practical effect:
  - definition, description, and registration must move together to avoid blank or misleading monitor output.

## Compatibility And Change Notes

- Stability-sensitive behavior:
  - `res_metrics.def` explicitly marks family order as immutable for compatibility purposes;
  - `metrics_description.def` is part of the semantic contract for operators reading the output;
  - runtime registration in `worker_oc_server.cpp` must stay aligned with both definitions and descriptions.
- Safe change guidance:
  - do not reorder existing families;
  - update definitions, descriptions, and registrations in the same change;
  - review whether downstream parsers, dashboards, or runbooks assume current order or family presence before changing them.

## Verification Hints

- Fast source checks:
  - confirm family order and new enum entries in `src/datasystem/common/metrics/res_metrics.def`;
  - confirm descriptions and units in `src/datasystem/common/metrics/metrics_description.def`;
  - confirm runtime registration in `src/datasystem/worker/worker_oc_server.cpp`.
- Fast validation targets:
  - `bazel test //tests/ut/worker:stream_usagemonitor_test`
  - `bazel test //tests/st/client/stream_cache:sc_metrics_test --test_tag_filters=manual`
- Manual validation:
  - run one representative monitor scenario and confirm the new or changed family appears in the expected order with the intended meaning.

## Bugfix And Review Notes

- Good first files when one family is missing, misordered, or undocumented:
  - `src/datasystem/common/metrics/res_metrics.def`
  - `src/datasystem/common/metrics/metrics_description.def`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Common risks:
  - reordering enum definitions can corrupt the meaning of existing output columns;
  - registering a handler for the wrong family can produce plausible-looking but semantically wrong metrics.

## Update Rules For This Document

- Keep this file focused on family definitions, semantic descriptions, and runtime registration alignment instead of repeating full collector or exporter architecture from `design.md`.
- Update this file when family order, family meaning, description units, or major registration points change.
- If a registration claim depends on a callsite not yet verified, mark it as pending rather than treating it as generally true.
