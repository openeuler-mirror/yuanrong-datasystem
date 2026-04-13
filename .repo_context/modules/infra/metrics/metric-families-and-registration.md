# Metric Families And Registration

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

## Bugfix And Review Notes

- Good first files when one family is missing, misordered, or undocumented:
  - `src/datasystem/common/metrics/res_metrics.def`
  - `src/datasystem/common/metrics/metrics_description.def`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Common risks:
  - reordering enum definitions can corrupt the meaning of existing output columns;
  - registering a handler for the wrong family can produce plausible-looking but semantically wrong metrics.
