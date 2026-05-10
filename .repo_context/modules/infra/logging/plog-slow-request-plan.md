# PLOG Slow Set/Get Request Plan

## Metadata

- Status:
  - `planning`
- Last updated:
  - `2026-05-11`
- Purpose:
  - preserve the design for request slow logs that bypass request log sampling when a Set/Get latency segment is slow.
- Related context:
  - `get-latency-trace-plan.md` is about client access-log latency summaries.
  - This file is about worker/master/runtime logs that must still appear when `log_rate_limit` sampling drops normal request
    logs.

## Problem And Constraints

- Current production-like setup:
  - `enable_urma=true`
  - `enable_worker_worker_batch_get=true`
  - `log_rate_limit=100`
- Requirement:
  - Set/Get P99.99 latency should stay within `2 ms`.
  - When a Set/Get request is slow but the request trace is not sampled, logs must still expose the slow segment.
- Current risk:
  - Existing `LOG(INFO)` and `VLOG(...)` request logs are controlled by request sampling.
  - A slow unsampled request can lose the key trace logs needed for diagnosis.
- Change constraints:
  - Keep the logging-module change small, target under about `100` lines.
  - Prefer existing log carriers and message formats.
  - Do not add synchronous disk writes, forced flushes, new allocation-heavy formatting, or broad refactors on hot paths.
  - Do not rely on `FLAGS_v` for slow logs that must be visible in production.

## PLOG Semantics

`PLOG` means performance slow log:

- It behaves like the current sampled log when the local slow condition is false.
- It bypasses request log sampling when the local slow condition is true, for example `elapsedUs >= thresholdUs`.
- It does not consult or mutate the per-trace sampling decision when it is force-logging a slow event.
- It still respects severity/min-log-level behavior and uses the existing async spdlog sink, rotation, and compression path.
- It should add a recognizable marker such as `[PLOG]` while preserving the existing business log body.
- It must not format the message unless either the request is sampled or the slow condition passes.

Minimal logging-module shape:

```cpp
PLOG_IF(INFO, true, elapsedUs >= thresholdUs) << "[Get] Remote done, ...";
```

For first-pass Get performance points, prefer a VLOG + PLOG balance:

```cpp
if (elapsedUs >= kGetRemoteWorkerRpcSlowUs) {
    PLOG(INFO) << FormatString("[Get] Remote done, count: %d, path: %s, cost: %.3fms", count, path, elapsedMs);
} else {
    VLOG(1) << FormatString("[Get] Remote done, count: %d, path: %s, cost: %.3fms", count, path, elapsedMs);
}
```

Do not pre-format the message outside the branch. Fast requests use `VLOG(1)` and stay silent in production `v=0`; slow
requests use `PLOG(INFO)` and bypass request sampling.

Implementation direction:

- Add a force-log path to `LogMessage`/`LogMessageImpl`.
- Add `ShouldCreateLogMessage(severity, forceLog)` or an equivalent overload.
- Skip `LogRateLimiter::ShouldLog(...)` only when `forceLog` is true.
- Make the public macro preserve the original log condition separately from the slow force condition, so existing sampled
  logs do not disappear for fast requests.
- Keep normal `LOG`, `VLOG`, `LOG_EVERY_N`, and error/fatal behavior unchanged.

## `v=0` Compatibility

- `VLOG(0)` is visible when `FLAGS_v=0`, but it is still a sampled request log today.
- `VLOG(1)` is not visible when `FLAGS_v=0`.
- Required slow logs must use `PLOG_IF`, not `VLOG(vlogLevel)`, so production `v=0` can still diagnose slow Set/Get paths.
- Existing slow logs whose `vlogLevel` becomes `0` can be converted to `PLOG_IF(INFO, slowCondition)` without changing the
  normal fast-path `VLOG(1)` behavior.

## Remote Get Sample Calibration

Reference sample:

- Client-observed total:
  - `1834 us`
- Local worker total:
  - `1492 us`
- QueryMeta RPC on local worker:
  - `433 us`
- Remote get RPC on local worker:
  - `850 us`
- Remote worker local total:
  - `513 us`
- Remote worker URMA wait:
  - `456 us`
- Master local QueryMeta:
  - effectively `0.0 ms`

The first-pass Get segmentation is intentionally limited to seven business segments. Worker API read, worker thread-pool
queueing, and RPC framework sub-breakdowns are not standalone Get PLOG segments.

## Get Thresholds

| Segment | Existing carrier | Source path | Sample | First threshold | Notes |
| --- | --- | --- | ---: | ---: | --- |
| Get local processing | new narrow `[Get] Local processing done ... cost` log | `worker_oc_service_get_impl.cpp` | about `200 us` | `1 ms` | Both local and remote Get run through `TryGetObjectFromLocal`; current aggregate logs are not enough, so use VLOG+PLOG. |
| QueryMeta RPC total | `[Get] Master query done ... cost` | `worker_oc_service_get_impl.cpp` | `433 us` | `1 ms` | Covers local worker to master QueryMeta RPC total. |
| Master local QueryMeta | `QueryMeta done ... cost` | `master_oc_service_impl.cpp` | `0.0 ms` | `2 ms` | Current log uses ms formatting; PLOG condition uses microseconds internally. |
| Remote worker RPC total | `[Get] Remote done ... cost` / `Remote get success, elapsed ...` | `worker_oc_service_batch_get_impl.cpp`, `worker_oc_service_get_impl.cpp` | `850 us` | `2 ms` | Batch path uses `[Get] Remote done`; non-batch path can reuse `Remote get success`. |
| Remote worker local total | `[Get/RemotePull] finish ... cost`; non-batch needs finish cost | `worker_worker_oc_service_impl.cpp` | `513 us` | `2 ms` | Aligns with remote get RPC/payload transfer thresholds. |
| URMA total | `[URMA_ELAPSED_TOTAL] ... cost` | `urma_manager.cpp` | `456 us` | `1 ms` | Use URMA wait total as the first-pass URMA total carrier; `[UrmaWrite]` stays supporting only. |
| Response construction/return | not in current first pass | `worker_request_manager.cpp` | about `90 us` | deferred | ReturnToClient PLOG was removed to keep risk low near release. |

## Get Log Applicability

| Segment | Local Get | Remote Get | Current carrier | Landing |
| --- | --- | --- | --- | --- |
| Get local processing | yes | yes | not enough; only aggregate `[Get] Done` | Add VLOG+PLOG around the `TryGetObjectFromLocal` call boundary. |
| QueryMeta RPC total | no | yes | suitable | Convert `[Get] Master query done` to VLOG+PLOG. |
| Master local QueryMeta | no | yes | suitable but needs us threshold | Use microseconds for the `2 ms` condition. |
| Remote worker RPC total | no | yes | batch suitable; non-batch has `Remote get success` | Use VLOG+PLOG on both batch and non-batch carriers. |
| Remote worker local total | no | yes | batch suitable; non-batch incomplete | Add non-batch finish VLOG+PLOG only if non-batch coverage is required. |
| URMA total | no | yes | suitable | Convert `[URMA_ELAPSED_TOTAL]` to VLOG+PLOG. |
| Response construction/return | yes | yes | not enough; no completion cost | Deferred; do not add ReturnToClient PLOG in the current first pass. |

## Set Thresholds

Set has no remote data-worker pull in the normal publish path. Its distributed latency is primarily worker local work plus
worker-to-master metadata RPC. The thresholds use the same `2 ms` budget and leave room for client-to-worker RPC overhead.

| Segment | Existing carrier | Source path | First threshold | Notes |
| --- | --- | --- | ---: | --- |
| client Set/Put total | `[Set] Done ... totalCost` | `object_client_impl.cpp` | `1 ms` | Single-key Set only; fast path stays behind `VLOG(1)`. |
| client Create RPC | `Finished creating object to worker` and remote `Create` RPC done | `object_client_impl.cpp`, `client_worker_remote_api.cpp` | `1 ms` | Single-key Create only; prints `SHM` or `UB` path when known. |
| client Publish RPC | `Finished publishing object to worker` and remote `Publish` RPC done | `object_client_impl.cpp`, `client_worker_remote_api.cpp` | `1 ms` | Single-key Publish/Set only. |
| worker create total | `Create done, cost` | `worker_oc_service_create_impl.cpp` | `1 ms` | Object creation should be small; slow create often points to allocation/shared-memory pressure. |
| publish metadata RPC | `[Set] CreateMeta/UpdateMeta RPC done` | `worker_oc_service_publish_impl.cpp` | `1 ms` | Covers the single-key worker-to-master metadata RPC wrapper. |
| master CreateMeta local | `CreateMeta done, cost` | `master_oc_service_impl.cpp` | `1 ms` | Single-key CreateMeta only. |
| master UpdateMeta local | `UpdateMeta done, cost` | `master_oc_service_impl.cpp` | `1 ms` | Single-key UpdateMeta only. |
| publish local save/copy | no exact existing completion log for `SaveBinaryObjectToMemory` | `worker_oc_service_publish_impl.cpp` | `300 us` | Needs confirmation before adding a new PLOG if exact local-copy attribution is required. |
| publish l2 write-through | `Save binary object to l2cache begin` and error logs | `worker_oc_service_publish_impl.cpp` | `800 us` | Exact done-time logging would be a new log; keep out of first pass unless write-through SLO diagnosis requires it. |
| publish aggregate | `Publish done, cost` | `worker_oc_service_publish_impl.cpp` | `1 ms` | Single-key Publish/Set only. |

## Remote Set/Get Distribution Definitions

### Get, local hit

1. Get local processing.
2. Response construction/return.

### Get, remote hit through another worker

1. Get local processing.
2. QueryMeta RPC total.
3. Master local QueryMeta.
4. Remote worker RPC total.
5. Remote worker local total.
6. URMA total.
7. Response construction/return.

### Set, normal publish

1. worker Create/Publish request read and basic validation.
2. local object reserve/lock and shared-memory attach or payload preparation.
3. CreateMeta or UpdateMeta RPC to master.
4. master local CreateMeta or UpdateMeta.
5. optional local payload save/copy.
6. optional l2 write-through.
7. response and aggregate `Publish done`.

### Set, remote/master path

Set does not pull data from a remote worker in the normal path. Its remote portion is the metadata RPC to the hash/master
owner. If redirect or meta moving occurs, PLOG should aggregate the retry wrapper duration; detailed per-attempt logging is
not part of the first pass.

## First-Pass Implementation Boundaries

- Convert existing carriers to force-capable `PLOG` where they already have the needed fields. Fast requests keep the current
  sampled behavior; slow requests bypass sampling:
  - `[Get] Local processing done`
  - `[Get] Master query done`
  - `[Get] Remote done`
  - `Remote get success`
  - `[Get/RemotePull] finish`
  - non-batch `[GetObjectRemote] finish`
  - `[URMA_ELAPSED_TOTAL]`
  - `Create done`
  - `Publish done`
  - `[Set] CreateMeta/UpdateMeta RPC done`
  - single-key client Create/Publish/Get/GetObjMetaInfo/Exist RPC done
  - master `CreateMeta`, `UpdateMeta`, `QueryMeta`
- Keep `MCreate`, `MSet`, `MultiCreate`, and `MultiPublish` out of this first pass.
- Keep fast-path sampled logs as they are unless the existing log already has a clear slow condition.
- Avoid changing access-log formatting in this PLOG work.
- Avoid changing `TimeCost::Append` global threshold; it is currently too coarse for sub-`2 ms` SLO diagnosis but changing it
  globally would risk log-volume and formatting changes outside Set/Get.

## New-Log Confirmation Points

## Validation Requirement

- Validation must use the `$ds-test` remote workflow.
- Local build or local test results do not replace remote validation.
- The validation branch must be pushed to the same-name branch on `git@gitcode.com:yaohaolin/yuanrong-datasystem.git`.
- Remote validation must run on `ssh marck@1.95.199.126 -p 22224` under
  `/home/marck/workspace/yuanrong-datasystem`.
- Long build/test commands must write full logs under `/home/marck/workspace/yuanrong-datasystem/logs/` and report concise
  status, failure summaries, and log paths.
- Do not claim validation passed unless the relevant remote build/test commands complete successfully.

The following exact segments are not cleanly covered by existing logs:

- precise Set local payload save/copy time around `SaveBinaryObjectToMemory`;
- precise Set l2 write-through completion time around `SaveBinaryObjectToPersistence`.

First-pass plan:

- Get local processing and response construction/return are now part of the first-pass VLOG+PLOG plan.
- Do not add Set local copy/l2 write-through logs yet.
- If exact attribution for one of these Set local-only segments becomes mandatory, add one narrow VLOG+PLOG at the segment
  boundary after explicit confirmation.

## Tuning Rules

- Start with the thresholds in this file.
- If logs are too sparse for tail diagnosis, lower only the segment that is missing evidence.
- If log volume is too high, raise segment thresholds before raising the aggregate Set/Get thresholds.
- Keep aggregate worker thresholds below the external `2 ms` SLO because client-to-worker overhead consumes part of the
  end-to-end budget.
- Recalibrate with production histograms and sampled full traces after rollout.
