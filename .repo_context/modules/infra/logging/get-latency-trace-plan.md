# Get Latency Trace Plan

## Metadata

- Status:
  - `planning`
- Last updated:
  - `2026-05-11`
- Purpose:
  - persist the agreed plan for adding business-stage latency ticks to the Get request flow.
- Reference sample:
  - commit `40979ec9a91d49be199c3cb4173cbb6394b21304`
- Primary code paths:
  - `src/datasystem/common/log/trace.h`
  - `src/datasystem/common/log/trace.cpp`
  - `src/datasystem/common/log/access_recorder.h`
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/access_point.def`
  - `src/datasystem/client/object_cache/object_client.cpp`
  - `src/datasystem/client/kv_cache/kv_client.cpp`
  - `src/datasystem/client/object_cache/object_client_impl.cpp`
  - `src/datasystem/worker/object_cache/service/worker_oc_service_get_impl.cpp`
  - `src/datasystem/worker/object_cache/service/worker_oc_service_batch_get_impl.cpp`
  - `src/datasystem/worker/object_cache/worker_request_manager.cpp`
  - `src/datasystem/master/object_cache/master_oc_service_impl.cpp`
  - `src/datasystem/worker/object_cache/worker_worker_oc_service_impl.cpp`

## Working Memory Rule

- Before continuing this feature, read this file after the standard `.repo_context` entry files.
- Persist newly agreed decisions here before large code edits, so context compaction does not lose the task state.
- Review this file before making final claims about implementation status, verification, or remaining risk.

## Agreed Decisions

- Summary output location:
  - Put latency summaries in access logs.
  - Only client-side slow-request access logs print segmented latency summaries.
  - Worker, master, and remote data worker access logs do not print segmented latency summaries; their normal trace IDs
    remain enough for correlation.
- Multi-master, multi-remote-worker, and retry scenarios:
  - Record aggregate phase duration only.
  - Do not record every RPC attempt or every target worker/master.
- Meta worker and data worker server-side execution:
  - Cover their server-side execution in the collected phase data.
  - Do not print separate segmented summaries from their local access logs.
- UB Get prefetch metadata:
  - Do not add a standalone `GetObjMetaInfo` latency phase.
  - Count that work as part of the overall client Get stage.
- Access-log summary shape:
  - Do not print `client_total_us` or `worker_total_us`; the existing access-log latency column already carries total cost.
  - Print derived phase durations, not raw tick timestamps.
  - Use compact phase names such as `client.local.get` and `worker.rpc.query_meta`.
- Slow-request gate:
  - Print segmented latency statistics only when the client-side total request latency exceeds `2 ms`.
  - Because printing happens only at the client, the `>2 ms` gate is applied once and does not require worker/master/data
    worker local suppression logic.
- Performance boundary:
  - Get/Set paths are high-QPS hot paths, with target workloads around `3000+ QPS` and `32` concurrent threads.
  - The feature must not add measurable overhead to normal fast requests.

## Compatibility Constraints

- Preserve the current access-log pipe-delimited column shape:
  - `code | handle | latency | dataSize | reqMsg | respMsg`
  - `REQUEST_OUT` may still append its existing async-elapsed column.
- Put the new latency summary inside `reqMsg`, not in a new pipe-delimited column.
- Keep existing `logSampled:true` behavior compatible with the new field.
- Keep RPC framework latency metrics separate from business-stage ticks:
  - existing ZMQ `MetaPb.latency_ticks` remains the RPC framework timeline;
  - Get business-stage ticks live in `Trace`.
- Do not log object payload data, tokens, credentials, or sensitive request fields.

## Proposed Access Log Field

Add one optional request parameter field, tentatively named `latencySummary`.

Example client access log request field:

```text
{Object_key:[key1,key2],timeout:1000,transportType:TCP,latencySummary:{client.local.get:300,client.rpc.get:900,worker.rpc.query_meta:230,master.local.query_meta:180,worker.rpc.remote_get:610,worker.local.remote_get:420}}
```

Values are derived phase durations in microseconds. The summary must never emit raw monotonic tick values.

Do not include total fields such as `client_total_us` or `worker_total_us`, because access logs already contain total
latency in the existing latency column.

Planned phase names:

| Field | Meaning |
| --- | --- |
| `client.local.get` | client-side local processing outside the client-to-worker Get RPC |
| `client.rpc.get` | client-to-worker Get RPC duration |
| `worker.local.get` | primary worker local processing outside outbound QueryMeta, outbound RemoteGet, and explicitly tracked transport subphases |
| `worker.rpc.query_meta` | primary worker to master QueryMeta RPC phase |
| `master.local.query_meta` | master-side local QueryMeta execution |
| `worker.rpc.remote_get` | primary worker to remote-worker RemoteGet phase |
| `worker.urma.urma_total` | URMA write plus URMA wait operation time |
| `worker.local.remote_get` | remote data worker local RemoteGet execution |

Open gating detail:

- Resolved direction:
  - worker/master/data worker access logs do not print segmented summaries.
  - the client access log is the single segmented-summary print point and applies the `>2 ms` client-total gate.
- Open implementation detail:
  - decide how worker/master/data worker derived phase durations are returned or merged back to the client with minimal
    overhead.

## Trace Data Model

Build on the sample commit shape:

- `LatencyTick`
  - `LatencyTickKey key`
  - `uint64_t tick`
- `LATENCY_TICK_MAX_NUM`
  - default `16`
- `Trace`
  - owns a bounded fixed-size tick array;
  - exposes `AddLatencyTick(key)`;
  - drops excess ticks instead of allocating or failing the request;
  - carries tick snapshots through `TraceContext` for cross-thread propagation.

Implementation detail:

- Use monotonic time for tick values.
- Prefer explicit `std::chrono::duration_cast<std::chrono::nanoseconds>` instead of relying on the raw representation of `steady_clock::duration::count()`.

## Proposed Tick Keys

```cpp
enum class LatencyTickKey : uint16_t {
    UNKNOWN = 0,
    CLIENT_GET_START,
    CLIENT_GET_RPC_START,
    CLIENT_GET_RPC_END,
    CLIENT_GET_END,
    WORKER_GET_START,
    WORKER_QUERYMETA_START,
    WORKER_QUERYMETA_END,
    WORKER_REMOTEGET_START,
    WORKER_REMOTEGET_END,
    WORKER_URMA_START,
    WORKER_URMA_END,
    WORKER_GET_END,
    META_QUERYMETA_START,
    META_QUERYMETA_END,
    DATA_REMOTEGET_START,
    DATA_REMOTEGET_END,
};
```

This uses 16 business keys and fills the default capacity of 16. `UNKNOWN` is a sentinel and should not be stored as a
real tick. If implementation needs to store `UNKNOWN` or add any extra phase, raise `LATENCY_TICK_MAX_NUM` slightly.

## Summary Ownership

- `Trace` should store and expose raw ticks.
- Get-specific code should decide how to summarize Get ticks.
- `AccessRecorder` should not need business-specific knowledge of every latency key.
- A small helper can format a summary string from the current `Trace`, but the operation-specific caller should choose when to add it to `RequestParam`.
- Formatting must compute phase durations from tick pairs and should omit phases with missing or invalid endpoints.
- Formatting must not emit raw tick timestamps.
- Avoid allocation and string formatting on the hot path.
- Defer summary construction until the client access-log record point, and only for requests that pass the configured
  slow-request gate.
- Worker/master/data worker paths should collect or return compact numeric phase data only; they should not format
  summary strings.

## Client Get Plan

Source-backed entrypoints:

- public Object Get access log:
  - `src/datasystem/client/object_cache/object_client.cpp`
- public KV Get access logs:
  - `src/datasystem/client/kv_cache/kv_client.cpp`
- shared Get backend:
  - `src/datasystem/client/object_cache/object_client_impl.cpp`

Plan:

- Add `CLIENT_GET_START` at shared `ObjectClientImpl::Get`.
- Add `CLIENT_GET_END` with RAII at the same shared Get boundary.
- Add `CLIENT_GET_RPC_START` and `CLIENT_GET_RPC_END` around `workerApi->Get`.
- Cover both normal `GetBuffersFromWorker` and UB batch `GetBuffersFromWorkerBatched`.
- At public KV/Object access-log record points, append a segmented summary to `RequestParam` only when the client total
  access-log latency is greater than `2 ms`.
- The client summary is the only place where segmented latency is printed.

Client summary should at least include:

- `client.rpc.get = CLIENT_GET_RPC_END - CLIENT_GET_RPC_START`
- `client.local.get = (CLIENT_GET_END - CLIENT_GET_START) - client.rpc.get`

Do not print client total; it is already the access-log latency column.

## Primary Worker Get Plan

Source-backed entrypoints:

- worker Get service:
  - `src/datasystem/worker/object_cache/service/worker_oc_service_get_impl.cpp`
- batch remote-get helper:
  - `src/datasystem/worker/object_cache/service/worker_oc_service_batch_get_impl.cpp`
- worker Get access log:
  - `src/datasystem/worker/object_cache/worker_request_manager.cpp`

Plan:

- Add `WORKER_GET_START` after `serverApi->Read(req)` succeeds in `WorkerOcServiceGetImpl::Get`.
- Add `WORKER_QUERYMETA_START` and `WORKER_QUERYMETA_END` around the aggregate `QueryMetadataFromMaster` phase in the Get flow.
- Add `WORKER_REMOTEGET_START` and `WORKER_REMOTEGET_END` around the aggregate data-fetch phase, preferably at `GetObjectsFromAnywhere` so serial, parallel, and batch implementations are covered.
- Add `WORKER_GET_END` in `GetRequest::ReturnToClient` or an equivalent response boundary so worker phase durations can
  be finalized.
- Do not append a worker latency summary to `DS_POSIX_GET`.
- Return or otherwise merge worker phase durations into the client-side summary data path.

Worker-derived fields for the client-side summary should at least include:

- `worker.rpc.query_meta = WORKER_QUERYMETA_END - WORKER_QUERYMETA_START`
- `worker.rpc.remote_get = WORKER_REMOTEGET_END - WORKER_REMOTEGET_START`
- `worker.urma.urma_total = WORKER_URMA_END - WORKER_URMA_START`
- `worker.local.get = (WORKER_GET_END - WORKER_GET_START) - known worker subphases`

Do not print worker summary locally. These values are compact numeric phase data intended for the client-side slow-request
summary if propagation is implemented for the request path.

## Meta Worker Plan

Source-backed entrypoint:

- `src/datasystem/master/object_cache/master_oc_service_impl.cpp`
  - `MasterOCServiceImpl::QueryMeta`

Plan:

- Add `META_QUERYMETA_START` at QueryMeta service entry after auth succeeds or at the narrowest source-backed service boundary chosen during implementation.
- Add `META_QUERYMETA_END` with RAII before returning QueryMeta.
- Do not add a meta-worker access key only for segmented latency summary printing.
- Return or otherwise merge the derived meta phase duration into the client-side summary data path.

Meta-worker-derived field for the client-side summary:

- `master.local.query_meta = META_QUERYMETA_END - META_QUERYMETA_START`

Open detail:

- Confirm the lowest-overhead propagation path for `master.local.query_meta` back to the original client.

## Data Worker Plan

Source-backed entrypoints:

- `src/datasystem/worker/object_cache/worker_worker_oc_service_impl.cpp`
  - `WorkerWorkerOCServiceImpl::GetObjectRemote`
  - `WorkerWorkerOCServiceImpl::BatchGetObjectRemote`

Plan:

- Add `DATA_REMOTEGET_START` at remote-get service entry after reading the request.
- Add `DATA_REMOTEGET_END` with RAII before returning remote-get response.
- Cover both single remote get and batch remote get.
- Do not add a data-worker access key only for segmented latency summary printing.
- Return or otherwise merge the derived data-worker phase duration into the client-side summary data path.

Data-worker-derived field for the client-side summary:

- `worker.local.remote_get = DATA_REMOTEGET_END - DATA_REMOTEGET_START`

## Important Source Findings

- `Trace` is thread-local.
  - Tick propagation through `TraceContext` handles thread-pool and timer callbacks.
  - It does not automatically merge child-thread ticks back into a parent thread.
- `ZMQ MetaPb.latency_ticks` already exists for RPC framework latency metrics.
  - This should not be reused for Get business-stage ticks.
- Access log formatting is centralized in `AccessRecorderManager::LogPerformance`.
  - It currently writes fixed pipe-delimited fields and delegates file output to `HardDiskExporter`.
- `RequestParam::ToString()` is the right compatibility-preserving place to add a new request metadata field.
- Worker `DS_POSIX_GET` access logging is done from `GetRequest::ReturnToClient`, but it should not print segmented
  latency summaries under the current decision.
- Derived summaries must be computed from tick differences; raw monotonic timestamps are implementation detail only.
- Client-only summary printing resolves strict slow-request gating without requiring worker/master/data-worker local log
  suppression.

## Open Questions

- Exact field name for the new `RequestParam` member:
  - current proposal: `latencySummary`.
- Propagation path for worker/master/data-worker phase durations back to the client:
  - pending implementation design; must be compact and avoid per-request string work.
- Whether to include dropped tick count in summaries:
  - current proposal: include only when non-zero.
- Whether `worker.urma.urma_total` is nested inside `worker.rpc.remote_get` or should be subtracted from it for
  non-overlapping phase totals:
  - current requirement names both phases; implementation must avoid misleading double-counted summaries.
- Whether summary field names should carry `_us` suffix:
  - current proposal: no suffix in names, values are documented as microseconds.

## Validation Plan

- Unit tests:
  - add or extend Trace tests for tick add, bound/drop, invalidation, and `TraceContext` propagation.
  - add access-recorder formatting test for `latencySummary` coexisting with `logSampled:true`.
  - add formatter tests proving raw tick timestamps are not emitted and total fields are omitted.
  - add threshold tests proving client-side segmented summary is omitted when total Get latency is at or below `2 ms`.
- Build-focused checks:
  - build affected logging, client, worker, and master targets.
- Runtime/manual validation:
  - run one representative Get path and confirm:
    - slow client access log contains the segmented summary;
    - fast client access log omits the segmented summary;
    - primary worker, meta worker, and data worker access logs do not contain segmented latency summaries;
    - all records share the same trace ID where the existing RPC trace propagation applies.

## Change Boundaries

- Do not widen scope to Create/Publish until Get is implemented and validated.
- Do not alter access-log pipe field order.
- Do not introduce dynamic allocation on the hot tick-add path.
- Do not format summary strings before the access-log record point.
- Do not print segmented summaries for fast client requests at or below the agreed `2 ms` threshold where the client-side
  gate is available.
- Do not print client or worker total fields inside `latencySummary`.
- Do not emit raw tick timestamps.
- Do not print segmented latency summaries from worker, master, or data worker access logs.
- Do not require all stages to be present for a summary to print.
- Missing stages should simply be omitted from the summary.
