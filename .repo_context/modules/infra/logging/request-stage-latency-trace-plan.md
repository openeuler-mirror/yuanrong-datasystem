# Get/Set/Create/Exist Latency Trace Plan

## Metadata

- Status:
  - `planning`
- Last updated:
  - `2026-05-27`
- Purpose:
  - persist the agreed plan for adding business-stage latency ticks and access-log summaries to Get, Set, Create, and
    Exist request flows.
- Reference sample:
  - commit `40979ec9a91d49be199c3cb4173cbb6394b21304`

## Working Memory Rule

- Before continuing this feature, read this file after the standard `.repo_context` entry files.
- Keep repository Chinese plan `request-stage-latency-trace-plan-zh.md` in sync with this file.
- Persist newly agreed decisions before large code edits, so context compaction does not lose the task state.

## Agreed Decisions

- Interface scope:
  - Cover `Get`, `Set`, `Create`, and `Exist` first.
  - `Set` includes KV `Set` APIs and the object `Put` path, because both enter `ObjectClientImpl::Set`/`Put` and may
    execute client-to-worker `Create` plus `Publish`.
  - Multi-operation variants such as `MSet`, `MCreate`, and `MSetNX` are not part of this plan; expanding to them needs
    a separate design update.
- Summary output location:
  - Put latency summaries in access logs.
  - Client-side slow-request access logs print the client-observed segmented latency summary.
  - Primary worker slow-request access logs also print worker-side segmented latency summaries, so server-side phase data
    is still available when the client times out before it can write its access log.
  - Master and remote data worker access logs do not print segmented latency summaries in this plan; their local phase
    durations are returned as compact numeric data and merged into primary-worker/client summaries.
- Slow-request gate:
  - The old single `2 ms` gate is not the final control surface.
  - Worker-side gates use `--slow_log_local_slower_than` and `--slow_log_rpc_slower_than`.
  - Client-side gates use `DATASYSTEM_CLIENT_SLOW_LOG_LOCAL_SLOWER_THAN` and
    `DATASYSTEM_CLIENT_SLOW_LOG_RPC_SLOWER_THAN`.
  - Config values are in microseconds. All four configs default to `0`, meaning segmented slow logging and phase
    collection are disabled by default.
  - Threshold semantics: `0` disables the gate; positive values log phases above that microsecond threshold.
  - `LatencyTraceEnabled()` is true only when at least one local/rpc threshold is positive.
  - The same local/rpc thresholds also drive the force condition for request-stage `SLOW_LOG` output; hard-coded
    `*_SLOW_US` constants must not remain the final control surface.
  - The gate must run before summary string construction on each side; request paths must not call `getenv()` or parse
    strings.
- Access-log shape:
  - Preserve current pipe-delimited columns: `code | handle | latency | dataSize | reqMsg | respMsg`.
  - Put the optional `latencySummary` field inside `reqMsg`; do not add a new pipe-delimited column.
  - Do not print `client_total_us` or `worker_total_us`; the existing access-log latency column already carries total
    cost.
  - Print derived phase durations in microseconds, not raw monotonic tick values.
- Multi-master, multi-remote-worker, and retry scenarios:
  - Record aggregate phase duration only.
  - Do not record every RPC attempt or every target worker/master.
- Meta worker and data worker server-side execution:
  - Cover their server-side execution in collected phase data.
  - Prefer returning or merging their compact numeric phase data into primary worker/client summaries instead of printing
    separate segmented summaries from their local access logs.
- UB Get prefetch metadata:
  - Do not add a standalone `GetObjMetaInfo` latency phase.
  - Count that work as part of the overall client Get stage.
- Performance boundary:
  - Get/Set paths are high-QPS hot paths, with target workloads around `3000+ QPS` and `32` concurrent threads.
  - Avoid dynamic allocation, map lookup, environment-variable reads, flag string parsing, and string formatting on hot
    tick-add paths.
  - When local/rpc configs are both `0` on a side, that side must not collect segmented ticks, merge downstream phases,
    attach compact phases, or forward `latency_phase_us`.
  - Slow logging itself must not introduce measurable throughput, p99, CPU, lock-contention, or response-size
    regression.
- Protobuf boundary:
  - Do not reuse `MetaPb.latency_ticks`; it belongs to ZMQ/RPC latency metrics and uses string-named `TickPb` entries.
  - Do not serialize raw ticks or formatted `latencySummary` strings across RPC.
  - Return derived phase durations as packed `uint32` key/value pairs in response protos:
    `repeated uint32 latency_phase_us = N [packed = true]`.
  - Add `uint32 latency_tick_dropped_count = M` only for non-zero dropped tick count.
  - Place the compact response fields only on the needed response messages:
    `CreateRspPb`, `PublishRspPb`, `GetRspPb`, `ExistRspPb`, `CreateMetaRspPb`, `QueryMetaRspPb`,
    `UpdateMetaRspPb`, `GetObjectRemoteRspPb`, and top-level `BatchGetObjectRemoteRspPb`.
- Formalization boundary:
  - The design is the formal target state, not an intermediate rollout note.
  - Do not introduce temporary fields, raw-tick debug output, or transitional logs that need later replacement.

## Planned Phase Names

| Field | Meaning |
| --- | --- |
| `client.local.get` | Client-side Get local processing outside the client-to-worker Get RPC. |
| `client.rpc.get` | Client-to-worker Get RPC duration. |
| `worker.local.get` | Primary worker Get local processing outside QueryMeta, RemoteGet, and explicitly tracked transport phases. |
| `worker.rpc.query_meta` | Primary worker to master QueryMeta RPC phase. |
| `master.local.query_meta` | Master-side local QueryMeta execution. |
| `worker.rpc.remote_get` | Primary worker to remote-worker RemoteGet phase. |
| `worker.urma.urma_total` | URMA write plus URMA wait operation time. |
| `worker.local.remote_get` | Remote data worker local RemoteGet execution. |
| `client.local.set` | Client-side Set/Put local processing outside tracked Create/Publish RPC and memory copy phases. |
| `client.rpc.create` | Client-to-worker Create RPC duration, used by Create and by Set paths that allocate a buffer first. |
| `client.local.memory_copy` | Client local copy into SHM/UB buffer during Set/Put paths when present. |
| `client.rpc.publish` | Client-to-worker Publish RPC duration during Set/Put or buffer publish. |
| `client.local.create` | Client-side Create local processing outside the Create RPC. |
| `client.local.exist` | Client-side Exist local processing outside the Exist RPC. |
| `client.rpc.exist` | Client-to-worker Exist RPC duration. |
| `worker.local.create` | Primary worker local Create execution. |
| `worker.local.publish` | Primary worker local Publish execution outside tracked CreateMeta/UpdateMeta RPC phases. |
| `worker.rpc.create_meta` | Primary worker to master CreateMeta RPC phase during Publish/Set. |
| `worker.rpc.update_meta` | Primary worker to master UpdateMeta RPC phase during Publish/Set. |
| `master.local.create_meta` | Master-side local CreateMeta execution. |
| `master.local.update_meta` | Master-side local UpdateMeta execution. |
| `worker.local.exist` | Primary worker local Exist execution outside QueryMeta. |

Example client Get request field:

```text
{Object_key:[key1,key2],timeout:1000,transportType:TCP,latencySummary:{client.local.get:300,client.rpc.get:900,worker.local.get:260,worker.rpc.query_meta:230,master.local.query_meta:180,worker.rpc.remote_get:610,worker.local.remote_get:420}}
```

Example client Set request field:

```text
{Object_key:key1,transportType:TCP,latencySummary:{client.local.set:320,client.rpc.create:430,client.local.memory_copy:210,client.rpc.publish:850,worker.local.publish:260,worker.rpc.create_meta:410,master.local.create_meta:180}}
```

Example primary worker request field:

```text
{Object_key:key1,latencySummary:{worker.local.publish:260,worker.rpc.create_meta:410,master.local.create_meta:180}}
```

## Trace Data Model

Build on the sample commit shape:

- `LatencyTick`
  - `LatencyTickKey key`
  - `uint64_t tick`
- `LATENCY_TICK_MAX_NUM`
  - default `16`, but this is per-request tick capacity, not the maximum enum value.
- `Trace`
  - owns a bounded fixed-size tick array;
  - exposes `AddLatencyTick(key)`;
  - drops excess ticks instead of allocating or failing the request;
  - carries tick snapshots through `TraceContext` for cross-thread propagation.

Implementation guidance:

- Use monotonic time for tick values.
- Prefer explicit `std::chrono::duration_cast<std::chrono::nanoseconds>` instead of relying on the raw representation of
  `steady_clock::duration::count()`.
- Keep operation-specific keys explicit enough for formatters to avoid ambiguous interpretation, for example
  `CLIENT_SET_START`, `CLIENT_CREATE_RPC_START`, `WORKER_CREATE_META_RPC_START`, and `META_CREATE_META_START`.
- `CLIENT_CREATE_RPC_START` / `CLIENT_CREATE_RPC_END` are the only client-to-worker Create RPC tick boundaries; standalone
  Create and Set/Put buffer allocation both reuse this key pair. Do not add `CLIENT_SET_CREATE_RPC_*`.
- Worker-to-master metadata RPC phases use `WORKER_CREATE_META_RPC_*` and `WORKER_UPDATE_META_RPC_*`; master local
  execution phases use `META_CREATE_META_*` and `META_UPDATE_META_*`. Do not add `WORKER_PUBLISH_CREATE_META_*`,
  `WORKER_PUBLISH_UPDATE_META_*`, `MASTER_CREATE_META_*`, or `MASTER_UPDATE_META_*`.
- If Set with Create + memory copy + Publish + meta phases exceeds `LATENCY_TICK_MAX_NUM`, raise the capacity
  deliberately instead of dropping required phases.
- `UNKNOWN` is a sentinel and should not be stored as a real tick.
- Keep RPC framework latency metrics separate: existing ZMQ `MetaPb.latency_ticks` remains the RPC framework timeline;
  Get/Set/Create/Exist business-stage ticks live in `Trace`.

## Summary Ownership

- `Trace` stores and exposes raw ticks.
- Operation-specific code summarizes ticks for Get/Set/Create/Exist.
- `AccessRecorder` should not need business-specific knowledge of every latency key.
- A small helper can format a summary string from the current `Trace`, but the operation-specific caller chooses when to
  add it to `RequestParam`.
- Formatting must compute phase durations from tick pairs and omit phases with missing or invalid endpoints.
- Formatting must not emit raw tick timestamps.
- Defer summary construction until the client or primary-worker access-log record point, and only after the cached
  local/rpc slow-request gate passes.
- Operation code must check cached `LatencyTraceEnabled()` before calling `AddLatencyTick()`; default-disabled requests
  keep the original access-log behavior and do not merge response phases.
- Once any phase passes the local/rpc gate, print every valid observed phase in that side's summary, not only the
  phases above threshold.
- Master/data worker paths return compact numeric phase data only; they do not format local segmented summary strings.
- Cross-RPC phase propagation uses packed `uint32` phase/duration pairs, never raw ticks or formatted strings.

## Cross-Process Enablement Rules

- Full-chain summaries require the participating processes to explicitly enable segmented tracing with positive local or
  rpc thresholds.
- If the client is enabled but the primary worker is disabled, the client can only print client-local and client-to-worker
  RPC phases; worker/master/data worker internals are absent.
- If the primary worker is enabled but master/data workers are disabled, the worker summary can still print primary
  worker local and outbound RPC/transport phases, but not master/data local execution phases.
- If master/data workers are enabled but the primary worker is disabled, their compact phases are not merged or forwarded
  upstream in this plan.
- A side that is disabled by `0/0` config should behave like the original request path except for reading cached scalar
  config; it should not call `AddLatencyTick()`.

## Source-Backed Plan

Client Get:

- Public access logs:
  - `src/datasystem/client/object_cache/object_client.cpp`
  - `src/datasystem/client/kv_cache/kv_client.cpp`
- Shared backend:
  - `src/datasystem/client/object_cache/object_client_impl.cpp`
- Planned fields:
  - `client.rpc.get = CLIENT_GET_RPC_END - CLIENT_GET_RPC_START`
  - `client.local.get = (CLIENT_GET_END - CLIENT_GET_START) - client.rpc.get`

Primary Worker Get:

- Source-backed entrypoints:
  - `src/datasystem/worker/object_cache/service/worker_oc_service_get_impl.cpp`
  - `src/datasystem/worker/object_cache/service/worker_oc_service_batch_get_impl.cpp`
  - `src/datasystem/worker/object_cache/worker_request_manager.cpp`
- Worker access-log key:
  - `DS_POSIX_GET`
- Planned fields:
  - `worker.rpc.query_meta`
  - `worker.rpc.remote_get`
  - `worker.urma.urma_total`
  - `worker.local.get`

Client Set/Create/Exist:

- Public access logs:
  - `src/datasystem/client/object_cache/object_client.cpp`
  - `src/datasystem/client/kv_cache/kv_client.cpp`
- Shared backend:
  - `src/datasystem/client/object_cache/object_client_impl.cpp`
  - `ObjectClientImpl::Create`
  - `ObjectClientImpl::Put`
  - `ObjectClientImpl::Set`
  - `ObjectClientImpl::Publish`
  - `ObjectClientImpl::ProcessShmPut`
  - `ObjectClientImpl::Exist`
- Planned fields:
  - Create: `client.rpc.create`, `client.local.create`
  - Set/Put: `client.rpc.create`, `client.local.memory_copy`, `client.rpc.publish`, `client.local.set`
  - Exist: `client.rpc.exist`, `client.local.exist`

Primary Worker Set/Create/Exist:

- Create:
  - `src/datasystem/worker/object_cache/service/worker_oc_service_create_impl.cpp`
  - `WorkerOcServiceCreateImpl::Create`
  - `DS_POSIX_CREATE`
  - field: `worker.local.create`
- Publish/Set:
  - `src/datasystem/worker/object_cache/service/worker_oc_service_publish_impl.cpp`
  - `WorkerOcServicePublishImpl::Publish`
  - `WorkerOcServicePublishImpl::CreateMetadataToMaster`
  - `WorkerOcServicePublishImpl::UpdateMetadataToMaster`
  - `DS_POSIX_PUBLISH`
  - fields: `worker.local.publish`, `worker.rpc.create_meta`, `worker.rpc.update_meta`
- Exist:
  - `src/datasystem/worker/object_cache/service/worker_oc_service_get_impl.cpp`
  - `WorkerOcServiceGetImpl::Exist`
  - `WorkerOcServiceGetImpl::QueryMetadataFromMaster`
  - `DS_POSIX_EXIST`
  - fields: `worker.local.exist`, `worker.rpc.query_meta`

Meta Worker:

- Source-backed entrypoints:
  - `src/datasystem/master/object_cache/master_oc_service_impl.cpp`
  - `MasterOCServiceImpl::QueryMeta`
  - `MasterOCServiceImpl::CreateMeta`
  - `MasterOCServiceImpl::UpdateMeta`
- Planned fields for primary-worker/client summaries:
  - `master.local.query_meta`
  - `master.local.create_meta`
  - `master.local.update_meta`

Data Worker:

- Source-backed entrypoints:
  - `src/datasystem/worker/object_cache/worker_worker_oc_service_impl.cpp`
  - `WorkerWorkerOCServiceImpl::GetObjectRemote`
  - `WorkerWorkerOCServiceImpl::BatchGetObjectRemote`
- Planned field for primary-worker/client summaries:
  - `worker.local.remote_get`

## Finalized Implementation Decisions

- `RequestParam` member name:
  - use `latencySummary`.
- Cross-RPC propagation:
  - use packed `uint32` response fields named `latency_phase_us`, encoded as
    `phase_id, duration_us, phase_id, duration_us...`;
  - use helpers to append/decode pairs and reject odd-length data;
  - skip unknown `phase_id` values for rolling-upgrade compatibility.
- Error requests:
  - do not bypass local/rpc thresholds;
  - failed but fast requests do not print `latencySummary`;
  - failed but fast requests do not force request-stage `SLOW_LOG`;
  - this plan is segmented slow logging, not all-error diagnostic logging.
- `SLOW_LOG` naming:
  - rename the current `PLOG*` macro surface to `SLOW_LOG*`;
  - print `[SLOW LOG]` instead of `[PLOG]`;
  - keep the existing `forceLog` implementation behavior: bypass request sampling/rate limiting only when the slow
    condition passes, while preserving severity/min-log-level and async logging behavior.
- Dropped ticks:
  - include dropped tick count only when non-zero.
- Nested phases:
  - subtract explicitly tracked child phases from parent local phases where the summary would otherwise double-count.
- Field suffix:
  - do not add `_us` to access-log field names; values are documented as microseconds.

## Process Artifact Audit

- Access logs do not print raw ticks; they print derived phase durations only.
- Protobuf responses do not carry raw ticks or formatted summary strings.
- `MetaPb.latency_ticks` is not reused for business phases.
- The old fixed `2 ms` value is not a hard-coded final gate; local/rpc defaults are both `0`, meaning disabled.
- Master/data worker access logs do not print segmented summaries in this plan; their phase data is merged upstream.
- This file does not keep unresolved open questions as implementation guidance. If new information is missing, align the
  requirement first and then update this formal plan.

## AI Implementation Workflow

Use this order for the next coding pass:

1. Re-read `request-stage-latency-trace-plan-zh.md`, this file, and the relevant source before editing.
2. Check `git status` and avoid overwriting unrelated user changes.
3. Implement shared tick/phase/config/formatter helpers first; use `slow_log_local_slower_than` and
   `slow_log_rpc_slower_than` names for the effective threshold config.
4. Rename the current `PLOG` macro surface in `src/datasystem/common/log/log.h` to `SLOW_LOG`, change the printed marker
   to `[SLOW LOG]`, and migrate request-stage call sites from hard-coded `*_SLOW_US` constants to the cached local/rpc
   threshold config.
5. Add packed `latency_phase_us` helpers and then the response proto fields with unused field numbers selected from
   the current proto source.
6. Instrument master/meta worker and remote data worker local phases so they return compact phase data only.
7. Instrument primary worker Get/Set/Create/Exist, merge child compact phases, and add worker access-log summary.
8. Instrument client Get/Set/Create/Exist, merge worker compact phases, and add client access-log summary.
9. Add unit tests for Trace, formatter, thresholds, `SLOW_LOG` marker/macro behavior, and proto helpers.
10. Run representative Get/Set/Create/Exist validation and a fast-path performance comparison for the default disabled
   configuration; also sample-check an explicitly enabled configuration.

Implementation-readiness notes:

- The Chinese design now includes runtime, development, and cross-process data-flow diagrams.
- The document is suitable as the next AI coding entrypoint after source verification.
- Error requests are settled: failed but fast requests do not print segmented summaries.

## Validation Plan

- Unit tests:
  - add or extend Trace tests for tick add, bound/drop, invalidation, and `TraceContext` propagation;
  - add access-recorder formatting tests for `latencySummary` coexisting with `logSampled:true`;
  - add formatter tests proving raw tick timestamps and total fields are not emitted;
  - add threshold tests for `0` disabled and positive local/rpc threshold semantics;
  - add error threshold tests proving failed but fast requests do not print segmented summaries;
  - add logging tests proving `SLOW_LOG` prints `[SLOW LOG]`, bypasses request sampling only when the local/rpc threshold
    condition passes, and the old `[PLOG]` marker is not emitted;
  - add proto helper tests for packed pair append/decode, odd-length rejection, unknown phase skipping, and `uint32`
    saturation.
- Build-focused checks:
  - build affected logging, client, worker, and master targets.
- Runtime/manual validation:
  - default-disabled configuration: local/rpc are both `0`; confirm client and worker access logs contain no
    `latencySummary`, responses contain no `latency_phase_us`, and representative Get/Set fast paths show no measurable
    throughput, p99, CPU, lock-contention, or response-size regression.
  - explicitly enabled configuration: set positive microsecond local/rpc thresholds; confirm slow client/worker access
    logs include summaries, fast access logs omit summaries, meta/data-worker access logs do not print segmented
    summaries, trace IDs can connect logs across processes, slow responses carry only necessary phase pairs, and fast
    responses do not carry `latency_phase_us`.

## Change Boundaries

- Do not widen scope beyond Get/Set/Create/Exist until this group is implemented and validated.
- Do not alter access-log pipe field order.
- Do not introduce dynamic allocation on the hot tick-add path.
- Do not format summary strings before the client or primary-worker access-log record point.
- Do not read environment variables or parse string flags on request paths.
- Do not serialize raw ticks or formatted summary strings across RPC.
- Do not print segmented summaries when no local/rpc phase passes the effective configured gate.
- Do not print client or worker total fields inside `latencySummary`.
- Do not emit raw tick timestamps.
- Do not print segmented latency summaries from master or data worker access logs in this plan.
- Missing stages should simply be omitted from the summary.
