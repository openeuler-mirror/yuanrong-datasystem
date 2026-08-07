# Get/Set/Create/Exist Latency Trace Plan

## Metadata

- Status:
  - `in-progress` (issue #862 comm-gate, B-complete + MAX redesign; A-version was implemented+validated, now being reworked to B-complete+MAX)
- Last updated:
  - `2026-08-06`
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
  - Worker-side gates use `--slow_log_process_slower_than` and `--slow_log_rpc_slower_than` (dynamic gflags,
    hot-reloadable via config file or UpdateConfig API).
  - Client-side gates use `--client_slow_log_process_slower_than` and `--client_slow_log_rpc_slower_than`
    (dynamic gflags, hot-reloadable via `DATASYSTEM_CLIENT_CONFIG_PATH` config file or UpdateConfig API).
  - Config values are in microseconds. Worker defaults are `2000`/`5000` (process/rpc); client defaults are
    `2000`/`5000`. `0` disables the corresponding gate.
  - Threshold semantics: `0` disables the gate; positive values log phases above that microsecond threshold.
  - `LatencyTraceEnabled()` is true only when at least one local/rpc threshold is positive.
  - The same local/rpc thresholds also drive the force condition for request-stage `SLOW_LOG` output; hard-coded
    `*_SLOW_US` constants must not remain the final control surface.
  - RPC threshold semantics (issue #862 correction, **B-complete + MAX**): `slow_log_rpc_slower_than` gates on
    **communication time** (network + RPC framework) and the displayed `*_RPC_*` phase value IS comm time (not the
    full caller-side wall-clock). The full-duration tick pairs for `*_RPC_*` are removed from the tick-pair table, so
    `worker.rpc.query_meta` / `client.rpc.get` etc. are populated solely from comm computed by the transport layer as
    `comm = e2e - server_processing` (brpc `BrpcPerfTrace` 8-point trace / ZMQ `MetaPb.latency_ticks`), stashed on
    `Trace::SetLastRpcCommUs`. **Aggregation = MAX** (not sum): for the concurrent QueryMeta fan-out to N masters,
    the parent takes `max` of per-leg comm — sum would re-introduce the false-positive the issue fixes (N fast legs
    summing past the threshold). Single-RPC paths store the single value. comm is NOT carried via the server response
    `latency_phase_us`; single-RPC paths `Trace::AddDownstreamPhase(*_RPC_*, comm)` directly on the caller thread, and
    QueryMeta carries per-leg comm in `BatchQueryMetaResult.commUs` with the parent setting the phase once (max).
    Process thresholds still gate on `*_PROCESS_*`; business slowness is caught there. URMA transfer and the legacy
    client direct-route RPC phases remain in the rpc gate.
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
| `client.process.get` | Client-side Get local processing outside the client-to-worker Get RPC. |
| `client.rpc.get` | Client-to-worker Get RPC duration. |
| `worker.process.get` | Primary worker Get local processing outside QueryMeta, RemoteGet, and explicitly tracked transport phases. |
| `worker.rpc.query_meta` | Primary worker to master QueryMeta RPC phase. |
| `master.process.query_meta` | Master-side local QueryMeta execution. |
| `worker.rpc.remote_get` | Primary worker to remote-worker RemoteGet phase. |
| `worker.urma.urma_total` | URMA write plus URMA wait operation time. |
| `worker.process.remote_get` | Remote data worker local RemoteGet execution. |
| `client.process.set` | Client-side Set/Put local processing outside tracked Create/Publish RPC and memory copy phases. |
| `client.rpc.create` | Client-to-worker Create RPC duration, used by Create and by Set paths that allocate a buffer first. |
| `client.process.memory_copy` | Client local copy into SHM/UB buffer during Set/Put paths when present. |
| `client.rpc.publish` | Client-to-worker Publish RPC duration during Set/Put or buffer publish. |
| `client.process.create` | Client-side Create local processing outside the Create RPC. |
| `client.process.exist` | Client-side Exist local processing outside the Exist RPC. |
| `client.rpc.exist` | Client-to-worker Exist RPC duration. |
| `worker.process.create` | Primary worker local Create execution. |
| `worker.process.publish` | Primary worker local Publish execution outside tracked CreateMeta/UpdateMeta RPC phases. |
| `worker.rpc.create_meta` | Primary worker to master CreateMeta RPC phase during Publish/Set. |
| `worker.rpc.update_meta` | Primary worker to master UpdateMeta RPC phase during Publish/Set. |
| `master.process.create_meta` | Master-side local CreateMeta execution. |
| `master.process.update_meta` | Master-side local UpdateMeta execution. |
| `worker.process.exist` | Primary worker local Exist execution outside QueryMeta. |

Example client Get request field:

```text
{Object_key:[key1,key2],timeout:1000,transportType:TCP,latencySummary:{client.process.get:300,client.rpc.get:900,worker.process.get:260,worker.rpc.query_meta:230,master.process.query_meta:180,worker.rpc.remote_get:610,worker.process.remote_get:420}}
```

Example client Set request field:

```text
{Object_key:key1,transportType:TCP,latencySummary:{client.process.set:320,client.rpc.create:430,client.process.memory_copy:210,client.rpc.publish:850,worker.process.publish:260,worker.rpc.create_meta:410,master.process.create_meta:180}}
```

Example primary worker request field:

```text
{Object_key:key1,latencySummary:{worker.process.publish:260,worker.rpc.create_meta:410,master.process.create_meta:180}}
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
- Bridge RPC framework latency into the business trace: brpc `BrpcPerfTrace` (8-point trace) and ZMQ
  `MetaPb.latency_ticks` both compute `comm = e2e - server_processing` and stash it on `Trace::SetLastRpcCommUs`.
  The `*_RPC_*` phases are **redefined to comm** (their full-duration tick pairs removed from the tick-pair table);
  they are NOT carried in the server response `latency_phase_us` (only server-side `master.process.*` /
  `worker.process.remote_get` downstream phases ride that field). Single-RPC paths
  `Trace::AddDownstreamPhase(*_RPC_*, comm)` directly on the caller thread; the QueryMeta concurrent fan-out stores
  per-leg comm in `BatchQueryMetaResult.commUs` and the parent `MergeQueryMetadataResults` takes MAX and sets the
  phase once. **Derived process-math fix**: split `ComputePhaseDurations` into `ComputeTickPhases` +
  `ComputeDerivedPhases`; `Finalize*/Emit*` reorder to tick → `MergeDownstreamPhases` → derived, so the derived
  `*_PROCESS_*` phases subtract the merged downstream server-exec phases (`master.process.*`,
  `worker.process.remote_get`) AND the comm `*_RPC_*` phases — giving an additive breakdown
  (process + comm + server-exec = total) with no double-count. `DerivedPhaseMapping.subPhases` expanded (`[5]→[12]`)
  to hold the added downstream sub-phases. (issue #862)

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
  - `client.process.get = (CLIENT_GET_END - CLIENT_GET_START) - client.rpc.get`

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
  - `worker.process.get`

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
  - Create: `client.rpc.create`, `client.process.create`
  - Set/Put: `client.rpc.create`, `client.process.memory_copy`, `client.rpc.publish`, `client.process.set`
  - Exist: `client.rpc.exist`, `client.process.exist`

Primary Worker Set/Create/Exist:

- Create:
  - `src/datasystem/worker/object_cache/service/worker_oc_service_create_impl.cpp`
  - `WorkerOcServiceCreateImpl::Create`
  - `DS_POSIX_CREATE`
  - field: `worker.process.create`
- Publish/Set:
  - `src/datasystem/worker/object_cache/service/worker_oc_service_publish_impl.cpp`
  - `WorkerOcServicePublishImpl::Publish`
  - `WorkerOcServicePublishImpl::CreateMetadataToMaster`
  - `WorkerOcServicePublishImpl::UpdateMetadataToMaster`
  - `DS_POSIX_PUBLISH`
  - fields: `worker.process.publish`, `worker.rpc.create_meta`, `worker.rpc.update_meta`
- Exist:
  - `src/datasystem/worker/object_cache/service/worker_oc_service_get_impl.cpp`
  - `WorkerOcServiceGetImpl::Exist`
  - `WorkerOcServiceGetImpl::QueryMetadataFromMaster`
  - `DS_POSIX_EXIST`
  - fields: `worker.process.exist`, `worker.rpc.query_meta`

Meta Worker:

- Source-backed entrypoints:
  - `src/datasystem/master/object_cache/master_oc_service_impl.cpp`
  - `MasterOCServiceImpl::QueryMeta`
  - `MasterOCServiceImpl::CreateMeta`
  - `MasterOCServiceImpl::UpdateMeta`
- Planned fields for primary-worker/client summaries:
  - `master.process.query_meta`
  - `master.process.create_meta`
  - `master.process.update_meta`

Data Worker:

- Source-backed entrypoints:
  - `src/datasystem/worker/object_cache/worker_worker_oc_service_impl.cpp`
  - `WorkerWorkerOCServiceImpl::GetObjectRemote`
  - `WorkerWorkerOCServiceImpl::BatchGetObjectRemote`
- Planned field for primary-worker/client summaries:
  - `worker.process.remote_get`

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
- The old fixed `2 ms` value is not a hard-coded final gate; worker defaults are `2000`/`5000` (process/rpc) and
  client defaults are `2000`/`5000`; `0` disables the corresponding gate.
- Master/data worker access logs do not print segmented summaries in this plan; their phase data is merged upstream.
- This file does not keep unresolved open questions as implementation guidance. If new information is missing, align the
  requirement first and then update this formal plan.

## AI Implementation Workflow

Use this order for the next coding pass:

1. Re-read `request-stage-latency-trace-plan-zh.md`, this file, and the relevant source before editing.
2. Check `git status` and avoid overwriting unrelated user changes.
3. Implement shared tick/phase/config/formatter helpers first; use `slow_log_process_slower_than` and
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
  - default-enabled configuration: worker defaults are `2000`/`5000` (process/rpc) and client defaults are
    `2000`/`5000`; confirm that fast requests below threshold do not produce `latencySummary`, responses contain no
    `latency_phase_us`, and representative Get/Set fast paths show no measurable throughput, p99, CPU,
    lock-contention, or response-size regression.
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

## Implementation Plan — issue #862, B-complete + MAX

> Working memory for the in-progress rework. The A-version (separate `*_RPC_COMM_*` field, comm carried via response
> `latency_phase_us` and summed) was implemented and validated (compiles, links, UTs pass) but is being replaced by
> B-complete+MAX per the agreed decision: redefining `*_RPC_*` to comm + fixing the derived process math + MAX
> aggregation for the concurrent QueryMeta fan-out.

### Decision recap
- Display: `worker.rpc.*` / `client.rpc.*` themselves hold comm (network + RPC framework). No new `*_RPC_COMM_*`
  field. Gate `slow_log_rpc_slower_than` checks `*_RPC_*` (= comm) directly.
- comm formula: `comm = e2e - server_processing` where `server_processing = server_send - server_recv`
  (= server req-queue + server-exec + server rsp-queue). Excludes remote business execution AND remote queue wait.
- Aggregation: **MAX** of per-leg comm (correct for slow-log gate; sum on concurrent fan-out re-introduces the
  issue's false-positive). Single-RPC paths = the single value.

### Files & changes (B-complete + MAX)

Source of truth checkout: `/home/liudongliang/Desktop/workspace/yhl_datasystem/fix-rpc-latency-trace` (branch
`fix-rpc-latency-trace`). Build/validate worktree: `.../build_cmake_normal` (branch `build_cmake_normal`, Release,
Unix Makefiles; thirdparty cached; build cmd `bash build.sh -t build -X off -b cmake -j2`; run UTs with
`LD_LIBRARY_PATH` set to the `/tmp/2d13d9...` thirdparty lib dirs).

1. `src/datasystem/common/log/latency_phase_types.h`
   - **Revert** the `WORKER_RPC_COMM_*`/`CLIENT_RPC_COMM_*` enum additions (29-36) and `LATENCY_SUMMARY_PHASE_MAX`
     back to 28 — B reuses the existing `*_RPC_*` ids; no new comm phases.
   - `DerivedPhaseMapping.subPhases[5]` → `[12]` to hold added downstream sub-phases.
2. `src/datasystem/common/log/trace.h` — keep `SetLastRpcCommUs`/`ConsumeLastRpcCommUs` + `lastRpcCommUs_` (unchanged
   from A; reused by B).
3. `src/datasystem/common/rpc/brpc_perf_trace.h` + `zmq_constants.h` — keep the comm stash in `RecordBrpcRpcTrace` /
   `RecordRpcLatencyMetrics` (unchanged from A).
4. `src/datasystem/common/log/latency_phase.h`
   - **Remove** the `AppendRpcCommPhase` template (B does not carry comm via response proto).
   - `FinalizeWorkerLatencyTrace` / `TryEncodeRemoteGetLatencySummary` / `EmitClientLatencySummary`: reorder to
     `ComputeTickPhases` → `MergeDownstreamPhases` → `ComputeDerivedPhases` (so derived can subtract merged
     downstream server-exec + comm).
5. `src/datasystem/common/log/latency_phase.cpp`
   - Split `ComputePhaseDurations` into `ComputeTickPhases` (TICK_PAIR_TABLE loop, **minus** the `*_RPC_*` full-dur
     pairs which are removed) + `ComputeDerivedPhases` (DERIVED_PHASE_TABLE loop). Keep `ComputePhaseDurations` as a
     thin wrapper (tick+derived, no downstream) for UTs that don't merge downstream.
   - `TICK_PAIR_TABLE`: remove the 8 `*_RPC_*` full-duration pairs (CLIENT_GET_RPC, CLIENT_CREATE_RPC,
     CLIENT_PUBLISH_RPC, CLIENT_EXIST_RPC, WORKER_QUERYMETA, WORKER_EXIST_QUERYMETA, WORKER_REMOTEGET,
     WORKER_CREATE_META_RPC, WORKER_UPDATE_META_RPC). Keep URMA, l2cache, direct-route, memory-copy, etc.
   - `DERIVED_PHASE_TABLE`: add downstream server-exec phases to the sub-lists:
     `worker.process.get` += `MASTER_PROCESS_QUERY_META`, `WORKER_PROCESS_REMOTE_GET`;
     `client.process.get` += the worker-side phases returned downstream (`WORKER_PROCESS_GET`, `WORKER_RPC_QUERY_META`,
     `WORKER_RPC_REMOTE_GET`, `WORKER_URMA_URMA_TOTAL`, `MASTER_PROCESS_QUERY_META`, `WORKER_PROCESS_REMOTE_GET`); and
     analogously for `client.process.set/create/exist`.
   - `RPC_PHASES`: put back the original `*_RPC_*` ids (now = comm); **drop** the `*_RPC_COMM_*` ids. Keep URMA +
     direct-route.
   - `PHASE_NAME_TABLE`: drop the `*_RPC_COMM_*` names (names `worker.rpc.*` stay, now meaning comm).
6. Business call sites — switch from A's `AppendRpcCommPhase(rsp, ...)` to direct `Trace::AddDownstreamPhase`:
   - `worker_oc_service_publish_impl.cpp` CreateMetadataToMaster/UpdateMetadataToMaster: after the RPC,
     `Trace::AddDownstreamPhase(WORKER_RPC_CREATE_META/UPDATE_META, Trace::Instance().ConsumeLastRpcCommUs())`.
   - `worker_oc_service_get_impl.cpp` PullObjectDataFromRemoteWorker: after `clientApi->Read(rspPb)`,
     `Trace::AddDownstreamPhase(WORKER_RPC_REMOTE_GET, ...)`.
   - `client_worker_remote_api.cpp` Get/Create/Publish/Exist: after the retry,
     `Trace::AddDownstreamPhase(CLIENT_RPC_GET/CREATE/PUBLISH/EXIST, ...)`.
   - `worker_master_oc_api.cpp` QueryMeta: **remove** the `AppendRpcCommPhase(response, ...)` (comm now via struct);
     the child `QueryMetaDataFromMasterImpl` instead does `result.commUs = Trace::Instance().ConsumeLastRpcCommUs()`.
7. QueryMeta MAX fan-out:
   - `worker_oc_service_get_impl.cpp`: add `uint64_t commUs = 0;` to `BatchQueryMetaResult`; in
     `QueryMetaDataFromMasterImpl` (or the dispatch lambda) set `result.commUs = Trace::Instance().ConsumeLastRpcCommUs()`
     after `WorkerRemoteMasterOCApi::QueryMeta`.
   - `MergeQueryMetadataResults`: `uint64_t maxComm = 0; for (r : results) maxComm = std::max(maxComm, r.commUs);`
     then `if (maxComm > 0) Trace::Instance().AddDownstreamPhase(WORKER_RPC_QUERY_META, maxComm);` (once).
8. Tests:
   - `latency_phase_test.cpp`: revert A's comm-field tests; add B tests — `worker.rpc.query_meta` populated from comm
     (AddDownstreamPhase), not full-duration tick; derived `worker.process.get` subtracts downstream server-exec
     (after merge); MAX aggregation test (two legs → max).
   - `zmq_metrics_test.cpp`: keep `ConsumeLastRpcCommUs()==6500` assertion (transport formula unchanged).
   - `latency_summary_st_test.cpp`: revert `*_rpc_comm.*` assertions; keep `worker.rpc.*`/`client.rpc.*` (now comm).

### Commit hygiene
- PR commit in `fix-rpc-latency-trace` includes ONLY the 13 src/test files + 2 `.repo_context` docs.
- Do NOT commit the 4 local-build cmake files (CMakeLists.txt / brpc.cmake / gflags.cmake / leveldb.cmake — keep
  modified-uncommitted) nor the migrate files (only in `build_cmake_normal` worktree).

### Validation status (B-complete + MAX)
- Full build in `build_cmake_normal`: EXIT=0, 100%, zero errors. All B source + UT + ST
  compile & link.
- UTs 34/34 PASSED (33 `LatencyPhaseTest` + 1 `ZmqMetricsTest.queue_flow_residual_network_matches_e2e_minus_framework`).
  New B tests: `RpcCommPhaseDrivesRpcThresholdGate`, `ConsumeCommAndAddDownstreamPhase`,
  `QueryMetaMaxAggregationConcurrentFanout`, `AdditiveDerivedMathWorkerGet`.
