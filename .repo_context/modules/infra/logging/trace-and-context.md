# Trace And Context

## Scope

- Paths:
  - `src/datasystem/common/log/trace.h`
  - `src/datasystem/common/log/trace.cpp`
  - trace-related callsites in public API and async runtime code
- Why this document exists:
  - explain how trace IDs are created, propagated, and cleaned up across threads and scopes.

## Primary Source Files

- `src/datasystem/common/log/trace.h`
- `src/datasystem/common/log/trace.cpp`
- `include/datasystem/context/context.h`
- `src/datasystem/client/context/context.cpp`
- `src/datasystem/common/util/uuid_generator.cpp`

## Responsibilities

- Verified:
  - `Trace::Instance()` uses the active BRPC `RequestContext` trace when one is bound to the current bthread and otherwise falls back to `thread_local` state.
  - `SetTraceUUID()` generates a new UUID-based trace ID unless the current thread already has one.
  - `SetRequestTraceUUID()` creates a root trace, marks it as a request-log-sampling trace for public SDK data-plane
    request APIs, and creates a local sampling decision immediately when LogSampler is enabled. Lifecycle and
    control-plane APIs (Init/ShutDown/Connect/UpdateToken/UpdateAkSk/Close/DeleteStream/PreRegisterDeviceMemory
    and PerfClient diagnostics) use `SetTraceUUID()` instead so
    their logs and downstream handler logs are never request-sampled (issue #1174).
  - `GenerateComponentTraceId()` constructs an owned component name plus a 12-character UUID suffix without changing current trace state.
  - `SetPrefix()` stores a trace prefix, currently set from `Context::SetTraceId`.
  - `SetTraceNewID()` imports a supplied ID; callers use it both for propagation and for manually constructed background IDs.
  - `GetContext()` / `SetTraceContext()` capture and restore trace ID, request marker, and request sampling decision together.
  - `SetSubTraceID()` appends sub-trace information inside the same thread-local buffer.
  - `SetRequestLogTrace()` / `IsRequestLogTrace()` explicitly mark whether current trace participates in request-log sampling.
  - `SetRequestSampleDecision()` and `GetRequestSampleDecision()` store and read request-log sampling decision in the same thread-local trace context.
  - `TraceGuard` clears trace or sub-trace state on scope exit unless the guard was created with `keep=true`.
  - `Trace` remains trivially destructible and stores latency summaries in a bounded inline buffer. This preserves safe
    teardown when a process-static SDK client is destroyed after the main thread's thread-local trace state.

## Main Types And APIs

| Type or API | Verified role | Notes |
| --- | --- | --- |
| `Trace::Instance()` | access current thread trace state | singleton is per-thread, not process-global |
| `SetTraceUUID()` | create root trace ID | use for non-request/internal scopes |
| `SetRequestTraceUUID()` | create request root trace ID | use at public SDK data-plane request entrypoints (Set/Get/Del/Exist/Create/Put/...) that should participate in request-log sampling; lifecycle, credential, and session-teardown entrypoints use `SetTraceUUID()` so they are never request-sampled; stores the first local sampling decision in `Trace` when local sampling is enabled |
| `GenerateComponentTraceId()` | construct a bounded component ID | accepts fixed character-array names of 1–36 bytes; returns name, semicolon, and the last 12 UUID characters without modifying trace/prefix/sampling state |
| `SetTraceNewID()` | import supplied trace ID | used for propagation and manually constructed IDs; truncates to 49 bytes |
| `GetContext()` / `SetTraceContext()` | capture and restore full trace context | use when request-log marker and sampling decision must follow async work; `SetTraceContext()` creates a local decision for undecided request contexts when local sampling is enabled |
| `SetSubTraceID()` | derive nested trace context | keeps same root context with appended suffix |
| `SetPrefix()` | store trace prefix string | currently used by `Context::SetTraceId` |
| `SetRequestLogTrace()` / `IsRequestLogTrace()` | explicit request-log-sampling marker | avoids treating every trace-bearing background thread log as a sampled request log |
| `SetRequestSampleDecision()` | bind request sampling admit/reject decision to current trace context | used together with RPC metadata propagation for consistent request-log sampling across hops |
| `GetRequestSampleDecision()` | read existing request sampling admit/reject decision | returns false when decision is not carried in current trace context |
| `TraceGuard` | scoped cleanup helper | can preserve state when `keep=true` |

## Propagation Model

- Practical effect:
  - public SDK data-plane request API entrypoints call `Trace::Instance().SetRequestTraceUUID()`;
  - lifecycle/control-plane SDK entrypoints (Init/ShutDown/Connect/UpdateToken/UpdateAkSk/Close/DeleteStream/
    PreRegisterDeviceMemory) call
    `Trace::Instance().SetTraceUUID()` — traceID still propagates, but the RPC carries `LOG_SAMPLE_NONE` so
    worker-side handler logs are background-classified and never request-sampled;
  - non-request/background work uses `Trace::Instance().SetTraceUUID()` or imported trace IDs without request markers;
  - asynchronous or cross-thread request flows capture and reapply full `TraceContext` explicitly;
  - BRPC request attachments carry the same request-log sampling state as a 1-byte `LogSampleState` appended after the `TRCID:V1` traceID frame; `AttachTraceIDToAttachment()` encodes traceID + state from the caller's `Trace`, and the generated `CallMethod` prologue (`ExtractTraceIDAndSampleState()` + `ScopedRequestContext` + `ApplyLogSampleState()`) restores both on the worker so the handler participates in `LogSampler` instead of being bypassed. Wire format and the transport-neutral helpers live in `src/datasystem/common/rpc/trace_attachment.h` and `src/datasystem/common/log/log_sample_state.h`;
  - coordinator startup establishes a `CoordMain` lifecycle trace before logging initialization, coordinator TTL/watch threads establish bounded component-scoped traces at thread entry, and topology recovery tasks capture and restore the submitting `TraceContext`;
  - the worker `RebalanceExecutor` single-task pool (`executorPool_` in `src/datasystem/worker/rebalance_executor.cpp::Submit`) propagates the caller's traceID via `GetTraceID()` + `SetTraceNewID` TraceGuard at task submit, so the executor/migrator logs and the downstream `ReportRebalanceResult`/`MigrateData` RPCs carry the same trace as the master scheduler logs; without it the executor logs had an empty traceID column and the target/master finish logs carried freshly-minted bare UUIDs;
  - request sampling decisions live in `Trace` rather than a process-wide trace-decision table; `LogSampler`
    owns the sampling decision and precomputed threshold; no per-second counter is used;
  - sub-operations can append sub-trace state without replacing the root trace.
- Review implication:
  - any new async boundary that forgets to capture and restore trace state can make observability look randomly broken even when business logic still works.

## Context Integration

- Verified:
  - `Context::SetTraceId` feeds trace prefix information into `Trace`.
  - logging macros and downstream log formatting rely on trace state already being present in the thread-local context.
- Pending verification:
  - whether every language binding and worker-internal async helper applies a consistent trace propagation helper.

## Length Boundaries

- `TRACEID_MAX_SIZE` is 49 bytes; the inline buffer includes one additional byte for the terminator.
- With no prefix, `SetTraceUUID()` uses the complete 36-character UUID. With a prefix, it uses at most 36 prefix
  bytes, a semicolon, and only the last 12 UUID characters.
- `Context::SetTraceId()` validates the prefix and rejects inputs longer than 36 bytes. It sets the prefix rather than
  the complete request ID; language bindings delegate to this API.
- `GenerateComponentTraceId()` enforces the component name length at compile time and follows the same 12-character
  UUID suffix rule. Background routing, topology, probe, eviction, stream-close, Coordinator and RocksDB async
  generation sites use this helper. For example, `TopologySnapshotWarmup;<12 hex characters>` is 35 bytes.
  RocksDB and Coordinator fallback helpers still inherit a nonempty caller trace without regeneration.
- `SetTraceNewID()` imports the supplied ID without reinterpreting semicolons or component names. Oversized input is
  truncated from the right, with a warning limited by a 60-second interval. The RPC attachment limit remains 49 bytes.
- `SetSubTraceID()` appends into the remaining buffer and truncates the suffix when necessary; its warning is not
  time-limited. Communicator creation preserves the parent trace instead of appending a communicator prefix.
  Its existing cache-miss log carries the full `commId`; existing send/receive RootInfo logs identify the direction
  and peer. Correlate these events through the parent trace without adding per-attempt or completion logs.

## Bugfix And Review Notes

- Coordinator identities are binary UUIDs. Use `CoordinatorIdLogPrefix()` from
  `src/datasystem/common/coordinator/coordinator_log.h` at Coordinator identity log boundaries, including
  watch diagnostics: valid IDs print eight hex characters, empty IDs stay empty,
  and malformed lengths print `invalid`. Never use the log prefix for identity comparison or fencing.
- Good first files when trace continuity looks wrong:
  - `src/datasystem/common/log/trace.cpp`
  - `src/datasystem/client/context/context.cpp`
- Common risks:
  - replacing `SetTraceUUID()` with unconditional regeneration can break correlation across a request chain;
  - forgetting `TraceGuard` or equivalent cleanup can leak trace/sub-trace state into unrelated work on reused threads;
  - assigning one permanent background trace guarantees component logs are non-empty but does not make batched work causally equivalent to the original request;
  - adding heap-owning members to `Trace` can reintroduce process-teardown use-after-free for process-static SDK clients.
