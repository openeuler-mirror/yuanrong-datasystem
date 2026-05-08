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
- `src/datasystem/context/context.h`
- `src/datasystem/context/context.cpp`

## Responsibilities

- Verified:
  - `Trace::Instance()` is `thread_local`.
  - `SetTraceUUID()` generates a new UUID-based trace ID unless the current thread already has one.
  - `SetRequestTraceUUID()` creates a root trace, marks it as a request-log-sampling trace for public SDK request APIs,
    and creates a local sampling decision immediately when local `log_rate_limit` is enabled.
  - `SetPrefix()` stores a trace prefix, currently set from `Context::SetTraceId`.
  - `SetTraceNewID()` is for propagating an existing trace ID across threads.
  - `GetContext()` / `SetTraceContext()` capture and restore trace ID, request marker, and request sampling decision together.
  - `SetSubTraceID()` appends sub-trace information inside the same thread-local buffer.
  - `SetRequestLogTrace()` / `IsRequestLogTrace()` explicitly mark whether current trace participates in request-log sampling.
  - `SetRequestSampleDecision()` and `GetRequestSampleDecision()` store and read request-log sampling decision in the same thread-local trace context.
  - `TraceGuard` clears trace or sub-trace state on scope exit unless the guard was created with `keep=true`.

## Main Types And APIs

| Type or API | Verified role | Notes |
| --- | --- | --- |
| `Trace::Instance()` | access current thread trace state | singleton is per-thread, not process-global |
| `SetTraceUUID()` | create root trace ID | use for non-request/internal scopes |
| `SetRequestTraceUUID()` | create request root trace ID | use at public SDK request entrypoints that should participate in request-log sampling; stores the first local sampling decision in `Trace` when local sampling is enabled |
| `SetTraceNewID()` | import existing trace ID | used for trace-only cross-thread propagation |
| `GetContext()` / `SetTraceContext()` | capture and restore full trace context | use when request-log marker and sampling decision must follow async work; `SetTraceContext()` creates a local decision for undecided request contexts when local sampling is enabled |
| `SetSubTraceID()` | derive nested trace context | keeps same root context with appended suffix |
| `SetPrefix()` | store trace prefix string | currently used by `Context::SetTraceId` |
| `SetRequestLogTrace()` / `IsRequestLogTrace()` | explicit request-log-sampling marker | avoids treating every trace-bearing background thread log as a sampled request log |
| `SetRequestSampleDecision()` | bind request sampling admit/reject decision to current trace context | used together with RPC metadata propagation for consistent request-log sampling across hops |
| `GetRequestSampleDecision()` | read existing request sampling admit/reject decision | returns false when decision is not carried in current trace context |
| `TraceGuard` | scoped cleanup helper | can preserve state when `keep=true` |

## Propagation Model

- Practical effect:
  - public SDK request API entrypoints call `Trace::Instance().SetRequestTraceUUID()`;
  - non-request/background work uses `Trace::Instance().SetTraceUUID()` or imported trace IDs without request markers;
  - asynchronous or cross-thread request flows capture and reapply full `TraceContext` explicitly;
  - ZMQ `MetaPb` carries one request-log sampling state (`NONE`, `UNDECIDED`, `ADMIT`, `REJECT`) and callsites restore both `trace_id` and request-sampling context when importing request context;
  - request sampling decisions live in `Trace` rather than a process-wide trace-decision table; `LogRateLimiter`
    only owns the per-second atomic admission counter;
  - sub-operations can append sub-trace state without replacing the root trace.
- Review implication:
  - any new async boundary that forgets to capture and restore trace state can make observability look randomly broken even when business logic still works.

## Context Integration

- Verified:
  - `Context::SetTraceId` feeds trace prefix information into `Trace`.
  - logging macros and downstream log formatting rely on trace state already being present in the thread-local context.
- Pending verification:
  - whether every language binding and worker-internal async helper applies a consistent trace propagation helper.

## Bugfix And Review Notes

- Good first files when trace continuity looks wrong:
  - `src/datasystem/common/log/trace.cpp`
  - `src/datasystem/context/context.cpp`
- Common risks:
  - replacing `SetTraceUUID()` with unconditional regeneration can break correlation across a request chain;
  - forgetting `TraceGuard` or equivalent cleanup can leak trace/sub-trace state into unrelated work on reused threads.
