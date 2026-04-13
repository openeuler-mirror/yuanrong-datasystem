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
  - `SetPrefix()` stores a trace prefix, currently set from `Context::SetTraceId`.
  - `SetTraceNewID()` is for propagating an existing trace ID across threads.
  - `SetSubTraceID()` appends sub-trace information inside the same thread-local buffer.
  - `TraceGuard` clears trace or sub-trace state on scope exit unless the guard was created with `keep=true`.

## Main Types And APIs

| Type or API | Verified role | Notes |
| --- | --- | --- |
| `Trace::Instance()` | access current thread trace state | singleton is per-thread, not process-global |
| `SetTraceUUID()` | create root trace ID | common at public entrypoints |
| `SetTraceNewID()` | import existing trace ID | used for cross-thread propagation |
| `SetSubTraceID()` | derive nested trace context | keeps same root context with appended suffix |
| `SetPrefix()` | store trace prefix string | currently used by `Context::SetTraceId` |
| `TraceGuard` | scoped cleanup helper | can preserve state when `keep=true` |

## Propagation Model

- Practical effect:
  - most public API entrypoints call `Trace::Instance().SetTraceUUID()`;
  - asynchronous or cross-thread flows often capture and reapply trace IDs explicitly;
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
