# Logging Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - current implementation | mixed
- Primary code paths:
  - `src/datasystem/common/log/*`
  - `src/datasystem/common/metrics/hard_disk_exporter/*`
- Primary source-of-truth files:
  - `src/datasystem/common/log/CMakeLists.txt`
  - `src/datasystem/common/log/log.h`
  - `src/datasystem/common/log/logging.h`
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/log/log_manager.h`
  - `src/datasystem/common/log/log_manager.cpp`
  - `src/datasystem/common/log/access_recorder.h`
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/access_point.def`
  - `src/datasystem/common/log/trace.h`
  - `src/datasystem/common/log/trace.cpp`
  - `src/datasystem/common/log/failure_handler.h`
  - `src/datasystem/common/log/failure_handler.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
- Last verified against source:
  - `2026-04-13`
- Related context docs:
  - `README.md`
  - `trace-and-context.md`
  - `access-recorder.md`
  - `log-lifecycle-and-rotation.md`
  - `../metrics/README.md`
- Related user-facing or internal docs:
  - none linked yet

## Purpose

- Why this design document exists:
  - provide a stable architecture-level reference before adding or modifying logging-related features.
- What problem this module solves:
  - give the repository one shared logging substrate for ordinary logs, trace correlation, access/performance records, monitor flush, and crash-path output.
- Who or what depends on this module:
  - client-facing APIs, worker/master runtime code, background threads, and metrics-linked observability output.

## Goals

- provide a single macro and lifecycle surface that can be reused across the repository;
- keep request and background work observable through trace IDs;
- separate synchronous business logic from asynchronous file maintenance and monitor flushing;
- support operational retention concerns such as rotation, compression, and crash logging.

## Non-Goals

- authn/authz policy enforcement;
- business-level event schema ownership outside access/performance records;
- replacing application-specific metrics collection.

## Scope

- In scope:
  - normal log initialization and provider setup;
  - trace ID generation and propagation helpers;
  - access/performance recorder lifecycle;
  - monitor/access/resource log flush coordination;
  - crash-path failure output integration.
- Out of scope:
  - business feature semantics that only happen to emit logs;
  - metric-family definition and collector registration details;
  - transport-level propagation semantics outside trace handoff points.
- Important boundaries with neighboring modules:
  - `logging` owns log lifecycle and trace helpers;
  - `metrics/hard_disk_exporter` owns the concrete buffered file exporter used by access and monitor paths;
  - runtime modules decide where instrumentation is attached and which trace IDs are propagated.

## Terminology

| Term | Meaning in this repository | Source or note |
| --- | --- | --- |
| `Trace` | thread-local trace context holder | `trace.h/.cpp` |
| `TraceGuard` | scoped cleanup helper for trace state | `trace.h/.cpp` |
| `AccessRecorder` | per-operation performance/access recorder | `access_recorder.h/.cpp` |
| `AccessRecorderManager` | exporter owner for access and monitor output | `access_recorder.h/.cpp` |
| `LogManager` | background log maintenance runner | `log_manager.h/.cpp` |
| `FailureWriter` | crash-path file writer for failure signal handling | `failure_handler.cpp` |

## Architecture Overview

- High-level structure:
  - the module exposes repository-wide logging macros and a singleton lifecycle entry through `Logging`;
  - trace state is kept thread-local through `Trace`;
  - performance/access records are produced by `AccessRecorder` and handed to `AccessRecorderManager`;
  - background maintenance and flush work is centralized in `LogManager`;
  - crash-path output is handled by `FailureWriter` through absl failure-signal integration.
- Key runtime roles or processes:
  - normal request or background threads emit logs and manage trace state;
  - background maintenance thread performs rotation, compression, and monitor flush;
  - exporter flush threads persist access/resource data through the shared hard-disk exporter path.
- Key persistent state, if any:
  - ordinary log files;
  - access/client-access/resource monitor files;
  - crash output file `container.log`.
- Key control-flow or data-flow stages:
  - startup and config override;
  - ordinary log emission;
  - trace correlation and propagation;
  - access record buffering and async flush;
  - background maintenance and crash-path output.

## Entry Points And Integration Points

- Public APIs:
  - macros from `log.h` such as `LOG`, `VLOG`, `CHECK`, `LOG_EVERY_N`, and `LOG_FIRST_N`;
  - `Trace::SetTraceUUID`, `Trace::SetTraceNewID`, `Trace::SetSubTraceID`;
  - `AccessRecorder` construction plus `Record()`.
- Internal service entrypoints:
  - `Logging::Start()`
  - `Logging::AccessRecorderManagerInstance()`
  - `InstallFailureSignalHandler(const char *arg0)`
- Background jobs / threads / callbacks:
  - `LogManager` maintenance loop;
  - exporter flush threads used beneath `AccessRecorderManager`.
- Config flags or environment variables:
  - `log_monitor`
  - `max_log_file_num`
  - `log_compress`
  - `log_retention_day`
  - `max_log_size`
  - `logtostderr`
  - `alsologtostderr`
  - `stderrthreshold`
  - `minloglevel`
  - `log_async_queue_size`
  - `DATASYSTEM_CLIENT_LOG_NAME`
  - `DATASYSTEM_CLIENT_ACCESS_LOG_NAME`
  - `DATASYSTEM_CLIENT_LOG_DIR`
  - `DATASYSTEM_CLIENT_MAX_LOG_SIZE`
  - `DATASYSTEM_MAX_LOG_FILE_NUM`
  - `DATASYSTEM_LOG_COMPRESS`
  - `DATASYSTEM_LOG_RETENTION_DAY`
  - `DATASYSTEM_LOG_TO_STDERR`
  - `DATASYSTEM_ALSO_LOG_TO_STDERR`
  - `DATASYSTEM_STD_THRESHOLD`
  - `DATASYSTEM_LOG_ASYNC_ENABLE`
  - `DATASYSTEM_LOG_ASYNC_QUEUE_SIZE`
  - `DATASYSTEM_LOG_V`
  - `DATASYSTEM_MIN_LOG_LEVEL`
  - `DATASYSTEM_LOG_MONITOR_ENABLE`
- Cross-module integration points:
  - `HardDiskExporter` from `metrics` backs access/monitor output;
  - client and worker code call trace helpers at request boundaries;
  - `LogManager::DoLogMonitorWrite()` drives monitor flushing.

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| `log.h` | macro surface and severity/check helpers | `src/datasystem/common/log/log.h` | entrypoint most callers see |
| `Logging` | provider init, config override, singleton lifecycle | `logging.h`, `logging.cpp` | starts log manager and access manager |
| `LogManager` | rotation, compression, periodic monitor flush | `log_manager.h`, `log_manager.cpp` | background maintenance owner |
| `Trace` | thread-local correlation state | `trace.h`, `trace.cpp` | supports generated and inherited trace IDs |
| `AccessRecorder` | per-operation access/performance timing | `access_recorder.h`, `access_recorder.cpp`, `access_point.def` | records elapsed time and metadata |
| `AccessRecorderManager` | shared export path for access/monitor records | `access_recorder.h`, `access_recorder.cpp` | uses `HardDiskExporter` underneath |
| `FailureWriter` | crash-path persistence | `failure_handler.h`, `failure_handler.cpp` | writes failure text to `container.log` |

## Main Flows

### Startup And Ordinary Logging

1. runtime or client code calls `Logging::Start()`;
2. client mode may override config from environment before provider initialization;
3. provider starts, `LogManager` starts, and `AccessRecorderManager` is initialized;
4. normal log macros emit through the configured backend.

Key files:

- `src/datasystem/common/log/logging.cpp`
- `src/datasystem/common/log/log_manager.cpp`

Failure-sensitive steps:

- provider init failure disables `FLAGS_log_monitor`, creating partial observability degradation;
- changing startup order can leave normal logs available while monitor/access logs silently stop.

### Trace Propagation And Scoped Correlation

1. public entrypoints or async boundaries create or import trace IDs;
2. downstream work reads the thread-local trace state while emitting logs;
3. scope exit clears or preserves trace state through `TraceGuard`.

Key files:

- `src/datasystem/common/log/trace.cpp`
- `src/datasystem/context/context.cpp`
- representative propagation callsites such as `src/datasystem/worker/worker_oc_server.cpp`

Failure-sensitive steps:

- missing `SetTraceNewID()` at async boundaries breaks request correlation without breaking functionality;
- incorrect `keep=true` or missing guard cleanup can leak trace state across reused threads.

### Access/Monitor Logging And Maintenance

1. operation code creates an `AccessRecorder`;
2. `Record()` classifies the key and forwards a structured line to `AccessRecorderManager`;
3. exporter buffers are flushed asynchronously through `HardDiskExporter`;
4. `LogManager` periodically forces monitor flush and handles rotation/compression.

Key files:

- `src/datasystem/common/log/access_recorder.cpp`
- `src/datasystem/common/log/log_manager.cpp`
- `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`

Failure-sensitive steps:

- missing `Record()` emits recorder-destruction errors and drops intended latency data;
- exporter or rotation changes affect access logs and resource monitor files together.

## Data And State Model

- Important in-memory state:
  - thread-local trace state in `Trace`;
  - singleton lifecycle state in `Logging`;
  - exporter ownership and per-channel writers in `AccessRecorderManager`;
  - background loop state in `LogManager`.
- Important on-disk or external state:
  - log directory and ordinary log files;
  - access/client-access/resource monitor files;
  - `container.log` for crash-path output.
- Ownership and lifecycle rules:
  - `Logging` owns startup sequence for the logging subsystem;
  - `AccessRecorderManager` is accessed through `Logging`;
  - `LogManager` owns maintenance scheduling rather than each caller doing its own file management.
- Concurrency or thread-affinity rules:
  - trace state is thread-local;
  - flush and maintenance work runs asynchronously;
  - access/monitor emission must tolerate producer threads different from flush threads.
- Ordering requirements or invariants:
  - provider init must happen before ordinary logging is relied on;
  - access manager must exist before access-monitor writes can succeed;
  - trace import must happen before logs emitted on the receiving thread.

## Configuration Model

| Config | Type | Default or source | Effect | Risk if changed |
| --- | --- | --- | --- | --- |
| `log_monitor` | gflag | common gflags | enables monitor/access/resource flushing paths | may silently disable monitor outputs |
| `max_log_file_num` | gflag/env override | gflag plus client env override | controls retention/pruning count | can remove history too aggressively or grow disk usage |
| `log_compress` | gflag/env override | gflag plus client env override | enables compression of rotated files | affects CPU cost and operational expectations |
| `log_retention_day` | gflag/env override | gflag plus client env override | retention horizon | can conflict with operational audit expectations |
| `max_log_size` | gflag/env override | gflag plus client env override | drives rotation threshold | can break downstream tailing or inflate files |
| `log_async_queue_size` | gflag/env override | gflag plus client env override | affects async buffering pressure | can increase drop/latency risk under load |
| `DATASYSTEM_CLIENT_LOG_DIR` | environment | client env | overrides client log directory | can redirect output unexpectedly |
| `DATASYSTEM_LOG_MONITOR_ENABLE` | environment | client env | client-side monitor enable override | can create client/runtime behavior mismatch |

## Failure Model

- Expected failure modes:
  - provider initialization failure;
  - missing access recorder completion;
  - exporter or file-write issues beneath access/monitor paths;
  - incomplete failure-signal installation.
- Partial-degradation behavior:
  - normal service logic may continue even when `FLAGS_log_monitor` is disabled or monitor flush fails;
  - crash output uses a separate direct-write path.
- Retry, fallback, or fail-fast rules:
  - monitor logging is degraded rather than fail-fast when provider init fails;
  - crash handler falls back to direct file writing instead of normal log flow.
- What must remain true after failure:
  - ordinary logging should remain usable when possible;
  - trace state should not leak across unrelated work;
  - log file lifecycle should not corrupt existing rotated files.

## Observability

- Main logs:
  - ordinary repository logs through logging macros;
  - access/client-access/request-out style performance logs;
  - crash output in `container.log`.
- Metrics:
  - none owned as primary output, but monitor/resource flushing is coordinated with metrics-backed exporters.
- Traces or correlation fields:
  - `Trace` IDs, imported or generated at request boundaries.
- Debug hooks or inspection commands:
  - inspect `logging.cpp`, `log_manager.cpp`, `access_recorder.cpp`, `trace.cpp`, and emitted log files;
  - confirm `log_monitor`-related flags and environment overrides.
- How to tell the module is healthy:
  - ordinary logs emit with expected severity behavior;
  - trace IDs remain correlated across async boundaries;
  - access/resource files flush and rotate as expected;
  - crash-path output is present when failure handler is triggered.

## Security And Safety Constraints

- Authn/authz expectations:
  - this module is infrastructure only and should not become the authority for access-control decisions.
- Input validation requirements:
  - logged payloads and metadata should be treated as caller-provided data and must not assume trusted formatting.
- Data sensitivity or privacy requirements:
  - new logging features must avoid leaking secrets, credentials, or sensitive request data into ordinary or access logs.
- Resource or isolation constraints:
  - async buffering and rotation settings must respect process disk and memory limits.
- Unsafe changes to avoid:
  - changing trace propagation semantics without reviewing async callsites;
  - changing access log schema or file handling without checking downstream consumers.

## Compatibility And Invariants

- External compatibility constraints:
  - operators and downstream tooling may depend on current file names, retention behavior, and access/monitor record shape.
- Internal invariants:
  - `Trace` remains thread-local;
  - access/performance records route through `AccessRecorderManager`;
  - `LogManager` remains the owner of periodic maintenance.
- File format / wire format / schema stability notes:
  - access log key mapping from `access_point.def` should be treated as stability-sensitive.
- Ordering or naming stability notes:
  - log rotation patterns and file naming conventions should be considered operationally visible.

## Performance Characteristics

- Hot paths:
  - ordinary log macro emission;
  - trace helper use on public and async boundaries;
  - access recorder `Record()` on request-critical paths.
- Known expensive operations:
  - file compression;
  - rotation and pruning;
  - flush-thread disk I/O.
- Buffering, batching, or async behavior:
  - access and monitor paths are buffered and flushed asynchronously through exporter threads;
  - maintenance is centralized in a background loop rather than done inline by producers.
- Resource limits or scaling assumptions:
  - file-count and size controls are expected to bound disk growth;
  - async queue size constrains producer/consumer pressure.

## Build, Test, And Verification

- Build entrypoints:
  - repository standard build flow via `build.sh`
  - target-level builds for `common_log` and `hard_disk_exporter`
- Fast verification commands:
  - `cmake --build <build-dir> --target common_log hard_disk_exporter`
- Representative unit/integration/system tests:
  - `Pending verification`: add specific logging-focused test targets when indexed
- Recommended validation for risky changes:
  - verify ordinary log emission, access/performance output, rotation/compression behavior, and trace continuity across one async path.

## Common Change Scenarios

### Adding A New Capability

- Recommended extension point:
  - ordinary log behavior in `logging.*` or `log_manager.*`;
  - request-scoped observability in `trace.*` or `access_recorder.*`.
- Required companion updates:
  - `.repo_context/modules/infra/logging/*`
  - relevant metrics/exporter docs if `HardDiskExporter` or monitor output changes
  - any downstream parsing or operational documentation affected by schema/file changes
- Review checklist:
  - does it preserve trace semantics?
  - does it preserve access/monitor file compatibility?
  - does it change startup or degradation behavior?

### Modifying Existing Behavior

- Likely breakpoints:
  - startup ordering in `Logging::Start()`
  - `AccessRecorderManager` flush behavior
  - `TraceGuard` cleanup semantics
- Compatibility checks:
  - file names, retention, record format, and access-point mappings
- Observability checks:
  - ordinary logs, access logs, monitor files, and one async trace path

### Debugging A Production Issue

- First files to inspect:
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/log/log_manager.cpp`
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/trace.cpp`
- First runtime evidence to inspect:
  - resolved log directory contents
  - `log_monitor` and retention-related config
  - whether trace IDs appear consistent across caller/callee logs
- Common misleading symptoms:
  - business logic succeeds while monitor logs disappear;
  - access logs look incomplete because `Record()` was skipped on one path rather than exporter being entirely broken.

## Open Questions

- whether all runtime startup paths consistently install the failure signal handler;
- whether any legacy non-spdlog backend path still exists and matters for future changes.

## Pending Verification

- exact repository test targets that exercise rotation, trace propagation, and access recording end to end;
- whether any downstream operational tooling depends on specific access-log field ordering beyond current code references.

## Update Rules For This Document

- Keep design statements aligned with the real implementation and linked source files.
- When module boundaries, major flows, config semantics, or compatibility rules change, update this doc in the same task when practical.
- If a section becomes speculative or stale, narrow it or move it to `Pending verification` until confirmed from source.
