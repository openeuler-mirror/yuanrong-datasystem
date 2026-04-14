# Logging Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - `current implementation | mixed`
- Primary code paths:
  - `src/datasystem/common/log/*`
  - `src/datasystem/common/log/spdlog/*`
  - `src/datasystem/common/metrics/hard_disk_exporter/*`
  - `include/datasystem/context/context.h`
  - `src/datasystem/client/context/context.cpp`
- Primary source-of-truth files:
  - `src/datasystem/common/log/CMakeLists.txt`
  - `src/datasystem/common/log/BUILD.bazel`
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
  - `src/datasystem/common/log/spdlog/logger_context.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
  - `include/datasystem/context/context.h`
  - `src/datasystem/client/context/context.cpp`
  - `tests/ut/common/log/BUILD.bazel`
  - `tests/st/common/log/BUILD.bazel`
- Last verified against source:
  - `2026-04-13`
- Related context docs:
  - `.repo_context/modules/infra/logging/README.md`
  - `.repo_context/modules/infra/logging/trace-and-context.md`
  - `.repo_context/modules/infra/logging/access-recorder.md`
  - `.repo_context/modules/infra/logging/log-lifecycle-and-rotation.md`
  - `.repo_context/modules/infra/metrics/README.md`
- Related user-facing or internal docs:
  - none linked yet

## Purpose

- Why this design document exists:
  - provide a source-backed architecture reference before adding or modifying repository-wide logging behavior.
- What problem this module solves:
  - give the repository one shared observability substrate for ordinary logs, trace correlation, access and performance records, log file lifecycle management, and crash-path output.
- Who or what depends on this module:
  - client wrappers, master and worker runtime code, async background tasks, and operators who depend on log files for diagnosis and postmortem analysis.

## Business And Scenario Overview

- Why this capability is needed:
  - the repository is a distributed system with async work, multi-language entrypoints, and background maintenance flows, so failures are hard to diagnose without a shared logging and trace model.
- Target users, callers, or operators:
  - C/C++/Java API callers, worker and master developers, performance-test authors, and service operators.
- Typical usage time or trigger conditions:
  - every process startup, every request boundary, every async handoff that needs trace continuity, and every monitored operation that should emit performance records.
- Typical deployment or runtime environment:
  - local client processes and long-lived worker or master processes writing to a log directory on the host filesystem.
- How the feature is expected to be used:
  - callers use `LOG` and `VLOG` macros for ordinary logs, `Trace` helpers for correlation, `Context::SetTraceId` for caller-supplied prefixes, and `AccessRecorder` for structured access or latency output.
- What other functions or modules it may affect:
  - public API wrappers, async execution pools, access-log parsing tooling, disk-usage behavior, and crash forensics.
- Expected user or operator experience:
  - performance:
    - ordinary log emission should stay cheap enough for common paths, with async buffering enabled by default.
  - security:
    - logging should not become a new path for leaking credentials or unsafe caller data.
  - resilience:
    - monitor or access logging may degrade independently without taking the whole service down.
  - reliability:
    - trace state and access-log semantics should remain stable across async boundaries and restarts.
  - availability:
    - core service logic should usually continue even when some logging subpaths fail.
  - privacy:
    - request metadata recorded into logs must be reviewed carefully before expansion.

## Goals

- provide one macro surface and lifecycle surface that can be reused across the repository;
- keep request and background work observable through trace IDs;
- separate request-path logging from background file maintenance and exporter flush work;
- support retention concerns such as rotation, pruning, compression, and crash-path persistence;
- preserve enough compatibility that operational tooling and existing callsites continue to work.

## Non-Goals

- owning business-level event schema outside the existing access and performance records;
- enforcing authn or authz policy;
- replacing metrics collection or metrics-family registration;
- providing cluster-wide replicated log storage inside this module.

## Scope

- In scope:
  - ordinary log initialization and provider setup;
  - client-side environment overrides for logging behavior;
  - trace ID generation, import, scoping, and cleanup;
  - access, request-out, client-access, and resource-monitor file flush coordination;
  - background rolling, compression, and retention management;
  - crash-path failure-signal output to `container.log`.
- Out of scope:
  - business feature semantics that only happen to emit logs;
  - distributed trace transport outside explicit trace handoff points;
  - metrics-family definitions and collection cadence;
  - centralized log aggregation infrastructure outside the local process and filesystem.
- Important boundaries with neighboring modules:
  - `src/datasystem/common/log/*` owns macro surface, lifecycle, trace state, recorder lifecycle, and crash-path output.
  - `src/datasystem/common/metrics/hard_disk_exporter/*` owns the buffered file exporters used under access and monitor logging.
  - runtime modules and language bindings decide where trace IDs are created, propagated, or imported.

## Terminology

| Term | Meaning in this repository | Source or note |
| --- | --- | --- |
| `Logging` | singleton lifecycle owner for logging startup and shutdown | `src/datasystem/common/log/logging.h/.cpp` |
| `Trace` | thread-local trace context holder | `src/datasystem/common/log/trace.h/.cpp` |
| `TraceGuard` | scoped helper that clears trace or sub-trace state on scope exit | `src/datasystem/common/log/trace.h/.cpp` |
| `AccessRecorder` | per-operation latency and metadata recorder | `src/datasystem/common/log/access_recorder.h/.cpp` |
| `AccessRecorderManager` | owner of exporter instances for access, request-out, and client-access logs | `src/datasystem/common/log/access_recorder.h/.cpp` |
| `LogManager` | background loop for compression, rolling, pruning, and monitor flush | `src/datasystem/common/log/log_manager.h/.cpp` |
| `FailureWriter` | crash-path writer used by absl failure-signal handling | `src/datasystem/common/log/failure_handler.cpp` |
| `HardDiskExporter` | buffered exporter that writes monitor-style records to disk asynchronously | `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h/.cpp` |

## Current State And Design Choice Notes

- Current implementation or baseline behavior:
  - the module wraps a local `ds_spdlog` backend for ordinary logs, uses a singleton `Logging` lifecycle, and delegates access or resource log flushing to `HardDiskExporter`.
- Relevant constraints from current release or deployment:
  - the design is local-process and filesystem-based, not a replicated logging service;
  - clients can override many logging behaviors from environment variables;
  - log rolling and compression are managed by `LogManager`, not by spdlog's built-in file-retention policy.
- Similar upstream, industry, or historical approaches considered:
  - the current codebase keeps a macro surface similar to glog-style logging while using spdlog under a wrapper layer;
  - crash handling uses absl failure-signal support rather than a fully custom signal handler stack.
- Selected approach and why:
  - the repository keeps a narrow repository-owned API surface while reusing mature OSS pieces for file logging, regex validation, and failure-signal symbolization.
- Known tradeoffs:
  - observability is strong for a local process, but availability is based on graceful degradation rather than redundant replicas;
  - access-log formatting is simple and stable, but changes are compatibility-sensitive for downstream parsing.

## Architecture Overview

- High-level structure:
  - `log.h` exposes macro-based log emission;
  - `Logging` initializes providers, config, `LogManager`, and `AccessRecorderManager`;
  - `Trace` carries correlation state through thread-local storage;
  - `AccessRecorder` and `AccessRecorderManager` serialize structured performance records;
  - `LogManager` performs periodic file maintenance and monitor flush;
  - `FailureWriter` persists crash output directly to file.
- Key runtime roles or processes:
  - request or worker threads emit logs and maintain trace state;
  - a background maintenance thread owned by `LogManager` performs rolling, compression, pruning, and monitor flush;
  - exporter flush threads owned under `HardDiskExporter` drain buffered records to disk.
- Key persistent state, if any:
  - ordinary severity log files under `FLAGS_log_dir`;
  - access, request-out, client-access, and resource monitor files;
  - `container.log` for crash-path output.
- Key control-flow or data-flow stages:
  - startup and config override;
  - ordinary log emission through the provider;
  - trace creation or import at request and async boundaries;
  - access-record buffering and async disk flush;
  - background rolling, compression, and retention pruning;
  - crash-path flush and direct write.

## Scenario Analysis

- Primary user or system scenarios:
  - a public API wrapper creates a new trace ID and emits ordinary logs for one request;
  - an async task captures the current trace ID and restores it on a different thread;
  - a request path records latency and metadata into access or request-out logs;
  - a long-running process rotates and compresses old logs without blocking request threads;
  - a fatal signal writes crash information into `container.log`.
- Key interactions between actors and services:
  - public entrypoints and async workers interact with `Trace`;
  - business code interacts with `AccessRecorder`;
  - `Logging` and `LogManager` interact with the filesystem and `HardDiskExporter`;
  - failure-signal handling interacts with absl symbolization and the direct file-write path.
- Normal path summary:
  - `Logging::Start()` initializes the provider and background services, callers emit logs and traces, `AccessRecorder` writes buffered structured records, and `LogManager` maintains the resulting files.
- Exceptional path summary:
  - provider or exporter initialization failures degrade monitor logging first, trace continuity can be lost if async boundaries omit `SetTraceNewID`, and crash handling bypasses normal logger paths with direct file writes.

## Entry Points, External Interfaces, And Integration Points

- Public APIs:
  - logging macros from `src/datasystem/common/log/log.h`, including `LOG`, `VLOG`, `CHECK`, `LOG_EVERY_N`, and `LOG_FIRST_N`;
  - `Trace::SetTraceUUID`, `Trace::SetTraceNewID`, `Trace::SetSubTraceID`, and `Trace::GetTraceID`;
  - `datasystem::Context::SetTraceId` from `include/datasystem/context/context.h`;
  - `AccessRecorder` construction plus `Record()`.
- Internal service entrypoints:
  - `Logging::Start(...)`
  - `Logging::AccessRecorderManagerInstance()`
  - `Logging::WriteLogToFile(...)`
  - `InstallFailureSignalHandler(const char *arg0)`
- External protocols, schemas, or data formats:
  - ordinary log records follow the repository log prefix format built by `ConstructLogPrefix(...)`;
  - access and request-out logs are pipe-delimited records assembled in `AccessRecorderManager::LogPerformance(...)`;
  - crash output is written as plain text lines to `container.log`.
- CLI commands, config flags, or environment variables:
  - gflags:
    - `log_monitor`
    - `max_log_file_num`
    - `log_compress`
    - `log_retention_day`
    - `logbufsecs`
    - `logfile_mode`
    - `max_log_size`
    - `logtostderr`
    - `alsologtostderr`
    - `stderrthreshold`
    - `minloglevel`
    - `log_async`
    - `log_async_queue_size`
    - `log_dir`
    - `log_filename`
  - client environment overrides:
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
- Background jobs, threads, or callbacks:
  - `LogManager` background thread started by `LogManager::Start()`;
  - exporter flush threads under `HardDiskExporter`;
  - failure-signal callback configured through absl `FailureSignalHandlerOptions`.
- Cross-module integration points:
  - `HardDiskExporter` from metrics is used underneath `AccessRecorderManager`;
  - `include/datasystem/context/context.h` and `src/datasystem/client/context/context.cpp` feed trace prefixes into `Trace`;
  - worker, master, and language-binding callsites create or restore trace IDs at request boundaries.
- Upstream and downstream dependencies:
  - upstream callers include public C/C++/Java wrappers and runtime code that emits logs or sets trace state;
  - downstream dependencies include the local filesystem, `ds_spdlog`, absl failure handling, RE2 log-name validation, and any operators or tooling that parse the resulting files.
- Backward-compatibility expectations for external consumers:
  - file names, access-log record layout, trace semantics, and environment variable meanings should be treated as operationally visible behavior.

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| `log.h` | repository-wide macro surface for logs and checks | `src/datasystem/common/log/log.h` | main entrypoint seen by most callers |
| `Logging` | provider init, env override, singleton lifecycle, startup ordering | `src/datasystem/common/log/logging.h/.cpp` | creates `LogManager` and `AccessRecorderManager` |
| `LogManager` | background rolling, compression, pruning, and monitor flush | `src/datasystem/common/log/log_manager.h/.cpp` | one local background thread |
| `Trace` | thread-local correlation state | `src/datasystem/common/log/trace.h/.cpp` | supports generated and imported trace IDs |
| `Context` | public API surface for setting trace prefix on current thread | `include/datasystem/context/context.h`, `src/datasystem/client/context/context.cpp` | validates format before feeding `Trace` |
| `AccessRecorder` | scoped request latency recording | `src/datasystem/common/log/access_recorder.h/.cpp`, `src/datasystem/common/log/access_point.def` | destructor logs if `Record()` was skipped |
| `AccessRecorderManager` | exporter ownership and record serialization | `src/datasystem/common/log/access_recorder.h/.cpp` | routes by `AccessKeyType` |
| `FailureWriter` | crash-path file persistence | `src/datasystem/common/log/failure_handler.h/.cpp` | bypasses normal logger stack on fatal paths |
| `ds_spdlog` wrapper | spdlog wrapper and provider abstraction | `src/datasystem/common/log/spdlog/*` | repository patches namespace and rotating sink behavior |

## Main Flows

### Startup And Ordinary Logging

1. Runtime or client code calls `Logging::Start(...)`.
2. Client mode applies environment-driven config overrides before provider initialization.
3. `InitLoggingWrapper(...)` creates the log directory, initializes the spdlog-backed provider, starts `LogManager`, and initializes `AccessRecorderManager`.
4. Ordinary `LOG` and `VLOG` macros emit through the configured provider and sinks.

Key files:

- `src/datasystem/common/log/logging.cpp`
- `src/datasystem/common/log/spdlog/logger_context.cpp`
- `src/datasystem/common/log/log_manager.cpp`

Failure-sensitive steps:

- if `InitLoggingWrapper(...)` fails, `Logging::Start(...)` disables `FLAGS_log_monitor`, which degrades monitor logging rather than terminating the process;
- startup ordering matters because access-recorder and background-maintenance paths assume provider and directory setup already succeeded.

### Trace Propagation And Scoped Correlation

1. Public entrypoints create a new root trace with `Trace::SetTraceUUID()` or import a trace with `Context::SetTraceId(...)`.
2. Async boundaries capture `Trace::GetTraceID()` and restore it with `Trace::SetTraceNewID(...)` on worker threads.
3. `TraceGuard` clears trace or sub-trace state on scope exit unless created with `keep=true`.

Key files:

- `src/datasystem/common/log/trace.cpp`
- `include/datasystem/context/context.h`
- `src/datasystem/client/context/context.cpp`
- representative propagation callsites such as `src/datasystem/client/object_cache/object_client_impl.cpp`

Failure-sensitive steps:

- missing `SetTraceNewID(...)` at async boundaries breaks request correlation without breaking business logic;
- incorrect guard semantics can leak trace state across unrelated work on reused threads.

### Access, Request-Out, And Monitor Logging

1. Operation code constructs an `AccessRecorder`.
2. `Record()` computes elapsed time and forwards a structured line through `AccessRecorderManager::LogPerformance(...)`.
3. `AccessRecorderManager` routes records to `HardDiskExporter` instances by `AccessKeyType`.
4. `LogManager::DoLogMonitorWrite()` periodically calls `SubmitWriteMessage()` to flush buffered records.

Key files:

- `src/datasystem/common/log/access_recorder.cpp`
- `src/datasystem/common/log/log_manager.cpp`
- `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`

Failure-sensitive steps:

- skipping `Record()` produces a destructor error log and loses intended latency output;
- exporter initialization failure leaves access or monitor paths unavailable;
- record-format changes affect downstream consumers immediately.

### Rolling, Compression, Retention, And Crash Output

1. `LogManager` wakes periodically and runs compression, rolling, pruning, and monitor flush.
2. It prunes files according to `max_log_file_num` and `log_retention_day`.
3. On fatal signals, absl invokes `FailureWriter`, which flushes buffered logs on null input and writes failure text to `container.log`.

Key files:

- `src/datasystem/common/log/log_manager.cpp`
- `src/datasystem/common/log/failure_handler.cpp`
- `src/datasystem/common/log/logging.cpp`

Failure-sensitive steps:

- changing filename patterns can break rolling and pruning for access or resource logs;
- crash-path logging depends on `FLAGS_log_dir` resolving successfully and bypasses normal logger sinks.

## Data And State Model

- Important in-memory state:
  - thread-local trace buffers inside `Trace`;
  - singleton initialization state inside `Logging`;
  - exporter map keyed by `AccessKeyType` inside `AccessRecorderManager`;
  - `LogManager` thread state and timer loop;
  - spdlog global thread pool and logger registration inside the `ds_spdlog` wrapper.
- Important on-disk or external state:
  - ordinary severity log files under `FLAGS_log_dir`;
  - `access.log`, `request_out.log`, `ds_client_access*.log`, and `resource*.log` families;
  - `container.log` for fatal backtraces;
  - environment-variable configuration visible during client initialization.
- Ownership and lifecycle rules:
  - `Logging` owns startup sequence and the `LogManager` plus `AccessRecorderManager` instances;
  - `LogManager` owns periodic maintenance instead of letting producers manipulate log files directly;
  - `AccessRecorder` is scoped to one operation and should call `Record()` before destruction.
- Concurrency or thread-affinity rules:
  - `Trace` is thread-local by design;
  - ordinary log emission may be asynchronous depending on `log_async`;
  - background maintenance and exporter flush occur on non-request threads;
  - trace import must be explicit across threads.
- Ordering requirements or invariants:
  - provider initialization must finish before ordinary logging is relied on;
  - access-recorder exporters must be initialized before monitor flush;
  - trace prefixes must be set before root trace generation if the prefix should be preserved;
  - `LogManager` remains the sole owner of periodic rolling and compression.

## External Interaction And Dependency Analysis

- Dependency graph summary:
  - callers use logging macros, trace helpers, `Context::SetTraceId`, and `AccessRecorder`;
  - `Logging` depends on flags, utility helpers, the spdlog wrapper, `HardDiskExporter`, absl failure handling, and RE2 validation;
  - operators depend on the resulting local files for diagnosis.
- Critical upstream services or modules:
  - public API wrappers in `src/datasystem/c_api/*` and `src/datasystem/java_api/*`;
  - async runtime code in client, worker, and master modules that must preserve trace IDs explicitly.
- Critical downstream services or modules:
  - local filesystem under `FLAGS_log_dir`;
  - `src/datasystem/common/metrics/hard_disk_exporter/*`;
  - `src/datasystem/common/log/spdlog/*`;
  - absl symbolization and failure-signal handling.
- Failure impact from each critical dependency:
  - filesystem failures prevent file creation or crash writing;
  - `HardDiskExporter` failures remove access or monitor visibility while business logic may continue;
  - provider init failures disable monitor logging by clearing `FLAGS_log_monitor`;
  - async callsite mistakes cause trace discontinuity without obvious functional failure.
- Version, protocol, or schema coupling:
  - access-log field ordering and access-point names are schema-like operational contracts;
  - filename patterns are coupled to rolling, pruning, and any external file collectors.
- Deployment or operations dependencies:
  - writable log directory permissions;
  - sufficient disk and async queue capacity;
  - consistent environment-variable usage for client deployments.

## Open Source Software Selection And Dependency Record

| Dependency | Purpose in this module | Why selected | Alternatives considered | License | Version / upgrade strategy | Security / maintenance notes |
| --- | --- | --- | --- | --- | --- | --- |
| `spdlog` via `ds_spdlog` | ordinary file and stderr logging, async logger support, rotating sink support | mature logging backend with async and sink abstractions while preserving a repository-owned macro surface | keep a fully custom logger, or expose raw glog-like implementation details | `MIT` | pinned to `spdlog-1.12.0` in `bazel/ds_deps.bzl`; upgrade through wrapper and patch review | repository applies namespace and rotating-sink patches under `third_party/patches/spdlog/*`; retention remains externally managed by `LogManager` |
| `abseil-cpp` | failure-signal handling and symbolization for crash-path output | reduces custom signal-handler code and keeps crash output integrated with stack symbolization | custom signal handling only | `Apache-2.0` | pinned to `abseil-cpp-20250127.1` in `bazel/ds_deps.bzl`; upgrade with crash-path regression checks | signal-handler behavior is high risk; verify `container.log` output and handler install paths after upgrades |
| `RE2` | log-name validation in `Logging::ValidateLogName(...)` | safe and predictable regex engine for user-controlled environment input | manual validation logic | `BSD-3-Clause` | pinned to `re2-2024-07-02` in `bazel/ds_deps.bzl`; upgrade with validation tests | repository carries an absl-related patch for Bazel integration |

## Configuration Model

| Config | Type | Default or source | Effect | Risk if changed |
| --- | --- | --- | --- | --- |
| `log_monitor` | gflag | runtime gflag | enables access and monitor flush paths | observability may partially disappear while service logic continues |
| `max_log_file_num` | gflag or env override | default `5` in `logging.cpp` for normal path | limits retained files per severity family | too small loses history; too large grows disk usage |
| `log_compress` | gflag or env override | default `true` | compresses rotated log files | changes CPU cost, file collector behavior, and file suffix expectations |
| `log_retention_day` | gflag or env override | default `0` meaning no age-based prune | age-based deletion | may violate audit expectations or exhaust disk if unset |
| `max_log_size` | gflag or env override | default `100` in `logging.cpp` for normal path, client env override available | rotation threshold in MB | alters file count, tailing behavior, and compression cadence |
| `log_async` | gflag or env override | default `true` | enables async ordinary log writing | can reorder failure perception under stress or drop oldest messages on overflow policy |
| `log_async_queue_size` | gflag or env override | default `1024` in `logging.cpp` for this path | bounds async queue depth | too low increases overwrite risk, too high increases memory use |
| `DATASYSTEM_CLIENT_LOG_DIR` | environment | client-only override | changes client log destination directory | can redirect output unexpectedly or fail on bad permissions |
| `DATASYSTEM_CLIENT_LOG_NAME` | environment | client-only override validated by RE2 | changes client ordinary log base name | unsafe naming would break file handling if validation changed |
| `DATASYSTEM_CLIENT_ACCESS_LOG_NAME` | environment | client-only override validated by RE2 | changes client access-log base name | can break downstream file discovery assumptions |
| `DATASYSTEM_LOG_MONITOR_ENABLE` | environment | client-only override | enables or disables client monitor logging | may create client and server observability mismatch |

## Examples And Migration Notes

- Example API requests or responses to retain:
  - `Context::SetTraceId("tenantA")` sets a prefix that is later prepended to root trace generation on the current thread.
- Example configuration or deployment snippets to retain:
  - client processes can override log location and naming with `DATASYSTEM_CLIENT_LOG_DIR`, `DATASYSTEM_CLIENT_LOG_NAME`, and `DATASYSTEM_CLIENT_ACCESS_LOG_NAME`.
- Example operator workflows or commands to retain:
  - inspect the resolved log directory for ordinary severity logs plus `access`, `request_out`, `ds_client_access`, `resource`, and `container.log` families when diagnosing logging issues.
- Upgrade, rollout, or migration examples if behavior changes:
  - when changing file naming, access-log record shape, or trace semantics, update downstream collectors, `.repo_context/modules/infra/logging/*.md`, and any operational runbooks in the same task.

## Availability

- Availability goals or service-level expectations:
  - logging is expected to improve diagnosis without becoming a hard dependency for most business logic;
  - ordinary service paths should usually continue even if monitor logging degrades.
- Deployment topology and redundancy model:
  - this is a per-process local subsystem with no cross-process redundancy inside the module itself.
- Single-point-of-failure analysis:
  - `Logging` singleton and the local log directory are single local control points;
  - `LogManager` uses one background thread for maintenance;
  - `AccessRecorderManager` is the sole owner of access-log exporters in a process.
- Failure domains and blast-radius limits:
  - ordinary logs, monitor logs, and crash output are partly separable;
  - provider init failures disable monitor logging first by setting `FLAGS_log_monitor = false`;
  - crash output has a direct-write path and does not depend on normal logger sinks.
- Health checks, readiness, and traffic removal conditions:
  - there is no dedicated readiness endpoint in this module;
  - health is inferred from successful initialization, expected file creation, and ongoing flush or rotation behavior.
- Failover, switchover, or recovery entry conditions:
  - recovery is local and process-scoped: restart the process, restore filesystem access, or re-enable config rather than fail over to a secondary logging replica.
- Capacity reservation or headroom assumptions:
  - async queue size, file-count limits, max file size, and retention settings are the main local capacity controls.

## Reliability

- Expected failure modes:
  - log directory creation failure;
  - provider initialization failure;
  - exporter initialization or file-write failures beneath access or resource logging;
  - skipped `AccessRecorder::Record()` calls;
  - trace propagation mistakes across async boundaries;
  - incomplete failure-signal installation.
- Data consistency, durability, or correctness requirements:
  - ordinary logs and structured access records should preserve line integrity once written;
  - trace IDs should accurately reflect the request or async lineage of the emitting thread;
  - crash output should be written to `container.log` even when normal logging is unavailable.
- Retry, idempotency, deduplication, or exactly-once assumptions:
  - this module does not provide exactly-once guarantees for logs;
  - reliability comes from bounded buffering, periodic flush, and file-based persistence rather than deduplicated delivery.
- Partial-degradation behavior:
  - monitor and access logging can degrade independently while ordinary business logic continues;
  - crash logging falls back to a direct file-write path;
  - client-only environment overrides affect client behavior without changing server-side defaults.
- Recovery, replay, repair, or reconciliation strategy:
  - restart or reinitialize the process after fixing config or filesystem issues;
  - inspect file families and trace continuity to determine whether the failure was provider-side, exporter-side, or callsite-side.
- RTO, RPO, MTTR, or equivalent recovery targets:
  - no explicit numeric targets are defined in code or current docs;
  - recovery is expected to be operationally fast because state is local and recreated at startup.
- What must remain true after failure:
  - trace state must not leak across unrelated work;
  - rolling and compression must not corrupt existing files;
  - file naming and access-key routing must remain internally consistent.

## Resilience

- Overload protection, rate limiting, or admission control:
  - async queue sizing and buffered exporters provide limited overload smoothing rather than hard admission control;
  - request-path code does not block on every file write because access and monitor paths are asynchronous.
- Timeout, circuit-breaker, and backpressure strategy:
  - no explicit circuit breaker is implemented;
  - resilience relies on background threads, bounded queues, and degrading monitor logging instead of failing the process.
- Dependency failure handling:
  - provider init failure disables monitor logging;
  - empty exporter maps return `K_NOT_READY` and log errors when monitor logging is enabled;
  - failure-signal path writes directly to file and flushes buffered logs on null callback input.
- Graceful-degradation behavior:
  - observability becomes partial rather than total in several failure cases;
  - business logic often survives even when access or monitor logs do not.
- Safe rollback, feature-gating, or kill-switch strategy:
  - most logging behavior remains configurable through gflags and client environment overrides;
  - `log_monitor` is the main kill switch for structured monitor logging.
- Attack or fault scenarios the module should continue to tolerate:
  - malformed client-provided log names rejected by `ValidateLogName(...)`;
  - async trace discontinuity at one callsite should not corrupt unrelated threads;
  - crash handler should still emit direct file output when normal logger sinks are unhealthy.

## Security, Privacy, And Safety Constraints

- Authn/authz expectations:
  - the module is infrastructure only and must not become an authority for permission decisions.
- Input validation requirements:
  - `Context::SetTraceId(...)` validates trace-prefix characters and length;
  - `Logging::ValidateLogName(...)` only accepts `[a-zA-Z0-9_]*` for client-supplied log names.
- Data sensitivity or privacy requirements:
  - request and response metadata logged by `AccessRecorder` may contain sensitive business context, so any schema expansion requires review for secrets, personal data, or credentials.
- Secrets, credentials, or key-management requirements:
  - credentials and tokens should not be written into ordinary or structured logs;
  - crash logs should not accidentally expand sensitive memory dumps beyond current behavior.
- Resource or isolation constraints:
  - log directories are created with `0700` permissions in `Logging::CreateLogDir()`;
  - `WriteLogToFile(...)` uses mode `0640` for crash-path files.
- Unsafe changes to avoid:
  - changing trace semantics without reviewing async callsites;
  - weakening log-name or trace-prefix validation;
  - changing access-log formatting without checking downstream consumers.
- Safety constraints for human, environment, or property impact:
  - none directly safety-critical are encoded here, but runaway disk growth can still harm service operation if retention controls are misconfigured.

## Observability

- Main logs:
  - ordinary severity logs from `LOG` and `VLOG`;
  - `access`, `request_out`, `ds_client_access`, and `resource` file families;
  - `container.log` for fatal backtrace output.
- Metrics:
  - no primary metrics are defined by this module, but resource-monitor and access flushing reuse `HardDiskExporter` from the metrics area.
- Traces or correlation fields:
  - thread-local trace IDs generated, imported, or extended through `Trace`;
  - trace prefixes set by `Context::SetTraceId(...)`.
- Alerts or SLO-related signals:
  - no built-in alerting definitions are encoded here;
  - operators should watch for missing log files, failed rolling, missing access output, or broken trace continuity.
- Debug hooks or inspection commands:
  - inspect `src/datasystem/common/log/logging.cpp`, `log_manager.cpp`, `access_recorder.cpp`, `trace.cpp`, and the actual files under `FLAGS_log_dir`;
  - verify `FLAGS_log_monitor`, directory permissions, and client env overrides.
- How to tell the module is healthy:
  - `Logging::Start(...)` initializes successfully;
  - ordinary logs appear with expected severity handling;
  - trace IDs remain correlated across one representative async boundary;
  - access or resource files flush and rotate as expected;
  - fatal-path tests or controlled crashes produce `container.log`.

## Compatibility And Invariants

- External compatibility constraints:
  - operators and tooling may depend on current file names, file suffixes, access-log field ordering, and env-var semantics.
- Internal invariants:
  - `Trace` remains thread-local;
  - `LogManager` remains owner of periodic maintenance;
  - `AccessRecorderManager` owns exporter routing by `AccessKeyType`;
  - provider initialization happens before access-recorder use.
- File format, wire format, or schema stability notes:
  - pipe-delimited access and request-out records are compatibility-sensitive;
  - access-point naming in `access_point.def` should be treated as schema-like state for observability consumers.
- Ordering or naming stability notes:
  - filename patterns used by `LogManager` rolling and pruning must stay aligned with what `AccessRecorderManager` creates;
  - log-name validation affects externally visible file paths.
- Upgrade, downgrade, or mixed-version constraints:
  - no explicit mixed-version protocol exists inside the module itself, but changing file shapes or trace semantics can create operational skew between old and new processes during rollout.

## Performance Characteristics

- Hot paths:
  - ordinary log macro emission;
  - trace setup on public and async boundaries;
  - `AccessRecorder::Record()` on request-critical paths.
- Known expensive operations:
  - file compression and pruning;
  - background flush disk I/O;
  - fatal-path direct writing and flush.
- Buffering, batching, or async behavior:
  - ordinary logging can be async;
  - `HardDiskExporter` buffers and flushes structured records asynchronously;
  - `LogManager` centralizes rolling and compression in a periodic background loop.
- Resource limits or scaling assumptions:
  - queue size, file-count limits, and max file size bound local resource use;
  - distributed-system scale increases log volume but not the number of local maintenance threads per process.

## Build, Test, And Verification

- Build entrypoints:
  - repository build flow via `build.sh`;
  - target-level builds for `common_log`, `failure_handler`, and `hard_disk_exporter`.
- Fast verification commands:
  - `bazel test //tests/ut/common/log:trace_test`
  - `bazel test //tests/ut/common/log:failure_handler_test`
  - `bazel test //tests/ut/common/log:hard_disk_exporter_test`
  - `bazel test //tests/ut/common/log:log_performance_test`
- Representative unit, integration, and system tests:
  - unit:
    - `//tests/ut/common/log:trace_test`
    - `//tests/ut/common/log:logging_test`
    - `//tests/ut/common/log:failure_handler_test`
    - `//tests/ut/common/log:hard_disk_exporter_test`
    - `//tests/ut/common/log:log_performance_test`
  - system or scenario style:
    - `//tests/st/common/log:kv_cache_log_performance_test`
    - `//tests/st/common/log:logging_free_test`
- Recommended validation for risky changes:
  - verify ordinary log emission, trace continuity across one async boundary, access-log shape, rolling or compression behavior, and crash-path output.

## Self-Verification Cases

| ID | Test scenario | Purpose | Preconditions | Input / steps | Expected result |
| --- | --- | --- | --- | --- | --- |
| `LOG-001` | client startup with env overrides | confirm client-only config override path | writable temp log directory and client entrypoint | set `DATASYSTEM_CLIENT_LOG_DIR` and `DATASYSTEM_CLIENT_LOG_NAME`, call `Logging::Start(..., true, ...)`, emit one `LOG(INFO)` | log files appear in overridden directory with validated filename |
| `LOG-002` | trace propagation across async boundary | confirm trace continuity | one async worker path that captures and restores trace ID | call `SetTraceUUID()`, capture `GetTraceID()`, restore with `SetTraceNewID()` in async task, emit logs on both threads | logs show the same root trace lineage and no leak to unrelated work |
| `LOG-003` | access recorder lifecycle | confirm structured performance output | `log_monitor` enabled and access exporter initialized | construct `AccessRecorder`, call `Record(...)`, trigger flush through `LogManager` or exporter | access or request-out file contains one pipe-delimited record with expected key and elapsed time |
| `LOG-004` | rolling and compression | confirm retention path | low `max_log_size`, compression enabled, writable log dir | generate enough logs to rotate, wait for `LogManager` loop | old files are rolled and optionally compressed without deleting active file unexpectedly |
| `LOG-005` | failure-signal output | confirm crash-path logging | failure handler installed and writable log dir | run `failure_handler_test` or invoke `FailureWriter` path in test harness | `container.log` is created and contains failure output or flushed logs |

## Common Change Scenarios

### Adding A New Capability

- Recommended extension point:
  - ordinary log behavior in `src/datasystem/common/log/logging.*` or `src/datasystem/common/log/spdlog/*`;
  - request-scoped observability in `src/datasystem/common/log/trace.*` or `access_recorder.*`.
- Required companion updates:
  - update `.repo_context/modules/infra/logging/design.md` and any affected subdocuments;
  - update tests under `tests/ut/common/log` or `tests/st/common/log` when behavior changes;
  - update metrics-area docs if `HardDiskExporter` behavior changes.
- Review checklist:
  - does it preserve trace semantics across async boundaries?
  - does it preserve file naming and access-log compatibility?
  - does it change degradation behavior or crash-path behavior?

### Modifying Existing Behavior

- Likely breakpoints:
  - startup ordering in `Logging::Start(...)`;
  - `AccessRecorderManager` initialization or routing;
  - `LogManager` filename patterns and pruning logic;
  - `TraceGuard` cleanup semantics.
- Compatibility checks:
  - file names, env vars, access-log field ordering, and access-point mappings.
- Observability checks:
  - ordinary logs, access logs, resource logs, `container.log`, and one async trace path.

### Debugging A Production Issue

- First files to inspect:
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/log/log_manager.cpp`
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/trace.cpp`
  - `src/datasystem/common/log/failure_handler.cpp`
- First runtime evidence to inspect:
  - resolved `FLAGS_log_dir` contents and permissions;
  - `log_monitor`, retention, and async queue config;
  - whether trace IDs remain consistent across caller and callee logs.
- Common misleading symptoms:
  - business logic succeeds while monitor logs disappear because `FLAGS_log_monitor` was disabled during init;
  - access logs look incomplete because one path skipped `Record()` rather than the exporter being entirely broken;
  - crash logs are missing because the handler was never installed, not because ordinary logging failed.

## Open Questions

- whether all runtime startup paths consistently install the failure signal handler;
- whether any downstream tooling depends on access-log field ordering beyond the code and tests already visible;
- whether mixed client and embedded-client deployments rely on additional undocumented log naming conventions.

## Pending Verification

- a complete list of production startup paths that call `InstallFailureSignalHandler(...)`;
- whether any performance-sensitive deployments override `log_async_queue_size` materially from the code defaults;
- whether any external runbooks or file collectors depend on `.gz` rotation timing beyond current code comments.

## Update Rules For This Document

- Keep design statements aligned with the real implementation and linked source files.
- When module boundaries, major flows, config semantics, external interfaces, or compatibility rules change, update this doc in the same task when practical.
- When DFX expectations change, update the Availability, Reliability, Resilience, Security, Privacy, Safety, and Observability sections in the same task when practical.
- If a section becomes speculative or stale, narrow it or move it to `Pending verification` until confirmed from source.
