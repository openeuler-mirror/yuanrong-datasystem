# Logging

## Scope

- Paths:
  - `src/datasystem/common/log`
  - `src/datasystem/common/log/spdlog`
- Why this module exists:
  - provide repository-wide structured logging, trace propagation, access/performance recording, failure-signal logging, and unified random log sampling;
  - centralize log file lifecycle management, client-side log configuration overrides, and per-request sampling decisions.
- Primary source files to verify against:
  - `src/datasystem/common/log/CMakeLists.txt`
  - `src/datasystem/common/log/log.h`
  - `src/datasystem/common/log/log_sampler.h`
  - `src/datasystem/common/log/log_sampler.cpp`
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

## Read Order Inside This Area

- If the question is about overall architecture, extension points, or feature design:
  - read `design.md`
- If the question is about trace ID generation, propagation, or scope cleanup:
  - read `trace-and-context.md`
- If the question is about access/performance logs, operation keys, or outbound request recording:
  - read `access-recorder.md`
- If the question is about `SLOW_LOG` request logs that must bypass request sampling:
  - read `slow-log-request-plan.md`
- If the question is about unified class-based random log sampling:
  - read `log-sampler-design.md`
- If the question is about request-stage access-log latency summaries:
  - read `request-stage-latency-trace-plan.md`
- If the question is about the implemented Get slow-log change or validation result:
  - read `plog-slow-get-implementation-summary.md`
- If the question is about startup, file rotation, compression, monitor flush, or crash log output:
  - read `log-lifecycle-and-rotation.md`

## Responsibilities Overview

- Verified:
  - `common_log` builds from `log_manager.cpp`, `logging.cpp`, `access_recorder.cpp`, `trace.cpp`, and `failure_handler.cpp`.
  - `log.h` provides the main logging macros used across the repository, including `LOG`, `VLOG`, `LOG_EVERY_N`, `LOG_FIRST_N`, and `CHECK`.
  - `Logging` is the main lifecycle singleton that initializes log directories, configures the spdlog-backed provider, starts background maintenance, and creates the access-recorder manager.
  - `LogManager` runs background work for log rolling, compression, and periodic monitor-log flush.
  - `Trace` is thread-local and is used pervasively to attach trace IDs to logs and cross-thread work.
- `AccessRecorder` records API/request performance and request/response metadata for client, access, and outbound-request style events.
- `LogSampler` provides unified random hash-threshold sampling across request/access/diagnostic dimensions, with per-request granularity and low-overhead hot-path decisions.
- `AccessRecorder::Object`, `AccessRecorder::Stream`, and `AccessRecorder::RequestOut` are the recommended access-log facades. Call sites describe fields and call `Record()`; sampling, lazy field materialization, elapsed time, formatting, and exporter writes are owned by `AccessRecorder`.
  - `FailureWriter` and `InstallFailureSignalHandler` integrate absl failure-signal handling with repository log files.
- Pending verification:
  - whether all runtime startup paths install the failure signal handler consistently;
  - whether any non-spdlog logging backends still exist in legacy paths.

## Subdocuments

- `design.md`
  - architecture, extension points, compatibility constraints, and implementation guardrails
- `trace-and-context.md`
  - thread-local trace model, trace ID generation, propagation, and cleanup behavior
- `access-recorder.md`
  - access/performance recorder lifecycle, operation-key mapping, and exporter coupling
- `slow-log-request-plan.md`
  - English `SLOW_LOG` request design, threshold source, macro rename target, and production `v=0` constraints
- `log-sampler-design.md`
  - production design for class-based random sampling coefficients and strict hot-path performance constraints
- `request-stage-latency-trace-plan.md`
  - request business-stage latency ticks, access-log summaries, slow-log gates, and compact proto phases; first scope is
    Get/Set/Create/Exist
- `plog-slow-get-implementation-summary.md`
  - implementation scope, changed slow Get segments, remote validation commands, and remaining validation blocker
- `log-lifecycle-and-rotation.md`
  - startup flow, file lifecycle, monitor flushing, crash logging, flags, and environment overrides

## Main Components

| Component | Verified role | Details live primarily in |
| --- | --- | --- |
| `log.h` | logging/check macro surface | this file, plus `log-lifecycle-and-rotation.md` |
| `Logging` | central lifecycle and configuration singleton | `log-lifecycle-and-rotation.md` |
| `LogManager` | background rolling/compression/flush manager | `log-lifecycle-and-rotation.md` |
| `Trace` | thread-local trace ID management | `trace-and-context.md` |
| `AccessRecorder` | structured performance/access logging and lazy sampled-out skip through Object/Stream/RequestOut facades | `access-recorder.md` and `log-sampler-design.md` |
| `AccessRecorderManager` | exporter owner for monitor/access logs | `access-recorder.md` and `log-lifecycle-and-rotation.md` |
| `LogSampler` | per-request random hash-threshold sampling | `log-sampler-design.md` |
| `failure_handler.*` | crash/backtrace logging | `log-lifecycle-and-rotation.md` |

## Cross-Module Coupling

- Logging depends on:
  - `common_flags`
  - `common_util`
  - `ds_spdlog`
  - `hard_disk_exporter`
  - absl failure/symbolization support
- Logging is consumed by:
  - public C/C++/Java/Python API wrappers via `Trace` and `AccessRecorder`
  - worker/master runtime code through `LOG`, `VLOG`, and resource/access logging
  - resource metrics flushing via `LogManager::DoLogMonitorWrite()`

## Review And Bugfix Notes

- Common change risks:
  - changing `Trace` behavior can silently affect observability across many unrelated modules;
  - changing `AccessRecorder` formatting or key mapping affects monitor/access logs and may break downstream parsing assumptions;
  - changing log rotation/compression rules affects both ordinary logs and monitor/resource logs.
- Good first files when observability looks wrong:
  - `logging.cpp`
  - `log_manager.cpp`
  - `access_recorder.cpp`
  - `trace.cpp`

## Current Split Layout

- start with this file for ownership and routing;
- go to `design.md` before designing a new logging feature or refactor;
- go to `trace-and-context.md`, `access-recorder.md`, or `log-lifecycle-and-rotation.md` for behavior-level details.
