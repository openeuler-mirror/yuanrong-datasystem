# Metrics Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - `current implementation | mixed`
- Primary code paths:
  - `src/datasystem/common/metrics/*`
  - `src/datasystem/common/metrics/hard_disk_exporter/*`
  - `src/datasystem/common/metrics/metrics_vector/*`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Primary source-of-truth files:
  - `src/datasystem/common/metrics/CMakeLists.txt`
  - `src/datasystem/common/metrics/BUILD.bazel`
  - `src/datasystem/common/metrics/metrics.h`
  - `src/datasystem/common/metrics/metrics.cpp`
  - `src/datasystem/common/metrics/metrics_exporter.h`
  - `src/datasystem/common/metrics/metrics_exporter.cpp`
  - `src/datasystem/common/metrics/res_metric_collector.h`
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/CMakeLists.txt`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
  - `src/datasystem/common/metrics/res_metrics.def`
  - `src/datasystem/common/metrics/metrics_description.def`
  - `src/datasystem/common/metrics/res_metric_name.h`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `tests/ut/common/log/BUILD.bazel`
  - `tests/ut/worker/BUILD.bazel`
  - `tests/st/client/kv_cache/BUILD.bazel`
  - `tests/st/client/stream_cache/BUILD.bazel`
  - `bazel/ds_deps.bzl`
- Last verified against source:
  - `2026-04-16`
- Related context docs:
  - `.repo_context/modules/infra/metrics/README.md`
  - `.repo_context/modules/infra/metrics/resource-collector.md`
  - `.repo_context/modules/infra/metrics/exporters-and-buffering.md`
  - `.repo_context/modules/infra/metrics/metric-families-and-registration.md`
  - `.repo_context/modules/infra/logging/design.md`
- Related user-facing or internal docs:
  - none linked yet

## Purpose

- Why this design document exists:
  - provide a source-backed architecture reference for adding, reviewing, or refactoring resource-monitor metrics and exporter behavior.
- What problem this module solves:
  - collect periodic resource and health-style metrics from runtime components, buffer them asynchronously, and persist them into monitor-style log files.
- Who or what depends on this module:
  - worker and master runtime registration points, monitor-log consumers, and logging-side access records that reuse `HardDiskExporter`.

## Business And Scenario Overview

- Why this capability is needed:
  - this repository is a distributed system with multiple runtime roles and background threads, so operators need periodic resource visibility without putting synchronous metrics I/O on request paths.
- Target users, callers, or operators:
  - worker and master developers registering metrics, client and runtime test authors, and operators reading resource-monitor files.
- Typical usage time or trigger conditions:
  - runtime startup registers handlers, collector initialization occurs when monitor logging is enabled, and periodic sampling runs throughout process lifetime.
- Typical deployment or runtime environment:
  - long-lived worker or master processes writing resource-monitor files under the configured log directory.
- How the feature is expected to be used:
  - runtime code registers one handler per metric family, `ResMetricCollector` samples on an interval, and exporter infrastructure flushes the collected string payloads to disk asynchronously.
- What other functions or modules it may affect:
  - logging and access-log behavior through shared exporter code, downstream parsing of monitor files, disk usage, and operational DFX analysis.
- Expected user or operator experience:
  - performance:
    - metric collection should stay periodic and bounded rather than blocking hot request paths.
  - security:
    - new metric payloads should not expose sensitive request or credential data.
  - resilience:
    - one missing handler should degrade one column or family rather than crash the process.
  - reliability:
    - metric family ordering and semantic meaning should remain stable across releases.
  - availability:
    - business logic should continue even if monitor output is blank, stale, or partially degraded.
  - privacy:
    - string payloads emitted by handlers should avoid carrying user-sensitive content.

## Goals

- provide one shared collector abstraction for periodic resource metrics;
- keep metric definition, registration, collection, and persistence responsibilities separated;
- keep metric emission asynchronous and bounded rather than synchronous on producer paths;
- reuse exporter infrastructure across resource metrics and logging-side access-monitor outputs;
- preserve stable family ordering and readable descriptions for operational consumers.

## Non-Goals

- owning business-specific metric semantics outside registered handlers;
- acting as a multi-backend metrics platform today;
- replacing ordinary repository logging or external dashboarding systems;
- auto-discovering metric handlers without explicit runtime registration.

## Scope

- In scope:
  - buffered exporter base and flush thread lifecycle;
  - `HardDiskExporter` file persistence and rotation logic;
  - `ResMetricCollector` singleton lifecycle and periodic sampling;
  - metric family definitions in `res_metrics.def` and descriptions in `metrics_description.def`;
  - major runtime registration points currently centered in `worker_oc_server.cpp`.
- Out of scope:
  - alerting, dashboard rendering, or external backend integration;
  - feature-specific business logic that only contributes one metric handler;
  - ordinary logging lifecycle outside exporter coupling;
  - typed metrics protocols such as Prometheus exposition in the current implementation.
- Important boundaries with neighboring modules:
  - `metrics` owns collection, buffering, family-definition infrastructure, and the concrete hard-disk exporter used for monitor outputs;
  - runtime modules own the live handler lambdas they register;
  - `logging` owns ordinary log lifecycle while sharing `HardDiskExporter` for access-monitor outputs.

## Terminology

| Term | Meaning in this repository | Source or note |
| --- | --- | --- |
| `ResMetricCollector` | singleton periodic collector for resource metrics | `src/datasystem/common/metrics/res_metric_collector.h/.cpp` |
| `RegisterCollectHandler` | explicit registration hook for a metric family | `src/datasystem/common/metrics/res_metric_collector.cpp` |
| `MetricsExporter` | abstract buffered exporter base with one flush thread | `src/datasystem/common/metrics/metrics_exporter.h/.cpp` |
| `HardDiskExporter` | concrete exporter writing monitor-style output to files | `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h/.cpp` |
| `ResMetricName` | enum-like family identifier set generated from `res_metrics.def` | `src/datasystem/common/metrics/res_metric_name.h` |
| `metrics_description.def` | description and unit definitions for metric families | `src/datasystem/common/metrics/metrics_description.def` |

## Current State And Design Choice Notes

- Current implementation or baseline behavior:
  - metrics are collected as string payloads, not typed numeric objects, and persisted through a single concrete exporter backend named `"harddisk"`.
  - `metrics.h/.cpp` also provide a release-scoped lightweight typed API with fixed-descriptor `Counter`, `Gauge`, `Histogram`, `ScopedTimer`, and one multi-line `LOG(INFO)` summary block per monitor interval.
  - the lightweight typed metrics API does not own a background thread; worker main and client runtime code drive periodic `Tick()`/`PrintSummary()` calls, mirroring the `PerfManager` ownership style.
  - request-path latency for `set/get` style APIs is mostly surfaced through the logging/access-recorder path rather than through typed metrics families.
- Relevant constraints from current release or deployment:
  - collector startup is gated by `log_monitor` and exporter initialization;
  - `HardDiskExporter` is shared with logging-side access records, so exporter changes are cross-cutting;
  - family ordering in `res_metrics.def` is explicitly marked as order-sensitive.
- Similar upstream, industry, or historical approaches considered:
  - instead of a standalone metrics service or protocol exporter, the current repository stores monitor output in local files aligned with existing logging flows.
- Selected approach and why:
  - the code favors a small repository-owned metrics surface with explicit registration and asynchronous flushing, which keeps runtime coupling visible and reduces hidden magic.
- Known tradeoffs:
  - string-based handlers are simple to wire but easier to misuse semantically;
  - local file persistence is operationally simple but does not provide replicated availability by itself;
  - exporter sharing reduces duplication but couples metrics changes to logging behavior.
  - operators cannot directly query request-path percentile series such as `set/get p95` from the current common-metrics surface.

## Current Request-Path Metrics Gap

- Verified current repository behavior:
  - `src/datasystem/common/metrics/res_metric_collector.cpp` samples periodic resource families;
  - `src/datasystem/common/log/access_recorder.cpp` records per-operation elapsed time into access/performance logs;
  - no common `src/datasystem` HTTP scrape endpoint or Prometheus exposition path is currently part of this module.
- What this means for `set/get` analysis:
  - request latency distribution is visible only indirectly through log samples, test output, or ad hoc analysis of recorded access lines;
  - the metrics module is stronger for resource trend diagnosis than for instant request percentile diagnosis.
- If future work needs richer online diagnosis:
  - prefer adding typed counters and histograms for hot request families before adding more file-only monitor strings;
  - keep request metrics separate from resource metrics so percentile and throughput questions do not depend on monitor-file parsing.

## Architecture Overview

- High-level structure:
  - runtime code registers one handler per metric family;
  - `ResMetricCollector` owns periodic sampling and assembles one line of metric output;
  - `MetricsExporter` owns bounded buffering and a flush thread;
  - `HardDiskExporter` persists the buffered output to files with rolling and pruning behavior.
- Key runtime roles or processes:
  - runtime modules registering handlers during startup;
  - a collector thread sampling on `log_monitor_interval_ms`;
  - a flush thread draining exporter buffers to disk.
- Key persistent state, if any:
  - `resource.log` style monitor files under `FLAGS_log_dir`;
  - rotated metric log files managed by `HardDiskExporter`.
- Key control-flow or data-flow stages:
  - define metric families and descriptions;
  - register handlers;
  - initialize exporter when monitor logging is enabled;
  - periodically collect family payloads;
  - buffer, flush, rotate, and prune files.

## Scenario Analysis

- Primary user or system scenarios:
  - worker startup registers handlers for memory, disk, thread-pool, and backend success-rate metrics;
  - the collector periodically samples all registered families and emits one structured monitor line;
  - shared exporter code flushes both resource metrics and access-monitor output;
  - an operator diagnoses blank columns caused by missing handler registration rather than complete collector failure.
- Key interactions between actors and services:
  - runtime modules interact with `ResMetricCollector::RegisterCollectHandler(...)`;
  - `ResMetricCollector` interacts with `MetricsExporter` and `HardDiskExporter`;
  - `HardDiskExporter` interacts with filesystem utilities and logging prefix formatting;
  - monitor-file readers depend on stable family order and file naming.
- Normal path summary:
  - metrics families are defined and described, handlers are registered during runtime startup, collector samples at a configured interval, and exporter threads persist the results to disk.
- Exceptional path summary:
  - unsupported exporter selection prevents initialization, missing handlers yield blank columns with one warning, and file or exporter failures degrade monitor visibility without necessarily stopping service logic.

## Entry Points, External Interfaces, And Integration Points

- Public APIs:
  - `ResMetricCollector::Instance()`
  - `ResMetricCollector::Init()`
  - `ResMetricCollector::Start()`
  - `ResMetricCollector::RegisterCollectHandler(...)`
  - `MetricsExporter::SubmitWriteMessage()`
  - `HardDiskExporter::Init(const std::string &filePath)`
  - `HardDiskExporter::Send(const std::string &, Uri &, int)`
- Internal service entrypoints:
  - `ResMetricCollector::CollectMetrics()`
  - `MetricsExporter::Init()`
  - `MetricsExporter::WriteMessage(...)`
  - `HardDiskExporter::FlushThread()`
- External protocols, schemas, or data formats:
  - monitor output is line-based text emitted by `HardDiskExporter::Send(...)` with the standard repository log prefix plus a pipe-delimited metric payload;
  - metric family ordering is derived from `res_metrics.def`;
  - family descriptions and units are sourced from `metrics_description.def`.
- CLI commands, config flags, or environment variables:
  - `log_monitor`
  - `log_monitor_exporter`
  - `log_monitor_interval_ms`
  - `log_dir`
  - `logfile_mode`
  - `max_log_size`
  - `max_log_file_num`
- Background jobs, threads, or callbacks:
  - collector thread named `CollectMetrics`;
  - exporter flush thread named `MetricsFlush`;
  - runtime registration callbacks expressed as `std::function<std::string()>`.
- Cross-module integration points:
  - `worker_oc_server.cpp` is the major current registration point;
  - `HardDiskExporter` is shared with `logging/access_recorder`;
  - exporter flush threads restore trace context with `Trace::SetTraceNewID(...)`.
- Upstream and downstream dependencies:
  - upstream runtime modules must register handlers before useful collection occurs;
  - downstream dependencies include filesystem access, shared logging utilities, and any tooling that parses resource monitor files.
- Backward-compatibility expectations for external consumers:
  - family order, family names, description meaning, file naming, and monitor file layout should be treated as operational contracts.

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| `MetricsExporter` | bounded buffering and flush-thread lifecycle | `src/datasystem/common/metrics/metrics_exporter.h/.cpp` | abstract base |
| `HardDiskExporter` | concrete file writer, rolling, pruning, and prefix formatting | `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h/.cpp` | shared with logging-side access output |
| `ResMetricCollector` | periodic sampling and exporter submission | `src/datasystem/common/metrics/res_metric_collector.h/.cpp` | singleton |
| `datasystem::metrics` typed API | fixed-id Counter/Gauge/Histogram updates and periodic `LOG(INFO)` summaries | `src/datasystem/common/metrics/metrics.h/.cpp` | no exporter, no buckets, no dynamic labels |
| `ResMetricName` families | stable family identity and iteration order | `src/datasystem/common/metrics/res_metric_name.h`, `src/datasystem/common/metrics/res_metrics.def` | order-sensitive |
| `metrics_description.def` | family descriptions and units | `src/datasystem/common/metrics/metrics_description.def` | human-readable semantic contract |
| runtime registration code | binds families to live handler lambdas | `src/datasystem/worker/worker_oc_server.cpp` | major current site |

## Main Flows

### Register And Collect Metrics

1. Runtime startup registers handler lambdas through `RegisterCollectHandler(...)`.
2. `ResMetricCollector::Init()` selects and initializes the exporter when monitor logging is enabled.
3. `ResMetricCollector::Start()` launches periodic sampling.
4. `CollectMetrics()` iterates families from `SHARED_MEMORY` through `RES_METRICS_END - 1`, invokes handlers, and assembles one line of output.

Key files:

- `src/datasystem/common/metrics/res_metric_collector.cpp`
- `src/datasystem/worker/worker_oc_server.cpp`
- `src/datasystem/common/metrics/res_metrics.def`

Failure-sensitive steps:

- unsupported `log_monitor_exporter` values make initialization fail;
- missing handler registration leaves blank output columns with only a warning;
- family-definition and registration drift can create syntactically valid but semantically wrong output.

### Buffer And Persist Metric Output

1. Collector or another producer calls `Send(...)` on the exporter.
2. `MetricsExporter::WriteMessage(...)` appends to the active buffer.
3. `SubmitWriteMessage()` or the size threshold moves the active buffer into the queue.
4. The flush thread writes queued data to disk and rotates files when needed.

Key files:

- `src/datasystem/common/metrics/metrics_exporter.cpp`
- `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`

Failure-sensitive steps:

- queueing or flush-threshold changes alter latency and batch size;
- file creation or write failures affect monitor visibility;
- exporter behavior changes also affect logging-side access output.

### Define Or Extend Metric Families

1. Add or modify a family entry in `res_metrics.def`.
2. Align human meaning and units in `metrics_description.def`.
3. Register a handler in runtime startup code.
4. Verify the collector and exporter expose the new family in monitor files.

Key files:

- `src/datasystem/common/metrics/res_metrics.def`
- `src/datasystem/common/metrics/metrics_description.def`
- `src/datasystem/worker/worker_oc_server.cpp`

Failure-sensitive steps:

- reordering families can corrupt the meaning of existing columns;
- adding definitions without registration creates misleading blanks;
- updating descriptions without matching runtime behavior can mislead operators.

## Data And State Model

- Important in-memory state:
  - handler map in `ResMetricCollector`;
  - `msInterval_`, `exitFlag_`, and collector thread lifecycle;
  - active buffer, queued buffers, and flush thread state in `MetricsExporter`;
  - file descriptor and rolling state in `HardDiskExporter`.
- Important on-disk or external state:
  - resource-monitor log files in the logging directory;
  - rotated metric log files governed by file size and retention count;
  - gflags controlling collector and exporter behavior.
- Ownership and lifecycle rules:
  - runtime modules own handler logic;
  - `ResMetricCollector` owns periodic sampling and exporter submission;
  - `MetricsExporter` owns flush-thread lifecycle;
  - `HardDiskExporter` owns one file handle and rolling behavior for its output path.
- Concurrency or thread-affinity rules:
  - registration typically happens during startup before long-lived collection;
  - collection and flush happen asynchronously on dedicated threads;
  - trace context is explicitly reattached in exporter flush threads.
- Ordering requirements or invariants:
  - exporter must initialize before useful registration or collection occurs;
  - family definitions, descriptions, and registrations must move together;
  - enum order in `res_metrics.def` must remain stable.

## External Interaction And Dependency Analysis

- Dependency graph summary:
  - runtime modules provide handler lambdas;
  - collector depends on flags, logging utilities, and the concrete exporter;
  - exporter depends on filesystem utilities and shared logging prefix formatting;
  - operational tooling depends on the emitted file layout and family order.
- Critical upstream services or modules:
  - `worker_oc_server.cpp` registration logic;
  - any future runtime startup path that calls `ResMetricCollector::Init()` and `Start()`.
- Critical downstream services or modules:
  - local filesystem under `FLAGS_log_dir`;
  - `logging` module through shared `HardDiskExporter`;
  - monitor-file readers and DFX workflows.
- Failure impact from each critical dependency:
  - runtime omission of registration leads to blank metrics;
  - filesystem issues prevent monitor persistence;
  - shared exporter regressions affect both metrics and access-monitor logs;
  - misconfigured flags can silently reduce observability while business logic continues.
- Version, protocol, or schema coupling:
  - `res_metrics.def` order and `metrics_description.def` semantics form a schema-like contract for monitor output;
  - file naming and line layout couple this module to log readers.
- Deployment or operations dependencies:
  - writable log directory;
  - adequate disk headroom and retention settings;
  - correct `log_monitor_exporter` selection, currently only `"harddisk"`.

## Open Source Software Selection And Dependency Record

| Dependency | Purpose in this module | Why selected | Alternatives considered | License | Version / upgrade strategy | Security / maintenance notes |
| --- | --- | --- | --- | --- | --- | --- |
| no metrics-local third-party dependency beyond shared repo infra | current metrics code uses repository utilities plus shared logging/exporter infrastructure rather than introducing its own external SDK | keeps the metrics surface small and avoids splitting operational behavior across multiple external backends | introduce a separate metrics SDK or backend library | n/a | current implementation should document when a direct metrics-local OSS dependency is introduced in the future | exporter behavior still inherits risk from shared logging infrastructure such as `spdlog` through `common_log` |

## Configuration Model

| Config | Type | Default or source | Effect | Risk if changed |
| --- | --- | --- | --- | --- |
| `log_monitor` | gflag | shared observability flag, runtime default in common flags | enables collector and exporter path | can disable all monitor output while runtime logic keeps working |
| `log_monitor_exporter` | gflag | current supported value `"harddisk"` | selects exporter backend | unsupported values fail initialization |
| `log_monitor_interval_ms` | gflag | defined in `res_metric_collector.cpp`, default `10000` | controls sampling cadence | changes load, freshness, and output volume |
| `log_dir` | gflag | shared logging config | controls output directory | bad path or permissions break persistence |
| `logfile_mode` | gflag | shared logging config | controls file permissions for hard-disk exporter | bad permissions may affect operators or file access |
| `max_log_size` | gflag | shared logging config | rotation threshold for resource log files | changes file churn and size |
| `max_log_file_num` | gflag | shared logging config | max retained rotated files | may prune too aggressively or allow disk growth |

## Examples And Migration Notes

- Example API requests or responses to retain:
  - `ResMetricCollector::Instance().RegisterCollectHandler(ResMetricName::SHARED_MEMORY, handler)` is the expected pattern for binding one family to one runtime callback.
- Example configuration or deployment snippets to retain:
  - enabling monitor collection requires `log_monitor=true` and a supported `log_monitor_exporter`, currently `"harddisk"`.
- Example operator workflows or commands to retain:
  - inspect `resource*.log` output under the configured log directory to confirm families appear in the expected order.
- Upgrade, rollout, or migration examples if behavior changes:
  - when adding or changing a family, update `res_metrics.def`, `metrics_description.def`, registration callsites, and the related `.repo_context/modules/infra/metrics/*.md` docs in the same task.

## Availability

- Availability goals or service-level expectations:
  - metrics are intended to improve diagnosis and DFX visibility without being a hard availability dependency for most business logic.
- Deployment topology and redundancy model:
  - this is a per-process local collector and file exporter with no built-in replicated backend.
- Single-point-of-failure analysis:
  - one collector singleton and one exporter backend per process are the main local control points;
  - `HardDiskExporter` is also a shared dependency for access-monitor logging.
- Failure domains and blast-radius limits:
  - one missing handler degrades a specific family more than the entire collector;
  - exporter failures can affect all monitor output for the process;
  - business paths often continue despite collector or exporter failure.
- Health checks, readiness, and traffic removal conditions:
  - there is no dedicated readiness endpoint inside this module;
  - health is inferred from successful initialization, periodic file updates, and non-empty expected columns.
- Failover, switchover, or recovery entry conditions:
  - recovery is local and process-scoped: correct config, restore filesystem access, or restart the process rather than failing over to another exporter instance.
- Capacity reservation or headroom assumptions:
  - low collection intervals and high output volume require disk and buffer headroom;
  - retention settings are the main local capacity controls.

## Reliability

- Expected failure modes:
  - unsupported exporter selection;
  - exporter initialization failure;
  - missing handler registration;
  - file write or rotation issues;
  - semantic drift between definitions, descriptions, and registrations.
- Data consistency, durability, or correctness requirements:
  - family ordering and semantic mapping must stay stable;
  - one handler must not accidentally populate the wrong family;
  - written monitor lines should preserve line integrity once flushed.
- Retry, idempotency, deduplication, or exactly-once assumptions:
  - the module does not provide exactly-once delivery semantics for monitor lines;
  - reliability comes from periodic sampling and bounded asynchronous flush, not deduplicated transport.
- Partial-degradation behavior:
  - one family can degrade independently because missing handlers emit blank fields plus a warning;
  - exporter or filesystem failures can remove monitor visibility while business logic keeps running.
- Recovery, replay, repair, or reconciliation strategy:
  - fix handler registration, config, or filesystem issues and restart or reinitialize the process;
  - verify file output and family completeness after recovery.
- RTO, RPO, MTTR, or equivalent recovery targets:
  - no explicit numeric recovery targets are encoded in current code or docs.
- What must remain true after failure:
  - family order and meaning must not shift silently;
  - exporter failures should not corrupt unrelated ordinary logging;
  - one misconfigured handler should not redefine neighboring columns.

## Resilience

- Overload protection, rate limiting, or admission control:
  - exporter buffering and interval-driven collection provide limited smoothing rather than explicit admission control.
- Timeout, circuit-breaker, and backpressure strategy:
  - no explicit circuit breaker is implemented;
  - backpressure is implicit through bounded buffer pool size and flush-thread pacing.
- Dependency failure handling:
  - invalid exporter type returns `K_INVALID` during initialization;
  - missing handlers trigger one warning and blank fields rather than process failure;
  - flush thread drains remaining buffers during shutdown.
- Graceful-degradation behavior:
  - the module prefers blank or stale metrics over taking down the runtime;
  - business services can often continue when monitor output is impaired.
- Safe rollback, feature-gating, or kill-switch strategy:
  - `log_monitor` is the primary feature gate;
  - collection cadence and backend choice are controlled by gflags.
- Attack or fault scenarios the module should continue to tolerate:
  - missing registration for one family;
  - transient file rotation pressure;
  - exporter queue pressure from bursty monitor output, within configured local limits.

## Security, Privacy, And Safety Constraints

- Authn/authz expectations:
  - this module is not the policy boundary for deciding which operations are allowed.
- Input validation requirements:
  - handlers return strings and should be treated as runtime-produced data that still needs review for schema correctness and safe content.
- Data sensitivity or privacy requirements:
  - metrics should not embed object keys, credentials, tokens, or detailed request payloads into monitor files unless explicitly reviewed.
- Secrets, credentials, or key-management requirements:
  - no credential handling belongs here; new metrics must avoid surfacing secrets derived from other modules.
- Resource or isolation constraints:
  - collection cadence, flush buffering, and disk retention must stay within acceptable CPU, memory, and disk budgets.
- Unsafe changes to avoid:
  - reordering metric families;
  - changing shared exporter behavior without reviewing logging-side coupling;
  - introducing sensitive payloads into string-based metrics.
- Safety constraints for human, environment, or property impact:
  - none directly safety-critical are encoded here, but runaway monitor output can still harm service stability through disk exhaustion.

## Observability

- Main logs:
  - resource monitor files emitted through `HardDiskExporter`;
  - warning logs for missing handlers or invalid exporter config.
- Metrics:
  - this module is the repository path that emits periodic resource-monitor metrics.
- Traces or correlation fields:
  - exporter flush threads restore trace IDs via `Trace::SetTraceNewID(...)`.
- Alerts or SLO-related signals:
  - no built-in alert definitions are encoded here;
  - operators should watch for missing files, blank columns, or repeated invalid-exporter warnings.
- Debug hooks or inspection commands:
  - inspect handler registration in `worker_oc_server.cpp`;
  - inspect collector behavior in `res_metric_collector.cpp`;
  - inspect exporter queues and file logic in `metrics_exporter.cpp` and `hard_disk_exporter.cpp`.
- How to tell the module is healthy:
  - `ResMetricCollector::Init()` succeeds;
  - resource files appear under the expected log directory;
  - expected families show non-empty values where registration exists;
  - no repeated warnings indicate unsupported exporter config or persistent missing handlers.

## Compatibility And Invariants

- External compatibility constraints:
  - monitor-file readers may depend on stable family ordering, field meanings, and file naming.
- Internal invariants:
  - metrics are explicitly registered, not auto-discovered;
  - collector iteration order follows `res_metrics.def`;
  - `HardDiskExporter` remains the current shared concrete backend.
- File format, wire format, or schema stability notes:
  - `res_metrics.def` and `metrics_description.def` together form a schema-like contract for resource monitor output;
  - output line layout is compatibility-sensitive for downstream consumers.
- Ordering or naming stability notes:
  - `res_metrics.def` explicitly warns not to change order;
  - rotated file naming must stay aligned with pruning logic.
- Upgrade, downgrade, or mixed-version constraints:
  - no explicit wire compatibility layer exists, so changing family order or semantics can create rollout-time interpretability issues between old and new processes.

## Performance Characteristics

- Hot paths:
  - frequently collected handler lambdas;
  - exporter buffer enqueue and queue drain;
  - disk flush and rolling.
- Known expensive operations:
  - low collection intervals that increase scan frequency;
  - file flush and rotation;
  - very large string payload generation inside handlers.
- Buffering, batching, or async behavior:
  - producer messages are buffered in memory;
  - flush is asynchronous in exporter threads;
  - collection cadence is interval-driven, not event-driven.
- Resource limits or scaling assumptions:
  - per-family handler cost is expected to stay bounded;
  - buffer pool size and file retention are the primary local resource controls.

## Build, Test, And Verification

- Build entrypoints:
  - repository build flow via `build.sh`;
  - target-level builds for `metrics_exporter_base`, `hard_disk_exporter`, and `common_metrics`.
- Fast verification commands:
  - `bazel test //tests/ut/common/log:hard_disk_exporter_test`
  - `bazel test //tests/ut/worker:stream_usagemonitor_test`
  - `bazel test //tests/st/client/stream_cache:sc_metrics_test --test_tag_filters=manual`
  - `bazel test //tests/st/client/kv_cache:kv_client_log_monitor_test --test_tag_filters=manual`
- Representative unit, integration, and system tests:
  - unit:
    - `//tests/ut/common/log:hard_disk_exporter_test`
    - `//tests/ut/worker:stream_usagemonitor_test`
  - system or scenario style:
    - `//tests/st/client/stream_cache:sc_metrics_test`
    - `//tests/st/client/kv_cache:kv_client_log_monitor_test`
- Recommended validation for risky changes:
  - verify family order, one representative handler registration path, resource-file creation, exporter rotation behavior, and logging-side access output if `HardDiskExporter` changed.

## Self-Verification Cases

| ID | Test scenario | Purpose | Preconditions | Input / steps | Expected result |
| --- | --- | --- | --- | --- | --- |
| `MET-001` | collector init with supported exporter | confirm normal startup path | `log_monitor=true`, writable log dir, `log_monitor_exporter=harddisk` | call `ResMetricCollector::Init()` then `Start()` | collector starts and resource output file is created |
| `MET-002` | missing handler registration | confirm graceful degradation | collector initialized, one family intentionally not registered | let one collection cycle run | output contains a blank field for that family and one warning is logged |
| `MET-003` | family extension flow | confirm definition/description/registration alignment | choose one new or modified family | update `res_metrics.def`, `metrics_description.def`, register handler, run collector | new family appears in expected order with meaningful payload |
| `MET-004` | exporter rolling and pruning | confirm file lifecycle | low `max_log_size`, finite `max_log_file_num`, writable log dir | generate enough output through exporter flush | rotated files are created and oldest files are pruned per config |
| `MET-005` | shared exporter coupling | confirm metrics change does not regress access-monitor output | logging-side access recorder path available | exercise resource metrics and access logging after exporter change | both `resource` and access-monitor style outputs remain healthy |

## Common Change Scenarios

### Adding A New Capability

- Recommended extension point:
  - new resource families through `res_metrics.def`, `metrics_description.def`, and runtime registration;
  - exporter behavior through `metrics_exporter.*` or `hard_disk_exporter.*`.
- Required companion updates:
  - update `.repo_context/modules/infra/metrics/design.md` and affected subdocuments;
  - update tests or add coverage when family definitions or exporter behavior change;
  - review logging docs if `HardDiskExporter` behavior changes.
- Review checklist:
  - does the change preserve family order and meaning?
  - does it preserve shared exporter behavior for logging-side access output?
  - does it degrade gracefully when registration is incomplete?

### Modifying Existing Behavior

- Likely breakpoints:
  - exporter selection and initialization in `res_metric_collector.cpp`;
  - flush and queue behavior in `metrics_exporter.cpp`;
  - rolling and pruning behavior in `hard_disk_exporter.cpp`;
  - family definitions and registration alignment.
- Compatibility checks:
  - family order, description meaning, file names, and monitor line layout.
- Observability checks:
  - resource files, blank-column behavior, exporter warnings, and logging-side access output if the exporter changed.

### Debugging A Production Issue

- First files to inspect:
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
  - `src/datasystem/common/metrics/metrics_exporter.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/common/metrics/res_metrics.def`
- First runtime evidence to inspect:
  - `log_monitor`, `log_monitor_exporter`, and `log_monitor_interval_ms`;
  - `resource` output files and rotation results;
  - whether expected families are blank, missing, or semantically shifted.
- Common misleading symptoms:
  - blank columns may mean missing registration rather than exporter failure;
  - missing files may be caused by `log_monitor=false` or bad directory permissions rather than collector-loop bugs;
  - a metrics exporter regression can surface first as logging-side access-output problems because the exporter is shared.

## Open Questions

- whether there are additional major registration sites outside `worker_oc_server.cpp` that deserve dedicated context notes;
- whether a non-harddisk exporter backend is planned but not yet implemented;
- whether any downstream tooling relies on current family order beyond visible code and tests.

## Pending Verification

- a complete inventory of all production callsites that initialize and start `ResMetricCollector`;
- any deployment-specific expectations around `resource.log` retention and archival;
- whether any metrics_vector helpers deserve their own focused context note after future changes.

## Update Rules For This Document

- Keep design statements aligned with the real implementation and linked source files.
- When module boundaries, major flows, config semantics, external interfaces, or compatibility rules change, update this doc in the same task when practical.
- When DFX expectations change, update the Availability, Reliability, Resilience, Security, Privacy, Safety, and Observability sections in the same task when practical.
- If a section becomes speculative or stale, narrow it or move it to `Pending verification` until confirmed from source.
