# Metrics Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - current implementation | mixed
- Primary code paths:
  - `src/datasystem/common/metrics/*`
  - `src/datasystem/common/metrics/hard_disk_exporter/*`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Primary source-of-truth files:
  - `src/datasystem/common/metrics/CMakeLists.txt`
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
- Last verified against source:
  - `2026-04-13`
- Related context docs:
  - `README.md`
  - `resource-collector.md`
  - `exporters-and-buffering.md`
  - `metric-families-and-registration.md`
  - `../logging/README.md`
- Related user-facing or internal docs:
  - none linked yet

## Purpose

- Why this design document exists:
  - provide an architecture-level reference for adding or reviewing resource metrics and exporter behavior.
- What problem this module solves:
  - collect periodic resource and service-health style metrics, buffer them asynchronously, and flush them into monitor logs.
- Who or what depends on this module:
  - worker/master runtime registration points, monitor logging, and access-record export through the shared hard-disk exporter.

## Goals

- provide one shared collector abstraction for resource metrics;
- separate metric definition, registration, collection, and persistence concerns;
- keep metric emission asynchronous and bounded rather than synchronous on producer paths;
- reuse exporter infrastructure across monitor and access-log style outputs.

## Non-Goals

- owning business-specific metric semantics outside registered handlers;
- acting as a general metrics backend with multiple concrete exporters today;
- replacing ordinary repository logging.

## Scope

- In scope:
  - buffered exporter framework;
  - hard-disk exporter implementation;
  - periodic resource collector;
  - metric family definitions and descriptions;
  - runtime registration points currently centered in `worker_oc_server.cpp`.
- Out of scope:
  - alerting, dashboarding, or external backend integration logic;
  - business features that merely expose one handler into the collector;
  - generic logging lifecycle logic outside exporter coupling.
- Important boundaries with neighboring modules:
  - `metrics` owns collection, exporter buffering, and family-definition infrastructure;
  - runtime code owns registration of concrete handler lambdas;
  - `logging` owns ordinary log lifecycle while sharing `HardDiskExporter` for access-monitor outputs.

## Terminology

| Term | Meaning in this repository | Source or note |
| --- | --- | --- |
| `ResMetricCollector` | singleton periodic collector | `res_metric_collector.h/.cpp` |
| `RegisterCollectHandler` | explicit registration hook for one metric family | `res_metric_collector.cpp` |
| `MetricsExporter` | abstract async buffered exporter base | `metrics_exporter.h/.cpp` |
| `HardDiskExporter` | current concrete exporter implementation | `hard_disk_exporter.h/.cpp` |
| `ResMetricName` | enum-like metric family identifier set | `res_metric_name.h`, `res_metrics.def` |
| `metrics_description.def` | descriptions and units for metric families | `metrics_description.def` |

## Architecture Overview

- High-level structure:
  - runtime code registers one collector handler per metric family;
  - `ResMetricCollector` periodically walks registered families and assembles string payloads;
  - buffered exporter infrastructure batches writes and flushes asynchronously;
  - `HardDiskExporter` persists output to files with rotation support.
- Key runtime roles or processes:
  - producer/runtime code that supplies lambdas through `RegisterCollectHandler`;
  - collector thread or loop that periodically scans metric families;
  - exporter flush thread that persists queued batches.
- Key persistent state, if any:
  - monitor/resource metric files on disk;
  - rotated metric files governed by size and retention knobs.
- Key control-flow or data-flow stages:
  - metric family definition;
  - runtime handler registration;
  - periodic collection;
  - buffered exporter write/submit;
  - flush and rotation.

## Entry Points And Integration Points

- Public APIs:
  - `ResMetricCollector::RegisterCollectHandler(...)`
  - `MetricsExporter::WriteMessage(...)`
  - `MetricsExporter::SubmitWriteMessage()`
- Internal service entrypoints:
  - `ResMetricCollector::Instance()`
  - `HardDiskExporter::Init(filePath)`
- Background jobs / threads / callbacks:
  - collector periodic loop in `ResMetricCollector`
  - exporter flush thread in `MetricsExporter`
  - concrete disk flush loop in `HardDiskExporter::FlushThread()`
- Config flags or environment variables:
  - `log_monitor`
  - `log_monitor_exporter`
  - `log_monitor_interval_ms`
  - `log_dir`
  - `logfile_mode`
  - `max_log_size`
  - `max_log_file_num`
- Cross-module integration points:
  - `worker_oc_server.cpp` registers many current handlers;
  - `logging/access_recorder` uses `HardDiskExporter`;
  - trace propagation is reused in exporter flush threads via `Trace::SetTraceNewID(...)`.

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| `MetricsExporter` | base buffering and flush-thread lifecycle | `metrics_exporter.h`, `metrics_exporter.cpp` | shared by concrete exporters |
| `HardDiskExporter` | concrete file writer and rotation logic | `hard_disk_exporter.h`, `hard_disk_exporter.cpp` | used by metrics and access logs |
| `ResMetricCollector` | periodic collection and export submission | `res_metric_collector.h`, `res_metric_collector.cpp` | singleton |
| `ResMetricName` family set | stable metric ordering and identity | `res_metric_name.h`, `res_metrics.def` | order-sensitive |
| `metrics_description.def` | semantic meaning and units | `metrics_description.def` | doc source for many families |
| runtime registration code | ties families to live handlers | `worker_oc_server.cpp` | major current registration site |

## Main Flows

### Register And Collect Metrics

1. runtime startup code registers handler lambdas through `RegisterCollectHandler`;
2. `ResMetricCollector` periodically iterates metric families from `SHARED_MEMORY` to `RES_METRICS_END - 1`;
3. collected strings are assembled and submitted to the exporter path.

Key files:

- `src/datasystem/common/metrics/res_metric_collector.cpp`
- `src/datasystem/worker/worker_oc_server.cpp`
- `src/datasystem/common/metrics/res_metrics.def`

Failure-sensitive steps:

- missing registration leaves blank columns plus a warning rather than failing hard;
- family-definition and registration drift can make output semantically wrong while still syntactically valid.

### Buffer And Persist Metric Output

1. collector or other producer calls `WriteMessage()` on the exporter;
2. active buffers accumulate content until a threshold or forced submit;
3. `SubmitWriteMessage()` moves the active buffer into the queue;
4. flush thread persists queued data and rotates files if needed.

Key files:

- `src/datasystem/common/metrics/metrics_exporter.cpp`
- `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`

Failure-sensitive steps:

- flush threshold or queue handling changes can alter latency and batching behavior;
- disk exporter issues affect metrics and access-monitor logs together.

### Define Or Extend Metric Families

1. a metric family is added or updated in `res_metrics.def`;
2. description and units are aligned in `metrics_description.def`;
3. runtime code registers a handler for the new family;
4. collector and exporter paths expose the new output in monitor files.

Key files:

- `src/datasystem/common/metrics/res_metrics.def`
- `src/datasystem/common/metrics/metrics_description.def`
- `src/datasystem/worker/worker_oc_server.cpp`

Failure-sensitive steps:

- changing enum order can corrupt the meaning of existing columns;
- adding a definition without a handler can create deceptively empty output.

## Data And State Model

- Important in-memory state:
  - registered handler map in `ResMetricCollector`;
  - active buffer plus queued buffers in `MetricsExporter`;
  - file descriptor and rotation state in `HardDiskExporter`.
- Important on-disk or external state:
  - monitor/resource output files;
  - rotated metric files on disk.
- Ownership and lifecycle rules:
  - collector owns sampling cadence and exporter submission;
  - exporter base owns flush-thread lifecycle;
  - runtime modules own the handler logic they register.
- Concurrency or thread-affinity rules:
  - registration happens from runtime setup code;
  - collection and flush happen asynchronously;
  - trace context is explicitly reattached to flush threads.
- Ordering requirements or invariants:
  - family definitions and handler registrations must align;
  - exporter must initialize before periodic submission can persist anything;
  - enum order in `res_metrics.def` must remain stable.

## Configuration Model

| Config | Type | Default or source | Effect | Risk if changed |
| --- | --- | --- | --- | --- |
| `log_monitor` | gflag | common gflags, default true | enables collector/exporter path | can disable all monitor output without breaking runtime logic |
| `log_monitor_exporter` | gflag | common gflags, current supported value `harddisk` | chooses exporter backend | unsupported values degrade initialization |
| `log_monitor_interval_ms` | gflag | defined in `res_metric_collector.cpp`, default `10000` | controls collection cadence | changes file volume, freshness, and load |
| `max_log_size` | gflag | common logging config | rotation threshold for disk exporter | affects file churn and file size |
| `max_log_file_num` | gflag | common logging config | controls retention count | may prune too aggressively or grow disk usage |
| `log_dir` | gflag | common logging config | output directory | can redirect metric and access-monitor files |

## Failure Model

- Expected failure modes:
  - unsupported exporter selection;
  - exporter init failure;
  - missing handler registration;
  - disk write or rotation issues.
- Partial-degradation behavior:
  - runtime features can continue even if monitor output becomes blank or stale;
  - one family can degrade independently of others because missing handlers only warn once.
- Retry, fallback, or fail-fast rules:
  - collector validates exporter type and logs invalid configuration;
  - missing handlers do not fail the process.
- What must remain true after failure:
  - metric family ordering stays stable;
  - one bad handler should not redefine another family’s meaning;
  - exporter failure should not corrupt unrelated ordinary logging.

## Observability

- Main logs:
  - monitor/resource output files via `HardDiskExporter`;
  - warning logs for missing handler coverage or bad exporter config.
- Metrics:
  - this module is the metric collection path itself.
- Traces or correlation fields:
  - exporter flush threads inherit trace IDs through `Trace::SetTraceNewID(...)`.
- Debug hooks or inspection commands:
  - inspect collector registration in `worker_oc_server.cpp`;
  - inspect exporter queue/flush logic in `metrics_exporter.cpp` and `hard_disk_exporter.cpp`;
  - verify `log_monitor*` flags and output files.
- How to tell the module is healthy:
  - monitor files are created and rotated as expected;
  - expected metric families appear with non-empty values;
  - no repeated exporter-init or unsupported-backend errors appear.

## Security And Safety Constraints

- Authn/authz expectations:
  - this module is not the policy boundary for deciding which operations are allowed.
- Input validation requirements:
  - handler payloads are string-based and should be treated as data produced by runtime components, not trusted schemas.
- Data sensitivity or privacy requirements:
  - new metric payloads should avoid embedding secrets or sensitive request contents into monitor files.
- Resource or isolation constraints:
  - collection frequency and exporter buffering must stay within acceptable CPU, memory, and disk budgets.
- Unsafe changes to avoid:
  - reordering metric families;
  - sharing exporter changes without reviewing logging-side access output impact.

## Compatibility And Invariants

- External compatibility constraints:
  - downstream tooling may read monitor files with stable column expectations.
- Internal invariants:
  - metrics are explicitly registered, not auto-discovered;
  - `ResMetricCollector` iterates a stable family order;
  - `HardDiskExporter` is the current concrete backend shared with access logging.
- File format / wire format / schema stability notes:
  - `metrics_description.def` and `res_metrics.def` should evolve together for readability and correctness.
- Ordering or naming stability notes:
  - enum order in `res_metrics.def` is stability-sensitive and explicitly warned in code comments.

## Performance Characteristics

- Hot paths:
  - handler lambdas for frequently collected families;
  - exporter buffer enqueue/dequeue;
  - disk flush and rotation.
- Known expensive operations:
  - file flushes and rotations;
  - low collection intervals that increase scan frequency.
- Buffering, batching, or async behavior:
  - producer writes are buffered;
  - flush is asynchronous in exporter threads;
  - collection cadence is interval-driven rather than event-driven.
- Resource limits or scaling assumptions:
  - the collector expects bounded per-family handler cost;
  - buffer and retention settings are the main mechanisms for resource control.

## Build, Test, And Verification

- Build entrypoints:
  - repository standard build flow via `build.sh`
  - target-level builds for `metrics_exporter_base`, `hard_disk_exporter`, and `common_metrics`
- Fast verification commands:
  - `cmake --build <build-dir> --target metrics_exporter_base hard_disk_exporter common_metrics`
- Representative unit/integration/system tests:
  - `Pending verification`: add specific metrics-focused tests when indexed
- Recommended validation for risky changes:
  - verify one registered family emits, one missing family warns as expected, and one exporter rotation path still writes valid output.

## Common Change Scenarios

### Adding A New Capability

- Recommended extension point:
  - new family definition in `res_metrics.def` plus registration in runtime code;
  - exporter work in `metrics_exporter.*` or `hard_disk_exporter.*` when behavior is persistence-related.
- Required companion updates:
  - `metrics_description.def`
  - runtime registration code such as `worker_oc_server.cpp`
  - `.repo_context/modules/infra/metrics/*`
  - logging docs if exporter changes affect access-monitor output
- Review checklist:
  - are family definition, description, and registration aligned?
  - does exporter behavior remain compatible for logging-side users?
  - does cadence or buffering stay within expected operational limits?

### Modifying Existing Behavior

- Likely breakpoints:
  - `RegisterCollectHandler` coverage
  - exporter buffer thresholds and submit semantics
  - `HardDiskExporter` rotation logic
- Compatibility checks:
  - family order, file output shape, and monitor file retention behavior
- Observability checks:
  - monitor files, warning logs for missing handlers, and access-monitor output if exporter code changed

### Debugging A Production Issue

- First files to inspect:
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
  - `src/datasystem/common/metrics/metrics_exporter.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
- First runtime evidence to inspect:
  - `log_monitor*` flags
  - monitor output files and rotation state
  - whether missing families are blank or entirely absent
- Common misleading symptoms:
  - output file exists but one family is blank because registration is missing;
  - exporter changes look like “metrics bug” even when they also broke access logging.

## Open Questions

- whether additional registration sites outside `worker_oc_server.cpp` deserve dedicated design notes;
- whether a non-harddisk exporter backend is planned or expected to remain dormant.

## Pending Verification

- exact repository test targets that validate collector cadence, exporter buffering, and family-definition stability;
- whether any production tooling depends on a specific monitor file column contract beyond the current code paths.

## Update Rules For This Document

- Keep design statements aligned with the real implementation and linked source files.
- When module boundaries, major flows, config semantics, or compatibility rules change, update this doc in the same task when practical.
- If a section becomes speculative or stale, narrow it or move it to `Pending verification` until confirmed from source.
