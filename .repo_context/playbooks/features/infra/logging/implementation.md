# Logging Feature Implementation Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - mixed
- Owning module or area:
  - `modules/infra/logging/`
- Primary code paths:
  - `src/datasystem/common/log/*`
  - `src/datasystem/common/metrics/hard_disk_exporter/*`
- Related module docs:
  - `../../modules/infra/logging/README.md`
  - `../../modules/infra/logging/trace-and-context.md`
  - `../../modules/infra/logging/access-recorder.md`
  - `../../modules/infra/logging/log-lifecycle-and-rotation.md`
- Related design docs:
  - `../../modules/infra/logging/design.md`
  - `../../modules/infra/metrics/design.md`
- Related tests or validation entrypoints:
  - `../../modules/quality/build-test-debug.md`
  - `../../modules/quality/tests-and-reproduction.md`
- Last verified against source:
  - `2026-04-13`

## Purpose

- Why this playbook exists:
  - standardize how to design and implement new logging features with low compatibility and observability risk.
- What change class it standardizes:
  - feature additions or behavior changes touching `Logging`, `Trace`, `AccessRecorder`, `LogManager`, failure handling, or shared exporter behavior.
- What risks it is meant to reduce:
  - broken trace propagation, missing access logs, silent monitor-log degradation, rotation/retention regressions, and accidental schema or file-contract drift.

## When To Use This Playbook

- Use when:
  - adding a new logging capability or log output path;
  - modifying trace behavior, access-record behavior, log rotation, startup config, or crash-path behavior;
  - changing anything in `src/datasystem/common/log/*` or `hard_disk_exporter` because of a logging feature.
- Do not use when:
  - the task is only to answer a code-reading question without proposing changes;
  - the change is a purely local callsite log message with no reusable behavior change.
- Escalate to design-first review when:
  - access-log file shape, field order, or key mapping changes;
  - trace propagation semantics change across threads or processes;
  - exporter behavior changes in ways that affect both logging and metrics;
  - startup sequence or failure-signal handling changes;
  - a new log type, new storage backend, or new external consumer contract is introduced.

## Preconditions

- Required context to read first:
  - `../../modules/infra/logging/design.md`
  - `../../modules/infra/logging/README.md`
  - the one or two most relevant logging subdocs for the intended change
- Required source files to inspect first:
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/log/log_manager.cpp`
  - `src/datasystem/common/log/trace.cpp`
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/failure_handler.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp` when exporter coupling is involved
- Required assumptions to verify before coding:
  - whether the feature belongs in ordinary logs, trace helpers, access records, monitor logs, or crash-path logging;
  - whether downstream tooling depends on current file names, record keys, or field order;
  - whether the feature must work in both client and runtime startup paths.

## Request Intake

- Requested behavior change:
  - write the desired logging behavior in one sentence using source-backed terms.
- Explicit non-goals:
  - record what must not change, especially schema, trace semantics, or retention behavior.
- Affected users, processes, or services:
  - client, worker, master, background jobs, or operators.
- Backward-compatibility expectations:
  - note whether existing log files, dashboards, parsers, or alerting must continue to work unchanged.

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| module boundaries | is the change isolated to one logging layer? | only one of `Trace`, `AccessRecorder`, `Logging`, or `LogManager` changes | change spans logging plus metrics/runtime contracts |
| compatibility | does any file name, key, or output shape change? | no externally visible output contract changes | access-log key/file/field changes |
| observability | can the feature fail partially without being noticed? | failure is obvious in local verification | monitor or access logs can silently disappear |
| performance | does the change add synchronous work on hot paths? | work stays async or bounded | hot-path string formatting, file I/O, or locking increases |
| security | can the feature expose sensitive data? | only non-sensitive metadata is logged | request payloads, tokens, or credentials may be logged |
| operations | does the feature change retention, rotation, or startup behavior? | no change to lifecycle semantics | changes to `log_monitor`, rotation, compression, or failure handler install |

## Source Verification Checklist

- [ ] confirm the actual extension point in source instead of inferring from doc names
- [ ] confirm whether the feature touches `Trace`, `AccessRecorder`, `Logging`, `LogManager`, or `FailureWriter`
- [ ] confirm whether `HardDiskExporter` is in the write path
- [ ] confirm config flags or env vars that already govern the target behavior
- [ ] confirm whether access-point keys or file naming are part of the change
- [ ] confirm whether client mode and runtime mode both need the feature
- [ ] confirm a validation path exists for normal logs, monitor logs, and error paths

## Design Checklist

- [ ] choose the narrowest layer that can implement the feature
- [ ] classify the feature as one of: ordinary log behavior, trace behavior, access-record behavior, lifecycle/rotation behavior, crash-path behavior
- [ ] record which invariants must stay unchanged
- [ ] decide whether the feature needs a gate, config flag, or environment override
- [ ] decide whether compatibility-sensitive outputs stay unchanged, are extended compatibly, or require migration review
- [ ] decide whether metrics/exporter coupling requires parallel review

## Implementation Plan

1. Write down the target behavior and non-goals in source-backed terms.
2. Read `logging/design.md` and the one or two specific logging subdocs that match the target layer.
3. Verify the extension point in source and identify all coupled files before editing.
4. Classify the change as low-risk or escalation-required using the risk table above.
5. If escalation-required, produce a short design note before code changes.
6. Implement the smallest viable change at the chosen layer.
7. Validate ordinary behavior plus one negative or degraded path.
8. Update `.repo_context` module docs, design docs, and this playbook if the workflow or risks changed.

## Guardrails

- Must preserve:
  - thread-local trace isolation;
  - ability of `AccessRecorderManager` and exporter flush to work asynchronously;
  - operator-visible rotation and retention behavior unless explicitly changed;
  - crash-path output remaining available even when normal logging is degraded.
- Must not change without explicit review:
  - `access_point.def` ordering or naming;
  - access-log field shape or file naming;
  - trace import/generation semantics on async boundaries;
  - `HardDiskExporter` behavior shared with metrics.
- Must verify in source before claiming:
  - startup order for `Logging::Start()`;
  - whether `FLAGS_log_monitor` can disable the intended path;
  - whether the feature reaches both client and worker/master scenarios.

## Validation Plan

- Fast checks:
  - build the affected logging targets and any shared exporter target
  - verify the code path compiles in the actual startup or caller context
- Representative tests:
  - reuse targets from `modules/quality/tests-and-reproduction.md` when available
  - if there is no existing test, record the gap explicitly
- Manual verification:
  - confirm ordinary logs still emit
  - confirm the new feature appears in the intended log path
  - confirm trace IDs still correlate across at least one async boundary when relevant
  - confirm access/monitor files still flush and rotate when relevant
- Negative-path verification:
  - check one early-return or error path for recorder/trace cleanup regressions
  - check one degraded path where monitor logging is disabled or exporter init fails if the feature depends on it

## Review Checklist

- [ ] the chosen layer is the narrowest correct implementation point
- [ ] cross-module coupling with metrics or runtime was checked against source
- [ ] compatibility-sensitive file names, key mappings, and output shapes were preserved or explicitly reviewed
- [ ] trace propagation and cleanup semantics still hold on affected async boundaries
- [ ] rotation, compression, retention, and monitor flush behavior remain correct if touched
- [ ] sensitive data is not newly exposed in logs
- [ ] `.repo_context` docs were updated if implementation or workflow changed

## Context Update Requirements

- Module docs to update:
  - `../../modules/infra/logging/README.md`
  - one or more of `trace-and-context.md`, `access-recorder.md`, `log-lifecycle-and-rotation.md`
- Design docs to update:
  - `../../modules/infra/logging/design.md`
  - `../../modules/infra/metrics/design.md` if exporter coupling semantics changed
- Additional playbooks to update:
  - this file when the logging feature workflow or risk gates change

## Open Questions

- whether the repository already has downstream tooling that strictly parses access-log field layouts beyond what current source references reveal.

## Pending Verification

- specific automated tests that exercise logging-only feature changes end to end;
- whether failure-signal installation is fully consistent across all runtime startup paths.
