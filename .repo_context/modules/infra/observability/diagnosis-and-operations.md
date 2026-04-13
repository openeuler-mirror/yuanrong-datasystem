# Diagnosis And Operations Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - `current implementation | mixed`
- Primary code paths:
  - `src/datasystem/worker/*`
  - `src/datasystem/common/log/*`
  - `src/datasystem/common/metrics/*`
  - `src/datasystem/worker/cluster_manager/*`
  - `src/datasystem/worker/hash_ring/*`
  - `tests/*`
- Primary source-of-truth files:
  - `src/datasystem/worker/worker_main.cpp`
  - `src/datasystem/worker/worker.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/worker_liveness_check.cpp`
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/log/log_manager.cpp`
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/failure_handler.cpp`
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
  - `src/datasystem/common/metrics/metrics_exporter.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
  - `src/datasystem/worker/cluster_manager/etcd_cluster_manager.cpp`
  - `src/datasystem/worker/hash_ring/hash_ring.cpp`
  - `tests/README.md`
  - `build.sh`
- Last verified against source:
  - `2026-04-13`
- Related context docs:
  - `.repo_context/modules/runtime/worker-runtime.md`
  - `.repo_context/modules/runtime/cluster-management.md`
  - `.repo_context/modules/infra/logging/design.md`
  - `.repo_context/modules/infra/metrics/design.md`
  - `.repo_context/modules/quality/tests-and-reproduction.md`
- Related user-facing or internal docs:
  - `docs/source_zh_cn/design_document/cluster_management.md`

## Purpose

- Why this design document exists:
  - provide a cross-module reference for how to localize functionality, performance, and operations problems using the signals already exposed by runtime, logging, metrics, and cluster-management code.
- What problem this module solves:
  - reduce time-to-localization by mapping symptoms to the right logs, metrics, configs, runtime files, and source entrypoints instead of treating diagnosis as an ad hoc code search.
- Who or what depends on this module:
  - on-call engineers, developers debugging runtime behavior, reviewers reasoning about DFX changes, and future playbooks that standardize incident and performance workflows.

## Business And Scenario Overview

- Why this capability is needed:
  - the repository is a distributed system with multiple processes, async background threads, configurable storage and metadata backends, and partially degradable observability paths, so issues often look like "the service is up but the evidence is misleading."
- Target users, callers, or operators:
  - worker or master developers, test authors, deployment or operations engineers, and reviewers evaluating DFX-sensitive changes.
- Typical usage time or trigger conditions:
  - startup failures, readiness or liveness probe failures, request errors, request latency regressions, missing logs, blank monitor columns, storage pressure, and topology-change incidents.
- Typical deployment or runtime environment:
  - long-lived worker and master processes, local client processes, k8s-style readiness and liveness probes, and filesystem-backed logs and monitor files.
- How the feature is expected to be used:
  - start from a symptom, jump to the right signal class, inspect the first code or config entrypoints, collect a minimal evidence set, then either localize to one module or escalate with a narrower problem statement.
- What other functions or modules it may affect:
  - logging and metrics design changes, worker startup ordering, cluster-management config, hash-ring recovery behavior, test selection, and future operator runbooks.
- Expected user or operator experience:
  - performance:
    - issue localization should not require reading the whole codebase when the right signal map exists.
  - security:
    - evidence collection should avoid spreading credentials, AK/SK, or sensitive payloads into tickets or logs.
  - resilience:
    - operators should be able to distinguish "observability degraded" from "service unavailable."
  - reliability:
    - common symptom classes should route to repeatable first checks rather than personal heuristics.
  - availability:
    - diagnosis guidance should help decide whether a process is healthy, degraded, or unsafe to keep in service.
  - privacy:
    - troubleshooting artifacts should prefer aggregate metrics and infra logs over raw request content when possible.

## Goals

- define a repeatable diagnosis flow across runtime, logging, metrics, and cluster behavior;
- describe which signals are primary for functionality, performance, and operations issues;
- make readiness, liveness, and degradation states explicit;
- record common evidence sets for incident triage and performance investigation;
- keep the guidance source-backed and narrow enough to stay maintainable.

## Non-Goals

- replacing module-level implementation docs for logging, metrics, or runtime internals;
- acting as a user-facing deployment manual for every environment;
- defining SLO dashboards or automated alert rules that are not encoded in the repository today.

## Scope

- In scope:
  - runtime health indicators such as startup success, readiness, liveness, and graceful shutdown;
  - signal classes from ordinary logs, access logs, crash logs, and resource monitor files;
  - high-level diagnosis flows for functionality, performance, and operations issues;
  - evidence collection guidance and escalation boundaries;
  - operator-facing config and file paths that materially affect diagnosis.
- Out of scope:
  - full implementation details already covered in logging or metrics subdocuments;
  - business-feature-specific troubleshooting outside shared runtime and infra concerns;
  - external observability stacks not represented in the repository.
- Important boundaries with neighboring modules:
  - `logging` and `metrics` own signal production;
  - `runtime` owns worker and cluster behavior;
  - `observability` owns cross-module routing, evidence prioritization, and health interpretation.

## Terminology

| Term | Meaning in this repository | Source or note |
| --- | --- | --- |
| `ready check` | file-based readiness signal created after worker service startup success | `src/datasystem/worker/worker_oc_server.cpp` |
| `liveness probe` | file-based health signal maintained by `WorkerLivenessCheck` | `src/datasystem/worker/worker_liveness_check.cpp` |
| `ordinary logs` | severity-based repository logs emitted through `LOG` and `VLOG` | `src/datasystem/common/log/log.h`, `logging.cpp` |
| `resource monitor` | periodic resource metric output persisted through `HardDiskExporter` | `src/datasystem/common/metrics/res_metric_collector.cpp` |
| `partial observability degradation` | business logic continues while one observability path, such as monitor logs, is reduced or missing | validated from logging and metrics init behavior |

## Current State And Design Choice Notes

- Current implementation or baseline behavior:
  - observability is file-centric and process-local, built from ordinary logs, access logs, resource monitor files, crash output, readiness files, and liveness files.
- Relevant constraints from current release or deployment:
  - monitor logging can be disabled or degraded without stopping the worker process;
  - readiness and liveness are file-based rather than network endpoint-based in the currently inspected worker paths;
  - some failures show up first as missing evidence, not immediate process exit.
- Similar upstream, industry, or historical approaches considered:
  - compared with externally centralized observability stacks, this repository currently relies on local evidence production plus explicit runtime health signals.
- Selected approach and why:
  - the current code favors direct local artifacts that can be consumed in tests, containers, and deployments without requiring an external metrics or tracing backend.
- Known tradeoffs:
  - local artifacts are easy to inspect but require disciplined routing guidance;
  - observability degradation can be mistaken for service failure or vice versa if operators do not know the distinction.

## Architecture Overview

- High-level structure:
  - worker startup and shutdown define the core runtime lifecycle;
  - logging provides severity logs, access logs, trace IDs, and crash output;
  - metrics provides periodic resource monitor files;
  - cluster-management and hash-ring code influence whether the node is logically healthy for traffic;
  - readiness and liveness files provide operator-facing health artifacts.
- Key runtime roles or processes:
  - request or service threads serving object, stream, and worker APIs;
  - background maintenance threads for logging, metrics flush, and liveness checks;
  - runtime startup and shutdown orchestration in `worker_main.cpp` and `worker.cpp`.
- Key persistent state, if any:
  - ordinary logs, access logs, resource monitor files, `container.log`, readiness file, and liveness file.
- Key control-flow or data-flow stages:
  - startup and config validation;
  - health artifact creation;
  - steady-state request handling and background observation;
  - periodic health monitoring and metrics flush;
  - graceful shutdown and evidence preservation.

## Scenario Analysis

- Primary user or system scenarios:
  - worker starts but never becomes ready;
  - worker is ready but liveness later fails;
  - request path succeeds but monitor logs or metrics disappear;
  - request latency rises while the process stays up;
  - topology or backend changes cause routing or readiness anomalies.
- Key interactions between actors and services:
  - startup code interacts with logging and worker-service initialization;
  - liveness code interacts with worker services, RocksDB checks, and probe files;
  - logging and metrics interact through shared file-exporter behavior;
  - cluster management influences whether a node is logically healthy for service.
- Normal path summary:
  - startup initializes logging and worker services, runtime creates readiness and liveness artifacts, request and background paths emit logs and metrics, and shutdown preserves a meaningful sequence of pre-shutdown then shutdown.
- Exceptional path summary:
  - startup may fail before readiness, liveness may fail after readiness, monitor logging may degrade independently, and cluster or backend misconfiguration may make a node unhealthy even when its process stays alive.

## Entry Points, External Interfaces, And Integration Points

- Public APIs:
  - no standalone user API is defined here; this document routes to health and evidence sources produced by runtime, logging, and metrics modules.
- Internal service entrypoints:
  - `main()` in `src/datasystem/worker/worker_main.cpp`
  - `Worker::Init`, `Worker::PreShutDown`, `Worker::ShutDown`
  - `WorkerLivenessCheck::Init`, `Run`, `SetLivenessProbe`
  - `Logging::Start(...)`
  - `ResMetricCollector::Init()` and `Start()`
- External protocols, schemas, or data formats:
  - filesystem-backed probe files for readiness and liveness;
  - severity log files, access log lines, resource monitor lines, and crash logs;
  - cluster-management metadata behavior through ETCD or Metastore-backed runtime paths.
- CLI commands, config flags, or environment variables:
  - runtime health and operations-related flags such as `ready_check_path`, `liveness_check_path`, `liveness_probe_timeout_s`, `log_monitor`, `log_monitor_exporter`, `log_monitor_interval_ms`, `log_dir`, `etcd_address`, and `metastore_address`
  - test and reproduction entrypoints through `build.sh`
- Background jobs, threads, or callbacks:
  - logging background maintenance;
  - metrics flush and collection threads;
  - liveness-check thread;
  - perf tick and config-file monitoring loop in `worker_main.cpp`.
- Cross-module integration points:
  - readiness and liveness depend on worker runtime behavior;
  - evidence collection depends on logging and metrics configuration;
  - cluster health interpretation depends on cluster-management and hash-ring behavior.
- Upstream and downstream dependencies:
  - upstream operators or tests depend on these artifacts to decide whether a node is healthy;
  - downstream diagnosis depends on stable file and config semantics.
- Backward-compatibility expectations for external consumers:
  - readiness and liveness file semantics, log file names, and monitor file layout should be treated as operationally visible behavior.

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| worker startup loop | process lifecycle, periodic perf tick, config monitoring, shutdown phases | `src/datasystem/worker/worker_main.cpp` | first place for process-level hangs |
| worker runtime orchestration | service init and shutdown composition | `src/datasystem/worker/worker.cpp`, `worker_oc_server.cpp` | runtime source of many health transitions |
| liveness check | periodic worker health probing and file update | `src/datasystem/worker/worker_liveness_check.cpp` | distinct from readiness |
| logging subsystem | ordinary logs, access logs, crash logs, trace continuity | `src/datasystem/common/log/*` | main evidence surface for failures |
| metrics subsystem | periodic resource monitor files | `src/datasystem/common/metrics/*` | main evidence surface for resource and backlog issues |
| cluster-management paths | node coordination, metadata backend behavior, routing state | `src/datasystem/worker/cluster_manager/*`, `hash_ring/*` | often root cause for "alive but unhealthy" behavior |

## Main Flows

### Startup, Readiness, And Steady State

1. `main()` calls `Worker::Init(...)`.
2. Startup initializes runtime services, logging, and other subsystem state.
3. Worker-service success allows readiness-related artifacts and service connectivity to become valid.
4. The main loop performs periodic perf ticking and config-file monitoring until termination.

Key files:

- `src/datasystem/worker/worker_main.cpp`
- `src/datasystem/worker/worker.cpp`
- `src/datasystem/worker/worker_oc_server.cpp`
- `src/datasystem/common/log/logging.cpp`

Failure-sensitive steps:

- startup failures prevent readiness from becoming true;
- startup ordering issues can leave the process alive with incomplete observability;
- config mistakes around metadata backends can block healthy steady state.

### Liveness Monitoring And Health File Updates

1. `WorkerLivenessCheck::Init()` creates the liveness thread and policy set.
2. The liveness thread periodically runs service checks and optional RocksDB checks for master nodes.
3. Success writes `"liveness check success"` to the probe file; failure writes `"liveness check failed"`.
4. Probe-file freshness and contents are revalidated on a timer.

Key files:

- `src/datasystem/worker/worker_liveness_check.cpp`
- `src/datasystem/worker/worker_oc_server.cpp`

Failure-sensitive steps:

- stale or missing probe files can indicate either the worker is unhealthy or the probe path is misconfigured;
- liveness may fail after readiness, so "was ready once" is not enough evidence of current health.

### Diagnosis And Evidence Collection

1. Start from the symptom class: startup, request failure, latency, missing evidence, or topology issue.
2. Choose the first signal family: ordinary logs, access logs, resource monitor files, readiness or liveness files, or cluster config.
3. Confirm key config flags and file paths.
4. Narrow to one primary module and select the nearest test or reproduction path.

Key files:

- `src/datasystem/common/log/*`
- `src/datasystem/common/metrics/*`
- `src/datasystem/worker/*`
- `build.sh`

Failure-sensitive steps:

- treating missing monitor logs as equivalent to full service failure;
- treating a live process as healthy without checking readiness, liveness, or cluster state;
- collecting too little evidence to distinguish runtime failure from observability failure.

## Data And State Model

- Important in-memory state:
  - worker lifecycle state and service availability;
  - logging subsystem initialization state;
  - collector and flush-thread state for monitor files;
  - liveness-check thread state.
- Important on-disk or external state:
  - log directory contents;
  - readiness and liveness files;
  - metadata backend configuration and runtime connectivity.
- Ownership and lifecycle rules:
  - runtime owns readiness and liveness semantics;
  - logging and metrics own evidence production;
  - operators and playbooks own evidence collection order, not the source modules.
- Concurrency or thread-affinity rules:
  - steady-state, flush, collector, and liveness work happen on different threads;
  - trace continuity must often be reattached explicitly in async paths.
- Ordering requirements or invariants:
  - readiness depends on successful startup;
  - liveness depends on successful periodic checks after startup;
  - `PreShutDown` and `ShutDown` are distinct phases that should be interpreted separately in evidence.

## External Interaction And Dependency Analysis

- Dependency graph summary:
  - diagnosis depends on worker runtime, logging, metrics, cluster-management, and tests together;
  - health decisions depend on both process state and signal state.
- Critical upstream services or modules:
  - worker startup and service initialization;
  - metadata backends such as ETCD or Metastore;
  - local filesystem for logs and probe files.
- Critical downstream services or modules:
  - operator probes and automation reading readiness or liveness files;
  - tooling and humans reading logs and resource monitor files;
  - playbooks and test workflows that need stable routing.
- Failure impact from each critical dependency:
  - logging failure reduces evidence quality;
  - metrics failure reduces resource or backlog visibility;
  - cluster-management or metadata failure can leave the process alive but functionally unhealthy;
  - filesystem failure can remove both evidence and health artifacts.
- Version, protocol, or schema coupling:
  - file names and monitor-line layouts are operational contracts;
  - metadata backend selection influences cluster-health interpretation.
- Deployment or operations dependencies:
  - writable `log_dir`;
  - correct readiness and liveness file paths;
  - correct metadata backend and worker address config.

## Open Source Software Selection And Dependency Record

| Dependency | Purpose in this module | Why selected | Alternatives considered | License | Version / upgrade strategy | Security / maintenance notes |
| --- | --- | --- | --- | --- | --- | --- |
| no observability-module-specific OSS dependency | this module documents how shared runtime, logging, and metrics signals are used together rather than introducing a new implementation dependency | keeps diagnosis guidance aligned with existing repo subsystems | add a direct external APM or telemetry dependency here | n/a | document a concrete dependency here only if a new observability-specific runtime dependency is introduced | inherited operational risk still exists through shared modules such as logging, metrics, and metadata backends |

## Configuration Model

| Config | Type | Default or source | Effect | Risk if changed |
| --- | --- | --- | --- | --- |
| `ready_check_path` | gflag | worker runtime flag | controls readiness artifact path | bad path makes healthy startup look unready |
| `liveness_check_path` | gflag | worker runtime flag | controls liveness artifact path | bad path makes runtime health hard to evaluate |
| `liveness_probe_timeout_s` | gflag | worker runtime flag | controls liveness freshness timeout | too low causes false unhealthy states, too high delays detection |
| `log_monitor` | gflag | shared observability flag | enables monitor logs and metrics collection | missing monitor evidence may be config-driven, not runtime failure |
| `log_monitor_exporter` | gflag | metrics collector config | chooses monitor exporter backend | invalid value prevents monitor collection |
| `log_dir` | gflag or env override | logging config | controls log and monitor file location | misconfigured path hides evidence |
| `etcd_address` / `metastore_address` | gflag or config | runtime and cluster config | selects metadata backend | wrong backend config causes routing or readiness anomalies |

## Examples And Migration Notes

- Example API requests or responses to retain:
  - not applicable as a primary concern; the key external artifacts here are files and logs rather than API schemas.
- Example configuration or deployment snippets to retain:
  - readiness and liveness path settings should be preserved in deployment configs when probe behavior changes.
- Example operator workflows or commands to retain:
  - inspect readiness and liveness files, then inspect ordinary logs, access logs, and resource monitor files under the configured `log_dir`.
- Upgrade, rollout, or migration examples if behavior changes:
  - when changing health-file semantics, log naming, monitor-file layout, or cluster-backend fallback, update this doc, the signal map, and the related operations playbooks in the same task.

## Availability

- Availability goals or service-level expectations:
  - operators should be able to distinguish "service unavailable", "service degraded", and "service available but observability degraded".
- Deployment topology and redundancy model:
  - the current evidence model is process-local and node-local; cluster availability is influenced by worker, metadata backend, and hash-ring behavior outside a single file artifact.
- Single-point-of-failure analysis:
  - a single worker process can lose evidence or health artifacts if its local filesystem or runtime init fails;
  - metadata backend misconfiguration can make a whole node unhealthy despite a live process.
- Failure domains and blast-radius limits:
  - ordinary logs, monitor files, and probe files can fail independently;
  - readiness and liveness are separate signals;
  - cluster-management issues can be broader than one node's local logs imply.
- Health checks, readiness, and traffic removal conditions:
  - readiness indicates worker service startup and stub connectivity success;
  - liveness indicates periodic internal checks still pass and the probe file is fresh;
  - traffic should not rely on process aliveness alone.
- Failover, switchover, or recovery entry conditions:
  - recovery often starts with process restart, config correction, or metadata-backend restoration rather than local log recovery alone.
- Capacity reservation or headroom assumptions:
  - operators need disk headroom for logs and monitor files; pressure here directly harms diagnosability.

## Reliability

- Expected failure modes:
  - worker startup failure;
  - readiness not established;
  - liveness probe failure or stale file;
  - missing logs or monitor files due to config or exporter degradation;
  - cluster or backend misconfiguration causing unhealthy-but-alive nodes.
- Data consistency, durability, or correctness requirements:
  - health artifacts should reflect current runtime state closely enough to guide traffic and debugging decisions;
  - signal semantics should remain stable across releases.
- Retry, idempotency, deduplication, or exactly-once assumptions:
  - diagnosis relies on repeated local sampling and logs rather than exactly-once evidence delivery guarantees.
- Partial-degradation behavior:
  - the system may continue processing some work even when monitor logs or metrics degrade;
  - readiness may have succeeded earlier even though liveness later fails.
- Recovery, replay, repair, or reconciliation strategy:
  - collect evidence first, then repair config or restart runtime, and validate probe files plus signal files after recovery.
- RTO, RPO, MTTR, or equivalent recovery targets:
  - no explicit numeric targets are encoded in the inspected code.
- What must remain true after failure:
  - operators must still be able to tell which signal class degraded first;
  - shutdown phase evidence should distinguish `PreShutDown` from `ShutDown` failure paths.

## Resilience

- Overload protection, rate limiting, or admission control:
  - not owned here directly, but diagnosis should consider queue pressure, thread pools, and async flush behavior before concluding logic regressions.
- Timeout, circuit-breaker, and backpressure strategy:
  - liveness timeout and monitor intervals are the most visible timing controls in the inspected paths.
- Dependency failure handling:
  - logging and metrics often degrade rather than crash the process;
  - metadata or runtime service failure can invalidate health even when logs continue.
- Graceful-degradation behavior:
  - a node can be partially usable or partially observable; this document exists to make that distinction explicit.
- Safe rollback, feature-gating, or kill-switch strategy:
  - observability and health-related gflags such as `log_monitor` and probe paths are primary operational controls.
- Attack or fault scenarios the module should continue to tolerate:
  - missing handler registration for one metric family;
  - startup-path failures with enough logs preserved for localization;
  - transient internal service issues that should flip liveness before becoming silent corruption.

## Security, Privacy, And Safety Constraints

- Authn/authz expectations:
  - troubleshooting must not weaken authentication or authorization assumptions to gather evidence.
- Input validation requirements:
  - probe paths and log directories rely on validated path-like config; invalid paths are an operations risk.
- Data sensitivity or privacy requirements:
  - incident evidence should avoid spreading secrets, AK/SK, and raw sensitive payloads outside controlled logs.
- Secrets, credentials, or key-management requirements:
  - AK/SK and secret-related flags in runtime config are especially sensitive during evidence capture.
- Resource or isolation constraints:
  - disk exhaustion harms both runtime and diagnosability; health artifacts must remain writable.
- Unsafe changes to avoid:
  - changing health semantics or evidence file names without updating diagnostics and runbooks;
  - assuming "no monitor data" means "no service" or vice versa.
- Safety constraints for human, environment, or property impact:
  - no direct physical safety contract is encoded here, but unsafe health interpretation can still amplify outage impact.

## Observability

- Main logs:
  - ordinary logs, access logs, resource monitor files, and `container.log`.
- Metrics:
  - resource monitor families from the metrics subsystem.
- Traces or correlation fields:
  - trace IDs in logging and async exporter paths.
- Alerts or SLO-related signals:
  - no alert rules are encoded here, but readiness, liveness, log continuity, and monitor freshness are the primary local health signals.
- Debug hooks or inspection commands:
  - inspect `build.sh` test entrypoints and the runtime files listed above;
  - inspect probe files and log directories directly on the node or test environment.
- How to tell the module is healthy:
  - readiness file exists when expected;
  - liveness file is present, fresh, and contains success;
  - logs and monitor files are updating normally;
  - no cluster-backend config contradiction is visible.

## Compatibility And Invariants

- External compatibility constraints:
  - health-file names and semantics, log file families, and monitor file layout are operationally visible.
- Internal invariants:
  - readiness and liveness are different signals;
  - startup, pre-shutdown, and shutdown are different lifecycle phases;
  - monitor logging and resource collection can degrade without implying full process failure.
- File format, wire format, or schema stability notes:
  - resource monitor and access-log line layout must remain stable enough for diagnosis consumers.
- Ordering or naming stability notes:
  - log and monitor file names should remain stable unless the related docs and playbooks are updated.
- Upgrade, downgrade, or mixed-version constraints:
  - rollout-time confusion is likely if health semantics or monitor file meaning change without matching doc updates.

## Performance Characteristics

- Hot paths:
  - request-serving code is not documented here directly, but diagnosis depends on the signals it emits;
  - perf tick, config monitoring, metrics collection, and flush threads are recurring background paths.
- Known expensive operations:
  - file compression, monitor flush, low monitor intervals, and any overloaded runtime thread pools.
- Buffering, batching, or async behavior:
  - access and resource evidence is asynchronous in several paths, so operators should expect delay between runtime events and file persistence.
- Resource limits or scaling assumptions:
  - diagnosis quality depends on enough disk, queue capacity, and reasonable monitor intervals to keep evidence current.

## Build, Test, And Verification

- Build entrypoints:
  - `bash build.sh -t build`
  - `bash build.sh -t run_cases -l st`
- Fast verification commands:
  - `bash build.sh -t run_cases -l ut`
  - `bash build.sh -t run_cases -l st`
- Representative unit, integration, and system tests:
  - see `.repo_context/modules/quality/tests-and-reproduction.md` for the current verified test routing layer.
- Recommended validation for risky changes:
  - validate one startup path, one readiness or liveness path, one monitor-log path, and one shutdown path when changing DFX-sensitive behavior.

## Self-Verification Cases

| ID | Test scenario | Purpose | Preconditions | Input / steps | Expected result |
| --- | --- | --- | --- | --- | --- |
| `OBS-001` | startup without readiness | distinguish startup failure from liveness failure | writable log dir and a worker startup path | start worker with a known-bad required config such as metadata backend omission | worker logs show startup error, readiness is not established, and shutdown path is visible |
| `OBS-002` | liveness failure after readiness | distinguish current health from historical readiness | worker reaches ready state and liveness probe path is configured | induce a liveness policy failure or stale probe file | readiness may have existed earlier, but liveness file indicates failure or staleness |
| `OBS-003` | observability degradation without hard service failure | confirm partial-degradation interpretation | worker path with `log_monitor` or exporter issues | disable or break monitor output while keeping ordinary service startup healthy | ordinary logs continue while monitor files or resource signals degrade |
| `OBS-004` | performance regression evidence set | validate performance diagnosis routing | worker or client perf scenario | run a representative ST or perf case and inspect logs plus monitor output | evidence shows which signal classes change first under load |

## Common Change Scenarios

### Adding A New Capability

- Recommended extension point:
  - new cross-module diagnosis guidance belongs here;
  - new signal-production behavior still belongs in logging, metrics, or runtime docs.
- Required companion updates:
  - update signal-map and relevant operations playbooks;
  - update module-specific docs if signal semantics changed.
- Review checklist:
  - does the change alter health interpretation?
  - does it alter evidence file names or semantics?
  - does it create a new partial-degradation mode that operators must know?

### Modifying Existing Behavior

- Likely breakpoints:
  - worker startup order;
  - readiness or liveness file behavior;
  - logging or metrics init gating;
  - metadata-backend validation.
- Compatibility checks:
  - probe files, log families, monitor files, and operational config semantics.
- Observability checks:
  - ordinary logs, monitor files, trace continuity, readiness, liveness, and one representative cluster-health path.

### Debugging A Production Issue

- First files to inspect:
  - `src/datasystem/worker/worker_main.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/worker_liveness_check.cpp`
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/metrics/res_metric_collector.cpp`
- First runtime evidence to inspect:
  - readiness and liveness files;
  - `log_dir` contents;
  - `log_monitor` and metadata-backend config;
  - whether the process is alive, ready, live, and producing signals.
- Common misleading symptoms:
  - a live process can still be unready or unhealthy;
  - missing monitor files may be config or exporter driven, not request-path failure;
  - healthy readiness at startup does not prove continuing liveness.

## Open Questions

- which startup paths beyond the worker binary should be documented as first-class health producers in future updates;
- whether more cluster-management recovery signals should be pulled into this area as dedicated subdocuments;
- whether deployment automation already depends on stable readiness and liveness file contents beyond current code comments.

## Pending Verification

- a full inventory of all worker init paths that create readiness artifacts;
- the most representative smoke-test subsets for startup, probe, and monitor validation;
- whether any additional operator scripts consume the probe files directly.

## Update Rules For This Document

- Keep design statements aligned with the real implementation and linked source files.
- When module boundaries, major flows, config semantics, external interfaces, or compatibility rules change, update this doc in the same task when practical.
- When DFX expectations change, update the Availability, Reliability, Resilience, Security, Privacy, Safety, and Observability sections in the same task when practical.
- If a section becomes speculative or stale, narrow it or move it to `Pending verification` until confirmed from source.
