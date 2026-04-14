# Observability And Operations

`observability/` contains cross-module guidance for diagnosis, runtime health, performance investigation, and operator-facing workflows.

Use this directory when the question is not only "how does logging or metrics work?" but also "how do I localize a failure, performance regression, or operations problem across runtime, logging, metrics, and cluster behavior?"

## Read Order Inside This Area

- If the question is about overall diagnosis flow, health signals, or operations design:
  - read `diagnosis-and-operations.md`
- If the question is about mapping a symptom to the first logs, metrics, files, and configs:
  - read `signal-map.md`
- If the question is about latency, throughput, backlog, or resource bottlenecks:
  - read `performance-troubleshooting.md`
- If the question is about readiness, liveness, shutdown, and runtime-health checks:
  - read `runtime-health-and-runbook.md`

## Current Docs

- `diagnosis-and-operations.md`
  - cross-module troubleshooting architecture, DFX expectations, and operator workflows
- `signal-map.md`
  - symptom-to-signal routing table for common runtime and observability issues
- `performance-troubleshooting.md`
  - performance investigation workflow and likely bottleneck classes
- `runtime-health-and-runbook.md`
  - readiness, liveness, shutdown, health probes, and operator checklists

## Boundaries

- `logging/*` explains how logs, trace IDs, and access records are generated and maintained.
- `metrics/*` explains how resource metrics are registered, collected, buffered, and persisted.
- `observability/*` explains how to use those signals together to localize faults, confirm health, and guide operations decisions.
