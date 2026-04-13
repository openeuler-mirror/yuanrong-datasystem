# Performance Investigation

## Metadata

- Status:
  - `active`
- Playbook scope:
  - performance investigation | diagnosis workflow
- Owning module or area:
  - `modules/infra/observability`
- Primary context docs:
  - `.repo_context/modules/infra/observability/performance-troubleshooting.md`
  - `.repo_context/modules/infra/logging/design.md`
  - `.repo_context/modules/infra/metrics/design.md`
  - `.repo_context/modules/quality/tests-and-reproduction.md`
- Last verified against source:
  - `2026-04-13`

## When To Use

- Use when:
  - latency, throughput, backlog, or resource-usage regressions are the main symptom.
- Do not use when:
  - the problem is clearly a hard startup or config failure with no performance dimension.

## Investigation Flow

1. Define the regression precisely:
   - latency up
   - throughput down
   - timeout rate up
   - backlog or queue growth
2. Align one time window across logs and monitor files.
3. Check access logs for request-path timing changes.
4. Check resource monitor files for memory, disk, thread-pool, or backend-rate changes.
5. Check ordinary logs for warnings about background tasks, flush, rotation, or startup degradation.
6. Choose the most likely bottleneck class:
   - request path
   - exporter or file I/O
   - thread pool
   - backend or metadata
7. Reproduce with the narrowest matching ST or perf test.

## Minimal Evidence Set

- One representative slow path or operation
- Matching access-log window
- Matching resource-monitor window
- Relevant warnings from ordinary logs
- Config values that affect monitor interval, log flushing, or backend choice

## Notes For `set/get` Latency Questions

- Use access logs first:
  - current `set/get` request timing is primarily available from the access-recorder path, not from a repository-wide typed percentile metric stream.
- Do not promise live percentiles from common metrics:
  - the current common metrics subsystem is resource-monitor oriented and file-backed, so `p95/p99` answers usually require log-window analysis or perf reproduction.
- Distinguish two different questions early:
  - `the request path became slow`
  - `the observability path became delayed or incomplete`
- If the user needs durable dashboard-grade latency tracking:
  - treat that as a feature gap, not just an investigation task, and review `modules/infra/metrics/design.md` before proposing changes.

## Exit Criteria

- Stop triage and switch to implementation or bugfix work when:
  - one module or subsystem is now the clear bottleneck owner;
  - the evidence distinguishes request-path cost from observability-path lag;
  - a concrete reproduction or validation target is chosen.
