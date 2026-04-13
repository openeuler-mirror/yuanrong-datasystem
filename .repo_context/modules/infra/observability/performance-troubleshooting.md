# Performance Troubleshooting

## Document Metadata

- Status:
  - `active`
- Doc type:
  - behavior note | troubleshooting reference
- Primary code paths:
  - `src/datasystem/worker/worker_main.cpp`
  - `src/datasystem/common/log/*`
  - `src/datasystem/common/metrics/*`
  - `tests/perf`
  - `tests/st`
- Last verified against source:
  - `2026-04-13`
- Related design docs:
  - `.repo_context/modules/infra/observability/diagnosis-and-operations.md`
  - `.repo_context/modules/infra/logging/design.md`
  - `.repo_context/modules/infra/metrics/design.md`
- Related tests:
  - `.repo_context/modules/quality/tests-and-reproduction.md`

## Scope

- Paths:
  - `src/datasystem/worker`
  - `src/datasystem/common/log`
  - `src/datasystem/common/metrics`
  - `tests/perf`
  - `tests/st`
- Why this document exists:
  - record a repeatable way to localize latency, throughput, backlog, and resource-usage regressions using the signals already exposed by the repository.

## Bottleneck Classes

- Request-path contention:
  - access logs show rising elapsed time while basic health artifacts remain good.
- Background queue pressure:
  - monitor files or logs suggest exporter, flush, or async queue delays.
- Thread-pool saturation:
  - worker or master service thread-pool metrics degrade before full request failure.
- Disk or file-maintenance pressure:
  - compression, rolling, or heavy file I/O correlate with latency spikes.
- Backend or metadata pressure:
  - ETCD, OBS, or other backend success-rate families degrade alongside request behavior.

## First Investigation Order

1. Confirm the symptom window and whether the issue is latency, throughput, timeout, or backlog growth.
2. Check ordinary logs for startup or steady-state warnings first.
3. Check access logs and resource monitor files for the same time window.
4. Decide whether the first visible signal is request-path, exporter, thread-pool, disk, or backend related.
5. Reproduce with the narrowest matching ST or perf case from `tests`.

## Evidence Priorities

- Access or performance logs:
  - best for request-path latency and operation-specific slowdown.
- Resource monitor files:
  - best for memory, disk, thread-pool, and backend success-rate trends.
- Ordinary logs:
  - best for init failures, warnings, and background-task anomalies.
- Perf and ST tests:
  - best for controlled reproduction after the signal class is known.

## Current Signal Limits For `set/get`

- Verified request-path timing surface today:
  - client `set/get` style operations are primarily timed through `AccessRecorder` and related request-path logs, for example `src/datasystem/client/kv_cache/kv_client.cpp` and `src/datasystem/common/log/access_recorder.cpp`.
- Verified resource signal surface today:
  - `src/datasystem/common/metrics/res_metric_collector.cpp` collects periodic resource-style strings and flushes them through `HardDiskExporter`, not through typed request histograms.
- Verified observability gap today:
  - the repository does not expose a built-in `/metrics` style scrape endpoint under `src/datasystem`;
  - the common metrics subsystem does not currently publish request-level `set/get` latency buckets, percentile series, or byte counters in a typed metrics protocol.
- Practical implication for investigations:
  - when a user asks whether `set/get` `p95` or `p99` suddenly regressed, the first answer must come from access-log samples or perf tests, not from a live percentile metric stream;
  - correlating slow requests with thread-pool, disk, or backend pressure still depends on aligning access logs with monitor files and ordinary logs.

## Priority Improvements If Richer `set/get` Metrics Are Needed

- Per-operation latency histograms:
  - add typed latency buckets for `KV set/get`, `Object put/get`, and major batch variants instead of relying only on per-request access records.
- Request volume and byte counters:
  - add counters for request count, failure count, read bytes, and write bytes on the same operation families.
- Exportable metrics endpoint:
  - provide one repository-owned scrape surface so dashboards and alerting do not depend only on local files.
- Contracted dashboards and tests:
  - treat metric names, labels, and dashboard queries as compatibility-sensitive artifacts and validate them in tests, not only in ad hoc manual checks.

## Common Pitfalls

- Treating exporter lag as request-path regression.
- Ignoring monitor interval and buffer delay when comparing timestamps.
- Looking only at one log family when the shared exporter might be the bottleneck.
- Skipping reproduction and trying to infer all behavior from one noisy live sample.

## Update Rules For This Document

- Keep this file focused on performance-localization workflow, not on duplicating metric-family or logging implementation details.
- Update it when new recurring bottleneck classes or better reproduction routes are identified.
