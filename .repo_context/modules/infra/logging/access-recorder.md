# Access Recorder

## Document Metadata

- Status:
  - `active`
- Doc type:
  - behavior note | submodule reference
- Primary code paths:
  - `src/datasystem/common/log/access_recorder.h`
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/access_point.def`
  - `src/datasystem/common/metrics/hard_disk_exporter/*`
- Last verified against source:
  - `2026-04-13`
- Related design docs:
  - `.repo_context/modules/infra/logging/design.md`
  - `.repo_context/modules/infra/logging/log-lifecycle-and-rotation.md`
- Related tests:
  - `//tests/ut/common/log:log_performance_test`
  - `//tests/ut/common/log:hard_disk_exporter_test`
  - `//tests/st/common/log:kv_cache_log_performance_test`

## Scope

- Paths:
  - `src/datasystem/common/log/access_recorder.h`
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/access_point.def`
- Why this document exists:
  - explain how access/performance records are keyed, timed, classified, and exported.

## Primary Source Files

- `src/datasystem/common/log/access_recorder.h`
- `src/datasystem/common/log/access_recorder.cpp`
- `src/datasystem/common/log/access_point.def`
- `src/datasystem/common/log/logging.h`
- `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h`
- `tests/ut/common/log/BUILD.bazel`
- `tests/st/common/log/BUILD.bazel`

## Responsibilities

- Verified:
  - `AccessRecorderKey` values are generated from `access_point.def`.
  - keys are classified into three access channels:
    - `CLIENT`
    - `ACCESS`
    - `REQUEST_OUT`
  - `Record()` computes elapsed microseconds from recorder construction to emission.
  - actual emission goes through `Logging::AccessRecorderManagerInstance()->LogPerformance(...)`.
  - if a recorder is destroyed without `Record()` being called, it logs an error.

## Recorder Lifecycle

- Typical flow:
  - construct an `AccessRecorder` near the start of an operation;
  - optionally attach request or response metadata;
  - call `Record()` when the operation completes;
  - `Record()` computes elapsed time and forwards a structured line to the manager/exporter path.
- Failure mode:
  - if code exits early and the recorder never records, destruction emits an error, which is useful for spotting instrumentation bugs or abandoned paths.

## Key Space And Operation Families

- Verified key families in `access_point.def`:
  - client-facing KV/Object/Stream/Hetero operations
  - worker-side POSIX/object/stream operations
  - ETCD outbound operations
- Review implication:
  - adding a new externally meaningful operation without a matching key often leaves observability incomplete even when functionality works.

## Export Path And Coupling

- Verified:
  - `AccessRecorderManager` owns the log-performance export path.
  - `AccessRecorderManager` uses `HardDiskExporter`, so access/performance logs depend on the metrics exporter infrastructure.
  - monitor/access log flush is later driven by the logging lifecycle path, not by each recorder doing synchronous file writes.
- Cross-module implication:
  - logging and metrics are coupled here; exporter bugs can show up as missing access logs.

## Compatibility And Change Notes

- Stability-sensitive behavior:
  - key ordering and names in `access_point.def` are operationally visible;
  - the pipe-delimited record shape produced by `AccessRecorderManager::LogPerformance(...)` should be treated as compatibility-sensitive;
  - client vs. access vs. request-out channel routing affects file names and downstream parsing.
- Safe change guidance:
  - add new keys in a way that preserves meaning for existing log consumers;
  - review downstream dashboards, parsers, or runbooks before changing record fields or ordering;
  - keep `Record()` coverage on success and error paths to avoid silent observability gaps.

## Verification Hints

- Fast source checks:
  - confirm new keys or channel mappings in `src/datasystem/common/log/access_point.def`;
  - confirm record formatting in `src/datasystem/common/log/access_recorder.cpp`;
  - confirm exporter behavior in `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`.
- Fast validation targets:
  - `bazel test //tests/ut/common/log:log_performance_test`
  - `bazel test //tests/ut/common/log:hard_disk_exporter_test`
- Manual validation:
  - trigger one representative operation and confirm the expected record lands in `access`, `request_out`, or `ds_client_access` output.

## Bugfix And Review Notes

- Good first files when access logs are missing or malformed:
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/access_point.def`
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
- Common risks:
  - changing key ordering or names in `access_point.def` can break downstream parsing or dashboards;
  - moving `Record()` behind conditional branches can silently drop latency samples on error paths.

## Update Rules For This Document

- Keep this file focused on access-recorder lifecycle, key space, and exporter coupling instead of repeating full module architecture from `design.md`.
- Update this file when access-key families, record layout, exporter ownership, or relevant verification entrypoints change.
- If a behavior claim is not yet confirmed in source, move it to a clearly marked pending note rather than broadening the claim.
