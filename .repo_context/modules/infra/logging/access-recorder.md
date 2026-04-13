# Access Recorder

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

## Bugfix And Review Notes

- Good first files when access logs are missing or malformed:
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/access_point.def`
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
- Common risks:
  - changing key ordering or names in `access_point.def` can break downstream parsing or dashboards;
  - moving `Record()` behind conditional branches can silently drop latency samples on error paths.
