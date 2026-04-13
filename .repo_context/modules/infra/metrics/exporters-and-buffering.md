# Exporters And Buffering

## Document Metadata

- Status:
  - `active`
- Doc type:
  - behavior note | submodule reference
- Primary code paths:
  - `src/datasystem/common/metrics/metrics_exporter.h`
  - `src/datasystem/common/metrics/metrics_exporter.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
  - `src/datasystem/common/log/access_recorder.cpp`
- Last verified against source:
  - `2026-04-13`
- Related design docs:
  - `.repo_context/modules/infra/metrics/design.md`
  - `.repo_context/modules/infra/logging/design.md`
- Related tests:
  - `//tests/ut/common/log:hard_disk_exporter_test`
  - `//tests/st/client/kv_cache:kv_client_log_monitor_test`

## Scope

- Paths:
  - `src/datasystem/common/metrics/metrics_exporter.h`
  - `src/datasystem/common/metrics/metrics_exporter.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
- Why this document exists:
  - explain how metric output is buffered, flushed, written to disk, and shared with logging-side access records.

## Primary Source Files

- `src/datasystem/common/metrics/metrics_exporter.h`
- `src/datasystem/common/metrics/metrics_exporter.cpp`
- `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h`
- `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
- `src/datasystem/common/log/access_recorder.cpp`
- `tests/ut/common/log/BUILD.bazel`
- `tests/st/client/kv_cache/BUILD.bazel`

## Exporter Model

- Verified from `metrics_exporter.h/.cpp`:
  - exporter has:
    - one active buffer
    - one buffer pool queue
    - one flush thread
  - `WriteMessage()` buffers data until a size threshold is reached.
  - `SubmitWriteMessage()` forces the active buffer into the queue.
  - flush thread inherits the current trace ID through `Trace::SetTraceNewID(...)`.
- Practical effect:
  - metric emission is asynchronous and batched, not line-by-line synchronous file I/O.

## HardDiskExporter Behavior

- Verified from `hard_disk_exporter.*`:
  - `Init(filePath)` creates the file, captures pod/hostname, and starts exporter infrastructure.
  - `Send()` prefixes messages with the standard log prefix format using `ConstructLogPrefix(...)`.
  - `FlushThread()` writes queued messages to the file descriptor and may rotate the log file.
  - rotation is driven by `max_log_size`.
  - pruning of rotated files is driven by `max_log_file_num`.

## Logging Coupling

- Important coupling:
  - `HardDiskExporter` is shared by:
    - resource metrics
    - access/performance monitor logs via `AccessRecorderManager`
- Review implication:
  - exporter changes are observability-wide changes, not metrics-local tweaks.

## Config Surface

- Exporter behavior depends on:
  - `log_dir`
  - `logfile_mode`
  - `max_log_size`
  - `max_log_file_num`
  - `log_monitor_exporter`

## Compatibility And Operations Notes

- Stability-sensitive behavior:
  - file naming and rotation behavior must stay aligned with downstream pruning logic and operational file discovery;
  - `HardDiskExporter` output format is shared between resource metrics and access-monitor logs;
  - flush-thread trace restoration affects debugging continuity for async exporter work.
- Operations implications:
  - changing buffer thresholds alters latency, batch size, and memory usage together;
  - changing rotation or pruning affects both metrics files and logging-side access outputs;
  - exporter failures can surface first in either metrics or logging depending on which path an operator inspects.

## Verification Hints

- Fast source checks:
  - confirm buffer threshold and queue behavior in `src/datasystem/common/metrics/metrics_exporter.cpp`;
  - confirm file creation, rolling, and pruning in `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`;
  - confirm logging-side coupling in `src/datasystem/common/log/access_recorder.cpp`.
- Fast validation targets:
  - `bazel test //tests/ut/common/log:hard_disk_exporter_test`
- Manual validation:
  - generate enough output to force batching and rotation, then confirm both monitor-style output and logging-side access output still persist as expected.

## Bugfix And Review Notes

- Good first files when output is delayed, truncated, or not persisted:
  - `src/datasystem/common/metrics/metrics_exporter.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
  - `src/datasystem/common/log/access_recorder.cpp`
- Common risks:
  - changing buffer flush conditions can alter latency and log volume together;
  - breaking `HardDiskExporter` rotation logic affects both metric files and access-monitor files.

## Update Rules For This Document

- Keep this file focused on buffering, flush behavior, hard-disk persistence, and logging coupling instead of repeating full metrics architecture from `design.md`.
- Update this file when buffer thresholds, queue semantics, file lifecycle behavior, or shared exporter coupling changes.
- If a change claim depends on deployment behavior not yet confirmed from source or tests, record it as pending rather than broadening it.
