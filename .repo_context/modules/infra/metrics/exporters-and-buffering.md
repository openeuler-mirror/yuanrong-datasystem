# Exporters And Buffering

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

## Bugfix And Review Notes

- Good first files when output is delayed, truncated, or not persisted:
  - `src/datasystem/common/metrics/metrics_exporter.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
  - `src/datasystem/common/log/access_recorder.cpp`
- Common risks:
  - changing buffer flush conditions can alter latency and log volume together;
  - breaking `HardDiskExporter` rotation logic affects both metric files and access-monitor files.
