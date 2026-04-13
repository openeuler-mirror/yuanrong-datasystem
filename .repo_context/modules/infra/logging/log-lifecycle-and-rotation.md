# Log Lifecycle And Rotation

## Scope

- Paths:
  - `src/datasystem/common/log/logging.h`
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/log/log_manager.h`
  - `src/datasystem/common/log/log_manager.cpp`
  - `src/datasystem/common/log/failure_handler.h`
  - `src/datasystem/common/log/failure_handler.cpp`
- Why this document exists:
  - explain startup, configuration overrides, background maintenance, file rotation, and crash-path logging.

## Primary Source Files

- `src/datasystem/common/log/logging.h`
- `src/datasystem/common/log/logging.cpp`
- `src/datasystem/common/log/log_manager.h`
- `src/datasystem/common/log/log_manager.cpp`
- `src/datasystem/common/log/failure_handler.h`
- `src/datasystem/common/log/failure_handler.cpp`

## Startup Flow

- Verified from `logging.cpp`:
  - `Logging::Start()` initializes client-specific config from environment when `isClient=true`.
  - it sets `log_filename`, initializes the provider, starts `LogManager`, then initializes `AccessRecorderManager`.
  - if logging initialization fails, `FLAGS_log_monitor` is disabled.
- Review implication:
  - startup failures can degrade observability partially instead of failing the whole process, so missing monitor logs do not necessarily mean the main service startup failed.

## Background Tasks

- Verified from `log_manager.cpp`:
  - background loop does:
    - file compression
    - file rolling
    - monitor-log flush via `AccessRecorderManager::SubmitWriteMessage()`
  - rolling and pruning are controlled by `max_log_file_num`, `log_retention_day`, and `max_log_size`.
  - access/resource/client-access log files are also included in the rolling/compression patterns.

## Environment Variables And Flags

- Verified in `logging.h/.cpp`:
  - `DATASYSTEM_CLIENT_LOG_NAME`
  - `DATASYSTEM_CLIENT_ACCESS_LOG_NAME`
  - `DATASYSTEM_CLIENT_LOG_DIR`
  - `DATASYSTEM_CLIENT_MAX_LOG_SIZE`
  - `DATASYSTEM_MAX_LOG_FILE_NUM`
  - `DATASYSTEM_LOG_COMPRESS`
  - `DATASYSTEM_LOG_RETENTION_DAY`
  - `DATASYSTEM_LOG_TO_STDERR`
  - `DATASYSTEM_ALSO_LOG_TO_STDERR`
  - `DATASYSTEM_STD_THRESHOLD`
  - `DATASYSTEM_LOG_ASYNC_ENABLE`
  - `DATASYSTEM_LOG_ASYNC_QUEUE_SIZE`
  - `DATASYSTEM_LOG_V`
  - `DATASYSTEM_MIN_LOG_LEVEL`
  - `DATASYSTEM_LOG_MONITOR_ENABLE`
- Useful runtime flags defined in `logging.cpp`:
  - `max_log_file_num`
  - `log_compress`
  - `log_retention_day`
  - `logbufsecs`
  - `logfile_mode`
  - `max_log_size`
  - `logtostderr`
  - `alsologtostderr`
  - `stderrthreshold`
  - `minloglevel`
  - `log_async_queue_size`

## Failure Logging

- Verified from `failure_handler.cpp`:
  - absl symbolizer and failure signal handler are installed through `InstallFailureSignalHandler`.
  - `FailureWriter` flushes normal logs on null input and writes failure text to `<resolved log dir>/container.log`.
- Review note:
  - crash-path logging bypasses some normal logging paths by writing directly through `Logging::WriteLogToFile`.
- Pending verification:
  - whether all runtime startup paths install the failure signal handler consistently.

## Bugfix And Review Notes

- Good first files when log files are not rotating, compressing, or flushing:
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/log/log_manager.cpp`
  - `src/datasystem/common/log/failure_handler.cpp`
- Common risks:
  - changing rotation/compression rules affects both ordinary logs and monitor/resource logs;
  - changing startup order between provider init, log manager start, and access-recorder manager init can create partial observability failures.
