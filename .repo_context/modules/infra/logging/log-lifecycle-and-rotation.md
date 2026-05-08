# Log Lifecycle And Rotation

## Document Metadata

- Status:
  - `active`
- Doc type:
  - behavior note | submodule reference
- Primary code paths:
  - `src/datasystem/common/log/logging.h`
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/log/log_manager.h`
  - `src/datasystem/common/log/log_manager.cpp`
  - `src/datasystem/common/log/failure_handler.h`
  - `src/datasystem/common/log/failure_handler.cpp`
- Last verified against source:
  - `2026-05-06`
- Related design docs:
  - `.repo_context/modules/infra/logging/design.md`
  - `.repo_context/modules/infra/logging/access-recorder.md`
- Related tests:
  - `//tests/ut/common/log:logging_test`
  - `//tests/ut/common/log:failure_handler_test`
  - `//tests/st/common/log:logging_free_test`

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
- `tests/ut/common/log/BUILD.bazel`
- `tests/st/common/log/BUILD.bazel`

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
- Verified in `provider.cpp`:
  - the early worker log prefix resolves the pod identifier from `POD_IP`, then `POD_NAME`, then `HOSTNAME`
- Verified in `logging.cpp` and `file_util.cpp`:
  - after `log_dir` is resolved, logging refreshes the pod identifier through `<log_dir>/env`;
  - if `POD_IP` exists, it is written to `pod_ip` in that file; if not, the persisted `pod_ip` is used before falling
    back to the provider value;
  - the persisted env file is guarded by a directory `flock` and atomic replace so multiple SDK/client processes can
    share the same log directory without dropping either tracked key or leaving a companion lock file.
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

## Compatibility And Operations Notes

- Stability-sensitive behavior:
  - log file naming patterns must stay aligned with `LogManager` rolling and pruning regexes;
  - `DATASYSTEM_*` environment overrides are externally visible operational contracts for client processes;
  - `container.log` remains the expected crash-path output location beneath the resolved log directory.
- Operations implications:
  - retention and compression settings affect both ordinary logs and monitor-style files;
  - bad directory permissions or path overrides can break startup and crash logging together;
  - changing startup order can create partial observability failures even when the service itself still runs.

## Verification Hints

- Fast source checks:
  - confirm startup ordering and env override behavior in `src/datasystem/common/log/logging.cpp`;
  - confirm rolling, compression, and pruning patterns in `src/datasystem/common/log/log_manager.cpp`;
  - confirm crash-path behavior in `src/datasystem/common/log/failure_handler.cpp`.
- Fast validation targets:
  - `bazel test //tests/ut/common/log:failure_handler_test`
  - `bazel test //tests/ut/common/log:logging_test`
- Manual validation:
  - start one process with a writable temp log dir, emit logs, force rotation conditions, and confirm expected file families plus `container.log` behavior.

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

## Update Rules For This Document

- Keep this file focused on startup, configuration, maintenance loop behavior, and crash-path handling instead of duplicating the full architecture in `design.md`.
- Update this file when startup order, flag or env semantics, rolling or pruning patterns, or failure-handler behavior changes.
- If an operational statement depends on an unverified deployment path, mark it as pending verification rather than generalizing it.
