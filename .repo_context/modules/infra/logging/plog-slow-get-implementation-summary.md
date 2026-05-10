# PLOG Slow Get Implementation Summary

Date: `2026-05-11`

## Scope

- Branch:
  - `codex/plog-slow-get`
- Main implementation commit:
  - `8d53a7e3 Add PLOG slow get diagnostics`
- Purpose:
  - when a key Get latency segment exceeds its threshold, print the diagnostic log through `PLOG` so it bypasses
    `log_rate_limit` request sampling;
  - when the segment is below threshold, keep the same log body behind `VLOG(1)`, so production `v=0` stays quiet on fast
    requests.

## Implemented Behavior

- The logging module now exposes `PLOG/PLOG_IF`.
- `PLOG_IF_OR_VLOG` keeps slow/fast log branching at call sites without duplicating the log message body.
- `PLOG` passes a `forceLog` flag through `LogMessage` and `LogMessageImpl` so only the rate-limiter decision is bypassed.
- Normal `LOG`, `VLOG`, min-log-level behavior, async spdlog output, rotation, and compression paths are unchanged.
- Implemented Get thresholds:

| Segment | Threshold |
| --- | ---: |
| Get local processing | `1000 us` |
| QueryMeta RPC total | `1000 us` |
| Master local QueryMeta | `2000 us` |
| Remote worker RPC total | `2000 us` |
| Remote worker local total | `2000 us` |
| URMA total | `1000 us` |
| Response construction/return | deferred |

## Log Carriers

Reused existing carriers:

- `[Get] Master query done`
- `[Get] Remote done`
- `Remote get success`
- `[Get/RemotePull] finish`
- `[URMA_ELAPSED_TOTAL]`
- `QueryMeta done`

Added narrow carriers where existing logs did not expose a completion cost:

- `[Get] Local processing done`
- `[Get] ReturnToClient done`
- `[GetObjectRemote] finish`

## Change Size

- Logging module diff:
  - `21` insertions
  - `11` deletions
- The business-path changes add only local timers, named threshold constants, and `VLOG/PLOG` branching at existing
  diagnosis points.
- Formatting stays inside the chosen branch, so fast requests do not pay formatting cost for slow logs.

## Validation

Validation followed the `$ds-test` remote workflow on:

- host:
  - `ssh marck@1.95.199.126 -p 22224`
- remote repository:
  - `/home/marck/workspace/yuanrong-datasystem`

Completed command:

- `cmake --build . --target ds_spdlog common_log -j 98`
  - result: passed
  - log: `/home/marck/workspace/yuanrong-datasystem/logs/plog-common-log-build-20260511-030142.log`

Blocked commands:

- `bash build.sh -j 98 -X off -t build`
  - result: failed
  - log: `/home/marck/workspace/yuanrong-datasystem/logs/plog-build-20260511-025748.log`
  - failure summary: early `tests/ut/flags_ut` link failure with missing C++ standard-library symbols such as
    `operator delete(void*, unsigned long)` and `__cxa_begin_catch`.
- `cmake --build . --target ds_spdlog common_rdma worker_object_cache master_object_cache datasystem_worker_bin -j 98`
  - result: failed
  - log: `/home/marck/workspace/yuanrong-datasystem/logs/plog-target-build-20260511-025844.log`
  - failure summary: `zmq_plugin` link failed with `DSO missing from command line`.
- Fresh build directory with `build.sh`
  - result: failed
  - log: `/home/marck/workspace/yuanrong-datasystem/logs/plog-build-fresh-20260511-030208.log`
  - failure summary: same early C++ standard-library link failure as the default build.

## Current Conclusion

- The PLOG logging-module target was validated by the remote `common_log` build.
- Full build and worker/master target validation are blocked by the remote link environment, so end-to-end validation cannot
  be claimed yet.
- After the remote link environment is repaired, rerun full build and the relevant Get-path tests, then tune thresholds with
  production distributions.
