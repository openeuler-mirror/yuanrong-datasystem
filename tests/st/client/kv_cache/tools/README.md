# KuaiRand trace replay

`prepare_kuairand_trace.py` converts official KuaiRand log CSV files into the small, strict format consumed by
`kv_client_eviction_rebalance_telemetry_test.cpp`. The dataset itself is not committed.

## Prepare a trace

Download and extract KuaiRand-Pure from the [official KuaiRand dataset page](https://kuairand.com/). KuaiRand is
published under CC BY-SA 4.0; retain its attribution and license when sharing derived traces.

The official KuaiRand-Pure archive used during validation had MD5
`0820331067a3784d9691136f772b35a7`. Normalize the three Pure interaction logs with:

```bash
python3 tests/st/client/kv_cache/tools/prepare_kuairand_trace.py \
  --input /path/to/KuaiRand-Pure/data/log_standard_4_08_to_4_21_pure.csv \
  --input /path/to/KuaiRand-Pure/data/log_standard_4_22_to_5_08_pure.csv \
  --input /path/to/KuaiRand-Pure/data/log_random_4_22_to_5_08_pure.csv \
  --output /tmp/kuairand_pure_100k.trace \
  --max-events 100000 \
  --source-label KuaiRand-Pure \
  --archive-md5 0820331067a3784d9691136f772b35a7
```

The converter validates required columns, takes a deterministic uniform positional sample across the combined input,
sorts it by `time_ms`, and writes atomically. An event is `engaged=1` when any of `is_click`, `is_like`, `is_follow`,
`is_comment`, `is_forward`, or `long_view` is non-zero.

## Run the telemetry ST

```bash
DS_TLM_KUAIRAND_TRACE=/tmp/kuairand_pure_100k.trace \
DS_TLM_VARIANT=kuairand_heat \
DS_TLM_EXTRA_WORKER_GFLAGS='-eviction_strategy=heat -rebalance_strategy=heat' \
  build/tests/st/ds_st_kv_cache \
  --gtest_filter='*WorkloadTelemetryTest.*Workload'
```

Recall replays all selected exposures. Fine-rank is an engaged-event proxy and replays only positive-feedback events;
KuaiRand-Pure does not provide a complete online candidate slate, so this is not a literal ranking-stage replay. For
each topology, the most frequent distinct videos that fit the existing fixed DS key universe are mapped deterministically
to keys, and the selected events keep chronological order. Object sizes and placement remain the test's controlled
distribution, isolating the access-locality change from capacity changes.

The CSV appends `traceSource` and `traceEvents`. Without `DS_TLM_KUAIRAND_TRACE`, the existing deterministic synthetic
workload remains the default, so normal CI has no dataset or extra preprocessing dependency.

## Normalized format

```text
# kuairand-trace-v1
# source=KuaiRand-Pure
time_ms,video_id,engaged
1649433600000,12345,0
1649433600100,67890,1
```

The C++ parser rejects a missing format marker/header, malformed values, engagement values outside `0/1`, empty input,
or decreasing timestamps.
