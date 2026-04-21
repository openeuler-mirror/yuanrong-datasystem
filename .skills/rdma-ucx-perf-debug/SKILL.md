---
name: rdma-ucx-perf-debug
description: Use this skill when diagnosing RDMA/UCX performance, latency, throughput, submit/flush bottlenecks, UCP worker scaling, batch get/pull remote latency, or UCX resource lifetime crashes in the datasystem codebase.
---

# RDMA/UCX Performance Debugging Skill

## Trigger Scenarios

Use this skill when any of the following happens:

- RDMA/UCX path shows lower-than-expected bandwidth or high latency.
- Small block transfers, especially 64KB-256KB, are much slower than large block transfers.
- Increasing concurrency causes throughput to drop or p99 latency to grow nonlinearly.
- `ucp_put_nbx`, `ucp_ep_flush_nbx`, `ucp_worker_progress`, or endpoint/rkey logic is suspected.
- Multi-worker or multi-endpoint scaling behaves nonlinearly.
- Benchmark results exceed physical bandwidth or look suspicious.
- UCX reports shared memory, rkey unpack, endpoint, or flush callback errors.
- Async submit/flush changes require UT or lifecycle validation.

## Core Principles

- Separate end-to-end latency into layers before optimizing.
- Do not assume UCX put/flush is the only bottleneck.
- Measure submit queue wait, RDMA write total, flush callback, RPC, metadata, request construction, and response merge separately.
- For RDMA put, receiver `ucp_worker_progress` usually does not make data arrive; hardware and sender flush completion matter more.
- After async submit, `Write()` usually means “request accepted/enqueued”, not “data reached remote memory”.
- Closed-loop benchmarks amplify long-tail latency into throughput loss.
- Resource teardown order matters after async submit/flush.

## SOP

### Step 1: Confirm Workload Shape

Collect:

- block size
- objects per batch
- client thread count
- worker count
- total key count
- whether batch path is enabled
- whether cross-node transfer is used
- blocking or nonblocking RDMA wait mode

Useful questions:

- Is the problematic case small-block-heavy?
- Does latency scale with object count per batch?
- Does throughput drop when concurrency increases?
- Is the benchmark closed-loop?

### Step 2: Check Benchmark Validity

Look for false bandwidth or timing artifacts.

```bash
rg -n "throughput|latency|p99|BatchGet|RetrieveRemotePayload|WaitFor|WaitToFinish" cli/benchmark src tests
```

Check:

- whether timing includes all remote data visibility waits
- whether RPC returns before data is actually visible
- whether event wait is really performed
- whether batch size/count/key count causes local cache hits
- whether total key count is large enough to avoid repeated hot keys

### Step 3: Add or Inspect RDMA-Layer Metrics

Use the project-standard perf framework, not ad-hoc latency loggers.

Instrumentation sources:

- Define keys in `src/datasystem/common/perf/perf_point.def`.
- Use `PerfPoint point(PerfKey::SOME_KEY);` for scoped latency.
- Call `point.Record();` when the measured scope should end before destruction or before an early return.
- Use `PerfPoint::RecordElapsed(PerfKey::SOME_KEY, value);` only when the value is already computed, for example queue wait time or a counted value such as batch size.
- Include `datasystem/common/perf/perf_manager.h` where needed.

Minimum RDMA perf keys:

```text
RDMA_TOTAL_WRITE
RDMA_GATHER_WRITE
RDMA_WAIT_TO_FINISH
RDMA_REGISTER_SEGMENT
RDMA_UCP_WORKER_WRITE_TOTAL
RDMA_UCP_WORKER_SUBMIT_QUEUE_WAIT
RDMA_UCP_WORKER_FLUSH_CALLBACK
RDMA_UCP_WORKER_FLUSH_BATCH_SIZE
```

Search commands:

```bash
rg -n "PerfKey::RDMA|RDMA_UCP|PerfPoint\\(" src/datasystem/common/rdma src/datasystem/worker/object_cache tests
sed -n '/Ucp Rdma/,/Log/p' src/datasystem/common/perf/perf_point.def
```

Perf export formats:

- Periodic logs from `PerfManager::Tick()` or explicit `PrintPerfLog()` look like `[Perf Log]:` followed by one line per key: `KEY_NAME: {"count":N,"minTime":...,"maxTime":...,"totalTime":...,"avgTime":...,"maxFrequency":...}`.
- `PerfClient::GetPerfLog("worker", out)` exports `out[key]["count"]`, `min_time`, `max_time`, `total_time`, `avg_time`, and `max_frequency`.
- Time values are nanoseconds unless a key intentionally records a value. Current special case: `RDMA_UCP_WORKER_FLUSH_BATCH_SIZE` records batch size through the same fields, so interpret `avgTime` / `avg_time` as average batch size, not time.

Log extraction template for the latest perf block:

```bash
awk '/\\[Perf Log\\]:/{block=""; capture=1; next} capture && NF{block=block $0 "\\n"} END{printf "%s", block}' \
  datasystem_worker.INFO.log | grep -E 'RDMA_|WORKER_.*REMOTE|WORKER_.*BATCH'
```

Include compressed logs:

```bash
zgrep -h -A200 "\\[Perf Log\\]:" datasystem_worker.INFO* | grep -E 'RDMA_|WORKER_.*REMOTE|WORKER_.*BATCH' | tail -80
```

### Step 4: Interpret RDMA Metrics

If `RDMA_UCP_WORKER_SUBMIT_QUEUE_WAIT` is high:

- submit thread is overloaded
- too few workers
- request production rate exceeds submit capacity
- batching/dequeue policy may be insufficient
- closed-loop may amplify queueing

If `RDMA_UCP_WORKER_WRITE_TOTAL` is high:

- UCX write path, endpoint/rkey lookup, or submit logic is blocking/contended
- worker count may be too high or too low
- UCX internal locks or QP pressure may be involved
- try fewer workers, submit thread, credit, or UCX env tuning

If `RDMA_UCP_WORKER_FLUSH_CALLBACK` is high:

- remote visibility / NIC completion / flush pressure is high
- too many small flushes
- flush batching may help
- check `RDMA_UCP_WORKER_FLUSH_BATCH_SIZE`

If RDMA metrics are low but end-to-end is high:

- bottleneck is likely above RDMA: RPC, metadata query, request construction, response merge, lock contention, or benchmark closed-loop scheduling

### Step 5: Analyze Worker Scaling

Run controlled tests:

- 1 worker
- 2 workers
- 4 workers
- 8 workers only if needed

Keep everything else unchanged. Compare avg latency, p99 latency, throughput, `RDMA_UCP_WORKER_SUBMIT_QUEUE_WAIT`, `RDMA_UCP_WORKER_WRITE_TOTAL`, and `RDMA_UCP_WORKER_FLUSH_CALLBACK`.

Interpretation:

- 1 -> 2 improves: single worker was bottleneck.
- 2 -> 4 improves: more parallel submit helps.
- 4/8 worsens: UCX/NIC/internal lock/QP pressure or scheduling overhead may dominate.
- queue wait drops but put submit rises: work is spread out but UCX submit contention increased.

### Step 6: Evaluate Submit Thread Design

If business threads block in `ucp_put_nbx`, consider submit-thread architecture:

- `Write/WriteN` only enqueue request.
- Submit thread performs dequeue, get/create endpoint, unpack rkey, `ucp_put_nbx`, enqueue flush context, progress worker, and callback notify event.

Required UTs:

- `Write` notifies event through submit loop.
- `WriteN` notifies event through submit loop.
- `Write` fails if submit thread is stopped.
- endpoint removal works across all workers.
- async write tests wait for event/data before asserting side effects.

### Step 7: Check Flush Strategy

Avoid assuming fewer flushes always improves latency.

Compare:

- per-put flush
- flush batching by endpoint
- opportunistic wait before flush
- credit limiting outstanding submits

Track:

```text
RDMA_UCP_WORKER_FLUSH_BATCH_SIZE
RDMA_UCP_WORKER_SUBMIT_QUEUE_WAIT
RDMA_UCP_WORKER_FLUSH_CALLBACK
```

Interpretation:

- batch size increases but latency unchanged: flush count is not the dominant bottleneck.
- callback latency drops but end-to-end unchanged: bottleneck moved upward.
- large callback max: long-tail completion may hurt closed-loop throughput.

### Step 8: Inspect Upper-Layer Batch Get Path

If RDMA layer looks healthy, add or inspect coarse upper-layer metrics.

Important metrics:

```text
WORKER_PROCESS_GET_OBJECT
WORKER_PROCESS_GET_OBJECT_REMOTE
WORKER_PULL_REMOTE_DATA
WORKER_PULL_REMOTE_DATA_FROM_WORKER
WORKER_QUERY_META
WORKER_CONSTRUCT_BATCH_GET_REQ
WORKER_BATCH_GET_CONSTRUCT_AND_SEND
WORKER_BATCH_REMOTE_GET_RPC
WORKER_SERVER_BATCH_GET_REMOTE
WORKER_SERVER_GET_REMOTE
WORKER_SERVER_GET_REMOTE_READ
WORKER_SERVER_GET_REMOTE_IMPL
WORKER_SERVER_GET_REMOTE_WRITE
WORKER_SERVER_GET_REMOTE_SENDPAYLOAD
```

Search commands:

```bash
rg -n "PerfKey::WORKER_.*(REMOTE|BATCH|QUERY_META|CONSTRUCT)|BatchGet|RetrieveRemotePayload|MergeParallelBatchGetResult" src/datasystem/worker/object_cache
```

Log extraction template:

```bash
awk '/\\[Perf Log\\]:/{block=""; capture=1; next} capture && NF{block=block $0 "\\n"} END{printf "%s", block}' \
  datasystem_worker.INFO.log | grep -E 'WORKER_.*(REMOTE|BATCH|QUERY_META|CONSTRUCT)'
```

Interpretation:

- high `WORKER_QUERY_META`: metadata/cache path dominates.
- high `WORKER_BATCH_REMOTE_GET_RPC` or `WORKER_SERVER_GET_REMOTE_READ`: RPC wait or remote server processing dominates.
- high `WORKER_CONSTRUCT_BATCH_GET_REQ`: per-object allocation/string/vector work is too heavy.
- high server-side batch keys: response assembly, memory movement, or payload construction dominates.

### Step 9: Clean Temporary Metrics

After diagnosis:

- keep only coarse, low-overhead metrics
- remove fine-grained temporary metrics
- remove stale environment variables
- remove compatibility logic no longer needed
- keep UTs that guard behavior

Search cleanup commands:

```bash
rg -n "startNs|TODO.*perf|temporary.*perf|debug.*perf" src tests
rg -n "PerfKey::RDMA|PerfPoint|RecordElapsed" src/datasystem/common/rdma tests/ut/common/rdma
```

Use project-standard `PerfPoint`:

```cpp
PerfPoint point(PerfKey::SOME_KEY);
// code
point.Record();
```

For elapsed values:

```cpp
PerfPoint::RecordElapsed(PerfKey::SOME_KEY, elapsedNs);
```

Do not add new ad-hoc latency helpers or custom perf logs for long-lived instrumentation. Prefer `PerfKey` + `PerfPoint` so data is available through logs and `PerfClient`.

### Step 10: Validate Resource Lifetime

After async submit/flush changes, inspect teardown order.

Correct principle:

1. Stop submit/progress threads.
2. Destroy sender worker pool/endpoints/rkeys.
3. Destroy remote server buffers/workers.
4. Unmap memory.
5. Cleanup UCX context.

Look for dangerous teardown order:

```cpp
remoteServer_.reset();
localBuffer_.reset();
manager.Reset();
```

Prefer:

```cpp
manager.ResetWorkerPool();
remoteServer_.reset();
localBuffer_.reset();
manager.Reset();
```

Symptoms of wrong order:

- `shmat failed: Invalid argument`
- `failed to unpack remote key`
- `shared memory error`
- `ucp_ep_rkey_unpack` crash
- `unable to detach shared memory segment`
- segmentation fault near low address like `0x38`

### Step 11: UCX Config Checks

When UCX internals are suspected, collect:

```bash
ucx_info -v
ucx_info -d
ucx_info -c
```

Suggested envs for investigation:

```bash
export UCX_LOG_LEVEL=info
export UCX_LOG_FILE=/tmp/ucx_%h_%p.log
export UCX_TLS=rc,sm,self
export UCX_RNDV_THRESH=...
export UCX_ZCOPY_THRESH=...
export UCX_MAX_EAGER_RAILS=...
export UCX_MAX_RNDV_RAILS=...
```

Do not tune blindly. Compare with metrics before/after.

## Key Decision Rules

- If RDMA metrics are microseconds but end-to-end is milliseconds, stop optimizing UCX first; inspect upper layers.
- If worker count improves then worsens, search for contention, not just lack of parallelism.
- If batch size increases but latency does not drop, flush count is not the dominant bottleneck.
- If only closed-loop throughput drops, inspect p99/max latency and queueing.
- If tests crash in UCX rkey/endpoint cleanup paths, inspect resource lifetime before changing UCX API use.
- If a getter mutates round-robin cursor, name it accordingly or document it; tests should cover cursor movement.
- If a function cannot obtain required UCP worker address/rkey/endpoint, return error immediately instead of filling partial pb and returning OK.

## Common Command Templates

Search RDMA/UCP call sites:

```bash
rg -n "ucp_put_nbx|ucp_ep_flush_nbx|ucp_worker_progress|ucp_ep_rkey_unpack|GetOrCreateEndpoint|RemoveEndpoint|WriteN|WriteDirect" src tests
```

Search performance metrics:

```bash
rg -n "PerfKey::RDMA|RDMA_UCP|PerfPoint|RecordElapsed|WORKER_.*REMOTE|WORKER_.*BATCH" src tests
```

Extract final metric lines:

```bash
awk '/\\[Perf Log\\]:/{block=""; capture=1; next} capture && NF{block=block $0 "\\n"} END{printf "%s", block}' \
  datasystem_worker.INFO.log
```

Include compressed logs:

```bash
zgrep -h -A200 "\\[Perf Log\\]:" datasystem_worker.INFO* | tail -200
```

Check stale debug leftovers:

```bash
rg -n "startNs|TODO.*perf|temporary.*perf|debug.*perf" src tests
```

Check async write tests:

```bash
rg -n "WaitFor|NotifyAll|SetFailed|submitThread|submitRunning|RemoveByIp|GetOrSelSendWorker|GetOrSelRecvWorker" tests/ut/common/rdma src/datasystem/common/rdma
```

## Expected Output From This Skill

When used, produce:

- suspected bottleneck layer
- evidence from metrics
- recommended next experiment
- code areas to inspect
- UTs to add or adjust
- cleanup items after diagnosis
