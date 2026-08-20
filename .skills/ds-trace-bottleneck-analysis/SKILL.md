---
name: ds-trace-bottleneck-analysis
description: Use when DataSystem trace work needs TopN key-bottleneck reporting, multi-run control-variable comparison, capped latency-band samples, QPS or object-size analysis, or separate detailed pages per run after ds-trace-triage.
---

# DataSystem Bottleneck Analysis

## Core boundary

Use `scripts/ds_trace_triage.py` first. It alone parses trace contents and owns grouping,
normalized RPC/URMA evidence, and run directories. This companion is a
post-processor: never change the base parser merely to build a comparison page,
never import it, and never duplicate its log regexes.

Keep run isolation: every configured run gets its own triage page,
`bottleneck.analysis.json`, and detailed bottleneck page. A suite dashboard may
compare summaries but must never merge Trace rows across runs.

## Single-run analysis

Run `scripts/ds_trace_bottleneck.py` against one completed triage run. Supply
`--local-cache` only from user/config evidence and omit `--deadline-ms` when the
deadline is unknown. Use any positive `--top`; for a preselected anomaly corpus,
use all collected Trace rows so each source band remains represented.

Preserve missing RPC, URMA, CPU, lock, and scheduling fields as unobserved.
Use strict `URMA_ELAPSED_TOTAL > 1.5ms` for slow WR. Do not convert absent fields
to zero or infer Worker-to-Worker transfer from service names. 页面上将字段缺失
明确显示为“未观测”。

Normalize `URMA_WAIT_TIMEOUT`, `URMA-WAIT-TIMEOUT`, `URMA WAIT TIMEOUT`, and
`Timed out waiting for urma_request_id` into the independent `URMA超时` error
family. A failed WR may have no completed `URMA_ELAPSED_TOTAL`; keep its URMA
stage duration unobserved and retain the largest explicit `elapsedMs` only as
timeout evidence. Do not let such traces fall through to “父窗口/未细分”.

Keep stage attribution and error taxonomy as two aligned dimensions. Stacked
bars contain only mutually exclusive stage duration; render `URMA超时` as an
overlay marker and as an explicit TopN table error column, never as another
stacked duration. The problem-latency chart uses completed stage duration for
normal bottlenecks and explicit timeout `elapsedMs` for `URMA超时`, with the
metric name shown in the tooltip/card.

Do not leave a broad `数据访问父窗口/未细分` or `远端供数处理` label when the
same Trace contains evidence that closes the path. Refine in this order:

- explicit `URMA_WAIT_TIMEOUT` → `URMA等待超时`;
- outer Get `server_req_queue_us` dominates its e2e → `Client→Worker RPC排队慢`;
- QueryMeta dominates the data parent → `QueryMeta慢`;
- `Processing pull object` / `[GetObjectRemote] finish` dominates while the
  logical URMA Write is small → `Data Worker供数处理慢`;
- a complete logical URMA Write is `>1.5ms` and covers at least 70% of the data
  parent → `URMA慢完成`;
- otherwise state `证据不足·数据访问窗口未闭合` and name the missing evidence.

Treat Client `TransportGet phasesUs.data_transfer`, Data Worker
`Processing pull object`, provider `[GetObjectRemote] finish`, logical URMA
Write, RPC network, RPC server queue, and server execution as separate observed
windows. Never rename a parent window as a root cause, and never call
`server_req_queue_us` network or SHM-copy time.

`MasterOCService.QueryAndGet` is not metadata-only when the request carries
`data_request`: the Meta Owner may run `TryGetQueryAndGetData`, call the local
`GetObjectRemoteForQueryAndGet`, and return resident data through UB/URMA. Give
each QueryAndGet Trace exactly one mutually exclusive detail class, in this
order: later data/URMA-connect failure, QueryAndGet RPC deadline, TryGet slow
URMA (`URMA_ELAPSED_TOTAL >1.5ms`), retry/multi-attempt accumulation, successful
RPC residual, Meta Owner queue/server, then unclosed. Keep TryGet/URMA presence
as an orthogonal evidence tag, not a second counted class. A successful
QueryAndGet log must never make a later `GetObjectRemote` or
`WorkerWorkerExchangeUrmaConnectInfo` deadline look like QueryMeta timeout.

Every shareable bottleneck output must carry the input packages preserved by
ds-trace-triage. Keep the exact bytes and SHA256: copy `<run-dir>/raw/inputs/*`
to the report directory's `raw-inputs/`, render download links, and do not
reconstruct an archive from the capped per-Trace evidence rows.

For failed traces, classify the evidence chain separately:

- `URMA completion超时·单pending WR` or `多pending WR` from send-lane evidence;
- `QueryMeta RPC deadline` or `Data RPC deadline` from the named slow RPC and
  deadline evidence;
- keep failure point, upward status chain, and recovery action separate.

`send lane` sealing/force-release is a recovery action, not the timeout cause.
Without receiver completion, device event, CQ/JFC polling, and scheduling
closure, state that the final root cause is unclosed rather than choosing
hardware, network, poll, or thread wakeup.

## Multi-run control variable analysis

Create a JSON manifest and run:

```bash
python3 scripts/ds_trace_bottleneck_suite.py \
  --manifest <suite.manifest.json> \
  --output <share-root>/index.html \
  --analysis-json <share-root>/data/suite.analysis.json
```

Each manifest run supplies an ID, experiment-intent axes, original archive,
per-run bottleneck JSON, and per-run report links. The suite reads archive member
names only to recover the collection band; ds-trace-triage remains the sole trace
content parser.

Match control variable groups explicitly:

- implementation: fix size, load, and client shape;
- load: fix implementation and size;
- client shape: compare equal or approximately equal aggregate QPS, marking the
  filename-derived assumption;
- object size: fix implementation, load, and client shape.

Treat filenames as intent, not runtime proof. Validate transport and topology
inside each detailed run using Trace evidence and current source.

## Sampling contract

Treat per-band limits such as 500 or 1000 as capped anomaly samples. A saturated
band says collection reached its cap; it is not an occurrence rate. Without total
request count and run duration, compare only within-band root-cause composition,
latency percentiles, stage percentiles, URMA WR behavior, and evidence gaps. Do
not report cross-run benefit percentages.

Require zero unmatched Trace IDs between the bottleneck model and archive member
band map. If unmatched IDs exist, stop instead of silently dropping them.

## Source interpretation

For `local_cache=false`, GET is Client-initiated: QueryMeta reaches the Meta Owner,
`GetObjectRemote` (single object) or `BatchGetObjectRemote` reaches the Data Worker,
and URMA returns data to Client. Treat `client.rpc.direct_get_data` as a Client-side
data-access parent window, never as Data Worker `ProcessGet`. Split a named RPC only
when its own e2e/network/server fields are present; a failed RPC without a server
trailer remains an unclosed deadline window. SAME/META data placement changes
Set/MSet placement; it does not directly change Get routing. Describe observed read
differences as indirect placement effects.

Use three URMA levels: **Client Get → 逻辑 URMA Write → WR分片**. One
`URMA_ELAPSED_TOTAL` request ID is one WR chunk, not one Client Get. For a complete
chunk-index/count group, use the wall span from earliest post to latest observed as
the logical Write duration. **WR耗时不可求和** because chunks may overlap. If chunk
or clock evidence is incomplete, label the value as a slowest-WR fallback. Completion
wait is per-WR waiting, not pure network; Inflight WR is the sender manager's global
snapshot, not the Get's WR count.

For QueryMeta root analysis, group `QueryMeta` and `QueryAndGet` by local timestamp
and emitting/initiating Worker. If the log lacks the peer/target address, state
`Meta Owner目标未观测`; never rename the logging Client or Worker as Meta Owner.
“同 Worker 时间关联” must support Worker, category, status, relation,
Client-latency-band, and local-time-range filters. Rebuild all four RPC/UB/metadata/
data charts and the detail table from the same filtered event set.

For `local_cache=true`, SHM final delivery can coexist with upstream Worker URMA.
Keep final Client transport and upstream data movement as separate dimensions.

## Required validation

Run:

```bash
python3 -m pytest -s -q tests/scripts/test_ds_trace_bottleneck_suite.py
python3 -m pytest -s -q tests/scripts/test_ds_trace_triage.py tests/scripts/test_ds_trace_bottleneck.py
node --check tests/scripts/check_ds_trace_bottleneck_suite.js
```

Browser-check the suite, one small run, and one largest run. Confirm distinct run
links, filters, centered chart titles, responsive tables, and detailed-page
pagination/downloads. Report-only changes should not modify the base parser; if
a new Trace-ID or log format really requires parser support, keep that parser
change focused and cover it in `test_ds_trace_triage.py`.
