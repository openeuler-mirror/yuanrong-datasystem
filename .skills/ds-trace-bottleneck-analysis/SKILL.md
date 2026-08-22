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

Keep read and write models separate. The read TopN uses QueryAndGet/Get stages.
The write TopN independently selects Client `SET/MSET/Create/Publish` traces and
uses the write surfaces already produced by `ds_trace_triage.py`: Client Set
total, Create RPC, memory copy with an optional nested Client→Worker URMA Write,
Publish RPC, Worker Publish, and metadata commit. Never reuse QueryAndGet,
RemoteGet, or read-side URMA attribution for writes. If a package contains no
write flows, render `0条/未采集` instead of inferring write behavior from GET
traces.

The write stacked bars are mutually exclusive:

- `Create RPC其他`: Create e2e after explicit network, request queue, and RPC
  framework are removed; without a complete RPC trailer it remains an
  unrefined Create parent;
- `写入MemoryCopy` and `写入URMA通信`: when both are present, URMA is nested
  inside the memory-copy/transfer window, so use `memory - URMA` plus URMA,
  never their sum;
- `Publish RPC其他`: Publish e2e after explicit network, request queue, and
  RPC framework are removed;
- `Worker Publish/元数据`: `worker.process.publish` plus one applicable
  `worker.rpc.create_meta` or `worker.rpc.update_meta`, carved from Publish;
- `调度/线程等待`, `RPC网络相关`, and `RPC框架`: the same evidence
  boundary as the read focus model;
- `未解释残差`: Client Set total not closed by the observed write stages.

Prefer `client.rpc.create_total` and `client.rpc.publish_total` when present so
retry time is not silently discarded; otherwise use `client.rpc.create` and
`client.rpc.publish`. Split Create and Publish RPC timing independently. A
partial/failed RPC trailer does not prove network, handler, or framework time.

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
bars contain only mutually exclusive windows. Render `URMA超时` as an overlay
marker and an explicit TopN error field. When the same Worker has exactly one
`URMA_WAIT_TIMEOUT` with explicit `elapsedMs` inside a timestamped
`QueryAndGet done` parent window, carve that bounded interval from
`QueryMeta/QueryAndGet` into the stacked stage `URMA超时等待`; label it as a
timeout-wait window, not a completed WR duration. If Worker, time, parent, or
request matching is missing or ambiguous, keep the parent unchanged and show
only the error marker. The problem-latency chart uses completed stage duration
for normal bottlenecks and explicit timeout `elapsedMs` for `URMA超时`, with
the metric name shown in the tooltip/card.

Use a compact focus breakdown for the main charts and Trace table. Keep the
legacy attribution fields only as internal evidence and compatibility data:

- `URMA建链`: explicit connect-info exchange and connection-finalize windows;
- `URMA通信`: completed logical URMA Write/WR critical path after removing
  explicit scheduling waits;
- `QueryAndGet其他业务`: QueryAndGet parent time left after URMA, RPC
  framework, RPC network, and explicit scheduling are removed;
- `Get其他业务`: Get/data-access parent time left after the same removals;
- `调度/线程等待`: explicit RPC queue, connection-lock wait, thread
  scheduling, notify-to-awake, or wait-to-poll evidence;
- `RPC网络相关`: explicit `network_residual_us` only;
- `RPC框架`: RPC e2e minus server handler execution, network residual, and
  explicit request queue/scheduling, only when all four timings close;
- `未解释残差`: only the remainder whose evidence chain is not closed.

For repeated observations of the same RPC level, use the largest compatible
window rather than summing overlaps. Outer Get framework time belongs outside
its handler window; nested Data RPC framework and request queue belong inside
Get business before being carved out. A failed or partial RPC without a
complete timing trailer is not evidence of RPC framework time; keep the
unclosed interval in `未解释残差`. Each focus stage must remain non-negative
and their sum must equal the Trace total within rounding tolerance.

Do not leave a broad `数据访问父窗口/未细分` or `远端供数处理` label when the
same Trace contains evidence that closes the path. Refine in this order:

- explicit `URMA_WAIT_TIMEOUT` → `URMA等待超时`; split its wait window from
  QueryAndGet only under the unique same-Worker parent-window rule above;
- `MasterOCService.QueryAndGet` or `WorkerOCService.QueryAndGet` with explicit
  inline UB evidence and a unique same-Worker/same-attempt URMA match → split
  the QueryAndGet parent into exclusive work and inline URMA;
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

Current `WorkerOCService.QueryAndGet` is not metadata-only when the request
carries `data_request`: `WorkerQueryAndGetImpl` may encode a resident local hit
through SHM, TCP, or UB/URMA and return locations for misses. Historical traces
may use `MasterOCService.QueryAndGet`; parse those for legacy reports, but do not
infer that current code still executes the old Master-side path. Give each
QueryAndGet Trace exactly one mutually exclusive detail class, in this order:
later data/URMA-connect failure, QueryAndGet RPC deadline, inline slow URMA
(`URMA_ELAPSED_TOTAL >1.5ms`), retry/multi-attempt accumulation, successful RPC
residual, Meta Owner queue/server, then unclosed. Keep inline/URMA presence as an
orthogonal evidence tag, not a second counted class. A successful QueryAndGet
log must never make a later `GetObjectRemote` or
`WorkerWorkerExchangeUrmaConnectInfo` deadline look like QueryMeta timeout.

Apply the PR2165 inline attribution only when `QueryAndGet done` explicitly has
`inlineHits > 0` and `transport: UB`, the emitting Worker and timestamp are
known, and the URMA evidence matches a unique same-Worker/same-attempt window.
Treat QueryAndGet as the parent window: move the matched logical URMA Write to
the URMA stage and retain only `parent - inline URMA` as QueryAndGet-exclusive
time. Sequential attempts on one Worker add; parallel Worker owners take the
maximum. Complete WR chunks use logical-Write wall span and incomplete chunks
fall back to the slowest WR; never sum overlapping chunks. If Worker, timestamp,
attempt, or matching evidence is ambiguous, keep the split unobserved.

After that inline-URMA split, a single successful QueryAndGet RPC trailer with
explicit `network_residual_us` and/or `server_req_queue_us` may further split
the remaining QueryAndGet-exclusive window into RPC communication residual,
RPC queue, and server-exclusive work. Clamp every child to the remaining parent
window so the stages stay mutually exclusive. The legacy `RPC网络` bucket is
an internal bRPC communication residual that may
include framework time. Do not expose it as physical-network latency. In the
focus breakdown, expose only explicit `network_residual_us` as `RPC网络相关`
and place the closed remainder in `RPC框架`.

Keep QueryAndGet coverage gaps as explicit diagnosis classes. A Worker
`QueryAndGet done` whose `localRead` dominates but whose same-Trace URMA detail
is absent is `localRead慢·URMA未观测`, not a confirmed slow WR. A Client parent
window without either an RPC trailer or Worker `QueryAndGet done` is
`QueryAndGet父窗口·服务端未观测`; do not label it network, metadata, or URMA.

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

For every Run, first create an independent triage run directory, then run the
single-run bottleneck analysis and optional NUMA analysis against that Run only.
Do not point the suite at raw trace contents. After all per-Run artifacts exist,
create a JSON manifest and run:

```bash
python3 scripts/ds_trace_bottleneck_suite.py \
  --manifest <suite.manifest.json> \
  --output <share-root>/index.html \
  --analysis-json <share-root>/data/suite.analysis.json
```

Each manifest run supplies a unique `id`, `label`, experiment-intent axes
(`implementation`, `local_cache`, `placement`, `size`, `load`, and
`client_shape`), the original `input_archive`, its per-Run `analysis_json`, and
links to `triage_report`, `bottleneck_report`, and optional `numa_report`.
`sampling_cap_per_band` overrides the suite default for one Run. The optional
top-level `overview` is a list of `{title, text}` source-backed conclusions; the
suite renders it before the comparison charts. Resolve relative archive and
analysis paths from the manifest directory.

The suite reads archive member names only to recover the collection band;
ds-trace-triage remains the sole trace-content parser. It rejects duplicate Run
IDs and any bottleneck Trace ID that cannot be mapped back to exactly one archive
band. One Run's rows, totals, and report links must never leak into another Run.

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
