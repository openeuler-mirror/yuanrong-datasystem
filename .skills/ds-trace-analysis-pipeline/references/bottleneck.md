# Read/write bottleneck mode reference

This is the read/write capability of `ds-trace-analysis-pipeline`, not a separate skill. Select `read`, `write`, or both from the user's request; the complete `pipeline` command is only needed for a full report.


## Core boundary

Use `scripts/ds_trace_analysis.py triage run` first. It owns grouping,
normalized RPC/URMA evidence, and Run directories. The full pipeline validates `evidence.json`
and branches read/write analysis from it; standalone partial commands can consume persisted Triage/read models;
do not duplicate its log regexes merely to build a comparison page.

Keep run isolation: every configured run gets its own triage page,
`bottleneck.analysis.json`, and detailed bottleneck page. A suite dashboard may
compare summaries but must never merge Trace rows across runs.

Keep read and write models separate. The read TopN uses QueryAndGet/Get stages.
The write TopN independently selects Client `SET/MSET/Create/Publish` traces and
uses the write surfaces already produced by `ds_trace_analysis.py triage`: Client Set
total, Create RPC, Copy, Publish RPC, Worker Publish, and metadata commit.
For SET, these are stages of one Trace. In the current source, routed writes send
URMA WR through `UbTransporter::Set` within Publish. Bound writes can send URMA
WR in `Buffer::MemoryCopyWithTransport` within Copy, or in `Buffer::Publish`
when no earlier send succeeded. Attribute a WR to a phase only when its actual
send callsite has evidence; a bound route alone is insufficient. Otherwise
report `unconfirmed`.
An independent CREATE request has no WR and is excluded from the zero-WR
denominator. Never reuse QueryAndGet,
RemoteGet, or read-side URMA attribution for writes. If a package contains no
write flows, render `0条/未采集` instead of inferring write behavior from GET
traces.

The write stacked bars are mutually exclusive:

- `Create RPC其他`: Create e2e after explicit network, request queue, and RPC
  framework are removed; without a complete RPC trailer it remains an
  unrefined Create parent;
- `写入MemoryCopy`, `写入URMA通信`, and `写入URMA调度/线程开销`: a bound
  WR may be nested in Copy, whereas a routed WR can occur in Publish. Keep the
  legacy mutually exclusive Client budget and expose route/phase observations
  separately; do not present `max(memory, URMA)` as proven phase placement.
  Never add asynchronous WR durations or nested windows to Client total;
- `Publish RPC其他`: Publish e2e after explicit network, request queue, and
  RPC framework are removed;
- `Worker Publish/元数据`: `worker.process.publish` plus one applicable
  `worker.rpc.create_meta` or `worker.rpc.update_meta`, carved from Publish;
- `其他调度/线程开销`, `RPC网络相关`, and `RPC框架`: the same evidence
  boundary as the read focus model;
- `未解释残差`: Client Set total not closed by the observed write stages.

Prefer `client.rpc.create_total` and `client.rpc.publish_total` when present so
retry time is not silently discarded; otherwise use `client.rpc.create` and
`client.rpc.publish`. Split Create and Publish RPC timing independently. A
partial/failed RPC trailer does not prove network, handler, or framework time.

## RPC attempts and QueryAndGet breakdown

Use triage `rpc_calls`, `client_processes`, and `query_and_get_calls` for complete
attempt accounting beyond the display-evidence cap. Do not parse those new fields
again in the post-processor. Enumerate every method; validate clocks within each
call and select a maximum non-overlapping residual path on the unique Client
process. Keep excluded/failed calls visible and separate measured residual from
budget-limited attribution. Never sum nested or cross-process RPCs as Client time.
Legacy summaries require re-triage for this audit; missing evidence is not zero.

When detailed RPC timing is absent, use triage `rpc_stage_windows` from access
`latencySummary`, projected as `rpc_analysis.summary_windows`. Keep this separate
from individual `calls`: a stage window can include retries and provides neither
RPC attempt count nor network/server decomposition. Deduplicate collection copies
in triage before the evidence display cap. The read QueryAndGet chart may show an
explicitly unrefined summary window; Chapter 3 separates detailed calls from summary
windows. Prefer complete detailed timing for the same method and owner. Validate
positive chart data against available observations, not just JavaScript errors.

Show QueryAndGet RPC and Worker phases as separate observation layers. Worker
preprocess/localRead/metadata/delivery must come from structured completion events;
localRead can contain inline URMA. Do not sum them with the RPC/Client parent or
truncate a matched timeout wait to another completed WR's duration. See the
[RFC](../../../docs/source_zh_cn/appendix/trace_rpc_accounting_rfc.md) for invariants.

## Single-run analysis

### Independent write page and compact sharing

After the bottleneck model exists, render writes without changing the read model:

```bash
python3 scripts/ds_trace_analysis.py write \
  --analysis-json <run-dir>/bottleneck.analysis.json \
  --output <run-dir>/bottleneck.write.html \
  --read-report bottleneck.local.html --triage-report report.local.html \
  --numa-report numa.local.html
```

Omit a report-link option when that page was not generated. Output must be beside
the input model so raw-download references retain their original base. The new
`write.refined.analysis.json` does not overwrite the original analysis. Supply
`write_bottleneck_report` in the suite manifest for a separate write entry.

Only one timestamped, same-Trace and same-observer `Client/WorkerRpc` Create or
Publish completion can supplement a missing parent. Deduplicate repeated
observations, reject ambiguous calls, and move the parent only from sufficient
remaining residual. Never infer network/framework/CPU from a failed trailer.
Client failure uses final access status; retry/error tags are independent.
The write page provides time/Worker views, numbered navigation, paginated
tables, grouped logs, and retained-evidence downloads.

For delivery, keep the full analysis directory and export a separate share tree:

```bash
python3 scripts/ds_trace_analysis.py package --root <report-root> \
  --entry index.html --manifest <report-root>/suite.manifest.json \
  --output <new-share-directory> --zip <new-share.zip>
```

The manifest's report paths are relative to `--root`. Explicitly include any
dynamically linked pages using repeated `--entry`; supplement pages can also
use `set_triage_report` / `set_numa_report`. The exporter follows static links
and embedded `download_path` references, retains linked JSON downloads, stores
identical raw files once by SHA256, extracts repeated ECharts libraries, and
applies shared presentation CSS. Unlinked site drafts/intermediate models stay
in the original analysis directory. It does not truncate evidence or change
the base parser. This is resource deduplication, not cross-model Trace merging.
Validate local-file navigation and downloads after export; unknown dynamic
references require explicit entrypoints, not silently missing artifacts.

Run `scripts/ds_trace_analysis.py read` against one completed triage run. Supply
`--local-cache` only from user/config evidence and omit `--deadline-ms` when the
deadline is unknown. Use `--top 0` for full analysis; standalone positive `--top N` retains the legacy input limit and intentionally reduces the persisted corpus. Pipeline always analyzes all collected Trace rows; its `top` alias or `view.read_top` selects only the initial read-page view (0/100/1000), preserving every source band and the full denominator.

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
- `URMA通信`: the slowest completed `URMA_ELAPSED_TOTAL` in one logical Write,
  after removing only explicit scheduling latency that is independently observed;
- `URMA调度/线程开销`: for the slowest WR selected as the logical Write
  critical path, the largest compatible explicit URMA scheduling observation
  among wake-scheduling, thread-scheduling, notify-to-awake,
  poll-JFC, and notify. These fields can overlap, so do not sum them. Do not
  classify the whole `wait-to-poll` or completion-wait window as scheduling;
  it is the reap wait that encloses URMA completion;
- `QueryAndGet其他业务`: QueryAndGet parent time left after URMA, RPC
  framework, RPC network, and explicit scheduling are removed;
- `Get其他业务`: Get/data-access parent time left after the same removals;
- `其他调度/线程开销`: explicit non-URMA RPC request queue,
  connection-lock wait, or other independently observed scheduling evidence;
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
maximum. Within one logical Write, keep every WR chunk but use the slowest
`URMA_ELAPSED_TOTAL`; never sum the WRs. If Worker, timestamp,
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
the triage stage. Keep the exact bytes and SHA256: copy `<run-dir>/raw/inputs/*`
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
python3 scripts/ds_trace_analysis.py suite \
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
The triage stage remains the sole trace-content parser. It rejects duplicate Run
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
`URMA_ELAPSED_TOTAL` request ID is one WR chunk, not one Client Get. In the current
read implementation, two WRs are posted asynchronously in order and then reaped
together. Keep both WR rows and use `max(WR1 elapsed, WR2 elapsed)` as the logical
Write's URMA critical duration; **WR耗时不可求和**. Completion wait and `wait-to-poll`
are per-WR reap windows, not pure network and not automatically thread scheduling;
Inflight WR is the sender manager's global snapshot, not the Get's WR count.

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
python3 -m pytest -s -q tests/scripts/ds_trace_analysis/analysis/test_ds_trace_bottleneck_suite.py
python3 -m pytest -s -q tests/scripts/ds_trace_analysis/pipeline/test_ds_trace_triage.py tests/scripts/ds_trace_analysis/analysis/test_ds_trace_bottleneck.py
node --check tests/scripts/ds_trace_analysis/browser/check_ds_trace_bottleneck_suite.js
```

Browser-check the suite, one small run, and one largest run. Confirm distinct run
links, filters, centered chart titles, responsive tables, and detailed-page
pagination/downloads. Report-only changes should not modify the base parser; if
a new Trace-ID or log format really requires parser support, keep that parser
change focused and cover it in `test_ds_trace_triage.py`.

### 自动分离读写输出

`ds_trace_analysis.py read --output report.html` 在有写入 Trace 时同时生成 `report.write.html`，写入伴随页提供返回读取页的链接。独立读取页不提供伴随页链接；完整 `pipeline` 统一组装页面互链。
读页仅包含 GET，写页仅包含 SET；analysis JSON 保持合并结构以兼容 suite 和独立写入后处理。
分享时保留两页及其依赖。全方法 RPC 审计与 QueryAndGet 分层口径见
`docs/source_zh_cn/appendix/trace_rpc_accounting_rfc.md`。

## 报告集成与版式验收

生成、后处理和打包页面时，遵守共享的
[报告集成与防遮挡约束](../../../.repo_context/modules/infra/observability/performance-troubleshooting.md#报告集成与防遮挡约束)。
保留原生布局；跨 Run 总结和统一口径放在首页或问题分析页，不在明细布局外追加重复说明横幅。
侧栏样式须隔离到专用容器，本页滚动高亮不能处理跨页链接。
交付前检查最终页面在桌面、窄屏及滚动后的实际遮挡、可读性和导航交互；
不能仅凭无横向溢出或 JavaScript 无异常判定版式通过。必要的局部证据限制仍保留在原有说明区域。

## 渲染完整性与 Worker 日志覆盖

读取模板的 `__REPORT_INIT__`、`__CORRELATION_SCRIPT__`、`__REPORT_RUNTIME__`
必须各出现一次。生成器拒绝缺失或重复插槽，不能靠修改函数正文字符串注入初始化。
读写页面提供 `ReportDiagnostics.audit()`；交付检查必须确认 `valid === true`，
逐图区分 `rendered`、`empty`、`awaiting_selection`、`error`。`empty` 仅说明没有可绘制数据，不说明原因；
`error` 和可见错误横幅不得当作“无样本”忽略。浏览器应同时检查未捕获异常。

中间模型的 `worker_log_assessment` 区分结构化 Worker 阶段覆盖与采集可用性。
没有采集清单时保留 `coverage_unknown`；已有部分阶段时标记 `partial`，不得推断全部日志完整。
如有外部采集证据，可在完成的 triage `manifest.json` 中提供 `worker_log_coverage`：
以 Trace ID 为键、列表为值，每项含 `worker`、可选 `pod_uid`、`collection_status`
（`not_collected` / `collected` / `out_of_window` / `lifecycle_mismatch`）和非空 `evidence_refs`。
这是外部证据声明，不自动从缺失日志推导。只有 `reason: pod_terminated` 且有证据引用时，
才展示采集记录报告的 POD 终止原因；不根据采样首末时间推断日志完整覆盖。
判定不改变计时预算；读写页继续展示 Client 证据，缺失 Worker 内部阶段不补零。

- WR数量（包括慢WR、Inflight、chip负载）用柱状图；同图的时延用折线，WR在左轴、ms在右轴，缺失值不补零。总WR与慢WR为包含关系，使用并列柱而非累加堆叠。

### Persisted observation contract

Read attribution consumes `evidence_facts` schema 1 from `evidence/observations.py` rather than parsing
raw strings in `analysis/read.py`. Validate persisted facts before rendering: malformed schema,
non-finite durations, missing observed-field provenance and invalid source indices fail the gate.
`source_ref.collection=record_sources` refers to the facts' compact source index (original evidence
record position, file/member/line and text hash), retained after raw-record compaction. It does not
refer to a removed `evidence_records` array. Historical models without facts remain supported;
rebuilding analysis produces the new contract. Do not edit raw evidence in a cached in-memory row.

Write attribution consumes `write_evidence_facts` schema 1 from `evidence/write.py`, persisted first in
`evidence.json` for write flows and then in the read compatibility model's write rows and
`write.refined.analysis.json`. It records the first Client
write access, ordered RPC observations, Create/Publish parent windows, error matches and component
identities. Each observation references the row's `evidence` array; ordered `source_hashes` detect
changed evidence. Existing malformed facts fail validation instead of silently reparsing. Historical
models without this block use the explicit Evidence adapter. Rebuild analysis to persist the new
contract; render-only never upgrades models. Multiple parent windows remain supporting evidence,
not a sum of presumed sequential calls. Keep original facts in downloadable models; HTML needs
only the attributed view and its evidence logs.
The refined write phase schema is version 2. `write_rpc_phase_evidence` keeps
separate Create/Publish RPC E2E, network residual, queue, framework residual,
and the selected evidence reference. These are supporting call measurements,
not additional Client-budget components. Validation accepts historical schema
1 models, while render-only leaves their missing v2 fields unobserved.
