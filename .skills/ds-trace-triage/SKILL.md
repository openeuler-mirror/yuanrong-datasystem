---
name: ds-trace-triage
description: >
  Use when analyzing DataSystem slow/error trace bundles or trace IDs, including
  requests for main-problem key-bottleneck TopN/Top100 reports, stacked bars,
  same-Worker time correlation, QueryMeta/RemoteGet failures, Worker/RPC/URMA
  breakdown, RemotePull, latencySummary, or source mapping.
---

# DataSystem Trace Triage

Use this skill when the input is a small or medium trace corpus and the goal is
root-cause analysis rather than broad access/resource trending.

## Agent compatibility

This skill is tool-agnostic. Codex, Cursor, Claude Code, and Hermes should all
use the same repository script and produce the same run-directory contract.

Recommended trigger text:

- Codex: `Use $ds-trace-triage to analyze these DataSystem trace bundles.`
- Cursor rule: `When the user asks for DataSystem trace triage, follow
  .skills/ds-trace-triage/SKILL.md and run scripts/ds_trace_triage.py.`
- Claude Code memory / `CLAUDE.md`: `For DS slow/error trace packages, follow
  .skills/ds-trace-triage/SKILL.md. Do not hand-write ad hoc parsers; run
  scripts/ds_trace_triage.py run/verify and report the run directory.`
- Hermes instruction: `For DataSystem trace analysis, execute the deterministic
  ds_trace_triage pipeline, keep raw inputs/extracted logs, and return
  report.local.html plus summary counts.`

Cross-agent invariants:

- Treat the skill file as the source of procedure; do not duplicate divergent
  logic in IDE/chat-specific prompts.
- Always run from the repository root that contains `scripts/ds_trace_triage.py`.
- Accept multiple gzip/tar/log inputs in one command; preserve input order only
  for provenance, not for cohort meaning.
- Return a clickable/local path to `report.local.html`, plus `trace_count`,
  time range, dominant classifications, error counts, and access latency
  percentiles when present.
- If the report is larger than the yche publish gate, keep it local and say so.
- Never publish to <publish-site> unless explicitly asked; use `publish-site --dry-run`
  first.

Minimal command template for any agent:

```bash
cd <yuanrong-datasystem-worktree>
python3 scripts/ds_trace_triage.py verify
python3 scripts/ds_trace_triage.py run <input1.gz> <input2.gz> \
  --code-ref "$(git rev-parse main/master 2>/dev/null || git rev-parse HEAD)" \
  --case <case-name> \
  --scenario <scenario-name> \
  --out <local-run-root> \
  --force
```

Minimal response template:

```text
report.local.html: <run-dir>/report.local.html
run_dir: <run-dir>
trace_count: <N>
time_range: <first_ts> .. <last_ts>
dominant_classifications: <top counts>
errors: <error counts>
access_latency: p50=<...> p90=<...> p99=<...> max=<...>
publish: local only / dry-run / published
```

## 主问题关键瓶颈报告

专项后处理只在各自 skill 维护：TopN、分档和多 run 控制变量分析使用
`ds-trace-bottleneck-analysis`；NUMA/source-chip、`srcChipInflight` 与
URMA timeout 错误链使用 `ds-trace-numa-analysis`。本 skill 只维护公共解析、
run-directory 和发布契约，以下内容保留为基础调用边界，不复制 NUMA 专项流程。

当用户要求“主问题关键瓶颈”、`TopN/Top100`、时间序列 stacked bars、
Worker/RPC/URMA breakdown、逐 Trace 日志或分类下载时，先完成 `ds_trace_triage.py run`，
再运行后置分析器。不要复制一次性解析器，也不要把某批
Trace 的数量、ID、Worker、时间或结论写进脚本。

```bash
python3 scripts/ds_trace_triage.py run <trace-input> [more-inputs ...] \
  --code-ref "$(git rev-parse main/master)" \
  --case <case-name> --scenario <scenario> --out <run-root>

python3 scripts/ds_trace_bottleneck.py \
  --run-dir <run-dir-returned-above> \
  --top <N> \
  --local-cache <true|false> \
  --source-ref "$(git rev-parse main/master)" \
  --deadline-ms <known-deadline-ms> \
  --output <run-dir-returned-above>/bottleneck.local.html
```

在生成关键瓶颈页前，从用户、运行配置或明确的 case 元数据取得
`local_cache`，并传入 `--local-cache true|false`。不要从服务类名猜测该模式；
若使用者无法确认，则省略参数，页面必须标注“local cache 模式未知”并保持拓扑中性。

- `local_cache=false`：GET 由 Client 侧发起，链路为
  `Client → Meta Owner → Data Worker`。`BatchGetObjectRemote` 是
  Client→Data Worker RPC，URMA 是 Data Worker→Client。日志出现
  `WorkerWorkerOCService`、`BatchGetObjectRemote` 或 `RemotePull`，不能单独作为
  Worker→Worker 互拉证据。
- `local_cache=true`：Client 通过绑定 Worker 访问。只有 Trace 同时明确调用方、
  目标 Data Worker 和远端请求，才可标为 Worker→Worker；服务名本身仍不是证据。
- 模式未知：使用“BatchGet”“Data Worker 服务端”“URMA 发送端/接收端”等中性术语，
  不输出确定的 Worker→Worker 拓扑结论。

`local_cache` 只确定调用拓扑，不替代逐 Trace 的访问位置判定。对每条 GET，优先读取
Client access 日志的 `DS_KV_CLIENT_GET transportType`：`SHM` 表示本节点共享内存访问，
`UB` 表示访问远端 Data Worker；`TCP` 可能是远端访问，也可能是同节点 SHM 失败回退，
必须标“位置不确定（TCP）”；字段缺失标“未确认”。`DS_POSIX_GET` 所在节点
只能称为有处理证据的 Data Worker；不得用 Worker 日志数量、BatchGet 服务名或
`transferPath: UB` 覆盖 Client 实际 transport。页面必须支持按该位置逐 Trace 筛选，
并注明位置统计只覆盖当前输入/TopN，不能外推整个运行。

只有已从配置或用户输入确认 deadline 时才传 `--deadline-ms`。省略时页面使用
20ms 作为图表参考线并在 limitations 中明确标为参考阈值，不得写成配置 deadline。

标准输出：

- `bottleneck.analysis.json`：TopN、读取九阶段互斥归因（URMA 建链/通信/调度、QueryAndGet/Get 其他业务、其他调度、RPC 网络/框架、未解释残差）、主问题、时间、Worker、
  RPC、URMA 和逐 Trace 诊断的机器可读模型。
- `bottleneck.local.html`：内联 ECharts 和数据的自包含页面，包含排序、8 行分页、
  筛选、Trace/日志联动及 TopN、分类和单条 triage 保留证据下载。若上游
  `dropped_evidence>0`，页面与下载必须标注截断，不能称为原始日志全量。
- `raw-inputs/`：从 triage run 的 `raw/inputs/` 原样复制输入包并在专项页提供下载；
  保留 manifest 中的大小和 SHA256。不得从最多 200 行的 evidence 重新拼接“原始包”。

当前读取九阶段模型面向 GET/read Trace。混合 run 中的 SET/CREATE/PUBLISH 等非 GET
Trace 会从 Client TopN 排除并计入 limitations；不得套用读链阶段名称解释写链。

`--top` 接受任意正整数，例如 100、1000。页面标题、计数、下载范围和分页都从
实际 TopN 数据生成，不绑定 Top100；长表仍先对完整筛选结果排序，再按 8 行分页。

关键瓶颈页按 Client 总时延提供五档参考筛选：`[5,6)ms`、`[6,7)ms`、
`[7,10)ms`、`[10,20]ms`、`>20ms`。精确 20ms 归入 10–20ms，只有大于
20ms 进入最后一档；低于 5ms 的 TopN Trace 保留在“全部”并明确显示为未纳入五档。
档位只联动主问题图、阶段占比、TopN stacked bars 与 Trace 表；URMA、同 Worker、
非 RPC 和 Worker 深挖继续使用全量 TopN，避免把不同 Worker 本地时钟误当同一时间轴。

页面表格必须在可用宽度内自动换行，避免依赖横向拖动；窄屏可隐藏次要 breakdown
列，但排序、分页、下载和 Trace 明细必须保留完整数据。“同 Worker 时间关联分析”
的 RPC、UB、元数据、数据访问四张图固定一行一张，避免并排压缩时间轴。

### 同 Worker 时间关联

当用户询问同一 Worker 上 QueryMeta/RemoteGet 失败、RPC/UB 同期特征或慢 WR
时间聚集时，使用关键瓶颈页的“同 Worker 时间关联分析”章节。该章节的 Worker
选择器只过滤本章节，不得改变全局 TopN、主问题或 Trace 表。

- 用日志发出方 Worker 的本地时间，按 1 秒桶聚合；对失败 QueryMeta 和
  RemoteGet/BatchGet 查看闭区间前后各 1 秒。
- RPC、UB、元数据、数据访问保持四个独立维度。RPC 分 network/server/queue；
  UB 分 total/completion wait/Inflight；元数据单列 QueryMeta；数据访问单列
  Local processing 与 RemoteGet/BatchGet。父子窗口不得相加。
- 只有带 `URMA_ELAPSED_TOTAL` 且 total 可观测的事件才是一个 WR。
  `URMA_ELAPSED_TOTAL > 1.5ms` 才标为慢 WR；等于 1.5ms 仍为正常。
  缺少 total 时保持未观测，不能按 0，也不能用 `transferPath: UB` 补造 WR。
- 同 Trace 同阶段是直接证据；不同 Trace 在同 Worker 前后各 1 秒出现只能写为
  “同期伴随”，不能写成因果。跨 Worker 的绝对时间不直接比较。
- 某一维没有证据时显示“未观测到对应证据”，不渲染零值趋势。

先从 `summary.json` 的结构化字段取值，只对已归入 Trace 的 evidence 做补充语义识别；
不得重新读取原始 gzip/tar 来二次解析 Trace；交付时只允许复制 triage 已归档的原包。
父窗口与子阶段不得重复相加，读取九阶段之和不得超过
Client 总时延。字段缺失时保留为“内部未细分”或“未解释残差”，不得猜测为网络、
CPU、锁或线程调度。

页面生成不得自动发布。只有用户明确要求发布时，才继续走本 skill 的
`publish-site --dry-run`、尺寸门禁和站点目录注册流程。

最小回复：

```text
bottleneck.local.html: <run-dir>/bottleneck.local.html
bottleneck.analysis.json: <run-dir>/bottleneck.analysis.json
top_n: <N>
main_problems: <top counts>
evidence_gaps: <missing surfaces>
source_ref: <triage ref and current source verification boundary>
publish: local only
```

## CLI and cache contract

Use `run` for normal analysis. It executes parse, aggregate, triage,
render-local, and render-site, then returns the run directory. The legacy
top-level form without `run` is only for quick JSON/Markdown summaries and does
not preserve the full run-directory contract.

Cache reuse is intentional. The run cache key includes the script version,
parser-rule fingerprint, code ref, case, scenario, input identity, tar members,
and input content hash. Use `--force` when you want a fresh timestamped run even
though the inputs and script did not change.

Input failures fail fast by default so reports do not silently miss logs. Use
`--allow-partial-inputs` only for best-effort triage; the run records failures in
`dimensions.input_failures`, and conclusions must call out the missing input
surface.

Real yche publishing is two-step. `publish-site` can copy and live-check the
HTML, but the report is fully published only after the site catalog
`index.html` has exactly one matching `var P` entry. If the URL works but the
catalog entry is absent, report status as copied but not catalog-registered.

## Required workflow

1. Pin the source:
   ```bash
   git fetch main master
   git rev-parse main/master
   ```
2. Build or refresh CodeGraph on a clean `main/master` worktree when source
   causality is requested:
   ```bash
   CODEGRAPH_BIN=${CODEGRAPH_BIN:-$(command -v codegraph || true)}
   test -n "$CODEGRAPH_BIN"
   "$CODEGRAPH_BIN" init <clean-worktree>
   "$CODEGRAPH_BIN" index <clean-worktree>
   ```
   If `CODEGRAPH_BIN` cannot be found, continue with log-only triage and say
   source causality is unverified. Do not claim code-level root cause until
   CodeGraph discovery and direct source reads are both available.
3. Run the deterministic parser first:
   ```bash
   python3 scripts/ds_trace_triage.py run <trace_dir_or_tar_gz> [more.gz ...] \
       --code-ref "$(git rev-parse main/master)" \
       --case <case-name> \
       --scenario <scenario> \
       --out <local-run-root>
   ```
   For manual debugging or CI artifact checks, the same pipeline can be run as
   explicit stages:
   ```bash
   run_dir=$(python3 scripts/ds_trace_triage.py parse <trace_dir_or_tar_gz> [more.gz ...] \
       --code-ref "$(git rev-parse main/master)" \
       --case <case-name> \
       --scenario <scenario> \
       --out <local-run-root>)
   python3 scripts/ds_trace_triage.py aggregate "$run_dir"
   python3 scripts/ds_trace_triage.py triage "$run_dir"
   python3 scripts/ds_trace_triage.py render-local "$run_dir"
   python3 scripts/ds_trace_triage.py render-site "$run_dir"
   python3 scripts/ds_trace_triage.py publish-site "$run_dir" --dry-run
   # After reviewing site_publish.md, set DS_TRACE_TRIAGE_PUBLISH_HOST and
   # DS_TRACE_TRIAGE_PUBLISH_ROOT, then omit --dry-run to pass the HTML size
   # gate, scp, curl HEAD, and verify live HTML markers.
   ```
4. Read the timestamped run directory:
   - `manifest.json`: case/scenario/ref/time range and render targets
   - `events.jsonl`: trace-scoped raw and UB events with source/member/line
   - `parsed_traces.json`: parser output consumed by aggregate
- `summary.json`: time/worker/flow/latency/RPC/UB/error dimensions
   - `triage.json` and `triage.md`: classifications and issue candidates
   - `report.local.html`: self-contained local report
   - `report.site.html`: <publish-site>-shaped report draft; keep the same core
     components as local HTML and include `/assets/css/site.css` plus
     `/assets/js/site.js`
   - `site_publish.md`: <publish-host>/<publish-site> publish checklist with target path,
     URL, HTML size, copy command, catalog-index registration, validation
     command, and the default publish size limit
   - `manifest.json` `render_targets.site.publish`: dry-run/publish status
     recorded by the `publish-site` stage
5. Inspect selected full logs for the top slow/error traces. Keep aggregate
   distributions first, then per-trace evidence.
6. Cross-check any source-level conclusion with CodeGraph plus direct source
   reads. CodeGraph is discovery, not sole proof.

## Self verification and CI

The script has a built-in fixture:

```bash
python3 scripts/ds_trace_triage.py verify
python3 scripts/ds_trace_triage.py --self-test
python3 -m pytest -s tests/scripts/test_ds_trace_triage.py -q
```

先不要接入 .gitee/ci_build.sh。这些命令是人工验证和 agent
变更自检入口，避免 trace 分析工具影响主工程构建、标签分流和已有 CI 时长。

它们验证 gzip-tar handling、trace grouping、access latency、breakdown、
rpc slow、URMA elapsed、UB field extraction、time buckets、worker/edge
aggregation、local/site HTML generation、inline report JavaScript syntax when
Node.js is available、error classification、yche publish checklist，以及
`publish-site --dry-run` manifest status。

候选 CI 门禁只在后续明确评审后再接入，建议先放到独立 job 或手动触发 job，
不要直接塞入 `.gitee/ci_build.sh` 主构建路径。候选命令如下：

```bash
python3 -m py_compile scripts/ds_trace_triage.py tests/scripts/test_ds_trace_triage.py
python3 scripts/ds_trace_triage.py verify
python3 -m pytest -s tests/scripts/test_ds_trace_triage.py -q
```

Real <publish-site> publish has a default 2 MiB `report.site.html` size gate to avoid
publishing oversized throw-away pages. If a large page is intentional, review
the report first and pass `--max-site-html-mb <N>` explicitly.
Real publish also requires `DS_TRACE_TRIAGE_PUBLISH_HOST` and
`DS_TRACE_TRIAGE_PUBLISH_ROOT`; `DS_TRACE_TRIAGE_PUBLISH_BASE_URL` defaults to
the public report base URL when it is not set.

Real publish is only complete after the report is discoverable from the site
catalog. After copying `report.site.html` to `<publish-root>/perf/<filename>`,
add or update exactly one `var P` metadata entry in `<publish-root>/index.html`
for `perf/<filename>`. Keep the edit minimal, preserve existing entries, and do
not rewrite the whole index when the remote tree has unrelated changes. Validate
the updated catalog before reporting success:

```bash
# On the publish host, from the site root that owns index.html:
# 1. Back up index.html.
# 2. Add/update the single `var P` entry for perf/<filename>.
# 3. Extract/check the inline JavaScript or otherwise run the site-local
#    validation command used by that host.
# 4. Verify both the report URL and the catalog entry over HTTPS.
```

If the page URL works but the catalog entry is missing, report the publish as
copied but not fully registered.

Run pytest locally when changing parser behavior:

```bash
python3 -m pytest -s tests/scripts/test_ds_trace_triage.py -q
```

The self-test must keep covering the historical contract learned from the trace
threads: `latencySummary` raw text and key/value fields, RPC slow server/network
subfields, `URMA_ELAPSED_TOTAL/POLL_JFC/NOTIFY/THREAD_SHED`, and classification
counts. When DataSystem log wording changes, update the fixture and tests in the
same patch as parser logic.

Trace ID extraction must follow the current source contract instead of a fixed
operation-name allowlist. Prefer the sixth field of the standard log prefix
(`... | pid:tid | traceId | az | ...`), validate it with the same character set
and 49-character runtime limit as `Trace`, and only then use constrained
fallbacks for unstructured input. Preserve all of these forms:

- canonical UUIDs;
- `Context::SetTraceId` prefixes followed by `;` and the 12-character UUID tail;
- historical kvtest `<operation>-<pid>-<counter>` and current
  `<operation>-<instanceId>-<pid>-<counter>` prefixes, for every pipeline
  operation; the counter has a minimum display width of eight, not a maximum;
- IDs installed directly through `Trace::SetTraceNewID`, including prefixed or
  length-truncated UUIDs, when they occupy the structured trace field.

If a structured line has an empty trace field, do not promote unrelated UUIDs
such as `remoteInstanceId` from the message body into a request Trace. An
explicit `traceId=` marker may still be used as a fallback. Keep focused tests
for every newly observed ID form and for this negative boundary.

For small log-format extensions, keep the analyzer stable by registering new
markers instead of rewriting the parse loop:

```python
mod.register_error_pattern("DMA_WAIT_TIMEOUT")
mod.register_metric_rule(
    "urma_dma",
    r"\[URMA_ELAPSED_DMA\].*?cost\s+([\d.]+)\s*(us|ms)",
    unit_group=2,
)
```

Every registered rule must have a focused pytest fixture that verifies the trace
level output and the aggregate `dimensions.custom_metrics_ms` or
`dimensions.errors` output.

The script keeps these responsibilities separated inside one file:

- `ParserRules`: owns log wording extension rules.
- `TraceInputReader`: reads directories, files, gzip logs, and tar bundles.
- `TraceParser`: parses one log line into trace-scoped facts.
- `TraceAccumulator`: ingests parsed lines into trace-scoped counters, evidence,
  RPC, latencySummary, URMA, worker, and error state.
- `TraceDimensionBuilder`: converts accumulated state into the stable
  `summary.json` schema and cross-dimensional rollups.
- `TraceAnalyzer`: only coordinates reader, parser, accumulator, and dimension
  builder.
- `TraceReportRenderer`: renders events, triage, Markdown, and HTML.
- `TraceRunStore`: owns staged run directories, cache, manifest, raw inputs, and
  artifact reads/writes.
- `TraceSitePublisher`: owns <publish-site> size guard, copy, and live-marker
  validation.
- `TraceRunPipeline`: only orchestrates parse, aggregate, triage, render-local,
  and render-site stage order.

Keep compatibility wrappers such as `analyze_inputs`, `parse_stage`, and
`run_pipeline`, but put new behavior behind the responsible class first.

## Report expectations

Always cover:

- time: first/last timestamp, burst windows if visible
- worker: entry/provider/target concentration where logs expose it
- UB worker: `transferPath: UB` is only a selected-path/capability label, not a
  timed WR. Build WR timing only from `URMA_ELAPSED_TOTAL`, and use its emitting
  Worker as the evidence-backed URMA source Worker. Keep RemoteGet/RPC and UB
  timing as separate dimensions.
- UB lifecycle: keep UB/URMA in a standalone report chapter. Compare
  `URMA_ELAPSED_TOTAL`, `wait os sched thread finish time`, `wakeSchedLatencyUs`,
  `srcChipInflight`, `URMA_ELAPSED_POLL_JFC`, `URMA_ELAPSED_NOTIFY`,
  poll-loop gap (`lastPollEndToThisPollStart` /
  `lastPollStartToThisPollStart`), and nanosleep wake
  (`nanosleep(1us) cost`). Render both metric percentiles and Top request rows
  with worker/IP/request/cpuid/data-size/status fields. Also track
  `inflightRemoteGet` as remote-get WR pressure, `urma_inflight_wr_count` as
  send-side URMA WR pressure, and `srcChipInflight` per chip.
- flow: Get/Set/Create/Publish/RemotePull/GetObjMetaInfo/RPC methods
- latency: access latency percentiles and top slow traces
- breakdown: `ProcessGetObjectRequest`, QueryMeta/CreateMeta, SafeObject locks,
  client summary and worker summary fields
- rpc slow: method, count, e2e/client/server/network fields when present
- URMA: `URMA_ELAPSED_TOTAL`, `URMA_ELAPSED_POLL_JFC`,
  `URMA_ELAPSED_NOTIFY`, `URMA_ELAPSED_THREAD_SHED`, `URMA_PERF`
- errors: non-zero access status, deadline exceeded, not found, object in use,
  URMA timeout, fallback rejection, etcd abnormal
- source: pinned ref, key files/functions, and evidence boundary

For customer-facing reports, write like a diagnosis note:

- Start with the user-visible symptom, then separate it from worker-side
  evidence. For example, a client 20ms deadline and a 250ms worker completion
  can both be true.
- Include one concise "core judgment" paragraph with negative boundaries:
  "not client local processing", "not QueryMeta with current evidence", "URMA
  total is only 0.x ms".
- Every chart needs a caption explaining what question it answers.
- Keep trace drilldown usable: search, filters, pagination, selected trace
  breakdown, and full logs.
- In selected trace summaries, split client access and worker access. Client
  access is the user-visible deadline/symptom side; worker access is
  server-side completion evidence and can legitimately exceed or lag the client
  window.
- For UB/URMA, describe the post/write wait timeline and compare total,
  `condition_variable.wait_for`, wake scheduling, poll JFC, notify, poll-loop
  gap, nanosleep wake, data size, CPU, inflight, source chip, and edge.
- Keep long report tables paginated at 8 rows per page. Do this for new
  UB lifecycle, UB request, UB worker role, UB time-bucket, worker, and edge
  tables so a single noisy run does not bury the charts.
- For noisy-vs-clean comparisons, treat different runs as cohorts: paths or tar
  members containing `dizao`/`底噪` are `有底噪(dizao)`, paths containing
  `wudizao`/`无底噪` are `无底噪(wudizao)`, and once a noise marker exists in
  the run, unmarked peers are the `无底噪(wudizao)` baseline.
- For generic multiple packages without noise markers, keep each package as a
  separate cohort and compare distributions before carrying over root-cause labels.
- The HTML report should follow the `<publish-root>/perf` trace-report pattern:
  fixed left navigation, KPI cards, a core-judgment panel, ECharts with
  captions, cohort comparison, trace search/filter/pagination, selected-trace
  breakdown, highlighted full logs, run/input provenance from `manifest.json`,
  evidence coverage/missing-surface tables, and downloads for selected raw logs
  plus filtered evidence and report-summary Markdown.

Machine-readable summaries should expose these buckets when the input contains
them:

- `dimensions.latency_summary_us`: parsed `latencySummary:{...}` fields, while
  `traces[*].latency_summary_raw` preserves the original summary line text
- `dimensions.rpc_slow`: method plus `e2e_us`, framework, server queue/exec, and
  `network_residual_us`
- `dimensions.urma_elapsed`: total, poll JFC, notify, and thread scheduling
- `dimensions.ub_summary`: transfer path and `src -> target` UB edges
- `dimensions.ub_lifecycle_summary`: lifecycle metric percentiles plus Top
  request rows for total/wait/wake/poll/notify/thread scheduling, worker, edge,
  CPU, data size, status, remote-get WR count, URMA inflight WR count, and
  source-chip inflight.
- `dimensions.cohorts`: per-input-package trace/error/classification/latency
  comparison for multi-package and noisy-vs-clean analysis
- `dimensions.diagnosis`: customer-facing diagnosis lines for symptom,
  latency, evidence boundary, and customer expression. HTML should render this
  data instead of re-deriving report language in JavaScript.
- `dimensions.recommendations`: source-validation, observability,
  cohort-compare, UB/URMA, and deadline follow-up actions for the appendix.
- `dimensions.source_appendix`: maps log surfaces to read/write flow stages,
  source hints, CodeGraph/source validation, and customer-facing report reading.
- `dimensions.flow_stages`: graph-ready Client/Entry/Meta/Data/UB nodes and
  read/write edges with evidence coverage for the flow-stage chart and table.
- `dimensions.time_buckets`: 1s/10s burst and gap candidates
- `dimensions.worker_summary`: role-aware client/entry/data/meta worker views
- `dimensions.classifications`: parser-assigned root-cause families such as
  `client_deadline_with_urma_wait`, `client_deadline_20ms`,
  `write_memory_copy_dominant`, `remote_fast_transport_wait`, and `rpc_slow`

## Error-trace tactics

Use several independent cuts before deciding root cause:

- **Status/error family cut**: group non-zero access status and repeated text
  such as deadline exceeded, not found, object in use, fallback rejected, and
  URMA wait timeout.
- **Deadline-budget cut**: align access latency with configured timeout,
  `reqTimeoutDuration.CalcRemainingTime()`, RPC slow e2e, and worker completion
  time. A client timeout can coexist with a later worker-side slow completion.
- **Worker ownership cut**: separate client, entry worker, provider/data worker,
  master, and fallback target. Do not label a target worker unless the log
  explicitly prints it.
- **Transport cut**: split TCP/UB/URMA/RDMA/fallback evidence. Tracker defaults
  or response-only fields are not proof of request-side transport.
- **URMA lifecycle cut**: compare `URMA_ELAPSED_TOTAL`, poll JFC, notify, thread
  scheduling, data size, CPU, inflight, source chip, and target address.
- **Source-evolution cut**: re-run CodeGraph on current `main/master`, then
  verify direct source for timeout propagation and current data-plane branches.

## Historical trace-thread lessons

The workflow is calibrated from eight Codex trace-analysis threads:

- `019f753c`: 248 Get traces showed RemotePull/URMA completion wait dominating
  `ProcessGetObjectRequest`; avoid treating QueryMeta as the bottleneck when
  URMA and worker completion distributions align.
- `019f75a9` and `019f7606`: after hardware-port isolation, seconds-scale URMA
  tails disappeared and residual failures clustered around 20ms client/worker
  RPC deadline. Keep these as separate families.
- `019f7686`: ZMQ/brpc slow reports need subfield parsing, especially
  `server_exec_us` and `network_residual_us`, plus source mapping for
  `GetObjMetaInfo`, `ProcessGetObjectRequest`, `BatchGetObjectRemote`, and
  `UrmaGatherWrite`.
- `019f76d0`: write traces require original `latencySummary` preservation;
  `client.process.memory_copy` can dominate Set/Create/Publish without a
  standalone slow log when below threshold.
- `019f7970`: interactive report fixes taught that table and card filters,
  category downloads, edge-role filtering, and complete evidence exports should
  be independently validated.
- `019f79c0`: generated HTML/index artifacts need inline JS syntax checks,
  deduped quoted metadata, and live verification; bad report registration can
  break the whole homepage.
- `019f7b27`: failure traces should be split into issue-grade families:
  DataWorker UB/URMA server exec, RPC network residual, client deadline with
  fast server completion, EntryWorker processing late, remote_get/brpc mismatch,
  and QueryMeta/log-mixing anomalies.
