# Performance Troubleshooting

## Document Metadata

- Status:
  - `active`
- Doc type:
  - behavior note | troubleshooting reference
- Primary code paths:
  - `src/datasystem/worker/worker_main.cpp`
  - `src/datasystem/common/log/*`
  - `src/datasystem/common/metrics/*`
  - `tests/perf`
  - `tests/st`
- Last verified against source:
  - `2026-04-13`
- Related design docs:
  - `.repo_context/modules/infra/observability/diagnosis-and-operations.md`
  - `.repo_context/modules/infra/logging/design.md`
  - `.repo_context/modules/infra/metrics/design.md`
- Related tests:
  - `.repo_context/modules/quality/tests-and-reproduction.md`

## Scope

- Paths:
  - `src/datasystem/worker`
  - `src/datasystem/common/log`
  - `src/datasystem/common/metrics`
  - `tests/perf`
  - `tests/st`
- Why this document exists:
  - record a repeatable way to localize latency, throughput, backlog, and resource-usage regressions using the signals already exposed by the repository.

## Bottleneck Classes

- Request-path contention:
  - access logs show rising elapsed time while basic health artifacts remain good.
- Background queue pressure:
  - monitor files or logs suggest exporter, flush, or async queue delays.
- Thread-pool saturation:
  - worker or master service thread-pool metrics degrade before full request failure.
- Disk or file-maintenance pressure:
  - compression, rolling, or heavy file I/O correlate with latency spikes.
- Backend or metadata pressure:
  - ETCD, OBS, or other backend success-rate families degrade alongside request behavior.

## First Investigation Order

1. Confirm the symptom window and whether the issue is latency, throughput, timeout, or backlog growth.
2. Check ordinary logs for startup or steady-state warnings first.
3. Check access logs and resource monitor files for the same time window.
4. Decide whether the first visible signal is request-path, exporter, thread-pool, disk, or backend related.
5. Reproduce with the narrowest matching ST or perf case from `tests`.

## Evidence Priorities

- Access or performance logs:
  - best for request-path latency and operation-specific slowdown.
- Resource monitor files:
  - best for memory, disk, thread-pool, and backend success-rate trends.
- Ordinary logs:
  - best for init failures, warnings, and background-task anomalies.
- Perf and ST tests:
  - best for controlled reproduction after the signal class is known.

## URMA Request Wait Slowdown

- Verified timing surface:
  - `src/datasystem/common/rdma/urma_manager.cpp` logs `[URMA_ELAPSED_TOTAL]` when a request exceeds 1 ms from just
    before `urma_post_jetty_send_wr` submission to write completion confirmation. The timestamp is captured when the
    event is created immediately before submit, so for chunked writes this is per-event lifetime latency rather than
    only the local blocking wait duration. The log includes request id, local source address, remote target address,
    data size, CPU id, status, and an embedded next-step suggestion.
  - If `[URMA_ELAPSED_TOTAL]` appears, check whether companion logs appear in the same time window:
    `[URMA_ELAPSED_THREAD_SHED]`, `[URMA_ELAPSED_POLL_JFC]`, and `[URMA_ELAPSED_NOTIFY]`.
  - `[URMA_ELAPSED_THREAD_SHED]` means `nanosleep(1us)` wake-up cost exceeded 100 us; route to OS scheduling
    overhead investigation.
  - `[URMA_ELAPSED_POLL_JFC]` means `urma_poll_jfc` cost exceeded 100 us; route to URMA analysis.
  - `[URMA_ELAPSED_NOTIFY]` means notify wake-up cost exceeded 1 ms; route to OS scheduling overhead investigation.
  - If `[URMA_ELAPSED_TOTAL]` appears but none of the companion logs appear, route to URMA and UDMA analysis.
- Verified error surfaces:
  - `src/datasystem/common/rdma/urma_resource.cpp` tags failed URMA resource calls with the underlying interface name,
    including `urma_create_jfr`, `urma_create_jetty`, `urma_import_jetty`, and `urma_import_seg`.
  - `src/datasystem/common/rdma/urma_manager.cpp` tags failed `urma_post_jetty_send_wr` logs with `[URMA_WRITE]` and
    failed `urma_poll_jfc` return or completion-record errors with `[URMA_POLL_JFC]`; these logs include the URMA
    return/status code and route to URMA further analysis.

## Current Signal Limits For `set/get`

- Verified request-path timing surface today:
  - client `set/get` style operations are primarily timed through `AccessRecorder` and related request-path logs, for example `src/datasystem/client/kv_cache/kv_client.cpp` and `src/datasystem/common/log/access_recorder.cpp`.
- Verified resource signal surface today:
  - `src/datasystem/common/metrics/res_metric_collector.cpp` collects periodic resource-style strings and flushes them through `HardDiskExporter`, not through typed request histograms.
- Verified observability gap today:
  - the repository does not expose a built-in `/metrics` style scrape endpoint under `src/datasystem`;
  - the common metrics subsystem does not currently publish request-level `set/get` latency buckets, percentile series, or byte counters in a typed metrics protocol.
- Practical implication for investigations:
  - when a user asks whether `set/get` `p95` or `p99` suddenly regressed, the first answer must come from access-log samples or perf tests, not from a live percentile metric stream;
  - correlating slow requests with thread-pool, disk, or backend pressure still depends on aligning access logs with monitor files and ordinary logs.

## Priority Improvements If Richer `set/get` Metrics Are Needed

- Per-operation latency histograms:
  - add typed latency buckets for `KV set/get`, `Object put/get`, and major batch variants instead of relying only on per-request access records.
- Request volume and byte counters:
  - add counters for request count, failure count, read bytes, and write bytes on the same operation families.
- Exportable metrics endpoint:
  - provide one repository-owned scrape surface so dashboards and alerting do not depend only on local files.
- Contracted dashboards and tests:
  - treat metric names, labels, and dashboard queries as compatibility-sensitive artifacts and validate them in tests, not only in ad hoc manual checks.

## Common Pitfalls

- Treating exporter lag as request-path regression.
- Ignoring monitor interval and buffer delay when comparing timestamps.
- Looking only at one log family when the shared exporter might be the bottleneck.
- Skipping reproduction and trying to infer all behavior from one noisy live sample.

## Update Rules For This Document

- Keep this file focused on performance-localization workflow, not on duplicating metric-family or logging implementation details.
- Update it when new recurring bottleneck classes or better reproduction routes are identified.

## Offline RPC attempt accounting

`trace_analysis/triage.py` preserves per-attempt `rpc_calls`, `client_processes` and
`query_and_get_calls` independently of the display-evidence cap. Bottleneck analysis
uses a maximum non-overlapping residual path within the unique Client process;
other processes, invalid clocks and overlapping attempts remain visible with reasons.
QueryAndGet RPC and Worker four-phase charts are separate parent/child observations.
See `docs/source_zh_cn/appendix/trace_rpc_accounting_rfc.md`; run
`python3 -m pytest -q tests/scripts/ds_trace_analysis/evidence/test_ds_trace_rpc_accounting.py` for the focused contract.

GET access, RPC and URMA log interpretation for the initial read row lives in
`scripts/trace_analysis/evidence/read.py`. `analysis/read_initial.py` assigns the initial
mutually exclusive Client budget, `analysis/read.py` refines its stages, and
`analysis/issues.py` classifies observed failure chains.
`analysis/read_rows.py` builds read Trace rows and topology from those facts;
`analysis/read_model.py` validates Triage inputs and assembles the read model.
`bottleneck.py` retains compatibility imports, CLI, and rendering/output adapters. Keep new
field-format parsing in the evidence module and verify the resulting model against an existing Run.
Triage persists `inventory.json` beside `manifest.json` with the same input identities, byte
totals and listed members. Pipeline validation rejects a mismatch before downstream analysis;
the stage cache hashes the inventory and rebuilds it after corruption. NUMA reuses this member
inventory only when its archive size and SHA256 match;
standalone or stale inventory falls back to scanning archive members. This avoids a second
gzip member walk without changing Core/Time cohort membership or Trace counts.
Stage provenance records the analyzer's Git revision with `+dirty` when its package or wheel
builder differs from HEAD. The standalone wheel embeds that build revision, while the tool
fingerprint still identifies its exact packaged bytes. Neither field asserts the deployed
DataSystem revision described by the report manifest.
The pipeline validates `issues.analysis.json` after independent read/write attribution. It
separates final failed GET/SET Trace counts from observed error Traces and timeout events;
retry-attempt count remains null until attempt reconstruction is supported. Issue groups
reference affected Trace IDs and preserve missing Worker-log evidence without asserting a
physical root cause. The suite validates these counts before publishing its homepage.

Triage trace accumulation and classification live in `analysis/triage_accumulator.py`;
schema assembly lives in `analysis/triage_builder.py`; its projections live in
`analysis/triage_dimensions.py`, `analysis/triage_flow.py`, and `analysis/triage_ub.py`.
Shared percentile behavior is in `analysis/triage_stats.py`. `triage.py` retains the
analyzer, CLI, and thin legacy class adapters.
Changing a projection must invalidate the Triage model cache and preserve the Run summary schema.
For directory inputs, Triage groups all contained Trace files into the top-level input cohort while
keeping the actual leaf path in each evidence source. Explicit inputs remain separate; noise-marked
groups retain their special comparison labels. NUMA archive fallback accepts compressed or plain tar,
and the pipeline rejects malformed manifest `sampling` before creating output or starting a Run.
The complete pipeline also preflights every Run's directory/archive roots and readable tar provenance
before starting analysis. `pipeline --preflight-only` makes this gate callable without output mutation;
one cold run records overall/preflight/publication wall time in `pipeline.validation.json` and
per-Run stage/queue time in `stage.execution.json`.

### 大 Trace 报告交互与读写输出

`trace_analysis/triage.py` 的 HTML 对证据使用惰性 JSON，全文搜索分片且取消过期结果，视口外图表延迟创建；
完整原始 JSON 通过下载按钮导出，不在首屏重复展开。回归入口为
`tests/scripts/ds_trace_analysis/rendering/test_ds_trace_triage_rendering.py` 和
`tests/scripts/ds_trace_analysis/browser/check_ds_trace_triage_visuals.js`。`trace_analysis/bottleneck.py` 有写入记录时自动生成
同名 `.write.html`，读取页和写入页互链；合并 analysis JSON 仍作为后处理输入。

读取总览分类由实际 Trace 集合生成，颜色表不能作为分类白名单。验证图表需对齐筛选 Trace 数与
阶段数值，不能只检查 JavaScript 无异常；对应回归见 `test_ds_trace_rpc_accounting.py`。
写入伴随页从内嵌 MODEL 下载完整 JSON，无需额外 JSON 文件才能打开下载入口。

### Write WR report

`trace_analysis/write_report.py` and `trace_analysis/assets/write/write.html` consume `write_wr_events` from the bottleneck model. The write page groups observed WR chunks by explicit destination and sender-local time buckets (100 ms / 1 s / 10 s / 1 min), with successful completion latency separated from non-success records. Unmapped targets retain their address; absent completion evidence is not a zero-latency WR.

URMA timeout accounting consumes original `urma_manager.cpp` timeout events preserved by triage independently of display caps. Per-process elapsed intervals are unioned before comparison with the Client transport parent; existing URMA budgets are retained without double addition. Propagated errors and successful WR completion counts remain separate.

### 图表分段交互与业务颜色

Trace、GET/SET 瓶颈和 NUMA 渲染器共用 `scripts/trace_analysis/assets/shared/charts.js`，通过 `TraceCharts.init` 创建实例。堆叠柱图悬浮时按同一 series 保持同类分段高亮，其他类型淡化；按当前分段显示名称、轴单位与可见同轴同 stack 分段合计占比，保留原始上下文和点击联动；该合计不代表多个阶段 P99 的端到端 P99。语义色固定为 URMA 通信橙色、RPC 网络蓝色、URMA 超时深红色、MemoryCopy 青色。新增别名在共用映射中维护，避免按数据顺序分配这些业务颜色。

页面标题、导航、说明和日志面板的浅色主题集中于 `scripts/trace_analysis/assets/shared/shared.css`。分段名称通过 `TraceCharts.label` 统一展示，保留原始 series 名称、统计字段和筛选键；耗时单位为 ms，错误类别不改写为耗时指标。移动端导航保留为顶部入口。

首页与 NUMA 页的直接子级 `body > nav` 使用 `trace_analysis/assets/shared/navigation.js` 同步滚动高亮；首页生成器内联同一资源。仅处理本页存在的锚点，设置 `.active` 和 `aria-current="location"`，在页尾选中最后一节；滚动更新通过 requestAnimationFrame 合并，窗口与内容尺寸变化会重新定位。不要把此逻辑重复绑定到已有滚动导航的读写瓶颈侧栏。

### 报告集成与防遮挡约束

以下是 Trace、读取瓶颈、写入瓶颈、NUMA 和问题分析页的生成、后处理与交付约束，
也是后续模板修改的验收要求，不表示现有渲染器已自动满足全部检查。

- 保留各分析页的原生布局和交互。跨 Run 总结、统一采样口径和问题分类集中在首页或问题分析页；
  不在明细页的原生布局外追加“本页……”等重复说明横幅。必要的局部证据限制放进已有说明区域，
  不能为清理遮挡而删除分析所需的采样边界或缺失证据说明。
- 左侧导航保持一致的入口名称、顺序与当前页标记；复用已有导航容器，不额外叠加第二套固定导航。
  桌面侧栏与正文必须留出各自空间，窄屏入口不能覆盖正文、筛选器或图表。
- 侧栏定位、视口高度、滚动和配色样式必须限定到明确的侧栏容器或专用类，不能以全局
  `aside`、`nav` 或其通用后代选择器赋予普通说明块 `sticky` / `fixed`、`100vh` 或侧栏配色。
  后处理新增元素要检查最终页面的样式继承与选择器冲突，不能仅根据插入片段判断布局安全。
- 本页滚动高亮仅处理有效的本页 `#id` 锚点并检查目标存在；跨页链接保持普通导航行为，
  不得将其 URL 传给 `querySelector`。集成时保留原有过滤、分页、下载与图表点击联动。
- 浏览器验收覆盖首页、Trace、读取、写入、NUMA 和问题分析页，选取小 Run 与大 Run，
  分别检查桌面和窄屏。初次加载时应能直接看到原生标题与主要内容入口；滚动到图表后，
  检查说明块、导航、图例及 tooltip 的实际遮挡、文字对比度和控件可点击性。
  还需点击本页锚点、跨页链接并操作筛选、分页和下载；无横向溢出或 JavaScript 无异常
  不能单独作为版式通过的依据。
- 在最终集成页面及解压后的离线分享包上执行检查，保存截图或浏览器检查结果。
  局部版式修复应核对未涉及页面的布局与交互；不将特定报告的批量删除规则、Run 数量或
  数据集路径写入通用生成流程。修改生成器时补充针对说明块与侧栏冲突的回归验证。

### 离线输出与写入交互保护

写入独立渲染器在读取前检查输入、HTML、固定 refined JSON 的路径及已有文件别名；
瓶颈批量输出同样要求所有目标互异，`--force` 不豁免目标冲突。重复生成不同目标仍保持原行为。
写入页顶部范围选择“全部 / Top 100 / Top 1000”，按 Client 总时延取最慢 Trace，统一驱动全页。总览的五档时延选择只进一步限定总览、时间序列和第6章 Trace 列表；写入阶段、WR、Worker 深挖仍使用顶部范围。第6章的搜索和明细筛选只作用于 Trace 列表与选中明细，不改变总览及深挖统计。错误分析先展示 Client 失败分类，再展示可重叠的问题证据；失败分类仅为日志可见信号，不外推最终根因。写入搜索合并150ms内输入，下拉筛选和重置即时更新并取消待执行搜索；防抖不保证一次完整搜索没有长任务。
写入导航在页尾选中最后一节，并在窗口尺寸变化时重新定位。Suite报告链接插入HTML属性前统一转义。
对应回归在 `tests/scripts/ds_trace_analysis/rendering/test_ds_trace_write_report.py`、`tests/scripts/ds_trace_analysis/analysis/test_ds_trace_bottleneck.py`、`tests/scripts/ds_trace_analysis/analysis/test_ds_trace_bottleneck_suite.py`；浏览器范围回归使用 `tests/scripts/ds_trace_analysis/browser/check_ds_trace_write_scope.js` 和 `check_ds_trace_write_overview.js`。

### 瓶颈页渲染与采集缺失判定

`trace_analysis/bottleneck.py` 使用读取模板的显式 runtime/init/correlation 插槽；
`trace_analysis/assets/read/read_query_breakdown.js` 承载读取 RPC 审计与 QueryAndGet 图表渲染，由生成器内联；
`trace_analysis/assets/read/read_init.js` 注册分段初始化，`report_runtime.js` 提供可见错误与
`ReportDiagnostics.audit()`。读取、写入页的浏览器验收需断言 audit.valid，不能仅检查HTML存在。
图表 `empty` 不等同于渲染失败，也不证明没有Worker执行。
首页、读取页和 Triage 页的服务端模板变量通过 `trace_analysis/rendering/template.py` 注入；
缺少映射时显式报错，不能把未替换的占位符留给浏览器。

`trace_analysis.diagnosis.worker_log_assessment` 消费triage结构化coverage与可选manifest中的
`worker_log_coverage`采集声明（按Trace ID映射）。无清单为coverage_unknown；
存在部分Worker阶段为partial；POD终止原因须由显式采集记录与evidence_refs支持。
具体字段及支持状态见 `.skills/ds-trace-analysis-pipeline/references/bottleneck.md`。该判定不改变RPC/URMA耗时归因。

- QueryAndGet 完成日志的 `preproc` / `preprocess` 由 `trace_analysis/triage.py::_ingest_query_and_get` 统一为 `phases_ms.preprocess`；`_query_and_get_breakdown` 为不可堆叠记录保留 `exclusion_reason` 与 `missing_phases`，缺日志与字段解析失败不得混同。

读写页通过 `trace_analysis/assets/shared/report_navigation.js` 将导航名称与正文题注同步，并按目标DOM顺序排列；图题置于图下，表题保留表上。浏览器回归校验目标存在、题注一致及代表性图表跳转。

Triage 的 `trace_visuals.js` 提供进程内相对事件时间线；时间线消费保留证据并显示覆盖边界。Triage 页面不再渲染读写 SVG 角色关系图，保留阶段证据表、Worker 统计图及其导航入口。相关纯数据回归为 `tests/scripts/ds_trace_analysis/rendering/test_ds_trace_visuals.py`。

读写瓶颈页的 `trace_analysis/assets/shared/bottleneck_timeline.js` 复用 `trace_visuals.js`，
提供独立的第8章事件时间线、Trace搜索、进程/日志组件筛选和证据表：筛选后不超过20条全部展示，超过20条每页4条。
优先读取 `evidence_records`，否则使用保留的 `evidence`；每个进程独立归零，
不推断跨进程先后或故障恢复时间。显示无时间戳及未保留证据数量。
`check_ds_trace_render_health.js` 验证时间线渲染、证据点击及无匹配筛选恢复。

读写日志及事件时间线共用 `trace_analysis/assets/shared/log_fields.js` 字段高亮：
URMA、RPC、Client/Worker summary 使用不同颜色，保留现有异常耗时标记。
字段着色不判断计数大小是否异常；原始日志转义后渲染，重复处理不嵌套字段标记。

读取页 `read_trace_stages.js` 将选中Trace的 `focus_breakdown_ms` 画为互斥阶段堆叠图，
置于阶段明细之前；逐WR图直接展示 `urma_requests.total_ms` 与 `wait_completion_ms`，
两者并列不相加。与Trace选择联动，无WR记录明确显示未观测。

事件明细的括号内 elapsed 是距同进程上一条保留事件的间隔，首条未定义；
组件/进程筛选保留原间隔，不将隐藏事件造成的间隙重新计算为执行耗时。

Pipeline 在同一Run的四份HTML生成后调用 `trace_analysis.navigation.link_reports`，
置顶切换栏按实际输出路径生成相对链接、当前标签页切换，并高亮当前页；首页进入Run保留新标签页。
单独生成器不猜测其他报告路径；打包方可用同一函数连接已完成的四份报告。
样式位于 `trace_analysis/assets/shared/report_switcher.css`；重复组装替换既有切换栏。

共用图表脚本在页面load后注册侧栏章节折叠：默认展开当前章节的图表子项，
滚动跨章节时切换展开组；箭头支持手动展开，不改变原锚点与图表内容。

NUMA 模板 `scripts/trace_analysis/assets/numa/numa.html` 按总览、错误与时延、NUMA/WR、Worker 与时间、Trace 查看组织为五章；源码链和证据边界放在附录。图注使用“图 章节-序号”置于图下，表题置于表上。跨报告入口由 pipeline 的顶部切换栏提供，NUMA 正文不再重复渲染报告链接。
图 4-3 从已归因的唯一 Trace 按 Worker 汇聚秒级序列，Top 5/10/20/全部以“失败或存在慢 WR 的唯一 Trace 数”排序，并支持逐个 Worker 切换。无法归属 Worker 或缺少首次观测时间的 Trace 分别标注；秒轴来自 Trace 首次观测时间，不代表 Worker 本地执行时间，也不用于跨节点因果判断。浏览器回归入口为 `tests/scripts/ds_trace_analysis/browser/check_ds_trace_numa_worker_seconds.js`。

事件时间线由 `trace_visuals.js` 统一生成 `component`、`offset_ms`（进程内首条证据起算）和 `elapsed_ms`（同进程上一条证据间隔，首条为 null）。读写页面复用组件识别；提示与原文同时显示累计及相邻间隔，不将日志间隔解释为执行时长。

读取页 `read_trace_stages.js` 将选中 Trace 的 Client 互斥堆叠条与全部 `urma_requests` 的 Chunk 时间窗集中到同一张图；隐藏逐 WR 时间点明细表；`readWrTimelineModel` 从 `trace_us` 计算相对 post 的区间，保留缺失和逆序标识。sleep 仅是日志记录的一次窗口，通知/唤醒结合 pre-completed、previous-event、valid 标志解读；区间重叠不可求和，缺少 CQ 就绪证据不归因网络或 sleep。切换 Trace 时复用统一图表实例。

Triage 日志框 6-3 使用进程内事件表：本地时间、累计/相邻 elapsed、组件与高亮原始证据。`TraceVisuals.uniqueEvidence` 只在同输入来源内按完整原始文件:行号日志文本合并采集副本，保留 origins；不同原始行号和不同来源不合并，unique_traces 索引不作为事件。该步骤只用于日志/时间线展示，不改写聚合指标和原始下载，页面须明确披露。日志框从原始日志路径识别 Client / Worker（不从消息中提及的远端角色猜测），按 IP/PID 或来源路径区分实例；支持角色、实例、组件与错误类别交集筛选，显示全部匹配行，不分页；筛选不重算进程内累计或相邻 elapsed。默认按进程分组后按本地时间排列，换进程累计归零；支持累计升序和相邻间隔降序，仅用于比较，不代表跨机器全局时序。错误类别仅表示日志标记，不作为根因或成功结论。

读写瓶颈的 Trace 证据日志由 `scripts/trace_analysis/assets/shared/trace_evidence_logs.js` 共用渲染，替代旧的重点 8 行加折叠分组。使用 evidence_records（缺失时回退 evidence），支持角色/实例/组件/错误类别交集筛选及三种排列，完整显示匹配的保留证据，仍披露上游截断；不改变聚合指标。

读取 URMA 章节只保留按时间和 Worker 的汇总图表；逐 Trace 统一从 Trace 查看进入，URMA 图点击仍可选择 Trace。不再生成独立“全部 URMA Trace”列表、筛选器或分页；导航复用表格外层已有标题，避免同一编号重复渲染。


中间产物门禁由 `scripts/trace_analysis/validation.py` 检查各 kind 必需容器类型和 Trace 标识，
读取模型的 `attribution_ms`、`focus_breakdown_ms` 分别校验有限非负耗时与 Client 预算闭合。
读写预算的固定阶段名必须全部存在；缺项显式失败，不能用默认 0 掩盖阶段漏计。
该门禁不证明 RPC/URMA 根因语义；仍需原始证据和浏览器验证。
离线导出使用 `trace_analysis/packaging.py`，同时收集静态链接与首页 `REPORT_DATA` 的 Run 链接和下载项，
所有本地依赖统一经过路径边界校验、流式去重复制及链接重写。
打包时将 `runs/<Run>/triage/<代次>/report.local.html` 的重复长代次目录映射为短哈希目录，
并重写首页与跨页相对链接；解压后应从 `package.manifest.json` 的映射定位页面，避免 Windows UNC 长路径无法访问。

同一 Run 的 core/time 采集副本在 `TraceAccumulator._ingest_latency_summary` 按带进程与时间的完整原始行去重后，再累计全部 latencySummary 字段；不同进程、时间或原始文件行的观测继续保留，不能按 Trace ID 去重整个请求。回归：`tests/scripts/ds_trace_analysis/evidence/test_ds_trace_evidence_accounting.py`。

`_urma_timeout_accounting` 不把完成日志的打印时间当作 post→completion 终点：日志在 WaitFor 返回后打印，而 total_ms 已扣除完成观察延迟。同进程同时出现完成与超时观测、缺少共同可靠时钟锚点时，分别保留 completion_observations 与 timeout_path_ms，urma_path_ms 保持未观测，不混合求并集或重分阶段预算。

### Trace tool package and entry point

Implementation and grouped assets live in `scripts/trace_analysis/`. `resources.py` owns asset discovery and recursive path/content fingerprints, including vendor resources; both pipeline and triage caches use this contract. `scripts/ds_trace_analysis.py` is the repository dispatcher; the wheel exposes the same commands as `ds-trace-analysis`. Package modules own each stage. Output paths remain compatible; explicit event/fact schema extensions are documented below. The single `ds-trace-analysis-pipeline` skill carries Triage, read, write, NUMA and report mode references. These modes may be used individually; they are not separate skills. See `docs/source_zh_cn/appendix/trace_analysis_usage.md` for commands, validation and rollback.

The pipeline calls `trace_analysis/stages.py` directly. Each stage returns explicit artifact paths in an
immutable `StageResult`; it does not mutate CLI arguments, change cwd, invoke the CLI dispatcher, or
infer paths from stdout. `validation.py` owns both persisted-model and refined-write checks, and neither
validation nor render_bundle imports pipeline. Run concurrency defaults to the existing thread
pool; `pipeline --run-executor process` instead assigns independent Runs to child processes and bounds
their count by declared maximum stage estimates when a memory budget is provided. The declaration
is not an RSS limit; measure child processes on the target host. `cached_stages.py` keeps content-verified per-stage model generations; `execution_budget.py`
admits stages by declared memory estimates, not measured RSS. After Evidence validation, read,
write, and NUMA model stages may run concurrently. The pipeline's NUMA model uses Triage Summary
and Evidence rather than the read model; the standalone NUMA command keeps its read-model input
for compatibility. `NumaEvidenceSpec` groups the independent stage's paths, config, and output owner.
The read stage continues to emit
base SET rows for compatible standalone consumers, so changes to `analysis/write_base.py` must
invalidate its model cache too.
Regression contracts: `test_ds_trace_pipeline_contract.py`, `test_ds_trace_stage_interfaces.py`, and
`test_ds_trace_validation_ownership.py` under `tests/scripts/ds_trace_analysis/pipeline/`. Formula changes require separate fixtures.
An independently installable Python 3.9+ wheel is built through `scripts/trace_analysis_dist/build_wheel.py`;
its zero-runtime-dependency `requirements.txt` and page assets are verified by `test_ds_trace_wheel.py`.

Trace responsibilities are split into `ingest/triage.py` (field parsing), `ingest/inventory.py`
(input inventory and bounded archive reads), `orchestration/store.py` (Run persistence),
`evidence/{urma,rpc,observations,errors}.py` (shared observed facts and error signals),
`analysis/read.py` (read attribution),
`analysis/{aggregation,correlation}.py` (summaries and evidence association),
`analysis/{write_base,write_pipeline,write}.py` (base budget, Evidence-fed write model, refinement),
and `rendering/{triage,read,write}.py`.
`analysis/triage_artifacts.py` owns machine-readable event and diagnosis construction;
these model transformations no longer live in the presentation module.
Compatibility facades retain the prior names. Read scopes/topology are prepared before rendering;
render-only validates and consumes the persisted refined write model without running attribution again.

URMA request deduplication in `evidence/urma.py` includes observed process owner and
`trace_us.post` alongside Worker, request ID, timestamp and chunk identity. Equal request IDs
from distinct processes or post clocks must remain distinct; absent identity fields remain unknown.
Regression: `test_ds_trace_urma_evidence.py::test_wr_identity_preserves_process_and_post_clock`.

Stage-cache primitives live in `stage_cache.py` and `stage_versions.py`; records verify content
hashes and publish completion metadata atomically. Pipeline write inputs are the Triage summary,
validated Evidence, Worker collection manifest and write-related configuration. `cache_inputs.py`
isolates NUMA-consumed fields from unrelated read presentation. `execution_budget.py` provides FIFO
slot and declared-memory admission; it is not an operating-system RSS limit.

Persisted Triage/read/NUMA model validation rejects explicit unsupported or malformed
`schema_version` values before interpreting records. Version 1 is supported; historical models
without the field remain accepted and report `schema_status=legacy_unversioned`. Write coverage
validation applies the same version guard to both source and refined models. Newly generated
`write.refined.analysis.json` declares version 1; legacy unversioned write models remain readable.

Logical WR chunk grouping also splits at sender/process or endpoint changes. A complete
chunk index sequence across different process clocks cannot establish a logical-write wall time.

Pipeline model-only stage calls avoid rendering intermediate HTML before publication. Historical
stage/CLI calls still render by default. `cached_stages.py` owns verified stage reuse and provenance;
all heavy stages share a `ResourceBudget`, including cache projection/validation and publication.
`orchestration/publication.py` commits immutable generations through one atomic stable HTML entry;
`orchestration/bundle.py` copies self-contained evidence/models and renders the new view.
`delivery_validation.py` and packaging resolve the authoritative publication, not stale root aliases.
An unsuccessful new attempt does not invalidate the previous published version. Publication tests
cover process interruption; this is not a claim of power-loss durability.

`rendering/registry.py` compiles expected page components from template captions and declares dynamic
components. Shared `report_registry.js` records render state and filter revision; final browser checks
must use `ReportRegistry.audit({requireComplete: true, checkLayout: true})` after lazy rendering.
Pending, missing DOM and stale filter revisions cannot stand in for successful rendering.

`validation.py` exposes pure `validate_data` and `validate_write_data` alongside the compatible
file APIs. Rendering and delivery validation reuse each loaded model rather than decoding it again;
large persisted models omit JSON indentation, while manifests and diagnostic records remain formatted.

`cached_stages.py` persists successful `stage.validation.json` receipts with generation artifacts.
Resume restores them only after artifact hashes, rule version and Run/stage/key bindings match;
missing or invalid receipts rebuild and revalidate. The cache assumes trusted local, single-writer storage.

Pipeline Top selections are presentation-only (`view.read_top`: 0/100/1000): all GET/SET/NUMA
models retain full Trace coverage and denominators. The independent `read --top N` subcommand still limits its persisted input; use `--top 0` for complete analysis.
`evidence/observations.py` persists read-rule inputs once as `evidence_facts` schema 1; read attribution
consumes these records instead of scanning messages. `evidence/observation_validation.py` checks typed
facts and resolvable provenance before publication. Compact `record_sources` survive raw-record removal.

`analysis/triage_artifacts.py` emits event schema 2 with per-line Client/Worker/component/process identity,
explicit Run or input scope and merged source references. `process_relative_ms` and `observed_gap_ms`
are same-Trace/process observed wall-time differences; only explicit `process_elapsed_ms` supplies
elapsed. Missing identity and wall-clock regressions remain explicit; PID lifetime is unverified.
Regression: `test_ds_trace_event_identity.py`, `test_ds_trace_evidence_observations.py`,
`test_ds_trace_pipeline_top_view.py`.

Registry component contracts declare model collections, fields and value types. Bound getters expose full
and scoped data using the same filter selectors as rendering; audit derives availability independently of
renderer counts. Non-empty observed data with no render and empty scopes with stale render both fail.
Missing model fields are errors, while source-empty, scope-empty and unobserved values remain distinct.

### URMA endpoint completeness

`ingest/triage.py` accepts `src addr` / `tgt addr` and legacy
`src address` / `target address`. `analysis/ub_edges.py` checks retained raw
`URMA_ELAPSED_TOTAL` evidence against parsed endpoints and aggregate counts;
a TOTAL with two endpoints but no corresponding event/edge is a validation error.
`dimensions.ub_summary.edges_by_operation` contains `read`, `write`, and `unknown`
buckets. Business flow evidence takes priority; ambiguous operations stay unknown.
URMA transport WRITE alone does not establish a business write. Counts represent
retained TOTAL observations under existing ingest deduplication, not unique WR IDs.
New models declare `edge_operation_schema_version: 1`; missing partitions fail
validation. Legacy unpartitioned models are explicitly marked
`legacy_unpartitioned`, and the page requests reanalysis instead of assigning all
edges to reads or claiming that no UB evidence exists.
Regression: `tests/scripts/ds_trace_analysis/evidence/test_ds_trace_ub_edges.py`.

Trace log presentation deduplication is shared in `assets/shared/trace_visuals.js`.
Original file:line prefixes identify collection copies even for multiline-error continuations
without timestamps. The full raw text and source scope remain part of identity; unlocated
text retains member/line identity. This changes displayed rows, not persisted aggregate counts.
Regression: `test_ds_trace_visuals.py::test_continuation_lines_merge_only_copies_of_the_same_source_location`.

### Persisted write observations

`evidence/write.py` owns write access/RPC/parent-window/error/identity interpretation.
`evidence.json` persists these facts for write-flow Trace IDs before attribution, and
`write_traces[*].write_evidence_facts` schema 1 preserves ordered RPC observations,
first Client access summary, deduplicated Create/Publish parents, issue sources and
ordered Client/Worker identity observations. Each observation references the row's
`evidence` index; `source_hashes` binds the complete ordered evidence list.
`analysis/{write_pipeline,write}.py` and `bottleneck._build_write_row` consume this contract without
regular-expression log interpretation. Legacy callers without facts use the Evidence
adapter; an existing invalid fact block fails instead of reparsing. Validation checks
schema, references, source hashes and field types, not authenticity against malicious
coordinated edits of evidence and facts. Parent windows with multiple observations
remain separate evidence and are not summed. First Client summary and last observed
Client identity remain intentionally distinct. The write cache fingerprint includes
both the fact parser and RPC parser; public CLI signatures remain unchanged.
Regression: `tests/scripts/ds_trace_analysis/evidence/test_ds_trace_write_facts.py`.

`evidence/errors.py` extracts URMA timeout, RPC deadline, send-lane and receive-buffer signals for
both read and NUMA diagnosis. It does not infer hardware root cause. NUMA builds these observations
once per Trace and removes the temporary block before persisting its model. Read model cache versions
exclude the `bottleneck.py` HTML/CLI wrappers but include model functions, `analysis/write_base.py`
and Evidence parsers. A change to write-base logic still invalidates read because read emits the
`write_traces` base collection. Regression: `test_ds_trace_error_observations.py` and
`test_ds_trace_stage_versions.py`. The error observer checks case-folded literal markers before
running whole-Trace regular expressions. Keep each marker a necessary condition of its regex;
compare all observation fields against real Triage summaries when changing these guards.

The full pipeline persists `evidence.json` after Triage and before read attribution. Its
`summary_sha256` and Trace ID set bind compact read observations and source-referenced
`evidence_facts` to one Triage summary. Raw log text stays in that summary; the Evidence
artifact does not duplicate it. The read stage consumes the validated observations,
while standalone historical read calls still adapt Triage directly. The pipeline write and NUMA
branches use validated Evidence without reading the GET model; standalone historical NUMA calls
still accept the read model. Stage-cache receipts cover Evidence;
the publication includes its model and provenance. Regression:
`tests/scripts/ds_trace_analysis/evidence/test_ds_trace_evidence_stage.py`.

### Trace pipeline resource admission

`python3 scripts/ds_trace_analysis.py pipeline --jobs N` accepts any positive concurrency;
`--jobs auto` uses CPU affinity/cgroup v2 quota and 80% of observed available memory, with a smaller
declared memory budget taking precedence. Supply measured `execution.stage_estimates_mb` for all stages;
unknown estimates fail before report production. The decision appears in `pipeline.validation.json.execution`.
This is estimate-based startup admission, not a live RSS hard limit. See `scripts/trace_analysis/execution_budget.py`
and `tests/scripts/ds_trace_analysis/pipeline/test_ds_trace_execution_budget.py`.
