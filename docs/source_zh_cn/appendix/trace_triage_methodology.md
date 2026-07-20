# DataSystem 慢时延与错误 Trace 分析方法论

本文沉淀一套可自验证、可随主线演进刷新的 trace 分析流程。它适用于给定慢时延或错误日志包后，从时间、worker、访问流程、关键日志字段、breakdown 和聚合分布中定位主要延迟族或错误族。

## 目标

输入可以是目录、普通日志文件、`.log.gz`，或 gzip 包裹的 tar trace 包。输出至少包含：

- 时间维度：首尾时间、密集窗口、异常 burst。
- Worker 维度：入口 Worker、Provider/Data Worker、目标地址、热点 Worker。
- 流程维度：Get、Set、Create、Publish、GetObjMetaInfo、RemotePull、BatchGetObjectRemote、Worker-Master RPC。
- 耗时维度：access latency、client summary、worker summary、rpc slow、URMA elapsed、锁和元数据操作。
- Breakdown 维度：`ProcessGetObjectRequest`、QueryMeta/CreateMeta、SafeObject lock、RemotePull、URMA wait/poll/notify。
- 错误维度：非 0 status、`RPC deadline exceeded`、`URMA_WAIT_TIMEOUT`、`Object in use`、`Key not found`、fallback rejected、Etcd 异常。
- 源码维度：固定 `main/master` ref，并用 CodeGraph + 源码交叉校验调用链。

## 快速入口

先跑脚本生成一个带时间戳的 run 目录：

```bash
python3 scripts/ds_trace_triage.py run <trace_dir_or_tar_gz> \
  --code-ref "$(git rev-parse main/master)" \
  --case <case-name> \
  --scenario <scenario> \
  --out /tmp/ds-trace-runs
```

run 目录包含：

- `manifest.json`：case、scenario、源码 ref、trace 时间范围、输入摘要和渲染目标。
- `events.jsonl`：逐 trace 的原始证据事件和 UB 事件，带 source/member/line/raw。
- `summary.json`：时间、worker、flow、latency、RPC、UB、error 等聚合维度。
- `triage.json` / `triage.md`：分类、issue candidates、代表 trace。
- `report.local.html`：本地自包含 HTML，可直接打开。
- `report.site.html`：yche.me 站点版 HTML 草稿，供后续发布流程使用。

旧的直接摘要入口仍可用于快速检查：

```bash
python3 scripts/ds_trace_triage.py <trace_dir_or_tar_gz> \
  --code-ref "$(git rev-parse main/master)" \
  --output-json /tmp/ds_trace_summary.json \
  --output-md /tmp/ds_trace_summary.md
```

脚本支持自验证：

```bash
python3 scripts/ds_trace_triage.py verify
python3 scripts/ds_trace_triage.py --self-test
python3 -m pytest -s tests/scripts/test_ds_trace_triage.py -q
```

这两个命令应接入 CI，作为日志格式和 parser contract 的低成本回归门禁。

当前自验证覆盖的契约包括：gzip-tar 识别、trace_id 归并、access us->ms 转换、`exceed 3ms` breakdown、`latencySummary` 原始文本和值解析、RPC slow 子字段、URMA 四类 elapsed 字段、UB request id/src/target/dataSize/cpuid/status/inflight 字段、时间桶、worker/edge 聚合、目录化 run 产物、本地/站点 HTML、错误族和分类聚合。DataSystem 日志格式演进时，应同一个变更里更新 parser、fixture 和测试。

## 当前主线校准流程

不要复用旧会话中的“latest”结论。每次分析先刷新并记录 ref：

```bash
git fetch main master
git rev-parse main/master
git log -1 --oneline main/master
```

需要源码因果时，在干净 worktree 上建 CodeGraph：

```bash
git worktree add --detach /tmp/ds-trace-main main/master
/home/t14s/.local/bin/codegraph init /tmp/ds-trace-main
/home/t14s/.local/bin/codegraph index /tmp/ds-trace-main
/home/t14s/.local/bin/codegraph callees WorkerWorkerOCServiceImpl::BatchGetObjectRemoteImpl --path /tmp/ds-trace-main
```

CodeGraph 用于发现符号和边，结论必须回到源码验证。`.worktrees`、生成代码和动态分发会导致重复或缺边，不能把“没有边”当成“没有调用”。

本次沉淀校准的 ref 为 `a7130ac9c3171bf3acb70601c7de99f7bc24f25a`。在该 ref 下，远端 Get 慢链路仍要关注：

- `ObjectClientImpl::GetFromTransportLayer` / `GetBuffersFromWorker`
- `ClientWorkerRemoteApi::GetObjMetaInfo`
- `WorkerOcServiceGetImpl::ProcessGetObjectRequest`
- `WorkerRemoteWorkerOCApi::BatchGetObjectRemote`
- `WorkerWorkerOCServiceImpl::BatchGetObjectRemote`
- `WorkerWorkerOCServiceImpl::BatchGetObjectRemoteImpl`
- `WorkerWorkerOCServiceImpl::MergeParallelBatchGetResult`
- `WorkerWorkerOCServiceImpl::WaitFastTransportAndFallback`
- `WaitFastTransportEvent`
- `UrmaManager::WaitToFinish`

主线已经包含 batch/aggregate gather、send-lane lease、fallback 等分支，所以报告应描述“当前 trace 实际命中的分支”，不要把历史单一路径写死。

## 分析顺序

1. 解包与输入确认：`.gz` 可能是 gzip tar，先 `tar -tzf` 看结构；解析输出必须放到输入目录之外，避免把 `summary.json` 当 trace 再扫进去。
2. Trace 归并：以 trace_id 为主键，保留来源文件、member、行号和精选原文。
3. 聚合先行：先给总 trace 数、时间范围、P50/P90/P99/max、worker Top、flow 分布、错误分布。
4. 延迟族分类：区分 20ms deadline、500ms/1s/2s URMA wait、rpc slow 4-8ms、锁/元数据小尾巴、无 summary 的 unknown。
5. 单 trace 证据：每个主要族选 top slow 和典型错误，保留足够日志上下文。
6. 源码校验：把日志里的 method、阶段和字段映射到当前 `main/master` 的函数和 timeout 传递。
7. 结论边界：明确 observed evidence、source-backed inference、unverified hypothesis。

## 错误 Trace 的几种切法

错误 trace 不能只按最后一条 ERROR 下结论，至少做以下几种正交切分：

1. **状态码 / 错误族切分**：按 access log 非 0 status 和错误文本聚合，例如 `RPC deadline exceeded`、`URMA_WAIT_TIMEOUT`、`Object in use`、`Key not found`、fallback rejected、Etcd 异常。先看数量和占比，再选典型 trace。
2. **Deadline budget 切分**：把 client access latency、RPC slow e2e、worker 完成时间、`reqTimeoutDuration.CalcRemainingTime()` 和配置 timeout 对齐。20ms client deadline 可以和 500ms 之后的 worker slow completion 同时存在，二者不是互斥证据。
3. **Worker ownership 切分**：区分 client、entry worker、provider/data worker、master 和 fallback target。日志没有显式打印目标 Worker 时，只标注“目标未显式打印”，不要用 IP 或目录名强行推断。
4. **Transport 切分**：分开 TCP、UB、URMA/RDMA、fallback 证据。`transportType:SHM` 或 tracker 默认值不等于请求实际走 SHM/UB；需要结合 slow log、payload source、fallback 日志和源码分支。
5. **URMA lifecycle 切分**：分别看 `URMA_ELAPSED_TOTAL`、`URMA_ELAPSED_POLL_JFC`、`URMA_ELAPSED_NOTIFY`、`URMA_ELAPSED_THREAD_SHED`、dataSize、CPU、inflight、source chip 和 target address。总耗时慢不自动等价于 poll 慢。
6. **源码演进切分**：每轮都刷新 `main/master`，用 CodeGraph 找符号，再回源码验证 timeout 传递、Batch/aggregate gather、send-lane lease、fallback 等当前分支。旧 trace 会话的结论只能作为 hypothesis。

## 字段字典

| 字段 | 含义 | 常见判断 |
|---|---|---|
| access log `cost` | SDK/Worker 访问耗时，单位 us | 用于 P50/P99 和 deadline 聚类 |
| `client summary` | Client 侧阶段摘要 | 判断 metadata、buffer、response、copy 分段 |
| `latencySummary:{...}` | Client/Worker 打印的阶段耗时原文和值 | 原文要保留，字段值用于聚合；不要重构后冒充 raw log |
| `rpc slow` / `ZMQ_RPC_FRAMEWORK_SLOW` | RPC 框架分段 | 区分 client framework、server queue/exec、network residual |
| `[Get] Done exceed 3ms` | Worker Get 分段 | `ProcessGetObjectRequest` 常用于判断远端拉取是否主体 |
| `[Get/RemotePull]` | Entry Worker 到远端 Worker 的同步拉取 | 和 Provider 侧 URMA 日志对齐 |
| `URMA_ELAPSED_TOTAL` | WR/Event 到 wait 返回的总耗时 | 大尾巴通常表示 completion wait，不等于业务 QueryMeta |
| `URMA_ELAPSED_POLL_JFC` | poll JFC 调用耗时 | 判断 URMA poll 本身是否慢 |
| `URMA_ELAPSED_NOTIFY` | poll 线程唤醒等待线程的耗时 | 判断跨线程唤醒/调度 |
| `URMA_ELAPSED_THREAD_SHED` | poll loop/sleep 调度间隔 | 判断 OS scheduling/sleep gap |
| `URMA_PERF` | URMA perf counters | 用于区分 write、poll gap、sleep、notify |
| `transferPath: UB/RDMA/TCP` | Worker Get/RemotePull 选择的传输路径 | 判断是否走 UB，不能和缺 URMA elapsed 混淆 |
| `src address` / `target address` | UB/RemotePull 边 | 用于 data worker -> entry worker UB write 边统计 |
| `request id` | URMA event 关联键 | 同一 trace 可有多个 request id，不能简单去重 |
| `urma_inflight_wr_count` | URMA event map 大小 | 判断 inflight 堆积和慢尾相关性 |

## 八个历史 Thread 的能力沉淀

这套方法来自 8 个 trace 分析会话的共同模式，不是单次报告模板：

| Thread | 产物/样本 | 应沉淀能力 |
|---|---|---|
| `019f753c` | 248 条 Get trace，页面 `/perf/ds-get-ub-remote-trace-rootcause-20260718.html` | gzip-tar 正确解包；按 trace 聚合；RemotePull、`ProcessGetObjectRequest`、`URMA_ELAPSED_TOTAL` 对齐；CodeGraph 只做发现，结论回源码 |
| `019f75a9` | 硬件端口隔离后 23 条 Round2 trace | 把秒级 URMA 尾巴和 20ms client WorkerRpc deadline 分开；不能用旧轮次根因覆盖新轮次 |
| `019f7606` | 12 条无基线噪声 trace，拓扑图 | 20ms deadline、RemotePull、QueryMeta、日志顺序错位要分别计数；角色图要区分 Client、Entry、Meta、DataWorker |
| `019f7686` | 4-8ms ZMQRPC sampled 报告 | `rpc slow` 必须解析 `server_exec_us`、`network_residual_us` 等子字段；每个 trace 支持下载 breakdown 和证据 |
| `019f76d0` | 273 条 Set/Create/Publish 写 trace | 保留原始 `latencySummary`；识别 `client.process.memory_copy` 主导；低于慢日志阈值也能通过 summary 解释 |
| `019f7970` | 04:00 错误日志互动页 | 表格/卡片筛选、当前类别下载、完整边统计、Entry/Data/Meta 角色过滤都要独立验证 |
| `019f79c0` | 首页白屏修复与 Top Trace 摘要移除 | HTML/索引产物必须跑 inline JS `node --check`、quoted metadata、去重、live 首页验证；Worker tag 过滤不应制造额外误导摘要 |
| `019f7b27` | 17 条 GET 失败 trace，页面 `/perf/ds-get-failure-noise-vs-clean-20260720.html`，#791-#796 | 失败 trace 要拆成 issue-grade 家族：DataWorker UB/URMA server exec、RPC network residual、client timeout but server fast、EntryWorker late、remote_get/brpc mismatch、QueryMeta/log mixing |

这些能力在脚本里对应为 `dimensions.latency_summary_us`、`dimensions.rpc_slow` 子字段、`dimensions.urma_elapsed`、`dimensions.classifications`、以及每条 trace 的精选原始证据。HTML 交互和 yche 发布验证保留在 skill 人工流程里，不强行塞进 parser。

## CI 集成建议

最小门禁：

```bash
python3 scripts/ds_trace_triage.py verify
python3 -m pytest -s tests/scripts/test_ds_trace_triage.py -q
```

扩展门禁可以在后续加入真实脱敏 fixture：

```bash
python3 scripts/ds_trace_triage.py tests/fixtures/trace_triage/*.tar.gz \
  --code-ref fixture \
  --output-json /tmp/trace_triage_fixture.json
python3 - <<'PY'
import json
data = json.load(open('/tmp/trace_triage_fixture.json'))
assert data['trace_count'] > 0
assert 'time' in data['dimensions']
assert 'workers' in data['dimensions']
PY
```

## 人工报告模板

每次输出报告时保持这个顺序：

1. 结论摘要：主要慢/错族、比例、最大影响。
2. 聚合分布：时间、worker、flow、latency、breakdown、errors。
3. 关键 trace 证据：每个族保留精选完整日志片段。
4. 当前源码链路：pinned ref、CodeGraph 查询、源码函数。
5. 解释边界：哪些是日志直接证明，哪些是源码推断，哪些需要补采样。
6. 后续建议：补字段、隔离 Worker、调整 timeout、拆分同步 wait、增加 inflight/queue 指标等。
