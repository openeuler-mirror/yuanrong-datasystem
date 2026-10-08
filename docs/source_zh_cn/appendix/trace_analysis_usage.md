# Trace 分析操作指导

目录重构设计：[RFC #1284](https://gitcode.com/openeuler/yuanrong-datasystem/issues/1284)。

## 1. 选择入口

统一使用 **`ds-trace-analysis-pipeline`** 一个 skill。它按请求选择完整报告、仅 Triage、
读写瓶颈、NUMA 或仅重绘模式；不要求专项问题也执行全套报告。原来三个专项 skill 已合并，
细则分别位于同一 skill 的 `references/triage.md`、`bottleneck.md`、`numa.md`。
“trace-triage”“bottleneck-analysis”“numa-analysis”是可单独请求的功能名称，不是独立 skill；
明确只分析读取或写入时，仅执行对应子命令和必需的上游校验。
后置分析消费同一份 Triage 产物，不另写原始日志解析器。

在仓库根目录执行：

```bash
python3 scripts/ds_trace_analysis.py --help
python3 scripts/ds_trace_analysis.py pipeline --help
```

独立部署时可构建 **Trace 专用 wheel**，不使用仓库根目录的产品 SDK `setup.py`。
运行时依赖见 `scripts/trace_analysis_dist/requirements.txt`（当前只有 Python 标准库），
构建依赖见同目录的 `requirements-build.txt`。最低支持 Python 3.9；wheel 内含页面模板、CSS、JS
和离线 ECharts。构建后在空目录安装并确认命令：

```bash
python3 -m pip install -r scripts/trace_analysis_dist/requirements-build.txt
python3 scripts/trace_analysis_dist/build_wheel.py --output /tmp/ds-trace-dist
python3 -m pip install /tmp/ds-trace-dist/ds_trace_analysis-*.whl
ds-trace-analysis pipeline --help
```

仓库入口与 wheel 的 `ds-trace-analysis` 使用相同子命令和参数。部署前仍需使用实际输入验证
模型、页面和离线资源；`--help` 成功不等于报告生成成功。

| 子命令 | 何时使用 | 所需输入 |
| --- | --- | --- |
| `pipeline` | 完整单/多 Run 报告、恢复或仅重绘 | Run 清单；仅重绘时使用已有模型 |
| `triage` | 只解析、聚合 Trace 并生成 Triage 页面 | 完整输入目录或归档 |
| `read` | 只生成读取瓶颈模型和页面 | 已完成的 Triage Run 目录 |
| `write` | 只生成独立写入细化模型和页面 | 已校验的 `bottleneck.analysis.json` |
| `numa` | 只生成 NUMA/WR 模型和页面 | 独立命令仍使用 Triage Run 目录及读取瓶颈模型；完整 pipeline 使用已校验的 Evidence |
| `suite` | 汇总已完成的各 Run | 各 Run 已生成的模型和报告路径 |
| `validate` | 检查单份中间模型 | 指定 kind 的 JSON |
| `package` | 离线依赖整理与 ZIP 打包 | 已验证的报告根目录 |

旧的独立脚本入口已移除。所有命令统一使用 `scripts/ds_trace_analysis.py <子命令>`，参数含义保持阶段原约定。
例如旧 `ds_trace_triage.py run` 改为 `ds_trace_analysis.py triage run`，旧 `ds_trace_bottleneck.py` 改为 `ds_trace_analysis.py read`，旧 `ds_trace_numa_analysis.py` 改为 `ds_trace_analysis.py numa`。自动化脚本需要同步替换调用路径；已有报告和中间产物格式不变。
从其他工作目录调用时，使用入口的绝对路径；输入、输出相对路径的解释仍按各命令约定。

局部任务可直接说“用 `ds-trace-analysis-pipeline` 只做 Triage”“只分析读取瓶颈”“基于已有模型只分析写入”或“基于已有 Run 做 NUMA 分析”。
提供现有 Triage Run 目录或模型路径时复用已校验产物；缺少必需的上游产物才补做相应阶段，不自动执行完整多 Run pipeline。

只做某个阶段时，可以独立执行，参数详情以对应子命令的 `--help` 为准：

```bash
# 首次解析：先获得命令返回的完整 Triage Run 目录
python3 scripts/ds_trace_analysis.py triage run /path/to/run-logs \
  --code-ref <verified-source-ref> --case <case-name> --out /path/to/triage-output

# 已有 Triage 产物：只生成读取页，避免顺带生成写入页
python3 scripts/ds_trace_analysis.py read --run-dir /path/to/triage-run \
  --top 0 --skip-write-page --source-ref <verified-source-ref> \
  --analysis-json /path/to/read.analysis.json --output /path/to/read.html

# 需要写入页时，消费模型中的独立写入记录
python3 scripts/ds_trace_analysis.py write --analysis-json /path/to/read.analysis.json \
  --output /path/to/write.html

python3 scripts/ds_trace_analysis.py numa --run-dir /path/to/triage-run \
  --bottleneck-analysis /path/to/read.analysis.json --archive /path/to/input.tar.gz \
  --source-head <verified-source-head> --source-base <verified-source-base> \
  --analysis-json /path/to/numa.analysis.json --output /path/to/numa.html
```

独立阶段不会猜测其他页面的路径；需要完整顶部互链和首页时使用 `pipeline`。

## 2. 准备 Run 清单并执行

### 从原始日志冷生成：输入契约与验收

目标流程固定为 **原包清点 → 按实验合并 core/time → 清单预检 → 唯一一次冷生成 → 校验 → 离线打包**。
每个 Run 的 `inputs` 只列完整目录或 tar 归档，`input_archive` 指向该 Run 的可读 tar；
独立 Trace 文件不能在完整 pipeline 中充当 cohort。需要比较单个日志文件时，使用独立的
`triage` 子命令。原包解压和每 Run 归档应记录 SHA256、文件数与耗时，避免把旧报告的模型当输入。

先运行不创建输出目录的预检，再用全新目录冷生成：

```bash
python3 scripts/ds_trace_analysis.py pipeline \
  --manifest /path/to/runs.manifest.json --preflight-only
/usr/bin/time -f 'wall_s=%e user_s=%U system_s=%S max_rss_kb=%M' \
  python3 scripts/ds_trace_analysis.py pipeline \
  --manifest /path/to/runs.manifest.json --output /path/to/new-report \
  --run-executor process --jobs 2
```

预检会在启动任意 Run 前检查清单类型、Run ID、全部输入路径、目录/归档粒度、tar 可读性与
解析输入归档的成员预算；清单中的 `pr` 如填写，应为 PR 编号（整数或十进制字符串），不能填 PR URL。
仅作溯源的 `input_archive` 不受解析输入预算截断。进程并发数仅为示例，应按可用内存和实测负载选择。单次冷生成的
`pipeline.validation.json.execution` 记录总墙钟、预检及汇总/发布耗时；每个 Run 的
`stage.execution.json` 记录阶段耗时和资源排队耗时。并行阶段会重叠，不能直接把阶段耗时求和
作为总墙钟。不要为了补齐阶段计时再次冷运行；失败后修正输入再使用 `--resume`，另记恢复耗时。
验收时检查 18 个 Run 的阶段/模型校验、每 Run 的 cohort 数及来源路径、首页与明细页图表、
`publication.validation.json`、离线链接和干净解压后的页面。

先检查输入包的目录结构，确认不同实验 Run 的边界。同 Run 的 core/time 采集合并到同一个
`inputs` 数组；不同 Run 不能混合去重。不要把目录包里的每个 Trace 文件单独当成一个 Run。
Triage 的第 2 章按每个顶层输入目录或归档形成 cohort；目录内的 Trace 文件保留逐行来源路径，
不再各自成为 cohort。无压缩 `.tar` 与 `.tar.gz` 都可供 NUMA 读取；`sampling` 如需填写必须是对象，
例如 `{"max_per_band": 500}`，类型错误会在启动任何 Run 之前报出。
下面是清单示例，所有尖括号字段都需要替换为真实值：

```json
{
  "schema_version": 1,
  "title": "Benchmark Trace 分析",
  "source_head": "<已核实的源码提交>",
  "source_base": "<已核实的基线提交>",
  "runs": [
    {
      "id": "run01-6clients-wr50",
      "label": "Run01 · 6 clients · WR capacity=50",
      "inputs": ["<同一Run的完整目录或归档绝对路径>"],
      "input_archive": "<原始归档绝对路径>",
      "top": 0
    }
  ]
}
```

增加 `runs` 条目即可分析多个 Run。`source_head` 是解释日志的源码参考，不能自动等同于实际
部署版本；部署版本未知时保留这一限制。`local_cache`、deadline、clients 和线程数仅在有
配置或日志依据时填写，未知时省略。`input_archive` 用于采集来源追溯；先准备可读归档。
Pipeline 始终分析全部已采集 GET/SET Trace，不能补回采集时未保留的日志。`view.read_top` 只控制读取页初始视图，允许 `0`（全部）、`100`、`1000`；旧 manifest 的 `top` 是此视图配置的兼容别名。两者同时填写时必须一致，其他值会在生成前拒绝。切换视图不改变模型、NUMA 样本或首页分母，也不使分析缓存失效。独立 `read --top N` 命令和旧 stage API 仍保留输入截断语义；完整分析使用 `--top 0`。

```bash
python3 scripts/ds_trace_analysis.py pipeline \
  --manifest /path/to/pipeline.manifest.json \
  --output /path/to/report \
  --jobs 1 --resume
```

`--jobs N` 接受任意正整数，默认 1，同时约束所有 Run 的重任务总并发，固定的 8 并发上限已移除。
`--jobs auto` 按可用 CPU 核数和内存计算：取 CPU affinity/cgroup v2 quota 与
“可用内存的 80% ÷ 最大阶段内存估计”的较小值；更小的 `execution.memory_mb` 会进一步收紧预算。
可用内存来自 Linux `MemAvailable` 和当前 cgroup v2 及祖先的剩余内存限制；不支持的系统可显式配置并发及内存预算。
Auto 模式要求所有阶段都有实测内存估计，缺失时拒绝生成，不猜测输入规模对应的峰值。
`pipeline.validation.json.execution` 记录核数、可用内存、最终 slots 和预算；`render-only` 同样支持 auto。
预算是启动时的估计值准入，不能替代执行中的进程内存监测。Evidence 校验后，
读取、写入和 NUMA 都直接消费已校验的 Evidence，资源预算允许时可并行；NUMA 不再等待读取模型。独立 `numa` 命令保留旧模型输入兼容。多 Run 可选择 `--run-executor process --jobs 2`，以独立进程分配 Run；
默认仍是线程模式。完整 pipeline 的 NUMA 页“观察分类”只根据错误状态、慢 WR 和 chip 观测形成，不复用读取页的主瓶颈归因；它不是 NUMA 根因判断。声明 `execution.memory_mb` 时，进程数还受各重阶段最大内存估计约束；
该限制依据声明值准入，不是实时 RSS 限流，因此必须监测所有子进程内存。
大包先在目标机器对比线程 1 与进程 2，依据实测耗时和内存决定是否增加；
同一输出目录只运行一个 pipeline 进程。
`--resume` 逐阶段核对输入内容、配置、实现版本与模型产物哈希；成功阶段可复用，
损坏或不匹配的阶段重建。旧 checkpoint 没有阶段清单时保守重跑。`--force` 强制重新执行。

可在 manifest 的 `execution` 对象配置 `memory_mb` 以及 `stage_estimates_mb`，后者须包含
`triage`、`evidence`、`read`、`write`、`numa`、`issues`、`render`、`suite` 八阶段的正数估计值，且单项不超过总预算。
旧 manifest 未配置 `evidence` 时，暂用 `read` 的估计值作为保守准入值；建议根据目标机器实测单独配置。
这是声明估计值的并发准入，不是 RSS 硬限制；未知估计拒绝运行，不自动猜测。未配置时只限制并发数。

评估并行收益时，固定同一 manifest、输入归档、代码 HEAD 和机器负载，对线程 1、进程 2
分别使用全新的输出目录记录冷生成时间；随后对同一目录运行一次 `--resume` 记录热生成时间。
例如在 Linux 上逐次运行（不要同时运行不同组）：

```bash
/usr/bin/time -f 'wall_s=%e user_s=%U system_s=%S max_rss_kb=%M' \
  python3 scripts/ds_trace_analysis.py pipeline \
  --manifest /path/to/pipeline.manifest.json --output /tmp/report-j1 --jobs 1
/usr/bin/time -f 'wall_s=%e user_s=%U system_s=%S max_rss_kb=%M' \
  python3 scripts/ds_trace_analysis.py pipeline \
  --manifest /path/to/pipeline.manifest.json --output /tmp/report-j1 --jobs 1 --resume
```

进程 2 使用独立目录和 `--run-executor process --jobs 2`。同时保存每个 Run 的 `stage.execution.json`、
`pipeline.validation.json`、输出目录实际大小以及读取/写入/NUMA 模型的语义比较结果。
阶段 wall 时间可能重叠，不能简单相加当作端到端时间；`--jobs` 较大并不保证更快。

2026-10-02 在 `tiantiyun-80c128g`（80 逻辑 CPU、Python 3.9.25）用同一份
Run01 输入归档（SHA256 `a8ed02e4092d70e66251ea62357b2f706ee50c7bd00d0733613d79a3fe674cd3`）
复制为四个独立 **合成 Run** 测试调度。每次从新输出目录冷生成，串行完成三轮，再依次执行四进程三轮；
复制数据只验证调度，不代表四次独立业务观测。分析包的 87 个源文件与当前代码逐文件哈希一致。

| 模式 | 三轮端到端秒数 | 中位数 | 相对串行 |
| --- | --- | ---: | ---: |
| 线程 `--jobs 1` | 123.16 / 129.35 / 130.90 | 129.35 s | 1.00× |
| 进程 `--run-executor process --jobs 4` | 73.37 / 76.25 / 78.32 | 76.25 s | 1.70× |

每轮四 Run 均通过 pipeline 校验；五组非基线输出与基线的四 Run × 读取/写入/NUMA 模型
共 60 次语义比较一致（仅忽略输出目录元数据）。串行/进程模式的阶段 wall 时间中位合计分别为：
Triage 44.07/47.39 s、读取 23.34/24.85 s、写入 6.78/6.31 s、NUMA 16.33/16.69 s。
并行阶段彼此重叠，以上合计不能相加推算端到端时间。`/usr/bin/time` 的 RSS 只覆盖
被测父进程，不能据此声称四个子进程的总内存峰值已受测量证明。
另一次真实三 Run、不等规模的冷生成，串行 185.52 s、进程 2 为 164.36 s（快约 11.4%）；
最大 Run 主导总时长，此单次结果不用于推断通用加速比。
原始计时、清单、每 Run 的 `stage.execution.json` 和校验文件保存在该主机的
`/home/root/ds-trace-perf-20261002/`；真实三 Run 数据保存在 `/tmp/ds-trace-pr2485-20261001/`。

## 3. 查验产物、重绘和打包

| 产物 | 作用 |
| --- | --- |
| `index.html` | 稳定入口；原子指向最近一次成功发布的首页 |
| `publications/<generation>/suite.manifest.json` / `suite.analysis.json` | 已发布版本的实际报告路径与汇总模型 |
| `pipeline.validation.json` | 本次执行状态及资源准入口径；失败不覆盖旧成功版本 |
| `publications/<generation>/publication.validation.json` | 已发布版本的 Run、模型与汇总语义门禁 |
| `runs/<id>/stage.execution.json` | 阶段命中、失效或失败原因，以及排队和执行时间 |
| Triage Run 下的 `manifest.json`、`inventory.json`、`summary.json`、`triage.json` | 输入来源、可校验的文件/归档成员清单与字节总量、规范化汇总及诊断证据 |
| 已发布目录下 `runs/<id>/evidence.json` | 绑定 `summary.json` SHA256 的紧凑公共观测事实；不重复保存原始日志文本 |
| `runs/<id>/generations/<stage>/<generation>/` | 经验证的阶段模型与 `stage.provenance.json`，缓存消费此处 |
| 已发布目录下 `runs/<id>/bottleneck.analysis.json` | 瓶颈分析模型副本 |
| 已发布目录下 `runs/<id>/write.refined.analysis.json` | 独立写入细化模型副本，含 `rows` |
| 已发布目录下 `runs/<id>/numa.analysis.json` | NUMA/WR 分析模型副本 |
| 已发布目录下 `runs/<id>/run.summary.json` | 首页消费的 Run 摘要 |
| 已发布目录下 `runs/<id>/issues.analysis.json` | 读写最终失败分别归类；独立记录错误 Trace、超时事件和未能重建的重试次数，保留缺证据边界 |
| 已发布目录下 `runs/<id>/provenance/` | 各阶段输入哈希、配置、规则版本、工具指纹与分析器构建修订；`+dirty` 表示构建时源码有改动，不等于被分析集群的部署版本 |

命令返回 `index`（实际发布首页）、`stable_index`（稳定入口）和 `manifest`（权威清单）。
不要把根目录兼容 `suite.manifest.json` 当作发布提交点。新执行失败时，稳定入口仍打开上一成功版本；
需要查看失败原因时读取根目录 `pipeline.validation.json`。成功时还应核对权威目录的
`publication.validation.json` 及所有请求的 Run。浏览器验收状态单独报告，不由模型校验冒充。
再通过首页打开 Trace、读取、写入、NUMA 页面，检查图表、筛选、分页、证据日志和下载。
离线包通过 `package.manifest.json` 映射保留每个 Run 的 `issues.analysis.json`，可在解压后核对问题分析明细。
浏览器回归覆盖 1500/1280/900/390px，检查图例和坐标轴遮挡；无 JavaScript 异常并不能代替图表数据校验。

只有模板、样式或交互变化时，可使用已有中间模型重新渲染：

```bash
python3 scripts/ds_trace_analysis.py pipeline \
  --output /path/to/report --render-only --jobs 3
```

此模式解析稳定入口指向的权威清单，生成新的发布目录并写出 `render.validation.json`；
旧发布页面及模型保持不变。历史平铺目录仍按其 `suite.manifest.json` 读取。
它不会重跑原始日志解析或读取归因；解析、归因逻辑变化必须重新分析。
未观测的 Worker/WR/时钟字段保留缺口，不能以 0 代替，也不能仅凭缺少日志断言 Pod 被 kill。

验证页面后制作可交付包，目标目录应与报告源目录分开：

```bash
python3 scripts/ds_trace_analysis.py package \
  --root /path/to/report \
  --entry index.html --manifest /path/to/report/suite.manifest.json \
  --output /path/to/report-offline \
  --zip /path/to/report-offline.zip
```

解压 ZIP 到新目录，再检查相对链接、图表和下载。打包成功不等于浏览器验收通过。
这些命令不要求发布网站；发布必须来自用户的明确请求。

## 4. 文件维护与回归

实现集中在 `scripts/trace_analysis/`：`pipeline.py` 负责流程，`stages.py` 提供包内阶段接口，
以只读 `StageResult.artifacts` 显式返回产物路径，不通过旧 CLI 或 stdout 查找路径。
`validation.py` 拥有持久化模型校验；pipeline 和 render-only 都依赖它，校验层不导入编排或渲染器。
`triage.py` 负责基础解析与
Triage，`bottleneck.py`、`write_report.py`、`numa.py` 负责专项分析，`suite.py` 和
`overview.py` 负责 Run 汇总与首页；解析、事实、归因和渲染职责已按下述模块拆分，兼容层保留既有算法。

字段解析维护于 `ingest/triage.py`；输入清点、归档读取和配额维护于 `ingest/inventory.py`。
`orchestration/contracts.py` 定义 Run 参数及解析产物，`orchestration/store.py` 管理 Run 目录和持久化。
公共 URMA 时间点、WR 事实及去重维护于 `evidence/urma.py`，RPC 事实维护于 `evidence/rpc.py`。
`evidence/errors.py` 统一提取读取与 NUMA 使用的超时、RPC 截止、send lane 和接收缓冲错误信号；
`analysis/write_base.py` 承载基础写入阶段预算，`analysis/write_pipeline.py` 从公共 Evidence
构建写入模型，`analysis/write.py` 承载写入细化模型。
读取阶段仍同时生成 GET 行与基础 SET 行，因此修改基础写入预算会使读取模型缓存失效；
读取 HTML/CLI 包装函数的修改不会使模型缓存失效。
`evidence/observations.py` 一次提取读取归因使用的 `evidence_facts`（schema 1），
包含 RPC 字段、Transport 阶段、QueryAndGet 尝试和超时观测；`analysis/read.py` 不再扫描日志原文。
完整 pipeline 在 Triage 后生成并校验 `evidence.json`，读取阶段消费其中的读观测和 `evidence_facts`；
缓存损坏时只重建该阶段及确实受影响的下游阶段。独立 `read` 命令仍可直接消费历史 Triage Run。
完整 pipeline 的写入阶段直接消费 Triage 摘要和 Evidence，不读取 `bottleneck.analysis.json`；
独立 `write` 命令仍接受已有读取分析 JSON，以兼容历史局部工作流。完整 pipeline 的 NUMA
阶段直接消费经校验的公共 Evidence；独立 `numa` 命令保留读取模型输入兼容。
`evidence/observation_validation.py` 校验字段类型、有限数值和来源索引，损坏事实阻止发布。
事实中的 `source_ref` 指向同一行模型的 `evidence` 或事实自带 `record_sources`；后者保存原
`evidence_records` 索引、文件/成员/行号和文本 SHA256，紧凑化时不依赖已删除的原文副本。
旧内存调用缺少事实时只适配一次；适配后不得原位改写该行原始证据，变化应重新构建模型。

`evidence/write.py` 提取写入专用 `write_evidence_facts`（schema 1），首先保存在 `evidence.json`
对应 Trace 的 `write` 字段，再随基础写入记录和 `write.refined.analysis.json` 持久化。它保留首条 Client 写入 access、按原顺序排列的 RPC、
Create/Publish 父窗口、错误命中及组件身份。写预算和 `analysis/write.py` 消费事实，不重新
解释日志文本；父窗口存在多次调用时不推断串并行后求和。每条事实引用当前 `evidence` 索引，
`source_hashes` 校验证据内容和顺序；版本、字段或来源损坏时停止发布。缺少新字段的历史模型
可通过 Evidence 兼容适配，重跑分析才持久化新契约，render-only 不修改模型。HTML 投影不携带
机器校验用的事实副本，但下载模型保留完整事实，页面证据日志也不裁剪。

写入细化模型的 `write_phase_schema_version=2`：`write_phase_observation` 分别记录
Create、Copy、Publish 父窗口的已观测/未观测状态，`write_rpc_phase_evidence` 分别保留
Create 与 Publish 中最长、完整且成功的 RPC 调用的 E2E、网络残差、排队、框架残差和
`source_ref`。这些 RPC 数值是可追溯旁证，不能与父窗口或 Client 互斥预算再次相加。
`wr_phase_attribution` 仅在有实际 WR 发送调用点证据时归入 Copy 或 Publish；只知道
bound/routed 路径仍不足以判定每条 WR 的发送阶段。通用校验器仍接受历史 schema 1
模型，但页面将其阶段字段标为“旧模型缺字段”，不写成“日志未观测”。`render-only` 要求
`write_phase_schema_version=2`；旧模型需通过正常 pipeline `--resume` 重算写入分析，
不能仅重绘页面。WR 调用点在原始证据中缺失时，即使重算仍保持未确认。
写入阶段的 `write.validation.json` 保留 `operation_counts`、`wr_coverage`、
`phase_coverage`、`wr_phase_counts` 和 `rpc_phase_observed`，用于核对各分母与
已观测/未观测/不适用状态。独立 CREATE 不进入 WR 适用分母，操作不明时
`wr_coverage.unknown` 单列。预算不闭合直接报错并给出 Trace ID；成功回执的
`budget_closure_bad_trace_count` 为 0。部分 chunk 日志无法证明同一逻辑 WR 的
完整成员关系，所以 `missing_chunk_count=null` 并说明原因，不能把未知写成 0。
`wr_coverage.observed` 表示该 Trace 有 WR 事件日志，不代表全部 chunk 齐全。
历史模型仍可校验，回执标识 `coverage_status=unavailable_legacy_model`，不伪造计数。

`events.jsonl` 使用事件 schema 2：每行有 `event_id`、`run_scope`、`trace_key`、
`role`、`component`、`worker_id`、`host_ip`、`process_id`、`thread_id` 和 `source_refs`。
身份取自该行，不能使用 Trace 聚合中的第一个 Worker。pipeline 传入真实 Run ID；独立 `triage` 子命令
未传 Run ID 时使用 `input_scope`/`unscoped`，不宣称全局 Run 身份。
`process_relative_ms`/`observed_gap_ms` 是同 Trace、同进程观测墙钟差，不能当成单调 elapsed；
只有显式日志字段 `process_elapsed_ms` 才进入真实 elapsed 字段，WR `elapsedMs` 是另一类耗时。
缺失字段为 null 并带 `missing_reasons`；墙钟回退不修正；未观测启动实例时
`process_instance_verified=false`，不能排除 PID 复用。

读、写、Triage 的页面实现位于 `rendering/`；读取归因位于 `analysis/read.py`，
聚合及关联位于 `analysis/aggregation.py`、`analysis/correlation.py`；写归因预算位于
`analysis/write.py`。`analysis/budget.py` 提供共享区间运算，不负责选择读写归因策略。
包内编排模块保留薄委托。读取聚合先由 `prepare_read_view()` 准备，再交 renderer；
写入 render-only 只消费 `write.refined.analysis.json`，不重新执行 `refine()`。

页面资源按 `assets/{shared,triage,read,write,numa,overview,vendor}/` 归类。
共用字体、颜色、图表生命周期、证据日志和导航应在 `shared/` 修改；单页逻辑进入对应页目录。
ECharts 属于工具资源，skills 仅描述使用流程。通过 `resources.py` 定位资源，避免散落的父目录
层级推算。缓存指纹需递归覆盖实现与资源，排除运行产物和字节码。
随离线报告分发的 ECharts 5.5.1 与官方 npm 发布包字节一致；`THIRD_PARTY/echarts/`
保留 Apache 许可证、NOTICE、d3 子组件许可证和来源记录。缺少声明文件时打包直接失败。

旧独立 CLI 已移除；修改分析行为进入 `scripts/trace_analysis/`，命令统一由 `scripts/ds_trace_analysis.py` 分派。
目录迁移、校验下沉和阶段接口迁移分别独立提交；回滚时按相反顺序撤销相关提交，
原始日志和已生成报告无需迁移。完整 pipeline 的读写分别从公共 Evidence 生成模型，
归因与阶段预算独立；兼容 `read`/`write` 局部命令仍保留旧模型衔接。
阶段模型缓存排除 CSS/HTML 资源；样式变化仅重绘。写缓存以 Triage 摘要、Evidence、
采集清单和写入相关配置为输入；
NUMA 缓存由 Triage 摘要、Evidence、归档和运行配置决定，读取归因规则变更不使 NUMA 模型失效。
阶段输出和报告分别存入独立 generation。模型与报告校验通过后才原子替换稳定首页入口；
进程中断测试验证旧版本仍可读取，不把这一保证扩大为整机断电持久性。旧 generation 不自动删除；
可直接打开旧版本的 `index.html` 回看结果。只更新样式无需重跑归因。
内部 `models_only` 阶段只返回真实模型路径，最终发布统一渲染一次；历史 stage/CLI 默认仍产出 HTML。
`rendering/registry.py` 与共用 `report_registry.js` 维护预期组件、编号和渲染状态。
组件的数据契约声明来源集合、实际字段及数值/文本类型；页面绑定完整模型和共享筛选数据函数。
校验器自行计算数据状态，不信任渲染器自报条数。模型非空却漏画、空筛选残留旧图/旧表、
必需字段缺失均失败；源为空、筛选无匹配、全部值未观测分别说明，不用“无数据”掩盖解析问题。
浏览器需滚动完成懒渲染并测试筛选，再执行
`ReportRegistry.audit({requireComplete: true, checkLayout: true})`；pending 或旧筛选 revision 不能算通过。
大型模型采用无缩进 JSON，减少序列化与复制开销；字段和校验规则不变，清单与诊断仍保留缩进。
人工查看模型时可用 `python3 -m json.tool <模型路径>`。渲染和交付校验复用各自已加载的模型，避免反复解码。
成功阶段保存 `stage.validation.json`，绑定 Run、阶段、缓存键及校验结果。热复用先核对全部产物哈希和规则版本，
再恢复有效收据，避免重复语义校验；收据缺失、损坏或规则变化时重新生成并校验。该机制面向本地可信缓存，不提供签名认证。
Trace 测试按职责位于 `tests/scripts/ds_trace_analysis/`，浏览器检查位于其 `browser/` 子目录：

```bash
python3 -m pytest -q tests/scripts/ds_trace_analysis
```

目录迁移后仍需比较同输入的中间 JSON 和页面；工具版本、生成时间及路径等来源字段要
单独解释，不得通过忽略任意差异掩盖分析结果变化。浏览器检查与测试脚本一起维护，不能只移动生产资源。

### 阶段边界的回归契约

| 设计约束 | 对应测试 |
| --- | --- |
| 编排不调用兼容脚本，不从 stdout 猜测路径 | `test_ds_trace_pipeline_contract.py` |
| 阶段返回显式产物、保留旧参数校验与输出，禁止进程状态串扰 | `test_ds_trace_stage_interfaces.py` |
| 校验不依赖 pipeline/渲染器；写覆盖率和预算校验保持 | `test_ds_trace_validation_ownership.py` |
| Run 输出隔离、失败门禁和恢复校验保持 | `test_ds_trace_analysis_pipeline.py`、`test_ds_trace_report_followup.py` |

这些测试位于 `tests/scripts/ds_trace_analysis/`。边界重构先添加失败测试再实现；归因公式变更必须另有人工核对的期望值。
完整验收还需在干净目录执行多 Run、resume、render-only、离线打包及真实浏览器检查。

## 5. 可直接使用的 Prompt

将占位符替换后发送给支持仓库 skills 的助手；无法自动发现 skill 时，明确提供
`.skills/ds-trace-analysis-pipeline/SKILL.md` 的路径。

```text
请使用 $ds-trace-analysis-pipeline 分析 <输入目录/归档> 的全部 Runs，输出到 <报告目录>。
源码参考 <source_head>，基线 <source_base>；重点关注 <异常现象或Run>。
先核对Run边界并生成manifest：同Run的core/time合并，不同Run隔离，top=0分析全部已有Trace。
使用统一入口 scripts/ds_trace_analysis.py pipeline，先 --jobs 1 --resume；多 Run 可实测 --run-executor process --jobs 2。复用仓库模板。
执行Triage与校验、独立读写瓶颈分析、NUMA分析，再生成单/多Run首页。
缺失Worker日志、未知配置和未观测时长如实标识；不要根据采样结果推断整场失败率或P99。
检查所有Run校验结果、图表数据、筛选与分页、1500/1280/900/390px排版和离线链接。
完成后交付首页路径、离线ZIP、关键结论及验证结果，不自动发布网站或推送代码。
```

只调整已有报告样式时可使用：

```text
请按 $ds-trace-analysis-pipeline 对 <报告目录> 执行 render-only，复用已校验的中间模型。
调整 <具体布局问题>，检查 render.validation.json，并回归首页及四类Run页面的图表和交互。
如果发现需要修改解析或归因逻辑，请明确说明并重新分析，不能把重绘当成重新诊断。
```

### Run 布尔字段契约

`allow_partial_inputs` 只接受 JSON `true` / `false`，省略时为 `false`，显式 `null` 无效。
`local_cache` 只接受 JSON `true` / `false` / `null`，省略时为 `null`（未确认）。
字符串 `"false"`、数字 `0` / `1`、数组和对象均不接受。pipeline 在任何 Run 的阶段启动前校验所有 Run；直接调用 triage/read 阶段也执行同一校验。
显式 `allow_partial_inputs: true` 可保留已解析数据及输入失败记录；`false` 遇到损坏输入则失败，不发布成功报告。
