# RFC：读写独立诊断、多 RPC 归因与大日志交互优化

状态：提议；本 PR 同时提供离线解析、归因与展示实现。适用范围：`ds_trace_analysis.py triage` →
`ds_trace_analysis.py read`，GET / SET 分离、run 独立。本文不修改服务端计时格式或运行时 RPC 行为。

## 问题与结论

一个 Client Trace 可以依次调用 URMA 建链握手、QueryAndGet、GetObjectRemote，写入则可能调用
握手、Create、Publish；同一方法还可能重试。仅按预设方法列表或每方法最大值计算，会遗漏不同
RPC 或重试；直接相加又会重复计算嵌套、并行以及其他进程的调用。

例如，同一 Client 进程的两个顺序调用分别记录握手网络残差 3.801ms、QueryAndGet 网络残差
0.621ms，应得到 4.422ms 的已观测串行路径，不能只显示 QueryAndGet 的 0.621ms。
`network_residual_us` 定位 RPC 通信与框架路径，不等于纯物理链路时延。

QueryAndGet 还有三个不同计时层：Client 调用窗口、RPC e2e、Worker total。
原总图中归并后的 QueryAndGet 大阶段不能替代 Worker 的 preprocess、localRead、metadata、delivery。
这些子阶段不能再次与父窗口相加，也不能把 localRead 直接当作 CPU 业务执行：它包括 inline
编码和交付，可能包含 URMA 等待。

## 输入契约与职责

解析层为每个 Trace 增加：

- `rpc_calls`：方法全名、日志发起进程 `(host, pid)`、日志时间、四个单调时钟时间戳、原始 `_us`
  字段、controller 错误状态、输入文件/行号引用。没有方法白名单。
- `client_processes`：从 Client access 日志独立获取；不通过服务名猜测 Client。
- `query_and_get_calls`：每条 Worker completion 日志的四阶段和 total；缺失字段保持缺失。

上述结构化事件在展示证据截断前解析，独立于 `evidence` 的行数上限。重复采集的相同事件去重，
不同时间的重试保留。既有汇总字段不删除；数据仍以 run 和 Trace ID 为边界。
后处理只消费新增结构化事件，不另写日志正则，也不导入解析器。原始输入包仍是最终复核依据。

## 网络残差计入规则

1. 保留所有方法、所有调用及排除原因，包括失败调用；controller 失败不自动否定完整时间戳。
2. 校验四个时间戳完整且单调、残差非负且不超过 e2e。用同一调用内的时间差检查：
   `ClientRecv - ClientSend - (ServerSend - ServerRecv)` 与残差一致，允许整数微秒记录的 2us 误差。
   不对不同主机的绝对时钟直接相减。
3. 只有从 access 日志唯一确定的 Client `(host, pid)` 可进入 Client 预算。其他进程、缺失身份、
   无效时序仍在全方法表显示，不能填成测得的 0。
4. 同一 Client 进程内，以调用的 `[ClientSend, ClientRecv)` 区间做加权区间调度：选取网络残差
   总和最大的非重叠集合。同方法重试与不同方法采用相同规则；接壤区间可顺序相加。
5. 对重叠调用，选取的是**保守的不重叠路径值**，不是并发调用残差总量，也不是完整关键路径证明。
   所有未选调用仍列出，标记 `overlapping_client_interval`；不能静默删除。
6. 新网络值替换既有网络桶，而非再次相加。只从计时差额、QueryAndGet/Get/建链父窗口剩余部分，
   或 SET Create/Publish 剩余部分转移预算，不侵占已独立归因的 URMA、拷贝等阶段。
   `network_ms` 保留原测量值；`attributed_network_ms` 和 `budget_clipped_ms` 暴露可用预算限制。
7. 无合法候选时 `network_ms=null`，不宣称“网络耗时为零”。旧版 summary 的既有归因作为兼容路径
   保留，但不能声称完成了全方法审计；报告提示重跑 triage 以生成新字段。

区间调度复杂度 O(R log R)，空间 O(R)，R 为该 Trace 的 RPC 次数。结构化事件增加离线内存和
JSON 体积；不会增加线上线程、锁、复制、日志或 RPC 开销。大规模输入仍受已有输入包大小限制。

## QueryAndGet breakdown

### Client / RPC 层

图 2-1 保留 Client 互斥预算。图 2-2 逐次显示 QueryAndGet RPC 的 network、server queue、
server execution、framework；只有时间戳与字段闭合才拆分。失败 RPC 的服务端计时缺失时保留
整个调用窗口，同时展示明确报错，不能凭总时延反推网络或服务端时间。

### Worker 层

图 2-3 使用 `WorkerQueryAndGetImpl::LogCompletion` 原始四阶段：

| 阶段 | 当前源码边界 |
| --- | --- |
| preprocess | 进入请求后到预处理 checkpoint |
| localRead | `PrepareLocalResponse`，包括 `EncodeLocalHits` 和 inline 交付 |
| metadata | `FillMissLocations` |
| delivery | `DeliverResponse` 到最终 checkpoint |

Worker total 与 RPC server execution 的开始、结束位置不同；分别成图，不能跨图按序号或
同 Trace 多次尝试的最大值强行拼接。四阶段缺失或合计超过 total 的记录保留而不强制堆叠；
小于等于 0.005ms 的舍入差异保留，正的阶段边界差单独显示。

同 Worker、同时间窗口唯一匹配的 URMA 超时仍沿用既有匹配规则。其明确等待窗口不能被另一个
已完成 WR 的较短耗时截断，也不标成“完成的 WR 耗时”。没有唯一匹配时，只保留错误标记。

## 页面与聚合一致性

- GET 页与独立 SET 页分别展示本操作的全部 RPC；GET 全方法表支持方法/Trace 搜索、分页、原始时间戳和排除原因核验。
- QueryAndGet 两层图跟随现有五档筛选，只面向 GET；SET 继续使用自身的 Create/Publish 模型。
- 五档图的类别来自实际数据，避免新增分类不在固定颜色表时被漏画。
- 多类别图例可翻页；验证桌面和窄屏下不覆盖绘图区。
- 明确错误、实测阶段与计时差额是不同维度；本 RFC 不把残差大小等同底层设备故障，也不改写
  Client 最终成功/失败结果。

## 读写页面与大日志渲染

沿用 PR #2308 的独立写入诊断、共享样式和精简离线打包。瓶颈 CLI 在有写入数据时自动生成
同目录 `<output-stem>.write.html`，读写页面互相链接；读取页不嵌入 SET Trace，写入页不展示
QueryAndGet 图。合并的 analysis JSON 保留兼容，独立写入 CLI 仍可使用。

明确 URMA、内存、建链及任意 RPC 方法错误单独列出。controller 错误与 Client 最终失败分开，
不能把已恢复的内部 RPC 报错统计为最终请求失败；无明确错误时显示实际观测窗口，不虚构底层根因。

Triage 首屏仅解析 Trace 元数据；证据、URMA 和新增 RPC 事件保存为惰性 JSON，按访问解析并缓存。
空搜索不扫描日志，全文搜索按 8ms 时间片让出主线程，后续输入取消旧搜索。图表在接近视口时创建，
重复筛选不重绘同一日志详情。附录默认仅预览当前 Trace，完整 JSON 下载分块序列化，保留全部字段。
这些改动优化浏览器交互，不缩减采集数据，不保证所有硬件上固定加载时间。

5,934 条 Trace、约 109MB HTML 的本地 Chromium 回放：基线首屏约 8.1s、最长任务约 2.8s；
两次优化后首屏约 1.7–2.3s、最长任务约 0.8–1.1s。该对照仅描述本机样例，不代表线上性能。
回归覆盖惰性载荷往返、空搜索不读日志、取消旧搜索、全文命中、完整 JSON 下载及滚动后图表初始化。

## 验证与验收

`tests/scripts/ds_trace_analysis/evidence/test_ds_trace_rpc_accounting.py` 覆盖多方法串行、同方法重试、重复采集、合法零残差、
失败但完整 trailer、缺失/矛盾时间戳、其他进程、嵌套/重叠、预算守恒、证据截断后结构化事件保留、
Worker 字段缺失及阶段合计、QueryAndGet 超时不被已完成 WR 截断。

运行 triage、bottleneck、suite 的既有测试以验证兼容性。对生成的实际 HTML 检查脚本执行、
QueryAndGet 四阶段、独立 GET/SET 页面跳转、搜索分页、五档筛选和窄屏布局。
分布式压测、线上 P99 改善、硬件与调度因果不属于这些离线测试的结论。

当前全量 Trace 回归命令（2026-10-02 迁移后 764 项通过）：

```bash
python3 -m pytest -q tests/scripts/ds_trace_analysis
node --check tests/scripts/ds_trace_analysis/browser/check_ds_trace_bottleneck_suite.js
python3 scripts/ai_context/validate_module_metadata.py
git diff --check
```

另以本机 Chromium 验证 1440/1000/390 像素读写页面、QueryAndGet 阶段值、RPC 搜索、五档联动，
并验证大日志滚动加载、单条 JSON 预览、全部 5,934 条 Trace JSON 下载且 evidence 字段保留。
这些原始日志与生成报告仅用于本地验证，不进入代码提交。

## 发布、兼容与回滚

维护者：trace 解析与瓶颈报告工具维护者；风险：中，主要风险是统计归因失真和离线资源增长。
新增字段为加法兼容；新报告需要新 triage 才能完整展示新增数据，旧输入会明确标出结构化证据缺失。
发布时先以已知 Trace 回放检查方法覆盖与预算守恒，再替换离线工具。回滚为恢复工具版本并从原始
输入包重新生成报告；不涉及公共 API、持久化格式迁移、线上恢复、并发共享状态或服务重启。

## 写入 WR 与超时等待计时

写入页新增独立 WR 章节，区分采集 WR 事件数、关联 Trace 数、成功完成数和非成功/未明记录。
按明确目标 Worker 或地址聚合，发送端本地时间支持 100ms、1s、10s、1min 分桶；
缺失时间不进入时间轴，目标未映射时不使用同 Trace 的其他 Worker 日志替代。
成功 WR 的 P50/P90/max 与 completion wait 分开展示；异步 WR 不求和，最终失败与单个 WR 成功独立。

解析层的 `urma_timeout_events` 只保留 `urma_manager.cpp` 原始超时事件，保存发出进程、时间、
request ID、elapsedMs 和源文件引用，不重复统计上浮错误；保留逻辑独立于展示证据上限。
同进程内按日志时间与 elapsedMs 重建区间并取并集，串行重试保留，跨进程取最大而不相加。
这种区间定位含日志点偏差，不是纯链路时延证明。

后处理以对应 Client 传输父窗口约束归因，只补已有 URMA 阶段未覆盖的部分。SET 的
`client.urma.ub_transfer` 已包含超时时不得再加一次；GET 后续等待不受首次超时或最慢成功 WR 截断。
优先从未细分传输预算补回，并保留已观测的 Client 本地处理时间；不增加 Client 总时延。
无匹配进程、缺少父窗口或计时超出父窗口时保留原始观测及未纳入量，不用强行分配掩盖证据边界。
每条 Trace 展示原始超时区间、URMA 路径、已计入预算、补回量及未纳入量。

RPC 校验同时修正：`ClientRecv - ClientSend` 对应 `remote_processing_us`，而不是包含 Client
框架前后处理的 `e2e_us`；后者只作为上界。保留 residual 重算一致性与缺失时钟校验。

## 读取总览分类与离线入口回归

读取总览按当前 Trace 的 `focus_primary_problem` 集合生成类别及颜色，筛选后重新计算；
颜色表不再充当分类白名单。计数图合计必须等于当前筛选 Trace 数，耗时取实测阶段字段，
未知类别不能静默归零。总览采用横向条形图并按类别数调整高度；长名称可查看完整 tooltip。
长 run 名称在 hero 区域允许换行，避免窄屏溢出。

写入页的完整模型下载直接序列化页面内 MODEL，不再依赖自动伴随页未生成的 JSON 文件；
独立写入 CLI 仍按原有契约输出 refined JSON。新增回归执行实际图表函数及 Blob 下载处理，
覆盖未知分类、筛选/空集合、计数守恒、非零阶段数值和模型完整性。

## 图表可读性约定

所有堆叠柱图悬浮某个分段时，同一图中同类型的全部分段保持高亮，其他类型淡化；提示仍对应鼠标所在分段。提示提供分段名称、数值、单位和同轴同堆叠内的可见分段占比；隐藏图例后更新分母，不把其他 stack、其他数值轴或 Client 总时延折线计入。保留原有 Trace 详情和点击筛选。缺失计时继续显示未观测，阶段分位数之和不解释为请求总时延分位数。

Trace、读取、写入及 NUMA 使用共用图表支持文件，URMA 通信固定橙色 `#f59e0b`、RPC 网络固定蓝色 `#2563eb`、URMA 超时固定深红色 `#b42318`，MemoryCopy 使用青色 `#0ea5a4`。业务别名共用语义映射，重排与筛选不改变颜色。颜色只编码业务类别，不改变原始计时或错误判定；超时标记仍与耗时分段区分。

成功 WR 的 P90、最大值和完成等待曲线沿用 URMA 色系，以实线/点线/虚线和点形区分指标，不以错误色表达成功 WR 最大值。

读写瓶颈页统一采用浅色页面、浅色标题卡片和左侧导航；窄屏导航转为顶部入口。分段展示名称统一为 URMA 通信耗时、RPC 网络耗时、URMA 调度/线程耗时、MemoryCopy 耗时及 Client 未细分耗时等。仅规范展示词汇，原始日志、分析字段、数值和筛选键不变；URMA 超时仍是错误类别，超时等待耗时单独按已有计时口径展示。

首页与 NUMA 导航共用滚动定位脚本，当前章节通过 `.active` 和 `aria-current` 表达。手动滚动、锚点跳转、返回顶部、窗口或内容尺寸变化均更新定位；页尾高亮最后一节，跨页面链接和不存在的锚点不参与。脚本无需 ECharts，首页可直接内联复用；保留读写瓶颈页已有的导航逻辑。
