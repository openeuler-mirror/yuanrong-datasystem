# 0928 Trace 报告未识别与未分解审计

审计对象：0928 全部 18 个逻辑 Run，冷运行源码 `e2dbf70cf`，中间产物为各 Run 的 `summary.json`、`bottleneck.analysis.json` 和 `write.refined.analysis.json`。以下计数是修复前基线；不同字段会覆盖同一 Trace，不能相加。采集包是按 Trace 抽取的日志，不是完整 POD 生命周期日志。

| 现象 | 基线数量 | 判定与处理 |
| --- | ---: | --- |
| Triage 的 `workers` 含 `unknown` | 6,267 条 Trace | 已确认处理问题。`traceId` 续行没有标准日志头，解析器忽略了行首 `collected/client_<IP>` 或 `collected_worker_logs/worker_<IP>` 路径。按采集路径恢复进程 IP，保留没有可靠路径的 `unknown`。 |
| `[NO MATCH]` | 21 条 Trace，分布在 Run01/03/04 | 输入包只给出 Trace ID，但没有详细日志命中。属于输入证据缺口；不能据此推断请求没有执行或 POD 被 kill。 |
| 读模型 `coverage_unknown` | 6,740 条 Trace | 没有匹配到该 Trace 的 Worker 阶段，且缺采集清单；是证据覆盖状态，不等同于缺整个 Worker 日志。 |
| 写模型 `coverage_unknown` | 3,518 条 Trace | 同上。写模型里 Create 失败可能在到达 Worker 阶段前退出，需逐 Trace 判定。 |
| 读模型主阶段“未解释残差” | 2,381 条 Trace | 其中 2,375 条在细分模型仍有部分可观测阶段；主阶段残差表示剩余预算最大，不能解释为整条请求完全未知。其余 6 条没有可用的细分阶段。 |
| 写模型主阶段“未解释残差” | 1,219 条 Trace | 1,158 条是单独的 CREATE，Create 父窗口未观测；59 条 SET 三段父窗口都未观测；2 条 SET 只观测到 Publish。不能把失败 RPC 的耗时伪归因给网络或 WR。 |
| 读拓扑 `unknown` | 18 个 Run | manifest 未确认 `local_cache`，属配置证据缺口。不能仅据服务名推定 Client/Worker 调用拓扑。 |

可复核的样例：Run02 的 `getBuffer-211-187-00001653;c48644a78b4e` 在 Client access 中记录 20.677 ms，并在 Client INFO 中出现两次 QueryAndGet 失败和一次约 11.1 ms 的 URMA 建链信息交换。对应读模型的细分阶段识别了 11.174 ms URMA 建链，剩余 9.503 ms 未解释；主阶段选择“未解释残差”是因为残差最大。该 Trace 的 Client 日志明确出现目标 `192.168.178.174:31402`，但该 Trace 的抽取内容没有 `collected_worker_logs` 行；该 IP 也未作为本 Run 任一已解析 Trace 的日志发出方出现。这支持“目标 Worker 日志在当前采集证据中未关联”，不证明完整原始日志不存在，更不证明 POD 被 kill。

Run02 的 `createBuffer-129-186-00000012;5b199dd67ddd` 在 Client access 中记录 CREATE 失败 21.045 ms、目标 `192.168.89.45:31402` 和 URMA 信息交换 RPC 超时；当前 Trace 无该 Worker 的日志行，Create 父窗口也未观测，因而保留残差。要把缺失进一步判为 POD 提前终止，仍需目标 POD UID、退出/重建时间、采集范围和原始 Worker 日志清单；这些证据不在当前 Trace 包中。

对后续报告的判定规则：先看原始 Trace 文件是否有该行，再看 Triage 是否保留并识别，再看 Evidence/读写模型是否使用；若有原始字段而中间产物丢失，记为处理缺陷并加入回归用例。若只缺目标 Worker 阶段，明确写出 Trace ID、目标 IP、已找到的 Client 证据和缺失的 Worker 证据；没有生命周期记录时只标“未关联/疑似采集缺口”，不写“已被 kill”。

修复后用 `b21122314` 的解析器对相同 18 个 Run 从空输出目录重建，`pipeline.validation.json` 和 `publication.validation.json` 均为 `valid: true`。含 `unknown worker` 的 Trace 从 6,267 降至 **21**，剩余 21 条全部是上表的 `[NO MATCH]`，分布为 Run01 10 条、Run03 7 条、Run04 4 条；Trace 总数及 Run 分组均未变化。该核对证明续行身份解析缺陷已消除，但不改变未采集的详细日志和读写阶段预算证据。
