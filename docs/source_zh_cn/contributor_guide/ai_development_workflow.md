# AI 开发流程

openYuanrong datasystem 在 `.skills/` 中配置了 10 个 AI 辅助开发 skill，覆盖从 issue 拉取到 PR 创建的完整开发流程。本文档面向希望了解或参与 AI 辅助开发的贡献者。

---

## Skills 总览

### 主链 Skills

| Skill | 触发示例 | 用途 |
|-------|---------|------|
| **ds-issue-intake** | "拉取 issue 572"、"分析这个 issue" | 从 GitCode 拉取 issue，脱敏敏感信息，生成结构化任务 spec |
| **ds-infra-engineering** | "修改 worker client cache 逻辑"、"分析 datasystem 代码" | 实现前的工程门控——变更分解、高风险面识别、API 审查、性能/并发/恢复评估 |
| **ds-pr-review** | "检视 1031"、"review this diff" | 多轮深入代码检视——正确性、API 设计、性能、并发、C++ 安全、敏感信息，发布 finding 到 PR 页面 |
| **ds-test** | "跑远端验证"、"验证这个改动" | 规划并运行本地/远端验证（build.sh / CMake / Bazel / CTest） |
| **ds-self-verify** | "完成前自检"、"提交前检查" | 完成前自检——验证 diff、测试、上下文更新，禁止无证据声称"完成" |
| **ds-create-pr** | "创建 PR"、"提交 PR" | 调用 GitCode API 创建 Pull Request，模板合规校验 + 敏感信息扫描 |
| **ds-pr-comment-proc** | "拉取 PR 评论"、"处理 review comments" | 处理 PR review 评论——拉取、回复、resolve discussion |

### 专项 Skills

| Skill | 触发示例 | 用途 |
|-------|---------|------|
| **ds-refresh-docs** | "刷新在线文档"、"更新中文文档" | 从 master 编译中文文档，同步到 doc_pages 分支，自动创建 PR |
| **ds-log-analysis** | "分析 access log"、"生成 KVCache 报告" | 分析 KVCache 日志，生成 QPS/延迟/命中率 ECharts 交互式 HTML 报告 |
| **rdma-ucx-perf-debug** | "RDMA 性能问题"、"UCX 延迟高" | 诊断 RDMA/UCX 吞吐、延迟、flush、batch get 性能瓶颈 |

---

## 开发主链

标准的 issue → PR 流程：

1. **ds-issue-intake**：拉取 issue，脱敏，生成结构化任务 spec
2. **ds-infra-engineering**：分析代码路径，识别高风险面，选择验证策略 → 实施最小改动
3. **ds-pr-review**：多轮深入检视——正确性、性能、并发、C++ 安全，发布 finding
4. **ds-test**：规划验证命令 → 跑本地/远端编译和测试 → 产出验证报告
5. **ds-self-verify**：diff 审查 → 工程门控 → 上下文更新，确认 PR 就绪
6. **ds-create-pr**：模板合规校验 + 敏感信息扫描 + 创建 PR
7. **ds-pr-comment-proc**：处理 review 反馈，回复/resolve discussion

对于 "帮我解决 issue#572 并提 PR" 这样的组合请求，AI 会自动串联上述 skills，无需手动逐个调用。

---

## 检视规则

ds-pr-review 按以下层次执行检视：

1. **Review Focus**——重点：低时延、高性能、高并发、高可靠、高可用、死锁预防、coredump 预防。要求多轮深入检视，不满足于单轮 happy-path 检查。
2. **11 条 Strict Review Passes**——声明可追溯性、模块内聚、功能正确性、热路径性能、C++ 安全与并发、公共接口与文档、构建与打包、测试、生产可诊断性、讨论生命周期
3. **13 条 System-Wide Design Gates**——内部 API 设计、开发者体验、防误用、所有权与生命周期、热路径参数开销、抽象边界、一致性、跨语言边界、构建闭包、测试契约、可运维性
4. **Comment Quality Gate**——每个 finding 必须引用具体证据、给出修复方向，按影响校准严重度
5. **Sensitive Information Gate**——扫描凭据/IP/私钥/敏感目录路径，prepare 阶段前置执行
6. **pthread mutex 禁止**——基于 bthread，禁止使用 pthread 同步原语，要求 bthread::Mutex / std::mutex / 无锁结构

---

## Infra 工程原则

ds-infra-engineering skill 强制执行以下纪律：

| 原则 | 内容 |
|------|------|
| 变更分解 | 单个 PR 只做一个连贯改动 |
| 高风险面识别 | 共享 API、RPC、持久化、恢复、并发、热路径 |
| 复用优先 | 加新逻辑前先搜已有 helpers、工具函数、线程池 |
| API 契约审视 | 内部 API 当半公开契约审：命名清晰、防误用、所有权明确 |
| 验证证据驱动 | 按风险选验证方式：定向测试、构建检查、性能证据、sanitizer |
| 回滚计划 | 记录默认行为、rollout、rollback、兼容性 |

详见 `.repo_context/playbooks/features/infra-engineering-workflow.md`。

---

## 文档索引

| 文件 | 用途 |
|------|------|
| `.repo_context/modules/overview/repository-skills.md` | Skill 注册表、触发路由 |
| `.repo_context/playbooks/features/infra-engineering-workflow.md` | Infra 工程开发流程 |
| `.repo_context/playbooks/features/performance-change.md` | 性能改动 playbook |
| `.repo_context/playbooks/features/concurrency-and-memory-safety.md` | 并发与内存安全 playbook |
| `.repo_context/playbooks/features/recovery-and-persistence.md` | 恢复与持久化 playbook |
| `.repo_context/playbooks/upkeep/ai-self-verification.md` | AI 自检 playbook |
| `.repo_context/modules/overview/engineering-principles.md` | 工程原则 |
| `AGENTS.md` | 项目级 AI 指令 |
| `CLAUDE.md` | Claude Code 入口指令 |
