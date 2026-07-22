---
name: ds-design
description: Use when the user wants to write, revise, or review a design document for a feature, refactor, or submodule in yuanrong-datasystem — overview design (概要设计) or detailed design (子模块详细设计). Triggers on phrases like 写设计/设计文档/概要设计/详细设计/子模块设计/改设计/修订设计文档. Also use when the user has a vague idea and needs to clarify what to design before producing the design doc.
---

# Datasystem Design Doc Authoring

## Purpose

引导用户在 yuanrong-datasystem 中撰写、修订、检视**概要设计**与**子模块详细设计**文档。
与 `ds-infra-engineering` 的分工：本 skill 管设计阶段（从需求到文档产出），
`ds-infra-engineering` 管实现/工程阶段（从设计到代码落地的工程准备）。两者前后衔接，不重叠。

核心特性：
- 设计前必问"概要还是详细"，小功能可只做详细设计（概要非必须前置）
- 文档写作逐章/逐节 gate，每节用户确认后才推进
- 机械校验下沉到脚本（self_check / mermaid_lint / scope_check）
- 修订模式：基于 git diff 只 gate 改动章/节，未改部分跳过

## When To Use / When Not

**Use when**（高置信度执行）：
- "写设计/设计文档/概要设计/详细设计/子模块设计"
- "我想重构 X / 加个 Y"（模糊，需先理清做什么设计）
- "改设计/修订设计文档/更新设计"（修订模式）

**Not use when**（需确认或跳过）：
- 纯设计哲学讨论，不产出文档
- 已在 `ds-infra-engineering` 编码流程中（那里已含设计前置，本 skill 是独立设计入口）
- 改代码而非写设计文档

## Required Reading

1. `.skills/ds-design/templates/overview-design.md`（概要模板，内嵌写作指导）
2. `.skills/ds-design/templates/detailed-design.md`（详细模板）
3. `.repo_context/modules/overview/engineering-principles.md`（写 §4 边界时参考）

## Workflow

### 阶段 0：判明入口

**先判新建 vs 修订：**
- 用户说"改/修订/更新设计文档"或指定已有文档路径 → **修订模式**，走阶段 0R
- 否则 → **新建模式**，走阶段 0N

#### 阶段 0N（新建）

- 用户直接说"写概要/详细设计" → **直接入口**：跑阶段 1 的 Q3（跳 Q1/Q2）
- 用户说"我想重构 X / 加个 Y"（模糊） → **模糊入口**：跑阶段 1 全部 Q1/Q2/Q3
- **设计前必问**：正式开展前先问用户"这次做概要设计还是详细设计？"——小功能用户可选只做详细设计，概要非必须前置。用户明确选择后按阶段 3a 或 3b 开展。

#### 阶段 0R（修订模式）

- 问用户要修订的文档路径 → 读取旧文档
- 用 `git diff`（或用户指认）识别改动的章/节 → 产出"改动清单"
- **只对改动的章/节跑阶段 3 的逐章 gate + self_check；未改章节跳过 gate**
- **改动传播检查**：改动章的跨章依赖项强制复核（不降级为 INFO）。如改了 §2 目标 → §3 UseCase-目标映射、§4 机制-目标引用都要重跑并报告"因 §2 改动，以下跨章项需复核"。
- 阶段 4 自检：全量跑（不只改动部分），局部改动可能引入全局不一致。

### 阶段 1：需求澄清（新建模式）

目标：模糊想法/直接入口 → 调研起点。

一次一个问题：
1. 这次要解决什么问题？（痛点/场景）—— 模糊入口必跑，直接入口跳
2. 验收标准是什么？（可度量）—— 模糊入口必跑，直接入口跳
3. 这次涉及哪些端/域？（SDK/Worker/Master/Common 等，不确定就说"不确定"）—— 两种入口都跑

产出：需求摘要存 `.claude/plans/<topic>-requirement.md`（直接入口可精简，只含端/域）。

【gate】摘要给用户确认 → 通过进阶段 2。

### 阶段 2：现状调研（AI 自主派 subagent）

目标：为 §1 背景挖 `file_path:line` 证据。

**边界收敛规则：**
- 概要复用：若做详细设计且已有概要文档，先读概要 §1 的证据表，只补挖概要未覆盖部分，避免重复劳动
- 若阶段 1 的 Q3 答出端/域 → 在该域内定位候选模块（读 `.repo_context/modules/` 该域元数据）
- 若答"不确定" → 分两轮：
  1. 轻量轮：只读 `.repo_context/modules/` 该域元数据，产出候选模块列表
  2. 用户圈定候选模块 → 深度轮：subagent 深挖圈定模块的 src/ 证据
- **降级**：轻量轮若该域元数据为空（冷门域/新模块），回退问用户"你印象中这功能在哪个文件？给个关键词"，subagent 用关键词搜 src/（限该域目录），再产出候选模块

**subagent 失败恢复（三次重试）：**
1. 第一次失败：原范围重试
2. 第二次失败：缩小范围重试（切附录 A 的"接口定义版"prompt，只读头文件/接口定义）
3. 第三次失败：降级为 AI 自己读圈定模块（不派 subagent），证据标注"未深度调研，AI 自读"

调研扩面规则：先读用户圈定的模块 → 沿调用链扩到调用方/被依赖方。

证据量控制：证据表上限 15 条，超限按调用频度/热度裁剪，裁掉的标注"另有 N 条次相关"；用户可说"展开第 X 条"按需看。

【gate】证据表给用户确认（含证据对应代码片段摘要，非只给 file:line）→ 通过进阶段 3。

### 阶段 3：文档写作（逐章 gate）

按阶段 0N 用户选择走 3a 或 3b。修订模式（0R）只对改动章/节走对应流程。

每章/每节重复：
1. 填写本节
2. `self_check.py --section §N`（闭合项 BLOCK 阻断，跨章项半检 INFO 不阻断）
3. `mermaid_lint.py`（本节有图时）
4. 【gate】呈现本节 + 自检结果，问"本节通过吗？"
5. 通过 → 下一节；未通过 → 按反馈修订，重复 2-5

#### 3a. 概要设计写作（逐章 gate，§4 分节 gate）

对 §1 背景 → §2 目标 → §3 UseCase → §4 整体设计 → §5 参数：
- §1/§2/§3/§5：单章 gate
- §4（硬约束）：按子节分 4 次 gate —— §4.1 模块划分 / §4.2 模块交互 / §4.3 关键机制 / §4.4 边界约束

#### 3b. 详细设计写作（逐章 gate，§4 分节 gate，轻章合并 gate，可独立开展）

- **前置**：详细设计可无概要前置独立开展（小功能场景）；若有概要设计则作为参照输入。
- §1 需求背景 → §2 需求边界 → §3 UseCase：单章 gate
- §4 方案设计（硬约束）：按子节分 6 次 gate —— §4.1 类图 / §4.2 开发视图 / §4.3 关键交互 / §4.4 模块依赖图 / §4.5 关键数据结构 / §4.6 组件接口设计
- §5 对外接口 + §6 约束风险（合并一次 gate）
- §7 落地步骤 + §8 测试方案（合并一次 gate）

### 阶段 4：自检与交接

- `self_check.py` 全量（含跨章项，阻断）；若详细设计有概要参照，传 `--overview <overview.md>` 跑 N6 反向越界
- `mermaid_lint.py` 全文
- `scope_check.py`（仅概要）
- 全 pass → 【gate】问"文档终审通过吗？"
- 通过 → 指向 `ds-infra-engineering`（交接，不直接编码）
- 未通过 → 按失败项回阶段 3 对应章/节修订

**交接话术**："设计文档已完成并通过自检。若进入实现阶段，请使用 `ds-infra-engineering` 进入工程流程。"

## 降级路径

脚本不可用时（shell 故障、cwd 失效等），**覆盖阶段 3 逐章 gate 和阶段 4 终审**：
- 降级为 AI 按 self_check 规则清单逐项自查，mermaid 由 AI 肉眼审，scope 由 AI 判断
- gate 仍执行，自检结果标注"AI 自查，未脚本验证"
- 阶段 3 逐章 gate 降级时：闭合项 AI 自查可信度降级但 gate 不卡死；闭合项 fail 仍阻断，INFO 仍不阻断
- 终审降级补偿：gate 提示"脚本恢复后可补跑校验"；提示用户对 AI 自查结果中关键词近似项（目标可感知、UseCase 无内部组件、参数不罗列历史）重点人工复核

## 脚本

| 脚本 | 用途 | 调用 |
|---|---|---|
| `scripts/self_check.py` | 文档结构自检 | `python scripts/self_check.py <doc.md> --type overview\|detailed [--section §N] [--overview <overview.md>]` |
| `scripts/mermaid_lint.py` | Mermaid 语法扫描 | `python scripts/mermaid_lint.py <doc.md> [--fix-hints]` |
| `scripts/scope_check.py` | 概要越界检查 | `python scripts/scope_check.py <doc.md>` |

脚本可独立调用，不必走完整流程。

## 附录 A：现状调研 subagent prompt 模板

三种收缩变体，按调研深度递减，重试时依次降级：

**深挖版（默认）：**
```
任务：读 yuanrong-datasystem 的 src/ 挖现状证据。
范围：{用户圈定的模块列表}
扩面：先读目标模块 → 沿调用链扩到调用方/被依赖方。
输出：每条 = 路径:行号 + 一句话现状 + 缺什么。上限 15 条，超限按调用频度裁剪并标注"另有 N 条次相关"。
禁止：不写方案，不读测试代码。
```

**接口定义版（第一次重试缩小范围）：**
```
任务：只读 {模块列表} 的头文件/接口定义。
输出：接口签名 + 职责一句话。不读实现。
```

**头文件版（第二次重试再缩小）：**
```
任务：只读 {模块列表} 的 .h 文件列表。
输出：模块对外暴露的类/函数签名清单。
```

## 附录 B：self_check 规则与模板对应

| self_check 规则 | 来源（模板原文） |
|---|---|
| 现状结论带 file_path:line | 概要模板 §1 写作指导 |
| 目标用户可感知 | 概要模板 §2"从需求推导目标的方法" |
| 技术约束未在 §2 以目标出现 | 概要模板 §2 写作指导 |
| UseCase 外部视角无内部组件 | 概要/详细模板 UseCase 抽象层级规则 |
| UseCase-目标映射一一对应 | 概要模板自检清单第 5 项 |
| 模块接口只列对外 API | 概要模板 §4 写作指导 |
| 性能规格可量化 | 概要模板 §4 性能规格表 |
| 机制是"设计应对"非现象 | 概要模板"判断是否需要独立 D 机制" |
| 参数不罗列历史既有 | 概要模板 §5 写作指导 |
| 图在文字前 | 概要模板"图在前，文字在后" |
| 接口签名完整 | 详细模板 §4.1 类图规则 |
| 约束带违规后果 | 详细模板 §6 约束表 |
| 落地步骤分阶段 | 详细模板 §7 落地步骤 |
| 详细模块数不超概要 | N6 反向越界（无概要前置时跳过） |
| 详细接口在概要范围内 | N6 反向越界（无概要前置时跳过） |
