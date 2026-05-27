# Repository Skills

This document describes the repository-local official Codex skills under `.skills/` and the routing rules for deciding
when a natural-language request should invoke one of them.

## Metadata

- Status:
  - `active`
- Primary source roots:
  - `.skills/`
  - `.gitee/PULL_REQUEST_TEMPLATE/PULL_REQUEST_TEMPLATE.zh-cn.md`
  - `docs/README_CN.md`
  - `.repo_context/modules/overview/engineering-principles.md`
  - `.repo_context/playbooks/`
- Key verification paths:
  - `.skills/ds-infra-engineering/SKILL.md`
  - `.skills/ds-pr-review/SKILL.md`
  - `~/workspace/oh-my-codex/plugins/yuanrong-pr-review/skills/yuanrong-pr-review/scripts/review_pr.py`
  - `.skills/ds-self-verify/SKILL.md`
  - `.skills/ds-create-pr/SKILL.md`
  - `.skills/ds-create-pr/scripts/create_pr.py`
  - `.skills/ds-refresh-docs/SKILL.md`
  - `.skills/ds-refresh-docs/scripts/refresh_online_docs.py`
  - `.skills/ds-log-analysis/SKILL.md`
  - `.skills/rdma-ucx-perf-debug/SKILL.md`
- Last verified against source:
  - `2026-05-22`

## Purpose

- `.skills/` is the repository-local automation and workflow layer for repeatable Codex tasks in this repo.
- These skills are not generic notes; they encode the repo's canonical process for actions such as refreshing online docs
  or opening GitCode PRs.
- Natural-language routing should prefer these skills when the request is a high-confidence match for a registered
  workflow.
- `.skills/` remains Codex-specific; shared guidance for Claude Code, Cursor, and other AI coding tools lives in
  `AGENTS.md`, `CLAUDE.md`, `.cursor/rules/repo-context.mdc`, and `.repo_context/`.

## Current Skills

| Skill | Canonical use | Source-backed trigger phrases | Ambiguous mentions that require confirmation |
| --- | --- | --- | --- |
| `ds-infra-engineering` | route implementation, debugging, refactor, design, and codebase Q&A through repository-level infrastructure engineering gates | “实现/修复/重构/分析 datasystem 代码”, “修改 worker/client/common/master”, “性能/并发/恢复相关改动”, “infra engineering” | broad discussion of engineering philosophy without asking for codebase-specific analysis |
| `ds-pr-review` | review code, tests, scripts, docs, diffs, PRs, commits, or designs using correctness, sensitive-information, performance, concurrency, persistence, recovery, and test gates; when the target is a GitCode PR/MR number or URL, prepare a review bundle and publish high-confidence findings back to the PR page through the YuanRong PR review workflow | “review this diff”, “检查这个 PR”, “检视 1031”, “做代码评审”, “审查改动”, “PR review” | asking how the checklist works without requesting a concrete review; explicitly requesting local-only review |
| `ds-self-verify` | verify diff, tests, context updates, and infra risk before Codex claims work is complete or PR-ready, using the shared AI self-verification playbook | “完成前自检”, “self verify”, “准备提交/PR前检查”, Codex is about to claim completion after file changes | general questions about verification policy |
| `ds-create-pr` | create a GitCode PR for a pushed branch with a template-compliant PR body | “创建PR”, “提交PR”, “开PR”, “create pull request”, “open a PR”, “发起合并请求” | mentions of PR review policy, PR template, or PR conflicts without asking to create a PR |
| `ds-refresh-docs` | rebuild and publish online Chinese docs from the latest upstream `master` into `doc_pages`, then automatically open the GitCode PR | “更新在线文档”, “刷新在线文档”, “发布在线文档”, “refresh online docs”, “update zh-cn latest docs” | mentions of online docs, docs publishing, or `doc_pages` without clearly asking to refresh or publish |
| `ds-log-analysis` | analyze KVCache access/resource logs and generate HTML reports | “日志分析”, “access log 分析”, “resource log 报告”, “QPS/延迟/错误率趋势”, “KVCache report” | asking what the log format means without requesting report generation |
| `rdma-ucx-perf-debug` | diagnose RDMA/UCX throughput, latency, flush, submit, batch get, or resource lifetime problems | “RDMA 性能”, “UCX 延迟”, “UCP flush”, “BatchGet 远端拉取慢”, “P2P/RDMA crash” | generic mention of RDMA code ownership without a performance/debugging task |

## Skill Package Structure

Each repository-maintained skill should stay within this package shape unless a source-backed reason requires more:

- `SKILL.md`: canonical trigger description and workflow instructions
- `agents/openai.yaml`: UI metadata for skill discovery
- `scripts/`: deterministic helpers used by the workflow
- `references/`: detailed repo-specific rules loaded on demand
- `tests/`: focused validation for skill scripts when behavior is easy to regress

## Trigger Routing Model

### Direct trigger

Invoke the skill immediately when the user explicitly names it or gives a high-confidence imperative request that maps
to one registered workflow.

Examples:

- “用 ds-refresh-docs 更新在线文档”
- “更新在线文档”
- “帮我创建这个分支的 PR”
- “review this diff”
- “修改 worker object cache 的恢复逻辑”
- “完成前跑自检”

### Ambiguous mention

Ask for confirmation before invoking a skill when the user mentions a managed artifact or workflow area but does not
clearly request execution.

Examples:

- “在线文档现在是怎么更新的”
- “doc_pages 分支是干什么的”
- “PR 模板里验证结果要怎么写”

### Non-trigger discussion

Do not invoke the skill when the user is asking for explanation, review, debugging, or policy clarification rather than
execution.

Examples:

- “ds-refresh-docs 现在是从哪个远端构建的”
- “帮我检查 ds-create-pr 的 token 提示逻辑”

### Tie-break rules

If multiple skills could apply:

1. Prefer the skill whose output is the user's explicit end goal.
2. If one skill produces an artifact required by another, invoke the producer first.
3. If intent is still ambiguous after source-backed routing, ask a narrow confirmation question instead of guessing.

## Maintenance Rules

- When a repository-local skill is added, removed, renamed, or materially repurposed, update this file in the same
  change.
- Keep the trigger phrases narrow and action-oriented; do not register broad topic words that would cause accidental
  execution.
- When a skill manages a repository artifact with frequent ambiguous mentions, record both:
  - high-confidence execution phrases
  - ambiguous phrases that require confirmation first
- When the trigger model changes, update the matching playbook
  `.repo_context/playbooks/upkeep/skill-trigger-routing.md`.

## Pending Verification

- None today.
