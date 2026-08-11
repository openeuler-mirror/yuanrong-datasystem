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
  - `.skills/ds-resolve-issue/SKILL.md`
  - `.skills/ds-resolve-issue/scripts/issue_intake.py`
  - `.skills/ds-pr-review/SKILL.md`
  - `.skills/ds-pr-review/scripts/review_pr.py`
  - `.skills/ds-pr-review/scripts/sensitive_scan.py`
  - `.skills/ds-pr-review/scripts/finding_validator.py`
  - `.skills/ds-self-verify/SKILL.md`
  - `.skills/ds-create-pr/SKILL.md`
  - `.skills/ds-create-pr/scripts/create_pr.py`
  - `.skills/ds-test/SKILL.md`
  - `.skills/ds-test/scripts/ds_test.py`
  - `.skills/ds-pr-comment-proc/SKILL.md`
  - `.skills/ds-pr-comment-proc/scripts/pr_comment_proc.py`
  - `.skills/ds-refresh-docs/SKILL.md`
  - `.skills/ds-refresh-docs/scripts/refresh_online_docs.py`
  - `.skills/ds-log-analysis/SKILL.md`
  - `.skills/rdma-ucx-perf-debug/SKILL.md`
  - `.skills/ds-design/SKILL.md`
  - `.skills/ds-design/scripts/self_check.py`
  - `.skills/ds-design/scripts/mermaid_lint.py`
  - `.skills/ds-design/scripts/scope_check.py`
- Last verified against source:
  - `2026-08-11`

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
| `ds-infra-engineering` | route implementation, debugging, refactor, design, and codebase Q&A through repository-level development gates for change decomposition, risk classification, qualified ownership, module boundaries, internal/public API quality, developer experience, misuse prevention, ownership/lifetime, production locatability, rollout/rollback, security boundaries, hot-path performance, concurrency, recovery, build/test behavior, and context updates | “实现/修复/重构/分析 datasystem 代码”, “修改 worker/client/common/master”, “性能/并发/恢复相关改动”, “infra engineering” | broad discussion of engineering philosophy without asking for codebase-specific analysis |
| `ds-resolve-issue` | resolve a concrete GitCode issue through authenticated intake, sanitized task specification, source-backed implementation, local review, static-first validation, one commit, fork push, and a conflict-checked PR | “解决 issue #572”, “修复这个 issue 并提 PR”, “take issue 572 to PR”, “用 ds-resolve-issue” | only asking to fetch, analyze, understand, or triage an issue; issue-template or issue-process discussion |
| `ds-pr-review` | review code, tests, scripts, docs, diffs, PRs, commits, or designs using strict infrastructure gates for correctness, design-contract compliance, internal/public API quality, naming clarity, developer experience, module locatability, production diagnosability, hot-path performance, concurrency/C++ safety, public API/config/docs coverage, Bazel/CMake support, mandatory changed-file sensitive-information scanning, line-count-based single-pass or parallel multi-round review planning, behavior-focused test quality, discussion lifecycle, and risk-calibrated rendered comments; when the target is a GitCode PR/MR number or URL, prepare a review bundle and publish validated high-confidence findings back to the PR page through the YuanRong PR review workflow | “review this diff”, “检查这个 PR”, “检视 1031”, “做代码评审”, “审查改动”, “PR review” | asking how the checklist works without requesting a concrete review; explicitly requesting local-only review |
| `ds-self-verify` | verify diff, tests, context updates, and infra risk before Codex claims work is complete or PR-ready, using the shared AI self-verification playbook | “完成前自检”, “self verify”, “准备提交/PR前检查”, Codex is about to claim completion after file changes | general questions about verification policy |
| `ds-test` | plan and run configured validation for code changes, including local command selection and remote validation through private user-provided SSH config | “验证这个改动”, “跑远端验证”, “跑测试并给 PR 验证结果”, “validate this branch”, “run ds-test” | asking what tests should exist in general, or discussing validation policy without asking to run validation |
| `ds-create-pr` | normalize the source branch to exactly one commit relative to the target base, push it safely to a fork, create a GitCode PR with a template-compliant body, then post `/retest` unless the target is `doc_pages` or all changes are limited to `.repo_context/` and `docs/` | “创建PR”, “提交PR”, “开PR”, “create pull request”, “open a PR”, “发起合并请求” | mentions of PR review policy, PR template, or PR conflicts without asking to create a PR |
| `ds-pr-comment-proc` | fetch unresolved GitCode PR review comments, prepare replies, mark discussions resolved, and verify final resolved state | “拉取 PR 评论”, “处理 review comments”, “回复并 resolve 评论”, “address PR comments”, “verify resolved comments” | asking how review comments work, or discussing PR review policy without asking to process comments |
| `ds-refresh-docs` | rebuild and publish online Chinese docs from the latest upstream `master` into `doc_pages`, then automatically open the GitCode PR | “更新在线文档”, “刷新在线文档”, “发布在线文档”, “refresh online docs”, “update zh-cn latest docs” | mentions of online docs, docs publishing, or `doc_pages` without clearly asking to refresh or publish |
| `ds-log-analysis` | analyze KVCache access/resource logs and generate HTML reports | “日志分析”, “access log 分析”, “resource log 报告”, “QPS/延迟/错误率趋势”, “KVCache report” | asking what the log format means without requesting report generation |
| `rdma-ucx-perf-debug` | diagnose RDMA/UCX throughput, latency, flush, submit, batch get, or resource lifetime problems | “RDMA 性能”, “UCX 延迟”, “UCP flush”, “BatchGet 远端拉取慢”, “P2P/RDMA crash” | generic mention of RDMA code ownership without a performance/debugging task |
| `ds-design` | author, revise, or review overview design (概要设计) and detailed design (子模块详细设计) documents for features, refactors, or submodules in yuanrong-datasystem, including requirement clarification when the entry point is vague, source-backed current-state investigation via subagent, chapter-by-chapter writing with per-section human gate, and structural self-check plus Mermaid lint and scope check | “写设计”, “设计文档”, “概要设计”, “详细设计”, “子模块设计”, “做个设计”, “改设计”, “修订设计文档”, “design doc” | discussion of design philosophy without asking to produce a design doc; already inside ds-infra-engineering coding flow |

## Skill Package Structure

Each repository-maintained skill should stay within this package shape unless a source-backed reason requires more:

- `SKILL.md`: canonical trigger description and workflow instructions
- `agents/openai.yaml`: UI metadata for skill discovery
- `scripts/`: deterministic helpers used by the workflow
- `references/`: detailed repo-specific rules loaded on demand
- `tests/`: focused validation for skill scripts when behavior is easy to regress

Repository-maintained skills must be reusable capabilities for this repository, not one-off wrappers for a single issue,
single feature, single PR, or temporary delivery plan. A canonical orchestration skill is justified only when it owns
cross-stage preconditions, state, side-effect boundaries, recovery, and a completion contract that ordinary trigger
routing cannot enforce. Keep issue-specific or feature-specific state in Git-internal task files or PR notes, not in a
new `.skills/<name>/` package.

All source and verification paths recorded in this document must be repository-relative, checked-in paths under the
source roots above. Do not reference personal home directories, local plugin workspaces, or machine-specific absolute
paths from this official repo context; move reusable workflow logic into `.skills/` or leave it out of the repository
skill registry.

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
- “解决 issue #572 并提 PR”
- “跑远端验证并整理 PR 验证结果”
- “处理这个 PR 的 unresolved comments”

### Composite issue development requests

Use `ds-resolve-issue` when the user's end goal is to solve or fix a concrete issue, whether or not the request repeats
“submit a PR”. The skill owns the full issue-to-PR state machine, including authenticated intake and task-spec creation,
and composes `ds-infra-engineering`, `ds-pr-review`, `ds-test`, `ds-self-verify`, and `ds-create-pr`.
It must announce each child-skill handoff and completion checkpoint with the current workflow state so the composition
is visible and resumable.

Requests that stop at fetching, analysis, understanding, or triage do not trigger the end-to-end skill; ask the user to
state the intended fix or resolution target. Use `ds-pr-comment-proc` separately only when the user later asks to
process review comments; the issue-resolution workflow stops after PR creation and conflict verification.

At each step, stop and give a friendly setup prompt when required configuration is missing. The prompt must be actionable
without asking the user to paste secrets into chat:

- GitCode API access: follow the local credential setup prompt from the active GitCode skill.
- Remote Linux validation: follow the private config setup prompt from `ds-test`, fill in the SSH target locally, then
  rerun `ds-test check-config`.
- macOS, Windows, and other non-Linux local hosts: treat them as orchestration hosts only; compile, CTest, Bazel, and
  remote validation require the local private `ds-test` config above.

Do not expose tokens, token-file absolute paths, private hosts, private ports, private usernames, remote paths, local
workspace paths, or raw sensitive logs in task state, PR bodies, review comments, or chat output.

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
- Add a new skill only when it is a general repository capability that can be reused across issues and features. Do not
  add skills that encode one issue, one PR, or one feature plan. Do not add wrapper-only orchestration skills; an
  orchestrator must own cross-stage state, safety gates, failure recovery, and completion verification.
- Keep the trigger phrases narrow and action-oriented; do not register broad topic words that would cause accidental
  execution.
- When a skill manages a repository artifact with frequent ambiguous mentions, record both:
  - high-confidence execution phrases
  - ambiguous phrases that require confirmation first
- When the trigger model changes, update the matching playbook
  `.repo_context/playbooks/upkeep/skill-trigger-routing.md`.

## Pending Verification

- None today.
