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
  - `.skills/ds-pr-review/scripts/review_pr.py`
  - `.skills/ds-pr-review/scripts/finding_validator.py`
  - `.skills/ds-self-verify/SKILL.md`
  - `.skills/ds-create-pr/SKILL.md`
  - `.skills/ds-create-pr/scripts/create_pr.py`
  - `.skills/ds-issue-intake/SKILL.md`
  - `.skills/ds-issue-intake/scripts/issue_intake.py`
  - `.skills/ds-test/SKILL.md`
  - `.skills/ds-test/scripts/ds_test.py`
  - `.skills/ds-pr-comment-proc/SKILL.md`
  - `.skills/ds-pr-comment-proc/scripts/pr_comment_proc.py`
  - `.skills/ds-refresh-docs/SKILL.md`
  - `.skills/ds-refresh-docs/scripts/refresh_online_docs.py`
  - `.skills/ds-log-analysis/SKILL.md`
  - `.skills/rdma-ucx-perf-debug/SKILL.md`
- Last verified against source:
  - `2026-06-18`

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
| `ds-pr-review` | review code, tests, scripts, docs, diffs, PRs, commits, or designs using strict infrastructure gates for correctness, design-contract compliance, internal/public API quality, naming clarity, developer experience, module locatability, production diagnosability, hot-path performance, concurrency/C++ safety, public API/config/docs coverage, Bazel/CMake support, sensitive-information exposure, behavior-focused test quality, discussion lifecycle, and risk-calibrated rendered comments; when the target is a GitCode PR/MR number or URL, prepare a review bundle and publish validated high-confidence findings back to the PR page through the YuanRong PR review workflow | “review this diff”, “检查这个 PR”, “检视 1031”, “做代码评审”, “审查改动”, “PR review” | asking how the checklist works without requesting a concrete review; explicitly requesting local-only review |
| `ds-self-verify` | verify diff, tests, context updates, and infra risk before Codex claims work is complete or PR-ready, using the shared AI self-verification playbook | “完成前自检”, “self verify”, “准备提交/PR前检查”, Codex is about to claim completion after file changes | general questions about verification policy |
| `ds-issue-intake` | fetch a GitCode issue, redact sensitive details, and write a structured task spec for AI-assisted implementation | “拉取 issue”, “分析 issue 572”, “从这个 issue 开始开发”, “issue intake”, “准备 issue 任务” | asking how issue templates work or discussing issue policy without asking to fetch/triage a concrete issue |
| `ds-test` | plan and run configured validation for code changes, including local command selection and remote validation through private user-provided SSH config | “验证这个改动”, “跑远端验证”, “跑测试并给 PR 验证结果”, “validate this branch”, “run ds-test” | asking what tests should exist in general, or discussing validation policy without asking to run validation |
| `ds-create-pr` | create a GitCode PR for a pushed branch with a template-compliant PR body | “创建PR”, “提交PR”, “开PR”, “create pull request”, “open a PR”, “发起合并请求” | mentions of PR review policy, PR template, or PR conflicts without asking to create a PR |
| `ds-pr-comment-proc` | fetch unresolved GitCode PR review comments, prepare replies, mark discussions resolved, and verify final resolved state | “拉取 PR 评论”, “处理 review comments”, “回复并 resolve 评论”, “address PR comments”, “verify resolved comments” | asking how review comments work, or discussing PR review policy without asking to process comments |
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

Repository-maintained skills must be reusable capabilities for this repository, not one-off wrappers for a single issue,
single feature, single PR, or temporary delivery plan. If a user request needs multiple steps, compose existing skills
through the routing model below. Keep issue-specific or feature-specific state in local task files or PR notes, not in a
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
- “拉取 572 号 issue 并生成任务”
- “跑远端验证并整理 PR 验证结果”
- “处理这个 PR 的 unresolved comments”

### Composite issue development requests

Do not create or require a dedicated orchestration skill for common multi-step development workflows. After the active
tool entrypoint loads `.repo_context/index.md` and this skill registry, infer the next concrete skill from the user's end
goal and current state.

For requests such as “帮我解决 issue#572”, “修复这个 issue 并提 PR”, or “take issue 572 to PR”, compose the existing
skills in order:

1. Use `ds-issue-intake` to fetch the concrete issue and produce a sanitized task spec.
2. Use `ds-infra-engineering` before product, build, test, config, or workflow edits.
3. Implement the smallest source-backed fix.
4. Use `ds-self-verify` before claiming completion or PR readiness.
5. Use `ds-test` for validation planning and configured local or remote validation.
6. Use `ds-create-pr` only after the branch is committed and pushed.
7. Use `ds-pr-comment-proc` when review comments need replies, fixes, resolution, or resolved-state verification.

At each step, stop and give a friendly setup prompt when required configuration is missing. The prompt must be actionable
without asking the user to paste secrets into chat:

- GitCode API access: ask the user to set `GITCODE_TOKEN` / `GITCODE_ACCESS_TOKEN`, or put the token in
  `~/.local/gitcode_token`.
- Remote Linux validation: ask the user to copy
  `.skills/ds-test/references/validation_config.example.toml` to `~/.config/yuanrong/ds-test.toml`, fill in the SSH
  target locally, then rerun `ds-test check-config`.
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
  add skills that encode one issue, one PR, one feature plan, or a fixed orchestration chain that routing can compose.
- Keep the trigger phrases narrow and action-oriented; do not register broad topic words that would cause accidental
  execution.
- When a skill manages a repository artifact with frequent ambiguous mentions, record both:
  - high-confidence execution phrases
  - ambiguous phrases that require confirmation first
- When the trigger model changes, update the matching playbook
  `.repo_context/playbooks/upkeep/skill-trigger-routing.md`.

## Pending Verification

- None today.
