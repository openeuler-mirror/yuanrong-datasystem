# Skill Trigger Routing Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - repository-local Codex skill routing
- Owning module or area:
  - `.skills/`
  - `.repo_context/`
- Primary source paths:
  - `.skills/*/SKILL.md`
  - `.skills/*/scripts/*`
  - `.repo_context/modules/overview/repository-skills.md`
- Last verified against source:
  - `2026-04-18`

## Purpose

- Standardize how natural-language user requests route to repository-maintained skills.
- Prevent accidental execution when the user mentions a managed area without clearly asking to run the workflow.
- Provide a reusable registration model so the mechanism scales beyond one skill such as `ds-refresh-docs`.

## When To Use This Playbook

- Use when:
  - adding or updating a repository-local skill under `.skills/`
  - adjusting how natural-language requests should trigger an existing skill
  - reviewing whether a new phrase should auto-invoke a skill or require confirmation
- Do not use when:
  - the task only changes skill internals without affecting invocation conditions
  - the question is about product code outside repository-local skills

## Trigger Decision Flow

1. Identify whether the request is action-oriented or discussion-oriented.
2. Match the request against the registered skill trigger table in
   `.repo_context/modules/overview/repository-skills.md`.
3. Invoke the matched skill immediately only when the request is a high-confidence execution request.
4. Ask for confirmation when the user mentions a managed artifact or workflow area without clearly requesting execution.
5. If no registered skill matches, proceed with ordinary repo exploration or implementation.

## Registration Rules For Each Skill

Record all of the following:

1. canonical task the skill performs
2. explicit skill name
3. high-confidence imperative trigger phrases
4. ambiguous mentions that should require confirmation
5. source files that verify the workflow contract

## Classification Heuristics

Treat the request as a high-confidence execution trigger when it contains both:

- an action verb such as “更新”, “刷新”, “创建”, “提交”, “发布”, “open”, “create”, “refresh”
- the managed artifact or workflow target such as “在线文档”, “PR”, “pull request”

Treat the request as ambiguous when it mentions the managed target but the verb is about understanding, checking,
comparing, or deciding.

Examples that should trigger:

- “更新在线文档”
- “发布最新中文在线文档”
- “为这个分支创建 PR”

Examples that should confirm first:

- “在线文档”
- “doc_pages 分支”
- “PR 模板”

Examples that should not trigger:

- “解释一下 ds-refresh-docs 的流程”
- “检查一下 ds-create-pr 为什么失败”

## Current Registration Table

| Skill | Trigger now | Confirm first when |
| --- | --- | --- |
| `ds-refresh-docs` | user explicitly asks to update, refresh, or publish online docs | user only mentions online docs, `doc_pages`, or docs publishing context |
| `ds-create-pr` | user explicitly asks to create or submit a PR for a branch | user only asks about PR policy, PR template, or merge conflicts |

## Update Checklist

- [ ] update `.skills/<skill>/SKILL.md` trigger description when workflow scope changes
- [ ] update `.repo_context/modules/overview/repository-skills.md`
- [ ] update `.repo_context/index.md` if the best routing entrypoint changes
- [ ] update metadata JSON if the canonical overview module set changes
- [ ] regenerate `.repo_context/generated/repo_index.*` when `.skills/` structure changes
- [ ] run `python3 scripts/ai_context/validate_module_metadata.py` if metadata changed
