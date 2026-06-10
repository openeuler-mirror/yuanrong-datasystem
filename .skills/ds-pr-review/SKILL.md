---
name: ds-pr-review
description: Use when reviewing yuanrong-datasystem code, tests, scripts, docs, diffs, commits, pull requests, PR descriptions, or design changes for correctness, design-contract compliance, hot-path performance, concurrency and C++ safety, persistence, recovery, public API/config/docs coverage, Bazel/CMake build support, sensitive-information exposure, testing, context-update risk, and GitCode PR review-comment publishing.
---

# Datasystem PR Review

## GitCode PR Publishing Workflow

When the review target is a GitCode PR/MR number or URL for `openeuler/yuanrong-datasystem`, use the
`yuanrong-pr-review` workflow as part of this skill. Do not stop after producing local findings unless the user
explicitly asks for local-only review or publishing is blocked by missing credentials/API failure.

1. Run prepare first with the repository-maintained review helper:

```bash
python3 .skills/ds-pr-review/scripts/review_pr.py prepare <PR_OR_URL>
```

2. Read the generated bundle path from the command output, then load the generated `bundle.json`.
3. Review changed files first using the bundle's annotated patch. Prefer `diff_line_index` from the bundle for comment
   anchoring. Open repository source when the diff and snippets are not enough for a high-confidence finding.
4. Write findings as JSON matching the output contract. Each finding must include:
   - `path`
   - preferred `diff_line_index`, with `line` as human fallback
   - `type`
   - `severity`
   - `title`
   - concrete `evidence`
   - concrete `problem`
   - concrete `impact`
   - concrete `suggestion`
   - `example_code` for code-heavy findings or non-trivial API/design suggestions
   - `verification` when tests, build commands, runtime budget, or manual checks are relevant
5. Dry-run non-trivial findings before posting:

```bash
python3 .skills/ds-pr-review/scripts/review_pr.py publish \
  --bundle <BUNDLE_JSON> \
  --findings <FINDINGS_JSON> \
  --dry-run
```

6. Publish findings back to GitCode after inspecting the rendered comments:

```bash
python3 .skills/ds-pr-review/scripts/review_pr.py publish \
  --bundle <BUNDLE_JSON> \
  --findings <FINDINGS_JSON>
```

Publishing rules:

- Post one remote comment per high-confidence finding.
- Do not post a summary comment.
- If there are no findings, do not post remote comments.
- Let the publish script resolve `diff_line_index` to the absolute file line and handle fingerprint deduplication.
- Search existing unresolved and resolved discussions for semantic duplicates before adding a new finding. If a new
  comment replaces a weaker or badly rendered one, prefer replying in the existing discussion when the comment tool can
  do so.
- If publishing fails because credentials are missing, API access is unavailable, or a line anchor cannot be safely
  resolved, report that limitation and include the local findings.

## Required Reading

1. `AGENTS.md`
2. `.repo_context/modules/overview/engineering-principles.md`
3. `.repo_context/playbooks/reviews/pr-review-checklist.md`
4. For GitCode PR/MR review targets, the prepared `yuanrong-pr-review` bundle
5. The touched module docs from `.repo_context/index.md`
6. The actual diff and source code being reviewed
7. PR descriptions, design documents, existing discussions, and commit messages when they are part of the review scope

## Language Policy

- Match the review-comment language to the PR title and description, unless the user explicitly requests another
  language.
- Use Chinese when the PR title/body contains meaningful Chinese or the user asks in Chinese.
- Use English only when the PR title/body is fully English and there is no Chinese-language user instruction.
- Formatter labels are not enough. Author `title`, `evidence`, `problem`, `impact`, `suggestion`, `example_code`
  comments, and `verification` in the selected language.

## Review Rules

- Lead with findings, ordered by severity.
- Prioritize correctness, data integrity, recovery, availability, hot-path performance, concurrency safety, and missing
  tests.
- Check source code, tests, scripts, docs, PR descriptions, and commit messages for sensitive information exposure.
- Do not quote secrets or private values in review output; identify the category and location, then ask for redaction or
  rotation when needed.
- Use exact file and line references.
- Keep summaries secondary to findings.
- If no issues are found, say that clearly and mention any residual test or verification gap.
- For PR/MR review targets, publish accepted findings to the PR page through the GitCode workflow before final response.
- For large, cross-cutting, or AI-assisted PRs, keep durable local review notes under `.codex/context/` when the user asks
  for repeated passes, context-survival, or strict auditability. Keep those notes source-backed and separate from official
  repo context unless the source tree itself changed.

## Strict Review Passes

For large or AI-assisted PRs, run these passes explicitly. Do not limit the review to modified diff lines when the change
creates or reshapes a shared module.

1. Claim traceability:
   - Map PR-description and design-document claims to actual code, tests, docs, and measurement evidence.
   - Flag claims such as "performance verified", "docs complete", or "Bazel supported" when the supporting artifact is
     missing or inconsistent.
2. Whole-module cohesion:
   - Review the whole touched module, not only modified lines.
   - Flag scattered call-site logic, duplicate helpers, dead code, unused compatibility shims, and patch-like wrappers.
   - Prefer centralizing behavior in the owning abstraction over exposing infrastructure details to business call sites.
3. Functional and design-contract correctness:
   - Check startup, runtime update, local/remote, SDK, worker, embedded, and config propagation paths where applicable.
   - Verify default values, explicit default values, derived values, sticky user values, validation, and idempotency.
4. Hot-path performance gate:
   - Treat SDK/client/worker/access-log/config paths as hot until source inspection proves otherwise.
   - Check latency, throughput, allocations, copies, string formatting, lock/atomic contention, map lookups, logging,
     exporter IO, background work interference, and queue/backpressure behavior.
   - Compare against explicit targets in the design or PR, for example concurrency 32 and QPS 4000.
   - For sampling/logging, inspect sampled-out and sampled-in paths separately.
5. Minimal implementation and reuse:
   - Search for existing utilities, config helpers, status patterns, test fixtures, and build targets before accepting new
     mechanisms.
   - Reject speculative public APIs or special-case infrastructure APIs when the caller can normalize locally.
6. C++ safety and concurrency:
   - Review ownership, lifetime, nullability, raw pointers, reference captures, async callbacks, singleton/static
     lifetime, RAII, memory bounds, use-after-free risk, lock order, shutdown, and thread visibility.
   - Use Google C++ guidance as a reference for readability, ownership clarity, self-contained headers,
     include-what-you-use, and avoiding surprising or dangerous constructs.
7. Public interface and docs:
   - Check public C++ and embedded APIs, dscli, K8s/Helm, config docs, examples, generated docs, and compatibility notes.
   - Require documentation for effective semantics, not only new flag names.
8. Build and packaging:
   - Check Bazel and CMake when both exist for the touched area.
   - Verify that targets depend on implementation objects, not only headers, and that public headers/package manifests are
     updated when needed.
9. Tests:
   - Check whether tests would fail before the fix and assert the intended semantics.
   - Flag default tests expected to run longer than 8 seconds unless they are explicitly manual/performance-only and
     documented.
   - Inspect flakiness risk from sleeps, wall-clock durations, randomness, thread scheduling, and shared global flags.
10. Discussion lifecycle:
   - Review existing comments before publishing new ones.
   - Resolve only comments that current code actually fixes.
   - Reopen or reply to incorrectly resolved comments when the issue remains, and state the source-backed reason.

## Comment Quality Gate

- Every finding must cite concrete evidence. Use exact `path` and `line` or `diff_line_index` and explain why the current
  behavior violates a correctness, design, performance, safety, documentation, test, or build contract.
- Wrap identifiers, flags, file names, and short code fragments in backticks.
- Put multi-line code in fenced blocks. Prefer `example_code` instead of embedding code in `problem` or `suggestion`.
- Include a concrete fix direction. For design/API/performance findings, include a code sketch when it reduces ambiguity.
- Do not publish comments that are only stylistic unless style affects correctness, safety, performance, or maintainability.
- Do not publish speculative concerns as findings. Ask an open question locally when evidence is insufficient.

## Sensitive Information Gate

Flag sensitive information in any reviewed artifact, including source files, test data, shell/Python/CMake/CI scripts,
docs, PR descriptions, and commit messages.

Treat the following as review findings:

- credentials, passwords, tokens, private or SSH keys, AK/SK values, access keys, secret keys, tenant/system keys, or
  other usable authentication material;
- server IPs, host:port pairs, internal endpoints, private URLs, or environment-specific service addresses that are not
  already public project documentation;
- local absolute paths, personal home directories, workspace paths, account names, user names, tenant identifiers, or raw
  log snippets that expose private environment details;
- logs, metrics, errors, tests, or scripts that newly print or persist sensitive request payloads, object data,
  credentials, tokens, or account metadata.

Use severity based on impact: `P0`/`P1` for usable secrets or credentials, `P1`/`P2` for private infrastructure or
personal environment leakage, and lower severity only when the exposure is clearly non-sensitive. In comments, cite the
file/line or PR/commit field and the sensitive category, but do not reproduce the sensitive value.

## Output Shape

Finding JSON contract:

```json
{
  "overall_risk": "high",
  "findings": [
    {
      "path": "src/datasystem/common/log/access_recorder.h",
      "diff_line_index": 42,
      "line": 181,
      "type": "design",
      "severity": "critical",
      "title": "收敛采样细节到 AccessRecorder",
      "evidence": "`AccessRecorderGuard` 在调用点暴露 `ShouldRecord()` 和手工 `RequestParam` 构造。",
      "problem": "采样判断和请求参数构造散落在业务调用点。",
      "impact": "这会削弱模块内聚性，并让 sampled-out 热路径更容易重新引入字符串构造或二次采样风险。",
      "suggestion": "保留 lazy `AccessRecorder::Record(...)` 接口，只在 sampled-in 后构造请求参数。",
      "example_code": "AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);\\nStatus rc = impl_->Get(keys, subTimeoutMs, buffers);\\naccessPoint.Record(rc, buffers.size(), [&](RequestParam &req) {\\n    req.objectKey = objectKeysToString(keys);\\n});",
      "verification": "执行 access-recorder 单元测试，并用 dry-run 确认 Markdown 渲染。"
    }
  ]
}
```

For local-only reviews or publish-blocked reviews:

```text
Findings:
- [P1] Title
  path:line
  Explanation and expected fix.

Open questions:
- ...

Residual risk:
- ...
```

For GitCode PR/MR reviews, also report the publish summary printed by `review_pr.py publish`, including posted line
comments, posted general comments, skipped duplicates, skipped suggestions, and any warnings.
