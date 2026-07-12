---
name: ds-pr-review
description: Use when reviewing yuanrong-datasystem code, tests, scripts, docs, diffs, commits, pull requests, PR descriptions, or design changes for correctness, design-contract compliance, internal/public API design quality, naming clarity, developer experience, abstraction boundaries, module locatability, production diagnosability, misuse prevention, ownership/lifecycle safety, hot-path performance, concurrency and C++ safety, persistence, recovery, public API/config/docs coverage, Bazel/CMake build support, sensitive-information exposure, testing, operability, risk-calibrated comment quality, context-update risk, and GitCode PR review-comment publishing.
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

   `prepare` runs a mandatory sensitive-information scan before writing `bundle.json`. It scans the PR title/body,
   every changed file path, and every patch line returned by GitCode. The scanner is designed to recognize common
   false positives automatically: C++ type declarations (`const std::string &accessKey`), enum/constant identifiers,
   parameter names in function signatures, and test-dummy IPs (e.g. `192.0.2.x`, `127.0.0.1`, `0.0.0.0` in test
   files). If any changed file has no scannable patch payload, or if any scanned field matches a genuine blocked
   category, stop and report only the category/location summary from the helper. Do not bypass this gate or paste
   raw sensitive values into local notes, findings, chat output, or PR comments.

2. Read the generated bundle path from the command output, then load the generated `bundle.json`.
3. Read `bundle.change_stats` and `bundle.review_plan` before reviewing. Follow `review_plan.mode`:
   - `single_integrated_pass`: one integrated pass is acceptable, but every triggered existing gate must be checked.
   - `parallel_multi_round`: run the listed rounds as independent focused passes; execute them in parallel when the
     active tooling supports independent reviewers/subagents, then merge, deduplicate, and calibrate findings before
     publishing.
4. Review changed files first using the bundle's annotated patch. Prefer `diff_line_index` from the bundle for comment
   anchoring. Open repository source when the diff and snippets are not enough for a high-confidence finding.
5. Write findings as JSON matching the output contract. Each finding must include:
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
6. Dry-run non-trivial findings before posting:

```bash
python3 .skills/ds-pr-review/scripts/review_pr.py publish \
  --bundle <BUNDLE_JSON> \
  --findings <FINDINGS_JSON> \
  --dry-run
```

7. Publish findings back to GitCode after inspecting the rendered comments:

```bash
python3 .skills/ds-pr-review/scripts/review_pr.py publish \
  --bundle <BUNDLE_JSON> \
  --findings <FINDINGS_JSON>
```

Publishing rules:

- **Post one diff_comment per high-confidence finding.** Every published comment must be a diff_comment anchored to a
  specific file and line (`path` + `position`), never a body-only general comment. Use the publish script to resolve
  `diff_line_index` to the absolute file line automatically. If you are publishing findings manually (e.g. when
  `prepare` was skipped or the bundle is unavailable), you must still compute the absolute line number from the patch
  and post a diff_comment with `path` + `position` + `need_to_resolve: true`.
- **Do not post a summary comment.** A summary of all findings is for the local review report only, not for the PR page.
- If there are no findings, do not post remote comments.
- Let the publish script resolve `diff_line_index` to the absolute file line and handle fingerprint deduplication.
- Search existing unresolved and resolved discussions for semantic duplicates before adding a new finding. If a new
  comment replaces a weaker or badly rendered one, prefer replying in the existing discussion when the comment tool can
  do so.
- If publishing fails because credentials are missing, API access is unavailable, or a line anchor cannot be safely
  resolved, report that limitation and include the local findings. Do not fall back to body-only general comments as a
  substitute for diff_comment — if you cannot anchor a finding to a line, include it in the local report and state the
  limitation.

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
- Treat widely used internal APIs as semi-public APIs. Review whether they are clear, hard to misuse, maintainable, and
  friendly to future developers, not only whether the current patch compiles or passes the happy path.
- Calibrate findings by impact. Block on correctness, data integrity, recovery, availability, hot-path performance,
  concurrency, memory safety, security, build, public contract, or serious maintainability regressions. Treat subjective
  polish as a local note, `suggestion`, or `nit`, and do not publish it as a blocking finding.
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

## Review Focus

Focus on low latency, high performance, high concurrency, high reliability, high availability, deadlock prevention, and
coredump prevention. These are the core quality red lines for yuanrong-datasystem as distributed cache infrastructure —
any change that could cause latency jitter, throughput bottleneck, concurrency race, data loss, service unavailability,
deadlock, or process crash must be identified and blocked during review.

Conduct multiple rounds of deep review: do not stop after a single-pass happy-path check. For changes touching hot paths,
shared state, concurrency primitives, persistence/recovery paths, or lifecycle management, follow the Strict Review
Passes multi-round mode, with each round focusing on a different dimension, until residual uncertainty on all high-risk
surfaces has been assessed or eliminated.

## Strict Review Passes

For large, AI-assisted, cross-module, hot-path, shared-interface, public-boundary, persistence/recovery, concurrency, or
security-sensitive PRs, run these passes explicitly. For narrow low-risk changes, apply the passes that match the touched
surface, but do not skip any gate that the code path actually triggers. Do not limit the review to modified diff lines
when the change creates or reshapes a shared module.

`review_pr.py prepare` writes `change_stats` and `review_plan` into the bundle. Use changed-line count to size the
review without replacing these existing gates: when `additions + deletions <100`, `review_plan.mode` is
`single_integrated_pass`; when `additions + deletions >=100`, `review_plan.mode` is `parallel_multi_round`. For
cross-module, hot-path, concurrent, lifecycle-heavy, build-system-facing, or AI-assisted PRs, upgrade to multiple focused
passes even when the line count is lower, and record which gates still have residual uncertainty. For
`parallel_multi_round`, run the bundle-listed rounds independently and in parallel when available; do not publish until
all round findings have been merged, deduplicated, and severity-calibrated.

1. Claim traceability:
   - Map PR-description and design-document claims to actual code, tests, docs, and measurement evidence.
   - Flag claims such as "performance verified", "docs complete", or "Bazel supported" when the supporting artifact is
     missing or inconsistent.
2. Whole-module cohesion:
   - Review the whole touched module, not only modified lines.
   - Flag scattered call-site logic, duplicate helpers, dead code, unused compatibility shims, and patch-like wrappers.
   - Prefer centralizing behavior in the owning abstraction over exposing infrastructure details to business call sites.
   - For shared internal interfaces, inspect call-site ergonomics across the whole module and adjacent modules, not only
     the edited call site.
   - Review names of shared types, functions, modes, flags, and helper APIs. Names should expose semantic intent,
     ownership, lifetime, sync/async behavior, and error-handling expectations without requiring hidden context.
   - Check module locatability: an on-call engineer should be able to identify the owning module, entrypoint, state owner,
     dependency boundary, and expected signal path without reverse-engineering unrelated code.
3. Functional and design-contract correctness:
   - Check startup, runtime update, local/remote, SDK, worker, embedded, and config propagation paths where applicable.
   - Verify default values, explicit default values, derived values, sticky user values, validation, and idempotency.
   - Check exception safety around operations that may throw or fail, including container insert/emplace, string growth,
     smart-pointer and thread construction, allocation, logging/formatting, and callback dispatch.
   - Treat multi-step state updates as transactions: flag erase-then-insert, remove-then-add, publish-before-validate, or
     partial-registration paths that can leave the old state gone or a new state half-visible when a later step fails.
   - For callbacks, plugin hooks, futures, and async completion paths, verify that thrown exceptions or failed status
     returns cannot leak resources, skip cleanup, leave locks held, or expose inconsistent state to later requests.
4. Hot-path performance gate:
   - Treat SDK, boundary adapters, client/worker/master/common infrastructure, config, RPC, metrics, logging, storage,
     and scheduler paths as hot until source inspection proves otherwise.
   - Check latency, throughput, allocations, copies, string formatting, lock/atomic contention, map lookups, logging,
     exporter IO, background work interference, and queue/backpressure behavior.
   - Compare against explicit targets in the design or PR, for example concurrency 32 and QPS 4000.
   - For guard, lazy, deferred, sampling, logging, cache, or fast-reject APIs, inspect both rejected/skipped and accepted
     paths. Check caller-side argument evaluation, not only the callee body.
   - Quantify hot-path findings when possible: estimate `QPS * per-call cost = CPU%`, allocation rate in MB/s, string
     copy count and SSO boundary crossings, shared-pointer atomic operations times fanout, and container complexity
     against the expected deployment scale.
   - Check microarchitecture-sensitive choices in tight loops: false sharing across atomics, mutexes, counters, and hot
     fields in the same cache line; unnecessary `seq_cst`; high-frequency `notify_one`/futex wakeups; virtual dispatch
     that blocks inlining; and unpredictable branches on the hot path.
   - For data-structure or fanout changes, state the Big-O behavior and the break point: O(N) scans per request, O(N^2)
     paths, single atomic or single-lock contention frequency, fanout throughput ceiling, and memory footprint as
     `instances * objects * bytes`.
5. Minimal implementation and reuse:
   - Search for existing utilities, config helpers, status patterns, test fixtures, and build targets before accepting new
     mechanisms.
   - Reject speculative public APIs or special-case infrastructure APIs when the caller can normalize locally.
6. C++ safety and concurrency:
   - Review ownership, lifetime, nullability, raw pointers, reference captures, async callbacks, singleton/static
     lifetime, RAII, memory bounds, use-after-free risk, lock order, shutdown, and thread visibility.
   - For RAII or one-shot objects with destructor side effects, check copy/move/delete semantics and moved-from safety.
   - List every lock acquisition path touched by the PR and check for AB/BA lock-order cycles, blocking IO or callbacks
     while locked, and lock nesting that changed across call sites.
   - **Prohibit pthread mutex.** Flag any new or changed code using `pthread_mutex_t`, `pthread_mutex_lock`,
     `pthread_mutex_unlock`, `pthread_mutex_init`, or other pthread synchronization primitives. yuanrong-datasystem is
     built on bthread — pthread mutex causes OS-level blocking rather than bthread-level yielding, leading to thread
     starvation and performance jitter under high concurrency. Flag as Critical and require replacement with
     `bthread::Mutex`, `std::mutex`, or lock-free structures. Exception: C interop layers or third-party library
     requirements that cannot be avoided, where the reason must be confirmed and documented in the review.
   - For atomics, verify the memory-order contract against the protection model. Flag `load` plus `store` Start/Stop
     races that need CAS, unnecessary strong ordering inside lock-protected state, and weak ordering without a documented
     happens-before edge.
   - Check lifecycle direction explicitly: who calls Start/Stop, whether construction/destruction owns those calls, which
     producer or consumer thread joins first, and whether callbacks, notifications, retries, or weak_ptr promotion can
     access an object after Stop or destruction.
   - Infinite or unbounded retry loops must have a retry budget, backoff, cancellation/shutdown check, and production
     signal for exhausted retries.
   - Use Google C++ guidance as a reference for readability, ownership clarity, self-contained headers,
     include-what-you-use, and avoiding surprising or dangerous constructs.
7. Public interface and docs:
   - Check public C++ and embedded APIs, dscli, K8s/Helm, config docs, examples, generated docs, and compatibility notes.
   - Require documentation for effective semantics, not only new flag names.
8. Build and packaging:
   - Check Bazel and CMake when both exist for the touched area.
   - Verify that targets depend on implementation objects, not only headers, and that public headers/package manifests are
     updated when needed.
   - Check API signatures against all call sites after facade or shared-header changes, including language bindings,
     CMake globbed sources, Bazel-only tests, and package entry points.
9. Tests:
   - Check whether tests would fail before the fix and assert the intended semantics.
   - Flag default tests expected to run longer than 8 seconds unless they are explicitly manual/performance-only and
     documented.
   - Inspect flakiness risk from sleeps, wall-clock durations, randomness, thread scheduling, and shared global flags.
   - Require invariant tests for shared abstractions: misuse prevention, error paths, async lifetime, move/copy behavior,
     skipped/fast-reject path cost, and boundary input validation where applicable.
   - For infrastructure changes, require coverage proportional to risk across normal flow, concurrency, boundary values,
     exception/error paths, fault injection, shutdown/restart, and retry/backoff behavior.
   - Prefer behavior, state, and invariant tests through public or semi-public APIs. Avoid tests that only lock in private
     call ordering or implementation interactions unless that ordering is itself the contract.
10. Production diagnosability and locatability:
   - For new or reshaped modules, ask whether a production bug can be quickly narrowed to the right owner, state machine,
     request path, background task, config, or boundary layer from symptoms and available telemetry.
   - Flag code that hides behavior behind overly generic wrappers, stringly typed dispatch, global registries, implicit
     background work, cross-module side effects, or multi-hop callbacks without clear ownership and signal correlation.
   - Check logs, metrics, traces, error codes, request IDs, object keys, worker/master/client identity, config source, and
     state-transition evidence where relevant. Signals must help locate the module without exposing sensitive data or
     adding hot-path overhead.
   - Analyze complexity of new code for incident response, not only for local readability: long control flows, duplicated
     branches, hidden retries, implicit defaults, and spread-out state updates increase mean time to diagnose.
11. Discussion lifecycle:
   - Review existing unresolved and resolved comments before publishing new ones.
   - Resolve only comments that current code actually fixes.
   - Reopen or reply to incorrectly resolved comments when the issue remains, and state the source-backed reason.

## System-Wide Design Gates

Apply these gates to every module in the system. They are not specific to any one feature area.

1. Internal API design:
   - Treat internal APIs used across files, modules, or ownership boundaries as semi-public APIs.
   - Check whether the API has a single clear responsibility, a small surface area, stable semantics, and one obvious
     recommended usage path.
   - Do not accept an interface that merely works while pushing policy, lifecycle, synchronization, or formatting details
     into business call sites.
2. Developer experience:
   - Review from the perspective of the next developer adding a similar feature.
   - Flag APIs that require reading implementation code, old review threads, or hidden invariants to use correctly.
   - Treat names as part of the interface. Names should make ownership, lifetime, mode, lazy/deferred behavior,
     concurrency expectations, and error semantics obvious at the call site.
   - Calling code should express business intent; infrastructure mechanics should stay inside the owning abstraction.
3. Misuse prevention:
   - Check common wrong usages, not only the intended happy path.
   - Prefer interfaces that make incorrect ownership, nullability, ordering, or mode choices fail at compile time or in
     focused tests instead of relying on comments.
4. Ownership and lifetime:
   - Audit `Ref`, `View`, `Owned`, `Provider`, callback, handle, buffer, iterator, and pointer-style APIs.
   - Verify who owns the object, who may extend its lifetime, whether it may cross threads or async boundaries, and who is
     responsible for release or completion.
   - References and views are only acceptable when the use is same-scope and synchronous; async/shared use needs owned
     state or explicit shared lifetime.
5. Hot-path argument cost:
   - Inspect caller expressions for expensive work before guarded/lazy/deferred APIs are entered.
   - Flag premature string formatting, proto serialization, container construction, copies, JNI/pybind conversion, memory
     allocation, locks, IO, time reads, atomics, and CAS in paths that are meant to be skipped, rejected, cached, or lazy.
6. Abstraction boundaries:
   - Review boundaries between common infrastructure, client, worker, master, SDKs, storage, RPC, persistence, config, and
     operations code.
   - Do not let infrastructure expose internal policy to business modules, and do not let one business special case
     pollute a shared abstraction.
   - Cross-boundary data structures should be minimal, explicit, and stable.
7. Minimal implementation and reuse:
   - Verify that existing helpers, status/error patterns, thread pools, serializers, config paths, metrics, persistence
     utilities, and test harnesses cannot reasonably serve the need before accepting new mechanisms.
   - Special cases should stay local unless multiple call sites genuinely need a shared API.
8. Consistency and learnability:
   - A capability should have one recommended style across the system.
   - Flag mixed old/new styles, compatibility shims without owners, duplicate helpers, and examples or tests that teach a
     non-recommended pattern.
   - Equivalent concepts should have equivalent names. Do not accept different names for the same lifecycle or mode, or
     one name that means different things across modules, unless the difference is documented and source-backed.
9. Cross-language and boundary layers:
   - Treat C API, JNI, pybind, RPC, protobuf, CLI, K8s/Helm, config files, and generated artifacts as system boundaries.
   - Check nullability, length-delimited versus NUL-terminated data, encoding, ownership transfer, buffer copies,
     exception/error propagation, ABI/API compatibility, and documentation synchronization.
   - Logging, metrics, tracing, or diagnostics added at a boundary must not change the boundary's error behavior.
10. Build closure:
   - Check not only Bazel/CMake dependencies, but also source-level signature compatibility across all direct and indirect
     consumers.
   - Shared headers and facade changes require grepping call sites and considering language bindings, package targets,
     tests, and generated code.
11. Test contracts:
   - Tests should encode design invariants, not only happy-path behavior.
   - Cover concurrency, async lifetime, error paths, boundary inputs, move/copy semantics, recovery/persistence, dynamic
     config, and hot-path performance where relevant.
   - Prefer behavior and state assertions through public or semi-public APIs. Keep implementation-interaction assertions
     only when they describe a stable contract, such as lock-free fast rejection, single-shot completion, or ordering.
   - Default tests must be fast; long-running stress or soak coverage belongs in manual, nightly, or performance targets.
12. Operability:
   - Review whether the change can be deployed, observed, diagnosed, throttled, rolled back, and documented.
   - Check logs, metrics, error codes, config defaults, compatibility behavior, and failure-mode visibility.
   - Check production locatability: from an alert, user report, or error log, an on-call engineer should know which module
     owns the behavior, which entrypoint to inspect first, which state/config changed, and which telemetry correlates the
     failing request across boundaries.
   - Reject diagnosability that depends on reading source-only hidden invariants, guessing cross-module side effects, or
     enabling high-cost debug logging on hot paths.
13. Review discussion lifecycle:
   - Pull current discussions before every serious review round.
   - Check resolved comments against current source when the area changed or the PR is AI-assisted.
   - Avoid duplicate findings; when a previous comment exists, reply or reopen with source-backed evidence instead of
     posting a semantic duplicate.

## Comment Quality Gate

- Every finding must cite concrete evidence. Use exact `path` and `line` or `diff_line_index` and explain why the current
  behavior violates a correctness, design, performance, safety, documentation, test, or build contract.
- Wrap identifiers, flags, file names, and short code fragments in backticks.
- Put multi-line code in fenced blocks. Prefer `example_code` instead of embedding code in `problem` or `suggestion`.
- Include a concrete fix direction. For design/API/performance findings, include a code sketch when it reduces ambiguity.
- Do not publish comments that are only stylistic unless style affects correctness, safety, performance, API clarity,
  developer misuse risk, or maintainability.
- Use `critical` or high severity only for findings that can break correctness, safety, performance targets, build
  closure, public contracts, or long-term code health. Use lower severity, `suggestion`, or `nit` for localized cleanup.
- Do not publish speculative concerns as findings. Ask an open question locally when evidence is insufficient.

## Sensitive Information Gate

The `review_pr.py prepare` helper enforces this gate before generating the review bundle. Reviewers must still apply the
same policy to any extra source, CI output, issue text, design document, or discussion they inspect outside the prepared
bundle.

Flag sensitive information in any reviewed artifact, including source files, test data, shell/Python/CMake/CI scripts,
docs, PR descriptions, and commit messages.

Treat the following as review findings:

- credentials, passwords, tokens, private or SSH keys, AK/SK values, access keys, secret keys, tenant/system keys, or
  other usable authentication material;
- server IPs, host:port pairs, internal endpoints, private URLs, or environment-specific service addresses that are not
  already public project documentation;
- local absolute paths, personal home directories, workspace paths, account names, user names, tenant identifiers, or raw
  log snippets that expose private environment details;
- employee identifiers, personal identity/contact numbers, personal names, or company labels that reveal non-public
  affiliation. Company labels are allowed only in copyright statements or when attributing referenced third-party
  libraries to their upstream source;
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
      "line": 140,
      "type": "design",
      "severity": "critical",
      "title": "收敛 AccessRecorder 参数构造职责",
      "evidence": "`AccessRecorder` 让调用方自行判断采样、填参和写出。",
      "problem": "共享接口把采样、延迟构造和记录顺序暴露给业务调用点。",
      "impact": "新增调用点容易漏状态码、参数填充或资源释放，降低模块内聚性。",
      "suggestion": "把采样和延迟填参收敛到 owning abstraction，调用方只表达记录意图。",
      "example_code": "access.Record(rc, [&](RequestParam &req) {\\n  req.objectKey = objectKeysToString(keys);\\n});",
      "verification": "补充接口误用和延迟填参单测，并用 dry-run 确认 Markdown 渲染。"
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
