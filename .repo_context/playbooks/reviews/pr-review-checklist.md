# PR Review Checklist

## Metadata

- Status:
  - `active`
- Review scope:
  - repository-wide code and design review for `yuanrong-datasystem`
- Owning module or area:
  - `.repo_context/modules/overview/engineering-principles.md`
- Primary source paths:
  - `AGENTS.md`
  - `CLAUDE.md`
  - `.cursor/rules/repo-context.mdc`
  - `.repo_context/modules/overview/engineering-principles.md`
  - `.gitee/PULL_REQUEST_TEMPLATE/PULL_REQUEST_TEMPLATE.zh-cn.md`
  - source and tests touched by the PR
- Last verified against source:
  - `2026-05-22`

## Review Stance

Lead with findings. Focus on bugs, regressions, data loss, recovery gaps, performance regressions, concurrency hazards,
and missing tests. Keep style suggestions secondary unless style affects correctness or maintainability.

## Must-Check Categories

| Category | Review questions |
| --- | --- |
| correctness | Does the change preserve API contract, status/error behavior, and edge cases? |
| performance | Does it add allocations, copies, locks, IO, logs, scans, or retry loops on hot paths? |
| concurrency | Are owner, lock, lifetime, shutdown, and async captures safe? |
| persistence | Is durable state written in a crash-safe order and recoverable after partial progress? |
| recovery/failover | Is restart, retry, idempotency, scale-down, and successor behavior safe? |
| compatibility | Does it alter public API, proto, persisted format, logs, metrics, or config semantics? |
| tests | Are UT/ST/recovery/perf tests sufficient for the risk surface? |
| observability | Are logs/metrics/inject points enough to diagnose failure without flooding hot paths? |
| sensitive information | Do source code, tests, scripts, docs, PR descriptions, and commit messages avoid secrets, private endpoints, local paths, account data, and raw sensitive payloads? |
| context | Did the PR update `.repo_context` when module behavior, commands, or workflows changed? |

## Sensitive Information Review Gate

Review every artifact in scope for accidental disclosure before approving:

- source files, test data, generated examples, shell/Python/CMake/CI scripts, docs, and `.repo_context` updates;
- PR title/body, verification notes, linked log snippets, reviewer-visible comments, and commit messages.

Flag and request cleanup for:

- passwords, tokens, private or SSH keys, AK/SK values, access keys, secret keys, tenant/system keys, or other usable
  authentication material;
- server IPs, host:port pairs, internal endpoints, private URLs, non-public service addresses, or environment-specific
  ports;
- local absolute paths, personal home directories, workspace paths, account names, user names, tenant identifiers, or raw
  logs that expose private environment details;
- logs, metrics, errors, tests, or scripts that newly print or persist sensitive request payloads, object data,
  credentials, tokens, or account metadata.

Do not copy the sensitive value into a finding or review comment. Cite the location and category only, then ask for a
repo-relative path, sanitized placeholder, or redacted example. For exposed usable credentials or keys, require rotation
in addition to redaction.

## Hot Path Review Gate

Flag and request justification for:

- request-path string formatting, heap allocation, or repeated container growth;
- global or broad locks in foreground paths;
- blocking IO/RPC under locks;
- success-path logging in high-frequency loops;
- O(N) scans where N is unbounded by request shape or cluster size;
- background tasks that can starve request threads;
- changed RDMA/transfer flush/wait semantics without measurement or correctness proof.

## Persistence And Recovery Review Gate

For durable or recoverable state, require answers for:

- What is the source of truth after restart?
- What happens if the process crashes after data write but before metadata write?
- What happens if metadata exists but data is missing?
- Can compaction or cleanup be interrupted and resumed?
- Can the same recovery task run twice safely?
- Is the format/schema compatible with old data?
- Are passive scale-down, voluntary scale-down, and restart interactions covered when relevant?

## Concurrency Review Gate

Require a shared-state model when the PR touches locks, atomics, queues, callbacks, or buffers:

- owner and lifetime;
- readers and writers;
- lock/atomic protection;
- lock order;
- shutdown and cancellation;
- whether IO/RPC/logging occurs while locked;
- async capture lifetime.

## Test Review Gate

Expected evidence by risk:

- local helper or validation change: focused UT;
- service behavior: CTest case or ST for the relevant binary;
- recovery/failover: injected failure, restart, scale-down, or cluster test;
- performance-sensitive change: benchmark, perf point, or explicit measurement limitation;
- docs/context-only change: context metadata and index validation when structure changed.

## Finding Format

Use file and line references. Order by severity.

```text
[P1] Short title
file:line
Why this can break correctness/performance/recovery, and what change would address it.
```

Severity guidance:

- `P0`: data loss, corruption, security issue, or unrecoverable outage.
- `P1`: likely production regression, recovery failure, hot-path regression, or test-blocking issue.
- `P2`: correctness or maintainability issue that should be fixed before merge.
- `P3`: minor cleanup or clarity suggestion.

## Approval Checklist

- [ ] No unaddressed correctness or data-integrity risk.
- [ ] Hot-path impact is acceptable and measured or justified.
- [ ] Shared state and async lifetimes are safe.
- [ ] Durable state is crash-safe and recoverable.
- [ ] Tests match the risk surface.
- [ ] Source code, tests, scripts, docs, PR body, and commit messages do not expose sensitive information.
- [ ] PR body includes verification and API/context impact when relevant.
