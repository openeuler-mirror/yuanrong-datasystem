# Repository Instructions

You are working in a high-performance, highly-available distributed cache and infrastructure system.

Treat every implementation, bugfix, refactor, design, and codebase analysis task as infrastructure engineering work, not
CRUD work.
Performance, concurrency safety, memory safety, recovery correctness, and operational availability are repository-level
requirements.

This repository keeps a shared AI context under `.repo_context/`.

Start here before large implementation, bugfix, refactor, design, or codebase Q&A tasks:

1. Read `.repo_context/README.md`.
2. Read `.repo_context/index.md`.
3. Read `.repo_context/maintenance.md`.
4. Read `.repo_context/generated/repo_index.md`.
5. Read `.repo_context/modules/overview/engineering-principles.md`.
6. Read the relevant file under `.repo_context/modules/<domain>/` or `.repo_context/playbooks/<category>/`.
7. Confirm important claims against the actual source before acting.

For implementation, bugfix, refactor, design, or codebase Q&A work, use
`.repo_context/playbooks/features/infra-engineering-workflow.md` as the default development workflow before editing,
then add narrower performance, concurrency, recovery, build, test, or module playbooks as the touched code requires.

Rules:

- Source code is the final source of truth.
- If the touched module's responsibilities, structure, build path, or test path changed, update the relevant `.repo_context/` files in the same task when practical.
- If `.repo_context/` is stale, fix it rather than silently working around it.
- Keep context additions narrow, source-backed, and reusable.
- Before adding new logic, search for existing helpers, utilities, status/error patterns, thread pools, persistence
  helpers, recovery paths, and test harnesses.
- Before defining or changing shared internal APIs, check developer experience, naming clarity, misuse prevention,
  ownership/lifetime semantics, module locatability, and production diagnosability. These are development requirements,
  not cleanup items to defer until later.
- Before coding, keep the change small enough to understand and roll back, identify high-risk owners or domain experts,
  choose risk-based validation evidence, and record rollout/rollback expectations when behavior or operator surfaces
  change.
- Identify whether the change touches a hot path. If it does, explicitly assess latency, throughput, lock contention,
  allocations, copies, cache locality, IO, and foreground impact from background work.
- For stateful changes, explicitly assess persistence, crash consistency, partial writes, startup rebuild, compaction,
  cleanup, idempotency, retry safety, and failover behavior.
- For shared state, document ownership, protection, lock ordering, visibility, and lifetime assumptions in the design or
  working notes.
- Prefer small, scoped changes that match existing style. Do not perform unrelated refactors or broad formatting churn.
- Before claiming completion, use `.repo_context/playbooks/upkeep/ai-self-verification.md`.
