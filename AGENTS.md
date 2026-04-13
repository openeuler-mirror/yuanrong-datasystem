# Repository Instructions

This repository keeps a shared AI context under `.repo_context/`.

Start here before large implementation, bugfix, review, or codebase Q&A tasks:

1. Read `.repo_context/README.md`.
2. Read `.repo_context/index.md`.
3. Read `.repo_context/maintenance.md`.
4. Read `.repo_context/generated/repo_index.md`.
5. Read the relevant file under `.repo_context/modules/<domain>/` or `.repo_context/playbooks/<category>/`.
6. Confirm important claims against the actual source before acting.

Rules:

- Source code is the final source of truth.
- If the touched module's responsibilities, structure, build path, or test path changed, update the relevant `.repo_context/` files in the same task when practical.
- If `.repo_context/` is stale, fix it rather than silently working around it.
- Keep context additions narrow, source-backed, and reusable.
