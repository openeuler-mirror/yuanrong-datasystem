# Claude Code Entry

Use the shared repository context in `.repo_context/`.

Recommended read order:

1. `.repo_context/README.md`
2. `.repo_context/index.md`
3. `.repo_context/maintenance.md`
4. `.repo_context/generated/repo_index.md`
5. relevant `.repo_context/modules/<domain>/*.md` or `.repo_context/playbooks/<category>/...`

Rules:

- Treat `.repo_context/` as indexed guidance, not final truth.
- Verify meaningful claims against source files before implementation or review.
- When a touched module changes, update the relevant context files and regenerate the index if structure changed.
