# AI Self Verification

## Metadata

- Status:
  - `active`
- Upkeep scope:
  - completion checks before an AI coding agent claims work is complete, passing, committed, or PR-ready
- Owning module or area:
  - `AGENTS.md`
  - `CLAUDE.md`
  - `.cursor/rules/repo-context.mdc`
  - `.repo_context/`
- Primary source paths:
  - touched source files
  - touched `.repo_context` files
  - touched `.skills` files
  - `scripts/ai_context/generate_repo_index.py`
  - `scripts/ai_context/validate_module_metadata.py`
- Last verified against source:
  - `2026-05-14`

## When To Use

Use before final response when an AI coding agent has changed files or is about to claim:

- implementation is complete;
- tests pass;
- context is updated;
- a branch is ready for commit or PR;
- a review found no issues.

## Required Checks

1. Confirm newest user request still matches the work performed.
2. Inspect `git status --short` and distinguish agent changes from pre-existing user changes.
3. Review the diff for accidental broad formatting, unrelated refactors, debug logs, generated noise, or credentials.
4. Verify the relevant path:
   - code change: targeted tests or build command;
   - `.repo_context` structural change: regenerate repo index;
   - metadata change: validate module metadata;
   - skill change: read the skill frontmatter and routing table for consistency.
5. Check engineering gates:
   - hot path impact;
   - concurrency and ownership;
   - persistence and recovery;
   - test and regression coverage;
   - context update requirement.
6. Report exact commands run and limitations.

## Suggested Commands

Use commands that match the change. Do not run broad expensive tests when a focused validation is enough and the user did
not ask for a full validation sweep.

```bash
rtk git status --short
rtk git diff -- AGENTS.md CLAUDE.md .cursor .repo_context .skills
rtk python3 scripts/ai_context/generate_repo_index.py
rtk python3 scripts/ai_context/validate_module_metadata.py
```

For product code, select tests from `.repo_context/modules/quality/tests-and-reproduction.md`.

## Completion Statement Rules

- Say "verified with ..." only for commands that actually ran and passed.
- Say "not run" with a short reason for checks that were skipped.
- Mention pre-existing untracked or modified files when relevant.
- Do not claim performance, recovery, or concurrency safety from documentation alone; say the gate was assessed.
- If a command fails, report the failure and stop short of completion claims.

## Self-Check Template

```text
Self verification:
- Request match: yes/no
- Diff reviewed: yes/no
- Hot path risk: none/assessed/needs follow-up
- Concurrency risk: none/assessed/needs follow-up
- Persistence/recovery risk: none/assessed/needs follow-up
- Tests or validation: command and result
- Context updates: yes/no/not needed
```
