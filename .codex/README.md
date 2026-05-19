# Codex Context Survival Hooks

This project-local `.codex/` layer keeps Codex useful when the active model
context is small or a long task is compacted.

## What It Does

- `SessionStart`: injects a short repository snapshot, current git state, and
  the most relevant `.repo_context/` entries.
- `UserPromptSubmit`: adds a compact context-routing reminder for coding,
  debugging, review, and analysis prompts.
- `PreToolUse`: routes pending tool calls to the smallest likely context docs
  and blocks a small set of destructive shell commands.
- `PostToolUse`: refreshes `.codex/context/working-state.md` after supported
  tool calls.
- `PreCompact`: saves `.codex/context/working-state.md` before compaction.
- `PostCompact`: confirms the saved working-state file is available after
  compaction. Stable model-visible rehydration still happens through
  `SessionStart`, `UserPromptSubmit`, and `PreToolUse`.
- `Stop`: refreshes `.codex/context/working-state.md` at turn end.

`PreCompact` and `PostCompact` are supported by the local Codex version used for
this repository. The stable public Codex hook events are still the main path.

## Files

- `hooks.json`: project-local hook registration.
- `hooks/context_survival.py`: shared implementation.
- `hooks/*.py`: event-specific wrappers.
- `context/module-map.json`: path-prefix to `.repo_context/` routing table.
- `context/working-state.md`: generated transient task snapshot.
- `tests/test_context_survival.py`: focused hook behavior tests.

## Verification

```bash
rtk python3 .codex/tests/test_context_survival.py
```

Codex requires project-local hooks to be trusted. Open `/hooks` in Codex after
this change is present and review/trust the new hook entries.
