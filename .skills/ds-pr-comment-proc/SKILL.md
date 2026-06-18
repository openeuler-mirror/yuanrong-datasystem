---
name: ds-pr-comment-proc
description: Fetch, inspect, reply to, resolve, and verify GitCode Pull Request review comments for yuanrong-datasystem or compatible GitCode repositories. Use when the user asks to pull PR comments, address review comments, reply under each review discussion, mark comments resolved, verify resolved state, or continue after PR review feedback.
---

# DS PR Comment Proc

## Purpose

Use this skill after code fixes are ready or when a PR review needs to be processed. Always fetch comments before
editing or replying, fix and verify code before resolving a discussion, and keep replies PR-safe.

## Configuration

The script reads GitCode owner/repo/API settings from `.skills/ds-pr-review/assets/default_config.toml` when available.
It reads tokens from `GITCODE_TOKEN`, `GITCODE_ACCESS_TOKEN`, `GITCODE_TOEKEN`, or `~/.local/gitcode_token`.

If credentials are missing, tell the user:

```text
Set GITCODE_TOKEN or GITCODE_ACCESS_TOKEN, or put the token in ~/.local/gitcode_token.
Do not paste the token into chat or commit it.
```

## Workflow

1. Fetch unresolved comments:

```bash
python3 .skills/ds-pr-comment-proc/scripts/pr_comment_proc.py list --pr <PR_NUMBER> --unresolved
```

2. Fix the code locally and validate it. Do not resolve a discussion before the fix is implemented and verified.

3. Prepare one reply per review discussion. Replies should say what changed and what was verified, without private
hosts, IPs, ports, usernames, local paths, remote log paths, tokens, AK/SK, or raw sensitive payloads.

4. Dry-run the plan:

```bash
python3 .skills/ds-pr-comment-proc/scripts/pr_comment_proc.py process --plan /tmp/pr-comments.json
```

5. Apply replies and resolution:

```bash
python3 .skills/ds-pr-comment-proc/scripts/pr_comment_proc.py process --plan /tmp/pr-comments.json --apply
```

6. Verify final state:

```bash
python3 .skills/ds-pr-comment-proc/scripts/pr_comment_proc.py verify --plan /tmp/pr-comments.json
```

## Plan Format

Use `comment_id` from the UI or `discussion_id` from script output.

```json
{
  "pr": 1048,
  "items": [
    {
      "comment_id": 173510908,
      "reply": "已修复：说明具体代码改动和验证命令。",
      "resolve": true
    }
  ]
}
```

## Rules

- Always reply under the corresponding discussion before marking it resolved unless the user explicitly asks to resolve
  only.
- Use `discussion_id` for mutation. `comment_id` is accepted as convenience input and mapped by the script.
- Re-fetch comments after mutation; do not rely on mutation responses alone.
- If verification reports `resolved=false` or missing replies, run the specific reply/resolve command again and
  re-verify.
- Keep PR replies sanitized and concise.
