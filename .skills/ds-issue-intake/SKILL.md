---
name: ds-issue-intake
description: Fetch GitCode issues for yuanrong-datasystem, redact sensitive details, and produce a structured task spec for AI-assisted development. Use when the user provides an issue number or URL and asks to analyze, triage, start work from, or prepare an implementation plan for that issue.
---

# DS Issue Intake

## Purpose

Use this skill to turn a GitCode issue into a sanitized, structured task spec that can drive implementation,
validation, PR creation, and review follow-up.

## Configuration

Public issues can often be fetched without credentials. For private repositories, API limits, or permission errors, set:

```bash
export GITCODE_TOKEN=<token>
```

or create `~/.local/gitcode_token`. Do not paste tokens into chat or commit them.

## Workflow

1. Fetch and sanitize the issue:

```bash
python3 .skills/ds-issue-intake/scripts/issue_intake.py --issue <ISSUE_NUMBER_OR_URL>
```

2. Save a reusable task spec:

```bash
python3 .skills/ds-issue-intake/scripts/issue_intake.py \
  --issue <ISSUE_NUMBER_OR_URL> \
  --output .codex/tasks/issue-<number>/task.json
```

3. Read the task spec before implementation. Confirm source-backed claims against code; issue text is not final truth.

## Output

The script emits JSON with:

- issue number, title, state, labels, and URL;
- sanitized body and short summary;
- likely PR kind;
- search terms for source exploration;
- security notes for redacted content;
- next-step checklist.

## Redaction Rules

The script redacts common private data from issue bodies before writing task specs:

- IPv4 addresses and host:port pairs;
- SSH-style `user@host` targets;
- local absolute paths under user homes;
- token, password, AK/SK, private key, and secret-like values.

Do not put raw issue logs containing private infrastructure details into PR bodies.
