---
name: ds-test
description: Run configured validation for yuanrong-datasystem changes, including local command planning, remote validation through user-provided SSH targets, build.sh/CMake/Bazel commands, and targeted CTest runs. Use when Codex needs to validate code changes, prepare PR verification evidence, or choose tests after implementation. Requires private server details to be supplied through a local config file, not committed to the repository.
---

# DS Test

## Purpose

Use this skill to validate `yuanrong-datasystem` changes and produce PR-safe verification evidence. The repository does
not store private validation hosts, IP addresses, usernames, ports, SSH paths, or remote log paths. Load them from a
local config file.

## Configuration

Keep the filled validation config only on the user's local machine. The default private path is:

```bash
~/.config/yuanrong/ds-test.toml
```

Create it from the checked-in placeholder template:

```bash
mkdir -p ~/.config/yuanrong
cp .skills/ds-test/references/validation_config.example.toml ~/.config/yuanrong/ds-test.toml
```

Then fill in the private SSH and remote validation fields locally. Do not commit the filled config, paste it into chat,
or include its path or values in PR text.

If the config is missing, run:

```bash
python3 .skills/ds-test/scripts/ds_test.py check-config
```

The script prints the exact fields to configure. Do not commit the filled config file.

## Workflow

1. Inspect the working tree and stage only task-owned files.
2. Treat macOS, Windows, and other non-Linux local machines as orchestration hosts only. Do not try to compile
   `yuanrong-datasystem` locally there; use local commands only for source inspection, diff checks, and script checks.
3. Commit and push the branch when Linux build, CTest, Bazel, or remote validation is needed.
4. Generate a validation plan:

```bash
python3 .skills/ds-test/scripts/ds_test.py plan --changed-files <file> [<file> ...]
```

If the local platform is not suitable for build/test, the plan must say that Linux validation requires `DS_TEST_CONFIG`
or the default local config path, plus a configured remote Linux target.

5. Check private remote configuration:

```bash
python3 .skills/ds-test/scripts/ds_test.py check-config
```

6. Probe configured targets by alias:

```bash
python3 .skills/ds-test/scripts/ds_test.py probe
```

7. Run a remote validation command only after the branch is pushed:

```bash
python3 .skills/ds-test/scripts/ds_test.py run-remote \
  --branch <branch> \
  --command 'bash build.sh -t build'
```

Use targeted commands for low-risk UT changes when the build already exists, and full compile before ST tests.

## Reporting Rules

- Report the branch, commit hash, command, target alias, and pass/fail status.
- Do not report private hostnames, IPs, ports, SSH usernames, local config paths, private remote worktree paths, or remote
  log file paths in PR bodies, review replies, or chat output.
- Say configuration is missing instead of guessing remote hosts.
- Do not claim remote validation passed unless the configured remote command completed successfully.
- Treat `--config` and `DS_TEST_CONFIG` as local-only overrides. Do not put either override path in committed files,
  PR descriptions, or review comments.

## Friendly Setup Failures

When config is missing, tell the user to copy `validation_config.example.toml` to
`~/.config/yuanrong/ds-test.toml`, then fill in the placeholders locally.
When the user's local machine is macOS, Windows, or otherwise unsuitable for compiling this repository, tell the user to
configure a Linux validation host in the local private config instead of attempting local compile/test commands.
When GitCode credentials are missing for push/PR steps, tell the user to set `GITCODE_TOKEN` or
`GITCODE_ACCESS_TOKEN`, or create `~/.local/gitcode_token`.
When SSH fails, tell the user which target alias failed and which config section to inspect, without printing private
addresses.
