#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shlex
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EXAMPLE_CONFIG = Path(__file__).resolve().parents[1] / "references" / "validation_config.example.toml"
DEFAULT_CONFIG = "~/.config/yuanrong/ds-test.toml"


class DsTestError(RuntimeError):
    pass


def expand_path(value: str) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(value))).resolve()


def config_source(args: argparse.Namespace) -> tuple[str, str]:
    if args.config:
        return args.config, "local override"
    if os.environ.get("DS_TEST_CONFIG"):
        return os.environ["DS_TEST_CONFIG"], "local override"
    return DEFAULT_CONFIG, "default local config"


def config_path(args: argparse.Namespace) -> Path:
    value, _ = config_source(args)
    path = expand_path(value)
    if not path.exists():
        raise DsTestError(
            "Missing validation config. Copy .skills/ds-test/references/validation_config.example.toml "
            f"to {DEFAULT_CONFIG}, fill in SSH targets locally, and do not commit the filled config. "
            "Use DS_TEST_CONFIG or --config only as a local private override."
        )
    return path


def load_config(args: argparse.Namespace) -> dict[str, Any]:
    _, source = config_source(args)
    path = config_path(args)
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    data["_config_source"] = source
    return data


def redact_text(value: str, data: dict[str, Any] | None = None) -> str:
    redacted = value or ""
    if data:
        repo = data.get("repo", {})
        ssh = data.get("ssh", {})
        sensitive_values = [
            repo.get("required_origin_url"),
            repo.get("clone_url"),
            repo.get("remote_worktree"),
            repo.get("remote_log_dir"),
            ssh.get("user"),
            ssh.get("jump"),
        ]
        for target in data.get("targets") or []:
            sensitive_values.append(target.get("host"))
            if ssh.get("user") and target.get("host"):
                sensitive_values.append(f"{ssh['user']}@{target['host']}")
        for item in sorted({str(v) for v in sensitive_values if v}, key=len, reverse=True):
            label = "<REDACTED_CONFIG_VALUE>"
            if item == repo.get("remote_worktree"):
                label = "<REDACTED_REMOTE_WORKTREE>"
            elif item == repo.get("remote_log_dir"):
                label = "<REDACTED_REMOTE_LOG_DIR>"
            elif item in {repo.get("required_origin_url"), repo.get("clone_url")}:
                label = "<REDACTED_REPO_URL>"
            redacted = redacted.replace(item, label)
    patterns = [
        (r"(?i)(token|password|passwd|secret|access[_-]?key|secret[_-]?key|ak|sk)\s*[:=]\s*\S+", r"\1=<REDACTED>"),
        (r"\b(?:\d{1,3}\.){3}\d{1,3}:\d{2,5}\b", "<REDACTED_HOST_PORT>"),
        (r"\b(?:\d{1,3}\.){3}\d{1,3}\b", "<REDACTED_IP>"),
        (r"\b[a-zA-Z0-9_.-]+@(?:[a-zA-Z0-9_.-]+|<REDACTED_IP>)\b", "<REDACTED_SSH_TARGET>"),
        (r"(?<![\w.-])/(?:Users|home|root|tmp|var|mnt|opt|workspace|Volumes)(?:/[^\s`'\"<>]+)+", "<REDACTED_PATH>"),
    ]
    for pattern, replacement in patterns:
        redacted = re.sub(pattern, replacement, redacted)
    return redacted


def redacted_lines(value: str, data: dict[str, Any] | None = None, limit: int | None = None) -> list[str]:
    lines = redact_text(value, data).splitlines()
    return lines[-limit:] if limit else lines


def missing_config_fields(data: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for key in ("required_origin_url", "clone_url", "remote_worktree", "remote_log_dir"):
        if not data.get("repo", {}).get(key) or str(data["repo"][key]).startswith(("<", "/path/")):
            missing.append(f"repo.{key}")
    ssh = data.get("ssh", {})
    if not ssh.get("user") or str(ssh.get("user")).startswith("<"):
        missing.append("ssh.user")
    targets = data.get("targets") or []
    if not targets:
        missing.append("targets[]")
    for idx, target in enumerate(targets):
        if not target.get("name"):
            missing.append(f"targets[{idx}].name")
        if not target.get("host") or str(target.get("host")).startswith("<"):
            missing.append(f"targets[{idx}].host")
        if not target.get("port"):
            missing.append(f"targets[{idx}].port")
    return missing


def ssh_base(data: dict[str, Any], target: dict[str, Any]) -> list[str]:
    ssh = data["ssh"]
    dest = f"{ssh['user']}@{target['host']}"
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8"]
    if ssh.get("jump"):
        cmd.extend(["-J", str(ssh["jump"])])
    cmd.extend(["-p", str(target["port"]), dest])
    return cmd


def remote_script(data: dict[str, Any], branch: str, command: str) -> str:
    repo = data["repo"]
    worktree = shlex.quote(str(repo["remote_worktree"]))
    log_dir = shlex.quote(str(repo["remote_log_dir"]))
    clone_url = shlex.quote(str(repo["clone_url"]))
    branch_q = shlex.quote(branch)
    command_q = command
    return f"""
set -euo pipefail
if [ ! -d {worktree}/.git ]; then
  mkdir -p "$(dirname {worktree})"
  git clone -b {branch_q} {clone_url} {worktree}
fi
cd {worktree}
git fetch origin {branch_q}
git checkout {branch_q}
git pull --ff-only origin {branch_q}
mkdir -p {log_dir}
log={log_dir}/ds-test-$(date +%Y%m%d-%H%M%S).log
set +e
({command_q}) >"$log" 2>&1
status=$?
set -e
echo "status=$status"
echo "log_saved=true"
grep -E "FAILED|FAILURE|ERROR|Error|error:|undefined reference|No rule to make target" "$log" | tail -80 || true
tail -120 "$log" || true
exit "$status"
""".strip()


def command_check_config(args: argparse.Namespace) -> int:
    data = load_config(args)
    missing = missing_config_fields(data)
    if missing:
        raise DsTestError(
            "Validation config has placeholders or missing fields: "
            + ", ".join(missing)
            + f". Edit the private local config using {EXAMPLE_CONFIG} as the template."
        )
    print(json.dumps({
        "ok": True,
        "config_source": data["_config_source"],
        "targets": [t["name"] for t in data["targets"]],
        "private_details_redacted": True,
    }, ensure_ascii=False, indent=2))
    return 0


def command_plan(args: argparse.Namespace) -> int:
    files = args.changed_files or []
    local_system = platform.system() or "unknown"
    compile_like_change = any(path.startswith(("src/", "include/", "tests/")) for path in files)
    bazel_change = any(path.endswith(("BUILD.bazel", ".bzl")) for path in files)
    remote_required = local_system != "Linux" and (compile_like_change or bazel_change)
    commands = ["local: rtk git diff --check"]
    labels: list[str] = []
    setup_hints: list[str] = []
    if remote_required:
        setup_hints.append(
            "Local platform is not a supported build/test host for yuanrong-datasystem. "
            f"Copy .skills/ds-test/references/validation_config.example.toml to {DEFAULT_CONFIG}, "
            "then fill in a Linux validation target locally."
        )
    if compile_like_change:
        prefix = "remote/Linux" if remote_required else "local or remote Linux"
        commands.append(f"{prefix}: rtk bash build.sh -t build")
    if any(path.startswith("tests/ut/") or "common/util/gflag" in path for path in files):
        prefix = "remote/Linux" if remote_required else "local or remote Linux"
        commands.append(f"{prefix}: run targeted UT/CTest for the touched test suite after build")
        labels.append("ut")
    if any(path.startswith("tests/st/") for path in files):
        prefix = "remote/Linux" if remote_required else "local or remote Linux"
        commands.append(f"{prefix}: run the targeted ST CTest after full compile")
        labels.append("st")
    if bazel_change:
        prefix = "remote/Linux" if remote_required else "local or remote Linux"
        commands.append(f"{prefix}: run the relevant Bazel build/test target")
    print(json.dumps({
        "changed_files": files,
        "local_platform": local_system,
        "remote_validation_required": remote_required,
        "recommended_commands": commands,
        "labels": labels,
        "setup_hints": setup_hints,
    }, ensure_ascii=False, indent=2))
    return 0


def command_probe(args: argparse.Namespace) -> int:
    data = load_config(args)
    missing = missing_config_fields(data)
    if missing:
        raise DsTestError("Cannot probe until config is complete: " + ", ".join(missing))
    results = []
    for target in data["targets"]:
        probe = "echo cpu=$(nproc); awk '/MemAvailable/ {print $1\"=\"$2$3}' /proc/meminfo; " \
                "pgrep -af 'build.sh|bazel|ctest|make -j|ninja' | head -10 || true"
        proc = subprocess.run(ssh_base(data, target) + [probe], text=True, capture_output=True)
        results.append({
            "target": target["name"],
            "ok": proc.returncode == 0,
            "summary": redacted_lines(proc.stdout.strip(), data)[:12],
            "error": redacted_lines(proc.stderr.strip(), data, 3),
        })
    print(json.dumps({"probed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                      "results": results}, ensure_ascii=False, indent=2))
    return 0 if any(item["ok"] for item in results) else 1


def command_run_remote(args: argparse.Namespace) -> int:
    data = load_config(args)
    missing = missing_config_fields(data)
    if missing:
        raise DsTestError("Cannot run remote validation until config is complete: " + ", ".join(missing))
    target = next((t for t in data["targets"] if t["name"] == args.target), data["targets"][0])
    script = remote_script(data, args.branch, args.command)
    proc = subprocess.run(ssh_base(data, target) + [script], text=True, capture_output=True)
    print(json.dumps({
        "target": target["name"],
        "branch": args.branch,
        "command": args.command,
        "status": proc.returncode,
        "stdout_tail": redacted_lines(proc.stdout, data, 120),
        "stderr_tail": redacted_lines(proc.stderr, data, 40),
        "private_details_redacted": True,
    }, ensure_ascii=False, indent=2))
    return proc.returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Configured validation helper for yuanrong-datasystem.")
    parser.add_argument(
        "--config",
        help=f"Private local ds-test TOML config. Defaults to {DEFAULT_CONFIG}; DS_TEST_CONFIG is a local override.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-config", help="Validate private remote config.").set_defaults(func=command_check_config)
    plan = sub.add_parser("plan", help="Recommend validation commands from changed files.")
    plan.add_argument("--changed-files", nargs="*")
    plan.set_defaults(func=command_plan)
    sub.add_parser("probe", help="Probe configured target aliases.").set_defaults(func=command_probe)
    run_remote = sub.add_parser("run-remote", help="Run one validation command on a configured remote target.")
    run_remote.add_argument("--branch", required=True)
    run_remote.add_argument("--command", required=True)
    run_remote.add_argument("--target", default="")
    run_remote.set_defaults(func=command_run_remote)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except DsTestError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
