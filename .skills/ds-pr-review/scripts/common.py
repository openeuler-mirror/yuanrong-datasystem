#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPT_DIR.parent
ASSETS_DIR = SKILL_ROOT / "assets"
DEFAULT_CONFIG_PATH = ASSETS_DIR / "default_config.toml"


class ReviewError(RuntimeError):
    """Raised when the review workflow cannot continue safely."""


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def expand_path(value: str) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(value))).resolve()


def load_settings() -> dict[str, Any]:
    data = tomllib.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))

    gitcode = data.setdefault("gitcode", {})
    gitcode["token_file"] = str(expand_path(gitcode["token_file"]))

    repo = data.setdefault("repo", {})
    repo["local_path"] = str(expand_path(repo["local_path"]))

    cache = data.setdefault("cache", {})
    cache["root"] = str(expand_path(cache["root"]))

    api_base = os.environ.get("GITCODE_API_BASE")
    if api_base:
        gitcode["api_base_url"] = api_base.rstrip("/")

    repo_override = os.environ.get("YUANRONG_REPO_PATH")
    if repo_override:
        repo["local_path"] = str(expand_path(repo_override))

    return data


def load_token(settings: dict[str, Any]) -> str:
    token_file = Path(settings["gitcode"]["token_file"])
    if token_file.exists():
        token = token_file.read_text(encoding="utf-8").strip()
        if token:
            return token

    for env_name in ("GITCODE_TOKEN", "GITCODE_ACCESS_TOKEN", "GITCODE_TOEKEN"):
        token = os.environ.get(env_name, "").strip()
        if token:
            return token

    raise ReviewError(
        "No GitCode token found. Expected ~/.local/gitcode_token or env GITCODE_TOKEN / "
        "GITCODE_ACCESS_TOKEN / GITCODE_TOEKEN."
    )


def parse_pr_ref(value: str) -> int:
    value = value.strip()
    if value.isdigit():
        return int(value)

    match = re.search(r"/(?:pull|pulls|merge_requests)/(\d+)", value)
    if match:
        return int(match.group(1))

    raise ReviewError(f"Unsupported PR reference: {value}")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def normalize_ws(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip().lower()


def run_git(args: list[str], cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd) if cwd else None,
        check=check,
        text=True,
        capture_output=True,
    )


def ensure_local_repo(settings: dict[str, Any]) -> tuple[Path | None, list[str]]:
    repo_settings = settings["repo"]
    repo_path = Path(repo_settings["local_path"])
    warnings: list[str] = []

    if not repo_settings.get("sync_on_prepare", True):
        return (repo_path if repo_path.exists() else None, warnings)

    if not repo_path.exists():
        try:
            ensure_dir(repo_path.parent)
            run_git(["clone", repo_settings["clone_url"], str(repo_path)])
        except Exception as exc:  # pragma: no cover - best effort fallback
            warnings.append(
                f"Local repo is unavailable. Clone to {repo_path} failed, so the review will continue with diff only: {exc}"
            )
            return None, warnings

    try:
        remote = run_git(["remote", "get-url", "origin"], cwd=repo_path).stdout.strip()
        if remote != repo_settings["clone_url"]:
            warnings.append(
                f"Local repo origin is {remote}, not {repo_settings['clone_url']}. Context reads may be inaccurate."
            )
        run_git(["fetch", "--all", "--prune"], cwd=repo_path)
    except Exception as exc:  # pragma: no cover - best effort fallback
        warnings.append(f"Local repo fetch failed; continuing with existing checkout or diff-only context: {exc}")

    return repo_path, warnings
