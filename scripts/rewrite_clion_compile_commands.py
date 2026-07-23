#!/usr/bin/env python3
import json
import logging
import os
import shlex
from pathlib import Path


LOG = logging.getLogger(__name__)
DEFAULT_REMOTE_THIRDPARTY = "/home/cache/ds-thirdparty-cache"
LEGACY_REMOTE_THIRDPARTY = "/home/ds-thirdparty-cache"


def include_paths(entry):
    command = entry.get("command")
    if command:
        args = shlex.split(command)
    else:
        args = entry.get("arguments", [])

    idx = 0
    while idx < len(args):
        arg = args[idx]
        path = None
        if arg in ("-I", "-isystem", "-iquote") and idx + 1 < len(args):
            idx += 1
            path = args[idx]
        elif arg.startswith("-I") and len(arg) > 2:
            path = arg[2:]
        elif arg.startswith("-isystem") and len(arg) > len("-isystem"):
            path = arg[len("-isystem"):]
        if path:
            yield Path(path)
        idx += 1


def ensure_local_include_dirs(data, root, build_dir, local_cache):
    allowed_roots = [root / build_dir, Path(local_cache)]
    created = 0
    seen = set()
    for entry in data:
        for path in include_paths(entry):
            if path in seen or path.exists():
                continue
            seen.add(path)
            if any(path.is_absolute() and path.is_relative_to(base) for base in allowed_roots):
                path.mkdir(parents=True, exist_ok=True)
                created += 1
    return created


def remote_cache_roots(primary_remote_cache):
    roots = [primary_remote_cache]
    aliases = os.environ.get("REMOTE_THIRDPARTY_ALIASES", LEGACY_REMOTE_THIRDPARTY)
    for alias in aliases.split(":"):
        if alias and alias not in roots:
            roots.append(alias)
    return roots


def rewrite_cache_paths(entry, remote_caches, local_cache):
    if "command" in entry:
        command = entry["command"]
        for remote_cache in remote_caches:
            command = command.replace(remote_cache, local_cache)
        entry["command"] = command
    if "arguments" in entry:
        rewritten = []
        for arg in entry["arguments"]:
            for remote_cache in remote_caches:
                arg = arg.replace(remote_cache, local_cache)
            rewritten.append(arg)
        entry["arguments"] = rewritten


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    root = Path.cwd()
    remote_cache = os.environ.get("REMOTE_THIRDPARTY", DEFAULT_REMOTE_THIRDPARTY)
    worktree_name = root.name
    default_remote_root = f".clion-remote/{worktree_name}"
    local_cache = str(root / os.environ.get("LOCAL_THIRDPARTY", f"{default_remote_root}/ds-thirdparty-cache"))
    build_dir = Path(os.environ.get("BUILD_DIR", f"{default_remote_root}/build"))
    src = root / build_dir / "compile_commands.json.remote"
    dst = root / build_dir / "compile_commands.json"

    data = json.loads(src.read_text())
    remote_caches = remote_cache_roots(remote_cache)
    for entry in data:
        rewrite_cache_paths(entry, remote_caches, local_cache)
    created = ensure_local_include_dirs(data, root, build_dir, local_cache)
    dst.write_text(json.dumps(data, indent=2) + "\n")
    LOG.info("compile_commands entries: %s", len(data))
    LOG.info("created missing local include dirs: %s", created)


if __name__ == "__main__":
    main()
