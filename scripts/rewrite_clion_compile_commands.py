#!/usr/bin/env python3
import json
import logging
import os
import shlex
from pathlib import Path


LOG = logging.getLogger(__name__)


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


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    root = Path.cwd()
    remote_cache = os.environ.get("REMOTE_THIRDPARTY", "/home/ds-thirdparty-cache")
    worktree_name = root.name
    default_remote_root = f".clion-remote/{worktree_name}"
    local_cache = str(root / os.environ.get("LOCAL_THIRDPARTY", f"{default_remote_root}/ds-thirdparty-cache"))
    build_dir = Path(os.environ.get("BUILD_DIR", f"{default_remote_root}/build"))
    src = root / build_dir / "compile_commands.json.remote"
    dst = root / build_dir / "compile_commands.json"

    data = json.loads(src.read_text())
    for entry in data:
        if "command" in entry:
            entry["command"] = entry["command"].replace(remote_cache, local_cache)
        if "arguments" in entry:
            entry["arguments"] = [arg.replace(remote_cache, local_cache) for arg in entry["arguments"]]
    created = ensure_local_include_dirs(data, root, build_dir, local_cache)
    dst.write_text(json.dumps(data, indent=2) + "\n")
    LOG.info("compile_commands entries: %s", len(data))
    LOG.info("created missing local include dirs: %s", created)


if __name__ == "__main__":
    main()
