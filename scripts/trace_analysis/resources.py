"""Locate versioned report resources independently of the working directory."""
import hashlib
import json
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
ASSETS_ROOT = PACKAGE_ROOT / "assets"


def repository_root():
    return PACKAGE_ROOT.parents[1]


def asset_path(name):
    if Path(name).name != name:
        raise ValueError("asset name must be a plain filename")
    group = next((item for item in ("triage", "read", "write", "numa", "overview")
                  if name.startswith((item + ".", item + "_"))), "shared")
    return ASSETS_ROOT / group / name


def echarts_path():
    return ASSETS_ROOT / "vendor/echarts/echarts-5.5.1.min.js"


@lru_cache(maxsize=4)
def producer_revision(package_root=PACKAGE_ROOT):
    """Identify this tool's source checkout or stamped wheel, never the analyzed service."""
    package_root = Path(package_root).resolve()
    build_info = package_root / "_build_info.json"
    if build_info.is_file():
        try:
            info = json.loads(build_info.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return "unknown"
        if not isinstance(info, dict) or info.get("schema_version") != 1:
            return "unknown"
        revision = info.get("revision")
        return revision if isinstance(revision, str) and revision else "unknown"
    checkout = package_root.parents[1]
    git = shutil.which("git")
    if git is None:
        return "unknown"
    git = str(Path(git).resolve())
    try:
        location = subprocess.run(
            [git, "-C", str(checkout), "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        if location.returncode or Path(location.stdout.strip()).resolve() != checkout:
            return "unknown"
        head = subprocess.run(
            [git, "-C", str(checkout), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        status = subprocess.run(
            [git, "-C", str(checkout), "status", "--porcelain", "--",
             "scripts/trace_analysis", "scripts/trace_analysis_dist"],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"
    if head.returncode or status.returncode:
        return "unknown"
    revision = head.stdout.strip()
    if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
        return "unknown"
    return revision + ("+dirty" if status.stdout else "")


def tool_fingerprint(package_root=PACKAGE_ROOT):
    """Hash paths and contents, excluding generated Python caches."""
    root = Path(package_root)
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if not path.is_file() or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(path.stat().st_size.to_bytes(8, "big"))
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()
