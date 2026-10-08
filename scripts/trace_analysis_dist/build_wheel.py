"""Build the standalone Trace wheel without involving the product SDK setup.py."""

import argparse
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


PACKAGE = Path(__file__).resolve().parents[1] / "trace_analysis"
BUILD_FILES = Path(__file__).resolve().parent
REQUIRED_ASSETS = (
    "trace_analysis/assets/read/read.html",
    "trace_analysis/assets/write/write.html",
    "trace_analysis/assets/triage/triage.html",
    "trace_analysis/assets/numa/numa.html",
    "trace_analysis/assets/overview/overview.html",
    "trace_analysis/assets/vendor/echarts/echarts-5.5.1.min.js",
)


def build(output: Path) -> Path:
    spec = importlib.util.spec_from_file_location("ds_trace_resources", PACKAGE / "resources.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load Trace package resources")
    resources = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(resources)

    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ds-trace-wheel-") as temporary:
        root = Path(temporary)
        shutil.copytree(PACKAGE, root / "src" / "trace_analysis",
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        (root / "src" / "trace_analysis" / "_build_info.json").write_text(
            json.dumps({"schema_version": 1, "revision": resources.producer_revision()}, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        shutil.copy2(BUILD_FILES / "pyproject.toml", root / "pyproject.toml")
        wheel_dir = root / "dist"
        subprocess.run([sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation",
                        "--wheel-dir", str(wheel_dir), str(root)], check=True)
        wheels = list(wheel_dir.glob("ds_trace_analysis-*.whl"))
        if len(wheels) != 1:
            raise RuntimeError("expected exactly one ds-trace-analysis wheel")
        with zipfile.ZipFile(wheels[0]) as archive:
            members = set(archive.namelist())
            expected_assets = {
                "trace_analysis/assets/" + str(path.relative_to(PACKAGE / "assets"))
                for path in (PACKAGE / "assets").rglob("*") if path.is_file()
            }
            missing = (set(REQUIRED_ASSETS) | expected_assets) - members
            if missing:
                raise ValueError("wheel lacks report assets: " + ", ".join(sorted(missing)))
        target = output / wheels[0].name
        shutil.copy2(wheels[0], target)
        return target


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=BUILD_FILES / "dist")
    args = parser.parse_args()
    sys.stdout.write(f"{build(args.output)}\n")


if __name__ == "__main__":
    main()
