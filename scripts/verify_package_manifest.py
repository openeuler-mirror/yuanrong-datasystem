#!/usr/bin/env python3
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Verify release package file manifests.

The build scripts own package creation. This helper is intentionally read-only:
it lists the generated deployment tarball and Python wheel, compares their
package paths with checked-in manifests, and exits non-zero on shape drift.
"""

import argparse
import sys
import tarfile
import zipfile
from pathlib import Path
from typing import Iterable, Optional, Sequence


DEFAULT_TAR_PATTERN = "yr-datasystem-v*.tar.gz"
DEFAULT_WHEEL_PATTERN = "openyuanrong_datasystem-*.whl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--install-dir",
        type=Path,
        default=Path("output"),
        help="Directory containing release package files. Default: output",
    )
    parser.add_argument(
        "--tar-file",
        type=Path,
        help="Deployment tarball path. Defaults to the single yr-datasystem-v*.tar.gz under --install-dir.",
    )
    parser.add_argument(
        "--wheel-file",
        type=Path,
        help="Python wheel path. Defaults to the single openyuanrong_datasystem-*.whl under --install-dir.",
    )
    parser.add_argument(
        "--tar-manifest",
        type=Path,
        required=True,
        help="Expected deployment tarball path manifest.",
    )
    parser.add_argument(
        "--wheel-manifest",
        type=Path,
        required=True,
        help="Expected Python wheel path manifest.",
    )
    parser.add_argument(
        "--dump-current",
        type=Path,
        help="Optional directory to write current tar and wheel manifests for review.",
    )
    return parser.parse_args()


def resolve_single_file(install_dir: Path, explicit_file: Optional[Path], pattern: str, package_name: str) -> Path:
    if explicit_file is not None:
        package_file = explicit_file
        if not package_file.is_absolute():
            package_file = Path.cwd() / package_file
        if not package_file.is_file():
            raise FileNotFoundError(f"{package_name} file not found: {package_file}")
        return package_file

    matches = sorted(install_dir.glob(pattern))
    if len(matches) != 1:
        match_list = "\n".join(f"  - {item}" for item in matches) or "  <none>"
        raise FileNotFoundError(
            f"Expected exactly one {package_name} matching {install_dir / pattern}, found {len(matches)}:\n"
            f"{match_list}\n"
            f"Pass --{package_name}-file to disambiguate."
        )
    return matches[0]


def normalize_package_path(path: str) -> str:
    normalized = path.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def read_expected_manifest(path: Path) -> list[str]:
    items: list[str] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        normalized = normalize_package_path(line)
        if normalized in items:
            raise ValueError(f"Duplicate manifest path in {path}:{line_no}: {normalized}")
        items.append(normalized)
    return sorted(items)


def list_tar_manifest(path: Path) -> list[str]:
    with tarfile.open(path, "r:gz") as archive:
        items = []
        for member in archive.getmembers():
            package_path = normalize_package_path(member.name)
            if member.isdir() and not package_path.endswith("/"):
                package_path = f"{package_path}/"
            items.append(package_path)
        return sorted(items)


def list_wheel_manifest(path: Path) -> list[str]:
    with zipfile.ZipFile(path) as archive:
        return sorted(
            normalize_package_path(name)
            for name in archive.namelist()
            if not normalize_package_path(name).endswith("/")
        )


def write_manifest(path: Path, items: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{item}\n" for item in items), encoding="utf-8")


def ensure_unique(package_name: str, items: Sequence[str]) -> bool:
    seen: set[str] = set()
    duplicates: list[str] = []
    for item in items:
        if item in seen and item not in duplicates:
            duplicates.append(item)
        seen.add(item)
    if not duplicates:
        return True

    print(f"[FAIL] {package_name}: duplicate package paths detected", file=sys.stderr)
    print(format_diff("Duplicate paths", duplicates), file=sys.stderr)
    return False


def format_diff(title: str, items: Sequence[str], limit: int = 80) -> str:
    if not items:
        return ""
    shown = "\n".join(f"  {item}" for item in items[:limit])
    remaining = len(items) - limit
    suffix = f"\n  ... {remaining} more" if remaining > 0 else ""
    return f"{title} ({len(items)}):\n{shown}{suffix}"


def verify_manifest(package_name: str, expected: Sequence[str], actual: Sequence[str]) -> bool:
    if not ensure_unique(package_name, actual):
        return False

    expected_set = set(expected)
    actual_set = set(actual)
    missing = sorted(expected_set - actual_set)
    extra = sorted(actual_set - expected_set)
    if not missing and not extra:
        print(f"[PASS] {package_name}: {len(actual)} paths match expected manifest")
        return True

    print(f"[FAIL] {package_name}: manifest drift detected", file=sys.stderr)
    missing_text = format_diff("Missing expected paths", missing)
    extra_text = format_diff("Unexpected extra paths", extra)
    if missing_text:
        print(missing_text, file=sys.stderr)
    if extra_text:
        print(extra_text, file=sys.stderr)
    return False


def main() -> int:
    args = parse_args()
    install_dir = args.install_dir
    if not install_dir.is_absolute():
        install_dir = Path.cwd() / install_dir

    try:
        tar_file = resolve_single_file(install_dir, args.tar_file, DEFAULT_TAR_PATTERN, "tar")
        wheel_file = resolve_single_file(install_dir, args.wheel_file, DEFAULT_WHEEL_PATTERN, "wheel")
        actual_tar = list_tar_manifest(tar_file)
        actual_wheel = list_wheel_manifest(wheel_file)

        if args.dump_current:
            write_manifest(args.dump_current / "current-tar-manifest.txt", actual_tar)
            write_manifest(args.dump_current / "current-wheel-manifest.txt", actual_wheel)

        expected_tar = read_expected_manifest(args.tar_manifest)
        expected_wheel = read_expected_manifest(args.wheel_manifest)
    except (FileNotFoundError, tarfile.TarError, zipfile.BadZipFile, ValueError) as err:
        print(f"[ERROR] {err}", file=sys.stderr)
        return 2

    tar_ok = verify_manifest("tar", expected_tar, actual_tar)
    wheel_ok = verify_manifest("wheel", expected_wheel, actual_wheel)
    return 0 if tar_ok and wheel_ok else 1


if __name__ == "__main__":
    sys.exit(main())
