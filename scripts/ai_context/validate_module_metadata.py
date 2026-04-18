#!/usr/bin/env python3

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
METADATA_DIR = REPO_ROOT / ".repo_context" / "modules" / "metadata"

REQUIRED_TOP_LEVEL_KEYS = {
    "id": str,
    "title": str,
    "status": str,
    "domain": str,
    "module_type": str,
    "canonical_docs": dict,
    "playbooks": dict,
    "source_roots": list,
    "key_tests": list,
    "module_boundary": dict,
    "retrieval": dict,
}

VALID_MODULE_TYPES = {"leaf", "composite"}
VALID_STATUSES = {"active", "draft", "deprecated"}


def rel_exists(rel_path: str) -> bool:
    return (REPO_ROOT / rel_path).exists()


def expect_type(errors: list[str], value, expected_type, label: str) -> None:
    if not isinstance(value, expected_type):
        errors.append(f"{label} must be {expected_type.__name__}")


def validate_metadata_file(path: Path) -> list[str]:
    errors: list[str] = []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"{path.name}: invalid JSON: {exc}"]

    for key, expected_type in REQUIRED_TOP_LEVEL_KEYS.items():
        if key not in payload:
            errors.append(f"{path.name}: missing top-level key `{key}`")
            continue
        expect_type(errors, payload[key], expected_type, f"{path.name}:{key}")

    if errors:
        return errors

    expected_id = path.stem
    if payload["id"] != expected_id:
        errors.append(f"{path.name}: id `{payload['id']}` must match filename stem `{expected_id}`")

    if payload["module_type"] not in VALID_MODULE_TYPES:
        errors.append(f"{path.name}: module_type must be one of {sorted(VALID_MODULE_TYPES)}")
    if payload["status"] not in VALID_STATUSES:
        errors.append(f"{path.name}: status must be one of {sorted(VALID_STATUSES)}")

    canonical_docs = payload["canonical_docs"]
    for key in ("overview", "design", "details"):
        if key not in canonical_docs:
            errors.append(f"{path.name}: canonical_docs missing `{key}`")
    if "overview" in canonical_docs:
        if not isinstance(canonical_docs["overview"], str):
            errors.append(f"{path.name}: canonical_docs.overview must be string")
        elif not rel_exists(canonical_docs["overview"]):
            errors.append(f"{path.name}: missing overview doc `{canonical_docs['overview']}`")
    if "design" in canonical_docs:
        if canonical_docs["design"] is not None and not isinstance(canonical_docs["design"], str):
            errors.append(f"{path.name}: canonical_docs.design must be string or null")
        elif isinstance(canonical_docs["design"], str) and not rel_exists(canonical_docs["design"]):
            errors.append(f"{path.name}: missing design doc `{canonical_docs['design']}`")
    if "details" in canonical_docs:
        if not isinstance(canonical_docs["details"], list):
            errors.append(f"{path.name}: canonical_docs.details must be list")
        else:
            for item in canonical_docs["details"]:
                if not isinstance(item, str):
                    errors.append(f"{path.name}: canonical_docs.details entries must be strings")
                elif not rel_exists(item):
                    errors.append(f"{path.name}: missing detail doc `{item}`")

    playbooks = payload["playbooks"]
    for group in ("feature", "operations", "upkeep"):
        if group not in playbooks:
            errors.append(f"{path.name}: playbooks missing `{group}`")
            continue
        if not isinstance(playbooks[group], list):
            errors.append(f"{path.name}: playbooks.{group} must be list")
            continue
        for item in playbooks[group]:
            if not isinstance(item, str):
                errors.append(f"{path.name}: playbooks.{group} entries must be strings")
            elif not rel_exists(item):
                errors.append(f"{path.name}: missing playbook `{item}`")

    for group_name in ("source_roots", "key_tests"):
        for item in payload[group_name]:
            if not isinstance(item, str):
                errors.append(f"{path.name}: {group_name} entries must be strings")
            elif not rel_exists(item):
                errors.append(f"{path.name}: missing path `{item}`")

    module_boundary = payload["module_boundary"]
    for key in ("related_modules", "split_signals"):
        if key not in module_boundary:
            errors.append(f"{path.name}: module_boundary missing `{key}`")
            continue
        if not isinstance(module_boundary[key], list):
            errors.append(f"{path.name}: module_boundary.{key} must be list")
            continue
        for item in module_boundary[key]:
            if not isinstance(item, str):
                errors.append(f"{path.name}: module_boundary.{key} entries must be strings")

    retrieval = payload["retrieval"]
    for key in ("keywords", "intent_examples"):
        if key not in retrieval:
            errors.append(f"{path.name}: retrieval missing `{key}`")
            continue
        if not isinstance(retrieval[key], list):
            errors.append(f"{path.name}: retrieval.{key} must be list")
            continue
        for item in retrieval[key]:
            if not isinstance(item, str):
                errors.append(f"{path.name}: retrieval.{key} entries must be strings")

    return errors


def main() -> int:
    if not METADATA_DIR.exists():
        print(f"metadata directory not found: {METADATA_DIR}", file=sys.stderr)
        return 1

    errors: list[str] = []
    metadata_files = sorted(METADATA_DIR.glob("*.json"))
    if not metadata_files:
        print(f"no metadata files found under {METADATA_DIR}", file=sys.stderr)
        return 1

    for path in metadata_files:
        errors.extend(validate_metadata_file(path))

    if errors:
        print("module metadata validation failed:", file=sys.stderr)
        for err in errors:
            print(f"- {err}", file=sys.stderr)
        return 1

    print(f"validated {len(metadata_files)} module metadata files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
