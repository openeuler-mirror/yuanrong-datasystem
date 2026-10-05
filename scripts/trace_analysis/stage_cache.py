"""Verified stage records; callers publish only after production and validation succeed.

One runner owns an output root. Different run/stage records may be written in
parallel; this cache does not serialize producers writing the same artifacts.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


def _canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def make_stage_key(input_hashes, config, version):
    """Hash caller-supplied inputs without changing their contents."""
    payload = {"inputs": input_hashes, "config": config, "version": version}
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class CacheLookup:
    status: str
    reason: str
    artifacts: Mapping[str, Path] = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, "artifacts", MappingProxyType(dict(self.artifacts)))


class StageCache:
    def __init__(self, root):
        self.root = Path(root).resolve()

    def manifest_path(self, run_id, stage):
        for value in (run_id, stage):
            if not isinstance(value, str) or not value:
                raise ValueError("run_id and stage must be nonempty strings")
        identity = hashlib.sha256(_canonical_json([run_id, stage]).encode("utf-8")).hexdigest()
        return self.root / identity[:2] / f"{identity}.json"

    @staticmethod
    def _valid_record(record, run_id, stage):
        if not isinstance(record, dict):
            return False
        if record.get("schema_version") != 1 or record.get("run_id") != run_id or record.get("stage") != stage:
            return False
        if not isinstance(record.get("key"), str) or not record["key"]:
            return False
        artifacts = record.get("artifacts")
        if not isinstance(artifacts, dict) or not artifacts:
            return False
        for name, item in artifacts.items():
            if not isinstance(name, str) or not name or not isinstance(item, dict):
                return False
            path, digest = item.get("path"), item.get("sha256")
            if not isinstance(path, str) or not path or not Path(path).is_absolute():
                return False
            if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                return False
        return True

    def lookup(self, run_id, stage, key):
        manifest = self.manifest_path(run_id, stage)
        try:
            record = json.loads(manifest.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return CacheLookup("miss", "manifest_missing")
        except (OSError, ValueError):
            return CacheLookup("miss", "manifest_invalid")
        if not self._valid_record(record, run_id, stage):
            return CacheLookup("miss", "manifest_invalid")
        if record["key"] != key:
            return CacheLookup("miss", "key_changed")
        artifacts = {}
        for name, item in record["artifacts"].items():
            path = Path(item["path"])
            try:
                if not path.is_file():
                    return CacheLookup("miss", "artifact_missing")
                if _file_hash(path) != item["sha256"]:
                    return CacheLookup("miss", "artifact_changed")
            except OSError:
                return CacheLookup("miss", "artifact_unreadable")
            artifacts[name] = path
        return CacheLookup("hit", "verified", artifacts)

    def record_success(self, run_id, stage, key, artifacts):
        manifest = self.manifest_path(run_id, stage)
        if not isinstance(key, str) or not key or not artifacts:
            raise ValueError("a nonempty key and explicit artifact file mapping are required")
        entries = {}
        for name, value in artifacts.items():
            if not isinstance(name, str) or not name:
                raise ValueError("artifact names must be nonempty strings")
            path = Path(value).resolve()
            if not path.is_file():
                raise ValueError(f"artifact must be an existing file: {path}")
            entries[name] = {"path": str(path), "sha256": _file_hash(path)}
        record = {"schema_version": 1, "run_id": run_id, "stage": stage, "key": key, "artifacts": entries}
        manifest.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=manifest.parent,
                                             prefix=manifest.name + ".", suffix=".tmp", delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(_canonical_json(record))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, manifest)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return manifest
