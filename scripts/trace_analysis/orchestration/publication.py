"""Immutable report generations committed through a single atomic HTML entry pointer."""
from __future__ import annotations

import html
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import tempfile
from uuid import uuid4

from ..stage_cache import _file_hash

REQUIRED = ("index.html", "suite.manifest.json", "suite.analysis.json", "publication.validation.json")


class _EntryPointer(HTMLParser):
    def __init__(self):
        super().__init__()
        self.pointer = None

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        if tag == "meta" and attrs.get("name") == "trace-publication":
            if self.pointer is not None:
                raise ValueError("duplicate publication pointer")
            self.pointer = attrs


def new_publication(root):
    directory = Path(root).resolve() / "publications" / uuid4().hex
    directory.mkdir(parents=True)
    return directory


def _inside(root, value):
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise ValueError("publication references must be nonempty relative paths")
    path = (root / value).resolve()
    if not path.is_relative_to(root):
        raise ValueError("publication artifact escapes its generation")
    return path


def current_publication(root):
    root = Path(root).resolve()
    entry = root / "index.html"
    if not entry.is_file():
        return None
    parser = _EntryPointer()
    parser.feed(entry.read_text(encoding="utf-8"))
    if parser.pointer is None:
        return None
    manifest = _inside(root, parser.pointer.get("content", ""))
    directory = manifest.parent
    if not directory.is_relative_to(root / "publications") or manifest.name != "suite.manifest.json":
        raise ValueError("invalid publication pointer")
    descriptor_path = directory / "publication.json"
    if _file_hash(descriptor_path) != parser.pointer.get("data-sha256"):
        raise ValueError("publication descriptor changed")
    return validate_publication(directory)


def validate_publication(directory):
    directory = Path(directory).resolve()
    descriptor_path = directory / "publication.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    if not isinstance(descriptor, dict):
        raise ValueError("invalid publication descriptor")
    artifacts = descriptor.get("artifacts")
    version = descriptor.get("schema_version")
    if type(version) is not int or version != 1 or not isinstance(artifacts, dict):
        raise ValueError("invalid publication descriptor")
    if not isinstance(descriptor.get("key"), str) or not descriptor["key"]:
        raise ValueError("invalid publication descriptor key")
    if not set(REQUIRED).issubset(artifacts):
        raise ValueError("publication descriptor is incomplete")
    for name, expected in artifacts.items():
        path = _inside(directory, name)
        if not path.is_file() or _file_hash(path) != expected:
            raise ValueError(f"published artifact missing or changed: {name}")
    return {"directory": directory, "manifest": directory / "suite.manifest.json",
            "index": directory / "index.html", "key": descriptor["key"]}


def commit_publication(root, directory, key):
    root, directory = Path(root).resolve(), Path(directory).resolve()
    if not directory.is_relative_to(root / "publications"):
        raise ValueError("publication must remain inside the publications directory")
    if (directory / "publication.json").exists():
        raise ValueError("a sealed publication cannot be overwritten")
    if any(not (directory / name).is_file() for name in REQUIRED):
        raise ValueError("publication is incomplete")
    validation = json.loads((directory / "publication.validation.json").read_text(encoding="utf-8"))
    if not isinstance(validation, dict) or validation.get("valid") is not True:
        raise ValueError("publication validation failed")
    artifacts = {}
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            raise ValueError("published artifacts cannot be symlinks")
        if path.is_file():
            artifacts[path.relative_to(directory).as_posix()] = _file_hash(path)
    descriptor = directory / "publication.json"
    descriptor.write_text(json.dumps({"schema_version": 1, "key": key, "artifacts": artifacts},
                                     ensure_ascii=False, sort_keys=True), encoding="utf-8")
    index = html.escape((directory / "index.html").relative_to(root).as_posix(), quote=True)
    manifest = html.escape((directory / "suite.manifest.json").relative_to(root).as_posix(), quote=True)
    page = (f'<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">'
            f'<meta name="trace-publication" content="{manifest}" data-sha256="{_file_hash(descriptor)}">'
            f'<meta http-equiv="refresh" content="0;url={index}"><title>Trace 分析报告</title></head>'
            f'<body style="font-family:Microsoft YaHei,sans-serif"><a href="{index}">打开最新完整报告</a></body></html>')
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=root,
                                         prefix=".index-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(page)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, root / "index.html")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return {"directory": directory, "manifest": directory / "suite.manifest.json",
            "index": directory / "index.html", "key": key}
