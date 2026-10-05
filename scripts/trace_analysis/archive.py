"""Collection identities and compressed-file digests without parsing log content."""
import hashlib
import re
from pathlib import Path, PurePosixPath


def archive_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def trace_member_id(name):
    leaf = PurePosixPath(name.replace("\\", "/")).name
    if leaf.startswith("unique_traces"):
        return None
    return re.sub(r"_\d+$", "", leaf)


def collection_cohort(name):
    if trace_member_id(name) is None:
        return None
    parts = PurePosixPath(name.replace("\\", "/")).parts
    for index, part in enumerate(parts[:-2]):
        if part in {"core", "all-core"} or part.startswith("all-core_situation_"):
            return "core/" + parts[index + 1].replace(",", "_")
        if part in {"time", "time-buckets", "timeCollect"} or part.startswith("time_situation_"):
            return "time/" + parts[index + 1].replace(",", "_")
    return None
