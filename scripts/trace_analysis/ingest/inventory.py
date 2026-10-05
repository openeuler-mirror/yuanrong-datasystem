"""Inventory, read, and preserve bounded input packages for one Run."""
import gzip
import hashlib
import io
import os
import re
import shutil
import tarfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path


NOISE_ON_LABEL = "有底噪(dizao)"
NOISE_OFF_LABEL = "无底噪(wudizao)"
DEFAULT_MAX_TAR_MEMBERS = 10000
DEFAULT_MAX_TAR_TOTAL_BYTES = 1024 * 1024 * 1024
DEFAULT_MAX_TAR_MEMBER_BYTES = 512 * 1024 * 1024
DERIVED_TRACE_FILE_RE = re.compile(
    r"^(?:unique_traces?|trace_summary|inputs|manifest|summary|triage|report)_[^/]*\.(?:txt|json|md|html)$",
    re.I,
)


@dataclass(frozen=True)
class TarBudget:
    max_members: int = DEFAULT_MAX_TAR_MEMBERS
    max_total_bytes: int = DEFAULT_MAX_TAR_TOTAL_BYTES
    max_member_bytes: int = DEFAULT_MAX_TAR_MEMBER_BYTES

    def check(self, path, member, member_count, total_bytes):
        if member_count > self.max_members:
            raise ValueError(f"Tar member count exceeds {self.max_members}: {path}")
        if member.size > self.max_member_bytes:
            raise ValueError(f"Tar member too large: {member.name}")
        total_bytes += member.size
        if total_bytes > self.max_total_bytes:
            raise ValueError(f"Tar extracted bytes exceed {self.max_total_bytes}: {path}")
        return total_bytes


class TraceInputReader:
    """Read log lines from files, directories, gzip files, and tar bundles."""

    def __init__(self, budget=None, budget_validator=None):
        self.failures = []
        self.budget_validator = budget_validator or (budget or TarBudget()).check

    def iter_lines(self, paths):
        self.failures.clear()
        for raw_path in paths:
            path = Path(raw_path)
            if path.is_dir():
                for root, _, files in os.walk(path):
                    for name in sorted(files):
                        if DERIVED_TRACE_FILE_RE.match(name):
                            continue
                        yield from self.iter_file(Path(root) / name)
            else:
                yield from self.iter_file(path)

    def iter_file(self, path):
        try:
            if tarfile.is_tarfile(path):
                with tarfile.open(path, "r:*") as tar:
                    member_count = 0
                    total_bytes = 0
                    for member in tar.getmembers():
                        if not member.isfile():
                            continue
                        member_count += 1
                        total_bytes = self.budget_validator(path, member, member_count, total_bytes)
                        stream = tar.extractfile(member)
                        if stream is None:
                            continue
                        text = io.TextIOWrapper(stream, encoding="utf-8", errors="replace")
                        for line_no, line in enumerate(text, 1):
                            yield str(path), member.name, line_no, line.rstrip("\n")
                return
            opener = gzip.open if path.suffix == ".gz" else open
            with opener(path, "rt", encoding="utf-8", errors="replace") as f:
                for line_no, line in enumerate(f, 1):
                    yield str(path), path.name, line_no, line.rstrip("\n")
        except (OSError, UnicodeError, tarfile.TarError) as exc:
            self.failures.append({
                "path": str(path),
                "member": path.name,
                "error": type(exc).__name__,
                "message": str(exc),
            })
            return


def has_noise_token(text):
    lowered = text.lower()
    return any(token in lowered for token in ("dizao", "wudizao", "底噪"))


def is_noise_off(text):
    lowered = text.lower()
    return any(token in lowered for token in ("wudizao", "wu-dizao", "wu_dizao", "无底噪"))


def is_noise_on(text):
    lowered = text.lower()
    if is_noise_off(lowered):
        return False
    return "dizao" in lowered or "底噪" in lowered


def noise_context_for_path(path):
    path = Path(path)
    return "/".join(part for part in (path.parent.name, path.name) if part)


def detect_noise_cohort_mode(paths):
    for raw_path in paths:
        path = Path(raw_path)
        if has_noise_token(noise_context_for_path(path)):
            return True
        if path.is_dir():
            for root, _, files in os.walk(path):
                root_path = Path(root)
                root_context = "/".join(part for part in (root_path.parent.name, root_path.name) if part)
                if has_noise_token(root_context) or any(has_noise_token(name) for name in files):
                    return True
        elif path.exists() and tarfile.is_tarfile(path):
            try:
                with tarfile.open(path, "r:*") as tar:
                    if any(has_noise_token(member.name) for member in tar.getmembers()):
                        return True
            except tarfile.TarError:
                continue
    return False


def iter_input_leaf_paths(paths):
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            for root, _, files in os.walk(path):
                for name in files:
                    yield Path(root) / name
        else:
            yield path


def duplicate_input_basenames(paths):
    counts = Counter(Path(path).name for path in paths)
    return {name for name, count in counts.items() if name and count > 1}


def source_cohort_label(source, member, noise_cohort_mode=False, duplicate_basenames=None, input_paths=None):
    text = f"{noise_context_for_path(source)}/{member}"
    if is_noise_off(text):
        return NOISE_OFF_LABEL
    if is_noise_on(text):
        return NOISE_ON_LABEL
    if noise_cohort_mode:
        return NOISE_OFF_LABEL
    path = Path(source)
    for input_path in sorted((Path(item) for item in (input_paths or ())),
                             key=lambda item: len(item.parts), reverse=True):
        if path == input_path or (input_path.is_dir() and input_path in path.parents):
            path = input_path
            break
    if path.name in (duplicate_basenames or set()):
        return "/".join(part for part in (path.parent.name, path.name) if part)
    return path.name or str(path)


def slug(text):
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "-", text.strip()).strip("-").lower()
    return clean or "trace-run"


def preserved_input_name(index, path):
    p = Path(path)
    return f"{index:02d}-{slug(p.name or 'input')}"


def input_identity(path, index=None):
    p = Path(path)
    h = hashlib.sha256()
    members = []
    if p.is_file():
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        if tarfile.is_tarfile(p):
            with tarfile.open(p, "r:*") as tar:
                members = sorted(member.name for member in tar.getmembers() if member.isfile())
        return {
            "path": str(p),
            "size": p.stat().st_size,
            "sha256": h.hexdigest(),
            "members": members,
            "preserved_name": preserved_input_name(index or 1, p),
        }
    if p.is_dir():
        total_size = 0
        for fp in sorted(item for item in p.rglob("*") if item.is_file()):
            relative = fp.relative_to(p).as_posix()
            members.append(relative)
            total_size += fp.stat().st_size
            h.update(relative.encode("utf-8"))
            h.update(b"\0")
            with open(fp, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(chunk)
            h.update(b"\0")
        return {
            "path": str(p),
            "size": total_size,
            "sha256": h.hexdigest(),
            "members": members,
            "preserved_name": preserved_input_name(index or 1, p),
        }
    h.update(str(p).encode("utf-8"))
    return {"path": str(p), "size": 0, "sha256": h.hexdigest(), "members": members,
            "preserved_name": preserved_input_name(index or 1, p)}


def safe_member_path(member_name, extract_root):
    pure = Path(member_name)
    if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
        raise ValueError(f"Unsafe tar member path: {member_name}")
    target = (Path(extract_root) / pure).resolve()
    target.relative_to(Path(extract_root).resolve())
    return target


def preserve_raw_inputs(inputs, run_dir, *, budget=None):
    budget = budget or TarBudget()
    run_dir = Path(run_dir)
    raw_inputs = run_dir / "raw" / "inputs"
    raw_extracted = run_dir / "raw" / "extracted"
    raw_inputs.mkdir(parents=True, exist_ok=True)
    raw_extracted.mkdir(parents=True, exist_ok=True)
    for index, raw in enumerate(inputs, 1):
        p = Path(raw)
        preserved_name = preserved_input_name(index, p)
        if p.is_dir():
            target_root = raw_inputs / preserved_name
            file_count = 0
            total_bytes = 0
            for fp in sorted(item for item in p.rglob("*") if item.is_file()):
                file_count += 1
                if file_count > budget.max_members:
                    raise ValueError(f"Directory file count exceeds {budget.max_members}: {p}")
                size = fp.stat().st_size
                if size > budget.max_member_bytes:
                    raise ValueError(f"Directory file too large: {fp}")
                total_bytes += size
                if total_bytes > budget.max_total_bytes:
                    raise ValueError(f"Directory preserved bytes exceed {budget.max_total_bytes}: {p}")
                dest = target_root / fp.relative_to(p)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(fp, dest)
        if p.is_file():
            copied = raw_inputs / preserved_name
            shutil.copy2(p, copied)
            if tarfile.is_tarfile(p):
                extract_root = raw_extracted / preserved_name
                member_count = 0
                total_bytes = 0
                with tarfile.open(p, "r:*") as tar:
                    for member in tar.getmembers():
                        if not member.isfile():
                            continue
                        member_count += 1
                        total_bytes = budget.check(p, member, member_count, total_bytes)
                        stream = tar.extractfile(member)
                        if stream is None:
                            continue
                        target = safe_member_path(member.name, extract_root)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with open(target, "wb") as out:
                            shutil.copyfileobj(stream, out)


def write_inputs_doc(run_dir, manifest):
    lines = [
        "# Trace Triage Inputs",
        "",
        f"- case_name: `{manifest.get('case_name', '')}`",
        f"- scenario: `{manifest.get('scenario', '')}`",
        f"- code_ref: `{manifest.get('code_ref', '')}`",
        f"- analysis_created_at: `{manifest.get('analysis_created_at', '')}`",
        "",
        "## Input Packages",
        "",
    ]
    for index, item in enumerate(manifest.get("inputs", []), 1):
        source = item.get("path", "")
        name = Path(source).name or f"input-{index}"
        preserved_name = item.get("preserved_name") or preserved_input_name(index, source)
        lines.extend([
            f"### {index}. `{name}`",
            "",
            f"- source_path: `{source}`",
            f"- size_bytes: {item.get('size', 0)}",
            f"- sha256: `{item.get('sha256', '')}`",
        ])
        raw_copy = Path("raw") / "inputs" / preserved_name
        if (Path(run_dir) / raw_copy).exists():
            lines.append(f"- preserved_raw: `{raw_copy.as_posix()}`")
        members = item.get("members", [])
        if members:
            extract_root = Path("raw") / "extracted" / preserved_name
            lines.append(f"- extracted_root: `{extract_root.as_posix()}`")
            lines.append("- members:")
            for member in members:
                lines.append(f"  - `{member}`")
        else:
            lines.append("- members: none")
        lines.append("")
    (Path(run_dir) / "inputs.md").write_text("\n".join(lines), encoding="utf-8")


class TraceInputInventory:
    """Own input identities and bounded raw preservation."""

    def __init__(self, budget=None):
        self.budget = budget or TarBudget()

    identity = staticmethod(input_identity)
    write_document = staticmethod(write_inputs_doc)

    def preserve(self, inputs, run_dir):
        preserve_raw_inputs(inputs, run_dir, budget=self.budget)
