"""Materialize a self-contained view generation before publishing its entry pointer."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
import tempfile
from pathlib import Path
import shutil

from .publication import new_publication, commit_publication, current_publication
from ..stage_cache import make_stage_key, _file_hash

REPORT_FIELDS = ("triage_report", "bottleneck_report", "write_bottleneck_report", "numa_report")
MODEL_FIELDS = ("triage_json", "evidence_json", "analysis_json", "write_analysis_json",
                "numa_analysis_json", "issues_analysis_json")


def _source_path(root, value):
    path = (root / value).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError(f"source artifact missing or outside bundle: {value}")
    return path


def _copy_run(config, source_root, directory):
    run_root = directory / "runs" / config["id"]
    run_root.mkdir(parents=True)
    triage_source = _source_path(source_root, config["triage_json"]).parent
    triage_target = run_root / "triage" / triage_source.name
    shutil.copytree(triage_source, triage_target)
    excluded_fields = (*MODEL_FIELDS, *REPORT_FIELDS, "input_archive")
    result = {key: value for key, value in config.items() if key not in excluded_fields}
    result["triage_json"] = (triage_target / "summary.json").relative_to(directory).as_posix()
    result["triage_report"] = (triage_target / "report.local.html").relative_to(directory).as_posix()
    names = {"evidence_json": "evidence.json", "analysis_json": "bottleneck.analysis.json",
             "write_analysis_json": "write.refined.analysis.json",
             "numa_analysis_json": "numa.analysis.json", "issues_analysis_json": "issues.analysis.json",
             "bottleneck_report": "bottleneck.local.html",
             "write_bottleneck_report": "bottleneck.write.html", "numa_report": "numa.local.html"}
    for field, name in names.items():
        if field in ("evidence_json", "issues_analysis_json") and field not in config:
            continue
        target = run_root / name
        if field in MODEL_FIELDS:
            shutil.copyfile(_source_path(source_root, config[field]), target)
        result[field] = target.relative_to(directory).as_posix()
    if config.get("stage_provenance"):
        provenance_root = run_root / "provenance"
        provenance_root.mkdir()
        result["stage_provenance"] = {}
        for stage, source in config["stage_provenance"].items():
            if stage not in {"triage", "evidence", "read", "write", "numa", "issues"}:
                raise ValueError("unknown provenance stage")
            target = provenance_root / (stage + ".json")
            shutil.copyfile(_source_path(source_root, source), target)
            result["stage_provenance"][stage] = target.relative_to(directory).as_posix()
    archive = _source_path(source_root, config["input_archive"])
    target_archive = directory / "inputs" / config["id"] / archive.name
    target_archive.parent.mkdir(parents=True)
    shutil.copyfile(archive, target_archive)
    result["input_archive"] = target_archive.relative_to(directory).as_posix()
    model = json.loads((run_root / "bottleneck.analysis.json").read_text(encoding="utf-8"))
    for item in model.get("metadata", {}).get("raw_input_archives", []):
        name = item.get("name", "")
        if not name or Path(name).name != name:
            raise ValueError("invalid raw archive name")
        preserved = triage_target / "raw" / "inputs" / name
        if not preserved.is_file():
            raise ValueError(f"preserved raw archive missing: {name}")
        target = run_root / "raw-inputs" / name
        target.parent.mkdir(exist_ok=True)
        shutil.copyfile(preserved, target)
    return result


def publication_key(manifest, source_root, tool_hash):
    inputs = {}
    for config in manifest["runs"]:
        inputs[config["id"]] = {field: _file_hash(_source_path(source_root, config[field]))
                                 for field in (*MODEL_FIELDS, "input_archive") if field in config}
    return make_stage_key(inputs, manifest, tool_hash)


def build_publication(manifest, source_root, output_root, *, key, jobs, budget, estimates,
                      render_run, run_suite, validate_suite, reuse=False):
    source_root, output_root = Path(source_root).resolve(), Path(output_root).resolve()
    if reuse:
        try:
            current = current_publication(output_root)
        except (OSError, ValueError):
            current = None
        if current is not None and current["key"] == key:
            _compatibility_manifest(current, output_root)
            return current
    directory = new_publication(output_root)

    def render(config):
        with budget.acquire(estimates.get("render")):
            copied = _copy_run(config, source_root, directory)
            render_run(copied, directory)
            return copied
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        copied_runs = list(pool.map(render, manifest["runs"]))
    copied_manifest = {**manifest, "runs": copied_runs}
    manifest_path = directory / "suite.manifest.json"
    manifest_path.write_text(json.dumps(copied_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    with budget.acquire(estimates.get("suite")):
        result = run_suite(manifest_path, directory, False)
        model = json.loads(result.artifacts["analysis_json"].read_text(encoding="utf-8"))
        checked = validate_suite(model, copied_manifest, directory, require_models=True)
        (directory / "publication.validation.json").write_text(
            json.dumps(checked, ensure_ascii=False, indent=2), encoding="utf-8")
        if not checked["valid"]:
            raise RuntimeError("suite validation failed: " + "; ".join(checked["errors"]))
    committed = commit_publication(output_root, directory, key)
    _compatibility_manifest(committed, output_root)
    return committed


def published_runs(publication, output_root):
    prefix = publication["directory"].relative_to(Path(output_root).resolve())
    manifest = json.loads(publication["manifest"].read_text(encoding="utf-8"))
    runs = []
    for config in manifest["runs"]:
        promoted = {**config, **{field: (prefix / config[field]).as_posix()
                                 for field in (*MODEL_FIELDS, *REPORT_FIELDS, "input_archive") if field in config}}
        if config.get("stage_provenance"):
            promoted["stage_provenance"] = {stage: (prefix / path).as_posix()
                                             for stage, path in config["stage_provenance"].items()}
        runs.append(promoted)
    return runs


def _compatibility_manifest(publication, root):
    # This external-CLI snapshot is not the commit point; package readers use index.html.
    temporary = None
    try:
        manifest = json.loads(publication["manifest"].read_text(encoding="utf-8"))
        manifest["runs"] = published_runs(publication, root)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=root,
                                         prefix=".suite-manifest-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(manifest, stream, ensure_ascii=False, indent=2)
        os.replace(temporary, root / "suite.manifest.json")
    except OSError as error:
        publication["compatibility_warning"] = str(error)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
