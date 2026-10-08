#!/usr/bin/env python3
"""Orchestrate validated triage, read/write bottleneck, NUMA, and suite reports."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import hashlib
import tarfile
from time import perf_counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .resources import tool_fingerprint
from . import stages
from .orchestration.contracts import validate_run_booleans
from .execution_budget import ResourceBudget, parse_jobs, resolve_execution
from .delivery_validation import validate_suite
from .cached_stages import CachedStages, file_inputs, restore_triage, restore_model
from .navigation import link_reports
from .ingest.inventory import TarBudget
from .validation import (validate_model_file as validate, validate_triage_bundle,
                         validate_write_evidence_model, validate_evidence_file,
                         validate_issue_file)


def relative(path: Path, root: Path) -> str:
    return os.path.relpath(path, root).replace(os.sep, "/")


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def input_fingerprint(config, manifest, tool_hash):
    inputs = []
    for raw in sorted(set(config.get("inputs", []) + [config.get("input_archive", "")])):
        if not raw:
            continue
        path = Path(raw)
        files = sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else [path]
        inputs.extend((str(p), file_hash(p)) for p in files)
    source_head = manifest.get("source_head") or manifest.get("source_ref")
    data = {"inputs": inputs, "config": config, "source_head": source_head,
            "source_base": manifest.get("source_base"), "pr": manifest.get("pr"), "tool": tool_hash}
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def artifact_hashes(config, root):
    return {value: file_hash(root / value) for key, value in config.items()
            if value and (key.endswith("_report") or key.endswith("_json"))}


def _preserve_archive(source, output_root, run_id):
    digest = file_hash(source)
    target = output_root / "inputs" / run_id / digest / source.name
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_file() and file_hash(target) == digest:
        return target
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".archive-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
        shutil.copy2(source, temporary)
        if file_hash(temporary) != digest:
            raise ValueError(f"input archive changed while preserving: {source}")
        os.replace(temporary, target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return target


def _read_view(config):
    view = config.get("view", {})
    if not isinstance(view, dict):
        raise ValueError("Run view must be an object with read_top")

    def selection(value):
        if isinstance(value, int) and not isinstance(value, bool) and value in (0, 100, 1000):
            return value
        if isinstance(value, str) and value in ("0", "100", "1000"):
            return int(value)
        raise ValueError("pipeline top/read_top must be 0, 100 or 1000; it selects a view, not an input limit")
    legacy = selection(config["top"]) if "top" in config else None
    selected = selection(view.get("read_top", legacy if legacy is not None else 0))
    if legacy is not None and legacy != selected:
        raise ValueError("pipeline top conflicts with view.read_top")
    return {**view, "read_top": selected}


def run_one(config, manifest, output_root, force, resume, tool_hash, budget=None, stage_estimates_mb=None):
    budget = budget if budget is not None else ResourceBudget(1)
    stage_estimates_mb = dict(stage_estimates_mb or {})
    cfg = config
    view = _read_view(cfg)
    checkpoint = output_root / "runs" / cfg["id"] / "pipeline.checkpoint.json"
    fingerprint = input_fingerprint(cfg, manifest, tool_hash)
    reusable_result = None
    if resume and not force and checkpoint.is_file():
        try:
            cached = json.loads(checkpoint.read_text())
        except (OSError, json.JSONDecodeError):
            cached = {}
        if not isinstance(cached, dict):
            cached = {}
        if cached.get("fingerprint") == fingerprint:
            try:
                if artifact_hashes(cached["result"]["run"], output_root) == cached["artifacts"]:
                    reusable_result = cached["result"]
            except (OSError, KeyError, TypeError, AttributeError):
                pass
    run_id = str(cfg["id"])
    run_root = output_root / "runs" / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    inputs = [Path(item) for item in cfg.get("inputs", [])]
    if not inputs:
        raise ValueError(f"run {run_id} has no inputs")
    if not all(item.exists() for item in inputs):
        missing = [str(item) for item in inputs if not item.exists()]
        raise ValueError(f"run {run_id} missing inputs: {missing}")
    cache = CachedStages(output_root, run_id, resume and not force, budget, stage_estimates_mb, tool_hash)
    parse_cfg = {key: cfg.get(key) for key in ("inputs", "case", "scenario", "allow_partial_inputs")}
    parse_source = {"source_head": manifest.get("source_head") or manifest.get("source_ref")}
    parse_inputs = input_fingerprint(parse_cfg, parse_source, "inputs")
    run_validation = {}

    def restore_validation(kind, report):
        run_validation[kind] = report
        pending = run_root / (kind + ".validation.json.tmp")
        try:
            pending.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            pending.replace(run_root / (kind + ".validation.json"))
        finally:
            pending.unlink(missing_ok=True)
        return report

    def check_triage(result):
        run_validation["triage"] = validate_triage_bundle(result.artifacts["summary_json"],
                                                            run_root / "triage.validation.json")
        return run_validation["triage"]

    triage_result = cache.run("triage", parse_inputs, parse_cfg,
                             lambda target: stages.run_triage(cfg, manifest, target, force or resume, models_only=True),
                             check_triage, restore_triage, generation=True,
                             restore_validation=lambda report: restore_validation("triage", report))
    run_dir = triage_result.artifacts["run_dir"]
    summary = triage_result.artifacts["summary_json"]

    def check_evidence(result):
        run_validation["evidence"] = validate_evidence_file(
            result.artifacts["analysis_json"], summary, run_root / "evidence.validation.json")
        return run_validation["evidence"]

    evidence_result = cache.run(
        "evidence", file_inputs(summary), {},
        lambda target: stages.run_evidence(summary, target), check_evidence,
        restore_model, generation=True,
        restore_validation=lambda report: restore_validation("evidence", report))
    evidence_json = evidence_result.artifacts["analysis_json"]

    def check_read(result):
        run_validation["bottleneck"] = validate(result.artifacts["analysis_json"], "bottleneck",
                                                 run_root / "bottleneck.validation.json")
        return run_validation["bottleneck"]

    read_cfg = {key: cfg.get(key) for key in ("deadline_ms", "local_cache", "read_path")}
    read_cfg["source_ref"] = manifest.get("source_head") or manifest.get("source_ref")

    def run_read_stage():
        return cache.run(
            "read", file_inputs(summary, run_dir / "manifest.json", run_dir / "triage.json", evidence_json), read_cfg,
            lambda target: stages.run_read(run_dir, {**cfg, "top": 0, "evidence_json": evidence_json},
                                           manifest, target, force or resume, models_only=True),
            check_read,
            restore_model, generation=True,
            restore_validation=lambda report: restore_validation("bottleneck", report))

    def check_write(result):
        checked = validate_write_evidence_model(result.artifacts["analysis_json"], summary, evidence_json)
        return restore_validation("write", checked)

    if not cfg.get("input_archive"):
        raise ValueError(f"run {run_id} requires input_archive for archive-level provenance")
    archive = Path(cfg["input_archive"])
    if not archive.exists():
        raise ValueError(f"run {run_id} input_archive not found: {archive}")
    archive_copy = _preserve_archive(archive, output_root, run_id)

    def check_numa(result):
        run_validation["numa"] = validate(result.artifacts["analysis_json"], "numa",
                                           run_root / "numa.validation.json")
        return run_validation["numa"]

    numa_fields = ("qps_per_node", "client_count", "threads_per_client", "workers_per_node")
    numa_cfg = {key: cfg.get(key) for key in numa_fields}
    numa_cfg["source"] = {key: manifest.get(key) for key in ("source_head", "source_ref", "source_base", "pr")}

    def run_write_stage():
        write_cfg = {key: cfg.get(key) for key in ("local_cache", "read_path")}
        return cache.run(
            "write", file_inputs(summary, evidence_json, run_dir / "manifest.json"), write_cfg,
            lambda target: stages.run_write_from_evidence(
                summary, evidence_json, run_dir / "manifest.json", cfg, target), check_write,
            restore_model, generation=True,
            restore_validation=lambda report: restore_validation("write", report))

    def run_numa_stage():
        return cache.run(
            "numa", lambda: file_inputs(summary, evidence_json, run_dir / "manifest.json", archive), numa_cfg,
            lambda target: stages.run_numa_from_evidence(stages.NumaEvidenceSpec(
                run_dir, evidence_json, archive, cfg, manifest, target, force or resume, models_only=True)),
            check_numa, restore_model, generation=True,
            restore_validation=lambda report: restore_validation("numa", report))

    with ThreadPoolExecutor(max_workers=3) as branches:
        read_future = branches.submit(run_read_stage)
        write_future = branches.submit(run_write_stage)
        numa_future = branches.submit(run_numa_stage)
        read_result = read_future.result()
        bottleneck_json = read_result.artifacts["analysis_json"]
        write_result = write_future.result()
        numa_result = numa_future.result()
    write_json = write_result.artifacts["analysis_json"]
    numa_json = numa_result.artifacts["analysis_json"]

    def check_issues(result):
        run_validation["issues"] = validate_issue_file(
            result.artifacts["analysis_json"], evidence_json, bottleneck_json, write_json,
            run_root / "issues.validation.json")
        return run_validation["issues"]

    issues_result = cache.run(
        "issues", file_inputs(evidence_json, bottleneck_json, write_json), {},
        lambda target: stages.run_issues(run_id, evidence_json, bottleneck_json, write_json, target),
        check_issues, restore_model, generation=True,
        restore_validation=lambda report: restore_validation("issues", report))
    issues_json = issues_result.artifacts["analysis_json"]
    run_validation["cache"] = cache.diagnostics

    metadata_keys = (
        "id", "label", "implementation", "local_cache", "placement", "read_path",
        "size", "load", "client_shape", "case_study_only", "sampling_cap_per_band",
    )
    provenance = {}
    for stage, result in (("triage", triage_result), ("evidence", evidence_result), ("read", read_result),
                          ("write", write_result), ("numa", numa_result), ("issues", issues_result)):
        if "provenance_json" in result.artifacts:
            provenance[stage] = relative(result.artifacts["provenance_json"], output_root)
    generated = {
        **{key: cfg.get(key) for key in metadata_keys},
        "view": view,
        "input_archive": relative(archive_copy, output_root),
        "analysis_json": relative(bottleneck_json, output_root),
        "evidence_json": relative(evidence_json, output_root),
        "write_analysis_json": relative(write_json, output_root),
        "triage_json": relative(summary, output_root),
        "numa_analysis_json": relative(numa_json, output_root),
        "issues_analysis_json": relative(issues_json, output_root),
        "stage_provenance": provenance,
    }
    if (reusable_result and reusable_result.get("run") == generated
            and all(item["status"] == "hit" for item in cache.diagnostics.values())):
        return {"run": reusable_result["run"], "validation": run_validation}
    result = {"run": generated, "validation": run_validation}
    checkpoint_data = {"fingerprint": fingerprint, "result": result,
                       "artifacts": artifact_hashes(generated, output_root)}
    checkpoint.write_text(json.dumps(checkpoint_data, ensure_ascii=False), encoding="utf-8")
    return result


def _validate_archive(path: Path, run_id: str, field: str, checked: set[Path]) -> None:
    if path in checked:
        return
    try:
        with tarfile.open(path, "r:*") as archive:
            count = total_bytes = 0
            budget = TarBudget()
            for member in archive:
                if member.isfile():
                    count += 1
                    if field == "inputs":
                        total_bytes = budget.check(path, member, count, total_bytes)
            if not count:
                raise ValueError(f"run {run_id} {field} has no files: {path}")
    except (OSError, tarfile.TarError) as error:
        raise ValueError(f"run {run_id} {field} is not a readable tar archive: {path}") from error
    checked.add(path)


def preflight_pipeline(manifest_path: Path) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1:
        raise ValueError("pipeline manifest schema_version must be 1")
    runs = manifest.get("runs")
    if not isinstance(runs, list) or not runs:
        raise ValueError("pipeline manifest must contain runs")
    if not all(isinstance(cfg, dict) for cfg in runs):
        raise ValueError("pipeline runs must contain objects")
    ids = [str(cfg.get("id", "")) for cfg in runs]
    if not all(ids) or len(ids) != len(set(ids)):
        raise ValueError("pipeline runs require unique non-empty id values")
    pr = manifest.get("pr")
    numeric_pr = type(pr) is int and pr >= 0
    decimal_pr = isinstance(pr, str) and pr.isdecimal()
    if pr is not None and not (numeric_pr or decimal_pr):
        raise ValueError("manifest pr must be a non-negative integer or decimal string")
    sampling = manifest.get("sampling")
    if sampling is not None:
        if not isinstance(sampling, dict):
            raise ValueError("manifest sampling must be an object")
        cap = sampling.get("max_per_band")
        if cap is not None and (type(cap) is not int or cap < 0):
            raise ValueError("manifest sampling.max_per_band must be a non-negative integer")
    source_head = manifest.get("source_head") or manifest.get("source_ref")
    source_base = manifest.get("source_base")
    if not source_head or not source_base:
        raise ValueError("pipeline manifest requires source_head and source_base")
    for cfg in runs:
        validate_run_booleans(cfg)
        _read_view(cfg)
        run_id = str(cfg.get("id", ""))
        if run_id in {"", ".", ".."} or any(c in run_id for c in "/\\:"):
            raise ValueError("Run id must be a single directory name")
        inputs = cfg.get("inputs")
        if not isinstance(inputs, list) or not inputs or any(not isinstance(item, str) or not item for item in inputs):
            raise ValueError(f"run {run_id} inputs must be a non-empty list of directory or tar archive paths")
        archive_value = cfg.get("input_archive")
        if not isinstance(archive_value, str) or not archive_value:
            raise ValueError(f"run {run_id} input_archive must be a tar archive path")
        for key in ("inputs", "input_archive"):
            values = cfg[key]

            def absolute(value):
                path = Path(value)
                return str((manifest_path.parent / path).resolve()) if not path.is_absolute() else str(path)
            cfg[key] = [absolute(v) for v in values] if key == "inputs" else absolute(values)
    checked_archives: set[Path] = set()
    for cfg in runs:
        run_id = cfg["id"]
        for raw in cfg["inputs"]:
            path = Path(raw)
            if not path.exists():
                raise ValueError(f"run {run_id} inputs not found: {path}")
            if path.is_dir():
                continue
            if not path.is_file():
                raise ValueError(f"run {run_id} inputs must be a directory or tar archive: {path}")
            if path.suffix not in (".tar", ".gz", ".tgz"):
                raise ValueError(f"run {run_id} inputs must be a directory or tar archive: {path}")
            _validate_archive(path, run_id, "inputs", checked_archives)
        archive = Path(cfg["input_archive"])
        if not archive.is_file():
            raise ValueError(f"run {run_id} input_archive not found: {archive}")
        _validate_archive(archive, run_id, "input_archive", checked_archives)
    return manifest


def run_pipeline(
    manifest_path: Path, output_root: Path, force: bool, jobs: int | str = 1, resume: bool = False,
    memory_mb=None, stage_estimates_mb=None, run_executor=None,
) -> dict[str, Any]:
    started = perf_counter()
    manifest = preflight_pipeline(manifest_path)
    preflight_wall_seconds = round(perf_counter() - started, 6)
    runs = manifest["runs"]
    source_head = manifest.get("source_head") or manifest.get("source_ref")
    execution = manifest.get("execution") or {}
    if not isinstance(execution, dict):
        raise ValueError("manifest execution must be an object")
    memory_mb = memory_mb if memory_mb is not None else execution.get("memory_mb")
    estimates = stage_estimates_mb if stage_estimates_mb is not None else execution.get("stage_estimates_mb", {})
    if not isinstance(estimates, dict):
        raise ValueError("stage_estimates_mb must map stages to declared memory estimates")
    if memory_mb is not None and "evidence" not in estimates and "read" in estimates:
        estimates = {**estimates, "evidence": estimates.get("read")}
    if memory_mb is not None and "issues" not in estimates and "read" in estimates:
        estimates = {**estimates, "issues": estimates.get("read")}
    run_executor = run_executor or execution.get("run_executor", "thread")
    if run_executor not in ("thread", "process"):
        raise ValueError("run_executor must be thread or process")
    required_stages = ("triage", "evidence", "read", "write", "numa", "issues", "render", "suite")
    jobs, memory_mb, resource_decision = resolve_execution(jobs, memory_mb, estimates, required_stages)
    budget = ResourceBudget(jobs, memory_mb)
    for stage in required_stages:
        with budget.acquire(estimates.get(stage)):
            pass
    output_root.mkdir(parents=True, exist_ok=True)
    run_workers = min(jobs, len(runs))
    if run_executor == "process" and memory_mb is not None:
        maximum = max(estimates.get(stage) for stage in ("triage", "evidence", "read", "write", "numa", "issues"))
        run_workers = min(run_workers, max(1, int(memory_mb // maximum)))
    generated: list[dict[str, Any]] = []
    validation: dict[str, Any] = {
        "schema_version": 1, "runs": {}, "suite": {},
        "execution": {"slots": jobs, "memory_mb": memory_mb, **resource_decision,
                      "run_executor": run_executor, "run_workers": run_workers,
                      "memory_mode": "declared_estimates" if memory_mb is not None else "not_limited",
                      "stage_estimates_mb": dict(estimates),
                      "preflight_wall_seconds": preflight_wall_seconds},
    }

    tool_hash = tool_fingerprint()
    validation["valid"] = False
    (output_root / "pipeline.validation.json").write_text(json.dumps(validation), encoding="utf-8")
    failures = {}
    completed = {}
    executor = ProcessPoolExecutor if run_executor == "process" else ThreadPoolExecutor
    with executor(max_workers=run_workers) as pool:
        tasks = {}
        for cfg in runs:
            worker_budget = None if run_executor == "process" else budget
            future = pool.submit(run_one, cfg, manifest, output_root, force, resume,
                                 tool_hash, worker_budget, estimates)
            tasks[future] = cfg["id"]
        for task in as_completed(tasks):
            run_id = tasks[task]
            try:
                result = task.result()
                completed[run_id] = result["run"]
                validation["runs"][run_id] = result["validation"]
            except Exception as error:
                failures[run_id] = str(error)
                validation["runs"][run_id] = {"valid": False, "error": str(error)}
            validation["valid"] = False
            (output_root / "pipeline.validation.json").write_text(
                json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8"
            )
    if failures:
        validation["execution"]["total_wall_seconds"] = round(perf_counter() - started, 6)
        (output_root / "pipeline.validation.json").write_text(
            json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError("failed Runs: " + ", ".join(failures))
    generated = [completed[cfg["id"]] for cfg in runs]

    suite_manifest = {key: value for key, value in manifest.items() if key != "runs"}
    suite_manifest.update({"schema_version": 1, "source_ref": source_head, "runs": generated})
    from .render_bundle import render_run
    from .orchestration.bundle import build_publication, publication_key, published_runs
    key = publication_key(suite_manifest, output_root, tool_hash)
    suite_started = perf_counter()
    try:
        publication = build_publication(
            suite_manifest, output_root, output_root, key=key, jobs=jobs, budget=budget, estimates=estimates,
            render_run=render_run, run_suite=stages.run_suite, validate_suite=validate_suite, reuse=resume)
    except Exception as error:
        validation["suite"] = {"valid": False, "errors": [str(error)]}
        validation["execution"]["suite_wall_seconds"] = round(perf_counter() - suite_started, 6)
        validation["execution"]["total_wall_seconds"] = round(perf_counter() - started, 6)
        (output_root / "pipeline.validation.json").write_text(
            json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
        raise
    checked = json.loads((publication["directory"] / "publication.validation.json").read_text(encoding="utf-8"))
    validation["suite"] = {"analysis": str(publication["directory"] / "suite.analysis.json"),
                           "html": str(publication["index"]), **checked}
    validation["valid"] = checked["valid"]
    validation["execution"]["suite_wall_seconds"] = round(perf_counter() - suite_started, 6)
    views = published_runs(publication, output_root)
    report_fields = ("triage_report", "bottleneck_report", "write_bottleneck_report", "numa_report")
    delivered = []
    for run, view in zip(generated, views):
        reports = {field: view[field] for field in report_fields}
        delivered.append({**run, **reports})
    validation["execution"]["total_wall_seconds"] = round(perf_counter() - started, 6)
    (output_root / "pipeline.validation.json").write_text(
        json.dumps(validation, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"output": str(output_root), "index": str(publication["index"]),
            "stable_index": str(output_root / "index.html"), "manifest": str(publication["manifest"]),
            "runs": delivered, "validation": validation}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--jobs", type=parse_jobs, default=1,
                        help="Positive stage concurrency or auto (CPU/memory with measured stage estimates)")
    parser.add_argument("--run-executor", choices=("thread", "process"),
                        help="Run-level parallelism; defaults to the manifest setting or thread")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--preflight-only", action="store_true",
                        help="Validate every Run input and archive without creating report output")
    parser.add_argument(
        "--render-only", action="store_true", help="Render cached suite models without parsing input logs"
    )
    args = parser.parse_args()
    if args.preflight_only:
        if args.manifest is None:
            parser.error("--manifest is required for --preflight-only")
        manifest = preflight_pipeline(args.manifest.resolve())
        result = {"valid": True, "run_count": len(manifest["runs"]),
                  "runs": [cfg["id"] for cfg in manifest["runs"]]}
    elif args.render_only:
        if args.output is None:
            parser.error("--output is required for --render-only")
        from .render_bundle import render_bundle
        result = render_bundle(args.output.resolve(), args.jobs)
    else:
        if args.manifest is None:
            parser.error("--manifest is required unless --render-only is used")
        if args.output is None:
            parser.error("--output is required unless --preflight-only is used")
        result = run_pipeline(args.manifest.resolve(), args.output.resolve(), args.force,
                              args.jobs, args.resume, run_executor=args.run_executor)
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
