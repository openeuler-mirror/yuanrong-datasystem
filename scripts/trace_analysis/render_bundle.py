"""Regenerate a completed offline bundle from its validated intermediate models."""
import json
from pathlib import Path

from .resources import echarts_path
from .navigation import link_reports


def _load_validated_model(path, kind, output):
    from .validation import validate_data
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        checked = validate_data(data, kind, path)
    except (OSError, json.JSONDecodeError) as error:
        checked = validate_data(None, kind, path)
        checked["errors"] = [f"unreadable JSON: {type(error).__name__}"]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(checked, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if not checked["valid"]:
        raise RuntimeError(f"{kind} model validation failed: {checked.get('errors', [])}")
    return data


def render_run(config, root):
    from . import triage as triage
    from . import bottleneck as read
    from . import write_report as write
    from . import numa as numa
    from .validation import validate_write_data
    import os

    artifact_keys = (
        "triage_report", "bottleneck_report", "write_bottleneck_report", "numa_report",
        "analysis_json", "numa_analysis_json",
    )
    paths = {key: root / config[key] for key in artifact_keys}
    if any(not path.resolve().is_relative_to(root.resolve()) for path in paths.values()):
        raise ValueError("cached report artifacts must remain inside output root")
    if not (root / config["write_analysis_json"]).resolve().is_relative_to(root.resolve()):
        raise ValueError("cached write artifact must remain inside output root")
    if (root / "publication.json").exists():
        raise ValueError("sealed publications are immutable; render a new generation")
    run_dir = paths["triage_report"].parent
    report = _load_validated_model(run_dir / "summary.json", "triage",
                                   paths["analysis_json"].parent / "triage.validation.json")
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    title = f"Trace Triage: {manifest.get('case_name', 'trace-case')}"
    paths["triage_report"].write_text(
        triage.TraceReportRenderer().html(report, title, manifest=manifest), encoding="utf-8")
    del report
    analysis = _load_validated_model(paths["analysis_json"], "bottleneck",
                                     paths["analysis_json"].parent / "bottleneck.validation.json")
    label = config.get("label") or config["id"]
    paths["bottleneck_report"].write_text(
        read.render_html(analysis, label + " · 读取分析", view_top=config.get("view", {}).get("read_top", 0)),
        encoding="utf-8")
    links = []
    for name, key in (("读取分析", "bottleneck_report"), ("Trace Triage", "triage_report"), ("NUMA分析", "numa_report")):
        links.append((name, os.path.relpath(paths[key], paths["write_bottleneck_report"].parent)))
    refined = root / config["write_analysis_json"]
    if refined.resolve() == paths["analysis_json"].resolve():
        raise ValueError("render-only requires an independent refined write model")
    model = json.loads(refined.read_text(encoding="utf-8"))
    validate_write_data(model, analysis, require_phase_schema=True)
    page = write.render_model(model, label + " · 写入分析", links,
                              analysis.get("metadata", {}).get("raw_input_archives", []))
    paths["write_bottleneck_report"].write_text(page, encoding="utf-8")
    del page, model, analysis
    analysis = _load_validated_model(paths["numa_analysis_json"], "numa",
                                     paths["analysis_json"].parent / "numa.validation.json")
    echarts = (echarts_path()).read_text()
    paths["numa_report"].write_text(numa.render_html(analysis, echarts), encoding="utf-8")
    del analysis
    link_reports({"triage": paths["triage_report"], "read": paths["bottleneck_report"],
                  "write": paths["write_bottleneck_report"], "numa": paths["numa_report"]})
    return {"id": config["id"], "valid": True}



def validate_targets(manifest, root):
    owners = {}
    ids = set()
    for config in manifest.get("runs", []):
        run_id = config.get("id")
        if not isinstance(run_id, str) or run_id in {"", ".", ".."}:
            raise ValueError("render-only requires unique Run directory names")
        if any(c in run_id for c in "/\\:") or run_id in ids:
            raise ValueError("render-only requires unique Run directory names")
        ids.add(run_id)
        if "issues_analysis_json" in config and "evidence_json" not in config:
            raise ValueError(f"Run {run_id} requires Evidence for issue validation")
        required = (
            "triage_report", "bottleneck_report", "write_bottleneck_report", "numa_report",
            "triage_json", "analysis_json", "write_analysis_json", "numa_analysis_json",
        ) + tuple(key for key in ("evidence_json", "issues_analysis_json") if key in config)
        for key in required:
            value = config.get(key)
            if key == "triage_json" and key not in config and isinstance(config.get("triage_report"), str):
                value = str(Path(config["triage_report"]).parent / "summary.json")
            if not isinstance(value, str) or not value:
                raise ValueError(f"Run {run_id} requires artifact path: {key}")
            path = (root / value).resolve()
            if not path.is_relative_to(root.resolve()) or path in owners:
                raise ValueError("render-only artifacts must be independent and inside the output root")
            owners[path] = run_id
    if not ids:
        raise ValueError("render-only requires at least one Run")


def _render_bundle(root, jobs=1):
    from . import stages
    from .delivery_validation import validate_suite
    from .execution_budget import ResourceBudget, resolve_execution
    from .resources import tool_fingerprint
    from .orchestration.publication import current_publication
    from .orchestration.bundle import build_publication, publication_key, published_runs
    root = Path(root).resolve()
    current = current_publication(root)
    source_root = current["directory"] if current else root
    manifest_path = current["manifest"] if current else root / "suite.manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for config in manifest.get("runs", []):
        if "triage_json" not in config and isinstance(config.get("triage_report"), str):
            config["triage_json"] = str(Path(config["triage_report"]).parent / "summary.json")
    validate_targets(manifest, source_root)
    execution = manifest.get("execution") or {}
    estimates = execution.get("stage_estimates_mb", {})
    jobs, memory_mb, resource_decision = resolve_execution(
        jobs, execution.get("memory_mb"), estimates, ("render", "suite"))
    budget = ResourceBudget(jobs, memory_mb)
    for stage in ("render", "suite"):
        with budget.acquire(estimates.get(stage)):
            pass
    from .validation import validate, validate_write_model
    from .analysis.issue_model import validate_issue_model
    with budget.acquire(estimates.get("render")):
        for config in manifest["runs"]:
            for field, kind in (("triage_json", "triage"), ("analysis_json", "bottleneck"),
                                ("numa_analysis_json", "numa")):
                checked = validate(source_root / config[field], kind)
                if not checked["valid"]:
                    raise RuntimeError(f"{kind} model validation failed: {checked.get('errors', [])}")
            validate_write_model(source_root / config["write_analysis_json"], source_root / config["analysis_json"],
                                 require_phase_schema=True)
            if "issues_analysis_json" in config:
                sources = []
                for field in ("issues_analysis_json", "evidence_json", "analysis_json", "write_analysis_json"):
                    sources.append(json.loads((source_root / config[field]).read_text(encoding="utf-8")))
                checked = validate_issue_model(*sources)
                if not checked["valid"]:
                    raise RuntimeError(f"issue model validation failed: {checked['errors']}")
            if not isinstance(config.get("input_archive"), str) or not config["input_archive"]:
                raise ValueError(f"Run {config['id']} requires artifact path: input_archive")
        key = publication_key(manifest, source_root, tool_fingerprint())
    generation = build_publication(
        manifest, source_root, root, key=key, jobs=jobs, budget=budget, estimates=estimates,
        render_run=render_run, run_suite=stages.run_suite, validate_suite=validate_suite)
    return {"valid": True, "mode": "cached_models", "index": str(generation["index"]),
            "execution": {"slots": jobs, "memory_mb": memory_mb, **resource_decision},
            "manifest": str(generation["manifest"]), "stable_index": str(root / "index.html"),
            "runs": published_runs(generation, root)}


def _write_render_diagnostic(root, diagnostic):
    pending = root / "render.validation.json.tmp"
    try:
        pending.write_text(json.dumps(diagnostic, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        pending.replace(root / "render.validation.json")
    finally:
        pending.unlink(missing_ok=True)


def render_bundle(root, jobs=1):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    pending = {"valid": False, "mode": "cached_models", "status": "running"}
    _write_render_diagnostic(root, pending)
    try:
        result = _render_bundle(root, jobs)
    except BaseException as error:
        _write_render_diagnostic(root, {**pending, "status": "failed",
                                      "errors": [f"{type(error).__name__}: {error}"]})
        raise
    _write_render_diagnostic(root, {**result, "status": "complete"})
    return result
