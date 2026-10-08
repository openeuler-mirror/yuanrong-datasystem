"""In-process stages with explicit artifact paths and no command-line state."""
from __future__ import annotations

import os
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from . import bottleneck, numa, suite, triage, write_report
from .evidence.normalized import build_evidence, load_evidence, summary_digest
from .analysis.write_pipeline import build_write_model
from .analysis.issue_model import build_issue_model
from .orchestration.contracts import validate_run_booleans


@dataclass(frozen=True)
class StageResult:
    artifacts: Mapping[str, Path]

    def __post_init__(self):
        object.__setattr__(self, "artifacts", MappingProxyType(dict(self.artifacts)))


@dataclass(frozen=True)
class NumaEvidenceSpec:
    run_dir: Path
    evidence_path: Path
    archive: Path
    config: Mapping
    manifest: Mapping
    run_root: Path
    force: bool = False
    models_only: bool = False


def run_triage(config, manifest, run_root, force=False, *, models_only=False):
    controls = validate_run_booleans(config)
    inputs = [Path(item) for item in config.get("inputs", [])]
    if not inputs:
        raise ValueError(f"run {config['id']} has no inputs")
    missing = [str(item) for item in inputs if not item.exists()]
    if missing:
        raise ValueError(f"run {config['id']} missing inputs: {missing}")
    options = triage.RunOptions(
        case_name=config.get("case", config["id"]), scenario=config.get("scenario", ""),
        code_ref=manifest.get("source_head") or manifest.get("source_ref"),
        force=force, allow_partial_inputs=controls.allow_partial_inputs, run_id=config["id"],
    )
    runner = triage.TraceRunPipeline()
    if models_only:
        run_dir = runner.parse(inputs, run_root / "triage", options=options)
        if not (run_dir / "summary.json").is_file():
            runner.aggregate(run_dir)
        if not (run_dir / "triage.json").is_file():
            runner.triage(run_dir)
    else:
        run_dir = runner.run(inputs, run_root / "triage", options=options)
    summary = run_dir / "summary.json"
    if not summary.exists():
        raise RuntimeError(f"triage did not produce {summary}")
    artifacts = {"run_dir": run_dir, "summary_json": summary}
    if not models_only:
        artifacts["report_html"] = run_dir / "report.local.html"
    return StageResult(artifacts)


def _optional_number(value, converter):
    return None if value is None else converter(str(value))


def _write_model(path, model, *, force=False, indent=None):
    if path.exists() and not force:
        raise FileExistsError(f"refusing to overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(model, ensure_ascii=False, indent=indent), encoding="utf-8")
    return StageResult({"analysis_json": path})


def run_evidence(summary_path, run_root):
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    model = build_evidence(summary, summary_digest(summary_path))
    return _write_model(run_root / "evidence.json", model)


def run_read(run_dir, config, manifest, run_root, force=False, *, models_only=False):
    controls = validate_run_booleans(config)
    read_path = config.get("read_path") or None
    if read_path not in (None, "legacy-worker-pull"):
        raise ValueError(f"unsupported read_path: {read_path}")
    analysis_options = {
        "top_n": int(str(config.get("top", 0))),
        "deadline_ms": _optional_number(config.get("deadline_ms"), float),
        "local_cache": controls.local_cache,
        "read_path": read_path,
        "source_ref": manifest.get("source_head") or manifest.get("source_ref"),
    }
    if config.get("evidence_json") is not None:
        analysis_options["evidence_json"] = config["evidence_json"]
    analysis = bottleneck.build_analysis(run_dir, **analysis_options)
    output, model = run_root / "bottleneck.local.html", run_root / "bottleneck.analysis.json"
    if models_only:
        return _write_model(model, analysis, force=force)
    title = f"{analysis['metadata'].get('case') or 'DataSystem'} · Top{analysis['trace_count']} 关键瓶颈"
    bottleneck.write_outputs(analysis, output, title=title, force=force, analysis_json=model,
                             source_run_dir=run_dir, write_companion=False)
    return StageResult({"report_html": output, "analysis_json": model})


def run_write(analysis_json, run_dir, run_root, *, models_only=False):
    if models_only:
        analysis = json.loads(analysis_json.read_text(encoding="utf-8"))
        return _write_model(run_root / "write.refined.analysis.json", write_report.build_model(analysis))
    output = run_root / "bottleneck.write.html"
    triage_link = os.path.relpath(run_dir / "report.local.html", run_root).replace(os.sep, "/")
    links = [("读取瓶颈", os.path.relpath(analysis_json.with_name("bottleneck.local.html"), run_root)),
             ("Trace Triage", triage_link)]
    if output.parent.resolve() == analysis_json.parent.resolve():
        write_report.write_outputs(analysis_json, output, links=links)
    else:
        analysis = json.loads(analysis_json.read_text(encoding="utf-8"))
        page, model = write_report.render_html(analysis, "写入瓶颈分析", links)
        run_root.mkdir(parents=True, exist_ok=True)
        output.write_text(page, encoding="utf-8")
        (run_root / "write.refined.analysis.json").write_text(
            json.dumps(model, ensure_ascii=False), encoding="utf-8")
    return StageResult({"report_html": output, "analysis_json": run_root / "write.refined.analysis.json"})


def run_write_from_evidence(summary_path, evidence_path, manifest_path, config, run_root):
    controls = validate_run_booleans(config)
    read_path = config.get("read_path") or None
    if read_path not in (None, "legacy-worker-pull"):
        raise ValueError(f"unsupported read_path: {read_path}")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    evidence = load_evidence(evidence_path)
    if evidence.get("summary_sha256") != summary_digest(summary_path):
        raise ValueError("write Evidence does not match Triage summary")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    model = build_write_model(summary, evidence, manifest, controls.local_cache, read_path)
    return _write_model(run_root / "write.refined.analysis.json", model)


def run_issues(run_id, evidence_path, read_path, write_path, run_root):
    sources = [json.loads(path.read_text(encoding="utf-8"))
               for path in (evidence_path, read_path, write_path)]
    model = build_issue_model(run_id, *sources)
    return _write_model(run_root / "issues.analysis.json", model)


def _numa_options(config, manifest):
    source = {"head": manifest.get("source_head") or manifest.get("source_ref"),
              "base": manifest.get("source_base"), "pr": _optional_number(manifest.get("pr"), int)}
    runtime = {key: _optional_number(config.get(key), float if key == "qps_per_node" else int)
               for key in ("qps_per_node", "client_count", "threads_per_client", "workers_per_node")}
    return source, runtime


def _numa_outputs(analysis, run_root, force, models_only):
    output, model = run_root / "numa.local.html", run_root / "numa.analysis.json"
    if models_only:
        return _write_model(model, analysis, force=force)
    numa.write_outputs(analysis, output, model, force=force)
    return StageResult({"report_html": output, "analysis_json": model})


def run_numa(run_dir, analysis_json, archive, config, manifest, run_root, force=False, *, models_only=False):
    source, runtime = _numa_options(config, manifest)
    analysis = numa.build_analysis(run_dir, analysis_json, archive, source, runtime)
    return _numa_outputs(analysis, run_root, force, models_only)


def run_numa_from_evidence(spec: NumaEvidenceSpec):
    source, runtime = _numa_options(spec.config, spec.manifest)
    analysis = numa.build_analysis(spec.run_dir, None, spec.archive, source, runtime,
                                   evidence_path=spec.evidence_path)
    return _numa_outputs(analysis, spec.run_root, spec.force, spec.models_only)


def run_suite(manifest_path, output_root, force=False):
    analysis = suite.build_suite(suite.load_manifest(manifest_path))
    output, model = output_root / "index.html", output_root / "suite.analysis.json"
    suite.write_outputs(analysis, output, model, force=force)
    return StageResult({"report_html": output, "analysis_json": model})
