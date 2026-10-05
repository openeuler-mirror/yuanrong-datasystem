#!/usr/bin/env python3
"""Build a reusable TopN bottleneck report from a ds-trace-triage run directory."""

from __future__ import annotations

import argparse
import collections
import json
import math
import re
import shutil
import sys
from pathlib import Path

from .analysis import read_model as _read_model
from .analysis.read_model import (
    InputContractError, _share_safe_input, _is_write_flow, _aggregate_write,
    prepare_read_view, _read_json,
)
from .analysis import read_rows as _read_rows
from .analysis.read_rows import _topology_contract
from .analysis.aggregation import (
    _aggregate_urma,
    _aggregate_non_transport,
    _build_latency_segments,
    _aggregate_query_meta,
    aggregate,
)
from .analysis.budget import (
    _max_rpc_framework,
    _take_focus_budget,
    _replace_rpc_network_budget,
    _urma_timeout_accounting,
)
from .analysis.contracts import (
    STAGE_NAMES,
    FOCUS_STAGE_NAMES,
    PROBLEM_NAMES,
    NON_TRANSPORT_CATEGORIES,
)
from .analysis.correlation import (
    _worker_roles,
    _worker_event_views,
    _query_worker_event_views,
    _build_worker_correlation,
)
from .evidence.observations import build_evidence_facts
from .evidence.normalized import load_evidence, read_observations, summary_digest
from .evidence.write import WRITE_CLIENT_FLOWS, is_write_flow, resolve_write_facts, rpc_group
from .analysis.read_initial import (
    _access_location,
    _metric_max,
    _max_rpc,
    _max_single_data_rpc,
    _extract_trace,
)
from .analysis.read import (
    _apply_focus_breakdown,
    _query_and_get_breakdown,
    _non_transport_analysis,
    _urma_critical_path,
    _sequential_urma_path,
    _apply_inline_query_urma_attribution,
    _apply_query_rpc_attribution,
    _apply_query_urma_timeout_attribution,
    _max_evidence_ms,
    _query_meta_detail,
    _refine_data_access_scope,
    _apply_explicit_rpc_errors,
)
from .analysis.stats import (
    _percentile,
    _pearson,
    _metric_summary,
    _group_metric,
)
from .analysis.write_base import _build_write_row, _write_rpc_split, _scaled_write_rpc_split
from .analysis.issues import _classify_urma_timeout_detail, _classify_rpc_deadline_detail
from .diagnosis import (
    READ_STAGES,
    WRITE_STAGES,
    worker_log_assessment,
)
from .evidence.rpc import (
    _rpc_summary_windows,
    _analyze_rpc_calls,
    _transport_phase_maps,
    _evidence_timestamp,
    _timestamp_value,
)
from .evidence.urma import (
    SLOW_WR_THRESHOLD_MS,
    _raw_float,
    _urma_timeout_evidence,
    _trace_us,
    _delta_ms,
    _request_from_event,
    _is_slow_wr,
    _group_urma_logical_writes,
    _dedupe_urma_requests,
    observed_urma_requests,
    worker_ip_mapping,
)
from .rendering import read as read_renderer
from .rendering.read import (
    CORRELATION_STYLE,
    WRITE_SECTION,
    WRITE_SCRIPT,
    CORRELATION_SECTION,
    CORRELATION_SCRIPT,
    QUERY_BREAKDOWN_SCRIPT,
    HTML_TEMPLATE,
)


WRITE_STAGE_NAMES = WRITE_STAGES
WRITE_CLIENT_OPERATION_RE = "|".join(
    re.escape(name.removeprefix("DS_KV_CLIENT_")) for name in sorted(WRITE_CLIENT_FLOWS)
)


def _write_rpc_group(evidence: list[str], operation: str) -> list[dict[str, int]]:
    facts = resolve_write_facts({"trace_id": "", "evidence": evidence})
    return rpc_group(facts, operation)


def build_trace_rows(
    summary: dict, local_cache: bool | None = None, read_path: str | None = None,
    evidence_data: dict | None = None,
) -> list[dict]:
    return _read_rows.build_trace_rows(summary, local_cache, read_path, evidence_data)


def build_analysis(
    run_dir: Path,
    top_n: int = 100,
    deadline_ms: float | None = None,
    local_cache: bool | None = None,
    read_path: str | None = None,
    source_ref: str | None = None,
    evidence_json: Path | None = None,
) -> dict:
    return _read_model.build_analysis(
        run_dir, top_n, deadline_ms, local_cache, read_path, source_ref, evidence_json
    )


def render_html(analysis: dict, title: str, write_report: str | None = None, *, view_top: int = 0) -> str:
    """Render one self-contained report from a precomputed analysis model."""

    scopes, topology = prepare_read_view(analysis["traces"], analysis["aggregate"], analysis["metadata"])
    return read_renderer.render_html(
        analysis, title, scope_aggregates=scopes, topology=topology,
        template=HTML_TEMPLATE, contract_error=InputContractError, view_top=view_top,
    )


def write_outputs(
    analysis: dict,
    output: Path,
    *,
    title: str,
    force: bool = False,
    analysis_json: Path | None = None,
    source_run_dir: Path | None = None,
    write_companion: bool = True,
) -> tuple[Path, Path]:
    output = Path(output)
    analysis_json = Path(analysis_json) if analysis_json else output.with_name("bottleneck.analysis.json")
    write_output = (
        output.with_name(output.stem + ".write.html")
        if write_companion and analysis.get("write_traces")
        else None
    )
    targets = [output, analysis_json] + ([write_output] if write_output else [])
    for i, path in enumerate(targets):
        for other in targets[:i]:
            same_file = path.resolve() == other.resolve()
            if not same_file and path.exists() and other.exists():
                same_file = path.samefile(other)
            if same_file:
                raise ValueError("output / analysis_json / write_output paths must be distinct")
    existing = [path for path in targets if path.exists()]
    if existing and not force:
        raise FileExistsError("refusing to overwrite: " + ", ".join(str(path) for path in existing))
    write_page = None
    if write_output:
        from . import write_report as writer
        write_page, _ = writer.render_html(analysis, title + " · 写入", [("读取瓶颈", output.name)])
    output.parent.mkdir(parents=True, exist_ok=True)
    analysis_json.parent.mkdir(parents=True, exist_ok=True)
    if source_run_dir is not None:
        source_run_dir = Path(source_run_dir)
        archive_dir = output.parent / "raw-inputs"
        for item in analysis.get("metadata", {}).get("raw_input_archives", []):
            name = Path(str(item.get("name") or "")).name
            if not name or name != str(item.get("name") or ""):
                continue
            source = source_run_dir / "raw" / "inputs" / name
            if source.is_file():
                archive_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, archive_dir / name)
    output.write_text(render_html(analysis, title, write_output.name if write_output else None), encoding="utf-8")
    if write_output:
        write_output.write_text(write_page, encoding="utf-8")
    analysis_json.write_text(json.dumps(analysis, ensure_ascii=False), encoding="utf-8")
    return output.resolve(), analysis_json.resolve()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument(
        "--top", type=int, default=0, help="Input limit; 0 keeps all traces for browser Top N selection"
    )
    parser.add_argument("--deadline-ms", type=float)
    parser.add_argument(
        "--local-cache",
        choices=("true", "false"),
        help="Access topology supplied by the user/config; omit when unknown",
    )
    parser.add_argument(
        "--source-ref",
        help="Current main/master ref used to verify code-path claims",
    )
    parser.add_argument(
        "--read-path",
        choices=("legacy-worker-pull",),
        help="Explicit historical runtime read topology; omit for current local-cache-derived topology",
    )
    parser.add_argument("--title")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--analysis-json", type=Path)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--skip-write-page", action="store_true", help="Leave independent write rendering to the pipeline"
    )
    args = parser.parse_args(argv)
    local_cache = None if args.local_cache is None else args.local_cache == "true"
    analysis = build_analysis(
        args.run_dir,
        top_n=args.top,
        deadline_ms=args.deadline_ms,
        local_cache=local_cache,
        read_path=args.read_path,
        source_ref=args.source_ref,
    )
    metadata = analysis["metadata"]
    title = args.title or f"{metadata.get('case') or 'DataSystem'} · Top{analysis['trace_count']} 关键瓶颈"
    output, _ = write_outputs(
        analysis,
        args.output,
        title=title,
        force=args.force,
        analysis_json=args.analysis_json,
        source_run_dir=args.run_dir,
        write_companion=not args.skip_write_page,
    )
    sys.stdout.write(f"{output}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
