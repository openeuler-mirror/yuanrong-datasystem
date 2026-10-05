#!/usr/bin/env python3
"""Aggregate independent DataSystem bottleneck runs without reparsing trace logs."""

from __future__ import annotations

import argparse
import html
import json
import math
import re
import tarfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
import sys

from .resources import echarts_path
from .delivery_validation import validate_suite
from .archive import collection_cohort, trace_member_id
from . import overview as ds_trace_overview

SCHEMA_VERSION = 1
BANDS = ("5–7ms", "7–10ms", "10–20ms", ">20ms", "错误采样（无时延桶）")
PROBLEMS = (
    "URMA建链",
    "URMA通信",
    "URMA调度/线程开销",
    "QueryAndGet其他业务",
    "Get其他业务",
    "其他调度/线程开销",
    "调度/线程等待",
    "RPC网络相关",
    "RPC框架",
    "QueryMeta",
    "URMA",
    "远端供数处理",
    "数据访问父窗口/未细分",
    "未解释残差",
    "URMA超时",
)
IMPL_ORDER = {"true": 0, "same": 1, "meta": 2}
BAND_PATTERNS = (
    (re.compile(r"(?:DS_KV_CLIENT_)?GET_5000_7000"), "5–7ms"),
    (re.compile(r"(?:DS_KV_CLIENT_)?GET_700(?:0|1)_10000"), "7–10ms"),
    (re.compile(r"(?:DS_KV_CLIENT_)?GET_1000(?:0|1)_20000"), "10–20ms"),
    (re.compile(r"(?:DS_KV_CLIENT_)?GET_2000(?:0|1)(?:/|$)"), ">20ms"),
)


def _percentile(values: list[float], ratio: float) -> float | None:
    ordered = sorted(float(v) for v in values if v is not None)
    if not ordered:
        return None
    pos = (len(ordered) - 1) * ratio
    low, high = math.floor(pos), math.ceil(pos)
    value = ordered[low] if low == high else ordered[low] + (ordered[high] - ordered[low]) * (pos - low)
    return round(value, 3)


def _band_from_member(member: str) -> str | None:
    normalized = member.replace("\\", "/").replace(",", "_") + "/"
    for pattern, label in BAND_PATTERNS:
        if pattern.search(normalized):
            return label
    return None


def _archive_trace_bands(path: Path) -> tuple[dict[str, str], Counter]:
    """Read tar headers only; trace member contents stay owned by ds_trace_triage."""
    mapping: dict[str, str] = {}
    counts: Counter = Counter()
    error_ids: set[str] = set()
    with tarfile.open(path, "r:*") as archive:
        for member in archive:
            if not member.isfile() or trace_member_id(member.name) is None:
                continue
            band = _band_from_member(member.name)
            if band is None:
                if (collection_cohort(member.name) or "").startswith("core/"):
                    error_ids.add(trace_member_id(member.name))
                continue
            trace_id = trace_member_id(member.name)
            if trace_id in mapping and mapping[trace_id] != band:
                raise ValueError(f"Trace ID appears in multiple bands: {trace_id}")
            if trace_id not in mapping:
                counts[band] += 1
            mapping[trace_id] = band
    for trace_id in error_ids - mapping.keys():
        mapping[trace_id] = "错误采样（无时延桶）"
        counts["错误采样（无时延桶）"] += 1
    return mapping, counts


def _stage_observed(row: dict[str, Any], stage: str, value: Any) -> bool:
    if value is None:
        return False
    if stage in {"URMA", "URMA通信"}:
        return bool(row.get("urma_observed"))
    if stage in {"RPC网络", "RPC网络相关"}:
        return bool(row.get("rpc_observed"))
    return float(value) > 0


def _summarize_band(rows: list[dict[str, Any]], raw_count: int, cap: int) -> dict[str, Any]:
    problems = Counter(
        row.get("focus_primary_problem") or row.get("primary_problem") or "未解释残差"
        for row in rows
    )
    transports = Counter(row.get("transport") or "未观测" for row in rows)
    locations = Counter(row.get("access_location") or "未观测" for row in rows)
    scopes = Counter(row.get("data_access_scope") or "未提供定位信息" for row in rows)
    stages: dict[str, list[float]] = {name: [] for name in PROBLEMS}
    requests: list[dict[str, Any]] = []
    for row in rows:
        breakdown = row.get("focus_breakdown_ms") or row.get("attribution_ms") or {}
        for name, value in breakdown.items():
            if name in stages and _stage_observed(row, name, value):
                stages[name].append(float(value))
        requests.extend(req for req in row.get("urma_requests", []) if req.get("total_ms") is not None)
    slow_requests = [req for req in requests if float(req["total_ms"]) > 1.5]
    sample_count = len(rows)
    return {
        "sample_count": sample_count,
        "collected_member_count": raw_count,
        "capped": bool(cap and raw_count >= cap),
        "failed_count": sum(bool(row.get("failed")) for row in rows),
        "client_p50_ms": _percentile([row.get("client_ms") for row in rows], 0.5),
        "client_p90_ms": _percentile([row.get("client_ms") for row in rows], 0.9),
        "client_max_ms": _percentile([row.get("client_ms") for row in rows], 1.0),
        "dominant_problem": problems.most_common(1)[0][0] if problems else "无样本",
        "problem_counts": {name: problems.get(name, 0) for name in PROBLEMS},
        "problem_shares_pct": {
            name: round(problems.get(name, 0) * 100 / sample_count, 1)
            if sample_count
            else 0.0
            for name in PROBLEMS
        },
        "stage_p50_ms": {name: _percentile(values, 0.5) for name, values in stages.items()},
        "stage_p90_ms": {name: _percentile(values, 0.9) for name, values in stages.items()},
        "transport_counts": dict(transports),
        "access_location_counts": dict(locations),
        "data_access_scope_counts": dict(scopes),
        "data_access_scope_shares_pct": {
            name: round(count * 100 / sample_count, 1) if sample_count else 0.0
            for name, count in scopes.items()
        },
        "urma_trace_count": sum(bool(row.get("urma_observed")) for row in rows),
        "urma_wr_count": len(requests),
        "slow_urma_wr_count": len(slow_requests),
        "slow_urma_wr_share_pct": round(len(slow_requests) * 100 / len(requests), 1) if requests else None,
        "urma_wr_p90_ms": _percentile([req["total_ms"] for req in requests], 0.9),
        "evidence_gap_count": problems.get("数据访问父窗口/未细分", 0) + problems.get("未解释残差", 0),
    }


def _control_groups(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []

    def add(family: str, fixed: dict[str, str], items: list[dict[str, Any]], varying: str) -> None:
        ordered = sorted(
            items,
            key=lambda r: (
                IMPL_ORDER.get(r["implementation"], 9),
                r["load"],
                r["size"],
                r["id"],
            ),
        )
        groups.append(
            {
                "id": f"{family}-{len(groups)+1}",
                "family": family,
                "fixed": fixed,
                "varying": varying,
                "run_ids": [item["id"] for item in ordered],
            }
        )

    by_impl: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    by_load: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    by_size: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    by_shape: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        if run.get("case_study_only"):
            continue
        by_impl[(run["size"], run["load"], run["client_shape"])].append(run)
        by_load[(run["implementation"], run["size"])].append(run)
        by_size[(run["implementation"], run["load"], run["client_shape"])].append(run)
        if run["load"] in {"315", "3x105"}:
            by_shape[(run["implementation"], run["size"])].append(run)
    for (size, load, shape), items in by_impl.items():
        if len({item["implementation"] for item in items}) >= 2:
            add("implementation", {"size": size, "load": load, "client_shape": shape}, items, "implementation")
    for (implementation, size), items in by_load.items():
        if len({item["load"] for item in items}) >= 2:
            add("load", {"implementation": implementation, "size": size}, items, "load")
    for (implementation, size), items in by_shape.items():
        if {item["load"] for item in items} == {"315", "3x105"}:
            add(
                "client_shape",
                {"implementation": implementation, "size": size, "aggregate_qps": "~315"},
                items,
                "client_shape",
            )
    for (implementation, load, shape), items in by_size.items():
        if len({item["size"] for item in items}) >= 2:
            add("object_size", {"implementation": implementation, "load": load, "client_shape": shape}, items, "size")
    return groups


def _control_insights(runs: list[dict[str, Any]], groups: list[dict[str, Any]]) -> list[dict[str, str]]:
    by_id = {run["id"]: run for run in runs}
    insights: list[dict[str, str]] = []
    for group in groups:
        members = [by_id[run_id] for run_id in group["run_ids"]]
        for band in BANDS:
            if not all(run["bands"][band]["sample_count"] for run in members):
                continue
            parts = []
            for run in members:
                stat = run["bands"][band]
                dominant = stat["dominant_problem"]
                share = stat["problem_shares_pct"].get(dominant, 0)
                urma = "未观测" if stat["urma_wr_p90_ms"] is None else f"{stat['urma_wr_p90_ms']:.3f}ms"
                parts.append(
                    f"{run['id']}：{dominant}（档内占比 {share:.1f}%），"
                    f"Client p90 {stat['client_p90_ms']:.3f}ms，URMA WR p90 {urma}"
                )
            fixed = "、".join(f"{key}={value}" for key, value in group["fixed"].items())
            insights.append({
                "group_id": group["id"],
                "family": group["family"],
                "band": band,
                "title": f"{group['family']}控制 · {band} · 固定 {fixed}",
                "text": "；".join(parts) + "。仅比较该异常档样本内部，不代表发生率。",
            })
    return insights


def build_suite(manifest: dict[str, Any]) -> dict[str, Any]:
    if manifest.get("schema_version") != 1:
        raise ValueError("suite manifest schema_version must be 1")
    configured = manifest.get("runs") or []
    ids = [item.get("id") for item in configured]
    if any(not isinstance(i, str) or i in {"", ".", ".."} or any(c in i for c in "/\\:") for i in ids):
        raise ValueError("run id must be a single directory name")
    duplicate = next((run_id for run_id, count in Counter(ids).items() if count > 1), None)
    if duplicate:
        raise ValueError(f"duplicate run id: {duplicate}")
    cap_default = int((manifest.get("sampling") or {}).get("max_per_band") or 0)
    runs: list[dict[str, Any]] = []
    for cfg in configured:
        archive_path, analysis_path = Path(cfg["input_archive"]), Path(cfg["analysis_json"])
        if not archive_path.is_file():
            raise ValueError(f"input archive not found: {archive_path}")
        if not analysis_path.is_file():
            raise ValueError(f"analysis JSON not found: {analysis_path}")
        analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
        trace_bands, member_counts = _archive_trace_bands(archive_path)
        grouped = {label: [] for label in BANDS}
        unmatched: list[str] = []
        for row in analysis.get("traces", []):
            band = trace_bands.get(row.get("trace_id"))
            if band is None:
                unmatched.append(str(row.get("trace_id")))
            else:
                grouped[band].append(row)
        if unmatched:
            raise ValueError(f"run {cfg['id']} has {len(unmatched)} unmatched Trace IDs")
        cap = int(cfg.get("sampling_cap_per_band") or cap_default)
        run_keys = (
            "id",
            "label",
            "implementation",
            "local_cache",
            "placement",
            "read_path",
            "size",
            "load",
            "client_shape",
            "triage_report",
            "bottleneck_report",
            "write_bottleneck_report",
            "numa_report",
            "input_archive",
            "analysis_json",
            "triage_json",
            "write_analysis_json",
            "numa_analysis_json",
            "case_study_only",
        )
        run = {key: cfg.get(key) for key in run_keys}
        if cfg.get("issues_analysis_json"):
            run["issues_analysis_json"] = cfg["issues_analysis_json"]
        write_model = (
            json.loads(Path(run["write_analysis_json"]).read_text(encoding="utf-8"))
            if run.get("write_analysis_json")
            else analysis
        )
        numa_model = (
            json.loads(Path(run["numa_analysis_json"]).read_text(encoding="utf-8"))
            if run.get("numa_analysis_json")
            else {}
        )
        issue_model = (
            json.loads(Path(run["issues_analysis_json"]).read_text(encoding="utf-8"))
            if run.get("issues_analysis_json") else None
        )
        write_rows = write_model.get("rows", write_model.get("write_traces", []))
        if not isinstance(write_rows, list):
            raise ValueError("write model requires a rows list")
        write_latencies = [float(row.get("client_ms")) for row in write_rows if row.get("client_ms") is not None]
        numa_aggregate = numa_model.get("aggregate") or {}
        error_families = Counter()
        for row in analysis.get("traces", []):
            if row.get("failed"):
                family = str(row.get("error_chain_category") or row.get("failure_reason") or "未分类")
                error_families[family] += 1
        read_failure_count = sum(bool(row.get("failed")) for row in analysis.get("traces", []))
        if issue_model is not None:
            issue_counts = issue_model.get("counts") or {}
            if issue_counts.get("read_final_failure_trace_count") != read_failure_count:
                raise ValueError(f"run {cfg['id']} issue/read failure counts differ")
            if issue_counts.get("write_final_failure_trace_count") != sum(
                    ds_trace_overview.failed(row) for row in write_rows):
                raise ValueError(f"run {cfg['id']} issue/write failure counts differ")
        run.update(
            {
                "trace_count": len(analysis.get("traces", [])),
                "error_count": (issue_model["counts"]["read_final_failure_trace_count"]
                                if issue_model is not None else read_failure_count),
                "error_family_counts": dict(error_families),
                "read_summary": {
                    "trace_count": len(analysis.get("traces", [])),
                    "failed_count": read_failure_count,
                },
                "write_summary": {
                    "trace_count": len(write_rows),
                    "failed_count": sum(ds_trace_overview.failed(row) for row in write_rows),
                    "p90_ms": _percentile(write_latencies, 0.9),
                },
                "numa_summary": {
                    "trace_count": len(numa_model.get("traces") or []),
                    "slow_wr_count": numa_aggregate.get("slow_wr_count", 0),
                    "dual_chip_trace_count": numa_aggregate.get("dual_chip_trace_count", 0),
                    "error_family_counts": numa_aggregate.get("error_family_counts", {}),
                },
                "unmatched_trace_count": 0,
                "bands": {
                    label: _summarize_band(grouped[label], member_counts[label], cap)
                    for label in BANDS
                },
            }
        )
        if issue_model is not None:
            run["issue_summary"] = issue_model["counts"]
        run["overview_summary"] = ds_trace_overview.summarize_run(cfg, analysis, write_model, numa_model)
        if issue_model is not None:
            run["overview_summary"]["issues"] = issue_model["counts"]
        runs.append(run)
    control_groups = _control_groups(runs)
    return {
        "schema_version": SCHEMA_VERSION,
        "title": manifest.get("title") or "DataSystem 多Run分档关键瓶颈分析",
        "source_ref": manifest.get("source_ref") or manifest.get("source_head") or "unknown",
        "tool_head": manifest.get("tool_head"), "source_note": manifest.get("source_note"),
        "focus": manifest.get("focus") or {},
        "sampling": manifest.get("sampling") or {},
        "overview": manifest.get("overview") or [],
        "runs": runs,
        "control_groups": control_groups,
        "insights": _control_insights(runs, control_groups),
        "limitations": [
            "Per-band counts are capped anomaly samples, not occurrence rates or a "
            "denominator for benefit percentages.",
            "Each run remains isolated; control-variable comparisons use normalized "
            "within-band composition and latency only.",
            "Archive access is limited to member names for band provenance; trace content "
            "is parsed only by ds_trace_analysis.py triage.",
            "Missing RPC, URMA, CPU, lock, or scheduling evidence remains unobserved and is never normalized to zero.",
        ],
    }


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    for run in manifest.get("runs") or []:
        for field in ("input_archive", "analysis_json", "write_analysis_json", "numa_analysis_json",
                      "issues_analysis_json"):
            if not run.get(field):
                continue
            value = Path(run[field])
            if not value.is_absolute():
                run[field] = str((path.parent / value).resolve())
    return manifest


HTML = ('<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>__REPORT_TITLE__</title><style>' r'''
:root{--bg:#f4f7fb;--card:#fff;--ink:#172033;--muted:#68738a;--line:#dfe6f1;--blue:#2878ff;--amber:#e99b24;--red:#d94352}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.55 "Microsoft YaHei","微软雅黑",sans-serif}.wrap{max-width:1540px;margin:auto;padding:18px}.hero{padding:24px 28px;border-radius:16px;background:linear-gradient(135deg,#12203c,#245bb3);color:#fff}.hero h1{margin:0 0 8px}.notice{margin:16px 0;padding:14px 18px;background:#fff7e8;border-left:5px solid var(--amber);border-radius:10px}.card{margin:16px 0;padding:18px;background:var(--card);border:1px solid var(--line);border-radius:14px}.chart-title,h2{text-align:center;margin:0 0 14px}.filters{display:flex;gap:10px;flex-wrap:wrap;margin:12px 0}.filters select{padding:8px;border:1px solid var(--line);border-radius:8px;background:white}.chart{height:430px;width:100%}.chart.tall{height:560px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}table{width:100%;table-layout:fixed;border-collapse:collapse;font-size:13px}th,td{padding:8px 5px;border-bottom:1px solid var(--line);text-align:center;overflow-wrap:anywhere}th{background:#f1f5fb}td:first-child,th:first-child{text-align:left}a{color:var(--blue)}.small{color:var(--red)}.muted{color:var(--muted)}.group{border-left:4px solid var(--blue);padding:9px 12px;margin:8px 0;background:#f6f9ff}.summary-grid{display:grid;grid-template-columns:repeat(4,minmax(160px,1fr));gap:10px;margin:12px 0}.summary-tile{border:1px solid var(--line);border-radius:10px;padding:12px;background:#f8fbff}.summary-tile b{display:block;font-size:20px}.summary-tile span{color:var(--muted);font-size:12px}@media(max-width:900px){.wrap{padding:8px}.grid{grid-template-columns:1fr}.summary-grid{grid-template-columns:repeat(2,minmax(140px,1fr))}.chart{height:360px}table{font-size:12px}th,td{padding:5px 2px}.optional{display:none}}
</style></head><body><div class="wrap"><section class="hero"><h1>__REPORT_TITLE__</h1><div>控制变量：实现 × 数据大小 × QPS/Client形态 × 异常时延档</div><div>源码校正：<code id="source-ref"></code></div></section><div class="notice"><b>采样边界：</b>本报告使用 capped anomaly samples。每档达到上限只表示采集封顶，不是线上发生率；跨run不计算收益百分比。每个run独立解析并链接到自己的详细页面。</div><section class="card" id="overview-card"><h2>0. 关键结论</h2><div id="overview"></div></section>
<section class="card"><h2>1. Run清单与详细页面</h2><div id="run-summary" class="summary-grid"></div><div class="filters"><select id="impl"><option value="">全部实现</option></select><select id="size"><option value="">全部大小</option></select><select id="load"><option value="">全部负载</option></select><select id="band"><option value="">全部档位</option></select></div><table><thead><tr><th>Run</th><th>实现</th><th>大小</th><th>负载</th><th>Client形态</th><th>读取</th><th>错误</th><th>写入</th><th>NUMA</th><th>主瓶颈</th><th>详细页面</th></tr></thead><tbody id="run-rows"></tbody></table></section>
<section class="card"><h2>2. 控制变量对比组</h2><div id="groups"></div></section><section class="card"><h2>3. 控制变量关键结论</h2><div id="insights"></div></section><section class="card"><h2 class="chart-title">4. 分档主瓶颈构成（档内100%）</h2><div id="problem" class="chart tall"></div></section><section class="card"><h2 class="chart-title">5. 数据访问定位细分（档内100%）</h2><div id="scope" class="chart tall"></div></section><section class="grid"><section class="card"><h2 class="chart-title">6. Client时延分位</h2><div id="latency" class="chart"></div></section><section class="card"><h2 class="chart-title">7. URMA WR（慢阈值 >1.5ms）</h2><div id="urma" class="chart"></div></section></section><section class="card"><h2 class="chart-title">8. 关键阶段p90</h2><div id="stages" class="chart tall"></div></section><section class="card"><h2>9. 证据与解释边界</h2><ul id="limitations"></ul></section></div>
<script>__ECHARTS_SOURCE__</script><script>const D=__DATA_JSON__;const COLORS=['#2878ff','#8759d6','#25a77a','#e99b24','#d95d6c','#8d96a8','#0ea5a4','#b7791f','#805ad5','#2b6cb0','#c53030','#4a5568','#38a169'];const BAND_ORDER=['5–7ms','7–10ms','10–20ms','>20ms'];let charts=[];const $=id=>document.getElementById(id),uniq=a=>[...new Set(a.filter(Boolean))],esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));$('source-ref').textContent=D.source_ref;$('limitations').innerHTML=D.limitations.map(x=>`<li>${esc(x)}</li>`).join('');$('overview').innerHTML=(D.overview||[]).map(x=>`<div class="group"><b>${esc(x.title)}</b><br>${esc(x.text)}</div>`).join('');if(!(D.overview||[]).length)$('overview-card').hidden=true;
function options(id,values){$(id).innerHTML+=uniq(values).map(v=>`<option value="${v}">${v}</option>`).join('')}options('impl',D.runs.map(r=>r.implementation));options('size',D.runs.map(r=>r.size));options('load',D.runs.map(r=>r.load));options('band',BAND_ORDER);function selectedRuns(){return D.runs.filter(r=>(!$('impl').value||r.implementation===$('impl').value)&&(!$('size').value||r.size===$('size').value)&&(!$('load').value||r.load===$('load').value))}function combos(){const band=$('band').value;return selectedRuns().flatMap(r=>(band?[band]:BAND_ORDER).map(b=>({r,b,s:r.bands[b]})).filter(x=>x.s.sample_count))}function fmtObj(o){return Object.entries(o||{}).map(x=>x.join(':')).join(' / ')||'未观测'}
function renderSummary(){const selected=selectedRuns(),read=selected.reduce((n,r)=>n+(r.read_summary?.trace_count||0),0),errors=selected.reduce((n,r)=>n+(r.error_count||0),0),write=selected.reduce((n,r)=>n+(r.write_summary?.trace_count||0),0),numa=selected.reduce((n,r)=>n+(r.numa_summary?.trace_count||0),0);$("run-summary").innerHTML=[[selected.length,"Runs"],[read,"读取 Traces"],[errors,"错误 Traces"],[write,"写入 Traces"],[numa,"NUMA Traces"]].map(([value,label])=>`<div class="summary-tile"><b>${value}</b><span>${label}</span></div>`).join("")}
function renderRows() {
  const band = $("band").value;
  $("run-rows").innerHTML = selectedRuns()
    .map((r) => {
      const bs = (band ? [band] : BAND_ORDER).filter(
          (b) => r.bands[b].sample_count,
        ),
        n = bs.reduce((a, b) => a + r.bands[b].sample_count, 0),
        ps = {};
      bs.forEach((b) =>
        Object.entries(r.bands[b].problem_counts).forEach(
          ([k, v]) => (ps[k] = (ps[k] || 0) + v),
        ),
      );
      const top =
          Object.entries(ps).sort((a, b) => b[1] - a[1])[0]?.[0] || "无样本",
        write = r.write_bottleneck_report
          ? ` · <a target="_blank" rel="noopener" href="${esc(r.write_bottleneck_report)}">写入瓶颈</a>`
          : "",
        numa = r.numa_report ? ` · <a target="_blank" rel="noopener" href="${esc(r.numa_report)}">WR/NUMA</a>` : "";
      const read = r.read_summary || {trace_count:n, failed_count:0}, ws = r.write_summary || {}, ns = r.numa_summary || {};
      return `<tr><td><b>${r.label}</b>${r.case_study_only ? '<br><span class="small">仅案例</span>' : ""}</td><td>${r.implementation}<br><span class="muted">${r.read_path || r.placement || ""}</span></td><td>${r.size}</td><td>${r.load}</td><td>${r.client_shape}</td><td>${read.trace_count}条</td><td>${r.error_count}条</td><td>${ws.trace_count || 0}条</td><td>${ns.trace_count || 0}条 / 慢WR ${ns.slow_wr_count || 0}</td><td>${top}</td><td><a target="_blank" rel="noopener" href="${esc(r.triage_report)}">Trace Triage</a> · <a target="_blank" rel="noopener" href="${esc(r.bottleneck_report)}">读取瓶颈</a>${write}${numa}</td></tr>`;
    })
    .join("");
}
$("groups").innerHTML = D.control_groups
  .map(
    (g) =>
      `<div class="group"><b>${g.family}</b> · 固定 ${fmtObj(g.fixed)}<br><span class="muted">变化：${g.varying}；Runs：${g.run_ids.join(" / ")}</span></div>`,
  )
  .join("");
$("insights").innerHTML = D.insights
  .map((x) => `<div class="group"><b>${x.title}</b><br>${x.text}</div>`)
  .join("");
function draw(id, opt) {
  const old = echarts.getInstanceByDom($(id));
  if (old) old.dispose();
  const c = echarts.init($(id));
  c.setOption({...opt,textStyle:{...opt.textStyle,fontFamily:'Microsoft YaHei'}});
  charts.push(c);
}
function base(cs) {
  return {
    tooltip: { trigger: "axis" },
    legend: { top: 2 },
    grid: { left: 58, right: 18, top: 52, bottom: 110 },
    xAxis: {
      type: "category",
      data: cs.map((x) => x.r.id + "\n" + x.b),
      axisLabel: { interval: 0, rotate: 28 },
    },
    yAxis: { type: "value" },
  };
}
'''
    "function renderCharts(){const cs=combos(),p=base(cs);draw('problem',{...p,yAxis:{type:'value',"
    "max:100,name:'档内占比 %'},series:D.problems.map((name,i)=>({name,type:'bar',stack:'root',"
    'data:cs.map(x=>x.s.problem_shares_pct[name]||0),itemStyle:{color:COLORS[i%COLORS.length]}}))});'
    "draw('scope',{...p,yAxis:{type:'value',max:100,name:'档内占比 %'},series:D.scopes.map((name,"
    "i)=>({name,type:'bar',stack:'scope',data:cs.map(x=>x.s.data_access_scope_shares_pct[name]||0),"
    "itemStyle:{color:COLORS[i%COLORS.length]}}))});draw('latency',{...p,yAxis:{type:'value',"
    "name:'ms'},series:[{name:'p50',type:'bar',data:cs.map(x=>x.s.client_p50_ms)},{name:'p90',"
    "type:'bar',data:cs.map(x=>x.s.client_p90_ms)},{name:'max',type:'line',"
    "data:cs.map(x=>x.s.client_max_ms)}]});draw('urma',{...p,yAxis:[{type:'value',name:'WR数'},"
    "{type:'value',name:'慢WR %',max:100}],series:[{name:'慢WR',type:'bar',"
    "data:cs.map(x=>x.s.slow_urma_wr_count),itemStyle:{color:'#d94352'}},{name:'慢WR占比',type:'line',"
    "yAxisIndex:1,data:cs.map(x=>x.s.slow_urma_wr_share_pct),itemStyle:{color:'#e99b24'}}]});"
    "draw('stages',{...p,yAxis:{type:'value',name:'阶段p90 ms'},series:D.problems.map((name,i)=>({name,"
    "type:'bar',data:cs.map(x=>x.s.stage_p90_ms[name]),"
    'itemStyle:{color:COLORS[i%COLORS.length]}}))})}function render(){renderSummary();renderRows();'
    "renderCharts()}['impl','size','load','band'].forEach(id=>$(id).onchange=render);render();"
    "addEventListener('resize',()=>charts.forEach(c=>c.resize()));</script></body>"
    '</html>'
)


def render_suite_html(suite: dict[str, Any], echarts_source: str) -> str:
    observed_problems = set()
    scopes = set()
    for run in suite["runs"]:
        for band in run["bands"].values():
            observed_problems.update(
                name for name, count in band["problem_counts"].items() if count
            )
            scopes.update(band["data_access_scope_counts"])
    active_problems = [name for name in PROBLEMS if name in observed_problems]
    scopes = sorted(scopes)
    data_json = json.dumps(
        {**suite, "problems": active_problems or ["未解释残差"], "scopes": scopes},
        ensure_ascii=False,
    ).replace("</", "<\\/")
    template = HTML.replace("__REPORT_TITLE__", html.escape(str(suite["title"])))
    template = template.replace(
        "const BAND_ORDER=['5–7ms','7–10ms','10–20ms','>20ms']",
        "const BAND_ORDER=" + json.dumps(BANDS, ensure_ascii=False),
    )
    template = template.replace(
        "</head>",
        '<style>body *{font-family:"Microsoft YaHei","微软雅黑",sans-serif!important}</style></head>',
        1,
    )
    return template.replace("__ECHARTS_SOURCE__", echarts_source, 1).replace("__DATA_JSON__", data_json, 1)


def write_outputs(suite, output, analysis_output, *, force=False, library_path=None, layout="overview"):
    checked = validate_suite(suite)
    if not checked["valid"]:
        raise ValueError("suite validation failed: " + "; ".join(checked["errors"]))
    for target in (output, analysis_output):
        if target.exists() and not force:
            raise FileExistsError(f"refusing to overwrite {target}; pass --force")
    library_path = (
        library_path
        or echarts_path()
    )
    import os
    suite["analysis_download"] = os.path.relpath(analysis_output, output.parent).replace(os.sep, "/")
    output.parent.mkdir(parents=True, exist_ok=True)
    analysis_output.parent.mkdir(parents=True, exist_ok=True)
    analysis_output.write_text(json.dumps(suite, ensure_ascii=False, indent=2), encoding="utf-8")
    renderer = ds_trace_overview.render if layout == "overview" else render_suite_html
    output.write_text(renderer(suite, library_path.read_text(encoding="utf-8")), encoding="utf-8")
    for run in suite["runs"]:
        summary_path = output.parent / "runs" / run["id"] / "run.summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(run["overview_summary"], ensure_ascii=False), encoding="utf-8")
    return output.resolve(), analysis_output.resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a controlled multi-run DataSystem bottleneck dashboard.")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--analysis-json", type=Path)
    parser.add_argument("--echarts", type=Path)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--layout", choices=("overview", "legacy"), default="overview")
    args = parser.parse_args()
    analysis_output = args.analysis_json or args.output.with_name("suite.analysis.json")
    for target in (args.output, analysis_output):
        if target.exists() and not args.force:
            raise SystemExit(f"refusing to overwrite {target}; pass --force")
    suite = build_suite(load_manifest(args.manifest))
    write_outputs(suite, args.output, analysis_output, force=args.force, library_path=args.echarts, layout=args.layout)
    sys.stdout.write(f"{args.output}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
