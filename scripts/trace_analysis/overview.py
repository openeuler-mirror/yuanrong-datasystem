"""Validated, compact per-Run summaries and an offline overview template."""
import html
import json
import math
import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

from .resources import asset_path
from .rendering.template import replace_tokens

BANDS = ['<5ms', '5–6ms', '6–7ms', '7–10ms', '10–20ms', '>20ms', '未观测']


def band(value):
    if value is None:
        return '未观测'
    if value < 5:
        return '<5ms'
    if value < 6:
        return '5–6ms'
    if value < 7:
        return '6–7ms'
    if value < 10:
        return '7–10ms'
    if value <= 20:
        return '10–20ms'
    return '>20ms'


def quantile(values, q):
    values = sorted(float(x) for x in values if x is not None and math.isfinite(float(x)))
    if not values:
        return None
    p = (len(values) - 1) * q
    lo = int(p)
    hi = math.ceil(p)
    return round(values[lo] + (values[hi] - values[lo]) * (p - lo), 4)


def failed(row):
    if 'failed' in row:
        return bool(row['failed'])
    return str(row.get('status') or '0') not in ('0', 'OK')


def summarize(rows):
    return {
        'count': len(rows),
        'failed': sum(failed(r) for r in rows),
        'bands': dict(Counter(band(r.get('client_ms')) for r in rows)),
        'p50_ms': quantile([r.get('client_ms') for r in rows], .5),
        'p90_ms': quantile([r.get('client_ms') for r in rows], .9),
        'max_ms': quantile([r.get('client_ms') for r in rows], 1),
        'problems': dict(Counter(
            r.get('focus_primary_problem') or r.get('write_primary_stage') or r.get('primary_problem') or '未细分'
            for r in rows
        )),
        'errors': dict(Counter(
            r.get('error_family') or r.get('error_chain_category') or r.get('failure_reason') or '未分类'
            for r in rows if failed(r)
        ))
    }


def summarize_run(config, read, write, numa):
    rows = read.get("traces") or []
    write_rows = write.get("rows", write.get("write_traces", []))
    for kind, items in (("read", rows), ("write", write_rows)):
        ids = [item["trace_id"] for item in items]
        if len(ids) != len(set(ids)):
            raise ValueError(f"duplicate {kind} Trace IDs in {config['id']}")
    links = {
        key: config.get(key)
        for key in ("triage_report", "bottleneck_report", "write_bottleneck_report", "numa_report")
    }
    for link in links.values():
        if link and (urlsplit(link).scheme or link.startswith(("/", "\\"))):
            raise ValueError("report links must be relative")
    agg = numa.get("aggregate") or {}
    return {"id": config["id"], "label": config.get("label") or config["id"], "links": links,
            "read": summarize(rows), "write": summarize(write_rows),
            "numa": {"count": len(numa.get("traces") or []), "slow_wr": agg.get("slow_wr_count"),
                     "dual_chip": agg.get("dual_chip_trace_count"), "observed": bool(numa)},
            "coverage": read.get("evidence_coverage") or {},
            "source_trace_count": read.get("source_trace_count", len(rows) + len(write_rows)),
            "excluded_without_client_window": read.get("excluded_without_client_window"),
            "excluded_non_get": read.get("excluded_non_get"),
            "metadata": {key: config.get(key) for key in ("size", "load", "client_shape")}}


def render(suite, echarts_source):
    data = {"schema_version": 1, "title": suite["title"],
            "source_ref": suite.get("tool_head") or suite.get("source_ref"),
            "source_note": suite.get("source_note") or "源码参考基线不等于已确认部署版本",
            "sampling": suite.get("sampling") or {}, "bands": BANDS,
            "runs": [run["overview_summary"] for run in suite["runs"]],
            "notes": suite.get("overview") or [], "focus": suite.get("focus") or {},
            "downloads": (
                [{"href": suite["analysis_download"], "label": "汇总中间数据"}] if suite.get("analysis_download") else []
            )}
    page = asset_path("overview.html").read_text(encoding="utf-8")
    values = {"__TITLE__": html.escape(str(data["title"])),
              "__DATA__": json.dumps(data, ensure_ascii=False).replace("<", "\\u003c"),
              "__OVERVIEW_CSS__": asset_path("overview.css").read_text(),
              "__OVERVIEW_JS__": asset_path("overview.js").read_text(), "__ECHARTS_SOURCE__": echarts_source,
              "__CHART_SUPPORT__": (
                  asset_path("charts.js").read_text() + "\n" + asset_path("chapter_navigation.js").read_text()
              )}
    from .rendering.registry import embed_registry
    return embed_registry(replace_tokens(page, "|".join(map(re.escape, values)), values), "overview")
