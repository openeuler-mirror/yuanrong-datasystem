"""Render normalized Trace reports without importing the log parser."""

import json

from ..resources import asset_path
from .registry import embed_registry
from .template import replace_tokens
from ..analysis.triage_artifacts import build_events, build_triage


def render_markdown(report):
    lines = [
        "# Trace Triage Summary",
        "",
        f"- code_ref: `{report['code_ref']}`",
        f"- trace_count: {report['trace_count']}",
        f"- time_range: {report['dimensions']['time']['first_ts']} -> {report['dimensions']['time']['last_ts']}",
        "",
        "## Top Workers",
    ]
    for worker, item in list(report["dimensions"]["workers"].items())[:10]:
        lines.append(f"- {worker}: {item['line_count']} lines")
    lines.extend(["", "## Flow"])
    for flow, count in report["dimensions"]["flow"].items():
        lines.append(f"- {flow}: {count}")
    lines.extend([
        "",
        "## Access Latency Ms",
        "```json",
        json.dumps(report["dimensions"]["latency_ms"], indent=2),
        "```",
    ])
    lines.extend(["", "## Breakdown Ms"])
    for key, item in sorted(report["dimensions"]["breakdown_ms"].items(), key=lambda kv: kv[1]["sum"], reverse=True):
        lines.append(f"- {key}: count={item['count']} sum={item['sum']} max={item['max']}")
    lines.extend(["", "## RPC Slow"])
    for method, item in report["dimensions"]["rpc_slow"].items():
        fields = " ".join(f"{k}={v}" for k, v in item.items() if k != "count")
        lines.append(f"- {method}: count={item['count']} {fields}".rstrip())
    lines.extend(["", "## URMA Elapsed"])
    for name, item in report["dimensions"]["urma_elapsed"].items():
        lines.append(f"- {name}: {item}")
    lines.extend(["", "## Latency Summary Us"])
    for name, item in report["dimensions"]["latency_summary_us"].items():
        lines.append(f"- {name}: {item}")
    lines.extend(["", "## Errors"])
    for error, count in report["dimensions"]["errors"].items():
        lines.append(f"- {error}: {count}")
    lines.extend(["", "## Classifications"])
    for name, count in report["dimensions"]["classifications"].items():
        lines.append(f"- {name}: {count}")
    lines.extend(["", "## Trace Classifications"])
    for trace_id, item in sorted(report["traces"].items()):
        lines.append(f"- {trace_id}: {item['classification']}")
    return "\n".join(lines) + "\n"


def render_html(report, title, site=False, manifest=None, *, asset_resolver=None):
    asset_resolver = asset_resolver or asset_path
    display_report = {**report, "traces": {}}
    deferred_traces, evidence_blocks = [], []
    deferred_keys = ("evidence", "ub_events", "rpc_calls", "rpc_stage_windows", "query_and_get_calls")
    for index, (trace_id, trace) in enumerate(report.get("traces", {}).items()):
        deferred = {key: trace[key] for key in deferred_keys if key in trace}
        display_report["traces"][trace_id] = {key: value for key, value in trace.items() if key not in deferred}
        deferred_traces.append([trace_id, list(deferred)])
        payload = json.dumps(deferred, ensure_ascii=False).replace("<", "\\u003c")
        evidence_blocks.append(f'<script type="application/json" id="trace-payload-{index}">{payload}</script>')
    data = json.dumps(display_report, ensure_ascii=False).replace("<", "\\u003c")
    manifest_data = json.dumps(manifest or {}, ensure_ascii=False).replace("</script>", "<\\/script>")
    base_style = (
        "<style>"
        + asset_resolver("triage.css").read_text(encoding="utf-8")
        + "</style>"
    )
    stylesheet = ('<link rel="stylesheet" href="/assets/css/site.css">' if site else "") + base_style
    script_ref = '<script src="/assets/js/site.js"></script>' if site else ""
    template = asset_resolver("triage.html").read_text(encoding="utf-8")
    chart_support = asset_resolver("charts.js").read_text(encoding="utf-8")
    chart_support += "\n" + asset_resolver("chapter_navigation.js").read_text(
        encoding="utf-8"
    )
    chart_support += '\n' + asset_resolver("log_fields.js").read_text(encoding='utf-8')
    chart_support += '\n' + asset_resolver("trace_visuals.js").read_text(encoding='utf-8')
    shared_style = asset_resolver("shared.css").read_text(encoding="utf-8")
    template = template.replace("</head>", "<style>" + shared_style + "</style></head>", 1)
    template = template.replace("</head>", "<script>" + chart_support + "</script></head>", 1)
    injections = {"__TITLE__": title, "__STYLESHEET__": stylesheet, "__DATA__": data,
                  "__MANIFEST__": manifest_data, "__SCRIPT_REF__": script_ref,
                  "__TRACE_PAYLOADS__": "".join(evidence_blocks),
                  "__DEFERRED_TRACES__": json.dumps(deferred_traces, ensure_ascii=False).replace("<", "\\u003c")}
    page = replace_tokens(
        template, r"__(?:TITLE|STYLESHEET|DATA|MANIFEST|SCRIPT_REF|TRACE_PAYLOADS|DEFERRED_TRACES)__",
        injections,
    )
    return embed_registry(page, "triage")


class TraceReportRenderer:
    """Render machine summaries into stage artifacts."""

    @staticmethod
    def events(report):
        return build_events(report)

    @staticmethod
    def triage(report):
        return build_triage(report)

    @staticmethod
    def markdown(report):
        return render_markdown(report)

    @staticmethod
    def html(report, title, site=False, manifest=None):
        return render_html(report, title, site=site, manifest=manifest)
