"""Render the already-attributed write model without changing its budget."""
import html
import json
from ..resources import asset_path, echarts_path
from .registry import embed_registry


def render_model(model, title, links=(), archives=()):
    template = asset_path("write.html").read_text(encoding="utf-8")
    script_names = (
        "report_navigation.js",
        "chapter_navigation.js",
        "trace_visuals.js",
        "trace_evidence_logs.js",
        "bottleneck_timeline.js",
    )
    script_sources = (asset_path(name).read_text(encoding="utf-8") for name in script_names)
    template = template.replace("</body>", "<script>" + "\n".join(script_sources) + "</script></body>", 1)
    for slot in ("__DATA__", "__ECHARTS__"):
        if template.count(slot) != 1:
            raise ValueError(f"write template requires exactly one {slot}")
    navigation = []
    for label, url in links:
        if ":" in url or url.startswith(("/", "\\\\")):
            raise ValueError("Report links must be relative paths")
        navigation.append(
            '<a href="'
            + html.escape(url, quote=True)
            + '">'
            + html.escape(label)
            + "</a>"
        )
    raw = "".join(
        '<li><a download href="'
        + html.escape(a["download_path"], quote=True)
        + '">'
        + html.escape(a["name"])
        + "</a></li>"
        for a in archives
        if ":" not in a["download_path"] and not a["download_path"].startswith("/")
    )
    echarts = (
        echarts_path()
    )
    page = template.replace("__TITLE__", html.escape(title)).replace(
        "__LINKS__", " · ".join(navigation)
    )
    page = page.replace("__RAW__", raw).replace(
        "__ECHARTS__",
        asset_path("report_runtime.js").read_text(encoding="utf-8")
        + "\n"
        + echarts.read_text(encoding="utf-8")
        + "</script><script>"
        + asset_path("charts.js").read_text(encoding="utf-8")
        + "\n"
        + asset_path("log_fields.js").read_text(encoding="utf-8"),
    )
    view_model = {**model, "rows": [
        {key: value for key, value in row.items() if key != "write_evidence_facts"}
        for row in model.get("rows", [])
    ]}
    page = page.replace(
        "__DATA__", json.dumps(view_model, ensure_ascii=False).replace("<", "\\u003c")
    )
    page = page.replace(
        "</head>",
        "<style>"
        + asset_path("shared.css").read_text(encoding="utf-8")
        + "\n"
        + asset_path("write.css").read_text(encoding="utf-8")
        + "</style></head>",
    )
    return embed_registry(page, "write")
