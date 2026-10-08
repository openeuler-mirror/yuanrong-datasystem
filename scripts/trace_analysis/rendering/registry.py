"""Compile expected report components from canonical template captions."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser

from ..resources import asset_path
from .triage_contracts import TRIAGE_DATA_CONTRACTS


READ_TITLES = {'direct-worker-chart': '图 1-6 Data Worker 负载与尾延迟',
 'direct-worker-table': '表 1-1 Data Worker 明细',
 'urma-source-chart': '图 4-3 URMA 源 Worker 时延',
 'urma-source-table': '表 4-3 URMA 源 Worker 明细'}


WRITE_TABLE_CONTAINERS = {
    "time-table": ("表 2-1 Client 时间分段", ("write_band", "rows", ["client_ms"], "number")),
    "worker-table": ("表 5-1 Worker 关联明细", ("write_worker", "rows", ["worker_observers.*"], "text")),
    "trace-table": ("表 6-1 写入 Trace", ("write_trace_filtered", "rows", ["trace_id"], "text")),
    "write-phase-table": ("表 3-1 阶段观测覆盖", ("write_phase", "rows", ["write_phase_observation.*.state"], "text")),
    "write-rpc-method-table": ("表 3-2 全 RPC 汇总", (
        "write_rpc_methods", "rows",
        ["rpc_analysis.calls.*.method", "rpc_analysis.summary_windows.*.stage_label"], "text")),
    "stage-root-table": ("表 1-1 问题证据明细", ("write_issue", "rows", ["issues.*"], "text")),
    "wr-time-table": ("表 4-1 WR 本地时间桶", ("write_wr_time", "events", ["trace_id"], "text")),
    "wr-worker-table": ("表 4-2 WR 目标 Worker", ("write_wr", "events", ["trace_id"], "text")),
    "wr-events-table": ("表 4-3 WR 分片证据", ("write_wr", "events", ["trace_id"], "text")),
}


DATA_CONTRACTS = {
    "overview": {
        'error-chart': ('overview', 'runs', ['read.failed', 'write.failed'], 'number'),
        'latency-chart': ('overview', 'runs', ['read.p90_ms', 'write.p90_ms'], 'number'),
        'wr-chart': ('overview', 'runs', ['numa.slow_wr'], 'number'),
        'run-summary-table': ('overview', 'runs', ['id'], 'text'),
        'coverage-summary-table': ('overview', 'runs', ['id'], 'text'),
        'focus-summary-table': ('overview_focus', 'runs', ['id'], 'text'),
        'family-chart': ('overview_errors', 'values', [''], 'number'),
        'band-chart': ('overview_bands', 'values', [''], 'number'),
        'problem-chart': ('overview_read_problems', 'values', [''], 'number'),
        'write-problem-chart': ('overview_write_problems', 'values', [''], 'number'),
    },
    "read": {
        'rpc-method-audit-table': ('read_rpc_audit', 'calls', ['method'], 'text'),

        'query-meta-detail-chart': ('read_aggregate', 'query_meta_analysis.detail_counts', [''], 'number'),
        'query-meta-time-chart': ('read_aggregate', 'query_meta_analysis.time_buckets', ['trace_count'], 'number'),
        'query-meta-worker-chart': ('read_aggregate', 'query_meta_analysis.initiators', ['trace_count'], 'number'),
        'query-meta-target-chart': ('read_aggregate', 'query_meta_analysis.meta_targets', ['trace_count'], 'number'),
        'worker-correlation-table': ('read_correlation', 'events', ['trace_id'], 'text'),
        'rpc-method-chart': ('read_rpc', 'calls', ['method'], 'text'),
        'rpc-histogram-chart': ('read_rpc', 'calls', ['total_ms'], 'number'),
        'rpc-summary-table': ('read_rpc', 'calls', ['method'], 'text'),
        'query-rpc-breakdown-chart': ('read_query_rpc', 'calls', ['total_ms'], 'number'),
        'query-worker-breakdown-chart': ('read_query_worker', 'calls', ['phases_ms.*'], 'number'),
        'urma-time-chart': ('read_urma', 'traces', ['urma_requests.*.total_ms'], 'number'),
        'worker-correlation-chart-rpc': ('read_correlation_rpc', 'events', ['trace_id'], 'text'),
        'worker-correlation-chart-ub': ('read_correlation_ub', 'events', ['trace_id'], 'text'),
        'worker-correlation-chart-metadata': ('read_correlation_metadata', 'events', ['trace_id'], 'text'),
        'worker-correlation-chart-data': ('read_correlation_data', 'events', ['trace_id'], 'text'),

        "error-subcategory-chart": ("read_errors", "traces", ["trace_id"], "text"),
        "error-chain-chart": ("read_errors", "traces", ["trace_id"], "text"),
        "time-segment-chart": ("read_aggregate", "latency_segments", ["trace_count"], "number"),
        "timeline-chart": ("read", "traces", ["focus_breakdown_ms.*"], "number"),
        "trace-table": ("read_filtered", "traces", ["trace_id"], "text"),
        "read-selected-stage-chart": (
            "read_selected", "traces", ["focus_breakdown_ms.*", "urma_requests.*.total_ms"], "number"),
        "direct-worker-chart": ("read_aggregate", "direct_data_workers", ["trace_count"], "number"),
        "direct-worker-table": ("read_aggregate", "direct_data_workers", ["worker"], "text"),
        "urma-source-chart": ("read_aggregate", "urma_source_workers", ["trace_count"], "number"),
        "urma-source-table": ("read_aggregate", "urma_source_workers", ["worker"], "text"),
        "urma-worker-chart": ("read_aggregate", "urma_analysis.source_workers", ["request_count"], "number"),
        "urma-time-table": ("read_aggregate", "urma_analysis.time_buckets", ["request_count"], "number"),
        "urma-edge-table": ("read_aggregate", "urma_analysis.worker_edges", ["request_count"], "number"),
        "problem-count-chart": ("read", "traces", ["focus_primary_problem"], "text"),
        "problem-latency-chart": ("read", "traces", ["client_ms"], "number"),
        "stage-share-chart": ("read", "traces", ["focus_breakdown_ms.*"], "number"),
    },
    "write": {
        **{key: item[1] for key, item in WRITE_TABLE_CONTAINERS.items()},
        'worker-chart': ('write_worker', 'rows', ['worker_observers.*'], 'text'),
        'error-chart': ('write_failed', 'rows', ['trace_id'], 'text'),
        'latency-band-chart': ('write_bands', 'rows', ['client_ms'], 'number'),
        'worker-time': ('write_worker_events', 'events', ['ms'], 'number'),
        'wr-count-chart': ('write_wr_count', 'rows', ['trace_id'], 'text'),
        'wr-time-chart': ('write_wr_time', 'events', ['trace_id'], 'text'),
        'wr-worker-chart': ('write_wr', 'events', ['trace_id'], 'text'),
        'write-rpc-method-chart': (
            'write_rpc_methods', 'rows',
            ['rpc_analysis.calls.*.method', 'rpc_analysis.summary_windows.*.stage_label'], 'text'),
        'write-rpc-histogram-chart': (
            'write_rpc_methods', 'rows',
            ['rpc_analysis.calls.*.fields_us.e2e_us', 'rpc_analysis.summary_windows.*.total_ms'], 'number'),

        "issue-chart": ("write_issue", "rows", ["issues.*"], "text"),
        "timeline": ("write_band", "rows", ["write_breakdown_ms.*"], "number"),
        "selected-chart": ("write_selected", "rows", ["write_breakdown_ms.*"], "number"),
        "selected-stage-table": ("write_selected", "rows", ["write_breakdown_ms.*"], "number"),
        "problem-count": ("write_band", "rows", ["write_primary_stage"], "text"),
        "problem-time": ("write_band", "rows", ["write_breakdown_ms.*"], "number"),
        "stage-share": ("write_band", "rows", ["write_breakdown_ms.*"], "number"),
    },
    "numa": {
        'chip-load-chart': ('numa_chip', 'traces', ['chip1_peak', 'chip2_peak'], 'number'),
        'read-worker-chart': ('numa_read', 'traces', ['trace_id'], 'text'),
        'read-time-chart': ('numa_read', 'time_buckets', ['trace_count'], 'number'),
        'read-worker-time-chart': ('numa_read_worker_time', 'time_buckets', ['trace_count'], 'number'),
        'write-worker-chart': ('numa_write', 'traces', ['trace_id'], 'text'),
        'write-time-chart': ('numa_write', 'time_buckets', ['trace_count'], 'number'),
        'write-worker-time-chart': ('numa_write_worker_time', 'time_buckets', ['trace_count'], 'number'),
        "trace-table": ("numa_filtered", "traces", ["trace_id"], "text"),
        "error-chart": ("numa", "aggregate.error_family_counts", [""], "number"),
        "error-op-chart": ("numa", "aggregate.operation_counts", [""], "number"),
        "latency-chart": ("numa", "latency_bands", ["unique_trace_count", "slow_wr_count"], "number"),
        "chip-mode-chart": ("numa", "aggregate.chip_mode_counts", [""], "number"),
    },
    "triage": TRIAGE_DATA_CONTRACTS,
}


@dataclass(eq=False)
class _Node:
    tag: str
    attrs: dict
    parent: object = None
    children: list = field(default_factory=list)
    text: str = ""


class _Inventory(HTMLParser):
    def __init__(self, page):
        super().__init__(convert_charrefs=True)
        self.root = _Node("root", {})
        self.stack = [self.root]
        self.nodes = []
        self.feed(page)

    def handle_starttag(self, tag, attrs):
        node = _Node(tag, dict(attrs), self.stack[-1])
        self.stack[-1].children.append(node)
        self.nodes.append(node)
        if tag not in {
            "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta",
            "param", "source", "track", "wbr",
        }:
            self.stack.append(node)

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                break

    def handle_data(self, text):
        if self.stack[-1].tag in {"script", "style"}:
            return
        for node in self.stack:
            if (node.tag in {"h1", "h2", "h3", "h4", "caption", "figcaption", "a"}
                    or "caption" in node.attrs.get("class", "")):
                node.text += text


def _heading(node, prefix=None):
    candidates = [x for x in node.children if x.tag in {"h1", "h2", "h3", "h4", "caption"}]
    return next((x.text.strip() for x in candidates if not prefix or x.text.strip().startswith(prefix)), "")


def _caption(node, kind):
    if node.parent:
        siblings = node.parent.children
        index = siblings.index(node)
        if index + 1 < len(siblings):
            after = siblings[index + 1]
            if (after.tag == "figcaption" or "caption" in after.attrs.get("class", "")):
                if re.match(r"^(图|表)\s*\d", after.text.strip()):
                    return after.text.strip()
    title = _heading(node)
    if title:
        return title
    child = node
    while child.parent:
        siblings = child.parent.children
        before = siblings[:siblings.index(child)]
        headings = [x.text.strip() for x in before if x.tag in {"h1", "h2", "h3", "h4", "caption"}]
        expected = "图" if kind == "chart" else "表"
        match = next((x for x in reversed(headings) if x.startswith(expected)), "")
        if match or headings:
            return match or headings[-1]
        child = child.parent
    return node.attrs.get("id", "")


def build_registry(page: str, page_kind: str) -> dict:
    """Preserve element IDs; captions are authoritative over duplicated nav labels."""
    inventory = _Inventory(page)
    chapters = []
    for node in inventory.nodes:
        title = ""
        for child in node.children:
            if child.tag in {"h1", "h2", "h3", "h4"}:
                if re.match(r"^(?:\d+\.\s|附录)", child.text.strip()):
                    title = child.text.strip()
                    break
        if node.attrs.get("id") and title:
            chapters.append({"id": node.attrs["id"], "title": title})
    chapter_ids = {item["id"] for item in chapters}
    nav_titles = {node.attrs["href"][1:]: node.text.strip() for node in inventory.nodes
                  if node.tag == "a" and node.attrs.get("href", "").startswith("#")}
    components, seen = [], set()
    for node in inventory.nodes:
        kind = "chart" if "chart" in node.attrs.get("class", "").split() else "table" if node.tag == "table" else None
        if page_kind == "write" and node.attrs.get("id") in WRITE_TABLE_CONTAINERS:
            kind = "table"
        if not kind or not node.attrs.get("id"):
            continue
        identity = node.attrs["id"]
        if identity in seen:
            raise ValueError(f"duplicate report component id: {identity}")
        seen.add(identity)
        parent = node.parent
        while parent and parent.attrs.get("id") not in chapter_ids:
            parent = parent.parent
        components.append({"id": identity, "kind": kind, "title": _caption(node, kind),
                           "chapter": parent.attrs["id"] if parent else "",
                           "required": True})
    for item in components:
        if not re.match(r"^(图|表)\s*\d", item["title"]) and re.match(r"^(图|表)\s*\d", nav_titles.get(item["id"], "")):
            item["title"] = nav_titles[item["id"]]
    if "section.id='trace-event-timeline'" in page and "trace-event-timeline" not in chapter_ids:
        chapters.append({"id": "trace-event-timeline", "title": "8. Trace 事件时间线"})
        components.extend([
            {"id": "trace-event-chart", "kind": "chart", "title": "图 8-1 Trace 分进程事件时间线",
             "chapter": "trace-event-timeline", "required": True},
            {"id": "trace-event-table", "kind": "table", "title": "表 8-1 Trace 事件明细",
             "chapter": "trace-event-timeline", "required": True},
        ])
    navigation = []
    titles = {}
    if page_kind == "read":
        titles.update(READ_TITLES)
        chapters = [item for item in chapters if item["id"] != "workers"]
        for item in components:
            if item["id"] in {"direct-worker-chart", "direct-worker-table"}:
                item["chapter"] = "overview"
            elif item["id"] in {"urma-source-chart", "urma-source-table"}:
                item["chapter"] = "urma-analysis"
        legacy_trace_sections = {"traces", "trace-detail-panel", "trace-log-panel"}
        for item in chapters + components:
            if item.get("chapter", item["id"]) in legacy_trace_sections:
                item["title"] = re.sub(r"^(表|日志框) 8-", r"\1 7-", item["title"])
        for item in chapters:
            if item["id"] == "traces":
                item["title"] = re.sub(r"^8\.", "7.", item["title"])
        last_chapter = 0
        for item in chapters:
            match = re.match(r"^(\d+)\.", item["title"])
            if match:
                last_chapter = max(last_chapter, int(match[1]))
        for item in chapters:
            if item["id"] == "source-logic":
                item["title"] = re.sub(r"^附录 \d+\.", f"附录 {last_chapter + 1}.", item["title"])
    if page_kind == "write":
        titles["selected-stage-table"] = "表 6-2 互斥阶段明细"
        titles.update({key: item[0] for key, item in WRITE_TABLE_CONTAINERS.items()})
    for item in chapters + components:
        item["title"] = titles.get(item["id"], item["title"])
    chapter_numbers = {}
    for item in chapters:
        match = re.match(r"\d+", item["title"])
        if match:
            chapter_numbers[item["id"]] = match.group()
    counters = {}
    for item in components:
        match = re.match(r"^(图|表)\s*(\d+)-(\d+)", item["title"])
        if match:
            key = (match[1], match[2])
            counters[key] = max(counters.get(key, 0), int(match[3]))
    for item in components:
        if not re.match(r"^(图|表)\s*\d", item["title"]):
            kind = "图" if item["kind"] == "chart" else "表"
            chapter = chapter_numbers.get(item["chapter"], "1")
            key = (kind, chapter)
            counters[key] = counters.get(key, 0) + 1
            label = {"read-flow-chart": "读取流程时延分解", "write-flow-chart": "写入流程时延分解",
                     "selected-stage-table": "Trace 阶段明细", "rpc-summary-table": "全 RPC 汇总"}.get(
                         item["id"], re.sub(r"^\d+\.\s*", "", item["title"]))
            item["title"] = f"{kind} {chapter}-{counters[key]} {label}"
    for item in components:
        if page_kind in {"read", "write"} and item["id"] in {"trace-event-chart", "trace-event-table"}:
            item["data_contract"] = {"source": "trace_events", "collection": "events",
                                     "model_fields": ["offset_ms"], "value_kind": "number",
                                     "empty_when": "source_empty_or_scope_empty"}
        contract = DATA_CONTRACTS.get(page_kind, {}).get(item["id"])
        if contract:
            source, collection, fields, value_kind = contract
            item["data_contract"] = {
                "source": source, "collection": collection, "model_fields": fields,
                "value_kind": value_kind, "empty_when": "source_empty_or_scope_empty",
            }
    if page_kind == "write":
        chapter_titles = {item["id"]: item["title"] for item in chapters}
        component_titles = {item["id"]: item["title"] for item in components}
        for node in inventory.nodes:
            identity = node.attrs.get("id")
            if identity in chapter_titles:
                navigation.append({"id": identity, "title": chapter_titles[identity], "sub": False})
            elif identity in component_titles:
                navigation.append({"id": identity, "title": component_titles[identity], "sub": True})
            elif identity in {"detail", "logs-panel"}:
                navigation.append({"id": identity, "title": _heading(node), "sub": True})
    return {"schema_version": 1, "page_kind": page_kind, "chapters": chapters, "navigation": navigation,
            "components": components, "dynamic_families": ["wr-timeline", "trace-stages"]}


def embed_registry(page: str, page_kind: str) -> str:
    registry = build_registry(page, page_kind)
    payload = json.dumps(registry, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    script = '<script id="report-registry">window.REPORT_COMPONENT_REGISTRY=' + payload + ';\n'
    script += asset_path("report_registry.js").read_text(encoding="utf-8") + '</script>'
    if "</head>" not in page:
        raise ValueError("report registry requires a document head")
    return page.replace("</head>", script + "</head>", 1)
