"""Parsing contracts survive the ingest-module extraction."""
import ast
import hashlib
import importlib
from trace_test_loader import REPO_ROOT, load_fresh
import json
from pathlib import Path
import sys

import pytest

ROOT = REPO_ROOT
sys.path.insert(0, str(ROOT / "scripts"))
LINES = [
    '2026-09-20T14:43:48.578604 | I | urma_manager.cpp:1726 | 172.16.5.0 | 6158:6384 | '
    'getBuffer-70-7612-00000224;62d8affe208f | jingpai | [URMA_ELAPSED_TOTAL] '
    '[urma_request_id:2581] urma post to completion cost: 2.635ms, condition wait: 2.63645ms, '
    'dataSize:4194304, writeChunkIdx:1, writeChunkCnt:2, cpuid:35, status: code: [OK], '
    'firstUrmaWriteWakeSchedLatencyUs:13, waited_for_notification:1',
    '2026-07-18T19:20:03.200000 | WARN | worker | 192.0.2.20 | 42 | '
    'getBuffer-70-7612-00000224;62d8affe208f | [URMA_ELAPSED_TOTAL] cost 517.732ms, request id:77, '
    'src address: 192.0.2.20, target address: 192.0.2.10, dataSize:4194304, cpuid:12, status: OK',
]


def triage_module():
    return load_fresh("triage")


@pytest.mark.parametrize("index,digest", [
    (0, "d6736575545be42a41c961eff3289814dd7f85d513a1d2b9b1755364645c448d"),
    (1, "bd95e321d3165d058330168a6fb2509afef4541ba79a4b45df2b29b6c04e33a9"),
])
def test_ingest_matches_new_and_old_urma_facts(index, digest):
    ingest = importlib.import_module("trace_analysis.ingest.triage")
    row = ingest.TraceParser().parse_line("source", "worker.log", 1, LINES[index])
    assert hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest() == digest
    assert row == triage_module().TraceParser().parse_line("source", "worker.log", 1, LINES[index])


def test_ingest_has_no_analysis_or_render_dependency():
    ingest = importlib.import_module("trace_analysis.ingest.triage")
    tree = ast.parse(Path(ingest.__file__).read_text())
    assert not any(isinstance(node, ast.ImportFrom) and node.level for node in ast.walk(tree))
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                   and node.func.id in {"exec", "eval", "__import__"} for node in ast.walk(tree))


def test_isolated_module_rules_and_parser_injection_stay_local(monkeypatch):
    first, second = triage_module(), triage_module()
    first.register_error_pattern("unique-rule-token")
    assert "unique-rule-token" not in second.DEFAULT_RULES.error_patterns
    monkeypatch.setattr(first, "_extract_ub_events", lambda context: [{"injected": True}])
    row = first.TraceParser().parse_line("source", "worker.log", 1, LINES[0])
    assert row["ub_events"] == [{"injected": True}]
    assert second.TraceParser().parse_line("source", "worker.log", 1, LINES[0])["ub_events"] != row["ub_events"]


@pytest.mark.parametrize("source,expected", [
    ("/collected/client_192.0.2.10/ds_client.INFO.log", "192.0.2.10"),
    ("/collected_worker_logs/worker_192.0.2.20/kvcache.INFO.log", "192.0.2.20"),
    ("/unidentified/log.txt", "unknown"),
])
def test_multiline_trace_continuation_uses_collected_process_identity(source, expected):
    ingest = importlib.import_module("trace_analysis.ingest.triage")
    line = f"{source}:28169:traceId      : getBuffer-70-7612-00000224;62d8affe208f]"
    row = ingest.TraceParser().parse_line("/trace-collect/1001/getBuffer-trace", "getBuffer-trace", 3, line)
    assert row["worker"] == expected
    assert row["evidence"]["worker"] == expected
