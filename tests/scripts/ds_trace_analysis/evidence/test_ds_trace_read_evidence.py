"""GET observation extraction keeps source facts separate from stage budgets."""

from trace_analysis.evidence.read import extract_read_observations
from trace_analysis.evidence.rpc import _rpc_fields


def test_read_observations_dedupe_display_lines_and_parse_urma_formats():
    client = "0 | DS_KV_CLIENT_GET | 12000 | 4096 | transportType:UB"
    trace = {
        "evidence": [
            {"text": f"first | {client}", "worker": "client-a"},
            {"text": f"second | {client}", "worker": "client-a"},
            {"text": "first | 0 | DS_POSIX_REMOTE_GET | 7000 | 4096 |", "worker": "worker-b"},
            {"text": "first | [URMA_ELAPSED_TOTAL] urma post to completion cost: 2.635ms",
             "worker": "worker-b"},
            {"text": "first | [URMA_ELAPSED_TOTAL] urma post to completion cost 2635us",
             "worker": "worker-b"},
        ],
    }

    facts = extract_read_observations(trace)

    assert len(facts.texts) == 4
    assert (facts.client_us, facts.worker_us, facts.size_bytes) == (12000, 7000, 4096)
    assert (facts.client_observer, facts.direct_data_worker) == ("client-a", "worker-b")
    assert facts.transport == "UB"
    assert facts.explicit_remote
    assert facts.urma_values == [2.635, 2.635]
    assert facts.urma_source_costs == {"worker-b": 2.635}


def test_rpc_field_filter_preserves_rpc_contract():
    assert _rpc_fields("method=service.Get no duration") == (None, {})
    assert _rpc_fields("e2e_us=2000 no method") == (None, {})
    assert _rpc_fields("method=service.Get e2e_us=2000 network_residual_us=400") == (
        "service.Get", {"e2e": 2000, "network_residual": 400}
    )
