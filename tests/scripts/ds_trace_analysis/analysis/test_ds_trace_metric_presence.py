"""Metric discovery preserves the legacy substring contract in one traversal."""
from trace_test_loader import REPO_ROOT
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(REPO_ROOT / "scripts"))
from trace_analysis import validation


@pytest.mark.parametrize('value', [
    None, 0, True, '', 'rpcopy URMA Timeout ERRORs',
    {'RPC_ms': 1, 'nested': [{'COPY': None}, ['URMA', {'x': 'timeout errors'}]]},
    {'values': [1, False, None, 'rpc'], 23: {'messages': ['ERRORS']}},
    {'rpc': None, 'copy': [], 'urma': {}, 'timeout': False, 'errors': 0},
    {'nested': [[], {}, None, ['absent']]},
    {'tuple_scalar': ('COPY', 'rpc')},
])
def test_presence_matches_legacy_for_every_metric(value):
    expected = {name: validation._contains(value, name) for name in validation.METRICS}
    assert validation.metric_presence(value) == expected


def test_all_metrics_short_circuit_without_visiting_rest():
    class Unreadable:
        def __str__(self):
            raise AssertionError('already satisfied traversal must stop')
    value = ['rpc copy urma timeout errors', Unreadable()]
    assert all(validation.metric_presence(value).values())


def test_validation_uses_single_pass_discovery(monkeypatch):
    def reject(*args):
        raise AssertionError('legacy repeated traversal was called')
    monkeypatch.setattr(validation, '_contains', reject)
    result = validation.validate_data({'traces': [], 'aggregate': {}}, 'bottleneck', 'fixture')
    assert result['metric_presence'] == dict.fromkeys(validation.METRICS, False)
