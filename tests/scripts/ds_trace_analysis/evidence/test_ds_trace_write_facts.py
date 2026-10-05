"""Persist write observations without changing attribution or legacy entry points."""
from trace_test_loader import REPO_ROOT
import copy
import importlib.util
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(REPO_ROOT / 'scripts'))
from trace_analysis.analysis import write
from trace_analysis import bottleneck


def evidence():
    prefix = '2026-01-01T00:00:00.000001 | I | file.cpp:1 | 192.0.2.1 | 1:2 | trace | zone | '
    return [prefix + '0 | DS_KV_CLIENT_SET | 10000 | 4 |',
            prefix + '[Client/WorkerRpc] Create done, costUs: 1000',
            prefix + '[Client/WorkerRpc] Create done, costUs: 1000',
            prefix.replace('000001', '000002') + '[Client/WorkerRpc] Create done, costUs: 2000',
            prefix + '[Client/WorkerRpc] Publish done, costUs: 500',
            prefix + 'Cannot assign requested address Create->[192.0.2.3:10]',
            prefix.replace('192.0.2.1', '192.0.2.2') + '0 | DS_POSIX_PUBLISH | 800 |',
            prefix.replace('000001', '000003') + '1 | DS_KV_CLIENT_PUBLISH | 20000 | 8 |']


def row():
    return {'trace_id': 'trace', 'timestamp': 'fallback', 'evidence': evidence(), 'client_ms': 10,
            'status': 0, 'create_rpc_ms': 0, 'publish_rpc_ms': 0, 'write_urma_ms': None,
            'write_breakdown_ms': {'未解释残差': 10, 'Create RPC其他': 0, 'Publish RPC其他': 0}}


def test_persisted_facts_are_used_without_reinterpreting_raw(monkeypatch):
    from trace_analysis.evidence.write import build_write_facts
    source = row()
    expected = write.refine(source)
    source['write_evidence_facts'] = build_write_facts(source['trace_id'], source['evidence'])
    before = copy.deepcopy(source)
    monkeypatch.setattr('trace_analysis.evidence.write.build_write_facts',
                        lambda *args: pytest.fail('persisted facts must not reparse'))
    actual = write.refine(source)
    assert source == before
    assert {k: v for k, v in actual.items() if k != 'write_evidence_facts'} == {
        k: v for k, v in expected.items() if k != 'write_evidence_facts'}
    assert len(actual['observed_parents']) == 3
    assert actual['create_rpc_ms'] == 0
    assert actual['publish_rpc_ms'] == .5
    assert actual['operation'] == 'SET'
    assert actual['client_timestamp'].endswith('000001')


def test_first_client_summary_is_independent_of_last_identity():
    output = bottleneck._build_write_row(row(), {})
    assert 'write_evidence_facts' in output
    assert output['client_ms'] == 10 and output['size_bytes'] == 4
    assert write.refine(output)['operation'] == 'SET'


def test_set_preserves_unobserved_create_and_observed_publish_on_one_trace():
    source = row()
    source['evidence'] = [line for line in source['evidence'] if 'Create done' not in line]
    result = write.refine(bottleneck._build_write_row(source, {}))
    assert result['operation'] == 'SET'
    assert result['write_phase_observation']['Create'] == {
        'state': 'unobserved', 'parent_ms': None, 'source': None,
    }
    assert result['write_phase_observation']['Publish'] == {
        'state': 'observed', 'parent_ms': .5, 'source': 'client_worker_rpc',
    }
    assert result['wr_applicable'] is True
    assert result['wr_phase_attribution'] == {'phase': 'unconfirmed', 'basis': 'wr_callsite_not_observed'}


def test_explicit_zero_create_window_remains_observed():
    source = row()
    source['evidence'] = [source['evidence'][0]]
    result = write.refine(bottleneck._build_write_row(
        source, {'latency_summary_us': {'client.rpc.create_total': 0}}))
    assert result['write_phase_observation']['Create'] == {
        'state': 'observed', 'parent_ms': 0.0, 'source': 'latency_summary',
    }


def test_explicit_zero_total_window_takes_precedence_over_fallback_tick():
    source = row()
    source['evidence'] = [source['evidence'][0]]
    result = write.refine(bottleneck._build_write_row(source, {
        'latency_summary_us': {
            'client.rpc.create_total': 0, 'client.rpc.create': 1000,
            'client.rpc.publish_total': 0, 'client.rpc.publish': 2000,
        },
    }))
    assert result['write_phase_observation']['Create']['parent_ms'] == 0
    assert result['write_phase_observation']['Publish']['parent_ms'] == 0


def test_set_copy_window_is_a_separate_observation():
    source = row()
    result = write.refine(bottleneck._build_write_row(
        source, {'latency_summary_us': {'client.process.memory_copy': 1500}}))
    assert result['write_phase_observation']['Copy'] == {
        'state': 'observed', 'parent_ms': 1.5, 'source': 'latency_summary',
    }


def test_create_and_publish_rpc_split_retains_distinct_source_references():
    source = row()
    source['evidence'] += [
        '[BRPC_RPC_FRAMEWORK_SLOW] method=svc.Create e2e_us=2000 '
        'network_residual_us=100 server_req_queue_us=200 server_exec_us=1500',
        '[BRPC_RPC_FRAMEWORK_SLOW] method=svc.Publish e2e_us=3000 '
        'network_residual_us=700 server_req_queue_us=100 server_exec_us=1800',
    ]
    result = write.refine(bottleneck._build_write_row(source, {}))
    calls = result['write_rpc_phase_evidence']
    assert calls['Create'] == {
        'state': 'observed', 'method': 'svc.Create',
        'e2e_ms': 2, 'network_ms': .1, 'queue_ms': .2, 'framework_ms': .2,
        'source_ref': {'collection': 'evidence', 'index': 8},
    }
    assert calls['Publish'] == {
        'state': 'observed', 'method': 'svc.Publish',
        'e2e_ms': 3, 'network_ms': .7, 'queue_ms': .1, 'framework_ms': .4,
        'source_ref': {'collection': 'evidence', 'index': 9},
    }


def test_failed_rpc_without_server_timing_remains_unobserved():
    source = row()
    source['evidence'].append(
        '[BRPC_RPC_FRAMEWORK_SLOW] method=svc.Create e2e_us=2000 '
        'network_residual_us=0 server_req_queue_us=0 server_exec_us=0 '
        'cntl_error_code=1008 cntl_failed=1'
    )
    result = write.refine(bottleneck._build_write_row(source, {}))
    assert result['write_rpc_phase_evidence']['Create']['state'] == 'unobserved'


def test_inconsistent_rpc_component_total_remains_unobserved():
    source = row()
    source['evidence'].append(
        '[BRPC_RPC_FRAMEWORK_SLOW] method=svc.Publish e2e_us=1000 '
        'network_residual_us=900 server_req_queue_us=200 server_exec_us=500'
    )
    result = write.refine(bottleneck._build_write_row(source, {}))
    assert result['write_rpc_phase_evidence']['Publish']['state'] == 'unobserved'
    assert bottleneck._write_rpc_split(1, [{
        'e2e': 1000, 'network_residual': 900,
        'server_req_queue': 200, 'server_exec': 500,
    }]) == {'other': 1, 'queue': 0, 'network': 0, 'framework': 0}


def test_create_only_request_is_not_a_publish_wr_candidate():
    source = row()
    source['evidence'] = [source['evidence'][0].replace('DS_KV_CLIENT_SET', 'DS_KV_CLIENT_CREATE')]
    result = write.refine(bottleneck._build_write_row(source, {}))
    assert result['operation'] == 'CREATE'
    assert result['write_phase_observation']['Copy']['state'] == 'not_applicable'
    assert result['write_phase_observation']['Publish']['state'] == 'not_applicable'
    assert result['wr_applicable'] is False
    assert result['wr_phase_attribution'] == {'phase': 'not_applicable', 'basis': 'create_only'}


def test_bound_route_alone_does_not_locate_wr_callsite():
    source = row()
    source['write_route'] = 'bound'
    source['write_route_source'] = 'fixture:route-evidence'
    result = write.refine(bottleneck._build_write_row(source, {}))
    assert result['wr_phase_attribution'] == {'phase': 'unconfirmed', 'basis': 'wr_callsite_not_observed'}


def test_explicit_wr_callsite_attributes_wr_to_the_source_phase():
    from trace_analysis.validation import _validate_write_rows
    for callsite, expected in [('buffer.memory_copy_ub', 'Copy'),
                               ('buffer.publish_ub', 'Publish'),
                               ('ub_transporter.set', 'Publish')]:
        source = row()
        source['write_wr_callsite'] = callsite
        source['write_wr_callsite_source'] = 'fixture:callsite-evidence'
        result = write.refine(bottleneck._build_write_row(source, {}))
        assert result['wr_phase_attribution'] == {
            'phase': expected, 'basis': 'fixture:callsite-evidence',
        }
        _validate_write_rows(
            {'schema_version': 1, 'write_phase_schema_version': 1, 'rows': [result]},
            {'trace'},
        )


def test_write_phase_contract_rejects_false_zero_and_unproven_route():
    from trace_analysis.validation import _validate_write_rows
    result = write.refine(bottleneck._build_write_row(row(), {}))
    model = {'schema_version': 1, 'write_phase_schema_version': 1, 'rows': [result]}
    _validate_write_rows(model, {'trace'})
    result['write_phase_observation']['Copy']['parent_ms'] = 0
    with pytest.raises(ValueError, match='unobserved'):
        _validate_write_rows(model, {'trace'})
    result['write_phase_observation']['Copy']['parent_ms'] = None
    result['wr_phase_attribution'] = {'phase': 'Publish', 'basis': 'wr_callsite_not_observed'}
    with pytest.raises(ValueError, match='WR phase'):
        _validate_write_rows(model, {'trace'})


def test_write_phase_contract_requires_boolean_wr_applicability():
    from trace_analysis.validation import _validate_write_phases
    result = write.refine(bottleneck._build_write_row(row(), {}))
    _validate_write_phases(result)
    result['wr_applicable'] = 1
    with pytest.raises(ValueError, match='WR applicability is invalid'):
        _validate_write_phases(result)


def test_write_phase_duration_accepts_numeric_subclass_but_rejects_bool():
    from trace_analysis.validation import _validate_write_phases

    class Milliseconds(float):
        pass

    result = write.refine(bottleneck._build_write_row(row(), {}))
    observation = result['write_phase_observation']['Create']
    observation.update(state='observed', parent_ms=Milliseconds(1.25))
    _validate_write_phases(result)
    observation['parent_ms'] = True
    with pytest.raises(ValueError, match='write phase duration is invalid'):
        _validate_write_phases(result)


def test_phase_rpc_evidence_contract_checks_source_and_nonnegative_durations():
    from trace_analysis.validation import _validate_write_rows
    source = row()
    source['evidence'].append(
        '[BRPC_RPC_FRAMEWORK_SLOW] method=svc.Create e2e_us=2000 '
        'network_residual_us=100 server_req_queue_us=200 server_exec_us=1500'
    )
    result = write.refine(bottleneck._build_write_row(source, {}))
    model = {'schema_version': 1, 'write_phase_schema_version': 2, 'rows': [result]}
    _validate_write_rows(model, {'trace'})
    result['write_rpc_phase_evidence']['Create']['source_ref']['index'] = 999
    with pytest.raises(ValueError, match='RPC phase source'):
        _validate_write_rows(model, {'trace'})
    result['write_rpc_phase_evidence']['Create']['source_ref']['index'] = 8
    result['write_rpc_phase_evidence']['Create']['network_ms'] = -1
    with pytest.raises(ValueError, match='RPC phase duration'):
        _validate_write_rows(model, {'trace'})


def test_rpc_phase_duration_accepts_numeric_subclass_but_rejects_bool():
    from trace_analysis.validation import _validate_write_rows

    class Milliseconds(float):
        pass

    source = row()
    source['evidence'].append(
        '[BRPC_RPC_FRAMEWORK_SLOW] method=svc.Create e2e_us=2000 '
        'network_residual_us=100 server_req_queue_us=200 server_exec_us=1500'
    )
    result = write.refine(bottleneck._build_write_row(source, {}))
    model = {'schema_version': 1, 'write_phase_schema_version': 2, 'rows': [result]}
    phase = result['write_rpc_phase_evidence']['Create']
    phase['network_ms'] = Milliseconds(phase['network_ms'])
    _validate_write_rows(model, {'trace'})
    phase['network_ms'] = True
    with pytest.raises(ValueError, match='RPC phase duration differs from evidence'):
        _validate_write_rows(model, {'trace'})


@pytest.mark.parametrize('damage', ['schema', 'source', 'ref', 'nan', 'missing'])
def test_present_invalid_facts_never_fallback(damage):
    from trace_analysis.evidence.write import build_write_facts
    source = row()
    facts = build_write_facts('trace', source['evidence'])
    source['write_evidence_facts'] = facts
    if damage == 'schema': facts['schema_version'] = 99
    elif damage == 'source': source['evidence'][0] += 'changed'
    elif damage == 'ref': facts['parents']['Create'][0]['source_ref']['index'] = 999
    elif damage == 'nan': facts['parents']['Create'][0]['ms'] = float('nan')
    else: del facts['identity']
    with pytest.raises(ValueError, match='write_evidence_facts'):
        write.refine(source)
    with pytest.raises(ValueError, match='write_evidence_facts'):
        bottleneck._build_write_row(source, {})


def test_budget_consumes_valid_facts_without_rpc_regex(monkeypatch):
    from trace_analysis.evidence import write as observations
    source = row()
    source['write_evidence_facts'] = observations.build_write_facts('trace', source['evidence'])
    monkeypatch.setattr(observations, '_rpc_fields', lambda *args: pytest.fail('RPC parsed twice'))
    monkeypatch.setattr(observations, 'build_write_facts', lambda *args: pytest.fail('facts rebuilt'))
    output = bottleneck._build_write_row(source, {})
    assert output['client_ms'] == 10
    assert write.refine(output)['publish_rpc_ms'] == .5


def test_parent_cost_text_identity_and_last_duplicate_reference():
    from trace_analysis.evidence.write import build_write_facts
    source = row()
    first = source['evidence'][1]
    source['evidence'] = [first, first + ' duplicate', first.replace('1000', '01000')]
    facts = build_write_facts('trace', source['evidence'])
    assert facts['parents']['Create'] == [
        {'ms': 1, 'source_ref': {'collection': 'evidence', 'index': 1}},
        {'ms': 1, 'source_ref': {'collection': 'evidence', 'index': 2}}]
    source['write_evidence_facts'] = facts
    assert write.refine(source)['create_rpc_ms'] == 0


def test_model_validators_reject_invalid_facts():
    from trace_analysis.evidence.write import build_write_facts
    from trace_analysis.validation import validate_data, validate_write_data
    source = row()
    source['write_evidence_facts'] = build_write_facts('trace', source['evidence'])
    source['write_evidence_facts']['schema_version'] = 99
    checked = validate_data({'traces': [], 'aggregate': {}, 'metadata': {}, 'write_traces': [source]},
                            'bottleneck', 'fixture')
    assert not checked['valid']
    assert any('write_evidence_facts' in error for error in checked['errors'])
    with pytest.raises(ValueError, match='write_evidence_facts'):
        validate_write_data({'rows': [source]}, {'write_traces': [source]})


def test_legacy_refinement_all_fields_match_frozen_output():
    import json
    expected = json.loads((Path(__file__).resolve().parents[1] / 'fixtures/write-facts-legacy.json').read_text())
    actual = write.refine(row())
    actual.pop('write_evidence_facts')
    assert actual == expected


def test_rpc_method_filter_order_duplicates_and_tie_preserved():
    from trace_analysis.evidence.write import build_write_facts, rpc_group
    prefix = '[BRPC_RPC_FRAMEWORK_SLOW] method=svc.'
    first = ' e2e_us=2000 network_residual_us=100 server_req_queue_us=200 server_exec_us=1500'
    second = ' e2e_us=2000 network_residual_us=700 server_req_queue_us=100 server_exec_us=1000'
    lines = [prefix + 'Create' + first, prefix + 'CreateMeta' + first,
             prefix + 'Publish' + first, prefix + 'Create' + second, prefix + 'Create' + first]
    facts = build_write_facts('trace', lines)
    fields = rpc_group(facts, 'create')
    assert len(fields) == 3
    assert [entry['network_residual'] for entry in fields] == [100, 700, 100]
    assert fields == bottleneck._write_rpc_group(lines, 'create')
    assert rpc_group(facts, 'unknown') == bottleneck._write_rpc_group(lines, 'unknown') == []
    assert bottleneck._write_rpc_split(2, fields)['network'] == .1


@pytest.mark.parametrize('field', ['rpc_entries', 'parents', 'identity', 'issues'])
@pytest.mark.parametrize('bad_value', [None, 'malformed', 3])
def test_malformed_fact_collections_report_validation_errors(field, bad_value):
    from trace_analysis.evidence.write import build_write_facts, write_fact_errors
    source = row()
    source['write_evidence_facts'] = build_write_facts('trace', source['evidence'])
    source['write_evidence_facts'][field] = bad_value
    assert write_fact_errors(source)


def test_write_model_cache_version_includes_fact_parser():
    from trace_analysis.stage_versions import DEPENDENCIES
    assert 'evidence' in DEPENDENCIES['read']
    assert 'evidence' in DEPENDENCIES['write']


def test_empty_worker_host_does_not_drop_observed_event():
    source = row()
    source['evidence'] = ['2026-01-01T00:00:00.000001 | I | f:1 |  | 1:2 | trace | zone | '
                          '0 | DS_POSIX_CREATE | 500 |']
    actual = write.refine(source)
    assert actual['worker_observers'] == ['']
    assert actual['worker_events'][0]['ms'] == .5
