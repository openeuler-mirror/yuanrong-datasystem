"""Model contracts for triage components and their independently filtered views."""

TRIAGE_DATA_CONTRACTS = {
    'run-metadata-table': ('triage_manifest', 'rows', ['case_name'], 'text'),
    'classification-table': ('triage', 'dimensions.classifications', [''], 'number'),
    'error-table': ('triage', 'dimensions.errors', [''], 'number'),
    'cohort-table': ('triage', 'dimensions.cohorts', ['trace_count'], 'number'),
    'coverage-table': ('triage', 'dimensions.coverage.surfaces', ['events'], 'number'),
    'classification-chart': ('triage', 'dimensions.classifications', [''], 'number'),
    'error-chart': ('triage', 'dimensions.errors', [''], 'number'),
    'cohort-chart': ('triage', 'dimensions.cohorts', ['trace_count'], 'number'),
    'worker-ip-alias-table': ('triage', 'dimensions.worker_ip_mapping', ['worker_full_name'], 'text'),
    'flow-candidate-edge-table': ('triage', 'dimensions.flow_stages.candidate_edges', ['operation'], 'text'),
    'ub-lifecycle-chart': ('triage_ub_latency', 'rows', ['max'], 'number'),
    'ub-wr-count-chart': ('triage_ub_counts', 'rows', ['max'], 'number'),
    'ub-lifecycle-table': ('triage_ub_metrics', 'rows', ['count'], 'number'),
    'ub-request-table': ('triage', 'dimensions.ub_lifecycle_summary.requests', ['trace_id'], 'text'),
    'top-trace-table': ('triage_traces', 'rows', ['classification'], 'text'),
    'selected-event-timeline': ('triage_selected_events', 'rows', ['time'], 'number'),
    'selected-trace-chart': ('triage_selected_stages', 'rows', ['duration_ms'], 'number'),
    'selected-stage-table': ('triage_selected_stage_table', 'rows', ['stage'], 'text'),
    'selected-trace-table': ('triage_selected', 'rows', ['classification'], 'text'),
    'recommendation-table': ('triage', 'dimensions.recommendations', ['title'], 'text'),
}
for _operation in ('read', 'write'):
    for _kind in ('chart', 'table'):
        TRIAGE_DATA_CONTRACTS[f'{_operation}-latency-{_kind}'] = (
            f'triage_{_operation}_latency', 'rows', ['max'], 'number')
        TRIAGE_DATA_CONTRACTS[f'{_operation}-flow-{_kind}'] = (
            f'triage_{_operation}_flow', 'rows', [''], 'number')
        TRIAGE_DATA_CONTRACTS[f'{_operation}-worker-{_kind}'] = (
            f'triage_{_operation}_worker_{_kind}', 'rows', ['workers.*'], 'number')
        TRIAGE_DATA_CONTRACTS[f'{_operation}-ub-edge-{_kind}'] = (
            f'triage_{_operation}_edges', 'rows', ['count'], 'number')
    TRIAGE_DATA_CONTRACTS[f'{_operation}-time-breakdown-chart'] = (
        f'triage_{_operation}_time', 'rows', ['p99_access_ms'], 'number')
    TRIAGE_DATA_CONTRACTS[f'{_operation}-flow-stage-table'] = (
        'triage', f'dimensions.flow_stages.{_operation}.edges', ['name'], 'text')
for _view in ('role', 'time'):
    for _kind in ('chart', 'table'):
        TRIAGE_DATA_CONTRACTS[f'ub-worker-{_view}-{_kind}'] = (
            f'triage_ub_worker_{_view}_{_kind}', 'rows', ['entry_events', 'exit_events'], 'number')
for _scope in ('common', 'read', 'write'):
    TRIAGE_DATA_CONTRACTS[f'source-appendix-{_scope}-table'] = (
        f'triage_appendix_{_scope}', 'rows', ['log_surface'], 'text')
