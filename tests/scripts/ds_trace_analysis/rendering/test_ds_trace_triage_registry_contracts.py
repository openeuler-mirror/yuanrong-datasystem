"""Every triage chart/table has an auditable source, independent of DOM output."""
from trace_test_loader import REPO_ROOT
import re
import sys
from pathlib import Path

sys.path.insert(0, str(REPO_ROOT / 'scripts'))
from trace_analysis.rendering.registry import build_registry

TEMPLATE = REPO_ROOT / 'scripts/trace_analysis/assets/triage/triage.html'


def test_all_triage_components_have_source_contracts_and_unique_captions():
    registry = build_registry(TEMPLATE.read_text(), 'triage')
    components = registry['components']
    assert len(components) == 47
    assert all(item.get('data_contract') for item in components)
    numbers = [re.match(r'(图|表)\s*(\d+-\d+)', item['title']).group(0) for item in components]
    assert len(numbers) == len(set(numbers))


def test_dynamic_selected_and_worker_views_have_distinct_scope_sources():
    entries = {item['id']: item['data_contract']
               for item in build_registry(TEMPLATE.read_text(), 'triage')['components']}
    assert entries['top-trace-table']['source'] == 'triage_traces'
    assert entries['selected-event-timeline']['source'] == 'triage_selected_events'
    assert entries['selected-trace-chart']['source'] == 'triage_selected_stages'
    assert entries['read-worker-chart']['source'] != entries['read-worker-table']['source']
    assert entries['ub-worker-role-chart']['source'] != entries['ub-worker-role-table']['source']


def test_triage_chart_captions_are_short_navigation_labels():
    template = TEMPLATE.read_text()
    charts = [item for item in build_registry(template, 'triage')['components']
              if item['kind'] == 'chart']
    assert charts
    assert all('：' not in item['title'] and not item['title'].endswith('。')
               for item in charts)
    assert '<div class="caption">图 3-5 写入时间桶 Breakdown</div>' in template
    assert '<p class="chart-note">柱为写 RPC/本地阶段 p99，线为写 trace access p99。</p>' in template
