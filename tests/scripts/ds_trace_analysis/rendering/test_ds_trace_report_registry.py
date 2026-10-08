"""Registry declares expected report content before browser rendering."""
from trace_test_loader import REPO_ROOT
import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(REPO_ROOT / "scripts"))


def registry():
    return importlib.import_module("trace_analysis.rendering.registry")


def test_registry_uses_caption_not_duplicated_navigation_title():
    page = '<html><head></head><body><aside id="nav"><a href="#plot">outdated</a></aside><main><section id="one"><h2>1. Overview</h2><h3>图 1-1 Actual title</h3><div id="plot" class="chart"></div><h3>表 1-1 Records</h3><table id="rows"><tbody></tbody></table></section></main></body></html>'
    specs = registry().build_registry(page, "read")
    entries = {item['id']: item for item in specs['components']}
    assert entries['plot']['title'] == '图 1-1 Actual title'
    assert entries['plot']['chapter'] == 'one'
    assert entries['rows']['kind'] == 'table'
    assert entries['rows']['title'] == '表 1-1 Records'
    rendered = registry().embed_registry(page, 'read')
    assert 'REPORT_COMPONENT_REGISTRY' in rendered
    assert 'report-registry' in rendered


def test_duplicate_expected_component_ids_rejected():
    page = '<main><div id="same" class="chart"></div><div id="same" class="chart"></div></main>'
    with pytest.raises(ValueError, match='duplicate'):
        registry().build_registry(page, 'read')


def test_registry_does_not_inventory_markup_inside_data_scripts():
    page = '<main><h2>图 1-1 Actual</h2><div id="real" class="chart"></div></main><script>const data="<div class=chart id=fake></div>";</script>'
    assert [x['id'] for x in registry().build_registry(page, 'triage')['components']] == ['real']


def test_registry_prefers_existing_caption_below_chart():
    page = '<section id="issues"><h2>2. Issues</h2><div id="plot" class="chart"></div><div class="caption">图 2-1 Failure count</div></section>'
    component = registry().build_registry(page, 'overview')['components'][0]
    assert component['title'] == '图 2-1 Failure count'


def test_numa_read_and_write_worker_views_have_separate_data_contracts():
    from trace_analysis.numa import render_html
    page = render_html({'metadata': {}}, '')
    components = {item['id']: item for item in registry().build_registry(page, 'numa')['components']}
    for operation, label, chapter in (('read', '读取', 4), ('write', '写入', 5)):
        for offset, suffix, source in ((0, 'worker-chart', f'numa_{operation}'),
                                       (1, 'time-chart', f'numa_{operation}'),
                                       (2, 'worker-time-chart', f'numa_{operation}_worker_time')):
            chart = components[f'{operation}-{suffix}']
            assert chart['title'].startswith(f'图 {chapter}-{offset + 1} {label} ·')
            assert chart['chapter'] == f'{operation}-worker'
            assert chart['data_contract']['source'] == source


def test_registry_contains_expected_injected_read_charts(tmp_path):
    from test_ds_trace_bottleneck import load_module, run_dir
    import json
    import re
    module = load_module()
    model = module.build_analysis(run_dir.__wrapped__(tmp_path), top_n=0)
    page = module.render_html(model, 'Registry contract')
    payload = json.loads(re.search(r'window.REPORT_COMPONENT_REGISTRY=(.*?);\n', page).group(1))
    ids = {item['id'] for item in payload['components']}
    assert not [item['id'] for item in payload['components'] if not item.get('data_contract')]
    assert {'query-rpc-breakdown-chart', 'query-worker-breakdown-chart',
            'worker-correlation-chart-rpc', 'worker-correlation-chart-ub',
            'worker-correlation-chart-metadata', 'worker-correlation-chart-data',
            'read-selected-stage-chart', 'trace-table'} <= ids


def test_read_appendix_follows_last_body_chapter():
    page = '<main><section id="traces"><h2>8. Trace 查看</h2></section><section id="event-time"><h2>8. Trace 事件时间线</h2></section><section id="source-logic"><h2>附录 9. 源码与访问拓扑</h2></section></main>'
    titles = {item['id']: item['title'] for item in registry().build_registry(page, 'read')['chapters']}
    assert titles['source-logic'].startswith('附录 9.')


def test_lazy_timeline_registered_before_dom_exists():
    page = '<section id="source-logic"><h2>附录 9. 源码与访问拓扑</h2></section><script>section.id=\'trace-event-timeline\'</script>'
    result = registry().build_registry(page, 'read')
    assert {item['id'] for item in result['components']} == {'trace-event-chart', 'trace-event-table'}
    assert next(item['title'] for item in result['chapters'] if item['id'] == 'source-logic').startswith('附录 9.')


def test_static_write_timeline_does_not_duplicate_lazy_registration():
    page = ('<main><section id="trace-event-timeline"><h2>8. Trace 事件时间线</h2>'
            '<div id="trace-event-chart" class="chart"></div><p class="chart-caption">图 8-1 Trace 分进程事件时间线</p>'
            '<h3>表 8-1 Trace 事件明细</h3><table id="trace-event-table"></table>'
            '</section></main><script>section.id=\'trace-event-timeline\'</script>')
    result = registry().build_registry(page, 'write')
    assert [item['id'] for item in result['chapters']] == ['trace-event-timeline']
    assert [item['id'] for item in result['components']] == ['trace-event-chart', 'trace-event-table']


@pytest.mark.parametrize('page_kind,legacy_number', [('read', 7), ('write', 8)])
def test_trace_renumbering_preserves_separate_timeline_chapter(page_kind, legacy_number):
    page = ('<main><section id="traces"><h2>8. Trace 查看</h2>'
            '<h3>表 8-2 旧 Trace 阶段</h3><table id="legacy-stage-table"></table></section>'
            '<section id="source-logic"><h2>附录 9. 源码与访问拓扑</h2></section></main>'
            "<script>section.id='trace-event-timeline'</script>")
    result = registry().build_registry(page, page_kind)
    chapters = {item['id']: item['title'] for item in result['chapters']}
    components = {item['id']: item for item in result['components']}
    assert components['legacy-stage-table']['title'] == f'表 {legacy_number}-2 旧 Trace 阶段'
    assert components['trace-event-table']['chapter'] == 'trace-event-timeline'
    assert components['trace-event-table']['title'] == '表 8-1 Trace 事件明细'
    assert components['trace-event-chart']['title'] == '图 8-1 Trace 分进程事件时间线'
    assert chapters['trace-event-timeline'] == '8. Trace 事件时间线'
    assert chapters['source-logic'].startswith('附录 9.')
