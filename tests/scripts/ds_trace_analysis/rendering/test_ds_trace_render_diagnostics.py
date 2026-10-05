"""Render-only attempts expose diagnostics independently of the published generation."""
import json

import pytest

from test_ds_trace_stage_resume import make_case
from trace_analysis import pipeline, render_bundle
from trace_analysis.orchestration.publication import current_publication


def test_render_attempt_is_pending_before_render_and_success_names_real_publication(tmp_path, monkeypatch):
    manifest, root = make_case(tmp_path)
    pipeline.run_pipeline(manifest, root, False)
    render = render_bundle.render_run
    gate = root / 'render.validation.json'
    def observed(config, directory):
        pending = json.loads(gate.read_text())
        assert pending['valid'] is False
        assert pending['status'] == 'running'
        return render(config, directory)
    monkeypatch.setattr(render_bundle, 'render_run', observed)
    result = render_bundle.render_bundle(root)
    diagnostic = json.loads(gate.read_text())
    assert diagnostic['valid'] is True
    assert diagnostic['status'] == 'complete'
    assert diagnostic['index'] == result['index']
    assert diagnostic['manifest'] == result['manifest']
    assert current_publication(root)['index'].as_posix() == diagnostic['index']


@pytest.mark.parametrize('failure', [RuntimeError('render failure'), KeyboardInterrupt('interrupted')])
def test_failed_render_attempt_records_error_and_preserves_old_publication(tmp_path, monkeypatch, failure):
    manifest, root = make_case(tmp_path)
    pipeline.run_pipeline(manifest, root, False)
    old = current_publication(root)
    frozen = {path: path.read_bytes() for path in old['directory'].rglob('*') if path.is_file()}
    pointer = (root / 'index.html').read_bytes()
    def broken(*args):
        raise failure
    monkeypatch.setattr(render_bundle, 'render_run', broken)
    with pytest.raises(type(failure)):
        render_bundle.render_bundle(root)
    diagnostic = json.loads((root / 'render.validation.json').read_text())
    assert diagnostic['valid'] is False
    assert diagnostic['status'] == 'failed'
    assert str(failure) in diagnostic['errors'][0]
    assert (root / 'index.html').read_bytes() == pointer
    assert all(path.read_bytes() == content for path, content in frozen.items())


def test_manifest_failure_is_recorded_before_render_starts(tmp_path):
    with pytest.raises(FileNotFoundError):
        render_bundle.render_bundle(tmp_path)
    diagnostic = json.loads((tmp_path / 'render.validation.json').read_text())
    assert diagnostic['status'] == 'failed'
    assert 'FileNotFoundError' in diagnostic['errors'][0]
