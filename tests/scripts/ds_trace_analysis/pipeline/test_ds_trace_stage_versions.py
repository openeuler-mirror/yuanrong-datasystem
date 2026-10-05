"""Model cache versions include every producer while ignoring read-page wrappers."""
from trace_test_loader import REPO_ROOT

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(REPO_ROOT / "scripts"))

from trace_analysis import stage_versions


def _clone_dependencies(tmp_path):
    root = tmp_path / "trace_analysis"
    root.mkdir()
    names = set(stage_versions.DEPENDENCIES["read"]) | {
        "stages.py", "stage_versions.py", "validation.py"}
    for name in names:
        source = stage_versions.PACKAGE_ROOT / name
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            shutil.copy2(source, target)
    return root


def test_read_model_ignores_only_render_wrappers(tmp_path):
    root = _clone_dependencies(tmp_path)
    before = stage_versions.model_version("read", root)
    bottleneck = root / "bottleneck.py"
    source = bottleneck.read_text()
    bottleneck.write_text(source.replace(
        'template=HTML_TEMPLATE, contract_error=InputContractError, view_top=view_top,',
        'template=HTML_TEMPLATE, contract_error=InputContractError, view_top=0,'))
    assert stage_versions.model_version("read", root) == before
    read_rules = root / "analysis/read_initial.py"
    read_source = read_rules.read_text()
    assert 'client_ms = client_us / 1000.0' in read_source
    read_rules.write_text(read_source.replace('client_ms = client_us / 1000.0',
                                              'client_ms = client_us / 1001.0'))
    assert stage_versions.model_version("read", root) != before


def test_read_row_builder_invalidates_read_model(tmp_path):
    root = _clone_dependencies(tmp_path)
    before = stage_versions.model_version("read", root)
    builder = root / "analysis/read_rows.py"
    builder.write_text(builder.read_text() + "\nREAD_ROW_CHANGE = True\n")
    assert stage_versions.model_version("read", root) != before


def test_read_model_assembly_invalidates_read_cache(tmp_path):
    root = _clone_dependencies(tmp_path)
    before = stage_versions.model_version("read", root)
    assembly = root / "analysis/read_model.py"
    assembly.write_text(assembly.read_text() + "\nREAD_MODEL_CHANGE = True\n")
    assert stage_versions.model_version("read", root) != before


def test_write_budget_and_error_interpreter_invalidate_read_model(tmp_path):
    root = _clone_dependencies(tmp_path)
    before = stage_versions.model_version("read", root)
    for relative in ("analysis/write_base.py", "analysis/issues.py", "evidence/errors.py"):
        target = root / relative
        original = target.read_text()
        target.write_text(original + "\nCACHE_SEMANTIC_CHANGE = True\n")
        assert stage_versions.model_version("read", root) != before
        target.write_text(original)


def test_evidence_parser_change_invalidates_evidence_stage(tmp_path):
    root = _clone_dependencies(tmp_path)
    before = stage_versions.model_version("evidence", root)
    parser = root / "evidence/read.py"
    parser.write_text(parser.read_text() + "\nEVIDENCE_RULE_CHANGE = True\n")
    assert stage_versions.model_version("evidence", root) != before
