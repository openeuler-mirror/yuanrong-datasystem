"""Design P2: package stages return artifacts without invoking compatibility CLIs."""
from trace_test_loader import REPO_ROOT
import ast
from pathlib import Path

ROOT = REPO_ROOT


def test_pipeline_does_not_execute_compatibility_entries_or_scrape_stdout():
    source = (ROOT / "scripts/trace_analysis/pipeline.py").read_text()
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert "subprocess" not in imports, "orchestration must call package stage interfaces"
    assert "scripts/ds_trace_" not in source
    assert "last_existing_path" not in source, "artifact paths must be explicit stage results"
