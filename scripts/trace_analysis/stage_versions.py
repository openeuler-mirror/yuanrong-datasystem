"""Explicit implementation dependencies for model caches, excluding presentation assets."""
import ast
import hashlib
from .resources import PACKAGE_ROOT

DEPENDENCIES = {
    "triage": ("triage.py", "ingest", "orchestration/contracts.py", "orchestration/store.py",
               "analysis/triage_accumulator.py", "analysis/triage_artifacts.py",
               "analysis/triage_builder.py", "analysis/triage_dimensions.py",
               "analysis/triage_flow.py", "analysis/triage_stats.py", "analysis/triage_ub.py",
               "analysis/ub_edges.py", "resources.py"),
    "evidence": ("evidence", "resources.py"),
    "read": ("bottleneck.py", "evidence", "analysis/read.py", "analysis/read_initial.py",
             "analysis/read_model.py", "analysis/read_rows.py",
             "analysis/issues.py", "analysis/write_base.py",
             "analysis/aggregation.py",
             "analysis/correlation.py", "analysis/contracts.py", "analysis/stats.py",
             "analysis/budget.py", "diagnosis.py", "resources.py"),
    "write": ("analysis/write.py", "analysis/write_base.py", "analysis/write_pipeline.py",
              "analysis/budget.py", "evidence", "diagnosis.py", "resources.py"),
    "numa": ("numa.py", "archive.py", "evidence", "resources.py"),
    "issues": ("analysis/issue_model.py", "resources.py"),
}


_READ_RENDER_FUNCTIONS = {"render_html", "write_outputs", "main"}


def _model_source(stage, item, source):
    if stage != "read" or item.name != "bottleneck.py":
        return source
    tree = ast.parse(source)
    model_nodes = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in _READ_RENDER_FUNCTIONS:
            continue
        model_nodes.append(node)
    tree.body = model_nodes
    return ast.dump(tree, include_attributes=False).encode()


def model_version(stage, root=PACKAGE_ROOT):
    digest = hashlib.sha256()
    for name in (*DEPENDENCIES[stage], "stages.py", "stage_versions.py", "validation.py"):
        path = root / name
        files = sorted(path.rglob("*.py")) if path.is_dir() else [path]
        for item in files:
            digest.update(item.relative_to(root).as_posix().encode())
            digest.update(_model_source(stage, item, item.read_bytes()))
    return digest.hexdigest()
