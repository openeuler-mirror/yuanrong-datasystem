"""The standalone Trace wheel must import on the supported Python 3.9 floor."""
from trace_test_loader import REPO_ROOT

import ast
from pathlib import Path


PACKAGE = REPO_ROOT / "scripts" / "trace_analysis"


def test_union_annotations_are_deferred_for_python_39():
    for path in PACKAGE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        deferred = any(isinstance(node, ast.ImportFrom) and node.module == "__future__"
                       and any(alias.name == "annotations" for alias in node.names)
                       for node in tree.body)
        if deferred:
            continue
        annotations = [node.annotation for node in ast.walk(tree)
                       if isinstance(node, (ast.AnnAssign, ast.arg))]
        annotations.extend(node.returns for node in ast.walk(tree)
                           if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)))
        assert not any(isinstance(part, ast.BinOp) and isinstance(part.op, ast.BitOr)
                       for annotation in annotations if annotation is not None
                       for part in ast.walk(annotation)), str(path)
