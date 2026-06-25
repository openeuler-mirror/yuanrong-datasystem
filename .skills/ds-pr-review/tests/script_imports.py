#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
REVIEW_PR_DEPENDENCIES = (
    "common",
    "comment_formatter",
    "context_builder",
    "dedupe",
    "diff_position",
    "finding_validator",
    "gitcode_api",
    "language_detect",
    "sensitive_scan",
)


def load_script_module(name: str) -> ModuleType:
    if name == "review_pr":
        for dependency in REVIEW_PR_DEPENDENCIES:
            load_script_module(dependency)

    if name in sys.modules:
        return sys.modules[name]

    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {name} from {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
