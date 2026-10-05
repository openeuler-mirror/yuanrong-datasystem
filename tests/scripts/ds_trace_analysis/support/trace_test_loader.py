"""Load an isolated trace package module for tests that patch module globals."""
import importlib.util
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))


def load_fresh(name):
    path = SCRIPTS / "trace_analysis" / f"{name}.py"
    module_name = f"trace_analysis._test_{name}_{id(path)}_{len(sys.modules)}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    return module
