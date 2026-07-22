import importlib.util
import sys
from pathlib import Path


REVIEW_SCRIPT_DIR = Path(__file__).resolve().parents[2] / ".skills" / "ds-pr-review" / "scripts"


def _load_review_pr():
    sys.path.insert(0, str(REVIEW_SCRIPT_DIR))
    spec = importlib.util.spec_from_file_location("review_pr", REVIEW_SCRIPT_DIR / "review_pr.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_allow_unscannable_filter_keeps_real_sensitive_matches():
    review_pr = _load_review_pr()
    from sensitive_scan import SensitiveMatch
    matches = [
        SensitiveMatch(location="scripts/big.py", category="unscannable changed file"),
        SensitiveMatch(location="docs/example.md", category="server IP or endpoint", line=12),
    ]

    filtered = review_pr.filter_sensitive_matches(matches, allow_unscannable=True)

    assert [match.category for match in filtered] == ["server IP or endpoint"]


def test_allow_unscannable_filter_is_disabled_by_default():
    review_pr = _load_review_pr()
    from sensitive_scan import SensitiveMatch
    matches = [SensitiveMatch(location="scripts/big.py", category="unscannable changed file")]

    assert review_pr.filter_sensitive_matches(matches, allow_unscannable=False) == matches
