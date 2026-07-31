#!/usr/bin/env python3
"""ds-design script regression tests.

Covers the three scripts (self_check / mermaid_lint / scope_check), using fixtures
to verify that compliant samples pass and violating samples fail. Uses unittest,
so no pytest dependency is required.

Usage:
  python test_checks.py
"""

import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(HERE, '..', 'scripts')
FIXTURES = os.path.join(HERE, 'fixtures')
PYTHON = sys.executable


def run_script(script: str, *args) -> tuple:
    """Run a script; returns (returncode, stdout)."""
    result = subprocess.run(
        [PYTHON, os.path.join(SCRIPTS, script)] + list(args),
        capture_output=True, text=True
    )
    return result.returncode, result.stdout + result.stderr


class TestSelfCheckOverview(unittest.TestCase):

    def test_overview_good_passes(self):
        """Compliant overview sample: self_check should exit 0."""
        rc, out = run_script('self_check.py',
                             os.path.join(FIXTURES, 'overview_good.md'),
                             '--type', 'overview')
        self.assertEqual(rc, 0, f'expected pass but exited {rc}\n{out}')
        self.assertIn('0 BLOCK', out)

    def test_overview_section_mode(self):
        """Per-section mode §1: should only emit §1-related items."""
        rc, out = run_script('self_check.py',
                             os.path.join(FIXTURES, 'overview_good.md'),
                             '--type', 'overview', '--section', '§1')
        self.assertIn('current-status conclusion', out)
        self.assertIn('PASS', out)


class TestSelfCheckDetailed(unittest.TestCase):

    def test_detailed_good_passes(self):
        """Compliant detailed sample: self_check should exit 0."""
        rc, out = run_script('self_check.py',
                             os.path.join(FIXTURES, 'detailed_good.md'),
                             '--type', 'detailed')
        self.assertEqual(rc, 0, f'expected pass but exited {rc}\n{out}')

    def test_detailed_n6_skip_without_overview(self):
        """Without --overview, N6 should SKIP."""
        rc, out = run_script('self_check.py',
                             os.path.join(FIXTURES, 'detailed_good.md'),
                             '--type', 'detailed')
        self.assertIn('SKIP', out)


class TestMermaidLint(unittest.TestCase):

    def test_mermaid_fail_detected(self):
        """Violating sample: mermaid_lint should exit 1 and report comma/paren."""
        rc, out = run_script('mermaid_lint.py',
                             os.path.join(FIXTURES, 'overview_mermaid_fail.md'))
        self.assertEqual(rc, 1, f'expected fail but exited {rc}\n{out}')
        self.assertIn('comma', out)

    def test_mermaid_good_passes(self):
        """Compliant sample: mermaid_lint should exit 0."""
        rc, out = run_script('mermaid_lint.py',
                             os.path.join(FIXTURES, 'overview_good.md'))
        self.assertEqual(rc, 0, f'expected pass but exited {rc}\n{out}')


class TestScopeCheck(unittest.TestCase):

    def test_bad_scope_detected(self):
        """Out-of-scope sample: scope_check should exit 1 and report inheritance/lock/field listing."""
        rc, out = run_script('scope_check.py',
                             os.path.join(FIXTURES, 'overview_bad_scope.md'))
        self.assertEqual(rc, 1, f'expected fail but exited {rc}\n{out}')
        self.assertIn('inheritance', out)

    def test_good_scope_passes(self):
        """Compliant sample: scope_check should exit 0."""
        rc, out = run_script('scope_check.py',
                             os.path.join(FIXTURES, 'overview_good.md'))
        self.assertEqual(rc, 0, f'expected pass but exited {rc}\n{out}')


if __name__ == '__main__':
    unittest.main()
