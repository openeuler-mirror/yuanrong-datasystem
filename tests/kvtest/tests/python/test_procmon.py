#!/usr/bin/env python3
"""Tests for tools/procmon.py pure functions."""

import io
import os
import unittest
from unittest.mock import MagicMock, patch, mock_open


# Make procmon importable
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'tools'))
from procmon import find_pid, read_proc_stat, read_proc_mem_breakdown, format_mb


class TestFindPid(unittest.TestCase):
    @patch('procmon.subprocess.check_output')
    def test_success(self, mock_run):
        mock_run.return_value = '12345\n'
        self.assertEqual(find_pid('myapp'), 12345)

    @patch('procmon.subprocess.check_output')
    def test_not_found(self, mock_run):
        import subprocess
        mock_run.side_effect = subprocess.CalledProcessError(1, 'pgrep')
        self.assertIsNone(find_pid('nonexistent'))

    @patch('procmon.subprocess.check_output')
    def test_multiple_pids(self, mock_run):
        mock_run.return_value = '100\n200\n300\n'
        self.assertEqual(find_pid('multi'), 100)

    @patch('procmon.subprocess.check_output')
    def test_empty_output(self, mock_run):
        mock_run.return_value = ''
        self.assertIsNone(find_pid('empty'))


class TestReadProcStat(unittest.TestCase):
    @patch('builtins.open', mock_open(
        read_data='42 (test) S 0 0 0 0 0 0 0 0 0 0 1000 500'))
    def test_valid_stat(self):
        result = read_proc_stat(42)
        self.assertEqual(result, 1500)

    def test_missing_file(self):
        result = read_proc_stat(999999999)
        self.assertIsNone(result)


class TestReadProcMemBreakdown(unittest.TestCase):
    """read_proc_mem_breakdown: smaps_rollup primary path, statm fallback."""

    def _make_open(self, files):
        """Return a mock for builtins.open that serves StringIO by path suffix.

        files: dict mapping path suffix -> file content. Unknown paths raise
        FileNotFoundError so the function falls through to the next source.
        """
        def fake_open(path, *args, **kwargs):
            p = str(path)
            for suffix, content in files.items():
                if p.endswith(suffix):
                    return io.StringIO(content)
            raise FileNotFoundError(p)
        return MagicMock(side_effect=fake_open)

    @patch('builtins.open')
    def test_smaps_primary(self, mock_open_):
        """smaps_rollup readable -> values come from Private_*/Shared_*."""
        smaps = (
            "00400000-7ffffff r--p 00000000 00:00 0\n"
            "Rss:                   1000 kB\n"
            "Pss:                    500 kB\n"
            "Shared_Clean:           200 kB\n"
            "Shared_Dirty:           200 kB\n"
            "Private_Clean:          100 kB\n"
            "Private_Dirty:          500 kB\n"
            "Referenced:            1000 kB\n"
            "Anonymous:              600 kB\n"
        )
        mock_open_.side_effect = self._make_open({"/smaps_rollup": smaps}).side_effect
        rss, anon, shared = read_proc_mem_breakdown(42)
        # RSS in kB -> bytes
        self.assertEqual(rss, 1000 * 1024)
        # Anon = Private_Clean + Private_Dirty = 100 + 500 = 600 kB
        self.assertEqual(anon, 600 * 1024)
        # Shared = Shared_Clean + Shared_Dirty = 200 + 200 = 400 kB
        self.assertEqual(shared, 400 * 1024)

    @patch('procmon.os.sysconf', return_value=4096, create=True)
    @patch('builtins.open')
    def test_statm_fallback(self, mock_open_, _mock_sysconf):
        """smaps_rollup missing -> falls back to statm derivation."""
        # statm fields: size resident shared text lib data dt
        # resident=500 pages, shared=200 pages -> anon=300 pages
        statm = "1000 500 200 100 0 300 0\n"
        mock_open_.side_effect = self._make_open({"/statm": statm}).side_effect
        rss, anon, shared = read_proc_mem_breakdown(42)
        self.assertEqual(rss, 500 * 4096)
        self.assertEqual(shared, 200 * 4096)
        self.assertEqual(anon, 300 * 4096)

    @patch('procmon.os.sysconf', return_value=4096, create=True)
    @patch('builtins.open')
    def test_smaps_without_rss_falls_back(self, mock_open_, _mock_sysconf):
        """smaps_rollup present but missing Rss -> fall through to statm."""
        smaps = "Pss:    10 kB\n"  # no Rss field
        statm = "1000 500 200 100 0 300 0\n"
        mock_open_.side_effect = self._make_open({
            "/smaps_rollup": smaps,
            "/statm": statm,
        }).side_effect
        rss, anon, shared = read_proc_mem_breakdown(42)
        self.assertEqual(rss, 500 * 4096)
        self.assertEqual(shared, 200 * 4096)

    def test_missing_files(self):
        """Neither smaps_rollup nor statm exist -> (None, None, None)."""
        rss, anon, shared = read_proc_mem_breakdown(999999999)
        self.assertIsNone(rss)
        self.assertIsNone(anon)
        self.assertIsNone(shared)


class TestFormatMb(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(format_mb(1024 * 1024), "1.0")

    def test_zero(self):
        self.assertEqual(format_mb(0), "0.0")

    def test_large(self):
        self.assertEqual(format_mb(2 * 1024 * 1024 * 1024), "2048.0")

    def test_fractional(self):
        self.assertEqual(format_mb(512 * 1024), "0.5")


if __name__ == '__main__':
    unittest.main()

