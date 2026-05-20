#!/usr/bin/env python3
"""Tests for tools/procmon.py pure functions."""

import os
import tempfile
import unittest
from unittest.mock import patch, mock_open


# Make procmon importable
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'tools'))
from procmon import find_pid, read_proc_stat, read_proc_statm, format_mb


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


class TestReadProcStatm(unittest.TestCase):
    @patch('builtins.open', mock_open(read_data='1000 500 200 100 0 300 0'))
    def test_valid_statm(self):
        result = read_proc_statm(42)
        page_size = os.sysconf("SC_PAGE_SIZE")
        self.assertEqual(result, 500 * page_size)

    def test_missing_file(self):
        result = read_proc_statm(999999999)
        self.assertIsNone(result)


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
