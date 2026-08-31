#!/usr/bin/env python3
"""Tests for tools/procmon.py pure functions."""

import io
import os
import unittest
from unittest.mock import MagicMock, patch, mock_open


# Make procmon importable
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'tools'))
from procmon import find_pid, read_proc_stat, read_proc_mem_breakdown, read_tcp_attempt_fails_stats, read_port_traffic, format_mb


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
        """smaps_rollup readable -> anon from Anonymous field, shared = Rss - Anonymous."""
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
        # Anon = Anonymous field = 600 kB
        self.assertEqual(anon, 600 * 1024)
        # Shared = Rss - Anonymous = 1000 - 600 = 400 kB
        self.assertEqual(shared, 400 * 1024)

    @patch('builtins.open')
    def test_smaps_single_attacher_shmem(self, mock_open_):
        """Single-attacher shmem must classify as shared, not anon.

        Regression for the bug the fix targets: a worker pins a shmem
        segment only it attaches. The kernel's mapcount-based smaps
        fields place all those shmem pages under Private_Dirty (because
        mapcount==1), so the old Private_Clean + Private_Dirty formula
        reported them as anon. The `Anonymous` field is PageAnon-based
        and correctly excludes shmem regardless of mapcount.
        """
        smaps = (
            "00400000-7ffffff r--p 00000000 00:00 0\n"
            # 12 GB shmem + 1 GB anon = 13 GB resident
            "Rss:                   13000 kB\n"
            # PageAnon only (the 1 GB true anon); shmem is NOT PageAnon
            "Anonymous:              1000 kB\n"
            # Mapcount-based split misclassifies the 12 GB shmem here
            "Private_Clean:             0 kB\n"
            "Private_Dirty:        13000 kB\n"
            "Shared_Clean:             0 kB\n"
            "Shared_Dirty:             0 kB\n"
            "Referenced:           13000 kB\n"
        )
        mock_open_.side_effect = self._make_open({"/smaps_rollup": smaps}).side_effect
        rss, anon, shared = read_proc_mem_breakdown(42)
        self.assertEqual(rss, 13000 * 1024)
        # Anon must NOT include the 12 GB shmem despite mapcount==1
        self.assertEqual(anon, 1000 * 1024)
        # Shared must pick up the pinned shmem segment
        self.assertEqual(shared, 12000 * 1024)

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


class TestReadTcpAttemptFailsStats(unittest.TestCase):
    """read_tcp_attempt_fails_stats: parses /proc/net/snmp for AttemptFails + ActiveOpens."""

    @patch('builtins.open', mock_open(
        read_data=(
            "Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens "
            "AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs "
            "InErrs OutRsts InCsumErrors\n"
            "Tcp: 1 200 120000 2000 100 50 7 0 5 12345 20000 200 0 0 0\n"
        )))
    def test_valid_snmp(self):
        fails, opens = read_tcp_attempt_fails_stats()
        # AttemptFails = 7, ActiveOpens = 100
        self.assertEqual(fails, 7)
        self.assertEqual(opens, 100)

    @patch('builtins.open', side_effect=FileNotFoundError("/proc/net/snmp"))
    def test_missing_file(self, _mock_open):
        fails, opens = read_tcp_attempt_fails_stats()
        self.assertIsNone(fails)
        self.assertIsNone(opens)


class TestReadPortTraffic(unittest.TestCase):
    """read_port_traffic: parses `ss -tin` output, aggregates
    bytes_sent/bytes_received across all sockets on the port.

    The `ss -tin 'sport = :PORT'` output is multi-line per socket: a header
    line (State/Recv-Q/Send-Q/Local/Peer) followed by indented TCP internal
    info lines. Real iproute2 ``ss -ti`` uses ``key:value`` colon-separated
    fields (e.g. ``bytes_sent:16703 bytes_received:1449``); the regex
    ``[:\\s]+`` matches both colon and whitespace so the parser works on the
    dominant colon format and is tolerant of whitespace-only variants.
    """

    # Realistic ss -tin output: two ESTAB sockets on port 31511, each with
    # colon-separated key:value TCP internal fields. The LISTEN socket (if
    # present) has no byte counters and is naturally excluded by the regex.
    _SS_OUTPUT = (
        "State  Recv-Q Send-Q Local Address:Port  Peer Address:Port   Process\n"
        "ESTAB  0      0      10.0.0.1:31511      10.0.0.2:54321\n"
        "    cubic wscale:7,7 rto:204 rtt:0.1/0.05 mss:1448 pmtu:1500 rcvmss:1404\n"
        "    bytes_sent:1048576 bytes_ack:1048576 bytes_received:524288 segs_out:1024 segs_in:512\n"
        "    send 8388.6Kbps rcv_rtt:0.1ms rcv_space:14600\n"
        "ESTAB  0      0      10.0.0.1:31511      10.0.0.3:54322\n"
        "    cubic wscale:7,7 rto:204 rtt:0.1/0.05 mss:1448 pmtu:1500 rcvmss:1404\n"
        "    bytes_sent:2097152 bytes_ack:2097152 bytes_received:131072 segs_out:2048 segs_in:128\n"
    )

    @patch('procmon.subprocess.run')
    def test_aggregates_bytes_across_multiple_sockets(self, mock_run):
        # Two ESTAB sockets: sent=1M+2M=3M, recv=512K+128K=640K.
        mock_run.return_value = MagicMock(returncode=0, stdout=self._SS_OUTPUT)
        sent, recv = read_port_traffic(31511)
        self.assertEqual(sent, 1048576 + 2097152)
        self.assertEqual(recv, 524288 + 131072)

    @patch('procmon.subprocess.run')
    def test_single_socket_colon_format(self, mock_run):
        output = (
            "ESTAB  0  0  10.0.0.1:31511  10.0.0.2:54321\n"
            "    cubic rtt:0.1ms\n"
            "    bytes_sent:100 bytes_received:200\n"
        )
        mock_run.return_value = MagicMock(returncode=0, stdout=output)
        sent, recv = read_port_traffic(31511)
        self.assertEqual(sent, 100)
        self.assertEqual(recv, 200)

    @patch('procmon.subprocess.run')
    def test_whitespace_format_also_supported(self, mock_run):
        # Defensive: if some iproute2 build uses whitespace instead of colon,
        # the parser must still work. This pins the [:\s]+ regex against
        # regression to \s+ (which would silently no-op on real colon output).
        output = (
            "ESTAB  0  0  10.0.0.1:31511  10.0.0.2:54321\n"
            "    bytes_sent 100 bytes_received 200\n"
        )
        mock_run.return_value = MagicMock(returncode=0, stdout=output)
        sent, recv = read_port_traffic(31511)
        self.assertEqual(sent, 100)
        self.assertEqual(recv, 200)

    @patch('procmon.subprocess.run')
    def test_no_sockets_returns_none(self, mock_run):
        # Port filter matches zero sockets (server not listening, or no
        # ESTAB connections yet). ss returns rc=0 but empty body (just the
        # header line, no byte fields). Must return (None, None) so the
        # caller skips the rate computation, not (0, 0) which would
        # produce a misleading 0.0MB/s line.
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        sent, recv = read_port_traffic(31511)
        self.assertIsNone(sent)
        self.assertIsNone(recv)

    @patch('procmon.subprocess.run')
    def test_ss_missing_returns_none(self, mock_run):
        # ss binary not installed in the container. FileNotFoundError is
        # caught; returns (None, None) so procmon degrades to no-traffic
        # mode without crashing the whole monitoring loop.
        mock_run.side_effect = FileNotFoundError('ss')
        sent, recv = read_port_traffic(31511)
        self.assertIsNone(sent)
        self.assertIsNone(recv)

    @patch('procmon.subprocess.run')
    def test_ss_nonzero_rc_returns_none(self, mock_run):
        # ss exits non-zero (insufficient privileges, invalid port filter
        # syntax, etc.). Treat as no data, not an error — procmon's
        # sample loop must keep going so CPU/MEM/FD stats are not lost.
        mock_run.return_value = MagicMock(returncode=1, stdout='', stderr='error')
        sent, recv = read_port_traffic(31511)
        self.assertIsNone(sent)
        self.assertIsNone(recv)

    @patch('procmon.subprocess.run')
    def test_ss_timeout_returns_none(self, mock_run):
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=['ss'], timeout=5)
        sent, recv = read_port_traffic(31511)
        self.assertIsNone(sent)
        self.assertIsNone(recv)

    @patch('procmon.subprocess.run')
    def test_listen_socket_without_counters_excluded(self, mock_run):
        # The LISTEN socket appears in ss -tin output but has no
        # bytes_sent/bytes_received (it never carries traffic, only
        # accepts connections). The regex naturally excludes it because
        # the field names are absent from the LISTEN socket's info lines.
        # This test pins that behavior: only ESTAB sockets contribute.
        output = (
            "LISTEN 0  128  0.0.0.0:31511  0.0.0.0:*\n"
            "    cubic\n"
            "ESTAB  0  0  10.0.0.1:31511  10.0.0.2:54321\n"
            "    bytes_sent:500 bytes_received:300\n"
        )
        mock_run.return_value = MagicMock(returncode=0, stdout=output)
        sent, recv = read_port_traffic(31511)
        # Only the ESTAB socket's 500/300 counted; LISTEN socket ignored.
        self.assertEqual(sent, 500)
        self.assertEqual(recv, 300)

    @patch('procmon.subprocess.run')
    def test_passes_port_filter_to_ss(self, mock_run):
        # Verify the ss command includes the port filter so the right
        # sockets are selected. Pins the `ss -tin 'sport = :PORT'` form
        # (same filter syntax as deploy_common.find_pid_by_port).
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        read_port_traffic(31511)
        call_args = mock_run.call_args[0]
        cmd = call_args[0]
        self.assertEqual(cmd[0], 'ss')
        self.assertIn('-tin', cmd)
        self.assertIn('sport = :31511', cmd)


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

