#!/usr/bin/env python3
"""Tests for tools/procmon.py pure functions."""

import io
import os
import socket
import unittest
from unittest.mock import MagicMock, patch, mock_open


# Make procmon importable
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'tools'))
from procmon import find_pid, read_proc_stat, read_proc_mem_breakdown, read_tcp_attempt_fails_stats, read_port_traffic, _parse_netlink_diag_response, format_mb


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
    """read_port_traffic: NETLINK_INET_DIAG query + _parse_netlink_diag_response.

    read_port_traffic queries the kernel's sock_diag interface directly
    (no external binary like ss needed). _parse_netlink_diag_response is
    the pure parser that extracts bytes_sent/bytes_received from the
    netlink dump's tcp_info payloads.
    """

    @staticmethod
    def _mock_sock_with_data(data):
        """Mock socket: recv returns data once, then raises socket.timeout."""
        sock = MagicMock()
        sock.recv.side_effect = [data, socket.timeout]
        return sock

    @staticmethod
    def _mock_sock_empty():
        """Mock socket: recv always raises socket.timeout (no data)."""
        sock = MagicMock()
        sock.recv.side_effect = socket.timeout
        return sock

    @patch('procmon.socket.socket')
    def test_parses_netlink_response(self, mock_socket_cls):
        # AF_INET returns one ESTAB socket with byte counters; AF_INET6
        # returns nothing. Aggregates to sent=1M, recv=512K.
        data = (_build_diag_msg(31511, 1, _build_tcp_info(
            bytes_sent=1048576, bytes_received=524288))
            + _build_done_msg())
        mock_socket_cls.side_effect = [
            self._mock_sock_with_data(data),
            self._mock_sock_empty(),
        ]
        sent, recv = read_port_traffic(31511, timeout=2)
        self.assertEqual(sent, 1048576)
        self.assertEqual(recv, 524288)

    @patch('procmon.socket.socket')
    def test_aggregates_across_inet_and_inet6(self, mock_socket_cls):
        # The function queries both AF_INET and AF_INET6 (dual-stack).
        # Each family's mock socket returns one socket; the byte counters
        # are summed.
        ipv4_data = (_build_diag_msg(31511, 1, _build_tcp_info(
            bytes_sent=1000, bytes_received=2000))
            + _build_done_msg())
        ipv6_data = (_build_diag_msg(31511, 1, _build_tcp_info(
            bytes_sent=3000, bytes_received=4000), family=10)
            + _build_done_msg())
        mock_socket_cls.side_effect = [
            self._mock_sock_with_data(ipv4_data),
            self._mock_sock_with_data(ipv6_data),
        ]
        sent, recv = read_port_traffic(31511, timeout=2)
        self.assertEqual(sent, 4000)
        self.assertEqual(recv, 6000)

    @patch('procmon.socket.socket')
    def test_no_estab_sockets_returns_none(self, mock_socket_cls):
        # Kernel responds with only NLMSG_DONE (no ESTAB sockets on the
        # port). Must return (None, None), not (0, 0), so the caller
        # skips the rate computation instead of printing 0.0MB/s.
        mock_socket_cls.side_effect = [
            self._mock_sock_with_data(_build_done_msg()),
            self._mock_sock_empty(),
        ]
        sent, recv = read_port_traffic(31511, timeout=2)
        self.assertIsNone(sent)
        self.assertIsNone(recv)

    @patch('procmon.socket.socket')
    def test_socket_open_failure_returns_none(self, mock_socket_cls):
        # AF_NETLINK socket creation fails (e.g. non-Linux platform, or
        # CAP_NET_ADMIN denied in a restricted container). Must return
        # (None, None) without crashing.
        mock_socket_cls.side_effect = OSError('Permission denied')
        sent, recv = read_port_traffic(31511, timeout=2)
        self.assertIsNone(sent)
        self.assertIsNone(recv)

    @patch('procmon.socket.socket')
    def test_request_includes_port_filter(self, mock_socket_cls):
        # Verify the netlink request carries the port in big-endian
        # (__be16) at the idiag_sport offset (req offset 24).
        import struct
        inet_sock = self._mock_sock_with_data(_build_done_msg())
        inet6_sock = self._mock_sock_empty()
        mock_socket_cls.side_effect = [inet_sock, inet6_sock]
        read_port_traffic(31511, timeout=2)
        # sendall is called on the first (AF_INET) mock socket
        sent_data = inet_sock.sendall.call_args[0][0]
        # idiag_sport is at offset 24 (nlmsghdr=16 + req header=8)
        sport = struct.unpack_from('>H', sent_data, 24)[0]
        self.assertEqual(sport, 31511)
        # Verify nlmsg_type = SOCK_DIAG_BY_FAMILY = 20
        nlmsg_type = struct.unpack_from('<H', sent_data, 4)[0]
        self.assertEqual(nlmsg_type, 20)


def _build_tcp_info(bytes_sent=None, bytes_received=None, length=208):
    """Build a tcp_info blob with byte counters at the correct offsets.

    Offset 128: tcpi_bytes_received (u64, kernel >= 4.4)
    Offset 200: tcpi_bytes_sent     (u64, kernel >= 4.8)
    """
    import struct as _s
    buf = bytearray(length)
    if bytes_received is not None and len(buf) >= 128 + 8:
        _s.pack_into('<Q', buf, 128, bytes_received)
    if bytes_sent is not None and len(buf) >= 200 + 8:
        _s.pack_into('<Q', buf, 200, bytes_sent)
    return bytes(buf)


def _build_diag_msg(port, state, tcp_info, family=2):
    """Build one netlink SOCK_DIAG_BY_FAMILY response message.

    Layout: nlmsghdr(16) + inet_diag_msg(72) + rtattr(INET_DIAG_INFO, tcp_info).
    inet_diag_msg.idiag_sport = port (big-endian __be16 at offset 4).
    inet_diag_msg.idiag_state = state (u8 at offset 1).
    """
    import struct as _s
    diag_msg = bytearray(72)
    diag_msg[0] = family
    diag_msg[1] = state
    _s.pack_into('>H', diag_msg, 4, port)
    rta_len = 4 + len(tcp_info)
    rta = _s.pack('<HH', rta_len, 2) + tcp_info
    while len(rta) % 4:
        rta += b'\x00'
    msg_len = 16 + len(diag_msg) + len(rta)
    nlmsghdr = _s.pack('<IHHII', msg_len, 20, 0, 1, 0)
    return nlmsghdr + bytes(diag_msg) + rta


def _build_done_msg():
    """Build an NLMSG_DONE trailer."""
    import struct as _s
    return _s.pack('<IHHII', 16, 3, 0, 1, 0)


class TestParseNetlinkDiagResponse(unittest.TestCase):
    """_parse_netlink_diag_response: parses NETLINK_INET_DIAG dump,
    aggregates bytes_sent/bytes_received across ESTABLISHED sockets.

    This is the pure parser that the netlink fallback path uses. It reads
    the same tcp_info byte counters as `ss -ti`, so a container without
    iproute2 installed still gets per-port throughput data.
    """

    def test_aggregates_bytes_across_multiple_estab_sockets(self):
        # Two ESTAB sockets on port 31511: sent=1M+2M=3M, recv=512K+128K=640K.
        msg1 = _build_diag_msg(31511, 1, _build_tcp_info(
            bytes_sent=1048576, bytes_received=524288))
        msg2 = _build_diag_msg(31511, 1, _build_tcp_info(
            bytes_sent=2097152, bytes_received=131072))
        data = msg1 + msg2 + _build_done_msg()
        sent, recv, found = _parse_netlink_diag_response(data, 31511)
        self.assertTrue(found)
        self.assertEqual(sent, 1048576 + 2097152)
        self.assertEqual(recv, 524288 + 131072)

    def test_listen_socket_excluded(self):
        # LISTEN socket (state=10) appears in the dump but carries no
        # byte counters; only ESTAB (state=1) contributes.
        listen_msg = _build_diag_msg(31511, 10, _build_tcp_info(
            bytes_sent=0, bytes_received=0))
        estab_msg = _build_diag_msg(31511, 1, _build_tcp_info(
            bytes_sent=500, bytes_received=300))
        data = listen_msg + estab_msg + _build_done_msg()
        sent, recv, found = _parse_netlink_diag_response(data, 31511)
        self.assertTrue(found)
        self.assertEqual(sent, 500)
        self.assertEqual(recv, 300)

    def test_empty_dump_returns_not_found(self):
        # No ESTAB sockets match the port filter; kernel responds with
        # only NLMSG_DONE. Returns found=False → caller treats as
        # (None, None), not (0, 0), to avoid a misleading 0.0MB/s line.
        data = _build_done_msg()
        sent, recv, found = _parse_netlink_diag_response(data, 31511)
        self.assertFalse(found)
        self.assertEqual(sent, 0)
        self.assertEqual(recv, 0)

    def test_old_kernel_no_bytes_sent(self):
        # Kernel < 4.8: tcp_info is only 136 bytes (up to and including
        # bytes_received at offset 128). bytes_sent at offset 200 is
        # absent — parser must skip it without error.
        tcp_info = _build_tcp_info(
            bytes_sent=None, bytes_received=4096, length=136)
        msg = _build_diag_msg(31511, 1, tcp_info)
        data = msg + _build_done_msg()
        sent, recv, found = _parse_netlink_diag_response(data, 31511)
        self.assertTrue(found)
        self.assertEqual(recv, 4096)
        # bytes_sent not available on this kernel → stays 0
        self.assertEqual(sent, 0)

    def test_very_old_kernel_no_byte_counters(self):
        # Kernel < 4.4: tcp_info < 136 bytes, neither bytes_sent nor
        # bytes_received is available. found=False → (None, None).
        tcp_info = _build_tcp_info(length=104)
        msg = _build_diag_msg(31511, 1, tcp_info)
        data = msg + _build_done_msg()
        sent, recv, found = _parse_netlink_diag_response(data, 31511)
        self.assertFalse(found)

    def test_nlmsg_error_terminates_parsing(self):
        # Kernel sends NLMSG_ERROR (type=2). Parser must stop and return
        # whatever it found so far (nothing) → (0, 0, False).
        import struct as _s
        error_msg = _s.pack('<IHHII', 16, 2, 0, 1, 0)
        data = error_msg + _build_done_msg()
        sent, recv, found = _parse_netlink_diag_response(data, 31511)
        self.assertFalse(found)

    def test_truncated_message_terminates_parsing(self):
        # A truncated nlmsghdr (fewer than 16 bytes) must not crash;
        # parser returns what it has so far.
        msg = _build_diag_msg(31511, 1, _build_tcp_info(
            bytes_sent=100, bytes_received=200))
        data = msg + b'\x03\x00\x00\x00'  # claims len=3 but < 16
        sent, recv, found = _parse_netlink_diag_response(data, 31511)
        self.assertTrue(found)
        self.assertEqual(sent, 100)
        self.assertEqual(recv, 200)


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

