#!/usr/bin/env python3
"""Unit + end-to-end tests for network_latency_analysis.py (stdlib unittest only).

Run:
    python3 -m unittest test_network_latency_analysis -v
or:
    python3 test_network_latency_analysis.py
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import network_latency_analysis as nla  # noqa: E402

SAMPLE_LOG_ROOT = "/home/wcy/log/mini_log"
DAY = datetime(2026, 8, 21)

INFO_LINE = ("2026-08-21T21:31:21.060757 | I | object_posix.brpc.stub.pb.cc:2127 | "
             "192.168.219.138 | 25487:25586 | getBuffer-25487-00004775;117c5c4a91c7 |  | "
             " yyl9 ClientSend ts 88035205620370 tid 25586\n")

SLOW_MSG = ("[BRPC_RPC_FRAMEWORK_SLOW] trace_id=getBuffer-25487-00004775;117c5c4a91c7 "
            "method=datasystem.WorkerOCService.QueryAndGet framework_us=16011 e2e_us=16252 "
            "client_req_framework_us=0 remote_processing_us=16241 server_req_queue_us=10 "
            "server_exec_us=240 network_residual_us=15989 cntl_timeout_ms=20 "
            "cntl_deadline_us=1787319081080758 cntl_error_code=0 cntl_failed=0 "
            "resp_attachment_bytes=56 ClientSend=88035205620370 ClientRecv=88035221862010 "
            "ServerSend=88038917846674 ServerRecv=88038917594514 tid=25872")

BPF_SEND_IN = ("21:31:21:060777 tcp  send in  tid 479093 cpu 50 size 270 "
               "192.168.219.138:37880 -> 192.168.102.161:31501\n")
BPF_RECV_IN = ("21:31:21:061203 tcp  recv in  tid 479193 cpu 332 size 4096 "
               "192.168.219.138:37880 <- 192.168.102.161:31501, "
               "copied_seq:358067377, rcv_nxt:358067377\n")
BPF_RECV_QUE = ("21:31:21:011008 tcp  recv que tid 594763 cpu 4 size 266 "
                "tp_rcv_nxt:4187256525, 192.168.102.161:31501 <- 192.168.219.138:37868\n")
BPF_SOCK = ("21:31:21:011011 sock_def_readable, tcp  wakeup 1 tid 594763 cpu 4 "
            "192.168.102.161:31501 <- 192.168.219.138:37868\n")
BPF_WAKING = ("21:31:21:011014 sched_waking tid 594763 cpu 4 comm brpc_wkr:0-22 "
              "pid 396241 target_cpu 84, wq:0\n")
BPF_WAKEUP = ("21:31:21:011016 sched_wakeup  tid 0 cur_comm:swapper/84 cpu 84 "
              "comm brpc_wkr:0-22 pid 396241, target_cpu:84\n")
BPF_SWITCH = ("21:31:21:011020 sched_switch tid 0 cpu 84 prev_comm=swapper/84 "
              "prev_pid=0 next_comm=brpc_wkr:0-22 next_pid=396241\n")
BPF_NOADDR = "21:31:21:018693 tcp  send out tid 396241 cpu 84 size 155\n"

LW_BLOCK = ("[88019.268007][2026-08-21 21:31:05:123144] !!! resched_latency_warn Triggered !!!\n"
            "Current CPU: 61 | Task Comm: kvtest | PID: 461221, latency: 4000850\n"
            "\n[ Kernel Call Stack ]:\n\n"
            "        resched_latency_warn+0\n")


class TestParsers(unittest.TestCase):
    def test_parse_info_line(self):
        info = nla.parse_info_line(INFO_LINE)
        self.assertIsNotNone(info)
        self.assertEqual(info["ts"], datetime(2026, 8, 21, 21, 31, 21, 607570 // 10))
        self.assertEqual(info["host"], "192.168.219.138")
        self.assertEqual(info["trace"], "getBuffer-25487-00004775;117c5c4a91c7")
        self.assertIn("ClientSend ts 88035205620370", info["msg"])

    def test_parse_info_line_rejects_garbage(self):
        self.assertIsNone(nla.parse_info_line("random text\n"))

    def test_fmt_dt_microsecond_precision(self):
        dt = datetime(2026, 8, 21, 21, 31, 21, 60757)
        self.assertEqual(nla.fmt_dt(dt), "21:31:21.060757")

    def test_parse_slow_kv(self):
        kv = nla.parse_slow_kv(SLOW_MSG)
        self.assertEqual(kv["trace_id"], "getBuffer-25487-00004775;117c5c4a91c7")
        self.assertEqual(kv["network_residual_us"], "15989")
        self.assertEqual(kv["ClientSend"], "88035205620370")
        self.assertEqual(kv["ServerRecv"], "88038917594514")
        self.assertEqual(kv["tid"], "25872")

    def test_parse_bpf_send_in(self):
        ev = nla.parse_bpf_line(BPF_SEND_IN, DAY)
        self.assertEqual(ev["kind"], "tcp_send_in")
        self.assertEqual(ev["tid"], 479093)
        self.assertEqual(ev["local_ip"], "192.168.219.138")
        self.assertEqual(ev["local_port"], 37880)
        self.assertEqual(ev["peer_ip"], "192.168.102.161")
        self.assertEqual(ev["peer_port"], 31501)
        self.assertEqual(ev["ts"], datetime(2026, 8, 21, 21, 31, 21, 607770 // 10))

    def test_parse_bpf_recv_in(self):
        ev = nla.parse_bpf_line(BPF_RECV_IN, DAY)
        self.assertEqual(ev["kind"], "tcp_recv_in")
        self.assertEqual(ev["copied_seq"], 358067377)
        self.assertEqual(ev["rcv_nxt"], 358067377)

    def test_parse_bpf_recv_que(self):
        ev = nla.parse_bpf_line(BPF_RECV_QUE, DAY)
        self.assertEqual(ev["kind"], "tcp_recv_que")
        self.assertEqual(ev["local_ip"], "192.168.102.161")
        self.assertEqual(ev["peer_port"], 37868)
        self.assertEqual(ev["rcv_nxt"], 4187256525)

    def test_parse_bpf_no_addr(self):
        ev = nla.parse_bpf_line(BPF_NOADDR, DAY)
        self.assertEqual(ev["kind"], "tcp_send_out")
        self.assertNotIn("local_ip", ev)

    def test_parse_bpf_sched(self):
        self.assertEqual(nla.parse_bpf_line(BPF_WAKING, DAY)["kind"], "sched_waking")
        ev = nla.parse_bpf_line(BPF_WAKEUP, DAY)
        self.assertEqual(ev["kind"], "sched_wakeup")
        self.assertEqual(ev["pid"], 396241)
        self.assertEqual(nla.parse_bpf_line(BPF_SWITCH, DAY)["next_pid"], 396241)
        ev = nla.parse_bpf_line(BPF_SOCK, DAY)
        self.assertEqual(ev["kind"], "sock_readable")
        self.assertEqual(ev["local_port"], 31501)
        self.assertEqual(ev["wakeup_n"], 1)
        self.assertIsNone(nla.parse_bpf_line("not a bpf line\n", DAY))


class TestLatencyWarn(unittest.TestCase):
    def test_parse_blocks(self):
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as fh:
            fh.write("sched_features:\nPLACE_LAG\nlatency_warn_ms: 2\n\n")
            fh.write(LW_BLOCK)
            fh.write("[88019.999999][2026-08-21 21:31:30:000001] !!! resched_latency_warn Triggered !!!\n")
            fh.write("Current CPU: 1 | Task Comm: x | PID: 2, latency: 3000\n")
            path = fh.name
        try:
            evs = nla.parse_latency_warn_blocks(path)
            self.assertEqual(len(evs), 2)
            self.assertEqual(evs[0]["ts"], datetime(2026, 8, 21, 21, 31, 5, 123144))
            self.assertEqual(evs[0]["comm"], "kvtest")
            self.assertEqual(evs[0]["latency_us"], 4000850)
            self.assertEqual(evs[1]["cpu"], 1)
        finally:
            os.unlink(path)


class TestMarkerScanner(unittest.TestCase):
    """字节块标记扫描器：大日志优化的基础组件。"""

    M1, M2 = b"MARK1", b"MARK2"

    def _write(self, content):
        with tempfile.NamedTemporaryFile("wb", suffix=".log", delete=False) as fh:
            fh.write(content)
            return fh.name

    def _scan(self, path, markers, chunk=None):
        if chunk is None:
            return list(nla.iter_marker_lines([Path(path)], markers))
        with mock.patch.object(nla, "SCAN_CHUNK", chunk):
            return list(nla.iter_marker_lines([Path(path)], markers))

    def test_basic_and_same_line_dedup(self):
        path = self._write(b"aaa\n" * 100 + b"xx MARK1 yy\n" + b"bbb\n" * 50
                           + b"zz MARK2 ww\n" + b"both MARK1 and MARK2\n" + b"tail\n")
        lines = self._scan(path, [self.M1, self.M2])
        self.assertEqual([l for _, l in lines],
                         ["xx MARK1 yy", "zz MARK2 ww", "both MARK1 and MARK2"])

    def test_marker_and_line_span_chunk_boundary(self):
        # 30 字节行 + 小 chunk：marker/行首跨块边界必须被正确拼接
        lines_txt = [("line%03d padded MARK1 end" % i) if i % 7 == 0
                     else ("line%03d padded nope end" % i) for i in range(200)]
        content = ("\n".join(lines_txt) + "\n").encode()
        path = self._write(content)
        got = [l for _, l in self._scan(path, [self.M1], chunk=64)]
        self.assertEqual(got, [t for t in lines_txt if "MARK1" in t])

    def test_marker_inside_marker_boundary(self):
        # marker 自身被 chunk 边界切断（如 "MAR" | "K1"），整行必须完整重组
        content = b"x" * 63 + b"MAR" + b"K1 rest\n" + b"y" * 100 + b"\n"
        path = self._write(content)
        got = self._scan(path, [self.M1], chunk=64)
        self.assertEqual([l for _, l in got], [("x" * 63) + "MARK1 rest"])

    def test_no_marker_file(self):
        path = self._write(b"nothing to see here\n" * 5000)
        self.assertEqual(self._scan(path, [self.M1, self.M2]), [])

    def test_final_line_without_newline(self):
        path = self._write(b"junk\nxx MARK1 tail-no-newline")
        got = self._scan(path, [self.M1])
        self.assertEqual([l for _, l in got], ["xx MARK1 tail-no-newline"])

    def test_multi_file_order(self):
        p1 = self._write(b"a MARK1\n")
        p2 = self._write(b"b MARK2\nc MARK1\n")
        got = list(nla.iter_marker_lines([Path(p1), Path(p2)], [self.M1, self.M2]))
        self.assertEqual([l for _, l in got], ["a MARK1", "b MARK2", "c MARK1"])
        self.assertEqual([str(p) for p, _ in got], [p1, p2, p2])


class TestSlowRecordScan(unittest.TestCase):
    """scan_slow_records 改用标记扫描后的行为回归。"""

    def _client_log(self, residual):
        slow = SLOW_MSG.replace("network_residual_us=15989",
                                "network_residual_us=%d" % residual)
        return ("2026-08-21T21:31:21.077013 | I | brpc_perf_trace.h:368 | 1.1.1.1 | "
                "1:1 | tr | u |  " + slow + "\n")

    def test_scan_threshold_and_marker_path(self):
        content = (self._client_log(1500) + "noise line\n" * 100
                   + self._client_log(999) + self._client_log(2000)).encode()
        with tempfile.NamedTemporaryFile("wb", suffix=".log", delete=False) as fh:
            fh.write(content)
            path = Path(fh.name)
        try:
            recs = nla.scan_slow_records([path], 1000)
            self.assertEqual([int(r.fields["network_residual_us"]) for r in recs],
                             [2000, 1500])  # 降序
            self.assertEqual(recs[0].pod_dir, path.parent.name)
            with mock.patch.object(nla, "SCAN_CHUNK", 64):
                recs2 = nla.scan_slow_records([path], 1000)
                self.assertEqual(len(recs2), 2)
        finally:
            os.unlink(path)


class TestAnchorIndex(unittest.TestCase):
    """collect_anchor_lines：标记扫描 + trace 精确匹配替代 O(traces×lines)。"""

    TRACE_A = "getBuffer-25487-00004775;117c5c4a91c7"
    TRACE_SIMILAR = "getBuffer-25487-00004775;117c5c4a91c8"  # 仅末位不同

    def _info(self, ts, host, trace, msg):
        return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n" % (ts, host, trace, msg))

    def _logs(self):
        cdir = tempfile.mkdtemp()
        wdir = tempfile.mkdtemp()
        cpath = Path(cdir) / "ds_client_1.INFO.1.log"
        wpath = Path(wdir) / "kvcache.INFO.1.log"
        cpath.write_text(
            self._info("2026-08-21T21:31:21.060757", "192.168.219.138", self.TRACE_A,
                       "yyl9 ClientSend ts 111 tid 5\n")
            + "noise no marker line\n" * 50
            + self._info("2026-08-21T21:31:21.077001", "192.168.219.138", self.TRACE_A,
                         "yyl9 ClientRecv ts 222 tid 5\n")
            + self._info("2026-08-21T21:31:21.060800", "10.0.0.9", self.TRACE_SIMILAR,
                         "yyl9 ClientSend ts 999 tid 9\n"), encoding="utf-8")
        wpath.write_text(
            self._info("2026-08-21T21:31:21.060848", "192.168.102.161", self.TRACE_A,
                       "yyl3 ServerRecv ts 333 tid 7\n")
            + self._info("2026-08-21T21:31:21.061096", "192.168.102.161", self.TRACE_A,
                         "yyl10 ServerSend ts 444 tid 7\n")
            + self._info("2026-08-21T21:31:21.061100", "10.0.0.8", self.TRACE_SIMILAR,
                         "yyl3 ServerRecv ts 888 tid 8\n"), encoding="utf-8")
        return cpath, wpath

    def test_index_and_exact_match(self):
        cpath, wpath = self._logs()
        idx = nla.collect_anchor_lines([cpath], [wpath], [self.TRACE_A])
        self.assertEqual(sorted(idx.keys()), [self.TRACE_A])
        a = idx[self.TRACE_A]
        self.assertEqual([i["msg"] for i in a["client"]],
                         ["yyl9 ClientSend ts 111 tid 5", "yyl9 ClientRecv ts 222 tid 5"])
        self.assertEqual([i["msg"] for i in a["worker"]],
                         ["yyl3 ServerRecv ts 333 tid 7", "yyl10 ServerSend ts 444 tid 7"])
        self.assertEqual(a["client"][0]["_pod_dir"], cpath.parent.name)
        self.assertEqual(a["worker"][0]["_path"], str(wpath))
        # 相似 trace（末位不同）不得被误收
        for i in a["client"] + a["worker"]:
            self.assertEqual(i["trace"], self.TRACE_A)

    def test_missing_trace_gives_empty_buckets(self):
        cpath, wpath = self._logs()
        idx = nla.collect_anchor_lines([cpath], [wpath], [self.TRACE_A, "no-such-trace"])
        self.assertEqual(idx["no-such-trace"], {"client": [], "worker": []})

    def test_sorted_by_ts(self):
        cpath, wpath = self._logs()
        idx = nla.collect_anchor_lines([cpath], [wpath], [self.TRACE_A])
        ts = [i["ts"] for i in idx[self.TRACE_A]["client"]]
        self.assertEqual(ts, sorted(ts))


class TestTodWindows(unittest.TestCase):
    """bpf 窗口的当日时间段(tod)模型与跨午夜拆分。"""

    def _win(self, s, e, trace="A", side="client"):
        return nla.TraceWindow(trace, side, s, e, "10.0.0.1", "10.0.0.2")

    def test_single_day_window(self):
        w = self._win(datetime(2026, 8, 21, 21, 31, 21, 58000),
                      datetime(2026, 8, 21, 21, 31, 21, 79000))
        self.assertEqual(w.start_us, (21 * 3600 + 31 * 60 + 21) * 1000000 + 58000)
        self.assertEqual(w.end_us, (21 * 3600 + 31 * 60 + 21) * 1000000 + 79000)
        self.assertEqual(w.start_tod, "21:31:21:058000")
        self.assertEqual(w.base_date, datetime(2026, 8, 21).date())

    def test_split_across_midnight(self):
        wins = nla.split_window_at_midnight("A", "client",
                                            datetime(2026, 8, 21, 23, 59, 59, 900000),
                                            datetime(2026, 8, 22, 0, 0, 0, 100000),
                                            "10.0.0.1", "10.0.0.2")
        self.assertEqual(len(wins), 2)
        self.assertEqual(wins[0].base_date, datetime(2026, 8, 21).date())
        self.assertEqual(wins[0].end_us, 86399999999)   # 23:59:59.999999
        self.assertEqual(wins[1].base_date, datetime(2026, 8, 22).date())
        self.assertEqual(wins[1].start_us, 0)
        self.assertEqual(wins[1].end_us, 100000)

    def test_split_no_midnight(self):
        wins = nla.split_window_at_midnight("A", "client",
                                            datetime(2026, 8, 21, 21, 0, 0),
                                            datetime(2026, 8, 21, 21, 0, 1),
                                            "a", "b")
        self.assertEqual(len(wins), 1)

    def test_cluster_merge(self):
        base = datetime(2026, 8, 21, 21, 31, 21)
        w1 = self._win(base, base + timedelta(milliseconds=20))
        w2 = self._win(base + timedelta(milliseconds=5),
                       base + timedelta(milliseconds=30))
        w3 = self._win(base + timedelta(hours=1), base + timedelta(hours=1, seconds=1))
        clusters = nla.merge_window_clusters([w2, w1, w3])
        self.assertEqual(len(clusters), 2)
        self.assertEqual(clusters[0].start_us, w1.start_us)
        self.assertEqual(clusters[0].end_us, w2.end_us)
        self.assertEqual(len(clusters[0].windows), 2)
        self.assertEqual(clusters[1].start_us, w3.start_us)


class TestBpfScanner(unittest.TestCase):
    """BpfScanner：窗口化扫描替代整文件载入（大日志核心优化）。"""

    CIP, SIP = "10.0.0.1", "10.0.0.2"

    def _win(self, s, e, trace="A", side="client"):
        return nla.TraceWindow(trace, side, s, e, self.CIP, self.SIP)

    def _write_bpf(self, lines):
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as fh:
            fh.write("\n".join(lines) + "\n")
            return fh.name

    def _events(self, lines):
        # 生成 bpf 日志行：窗口前后噪声 + 窗口内事件
        return (["21:31:20:000000 tcp  send in  tid 1 cpu 1 size 10 10.0.0.1:1 -> 10.0.0.2:2",
                 "21:31:19:500000 tcp  recv in  tid 1 cpu 1 size 10 10.0.0.1:1 <- 10.0.0.2:2"]
                + lines +
                ["21:31:22:000000 tcp  send in  tid 1 cpu 1 size 10 10.0.0.1:1 -> 10.0.0.2:2"])

    def test_full_scan_attach_rules(self):
        lines = self._events([
            "21:31:21:060000 tcp  send in  tid 1 cpu 1 size 10 10.0.0.1:1 -> 10.0.0.2:2",
            "21:31:21:060100 tcp  recv in  tid 2 cpu 2 size 10 10.0.0.2:2 <- 10.0.0.1:1",
            "21:31:21:060200 tcp  send in  tid 1 cpu 1 size 10 10.0.0.9:1 -> 10.0.0.8:2",  # 无关 IP 对
            "21:31:21:060300 sched_waking tid 3 cpu 4 comm x pid 5 target_cpu 4",
            "21:31:21:060400 tcp  send out tid 9 cpu 9 size 5",  # tcp 无地址 → 丢弃
            "21:31:21:060500 totally garbage line",
        ])
        path = self._write_bpf(lines)
        try:
            w = self._win(datetime(2026, 8, 21, 21, 31, 21, 50000),
                          datetime(2026, 8, 21, 21, 31, 21, 100000))
            res, trunc = nla.BpfScanner(path, [w], full_scan=True).scan()
            evs = res[("A", "client")]
            kinds = [(e["kind"], e["ts"].strftime("%H:%M:%S:%f")) for e in evs]
            self.assertEqual(kinds, [("tcp_send_in", "21:31:21:060000"),
                                     ("tcp_recv_in", "21:31:21:060100"),
                                     ("sched_waking", "21:31:21:060300")])
            self.assertEqual(trunc, set())
        finally:
            os.unlink(path)

    def test_seek_equals_full(self):
        lines = self._events([
            "21:31:21:060000 tcp  send in  tid 1 cpu 1 size 10 10.0.0.1:1 -> 10.0.0.2:2",
            "21:31:21:060100 tcp  recv in  tid 2 cpu 2 size 10 10.0.0.2:2 <- 10.0.0.1:1",
            "21:31:21:060300 sched_waking tid 3 cpu 4 comm x pid 5 target_cpu 4",
        ] + ["21:31:21:06%04d tcp  recv in  tid 4 cpu 4 size 10 10.0.0.2:2 <- 10.0.0.1:1"
             % i for i in range(400, 480)])  # 簇内多行
        path = self._write_bpf(lines)
        try:
            w1 = self._win(datetime(2026, 8, 21, 21, 31, 21, 50000),
                           datetime(2026, 8, 21, 21, 31, 21, 100000), trace="A")
            w2 = self._win(datetime(2026, 8, 21, 21, 31, 22, 0),
                           datetime(2026, 8, 21, 21, 31, 22, 5000), trace="B")
            full = nla.BpfScanner(path, [w1, w2], full_scan=True).scan()
            seek = nla.BpfScanner(path, [w1, w2], full_scan=False).scan()
            key = lambda r: {k: [(e["kind"], e["ts"].isoformat()) for e in v]
                             for k, v in r.items()}
            self.assertEqual(key(full[0]), key(seek[0]))
            self.assertEqual(len(seek[0][("A", "client")]), 3 + 80)
        finally:
            os.unlink(path)

    def test_seek_fallback_on_unsorted(self):
        # 乱序 > 1s：seek 自校验失败 → 自动回退 full，结果仍正确
        lines = self._events([
            "21:31:21:060000 tcp  send in  tid 1 cpu 1 size 10 10.0.0.1:1 -> 10.0.0.2:2",
            "21:31:22:500000 tcp  send in  tid 1 cpu 1 size 10 10.0.0.1:1 -> 10.0.0.2:2",
            "21:31:21:060100 tcp  recv in  tid 2 cpu 2 size 10 10.0.0.2:2 <- 10.0.0.1:1",
        ])
        path = self._write_bpf(lines)
        try:
            w = self._win(datetime(2026, 8, 21, 21, 31, 21, 50000),
                          datetime(2026, 8, 21, 21, 31, 21, 100000))
            res, _ = nla.BpfScanner(path, [w], full_scan=False).scan()
            kinds = sorted(e["kind"] for e in res[("A", "client")])
            self.assertEqual(kinds, ["tcp_recv_in", "tcp_send_in"])
        finally:
            os.unlink(path)

    def test_seek_skips_untimestamped_header(self):
        # 回归：bpftrace BEGIN printf 的无时间戳头行（用户 net.bt 实际输出）
        # 不能触发 seek 模式的 head>hi_b 提前 break，否则整个窗口漏读 0 事件
        lines = (["Tracing brpc_wkr & datasystem network events... (no IP filter)"]
                 + self._events([
                     "21:31:21:060000 tcp  send in  tid 1 cpu 1 size 10 10.0.0.1:1 -> 10.0.0.2:2",
                     "21:31:21:060100 tcp  recv in  tid 2 cpu 2 size 10 10.0.0.2:2 <- 10.0.0.1:1",
                 ]))
        path = self._write_bpf(lines)
        try:
            w = self._win(datetime(2026, 8, 21, 21, 31, 21, 50000),
                          datetime(2026, 8, 21, 21, 31, 21, 100000))
            for full in (False, True):
                scanner = nla.BpfScanner(path, [w], full_scan=full)
                res, _ = scanner.scan()
                kinds = sorted(e["kind"] for e in res[("A", "client")])
                self.assertEqual(kinds, ["tcp_recv_in", "tcp_send_in"])
                self.assertGreater(scanner.diag["n_read"], 0)
                # 诊断时间范围取首个有效 tod 行（而非 "Tracing..." 头）
                self.assertEqual(scanner.diag["file_first_tod"], "21:31:20:000000")
        finally:
            os.unlink(path)

    def test_sched_quota_truncation(self):
        lines = self._events([
            "21:31:21:060000 sched_waking tid 3 cpu 4 comm x pid 5 target_cpu 4",
            "21:31:21:060100 sched_waking tid 4 cpu 4 comm x pid 6 target_cpu 4",
            "21:31:21:060200 sched_waking tid 5 cpu 4 comm x pid 7 target_cpu 4",
        ])
        path = self._write_bpf(lines)
        try:
            w = self._win(datetime(2026, 8, 21, 21, 31, 21, 50000),
                          datetime(2026, 8, 21, 21, 31, 21, 100000))
            res, trunc = nla.BpfScanner(path, [w], full_scan=True,
                                        max_sched_events=1).scan()
            self.assertEqual(len(res[("A", "client")]), 1)
            self.assertEqual(trunc, {("A", "client")})
        finally:
            os.unlink(path)


class TestWarnWindowScan(unittest.TestCase):
    """warn 流式窗口扫描：替代整文件解析驻留内存。"""

    def _block(self, ts, pid=1):
        return ("[88019.268007][%s] !!! resched_latency_warn Triggered !!!\n"
                "Current CPU: 61 | Task Comm: kvtest | PID: %d, latency: 4000850\n"
                "stack line for %d\n" % (ts, pid, pid))

    def _write(self, content):
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as fh:
            fh.write(content)
            return fh.name

    def test_window_filter(self):
        content = ("header noise\n" + self._block("2026-08-21 21:31:05:123144", 1)
                   + self._block("2026-08-21 21:31:21:065000", 2)   # 窗口内
                   + self._block("2026-08-21 21:32:00:000001", 3))
        path = self._write(content)
        try:
            wins = {"A": (datetime(2026, 8, 21, 21, 31, 21, 50000),
                          datetime(2026, 8, 21, 21, 31, 21, 79000))}
            out = nla.scan_warn_windows(path, wins)
            self.assertEqual(len(out["A"]), 1)
            self.assertEqual(out["A"][0]["pid"], 2)
            self.assertEqual(out["A"][0]["latency_us"], 4000850)
            self.assertIn("stack line for 2", "\n".join(out["A"][0]["raw"]))
        finally:
            os.unlink(path)

    def test_multi_trace_windows(self):
        content = self._block("2026-08-21 21:31:21:060000", 10) \
                  + self._block("2026-08-21 22:00:00:000000", 20)
        path = self._write(content)
        try:
            wins = {"A": (datetime(2026, 8, 21, 21, 31, 21, 50000),
                          datetime(2026, 8, 21, 21, 31, 21, 79000)),
                    "B": (datetime(2026, 8, 21, 21, 59, 59, 0),
                          datetime(2026, 8, 21, 22, 0, 0, 100000))}
            out = nla.scan_warn_windows(path, wins)
            self.assertEqual([b["pid"] for b in out["A"]], [10])
            self.assertEqual([b["pid"] for b in out["B"]], [20])
        finally:
            os.unlink(path)

    def test_equivalence_with_full_parse(self):
        blocks = [self._block("2026-08-21 21:%02d:%02d:000000" % (m, s), m)
                  for m in range(0, 60, 2) for s in (0, 30)]
        content = "".join(blocks)
        path = self._write(content)
        try:
            wins = {"A": (datetime(2026, 8, 21, 21, 10, 0),
                          datetime(2026, 8, 21, 21, 20, 0))}
            out = nla.scan_warn_windows(path, wins)
            full = [b for b in nla.parse_latency_warn_blocks(path)
                    if b["ts"] and wins["A"][0] <= b["ts"] <= wins["A"][1]]
            self.assertEqual([b["pid"] for b in out["A"]], [b["pid"] for b in full])
        finally:
            os.unlink(path)


class TestReportCaps(unittest.TestCase):
    """HTML 报告体积上限保护（大日志下事件明细可能很多）。"""

    def _ev(self, i):
        ts = datetime(2026, 8, 21, 21, 31, 21) + timedelta(microseconds=i)
        return {"ts": ts, "kind": "tcp_recv_in", "tid": i, "cpu": 1, "raw": "x"}

    def test_events_table_row_cap(self):
        evs = [self._ev(i) for i in range(nla.EVENTS_TABLE_MAX_ROWS + 10)]
        tbl = nla._events_table(evs, "t")
        self.assertIn("仅列前 %d 条" % nla.EVENTS_TABLE_MAX_ROWS, tbl)
        self.assertEqual(tbl.count("<tr><td>"), nla.EVENTS_TABLE_MAX_ROWS)  # 数据行被截断

    def test_events_table_small_no_cap(self):
        evs = [self._ev(i) for i in range(10)]
        tbl = nla._events_table(evs, "t")
        self.assertNotIn("仅列前", tbl)

    def test_index_cap(self):
        import argparse
        ctxs = []
        for i in range(nla.INDEX_MAX_TRACES + 5):
            rec = nla.SlowRecord("t%d" % i, DAY, {"network_residual_us": "2000"},
                                 "x.log", "pod")
            ctx = nla.TraceContext(rec)
            ctx.conclusion = {"label": "L", "confidence": "高"}
            ctxs.append(ctx)
        ns = argparse.Namespace(residual_threshold=1000)
        out = nla.generate_report(ctxs, ns, "/tmp")
        self.assertIn("仅列前 %d 条" % nla.INDEX_MAX_TRACES, out)


class TestNodeMapping(unittest.TestCase):
    def test_longest_match(self):
        nodes = ["master", "worker1", "worker13"]
        self.assertEqual(nla.match_node("kvchachjpworker-0-worker13", nodes), "worker13")
        self.assertEqual(nla.match_node("kvchachjpclient-2-master_26", nodes), "master")
        self.assertEqual(nla.match_node("pod-on-worker1", nodes), "worker1")
        self.assertIsNone(nla.match_node("pod-on-other", nodes))


class TestConnIdentify(unittest.TestCase):
    def _ev(self, hh, mm, ss, us, kind, local, peer, tid=1):
        line = ("%02d:%02d:%02d:%06d tcp  %s tid %d cpu 1 size 100 %s:%d %s %s:%d\n"
                % (hh, mm, ss, us, kind.replace("tcp_", "").replace("_", " "), tid,
                   local[0], local[1], "->" if kind == "tcp_send_in" else "<-",
                   peer[0], peer[1]))
        return nla.parse_bpf_line(line, DAY)

    def test_identify_conn(self):
        cs_ts = datetime(2026, 8, 21, 21, 31, 21, 60757)  # 21:31:21.060757
        evs = [
            self._ev(21, 31, 21, 59900, "tcp_send_in", ("192.168.219.138", 37868),
                     ("192.168.102.161", 31501)),   # earlier conn, before ClientSend
            self._ev(21, 31, 21, 60777, "tcp_send_in", ("192.168.219.138", 37880),
                     ("192.168.102.161", 31501)),   # target conn
            self._ev(21, 31, 21, 60831, "tcp_send_in", ("192.168.219.138", 37868),
                     ("192.168.102.161", 31501)),   # other conn after
        ]
        conn = nla.BpfCorrelator._identify_conn(evs, cs_ts, "192.168.219.138", "192.168.102.161")
        self.assertEqual(conn, ("192.168.219.138", 37880, "192.168.102.161", 31501))

    def test_identify_conn_none(self):
        cs_ts = datetime(2026, 8, 21, 21, 31, 21, 607570)
        conn = nla.BpfCorrelator._identify_conn([], cs_ts, "1.1.1.1", "2.2.2.2")
        self.assertIsNone(conn)


class TestKernelSegments(unittest.TestCase):
    def test_segments_and_flags(self):
        rec = nla.SlowRecord("t", DAY, {"server_req_queue_us": "10", "server_exec_us": "240"},
                             "x.log", "pod")
        ctx = nla.TraceContext(rec)
        ctx.milestones = {
            "ClientSend": datetime(2026, 8, 21, 21, 31, 21, 60757),
            "ClientTcpSendIn": datetime(2026, 8, 21, 21, 31, 21, 60777),
            "ServerTcpRecvFirst": datetime(2026, 8, 21, 21, 31, 21, 60822),
            "ServerTcpRecvLast": datetime(2026, 8, 21, 21, 31, 21, 60827),
            "ServerRecv": datetime(2026, 8, 21, 21, 31, 21, 60848),
            "ServerSend": datetime(2026, 8, 21, 21, 31, 21, 61096),
            "ServerTcpSendIn": datetime(2026, 8, 21, 21, 31, 21, 61100),
            "ClientTcpRecvFirst": datetime(2026, 8, 21, 21, 31, 21, 61195),
            "ClientTcpRecvLast": datetime(2026, 8, 21, 21, 31, 21, 61203),
            "ClientRecv": datetime(2026, 8, 21, 21, 31, 21, 77001),
        }
        nla.build_kernel_segments(ctx)
        by_key = {s["key"]: s for s in ctx.kernel_segments}
        self.assertAlmostEqual(by_key["client_user_to_kernel"]["dur_us"], 20, delta=1)
        self.assertFalse(by_key["client_user_to_kernel"]["abnormal"])
        self.assertAlmostEqual(by_key["server_processing"]["dur_us"], 248, delta=1)
        self.assertAlmostEqual(by_key["client_kernel_to_user"]["dur_us"], 15798, delta=1)
        self.assertTrue(by_key["client_kernel_to_user"]["abnormal"])
        self.assertEqual(len(ctx.kernel_segments), len(nla.SEGMENT_DEFS))


@unittest.skipUnless(Path(SAMPLE_LOG_ROOT).is_dir(), "sample logs not available")
class TestEndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.disc, cls.contexts, cls.trace_lines = nla.analyze(SAMPLE_LOG_ROOT)

    def test_problem_count(self):
        self.assertEqual(len(self.contexts), 6)

    def test_trace_117c5c4a91c7(self):
        ctx = next(c for c in self.contexts
                   if c.trace_id.endswith("117c5c4a91c7"))
        # macro segments match the manual analysis (0.091 / 0.248 / 15.905 ms)
        self.assertAlmostEqual(ctx.macro["cs_sr"], 91, delta=50)
        self.assertAlmostEqual(ctx.macro["sr_ss"], 248, delta=50)
        self.assertAlmostEqual(ctx.macro["ss_cr"], 15905, delta=50)
        # node resolution
        self.assertEqual(ctx.client_node, "master")
        self.assertEqual(ctx.server_node, "worker13")
        self.assertEqual(ctx.client_ip, "192.168.219.138")
        self.assertEqual(ctx.server_ip, "192.168.102.161")
        self.assertEqual(ctx.conn, ("192.168.219.138", 37880, "192.168.102.161", 31501))
        # kernel milestones match manual timeline
        ms = ctx.milestones
        self.assertAlmostEqual((ms["ClientTcpSendIn"] - ms["ClientSend"]).total_seconds() * 1e6,
                               20, delta=15)
        self.assertAlmostEqual((ms["ServerTcpRecvLast"] - ms["ClientTcpSendIn"]).total_seconds() * 1e6,
                               50, delta=30)
        self.assertAlmostEqual((ms["ClientTcpRecvLast"] - ms["ServerTcpSendIn"]).total_seconds() * 1e6,
                               103, delta=60)
        # conclusion points at client kernel→user pickup (~15.8ms)
        self.assertEqual(ctx.conclusion["category"], "client_kernel_to_user_delay")
        seg = {s["key"]: s for s in ctx.kernel_segments}
        self.assertAlmostEqual(seg["client_kernel_to_user"]["dur_us"], 15798, delta=100)

    def test_conclusion_fields(self):
        for ctx in self.contexts:
            self.assertIn(ctx.conclusion["confidence"], ("高", "中", "低"))
            self.assertTrue(ctx.conclusion["label"])
            self.assertTrue(ctx.conclusion["evidence"])

    def test_report_generation(self):
        import argparse
        ns = argparse.Namespace(residual_threshold=1000)
        html_out = nla.generate_report(self.contexts, ns, SAMPLE_LOG_ROOT)
        self.assertIn("网络/调度时延定位分析报告", html_out)
        self.assertIn("117c5c4a91c7", html_out)


@unittest.skipUnless(Path(SAMPLE_LOG_ROOT).is_dir(), "sample logs not available")
class TestJsonOutput(unittest.TestCase):
    """generate_json：结构化原始结果（供其他工具/skill 二次消费）。"""

    @classmethod
    def setUpClass(cls):
        cls.disc, cls.contexts, cls.trace_lines = nla.analyze(SAMPLE_LOG_ROOT)

    def _dump(self):
        import argparse
        ns = argparse.Namespace(residual_threshold=1000)
        return json.loads(nla.generate_json(self.contexts, ns, SAMPLE_LOG_ROOT))

    def test_meta_and_summary(self):
        data = self._dump()
        self.assertEqual(data["schema"], "ds-network-latency-analysis/result")
        self.assertEqual(data["schema_version"], 1)
        self.assertEqual(data["log_root"], SAMPLE_LOG_ROOT)
        self.assertEqual(data["residual_threshold_us"], 1000)
        self.assertEqual(data["total_traces"], 6)
        self.assertEqual(sum(data["category_distribution"].values()), 6)
        self.assertEqual(len(data["traces"]), 6)

    def test_full_trace_payload(self):
        data = self._dump()
        tr = next(t for t in data["traces"] if t["trace_id"].endswith("117c5c4a91c7"))
        # 元信息与节点
        self.assertEqual(tr["client"]["node"], "master")
        self.assertEqual(tr["server"]["node"], "worker13")
        self.assertEqual(tr["client"]["ip"], "192.168.219.138")
        self.assertEqual(tr["server"]["ip"], "192.168.102.161")
        self.assertEqual(tr["conn"], {"client_ip": "192.168.219.138", "client_port": 37880,
                                      "server_ip": "192.168.102.161", "server_port": 31501})
        # 锚点：ISO 微秒精度
        self.assertEqual(tr["anchors"]["ClientSend"]["ts"],
                         "2026-08-21T21:31:21.060757")
        # 分段与结论
        seg = {s["key"]: s for s in tr["kernel_segments"]}
        self.assertAlmostEqual(seg["client_kernel_to_user"]["dur_us"], 15798, delta=100)
        self.assertTrue(seg["client_kernel_to_user"]["abnormal"])
        self.assertEqual(seg["client_kernel_to_user"]["start_ts"], "2026-08-21T21:31:21.061203")
        self.assertEqual(tr["conclusion"]["category"], "client_kernel_to_user_delay")
        self.assertEqual(tr["conclusion"]["confidence"], "高")
        self.assertIn("bottleneck", tr["conclusion"])
        # 内核事件与唤醒链
        self.assertTrue(tr["kernel_events"]["client"])
        sends = [e for e in tr["kernel_events"]["client"]
                 if e["kind"] == "tcp_send_in" and e.get("local") == "192.168.219.138:37880"]
        self.assertEqual(sends[0]["ts"], "2026-08-21T21:31:21.060777")
        self.assertEqual(sends[0]["peer"], "192.168.102.161:31501")
        self.assertTrue(sends[0]["raw"].startswith("21:31:21:060777 tcp"))
        self.assertTrue(any(e["kind"].startswith("sched") for e in tr["wakeup_chain"]))
        # 宏观三段
        macro = {m["key"]: m for m in tr["macro_segments"]}
        self.assertAlmostEqual(macro["ss_cr"]["dur_us"], 15905, delta=60)

    def test_evidence_missing_traces(self):
        data = self._dump()
        for tr in data["traces"]:
            self.assertIn(tr["conclusion"]["confidence"], ("高", "中", "低"))
            self.assertTrue(tr["conclusion"]["label"])
            self.assertIn("evidence", tr["conclusion"])
            self.assertIn("suggestions", tr["conclusion"])

    def test_cli_json_flag(self):
        # --json 参数存在且 main 会写文件（mock analyze 隔离文件系统）
        from unittest import mock
        with tempfile.TemporaryDirectory() as td:
            out_html = os.path.join(td, "r.html")
            out_json = os.path.join(td, "r.json")
            fake_ctxs = self.contexts[:2]
            with mock.patch.object(nla, "analyze", return_value=(None, fake_ctxs, {})), \
                 mock.patch.object(nla, "generate_report", return_value="<html>x</html>"):
                rc = nla.main([SAMPLE_LOG_ROOT, "-o", out_html, "--json", out_json])
            self.assertEqual(rc, 0)
            data = json.loads(open(out_json, encoding="utf-8").read())
            self.assertEqual(data["total_traces"], 2)


@unittest.skipUnless(Path(SAMPLE_LOG_ROOT).is_dir(), "sample logs not available")
class TestNoSchedCompat(unittest.TestCase):
    """唤醒链 sched_* 日志被关闭时的兼容降级（bpf 仅剩 tcp 事件）。"""

    TRACE = "getBuffer-25487-00004775;117c5c4a91c7"

    @classmethod
    def setUpClass(cls):
        cls.root = Path(tempfile.mkdtemp(prefix="nosched_"))
        cdir = cls.root / "collected" / "kvclient-1-master_26"
        wdir = cls.root / "collected_worker_logs" / "kvchachjpworker-0-worker1"
        bdir = cls.root / "dscollect_log"
        ldir = cls.root / "latency_warn_log"
        for d in (cdir, wdir, bdir, ldir):
            d.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, cls.TRACE, msg))

        # client 日志：SLOW + ClientSend/ClientRecv 锚点
        (cdir / "ds_client_1.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060757", "192.168.219.138",
                 "yyl9 ClientSend ts 88035205620370 tid 2")
            + info("2026-08-21T21:31:21.077001", "192.168.219.138",
                   "yyl9 ClientRecv ts 88035221862010 tid 2")
            + info("2026-08-21T21:31:21.077013", "192.168.219.138", SLOW_MSG),
            encoding="utf-8")
        # worker 日志：ServerRecv/ServerSend 锚点
        (wdir / "kvcache.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060843", "192.168.102.161",
                 "yyl3 ServerRecv ts 88038917594514 tid 4")
            + info("2026-08-21T21:31:21.061091", "192.168.102.161",
                   "yyl10 ServerSend ts 88038917846674 tid 4"),
            encoding="utf-8")
        # bpf：仅 tcp 事件，无 sched_*/sock_def_readable/tcpwakeup
        (bdir / "bpf-master-192.168.219.1.log").write_text(
            "21:31:21:060777 tcp  send in  tid 479093 cpu 50 size 270 "
            "192.168.219.138:37880 -> 192.168.102.161:31501\n"
            "21:31:21:061203 tcp  recv in  tid 479193 cpu 332 size 4096 "
            "192.168.219.138:37880 <- 192.168.102.161:31501, "
            "copied_seq:358067377, rcv_nxt:358067377\n",
            encoding="utf-8")
        (bdir / "bpf-worker1-192.168.102.1.log").write_text(
            "21:31:21:060842 tcp  recv que tid 594763 cpu 4 size 266 "
            "tp_rcv_nxt:4187256525, 192.168.102.161:31501 <- 192.168.219.138:37880\n"
            "21:31:21:060845 tcp  recv in  tid 594763 cpu 4 size 266 "
            "192.168.102.161:31501 <- 192.168.219.138:37880\n"
            "21:31:21:061091 tcp  send in  tid 396241 cpu 84 size 155 "
            "192.168.102.161:31501 -> 192.168.219.138:37880\n",
            encoding="utf-8")
        (ldir / "master_192.168.219.1").write_text("", encoding="utf-8")
        (ldir / "worker1_192.168.102.1").write_text("", encoding="utf-8")
        cls.disc, cls.contexts, cls.trace_lines = nla.analyze(str(cls.root))
        cls.ctx = cls.contexts[0]

    @classmethod
    def tearDownClass(cls):
        import shutil
        shutil.rmtree(cls.root, ignore_errors=True)

    def test_conclusion_still_correct(self):
        self.assertEqual(len(self.contexts), 1)
        c = self.ctx.conclusion
        self.assertEqual(c["category"], "client_kernel_to_user_delay")
        # 无唤醒链/告警佐证时按超阈值倍数判定，仍为高
        self.assertEqual(c["confidence"], "高")

    def test_missing_noted(self):
        self.assertTrue(any("唤醒链" in m for m in self.ctx.missing),
                        "missing 应注明唤醒链日志缺失")

    def test_suggestion_has_fallback(self):
        joined = "".join(self.ctx.conclusion["suggestions"])
        self.assertIn("latency_warn", joined)

    def test_wakeup_chain_no_sched_and_renders(self):
        self.assertFalse(any(e["kind"].startswith("sched") for e in self.ctx.wakeup_chain))
        import argparse
        ns = argparse.Namespace(residual_threshold=1000)
        html_out = nla.generate_report(self.contexts, ns, str(self.root))
        self.assertIn("唤醒链", html_out)
        data = json.loads(nla.generate_json(self.contexts, ns, str(self.root)))
        self.assertEqual(data["total_traces"], 1)
        self.assertTrue(any("唤醒链" in m for m in data["traces"][0]["missing_evidence"]))


class TestTraceInfoLines(unittest.TestCase):
    """collect_trace_info_lines：收集 trace 的全部 INFO 行（不只锚点）。"""

    TRACE_A = "getBuffer-25487-00004775;117c5c4a91c7"
    TRACE_SIMILAR = "getBuffer-25487-00004775;117c5c4a91c8"

    def _info(self, ts, host, trace, msg):
        return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n" % (ts, host, trace, msg))

    def test_collect_all_lines_exact_trace(self):
        with tempfile.TemporaryDirectory() as td:
            cdir = Path(td) / "collected" / "pod-master_1"
            cdir.mkdir(parents=True)
            clog = cdir / "ds_client_1.INFO.1.log"
            clog.write_text(
                self._info("2026-08-21T21:31:21.060757", "1.1.1.1", self.TRACE_A,
                           "yyl9 ClientSend ts 111 tid 5\n")
                + self._info("2026-08-21T21:31:21.061000", "1.1.1.1", self.TRACE_A,
                             "some intermediate business log line\n")
                + self._info("2026-08-21T21:31:21.077001", "1.1.1.1", self.TRACE_A,
                             "yyl9 ClientRecv ts 222 tid 5\n")
                + self._info("2026-08-21T21:31:21.060800", "1.1.1.1", self.TRACE_SIMILAR,
                             "yyl9 ClientSend ts 999 tid 9\n")
                + "noise without trace column\n", encoding="utf-8")
            idx = nla.collect_trace_info_lines([clog], [], [self.TRACE_A])
            self.assertEqual(len(idx[self.TRACE_A]), 3)  # 中间业务行也收集
            joined = "\n".join(l for _, _, l in idx[self.TRACE_A])
            self.assertIn("some intermediate business log line", joined)
            # 相似 trace 不误收
            self.assertNotIn(self.TRACE_SIMILAR, joined)
            self.assertEqual(idx.get(self.TRACE_SIMILAR), None)


@unittest.skipUnless(Path(SAMPLE_LOG_ROOT).is_dir(), "sample logs not available")
class TestRawOutput(unittest.TestCase):
    """generate_raw：问题请求相关原始日志汇总（标注来源）。"""

    @classmethod
    def setUpClass(cls):
        cls.disc, cls.contexts, cls.trace_lines = nla.analyze(SAMPLE_LOG_ROOT)
        cls.trace_ids = [ctx.trace_id for ctx in cls.contexts]

    def _raw(self):
        import argparse
        ns = argparse.Namespace(residual_threshold=1000)
        return nla.generate_raw(self.contexts, ns, SAMPLE_LOG_ROOT,
                                self.disc, self.trace_lines)

    def test_sections_and_sources(self):
        raw = self._raw()
        # 每 trace 一个分节头，含 trace_id 与结论
        self.assertIn("#2 trace=getBuffer-25487-00004775;117c5c4a91c7", raw)
        self.assertIn("结论：client 收包后唤醒/用户态取包慢（置信度:高）", raw)
        # 来源标注：client INFO / worker INFO / bpf（相对 log_root 路径）
        self.assertIn("---- client INFO 日志：collected/", raw)
        self.assertIn("---- worker INFO 日志：collected_worker_logs/", raw)
        self.assertIn("---- bpf 内核日志（client 节点 master，时间窗内）：dscollect_log/bpf-", raw)
        self.assertIn("---- bpf 内核日志（server 节点 worker13，时间窗内）：dscollect_log/bpf-", raw)
        # 原始行内容：SLOW 行 + 锚点行 + bpf 事件行
        self.assertIn("[BRPC_RPC_FRAMEWORK_SLOW]", raw)
        self.assertIn("yyl9 ClientSend ts 88035205620370", raw)
        self.assertIn("21:31:21:060777 tcp  send in", raw)
        # 连接四元组与窗口标注
        self.assertIn("连接：192.168.219.138:37880 <-> 192.168.102.161:31501", raw)

    def test_trace_2_full_payload(self):
        raw = self._raw()
        seg = raw.split("#2 trace=")[1]
        # 分节内含该 trace 的全部 INFO 行（SLOW + ClientSend/ClientRecv + ServerRecv/ServerSend）
        for marker in ("yyl3 ServerRecv ts 88038917594514",
                       "yyl10 ServerSend ts 88038917846674",
                       "yyl9 ClientRecv ts 88035221862010"):
            self.assertIn(marker, seg)
        # 唤醒链原始行（sched 事件）在 bpf 分节中
        self.assertIn("sched_waking", seg)

    def test_cli_raw_flag(self):
        from unittest import mock
        with tempfile.TemporaryDirectory() as td:
            out_html = os.path.join(td, "r.html")
            out_raw = os.path.join(td, "r.raw.log")
            fake_disc = mock.Mock()
            fake_disc.client_logs = []
            fake_disc.worker_logs = []
            fake_disc.bpf_by_node = {}
            fake_disc.warn_by_node = {}
            with mock.patch.object(nla, "analyze", return_value=(fake_disc, self.contexts[:2], {})), \
                 mock.patch.object(nla, "generate_report", return_value="<html>x</html>"):
                rc = nla.main([SAMPLE_LOG_ROOT, "-o", out_html, "--raw", out_raw])
            self.assertEqual(rc, 0)
            content = open(out_raw, encoding="utf-8").read()
            self.assertIn("#1 trace=", content)
            self.assertIn("结论：", content)


class TestSeekSlack(unittest.TestCase):
    """SEEK_SLACK 默认 2s + --seek-slack-s 参数化。"""

    def test_default_slack_is_2s(self):
        self.assertEqual(nla.SEEK_SLACK_US, 2 * 1000 * 1000)

    def test_slack_param_changeable(self):
        w1 = self._win()
        # 两个间隔 ~7s 的窗口：slack 10s 合并为一簇，slack 2s 保持两簇
        w2 = nla.TraceWindow("B", "client",
                             datetime(2026, 8, 21, 21, 31, 28),
                             datetime(2026, 8, 21, 21, 31, 28, 10000),
                             "10.0.0.1", "10.0.0.2")
        c_default = nla.merge_window_clusters([w1, w2])  # 默认 2s
        c_wide = nla.merge_window_clusters([w1, w2], slack_us=10 * 1000 * 1000)
        self.assertEqual(len(c_default), 2)
        self.assertEqual(len(c_wide), 1)

    def _win(self):
        return nla.TraceWindow("A", "client",
                               datetime(2026, 8, 21, 21, 31, 21, 50000),
                               datetime(2026, 8, 21, 21, 31, 21, 70000),
                               "10.0.0.1", "10.0.0.2")


class TestBpfDiag(unittest.TestCase):
    """零事件自动诊断：区分时间偏移 / IP 不匹配。"""

    TRACE = "getBuffer-25487-00004775;117c5c4a91c7"
    HOUR_US = 3600 * 1000 * 1000

    def _build_root(self, bpf_shift_us=0, bpf_ip="192.168.219.138"):
        """复用 TestNoSchedCompat 布局；bpf_shift_us 为 bpf 时间整体偏移。"""

        def tod(us_of_day):
            h, rem = divmod(us_of_day, 3600000000)
            m, rem = divmod(rem, 60000000)
            s, u = divmod(rem, 1000000)
            return "%02d:%02d:%02d:%06d" % (h, m, s, u)

        root = Path(tempfile.mkdtemp(prefix="bpfdiag_"))
        cdir = root / "collected" / "kvclient-1-master_26"
        wdir = root / "collected_worker_logs" / "kvchachjpworker-0-worker1"
        bdir = root / "dscollect_log"
        ldir = root / "latency_warn_log"
        for d in (cdir, wdir, bdir, ldir):
            d.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, self.TRACE, msg))

        (cdir / "ds_client_1.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060757", "192.168.219.138",
                 "yyl9 ClientSend ts 88035205620370 tid 2")
            + info("2026-08-21T21:31:21.077001", "192.168.219.138",
                   "yyl9 ClientRecv ts 88035221862010 tid 2")
            + info("2026-08-21T21:31:21.077013", "192.168.219.138", SLOW_MSG),
            encoding="utf-8")
        (wdir / "kvcache.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060843", "192.168.102.161",
                 "yyl3 ServerRecv ts 88038917594514 tid 4")
            + info("2026-08-21T21:31:21.061091", "192.168.102.161",
                   "yyl10 ServerSend ts 88038917846674 tid 4"),
            encoding="utf-8")
        # bpf send in：client 窗口起点 21:31:21.060757 → bpf 行整体 +shift
        base = (21 * 3600 + 31 * 60 + 21) * 1000000 + 60757
        (bdir / "bpf-master-192.168.219.1.log").write_text(
            "%s tcp  send in  tid 479093 cpu 50 size 270 %s:37880 -> 192.168.102.161:31501\n"
            % (tod(base + bpf_shift_us), bpf_ip), encoding="utf-8")
        (ldir / "master_192.168.219.1").write_text("", encoding="utf-8")
        return root

    def _run(self, root, extra_argv=()):
        import io
        import contextlib
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            disc, contexts, _tl = nla.analyze(str(root), *extra_argv)
        return contexts, err.getvalue()

    def tearDown(self):
        if hasattr(self, "_root"):
            import shutil
            shutil.rmtree(self._root, ignore_errors=True)

    def test_time_shift_diag(self):
        # bpf 整体快 1h → 窗口内时间匹配 0 → 诊断提示时钟/时区偏移
        self._root = self._build_root(bpf_shift_us=self.HOUR_US)
        contexts, err = self._run(self._root)
        self.assertEqual(len(contexts[0].kernel_events["client"]), 0)
        self.assertIn("零事件诊断", err)
        self.assertIn("疑似时钟/时区偏移", err)
        self.assertIn("--bpf-time-offset-ms", err)

    def test_ip_mismatch_diag(self):
        # bpf 行时间正确但连接 IP 不同 → 诊断输出样例 IP 对
        self._root = self._build_root(bpf_ip="10.99.0.1")
        contexts, err = self._run(self._root)
        self.assertEqual(len(contexts[0].kernel_events["client"]), 0)
        self.assertIn("零事件诊断", err)
        self.assertIn("10.99.0.1", err)
        self.assertIn("IP", err)

    def test_no_diag_when_matched(self):
        self._root = self._build_root()
        contexts, err = self._run(self._root)
        self.assertEqual(len(contexts[0].kernel_events["client"]), 1)
        self.assertNotIn("零事件诊断", err)


class TestTimeOffset(unittest.TestCase):
    """--bpf-time-offset-ms：bpf 时间整体快 1h 时用偏移修正。"""

    HOUR_MS = 3600 * 1000
    TRACE = TestBpfDiag.TRACE

    def setUp(self):
        self._root = TestBpfDiag._build_root(self, bpf_shift_us=3600 * 1000 * 1000)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._root, ignore_errors=True)

    def test_offset_restores_events(self):
        # 无偏移：0 事件（时间错位）
        disc, contexts, _tl = nla.analyze(str(self._root))
        self.assertEqual(len(contexts[0].kernel_events["client"]), 0)
        # 带偏移：bpf 快 1h → offset=+3600000ms 后事件恢复
        disc, contexts, _tl = nla.analyze(str(self._root),
                                     bpf_time_offset_ms=self.HOUR_MS)
        evs = contexts[0].kernel_events["client"]
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["kind"], "tcp_send_in")


class TestParallelScan(unittest.TestCase):
    """多进程文件级并行：workers>1 与串行结果一致。"""

    def _make_logs(self, n=6):
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        paths = []
        for i in range(n):
            d = Path(tmp) / ("pod%d-node1" % i)
            d.mkdir()
            p = d / ("ds_client_%d.INFO.1.log" % i)
            slow = SLOW_MSG.replace("network_residual_us=15989",
                                    "network_residual_us=%d" % (1000 + i * 100))
            p.write_text(
                ("2026-08-21T21:31:21.060757 | I | f:1 | 192.168.219.138 | 1:2 | "
                 "tr-%d | u |  yyl9 ClientSend ts %d tid 2\n" % (i, 1000 + i))
                + "noise line\n" * 200
                + ("2026-08-21T21:31:21.077001 | I | f:1 | 192.168.219.138 | 1:2 | "
                   "tr-%d | u |  yyl9 ClientRecv ts %d tid 2\n" % (i, 2000 + i))
                + "noise line\n" * 200
                + ("2026-08-21T21:31:21.077013 | I | f:1 | 192.168.219.138 | 1:2 | "
                   "tr-%d | u |  %s\n" % (i, slow)),
                encoding="utf-8")
            paths.append(p)
        return paths

    def test_slow_scan_parallel_equals_serial(self):
        paths = self._make_logs()
        serial = nla.scan_slow_records(paths, 1000, workers=1)
        par = nla.scan_slow_records(paths, 1000, workers=3)
        key = lambda rs: [(r.trace_id, r.fields["network_residual_us"], r.pod_dir)
                          for r in rs]
        self.assertEqual(key(serial), key(par))

    def test_anchor_info_parallel_equals_serial(self):
        paths = self._make_logs()
        traces = ["tr-%d" % i for i in range(6)]
        s_idx, s_lines = nla.collect_anchor_and_info(paths, [], traces, workers=1)
        p_idx, p_lines = nla.collect_anchor_and_info(paths, [], traces, workers=3)
        self.assertEqual(sorted(s_idx.keys()), sorted(p_idx.keys()))
        for t in traces:
            self.assertEqual([x["msg"] for x in s_idx[t]["client"]],
                             [x["msg"] for x in p_idx[t]["client"]])
            self.assertEqual([l for _, _, l in s_lines[t]],
                             [l for _, _, l in p_lines[t]])


class TestMergedScan(unittest.TestCase):
    """collect_anchor_and_info：锚点行 + 全部 INFO 行单遍收集。"""

    TRACE_A = "getBuffer-25487-00004775;117c5c4a91c7"
    TRACE_SIMILAR = "getBuffer-25487-00004775;117c5c4a91c8"

    def _info(self, ts, host, trace, msg):
        return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n" % (ts, host, trace, msg))

    def test_anchor_and_info_collected_together(self):
        with tempfile.TemporaryDirectory() as td:
            cdir = Path(td) / "collected" / "pod-master_1"
            cdir.mkdir(parents=True)
            clog = cdir / "ds_client_1.INFO.1.log"
            clog.write_text(
                self._info("2026-08-21T21:31:21.060757", "1.1.1.1", self.TRACE_A,
                           "yyl9 ClientSend ts 111 tid 5\n")
                + self._info("2026-08-21T21:31:21.061000", "1.1.1.1", self.TRACE_A,
                             "business intermediate line\n")
                + self._info("2026-08-21T21:31:21.060800", "1.1.1.1", self.TRACE_SIMILAR,
                             "yyl9 ClientSend ts 999 tid 9\n")
                + "noise without markers\n" * 50, encoding="utf-8")
            idx, lines = nla.collect_anchor_and_info([clog], [], [self.TRACE_A])
            # 锚点桶：仅精确 trace 的锚点行
            self.assertEqual([i["msg"] for i in idx[self.TRACE_A]["client"]],
                             ["yyl9 ClientSend ts 111 tid 5"])
            self.assertNotIn(self.TRACE_SIMILAR, idx)
            # INFO 行桶：含业务中间行
            joined = "\n".join(l for _, _, l in lines[self.TRACE_A])
            self.assertIn("business intermediate line", joined)
            self.assertNotIn(self.TRACE_SIMILAR, joined)


class TestAnchorBidCpu(unittest.TestCase):
    """锚点行新格式（tid N cpu N bid N）解析与旧格式向后兼容。"""

    NEW_SEND = "yyl9 ClientSend ts 222209933782554 tid 9433 cpu 92 bid 4294969385"
    NEW_RECV = "yyl9 ClientRecv ts 222209944133060 tid 9223 cpu 8 bid 4294969385"
    NEW_SRECV = "yyl3 ServerRecv ts 222214927454215 tid 6289 cpu 91 bid 300647719318"
    NEW_SSEND = "yyl10 ServerSend ts 222214928428372 tid 6313 cpu 82 bid 300647719318"
    OLD_SEND = "yyl9 ClientSend ts 88035205620370 tid 25586"

    def _info(self, msg, ts="2026-08-23T10:47:26.960359", host="192.168.189.131"):
        return {"ts": datetime(2026, 8, 23, 10, 47, 26, 960359), "host": host,
                "_pod_dir": "pod", "_path": "/p/x.log", "raw": msg}

    def test_new_format_captures_cpu_bid(self):
        for line, rx in ((self.NEW_SEND, nla.CLIENT_SEND_RE),
                         (self.NEW_RECV, nla.CLIENT_RECV_RE),
                         (self.NEW_SRECV, nla.SERVER_RECV_RE),
                         (self.NEW_SSEND, nla.SERVER_SEND_RE)):
            m = rx.search(line)
            self.assertIsNotNone(m, line)
            self.assertIsNotNone(m.group("cpu"))
            self.assertIsNotNone(m.group("bid"))
        m = nla.CLIENT_SEND_RE.search(self.NEW_SEND)
        self.assertEqual((m.group(1), m.group(2)), ("222209933782554", "9433"))
        self.assertEqual(m.group("cpu"), "92")
        self.assertEqual(m.group("bid"), "4294969385")

    def test_old_format_cpu_bid_none(self):
        m = nla.CLIENT_SEND_RE.search(self.OLD_SEND)
        self.assertEqual(m.group(1), "88035205620370")
        self.assertEqual(m.group(2), "25586")
        self.assertIsNone(m.group("cpu"))
        self.assertIsNone(m.group("bid"))

    def test_anchor_dict_fields(self):
        a = nla._anchor(self._info(self.NEW_SEND),
                        nla.CLIENT_SEND_RE.search(self.NEW_SEND))
        self.assertEqual(a["tid"], "9433")
        self.assertEqual(a["cpu"], "92")
        self.assertEqual(a["bid"], "4294969385")
        b = nla._anchor(self._info(self.OLD_SEND),
                        nla.CLIENT_SEND_RE.search(self.OLD_SEND))
        self.assertEqual(b["tid"], "25586")
        self.assertIsNone(b["cpu"])
        self.assertIsNone(b["bid"])
        c = nla._anchor(self._info(self.OLD_SEND), None)
        self.assertIsNone(c["tid"])

    def test_pick_anchor_ts_match_unaffected(self):
        # group(1)（ts 单调时钟值）精确匹配逻辑不受新捕获组影响
        info_new = self._info(self.NEW_SEND)
        info_new["msg"] = self.NEW_SEND
        got = nla._pick_anchor([info_new], "222209933782554", nla.CLIENT_SEND_RE, True)
        self.assertIsNotNone(got[0])
        got = nla._pick_anchor([info_new], "999", nla.CLIENT_SEND_RE, True)
        # 精确匹配失败时回退首个正则命中行
        self.assertIsNotNone(got[0])


class TestServerWakeupChain(unittest.TestCase):
    """server 侧唤醒链：内核收包 → ServerRecv（协程开始执行）。"""

    SIP, SPORT = "192.168.102.161", 31501
    CIP, CPORT = "192.168.219.138", 37868
    SR_TS = datetime(2026, 8, 21, 21, 31, 21, 20000)  # ServerRecv 时刻

    def _ev(self, line):
        return nla.parse_bpf_line(line, DAY)

    def _ms_and_events(self, with_sched=True):
        evs = [self._ev(BPF_RECV_QUE), self._ev(BPF_SOCK)]
        ms = {"ServerTcpRecvQue": evs[0]["ts"], "ServerSockReadable": evs[1]["ts"]}
        if with_sched:
            evs += [self._ev(BPF_WAKING), self._ev(BPF_WAKEUP), self._ev(BPF_SWITCH)]
        return ms, evs

    def _chain(self, ms, evs, sr_tid):
        return nla.BpfCorrelator._server_wakeup_chain(
            evs, ms, self.SR_TS, self.CIP, self.CPORT, self.SIP, self.SPORT, sr_tid)

    def test_full_chain_with_precise_tid(self):
        ms, evs = self._ms_and_events()
        chain, oncpu = self._chain(ms, evs, "396241")  # 锚点 tid == host pid
        self.assertEqual([e["kind"] for e in chain],
                         ["sock_readable", "sched_waking", "sched_wakeup", "sched_switch"])
        self.assertEqual(oncpu, self._ev(BPF_SWITCH)["ts"])

    def test_tid_mismatch_fallback_to_derived_chain(self):
        ms, evs = self._ms_and_events()
        chain, oncpu = self._chain(ms, evs, "6289")  # 容器 tid ≠ host pid
        self.assertEqual(len(chain), 4)
        self.assertEqual(oncpu, self._ev(BPF_SWITCH)["ts"])

    def test_no_sched_events(self):
        ms, evs = self._ms_and_events(with_sched=False)
        chain, oncpu = self._chain(ms, evs, "396241")
        self.assertEqual([e["kind"] for e in chain], ["sock_readable"])
        self.assertIsNone(oncpu)

    def test_no_sock_with_precise_tid(self):
        sw = self._ev(BPF_SWITCH)
        rq = self._ev(BPF_RECV_QUE)
        ms = {"ServerTcpRecvQue": rq["ts"]}
        chain, oncpu = self._chain(ms, [rq, sw], "396241")
        self.assertEqual([e["kind"] for e in chain], ["sched_switch"])
        self.assertEqual(oncpu, sw["ts"])

    def test_no_milestones(self):
        chain, oncpu = self._chain({}, [self._ev(BPF_SOCK)], "396241")
        self.assertEqual(chain, [])
        self.assertIsNone(oncpu)


class TestThreadSchedTrace(unittest.TestCase):
    """锚点线程调度轨迹：sched 事件按 tid 的 prev/next/pid 双向捕获。"""

    T0 = datetime(2026, 8, 21, 21, 31, 21, 11020)

    def _evs(self):
        # tid 396241 的轨迹：waking(011014) → wakeup(011016) → switch next(011020)
        # → switch prev(011100，被切出)；另有无关线程 999 的事件
        lines = [BPF_WAKING, BPF_WAKEUP, BPF_SWITCH,
                 "21:31:21:011100 sched_switch tid 0 cpu 4 prev_comm=brpc_wkr:0-22 "
                 "prev_pid=396241 next_comm=swapper/4 next_pid=0\n",
                 "21:31:21:011200 sched_waking tid 1 cpu 4 comm other pid 999 "
                 "target_cpu 4\n"]
        return [nla.parse_bpf_line(l, DAY) for l in lines]

    def test_trace_sorted_and_bidirectional(self):
        lo = self.T0 - timedelta(milliseconds=10)
        hi = self.T0 + timedelta(milliseconds=10)
        got = nla._thread_sched_trace(self._evs(), "396241", lo, hi)
        self.assertEqual([e["kind"] for e in got],
                         ["sched_waking", "sched_wakeup", "sched_switch", "sched_switch"])
        self.assertEqual(got[-1]["prev_pid"], 396241)  # 被切出方向也捕获

    def test_window_filter(self):
        # 窗口仅覆盖前两个事件
        lo = datetime(2026, 8, 21, 21, 31, 21, 11014)
        hi = datetime(2026, 8, 21, 21, 31, 21, 11016)
        got = nla._thread_sched_trace(self._evs(), "396241", lo, hi)
        self.assertEqual([e["kind"] for e in got], ["sched_waking", "sched_wakeup"])

    def test_unknown_tid_empty(self):
        got = nla._thread_sched_trace(self._evs(), "12345", self.T0, self.T0)
        self.assertEqual(got, [])

    def test_none_tid_empty(self):
        self.assertEqual(nla._thread_sched_trace(self._evs(), None, self.T0, self.T0), [])
        self.assertEqual(nla._thread_sched_trace(self._evs(), "abc", self.T0, self.T0), [])


class TestCoroutineEvidence(unittest.TestCase):
    """协程迁移与 CPU 一致性证据（用户样例：同 bid 不同 tid/cpu）。"""

    def _ctx(self, sr, ss, cs=None, client_events=(), server_events=(), ms=None):
        ctx = mock.Mock(spec=nla.TraceContext)
        ctx.anchors = {"ServerRecv": sr, "ServerSend": ss}
        if cs:
            ctx.anchors["ClientSend"] = cs
        ctx.milestones = ms or {}
        ctx.kernel_events = {"client": list(client_events), "server": list(server_events)}
        ctx.coro_evidence = []
        ctx.migration = None
        nla._coroutine_evidence(ctx)
        return ctx

    def _sr(self, tid="6289", cpu="91", bid="300647719318"):
        return {"ts": DAY, "tid": tid, "cpu": cpu, "bid": bid}

    def test_migration_detected(self):
        # 用户样例：ServerRecv(6289,91) → ServerSend(6313,82)，同 bid
        ctx = self._ctx(self._sr(), self._sr(tid="6313", cpu="82"))
        self.assertIsNotNone(ctx.migration)
        self.assertEqual(ctx.migration["bid"], "300647719318")
        self.assertEqual(ctx.migration["recv_tid"], "6289")
        self.assertEqual(ctx.migration["send_cpu"], "82")
        self.assertTrue(any("跨线程迁移" in s for s in ctx.coro_evidence))

    def test_no_migration_same_thread(self):
        ctx = self._ctx(self._sr(), self._sr())
        self.assertIsNone(ctx.migration)
        self.assertEqual(ctx.coro_evidence, [])

    def test_no_migration_without_bid(self):
        # 旧格式无 bid：不误报
        ctx = self._ctx(self._sr(bid=None), self._sr(tid="6313", bid=None))
        self.assertIsNone(ctx.migration)

    def test_send_path_cpu_cross(self):
        cs = {"ts": DAY, "tid": "9433", "cpu": "92", "bid": "1"}
        send_in = self._ev(BPF_SEND_IN)  # cpu 50 ≠ 92
        ms = {"ClientTcpSendIn": send_in["ts"]}
        ctx = self._ctx(self._sr(), self._sr(), cs=cs,
                        client_events=[send_in], ms=ms)
        self.assertTrue(any("发送路径跨核" in s for s in ctx.coro_evidence))

    def test_recv_softirq_cpu_cross(self):
        recv_in = self._ev(BPF_RECV_IN)  # cpu 332 ≠ ServerRecv cpu 91
        ms = {"ServerTcpRecvFirst": recv_in["ts"]}
        ctx = self._ctx(self._sr(cpu="91"), self._sr(cpu="91"),
                        server_events=[recv_in], ms=ms)
        self.assertTrue(any("跨核收包" in s for s in ctx.coro_evidence))

    def _ev(self, line):
        return nla.parse_bpf_line(line, DAY)


class TestCoroutineScheduleDelay(unittest.TestCase):
    """server 收包→协程执行 细分分段与 coroutine_schedule_delay 改判。"""

    T = datetime(2026, 8, 21, 21, 31, 21)

    def _ctx(self, oncpu_delay_us, readable_delay_us):
        """构造：recv_que(0) → sock_readable(1ms) → oncpu(1ms+r) → ServerRecv(+o)。

        oncpu_delay_us = oncpu→ServerRecv；readable_delay_us = readable→oncpu。
        """
        rq = self.T
        rd = rq + timedelta(milliseconds=1)
        oc = rd + timedelta(microseconds=readable_delay_us)
        sr = oc + timedelta(microseconds=oncpu_delay_us)
        ctx = mock.Mock(spec=nla.TraceContext)
        ctx.anchors = {"ServerRecv": {"ts": sr, "tid": "6289", "cpu": "91",
                                       "bid": "300647719318"}}
        ctx.milestones = {"ServerTcpRecvQue": rq, "ServerSockReadable": rd}
        ctx.kernel_segments = []       # 空的常规分段（细分不依赖）
        ctx.thread_oncpu_ts = oc
        ctx.thread_traces = {}
        ctx.coro_evidence = []
        ctx.nic_evidence = []
        ctx.migration = None
        ctx.server_wakeup_chain = []
        nla._server_pickup_segments(ctx)
        return ctx

    def test_pickup_segments_generated(self):
        ctx = self._ctx(oncpu_delay_us=5000, readable_delay_us=50)
        keys = [s["key"] for s in ctx.kernel_segments]
        self.assertEqual(keys, ["server_recvq_to_readable",
                                "server_readable_to_oncpu", "server_oncpu_to_user"])
        by_key = {s["key"]: s for s in ctx.kernel_segments}
        self.assertAlmostEqual(by_key["server_oncpu_to_user"]["dur_us"], 5000)
        self.assertAlmostEqual(by_key["server_readable_to_oncpu"]["dur_us"], 50)
        for s in ctx.kernel_segments:
            self.assertTrue(s["evidence"])
            self.assertFalse(s["abnormal"])
            self.assertIsNone(s["threshold_us"])

    def test_missing_oncpu_skips_related_segments(self):
        # thread_oncpu_ts 为 None：仅生成 recvq→readable 段
        ctx = self._ctx(0, 0)
        ctx.thread_oncpu_ts = None
        ctx.kernel_segments = []
        nla._server_pickup_segments(ctx)
        self.assertEqual([s["key"] for s in ctx.kernel_segments],
                         ["server_recvq_to_readable"])

    def _conclude_ctx(self, oncpu_delay_us, readable_delay_us):
        ctx = self._ctx(oncpu_delay_us, readable_delay_us)
        # 常规分段：server_kernel_to_user（异常）
        ctx.kernel_segments.insert(0, {
            "key": "server_kernel_to_user", "start": "ServerTcpRecvLast",
            "end": "ServerRecv",
            "dur_us": oncpu_delay_us + readable_delay_us + 1000,
            "threshold_us": 100, "category": "server_kernel_to_user_delay",
            "desc": "server 内核收包完成 → server 用户态 ServerRecv（唤醒/调度）",
            "abnormal": True})
        ctx.wakeup_chain = []
        ctx.warn_events = {"client": [], "server": []}
        ctx.irqoff_events = {"client": [], "server": []}
        ctx.missing = []
        ctx.slow = mock.Mock()
        ctx.slow.fields = {}
        nla.ConclusionEngine.conclude(ctx)
        return ctx

    def test_reclassified_to_coroutine_delay(self):
        # oncpu→ServerRecv(5ms) > readable→oncpu(50us)：改判协程调度排队
        ctx = self._conclude_ctx(oncpu_delay_us=5000, readable_delay_us=50)
        self.assertEqual(ctx.conclusion["category"], "coroutine_schedule_delay")
        self.assertTrue(any("协程排队" in e for e in ctx.conclusion["evidence"]))
        self.assertTrue(ctx.conclusion["suggestions"])

    def test_keeps_kernel_delay_when_wakeup_dominates(self):
        # readable→oncpu(5ms) > oncpu→ServerRecv(50us)：保持内核唤醒/调度分类
        ctx = self._conclude_ctx(oncpu_delay_us=50, readable_delay_us=5000)
        self.assertEqual(ctx.conclusion["category"], "server_kernel_to_user_delay")


class TestBidCpuEndToEnd(unittest.TestCase):
    """新格式（tid/cpu/bid）端到端：协程调度排队定界 + 迁移 + server 唤醒链。

    时间线（同一时钟域）：
      ClientSend .060757 → ClientTcpSendIn .060770 → server recv que/in .060800
      → sock_readable .060850 → waking .060860 → wakeup .060870
      → switch(next_pid=6289) .060900 →【协程排队 5ms】ServerRecv .065900
      → ServerSend .066300 → ClientRecv .077001
    预期：瓶颈段 server_kernel_to_user(5.1ms) 改判 coroutine_schedule_delay。
    """

    TRACE = "getBuffer-25487-00004775;117c5c4a91c7"

    def setUp(self):
        root = Path(tempfile.mkdtemp(prefix="bidcpu_"))
        self._root = root
        cdir = root / "collected" / "kvclient-1-master_26"
        wdir = root / "collected_worker_logs" / "kvchachjpworker-0-worker1"
        bdir = root / "dscollect_log"
        ldir = root / "latency_warn_log"
        for d in (cdir, wdir, bdir, ldir):
            d.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, self.TRACE, msg))

        (cdir / "ds_client_1.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060757", "192.168.219.138",
                 "yyl9 ClientSend ts 88035205620370 tid 2 cpu 92 bid 4294969385")
            + info("2026-08-21T21:31:21.077001", "192.168.219.138",
                   "yyl9 ClientRecv ts 88035221862010 tid 3 cpu 8 bid 4294969385")
            + info("2026-08-21T21:31:21.077013", "192.168.219.138", SLOW_MSG),
            encoding="utf-8")
        # ServerRecv/ServerSend：同 bid 不同 tid/cpu → 协程跨线程迁移
        (wdir / "kvcache.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.065900", "192.168.102.161",
                 "yyl3 ServerRecv ts 88038917594514 tid 6289 cpu 91 bid 300647719318")
            + info("2026-08-21T21:31:21.066300", "192.168.102.161",
                   "yyl10 ServerSend ts 88038917846674 tid 6313 cpu 82 bid 300647719318"),
            encoding="utf-8")

        (bdir / "bpf-master-192.168.219.1.log").write_text(
            "21:31:21:060770 tcp  send in  tid 479093 cpu 50 size 270 "
            "192.168.219.138:37880 -> 192.168.102.161:31501\n", encoding="utf-8")
        (bdir / "bpf-worker1-192.168.102.1.log").write_text(
            "21:31:21:060800 tcp  recv que tid 594763 cpu 4 size 266 "
            "tp_rcv_nxt:4187256525, 192.168.102.161:31501 <- 192.168.219.138:37880\n"
            "21:31:21:060801 tcp  recv in  tid 479193 cpu 332 size 4096 "
            "192.168.102.161:31501 <- 192.168.219.138:37880, "
            "copied_seq:358067377, rcv_nxt:358067377\n"
            "21:31:21:060850 sock_def_readable, tcp  wakeup 1 tid 594763 cpu 4 "
            "192.168.102.161:31501 <- 192.168.219.138:37880\n"
            "21:31:21:060860 sched_waking tid 594763 cpu 4 comm brpc_wkr:0-22 "
            "pid 6289 target_cpu 91, wq:0\n"
            "21:31:21:060870 sched_wakeup  tid 0 cur_comm:swapper/91 cpu 91 "
            "comm brpc_wkr:0-22 pid 6289, target_cpu:91\n"
            "21:31:21:060900 sched_switch tid 0 cpu 91 prev_comm=swapper/91 "
            "prev_pid=0 next_comm=brpc_wkr:0-22 next_pid=6289\n", encoding="utf-8")
        (ldir / "master_192.168.219.1").write_text("", encoding="utf-8")
        (ldir / "worker1_192.168.102.1").write_text("", encoding="utf-8")

    def tearDown(self):
        import shutil
        shutil.rmtree(self._root, ignore_errors=True)

    def test_coroutine_delay_end_to_end(self):
        disc, contexts, _tl = nla.analyze(str(self._root))
        ctx = contexts[0]
        # 锚点解析出新字段
        self.assertEqual(ctx.anchors["ServerRecv"]["bid"], "300647719318")
        self.assertEqual(ctx.anchors["ClientSend"]["cpu"], "92")
        # 协程迁移
        self.assertIsNotNone(ctx.migration)
        self.assertEqual(ctx.migration["recv_tid"], "6289")
        self.assertEqual(ctx.migration["send_tid"], "6313")
        # server 侧唤醒链 + 线程上 CPU 时刻
        self.assertTrue(ctx.server_wakeup_chain)
        self.assertEqual(ctx.thread_oncpu_ts,
                         datetime(2026, 8, 21, 21, 31, 21, 60900))
        # 细分分段：协程排队 5ms 主导
        by_key = {s["key"]: s for s in ctx.kernel_segments}
        self.assertIn("server_oncpu_to_user", by_key)
        self.assertGreater(by_key["server_oncpu_to_user"]["dur_us"], 4900)
        self.assertLess(by_key["server_readable_to_oncpu"]["dur_us"], 100)
        # 定界改判
        self.assertEqual(ctx.conclusion["category"], "coroutine_schedule_delay")
        self.assertTrue(any("协程排队" in e for e in ctx.conclusion["evidence"]))
        # 关键线程调度轨迹（锚点 tid 6289 命中 sched 事件）
        self.assertTrue(ctx.thread_traces.get("ServerRecv"))

    def test_json_output_new_fields(self):
        disc, contexts, _tl = nla.analyze(str(self._root))
        args = mock.Mock(residual_threshold=1000)
        doc = json.loads(nla.generate_json(contexts, args, str(self._root)))
        t = doc["traces"][0]
        self.assertEqual(t["anchors"]["ServerRecv"]["bid"], "300647719318")
        self.assertEqual(t["anchors"]["ServerSend"]["cpu"], "82")
        self.assertEqual(t["migration"]["recv_tid"], "6289")
        self.assertTrue(t["server_wakeup_chain"])
        self.assertTrue(t["thread_traces"]["ServerRecv"])
        self.assertTrue(t["coro_evidence"])
        self.assertTrue(any(s["key"] == "server_oncpu_to_user"
                            for s in t["kernel_segments"]))
        self.assertTrue(t["thread_oncpu_ts"])


NIC_DEV_START = ("20:45:39:650604 dev_start_xmit: sip:192.168.32.61, sport:39776 -> "
                 "dip:192.168.52.197, dport:31501, seq:1150439944, len:7254, "
                 "dev:cali089cfeed321\n")
NIC_NET_DEV_XMIT = ("20:45:39:650609 net_dev_xmit: sip:192.168.32.61, sport:39776 -> "
                    "dip:192.168.52.197, dport:31501, seq:1150439944, len:7254, "
                    "dev:cali089cfeed321, rc:0\n")
NIC_NETIF_RX = ("20:45:39:650568 netif_receive_skb: sip:192.168.32.61, sport:39776 -> "
                "dip:192.168.52.197, dport:31501, seq:1150439944, len:7240, "
                "dev:enp38s0f0np0\n")
TCP_RETRANS = ("20:45:39:651234 __tcp_retransmit_skb  tid 479093 cpu 50 size 7254 "
               "tx_seq: 1150439944, snd_una:1150430000, snd_next: 1150447144 "
               "tcb:seq: 1150439944,192.168.32.61:39776 -> 192.168.52.197:31501\n")

NIC_DAY = datetime(2026, 8, 23)


class TestNicParsers(unittest.TestCase):
    """网卡层点位解析：dev_start_xmit / net_dev_xmit / netif_receive_skb / 重传。"""

    def test_dev_start_xmit(self):
        ev = nla.parse_bpf_line(NIC_DEV_START, NIC_DAY)
        self.assertEqual(ev["kind"], "nic_dev_xmit_start")
        self.assertEqual(ev["src_ip"], "192.168.32.61")
        self.assertEqual(ev["src_port"], 39776)
        self.assertEqual(ev["dst_ip"], "192.168.52.197")
        self.assertEqual(ev["dst_port"], 31501)
        self.assertEqual(ev["seq"], 1150439944)
        self.assertEqual(ev["len"], 7254)
        self.assertEqual(ev["dev"], "cali089cfeed321")
        self.assertNotIn("rc", ev)
        self.assertEqual(ev["ts"], datetime(2026, 8, 23, 20, 45, 39, 650604))

    def test_net_dev_xmit_with_rc(self):
        ev = nla.parse_bpf_line(NIC_NET_DEV_XMIT, NIC_DAY)
        self.assertEqual(ev["kind"], "nic_dev_xmit")
        self.assertEqual(ev["rc"], 0)
        self.assertEqual(ev["dev"], "cali089cfeed321")

    def test_netif_receive_skb(self):
        ev = nla.parse_bpf_line(NIC_NETIF_RX, NIC_DAY)
        self.assertEqual(ev["kind"], "nic_rx_skb")
        self.assertEqual(ev["src_port"], 39776)
        self.assertEqual(ev["len"], 7240)
        self.assertEqual(ev["dev"], "enp38s0f0np0")

    def test_tcp_retransmit(self):
        ev = nla.parse_bpf_line(TCP_RETRANS, NIC_DAY)
        self.assertEqual(ev["kind"], "tcp_retransmit")
        self.assertEqual(ev["tid"], 479093)
        self.assertEqual(ev["cpu"], 50)
        self.assertEqual(ev["size"], 7254)
        self.assertEqual(ev["tx_seq"], 1150439944)
        self.assertEqual(ev["snd_una"], 1150430000)
        self.assertEqual(ev["snd_nxt"], 1150447144)
        self.assertEqual(ev["local_ip"], "192.168.32.61")
        self.assertEqual(ev["local_port"], 39776)
        self.assertEqual(ev["peer_ip"], "192.168.52.197")
        self.assertEqual(ev["peer_port"], 31501)
        self.assertEqual(ev["dir_arrow"], "->")

    def test_garbage_not_matched(self):
        # 缺字段/格式噪声不误匹配
        for line in ("20:45:39:650604 dev_start_xmit: sip:1.2.3.4\n",
                     "20:45:39:650604 netif_receive_skb garbage line here\n",
                     "20:45:39:650604 __tcp_retransmit_skb\n"):
            ev = nla.parse_bpf_line(line, NIC_DAY)
            self.assertEqual(ev["kind"], "other", line)


class TestNicWindowAttach(unittest.TestCase):
    """网卡/重传事件在窗口收集层的连接匹配（双向 IP）。"""

    CIP, SIP = "192.168.32.61", "192.168.52.197"

    def _run(self, lines):
        win = nla.TraceWindow("tr", "client",
                              datetime(2026, 8, 23, 20, 45, 39),
                              datetime(2026, 8, 23, 20, 45, 41),
                              self.CIP, self.SIP)
        scanner = nla.BpfScanner("/nonexistent", [win])
        cluster = scanner.clusters[0]
        results, counts, truncated = {}, {}, set()
        for line in lines:
            scanner._handle_line(line, cluster, results, counts, truncated)
        return results

    def test_nic_events_attached_bidirectionally(self):
        res = self._run([
            # client→server 方向（src=client）
            "20:45:39:650604 dev_start_xmit: sip:192.168.32.61, sport:39776 -> "
            "dip:192.168.52.197, dport:31501, seq:1, len:100, dev:cali0\n",
            # server→client 方向（src=server）
            "20:45:39:650568 netif_receive_skb: sip:192.168.52.197, sport:31501 -> "
            "dip:192.168.32.61, dport:39776, seq:2, len:200, dev:eth0\n",
        ])
        got = res[("tr", "client")]
        self.assertEqual([e["kind"] for e in got],
                         ["nic_dev_xmit_start", "nic_rx_skb"])  # 按传入顺序

    def test_unrelated_ip_not_attached(self):
        res = self._run([
            "20:45:39:650604 dev_start_xmit: sip:10.9.9.9, sport:1 -> "
            "dip:10.8.8.8, dport:31501, seq:1, len:100, dev:eth0\n",
        ])
        self.assertEqual(res, {})

    def test_retransmit_attached_via_tcp_branch(self):
        res = self._run([TCP_RETRANS])
        got = res[("tr", "client")]
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["kind"], "tcp_retransmit")


class TestNicMilestones(unittest.TestCase):
    """网卡里程碑：方向/侧别判定 + first 语义 + 时间线渲染。"""

    CIP, CPORT = "192.168.32.61", 39776
    SIP, SPORT = "192.168.52.197", 31501

    def _ev(self, kind, src, dst, us):
        return {"kind": kind, "src_ip": src, "src_port": 1, "dst_ip": dst,
                "dst_port": 2, "seq": 1, "len": 100, "dev": "eth0",
                "ts": datetime(2026, 8, 23, 20, 45, 39, us)}

    def _fill(self, evs, side):
        ms = {}
        for ev in evs:
            nla.BpfCorrelator._fill_milestone(
                ms, ev, self.CIP, self.CPORT, self.SIP, self.SPORT, side)
        return ms

    def test_client_send_direction(self):
        ms = self._fill([
            self._ev("nic_dev_xmit_start", self.CIP, self.SIP, 650604),
            self._ev("nic_dev_xmit", self.CIP, self.SIP, 650609),
            # 反方向收包（client 收 server 包）在 client 侧也应有里程碑
            self._ev("nic_rx_skb", self.SIP, self.CIP, 660000),
        ], "Client")
        self.assertEqual(ms["ClientDevStartXmit"].microsecond, 650604)
        self.assertEqual(ms["ClientNetDevXmit"].microsecond, 650609)
        self.assertEqual(ms["ClientNetifRx"].microsecond, 660000)

    def test_server_direction(self):
        ms = self._fill([
            self._ev("nic_rx_skb", self.CIP, self.SIP, 650568),   # server 收 client 包
            self._ev("nic_dev_xmit_start", self.SIP, self.CIP, 670000),  # server 发出
            self._ev("nic_dev_xmit", self.SIP, self.CIP, 670005),
        ], "Server")
        self.assertEqual(ms["ServerNetifRx"].microsecond, 650568)
        self.assertEqual(ms["ServerDevStartXmit"].microsecond, 670000)
        self.assertEqual(ms["ServerNetDevXmit"].microsecond, 670005)

    def test_wrong_side_not_matched(self):
        # client 发出方向的事件出现在 server 侧上下文（side=Server 且 src==cip）
        # → ServerNetifRx 允许（server 收包），但 DevStartXmit 不允许
        ms = self._fill([self._ev("nic_dev_xmit_start", self.CIP, self.SIP, 650604)],
                        "Server")
        self.assertNotIn("ServerDevStartXmit", ms)
        self.assertNotIn("ClientDevStartXmit", ms)

    def test_first_wins_for_duplicate_dev_rx(self):
        # 同包多 dev 重复触发 netif_receive_skb：取最早
        ms = self._fill([
            self._ev("nic_rx_skb", self.CIP, self.SIP, 660000),
            self._ev("nic_rx_skb", self.CIP, self.SIP, 660020),
        ], "Server")
        self.assertEqual(ms["ServerNetifRx"].microsecond, 660000)

    def test_timeline_renders_nic_points(self):
        ms = {"ClientSend": datetime(2026, 8, 23, 20, 45, 39, 650000),
              "ClientTcpSendIn": datetime(2026, 8, 23, 20, 45, 39, 650100),
              "ClientDevStartXmit": datetime(2026, 8, 23, 20, 45, 39, 650604),
              "ClientNetDevXmit": datetime(2026, 8, 23, 20, 45, 39, 650609),
              "ServerNetifRx": datetime(2026, 8, 23, 20, 45, 39, 650568),
              "ClientRecv": datetime(2026, 8, 23, 20, 45, 39, 700000)}
        html_out = nla._timeline_html(ms, [])
        self.assertIn("ClientDevStartXmit→ClientNetDevXmit", html_out)
        self.assertIn("ServerNetifRx", html_out)
        # 旧格式（无网卡点）：缺失点位在点位明细表中显式标注，
        # 跨缺段（ClientTcpSendIn→ClientRecv）legend 带 ⚠ 标注
        old_ms = {k: v for k, v in ms.items()
                  if k not in ("ClientDevStartXmit", "ClientNetDevXmit", "ServerNetifRx")}
        old_html = nla._timeline_html(old_ms, [])
        self.assertIn("ClientDevStartXmit", old_html)          # 点位表列出
        self.assertIn("缺失", old_html)                        # 缺失 badge
        self.assertIn("⚠ 缺：", old_html)                      # legend 跨缺标注
        self.assertIn("seg gap", old_html)                     # 跨缺段斜纹样式


class TestNicSegments(unittest.TestCase):
    """网卡证据分段 + TCP 重传证据 + 传输类置信度提升。"""

    CIP, CPORT = "192.168.32.61", 39776
    SIP, SPORT = "192.168.52.197", 31501

    def _ctx(self, milestones=None, kernel_events=None):
        slow = nla.SlowRecord(
            "tr", datetime(2026, 8, 23, 20, 45, 39),
            {"network_residual_us": "2000", "e2e_us": "3000", "framework_us": "2500",
             "method": "m", "remote_processing_us": "0", "server_req_queue_us": "0",
             "server_exec_us": "0"},
            "/tmp/x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.client_ip, ctx.server_ip = self.CIP, self.SIP
        ctx.conn = (self.CIP, self.CPORT, self.SIP, self.SPORT)
        ctx.milestones = milestones or {}
        ctx.kernel_events = kernel_events or {"client": [], "server": []}
        return ctx

    def _seg(self, ctx, key):
        return next((s for s in ctx.kernel_segments if s["key"] == key), None)

    def test_nic_segments_built_from_milestones(self):
        ms = {
            "ClientTcpSendIn": datetime(2026, 8, 23, 20, 45, 39, 650100),
            "ClientDevStartXmit": datetime(2026, 8, 23, 20, 45, 39, 650604),
            "ClientNetDevXmit": datetime(2026, 8, 23, 20, 45, 39, 650609),
            "ServerNetifRx": datetime(2026, 8, 23, 20, 45, 39, 650568),
            "ServerTcpRecvFirst": datetime(2026, 8, 23, 20, 45, 39, 651000),
        }
        ctx = self._ctx(milestones=ms)
        nla._nic_segments(ctx)
        seg = self._seg(ctx, "client_stack_to_nic")
        self.assertIsNotNone(seg)
        self.assertAlmostEqual(seg["dur_us"], 504)  # 650100 → 650604
        self.assertTrue(seg["evidence"])   # 证据段不参与异常竞争
        self.assertFalse(seg["abnormal"])
        self.assertEqual(seg["category"], "nic_evidence")
        self.assertAlmostEqual(self._seg(ctx, "client_nic_xmit")["dur_us"], 5)
        self.assertAlmostEqual(self._seg(ctx, "server_nic_to_stack")["dur_us"], 432)
        self.assertIsNone(self._seg(ctx, "client_nic_to_stack"))  # 缺 ClientNetifRx
        self.assertEqual(ctx.nic_evidence, [])  # 无重传 → 无证据行

    def test_retransmit_evidence_collected(self):
        ev = nla.parse_bpf_line(TCP_RETRANS, NIC_DAY)
        ctx = self._ctx(kernel_events={"client": [ev], "server": []})
        nla._nic_segments(ctx)
        self.assertTrue(any("TCP 重传" in s for s in ctx.nic_evidence))
        self.assertTrue(any("重传样例" in s for s in ctx.nic_evidence))
        # 重传证据并入结论 evidence（◆ 前缀）且传输类置信度提升为高
        ctx.kernel_segments = [{
            "key": "wire_c2s", "start": "ClientTcpSendIn", "end": "ServerTcpRecvFirst",
            "dur_us": 900, "threshold_us": 200, "category": "network_c2s_transmission",
            "desc": "d", "abnormal": True, "evidence": False}]
        ctx.milestones = {"ClientTcpSendIn": NIC_DAY, "ServerTcpRecvFirst": NIC_DAY}
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "network_c2s_transmission")
        self.assertEqual(ctx.conclusion["confidence"], "高")
        self.assertTrue(any(s.startswith("◆") for s in ctx.conclusion["evidence"]))

    def test_transmission_confidence_high_with_netif_rx_only(self):
        # 无重传但有网卡收包点位佐证 → 传输类同样高置信
        ctx = self._ctx(milestones={"ClientNetifRx": NIC_DAY})
        ctx.kernel_segments = [{
            "key": "wire_s2c", "start": "ServerTcpSendIn", "end": "ClientTcpRecvFirst",
            "dur_us": 900, "threshold_us": 200, "category": "network_s2c_transmission",
            "desc": "d", "abnormal": True, "evidence": False}]
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["confidence"], "高")


class TestNicRenderJson(unittest.TestCase):
    """网卡事件在 HTML 事件表与 JSON 序列化中的呈现。"""

    def test_event_json_nic_fields(self):
        ev = nla.parse_bpf_line(NIC_NET_DEV_XMIT, NIC_DAY)
        d = nla._event_json(ev)
        self.assertEqual(d["kind"], "nic_dev_xmit")
        self.assertEqual(d["src"], "192.168.32.61:39776")
        self.assertEqual(d["dst"], "192.168.52.197:31501")
        self.assertEqual(d["dev"], "cali089cfeed321")
        self.assertEqual(d["rc"], 0)
        self.assertEqual(d["seq"], 1150439944)
        self.assertEqual(d["len"], 7254)

    def test_event_json_retransmit_fields(self):
        ev = nla.parse_bpf_line(TCP_RETRANS, NIC_DAY)
        d = nla._event_json(ev)
        self.assertEqual(d["kind"], "tcp_retransmit")
        self.assertEqual(d["local"], "192.168.32.61:39776")
        self.assertEqual(d["peer"], "192.168.52.197:31501")
        self.assertEqual(d["tx_seq"], 1150439944)
        self.assertEqual(d["snd_una"], 1150430000)

    def test_events_table_nic_row(self):
        ev = nla.parse_bpf_line(NIC_NETIF_RX, NIC_DAY)
        out = nla._events_table([ev], "client 节点 bpf 事件")
        self.assertIn("nic_rx_skb", out)
        self.assertIn("192.168.32.61:39776 -&gt; 192.168.52.197:31501", out)
        self.assertIn("dev=enp38s0f0np0", out)

    def test_events_table_retransmit_row(self):
        ev = nla.parse_bpf_line(TCP_RETRANS, NIC_DAY)
        out = nla._events_table([ev], "client 节点 bpf 事件")
        self.assertIn("tcp_retransmit", out)
        self.assertIn("tx_seq=1150439944", out)


class TestGlobalTimeline(unittest.TestCase):
    """全路径时间线：16 点位明细表（5 个 net.bt 探针点位）+ 缺失标注 + 跨缺段 ⚠ legend。"""

    D = lambda self, us: datetime(2026, 8, 23, 20, 45, 39, us)  # noqa: E731

    def test_point_table_covers_all_16_points(self):
        out = nla._timeline_html({}, [])
        # 点位表 1 表头 + 16 行；层级标签 业务4/协议栈6/网卡6
        self.assertEqual(out.count("<tr>"), 17)
        self.assertEqual(out.count("<td>业务</td>"), 4)
        self.assertEqual(out.count("<td>协议栈</td>"), 6)
        self.assertEqual(out.count("<td>网卡</td>"), 6)
        for k in nla.POINT_ORDER:
            self.assertIn(k, out)
        # 主干只含 5 个 net.bt 探针点位；recv que（tcp_queue_rcv）在事件明细展开
        self.assertNotIn("ServerTcpRecvQue", out)
        self.assertNotIn("ClientTcpRecvQue", out)
        # 全缺失时条形图提示仍在
        self.assertIn("无法绘制时间线", out)
        self.assertEqual(out.count('badge b-low">缺失'), 16)

    def test_missing_points_annotated(self):
        ms = {"ClientSend": self.D(650000), "ClientTcpSendIn": self.D(650100),
              "ClientRecv": self.D(700000)}
        out = nla._timeline_html(ms, [])
        # 13 个缺失点位显式标注
        self.assertEqual(out.count('badge b-low">缺失'), 16 - 3)
        # 跨缺段（ClientTcpSendIn→ClientRecv）legend 标注被跳过的缺失点位
        self.assertIn("ClientTcpSendIn→ClientRecv", out)
        self.assertIn("⚠ 缺：ClientDevStartXmit、ClientNetDevXmit、ServerNetifRx", out)
        # 跨缺段加斜纹样式
        self.assertIn("seg gap", out)
        # 存在点位的时间正确渲染
        self.assertIn("20:45:39.650000", out)

    def test_anchor_only_timeline_renders(self):
        # 纯锚点（无 bpf 事件）：时间线仍按锚点分段渲染
        ms = {"ClientSend": self.D(650000), "ServerRecv": self.D(660000),
              "ServerSend": self.D(665000), "ClientRecv": self.D(700000)}
        out = nla._timeline_html(ms, [])
        self.assertIn("ClientSend→ServerRecv", out)
        self.assertIn("ServerRecv→ServerSend", out)
        self.assertIn("ServerSend→ClientRecv", out)
        # 跨缺段带 ⚠ 标注（ServerRecv→ServerSend 在全路径序中相邻，无缺失点）
        self.assertEqual(out.count("⚠ 缺："), 2)


class TestTimelineOrder(unittest.TestCase):
    """日志按时间线排序：kernel_events 数据层排序 + 锚点表时间序 + 表格保序。"""

    CIP, SIP = "192.168.219.138", "192.168.102.161"

    def _line(self, us, size=270):
        return ("21:31:21:%06d tcp  send in  tid 479093 cpu 50 size %d "
                "%s:37880 -> %s:31501\n" % (us, size, self.CIP, self.SIP))

    def test_correlate_kernel_sorts_events(self):
        slow = nla.SlowRecord(
            "tr", datetime(2026, 8, 21, 21, 31, 21),
            {"network_residual_us": "2000"}, "/tmp/x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.idx = 0
        ctx.client_node, ctx.server_node = "m1", "w1"
        ctx.server_pod_dir = "wpod"
        ctx.client_ip, ctx.server_ip = self.CIP, self.SIP
        ctx.anchors["ClientSend"] = {"ts": datetime(2026, 8, 21, 21, 31, 21, 50000),
                                     "tid": "5"}
        ctx.anchors["ClientRecv"] = {"ts": datetime(2026, 8, 21, 21, 31, 21, 80000),
                                     "tid": "5"}
        # 乱序输入（ts 递减）
        evs = [nla.parse_bpf_line(self._line(62000), DAY),
               nla.parse_bpf_line(self._line(58000), DAY),
               nla.parse_bpf_line(self._line(60000), DAY)]
        nla.correlate_kernel(ctx, {(0, "client"): evs, (0, "server"): []})
        ts_list = [e["ts"] for e in ctx.kernel_events["client"]]
        self.assertEqual(ts_list, sorted(ts_list))
        self.assertEqual([t.microsecond for t in ts_list], [58000, 60000, 62000])

    def test_anchor_table_sorted_by_time(self):
        slow = nla.SlowRecord(
            "tr", datetime(2026, 8, 21, 21, 31, 21),
            {"network_residual_us": "2000"}, "/tmp/x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.conclusion = {"category": "unknown", "label": "无法定界",
                          "confidence": "低", "evidence": [], "suggestions": []}
        # 插入序打乱：ClientRecv / ServerSend / ClientSend / ServerRecv
        for k, us in (("ClientRecv", 80000), ("ServerSend", 66000),
                      ("ClientSend", 50000), ("ServerRecv", 60000)):
            ctx.anchors[k] = {"ts": datetime(2026, 8, 21, 21, 31, 21, us),
                              "tid": "5", "cpu": None, "bid": None,
                              "host": "h", "pod_dir": "p", "log_path": "/l",
                              "raw": "ANCH_" + k}
        out = nla._trace_html(ctx, 1)
        # 锚点表按时间序：ClientSend → ServerRecv → ServerSend → ClientRecv
        self.assertLess(out.index("ANCH_ClientSend"), out.index("ANCH_ServerRecv"))
        self.assertLess(out.index("ANCH_ServerRecv"), out.index("ANCH_ServerSend"))
        self.assertLess(out.index("ANCH_ServerSend"), out.index("ANCH_ClientRecv"))

    def test_events_table_preserves_input_order(self):
        # 排序职责在 correlate_kernel（数据层）；渲染函数保持传入序
        evs = [nla.parse_bpf_line(self._line(62000), DAY),
               nla.parse_bpf_line(self._line(58000), DAY)]
        out = nla._events_table(evs, "t")
        self.assertLess(out.index("21:31:21.062000"), out.index("21:31:21.058000"))


class TestNicEndToEnd(unittest.TestCase):
    """网卡点位端到端：全路径时间线 + 证据分段 + 重传证据 + 原始日志汇总。

    时间线（同一时钟域）：
      ClientSend .060757 → ClientTcpSendIn .060770 → ClientDevStartXmit .060790
      → ClientNetDevXmit .060800 →(重传 .060850)→ ServerNetifRx .060810
      → ServerTcpRecvQue .060820 → ServerRecv .061900 → ServerSend .062300
      → ServerTcpSendIn .062400 → ClientNetifRx .070000 → ClientTcpRecvIn .070050
      → ClientTcpRecvQue .070100 → ClientRecv .077001
    预期：wire_s2c(7.65ms) 为瓶颈段 → network_s2c_transmission，
    网卡收包点位 + 重传证据 → 置信度高。
    """

    TRACE = "getBuffer-25487-00004775;117c5c4a91c7"
    CIP, SIP = "192.168.219.138", "192.168.102.161"

    def setUp(self):
        root = Path(tempfile.mkdtemp(prefix="nice2e_"))
        self._root = root
        cdir = root / "collected" / "kvclient-1-master_26"
        wdir = root / "collected_worker_logs" / "kvchachjpworker-0-worker1"
        bdir = root / "dscollect_log"
        ldir = root / "latency_warn_log"
        for d in (cdir, wdir, bdir, ldir):
            d.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, self.TRACE, msg))

        (cdir / "ds_client_1.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060757", self.CIP,
                 "yyl9 ClientSend ts 88035205620370 tid 2")
            + info("2026-08-21T21:31:21.077001", self.CIP,
                   "yyl9 ClientRecv ts 88035221862010 tid 3")
            + info("2026-08-21T21:31:21.077013", self.CIP, SLOW_MSG),
            encoding="utf-8")
        (wdir / "kvcache.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.061900", self.SIP,
                 "yyl3 ServerRecv ts 88038917594514 tid 7")
            + info("2026-08-21T21:31:21.062300", self.SIP,
                   "yyl10 ServerSend ts 88038917846674 tid 7"),
            encoding="utf-8")

        (bdir / "bpf-master-192.168.219.1.log").write_text(
            "21:31:21:060770 tcp  send in  tid 479093 cpu 50 size 270 "
            "%s:37880 -> %s:31501\n" % (self.CIP, self.SIP)
            + "21:31:21:060790 dev_start_xmit: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1150439944, len:7254, dev:cali089cfeed321\n"
              % (self.CIP, self.SIP)
            + "21:31:21:060800 net_dev_xmit: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1150439944, len:7254, dev:cali089cfeed321, rc:0\n"
              % (self.CIP, self.SIP)
            + "21:31:21:060850 __tcp_retransmit_skb  tid 479093 cpu 50 size 7254 "
              "tx_seq: 1150439944, snd_una:1150430000, snd_next: 1150447144 "
              "tcb:seq: 1150439944,%s:37880 -> %s:31501\n" % (self.CIP, self.SIP)
            + "21:31:21:070000 netif_receive_skb: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2222, len:120, dev:enp38s0f0np0\n" % (self.SIP, self.CIP)
            + "21:31:21:070050 tcp  recv in  tid 479193 cpu 332 size 120 "
              "%s:37880 <- %s:31501, copied_seq:358067377, rcv_nxt:358067377\n"
              % (self.CIP, self.SIP)
            + "21:31:21:070100 tcp  recv que tid 479193 cpu 332 size 120 "
              "tp_rcv_nxt:4187256525, %s:37880 <- %s:31501\n" % (self.CIP, self.SIP),
            encoding="utf-8")
        (bdir / "bpf-worker1-192.168.102.1.log").write_text(
            "21:31:21:060810 netif_receive_skb: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1150439944, len:7240, dev:enp38s0f0np0\n"
            % (self.CIP, self.SIP)
            + "21:31:21:060820 tcp  recv que tid 594763 cpu 4 size 266 "
              "tp_rcv_nxt:4187256525, %s:31501 <- %s:37880\n" % (self.SIP, self.CIP)
            + "21:31:21:060900 tcp  recv in  tid 396241 cpu 4 size 266 "
              "%s:31501 <- %s:37880, copied_seq:4187256525, rcv_nxt:4187256795\n"
              % (self.SIP, self.CIP)
            + "21:31:21:062400 tcp  send in  tid 594763 cpu 4 size 155 "
              "%s:31501 -> %s:37880\n" % (self.SIP, self.CIP),
            encoding="utf-8")
        (ldir / "master_192.168.219.1").write_text("", encoding="utf-8")
        (ldir / "worker1_192.168.102.1").write_text("", encoding="utf-8")

    def tearDown(self):
        import shutil
        shutil.rmtree(self._root, ignore_errors=True)

    def test_nic_full_path_end_to_end(self):
        disc, contexts, _tl = nla.analyze(str(self._root))
        ctx = contexts[0]
        ms = ctx.milestones
        # 网卡里程碑（全路径时间线上的 4 个新点位）
        self.assertEqual(ms["ClientDevStartXmit"],
                         datetime(2026, 8, 21, 21, 31, 21, 60790))
        self.assertEqual(ms["ClientNetDevXmit"],
                         datetime(2026, 8, 21, 21, 31, 21, 60800))
        self.assertEqual(ms["ServerNetifRx"],
                         datetime(2026, 8, 21, 21, 31, 21, 60810))
        self.assertEqual(ms["ClientNetifRx"],
                         datetime(2026, 8, 21, 21, 31, 21, 70000))
        # 网卡证据分段
        by_key = {s["key"]: s for s in ctx.kernel_segments}
        self.assertAlmostEqual(by_key["client_stack_to_nic"]["dur_us"], 20)
        self.assertAlmostEqual(by_key["client_nic_xmit"]["dur_us"], 10)
        # 终点为 ServerTcpRecvFirst（recvmsg 读到，含 veth 转发/排队/唤醒）
        self.assertAlmostEqual(by_key["server_nic_to_stack"]["dur_us"], 90)
        self.assertAlmostEqual(by_key["client_nic_to_stack"]["dur_us"], 50)
        # TCP 重传证据 + 传输类定界高置信
        self.assertTrue(any("TCP 重传" in s for s in ctx.nic_evidence))
        self.assertEqual(ctx.conclusion["category"], "network_s2c_transmission")
        self.assertEqual(ctx.conclusion["confidence"], "高")
        # 网卡事件进入事件明细（HTML 表格源数据）
        kinds = [e["kind"] for e in ctx.kernel_events["client"]]
        self.assertIn("nic_dev_xmit_start", kinds)
        self.assertIn("nic_dev_xmit", kinds)
        self.assertIn("nic_rx_skb", kinds)
        self.assertIn("tcp_retransmit", kinds)

    def test_nic_json_and_raw(self):
        import argparse
        disc, contexts, trace_lines = nla.analyze(str(self._root))
        ns = argparse.Namespace(residual_threshold=1000)
        doc = json.loads(nla.generate_json(contexts, ns, str(self._root)))
        t = doc["traces"][0]
        self.assertTrue(t["nic_evidence"])
        nic_evs = [e for e in t["kernel_events"]["client"] if e["kind"].startswith("nic_")]
        self.assertTrue(nic_evs)
        self.assertEqual(nic_evs[0]["src"], "192.168.219.138:37880")
        self.assertEqual(nic_evs[0]["dst"], "192.168.102.161:31501")
        self.assertEqual(nic_evs[0]["dev"], "cali089cfeed321")
        rets = [e for e in t["kernel_events"]["client"] if e["kind"] == "tcp_retransmit"]
        self.assertEqual(rets[0]["tx_seq"], 1150439944)
        self.assertTrue(any(s["key"] == "client_stack_to_nic" for s in t["kernel_segments"]))
        # trace 全量日志带上网卡事件原始行
        raw = nla.generate_raw(contexts, ns, str(self._root), disc, trace_lines)
        self.assertIn("dev_start_xmit", raw)
        self.assertIn("net_dev_xmit", raw)
        self.assertIn("netif_receive_skb", raw)
        self.assertIn("__tcp_retransmit_skb", raw)


class TestConn5tupleFilter(unittest.TestCase):
    """五元组过滤：_match_conn_5tuple 与 filtered_events 展示。"""

    CIP, CPORT = "10.0.0.1", 12345
    SIP, SPORT = "10.0.0.2", 8080

    def test_tcp_event_client_side_matches(self):
        ev = {"local_ip": self.CIP, "local_port": self.CPORT,
              "peer_ip": self.SIP, "peer_port": self.SPORT}
        self.assertTrue(nla._match_conn_5tuple(ev, self.CIP, self.CPORT, self.SIP, self.SPORT))

    def test_tcp_event_server_side_matches(self):
        ev = {"local_ip": self.SIP, "local_port": self.SPORT,
              "peer_ip": self.CIP, "peer_port": self.CPORT}
        self.assertTrue(nla._match_conn_5tuple(ev, self.CIP, self.CPORT, self.SIP, self.SPORT))

    def test_tcp_event_different_conn_rejected(self):
        ev = {"local_ip": self.CIP, "local_port": 9999,
              "peer_ip": self.SIP, "peer_port": self.SPORT}
        self.assertFalse(nla._match_conn_5tuple(ev, self.CIP, self.CPORT, self.SIP, self.SPORT))

    def test_nic_event_c2s_matches(self):
        ev = {"src_ip": self.CIP, "src_port": self.CPORT,
              "dst_ip": self.SIP, "dst_port": self.SPORT}
        self.assertTrue(nla._match_conn_5tuple(ev, self.CIP, self.CPORT, self.SIP, self.SPORT))

    def test_nic_event_s2c_matches(self):
        ev = {"src_ip": self.SIP, "src_port": self.SPORT,
              "dst_ip": self.CIP, "dst_port": self.CPORT}
        self.assertTrue(nla._match_conn_5tuple(ev, self.CIP, self.CPORT, self.SIP, self.SPORT))

    def test_nic_event_different_conn_rejected(self):
        ev = {"src_ip": "10.0.0.3", "src_port": 1,
              "dst_ip": "10.0.0.4", "dst_port": 2}
        self.assertFalse(nla._match_conn_5tuple(ev, self.CIP, self.CPORT, self.SIP, self.SPORT))

    def test_sched_event_always_passes(self):
        ev = {"kind": "sched_switch", "prev_pid": 123, "next_pid": 456}
        self.assertTrue(nla._match_conn_5tuple(ev, self.CIP, self.CPORT, self.SIP, self.SPORT))

    def test_no_conn_always_passes(self):
        ev = {"local_ip": self.CIP, "local_port": self.CPORT,
              "peer_ip": self.SIP, "peer_port": self.SPORT}
        self.assertTrue(nla._match_conn_5tuple(ev, None, None, None, None))


class TestFiveTupleFilteringInReport(unittest.TestCase):
    """端到端：五元组过滤后的 HTML / JSON / raw 输出。

    HTML 事件明细已升级为问题时间窗全景（问题连接高亮、其他连接混排），
    JSON / raw 仍按五元组过滤输出。
    """

    def setUp(self):
        self._root = Path(tempfile.mkdtemp(prefix="tst_5tuple_"))
        cdir = self._root / "collected" / "pod_node1_client"
        cdir.mkdir(parents=True)
        wdir = self._root / "collected_worker_logs" / "pod_node1_worker"
        wdir.mkdir(parents=True)
        bdir = self._root / "dscollect_log"
        bdir.mkdir(parents=True)
        wdir2 = self._root / "latency_warn_log"
        wdir2.mkdir(parents=True)

        # client log: 问题 trace
        slow_line = ("2026-08-22T10:00:00.200000 | I | f.cpp:1 | 10.0.0.1 | 1:100 | "
                     "t1;aaa |  |  "
                     + SLOW_MSG.replace("trace_id=getBuffer-25487-00004775;117c5c4a91c7",
                                        "trace_id=t1;aaa")
                     .replace("ClientSend=88035205620370", "ClientSend=100000000000")
                     .replace("ClientRecv=88035221862010", "ClientRecv=100000200000")
                     .replace("192.168.219.138", "10.0.0.1")
                     + "\n")
        (cdir / "c.log").write_text(
            "2026-08-22T10:00:00.100000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
            "t1;aaa |  |  yyl1 ClientSend ts 100000000000 tid 100 cpu 1\n"
            "2026-08-22T10:00:00.200000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
            "t1;aaa |  |  yyl1 ClientRecv ts 100000200000 tid 100 cpu 1\n"
            + slow_line,
            encoding="utf-8")
        # worker log: 锚点
        (wdir / "w.log").write_text(
            "2026-08-22T10:00:00.110000 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
            "t1;aaa |  |  yyl1 ServerRecv ts 100000110000 tid 200 cpu 2\n"
            "2026-08-22T10:00:00.150000 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
            "t1;aaa |  |  yyl1 ServerSend ts 100000150000 tid 200 cpu 2\n",
            encoding="utf-8")
        (wdir2 / "node1_latency_warn.log").write_text("", encoding="utf-8")

        self._bpf = bdir / "bpf-node1-192.168.1.1.log"

    def _write_bpf(self, content):
        (self._bpf).write_text(content, encoding="utf-8")

    def _run(self, **kw):
        ns = mock.Mock(spec=nla.argparse.Namespace)
        ns.residual_threshold = 1000
        ns.top = None
        ns.window_pad_ms = 2
        ns.sched_pad_ms = 10
        ns.bpf_full_scan = False
        ns.max_sched_events = 5000
        ns.verbose = False
        ns.workers = 1
        ns.seek_slack_s = 2.0
        ns.bpf_time_offset_ms = 0
        for k, v in kw.items():
            setattr(ns, k, v)
        disc, contexts, trace_lines = nla.analyze(
            str(self._root),
            residual_threshold=ns.residual_threshold,
            top=ns.top,
            window_pad_ms=ns.window_pad_ms,
            sched_pad_ms=ns.sched_pad_ms,
            bpf_full_scan=ns.bpf_full_scan,
            max_sched_events=ns.max_sched_events,
            verbose=ns.verbose,
            workers=ns.workers,
            seek_slack_s=ns.seek_slack_s,
            bpf_time_offset_ms=ns.bpf_time_offset_ms,
        )
        return disc, contexts, trace_lines, ns

    def test_html_events_filtered_by_5tuple(self):
        """HTML 事件明细按问题时间窗全景展示：问题五元组高亮，其他连接混排。"""
        self._write_bpf(
            # 匹配连接：10.0.0.1:12345 -> 10.0.0.2:8080
            "10:00:00:100050 tcp  send in  tid 1 cpu 1 size 100 "
            "10.0.0.1:12345 -> 10.0.0.2:8080\n"
            # 同节点无关连接（IP 相同但端口不同）：全景中直接展示并标注归属
            "10:00:00:100100 tcp  send in  tid 2 cpu 2 size 200 "
            "10.0.0.1:9999 -> 10.0.0.2:8888\n"
        )
        disc, contexts, _, ns = self._run()
        self.assertTrue(contexts)
        ctx = contexts[0]
        # 全量 2 条，五元组过滤后 1 条（无关 tcp 被过滤）
        self.assertEqual(len(ctx.kernel_events["client"]), 2)
        self.assertEqual(len(ctx.filtered_events["client"]), 1)
        # HTML 事件明细改为问题时间窗全景：问题连接高亮，其他连接直接混排展示
        html = nla._trace_html(ctx, 1)
        self.assertIn("问题时间窗全景", html)
        self.assertIn("10.0.0.1:12345", html)
        self.assertIn('class="hl5t"', html)
        self.assertIn("10.0.0.1:9999", html)
        self.assertIn("其他连接", html)

    def test_raw_events_filtered_by_5tuple(self):
        """raw 输出中 bpf 事件按五元组过滤。"""
        self._write_bpf(
            "10:00:00:100050 tcp  send in  tid 1 cpu 1 size 100 "
            "10.0.0.1:12345 -> 10.0.0.2:8080\n"
            "10:00:00:100100 tcp  send in  tid 2 cpu 2 size 200 "
            "10.0.0.1:9999 -> 10.0.0.2:8888\n"
        )
        disc, contexts, trace_lines, ns = self._run()
        raw = nla.generate_raw(contexts, ns, str(self._root), disc, trace_lines)
        self.assertIn("10.0.0.1:12345", raw)
        self.assertNotIn("10.0.0.1:9999", raw)
        self.assertIn("过滤后", raw)

    def test_json_events_filtered_by_5tuple(self):
        """JSON 输出中 kernel_events 按五元组过滤。"""
        self._write_bpf(
            "10:00:00:100050 tcp  send in  tid 1 cpu 1 size 100 "
            "10.0.0.1:12345 -> 10.0.0.2:8080\n"
            "10:00:00:100100 tcp  send in  tid 2 cpu 2 size 200 "
            "10.0.0.1:9999 -> 10.0.0.2:8888\n"
        )
        disc, contexts, _, ns = self._run()
        doc = json.loads(nla.generate_json(contexts, ns, str(self._root)))
        evs = doc["traces"][0]["kernel_events"]["client"]
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["local"], "10.0.0.1:12345")


class TestServerPrecedingCoroutine(unittest.TestCase):
    """前序协程执行轨迹：_scan_server_all_anchors + _server_preceding_coroutine_evidence。"""

    def setUp(self):
        self._root = Path(tempfile.mkdtemp(prefix="tst_precoro_"))
        cdir = self._root / "collected" / "pod_node1_client"
        cdir.mkdir(parents=True)
        wdir = self._root / "collected_worker_logs" / "pod_node1_worker"
        wdir.mkdir(parents=True)
        bdir = self._root / "dscollect_log"
        bdir.mkdir(parents=True)
        wdir2 = self._root / "latency_warn_log"
        wdir2.mkdir(parents=True)

        # server 日志：多个协程在同一 tid 上执行
        (wdir / "w.log").write_text(
            # 前序协程 A：ServerRecv → ServerSend
            "2026-08-22T10:00:00.100000 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
            "t_prev;bbb |  |  yyl1 ServerRecv ts 100000100000 tid 200 cpu 2 bid 111\n"
            "2026-08-22T10:00:00.150000 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
            "t_prev;bbb |  |  yyl1 ServerSend ts 100000150000 tid 200 cpu 2 bid 111\n"
            # 当前协程 B：ServerRecv（被前序协程阻塞）
            "2026-08-22T10:00:00.160000 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
            "t_cur;ccc |  |  yyl1 ServerRecv ts 100000160000 tid 200 cpu 2 bid 222\n"
            "2026-08-22T10:00:00.170000 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
            "t_cur;ccc |  |  yyl1 ServerSend ts 100000170000 tid 200 cpu 2 bid 222\n",
            encoding="utf-8")
        # client 日志
        slow_line = ("2026-08-22T10:00:00.200000 | I | f.cpp:1 | 10.0.0.1 | 1:100 | "
                     "t_cur;ccc |  |  "
                     + SLOW_MSG.replace("trace_id=getBuffer-25487-00004775;117c5c4a91c7",
                                        "trace_id=t_cur;ccc")
                     .replace("ClientSend=88035205620370", "ClientSend=100000090000")
                     .replace("ClientRecv=88035221862010", "ClientRecv=100000200000")
                     .replace("ServerRecv=88038917594514", "ServerRecv=100000160000")
                     .replace("ServerSend=88038917846674", "ServerSend=100000170000")
                     .replace("192.168.219.138", "10.0.0.1")
                     + "\n")
        (cdir / "c.log").write_text(
            "2026-08-22T10:00:00.090000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
            "t_cur;ccc |  |  yyl1 ClientSend ts 100000090000 tid 100 cpu 1\n"
            "2026-08-22T10:00:00.200000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
            "t_cur;ccc |  |  yyl1 ClientRecv ts 100000200000 tid 100 cpu 1\n"
            + slow_line,
            encoding="utf-8")
        # bpf: 让内核关联成功
        (bdir / "bpf-node1-192.168.1.1.log").write_text(
            "10:00:00:090050 tcp  send in  tid 1 cpu 1 size 100 "
            "10.0.0.1:12345 -> 10.0.0.2:8080\n"
            "10:00:00:200000 tcp  recv in  tid 1 cpu 1 size 100 "
            "10.0.0.1:12345 <- 10.0.0.2:8080, copied_seq:1, rcv_nxt:1\n",
            encoding="utf-8")
        (wdir2 / "node1_latency_warn.log").write_text("", encoding="utf-8")

    def _run(self, **kw):
        ns = mock.Mock(spec=nla.argparse.Namespace)
        ns.residual_threshold = 1000
        ns.top = None
        ns.window_pad_ms = 2
        ns.sched_pad_ms = 10
        ns.bpf_full_scan = False
        ns.max_sched_events = 5000
        ns.verbose = False
        ns.workers = 1
        ns.seek_slack_s = 2.0
        ns.bpf_time_offset_ms = 0
        for k, v in kw.items():
            setattr(ns, k, v)
        disc, contexts, trace_lines = nla.analyze(
            str(self._root),
            residual_threshold=ns.residual_threshold,
            top=ns.top,
            window_pad_ms=ns.window_pad_ms,
            sched_pad_ms=ns.sched_pad_ms,
            bpf_full_scan=ns.bpf_full_scan,
            max_sched_events=ns.max_sched_events,
            verbose=ns.verbose,
            workers=ns.workers,
            seek_slack_s=ns.seek_slack_s,
            bpf_time_offset_ms=ns.bpf_time_offset_ms,
        )
        return disc, contexts, trace_lines, ns

    def test_scan_all_anchors_collects_all(self):
        """_scan_server_all_anchors 收集全部 ServerRecv/ServerSend。"""
        wpath = self._root / "collected_worker_logs" / "pod_node1_worker" / "w.log"
        anchors = nla._scan_server_all_anchors(str(wpath))
        # 2 个 ServerRecv + 2 个 ServerSend
        kinds = [a[1] for a in anchors]
        self.assertEqual(kinds, ["ServerRecv", "ServerSend", "ServerRecv", "ServerSend"])
        self.assertTrue(all(a[0] for a in anchors))  # ts 有效

    def test_cache_reuses(self):
        """同文件多次扫描走缓存。"""
        wpath = str(self._root / "collected_worker_logs" / "pod_node1_worker" / "w.log")
        nla._server_anchors_cache.clear()
        a1 = nla._scan_server_all_anchors(wpath)
        a2 = nla._scan_server_all_anchors(wpath)
        a3 = nla._scan_server_all_anchors(wpath)
        self.assertEqual(len(a1), len(a2))
        # 第二次从缓存取
        self.assertIn(wpath, nla._server_anchors_cache)

    def test_preceding_coroutine_evidence_generated(self):
        """server_oncpu_to_user > 1ms 时生成前序协程证据。"""
        _, contexts, _, _ = self._run()
        ctx = contexts[0]
        # 手动设置 thread_oncpu_ts 模拟协程排队延迟
        ctx.thread_oncpu_ts = ctx.anchors["ServerRecv"]["ts"] - timedelta(milliseconds=5)
        nla._server_preceding_coroutine_evidence(ctx)
        self.assertTrue(any("前序协程" in e for e in ctx.coro_evidence),
                        "应生成前序协程证据: %s" % ctx.coro_evidence)

    def test_no_evidence_when_pickup_fast(self):
        """server_oncpu_to_user <= 1ms 时不生成前序协程证据。"""
        _, contexts, _, _ = self._run()
        ctx = contexts[0]
        ctx.thread_oncpu_ts = ctx.anchors["ServerRecv"]["ts"] - timedelta(microseconds=500)
        before = len(ctx.coro_evidence)
        nla._server_preceding_coroutine_evidence(ctx)
        self.assertEqual(len(ctx.coro_evidence), before)

    def test_latency_warn_in_window(self):
        """latency_warn 在协程排队窗口内时关联到证据。"""
        _, contexts, _, _ = self._run()
        ctx = contexts[0]
        oncpu = ctx.anchors["ServerRecv"]["ts"] - timedelta(milliseconds=5)
        ctx.thread_oncpu_ts = oncpu
        # 注入一条 latency_warn
        ctx.warn_events["server"] = [{
            "ts": oncpu + timedelta(milliseconds=1),
            "cpu": "2", "comm": "busy_task", "pid": "999",
            "latency_us": 5000000, "raw": ["dummy"],
        }]
        nla._server_preceding_coroutine_evidence(ctx)
        self.assertTrue(any("latency_warn 告警" in e for e in ctx.coro_evidence),
                        "应关联 latency_warn 告警: %s" % ctx.coro_evidence)

    def test_degraded_trigger_without_sched(self):
        """oncpu 缺失（无 sched 事件）时按 ServerTcpRecvFirst→ServerRecv 降级触发。"""
        # 追加 server 侧协议栈收包事件：ServerTcpRecvFirst=10:00:00.155
        bpath = self._root / "dscollect_log" / "bpf-node1-192.168.1.1.log"
        bpath.write_text(bpath.read_text(encoding="utf-8")
                         + "10:00:00:155000 tcp  recv in  tid 3 cpu 3 size 100 "
                         "10.0.0.2:8080 <- 10.0.0.1:12345, copied_seq:1, rcv_nxt:1\n",
                         encoding="utf-8")
        _, contexts, _, _ = self._run()
        ctx = contexts[0]
        self.assertIsNone(ctx.thread_oncpu_ts)
        self.assertIn("ServerTcpRecvFirst", ctx.milestones)
        # analyze 主流程已走降级路径触发
        self.assertTrue(any("降级判定" in e for e in ctx.coro_evidence),
                        "应生成降级触发证据: %s" % ctx.coro_evidence)
        rows = ctx.preceding_trace_lines["server"]
        self.assertTrue(rows)
        self.assertEqual(rows[-1][1], "▶ ServerRecv")

    def test_preceding_trace_lines_collected(self):
        """触发后收集轨迹明细行：时间升序、同 tid、含原始行、末行 ▶ 当前锚点。"""
        _, contexts, _, _ = self._run()
        ctx = contexts[0]
        # 窗口覆盖前序 ServerSend(t_prev @10:00:00.150)
        ctx.thread_oncpu_ts = ctx.anchors["ServerRecv"]["ts"] - timedelta(milliseconds=10)
        nla._server_preceding_coroutine_evidence(ctx)
        rows = ctx.preceding_trace_lines["server"]
        self.assertTrue(rows)
        tss = [r[0] for r in rows]
        self.assertEqual(tss, sorted(tss))
        self.assertTrue(all(r[2] == "200" for r in rows))  # 同 tid
        self.assertEqual([r[1] for r in rows], ["ServerSend", "▶ ServerRecv"])
        self.assertIn("ServerSend ts 100000150000", rows[0][6])  # 原始行
        self.assertIn("ServerRecv ts 100000160000", rows[-1][6])

    def test_preceding_trace_html_renders(self):
        """HTML 详情页渲染前序协程执行轨迹区块；未触发时不渲染。"""
        _, contexts, _, _ = self._run()
        ctx = contexts[0]
        out = nla._trace_html(ctx, 1)
        self.assertNotIn("前序协程执行轨迹", out)  # 默认夹具两侧均不触发
        ctx.thread_oncpu_ts = ctx.anchors["ServerRecv"]["ts"] - timedelta(milliseconds=10)
        nla._server_preceding_coroutine_evidence(ctx)
        out = nla._trace_html(ctx, 1)
        self.assertIn("server 侧前序协程执行轨迹", out)
        self.assertIn("▶ ServerRecv", out)
        self.assertIn("ServerSend ts 100000150000", out)

    def test_preceding_trace_raw_renders(self):
        """raw 汇总输出含前序协程执行轨迹段落。"""
        disc, contexts, trace_lines, ns = self._run()
        ctx = contexts[0]
        ctx.thread_oncpu_ts = ctx.anchors["ServerRecv"]["ts"] - timedelta(milliseconds=10)
        nla._server_preceding_coroutine_evidence(ctx)
        raw = nla.generate_raw(contexts, ns, str(self._root), disc, trace_lines)
        self.assertIn("前序协程执行轨迹（server 侧", raw)
        self.assertIn("[▶ ServerRecv]", raw)
        self.assertIn("[ServerSend] ", raw)

    def test_preceding_trace_json_field(self):
        """JSON 输出含 preceding_trace_lines 字段。"""
        _, contexts, _, ns = self._run()
        ctx = contexts[0]
        ctx.thread_oncpu_ts = ctx.anchors["ServerRecv"]["ts"] - timedelta(milliseconds=10)
        nla._server_preceding_coroutine_evidence(ctx)
        doc = json.loads(nla.generate_json(contexts, ns, str(self._root)))
        pl = doc["traces"][0]["preceding_trace_lines"]["server"]
        self.assertTrue(pl)
        self.assertEqual([r["kind"] for r in pl], ["ServerSend", "▶ ServerRecv"])
        self.assertEqual(pl[0]["tid"], "200")
        self.assertIn("ServerSend ts 100000150000", pl[0]["raw"])


class TestClientPreceding(unittest.TestCase):
    """client 侧前序协程轨迹：ClientTcpRecvFirst → ClientRecv >1ms 触发。

    时间线（同一时钟域）：
      ClientSend(t_cur) .090 → ServerTcpRecvFirst .149800 → ServerRecv .150
      → ServerSend .150400 → ClientTcpRecvFirst .190
      →【client 协程排队 10ms】ClientRecv(t_prev) .195 → ClientRecv(t_cur) .200
    """

    def setUp(self):
        self._root = Path(tempfile.mkdtemp(prefix="tst_precoroc_"))
        cdir = self._root / "collected" / "pod_node1_client"
        wdir = self._root / "collected_worker_logs" / "pod_node1_worker"
        bdir = self._root / "dscollect_log"
        wdir2 = self._root / "latency_warn_log"
        for d in (cdir, wdir, bdir, wdir2):
            d.mkdir(parents=True)

        slow_line = ("2026-08-22T10:00:00.200500 | I | f.cpp:1 | 10.0.0.1 | 1:100 | "
                     "t_cur;ccc |  |  "
                     + SLOW_MSG.replace("trace_id=getBuffer-25487-00004775;117c5c4a91c7",
                                        "trace_id=t_cur;ccc")
                     .replace("ClientSend=88035205620370", "ClientSend=100000090000")
                     .replace("ClientRecv=88035221862010", "ClientRecv=100000200000")
                     .replace("ServerRecv=88038917594514", "ServerRecv=100000150000")
                     .replace("ServerSend=88038917846674", "ServerSend=100000150400")
                     .replace("192.168.219.138", "10.0.0.1")
                     + "\n")
        # client 日志：同 tid 100 串行处理多个协程任务
        (cdir / "c.log").write_text(
            "2026-08-22T10:00:00.090000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
            "t_cur;ccc |  |  yyl1 ClientSend ts 100000090000 tid 100 cpu 1\n"
            # 前序 client 协程：发送早于窗口，响应 ClientRecv 在窗口内
            "2026-08-22T10:00:00.095000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
            "t_prev;bbb |  |  yyl1 ClientSend ts 100000095000 tid 100 cpu 1\n"
            "2026-08-22T10:00:00.195000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
            "t_prev;bbb |  |  yyl1 ClientRecv ts 100000195000 tid 100 cpu 1\n"
            "2026-08-22T10:00:00.200000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
            "t_cur;ccc |  |  yyl1 ClientRecv ts 100000200000 tid 100 cpu 1\n"
            + slow_line,
            encoding="utf-8")
        # server 日志：ServerTcpRecvFirst .149800 → ServerRecv .150（200us，不触发）
        (wdir / "w.log").write_text(
            "2026-08-22T10:00:00.150000 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
            "t_cur;ccc |  |  yyl3 ServerRecv ts 100000150000 tid 200 cpu 2\n"
            "2026-08-22T10:00:00.150400 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
            "t_cur;ccc |  |  yyl10 ServerSend ts 100000150400 tid 200 cpu 2\n",
            encoding="utf-8")
        # bpf：ClientTcpRecvFirst=10:00:00.190 → ClientRecv=10:00:00.200（10ms）
        (bdir / "bpf-node1-192.168.1.1.log").write_text(
            "10:00:00:090050 tcp  send in  tid 1 cpu 1 size 100 "
            "10.0.0.1:12345 -> 10.0.0.2:8080\n"
            "10:00:00:149800 tcp  recv in  tid 3 cpu 3 size 100 "
            "10.0.0.2:8080 <- 10.0.0.1:12345, copied_seq:1, rcv_nxt:1\n"
            "10:00:00:190000 tcp  recv in  tid 1 cpu 1 size 100 "
            "10.0.0.1:12345 <- 10.0.0.2:8080, copied_seq:1, rcv_nxt:1\n",
            encoding="utf-8")
        (wdir2 / "node1_latency_warn.log").write_text("", encoding="utf-8")

    def _run(self, **kw):
        ns = mock.Mock(spec=nla.argparse.Namespace)
        ns.residual_threshold = 1000
        ns.top = None
        ns.window_pad_ms = 2
        ns.sched_pad_ms = 10
        ns.bpf_full_scan = False
        ns.max_sched_events = 5000
        ns.verbose = False
        ns.workers = 1
        ns.seek_slack_s = 2.0
        ns.bpf_time_offset_ms = 0
        for k, v in kw.items():
            setattr(ns, k, v)
        disc, contexts, trace_lines = nla.analyze(
            str(self._root),
            residual_threshold=ns.residual_threshold,
            top=ns.top,
            window_pad_ms=ns.window_pad_ms,
            sched_pad_ms=ns.sched_pad_ms,
            bpf_full_scan=ns.bpf_full_scan,
            max_sched_events=ns.max_sched_events,
            verbose=ns.verbose,
            workers=ns.workers,
            seek_slack_s=ns.seek_slack_s,
            bpf_time_offset_ms=ns.bpf_time_offset_ms,
        )
        return disc, contexts, trace_lines, ns

    def test_client_trigger_and_evidence(self):
        """client 侧 ClientTcpRecvFirst→ClientRecv >1ms 触发前序协程证据。"""
        _, contexts, _, _ = self._run()
        ctx = contexts[0]
        self.assertIn("ClientTcpRecvFirst", ctx.milestones)
        self.assertTrue(any("前序协程" in e and "ClientRecv=" in e
                            for e in ctx.coro_evidence),
                        "应生成 client 前序协程证据: %s" % ctx.coro_evidence)
        rows = ctx.preceding_trace_lines["client"]
        self.assertTrue(rows)
        # 窗口 [190000, 200000]：前序 ClientRecv + 当前 ClientRecv（▶ 标记）
        self.assertEqual([r[1] for r in rows], ["ClientRecv", "▶ ClientRecv"])
        self.assertEqual(rows[0][5], "t_prev;bbb")  # 前序协程 trace_id
        self.assertEqual(rows[-1][5], "t_cur;ccc")

    def test_client_latency_warn_in_window(self):
        """client 侧 latency_warn 在窗口内时关联到证据。"""
        _, contexts, _, _ = self._run()
        ctx = contexts[0]
        lo = ctx.milestones["ClientTcpRecvFirst"]
        ctx.warn_events["client"] = [{
            "ts": lo + timedelta(milliseconds=5), "cpu": "2", "comm": "busy_task",
            "pid": "999", "latency_us": 4000000, "raw": ["dummy"]}]
        before = len(ctx.coro_evidence)
        nla._preceding_coroutine_evidence(ctx, "client")
        self.assertTrue(any("latency_warn 告警（client）" in e
                            for e in ctx.coro_evidence[before:]),
                        "应关联 client latency_warn: %s" % ctx.coro_evidence[before:])

    def test_client_no_trigger_when_fast(self):
        """协议栈收包→ClientRecv ≤1ms 时不触发 client 侧分析。"""
        bpath = self._root / "dscollect_log" / "bpf-node1-192.168.1.1.log"
        bpath.write_text(bpath.read_text(encoding="utf-8")
                         .replace("10:00:00:190000", "10:00:00:199999"),
                         encoding="utf-8")
        _, contexts, _, _ = self._run()
        ctx = contexts[0]
        self.assertFalse(any("前序协程" in e and "ClientRecv=" in e
                             for e in ctx.coro_evidence))
        self.assertFalse(ctx.preceding_trace_lines["client"])


class TestPhysWire(unittest.TestCase):
    """物理网卡间线路定界（seq 关联）：_phys_wire_evidence 单元测试。

    数据复刻 k8s 双跳 veth 链用户样例（时间缩放到 2026-08-23 20:45:39）：
      s2c: ServerTcpSendIn .060939 → server eth0 xmit(.060946)/cali rx(.060948)
           → enp38s0f0np0 xmit(.060958，物理网卡发出=同 seq 最后一个 xmit)
           → client enp38s0f0np0 rx(.067544，物理网卡收到=同 seq 第一个 rx)
           → ClientTcpRecvFirst .067601；wire=6586us，占线路段 98.9%
      c2s: ClientTcpSendIn .060770 → client enp38s0f0np0 xmit(.060793)
           → server enp38s0f0np0 rx(.060830) → ServerTcpRecvFirst .060885；wire=37us
    """

    CIP, CPORT = "192.168.42.205", 43144
    SIP, SPORT = "192.168.42.131", 31501
    PW_DAY = datetime(2026, 8, 23)

    def D(self, us):
        return datetime(2026, 8, 23, 20, 45, 39, us)

    def _nic(self, hhmmss_us, ev, sip, sport, dip, dport, seq, ln, dev, rc=None):
        line = ("%s %s: sip:%s, sport:%d -> dip:%s, dport:%d, seq:%d, len:%d, dev:%s"
                % (hhmmss_us, ev, sip, sport, dip, dport, seq, ln, dev))
        if rc is not None:
            line += ", rc:%d" % rc
        return nla.parse_bpf_line(line + "\n", self.PW_DAY)

    def _s2c_server_events(self):
        return [
            self._nic("20:45:39:060944", "dev_start_xmit", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 242, "eth0"),
            self._nic("20:45:39:060946", "net_dev_xmit", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 242, "eth0", 0),
            self._nic("20:45:39:060948", "netif_receive_skb", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 228, "calibad58b5daed"),
            self._nic("20:45:39:060957", "dev_start_xmit", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 242, "enp38s0f0np0"),
            self._nic("20:45:39:060958", "net_dev_xmit", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 242, "enp38s0f0np0", 0),
        ]

    def _s2c_client_events(self):
        return [
            self._nic("20:45:39:067544", "netif_receive_skb", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 228, "enp38s0f0np0"),
            self._nic("20:45:39:067566", "dev_start_xmit", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 242, "calia22497db8ca"),
            self._nic("20:45:39:067567", "net_dev_xmit", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 242, "calia22497db8ca", 0),
            self._nic("20:45:39:067579", "netif_receive_skb", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 228, "eth0"),
        ]

    def _c2s_client_events(self):
        return [
            self._nic("20:45:39:060779", "dev_start_xmit", self.CIP, self.CPORT,
                      self.SIP, self.SPORT, 1000, 266, "eth0"),
            self._nic("20:45:39:060781", "net_dev_xmit", self.CIP, self.CPORT,
                      self.SIP, self.SPORT, 1000, 266, "eth0", 0),
            self._nic("20:45:39:060783", "netif_receive_skb", self.CIP, self.CPORT,
                      self.SIP, self.SPORT, 1000, 252, "calia22497db8ca"),
            self._nic("20:45:39:060792", "dev_start_xmit", self.CIP, self.CPORT,
                      self.SIP, self.SPORT, 1000, 266, "enp38s0f0np0"),
            self._nic("20:45:39:060793", "net_dev_xmit", self.CIP, self.CPORT,
                      self.SIP, self.SPORT, 1000, 266, "enp38s0f0np0", 0),
        ]

    def _c2s_server_events(self):
        return [
            self._nic("20:45:39:060830", "netif_receive_skb", self.CIP, self.CPORT,
                      self.SIP, self.SPORT, 1000, 252, "enp38s0f0np0"),
            self._nic("20:45:39:060852", "dev_start_xmit", self.CIP, self.CPORT,
                      self.SIP, self.SPORT, 1000, 266, "calibad58b5daed"),
            self._nic("20:45:39:060853", "net_dev_xmit", self.CIP, self.CPORT,
                      self.SIP, self.SPORT, 1000, 266, "calibad58b5daed", 0),
            self._nic("20:45:39:060865", "netif_receive_skb", self.CIP, self.CPORT,
                      self.SIP, self.SPORT, 1000, 252, "eth0"),
        ]

    def _ctx(self, server_events, client_events, milestones=None):
        slow = nla.SlowRecord(
            "tr", datetime(2026, 8, 23, 20, 45, 39),
            {"network_residual_us": "6586", "e2e_us": "7000", "framework_us": "6500",
             "method": "m", "remote_processing_us": "0", "server_req_queue_us": "0",
             "server_exec_us": "0"},
            "/tmp/x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.client_ip, ctx.server_ip = self.CIP, self.SIP
        ctx.conn = (self.CIP, self.CPORT, self.SIP, self.SPORT)
        ctx.kernel_events = {"client": sorted(client_events, key=lambda e: e["ts"]),
                             "server": sorted(server_events, key=lambda e: e["ts"])}
        ms = {
            "ClientTcpSendIn": self.D(60770),
            "ServerTcpRecvFirst": self.D(60885),
            "ServerTcpSendIn": self.D(60939),
            "ClientTcpRecvFirst": self.D(67601),
        }
        ms.update(milestones or {})
        ctx.milestones = ms
        return ctx

    def _full_ctx(self):
        return self._ctx(self._s2c_server_events() + self._c2s_server_events(),
                         self._s2c_client_events() + self._c2s_client_events())

    def _seg(self, ctx, key):
        return next((s for s in ctx.kernel_segments if s["key"] == key), None)

    def test_s2c_phys_wire_points(self):
        """s2c：物理网卡发出取同 seq 最后一个 xmit，收到取第一个 rx，wire=6586us。"""
        ctx = self._full_ctx()
        nla._phys_wire_evidence(ctx)
        pw = ctx.phys_wire["s2c"]
        self.assertIsNotNone(pw)
        self.assertEqual(pw["seq"], 2000)
        self.assertEqual(pw["egress_side"], "server")
        self.assertEqual(pw["egress_dev"], "enp38s0f0np0")   # 最后一个 xmit（非最早的 eth0）
        self.assertEqual(pw["egress_ts"], self.D(60958))
        self.assertEqual(pw["ingress_side"], "client")
        self.assertEqual(pw["ingress_dev"], "enp38s0f0np0")  # 最早的 rx（物理网卡先收到）
        self.assertEqual(pw["ingress_ts"], self.D(67544))
        self.assertAlmostEqual(pw["wire_us"], 6586)
        self.assertAlmostEqual(pw["egress_internal_us"], 19)   # TcpSendIn→物理网卡发出
        self.assertAlmostEqual(pw["ingress_internal_us"], 57)  # 物理网卡收到→TcpRecvFirst
        self.assertAlmostEqual(pw["line_us"], 6662)
        self.assertAlmostEqual(pw["share_pct"], 98.9, delta=0.1)
        self.assertTrue(pw["dominant"])
        # 里程碑 + 证据段（不参与异常竞争）
        self.assertEqual(ctx.milestones["ServerPhysNicXmit"], self.D(60958))
        self.assertEqual(ctx.milestones["ClientPhysNicRx"], self.D(67544))
        seg = self._seg(ctx, "wire_s2c_phys")
        self.assertIsNotNone(seg)
        self.assertTrue(seg["evidence"])
        self.assertFalse(seg["abnormal"])
        self.assertAlmostEqual(seg["dur_us"], 6586)
        # 证据句：耗时、两侧 dev、seq、节点内排除
        joined = " | ".join(ctx.nic_evidence)
        self.assertIn("seq=2000", joined)
        self.assertIn("enp38s0f0np0", joined)
        self.assertIn("6.586 ms", joined)
        self.assertTrue(any("物理网卡间" in s for s in ctx.nic_evidence))

    def test_c2s_phys_wire_symmetric(self):
        """c2s 对称：client 最后一个 xmit → server 第一个 rx，wire=37us 非主导。"""
        ctx = self._full_ctx()
        nla._phys_wire_evidence(ctx)
        pw = ctx.phys_wire["c2s"]
        self.assertIsNotNone(pw)
        self.assertEqual(pw["seq"], 1000)
        self.assertEqual(pw["egress_dev"], "enp38s0f0np0")
        self.assertEqual(pw["egress_ts"], self.D(60793))
        self.assertEqual(pw["ingress_dev"], "enp38s0f0np0")
        self.assertEqual(pw["ingress_ts"], self.D(60830))
        self.assertAlmostEqual(pw["wire_us"], 37)
        self.assertFalse(pw["dominant"])
        self.assertEqual(ctx.milestones["ClientPhysNicXmit"], self.D(60793))
        self.assertEqual(ctx.milestones["ServerPhysNicRx"], self.D(60830))
        self.assertAlmostEqual(self._seg(ctx, "wire_c2s_phys")["dur_us"], 37)

    def test_missing_receiver_events_graceful(self):
        """对侧无 nic 事件：该方向 None，不抛异常、不生成段。"""
        ctx = self._ctx(self._s2c_server_events(), [])
        nla._phys_wire_evidence(ctx)
        self.assertIsNone(ctx.phys_wire["s2c"])
        self.assertIsNone(self._seg(ctx, "wire_s2c_phys"))
        self.assertNotIn("ServerPhysNicXmit", ctx.milestones)

    def test_non_dominant_marks_internal(self):
        """wire 占比低：dominant=False，证据标注耗时在节点内。"""
        client_events = [
            self._nic("20:45:39:061100", "netif_receive_skb", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 228, "enp38s0f0np0"),
        ]
        ms = {"ClientTcpRecvFirst": self.D(67500)}  # 节点内 6.4ms，线路 142us
        ctx = self._ctx(self._s2c_server_events(), client_events, milestones=ms)
        nla._phys_wire_evidence(ctx)
        pw = ctx.phys_wire["s2c"]
        self.assertAlmostEqual(pw["wire_us"], 142)
        self.assertFalse(pw["dominant"])
        self.assertTrue(any("节点内" in s for s in ctx.nic_evidence))

    def test_conclusion_refined_when_dominant(self):
        """传输类瓶颈 + 物理网卡间主导 → 改判 network_s2c_phys_wire_delay，置信度高。"""
        ctx = self._full_ctx()
        nla._phys_wire_evidence(ctx)
        ctx.kernel_segments.insert(0, {
            "key": "wire_s2c", "start": "ServerTcpSendIn", "end": "ClientTcpRecvFirst",
            "dur_us": 6662, "threshold_us": 200, "category": "network_s2c_transmission",
            "desc": "server 内核发送 → client 内核收包（线路传输+软中断）",
            "abnormal": True})
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "network_s2c_phys_wire_delay")
        self.assertIn("物理网卡间", ctx.conclusion["label"])
        self.assertEqual(ctx.conclusion["confidence"], "高")
        self.assertTrue(any("物理网卡" in e and "seq=2000" in e
                            for e in ctx.conclusion["evidence"]))
        self.assertTrue(ctx.conclusion["suggestions"])

    def test_conclusion_kept_when_not_dominant(self):
        """非主导：分类保持 network_s2c_transmission，证据带节点内分解。"""
        client_events = [
            self._nic("20:45:39:061100", "netif_receive_skb", self.SIP, self.SPORT,
                      self.CIP, self.CPORT, 2000, 228, "enp38s0f0np0"),
        ]
        ms = {"ClientTcpRecvFirst": self.D(67500)}
        ctx = self._ctx(self._s2c_server_events(), client_events, milestones=ms)
        nla._phys_wire_evidence(ctx)
        ctx.kernel_segments.insert(0, {
            "key": "wire_s2c", "start": "ServerTcpSendIn", "end": "ClientTcpRecvFirst",
            "dur_us": 6561, "threshold_us": 200, "category": "network_s2c_transmission",
            "desc": "d", "abnormal": True})
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "network_s2c_transmission")
        self.assertTrue(any("节点内" in e for e in ctx.conclusion["evidence"]))


class TestPhysWireEndToEnd(unittest.TestCase):
    """物理网卡间定界端到端：双跳 veth 链 → network_s2c_phys_wire_delay + 三输出。"""

    TRACE = "getBuffer-25487-00004775;117c5c4a91c7"
    CIP, SIP = "192.168.219.138", "192.168.102.161"

    def setUp(self):
        root = Path(tempfile.mkdtemp(prefix="pwire2e_"))
        self._root = root
        cdir = root / "collected" / "kvclient-1-master_26"
        wdir = root / "collected_worker_logs" / "kvchachjpworker-0-worker1"
        bdir = root / "dscollect_log"
        ldir = root / "latency_warn_log"
        for d in (cdir, wdir, bdir, ldir):
            d.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, self.TRACE, msg))

        (cdir / "ds_client_1.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060757", self.CIP,
                 "yyl9 ClientSend ts 88035205620370 tid 523")
            + info("2026-08-21T21:31:21.067624", self.CIP,
                   "yyl9 ClientRecv ts 88035221862010 tid 523")
            + info("2026-08-21T21:31:21.067636", self.CIP, SLOW_MSG),
            encoding="utf-8")
        (wdir / "kvcache.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060900", self.SIP,
                 "yyl3 ServerRecv ts 88038917594514 tid 275")
            + info("2026-08-21T21:31:21.060930", self.SIP,
                   "yyl10 ServerSend ts 88038917846674 tid 275"),
            encoding="utf-8")

        c2s = "%s:37880" % self.CIP
        s2c = "%s:31501" % self.SIP
        # client 节点：c2s 发送链（eth0→cali→物理网卡）+ s2c 接收链（物理网卡→cali→eth0）
        (bdir / "bpf-master-192.168.219.1.log").write_text(
            "21:31:21:060770 tcp  send in  tid 394303 cpu 252 size 266 "
            "%s -> %s:31501\n" % (c2s, self.SIP)
            + "21:31:21:060779 dev_start_xmit: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1000, len:266, dev:eth0\n" % (self.CIP, self.SIP)
            + "21:31:21:060781 net_dev_xmit: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1000, len:266, dev:eth0, rc:0\n" % (self.CIP, self.SIP)
            + "21:31:21:060783 netif_receive_skb: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1000, len:252, dev:calia22497db8ca\n" % (self.CIP, self.SIP)
            + "21:31:21:060792 dev_start_xmit: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1000, len:266, dev:enp38s0f0np0\n" % (self.CIP, self.SIP)
            + "21:31:21:060793 net_dev_xmit: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1000, len:266, dev:enp38s0f0np0, rc:0\n" % (self.CIP, self.SIP)
            + "21:31:21:067544 netif_receive_skb: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2000, len:228, dev:enp38s0f0np0\n" % (self.SIP, self.CIP)
            + "21:31:21:067566 dev_start_xmit: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2000, len:242, dev:calia22497db8ca\n" % (self.SIP, self.CIP)
            + "21:31:21:067567 net_dev_xmit: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2000, len:242, dev:calia22497db8ca, rc:0\n" % (self.SIP, self.CIP)
            + "21:31:21:067579 netif_receive_skb: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2000, len:228, dev:eth0\n" % (self.SIP, self.CIP)
            + "21:31:21:067582 tcp  recv que tid 151 cpu 27 size 228 "
              "tp_rcv_nxt:9187256525, %s <- %s:31501\n" % (c2s, self.SIP)
            + "21:31:21:067601 tcp  recv in  tid 390246 cpu 246 size 176 "
              "%s <- %s:31501, copied_seq:3891470867, rcv_nxt:3891471043\n" % (c2s, self.SIP),
            encoding="utf-8")
        # server 节点：c2s 接收链 + s2c 发送链（物理网卡发出前经过 cali veth）
        (bdir / "bpf-worker1-192.168.102.1.log").write_text(
            "21:31:21:060830 netif_receive_skb: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1000, len:252, dev:enp38s0f0np0\n" % (self.CIP, self.SIP)
            + "21:31:21:060852 dev_start_xmit: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1000, len:266, dev:calibad58b5daed\n" % (self.CIP, self.SIP)
            + "21:31:21:060853 net_dev_xmit: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1000, len:266, dev:calibad58b5daed, rc:0\n" % (self.CIP, self.SIP)
            + "21:31:21:060865 netif_receive_skb: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1000, len:252, dev:eth0\n" % (self.CIP, self.SIP)
            + "21:31:21:060870 tcp  recv que tid 594763 cpu 4 size 266 "
              "tp_rcv_nxt:4187256525, %s:31501 <- %s\n" % (self.SIP, c2s)
            + "21:31:21:060885 tcp  recv in  tid 396241 cpu 4 size 266 "
              "%s:31501 <- %s, copied_seq:4187256525, rcv_nxt:4187256795\n" % (self.SIP, c2s)
            + "21:31:21:060939 tcp  send in  tid 299296 cpu 50 size 242 "
              "%s:31501 -> %s\n" % (self.SIP, c2s)
            + "21:31:21:060944 dev_start_xmit: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2000, len:242, dev:eth0\n" % (self.SIP, self.CIP)
            + "21:31:21:060946 net_dev_xmit: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2000, len:242, dev:eth0, rc:0\n" % (self.SIP, self.CIP)
            + "21:31:21:060948 netif_receive_skb: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2000, len:228, dev:calibad58b5daed\n" % (self.SIP, self.CIP)
            + "21:31:21:060957 dev_start_xmit: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2000, len:242, dev:enp38s0f0np0\n" % (self.SIP, self.CIP)
            + "21:31:21:060958 net_dev_xmit: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2000, len:242, dev:enp38s0f0np0, rc:0\n" % (self.SIP, self.CIP),
            encoding="utf-8")
        (ldir / "master_192.168.219.1").write_text("", encoding="utf-8")
        (ldir / "worker1_192.168.102.1").write_text("", encoding="utf-8")

    def tearDown(self):
        import shutil
        shutil.rmtree(self._root, ignore_errors=True)

    def test_phys_wire_end_to_end(self):
        disc, contexts, _tl = nla.analyze(str(self._root))
        ctx = contexts[0]
        # 定界结论：物理网卡间传输慢（seq 关联），高置信
        self.assertEqual(ctx.conclusion["category"], "network_s2c_phys_wire_delay")
        self.assertEqual(ctx.conclusion["confidence"], "高")
        self.assertTrue(any("物理网卡" in e and "seq=2000" in e
                            for e in ctx.conclusion["evidence"]))
        # 里程碑与结构化结果
        ms = ctx.milestones
        self.assertEqual(ms["ServerPhysNicXmit"],
                         datetime(2026, 8, 21, 21, 31, 21, 60958))
        self.assertEqual(ms["ClientPhysNicRx"],
                         datetime(2026, 8, 21, 21, 31, 21, 67544))
        pw = ctx.phys_wire["s2c"]
        self.assertAlmostEqual(pw["wire_us"], 6586)
        self.assertEqual(pw["egress_dev"], "enp38s0f0np0")
        self.assertEqual(pw["ingress_dev"], "enp38s0f0np0")
        self.assertTrue(pw["dominant"])
        self.assertAlmostEqual(ctx.phys_wire["c2s"]["wire_us"], 37)

    def test_phys_wire_json_raw_html(self):
        import argparse
        disc, contexts, trace_lines = nla.analyze(str(self._root))
        ctx = contexts[0]
        ns = argparse.Namespace(residual_threshold=1000)
        doc = json.loads(nla.generate_json(contexts, ns, str(self._root)))
        t = doc["traces"][0]
        self.assertEqual(t["conclusion"]["category"], "network_s2c_phys_wire_delay")
        self.assertAlmostEqual(t["phys_wire"]["s2c"]["wire_us"], 6586)
        self.assertEqual(t["phys_wire"]["s2c"]["egress_dev"], "enp38s0f0np0")
        self.assertEqual(t["phys_wire"]["s2c"]["ingress_dev"], "enp38s0f0np0")
        self.assertTrue(t["phys_wire"]["s2c"]["dominant"])
        self.assertFalse(t["phys_wire"]["c2s"]["dominant"])
        self.assertEqual(doc["category_distribution"].get("network_s2c_phys_wire_delay"), 1)
        raw = nla.generate_raw(contexts, ns, str(self._root), disc, trace_lines)
        self.assertIn("网卡链路定界", raw)
        self.assertIn("enp38s0f0np0", raw)
        html_out = nla._trace_html(ctx, 1)
        self.assertIn("网卡链路定界", html_out)


# ── 辅助日志：irqoff 关中断 / sar 网卡利用率 / brpc bthread ────────────────────

IRQOFF_SAMPLE = (
    "hardirq: \n"
    "cpu: 4 \n"
    "      COMMAND: kubelet PID: 38557 LATENCY: 2ms TIMESTAMP: 2026-08-24 14:31:34.687803 \n"
    "      save_trace.isra.0+0x190/0x1d8 [trace_irqoff] \n"
    "      cgroup_rstat_flush+0x58/0xe8 \n"
    "softirq: \n"
    "cpu: 7 \n"
    "      COMMAND: ksoftirqd PID: 99 LATENCY: 3ms TIMESTAMP: 2026-08-24 14:31:40.100000 \n"
    "      net_rx_action+0x30/0x58 \n"
    "hardirq: \n"
    "cpu: 4 \n"
    "      COMMAND: kubelet PID: 38558 LATENCY: 1500us TIMESTAMP: 2026-08-24 14:31:50.200000 \n"
    "      do_IRQ+0x1/0x10 \n"
)

NIC_SAR_SAMPLE = (
    "Settings for enp38s0f0np0: \n"
    "        Supported ports: [ FIBRE ] \n"
    "        Speed: 100000Mb/s \n"
    "        Duplex: Full \n"
    "        Link detected: yes \n"
    "Linux 6.6.0-145.3.18.660.oe2403sp3.aarch64 (worker15)   08/24/2026"
    "      _aarch64_       (384 CPU) \n"
    " \n"
    "10:30:47 PM     IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s"
    "   rxcmp/s   txcmp/s  rxmcst/s   %ifutil \n"
    "10:30:48 PM enp38s0f0np0      7.00      2.00      0.42      0.12"
    "      0.00      0.00      5.00      0.00 \n"
    " \n"
    "10:30:48 PM     IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s"
    "   rxcmp/s   txcmp/s  rxmcst/s   %ifutil \n"
    "10:30:49 PM enp38s0f0np0     10.00      8.00      1.37      1.28"
    "      0.00      0.00      2.00     62.50 \n"
)

BTHREAD_SAMPLE = (
    "I0824 22:32:23.661136  6267 4294969346 external/com_github_apache_brpc/src/"
    "bthread/task_group.cpp:520 start_foreground] [WZY] bthread created: "
    "creator_tid=6267 bthread_id=3693671876360 creation_time_ns=210034956447684 "
    "creation_mode=foreground target_local_pending_tasks=0 "
    "target_remote_pending_tasks=0 target_pending_tasks=0\n"
    "I0824 22:32:23.661172  6267 3693671876360 external/com_github_apache_brpc/src/"
    "bthread/task_group.cpp:383 task_runner] [WZY] bthread first scheduled: "
    "worker_tid=6267 bthread_id=3693671876360 fn=0xfffd327cc070 arg=0x120b97c0 "
    "creation_time_ns=210034956447684 first_run_time_ns=210034956484684 "
    "pending_time_us=37\n"
    "I0824 22:40:00.000000  6267 99 file.cpp:1 f] [WZY] bthread created: "
    "creator_tid=6267 bthread_id=99 creation_time_ns=1 creation_mode=foreground "
    "target_local_pending_tasks=5 target_remote_pending_tasks=0 "
    "target_pending_tasks=5\n"
)


class TestIrqoffParsers(unittest.TestCase):
    """irqoff_latency_<ip>.log：块状态机解析 + 全周期统计 + 窗口过滤。"""

    def _write(self, text):
        fd, path = tempfile.mkstemp(suffix=".log")
        os.close(fd)
        Path(path).write_text(text, encoding="utf-8")
        self.addCleanup(os.unlink, path)
        return path

    def test_irqoff_blocks_and_stats(self):
        path = self._write(IRQOFF_SAMPLE)
        wins = {"in": (datetime(2026, 8, 24, 14, 31, 30),
                       datetime(2026, 8, 24, 14, 31, 45)),
                "out": (datetime(2026, 8, 24, 15, 0), datetime(2026, 8, 24, 15, 1))}
        stats, blocks = nla.scan_irqoff(path, wins)
        # 全周期统计：3 条（hardirq 2 + softirq 1），单位统一 us
        self.assertEqual(stats["total"], 3)
        self.assertEqual(stats["hardirq_n"], 2)
        self.assertEqual(stats["softirq_n"], 1)
        self.assertEqual(stats["max_us"], 3000)
        self.assertEqual(stats["by_comm"]["kubelet"]["n"], 2)
        self.assertEqual(stats["by_comm"]["kubelet"]["max_us"], 2000)
        self.assertEqual(stats["by_cpu"][4]["n"], 2)
        self.assertEqual(stats["by_cpu"][7]["n"], 1)
        self.assertEqual(stats["buckets"][1000], 3)
        self.assertEqual(stats["buckets"][2000], 2)   # 2000/3000 ≥2ms，1500 <2ms
        self.assertEqual(stats["buckets"][5000], 0)
        self.assertEqual(len(stats["series"]), 3)
        # 窗口内 2 条 / 窗口外 0 条
        self.assertEqual(len(blocks["in"]), 2)
        self.assertEqual(blocks["out"], [])
        b = blocks["in"][0]
        self.assertEqual(b["irq"], "hardirq")
        self.assertEqual(b["cpu"], 4)
        self.assertEqual(b["comm"], "kubelet")
        self.assertEqual(b["pid"], 38557)
        self.assertEqual(b["latency_us"], 2000)
        self.assertEqual(b["ts"], datetime(2026, 8, 24, 14, 31, 34, 687803))
        self.assertTrue(any("save_trace" in r for r in b["raw"]))
        self.assertEqual(blocks["in"][1]["irq"], "softirq")
        self.assertEqual(blocks["in"][1]["cpu"], 7)
        self.assertEqual(blocks["in"][1]["latency_us"], 3000)

    def test_irqoff_empty_windows(self):
        path = self._write(IRQOFF_SAMPLE)
        stats, blocks = nla.scan_irqoff(path, {})
        self.assertEqual(stats["total"], 3)
        self.assertEqual(blocks, {})


class TestNicSarParsers(unittest.TestCase):
    """nic-<ip>.log：ethtool 属性 + sar 采样（AM/PM → 24h）+ 窗口日期组合。"""

    def _write(self, text):
        fd, path = tempfile.mkstemp(suffix=".log")
        os.close(fd)
        Path(path).write_text(text, encoding="utf-8")
        self.addCleanup(os.unlink, path)
        return path

    def test_parse_nic_log(self):
        path = self._write(NIC_SAR_SAMPLE)
        devs = nla.parse_nic_log(path)
        self.assertIn("enp38s0f0np0", devs)
        d = devs["enp38s0f0np0"]
        self.assertEqual(d["ethtool"]["Speed"], "100000Mb/s")
        self.assertEqual(d["ethtool"]["Duplex"], "Full")
        self.assertEqual(d["ethtool"]["Link detected"], "yes")
        self.assertEqual(len(d["samples"]), 2)
        s0, s1 = d["samples"]
        self.assertEqual(s0["hms"], "22:30:48")   # 10:30:48 PM → 22:30:48
        self.assertEqual(s0["rxpck"], 7.0)
        self.assertEqual(s1["hms"], "22:30:49")
        self.assertAlmostEqual(s1["ifutil"], 62.5)

    def test_sar_dt_and_window(self):
        dt = nla._sar_dt("22:30:48", DAY)
        self.assertEqual(dt, datetime(2026, 8, 21, 22, 30, 48))
        # 窗口 [21:31:21.068, 21:31:21.077]：同秒样本 21:31:21.000 命中
        samples = [{"hms": "21:31:21", "rxpck": 12.0, "txpck": 8.0, "rxkB": 1.0,
                    "txkB": 1.0, "ifutil": 0.1},
                   {"hms": "21:31:22", "rxpck": 12.0, "txpck": 8.0, "rxkB": 1.0,
                    "txkB": 1.0, "ifutil": 0.2}]
        hit = nla._sar_in_window(samples, datetime(2026, 8, 21, 21, 31, 21, 68000),
                                 datetime(2026, 8, 21, 21, 31, 21, 77000))
        self.assertEqual(len(hit), 1)
        self.assertEqual(hit[0]["hms"], "21:31:21")


class TestBthreadParsers(unittest.TestCase):
    """brpc bthread 日志（glog）：字段解析 + 窗口外行不解析。"""

    def _write(self, text):
        fd, path = tempfile.mkstemp(suffix=".log")
        os.close(fd)
        Path(path).write_text(text, encoding="utf-8")
        self.addCleanup(os.unlink, path)
        return path

    def test_scan_bthread_windows(self):
        path = self._write(BTHREAD_SAMPLE)
        wins = {"in": (datetime(2026, 8, 24, 22, 32, 20),
                       datetime(2026, 8, 24, 22, 32, 25))}
        evs = nla.scan_bthread_windows(path, wins)
        self.assertEqual(len(evs["in"]), 2)   # 第 3 行窗口外，不解析
        created, sched = evs["in"]
        self.assertEqual(created["kind"], "created")
        self.assertEqual(created["tid"], 6267)
        self.assertEqual(created["bthread_id"], 3693671876360)
        self.assertEqual(created["target_pending_tasks"], 0)
        self.assertEqual(created["creation_mode"], "foreground")
        self.assertEqual(created["ts"], datetime(2026, 8, 24, 22, 32, 23, 661136))
        self.assertEqual(sched["kind"], "scheduled")
        self.assertEqual(sched["tid"], 6267)
        self.assertEqual(sched["bthread_id"], 3693671876360)
        self.assertEqual(sched["pending_time_us"], 37)
        self.assertEqual(sched["ts"], datetime(2026, 8, 24, 22, 32, 23, 661172))
        self.assertIn("bthread first scheduled", sched["raw"])


class TestAuxEndToEnd(unittest.TestCase):
    """三类辅助日志端到端：client 收包后取包慢场景。

    bpf：ClientTcpSendIn .060770 → server 快速收发（ServerRecv .060950 /
    ServerSend .061200 / tcp send in .061250）→ ClientNetifRx .064900 →
    ClientTcpRecvFirst .065000 → ClientRecv .077001（内核→用户态 12001us，
    瓶颈段；s2c 线路仅 3750us）。
    irqoff：client 节点 cpu 332 在 .070100 关中断 3ms（收包段窗口内）。
    sar：窗口内同秒样本 ifutil 0.10%（排除带宽打满）。
    bthread：client pod 线程 523 在收包窗口内 pending_time_us=4900（协程排队）。
    预期：client_kernel_to_user_delay + 关中断/网卡利用率/协程排队证据，高置信。
    """

    TRACE = "getBuffer-25487-00004775;117c5c4a91c7"
    CIP, SIP = "192.168.219.138", "192.168.102.161"

    def setUp(self):
        root = Path(tempfile.mkdtemp(prefix="auxe2e_"))
        self._root = root
        cdir = root / "collected" / "kvclient-1-master_26"
        wdir = root / "collected_worker_logs" / "kvworker-0-worker1"
        bdir = root / "dscollect_log"
        ldir = root / "latency_warn_log"
        for d in (cdir, wdir, bdir, ldir):
            d.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, self.TRACE, msg))

        (cdir / "ds_client_1.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060757", self.CIP,
                 "yyl9 ClientSend ts 88035205620370 tid 523")
            + info("2026-08-21T21:31:21.077001", self.CIP,
                   "yyl9 ClientRecv ts 88035221862010 tid 523")
            + info("2026-08-21T21:31:21.077013", self.CIP, SLOW_MSG),
            encoding="utf-8")
        (wdir / "kvcache.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060950", self.SIP,
                 "yyl3 ServerRecv ts 88038917594514 tid 275")
            + info("2026-08-21T21:31:21.061200", self.SIP,
                   "yyl10 ServerSend ts 88038917846674 tid 275"),
            encoding="utf-8")

        (bdir / "bpf-master-192.168.219.1.log").write_text(
            "21:31:21:060770 tcp  send in  tid 479093 cpu 50 size 270 "
            "%s:37880 -> %s:31501\n" % (self.CIP, self.SIP)
            + "21:31:21:064900 netif_receive_skb: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2222, len:120, dev:enp38s0f0np0\n" % (self.SIP, self.CIP)
            + "21:31:21:065000 tcp  recv in  tid 479193 cpu 332 size 120 "
              "%s:37880 <- %s:31501, copied_seq:358067377, rcv_nxt:358067377\n"
              % (self.CIP, self.SIP),
            encoding="utf-8")
        (bdir / "bpf-worker1-192.168.102.1.log").write_text(
            "21:31:21:060810 netif_receive_skb: sip:%s, sport:37880 -> dip:%s, "
              "dport:31501, seq:1111, len:266, dev:enp38s0f0np0\n" % (self.CIP, self.SIP)
            + "21:31:21:060820 tcp  recv que tid 594763 cpu 4 size 266 "
              "tp_rcv_nxt:4187256525, %s:31501 <- %s:37880\n" % (self.SIP, self.CIP)
            + "21:31:21:060900 tcp  recv in  tid 396241 cpu 4 size 266 "
              "%s:31501 <- %s:37880, copied_seq:4187256525, rcv_nxt:4187256795\n"
              % (self.SIP, self.CIP)
            + "21:31:21:061250 tcp  send in  tid 594763 cpu 4 size 155 "
              "%s:31501 -> %s:37880\n" % (self.SIP, self.CIP),
            encoding="utf-8")
        # 关中断日志（client 节点，收包段窗口内）
        (bdir / "irqoff_latency_192.168.219.1.log").write_text(
            "hardirq: \n"
            "cpu: 332 \n"
            "      COMMAND: kubelet PID: 38557 LATENCY: 3ms "
            "TIMESTAMP: 2026-08-21 21:31:21.070100 \n"
            "      save_trace.isra.0+0x190/0x1d8 [trace_irqoff] \n"
            "      cgroup_rstat_flush+0x58/0xe8 \n"
            "softirq: \n"
            "cpu: 4 \n"
            "      COMMAND: ksoftirqd PID: 99 LATENCY: 2ms "
            "TIMESTAMP: 2026-08-21 21:35:00.000000 \n"
            "      net_rx_action+0x30/0x58 \n",
            encoding="utf-8")
        # sar 网卡利用率（client 节点，窗口同秒样本 ifutil 0.10）
        (bdir / "nic-192.168.219.1.log").write_text(
            "Settings for enp38s0f0np0: \n"
            "        Speed: 100000Mb/s \n"
            "        Link detected: yes \n"
            " \n"
            "09:31:20 PM     IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s"
            "   rxcmp/s   txcmp/s  rxmcst/s   %ifutil \n"
            "09:31:21 PM enp38s0f0np0     12.00      8.00      1.37      1.28"
            "      0.00      0.00      2.00      0.10 \n"
            "09:31:22 PM enp38s0f0np0     12.00      8.00      1.37      1.28"
            "      0.00      0.00      2.00      0.20 \n",
            encoding="utf-8")
        # brpc bthread 日志（client pod，线程 523 = ClientRecv tid）
        (bdir / "kvclient-1-master-brpc_client.log").write_text(
            "I0821 21:31:21.070100  523 111 f.cpp:1 start_foreground] [WZY] "
            "bthread created: creator_tid=523 bthread_id=100 "
            "creation_time_ns=1 creation_mode=foreground "
            "target_local_pending_tasks=3 target_remote_pending_tasks=0 "
            "target_pending_tasks=3\n"
            "I0821 21:31:21.075000  523 111 f.cpp:1 task_runner] [WZY] "
            "bthread first scheduled: worker_tid=523 bthread_id=100 fn=0x1 "
            "arg=0x2 creation_time_ns=1 first_run_time_ns=2 pending_time_us=4900\n"
            "I0821 21:31:30.000000  523 112 f.cpp:1 start_foreground] [WZY] "
            "bthread created: creator_tid=523 bthread_id=101 "
            "creation_time_ns=1 creation_mode=foreground "
            "target_local_pending_tasks=0 target_remote_pending_tasks=0 "
            "target_pending_tasks=0\n",
            encoding="utf-8")
        (ldir / "master_192.168.219.1").write_text("", encoding="utf-8")
        (ldir / "worker1_192.168.102.1").write_text("", encoding="utf-8")

    def tearDown(self):
        import shutil
        shutil.rmtree(self._root, ignore_errors=True)

    def test_aux_evidence_end_to_end(self):
        disc, contexts, _tl = nla.analyze(str(self._root))
        ctx = contexts[0]
        self.assertEqual(ctx.conclusion["category"], "client_kernel_to_user_delay")
        # irqoff：窗口内 1 条（kubelet cpu 332 hardirq 3ms），softirq 窗口外不计
        self.assertEqual(len(ctx.irqoff_events["client"]), 1)
        ev = ctx.irqoff_events["client"][0]
        self.assertEqual(ev["comm"], "kubelet")
        self.assertEqual(ev["latency_us"], 3000)
        self.assertEqual(ev["cpu"], 332)
        joined_nic = " | ".join(ctx.nic_evidence)
        self.assertIn("关中断", joined_nic)
        self.assertIn("kubelet", joined_nic)
        # sar：窗口内 1 条样本，低利用率 → 排除性证据
        self.assertEqual(len(ctx.nic_samples["client"]), 1)
        self.assertAlmostEqual(ctx.nic_samples["client"][0]["ifutil"], 0.1)
        self.assertIn("排除网卡带宽打满", joined_nic)
        # bthread：窗口内 2 条（第 3 行窗口外），线程 523 统计证据
        self.assertEqual(len(ctx.bthread_events["client"]), 2)
        joined_coro = " | ".join(ctx.coro_evidence)
        self.assertIn("4900", joined_coro)   # pending_time_us
        self.assertIn("523", joined_coro)    # worker tid
        # 全局统计
        self.assertIn("master", disc.aux_stats["irqoff"])
        self.assertEqual(disc.aux_stats["irqoff"]["master"]["total"], 2)
        self.assertIn("enp38s0f0np0", disc.aux_stats["nic"]["master"])
        self.assertAlmostEqual(
            disc.aux_stats["nic"]["master"]["enp38s0f0np0"]["max_ifutil"], 0.2)
        # 置信度：kernel_to_user + irqoff/bthread 证据 → 高
        self.assertEqual(ctx.conclusion["confidence"], "高")

    def test_aux_render(self):
        import argparse
        disc, contexts, trace_lines = nla.analyze(str(self._root))
        ctx = contexts[0]
        ns = argparse.Namespace(residual_threshold=1000)
        # HTML 概览两卡
        rep = nla.generate_report(contexts, ns, str(self._root),
                                  aux_stats=disc.aux_stats)
        self.assertIn("关中断统计", rep)
        self.assertIn("网卡利用率统计", rep)
        self.assertIn("kubelet", rep)
        # trace 卡三块
        h = nla._trace_html(ctx, 1)
        self.assertIn("关中断记录", h)
        self.assertIn("sar 网卡采样", h)
        self.assertIn("bthread 协程事件", h)
        # JSON：全局 + trace 级字段
        doc = json.loads(nla.generate_json(contexts, ns, str(self._root),
                                           aux_stats=disc.aux_stats))
        self.assertIn("master", doc["irqoff_stats"])
        self.assertIn("enp38s0f0np0", doc["nic_stats"]["master"])
        t = doc["traces"][0]
        self.assertEqual(len(t["irqoff_events"]["client"]), 1)
        self.assertEqual(len(t["nic_samples"]["client"]), 1)
        self.assertEqual(len(t["bthread_events"]["client"]), 2)
        # raw：三段
        raw = nla.generate_raw(contexts, ns, str(self._root), disc, trace_lines)
        self.assertIn("关中断记录", raw)
        self.assertIn("sar 网卡采样", raw)
        self.assertIn("bthread 协程事件", raw)


class TestBpfScannerWindowBucket(unittest.TestCase):
    """BpfScanner 窗口全景桶：连接类事件（tcp/nic/sock）不限 IP 全量保留。

    res（kernel_results）仍按 pod IP 对过滤；window_results 为窗口内全部
    连接类事件（其他 pod 流量也保留），供问题窗口全景 + cpu 侵占分析使用。
    """

    CIP, SIP = "10.0.0.1", "10.0.0.2"

    def _win(self, s, e, trace="A", side="client"):
        return nla.TraceWindow(trace, side, s, e, self.CIP, self.SIP)

    def _write_bpf(self, lines):
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as fh:
            fh.write("\n".join(lines) + "\n")
            return fh.name

    def test_window_bucket_keeps_other_connections(self):
        lines = [
            "21:31:21:060000 tcp  send in  tid 1 cpu 1 size 10 10.0.0.1:1 -> 10.0.0.2:2",
            # 其他 pod 的连接（IP 对不匹配）→ res 丢弃，窗口桶保留
            "21:31:21:060100 tcp  recv in  tid 2 cpu 2 size 10 10.0.0.9:1 <- 10.0.0.8:2",
            # 其他 pod 的网卡事件 → 同上
            "21:31:21:060200 netif_receive_skb: sip:10.0.0.7, sport:1 -> "
            "dip:10.0.0.6, dport:2, seq:1, len:10, dev:eth0",
            # 同 pod 对但端口不同（其他请求）
            "21:31:21:060250 tcp  recv que tid 7 cpu 3 size 10 tp_rcv_nxt:9, "
            "10.0.0.2:2 <- 10.0.0.1:9",
            # 调度类事件：仅进 res（配额），不进窗口桶
            "21:31:21:060300 sched_waking tid 3 cpu 4 comm x pid 5 target_cpu 4",
            # 窗口外
            "21:31:22:000000 tcp  send in  tid 1 cpu 1 size 10 10.0.0.1:1 -> 10.0.0.2:2",
        ]
        path = self._write_bpf(lines)
        try:
            w = self._win(datetime(2026, 8, 21, 21, 31, 21, 50000),
                          datetime(2026, 8, 21, 21, 31, 21, 100000))
            scanner = nla.BpfScanner(path, [w], full_scan=True)
            res, trunc = scanner.scan()
            # res：pod IP 对匹配的 tcp 事件 + sched 事件
            kinds = sorted(e["kind"] for e in res[("A", "client")])
            self.assertEqual(kinds, ["sched_waking", "tcp_recv_que", "tcp_send_in"])
            self.assertEqual(trunc, set())
            # 窗口桶：全部连接类事件（含其他 pod），不含 sched / 窗口外
            wevs = scanner.window_results[("A", "client")]
            wkinds = [e["kind"] for e in wevs]
            self.assertEqual(wkinds, ["tcp_send_in", "tcp_recv_in",
                                      "nic_rx_skb", "tcp_recv_que"])
            self.assertEqual(scanner.window_truncated, set())
        finally:
            os.unlink(path)

    def test_window_bucket_quota_truncates(self):
        lines = ["21:31:21:060%03d tcp  recv in  tid 2 cpu 2 size 10 "
                 "10.0.0.9:1 <- 10.0.0.8:2" % i for i in range(5)]
        path = self._write_bpf(lines)
        try:
            w = self._win(datetime(2026, 8, 21, 21, 31, 21, 50000),
                          datetime(2026, 8, 21, 21, 31, 21, 100000))
            scanner = nla.BpfScanner(path, [w], full_scan=True,
                                     max_window_net_events=2)
            scanner.scan()
            self.assertEqual(len(scanner.window_results[("A", "client")]), 2)
            self.assertEqual(scanner.window_truncated, {("A", "client")})
        finally:
            os.unlink(path)


class TestCpuBusyEndToEnd(unittest.TestCase):
    """问题窗口全景 + cpu 侵占分析端到端：client 收包后业务处理开始晚。

    时间线（client 节点业务线程 tid 523 运行在 cpu 50）：
      ClientSend .060757(cpu50) → tcp send in .060770(cpu50)
      → server 快速收发（recv que .060820 / recv in .060900 / ServerRecv .060950
      / ServerSend .064740 / tcp send in .064810）
      → client netif_rx .064900 → tcp recv in .065000(cpu332)
      →【client 内核→用户态 3001us，瓶颈段，margin 3.0 < 5】
      → ClientRecv .068001(cpu50)。
    问题窗口 [.064900, .068001] 内 cpu 50 上穿插其他请求：
      - 其他 pod 连接收包 192.168.219.200:40000（旧逻辑按 IP 过滤会丢弃）
      - 同 pod 其他端口连接 192.168.219.138:39999
    预期：client_kernel_to_user_delay + 软中断抢占证据（◎）+ 高置信。
    """

    TRACE = "getBuffer-25487-00004775;117c5c4a91c7"
    CIP, SIP = "192.168.219.138", "192.168.102.161"

    def _slow(self):
        # 抬高 server_req_queue/exec 指标，避免 server_processing 段被判异常
        return SLOW_MSG.replace("server_req_queue_us=10 server_exec_us=240",
                                "server_req_queue_us=2000 server_exec_us=2000")

    def _build(self, other_cpu=50, with_switch=False):
        root = Path(tempfile.mkdtemp(prefix="cpubusy_"))
        cdir = root / "collected" / "kvclient-1-master_26"
        wdir = root / "collected_worker_logs" / "kvworker-0-worker1"
        bdir = root / "dscollect_log"
        ldir = root / "latency_warn_log"
        for d in (cdir, wdir, bdir, ldir):
            d.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, self.TRACE, msg))

        (cdir / "ds_client_1.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060757", self.CIP,
                 "yyl9 ClientSend ts 88035205620370 tid 523 cpu 50")
            + info("2026-08-21T21:31:21.068001", self.CIP,
                   "yyl9 ClientRecv ts 88035221862010 tid 523 cpu 50")
            + info("2026-08-21T21:31:21.068013", self.CIP, self._slow()),
            encoding="utf-8")
        (wdir / "kvcache.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060950", self.SIP,
                 "yyl3 ServerRecv ts 88038917594514 tid 275")
            + info("2026-08-21T21:31:21.064740", self.SIP,
                   "yyl10 ServerSend ts 88038917846674 tid 275"),
            encoding="utf-8")

        client_bpf = [
            "21:31:21:060770 tcp  send in  tid 523 cpu 50 size 270 "
            "%s:37880 -> %s:31501\n" % (self.CIP, self.SIP),
            "21:31:21:064900 netif_receive_skb: sip:%s, sport:31501 -> dip:%s, "
            "dport:37880, seq:2222, len:120, dev:enp38s0f0np0\n" % (self.SIP, self.CIP),
            "21:31:21:065000 tcp  recv in  tid 479193 cpu 332 size 120 "
            "%s:37880 <- %s:31501, copied_seq:358067377, rcv_nxt:358067377\n"
            % (self.CIP, self.SIP),
            # 其他 pod 连接的收包（IP 对不匹配 → 旧逻辑丢弃，窗口全景保留）
            "21:31:21:065500 tcp  recv in  tid 888001 cpu %d size 200 "
            "192.168.219.200:40000 <- %s:31501, copied_seq:1, rcv_nxt:1\n"
            % (other_cpu, self.SIP),
            # 同 pod 其他端口的连接（其他请求）
            "21:31:21:066000 tcp  recv que tid 888002 cpu %d size 90 "
            "tp_rcv_nxt:99, %s:31501 <- %s:39999\n" % (other_cpu, self.SIP, self.CIP),
        ]
        if with_switch:
            # 业务线程（tid 523）在 cpu 50 上被切换出（被 ksoftirqd 抢占）
            client_bpf.append(
                "21:31:21:066500 sched_switch tid 0 cpu 50 prev_comm=kvclient "
                "prev_pid=523 next_comm=ksoftirqd/50 next_pid=999\n")
        (bdir / "bpf-master-192.168.219.1.log").write_text(
            "".join(client_bpf), encoding="utf-8")
        (bdir / "bpf-worker1-192.168.102.1.log").write_text(
            "21:31:21:060810 netif_receive_skb: sip:%s, sport:37880 -> dip:%s, "
            "dport:31501, seq:1111, len:266, dev:enp38s0f0np0\n" % (self.CIP, self.SIP)
            + "21:31:21:060820 tcp  recv que tid 594763 cpu 4 size 266 "
              "tp_rcv_nxt:4187256525, %s:31501 <- %s:37880\n" % (self.SIP, self.CIP)
            + "21:31:21:060900 tcp  recv in  tid 396241 cpu 4 size 266 "
              "%s:31501 <- %s:37880, copied_seq:4187256525, rcv_nxt:4187256795\n"
            % (self.SIP, self.CIP)
            + "21:31:21:064810 tcp  send in  tid 594763 cpu 4 size 155 "
              "%s:31501 -> %s:37880\n" % (self.SIP, self.CIP),
            encoding="utf-8")
        (ldir / "master_192.168.219.1").write_text("", encoding="utf-8")
        (ldir / "worker1_192.168.102.1").write_text("", encoding="utf-8")
        return root

    def test_cpu_busy_preempt_evidence(self):
        root = self._build(other_cpu=50)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        self.assertEqual(ctx.conclusion["category"], "client_kernel_to_user_delay")
        # 仅 client 侧分析（server kernel_to_user 段未超阈值）
        self.assertIn("client", ctx.cpu_busy)
        self.assertNotIn("server", ctx.cpu_busy)
        info = ctx.cpu_busy["client"]
        self.assertEqual(info["anchor_cpu"], 50)
        self.assertEqual(info["anchor_tid"], "523")
        # 问题窗口 [.064900, .068001]：问题连接 2 条（netif_rx + tcp recv in）
        # / 其他连接 2 条，均在业务 cpu 50 上
        self.assertEqual(info["n_mine"], 2)
        self.assertEqual(info["n_other"], 2)
        self.assertEqual(len(info["other_on_cpu"]), 2)
        self.assertEqual(len(info["other_conns"]), 2)
        self.assertIn("192.168.219.200:40000", "".join(info["other_conns"]))
        self.assertTrue(info["preempt"])
        self.assertTrue(ctx.cpu_busy_preempt)
        # 证据：软中断抢占（margin 3.0 < 5，无其他佐证 → 高置信来自抢占证据）
        joined = " | ".join(ctx.cpu_evidence)
        self.assertIn("cpu 50", joined)
        self.assertIn("软中断", joined)
        self.assertIn("抢占", joined)
        joined_ev = " | ".join(ctx.conclusion["evidence"])
        self.assertIn("◎", joined_ev)
        self.assertEqual(ctx.conclusion["confidence"], "高")
        self.assertTrue(any("RSS" in s for s in ctx.conclusion["suggestions"]))
        # 其他请求事件未被 pod IP 过滤丢弃（窗口全景含其他 pod 连接）
        self.assertTrue(any(e.get("match5t") is False
                            for e in ctx.bpf_window_events["client"]))
        self.assertEqual(len([e for e in ctx.bpf_window_events["client"]
                               if e.get("match5t") is False]), 2)

    def test_cpu_busy_switched_out(self):
        root = self._build(other_cpu=50, with_switch=True)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        info = ctx.cpu_busy["client"]
        # 业务线程在 cpu 50 上被 sched_switch 切出 1 次（切至 ksoftirqd/50）
        self.assertEqual(len(info["switched_out"]), 1)
        self.assertEqual(info["switched_out"][0]["next_comm"], "ksoftirqd/50")
        joined = " | ".join(ctx.cpu_evidence)
        self.assertIn("被切换出 1 次", joined)
        self.assertIn("直接抢占", joined)
        # sched 事件（无 IP）在全景中以 match5t=None 呈现
        self.assertTrue(any(e["kind"] == "sched_switch" and e.get("match5t") is None
                            for e in ctx.bpf_window_events["client"]))

    def test_cpu_busy_negative_no_other_on_cpu(self):
        # 其他请求事件在别的 cpu（77）上 → 业务 cpu 50 无抢占证据，中置信
        root = self._build(other_cpu=77)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        info = ctx.cpu_busy["client"]
        self.assertEqual(info["other_on_cpu"], [])
        self.assertFalse(info["preempt"])
        self.assertFalse(ctx.cpu_busy_preempt)
        joined = " | ".join(ctx.cpu_evidence)
        self.assertIn("未发现其他连接的收包/协议栈事件", joined)
        self.assertIn("#77", joined)   # 其他连接事件分布标注
        # margin 3.0 < 5 且无抢占佐证 → 中置信
        self.assertEqual(ctx.conclusion["confidence"], "中")

    def test_cpu_busy_render_json_raw(self):
        import argparse
        root = self._build(other_cpu=50)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        disc, contexts, trace_lines = nla.analyze(str(root))
        ctx = contexts[0]
        ns = argparse.Namespace(residual_threshold=1000)
        # HTML：全景表 + 高亮 + cpu 标注 + 归属列
        h = nla._trace_html(ctx, 1)
        self.assertIn("问题窗口 bpf 事件全景", h)
        self.assertIn('class="hl5t"', h)
        self.assertIn("其他连接", h)
        self.assertIn('<span class="cpuflag">50</span>', h)
        self.assertIn("192.168.219.200:40000", h)
        # JSON：cpu_busy 结构 + window_events match5t 标注
        doc = json.loads(nla.generate_json(contexts, ns, str(root),
                                           aux_stats=disc.aux_stats))
        cb = doc["traces"][0]["cpu_busy"]["client"]
        self.assertEqual(cb["anchor_cpu"], 50)
        self.assertTrue(cb["preempt"])
        self.assertEqual(cb["n_mine"], 2)
        self.assertEqual(cb["n_other"], 2)
        self.assertEqual(len(cb["window_events"]), 4)
        self.assertEqual([e["match5t"] for e in cb["window_events"]],
                         [True, True, False, False])
        self.assertEqual(len(cb["other_on_cpu"]), 2)
        self.assertIn("192.168.219.200:40000", "".join(cb["other_conns"]))
        # raw：全景节 + 问题五元组 ▶ 标注
        raw = nla.generate_raw(contexts, ns, str(root), disc, trace_lines)
        self.assertIn("问题窗口 bpf 事件全景", raw)
        self.assertIn("▶ 21:31:21:064900 netif_receive_skb", raw)
        self.assertIn("  21:31:21:065500 tcp  recv in", raw)


class TestReportStyleAndPerf(unittest.TestCase):
    """报告风格重构（参考 ds-log-deep-analysis）+ 大表渲染性能优化。

    - 卡顿优化：大表包 .table-wrap 滚动容器 + table-layout:fixed（ev-tbl）
      + colgroup 固定列宽；CSS 中 trace 卡片与事件行启用
      content-visibility:auto，视口外内容跳过渲染，展开/收起不触发整页重排；
    - bpf 事件明细按问题时间窗全景展示（_window_events_table），
      问题五元组行黄底高亮（hl5t）+ 归属列，其他连接事件直接混排。
    """

    def _ctx(self):
        slow = nla.SlowRecord("t1;abc", DAY,
                              {"network_residual_us": "2000", "e2e_us": "3000",
                               "framework_us": "2500", "method": "m"},
                              "x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.conclusion = {"label": "测试结论", "confidence": "高",
                          "evidence": ["e1"], "suggestions": ["s1"]}
        return ctx

    def _ev(self, **kw):
        ev = {"ts": DAY, "kind": "tcp_send_in", "tid": 1, "cpu": 1, "raw": "x",
              "local_ip": "1.1.1.1", "local_port": 1, "peer_ip": "2.2.2.2",
              "peer_port": 2, "dir_arrow": "->"}
        ev.update(kw)
        return ev

    def test_events_table_wrapped_for_perf(self):
        out = nla._events_table([self._ev()], "t")
        self.assertIn('class="table-wrap"', out)   # 滚动容器（限高）
        self.assertIn('class="ev-tbl"', out)       # table-layout:fixed
        self.assertIn("<colgroup>", out)           # 固定列宽

    def test_window_events_table_highlight_and_owner(self):
        evs = [self._ev(match5t=True),
               self._ev(kind="tcp_recv_in", tid=2, cpu=9, local_ip="9.9.9.9",
                        match5t=False)]
        out = nla._window_events_table(evs, "全景标题", note="共 2 条")
        self.assertIn("全景标题", out)
        self.assertIn("共 2 条", out)
        self.assertIn('class="hl5t"', out)         # 问题连接高亮行
        self.assertIn("问题连接", out)
        self.assertIn("其他连接", out)               # 归属列
        self.assertIn('class="table-wrap"', out)
        self.assertEqual(out.count("<tr"), 1 + 2)   # 表头 + 2 数据行
        # 空事件 → 提示
        self.assertIn("无匹配事件", nla._window_events_table([], "t"))

    def test_window_events_table_row_cap(self):
        evs = [self._ev(match5t=True, tid=i)
               for i in range(nla.EVENTS_TABLE_MAX_ROWS + 10)]
        out = nla._window_events_table(evs, "t")
        self.assertIn("仅列前 %d 条" % nla.EVENTS_TABLE_MAX_ROWS, out)

    def test_side_events_fallback_to_filtered(self):
        # bpf_window_events 为空（无 bpf 日志/关联失败）→ 五元组过滤版兜底
        ctx = self._ctx()
        ctx.filtered_events["client"] = [self._ev()]
        out = nla._side_events_html(ctx, "client")
        self.assertIn("tcp_send_in", out)
        self.assertIn("client 节点 bpf 事件", out)
        self.assertNotIn("归属", out)              # 兜底表无归属列
        # 全景优先：有全景数据时输出全景表
        ctx.bpf_window_events["client"] = [self._ev(match5t=True)]
        out2 = nla._side_events_html(ctx, "client")
        self.assertIn("问题时间窗全景", out2)
        self.assertIn("归属", out2)

    def test_report_shell_style_and_perf_css(self):
        import argparse
        ns = argparse.Namespace(residual_threshold=1000)
        out = nla.generate_report([self._ctx()], ns, "/tmp")
        # 外壳：头部横幅 + 汇总统计卡 + 展开/收起工具条（参考 skill 风格）
        self.assertIn('class="header"', out)
        self.assertIn("summary-cards", out)
        self.assertIn("问题请求总数", out)
        self.assertIn("高置信结论", out)
        self.assertIn("toggleAllDetails", out)
        self.assertIn('class="toc"', out)
        # 性能：视口外跳过渲染 + 大表固定布局 + 滚动容器限高
        self.assertIn("content-visibility:auto", out)
        self.assertIn("table-layout:fixed", out)
        self.assertIn("max-height:520px", out)
        # trace 卡片头：索引 + trace id + 徽章 + 指标
        self.assertIn("card trace-card", out)
        self.assertIn('class="trace-head"', out)
        self.assertIn('class="trace-id"', out)
        self.assertIn("residual=2000", out)
        self.assertIn("置信度：高", out)


class TestSlowSegWindowView(unittest.TestCase):
    """慢段窗口 bpf 事件 + 问题请求相关子项（bpf 事件明细多子项结构）。

    client/server 节点 bpf 事件明细下包含三个子项：
    - 子项1「问题请求相关事件」（默认展开）：仅问题连接五元组事件 + 关键线程
      （锚点 tid）调度事件，其他连接/无关线程事件排除；
    - 子项2「慢段时间窗事件」（定界出瓶颈段且涉及本侧时）：按结论瓶颈段的
      时间窗过滤该节点全部连接 bpf 事件（高亮问题五元组），事件过多时支持
      过滤选择（全部 / 仅问题连接 / 仅其他连接 + 关键字，行 data-o 属性 +
      报告级 evf 过滤 JS）；
    - 子项3「问题时间窗全景」：ClientSend→ClientRecv 整窗全景，同样带
      过滤工具条（_window_events_table with_filter=True）。
    """

    def T(self, s):
        return datetime.combine(DAY, datetime.strptime(s, "%H:%M:%S.%f").time())

    def _ctx(self):
        slow = nla.SlowRecord("t1;abc", DAY,
                              {"network_residual_us": "2000", "e2e_us": "3000",
                               "framework_us": "2500", "method": "m"},
                              "x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.client_ip, ctx.server_ip = "10.0.0.1", "10.0.0.2"
        ctx.client_node, ctx.server_node = "node1", "node2"
        ctx.conn = ("10.0.0.1", 1111, "10.0.0.2", 2222)
        ctx.anchors = {
            "ClientSend": {"ts": self.T("10:00:00.100000"), "tid": 100, "cpu": 1,
                           "bid": None, "raw": "cs"},
            "ClientRecv": {"ts": self.T("10:00:00.400000"), "tid": 100, "cpu": 1,
                           "bid": None, "raw": "cr"},
            "ServerRecv": {"ts": self.T("10:00:00.200000"), "tid": 200, "cpu": 2,
                           "bid": None, "raw": "sr"},
            "ServerSend": {"ts": self.T("10:00:00.300000"), "tid": 200, "cpu": 2,
                           "bid": None, "raw": "ss"},
        }
        ctx.conclusion = {"category": "c", "label": "测试结论", "confidence": "高",
                          "bottleneck": None, "evidence": [], "suggestions": []}
        return ctx

    def _ev(self, **kw):
        ev = {"ts": self.T("10:00:00.310000"), "kind": "tcp_send_in", "tid": 1,
              "cpu": 1, "raw": "raw-line", "local_ip": "10.0.0.1",
              "local_port": 1111, "peer_ip": "10.0.0.2", "peer_port": 2222,
              "dir_arrow": "->", "match5t": True}
        ev.update(kw)
        return ev

    def _seg(self, key, start, end, desc="瓶颈段", abnormal=True):
        return {"key": key, "start": start, "end": end, "dur_us": 50000,
                "threshold_us": 200, "category": "c", "desc": desc,
                "abnormal": abnormal}

    # -- 分析层：_slow_seg_window_analysis --------------------------------

    def test_slow_seg_window_wire_both_sides(self):
        """线路段瓶颈（wire_s2c）→ 双侧窗口事件，窗口外事件排除。"""
        ctx = self._ctx()
        ctx.milestones["ServerTcpSendIn"] = self.T("10:00:00.300000")
        ctx.milestones["ClientTcpRecvFirst"] = self.T("10:00:00.350000")
        seg = self._seg("wire_s2c", "ServerTcpSendIn", "ClientTcpRecvFirst")
        ctx.kernel_segments.append(seg)
        ctx.conclusion["bottleneck"] = seg
        ctx.bpf_window_events["client"] = [
            self._ev(ts=self.T("10:00:00.310000"), match5t=True),
            self._ev(ts=self.T("10:00:00.320000"), match5t=False,
                     local_ip="9.9.9.9", local_port=9, peer_ip="8.8.8.8",
                     peer_port=8),
            self._ev(ts=self.T("10:00:00.390000"), match5t=True),  # 窗口外
        ]
        ctx.bpf_window_events["server"] = [
            self._ev(ts=self.T("10:00:00.305000"), match5t=True),
        ]
        nla._slow_seg_window_analysis(ctx)
        sw = ctx.slow_seg
        self.assertEqual(sw["seg_key"], "wire_s2c")
        self.assertEqual(sw["window_start"], self.T("10:00:00.300000"))
        self.assertEqual(sw["window_end"], self.T("10:00:00.350000"))
        self.assertEqual(sorted(sw["sides"]), ["client", "server"])  # 线路段涉及双侧
        cev = sw["sides"]["client"]["events"]
        self.assertEqual(len(cev), 2)
        self.assertEqual(sw["sides"]["client"]["n_mine"], 1)
        self.assertEqual(sw["sides"]["client"]["n_other"], 1)
        self.assertEqual(sw["sides"]["server"]["n_mine"], 1)

    def test_slow_seg_window_server_only_seg(self):
        """server 侧段瓶颈（server_kernel_to_user）→ 仅 server 侧窗口事件。"""
        ctx = self._ctx()
        ctx.milestones["ServerTcpRecvLast"] = self.T("10:00:00.150000")
        # end=ServerRecv 为锚点而非 milestone：窗口终点取锚点
        seg = self._seg("server_kernel_to_user", "ServerTcpRecvLast", "ServerRecv")
        ctx.kernel_segments.append(seg)
        ctx.conclusion["bottleneck"] = seg
        ctx.bpf_window_events["server"] = [self._ev(ts=self.T("10:00:00.160000"))]
        ctx.bpf_window_events["client"] = [self._ev(ts=self.T("10:00:00.160000"))]
        nla._slow_seg_window_analysis(ctx)
        sw = ctx.slow_seg
        self.assertEqual(sw["seg_key"], "server_kernel_to_user")
        self.assertEqual(list(sw["sides"]), ["server"])
        self.assertEqual(sw["window_end"], self.T("10:00:00.200000"))

    def test_slow_seg_window_macro_bottleneck(self):
        """宏观三段瓶颈（ss_cr）→ 窗口取 ServerSend→ClientRecv 锚点，双侧。"""
        ctx = self._ctx()
        bott = {"key": "ss_cr", "label": "ServerSend→ClientRecv", "dur_us": 100000,
                "threshold_us": 500, "category": "server_to_client_path",
                "abnormal": True}
        ctx.conclusion["bottleneck"] = bott
        ctx.bpf_window_events["client"] = [self._ev(ts=self.T("10:00:00.350000"))]
        ctx.bpf_window_events["server"] = [self._ev(ts=self.T("10:00:00.310000"))]
        nla._slow_seg_window_analysis(ctx)
        sw = ctx.slow_seg
        self.assertEqual(sw["seg_key"], "ss_cr")
        self.assertEqual(sw["window_start"], self.T("10:00:00.300000"))
        self.assertEqual(sw["window_end"], self.T("10:00:00.400000"))
        self.assertEqual(sorted(sw["sides"]), ["client", "server"])

    def test_slow_seg_window_no_bottleneck(self):
        """无瓶颈段（证据不足）→ 不生成慢段窗口。"""
        ctx = self._ctx()
        ctx.bpf_window_events["client"] = [self._ev()]
        nla._slow_seg_window_analysis(ctx)
        self.assertEqual(ctx.slow_seg, {})

    def test_slow_seg_window_evidence_seg_uses_ts(self):
        """证据分段（_start_ts/_end_ts）作为瓶颈时窗口直接取时间戳。"""
        ctx = self._ctx()
        seg = self._seg("server_oncpu_to_user", "ThreadOnCpu", "ServerRecv")
        seg["_start_ts"] = self.T("10:00:00.170000")
        seg["_end_ts"] = self.T("10:00:00.190000")
        ctx.kernel_segments.append(seg)
        ctx.conclusion["bottleneck"] = seg
        ctx.bpf_window_events["server"] = [
            self._ev(ts=self.T("10:00:00.180000")),
            self._ev(ts=self.T("10:00:00.150000")),  # 窗口外
        ]
        nla._slow_seg_window_analysis(ctx)
        self.assertEqual(ctx.slow_seg["window_start"], self.T("10:00:00.170000"))
        self.assertEqual(ctx.slow_seg["window_end"], self.T("10:00:00.190000"))
        self.assertEqual(len(ctx.slow_seg["sides"]["server"]["events"]), 1)

    # -- 分析层：_problem_request_events ----------------------------------

    def test_problem_request_events(self):
        """问题请求相关 = 问题连接五元组事件 + 锚点 tid 调度事件，其余排除。"""
        ctx = self._ctx()
        ctx.bpf_window_events["client"] = [
            self._ev(match5t=True),                                     # 问题连接
            self._ev(match5t=False, local_ip="9.9.9.9", local_port=9,
                     peer_ip="8.8.8.8", peer_port=8),                    # 其他连接
            {"ts": self.T("10:00:00.110000"), "kind": "sched_switch", "cpu": 1,
             "prev_comm": "a", "prev_pid": 100, "next_comm": "b",
             "next_pid": 555, "raw": "sw-anchor", "match5t": None},     # 锚点tid
            {"ts": self.T("10:00:00.120000"), "kind": "sched_switch", "cpu": 1,
             "prev_comm": "a", "prev_pid": 777, "next_comm": "b",
             "next_pid": 888, "raw": "sw-other", "match5t": None},      # 无关线程
            {"ts": self.T("10:00:00.130000"), "kind": "sched_waking",
             "tid": 5, "cpu": 1, "comm": "c", "pid": 100, "target_cpu": 1,
             "raw": "wk-anchor", "match5t": None},                      # pid=锚点tid
        ]
        out = nla._problem_request_events(ctx, "client")
        raws = [e["raw"] for e in out]
        self.assertEqual(len(out), 3)
        self.assertIn("raw-line", raws)
        self.assertIn("sw-anchor", raws)
        self.assertIn("wk-anchor", raws)
        self.assertNotIn("sw-other", raws)
        # server 侧锚点 tid（200）无关的调度事件不被 client 侧收入
        out_srv = nla._problem_request_events(ctx, "server")
        self.assertEqual(out_srv, [])

    # -- 渲染层：子项结构 + 过滤控件 --------------------------------------

    def test_side_events_html_sub_items(self):
        """bpf 事件明细 = 问题请求相关（默认展开）+ 慢段窗口 + 时间窗全景。"""
        ctx = self._ctx()
        seg = self._seg("wire_s2c", "ServerTcpSendIn", "ClientTcpRecvFirst")
        ctx.milestones["ServerTcpSendIn"] = self.T("10:00:00.300000")
        ctx.milestones["ClientTcpRecvFirst"] = self.T("10:00:00.350000")
        ctx.conclusion["bottleneck"] = seg
        ctx.bpf_window_events["client"] = [
            self._ev(match5t=True),
            self._ev(match5t=False, local_ip="9.9.9.9", local_port=9,
                     peer_ip="8.8.8.8", peer_port=8),
        ]
        ctx.bpf_window_events["server"] = [self._ev(match5t=True)]
        nla._slow_seg_window_analysis(ctx)
        out = nla._side_events_html(ctx, "client")
        # 子项1：问题请求相关（默认展开）
        self.assertIn("问题请求相关事件", out)
        self.assertIn("<details open>", out)
        # 子项2：慢段时间窗（含瓶颈段描述 + 过滤控件）
        self.assertIn("慢段时间窗事件", out)
        self.assertIn("瓶颈段", out)
        self.assertIn('class="evf-bar"', out)
        self.assertIn('data-f="mine"', out)
        self.assertIn('data-f="other"', out)
        self.assertIn('class="evf-input"', out)
        self.assertIn('data-o="mine"', out)
        self.assertIn('data-o="other"', out)
        self.assertIn('class="hl5t"', out)
        # 子项3：问题时间窗全景
        self.assertIn("问题时间窗全景", out)
        # server 侧只涉及全景（wire 段双侧都有慢段窗口）
        out_srv = nla._side_events_html(ctx, "server")
        self.assertIn("慢段时间窗事件", out_srv)

    def test_slow_win_table_row_cap_and_empty(self):
        """慢段窗口表（with_filter）：行数上限 + 空事件提示。"""
        evs = [self._ev(match5t=True, tid=i, ts=self.T("10:00:00.31%04d" % i))
               for i in range(nla.EVENTS_TABLE_MAX_ROWS + 10)]
        out = nla._window_events_table(evs, "标题", with_filter=True)
        self.assertIn("仅列前 %d 条" % nla.EVENTS_TABLE_MAX_ROWS, out)
        self.assertIn('class="evf-bar"', out)
        self.assertIn("无匹配事件",
                      nla._window_events_table([], "标题", with_filter=True))

    def test_side_events_slow_seg_not_involved(self):
        """瓶颈段不涉及本侧 → 无慢段窗口子项，仍有问题请求相关 + 全景。"""
        ctx = self._ctx()
        ctx.milestones["ServerTcpRecvLast"] = self.T("10:00:00.150000")
        seg = self._seg("server_kernel_to_user", "ServerTcpRecvLast", "ServerRecv")
        ctx.conclusion["bottleneck"] = seg
        ctx.bpf_window_events["client"] = [self._ev(match5t=True)]
        ctx.bpf_window_events["server"] = [self._ev(match5t=True)]
        nla._slow_seg_window_analysis(ctx)
        out = nla._side_events_html(ctx, "client")
        self.assertNotIn("慢段时间窗事件", out)
        self.assertIn("问题请求相关事件", out)
        self.assertIn("问题时间窗全景", out)

    def test_report_evf_js_and_css(self):
        """报告级：evf 过滤 JS（evfApply）+ 工具条样式注入。"""
        import argparse
        ctx = self._ctx()
        ctx.bpf_window_events["client"] = [self._ev(match5t=True)]
        ns = argparse.Namespace(residual_threshold=1000)
        out = nla.generate_report([ctx], ns, "/tmp")
        self.assertIn("evfApply", out)
        self.assertIn("evf-btn", out)
        self.assertIn("evf-input", out)
        self.assertIn("evf-count", out)

    # -- JSON / raw 输出 ---------------------------------------------------

    def test_json_slow_seg_window(self):
        import argparse
        ctx = self._ctx()
        seg = self._seg("wire_s2c", "ServerTcpSendIn", "ClientTcpRecvFirst")
        ctx.milestones["ServerTcpSendIn"] = self.T("10:00:00.300000")
        ctx.milestones["ClientTcpRecvFirst"] = self.T("10:00:00.350000")
        ctx.conclusion["bottleneck"] = seg
        ctx.bpf_window_events["client"] = [
            self._ev(match5t=True),
            self._ev(match5t=False, local_ip="9.9.9.9", local_port=9,
                     peer_ip="8.8.8.8", peer_port=8),
        ]
        ctx.bpf_window_events["server"] = [self._ev(match5t=True)]
        nla._slow_seg_window_analysis(ctx)
        ns = argparse.Namespace(residual_threshold=1000)
        doc = json.loads(nla.generate_json([ctx], ns, "/tmp"))
        sw = doc["traces"][0]["slow_seg_window"]
        self.assertEqual(sw["seg_key"], "wire_s2c")
        self.assertEqual(sw["window_start"],
                         self.T("10:00:00.300000").isoformat())
        self.assertEqual(sw["sides"]["client"]["n_mine"], 1)
        self.assertEqual(sw["sides"]["client"]["n_other"], 1)
        self.assertTrue(sw["sides"]["client"]["events"][0]["match5t"])
        # 无慢段时输出 null
        doc2 = json.loads(nla.generate_json([self._ctx()], ns, "/tmp"))
        self.assertIsNone(doc2["traces"][0]["slow_seg_window"])

    def test_raw_slow_seg_window(self):
        import argparse
        ctx = self._ctx()
        seg = self._seg("wire_s2c", "ServerTcpSendIn", "ClientTcpRecvFirst")
        ctx.milestones["ServerTcpSendIn"] = self.T("10:00:00.300000")
        ctx.milestones["ClientTcpRecvFirst"] = self.T("10:00:00.350000")
        ctx.conclusion["bottleneck"] = seg
        ctx.bpf_window_events["client"] = [
            self._ev(match5t=True),
            self._ev(match5t=False, local_ip="9.9.9.9", local_port=9,
                     peer_ip="8.8.8.8", peer_port=8, raw="other-conn-line"),
        ]
        ctx.bpf_window_events["server"] = [self._ev(match5t=True)]
        nla._slow_seg_window_analysis(ctx)
        ns = argparse.Namespace(residual_threshold=1000)
        disc = mock.Mock()
        disc.bpf_by_node = {}
        raw = nla.generate_raw([ctx], ns, "/tmp", disc, {})
        self.assertIn("慢段时间窗 bpf 事件", raw)
        self.assertIn("瓶颈段", raw)
        self.assertIn("▶ raw-line", raw)            # 问题五元组行 ▶ 标注
        self.assertIn("other-conn-line", raw)       # 其他连接事件混排
        # 无慢段时不输出该节
        raw2 = nla.generate_raw([self._ctx()], ns, "/tmp", disc, {})
        self.assertNotIn("慢段时间窗 bpf 事件", raw2)


class TestSlowSegEndToEnd(unittest.TestCase):
    """端到端：analyze 流程接通慢段窗口 + 问题请求相关子项。"""

    def setUp(self):
        self._root = Path(tempfile.mkdtemp(prefix="tst_slowseg_"))
        cdir = self._root / "collected" / "pod_node1_client"
        cdir.mkdir(parents=True)
        wdir = self._root / "collected_worker_logs" / "pod_node1_worker"
        wdir.mkdir(parents=True)
        bdir = self._root / "dscollect_log"
        bdir.mkdir(parents=True)
        (self._root / "latency_warn_log").mkdir(parents=True)
        (self._root / "latency_warn_log" / "node1_latency_warn.log").write_text(
            "", encoding="utf-8")
        slow_line = ("2026-08-22T10:00:00.400000 | I | f.cpp:1 | 10.0.0.1 | 1:100 | "
                     "t1;aaa |  |  "
                     + SLOW_MSG.replace("trace_id=getBuffer-25487-00004775;117c5c4a91c7",
                                        "trace_id=t1;aaa")
                     .replace("ClientSend=88035205620370", "ClientSend=100000000000")
                     .replace("ClientRecv=88035221862010", "ClientRecv=100000400000")
                     .replace("ServerSend=88038917846674", "ServerSend=100000105400")
                     .replace("ServerRecv=88038917594514", "ServerRecv=100000105000")
                     .replace("192.168.219.138", "10.0.0.1")
                     + "\n")
        (cdir / "c.log").write_text(
            "2026-08-22T10:00:00.100000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
            "t1;aaa |  |  yyl1 ClientSend ts 100000000000 tid 100 cpu 1\n"
            "2026-08-22T10:00:00.400000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
            "t1;aaa |  |  yyl1 ClientRecv ts 100000400000 tid 100 cpu 1\n"
            + slow_line,
            encoding="utf-8")
        # server 处理段 400us（< max(500, 2*(queue+exec))，不异常）→
        # 瓶颈落到 client_user_to_kernel（300us > 100us 阈值）
        (wdir / "w.log").write_text(
            "2026-08-22T10:00:00.105000 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
            "t1;aaa |  |  yyl1 ServerRecv ts 100000105000 tid 200 cpu 2\n"
            "2026-08-22T10:00:00.105400 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
            "t1;aaa |  |  yyl1 ServerSend ts 100000105400 tid 200 cpu 2\n",
            encoding="utf-8")
        self._bpf = bdir / "bpf-node1-192.168.1.1.log"

    def test_slow_seg_wired_into_analyze(self):
        """client_user_to_kernel 异常为瓶颈 → 慢段窗口 + 三子项渲染。"""
        self._bpf.write_text(
            # 问题连接 tcp send（.100300 → client_user_to_kernel=300us > 100us 异常）
            "10:00:00:100300 tcp  send in  tid 1 cpu 1 size 100 "
            "10.0.0.1:12345 -> 10.0.0.2:8080\n"
            # 同窗口内其他连接事件（不同 IP → 全景混排，不干扰连接识别）
            "10:00:00:100100 tcp  send in  tid 2 cpu 2 size 200 "
            "9.9.9.9:9999 -> 8.8.8.8:8888\n",
            encoding="utf-8")
        disc, contexts, _ = nla.analyze(str(self._root), residual_threshold=1000,
                                        window_pad_ms=2, sched_pad_ms=10)
        ctx = contexts[0]
        # 瓶颈段 client_user_to_kernel（300us > 100us 阈值）→ 慢段窗口
        self.assertEqual(ctx.conclusion["bottleneck"]["key"],
                         "client_user_to_kernel")
        sw = ctx.slow_seg
        self.assertEqual(sw["seg_key"], "client_user_to_kernel")
        self.assertIn("client", sw["sides"])
        self.assertNotIn("server", sw["sides"])   # client 侧段不涉及 server
        self.assertEqual(sw["sides"]["client"]["n_mine"], 1)
        self.assertEqual(sw["sides"]["client"]["n_other"], 1)
        # 问题请求相关事件：仅问题连接（bpf 无 sched 事件）
        req = nla._problem_request_events(ctx, "client")
        self.assertEqual(len(req), 1)
        self.assertEqual(req[0]["local_port"], 12345)
        # HTML：三子项 + 过滤控件
        out = nla._trace_html(ctx, 1)
        self.assertIn("问题请求相关事件", out)
        self.assertIn("慢段时间窗事件", out)
        self.assertIn("问题时间窗全景", out)
        self.assertIn('class="evf-bar"', out)
        self.assertIn('data-o="mine"', out)
        # 问题请求相关子项不含其他连接事件行
        req_html = nla._side_events_html(ctx, "client")
        first_sub = req_html.split("慢段时间窗事件")[0]
        self.assertIn("10.0.0.1:12345", first_sub)
        self.assertNotIn("10.0.0.1:9999", first_sub)


if __name__ == "__main__":
    unittest.main(verbosity=2)
