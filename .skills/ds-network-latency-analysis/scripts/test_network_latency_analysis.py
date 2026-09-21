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
        # que 行序号字段为 tp_rcv_nxt（显式解析，不再从 tp_ 前缀误取为 rcv_nxt）
        self.assertEqual(ev["tp_rcv_nxt"], 4187256525)
        self.assertNotIn("rcv_nxt", ev)

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


class TestWarmupTraceFilter(unittest.TestCase):
    """预热阶段 UUID 格式 trace_id（如 d855850b-54c6-4968-8cc2-1d4b974d88bc，
    不计入 p99）在慢请求扫描时过滤；正常业务 trace（setStringView-/getBuffer-
    等前缀）保留；--trace 显式指定的 UUID trace 保留（用户主动要求）。"""

    UUID_TRACE = "d855850b-54c6-4968-8cc2-1d4b974d88bc"
    NORMAL_TRACE = "setStringView-106-96-00082604;abcdef012345"

    def _log(self, trace, residual):
        slow = (SLOW_MSG.replace("network_residual_us=15989",
                                 "network_residual_us=%d" % residual)
                .replace("getBuffer-25487-00004775;117c5c4a91c7", trace))
        return ("2026-08-21T21:31:21.077013 | I | brpc_perf_trace.h:368 | 1.1.1.1 | "
                "1:1 | tr | u |  " + slow + "\n")

    def _scan(self, content, only=None):
        with tempfile.NamedTemporaryFile("wb", suffix=".log", delete=False) as fh:
            fh.write(content.encode())
            path = Path(fh.name)
        try:
            return nla.scan_slow_records([path], 1000, only_traces=only)
        finally:
            os.unlink(path)

    def test_uuid_trace_filtered(self):
        recs = self._scan(self._log(self.UUID_TRACE, 5000))
        self.assertEqual(recs, [])

    def test_normal_trace_kept(self):
        recs = self._scan(self._log(self.NORMAL_TRACE, 5000))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].trace_id, self.NORMAL_TRACE)

    def test_mixed_only_normal_survives(self):
        recs = self._scan(self._log(self.UUID_TRACE, 9000)
                          + self._log(self.NORMAL_TRACE, 5000))
        self.assertEqual([r.trace_id for r in recs], [self.NORMAL_TRACE])

    def test_uuid_trace_explicit_only_kept(self):
        """--trace 显式指定 UUID 子串 = 用户主动要求，不过滤。"""
        recs = self._scan(self._log(self.UUID_TRACE, 5000),
                          only=[self.UUID_TRACE[:8]])
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].trace_id, self.UUID_TRACE)


class TestBusinessRecvSlow(unittest.TestCase):
    """业务收包慢根因：tcp_recv_que（协议栈收包入队）→ tcp_recv_in（业务调 recv）
    耗时长 → 归类 server/client_business_recv_slow，参与异常竞争成为定界根因。"""

    CIP, CPORT = "192.168.32.61", 39776
    SIP, SPORT = "192.168.52.197", 31501
    T0 = datetime(2026, 8, 23, 20, 45, 39)

    def _ctx(self, milestones):
        slow = nla.SlowRecord(
            "tr", self.T0,
            {"network_residual_us": "2000", "e2e_us": "3000", "framework_us": "2500",
             "method": "m", "remote_processing_us": "0", "server_req_queue_us": "0",
             "server_exec_us": "0"},
            "/tmp/x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.idx = 0
        ctx.client_ip, ctx.server_ip = self.CIP, self.SIP
        ctx.conn = (self.CIP, self.CPORT, self.SIP, self.SPORT)
        ctx.milestones = milestones
        ctx.kernel_events = {"client": [], "server": []}
        return ctx

    def _seg(self, ctx, key):
        return next((s for s in ctx.kernel_segments if s["key"] == key), None)

    def test_server_recvq_to_recv_segment_built(self):
        """ServerTcpRecvQue → ServerTcpRecvFirst 段构建；超阈值 abnormal。"""
        ms = {
            "ServerTcpRecvQue": self.T0.replace(microsecond=100000),
            "ServerTcpRecvFirst": self.T0.replace(microsecond=150000),
            "ServerTcpRecvLast": self.T0.replace(microsecond=150100),
        }
        ctx = self._ctx(ms)
        nla.build_kernel_segments(ctx)
        seg = self._seg(ctx, "server_recvq_to_recv")
        self.assertIsNotNone(seg)
        self.assertAlmostEqual(seg["dur_us"], 50000)  # 100000 → 150000
        self.assertTrue(seg["abnormal"])              # 50000 > 100
        self.assertEqual(seg["category"], "server_business_recv_slow")
        self.assertFalse(seg.get("evidence"))

    def test_client_recvq_to_recv_segment_built(self):
        ms = {
            "ClientTcpRecvQue": self.T0.replace(microsecond=100000),
            "ClientTcpRecvFirst": self.T0.replace(microsecond=105000),
            "ClientTcpRecvLast": self.T0.replace(microsecond=105100),
        }
        ctx = self._ctx(ms)
        nla.build_kernel_segments(ctx)
        seg = self._seg(ctx, "client_recvq_to_recv")
        self.assertIsNotNone(seg)
        self.assertAlmostEqual(seg["dur_us"], 5000)   # 5000 > 1000 阈值
        self.assertTrue(seg["abnormal"])
        self.assertEqual(seg["category"], "client_business_recv_slow")

    def test_recvq_to_recv_normal_not_abnormal(self):
        """入队→业务 recv 在阈值内（server 100us / client 1000us）不异常。"""
        ms = {
            "ServerTcpRecvQue": self.T0.replace(microsecond=100000),
            "ServerTcpRecvFirst": self.T0.replace(microsecond=100080),
            "ServerTcpRecvLast": self.T0.replace(microsecond=100100),
        }
        ctx = self._ctx(ms)
        nla.build_kernel_segments(ctx)
        seg = self._seg(ctx, "server_recvq_to_recv")
        self.assertIsNotNone(seg)
        self.assertFalse(seg["abnormal"])

    def test_blocking_recv_first_before_que_skipped(self):
        """阻塞收包场景 recvmsg 先于入队（TcpRecvFirst < TcpRecvQue）→ 跳过该段。"""
        ms = {
            "ServerTcpRecvQue": self.T0.replace(microsecond=200000),
            "ServerTcpRecvFirst": self.T0.replace(microsecond=100000),
            "ServerTcpRecvLast": self.T0.replace(microsecond=200500),
        }
        ctx = self._ctx(ms)
        nla.build_kernel_segments(ctx)
        self.assertIsNone(self._seg(ctx, "server_recvq_to_recv"))

    def test_conclusion_picks_business_recv_slow(self):
        """业务收包段为最大异常段 → 定界 server_business_recv_slow。"""
        ms = {
            "ServerTcpRecvQue": self.T0.replace(microsecond=100000),
            "ServerTcpRecvFirst": self.T0.replace(microsecond=160000),
            "ServerTcpRecvLast": self.T0.replace(microsecond=160100),
            "ServerRecv": self.T0.replace(microsecond=160200),
        }
        ctx = self._ctx(ms)
        nla.build_kernel_segments(ctx)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "server_business_recv_slow")
        self.assertIn("server_business_recv_slow", nla.CATEGORY_LABELS)
        self.assertIn("server_business_recv_slow", nla.CATEGORY_SUGGESTIONS)
        self.assertIn("client_business_recv_slow", nla.CATEGORY_LABELS)
        self.assertIn("client_business_recv_slow", nla.CATEGORY_SUGGESTIONS)


class TestNodeInternalAttribution(unittest.TestCase):
    """传输类瓶颈（network_*）命中且瓶颈侧节点内段（网卡↔协议栈）占主导时，
    细分改写为节点内根因（不再笼统归"网络传输"）。"""

    T0 = datetime(2026, 8, 23, 20, 45, 39)

    def _ctx(self, base_cat, wire_key, internal_key, wire_us, internal_us):
        slow = nla.SlowRecord(
            "tr", self.T0,
            {"network_residual_us": "2000", "e2e_us": "3000", "framework_us": "2500",
             "method": "m", "remote_processing_us": "0", "server_req_queue_us": "0",
             "server_exec_us": "0"},
            "/tmp/x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.idx = 0
        ctx.client_ip, ctx.server_ip = "1.1.1.1", "2.2.2.2"
        ctx.kernel_segments = [
            {"key": wire_key, "start": "A", "end": "B", "dur_us": wire_us,
             "threshold_us": 200, "category": base_cat, "desc": "wire",
             "abnormal": True, "evidence": False},
            {"key": internal_key, "start": "C", "end": "D", "dur_us": internal_us,
             "threshold_us": None, "category": "nic_evidence", "desc": "节点内",
             "abnormal": False, "evidence": True},
        ]
        return ctx

    def test_c2s_server_ingress_dominant_rewrites(self):
        """wire_c2s 慢且 server_nic_to_stack 占 80% → server_node_ingress_delay。"""
        ctx = self._ctx("network_c2s_transmission", "wire_c2s",
                        "server_nic_to_stack", 10000, 8000)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "server_node_ingress_delay")
        self.assertEqual(ctx.conclusion["confidence"], "高")
        self.assertTrue(any("节点内" in s and "80.0%" in s
                            for s in ctx.conclusion["evidence"]))

    def test_c2s_client_egress_dominant_rewrites(self):
        ctx = self._ctx("network_c2s_transmission", "wire_c2s",
                        "client_stack_to_nic", 12000, 9000)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "client_node_egress_delay")

    def test_s2c_client_ingress_dominant_rewrites(self):
        ctx = self._ctx("network_s2c_transmission", "wire_s2c",
                        "client_nic_to_stack", 10000, 7500)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "client_node_ingress_delay")

    def test_s2c_server_egress_dominant_rewrites(self):
        ctx = self._ctx("network_s2c_transmission", "wire_s2c",
                        "server_stack_to_nic", 10000, 9500)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "server_node_egress_delay")

    def test_share_below_threshold_keeps_network(self):
        """节点内占比 <70% → 维持 network_* 不改写。"""
        ctx = self._ctx("network_c2s_transmission", "wire_c2s",
                        "server_nic_to_stack", 10000, 5000)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "network_c2s_transmission")

    def test_absolute_below_1ms_keeps_network(self):
        """节点内占比达标但绝对值 <1ms → 不改写（避免小段噪声）。"""
        ctx = self._ctx("network_c2s_transmission", "wire_c2s",
                        "server_nic_to_stack", 1200, 900)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "network_c2s_transmission")

    def test_categories_have_labels_and_suggestions(self):
        for cat in ("server_node_ingress_delay", "client_node_ingress_delay",
                    "client_node_egress_delay", "server_node_egress_delay"):
            self.assertIn(cat, nla.CATEGORY_LABELS)
            self.assertIn(cat, nla.CATEGORY_SUGGESTIONS)


class TestServerIpRecover(unittest.TestCase):
    """ServerRecv/ServerSend 锚点行缺失（日志可选）但 worker 日志中有该 trace 的
    业务行时：从业务行恢复 server pod IP（host 列），server 侧链路可正常关联。"""

    T0 = "2026-08-21T21:31:21.077013"
    WORKER_IP = "192.168.52.197"

    def _worker_line(self, msg="handle request step1"):
        # iter_marker_lines 产出的命中行为 str（已按 utf-8 解码）
        return ("%s | I | svc.cpp:88 | %s | 6289:6289 | tr | u |  %s\n"
                % (self.T0, self.WORKER_IP, msg))

    def _ctx(self):
        slow = nla.SlowRecord(
            "tr", datetime(2026, 8, 21, 21, 31, 21),
            {"network_residual_us": "2000", "ClientSend": "100",
             "ClientRecv": "200", "method": "m"},
            "/tmp/c.log", "pod")
        return nla.TraceContext(slow)

    def test_recover_from_worker_info_line(self):
        """锚点缺失 + worker 业务行存在 → server_ip/pod_dir 恢复 + 推测说明。"""
        ctx = self._ctx()
        lines = [("worker", "/tmp/w/worker_192.168.52.197/kvcache.INFO.log",
                  self._worker_line())]
        ok = nla._recover_server_from_info(ctx, lines)
        self.assertTrue(ok)
        self.assertEqual(ctx.server_ip, self.WORKER_IP)
        self.assertEqual(ctx.server_pod_dir, "worker_192.168.52.197")
        self.assertTrue(any("ServerRecv/ServerSend" in m and "业务行" in m
                            for m in ctx.missing))

    def test_no_worker_lines_returns_false(self):
        ctx = self._ctx()
        ok = nla._recover_server_from_info(
            ctx, [("client", "/tmp/c/pod/ds_client.INFO.log",
                   self._worker_line())])
        self.assertFalse(ok)
        self.assertIsNone(ctx.server_ip)

    def test_garbage_line_skipped(self):
        ctx = self._ctx()
        ok = nla._recover_server_from_info(
            ctx, [("worker", "/tmp/w/x/kvcache.INFO.log", "not an info line\n")])
        self.assertFalse(ok)
        self.assertIsNone(ctx.server_ip)


class TestSynthesizedAnchors(unittest.TestCase):
    """锚点行缺失（新日志格式不再输出 ClientSend/ServerRecv ts 锚点行）时，
    从 SLOW 行内嵌的四个 ns 时间戳合成锚点：

    - ClientSend/ClientRecv 为 client 机器单调钟（差值 = e2e 精确）；
      墙钟换算：ClientRecv ≈ SLOW 行墙钟，ClientSend = 其减 e2e
    - ServerRecv/ServerSend 为 worker 机器单调钟；墙钟换算优先 worker
      URMA 行 trace_us（observed），无 URMA 行时用 worker 首行墙钟近似
    - sr/ss 超出 [cs-60s, cr+60s] 合理性窗 → 放弃合成 + 原因
    - 合成锚点带 synth 标记 + ◇ 推测证据说明方法
    夹具数值取自真实新格式日志（getBuffer UB 场景）。
    """

    SLOW_TS = datetime(2026, 9, 15, 18, 3, 9, 42342)   # SLOW 行墙钟
    CS_NS, CR_NS = "68710416514425", "68710430287853"  # client 单调钟 ns
    SR_NS, SS_NS = "68709892262763", "68709892509813"  # worker 单调钟 ns
    E2E_US = (68710430287853 - 68710416514425) / 1000.0      # 13773.428
    SRSS_US = (68709892509813 - 68709892262763) / 1000.0     # 247.05
    CIP, SIP = "192.168.219.103", "192.168.100.195"

    def _slow(self, with_ns=True, with_client_ns=True):
        fields = {"network_residual_us": "13526", "e2e_us": "13774",
                  "framework_us": "13531", "method": "datasystem.WorkerOCService.QueryAndGet",
                  "tid": "187"}
        if with_ns:
            fields.update({"ServerSend": self.SS_NS, "ServerRecv": self.SR_NS})
        if with_client_ns:
            fields.update({"ClientSend": self.CS_NS, "ClientRecv": self.CR_NS})
        return nla.SlowRecord(
            "getBuffer-86-96-00051136;b34ccc369f0b", self.SLOW_TS, fields,
            "/tmp/collected/SDK_%s/ds_client_96.INFO.log" % self.CIP,
            "SDK_%s" % self.CIP, host=self.CIP)

    def _urma_line(self, observed="68709892491",
                   wall="2026-09-15T18:03:09.028903"):
        return ("%s | I | urma_manager.cpp:1680 | %s | 9:248 | "
                "getBuffer-86-96-00051136;b34ccc369f0b | lzw-jingpai |  "
                "[SLOW LOG] [URMA_ELAPSED_TOTAL]: [urma_request_id:699618] "
                "total cost 0.188ms, trace_us:{post:68709892293, "
                "wait:68709892491, poll_begin:68709892474, "
                "observed:%s, waited_for_notification:1}"
                % (wall, self.SIP, observed))

    def _plain_worker_line(self, wall="2026-09-15T18:03:09.028959"):
        return ("%s | I | access_recorder.cpp:1066 | %s | 9:248 | "
                "getBuffer-86-96-00051136;b34ccc369f0b | lzw-jingpai | "
                "0 | DS_POSIX_QUERY_AND_GET | 283"
                % (wall, self.SIP))

    def _ctx(self, with_ns=True, with_client_ns=True):
        ctx = nla.TraceContext(self._slow(with_ns, with_client_ns))
        ctx.idx = 0
        return ctx

    def test_client_anchors_synthesized(self):
        """锚点行缺失 + SLOW 内嵌 client ns → cs/cr 合成（e2e 精确、墙钟≈SLOW）。"""
        ctx = self._ctx()
        nla._synthesize_anchors_from_slow(ctx, [])
        cs, cr = ctx.anchors.get("ClientSend"), ctx.anchors.get("ClientRecv")
        self.assertIsNotNone(cs)
        self.assertIsNotNone(cr)
        self.assertEqual(cr["ts"], self.SLOW_TS)
        # datetime 精度为微秒级，断言放宽到 ±1us
        self.assertAlmostEqual(
            (self.SLOW_TS - cs["ts"]).total_seconds() * 1e6,
            self.E2E_US, delta=1.0)
        self.assertTrue(cs.get("synth") and cr.get("synth"))
        self.assertEqual(cs["host"], self.CIP)
        self.assertEqual(ctx.client_ip, self.CIP)
        self.assertTrue(any("SLOW" in s and "合成" in s
                            for s in ctx.infer_evidence))

    def test_server_anchors_urma_reference(self):
        """worker URMA 行 trace_us → worker 单调钟→墙钟换算 sr/ss。"""
        ctx = self._ctx()
        ctx.server_ip = self.SIP
        ctx.server_pod_dir = "worker_%s" % self.SIP
        lines = [("worker", "/tmp/w/worker_%s/kvcache.INFO.log" % self.SIP,
                  self._urma_line())]
        nla._synthesize_anchors_from_slow(ctx, lines)
        sr, ss = ctx.anchors.get("ServerRecv"), ctx.anchors.get("ServerSend")
        self.assertIsNotNone(sr)
        self.assertIsNotNone(ss)
        # offset = 行墙钟 - observed(us)；sr = offset + sr_ns/1000
        off = datetime(2026, 9, 15, 18, 3, 9, 28903) - \
            timedelta(microseconds=68709892491)
        self.assertEqual(sr["ts"], off + timedelta(
            microseconds=int(self.SR_NS) / 1000.0))
        self.assertAlmostEqual(
            (ss["ts"] - sr["ts"]).total_seconds() * 1e6,
            self.SRSS_US, delta=1.0)
        self.assertTrue(sr.get("synth"))
        self.assertEqual(sr["host"], self.SIP)
        self.assertTrue(any("URMA" in s for s in ctx.infer_evidence))
        # macro / milestones 已由 _finalize_anchors 填充
        self.assertAlmostEqual(ctx.macro["sr_ss"], self.SRSS_US, delta=1.0)
        self.assertIn("ServerRecv", ctx.milestones)

    def test_server_anchors_first_line_fallback(self):
        """无 URMA 行 → worker 首行业务行墙钟近似 + ns 差值。"""
        ctx = self._ctx()
        ctx.server_ip = self.SIP
        first = datetime(2026, 9, 15, 18, 3, 9, 28959)
        lines = [("worker", "/tmp/w/worker_%s/kvcache.INFO.log" % self.SIP,
                  self._plain_worker_line())]
        nla._synthesize_anchors_from_slow(ctx, lines)
        sr, ss = ctx.anchors.get("ServerRecv"), ctx.anchors.get("ServerSend")
        self.assertIsNotNone(sr)
        self.assertEqual(sr["ts"], first)
        self.assertAlmostEqual(
            (ss["ts"] - sr["ts"]).total_seconds() * 1e6,
            self.SRSS_US, delta=1.0)
        self.assertTrue(any("首行" in s for s in ctx.infer_evidence))

    def test_server_sanity_check_skips(self):
        """URMA observed 异常（换算出 sr 超出 ±60s 窗）→ 放弃 server 合成。"""
        ctx = self._ctx()
        ctx.server_ip = self.SIP
        lines = [("worker", "/tmp/w/x/kvcache.INFO.log",
                  self._urma_line(observed="67609892491"))]  # 偏 10000s
        nla._synthesize_anchors_from_slow(ctx, lines)
        self.assertIsNone(ctx.anchors.get("ServerRecv"))
        self.assertIsNone(ctx.anchors.get("ServerSend"))
        # client 侧不受影响
        self.assertIsNotNone(ctx.anchors.get("ClientSend"))
        self.assertTrue(any("换算" in m for m in ctx.missing))

    def test_missing_ns_fields_no_synth(self):
        """SLOW 行无内嵌 ns 字段 → 不合成 + 原因说明。"""
        ctx = self._ctx(with_ns=False, with_client_ns=False)
        nla._synthesize_anchors_from_slow(ctx, [])
        self.assertEqual(ctx.anchors, {})
        self.assertTrue(any("内嵌时间戳" in m for m in ctx.missing))

    def test_build_context_anchors_integration(self):
        """主流程接入：锚点行全缺 + SLOW ns + worker URMA 行 → 四锚点齐全。"""
        ctx = self._ctx()
        lines = [("worker", "/tmp/w/worker_%s/kvcache.INFO.log" % self.SIP,
                  self._urma_line())]
        nla._build_context_anchors(
            ctx, [], [], lines)   # 锚点行为空（新日志格式）
        for k in ("ClientSend", "ClientRecv", "ServerRecv", "ServerSend"):
            self.assertIn(k, ctx.anchors, k)
        self.assertEqual(ctx.client_ip, self.CIP)
        self.assertEqual(ctx.server_ip, self.SIP)
        self.assertAlmostEqual(ctx.macro["sr_ss"], self.SRSS_US, delta=1.0)

    def test_slow_record_host_field(self):
        """SlowRecord 携带 SLOW 行 host 列（client pod IP）。"""
        slow = self._slow()
        self.assertEqual(slow.host, self.CIP)

    def test_worker_log_re_matches_access_log(self):
        """worker 日志发现纳入 access.log（worker 目录访问日志含 trace 行）。"""
        self.assertTrue(nla.WORKER_LOG_RE.match("access.log"))
        self.assertTrue(nla.WORKER_LOG_RE.match("kvcache.INFO.20260911.log"))
        self.assertFalse(nla.WORKER_LOG_RE.match("ds_client_96.INFO.log"))

    def test_build_anchors_unchanged_with_anchor_lines(self):
        """老格式回归：锚点行存在时行为不变（无 synth、macro 照常）。"""
        ctx = self._ctx()
        t = "2026-09-15T18:03:09.028529"
        cline = ("%s | I | a.cpp:1 | %s | 96:187 | "
                 "getBuffer-86-96-00051136;b34ccc369f0b | u |  "
                 "ClientSend ts %s tid 187" % (t, self.CIP, self.CS_NS))
        info = nla.parse_info_line(cline)
        info["_path"] = "/tmp/c.log"
        info["_pod_dir"] = "SDK_%s" % self.CIP
        nla.build_anchors(ctx, [info], [])
        self.assertIn("ClientSend", ctx.anchors)
        self.assertNotIn("synth", ctx.anchors["ClientSend"])
        self.assertEqual(ctx.client_ip, self.CIP)


class TestInferredLinkEvidence(unittest.TestCase):
    """锚点日志缺失（可选）时的推测链路证据。

    - _inferred_link_evidence：server 侧 bpf 事件补扫后，按内核点位推测链路
      （请求交付/响应发出/宏观三段等价）+ 原因；
    - _client_only_infer_evidence：server 侧完全无证据时，client 单侧事件
      推测方向 + 原因（server 处理与线路不可区分）。
    """

    CIP, CPORT = "192.168.32.61", 39776
    SIP, SPORT = "192.168.52.197", 31501
    T0 = datetime(2026, 8, 23, 20, 45, 39)

    def _ctx(self, milestones=None, client_events=None):
        slow = nla.SlowRecord(
            "tr", self.T0,
            {"network_residual_us": "2000", "e2e_us": "3000", "framework_us": "2500",
             "method": "m", "remote_processing_us": "0", "server_req_queue_us": "0",
             "server_exec_us": "0"},
            "/tmp/x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.idx = 0
        ctx.client_ip, ctx.server_ip = self.CIP, self.SIP
        ctx.conn = (self.CIP, self.CPORT, self.SIP, self.SPORT)
        ctx.milestones = milestones or {}
        ctx.kernel_events = {"client": list(client_events or []), "server": []}
        ctx.filtered_events = {"client": [], "server": []}
        ctx.bpf_window_events = {"client": [], "server": []}
        ctx.anchors["ClientSend"] = {"ts": self.T0, "tid": "1", "cpu": "1",
                                     "bid": None, "host": self.CIP,
                                     "pod_dir": "pod", "log_path": "p", "raw": "r"}
        ctx.anchors["ClientRecv"] = {"ts": self.T0, "tid": "1", "cpu": "1",
                                     "bid": None, "host": self.CIP,
                                     "pod_dir": "pod", "log_path": "p", "raw": "r"}
        return ctx

    def test_inferred_link_with_server_milestones(self):
        """server 侧里程碑齐 → 推测链路文本含请求交付/响应发出/三段等价。"""
        ms = {
            "ClientTcpSendIn": self.T0.replace(microsecond=100000),
            "ServerTcpRecvFirst": self.T0.replace(microsecond=105000),
            "ServerTcpSendIn": self.T0.replace(microsecond=300000),
            "ClientTcpRecvFirst": self.T0.replace(microsecond=400000),
        }
        ctx = self._ctx(ms)
        nla._inferred_link_evidence(ctx)
        self.assertTrue(ctx.infer_evidence)
        joined = "\n".join(ctx.infer_evidence)
        self.assertIn("推测", joined)
        self.assertIn("请求交付", joined)          # ServerTcpRecvFirst
        self.assertIn("响应发出", joined)          # ServerTcpSendIn
        self.assertIn("CS→SR", joined)             # 宏观三段等价
        self.assertIn("SR→SS", joined)
        self.assertIn("SS→CR", joined)
        # 结论 evidence 带 ◇ 前缀
        nla.ConclusionEngine.conclude(ctx)
        self.assertTrue(any(s.startswith("◇") for s in ctx.conclusion["evidence"]))

    def test_client_only_infer_direction(self):
        """无 server 侧证据 → client 单侧推测：发出/到达时刻 + 不可区分说明。"""
        ms = {
            "ClientTcpSendIn": self.T0.replace(microsecond=100000),
            "ClientNetifRx": self.T0.replace(microsecond=400000),
        }
        retrans = {"kind": "tcp_retransmit", "ts": self.T0, "raw": "r",
                   "local_ip": self.CIP, "local_port": self.CPORT,
                   "peer_ip": self.SIP, "peer_port": self.SPORT}
        ctx = self._ctx(ms, client_events=[retrans])
        nla._client_only_infer_evidence(ctx)
        joined = "\n".join(ctx.infer_evidence)
        self.assertIn("发出", joined)
        self.assertIn("到达", joined)
        self.assertIn("无法区分", joined)
        self.assertIn("重传", joined)               # 1 次重传 → 线路丢包提示

    def test_client_only_infer_no_events(self):
        """client 侧点位也缺 → 说明证据不足原因。"""
        ctx = self._ctx()
        nla._client_only_infer_evidence(ctx)
        joined = "\n".join(ctx.infer_evidence)
        self.assertIn("无法推测", joined)

    def test_client_only_infer_nic_fallback(self):
        """tcp 层点位缺失（SDK 直连 tcp 探针未启用）→ nic 层点位兜底推测：
        发出点位回退 ClientDevStartXmit，并给出 client 网卡→业务取包耗时。"""
        ms = {
            "ClientDevStartXmit": self.T0.replace(microsecond=100000),
            "ClientNetDevXmit": self.T0.replace(microsecond=101000),
            "ClientNetifRx": self.T0.replace(microsecond=400000),
        }
        ctx = self._ctx(ms)
        ctx.milestones["ClientRecv"] = self.T0.replace(microsecond=500000)
        nla._client_only_infer_evidence(ctx)
        joined = "\n".join(ctx.infer_evidence)
        self.assertIn("ClientDevStartXmit", joined)   # nic 层发出点位兜底
        self.assertIn("无法区分", joined)
        self.assertIn("业务取包", joined)              # 网卡→业务取包耗时


class TestSupplementServerScan(unittest.TestCase):
    """client_only（worker 日志未收集）且连接五元组经端口+时间推测识别后：
    server 节点 bpf 补扫结果的回填（事件/里程碑/全景/五元组过滤）。"""

    CIP, CPORT = "192.168.32.61", 39776
    SIP, SPORT = "192.168.52.197", 31501
    T0 = datetime(2026, 8, 23, 20, 45, 39)

    def _ctx(self):
        slow = nla.SlowRecord(
            "tr", self.T0,
            {"network_residual_us": "2000", "e2e_us": "3000", "framework_us": "2500",
             "method": "m", "remote_processing_us": "0", "server_req_queue_us": "0",
             "server_exec_us": "0"},
            "/tmp/x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.idx = 0
        ctx.client_only = True
        ctx.client_ip, ctx.server_ip = self.CIP, self.SIP
        ctx.conn = (self.CIP, self.CPORT, self.SIP, self.SPORT)
        ctx.milestones = {}
        ctx.kernel_events = {"client": [], "server": []}
        ctx.filtered_events = {"client": [], "server": []}
        ctx.bpf_window_events = {"client": [], "server": []}
        ctx.anchors["ClientSend"] = {"ts": self.T0, "tid": "1", "cpu": "1",
                                     "bid": None, "host": self.CIP,
                                     "pod_dir": "pod", "log_path": "p", "raw": "r"}
        ctx.anchors["ClientRecv"] = {"ts": self.T0, "tid": "1", "cpu": "1",
                                     "bid": None, "host": self.CIP,
                                     "pod_dir": "pod", "log_path": "p", "raw": "r"}
        return ctx

    @staticmethod
    def _srv_ev(kind, us, match=True):
        ip, port = (TestSupplementServerScan.SIP,
                    TestSupplementServerScan.SPORT) if match else ("10.0.0.9", 1234)
        return {"kind": kind, "ts": TestSupplementServerScan.T0.replace(
                    microsecond=us), "cpu": 3, "tid": 9,
                "local_ip": ip, "local_port": port,
                "peer_ip": TestSupplementServerScan.CIP,
                "peer_port": TestSupplementServerScan.CPORT, "raw": "r"}

    def test_fill_server_side_from_scan(self):
        """补扫事件回填：kernel_events/里程碑/五元组过滤/全景标注。"""
        ctx = self._ctx()
        kernel_results = {(0, "server"): [
            self._srv_ev("tcp_recv_in", 50000),
            self._srv_ev("tcp_recv_in", 50100),
            self._srv_ev("tcp_recv_que", 49000),
            self._srv_ev("tcp_send_in", 200000),
            self._srv_ev("tcp_recv_in", 60000, match=False),  # 其他连接
        ]}
        window_net_results = {(0, "server"): [self._srv_ev("nic_rx_skb", 48000)]}
        nla._fill_server_side_from_scan(ctx, kernel_results, window_net_results)
        # 里程碑填充（含推测说明）
        self.assertIn("ServerTcpRecvQue", ctx.milestones)
        self.assertIn("ServerTcpRecvFirst", ctx.milestones)
        self.assertIn("ServerTcpRecvLast", ctx.milestones)
        self.assertIn("ServerTcpSendIn", ctx.milestones)
        # 五元组过滤：其他连接事件不进 filtered
        self.assertTrue(all(e["local_ip"] == self.SIP
                            for e in ctx.filtered_events["server"]))
        self.assertEqual(len(ctx.filtered_events["server"]), 4)
        # 全景事件带 match5t 标注
        self.assertTrue(all("match5t" in e for e in ctx.bpf_window_events["server"]))
        self.assertTrue(any(e["match5t"] is False
                            for e in ctx.bpf_window_events["server"]))
        # 推测链路证据生成
        self.assertTrue(ctx.infer_evidence)

    def test_fill_server_side_empty_events(self):
        """补扫无事件 → 不填充，注明原因（连接推测可能有误）。"""
        ctx = self._ctx()
        nla._fill_server_side_from_scan(ctx, {}, {})
        self.assertEqual(ctx.kernel_events["server"], [])
        self.assertTrue(any("无该连接" in m or "未命中" in m
                            for m in ctx.missing))


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


class TestBpfSeqPrefix(unittest.TestCase):
    """新格式 bpf 日志：行首带序号（"16438196 18:03:09:028931 dev_start_xmit: ..."）。

    解析层（parse_bpf_line）与扫描层（BpfScanner seek/full）都必须兼容，
    老格式（行首直接是 HH:MM:SS:uuuuuu）行为不变。
    """

    CIP, SIP = "192.168.219.103", "192.168.100.195"

    def _win(self, s, e, trace="A", side="client"):
        return nla.TraceWindow(trace, side, s, e, self.CIP, self.SIP)

    def _write_bpf(self, lines):
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as fh:
            fh.write("\n".join(lines) + "\n")
            return fh.name

    def test_parse_bpf_line_seq_prefix(self):
        # 用户实测新格式：前导序号 + nic/tcp 事件（数值取自真实示例）
        day = datetime(2026, 9, 15)
        nic = nla.parse_bpf_line(
            "16438196 18:03:09:028931 dev_start_xmit: sip:192.168.100.195, "
            "sport:31402 -> dip:192.168.219.103, dport:55064, seq:1316402115, "
            "len:276, dev:eth0 cpu:25", day)
        self.assertIsNotNone(nic)
        self.assertEqual(nic["kind"], "nic_dev_xmit_start")
        self.assertEqual(nic["ts"], datetime(2026, 9, 15, 18, 3, 9, 28931))
        self.assertEqual(nic["src_ip"], "192.168.100.195")
        self.assertEqual(nic["dst_ip"], "192.168.219.103")
        self.assertEqual(nic["dev"], "eth0")
        tcp = nla.parse_bpf_line(
            "16360719 18:03:09:036307 tcp  recv que tid 3913697 cpu 193 size 210 "
            "tp_rcv_nxt:1316402115, 192.168.219.103:55064 <- 192.168.100.195:31402",
            day)
        self.assertIsNotNone(tcp)
        self.assertEqual(tcp["kind"], "tcp_recv_que")
        self.assertEqual(tcp["local_ip"], "192.168.219.103")
        self.assertEqual(tcp["local_port"], 55064)
        self.assertEqual(tcp["peer_ip"], "192.168.100.195")
        self.assertEqual(tcp["peer_port"], 31402)

    def test_parse_bpf_line_old_format_unchanged(self):
        ev = nla.parse_bpf_line(
            "21:31:21:060000 tcp  send in  tid 1 cpu 1 size 10 10.0.0.1:1 -> 10.0.0.2:2",
            datetime(2026, 8, 21))
        self.assertEqual(ev["kind"], "tcp_send_in")
        self.assertEqual(ev["ts"], datetime(2026, 8, 21, 21, 31, 21, 60000))

    def _seq_events(self):
        # 噪声（前后）+ 窗口内 nic/tcp 事件，全部带前导序号
        return (["16430000 18:03:08:900000 dev_start_xmit: sip:192.168.100.195, "
                 "sport:31402 -> dip:192.168.219.103, dport:55064, seq:1, "
                 "len:60, dev:eth0 cpu:25",
                 "16430001 18:03:08:950000 tcp  recv in  tid 1 cpu 1 size 10 "
                 "192.168.219.103:55064 <- 192.168.100.195:31402",
                 "16360715 18:03:09:036286 netif_receive_skb: sip:192.168.100.195, "
                 "sport:31402 -> dip:192.168.219.103, dport:55064, seq:1316402115, "
                 "len:262, dev:enp38s0f0np0 cpu:193",
                 "16360719 18:03:09:036307 tcp  recv que tid 3913697 cpu 193 size 210 "
                 "tp_rcv_nxt:1316402115, 192.168.219.103:55064 <- 192.168.100.195:31402",
                 "16363100 18:03:09:042216 tcp  recv in  tid 3905867 cpu 255 size 4096 "
                 "192.168.219.103:55064 <- 192.168.100.195:31402, "
                 "copied_seq:1316402115, rcv_nxt:1316402325"]
                + ["1636%04d 18:03:09:03%04d tcp  recv in  tid 4 cpu 4 size 10 "
                   "192.168.219.103:55064 <- 192.168.100.195:31402" % (i, i)
                   for i in range(700, 780)]
                + ["16369999 18:03:09:900000 tcp  recv in  tid 9 cpu 9 size 10 "
                   "192.168.219.103:55064 <- 192.168.100.195:31402"])

    def test_full_scan_seq_prefix(self):
        path = self._write_bpf(self._seq_events())
        try:
            w = self._win(datetime(2026, 9, 15, 18, 3, 9, 30000),
                          datetime(2026, 9, 15, 18, 3, 9, 50000))
            sc = nla.BpfScanner(path, [w], full_scan=True)
            res, _ = sc.scan()
            evs = res[("A", "client")]
            kinds = [e["kind"] for e in evs]
            self.assertIn("nic_rx_skb", kinds)
            self.assertIn("tcp_recv_que", kinds)
            self.assertEqual(len(evs), 3 + 80)
            # 诊断 tod 范围取自序号行内的 tod（而非把序号当 tod）
            self.assertEqual(sc.diag["file_first_tod"], "18:03:08:900000")
        finally:
            os.unlink(path)

    def test_seek_equals_full_seq_prefix(self):
        path = self._write_bpf(self._seq_events())
        try:
            w = self._win(datetime(2026, 9, 15, 18, 3, 9, 30000),
                          datetime(2026, 9, 15, 18, 3, 9, 50000))
            full = nla.BpfScanner(path, [w], full_scan=True).scan()
            seek = nla.BpfScanner(path, [w], full_scan=False).scan()
            key = lambda r: {k: [(e["kind"], e["ts"].isoformat()) for e in v]
                             for k, v in r.items()}
            self.assertEqual(key(full[0]), key(seek[0]))
            self.assertEqual(len(seek[0][("A", "client")]), 3 + 80)
        finally:
            os.unlink(path)

    def test_seek_header_plus_seq_lines(self):
        # bpftrace 无时间戳头 + 序号行：头行不得触发 seek 提前 break
        lines = (["Tracing brpc_wkr & datasystem network events... (no IP filter)"]
                 + self._seq_events())
        path = self._write_bpf(lines)
        try:
            w = self._win(datetime(2026, 9, 15, 18, 3, 9, 30000),
                          datetime(2026, 9, 15, 18, 3, 9, 50000))
            for full in (False, True):
                scanner = nla.BpfScanner(path, [w], full_scan=full)
                res, _ = scanner.scan()
                kinds = [e["kind"] for e in res[("A", "client")]]
                self.assertIn("tcp_recv_que", kinds)
                self.assertEqual(scanner.diag["file_first_tod"],
                                 "18:03:08:900000")
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

    def test_identify_conn_from_server(self):
        """client 节点无 bpf：server 侧事件回退识别连接（收包优先 / 发送兜底）。"""
        CIP, SIP = "192.168.49.66", "192.168.210.192"
        sr = datetime(2026, 9, 4, 17, 46, 53, 999665)

        def ev(kind, ts, lport, pport, lip=SIP, pip=CIP):
            arrow = "->" if kind == "tcp_send_in" else "<-"
            line = ("17:46:54:000000 tcp  %s tid 1 cpu 1 size 100 %s:%s %s %s:%s\n"
                    % (kind.replace("tcp_", "").replace("_", " "),
                       lip, lport, arrow, pip, pport))
            return nla.parse_bpf_line(line, sr.date())

        # 1) 请求方向收包事件优先：取 ≥ ServerRecv−200us 中最早
        evs = [ev("tcp_recv_in", 0, 31402, 53896),
               ev("tcp_recv_in", 0, 31402, 53897),   # 另一条连接
               ev("tcp_send_in", 0, 31402, 53896)]
        # tcp recv in 两行 tod 相同，端口区分：模拟时间偏移不可行，改用 recv_que+recv_in
        evs[0] = ev("tcp_recv_que", 0, 31402, 53896)
        conn = nla.BpfCorrelator._identify_conn_from_server(evs, sr, CIP, SIP)
        self.assertEqual(conn, (CIP, 53896, SIP, 31402))
        # 2) 无收包事件（bpf 开始记录晚于收包）→ 响应方向 send 兜底
        evs2 = [ev("tcp_send_in", 0, 31402, 53896),
                ev("tcp_send_in", 0, 31402, 53897)]
        conn2 = nla.BpfCorrelator._identify_conn_from_server(evs2, sr, CIP, SIP)
        self.assertEqual(conn2, (CIP, 53896, SIP, 31402))
        # 3) 其他 client IP 的事件不参与
        evs3 = [ev("tcp_send_in", 0, 31402, 1111, pip="9.9.9.9")]
        self.assertIsNone(
            nla.BpfCorrelator._identify_conn_from_server(evs3, sr, CIP, SIP))

    def test_identify_conn_from_nic(self):
        """tcp 层探针丢失时：client 侧 nic 层事件（含完整五元组）兜底识别连接。"""
        CIP, SIP = "192.168.49.66", "192.168.210.192"
        cs = datetime(2026, 9, 4, 17, 46, 54, 100000)

        def nic(ts_us, sport, dport, sip=CIP, dip=SIP, kind="dev_start_xmit"):
            line = ("17:46:54:%06d %s: sip:%s, sport:%d -> dip:%s, dport:%d, "
                    "seq:913492323, len:339, dev:eth0\n" % (ts_us, kind, sip, sport,
                                                            dip, dport))
            return nla.parse_bpf_line(line, cs.date())

        # 1) 请求方向（cip→sip）dev_start_xmit ≥ ClientSend−200us 中最早；
        #    另一条连接（sport 不同）与响应方向 rx 事件不干扰
        evs = [nic(99000, 53895, 31402),                    # 容差（200us）前另一连接
               nic(100050, 53896, 31402),                   # 目标连接
               nic(100050, 53897, 31402, kind="net_dev_xmit"),  # 另一连接 xmit
               nic(103457, 53896, 31402, sip=SIP, dip=CIP,
                   kind="netif_receive_skb")]               # 响应方向 rx
        conn = nla.BpfCorrelator._identify_conn_from_nic(evs, cs, CIP, SIP)
        self.assertEqual(conn, (CIP, 53896, SIP, 31402))
        # 2) 无窗口后事件 → 全窗内取离 ClientSend 最近
        evs2 = [nic(98000, 53896, 31402)]
        conn2 = nla.BpfCorrelator._identify_conn_from_nic(evs2, cs, CIP, SIP)
        self.assertEqual(conn2, (CIP, 53896, SIP, 31402))
        # 3) 无请求方向 nic 事件 → None
        evs3 = [nic(100050, 53896, 31402, sip=SIP, dip=CIP,
                    kind="netif_receive_skb")]
        self.assertIsNone(
            nla.BpfCorrelator._identify_conn_from_nic(evs3, cs, CIP, SIP))

    def test_identify_conn_from_response(self):
        """请求方向事件全缺（UB 传输：bpf 仅观测响应方向数据）时：
        client 侧响应方向收包事件（tcp recv / nic rx）兜底识别连接。"""
        CIP, SIP = "192.168.219.103", "192.168.100.195"
        cr = datetime(2026, 9, 15, 18, 3, 9, 42342)

        def recv(ts_us, lport, pport, kind="recv que"):
            line = ("18:03:09:%06d tcp  %s tid 1 cpu 1 size 210 %s:%d <- %s:%d\n"
                    % (ts_us, kind, CIP, lport, SIP, pport))
            return nla.parse_bpf_line(line, cr.date())

        def rx(ts_us, dport, sport):
            line = ("18:03:09:%06d netif_receive_skb: sip:%s, sport:%d -> "
                    "dip:%s, dport:%d, seq:1316402115, len:262, "
                    "dev:enp38s0f0np0\n" % (ts_us, SIP, sport, CIP, dport))
            return nla.parse_bpf_line(line, cr.date())

        # 1) tcp recv（que/in）local=client peer=server：取离 ClientRecv 最近
        evs = [recv(36307, 55064, 31402),
               recv(36310, 55065, 31403),          # 另一条连接（远离 ClientRecv）
               recv(42216, 55064, 31402, kind="recv in")]
        conn = nla.BpfCorrelator._identify_conn_from_response(evs, cr, CIP, SIP)
        self.assertEqual(conn, (CIP, 55064, SIP, 31402))
        # 2) 无 tcp 事件 → nic rx（dst=client src=server）兜底
        evs2 = [rx(36286, 55064, 31402), rx(36200, 55065, 31403)]
        conn2 = nla.BpfCorrelator._identify_conn_from_response(evs2, cr, CIP, SIP)
        self.assertEqual(conn2, (CIP, 55064, SIP, 31402))
        # 3) 其他方向 / 其他 IP 的事件不参与 → None
        evs3 = [recv(42216, 55064, 31402, kind="recv in")]
        for bad_ip in ("9.9.9.9",):
            self.assertIsNone(nla.BpfCorrelator._identify_conn_from_response(
                evs3, cr, bad_ip, SIP))
        line = ("18:03:09:042216 tcp  send in  tid 1 cpu 1 size 210 "
                "%s:55064 -> %s:31402\n" % (CIP, SIP))
        evs4 = [nla.parse_bpf_line(line, cr.date())]
        self.assertIsNone(nla.BpfCorrelator._identify_conn_from_response(
            evs4, cr, CIP, SIP))

    def test_identify_conn_from_server_nic(self):
        """server 侧 tcp 探针丢失（UB/URMA 发送仅有 nic 层事件）时：
        server 侧响应方向 nic 发送事件兜底识别连接。"""
        CIP, SIP = "192.168.219.103", "192.168.100.195"
        ss = datetime(2026, 9, 15, 18, 3, 9, 29139)

        def xmit(ts_us, sport, dport, sip=SIP, dip=CIP, kind="dev_start_xmit"):
            line = ("18:03:09:%06d %s: sip:%s, sport:%d -> dip:%s, dport:%d, "
                    "seq:1316402115, len:276, dev:eth0\n"
                    % (ts_us, kind, sip, sport, dip, dport))
            return nla.parse_bpf_line(line, ss.date())

        # 1) 响应方向（server→client）xmit：取离 ServerSend 最近
        evs = [xmit(28800, 31403, 55065),          # 另一条连接（远离 ServerSend）
               xmit(28931, 31402, 55064),
               xmit(28935, 31402, 55064, kind="net_dev_xmit")]
        conn = nla.BpfCorrelator._identify_conn_from_server_nic(
            evs, ss, CIP, SIP)
        self.assertEqual(conn, (CIP, 55064, SIP, 31402))
        # 2) 请求方向（client→server）xmit 不参与 → None
        evs2 = [xmit(28931, 55064, 31402, sip=CIP, dip=SIP)]
        self.assertIsNone(nla.BpfCorrelator._identify_conn_from_server_nic(
            evs2, ss, CIP, SIP))


class TestKernelSegments(unittest.TestCase):
    def test_segments_and_flags(self):
        rec = nla.SlowRecord("t", DAY, {"server_req_queue_us": "10", "server_exec_us": "240"},
                             "x.log", "pod")
        ctx = nla.TraceContext(rec)
        ctx.milestones = {
            "ClientSend": datetime(2026, 8, 21, 21, 31, 21, 60757),
            "ClientTcpSendIn": datetime(2026, 8, 21, 21, 31, 21, 60777),
            "ServerTcpRecvQue": datetime(2026, 8, 21, 21, 31, 21, 60821),
            "ServerTcpRecvFirst": datetime(2026, 8, 21, 21, 31, 21, 60822),
            "ServerTcpRecvLast": datetime(2026, 8, 21, 21, 31, 21, 60827),
            "ServerRecv": datetime(2026, 8, 21, 21, 31, 21, 60848),
            "ServerSend": datetime(2026, 8, 21, 21, 31, 21, 61096),
            "ServerTcpSendIn": datetime(2026, 8, 21, 21, 31, 21, 61100),
            "ClientTcpRecvQue": datetime(2026, 8, 21, 21, 31, 21, 61193),
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
        self.assertEqual(data["schema_version"], 2)
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
                                      "server_ip": "192.168.102.161", "server_port": 31501,
                                      "source": "client_tcp"})
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
            fake_disc.aux_stats = {}
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
        s_idx, s_lines, _ = nla.collect_anchor_and_info(paths, [], traces, workers=1)
        p_idx, p_lines, _ = nla.collect_anchor_and_info(paths, [], traces, workers=3)
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
            idx, lines, _hp = nla.collect_anchor_and_info([clog], [], [self.TRACE_A])
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


class TestRecvMilestoneRequestWindow(unittest.TestCase):
    """收包里程碑请求时间窗：长连接同五元组多次请求交互时，
    TcpRecvFirst/Last/Que/SockReadable 限定在本请求锚点区间内——
    相邻请求的同连接 recv 事件不再污染（典型：本请求 ClientRecv 之后
    下一请求的 recv_in 把 ClientTcpRecvLast 推后，慢段时间窗起点后移，
    真正慢的 seq 事件被挤出窗口）。"""

    CIP, CPORT = "192.168.32.61", 39776
    SIP, SPORT = "192.168.52.197", 31501
    T0 = datetime(2026, 8, 23, 20, 45, 39)

    def _ev(self, kind, us, side="Client"):
        """同连接事件（local=client 端口对 → Client 侧；local=server → Server 侧）。"""
        if side == "Client":
            lip, lport, pip, pport = self.CIP, self.CPORT, self.SIP, self.SPORT
        else:
            lip, lport, pip, pport = self.SIP, self.SPORT, self.CIP, self.CPORT
        return {"kind": kind, "ts": self.T0.replace(microsecond=us),
                "cpu": 1, "tid": 7, "local_ip": lip, "local_port": lport,
                "peer_ip": pip, "peer_port": pport, "raw": "r"}

    def _ms(self, **kw):
        ms = {"ClientSend": self.T0.replace(microsecond=100000),
              "ClientRecv": self.T0.replace(microsecond=200000)}
        for k, us in kw.items():
            ms[k] = self.T0.replace(microsecond=us)
        return ms

    def _fill(self, ms, evs, side):
        nla.BpfCorrelator._fill_milestones_side(
            ms, evs, side, self.CIP, self.CPORT, self.SIP, self.SPORT)
        return ms

    def test_client_recv_last_not_polluted_by_next_request(self):
        # 本请求响应 .150/.180 收包；下一请求响应 .400（ClientRecv 后
        # 200ms，超 50ms 容差）→ 不得更新 ClientTcpRecvLast
        ms = self._ms()
        self._fill(ms, [self._ev("tcp_recv_in", 150000),
                        self._ev("tcp_recv_in", 180000),
                        self._ev("tcp_recv_in", 400000)], "Client")
        self.assertEqual(ms["ClientTcpRecvFirst"].microsecond, 150000)
        self.assertEqual(ms["ClientTcpRecvLast"].microsecond, 180000)

    def test_client_recv_first_not_polluted_by_prev_request(self):
        # 上一请求响应 .010（ClientSend 前 90ms，超 50ms 容差）→ 不得定格 First
        ms = self._ms()
        self._fill(ms, [self._ev("tcp_recv_in", 10000),
                        self._ev("tcp_recv_in", 150000),
                        self._ev("tcp_recv_in", 180000)], "Client")
        self.assertEqual(ms["ClientTcpRecvFirst"].microsecond, 150000)
        self.assertEqual(ms["ClientTcpRecvLast"].microsecond, 180000)

    def test_client_window_tolerance(self):
        # 容差边界：ClientSend−40ms / ClientRecv+40ms 计入；
        # ClientSend−60ms / ClientRecv+70ms 排除
        ms = self._ms()
        self._fill(ms, [self._ev("tcp_recv_in", 40000),
                        self._ev("tcp_recv_in", 60000),
                        self._ev("tcp_recv_in", 240000),
                        self._ev("tcp_recv_in", 270000)], "Client")
        self.assertEqual(ms["ClientTcpRecvFirst"].microsecond, 60000)
        self.assertEqual(ms["ClientTcpRecvLast"].microsecond, 240000)

    def test_server_recv_window_bounded_by_server_anchors(self):
        # server 侧窗口上界 = max(ServerRecv, ServerSend)：本请求 .120/.145
        # 收包；下一请求 .500（ServerSend 后 320ms）→ 排除
        ms = self._ms(ServerRecv=150000, ServerSend=180000)
        self._fill(ms, [self._ev("tcp_recv_in", 120000, "Server"),
                        self._ev("tcp_recv_in", 145000, "Server"),
                        self._ev("tcp_recv_in", 500000, "Server")], "Server")
        self.assertEqual(ms["ServerTcpRecvFirst"].microsecond, 120000)
        self.assertEqual(ms["ServerTcpRecvLast"].microsecond, 145000)

    def test_no_anchors_no_window(self):
        # 锚点缺失（如 client_only 补扫场景）→ 不加窗，保持旧全量行为
        ms = {}
        self._fill(ms, [self._ev("tcp_recv_in", 10000),
                        self._ev("tcp_recv_in", 500000)], "Client")
        self.assertEqual(ms["ClientTcpRecvFirst"].microsecond, 10000)
        self.assertEqual(ms["ClientTcpRecvLast"].microsecond, 500000)

    def test_all_outside_window_falls_back(self):
        # 全部事件在窗外（锚点偏差超容差的极端场景）→ 回退旧全量行为，
        # 避免收包里程碑整体丢失
        ms = self._ms()
        self._fill(ms, [self._ev("tcp_recv_in", 900000)], "Client")
        self.assertEqual(ms["ClientTcpRecvFirst"].microsecond, 900000)
        self.assertEqual(ms["ClientTcpRecvLast"].microsecond, 900000)

    def test_recv_que_sock_readable_bounded(self):
        # 收包类里程碑（Que/SockReadable）同样限定请求窗口
        ms = self._ms()
        self._fill(ms, [self._ev("tcp_recv_que", 10000),
                        self._ev("sock_readable", 12000),
                        self._ev("tcp_recv_que", 150000),
                        self._ev("sock_readable", 155000)], "Client")
        self.assertEqual(ms["ClientTcpRecvQue"].microsecond, 150000)
        self.assertEqual(ms["ClientSockReadable"].microsecond, 155000)

    def test_send_milestone_unaffected(self):
        # 发送类里程碑（TcpSendIn）不加窗：维持原有 first 语义
        ms = self._ms()
        self._fill(ms, [self._ev("tcp_send_in", 10000),
                        self._ev("tcp_send_in", 150000)], "Client")
        self.assertEqual(ms["ClientTcpSendIn"].microsecond, 10000)


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

    def test_server_egress_segments_built(self):
        """server 发送侧证据段对称补全：stack_to_nic（qdisc）/ nic_xmit（驱动）。"""
        ms = {
            "ServerTcpSendIn": datetime(2026, 8, 23, 20, 45, 39, 700100),
            "ServerDevStartXmit": datetime(2026, 8, 23, 20, 45, 39, 700900),
            "ServerNetDevXmit": datetime(2026, 8, 23, 20, 45, 39, 700905),
        }
        ctx = self._ctx(milestones=ms)
        nla._nic_segments(ctx)
        seg = self._seg(ctx, "server_stack_to_nic")
        self.assertIsNotNone(seg)
        self.assertAlmostEqual(seg["dur_us"], 800)
        self.assertTrue(seg["evidence"])
        self.assertEqual(seg["start"], "ServerTcpSendIn")
        self.assertEqual(seg["end"], "ServerDevStartXmit")
        xmit = self._seg(ctx, "server_nic_xmit")
        self.assertIsNotNone(xmit)
        self.assertAlmostEqual(xmit["dur_us"], 5)
        self.assertEqual(xmit["start"], "ServerDevStartXmit")

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


class TestProblemSeqHighlight(unittest.TestCase):
    """问题包序号高亮：问题连接事件的 seq 族序号在事件表中红色标注。

    用户需求：出问题的 seq 号换个颜色/高亮标识——同一问题包在
    dev_start_xmit / netif_receive_skb / tcp recv que/in 的
    seq / tp_rcv_nxt / copied_seq / rcv_nxt 等字段中可追踪。
    """

    DAY = datetime(2026, 9, 15)

    def _nic(self, seq=1316402115):
        return nla.parse_bpf_line(
            "16438196 18:03:09:028931 dev_start_xmit: sip:192.168.100.195, "
            "sport:31402 -> dip:192.168.219.103, dport:55064, seq:%d, "
            "len:276, dev:enp38s0f0np0 cpu:25" % seq, self.DAY)

    def _recv_que(self, rcv_nxt=1316402115):
        return nla.parse_bpf_line(
            "16360719 18:03:09:036307 tcp  recv que tid 3913697 cpu 193 "
            "size 210 tp_rcv_nxt:%d, 192.168.219.103:55064 <- "
            "192.168.100.195:31402" % rcv_nxt, self.DAY)

    def _recv_in(self, copied=1316402115, rcv_nxt=1316402325):
        return nla.parse_bpf_line(
            "16363100 18:03:09:042216 tcp  recv in  tid 3905867 cpu 255 "
            "size 4096 192.168.219.103:55064 <- 192.168.100.195:31402, "
            "copied_seq:%d, rcv_nxt:%d" % (copied, rcv_nxt), self.DAY)

    def test_row_html_highlights_problem_seq(self):
        ev = self._nic()
        row = nla._event_row_html(ev, hl_seqs={1316402115})
        self.assertIn('<span class="seqhl">1316402115</span>', row)
        # 其他 seq（别的包）不高亮
        ev2 = self._nic(seq=999)
        row2 = nla._event_row_html(ev2, hl_seqs={1316402115})
        self.assertNotIn("seqhl", row2)

    def test_row_html_highlights_tcp_seq_fields(self):
        # tcp recv que（rcv_nxt）与 recv in（copied_seq/rcv_nxt）
        row = nla._event_row_html(self._recv_que(), hl_seqs={1316402115})
        self.assertIn('<span class="seqhl">1316402115</span>', row)
        row = nla._event_row_html(self._recv_in(), hl_seqs={1316402115})
        self.assertIn('copied_seq:<span class="seqhl">1316402115</span>', row)
        self.assertIn('rcv_nxt:<span class="seqhl">1316402325</span>', row)

    def test_row_html_no_seqs_unchanged(self):
        # 不传 hl_seqs（默认）时输出与旧格式完全一致
        ev = self._nic()
        row = nla._event_row_html(ev)
        self.assertIn("seq=1316402115", row)
        self.assertNotIn("seqhl", row)

    def test_events_table_and_window_table_pass_seqs(self):
        evs = [self._nic(), self._recv_que()]
        out = nla._events_table(evs, "t", hl_seqs={1316402115})
        self.assertEqual(out.count('<span class="seqhl">1316402115</span>'), 2)
        evs[0]["match5t"] = True
        evs[1]["match5t"] = False
        out2 = nla._window_events_table(evs, "t", hl_seqs={1316402115})
        self.assertEqual(out2.count('<span class="seqhl">1316402115</span>'), 2)

    def test_collect_problem_seqs(self):
        CIP, CPORT, SIP, SPORT = "192.168.219.103", 55064, "192.168.100.195", 31402
        mine = [self._nic(), self._recv_que(), self._recv_in()]
        for e in mine:
            e["match5t"] = True
        other = self._nic(seq=777)
        other["match5t"] = False
        seqs = nla._collect_problem_seqs(
            mine + [other], CIP, CPORT, SIP, SPORT)
        self.assertIn(1316402115, seqs)     # nic seq / tp_rcv_nxt / copied_seq
        self.assertIn(1316402325, seqs)     # rcv_nxt
        self.assertNotIn(777, seqs)         # 其他连接不参与

    def test_row_html_tp_rcv_nxt_displayed(self):
        # que 行序号字段显式展示为 tp_rcv_nxt（不再误显示为 rcv_nxt）
        ev = self._recv_que()
        row = nla._event_row_html(ev)
        self.assertIn("tp_rcv_nxt:1316402115", row)
        self.assertNotIn(" rcv_nxt:", row)
        # 命中问题序号 → tp_rcv_nxt 红色高亮
        row2 = nla._event_row_html(ev, hl_seqs={1316402115})
        self.assertIn('tp_rcv_nxt:<span class="seqhl">1316402115</span>', row2)

    def test_event_json_tp_rcv_nxt(self):
        ev = self._recv_que()
        d = nla._event_json(ev)
        self.assertEqual(d["tp_rcv_nxt"], 1316402115)
        self.assertNotIn("rcv_nxt", d)


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
        # HTML 摘要卡（全量交互探索收敛到独立 os_monitor 报告）
        rep = nla.generate_report(contexts, ns, str(self._root),
                                  aux_stats=disc.aux_stats)
        self.assertIn("关中断", rep)
        self.assertIn("网卡利用率", rep)
        self.assertIn("os_monitor_report.html", rep)
        osr = nla.generate_os_monitor_report(disc.aux_stats, str(self._root))
        self.assertIn("kubelet", osr)
        self.assertIn("关中断统计", osr)
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
        # JSON：cpu_busy 结构（schema v2 摘要 + 归属统计）
        doc = json.loads(nla.generate_json(contexts, ns, str(root),
                                           aux_stats=disc.aux_stats))
        cb = doc["traces"][0]["cpu_busy"]["client"]
        self.assertEqual(cb["anchor_cpu"], 50)
        self.assertTrue(cb["preempt"])
        self.assertEqual(cb["n_mine"], 2)
        self.assertEqual(cb["n_other"], 2)
        # window_events → {n, first_ts, last_ts} 摘要（明细按 ts 对齐 kernel_events）
        we = cb["window_events"]
        self.assertEqual(we["n"], 4)
        self.assertTrue(we["first_ts"])
        self.assertTrue(we["last_ts"])
        # other_on_cpu → {n, ts_list}
        self.assertEqual(cb["other_on_cpu"]["n"], 2)
        self.assertEqual(len(cb["other_on_cpu"]["ts_list"]), 2)
        self.assertIn("192.168.219.200:40000", "".join(cb["other_conns"]))
        # raw：全景节 + 问题五元组 ▶ 标注
        raw = nla.generate_raw(contexts, ns, str(root), disc, trace_lines)
        self.assertIn("问题窗口 bpf 事件全景", raw)
        self.assertIn("▶ 21:31:21:064900 netif_receive_skb", raw)
        self.assertIn("  21:31:21:065500 tcp  recv in", raw)


class TestSoftirqParse(unittest.TestCase):
    """softirq 探针事件行解析：high irq-to-softirq（raise→entry 慢）/
    slow softirq!（entry→exit 慢）。"""

    def test_parse_raise_delay(self):
        ev = nla.parse_bpf_line(
            "21:31:21:065200 high irq-to-softirq  vec=3 latency: 1500 usec (1 ms) "
            "on CPU:50 comm:kvclient kstack:__do_softirq+0x1\n",
            datetime(2026, 8, 21))
        self.assertIsNotNone(ev)
        self.assertEqual(ev["kind"], "softirq_raise_delay")
        self.assertEqual(ev["vec"], 3)
        self.assertEqual(ev["latency_us"], 1500)
        self.assertEqual(ev["cpu"], 50)
        self.assertEqual(ev["comm"], "kvclient")
        self.assertIn("__do_softirq", ev["kstack"])

    def test_parse_exit_delay(self):
        ev = nla.parse_bpf_line(
            "21:31:21:066800 slow softirq! cpu: 50   | Type: 3 | Latency: 2300    us, "
            "timercnt:5/1\n",
            datetime(2026, 8, 21))
        self.assertIsNotNone(ev)
        self.assertEqual(ev["kind"], "softirq_exit_delay")
        self.assertEqual(ev["cpu"], 50)
        self.assertEqual(ev["vec"], 3)
        self.assertEqual(ev["latency_us"], 2300)
        self.assertEqual(ev["timer_cnt"], 5)
        self.assertEqual(ev["timer_large_cnt"], 1)

    def test_vec_label(self):
        self.assertEqual(nla.SOFTIRQ_VEC_LABELS[3], "NET_RX")


class TestSoftirqEndToEnd(TestCpuBusyEndToEnd):
    """收包慢（kernel_to_user 段异常）结合 softirq 探针信息定界。

    - softirq_exit_delay（entry→exit >1ms）出现在业务 cpu 上：
      软中断本身处理太慢，期间业务线程无法运行 → 抢占证据 + 高置信；
    - softirq_raise_delay（raise→entry >1ms）出现在业务 cpu 上：
      软中断发起后被其他任务抢占/延迟，收包协议栈处理被推迟。
    """

    def _build_with_softirq(self, raise_cpu=None, exit_cpu=None):
        root = self._build(other_cpu=99)  # 其他连接放非业务 cpu，隔离抢占因素
        bpf = root / "dscollect_log" / "bpf-master-192.168.219.1.log"
        lines = []
        if raise_cpu is not None:
            lines.append(
                "21:31:21:065200 high irq-to-softirq  vec=3 latency: 1500 usec "
                "(1 ms) on CPU:%d comm:kvclient kstack:__do_softirq+0x1\n"
                % raise_cpu)
        if exit_cpu is not None:
            lines.append(
                "21:31:21:066800 slow softirq! cpu: %d   | Type: 3 | "
                "Latency: 2300    us, timercnt:5/1\n" % exit_cpu)
        # 追加后按时间重排，保持 bpf 文件时间有序（seek 模式前提）
        lines = sorted(bpf.read_text().splitlines(True) + lines)
        bpf.write_text("".join(lines), encoding="utf-8")
        return root

    def test_softirq_exit_slow_is_preempt_evidence(self):
        root = self._build_with_softirq(exit_cpu=50)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        self.assertEqual(ctx.conclusion["category"], "client_kernel_to_user_delay")
        info = ctx.cpu_busy["client"]
        # 软中断处理慢（2300us > 1ms）在业务 cpu 50 上 → 抢占证据 + 高置信
        self.assertEqual(len(info["softirq_exit_on_cpu"]), 1)
        self.assertEqual(info["softirq_exit_on_cpu"][0]["latency_us"], 2300)
        self.assertTrue(info["preempt"])
        self.assertTrue(ctx.cpu_busy_preempt)
        joined = " | ".join(ctx.cpu_evidence)
        self.assertIn("软中断本身处理", joined)
        self.assertIn("2300", joined)
        self.assertIn("NET_RX", joined)
        joined_ev = " | ".join(ctx.conclusion["evidence"])
        self.assertIn("◎", joined_ev)
        self.assertEqual(ctx.conclusion["confidence"], "高")

    def test_softirq_raise_delay_evidence(self):
        root = self._build_with_softirq(raise_cpu=50)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        info = ctx.cpu_busy["client"]
        self.assertEqual(len(info["softirq_raise_on_cpu"]), 1)
        joined = " | ".join(ctx.cpu_evidence)
        self.assertIn("软中断发起后", joined)
        self.assertIn("1500", joined)
        self.assertIn("被其他任务抢占", joined)
        # raise→entry 慢说明软中断被延迟（非业务线程被软中断占用），不算 preempt
        self.assertFalse(info["preempt"])
        self.assertEqual(ctx.conclusion["confidence"], "中")

    def test_softirq_on_other_cpu_no_evidence(self):
        root = self._build_with_softirq(raise_cpu=77, exit_cpu=77)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        info = ctx.cpu_busy["client"]
        self.assertEqual(info["softirq_raise_on_cpu"], [])
        self.assertEqual(info["softirq_exit_on_cpu"], [])
        self.assertFalse(info["preempt"])
        joined = " | ".join(ctx.cpu_evidence)
        self.assertNotIn("软中断本身处理", joined)
        self.assertNotIn("软中断发起后", joined)
        # 窗口内 softirq 事件仍进全景表（match5t=None，不限 cpu）
        self.assertTrue(any(e["kind"] == "softirq_exit_delay"
                            for e in ctx.bpf_window_events["client"]))

    def test_softirq_render_json_raw(self):
        import argparse
        root = self._build_with_softirq(raise_cpu=50, exit_cpu=50)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        disc, contexts, trace_lines = nla.analyze(str(root))
        ctx = contexts[0]
        ns = argparse.Namespace(residual_threshold=1000)
        # HTML：全景表含 softirq 行（事件名 + vec/latency 附加信息）+ 摘要结论
        h = nla._trace_html(ctx, 1)
        self.assertIn("softirq_exit_delay", h)
        self.assertIn("softirq_raise_delay", h)
        self.assertIn("NET_RX", h)
        self.assertIn("2300", h)
        self.assertIn("软中断本身处理", h)
        # JSON：cpu_busy.softirq_* 摘要（schema v2：顶层 softirq_events 已删，
        # 明细在 kernel_events 全量含 raw）
        doc = json.loads(nla.generate_json(contexts, ns, str(root),
                                           aux_stats=disc.aux_stats))
        tr = doc["traces"][0]
        cb = tr["cpu_busy"]["client"]
        self.assertEqual(cb["softirq_exit_on_cpu"]["n"], 1)
        self.assertNotIn("softirq_events", tr)
        kinds = {e["kind"] for e in tr["kernel_events"]["client"]}
        self.assertTrue({"softirq_raise_delay", "softirq_exit_delay"} <= kinds)
        ev = next(e for e in tr["kernel_events"]["client"]
                  if e["kind"] == "softirq_exit_delay")
        self.assertEqual(ev["vec"], 3)
        self.assertEqual(ev["latency_us"], 2300)
        self.assertEqual(ev["timer_cnt"], 5)
        self.assertTrue(ev["raw"])
        # raw：softirq 原始行进全景节
        raw = nla.generate_raw(contexts, ns, str(root), disc, trace_lines)
        self.assertIn("high irq-to-softirq", raw)
        self.assertIn("slow softirq!", raw)


class TestSoftirqLocalization(TestCpuBusyEndToEnd):
    """收包慢 softirq 定位：收包时间往前推，业务 cpu（或其 SMT 姊妹核）上的
    raise→entry 延迟事件 → 直接定位到占用 cpu 的任务（comm + 完整调用栈）。

    raise 事件放在 bpf 扫描窗口（cs−2ms）之外、收包点（NetifRx=064900）前
    ~8ms → 只有 softirq 回溯扫描（50ms lookback）能抓到。
    kstack 为 bpf 日志中 raise 行之后的无时间戳续行（symbol+offset）。
    """

    KSTACK_FRAMES = [
        "        handle_softirqs+744",
        "        __do_softirq+28",
        "        ____do_softirq+24",
        "        call_on_irq_stack+48",
        "        do_softirq_own_stack+36",
        "        __local_bh_enable_ip+164",
        "        ubase_send_cmd+496",
        "        ubase_cmd_send_inout_real+248",
        "        ubctl_ubase_cmd_send+136",
        "        ubctl_query_data+332",
        "        ubctl_query_dl_pkt_stats_data+96",
        "        ub_cmd_do+292",
        "        ubctl_fw_rpc+388",
        "        fwctl_cmd_rpc+292",
        "        fwctl_fops_ioctl+356",
        "        __arm64_sys_ioctl+180",
        "        invoke_syscall+80",
    ]

    def _build_localization(self, ev_cpu=50, ts_us=57000, vec=3):
        """raise 事件 ts=ts_us（默认 057000，NetifRx 前 7.9ms，bpf 扫描窗口外）。"""
        root = self._build(other_cpu=99)  # 其他连接放远 cpu，隔离抢占因素
        bpf = root / "dscollect_log" / "bpf-master-192.168.219.1.log"
        raise_line = ("21:31:21:%06d high irq-to-softirq  vec=%d latency: 5044 "
                      "usec (5 ms) on CPU:%d comm:ubctl kstack:\n"
                      % (ts_us, vec, ev_cpu))
        lines = sorted(bpf.read_text().splitlines(True) + [raise_line])
        out = []
        for ln in lines:
            out.append(ln)
            if "irq-to-softirq" in ln:  # kstack 续行紧跟 raise 行
                out.extend(f + "\n" for f in self.KSTACK_FRAMES)
        bpf.write_text("".join(out), encoding="utf-8")
        return root

    def test_localization_same_cpu(self):
        root = self._build_localization(ev_cpu=50)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        loc = ctx.softirq_localization.get("client")
        self.assertIsNotNone(loc)
        self.assertEqual(loc["comm"], "ubctl")
        self.assertEqual(loc["latency_us"], 5044)
        self.assertEqual(loc["vec"], 3)
        self.assertEqual(loc["cpu"], 50)
        self.assertFalse(loc["smt"])
        # kstack 完整解析（多行续行，帧数一致，含关键帧）
        frames = loc["kstack"].split("\n")
        self.assertEqual(len(frames), len(self.KSTACK_FRAMES))
        self.assertIn("ubctl_query_dl_pkt_stats_data", loc["kstack"])
        self.assertIn("__arm64_sys_ioctl", loc["kstack"])
        # 定位结论计入抢占证据 → 高置信
        self.assertTrue(ctx.cpu_busy_preempt)
        self.assertTrue(ctx.cpu_busy["client"]["preempt"])
        self.assertEqual(ctx.conclusion["confidence"], "高")
        joined = " | ".join(ctx.cpu_evidence)
        self.assertIn("定位", joined)
        self.assertIn("ubctl", joined)
        self.assertIn("5044", joined)
        self.assertIn("ubctl_query_dl_pkt_stats_data", joined)
        # 建议给出下一步方向（分析该任务为何执行）
        self.assertTrue(any("ubctl" in s for s in ctx.conclusion["suggestions"]))

    def test_localization_smt_sibling(self):
        # cpu 51 与业务 cpu 50 互为 SMT 姊妹核（相邻配对）→ 同样定位
        root = self._build_localization(ev_cpu=51)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        loc = ctx.softirq_localization.get("client")
        self.assertIsNotNone(loc)
        self.assertTrue(loc["smt"])
        self.assertEqual(loc["cpu"], 51)
        self.assertEqual(loc["anchor_cpu"], 50)
        joined = " | ".join(ctx.cpu_evidence)
        self.assertIn("SMT", joined)
        self.assertTrue(ctx.cpu_busy_preempt)

    def test_localization_negative_far_cpu(self):
        # cpu 77 既非业务 cpu 也非其 SMT 姊妹核 → 不定位
        root = self._build_localization(ev_cpu=77)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        self.assertNotIn("client", ctx.softirq_localization)
        self.assertFalse(ctx.cpu_busy_preempt)

    def test_localization_negative_after_recv(self):
        # raise 事件在收包点之后（065200 > NetifRx 064900）→ 不定位
        #（窗口内，走既有证据 3 路径，不算抢占）
        root = self._build_localization(ev_cpu=50, ts_us=65200)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        self.assertNotIn("client", ctx.softirq_localization)
        self.assertFalse(ctx.cpu_busy["client"]["preempt"])
        self.assertEqual(ctx.conclusion["confidence"], "中")

    def test_localization_render_json_raw(self):
        import argparse
        root = self._build_localization(ev_cpu=50)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        disc, contexts, trace_lines = nla.analyze(str(root))
        ctx = contexts[0]
        ns = argparse.Namespace(residual_threshold=1000)
        # HTML：定位结论块 + 完整 kstack（<pre>，非截断）
        h = nla._trace_html(ctx, 1)
        self.assertIn("定位结论", h)
        self.assertIn("ubctl", h)
        self.assertIn("fwctl_fops_ioctl", h)
        self.assertIn("<pre", h)
        # JSON：cpu_busy.client.softirq_localization（comm/kstack/vec/smt）
        doc = json.loads(nla.generate_json(contexts, ns, str(root),
                                           aux_stats=disc.aux_stats))
        loc = doc["traces"][0]["cpu_busy"]["client"]["softirq_localization"]
        self.assertEqual(loc["comm"], "ubctl")
        self.assertEqual(loc["latency_us"], 5044)
        self.assertEqual(loc["vec_txt"], "3(NET_RX)")
        self.assertFalse(loc["smt"])
        self.assertIn("ubctl_query_dl_pkt_stats_data", loc["kstack"])
        # raw：定位结论 + raise 原始行（含 kstack 续行）
        raw = nla.generate_raw(contexts, ns, str(root), disc, trace_lines)
        self.assertIn("定位", raw)
        self.assertIn("irq-to-softirq", raw)
        self.assertIn("ubctl_query_dl_pkt_stats_data", raw)


class TestSoftirqWireLocalization(TestCpuBusyEndToEnd):
    """接收侧线路段慢（wire_s2c_phys）的 softirq 定位（wire 模式）。

    server 物理网卡 .064815 发出 → client 收包点 .070000（5185us）：
    包到达后 NET_RX 软中断被任务占用（raise .064946 → entry .069990，
    延迟 5044us，cpu 44），netif 收包推迟到 entry 之后 → "线路慢"实为
    收包软中断被抢占。client kernel_to_user 段不异常（tcp recv .070100 →
    ClientRecv .070200 = 100us）→ 只有 wire 回溯定位能命中。
    """

    KSTACK_FRAMES = [
        "        handle_softirqs+744",
        "        __do_softirq+28",
        "        __local_bh_enable_ip+164",
        "        ubase_send_cmd+496",
        "        ubctl_query_data+332",
        "        ubctl_query_dl_pkt_stats_data+96",
        "        fwctl_fops_ioctl+356",
        "        __arm64_sys_ioctl+180",
    ]

    def _build_wire(self, ev_cpu=44, entry_us=69990, lat_us=5044, vec=3,
                    xmit_us=64815):
        """raise 事件 entry=entry_us（默认 .069990，收包点 .070000 前 10us）；
        server 物理网卡发出=xmit_us（默认 .064815 → wire 段 5185us）。"""
        root = Path(tempfile.mkdtemp(prefix="softirq_wire_"))
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
            + info("2026-08-21T21:31:21.070200", self.CIP,
                   "yyl9 ClientRecv ts 88035221862010 tid 523 cpu 50")
            + info("2026-08-21T21:31:21.070213", self.CIP, self._slow()),
            encoding="utf-8")
        (wdir / "kvcache.INFO.1.log").write_text(
            info("2026-08-21T21:31:21.060950", self.SIP,
                 "yyl3 ServerRecv ts 88038917594514 tid 275")
            + info("2026-08-21T21:31:21.064740", self.SIP,
                   "yyl10 ServerSend ts 88038917846674 tid 275"),
            encoding="utf-8")

        raise_block = [("21:31:21:%06d high irq-to-softirq  vec=%d latency: %d "
                        "usec (5 ms) on CPU:%d comm:ubctl kstack:\n"
                        % (entry_us, vec, lat_us, ev_cpu))]
        raise_block += [f + "\n" for f in self.KSTACK_FRAMES]
        client_bpf = (
            ["21:31:21:060770 tcp  send in  tid 523 cpu 50 size 270 "
             "%s:37880 -> %s:31501\n" % (self.CIP, self.SIP)]
            + raise_block
            + ["21:31:21:070000 netif_receive_skb: sip:%s, sport:31501 -> dip:%s, "
               "dport:37880, seq:2222, len:120, dev:enp38s0f0np0\n"
               % (self.SIP, self.CIP),
               "21:31:21:070100 tcp  recv in  tid 479193 cpu 332 size 120 "
               "%s:37880 <- %s:31501, copied_seq:358067377, rcv_nxt:358067377\n"
               % (self.CIP, self.SIP)])
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
              "%s:31501 -> %s:37880\n" % (self.SIP, self.CIP)
            + "21:31:21:%06d net_dev_xmit: sip:%s, sport:31501 -> dip:%s, "
              "dport:37880, seq:2222, len:120, dev:enp38s0f0np0, rc:0\n"
              % (xmit_us, self.SIP, self.CIP),
            encoding="utf-8")
        (ldir / "master_192.168.219.1").write_text("", encoding="utf-8")
        (ldir / "worker1_192.168.102.1").write_text("", encoding="utf-8")
        return root

    def test_wire_localization(self):
        root = self._build_wire()
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        loc = ctx.softirq_localization.get("client")
        self.assertIsNotNone(loc)
        self.assertEqual(loc["mode"], "wire")
        self.assertEqual(loc["wire_key"], "wire_s2c_phys")
        self.assertAlmostEqual(loc["wire_dur_us"], 5185, delta=5)
        self.assertEqual(loc["comm"], "ubctl")
        self.assertEqual(loc["cpu"], 44)
        self.assertEqual(loc["latency_us"], 5044)
        self.assertEqual(loc["vec_txt"], "3(NET_RX)")
        self.assertFalse(loc["smt"])
        self.assertEqual(loc["n_candidates"], 1)
        self.assertIn("ubctl_query_dl_pkt_stats_data", loc["kstack"])
        # wire 模式下 kernel_to_user 段不异常 → cpu_busy 为最小信息（wire 段窗口）
        info = ctx.cpu_busy["client"]
        self.assertEqual(info["seg_key"], "wire_s2c_phys")
        self.assertTrue(info["preempt"])
        self.assertTrue(ctx.cpu_busy_preempt)
        joined = " | ".join(ctx.cpu_evidence)
        self.assertIn("【已定位】", joined)
        self.assertIn("ubctl", joined)
        self.assertIn("5044", joined)
        # 结论建议指向占用任务
        self.assertTrue(any("ubctl" in s for s in ctx.conclusion["suggestions"]))
        # 传输类 + 网卡点位佐证 → 高置信
        self.assertIn("s2c", ctx.conclusion["category"])
        self.assertEqual(ctx.conclusion["confidence"], "高")

    def test_wire_negative_non_netrx(self):
        # 非 NET_RX（vec=7）不定位：收包软中断为 vec=3
        root = self._build_wire(vec=7)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        self.assertNotIn("client", ctx.softirq_localization)
        self.assertFalse(ctx.cpu_busy_preempt)

    def test_wire_negative_raise_before_xmit(self):
        # raise 时间（entry .065000 − 6000us = .064400）早于 server 物理网卡
        # 发出 .064815 → 更早一批包的软中断，非本次收包 → 不定位
        root = self._build_wire(entry_us=65000, lat_us=6000)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        self.assertNotIn("client", ctx.softirq_localization)

    def test_wire_negative_short_wire(self):
        # 线路段仅 185us（< 1000us）→ 未达 wire 定位门槛
        root = self._build_wire(xmit_us=69815)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        self.assertNotIn("client", ctx.softirq_localization)

    def test_wire_render_json_raw(self):
        import argparse
        root = self._build_wire()
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        disc, contexts, trace_lines = nla.analyze(str(root))
        ctx = contexts[0]
        ns = argparse.Namespace(residual_threshold=1000)
        # HTML：定位结论块（wire 模式）+ 完整 kstack（<pre>）
        h = nla._trace_html(ctx, 1)
        self.assertIn("定位结论", h)
        self.assertIn("ubctl", h)
        self.assertIn("fwctl_fops_ioctl", h)
        self.assertIn("<pre", h)
        self.assertIn("NET_RX 软中断被任务", h)
        self.assertIn("线路段慢实为收包软中断被抢占", h)
        # JSON：cpu_busy.client.softirq_localization（mode=wire）
        doc = json.loads(nla.generate_json(contexts, ns, str(root),
                                           aux_stats=disc.aux_stats))
        loc = doc["traces"][0]["cpu_busy"]["client"]["softirq_localization"]
        self.assertEqual(loc["mode"], "wire")
        self.assertEqual(loc["comm"], "ubctl")
        self.assertEqual(loc["cpu"], 44)
        self.assertIn("ubctl_query_dl_pkt_stats_data", loc["kstack"])
        # raw：定位结论 + raise 原始行（含 kstack 续行）
        raw = nla.generate_raw(contexts, ns, str(root), disc, trace_lines)
        self.assertIn("定位", raw)
        self.assertIn("irq-to-softirq", raw)
        self.assertIn("ubctl_query_dl_pkt_stats_data", raw)


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
        self.assertIn('class="ev-tbl"', out)       # 事件表
        # 列宽按内容自适应：无固定列宽/截断（连接端口、事件名称完整显示）
        self.assertNotIn("<colgroup>", out)
        self.assertNotIn("text-overflow", out)

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
        # 性能：视口外跳过渲染 + 滚动容器限高；
        # 列宽按内容自适应（连接端口/事件名称等关键信息不截断）
        self.assertIn("content-visibility:auto", out)
        self.assertNotIn("table-layout:fixed", out)
        self.assertNotIn("text-overflow:ellipsis", out)
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
        # schema v2：事件明细改 by_kind 计数（明细按 ts 对齐 kernel_events）
        self.assertEqual(sw["sides"]["client"]["by_kind"], {"tcp_send_in": 2})
        self.assertNotIn("events", sw["sides"]["client"])
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


class TestJsonDedup(unittest.TestCase):
    """schema v2：JSON 事件去重保原始（A1）。

    事件明细（含 raw 行）仅在 kernel_events 全量存一份；其余位置改引用：
    - cpu_busy.other_on_cpu / switches_on_cpu / switched_out /
      softirq_raise_on_cpu / softirq_exit_on_cpu → {n, ts_list}；
    - cpu_busy.window_events → {n, first_ts, last_ts} 摘要；
    - slow_seg_window → 窗口定义 + 各 kind 计数（事件明细删）；
    - 顶层 softirq_events 字段删除（cpu_busy 摘要已覆盖）；
    - softirq_localization.events 保留（kstack 独有）：与 kernel_events
      重复的事件去 raw（ts 引用），窗口外回溯事件（kernel_events 无）保 raw。
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

    def _dump(self, ctx):
        import argparse
        ns = argparse.Namespace(residual_threshold=1000)
        return json.loads(nla.generate_json([ctx], ns, "/tmp"))

    def test_schema_v2(self):
        doc = self._dump(self._ctx())
        self.assertEqual(doc["schema_version"], 2)
        self.assertTrue(any("kernel_events" in n and "去重" in n
                            for n in doc["notes"]))

    def test_kernel_events_keeps_full_raw(self):
        ctx = self._ctx()
        ctx.filtered_events = {"client": [self._ev()], "server": []}
        tr = self._dump(ctx)["traces"][0]
        self.assertEqual(len(tr["kernel_events"]["client"]), 1)
        ev = tr["kernel_events"]["client"][0]
        self.assertEqual(ev["raw"], "raw-line")
        self.assertEqual(ev["kind"], "tcp_send_in")

    def test_cpu_busy_lists_are_ts_refs(self):
        ctx = self._ctx()
        ctx.filtered_events = {"client": [], "server": []}
        ctx.cpu_busy = {"client": {
            "seg_key": "client_kernel_to_user", "seg_desc": "d",
            "seg_dur_us": 5000,
            "window_start": self.T("10:00:00.300000"),
            "window_end": self.T("10:00:00.305000"),
            "anchor_name": "ClientRecv", "anchor_tid": 100, "anchor_cpu": 1,
            "conn": ctx.conn, "n_mine": 2, "n_other": 2,
            "other_conns": {"9.9.9.9:9 <-> 8.8.8.8:8": 2},
            "other_by_cpu": {1: 2},
            "other_on_cpu": [self._ev(match5t=False),
                             self._ev(match5t=False, raw="raw2")],
            "switches_on_cpu": [
                {"ts": self.T("10:00:00.301000"), "kind": "sched_switch",
                 "cpu": 1, "raw": "sw1", "prev_pid": 100, "next_pid": 7,
                 "prev_comm": "biz", "next_comm": "other"}],
            "switched_out": [
                {"ts": self.T("10:00:00.301000"), "kind": "sched_switch",
                 "cpu": 1, "raw": "sw1", "prev_pid": 100, "next_pid": 7,
                 "prev_comm": "biz", "next_comm": "other"}],
            "softirq_raise_on_cpu": [
                {"ts": self.T("10:00:00.302000"), "kind": "softirq_raise_delay",
                 "cpu": 1, "raw": "sr1", "vec": 3, "latency_us": 1500,
                 "comm": "kvclient", "kstack": "k"}],
            "softirq_exit_on_cpu": [
                {"ts": self.T("10:00:00.303000"), "kind": "softirq_exit_delay",
                 "cpu": 1, "raw": "se1", "vec": 3, "latency_us": 2300,
                 "timer_cnt": 5}],
            "softirq_localization": None,
            "preempt": True,
            "events": [self._ev(), self._ev(raw="m2"),
                       self._ev(match5t=False), self._ev(match5t=False,
                                                         ts=self.T("10:00:00.304000"))],
        }}
        cb = self._dump(ctx)["traces"][0]["cpu_busy"]["client"]
        # 各事件列表 → {n, ts_list}（计数 + 时间戳引用，无 raw）
        for key, n in (("other_on_cpu", 2), ("switches_on_cpu", 1),
                       ("switched_out", 1), ("softirq_raise_on_cpu", 1),
                       ("softirq_exit_on_cpu", 1)):
            self.assertEqual(cb[key]["n"], n, key)
            self.assertEqual(len(cb[key]["ts_list"]), n, key)
            self.assertNotIn("raw", json.dumps(cb[key]), key)
        self.assertEqual(cb["other_on_cpu"]["ts_list"][0],
                         self.T("10:00:00.310000").isoformat())
        # window_events → {n, first_ts, last_ts} 摘要（first/last 按时间序）
        we = cb["window_events"]
        self.assertEqual(we["n"], 4)
        self.assertEqual(we["first_ts"], self.T("10:00:00.304000").isoformat())
        self.assertEqual(we["last_ts"], self.T("10:00:00.310000").isoformat())
        self.assertNotIn("raw", json.dumps(we))
        # 计数与归属摘要保留
        self.assertEqual(cb["n_mine"], 2)
        self.assertEqual(cb["n_other"], 2)
        self.assertIn("9.9.9.9:9 <-> 8.8.8.8:8", cb["other_conns"])

    def test_softirq_events_field_removed(self):
        ctx = self._ctx()
        ctx.filtered_events = {"client": [], "server": []}
        ctx.cpu_busy = {"client": {
            "seg_key": "client_kernel_to_user", "seg_desc": "d",
            "seg_dur_us": 5000,
            "window_start": self.T("10:00:00.300000"),
            "window_end": self.T("10:00:00.305000"),
            "anchor_name": "ClientRecv", "anchor_tid": 100, "anchor_cpu": 1,
            "conn": None, "n_mine": 0, "n_other": 0,
            "other_conns": {}, "other_by_cpu": {},
            "other_on_cpu": [], "switches_on_cpu": [], "switched_out": [],
            "softirq_raise_on_cpu": [], "softirq_exit_on_cpu": [],
            "softirq_localization": None, "preempt": False, "events": [],
        }}
        tr = self._dump(ctx)["traces"][0]
        self.assertNotIn("softirq_events", tr)
        self.assertIn("cpu_busy", tr)

    def test_slow_seg_window_summary(self):
        ctx = self._ctx()
        seg = {"key": "wire_s2c", "start": "ServerTcpSendIn",
               "end": "ClientTcpRecvFirst", "dur_us": 50000,
               "threshold_us": 200, "category": "c", "desc": "瓶颈段",
               "abnormal": True}
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
        sw = self._dump(ctx)["traces"][0]["slow_seg_window"]
        self.assertEqual(sw["seg_key"], "wire_s2c")
        self.assertEqual(sw["window_start"],
                         self.T("10:00:00.300000").isoformat())
        self.assertEqual(sw["window_end"],
                         self.T("10:00:00.350000").isoformat())
        self.assertEqual(sw["sides"]["client"]["n_mine"], 1)
        self.assertEqual(sw["sides"]["client"]["n_other"], 1)
        # 各 kind 计数替代事件明细
        self.assertEqual(sw["sides"]["client"]["by_kind"], {"tcp_send_in": 2})
        self.assertNotIn("events", sw["sides"]["client"])
        self.assertNotIn("raw", json.dumps(sw))

    def test_softirq_loc_events_ts_ref(self):
        ctx = self._ctx()
        # 窗口内 raise 事件同时进 kernel_events（可 ts 引用）；
        # 回溯区事件（窗口外）kernel_events 没有 → 保留 raw
        in_win = {"ts": self.T("10:00:00.302000"),
                  "kind": "softirq_raise_delay", "cpu": 1, "raw": "in-win-raw",
                  "vec": 3, "latency_us": 1500, "comm": "kvclient",
                  "kstack": "in-kstack"}
        lookback = {"ts": self.T("10:00:00.280000"),
                    "kind": "softirq_raise_delay", "cpu": 1,
                    "raw": "lookback-raw", "vec": 3, "latency_us": 5044,
                    "comm": "ubctl", "kstack": "lookback-kstack"}
        ctx.filtered_events = {"client": [dict(in_win)], "server": []}
        ctx.cpu_busy = {"client": {
            "seg_key": "client_kernel_to_user", "seg_desc": "d",
            "seg_dur_us": 5000,
            "window_start": self.T("10:00:00.300000"),
            "window_end": self.T("10:00:00.305000"),
            "anchor_name": "ClientRecv", "anchor_tid": 100, "anchor_cpu": 1,
            "conn": None, "n_mine": 0, "n_other": 0,
            "other_conns": {}, "other_by_cpu": {},
            "other_on_cpu": [], "switches_on_cpu": [], "switched_out": [],
            "softirq_raise_on_cpu": [], "softirq_exit_on_cpu": [],
            "softirq_localization": {
                "mode": "kernel_to_user", "comm": "ubctl",
                "kstack": "lookback-kstack", "latency_us": 5044, "vec": 3,
                "vec_txt": "3(NET_RX)", "cpu": 1, "anchor_cpu": 1,
                "smt": False, "ts": self.T("10:00:00.280000"),
                "recv_ts": self.T("10:00:00.300000"), "n_candidates": 2,
                "events": [lookback, in_win]},
            "preempt": True, "events": [],
        }}
        loc = self._dump(ctx)["traces"][0]["cpu_busy"]["client"][
            "softirq_localization"]
        # kstack / vec / latency 保留（定位核心证据）
        self.assertEqual(loc["comm"], "ubctl")
        self.assertIn("kstack", loc["events"][0])
        evs = {e["ts"]: e for e in loc["events"]}
        # 窗口内事件（kernel_events 有）→ raw 删（ts 引用）
        in_ref = evs[self.T("10:00:00.302000").isoformat()]
        self.assertNotIn("raw", in_ref)
        self.assertEqual(in_ref["kind"], "softirq_raise_delay")
        # 回溯区事件（kernel_events 无）→ raw 保留
        lb = evs[self.T("10:00:00.280000").isoformat()]
        self.assertEqual(lb["raw"], "lookback-raw")
        self.assertEqual(lb["kstack"], "lookback-kstack")


class TestTraceCardMerge(unittest.TestCase):
    """A2 HTML trace 卡收敛：

    - 宏观三段表并入内核分段表（宏观段作组头行，点位明细时间线保留）；
    - _side_events_html 子项3"问题时间窗全景"与 _cpu_busy_html 问题窗口
      全景重复 → cpu_busy 已覆盖该侧时删除，无 cpu_busy 时保留兜底；
    - 行渲染合一：_events_table 与全景表共用 _event_row_html
      （明细 6 列 / 全景 7 列带归属+高亮+cpu 标注）；
    - 主报告 irqoff/nic 周期监控卡收敛为摘要行 + 指向 os_monitor_report.html；
    - 探索器 JS 提取公共工厂（XEXP），numa/irq 卡只留 init 调用，CSS 一份。
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

    def _seg(self, key, start, end, dur_us=100, desc="段", abnormal=False):
        return {"key": key, "start": start, "end": end, "dur_us": dur_us,
                "threshold_us": 100, "category": "c", "desc": desc,
                "abnormal": abnormal}

    # -- 宏观三段并入分段表 ------------------------------------------------

    def test_macro_merged_into_seg_table(self):
        ctx = self._ctx()
        ctx.macro = {"cs_sr": 100000, "sr_ss": 100000, "ss_cr": 100000}
        ctx.milestones.update({
            "ClientTcpSendIn": self.T("10:00:00.110000"),
            "ServerTcpRecvFirst": self.T("10:00:00.150000"),
            "ServerTcpRecvLast": self.T("10:00:00.160000"),
            "ServerTcpSendIn": self.T("10:00:00.310000"),
            "ClientTcpRecvFirst": self.T("10:00:00.350000"),
            "ClientTcpRecvLast": self.T("10:00:00.360000"),
        })
        ctx.kernel_segments = [
            self._seg("client_user_to_kernel", "ClientSend", "ClientTcpSendIn"),
            self._seg("wire_c2s", "ClientTcpSendIn", "ServerTcpRecvFirst"),
            self._seg("server_kernel_to_user", "ServerTcpRecvLast", "ServerRecv"),
            self._seg("server_user_to_kernel", "ServerSend", "ServerTcpSendIn"),
            self._seg("client_kernel_to_user", "ClientTcpRecvLast", "ClientRecv",
                      abnormal=True),
        ]
        h = nla._trace_html(ctx, 1)
        # 独立"RPC 宏观分段"表删除（信息并入分段表组头）
        self.assertNotIn("RPC 宏观分段", h)
        # 分段表带宏观组头行（3 组，含段名与耗时）
        self.assertEqual(h.count('class="seg-grp"'), 3)
        self.assertIn("ClientSend→ServerRecv", h)
        self.assertIn("ServerRecv→ServerSend", h)
        self.assertIn("ServerSend→ClientRecv", h)
        # 组头含宏观段耗时与异常判定（cs_sr 100ms > 500us 阈值 → 异常）
        self.assertIn("异常", h)
        # 点位明细时间线保留
        self.assertIn("全路径时间线", h)

    # -- 整窗全景去重 ------------------------------------------------------

    def test_pano_removed_when_cpu_busy_present(self):
        ctx = self._ctx()
        ctx.bpf_window_events["client"] = [self._ev(match5t=True)]
        ctx.cpu_busy = {"client": {
            "seg_key": "client_kernel_to_user", "seg_desc": "d",
            "seg_dur_us": 5000,
            "window_start": self.T("10:00:00.350000"),
            "window_end": self.T("10:00:00.400000"),
            "anchor_name": "ClientRecv", "anchor_tid": 100, "anchor_cpu": 1,
            "conn": None, "n_mine": 1, "n_other": 0,
            "other_conns": {}, "other_by_cpu": {},
            "other_on_cpu": [], "switches_on_cpu": [], "switched_out": [],
            "softirq_raise_on_cpu": [], "softirq_exit_on_cpu": [],
            "softirq_localization": None, "preempt": False,
            "events": [self._ev(match5t=True)]}}
        out = nla._side_events_html(ctx, "client")
        # cpu_busy 已渲染该侧问题窗口全景 → 整窗全景子项删除
        self.assertNotIn("问题时间窗全景", out)
        self.assertIn("问题请求相关事件", out)

    def test_pano_kept_when_no_cpu_busy(self):
        ctx = self._ctx()
        ctx.bpf_window_events["client"] = [self._ev(match5t=True)]
        out = nla._side_events_html(ctx, "client")
        # 无 cpu_busy 分析（kernel_to_user 段不异常）→ 整窗全景保留兜底
        self.assertIn("问题时间窗全景", out)

    # -- 行渲染合一 --------------------------------------------------------

    def test_unified_event_row_renderer(self):
        self.assertTrue(callable(nla._event_row_html))
        # 明细表 6 列（无归属列/高亮/data-o）
        row6 = nla._event_row_html(self._ev())
        self.assertEqual(row6.count("<td>"), 6)
        self.assertNotIn("data-o=", row6)
        # 全景表 7 列：归属列 + 问题连接高亮 + data-o 过滤属性
        row7 = nla._event_row_html(self._ev(), with_owner=True)
        self.assertEqual(row7.count("<td>"), 7)
        self.assertIn('class="hl5t"', row7)
        self.assertIn('data-o="mine"', row7)
        self.assertIn("问题连接", row7)
        # cpu 标注（业务 cpu 红色）
        row_cpu = nla._event_row_html(self._ev(), cpu_val=1, with_owner=True)
        self.assertIn('class="cpuflag"', row_cpu)
        # 推测 badge（明细表）
        row_inf = nla._event_row_html(self._ev(), inferred=True)
        self.assertIn("推测", row_inf)
        # nic 事件 rc 字段不丢（两套行渲染合一后信息保留）
        nic = self._ev(kind="nic_rx", local_ip=None, local_port=None,
                       peer_ip=None, peer_port=None, dir_arrow=None,
                       match5t=None, src_ip="1.1.1.1", src_port=1,
                       dst_ip="2.2.2.2", dst_port=2, dev="eth0", seq=7,
                       len=100, rc=0)
        self.assertIn("rc=0", nla._event_row_html(nic))
        self.assertIn("rc=0", nla._event_row_html(nic, with_owner=True))

    # -- 主报告周期监控卡收敛 ----------------------------------------------

    def test_main_report_os_monitor_summary(self):
        import argparse
        aux = {"irqoff": {"10.1.2.3": {"total": 3, "hardirq_n": 2,
                                        "softirq_n": 1, "max_us": 4200,
                                        "total_us": 7700, "buckets": {"1000": 3},
                                        "by_comm": {}, "series": []}},
               "nic": {"10.1.2.3": {"eth0": {"n_samples": 10,
                                             "max_ifutil": 91.5,
                                             "avg_ifutil": 12.0,
                                             "peak_hms": "10:00:05"}}}}
        ctx = self._ctx()
        ns = argparse.Namespace(residual_threshold=1000)
        out = nla.generate_report([ctx], ns, "/tmp", aux_stats=aux)
        # 收敛为摘要卡：一行结论 + 指向独立报告
        self.assertIn("os_monitor_report.html", out)
        self.assertIn("关中断", out)
        self.assertIn("4.200 ms", out)
        # 旧静态卡（分桶直方图 / 进程 top10 表 / SVG 散点）不再进主报告
        self.assertNotIn("时长分桶", out)
        self.assertNotIn("进程 top10", out)
        self.assertNotIn("<svg", out)
        self.assertFalse(hasattr(nla, "_irqoff_overview_html"))

    # -- 探索器 JS 提取公共工厂 ---------------------------------------------

    @staticmethod
    def _numa_aux():
        return {"numa": {"141.61.91.189": {
            "memory": [{"ts": DAY, "ddrc_read_mb_s": 214.49,
                        "ddrc_write_mb_s": 1258.2}]}}}

    @staticmethod
    def _irqoff_aux():
        return {"irqoff": {"10.1.2.3": {
            "total": 1, "hardirq_n": 1, "softirq_n": 0, "max_us": 2000,
            "total_us": 2000, "buckets": {"1000": 1},
            "by_comm": {"kubelet": {"n": 1, "max_us": 2000, "total_us": 2000}},
            "series": [[DAY, 2000, "kubelet", 4]]}}}

    def test_explorer_js_factory(self):
        h_numa = nla._numa_explorer_html(self._numa_aux())
        h_irq = nla._irqoff_explorer_html(self._irqoff_aux())
        # 卡内只保留 init 调用，不再各嵌一份完整 JS/CSS
        self.assertIn("XEXP('numa')", h_numa)
        self.assertIn("XEXP('irq')", h_irq)
        self.assertNotIn("mousemove", h_numa)
        self.assertNotIn("mousemove", h_irq)
        self.assertNotIn("<style>", h_numa)
        self.assertNotIn("<style>", h_irq)
        # 报告级：工厂 JS 与 CSS 各只出现一次
        aux = {"numa": self._numa_aux()["numa"],
               "irqoff": self._irqoff_aux()["irqoff"]}
        rep = nla.generate_os_monitor_report(aux, "/tmp/fake")
        self.assertEqual(rep.count("window.XEXP"), 1)
        self.assertEqual(rep.count(".xexp-tooltip{"), 1)
        self.assertIn("mousemove", rep)
        self.assertIn("XEXP('numa')", rep)
        self.assertIn("XEXP('irq')", rep)


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


BTHREAD_COMPLETED_SAMPLE = (
    # 新格式：first scheduled 含 cpu_id；completed 含 execution/lifetime_time_us
    "I0831 12:58:15.711692  6325 4294969346 task_group.cpp:551 start_foreground] [WZY] "
    "bthread created: creator_tid=6325 bthread_id=19228568585221 "
    "creation_time_ns=334720225044535 creation_mode=foreground "
    "target_local_pending_tasks=0 target_remote_pending_tasks=0 "
    "target_pending_tasks=0\n"
    "I0831 12:58:15.711722  6325 19228568585221 task_group.cpp:398 task_runner] [WZY] "
    "bthread first scheduled: worker_tid=6325 cpu_id=187 bthread_id=19228568585221 "
    "fn=0xfffbec92df60 arg=0x280043c0 creation_time_ns=334720225044535 "
    "first_run_time_ns=334720225072314 pending_time_us=27\n"
    "I0831 12:58:15.711739  6325 19228568585221 task_group.cpp:422 task_runner] [WZY] "
    "bthread completed: worker_tid=6325 cpu_id=187 bthread_id=19228568585221 "
    "fn=0xfffbec92df60 arg=0x280043c0 completion_time_ns=334720225092094 "
    "execution_time_us=19 lifetime_time_us=47\n"
    "I0831 13:30:00.000000  6325 99 f.cpp:1 f] [WZY] "
    "bthread completed: worker_tid=6325 cpu_id=1 bthread_id=99 "
    "fn=0x1 arg=0x2 completion_time_ns=1 execution_time_us=5 lifetime_time_us=9\n"
)


class TestBthreadCompleted(unittest.TestCase):
    """bthread completed 事件解析 + 新格式 first scheduled（含 cpu_id）。"""

    def _write(self, text):
        fd, path = tempfile.mkstemp(suffix=".log")
        os.close(fd)
        Path(path).write_text(text, encoding="utf-8")
        self.addCleanup(os.unlink, path)
        return path

    def test_parse_completed_and_cpu_id(self):
        path = self._write(BTHREAD_COMPLETED_SAMPLE)
        wins = {"in": (datetime(2026, 8, 31, 12, 58, 15),
                       datetime(2026, 8, 31, 12, 58, 16))}
        evs = nla.scan_bthread_windows(path, wins)
        self.assertEqual(len(evs["in"]), 3)   # 第 4 行窗口外
        created, sched, done = evs["in"]
        self.assertEqual(created["kind"], "created")
        self.assertEqual(sched["kind"], "scheduled")
        self.assertEqual(sched["cpu"], 187)          # 新格式 cpu_id
        self.assertEqual(sched["pending_time_us"], 27)
        self.assertEqual(done["kind"], "completed")
        self.assertEqual(done["tid"], 6325)
        self.assertEqual(done["bthread_id"], 19228568585221)
        self.assertEqual(done["cpu"], 187)
        self.assertEqual(done["execution_time_us"], 19)
        self.assertEqual(done["lifetime_time_us"], 47)
        self.assertIn("bthread completed", done["raw"])
        # 旧格式（无 cpu_id）仍可解析
        old = nla._parse_bthread_event(
            "I0821 21:31:21.075000  523 111 f.cpp:1 task_runner] [WZY] "
            "bthread first scheduled: worker_tid=523 bthread_id=100 fn=0x1 "
            "arg=0x2 creation_time_ns=1 first_run_time_ns=2 pending_time_us=4900\n",
            datetime(2026, 8, 21, 21, 31, 21, 75000))
        self.assertEqual(old["kind"], "scheduled")
        self.assertIsNone(old["cpu"])
        self.assertEqual(old["pending_time_us"], 4900)

    def test_bthread_evidence_completed_stats(self):
        ctx = nla.TraceContext.__new__(nla.TraceContext)
        ctx.bthread_events = {"client": [
            {"ts": None, "kind": "created", "tid": 523, "bthread_id": 1,
             "creation_mode": "foreground", "target_pending_tasks": 3,
             "pending_time_us": None, "execution_time_us": None,
             "lifetime_time_us": None, "cpu": None, "raw": "r1"},
            {"ts": None, "kind": "scheduled", "tid": 523, "bthread_id": 1,
             "creation_mode": None, "target_pending_tasks": None,
             "pending_time_us": 4900, "execution_time_us": None,
             "lifetime_time_us": None, "cpu": 3, "raw": "r2"},
            {"ts": None, "kind": "completed", "tid": 523, "bthread_id": 1,
             "creation_mode": None, "target_pending_tasks": None,
             "pending_time_us": None, "execution_time_us": 2200,
             "lifetime_time_us": 7100, "cpu": 3, "raw": "r3"},
        ], "server": []}
        ctx.anchors = {"ClientRecv": {"tid": 523}}
        ctx.coro_evidence = []
        nla._bthread_evidence(ctx)
        joined = " | ".join(ctx.coro_evidence)
        self.assertIn("完成 1 个", joined)
        self.assertIn("execution_time_us 峰值 2200us", joined)
        self.assertIn("lifetime_time_us 峰值 7100us", joined)
        self.assertIn("pending_time_us 峰值 4900us", joined)
        # JSON 新字段
        j = nla._bthread_json(ctx.bthread_events["client"][2])
        self.assertEqual(j["execution_time_us"], 2200)
        self.assertEqual(j["lifetime_time_us"], 7100)
        self.assertEqual(j["cpu"], 3)


class TestDiscoveryTarGz(unittest.TestCase):
    """超大日志 tar.gz 归档：发现阶段就地解压后按普通文件匹配。"""

    def setUp(self):
        import shutil
        import tarfile
        root = Path(tempfile.mkdtemp(prefix="targz_"))
        self._root = root
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        bdir = root / "dscollect_log"
        ldir = root / "latency_warn_log"
        bdir.mkdir()
        ldir.mkdir()
        # bpf 日志打包成 tar.gz（内层文件名 bpf-<node>-<ip>.log）
        bpf_text = ("21:31:21:060777 tcp  send in  tid 1 cpu 1 size 100 "
                    "192.168.219.138:37880 -> 192.168.102.161:31501\n")
        inner = bdir / "bpf-worker1-192.168.219.1.log"
        inner.write_text(bpf_text, encoding="utf-8")
        with tarfile.open(bdir / "bpf-worker1-192.168.219.1.log.tar.gz",
                          "w:gz") as tf:
            tf.add(inner, arcname=inner.name)
        inner.unlink()
        # 坏包不影响其余发现
        (bdir / "broken.tar.gz").write_bytes(b"not a tar")
        # irqoff / nic / brpc 为普通文件
        (bdir / "irqoff_latency_192.168.219.1.log").write_text("", encoding="utf-8")
        (bdir / "nic-192.168.219.1.log").write_text("", encoding="utf-8")
        (bdir / "kvclient-1-master-brpc_client.log").write_text("", encoding="utf-8")
        # latency_warn 也归档
        warn_inner = ldir / "worker1_192.168.219.1"
        warn_inner.write_text("", encoding="utf-8")
        with tarfile.open(ldir / "worker1_192.168.219.1.tar.gz", "w:gz") as tf:
            tf.add(warn_inner, arcname=warn_inner.name)
        warn_inner.unlink()

    def test_discover_and_scan(self):
        disc = nla.LogDiscovery(str(self._root))
        # tar.gz 已就地解压，bpf 按节点名注册且内容可扫描
        self.assertIn("worker1", disc.bpf_by_node)
        bpf_path = disc.bpf_by_node["worker1"]
        self.assertEqual(bpf_path.name, "bpf-worker1-192.168.219.1.log")
        self.assertTrue(bpf_path.is_file())
        self.assertIn("worker1", disc.warn_by_node)
        self.assertIn("worker1", disc.irqoff_by_node)
        self.assertIn("worker1", disc.nic_by_node)
        self.assertIn("kvclient-1-master", disc.brpc_by_pod)
        # BpfScanner 可直接扫描解压后的文件
        wins = [nla.TraceWindow("t0", "client", datetime(2026, 8, 21, 21, 31, 21, 0),
                                datetime(2026, 8, 21, 21, 31, 21, 200000),
                                "192.168.219.138", "192.168.102.161")]
        res, _trunc = nla.BpfScanner(str(bpf_path), wins).scan()
        self.assertEqual(len(res[("t0", "client")]), 1)


class TestPigzExtract(unittest.TestCase):
    """tar.gz 解压优先走 tar --use-compress-program="pigz -p N"（N=workers）。"""

    def setUp(self):
        import shutil
        import tarfile
        root = Path(tempfile.mkdtemp(prefix="pigz_"))
        self._root = root
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        # tar.gz 放入 dscollect_log/（_extract_archives 只处理该子目录）
        bdir = root / "dscollect_log"
        bdir.mkdir()
        inner = bdir / "bpf-worker1-192.168.219.1.log"
        inner.write_text("hello\n", encoding="utf-8")
        with tarfile.open(bdir / "bpf-worker1-192.168.219.1.log.tar.gz",
                          "w:gz") as tf:
            tf.add(inner, arcname=inner.name)
        inner.unlink()

    def test_pigz_command_matches_workers(self):
        """pigz/tar 可用：调用 tar 且 -p 并行数与 workers 一致。"""
        import subprocess
        calls = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        with mock.patch.object(nla.shutil, "which",
                               lambda p: "/usr/bin/%s" % p), \
             mock.patch.object(nla.subprocess, "run", fake_run):
            nla.LogDiscovery._extract_archives(self._root / "dscollect_log", workers=8)
        self.assertEqual(len(calls), 1)
        cmd = " ".join(calls[0])
        self.assertIn("--use-compress-program=pigz -p 8", cmd)
        self.assertIn(str(self._root / "dscollect_log" /
                          "bpf-worker1-192.168.219.1.log.tar.gz"), cmd)

    def test_tar_failure_falls_back_to_tarfile(self):
        """tar 命令失败（非零退出）：回退 Python tarfile 完成解压。"""
        import subprocess

        def fake_run(cmd, **kw):
            return subprocess.CompletedProcess(cmd, 1)

        with mock.patch.object(nla.shutil, "which",
                               lambda p: "/usr/bin/%s" % p), \
             mock.patch.object(nla.subprocess, "run", fake_run):
            nla.LogDiscovery._extract_archives(self._root / "dscollect_log", workers=4)
        out = self._root / "dscollect_log" / "bpf-worker1-192.168.219.1.log"
        self.assertTrue(out.is_file())
        self.assertEqual(out.read_text(encoding="utf-8"), "hello\n")

    def test_no_pigz_uses_tarfile(self):
        """pigz 不可用：不调用 tar 子进程，直接 Python tarfile 解压。"""
        with mock.patch.object(nla.shutil, "which", lambda p: None), \
             mock.patch.object(nla.subprocess, "run",
                               lambda cmd, **kw: self.fail("不应调用 tar 子进程")):
            nla.LogDiscovery._extract_archives(self._root / "dscollect_log", workers=4)
        out = self._root / "dscollect_log" / "bpf-worker1-192.168.219.1.log"
        self.assertTrue(out.is_file())
        self.assertEqual(out.read_text(encoding="utf-8"), "hello\n")

    def test_discovery_forwards_workers(self):
        """LogDiscovery 把 workers 透传给 _extract_archives。"""
        seen = []
        real = nla.LogDiscovery._extract_archives

        def spy(directory, workers=1):
            seen.append(workers)
            return real(directory, workers=workers)

        with mock.patch.object(nla.LogDiscovery, "_extract_archives",
                               staticmethod(spy)):
            nla.LogDiscovery(str(self._root), workers=6)
        self.assertIn(6, seen)


class TestEnvNodeMapping(unittest.TestCase):
    """env 文件（pod_ip → 宿主机 IP）+ 同 IP 多命名别名收敛。"""

    def setUp(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="envmap_"))
        self._root = root
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        bdir = root / "dscollect_log"
        ldir = root / "latency_warn_log"
        wdir = root / "collected_worker_logs" / "worker_192.168.210.192"
        for d in (bdir, ldir, wdir):
            d.mkdir(parents=True)
        # env：pod ip → 宿主机 IP
        (wdir / "env").write_text("pod_ip=192.168.210.192\nJD_HOST_IP=192.168.0.59\n",
                                  encoding="utf-8")
        # 同一宿主机 IP 192.168.0.59：bpf 命名 worker12，warn 命名 worker16
        (bdir / "bpf-worker12-192.168.0.59.log").write_text("", encoding="utf-8")
        (bdir / "irqoff_latency_192.168.0.59.log").write_text("", encoding="utf-8")
        (bdir / "nic-192.168.0.59.log").write_text("", encoding="utf-8")
        (ldir / "worker16_192.168.0.59").write_text("", encoding="utf-8")

    def test_env_and_alias(self):
        disc = nla.LogDiscovery(str(self._root))
        # env 映射：pod ip → 宿主机 ip
        self.assertEqual(disc.host_by_podip.get("192.168.210.192"), "192.168.0.59")
        # 同 IP 双命名：bpf 先注册为规范名 worker12，warn 的 worker16 收敛为别名
        self.assertEqual(disc.node_by_ip.get("192.168.0.59"), "worker12")
        self.assertEqual(disc.node_alias.get("worker16"), "worker12")
        self.assertIn("worker12", disc.warn_by_node)   # warn 挂到规范名下
        self.assertIn("worker12", disc.irqoff_by_node)
        self.assertIn("worker12", disc.nic_by_node)
        # pod 目录名 worker_<podIp> → env → 宿主机 → 规范节点名
        self.assertEqual(disc.resolve_node("worker_192.168.210.192"), "worker12")
        # 别名 pod 目录也能解析（子串匹配别名后映射回规范名）
        self.assertEqual(disc.resolve_node("kvworker-0-worker16"), "worker12")
        self.assertEqual(disc.node_names_for("worker12"), ("worker12", "worker16"))

    def test_resolve_node_fallback(self):
        disc = nla.LogDiscovery(str(self._root))
        # 无 env 映射时回退子串匹配 / 不匹配返回 None
        self.assertEqual(disc.resolve_node("bpf-worker12"), "worker12")
        self.assertIsNone(disc.resolve_node("pod-on-other"))


class TestBrpcFileMatching(unittest.TestCase):
    """brpc 文件关联：节点名段匹配 + 唯一同角色文件兜底。"""

    def _mk(self, name):
        fd, path = tempfile.mkstemp(suffix=name)
        os.close(fd)
        self.addCleanup(os.unlink, path)
        return Path(path)

    def test_name_match_first(self):
        p = self._mk("kvclient-1-master-brpc_client.log")
        brpc = {"kvclient-1-master": p}
        self.assertEqual(nla._brpc_files_for_pod(brpc, "kvclient-1-master_26"), [p])

    def test_node_name_match(self):
        # pod 目录名被简化成 worker_<podIp>，brpc pod 名含宿主机节点名段
        p = self._mk("kvworker-0-worker19-brpc_server.log")
        brpc = {"kvworker-0-worker19": p}
        self.assertEqual(
            nla._brpc_files_for_pod(brpc, "worker_192.168.210.192",
                                    node_names=("worker19",)), [p])

    def test_unique_role_fallback(self):
        # 名称 / 节点名都匹配不上时，同角色文件全局唯一则兜底
        p = self._mk("kvworker-0-worker19-brpc_server.log")
        brpc = {"kvworker-0-worker19": p}
        self.assertEqual(
            nla._brpc_files_for_pod(brpc, "worker_10.0.0.9", role="server"), [p])
        # 同角色文件不唯一时不猜
        p2 = self._mk("other-0-worker2-brpc_server.log")
        brpc2 = {"kvworker-0-worker19": p, "other-0-worker2": p2}
        self.assertEqual(
            nla._brpc_files_for_pod(brpc2, "worker_10.0.0.9", role="server"), [])
        # 角色不符也不兜底
        pc = self._mk("kvclient-1-master-brpc_client.log")
        brpc3 = {"kvclient-1-master": pc}
        self.assertEqual(
            nla._brpc_files_for_pod(brpc3, "worker_10.0.0.9", role="server"), [])


class TestHostIdLogExtraction(unittest.TestCase):
    """日志正文 Host ID is/id is 行：podIP→宿主机 IP 映射（env 缺失时兜底）。

    场景（/home/wcy/minilog 新布局）：
      - client 目录 SDK_192.168.49.66 无 env，宿主机 IP 只在 ds_client 日志正文
        "Host ID is 141.62.33.21 from env HOST_IP"；
      - worker 目录 worker_192.168.210.192 的 env 已有映射，日志正文另有
        "Host id is 141.62.32.59 from env JD_HOST_IP"（一致性来源）。
    """

    TRACE = "getBuffer-15-62-00000003;505d6e895e86"
    CIP, SIP = "192.168.49.66", "192.168.210.192"
    CHOST, SHOST = "141.62.33.21", "141.62.32.59"

    def _make_root(self, with_env=False, with_client_bpf=True,
                   with_server_bpf=True):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="hostid_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        cdir = root / "collected" / ("SDK_%s" % self.CIP)
        wdir = root / "collected_worker_logs" / ("worker_%s" % self.SIP)
        bdir = root / "dscollect_log"
        for d in (cdir, wdir, bdir):
            d.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, self.TRACE, msg))

        (cdir / "ds_client_62.INFO.log").write_text(
            info("2026-09-04T17:46:54.100000", self.CIP,
                 "yyl9 ClientSend ts 838951000000000 tid 245 cpu 246")
            + info("2026-09-04T17:46:54.130000", self.CIP,
                   "yyl9 ClientRecv ts 838951030000000 tid 245 cpu 246")
            + info("2026-09-04T17:46:54.130013", self.CIP, SLOW_MSG.replace(
                "getBuffer-25487-00004775;117c5c4a91c7", self.TRACE).replace(
                "network_residual_us=15989", "network_residual_us=25000"))
            + ("2026-09-04T17:46:45.365435 | I | service_discovery.cpp:74 | "
               "%s | 62:62 |  |  |  Host ID is %s from env HOST_IP\n"
               % (self.CIP, self.CHOST)),
            encoding="utf-8")
        (wdir / "kvcache.INFO.log").write_text(
            info("2026-09-04T17:46:54.100500", self.SIP,
                 "yyl3 ServerRecv ts 697732000005000 tid 226 cpu 28")
            + info("2026-09-04T17:46:54.101000", self.SIP,
                   "yyl10 ServerSend ts 697732000010000 tid 226 cpu 28")
            + ("2026-09-04T17:44:35.408402 | I | ds_coordination_backend.cpp:621 | "
               "%s | 9:9 | 9a407f3c | jingpai |  Host id is %s from env JD_HOST_IP\n"
               % (self.SIP, self.SHOST)),
            encoding="utf-8")
        if with_env:
            (wdir / "env").write_text(
                "pod_ip=%s\nJD_HOST_IP=%s\n" % (self.SIP, self.SHOST),
                encoding="utf-8")
        if with_client_bpf:
            (bdir / ("bpf-worker22-%s.log" % self.CHOST)).write_text(
                "17:46:54:100050 tcp  send in  tid 245 cpu 50 size 270 "
                "%s:53896 -> %s:31402\n" % (self.CIP, self.SIP),
                encoding="utf-8")
        if with_server_bpf:
            (bdir / ("bpf-worker12-%s.log" % self.SHOST)).write_text(
                "17:46:54:100550 tcp  recv in  tid 226 cpu 28 size 206 "
                "%s:31402 <- %s:53896\n" % (self.SIP, self.CIP),
                encoding="utf-8")
        return root

    def test_hostid_pairs_extracted(self):
        """collect_anchor_and_info 一并提取日志正文 Host ID 映射对。"""
        root = self._make_root()
        disc = nla.LogDiscovery(str(root))
        anchor_idx, info_idx, hostid_pairs = nla.collect_anchor_and_info(
            disc.client_logs, disc.worker_logs, [self.TRACE])
        got = dict(hostid_pairs)
        self.assertEqual(got.get(self.CIP), self.CHOST)
        self.assertEqual(got.get(self.SIP), self.SHOST)

    def test_client_node_resolved_and_events_recovered(self):
        """client 侧经日志正文 Host ID 解析到 worker22，bpf 事件恢复。"""
        root = self._make_root()
        disc, contexts, _tl = nla.analyze(str(root), window_pad_ms=2)
        ctx = contexts[0]
        self.assertEqual(ctx.client_node, "worker22")
        self.assertEqual(ctx.server_node, "worker12")
        # client 侧有 bpf 事件，连接从 client 侧 tcp send 识别（非回退）
        evs = ctx.kernel_events.get("client") or []
        self.assertTrue(evs, "client 侧 bpf 事件应恢复解析")
        self.assertEqual(ctx.conn, (self.CIP, 53896, self.SIP, 31402))
        joined = " | ".join(ctx.missing)
        self.assertNotIn("无法映射到 bpf 节点", joined)
        self.assertNotIn("回退识别", joined)
        self.assertTrue(ctx.kernel_events.get("server"))

    def test_env_wins_over_log(self):
        """env 与日志正文同时存在且一致来源优先：env 值生效。"""
        root = self._make_root(with_env=True)
        # env 指向另一台宿主机（提供该节点 bpf），验证 env 优先
        bdir = root / "dscollect_log"
        (bdir / "bpf-worker30-9.9.9.9.log").write_text("", encoding="utf-8")
        wenv = root / "collected_worker_logs" / ("worker_%s" % self.SIP) / "env"
        wenv.write_text("pod_ip=%s\nJD_HOST_IP=9.9.9.9\n" % self.SIP,
                        encoding="utf-8")
        disc, contexts, _tl = nla.analyze(str(root), window_pad_ms=2)
        self.assertEqual(contexts[0].server_node, "worker30")

    def test_missing_host_bpf_hint(self):
        """宿主机 IP 已知但该节点 bpf 未采集：missing 明确提示。"""
        root = self._make_root(with_client_bpf=False)
        disc, contexts, _tl = nla.analyze(str(root), window_pad_ms=2)
        ctx = contexts[0]
        self.assertIsNone(ctx.client_node)
        joined = " | ".join(ctx.missing)
        self.assertIn("宿主机 %s 的 bpf 日志未采集" % self.CHOST, joined)


class TestPortPairConnInference(unittest.TestCase):
    """server IP 未知但 server 服务端口固定：端口 + 双向时间配对推测连接。

    场景（/home/wcy/minilog 79 条 client-only trace）：同 run 内完整 trace
    识别出的连接提供已知 server 服务端口（31402）；client-only trace 的
    client 侧事件按「请求发送邻近 ClientSend + 响应接收邻近 ClientRecv
    同四元组配对」打分推测目标连接（实测并发扇出 ~20 连接时中位分差 340us）。
    """

    TRACE_A = "getBuffer-15-62-00099999;aaaabbbbcccc"
    TRACE_B = "getBuffer-15-62-00022342;f395d0eb066e"
    CIP, SIP_A, SIP_B = "192.168.49.66", "192.168.210.192", "192.168.49.64"
    CHOST, SHOST_A = "141.62.33.21", "141.62.32.59"

    def _make_root(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="portpair_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        cdir = root / "collected" / ("SDK_%s" % self.CIP)
        wdir = root / "collected_worker_logs" / ("worker_%s" % self.SIP_A)
        bdir = root / "dscollect_log"
        for d in (cdir, wdir, bdir):
            d.mkdir(parents=True)

        def info(ts, host, trace, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, trace, msg))

        def slow(trace, residual):
            return SLOW_MSG.replace(
                "getBuffer-25487-00004775;117c5c4a91c7", trace).replace(
                "network_residual_us=15989", "network_residual_us=%d" % residual)

        # trace A（完整：worker 日志已收集）+ trace B（client-only）
        (cdir / "ds_client_62.INFO.log").write_text(
            info("2026-09-04T17:46:54.100000", self.CIP, self.TRACE_B,
                 "yyl9 ClientSend ts 838951000000000 tid 217 cpu 246")
            + info("2026-09-04T17:46:54.130000", self.CIP, self.TRACE_B,
                   "yyl9 ClientRecv ts 838951030000000 tid 217 cpu 246")
            + info("2026-09-04T17:46:54.130013", self.CIP, self.TRACE_B,
                   slow(self.TRACE_B, 25000))
            + info("2026-09-04T17:46:54.200000", self.CIP, self.TRACE_A,
                   "yyl9 ClientSend ts 838951200000000 tid 245 cpu 246")
            + info("2026-09-04T17:46:54.230000", self.CIP, self.TRACE_A,
                   "yyl9 ClientRecv ts 838951230000000 tid 245 cpu 246")
            + info("2026-09-04T17:46:54.230013", self.CIP, self.TRACE_A,
                   slow(self.TRACE_A, 15000))
            + ("2026-09-04T17:46:45.365435 | I | service_discovery.cpp:74 | "
               "%s | 62:62 |  |  |  Host ID is %s from env HOST_IP\n"
               % (self.CIP, self.CHOST)),
            encoding="utf-8")
        # worker pod 日志：仅 trace A
        (wdir / "kvcache.INFO.log").write_text(
            info("2026-09-04T17:46:54.200500", self.SIP_A, self.TRACE_A,
                 "yyl3 ServerRecv ts 697732200005000 tid 226 cpu 28")
            + info("2026-09-04T17:46:54.201000", self.SIP_A, self.TRACE_A,
                   "yyl10 ServerSend ts 697732200010000 tid 226 cpu 28"),
            encoding="utf-8")
        (wdir / "env").write_text(
            "pod_ip=%s\nJD_HOST_IP=%s\n" % (self.SIP_A, self.SHOST_A),
            encoding="utf-8")
        # client 节点 bpf：A 的 tcp 事件（识别连接 → 已知 server 端口 31402）+
        # B 的目标连接与两类干扰连接
        (bdir / ("bpf-worker22-%s.log" % self.CHOST)).write_text(
            # A：tcp send in（client_tcp 识别）
            "17:46:54:200040 tcp  send in  tid 245 cpu 50 size 270 "
            "%s:53896 -> %s:31402\n" % (self.CIP, self.SIP_A)
            # B 目标连接：发送邻近 ClientSend、响应邻近 ClientRecv
            + "17:46:54:100040 dev_start_xmit: sip:%s, sport:53900 -> "
              "dip:%s, dport:31402, seq:1, len:338, dev:eth0\n"
              % (self.CIP, self.SIP_B)
            + "17:46:54:100042 net_dev_xmit: sip:%s, sport:53900 -> "
              "dip:%s, dport:31402, seq:1, len:338, dev:eth0, rc:0\n"
              % (self.CIP, self.SIP_B)
            + "17:46:54:129970 netif_receive_skb: sip:%s, sport:31402 -> "
              "dip:%s, dport:53900, seq:2, len:200, dev:eth9\n"
              % (self.SIP_B, self.CIP)
            # B 干扰1：同端口更早发送，但响应远离 ClientRecv（不配对）
            + "17:46:54:100010 dev_start_xmit: sip:%s, sport:53901 -> "
              "dip:192.168.49.65, dport:31402, seq:1, len:100, dev:eth0\n"
              % self.CIP
            + "17:46:54:130500 netif_receive_skb: sip:192.168.49.65, "
              "sport:31402 -> dip:%s, dport:53901, seq:2, len:100, dev:eth9\n"
              % self.CIP
            # B 干扰2：非 server 端口（31501），时间完美但端口不符
            + "17:46:54:100005 dev_start_xmit: sip:%s, sport:53902 -> "
              "dip:192.168.49.66, dport:31501, seq:1, len:100, dev:eth0\n"
              % self.CIP
            + "17:46:54:129990 netif_receive_skb: sip:192.168.49.66, "
              "sport:31501 -> dip:%s, dport:53902, seq:2, len:100, dev:eth9\n"
              % self.CIP,
            encoding="utf-8")
        # server 节点 bpf：A 的 server 侧 tcp 事件
        (bdir / ("bpf-worker12-%s.log" % self.SHOST_A)).write_text(
            "17:46:54:200550 tcp  recv in  tid 226 cpu 28 size 206 "
            "%s:31402 <- %s:53896\n" % (self.SIP_A, self.CIP),
            encoding="utf-8")
        return root

    def test_port_pair_inference(self):
        root = self._make_root()
        disc, contexts, _tl = nla.analyze(str(root), window_pad_ms=2)
        ctxB, ctxA = contexts[0], contexts[1]   # B residual 更大排前
        # A：完整关联，识别连接提供已知 server 端口
        self.assertEqual(ctxA.conn, (self.CIP, 53896, self.SIP_A, 31402))
        self.assertEqual(ctxA.conn_source, "client_tcp")
        # B：端口 + 双向时间配对推测（排除更早的同端口干扰与非端口干扰）
        self.assertEqual(ctxB.conn, (self.CIP, 53900, self.SIP_B, 31402))
        self.assertEqual(ctxB.conn_source, "client_port")
        joined = " | ".join(ctxB.missing)
        self.assertIn("服务端口", joined)
        self.assertIn("配对", joined)
        # 里程碑来自目标连接（准确路径，非时间邻近兜底）
        self.assertEqual(ctxB.milestones.get("ClientDevStartXmit"),
                         datetime(2026, 9, 4, 17, 46, 54, 100040))
        self.assertNotIn("时间邻近推测", joined)
        # 五元组过滤：仅目标连接 3 条事件
        self.assertEqual(len(ctxB.filtered_events["client"]), 3)
        # 置信度封顶"中"
        self.assertIn(ctxB.conclusion["confidence"], ("中", "低"))


class TestClientOnlyCorrelation(unittest.TestCase):
    """server pod 日志未收集（server IP 未知）时：client 侧 bpf 照常关联。

    场景（/home/wcy/minilog 79/81 条 trace 的实况）：只有 client 日志 + client
    节点 bpf；无 worker pod 日志 → server IP 未知。期望：client 侧窗口匹配 +
    全景照常输出；连接五元组标注"未能识别"；client 侧里程碑按时间邻近推测
    （多连接有混淆风险，注明）；client 段产出；置信度封顶"中"。
    """

    TRACE = "getBuffer-15-62-00022655;3fbfde650c53"
    CIP, CHOST = "192.168.49.66", "141.62.33.21"

    def _make_root(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="conly_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        cdir = root / "collected" / ("SDK_%s" % self.CIP)
        bdir = root / "dscollect_log"
        cdir.mkdir(parents=True)
        bdir.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, self.TRACE, msg))

        # worker pod 日志目录不存在（server IP 未知）
        (cdir / "ds_client_62.INFO.log").write_text(
            info("2026-09-04T17:46:54.100000", self.CIP,
                 "yyl9 ClientSend ts 838951000000000 tid 245 cpu 246")
            + info("2026-09-04T17:46:54.130000", self.CIP,
                   "yyl9 ClientRecv ts 838951030000000 tid 245 cpu 246")
            + info("2026-09-04T17:46:54.130013", self.CIP, SLOW_MSG.replace(
                "getBuffer-25487-00004775;117c5c4a91c7", self.TRACE).replace(
                "network_residual_us=15989", "network_residual_us=25000"))
            + ("2026-09-04T17:46:45.365435 | I | service_discovery.cpp:74 | "
               "%s | 62:62 |  |  |  Host ID is %s from env HOST_IP\n"
               % (self.CIP, self.CHOST)),
            encoding="utf-8")
        # client 节点 bpf：tcp 探针存活，该 pod 同时连向两个 server
        (bdir / ("bpf-worker22-%s.log" % self.CHOST)).write_text(
            # 目标流量（时间邻近）
            "17:46:54:100040 tcp  send in  tid 245 cpu 50 size 270 "
            "%s:53896 -> 192.168.210.192:31402\n" % self.CIP
            + "17:46:54:100050 dev_start_xmit: sip:%s, sport:53896 -> "
            "dip:192.168.210.192, dport:31402, seq:913492323, len:339, dev:eth0\n"
            % self.CIP
            + "17:46:54:100052 net_dev_xmit: sip:%s, sport:53896 -> "
            "dip:192.168.210.192, dport:31402, seq:913492323, len:339, "
            "dev:enp37s0f0np0, rc:0\n" % self.CIP
            + "17:46:54:103457 netif_receive_skb: sip:192.168.210.192, "
            "sport:31402 -> dip:%s, dport:53896, seq:1728523522, len:208, "
            "dev:eth9\n" % self.CIP
            # 其他 server 的流量（时间上更早，验证"最早 tcp send"推测会被它抢先）
            + "17:46:54:100030 tcp  send in  tid 245 cpu 50 size 120 "
            "%s:41734 -> 192.168.49.64:31402\n" % self.CIP,
            encoding="utf-8")
        return root

    def test_client_only_flow(self):
        root = self._make_root()
        disc, contexts, _tl = nla.analyze(str(root), window_pad_ms=2)
        ctx = contexts[0]
        self.assertEqual(ctx.client_node, "worker22")
        self.assertIsNone(ctx.server_ip)
        # client 侧 bpf 明细照常匹配（核心断言）
        self.assertEqual(len(ctx.kernel_events["client"]), 5)
        self.assertTrue(ctx.filtered_events["client"])
        self.assertTrue(ctx.bpf_window_events["client"])
        # 连接五元组未能识别 + 注明原因
        self.assertIsNone(ctx.conn)
        joined = " | ".join(ctx.missing)
        self.assertIn("worker pod 日志未收集", joined)
        self.assertIn("未能识别", joined)
        # client 侧里程碑按时间邻近推测（窗口内该 pod 最早事件，含混淆风险注明）
        self.assertIn("时间邻近推测", joined)
        self.assertIn("ClientTcpSendIn", ctx.milestones)   # 最早 tcp send（另一 server 连接）
        self.assertIn("ClientDevStartXmit", ctx.milestones)
        # client 段产出 + 置信度封顶"中"
        self.assertTrue(ctx.kernel_segments)
        self.assertIn(ctx.conclusion["confidence"], ("中", "低"))
        self.assertNotEqual(ctx.conclusion["category"], "unknown")

    def test_client_only_render(self):
        import argparse
        root = self._make_root()
        disc, contexts, _tl = nla.analyze(str(root), window_pad_ms=2)
        ns = argparse.Namespace(residual_threshold=1000)
        out = nla.generate_report(contexts, ns, str(root))
        # 事件表输出 + 推测 badge
        self.assertIn("client 节点 bpf 事件", out)
        self.assertIn("推测", out)
        data = json.loads(nla.generate_json(contexts, ns, str(root)))
        self.assertIsNone(data["traces"][0]["conn"])
        self.assertTrue(data["traces"][0]["kernel_events"]["client"])


class TestNicConnFallbackE2E(unittest.TestCase):
    """client 侧 tcp 层探针丢失（仅 nic 层事件）时：nic 五元组兜底识别连接。

    场景（/home/wcy/minilog worker22 实测）：client 节点 bpf 无 tcp 层事件，
    仅 dev_start_xmit/net_dev_xmit/netif_receive_skb（raw 内含完整五元组）。
    期望：连接从 client 侧 nic 事件识别（不依赖 server 回退），事件经推测
    五元组匹配并以"推测"badge 同表标注。
    """

    TRACE = "getBuffer-15-62-00022655;3fbfde650c53"
    CIP, SIP = "192.168.49.66", "192.168.210.192"
    CHOST, SHOST = "141.62.33.21", "141.62.32.59"

    def _make_root(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="nicfb_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        cdir = root / "collected" / ("SDK_%s" % self.CIP)
        wdir = root / "collected_worker_logs" / ("worker_%s" % self.SIP)
        bdir = root / "dscollect_log"
        for d in (cdir, wdir, bdir):
            d.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, self.TRACE, msg))

        (cdir / "ds_client_62.INFO.log").write_text(
            info("2026-09-04T17:46:54.100000", self.CIP,
                 "yyl9 ClientSend ts 838951000000000 tid 245 cpu 246")
            + info("2026-09-04T17:46:54.130000", self.CIP,
                   "yyl9 ClientRecv ts 838951030000000 tid 245 cpu 246")
            + info("2026-09-04T17:46:54.130013", self.CIP, SLOW_MSG.replace(
                "getBuffer-25487-00004775;117c5c4a91c7", self.TRACE).replace(
                "network_residual_us=15989", "network_residual_us=25000"))
            + ("2026-09-04T17:46:45.365435 | I | service_discovery.cpp:74 | "
               "%s | 62:62 |  |  |  Host ID is %s from env HOST_IP\n"
               % (self.CIP, self.CHOST)),
            encoding="utf-8")
        (wdir / "kvcache.INFO.log").write_text(
            info("2026-09-04T17:46:54.100500", self.SIP,
                 "yyl3 ServerRecv ts 697732000005000 tid 226 cpu 28")
            + info("2026-09-04T17:46:54.101000", self.SIP,
                   "yyl10 ServerSend ts 697732000010000 tid 226 cpu 28"),
            encoding="utf-8")
        (wdir / "env").write_text(
            "pod_ip=%s\nJD_HOST_IP=%s\n" % (self.SIP, self.SHOST), encoding="utf-8")
        # client 节点 bpf：仅 nic 层事件（tcp 探针丢失），含请求/响应双向
        (bdir / ("bpf-worker22-%s.log" % self.CHOST)).write_text(
            "17:46:54:100050 dev_start_xmit: sip:%s, sport:53896 -> dip:%s, "
            "dport:31402, seq:913492323, len:339, dev:eth0\n"
            "17:46:54:100052 net_dev_xmit: sip:%s, sport:53896 -> dip:%s, "
            "dport:31402, seq:913492323, len:339, dev:enp37s0f0np0, rc:0\n"
            "17:46:54:103457 netif_receive_skb: sip:%s, sport:31402 -> dip:%s, "
            "dport:53896, seq:1728523522, len:208, dev:eth9\n"
            % (self.CIP, self.SIP, self.CIP, self.SIP, self.SIP, self.CIP),
            encoding="utf-8")
        # server 节点 bpf：tcp 层事件正常
        (bdir / ("bpf-worker12-%s.log" % self.SHOST)).write_text(
            "17:46:54:100550 tcp  recv in  tid 226 cpu 28 size 206 "
            "%s:31402 <- %s:53896\n" % (self.SIP, self.CIP),
            encoding="utf-8")
        return root

    def test_nic_fallback_full_flow(self):
        root = self._make_root()
        disc, contexts, _tl = nla.analyze(str(root), window_pad_ms=2)
        ctx = contexts[0]
        # 连接从 client 侧 nic 事件识别（非 server 回退）
        self.assertEqual(ctx.conn, (self.CIP, 53896, self.SIP, 31402))
        self.assertEqual(ctx.conn_source, "client_nic")
        joined = " | ".join(ctx.missing)
        self.assertIn("tcp 层 bpf 事件丢失", joined)
        self.assertIn("已按 nic 层事件推测连接五元组", joined)
        self.assertNotIn("回退识别", joined)   # 未走 server 回退
        # client 侧 nic 事件经推测五元组匹配保留
        self.assertEqual(len(ctx.filtered_events["client"]), 3)
        self.assertEqual(len(ctx.filtered_events["server"]), 1)
        # HTML：事件同表展示 + "推测"badge
        import argparse
        ns = argparse.Namespace(residual_threshold=1000)
        out = nla.generate_report(contexts, ns, str(root))
        self.assertIn("inf-badge", out)
        self.assertIn("推测", out)
        # JSON：conn.source 标注识别来源
        data = json.loads(nla.generate_json(contexts, ns, str(root)))
        self.assertEqual(data["traces"][0]["conn"]["source"], "client_nic")
        # raw：bpf 内核日志段落头标注推测关联
        raw = nla.generate_raw(contexts, ns, str(root), disc, {})
        self.assertIn("推测关联：连接五元组经 nic 层事件推测识别", raw)

    def test_client_tcp_present_uses_tcp(self):
        """client 侧 tcp 事件存在时优先 tcp，conn_source=client_tcp。"""
        root = self._make_root()
        bdir = root / "dscollect_log"
        with open(bdir / ("bpf-worker22-%s.log" % self.CHOST), "a",
                  encoding="utf-8") as f:
            f.write("17:46:54:100040 tcp  send in  tid 245 cpu 50 size 270 "
                    "%s:53896 -> %s:31402\n" % (self.CIP, self.SIP))
        disc, contexts, _tl = nla.analyze(str(root), window_pad_ms=2)
        ctx = contexts[0]
        self.assertEqual(ctx.conn, (self.CIP, 53896, self.SIP, 31402))
        self.assertEqual(ctx.conn_source, "client_tcp")
        self.assertNotIn("推测连接五元组", " | ".join(ctx.missing))


class TestNewLayoutEndToEnd(unittest.TestCase):
    """新采集布局端到端：tar.gz bpf + worker_<podIp> 目录 + env + brpc 兜底。

    场景（server 侧收包后取包慢）：
      - client pod 目录 SDK_192.168.49.66（无 bpf，client 侧降级）；
      - worker pod 目录 worker_192.168.210.192，env 映射宿主机 192.168.0.59
        → bpf tar.gz 解压后按节点 worker12 扫描，server 内核事件恢复关联；
      - brpc_server 文件 pod 名与目录名无关 → 唯一同角色兜底关联。
    """

    TRACE = "getBuffer-15-62-00000001;312c2e895e84"
    CIP, SIP = "192.168.49.66", "192.168.210.192"

    def setUp(self):
        import shutil
        import tarfile
        root = Path(tempfile.mkdtemp(prefix="newlayout_"))
        self._root = root
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        cdir = root / "collected" / "SDK_192.168.49.66"
        wdir = root / "collected_worker_logs" / "worker_192.168.210.192"
        bdir = root / "dscollect_log"
        for d in (cdir, wdir, bdir):
            d.mkdir(parents=True)

        def info(ts, host, msg):
            return ("%s | I | f.cpp:1 | %s | 1:2 | %s | u |  %s\n"
                    % (ts, host, self.TRACE, msg))

        (cdir / "ds_client_62.INFO.log").write_text(
            info("2026-09-04T17:46:54.100000", self.CIP,
                 "yyl9 ClientSend ts 838951000000000 tid 245 cpu 246")
            + info("2026-09-04T17:46:54.130000", self.CIP,
                   "yyl9 ClientRecv ts 838951030000000 tid 245 cpu 246")
            + info("2026-09-04T17:46:54.130013", self.CIP, SLOW_MSG.replace(
                "getBuffer-25487-00004775;117c5c4a91c7", self.TRACE).replace(
                "network_residual_us=15989", "network_residual_us=25000")),
            encoding="utf-8")
        (wdir / "kvcache.INFO.log").write_text(
            info("2026-09-04T17:46:54.100500", self.SIP,
                 "yyl3 ServerRecv ts 697732000005000 tid 226 cpu 28")
            + info("2026-09-04T17:46:54.101000", self.SIP,
                   "yyl10 ServerSend ts 697732000010000 tid 226 cpu 28"),
            encoding="utf-8")
        (wdir / "env").write_text(
            "pod_ip=192.168.210.192\nJD_HOST_IP=192.168.0.59\n", encoding="utf-8")

        # bpf 日志（宿主机 192.168.0.59，含 server pod 收发事件）打包 tar.gz
        bpf_text = (
            "17:46:54:100100 netif_receive_skb: sip:%s, sport:53896 -> dip:%s, "
            "dport:31402, seq:1, len:206, dev:eth0\n" % (self.CIP, self.SIP)
            + "17:46:54:100110 tcp  recv in  tid 226 cpu 28 size 206 "
              "%s:31402 <- %s:53896\n" % (self.SIP, self.CIP)
            + "17:46:54:100120 tcp  send in  tid 226 cpu 28 size 155 "
              "%s:31402 -> %s:53896\n" % (self.SIP, self.CIP))
        inner = bdir / "bpf-worker12-192.168.0.59.log"
        inner.write_text(bpf_text, encoding="utf-8")
        with tarfile.open(bdir / "bpf-worker12-192.168.0.59.log.tar.gz",
                          "w:gz") as tf:
            tf.add(inner, arcname=inner.name)
        inner.unlink()
        # brpc_server：pod 名与 worker 目录名无关（走唯一角色兜底）
        (bdir / "kvworker-0-worker19-brpc_server.log").write_text(
            "I0904 17:46:54.100900  226 1 f.cpp:1 task_runner] [WZY] "
            "bthread first scheduled: worker_tid=226 cpu_id=28 bthread_id=100 "
            "fn=0x1 arg=0x2 creation_time_ns=1 first_run_time_ns=2 "
            "pending_time_us=3800\n"
            "I0904 17:46:54.101100  226 1 f.cpp:1 task_runner] [WZY] "
            "bthread completed: worker_tid=226 cpu_id=28 bthread_id=100 "
            "fn=0x1 arg=0x2 completion_time_ns=3 execution_time_us=180 "
            "lifetime_time_us=3980\n",
            encoding="utf-8")

    def test_server_side_bpf_recovered(self):
        disc, contexts, _tl = nla.analyze(str(self._root), window_pad_ms=2)
        ctx = contexts[0]
        # env + tar.gz：server pod → 宿主机 worker12 → bpf 事件恢复关联
        self.assertEqual(ctx.server_node, "worker12")
        evs = ctx.kernel_events.get("server") or []
        self.assertTrue(evs, "server 侧 bpf 事件应恢复解析")
        kinds = {e["kind"] for e in evs}
        self.assertIn("tcp_recv_in", kinds)
        # client 节点无 bpf（SDK 直连）→ server 侧回退识别连接（收包事件优先）
        self.assertEqual(ctx.conn, (self.CIP, 53896, self.SIP, 31402))
        # 回退成功后，"无法映射"告警改写为回退说明
        self.assertIn("已从 server 侧 bpf 事件回退识别连接五元组",
                      " | ".join(ctx.missing))
        self.assertTrue(ctx.filtered_events["server"])
        self.assertIn("ServerTcpSendIn", ctx.milestones)
        # brpc 兜底关联：server 侧 bthread 事件（窗口内、tid 226）
        bev = ctx.bthread_events.get("server") or []
        self.assertEqual(len(bev), 2)
        self.assertEqual(bev[1]["kind"], "completed")
        self.assertEqual(bev[1]["execution_time_us"], 180)
        joined = " | ".join(ctx.coro_evidence)
        self.assertIn("execution_time_us 峰值 180us", joined)
        # client 侧无 bpf 文件 → 降级为空，不报错
        self.assertFalse(ctx.kernel_events.get("client"))


class TestSoftirqLocBanner(TestSoftirqWireLocalization):
    """softirq 定位结论的醒目呈现。

    - trace 头部红色 badge（根因已定位，未展开卡片即可见）；
    - trace 卡顶部红色高亮横幅（根因已定位 —— 收包慢直接定界，
      含占用任务/cpu/延迟/完整调用栈），位于定界结论块之前；
    - 概览索引中带"已定位"标记；
    - 未命中定位时不渲染横幅/badge。
    """

    def _report(self, **kw):
        import argparse
        root = self._build_wire(**kw)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        _disc, contexts, _tl = nla.analyze(str(root))
        ns = argparse.Namespace(residual_threshold=1000)
        return contexts[0], nla.generate_report(contexts, ns, str(root))

    def test_banner_and_badge(self):
        _ctx, h = self._report()
        # trace 头部 badge（在 trace-body 之前，未展开即可见）
        self.assertIn('class="badge b-loc"', h)
        self.assertLess(h.find('class="badge b-loc"'), h.find('class="trace-body"'))
        # trace 卡顶部红色高亮横幅，位于定界结论块之前
        self.assertIn('class="loc-banner"', h)
        self.assertLess(h.find('class="loc-banner"'), h.find('class="concl"'))
        self.assertIn("根因已定位", h)
        self.assertIn("收包慢直接定界", h)
        self.assertIn("ubctl", h)
        self.assertIn("5044", h)
        # 横幅内含完整调用栈
        self.assertIn("ubctl_query_dl_pkt_stats_data", h)
        # 概览索引（第一个 trace 卡之前）带"已定位"标记
        self.assertIn('class="badge b-loc"', h[:h.find('class="card trace-card"')])

    def test_banner_absent_without_localization(self):
        # vec=7（非 NET_RX）→ 不定位 → 无横幅/badge
        _ctx, h = self._report(vec=7)
        self.assertNotIn('class="loc-banner"', h)
        self.assertNotIn('class="badge b-loc"', h)


class TestDirAgnosticDiscovery(unittest.TestCase):
    """目录名无关的日志发现：按文件名模式 + 内容嗅探分类。

    采集目录名（collected/collected_worker_logs/dscollect_log/
    latency_warn_log 及 pod 目录名）后续可能变化，发现逻辑不依赖目录名：
    - 已知文件名模式：ds_client*（client）/ kvcache*（worker）/
      bpf-<node>-<ip>.log / irqoff_latency_<ip>.log / nic-<ip>.log /
      *-brpc*.log / <node>_<ip>（warn）/ env / *.tar.gz；
    - 未知名 *.log 按内容嗅探：ClientSend/ClientRecv/慢请求行 → client，
      ServerRecv/ServerSend → worker，无标记 → 跳过。
    """

    def _write(self, path, text=""):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def test_arbitrary_dir_names(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="diragn_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        # 任意命名的目录（全部非标准目录名）
        self._write(root / "app_client" / "podA" / "ds_client_1.INFO.1.log",
                    "2026-08-22T10:00:00.100000 | I | a.cc:1 | 10.0.0.1 | 1:100 | "
                    "t1;a |  |  yyl1 ClientSend ts 100000000000 tid 100 cpu 1\n")
        self._write(root / "app_worker" / "podB" / "kvcache.INFO.1.log",
                    "2026-08-22T10:00:00.110000 | I | b.cc:1 | 10.0.0.2 | 2:200 | "
                    "t1;a |  |  yyl1 ServerRecv ts 100000110000 tid 200 cpu 2\n")
        self._write(root / "kern" / "bpf-master-1.2.3.4.log")
        self._write(root / "kern" / "bpf-worker1-5.6.7.8.log")
        self._write(root / "warns" / "master_1.2.3.4")
        self._write(root / "warns" / "worker1_5.6.7.8")
        self._write(root / "app_worker" / "podB" / "env",
                    "pod_ip=10.0.0.2\nJD_HOST_IP=5.6.7.8\n")
        self._write(root / "app_worker" / "podB" /
                    "kvworker-0-worker1-brpc_client.log")
        disc = nla.LogDiscovery(str(root))
        self.assertEqual([p.name for p in disc.client_logs],
                         ["ds_client_1.INFO.1.log"])
        self.assertEqual([p.name for p in disc.worker_logs],
                         ["kvcache.INFO.1.log"])
        self.assertIn("master", disc.bpf_by_node)
        self.assertIn("worker1", disc.bpf_by_node)
        self.assertIn("master", disc.warn_by_node)
        self.assertIn("worker1", disc.warn_by_node)
        self.assertEqual(disc.host_by_podip.get("10.0.0.2"), "5.6.7.8")
        self.assertIn("kvworker-0-worker1", disc.brpc_by_pod)

    def test_content_sniff_unknown_log_names(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="sniff_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        # 未知名 *.log：按内容嗅探判定角色
        self._write(root / "x" / "aaa.log",
                    "2026-08-22T10:00:00.100000 | I | a.cc:1 | 10.0.0.1 | 1:1 | "
                    "t1;a |  |  yyl1 ClientSend ts 100000000000 tid 1 cpu 1\n"
                    "2026-08-22T10:00:00.200000 | I | a.cc:1 | 10.0.0.1 | 1:1 | "
                    "t1;a |  |  yyl1 ClientRecv ts 100000200000 tid 1 cpu 1\n")
        self._write(root / "y" / "bbb.log",
                    "2026-08-22T10:00:00.110000 | I | b.cc:1 | 10.0.0.2 | 2:2 | "
                    "t1;a |  |  yyl1 ServerRecv ts 100000110000 tid 2 cpu 2\n")
        self._write(root / "z" / "ccc.log", "nothing relevant here\n")
        self._write(root / "z" / "ddd.log",
                    "2026-08-22T10:00:00.300000 | I | a.cc:1 | 10.0.0.1 | 1:1 | "
                    "t1;a |  |  [BRPC_RPC_FRAMEWORK_SLOW] xxx\n")
        # 工具自身的 --raw 输出（含锚点/慢请求行，开头为 "="*80 分隔线）
        # 不应被再次当作 client 日志吸入（输出落在日志根目录时的自污染防护）
        self._write(root / "z" / "self_output.log",
                    "=" * 80 + "\n"
                    "#1 trace=t1;a  residual=5000us\n"
                    "结论：xxx\n"
                    "2026-08-22T10:00:00.100000 | I | a.cc:1 | 10.0.0.1 | 1:1 | "
                    "t1;a |  |  yyl1 ClientSend ts 100000000000 tid 1 cpu 1\n")
        disc = nla.LogDiscovery(str(root))
        self.assertEqual([p.name for p in disc.client_logs],
                         ["aaa.log", "ddd.log"])
        self.assertEqual([p.name for p in disc.worker_logs], ["bbb.log"])

    def test_end_to_end_renamed_dirs(self):
        # 标准 wire 布局 + 顶层目录全部重命名 → 全链路分析不受影响
        fixture = TestSoftirqWireLocalization()
        root = fixture._build_wire()
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        for old, new in (("collected", "clientlogs_x"),
                         ("collected_worker_logs", "workerlogs_y"),
                         ("dscollect_log", "kern_z"),
                         ("latency_warn_log", "warns_w")):
            (root / old).rename(root / new)
        _disc, contexts, _tl = nla.analyze(str(root))
        self.assertEqual(len(contexts), 1)
        ctx = contexts[0]
        # softirq wire 定位照常命中（目录名无关）
        loc = ctx.softirq_localization.get("client")
        self.assertIsNotNone(loc)
        self.assertEqual(loc["comm"], "ubctl")
        self.assertEqual(loc["cpu"], 44)
        self.assertEqual(loc["latency_us"], 5044)


class TestNodeProbeFallback(TestSoftirqWireLocalization):
    """pod 目录名不可识别（无节点子串/IP/env/Host ID）时的 bpf 探测兜底。

    目录名全改后 resolve_node 失败：pod IP 作为 local_ip 只出现在 pod
    所在节点的 bpf 日志（对端节点上它是 peer_ip）——用 trace 窗口探测
    各节点 bpf 文件，命中 local_ip==podIP 的节点回填为该侧节点，
    client/server 两侧独立探测，bpf 关联与 softirq 定位照常。
    """

    def test_probe_resolves_nodes(self):
        root = self._build_wire()
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True)
                        if root.exists() else None)
        # pod 目录改成无节点子串、无 IP 的任意名 → resolve_node 失败
        (root / "collected" / "kvclient-1-master_26").rename(
            root / "collected" / "目录甲")
        (root / "collected_worker_logs" / "kvworker-0-worker1").rename(
            root / "collected_worker_logs" / "目录乙")
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        # 节点经 bpf 探测识别（client→master / server→worker1）
        self.assertEqual(ctx.client_node, "master")
        self.assertEqual(ctx.server_node, "worker1")
        # bpf 关联照常（两侧窗口事件非空）
        self.assertTrue(ctx.kernel_events["client"])
        self.assertTrue(ctx.kernel_events["server"])
        # softirq wire 定位照常命中（目录名无关）
        loc = ctx.softirq_localization.get("client")
        self.assertIsNotNone(loc)
        self.assertEqual(loc["comm"], "ubctl")

    def test_probe_negative_wrong_node_ip_absent(self):
        # bpf 日志窗口内无 local_ip==podIP 的事件 → 探测失败，节点保持
        # None（不误绑到别的节点），走既有 missing 诊断路径
        root = self._build_wire()
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True)
                        if root.exists() else None)
        (root / "collected" / "kvclient-1-master_26").rename(
            root / "collected" / "目录甲")
        # master 的 bpf 日志清空（只剩无 IP 的调度事件）→ client 探测无命中
        (root / "dscollect_log" / "bpf-master-192.168.219.1.log").write_text(
            "21:31:21:060770 sched_switch prev_comm=x next_comm=y\n",
            encoding="utf-8")
        _disc, contexts, _tl = nla.analyze(str(root))
        ctx = contexts[0]
        self.assertIsNone(ctx.client_node)
        self.assertFalse(ctx.kernel_events["client"])


class TestNumaLogParse(unittest.TestCase):
    """NUMA 访存监控三类日志解析（numafast / memory / perf）。

    样例取自 /home/wcy/minilog/dscollect_log/<ip>-<ip>-data_*/：
      - numafast：NUMAFAST Report-N 块（score + NID 表）；
      - memory：Memory Summary Report-N 块（cache miss% + DDR 带宽）；
      - perf：perf stat round N 块（dTLB/iTLB-load-misses，1s/轮）。
    """

    NUMAFAST_SAMPLE = """================================================================================
Version     : DevKit 26.0.RC1
CPU Model   : Kunpeng 950 7592C To Be Filled By O.E.M. CPU @ 2.3GHz
Command     : devkit tuner numafast -d N -i 1 -n 30 -t 5
================================================================================

NUMAFAST ANALYSIS(Press Ctrl+C or Ctrl+\\ to exit and generate the summary report)

NUMAFAST Report-1(Press Ctrl+C or Ctrl+\\ to exit)                     Time:20260910-202917
==========================================================================================
1. System's numa score : 0.84

              DST_0               DST_1               DST_2               DST_3
SRC_0   0.12GB|10|78.71%    0.00GB|15|0.00%     0.00GB|20|0.00%     0.00GB|20|0.00%
SRC_1   0.00GB|15|0.00%     0.00GB|10|0.00%     0.00GB|20|0.00%     0.02GB|20|14.19%

==========================================================================================
2. System node detail information of memory access traffic:

 NID  RMA_Die  RMA_Skt      LMA    %RMA   MEM_all  MEM_free   %MEM      %CPU
   0   0.00GB   0.00GB   0.12GB    0.00  163.05GB    4.46GB  97.26    158.73
   1   0.00GB   0.02GB   0.00GB  100.00  201.02GB   16.54GB  91.77     38.21

==========================================================================================
3. Show top 1 processes and top 5 threads which sorted by memory access:

 PID(TID)  SCORE  ACCESS  RMA_Die  RMA_Skt      LMA    %RMA  MIGRATED    %CPU    COMMAND
   6381     0.84 100.00%   0.01GB   0.02GB   0.13GB   17.74    0|7        --     containerd
├─3107874   1.00  78.71%   0.00GB   0.00GB   0.12GB    0.00    -|-        --     containerd

==========================================================================================

NUMAFAST Report-2(Press Ctrl+C or Ctrl+\\ to exit)                     Time:20260910-202919
==========================================================================================
1. System's numa score : 0.28

 NID  RMA_Die  RMA_Skt      LMA    %RMA   MEM_all  MEM_free   %MEM      %CPU
   0   0.00GB   0.00GB   0.00GB    0.00  163.05GB    3.65GB  97.76    147.47
   3   0.00GB   0.22GB   0.09GB   71.88  166.45GB   26.08GB  84.33    245.92
"""

    MEMORY_SAMPLE = """================================================================================
Version     : DevKit 26.0.RC1
Command     : devkit tuner memory -d N -i 1 -P 100 -m 1
================================================================================
Memory Summary Report-1                                 Time:2026/09/10 20:29:17
================================================================================

System Information
--------------------------------------------------------------------------------
Linux Kernel Version  6.6.0-159.4.1.151.oe2403sp4+64k.aarch64
NUMA NODE(cpus)       0(0-83)    1(84-153)  2(154-223) 3(224-307)

Percentage of core Cache miss
--------------------------------------------------------------------------------
L1D         1.23%
L1I         0.00%
L2D         0.57%
L2I        16.25%


DDR Bandwidth (system wide)
--------------------------------------------------------------------------------
ddrc_write        1258.20MB/s
ddrc_read          214.49MB/s

Memory metrics of the Cache
--------------------------------------------------------------------------------
1. L1/L2/TLB Access Bandwidth and Hit Rate
Value Format: X|Y = Bandwidth | Hit Rate
--------------------------------------------------------------------------------
  CPU                       L1D                        L1I                     L2D                  L2I       L2D_TLB       L2I_TLB
--------------------------------------------------------------------------------
  all    18413958.00MB/s|98.77%    18751420.00MB/s|100.00%    454447.88MB/s|99.43%    727.05MB/s|83.75%    N/A|85.53%    N/A|94.57%

2. L3 Read Bandwidth and Hit Rate
--------------------------------------------------------------------------------
  NODE    CCL     Read Hit Bandwidth    Read Bandwidth    Read Hit Rate
--------------------------------------------------------------------------------
  0       --              691.08MB/s       1027.04MB/s           67.29%
  0       0               174.83MB/s        233.23MB/s           74.96%
  1       --              124.49MB/s        201.00MB/s           61.93%

Memory Summary Report-2                                 Time:2026/09/10 20:29:18
================================================================================

Percentage of core Cache miss
--------------------------------------------------------------------------------
L1D         2.34%
L1I         0.01%
L2D         0.61%
L2I        17.25%

DDR Bandwidth (system wide)
--------------------------------------------------------------------------------
ddrc_write         987.60MB/s
ddrc_read          301.05MB/s
"""

    PERF_SAMPLE = """==== perf 采集开始 2026-09-11 10:03:25 循环=3600次 间隔=1s ====
==== 事件: ummu_pmcg_0/tbu_tlb_cache_hit_rate/,ummu_pmcg_1/tbu_tlb_cache_hit_rate/,dTLB-load-misses,iTLB-load-misses ====

==== perf stat round 1/3600 开始时间 2026-09-11 10:03:25 ====

 Performance counter stats for 'system wide':

                 0      ummu_pmcg_0/tbu_tlb_cache_hit_rate/
                 0      ummu_pmcg_1/tbu_tlb_cache_hit_rate/
         6,314,507      dTLB-load-misses                                      (91.45%)
            52,128      iTLB-load-misses                                      (91.81%)

       1.024003390 seconds time elapsed

==== perf stat round 1/3600 结束时间 2026-09-11 10:03:26 ====

==== perf stat round 2/3600 开始时间 2026-09-11 10:03:26 ====

 Performance counter stats for 'system wide':

                 0      ummu_pmcg_0/tbu_tlb_cache_hit_rate/
         5,880,377      dTLB-load-misses                                      (19.03%)
           551,634      iTLB-load-misses                                      (18.92%)

       1.044025930 seconds time elapsed

==== perf stat round 2/3600 结束时间 2026-09-11 10:03:27 ====
"""

    def setUp(self):
        import shutil
        self._root = Path(tempfile.mkdtemp(prefix="numaparse_"))
        self.addCleanup(shutil.rmtree, self._root, ignore_errors=True)

    def _write(self, name, text):
        p = self._root / name
        p.write_text(text, encoding="utf-8")
        return p

    def test_parse_numafast(self):
        recs = nla.parse_numafast_log(self._write("numafast_x.log",
                                                  self.NUMAFAST_SAMPLE))
        self.assertEqual(len(recs), 2)
        r1 = recs[0]
        self.assertEqual(r1["ts"].strftime("%Y%m%d-%H%M%S"), "20260910-202917")
        self.assertAlmostEqual(r1["score"], 0.84)
        self.assertEqual(r1["nids"]["0"]["rma_pct"], 0.0)
        self.assertEqual(r1["nids"]["1"]["rma_pct"], 100.0)
        self.assertAlmostEqual(r1["nids"]["0"]["cpu_pct"], 158.73)
        self.assertAlmostEqual(r1["nids"]["1"]["mem_pct"], 91.77)
        # PID 表行不误入 NID 表
        self.assertNotIn("6381", r1["nids"])
        r2 = recs[1]
        self.assertAlmostEqual(r2["score"], 0.28)
        self.assertEqual(r2["nids"]["3"]["rma_pct"], 71.88)

    def test_parse_memory(self):
        recs = nla.parse_memory_log(self._write("memory_x.log",
                                                self.MEMORY_SAMPLE))
        self.assertEqual(len(recs), 2)
        r1 = recs[0]
        self.assertEqual(r1["ts"].strftime("%Y/%m/%d %H:%M:%S"),
                         "2026/09/10 20:29:17")
        self.assertAlmostEqual(r1["l1d_miss_pct"], 1.23)
        self.assertAlmostEqual(r1["l2i_miss_pct"], 16.25)
        self.assertAlmostEqual(r1["ddrc_write_mb_s"], 1258.20)
        self.assertAlmostEqual(r1["ddrc_read_mb_s"], 214.49)
        self.assertAlmostEqual(recs[1]["l1d_miss_pct"], 2.34)
        self.assertAlmostEqual(recs[1]["ddrc_write_mb_s"], 987.60)

    def test_parse_memory_cache_l3(self):
        """Memory metrics of the Cache（L1/L2/TLB 带宽+命中率）与
        L3 Read Bandwidth / Hit Rate（NODE 汇总行，CCL 明细行不取）。
        """
        recs = nla.parse_memory_log(self._write("memory_c.log",
                                                self.MEMORY_SAMPLE))
        r1 = recs[0]
        # L1/L2/TLB Access Bandwidth and Hit Rate（all 行，X|Y 六列）
        self.assertAlmostEqual(r1["l1d_bw_mb_s"], 18413958.00)
        self.assertAlmostEqual(r1["l1d_hit_pct"], 98.77)
        self.assertAlmostEqual(r1["l2d_bw_mb_s"], 454447.88)
        self.assertAlmostEqual(r1["l2i_hit_pct"], 83.75)
        # TLB 带宽 N/A → None；命中率照常
        self.assertIsNone(r1["l2dtlb_bw_mb_s"])
        self.assertAlmostEqual(r1["l2dtlb_hit_pct"], 85.53)
        self.assertAlmostEqual(r1["l2itlb_hit_pct"], 94.57)
        # L3 Read（仅 NODE 汇总行 CCL=--；CCL 明细行不解析）
        self.assertAlmostEqual(r1["l3_nid0_hit_bw_mb_s"], 691.08)
        self.assertAlmostEqual(r1["l3_nid0_read_bw_mb_s"], 1027.04)
        self.assertAlmostEqual(r1["l3_nid0_hit_pct"], 67.29)
        self.assertAlmostEqual(r1["l3_nid1_hit_pct"], 61.93)
        self.assertNotIn("l3_nid0_ccl0_read_bw_mb_s", r1)
        # report-2 无 cache/L3 段 → 字段缺失（不误留上一轮值）
        self.assertNotIn("l1d_bw_mb_s", recs[1])
        self.assertNotIn("l3_nid0_hit_pct", recs[1])

    def test_parse_numafast_procs(self):
        """numafast 第 3 章 top 进程（按访存排序）：只解析进程行，
        线程行（├─/└─ 树前缀）不解析；%CPU "--" → None。
        """
        recs = nla.parse_numafast_log(self._write("numafast_p.log",
                                                  self.NUMAFAST_SAMPLE))
        r1 = recs[0]
        self.assertEqual(len(r1["procs"]), 1)
        p = r1["procs"][0]
        self.assertEqual(p["pid"], 6381)
        self.assertAlmostEqual(p["score"], 0.84)
        self.assertAlmostEqual(p["access_pct"], 100.0)
        self.assertAlmostEqual(p["rma_die_gb"], 0.01)
        self.assertAlmostEqual(p["lma_gb"], 0.13)
        self.assertAlmostEqual(p["rma_pct"], 17.74)
        self.assertEqual(p["migrated"], "0|7")
        self.assertIsNone(p["cpu_pct"])          # 首轮 %CPU 为 "--"
        self.assertEqual(p["command"], "containerd")
        # 线程行（├─3107874 …）不进 procs
        self.assertNotEqual(p["pid"], 3107874)
        # report-2 无第 3 章 → procs 为空列表
        self.assertEqual(recs[1]["procs"], [])

    def test_parse_numafast_proc_cpu_value(self):
        """进程行 %CPU 有值时正常解析（如 java 0.00）。"""
        recs = nla.parse_numafast_log(self._write("numafast_pc.log", (
            "NUMAFAST Report-1(x)                     Time:20260910-202917\n"
            " PID(TID)  SCORE  ACCESS  RMA_Die  RMA_Skt      LMA    %RMA"
            "  MIGRATED    %CPU    COMMAND\n"
            "2712305     0.28 100.00%   0.00GB   0.22GB   0.09GB   71.88"
            "    0|3       0.00    java\n")))
        p = recs[0]["procs"][0]
        self.assertEqual(p["pid"], 2712305)
        self.assertAlmostEqual(p["cpu_pct"], 0.00)
        self.assertEqual(p["command"], "java")

    def test_parse_numafast_matrix(self):
        """第 1 章访存矩阵：DST 表头 + SRC 行（traffic|distance|access%）。
        键 "s_d"（SRC→DST）；report 无矩阵段 → 无 matrix 键。
        """
        recs = nla.parse_numafast_log(self._write("numafast_m.log",
                                                  self.NUMAFAST_SAMPLE))
        r1 = recs[0]
        # 对角线本地访存 SRC_0→DST_0
        self.assertAlmostEqual(r1["matrix"]["0_0"]["gb"], 0.12)
        self.assertEqual(r1["matrix"]["0_0"]["dist"], 10)
        self.assertAlmostEqual(r1["matrix"]["0_0"]["pct"], 78.71)
        # 远程访存 SRC_1→DST_3
        self.assertAlmostEqual(r1["matrix"]["1_3"]["gb"], 0.02)
        self.assertEqual(r1["matrix"]["1_3"]["dist"], 20)
        self.assertAlmostEqual(r1["matrix"]["1_3"]["pct"], 14.19)
        # 全 4×4 单元格都解析
        self.assertEqual(len(r1["matrix"]), 8)   # 2 SRC 行 × 4 DST 列
        # report-2 无矩阵段 → 无 matrix 键
        self.assertNotIn("matrix", recs[1])

    def test_parse_perf(self):
        recs = nla.parse_perf_log(self._write("perf_x.log", self.PERF_SAMPLE))
        self.assertEqual(len(recs), 2)
        r1 = recs[0]
        self.assertEqual(r1["ts"].strftime("%Y-%m-%d %H:%M:%S"),
                         "2026-09-11 10:03:25")
        self.assertEqual(r1["dtlb_load_misses"], 6314507)
        self.assertEqual(r1["itlb_load_misses"], 52128)
        # ummu_pmcg TBU TLB 命中率（真实样例恒 0，照常解析）
        self.assertEqual(r1["ummu_pmcg_0_tlb_hit_rate"], 0)
        self.assertEqual(r1["ummu_pmcg_1_tlb_hit_rate"], 0)
        # round 2 缺 ummu_pmcg_1 行 → None（数据缺失断线）
        self.assertEqual(recs[1]["dtlb_load_misses"], 5880377)
        self.assertEqual(recs[1]["itlb_load_misses"], 551634)
        self.assertEqual(recs[1]["ummu_pmcg_0_tlb_hit_rate"], 0)
        self.assertIsNone(recs[1]["ummu_pmcg_1_tlb_hit_rate"])

    def test_parse_perf_ummu_nonzero(self):
        """ummu_pmcg 非零值 + 千分位逗号 + 行尾空格。"""
        recs = nla.parse_perf_log(self._write("perf_nz.log", (
            "==== perf stat round 1/3600 开始时间 2026-09-11 10:03:25 ====\n"
            "             1,234      ummu_pmcg_0/tbu_tlb_cache_hit_rate/    \n"
            "                56      ummu_pmcg_1/tbu_tlb_cache_hit_rate/\n"
            "         6,314,507      dTLB-load-misses                    (91.45%)\n")))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["ummu_pmcg_0_tlb_hit_rate"], 1234)
        self.assertEqual(recs[0]["ummu_pmcg_1_tlb_hit_rate"], 56)

    def test_parse_perf_dynamic_events(self):
        """perf stat 事件持续增加：事件头目录 + 通用数据行动态识别。

        真实样例（141.61.91.189 perf_20260914-152704.log）：事件头列出
        dTLB-loads / dTLB-load-misses / ummu_pmcg_{0,1}/tcu_cntx_cache_miss_num/
        / tcu_pptw_req_num/ / tbu_tlb_cache_hit_rate/ —— 全部事件必须进
        values（新增指标不丢失），已知事件旧字段照常填充，`#` 注释进
        comments，`(91.45%)` 缩放标注不误当注释。
        """
        sample = (
            "==== perf 采集开始 2026-09-14 15:27:04 循环=10次 间隔=1s ====\n"
            "==== 事件: dTLB-loads,dTLB-load-misses,"
            "ummu_pmcg_0/tcu_cntx_cache_miss_num/,"
            "ummu_pmcg_1/tcu_cntx_cache_miss_num/,"
            "ummu_pmcg_0/tcu_pptw_req_num/,ummu_pmcg_1/tcu_pptw_req_num/,"
            "ummu_pmcg_0/tbu_tlb_cache_hit_rate/,"
            "ummu_pmcg_1/tbu_tlb_cache_hit_rate/ ====\n"
            "\n"
            "==== perf stat round 1/10 开始时间 2026-09-14 15:27:04 ====\n"
            "\n"
            " Performance counter stats for 'system wide':\n"
            "\n"
            "     1,090,095,445      dTLB-loads\n"
            "           951,082      dTLB-load-misses                 "
            "#    0.09% of all dTLB cache accesses\n"
            "                 0      ummu_pmcg_0/tcu_cntx_cache_miss_num/\n"
            "                 0      ummu_pmcg_1/tcu_cntx_cache_miss_num/\n"
            "                 0      ummu_pmcg_0/tcu_pptw_req_num/\n"
            "                 0      ummu_pmcg_1/tcu_pptw_req_num/\n"
            "                 7      ummu_pmcg_0/tbu_tlb_cache_hit_rate/\n"
            "                 0      ummu_pmcg_1/tbu_tlb_cache_hit_rate/\n"
            "\n"
            "       1.006413289 seconds time elapsed\n"
            "\n"
            "==== perf stat round 1/10 结束时间 2026-09-14 15:27:05 ====\n")
        recs = nla.parse_perf_log(self._write("perf_dyn.log", sample))
        self.assertEqual(len(recs), 1)
        r1 = recs[0]
        # 事件头列出的全部事件动态识别进 values（含新增的
        # tcu_cntx_cache_miss_num / tcu_pptw_req_num 与 dTLB-loads）
        self.assertEqual(r1["values"]["dTLB-loads"], 1090095445)
        self.assertEqual(r1["values"]["dTLB-load-misses"], 951082)
        self.assertEqual(
            r1["values"]["ummu_pmcg_0/tcu_cntx_cache_miss_num/"], 0)
        self.assertEqual(
            r1["values"]["ummu_pmcg_1/tcu_pptw_req_num/"], 0)
        self.assertEqual(r1["values"]["ummu_pmcg_0/tbu_tlb_cache_hit_rate/"], 7)
        self.assertEqual(r1["values"]["ummu_pmcg_1/tbu_tlb_cache_hit_rate/"], 0)
        self.assertEqual(len(r1["values"]), 8)   # 事件头全部 8 个事件
        # `#` 注释进 comments；(91.45%) 缩放标注不产生注释
        self.assertEqual(r1["comments"]["dTLB-load-misses"],
                         "0.09% of all dTLB cache accesses")
        self.assertNotIn("dTLB-loads", r1["comments"])
        # 已知事件旧字段照常填充（向后兼容）
        self.assertEqual(r1["dtlb_load_misses"], 951082)
        self.assertEqual(r1["ummu_pmcg_0_tlb_hit_rate"], 7)
        # "1.006413289 seconds time elapsed" 不误入 values
        self.assertNotIn("seconds", " ".join(r1["values"]))

    def test_parse_perf_dynamic_no_header(self):
        """无事件头（老格式）时通用数据行照常识别（不依赖事件目录）。"""
        recs = nla.parse_perf_log(self._write("perf_nohdr.log", (
            "==== perf stat round 1/3600 开始时间 2026-09-11 10:03:25 ====\n"
            "     1,090,095,445      dTLB-loads\n"
            "           951,082      dTLB-load-misses\n"
            "       1.006413289 seconds time elapsed\n")))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["values"]["dTLB-loads"], 1090095445)
        self.assertEqual(recs[0]["values"]["dTLB-load-misses"], 951082)
        self.assertNotIn("seconds", " ".join(recs[0]["values"]))


class TestNumaDiscovery(unittest.TestCase):
    """NUMA 监控日志发现：numafast_/memory_/perf_*.log 任意目录，
    父目录名 IP → 节点（node_by_ip 命中用规范名，未命中以 IP 为键）。
    """

    def setUp(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="numadisc_"))
        self._root = root
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        (root / "dscollect_log").mkdir()

    def _write_numa_logs(self, parent):
        d = self._root / "dscollect_log" / parent
        d.mkdir(parents=True, exist_ok=True)
        (d / "numafast_20260911-100325.log").write_text(
            "NUMAFAST Report-1(x)                     Time:20260910-202917\n"
            "1. System's numa score : 0.84\n", encoding="utf-8")
        (d / "memory_20260911-100325.log").write_text(
            "Memory Summary Report-1                                 "
            "Time:2026/09/10 20:29:17\n"
            "L1D         1.23%\nddrc_write        1258.20MB/s\n",
            encoding="utf-8")
        (d / "perf_20260911-100325.log").write_text(
            "==== perf stat round 1/3600 开始时间 2026-09-11 10:03:25 ====\n"
            "         6,314,507      dTLB-load-misses\n", encoding="utf-8")

    def test_known_node_ip_maps_to_canonical(self):
        # 父目录 IP 141.62.32.59 对应 bpf-worker12 → 归到规范节点 worker12
        (self._root / "dscollect_log" / "bpf-worker12-141.62.32.59.log").write_text(
            "", encoding="utf-8")
        self._write_numa_logs("141.62.32.59-141.62.32.59-data_20260911-100322")
        disc = nla.LogDiscovery(str(self._root))
        self.assertIn("worker12", disc.numa_by_node)
        entry = disc.numa_by_node["worker12"]
        self.assertIn("numafast", entry)
        self.assertIn("memory", entry)
        self.assertIn("perf", entry)

    def test_unknown_ip_keyed_by_ip(self):
        # IP 不在任何已知节点 → 以 IP 为键独立呈现
        self._write_numa_logs("141.61.91.189-141.61.91.189-data_20260911-100322")
        disc = nla.LogDiscovery(str(self._root))
        self.assertIn("141.61.91.189", disc.numa_by_node)

    def test_arbitrary_dir_name_with_ip_still_found(self):
        # 目录名改掉（保留 IP）→ 照常发现（目录名无关约定）
        self._write_numa_logs("随便改名-141.61.91.189")
        disc = nla.LogDiscovery(str(self._root))
        self.assertIn("141.61.91.189", disc.numa_by_node)

    def test_no_ip_dir_skipped(self):
        # 父目录无 IP → 跳过（无节点身份来源）
        self._write_numa_logs("no_ip_here")
        disc = nla.LogDiscovery(str(self._root))
        self.assertEqual(disc.numa_by_node, {})


class TestNumaOverviewHtml(unittest.TestCase):
    """NUMA 指标交互探索器：aux_stats["numa"] → _numa_explorer_html
    （单图 + 节点选择 + 指标勾选 + 悬浮透明提示窗 + 拖拽时间段缩放，
    原生 JS 离线自包含）+ generate_json 的 numa_stats。
    """

    @staticmethod
    def _aux_stats():
        from datetime import datetime as _dt
        return {"numa": {
            "141.61.91.189": {
                "numafast": [
                    {"ts": _dt(2026, 9, 10, 20, 29, 17), "score": 0.84,
                     "nids": {"0": {"rma_pct": 0.0, "cpu_pct": 158.73,
                                    "mem_pct": 97.26, "rma_die_gb": 0.0,
                                    "rma_skt_gb": 0.0, "lma_gb": 0.12,
                                    "mem_all_gb": 163.05,
                                    "mem_free_gb": 4.46},
                              "1": {"rma_pct": 100.0, "cpu_pct": 38.21,
                                    "mem_pct": 91.77, "rma_die_gb": 0.0,
                                    "rma_skt_gb": 0.02, "lma_gb": 0.0,
                                    "mem_all_gb": 201.02,
                                    "mem_free_gb": 16.54}},
                     "procs": [
                         {"pid": 6381, "command": "containerd",
                          "score": 0.84, "access_pct": 100.0,
                          "rma_die_gb": 0.01, "rma_skt_gb": 0.02,
                          "lma_gb": 0.13, "rma_pct": 17.74,
                          "migrated": "0|7", "cpu_pct": None}],
                     "matrix": {"0_0": {"gb": 0.12, "dist": 10,
                                        "pct": 78.71},
                                "1_3": {"gb": 0.02, "dist": 20,
                                        "pct": 14.19}}},
                    {"ts": _dt(2026, 9, 10, 20, 29, 19), "score": 0.28,
                     "nids": {"0": {"rma_pct": 0.0, "cpu_pct": 147.47,
                                    "mem_pct": 97.76, "rma_die_gb": 0.0,
                                    "rma_skt_gb": 0.0, "lma_gb": 0.0,
                                    "mem_all_gb": 163.05,
                                    "mem_free_gb": 3.65},
                              "3": {"rma_pct": 71.88, "cpu_pct": 245.92,
                                    "mem_pct": 84.33, "rma_die_gb": 0.0,
                                    "rma_skt_gb": 0.22, "lma_gb": 0.09,
                                    "mem_all_gb": 166.45,
                                    "mem_free_gb": 26.08}},
                     "procs": [
                         {"pid": 2712305, "command": "java",
                          "score": 0.28, "access_pct": 100.0,
                          "rma_die_gb": 0.0, "rma_skt_gb": 0.22,
                          "lma_gb": 0.09, "rma_pct": 71.88,
                          "migrated": "0|3", "cpu_pct": 0.0}],
                     "matrix": {"3_0": {"gb": 0.22, "dist": 20,
                                        "pct": 71.88}}},
                ],
                "memory": [
                    {"ts": _dt(2026, 9, 10, 20, 29, 17),
                     "l1d_miss_pct": 1.23, "l1i_miss_pct": 0.0,
                     "l2d_miss_pct": 0.57, "l2i_miss_pct": 16.25,
                     "ddrc_read_mb_s": 214.49, "ddrc_write_mb_s": 1258.20,
                     "l1d_bw_mb_s": 18413958.0, "l1d_hit_pct": 98.77,
                     "l1i_bw_mb_s": 18751420.0, "l1i_hit_pct": 100.0,
                     "l2d_bw_mb_s": 454447.88, "l2d_hit_pct": 99.43,
                     "l2i_bw_mb_s": 727.05, "l2i_hit_pct": 83.75,
                     "l2dtlb_bw_mb_s": None, "l2dtlb_hit_pct": 85.53,
                     "l2itlb_bw_mb_s": None, "l2itlb_hit_pct": 94.57,
                     "l3_nid0_hit_bw_mb_s": 691.08,
                     "l3_nid0_read_bw_mb_s": 1027.04,
                     "l3_nid0_hit_pct": 67.29,
                     "l3_nid1_hit_bw_mb_s": 124.49,
                     "l3_nid1_read_bw_mb_s": 201.00,
                     "l3_nid1_hit_pct": 61.93},
                ],
                "perf": [
                    {"ts": _dt(2026, 9, 11, 10, 3, 25),
                     "dtlb_load_misses": 6314507, "itlb_load_misses": 52128,
                     "ummu_pmcg_0_tlb_hit_rate": 1234,
                     "ummu_pmcg_1_tlb_hit_rate": 56},
                ],
            }}}

    def test_overview_html(self):
        h = nla._numa_explorer_html(self._aux_stats())
        # 卡片标题 + 来源说明
        self.assertIn("NUMA 访存监控", h)
        self.assertIn("141.61.91.189", h)
        # 节点下拉选择
        self.assertIn('id="numa-node-sel"', h)
        self.assertIn("<option", h)
        # 指标分组勾选（numafast / numafast top进程 / memory / L3 / perf）
        self.assertIn("numafast", h)
        self.assertIn("memory", h)
        self.assertIn("perf", h)
        self.assertIn("top进程", h)             # numafast 第 3 章进程指标分组
        self.assertIn("L3", h)                  # L3 读带宽/命中率分组
        self.assertIn("NUMA score(×100)", h)     # 指标 label
        self.assertIn("NID0 %RMA", h)
        self.assertIn("ddrc_write", h)
        self.assertIn("L2I", h)
        self.assertIn("dTLB", h)
        self.assertIn("TBU TLB 命中率", h)
        self.assertIn("ummu_pmcg_0", h)
        # numafast 全量指标（非常用的先隐藏，展开后勾选）
        self.assertIn("NID0 %CPU", h)
        self.assertIn("NID0 %MEM", h)
        self.assertIn("NID0 RMA_Skt", h)
        self.assertIn("NID0 MEM_free", h)
        # 常用/隐藏两级展示：common 标记（展开/收起交互由报告级工厂 JS 渲染）
        self.assertIn('"common"', h)
        # top 进程指标（ACCESS% 默认勾选；悬浮窗显示进程名 cmd=…）
        self.assertIn("top1 进程 ACCESS%", h)
        self.assertIn("top1 进程 %RMA", h)
        self.assertIn("cmd=containerd", h)
        self.assertIn("cmd=java", h)
        # memory cache 带宽 + 命中率、L3 读带宽/命中率
        self.assertIn("L1D 带宽", h)
        self.assertIn("L1D 命中率", h)
        self.assertIn("L2D_TLB 命中率", h)
        self.assertIn("NID0 L3 读带宽", h)
        self.assertIn("NID0 L3 命中率", h)
        # 嵌入数据（epoch + 数值）：<script type="application/json">
        self.assertIn('id="numa-data"', h)
        self.assertIn("1258.2", h)        # DDR 带宽值
        self.assertIn("6314507", h)       # dTLB 值
        self.assertIn("1234", h)          # ummu_pmcg_0 值
        self.assertIn("18413958", h)      # L1D 带宽值
        self.assertIn("1027.04", h)       # L3 NID0 读带宽值
        # 默认勾选：score + %RMA + top1 进程 ACCESS%（其余默认隐藏不勾选）
        import json as _json
        import re as _re
        m = _re.search(r'<script type="application/json" id="numa-data">'
                       r"(.*?)</script>", h, _re.S)
        data = _json.loads(m.group(1))
        self.assertIn("score", data["default"])
        self.assertIn("rma_0", data["default"])
        self.assertIn("proc_access", data["default"])
        self.assertNotIn("l1d_bw", data["default"])
        # common 标记：常用直接展示，非常用默认收起（xexp-more）
        self.assertEqual(data["metrics"]["score"]["common"], 1)
        self.assertEqual(data["metrics"]["rma_0"]["common"], 1)
        self.assertEqual(data["metrics"]["proc_access"]["common"], 1)
        self.assertEqual(data["metrics"]["cpu_0"]["common"], 0)
        self.assertEqual(data["metrics"]["l1d_bw"]["common"], 0)
        # top 进程序列点带 cmd= 附加信息（悬浮窗展示进程名）
        self.assertIn("cmd=containerd",
                      [p[2] for p in data["series"]["141.61.91.189"]
                       ["proc_access"]])
        # L3 序列嵌入（NID 汇总行）
        self.assertEqual(
            data["series"]["141.61.91.189"]["l3_0_read_bw"][0][1],
            1027.04)
        # 访存矩阵（SRC→DST，第 1 章）：分组 + 序列（含 traffic/dist 附加信息）
        self.assertIn("numafast 访存矩阵", h)
        self.assertIn("SRC0→DST0 访存%", h)
        self.assertIn("SRC1→DST3 访存%", h)
        self.assertEqual(
            data["series"]["141.61.91.189"]["mx_0_0"][0][2],
            "0.12GB|dist10")
        self.assertEqual(
            data["series"]["141.61.91.189"]["mx_3_0"][0][1], 71.88)
        self.assertEqual(data["metrics"]["mx_0_0"]["common"], 0)
        self.assertNotIn("mx_0_0", data["default"])
        # 展开修复 + 交互动能收敛到报告级工厂 JS（卡内只留 init 调用）：
        # JS 切换用 'block'（不能用 ''，否则 CSS display:none 生效导致
        # 展开后仍不可见），CSS 不再强制隐藏 xexp-more
        self.assertIn('id="numa-tooltip"', h)     # 悬浮提示容器
        self.assertIn("重置缩放", h)               # 缩放重置按钮
        self.assertIn("拖拽", h)                   # 操作提示文案
        self.assertIn("XEXP('numa')", h)          # 卡内 init 调用
        self.assertNotIn("<polyline", h)          # 不再服务端平铺渲染折线
        rep = nla.generate_os_monitor_report(self._aux_stats(), "/tmp/fake")
        self.assertIn("xexp-toggle", rep)         # 展开按钮（工厂 JS）
        self.assertIn("xexp-more", rep)           # 隐藏指标容器（默认收起）
        self.assertIn("'block' : 'none'", rep)    # 展开切换实现
        self.assertNotIn(".xexp-more{display:none", rep)
        self.assertIn("mousemove", rep)           # 悬停联动
        self.assertIn("mousedown", rep)           # 拖拽选择时间段
        # 分组批量选中/取消（组级复选框 + 部分勾选半选状态）
        self.assertIn("xexp-gcheck", rep)
        self.assertIn("indeterminate", rep)
        # 空 aux_stats / 无 numa → 空串（可选输入降级）
        self.assertEqual(nla._numa_explorer_html({}), "")
        self.assertEqual(nla._numa_explorer_html(None), "")

    def test_node_switch_keeps_selection(self):
        """交互探索器切换节点时记住已勾选指标，不重置为默认。

        NUMA 卡指标字段多，逐个勾选成本高；切节点对比各物理机时勾选
        应保留：
        - 节点切换调 buildMetrics(false)（保留勾选）而非 true（重置默认）；
        - buildMetrics 不做破坏性过滤（checked 是用户选择的完整记忆，
          切到无该指标的节点再切回，勾选恢复）；
        - render 按当前节点可用序列过滤渲染，新节点完全无交集时
          补默认勾选（并集，不丢原记忆）。
        """
        rep = nla.generate_os_monitor_report(self._aux_stats(), "/tmp/fake")
        # 节点切换保留勾选（resetDefault=false）
        self.assertIn("buildMetrics(false)", rep)
        self.assertNotIn("buildMetrics(true)", rep)
        # render 按当前节点可用序列过滤（checked 完整记忆不被裁剪）
        self.assertIn("checked.filter", rep)
        # 完全无交集时并集补默认（而非覆盖 checked 记忆）
        self.assertIn("checked.concat(defaultKeys(av))", rep)

    def test_overview_html_perf_dynamic(self):
        """perf stat 事件持续增加：报告动态识别新事件进 perf 分组。

        - 新事件（tcu_cntx_cache_miss_num / tcu_pptw_req_num / dTLB-loads）
          自动出现在勾选面板（默认收起，展开后勾选），序列值完整嵌入；
        - 已知事件沿用旧 key + 友好标签 + 默认展示；
        - 旧格式记录（无 values，仅旧字段）照常合成已知事件序列；
        - `#` 注释进序列点附加信息（悬浮窗展示）。
        """
        from datetime import datetime as _dt
        aux = self._aux_stats()
        aux["numa"]["141.61.91.189"]["perf"] = [
            {"ts": _dt(2026, 9, 14, 15, 27, 4),
             "values": {
                 "dTLB-loads": 1090095445,
                 "dTLB-load-misses": 951082,
                 "iTLB-load-misses": 52128,
                 "ummu_pmcg_0/tcu_cntx_cache_miss_num/": 3,
                 "ummu_pmcg_1/tcu_pptw_req_num/": 0,
                 "ummu_pmcg_0/tbu_tlb_cache_hit_rate/": 7,
                 "ummu_pmcg_1/tbu_tlb_cache_hit_rate/": 8},
             "comments": {"dTLB-load-misses":
                          "0.09% of all dTLB cache accesses"}},
            # 旧格式记录（无 values）→ 已知事件从旧字段合成序列
            {"ts": _dt(2026, 9, 14, 15, 27, 5),
             "dtlb_load_misses": 588037, "itlb_load_misses": 55163,
             "ummu_pmcg_0_tlb_hit_rate": 1234,
             "ummu_pmcg_1_tlb_hit_rate": 56}]
        h = nla._numa_explorer_html(aux)
        # 新事件自动进勾选面板；label 与原始指标名完全一致（含尾斜杠）
        self.assertIn("tcu_cntx_cache_miss_num", h)
        self.assertIn("tcu_pptw_req_num", h)
        self.assertIn("dTLB-loads", h)
        import json as _json
        import re as _re
        m = _re.search(r'<script type="application/json" id="numa-data">'
                       r"(.*?)</script>", h, _re.S)
        data = _json.loads(m.group(1))
        s = data["series"]["141.61.91.189"]
        # label 与原始指标名一致（含尾斜杠，不改写）
        self.assertEqual(
            data["metrics"]["perf_evt:dTLB-loads"]["label"], "dTLB-loads")
        self.assertEqual(
            data["metrics"]["perf_evt:ummu_pmcg_0/tcu_cntx_cache_miss_num/"]
            ["label"], "ummu_pmcg_0/tcu_cntx_cache_miss_num/")
        # 新事件序列完整嵌入（值不丢，含全 0 序列）
        self.assertEqual(s["perf_evt:dTLB-loads"][0][1], 1090095445)
        self.assertEqual(
            s["perf_evt:ummu_pmcg_0/tcu_cntx_cache_miss_num/"][0][1], 3)
        # 恒 0 指标也如实展示：序列点完整（0 值不过滤）
        self.assertEqual(
            s["perf_evt:ummu_pmcg_1/tcu_pptw_req_num/"],
            [[s["perf_evt:ummu_pmcg_1/tcu_pptw_req_num/"][0][0], 0]])
        self.assertIn("perf_evt:ummu_pmcg_1/tcu_pptw_req_num/",
                      data["metrics"])
        self.assertEqual(data["metrics"]["perf_evt:dTLB-loads"]["group"],
                         "perf")
        # 新事件默认收起（common=0，展开后勾选）
        self.assertEqual(data["metrics"]["perf_evt:dTLB-loads"]["common"], 0)
        self.assertEqual(
            data["metrics"]["perf_evt:ummu_pmcg_0/tcu_cntx_cache_miss_num/"]
            ["common"], 0)
        # 已知事件沿用旧 key + 默认展示；label 与原始指标名一致
        # （友好说明放括号后缀，原始名在前）
        self.assertEqual(data["metrics"]["dtlb"]["common"], 1)
        self.assertEqual(data["metrics"]["dtlb"]["label"], "dTLB-load-misses")
        self.assertEqual(data["metrics"]["ummu_0"]["label"],
                         "ummu_pmcg_0/tbu_tlb_cache_hit_rate/（TBU TLB 命中率）")
        self.assertEqual(s["dtlb"][0][1], 951082)
        self.assertEqual(s["dtlb"][1][1], 588037)   # 旧格式记录合成
        self.assertEqual(s["itlb"][1][1], 55163)
        self.assertEqual(s["ummu_0"][0][1], 7)
        self.assertEqual(s["ummu_0"][1][1], 1234)
        self.assertEqual(s["ummu_1"][1][1], 56)
        # `#` 注释进序列点附加信息（悬浮窗展示）；无注释点不带附加段
        self.assertEqual(s["dtlb"][0][2],
                         "0.09% of all dTLB cache accesses")
        self.assertEqual(s["dtlb"][1], [s["dtlb"][1][0], 588037])

    def test_json_numa_stats(self):
        import argparse
        import json
        ns = argparse.Namespace(residual_threshold=1000)
        doc = json.loads(nla.generate_json([], ns, "/tmp/fake_root",
                                           aux_stats=self._aux_stats()))
        numa = doc["numa_stats"]["141.61.91.189"]
        self.assertEqual(numa["numafast"][0]["score"], 0.84)
        self.assertEqual(numa["numafast"][0]["ts"],
                         "2026-09-10T20:29:17")
        self.assertEqual(numa["memory"][0]["ddrc_write_mb_s"], 1258.2)
        self.assertEqual(numa["perf"][0]["dtlb_load_misses"], 6314507)
        # top 进程（第 3 章）+ cache/L3 指标进 JSON
        self.assertEqual(numa["numafast"][0]["procs"][0]["command"],
                         "containerd")
        self.assertEqual(numa["memory"][0]["l3_nid0_read_bw_mb_s"], 1027.04)
        self.assertAlmostEqual(numa["memory"][0]["l1d_hit_pct"], 98.77)

    def test_report_places_numa_at_beginning(self):
        """NUMA 周期监控改为独立 OS 资源周期监控报告：
        原（定界）报告不再包含 NUMA 卡（聚焦问题请求定界）；
        irqoff/nic 概览保持在原报告（概览 TOC 之后，与 trace 定界联动）。
        """
        import argparse
        rec = nla.SlowRecord("t1", DAY, {"network_residual_us": "2000"},
                             "x.log", "pod")
        ctx = nla.TraceContext(rec)
        ctx.conclusion = {"label": "L", "confidence": "低"}
        ns = argparse.Namespace(residual_threshold=1000)
        aux = self._aux_stats()
        aux["irqoff"] = {"10.1.2.3": {"total": 1, "hardirq_n": 1,
                                      "softirq_n": 0, "max_us": 2000,
                                      "total_us": 2000, "buckets": {},
                                      "by_comm": {}, "series": []}}
        out = nla.generate_report([ctx], ns, "/tmp", aux_stats=aux)
        # 原（定界）报告不再体现周期监控（NUMA）
        self.assertNotIn("NUMA 访存监控", out)
        # irqoff 概览保持在概览 TOC 之后
        self.assertGreater(out.index("关中断"),
                           out.index("<h2>概览</h2>"))


class TestIrqoffExplorerHtml(unittest.TestCase):
    """关中断统计交互探索器：与 NUMA 访存监控同风格（单图 + 节点选择 +
    按进程（comm）序列勾选 + 组级批量选中/取消 + 悬浮透明提示窗 +
    拖拽时间段缩放），替代逐节点平铺散点，报告不再过长。
    """

    @staticmethod
    def _aux_stats():
        from datetime import datetime as _dt
        return {"irqoff": {
            "10.1.2.3": {
                "total": 3, "hardirq_n": 2, "softirq_n": 1,
                "max_us": 4200, "total_us": 7700,
                "buckets": {"1000": 3, "2000": 1, "4000": 1},
                "by_comm": {"kubelet": {"n": 2, "max_us": 2000,
                                        "total_us": 3500},
                            "svc": {"n": 1, "max_us": 4200,
                                    "total_us": 4200}},
                "series": [
                    [_dt(2026, 9, 11, 10, 0, 1), 2000, "kubelet", 4],
                    [_dt(2026, 9, 11, 10, 0, 5), 1500, "kubelet", 7],
                    [_dt(2026, 9, 11, 10, 0, 9), 4200, "svc", 9]]},
            "10.1.2.4": {
                "total": 1, "hardirq_n": 1, "softirq_n": 0,
                "max_us": 1200, "total_us": 1200,
                "buckets": {"1000": 1},
                "by_comm": {"kubelet": {"n": 1, "max_us": 1200,
                                        "total_us": 1200}},
                "series": [[_dt(2026, 9, 11, 10, 1, 2), 1200,
                            "kubelet", 2]]}}}

    def test_explorer_html(self):
        h = nla._irqoff_explorer_html(self._aux_stats())
        # 卡片标题 + 节点下拉选择
        self.assertIn("关中断统计", h)
        self.assertIn('id="irq-node-sel"', h)
        self.assertIn("<option", h)
        self.assertIn("10.1.2.3", h)
        self.assertIn("10.1.2.4", h)
        # 按进程（comm）序列勾选（组级批量勾选由报告级工厂 JS 渲染）
        self.assertIn("kubelet", h)
        self.assertIn("按进程", h)
        # 嵌入数据（散点模式 + epoch/时长/cpu 附加信息）
        self.assertIn('id="irq-data"', h)
        self.assertIn('"scatter"', h)
        self.assertIn("4200", h)                 # 时长值
        self.assertIn("cpu=9", h)                # 事件附加信息
        # 悬浮透明提示窗 + 拖拽缩放（原生 JS，离线自包含）
        self.assertIn('id="irq-tooltip"', h)
        self.assertIn("XEXP('irq')", h)          # 卡内 init 调用
        self.assertIn("重置缩放", h)
        self.assertIn("拖拽", h)
        # 交互动能（悬停/拖拽/组级勾选）在报告级工厂 JS 定义一次
        rep = nla.generate_os_monitor_report(self._aux_stats(), "/tmp/fake")
        self.assertIn("mousemove", rep)
        self.assertIn("mousedown", rep)
        self.assertIn("xexp-gcheck", rep)        # 组级批量勾选
        self.assertIn("indeterminate", rep)      # 部分勾选半选状态
        # 汇总统计表（跨节点，不再逐节点平铺）
        self.assertIn("hardirq", h)
        self.assertIn("Top20", h)
        self.assertNotIn("_irqoff_svg", h)       # 旧平铺散点不再使用
        # 空 aux_stats / 无 series → 空串（可选输入降级）
        self.assertEqual(nla._irqoff_explorer_html({}), "")
        self.assertEqual(nla._irqoff_explorer_html(None), "")
        self.assertEqual(nla._irqoff_explorer_html(
            {"irqoff": {"10.1.2.3": {"total": 0, "series": []}}}), "")


class TestPigzParallelExtract(unittest.TestCase):
    """多归档并行解压：并发数 = min(归档数, workers)，单归档 pigz -p
    = workers // 并发数（pigz 解压近似单线程，多归档并行才是主要收益）。
    """

    def setUp(self):
        import shutil
        import tarfile
        root = Path(tempfile.mkdtemp(prefix="pigzpar_"))
        self._root = root
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self._bdir = root / "dscollect_log"
        self._bdir.mkdir()
        for name, text in (("bpf-worker1-192.168.219.1.log", "hello1\n"),
                           ("bpf-worker2-192.168.219.2.log", "hello2\n")):
            inner = self._bdir / name
            inner.write_text(text, encoding="utf-8")
            with tarfile.open(self._bdir / (name + ".tar.gz"), "w:gz") as tf:
                tf.add(inner, arcname=name)
            inner.unlink()

    def test_parallel_pigz_threads_split(self):
        """2 归档 + workers=4 → 各归档 pigz -p 2，且两次 tar 调用。"""
        import subprocess
        calls = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        with mock.patch.object(nla.shutil, "which",
                               lambda p: "/usr/bin/%s" % p), \
             mock.patch.object(nla.subprocess, "run", fake_run):
            nla.LogDiscovery._extract_archives(self._bdir, workers=4)
        self.assertEqual(len(calls), 2)
        for cmd in calls:
            self.assertIn("--use-compress-program=pigz -p 2", " ".join(cmd))

    def test_parallel_extraction_correct(self):
        """并行解压后两个归档内容均正确落地。"""
        nla.LogDiscovery._extract_archives(self._bdir, workers=4)
        self.assertEqual((self._bdir / "bpf-worker1-192.168.219.1.log")
                         .read_text(encoding="utf-8"), "hello1\n")
        self.assertEqual((self._bdir / "bpf-worker2-192.168.219.2.log")
                         .read_text(encoding="utf-8"), "hello2\n")

    def test_single_archive_keeps_full_workers(self):
        """单归档：pigz -p = workers（与旧行为一致，不因并行分摊）。"""
        import subprocess
        (self._bdir / "bpf-worker2-192.168.219.2.log.tar.gz").unlink()
        calls = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        with mock.patch.object(nla.shutil, "which",
                               lambda p: "/usr/bin/%s" % p), \
             mock.patch.object(nla.subprocess, "run", fake_run):
            nla.LogDiscovery._extract_archives(self._bdir, workers=6)
        self.assertEqual(len(calls), 1)
        self.assertIn("--use-compress-program=pigz -p 6", " ".join(calls[0]))

    def test_failed_archive_does_not_block_others(self):
        """一个归档 tar 失败回退 tarfile，另一个走 pigz 成功。"""
        import subprocess
        real_run = subprocess.run

        def fake_run(cmd, **kw):
            # worker2 的归档 tar 失败（回退 tarfile），worker1 走真实 pigz/tar
            if "worker2" in " ".join(cmd):
                return subprocess.CompletedProcess(cmd, 1)
            return real_run(cmd, **kw)

        with mock.patch.object(nla.shutil, "which",
                               lambda p: "/usr/bin/%s" % p), \
             mock.patch.object(nla.subprocess, "run", fake_run):
            nla.LogDiscovery._extract_archives(self._bdir, workers=4)
        self.assertEqual((self._bdir / "bpf-worker1-192.168.219.1.log")
                         .read_text(encoding="utf-8"), "hello1\n")
        self.assertEqual((self._bdir / "bpf-worker2-192.168.219.2.log")
                         .read_text(encoding="utf-8"), "hello2\n")


class TestHostIpInReport(unittest.TestCase):
    """worker/client 宿主机 IP 入报告 + JSON（节点名 → IP 反查）。"""

    def test_host_ip_of_node(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="hostip_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        bdir = root / "dscollect_log"
        bdir.mkdir()
        (bdir / "bpf-worker1-192.168.219.1.log").write_text("", encoding="utf-8")
        (bdir / "bpf-master-192.168.219.2.log").write_text("", encoding="utf-8")
        disc = nla.LogDiscovery(root)
        self.assertEqual(disc.host_ip_of_node("worker1"), "192.168.219.1")
        self.assertEqual(disc.host_ip_of_node("master"), "192.168.219.2")
        # 节点名本身是 IP（irqoff/nic 兜底键）时直接返回
        self.assertEqual(disc.host_ip_of_node("10.1.2.3"), "10.1.2.3")
        self.assertIsNone(disc.host_ip_of_node("unknown"))
        self.assertIsNone(disc.host_ip_of_node(None))

    def test_e2e_report_and_json(self):
        import argparse
        _, contexts, _ = nla.analyze(SAMPLE_LOG_ROOT)
        ctx = next(c for c in contexts if c.trace_id.endswith("117c5c4a91c7"))
        self.assertEqual(ctx.client_node, "master")
        self.assertEqual(ctx.server_node, "worker13")
        self.assertEqual(ctx.client_host_ip, "141.62.32.3")
        self.assertEqual(ctx.server_host_ip, "141.62.32.63")
        ns = argparse.Namespace(residual_threshold=1000)
        html_out = nla.generate_report(contexts, ns, SAMPLE_LOG_ROOT)
        self.assertIn("client 宿主机 IP", html_out)
        self.assertIn("141.62.32.3", html_out)
        self.assertIn("server 宿主机 IP", html_out)
        self.assertIn("141.62.32.63", html_out)
        data = json.loads(nla.generate_json(contexts, ns, SAMPLE_LOG_ROOT))
        tr = next(t for t in data["traces"] if t["trace_id"].endswith("117c5c4a91c7"))
        self.assertEqual(tr["client"]["host_ip"], "141.62.32.3")
        self.assertEqual(tr["server"]["host_ip"], "141.62.32.63")

    def test_overview_node_table(self):
        """概览节点信息表：节点 / 宿主机 IP / bpf / 告警 / irqoff / nic / NUMA。"""
        import argparse
        aux = {"nodes": {
            "worker12": {"host_ip": "192.168.0.59", "bpf": "bpf-worker12-192.168.0.59.log",
                         "warn": "worker12_192.168.0.59", "irqoff": "", "nic": "",
                         "numa": "numafast / memory / perf"},
            "10.1.2.3": {"host_ip": "10.1.2.3", "bpf": "", "warn": "",
                         "irqoff": "irqoff_latency_10.1.2.3.log", "nic": "", "numa": ""}}}
        rec = nla.SlowRecord("t1", DAY, {"network_residual_us": "2000"}, "x.log", "pod")
        ctx = nla.TraceContext(rec)
        ctx.conclusion = {"label": "L", "confidence": "低"}
        ns = argparse.Namespace(residual_threshold=1000)
        out = nla.generate_report([ctx], ns, "/tmp", aux_stats=aux)
        self.assertIn("节点信息", out)
        self.assertIn("worker12", out)
        self.assertIn("192.168.0.59", out)
        self.assertIn("bpf-worker12-192.168.0.59.log", out)
        self.assertIn("irqoff_latency_10.1.2.3.log", out)
        self.assertIn("numafast / memory / perf", out)
        # 无节点数据时不渲染空表
        out2 = nla.generate_report([ctx], ns, "/tmp", aux_stats={})
        self.assertNotIn("节点信息", out2)


class TestOsMonitorReport(unittest.TestCase):
    """OS 资源类周期监控独立报告（后续新增周期监控指标统一在此扩展）：

    1) generate_os_monitor_report：节点表 + irqoff/nic/NUMA 概览集中渲染，
       自包含单文件 HTML；无任何周期监控数据 → 空串；
    2) analyze(os_monitor_only=True)：仅有周期监控日志（无 client 日志）时
       照常发现并全周期统计，跳过慢请求分析；
    3) CLI --os-monitor-only / --os-monitor-report：独立分析周期监控输出报告；
       正常分析模式下采集到周期监控数据也默认自动生成。
    """

    NUMA_DIR = "141.61.91.189-141.61.91.189-data_20260911-100322"

    def _write_numa_logs(self, root):
        d = Path(root) / "dscollect_log" / self.NUMA_DIR
        d.mkdir(parents=True, exist_ok=True)
        (d / "numafast_20260911-100325.log").write_text(
            "NUMAFAST Report-1(x)                     Time:20260910-202917\n"
            "1. System's numa score : 0.84\n", encoding="utf-8")
        (d / "memory_20260911-100325.log").write_text(
            "Memory Summary Report-1                                 "
            "Time:2026/09/10 20:29:17\n"
            "L1D         1.23%\nddrc_write        1258.20MB/s\n",
            encoding="utf-8")
        (d / "perf_20260911-100325.log").write_text(
            "==== perf stat round 1/3600 开始时间 2026-09-11 10:03:25 ====\n"
            "         6,314,507      dTLB-load-misses\n", encoding="utf-8")

    @staticmethod
    def _full_aux_stats():
        return {"numa": {
            "141.61.91.189": {
                "numafast": [{"ts": DAY, "score": 0.84, "nids": {}}],
                "memory": [{"ts": DAY, "l1d_miss_pct": 1.23,
                            "ddrc_read_mb_s": 214.49, "ddrc_write_mb_s": 1258.2}],
                "perf": [{"ts": DAY, "dtlb_load_misses": 6314507,
                          "itlb_load_misses": 52128}]}},
                "irqoff": {"10.1.2.3": {"total": 1, "hardirq_n": 1,
                                        "softirq_n": 0, "max_us": 2000,
                                        "total_us": 2000,
                                        "buckets": {"1000": 1},
                                        "by_comm": {"kubelet":
                                                    {"n": 1, "max_us": 2000,
                                                     "total_us": 2000}},
                                        "series": [[DAY, 2000,
                                                    "kubelet", 4]]}},
                "nic": {},
                "nodes": {"141.61.91.189": {"host_ip": "141.61.91.189",
                                            "bpf": "", "warn": "", "irqoff": "",
                                            "nic": "",
                                            "numa": "memory / numafast / perf"}}}

    def test_generate_os_monitor_report(self):
        h = nla.generate_os_monitor_report(self._full_aux_stats(), "/tmp/fake")
        self.assertIn("OS 资源周期监控报告", h)
        self.assertIn("<html", h)          # 自包含单文件 HTML
        self.assertIn("NUMA 访存监控", h)
        self.assertIn("关中断统计", h)
        self.assertIn('id="irq-node-sel"', h)   # 关中断交互探索器
        self.assertIn("节点信息", h)
        # 无任何周期监控数据 → 空串（不生成空报告）
        self.assertEqual(nla.generate_os_monitor_report({}, "/tmp"), "")
        self.assertEqual(nla.generate_os_monitor_report(None, "/tmp"), "")

    def test_analyze_os_monitor_only(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="osmon_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self._write_numa_logs(root)   # 只有周期监控日志，无 collected client 日志
        disc, contexts, trace_lines = nla.analyze(str(root), os_monitor_only=True)
        self.assertEqual(contexts, [])
        self.assertEqual(trace_lines, {})
        self.assertIn("141.61.91.189", disc.aux_stats["numa"])
        self.assertEqual(disc.aux_stats["numa"]["141.61.91.189"]["numafast"][0]
                         ["score"], 0.84)
        # 节点信息表数据也已构建
        self.assertIn("141.61.91.189", disc.aux_stats["nodes"])
        # 默认模式（非独立）无 client 日志 → 仍报错
        with self.assertRaises(FileNotFoundError):
            nla.analyze(str(root))

    def test_main_os_monitor_only(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="osmoncli_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self._write_numa_logs(root)
        out_html = root / "r.html"
        rc = nla.main([str(root), "--os-monitor-only", "-o", str(out_html)])
        self.assertEqual(rc, 0)
        os_report = root / "os_monitor_report.html"   # 默认 -o 同目录
        self.assertTrue(os_report.exists())
        content = os_report.read_text(encoding="utf-8")
        self.assertIn("OS 资源周期监控报告", content)
        self.assertIn("NUMA 访存监控", content)
        # 独立模式：无问题请求分析，主（定界）报告不生成
        self.assertFalse(out_html.exists())

    def test_main_generates_os_report_when_data(self):
        """正常分析模式：采集到周期监控数据即默认生成 OS 监控报告。"""
        import shutil
        import types
        root = Path(tempfile.mkdtemp(prefix="osmonauto_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self._write_numa_logs(root)
        fake_disc = types.SimpleNamespace(aux_stats=self._full_aux_stats())
        with mock.patch.object(nla, "analyze",
                               return_value=(fake_disc, [], {})) as _:
            rc = nla.main([str(root), "-o", str(root / "r.html")])
        # 无问题请求 → 定界分析退出码 1，但 OS 监控报告已默认生成
        self.assertEqual(rc, 1)
        self.assertFalse((root / "r.html").exists())
        os_report = root / "os_monitor_report.html"
        self.assertTrue(os_report.exists())
        self.assertIn("NUMA 访存监控",
                      os_report.read_text(encoding="utf-8"))

    def test_main_os_monitor_report_custom_path(self):
        import shutil
        root = Path(tempfile.mkdtemp(prefix="osmonpath_"))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self._write_numa_logs(root)
        custom = root / "sub" / "my_os.html"
        rc = nla.main([str(root), "--os-monitor-only",
                       "--os-monitor-report", str(custom),
                       "-o", str(root / "r.html")])
        self.assertEqual(rc, 0)
        self.assertTrue(custom.exists())
        self.assertFalse((root / "os_monitor_report.html").exists())


class TestWindowQuantAttribution(unittest.TestCase):
    """窗口级定量归因：irqoff/nic/numa 从"展示+置信度"升级为参与推理。"""

    CIP, CPORT = "192.168.32.61", 39776
    SIP, SPORT = "192.168.52.197", 31501
    T0 = datetime(2026, 8, 23, 20, 45, 39)

    def _ctx(self):
        slow = nla.SlowRecord(
            "tr", self.T0,
            {"network_residual_us": "2000", "e2e_us": "3000", "framework_us": "2500",
             "method": "m", "remote_processing_us": "0", "server_req_queue_us": "0",
             "server_exec_us": "0"},
            "/tmp/x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.idx = 0
        ctx.client_node, ctx.server_node = "m1", "w1"
        ctx.client_ip, ctx.server_ip = self.CIP, self.SIP
        ctx.conn = (self.CIP, self.CPORT, self.SIP, self.SPORT)
        # server kernel_to_user 段异常：8000us（NetifRx → ServerRecv）
        ctx.milestones = {
            "ServerNetifRx": self.T0.replace(microsecond=650000),
            "ServerTcpRecvFirst": self.T0.replace(microsecond=651000),
            "ServerRecv": self.T0.replace(microsecond=658000)}
        ctx.kernel_segments = [{
            "key": "server_kernel_to_user", "start": "ServerNetifRx",
            "end": "ServerRecv", "dur_us": 8000, "threshold_us": 1000,
            "category": "server_kernel_to_user_delay", "desc": "d",
            "abnormal": True, "evidence": False}]
        ctx.anchors["ServerRecv"] = {"ts": self.T0.replace(microsecond=658000),
                                     "tid": "123", "cpu": "4", "bid": "b"}
        return ctx

    @staticmethod
    def _irqoff_ev(cpu, lat_us, ts):
        return {"ts": ts, "irq": "hardirq", "cpu": cpu, "comm": "kubelet",
                "pid": 99, "latency_us": lat_us,
                "raw": ["head %dus" % lat_us]}

    def test_irqoff_quant_hit_rewrites_category(self):
        """cpu 匹配的最长关中断 ≥ max(2ms, 50% 段耗时) → category 改写
        interrupt_off_delay，结论带定量文本。"""
        ctx = self._ctx()
        t = self.T0.replace(microsecond=653000)
        ctx.irqoff_events["server"] = [
            self._irqoff_ev(4, 5000, t),        # 命中：5000 ≥ max(2000, 4000)
            self._irqoff_ev(9, 6000, t)]        # cpu 不匹配不计
        nla._window_quant_attribution(ctx, {})
        self.assertTrue(ctx.irqoff_quant)
        self.assertEqual(ctx.irqoff_quant["max_us"], 5000)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "interrupt_off_delay")
        self.assertEqual(ctx.conclusion["confidence"], "高")
        self.assertTrue(any("占" in s and "62.5%" in s
                            for s in ctx.conclusion["evidence"]))

    def test_irqoff_quant_smt_sibling_cpu(self):
        """SMT 姊妹核（cpu^1）上的关中断同样计入。"""
        ctx = self._ctx()
        ctx.irqoff_events["server"] = [
            self._irqoff_ev(5, 4200, self.T0.replace(microsecond=653000))]
        nla._window_quant_attribution(ctx, {})
        self.assertTrue(ctx.irqoff_quant)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "interrupt_off_delay")

    def test_irqoff_quant_no_hit_below_thresholds(self):
        """低于 2ms 或不足段耗时 50% → 不改写。"""
        ctx = self._ctx()
        ctx.irqoff_events["server"] = [
            self._irqoff_ev(4, 1500, self.T0.replace(microsecond=653000)),
            self._irqoff_ev(4, 3000, self.T0.replace(microsecond=653000))]
        nla._window_quant_attribution(ctx, {})
        self.assertIsNone(ctx.irqoff_quant)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "server_kernel_to_user_delay")

    def test_irqoff_quant_window_filter(self):
        """慢段窗口外的关中断记录不参与归因（trace 窗口 ≠ 段窗口）。"""
        ctx = self._ctx()
        ctx.irqoff_events["server"] = [
            self._irqoff_ev(4, 6000, self.T0.replace(microsecond=700000))]
        nla._window_quant_attribution(ctx, {})
        self.assertIsNone(ctx.irqoff_quant)

    def test_numa_window_ddr_saturation(self):
        """问题窗口 DDR 带宽 vs 全周期基线（均值+2σ）超限 → 定量证据。"""
        ctx = self._ctx()
        from datetime import timedelta
        base = [{"ts": self.T0.replace(microsecond=650000) - timedelta(seconds=60 * i),
                 "ddrc_read_mb_s": 900.0, "ddrc_write_mb_s": 100.0}
                for i in range(1, 11)]
        # 全周期含基线 10 点 + 窗口内 2 点飙高（各 10000MB/s）
        win_hi = [{"ts": self.T0.replace(microsecond=652000),
                   "ddrc_read_mb_s": 9500.0, "ddrc_write_mb_s": 500.0},
                  {"ts": self.T0.replace(microsecond=656000),
                   "ddrc_read_mb_s": 10500.0, "ddrc_write_mb_s": 500.0}]
        numa = {"w1": {"memory": win_hi + base}}
        nla._window_quant_attribution(ctx, numa)
        self.assertTrue(ctx.numa_quant)
        self.assertTrue(any("DDR" in s for s in ctx.quant_evidence))
        nla.ConclusionEngine.conclude(ctx)
        self.assertTrue(any("DDR" in s and "带宽" in s
                            for s in ctx.conclusion["evidence"]))

    def test_numa_window_no_saturation(self):
        """窗口带宽在基线范围内 → 无 numa 定量证据。"""
        ctx = self._ctx()
        from datetime import timedelta
        recs = ([{"ts": self.T0.replace(microsecond=652000),
                  "ddrc_read_mb_s": 950.0, "ddrc_write_mb_s": 100.0}] +
                [{"ts": self.T0.replace(microsecond=650000) - timedelta(seconds=60 * i),
                  "ddrc_read_mb_s": 900.0 + i * 5.0, "ddrc_write_mb_s": 100.0}
                 for i in range(1, 11)])
        nla._window_quant_attribution(ctx, {"w1": {"memory": recs}})
        self.assertIsNone(ctx.numa_quant)

    def test_nic_quant_text(self):
        """窗口 ifutil 峰值 ≥80% 且有重传 → 定量文本进结论证据。"""
        ctx = self._ctx()
        ctx.kernel_segments = [{
            "key": "wire_c2s", "start": "ClientTcpSendIn", "end": "ServerTcpRecvFirst",
            "dur_us": 900, "threshold_us": 200,
            "category": "network_c2s_transmission", "desc": "d",
            "abnormal": True, "evidence": False}]
        ctx.milestones = {
            "ClientTcpSendIn": self.T0.replace(microsecond=640000),
            "ServerTcpRecvFirst": self.T0.replace(microsecond=650000)}
        ctx.nic_samples["client"] = [
            {"ts": self.T0.replace(microsecond=645000), "dev": "eth0",
             "ifutil": 85.0, "rxkB": 100.0, "txkB": 200.0}]
        ctx.nic_evidence.append("窗口内检测到 2 次 TCP 重传（client 侧 2）")
        nla._window_quant_attribution(ctx, {})
        self.assertTrue(ctx.nic_quant)
        nla.ConclusionEngine.conclude(ctx)
        self.assertEqual(ctx.conclusion["category"], "network_c2s_transmission")
        self.assertTrue(any("峰值" in s and "85.0%" in s
                            for s in ctx.conclusion["evidence"]))


class TestPreemptorWakeupTrace(unittest.TestCase):
    """抢占任务唤醒者回溯：回答"该任务为何在该 cpu 运行"。"""

    T0 = datetime(2026, 8, 23, 20, 45, 39)

    def _ctx(self, events):
        slow = nla.SlowRecord(
            "tr", self.T0, {"network_residual_us": "2000"}, "/tmp/x.log", "pod")
        ctx = nla.TraceContext(slow)
        ctx.idx = 0
        ctx.server_node = "w1"
        evs = sorted(events, key=lambda e: e["ts"])
        ctx.bpf_window_events["server"] = evs
        ctx.softirq_localization["server"] = {
            "comm": "kworker/6:1", "kstack": "ks", "latency_us": 2500,
            "vec": 3, "vec_txt": "3(NET_RX)", "cpu": 6, "anchor_cpu": 7,
            "smt": True, "ts": self.T0.replace(microsecond=655000),
            "recv_ts": self.T0.replace(microsecond=655000),
            "n_candidates": 1, "events": []}
        return ctx

    def _ev(self, kind, us, **kw):
        ev = {"ts": self.T0.replace(microsecond=us), "kind": kind, "cpu": kw.pop("cpu", 6),
              "raw": "%s@%d" % (kind, us)}
        ev.update(kw)
        return ev

    def test_wakeup_trace_found(self):
        """sched_wakeup（target_cpu 匹配）→ sched_switch 切入链可回溯。"""
        ctx = self._ctx([
            self._ev("netif_receive", 649900),
            self._ev("sched_wakeup", 651000, comm="kworker/6:1", pid=777,
                     target_cpu=6),
            self._ev("sched_switch", 652000, prev_comm="swapper/6",
                     prev_pid=0, next_comm="kworker/6:1", next_pid=777),
            self._ev("softirq_raise_delay", 655000, vec=3, latency_us=2500,
                     comm="kworker/6:1", kstack="ks")])
        nla._preemptor_wakeup_trace(ctx)
        wt = ctx.softirq_localization["server"]["wakeup_trace"]
        self.assertIsNotNone(wt)
        self.assertEqual(wt["wakeup_ts"], self.T0.replace(microsecond=651000))
        self.assertEqual(wt["switch_in_ts"], self.T0.replace(microsecond=652000))
        self.assertEqual(wt["delta_to_recv_us"], 3000)  # 652000 → 655000
        self.assertEqual(wt["waker_cpu"], 6)

    def test_wakeup_trace_trigger_source(self):
        """唤醒事件前同 cpu 相邻事件作为触发源提示。"""
        ctx = self._ctx([
            self._ev("netif_receive", 650900),
            self._ev("sched_wakeup", 651000, comm="kworker/6:1", pid=777,
                     target_cpu=6),
            self._ev("sched_switch", 652000, prev_comm="swapper/6",
                     prev_pid=0, next_comm="kworker/6:1", next_pid=777),
            self._ev("softirq_raise_delay", 655000, vec=3, latency_us=2500,
                     comm="kworker/6:1", kstack="ks")])
        nla._preemptor_wakeup_trace(ctx)
        wt = ctx.softirq_localization["server"]["wakeup_trace"]
        self.assertEqual(wt["trigger_kind"], "netif_receive")

    def test_wakeup_trace_missing_degrades(self):
        """无 wakeup/switch 事件 → wakeup_trace 为 None（降级，不报错）。"""
        ctx = self._ctx([
            self._ev("softirq_raise_delay", 655000, vec=3, latency_us=2500,
                     comm="kworker/6:1", kstack="ks")])
        nla._preemptor_wakeup_trace(ctx)
        self.assertIsNone(
            ctx.softirq_localization["server"].get("wakeup_trace"))

    def test_wakeup_trace_in_conclusion(self):
        """回溯结果进 conclusion.preemptor_origin + 建议文本替换。"""
        ctx = self._ctx([
            self._ev("sched_wakeup", 651000, comm="kworker/6:1", pid=777,
                     target_cpu=6),
            self._ev("sched_switch", 652000, prev_comm="swapper/6",
                     prev_pid=0, next_comm="kworker/6:1", next_pid=777),
            self._ev("softirq_raise_delay", 655000, vec=3, latency_us=2500,
                     comm="kworker/6:1", kstack="ks")])
        nla._preemptor_wakeup_trace(ctx)
        nla.ConclusionEngine.conclude(ctx)
        po = ctx.conclusion.get("preemptor_origin")
        self.assertIsNotNone(po)
        self.assertIn("3.000", po)         # 距收包点时间差（fmt_us 格式）
        self.assertTrue(any("唤醒链" in s for s in ctx.conclusion["suggestions"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
