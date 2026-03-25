import multiprocessing as mp
import socket
import unittest

from yr.datasystem import StatusCode, TransferEngine

try:
    import torch
    import torch_npu  # noqa: F401
    HAS_TORCH_NPU = hasattr(torch, "npu")
except ImportError:
    HAS_TORCH_NPU = False

def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _owner_worker(local_hostname: str, device_id: int, size: int, batch_count: int, ready_queue, stop_event) -> None:
    engine = TransferEngine()
    try:
        init_rc = engine.initialize(local_hostname, "ascend", f"npu:{device_id}")
        if init_rc.is_error():
            ready_queue.put({"ok": False, "error": init_rc.to_string()})
            return

        src_tensors = []
        src_addrs = []
        lengths = []
        expected_values = []
        dev = torch.device(f"npu:{device_id}")
        for i in range(batch_count):
            v = ((i + 1) * 17) & 0xFF
            t = torch.full((size,), v, dtype=torch.uint8, device=dev)
            src_tensors.append(t)
            src_addrs.append(int(t.data_ptr()))
            lengths.append(size)
            expected_values.append(v)

        reg_rc = engine.batch_register_memory(src_addrs, lengths)
        if reg_rc.is_error():
            ready_queue.put({"ok": False, "error": reg_rc.to_string()})
            return

        ready_queue.put({
            "ok": True,
            "remote_addrs": src_addrs,
            "lengths": lengths,
            "expected_values": expected_values,
            "owner_hostname": local_hostname,
        })
        stop_event.wait(timeout=120.0)
    except Exception as exc:  # noqa: BLE001
        ready_queue.put({"ok": False, "error": str(exc)})
    finally:
        engine.finalize()


def _requester_worker(local_hostname: str, device_id: int, owner_hostname: str, remote_addrs, lengths, expected_values,
                      result_queue) -> None:
    engine = TransferEngine()
    try:
        init_rc = engine.initialize(local_hostname, "ascend", f"npu:{device_id}")
        if init_rc.is_error():
            result_queue.put({"ok": False, "error": init_rc.to_string()})
            return

        dev = torch.device(f"npu:{device_id}")
        dst_tensors = [torch.zeros((int(lengths[i]),), dtype=torch.uint8, device=dev) for i in range(len(remote_addrs))]
        dst_addrs = [int(t.data_ptr()) for t in dst_tensors]

        batch_rc = engine.batch_transfer_sync_read(owner_hostname, dst_addrs, remote_addrs, lengths)
        if batch_rc.is_error():
            result_queue.put({"ok": False, "error": batch_rc.to_string()})
            return

        # Also validate single-transfer API on the first item.
        single_dst = torch.zeros((int(lengths[0]),), dtype=torch.uint8, device=dev)
        single_rc = engine.transfer_sync_read(owner_hostname, int(single_dst.data_ptr()), int(remote_addrs[0]), int(lengths[0]))
        if single_rc.is_error():
            result_queue.put({"ok": False, "error": single_rc.to_string()})
            return

        for i, t in enumerate(dst_tensors):
            expected = torch.full((int(lengths[i]),), int(expected_values[i]), dtype=torch.uint8)
            if not torch.equal(t.cpu(), expected):
                result_queue.put({"ok": False, "error": f"batch verify failed at idx={i}"})
                return
        single_expected = torch.full((int(lengths[0]),), int(expected_values[0]), dtype=torch.uint8)
        if not torch.equal(single_dst.cpu(), single_expected):
            result_queue.put({"ok": False, "error": "single transfer verify failed"})
            return

        result_queue.put({"ok": True})
    except Exception as exc:  # noqa: BLE001
        result_queue.put({"ok": False, "error": str(exc)})
    finally:
        engine.finalize()


@unittest.skipUnless(HAS_TORCH_NPU, "torch_npu is required for python st tests")
class PythonApiStTest(unittest.TestCase):
    def setUp(self):
        if torch.npu.device_count() < 2:
            self.skipTest("requires at least 2 NPUs for same-node different-device validation")
        self.ctx = mp.get_context("fork")

    def test_batch_and_single_transfer_with_fork(self):
        owner_port = _free_port()
        requester_port = _free_port()
        owner_hostname = f"127.0.0.1:{owner_port}"
        requester_hostname = f"127.0.0.1:{requester_port}"

        ready_q = self.ctx.Queue()
        stop_event = self.ctx.Event()
        owner_proc = self.ctx.Process(
            target=_owner_worker,
            args=(owner_hostname, 0, 256, 3, ready_q, stop_event),
        )
        owner_proc.start()

        owner_info = ready_q.get(timeout=30)
        self.assertTrue(owner_info.get("ok"), owner_info.get("error"))

        result_q = self.ctx.Queue()
        requester_proc = self.ctx.Process(
            target=_requester_worker,
            args=(
                requester_hostname,
                1,
                owner_info["owner_hostname"],
                owner_info["remote_addrs"],
                owner_info["lengths"],
                owner_info["expected_values"],
                result_q,
            ),
        )
        requester_proc.start()
        requester_proc.join(timeout=60)
        self.assertFalse(requester_proc.is_alive(), "requester process timeout")
        self.assertEqual(requester_proc.exitcode, 0)
        requester_result = result_q.get(timeout=10)
        self.assertTrue(requester_result.get("ok"), requester_result.get("error"))

        stop_event.set()
        owner_proc.join(timeout=30)
        self.assertFalse(owner_proc.is_alive(), "owner process timeout")
        self.assertEqual(owner_proc.exitcode, 0)

    def test_concurrent_requesters_with_fork(self):
        owner_port = _free_port()
        owner_hostname = f"127.0.0.1:{owner_port}"

        ready_q = self.ctx.Queue()
        stop_event = self.ctx.Event()
        owner_proc = self.ctx.Process(
            target=_owner_worker,
            args=(owner_hostname, 0, 128, 2, ready_q, stop_event),
        )
        owner_proc.start()
        owner_info = ready_q.get(timeout=30)
        self.assertTrue(owner_info.get("ok"), owner_info.get("error"))

        requester_count = 3
        requesters = []
        result_queues = []
        for _ in range(requester_count):
            rq = self.ctx.Queue()
            result_queues.append(rq)
            proc = self.ctx.Process(
                target=_requester_worker,
                args=(
                    f"127.0.0.1:{_free_port()}",
                    1,
                    owner_info["owner_hostname"],
                    owner_info["remote_addrs"],
                    owner_info["lengths"],
                    owner_info["expected_values"],
                    rq,
                ),
            )
            proc.start()
            requesters.append(proc)

        for i, proc in enumerate(requesters):
            proc.join(timeout=60)
            self.assertFalse(proc.is_alive(), f"requester-{i} timeout")
            self.assertEqual(proc.exitcode, 0)
            result = result_queues[i].get(timeout=10)
            self.assertTrue(result.get("ok"), result.get("error"))

        stop_event.set()
        owner_proc.join(timeout=30)
        self.assertFalse(owner_proc.is_alive(), "owner process timeout")
        self.assertEqual(owner_proc.exitcode, 0)

    def test_boundary_invalid_arguments(self):
        engine = TransferEngine()

        rc = engine.batch_transfer_sync_read("", [], [], [])
        self.assertEqual(rc.get_code(), StatusCode.kInvalid)

        rc = engine.batch_transfer_sync_read("127.0.0.1:1", [1], [2, 3], [4])
        self.assertEqual(rc.get_code(), StatusCode.kInvalid)

        rc = engine.batch_register_memory([1], [])
        self.assertEqual(rc.get_code(), StatusCode.kInvalid)

        rc = engine.transfer_sync_read("127.0.0.1:1", 0, 1, 16)
        self.assertEqual(rc.get_code(), StatusCode.kInvalid)


if __name__ == "__main__":
    unittest.main()
