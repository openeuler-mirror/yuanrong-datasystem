#!/usr/bin/env python3
import argparse
import multiprocessing as mp
import sys
import time
from typing import List, Optional, Tuple

from yr.datasystem import TransferEngine

torch = None  # type: ignore


def ensure_torch_npu() -> None:
    global torch
    if torch is not None:
        return
    try:
        import torch as torch_mod  # type: ignore
        import torch_npu  # noqa: F401
    except ImportError as exc:
        raise RuntimeError("torch_npu is required for python cross-node smoke test") from exc
    torch = torch_mod


def parse_addr_list(text: str) -> List[int]:
    out: List[int] = []
    for item in text.split(","):
        item = item.strip()
        if not item:
            raise ValueError("empty address item in list")
        addr = int(item, 0)
        if addr <= 0:
            raise ValueError(f"address must be positive: {item}")
        out.append(addr)
    if not out:
        raise ValueError("address list is empty")
    return out


def verify_tensor_value(tensor: "torch.Tensor", expected: int) -> None:
    host = tensor.cpu()
    expected_tensor = torch.full_like(host, expected)
    if not torch.equal(host, expected_tensor):
        first = int(host.flatten()[0].item()) if host.numel() > 0 else -1
        raise RuntimeError(f"data verify failed: first={first}, expected={expected}")


def parse_hostname_port(hostname: str) -> Tuple[str, int]:
    if hostname.startswith("["):
        close = hostname.find("]")
        if close <= 0 or close + 2 > len(hostname) or hostname[close + 1] != ":":
            raise ValueError(f"invalid hostname: {hostname}")
        host = hostname[1:close]
        port = int(hostname[close + 2:])
    else:
        if ":" not in hostname:
            raise ValueError(f"invalid hostname: {hostname}")
        host, port_str = hostname.rsplit(":", 1)
        port = int(port_str)
    if not host or port <= 0 or port > 65535:
        raise ValueError(f"invalid hostname: {hostname}")
    return host, port


def format_hostname(host: str, port: int) -> str:
    if ":" in host and not host.startswith("["):
        return f"[{host}]:{port}"
    return f"{host}:{port}"


def run_owner(args: argparse.Namespace) -> int:
    ensure_torch_npu()
    engine = TransferEngine()
    rc = engine.initialize(args.local_hostname, "ascend", f"npu:{args.device_id}")
    if rc.is_error():
        print(f"[ERROR] initialize failed: {rc.to_string()}", file=sys.stderr)
        return 1

    src_tensors: List["torch.Tensor"] = []
    src_addrs: List[int] = []
    try:
        if args.src_addrs:
            src_addrs = parse_addr_list(args.src_addrs)
        else:
            fill = (args.device_id + 1) & 0xFF
            dev = torch.device(f"npu:{args.device_id}")
            for _ in range(args.register_count):
                t = torch.full((args.size,), fill, dtype=torch.uint8, device=dev)
                src_tensors.append(t)
                src_addrs.append(int(t.data_ptr()))

        lengths = [args.size] * len(src_addrs)
        rc = engine.batch_register_memory(src_addrs, lengths)
        if rc.is_error():
            print(f"[ERROR] batch_register_memory failed: {rc.to_string()}", file=sys.stderr)
            return 1

        remote_addrs = ",".join(hex(x) for x in src_addrs)
        print(f"[OWNER_READY] local_hostname={args.local_hostname} device_id={args.device_id} "
              f"register_count={len(src_addrs)} size={args.size} remote_addrs={remote_addrs}")
        print(f"[OWNER_READY_FOR_REQUESTER] --peer-hostname {args.local_hostname} "
              f"--peer-device-id {args.device_id} --remote-addrs {remote_addrs}")
        sys.stdout.flush()

        if args.hold_seconds == 0:
            input("Press ENTER to exit owner...")
        else:
            time.sleep(args.hold_seconds)
        return 0
    finally:
        engine.finalize()


def run_requester(args: argparse.Namespace) -> int:
    ensure_torch_npu()
    if not args.peer_hostname:
        print("[ERROR] requester requires --peer-hostname", file=sys.stderr)
        return 2
    if not args.remote_addrs:
        print("[ERROR] requester requires --remote-addrs", file=sys.stderr)
        return 2

    remote_addrs = parse_addr_list(args.remote_addrs)
    lengths = [args.size] * len(remote_addrs)
    dev = torch.device(f"npu:{args.device_id}")
    dst_tensors = [torch.zeros((args.size,), dtype=torch.uint8, device=dev) for _ in remote_addrs]
    dst_addrs = [int(t.data_ptr()) for t in dst_tensors]

    engine = TransferEngine()
    rc = engine.initialize(args.local_hostname, "ascend", f"npu:{args.device_id}")
    if rc.is_error():
        print(f"[ERROR] initialize failed: {rc.to_string()}", file=sys.stderr)
        return 1

    try:
        rc = engine.batch_transfer_sync_read(args.peer_hostname, dst_addrs, remote_addrs, lengths)
        if rc.is_error():
            print(f"[ERROR] batch_transfer_sync_read failed: {rc.to_string()}", file=sys.stderr)
            return 1

        if args.auto_verify_data:
            if args.peer_device_id is None:
                print("[ERROR] --auto-verify-data requires --peer-device-id", file=sys.stderr)
                return 2
            expected = (args.peer_device_id + 1) & 0xFF
            for t in dst_tensors:
                verify_tensor_value(t, expected)
        elif args.verify_byte is not None:
            expected = args.verify_byte & 0xFF
            for t in dst_tensors:
                verify_tensor_value(t, expected)

        print(f"[REQUESTER_DONE] peer_hostname={args.peer_hostname} batch_size={len(remote_addrs)} "
              f"bytes_each={args.size}")
        return 0
    finally:
        engine.finalize()


def requester_worker(worker_args: dict, result_queue: "mp.Queue[int]") -> None:
    try:
        rc = run_requester(argparse.Namespace(**worker_args))
        result_queue.put(rc)
    except Exception:  # noqa: BLE001
        result_queue.put(1)


def run_requester_concurrent(args: argparse.Namespace) -> int:
    if args.requester_count <= 1:
        return run_requester(args)

    base_host, base_port = parse_hostname_port(args.local_hostname)
    ctx = mp.get_context("fork")
    processes = []
    queues = []

    for i in range(args.requester_count):
        worker = dict(vars(args))
        worker_port = base_port + i * args.requester_port_step
        if worker_port <= 0 or worker_port > 65535:
            print(f"[ERROR] worker port out of range: {worker_port}", file=sys.stderr)
            return 2
        worker["local_hostname"] = format_hostname(base_host, worker_port)
        worker["device_id"] = args.device_id + i * args.requester_device_step
        if worker["device_id"] < 0:
            print(f"[ERROR] worker device_id out of range: {worker['device_id']}", file=sys.stderr)
            return 2

        q = ctx.Queue()
        p = ctx.Process(target=requester_worker, args=(worker, q))
        p.start()
        processes.append(p)
        queues.append(q)

    all_ok = True
    for i, p in enumerate(processes):
        p.join(timeout=300)
        worker_rc = 1
        if not queues[i].empty():
            worker_rc = queues[i].get()
        if p.is_alive() or p.exitcode != 0 or worker_rc != 0:
            all_ok = False
            print(f"[ERROR] requester worker failed idx={i} exit={p.exitcode} rc={worker_rc}", file=sys.stderr)

    if not all_ok:
        return 1
    print(f"[REQUESTER_CONCURRENT_DONE] requester_count={args.requester_count}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Python cross-node smoke tool for transfer_engine (owner/requester).")
    parser.add_argument("--role", choices=["owner", "requester"], required=True)
    parser.add_argument("--local-hostname", required=True, help='e.g. "127.0.0.1:18481"')
    parser.add_argument("--device-id", type=int, required=True)
    parser.add_argument("--size", type=int, required=True, help="bytes per buffer")
    parser.add_argument("--hold-seconds", type=int, default=600)
    parser.add_argument("--register-count", type=int, default=1)
    parser.add_argument("--src-addrs", default="", help="owner custom source addrs, comma-separated")

    parser.add_argument("--peer-hostname", default="", help='e.g. "10.1.1.8:18481"')
    parser.add_argument("--peer-device-id", type=int, default=None)
    parser.add_argument("--remote-addrs", default="", help="requester remote addrs, comma-separated")
    parser.add_argument("--auto-verify-data", action="store_true")
    parser.add_argument("--verify-byte", type=int, default=None)
    parser.add_argument("--requester-count", type=int, default=1, help="number of requester processes")
    parser.add_argument("--requester-port-step", type=int, default=1, help="local port increment per requester")
    parser.add_argument("--requester-device-step", type=int, default=0, help="device id increment per requester")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.device_id < 0:
        print("[ERROR] --device-id must be non-negative", file=sys.stderr)
        return 2
    if args.size <= 0:
        print("[ERROR] --size must be positive", file=sys.stderr)
        return 2
    if args.role == "owner":
        if args.register_count <= 0:
            print("[ERROR] --register-count must be positive", file=sys.stderr)
            return 2
        return run_owner(args)
    if args.requester_count <= 0:
        print("[ERROR] --requester-count must be positive", file=sys.stderr)
        return 2
    if args.requester_port_step <= 0:
        print("[ERROR] --requester-port-step must be positive", file=sys.stderr)
        return 2
    return run_requester_concurrent(args)


if __name__ == "__main__":
    sys.exit(main())
