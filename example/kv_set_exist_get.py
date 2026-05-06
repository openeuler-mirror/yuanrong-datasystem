# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Steady benchmark for split clients:
client1(set) -> client2(exist + get) at fixed QPS.
"""

from __future__ import absolute_import

import argparse
import logging
import time
from collections import deque
from typing import Deque

from yr.datasystem import KVClient


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run steady set/exist/get benchmark with two clients on different workers."
    )
    parser.add_argument("--set-host", default="141.62.32.17", help="Host used by client1 for set")
    parser.add_argument("--set-port", type=int, default=31101, help="Port used by client1 for set")
    parser.add_argument("--read-host", default="141.62.32.41", help="Host used by client2 for exist/get")
    parser.add_argument("--read-port", type=int, default=31101, help="Port used by client2 for exist/get")
    parser.add_argument("--qps", type=float, default=35.0, help="Operation-group QPS, default: 35")
    parser.add_argument("--duration-sec", type=int, default=0, help="Run seconds, 0 means forever")
    parser.add_argument("--report-interval-sec", type=int, default=10, help="Progress report interval")
    parser.add_argument("--payload-bytes", type=int, default=256, help="set payload bytes")
    parser.add_argument("--key-prefix", default="steady_kv", help="Key prefix")
    parser.add_argument("--connect-timeout-ms", type=int, default=60000)
    parser.add_argument("--req-timeout-ms", type=int, default=0)
    return parser.parse_args()


def percentile(values: Deque[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int((len(ordered) - 1) * p)
    return ordered[idx]


def run():
    args = parse_args()
    if args.qps <= 0:
        raise ValueError("--qps must be > 0")
    if args.payload_bytes <= 0:
        raise ValueError("--payload-bytes must be > 0")
    if args.report_interval_sec <= 0:
        raise ValueError("--report-interval-sec must be > 0")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client1 = KVClient(
        host=args.set_host,
        port=args.set_port,
        connect_timeout_ms=args.connect_timeout_ms,
        req_timeout_ms=args.req_timeout_ms,
    )
    client2 = KVClient(
        host=args.read_host,
        port=args.read_port,
        connect_timeout_ms=args.connect_timeout_ms,
        req_timeout_ms=args.req_timeout_ms,
    )
    client1.init()
    client2.init()
    logging.info(
        "Connected: client1(set)=%s:%d, client2(exist/get)=%s:%d, qps=%.2f",
        args.set_host,
        args.set_port,
        args.read_host,
        args.read_port,
        args.qps,
    )

    value = ("x" * args.payload_bytes).encode("utf-8")
    interval = 1.0 / args.qps
    start = time.monotonic()
    end_time = start + args.duration_sec if args.duration_sec > 0 else None
    next_tick = start
    next_report = start + args.report_interval_sec

    total = 0
    success = 0
    set_fail = 0
    exist_fail = 0
    get_fail = 0
    cycle_latency_ms: Deque[float] = deque(maxlen=5000)

    try:
        while True:
            now = time.monotonic()
            if end_time is not None and now >= end_time:
                break

            key = f"{args.key_prefix}_{total}"
            total += 1
            cycle_begin = time.monotonic()

            # Step 1: set on 141.62.32.17 (client1)
            try:
                client1.set(key, value)
            except Exception as exc:  # pylint: disable=broad-except
                set_fail += 1
                logging.error("set failed key=%s err=%s", key, exc)
                goto_sleep = True
            else:
                goto_sleep = False

            # Step 2: exist on 141.62.32.41 (client2)
            if not goto_sleep:
                try:
                    exists = client2.exist([key])
                    if len(exists) != 1 or not exists[0]:
                        raise RuntimeError("exist result is false")
                except Exception as exc:  # pylint: disable=broad-except
                    exist_fail += 1
                    logging.error("exist failed key=%s err=%s", key, exc)
                    goto_sleep = True

            # Step 3: get on 141.62.32.41 (client2)
            if not goto_sleep:
                try:
                    got = client2.get([key], convert_to_str=False)
                    if len(got) != 1 or got[0] != value:
                        raise RuntimeError("get value mismatch")
                    success += 1
                except Exception as exc:  # pylint: disable=broad-except
                    get_fail += 1
                    logging.error("get failed key=%s err=%s", key, exc)

            cycle_latency_ms.append((time.monotonic() - cycle_begin) * 1000.0)

            now = time.monotonic()
            if now >= next_report:
                elapsed = now - start
                actual_qps = success / elapsed if elapsed > 0 else 0.0
                logging.info(
                    "progress total=%d success=%d set_fail=%d exist_fail=%d get_fail=%d actual_qps=%.2f "
                    "latency_ms(p50=%.2f p95=%.2f p99=%.2f)",
                    total,
                    success,
                    set_fail,
                    exist_fail,
                    get_fail,
                    actual_qps,
                    percentile(cycle_latency_ms, 0.50),
                    percentile(cycle_latency_ms, 0.95),
                    percentile(cycle_latency_ms, 0.99),
                )
                next_report += args.report_interval_sec

            next_tick += interval
            sleep_sec = next_tick - time.monotonic()
            if sleep_sec > 0:
                time.sleep(sleep_sec)
            elif sleep_sec < -interval * 10:
                next_tick = time.monotonic()
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")

    elapsed = time.monotonic() - start
    actual_qps = success / elapsed if elapsed > 0 else 0.0
    logging.info(
        "finished elapsed_sec=%.2f total=%d success=%d set_fail=%d exist_fail=%d get_fail=%d actual_qps=%.2f "
        "latency_ms(p50=%.2f p95=%.2f p99=%.2f)",
        elapsed,
        total,
        success,
        set_fail,
        exist_fail,
        get_fail,
        actual_qps,
        percentile(cycle_latency_ms, 0.50),
        percentile(cycle_latency_ms, 0.95),
        percentile(cycle_latency_ms, 0.99),
    )


if __name__ == "__main__":
    run()