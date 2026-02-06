# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
Hetero cache client benchmark interface Test.
"""

import logging
import multiprocessing as mp
import os
import random
import threading
import time
import argparse
import json

from typing import Dict, List

import acl
import numpy as np

from yr.datasystem.hetero_client import (
    HeteroClient,
    Blob,
    DeviceBlobList,
)

# log config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BARRIER_WAIT_TIMEOUT = 60
DEFAULT_WORKER_IP = "127.0.0.1"
DEFAULT_WORKER_PORT = 31699

DEVICE_START_INDEX = 3
TOTAL_NPU_COUNT = 8

timestamp_sec = int(time.time())
RESULT_FILENAME = f"perf_result_{timestamp_sec}.csv"

config_map = {

    # Single process, single thread, 1 key tests
    '1process_1thread_1key_72KB': {'num_processes': 1, 'key_num': 1, 'blob_nums': [61], 'blob_sizes': [72 * 1024],
                              'iterations': 100, 'thread_number': 1},
    '1process_1thread_1key_144KB': {'num_processes': 1, 'key_num': 1, 'blob_nums': [61], 'blob_sizes': [144 * 1024],
                              'iterations': 100, 'thread_number': 1},
    '1process_1thread_1key_1024KB': {'num_processes': 1, 'key_num': 1, 'blob_nums': [28], 'blob_sizes': [1024 * 1024],
                              'iterations': 100, 'thread_number': 1},

    # Single process, single thread, 32 key tests
    '1process_1thread_32key_72KB': {'num_processes': 1, 'key_num': 32, 'blob_nums': [61], 'blob_sizes': [72 * 1024],
                              'iterations': 75, 'thread_number': 1},
    '1process_1thread_32key_144KB': {'num_processes': 1, 'key_num': 32, 'blob_nums': [61], 'blob_sizes': [144 * 1024],
                               'iterations': 40, 'thread_number': 1},
    '1process_1thread_32key_1024KB': {'num_processes': 1, 'key_num': 32, 'blob_nums': [28], 'blob_sizes': [1024 * 1024],
                                'iterations': 13, 'thread_number': 1},

    # Single process, multiple thread, 32 key tests
    '1process_8thread_32key_72KB': {'num_processes': 1, 'key_num': 32, 'blob_nums': [61], 'blob_sizes': [72 * 1024],
                               'iterations': 10, 'thread_number': 8},
    '1process_8thread_32key_144KB': {'num_processes': 1, 'key_num': 32, 'blob_nums': [61], 'blob_sizes': [144 * 1024],
                                'iterations': 5, 'thread_number': 8},
    '1process_8thread_32key_1024KB': {'num_processes': 1, 'key_num': 32, 'blob_nums': [28], 'blob_sizes': [1024 * 1024],
                                'iterations': 1, 'thread_number': 8},

    # Eight processes, single thread, 1 key tests
    '8process_1thread_1key_72KB': {'num_processes': 8, 'key_num': 1, 'blob_nums': [61], 'blob_sizes': [72 * 1024],
                              'iterations': 100, 'thread_number': 1},
    '8process_1thread_1key_144KB': {'num_processes': 8, 'key_num': 1, 'blob_nums': [61], 'blob_sizes': [144 * 1024],
                              'iterations': 100, 'thread_number': 1},
    '8process_1thread_1key_1024KB': {'num_processes': 8, 'key_num': 1, 'blob_nums': [28], 'blob_sizes': [1024 * 1024],
                              'iterations': 40, 'thread_number': 1},

    # Eight processes, single thread, 32 key tests
    '8process_1thread_32key_72KB': {'num_processes': 8, 'key_num': 32, 'blob_nums': [61], 'blob_sizes': [72 * 1024],
                              'iterations': 10, 'thread_number': 1},
    '8process_1thread_32key_144KB': {'num_processes': 8, 'key_num': 32, 'blob_nums': [61], 'blob_sizes': [144 * 1024],
                               'iterations': 5, 'thread_number': 1},
    '8process_1thread_32key_1024KB': {'num_processes': 8, 'key_num': 32, 'blob_nums': [28], 'blob_sizes': [1024 * 1024],
                                'iterations': 1, 'thread_number': 1},

    # Hixl benchmark cases
    '1process_1key_16blob_8MB': {'num_processes': 1, 'key_num': 1, 'blob_nums': [16], 'blob_sizes': [8 * 1024 * 1024],
                               'iterations': 80, 'thread_number': 1},
    '1process_1key_32blob_4MB': {'num_processes': 1, 'key_num': 1, 'blob_nums': [32], 'blob_sizes': [4 * 1024 * 1024],
                               'iterations': 80, 'thread_number': 1},
    '1process_1key_64blob_2MB': {'num_processes': 1, 'key_num': 1, 'blob_nums': [64], 'blob_sizes': [2 * 1024 * 1024],
                               'iterations': 80, 'thread_number': 1},
    '1process_1key_128blob_1MB': {'num_processes': 1, 'key_num': 1, 'blob_nums': [128], 'blob_sizes': [1024 * 1024],
                               'iterations': 80, 'thread_number': 1},
    '1process_1key_256blob_512KB': {'num_processes': 1, 'key_num': 1, 'blob_nums': [256], 'blob_sizes': [512 * 1024],
                               'iterations': 80, 'thread_number': 1},
    '1process_1key_512blob_256KB': {'num_processes': 1, 'key_num': 1, 'blob_nums': [512], 'blob_sizes': [256 * 1024],
                               'iterations': 80, 'thread_number': 1}
}

mode = "all"
current_test = ""


def init_test_hetero_client():
    """Initialize hetero client"""
    client = HeteroClient(args.ip, args.port, enable_remote_h2d=True)
    client.init()
    return client


def random_str(slen=10):
    """Get rendom string by lens"""
    seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#%^*()_+=-"
    sa = []
    for _ in range(slen):
        sa.append(random.choice(seed))
    return "".join(sa)


def init_file(filename):
    """Initialize file: clear if it exists, create if it does not exist"""
    with open(filename, 'w') as _:
        pass


def write_data(data, filename):
    """process safe to write data"""
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        init_file(filename)

    # write with add
    with open(filename, 'a') as f:
        f.write(data + '\n')


def check_ptr_content(dev_ptr: int, check_value: str):
    """Check dev ptr with string value"""
    size = len(check_value)
    output_byte = bytes("0", "utf-8").zfill(size)
    output_ptr = acl.util.bytes_to_ptr(output_byte)
    ret = acl.rt.memcpy(output_ptr, size, dev_ptr, size, 2)
    if ret != 0:
        logging.info(f"[ERROR] acl.rt.memcpy failed with ret : {ret}")
    if output_byte != check_value.encode():
        logging.info(f"[ERROR] output_byte is not same as value {output_byte},  with  {check_value.encode()}")


def batch_data_check(out_data_blob_list, test_value):
    """Batch blob_list check with string value"""
    for batch_info in out_data_blob_list:
        for info in batch_info.blob_list:
            check_ptr_content(info.dev_ptr, test_value)


def mset_thread(client, device_id: int, key_num: int, iterations: int, thread_id: int, list_random_suffix: list,
                all_in_data_blob_list: list, time_list: list):
    """Thread for mset_d2h"""
    logging.info(f"[Dev {device_id}] Start set by thread_id {thread_id}")
    for i in range(iterations):
        keys = [f"dev{device_id}_key{i}_{j}_{list_random_suffix[thread_id]}" for j in range(key_num)]
        start = time.perf_counter()
        client.mset_d2h(keys, all_in_data_blob_list[thread_id])
        mset_time = (time.perf_counter() - start) * 1000  # ms
        time_list.append(mset_time)


def mget_thread(client, device_id: int, key_num: int, iterations: int, thread_id: int, list_random_suffix: list,
                all_out_data_blob_list: list, time_list: list):
    """Thread for mget_h2d"""
    for _ in range(args.get_multiplier):
        logging.info(f"[Dev {device_id}] Start get by thread_id {thread_id}")
        for i in range(iterations):
            keys = [f"dev{device_id}_key{i}_{j}_{list_random_suffix[thread_id]}" for j in range(key_num)]
            start = time.perf_counter()
            client.mget_h2d(keys, all_out_data_blob_list[thread_id], 60000)
            mget_time = (time.perf_counter() - start) * 1000  # ms
            time_list.append(mget_time)


def prepare_hbm_data(test_value, thread_number, key_num, blob_nums_per_key, blob_size, device_id, all_in_data_blob_list,
                     all_out_data_blob_list):
    """prepare HBM data for set and get"""
    logging.info(f"[Dev {device_id}]Start prepare data")
    for _ in range(thread_number):
        in_data_blob_list = []
        for _ in range(key_num):
            tmp_batch_list = []
            for _ in range(blob_nums_per_key):
                dev_ptr, _ = acl.rt.malloc(blob_size, 0)
                acl.rt.memcpy(dev_ptr, blob_size, acl.util.bytes_to_ptr(test_value.encode()), blob_size, 1)
                blob = Blob(dev_ptr, blob_size)
                tmp_batch_list.append(blob)
            blob_list = DeviceBlobList(device_id, tmp_batch_list)
            in_data_blob_list.append(blob_list)
        all_in_data_blob_list.append(in_data_blob_list)

    # prepare HBM data and get from datasystem
    for _ in range(thread_number):
        out_data_blob_list = []
        for _ in range(key_num):
            tmp_batch_list = []
            for _ in range(blob_nums_per_key):
                dev_ptr, _ = acl.rt.malloc(blob_size, 0)
                blob = Blob(dev_ptr, blob_size)
                tmp_batch_list.append(blob)
            blob_list = DeviceBlobList(device_id, tmp_batch_list)
            out_data_blob_list.append(blob_list)
        all_out_data_blob_list.append(out_data_blob_list)
    logging.info(f"[Dev {device_id}]Success prepare data")


def blocking_wait_sync(barrier, op_name, device_id):
    """blocking wait all process ready"""
    logging.info(f"[Dev {device_id}] start wait for {op_name}")
    try:
        logging.info(f"{op_name} synchronization point !")
        barrier.wait(timeout=BARRIER_WAIT_TIMEOUT)
    except threading.BrokenBarrierError:
        logging.info("Some processes failed to reach the synchronization point on time!")


def operate_thread(op_name, client, device_id, key_num, iterations, suffix, blob_list, shared_matrix):
    """worker threads : execute benchmark by op_name and wait each thread result"""
    threads_list = []
    thread_size = len(shared_matrix)
    for i in range(thread_size):
        if op_name == "mget_thread":
            thread = threading.Thread(target=mget_thread, args=(
                client, device_id, key_num, iterations, i, suffix, blob_list, shared_matrix[i]))
            thread.start()
            threads_list.append(thread)
        elif op_name == "mset_thread":
            thread = threading.Thread(target=mset_thread, args=(
                client, device_id, key_num, iterations, i, suffix, blob_list, shared_matrix[i]))
            thread.start()
            threads_list.append(thread)
    for t in threads_list:
        t.join()


def worker_process(
        device_id: int,
        key_num: int,
        blob_nums_per_key: List[int],
        blob_sizes: List[int],
        iterations: int,
        thread_number: int,
        result_queue: mp.Queue,
        barrier: mp.Barrier,
        lock: mp.Lock
):
    """worker process : execute benchmark and collect result"""

    logging.info(f"[Dev {device_id}]Start performance test")
    acl.init()
    acl.rt.set_device(device_id)
    client = init_test_hetero_client()
    logging.info(f"[Dev {device_id}]Success init hetero client ")

    # record the time point
    metrics_data = {"mset_times": [], "mget_times": []}

    # prepare data
    test_value = random_str(blob_sizes[0])
    all_in_data_blob_list, all_out_data_blob_list = [], []

    for i, blob_num in enumerate(blob_nums_per_key):
        in_data_blob_list, out_data_blob_list = [], []
        prepare_hbm_data(test_value, thread_number, key_num, blob_num, blob_sizes[i], device_id,
                         in_data_blob_list, out_data_blob_list)

        all_in_data_blob_list.extend(in_data_blob_list)
        all_out_data_blob_list.extend(out_data_blob_list)

    # warm up
    warmup(client, device_id, key_num, all_in_data_blob_list, all_out_data_blob_list)

    list_random_suffix = []
    key_dict = {}
    if mode in ["set", "get"]:
        with lock:
            read_write_keys(list_random_suffix, key_dict, thread_number, device_id)
    else:
        for _ in range(thread_number):
            random_suffix = random_str(10)
            list_random_suffix.append(random_suffix)

    if mode in ["set", "all"]:
        # wait all process ready, start mset_d2h at same time
        blocking_wait_sync(barrier, "mset_d2h", device_id)

        logging.info(f"[Dev {device_id}] Start mset_d2h")
        metrics_data["mset_d2h_start"] = time.perf_counter()

        shared_matrix = [[] for _ in range(thread_number)]
        operate_thread("mset_thread", client, device_id, key_num, iterations, list_random_suffix,
                       all_out_data_blob_list, shared_matrix)

        for sublist in shared_matrix:
            metrics_data["mset_times"].extend(sublist)
    
        metrics_data["mset_d2h_end"] = time.perf_counter()

    if mode in ["get", "all"]:
        # wait all process ready，start mget_h2d at same time
        blocking_wait_sync(barrier, "mget_h2d", device_id)

        logging.info(f"[Dev {device_id}]Start mget_h2d")
        metrics_data["mget_h2d_start"] = time.perf_counter()

        shared_matrix = [[] for _ in range(thread_number)]
        operate_thread("mget_thread", client, device_id, key_num, iterations, list_random_suffix,
                       all_out_data_blob_list, shared_matrix)

        for sublist in shared_matrix:
            metrics_data["mget_times"].extend(sublist)

        metrics_data["mget_h2d_end"] = time.perf_counter()
        logging.info(f"[Dev {device_id}]Success mget_h2d")

        if mode in ["all"]:
            batch_data_check(all_out_data_blob_list[0], test_value)

        # clear data
        for thread_id in range(thread_number):
            for i in range(iterations):
                del_keys = [f"dev{device_id}_key{i}_{j}_{list_random_suffix[thread_id]}" for j in range(key_num)]
                client.delete(del_keys)
        logging.info(f"[Dev {device_id}]Success delete data")

    # calculate the total data bytes
    metrics_data["total_data_bytes"] = 0
    for i, blob_num in enumerate(blob_nums_per_key):
        metrics_data["total_data_bytes"] += key_num * blob_num * blob_sizes[i] * iterations * thread_number

    # construct the record points
    result_queue.put(metrics_data)

    logging.info(f"[Dev {device_id}]Success test performance")
    acl.finalize()
    time.sleep(1)


def warmup(client: HeteroClient, dev_id: int, key_num: int, all_in_data_blob_list: list, all_out_data_blob_list: list):
    """Run a warmup before executing benchmark"""
    if not args.no_warmup:
        keys_list = [f"dev{dev_id}_key{i}" for i in range(key_num)]

        client.mset_d2h(keys_list, all_in_data_blob_list[0])
        client.mget_h2d(keys_list, all_out_data_blob_list[0], 60000)
        client.delete(keys_list)


def read_write_keys(
    list_random_suffix: List[str],
    key_dict: Dict[str, Dict[str, List[str]]],
    thread_number: int,
    device_id: int):
    """If --set-only on, write keys to keys.json file. If --get-only on, read keys from keys.json file"""
    # Deserialize keys.json to access previously set data
    try:
        with open(args.keys, 'r') as json_file:
            content = json_file.read()
            if content:
                key_dict = json.loads(content)
            if content and mode in ["get"]:
                list_random_suffix.extend(key_dict[current_test][str(device_id)])
    except FileNotFoundError as e:
        if mode in ["get"]:
            raise RuntimeError("keys.json file not found") from e
    except Exception as e:
        raise RuntimeError(f"Unknown exception: {e}") from e
    
    if mode in ["set"]:
        for _ in range(thread_number):
            random_suffix = random_str(10)
            list_random_suffix.append(random_suffix)
        
        # Serialize keys into json to re-use keys for "--get-only"
        if current_test not in key_dict:
            key_dict[current_test] = {}

        key_dict[current_test][str(device_id)] = list_random_suffix
        logging.info(f"Wrote {len(list_random_suffix)} keys to {current_test}")

        with open(args.keys, 'w') as json_file:
            json.dump(key_dict, json_file)


def calculate_metrics(times: List[float], duration: float = None) -> Dict[str, float]:
    """cal the metrics for one config"""
    if not times:
        return {}

    sorted_times = np.sort(times)
    metrics = {
        "min": float(np.min(times)),
        "max": float(np.max(times)),
        "mean": float(np.mean(times)),
        "p90": float(np.percentile(sorted_times, 90)),
        "p95": float(np.percentile(sorted_times, 95)),
        "p99": float(np.percentile(sorted_times, 99)),
        "tps": float(len(times) / duration)
    }

    return metrics


def format_metrics(metrics: Dict[str, float]) -> str:
    """format the time point result"""
    return (f"min: {metrics['min']:.2f}ms | max: {metrics['max']:.2f}ms | "
            f"mean: {metrics['mean']:.2f}ms | p90: {metrics['p90']:.2f}ms | "
            f"p95: {metrics['p95']:.2f}ms | p99: {metrics['p99']:.2f}ms | "
            f"tps: {metrics['tps']:.2f}")


def run_benchmark(
        num_processes: int,
        key_num: int,
        blob_nums: List[int],
        blob_sizes: List[int],
        iterations: int,
        thread_number: int
):
    """
        start benchmark
        :param num_processes: Number of concurrent processes
        :param key_num: Number of keys per operation
        :param blob_nums: Number of blobs corresponding to each key
        :param blob_sizes: The size of each blob (in bytes)
        :param iterations: Number of iterations for each process
        :param thread_number: Number of threads per process
    """
    # Parameter verification
    if num_processes > 16:
        raise RuntimeError(f"The number of processes must be less than the number of device.")

    # Prepare a multi-process environment
    result_queue = mp.Queue()
    barrier = mp.Barrier(num_processes)
    processes = []
    lock = mp.Lock()

    logging.info(f"Start {num_processes} process...")

    # Start work process
    for i in range(num_processes):
        p = mp.Process(
            target=worker_process,
            args=(
                ((i + args.deviceid) % TOTAL_NPU_COUNT), key_num, blob_nums, blob_sizes, iterations, thread_number,
                result_queue, barrier, lock)
        )
        p.start()
        processes.append(p)

    # Wait for all processes to complete
    for p in processes:
        p.join()

    # Calculate individual request size for metrics
    request_size = 0
    for i, blob_num in enumerate(blob_nums):
        request_size += (key_num * blob_num * blob_sizes[i] * thread_number) / (1024 * 1024)
    
    # Processing data
    handle_metrics(result_queue, iterations, key_num, num_processes, request_size)


def print_performance_result(operate, ops_times, metrics, stats):
    total_byte, duration, throughput, latency_per_req, request_size = stats
    """Print and save perfomance result"""
    logging.info(f"\n{operate} data:")
    logging.info(f"Total number of operations: {ops_times}")
    total_data_mb = f"{total_byte / (1024 * 1024):.2f}"
    logging.info(f"Total data transfer volume: {total_data_mb} MB")
    logging.info(f"Individual request size: {request_size} MB")
    logging.info(f"Transmission duration: {duration:.2f}s")
    logging.info(f"Latency per request: {latency_per_req:.5f}s")
    logging.info(f"Throughput: {throughput:.2f} MB/s")

    logging.info(format_metrics(metrics))
    record_data = (f"{operate},{ops_times},{total_data_mb},{request_size},{duration:.2f},{latency_per_req:.5f},"
                   f"{throughput:.2f},{metrics['min']:.2f},{metrics['max']:.2f},{metrics['mean']:.2f},"
                   f"{metrics['p90']:.2f},{metrics['p95']:.2f},{metrics['p99']:.2f},{metrics['tps']:.2f}")

    write_data(record_data, RESULT_FILENAME)


def handle_metrics(result_queue, iterations, key_num, num_processes, request_size):
    """Process time point into performance information"""
    # Collected results
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    # Calculate overall performance metrics
    all_mset_times = []
    all_mget_times = []

    # Record time boundaries
    mset_start = float('inf')
    mset_end = float('-inf')
    mget_start = float('inf')
    mget_end = float('-inf')

    # total data size
    total_data_bytes = 0

    # Total number of times for each operation
    total_ops = 0

    for res in results:
        all_mset_times.extend(res["mset_times"])
        all_mget_times.extend(res["mget_times"])

        # Update mset time boundary
        if res["mset_times"]:
            mset_start = min(mset_start, res["mset_d2h_start"])
            mset_end = max(mset_end, res["mset_d2h_end"])

        # Update mget time boundary
        if res["mget_times"]:
            mget_start = min(mget_start, res["mget_h2d_start"])
            mget_end = max(mget_end, res["mget_h2d_end"])

        total_data_bytes += res["total_data_bytes"]
        if mode in ["all"] and len(res["mset_times"]) != len(res["mget_times"]):
            raise RuntimeError(f"The number of set length not equal as get length")

        total_ops += len(res["mset_times"])
        if mode in ["get"]:
            total_ops += len(res["mget_times"])

    # Show individual request latencies if requested
    if args.show_all_request_times:
        if mode in ["set", "all"]:
            logging.info(f"Set Request Latencies: {all_mset_times}")
        if mode in ["get", "all"]:
            logging.info(f"Get Request Latencies: {all_mget_times}")

    mset_duration = mset_end - mset_start
    mget_duration = mget_end - mget_start

    # Calculate throughput(MB/s) and average latency per request
    if mode in ["set", "all"]:
        mset_per_req = ((sum(all_mset_times) - (all_mset_times[0] if len(all_mset_times) > 1 else 0)) \
                       / (len(all_mset_times) - (1 if len(all_mset_times) > 1 else 0))) / 1000
        mset_throughput = (request_size / mset_per_req) * num_processes
    if mode in ["get", "all"]:
        mget_per_req = ((sum(all_mget_times) - (all_mget_times[0] if len(all_mget_times) > 1 else 0)) \
                       / (len(all_mget_times) - (1 if len(all_mget_times) > 1 else 0))) / 1000
        mget_throughput = (request_size / mget_per_req) * num_processes
        total_data_bytes *= args.get_multiplier

    # Print summary information
    logging.info(f"\n{'>' * 50}\nTest finish! ")
    if mode in ["set", "all"]:
        stats = (total_data_bytes, mset_duration, mset_throughput, mset_per_req, request_size)
        print_performance_result("mset_d2h", total_ops, calculate_metrics(all_mset_times, mset_duration), stats)
    if mode in ["get", "all"]:
        stats = (total_data_bytes, mget_duration, mget_throughput, mget_per_req, request_size)
        print_performance_result("mget_h2d", total_ops, calculate_metrics(all_mget_times, mget_duration), stats)


if __name__ == "__main__":

    init_file(RESULT_FILENAME)
    parser = argparse.ArgumentParser()

    # Benchmark arguments
    parser.add_argument("--set-only", action="store_true", dest="set_only", help="Only run MSetD2H")
    parser.add_argument("--get-only", action="store_true", dest="get_only", help="Only run MSetD2H")
    parser.add_argument("-i", "--ip", type=str, default=DEFAULT_WORKER_IP,
                        help=f"IP address of the worker (Default {DEFAULT_WORKER_IP})")
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_WORKER_PORT,
                        help=f"Port of the worker (Default {DEFAULT_WORKER_PORT})")
    parser.add_argument("-k", "--keys", type=str, default="./keys.json",
                        help="File path of keys.json to write or read from (Default ./keys.json)")
    parser.add_argument("--no-warmup", action="store_true", dest="no_warmup", help="Skip the initial warmup")
    parser.add_argument("-n", "--name", type=str, default="", help="Name of test to run")
    parser.add_argument("-d", "--deviceid", type=int, default=3,
                       help=f"Starting device id (Default {DEVICE_START_INDEX})")
    parser.add_argument("--get-multiplier", type=int, default=1, dest="get_multiplier",
                        help="Multiply the amount of get requests by x amount")
    parser.add_argument("--show-all-request-times", action="store_true", dest="show_all_request_times",
                        help="Show the individual latency of each request")
    args = parser.parse_args()

    if args.set_only and not args.get_only:
        mode = "set"
    elif args.get_only and not args.set_only:
        mode = "get"

    for test_name, value in config_map.items():
        if test_name == args.name or (mode in ["all"] and not args.name):
            logging.info(f"\n{'=' * 50} Benchmark start {'=' * 50}")
            current_test = test_name # Save current test name globally
            CONFIG = {
                "num_processes": value["num_processes"],  # multi process nums (bind diff devs)
                "key_num": value["key_num"],  # key nums for one operate
                "blob_nums": value["blob_nums"],  # blob nums for one key
                "blob_sizes": value["blob_sizes"],  # blob sizes (byte)
                "iterations": value["iterations"],  # the iterations for performance test
                "thread_number": value["thread_number"]  # the thread num for one process
            }

            if len(CONFIG['blob_nums']) != len(CONFIG['blob_sizes']):
                logging.info(f"blob_nums size does not match blob_sizes for test {test_name}")
                continue

            total_size = 0
            for i in range(len(CONFIG["blob_nums"])):
                total_size += CONFIG['num_processes'] * CONFIG['key_num'] * CONFIG['blob_nums'][i] *\
                              CONFIG['blob_sizes'][i] * CONFIG['iterations'] * CONFIG['thread_number'] / (1024 * 1024)
            logging.info(
                f"Perf conf : num_processes: {CONFIG['num_processes']} | thread_num: {CONFIG['thread_number']} | "
                f"key_num: {CONFIG['key_num']} | blob_nums: {CONFIG['blob_nums']} | blob_sizes: {CONFIG['blob_sizes']}"
                f" | iterations: {CONFIG['iterations']} | total_size: {total_size} MB")

            run_benchmark(**CONFIG)
