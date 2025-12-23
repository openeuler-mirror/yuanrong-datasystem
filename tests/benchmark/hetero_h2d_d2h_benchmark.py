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
WORKER_IP = "127.0.0.1"
WORKER_PORT = 31699

DEVICE_START_INDEX = 4

timestamp_sec = int(time.time())
RESULT_FILENAME = f"perf_result_{timestamp_sec}.csv"

config_map = {
    '1process_1thread_72KB': {'num_processes': 1, 'key_num': 32, 'blob_num': 61, 'blob_size': 72 * 1024,
                              'iterations': 100, 'thread_number': 1},
    '1process_1thread_144KB': {'num_processes': 1, 'key_num': 32, 'blob_num': 61, 'blob_size': 144 * 1024,
                               'iterations': 100, 'thread_number': 1},
    '1process_1thread_1024KB': {'num_processes': 1, 'key_num': 32, 'blob_num': 28, 'blob_size': 1024 * 1024,
                                'iterations': 50, 'thread_number': 1},

    '1process_16thread_72KB': {'num_processes': 1, 'key_num': 32, 'blob_num': 61, 'blob_size': 72 * 1024,
                               'iterations': 30, 'thread_number': 16},
    '1process_16thread_144KB': {'num_processes': 1, 'key_num': 32, 'blob_num': 61, 'blob_size': 144 * 1024,
                                'iterations': 15, 'thread_number': 16},
    '1process_8thread_1024KB': {'num_processes': 1, 'key_num': 32, 'blob_num': 28, 'blob_size': 1024 * 1024,
                                'iterations': 10, 'thread_number': 8},

    '8process_1thread_72KB': {'num_processes': 8, 'key_num': 32, 'blob_num': 61, 'blob_size': 72 * 1024,
                              'iterations': 50, 'thread_number': 1},
    '8process_1thread_144KB': {'num_processes': 8, 'key_num': 32, 'blob_num': 61, 'blob_size': 144 * 1024,
                               'iterations': 30, 'thread_number': 1},
    '8process_1thread_1024KB': {'num_processes': 8, 'key_num': 32, 'blob_num': 28, 'blob_size': 1024 * 1024,
                                'iterations': 10, 'thread_number': 1}
}


def init_test_hetero_client():
    """Initialize hetero client"""
    client = HeteroClient(WORKER_IP, WORKER_PORT)
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
        blob_nums_per_key: int,
        blob_size: int,
        iterations: int,
        thread_number: int,
        result_queue: mp.Queue,
        barrier: mp.Barrier,
):
    """worker process : execute benchmark and collect result"""

    logging.info(f"[Dev {device_id}]Start perfromance test")
    acl.init()
    acl.rt.set_device(device_id)
    client = init_test_hetero_client()
    logging.info(f"[Dev {device_id}]Success init hetero client ")

    # record the time point
    metrics_data = {"mset_times": [], "mget_times": []}

    # prepare data
    test_value = random_str(blob_size)
    all_in_data_blob_list = []
    all_out_data_blob_list = []
    prepare_hbm_data(test_value, thread_number, key_num, blob_nums_per_key, blob_size, device_id, all_in_data_blob_list,
                     all_out_data_blob_list)

    # warm up
    keys_list = [f"dev{device_id}_key{i}" for i in range(key_num)]
    client.mset_d2h(keys_list, all_in_data_blob_list[0])
    client.mget_h2d(keys_list, all_out_data_blob_list[0], 60000)
    client.delete(keys_list)

    # wait all process ready，start mset_d2h at same time
    blocking_wait_sync(barrier, "mset_d2h", device_id)

    logging.info(f"[Dev {device_id}]Start mset_d2h")
    list_random_suffix = []
    for _ in range(thread_number):
        random_suffix = random_str(10)
        list_random_suffix.append(random_suffix)
    metrics_data["mset_d2h_start"] = time.perf_counter()

    shared_matrix = [[] for _ in range(thread_number)]
    operate_thread("mset_thread", client, device_id, key_num, iterations, list_random_suffix, all_out_data_blob_list,
                   shared_matrix)
    for sublist in shared_matrix:
        metrics_data["mset_times"].extend(sublist)

    metrics_data["mset_d2h_end"] = time.perf_counter()

    # wait all process ready，start mget_h2d at same time
    blocking_wait_sync(barrier, "mget_h2d", device_id)

    logging.info(f"[Dev {device_id}]Start mget_h2d")
    metrics_data["mget_h2d_start"] = time.perf_counter()

    shared_matrix = [[] for _ in range(thread_number)]
    operate_thread("mget_thread", client, device_id, key_num, iterations, list_random_suffix, all_out_data_blob_list,
                   shared_matrix)
    for sublist in shared_matrix:
        metrics_data["mget_times"].extend(sublist)

    metrics_data["mget_h2d_end"] = time.perf_counter()
    logging.info(f"[Dev {device_id}]Success mget_h2d")
    batch_data_check(all_out_data_blob_list[0], test_value)

    # clear data
    for thread_id in range(thread_number):
        for i in range(iterations):
            del_keys = [f"dev{device_id}_key{i}_{j}_{list_random_suffix[thread_id]}" for j in range(key_num)]
            client.delete(del_keys)
    logging.info(f"[Dev {device_id}]Success delete data")

    # cal the tital data bytes
    metrics_data["total_data_bytes"] = key_num * blob_nums_per_key * blob_size * iterations * thread_number

    # construct the record points
    result_queue.put(metrics_data)

    logging.info(f"[Dev {device_id}]Success test perfromance")
    acl.finalize()
    time.sleep(1)


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
        blob_num: int,
        blob_size: int,
        iterations: int,
        thread_number: int
):
    """
        start benchmark
        :param num_processes: Number of concurrent processes
        :param key_num: Number of keys per operation
        :param blob_num: Number of blobs corresponding to each key
        :param blob_size: The size of each blob (in bytes)
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

    logging.info(f"Start {num_processes} process...")

    # Start work process
    for i in range(num_processes):
        p = mp.Process(
            target=worker_process,
            args=(
                i + DEVICE_START_INDEX, key_num, blob_num, blob_size, iterations, thread_number, result_queue, barrier)
        )
        p.start()
        processes.append(p)

    # Wait for all processes to complete
    for p in processes:
        p.join()

    # Processing data
    handle_metrics(result_queue)


def print_performance_result(operate, ops_times, total_byte, duration, throughput, metrics):
    """Print and save perfomance result"""
    logging.info(f"\n{operate} data:")
    logging.info(f"Total number of operations: {ops_times}")
    total_data_mb = f"{total_byte / (1024 * 1024):.2f}"
    logging.info(f"Total data transfer volume: {total_data_mb} MB")
    logging.info(f"Transmission duration: {duration:.2f}s")
    logging.info(f"Throughput: {throughput:.2f} MB/s")
    logging.info(format_metrics(metrics))
    record_data = (f"{operate},{ops_times},{total_data_mb},{duration:.2f},{throughput:.2f},{metrics['min']:.2f},"
                   f"{metrics['max']:.2f},{metrics['mean']:.2f},{metrics['p90']:.2f},{metrics['p95']:.2f},"
                   f"{metrics['p99']:.2f},{metrics['tps']:.2f}")
    write_data(record_data, RESULT_FILENAME)


def handle_metrics(result_queue):
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
        if len(res["mset_times"]) != len(res["mget_times"]):
            raise RuntimeError(f"The number of set length not equal as get length")

        total_ops += len(res["mset_times"])

    mset_duration = mset_end - mset_start
    mget_duration = mget_end - mget_start

    # Calculate throughput(MB/s)
    mset_throughput = (total_data_bytes / mset_duration) / (1024 * 1024)
    mget_throughput = (total_data_bytes / mget_duration) / (1024 * 1024)

    # Print summary information
    logging.info(f"\n{'>' * 50}\nTest finish! ")
    print_performance_result("mset_d2h", total_ops, total_data_bytes, mset_duration, mset_throughput,
                             calculate_metrics(all_mset_times, mset_duration))
    print_performance_result("mget_h2d", total_ops, total_data_bytes, mget_duration, mget_throughput,
                             calculate_metrics(all_mget_times, mget_duration))


if __name__ == "__main__":

    init_file(RESULT_FILENAME)

    for _, value in config_map.items():
        logging.info(f"\n{'=' * 50} Benchmark start {'=' * 50}")
        CONFIG = {
            "num_processes": value["num_processes"],  # multi process nums (bind diff devs)
            "key_num": value["key_num"],  # key nums for one operate
            "blob_num": value["blob_num"],  # blob nums for one key
            "blob_size": value["blob_size"],  # blob size (byte)
            "iterations": value["iterations"],  # the iterations for performance test
            "thread_number": value["thread_number"]  # the thread num for one process
        }
        total_size = CONFIG['num_processes'] * CONFIG['key_num'] * CONFIG['blob_num'] * CONFIG['blob_size'] * CONFIG[
            'iterations'] * CONFIG['thread_number'] / (1024 * 1024)
        logging.info(
            f"Perf conf : num_processes: {CONFIG['num_processes']} | thread_num: {CONFIG['thread_number']} | "
            f"key_num: {CONFIG['key_num']} | blob_num: {CONFIG['blob_num']} | blob_size: {CONFIG['blob_size']} | "
            f"iterations: {CONFIG['iterations']} | total_size: {total_size} MB")

        run_benchmark(**CONFIG)
