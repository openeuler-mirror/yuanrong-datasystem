#!/usr/bin/env python3
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
trace_collector.py - Unified trace collection tool for scripts/perf_analyze.

Supports four modes:
  1. core: Extract traces from access logs by search string, then search in parallel.
  2. time: Extract traces by operation type and latency range, then search in parallel.
  3. percentile: Extract high-latency traces by percentile, then search in parallel.
  4. all-core: Extract traces for every non-zero access-log status code, then search in parallel.

Directory layout:
  Place this script beside collected/ and collected_worker_logs/.

  /parent/                         # Script directory
  |-- trace_collector.py            # This script
  |-- unique_traces_*.txt           # Generated trace files
  |-- collected/                    # Client logs used to extract traces
  |   |-- client-worker/
  |   |   |-- ds_client_access.log  # Trace source
  |   |   |-- ds_client.INFO.log    # Detailed log search source
  |   |   `-- ds_client.INFO.log.gz # Compressed detailed log search source
  |   `-- ...
  |-- collected_worker_logs/        # Worker logs used only for searching
  |   |-- worker/
  |   |   `-- kvcache.INFO.log
  |   `-- ...
  `-- trace_collect/                # Output root (default)
      |-- core/                     # Core-mode results
      |-- time/                     # Time-mode results
      `-- percentile/               # Percentile-mode results

Examples:
  # core mode: extract traces by search string
  python3 trace_collector.py --type core "| 1001 | DS"
  python3 trace_collector.py --type core "| 1001 | DS:| 1002 | DS"

  # all-core mode: collect every non-zero access-log status code
  python3 trace_collector.py --type all-core

  # time mode: extract traces by operation type and latency range
  python3 trace_collector.py --type time DS_KV_CLIENT_GET 1000,2000
  python3 trace_collector.py --type time DS_KV_CLIENT_GET 1000
  python3 trace_collector.py --type time DS_KV_CLIENT_GET ,2000
  python3 trace_collector.py --type time "DS_KV_CLIENT_GET:DS_KV_CLIENT_PUT" 1000,2000

  # percentile mode: extract high-latency traces by percentile
  python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99
  python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99.9
  python3 trace_collector.py --type percentile "DS_KV_CLIENT_GET:DS_KV_CLIENT_PUT" P99.99
"""

import os
import sys
import subprocess
import re
import random
import math
import argparse
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed


DEFAULT_MAX_TRACES = 96
DEFAULT_JOBS = 32
ACCESS_STATUS_CODE_RE = re.compile(r'\|\s*(\d+)\s*\|\s*DS(?:_|\b)')


def sanitize_filename(name: str) -> str:
    """Convert a trace string to a valid filename."""
    name = name.strip()
    invalid_chars = '/\\:*?"<>|\n\r\t'
    for ch in invalid_chars:
        name = name.replace(ch, '_')
    if not name:
        name = "empty_trace"
    return name


def sanitize_dirname(name: str) -> str:
    """Convert a string to a valid directory name."""
    name = name.strip()
    invalid_chars = '/\\:*?"<>|\n\r\t| '
    for ch in invalid_chars:
        name = name.replace(ch, '_')
    while '__' in name:
        name = name.replace('__', '_')
    name = name.strip('_')
    if not name:
        name = "default"
    return name


def mode_output_dir(output_root: str, mode: str) -> str:
    """Return the dedicated output directory for one collection mode."""
    return os.path.join(output_root, mode)


def extract_trace_id(line: str) -> str:
    """Extract the trace ID from the structured access-log trace column."""
    parts = line.split('|')
    if len(parts) <= 5:
        return ""
    return parts[5].strip()


def run_grep_and_zgrep(trace: str, search_dirs: list, gz_files_by_dir: dict, output_path: str) -> dict:
    """Search one trace across directories and write results to a file."""
    trace = trace.strip()
    if not trace:
        return {"trace": trace, "status": "skipped"}

    results = []

    for search_dir in search_dirs:
        dir_name = os.path.basename(search_dir)

        # grep -raFn: recursive, binary-safe fixed-string matching with line numbers.
        try:
            result = subprocess.run(
                ["grep", "-raFn", trace, search_dir],
                capture_output=True, text=True,
                encoding='utf-8', errors='replace',
                timeout=300
            )
            if result.stdout:
                results.append(f"=== grep -raFn '{trace}' {search_dir} ({dir_name}) ===\n")
                results.append(result.stdout)
            if result.stderr:
                results.append(f"\n[STDERR grep {dir_name}]\n{result.stderr}\n")
        except subprocess.TimeoutExpired:
            results.append(f"\n[TIMEOUT] grep timed out ({dir_name}): {trace}\n")
        except Exception as e:
            results.append(f"\n[ERROR grep {dir_name}] {e}\n")

        # zgrep -aFn: binary-safe fixed-string matching with line numbers.
        try:
            gz_files = gz_files_by_dir[search_dir]

            if gz_files:
                result = subprocess.run(
                    ["zgrep", "-aFn", trace] + gz_files,
                    capture_output=True, text=True,
                    encoding='utf-8', errors='replace',
                    timeout=300
                )
                if result.stdout:
                    results.append(f"\n=== zgrep -aFn '{trace}' {search_dir}/**/*.gz ({dir_name}) ===\n")
                    results.append(result.stdout)
                if result.stderr:
                    results.append(f"\n[STDERR zgrep {dir_name}]\n{result.stderr}\n")
            else:
                results.append(f"\n[INFO] No .gz files found in {dir_name}\n")
        except subprocess.TimeoutExpired:
            results.append(f"\n[TIMEOUT] zgrep timed out ({dir_name}): {trace}\n")
        except Exception as e:
            results.append(f"\n[ERROR zgrep {dir_name}] {e}\n")

    # Write merged results for this trace.
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            if results:
                f.write(''.join(results))
            else:
                f.write(f"[NO MATCH] No matches found for trace '{trace}'\n")
    except Exception as e:
        return {"trace": trace, "status": "error", "error": f"Failed to write file: {str(e)}"}

    match_count = sum(1 for r in results if r.startswith("==="))
    return {"trace": trace, "status": "success", "sections": match_count}


def search_traces(traces: list, search_dirs: list, output_dir: str, jobs: int) -> None:
    """Search all traces in parallel and write results to the output directory."""
    total = len(traces)
    if total == 0:
        print("  No traces to search")
        return

    gz_files_by_dir = {}
    for d in search_dirs:
        gz_files = []
        for root, _, files in os.walk(d):
            gz_files.extend(
                os.path.join(root, filename)
                for filename in files
                if filename.endswith('.gz')
            )
        gz_files_by_dir[d] = gz_files
    total_gz = sum(len(gz_files) for gz_files in gz_files_by_dir.values())
    print(f"  Found {total_gz} .gz files")
    print(f"  Processing {total} traces in parallel...")

    print(f"  Using {jobs} threads")

    os.makedirs(output_dir, exist_ok=True)

    completed = 0
    success_count = 0

    with ThreadPoolExecutor(max_workers=jobs) as executor:
        future_to_trace = {}
        for trace in traces:
            filename = sanitize_filename(trace)
            output_path = os.path.join(output_dir, filename)

            counter = 1
            orig = output_path
            while os.path.exists(output_path):
                output_path = f"{orig}_{counter}"
                counter += 1

            future = executor.submit(
                run_grep_and_zgrep,
                trace,
                search_dirs,
                gz_files_by_dir,
                output_path,
            )
            future_to_trace[future] = trace

        for future in as_completed(future_to_trace):
            result = future.result()
            completed += 1

            if result["status"] == "success":
                success_count += 1
                print(f"  [{completed}/{total}] OK {result['trace'][:50]}... -> {result['sections']} sections")
            elif result["status"] == "skipped":
                print(f"  [{completed}/{total}] SKIPPED empty trace")
            else:
                print(f"  [{completed}/{total}] FAILED {result['trace'][:50]}... -> {result['status']}")

    print(f"  Search complete: {success_count}/{total} succeeded")


def limit_traces(traces: list, max_traces: int) -> list:
    """Randomly retain at most max_traces unique traces."""
    total = len(traces)
    if total > max_traces:
        sampled = random.sample(traces, max_traces)
        print(f"  Trace limit: {total} > {max_traces}; sampled {max_traces}")
        return sorted(sampled)
    else:
        print(f"  Trace count {total} <= {max_traces}; retaining all")
        return sorted(traces)


def find_access_log_files(collected_dir: str) -> list:
    """Return every plain or gzip-compressed client access log under collected_dir."""
    access_files = []
    for root, _, files in os.walk(collected_dir):
        for filename in files:
            if not filename.startswith("ds_client_access_"):
                continue
            if filename.endswith(".log") or filename.endswith(".log.gz"):
                access_files.append(os.path.join(root, filename))
    return sorted(access_files)


def open_access_log(filepath: str):
    """Open a plain or gzip-compressed access log as replacement-decoded text."""
    opener = gzip.open if filepath.endswith('.gz') else open
    return opener(filepath, 'rt', encoding='utf-8', errors='replace')


# ==================== Core mode ====================

def extract_traces_core(search_core: str, collected_dir: str) -> list:
    """
    Extract trace UUIDs from collected/ ds_client_access_*.log files.
    Workers without access logs are skipped.
    """
    print(f"  Search string: '{search_core}'")

    access_files = find_access_log_files(collected_dir)

    if not access_files:
        print("  [WARNING] No access logs found")
        return []

    print(f"  Found {len(access_files)} access logs")

    traces = set()

    for filepath in access_files:
        try:
            with open_access_log(filepath) as f:
                for line in f:
                    if search_core in line:
                        trace = extract_trace_id(line)
                        if trace:
                            traces.add(trace)
        except Exception as e:
            print(f"  [SKIPPED] {os.path.basename(filepath)}: {e}")

    all_traces = sorted(traces)
    print(f"  Extracted {len(all_traces)} unique traces")
    return all_traces


def process_core(search_core: str, collected_dir: str, logs_dir: str, base_output_dir: str,
                 trace_file_base: str, max_traces: int, jobs: int) -> None:
    """Process one search string in core mode."""
    core_dirname = sanitize_dirname(search_core)
    output_dir = os.path.join(base_output_dir, core_dirname)
    trace_file = os.path.join(base_output_dir, f"{trace_file_base}_{core_dirname}.txt")

    os.makedirs(base_output_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"[core mode] Processing search string: '{search_core}'")
    print(f"  Output subdirectory: {core_dirname}")
    print(f"{'='*60}")

    # Step 1: Extract traces.
    all_traces = extract_traces_core(search_core, collected_dir)

    # Step 2: Limit traces.
    traces = limit_traces(all_traces, max_traces)

    # Write trace file.
    with open(trace_file, 'w', encoding='utf-8') as f:
        for t in traces:
            f.write(t + '\n')
    print(f"  Wrote trace file: {trace_file}")

    # Step 3: Search in parallel.
    search_dirs = [collected_dir, logs_dir]
    search_traces(traces, search_dirs, output_dir, jobs)

    print(f"  Search string '{search_core}' complete, results: {output_dir}/")


def extract_traces_by_status_code(collected_dir: str) -> dict:
    """Extract trace IDs grouped by non-zero status code in one access-log pass."""
    access_files = find_access_log_files(collected_dir)
    if not access_files:
        print("  [WARNING] No access logs found")
        return {}

    print(f"  Found {len(access_files)} access logs")
    traces_by_code = {}
    for filepath in access_files:
        try:
            with open_access_log(filepath) as f:
                for line in f:
                    match = ACCESS_STATUS_CODE_RE.search(line)
                    if match is None:
                        continue
                    code = match.group(1)
                    if int(code) == 0:
                        continue
                    trace = extract_trace_id(line)
                    if trace:
                        traces_by_code.setdefault(code, set()).add(trace)
        except Exception as e:
            print(f"  [SKIPPED] {os.path.basename(filepath)}: {e}")

    print(f"  Extracted {len(traces_by_code)} non-zero status codes")
    return traces_by_code


def process_all_core(collected_dir: str, logs_dir: str, base_output_dir: str, trace_file_base: str,
                     max_traces: int, jobs: int) -> None:
    """Collect traces for every access-log error code with an independent trace limit."""
    print(f"\n{'='*60}")
    print("[all-core mode] Extracting all non-zero access-log status codes")
    print(f"{'='*60}")
    os.makedirs(base_output_dir, exist_ok=True)
    traces_by_code = extract_traces_by_status_code(collected_dir)
    if not traces_by_code:
        return

    for code in sorted(traces_by_code, key=int):
        all_traces = sorted(traces_by_code[code])
        code_dir = os.path.join(base_output_dir, code)
        trace_file = os.path.join(base_output_dir, f"{trace_file_base}_{code}.txt")
        print(f"\n{'='*60}")
        print(f"[all-core mode] Processing status code: {code}")
        print(f"  Extracted {len(all_traces)} unique traces")
        print(f"  Output subdirectory: {code}")
        print(f"{'='*60}")

        traces = limit_traces(all_traces, max_traces)
        with open(trace_file, 'w', encoding='utf-8') as f:
            f.write(f"# Status code: {code}\n")
            f.write(f"# Total traces: {len(all_traces)}\n")
            f.write(f"# Final traces: {len(traces)}\n")
            for trace in traces:
                f.write(trace + '\n')
        print(f"  Wrote trace file: {trace_file}")
        search_traces(traces, [collected_dir, logs_dir], code_dir, jobs)
        print(f"  Status code {code} complete, results: {code_dir}/")


# ==================== Time mode ====================

def parse_time_range(time_str: str) -> tuple:
    """
    Parse a latency range string.
    "1000,2000" -> (1000, 2000), greater than 1000 and less than 2000.
    "1000"      -> (1000, None), greater than 1000.
    ",2000"     -> (None, 2000), less than 2000.
    ""          -> (None, None), no limit.
    """
    if not time_str or time_str.strip() == '':
        return (None, None)

    parts = time_str.strip().split(',')
    if len(parts) == 1:
        return (float(parts[0].strip()), None)
    elif len(parts) == 2:
        lower = parts[0].strip()
        upper = parts[1].strip()
        return (
            float(lower) if lower else None,
            float(upper) if upper else None
        )
    else:
        raise ValueError(f"Invalid latency range format: {time_str}")


def check_time_value(time_val: float, time_range: tuple) -> bool:
    """Return whether a latency value is within the requested range."""
    lower, upper = time_range
    if lower is not None and time_val <= lower:
        return False
    if upper is not None and time_val >= upper:
        return False
    return True


def extract_traces_time(op_type: str, time_range: tuple, collected_dir: str) -> list:
    """
    Extract traces from collected/ access logs by operation type and latency range.
    """
    print(f"  Operation type: {op_type}")
    print("  Latency range: ", end="")
    lower, upper = time_range
    if lower is not None and upper is not None:
        print(f"{lower} < time < {upper}")
    elif lower is not None:
        print(f"time > {lower}")
    elif upper is not None:
        print(f"time < {upper}")
    else:
        print("unlimited")

    access_files = find_access_log_files(collected_dir)

    if not access_files:
        print("  [WARNING] No access logs found")
        return []

    print(f"  Found {len(access_files)} access logs")

    traces = set()

    for filepath in access_files:
        try:
            with open_access_log(filepath) as f:
                match_count = 0
                for line in f:
                    if op_type not in line:
                        continue

                    parts = line.split('|')
                    if len(parts) < 10:
                        continue

                    time_str = parts[9].strip()
                    try:
                        time_val = float(time_str)
                    except ValueError:
                        continue

                    if not check_time_value(time_val, time_range):
                        continue

                    trace = extract_trace_id(line)
                    if trace:
                        traces.add(trace)
                        match_count += 1

                if match_count > 0:
                    print(f"  OK {os.path.basename(filepath)}: {match_count} traces")

        except Exception as e:
            print(f"  [SKIPPED] {os.path.basename(filepath)}: {e}")

    all_traces = sorted(traces)
    print(f"  Extracted {len(all_traces)} unique traces")
    return all_traces


def process_time(op_type: str, time_range: tuple, collected_dir: str, logs_dir: str,
                 base_output_dir: str, trace_file_base: str, max_traces: int, jobs: int) -> None:
    """Process one operation type in time mode."""
    lower_bound = int(time_range[0]) if time_range[0] else ''
    upper_bound = int(time_range[1]) if time_range[1] else ''
    core_dirname = sanitize_dirname(f"{op_type}_{lower_bound}_{upper_bound}")
    output_dir = os.path.join(base_output_dir, core_dirname)
    trace_file = os.path.join(base_output_dir, f"{trace_file_base}_{core_dirname}.txt")

    os.makedirs(base_output_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"[time mode] Processing operation type: {op_type}")
    print(f"  Output subdirectory: {core_dirname}")
    print(f"{'='*60}")

    # Step 1: Extract traces.
    all_traces = extract_traces_time(op_type, time_range, collected_dir)

    # Step 2: Limit traces.
    traces = limit_traces(all_traces, max_traces)

    # Write trace file.
    with open(trace_file, 'w', encoding='utf-8') as f:
        for t in traces:
            f.write(t + '\n')
    print(f"  Wrote trace file: {trace_file}")

    # Step 3: Search in parallel.
    search_dirs = [collected_dir, logs_dir]
    search_traces(traces, search_dirs, output_dir, jobs)

    print(f"  Operation '{op_type}' complete, results: {output_dir}/")


# ==================== Percentile mode ====================

def parse_percentile(p_str: str) -> float:
    """
    Parse a percentile string.
    P99 -> 0.99, P99.9 -> 0.999, P99.99 -> 0.9999
    """
    p_str = p_str.strip().upper()
    if not p_str.startswith('P'):
        raise ValueError(f"Invalid percentile format: {p_str}; expected P99/P99.9/P99.99")

    num_str = p_str[1:]
    try:
        num = float(num_str)
    except ValueError:
        raise ValueError(f"Invalid percentile format: {p_str}")

    if not (0 < num < 100):
        raise ValueError(f"Percentile must be between 0 and 100: {num}")

    return num / 100.0


def calculate_percentile(values: list, percentile: float) -> float:
    """Calculate a percentile using linear interpolation."""
    if not values:
        return 0.0

    sorted_values = sorted(values)
    n = len(sorted_values)

    index = (n - 1) * percentile
    lower_idx = int(math.floor(index))
    upper_idx = int(math.ceil(index))

    if lower_idx == upper_idx:
        return sorted_values[lower_idx]

    weight = index - lower_idx
    return sorted_values[lower_idx] * (1 - weight) + sorted_values[upper_idx] * weight


def extract_times_and_traces(op_type: str, collected_dir: str) -> tuple:
    """
    Extract all latency values and associated traces from access logs.
    Returns: (times_list, time_trace_pairs).
    """
    print(f"  Operation type: {op_type}")

    access_files = find_access_log_files(collected_dir)

    if not access_files:
        print("  [WARNING] No access logs found")
        return [], []

    print(f"  Found {len(access_files)} access logs")

    times = []
    time_trace_pairs = []

    for filepath in access_files:
        try:
            with open_access_log(filepath) as f:
                file_times = 0
                for line in f:
                    if op_type not in line:
                        continue

                    parts = line.split('|')
                    if len(parts) < 10:
                        continue

                    time_str = parts[9].strip()
                    try:
                        time_val = float(time_str)
                    except ValueError:
                        continue

                    trace = extract_trace_id(line)
                    if trace:
                        times.append(time_val)
                        time_trace_pairs.append((time_val, trace))
                        file_times += 1

                if file_times > 0:
                    print(f"  OK {os.path.basename(filepath)}: {file_times} records")

        except Exception as e:
            print(f"  [SKIPPED] {os.path.basename(filepath)}: {e}")

    print(f"  Extracted {len(times)} records")
    return times, time_trace_pairs


def filter_traces_by_percentile(times: list, time_trace_pairs: list, percentile: float) -> tuple:
    """
    Filter traces by percentile.
    Returns: (threshold, traces).
    """
    if not times:
        return 0.0, []

    threshold = calculate_percentile(times, percentile)
    p_str = f"P{percentile * 100}".replace('.0', '')

    print("\n  Latency statistics:")
    print(f"    Min: {min(times):.2f}")
    print(f"    Max: {max(times):.2f}")
    print(f"    Average: {sum(times)/len(times):.2f}")
    print(f"    Median: {calculate_percentile(times, 0.5):.2f}")
    print(f"    {p_str}: {threshold:.2f}")

    seen = set()
    filtered_traces = []
    for time_val, trace in time_trace_pairs:
        if time_val >= threshold and trace not in seen:
            seen.add(trace)
            filtered_traces.append(trace)

    print(f"  Traces >= {p_str} ({threshold:.2f}): {len(filtered_traces)}")

    return threshold, filtered_traces


def process_percentile(op_type: str, percentile: float, collected_dir: str, logs_dir: str,
                       base_output_dir: str, trace_file_base: str, max_traces: int, jobs: int) -> None:
    """Process one operation type in percentile mode."""
    p_str = f"P{percentile * 100}".replace('.0', '')
    core_dirname = sanitize_dirname(f"{op_type}_{p_str}")
    output_dir = os.path.join(base_output_dir, core_dirname)
    trace_file = os.path.join(base_output_dir, f"{trace_file_base}_{core_dirname}.txt")

    os.makedirs(base_output_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"[percentile mode] Processing operation type: {op_type}")
    print(f"  Percentile: {p_str}")
    print(f"  Output subdirectory: {core_dirname}")
    print(f"{'='*60}")

    # Step 1: Extract all latency values and traces.
    times, time_trace_pairs = extract_times_and_traces(op_type, collected_dir)

    if not times:
        print("  [WARNING] No data extracted; skipping")
        return

    # Step 2: Filter by percentile.
    threshold, filtered_traces = filter_traces_by_percentile(times, time_trace_pairs, percentile)

    # Step 3: Limit traces.
    traces = limit_traces(filtered_traces, max_traces)

    # Write trace file.
    with open(trace_file, 'w', encoding='utf-8') as f:
        f.write(f"# Operation type: {op_type}\n")
        f.write(f"# Percentile: {p_str}\n")
        f.write(f"# Threshold: {threshold:.2f}\n")
        f.write(f"# Total records: {len(times)}\n")
        f.write(f"# Filtered traces: {len(filtered_traces)}\n")
        f.write(f"# Final traces: {len(traces)}\n")
        f.write("#" + "=" * 50 + "\n")
        for t in traces:
            f.write(t + '\n')
    print(f"  Wrote trace file: {trace_file}")

    # Step 4: Search in parallel.
    search_dirs = [collected_dir, logs_dir]
    search_traces(traces, search_dirs, output_dir, jobs)

    print(f"  Operation '{op_type}' complete, results: {output_dir}/")


# ==================== Main entry point ====================

def main():
    # Append the directory layout and mode details to the standard help text.
    class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter):
        def format_help(self):
            help_text = super().format_help()
            dir_structure = """

Directory layout:
  Place this script beside collected/ and collected_worker_logs/.

  /parent/                         # Script directory
  |-- trace_collector.py            # This script
  |-- unique_traces_*.txt           # Generated trace files
  |-- collected/                    # Client logs used to extract traces
  |   |-- client-worker/
  |   |   |-- ds_client_access.log  # Trace source
  |   |   |-- ds_client.INFO.log    # Detailed log search source
  |   |   `-- ds_client.INFO.log.gz # Compressed detailed log search source
  |   `-- ...
  |-- collected_worker_logs/        # Worker logs used only for searching
  |   |-- worker/
  |   |   `-- kvcache.INFO.log
  |   `-- ...
  `-- [output_dir]/                 # Output subdirectories created by mode
      |-- core_xxx/                 # Core mode output
      |-- all-core/                 # All error-code output
      |   |-- 1001/                 # One status code per subdirectory
      |-- time_xxx/                 # Time mode output
      `-- percentile_xxx/           # Percentile mode output

Modes:

  [core mode]
    Extract traces from access logs by search string, then search all logs in parallel.
    Use when a known log marker, such as "| 1001 | DS", identifies the target traces.

    Examples:
      python3 trace_collector.py --type core "| 1001 | DS"
      python3 trace_collector.py --type core "| 1001 | DS:| 1002 | DS"

  [all-core mode]
    Extract every non-zero status code from client access logs. Each status code gets an
    independent --max-traces limit and a separate output subdirectory. No positional value is used.

    Example:
      python3 trace_collector.py --type all-core

  [time mode]
    Extract traces from access logs by operation type and latency range, then search in parallel.
    Use when locating traces for an operation within a latency range.

    Examples:
      python3 trace_collector.py --type time DS_KV_CLIENT_GET 1000,2000   # 1000 < time < 2000
      python3 trace_collector.py --type time DS_KV_CLIENT_GET 1000        # time > 1000
      python3 trace_collector.py --type time DS_KV_CLIENT_GET ,2000       # time < 2000
      python3 trace_collector.py --type time "GET:PUT" 1000,2000          # Multiple operation types

  [percentile mode]
    Calculate P99/P99.9/P99.99 by operation type, extract traces at or above the threshold, then search in parallel.
    Use when locating tail-latency traces for an operation.

    Examples:
      python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99
      python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99.9
      python3 trace_collector.py --type percentile "GET:PUT" P99.99

Common options:
  --collected-dir   Client log directory (default: collected)
  --logs-dir        Worker log directory (default: collected_worker_logs)
  --output-dir      Output root directory (default depends on mode)
  --trace-file      Trace file prefix (default: unique_traces)
"""
            return help_text + dir_structure

    parser = argparse.ArgumentParser(
        description="Unified trace collection tool supporting core, all-core, time, and percentile modes",
        formatter_class=CustomHelpFormatter,
        epilog="""
Examples:
  # core mode
  python3 trace_collector.py --type core "| 1001 | DS"

  # all-core mode
  python3 trace_collector.py --type all-core

  # time mode
  python3 trace_collector.py --type time DS_KV_CLIENT_GET 1000,2000

  # percentile mode
  python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99.9
        """
    )

    parser.add_argument(
        "--type",
        required=True,
        choices=["core", "all-core", "all_core", "time", "percentile"],
        help=("Mode: core (search string), all-core (all non-zero status codes), "
              "time (latency range), or percentile (latency percentile)")
    )
    parser.add_argument(
        "values",
        nargs='*',
        help=("Mode arguments: core uses search strings separated by ':', all-core uses none, "
              "time uses operation type and latency range, percentile uses operation type and percentile")
    )
    parser.add_argument(
        "--collected-dir",
        default="collected",
        help="Client log directory used to extract and search traces (default: collected)"
    )
    parser.add_argument(
        "--logs-dir",
        default="collected_worker_logs",
        help="Worker log directory used only for searching (default: collected_worker_logs)"
    )
    parser.add_argument(
        "--output-dir",
        default="trace_collect",
        help="Output root directory; results are placed in a mode-specific subdirectory (default: trace_collect)"
    )
    parser.add_argument(
        "--trace-file",
        default="unique_traces",
        help="Trace file prefix (default: unique_traces)"
    )
    parser.add_argument(
        "--max-traces",
        type=int,
        default=DEFAULT_MAX_TRACES,
        help=f"Randomly retain at most this many traces per filter (default: {DEFAULT_MAX_TRACES})"
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=DEFAULT_JOBS,
        help=f"Parallel trace-search workers (default: {DEFAULT_JOBS})"
    )

    args = parser.parse_args()
    if args.type == "all_core":
        args.type = "all-core"

    if args.max_traces < 1:
        parser.error("--max-traces must be at least 1")
    if args.jobs < 1:
        parser.error("--jobs must be at least 1")

    exec_dir = os.path.dirname(os.path.abspath(__file__)) or os.getcwd()

    collected_dir = os.path.join(exec_dir, args.collected_dir)
    logs_dir = os.path.join(exec_dir, args.logs_dir)
    output_root = os.path.join(exec_dir, args.output_dir)
    output_dir = mode_output_dir(output_root, args.type)

    if not os.path.exists(collected_dir):
        print(f"ERROR: collected directory does not exist: {collected_dir}")
        sys.exit(1)
    if not os.path.exists(logs_dir):
        print(f"ERROR: worker log directory does not exist: {logs_dir}")
        sys.exit(1)

    print(f"\n{'#'*60}")
    print(f"Mode: {args.type}")
    print(f"Script directory: {exec_dir}")
    print(f"Client logs: {collected_dir}")
    print(f"Worker logs: {logs_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Trace limit per filter: {args.max_traces}")
    print(f"Parallel workers: {args.jobs}")
    print(f"{'#'*60}")

    # Dispatch processing by mode.
    if args.type == "core":
        # Core mode accepts one or more ':'-separated search strings.
        search_cores = [c.strip() for c in ':'.join(args.values).split(':') if c.strip()]
        if not search_cores:
            print("ERROR: core mode requires at least one search string")
            sys.exit(1)

        print(f"\nSearch strings: {len(search_cores)}")
        for i, c in enumerate(search_cores, 1):
            print(f"  {i}. '{c}'")

        for core in search_cores:
            process_core(core, collected_dir, logs_dir, output_dir, args.trace_file, args.max_traces, args.jobs)

    elif args.type == "all-core":
        if args.values:
            parser.error("all-core mode does not accept positional values")
        process_all_core(collected_dir, logs_dir, output_dir, args.trace_file, args.max_traces, args.jobs)

    elif args.type == "time":
        # Time mode requires an operation type and a latency range.
        # It supports "op1:op2 time_range" and "op time_range".
        if len(args.values) < 2:
            print("ERROR: time mode requires an operation type and latency range")
            print("  Example: python3 trace_collector.py --type time DS_KV_CLIENT_GET 1000,2000")
            sys.exit(1)

        time_range_str = args.values[-1]
        op_types_str = ' '.join(args.values[:-1])

        try:
            time_range = parse_time_range(time_range_str)
        except ValueError as e:
            print(f"ERROR: invalid latency range: {e}")
            sys.exit(1)

        op_types = [op.strip() for op in op_types_str.split(':') if op.strip()]
        if not op_types:
            print("ERROR: provide at least one operation type")
            sys.exit(1)

        print(f"\nOperation types: {len(op_types)}")
        for i, op in enumerate(op_types, 1):
            print(f"  {i}. '{op}'")
        print(f"Latency range: {time_range_str if time_range_str else 'unlimited'}")

        for op_type in op_types:
            process_time(op_type, time_range, collected_dir, logs_dir, output_dir,
                         args.trace_file, args.max_traces, args.jobs)

    elif args.type == "percentile":
        # Percentile mode requires an operation type and a percentile.
        if len(args.values) < 2:
            print("ERROR: percentile mode requires an operation type and percentile")
            print("  Example: python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99.9")
            sys.exit(1)

        percentile_str = args.values[-1]
        op_types_str = ' '.join(args.values[:-1])

        try:
            percentile = parse_percentile(percentile_str)
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)

        op_types = [op.strip() for op in op_types_str.split(':') if op.strip()]
        if not op_types:
            print("ERROR: provide at least one operation type")
            sys.exit(1)

        print(f"\nOperation types: {len(op_types)}")
        for i, op in enumerate(op_types, 1):
            print(f"  {i}. '{op}'")
        print(f"Percentile: {percentile_str} ({percentile})")

        for op_type in op_types:
            process_percentile(op_type, percentile, collected_dir, logs_dir, output_dir,
                               args.trace_file, args.max_traces, args.jobs)

    print(f"\n{'#'*60}")
    print("All processing complete")
    print(f"Output root directory: {os.path.abspath(output_dir)}/")
    print(f"{'#'*60}")


if __name__ == "__main__":
    main()
