#!/usr/bin/env bash
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

# Collect trace reports from one task directory or from a list of task directories.
# Each task directory must contain collected/ and collected_worker_logs/.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
TRACE_COLLECTOR="${SCRIPT_DIR}/trace_collector.py"
OUTPUT_ROOT="$(pwd -P)/collect"

MODE=""
SINGLE_DIR=""
LIST_FILE=""

usage() {
    cat <<EOF
Usage: $(basename "$0") -t <core|time|coreandtime> [-f <task-dir> | -l <list-file>]

Collect trace reports from task directories. Each task directory must contain
collected/ and collected_worker_logs/. The collector is copied into every task
directory before execution. Results are written under ./collect/.

Options:
  -t MODE       Collection mode: core, time, or coreandtime (required).
                coreandtime runs both collections once per task directory.
  -f TASK_DIR   Collect one task directory. Relative paths such as ./case are supported.
  -l LIST_FILE  Collect each task directory listed in this text file. Blank lines and
                lines beginning with # are ignored.
  -h, --help    Show this help message.

Examples:
  # Collect all non-zero status-code traces from one task directory.
  $(basename "$0") -t core -f ./mode1_case6

  # Collect the predefined DS_KV_CLIENT_GET latency buckets from one directory.
  $(basename "$0") -t time -f mode1_case6

  # Collect both all-core traces and the predefined time buckets in one run.
  $(basename "$0") -t coreandtime -f ./mode1_case6

  # Process one task directory per line in dirs.txt.
  $(basename "$0") -t core -l dirs.txt
  $(basename "$0") -t time -l dirs.txt
EOF
}

require_option_value() {
    if [ "$#" -lt 2 ] || [ -z "$2" ]; then
        echo "Error: $1 requires a value." >&2
        usage >&2
        exit 2
    fi
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        -t)
            require_option_value "$@"
            MODE="$2"
            shift 2
            ;;
        -f)
            require_option_value "$@"
            SINGLE_DIR="$2"
            shift 2
            ;;
        -l)
            require_option_value "$@"
            LIST_FILE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Error: unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [ -z "$MODE" ]; then
    echo "Error: -t is required." >&2
    usage >&2
    exit 2
fi

if [ "$MODE" != "core" ] && [ "$MODE" != "time" ] && [ "$MODE" != "coreandtime" ]; then
    echo "Error: -t must be core, time, or coreandtime." >&2
    exit 2
fi

if [ -z "$SINGLE_DIR" ] && [ -z "$LIST_FILE" ]; then
    echo "Error: specify exactly one of -f or -l." >&2
    usage >&2
    exit 2
fi

if [ -n "$SINGLE_DIR" ] && [ -n "$LIST_FILE" ]; then
    echo "Error: -f and -l cannot be used together." >&2
    exit 2
fi

if [ -n "$LIST_FILE" ] && [ ! -r "$LIST_FILE" ]; then
    echo "Error: list file is not readable: $LIST_FILE" >&2
    exit 2
fi

if [ ! -f "$TRACE_COLLECTOR" ]; then
    echo "Error: trace_collector.py was not found beside this script: $TRACE_COLLECTOR" >&2
    exit 1
fi

mkdir -p "$OUTPUT_ROOT"
echo "Mode: $MODE"
echo "Output directory: $OUTPUT_ROOT"

process_dir() {
    local dir="$1"
    local dir_abs
    local dir_name
    local output_mode
    local collect_subdir
    local destination
    local src_dir

    echo
    echo "========== Processing: $dir =========="

    if ! dir_abs="$(cd -- "$dir" 2>/dev/null && pwd -P)"; then
        echo "  [SKIP] Directory not found: $dir"
        return 1
    fi

    if [ ! -d "$dir_abs/collected" ]; then
        echo "  [SKIP] collected/ not found: $dir_abs"
        return 1
    fi
    if [ ! -d "$dir_abs/collected_worker_logs" ]; then
        echo "  [SKIP] collected_worker_logs/ not found: $dir_abs"
        return 1
    fi

    if [ "$TRACE_COLLECTOR" -ef "$dir_abs/trace_collector.py" ]; then
        echo "  [OK] Using trace_collector.py already in the task directory"
    else
        cp -- "$TRACE_COLLECTOR" "$dir_abs/trace_collector.py"
        echo "  [OK] Copied trace_collector.py"
    fi

    collect_mode() {
        output_mode="$1"
        collect_subdir="$2"
        destination="$OUTPUT_ROOT/${output_mode}_${dir_name}"

        if [ -e "$destination" ]; then
            echo "  [SKIP] Destination already exists: $destination"
            return 1
        fi

        if [ "$output_mode" = "all-core" ]; then
        echo "  [RUN] python3 ./trace_collector.py --type all_core --max-traces 1500 --jobs 50"
        if ! (cd -- "$dir_abs" && python3 ./trace_collector.py --type all_core --max-traces 1500 --jobs 50); then
            echo "  [FAIL] trace_collector.py failed in $dir_abs"
            return 1
        fi
        else
        echo "  [RUN] Collecting DS_KV_CLIENT_GET latency buckets"
        if ! (cd -- "$dir_abs" && python3 ./trace_collector.py --type time DS_KV_CLIENT_GET 5000,7000 --max-traces 1500 --jobs 50); then
            echo "  [FAIL] DS_KV_CLIENT_GET 5000,7000 failed"
            return 1
        fi
        if ! (cd -- "$dir_abs" && python3 ./trace_collector.py --type time DS_KV_CLIENT_GET 7000,10000 --max-traces 1500 --jobs 50); then
            echo "  [FAIL] DS_KV_CLIENT_GET 7000,10000 failed"
            return 1
        fi
        if ! (cd -- "$dir_abs" && python3 ./trace_collector.py --type time DS_KV_CLIENT_GET 10000,20000 --max-traces 1500 --jobs 50); then
            echo "  [FAIL] DS_KV_CLIENT_GET 10000,20000 failed"
            return 1
        fi
        if ! (cd -- "$dir_abs" && python3 ./trace_collector.py --type time DS_KV_CLIENT_GET 20000 --max-traces 1500 --jobs 50); then
            echo "  [FAIL] DS_KV_CLIENT_GET 20000 failed"
            return 1
        fi
        fi

        if [ -d "$dir_abs/trace_collect/$collect_subdir" ]; then
            src_dir="$dir_abs/trace_collect/$collect_subdir"
        elif [ "$output_mode" = "all-core" ] && [ -d "$dir_abs/trace_collect" ]; then
            src_dir="$dir_abs/trace_collect"
        else
            echo "  [FAIL] trace_collect/$collect_subdir was not generated in $dir_abs"
            return 1
        fi

        mv -- "$src_dir" "$destination"
        echo "  [OK] Moved results to $destination"
    }

    dir_name="$(basename -- "$dir_abs")"
    if [ "$MODE" = "core" ] || [ "$MODE" = "coreandtime" ]; then
        collect_mode "all-core" "all-core" || return 1
    fi
    if [ "$MODE" = "time" ] || [ "$MODE" = "coreandtime" ]; then
        collect_mode "time" "time" || return 1
    fi
}

if [ -n "$SINGLE_DIR" ]; then
    process_dir "$SINGLE_DIR"
else
    while IFS= read -r dir || [ -n "$dir" ]; do
        dir="${dir#"${dir%%[![:space:]]*}"}"
        dir="${dir%"${dir##*[![:space:]]}"}"
        [ -z "$dir" ] || [[ "$dir" == \#* ]] && continue
        process_dir "$dir" || true
    done < "$LIST_FILE"
fi

echo
echo "========== All done =========="
echo "Results are in: $OUTPUT_ROOT"
find "$OUTPUT_ROOT" -mindepth 1 -maxdepth 1 -type d \( -name 'all-core_*' -o -name 'time_*' \) -print
