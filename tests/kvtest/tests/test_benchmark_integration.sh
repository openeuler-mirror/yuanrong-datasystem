#!/bin/bash
# Benchmark integration test: T01-T11
# Runs on remote server against real workers
# Usage: bash tests/test_benchmark_integration.sh [SSH_HOST] [SSH_PORT] [REMOTE_DIR]
#
# Default: SSH_HOST=root@1.95.199.126 SSH_PORT=22223 REMOTE_DIR=/root/tmp/yr/bench_test
set -euo pipefail

SSH_HOST="${1:-root@1.95.199.126}"
SSH_PORT="${2:-22223}"
REMOTE_DIR="${3:-/root/tmp/yr/bench_test}"
SSH="ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p $SSH_PORT $SSH_HOST"

# ─── Test Matrix ─────────────────────────────────────────────
TOTAL_ROUNDS=3
DATA_SIZE="1MB"

# T01-T07: single worker
SINGLE_TESTS=(
    #  ID      test_mode        set_api        cleanup  threads mem_mb
    "T01|set_local|string_view|del|16|4096"
    "T02|set_local|create_buffer|del|16|4096"
    "T03|get_local|string_view|del|16|4096"
    "T04|get_local|create_buffer|del|16|4096"
    "T05|set_local|string_view|ttl|16|4096"
    "T06|set_local|string_view|del|1|4096"
    "T07|set_local|string_view|del|16|100"
)

# T08-T11: dual worker
DUAL_TESTS=(
    #  ID      test_mode             set_api        cleanup
    "T08|set_remote|string_view|del"
    "T09|get_cross_node|string_view|del"
    "T10|get_remote_direct|string_view|del"
    "T11|get_remote_cross|string_view|del"
)

LOCAL_RESULTS_DIR="$(pwd)/test_benchmark_results"
mkdir -p "$LOCAL_RESULTS_DIR"

# ─── Helper Functions ────────────────────────────────────────

calc_keys_per_round() {
    local mem_mb=$1
    # CalcKeysPerRound: mem_mb * 1024 * 1024 * 80% / (1 * 1024 * 1024) = mem_mb * 80 / 100
    local keys=$(( mem_mb * 80 / 100 ))
    echo $(( keys < 1 ? 1 : keys ))
}

gen_config() {
    local test_id=$1 test_mode=$2 set_api=$3 cleanup=$4 threads=$5 mem_mb=$6
    local conf_file=$7
    local ttl_line="\"ttl_second\": 0"

    if [ "$cleanup" = "ttl" ]; then
        ttl_line="\"ttl_second\": 5"
    fi

    cat > "$conf_file" <<EOFCFG
{
  "mode": "benchmark",
  "instance_id": 0,
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "connect_options": {
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 20,
    "enable_cross_node_connection": true,
    "enable_local_cache": true,
    "fast_transport_mem_size": "512MB"
  },
  "data_sizes": ["1MB"],
  "set_param": {$ttl_line},
  "num_threads": $threads,
  "metrics_interval_ms": 3000,
  "test_mode": "$test_mode",
  "worker_memory_mb": $mem_mb,
  "set_api": "$set_api",
  "cleanup_method": "$cleanup",
  "total_rounds": $TOTAL_ROUNDS,
  "duration_seconds": 0
}
EOFCFG
}

gen_config_dual() {
    local test_id=$1 test_mode=$2 set_api=$3 cleanup=$4
    local conf_file=$5

    cat > "$conf_file" <<EOFCFG
{
  "mode": "benchmark",
  "instance_id": 0,
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "connect_options": {
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 20,
    "enable_cross_node_connection": true,
    "enable_local_cache": true,
    "fast_transport_mem_size": "512MB"
  },
  "data_sizes": ["1MB"],
  "set_param": {"ttl_second": 0},
  "num_threads": 16,
  "metrics_interval_ms": 3000,
  "test_mode": "$test_mode",
  "worker_memory_mb": 4096,
  "set_api": "$set_api",
  "cleanup_method": "$cleanup",
  "total_rounds": $TOTAL_ROUNDS,
  "duration_seconds": 0,
  "remote_worker": {"host": "127.0.0.1", "port": 31502}
}
EOFCFG
}

run_test_case() {
    local test_id=$1 test_mode=$2 set_api=$3 cleanup=$4 threads=$5 mem_mb=$6
    local is_get=$([[ "$test_mode" == get_* ]] && echo 1 || echo 0)

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "$test_id: test_mode=$test_mode set_api=$set_api cleanup=$cleanup threads=$threads mem=$mem_mb"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Generate config locally and upload
    local tag="${test_id}_${test_mode}_${set_api:0:2}_${cleanup}"
    local local_dir="$LOCAL_RESULTS_DIR/$tag"
    mkdir -p "$local_dir"

    if [ $# -eq 6 ]; then
        gen_config "$test_id" "$test_mode" "$set_api" "$cleanup" "$threads" "$mem_mb" "$local_dir/config.json"
    else
        gen_config_dual "$test_id" "$test_mode" "$set_api" "$cleanup" "$local_dir/config.json"
    fi

    # Upload config
    scp -P $SSH_PORT -o StrictHostKeyChecking=no "$local_dir/config.json" "$SSH_HOST:$REMOTE_DIR/config_${test_id}.json" 2>/dev/null

    # Run kvtest on remote
    echo "  Running kvtest..."
    $SSH "cd $REMOTE_DIR && timeout 300 ./kvtest config_${test_id}.json" > "$local_dir/run.log" 2>&1
    local exit_code=$?

    # Collect metrics from remote
    echo "  Collecting metrics..."
    # Find the output directory (metrics_*) from run.log
    local output_dir
    output_dir=$(grep "Output directory:" "$local_dir/run.log" 2>/dev/null | head -1 | sed 's/.*: //' | tr -d '[:space:]')
    if [ -n "$output_dir" ]; then
        scp -P $SSH_PORT -o StrictHostKeyChecking=no -r "$SSH_HOST:$output_dir/*" "$local_dir/" 2>/dev/null || true
    fi
    # Also try to grab run.log from remote output dir
    scp -P $SSH_PORT -o StrictHostKeyChecking=no "$SSH_HOST:$REMOTE_DIR/run.log" "$local_dir/remote_run.log" 2>/dev/null || true

    # ─── Validate ───
    local pass=true

    # 1. Exit code
    if [ $exit_code -ne 0 ]; then
        echo "  FAIL: exit_code=$exit_code"
        pass=false
    fi

    # 2. Parse "Benchmark finished" line
    local finished_line
    finished_line=$(grep "Benchmark finished:" "$local_dir/run.log" 2>/dev/null | tail -1)
    if [ -z "$finished_line" ]; then
        echo "  FAIL: no 'Benchmark finished' line found"
        pass=false
    else
        local actual_rounds actual_set actual_get actual_del
        actual_rounds=$(echo "$finished_line" | grep -oP 'rounds=\K[0-9]+')
        actual_set=$(echo "$finished_line" | grep -oP 'set=\K[0-9]+')
        actual_get=$(echo "$finished_line" | grep -oP 'get=\K[0-9]+')
        actual_del=$(echo "$finished_line" | grep -oP 'del=\K[0-9]+')

        local keys_per_round
        if [ $# -eq 6 ]; then
            keys_per_round=$(calc_keys_per_round "$mem_mb")
        else
            keys_per_round=$(calc_keys_per_round 4096)
        fi
        local expected_ops=$(( keys_per_round * TOTAL_ROUNDS ))

        # Expected get
        local expected_get=0
        if [ "$is_get" = "1" ]; then
            expected_get=$expected_ops
        fi

        # Expected del
        local expected_del=0
        if [ "$cleanup" = "del" ]; then
            expected_del=$expected_ops
        fi

        echo "  Results: rounds=$actual_rounds set=$actual_set get=$actual_get del=$actual_del"
        echo "  Expected: set=$expected_ops get=$expected_get del=$expected_del (keys_per_round=$keys_per_round)"

        if [ "${actual_set:-0}" -ne "$expected_ops" ]; then
            echo "  FAIL: set count mismatch (expected=$expected_ops actual=${actual_set:-0})"
            pass=false
        fi
        if [ "${actual_get:-0}" -ne "$expected_get" ]; then
            echo "  FAIL: get count mismatch (expected=$expected_get actual=${actual_get:-0})"
            pass=false
        fi
        if [ "${actual_del:-0}" -ne "$expected_del" ]; then
            echo "  FAIL: del count mismatch (expected=$expected_del actual=${actual_del:-0})"
            pass=false
        fi
    fi

    # 3. Check benchmark_phases.csv exists and has data
    if [ -f "$local_dir/benchmark_phases.csv" ]; then
        local rows
        rows=$(tail -n +2 "$local_dir/benchmark_phases.csv" | wc -l)
        echo "  CSV: $rows phase records"
        if [ "$rows" -lt 1 ]; then
            echo "  FAIL: benchmark_phases.csv is empty"
            pass=false
        fi
    else
        echo "  WARN: benchmark_phases.csv not found"
    fi

    # 4. Latency sanity check from CSV
    if [ -f "$local_dir/benchmark_phases.csv" ]; then
        local set_avg
        set_avg=$(tail -n +2 "$local_dir/benchmark_phases.csv" | awk -F, '$2=="set"{sum+=$4; cnt++} END{if(cnt>0) printf "%.1f", sum/cnt}')
        if [ -n "$set_avg" ]; then
            echo "  Set avg latency: ${set_avg}ms"
            # Basic sanity: avg should be positive and < 1s
            if (( $(echo "$set_avg <= 0" | bc -l) )) || (( $(echo "$set_avg > 1000" | bc -l) )); then
                echo "  WARN: set avg latency out of range: ${set_avg}ms"
            fi
        fi
    fi

    if $pass; then
        echo "  ✓ PASS"
        echo "$test_id PASS" >> "$LOCAL_RESULTS_DIR/summary.txt"
    else
        echo "  ✗ FAIL"
        echo "$test_id FAIL" >> "$LOCAL_RESULTS_DIR/summary.txt"
    fi

    # Cleanup remote output dir
    $SSH "rm -rf $REMOTE_DIR/metrics_*" 2>/dev/null || true
}

# ─── Main ────────────────────────────────────────────────────

echo "╔══════════════════════════════════════════════════════════╗"
echo "║   Benchmark Integration Tests (T01-T11)                ║"
echo "║   Remote: $SSH_HOST:$SSH_PORT                          "
echo "╚══════════════════════════════════════════════════════════╝"

# Clear previous summary
> "$LOCAL_RESULTS_DIR/summary.txt"
echo "Benchmark Integration Test Results - $(date)" >> "$LOCAL_RESULTS_DIR/summary.txt"
echo "============================================" >> "$LOCAL_RESULTS_DIR/summary.txt"

# ─── Phase 1: Ensure remote environment ──────────────────────

echo ""
echo "[Phase 1] Checking remote environment..."
$SSH "mkdir -p $REMOTE_DIR"

# Check kvtest binary exists
if ! $SSH "test -x $REMOTE_DIR/kvtest"; then
    echo "ERROR: $REMOTE_DIR/kvtest not found on remote. Deploy first."
    echo "Run: scp -P $SSH_PORT output/kvtest $SSH_HOST:$REMOTE_DIR/"
    exit 1
fi

# Check etcd is running
echo "Checking etcd..."
$SSH "etcdctl --endpoints 127.0.0.1:2379 endpoint health" 2>/dev/null || {
    echo "Starting etcd..."
    $SSH "nohup etcd --data-dir /tmp/etcd-data-bench --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://0.0.0.0:2379 --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://0.0.0.0:2380 --initial-cluster default=http://0.0.0.0:2380 > /dev/null 2>&1 &"
    sleep 3
}

# Copy SDK libs if needed
$SSH "test -d $REMOTE_DIR/lib" || {
    echo "WARN: $REMOTE_DIR/lib not found. SDK libs may be missing."
}

# ─── Phase 2: Single Worker Tests (T01-T07) ─────────────────

echo ""
echo "[Phase 2] Single Worker Tests (T01-T07)"

echo "Starting worker on port 31501 (shm=4096MB)..."
$SSH "dscli stop --worker_address 127.0.0.1:31501" 2>/dev/null || true
sleep 1
$SSH "dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379 --shared_memory_size_mb 4096 -v 0" 2>/dev/null || {
    echo "WARN: dscli start failed, trying alternative..."
    $SSH "cd /root/tmp/yr/datasystem && python3 -c \"
from yr.datasystem import KVClient
print('SDK available')
\"" 2>/dev/null || echo "WARN: SDK not installed"
}
sleep 3

for entry in "${SINGLE_TESTS[@]}"; do
    IFS='|' read -r tid tmode sapi cleanup threads mem <<< "$entry"

    # T07 needs small memory worker - restart
    if [ "$tid" = "T07" ]; then
        echo "Restarting worker with 100MB for T07..."
        $SSH "dscli stop --worker_address 127.0.0.1:31501" 2>/dev/null || true
        sleep 2
        $SSH "dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379 --shared_memory_size_mb 100 -v 0" 2>/dev/null || true
        sleep 3
    fi

    run_test_case "$tid" "$tmode" "$sapi" "$cleanup" "$threads" "$mem"
done

# Restore worker for safety
echo "Stopping single worker..."
$SSH "dscli stop --worker_address 127.0.0.1:31501" 2>/dev/null || true
sleep 2

# ─── Phase 3: Dual Worker Tests (T08-T11) ────────────────────

echo ""
echo "[Phase 3] Dual Worker Tests (T08-T11)"

echo "Starting worker 1 on port 31501..."
$SSH "dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379 --shared_memory_size_mb 4096 -v 0" 2>/dev/null || true
sleep 2

echo "Starting worker 2 on port 31502..."
$SSH "dscli start -w --worker_address 127.0.0.1:31502 --etcd_address 127.0.0.1:2379 --shared_memory_size_mb 4096 -v 0" 2>/dev/null || true
sleep 3

for entry in "${DUAL_TESTS[@]}"; do
    IFS='|' read -r tid tmode sapi cleanup <<< "$entry"
    run_test_case "$tid" "$tmode" "$sapi" "$cleanup"
done

echo "Stopping dual workers..."
$SSH "dscli stop --worker_address 127.0.0.1:31501" 2>/dev/null || true
$SSH "dscli stop --worker_address 127.0.0.1:31502" 2>/dev/null || true

# ─── Phase 4: Summary ────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  Summary"
echo "══════════════════════════════════════════════════════════"

echo "" >> "$LOCAL_RESULTS_DIR/summary.txt"
echo "============================================" >> "$LOCAL_RESULTS_DIR/summary.txt"

pass_count=$(grep -c "PASS" "$LOCAL_RESULTS_DIR/summary.txt" || true)
fail_count=$(grep -c "FAIL" "$LOCAL_RESULTS_DIR/summary.txt" || true)
echo "  Passed: $pass_count / $((pass_count + fail_count))"
echo "  Failed: $fail_count"
echo ""
echo "Results saved to: $LOCAL_RESULTS_DIR/"
echo "  summary.txt"
ls "$LOCAL_RESULTS_DIR/" | grep "^T[0-9]" | while read d; do
    echo "  $d/"
done

cat "$LOCAL_RESULTS_DIR/summary.txt"

exit $fail_count
