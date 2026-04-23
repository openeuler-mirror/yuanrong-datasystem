#!/usr/bin/env bash
# E2E test for kvclient_standalone_test deploy/stop/collect
# Validates TC1-TC7 as defined in docs/superpowers/specs/2026-04-24-e2e-deploy-test-design.md
set -euo pipefail

SSH_HOST=1.95.199.126
SSH_USER=root
SSH_OPTS="-o StrictHostKeyChecking=no"
REMOTE_DIR=/tmp/kvclient_test
DEPLOY_CFG=config/deploy.test.json
KVCLIENT_CFG=config/config.test.json
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PASS=0
FAIL=0

log_pass() { echo "  PASS: $1"; ((PASS++)); }
log_fail() { echo "  FAIL: $1"; ((FAIL++)); }

run_on() {
    # Usage: run_on A|B "command"
    local m=$1; shift
    case $m in
        A) ssh $SSH_OPTS -p 22223 "${SSH_USER}@${SSH_HOST}" "$@" ;;
        B) ssh $SSH_OPTS -p 22224 "${SSH_USER}@${SSH_HOST}" "$@" ;;
    esac
}

check_process() {
    # Usage: check_process A|B "pattern" — returns 0 if alive
    run_on "$1" "pgrep -f '$2'" >/dev/null 2>&1
}

# === Setup ===
echo "=== Setup ==="

# 1. Discover IPs
echo "Discovering machine IPs..."
IP_A=$(run_on A "ip -4 addr show | grep -oP '(?<=inet )\\S+' | grep -v 127.0.0.1 | head -1" | tr -d '\r\n')
IP_B=$(run_on B "ip -4 addr show | grep -oP '(?<=inet )\\S+' | grep -v 127.0.0.1 | head -1" | tr -d '\r\n')
echo "  Machine A: $IP_A"
echo "  Machine B: $IP_B"

# 2. Fill config placeholders
cd "$SCRIPT_DIR"
cp "$DEPLOY_CFG" "${DEPLOY_CFG}.bak"
cp "$KVCLIENT_CFG" "${KVCLIENT_CFG}.bak"
sed -i "s/__MACHINE_A_IP__/$IP_A/g" "$DEPLOY_CFG" "$KVCLIENT_CFG"
sed -i "s/__MACHINE_B_IP__/$IP_B/g" "$DEPLOY_CFG" "$KVCLIENT_CFG"
echo "  Configs updated with actual IPs"

# Restore configs on exit
restore_configs() {
    mv "${DEPLOY_CFG}.bak" "$DEPLOY_CFG" 2>/dev/null || true
    mv "${KVCLIENT_CFG}.bak" "$KVCLIENT_CFG" 2>/dev/null || true
}
trap restore_configs EXIT

# 3. Install and start etcd on Machine A
echo "Checking etcd on Machine A..."
if ! run_on A "pgrep -x etcd" >/dev/null 2>&1; then
    run_on A "apt-get update -qq && apt-get install -y -qq etcd-server etcd-client 2>/dev/null || true"
    run_on A "nohup etcd --listen-client-urls http://0.0.0.0:2379 \
        --advertise-client-urls http://${IP_A}:2379 \
        > /tmp/etcd.log 2>&1 &"
    sleep 3
fi
if run_on A "ETCDCTL_API=3 etcdctl --endpoints=http://localhost:2379 endpoint health" >/dev/null 2>&1; then
    echo "  etcd: OK"
else
    log_fail "etcd not running on Machine A"
    exit 1
fi

# 4. Verify datasystem workers
for m in A B; do
    if check_process "$m" "datasystem_worker"; then
        echo "  Machine $m worker: OK"
    else
        echo "  WARNING: Machine $m worker not running"
    fi
done

# 5. Verify binary exists locally
if [[ ! -f kvclient_standalone_test ]]; then
    echo "ERROR: kvclient_standalone_test binary not found. Run build.sh first."
    exit 1
fi

# 6. Clean up any previous instances
./deploy.py --clean "$DEPLOY_CFG" "$KVCLIENT_CFG" 2>/dev/null || true

# === TC1: Deploy ===
echo ""
echo "=== TC1: Deploy ==="
./deploy.py --deploy "$DEPLOY_CFG" "$KVCLIENT_CFG"

# Verify kvclient process on both machines
for m in A B; do
    if check_process "$m" "kvclient_standalone_test"; then
        log_pass "Machine $m: kvclient process running"
    else
        log_fail "Machine $m: kvclient process NOT running"
    fi
done

# Verify procmon on both machines
for m in A B; do
    if check_process "$m" "procmon.py"; then
        log_pass "Machine $m: procmon running"
    else
        log_fail "Machine $m: procmon NOT running"
    fi
done

# Verify config files exist on remote
for m in A B; do
    local_iid=0
    [[ "$m" == "B" ]] && local_iid=1
    if run_on "$m" "test -f ${REMOTE_DIR}/config_${local_iid}.json" 2>/dev/null; then
        log_pass "Machine $m: config_${local_iid}.json exists"
    else
        log_fail "Machine $m: config_${local_iid}.json missing"
    fi
done

# === TC2: Metrics Data Accuracy (30s run) ===
echo ""
echo "=== TC2: Metrics Data Accuracy ==="
echo "Waiting 35s for metrics to accumulate..."
sleep 35

# Check Machine A (writer) metrics
METRICS_A=$(run_on A "cat ${REMOTE_DIR}/metrics_0.csv 2>/dev/null" || echo "")
if [[ -n "$METRICS_A" ]]; then
    SET_ROWS=$(echo "$METRICS_A" | tail -n +2 | grep "setStringView" | awk -F, '$3 > 0' | wc -l)
    if (( SET_ROWS > 0 )); then
        log_pass "Machine A: $SET_ROWS setStringView rows with count > 0"
    else
        log_fail "Machine A: no setStringView rows with count > 0"
    fi
    ZERO_LAT=$(echo "$METRICS_A" | tail -n +2 | grep "setStringView" | awk -F, '$4 == "0.000"' | wc -l)
    if (( ZERO_LAT == 0 )); then
        log_pass "Machine A: no zero latencies in setStringView"
    else
        log_fail "Machine A: $ZERO_LAT zero-latency rows"
    fi
else
    log_fail "Machine A: metrics_0.csv not found or empty"
fi

# Check Machine B (reader) metrics
METRICS_B=$(run_on B "cat ${REMOTE_DIR}/metrics_1.csv 2>/dev/null" || echo "")
if [[ -n "$METRICS_B" ]]; then
    GET_ROWS=$(echo "$METRICS_B" | tail -n +2 | grep "getBuffer" | awk -F, '$3 > 0' | wc -l)
    if (( GET_ROWS > 0 )); then
        log_pass "Machine B: $GET_ROWS getBuffer rows with count > 0"
    else
        log_fail "Machine B: no getBuffer rows with count > 0"
    fi
else
    log_fail "Machine B: metrics_1.csv not found or empty"
fi

# Check for verify_fail
VF_A=$(run_on A "grep -c 'verify_fail' ${REMOTE_DIR}/stdout_0.log 2>/dev/null" || echo "0")
VF_B=$(run_on B "grep -c 'verify_fail' ${REMOTE_DIR}/stdout_1.log 2>/dev/null" || echo "0")
VF_A=$(echo "$VF_A" | tr -d '\r\n')
VF_B=$(echo "$VF_B" | tr -d '\r\n')
if (( VF_A == 0 && VF_B == 0 )); then
    log_pass "No verify_fail in output"
else
    log_fail "verify_fail found: Machine A=$VF_A, Machine B=$VF_B"
fi

# === TC3: Stop ===
echo ""
echo "=== TC3: Stop ==="
./deploy.py --stop "$DEPLOY_CFG" "$KVCLIENT_CFG"
sleep 2

# Verify processes stopped
for m in A B; do
    if ! check_process "$m" "kvclient_standalone_test"; then
        log_pass "Machine $m: kvclient stopped"
    else
        log_fail "Machine $m: kvclient still running"
    fi
    if ! check_process "$m" "procmon.py"; then
        log_pass "Machine $m: procmon stopped"
    else
        log_fail "Machine $m: procmon still running"
    fi
done

# === TC4: Collect ===
echo ""
echo "=== TC4: Collect ==="
rm -rf collected
./deploy.py --collect "$DEPLOY_CFG" "$KVCLIENT_CFG"

COLLECTED_DIRS=$(ls -d collected/*/ 2>/dev/null | wc -l)
if (( COLLECTED_DIRS == 2 )); then
    log_pass "2 subdirectories in collected/"
else
    log_fail "Expected 2 subdirectories, found $COLLECTED_DIRS"
fi

for dir in collected/*/; do
    dirname=$(basename "$dir")

    has_metrics=$(ls "$dir"metrics_*.csv 2>/dev/null | wc -l)
    has_summary=$(ls "$dir"summary_*.txt 2>/dev/null | wc -l)
    has_procmon=$(ls "$dir"procmon_*.log 2>/dev/null | wc -l)
    has_stdout=$(ls "$dir"stdout_*.log 2>/dev/null | wc -l)

    if (( has_metrics > 0 && has_summary > 0 && has_procmon > 0 && has_stdout > 0 )); then
        log_pass "$dirname: all required files present"
    else
        (( has_metrics == 0 )) && log_fail "$dirname: missing metrics CSV"
        (( has_summary == 0 )) && log_fail "$dirname: missing summary TXT"
        (( has_procmon == 0 )) && log_fail "$dirname: missing procmon log"
        (( has_stdout == 0 )) && log_fail "$dirname: missing stdout log"
    fi

    # Check CSV non-empty
    for csv in "$dir"metrics_*.csv; do
        rows=$(tail -n +2 "$csv" 2>/dev/null | wc -l)
        if (( rows > 0 )); then
            log_pass "$(basename "$csv"): $rows data rows"
        else
            log_fail "$(basename "$csv"): empty"
        fi
    done

    # Check summary Total > 0
    for sumf in "$dir"summary_*.txt; do
        if grep -q "Total.*[1-9]" "$sumf" 2>/dev/null; then
            log_pass "$(basename "$sumf"): Total > 0"
        else
            log_fail "$(basename "$sumf"): Total is 0"
        fi
    done
done

# === TC5: Stability (2-round deploy-stop cycle) ===
echo ""
echo "=== TC5: Stability (2-round cycle) ==="
for round in 1 2; do
    echo "--- Round $round ---"

    # Deploy
    ./deploy.py --deploy "$DEPLOY_CFG" "$KVCLIENT_CFG"
    for m in A B; do
        if check_process "$m" "kvclient_standalone_test"; then
            log_pass "Round $round deploy: Machine $m kvclient alive"
        else
            log_fail "Round $round deploy: Machine $m kvclient NOT alive"
        fi
    done

    # Run 30s with health check
    echo "  Running 30s with health check..."
    sleep 30

    # Coredump check during run
    for m in A B; do
        CORES=$(run_on "$m" "dmesg | grep -ic 'coredump\|segfault'" 2>/dev/null || echo "0")
        CORES=$(echo "$CORES" | tr -d '\r\n')
        if (( CORES > 0 )); then
            log_fail "Round $round Machine $m: $CORES coredump/segfault events in dmesg"
        fi

        # Process liveness during run
        if check_process "$m" "kvclient_standalone_test" && check_process "$m" "datasystem_worker"; then
            log_pass "Round $round Machine $m: all processes alive during run"
        else
            if ! check_process "$m" "kvclient_standalone_test"; then
                log_fail "Round $round Machine $m: kvclient died during run"
            fi
            if ! check_process "$m" "datasystem_worker"; then
                log_fail "Round $round Machine $m: datasystem_worker died during run"
            fi
        fi
    done

    # Stop
    ./deploy.py --stop "$DEPLOY_CFG" "$KVCLIENT_CFG"
    sleep 2
    for m in A B; do
        if ! check_process "$m" "kvclient_standalone_test"; then
            log_pass "Round $round stop: Machine $m kvclient dead"
        else
            log_fail "Round $round stop: Machine $m kvclient still alive"
        fi
    done
done

# Final collect after stability test
rm -rf collected
./deploy.py --collect "$DEPLOY_CFG" "$KVCLIENT_CFG"
if (( $(ls -d collected/*/ 2>/dev/null | wc -l) == 2 )); then
    log_pass "Final collect: 2 directories"
else
    log_fail "Final collect: wrong number of directories"
fi

# Check second-round metrics present
for dir in collected/*/; do
    csv_files=$(ls "$dir"metrics_*.csv 2>/dev/null | wc -l)
    if (( csv_files > 0 )); then
        rows=$(tail -n +2 "$dir"metrics_*.csv 2>/dev/null | wc -l)
        if (( rows > 0 )); then
            log_pass "$(basename "$dir"): second-round metrics present ($rows rows)"
        else
            log_fail "$(basename "$dir"): second-round metrics empty"
        fi
    else
        log_fail "$(basename "$dir"): no metrics CSV in second round"
    fi
done

# === TC6: Stop on Empty Instances ===
echo ""
echo "=== TC6: Stop on Empty ==="
START_TIME=$(date +%s)
./deploy.py --stop "$DEPLOY_CFG" "$KVCLIENT_CFG" 2>&1 || true
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
if (( ELAPSED < 15 )); then
    log_pass "Stop on empty completed in ${ELAPSED}s (< 15s)"
else
    log_fail "Stop on empty took ${ELAPSED}s (expected < 15s)"
fi

# === TC7: Runtime Health Report ===
echo ""
echo "=== TC7: Runtime Health Report ==="

echo ""
echo "=== Health Report ==="
for m in A B; do
    local_iid=0
    [[ "$m" == "B" ]] && local_iid=1
    label="Machine $m"
    [[ "$m" == "A" ]] && label="Machine $m (writer)"
    [[ "$m" == "B" ]] && label="Machine $m (reader)"
    echo "$label:"

    # Process status (should be dead after TC6 stop)
    if check_process "$m" "kvclient_standalone_test"; then
        echo "  kvclient process: ALIVE (unexpected after stop)"
    else
        echo "  kvclient process: STOPPED"
    fi

    if check_process "$m" "datasystem_worker"; then
        echo "  worker process: ALIVE"
    else
        echo "  worker process: NOT RUNNING"
    fi

    # Coredumps
    CORES=$(run_on "$m" "dmesg | grep -ic 'coredump\|segfault'" 2>/dev/null || echo "0")
    CORES=$(echo "$CORES" | tr -d '\r\n')
    if (( CORES > 0 )); then
        echo "  coredumps: $CORES events detected"
        log_fail "$label: $CORES coredump/segfault events"
    else
        echo "  coredumps: NONE"
    fi

    # Core files on disk
    CORE_FILES=$(run_on "$m" "ls ${REMOTE_DIR}/core.* 2>/dev/null; ls /var/lib/apport/coredump/ 2>/dev/null" || echo "")
    if [[ -n "$CORE_FILES" ]]; then
        echo "  core files on disk: FOUND"
        log_fail "$label: core files found on disk"
    else
        echo "  core files on disk: NONE"
    fi

    # Error scan in stdout log
    ERRORS=$(run_on "$m" "grep -ic 'error\|fail\|exception\|abort' ${REMOTE_DIR}/stdout_${local_iid}.log 2>/dev/null" || echo "0")
    ERRORS=$(echo "$ERRORS" | tr -d '\r\n')
    # Filter known benign patterns
    BENIGN=$(run_on "$m" "grep -ic 'connection refused\|ZMQ.*retry\|Timeout waiting for notify' ${REMOTE_DIR}/stdout_${local_iid}.log 2>/dev/null" || echo "0")
    BENIGN=$(echo "$BENIGN" | tr -d '\r\n')
    UNEXPECTED=$((ERRORS - BENIGN))
    if (( UNEXPECTED > 0 )); then
        echo "  errors in stdout_${local_iid}.log: $UNEXPECTED unexpected ($ERRORS total, $BENIGN benign)"
        log_fail "$label: $UNEXPECTED unexpected errors in stdout"
    else
        echo "  errors in stdout_${local_iid}.log: 0 unexpected ($BENIGN benign)"
    fi

    # Summary check (if collected)
    for sumf in collected/*_${local_iid}/summary_*.txt; do
        if [[ -f "$sumf" ]]; then
            SET_LINE=$(grep "setStringView" "$sumf" 2>/dev/null || echo "")
            GET_LINE=$(grep "getBuffer" "$sumf" 2>/dev/null || echo "")
            VF_LINE=$(grep "verify_fail" "$sumf" 2>/dev/null || echo "")

            if [[ -n "$SET_LINE" ]]; then
                SUCCESS=$(echo "$SET_LINE" | grep -oP 'success.*?(\d+)' | grep -oP '\d+' | head -1)
                FAIL_CNT=$(echo "$SET_LINE" | grep -oP 'fail.*?(\d+)' | grep -oP '\d+' | head -1)
                echo "  summary: setStringView success=$SUCCESS fail=${FAIL_CNT:-0}"
            fi
            if [[ -n "$GET_LINE" ]]; then
                SUCCESS=$(echo "$GET_LINE" | grep -oP 'success.*?(\d+)' | grep -oP '\d+' | head -1)
                FAIL_CNT=$(echo "$GET_LINE" | grep -oP 'fail.*?(\d+)' | grep -oP '\d+' | head -1)
                echo "  summary: getBuffer success=$SUCCESS fail=${FAIL_CNT:-0}"
            fi
            if [[ -n "$VF_LINE" ]]; then
                VF_COUNT=$(echo "$VF_LINE" | grep -oP '\d+' | head -1)
                log_fail "$label: verify_fail=$VF_COUNT"
            else
                echo "  summary: verify_fail=0"
            fi
        fi
    done
    echo ""
done

# === Results ===
echo "=== Results: $PASS passed, $FAIL failed ==="
if (( FAIL > 0 )); then
    exit 1
fi
exit 0
