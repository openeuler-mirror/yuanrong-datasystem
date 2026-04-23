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
