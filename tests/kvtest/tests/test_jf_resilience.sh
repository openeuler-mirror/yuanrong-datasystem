#!/usr/bin/env bash
# E2E test for JF service-discovery resilience (worker/coordinator standalone
# deployment with the JF mock):
#   TC1: coordinator process stall (SIGSTOP) > TTL -> registry expiry
#        (heartbeat 404) -> JfClient re-registers itself, no restart needed.
#   TC2: worker started while discovery is empty -> InitAndRun retries with
#        backoff -> coordinator returns -> worker finally starts.
#   TC3: non-discovery failures (bad config) still fail fast, no retry.
set -uo pipefail
set +e

# Two layouts: source tree (<repo>/tests/kvtest/tests/<script>, binaries in
# ../build, mock in ../src, SDK at ../../../output/cpp) and package output
# (script, binaries, mock_jf_server.py and lib/ are siblings).
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -x "$SELF_DIR/../build/worker_test" ] && [ -f "$SELF_DIR/../src/mock_jf_server.py" ]; then
    SCRIPT_DIR="$(cd "$SELF_DIR/.." && pwd)"
    BUILD_DIR="$SCRIPT_DIR/build"
    MOCK="$SCRIPT_DIR/src/mock_jf_server.py"
    SDK_DIR="${SDK_DIR:-$SCRIPT_DIR/../../output/cpp}"
else
    SCRIPT_DIR="$SELF_DIR"
    BUILD_DIR="$SELF_DIR"
    MOCK="$SELF_DIR/mock_jf_server.py"
    SDK_DIR="${SDK_DIR:-$SELF_DIR}"
fi
ROOT_DIR="/tmp/jf_res_test_$$"
SERVICE_NAME="kvcache_coordinator"
TTL_SEC=12
PASS=0
FAIL=0

log_pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
log_fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

stop_process() {
    local pid=$1
    kill -TERM "$pid" 2>/dev/null || true
    for i in $(seq 1 10); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 1
    done
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}

get_free_port() {
    python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()"
}

wait_health() {
    local path=$1 timeout="${2:-30}"
    for i in $(seq 1 "$timeout"); do
        [[ -f "$path" ]] && return 0
        sleep 1
    done
    return 1
}

wait_tcp() {
    local host=$1 port=$2 timeout="${3:-10}"
    for i in $(seq 1 "$timeout"); do
        python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('$host',$port)); s.close()" 2>/dev/null && return 0
        sleep 1
    done
    return 1
}

wait_discover() {
    local jf=$1 service=$2 want_empty=$3 timeout="${4:-15}"
    for i in $(seq 1 "$timeout"); do
        local count
        count=$(curl -s "http://$jf/discover/$service" | python3 -c "import json,sys;print(len(json.load(sys.stdin)['instances']))" 2>/dev/null)
        if [[ "$want_empty" == "true" && "$count" == "0" ]]; then return 0; fi
        if [[ "$want_empty" == "false" && "$count" != "0" && -n "$count" ]]; then return 0; fi
        sleep 1
    done
    return 1
}

# Print the timestamps (epoch float) of the first `action` event for `addr`,
# or empty if absent.
event_ts() {
    local jf=$1 addr=$2 action=$3
    curl -s "http://$jf/events" | python3 -c "
import json, sys
events = json.load(sys.stdin)
for e in events:
    if e['action'] == '$action' and e['address'] == '$addr':
        print(e['ts']); break
"
}

latest_event_ts() {
    local jf=$1 addr=$2 action=$3
    curl -s "http://$jf/events" | python3 -c "
import json, sys
events = json.load(sys.stdin)
ts = [e['ts'] for e in events if e['action'] == '$action' and e['address'] == '$addr']
print(ts[-1] if ts else '')
"
}

if [[ ! -f "$BUILD_DIR/coordinator_test" ]] || [[ ! -f "$BUILD_DIR/worker_test" ]]; then
    echo "ERROR: test binaries not found. Run build.sh first."
    exit 1
fi

JF_PORT=$(get_free_port)
COORD_PORT=$(get_free_port)
WORKER_PORT=$(get_free_port)
JF_ADDR="127.0.0.1:$JF_PORT"
COORD_ADDR="127.0.0.1:$COORD_PORT"

mkdir -p "$ROOT_DIR/coord/log" "$ROOT_DIR/coord/raft"
mkdir -p "$ROOT_DIR/worker/log" "$ROOT_DIR/worker/rocksdb" "$ROOT_DIR/worker/uds"

cat > "$ROOT_DIR/coordinator_config.json" << EOF
{
    "coordinator_address": {"value": "$COORD_ADDR"},
    "coordinator_raft_data_dir": {"value": "$ROOT_DIR/coord/raft"},
    "coordinator_raft_heartbeat_interval_ms": {"value": "500"},
    "coordinator_raft_election_timeout_ms": {"value": "3000"},
    "log_dir": {"value": "$ROOT_DIR/coord/log"},
    "log_async": {"value": "false"},
    "node_dead_timeout_s": {"value": "6"}
}
EOF

cat > "$ROOT_DIR/worker_config.json" << EOF
{
    "worker_address": {"value": "127.0.0.1:$WORKER_PORT"},
    "shared_memory_size_mb": {"value": "64"},
    "log_dir": {"value": "$ROOT_DIR/worker/log"},
    "rocksdb_store_dir": {"value": "$ROOT_DIR/worker/rocksdb"},
    "rocksdb_write_mode": {"value": "none"},
    "health_check_path": {"value": "$ROOT_DIR/worker/health"},
    "node_timeout_s": {"value": "3"},
    "node_dead_timeout_s": {"value": "6"},
    "add_node_wait_time_s": {"value": "1"},
    "log_async": {"value": "false"},
    "enable_distributed_master": {"value": "true"}
}
EOF

export LD_LIBRARY_PATH="${SDK_DIR}/lib:${LD_LIBRARY_PATH:-}"

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 3
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    rm -rf "$ROOT_DIR"
}
trap cleanup EXIT
PIDS=()

echo "=== Setup: start JF mock (ttl=$TTL_SEC) ==="
python3 "$MOCK" --port "$JF_PORT" --ttl-default "$TTL_SEC" &
JF_PID=$!
PIDS+=("$JF_PID")
sleep 1
if curl -s "http://$JF_ADDR/health" | grep -q "ok"; then
    log_pass "JF mock ready on $JF_ADDR"
else
    log_fail "JF mock not ready"
    exit 1
fi

echo ""
echo "=== TC1: coordinator stall (SIGSTOP) > TTL -> self re-register ==="
"$BUILD_DIR/coordinator_test" \
    --config "$ROOT_DIR/coordinator_config.json" \
    --coordinator "$COORD_ADDR" \
    --jf "$JF_ADDR" --service "$SERVICE_NAME" \
    --hooks --ttl "$TTL_SEC" > "$ROOT_DIR/coord/stdout.log" 2>&1 &
COORD_PID=$!
PIDS+=("$COORD_PID")

wait_tcp "127.0.0.1" "$COORD_PORT" 15 || log_fail "coordinator port not connectable"
if wait_discover "$JF_ADDR" "$SERVICE_NAME" false 10; then
    log_pass "coordinator registered in JF"
else
    log_fail "coordinator not registered in JF"
    exit 1
fi

# Replay the incident: process-level multi-second stall freezes the heartbeat
# thread past the TTL, the sweeper deletes the instance, and every later
# /heartbeat would 404 forever without a re-register.
kill -STOP "$COORD_PID"
sleep $((TTL_SEC + 4))
if wait_discover "$JF_ADDR" "$SERVICE_NAME" true 5; then
    log_pass "instance expired after stall (discover empty)"
else
    log_fail "instance did not expire after stall"
fi
EXPIRE_TS=$(event_ts "$JF_ADDR" "$COORD_ADDR" "expire")
kill -CONT "$COORD_PID"

if wait_discover "$JF_ADDR" "$SERVICE_NAME" false 15; then
    log_pass "coordinator re-registered itself after resume"
else
    log_fail "coordinator did not re-register after resume"
fi
REG_TS=$(latest_event_ts "$JF_ADDR" "$COORD_ADDR" "register")
if [[ -n "$EXPIRE_TS" && -n "$REG_TS" ]]; then
    GAP=$(python3 -c "print(round($REG_TS - $EXPIRE_TS, 1))")
    # Heartbeat interval is TTL/6 = 2s; recovery must not take more than a
    # few rounds.
    GAP_PASS=$(python3 -c "print(1 if $REG_TS > $EXPIRE_TS and ($REG_TS - $EXPIRE_TS) <= 10 else 0)")
    if [[ "$GAP_PASS" == "1" ]]; then
        log_pass "re-register ${GAP}s after expiry (<=10s)"
    else
        log_fail "re-register gap ${GAP}s invalid (expire=$EXPIRE_TS register=$REG_TS)"
    fi
else
    log_fail "missing expire/register events (expire=$EXPIRE_TS register=$REG_TS)"
fi
# The first post-re-register heartbeat lands within one interval (TTL/6 =
# 2s); poll instead of checking instantly.
HB_OK=0
for i in $(seq 1 10); do
    HB_AFTER_REG=$(curl -s "http://$JF_ADDR/events" | python3 -c "
import json, sys
events = json.load(sys.stdin)
regs = [e['ts'] for e in events if e['action'] == 'register' and e['address'] == '$COORD_ADDR']
hbs = [e['ts'] for e in events if e['action'] == 'heartbeat' and e['address'] == '$COORD_ADDR']
print(1 if regs and hbs and max(hbs) > max(regs) else 0)")
    if [[ "$HB_AFTER_REG" == "1" ]]; then
        HB_OK=1
        break
    fi
    sleep 1
done
if [[ "$HB_OK" == "1" ]]; then
    log_pass "heartbeats resumed after re-register"
else
    log_fail "no heartbeat after re-register"
fi

echo ""
echo "=== TC2: worker retries through an empty-discovery window ==="
# Open the window: kill the coordinator, let TTL expire.
kill -9 "$COORD_PID"
PIDS=("${PIDS[@]/$COORD_PID/}")
if wait_discover "$JF_ADDR" "$SERVICE_NAME" true $((TTL_SEC + 5)); then
    log_pass "discovery window open (empty)"
else
    log_fail "discovery not empty after coordinator kill"
fi

"$BUILD_DIR/worker_test" \
    --config "$ROOT_DIR/worker_config.json" \
    --jf "$JF_ADDR" --service "$SERVICE_NAME" > "$ROOT_DIR/worker/stdout.log" 2>&1 &
WORKER_PID=$!
PIDS+=("$WORKER_PID")
sleep 4
if kill -0 "$WORKER_PID" 2>/dev/null && grep -q "next retry" "$ROOT_DIR/worker/stdout.log"; then
    log_pass "worker alive and retrying during empty window"
else
    log_fail "worker died or is not retrying"
    tail -5 "$ROOT_DIR/worker/stdout.log"
fi

# Close the window: operator restarts the coordinator (same address).
"$BUILD_DIR/coordinator_test" \
    --config "$ROOT_DIR/coordinator_config.json" \
    --coordinator "$COORD_ADDR" \
    --jf "$JF_ADDR" --service "$SERVICE_NAME" \
    --hooks --ttl "$TTL_SEC" > "$ROOT_DIR/coord/stdout2.log" 2>&1 &
COORD_PID=$!
PIDS+=("$COORD_PID")
wait_tcp "127.0.0.1" "$COORD_PORT" 15 || log_fail "restarted coordinator port not connectable"

if wait_health "$ROOT_DIR/worker/health" 90; then
    log_pass "worker started after discovery window closed"
else
    log_fail "worker never started"
    tail -5 "$ROOT_DIR/worker/stdout.log"
fi

stop_process "$WORKER_PID"
PIDS=("${PIDS[@]/$WORKER_PID/}")

echo ""
echo "=== TC3: non-discovery failure fails fast (no retry) ==="
timeout 10 "$BUILD_DIR/worker_test" --config "$ROOT_DIR/no_such_config.json" \
    --jf "$JF_ADDR" --service "$SERVICE_NAME" > "$ROOT_DIR/worker/fastfail.log" 2>&1
RC=$?
if [[ "$RC" == "1" ]]; then
    log_pass "bad config exits immediately (rc=1)"
else
    log_fail "bad config rc=$RC (expected 1; 124 would mean it retried/hung)"
fi
if grep -q "next retry" "$ROOT_DIR/worker/fastfail.log"; then
    log_fail "bad config was retried (must not be)"
else
    log_pass "no retry on non-discovery failure"
fi

echo ""
echo "=== TC4: JF transport error fails fast (no retry) ==="
DEAD_PORT=$(get_free_port)
timeout 10 "$BUILD_DIR/worker_test" --config "$ROOT_DIR/worker_config.json" \
    --jf "127.0.0.1:$DEAD_PORT" --service "$SERVICE_NAME" > "$ROOT_DIR/worker/tc4.log" 2>&1
RC=$?
if [[ "$RC" == "1" ]]; then
    log_pass "dead JF port exits immediately (rc=1)"
else
    log_fail "dead JF port rc=$RC (124 would mean it retried/hung)"
fi
if grep -q "discovery failed, not retried" "$ROOT_DIR/worker/tc4.log" && ! grep -q "next retry" "$ROOT_DIR/worker/tc4.log"; then
    log_pass "transport error surfaced without retry"
else
    log_fail "unexpected transport-error handling"
    tail -3 "$ROOT_DIR/worker/tc4.log"
fi

echo ""
echo "=== TC5: wrong service name exhausts the 120s budget and exits ==="
T0=$(date +%s)
timeout 200 "$BUILD_DIR/worker_test" --config "$ROOT_DIR/worker_config.json" \
    --jf "$JF_ADDR" --service "no_such_service_jf_res" > "$ROOT_DIR/worker/tc5.log" 2>&1
RC=$?
ELAPSED=$(( $(date +%s) - T0 ))
RETRIES=$(grep -c "next retry" "$ROOT_DIR/worker/tc5.log")
if [[ "$RC" == "1" && "$ELAPSED" -ge 110 && "$RETRIES" -ge 4 ]]; then
    log_pass "wrong service exits rc=1 after ${ELAPSED}s with $RETRIES retries"
else
    log_fail "wrong service rc=$RC elapsed=${ELAPSED}s retries=$RETRIES"
    tail -3 "$ROOT_DIR/worker/tc5.log"
fi
if grep -q "discovery stayed empty for 120s" "$ROOT_DIR/worker/tc5.log"; then
    log_pass "terminal line identifies the empty-registry abort"
else
    log_fail "missing identifiable terminal line"
fi

stop_process "$COORD_PID"
PIDS=("${PIDS[@]/$COORD_PID/}")

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
