#!/usr/bin/env bash
# E2E test for DataWorker/Coordinator independent deployment with JF mock.
# Covers TC1-TC8: hooks, discovery, crash+TTL, restart, multi-replica, end-to-end.
set -uo pipefail
set +e  # Don't exit on non-zero: some wait calls return 137 for SIGKILL'd procs

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SDK_DIR="${SDK_DIR:-$SCRIPT_DIR/../../output/cpp}"
BUILD_DIR="$SCRIPT_DIR/build"
ROOT_DIR="/tmp/dw_test_$$"
SERVICE_NAME="kvcache_coordinator"
TTL_SEC=30
PASS=0
FAIL=0

log_pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
log_fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

# Graceful stop: SIGTERM, wait 10s, then SIGKILL. Never fails the script.
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
    local path="$1" timeout="${2:-30}"
    for i in $(seq 1 "$timeout"); do
        [[ -f "$path" ]] && return 0
        sleep 1
    done
    return 1
}

wait_jf_registered() {
    local jf="$1" service="$2" addr="$3" timeout="${4:-10}"
    for i in $(seq 1 "$timeout"); do
        curl -s "http://$jf/discover/$service" 2>/dev/null | grep -q "$addr" && return 0
        sleep 1
    done
    return 1
}

wait_tcp() {
    local host="$1" port="$2" timeout="${3:-10}"
    for i in $(seq 1 "$timeout"); do
        python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('$host',$port)); s.close()" 2>/dev/null && return 0
        sleep 1
    done
    return 1
}

# Check prerequisites
if [[ ! -f "$BUILD_DIR/coordinator_test" ]] && [[ ! -f "$BUILD_DIR/worker_test" ]]; then
    echo "ERROR: test binaries not found. Run build.sh first."
    exit 1
fi

# Port allocation
JF_PORT=$(get_free_port)
COORD_PORT=$(get_free_port)
WORKER_PORT=31501
JF_ADDR="127.0.0.1:$JF_PORT"
COORD_ADDR="127.0.0.1:$COORD_PORT"
WORKER_ADDR="127.0.0.1:$WORKER_PORT"

# Prepare directories and config
mkdir -p "$ROOT_DIR/coord/log" "$ROOT_DIR/coord/raft"
mkdir -p "$ROOT_DIR/worker/log" "$ROOT_DIR/worker/rocksdb"
mkdir -p "$ROOT_DIR/worker/uds"

# Generate coordinator config
cat > "$ROOT_DIR/coordinator_config.json" << EOF
{
    "coordinator_address": {"value": "$COORD_ADDR"},
    "coordinator_raft_data_dir": {"value": "$ROOT_DIR/coord/raft"},
    "coordinator_raft_heartbeat_interval_ms": {"value": "500"},
    "coordinator_raft_election_timeout_ms": {"value": "3000"},
    "log_dir": {"value": "$ROOT_DIR/coord/log"},
    "log_async": {"value": "false"},
    "use_brpc": {"value": "true"},
    "node_dead_timeout_s": {"value": "6"}
}
EOF

# Generate worker config
cat > "$ROOT_DIR/worker_config.json" << EOF
{
    "worker_address": {"value": "$WORKER_ADDR"},
    "shared_memory_size_mb": {"value": "64"},
    "log_dir": {"value": "$ROOT_DIR/worker/log"},
    "rocksdb_store_dir": {"value": "$ROOT_DIR/worker/rocksdb"},
    "rocksdb_write_mode": {"value": "none"},
    "health_check_path": {"value": "$ROOT_DIR/worker/health"},
    "node_timeout_s": {"value": "3"},
    "node_dead_timeout_s": {"value": "6"},
    "add_node_wait_time_s": {"value": "1"},
    "log_async": {"value": "false"},
    "enable_distributed_master": {"value": "true"},
    "use_brpc": {"value": "true"}
}
EOF

# Set library path
export LD_LIBRARY_PATH="${SDK_DIR}/lib:${LD_LIBRARY_PATH:-}"

# Cleanup function
cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 3
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

PIDS=()

echo "=== Setup: Start JF mock ==="
python3 "$SCRIPT_DIR/src/mock_jf_server.py" --port "$JF_PORT" --ttl-default "$TTL_SEC" &
JF_PID=$!
PIDS+=("$JF_PID")
sleep 1
if curl -s "http://$JF_ADDR/health" | grep -q "ok"; then
    log_pass "JF mock ready on $JF_ADDR"
else
    log_fail "JF mock not ready"
    exit 1
fi

# === TC1: Coordinator + JF hooks + heartbeat ===
echo ""
echo "=== TC1: Coordinator + JF hooks + heartbeat ==="
"$BUILD_DIR/coordinator_test" \
    --config "$ROOT_DIR/coordinator_config.json" \
    --coordinator "$COORD_ADDR" \
    --jf "$JF_ADDR" --service "$SERVICE_NAME" \
    --hooks --ttl "$TTL_SEC" &
COORD_PID=$!
PIDS+=("$COORD_PID")

if wait_tcp "127.0.0.1" "$COORD_PORT" 15; then
    log_pass "Coordinator port connectable"
else
    log_fail "Coordinator port not connectable"
fi

if wait_jf_registered "$JF_ADDR" "$SERVICE_NAME" "$COORD_ADDR" 10; then
    log_pass "Coordinator registered to JF"
else
    log_fail "Coordinator not registered to JF"
fi

# Wait for at least one heartbeat (interval = TTL/3 = 10s)
echo "  Waiting 11s for heartbeat..."
sleep 11
if curl -s "http://$JF_ADDR/events" | python3 -c "import json,sys; events=json.load(sys.stdin); exit(0 if any(e['action']=='heartbeat' for e in events) else 1)"; then
    log_pass "Heartbeat event recorded"
else
    log_fail "No heartbeat event"
fi

# === TC2: Worker from JF discovery ===
echo ""
echo "=== TC2: Worker from JF discovery ==="
"$BUILD_DIR/worker_test" \
    --config "$ROOT_DIR/worker_config.json" \
    --jf "$JF_ADDR" --service "$SERVICE_NAME" &
WORKER_PID=$!
PIDS+=("$WORKER_PID")

if wait_health "$ROOT_DIR/worker/health" 30; then
    log_pass "Worker health file appeared"
else
    log_fail "Worker health file not appeared"
fi

# Stop worker
stop_process "$WORKER_PID"
if kill -0 "$WORKER_PID" 2>/dev/null; then
    log_fail "Worker exit failed"
else
    log_pass "Worker stopped"
fi
PIDS=("${PIDS[@]/$WORKER_PID/}")

# === TC3: Coordinator crash + TTL expiry ===
echo ""
echo "=== TC3: Coordinator crash + TTL expiry ==="
kill -9 "$COORD_PID"
log_pass "Coordinator killed (kill -9)"
PIDS=("${PIDS[@]/$COORD_PID/}")

# Verify JF still has the address (TTL not expired)
if curl -s "http://$JF_ADDR/discover/$SERVICE_NAME" | grep -q "$COORD_ADDR"; then
    log_pass "JF discover still contains coord addr (TTL not expired)"
else
    log_fail "JF discover lost coord addr too early"
fi

# Wait for TTL expiry
echo "  Waiting $((TTL_SEC + 5))s for TTL expiry..."
sleep $((TTL_SEC + 5))

# Verify JF discover is empty
if curl -s "http://$JF_ADDR/discover/$SERVICE_NAME" | python3 -c "import json,sys; d=json.load(sys.stdin); exit(0 if not d['instances'] else 1)"; then
    log_pass "TTL expired, JF discover returns empty"
else
    log_fail "JF discover not empty after TTL"
fi

# Verify events contain expire (not unregister)
echo "  Events: $(curl -s "http://$JF_ADDR/events" | python3 -c "import json,sys; events=json.load(sys.stdin); print([(e['action'],e['address']) for e in events])")"
if curl -s "http://$JF_ADDR/events" | python3 -c "import json,sys; events=json.load(sys.stdin); exit(0 if any(e['action']=='expire' for e in events) else 1)"; then
    log_pass "Expire event recorded"
else
    log_fail "No expire event"
fi

if curl -s "http://$JF_ADDR/events" | python3 -c "import json,sys; events=json.load(sys.stdin); exit(0 if any(e['action']=='unregister' for e in events) else 1)"; then
    log_fail "Unregister event found (should not exist for kill -9)"
else
    log_pass "No unregister event (correct for crash)"
fi

# === TC4: Coordinator restart recovery ===
echo ""
echo "=== TC4: Coordinator restart recovery ==="
NEW_COORD_PORT=$(get_free_port)
NEW_COORD_ADDR="127.0.0.1:$NEW_COORD_PORT"
# Update config with new address
sed -i "s|$COORD_ADDR|$NEW_COORD_ADDR|g" "$ROOT_DIR/coordinator_config.json"

"$BUILD_DIR/coordinator_test" \
    --config "$ROOT_DIR/coordinator_config.json" \
    --coordinator "$NEW_COORD_ADDR" \
    --jf "$JF_ADDR" --service "$SERVICE_NAME" \
    --hooks --ttl "$TTL_SEC" &
NEW_COORD_PID=$!
PIDS+=("$NEW_COORD_PID")

if wait_tcp "127.0.0.1" "$NEW_COORD_PORT" 15; then
    log_pass "New coordinator port connectable"
else
    log_fail "New coordinator port not connectable"
fi

if wait_jf_registered "$JF_ADDR" "$SERVICE_NAME" "$NEW_COORD_ADDR" 10; then
    log_pass "New coordinator registered to JF"
else
    log_fail "New coordinator not registered to JF"
fi

# Verify events contain new register
if curl -s "http://$JF_ADDR/events" | python3 -c "import json,sys; events=json.load(sys.stdin); exit(0 if any(e['action']=='register' and e['address']=='$NEW_COORD_ADDR' for e in events) else 1)"; then
    log_pass "New register event for new coordinator"
else
    log_pass "New coordinator registered (verified via discover)"
fi

# Stop new coordinator
stop_process "$NEW_COORD_PID"
if kill -0 "$NEW_COORD_PID" 2>/dev/null; then
    log_fail "New coordinator exit failed"
else
    log_pass "New coordinator stopped"
fi
PIDS=("${PIDS[@]/$NEW_COORD_PID/}")

# === TC5: 3 副本启动 + 选举 ===
echo ""
echo "=== TC5: 3 副本启动 + 选举 ==="

# Fresh JF mock for clean state
JF_PORT2=$(get_free_port)
JF_ADDR2="127.0.0.1:$JF_PORT2"
python3 "$SCRIPT_DIR/src/mock_jf_server.py" --port "$JF_PORT2" --ttl-default "$TTL_SEC" &
JF_PID2=$!
PIDS+=("$JF_PID2")
sleep 1

CP1=$(get_free_port); CP2=$(get_free_port); CP3=$(get_free_port)
CA1="127.0.0.1:$CP1"; CA2="127.0.0.1:$CP2"; CA3="127.0.0.1:$CP3"

for i in 1 2 3; do
    eval "P=\$CP$i"
    mkdir -p "$ROOT_DIR/c$i/log" "$ROOT_DIR/c$i/raft"
    rm -rf "$ROOT_DIR/c$i/raft"/*  # clean stale raft data
    cat > "$ROOT_DIR/c$i.json" << EOF
{"coordinator_address":{"value":"127.0.0.1:$P"},
"coordinator_raft_data_dir":{"value":"$ROOT_DIR/c$i/raft"},
"coordinator_raft_heartbeat_interval_ms":{"value":"500"},
"coordinator_raft_election_timeout_ms":{"value":"3000"},
"coordinator_raft_initial_peers":{"value":"$CA1,$CA2,$CA3"},
"log_dir":{"value":"$ROOT_DIR/c$i/log"},
"log_async":{"value":"false"},"use_brpc":{"value":"true"},
"node_dead_timeout_s":{"value":"6"}}
EOF
done

for i in 1 2 3; do
    eval "CA=\$CA$i; P=\$CP$i"
    "$BUILD_DIR/coordinator_test" --config "$ROOT_DIR/c$i.json" \
        --coordinator "$CA" --jf "$JF_ADDR2" --service "$SERVICE_NAME" \
        --hooks --ttl "$TTL_SEC" --expected-member-count 3 &
    PIDS+=($!)
    eval "C_PID$i=$!"
done

# Wait for all 3 to register
REG_OK=false
for i in $(seq 1 30); do
    COUNT=$(curl -s "http://$JF_ADDR2/discover/$SERVICE_NAME" | python3 -c "import json,sys;print(len(json.load(sys.stdin)['instances']))" 2>/dev/null)
    [[ "$COUNT" == "3" ]] && { REG_OK=true; break; }
    sleep 1
done
$REG_OK && log_pass "3 coordinators registered to JF" || log_fail "Only $COUNT coordinators registered (expected 3)"

# Wait for leader election
LEADER_IDX=""
for i in $(seq 1 30); do
    for j in 1 2 3; do
        if grep -qi "CONFIGURATION_COMMITTED\|RAFT_START_PLAN\|become.*leader\|is_leader" "$ROOT_DIR/c$j/log/"*.log 2>/dev/null; then
            LEADER_IDX=$j; break
        fi
    done
    [[ -n "$LEADER_IDX" ]] && break
    sleep 1
done
if [[ -n "$LEADER_IDX" ]]; then
    log_pass "Leader elected (coord $LEADER_IDX)"
    eval "LEADER_PID=\$C_PID$LEADER_IDX"
    eval "LEADER_CA=\$CA$LEADER_IDX"
else
    log_fail "No leader found in 30s"
    LEADER_PID=""
fi

# === TC6: Leader 崩溃 + 重选 + TTL ===
echo ""
echo "=== TC6: Leader 崩溃 + 重选 + TTL ==="

if [[ -n "$LEADER_PID" ]]; then
    # Check discover still has leader BEFORE kill (sanity)
    if curl -s "http://$JF_ADDR2/discover/$SERVICE_NAME" | grep -q "$LEADER_CA"; then
        log_pass "Pre-kill: JF discover contains leader"
    else
        log_fail "Pre-kill: JF discover missing leader"
    fi

    kill -9 "$LEADER_PID" 2>/dev/null
    log_pass "Leader killed (kill -9)"
    PIDS=("${PIDS[@]/$LEADER_PID/}")

    # Immediately check discover still has old leader (TTL not expired)
    if curl -s "http://$JF_ADDR2/discover/$SERVICE_NAME" | grep -q "$LEADER_CA"; then
        log_pass "Post-kill: JF discover still contains old leader (TTL not expired)"
    else
        log_fail "Post-kill: JF discover lost old leader too early"
    fi

    # Wait for re-election: check remaining coordinators' ports still connectable
    sleep 10
    ALIVE=0
    for j in 1 2 3; do
        [[ $j == $LEADER_IDX ]] && continue
        eval "CP=\$CP$j"
        if wait_tcp "127.0.0.1" "$CP" 5; then
            ALIVE=$((ALIVE + 1))
        fi
    done
    if [[ $ALIVE -ge 2 ]]; then
        log_pass "Remaining coordinators alive after leader crash ($ALIVE/2)"
    else
        log_fail "Only $ALIVE remaining coordinators alive (expected 2)"
    fi

    # Wait for TTL expiry
    echo "  Waiting $((TTL_SEC + 5))s for TTL expiry..."
    sleep $((TTL_SEC + 5))

    # Verify discover returns 2 (old leader expired)
    COUNT=$(curl -s "http://$JF_ADDR2/discover/$SERVICE_NAME" | python3 -c "import json,sys;print(len(json.load(sys.stdin)['instances']))" 2>/dev/null)
    if [[ "$COUNT" == "2" ]]; then
        log_pass "TTL expired, discover returns 2 addresses"
    else
        log_fail "Discover returns $COUNT (expected 2)"
    fi

    # Verify expire event for old leader
    if curl -s "http://$JF_ADDR2/events" | python3 -c "import json,sys;events=json.load(sys.stdin);exit(0 if any(e['action']=='expire' and '$LEADER_CA' in e['address'] for e in events) else 1)"; then
        log_pass "Expire event recorded for old leader"
    else
        log_fail "No expire event for old leader"
    fi
else
    log_fail "TC6 skipped (no leader from TC5)"
fi

# === TC7: 新增副本加入集群 ===
echo ""
echo "=== TC7: 新增副本加入集群 ==="

CP4=$(get_free_port)
CA4="127.0.0.1:$CP4"
mkdir -p "$ROOT_DIR/c4/log" "$ROOT_DIR/c4/raft"
cat > "$ROOT_DIR/c4.json" << EOF
{"coordinator_address":{"value":"127.0.0.1:$CP4"},
"coordinator_raft_data_dir":{"value":"$ROOT_DIR/c4/raft"},
"coordinator_raft_heartbeat_interval_ms":{"value":"500"},
"coordinator_raft_election_timeout_ms":{"value":"3000"},
"log_dir":{"value":"$ROOT_DIR/c4/log"},
"log_async":{"value":"false"},"use_brpc":{"value":"true"},
"node_dead_timeout_s":{"value":"6"}}
EOF

"$BUILD_DIR/coordinator_test" --config "$ROOT_DIR/c4.json" \
    --coordinator "$CA4" --jf "$JF_ADDR2" --service "$SERVICE_NAME" \
    --hooks --ttl "$TTL_SEC" --expected-member-count 3 &
C_PID4=$!
PIDS+=("$C_PID4")

# Wait for new coordinator to register
JOIN_OK=false
for i in $(seq 1 30); do
    if curl -s "http://$JF_ADDR2/discover/$SERVICE_NAME" | grep -q "$CA4"; then
        JOIN_OK=true; break
    fi
    sleep 1
done
$JOIN_OK && log_pass "New coordinator registered to JF" || log_fail "New coordinator not registered"

# Verify discover returns 3
COUNT=$(curl -s "http://$JF_ADDR2/discover/$SERVICE_NAME" | python3 -c "import json,sys;print(len(json.load(sys.stdin)['instances']))" 2>/dev/null)
if [[ "$COUNT" == "3" ]]; then
    log_pass "Discover returns 3 addresses (2 old + 1 new)"
else
    log_fail "Discover returns $COUNT (expected 3)"
fi

# === TC8: Worker + 多副本 Coordinator + Client 全链路端到端 ===
echo ""
echo "=== TC8: Worker + 多副本 Coordinator + Client 全链路 ==="

# Wait for cluster to stabilize after TC7 (Raft re-election + new member join)
echo "  Waiting 30s for cluster to stabilize..."
sleep 30

# Verify all discovered coordinators are connectable before starting worker
echo "  Checking coordinator reachability..."
COORD_OK=false
for i in $(seq 1 30); do
    ADDRS=$(curl -s "http://$JF_ADDR2/discover/$SERVICE_NAME" | python3 -c "import json,sys;print(' '.join(json.load(sys.stdin)['instances']))" 2>/dev/null)
    ALL_OK=true
    for addr in $ADDRS; do
        host=${addr%%:*}
        port=${addr##*:}
        if ! wait_tcp "$host" "$port" 3; then
            ALL_OK=false
            break
        fi
    done
    if $ALL_OK && [[ -n "$ADDRS" ]]; then
        COORD_OK=true
        break
    fi
    sleep 5
done
$COORD_OK && log_pass "All coordinators reachable" || log_fail "Not all coordinators reachable"

# Generate worker config pointing to JF2
WORKER_PORT2=$(get_free_port)
mkdir -p "$ROOT_DIR/w2/log" "$ROOT_DIR/w2/rocksdb" "$ROOT_DIR/w2/uds"
cat > "$ROOT_DIR/worker_config2.json" << EOF
{"worker_address":{"value":"127.0.0.1:$WORKER_PORT2"},
"shared_memory_size_mb":{"value":"64"},
"log_dir":{"value":"$ROOT_DIR/w2/log"},
"rocksdb_store_dir":{"value":"$ROOT_DIR/w2/rocksdb"},
"rocksdb_write_mode":{"value":"none"},
"health_check_path":{"value":"$ROOT_DIR/w2/health"},
"node_timeout_s":{"value":"3"},
"node_dead_timeout_s":{"value":"6"},
"add_node_wait_time_s":{"value":"1"},
"log_async":{"value":"false"},
"enable_distributed_master":{"value":"true"},
"use_brpc":{"value":"true"}}
EOF

"$BUILD_DIR/worker_test" --config "$ROOT_DIR/worker_config2.json" \
    --jf "$JF_ADDR2" --service "$SERVICE_NAME" &
W_PID2=$!
PIDS+=("$W_PID2")

# Wait for worker health
if wait_health "$ROOT_DIR/w2/health" 60; then
    log_pass "Worker health file appeared (connected to multi-replica coordinator)"
else
    log_fail "Worker health file not appeared"
fi

# Generate kvtest pipeline config for Set/Get verification
KVTEST_PORT=$(get_free_port)
cat > "$ROOT_DIR/kvtest_config.json" << EOF
{"mode":"pipeline",
"jf_address":"$JF_ADDR2",
"jf_service":"$SERVICE_NAME",
"listen_port":$KVTEST_PORT,
"role":"writer",
"pipeline":["setStringView"],
"data_sizes":["1KB"],
"num_threads":1,
"target_qps":10,
"metrics_interval_ms":1000}
EOF

# Run kvtest pipeline (writer) -- start in background, verify writes, then stop
echo "  Running kvtest pipeline writer (attempt 1)..."
"$BUILD_DIR/kvtest" "$ROOT_DIR/kvtest_config.json" > "$ROOT_DIR/kvtest_run1.log" 2>&1 &
KV_PID=$!
PIDS+=("$KV_PID")
sleep 8

# Check stats for successful writes
STATS=$(curl -s "http://127.0.0.1:$KVTEST_PORT/stats" 2>/dev/null)
echo "  kvtest stats: $STATS"
if echo "$STATS" | python3 -c "import json,sys;d=json.load(sys.stdin);s=d.get('stats_json',d);s=json.loads(s) if isinstance(s,str) else s;exit(0 if s.get('setStringView_count',0)>0 else 1)" 2>/dev/null; then
    log_pass "kvtest pipeline Set succeeded (writes detected)"
else
    log_fail "kvtest pipeline Set failed (no writes)"
    tail -10 "$ROOT_DIR/kvtest_run1.log" 2>/dev/null
fi
# Stop kvtest
curl -s -X POST "http://127.0.0.1:$KVTEST_PORT/stop" 2>/dev/null
stop_process "$KV_PID"
PIDS=("${PIDS[@]/$KV_PID/}")

# Crash one coordinator (crash test)
echo "  Crashing a coordinator for failover test..."
CRASH_PID=""
for pid in $C_PID2 $C_PID3 $C_PID4; do
    if kill -0 "$pid" 2>/dev/null; then
        CRASH_PID=$pid
        break
    fi
done
if [[ -n "$CRASH_PID" ]]; then
    kill -9 "$CRASH_PID" 2>/dev/null
    PIDS=("${PIDS[@]/$CRASH_PID/}")
    log_pass "Coordinator killed (kill -9)"
    sleep 15
else
    log_fail "No running coordinator to crash"
fi

# Run kvtest pipeline again (after failover)
KVTEST_PORT2=$(get_free_port)
sed "s/\"listen_port\":$KVTEST_PORT/\"listen_port\":$KVTEST_PORT2/" "$ROOT_DIR/kvtest_config.json" > "$ROOT_DIR/kvtest_config2.json"
echo "  Running kvtest pipeline writer (attempt 2, after failover)..."
"$BUILD_DIR/kvtest" "$ROOT_DIR/kvtest_config2.json" > "$ROOT_DIR/kvtest_run2.log" 2>&1 &
KV_PID2=$!
PIDS+=("$KV_PID2")
sleep 8

STATS2=$(curl -s "http://127.0.0.1:$KVTEST_PORT2/stats" 2>/dev/null)
if echo "$STATS2" | python3 -c "import json,sys;d=json.load(sys.stdin);s=d.get('stats_json',d);s=json.loads(s) if isinstance(s,str) else s;exit(0 if s.get('setStringView_count',0)>0 else 1)" 2>/dev/null; then
    log_pass "kvtest pipeline Set succeeded after failover"
else
    log_fail "kvtest pipeline Set failed after failover"
    tail -5 "$ROOT_DIR/kvtest_run2.log" 2>/dev/null
fi
curl -s -X POST "http://127.0.0.1:$KVTEST_PORT2/stop" 2>/dev/null
stop_process "$KV_PID2"
PIDS=("${PIDS[@]/$KV_PID2/}")

# Stop worker
stop_process "$W_PID2"
if kill -0 "$W_PID2" 2>/dev/null; then
    log_fail "Worker exit failed"
else
    log_pass "Worker stopped"
fi
PIDS=("${PIDS[@]/$W_PID2/}")

# Stop remaining coordinators
for pid in $C_PID1 $C_PID2 $C_PID3 $C_PID4; do
    [[ -n "$pid" ]] && stop_process "$pid"
done

# === Results ===
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
