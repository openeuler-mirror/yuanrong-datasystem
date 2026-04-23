#!/usr/bin/env bash
set -euo pipefail
# Multi-node deployment test: local (writer) + openclaw (reader)
# Uses SSH tunnels for etcd and HTTP notification.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKTREE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SDK_LIB_DIR="$WORKTREE_ROOT/output/datasystem/sdk/cpp/lib"
SERVICE_DIR="$WORKTREE_ROOT/output/datasystem/service"
BINARY="$SCRIPT_DIR/build/kvclient_standalone_test"
REMOTE_HOST="${REMOTE_HOST:-openclaw}"
REMOTE_USER="ubuntu"
REMOTE_WORK_DIR="/home/ubuntu/kvclient_test"
LOCAL_ETCD_PORT=2379
LOCAL_HTTP_PORT=9000
REMOTE_HTTP_PORT=9000
# Tunnel ports on openclaw: local etcd → openclaw:22379, local HTTP → openclaw:19000
TUNNEL_ETCD_PORT=22379
TUNNEL_HTTP_LOCAL_PORT=19000  # local sees openclaw:9000 at localhost:19000

cleanup() {
    echo "=== Cleanup ==="
    # Stop local kvclient instance
    pkill -f "kvclient_standalone_test.*config_tunnel" 2>/dev/null || true
    # Stop remote kvclient instance
    ssh "$REMOTE_HOST" "pkill -f kvclient_standalone_test" 2>/dev/null || true
    sleep 1
    # Stop local worker
    export LD_LIBRARY_PATH="$SERVICE_DIR/lib:$SDK_LIB_DIR:${LD_LIBRARY_PATH:-}"
    dscli stop -w "127.0.0.1:31501" 2>/dev/null || true
    # Kill SSH tunnels
    pkill -f "ssh.*-fNR.*22379" 2>/dev/null || true
    pkill -f "ssh.*-fNL.*19000" 2>/dev/null || true
    pkill -f "ssh.*-fNR.*31501" 2>/dev/null || true
    # Clean remote
    ssh "$REMOTE_HOST" "rm -rf $REMOTE_WORK_DIR" 2>/dev/null || true
    echo "Cleanup done"
}
trap cleanup EXIT

echo "=== Multi-Node Deployment Test ==="
echo "Local (writer): instance_id=0, port=$LOCAL_HTTP_PORT"
echo "Remote ($REMOTE_HOST, reader): instance_id=1, port=$REMOTE_HTTP_PORT"

# --- Prerequisite checks ---
if [ ! -f "$BINARY" ]; then
    echo "ERROR: Binary not found at $BINARY. Run 'make' first."
    exit 1
fi
if ! ssh -o ConnectTimeout=5 "$REMOTE_HOST" "echo OK" >/dev/null 2>&1; then
    echo "ERROR: Cannot SSH to $REMOTE_HOST"
    exit 1
fi
if ! etcdctl endpoint health >/dev/null 2>&1; then
    echo "ERROR: etcd not running locally"
    exit 1
fi

# --- Step 1: Set up SSH tunnels ---
echo ""
echo "--- Setting up SSH tunnels ---"
# Tunnel 1: local etcd (2379) → openclaw localhost:22379 (reverse)
ssh -fNR ${TUNNEL_ETCD_PORT}:localhost:${LOCAL_ETCD_PORT} "$REMOTE_HOST"
echo "  etcd tunnel: openclaw:localhost:${TUNNEL_ETCD_PORT} → local:${LOCAL_ETCD_PORT}"
sleep 1

# Verify tunnel
if ssh "$REMOTE_HOST" "curl -s http://127.0.0.1:${TUNNEL_ETCD_PORT}/health" 2>/dev/null | grep -q "true"; then
    echo "  etcd tunnel OK"
else
    echo "ERROR: etcd tunnel not working"
    exit 1
fi

# Tunnel 2: openclaw:9000 → local:19000 (forward)
ssh -fNL ${TUNNEL_HTTP_LOCAL_PORT}:localhost:${REMOTE_HTTP_PORT} "$REMOTE_HOST"
echo "  HTTP tunnel: local:localhost:${TUNNEL_HTTP_LOCAL_PORT} → openclaw:${REMOTE_HTTP_PORT}"
sleep 1

# Tunnel 3: local worker (31501) → openclaw:31501 (reverse)
# KVClient on openclaw discovers worker at 127.0.0.1:31501 from etcd,
# needs to reach it via this tunnel.
ssh -fNR 31501:localhost:31501 "$REMOTE_HOST"
echo "  Worker tunnel: openclaw:localhost:31501 → local:31501"
sleep 1

# --- Step 2: Start local worker ---
echo ""
echo "--- Starting local worker ---"
export LD_LIBRARY_PATH="$SERVICE_DIR/lib:$SDK_LIB_DIR:${LD_LIBRARY_PATH:-}"
# Use short path to avoid Unix domain socket path length limit (108 chars)
rm -rf /tmp/ds_worker_test && mkdir -p /tmp/ds_worker_test
cd /tmp/ds_worker_test
dscli start -w --worker_address "127.0.0.1:31501" \
    --etcd_address "127.0.0.1:${LOCAL_ETCD_PORT}" 2>&1 | tail -1
cd "$SCRIPT_DIR"
echo "  Worker started"

# --- Step 3: Generate configs ---
echo ""
echo "--- Generating configs ---"

# Writer config (local, instance 0)
cat > /tmp/config_tunnel_local.json << 'LOCAL_EOF'
{
  "instance_id": 0,
  "listen_port": 9000,
  "etcd_address": "127.0.0.1:2379",
  "cluster_name": "",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "data_sizes": ["1KB", "4KB"],
  "ttl_seconds": 30,
  "target_qps": 10,
  "num_set_threads": 1,
  "notify_count": 1,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_local.csv",
  "role": "writer",
  "pipeline": ["setStringView"],
  "notify_pipeline": ["getBuffer", "exist"],
  "peers": ["http://127.0.0.1:19000"]
}
LOCAL_EOF

# Reader config (openclaw, instance 1)
cat > /tmp/config_tunnel_remote.json << REMOTE_EOF
{
  "instance_id": 1,
  "listen_port": ${REMOTE_HTTP_PORT},
  "etcd_address": "127.0.0.1:${TUNNEL_ETCD_PORT}",
  "cluster_name": "",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "data_sizes": ["1KB", "4KB"],
  "ttl_seconds": 30,
  "target_qps": 10,
  "num_set_threads": 1,
  "notify_count": 0,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_remote.csv",
  "role": "reader",
  "pipeline": [],
  "notify_pipeline": ["getBuffer", "exist"],
  "peers": []
}
REMOTE_EOF

echo "  Local config: /tmp/config_tunnel_local.json"
echo "  Remote config: /tmp/config_tunnel_remote.json"

# --- Step 4: Deploy to openclaw ---
echo ""
echo "--- Deploying to $REMOTE_HOST ---"
ssh "$REMOTE_HOST" "mkdir -p $REMOTE_WORK_DIR"
scp "$BINARY" "${REMOTE_HOST}:${REMOTE_WORK_DIR}/kvclient_standalone_test"
scp -r "$SDK_LIB_DIR" "${REMOTE_HOST}:${REMOTE_WORK_DIR}/sdk_lib"
scp /tmp/config_tunnel_remote.json "${REMOTE_HOST}:${REMOTE_WORK_DIR}/config_1.json"
echo "  Binary + SDK libs + config deployed"

# Start remote reader instance
ssh "$REMOTE_HOST" "cd ${REMOTE_WORK_DIR} && chmod +x kvclient_standalone_test && \
    LD_LIBRARY_PATH=${REMOTE_WORK_DIR}/sdk_lib:\$LD_LIBRARY_PATH \
    nohup ./kvclient_standalone_test config_1.json > stdout_1.log 2>&1 &"
sleep 3
echo "  Remote reader started"

# Verify remote is listening
if ssh "$REMOTE_HOST" "curl -s http://127.0.0.1:${REMOTE_HTTP_PORT}/stats" 2>/dev/null | grep -q "instance_id"; then
    echo "  Remote reader is alive"
else
    echo "WARNING: Remote reader may not be responding yet, continuing..."
fi

# --- Step 5: Start local writer instance ---
echo ""
echo "--- Starting local writer ---"
cd "$SCRIPT_DIR"
export LD_LIBRARY_PATH="$SDK_LIB_DIR:${LD_LIBRARY_PATH:-}"
cp /tmp/config_tunnel_local.json config/config_tunnel_local.json
./build/kvclient_standalone_test config/config_tunnel_local.json > stdout_local.log 2>&1 &
LOCAL_PID=$!
echo "  Local writer PID: $LOCAL_PID"

# --- Step 6: Wait and collect results ---
echo ""
echo "--- Running test for 20 seconds ---"
sleep 20

# --- Step 7: Collect stats ---
echo ""
echo "--- Results ---"

echo ""
echo "Local writer stats:"
curl -s http://127.0.0.1:${LOCAL_HTTP_PORT}/stats 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "  (no stats)"

echo ""
echo "Remote reader stats (via tunnel):"
curl -s http://127.0.0.1:${TUNNEL_HTTP_LOCAL_PORT}/stats 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "  (no stats)"

echo ""
echo "Remote stdout (last 10 lines):"
ssh "$REMOTE_HOST" "tail -10 ${REMOTE_WORK_DIR}/stdout_1.log" 2>/dev/null

# --- Step 8: Stop ---
echo ""
echo "--- Stopping ---"
kill $LOCAL_PID 2>/dev/null; wait $LOCAL_PID 2>/dev/null
echo "  Local writer stopped"

# Read local summary
echo ""
echo "Local summary:"
cat "$SCRIPT_DIR/summary_0.txt" 2>/dev/null || echo "  (no summary file)"

# Read remote summary
echo ""
echo "Remote summary:"
ssh "$REMOTE_HOST" "cat ${REMOTE_WORK_DIR}/summary_1.txt" 2>/dev/null || echo "  (no summary file)"

# Check remote metrics CSV
echo ""
echo "Remote metrics CSV (last 5 lines):"
ssh "$REMOTE_HOST" "tail -5 ${REMOTE_WORK_DIR}/metrics_remote.csv" 2>/dev/null || echo "  (no metrics file)"

echo ""
echo "=== Test Complete ==="
