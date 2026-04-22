#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

usage() {
    echo "Usage: $0 --deploy|--stop|--clean <deploy.json> [config_template.json]"
    echo ""
    echo "  --deploy  Deploy binary + config to all nodes and start"
    echo "  --stop    Stop all instances via kvclient_standalone_test --stop"
    echo "  --clean   Kill processes and remove remote work dirs"
    exit 1
}

if [ $# -lt 2 ]; then
    usage
fi

ACTION="$1"
shift
DEPLOY_JSON="$1"
shift

CONFIG_TEMPLATE="${1:-config.json.example}"

# Parse deploy.json with python
parse_deploy() {
    python3 -c "
import json, sys
with open('$DEPLOY_JSON') as f:
    d = json.load(f)
print(d.get('remote_work_dir', ''))
print(d.get('binary_path', ''))
print(d.get('ssh_user', 'root'))
print(d.get('ssh_options', '-o StrictHostKeyChecking=no'))
"
}

readarray -t DEPLOY_FIELDS < <(parse_deploy)
REMOTE_WORK_DIR="${DEPLOY_FIELDS[0]}"
BINARY_PATH="${DEPLOY_FIELDS[1]}"
DEFAULT_SSH_USER="${DEPLOY_FIELDS[2]}"
SSH_OPTIONS="${DEPLOY_FIELDS[3]}"

# Read nodes
NODES=$(python3 -c "
import json
with open('$DEPLOY_JSON') as f:
    d = json.load(f)
for n in d.get('nodes', []):
    print(n['host'], n['instance_id'], n.get('ssh_user', ''))
")

LISTEN_PORT=$(python3 -c "
import json
with open('$CONFIG_TEMPLATE') as f:
    d = json.load(f)
print(d.get('listen_port', 9000))
")

do_deploy() {
    local total=0
    local ok=0
    local failed=""

    # Build peers list from all nodes
    local peers=""
    while read -r host instance_id ssh_user; do
        [ -z "$host" ] && continue
        peers="${peers}\"http://${host}:${LISTEN_PORT}\","
        total=$((total + 1))
    done <<< "$NODES"
    peers="${peers%,}"

    pids=()
    while read -r host instance_id ssh_user; do
        [ -z "$host" ] && continue
        local user="${ssh_user:-$DEFAULT_SSH_USER}"
        local ssh_target="${user}@${host}"

        echo "Deploying to ${ssh_target} (instance_id=${instance_id})..."

        local node_config="/tmp/config_${instance_id}.json"
        python3 -c "
import json
with open('$CONFIG_TEMPLATE') as f:
    d = json.load(f)
d['instance_id'] = $instance_id
d['peers'] = [$peers]
with open('$node_config', 'w') as f:
    json.dump(d, f, indent=2)
"

        (
            if ssh $SSH_OPTIONS "${ssh_target}" "mkdir -p ${REMOTE_WORK_DIR}" 2>/dev/null && \
               scp $SSH_OPTIONS "${BINARY_PATH}" "${ssh_target}:${REMOTE_WORK_DIR}/kvclient_standalone_test" 2>/dev/null && \
               scp $SSH_OPTIONS "${node_config}" "${ssh_target}:${REMOTE_WORK_DIR}/config_${instance_id}.json" 2>/dev/null && \
               ssh $SSH_OPTIONS "${ssh_target}" "cd ${REMOTE_WORK_DIR} && chmod +x kvclient_standalone_test && nohup ./kvclient_standalone_test config_${instance_id}.json > stdout_${instance_id}.log 2>&1 &" 2>/dev/null; then
                echo "  ${host} -> OK"
                exit 0
            else
                echo "  ${host} -> FAILED"
                exit 1
            fi
            rm -f "${node_config}"
        ) &
        pids+=($!)
    done <<< "$NODES"

    for pid in "${pids[@]}"; do
        if wait "$pid"; then
            ok=$((ok + 1))
        else
            failed="${failed} unknown"
        fi
    done

    echo ""
    echo "Deploy result: ${ok}/${total} succeeded"
    [ -n "$failed" ] && echo "Failed: ${failed}"
}

do_stop() {
    local full_config="/tmp/config_full.json"
    local peers=""
    while read -r host instance_id ssh_user; do
        [ -z "$host" ] && continue
        peers="${peers}\"http://${host}:${LISTEN_PORT}\","
    done <<< "$NODES"
    peers="${peers%,}"

    python3 -c "
import json
with open('$CONFIG_TEMPLATE') as f:
    d = json.load(f)
d['peers'] = [$peers]
with open('$full_config', 'w') as f:
    json.dump(d, f, indent=2)
"

    echo "Stopping all instances..."
    "${BINARY_PATH}" --stop "${full_config}"
    rm -f "${full_config}"
}

do_clean() {
    local total=0
    local ok=0
    local pids=()

    while read -r host instance_id ssh_user; do
        [ -z "$host" ] && continue
        local user="${ssh_user:-$DEFAULT_SSH_USER}"
        local ssh_target="${user}@${host}"
        total=$((total + 1))

        echo "Cleaning ${ssh_target}..."
        (
            if ssh $SSH_OPTIONS "${ssh_target}" "pkill -f kvclient_standalone_test; rm -rf ${REMOTE_WORK_DIR}" 2>/dev/null; then
                echo "  ${host} -> OK"
                exit 0
            else
                echo "  ${host} -> FAILED (may already be stopped)"
                exit 1
            fi
        ) &
        pids+=($!)
    done <<< "$NODES"

    for pid in "${pids[@]}"; do
        if wait "$pid"; then
            ok=$((ok + 1))
        fi
    done

    echo ""
    echo "Clean result: ${ok}/${total}"
}

case "$ACTION" in
    --deploy) do_deploy ;;
    --stop)   do_stop ;;
    --clean)  do_clean ;;
    *)        usage ;;
esac
