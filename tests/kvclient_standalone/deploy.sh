#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

usage() {
    echo "Usage: $0 --deploy|--stop|--clean <deploy.json> [config_template.json]"
    echo ""
    echo "  --deploy  Deploy binary + SDK libs + config to all nodes and start"
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
print(d.get('sdk_lib_dir', ''))
"
}

readarray -t DEPLOY_FIELDS < <(parse_deploy)
REMOTE_WORK_DIR="${DEPLOY_FIELDS[0]}"
BINARY_PATH="${DEPLOY_FIELDS[1]}"
DEFAULT_SSH_USER="${DEPLOY_FIELDS[2]}"
SSH_OPTIONS="${DEPLOY_FIELDS[3]}"
SDK_LIB_DIR="${DEPLOY_FIELDS[4]}"

LISTEN_PORT=$(python3 -c "
import json
with open('$CONFIG_TEMPLATE') as f:
    d = json.load(f)
print(d.get('listen_port', 9000))
")

# Build default peers list (host:port for all nodes).
# Individual nodes can override with their own "peers" field in deploy.json.
DEFAULT_PEERS=$(python3 -c "
import json
with open('$DEPLOY_JSON') as f:
    d = json.load(f)
port = $LISTEN_PORT
peers = []
for n in d.get('nodes', []):
    peers.append('\"http://' + n['host'] + ':' + str(port) + '\"')
print(','.join(peers))
")

# Get per-node peers override as JSON lines: instance_id <tab> peers_json
NODE_PEERS_OVERRIDES=$(python3 -c "
import json
with open('$DEPLOY_JSON') as f:
    d = json.load(f)
for n in d.get('nodes', []):
    if 'peers' in n:
        print(str(n['instance_id']) + '\t' + json.dumps(n['peers']))
" 2>/dev/null || true)

# Read nodes
NODES=$(python3 -c "
import json
with open('$DEPLOY_JSON') as f:
    d = json.load(f)
for n in d.get('nodes', []):
    print(n['host'], n['instance_id'], n.get('ssh_user', ''))
")

# Get peers for a specific instance_id: check override first, else default
get_peers_for_instance() {
    local iid="$1"
    local override
    override=$(echo "$NODE_PEERS_OVERRIDES" | grep "^${iid}	" | cut -f2)
    if [ -n "$override" ]; then
        # Convert JSON array to comma-separated quoted strings
        python3 -c "
import json
peers = json.loads('$override')
print(','.join('\"' + p.strip('\"') + '\"' for p in peers))
"
    else
        echo "$DEFAULT_PEERS"
    fi
}

# Run a command either locally or via SSH depending on host
run_on() {
    local host="$1"
    local user="$2"
    shift 2
    if [ "$host" = "localhost" ]; then
        "$@"
    else
        ssh $SSH_OPTIONS "${user}@${host}" "$@"
    fi
}

# scp to remote (copies locally for localhost)
scp_to() {
    local host="$1"
    local user="$2"
    local src="$3"
    local dst="$4"
    if [ "$host" = "localhost" ]; then
        cp -r "$src" "$dst"
    else
        scp $SSH_OPTIONS -r "$src" "${user}@${host}:${dst}"
    fi
}

do_deploy() {
    local total=0
    local ok=0
    local failed=""

    pids=()
    config_files=()
    while read -r host instance_id ssh_user; do
        [ -z "$host" ] && continue
        local user="${ssh_user:-$DEFAULT_SSH_USER}"
        local is_local="no"
        [ "$host" = "localhost" ] && is_local="yes"

        echo "Deploying to ${host} (instance_id=${instance_id}, local=${is_local})..."

        local peers
        peers=$(get_peers_for_instance "$instance_id")

        # Extract node-level overrides (role, pipeline, notify_pipeline)
        local node_overrides
        node_overrides=$(python3 -c "
import json
with open('$DEPLOY_JSON') as f:
    d = json.load(f)
for n in d.get('nodes', []):
    if n['instance_id'] == $instance_id:
        o = {k: v for k, v in n.items() if k in ('role', 'pipeline', 'notify_pipeline')}
        if o:
            print(json.dumps(o))
        break
" 2>/dev/null || echo "{}")

        local node_config="/tmp/config_${instance_id}.json"
        python3 -c "
import json
with open('$CONFIG_TEMPLATE') as f:
    d = json.load(f)
d['instance_id'] = $instance_id
d['peers'] = [$peers]
import json as _json
try:
    overrides = _json.loads('''$node_overrides''')
    d.update(overrides)
except:
    pass
with open('$node_config', 'w') as f:
    json.dump(d, f, indent=2)
"

        # Build LD_LIBRARY_PATH for the remote command
        local ld_path=""
        if [ -n "$SDK_LIB_DIR" ]; then
            ld_path="${REMOTE_WORK_DIR}/sdk_lib"
        fi

        (
            set -e
            # Create remote dir
            run_on "$host" "$user" mkdir -p "${REMOTE_WORK_DIR}"

            # Copy binary
            scp_to "$host" "$user" "${BINARY_PATH}" "${REMOTE_WORK_DIR}/kvclient_standalone_test"

            # Copy SDK libs if specified
            if [ -n "$SDK_LIB_DIR" ] && [ -d "$SDK_LIB_DIR" ]; then
                echo "  Deploying SDK libs to ${host}..."
                run_on "$host" "$user" rm -rf "${REMOTE_WORK_DIR}/sdk_lib"
                if [ "$host" = "localhost" ]; then
                    cp -r "${SDK_LIB_DIR}" "${REMOTE_WORK_DIR}/sdk_lib"
                else
                    scp $SSH_OPTIONS -r "${SDK_LIB_DIR}" "${user}@${host}:${REMOTE_WORK_DIR}/sdk_lib"
                fi
            fi

            # Copy config
            scp_to "$host" "$user" "${node_config}" "${REMOTE_WORK_DIR}/config_${instance_id}.json"

            # Set binary executable and start
            run_on "$host" "$user" chmod +x "${REMOTE_WORK_DIR}/kvclient_standalone_test"

            # Build nohup command with LD_LIBRARY_PATH
            local env_prefix=""
            if [ -n "$ld_path" ]; then
                env_prefix="LD_LIBRARY_PATH=${ld_path}:\${LD_LIBRARY_PATH:-} "
            fi

            run_on "$host" "$user" bash -c "cd ${REMOTE_WORK_DIR} && ${env_prefix}nohup ./kvclient_standalone_test config_${instance_id}.json > stdout_${instance_id}.log 2>&1 &"

            echo "  ${host} -> OK"
        ) &
        pids+=($!)
        config_files+=("${node_config}")
        total=$((total + 1))
    done <<< "$NODES"

    for pid in "${pids[@]}"; do
        if wait "$pid"; then
            ok=$((ok + 1))
        else
            failed="${failed} unknown"
        fi
    done

    # Clean up temp config files after all background jobs finish
    for cf in "${config_files[@]}"; do
        rm -f "$cf"
    done

    echo ""
    echo "Deploy result: ${ok}/${total} succeeded"
    [ -n "$failed" ] && echo "Failed: ${failed}"
}

do_stop() {
    # For stop mode, use the default peers list (addresses the binary can reach)
    local full_config="/tmp/config_full.json"
    local peers="$DEFAULT_PEERS"

    python3 -c "
import json
with open('$CONFIG_TEMPLATE') as f:
    d = json.load(f)
d['peers'] = [$peers]
with open('$full_config', 'w') as f:
    json.dump(d, f, indent=2)
"

    # Build LD_LIBRARY_PATH for local stop command
    local ld_path=""
    if [ -n "$SDK_LIB_DIR" ]; then
        local abs_sdk_dir
        if [ -d "$SDK_LIB_DIR" ]; then
            abs_sdk_dir="$(cd "$SDK_LIB_DIR" && pwd)"
        else
            abs_sdk_dir="$SDK_LIB_DIR"
        fi
        ld_path="${abs_sdk_dir}"
    fi

    echo "Stopping all instances..."
    if [ -n "$ld_path" ]; then
        LD_LIBRARY_PATH="${ld_path}:${LD_LIBRARY_PATH:-}" "${BINARY_PATH}" --stop "${full_config}"
    else
        "${BINARY_PATH}" --stop "${full_config}"
    fi
    rm -f "${full_config}"
}

do_clean() {
    local total=0
    local ok=0
    local pids=()

    while read -r host instance_id ssh_user; do
        [ -z "$host" ] && continue
        local user="${ssh_user:-$DEFAULT_SSH_USER}"
        total=$((total + 1))

        echo "Cleaning ${host}..."
        (
            if run_on "$host" "$user" bash -c "pkill -f kvclient_standalone_test; rm -rf ${REMOTE_WORK_DIR}"; then
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
