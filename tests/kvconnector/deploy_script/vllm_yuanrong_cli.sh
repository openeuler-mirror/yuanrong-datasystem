#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

# =================================================================
#           vLLM & YuanRong Intelligent Deployment Management CLI (Command Line Version)
# =================================================================

set +x

# --- Initialization and Color Definitions ---
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
BLUE="\033[0;34m"
NC="\033[0m"

# --- Help Information ---
usage() {
    echo -e "${GREEN}vLLM & YuanRong Deployment Management Tool${NC}"
    echo ""
    echo -e "Usage: ${YELLOW}$0 <action> <role>${NC}"
    echo ""
    echo -e "${BLUE}Actions:${NC}"
    echo "  start       - Start services for the specified role"
    echo "  stop        - Stop services for the specified role"
    echo "  restart     - Restart services for the specified role"
    echo "  clean       - Stop and clean the environment for the specified role"
    echo "  status      - Check the service status for the specified role"
    echo ""
    echo -e "${BLUE}Roles:${NC}"
    echo "  merged      - PD merged mode deployment"
    echo "  primary     - Primary node in PD separated mode"
    echo "  secondary   - Secondary node in PD separated mode"
    echo ""
    echo "Example: ./$0 start primary"
    exit 1
}

# --- Check Parameters ---
if [ "$#" -ne 2 ]; then
    usage
fi

ACTION="$1"
ROLE="$2"

# --- Load Configuration File ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
ENV_FILE="${SCRIPT_DIR}/deploy_env.sh"
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    echo -e "${RED}❌ Error: Configuration file 'deploy_env.sh' not found!${NC}"
    exit 1
fi

# --- Core Function: Load Dynamic Configuration Based on Role ---
load_role_config() {
    local role_to_load="$1"
    echo -e "${BLUE}--- Loading configuration for role [${YELLOW}${role_to_load}${YELLOW}] ---${NC}"
    export ASCENDRT_VISIBLE_DEVICES="$VISIBLE_DEVICES"

    case "$role_to_load" in
        merged)
            export HOST_IP="$MERGED_HOST_IP"
            export ETCD_IP="$MERGED_ETCD_IP"
            export VLLM_PORT="$MERGED_VLLM_PORT"
            export VLLM_KV_TRANSFER_CONFIG="$MERGED_KV_CONFIG"
            export VLLM_EXTRA_ARGS="${MERGED_VLLM_EXTRA_ARGS[@]}"
            export ENABLE_ETCD=true
            export ENABLE_PROXY=false
            ;;
        primary)
            export HOST_IP="$PRIMARY_HOST_IP"
            export ETCD_IP="$PRIMARY_ETCD_IP"
            export VLLM_PORT="$PRIMARY_VLLM_PORT"
            export VLLM_KV_TRANSFER_CONFIG="$PRIMARY_KV_CONFIG"
            export VLLM_EXTRA_ARGS="${PRIMARY_VLLM_EXTRA_ARGS[@]}"
            export ENABLE_ETCD=true
            export ENABLE_PROXY=true
            ;;
        secondary)
            export HOST_IP="$SECONDARY_HOST_IP"
            export ETCD_IP="$PRIMARY_HOST_IP"
            export VLLM_PORT="$SECONDARY_VLLM_PORT"
            export VLLM_KV_TRANSFER_CONFIG="$SECONDARY_KV_CONFIG"
            export VLLM_EXTRA_ARGS="${SECONDARY_VLLM_EXTRA_ARGS[@]}"
            export ENABLE_ETCD=false
            export ENABLE_PROXY=false
            ;;
        *)
            echo -e "${RED}Error: Unknown role '$role_to_load'.${NC}"
            usage
            ;;
    esac

    echo -e "${GREEN}Configuration loaded successfully: HOST_IP=${HOST_IP}, ETCD_IP=${ETCD_IP}${NC}"
}

# --- Load Configuration for the Specified Role ---
load_role_config "$ROLE"

# --- Service Control Functions ---
stop_vllm() {
    echo -e "\n${YELLOW}--- Stopping vLLM Instance ---${NC}"
    local PID=$(pgrep -f "vllm.*--port ${VLLM_PORT}")

    if [ -z "$PID" ]; then
        echo -e "${GREEN}✅ No vLLM instance found.${NC}"
        return
    fi

    echo "👍 Found PID: ${PID}, terminating..."
    kill ${PID} &>/dev/null
    sleep 3

    if kill -0 ${PID} &>/dev/null; then
        echo "⚠️ Force termination (SIGKILL)..."
        kill -9 ${PID} &>/dev/null
    fi

    echo -e "${GREEN}🎉 vLLM has been stopped.${NC}"
}

stop_ds_worker() {
    echo -e "\n${YELLOW}--- Stopping YuanRong DS-Worker ---${NC}"
    dscli stop --worker_address "${HOST_IP}:${YUANRONG_WORKER_PORT}"
}

stop_etcd() {
    echo -e "\n${YELLOW}--- Stopping ETCD Service ---${NC}"
    if pgrep -f "etcd" > /dev/null; then
        echo "🚀 Stopping etcd process..."
        pkill etcd
        sleep 2
        echo -e "${GREEN}🎉 ETCD has been stopped.${NC}"
    else
        echo -e "${GREEN}✅ No ETCD process found.${NC}"
    fi
}

stop_proxy() {
    echo -e "\n${YELLOW}--- Stopping Proxy Service ---${NC}"
    local PID=$(pgrep -f "${PROXY_SERVER_SCRIPT}")

    if [ -z "$PID" ]; then
        echo -e "${GREEN}✅ No proxy service process found.${NC}"
        return
    fi

    echo "👍 Found PID: ${PID}, terminating..."
    kill -9 ${PID} &>/dev/null
    echo -e "${GREEN}🎉 Proxy service has been stopped.${NC}"
}

clean_files() {
    echo -e "\n${YELLOW}--- Cleaning Generated Files ---${NC}"
    rm -rfv ${LOG_PATH}/* client_logs/* kernel_meta ${VLLM_LOG_FILE} proxy_server_log.txt
    echo -e "${GREEN}🎉 Cleaning completed.${NC}"
}

start_etcd() {
    echo -e "\n${YELLOW}--- Starting ETCD Service ---${NC}"
    local ETCD_PEER_PORT=$((ETCD_CLIENT_PORT + 1))

    etcd --name etcd-yuanrong \
         --data-dir /tmp/etcd-yuanrong \
         --listen-client-urls "http://${ETCD_IP}:${ETCD_CLIENT_PORT}" \
         --advertise-client-urls "http://${ETCD_IP}:${ETCD_CLIENT_PORT}" \
         --listen-peer-urls "http://${ETCD_IP}:${ETCD_PEER_PORT}" \
         --initial-advertise-peer-urls "http://${ETCD_IP}:${ETCD_PEER_PORT}" \
         --initial-cluster "etcd-yuanrong=http://${ETCD_IP}:${ETCD_PEER_PORT}" &

    sleep 3
    echo -e "${GREEN}🎉 ETCD start command sent.${NC}"
}

start_ds_worker() {
    echo -e "\n${YELLOW}--- Starting YuanRong DS-Worker ---${NC}"
    export DS_WORKER_ADDR="${HOST_IP}:${YUANRONG_WORKER_PORT}"

    dscli start \
        -d "${LOG_PATH}" \
        -w \
        --worker_address "${HOST_IP}:${YUANRONG_WORKER_PORT}" \
        --shared_memory_size_mb ${SHARED_MEMORY_MB} \
        --enable_worker_worker_batch_get True \
        --etcd_address "${ETCD_IP}:${ETCD_CLIENT_PORT}" &

    sleep 3
    echo -e "${GREEN}🎉 DS-Worker start command sent.${NC}"
}

start_vllm() {
    echo -e "\n${YELLOW}--- Starting vLLM Inference Instance ---${NC}"
    echo "   - Role Configuration: ${VLLM_KV_TRANSFER_CONFIG}"
    export HCCL_IF_IP="${HOST_IP}"

    vllm serve "${MODEL_PATH}" \
        --host "${HOST_IP}" \
        --port "${VLLM_PORT}" \
        --max-num-batched-tokens ${MAX_NUM_BATCHED_TOKENS} \
        --gpu-memory-utilization ${GPU_MEMORY_UTILIZATION} \
        --trust-remote-code \
        --tensor-parallel-size ${TENSOR_PARALLEL_SIZE} \
        --kv-transfer-config "${VLLM_KV_TRANSFER_CONFIG}" \
        $VLLM_EXTRA_ARGS \
        > "${VLLM_LOG_FILE}" 2>&1 &

    echo -e "${GREEN}🎉 vLLM start command sent, see logs in '${VLLM_LOG_FILE}'.${NC}"
}

start_proxy() {
    echo -e "\n${YELLOW}--- Starting Load Balancing Proxy Service ---${NC}"
    python "${PROXY_SERVER_SCRIPT}" \
        --host "${PRIMARY_HOST_IP}" \
        --port "${PRIMARY_PROXY_PORT}" \
        --prefiller-hosts "${PROXY_PREFILL_HOST}" \
        --prefiller-ports "${PROXY_PREFILL_PORT}" \
        --decoder-hosts "${PROXY_DECODE_HOST}" \
        --decoder-ports "${PROXY_DECODE_PORT}" \
        > ${LOG_PATH}/proxy_server_log.txt 2>&1 &

    echo -e "${GREEN}🎉 Proxy service start command sent, see logs in 'proxy_server_log.txt'.${NC}"
}

# --- Main Logic: Execute Tasks Based on Action ---
echo -e "\n${GREEN}===== Executing Action [${ACTION}] for Role [${ROLE}] =====${NC}"

case "$ACTION" in
    start)
        [ "$ENABLE_ETCD" = true ] && start_etcd
        start_ds_worker
        start_vllm
        [ "$ENABLE_PROXY" = true ] && start_proxy
        echo -e "\n${GREEN}✅ Start process completed.${NC}"
        ;;
    stop)
        [ "$ENABLE_PROXY" = true ] && stop_proxy
        stop_vllm
        stop_ds_worker
        [ "$ENABLE_ETCD" = true ] && stop_etcd
        echo -e "\n${GREEN}✅ Stop process completed.${NC}"
        ;;
    clean)
        $0 stop "$ROLE"
        clean_files
        ;;
    restart)
        $0 stop "$ROLE"
        sleep 2
        $0 start "$ROLE"
        ;;
    status)
        echo -e "\n${YELLOW}--- Service Status Check (Role: ${ROLE}) ---${NC}"
        pgrep -af "dscli|etcd|vllm.*${VLLM_PORT}|${PROXY_SERVER_SCRIPT}" \
            || echo -e "No related service processes found."
        echo ""
        ;;
    *)
        echo -e "${RED}Error: Unknown action '$ACTION'.${NC}"
        usage
        ;;
esac
