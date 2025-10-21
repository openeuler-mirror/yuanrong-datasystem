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
#               Environment Configuration Library
# =================================================================
# This file defines all possible deployment roles and their configurations.
# The CLI tool will select which set of configurations to load based on command line arguments.

# =================================================================
#               General Parameters
# =================================================================
# The following parameters are shared across all modes and roles

# --- YuanRong & ETCD Common Ports ---
export YUANRONG_WORKER_PORT=32451
export ETCD_CLIENT_PORT=35440

# --- Model and Path Configuration ---
export MODEL_PATH="/workspace/models/Qwen2.5-VL-7B-Instruct"
export PROXY_SERVER_SCRIPT="/workspace/path/vllm_ascend/vllm-ascend/examples/disaggregated_prefill_v1/load_balance_proxy_server_example.py"

# --- Log Path Configuration ---
export LOG_PATH="/workspace/path/vllm_ascend/logs"
export VLLM_LOG_FILE="${LOG_PATH}/vllm_log.txt"
export DATASYSTEM_CLIENT_LOG_DIR="${LOG_PATH}/client_logs"

# --- vLLM Startup Parameters ---
export VISIBLE_DEVICES="0,1,2,3"
export TENSOR_PARALLEL_SIZE=2
export GPU_MEMORY_UTILIZATION=0.9
export MAX_NUM_BATCHED_TOKENS=45000

# --- YuanRong Worker Configuration ---
export SHARED_MEMORY_MB=614400

# --- Advanced Environment Variables ---
export LD_LIBRARY_PATH=/usr/local/python3.11.13/lib/python3.11/site-packages/datasystem/lib:$LD_LIBRARY_PATH 
export USING_PREFIX_CONNECTOR=0
# Temporarily disable multi-level caching for multimodal
export VLLM_USE_V1=1
export HCL_OP_EXPANSION_MODE="AIV"

# --- [PD Merged Mode] Configuration ---
export MERGED_HOST_IP="192.168.0.1"
export MERGED_ETCD_IP="${MERGED_HOST_IP}"
export MERGED_VLLM_PORT=18300
export MERGED_KV_CONFIG='{"kv_connector":"YuanRongConnector","kv_role":"kv_both"}'
export MERGED_VLLM_EXTRA_ARGS=()
export MERGED_VLLM_EXTRA_ARGS=(
    "--no-enable-prefix-caching"
    "--seed" "1024"
    "--served-model-name" "qwen25vl"
    "--max-num-seqs" "400"
    "--max-model-len" "30000"
    "--max-num-batched-tokens" "40000"
    "--trust-remote-code"
    "--allowed-local-media-path" "$MODEL_PATH"
    "--additional-config" '{"torchair_graph_config":{"enabled":false},"ascend_scheduler_config":{"enabled":true,"enable_chunked_prefill":false}}'
)

# --- [PD Separated Mode - Primary Node] Configuration ---
export PRIMARY_HOST_IP="192.168.0.1"
export PRIMARY_ETCD_IP="${PRIMARY_HOST_IP}"
export PRIMARY_VLLM_PORT=18300
export PRIMARY_PROXY_PORT=18500
export PRIMARY_KV_CONFIG='{"kv_connector":"YuanRongConnector","kv_role":"kv_producer"}'
export PRIMARY_VLLM_EXTRA_ARGS=(
    "--no-enable-prefix-caching"
    "--seed" "1024"
    "--served-model-name" "qwen25vl"
    "--max-num-seqs" "400"
    "--max-model-len" "30000"
    "--max-num-batched-tokens" "40000"
    "--trust-remote-code"
    "--allowed-local-media-path" "$MODEL_PATH"
    "--additional-config" '{"torchair_graph_config":{"enabled":false},"ascend_scheduler_config":{"enabled":true,"enable_chunked_prefill":false}}'
)

# --- [PD Separated Mode - Secondary Node] Configuration ---
export SECONDARY_HOST_IP="192.168.0.2"
export SECONDARY_VLLM_PORT=18300
export SECONDARY_KV_CONFIG='{"kv_connector":"YuanRongConnector","kv_role":"kv_consumer"}'
export SECONDARY_VLLM_EXTRA_ARGS=()
export SECONDARY_VLLM_EXTRA_ARGS=(
    "--no-enable-prefix-caching"
    "--seed" "1024"
    "--served-model-name" "qwen25vl"
    "--max-num-seqs" "400"
    "--max-model-len" "30000"
    "--max-num-batched-tokens" "40000"
    "--trust-remote-code"
    "--allowed-local-media-path" "$MODEL_PATH"
    "--additional-config" '{"torchair_graph_config":{"enabled":false,"enable_multistream_shared_expert":false},"ascend_scheduler_config":{"enabled":false},"chunked_prefill_for_mla":true,"enable_weight_nz_layout":true}'
)

# --- Proxy Server General Configuration (Primary Node Only) ---
# Proxy needs to know the complete addresses of Prefill and Decode nodes
export PROXY_PREFILL_HOST="${PRIMARY_HOST_IP}"
export PROXY_PREFILL_PORT="${PRIMARY_VLLM_PORT}"
export PROXY_DECODE_HOST="${SECONDARY_HOST_IP}"
export PROXY_DECODE_PORT="${SECONDARY_VLLM_PORT}"
