#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

set -e
readonly WORK_DIR=$(dirname "$(readlink -f "$0")")

extract_json_value() {
    local file=$1
    local key=$2

    grep -A2 "\"${key}\":" "${file}" | \
        grep '"value":' | \
        sed -E 's/.*"value": *"([^"]*)".*/\1/' | \
        head -1
}
CONFIG_FILE="${KVCACHE_DIR}/worker.config"

WORKER_LOG_DIR=$(extract_json_value "${CONFIG_FILE}" "log_dir")
export WORKER_LOG_DIR=${WORKER_LOG_DIR}

STATUS_FILE_PATH=$(extract_json_value "${CONFIG_FILE}" "log_dir")/worker/worker-status
export STATUS_FILE_PATH=${STATUS_FILE_PATH}

source ${WORK_DIR}/utils.sh

if [ -n "${WORKER_LOG_DIR}" ] && [ ! -d "${WORKER_LOG_DIR}" ]; then
    umask 0027
    mkdir -p ${WORKER_LOG_DIR}
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
    elog "ERROR: Config file not found: ${CONFIG_FILE}"
    exit 1
fi

ilog "Loading configuration from: ${CONFIG_FILE}"

WORKER_ADDR=$(extract_json_value ${CONFIG_FILE} "worker_address" | cut -d':' -f1)

sed -i \
    -e "s|${WORKER_ADDR}|${!WORKER_ADDR}|g" \
    "${CONFIG_FILE}"

if [ -n "${POD_MEMORY_LIMIT_MB}" ]; then
    POD_MEMORY_MB="${POD_MEMORY_LIMIT_MB}"
    ilog "Using memory limit from env: ${POD_MEMORY_MB} MB"
else
    if [ -f "/sys/fs/cgroup/memory/memory.limit_in_bytes" ]; then
        POD_MEMORY_MB=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes | awk '{print int($1/1024/1024)}')
        ilog "Using cgroup v1 memory limit: ${POD_MEMORY_MB} MB"
    elif [ -f "/sys/fs/cgroup/memory.max" ]; then
        MEMORY_MAX=$(cat /sys/fs/cgroup/memory.max)
        if [[ "$MEMORY_MAX" == "max" ]]; then
            POD_MEMORY_MB=8192
            ilog "No memory limit set (cgroup v2), using default: ${POD_MEMORY_MB} MB"
        else
            POD_MEMORY_MB=$(echo "$MEMORY_MAX" | awk '{print int($1/1024/1024)}')
            ilog "Using cgroup v2 memory limit: ${POD_MEMORY_MB} MB"
        fi
    else
        elog "ERROR: Cannot determine memory limit. Set POD_MEMORY_LIMIT_MB env variable." >&2
        exit 1
    fi
fi

ilog "check pod memory limit"

SHARE_MEMORY_SIZE=$(extract_json_value "${CONFIG_FILE}" "shared_memory_size_mb")
if [[ "${SHARE_MEMORY_SIZE}" -gt "${POD_MEMORY_MB}" ]]; then
    elog "The pod memory (resources.datasystemWorker.limits.memory) is less than\
    the shared memory (resources.datasystemWorker.sharedMemory).\
    Pod memory: ${POD_MEMORY_MB} MB, Shared memory: ${SHARE_MEMORY_SIZE} MB."
    exit 1
fi

ilog "container start."

LIVENESS_CHECK_PATH=$(extract_json_value "${CONFIG_FILE}" "liveness_check_path")
if [[ -f "${LIVENESS_CHECK_PATH}" ]]; then
    # the startup probe will start after 5sec
    # remove the existing liveness probe file before startup probe start.
    ilog "delete the existing liveness probe file ${LIVENESS_CHECK_PATH}"
    rm "${LIVENESS_CHECK_PATH}"
fi

BASE_DIR=$(cd "$(dirname "$0")" || exit 1; pwd)

(
    while [ ! -f "${HEALTH_CHECK_PATH}" ]; do
        sleep 1
    done
    [[ -d "${DS_CURVE_KEY_DIR}" ]] && rm -rf "${DS_CURVE_KEY_DIR:?}/"*
    [[ -d "${A_DIR}" ]] && rm -rf "${A_DIR:?}/"*
    [[ -d "${B_DIR}" ]] && rm -rf "${B_DIR:?}/"*
    [[ -d "${C_DIR}" ]] && rm -rf "${C_DIR:?}/"*
    [[ -d "${D_DIR}" ]] && rm -rf "${D_DIR:?}/"*
) &

umask 0027

if [[ "${ENABLE_RDMA}" == "true" ]]; then
    ulimit -l unlimited
fi

ilog "start worker"

WORKER_PID=$(dscli start -f ${CONFIG_FILE} 2>&1 | tee /dev/stderr | grep -oP 'PID: \K\d+' | tail -1)

if [ -z "$WORKER_PID" ]; then
    echo "[ERROR] Failed to extract worker PID"
    exit 1
fi

echo "[INFO] Captured WORKER_PID: $WORKER_PID"

if [ ! -d "${STATUS_FILE_PATH%/*}" ]; then
    echo -e "create ${STATUS_FILE_PATH%/*}"
    mkdir ${STATUS_FILE_PATH%/*}
fi

if [[ ! -e "${STATUS_FILE_PATH}" ]]; then
    touch "${STATUS_FILE_PATH}"
else
    echo "" > "${STATUS_FILE_PATH}"
fi

function handle_term() {
    wlog "receive signal TERM"
    kill -TERM ${WORKER_PID}
}

trap handle_term TERM

while kill -0 ${WORKER_PID}; do
    sleep 1
done

ilog "worker pid ${WORKER_PID} not exists, container exit"
