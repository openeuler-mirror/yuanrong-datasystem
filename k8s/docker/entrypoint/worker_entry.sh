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
source ${WORK_DIR}/utils.sh

if [ -n "${WORKER_LOG_DIR}" ] && [ ! -d "${WORKER_LOG_DIR}" ]; then
    umask 0027
    mkdir -p ${WORKER_LOG_DIR}
fi

POD_MEMORY_MB=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes | awk '{print int($1/1024/1024)}')
ilog "check pod memory limit"
if [[ "${SHARE_MEMORY_SIZE}" -gt "${POD_MEMORY_MB}" ]]; then
    elog "The pod memory (resources.datasystemWorker.limits.memory) is less than\
    the shared memory (resources.datasystemWorker.sharedMemory).\
    Pod memory: ${POD_MEMORY_MB} MB, Shared memory: ${SHARE_MEMORY_SIZE} MB."
    exit 1
fi

ilog "container start."

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

bash "${HOME}"/install.sh
source /home/sn/.bashrc
umask 0027

ilog "start worker"
LD_LIBRARY_PATH="${HOME}/datasystem/lib:${LD_LIBRARY_PATH}" datasystem_worker "$@" &
WORKER_PID=$!

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
    bash "${HOME}"/check_taint.sh -p ${WORKER_PID} -f on -n HANDLE_TERM
}

trap handle_term TERM

CHECK_TAINT_INTERVAL_S=5
COUNT=1
while kill -0 ${WORKER_PID}; do
    if [[ $COUNT -eq $CHECK_TAINT_INTERVAL_S ]]; then
        bash "${HOME}"/check_taint.sh -p ${WORKER_PID} -n ROUTINE_INSPECTION || true
        COUNT=1
    else
        COUNT=$((COUNT+1))
    fi
    sleep 1
done
ilog "worker pid ${WORKER_PID} not exists, container exit"
