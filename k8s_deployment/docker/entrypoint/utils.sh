#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

shopt -s expand_aliases

readonly UTILS_WORK_DIR=$(dirname "$(readlink -f "$0")")
readonly UTILS_LOG_FILE=${WORKER_LOG_DIR}/container.log
readonly UTILS_LOCK_FILE=${WORKER_LOG_DIR}/.loglock
readonly UTILS_LOCK_DIR=${WORKER_LOG_DIR}/.loglockdir
readonly UTILS_MAX_LOG_SIZE=10485760 # 10MB
readonly UTILS_MAX_LOG_COUNT=9 # not include the current log file.
readonly UTILS_PREFIX=${UTILS_LOG_FILE%.*}
readonly UTILS_SUFFIX=${UTILS_LOG_FILE##*.}
alias ilog='utils_log I ${BASH_SOURCE##*/}:$LINENO'
alias wlog='utils_log W ${BASH_SOURCE##*/}:$LINENO'
alias elog='utils_log E ${BASH_SOURCE##*/}:$LINENO'

function utils_log_impl() {
    echo -e "$(date -u '+%Y-%m-%dT%H:%M:%S.%6N') | $1 | $2 |  | $$ |  |  | $3" >> ${UTILS_LOG_FILE}
    if [ "$1" == "E" -o "$1" == "W" ]; then
        echo -e "$3" >&2
    fi
}

function utils_rm_logfile() {
    local files=($(ls ${UTILS_PREFIX}.*.${UTILS_SUFFIX}))
    local to_del_count=$((${#files[@]} - $UTILS_MAX_LOG_COUNT))
    for file in "${files[@]}"; do
        if [ ${to_del_count} -lt 1 ]; then
            break
        fi
        to_del_count=$((to_del_count-1))
        utils_log_impl I ${BASH_SOURCE##*/}:$LINENO "rm log file ${file}"
        rm ${file}
    done
}

# rotate the logs if log file exceeds the max size
function utils_rotate_logfile() {
    local cur_time_str=$(date -u "+%Y%m%d%H%M%S")
    local new_log_file=${UTILS_PREFIX}.${cur_time_str}.${UTILS_SUFFIX}
    local file_size=$((du -b ${UTILS_LOG_FILE} 2>/dev/null || echo 0) | awk '{print $1}')
    if [ ${file_size} -gt ${UTILS_MAX_LOG_SIZE} ]; then
        mv ${UTILS_LOG_FILE} ${new_log_file}
        touch ${UTILS_LOG_FILE}
        utils_rm_logfile
    fi
}

function utils_trylock_exec() {
    local cmd="$1"
    if command -v flock >/dev/null 2>&1; then
        flock -n ${UTILS_LOCK_FILE} -c "$cmd"
    else
        # using create dir to simulate flock.
        if mkdir "${UTILS_LOCK_DIR}" 2>/dev/null; then
            ${cmd} || true
            rmdir "${UTILS_LOCK_DIR}"
        else
            # remove dir if the lock dir was created 30s ago.
            local current_time=$(date +%s)
            local update_time=$(stat -c %Z ${UTILS_LOCK_DIR} 2>/dev/null || echo ${current_time})
            local timeout=30
            if [[ $((current_time - update_time)) -gt $timeout ]]; then
                utils_log_impl I ${BASH_SOURCE##*/}:$LINENO "rm timeout lock dir ${UTILS_LOCK_DIR}"
                rmdir "${UTILS_LOCK_DIR}" 2>/dev/null
	        fi
        fi
    fi
}

function utils_log() {
    utils_log_impl "$@"
    local file_size=$((du -b ${UTILS_LOG_FILE} 2>/dev/null || echo 0) | awk '{print $1}')
    if [ ${file_size} -gt ${UTILS_MAX_LOG_SIZE} ]; then
        utils_trylock_exec "bash ${UTILS_WORK_DIR}/utils.sh ROTATE_LOG" || true
    fi
    chmod 640 ${UTILS_LOG_FILE} 2>/dev/null || true
}

# get value from datasystem_worker args, like: --key=value
function utils_get_worker_arg_value() {
    local key="$1"
    ps -ef | awk -v key="${key}" '/datasystem_worker/ {
        for (i = 1; i <= NF; i++) {
            idx = index($i, "=")
            if(idx > 1 && substr($i, 0, idx - 1) == "--" key) {
                print substr($i, idx + 1)
                exit
            }
        }
    }'
}

if [ "${1}" == "ROTATE_LOG" ]; then
    utils_rotate_logfile
fi
