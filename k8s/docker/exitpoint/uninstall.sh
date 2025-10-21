#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

readonly USAGE="
Options:
    -f Location of the file for checking whether the node need to voluntary scale down, it must be set.
       For example, set to '~/.datasystem/worker-status'.
"
readonly WORK_DIR=$(dirname "$(readlink -f "$0")")
source ${WORK_DIR}/utils.sh

function force_kill_worker() {
  wlog "force kill worker."
  pkill -P 1 -11 worker || kill -TERM 1 
  exit 0
}

function liveness_check() {
  local probe_path=$1
  local probe_timeout=$2

  if [[ ! -e "${probe_path}" ]]; then
    ilog "liveness probe file ${probe_path} not exists!"
    return 0
  fi

  local content="$(cat ${probe_path})"
  if grep -q "liveness check failed" "${probe_path}"; then
    elog "liveness probe ${probe_path} check failed: ${content}"
    return 1
  fi

  local probe_last_updata_time=$(stat -c %Z ${probe_path})
  local current_time=$(date +%s)
  if [[ $((current_time - probe_last_updata_time)) -gt $probe_timeout ]]; then
    elog "liveness probe not update in ${probe_timeout}"
    return 1
  fi
}

function main() {
  ilog "container prestop start"
  PROBE_TIMEOUT=30
  while getopts 'f:t:' OPT; do
    case "${OPT}" in
    f)
      STATUS_FILE_PATH="${OPTARG}"
      ;;
    t)
      PROBE_TIMEOUT="${OPTARG}"
      ;;
    ?)
      echo -e "${USAGE}"
      exit 1
      ;;
    esac
  done

  liveness_check ${LIVENESS_CHECK_PATH} ${PROBE_TIMEOUT} || force_kill_worker

  ilog "send TERM to process 1 to gracefully notify worker to exit"
  kill -TERM 1

  while true; do
    if grep -q "worker_stop_status:ready" "${STATUS_FILE_PATH}"; then
      ilog "worker shutdown process finished."
      break;
    fi
    sleep 1
  done

  ilog "delete health check file..."
  [[ -f "${HEALTH_CHECK_PATH}" ]] && rm "${HEALTH_CHECK_PATH:?}"
  [[ -f "${READY_CHECK_PATH}" ]] && rm "${READY_CHECK_PATH:?}"
  ilog "ready to exit, byebye"
  exit 0
}

main "$@"
