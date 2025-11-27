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

# Description: util module for test.

set -e

readonly DEFAULT_MIN_PORT=15000
readonly DEFAULT_MAX_PORT=20000
readonly WAIT_COMPONENT_READY_TIMES=60
readonly WAIT_COMPONENT_READY_INTERLEAVED_SECS=2

function is_listening()
{
  local tcp_num
  tcp_num=$(netstat -atun | grep ":$1 " | awk '$1 == "tcp" && $NF == "LISTEN" {print $0}' | wc -l)
  local udp_num
  udp_num=$(netstat -atun | grep ":$1 " | awk '$1 == "udp" && $NF == "127.0.0.1:*" {print $0}' | wc -l)
  (( total_num = tcp_num + udp_num ))
  if [ "${total_num}" == 0 ]; then
    echo "0"
  else
    echo "1"
  fi
}

function random_range()
{
  shuf -i $1-$2 -n1
}

function get_random_port()
{
  local min_port=$1
  local max_port=$2
  local PORT=0
  local res=0
  while [[ "$PORT" == 0 ]]; do
    res=$(random_range "${min_port}" "${max_port}")
    if [ "$(is_listening "${res}")" == 0 ] ; then
        PORT=$res
    fi
  done
  echo "$PORT"
}

function check_etcd_ready()
{
  echo "start check etcd status"
  local check_flag=false
  for i in $(seq 1 ${WAIT_COMPONENT_READY_TIMES}); do
    if etcdctl --endpoints ${etcd_client_url} endpoint health; then
      echo "Etcd is healthy."
      check_flag=true
      break
    else
      sleep ${WAIT_COMPONENT_READY_INTERLEAVED_SECS}
    fi
  done
  if [ "${check_flag}" = false ] ; then
    echo "Etcd is unhealthy after 120s."
    exit 1
  fi
}

function check_worker_ready()
{
  if [ $# != 1 ] ; then
    echo "USAGE: check_worker_ready /path/to/health/check/file"
    exit 1;
  fi
  local health_check_path=$1
  local check_flag=false
  for i in $(seq 1 ${WAIT_COMPONENT_READY_TIMES}); do
    if grep -q "success" ${health_check_path}; then
      echo "Worker is healthy."
      check_flag=true
      break
    else
      sleep ${WAIT_COMPONENT_READY_INTERLEAVED_SECS}
    fi
  done
  if [ "${check_flag}" = false ] ; then
    echo "Worker is unhealthy after 120s."
    exit 1
  fi
}

function start_all()
{
  if [ $# != 2 ] ; then
    echo "USAGE: start_worker /path/to/store/tmp/file /path/to/deploy_dir"
    exit 1;
  fi

  local root_dir=$1
  local deploy_dir=$2

  # Save the pid for shutting it down later
  etcd_pid="0"

 # start etcd cluster
  echo -e "---- Start the etcd cluster"
  local etcd_dir="${root_dir}/etcd_store"
  [[ -d "${etcd_dir}" ]] && rm -rf "${etcd_dir}"
  local etcd_client_port=$(get_random_port "${DEFAULT_MIN_PORT}" "${DEFAULT_MAX_PORT}")
  local etcd_peer_port=$(get_random_port "${DEFAULT_MIN_PORT}" "${DEFAULT_MAX_PORT}")
  local etcd_client_url="http://127.0.0.1:${etcd_client_port}"
  local etcd_peer_url="http://127.0.0.1:${etcd_peer_port}"
  etcd -name "etcd0" --data-dir "${etcd_dir}/etcd0" --log-level "error" --listen-client-urls "${etcd_client_url}" --advertise-client-urls "${etcd_client_url}" --listen-peer-urls "${etcd_peer_url}" --initial-advertise-peer-urls "${etcd_peer_url}" --initial-cluster "etcd0=${etcd_peer_url}" --initial-cluster-token "etcd-cluster" > /dev/null 2>&1 &
  local etcd_rc=$?
  if [ ${etcd_rc} -ne 0 ] ; then
    echo "Launching etcd-server failed"
    exit 1
  fi
  check_etcd_ready
  etcd_pid=$!
  echo "ETCD cluster launched on port ${etcd_client_port} in process id ${etcd_pid}"

  # start worker
  DSCLI=$(command -v dscli 2>/dev/null || {
    LOCAL_DS_CLI="${HOME}/.local/bin/dscli"
    [ -x "$LOCAL_DS_CLI" ] && echo "$LOCAL_DS_CLI" || {
        echo "---- Could not find command dscli" >&2
        exit 1
    }
  })

  # generate worker config
  ${DSCLI} generate_config -o "${deploy_dir}/datasystem/service"

  worker_port=$(get_random_port "${DEFAULT_MIN_PORT}" "${DEFAULT_MAX_PORT}")
  WORKER_ADDRESS="127.0.0.1:${worker_port}"
  WORKER_ETCD_ADDRESS="127.0.0.1:${etcd_client_port}"
  # make this parameter available to python unit tests.
  sed -i \
    -e '/"worker_address": {/,/}/ s|"value": "[^"]*"|"value": "'"${WORKER_ADDRESS}"'"|' \
    -e '/"etcd_address": {/,/}/ s|"value": "[^"]*"|"value": "'"${WORKER_ETCD_ADDRESS}"'"|' \
    -e '/"add_node_wait_time_s": {/,/}/ s|"value": "[^"]*"|"value": "0"|' \
    "${deploy_dir}/datasystem/service/worker_config.json"

  ${DSCLI} start -d "${root_dir}" -f "${deploy_dir}/datasystem/service/worker_config.json"
  WORKER_HEALTH_CHECK_PATH=$(find "${root_dir}" -type f -path "*/probe/healthy" 2>/dev/null | head -1)

  # wait worker ready
  check_worker_ready "${WORKER_HEALTH_CHECK_PATH}"
}

function stop_all()
{
  if [ $# != 1 ] ; then
    echo "USAGE: stop_worker /path/to/deploy_dir"
    exit 1;
  fi
  local deploy_dir=$1
  ${DSCLI} stop -f "${deploy_dir}/datasystem/service/worker_config.json"
  if ps -p ${etcd_pid} >/dev/null; then
    # interrupt signal will shutdown the etcd cluster
    echo "Shutting down etcd service pid: ${etcd_pid}"
    kill -2 ${etcd_pid} || echo "stop etcd failed: $!"
  fi
}
