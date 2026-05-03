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

readonly DEFAULT_MIN_PORT=10000
readonly DEFAULT_MAX_PORT=32767
readonly WAIT_COMPONENT_READY_TIMES=60
readonly WAIT_COMPONENT_READY_INTERLEAVED_SECS=2

ST_PORT_LOCK_FDS=()
ST_PORT_LOCK_PORTS=()
ST_DEPLOY_DIR=""
ST_STOPPED=0

function st_port_state_dir()
{
  local ns_suffix=""
  if [ -n "${DS_ST_PORT_NAMESPACE:-}" ]; then
    ns_suffix="-${DS_ST_PORT_NAMESPACE}"
  fi
  echo "/tmp/datasystem-st-ports-$(id -u)${ns_suffix}"
}

function st_port_init()
{
  local state_dir
  state_dir=$(st_port_state_dir)
  mkdir -p "${state_dir}/ports" "${state_dir}/leases"
  chmod 700 "${state_dir}" "${state_dir}/ports" "${state_dir}/leases"
}

function st_port_event()
{
  local event=$1
  local port=$2
  local role=$3
  local reason=${4:-}
  local state_dir
  state_dir=$(st_port_state_dir)
  python3 - "$event" "$port" "$role" "$reason" "${PYTEST_CURRENT_TEST:-shell}" "$$" "${state_dir}/events.log" <<'PY'
import json
import sys
import time

event, port, role, reason, test_name, owner_pid, path = sys.argv[1:]
with open(path, "a", encoding="utf-8") as out:
    out.write(json.dumps({
        "ts_ms": int(time.time() * 1000),
        "event": event,
        "port": int(port),
        "role": role,
        "owner_pid": int(owner_pid),
        "test": test_name,
        "reason": reason,
    }, separators=(",", ":")) + "\n")
PY
}

function st_port_probe()
{
  python3 - "$1" <<'PY'
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    sock.bind(("127.0.0.1", port))
finally:
    sock.close()
PY
}

function st_port_range()
{
  if [ -n "${DS_ST_PORT_RANGE:-}" ]; then
    echo "${DS_ST_PORT_RANGE}"
    return
  fi
  if [ -r /proc/sys/net/ipv4/ip_local_port_range ]; then
    local ephemeral_low
    ephemeral_low=$(awk '{print $1}' /proc/sys/net/ipv4/ip_local_port_range)
    if [ "${ephemeral_low}" -gt 10000 ]; then
      echo "10000-$((ephemeral_low - 1))"
      return
    fi
  fi
  echo "${DEFAULT_MIN_PORT}-${DEFAULT_MAX_PORT}"
}

function st_port_reap_once()
{
  local state_dir=$1
  local checks=0
  local lease
  shopt -s nullglob
  for lease in "${state_dir}"/leases/*.json; do
    if [ "${checks}" -ge 32 ]; then
      break
    fi
    checks=$((checks + 1))
    local port owner_pid
    port=$(python3 - "$lease" <<'PY'
import json
import sys
try:
    print(json.load(open(sys.argv[1], encoding="utf-8")).get("port", ""))
except Exception:
    print("")
PY
)
    owner_pid=$(python3 - "$lease" <<'PY'
import json
import sys
try:
    print(json.load(open(sys.argv[1], encoding="utf-8")).get("owner_pid", ""))
except Exception:
    print("")
PY
)
    if [ -z "${port}" ] || [ -z "${owner_pid}" ]; then
      mv "${lease}" "${lease}.corrupt.$(date +%s%3N)" || true
      continue
    fi
    if kill -0 "${owner_pid}" 2>/dev/null; then
      continue
    fi
    local lock_fd
    eval "exec {lock_fd}>\"${state_dir}/ports/${port}.lock\""
    if flock -n "${lock_fd}"; then
      local killed_child=0
      local child_pid
      for child_pid in $(python3 - "$lease" <<'PY'
import json
import sys
try:
    data = json.load(open(sys.argv[1], encoding="utf-8"))
    print(" ".join(str(pid) for pid in data.get("child_pids", []) if int(pid) > 0))
except Exception:
    print("")
PY
); do
        if kill -0 "${child_pid}" 2>/dev/null; then
          kill -TERM "${child_pid}" 2>/dev/null || true
          killed_child=1
        fi
      done
      if [ "${killed_child}" = "1" ]; then
        sleep 0.1
      fi
      if st_port_probe "${port}" >/dev/null 2>&1; then
        rm -f "${lease}"
        st_port_event "stale_reaped" "${port}" "" "owner_pid=${owner_pid}"
      else
        st_port_event "quarantine" "${port}" "" "still_listening_after_owner_exit"
      fi
      flock -u "${lock_fd}" || true
    fi
    eval "exec ${lock_fd}>&-"
  done
  shopt -u nullglob
}

function reserve_st_port()
{
  if [ $# != 2 ] ; then
    echo "USAGE: reserve_st_port role output_var" >&2
    exit 1
  fi
  local role=$1
  local output_var=$2
  st_port_init
  local state_dir range min_port max_port
  state_dir=$(st_port_state_dir)
  range=$(st_port_range)
  min_port=${range%-*}
  max_port=${range#*-}

  local global_fd
  eval "exec {global_fd}>\"${state_dir}/global.lock\""
  flock "${global_fd}"
  st_port_reap_once "${state_dir}"

  local attempts=$((max_port - min_port + 1))
  local i
  for i in $(seq 1 "${attempts}"); do
    local port lock_fd lease_file
    port=$(random_range "${min_port}" "${max_port}")
    if [[ " ${ST_PORT_LOCK_PORTS[*]} " == *" ${port} "* ]]; then
      continue
    fi
    lease_file="${state_dir}/leases/${port}.json"
    eval "exec {lock_fd}>\"${state_dir}/ports/${port}.lock\""
    if ! flock -n "${lock_fd}"; then
      eval "exec ${lock_fd}>&-"
      continue
    fi
    if st_port_probe "${port}" >/dev/null 2>&1; then
      python3 - "$port" "$role" "$$" "${PYTEST_CURRENT_TEST:-shell}" "$lease_file" <<'PY'
import json
import os
import sys
import time

port, role, owner_pid, test_name, path = sys.argv[1:]
with open(path, "w", encoding="utf-8") as out:
    json.dump({
        "port": int(port),
        "host": "127.0.0.1",
        "role": role,
        "owner_pid": int(owner_pid),
        "owner_pgid": os.getpgrp(),
        "test_binary": "llt_util.sh",
        "test_name": test_name,
        "root_dir": os.environ.get("ST_ROOT_DIR", ""),
        "child_pids": [],
        "created_at_unix_ms": int(time.time() * 1000),
        "last_seen_unix_ms": int(time.time() * 1000),
        "allocator_version": 1,
    }, out, indent=2)
    out.write("\n")
PY
      ST_PORT_LOCK_FDS+=("${lock_fd}")
      ST_PORT_LOCK_PORTS+=("${port}")
      st_port_event "reserve" "${port}" "${role}" ""
      flock -u "${global_fd}" || true
      eval "exec ${global_fd}>&-"
      printf -v "${output_var}" '%s' "${port}"
      return
    fi
    st_port_event "skip_bind_failed" "${port}" "${role}" "bind_probe_failed"
    flock -u "${lock_fd}" || true
    eval "exec ${lock_fd}>&-"
  done
  flock -u "${global_fd}" || true
  eval "exec ${global_fd}>&-"
  echo "No free ST port found in ${range}" >&2
  exit 1
}

function st_port_register_child()
{
  local port=$1
  local pid=$2
  local state_dir lease_file
  state_dir=$(st_port_state_dir)
  lease_file="${state_dir}/leases/${port}.json"
  [ -f "${lease_file}" ] || return 0
  python3 - "$lease_file" "$pid" <<'PY'
import json
import sys
import time

path, pid = sys.argv[1], int(sys.argv[2])
with open(path, encoding="utf-8") as inp:
    data = json.load(inp)
children = data.setdefault("child_pids", [])
if pid not in children:
    children.append(pid)
data["last_seen_unix_ms"] = int(time.time() * 1000)
with open(path, "w", encoding="utf-8") as out:
    json.dump(data, out, indent=2)
    out.write("\n")
PY
  st_port_event "register_child" "${port}" "" "${pid}"
}

function release_all_st_ports()
{
  local state_dir
  state_dir=$(st_port_state_dir)
  local i
  for i in "${!ST_PORT_LOCK_FDS[@]}"; do
    local fd=${ST_PORT_LOCK_FDS[$i]}
    local port=${ST_PORT_LOCK_PORTS[$i]}
    rm -f "${state_dir}/leases/${port}.json"
    st_port_event "release" "${port}" "" ""
    flock -u "${fd}" || true
    eval "exec ${fd}>&-"
  done
  ST_PORT_LOCK_FDS=()
  ST_PORT_LOCK_PORTS=()
}

function st_cleanup_on_exit()
{
  set +e
  if [ "${ST_STOPPED}" != "1" ] && [ -n "${ST_DEPLOY_DIR}" ]; then
    stop_all "${ST_DEPLOY_DIR}" || true
  else
    release_all_st_ports || true
  fi
}

function random_range()
{
  shuf -i $1-$2 -n1
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
  ST_ROOT_DIR="${root_dir}"
  export ST_ROOT_DIR
  ST_DEPLOY_DIR="${deploy_dir}"
  ST_STOPPED=0
  trap 'st_cleanup_on_exit' EXIT INT TERM
  local etcd_client_port
  local etcd_peer_port
  reserve_st_port "etcd_client" etcd_client_port
  reserve_st_port "etcd_peer" etcd_peer_port
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
  st_port_register_child "${etcd_client_port}" "${etcd_pid}"
  st_port_register_child "${etcd_peer_port}" "${etcd_pid}"
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

  reserve_st_port "worker_rpc" worker_port
  WORKER_ADDRESS="127.0.0.1:${worker_port}"
  WORKER_ETCD_ADDRESS="127.0.0.1:${etcd_client_port}"
  # make this parameter available to python unit tests.
  # Create SFS directory
  mkdir -p "${root_dir}/datasystem/sfs"
  
  sed -i \
    -e '/"worker_address": {/,/}/ s|"value": "[^"]*"|"value": "'"${WORKER_ADDRESS}"'"|' \
    -e '/"etcd_address": {/,/}/ s|"value": "[^"]*"|"value": "'"${WORKER_ETCD_ADDRESS}"'"|' \
    -e '/"add_node_wait_time_s": {/,/}/ s|"value": "[^"]*"|"value": "0"|' \
    -e '/"l2_cache_type": {/,/}/ s|"value": "[^"]*"|"value": "sfs"|' \
    -e '/"sfs_path": {/,/}/ s|"value": "[^"]*"|"value": "'"${root_dir}/datasystem/sfs"'"|' \
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
  release_all_st_ports
  ST_STOPPED=1
}
