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

readonly curr_dir=$(dirname "$(readlink -f "$0")")
example_cpp_dir="${curr_dir}/cpp/build"
example_python_dir="${curr_dir}/python"
config_file="${curr_dir}/../config.cmake"
old_ld_path=${LD_LIBRARY_PATH}

function get_var_from_cmake() {
    local var_name="$1"
    local file="$2"
    grep -E "^set\(${var_name} " "$file" | sed -E "s/^set\(${var_name} \"(.*)\"\)/\1/"
}
ds_output_dir=$(get_var_from_cmake "INSTALL_DIR" "$config_file")
run_hetero=$(get_var_from_cmake "BUILD_HETERO" "$config_file")
run_python=$(get_var_from_cmake "PACKAGE_PYTHON" "$config_file")

[[ ! -d "${ds_output_dir}"/service ]] && tar -zxf "${ds_output_dir}"/yr-datasystem-v$(cat "${curr_dir}/../VERSION").tar.gz -C ${ds_output_dir}
python3 -m pip install ${ds_output_dir}/openyuanrong_datasystem-*.whl --force-reinstall

export PATH=$PATH:${curr_dir}/../scripts/modules
export PATH=$PATH:/usr/sbin
. llt_util.sh
run_go="$1"

function run_go_example()
{
    echo "Start to run go example"
    BUILD_GO_DIR="$(realpath "${curr_dir}/../build/package-go")"
    export WORKER_ADDR=$1
    export WORKER_PORT=$2
    export WORKER_ETCD_ADDRESS=$3

    # Run the golang tests
    # We do this here during the example run because we lack an infrastructure for starting
    # and stopping the cluster.  The example has this built in already.
    # In the future we should move this "go test" logic into the unit test paths
    export LD_LIBRARY_PATH=${BUILD_GO_DIR}/lib:${LD_LIBRARY_PATH}
    echo "Set LD_LIBRARY_PATH=${LD_LIBRARY_PATH} before go example test."
    echo "Running golang tests. Using worker ${WORKER_ADDR} on port ${WORKER_PORT}, etcd address: ${WORKER_ETCD_ADDRESS}"
    cd ${BUILD_GO_DIR}/common
    go test
  
    cd ${BUILD_GO_DIR}/object
    go test

    cd ${BUILD_GO_DIR}/kv
    go test

    cd ${BUILD_GO_DIR}/stream
    go test

    echo "Running golang state/object cache demo. Using worker ${WORKER_ADDR} on port ${WORKER_PORT}"
    ${BUILD_GO_DIR}/kv_client_example
    ${BUILD_GO_DIR}/object_client_example

    export LD_LIBRARY_PATH=${old_ld_path}
    echo "Restore LD_LIBRARY_PATH=${LD_LIBRARY_PATH} after go example test."
}

start_all "${example_cpp_dir}" "${ds_output_dir}"
cleanup_once=false
cleanup() {
    if [ "$cleanup_once" = true ]; then
        return
    fi
    cleanup_once=true
    trap '' INT TERM
    echo "stop all service..."
    stop_all "${ds_output_dir}"
}
trap cleanup EXIT INT TERM

# run cpp example
echo -e "---- Running cpp example..."
export LD_LIBRARY_PATH="${ds_output_dir}/datasystem/sdk/cpp/lib:${LD_LIBRARY_PATH}"
echo "Set LD_LIBRARY_PATH=${LD_LIBRARY_PATH} before cpp example test."
${example_cpp_dir}/stream_client_example "127.0.0.1" "${worker_port}"
${example_cpp_dir}/datasystem_example "127.0.0.1" "${worker_port}"
${example_cpp_dir}/object_client_example "127.0.0.1" "${worker_port}" "1000" "false"
${example_cpp_dir}/kv_client_example "127.0.0.1" "${worker_port}"
if [ "x$run_hetero" == "xon" ]; then
    ${example_cpp_dir}/hetero_client_example "127.0.0.1" "${worker_port}"
fi
export LD_LIBRARY_PATH="${old_ld_path}"

# run python example
if [ "x$run_python" == "xon" ]; then
    echo -e "---- Running python example..."
    python ${example_python_dir}/object_client_example.py --host "127.0.0.1" --port "${worker_port}"
    python ${example_python_dir}/kv_client_example.py --host "127.0.0.1" --port "${worker_port}"
    if [ "x$run_hetero" == "xon" ]; then
        python ${example_python_dir}/hetero_client_example.py --host "127.0.0.1" --port "${worker_port}"
        python ${example_python_dir}/ds_tensor_client_example.py --host "127.0.0.1" --port "${worker_port}"
    fi
fi

if [[ "x$run_go" = "xon" ]] ; then
  run_go_example "127.0.0.1" ${worker_port} ${WORKER_ETCD_ADDRESS}
fi