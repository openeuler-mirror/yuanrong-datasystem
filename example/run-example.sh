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

readonly EXAMPLE_DIR=$(dirname "$(readlink -f "$0")")
example_build_dir="${EXAMPLE_DIR}/build"
ds_output_dir="${EXAMPLE_DIR}/../output"
old_ld_path=${LD_LIBRARY_PATH}

export PATH=$PATH:${EXAMPLE_DIR}/../scripts/modules
export PATH=$PATH:/usr/sbin
. llt_util.sh
run_hetero="$1"
run_perf="$2"

function run_go_example()
{
    echo "Start to run go example"
    BUILD_GO_DIR="$(realpath "${EXAMPLE_DIR}/../build/package-go")"
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
  
    cd ${BUILD_GO_DIR}/objectcache
    go test

    cd ${BUILD_GO_DIR}/statecache
    go test

    echo "Running golang state/object cache demo. Using worker ${WORKER_ADDR} on port ${WORKER_PORT}"
    ${BUILD_GO_DIR}/state_cache_client_demo
    ${BUILD_GO_DIR}/object_cache_client_demo

    export LD_LIBRARY_PATH=${old_ld_path}
    echo "Restore LD_LIBRARY_PATH=${LD_LIBRARY_PATH} after go example test."
}

start_all "${EXAMPLE_DIR}/build" "${ds_output_dir}"

echo -e "---- Running the example..."
export LD_LIBRARY_PATH="${ds_output_dir}/sdk/cpp/lib:${LD_LIBRARY_PATH}"
echo "Set LD_LIBRARY_PATH=${LD_LIBRARY_PATH} before cpp example test."
${example_build_dir}/ds_example "127.0.0.1" "${worker_port}"
${example_build_dir}/object_example "127.0.0.1" "${worker_port}" "1000" "false"
${example_build_dir}/kv_example "127.0.0.1" "${worker_port}"

if [ "x$run_perf" == "xon" ]; then
  ${example_build_dir}/perf_example "127.0.0.1" "${worker_port}"
fi

export LD_LIBRARY_PATH="${old_ld_path}"

stop_all "${ds_output_dir}"
