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
