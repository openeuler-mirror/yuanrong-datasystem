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

set -e

# Check the number of arguments provided
if [ $# -eq 2 ]; then
    # Use the provided IP address and port
    IP=$1
    PORT=$2
elif [ $# -eq 0 ]; then
    # Use default values if no arguments are provided
    IP="127.0.0.1"
    PORT="31501"
else
    # Show error message if only one argument is provided
    echo "Error: Please provide either 0 or 2 arguments."
    echo "Usage: $0 [IP_ADDRESS] [PORT_NUMBER]"
    exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)"

export LD_LIBRARY_PATH="${ROOT_DIR}/lib:${LD_LIBRARY_PATH}"

mkdir -p "${ROOT_DIR}/build"

cmake -B "${ROOT_DIR}/build" "${ROOT_DIR}"

make -C "${ROOT_DIR}/build"

# Execute the command with the determined IP and port
"${ROOT_DIR}/build/kv_example" $IP $PORT
