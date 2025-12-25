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

# Description: Packaged Golang client
set -e

BASEPATH=$(cd "$(dirname $0)"; pwd)
DATASYSTEM_HOME="$(realpath "${BASEPATH}/..")"
echo "${DATASYSTEM_HOME}"
BUILD_PATH=$1
PACKAGE_PATH="$BUILD_PATH/package-go"

echo "=========${DATASYSTEM_HOME}==================="
if [[ -d "${PACKAGE_PATH}" ]];then
    rm -rf "${PACKAGE_PATH}"
fi

# Copy necessary file to pack_path
mkdir -pv ${PACKAGE_PATH}
cp -rf ${DATASYSTEM_HOME}/output/datasystem/sdk/go/* "${PACKAGE_PATH}/"
cp  ${DATASYSTEM_HOME}/example/go/*.go "${PACKAGE_PATH}/"

# Remove test file in output dir.
find "${DATASYSTEM_HOME}/output/datasystem/sdk/go" -name '*test.go' -exec rm {} \;
find "${DATASYSTEM_HOME}/output/datasystem/sdk/go" -name '*demo.go' -exec rm {} \;
if [[ -d "${DATASYSTEM_HOME}/output/datasystem/sdk/go/test" ]];then
    rm -rf "${DATASYSTEM_HOME}/output/datasystem/sdk/go/test";
fi

old_ld_path=$LD_LIBRARY_PATH
export LD_LIBRARY_PATH="$PACKAGE_PATH/lib:$LD_LIBRARY_PATH"
echo "LD_LIBRARY_PATH is: $LD_LIBRARY_PATH"

# This only validates the compile of the clients.  Does not produce any binary
cd "${PACKAGE_PATH}/kv"
echo "go build in ${PWD}"
go build || exit 1

cd "${PACKAGE_PATH}/object"
echo "go build in ${PWD}"
go build || exit 1

cd "${PACKAGE_PATH}/stream"
echo "go build in ${PWD}"
go build || exit 1

# compile the demo program.  This produces a binary
cd "${PACKAGE_PATH}"
echo "go build in ${PWD}"
go build kv_client_example.go || exit 1
go build object_client_example.go || exit 1

cd "${DATASYSTEM_HOME}"
export LD_LIBRARY_PATH="${old_ld_path}"
echo "reset LD_LIBRARY_PATH to ${LD_LIBRARY_PATH}"
echo "------Successfully created datasystem go package------"
