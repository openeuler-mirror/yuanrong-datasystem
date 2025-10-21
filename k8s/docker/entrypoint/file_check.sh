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

readonly WORK_DIR=$(dirname "$(readlink -f "$0")")
source ${WORK_DIR}/utils.sh

if [ $# -lt 2 ]; then
    wlog "invalid args, example ./file_check.sh startupProbe file1 file2"
    exit
fi

CHECK_TYPE=$1
shift
NOT_EXISTS_FILES=""
for file in "$@"; do
    if [[ ! -e "${file}" ]]; then
        NOT_EXISTS_FILES="${NOT_EXISTS_FILES} ${file}"
    fi
done

if [ -n "$NOT_EXISTS_FILES" ]; then
    elog "file${NOT_EXISTS_FILES} not exists, ${CHECK_TYPE} failed."
    exit 1
fi
