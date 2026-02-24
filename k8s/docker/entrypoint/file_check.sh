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

if [ $# -ne 1 ]; then
    wlog "invalid args, example ./file_check.sh startupProbe"
    exit 1
fi

CHECK_TYPE=$1
CHECK_FILES=()
case "${CHECK_TYPE}" in
    startupProbe)
        CHECK_FILES=(`utils_get_worker_arg_value liveness_check_path`)
        ;;
    readinessProbe)
        CHECK_FILES=(
            `utils_get_worker_arg_value health_check_path`
            `utils_get_worker_arg_value ready_check_path`
        )
        ;;
    *)
        wlog "unsupported check type: ${CHECK_TYPE}"
        exit 1
        ;;
esac

NOT_EXISTS_FILES=""
for file in "${CHECK_FILES[@]}"; do
    if [[ ! -e "${file}" ]]; then
        NOT_EXISTS_FILES="${NOT_EXISTS_FILES} ${file}"
    fi
done

if [ -n "$NOT_EXISTS_FILES" ]; then
    elog "file${NOT_EXISTS_FILES} not exists, ${CHECK_TYPE} failed."
    exit 1
fi
