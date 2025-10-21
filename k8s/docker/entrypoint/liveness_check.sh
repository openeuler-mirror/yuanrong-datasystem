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
    -f Location of the liveness probe file, it must be set.
       For example, set to '~/.datasystem/liveness'.
    -t liveness probe timeout in seconds.
"
readonly WORK_DIR=$(dirname "$(readlink -f "$0")")
source ${WORK_DIR}/utils.sh

function main() {
	PROBE_PATH=~/.datasystem/liveness
	PROBE_TIMEOUT=30
	while getopts 'f:t:' OPT; do
		case "${OPT}" in
		f)
			PROBE_PATH="${OPTARG}"
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
	if [[ ! -e "${PROBE_PATH}" ]]; then
		elog "liveness probe file ${PROBE_PATH} not exists!"
		exit 1
	fi

    content="$(cat ${PROBE_PATH})"
    if ! grep -q "liveness check success" "${PROBE_PATH}"; then
        elog "liveness probe ${PROBE_PATH} check failed: ${content}"
        exit 1
    fi

	PROBE_LAST_UPDATA_TIME=$(stat -c %Z ${PROBE_PATH})
	CURRENT_TIME=$(date +%s)
	if [[ $((CURRENT_TIME - PROBE_LAST_UPDATA_TIME)) -gt $PROBE_TIMEOUT ]]; then
		elog "liveness probe not update in ${PROBE_TIMEOUT}"
		exit 1
	fi
}
main "$@"
