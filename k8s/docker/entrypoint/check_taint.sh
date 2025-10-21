#!/bin/bash
# Copyright (c) 2024 Huawei Technologies Co., Ltd.
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
    -p The worker's pid.
    -f Force kill or not, choose from: on/off, default: off.
    -n The name of caller.
"
readonly WORK_DIR=$(dirname "$(readlink -f "$0")")
source ${WORK_DIR}/utils.sh

APISERVER="https://kubernetes.default.svc"
SERVICEACCOUNT="/var/run/secrets/kubernetes.io/serviceaccount"
AUTH=$(cat ${SERVICEACCOUNT}/token)
CACERT=${SERVICEACCOUNT}/ca.crt

function main() {
    FORCE_EXIT_WORKER="off"
    CALLER_NAME="UNKNOWN"
	while getopts 'p:f:n:' OPT; do
		case "${OPT}" in
        p)
			WORKER_PID="${OPTARG}"
			;;
        f)
			FORCE_EXIT_WORKER="${OPTARG}"
			;;
        n)
			CALLER_NAME="${OPTARG}"
			;;
		?)
			echo -e "${USAGE}"
			exit 1
			;;
		esac
	done

    # Check whether the worker is exiting. If it is exiting, ignore this round of check.
    if grep -q "worker_stop_status:ready" "${STATUS_FILE_PATH}" || grep -q "voluntary scale in" "${STATUS_FILE_PATH}"\
        || grep -q "waitting worker exit" "${STATUS_FILE_PATH}"; then
        wlog "[${CALLER_NAME}]The worker is ready to exit, so this round of checking is abandoned."
        exit 0
    fi

    # Start checking for taint.
    NEED_VOLUNTARY_SCALE_IN="false"
    TAINT_EFFECT="no_taint"
    # Continue execution even if the curl command fails
    set +e
    response=$(curl -sS --cacert ${CACERT} --header "Authorization: Bearer ${AUTH}" -X GET ${APISERVER}/api/v1/nodes/"${NODE_ID}" 2>&1)
    ret_code=$?
    set -e
    if [ ${ret_code} -ne 0 ]; then
        wlog "execute curl failed, ret code: ${ret_code}, ${response}"
    elif echo "$response" | grep -q "\"status\": \"Failure\","; then
        wlog "[${CALLER_NAME}]Query taint failed, ${response}!"
    else
        # Parse all the taint on this node from the response returned by curl.
        local curr_raw_taint_msg=$(echo $response | grep -zoP '"taints": \[\K[^\]]*' | tr -d '[:space:]')
        local curr_taint_array=()
        while [[ "${curr_raw_taint_msg}" =~ \"key\":\"([^\"]+)\",\"value\":\"([^\"]+)\",\"effect\":\"([^\"]+)\" ]]; do
            local key="${BASH_REMATCH[1]}"
            local value="${BASH_REMATCH[2]}"
            local effect="${BASH_REMATCH[3]}"
            curr_taint_array+=("$key=$value:$effect")
            curr_raw_taint_msg="${curr_raw_taint_msg#*"${BASH_REMATCH[0]}"}"
        done
        # Parse the taints that allows voluntary scaling in from the user's configuration.
        local config_taints_need_to_be_parsed="${SCALE_IN_TAINT}"
        IFS=',' read -ra config_parsed_taints <<<"$config_taints_need_to_be_parsed"
        # Iterate through each taint and check if the taint is currently set.
        for config_taint in "${config_parsed_taints[@]}"; do
            # If the user configures a taint of the PreferNoSchedule type, this round of matching will be skipped
            # because this type of taint is currently not supported to trigger worker voluntary scaling in.
            local effect=$(echo $config_taint | sed 's/^[^:]*://')
            if [[ "${effect}" == "PreferNoSchedule" ]]; then
                continue
            fi
            for curr_taint in "${curr_taint_array[@]}"; do
                curr_taint_without_val=$(echo "$curr_taint" | sed 's/=[^:]*/=/')
                if [[ "$curr_taint" == "$config_taint" || "$curr_taint_without_val" == "$config_taint" ]]; then
                    ilog "[${CALLER_NAME}]The taint check has just detected '${curr_taint}', and will notify worker to voluntary scale in"
                    NEED_VOLUNTARY_SCALE_IN="true"
                    TAINT_EFFECT="${effect}"
                    printf "voluntary scale in\n" > "${STATUS_FILE_PATH}"
                    break
                fi
            done
        done
    fi

    if [ "${FORCE_EXIT_WORKER}" == "on" ] && [ "${NEED_VOLUNTARY_SCALE_IN}" == "false" ]; then
        wlog "[${CALLER_NAME}]Mark worker is exitting"
        echo "waitting worker exit" > "${STATUS_FILE_PATH}"
    fi

    if [ "${FORCE_EXIT_WORKER}" == "on" ] || [ "${NEED_VOLUNTARY_SCALE_IN}" == "true" ]; then
        wlog "[${CALLER_NAME}]Notify worker to begin graceful exit"
        kill -TERM ${WORKER_PID}
        if [ "${TAINT_EFFECT}" == "NoSchedule" ]; then
            wlog "[${CALLER_NAME}]Actively delete pods with taint: ${TAINT_EFFECT}"
            RETRY_NUM=1
            RETRY_NUM_MAX=3
            DELETE_RESPONSE=""
            while [[ ${RETRY_NUM} -lt ${RETRY_NUM_MAX} ]]; do
                set +e
                DELETE_RESPONSE=$(curl -sS --cacert ${CACERT} --header "Authorization: Bearer ${AUTH}" -X DELETE ${APISERVER}/api/v1/namespaces/${NAMESPACE}/pods/"${POD_NAME}" 2>&1)
                RET_CODE=$?
                set -e
                if [ ${RET_CODE} -ne 0 ]; then
                    wlog "execute curl failed, ret code: ${RET_CODE}, ${DELETE_RESPONSE}"
                elif echo "${DELETE_RESPONSE}" | grep -q "Failure"; then
                    elog "[${CALLER_NAME}]Actively delete pods failed, ${DELETE_RESPONSE}!"
                else
                    break
                fi
                RETRY_NUM=$((RETRY_NUM+1))
            done
        fi
    fi
}

main "$@"
