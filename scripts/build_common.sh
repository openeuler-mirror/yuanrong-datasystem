#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

# Description: Common functions shared by cmake and bazel build scripts.

function init_default_opts() {
  export BUILD_TYPE="Release"
  export INSTALL_DIR="${DATASYSTEM_DIR}/output"
  export BUILD_DIR="${DATASYSTEM_DIR}/build"
  export BUILD_THREAD_NUM=8
  export TRANSFER_ENGINE_BUILD_THREAD_NUM=8
  export ADDITIONAL_CMAKE_OPTIONS=()
  export BUILD_INCREMENT="off"
  export BUILD_WITH_NINJA="off"

  # Build system selector
  export BUILD_SYSTEM="cmake"

  # For communication layer
  export BUILD_WITH_URMA="off"
  export BUILD_WITH_URMA_MOCK="off"
  export DOWNLOAD_UB="off"
  export BUILD_WITH_RDMA="off"

  # For testcase
  export BUILD_TESTCASE="off"
  export BUILD_COVERAGE="off"
  export GEN_HTML_COVERAGE="off"
  export RUN_TESTS="off"
  export TEST_PARALLEL_JOBS=8
  export LLT_LABELS="level*"
  export LLT_LABELS_EXCLUDE=""
  export LLT_TIMEOUT_S=80

  # For debug
  export ENABLE_STRIP="on"
  export ENABLE_PERF="off"
  export USE_SANITIZER="off"

  # For packaging multi-language
  export PACKAGE_PYTHON="on"
  export PACKAGE_JAVA="off"
  export PYTHON_ROOT_DIR=""
  export PACKAGE_GO="off"

  # Whether to build device object.
  export BUILD_HETERO="on"
  export BUILD_HETERO_MODE="on"

  # Hetero sub-options (defaults match cmake/options.cmake)
  export BUILD_HETERO_NPU="on"
  export BUILD_HETERO_GPU="off"

  # Whether to support os pipeline h2d
  export BUILD_PIPLN_H2D="off"

  # Whether to support jemalloc memory profiling
  export SUPPORT_JEPROF="off"
}

function check_on_off() {
  if [[ "X$1" != "Xon" && "X$1" != "Xoff" ]]; then
    echo -e "Invalid value $1 for option -$2"
    echo -e "${USAGE}"
    exit 1
  fi
}

function check_sanitizers() {
  typeset -u name
  local name
  name="$1"
  if [[ "X$name" != "XADDRESS" && "X$name" != "XTHREAD" && "X$name" != "XUNDEFINED" && "X$name" != "XOFF" ]]; then
    echo -e "Invalid value $1 for option -$2"
    echo -e "${USAGE}"
    exit 1
  fi
}

function check_labels() {
  typeset -u labels
  local labels
  labels="$1"
  if [[ ! "$labels" =~ ^(OBJECT|UT|ST|LEVEL)* ]]; then
    echo -e "Invalid value $1 for option -$2"
    echo -e "${USAGE}"
    exit 1
  fi
}

function is_on() {
  if [[ "X$1" = "Xon" ]]; then
    return 0
  else
    return 1
  fi
}

function has_urma_runtime_lib() {
  if command -v ldconfig >/dev/null 2>&1 && ldconfig -p 2>/dev/null | grep -Eq 'liburma\.so' \
    && ldconfig -p 2>/dev/null | grep -Eq 'liburma_ubagg\.so'; then
    return 0
  fi
  [[ -e /usr/lib64/liburma.so || -e /usr/lib64/liburma.so.0 || -e /usr/lib/liburma.so || -e /usr/lib/liburma.so.0 ]] \
    && [[ -e /usr/lib64/urma/liburma_ubagg.so || -e /usr/lib64/urma/liburma_ubagg.so.0 ]]
}

function normalize_build_options() {
  if is_on "${BUILD_PIPLN_H2D}" && [[ "${BUILD_SYSTEM}" == "cmake" ]] && ! is_on "${BUILD_WITH_URMA}"; then
    echo -e "-- [INFO] Pipeline H2D requires URMA. Enabling BUILD_WITH_URMA because -T on was specified."
    BUILD_WITH_URMA="on"
  fi
  if [[ "${BUILD_SYSTEM}" == "cmake" && "${RUN_TESTS}" != "off" ]] && is_on "${BUILD_WITH_URMA}" \
    && ! is_on "${BUILD_WITH_URMA_MOCK}" && ! has_urma_runtime_lib; then
    echo -e "-- [INFO] URMA runtime libraries are not available for tests. Enabling BUILD_WITH_URMA_MOCK instead."
    BUILD_WITH_URMA="off"
    BUILD_WITH_URMA_MOCK="on"
  fi
  # BUILD_WITH_URMA_MOCK and BUILD_WITH_URMA are mutually exclusive.
  if is_on "${BUILD_WITH_URMA_MOCK}" && is_on "${BUILD_WITH_URMA}"; then
    echo -e "-- [Error] -U (BUILD_WITH_URMA_MOCK) and -M (BUILD_WITH_URMA) are mutually exclusive."
    echo -e "${USAGE}"
    exit 1
  fi
}

function check_number() {
  local number_check
  number_check='^([0-9]+)$'
  if [[ "$1" =~ ${number_check} ]]; then
    return 0
  else
    echo -e "Invalid value $1 for option -$2"
    echo -e "${USAGE}"
    exit 1
  fi
}

function go_die() {
  local err_msg="$1"
  local ret="$2"
  echo -e "${err_msg}" >&2
  if [[ -n "${ret}" ]]; then
    exit "${ret}"
  else
    exit 1
  fi
}

function parse_ub_download_options() {
  local args
  check_on_off "$1" D
  DOWNLOAD_UB="$1"
  if [[ $# -eq 3 ]]; then
    UB_URL="$2"
    UB_SHA256="$3"
  fi
}

function remove_running_pids() {
  echo -e "-- Cleaning master processes and worker processes..."
  ps -ef | grep -E "worker|master" | grep "${DATASYSTEM_DIR}" | grep -v grep | awk '{print $2}' | xargs kill -15
}

function clean_dirs() {
  echo -e "cleaning..."
  [[ -d "${DATASYSTEM_DIR}/build" ]] && rm -rf "${DATASYSTEM_DIR}/build"
  [[ -d "${INSTALL_DIR}" ]] && rm -rf "${INSTALL_DIR}"
  [[ -d "${DATASYSTEM_DIR}/third_party/build" ]] && rm -rf "${DATASYSTEM_DIR}/third_party/build"
  [[ -d "${DATASYSTEM_DIR}/example/build" ]] && rm -rf "${DATASYSTEM_DIR}/example/build"
  echo -e "done!"
}

function set_datasystem_version() {
  if [[ "x${DS_VERSION}" != "x" ]]; then
    local char_check='^([a-zA-Z0-9\.]+)$'
    if [[ "$DS_VERSION" =~ ${char_check} ]]; then
      echo "$DS_VERSION" >"${DATASYSTEM_DIR}/VERSION"
    else
      echo -e "The env DS_VERSION $DS_VERSION is invalid."
      echo -e "${USAGE}"
      exit 1
    fi
  fi
}

function version_lt() {
  local lhs="$1"
  local rhs="$2"
  # Compare two version strings. Returns 0 if lhs < rhs.
  local IFS='.'
  local i ver1=($lhs) ver2=($rhs)
  for ((i = 0; i < ${#ver1[@]} || i < ${#ver2[@]}; i++)); do
    if [[ $((10#${ver1[i]:-0})) -lt $((10#${ver2[i]:-0})) ]]; then
      return 0
    elif [[ $((10#${ver1[i]:-0})) -gt $((10#${ver2[i]:-0})) ]]; then
      return 1
    fi
  done
  return 1
}

# Check if Ascend toolkit is available in the environment.
# Detection priority matches cmake/external_libs/ascend.cmake:
#   1. ASCEND_HOME_PATH env var
#   2. ASCEND_CUSTOM_PATH/latest
#   3. /usr/local/Ascend/ascend-toolkit/latest
# Returns 0 if found, 1 if not found.
function check_ascend_env() {
  local ascend_root=""
  if [[ -n "${ASCEND_HOME_PATH}" ]]; then
    ascend_root="${ASCEND_HOME_PATH}"
  elif [[ -n "${ASCEND_CUSTOM_PATH}" ]]; then
    ascend_root="${ASCEND_CUSTOM_PATH}/latest"
  else
    ascend_root="/usr/local/Ascend/ascend-toolkit/latest"
  fi

  if [[ -d "${ascend_root}" ]]; then
    return 0
  fi
  return 1
}
