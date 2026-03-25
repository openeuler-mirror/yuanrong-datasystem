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

# Description: To compile third-party libraries in parallel
set -e

CMAKE_FILES_L0=(
  "cjson.cmake"
  "absl.cmake"
  "iconv.cmake"
  "jemalloc.cmake"
  "libzmq.cmake"
  "nlohmann_json.cmake"
  "openssl.cmake"
  "pcre.cmake"
  "rocksdb.cmake"
  "securec.cmake"
  "spdlog.cmake"
  "tbb.cmake"
  "xml2.cmake"
  "zlib.cmake"
  )

CMAKE_FILES_L1=(
  "re2.cmake" # absl
  "protobuf.cmake" # absl
  "libcurl.cmake" # openssl
  "grpc.cmake" # protobuf/openssl/zlib
  "sdk_c_obs.cmake"
  )

DEPENDENCIES=(
  "re2:absl"
  "protobuf:absl" 
  "libcurl:openssl"
  "grpc:protobuf:openssl:zlib:re2"
  "sdk_c_obs:libcurl:securec:openssl:iconv:xml2:pcre:cjson:spdlog"
)

function is_on() {
  if [[ "X$1" = "Xon" ]]; then
    return 0
  else
    return 1
  fi
}

if is_on "${BUILD_TESTCASE}"; then
  CMAKE_FILES_L0+=("gtest.cmake")
fi

if is_on "${PACKAGE_PYTHON}"; then
  CMAKE_FILES_L0+=("pybind11.cmake")
fi

if is_on "${BUILD_HETERO}"; then
  CMAKE_FILES_L0+=("ascend.cmake")
fi

readonly BASE_DIR=$(dirname "$(readlink -f "$0")")
BASEPATH=$(cd "$(dirname $0)"; pwd)
DATASYSTEM_HOME="$(realpath "${BASEPATH}/..")"

BUILD_PATH=$1
MULTI_BUILD_PATH="$(realpath "${BUILD_PATH}/multi_build")"
BUILD_THREAD_NUM=$2
CMAKE_OPTIONS=("${@:3}")
DEPENDENCY_DIR="${MULTI_BUILD_PATH}"/dependency
TIME_DIR="${MULTI_BUILD_PATH}"/timing
PROGRESS_DIR="${MULTI_BUILD_PATH}"/progress

# total libs number
TOTAL_LIBS=$((${#CMAKE_FILES_L0[@]} + ${#CMAKE_FILES_L1[@]}))

function log() {
  local progress=$(get_progress_percent)
  printf "[%3d%%] %s\n" "$progress" "$1"
}

function log_failed() {
  log "$1"
}

function get_progress_percent() {
  local completed_count=$(ls -1 "${PROGRESS_DIR}" 2>/dev/null | wc -l)
  local percent=$((completed_count * 100 / TOTAL_LIBS))
  echo "$percent"
}

function on_exit() {
  local exit_status=$?
  local lib_name="${cmake_file%%.*}"
  local end_time=$(date +%s)
  local total_duration=$((end_time - overall_start_time))
  
  if [ $exit_status -eq 0 ]; then
    echo "$total_duration" > "${TIME_DIR}/${lib_name}.time"
    touch "${PROGRESS_DIR}/${lib_name}.complete"
    touch "${DEPENDENCY_DIR}/${lib_name}.complete"
    log "$lib_name build success - ${total_duration}s"
  else
    touch "${DEPENDENCY_DIR}/${lib_name}.failed"
    log "$lib_name build failed, see ${tmp_compile_dir}/log for details"
  fi
}

function wait_for_dependencies() {
  local lib_name=$1
  local dependencies=$2
  local lib_dependencies=()

  if [ -n "$dependencies" ]; then
    lib_dependencies_str=$(echo "$dependencies" | tr ':' ' ')
    IFS=' ' read -r -a lib_dependencies <<< "${lib_dependencies_str}"

    log "$lib_name is still waiting for: ${lib_dependencies_str}"
    
    local timeout=1800
    local wait_start_time=$(date +%s)
    
    for ((i=1; i<=timeout; i++)); do
      all_files_created=true
      missing_deps=()
      failed_deps=()
      for file in "${lib_dependencies[@]}"; do
        if [ -f "${DEPENDENCY_DIR}/${file}.failed" ]; then
          failed_deps+=("$file")
        elif [ ! -f "${DEPENDENCY_DIR}/${file}.complete" ]; then
          all_files_created=false
          missing_deps+=("$file")
        fi
      done

      if [ ${#failed_deps[@]} -gt 0 ]; then
        log_failed "$lib_name failed because dependencies failed: ${failed_deps[*]}"
        return 1
      fi

      if [ "$all_files_created" = true ]; then
        local current_time=$(date +%s)
        local wait_duration=$((current_time - wait_start_time))
        log "$lib_name all dependencies ready (waited ${wait_duration}s)"
        return 0
      fi

      # Display wait status every 30 seconds.
      if [ $((i % 30)) -eq 0 ]; then
        local current_time=$(date +%s)
        local elapsed=$((current_time - wait_start_time))
        log "$lib_name still waiting for: ${missing_deps[*]} (${elapsed}s elapsed)"
      fi
      sleep 1
    done
    
    log_failed "$lib_name timeout waiting for dependencies"
    return 1
  fi
  return 0
}

function main() {
  log "Start compiling thirdparty libraries in parallel"
  local start_time_s
  local end_time_s
  local sumTime
  start_time_s=$(date +%s)
  mkdir -p ${DEPENDENCY_DIR}
  mkdir -p ${TIME_DIR}
  mkdir -p ${PROGRESS_DIR}
  
  log "Total libraries to compile: $TOTAL_LIBS"

  # Compile L0-level libraries (dependency-free)
  log "Building independent libraries (L0)"
  for cmake_file in "${CMAKE_FILES_L0[@]}";
  do
    tmp_compile_dir="${MULTI_BUILD_PATH}/${cmake_file%%.*}"
    mkdir -p "${tmp_compile_dir}"
    (
      lib_name="${cmake_file%%.*}"
      overall_start_time=$(date +%s)
      log "Building $lib_name..."
      trap on_exit EXIT
      cd "${tmp_compile_dir}"
      cat > "CMakeLists.txt" << EOF
cmake_minimum_required(VERSION 3.14.1)
set(CMAKE_SOURCE_DIR ${DATASYSTEM_HOME})
set(CMAKE_BINARY_DIR ${BUILD_PATH})
set(EXPORT_TO_USER_ENV_FILE "${MULTI_BUILD_PATH}/L0_ENV")
include(${DATASYSTEM_HOME}/cmake/options.cmake)
include(${DATASYSTEM_HOME}/cmake/util.cmake)
include(${DATASYSTEM_HOME}/cmake/external_libs/${cmake_file})
EOF
      mkdir -p build && cd build
      cmake "${CMAKE_OPTIONS[@]}" .. >> "${tmp_compile_dir}/log" 2>&1
      cmake --build . >> "${tmp_compile_dir}/log" 2>&1
    ) &
  done

  touch ${MULTI_BUILD_PATH}/L1_ENV
  
  # Compile L1-level libraries
  log "Building dependent libraries (L1)"
  for cmake_file in "${CMAKE_FILES_L1[@]}";
  do
    tmp_compile_dir="${MULTI_BUILD_PATH}/${cmake_file%%.*}"
    mkdir -p "${tmp_compile_dir}"
    
    # Find dependencies
    dependencies=""
    for dep in "${DEPENDENCIES[@]}"; do
      if [[ "$dep" == "${cmake_file%%.*}:"* ]]; then
        dependencies=$(echo "${dep}" | cut -d':' -f2-)
        break
      fi
    done

    (
      lib_name="${cmake_file%%.*}"
      overall_start_time=$(date +%s)
      # Wait for dependencies to complete
      if ! wait_for_dependencies "$lib_name" "$dependencies"; then
          exit 1
      fi

      log "Building $lib_name..."
      trap on_exit EXIT
      cd "${tmp_compile_dir}"
      cat > "CMakeLists.txt" << EOF
cmake_minimum_required(VERSION 3.14.1)
set(CMAKE_SOURCE_DIR ${DATASYSTEM_HOME})
set(CMAKE_BINARY_DIR ${BUILD_PATH})
$(cat ${MULTI_BUILD_PATH}/L0_ENV)
$(cat ${MULTI_BUILD_PATH}/L1_ENV)
set(EXPORT_TO_USER_ENV_FILE "${MULTI_BUILD_PATH}/L1_ENV")
include(${DATASYSTEM_HOME}/cmake/options.cmake)
include(${DATASYSTEM_HOME}/cmake/util.cmake)
include(${DATASYSTEM_HOME}/cmake/external_libs/${cmake_file})
EOF
      mkdir -p build && cd build
      cmake "${CMAKE_OPTIONS[@]}" .. >> "${tmp_compile_dir}/log" 2>&1
      cmake --build . -j "${BUILD_THREAD_NUM}" >> "${tmp_compile_dir}/log" 2>&1
    ) &
  done
  wait

  end_time_s=$(date +%s)
  sumTime=$(($end_time_s - $start_time_s))

  log "Compile thirdparty libraries success, total wall time: ${sumTime}s"
  exit 0
}

main "$@"