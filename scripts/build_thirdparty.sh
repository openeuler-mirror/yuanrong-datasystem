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
  CMAKE_FILES_L1+=("multipath.cmake")
  DEPENDENCIES+=("multipath:ascend:securec")
fi

readonly BASE_DIR=$(dirname "$(readlink -f "$0")")
BASEPATH=$(cd "$(dirname $0)"; pwd)
DATASYSTEM_HOME="$(realpath "${BASEPATH}/..")"

BUILD_PATH=$1
MULTI_BUILD_PATH="$(realpath "${BUILD_PATH}/multi_build")"
BUILD_THREAD_NUM=$2
CMAKE_OPTIONS=("${@:3}")
DEPENDENCY_DIR="${MULTI_BUILD_PATH}"/dependency

on_exit() {
    local exit_status=$?
    if [ $exit_status -eq 0 ]; then
        echo -e "---- Compilie ${cmake_file%%.*} successfully ----"
    else
        echo -e "---- Compilie ${cmake_file%%.*} failed, see ${tmp_compile_dir}/log for more details ----"
    fi
}

function main() {
  echo -e "---- Start compiling third-party libraries in parallel ----"
  local start_time_s
  local end_time_s
  local sumTime
  start_time_s=$(date +%s)
  mkdir -p ${DEPENDENCY_DIR}

  for cmake_file in "${CMAKE_FILES_L0[@]}";
  do
    tmp_compile_dir="${MULTI_BUILD_PATH}/${cmake_file%%.*}"
    mkdir -p "${tmp_compile_dir}"
    (
        echo -e "---- Start compiling ${cmake_file%%.*} ----"
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
        touch ${DEPENDENCY_DIR}/${cmake_file%%.*}.complete
    ) &
  done

  touch ${MULTI_BUILD_PATH}/L1_ENV
  for cmake_file in "${CMAKE_FILES_L1[@]}";
  do
    tmp_compile_dir="${MULTI_BUILD_PATH}/${cmake_file%%.*}"
    mkdir -p "${tmp_compile_dir}"
    for dep in "${DEPENDENCIES[@]}";
    do
      local lib_dependencies=()
      dependencies=$(echo "${dep}" | grep "${cmake_file%%.*}:" | cut -d':' -f2-)
        if [ -z "${dependencies}" ]; then
          continue
        else 
          break
        fi
      done
    (
        lib_dependencies_str=$(echo ${dependencies} | tr ':' ' ')
        IFS=' ' read -r -a lib_dependencies <<< "${lib_dependencies_str}"
        timeout=1800

        echo -e "---- Prepare compiling ${cmake_file%%.*} ----"
        for ((i=1; i<=timeout; i++)); do
            all_files_created=true
            for file in "${lib_dependencies[@]}"; do
                if [ ! -f "${DEPENDENCY_DIR}/${file}.complete" ]; then
                    echo "Waiting for file $file to be created... (Attempt $i of $timeout)"
                    all_files_created=false
                    break
                fi
            done

            if [ "$all_files_created" = true ]; then
                echo "All files have been created."
                break
            fi

            sleep 1  # 每隔1秒检查一次
        done
        echo -e "---- Start compiling ${cmake_file%%.*} ----"
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
        touch ${DEPENDENCY_DIR}/${cmake_file%%.*}.complete
    ) &
  done
  wait

  end_time_s=$(date +%s)
  sumTime=$(($end_time_s - $start_time_s))
  echo -e "---- Finish compiling third-party libraries in parallel. Total use time:$sumTime seconds ----"
  exit 0
}

main "$@"
