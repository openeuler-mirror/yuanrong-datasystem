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

# Description: CMake build functions for datasystem.

function generate_config() {
  local config_file=${BASE_DIR}/config.cmake
  [[ -f "${config_file}" ]] && rm -f "${config_file}"
  echo "set(INSTALL_DIR \"${INSTALL_DIR}\")" >> "${config_file}"
  echo "set(BUILD_HETERO \"${BUILD_HETERO}\")" >> "${config_file}"
  echo "set(BUILD_PIPLN_H2D \"${BUILD_PIPLN_H2D}\")" >> "${config_file}"
  echo "set(PACKAGE_PYTHON \"${PACKAGE_PYTHON}\")" >> "${config_file}"
}

function strip_symbols() {
  local src_dir="$1"
  local dest_dir="$2"
  if [[ ! -d "${dest_dir}" ]]; then
    mkdir -p "${dest_dir}"
  fi

  find "$src_dir" -type f -print0 | while IFS= read -r -d '' file; do
    local type
    type="$(file -b --mime-type ${file} | sed 's|/.*||')"
    local basename
    basename=$(basename "${file}")
    if [[ ! -L "${file}" ]] && [[ ! -d "${file}" ]] && [[ "x${type}" != "xtext" ]] && [[ "x${basename}" != "xlibacl_plugin.so" ]]; then
      echo "---- start to strip ${file}"
      objcopy --only-keep-debug "${file}" "${dest_dir}/${basename}.sym"
      objcopy --add-gnu-debuglink="${dest_dir}/${basename}.sym" "${file}"
      objcopy --strip-all "${file}"
    fi
  done
}

function build_datasystem_cmake() {
  # clean and create build dir.
  [[ "${BUILD_INCREMENT}" == "off" && -d "${BUILD_DIR}" ]] && rm -rf "${BUILD_DIR}" && echo -e "--[Warning] Removed build folder ${BUILD_DIR}"
  mkdir -p "${BUILD_DIR}" && cd "${BUILD_DIR}"
  [[ "${BUILD_INCREMENT}" == "off" && -d "${INSTALL_DIR}" ]] && rm -rf "${INSTALL_DIR}" && echo -e "--[Warning] Removed output folder ${INSTALL_DIR}"
  mkdir -p "${INSTALL_DIR}"

  echo -e "-- building datasystem (cmake) in ${BUILD_DIR}..."

  if [[ "${RUN_TESTS}" != "off" ]]; then
    BUILD_TESTCASE="on"
  fi

  generate_config

  local cmake_options=(
    "${DATASYSTEM_DIR}"
    "-DCMAKE_BUILD_TYPE:STRING=${BUILD_TYPE}"
    "-DUSE_SANITIZER:STRING=${USE_SANITIZER}"
    "-DCMAKE_INSTALL_PREFIX:PATH=${INSTALL_DIR}"
    "-DWITH_TESTS:BOOL=${BUILD_TESTCASE}"
    "-DBUILD_COVERAGE:BOOL=${BUILD_COVERAGE}"
    "-DENABLE_PERF:BOOL=${ENABLE_PERF}"
    "-DBUILD_PYTHON_API:BOOL=${PACKAGE_PYTHON}"
    "-DBUILD_THREAD_NUM:STRING=${BUILD_THREAD_NUM}"
    "-DTRANSFER_ENGINE_BUILD_THREAD_NUM:STRING=${TRANSFER_ENGINE_BUILD_THREAD_NUM}"
    "-DENABLE_STRIP:BOOL=${ENABLE_STRIP}"
    "-DBUILD_HETERO:BOOL=${BUILD_HETERO}"
    "-DBUILD_PIPLN_H2D:BOOL=${BUILD_PIPLN_H2D}"
    "-DBUILD_GO_API:BOOL=${PACKAGE_GO}"
    "-DBUILD_JAVA_API:BOOL=${PACKAGE_JAVA}"
    "-DSUPPORT_JEPROF:BOOL=${SUPPORT_JEPROF}"
    "-DBUILD_WITH_URMA:BOOL=${BUILD_WITH_URMA}"
    "-DBUILD_WITH_RDMA:BOOL=${BUILD_WITH_RDMA}"
  )

  if [[ "${BUILD_WITH_URMA}" == "on" ]]; then
    cmake_options+=(
      "-DDOWNLOAD_UB:BOOL=${DOWNLOAD_UB}"
      "-DUB_URL:STRING=${UB_URL}"
      "-DUB_SHA256:STRING=${UB_SHA256}"
    )
  fi

  if is_on "${PACKAGE_PYTHON}" && [ -n "${PYTHON_ROOT_DIR}" ]; then
    echo -e "-- Specify python root path: ${PYTHON_ROOT_DIR}"
    cmake_options=("${cmake_options[@]}" "-DPython3_ROOT_DIR:PATH=${PYTHON_ROOT_DIR}")
  fi
  if is_on "${BUILD_WITH_NINJA}"; then
    cmake_options=("-G" "Ninja" "${cmake_options[@]}")
  else
    cmake_options=("-G" "Unix Makefiles" "${cmake_options[@]}")
  fi
  cmake_options=("${cmake_options[@]}" "${ADDITIONAL_CMAKE_OPTIONS[@]}")

  local baseTime_s
  baseTime_s=$(date +%s)
  cmake_version=$(cmake --version | head -n 1 | cut -d ' ' -f 3)
  cmake_version_release=3.28

  if [ -z "$cmake_version" ]; then
    echo "Unable to retrieve the CMake version"
    exit 1
  fi

  if !(version_lt "${cmake_version}" ${cmake_version_release}); then
    echo "CMake version: ${cmake_version} greater than or equal to ${cmake_version_release}"
    bash "${DATASYSTEM_DIR}/scripts/build_thirdparty.sh" "${BUILD_DIR}" "${BUILD_THREAD_NUM}" "${cmake_options[@]}"
  else
    echo " CMake version: ${cmake_version} less than ${cmake_version_release}"
  fi

  cmake "${cmake_options[@]}" || go_die "-- build datasystem CMake project failed!"
  cmake --build "${BUILD_DIR}" -j "${BUILD_THREAD_NUM}" || go_die "-- datasystem cmake build failed!"
  cmake --install "${BUILD_DIR}" || go_die "-- datasystem cmake install failed!"
  echo -e "---- [TIMER] Build source: $(($(date +%s)-$baseTime_s)) seconds"

  # erase symbol table if need.
  if is_on "${ENABLE_STRIP}"; then
    if [[ "${BUILD_TYPE}" = "Debug" ]]; then
      echo -e "WARNING: Build in debug mode and use strip tool to erase symbol table, it could be a problem when you use gdb."
    fi
    strip_symbols "${INSTALL_DIR}/datasystem/sdk/cpp/lib" "${INSTALL_DIR}/datasystem/sdk/DATASYSTEM_SYM"
    strip_symbols "${INSTALL_DIR}/cpp/lib" "${INSTALL_DIR}/cpp/DATASYSTEM_SYM"
    if is_on "${BUILD_HETERO}"; then
      IF_SYM_FILE="${BUILD_DIR}/src/datasystem/common/device/ascend/plugin/libacl_plugin.so.sym"
      if [ -f "${IF_SYM_FILE}" ]; then
        cp "${IF_SYM_FILE}" "${INSTALL_DIR}/datasystem/sdk/DATASYSTEM_SYM"
      else
        echo "Notice: libacl_plugin.so.sym not found, skipping (Normal for GPU build)."
      fi
    fi
    if is_on "${PACKAGE_JAVA}"; then
      cp ${BUILD_DIR}/java/jni_lib/*.sym "${INSTALL_DIR}/datasystem/sdk/DATASYSTEM_SYM"
    fi
    if is_on "${PACKAGE_PYTHON}"; then
      cp ${BUILD_DIR}/python_lib/*.sym "${INSTALL_DIR}/datasystem/sdk/DATASYSTEM_SYM"
    fi
    strip_symbols "${INSTALL_DIR}/datasystem/service" "${INSTALL_DIR}/datasystem/service/DATASYSTEM_SYM"
    strip_symbols "${INSTALL_DIR}/datasystem/service/lib" "${INSTALL_DIR}/datasystem/service/DATASYSTEM_SYM"
  fi

  # build example if -t is build or run
  if is_on "${BUILD_TESTCASE}"; then
    baseTime_s=$(date +%s)
    build_example
    echo -e "---- [TIMER] Build example: $(($(date +%s)-$baseTime_s)) seconds"
  fi

  # package golang client
  if is_on "${PACKAGE_GO}"; then
    baseTime_s=$(date +%s)
    echo -e "---- start to package golang client!"
    build_golang_client
    echo -e "---- [TIMER] Build go client: $(($(date +%s)-$baseTime_s)) seconds"
  fi

  # create tarball
  cd "${INSTALL_DIR}"
  tar --remove-files -zcf yr-datasystem-v$(cat "${BASE_DIR}/VERSION").tar.gz datasystem
  cd -
  echo -e "-- build datasystem success!"
}

function gen_html_coverage_report() {
  echo -e "---- generating coverage report, please wait a moments..."
  local cov_report_dir="${DATASYSTEM_DIR}/coverage_report"
  [[ -d "${cov_report_dir}" ]] && rm -rf "${cov_report_dir}"
  mkdir -p "${cov_report_dir}/.info"

  find ./ -type f | grep ".*\.gcda" | xargs dirname | sort -u | xargs realpath >"${cov_report_dir}/.info.txt"

  local cnt=0
  while IFS= read -r dir; do
    lcov --rc lcov_branch_coverage=1 -c -d "${dir}" -o "${cov_report_dir}/.info/${cnt}.info" 1>/dev/null || go_die "---- generate coverage report failed!" 0
    cnt=$((cnt + 1))
  done <"${cov_report_dir}/.info.txt"

  local files
  files=$(ls "${cov_report_dir}/.info")
  local cmd="lcov "
  for file in ${files}; do
    cmd="${cmd} -a ${cov_report_dir}/.info/${file}"
  done
  cmd="${cmd} -o ${cov_report_dir}/.info/raw_cov.info 1>/dev/null"
  eval "${cmd}" || go_die "---- generate coverage report failed!" 0

  lcov --extract "${cov_report_dir}/.info/raw_cov.info" "*src/datasystem/*" -o "${cov_report_dir}/.info/half_baked_cov.info" 1>/dev/null || go_die "---- generate coverage report failed!" 0
  lcov --remove "${cov_report_dir}/.info/half_baked_cov.info" "*/protos/*" -o "${cov_report_dir}/.info/well_done_cov.info" 1>/dev/null || go_die "---- generate coverage report failed!" 0

  genhtml -t "datasystem" -o "${cov_report_dir}" "${cov_report_dir}/.info/well_done_cov.info" 1>/dev/null || go_die "---- generate coverage report failed!"
  rm -rf "${cov_report_dir}/.info" "${cov_report_dir}/.info.txt"
  echo -e "---- generate coverage report done, saved in ${cov_report_dir}"
}

function build_example() {
  echo -e "---- building example..."
  local example_build_dir="${DATASYSTEM_DIR}/example/cpp/build"
  [[ "${BUILD_INCREMENT}" == "off" && -d "${example_build_dir}" ]] && rm -rf "${example_build_dir}"
  mkdir -p "${example_build_dir}" && cd "${example_build_dir}"

  cmake "${DATASYSTEM_DIR}/example/cpp" || go_die "---- build example CMake project failed!"
  make || go_die "---- example make failed!"

  echo -e "---- build example success!"
}

function build_golang_client() {
  echo -e "---- Start building Golang client..."
  bash "${DATASYSTEM_DIR}/scripts/package_go_sdk.sh" "${BUILD_DIR}" "$INSTALL_DIR" || go_die "Build Golang client failed!"
  echo -e "---- Build Golang client success!"
}

function run_example() {
  if [[ "${RUN_TESTS}" = "run" || "${RUN_TESTS}" = "run_except_cpp" || "${RUN_TESTS}" = "run_cases" || "${RUN_TESTS}" = "run_example" ]]; then
    local baseTime_s
    baseTime_s=$(date +%s)
    echo -e "---- Start Smoke Testing..."

    cd "${INSTALL_DIR}"
    tar -zxf yr-datasystem-v$(cat "${BASE_DIR}/VERSION").tar.gz
    cd -

    local old_ld_path=$LD_LIBRARY_PATH
    local new_ld_path=$(echo $old_ld_path | tr ':' '\n' | grep -v "output/datasystem/service" | grep -v "output/datasystem/sdk" | tr '\n' ':')
    export LD_LIBRARY_PATH=$new_ld_path
    echo -e "---- Sanitize LD_LIBRARY_PATH from ${old_ld_path} to ${new_ld_path}"

    bash "${DATASYSTEM_DIR}/example/run-example.sh" "${PACKAGE_GO}" ||
      (remove_running_pids && go_die "---- Smoke Testing failed!")
    echo -e "---- Smoke Testing success!"
    echo -e "---- [TIMER] Run example: $(($(date +%s)-$baseTime_s)) seconds"

    rm -rf "${INSTALL_DIR}/datasystem/service" "${INSTALL_DIR}/datasystem/sdk"
  fi
}

function run_ut() {
  cd "${BUILD_DIR}"
  echo -e "---- running datasystem testcases..."

  if [[ "${RUN_TESTS}" = "run_java" ]]; then
    LLT_LABELS="java"
  elif [[ "${RUN_TESTS}" = "run_cpp" ]]; then
    LLT_LABELS_EXCLUDE="java"
  fi

  local baseTime_s
  baseTime_s=$(date +%s)
  ctest --timeout "${LLT_TIMEOUT_S}" --schedule-random --parallel "${TEST_PARALLEL_JOBS}" \
        --label-regex "${LLT_LABELS}" --label-exclude "${LLT_LABELS_EXCLUDE}" ||
    ctest --rerun-failed --timeout "${LLT_TIMEOUT_S}" --parallel "$((TEST_PARALLEL_JOBS/2))" --output-on-failure ||
    ctest --rerun-failed --timeout "${LLT_TIMEOUT_S}" --output-on-failure ||
    go_die "---- run datasystem testcases failed!"
  echo -e "---- run datasystem testcases success!"
  echo -e "---- [TIMER] Run ctest: $(($(date +%s)-$baseTime_s)) seconds"

  if is_on "${GEN_HTML_COVERAGE}"; then
    gen_html_coverage_report
  fi
}

function run_ut_python() {
  echo -e "---- running python testcases..."
  local python_test_dir="${DATASYSTEM_DIR}/tests/python"
  cd ${python_test_dir}
  python3 -m unittest || (remove_running_pids && go_die "---- run datasystem testcases failed!")
  echo -e "---- run datasystem python testcases success!"
}

function run_manual_ut() {
  if [[ "${RUN_TESTS}" = "run" || "${RUN_TESTS}" = "run_except_cpp" || "${RUN_TESTS}" = "run_cases" || "${RUN_TESTS}" = "run_python" ]]; then
    if is_on "${PACKAGE_PYTHON}"; then
      if is_on "${BUILD_HETERO}"; then
        export BUILD_HETERO="on"
      fi

      cd "${INSTALL_DIR}"
      tar -zxf yr-datasystem-v$(cat "${BASE_DIR}/VERSION").tar.gz
      cd -

      local baseTime_s
      baseTime_s=$(date +%s)
      if [[ "${RUN_TESTS}" = "run_python" ]]; then
        python3 -m pip install ${INSTALL_DIR}/openyuanrong_datasystem-*.whl --force-reinstall
      fi
      start_all "${BUILD_DIR}" "${INSTALL_DIR}"
      run_ut_python
      stop_all "${INSTALL_DIR}"
      echo -e "---- [TIMER] Run python llt: $(($(date +%s)-$baseTime_s)) seconds"

      rm -rf "${INSTALL_DIR}/datasystem/service" "${INSTALL_DIR}/datasystem/sdk"
    fi
  fi
}

function run_automated_ut() {
  if [[ "${RUN_TESTS}" = "run" || "${RUN_TESTS}" = "run_cases" || "${RUN_TESTS}" = "run_cpp" || "${RUN_TESTS}" = "run_java" ]]; then
    run_ut
  fi
}

function run_cmake_testcases() {
  run_example
  run_manual_ut
  run_automated_ut
}
