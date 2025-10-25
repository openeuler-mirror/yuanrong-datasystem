#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
source /etc/profile.d/*.sh

readonly USAGE="
Usage: bash build.sh [-h] [-r] [-d] [-c off/on/html] [-t off|build|run] [-s on|off] [-j <thread_num>]
                     [-p on|off] [-S address|thread|undefined|off] [-o <install dir>] [-u <thread_num>]
                     [-B <build_dir>] [-P on/off] [-X on/off] [-T <thirdparty_versions>]
                     [-R on/off] [-D \"on <ub_url> <ub_sha256>\"/off] [-l <llt_label>] [-i on/off] [-n on/off]
                     [-x on/off]

Options:
    -h Output this help and exit.
    -r Release mode, default mode.
    -d Debug mode.
    -B Specifies the directory where the compilation process content is stored. If this parameter is specified,
       an empty directory is recommended to prevent other files from being deleted, default is './build'.
    -o Set the output path of the compilation result. If this parameter is specified, an empty directory
       is recommended to prevent other files from being deleted, default is './output'.
    -j Set the number of threads for compiling source code and compiling open source software, default is 8.
    -T Set the third party versions if the default version need to be changed, default is ''.
       Format: '<lib1:version1>;<lib2:version2>...', example: 'protobuf:3.13.0;'
    -i Incremental compilation, choose from: on/off, default: off.
    -n Use Ninja to speed up compilation, the default is to compile using Unix Makefiles, choose from: on/off, default: off.

    For multiple programming languages:
    -P Build Python sdk, choose from: on/off, default: on.
    -X Compiles the code for heterogeneous objects. The options are on and off. The default value is 'on'.

    For communication layer
    -M Build with URMA framework in addition to ZMQ, choose from on/off, default: off.
    -D Download UB package that is needed for URMA, choose from on/off. When on, can also provide UB download options, default: on.
       Notes to compile and run with URMA:
       1. The default packages are for EulerOS-V2R10 environment
       2. The downloaded rpm packages and the kernel modules need to be installed before run
          Command to remove an rpm package: sudo rpm -e --nodeps <package name>
          Command to install rpm packages: sudo rpm -ivh --force --nodeps *.rpm
          Command to remove kernel modules: sudo rmmod uburma uboip uboib ubcore
          Command to install kernel modules: sudo modprobe uburma;sudo modprobe uboib;sudo modprobe uboip;sudo modprobe ubcore
       3. OFED environment need to be configured for RDMA IB mode. IP mode can run without OFED.
          Command to set up OFED environment:
            ./mlnxofedinstall --without-depcheck --without-fw-update --force
            /etc/init.d/openibd restart

    For debug code:
    -p Generate perf point logs, choose from: on/off, default: off.
    -s Use strip tool to export the symbol table as sym and erase symbols based on it, choose from: on/off,
       default: on.
    -S Use Google Sanitizers tools to detect bugs. Choose from off/address/thread/undefined,
       if set the value to 'address' enable AddressSanitizer,
       if set the value to 'thread' enable ThreadSanitizer,
       if set the value to 'undefined' enable UndefinedBehaviorSanitizer,
       default off.
    -x Support jemalloc memory profiling.

    For testcase:
    -c Build coverage, choose from: off/on/html, default: off.
    -u CTest run the testcases in parallel using the given number of jobs, default is 8.
    -t Compiling or running testcases, default off. Choose from: off/build/run/run_except_cpp/run_cases/run_cpp/run_python/run_example.
       Field 'off' indicates that testcases are not compiled and executed.
       Field not 'off', The 'tools' directory will be generated under the compilation result path
             curve_keygen (generate zmq public and private keys) and 
             hashring_parser (parse hashringPb) will be generated under the 'tools' directory.
       Field 'build' indicates that testcases are compiled but not run.
       Field 'run' indicates that testcases are compiled and run.
       Field 'run_except_cpp' indicates that testcases are run, except cpp ut.
       Field 'run_cases' indicates that only testcases are run.
       Field 'run_cpp' indicates that only cpp testcases are run.
       Field 'run_python' indicates that only python testcases are run.
       Field 'run_example' indicates that only example testcases are run.
    -l The label of testcases, effective when '-t run/run_cases/run_cpp', default: level*.
       Regular expression: object*/ut*/st*/level*, example: object/ut/st/level0/level1.
    -m The timeout period of testcases, the unit is second, default: 40.

Environment:
1) DS_OPENSOURCE_DIR: Specifies a directory to cache the opensource compilation result.
    Cache the compilation result to speed up the compilation. Default: /tmp/{sha256(pwd)}/
2) DS_VERSION: Customize a version number during compilation.
3) DS_PACKAGE: If specified, third-party libs for the path provided by this variable will be compiled, for
    version build only.
4) CTEST_OUTPUT_ON_FAILURE: Boolean environment variable that controls if the sdk output should be logged for
    failed tests. Set the value to 1, True, or ON to enable output on failure.

Example:
1) Compile a release version and export compilation result to the output directory.
  $ bash build.sh -r -o ./output
"

readonly BASE_DIR=$(dirname "$(readlink -f "$0")")
readonly DATASYSTEM_DIR="${BASE_DIR}"

export PATH=$PATH:${BASE_DIR}/scripts/modules

. llt_util.sh

function init_default_opts() {
  export BUILD_TYPE="Release"
  export INSTALL_DIR="${DATASYSTEM_DIR}/output"
  export BUILD_DIR="${DATASYSTEM_DIR}/build"
  export BUILD_THREAD_NUM=8
  export ADDITIONAL_CMAKE_OPTIONS=()
  export BUILD_INCREMENT="off"
  export BUILD_WITH_NINJA="off"

  # For communication layer
  export BUILD_WITH_URMA="off"
  export DOWNLOAD_UB="off"
  export UB_URL=""
  export UB_SHA256=""

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

  # For packaging mutil language
  export PACKAGE_PYTHON="on"
  export PYTHON_VERSION=""

  # Whether to build device object.
  export BUILD_HETERO="on"

  # Whether support jemalloc memory profiling
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

function parse_thirdparty_versions() {
  local versions_str="$1"
  IFS=';' read -ra lib_version_pairs <<<"$versions_str"
  for ((i = 0; i < ${#lib_version_pairs[@]}; i++)); do
    local str=${lib_version_pairs[i]}
    IFS=':' read -ra lib_version_pair <<<"$str"
    if [[ ${#lib_version_pair[@]} -eq 2 ]]; then
      local lib_name=${lib_version_pair[0]}
      local version=${lib_version_pair[1]}
      ADDITIONAL_CMAKE_OPTIONS[${#ADDITIONAL_CMAKE_OPTIONS[@]}]="-D${lib_name}_VERSION=${version}"
    else
      go_die "-- Error thirdparty specified: ${versions_str}"
    fi
  done
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
  # Clean master and worker processes because the processes continue to run if the test exits abnormally.
  echo -e "-- Cleaning master processes and worker processes..."
  ps -ef | grep -E "worker|master" | grep "${DATASYSTEM_DIR}" | grep -v grep | awk '{print $2}' | xargs kill -15
}

function gen_html_coverage_report() {
  echo -e "---- generating coverage report, please wait a moments..."
  local cov_report_dir="${DATASYSTEM_DIR}/coverage_report"
  [[ -d "${cov_report_dir}" ]] && rm -rf "${cov_report_dir}"
  mkdir -p "${cov_report_dir}/.info"

  # find all directories with the suffix ".gcda"
  find ./ -type f | grep ".*\.gcda" | xargs dirname | sort -u | xargs realpath >"${cov_report_dir}/.info.txt"

  # generate coverage info file.
  local cnt=0
  while IFS= read -r dir; do
    lcov --rc lcov_branch_coverage=1 -c -d "${dir}" -o "${cov_report_dir}/.info/${cnt}.info" 1>/dev/null || go_die "---- generate coverage report failed!" 0
    cnt=$((cnt + 1))
  done <"${cov_report_dir}/.info.txt"

  # aggregate all Info files into one file.
  local files
  files=$(ls "${cov_report_dir}/.info")
  local cmd="lcov "
  for file in ${files}; do
    cmd="${cmd} -a ${cov_report_dir}/.info/${file}"
  done
  cmd="${cmd} -o ${cov_report_dir}/.info/raw_cov.info 1>/dev/null"
  eval "${cmd}" || go_die "---- generate coverage report failed!" 0

  # picking out the parts we care about
  lcov --extract "${cov_report_dir}/.info/raw_cov.info" "*src/datasystem/*" -o "${cov_report_dir}/.info/half_baked_cov.info" 1>/dev/null || go_die "---- generate coverage report failed!" 0
  lcov --remove "${cov_report_dir}/.info/half_baked_cov.info" "*/protos/*" -o "${cov_report_dir}/.info/well_done_cov.info" 1>/dev/null || go_die "---- generate coverage report failed!" 0

  # generate html report
  genhtml -t "datasystem" -o "${cov_report_dir}" "${cov_report_dir}/.info/well_done_cov.info" 1>/dev/null || go_die "---- generate coverage report failed!"
  rm -rf "${cov_report_dir}/.info" "${cov_report_dir}/.info.txt"
  echo -e "---- generate coverage report done, saved in ${cov_report_dir}"
}

function build_example() {
  echo -e "---- building example..."
  local example_build_dir="${DATASYSTEM_DIR}/example/build"
  # clean and create build dir.
  [[ "${BUILD_INCREMENT}" == "off" && -d "${example_build_dir}" ]] && rm -rf "${example_build_dir}"
  mkdir -p "${example_build_dir}" && cd "${example_build_dir}"

  local prefix_path
  prefix_path=${INSTALL_DIR}/sdk/cpp

  cmake "${DATASYSTEM_DIR}/example" \
    -DCMAKE_PREFIX_PATH="${prefix_path}" \
    -DBUILD_HETERO="${BUILD_HETERO}" \
    -DBUILD_WITH_URMA="${BUILD_WITH_URMA}" \
    -DENABLE_PERF=${ENABLE_PERF} || go_die "---- build example CMake project failed!"
  make || go_die "---- example make failed!"

  echo -e "---- build example success!"
}

function run_example() {
  if [[ "${RUN_TESTS}" = "run" || "${RUN_TESTS}" = "run_except_cpp" || "${RUN_TESTS}" = "run_cases" || "${RUN_TESTS}" = "run_example" ]]; then
    local baseTime_s
    baseTime_s=$(date +%s)
    echo -e "---- Start Smoke Testing..."

    # unpackage the tar file.
    cd "${INSTALL_DIR}"
    tar -zxf yr-datasystem-v$(cat "${BASE_DIR}/VERSION").tar.gz
    cd -

    # sanitize path
    local old_ld_path=$LD_LIBRARY_PATH
    local new_ld_path=$(echo $old_ld_path | tr ':' '\n' | grep -v "output/service" | grep -v "output/sdk" | tr '\n' ':')
    export LD_LIBRARY_PATH=$new_ld_path
    echo -e "---- Sanitize LD_LIBRARY_PATH from ${old_ld_path} to ${new_ld_path}"

    python3 -m pip install ${INSTALL_DIR}/openyuanrong_datasystem-*.whl --force-reinstall
    bash "${DATASYSTEM_DIR}/example/run-example.sh" "${BUILD_HETERO}" "${ENABLE_PERF}" ||
      (remove_running_pids && go_die "---- Smoke Testing failed!")
    echo -e "---- Smoke Testing success!"
    echo -e "---- [TIMER] Run example: $(($(date +%s)-$baseTime_s)) seconds"

    # clean unpackage files.
    rm -rf "${INSTALL_DIR}/service" "${INSTALL_DIR}/sdk"
  fi
}

function run_ut() {
  cd "${BUILD_DIR}"
  # run testcases if RUN_TESTS is run.
  echo -e "---- running datasystem testcases..."

  local baseTime_s
  baseTime_s=$(date +%s)
  # Run all testcases in parallel and don't print the log out.
  # In general, people would not see any log messages at this time.
  # The timeout period is 100 seconds.
  ctest --timeout "${LLT_TIMEOUT_S}" --schedule-random --parallel "${TEST_PARALLEL_JOBS}" \
        --label-regex "${LLT_LABELS}" --label-exclude "${LLT_LABELS_EXCLUDE}" ||
    # We will give the failed test cases two more chances to redeem
    # themselves, but just run with single process to ensure the
    # cases that fail occasionally can succeed. This time we will
    # print the client log if user set `CTEST_OUTPUT_ON_FAILURE` env.
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

function clean_dirs() {
  echo -e "cleaning..."
  [[ -d "${DATASYSTEM_DIR}/build" ]] && rm -rf "${DATASYSTEM_DIR}/build"
  [[ -d "${INSTALL_DIR}" ]] && rm -rf "${INSTALL_DIR}"
  [[ -d "${DATASYSTEM_DIR}/third_party/build" ]] && rm -rf "${DATASYSTEM_DIR}/third_party/build"
  [[ -d "${DATASYSTEM_DIR}/example/build" ]] && rm -rf "${DATASYSTEM_DIR}/example/build"
  echo -e "done!"
}

function strip_symbols() {
  local src_dir="$1"
  local dest_dir="$2"
  if [[ ! -d "${dest_dir}" ]]; then
    mkdir -p "${dest_dir}"
  fi

  for file in ${src_dir}/*; do
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

function run_manual_ut()
{
  if [[ "${RUN_TESTS}" = "run" || "${RUN_TESTS}" = "run_except_cpp" || "${RUN_TESTS}" = "run_cases" || "${RUN_TESTS}" = "run_python" ]]; then
    if is_on "${PACKAGE_PYTHON}"; then
      # Export "BUILD_HETERO", which is used to determine whether to execute heterogeneous related python test cases.
      if is_on "${BUILD_HETERO}"; then
        export BUILD_HETERO="on"
      fi

      # unpackage the tar file.
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

      # clean unpackage files.
      rm -rf "${INSTALL_DIR}/service" "${INSTALL_DIR}/sdk"
    fi
  fi
}

function run_automated_ut()
{
  if [[ "${RUN_TESTS}" = "run" || "${RUN_TESTS}" = "run_cases" || "${RUN_TESTS}" = "run_cpp" ]]; then
      run_ut
  fi
}

function run_testcases()
{
  # run smoke test
  run_example
  # run ut that require manually starting workers.
  run_manual_ut
  # run ut that automatically start workers
  run_automated_ut
}

function version_lt() 
{
    [ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ] && [ "$1" != "$2" ]
}

function build_datasystem()
{
  # clean and create build dir.
  [[ "${BUILD_INCREMENT}" == "off" && -d "${BUILD_DIR}" ]] && rm -rf "${BUILD_DIR}" && echo -e "--[Warning] Removed build folder ${BUILD_DIR}"
  mkdir -p "${BUILD_DIR}" && cd "${BUILD_DIR}"
  [[ "${BUILD_INCREMENT}" == "off" && -d "${INSTALL_DIR}" ]] && rm -rf "${INSTALL_DIR}" && echo -e "--[Warning] Removed output folder ${INSTALL_DIR}"
  mkdir -p "${INSTALL_DIR}"

  echo -e "-- building datasystem in ${BUILD_DIR}..."

  # # build datasystem framework.
  if [[ "${RUN_TESTS}" != "off" ]]; then
    BUILD_TESTCASE="on"
  fi

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
    "-DENABLE_STRIP:BOOL=${ENABLE_STRIP}"
    "-DBUILD_HETERO:BOOL=${BUILD_HETERO}"
    "-DBUILD_WITH_URMA:BOOL=${BUILD_WITH_URMA}"
    "-DDOWNLOAD_UB:BOOL=${DOWNLOAD_UB}"
    "-DUB_URL:STRING=${UB_URL}"
    "-DUB_SHA256:STRING=${UB_SHA256}"
    "-DSUPPORT_JEPROF:BOOL=${SUPPORT_JEPROF}"
    )
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
  cp "${DATASYSTEM_DIR}/LOG_README" "${INSTALL_DIR}/service/"
  echo -e "---- [TIMER] Build source: $(($(date +%s)-$baseTime_s)) seconds"

  # erase symbol table if need.
  if is_on "${ENABLE_STRIP}"; then
    if [[ "${BUILD_TYPE}" = "Debug" ]]; then
      echo -e "WARNING: Build in debug mode and use strip tool to erase symbol table, it could be a problem when you use gdb."
    fi
    strip_symbols "${INSTALL_DIR}/sdk/cpp/lib" "${INSTALL_DIR}/sdk/DATASYSTEM_SYM"
    if is_on "${BUILD_HETERO}"; then
      cp ${BUILD_DIR}/src/datasystem/common/device/ascend/plugin/libacl_plugin.so.sym "${INSTALL_DIR}/sdk/DATASYSTEM_SYM"
    fi
    if is_on "${PACKAGE_PYTHON}"; then
      cp ${BUILD_DIR}/python_lib/*.sym "${INSTALL_DIR}/sdk/DATASYSTEM_SYM"
    fi
    strip_symbols "${INSTALL_DIR}/service" "${INSTALL_DIR}/service/DATASYSTEM_SYM"
    strip_symbols "${INSTALL_DIR}/service/lib" "${INSTALL_DIR}/service/DATASYSTEM_SYM"
  fi

  # build example if -t is build or run
  if is_on "${BUILD_TESTCASE}"; then
    baseTime_s=$(date +%s)
    build_example
    echo -e "---- [TIMER] Build example: $(($(date +%s)-$baseTime_s)) seconds"
  fi

  # package 
  cd "${INSTALL_DIR}"
  tar --remove-files -zcf yr-datasystem-v$(cat "${BASE_DIR}/VERSION").tar.gz service sdk
  cd -
  echo -e "-- build datasystem success!"
}

function main() {
  if [[ x"$1" == "xclean" ]]; then
    clean_dirs
    exit 0
  fi
  init_default_opts
  set_datasystem_version
  local logical_cpu_cout
  logical_cpu_cout=$(cat /proc/cpuinfo | grep "processor" | wc -l)
  if [[ "${logical_cpu_cout}" == "0" ]]; then
    echo "Can't get logical cpu count, set to default 16"
    logical_cpu_cout=16
  fi
  while getopts 'hdro:j:t:u:c:e:p:s:l:i:n:B:F:S:P:T:X:R:D:C:M:x:m:' OPT; do
    case "${OPT}" in
    d)
      BUILD_TYPE="Debug"
      ;;
    r)
      BUILD_TYPE="Release"
      ;;
    o)
      INSTALL_DIR=$(realpath -m "${OPTARG}")
      ;;
    B)
      BUILD_DIR=$(realpath -m "${OPTARG}")
      ;;
    t)
      if [[ "X${OPTARG}" != "Xoff" && "X${OPTARG}" != "Xbuild" && "X${OPTARG}" != "Xrun" && "X${OPTARG}" != "Xrun_except_cpp" &&
        "X${OPTARG}" != "Xrun_cases" && "X${OPTARG}" != "Xrun_cpp" && "X${OPTARG}" != "Xrun_python" &&
        "X${OPTARG}" != "Xrun_example" ]]; then
        echo -e "Invalid value ${OPTARG} for option -t"
        echo -e "${USAGE}"
        exit 1
      fi
      RUN_TESTS="${OPTARG}"
      ;;
    j)
      check_number "${OPTARG}" j
      BUILD_THREAD_NUM="${OPTARG}"
      if [ ${OPTARG} -gt $logical_cpu_cout ]; then
        echo -e "-- [Warning] The -j $BUILD_THREAD_NUM is over the max logical cpu count($logical_cpu_cout), set BUILD_THREAD_NUM to ($logical_cpu_cout)."
        BUILD_THREAD_NUM="$logical_cpu_cout"
      fi
      ;;
    u)
      check_number "${OPTARG}" u
      TEST_PARALLEL_JOBS="${OPTARG}"
      if [ ${OPTARG} -gt $logical_cpu_cout ]; then
        echo -e "-- [Warning] The -u $TEST_PARALLEL_JOBS is over the max logical cpu count($logical_cpu_cout), set TEST_PARALLEL_JOBS to ($logical_cpu_cout)."
        TEST_PARALLEL_JOBS="$logical_cpu_cout"
      fi
      ;;
    c)
      if [ "${OPTARG}" == "off" ]; then
        echo -e "Do not compile coverage reports."
      elif [ "${OPTARG}" == "on" ]; then
        BUILD_COVERAGE="on"
      elif [ "${OPTARG}" == "html" ]; then
        BUILD_COVERAGE="on"
        GEN_HTML_COVERAGE="on"
      else
        echo -e "Invalid value ${OPTARG} for option -c, choose from off/on/html"
        echo -e "${USAGE}"
        exit 1
      fi
      ;;
    s)
      check_on_off "${OPTARG}" s
      ENABLE_STRIP=${OPTARG}
      ;;
    F)
      echo "Should not set -F on"
      ;;
    S)
      check_sanitizers "${OPTARG}" S
      USE_SANITIZER="${OPTARG}"
      ;;
    P)
      check_on_off "${OPTARG}" P
      PACKAGE_PYTHON=${OPTARG}
      ;;
    p)
      check_on_off "${OPTARG}" p
      ENABLE_PERF="${OPTARG}"
      ;;
    T)
      parse_thirdparty_versions "${OPTARG}"
      ;;
    M)
      check_on_off "${OPTARG}" M
      BUILD_WITH_URMA="${OPTARG}"
      ;;

    D)
      parse_ub_download_options ${OPTARG}
      ;;
    h)
      echo -e "${USAGE}"
      exit 0
      ;;
    X)
      check_on_off "${OPTARG}" X
      BUILD_HETERO="${OPTARG}"
      ;;
    l)
      check_labels "${OPTARG}" l
      LLT_LABELS=${OPTARG}
      ;;
    i)
      check_on_off "${OPTARG}" i
      BUILD_INCREMENT=${OPTARG}
      ;;
    n)
      check_on_off "${OPTARG}" n
      BUILD_WITH_NINJA=${OPTARG}
      ;;
    x)
      check_on_off "${OPTARG}" x
      SUPPORT_JEPROF=${OPTARG}
      ;;
    m)
      check_number "${OPTARG}" m
      LLT_TIMEOUT_S=${OPTARG}
      ;;
    ?)
      echo -e "${USAGE}"
      exit 1
      ;;
    esac
  done

  shift $(($OPTIND - 1))
  if [[ -n "$1" ]]; then
    echo -e "Invalid parameter $1"
    echo -e "${USAGE}"
    exit 1
  fi

  local start_time_s
  local end_time_s
  local sumTime
  start_time_s=$(date +%s)

  if [[ "${RUN_TESTS}" == "off" || "${RUN_TESTS}" == "build" || "${RUN_TESTS}" == "run" ]]; then
    build_datasystem
  fi

  run_testcases

  end_time_s=$(date +%s)
  sumTime=$(($end_time_s - $start_time_s))
  echo -e "---- [TIMER] Total use time: $sumTime seconds"

  exit 0
}

main "$@"
