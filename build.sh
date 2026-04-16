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
Usage: bash build.sh [-h] [-r] [-d] [-b cmake|bazel] [-c off/on/html] [-t off|build|run] [-s on|off] [-j <thread_num>]
                     [-p on|off] [-S address|thread|undefined|off] [-o <install dir>] [-u <thread_num>]
                     [-B <build_dir>] [-J on|off] [-P on/off] [-G on/off] [-v <version>] [-X on/off] [-e on/off]
                     [-R on/off] [-O on/off] [-I <observability install dir>] [-M off/on] [-T on/off]
                     [-A on/off] [-C on/off] [-l <llt_label>] [-i on/off] [-n on/off] [-x on/off]

Options:
    -h Output this help and exit.
    -r Release mode, default mode.
    -d Debug mode.
    -b Select build system: cmake or bazel, default: cmake.
    -B Specifies the directory where the compilation process content is stored. If this parameter is specified,
       an empty directory is recommended to prevent other files from being deleted, default is './build'.
    -o Set the output path of the compilation result. If this parameter is specified, an empty directory
       is recommended to prevent other files from being deleted, default is './output'.
    -j Set the number of threads for compiling source code and compiling open source software, default is 8.
    -i Incremental compilation, choose from: on/off, default: off. (cmake only)
    -n Use Ninja to speed up compilation, the default is to compile using Unix Makefiles, choose from: on/off, default: off. (cmake only)

    For multiple programming languages:
    -J Build Java api client, choose from: on/off, default: off.
    -P Build Python sdk, choose from: on/off/<python_root_dir>, default: on. Accepts on (default) to auto-detect a Python
       version from the system environment, off to disable the build, or a <python_root_dir> path to prioritize the Python
       at that specified location for search.
    -G Build Go sdk, choose from: on/off, default: off.
    -v Specified version.The default value is using 'cat ./VERSION'.
    -X Compiles the code for heterogeneous objects. The options are on and off. The default value is 'on'.
    -T Compiles the code for os pipeline h2d feature in kvclient. The options are on and off. The default value is 'off'.

    For communication layer
    -M Build with URMA framework in addition to ZMQ, choose from on/off, default: off.
    -D Download UB package that is needed for URMA, choose from on/off. When on, can also provide UB download options, default: off.
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
    -A Build with UCX framework to support RDMA transport, choose from on/off, default: off.
       Notes for compiling and running with RDMA support:
       1. An RDMA-capable NIC and its driver must be installed and properly configured.
       2. The RDMA userspace libraries (libibverbs, librdmacm) from rdma-core must be installed.

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
    -t Compiling or running testcases, default off. Choose from: off/build/run/run_except_cpp/run_cases/run_cpp/run_java/run_python/run_example.
       Field 'off' indicates that testcases are not compiled and executed.
       Field not 'off', The 'tools' directory will be generated under the compilation result path
             curve_keygen (generate zmq public and private keys) and
             hashring_parser (parse hashringPb) will be generated under the 'tools' directory.
       Field 'build' indicates that testcases are compiled but not run.
       Field 'run' indicates that testcases are compiled and run.
       Field 'run_except_cpp' indicates that testcases are run, except cpp ut.
       Field 'run_cases' indicates that only testcases are run.
       Field 'run_cpp' indicates that only cpp testcases are run.
       Field 'run_java' indicates that only java testcases are run.
       Field 'run_python' indicates that only python testcases are run.
       Field 'run_example' indicates that only example testcases are run.
    -l The label of testcases, effective when '-t run/run_cases/run_cpp', default: level*.
       Regular expression: object*/ut*/st*/level*, example: object/ut/st/level0/level1.
    -m The timeout period of testcases, the unit is second, default: 40.

Environment:
1) DS_JEMALLOC_LG_PAGE: Explicitly sets the page size used by the jemalloc. page size=2^\${DS_JEMALLOC_LG_PAGE} bytes.
    When this variable is omitted, jemalloc infers the system's page size at build time from the build environment
    (e.g., via sysconf(_SC_PAGESIZE) or equivalent). Only set DS_JEMALLOC_LG_PAGE if the runtime system's page size
    differs from the one detected at build time and you must override the value.
2) DS_OPENSOURCE_DIR: Specifies a directory to cache the opensource compilation result.
    Cache the compilation result to speed up the compilation. Default: /tmp/{sha256(pwd)}/
3) DS_VERSION: Customize a version number during compilation.
4) DS_PACKAGE: If specified, third-party libs for the path provided by this variable will be compiled, for
    version build only.
5) CTEST_OUTPUT_ON_FAILURE: Boolean environment variable that controls if the sdk output should be logged for
    failed tests. Set the value to 1, True, or ON to enable output on failure.

Example:
1) Compile a release version and export compilation result to the output directory.
  $ bash build.sh
2) Compile using bazel build system.
  $ bash build.sh -b bazel
3) Compile a debug version with bazel.
  $ bash build.sh -b bazel -d
"

readonly BASE_DIR=$(dirname "$(readlink -f "$0")")
readonly DATASYSTEM_DIR="${BASE_DIR}"

export PATH=$PATH:${BASE_DIR}/scripts/modules

# Source common functions
source "${DATASYSTEM_DIR}/scripts/build_common.sh"

# Source llt_util.sh for test utilities (start_all, stop_all, etc.)
. llt_util.sh

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
  while getopts 'hdro:j:t:u:c:e:p:s:l:i:n:A:B:F:S:J:G:P:v:X:R:D:C:M:x:m:T:b:' OPT; do
    case "${OPT}" in
    d)
      BUILD_TYPE="Debug"
      ;;
    r)
      BUILD_TYPE="Release"
      ;;
    b)
      if [[ "${OPTARG}" != "cmake" && "${OPTARG}" != "bazel" ]]; then
        echo -e "Invalid value ${OPTARG} for option -b, choose from cmake or bazel"
        echo -e "${USAGE}"
        exit 1
      fi
      BUILD_SYSTEM="${OPTARG}"
      ;;
    o)
      INSTALL_DIR=$(realpath -m "${OPTARG}")
      ;;
    B)
      BUILD_DIR=$(realpath -m "${OPTARG}")
      ;;
    t)
      if [[ "X${OPTARG}" != "Xoff" && "X${OPTARG}" != "Xbuild" && "X${OPTARG}" != "Xrun" && "X${OPTARG}" != "Xrun_except_cpp" &&
        "X${OPTARG}" != "Xrun_cases" && "X${OPTARG}" != "Xrun_cpp" && "X${OPTARG}" != "Xrun_java" && "X${OPTARG}" != "Xrun_python" &&
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
      TRANSFER_ENGINE_BUILD_THREAD_NUM="${OPTARG}"
      if [ ${OPTARG} -gt $logical_cpu_cout ]; then
        echo -e "-- [Warning] The -j $BUILD_THREAD_NUM is over the max logical cpu count($logical_cpu_cout), set BUILD_THREAD_NUM & TRANSFER_ENGINE_BUILD_THREAD_NUM to ($logical_cpu_cout)."
        BUILD_THREAD_NUM="$logical_cpu_cout"
        TRANSFER_ENGINE_BUILD_THREAD_NUM="$logical_cpu_cout"
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
      if [ "${OPTARG}" == "off" ] || [ "${OPTARG}" == "on" ]; then
        PACKAGE_PYTHON="${OPTARG}"
        PYTHON_ROOT_DIR=""
      elif [[ -d "${OPTARG}" ]]; then
        PACKAGE_PYTHON="on"
        PYTHON_ROOT_DIR="${OPTARG}"
      else
        echo -e "Invalid value ${OPTARG} for option -P, choose from off, on or <python_root_dir>. If specifying <python_root_dir>, ensure the path exists."
        echo -e "${USAGE}"
        exit 1
      fi
      ;;
    G)
      check_on_off "${OPTARG}" G
      PACKAGE_GO=${OPTARG}
      ;;
    p)
      check_on_off "${OPTARG}" p
      ENABLE_PERF="${OPTARG}"
      ;;
    J)
      check_on_off "${OPTARG}" J
      PACKAGE_JAVA="${OPTARG}"
      ;;
    M)
      check_on_off "${OPTARG}" M
      BUILD_WITH_URMA="${OPTARG}"
      ;;

    D)
      parse_ub_download_options ${OPTARG}
      ;;
    A)
      check_on_off "${OPTARG}" A
      BUILD_WITH_RDMA="${OPTARG}"
      ;;
    h)
      echo -e "${USAGE}"
      exit 0
      ;;
    v)
      echo "${OPTARG}" > "${BASE_DIR}/VERSION"
      ;;
    X)
      check_on_off "${OPTARG}" X
      BUILD_HETERO="${OPTARG}"
      ;;
    T)
      check_on_off "${OPTARG}" T
      BUILD_PIPLN_H2D="${OPTARG}"
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

  # Auto-detect Ascend environment for BUILD_HETERO
  if is_on "${BUILD_HETERO}" && ! check_ascend_env; then
    echo -e "-- [Warning] Ascend toolkit not found in the environment (checked: \$ASCEND_HOME_PATH, \$ASCEND_CUSTOM_PATH, /usr/local/Ascend/ascend-toolkit/latest). Setting -X to off. The build will not include Ascend capabilities."
    BUILD_HETERO="off"
  fi

  echo -e "-- Build system: ${BUILD_SYSTEM}"

  # Dispatch to build system
  if [[ "${BUILD_SYSTEM}" == "cmake" ]]; then
    source "${DATASYSTEM_DIR}/scripts/build_cmake.sh"
    if [[ "${RUN_TESTS}" == "off" || "${RUN_TESTS}" == "build" || "${RUN_TESTS}" == "run" ]]; then
      build_datasystem_cmake
    fi
    run_cmake_testcases
  else
    source "${DATASYSTEM_DIR}/scripts/build_bazel.sh"
    if [[ "${RUN_TESTS}" == "off" || "${RUN_TESTS}" == "build" || "${RUN_TESTS}" == "run" ]]; then
      build_datasystem_bazel
    fi
    run_bazel_testcases
  fi

  end_time_s=$(date +%s)
  sumTime=$(($end_time_s - $start_time_s))
  echo -e "---- [TIMER] Total use time: $sumTime seconds"

  exit 0
}

main "$@"
