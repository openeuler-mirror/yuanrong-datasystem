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

# Description: Supports distributed running of ut.
set -e

readonly USAGE="
Usage: bash distribute_run_ut.sh [-h] [-d <build dir>] [-n <node count>] [-i <ut index>] [-j <thread num>]
                                 [-o <output dir>]

Options:
    -h Output this help and exit.
    -d Directory of the build result after datasystem compilation.
    -n Indicates the number of UT splits for execution. Maximum value: 100
    -i Indicates the sequence number of the UT test case to be executed. The value starts from 0.
    -j Set the number of threads for compiling source code and compiling open source software, default is 8.
    -o Set the output path of the result.

Example:
1) Divide the UT into 10 parts and run the 7th test case.
  $ bash distribute_run_ut.sh -d /home/code/datasystem/build -n 10 -i 7 -j 8 -o ./output
"
readonly BASE_DIR=$(dirname "$(readlink -f "$0")")

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

function init_default_opts() {
  export BUILD_DIR
  BUILD_DIR=$(realpath -m "${BASE_DIR}/../build")

  export UT_COUNT=0
  export UT_INDEX=0
  export TEST_PARALLEL_JOBS=8

  OUTPUT_DIR=$(realpath -m "${BASE_DIR}/output")
  export OUTPUT_DIR
  export UT_INDEX_DIR
}

function split_ut() {
  UT_INDEX_DIR="${OUTPUT_DIR}/ut_index"
  "${BUILD_DIR}/tests/ut/ds_llt" --gtest_list_tests | grep '\.' > test_fixtures.txt
  local total_line
  total_line=$(awk 'END{print NR}' test_fixtures.txt)
  local avg_line=0
  if [ ${UT_COUNT} -ne 0 ]; then
    avg_line=$((total_line / UT_COUNT))
  fi

  avg_line=$((avg_line + 1))
  split -l ${avg_line} test_fixtures.txt -d -a 2 ut_index_
  mkdir -p "${UT_INDEX_DIR}"
  mv ut_index_* "${UT_INDEX_DIR}"
}

function run_ut() {
  echo -e "---- Start to run datasystem testcases ...!"
  local filepath="${UT_INDEX_DIR}/ut_index_${UT_INDEX}"
  if [ ! -f "${filepath}" ]; then
    echo -e "Invalid ut index ${UT_INDEX}, ${filepath} is not exist"
    exit 1
  fi

  sed -i 's/$/&*'/g "${UT_INDEX_DIR}/ut_index_${UT_INDEX}"
  sed -i 's/^/^&'/g "${UT_INDEX_DIR}/ut_index_${UT_INDEX}"
  local pattern
  pattern=$(cat "${UT_INDEX_DIR}/ut_index_${UT_INDEX}" | tr "\n" "|")
  pattern=${pattern::-1} # To remove the last |

  echo """---- fixtures count: $(wc "${filepath}" -l)"""
  echo "---- pattern is ${pattern}"
  cd "${BUILD_DIR}"

  # Run all testcases in parallel and don't print the log out.
  # In general, people would not see any log messages at this time.
  # The timeout period is 240 seconds.
  ctest --timeout 240 -R "${pattern}" --schedule-random --parallel "${TEST_PARALLEL_JOBS}" ||
    # We will give the failed test cases two more chances to redeem
    # themselves, but just run with single process to ensure the
    # cases that fail occasionally can succeed. This time we will
    # print the client log if user set `CTEST_OUTPUT_ON_FAILURE` env.
    ctest --rerun-failed --timeout 240 || ctest --rerun-failed --timeout 240 ||
    go_die "---- run datasystem testcases failed!"
  echo -e "---- run datasystem testcases success!"
}

function gen_coverage_info() {
  echo -e "---- generating coverage info, please wait a moments..."
  local cov_report_dir="${OUTPUT_DIR}/coverage_report"
  [[ -d "${cov_report_dir}" ]] && rm -rf "${cov_report_dir}"
  mkdir -p "${cov_report_dir}/info"

  cd "${BUILD_DIR}"

  # find all directories with the suffix ".gcda"
  find ./ -type f | grep ".*\.gcda" | xargs dirname | sort -u | xargs realpath >"${cov_report_dir}/gcda_dirs.txt"

  # generate coverage info file.
  echo -e "---- Start to convert the gcda file to the lcov info file"
  local start_time_s
  local end_time_s
  local sumTime
  start_time_s=$(date +%s)
  local cnt=0
  while IFS= read -r dir; do
    lcov --rc lcov_branch_coverage=1 -c -d "${dir}" -o "${cov_report_dir}/info/${cnt}.info" 1>/dev/null &
    cnt=$((cnt + 1))
  done <"${cov_report_dir}/gcda_dirs.txt"
  wait
  end_time_s=$(date +%s)
  sumTime=$(($end_time_s - $start_time_s))
  echo -e "---- convert the gcda file to the lcov info file success, use time:$sumTime seconds"

  # aggregate all Info files into one file.
  echo -e "---- Start to aggregate all Info files into one file"
  start_time_s=$(date +%s)
  local files
  files=$(ls "${cov_report_dir}/info")
  local cmd="lcov "
  for file in ${files}; do
    cmd="${cmd} -a ${cov_report_dir}/info/${file}"
  done
  cmd="${cmd} -o ${cov_report_dir}/info/raw_cov.info 1>/dev/null"
  eval "${cmd}" || go_die "---- generate coverage report failed!" 0
  end_time_s=$(date +%s)
  sumTime=$(($end_time_s - $start_time_s))
  echo -e "---- aggregate all Info files into one file success, use time:$sumTime seconds"

  # picking out the parts we care about
  lcov --extract "${cov_report_dir}/info/raw_cov.info" "*src/datasystem/*" -o "${cov_report_dir}/info/half_baked_cov.info" 1>/dev/null || go_die "---- generate coverage report failed!" 0
  lcov --remove "${cov_report_dir}/info/half_baked_cov.info" "*/protos/*" -o "${cov_report_dir}/info/well_done_cov.info" 1>/dev/null || go_die "---- generate coverage report failed!" 0

  cp "${cov_report_dir}/info/well_done_cov.info" "${OUTPUT_DIR}/coverage_${UT_INDEX}.info"
  echo -e "---- generate coverage info success"
}

function main() {
  init_default_opts

  while getopts 'hd:n:i:j:o:' OPT; do
    case "${OPT}" in
    d)
      BUILD_DIR=$(realpath -m "${OPTARG}")
      ;;
    h)
      echo -e "${USAGE}"
      exit 0
      ;;
    n)
      check_number "${OPTARG}" u
      UT_COUNT="${OPTARG}"
      ;;
    i)
      check_number "${OPTARG}" i
      UT_INDEX="${OPTARG}"
      if [[ ${#UT_INDEX} -lt 2 ]]; then
        UT_INDEX="0${UT_INDEX}"
      fi
      ;;
    j)
      check_number "${OPTARG}" j
      TEST_PARALLEL_JOBS="${OPTARG}"
      ;;
    o)
      OUTPUT_DIR=$(realpath -m "${OPTARG}")
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

  OUTPUT_DIR="${OUTPUT_DIR}/result${UT_INDEX}"
  [[ -d "${OUTPUT_DIR}" ]] && rm -rf "${OUTPUT_DIR}"

  local start_time_s
  local end_time_s
  local sumTime
  start_time_s=$(date +%s)
  split_ut
  run_ut
  gen_coverage_info
  end_time_s=$(date +%s)
  sumTime=$(($end_time_s - $start_time_s))
  echo -e "---- Total use time:$sumTime seconds"

  exit 0
}

main "$@"
