#!/bin/bash
readonly BASE_DIR=$(dirname "$(readlink -f "$0")")
echo $BASE_DIR
readonly DATASYSTEM_DIR="${BASE_DIR}"

export PATH=$PATH:${BASE_DIR}/scripts/modules
export INSTALL_DIR="${DATASYSTEM_DIR}/output"
export BUILD_DIR="${DATASYSTEM_DIR}/build"
export PATH=$PATH:${BASE_DIR}/scripts/modules
. llt_util.sh

function run_example() {
  echo -e "---- Start Smoke Testing..."
  bash "${DATASYSTEM_DIR}/example/run-example.sh" "off" "off" "on" ||
    (remove_running_pids && go_die "---- Smoke Testing failed!")
  echo -e "---- Smoke Testing success!"
}

function run_ut_python() {
  echo -e "---- running python testcases..."
  local version_file
  version_file="${DATASYSTEM_DIR}/VERSION"
  local version_value
  version_value=$(cat "${version_file}")
  DATASYSTEM_VERSION=${version_value#*=}
  PYTHON_VERSION=$(python3 -c 'import sys; version=sys.version_info[:3]; print("{0}.{1}".format(*version))')
  PYTHON=$(which python${PYTHON_VERSION}) ||
    go_die "---- Could not find python${PYTHON_VERSION}, run datasystem testcases failed!"
  local python_test_dir="${DATASYSTEM_DIR}/tests/python"
  ${PYTHON} -m pip install ${INSTALL_DIR}/yr_datasystem-${DATASYSTEM_VERSION}*.whl --force-reinstall
  dscli generate_config -o ${INSTALL_DIR}/service
  cd ${python_test_dir}
  ${PYTHON} -m unittest || (remove_running_pids && go_die "---- run datasystem testcases failed!")
  echo -e "---- run datasystem python testcases success!"
}

if [[ "X$1" == "Xstart" ]]; then
    start_all "${BUILD_DIR}" "${INSTALL_DIR}"/service/deploy
elif [[ "X$1" == "Xrun_ut" ]]; then
    run_ut_python
elif [[ "X$1" == "Xstop" ]]; then
    stop_all "${INSTALL_DIR}"/service/deploy
elif [[ "X$1" == "Xrun_example" ]]; then
    run_example
else
    echo "invalid parameter"
fi
