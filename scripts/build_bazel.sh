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

# Description: Bazel build functions for datasystem.

# Map sanitizer name from build.sh value to bazel config name.
function _sanitizer_to_bazel_config() {
  case "$1" in
    address)  echo "asan" ;;
    thread)   echo "tsan" ;;
    undefined) echo "ubsan" ;;
    *)        echo "$1" ;;
  esac
}

# Build bazel config flags from build.sh parameters.
# Outputs one config per line to avoid eval issues.
function _bazel_build_configs() {
  local test_included="$1"

  # Build type
  if [[ "${BUILD_TYPE}" = "Debug" ]]; then
    echo "--config=debug"
  else
    echo "--config=release"
  fi

  # Sanitizers
  local sanitizer_lower
  sanitizer_lower=$(echo "${USE_SANITIZER}" | tr '[:upper:]' '[:lower:]')
  if [[ "${sanitizer_lower}" != "off" ]]; then
    echo "--config=$(_sanitizer_to_bazel_config "${sanitizer_lower}")"
  fi

  # Coverage
  if is_on "${BUILD_COVERAGE}"; then
    echo "--config=coverage"
  fi

  # Hetero
  if is_on "${BUILD_HETERO}"; then
    echo "--config=hetero"
  fi

  # Pipeline H2D
  if is_on "${BUILD_PIPLN_H2D}"; then
    echo "--config=pipeline_h2d"
  fi

  # URMA
  if is_on "${BUILD_WITH_URMA}"; then
    echo "--config=urma"
  fi

  # Test config
  if [[ "${test_included}" != "no" && "${RUN_TESTS}" != "off" ]]; then
    echo "--config=test"
  fi
}

# Build and install using bazel.
function build_datasystem_bazel() {
  echo -e "-- building datasystem (bazel)..."

  # Warn about unsupported options
  if is_on "${BUILD_WITH_NINJA}"; then
    echo -e "-- [INFO] bazel mode: ignoring -n (Ninja) option, bazel has its own build scheduler."
  fi
  if is_on "${BUILD_INCREMENT}"; then
    echo -e "-- [INFO] bazel mode: ignoring -i (incremental) option, bazel is incremental by default."
  fi
  if is_on "${DOWNLOAD_UB}"; then
    echo -e "-- [INFO] bazel mode: ignoring -D (download UB) option."
  fi
  if is_on "${ENABLE_PERF}"; then
    echo -e "-- [INFO] bazel mode: -p (perf) not yet supported."
  fi
  if is_on "${SUPPORT_JEPROF}"; then
    echo -e "-- [INFO] bazel mode: -x (jemalloc profiling) not yet supported."
  fi
  if is_on "${PACKAGE_JAVA}"; then
    echo -e "-- [INFO] bazel mode: -J (Java API) not yet supported."
  fi
  if is_on "${PACKAGE_GO}"; then
    echo -e "-- [INFO] bazel mode: -G (Go SDK) not yet supported."
  fi
  if is_on "${BUILD_WITH_RDMA}"; then
    echo -e "-- [INFO] bazel mode: -A (RDMA) not yet supported."
  fi

  # Check bazel is available
  if ! command -v bazel &>/dev/null; then
    go_die "ERROR: bazel not found in PATH. Please install bazel first."
  fi

  # Build configs as array (avoid eval)
  local -a configs
  while IFS= read -r cfg; do
    configs+=("${cfg}")
  done < <(_bazel_build_configs "yes")

  # Build targets
  local -a targets=("//:datasystem")
  if is_on "${PACKAGE_PYTHON}"; then
    targets+=("//bazel:datasystem_wheel")
  fi

  local baseTime_s
  baseTime_s=$(date +%s)

  echo -e "-- bazel command: bazel build ${configs[@]} --jobs=${BUILD_THREAD_NUM} ${targets[@]}"
  cd "${DATASYSTEM_DIR}"
  bazel build "${configs[@]}" --jobs="${BUILD_THREAD_NUM}" "${targets[@]}" || go_die "-- bazel build failed!"
  echo -e "---- [TIMER] Build source: $(($(date +%s)-$baseTime_s)) seconds"

  # Install outputs
  _bazel_install_outputs
}

# Copy bazel build outputs to INSTALL_DIR.
function _bazel_install_outputs() {
  echo -e "-- installing bazel outputs to ${INSTALL_DIR}..."
  mkdir -p "${INSTALL_DIR}"

  local bazel_bin="${DATASYSTEM_DIR}/bazel-bin"

  # Copy main library
  if [[ -f "${bazel_bin}/libdatasystem.so" ]]; then
    mkdir -p "${INSTALL_DIR}/datasystem/sdk/cpp/lib"
    cp -f "${bazel_bin}/libdatasystem.so" "${INSTALL_DIR}/datasystem/sdk/cpp/lib/"
  fi

  # Copy SDK headers from source tree
  mkdir -p "${INSTALL_DIR}/datasystem/sdk/cpp/include"
  if [[ -d "${DATASYSTEM_DIR}/src" ]]; then
    find "${DATASYSTEM_DIR}/src" -name "*.h" -path "*/datasystem/*" | while read -r hdr; do
      local rel_path
      rel_path=$(echo "${hdr}" | sed "s|${DATASYSTEM_DIR}/||")
      mkdir -p "$(dirname "${INSTALL_DIR}/datasystem/sdk/cpp/${rel_path}")"
      cp -f "${hdr}" "${INSTALL_DIR}/datasystem/sdk/cpp/${rel_path}"
    done
  fi

  # Copy Python wheel if built
  if is_on "${PACKAGE_PYTHON}"; then
    local wheel_file
    wheel_file=$(find "${bazel_bin}" -name "openyuanrong_datasystem-*.whl" -type f 2>/dev/null | head -1)
    if [[ -n "${wheel_file}" ]]; then
      cp -f "${wheel_file}" "${INSTALL_DIR}/"
      echo -e "-- copied wheel: $(basename "${wheel_file}")"
    fi
  fi

  # Copy worker binary
  if [[ -f "${bazel_bin}/src/datasystem/worker/datasystem_worker" ]]; then
    mkdir -p "${INSTALL_DIR}/datasystem/service"
    cp -f "${bazel_bin}/src/datasystem/worker/datasystem_worker" "${INSTALL_DIR}/datasystem/service/"
  fi

  # Create tarball
  local version
  version=$(cat "${DATASYSTEM_DIR}/VERSION")
  if [[ -d "${INSTALL_DIR}/datasystem" ]]; then
    local orig_dir
    orig_dir=$(pwd)
    cd "${INSTALL_DIR}"
    tar -zcf "yr-datasystem-v${version}.tar.gz" datasystem
    cd "${orig_dir}"
  fi

  echo -e "-- bazel install done."
  echo -e "-- build datasystem (bazel) success!"
}

# Run tests using bazel test.
function run_bazel_testcases() {
  if [[ "${RUN_TESTS}" = "off" ]]; then
    return 0
  fi

  if [[ "${RUN_TESTS}" = "build" ]]; then
    echo -e "-- bazel mode: test build already done in build step."
    return 0
  fi

  echo -e "-- running datasystem testcases (bazel)..."

  # Build configs without test (will add separately to avoid double)
  local -a configs
  while IFS= read -r cfg; do
    configs+=("${cfg}")
  done < <(_bazel_build_configs "no")
  configs+=("--config=test")

  local baseTime_s
  baseTime_s=$(date +%s)

  echo -e "-- bazel test command: bazel test ${configs[@]} --jobs=${TEST_PARALLEL_JOBS} //..."
  cd "${DATASYSTEM_DIR}"
  bazel test "${configs[@]}" --jobs="${TEST_PARALLEL_JOBS}" --test_timeout="${LLT_TIMEOUT_S}" //... \
    || go_die "-- bazel test failed!"
  echo -e "---- run datasystem testcases success!"
  echo -e "---- [TIMER] Run bazel test: $(($(date +%s)-$baseTime_s)) seconds"
}
