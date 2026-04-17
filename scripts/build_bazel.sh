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

  # Perf
  if is_on "${ENABLE_PERF}"; then
    echo "--config=perf"
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
  local -a targets=(
    "//bazel:datasystem_sdk"                           # SDK tar (headers + cmake + stripped lib)
    "//src/datasystem/worker:datasystem_worker_shared"  # worker shared library
    "//:libjemalloc_shared_file"                        # jemalloc
  )
  if is_on "${PACKAGE_PYTHON}"; then
    targets+=("//bazel:datasystem_wheel")
  fi
  # Tools (only when tests enabled, matching CMake behavior)
  if [[ "${RUN_TESTS}" != "off" ]]; then
    targets+=("//:hashring_parser_file" "//:curve_keygen_file")
  fi
  # Strip/sym artifacts (only for non-debug builds)
  if [[ "${BUILD_TYPE}" != "Debug" ]] && is_on "${ENABLE_STRIP}"; then
    targets+=("//:libdatasystem_sym" "//:datasystem_worker_stripped" "//:datasystem_worker_sym")
  else
    targets+=("//src/datasystem/worker:datasystem_worker")
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

# Copy bazel build outputs to INSTALL_DIR, matching CMake output structure.
function _bazel_install_outputs() {
  echo -e "-- installing bazel outputs to ${INSTALL_DIR}..."

  local bazel_bin="${DATASYSTEM_DIR}/bazel-bin"

  # Clean and create output dir
  if [[ "${BUILD_INCREMENT}" == "off" && -d "${INSTALL_DIR}" ]]; then
    rm -rf "${INSTALL_DIR}"
    echo -e "-- [Warning] Removed output folder ${INSTALL_DIR}"
  fi
  mkdir -p "${INSTALL_DIR}"

  local DS_DIR="${INSTALL_DIR}/datasystem"

  # --- 1. SDK from datasystem_sdk tar ---
  local sdk_tar="${bazel_bin}/bazel/datasystem_sdk.tar"
  if [[ -f "${sdk_tar}" ]]; then
    local sdk_tmp
    sdk_tmp=$(mktemp -d)
    tar -xf "${sdk_tar}" -C "${sdk_tmp}" || go_die "-- failed to extract SDK tar"

    # Verify expected structure
    if [[ ! -d "${sdk_tmp}/datasystem_sdk/cpp" ]]; then
      rm -rf "${sdk_tmp}"
      go_die "-- SDK tar did not contain expected datasystem_sdk/cpp directory"
    fi

    # Copy to output/datasystem/sdk/cpp/ (for deployment)
    mkdir -p "${DS_DIR}/sdk"
    cp -rL "${sdk_tmp}/datasystem_sdk/cpp" "${DS_DIR}/sdk/cpp"

    # Copy to output/cpp/ (external SDK for find_package)
    cp -rL "${sdk_tmp}/datasystem_sdk/cpp" "${INSTALL_DIR}/cpp"

    rm -rf "${sdk_tmp}"
  else
    echo -e "-- [WARN] SDK tar not found: ${sdk_tar}"
  fi

  # --- 2. Symbol files for libdatasystem ---
  if [[ -f "${bazel_bin}/libdatasystem.so.sym" ]]; then
    # SDK syms at sdk/DATASYSTEM_SYM (sibling of cpp/), matching CMake strip_symbols layout
    mkdir -p "${DS_DIR}/sdk/DATASYSTEM_SYM"
    cp -f "${bazel_bin}/libdatasystem.so.sym" "${DS_DIR}/sdk/DATASYSTEM_SYM/"
    # External SDK syms at cpp/DATASYSTEM_SYM (sibling of lib/)
    mkdir -p "${INSTALL_DIR}/cpp/DATASYSTEM_SYM"
    cp -f "${bazel_bin}/libdatasystem.so.sym" "${INSTALL_DIR}/cpp/DATASYSTEM_SYM/"
  fi

  # --- 3. Service ---
  mkdir -p "${DS_DIR}/service/lib"
  mkdir -p "${DS_DIR}/service/DATASYSTEM_SYM"

  # Worker binary (stripped if available)
  if [[ -f "${bazel_bin}/datasystem_worker.stripped" ]]; then
    cp -f "${bazel_bin}/datasystem_worker.stripped" "${DS_DIR}/service/datasystem_worker"
  elif [[ -f "${bazel_bin}/src/datasystem/worker/datasystem_worker" ]]; then
    cp -f "${bazel_bin}/src/datasystem/worker/datasystem_worker" "${DS_DIR}/service/datasystem_worker"
  fi

  # Worker symbol file
  if [[ -f "${bazel_bin}/datasystem_worker.sym" ]]; then
    cp -f "${bazel_bin}/datasystem_worker.sym" "${DS_DIR}/service/DATASYSTEM_SYM/"
  fi

  # Worker shared library (rename to match CMake output name)
  if [[ -f "${bazel_bin}/src/datasystem/worker/libdatasystem_worker_shared.so" ]]; then
    cp -f "${bazel_bin}/src/datasystem/worker/libdatasystem_worker_shared.so" "${DS_DIR}/service/lib/libdatasystem_worker.so"
  fi

  # jemalloc
  if [[ -f "${bazel_bin}/yr/datasystem/lib/libjemalloc.so.2" ]]; then
    cp -f "${bazel_bin}/yr/datasystem/lib/libjemalloc.so.2" "${DS_DIR}/service/lib/"
  fi

  # --- 4. Config files ---
  cp -f "${DATASYSTEM_DIR}/cli/deploy/conf/worker_config.json" "${DS_DIR}/service/"
  cp -f "${DATASYSTEM_DIR}/cli/deploy/conf/cluster_config.json" "${DS_DIR}/service/"

  # --- 5. Tools (only when tests enabled, matching CMake behavior) ---
  if [[ "${RUN_TESTS}" != "off" ]]; then
    mkdir -p "${DS_DIR}/tools"
    if [[ -f "${bazel_bin}/yr/datasystem/tools/hashring_parser" ]]; then
      cp -f "${bazel_bin}/yr/datasystem/tools/hashring_parser" "${DS_DIR}/tools/"
    fi
    if [[ -f "${bazel_bin}/yr/datasystem/tools/curve_keygen" ]]; then
      cp -f "${bazel_bin}/yr/datasystem/tools/curve_keygen" "${DS_DIR}/tools/"
    fi
  fi

  # --- 6. CLI ---
  mkdir -p "${DS_DIR}/cli"
  if [[ -d "${DATASYSTEM_DIR}/cli" ]]; then
    cp -rL "${DATASYSTEM_DIR}/cli/." "${DS_DIR}/cli/"
  fi

  # --- 7. Misc ---
  cp -f "${DATASYSTEM_DIR}/VERSION" "${DS_DIR}/VERSION"
  cp -f "${DATASYSTEM_DIR}/README.md" "${DS_DIR}/README.md"

  # Commit ID
  local git_hash
  git_hash=$(cd "${DATASYSTEM_DIR}" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")
  echo "${git_hash}" > "${DS_DIR}/.commit_id"

  # --- 8. Python wheel ---
  if is_on "${PACKAGE_PYTHON}"; then
    local wheel_file
    wheel_file=$(find -L "${bazel_bin}" -name "openyuanrong_datasystem-*.whl" -type f 2>/dev/null | head -1)
    if [[ -n "${wheel_file}" ]]; then
      cp -f "${wheel_file}" "${INSTALL_DIR}/"
      echo -e "-- copied wheel: $(basename "${wheel_file}")"
    fi
  fi

  # --- 9. Create tarball ---
  local version
  version=$(cat "${DATASYSTEM_DIR}/VERSION")
  if [[ -d "${DS_DIR}" ]]; then
    cd "${INSTALL_DIR}"
    tar --remove-files -zcf "yr-datasystem-v${version}.tar.gz" datasystem || go_die "-- failed to create deployment tarball"
    cd "${DATASYSTEM_DIR}"
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
