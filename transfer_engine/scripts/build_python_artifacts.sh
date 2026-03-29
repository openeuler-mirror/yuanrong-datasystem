#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "Usage: $0 <python_executable> <transfer_engine_source_dir> <build_dir> <package_dir>" >&2
  exit 2
fi

PYTHON_EXECUTABLE="$1"
TRANSFER_ENGINE_SOURCE_DIR="$2"
BUILD_DIR="$3"
PACKAGE_DIR="$4"

detect_parallel_jobs() {
  if [[ -n "${CMAKE_BUILD_PARALLEL_LEVEL:-}" ]]; then
    printf '%s\n' "${CMAKE_BUILD_PARALLEL_LEVEL}"
    return
  fi

  if [[ -n "${BUILD_THREAD_NUM:-}" ]]; then
    printf '%s\n' "${BUILD_THREAD_NUM}"
    return
  fi

  if command -v nproc >/dev/null 2>&1; then
    nproc
    return
  fi

  if command -v getconf >/dev/null 2>&1; then
    getconf _NPROCESSORS_ONLN
    return
  fi

  if command -v sysctl >/dev/null 2>&1; then
    sysctl -n hw.logicalcpu
    return
  fi

  printf '4\n'
}

PARALLEL_JOBS="$(detect_parallel_jobs)"
GENERATOR_ARGS=()
if command -v ninja >/dev/null 2>&1; then
  GENERATOR_ARGS=(-G Ninja)
fi

mkdir -p "${BUILD_DIR}" "${PACKAGE_DIR}" "${PACKAGE_DIR}/lib"

cmake -S "${TRANSFER_ENGINE_SOURCE_DIR}" -B "${BUILD_DIR}" \
  "${GENERATOR_ARGS[@]}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DTRANSFER_ENGINE_BUILD_PYTHON=ON \
  -DTRANSFER_ENGINE_BUILD_TESTS=OFF \
  -DTRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY=ON \
  -DTRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO=ON \
  -DTRANSFER_ENGINE_PYTHON_OUTPUT_DIR="${PACKAGE_DIR}" \
  -DPython3_EXECUTABLE="${PYTHON_EXECUTABLE}"

cmake --build "${BUILD_DIR}" --target _transfer_engine --parallel "${PARALLEL_JOBS}"

if compgen -G "${BUILD_DIR}/lib/libp2p_transfer.so*" >/dev/null; then
  cp -a "${BUILD_DIR}"/lib/libp2p_transfer.so* "${PACKAGE_DIR}/lib/"
fi
