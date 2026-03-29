#!/usr/bin/env bash
set -euo pipefail

# Real NPU dual-process validation based on bundled p2p_transfer benchmark.
# Note: current benchmark target p2p_transfer_test uses fixed device ids: sender=3, receiver=4.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
P2P_ROOT="${TE_ROOT}/src/internal/backend/ascend/p2p_transfer"
P2P_BUILD_DIR="${P2P_ROOT}/build"

BUILD_JOBS="${BUILD_JOBS:-8}"
TEST_BIN="${P2P_BUILD_DIR}/test/p2p_transfer_test"

# Ascend runtime path (same policy as project).
if [[ -n "${ASCEND_HOME_PATH:-}" ]]; then
  ASCEND_ROOT="${ASCEND_HOME_PATH}"
elif [[ -n "${ASCEND_CUSTOM_PATH:-}" ]]; then
  ASCEND_ROOT="${ASCEND_CUSTOM_PATH}/latest"
else
  ASCEND_ROOT="/usr/local/Ascend/ascend-toolkit/latest"
fi

if [[ ! -d "${ASCEND_ROOT}" ]]; then
  echo "[ERROR] Ascend root not found: ${ASCEND_ROOT}" >&2
  exit 1
fi

if [[ ! -f "${ASCEND_ROOT}/lib64/libascendcl.so" ]]; then
  echo "[ERROR] libascendcl.so not found under ${ASCEND_ROOT}/lib64" >&2
  exit 1
fi

if [[ ! -f "${ASCEND_ROOT}/lib64/libhccl.so" ]]; then
  echo "[ERROR] libhccl.so not found under ${ASCEND_ROOT}/lib64" >&2
  exit 1
fi

echo "[INFO] Building bundled p2p_transfer and test target..."
cmake -S "${P2P_ROOT}" -B "${P2P_BUILD_DIR}" -DCMAKE_BUILD_TYPE=Release
cmake --build "${P2P_BUILD_DIR}" --target p2p_transfer_test -j"${BUILD_JOBS}"

if [[ ! -x "${TEST_BIN}" ]]; then
  echo "[ERROR] test binary not found: ${TEST_BIN}" >&2
  exit 1
fi

export LD_LIBRARY_PATH="${ASCEND_ROOT}/lib64:${P2P_BUILD_DIR}:${LD_LIBRARY_PATH:-}"

echo "[INFO] Running real NPU dual-process validation (fork model in binary)..."
echo "[INFO] Binary: ${TEST_BIN}"
echo "[INFO] Fixed device ids inside binary: sender=3, receiver=4"

"${TEST_BIN}"

echo "[PASS] Real NPU dual-process validation completed."
