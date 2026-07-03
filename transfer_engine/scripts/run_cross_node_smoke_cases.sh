#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${TE_ROOT}/.." && pwd)"

if [[ -n "${TRANSFER_ENGINE_CROSS_NODE_SMOKE_BIN:-}" ]]; then
  BIN="${TRANSFER_ENGINE_CROSS_NODE_SMOKE_BIN}"
elif [[ -x "${TE_ROOT}/build/transfer_engine_cross_node_smoke" ]]; then
  BIN="${TE_ROOT}/build/transfer_engine_cross_node_smoke"
else
  BIN="${REPO_ROOT}/build/transfer_engine/transfer_engine_cross_node_smoke"
fi

usage() {
  cat <<USAGE
Usage:
  # Case 1: basic cross-node validate
  ${0} basic owner \\
      --local-ip <owner_ip> --local-port <owner_port> --device-id <owner_dev> --size <bytes> [--hold-seconds <sec>] [--contiguous-batch]
  ${0} basic requester \\
      --local-ip <requester_ip> --local-port <requester_port> --device-id <requester_dev> --size <bytes> \\
      --peer-ip <owner_ip> --peer-port <owner_port> --remote-addrs <addr0[,addr1,...]>
      [--skip-readback] [--contiguous-batch] [--perf-warmup <n>] [--perf-repeats <n>]

  # Case 2: one owner + multiple requester processes concurrent read
  ${0} concurrent owner \\
      --local-ip <owner_ip> --local-port <owner_port> --device-id <owner_dev> --size <bytes> [--hold-seconds <sec>]
  ${0} concurrent requester \\
      --local-ip <requester_ip> --local-port <requester_base_port> --device-id <requester_base_dev> --size <bytes> \\
      --peer-ip <owner_ip> --peer-port <owner_port> --remote-addrs <addr0[,addr1,...]> --requester-count <n> \\
      [--requester-port-step <n>] [--requester-device-step <n>]

Tips:
  1) Run owner first. Owner log prints a line like:
     [OWNER_READY_FOR_REQUESTER] --peer-ip ... --peer-port ... --peer-device-base-id ... --remote-addrs 0x...
  2) remote-addrs 的数量就是 BatchTransferSyncRead 的 batch size。
USAGE
}

if [[ $# -lt 2 ]]; then
  usage
  exit 2
fi

case_name="$1"
role="$2"
shift 2

if [[ ! -x "${BIN}" ]]; then
  echo "[ERROR] binary not found: ${BIN}" >&2
  echo "[INFO] build first with repository build.sh: bash build.sh -X on" >&2
  echo "[INFO] or standalone: cmake -S ${TE_ROOT} -B ${TE_ROOT}/build -DTRANSFER_ENGINE_ENABLE_HIXL=ON && cmake --build ${TE_ROOT}/build -j" >&2
  echo "[INFO] override binary path with TRANSFER_ENGINE_CROSS_NODE_SMOKE_BIN=/path/to/transfer_engine_cross_node_smoke" >&2
  exit 1
fi

common_owner_args=(--role owner --pattern 7)
common_req_args=(--role requester)

run_basic() {
  if [[ "${role}" == "owner" ]]; then
    exec "${BIN}" "${common_owner_args[@]}" "$@"
  elif [[ "${role}" == "requester" ]]; then
    exec "${BIN}" "${common_req_args[@]}" "$@"
  else
    echo "[ERROR] role must be owner/requester" >&2
    exit 2
  fi
}

run_concurrent() {
  if [[ "${role}" == "owner" ]]; then
    exec "${BIN}" "${common_owner_args[@]}" "$@"
  elif [[ "${role}" == "requester" ]]; then
    exec "${BIN}" "${common_req_args[@]}" --auto-verify-data "$@"
  else
    echo "[ERROR] role must be owner/requester" >&2
    exit 2
  fi
}

case "${case_name}" in
  basic)
    run_basic "$@"
    ;;
  concurrent)
    run_concurrent "$@"
    ;;
  *)
    echo "[ERROR] case must be basic or concurrent" >&2
    usage
    exit 2
    ;;
esac
