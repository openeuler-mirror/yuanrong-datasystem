#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run_cross_node_smoke_cases.sh"

LOCAL_IP="${LOCAL_IP:-127.0.0.1}"
RPC_BASE_PORT="${RPC_BASE_PORT:-65051}"
HIXL_BASE_PORT="${TRANSFER_ENGINE_HIXL_BASE_PORT:-21000}"
ROUTE="${TRANSFER_ENGINE_HIXL_ROUTE:-hccs}"
LOG_DIR="${TRANSFER_ENGINE_HIXL_SMOKE_LOG_DIR:-/tmp/te_hixl_d2d_smoke_$(date +%Y%m%d_%H%M%S)}"
OWNER_HOLD_SECONDS="${OWNER_HOLD_SECONDS:-600}"
OWNER_READY_TIMEOUT_S="${OWNER_READY_TIMEOUT_S:-30}"
REQUESTER_COUNT="${REQUESTER_COUNT:-2}"
OWNER_DEVICE="${OWNER_DEVICE:-0}"
REQUESTER_DEVICE="${REQUESTER_DEVICE:-1}"
REQUESTER_DEVICE_STEP="${REQUESTER_DEVICE_STEP:-1}"

OWNER_PIDS=()

usage() {
  cat <<USAGE
Usage:
  ${0}

Environment overrides:
  LOCAL_IP                         default: ${LOCAL_IP}
  RPC_BASE_PORT                    default: ${RPC_BASE_PORT}
  TRANSFER_ENGINE_HIXL_BASE_PORT   default: ${HIXL_BASE_PORT}
  TRANSFER_ENGINE_HIXL_ROUTE       default: ${ROUTE}
  TRANSFER_ENGINE_HIXL_SMOKE_LOG_DIR
  OWNER_HOLD_SECONDS               default: ${OWNER_HOLD_SECONDS}
  OWNER_READY_TIMEOUT_S            default: ${OWNER_READY_TIMEOUT_S}
  REQUESTER_COUNT                  default: ${REQUESTER_COUNT}
  OWNER_DEVICE                     default: ${OWNER_DEVICE}
  REQUESTER_DEVICE                 default: ${REQUESTER_DEVICE}
  REQUESTER_DEVICE_STEP            default: ${REQUESTER_DEVICE_STEP}

This suite runs:
  1. requester device reads owner device, batch=4, 1 MiB each.
  2. owner device reads requester device, batch=4, 1 MiB each.
  3. ${REQUESTER_COUNT} requester processes read the same owner concurrently.
  4. unregistered remote address is rejected.
  5. requester device reads owner device, batch=4, 16 MiB each.
USAGE
}

cleanup() {
  for pid in "${OWNER_PIDS[@]:-}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
    fi
  done
  for pid in "${OWNER_PIDS[@]:-}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      wait "${pid}" 2>/dev/null || true
    fi
  done
}

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

require_port_range() {
  local port="$1"
  local name="$2"
  if (( port <= 0 || port > 65535 )); then
    fail "${name} is out of TCP port range: ${port}"
  fi
}

require_nonnegative_int() {
  local value="$1"
  local name="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]]; then
    fail "${name} must be a non-negative integer: ${value}"
  fi
}

ensure_runner() {
  if [[ ! -x "${RUNNER}" ]]; then
    fail "runner not executable: ${RUNNER}"
  fi
}

wait_for_owner() {
  local pid="$1"
  local log_file="$2"
  local waited=0
  while (( waited < OWNER_READY_TIMEOUT_S )); do
    if grep -q "\[OWNER_READY_FOR_REQUESTER\]" "${log_file}" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "[OWNER_LOG] ${log_file}" >&2
      cat "${log_file}" >&2 || true
      fail "owner exited before ready"
    fi
    sleep 1
    waited=$((waited + 1))
  done
  echo "[OWNER_LOG] ${log_file}" >&2
  cat "${log_file}" >&2 || true
  fail "owner did not become ready within ${OWNER_READY_TIMEOUT_S}s"
}

extract_remote_addrs() {
  local log_file="$1"
  grep -m1 "\[OWNER_READY_FOR_REQUESTER\]" "${log_file}" |
    sed -n 's/.*--remote-addrs \([^ ]*\).*/\1/p'
}

start_owner() {
  local name="$1"
  local port="$2"
  local device="$3"
  local size="$4"
  local register_count="$5"
  local log_file="${LOG_DIR}/${name}_owner.log"

  require_port_range "${port}" "${name} owner port"
  echo "[RUN] owner ${name}: device=${device}, port=${port}, size=${size}, register_count=${register_count}"
  "${RUNNER}" basic owner \
    --local-ip "${LOCAL_IP}" --local-port "${port}" --device-id "${device}" \
    --size "${size}" --register-count "${register_count}" --hold-seconds "${OWNER_HOLD_SECONDS}" \
    >"${log_file}" 2>&1 &
  local pid=$!
  OWNER_PIDS+=("${pid}")
  wait_for_owner "${pid}" "${log_file}"

  local addrs
  addrs="$(extract_remote_addrs "${log_file}")"
  if [[ -z "${addrs}" ]]; then
    cat "${log_file}" >&2 || true
    fail "cannot parse owner remote addrs from ${log_file}"
  fi
  echo "[READY] owner ${name}: pid=${pid}, addrs=${addrs}"
  OWNER_ADDRS="${addrs}"
}

stop_owners() {
  cleanup
  OWNER_PIDS=()
}

run_requester_success() {
  local name="$1"
  local port="$2"
  local device="$3"
  local peer_port="$4"
  local peer_device="$5"
  local size="$6"
  local addrs="$7"
  local log_file="${LOG_DIR}/${name}_requester.log"

  require_port_range "${port}" "${name} requester port"
  echo "[RUN] requester ${name}: device=${device}, port=${port}, peer_port=${peer_port}, size=${size}"
  if ! "${RUNNER}" basic requester \
      --local-ip "${LOCAL_IP}" --local-port "${port}" --device-id "${device}" \
      --size "${size}" \
      --peer-ip "${LOCAL_IP}" --peer-port "${peer_port}" --peer-device-base-id "${peer_device}" \
      --remote-addrs "${addrs}" --auto-verify-data \
      >"${log_file}" 2>&1; then
    cat "${log_file}" >&2 || true
    fail "requester ${name} failed"
  fi
  cat "${log_file}"
  grep -q "\[REQUESTER_BATCH_DONE\]" "${log_file}" || fail "requester ${name} did not finish a batch"
}

run_concurrent_requesters() {
  local name="$1"
  local port="$2"
  local device="$3"
  local peer_port="$4"
  local peer_device="$5"
  local size="$6"
  local addrs="$7"
  local log_file="${LOG_DIR}/${name}_requester.log"

  require_port_range "${port}" "${name} requester port"
  echo "[RUN] concurrent requesters ${name}: count=${REQUESTER_COUNT}, device_base=${device}, port_base=${port}"
  if ! "${RUNNER}" concurrent requester \
      --local-ip "${LOCAL_IP}" --local-port "${port}" --device-id "${device}" \
      --size "${size}" \
      --peer-ip "${LOCAL_IP}" --peer-port "${peer_port}" --peer-device-base-id "${peer_device}" \
      --remote-addrs "${addrs}" \
      --requester-count "${REQUESTER_COUNT}" --requester-port-step 1 --requester-device-step "${REQUESTER_DEVICE_STEP}" \
      >"${log_file}" 2>&1; then
    cat "${log_file}" >&2 || true
    fail "concurrent requester ${name} failed"
  fi
  cat "${log_file}"
  local done_count
  done_count="$(grep -c "\[REQUESTER_BATCH_DONE\]" "${log_file}" || true)"
  if (( done_count != REQUESTER_COUNT )); then
    fail "expected ${REQUESTER_COUNT} completed requester batches, got ${done_count}"
  fi
}

run_requester_expect_reject() {
  local name="$1"
  local port="$2"
  local device="$3"
  local peer_port="$4"
  local peer_device="$5"
  local size="$6"
  local bad_addr="$7"
  local log_file="${LOG_DIR}/${name}_requester.log"

  require_port_range "${port}" "${name} requester port"
  echo "[RUN] requester rejection ${name}: bad_addr=${bad_addr}"
  if "${RUNNER}" basic requester \
      --local-ip "${LOCAL_IP}" --local-port "${port}" --device-id "${device}" \
      --size "${size}" \
      --peer-ip "${LOCAL_IP}" --peer-port "${peer_port}" --peer-device-base-id "${peer_device}" \
      --remote-addrs "${bad_addr}" --auto-verify-data \
      >"${log_file}" 2>&1; then
    cat "${log_file}" >&2 || true
    fail "requester ${name} unexpectedly succeeded"
  fi
  cat "${log_file}"
  grep -q "remote range is not registered" "${log_file}" ||
    fail "requester ${name} failed for an unexpected reason"
}

set_case_hixl_base_port() {
  local case_index="$1"
  local case_name="$2"
  local case_base=$((HIXL_BASE_PORT + case_index * 1000))
  if (( case_base <= 0 || case_base > 65000 )); then
    fail "${case_name} HIXL base port is out of supported smoke range: ${case_base}"
  fi
  export TRANSFER_ENGINE_HIXL_BASE_PORT="${case_base}"
  echo "[INFO] case ${case_name}: TRANSFER_ENGINE_HIXL_BASE_PORT=${TRANSFER_ENGINE_HIXL_BASE_PORT}"
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  ensure_runner
  require_nonnegative_int "${OWNER_DEVICE}" "OWNER_DEVICE"
  require_nonnegative_int "${REQUESTER_DEVICE}" "REQUESTER_DEVICE"
  require_nonnegative_int "${REQUESTER_DEVICE_STEP}" "REQUESTER_DEVICE_STEP"
  mkdir -p "${LOG_DIR}"
  export TRANSFER_ENGINE_BACKEND=hixl
  export TRANSFER_ENGINE_HIXL_ROUTE="${ROUTE}"
  export TRANSFER_ENGINE_HIXL_BASE_PORT="${HIXL_BASE_PORT}"
  export TRANSFER_ENGINE_ACL_MALLOC_POLICY="${TRANSFER_ENGINE_ACL_MALLOC_POLICY:-huge_only}"
  if [[ "${ROUTE}" == "hccs" ]]; then
    unset HCCL_INTRA_ROCE_ENABLE
  fi

  echo "[INFO] log dir: ${LOG_DIR}"
  echo "[INFO] env: TRANSFER_ENGINE_BACKEND=${TRANSFER_ENGINE_BACKEND}, TRANSFER_ENGINE_HIXL_ROUTE=${TRANSFER_ENGINE_HIXL_ROUTE}, TRANSFER_ENGINE_HIXL_BASE_PORT=${TRANSFER_ENGINE_HIXL_BASE_PORT}, TRANSFER_ENGINE_ACL_MALLOC_POLICY=${TRANSFER_ENGINE_ACL_MALLOC_POLICY}"
  echo "[INFO] devices: OWNER_DEVICE=${OWNER_DEVICE}, REQUESTER_DEVICE=${REQUESTER_DEVICE}, REQUESTER_COUNT=${REQUESTER_COUNT}, REQUESTER_DEVICE_STEP=${REQUESTER_DEVICE_STEP}"
  trap cleanup EXIT

  local owner_port requester_port remote_addrs
  local forward_case="dev${REQUESTER_DEVICE}_reads_dev${OWNER_DEVICE}"
  local reverse_case="dev${OWNER_DEVICE}_reads_dev${REQUESTER_DEVICE}"

  set_case_hixl_base_port 0 "${forward_case}_batch4_1m"
  owner_port="${RPC_BASE_PORT}"
  requester_port="$((RPC_BASE_PORT + 100))"
  start_owner "${forward_case}_batch4_1m" "${owner_port}" "${OWNER_DEVICE}" 1048576 4
  remote_addrs="${OWNER_ADDRS}"
  run_requester_success "${forward_case}_batch4_1m" "${requester_port}" "${REQUESTER_DEVICE}" "${owner_port}" "${OWNER_DEVICE}" 1048576 "${remote_addrs}"
  stop_owners

  set_case_hixl_base_port 1 "${reverse_case}_batch4_1m"
  owner_port="$((RPC_BASE_PORT + 1))"
  requester_port="$((RPC_BASE_PORT + 101))"
  start_owner "${reverse_case}_batch4_1m" "${owner_port}" "${REQUESTER_DEVICE}" 1048576 4
  remote_addrs="${OWNER_ADDRS}"
  run_requester_success "${reverse_case}_batch4_1m" "${requester_port}" "${OWNER_DEVICE}" "${owner_port}" "${REQUESTER_DEVICE}" 1048576 "${remote_addrs}"
  stop_owners

  set_case_hixl_base_port 2 "concurrent_${forward_case}"
  owner_port="$((RPC_BASE_PORT + 2))"
  requester_port="$((RPC_BASE_PORT + 102))"
  start_owner "concurrent_${forward_case}" "${owner_port}" "${OWNER_DEVICE}" 1048576 4
  remote_addrs="${OWNER_ADDRS}"
  run_concurrent_requesters "concurrent_${forward_case}" "${requester_port}" "${REQUESTER_DEVICE}" "${owner_port}" "${OWNER_DEVICE}" 1048576 "${remote_addrs}"
  run_requester_expect_reject "reject_unregistered_remote_addr" "$((RPC_BASE_PORT + 103))" "${REQUESTER_DEVICE}" "${owner_port}" "${OWNER_DEVICE}" 1048576 "0x123456780000"
  stop_owners

  set_case_hixl_base_port 3 "${forward_case}_batch4_16m"
  owner_port="$((RPC_BASE_PORT + 4))"
  requester_port="$((RPC_BASE_PORT + 104))"
  start_owner "${forward_case}_batch4_16m" "${owner_port}" "${OWNER_DEVICE}" 16777216 4
  remote_addrs="${OWNER_ADDRS}"
  run_requester_success "${forward_case}_batch4_16m" "${requester_port}" "${REQUESTER_DEVICE}" "${owner_port}" "${OWNER_DEVICE}" 16777216 "${remote_addrs}"
  stop_owners

  echo "[PASS] HIXL D2D smoke suite passed"
  echo "[INFO] logs: ${LOG_DIR}"
}

main "$@"
