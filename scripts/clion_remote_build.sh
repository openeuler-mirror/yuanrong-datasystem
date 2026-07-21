#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-tiantiyun-80c128g}"
WORKTREE_NAME="$(basename "$(pwd)")"
REMOTE_DIR="${REMOTE_DIR:-$(pwd)}"
REMOTE_THIRDPARTY="${REMOTE_THIRDPARTY:-/home/ds-thirdparty-cache}"
LOCAL_THIRDPARTY="${LOCAL_THIRDPARTY:-.clion-remote/${WORKTREE_NAME}/ds-thirdparty-cache}"
BUILD_DIR="${BUILD_DIR:-.clion-remote/${WORKTREE_NAME}/build}"
OUTPUT_DIR="${OUTPUT_DIR:-.clion-remote/${WORKTREE_NAME}/output}"
JOBS="${JOBS:-80}"
TEST_JOBS="${TEST_JOBS:-20}"
TEST_TIMEOUT="${TEST_TIMEOUT:-40}"
PROFILE="${1:-index}"
REMOTE_HTTP_PROXY="${REMOTE_HTTP_PROXY:-}"
REMOTE_HTTPS_PROXY="${REMOTE_HTTPS_PROXY:-${REMOTE_HTTP_PROXY}}"
REMOTE_NO_PROXY="${REMOTE_NO_PROXY:-}"

case "${PROFILE}" in
  index)
    BUILD_ARGS=(-B "${BUILD_DIR}" -o "${OUTPUT_DIR}" -X off -P off -U on -j "${JOBS}" -i on)
    ;;
  fresh-index)
    BUILD_ARGS=(-B "${BUILD_DIR}" -o "${OUTPUT_DIR}" -X off -P off -U on -j "${JOBS}" -i off)
    ;;
  tests-index)
    BUILD_ARGS=(-B "${BUILD_DIR}" -o "${OUTPUT_DIR}" -X off -P off -U on -j "${JOBS}" -i on -t build)
    ;;
  *)
    echo "Usage: $0 [index|fresh-index|tests-index]" >&2
    exit 2
    ;;
esac

echo "==> Ensuring local CLion project uses CompDB"
python3 scripts/ensure_clion_compdb_project.py

echo "==> Syncing worktree to ${REMOTE_HOST}:${REMOTE_DIR}"
ssh "${REMOTE_HOST}" "mkdir -p '${REMOTE_DIR}'"
rsync -az --delete \
  --exclude '.worktrees/' \
  --exclude '.clion-remote/' \
  --exclude '.bazel-cache/' \
  --exclude 'build/' \
  --exclude 'output/' \
  --exclude 'results/' \
  --exclude '.codegraph/' \
  --exclude '.pytest_cache/' \
  --exclude '.cache/' \
  ./ "${REMOTE_HOST}:${REMOTE_DIR}/"

REMOTE_ENV=("DS_OPENSOURCE_DIR='${REMOTE_THIRDPARTY}'")
if [[ -n "${REMOTE_HTTP_PROXY}" ]]; then
  REMOTE_ENV+=("HTTP_PROXY='${REMOTE_HTTP_PROXY}'" "http_proxy='${REMOTE_HTTP_PROXY}'")
fi
if [[ -n "${REMOTE_HTTPS_PROXY}" ]]; then
  REMOTE_ENV+=("HTTPS_PROXY='${REMOTE_HTTPS_PROXY}'" "https_proxy='${REMOTE_HTTPS_PROXY}'")
fi
if [[ -n "${REMOTE_NO_PROXY}" ]]; then
  REMOTE_ENV+=("NO_PROXY='${REMOTE_NO_PROXY}'" "no_proxy='${REMOTE_NO_PROXY}'")
fi

echo "==> Building on ${REMOTE_HOST} with DS_OPENSOURCE_DIR=${REMOTE_THIRDPARTY}"
ssh "${REMOTE_HOST}" "git config --global --add safe.directory '${REMOTE_DIR}' >/dev/null 2>&1 || true; \
  cd '${REMOTE_DIR}' && ${REMOTE_ENV[*]} bash build.sh ${BUILD_ARGS[*]}"

echo "==> Pulling compile database and generated sources"
mkdir -p "${BUILD_DIR}" "${LOCAL_THIRDPARTY}"
rsync -az "${REMOTE_HOST}:${REMOTE_DIR}/${BUILD_DIR}/compile_commands.json" "${BUILD_DIR}/compile_commands.json.remote"
rsync -az --include '*/' --include '*.h' --include '*.hpp' --include '*.hh' \
  --include '*.cc' --include '*.cpp' --include '*.c' --include '*.cxx' \
  --exclude '*' "${REMOTE_HOST}:${REMOTE_DIR}/${BUILD_DIR}/" "${BUILD_DIR}/"
rsync -az --include '*/' --include 'include/***' --exclude '*' \
  "${REMOTE_HOST}:${REMOTE_THIRDPARTY}/" "${LOCAL_THIRDPARTY}/"

echo "==> Rewriting third-party include paths for local CLion indexing"
REMOTE_THIRDPARTY="${REMOTE_THIRDPARTY}" \
LOCAL_THIRDPARTY="${LOCAL_THIRDPARTY}" \
BUILD_DIR="${BUILD_DIR}" \
python3 scripts/rewrite_clion_compile_commands.py

ln -sfn "${BUILD_DIR}/compile_commands.json" compile_commands.json
python3 scripts/ensure_clion_compdb_project.py
echo "==> Ready: $(readlink compile_commands.json)"
