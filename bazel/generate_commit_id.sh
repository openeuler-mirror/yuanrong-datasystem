#!/usr/bin/env bash
# Generates yr/datasystem/.commit_id for Bazel wheel packaging.
# Format matches python/setup.py build_depends() and CMake GIT_HASH / COMMIT_ID.
set -euo pipefail

version_file="${1:?version file path required}"
out_file="${2:?output path required}"
repo_root="$(dirname "$(realpath "${version_file}")")"

commit="${COMMIT_ID:-}"
if [[ -z "${commit}" ]]; then
  commit="$(git -C "${repo_root}" log -1 --pretty=format:'[%H] [%ai]' 2>/dev/null || true)"
fi
if [[ -z "${commit}" ]]; then
  commit="[unknown]"
fi

printf "__commit_id__ = '%s'\n" "${commit}" > "${out_file}"
