#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

git_hash="$(git -C "${repo_root}" log -1 --pretty=format:'[%H] [%ai]' 2>/dev/null || true)"
git_branch="$(git -C "${repo_root}" symbolic-ref --short -q HEAD 2>/dev/null || true)"
if [[ -z "${git_branch}" ]]; then
  git_branch="$(git -C "${repo_root}" rev-parse --short HEAD 2>/dev/null || true)"
fi

if [[ -z "${git_hash}" ]]; then
  git_hash="[unknown]"
fi
if [[ -z "${git_branch}" ]]; then
  git_branch="unknown"
fi

printf 'STABLE_GIT_HASH %s\n' "${git_hash}"
printf 'STABLE_GIT_BRANCH %s\n' "${git_branch}"
