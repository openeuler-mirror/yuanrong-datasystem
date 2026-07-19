def _git_version_header_impl(ctx):
    out = ctx.outputs.out
    ctx.actions.run_shell(
        inputs = [ctx.info_file, ctx.version_file],
        outputs = [out],
        arguments = [ctx.info_file.path, ctx.version_file.path, out.path],
        command = r"""
set -euo pipefail

stable_status_file="$1"
volatile_status_file="$2"
out_file="$3"
git_hash="unknown"
git_branch="unknown"

read_status_file() {
  local status_file="$1"
  [[ -f "${status_file}" ]] || return 0
  while IFS= read -r line; do
    key="${line%% *}"
    value="${line#* }"
    if [[ "${key}" == "${value}" ]]; then
      value=""
    fi
    case "${key}" in
      STABLE_GIT_HASH)
        git_hash="${value}"
        ;;
      STABLE_GIT_BRANCH)
        git_branch="${value}"
        ;;
    esac
  done < "${status_file}"
}

read_status_file "${stable_status_file}"
read_status_file "${volatile_status_file}"

escape_cpp_string() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

{
  printf '#ifndef DATASYSTEM_COMMON_UTIL_GIT_VERSION_DEF_H\n'
  printf '#define DATASYSTEM_COMMON_UTIL_GIT_VERSION_DEF_H\n\n'
  printf '#define GIT_HASH "%s"\n' "$(escape_cpp_string "${git_hash}")"
  printf '#define GIT_BRANCH "%s"\n\n' "$(escape_cpp_string "${git_branch}")"
  printf '#endif  // DATASYSTEM_COMMON_UTIL_GIT_VERSION_DEF_H\n'
} > "${out_file}"
""",
        mnemonic = "GitVersionHeader",
    )

git_version_header = rule(
    implementation = _git_version_header_impl,
    attrs = {
        "out": attr.output(mandatory = True),
    },
)


def _commit_id_file_impl(ctx):
    out = ctx.outputs.out
    ctx.actions.run_shell(
        inputs = [ctx.info_file, ctx.version_file],
        outputs = [out],
        arguments = [ctx.info_file.path, ctx.version_file.path, out.path],
        command = r"""
set -euo pipefail

stable_status_file="$1"
volatile_status_file="$2"
out_file="$3"
git_hash="unknown"

read_status_file() {
  local status_file="$1"
  [[ -f "${status_file}" ]] || return 0
  while IFS= read -r line; do
    key="${line%% *}"
    value="${line#* }"
    if [[ "${key}" == "${value}" ]]; then
      value=""
    fi
    case "${key}" in
      STABLE_GIT_HASH)
        git_hash="${value}"
        ;;
    esac
  done < "${status_file}"
}

read_status_file "${stable_status_file}"
read_status_file "${volatile_status_file}"

printf "__commit_id__ = '%s'\n" "${git_hash}" > "${out_file}"
""",
        mnemonic = "CommitIdFile",
    )

commit_id_file = rule(
    implementation = _commit_id_file_impl,
    attrs = {
        "out": attr.output(mandatory = True),
    },
)
