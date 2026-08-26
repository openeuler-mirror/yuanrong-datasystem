#!/usr/bin/env bash
set -euo pipefail

config_path="$1"
expected_value="$2"

grep -Fx "JEMALLOC_PROF_ENABLED = ${expected_value}" "${config_path}"
