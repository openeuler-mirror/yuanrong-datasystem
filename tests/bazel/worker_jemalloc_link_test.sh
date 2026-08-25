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

set -euo pipefail

readonly WORKER="$1"
readonly JEMALLOC="$2"

jemalloc_needed_line=$(readelf -d "${WORKER}" | awk '/\(NEEDED\).*libjemalloc\.so\.2/ && !line {line=NR} END {print line}')
libc_needed_line=$(readelf -d "${WORKER}" | awk '/\(NEEDED\).*libc\.so/ && !line {line=NR} END {print line}')

[[ -n "${jemalloc_needed_line}" ]]
[[ -n "${libc_needed_line}" ]]
(( jemalloc_needed_line < libc_needed_line ))
readelf --dyn-syms --wide "${JEMALLOC}" | grep -E 'GLOBAL +DEFAULT +[0-9]+ +malloc$' >/dev/null
