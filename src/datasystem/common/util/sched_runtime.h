/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Linux scheduling runtime configuration for the calling thread.
 */
#ifndef DATASYSTEM_COMMON_UTIL_SCHED_RUNTIME_H
#define DATASYSTEM_COMMON_UTIL_SCHED_RUNTIME_H

#include <cstdint>

namespace datasystem {

struct SetSchedRuntimeResult {
    bool success;
    int err;
};

uint64_t GetSchedRuntimeNs();
SetSchedRuntimeResult SetCurrentThreadSchedRuntime();

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_SCHED_RUNTIME_H
