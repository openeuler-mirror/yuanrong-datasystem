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

#include "datasystem/common/util/sched_runtime.h"

#include <cerrno>
#include <linux/sched.h>
#include <linux/sched/types.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace datasystem {
namespace {
// Half of the kernel CFS base slice reduces scheduling latency while retaining
// the previous worker runtime value.
constexpr uint64_t SCHED_RUNTIME_NS = 1'400'000;
}  // namespace

uint64_t GetSchedRuntimeNs()
{
    return SCHED_RUNTIME_NS;
}

SetSchedRuntimeResult SetCurrentThreadSchedRuntime(bool enabled)
{
    if (!enabled) {
        return { false, true, 0 };
    }
    struct sched_attr attr = {};
    attr.size = SCHED_ATTR_SIZE_VER0;
    attr.sched_flags = 0;
    attr.sched_runtime = SCHED_RUNTIME_NS;
#ifdef __NR_sched_setattr
    auto ret = syscall(__NR_sched_setattr, 0, &attr, 0);
    if (ret != 0) {
        return { false, false, errno };
    }
    return { true, false, 0 };
#else
    return { false, false, ENOSYS };
#endif
}

}  // namespace datasystem
