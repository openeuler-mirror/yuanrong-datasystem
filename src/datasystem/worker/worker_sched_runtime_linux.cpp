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

#include <cerrno>
#include <cstdint>
#include <sys/syscall.h>
#include <unistd.h>
#include <linux/sched.h>
#include <linux/sched/types.h>

struct SetSchedRuntimeResult {
    bool success;
    int err;
};

namespace {
// Use half of /sys/kernel/debug/sched/base_slice_ns to reduce worker scheduling latency while keeping the value
// tied to the kernel CFS base slice for future tuning.
constexpr uint64_t WORKER_SCHED_RUNTIME_NS = 1'400'000;
}

uint64_t GetWorkerSchedRuntimeNs()
{
    return WORKER_SCHED_RUNTIME_NS;
}

SetSchedRuntimeResult SetWorkerSchedRuntime()
{
    struct sched_attr attr = {};
    attr.size = SCHED_ATTR_SIZE_VER0;
    attr.sched_flags = 0;
    attr.sched_runtime = WORKER_SCHED_RUNTIME_NS;
#ifdef __NR_sched_setattr
    auto ret = syscall(__NR_sched_setattr, 0, &attr, 0);
    if (ret != 0) {
        return { false, errno };
    }
    return { true, 0 };
#else
    return { false, ENOSYS };
#endif
}
