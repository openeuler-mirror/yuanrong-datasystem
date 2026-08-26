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

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_EXECUTION_POLICY_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_EXECUTION_POLICY_H

#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

// brpc is the only RPC transport: service handlers run on the caller thread
// (fanout would join worker tasks with std::future::get/wait, which is not used).
inline bool ShouldUseServiceThreadPoolFanout()
{
    return false;
}

template <typename Iterator, typename TaskFn>
Status RunServiceTasksSerially(Iterator begin, Iterator end, TaskFn &&taskFn)
{
    Status firstRc;
    for (auto iter = begin; iter != end; ++iter) {
        Status taskRc = taskFn(*iter);
        if (firstRc.IsOk() && taskRc.IsError()) {
            firstRc = taskRc;
        }
    }
    return firstRc;
}

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_EXECUTION_POLICY_H
