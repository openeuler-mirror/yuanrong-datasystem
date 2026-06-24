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
 * Description: Implementation of WorkerServiceAccessor.
 */
#include "datasystem/worker/worker_service_accessor.h"

#include "datasystem/worker/worker_oc_server.h"
#include "datasystem/worker/worker_service_impl.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

namespace datasystem {
namespace worker {

WorkerServiceAccessor &WorkerServiceAccessor::Instance()
{
    static WorkerServiceAccessor instance;
    return instance;
}

void WorkerServiceAccessor::Register(WorkerOCServer *server)
{
    server_.store(server, std::memory_order_release);
}

void WorkerServiceAccessor::Unregister()
{
    server_.store(nullptr, std::memory_order_release);
}

WorkerServiceImpl *WorkerServiceAccessor::GetWorkerService()
{
    auto *s = server_.load(std::memory_order_acquire);
    if (s == nullptr) {
        return nullptr;
    }
    return s->GetWorkerService();
}

object_cache::WorkerOCServiceImpl *WorkerServiceAccessor::GetWorkerOCService()
{
    auto *s = server_.load(std::memory_order_acquire);
    if (s == nullptr) {
        return nullptr;
    }
    return s->GetWorkerOCService();
}

}  // namespace worker
}  // namespace datasystem
