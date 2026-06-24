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
 * Description: Internal service accessor for Worker services.
 *              Not part of the public API.
 */
#ifndef DATASYSTEM_WORKER_WORKER_SERVICE_ACCESSOR_H
#define DATASYSTEM_WORKER_WORKER_SERVICE_ACCESSOR_H

#include <atomic>

#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/worker_oc_server.h"
#include "datasystem/worker/worker_service_impl.h"

namespace datasystem {

namespace worker {

/**
 * @brief Internal accessor for Worker service pointers.
 *
 * Provides a singleton that internal modules (e.g. C API wrapper) use
 * to obtain service pointers without exposing them on the public Worker class.
 *
 * Thread-safety:
 *   - Register/Unregister are called from the Worker main thread during init/shutdown.
 *   - Get* methods are called from client request threads.
 *   - Atomic pointer provides lock-free read access.
 */
class WorkerServiceAccessor {
public:
    static WorkerServiceAccessor &Instance();

    /// @brief Register the server during Worker initialization.
    void Register(WorkerOCServer *server);

    /// @brief Unregister the server during Worker shutdown.
    void Unregister();

    /// @brief Get the WorkerServiceImpl pointer. Returns nullptr if not registered.
    WorkerServiceImpl *GetWorkerService();

    /// @brief Get the WorkerOCServiceImpl pointer. Returns nullptr if not registered.
    object_cache::WorkerOCServiceImpl *GetWorkerOCService();

private:
    WorkerServiceAccessor() = default;
    ~WorkerServiceAccessor() = default;
    WorkerServiceAccessor(const WorkerServiceAccessor &) = delete;
    WorkerServiceAccessor &operator=(const WorkerServiceAccessor &) = delete;

    std::atomic<WorkerOCServer *> server_{nullptr};
};

}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_WORKER_SERVICE_ACCESSOR_H
