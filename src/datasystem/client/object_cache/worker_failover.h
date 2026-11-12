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

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_WORKER_FAILOVER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_WORKER_FAILOVER_H

#include "datasystem/client/object_cache/object_client_impl.h"

namespace datasystem {
namespace object_cache {

class WorkerFailover {
public:
    explicit WorkerFailover(ObjectClientImpl &owner);
    ~WorkerFailover() = default;

    void ConfigureUbHealthCallback(const std::shared_ptr<IClientWorkerApi> &workerApi);
    Status ProcessWorkerLost(client::WorkerRecoveryReason reason);
    void ProcessWorkerTimeout();

private:
    Status RegisterWorkerAfterWorkerLost(client::WorkerRecoveryReason reason);
    Status RebuildWorkerShm();
    void CleanupWorkerShmAfterWorkerLost();

    ObjectClientImpl &owner_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_WORKER_FAILOVER_H
