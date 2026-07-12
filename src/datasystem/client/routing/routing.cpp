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

#include "datasystem/client/routing/routing.h"

namespace datasystem {
namespace client {

Routing::Routing(std::shared_ptr<WorkerRouter> router, std::shared_ptr<HashRingRefresher> refresher)
    : router_(std::move(router)), refresher_(std::move(refresher)) {}

Status Routing::Init(const std::string &hostId, const HostPort &initialWorkerAddr)
{
    (void)hostId;  // hostId is passed to WorkerRouter constructor by caller
    return refresher_->InitialFetch(initialWorkerAddr);
}

Status Routing::SelectWorker(const std::string &key, SelectStrategy strategy, HostPort &worker,
                             const std::vector<HostPort> &exclude)
{
    return router_->SelectWorker(key, strategy, worker, exclude);
}

Status Routing::SelectWorkers(const std::vector<std::string> &keys, SelectStrategy strategy,
                              std::unordered_map<HostPort, std::vector<std::string>> &groups)
{
    return router_->SelectWorkers(keys, strategy, groups);
}

void Routing::UpdateState(const HostPort &addr, StatusCode status)
{
    router_->UpdateState(addr, status);
}

void Routing::Shutdown()
{
    refresher_->Stop();
}

}  // namespace client
}  // namespace datasystem
