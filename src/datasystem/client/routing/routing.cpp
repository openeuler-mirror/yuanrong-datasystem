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

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

Routing::Routing(std::shared_ptr<WorkerRouter> router, std::shared_ptr<HashRingRefresher> refresher,
                 int64_t refreshIntervalMs)
    : router_(std::move(router)), refresher_(std::move(refresher)), refreshIntervalMs_(refreshIntervalMs)
{
}

Routing::~Routing()
{
    Shutdown();
}

Status Routing::Init(const std::string &hostId, const HostPort &initialWorkerAddr)
{
    RETURN_RUNTIME_ERROR_IF_NULL(router_);
    RETURN_RUNTIME_ERROR_IF_NULL(refresher_);
    CHECK_FAIL_RETURN_STATUS(!initialWorkerAddr.Empty(), K_INVALID, "Initial worker address must not be empty");
    CHECK_FAIL_RETURN_STATUS(!initialized_.load(), K_INVALID, "Routing is already initialized");

    router_->SetHostId(hostId);
    RETURN_IF_NOT_OK(refresher_->InitialFetch(initialWorkerAddr));
    RETURN_IF_NOT_OK(refresher_->StartPeriodicRefresh(refreshIntervalMs_));
    initialized_.store(true);
    return Status::OK();
}

Status Routing::SelectWorker(const std::string &key, SelectStrategy strategy, HostPort &worker,
                             const std::vector<HostPort> &exclude)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(), K_NOT_READY, "Routing is not initialized");
    return router_->SelectWorker(key, strategy, worker, exclude);
}

Status Routing::SelectWorkers(const std::vector<std::string> &keys, SelectStrategy strategy,
                              std::unordered_map<HostPort, std::vector<std::string>> &groups)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(), K_NOT_READY, "Routing is not initialized");
    return router_->SelectWorkers(keys, strategy, groups);
}

void Routing::UpdateState(const HostPort &addr, StatusCode status)
{
    if (initialized_.load()) {
        router_->UpdateState(addr, status);
    }
}

void Routing::Shutdown()
{
    if (refresher_ != nullptr) {
        refresher_->Stop();
    }
    initialized_.store(false);
}

}  // namespace client
}  // namespace datasystem
