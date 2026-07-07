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
 * Description: Worker-side coordinator watch RPC service implementation skeleton.
 */
#ifndef DATASYSTEM_WORKER_COORDINATOR_COORDINATOR_WATCH_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_COORDINATOR_COORDINATOR_WATCH_SERVICE_IMPL_H

#include <utility>

#include "datasystem/protos/coordinator.irpc.pb.h"
#include "datasystem/protos/coordinator.service.rpc.pb.h"
#include "datasystem/worker/cluster_manager/cluster_manager.h"

namespace datasystem {
namespace coordinator {
class CoordinatorWatchServiceImpl : public CoordinatorWatchService, public ICoordinatorWatchService {
public:
    explicit CoordinatorWatchServiceImpl(HostPort localAddress) : CoordinatorWatchService(std::move(localAddress))
    {
    }

    CoordinatorWatchServiceImpl(HostPort localAddress, ClusterManager *clusterManager)
        : CoordinatorWatchService(std::move(localAddress)), clusterManager_(clusterManager)
    {
    }
    ~CoordinatorWatchServiceImpl() override = default;

    void SetClusterManager(ClusterManager *clusterManager)
    {
        clusterManager_ = clusterManager;
    }

    Status HandleEvent(const EventReqPb &req, EventRspPb &rsp) override;

private:
    ClusterManager *clusterManager_ = nullptr;
};
}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_COORDINATOR_COORDINATOR_WATCH_SERVICE_IMPL_H
