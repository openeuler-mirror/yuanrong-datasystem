/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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

#include <functional>
#include <mutex>
#include <utility>

#include "datasystem/protos/coordinator.irpc.pb.h"
#include "datasystem/protos/coordinator.service.rpc.pb.h"
#include "datasystem/cluster/coordination_backend/coordination_backend.h"

namespace datasystem {
namespace coordinator {
class CoordinatorWatchServiceImpl : public CoordinatorWatchService, public ICoordinatorWatchService {
public:
    using EventHandler =
        std::function<Status(const std::string &coordinatorId, int64_t watchId, cluster::CoordinationEvent &&event)>;

    /**
     * @brief Construct a callback service before its event route is bound.
     * @param[in] localAddress Worker callback address.
     */
    explicit CoordinatorWatchServiceImpl(HostPort localAddress) : CoordinatorWatchService(std::move(localAddress))
    {
    }

    /**
     * @brief Construct a callback service with an identity-aware event route.
     * @param[in] localAddress Worker callback address.
     * @param[in] eventHandler CoordinatorId and watch-bound event route.
     */
    CoordinatorWatchServiceImpl(HostPort localAddress, EventHandler eventHandler)
        : CoordinatorWatchService(std::move(localAddress)), eventHandler_(std::move(eventHandler))
    {
    }

    /**
     * @brief Release the callback service after RPC ingress is stopped.
     */
    ~CoordinatorWatchServiceImpl() override = default;

    /**
     * @brief Replace the event route during composition or shutdown.
     * @param[in] eventHandler New identity-aware event route; empty disables delivery.
     */
    void SetEventHandler(EventHandler eventHandler)
    {
        std::lock_guard<std::mutex> lock(handlerMutex_);
        eventHandler_ = std::move(eventHandler);
    }

    /**
     * @brief Validate a complete callback batch before delivering any event.
     * @param[in] req CoordinatorId, watch identity, and event batch.
     * @param[out] rsp Empty success response.
     * @return K_OK after delivery or a protocol/lifecycle status.
     */
    Status HandleEvent(const EventReqPb &req, EventRspPb &rsp) override;

private:
    // Protects eventHandler_.
    std::mutex handlerMutex_;
    EventHandler eventHandler_;
};
}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_COORDINATOR_COORDINATOR_WATCH_SERVICE_IMPL_H
