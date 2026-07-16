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

#include <chrono>
#include <condition_variable>
#include <cstddef>
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
     * @brief Release the callback service after RPC ingress is stopped.
     */
    ~CoordinatorWatchServiceImpl() override = default;

    /**
     * @brief Bind the identity-aware route before Coordinator watch registration starts.
     * @param[in] eventHandler Non-empty event route to consume.
     * @return K_OK on success; K_INVALID when already bound; K_NOT_READY while an old route is draining.
     */
    Status BindEventHandler(EventHandler eventHandler);

    /**
     * @brief Reject new deliveries and wait for every in-flight route invocation.
     * @param[in] deadline Absolute drain deadline.
     * @return K_OK after drain or K_RPC_DEADLINE_EXCEEDED when invocations remain.
     */
    Status UnbindEventHandlerAndWait(std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Validate a complete callback batch before delivering any event.
     * @param[in] req CoordinatorId, watch identity, and event batch.
     * @param[out] rsp Empty success response.
     * @return K_OK after delivery or a protocol/lifecycle status.
     */
    Status HandleEvent(const EventReqPb &req, EventRspPb &rsp) override;

private:
    /**
     * @brief Acquire one stable event route and register the invocation as in flight.
     * @param[out] eventHandler Stable route copy; unchanged on failure.
     * @return K_OK on success or K_NOT_READY while unbound.
     */
    Status AcquireEventHandler(EventHandler &eventHandler);

    /**
     * @brief Release one in-flight event route invocation.
     */
    void ReleaseEventHandler();

    /**
     * @brief Decode and deliver one fully validated batch under an acquired route lease.
     * @param[in] req Validated Coordinator event batch.
     * @param[in] eventHandler Stable route copy acquired for this batch.
     * @return First decoding or route status, or K_OK after ordered delivery.
     */
    Status DeliverEventBatch(const EventReqPb &req, const EventHandler &eventHandler);

    // Protects eventHandler_ and activeHandlerCount_; handlerDrained_ observes the same predicate.
    std::mutex handlerMutex_;
    std::condition_variable handlerDrained_;
    EventHandler eventHandler_;
    size_t activeHandlerCount_{ 0 };
};
}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_COORDINATOR_COORDINATOR_WATCH_SERVICE_IMPL_H
