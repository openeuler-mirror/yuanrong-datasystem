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
 * Description: RPC watch dispatcher implementation for coordinator server.
 */
#ifndef DATASYSTEM_COORDINATOR_WATCH_DISPATCHER_IMPL_H
#define DATASYSTEM_COORDINATOR_WATCH_DISPATCHER_IMPL_H

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/common/coordinator/watch_dispatcher.h"

namespace datasystem {
namespace coordinator {
class EventReqPb;
// rpcDispatched is true only immediately before HandleEvent on a validated stub. Any other error is local,
// unattributed evidence and must not be interpreted as a Worker application response.
struct WorkerReachabilityProbeResult {
    Status status;
    bool rpcDispatched{ false };
};

class WatchDispatcherImpl : public WatchDispatcher {
public:
    /**
     * @brief Construct an RPC dispatcher bound to one CoordinatorId.
     * @param[in] watchRegistry Registry that owns watch IDs.
     * @param[in] coordinatorId Immutable Coordinator process identity.
     */
    WatchDispatcherImpl(WatchRegistry *watchRegistry, std::string coordinatorId, size_t dispatchThreadCount)
        : WatchDispatcher(watchRegistry, BTHREAD_TAG_DEFAULT, dispatchThreadCount),
          coordinatorId_(std::move(coordinatorId))
    {
    }

    /**
     * @brief Destroy the RPC watch dispatcher after its base dispatcher has stopped.
     */
    ~WatchDispatcherImpl() override;

    // The absolute deadline covers endpoint parsing, stub acquisition/cast, and HandleEvent.
    virtual WorkerReachabilityProbeResult ProbeWorkerReachable(const std::string &watcherAddr,
                                                               std::chrono::steady_clock::time_point absoluteDeadline);

protected:
    /**
     * @brief Deliver a watch-event batch to one worker RPC endpoint.
     * @param[in] watchId Watch ID associated with the worker stream.
     * @param[in] watcherAddr Worker RPC endpoint.
     * @param[in] events Watch events to serialize and deliver.
     * @return Status of the worker notification RPC.
     */
    Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                    std::vector<std::shared_ptr<WatchEvent>> &events) override;

private:
    Status SendEventRequest(
        const std::string &watcherAddr, const EventReqPb &req, int32_t timeoutMs,
        std::chrono::steady_clock::time_point absoluteDeadline = std::chrono::steady_clock::time_point::max(),
        bool *rpcDispatched = nullptr);

    const std::string coordinatorId_;
};
}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_WATCH_DISPATCHER_IMPL_H
