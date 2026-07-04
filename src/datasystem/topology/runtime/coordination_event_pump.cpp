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
 * Description: Lifecycle guard for topology coordination events.
 */
#include "datasystem/topology/runtime/coordination_event_pump.h"

#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

Status CoordinationEventPump::Start(EventHandler handler, RebuildHandler rebuildHandler)
{
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(handler), K_INVALID, "topology event handler is empty");
    std::lock_guard<std::mutex> lock(mutex_);
    handler_ = std::move(handler);
    rebuildHandler_ = std::move(rebuildHandler);
    running_ = true;
    stats_.running = true;
    stats_.lastStatus = Status::OK();
    LOG(INFO) << "Topology event pump started.";
    return Status::OK();
}

void CoordinationEventPump::Shutdown()
{
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = false;
    handler_ = EventHandler();
    rebuildHandler_ = RebuildHandler();
    stats_.running = false;
    LOG(INFO) << "Topology event pump stopped.";
}

Status CoordinationEventPump::Submit(CoordinationEvent &&event)
{
    EventHandler handler;
    RebuildHandler rebuildHandler;
    bool needsRebuild = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ || !handler_) {
            ++stats_.ignoredEvents;
            RETURN_STATUS(K_NOT_READY,
                          "topology event pump is stopped, event revision: " + std::to_string(event.revision));
        }
        ++stats_.receivedEvents;
        if (event.revision > 0 && stats_.lastRevision > 0 && event.revision > stats_.lastRevision + 1) {
            ++stats_.gapEvents;
            needsRebuild = true;
            LOG(WARNING) << "Topology watch revision gap detected, lastRevision: " << stats_.lastRevision
                         << ", eventRevision: " << event.revision;
        }
        if (event.revision > 0 && event.revision <= stats_.lastRevision) {
            ++stats_.outOfOrderEvents;
            needsRebuild = true;
            LOG(WARNING) << "Topology watch out-of-order event detected, lastRevision: " << stats_.lastRevision
                         << ", eventRevision: " << event.revision;
        }
        if (event.revision > stats_.lastRevision) {
            stats_.lastRevision = event.revision;
        }
        handler = handler_;
        rebuildHandler = rebuildHandler_;
    }

    Status rc;
    if (needsRebuild) {
        rc = static_cast<bool>(rebuildHandler) ? rebuildHandler()
                                               : Status(K_TRY_AGAIN, "topology rebuild handler is empty");
    } else {
        rc = handler(std::move(event));
    }
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.lastStatus = rc;
    if (rc.IsOk()) {
        ++stats_.appliedEvents;
    } else {
        ++stats_.failedEvents;
        LOG(WARNING) << "Topology event handling failed, status: " << rc.ToString();
    }
    return rc;
}

CoordinationEventPumpStats CoordinationEventPump::GetStats() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto stats = stats_;
    stats.running = running_;
    return stats;
}

}  // namespace topology
}  // namespace datasystem
