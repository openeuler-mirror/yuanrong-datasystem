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

#include "datasystem/worker/coordinator/coordinator_watch_service_impl.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace coordinator {
namespace {
Status ConvertEventType(EventPb::EventType pbType, topology::CoordinationEventType &type)
{
    switch (pbType) {
        case EventPb::PUT:
            type = topology::CoordinationEventType::PUT;
            return Status::OK();
        case EventPb::DELETE:
            type = topology::CoordinationEventType::DELETE;
            return Status::OK();
        default:
            RETURN_STATUS(StatusCode::K_INVALID, "unknown coordinator watch event type");
    }
}
}  // namespace

Status CoordinatorWatchServiceImpl::HandleEvent(const EventReqPb &req, EventRspPb &rsp)
{
    (void)rsp;
    CHECK_FAIL_RETURN_STATUS(clusterManager_ != nullptr, StatusCode::K_NOT_READY,
                             "coordinator watch handler is not bound");

    for (const auto &pbEvent : req.events()) {
        topology::CoordinationEvent event;
        RETURN_IF_NOT_OK(ConvertEventType(pbEvent.type(), event.type));
        event.key = pbEvent.kv().key();
        event.value = pbEvent.kv().value();
        event.version = pbEvent.kv().version();
        event.revision = pbEvent.kv().mod_revision();
        VLOG(1) << "HandleEvent watchId: " << req.watch_id() << " event: " << event.ToString();
        clusterManager_->EnqueueExternalEvent(std::move(event));
    }
    return Status::OK();
}
}  // namespace coordinator
}  // namespace datasystem
