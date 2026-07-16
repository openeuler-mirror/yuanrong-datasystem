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

#include "datasystem/worker/coordinator/coordinator_watch_service_impl.h"

#include "datasystem/common/coordinator/watch_event.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
namespace coordinator {
namespace {
Status ConvertEventType(EventPb::EventType pbType, cluster::CoordinationEventType &type)
{
    switch (pbType) {
        case EventPb::PUT:
            type = cluster::CoordinationEventType::PUT;
            return Status::OK();
        case EventPb::DELETE:
            type = cluster::CoordinationEventType::DELETE;
            return Status::OK();
        case EventPb::RESET:
            type = cluster::CoordinationEventType::RESET;
            return Status::OK();
        default:
            RETURN_STATUS(StatusCode::K_INVALID, "unknown coordinator watch event type");
    }
}

Status DecodeEvent(const EventPb &pbEvent, cluster::CoordinationEvent &event)
{
    RETURN_IF_NOT_OK(ConvertEventType(pbEvent.type(), event.type));
    CHECK_FAIL_RETURN_STATUS(event.type == cluster::CoordinationEventType::RESET || !pbEvent.kv().key().empty(),
                             K_INVALID, "coordinator watch event key is empty");
    event.key = pbEvent.kv().key();
    event.value = pbEvent.kv().value();
    event.version = pbEvent.kv().version();
    event.revision = pbEvent.kv().mod_revision();
    return Status::OK();
}
}  // namespace

Status CoordinatorWatchServiceImpl::HandleEvent(const EventReqPb &req, EventRspPb &rsp)
{
    (void)rsp;
    CHECK_FAIL_RETURN_STATUS(req.coordinator_id().size() == UUID_SIZE && req.watch_id() > 0 && !req.events().empty(),
                             K_INVALID, "invalid coordinator watch batch identity");
    CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(req.events_size()) <= MAX_WATCH_EVENTS_PER_BATCH
                                 && req.ByteSizeLong() <= MAX_WATCH_EVENT_BATCH_BYTES,
                             K_INVALID, "coordinator watch batch exceeds protocol limit");
    for (const auto &pbEvent : req.events()) {
        cluster::CoordinationEventType type;
        RETURN_IF_NOT_OK(ConvertEventType(pbEvent.type(), type));
        CHECK_FAIL_RETURN_STATUS(type == cluster::CoordinationEventType::RESET || !pbEvent.kv().key().empty(),
                                 K_INVALID, "coordinator watch event key is empty");
    }
    EventHandler eventHandler;
    {
        std::lock_guard<std::mutex> lock(handlerMutex_);
        eventHandler = eventHandler_;
    }
    CHECK_FAIL_RETURN_STATUS(eventHandler != nullptr, StatusCode::K_NOT_READY,
                             "coordinator watch handler is not bound");

    for (const auto &pbEvent : req.events()) {
        cluster::CoordinationEvent event;
        RETURN_IF_NOT_OK(DecodeEvent(pbEvent, event));
        VLOG(1) << "HandleEvent watchId: " << req.watch_id() << " event: " << event.ToString();
        RETURN_IF_NOT_OK(eventHandler(req.coordinator_id(), req.watch_id(), std::move(event)));
    }
    return Status::OK();
}
}  // namespace coordinator
}  // namespace datasystem
