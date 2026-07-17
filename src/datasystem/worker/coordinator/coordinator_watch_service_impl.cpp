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
#include "datasystem/common/util/raii.h"
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

Status CoordinatorWatchServiceImpl::BindEventHandler(EventHandler eventHandler)
{
    CHECK_FAIL_RETURN_STATUS(eventHandler != nullptr, K_INVALID, "Coordinator watch handler is empty");
    std::lock_guard<std::mutex> lock(handlerMutex_);
    CHECK_FAIL_RETURN_STATUS(eventHandler_ == nullptr, K_INVALID, "Coordinator watch handler is already bound");
    CHECK_FAIL_RETURN_STATUS(activeHandlerCount_ == 0, K_NOT_READY, "Previous Coordinator watch handler is draining");
    eventHandler_ = std::move(eventHandler);
    VLOG(1) << "CLUSTER_WATCH ingress handler bound";
    return Status::OK();
}

Status CoordinatorWatchServiceImpl::UnbindEventHandlerAndWait(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(handlerMutex_);
    eventHandler_ = nullptr;
    if (!handlerDrained_.wait_until(lock, deadline, [this] { return activeHandlerCount_ == 0; })) {
        LOG(WARNING) << "CLUSTER_WATCH ingress drain deadline exceeded, active_handlers=" << activeHandlerCount_;
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "Coordinator watch handler drain deadline exceeded");
    }
    VLOG(1) << "CLUSTER_WATCH ingress handler unbound and drained";
    return Status::OK();
}

Status CoordinatorWatchServiceImpl::AcquireEventHandler(EventHandler &eventHandler)
{
    std::lock_guard<std::mutex> lock(handlerMutex_);
    CHECK_FAIL_RETURN_STATUS(eventHandler_ != nullptr, StatusCode::K_NOT_READY,
                             "Coordinator watch handler is not bound");
    eventHandler = eventHandler_;
    ++activeHandlerCount_;
    return Status::OK();
}

void CoordinatorWatchServiceImpl::ReleaseEventHandler()
{
    std::lock_guard<std::mutex> lock(handlerMutex_);
    --activeHandlerCount_;
    if (activeHandlerCount_ == 0) {
        handlerDrained_.notify_all();
    }
}

Status CoordinatorWatchServiceImpl::DeliverEventBatch(const EventReqPb &req, const EventHandler &eventHandler)
{
    Raii releaseHandler([this] { ReleaseEventHandler(); });
    Status result = Status::OK();
    for (const auto &pbEvent : req.events()) {
        cluster::CoordinationEvent event;
        result = DecodeEvent(pbEvent, event);
        if (result.IsError()) {
            break;
        }
        VLOG(1) << "CLUSTER_WATCH watch_id=" << req.watch_id() << " event=" << event.ToString();
        result = eventHandler(req.coordinator_id(), req.watch_id(), std::move(event));
        if (result.IsError()) {
            break;
        }
    }
    return result;
}

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
    RETURN_IF_NOT_OK(AcquireEventHandler(eventHandler));
    return DeliverEventBatch(req, eventHandler);
}
}  // namespace coordinator
}  // namespace datasystem
