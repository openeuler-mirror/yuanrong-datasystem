/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Code to manage npu subscribing events.
 */
#include "datasystem/master/object_cache/device/master_dev_npu_events.h"
#include <shared_mutex>

namespace datasystem {
namespace master {
NpuEvent::NpuEvent(const SubscribeEventTypePb &eventType, const std::string &objectKey, const std::string &dstClientId,
                   int32_t dstDeviceId, bool isSameNode)
    : eventType(eventType),
      objectKey(objectKey),
      dstClientId(dstClientId),
      dstDeviceId(dstDeviceId),
      isSameNode(isSameNode)
{
}

NpuEvent::NpuEvent(const SubscribeEventTypePb &eventType, const std::string &objectKey, const std::string &dstClientId,
                   int32_t dstDeviceId, bool isSameNode, int32_t srcOffset, int32_t length)
    : eventType(eventType),
      objectKey(objectKey),
      dstClientId(dstClientId),
      dstDeviceId(dstDeviceId),
      isSameNode(isSameNode),
      srcOffset(srcOffset),
      length(length)
{
}

std::shared_ptr<NpuEvent> NpuEvent::CreateCommExitNotification(const std::string &dstClientId, int32_t dstDeviceId)
{
    return std::make_shared<NpuEvent>(SubscribeEventTypePb::COMM_DESTROY_NOTIFICATION, "DestroyEvent", dstClientId,
                                      dstDeviceId, false);
}

std::shared_ptr<NpuEvent> NpuEvent::CreateGetNotification(const std::string &objectKey, const std::string &dstClientId,
                                                          int32_t dstDeviceId, bool isSameNode, int32_t srcOffset,
                                                          int32_t length)
{
    return std::make_shared<NpuEvent>(SubscribeEventTypePb::GET_NOTIFICATION, objectKey, dstClientId, dstDeviceId,
                                      isSameNode, srcOffset, length);
}

std::shared_ptr<NpuEvent> NpuEvent::CreateLifeTimeExitNotification(const std::string &objectKey)
{
    return std::make_shared<NpuEvent>(SubscribeEventTypePb::LIFECYCLE_EXIT_NOTIFICATION, objectKey, "", -1, false);
}

std::shared_ptr<NpuEvent> NpuEvent::CreateSubscribeCancelNotification()
{
    return std::make_shared<NpuEvent>(SubscribeEventTypePb::SUBSCRIBE_CANCEL_NOTIFICATION, "", "", -1, false);
}

std::shared_ptr<NpuEvent> NpuEvent::FromPb(const SubscribeReceiveEventRspPb &pb)
{
    // only applicable to expansion and contraction capacity matching
    if (!pb.npuevents().empty()) {
        auto npuEvent = pb.npuevents(0);
        return std::make_shared<NpuEvent>(npuEvent.event_type(), npuEvent.object_key(), npuEvent.dst_client_id(),
                                          npuEvent.dst_device_id(), npuEvent.is_same_node());
    }
    return nullptr;
}

SubscribeReceiveEventRspPb NpuEvent::ToPb()
{
    SubscribeReceiveEventRspPb pb;
    auto event = pb.add_npuevents();
    event->set_object_key(objectKey);
    event->set_dst_client_id(dstClientId);
    event->set_dst_device_id(dstDeviceId);
    event->set_is_same_node(isSameNode);
    return pb;
}

void NpuEventsTable::PutNpuEvent(const std::string &srcNpuId, std::shared_ptr<NpuEvent> npuEvent)
{
    std::shared_lock<std::shared_timed_mutex> lock(npuEventTableMutex_);
    TbbSrcNpuGetReqsTable::accessor npuPendingGetReqsAccess;
    LOG(INFO) << FormatString("[PutNpuEvent] Update subscribe event of object: %s for src npu (%s), event_type:(%d)",
                              npuEvent->objectKey, srcNpuId, static_cast<int>(npuEvent->eventType));
    (void)npuPendingGetReqsTable_.insert(npuPendingGetReqsAccess,
                                         std::make_pair<ImmutableString, QueuePtr>(
                                             srcNpuId, std::make_shared<BlockingQueue<std::shared_ptr<NpuEvent>>>()));
    npuPendingGetReqsAccess->second->Push(npuEvent);
}

void NpuEventsTable::GetNpuEvents(const std::string &srcNpuId,
                                  const std::function<void(const QueuePtr &)> &onGetCallback)
{
    std::shared_lock<std::shared_timed_mutex> lock(npuEventTableMutex_);
    TbbSrcNpuGetReqsTable::accessor srcNPUGetReqsAcc;
    if (npuPendingGetReqsTable_.find(srcNPUGetReqsAcc, srcNpuId) && !srcNPUGetReqsAcc->second->Empty()) {
        onGetCallback(srcNPUGetReqsAcc->second);
    }
}

void NpuEventsTable::EraseNpuEvent(const std::string &npuId)
{
    std::shared_lock<std::shared_timed_mutex> lock(npuEventTableMutex_);
    TbbSrcNpuGetReqsTable::accessor npuGetReqsAcc;
    if (npuPendingGetReqsTable_.find(npuGetReqsAcc, npuId)) {
        npuPendingGetReqsTable_.erase(npuGetReqsAcc);
    }
    return;
}

Status NpuEventsSubscriptionTable::AddSubscribeReceiveEventRequest(
    const std::string &objectKey, std::shared_ptr<SubscribeReceiveEventRequest> &request)
{
    return subscribeReceiveEventRequestTable_.AddRequest(objectKey, request);
}

void NpuEventsSubscriptionTable::RemoveSubscribeReceiveEventRequest(
    std::shared_ptr<SubscribeReceiveEventRequest> &request)
{
    return subscribeReceiveEventRequestTable_.RemoveRequest(request);
}

Status NpuEventsSubscriptionTable::UpdateNpuPendingGetReqsTableForSuccess(const std::string &srcNpuId,
                                                                          const QueuePtr &queue)
{
    bool isUpdateSubRecvEventRequest = true;
    return subscribeReceiveEventRequestTable_.UpdateRequest(
        srcNpuId, queue, Status::OK(),
        [this](std::shared_ptr<SubscribeReceiveEventRequest> req) {
            LOG_IF_ERROR(ReturnFromSubscribeReceiveEventRequest(req), "ReturnFromSubscribeReceiveEventRequest failed");
        },
        nullptr, isUpdateSubRecvEventRequest);
}

void SubscribeReceiveEventRspAddNpuEvent(SubscribeReceiveEventRspPb &resp, std::shared_ptr<NpuEvent> event)
{
    auto npuevent = resp.add_npuevents();
    npuevent->set_object_key(event->objectKey);
    npuevent->set_dst_client_id(event->dstClientId);
    npuevent->set_dst_device_id(event->dstDeviceId);
    npuevent->set_event_type(event->eventType);
    npuevent->set_is_same_node(event->isSameNode);
    npuevent->set_src_offset(event->srcOffset);
    npuevent->set_length(event->length);
}

Status NpuEventsSubscriptionTable::ReturnFromSubscribeReceiveEventRequest(
    std::shared_ptr<SubscribeReceiveEventRequest> request)
{
    RETURN_RUNTIME_ERROR_IF_NULL(request);
    bool expected = false;
    RETURN_OK_IF_TRUE(!request->isReturn_.compare_exchange_strong(expected, true));

    LOG(INFO) << FormatString("Begin to ReturnFromSubscribeReceiveEventRequest, client id: %d, deviceId: %d",
                              request->clientId_, request->deviceId_);
    std::lock_guard<std::mutex> lck(request->mutex_);
    int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
    if (remainingTimeMs <= 0) {
        RemoveSubscribeReceiveEventRequest(request);
        return request->serverApi_->SendStatus({ K_RPC_DEADLINE_EXCEEDED, "Rpc timeout" });
    }

    SubscribeReceiveEventRspPb resp;
    for (auto &objectKey : request->rawObjectKeys_) {
        SubscribeReceiveEventRequest::TbbGetObjsTable::const_accessor accessor;
        bool isFindObj = false;

        if (request->objects_.find(accessor, objectKey) && accessor->second != nullptr && !accessor->second->Empty()) {
            std::shared_ptr<NpuEvent> event;
            while (!accessor->second->Empty()) {
                Status rc = accessor->second->Pop(event);
                if (rc.IsOk()) {
                    SubscribeReceiveEventRspAddNpuEvent(resp, event);
                    isFindObj = true;
                } else {
                    LOG(ERROR) << "objectKey: " << objectKey
                               << " pop SubscribeReceiveEventRequest table have err in "
                                  "ReturnFromSubscribeReceiveEventRequest with : "
                               << rc;
                    break;
                }
            }
        }
        if (!isFindObj) {
            LOG(WARNING) << FormatString("Can't find object: %s, clientId: %s, deviceId: %s", objectKey,
                                         request->clientId_, request->deviceId_);
        } else {
            // The current subscription history logic supports the return of only one successful subscription object.
            // Therefore, if one object meets the requirement, the object is returned immediately and does not need to
            // be queried. In the future, multiple success objects should be returned for one subscription.
            break;
        }
    }
    RemoveSubscribeReceiveEventRequest(request);

    if (request->timer_ != nullptr) {
        if (!TimerQueue::GetInstance()->Cancel(*(request->timer_))) {
            LOG(ERROR) << "Failed to Cancel the timer: " << request->timer_->GetId();
        }
        request->timer_.reset();
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(request->serverApi_->Write(resp), "Write reply to client stream failed.");
    return Status::OK();
}

bool NpuEventsSubscriptionTable::SrcNpuIdInSubscribe(const std::string &srcNpuId)
{
    return subscribeReceiveEventRequestTable_.ObjectInRequest(srcNpuId);
}

void NpuEventsSubscriptionTable::EraseNpuSubscribe(const std::string &NpuId)
{
    subscribeReceiveEventRequestTable_.EraseSub(NpuId);
}

void NpuEventsTable::FillMigrateData(MigrateMetadataReqPb &req)
{
    std::unique_lock<std::shared_timed_mutex> lock(npuEventTableMutex_);
    for (const auto &iter : npuPendingGetReqsTable_) {
        auto npuEventListPb = req.add_npu_events_metas();
        npuEventListPb->set_npu_id(iter.first);
        std::vector<std::shared_ptr<NpuEvent>> npuEventList;
        npuEventList.reserve(iter.second->Size());
        while (!iter.second->Empty()) {
            std::shared_ptr<NpuEvent> event;
            if (iter.second->Pop(event).IsOk() && event != nullptr) {
                npuEventList.push_back(event);
                npuEventListPb->mutable_npu_events()->Add(event->ToPb());
            }
        }
        for (auto &event : npuEventList) {
            iter.second->Push(event);
        }
    }
}
void NpuEventsTable::SaveMigrateData(const MigrateMetadataReqPb &req)
{
    for (const auto &iter : req.npu_events_metas()) {
        for (const auto &event : iter.npu_events()) {
            auto eventPtr = NpuEvent::FromPb(event);
            if (eventPtr != nullptr) {
                PutNpuEvent(iter.npu_id(), eventPtr);
            }
        }
    }
}
void NpuEventsTable::Clear()
{
    std::unique_lock<std::shared_timed_mutex> lock(npuEventTableMutex_);
    LOG(INFO) << "Clear npu event table.";
    npuPendingGetReqsTable_.clear();
}
}  // namespace master
}  // namespace datasystem
