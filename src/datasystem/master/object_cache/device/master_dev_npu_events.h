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
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_NPU_EVENTS_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_NPU_EVENTS_H

#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/util/queue/blocking_queue.h"
#include "datasystem/common/util/request_table.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"

namespace datasystem {
namespace master {
struct NpuEvent {
    NpuEvent(const SubscribeEventTypePb &eventType, const std::string &objectKey, const std::string &dstClientId,
             int32_t dstDeviceId, bool isSameNode);

    NpuEvent(const SubscribeEventTypePb &eventType, const std::string &objectKey, const std::string &dstClientId,
             int32_t dstDeviceId, bool isSameNode, int32_t srcOffset, int32_t length);

    /**
     * @brief Create get notification npu event.
     * @param[in] objectKey The object key.
     * @param[in] dstClientId The dst client id.
     * @param[in] dstDeviceId The dst device id.
     * @return The npu event shared_ptr.
     */
    static std::shared_ptr<NpuEvent> CreateGetNotification(const std::string &objectKey, const std::string &dstClientId,
                                                           int32_t dstDeviceId, bool isSameNode, int32_t srcOffset,
                                                           int32_t length);

    /**
     * @brief Create life time exit notification npu event.
     * @param[in] objectKey The object key.
     * @return The npu event shared_ptr.
     */
    static std::shared_ptr<NpuEvent> CreateLifeTimeExitNotification(const std::string &objectKey);

    /**
     * @brief Create subscribe evnet cancel notification npu event.
     * @return The npu event shared_ptr.
     */
    static std::shared_ptr<NpuEvent> CreateSubscribeCancelNotification();

    /**
     * @brief Create communication destroy notification npu event.
     * @param[in] dstClientId The dst client id.
     * @param[in] dstDeviceId The dst device id.
     * @return The npu event shared_ptr.
     */
    static std::shared_ptr<NpuEvent> CreateCommExitNotification(const std::string &dstClientId, int32_t dstDeviceId);

    /**
     * @brief Create npu event from pb.
     * @param[in] pb The SubscribeReceiveEventRspPb message.
     * @return The npu event shared_ptr.
     */
    static std::shared_ptr<NpuEvent> FromPb(const SubscribeReceiveEventRspPb &pb);

    /**
     * @brief Return proto message.
     * @return The SubscribeReceiveEventRspPb message.
     */
    SubscribeReceiveEventRspPb ToPb();

    SubscribeEventTypePb eventType;
    std::string objectKey;
    std::string dstClientId;
    int32_t dstDeviceId;
    bool isSameNode;
    int32_t srcOffset;
    int32_t length;
};

// Subscribe Event Q. srcNPUId -> Get Requests (objKey, dstNPUId). Pub: Get, Sub: SubscribeRecvEvent.
using QueuePtr = std::shared_ptr<BlockingQueue<std::shared_ptr<NpuEvent>>>;
using TbbSrcNpuGetReqsTable = tbb::concurrent_hash_map<ImmutableString, QueuePtr>;

class NpuEventsTable {
public:
    /**
     * @brief Put to-serve object get events to npu event table.
     * @param[in] srcNpuId The source npu id.
     * @param[in] event the npu event.
     */
    void PutNpuEvent(const std::string &srcNpuId, std::shared_ptr<NpuEvent> event);

    /**
     * @brief Get npu pending-to-process events. Perform onGetCallback with toServeGetRequest.
     * @param[in] srcNpuId The source npu id.
     * @param[in] onGetCallback The callback to perform operations on get event operation success.
     */
    void GetNpuEvents(const std::string &srcNpuId, const std::function<void(const QueuePtr &event)> &onGetCallback);

    /**
     * @brief Delete NPU event table based on NPUID.
     * @param[in] NpuId The source npu id.
     */
    void EraseNpuEvent(const std::string &npuId);

    /**
     * @brief Fill migrate data.
     * @param[out] req The migrate rpc request
     */
    void FillMigrateData(MigrateMetadataReqPb &req);

    /**
     * @brief Save migrate data.
     * @param[in] req The migrate rpc request
     */
    void SaveMigrateData(const MigrateMetadataReqPb &req);

    /**
     * @brief Clear NpuEvent table.
     */
    void Clear();

private:
    // srcNPUId -> Get Requests (objKey, dstNPUId).
    TbbSrcNpuGetReqsTable npuPendingGetReqsTable_;
    std::shared_timed_mutex npuEventTableMutex_;
};

using SubscribeReceiveEventRequest =
    UnaryRequest<SubscribeReceiveEventReqPb, SubscribeReceiveEventRspPb, BlockingQueue<std::shared_ptr<NpuEvent>>>;

class NpuEventsSubscriptionTable {
public:
    /**
     * @brief Add SubscribeReceiveEvent request to HcclRootInfoSubscriptionTable.
     * @param[in] objectKey The object key.
     * @param[in] request The request that is waiting on the object key.
     * @return Status of the call.
     */
    Status AddSubscribeReceiveEventRequest(const std::string &objectKey,
                                           std::shared_ptr<SubscribeReceiveEventRequest> &request);

    /**
     * @brief Remove the SubscribeReceiveEvent request from the waiting requests table.
     * @param[in] request The request need to remove.
     */
    void RemoveSubscribeReceiveEventRequest(std::shared_ptr<SubscribeReceiveEventRequest> &request);

    /**
     * @brief Update the SubscribeReceiveEvent request info after object is get.
     * @param[in] srcNpuId The source npu id.
     * @param[in] onTheFlyGetRequest The subscribed event.
     * @return Status of the call.
     */
    Status UpdateNpuPendingGetReqsTableForSuccess(const std::string &srcNpuId, const QueuePtr &queue);

    /**
     * @brief Reply to client with the device SubscribeReceiveEvent request.
     * @param[in] request The SubscribeReceiveEvent request which to return.
     * @return Status of the call.
     */
    Status ReturnFromSubscribeReceiveEventRequest(std::shared_ptr<SubscribeReceiveEventRequest> request);

    /**
     * @brief Check whether srcNpuId in subscription table.
     * @param[in] srcNpuId The source npu id.
     * @return True if srcNpuId in subscription table.
     */
    bool SrcNpuIdInSubscribe(const std::string &srcNpuId);

    /**
     * @brief Erase srcNpuId in subscription table.
     * @param[in] srcNpuId The source npu id.
     */
    void EraseNpuSubscribe(const std::string &srcNpuId);

private:
    RequestTable<SubscribeReceiveEventRequest> subscribeReceiveEventRequestTable_;
};
}  // namespace master
}  // namespace datasystem

#endif  // DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_NPU_EVENTS_H
