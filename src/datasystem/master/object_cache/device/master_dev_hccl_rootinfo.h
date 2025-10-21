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
 * Description: Defines the class to manage hccl rootinfo, established in destination npus.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_REQ_MANAGER_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_REQ_MANAGER_H

#include "datasystem/common/util/request_table.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include <shared_mutex>

namespace datasystem {
namespace master {

// RootInfo Map. dstNPUId -> {srcNPUId: rootInfo}. Pub: SendRootInfo (Receiver side), Sub: RecvRootInfo.
using TbbRootInfoTable = tbb::concurrent_hash_map<ImmutableString, std::string>;
class HcclRootInfoTable {
public:
    Status PutRootInfo(const std::string &dstNpuId, const std::string &rootInfo);

    void GetAndEraseRootInfo(const std::string &dstNpuId,
                             const std::function<void(const std::string &rootInfo)> &onGetCallback);
    /**
     * @brief Remove the RootInfo from the table.
     * @param[in] dstNpuId The npu id need to remove.
     */
    void EraseRootInfo(const std::string &dstNpuId);

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
     * @brief Clear RootInfo table.
     */
    void Clear();

private:
    // dstNPUId -> {srcNPUId: rootInfo}.
    TbbRootInfoTable rootInfoTable_;
    std::shared_timed_mutex rootInfoTableMutex_;
};

struct RecvRootInfoEntryParams {
    static std::shared_ptr<RecvRootInfoEntryParams> ConstructRecvRootInfoEntryParams(const std::string &rootInfo)
    {
        return std::make_shared<RecvRootInfoEntryParams>(RecvRootInfoEntryParams{ .rootInfo = rootInfo });
    }

    const std::string rootInfo;
};

using RecvRootInfoRequest = UnaryRequest<RecvRootInfoReqPb, RecvRootInfoRspPb, RecvRootInfoEntryParams>;

class HcclRootInfoSubscriptionTable {
public:
    HcclRootInfoSubscriptionTable() = default;

    ~HcclRootInfoSubscriptionTable() = default;

    /**
     * @brief Add RecvRootInfo request to HcclRootInfoSubscriptionTable.
     * @param[in] objectKey The object key.
     * @param[in] request The RecvRootInfo request that is waiting on the object key.
     * @return Status of the call.
     */
    Status AddRecvRootInfoRequest(const std::string &objectKey, std::shared_ptr<RecvRootInfoRequest> &request);

    /**
     * @brief Remove the RecvRootInfo request from the waiting requests table.
     * @param[in] request The request need to remove.
     */
    void RemoveRecvRootInfoRequest(std::shared_ptr<RecvRootInfoRequest> &request);

    /**
     * @brief Update the RecvRootInfo request info after object is sent.
     * @param[in] objectKey The objectKey, which is the mix of dst_client_id and dst_device_id.
     * @param[in] rootInfo The rootInfo message.
     * @return Status of the call.
     */
    Status UpdateRecvRootInfoRequestForSuccess(const std::string &objectKey, const std::string &rootInfo);

    /**
     * @brief Reply to client with the device RecvRootInfo request.
     * @param[in] request The RecvRootInfo request which to return.
     * @return Status of the call.
     */
    Status ReturnFromRecvRootInfoRequest(std::shared_ptr<RecvRootInfoRequest> request);

    /**
     * @brief Erase subscription table based on npuid.
     * @param[in] NpuId The NPUID of subscription table records.
     */
    void EraseRootInfoSubscription(const std::string &npuId);

private:
    RequestTable<RecvRootInfoRequest> recvRootInfoRequestTable_;
};

using TbbHcclRelationshipTable = tbb::concurrent_hash_map<std::string, tbb::concurrent_unordered_set<std::string>>;

class HcclRelationshipTable {
public:
    // key: DataReceiver --> value: Set(DataSender)
    
    HcclRelationshipTable() = default;

    ~HcclRelationshipTable() = default;

    /**
     * @brief Add an hccl connection record.
     * @param[in] dataReceiver Indicates the receiver, which is a string ID consisting of client and device.
     * @param[in] dataSender Indicates the receiver, which is a string ID consisting of client and device.
     */
    void AddEdge(const std::string &dataReceiver, const std::string &dataSender);

    /**
     * @brief Check whether dataSender exists in the value whose key is dataReceiver.
     * @param[in] dataReceiver Indicates the receiver, which is a string ID consisting of client and device.
     * @param[in] dataSender Indicates the receiver, which is a string ID consisting of client and device.
     * @return The concat exist check result.
     */
    bool Contains(const std::string &dataReceiver, const std::string &dataSender);

    /**
     * @brief Get All client npuIds by client id.
     * @param[in] clientId The client id.
     * @return The client npuIds.
     */
    std::set<std::string> GetClientNpuId(const std::string &clientId);

    /**
     * @brief Delete the dataSender edge and node.
     * @param[in] dataSender Indicates the receiver, which is a string ID consisting of client and device.
     * @param[out] connectId The connect id with dataSender.
     */
    void EraseNode(const std::string &dataSender, std::set<std::string> &connectIds);

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
     * @brief Clear graph_ table.
     */
    void Clear();

private:
    std::shared_timed_mutex mutex_;
    TbbHcclRelationshipTable graph_;
};

}  // namespace master
}  // namespace datasystem

#endif
