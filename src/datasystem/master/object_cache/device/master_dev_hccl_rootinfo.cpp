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
 * Description: Defines the class to  manage hccl rootinfo, established in destination npus.
 */
#include "datasystem/master/object_cache/device/master_dev_hccl_rootinfo.h"

#include <memory>
#include <vector>

namespace datasystem {
namespace master {
Status HcclRootInfoTable::PutRootInfo(const std::string &hcclPeerId, const std::string &rootInfo)
{
    std::shared_lock<std::shared_timed_mutex> lck(rootInfoTableMutex_);
    bool rc = rootInfoTable_.insert(std::make_pair(hcclPeerId, rootInfo));
    if (!rc) {
        RETURN_STATUS_LOG_ERROR(
            K_RUNTIME_ERROR,
            FormatString("Failed to insert the key %s into the npuPendingGetReqsTable_ table.", hcclPeerId));
    }
    return Status::OK();
}

bool HcclRootInfoTable::IsExistRootInfo(const std::string &dstNpuId)
{
    std::shared_lock<std::shared_timed_mutex> lock(rootInfoTableMutex_);
    TbbRootInfoTable::accessor rootInfoAccess;
    return rootInfoTable_.find(rootInfoAccess, dstNpuId);
}

void HcclRootInfoTable::GetAndEraseRootInfo(const std::string &hcclPeerId,
                                            const std::function<void(const std::string &)> &onGetCallback)
{
    std::shared_lock<std::shared_timed_mutex> lck(rootInfoTableMutex_);
    TbbRootInfoTable::accessor rootInfoAccess;
    auto found = rootInfoTable_.find(rootInfoAccess, hcclPeerId);
    if (found) {
        onGetCallback(rootInfoAccess->second);
        // erase dstNpuId from rootInfoTable_
        (void)rootInfoTable_.erase(rootInfoAccess);
    }
}

void HcclRootInfoTable::EraseRootInfo(const std::string &hcclPeerId)
{
    TbbRootInfoTable::accessor rootInfoAccess;
    if (rootInfoTable_.find(rootInfoAccess, hcclPeerId)) {
        // erase dstNpuId from rootInfoTable_
        (void)rootInfoTable_.erase(rootInfoAccess);
    }
}

bool HcclRootInfoSubscriptionTable::IsExistRecvRootInfoReq(const std::string &objectKey)
{
    return recvRootInfoRequestTable_.ObjectInRequest(objectKey);
}

void HcclRootInfoSubscriptionTable::EraseRootInfoSubscription(const std::string &hcclPeerId)
{
    recvRootInfoRequestTable_.EraseSub(hcclPeerId);
}

void HcclRootInfoTable::FillMigrateData(MigrateMetadataReqPb &req)
{
    std::unique_lock<std::shared_timed_mutex> lck(rootInfoTableMutex_);
    for (const auto &iter : rootInfoTable_) {
        auto *metaPb = req.add_root_info_metas();
        metaPb->set_peer_id(iter.first);
        // RootInfo is a struct but export as char array, must use std::begin and std::end to copy.
        metaPb->set_root_info(std::string(std::begin(iter.second), std::end(iter.second)));
    }
}

void HcclRootInfoTable::SaveMigrateData(const MigrateMetadataReqPb &req)
{
    for (const auto &iter : req.root_info_metas()) {
        (void)PutRootInfo(iter.peer_id(), iter.root_info());
    }
}

Status HcclRootInfoSubscriptionTable::AddRecvRootInfoRequest(const std::string &objectKey,
                                                             std::shared_ptr<RecvRootInfoRequest> &request)
{
    recvRootInfoRequestTable_.AddRequest(objectKey, request);
    return Status::OK();
}

void HcclRootInfoSubscriptionTable::RemoveRecvRootInfoRequest(std::shared_ptr<RecvRootInfoRequest> &request)
{
    return recvRootInfoRequestTable_.RemoveRequest(request);
}

Status HcclRootInfoSubscriptionTable::UpdateRecvRootInfoRequestForSuccess(const std::string &objectKey,
                                                                          const std::string &rootInfo,
                                                                          const bool isDeadLock)
{
    auto entryParam = RecvRootInfoEntryParams::ConstructRecvRootInfoEntryParams(rootInfo, isDeadLock);
    return recvRootInfoRequestTable_.UpdateRequest(
        objectKey, entryParam, Status::OK(), [this](std::shared_ptr<RecvRootInfoRequest> req) {
            LOG_IF_ERROR(ReturnFromRecvRootInfoRequest(req), "ReturnFromRecvRootInfoRequest failed");
        });
}

Status HcclRootInfoSubscriptionTable::ReturnFromRecvRootInfoRequest(std::shared_ptr<RecvRootInfoRequest> request)
{
    RETURN_RUNTIME_ERROR_IF_NULL(request);
    bool expected = false;
    RETURN_OK_IF_TRUE(!request->isReturn_.compare_exchange_strong(expected, true));

    VLOG(1) << FormatString("Begin to ReturnFromRecvRootInfoRequest, client id: %d, deviceId: %d", request->clientId_,
                            request->deviceId_);
    std::lock_guard<std::mutex> lck(request->mutex_);
    int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
    if (remainingTimeMs <= 0) {
        RemoveRecvRootInfoRequest(request);
        return request->serverApi_->SendStatus({ K_RPC_DEADLINE_EXCEEDED, "Rpc timeout" });
    }

    RecvRootInfoRspPb resp;
    for (auto &objectKey : request->rawObjectKeys_) {
        RecvRootInfoRequest::TbbGetObjsTable::const_accessor accessor;
        bool isFindObj = false;
        if (request->objects_.find(accessor, objectKey) && accessor->second != nullptr) {
            auto &param = accessor->second;
            resp.set_root_info(std::string(std::begin(param->rootInfo), std::end(param->rootInfo)));
            resp.set_is_dead_lock(param->isDeadLock);
            isFindObj = true;
        }
        if (!isFindObj) {
            LOG(ERROR) << FormatString("Can't find object: %s, clientId: %s, deviceId: %s", objectKey,
                                       request->clientId_, request->deviceId_);
        }
    }
    RemoveRecvRootInfoRequest(request);

    if (request->timer_ != nullptr) {
        if (!TimerQueue::GetInstance()->Cancel(*(request->timer_))) {
            LOG(ERROR) << "Failed to Cancel the timer: " << request->timer_->GetId();
        }
        request->timer_.reset();
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(request->serverApi_->Write(resp), "Write reply to client stream failed.");
    return Status::OK();
}

void HcclRootInfoTable::Clear()
{
    std::unique_lock<std::shared_timed_mutex> lock(rootInfoTableMutex_);
    LOG(INFO) << "Clear npu event table.";
    rootInfoTable_.clear();
}

void HcclRelationshipTable::AddEdge(const std::string &dataReceiver, const std::string &dataSender)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    TbbHcclRelationshipTable::accessor acc;
    (void)hcclRelationshipGraph_.insert(acc, dataReceiver);
    acc->second.insert(dataSender);
}

bool HcclRelationshipTable::Contains(const std::string &dataReceiver, const std::string &dataSender)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    TbbHcclRelationshipTable::const_accessor acc;
    if (!hcclRelationshipGraph_.find(acc, dataReceiver)) {
        return false;
    }
    return acc->second.count(dataSender) > 0;
}

std::set<std::string> HcclRelationshipTable::GetClientNpuId(const std::string &clientId)
{
    std::set<std::string> clientNpuIds;
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    for (const auto &iter : hcclRelationshipGraph_) {
        if (iter.first.find(clientId) != std::string::npos) {
            clientNpuIds.insert(iter.first);
        }
        for (const auto &strData : iter.second) {
            if (strData.find(clientId) != std::string::npos) {
                clientNpuIds.insert(strData);
            }
        }
    }
    return clientNpuIds;
}

void HcclRelationshipTable::EraseNode(const std::string &dataSender, std::set<std::string> &connectIds)
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);

    TbbHcclRelationshipTable::const_accessor acc;
    if (hcclRelationshipGraph_.find(acc, dataSender)) {
        connectIds = std::set<std::string>(acc->second.begin(), acc->second.end());
    }
    hcclRelationshipGraph_.erase(acc);
    std::set<std::string> eraseList;

    for (auto &iter : hcclRelationshipGraph_) {
        auto &connectSet = iter.second;
        if (connectSet.find(dataSender) == connectSet.end()) {
            continue;
        }
        (void)connectSet.unsafe_erase(dataSender);
        connectIds.insert(iter.first);
        if (connectSet.empty()) {
            eraseList.insert(iter.first);
        }
    }
    for (auto &key : eraseList) {
        hcclRelationshipGraph_.erase(key);
    }
}

void HcclRelationshipTable::FillMigrateData(MigrateMetadataReqPb &req)
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    for (const auto &iter : hcclRelationshipGraph_) {
        auto HcclRelationshipPb = req.add_hccl_relationship();
        HcclRelationshipPb->set_data_receiver_id(iter.first);

        for (auto setIter = iter.second.begin(); setIter != iter.second.end(); ++setIter) {
            HcclRelationshipPb->add_data_sender_id(*setIter);
        }
    }
}

void HcclRelationshipTable::SaveMigrateData(const MigrateMetadataReqPb &req)
{
    for (const auto &iter : req.hccl_relationship()) {
        for (auto sender : iter.data_sender_id()) {
            AddEdge(iter.data_receiver_id(), sender);
        }
    }
}

void HcclRelationshipTable::Clear()
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    LOG(INFO) << "Clear HcclRelationshipTable.";
    hcclRelationshipGraph_.clear();
}

}  // namespace master
}  // namespace datasystem