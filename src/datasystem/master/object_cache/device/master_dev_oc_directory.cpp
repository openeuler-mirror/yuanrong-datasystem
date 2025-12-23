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
 * Description: Code to manage device object directory.
 */
#include "datasystem/master/object_cache/device/master_dev_oc_directory.h"

#include <memory>
#include <vector>

#include "datasystem/common/device/device_helper.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace master {
Status ObjectDirectoryTable::PutObject(const std::string &objectKey, const DeviceObjectMetaPb &metaPb,
                                       std::shared_ptr<ObjectDirectory> &objDirectory)
{
    std::shared_lock<std::shared_timed_mutex> lock(objDirTableMutex_);
    TbbObjDirTable::accessor p2pMetaAccess;
    auto found = objDirectoryTable_.find(p2pMetaAccess, objectKey);
    // if all location found in directory , it's a retry ,return status ok ;
    if (found) {
        for (auto it : metaPb.locations()) {
            auto res = p2pMetaAccess->second->FindLocation(it.client_id(), it.device_id());
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                std::get<0>(res), StatusCode::K_INVALID,
                FormatString("The objectKey %s already exists,and does not belong to the client, Delete it and try "
                             "again or using a new object key to put.",
                             objectKey));
        }
        objDirectory = p2pMetaAccess->second;
        return Status::OK();
    }
    // If not duplicate, update the object directory, i.e., object primary copy location and property.
    objDirectory = std::make_shared<ObjectDirectory>(metaPb);
    objDirectoryTable_.emplace(objectKey, objDirectory);
    return Status::OK();
}

Status ObjectDirectoryTable::GetObjectDirectory(const std::string &objectKey,
                                                std::shared_ptr<ObjectDirectory> &objectDirectory)
{
    std::shared_lock<std::shared_timed_mutex> lock(objDirTableMutex_);
    TbbObjDirTable::accessor objDirectoryAccess;
    if (objDirectoryTable_.find(objDirectoryAccess, objectKey)) {
        objectDirectory = objDirectoryAccess->second;
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND, FormatString("The objectKey %s is not found in directory table.", objectKey));
}

Status ObjectDirectoryTable::GetDataInfos(const std::string &objectKey,
                                          const std::function<void(std::shared_ptr<ObjectDirectory> &)> &onGetCallback)
{
    std::shared_lock<std::shared_timed_mutex> lock(objDirTableMutex_);
    TbbObjDirTable::accessor objDirectoryAccess;
    if (objDirectoryTable_.find(objDirectoryAccess, objectKey)) {
        auto &objDirectory = objDirectoryAccess->second;
        onGetCallback(objDirectory);
    }
    return Status::OK();
}

void ObjectDirectoryTable::RemoveObjectDirectory(const std::string &objectKey)
{
    std::shared_lock<std::shared_timed_mutex> lock(objDirTableMutex_);
    objDirectoryTable_.erase(objectKey);
}

Status ObjectGetDataInfosReqSubscriptionTable::AddGetDataInfosRequest(const std::string &objectKey,
                                                                      std::shared_ptr<GetDataInfosRequest> &request)
{
    auto rc = getDataInfosRequestTable_.AddRequest(objectKey, request);
    return rc;
}

void ObjectGetDataInfosReqSubscriptionTable::RemoveGetDataInfosRequest(std::shared_ptr<GetDataInfosRequest> &request)
{
    VLOG(1) << "Begin to remove RemoveGetRequest, client id: " << request->clientId_;
    return getDataInfosRequestTable_.RemoveRequest(request);
}

Status ObjectGetDataInfosReqSubscriptionTable::UpdateGetDataInfosRequest(const std::string &objectKey,
                                                                         std::shared_ptr<ObjectDirectory> &objDirectory)
{
    auto entryParam = GetDataInfosEntryParams::ConstructGetDataInfosEntryParams(objDirectory);
    return getDataInfosRequestTable_.UpdateRequest(
        objectKey, entryParam, Status::OK(), [this](std::shared_ptr<GetDataInfosRequest> req) {
            LOG_IF_ERROR(ReturnGetDataInfosRequest(req), "ReturnGetDataInfoRequest failed");
        });
}

Status ObjectGetDataInfosReqSubscriptionTable::ReturnGetDataInfosRequest(std::shared_ptr<GetDataInfosRequest> request)
{
    // Ensure return once and handle concurrency cases:
    // Case 1: Timeout.
    // Case 2: Put triggered notification.
    // Case 3: Look-up the object directory and condition satisfies.
    RETURN_RUNTIME_ERROR_IF_NULL(request);
    bool expected = false;
    RETURN_OK_IF_TRUE(!request->isReturn_.compare_exchange_strong(expected, true));

    // Ensure timer canceled on cases 2 and 3.
    VLOG(1) << FormatString("[ReturnGetDataInfoRequest] begin, client id: %d, deviceId: %d", request->clientId_,
                            request->deviceId_);
    std::lock_guard<std::mutex> lck(request->mutex_);
    if (request->timer_ != nullptr) {
        if (!TimerQueue::GetInstance()->Cancel(*(request->timer_))) {
            LOG(ERROR) << "Failed to Cancel the timer: " << request->timer_->GetId();
        }
        request->timer_.reset();
    }

    // Case 1: Timeout.
    int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
    if (remainingTimeMs <= 0) {
        VLOG(1) << FormatString("[ReturnGetDataInfoRequest] timeout, client id: %d, deviceId: %d", request->clientId_,
                                request->deviceId_);
        RemoveGetDataInfosRequest(request);
        return request->serverApi_->SendStatus({ K_RPC_DEADLINE_EXCEEDED, "Rpc timeout" });
    }

    // Case 2: Subscription condition satisfied.
    GetDataInfoRspPb resp;
    for (auto &objectKey : request->rawObjectKeys_) {
        GetDataInfosRequest::TbbGetObjsTable::const_accessor accessor;
        if (request->objects_.find(accessor, objectKey) && accessor->second != nullptr) {
            // Verifying datainfoNum and clientId.
            VLOG(1) << FormatString("[ReturnGetDataInfoRequest] satisfied, objectKey: %s", objectKey);
            *resp.mutable_data_infos() = accessor->second->dataInfos;
        } else {
            auto message = FormatString("GetDataInfo can't find object: %s", objectKey);
            LOG(ERROR) << message;
            return request->serverApi_->SendStatus({ K_NOT_FOUND, message });
        }
    }
    RemoveGetDataInfosRequest(request);

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(request->serverApi_->Write(resp), "Write reply to client stream failed.");
    return Status::OK();
}

DataInfo ParseDataInfoFromPb(const DataInfoPb &pb)
{
    return { nullptr, static_cast<DataType>(pb.data_type()), pb.count() };
}

std::vector<DataInfo> ParseDataInfosFromRepeatedField(
    const google::protobuf::RepeatedPtrField<DataInfoPb> &dataInfosRepeatedField)
{
    std::vector<DataInfo> dataInfos;
    dataInfos.reserve(dataInfosRepeatedField.size());
    for (const auto &dataInfo : dataInfosRepeatedField) {
        dataInfos.push_back(ParseDataInfoFromPb(dataInfo));
    }
    return dataInfos;
}

ObjectDirectory::ObjectDirectory(DeviceObjectMetaPb metaPb) : metaPb_(std::move(metaPb))
{
    for (auto loc : metaPb_.locations()) {
        auto clientId = loc.client_id();
        auto deviceId = loc.device_id();
        loc.set_create_timestamp(GetSystemClockTimeStampUs());
        locMap_.emplace(ConcatClientAndDeviceId(clientId, deviceId), loc);
    }
    // use locMap_ to store location meta.
    metaPb_.mutable_locations()->Clear();
}

std::lock_guard<std::recursive_mutex> ObjectDirectory::GetLockGuard()
{
    return std::lock_guard<std::recursive_mutex>(rMutex_);
}

LifetimeType ObjectDirectory::GetLifetime() const
{
    return static_cast<LifetimeType>(metaPb_.lifetime());
}

Status ObjectDirectory::ClearAllLocations()
{
    for (const auto &iter : locMap_) {
        if (iter.second.location_state() == PENDING_REMOVE) {
            RETURN_STATUS(K_TRY_AGAIN, FormatString("Location: %s is pending remove", iter.first));
        }
    }
    for (const auto &iter : locMap_) {
        removeLocCallBack_(metaPb_.object_key(), iter.second.client_id(), iter.second.device_id());
    }
    removeDirCallBack_(metaPb_.object_key());
    return Status::OK();
}

Status ObjectDirectory::ClearAllLocationsExceptClient(const std::string &clientId)
{
    for (const auto &iter : locMap_) {
        if (iter.second.location_state() == PENDING_REMOVE) {
            RETURN_STATUS(K_TRY_AGAIN, FormatString("Location: %s is pending remove", iter.first));
        }
    }

    // Notify all clients except the requester to remove this object's metadata
    for (const auto &iter : locMap_) {
        // The client invoking the Delete operation will clean up its local metadata itself,
        // so the master doesn't need to send LIFECYCLE_EXIT_NOTIFICATION to it.
        if (iter.second.client_id() != clientId) {
            removeLocCallBack_(metaPb_.object_key(), iter.second.client_id(), iter.second.device_id());
        }
    }
    removeDirCallBack_(metaPb_.object_key());
    return Status::OK();
}

Status ObjectDirectory::ClearClientAllLocations(const std::string &clientId)
{
    auto guard = GetLockGuard();
    for (const auto &iter : locMap_) {
        if (iter.second.client_id() == clientId && iter.second.location_state() == PENDING_REMOVE) {
            RETURN_STATUS(K_TRY_AGAIN, FormatString("Location: %s is pending remove", iter.first));
        }
    }
    std::vector<int32_t> deviceIds;
    for (const auto &iter : locMap_) {
        if (iter.second.client_id() != clientId) {
            continue;
        }
        deviceIds.emplace_back(iter.second.device_id());
    }
    for (auto deviceId : deviceIds) {
        if (deviceId == ALL_DEVICE_ID) {
            RETURN_STATUS(K_INVALID, FormatString("device_id: %d is invalid", deviceId));
        }
        RETURN_IF_NOT_OK(RemoveLocation(clientId, deviceId));
    }
    return Status::OK();
}

void ObjectDirectory::AddLocation(const std::string &clientId, int32_t deviceId)
{
    DeviceLocationPb loc;
    loc.set_client_id(clientId);
    loc.set_device_id(deviceId);
    loc.set_create_timestamp(GetSystemClockTimeStampUs());
    locMap_.emplace(ConcatClientAndDeviceId(clientId, deviceId), loc);
}

std::tuple<bool, DeviceLocationPb *> ObjectDirectory::FindLocation(const std::string &clientId, int32_t deviceId)
{
    auto iter = locMap_.find(ConcatClientAndDeviceId(clientId, deviceId));
    auto find = iter != locMap_.end();
    return std::make_tuple(find, &(iter->second));
}

void ObjectDirectory::UpdateLocationAfterSelect(DeviceLocationPb *loc)
{
    // ref count can add in this func.
    loc->set_location_state(IN_USE);
    LOG(INFO) << "update location: INIT --> IN_USE";
}

google::protobuf::RepeatedPtrField<DataInfoPb> ObjectDirectory::GetDataInfos()
{
    return metaPb_.data_infos();
}

Status ObjectDirectory::VerifyDataRangeLegality(const DeviceObjectMetaPb &getMetaPb)
{
    auto &blobsFromPut = metaPb_.data_infos();
    auto &blobsFromGet = getMetaPb.data_infos();

    // 1. Validate that the number of data blobs matches
    if (blobsFromPut.size() != blobsFromGet.size()) {
        RETURN_STATUS_LOG_ERROR(K_INVALID,
                                FormatString("The number of data blobs corresponding to the same key is inconsistent "
                                             "between sender and receiver, src size is: %zu, dst size is: %zu",
                                             blobsFromPut.size(), blobsFromGet.size()));
    }

    // 2. Validate each data blob individually
    for (auto i = 0; i < blobsFromPut.size(); i++) {
        const auto &putBlob = blobsFromPut[i];
        const auto &getBlob = blobsFromGet[i];

        // 2.1 Validate data types match
        if (putBlob.data_type() != getBlob.data_type()) {
            RETURN_STATUS_LOG_ERROR(
                K_INVALID, FormatString("The dataInfo.dataType does not match, Put: %d, Get: %d",
                                        static_cast<int>(putBlob.data_type()), static_cast<int>(getBlob.data_type())));
        }

        // 2.2 Validate offset is non-negative
        if (getMetaPb.src_offset() < 0) {
            RETURN_STATUS_LOG_ERROR(K_INVALID, FormatString("getMetaPb: %d < 0", getMetaPb.src_offset()));
        }

        // 2.3 Validate data range boundaries
        // Check for integer overflow in range calculation
        size_t requestedOffset = static_cast<size_t>(getMetaPb.src_offset());
        size_t requestedSize = getBlob.count();
        if (SIZE_MAX - requestedOffset < requestedSize) {
            RETURN_STATUS_LOG_ERROR(K_INVALID, FormatString("request offset: %zu + request size: %zu > SIZE_MAX",
                                                            requestedOffset, requestedSize));
        }

        // Verify requested range does not exceed available data
        size_t requestedEnd = requestedOffset + requestedSize;
        size_t availableSize = putBlob.count();
        if (requestedEnd > availableSize) {
            RETURN_STATUS_LOG_ERROR(
                K_INVALID, FormatString("The requested data range of receiver exceeds the available data of sender. "
                                        "Available data size: %zu, Requested: offset=%zu, size=%zu, end=%zu",
                                        availableSize, requestedOffset, requestedSize, requestedEnd));
        }
    }

    // 3. ensure requested locations are unique
    for (const auto &loc : getMetaPb.locations()) {
        auto npuId = ConcatClientAndDeviceId(loc.client_id(), loc.device_id());
        if (locMap_.find(npuId) != locMap_.end()) {
            RETURN_STATUS_LOG_ERROR(
                K_INVALID, FormatString("Duplicate location request: %s already exists in the directory", npuId));
        }
    }

    return Status::OK();
}

Status ObjectDirectory::AckLocation(const std::string &clientId, int32_t deviceId)
{
    auto npuId = ConcatClientAndDeviceId(clientId, deviceId);
    auto iter = locMap_.find(npuId);
    if (iter == locMap_.end()) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("The location %s is not found.", npuId));
    }
    auto state = iter->second.location_state();
    if (state == IN_USE) {
        iter->second.set_location_state(INIT);
        LOG(INFO) << "update location: IN_USE --> INIT";
    } else if (state == PENDING_REMOVE) {
        removeLocCallBack_(metaPb_.object_key(), clientId, deviceId);
        LOG(INFO) << "update location: PENDING_REMOVE --> REMOVE";
        locMap_.erase(npuId);
    }
    if (locMap_.empty()) {
        LOG(INFO) << "location is empty , remove meta";
        removeDirCallBack_(metaPb_.object_key());
    }
    return Status::OK();
}

Status ObjectDirectory::RemoveLocation(const std::string &clientId, int32_t deviceId)
{
    if (deviceId == ALL_DEVICE_ID) {
        return ClearClientAllLocations(clientId);
    }
    auto npuId = ConcatClientAndDeviceId(clientId, deviceId);
    auto iter = locMap_.find(npuId);
    if (iter == locMap_.end()) {
        return Status::OK();
    }
    auto state = iter->second.location_state();
    if (state == IN_USE) {
        iter->second.set_location_state(PENDING_REMOVE);
        LOG(INFO) << "update location: IN_USE --> PENDING_REMOVE";
        RETURN_STATUS(K_TRY_AGAIN, "Location is in PENDING_REMOVE state, please try again later.");
    } else if (state == INIT) {
        locMap_.erase(npuId);
        removeLocCallBack_(metaPb_.object_key(), clientId, deviceId);
        LOG(INFO) << "update location: INIT --> REMOVE";
    }
    if (locMap_.empty()) {
        LOG(INFO) << "location is empty , remove meta";
        removeDirCallBack_(metaPb_.object_key());
    }
    return Status::OK();
}

void ObjectDirectory::RegisterCallback(
    std::function<void(const std::string &, const std::string &, int32_t)> removeLocCallBack,
    std::function<void(const std::string &)> removeDirCallBack)
{
    removeLocCallBack_ = removeLocCallBack;
    removeDirCallBack_ = removeDirCallBack;
}

void ObjectGetDataInfosReqSubscriptionTable::EraseDataInfosReqSubscription(const std::string &objectKey)
{
    getDataInfosRequestTable_.EraseSub(objectKey);
}

Status ObjectGetP2PMetaReqSubscriptionTable::AddGetP2PMetaRequest(const std::string &objectKey,
                                                                  std::shared_ptr<GetP2PMetaRequest> &request)
{
    return getP2PMetaRequestTable_.AddRequest(objectKey, request);
}

void ObjectGetP2PMetaReqSubscriptionTable::RemoveGetP2PMetaRequest(std::shared_ptr<GetP2PMetaRequest> &request)
{
    LOG(INFO) << "Begin to remove GetP2PMetaSubscriptiont, client id: " << request->clientId_;
    return getP2PMetaRequestTable_.RemoveRequest(request);
}

Status ObjectGetP2PMetaReqSubscriptionTable::UpdateGetP2PMetaRequest(
    const std::string &objectKey, std::string srcClientId, int32_t srcDeviceId, std::string srcWorkerIP,
    const std::unordered_map<std::shared_ptr<GetP2PMetaRequest>, Status> &requestVerificationResults)
{
    auto entryParam = GetP2PMetaEntryParams::ConstructGetP2PMetaEntryParams(srcClientId, srcDeviceId, srcWorkerIP);
    return getP2PMetaRequestTable_.UpdateRequestsWithVerificationResults(
        objectKey, entryParam, requestVerificationResults, [this](std::shared_ptr<GetP2PMetaRequest> req) {
            LOG_IF_ERROR(ReturnGetP2PMetaRequest(req), "ReturnGetP2PMetaRequest failed");
        });
}

std::vector<std::shared_ptr<GetP2PMetaRequest>> ObjectGetP2PMetaReqSubscriptionTable::GetAllP2PMetaRequest(
    const std::string &objectKey)
{
    return getP2PMetaRequestTable_.GetRequestsByObject(objectKey);
}

Status ObjectGetP2PMetaReqSubscriptionTable::ReturnGetP2PMetaRequest(std::shared_ptr<GetP2PMetaRequest> request)
{
    // Ensure return once and handle concurrency cases:
    // Case 1: Timeout.
    // Case 2: Put triggered notification.
    // Case 3: Look-up the object directory and condition satisfies.
    RETURN_RUNTIME_ERROR_IF_NULL(request);
    bool expected = false;
    RETURN_OK_IF_TRUE(!request->isReturn_.compare_exchange_strong(expected, true));

    // Ensure timer canceled on cases 2 and 3.
    LOG(INFO) << FormatString("ReturnGetP2PMetaRequest begin, client id: %d, deviceId: %d", request->clientId_,
                              request->deviceId_);
    std::lock_guard<std::mutex> lck(request->mutex_);
    if (request->timer_ != nullptr) {
        if (!TimerQueue::GetInstance()->Cancel(*(request->timer_))) {
            LOG(ERROR) << "Failed to Cancel the timer: " << request->timer_->GetId();
        }
        request->timer_.reset();
    }

    // Case 1: Timeout.
    int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
    if (remainingTimeMs <= 0) {
        VLOG(1) << FormatString("ReturnGetP2PMetaRequest timeout, client id: %d, deviceId: %d", request->clientId_,
                                request->deviceId_);
        RemoveGetP2PMetaRequest(request);
        return request->serverApi_->SendStatus({ K_RPC_DEADLINE_EXCEEDED, "Rpc timeout" });
    }

    // Case 2: Subscription condition satisfied.
    GetP2PMetaRspPb resp;
    std::unordered_map<std::string, std::tuple<std::string, int64_t>> tmpRespMap;
    for (auto &objectKey : request->rawObjectKeys_) {
        GetP2PMetaRequest::TbbGetObjsTable::const_accessor accessor;
        if (request->objects_.find(accessor, objectKey) && accessor->second != nullptr) {
            auto childResp = resp.add_dev_obj_resp_meta();
            auto rc = request->GetObjectStatus(objectKey);
            childResp->set_object_key(objectKey);
            if (rc.IsError()) {
                childResp->mutable_error()->set_error_code(rc.GetCode());
                childResp->mutable_error()->set_error_msg(rc.GetMsg());
                continue;
            }
            childResp->set_src_client_id(accessor->second->srcClientId_);
            childResp->set_src_device_id(accessor->second->srcDeviceId_);
            auto &srcWorkerIP = accessor->second->srcWorkerIP_;
            auto &dstWorkerIP = request->workerIP_;
            childResp->set_is_same_node(dstWorkerIP == srcWorkerIP);
            LOG(INFO) << "ReturnGetP2PMetaRequest satisfied, objectKey: " << objectKey;
        } else {
            auto message = FormatString("GetP2PMeta can't find object: %s", objectKey);
            LOG(ERROR) << message;
            INJECT_POINT("MasterDevOcManager.ReturnGetP2PMetaRequest.TestPutGetMultThreadSync", []() {
                LOG(INFO) << "MasterDevOcManager.ReturnGetP2PMetaRequest.TestPutGetMultThreadSync";
                return Status::OK();
            });
            RemoveGetP2PMetaRequest(request);
            return request->serverApi_->SendStatus({ K_NOT_FOUND, message });
        }
    }

    RemoveGetP2PMetaRequest(request);

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(request->serverApi_->Write(resp), "Write reply to client stream failed.");
    return Status::OK();
}

void ObjectDirectoryTable::FillMigrateData(MigrateMetadataReqPb &req)
{
    std::unique_lock<std::shared_timed_mutex> lock(objDirTableMutex_);
    for (const auto &iter : objDirectoryTable_) {
        auto metaPb = iter.second->GetMetaPb();
        req.mutable_device_object_metas()->Add(std::move(metaPb));
    }
}

void ObjectDirectoryTable::SaveMigrateData(const MigrateMetadataReqPb &req)
{
    for (const auto &iter : req.device_object_metas()) {
        std::shared_ptr<ObjectDirectory> dir;
        // PutObject only failed when objectKey exist, ignore it.
        (void)PutObject(iter.object_key(), iter, dir);
    }
}

const DeviceObjectMetaPb ObjectDirectory::GetMetaPb() const
{
    auto output = metaPb_;
    output.mutable_locations()->Clear();
    for (auto iter : locMap_) {
        output.mutable_locations()->Add(std::move(iter.second));
    }
    return output;
}
void ObjectDirectoryTable::Clear()
{
    std::unique_lock<std::shared_timed_mutex> lock(objDirTableMutex_);
    LOG(INFO) << "Clear object directory table.";
    objDirectoryTable_.clear();
}
}  // namespace master
}  // namespace datasystem
