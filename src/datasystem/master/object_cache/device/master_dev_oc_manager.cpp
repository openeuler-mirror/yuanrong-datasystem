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
 * Description: Code to manage device object.
 */
#include "datasystem/master/object_cache/device/master_dev_oc_manager.h"

#include <algorithm>
#include <memory>
#include <vector>

#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/common/device/device_helper.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/object_cache/device/master_dev_hccl_rootinfo.h"
#include "datasystem/master/object_cache/device/master_dev_npu_events.h"
#include "datasystem/master/object_cache/device/master_dev_oc_directory.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_event_type.h"

namespace datasystem {
namespace master {
const std::string MASTER_DEV_OC_MANAGER = "MasterDevOcManager";
void MasterDevOcManager::Init()
{
    // Object directory table and related subscription tables.
    objectDirectoryTable_ = std::make_shared<ObjectDirectoryTable>();
    objectGetDataInfosReqSubscriptionTable_ = std::make_shared<ObjectGetDataInfosReqSubscriptionTable>();
    objectGetP2PMetaReqSubscriptionTable_ = std::make_shared<ObjectGetP2PMetaReqSubscriptionTable>();

    npuEventsTable_ = std::make_shared<NpuEventsTable>();
    npuEventsSubscriptionTable_ = std::make_shared<NpuEventsSubscriptionTable>();

    rootInfoTable_ = std::make_shared<HcclRootInfoTable>();
    rootInfoSubscriptionTable_ = std::make_shared<HcclRootInfoSubscriptionTable>();

    deviceMetaOpRecordTable_ = std::make_shared<MasterDevClientMetaManager>();
    commRelationMap_ = std::make_shared<HcclRelationshipTable>();

    objectKeyLockTable_ = std::make_shared<TbbLockTable>();

    CheckAndClearDeviceMeta::GetInstance().AddSubscriber(
        MASTER_DEV_OC_MANAGER, [this](const std::string &objectKey) { return CheckAndClearDeviceMeta(objectKey); });

    ObjectDirectory::RegisterCallback(std::bind(&MasterDevOcManager::NotifyClientClearMeta, this, std::placeholders::_1,
                                                std::placeholders::_2, std::placeholders::_3),
                                      [this](const std::string &objectKey) {
                                          LOG(INFO) << FormatString("Remove directory: %s", objectKey);
                                          objectDirectoryTable_->RemoveObjectDirectory(objectKey);
                                      });
}

Status MasterDevOcManager::PutP2PMetaImpl(const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp)
{
    (void)resp;
    for (auto &subReq : req.dev_obj_meta()) {
        const std::string &objectKey = subReq.object_key();
        LOG(INFO) << FormatString("Master received PutP2PMeta req, objectKey: %s", objectKey);

        // add object lock
        TbbLockTable::accessor pLock;
        Raii eraseLock([this, &pLock]() { (void)objectKeyLockTable_->erase(pLock); });
        (void)objectKeyLockTable_->insert(pLock, objectKey);

        // Step 1: Update the key value table, i.e., object directory (objKey -> object directory).
        std::shared_ptr<ObjectDirectory> objDirectory;

        RETURN_IF_NOT_OK(objectDirectoryTable_->PutObject(objectKey, subReq, objDirectory));

        for (const auto &iter : subReq.locations()) {
            auto rc = deviceMetaOpRecordTable_->AddValue(RecordType::OBJECTKEY, iter.client_id(), objectKey);
            if (rc != Status::OK()) {
                LOG(ERROR) << FormatString("ERROR addvalue client %s, objectKey: %s", iter.client_id(), objectKey);
            }
        }

        INJECT_POINT("MasterDevOcManager.PutP2PMeta.SleepBeforeUpdate");

        // Step 2: Update the key request table, i.e., get request (objKey -> get requests).
        // Once the request return condition is satisfied, a request returning callback will be invoked.
        LOG_IF_ERROR(objectGetDataInfosReqSubscriptionTable_->UpdateGetDataInfosRequest(objectKey, objDirectory),
                     FormatString("Failed to update get requests for objectKey %s.", objectKey));

        INJECT_POINT("MasterDevOcManager.PutP2PMetaImpl.TestPutGetMultThreadSync", []() {
            LOG(INFO) << "MasterDevOcManager.PutP2PMetaImpl.TestPutGetMultThreadSync";
            return Status::OK();
        });
        auto p2pReqVec = objectGetP2PMetaReqSubscriptionTable_->GetAllP2PMetaRequest(objectKey);
        // For subscriptions of unary key buffers only
        auto locs = req.dev_obj_meta().begin()->locations();
        std::string srcClientId = locs.begin()->client_id();
        int32_t srcDeviceId = locs.begin()->device_id();
        std::string srcWorkerIP = locs.begin()->worker_ip();
        LOG_IF_ERROR(objectGetP2PMetaReqSubscriptionTable_->UpdateGetP2PMetaRequest(objectKey, srcClientId, srcDeviceId,
                                                                                    srcWorkerIP),
                     FormatString("Failed to update GetP2PMeta requests for objectKey %s.", objectKey));
        for (auto &getP2PReq : p2pReqVec) {
            if (getP2PReq != nullptr && getP2PReq->isReturn_) {
                auto &dstClientId = getP2PReq->clientId_;
                auto &dstDeviceId = getP2PReq->deviceId_;
                auto &dstWorkerIP = getP2PReq->workerIP_;
                bool isSameNode = (dstWorkerIP == srcWorkerIP);
                int32_t srcOffset = 0;
                int32_t length = 0;
                for (auto &devObjMeta : getP2PReq->requestInfo_.dev_obj_meta()) {
                    if (devObjMeta.object_key() == objectKey) {
                        srcOffset = devObjMeta.src_offset();
                        length = devObjMeta.data_infos()[0].count();
                    }
                }
                UpdateNpuPendingGetReqsTable(srcClientId, srcDeviceId, dstClientId, dstDeviceId, objectKey, isSameNode,
                                             srcOffset, length);
            }
        }
    }

    // Record the client request from which worker for future cleanup the client metadata When worker is scaled down.
    auto workerAddr = req.worker_address();
    auto clientId = req.dev_obj_meta().begin()->locations().begin()->client_id();
    return deviceMetaOpRecordTable_->AddValue(RecordType::WORKER2CLIENT, workerAddr, clientId);
}

Status MasterDevOcManager::ProcessGetP2PMetaRequest(
    const GetP2PMetaReqPb &req,
    const std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> &serverApi)
{
    auto process = [this, &req, &serverApi]() {
        GetP2PMetaRspPb resp;
        for (auto &subReq : req.dev_obj_meta()) {
            // add object lock
            TbbLockTable::accessor pLock;
            Raii eraseLock([this, &pLock]() { (void)objectKeyLockTable_->erase(pLock); });
            (void)objectKeyLockTable_->insert(pLock, subReq.object_key());

            auto rc = ProcessSingleGetP2PMetaReq(subReq, resp);
            if (rc.GetCode() == K_NOT_FOUND) {
                if (!resp.dev_obj_resp_meta().empty()) {
                    break;
                }
                const std::string &objectKey = subReq.object_key();
                auto locs = subReq.locations();
                CHECK_FAIL_RETURN_STATUS(locs.size() == 1, K_RUNTIME_ERROR,
                                         "The size of locations from GetP2PMetaReq not equal to 1");
                auto dstClientId = locs.begin()->client_id();
                auto dstDeviceId = locs.begin()->device_id();
                auto dstWorkerIP = locs.begin()->worker_ip();
                auto request = std::make_shared<GetP2PMetaRequest>(std::vector<std::string>{ objectKey }, serverApi,
                                                                   dstClientId, dstDeviceId, req, dstWorkerIP);
                (void)objectGetP2PMetaReqSubscriptionTable_->AddGetP2PMetaRequest(objectKey, request);
                Timer timer;
                int64_t subTimeout = req.sub_timeout();
                return DirectRespOrAddTimer<GetP2PMetaRequest>(
                    request,
                    [timer, subTimeout, this](std::shared_ptr<GetP2PMetaRequest> req) {
                        int64_t elapsed = timer.ElapsedMilliSecond();
                        reqTimeoutDuration.Init(subTimeout - elapsed);
                        return objectGetP2PMetaReqSubscriptionTable_->ReturnGetP2PMetaRequest(std::move(req));
                    },
                    subTimeout);
            } else if (rc.IsError() && rc.GetCode() != K_NOT_FOUND) {
                auto subResp = resp.add_dev_obj_resp_meta();
                subResp->set_object_key(subReq.object_key());
                subResp->mutable_error()->set_error_code(rc.GetCode());
                subResp->mutable_error()->set_error_msg(rc.GetMsg());
            }
        }
        INJECT_POINT("MasterDevOcManager.ProcessGetP2PMetaRequest.CheckLocSelect", []() { return Status::OK(); });
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(resp), "Write reply to client stream failed.");
        return Status::OK();
    };
    auto rc = process();
    if (rc.IsError()) {
        LOG_IF_ERROR(serverApi->SendStatus(rc), "Write reply to client stream failed");
    }

    // Record the client request from which worker for future cleanup the client metadata When worker is scaled down.
    auto workerAddr = req.worker_address();
    auto clientId = req.dev_obj_meta().begin()->locations().begin()->client_id();
    return deviceMetaOpRecordTable_->AddValue(RecordType::WORKER2CLIENT, workerAddr, clientId);
}

Status MasterDevOcManager::ProcessSingleGetP2PMetaReq(const DeviceObjectMetaPb &subReq, GetP2PMetaRspPb &resp)
{
    const std::string &objectKey = subReq.object_key();
    auto locs = subReq.locations();
    CHECK_FAIL_RETURN_STATUS(locs.size() == 1, K_RUNTIME_ERROR,
                             "The size of locations from GetP2PMetaReq not equal to 1");
    auto dstClientId = locs.begin()->client_id();
    auto dstDeviceId = locs.begin()->device_id();
    LOG(INFO) << FormatString("Master received GetP2PMeta Event req, clientId: %s, deviceId: %s, objectKey: %s",
                              dstClientId, dstDeviceId, objectKey);
    std::shared_ptr<ObjectDirectory> objDirectory;
    RETURN_IF_NOT_OK(objectDirectoryTable_->GetObjectDirectory(objectKey, objDirectory));
    DeviceLocationPb *returnLoc = nullptr;
    auto guard = objDirectory->GetLockGuard();
    RETURN_IF_NOT_OK(objDirectory->VerifyGetRequest(subReq));
    auto dataReceiver = ConcatClientAndDeviceId(dstClientId, dstDeviceId);
    auto selectFunc = [this, dataReceiver](std::vector<DeviceLocationPb *> locPtrVec, DeviceLocationPb *&returnLoc) {
        CHECK_FAIL_RETURN_STATUS(locPtrVec.size() != 0, K_RUNTIME_ERROR, "No available location.");
        std::sort(locPtrVec.begin(), locPtrVec.end(), [](const DeviceLocationPb *l, const DeviceLocationPb *r) {
            return l->create_timestamp() > r->create_timestamp();
        });
        bool hasHcclComm = false;
        for (const auto &loc : locPtrVec) {
            auto dataSender = ConcatClientAndDeviceId(loc->client_id(), loc->device_id());
            if (commRelationMap_->Contains(dataReceiver, dataSender)) {
                returnLoc = loc;
                hasHcclComm = true;
                LOG(INFO) << "select: " << dataSender;
                break;
            }
        }
        if (!hasHcclComm) {
            returnLoc = locPtrVec[0];
            LOG(INFO) << "select: " << ConcatClientAndDeviceId(returnLoc->client_id(), returnLoc->device_id());
        }
        return Status::OK();
    };
    RETURN_IF_NOT_OK(objDirectory->SelectLocation(selectFunc, returnLoc));

    auto &srcClientId = returnLoc->client_id();
    auto srcDeviceId = returnLoc->device_id();
    auto &srcWorkerIP = returnLoc->worker_ip();
    auto &dstWorkerIP = subReq.locations(0).worker_ip();
    auto subResp = resp.add_dev_obj_resp_meta();
    subResp->set_src_client_id(srcClientId);
    subResp->set_src_device_id(srcDeviceId);
    subResp->set_object_key(objectKey);
    bool isSameNode = (srcWorkerIP == dstWorkerIP);
    subResp->set_is_same_node(isSameNode);
    int32_t length = subReq.data_infos()[0].count();
    UpdateNpuPendingGetReqsTable(srcClientId, srcDeviceId, dstClientId, dstDeviceId, objectKey, isSameNode,
                                 subReq.src_offset(), length);
    objDirectory->UpdateLocationAfterSelect(returnLoc);
    return Status::OK();
}

void MasterDevOcManager::UpdateNpuPendingGetReqsTable(const std::string &srcClientId, int32_t srcDeviceId,
                                                      const std::string &dstClientId, int32_t dstDeviceId,
                                                      const std::string &objectKey, bool isSameNode, int32_t srcOffset,
                                                      int32_t length)
{
    // Step 1: Update Key Value Table.
    auto srcNPUId = ConcatClientAndDeviceId(srcClientId, srcDeviceId);
    auto event = NpuEvent::CreateGetNotification(objectKey, dstClientId, dstDeviceId, isSameNode, srcOffset, length);
    npuEventsTable_->PutNpuEvent(srcNPUId, event);
    LOG_IF_ERROR(deviceMetaOpRecordTable_->AddValue(RecordType::NPUID, srcClientId, srcNPUId), "add record error");

    // Step 2: Notify the Request Table.
    LOG(INFO) << FormatString(
        "[UpdateNpuPendingGetReqsTable] Notify subscribe event request table for src npu (%s, %d) of object: %s, "
        "dst "
        "npu (%s, %d)",
        srcClientId, srcDeviceId, objectKey, dstClientId, dstDeviceId);
    NotifyNpuEventSubscribe(srcNPUId, objectKey);
}

void MasterDevOcManager::NotifyNpuEventSubscribe(const std::string &srcNPUId, const std::string &objectKey)
{
    if (npuEventsSubscriptionTable_->SrcNpuIdInSubscribe(srcNPUId)) {
        npuEventsTable_->GetNpuEvents(srcNPUId, [&srcNPUId, &objectKey, this](const QueuePtr &queue) {
            LOG_IF_ERROR(npuEventsSubscriptionTable_->UpdateNpuPendingGetReqsTableForSuccess(srcNPUId, queue),
                         FormatString("Failed to update SubscribeReceiveEventRequest for objectKey %s.", objectKey));
        });
    }
}

Status MasterDevOcManager::ProcessSubscribeReceiveEventRequest(
    const SubscribeReceiveEventReqPb &req,
    const std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> &serverApi)
{
    auto srcClientId = req.src_client_id();
    auto srcDeviceId = req.src_device_id();
    auto srcNpuId = ConcatClientAndDeviceId(srcClientId, srcDeviceId);

    auto request = std::make_shared<SubscribeReceiveEventRequest>(std::vector<std::string>{ srcNpuId }, serverApi,
                                                                  srcClientId, srcDeviceId, req);
    // Step 1: To avoid missing subscription notifications, we subscribe and then check the key value table.
    RETURN_IF_NOT_OK(npuEventsSubscriptionTable_->AddSubscribeReceiveEventRequest(srcNpuId, request));
    LOG_IF_ERROR(deviceMetaOpRecordTable_->AddValue(RecordType::NPUID, srcClientId, srcNpuId),
                 "add the record of npuEventsSubscriptionTable_ operation error");

    // Step 2: Check npu get requests.
    npuEventsTable_->GetNpuEvents(srcNpuId, [&request, &srcNpuId](const QueuePtr &queue) {
        request->objects_.emplace(srcNpuId, queue);
        LOG(INFO) << "Src NPUId : " << srcNpuId << " NPU Pending Queue Size " << queue->Size();
        (void)request->numSatisfiedObjects_.fetch_add(1);
    });
    return DirectRespOrAddTimer<SubscribeReceiveEventRequest>(
        request, [this](std::shared_ptr<SubscribeReceiveEventRequest> req) {
            return npuEventsSubscriptionTable_->ReturnFromSubscribeReceiveEventRequest(std::move(req));
        });
}

Status MasterDevOcManager::SendRootInfoImpl(const SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    auto hcclPeerId = GetHcclPeerId(req.src_client_id(), req.src_device_id(), req.dst_client_id(), req.dst_device_id());
    std::string rootInfo = std::string(std::begin(req.root_info()), std::end(req.root_info()));
    // Step 1: Update Key Value Table.

    LOG_IF_ERROR(deviceMetaOpRecordTable_->AddValue(RecordType::HCCLPEERID, req.dst_client_id(), hcclPeerId),
                 "add record error");
    LOG_IF_ERROR(deviceMetaOpRecordTable_->AddValue(RecordType::HCCLPEERID, req.src_client_id(), hcclPeerId),
                 "add record error");

    rootInfoTable_->PutRootInfo(hcclPeerId, rootInfo);

    auto dataSender = ConcatClientAndDeviceId(req.src_client_id(), req.src_device_id());
    auto dataReceiver = ConcatClientAndDeviceId(req.dst_client_id(), req.dst_device_id());
    commRelationMap_->AddEdge(dataReceiver, dataSender);

    // Step 2: Notify the Request Table.
    // Try to update the SubscribeReceiveEvent request because there may be a request subscribed to the objectKey
    RETURN_IF_NOT_OK(rootInfoSubscriptionTable_->UpdateRecvRootInfoRequestForSuccess(hcclPeerId, rootInfo));
    (void)resp;
    return Status::OK();
}

Status MasterDevOcManager::ProcessRecvRootInfoRequest(
    const RecvRootInfoReqPb &req,
    const std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> &serverApi)
{
    auto hcclPeerId = GetHcclPeerId(req.src_client_id(), req.src_device_id(), req.dst_client_id(), req.dst_device_id());
    auto request = std::make_shared<RecvRootInfoRequest>(std::vector<std::string>{ hcclPeerId }, serverApi,
                                                         req.dst_client_id(), req.dst_device_id(), req);

    // Step 1: To avoid missing subscription notifications,
    // we firstly subscribe and then check the key value table.
    RETURN_IF_NOT_OK(rootInfoSubscriptionTable_->AddRecvRootInfoRequest(hcclPeerId, request));

    // Step 2: Check root info.
    rootInfoTable_->GetAndEraseRootInfo(hcclPeerId, [&request, &hcclPeerId](const std::string &rootInfo) {
        request->objects_.emplace(hcclPeerId, RecvRootInfoEntryParams::ConstructRecvRootInfoEntryParams(rootInfo));
        request->numSatisfiedObjects_.fetch_add(1);
    });

    return DirectRespOrAddTimer<RecvRootInfoRequest>(request, [this](std::shared_ptr<RecvRootInfoRequest> req) {
        return rootInfoSubscriptionTable_->ReturnFromRecvRootInfoRequest(std::move(req));
    });
}

Status MasterDevOcManager::ReleaseClientMetaForScaledInWorker(const std::vector<std::string> &removeNodes)
{
    for (const auto &removeNode : removeNodes) {
        ReleaseMetaDataReqPb req;
        ReleaseMetaDataRspPb resp;
        auto clientIds = deviceMetaOpRecordTable_->GetValue(RecordType::WORKER2CLIENT, removeNode);
        for (const auto &clientId : clientIds) {
            req.set_client_id(clientId);
            req.set_worker_address(removeNode);
            RETURN_IF_NOT_OK(DeviceReleaseMetaDataImpl(req, resp));
        }
    }
    return Status::OK();
}

Status MasterDevOcManager::DeviceReleaseMetaDataImpl(const ReleaseMetaDataReqPb &req, ReleaseMetaDataRspPb &resp)
{
    std::string clientId = req.client_id();
    std::string workerAddr = req.worker_address();
    LOG(INFO) << FormatString("Master received req to release dev metadata for client: %s frome worker: %s", clientId,
                              workerAddr);

    // Step 1: use the clientid to find npuid and hcclpeerid
    std::set<std::string> npuIdSet = deviceMetaOpRecordTable_->GetValueAndErase(RecordType::NPUID, clientId);
    std::set<std::string> hcclPeerIdSet = deviceMetaOpRecordTable_->GetValueAndErase(RecordType::HCCLPEERID, clientId);

    // clear connect information
    std::set<std::string> exitClientNpuIds = commRelationMap_->GetClientNpuId(clientId);
    for (const auto &npuId : exitClientNpuIds) {
        std::set<std::string> connectIds;
        commRelationMap_->EraseNode(npuId, connectIds);
        LOG(INFO) << FormatString("Master got erase call from %s to %s", npuId, VectorToString(connectIds));
        for (const auto &connectClient : connectIds) {
            auto tupleData = SplitNpuId(npuId);
            auto event = NpuEvent::CreateCommExitNotification(std::get<0>(tupleData), std::get<1>(tupleData));
            npuEventsTable_->PutNpuEvent(connectClient, event);
            NotifyNpuEventSubscribe(connectClient, "null");
        }
    }

    // Step 2: clear clientId from worker2ClientsTable_
    deviceMetaOpRecordTable_->EraseValue(RecordType::WORKER2CLIENT, workerAddr, clientId);

    // Step 3: for loop the list to erase, rootinfo table and rootinfosub table
    for (const std::string &hcclPeerId : hcclPeerIdSet) {
        rootInfoTable_->EraseRootInfo(hcclPeerId);
        rootInfoSubscriptionTable_->EraseRootInfoSubscription(hcclPeerId);
    }

    // Step 4: use the clientid to find objectkey
    std::set<std::string> objectKeySet = deviceMetaOpRecordTable_->GetValueAndErase(RecordType::OBJECTKEY, clientId);

    for (auto objKey = objectKeySet.begin(); objKey != objectKeySet.end();) {
        (void)objectGetDataInfosReqSubscriptionTable_->EraseDataInfosReqSubscription(*objKey);

        std::shared_ptr<ObjectDirectory> objDir;
        Status rc = objectDirectoryTable_->GetObjectDirectory(*objKey, objDir);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("have not find the object %s in objectDirectoryTable_", *objKey);
            ++objKey;
            continue;
        }
        LOG_IF_ERROR(objDir->ClearClientAllLocations(clientId),
                     "DeviceReleaseMetaDataImpl ClearClientAllLocations ERROR clientid: " + clientId);
        objKey = objectKeySet.erase(objKey);
    }

    if (objectKeySet.size() != 0) {
        LOG(INFO) << FormatString("objKeySet have in-used objKey, Which have not be delete client : %s, objKey: %s",
                                  clientId, VectorToString(objectKeySet));
        for (const auto &objKey : objectKeySet) {
            auto rc = deviceMetaOpRecordTable_->AddValue(RecordType::OBJECTKEY, clientId, objKey);
            if (rc != Status::OK()) {
                LOG(ERROR) << FormatString(
                    "cannot add value in deviceMetaOpRecordTable_ in "
                    "DeviceReleaseMetaDataImpl() client : %s, objectkey: %s",
                    clientId, objKey);
            }
        }
    }

    // Step 4: for loop the list to erase, npuevent table and npusub table
    for (const std::string &npuId : npuIdSet) {
        // Cancel the NpuEventSubscribe rpc to solve the problem that the client needs to wait for the RPC timeout when
        // the client exits.
        auto event = NpuEvent::CreateSubscribeCancelNotification();
        npuEventsTable_->PutNpuEvent(npuId, event);
        NotifyNpuEventSubscribe(npuId, "null");

        npuEventsTable_->EraseNpuEvent(npuId);
        npuEventsSubscriptionTable_->EraseNpuSubscribe(npuId);
    }
    (void)resp;
    return Status::OK();
}

Status MasterDevOcManager::AckRecvFinish(const AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp)
{
    auto &objectKey = req.object_key();
    auto &srcClientId = req.src_client_id();
    auto srcDeviceId = req.src_device_id();
    bool cacheLocation = req.cache_location();
    auto &dstClientId = req.dst_client_id();
    auto dstDeviceId = req.dst_device_id();
    std::shared_ptr<ObjectDirectory> objDir;
    Status rc = objectDirectoryTable_->GetObjectDirectory(objectKey, objDir);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("The object key: %s is not found, ack not work. ", objectKey) << rc.ToString();
        // If meta is not found, just return ok to finish the rpc.
        return Status::OK();
    }
    auto guard = objDir->GetLockGuard();
    // after get entry lock, need check whether dir is deleted, if it's deleted, no need to ack anymore.
    std::shared_ptr<ObjectDirectory> objDirectory;
    auto status = objectDirectoryTable_->GetObjectDirectory(objectKey, objDirectory);
    if (status.IsError()) {
        if (status.GetCode() == K_NOT_FOUND) {
            return Status::OK();
        }
        return status;
    }
    auto lifetime = objDir->GetLifetime();
    if (lifetime == LifetimeType::MOVE) {
        if (cacheLocation) {
            objDir->AddLocation(dstClientId, dstDeviceId);
            LOG_IF_ERROR(deviceMetaOpRecordTable_->AddValue(RecordType::OBJECTKEY, dstClientId, objectKey),
                         "Record location failed.");
            objDir->RemoveLocation(srcClientId, srcDeviceId);
            LOG_IF_ERROR(deviceMetaOpRecordTable_->EraseValue(RecordType::OBJECTKEY, srcClientId, objectKey),
                         "Record location failed.");
        } else {
            objDir->ClearAllLocations();
            deviceMetaOpRecordTable_->EraseValue(RecordType::OBJECTKEY, srcClientId, objectKey);
        }
    } else if (lifetime == LifetimeType::REFERENCE) {
        if (cacheLocation) {
            objDir->AddLocation(dstClientId, dstDeviceId);
            LOG_IF_ERROR(deviceMetaOpRecordTable_->AddValue(RecordType::OBJECTKEY, dstClientId, objectKey),
                         "Record location failed.");
        }
    }
    objDir->AckLocation(srcClientId, srcDeviceId);
    (void)resp;
    return Status::OK();
}

void MasterDevOcManager::NotifyClientClearMeta(const std::string &objectKey, const std::string &srcClientId,
                                               int srcDeviceId)
{
    // Step 1: Update Key Value Table.
    auto srcNPUId = ConcatClientAndDeviceId(srcClientId, srcDeviceId);
    auto event = NpuEvent::CreateLifeTimeExitNotification(objectKey);
    npuEventsTable_->PutNpuEvent(srcNPUId, event);
    LOG_IF_ERROR(deviceMetaOpRecordTable_->AddValue(RecordType::NPUID, srcClientId, srcNPUId),
                 "add npu record in NotifyClientClearMeta error");
    LOG_IF_ERROR(deviceMetaOpRecordTable_->EraseValue(RecordType::OBJECTKEY, srcClientId, objectKey),
                 "earase objectkey NotifyClientClearMeta record error");

    // Step 2: Notify the Request Table.
    LOG(INFO) << FormatString(
        "[NotifyClientClearMeta] Notify subscribe event request table for src npu (%s, %d) of object: %s", srcClientId,
        srcDeviceId, objectKey);
    NotifyNpuEventSubscribe(srcNPUId, objectKey);
}

Status MasterDevOcManager::RemoveP2PLocation(const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp)
{
    const std::string &objectKey = req.object_key();
    const std::string &clientId = req.client_id();
    auto deviceId = req.device_id();
    std::shared_ptr<ObjectDirectory> objDir;
    Status rc = objectDirectoryTable_->GetObjectDirectory(objectKey, objDir);
    if (rc.IsOk()) {
        auto guard = objDir->GetLockGuard();
        return objDir->RemoveLocation(clientId, deviceId);
    } else {
        LOG(INFO) << rc.ToString();
    }
    (void)resp;
    return Status::OK();
}

Status MasterDevOcManager::ProcessGetDataInfoRequest(
    const GetDataInfoReqPb &req,
    const std::shared_ptr<ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> &serverApi)
{
    const std::string &objectKey = req.object_key();
    const std::string &clientId = req.client_id();
    auto request =
        std::make_shared<GetDataInfosRequest>(std::vector<std::string>{ objectKey }, serverApi, clientId, -1, req);

    // Step 1: To avoid missing subscription notifications,
    // we firstly subscribe and then check the key value table.
    (void)objectGetDataInfosReqSubscriptionTable_->AddGetDataInfosRequest(objectKey, request);

    // Step 2: Check object directory of objectKey.
    auto rc = objectDirectoryTable_->GetDataInfos(objectKey, [&request, &objectKey](
                                                                 std::shared_ptr<ObjectDirectory> &objDirectory) {
        request->objects_.emplace(objectKey, GetDataInfosEntryParams::ConstructGetDataInfosEntryParams(objDirectory));
        request->numSatisfiedObjects_.fetch_add(1);
    });
    if (rc.IsError()) {
        objectGetDataInfosReqSubscriptionTable_->RemoveGetDataInfosRequest(request);
        LOG_IF_ERROR(serverApi->SendStatus(rc), "Write reply to client stream failed");
        return Status::OK();
    }

    return DirectRespOrAddTimer<GetDataInfosRequest>(
        request,
        [this](std::shared_ptr<GetDataInfosRequest> req) {
            return objectGetDataInfosReqSubscriptionTable_->ReturnGetDataInfosRequest(std::move(req));
        },
        req.sub_timeout());
}

Status MasterDevOcManager::CheckAndClearDeviceMeta(const std::string &objectKey)
{
    LOG(INFO) << "check and clear device meta: " << objectKey;
    std::shared_ptr<ObjectDirectory> objDirectory;
    auto rc = objectDirectoryTable_->GetObjectDirectory(objectKey, objDirectory);
    if (rc.IsError()) {
        // return ok directly if get not found.
        return Status::OK();
    }
    auto guard = objDirectory->GetLockGuard();
    // after get entry lock, need check whether dir is deleted
    auto status = objectDirectoryTable_->GetObjectDirectory(objectKey, objDirectory);
    if (status.IsError()) {
        if (status.GetCode() == K_NOT_FOUND) {
            return Status::OK();
        }
        return status;
    }
    return objDirectory->ClearAllLocations();
}

MasterDevOcManager::~MasterDevOcManager()
{
    CheckAndClearDeviceMeta::GetInstance().RemoveSubscriber(MASTER_DEV_OC_MANAGER);
}

void MasterDevOcManager::GetDeviceMetasMatch(std::function<bool(const std::string &)> &&matchFunc,
                                             std::vector<std::string> &deviceMetaKeys)
{
    if (matchFunc(P2P_DEFAULT_MASTER)) {
        deviceMetaKeys.emplace_back(P2P_DEFAULT_MASTER);
    }
}

void MasterDevOcManager::FillDeviceMetas(MigrateMetadataReqPb &req)
{
    objectDirectoryTable_->FillMigrateData(req);
    rootInfoTable_->FillMigrateData(req);
    npuEventsTable_->FillMigrateData(req);
    commRelationMap_->FillMigrateData(req);
}

void MasterDevOcManager::SaveMigrationDeviceMeta(const MigrateMetadataReqPb &req)
{
    objectDirectoryTable_->SaveMigrateData(req);
    rootInfoTable_->SaveMigrateData(req);
    npuEventsTable_->SaveMigrateData(req);
    commRelationMap_->SaveMigrateData(req);
}

void MasterDevOcManager::HandleMigrateSuccess()
{
    LOG(INFO) << "Migrate success, start to clear device meta";
    objectDirectoryTable_->Clear();
    rootInfoTable_->Clear();
    npuEventsTable_->Clear();
    commRelationMap_->Clear();
}

bool MasterDevOcManager::CheckDeviceMetasMigrateInfoIsEmpty(const MigrateMetadataReqPb &req)
{
    return req.device_object_metas().empty() && req.root_info_metas().empty() && req.npu_events_metas().empty();
}

Status MasterDevOcManager::DeleteDevObjects(
    const DeleteAllCopyMetaReqPb &request,
    const std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> &serverApi)
{
    PerfPoint point(PerfKey::MASTER_DELETE_OBJECT);
    DeleteAllCopyMetaRspPb resp;
    DeleteDevObjectsImpl(request, resp);
    LOG_IF_ERROR(serverApi->Write(resp), "Write reply to client stream failed.");
    return Status::OK();
}

void MasterDevOcManager::DeleteDevObjectsImpl(const DeleteAllCopyMetaReqPb &request, DeleteAllCopyMetaRspPb &resp)
{
    auto clientId = request.address();
    auto lastRc = Status::OK();
    for (const auto &objKey : request.object_keys()) {
        auto rc = CheckAndClearDeviceMeta(objKey);
        if (rc.IsError()) {
            lastRc = rc;
            resp.add_failed_object_keys(objKey);
        }
    }
    resp.mutable_last_rc()->set_error_code(lastRc.GetCode());
    resp.mutable_last_rc()->set_error_msg(lastRc.GetMsg());
}

Status MasterDevOcManager::GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp)
{
    auto keys = req.object_keys();
    for (const auto &key : keys) {
        std::shared_ptr<ObjectDirectory> objDirectory;
        auto blobSize = rsp.add_dev_meta_infos()->mutable_blob_sizes();
        auto status = objectDirectoryTable_->GetObjectDirectory(key, objDirectory);
        if (status.IsError()) {
            continue;
        }
        auto dataInfos = objDirectory->GetMetaPb().data_infos();
        blobSize->Reserve(dataInfos.size());
        for (const auto &dataInfo : dataInfos) {
            *(blobSize->Add()) = GetBytesFromDataType(static_cast<DataType>(dataInfo.data_type())) * dataInfo.count();
        }
    }
    return Status::OK();
}

}  // namespace master
}  // namespace datasystem
