/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Implement the object cache remote services on the master.
 */

#include "datasystem/master/object_cache/master_oc_service_impl.h"

#include <chrono>
#include <memory>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/master/object_cache/device/master_dev_oc_manager.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"
#include "datasystem/protos/master_object.service.rpc.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"

namespace datasystem {
namespace master {
static constexpr int ASYNC_MIN_THREAD_NUM = 1;
static constexpr int ASYNC_MAX_THREAD_NUM = 4;

MasterOCServiceImpl::MasterOCServiceImpl(HostPort serverAddress, std::shared_ptr<PersistenceApi> persistApi,
                                         std::shared_ptr<AkSkManager> akSkManager,
                                         ReplicaManager *replicaManager,
                                         ResourceManager *resourceManager)
    : MasterOCService(serverAddress),
      masterAddress_(std::move(serverAddress)),
      persistenceApi_(persistApi),
      akSkManager_(akSkManager),
      replicaManager_(replicaManager),
      resourceManager_(resourceManager)
{
}

MasterOCServiceImpl::~MasterOCServiceImpl()
{
}

void MasterOCServiceImpl::Shutdown()
{
    LOG(INFO) << "MasterOCServiceImpl exit";
    OCMigrateMetadataManager::Instance().Shutdown();
}

Status MasterOCServiceImpl::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(etcdCM_);
    reconciliationAsyncPool_ =
        std::make_unique<ThreadPool>(ASYNC_MIN_THREAD_NUM, ASYNC_MAX_THREAD_NUM, "Reconciliation");
    RETURN_IF_NOT_OK(OCMigrateMetadataManager::Instance().Init(masterAddress_, akSkManager_, etcdCM_, replicaManager_));
    return Status::OK();
}

Status MasterOCServiceImpl::GIncNestedRef(const GIncNestedRefReqPb &req, GIncNestedRefRspPb &resp)
{
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Master recv GIncNestedRef req: %s", LogHelper::IgnoreSensitive(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->IncreaseNestedRefCnt(req, resp),
                                     "Inc global nested refs failed with a error");
    masterOperationTimeCost.Append("Total GIncNestedRef", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master GIncNestedRef %s", masterOperationTimeCost.GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::GDecNestedRef(const GDecNestedRefReqPb &req, GDecNestedRefRspPb &resp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Master recv GDecNestedRef req: %s", LogHelper::IgnoreSensitive(req));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->DecreaseNestedRefCnt(req, resp),
                                     "Dec global nested refs failed with error");
    masterOperationTimeCost.Append("Total GDecNestedRef", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master GDecNestedRef %s", masterOperationTimeCost.GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::CreateMeta(const CreateMetaReqPb &req, CreateMetaRspPb &rsp)
{
    constexpr uint64_t logMinTimeMs = 1;
    INJECT_POINT("master.CreateMeta.begin");
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    PerfPoint point(PerfKey::MASTER_CREATE_META);
    LOG(INFO) << FormatString("Processing CreateMetaReq, redirect: %d", req.redirect());

    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    // Call MetadataManager to create the object meta.
    Status status = ocMetadataManager->CreateMeta(req, rsp);
    if (!status.IsOk()) {
        LOG(ERROR) << FormatString("[ObjectKey %s] Create object failed with error: %s", req.meta().object_key(),
                                   status.ToString());
    } else {
        INJECT_POINT("MasterOCServiceImpl.CreateMeta.idempotence");
    }
    point.Record();
    masterOperationTimeCost.Append("Total CreateMeta", timer.ElapsedMilliSecond());
    if (timer.ElapsedMilliSecond() > logMinTimeMs) {
        LOG(INFO) << FormatString("The operations of master CreateMeta %s", masterOperationTimeCost.GetInfo());
    }
    return status;
}

Status MasterOCServiceImpl::CreateMultiMeta(const CreateMultiMetaReqPb &req, CreateMultiMetaRspPb &rsp)
{
    INJECT_POINT("master.CreateMultiMeta.begin");
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    Status status = ocMetadataManager->CreateMultiMeta(req, rsp);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("CreateMultiMeta objects failed with error: %s", status.ToString());
    } else {
        VLOG(1) << FormatString("Master %s CreateMultiMeta rsp: %s", GetLocalAddr().ToString(),
                                LogHelper::IgnoreSensitive(rsp));
    }
    masterOperationTimeCost.Append("Total CreateMultiMeta", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master CreateMultiMeta %s", masterOperationTimeCost.GetInfo());
    return status;
}

Status MasterOCServiceImpl::CreateMultiMetaPhaseTwo(const CreateMultiMetaPhaseTwoReqPb &req, CreateMultiMetaRspPb &rsp)
{
    INJECT_POINT("master.CreateMultiMetaPhaseTwo.begin");
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    Status status = ocMetadataManager->CreateMultiMetaPhaseTwo(req, rsp);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("CreateMultiMetaPhaseTwo objects failed with error: %s", status.ToString());
    } else {
        LOG(INFO) << "CreateMultiMetaPhaseTwo finished";
    }
    masterOperationTimeCost.Append("Total CreateMultiMetaPhaseTwo", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master CreateMultiMetaPhaseTwo %s", masterOperationTimeCost.GetInfo());
    return status;
}

Status MasterOCServiceImpl::CreateCopyMeta(const CreateCopyMetaReqPb &req, CreateCopyMetaRspPb &rsp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    PerfPoint point(PerfKey::MASTER_CREATE_COPY_META);
    LOG(INFO) << FormatString("Processing CreateCopyMetaReq, redirect: %d", req.redirect());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    // Call MetadataManager to create the object meta.
    Status status = ocMetadataManager->CreateCopyMeta(req, rsp);
    if (!status.IsOk()) {
        if (status.GetCode() == K_NOT_FOUND) {
            LOG(INFO) << FormatString("[ObjectKey %s] meta already deleted", req.object_key());
        } else {
            LOG(ERROR) << FormatString("[ObjectKey %s] Create copy meta failed with error: %s", req.object_key(),
                                       status.ToString());
        }
    } else {
        LOG(INFO) << FormatString("[ObjectKey %s] Create copy meta success", req.object_key());
        INJECT_POINT("MasterOCServiceImpl.CreateCopyMeta.idempotence");
    }
    VLOG(1) << FormatString("Master %s CreateCopyMeta rsp: %s", GetLocalAddr().ToString(),
                            LogHelper::IgnoreSensitive(rsp));
    point.Record();
    masterOperationTimeCost.Append("Total CreateCopyMeta", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master CreateCopyMeta %s", masterOperationTimeCost.GetInfo());
    return status;
}

Status MasterOCServiceImpl::CreateMultiCopyMeta(const CreateMultiCopyMetaReqPb &req, CreateMultiCopyMetaRspPb &rsp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    PerfPoint point(PerfKey::MASTER_CREATE_MULTI_COPY_META);
    LOG(INFO) << FormatString("Processing CreateMultiCopyMeta, req: ") << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    Status status = ocMetadataManager->CreateMultiCopyMeta(req, rsp);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("CreateMultiCopyMeta objects failed with error: %s", status.ToString());
    } else {
        LOG(INFO) << "CreateMultiCopyMeta finished";
        VLOG(1) << FormatString("Master %s CreateMultiCopyMeta rsp: %s", GetLocalAddr().ToString(),
                                LogHelper::IgnoreSensitive(rsp));
    }
    point.Record();
    masterOperationTimeCost.Append("Total CreateMultiCopyMeta", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master CreateMultiCopyMeta %s", masterOperationTimeCost.GetInfo());
    return status;
}

Status MasterOCServiceImpl::QueryMeta(const QueryMetaReqPb &req, QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads)
{
    PerfPoint point(PerfKey::MASTER_QUERY_META);
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    INJECT_POINT("MasterOCServiceImpl.QueryMeta.busy");
    LOG(INFO) << FormatString("Processing QueryMetaReq, requestId: %s", req.request_id());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    Status status;
    // Call MetadataManager to query object meta.
    status = ocMetadataManager->QueryMeta(req, rsp, payloads);
    masterOperationTimeCost.Append("Total QueryMeta", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString(
        "QueryMeta on master %s, target num %d, success num %d. The operations of master QueryMeta %s", req.address(),
        req.ids().size(), rsp.query_metas_size(), masterOperationTimeCost.GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetDeviceOcManager(GetDbName(), masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->GetMetaInfo(req, rsp);
}

Status MasterOCServiceImpl::RemoveMeta(const RemoveMetaReqPb &req, RemoveMetaRspPb &rsp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });

    PerfPoint point(PerfKey::MASTER_REMOVE_META);
    INJECT_POINT("master.remove_meta");
    LOG(INFO) << FormatString("Master received RemoveMeta req: %s", LogHelper::IgnoreSensitive(req));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    // Call MetadataManager to remove object meta.
    Status status = ocMetadataManager->RemoveMeta(req, rsp);
    LOG_IF_ERROR(status, "RemoveMeta failed");
    point.Record();
    masterOperationTimeCost.Append("Total RemoveMeta", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master RemoveMeta %s", masterOperationTimeCost.GetInfo());
    return status;
}

void MasterOCServiceImpl::AsyncNotifyCrossAzDelete(
    const std::unordered_map<std::string, std::vector<std::string>> &objsNeedAsyncNotify)
{
    masterOperationTimeCost.Clear();
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    auto status = replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager);
    if (status.IsError()) {
        LOG_IF_ERROR(status, "GetOcMetadataManager failed");
        return;
    }
    ocMetadataManager->AsyncNotifyCrossAzDelete(objsNeedAsyncNotify);
}

Status MasterOCServiceImpl::UpdateMeta(const UpdateMetaReqPb &req, UpdateMetaRspPb &rsp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed in UpdateMeta");
    LOG(INFO) << FormatString("Processing UpdateMetaReq, redirect: %d", req.redirect());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });

    // Call MetadataManager to update object meta.
    Status status = ocMetadataManager->UpdateMeta(req, rsp);
    if (!status.IsOk()) {
        LOG(ERROR) << "UpdateMeta failed with error: " << status.ToString();
    }
    VLOG(1) << FormatString("Master %s UpdateMeta rsp: %s", GetLocalAddr().ToString(), LogHelper::IgnoreSensitive(rsp));
    INJECT_POINT("master.update_meta_failure");
    masterOperationTimeCost.Append("Total UpdateMeta", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master UpdateMeta %s", masterOperationTimeCost.GetInfo());
    return status;
}

Status MasterOCServiceImpl::GetObjectLocations(const GetObjectLocationsReqPb &req, GetObjectLocationsRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master " << GetLocalAddr().ToString()
              << " received GetObjectLocations req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    std::vector<ObjectLocationInfoPb> locations;
    locations.reserve(req.object_keys().size());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->GetObjectLocations(req, locations),
                                     "Master get object locations failed");
    *resp.mutable_location_infos() = { locations.begin(), locations.end() };
    return Status::OK();
}

Status MasterOCServiceImpl::DeleteAllCopyMeta(
    std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> serverApi)
{
    DeleteAllCopyMetaReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Processing DeleteAllCopyMetaReq: objects[%s]", VectorToString(req.object_keys()));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    if (req.are_device_objects()) {
        return ocMetadataManager->GetDeviceOcManager()->DeleteDevObjects(req, serverApi);
    }

    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    ocMetadataManager->DeleteAllCopyMetaWithServerApi(req, serverApi);
    return Status::OK();
}

Status MasterOCServiceImpl::DeleteAllCopyMeta(const DeleteAllCopyMetaReqPb &req, DeleteAllCopyMetaRspPb &rsp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Processing DeleteAllCopyMetaReq: objects[%s]", VectorToString(req.object_keys()));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    if (req.are_device_objects()) {
        ocMetadataManager->GetDeviceOcManager()->DeleteDevObjectsImpl(req, rsp);
        return Status::OK();
    }
    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    ocMetadataManager->DeleteAllCopyMeta(req, rsp);
    masterOperationTimeCost.Append("Total DeleteAllCopyMeta", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master DeleteAllCopyMeta %s", masterOperationTimeCost.GetInfo());
    return Status::OK();
}

std::unordered_map<std::string, QueryGlobalRefNumReqPb> MasterOCServiceImpl::QueryWorkerGRefReqPbGen(
    const std::unordered_set<std::string> &objectKeys, std::shared_ptr<master::OCMetadataManager> &ocMetadataManager)
{
    // Generate the pb used by each worker, with pb containing the required object_keys.
    std::unordered_map<std::string, QueryGlobalRefNumReqPb> queryTarget;  // ip-port string - pb.
    std::unordered_map<std::string, std::unordered_set<std::string>> refTable;
    ocMetadataManager->globalRefTable_->GetAllRef(refTable);
    for (const auto &objKey : objectKeys) {  // For each object_key in referrer set,
        auto iter = refTable.find(objKey);   // get the set of ref addr of this obj.
        if (iter == refTable.end()) {
            continue;
        }
        for (const auto &addr : iter->second) {      // For each ref addr,
            auto targetPb = queryTarget.find(addr);  // check if this addr is in queryTarget.
            if (targetPb == queryTarget.end()) {     // If not, add new the pb req with the obj.
                QueryGlobalRefNumReqPb req;
                req.add_object_keys(objKey);
                queryTarget[addr] = req;
            } else {  // If yes, add this obj to the req pb.
                targetPb->second.add_object_keys(objKey);
            }
            LOG(INFO) << "[GRef] Generate the targetPb with key:" << addr
                      << ", val: " << LogHelper::IgnoreSensitive(queryTarget.at(addr));
        }
    }
    return queryTarget;
}

Status MasterOCServiceImpl::SendQueryGRefReq(const HostPort &addr, QueryGlobalRefNumReqPb req,
                                             QueryGlobalRefNumRspPb &rsp)
{
    LOG(INFO) << "Master send gRef query to worker: " << addr.ToString();
    auto masterWorkerApi =
        MasterWorkerOCApi::CreateMasterWorkerOCApi(addr, masterAddress_, akSkManager_, masterWorkerOCService_);
    RETURN_IF_NOT_OK(masterWorkerApi->Init());
    RETURN_IF_NOT_OK(masterWorkerApi->QueryGlobalRefNumOnWorker(req, rsp));
    LOG(INFO) << "Master receive gRef query rsp from worker: " << addr.ToString();
    return Status::OK();
}

Status MasterOCServiceImpl::QueryGlobalRefNum(const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received QueryGlobalRefNum req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    timeoutDuration.InitWithPositiveTime(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });

    // Iterate all workers concurrently.
    std::vector<HostPort> allWorkers;
    RETURN_RUNTIME_ERROR_IF_NULL(etcdCM_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdCM_->GetClusterNodeAddresses(allWorkers), "etcdcm get cluster addrs failed");

    std::unordered_set<std::string> reqObjectKeys = { req.object_keys().begin(), req.object_keys().end() };
    std::vector<std::string> objectKeys = { reqObjectKeys.begin(), reqObjectKeys.end() };
    ocMetadataManager->RedirectObjRefs(rsp, req.redirect(), objectKeys);
    if (rsp.ref_is_moving()) {
        return Status::OK();
    }
    if (req.object_keys_size() > 0) {
        std::unordered_map<std::string, QueryGlobalRefNumReqPb> queryTarget =
            QueryWorkerGRefReqPbGen(reqObjectKeys, ocMetadataManager);
        for (const auto &targetWorker : queryTarget) {
            QueryGlobalRefNumRspPb rspWorker;
            HostPort addr;
            LOG(INFO) << "[GRef] Master find the worker: " << targetWorker.first
                      << "has the following objs to check: " << LogHelper::IgnoreSensitive(targetWorker.second);
            addr.ParseString(targetWorker.first);
            auto queryAllReq = targetWorker.second;
            Status rc = SendQueryGRefReq(addr, queryAllReq, rspWorker);
            if (rc.IsOk()) {
                rsp.add_objs_glb_refs()->CopyFrom(rspWorker);
            } else {
                LOG(ERROR) << "Query the Objects Global References on Worker " << addr.ToString() << " Failed.\n"
                           << rc.GetMsg();
            }
        }
        return Status::OK();
    }
    // For query-all scenario.
    for (const auto &worker : allWorkers) {
        QueryGlobalRefNumRspPb rspWorker;
        Status rc = SendQueryGRefReq(worker, req, rspWorker);
        if (rc.IsOk()) {
            rsp.add_objs_glb_refs()->CopyFrom(rspWorker);
        } else {
            LOG(ERROR) << "Query the Objects Global References on Worker " << worker.ToString() << " Failed.\n"
                       << rc.GetMsg();
        }
    }
    return Status::OK();
}

Status MasterOCServiceImpl::GIncreaseMasterAppRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "[Ref] Master received GIncreaseMasterAppRef request, src:" << req.address()
              << ", remoteClientId:" << req.remote_client_id();
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    Status status = ocMetadataManager->GIncreaseMasterAppRef(req, resp);
    if (!status.IsOk()) {
        LOG(ERROR) << "[Ref] Increase global reference: failed with error: " << status.ToString();
    } else {
        LOG(INFO) << "[Reference Counting] Increase Master App reference success";
    }
    resp.mutable_last_rc()->set_error_code(status.GetCode());
    resp.mutable_last_rc()->set_error_msg(status.GetMsg());
    return Status::OK();
}

Status MasterOCServiceImpl::GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "[Ref] Master received GIncreaseRef request, address:" << req.address()
              << ", objects:" << VectorToString(req.object_keys());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    INJECT_POINT("master.GIncrease_ref_failure");
    Status status = ocMetadataManager->GIncreaseRef(req, resp);
    if (!status.IsOk()) {
        LOG(ERROR) << "[Ref] Increase global reference: failed with error: " << status.ToString();
    } else {
        LOG(INFO) << "[Reference Counting] Increase global reference success";
        INJECT_POINT("master.GIncrease_ref_Idempotence");
    }
    resp.mutable_last_rc()->set_error_code(status.GetCode());
    resp.mutable_last_rc()->set_error_msg(status.GetMsg());
    masterOperationTimeCost.Append("Total GIncreaseRef", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master GIncreaseRef %s", masterOperationTimeCost.GetInfo());

    return Status::OK();
}

Status MasterOCServiceImpl::GDecreaseRef(
    std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> serverApi)
{
    masterOperationTimeCost.Clear();
    GDecreaseReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "[Ref] Master received GDecreaseRef request, address:" << req.address()
              << ", objects:" << VectorToString(req.object_keys());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    INJECT_POINT("master.GDecreaseRef.before");
    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    ocMetadataManager->GDecreaseRefWithServerApi(req, serverApi);
    LOG(INFO) << FormatString("The operations of master GDecreaseRef %s", masterOperationTimeCost.GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "[Ref] Master received GDecreaseRef request, address:" << req.address()
              << ", objects:" << VectorToString(req.object_keys());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    INJECT_POINT("master.GDecreaseRef.before");
    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    ocMetadataManager->GDecreaseRef(req, resp);
    masterOperationTimeCost.Append("Total GDecreaseRef", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master GDecreaseRef %s", masterOperationTimeCost.GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("[Ref] Master received ReleaseGRefs req: %s", LogHelper::IgnoreSensitive(req));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    ocMetadataManager->ReleaseGRefs(req, resp);
    return Status::OK();
}

Status MasterOCServiceImpl::ReleaseGRefsOfRemoteClientId(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("[Ref] Master recv ReleaseGRefsOfRemoteClientId: %s", LogHelper::IgnoreSensitive(req));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    ocMetadataManager->ReleaseGRefsOfRemoteClientId(req, resp);
    return Status::OK();
}

Status MasterOCServiceImpl::PushMetaToMaster(const PushMetaToMasterReqPb &req, PushMetaToMasterRspPb &rsp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received PushMetaToMaster req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->ProcessWorkerPushMeta(req, rsp),
                                     "Master process PushMetaToMaster failed");
    masterOperationTimeCost.Append("Total PushMetaToMaster", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master PushMetaToMaster %s", masterOperationTimeCost.GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::RollbackSeal(const RollbackSealReqPb &req, RollbackSealRspPb &rsp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->RollbackSeal(req, rsp), "Master process RollbackSeal failed");
    INJECT_POINT("MasterOCServiceImpl.RollbackSeal.idempotence");
    masterOperationTimeCost.Append("Total RollbackSeal", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master PushMetaToMaster %s", masterOperationTimeCost.GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::IfNeedTriggerReconciliationImpl(const ReconciliationQueryPb &req, ReconciliationRspPb &rsp)
{
    (void)rsp;
    masterOperationTimeCost.Clear();
    Timer timer;
    // This is called by worker when centralized master is used. Do sync reconciliation.
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    HostPort workerAddr;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerAddr.ParseString(req.hostport()), "workeradd parse failed");
    LOG(INFO) << "The master receives a reconciliation request from the worker on " << req.hostport() << ".";
    CHECK_FAIL_RETURN_STATUS(etcdCM_ != nullptr, StatusCode::K_INVALID, "No EtcdClusterManager is provided.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdCM_->IfNeedTriggerReconciliation(workerAddr, req.event_timestamp(), true),
                                     "reconciliation failed");
    masterOperationTimeCost.Append("Total IfNeedTriggerReconciliation", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master IfNeedTriggerReconciliation %s",
                              masterOperationTimeCost.GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::IfNeedTriggerReconciliation(
    std::shared_ptr<ServerUnaryWriterReader<ReconciliationRspPb, ReconciliationQueryPb>> serverApi)
{
    (void)reconciliationAsyncPool_->Submit([this, serverApi] {
        ReconciliationQueryPb req;
        ReconciliationRspPb rsp;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "IfNeedTriggerReconciliation read request failed");
        RETURN_IF_NOT_OK(IfNeedTriggerReconciliationImpl(req, rsp));
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "IfNeedTriggerReconciliation write failed");
        return Status::OK();
    });
    return Status::OK();
}

void MasterOCServiceImpl::AssignLocalWorker(object_cache::MasterWorkerOCServiceImpl *masterWorkerService)
{
    masterWorkerOCService_ = masterWorkerService;  // save a copy of the ptr for the SendQueryGRefReq
}

bool MasterOCServiceImpl::HaveAsyncMetaRequest()
{
    return replicaManager_->HaveAsyncMetaRequest();
}

std::string MasterOCServiceImpl::GetETCDAsyncQueueUsage()
{
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    Status rc = replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager);
    if (rc.IsError()) {
        return "";
    }
    return ocMetadataManager->GetETCDAsyncQueueUsage();
}

std::string MasterOCServiceImpl::GetMasterAsyncPoolUsage()
{
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    Status rc = replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager);
    if (rc.IsError()) {
        return "";
    }
    return ocMetadataManager->GetMasterAsyncPoolUsage();
}

Status MasterOCServiceImpl::MigrateMetadata(const MigrateMetadataReqPb &req, MigrateMetadataRspPb &rsp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    LOG(INFO) << GetDbName() << " save migrate data";
    if (!ocMetadataManager->GetDeviceOcManager()->CheckDeviceMetasMigrateInfoIsEmpty(req)) {
        LOG(ERROR) << "Receive device meta, start to save.";
        ocMetadataManager->GetDeviceOcManager()->SaveMigrationDeviceMeta(req);
    }
    RETURN_IF_NOT_OK(ocMetadataManager->SaveMigrationMetadata(req, rsp));
    if (!req.client_id_refs().empty()) {
        ocMetadataManager->SaveMigrationRemoteClientRefData(req);
    }
    if (!req.sub_metas().empty()) {
        ocMetadataManager->SaveSubscribeData(req);
    }
    masterOperationTimeCost.Append("Total MigrateMetadata", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master MigrateMetadata %s", masterOperationTimeCost.GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::ReportResource(const ResourceReportReqPb &req, ResourceReportRspPb &rsp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    RETURN_IF_NOT_OK(resourceManager_->ReportResource(req, rsp));
    masterOperationTimeCost.Append("Total ReportResource", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master ReportResource %s", masterOperationTimeCost.GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::PutP2PMeta(const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");

    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetDeviceOcManager(GetDbName(), masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->PutP2PMetaImpl(req, resp);
}

Status MasterOCServiceImpl::ReplacePrimary(const ReplacePrimaryReqPb &req, ReplacePrimaryRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Master received ReplacePrimary req: %s, size: %ld", req.new_primary_addr(),
                              req.object_infos_size());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    return ocMetadataManager->ReplacePrimary(req, rsp);
}

Status MasterOCServiceImpl::PureQueryMeta(const PureQueryMetaReqPb &req, PureQueryMetaRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received PureQueryMeta request, object size: " << req.object_keys_size();
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    return ocMetadataManager->PureQueryMeta(req, rsp);
}

Status MasterOCServiceImpl::CheckObjectDataLocation(const CheckObjectDataLocationReqPb &req,
                                                    CheckObjectDataLocationRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received CheckObjectDataLocation request, object size: " << req.object_versions_size()
              << ", address: " << req.address();
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    return ocMetadataManager->CheckObjectDataLocation(req, rsp);
}

Status MasterOCServiceImpl::SubscribeReceiveEvent(
    std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> serverApi)
{
    SubscribeReceiveEventReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received SubscribeReceiveEvent Event req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetDeviceOcManager(GetDbName(), masterDevOcManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterDevOcManager->ProcessSubscribeReceiveEventRequest(req, serverApi),
                                     "Process SubscribeReceiveEvent failed");
    return Status::OK();
}

Status MasterOCServiceImpl::GetP2PMeta(
    std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi)
{
    GetP2PMetaReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetDeviceOcManager(GetDbName(), masterDevOcManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterDevOcManager->ProcessGetP2PMetaRequest(req, serverApi),
                                     "Process GetP2PMeta failed");
    return Status::OK();
}

Status MasterOCServiceImpl::SendRootInfo(const SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received Send RootInfo req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetDeviceOcManager(GetDbName(), masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->SendRootInfoImpl(req, resp);
}

Status MasterOCServiceImpl::RecvRootInfo(
    std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi)
{
    RecvRootInfoReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received Recv RootInfo req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetDeviceOcManager(GetDbName(), masterDevOcManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterDevOcManager->ProcessRecvRootInfoRequest(req, serverApi),
                                     "Process GetP2PMeta failed");
    return Status::OK();
}

Status MasterOCServiceImpl::AckRecvFinish(const AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received AckRecvFinish req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetDeviceOcManager(GetDbName(), masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->AckRecvFinish(req, resp);
}

Status MasterOCServiceImpl::RemoveP2PLocation(const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetDeviceOcManager(GetDbName(), masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->RemoveP2PLocation(req, resp);
}

std::string MasterOCServiceImpl::GetDbName()
{
    if (replicaManager_->MultiReplicaEnabled()) {
        return g_MetaRocksDbName;
    }
    return replicaManager_->GetCurrentWorkerUuid();
}

Status MasterOCServiceImpl::GetDataInfo(
    std::shared_ptr<ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> serverApi)
{
    GetDataInfoReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received GetDataInfo req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetDeviceOcManager(GetDbName(), masterDevOcManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterDevOcManager->ProcessGetDataInfoRequest(req, serverApi),
                                     "Process GetDataInfo failed");
    return Status::OK();
}

Status MasterOCServiceImpl::ReleaseMetaData(const ReleaseMetaDataReqPb &req, ReleaseMetaDataRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetDeviceOcManager(GetDbName(), masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->DeviceReleaseMetaDataImpl(req, resp);
}

Status MasterOCServiceImpl::RollbackMultiMeta(const RollbackMultiMetaReqPb &req, RollbackMultiMetaRspPb &rsp)
{
    INJECT_POINT("master.RollbackMultiMeta.begin");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");

    timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { timeoutDuration.Reset(); });
    LOG(INFO) << FormatString("Processing RollbackMultiMeta, address: %s objectKeys: %s", req.address(),
                              VectorToString(req.object_keys()));
    return ocMetadataManager->RollbackMultiMeta(req, rsp);
}

Status MasterOCServiceImpl::Expire(const ExpireReqPb &req, ExpireRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received ExpireMeta request, object size: " << req.object_keys_size();
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    return ocMetadataManager->Expire(req, rsp);
}

Status MasterOCServiceImpl::LivenessCheck(const LivenessCheckReqPb &req, LivenessCheckRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(GetDbName(), ocMetadataManager),
                                     "GetOcMetadataManager failed");
    (void)rsp;
    return ocMetadataManager->LivenessCheck();
}
}  // namespace master
}  // namespace datasystem
