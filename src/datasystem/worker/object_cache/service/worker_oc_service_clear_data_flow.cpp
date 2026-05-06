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

/**
 * Description: Implements clear-data workflow helper for WorkerOCServiceImpl.
 */

#include "datasystem/worker/object_cache/service/worker_oc_service_clear_data_flow.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/object_cache/metadata_recovery_selector.h"
#include "datasystem/worker/object_cache/object_kv.h"

DS_DECLARE_bool(enable_metadata_recovery);

namespace datasystem {
namespace object_cache {
namespace {
constexpr uint64_t CLEAR_OBJECT_RETRY_INTERVAL_MS = 200;
constexpr int CLEAR_DATA_THREAD_NUM = 1;
constexpr size_t CHECK_OBJECT_DATA_LOCATION_BATCH = 500;
constexpr size_t GET_MASTER_ERROR_SAMPLE_LIMIT = 3;
const std::string WORKER_OC_SERVICE_CLEAR_DATA_FLOW = "WorkerOcServiceClearDataFlow";

void InsertFailedIds(const std::vector<std::string> &objectKeys, std::unordered_set<std::string> &failedIds)
{
    failedIds.insert(objectKeys.begin(), objectKeys.end());
}

void HandleCheckObjectDataLocationRsp(const Status &rc, const master::CheckObjectDataLocationRspPb &rsp,
                                      const std::vector<std::string> &requestObjectKeys,
                                      std::vector<std::string> &needClearObjectKeys,
                                      std::unordered_set<std::string> &failedIds)
{
    if (rc.IsError()) {
        InsertFailedIds(requestObjectKeys, failedIds);
        LOG(ERROR) << "CheckObjectDataLocation failed, status: " << rc.ToString();
        return;
    }
    if (rsp.meta_is_moving() || !rsp.info().empty()) {
        InsertFailedIds(requestObjectKeys, failedIds);
        LOG(WARNING) << "CheckObjectDataLocation need redirect or meta is moving";
        return;
    }
    needClearObjectKeys.insert(needClearObjectKeys.end(), rsp.need_clear_object_keys().begin(),
                               rsp.need_clear_object_keys().end());
}

void HandleGroupObjKeysByMasterErrors(const std::optional<std::unordered_map<std::string, Status>> &errInfos,
                                      std::unordered_set<std::string> &failedIds)
{
    if (!errInfos || errInfos->empty()) {
        return;
    }
    struct GetMasterErrorSummary {
        size_t count = 0;
        std::vector<std::string> sampleObjectKeys;
    };
    std::unordered_map<std::string, GetMasterErrorSummary> summaries;
    for (const auto &kv : *errInfos) {
        failedIds.emplace(kv.first);
        auto &summary = summaries[kv.second.ToString()];
        summary.count++;
        if (summary.sampleObjectKeys.size() < GET_MASTER_ERROR_SAMPLE_LIMIT) {
            summary.sampleObjectKeys.emplace_back(kv.first);
        }
    }
    for (const auto &kv : summaries) {
        LOG(INFO) << "Get master for ClearObject failed, object size: " << kv.second.count
                  << ", status: " << kv.first
                  << ", sample object keys: " << VectorToString(kv.second.sampleObjectKeys);
    }
}

std::shared_ptr<worker::WorkerMasterOCApi> GetWorkerMasterApiForClear(
    EtcdClusterManager *etcdCM,
    const std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> &workerMasterApiManager,
    const MetaAddrInfo &metaAddrInfo, const std::vector<std::string> &objectKeys,
    std::unordered_set<std::string> &failedIds)
{
    HostPort masterAddr = metaAddrInfo.GetAddressAndSaveDbName();
    if (masterAddr.Empty()) {
        InsertFailedIds(objectKeys, failedIds);
        LOG(WARNING) << "Skip ClearObject because master address is unresolved, object size: " << objectKeys.size();
        return nullptr;
    }
    auto rc = etcdCM->CheckConnection(masterAddr);
    if (rc.IsError()) {
        InsertFailedIds(objectKeys, failedIds);
        LOG(ERROR) << "CheckConnection before ClearObject failed, status: " << rc.ToString();
        return nullptr;
    }
    auto workerMasterApi = workerMasterApiManager->GetWorkerMasterApi(masterAddr);
    if (workerMasterApi == nullptr) {
        InsertFailedIds(objectKeys, failedIds);
        LOG(ERROR) << "workerMasterApi get failed, master: " << masterAddr.ToString();
    }
    return workerMasterApi;
}
}

WorkerOcServiceClearDataFlow::WorkerOcServiceClearDataFlow(
    std::shared_ptr<ObjectTable> objectTable, std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable,
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager,
    std::shared_ptr<WorkerOcServiceGlobalReferenceImpl> gRefProc, std::shared_ptr<WorkerOcServiceDeleteImpl> deleteProc,
    MetaDataRecoveryManager *metadataRecoveryManager, EtcdClusterManager *etcdCM, std::string localAddress)
    : objectTable_(std::move(objectTable)),
      globalRefTable_(std::move(globalRefTable)),
      workerMasterApiManager_(std::move(workerMasterApiManager)),
      gRefProc_(std::move(gRefProc)),
      deleteProc_(std::move(deleteProc)),
      metadataRecoveryManager_(metadataRecoveryManager),
      etcdCM_(etcdCM),
      localAddress_(std::move(localAddress))
{
    exitFlag_ = std::make_shared<std::atomic_bool>(false);
    clearDataThreadPool_ = std::make_shared<ThreadPool>(0, CLEAR_DATA_THREAD_NUM, "scaledown_handle_thread");
    HashRingEvent::LocalClearDataWithoutMeta::GetInstance().AddSubscriber(
        WORKER_OC_SERVICE_CLEAR_DATA_FLOW,
        [this](const worker::HashRange &ranges, const std::vector<std::string> &uuids) {
            ClearDataReqPb req;
            for (const auto &range : ranges) {
                auto *reqRange = req.add_ranges();
                reqRange->set_from(range.first);
                reqRange->set_end(range.second);
            }
            *req.mutable_worker_ids() = { uuids.begin(), uuids.end() };
            SubmitClearDataAsync(req, ranges);
            return Status::OK();
        });
}

WorkerOcServiceClearDataFlow::~WorkerOcServiceClearDataFlow()
{
    if (exitFlag_ != nullptr) {
        exitFlag_->store(true);
    }
    clearDataThreadPool_.reset();
    HashRingEvent::LocalClearDataWithoutMeta::GetInstance().RemoveSubscriber(WORKER_OC_SERVICE_CLEAR_DATA_FLOW);
}

void WorkerOcServiceClearDataFlow::SubmitClearDataAsync(const ClearDataReqPb &req,
                                                        const worker::HashRange &clearRanges, uint64_t retryTimes)
{
    if (clearDataThreadPool_ == nullptr) {
        LOG(ERROR) << "ClearData thread pool is nullptr, skip clear data.";
        return;
    }
    auto traceID = Trace::Instance().GetTraceID();
    auto exitFlag = exitFlag_;
    clearDataThreadPool_->Execute([this, req, clearRanges, retryTimes, traceID, exitFlag] {
        if (exitFlag != nullptr && exitFlag->load()) {
            return;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        ClearDataRetryIds retryIds;
        auto rc = ClearDataImpl(req, retryIds);
        if (!retryIds.Empty()) {
            LOG(WARNING) << "ClearData async task need retry, retryTimes: " << retryTimes
                         << ", failed object size: " << retryIds.Size() << ", status: " << rc.ToString();
            RetryClearDataAsync(req, clearRanges, retryIds, retryTimes + 1);
            return;
        }
        LOG_IF_ERROR(HashRingEvent::LocalClearDataWithoutMetaFinish::GetInstance().NotifyAll(clearRanges),
                     "Notify local clear-data finish failed");
        LOG_IF_ERROR(rc, "ClearData async task failed");
    });
}

Status WorkerOcServiceClearDataFlow::ClearObject(const ClearDataReqPb &req)
{
    ClearDataRetryIds retryIds;
    auto rc = ClearDataImpl(req, retryIds);
    if (!retryIds.Empty()) {
        LOG(WARNING) << "ClearData finished with failed object size: " << retryIds.Size();
    }
    return rc;
}

void WorkerOcServiceClearDataFlow::SubmitRetryClearDataAsync(const ClearDataReqPb &req,
                                                             const worker::HashRange &clearRanges,
                                                             uint64_t retryTimes,
                                                             const ClearDataRetryIds &retryIds)
{
    if (clearDataThreadPool_ == nullptr) {
        LOG(ERROR) << "ClearData thread pool is nullptr, skip retry clear data.";
        return;
    }
    auto traceID = Trace::Instance().GetTraceID();
    auto exitFlag = exitFlag_;
    clearDataThreadPool_->Execute([this, req, clearRanges, retryTimes, retryIds, traceID, exitFlag] {
        if (exitFlag != nullptr && exitFlag->load()) {
            return;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        ClearDataRetryIds nextRetryIds;
        ClearDataRetryImpl(req, retryIds, nextRetryIds);
        if (!nextRetryIds.Empty()) {
            LOG(WARNING) << "Retry ClearData async task need retry, retryTimes: " << retryTimes
                         << ", failed object size: " << nextRetryIds.Size();
            RetryClearDataAsync(req, clearRanges, nextRetryIds, retryTimes + 1);
            return;
        }
        LOG_IF_ERROR(HashRingEvent::LocalClearDataWithoutMetaFinish::GetInstance().NotifyAll(clearRanges),
                     "Notify local clear-data finish failed");
    });
}

void WorkerOcServiceClearDataFlow::RetryClearDataAsync(const ClearDataReqPb &req,
                                                       const worker::HashRange &clearRanges,
                                                       const ClearDataRetryIds &retryIds, uint64_t retryTimes)
{
    TimerQueue::TimerImpl timer;
    auto traceID = Trace::Instance().GetTraceID();
    auto exitFlag = exitFlag_;
    auto retryTask = [this, req, clearRanges, retryIds, retryTimes, traceID, exitFlag] {
        if (exitFlag != nullptr && exitFlag->load()) {
            return;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        SubmitRetryClearDataAsync(req, clearRanges, retryTimes, retryIds);
    };
    LOG_IF_ERROR(TimerQueue::GetInstance()->AddTimer(CLEAR_OBJECT_RETRY_INTERVAL_MS, retryTask, timer),
                 "Add retry ClearData timer failed");
}

Status WorkerOcServiceClearDataFlow::GetMatchObjectIds(const ClearDataReqPb &req,
                                                       std::vector<std::string> &matchObjIds)
{
    bool includeL2CacheIds = FLAGS_enable_metadata_recovery;
    auto selectionRequest = MetadataRecoverySelector::BuildSelectionRequest(req, includeL2CacheIds);
    if (selectionRequest.Empty()) {
        LOG(INFO) << "range and worker ids all empty";
        return Status::OK();
    }
    LOG(INFO) << selectionRequest.ToString();
    MetadataRecoverySelector selector(objectTable_, etcdCM_);
    return selector.Select(selectionRequest, matchObjIds);
}

Status WorkerOcServiceClearDataFlow::ClearDataImpl(const ClearDataReqPb &req, ClearDataRetryIds &retryIds)
{
    LOG(INFO) << "clear data in worker, standby worker: " << req.standby_worker();
    std::vector<std::string> matchObjIds;
    RETURN_IF_NOT_OK(GetMatchObjectIds(req, matchObjIds));
    ClearMatchedObjects(matchObjIds, retryIds);
    RebuildRefForMatchedObjects(matchObjIds, retryIds);
    return Status::OK();
}

void WorkerOcServiceClearDataFlow::ClearDataRetryImpl(const ClearDataReqPb &req,
                                                      const ClearDataRetryIds &retryIds,
                                                      ClearDataRetryIds &nextRetryIds)
{
    LOG(INFO) << "retry clear data without meta in worker, clear failed object size: "
              << retryIds.clearFailedIds.size() << ", increase failed object size: "
              << retryIds.increaseFailedIds.size() << ", recover app ref failed object size: "
              << retryIds.recoverAppRefFailedIds.size();
    std::vector<std::string> retryClearObjectKeys{ retryIds.clearFailedIds.begin(), retryIds.clearFailedIds.end() };
    (void)req;
    ClearMatchedObjects(retryClearObjectKeys, nextRetryIds);
    std::vector<std::string> retryIncreaseObjectKeys{ retryIds.increaseFailedIds.begin(),
                                                      retryIds.increaseFailedIds.end() };
    RetryIncreaseMasterRef(retryIncreaseObjectKeys, nextRetryIds);
    std::vector<std::string> retryRecoverAppRefObjectKeys{ retryIds.recoverAppRefFailedIds.begin(),
                                                           retryIds.recoverAppRefFailedIds.end() };
    RetryRecoverMasterAppRef(retryRecoverAppRefObjectKeys, nextRetryIds);
}

void WorkerOcServiceClearDataFlow::FillCheckObjectDataLocationReq(
    const std::vector<std::string> &objectKeys, master::CheckObjectDataLocationReqPb &req,
    std::vector<std::string> &requestObjectKeys, std::unordered_set<std::string> &failedIds) const
{
    req.set_address(localAddress_);
    req.set_redirect(true);
    for (const auto &objectKey : objectKeys) {
        std::shared_ptr<SafeObjType> currSafeObj;
        if (objectTable_->Get(objectKey, currSafeObj).IsError()) {
            continue;
        }
        auto rc = currSafeObj->RLock();
        if (rc.IsError()) {
            failedIds.emplace(objectKey);
            LOG(ERROR) << FormatString("[ObjectKey %s] Lock object failed, status: %s", objectKey, rc.ToString());
            continue;
        }
        Raii unLockRaii([&currSafeObj]() { currSafeObj->RUnlock(); });
        auto *objectVersion = req.add_object_versions();
        objectVersion->set_object_key(objectKey);
        objectVersion->set_version((*currSafeObj)->GetCreateTime());
        requestObjectKeys.emplace_back(objectKey);
    }
}

void WorkerOcServiceClearDataFlow::CheckNeedClearObjectsByMasterInBatches(
    const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi, const std::vector<std::string> &objectKeys,
    std::vector<std::string> &needClearObjectKeys, std::unordered_set<std::string> &failedIds) const
{
    for (size_t start = 0; start < objectKeys.size(); start += CHECK_OBJECT_DATA_LOCATION_BATCH) {
        size_t end = std::min(start + CHECK_OBJECT_DATA_LOCATION_BATCH, objectKeys.size());
        std::vector<std::string> batchObjectKeys(objectKeys.begin() + start, objectKeys.begin() + end);
        master::CheckObjectDataLocationReqPb req;
        std::vector<std::string> requestObjectKeys;
        FillCheckObjectDataLocationReq(batchObjectKeys, req, requestObjectKeys, failedIds);
        if (req.object_versions_size() == 0) {
            continue;
        }

        master::CheckObjectDataLocationRspPb rsp;
        auto rc = workerMasterApi->CheckObjectDataLocation(req, rsp);
        HandleCheckObjectDataLocationRsp(rc, rsp, requestObjectKeys, needClearObjectKeys, failedIds);
    }
}

void WorkerOcServiceClearDataFlow::FilterObjectsNeedClearByMaster(const std::vector<std::string> &objectKeys,
                                                                  std::vector<std::string> &needClearObjectKeys,
                                                                  std::unordered_set<std::string> &failedIds)
{
    if (objectKeys.empty()) {
        return;
    }
    std::unordered_map<MetaAddrInfo, std::vector<std::string>> objKeysGrpByMasterId;
    std::optional<std::unordered_map<std::string, Status>> errInfos;
    errInfos.emplace();
    etcdCM_->GroupObjKeysByMasterHostPortWithStatus(objectKeys, objKeysGrpByMasterId, errInfos);
    HandleGroupObjKeysByMasterErrors(errInfos, failedIds);

    for (auto &item : objKeysGrpByMasterId) {
        if (item.first.Empty()) {
            InsertFailedIds(item.second, failedIds);
            continue;
        }
        auto workerMasterApi =
            GetWorkerMasterApiForClear(etcdCM_, workerMasterApiManager_, item.first, item.second, failedIds);
        if (workerMasterApi == nullptr) {
            continue;
        }
        CheckNeedClearObjectsByMasterInBatches(workerMasterApi, item.second, needClearObjectKeys, failedIds);
    }
}

void WorkerOcServiceClearDataFlow::ClearMatchedObjects(const std::vector<std::string> &matchObjIds,
                                                       ClearDataRetryIds &retryIds)
{
    std::vector<std::string> needClearObjIds;
    FilterObjectsNeedClearByMaster(matchObjIds, needClearObjIds, retryIds.clearFailedIds);
    ClearNeedClearObjects(needClearObjIds);
    LOG(INFO) << "clear data without meta in worker finished";
}

void WorkerOcServiceClearDataFlow::ClearObject(const std::vector<std::string> &objectKeys)
{
    for (const auto &objectKey : objectKeys) {
        std::shared_ptr<SafeObjType> entry;
        bool isInsert = false;
        auto status = objectTable_->ReserveGetAndLock(objectKey, entry, isInsert);
        if (status.IsError()) {
            LOG(WARNING) << FormatString("objectKey: %s ReserveGetAndLock failed, status: %s", objectKey,
                                         status.ToString());
            continue;
        }
        Raii unlock([&entry]() { entry->WUnlock(); });
        ObjectKV objectKV(objectKey, *entry);
        LOG_IF_ERROR(deleteProc_->ClearObject(objectKV),
                     FormatString("Failed to erase object %s from object table", objectKey));
    }
}

void WorkerOcServiceClearDataFlow::FilterObjectsNeedRebuildRefByLocalRef(
    const std::vector<std::string> &objectKeys, std::vector<std::string> &rebuildObjectKeys) const
{
    rebuildObjectKeys.clear();
    rebuildObjectKeys.reserve(objectKeys.size());
    for (const auto &objectKey : objectKeys) {
        if (globalRefTable_->GetRefWorkerCount(objectKey) > 0) {
            rebuildObjectKeys.emplace_back(objectKey);
        }
    }
}

void WorkerOcServiceClearDataFlow::RebuildRefForMatchedObjects(const std::vector<std::string> &matchObjIds,
                                                               ClearDataRetryIds &retryIds)
{
    std::vector<std::string> rebuildObjIds;
    FilterObjectsNeedRebuildRefByLocalRef(matchObjIds, rebuildObjIds);
    if (rebuildObjIds.empty()) {
        return;
    }
    std::vector<std::string> increaseFailedIds;
    std::unordered_set<std::string> rebuildObjSet(rebuildObjIds.begin(), rebuildObjIds.end());
    auto rebuildMatchFunc = [&rebuildObjSet](const std::string &objKey) {
        return rebuildObjSet.find(objKey) != rebuildObjSet.end();
    };
    auto rc = gRefProc_->GIncreaseMasterRefWithLock(rebuildMatchFunc, increaseFailedIds);
    retryIds.increaseFailedIds.insert(increaseFailedIds.begin(), increaseFailedIds.end());
    if (rc.IsError()) {
        LOG(ERROR) << "GIncreaseMasterRefWithLock failed, status: " << rc.ToString();
    }

    std::unordered_set<std::string> increaseFailedIdSet(increaseFailedIds.begin(), increaseFailedIds.end());
    std::vector<std::string> recoverAppRefObjIds;
    for (const auto &objectKey : rebuildObjIds) {
        if (increaseFailedIdSet.find(objectKey) == increaseFailedIdSet.end()) {
            recoverAppRefObjIds.emplace_back(objectKey);
        }
    }
    std::unordered_set<std::string> recoverAppRefObjSet(recoverAppRefObjIds.begin(), recoverAppRefObjIds.end());
    auto recoverAppRefMatchFunc = [&recoverAppRefObjSet](const std::string &objKey) {
        return recoverAppRefObjSet.find(objKey) != recoverAppRefObjSet.end();
    };
    rc = RecoverMasterAppRefEvent::GetInstance().NotifyAll(recoverAppRefMatchFunc, "");
    if (rc.IsError()) {
        retryIds.recoverAppRefFailedIds.insert(recoverAppRefObjIds.begin(), recoverAppRefObjIds.end());
        LOG(ERROR) << "RecoverMasterAppRefEvent failed, status: " << rc.ToString();
    }
}

void WorkerOcServiceClearDataFlow::RetryIncreaseMasterRef(const std::vector<std::string> &objectKeys,
                                                          ClearDataRetryIds &retryIds)
{
    if (objectKeys.empty()) {
        return;
    }
    std::unordered_set<std::string> objectKeySet(objectKeys.begin(), objectKeys.end());
    auto matchFunc = [&objectKeySet](const std::string &objKey) {
        return objectKeySet.find(objKey) != objectKeySet.end();
    };
    std::vector<std::string> increaseFailedIds;
    auto rc = gRefProc_->GIncreaseMasterRefWithLock(matchFunc, increaseFailedIds);
    retryIds.increaseFailedIds.insert(increaseFailedIds.begin(), increaseFailedIds.end());
    if (rc.IsError()) {
        LOG(ERROR) << "GIncreaseMasterRefWithLock failed, status: " << rc.ToString();
    }
}

void WorkerOcServiceClearDataFlow::RetryRecoverMasterAppRef(const std::vector<std::string> &objectKeys,
                                                            ClearDataRetryIds &retryIds)
{
    if (objectKeys.empty()) {
        return;
    }
    std::unordered_set<std::string> objectKeySet(objectKeys.begin(), objectKeys.end());
    auto matchFunc = [&objectKeySet](const std::string &objKey) {
        return objectKeySet.find(objKey) != objectKeySet.end();
    };
    auto rc = RecoverMasterAppRefEvent::GetInstance().NotifyAll(matchFunc, "");
    if (rc.IsError()) {
        retryIds.recoverAppRefFailedIds.insert(objectKeys.begin(), objectKeys.end());
        LOG(ERROR) << "RecoverMasterAppRefEvent failed, status: " << rc.ToString();
    }
}

void WorkerOcServiceClearDataFlow::ClearNeedClearObjects(const std::vector<std::string> &needClearObjIds)
{
    if (needClearObjIds.empty()) {
        return;
    }
    if (FLAGS_enable_metadata_recovery) {
        auto summary = metadataRecoveryManager_->RecoverMetadataWithSummary(needClearObjIds, "");
        if (summary.status.IsError()) {
            LOG(ERROR) << "RecoverMetadataWithSummary failed, status: " << summary.status.ToString();
        }
        ClearObject(summary.failedIds);
    } else {
        ClearObject(needClearObjIds);
    }
}
}  // namespace object_cache
}  // namespace datasystem
