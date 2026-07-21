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

/**
 * Description: Implements clear-data workflow helper for WorkerOCServiceImpl.
 */

#include "datasystem/worker/object_cache/service/worker_oc_service_clear_data_flow.h"

#include "datasystem/cluster/executor/key_filter.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/object_cache/metadata_recovery_selector.h"
#include "datasystem/worker/object_cache/object_kv.h"

DS_DECLARE_bool(enable_metadata_recovery);

namespace datasystem {
namespace object_cache {
namespace {
constexpr uint64_t CLEAR_OBJECT_RETRY_INTERVAL_MS = 200;
constexpr int CLEAR_DATA_THREAD_NUM = 1;
constexpr size_t CHECK_OBJECT_DATA_LOCATION_BATCH = 500;
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

    std::unordered_set<std::string> requestedIds(requestObjectKeys.begin(), requestObjectKeys.end());
    std::unordered_set<std::string> classifiedIds;
    std::vector<std::string> validatedNeedClearIds;
    validatedNeedClearIds.reserve(rsp.need_clear_object_keys_size());
    bool isValidPartition = requestedIds.size() == requestObjectKeys.size();
    auto validateClassifiedId = [&requestedIds, &classifiedIds, &isValidPartition](const std::string &objectKey) {
        if (requestedIds.count(objectKey) == 0 || !classifiedIds.emplace(objectKey).second) {
            isValidPartition = false;
        }
    };
    for (const auto &objectKey : rsp.need_clear_object_keys()) {
        validateClassifiedId(objectKey);
        validatedNeedClearIds.emplace_back(objectKey);
    }
    for (const auto &objectKey : rsp.no_need_clear_object_keys()) {
        validateClassifiedId(objectKey);
    }
    if (!isValidPartition || classifiedIds.size() != requestedIds.size()) {
        InsertFailedIds(requestObjectKeys, failedIds);
        LOG(ERROR) << "CheckObjectDataLocation returned an invalid object partition, requested size: "
                   << requestObjectKeys.size() << ", classified size: " << classifiedIds.size();
        return;
    }
    needClearObjectKeys.insert(needClearObjectKeys.end(), validatedNeedClearIds.begin(), validatedNeedClearIds.end());
}

void HandleGroupKeysByMetaOwnerFailures(const std::unordered_map<std::string, Status> &failures,
                                        std::unordered_set<std::string> &failedIds)
{
    if (failures.empty()) {
        return;
    }
    struct GetMasterErrorSummary {
        size_t count = 0;
    };
    std::unordered_map<std::string, GetMasterErrorSummary> summaries;
    for (const auto &kv : failures) {
        failedIds.emplace(kv.first);
        auto &summary = summaries[kv.second.ToString()];
        summary.count++;
    }
    for (const auto &kv : summaries) {
        LOG(INFO) << "Get master for ClearObject failed, object size: " << kv.second.count << ", status: " << kv.first;
    }
}

std::shared_ptr<worker::WorkerMasterOCApi> GetWorkerMasterApiForClear(
    const ObjectEndpointPolicy &endpointPolicy,
    const std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> &workerMasterApiManager,
    const HostPort &masterAddr, const std::vector<std::string> &objectKeys, std::unordered_set<std::string> &failedIds)
{
    if (masterAddr.Empty()) {
        InsertFailedIds(objectKeys, failedIds);
        LOG(WARNING) << "Skip ClearObject because master address is unresolved, object size: " << objectKeys.size();
        return nullptr;
    }
    auto rc = endpointPolicy.CheckEndpoint(masterAddr, false);
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

}  // namespace

WorkerOcServiceClearDataFlow::WorkerOcServiceClearDataFlow(
    std::shared_ptr<ObjectTable> objectTable, std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable,
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager,
    std::shared_ptr<WorkerOcServiceGlobalReferenceImpl> gRefProc, std::shared_ptr<WorkerOcServiceDeleteImpl> deleteProc,
    MetaDataRecoveryManager *metadataRecoveryManager, const worker::MetadataRouteResolver &metadataRoute,
    const ObjectEndpointPolicy &endpointPolicy, std::string localAddress)
    : objectTable_(std::move(objectTable)),
      globalRefTable_(std::move(globalRefTable)),
      workerMasterApiManager_(std::move(workerMasterApiManager)),
      gRefProc_(std::move(gRefProc)),
      deleteProc_(std::move(deleteProc)),
      metadataRecoveryManager_(metadataRecoveryManager),
      metadataRoute_(metadataRoute),
      endpointPolicy_(endpointPolicy),
      localAddress_(std::move(localAddress))
{
    exitFlag_ = std::make_shared<std::atomic_bool>(false);
    clearDataThreadPool_ = std::make_shared<ThreadPool>(0, CLEAR_DATA_THREAD_NUM, "scaledown_handle_thread");
}

WorkerOcServiceClearDataFlow::~WorkerOcServiceClearDataFlow()
{
    if (exitFlag_ != nullptr) {
        exitFlag_->store(true);
    }
    clearDataThreadPool_.reset();
}

Status WorkerOcServiceClearDataFlow::SubmitTopologyFailureCleanup(const cluster::TopologyPhaseAction &action,
                                                                  const cluster::IKeyFilter &filter,
                                                                  const std::string &businessOperationId,
                                                                  std::chrono::steady_clock::time_point deadline,
                                                                  const cluster::CancellationToken &cancellation)
{
    CHECK_FAIL_RETURN_STATUS(action.failed.has_value(), K_INVALID, "failure cleanup lacks failed member");
    CHECK_FAIL_RETURN_STATUS(!businessOperationId.empty(), K_INVALID, "empty topology business operation id");
    CHECK_FAIL_RETURN_STATUS(clearDataThreadPool_ != nullptr, K_NOT_READY, "clear-data flow is stopped");
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology data cleanup cancelled");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology data cleanup deadline exceeded");
    OwnedTopologyClearRequest request{ action, businessOperationId, {} };
    request.objectIds.reserve(objectTable_->GetSize());
    for (const auto &entry : *objectTable_) {
        if (filter.Contains(entry.first)) {
            request.objectIds.emplace_back(entry.first);
        }
    }
    SubmitOwnedTopologyCleanup(std::move(request));
    return Status::OK();
}

void WorkerOcServiceClearDataFlow::SubmitOwnedTopologyCleanup(OwnedTopologyClearRequest request)
{
    if (clearDataThreadPool_ == nullptr) {
        LOG(ERROR) << "ClearData thread pool is nullptr, skip topology cleanup.";
        return;
    }
    auto exitFlag = exitFlag_;
    clearDataThreadPool_->Execute([this, request = std::move(request), exitFlag] {
        if (exitFlag != nullptr && exitFlag->load()) {
            return;
        }
        ClearDataRetryIds retryIds;
        ClearMatchedObjects(request.objectIds, retryIds);
        RebuildRefForMatchedObjects(request.objectIds, retryIds);
        if (!retryIds.Empty()) {
            LOG(WARNING) << "Topology cleanup needs retry, operation: " << request.businessOperationId
                         << ", failed object size: " << retryIds.Size();
            SubmitRetryClearDataAsync({}, 0, retryIds);
        }
    });
}

void WorkerOcServiceClearDataFlow::SubmitClearDataAsync(const ClearDataReqPb &req, uint64_t retryTimes)
{
    if (clearDataThreadPool_ == nullptr) {
        LOG(ERROR) << "ClearData thread pool is nullptr, skip clear data.";
        return;
    }
    auto traceID = Trace::Instance().GetTraceID();
    auto exitFlag = exitFlag_;
    clearDataThreadPool_->Execute([this, req, retryTimes, traceID, exitFlag] {
        if (exitFlag != nullptr && exitFlag->load()) {
            return;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        ClearDataRetryIds retryIds;
        auto rc = ClearDataImpl(req, retryIds);
        if (!retryIds.Empty()) {
            LOG(WARNING) << "ClearData async task need retry, retryTimes: " << retryTimes
                         << ", failed object size: " << retryIds.Size() << ", status: " << rc.ToString();
            RetryClearDataAsync(req, retryIds, retryTimes + 1);
            return;
        }
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

void WorkerOcServiceClearDataFlow::SubmitRetryClearDataAsync(const ClearDataReqPb &req, uint64_t retryTimes,
                                                             const ClearDataRetryIds &retryIds)
{
    if (clearDataThreadPool_ == nullptr) {
        LOG(ERROR) << "ClearData thread pool is nullptr, skip retry clear data.";
        return;
    }
    auto traceID = Trace::Instance().GetTraceID();
    auto exitFlag = exitFlag_;
    clearDataThreadPool_->Execute([this, req, retryTimes, retryIds, traceID, exitFlag] {
        if (exitFlag != nullptr && exitFlag->load()) {
            return;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        ClearDataRetryIds nextRetryIds;
        ClearDataRetryImpl(req, retryIds, nextRetryIds);
        if (!nextRetryIds.Empty()) {
            LOG(WARNING) << "Retry ClearData async task need retry, retryTimes: " << retryTimes
                         << ", failed object size: " << nextRetryIds.Size();
            RetryClearDataAsync(req, nextRetryIds, retryTimes + 1);
            return;
        }
    });
}

void WorkerOcServiceClearDataFlow::RetryClearDataAsync(const ClearDataReqPb &req, const ClearDataRetryIds &retryIds,
                                                       uint64_t retryTimes)
{
    TimerQueue::TimerImpl timer;
    auto traceID = Trace::Instance().GetTraceID();
    auto exitFlag = exitFlag_;
    auto retryTask = [this, req, retryIds, retryTimes, traceID, exitFlag] {
        if (exitFlag != nullptr && exitFlag->load()) {
            return;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        SubmitRetryClearDataAsync(req, retryTimes, retryIds);
    };
    LOG_IF_ERROR(TimerQueue::GetInstance()->AddTimer(CLEAR_OBJECT_RETRY_INTERVAL_MS, retryTask, timer),
                 "Add retry ClearData timer failed");
}

Status WorkerOcServiceClearDataFlow::GetMatchObjectIds(const ClearDataReqPb &req, std::vector<std::string> &matchObjIds)
{
    bool includeL2CacheIds = FLAGS_enable_metadata_recovery;
    auto selectionRequest = MetadataRecoverySelector::BuildSelectionRequest(req, includeL2CacheIds);
    if (selectionRequest.Empty()) {
        LOG(INFO) << "range and worker ids all empty";
        return Status::OK();
    }
    LOG(INFO) << selectionRequest.ToString();
    MetadataRecoverySelector selector(objectTable_);
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

void WorkerOcServiceClearDataFlow::ClearDataRetryImpl(const ClearDataReqPb &req, const ClearDataRetryIds &retryIds,
                                                      ClearDataRetryIds &nextRetryIds)
{
    LOG(INFO) << "retry clear data without meta in worker, clear failed object size: " << retryIds.clearFailedIds.size()
              << ", increase failed object size: " << retryIds.increaseFailedIds.size()
              << ", recover app ref failed object size: " << retryIds.recoverAppRefFailedIds.size();
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
    std::vector<std::string> &requestObjectKeys, std::unordered_set<std::string> &failedIds,
    std::unordered_map<std::string, uint64_t> &queriedVersions) const
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
        queriedVersions[objectKey] = (*currSafeObj)->GetCreateTime();
        requestObjectKeys.emplace_back(objectKey);
    }
}

void WorkerOcServiceClearDataFlow::CheckNeedClearObjectsByMasterInBatches(
    const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi, const std::vector<std::string> &objectKeys,
    std::vector<std::string> &needClearObjectKeys, std::unordered_set<std::string> &failedIds,
    std::unordered_map<std::string, uint64_t> &queriedVersions) const
{
    for (size_t start = 0; start < objectKeys.size(); start += CHECK_OBJECT_DATA_LOCATION_BATCH) {
        size_t end = std::min(start + CHECK_OBJECT_DATA_LOCATION_BATCH, objectKeys.size());
        std::vector<std::string> batchObjectKeys(objectKeys.begin() + start, objectKeys.begin() + end);
        master::CheckObjectDataLocationReqPb req;
        std::vector<std::string> requestObjectKeys;
        FillCheckObjectDataLocationReq(batchObjectKeys, req, requestObjectKeys, failedIds, queriedVersions);
        if (req.object_versions_size() == 0) {
            continue;
        }

        master::CheckObjectDataLocationRspPb rsp;
        auto rc = workerMasterApi->CheckObjectDataLocation(req, rsp);
        HandleCheckObjectDataLocationRsp(rc, rsp, requestObjectKeys, needClearObjectKeys, failedIds);
    }
}

void WorkerOcServiceClearDataFlow::FilterObjectsNeedClearByMaster(
    const std::vector<std::string> &objectKeys, std::vector<std::string> &needClearObjectKeys,
    std::unordered_set<std::string> &failedIds, std::unordered_map<std::string, uint64_t> &queriedVersions)
{
    if (objectKeys.empty()) {
        return;
    }
    auto grouped = metadataRoute_.GroupOwners(objectKeys);
    auto &objKeysGrpByMasterId = grouped.groups;
    HandleGroupKeysByMetaOwnerFailures(grouped.failures, failedIds);

    for (auto &item : objKeysGrpByMasterId) {
        auto workerMasterApi =
            GetWorkerMasterApiForClear(endpointPolicy_, workerMasterApiManager_, item.first, item.second, failedIds);
        if (workerMasterApi == nullptr) {
            continue;
        }
        CheckNeedClearObjectsByMasterInBatches(workerMasterApi, item.second, needClearObjectKeys, failedIds,
                                               queriedVersions);
    }
}

void WorkerOcServiceClearDataFlow::ClearMatchedObjects(const std::vector<std::string> &matchObjIds,
                                                       ClearDataRetryIds &retryIds)
{
    std::vector<std::string> needClearObjIds;
    std::unordered_map<std::string, uint64_t> queriedVersions;
    FilterObjectsNeedClearByMaster(matchObjIds, needClearObjIds, retryIds.clearFailedIds, queriedVersions);
    ClearNeedClearObjects(needClearObjIds, retryIds);
    LOG(INFO) << "clear data without meta in worker finished";
}

void WorkerOcServiceClearDataFlow::ClearObject(const std::vector<std::string> &objectKeys)
{
    (void)ClearObjectsWithSummary(objectKeys, true);
}

WorkerOcServiceClearDataFlow::ClearObjectSummary WorkerOcServiceClearDataFlow::ClearObjectsWithSummary(
    const std::vector<std::string> &objectKeys, bool logObjectKeys)
{
    return ClearObjectsWithSummaryImpl(objectKeys, logObjectKeys, nullptr);
}

WorkerOcServiceClearDataFlow::ClearObjectSummary WorkerOcServiceClearDataFlow::ClearObjectsWithSummary(
    const std::vector<std::string> &objectKeys, bool logObjectKeys,
    const std::unordered_map<std::string, uint64_t> &expectedVersions)
{
    return ClearObjectsWithSummaryImpl(objectKeys, logObjectKeys, &expectedVersions);
}

WorkerOcServiceClearDataFlow::ClearObjectSummary WorkerOcServiceClearDataFlow::ClearObjectsWithSummaryImpl(
    const std::vector<std::string> &objectKeys, bool logObjectKeys,
    const std::unordered_map<std::string, uint64_t> *expectedVersions)
{
    METRIC_TIMER(metrics::KvMetricId::WORKER_CLEANUP_BATCH_LATENCY);
    ClearObjectSummary summary;
    for (const auto &objectKey : objectKeys) {
        std::shared_ptr<SafeObjType> entry;
        bool isInsert = false;
        auto status = objectTable_->ReserveGetAndLock(objectKey, entry, isInsert);
        if (status.IsError()) {
            if (status.GetCode() == K_NOT_FOUND) {
                summary.clearedCount++;
                continue;
            }
            if (logObjectKeys) {
                LOG(WARNING) << FormatString("objectKey: %s ReserveGetAndLock failed, status: %s", objectKey,
                                             status.ToString());
            }
            summary.unresolvedCount++;
            summary.unresolvedIds.emplace(objectKey);
            continue;
        }
        Raii unlock([&entry]() { entry->WUnlock(); });
        if (expectedVersions != nullptr) {
            auto versionIt = expectedVersions->find(objectKey);
            if (versionIt == expectedVersions->end() || entry->Get()->GetCreateTime() != versionIt->second) {
                summary.unresolvedCount++;
                summary.unresolvedIds.emplace(objectKey);
                continue;
            }
        }
        ObjectKV objectKV(objectKey, *entry);
        auto clearRc = deleteProc_->ClearObject(objectKV, expectedVersions != nullptr);
        if (clearRc.IsError()) {
            if (logObjectKeys) {
                LOG(WARNING) << FormatString("Failed to erase object %s from object table, status: %s", objectKey,
                                             clearRc.ToString());
            }
            summary.unresolvedCount++;
            summary.unresolvedIds.emplace(objectKey);
            continue;
        }
        summary.clearedCount++;
    }
    return summary;
}

void WorkerOcServiceClearDataFlow::FinalizeRetryMetadataRecoveryResult(
    const std::vector<std::string> &uniqueFailedObjectKeys,
    const std::unordered_set<std::string> &authorityCheckFailedIds, const ClearObjectSummary &clearSummary,
    RetryMetadataRecoveryResult &result) const
{
    result.clearedCount = clearSummary.clearedCount;
    result.unresolvedIds = authorityCheckFailedIds;
    result.unresolvedIds.insert(clearSummary.unresolvedIds.begin(), clearSummary.unresolvedIds.end());
    result.unresolvedCount = result.unresolvedIds.size();
    if (result.unresolvedCount != 0) {
        result.status = Status(K_RUNTIME_ERROR, "failed to clear unrecoverable metadata");
    }
    if (result.recoveredCount + result.clearedCount + result.unresolvedCount != uniqueFailedObjectKeys.size()) {
        result.status = Status(K_RUNTIME_ERROR, "metadata recovery cleanup returned inconsistent evidence");
        result.unresolvedCount = uniqueFailedObjectKeys.size();
        result.unresolvedIds.clear();
        result.unresolvedIds.insert(uniqueFailedObjectKeys.begin(), uniqueFailedObjectKeys.end());
    }
}

bool WorkerOcServiceClearDataFlow::BuildValidRetryFailedIds(
    const std::unordered_set<std::string> &requestedIds, const MetaDataRecoveryManager::RecoverySummary &retrySummary,
    size_t expectedCount, std::unordered_set<std::string> &retryFailedIds) const
{
    retryFailedIds.clear();
    bool valid = retrySummary.requestedCount == expectedCount;
    for (const auto &objectKey : retrySummary.failedIds) {
        if (requestedIds.count(objectKey) == 0 || !retryFailedIds.emplace(objectKey).second) {
            valid = false;
        }
    }
    return valid && retrySummary.recoveredCount + retryFailedIds.size() == expectedCount;
}

auto WorkerOcServiceClearDataFlow::RetryFailedMetadataRecoveryAndClearUnrecoverable(
    const std::vector<std::string> &failedObjectKeys) -> RetryMetadataRecoveryResult
{
    RetryMetadataRecoveryResult result;
    std::unordered_set<std::string> requestedIds;
    std::vector<std::string> uniqueFailedObjectKeys;
    uniqueFailedObjectKeys.reserve(failedObjectKeys.size());
    for (const auto &objectKey : failedObjectKeys) {
        if (requestedIds.emplace(objectKey).second) {
            uniqueFailedObjectKeys.emplace_back(objectKey);
        }
    }
    if (uniqueFailedObjectKeys.empty()) {
        return result;
    }
    if (metadataRecoveryManager_ == nullptr) {
        result.status = Status(K_RUNTIME_ERROR, "metadata recovery manager is null");
        result.unresolvedCount = uniqueFailedObjectKeys.size();
        result.unresolvedIds.insert(uniqueFailedObjectKeys.begin(), uniqueFailedObjectKeys.end());
        return result;
    }

    INJECT_POINT_NO_RETURN("WorkerOcServiceClearDataFlow.BeforeRetryFailedMetadataRecovery");
    auto retrySummary = metadataRecoveryManager_->RecoverMetadataWithSummary(uniqueFailedObjectKeys, "");
    std::unordered_set<std::string> retryFailedIds;
    if (!BuildValidRetryFailedIds(requestedIds, retrySummary, uniqueFailedObjectKeys.size(), retryFailedIds)) {
        result.status = Status(K_RUNTIME_ERROR, "metadata recovery retry returned inconsistent evidence");
        result.unresolvedCount = uniqueFailedObjectKeys.size();
        result.unresolvedIds.insert(uniqueFailedObjectKeys.begin(), uniqueFailedObjectKeys.end());
        return result;
    }

    result.recoveredCount = retrySummary.recoveredCount;
    if (retrySummary.failedIds.empty()) {
        return result;
    }

    std::vector<std::string> uniqueRetryFailedIds(retryFailedIds.begin(), retryFailedIds.end());
    std::vector<std::string> needClearObjectKeys;
    std::unordered_set<std::string> authorityCheckFailedIds;
    std::unordered_map<std::string, uint64_t> queriedVersions;
    FilterObjectsNeedClearByMaster(uniqueRetryFailedIds, needClearObjectKeys, authorityCheckFailedIds, queriedVersions);

    std::unordered_set<std::string> needClearIds(needClearObjectKeys.begin(), needClearObjectKeys.end());
    for (const auto &objectKey : retryFailedIds) {
        if (needClearIds.count(objectKey) == 0 && authorityCheckFailedIds.count(objectKey) == 0) {
            result.recoveredCount++;
        }
    }

    if (!needClearObjectKeys.empty()) {
        INJECT_POINT_NO_RETURN("WorkerOcServiceClearDataFlow.BeforeClearUnrecoverableObjects");
    }
    auto clearSummary = ClearObjectsWithSummary(needClearObjectKeys, false, queriedVersions);
    FinalizeRetryMetadataRecoveryResult(uniqueFailedObjectKeys, authorityCheckFailedIds, clearSummary, result);
    return result;
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

void WorkerOcServiceClearDataFlow::ClearNeedClearObjects(const std::vector<std::string> &needClearObjIds,
                                                         ClearDataRetryIds &retryIds)
{
    if (needClearObjIds.empty()) {
        return;
    }
    auto result = RetryFailedMetadataRecoveryAndClearUnrecoverable(needClearObjIds);
    retryIds.clearFailedIds.insert(result.unresolvedIds.begin(), result.unresolvedIds.end());
    if (result.status.IsError()) {
        LOG(ERROR) << "Recover or clear local objects without metadata failed, unresolved size: "
                   << result.unresolvedCount << ", status: " << result.status.ToString();
    }
}
}  // namespace object_cache
}  // namespace datasystem
