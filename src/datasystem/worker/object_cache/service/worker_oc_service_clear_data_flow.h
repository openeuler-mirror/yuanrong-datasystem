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
 * Description: Defines clear-data workflow helper for WorkerOCServiceImpl.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_OC_SERVICE_CLEAR_DATA_FLOW_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_OC_SERVICE_CLEAR_DATA_FLOW_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/cluster/executor/cancellation_token.h"
#include "datasystem/cluster/executor/key_filter.h"
#include "datasystem/cluster/executor/topology_phase_action.h"
#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/metadata_recovery_manager.h"
#include "datasystem/worker/object_cache/object_endpoint_policy.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_delete_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_global_reference_impl.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/worker_master_api_manager_base.h"

namespace datasystem {
namespace object_cache {
class WorkerOcServiceClearDataFlow {
public:
    /**
     * @brief Outcome of retrying failed restart metadata recovery before local cleanup.
     */
    struct RetryMetadataRecoveryResult {
        // Objects neither rebuilt through retry nor successfully cleared locally.
        size_t recoveredCount{ 0 };
        size_t clearedCount{ 0 };
        size_t unresolvedCount{ 0 };
        std::unordered_set<std::string> unresolvedIds;
        Status status{ Status::OK() };
    };

    /**
     * @brief Failed object ids grouped by clear-data workflow stage.
     */
    struct ClearDataRetryIds {
        std::unordered_set<std::string> clearFailedIds;
        std::unordered_set<std::string> increaseFailedIds;
        std::unordered_set<std::string> recoverAppRefFailedIds;

        bool Empty() const
        {
            return clearFailedIds.empty() && increaseFailedIds.empty() && recoverAppRefFailedIds.empty();
        }

        size_t Size() const
        {
            return clearFailedIds.size() + increaseFailedIds.size() + recoverAppRefFailedIds.size();
        }
    };

    /**
     * @brief Construct clear-data workflow with explicit runtime dependencies.
     * @param[in] objectTable Local object table used for object selection and cleanup.
     * @param[in] globalRefTable Local global-ref table used to determine whether refs need rebuilding.
     * @param[in] workerMasterApiManager Master API manager used for querying masters during cleanup.
     * @param[in] gRefProc Global-ref processor used to rebuild master refs.
     * @param[in] deleteProc Delete processor used to clear local object data.
     * @param[in] metadataRecoveryManager Metadata recovery manager used before local cleanup.
     * @param[in] metadataRoute Metadata owner resolver that outlives this workflow.
     * @param[in] endpointPolicy Object endpoint policy that outlives this workflow.
     * @param[in] localAddress Current worker address string.
     */
    WorkerOcServiceClearDataFlow(
        std::shared_ptr<ObjectTable> objectTable, std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable,
        std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager,
        std::shared_ptr<WorkerOcServiceGlobalReferenceImpl> gRefProc,
        std::shared_ptr<WorkerOcServiceDeleteImpl> deleteProc, MetaDataRecoveryManager *metadataRecoveryManager,
        const worker::MetadataRouteResolver &metadataRoute, const ObjectEndpointPolicy &endpointPolicy,
        std::string localAddress);

    /**
     * @brief Stop clear-data workflow and unsubscribe local clear-data event handlers.
     */
    ~WorkerOcServiceClearDataFlow();

    /**
     * @brief Submit the full clear-data workflow asynchronously.
     * @param[in] req Clear-data request used for initial object selection.
     * @param[in] clearRanges Range markers that remain in-progress until the workflow fully finishes.
     * @param[in] retryTimes Current retry round number.
     */
    void SubmitClearDataAsync(const ClearDataReqPb &req, uint64_t retryTimes = 0);

    /**
     * @brief Execute the full clear-data workflow synchronously.
     * @param[in] req Clear-data request used for initial object selection.
     * @return Status of the initial object-selection step.
     */
    Status ClearObject(const ClearDataReqPb &req);

    /**
     * @brief Remove local object data for the given object ids.
     * @param[in] objectKeys Object ids that should be removed from the local worker.
     */
    void ClearObject(const std::vector<std::string> &objectKeys);

    /**
     * @brief Retry metadata recovery once for exact restart-recovery failures, then clear only retry failures.
     * @param[in] failedObjectKeys Exact object keys from the failed restart metadata recovery attempt.
     * @return Counts for recovered, cleared, and neither-recovered-nor-cleared objects.
     */
    RetryMetadataRecoveryResult RetryFailedMetadataRecoveryAndClearUnrecoverable(
        const std::vector<std::string> &failedObjectKeys);

    /**
     * @brief Materialize one failure scope and submit owned asynchronous cleanup work.
     * @param[in] action Validated Failure participants and task identity.
     * @param[in] filter Final object-key predicate for the task scope.
     * @param[in] businessOperationId Stable idempotency identity for this phase.
     * @param[in] deadline Absolute monotonic deadline for synchronous materialization.
     * @param[in] cancellation Executor-owned cooperative cancellation signal.
     * @return K_OK after owned work is accepted; an error otherwise.
     */
    Status SubmitTopologyFailureCleanup(const cluster::TopologyPhaseAction &action, const cluster::IKeyFilter &filter,
                                        const std::string &businessOperationId,
                                        std::chrono::steady_clock::time_point deadline,
                                        const cluster::CancellationToken &cancellation);

private:
    struct ClearObjectSummary {
        size_t clearedCount{ 0 };
        size_t unresolvedCount{ 0 };
        std::unordered_set<std::string> unresolvedIds;
    };

    struct OwnedTopologyClearRequest {
        cluster::TopologyPhaseAction action;
        std::string businessOperationId;
        std::vector<std::string> objectIds;
    };

    /**
     * @brief Submit callback-independent topology cleanup input to the existing retry workflow.
     * @param[in] request Owned cleanup facts that remain valid after the topology callback returns.
     */
    void SubmitOwnedTopologyCleanup(OwnedTopologyClearRequest request);

    /**
     * @brief Submit retry execution for failed workflow stages.
     * @param[in] req Original clear-data request.
     * @param[in] clearRanges Range markers that remain in-progress until the workflow fully finishes.
     * @param[in] retryTimes Current retry round number.
     * @param[in] retryIds Failed object ids grouped by workflow stage.
     */
    void SubmitRetryClearDataAsync(const ClearDataReqPb &req, uint64_t retryTimes, const ClearDataRetryIds &retryIds);

    /**
     * @brief Schedule the next retry round for failed workflow stages.
     * @param[in] req Original clear-data request.
     * @param[in] clearRanges Range markers that remain in-progress until the workflow fully finishes.
     * @param[in] retryIds Failed object ids grouped by workflow stage.
     * @param[in] retryTimes Next retry round number.
     */
    void RetryClearDataAsync(const ClearDataReqPb &req, const ClearDataRetryIds &retryIds, uint64_t retryTimes);

    /**
     * @brief Select local objects matched by ranges or worker ids in the request.
     * @param[in] req Clear-data request used for object selection.
     * @param[out] matchObjIds Matched local object ids.
     * @return Status of the selection step.
     */
    Status GetMatchObjectIds(const ClearDataReqPb &req, std::vector<std::string> &matchObjIds);

    /**
     * @brief Run one round of clear and ref rebuild for matched objects.
     * @param[in] req Clear-data request used for initial object selection.
     * @param[out] retryIds Failed object ids grouped by workflow stage for later retries.
     * @return Status of the initial object-selection step.
     */
    Status ClearDataImpl(const ClearDataReqPb &req, ClearDataRetryIds &retryIds);

    /**
     * @brief Retry failed clear-data workflow stages with failed object ids.
     * @param[in] req Original clear-data request.
     * @param[in] retryIds Failed object ids grouped by workflow stage.
     * @param[out] nextRetryIds Failed object ids that still need another retry round.
     */
    void ClearDataRetryImpl(const ClearDataReqPb &req, const ClearDataRetryIds &retryIds,
                            ClearDataRetryIds &nextRetryIds);

    /**
     * @brief Build one master-side data-location check request for the given object keys.
     * @param[in] objectKeys Candidate local object ids in the current batch.
     * @param[out] req Built request sent to master.
     * @param[out] requestObjectKeys Object ids that were successfully added to the request.
     * @param[out] failedIds Object ids whose local state could not be read or locked.
     */
    void FillCheckObjectDataLocationReq(const std::vector<std::string> &objectKeys,
                                        master::CheckObjectDataLocationReqPb &req,
                                        std::vector<std::string> &requestObjectKeys,
                                        std::unordered_set<std::string> &failedIds,
                                        std::unordered_map<std::string, uint64_t> &queriedVersions) const;

    /**
     * @brief Query one master's batches to decide which local objects still need clearing.
     * @param[in] workerMasterApi Master api for the current grouped objects.
     * @param[in] objectKeys Candidate local object ids routed to the same master.
     * @param[out] needClearObjectKeys Object ids that still need local cleanup.
     * @param[out] failedIds Object ids whose master-side check failed.
     */
    void CheckNeedClearObjectsByMasterInBatches(const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                                                const std::vector<std::string> &objectKeys,
                                                std::vector<std::string> &needClearObjectKeys,
                                                std::unordered_set<std::string> &failedIds,
                                                std::unordered_map<std::string, uint64_t> &queriedVersions) const;

    /**
     * @brief Ask masters which matched objects still need local cleanup.
     * @param[in] objectKeys Candidate local object ids.
     * @param[out] needClearObjectKeys Object ids that still need local cleanup.
     * @param[out] failedIds Object ids whose master-side check failed.
     */
    void FilterObjectsNeedClearByMaster(const std::vector<std::string> &objectKeys,
                                        std::vector<std::string> &needClearObjectKeys,
                                        std::unordered_set<std::string> &failedIds,
                                        std::unordered_map<std::string, uint64_t> &queriedVersions);

    /**
     * @brief Validate the retry summary and materialize unique retry-failed ids.
     * @param[in] requestedIds Unique ids submitted to the retry.
     * @param[in] retrySummary Summary returned by metadata recovery retry.
     * @param[in] expectedCount Number of unique ids submitted to the retry.
     * @param[out] retryFailedIds Unique retry-failed object ids.
     * @return True when the summary is self-consistent.
     */
    bool BuildValidRetryFailedIds(const std::unordered_set<std::string> &requestedIds,
                                  const MetaDataRecoveryManager::RecoverySummary &retrySummary, size_t expectedCount,
                                  std::unordered_set<std::string> &retryFailedIds) const;

    /**
     * @brief Clear the subset of matched objects that masters require to be removed locally.
     * @param[in] matchObjIds Matched local object ids from selection or retry ids.
     * @param[out] retryIds Failed object ids for later clear retries.
     */
    void ClearMatchedObjects(const std::vector<std::string> &matchObjIds, ClearDataRetryIds &retryIds);

    /**
     * @brief Keep only objects that still have local ref state and need ref rebuild.
     * @param[in] objectKeys Candidate local object ids.
     * @param[out] rebuildObjectKeys Object ids that still need ref rebuild.
     */
    void FilterObjectsNeedRebuildRefByLocalRef(const std::vector<std::string> &objectKeys,
                                               std::vector<std::string> &rebuildObjectKeys) const;

    /**
     * @brief Rebuild master refs and master app refs for matched local objects.
     * @param[in] matchObjIds Matched local object ids from selection or retry ids.
     * @param[out] retryIds Failed object ids for later ref-rebuild retries.
     */
    void RebuildRefForMatchedObjects(const std::vector<std::string> &matchObjIds, ClearDataRetryIds &retryIds);

    /**
     * @brief Retry master-ref increase for objects that failed in previous rounds.
     * @param[in] objectKeys Object ids whose master-ref increase needs retrying.
     * @param[out] retryIds Failed object ids for the next retry round.
     */
    void RetryIncreaseMasterRef(const std::vector<std::string> &objectKeys, ClearDataRetryIds &retryIds);

    /**
     * @brief Retry master app-ref recovery for objects that failed in previous rounds.
     * @param[in] objectKeys Object ids whose master app-ref recovery needs retrying.
     * @param[out] retryIds Failed object ids for the next retry round.
     */
    void RetryRecoverMasterAppRef(const std::vector<std::string> &objectKeys, ClearDataRetryIds &retryIds);

    /**
     * @brief Recover metadata, re-prove failures with the master, and clear only version-matched local data.
     * @param[in] needClearObjIds Object ids that should be removed locally.
     * @param[out] retryIds Exact unresolved object ids for the existing retry workflow.
     */
    void ClearNeedClearObjects(const std::vector<std::string> &needClearObjIds, ClearDataRetryIds &retryIds);

    /**
     * @brief Clear local objects through the existing reserve-lock-delete path and collect aggregate outcomes.
     * @param[in] objectKeys Object ids that should be removed from the local worker.
     * @param[in] logObjectKeys Whether legacy per-object failure logs should be emitted.
     * @return Aggregate successful-clear and unresolved counts.
     */
    ClearObjectSummary ClearObjectsWithSummary(const std::vector<std::string> &objectKeys, bool logObjectKeys);

    ClearObjectSummary ClearObjectsWithSummary(const std::vector<std::string> &objectKeys, bool logObjectKeys,
                                               const std::unordered_map<std::string, uint64_t> &expectedVersions);

    ClearObjectSummary ClearObjectsWithSummaryImpl(const std::vector<std::string> &objectKeys, bool logObjectKeys,
                                                   const std::unordered_map<std::string, uint64_t> *expectedVersions);

    void FinalizeRetryMetadataRecoveryResult(const std::vector<std::string> &uniqueFailedObjectKeys,
                                             const std::unordered_set<std::string> &authorityCheckFailedIds,
                                             const ClearObjectSummary &clearSummary,
                                             RetryMetadataRecoveryResult &result) const;

    std::shared_ptr<ObjectTable> objectTable_{ nullptr };
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable_{ nullptr };
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager_{ nullptr };
    std::shared_ptr<WorkerOcServiceGlobalReferenceImpl> gRefProc_{ nullptr };
    std::shared_ptr<WorkerOcServiceDeleteImpl> deleteProc_{ nullptr };
    MetaDataRecoveryManager *metadataRecoveryManager_{ nullptr };
    const worker::MetadataRouteResolver &metadataRoute_;
    const ObjectEndpointPolicy &endpointPolicy_;
    std::string localAddress_;
    std::shared_ptr<ThreadPool> clearDataThreadPool_{ nullptr };
    std::shared_ptr<std::atomic_bool> exitFlag_{ nullptr };
};
}  // namespace object_cache
}  // namespace datasystem

#endif
