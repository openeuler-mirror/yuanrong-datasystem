/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines the worker service processing publish process.
 */

#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_MIGRATE_IMPL_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_MIGRATE_IMPL_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <google/protobuf/repeated_field.h>
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/limiter/data_limiter.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

namespace datasystem {
namespace object_cache {

using ObjInfoPbList = google::protobuf::RepeatedPtrField<MigrateDataReqPb::ObjectInfoPb>;
using ObjectInfoMap = std::unordered_map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>>;
using QueryMetaMap = std::unordered_map<std::string, master::QueryMetaInfoPb>;
using LockedEntryMap = std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, uint64_t>>;
using RedirectMap =
    std::unordered_map<std::string, google::protobuf::RepeatedPtrField<master::ReplacePrimaryReqPb::ObjectInfoPb>>;

class WorkerOcServiceMigrateImpl : public WorkerOcServiceCrudCommonApi,
                                   std::enable_shared_from_this<WorkerOcServiceMigrateImpl> {
    friend class WorkerOCServiceImpl;

public:
    /**
     * @brief Construct WorkerOcServicePublishImpl.
     * @param[in] initParam The parameter used to init WorkerOcServiceCrudCommonApi.
     * @param[in] memcpyThreadPool Memory copy thread pool.
     * @param[in] localAddr Local worker address.
     * @param[in] rateController Shared migration rate controller.
     */
    WorkerOcServiceMigrateImpl(WorkerOcServiceCrudParam &initParam, std::shared_ptr<ThreadPool> memcpyThreadPool,
                               const std::string &localAddr,
                               std::shared_ptr<MigrateDataRateController> rateController);

    /**
     * @brief Destroy the migration request processor.
     */
    ~WorkerOcServiceMigrateImpl() = default;

    /**
     * @brief Migrate data.
     * @param[in] req Migrate data request.
     * @param[out] rsp Migrate data response.
     * @param[in] payloads Object data.
     * @return K_OK on success, the error otherwise.
     */
    Status MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp, std::vector<RpcMessage> payloads);

    /**
     * @brief Close incoming migration admission and wait for admitted requests until deadline.
     * @param[in] deadline Absolute monotonic drain deadline.
     * @return K_OK after the drain; K_RPC_DEADLINE_EXCEEDED if admitted requests remain.
     */
    Status CloseIncomingMigrationAdmissionAndWait(std::chrono::steady_clock::time_point deadline);
    void ReopenIncomingMigrationAdmissionAfterRejoin(const std::atomic<bool> &scaleInDataDrainStarted,
                                                   const std::atomic<bool> &shutdownRequested);
    /** Pause new migrations and return K_TRY_AGAIN instead of waiting while admitted requests drain. */
    Status PauseIncomingMigrationAdmissionAndCheckDrained();
    void ResumeIncomingMigrationAdmission();

    /**
     * @brief Whether the admission gate has been closed by CloseIncomingMigrationAdmissionAndWait.
     * @return true once the drain starts; admitted requests use this to short-circuit.
     */
    bool IsIncomingMigrationAdmissionClosed() const
    {
        return incomingMigrationAdmissionClosed_.load(std::memory_order_acquire);
    }

    /**
     * @brief Whether the admission drain timed out with in-flight requests still running.
     * @return true after CloseIncomingMigrationAdmissionAndWait exceeded its deadline.
     */
    bool IsIncomingMigrationDrainTimedOut() const
    {
        return incomingMigrationDrainTimedOut_.load(std::memory_order_acquire);
    }

    /**
     * @brief Query metadata from master.
     * @param[in] objectKeys Need query object key list.
     * @param[out] queryMetas Query meta list.
     * @param[out] failedIds Failed object key list.
     * @return K_OK on success, K_RUNTIME_ERROR on error.
     */
    Status QueryMasterMetadata(const std::unordered_set<std::string> &objectKeys, QueryMetaMap &queryMetas,
                               std::unordered_set<std::string> &failedIds);

    void QueryMasterMetadataForGroup(
        const HostPort &masterAddr, const std::vector<std::string> &ids, QueryMetaMap &queryMetas,
        std::unordered_map<std::string, std::unordered_set<std::string>> &redirectIds,
        std::unordered_set<std::string> &tmpFailedIds, Status &lastRc);

#ifdef WITH_TESTS
public:
#else
private:
#endif
    struct PrimarySwitchOutcome {
        std::unordered_set<std::string> confirmedIds;
        std::unordered_set<std::string> expiredIds;
        std::unordered_set<std::string> failedIds;
    };

    Status PrepareMigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                              std::unordered_map<std::string, std::shared_ptr<ShmUnit>> &units);

    /**
     * @brief Batch lock objects.
     * @param[in] infoList Object info list.
     * @param[out] lockedEntries Locked object list.
     * @param[out] successIds Success object key list.
     * @param[out] failedIds Failed object key list.
     * @param[out] needModifyPrimary Need modify primary copy list.
     */
    template <typename T>
    void BatchLockForMigrateData(const T &infoList, LockedEntryMap &lockedEntries,
                                 std::unordered_set<std::string> &successIds,
                                 std::unordered_set<std::string> &failedIds,
                                 LockedEntryMap &needModifyPrimary)
    {
        lockedEntries.clear();
        std::map<std::string, uint64_t> toLockIds;
        std::transform(infoList.begin(), infoList.end(), std::inserter(toLockIds, toLockIds.end()),
                       [](const auto &info) { return std::make_pair(info.object_key(), info.version()); });

        BatchLock(toLockIds, lockedEntries, successIds, failedIds, needModifyPrimary);
    }

    /**
     * @brief Batch lock objects.
     * @param[in] toLockIds Object key and version map.
     * @param[out] lockedEntries Locked object list.
     * @param[out] successIds Success object key list.
     * @param[out] failedIds Failed object key list.
     * @param[out] needModifyPrimary Need modify primary copy list.
     */
    void BatchLock(const std::map<std::string, uint64_t> &toLockIds, LockedEntryMap &lockedEntries,
                   std::unordered_set<std::string> &successIds, std::unordered_set<std::string> &failedIds,
                   LockedEntryMap &needModifyPrimary);
    void HandleExistingLockedEntry(const std::string &objectKey, uint64_t version,
                                   std::shared_ptr<SafeObjType> entry, LockedEntryMap &lockedEntries,
                                   std::unordered_set<std::string> &successIds,
                                   std::unordered_set<std::string> &failedIds, LockedEntryMap &needModifyPrimary);

    /**
     * @brief Batch unlock objects.
     * @param[in] lockedEntries Locked object list.
     */
    void BatchUnlock(const LockedEntryMap &lockedEntries)
    {
        for (auto &entry : lockedEntries) {
            entry.second.first->WUnlock();
        }
    }

    /**
     * @brief Fill objects in lock state.
     * @param[in] req Migrate data request.
     * @param[in] lockedEntries Locked object list.
     * @param[in] metas Query meta list.
     * @param[in] payloads Object datas.
     * @param[out] successIds Success object key list.
     * @param[out] failedIds Failed object key list.
     * @param[out] needSendMasterIds Need send master object keys.
     * @return K_OK on success, the error otherwise.
     */
    Status FillObjectsLocked(const MigrateDataReqPb &req, LockedEntryMap &lockedEntries, const QueryMetaMap &metas,
                             std::vector<RpcMessage> &payloads, std::unordered_set<std::string> &failedIds,
                             ObjectInfoMap &needSendMasterIds,
                             const std::unordered_map<std::string, std::shared_ptr<ShmUnit>> &units,
                             std::unordered_set<std::string> &skippedIds);

    /**
     * @brief Fill one object data in lock state.
     * @param[in] entry Object entry.
     * @param[in] info Object info.
     * @param[in] meta Metadata query from master.
     * @param[in] payloads Data.
     * @param[in] type Migrate type.
     * @param[out] needSendMasterIds Need send master object keys.
     * @return K_OK on success, the error otherwise.
     */
    Status FillOneObjectLocked(std::shared_ptr<SafeObjType> &entry, const MigrateDataReqPb::ObjectInfoPb &info,
                               const master::QueryMetaInfoPb &meta, std::vector<RpcMessage> &payloads,
                               const MigrateType &type, ObjectInfoMap &needSendMasterIds,
                               std::shared_ptr<ShmUnit> &unit);

    /**
     * @brief Notify master to replace object primary copy.
     * @param[in] originAddr Original primary worker address.
     * @param[in] needSendMasterIds Need replace primary copy objects.
     * @param[in] type Migrate type.
     * @param[out] outcome Per-object primary switch outcome.
     * @return K_OK on success, the error otherwise.
     */
    Status ReplacePrimaryImpl(const std::string &originAddr, const ObjectInfoMap &needSendMasterIds,
                              const MigrateType &type, PrimarySwitchOutcome &outcome);
    Status ReplacePrimaryForMaster(const std::string &originAddr, const HostPort &masterAddr,
                                   const std::vector<std::string> &ids, const ObjectInfoMap &needSendMasterIds,
                                   const MigrateType &type, PrimarySwitchOutcome &outcome,
                                   RedirectMap &needRedirectIds);

    /**
     * @brief Commit confirmed primary switches locally and rollback uncommitted staged objects.
     * @param[in] needSendMasterIds All objects sent to master for primary replacement.
     * @param[in] stagedObjectInfos Objects whose data was staged by this migrate request.
     * @param[in] outcome Per-object primary switch outcome.
     * @param[in,out] successIds Success object key list returned to the source worker.
     * @param[in,out] failedIds Failed object key list returned to the source worker.
     */
    void FinalizePrimarySwitch(const ObjectInfoMap &needSendMasterIds, const ObjectInfoMap &stagedObjectInfos,
                               const PrimarySwitchOutcome &outcome, std::unordered_set<std::string> &successIds,
                               std::unordered_set<std::string> &failedIds);

    /**
     * @brief Apply source heat only for primary switches confirmed by master.
     */
    void ApplyConfirmedMigratedHeats(const MigrateDataReqPb &req, const ObjectInfoMap &needSendMasterIds,
                                     const ObjectInfoMap &stagedObjectInfos, const PrimarySwitchOutcome &outcome);

    /**
     * @brief Calculate remain bytes according to migrate type.
     * @param[in] type Migrate type.
     * @return The remain bytes.
     */
    uint64_t CalcRemainBytes(const MigrateType &type);

    /**
     * @brief Fill migrate data response.
     * @param[in] req Migrate data request.
     * @param[in] successIds Success object key list.
     * @param[in] expiredIds Expired source object key list.
     * @param[in] failedIds Failed object key list.
     * @param[in] oom Indicate is OOM or not.
     * @param[out] rsp Migrate data response.
     */
    void FillMigrateDataResponse(const MigrateDataReqPb &req, const std::unordered_set<std::string> &successIds,
                                 const std::unordered_set<std::string> &expiredIds,
                                 const std::unordered_set<std::string> &failedIds, bool oom, MigrateDataRspPb &rsp,
                                 const std::unordered_set<std::string> &skipIds = {});

    /**
     * @brief For test mock purpose.
     * @param[in] api Worker master api.
     * @param[in] req Pure query meta request.
     * @param[out] rsp Pure query meta response.
     * @return K_OK on success, the error otherwise.
     */
    Status PureQueryMetaOnce(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::PureQueryMetaReqPb &req,
                             master::PureQueryMetaRspPb &rsp);

    /**
     * @brief Pure query meta with retry.
     * @param[in] api Worker master api.
     * @param[in] req Pure query meta request.
     * @param[out] rsp Pure query meta response.
     * @return K_OK on success, the error otherwise.
     */
    Status PureQueryMetaRetry(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::PureQueryMetaReqPb &req,
                              master::PureQueryMetaRspPb &rsp);

    /**
     * @brief Pure query meta to redirect master.
     * @param[in] redirectIds Need redirect object keys.
     * @param[out] queryMetas Query metas.
     * @param[out] failedIds Failed object keys.
     * @return K_OK on success, the error otherwise.
     */
    Status PureQueryMetaToRedirectMaster(
        const std::unordered_map<std::string, std::unordered_set<std::string>> &redirectIds, QueryMetaMap &queryMetas,
        std::unordered_set<std::string> &failedIds);

    /**
     * @brief Save data to object entry in lock state.
     * @param[in] entry Object entry.
     * @param[in] info Object info.
     * @param[in] payloads Data.
     * @param[in] type Migrate type.
     * @return K_OK on success, the error otherwise.
     */
    Status SaveDataWithObjectLocked(std::shared_ptr<SafeObjType> &entry, const MigrateDataReqPb::ObjectInfoPb &info,
                                    std::vector<RpcMessage> &payloads, const MigrateType &type,
                                    std::shared_ptr<ShmUnit> unit);

    /**
     * @brief Allocate memory and assign data to object.
     * @param[in] objectKey Object key.
     * @param[in] entry Object entry.
     * @param[in] payloads Data payload.
     * @param[in] size Data size.
     * @return K_OK on success, the error otherwise.
     */
    Status AllocateAndAssignData(const std::string &objectKey, std::shared_ptr<SafeObjType> &entry,
                                 const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads, uint64_t size,
                                 std::shared_ptr<ShmUnit> unit, bool retryOnOOM = false);

    Status BatchAllocateObjectGroupBySlot(const MigrateDataReqPb &req,
                                          std::unordered_map<std::string, std::shared_ptr<ShmUnit>> &units);

    /**
     * @brief For test mock purpose.
     * @param[in] api Worker master api.
     * @param[in] req Replace primary request.
     * @param[out] rsp Replace primary response.
     * @return K_OK on success, the error otherwise.
     */
    Status ReplacePrimaryOnce(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::ReplacePrimaryReqPb &req,
                              master::ReplacePrimaryRspPb &rsp);

    /**
     * @brief Replace objects primary copy with retry.
     * @param[in] api Worker master api.
     * @param[in] req Replace primary request.
     * @param[out] rsp Replace primary response.
     * @return K_OK on success, the error otherwise.
     */
    Status ReplacePrimaryRetry(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::ReplacePrimaryReqPb &req,
                               master::ReplacePrimaryRspPb &rsp);

    /**
     * @brief Process via replace primary response.
     * @param[in] rsp Replace primary response.
     * @param[in] needSendMasterIds Need replace primary copy objects.
     * @param[in,out] outcome Per-object primary switch outcome.
     * @param[out] needRedirectIds Need redirect objects.
     */
    void ProcessReplacePrimaryRsp(master::ReplacePrimaryRspPb &rsp, const ObjectInfoMap &needSendMasterIds,
                                  PrimarySwitchOutcome &outcome, RedirectMap &needRedirectIds);

    /**
     * @brief Replace objects primary copy to redirect master.
     * @param[in] originAddr Original primary worker address.
     * @param[in] needRedirectIds Need redirect objects.
     * @param[in] needSendMasterIds Need replace primary copy objects.
     * @param[in] type Migrate type.
     * @param[in,out] outcome Per-object primary switch outcome.
     * @return K_OK on success, the error otherwise.
     */
    Status ReplacePrimaryToRedirectMaster(const std::string &originAddr, const RedirectMap &needRedirectIds,
                                          const ObjectInfoMap &needSendMasterIds, const MigrateType &type,
                                          PrimarySwitchOutcome &outcome);

    /**
     * @brief Rollback the objects.
     * @param[in] objectKeys Need rollback object keys.
     * @param[in] objectInfos Object infos.
     */
    template <typename Container>
    void RollbackObjects(const Container &objectKeys, const ObjectInfoMap &objectInfos);

    /**
     * @brief Indicate the object is equal to the provided version or not.
     * @param[in] entry Object entry.
     * @param[in] version Version to compare.
     * @return True if object is newer than the provided version
     */
    bool IsEqualVersion(const std::shared_ptr<SafeObjType> &entry, uint64_t version);

    /**
     * @brief Indicate the object is newer than the provided version or not.
     * @param[in] entry Object entry.
     * @param[in] version Version to compare.
     * @return True if object is newer than the provided version
     */
    bool IsNewerVersion(const std::shared_ptr<SafeObjType> &entry, uint64_t version);

    /**
     * @brief Indicate the obejct is new created object or not.
     * @param[in] entry Object entry.
     * @return True if obejct is new created.
     */
    bool IsNewCreatedObject(std::shared_ptr<SafeObjType> &entry) const;

    /**
     * @brief Indicate if memory is available.
     * @param[in] size Memory size.
     * @param[in] type Migrate type.
     * @return True if memory is available.
     */
    bool IsMemoryAvailable(uint64_t size = 0, MigrateType type = MigrateType::SCALE_DOWN) const;

    /**
     * @brief Indicate if resource space is available.
     * @param[in] type Migrate type.
     * @param[in] size The resource size.
     * @return True if resource space is available.
     */
    bool IsResourceAvailable(const MigrateType &type, uint64_t size) const;

    /**
     * @brief Indicate if spill disk is available.
     * @param[in] size Spill size.
     * @return True if spill disk is available.
     */
    bool IsSpillAvaialble(uint64_t size = 0) const;

    /**
     * @brief Indicate the status is OOM or No space.
     * @return True if status is OOM or No space.
     */
    bool IsNoSpace(const Status &status) const;

    /**
     * @brief Check resource before migrate data.
     * @param[in] req Migrate data request.
     * @param[out] rsp Migrate data response.
     * @return K_OK on success, the error otherwise.
     */
    Status CheckResource(const MigrateDataReqPb &req, MigrateDataRspPb &rsp);

    /**
     * @brief Reject an incoming migration after this Worker closes its local drain gate.
     * @param[in] req Migrate data request.
     * @param[out] rsp Migrate data response carrying retryable target state.
     * @return K_OK while accepting migrations; K_NOT_READY after local ScaleIn drain starts.
     */
    Status CheckMigrateDataAdmission(const MigrateDataReqPb &req, MigrateDataRspPb &rsp);

    Status ValidateRebalancePolicyFence(bool enabled, uint32_t targetPolicy, uint64_t targetEpoch) const;

    /**
     * @brief Acquire one incoming migration admission slot.
     * @return K_OK while the gate is open; K_NOT_READY after local ScaleIn drain starts.
     */
    Status AcquireIncomingMigrationAdmission();

    /**
     * @brief Release one incoming migration admission slot.
     */
    void ReleaseIncomingMigrationAdmission();

    /**
     * @brief Perform one admitted socket migration request.
     * @param[in] req Migrate data request.
     * @param[out] rsp Migrate data response.
     * @param[in] payloads Object data.
     * @return K_OK on success, the error otherwise.
     */
    Status MigrateDataImpl(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                           std::vector<RpcMessage> payloads);
    ObjectInfoMap BuildPrimarySwitchInputs(const ObjectInfoMap &stagedObjectInfos,
                                           const LockedEntryMap &needModifyPrimary, const QueryMetaMap &metas,
                                           const std::unordered_set<std::string> &needQueryIds,
                                           const std::unordered_set<std::string> &failedIds,
                                           std::unordered_set<std::string> &successIds,
                                           std::unordered_set<std::string> &skippedIds) const;

    /**
     * @brief Get migrate data objects.
     * @param[in] req Migrate data request.
     * @return Object list.
     */
    template <typename Req>
    std::vector<std::string> GetObjects(const Req &req) const
    {
        std::vector<std::string> objectKeys;
        objectKeys.reserve(req.objects_size());
        const auto &infos = req.objects();
        for (const auto &info : infos) {
            objectKeys.emplace_back(info.object_key());
        }
        return objectKeys;
    }

    std::shared_ptr<ThreadPool> memcpyThreadPool_{ nullptr };

    std::string localAddr_;

    std::shared_ptr<MigrateDataRateController> rateController_;

    // Protects incomingMigrationAdmissionPaused_ and incomingMigrationCount_. Closed remains atomic for lock-free
    // processing checks after admission.
    std::mutex incomingMigrationMutex_;
    std::condition_variable incomingMigrationCv_;
    std::atomic<bool> incomingMigrationAdmissionClosed_{ false };
    bool incomingMigrationAdmissionPaused_{ false };
    std::atomic<bool> incomingMigrationDrainTimedOut_{ false };
    uint64_t incomingMigrationCount_{ 0 };

    std::shared_timed_mutex unitMutex_;
    std::unordered_map<std::string, std::shared_ptr<ShmUnit>> failedSlotUnits_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif
