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

#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <google/protobuf/repeated_field.h>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/master_object.service.rpc.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"

DS_DECLARE_uint32(data_migrate_rate_limit_mb);

using datasystem::worker::WorkerMasterOCApi;

constexpr double MIGRATE_SCALE_DOWN_HIGH_WATER_FACTOR = 0.95;

namespace datasystem {
namespace object_cache {

namespace {
template <typename ObjectList>
std::unordered_set<std::string> CollectRequestObjectKeys(const ObjectList &objects)
{
    std::unordered_set<std::string> objectKeys;
    std::transform(objects.begin(), objects.end(), std::inserter(objectKeys, objectKeys.end()),
                   [](const auto &info) { return info.object_key(); });
    return objectKeys;
}

std::unordered_set<std::string> CollectObjectInfoKeys(const ObjectInfoMap &objectInfos)
{
    std::unordered_set<std::string> objectKeys;
    objectKeys.reserve(objectInfos.size());
    std::transform(objectInfos.begin(), objectInfos.end(), std::inserter(objectKeys, objectKeys.end()),
                   [](const auto &item) { return item.first; });
    return objectKeys;
}

void AddReplacePrimaryObjectInfos(master::ReplacePrimaryReqPb &req, const std::vector<std::string> &ids,
                                  const ObjectInfoMap &objectInfos)
{
    for (const auto &id : ids) {
        auto info = req.add_object_infos();
        info->set_object_key(id);
        auto iter = objectInfos.find(id);
        if (iter != objectInfos.end()) {
            info->set_version((*iter->second.first)->GetCreateTime());
        }
    }
}
}  // namespace

WorkerOcServiceMigrateImpl::WorkerOcServiceMigrateImpl(WorkerOcServiceCrudParam &initParam,
                                                       std::shared_ptr<ThreadPool> memcpyThreadPool,
                                                       std::shared_ptr<AkSkManager> akSkManager,
                                                       const std::string &localAddr,
                                                       std::shared_ptr<MigrateDataRateController> rateController)
    : WorkerOcServiceCrudCommonApi(initParam),
      memcpyThreadPool_(std::move(memcpyThreadPool)),
      akSkManager_(std::move(akSkManager)),
      localAddr_(localAddr),
      rateController_(std::move(rateController)),
      ubAdmission_(initParam.ubAdmission)
{
}

Status WorkerOcServiceMigrateImpl::PrepareMigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                                                      std::unordered_map<std::string, std::shared_ptr<ShmUnit>> &units)
{
    if (req.is_slot_migration()) {
        auto allocRc = BatchAllocateObjectGroupBySlot(req, units);
        if (IsNoSpace(allocRc)) {
            auto failedIds = CollectRequestObjectKeys(req.objects());
            FillMigrateDataResponse(req, {}, {}, failedIds, true, rsp);
            return Status(StatusCode::K_NO_SPACE, "Slot migration allocate memory failed");
        }
        RETURN_IF_NOT_OK(allocRc);
    }
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::CheckResource(const MigrateDataReqPb &req, MigrateDataRspPb &rsp)
{
    bool oom = false;
    switch (req.type()) {
        case MigrateType::SCALE_DOWN:
            oom = !IsMemoryAvailable(0, req.type()) && !IsSpillAvaialble() && !IsDiskAvailable();
            break;
        case MigrateType::SPILL:
        case MigrateType::REBALANCE_KEEP_LOCAL:
            // A rebalance target can already be at the high water mark while still containing evictable objects.
            // Defer the exact-size decision to SaveDataWithObjectLocked, which uses the regular
            // allocate-evict-retry path. Rejecting here prevents heat eviction from recycling cold migrated data.
            oom = false;
            break;
        default:
            RETURN_STATUS(StatusCode::K_INVALID, "Invalid migrate type");
    }
    RETURN_OK_IF_TRUE(!oom);

    std::unordered_set<std::string> failedIds;
    std::transform(req.objects().begin(), req.objects().end(), std::inserter(failedIds, failedIds.end()),
                   [](const auto &info) { return info.object_key(); });
    FillMigrateDataResponse(req, {}, {}, failedIds, true, rsp);
    LOG(INFO) << "[Migrate Data] OOM";
    RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY, "OOM");
}

Status WorkerOcServiceMigrateImpl::CheckMigrateDataAdmission(const MigrateDataReqPb &req, MigrateDataRspPb &rsp)
{
    auto fenceRc = ValidateRebalancePolicyFence(req.has_rebalance_policy_fence(), req.target_eviction_policy(),
                                                req.target_eviction_policy_epoch());
    if (fenceRc.IsError()) {
        std::unordered_set<std::string> failedIds;
        std::transform(req.objects().begin(), req.objects().end(), std::inserter(failedIds, failedIds.end()),
                       [](const auto &info) { return info.object_key(); });
        FillMigrateDataResponse(req, {}, {}, failedIds, false, rsp);
        return fenceRc;
    }
    auto rc = AcquireIncomingMigrationAdmission();
    if (rc.IsOk()) {
        return Status::OK();
    }
    std::unordered_set<std::string> failedIds;
    std::transform(req.objects().begin(), req.objects().end(), std::inserter(failedIds, failedIds.end()),
                   [](const auto &info) { return info.object_key(); });
    FillMigrateDataResponse(req, {}, {}, failedIds, false, rsp);
    rsp.set_scale_down_state(MigrateDataRspPb::DATA_MIGRATION_STARTED);
    return rc;
}

Status WorkerOcServiceMigrateImpl::ValidateRebalancePolicyFence(bool enabled, uint32_t targetPolicy,
                                                                uint64_t targetEpoch) const
{
    RETURN_OK_IF_TRUE(!enabled);
    CHECK_FAIL_RETURN_STATUS(evictionManager_ != nullptr, StatusCode::K_NOT_READY,
                             "Target eviction manager is not initialized");
    return evictionManager_->ValidateRebalancePolicy(targetPolicy, targetEpoch);
}

Status WorkerOcServiceMigrateImpl::AcquireIncomingMigrationAdmission(bool requireUbAdmission)
{
    HostPort local;
    RETURN_IF_NOT_OK(local.ParseString(localAddr_));
    if (endpointPolicy_ != nullptr) {
        RETURN_IF_NOT_OK(endpointPolicy_->CheckDataPlaneAdmission(local, DataPlaneAdmissionRole::INCOMING_TARGET));
    }
    if (requireUbAdmission && ubAdmission_ != nullptr) {
        RETURN_IF_NOT_OK(ubAdmission_->CheckWriteTarget(local, UbOperationKind::MIGRATION_READ));
    }
    std::lock_guard<std::mutex> lock(incomingMigrationMutex_);
    const bool localExiting = exitRequested_ != nullptr && exitRequested_->load(std::memory_order_relaxed);
    CHECK_FAIL_RETURN_STATUS(!incomingMigrationAdmissionClosed_ && !incomingMigrationAdmissionPaused_ && !localExiting,
                             StatusCode::K_NOT_READY,
                             "Target Worker is exiting or migration admission is paused");
    ++incomingMigrationCount_;
    return Status::OK();
}

void WorkerOcServiceMigrateImpl::ReleaseIncomingMigrationAdmission()
{
    std::lock_guard<std::mutex> lock(incomingMigrationMutex_);
    if (incomingMigrationCount_ == 0) {
        LOG(ERROR) << "Incoming migration admission counter is already zero.";
        return;
    }
    --incomingMigrationCount_;
    if (incomingMigrationCount_ == 0) {
        incomingMigrationCv_.notify_all();
    }
}

Status WorkerOcServiceMigrateImpl::CloseIncomingMigrationAdmissionAndWait(
    std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(incomingMigrationMutex_);
    incomingMigrationAdmissionClosed_.store(true, std::memory_order_release);
    INJECT_POINT_NO_RETURN("WorkerOcServiceMigrateImpl.CloseIncomingMigrationAdmissionAndWait.closed");
    if (incomingMigrationCount_ > 0) {
        LOG(INFO) << "[Graceful exit] Waiting for " << incomingMigrationCount_
                  << " admitted incoming migration request(s) to finish.";
    }
    const bool drained =
        incomingMigrationCv_.wait_until(lock, deadline, [this] { return incomingMigrationCount_ == 0; });
    if (!drained) {
        incomingMigrationDrainTimedOut_.store(true, std::memory_order_release);
        INJECT_POINT_NO_RETURN("WorkerOcServiceMigrateImpl.CloseIncomingMigrationAdmissionAndWait.timedOut");
        LOG(ERROR) << "[Graceful exit] Timed out with " << incomingMigrationCount_
                   << " incoming migration request(s) still running; admission remains closed.";
        RETURN_STATUS(StatusCode::K_RPC_DEADLINE_EXCEEDED,
                      "Timed out waiting for admitted incoming migrations to finish");
    }
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::PauseIncomingMigrationAdmissionAndCheckDrained()
{
    std::lock_guard<std::mutex> lock(incomingMigrationMutex_);
    CHECK_FAIL_RETURN_STATUS(!incomingMigrationAdmissionClosed_, StatusCode::K_NOT_READY,
                             "Incoming migration admission is permanently closed");
    incomingMigrationAdmissionPaused_ = true;
    INJECT_POINT_NO_RETURN("WorkerOcServiceMigrateImpl.PauseIncomingMigrationAdmissionAndCheckDrained.afterPaused");
    RETURN_OK_IF_TRUE(incomingMigrationCount_ == 0);
    RETURN_STATUS(K_TRY_AGAIN, "Incoming migrations are still draining");
}

void WorkerOcServiceMigrateImpl::ResumeIncomingMigrationAdmission()
{
    std::lock_guard<std::mutex> lock(incomingMigrationMutex_);
    if (!incomingMigrationAdmissionClosed_.load(std::memory_order_acquire)) {
        incomingMigrationAdmissionPaused_ = false;
    }
}

Status WorkerOcServiceMigrateImpl::MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                                               std::vector<RpcMessage> payloads)
{
    LOG(INFO) << FormatString("[Migrate Data] Type: %d, Count: %d, Objects: %s, is_slot_migration: %d, slot_id: %u",
                              static_cast<int>(req.type()), req.objects_size(), VectorToString(GetObjects(req)),
                              req.is_slot_migration(), req.slot_id());
    INJECT_POINT("worker.migrate_service.return");
    RETURN_IF_NOT_OK(CheckMigrateDataAdmission(req, rsp));
    Raii admission([this] { ReleaseIncomingMigrationAdmission(); });
    INJECT_POINT_NO_RETURN("WorkerOcServiceMigrateImpl.MigrateData.afterAdmission");
    if (IsIncomingMigrationAdmissionClosed()) {
        std::unordered_set<std::string> failedIds;
        std::transform(req.objects().begin(), req.objects().end(), std::inserter(failedIds, failedIds.end()),
                       [](const auto &info) { return info.object_key(); });
        FillMigrateDataResponse(req, {}, {}, failedIds, false, rsp);
        rsp.set_scale_down_state(MigrateDataRspPb::DATA_MIGRATION_STARTED);
        return Status(StatusCode::K_NOT_READY, "admission closed before data processing");
    }
    auto rc = MigrateDataImpl(req, rsp, std::move(payloads));
    if (rc.IsOk() && IsIncomingMigrationDrainTimedOut()) {
        // Data may already be written to target; intentionally return failure so Source
        // keeps its local copy, accepting a transient double-write rather than data loss.
        LOG(WARNING) << "[Migrate Data] Drain timed out during processing; "
                     << "returning K_NOT_READY so Source keeps its local copy "
                     << "(target may already have the data; transient double-write is expected)";
        return Status(StatusCode::K_NOT_READY, "admission drain timed out during processing");
    }
    return rc;
}

Status WorkerOcServiceMigrateImpl::MigrateDataImpl(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                                                   std::vector<RpcMessage> payloads)
{
    RETURN_IF_NOT_OK(CheckResource(req, rsp));
    std::unordered_map<std::string, std::shared_ptr<ShmUnit>> units;
    RETURN_IF_NOT_OK(PrepareMigrateData(req, rsp, units));

    // Query master metadata before taking target object locks. Master RPC retries and redirects can take seconds;
    // holding a whole migration batch WLocked across that network path stalls foreground access to hot targets.
    std::unordered_set<std::string> needQueryIds = CollectRequestObjectKeys(req.objects());
    QueryMetaMap metas;
    std::unordered_set<std::string> failedIds;
    Status rc = QueryMasterMetadata(needQueryIds, metas, failedIds);
    if (rc.IsError()) {
        FillMigrateDataResponse(req, {}, {}, failedIds, false, rsp);
        return rc;
    }

    // Lock only objects whose metadata query did not fail. FillObjectsLocked revalidates the requested version under
    // each WLock, so a concurrent target update between the query and lock is handled by the existing version rules.
    LockedEntryMap lockedEntries;
    std::unordered_set<std::string> successIds;
    std::unordered_set<std::string> expiredIds;
    std::unordered_set<std::string> skippedIds;
    LockedEntryMap needModifyPrimary;
    BatchLockForMigrateData(req.objects(), lockedEntries, successIds, failedIds, needModifyPrimary);
    Raii raii([this, &lockedEntries, &needModifyPrimary]() {
        BatchUnlock(lockedEntries);
        BatchUnlock(needModifyPrimary);
    });
    // Fill data and metadata to object entry.
    ObjectInfoMap stagedObjectInfos;
    Status status = FillObjectsLocked(req, lockedEntries, metas, payloads, failedIds, stagedObjectInfos, units,
                                      skippedIds);
    bool oom = IsNoSpace(status);

    ObjectInfoMap needSendMasterIds = BuildPrimarySwitchInputs(stagedObjectInfos, needModifyPrimary, metas,
                                                               needQueryIds, failedIds, successIds, skippedIds);
    // 4. Send replace primary copy to master.
    if (!needSendMasterIds.empty()) {
        PrimarySwitchOutcome outcome;
        status = ReplacePrimaryImpl(req.worker_addr(), needSendMasterIds, req.type(), outcome);
        FinalizePrimarySwitch(needSendMasterIds, stagedObjectInfos, outcome, successIds, failedIds);
        expiredIds = outcome.expiredIds;
        ApplyConfirmedMigratedHeats(req, needSendMasterIds, stagedObjectInfos, outcome);
    }

    // 5. Fill response.
    FillMigrateDataResponse(req, successIds, expiredIds, failedIds, oom, rsp, skippedIds);
    if ((!successIds.empty() || !expiredIds.empty()) && req.is_slot_migration() && !req.is_retry()) {
        auto merStatus = persistenceApi_->MergeSlot(req.worker_addr(), req.slot_id());
        LOG_IF_ERROR(merStatus, FormatString("Merge slot failed after migrate data, slotId: %u, status: %s",
                                             req.slot_id(), merStatus.ToString()));
    }
    LOG(INFO) << "[Migrate Data] Migrate finish, success size: " << successIds.size()
              << ", expired size: " << expiredIds.size() << ", skipped size: " << skippedIds.size()
              << ", failed size: " << failedIds.size()
              << ", last status: " << status.ToString();
    return successIds.empty() && expiredIds.empty() ? status : Status::OK();
}

ObjectInfoMap WorkerOcServiceMigrateImpl::BuildPrimarySwitchInputs(
    const ObjectInfoMap &stagedObjectInfos, const LockedEntryMap &needModifyPrimary, const QueryMetaMap &metas,
    const std::unordered_set<std::string> &needQueryIds, const std::unordered_set<std::string> &failedIds,
    std::unordered_set<std::string> &successIds, std::unordered_set<std::string> &skippedIds) const
{
    // A target-side newer copy can be classified as successful without entering lockedEntries. If authoritative
    // metadata does not contain that key, keep the source copy just like the ordinary fill path.
    for (const auto &objectKey : needQueryIds) {
        if (failedIds.count(objectKey) == 0 && metas.count(objectKey) == 0) {
            (void)skippedIds.emplace(objectKey);
            (void)successIds.erase(objectKey);
        }
    }
    ObjectInfoMap needSendMasterIds = stagedObjectInfos;
    for (const auto &[objectKey, entry] : needModifyPrimary) {
        if (metas.count(objectKey) == 0) {
            (void)skippedIds.emplace(objectKey);
            (void)successIds.erase(objectKey);
            continue;
        }
        needSendMasterIds.emplace(objectKey, std::make_pair(entry.first, false));
    }
    return needSendMasterIds;
}

Status WorkerOcServiceMigrateImpl::MigrateDataDirect(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp)
{
    PerfPoint pointAll(PerfKey::WORKER_SERVER_MIGRATE_DATA_DIRECT);
    LOG(INFO) << FormatString("[Migrate Data] Count: %d, Objects: %s", req.objects_size(),
                              VectorToString(GetObjects(req)));
    RETURN_OK_IF_TRUE(req.objects().empty());
    auto fenceRc = ValidateRebalancePolicyFence(req.has_rebalance_policy_fence(), req.target_eviction_policy(),
                                                req.target_eviction_policy_epoch());
    if (fenceRc.IsError()) {
        return PrepareMigrateDataDirectError(req, rsp, fenceRc.GetCode(), fenceRc.GetMsg());
    }
    auto admissionRc = AcquireIncomingMigrationAdmission(true);
    if (admissionRc.IsError()) {
        return PrepareMigrateDataDirectError(req, rsp, admissionRc.GetCode(), admissionRc.GetMsg());
    }
    Raii admission([this] { ReleaseIncomingMigrationAdmission(); });
    INJECT_POINT_NO_RETURN("WorkerOcServiceMigrateImpl.MigrateDataDirect.afterAdmission");
    if (IsIncomingMigrationAdmissionClosed()) {
        return PrepareMigrateDataDirectError(req, rsp, StatusCode::K_NOT_READY,
                                             "admission closed before data processing");
    }
    RETURN_IF_NOT_OK(PreCheckMigrateDataDirect(req, rsp));
    auto rc = MigrateDataDirectImpl(req, rsp);
    if (rc.IsOk() && IsIncomingMigrationDrainTimedOut()) {
        // Data may already be written to target; intentionally return failure so Source
        // keeps its local copy, accepting a transient double-write rather than data loss.
        LOG(WARNING) << "[Migrate Data Direct] Drain timed out during processing; "
                     << "returning K_NOT_READY so Source keeps its local copy "
                     << "(target may already have the data; transient double-write is expected)";
        return PrepareMigrateDataDirectError(req, rsp, StatusCode::K_NOT_READY,
                                             "admission drain timed out during processing");
    }
    return rc;
}

Status WorkerOcServiceMigrateImpl::PrepareMigrateDataDirectError(const MigrateDataDirectReqPb &req,
                                                                 MigrateDataDirectRspPb &rsp, StatusCode code,
                                                                 const std::string &message)
{
    std::unordered_set<std::string> failedIds;
    std::transform(req.objects().begin(), req.objects().end(), std::inserter(failedIds, failedIds.end()),
                   [](const auto &info) { return info.object_key(); });
    const bool noSpace = (code == StatusCode::K_OUT_OF_MEMORY || code == StatusCode::K_NO_SPACE);
    FillMigrateDataDirectResponse(req, failedIds, noSpace, 0, rsp);
    LOG(INFO) << "[Migrate Data] " << message;
    return Status(code, message);
}

Status WorkerOcServiceMigrateImpl::PreCheckMigrateDataDirect(const MigrateDataDirectReqPb &req,
                                                             MigrateDataDirectRspPb &rsp)
{
    // Direct migration supports SPILL only.
    if (req.has_type() && req.type() != MigrateType::SPILL) {
        return PrepareMigrateDataDirectError(req, rsp, StatusCode::K_INVALID,
                                             "MigrateDataDirect only supports SPILL type");
    }
    if (!IsUrmaEnabled()) {
        return PrepareMigrateDataDirectError(req, rsp, StatusCode::K_RUNTIME_ERROR, "URMA is not enabled");
    }
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::PrepareMigrateDataDirectEntries(
    const MigrateDataDirectReqPb &req, PerfPoint &point, LockedEntryMap &lockedEntries,
    std::unordered_set<std::string> &successIds, std::unordered_set<std::string> &failedIds,
    LockedEntryMap &needModifyPrimary, ObjectInfoMap &needReadDataIds, const QueryMetaMap &metas,
    std::unordered_set<std::string> &skippedIds)
{
    BatchLockForMigrateData(req.objects(), lockedEntries, successIds, failedIds, needModifyPrimary);
    point.RecordAndReset(PerfKey::WORKER_SERVER_MIGRATE_DIRECT_FILL_META);
    FillMetaToObjectEntries(lockedEntries, metas, successIds, failedIds, needReadDataIds, skippedIds);
    for (auto it = needModifyPrimary.begin(); it != needModifyPrimary.end();) {
        if (metas.find(it->first) == metas.end()) {
            (void)skippedIds.emplace(it->first);
            (void)successIds.erase(it->first);
            it = needModifyPrimary.erase(it);
        } else {
            ++it;
        }
    }
    for (const auto &object : req.objects()) {
        const auto &objectKey = object.object_key();
        if (failedIds.count(objectKey) == 0 && metas.count(objectKey) == 0) {
            (void)skippedIds.emplace(objectKey);
            (void)successIds.erase(objectKey);
        }
    }
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::HandleMigrateDataDirectNoSpace(const MigrateDataDirectReqPb &req,
                                                                  MigrateDataDirectRspPb &rsp,
                                                                  const ObjectInfoMap &needReadDataIds,
                                                                  std::unordered_set<std::string> &failedIds,
                                                                  Status status)
{
    RETURN_OK_IF_TRUE(!req.is_slot_migration() || !IsNoSpace(status));
    for (const auto &object : req.objects()) {
        failedIds.insert(object.object_key());
    }
    RollbackObjects(failedIds, needReadDataIds);
    FillMigrateDataDirectResponse(req, failedIds, true, 0, rsp);
    LOG(WARNING) << "[Migrate Data] Slot migration allocate memory failed, retry all objects.";
    return Status(StatusCode::K_NO_SPACE, "Slot migration allocate memory failed");
}

void WorkerOcServiceMigrateImpl::ReplacePrimaryForMigrateDataDirect(const MigrateDataDirectReqPb &req, PerfPoint &point,
                                                                    const LockedEntryMap &needModifyPrimary,
                                                                    const ObjectInfoMap &needReadDataIds,
                                                                    std::unordered_set<std::string> &successIds,
                                                                    std::unordered_set<std::string> &failedIds,
                                                                    ObjectInfoMap &needSendMasterIds, Status &status)
{
    point.RecordAndReset(PerfKey::WORKER_SERVER_MIGRATE_DIRECT_REPLACE_PRIMARY);
    if (!failedIds.empty()) {
        RollbackObjects(failedIds, needReadDataIds);
    }
    for (const auto &[objectKey, it] : needModifyPrimary) {
        needSendMasterIds.emplace(objectKey, std::make_pair(it.first, false));
    }
    if (!needSendMasterIds.empty()) {
        // Old sources that don't set the optional 'type' field are legacy SPILL migrations.
        // has_type()=false → default to SPILL so remove_location=true (erase source location),
        // matching the pre-REBALANCE_KEEP_LOCAL behavior.
        MigrateType type = req.has_type() ? req.type() : MigrateType::SPILL;
        PrimarySwitchOutcome outcome;
        status = ReplacePrimaryImpl(req.worker_addr(), needSendMasterIds, type, outcome);
        FinalizePrimarySwitch(needSendMasterIds, needReadDataIds, outcome, successIds, failedIds);
    }
}

Status WorkerOcServiceMigrateImpl::MigrateDataDirectImpl(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp)
{
    PerfPoint point(PerfKey::WORKER_SERVER_MIGRATE_DIRECT_QUERY_META);
    std::unordered_set<std::string> failedIds;
    QueryMetaMap metas;
    auto queryIds = CollectRequestObjectKeys(req.objects());
    Status rc = QueryMasterMetadata(queryIds, metas, failedIds);
    if (rc.IsError()) {
        FillMigrateDataDirectResponse(req, failedIds, false, 0, rsp);
        return rc;
    }

    point.RecordAndReset(PerfKey::WORKER_SERVER_MIGRATE_DIRECT_LOCK);
    LockedEntryMap lockedEntries;
    std::unordered_set<std::string> successIds;
    std::unordered_set<std::string> skippedIds;
    LockedEntryMap needModifyPrimary;
    ObjectInfoMap needReadDataIds;
    Raii raii([this, &lockedEntries, &needModifyPrimary]() {
        BatchUnlock(lockedEntries);
        BatchUnlock(needModifyPrimary);
    });
    RETURN_IF_NOT_OK(PrepareMigrateDataDirectEntries(req, point, lockedEntries, successIds, failedIds,
                                                     needModifyPrimary, needReadDataIds, metas, skippedIds));

    point.RecordAndReset(PerfKey::WORKER_SERVER_MIGRATE_DIRECT_FILL_DATA);
    DirectReadOutcome readOutcome{ req, needReadDataIds, {}, failedIds, {}, {} };
    Status status = FillDataToObjectEntries(readOutcome);
    if (readOutcome.failureDetail.has_value()) {
        rsp.mutable_provider_ub_failure_detail()->CopyFrom(*readOutcome.failureDetail);
    }
    Status noSpaceStatus = HandleMigrateDataDirectNoSpace(req, rsp, needReadDataIds, failedIds, status);
    if (noSpaceStatus.IsError()) {
        return noSpaceStatus;
    }
    bool oom = IsNoSpace(status);
    ReplacePrimaryForMigrateDataDirect(req, point, needModifyPrimary, needReadDataIds, successIds, failedIds,
                                       readOutcome.needSendMasterIds, status);
    point.RecordAndReset(PerfKey::WORKER_SERVER_MIGRATE_DIRECT_FILL_RSP);
    FillMigrateDataDirectResponse(req, failedIds, oom, readOutcome.migratedBytes, rsp, skippedIds);
    if (!successIds.empty() && req.is_slot_migration() && !req.is_retry()) {
        auto merStatus = persistenceApi_->MergeSlot(req.worker_addr(), req.slot_id());
        LOG_IF_ERROR(merStatus, FormatString("Merge slot failed after migrate data, slotId: %u, status: %s",
                                             req.slot_id(), merStatus.ToString()));
    }
    LOG(INFO) << "[Migrate Data] Migrate direct finish, success size: " << successIds.size()
              << ", failed size: " << failedIds.size() << ", last status: " << status.ToString();
    return successIds.empty() ? status : Status::OK();
}

void WorkerOcServiceMigrateImpl::BatchLock(const std::map<std::string, uint64_t> &toLockIds,
                                           LockedEntryMap &lockedEntries, std::unordered_set<std::string> &successIds,
                                           std::unordered_set<std::string> &failedIds,
                                           LockedEntryMap &needModifyPrimary)
{
    for (const auto &[objectKey, version] : toLockIds) {
        if (failedIds.count(objectKey) > 0 || successIds.count(objectKey) > 0) {
            continue;
        }
        std::shared_ptr<SafeObjType> entry;
        bool isInsert = false;

        Status s = objectTable_->ReserveGetAndLock(objectKey, entry, isInsert, false, false);
        if (!s.IsOk()) {
            LOG(ERROR) << FormatString("[Migrate Data] %s get failed, would not be process this time.", objectKey);
            (void)failedIds.emplace(objectKey);
            continue;
        }
        if (isInsert) {
            SetEmptyObjectEntry(objectKey, *entry);
            (void)lockedEntries.emplace(objectKey, std::make_pair(std::move(entry), version));
        } else {
            HandleExistingLockedEntry(objectKey, version, std::move(entry), lockedEntries, successIds, failedIds,
                                      needModifyPrimary);
        }
    }
}

void WorkerOcServiceMigrateImpl::HandleExistingLockedEntry(
    const std::string &objectKey, uint64_t version, std::shared_ptr<SafeObjType> entry, LockedEntryMap &lockedEntries,
    std::unordered_set<std::string> &successIds, std::unordered_set<std::string> &failedIds,
    LockedEntryMap &needModifyPrimary)
{
    Status rc = TryLockWithRetry(objectKey, entry, true);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[Migrate Data] %s try lock failed, would not be process this time.", objectKey);
        (void)failedIds.emplace(objectKey);
        return;
    }
    if (entry->Get() == nullptr) {
        SetEmptyObjectEntry(objectKey, *entry);
    }
    if (!IsNewerVersion(entry, version)) {
        (void)lockedEntries.emplace(objectKey, std::make_pair(std::move(entry), version));
        return;
    }
    std::stringstream ss;
    ss << FormatString(
        "[Migrate Data] %s version [%ld >= %ld] is newer, cache invalid: %s, primary copy: %s, not need to migrate "
        "data.",
        objectKey, (*entry)->GetCreateTime(), version, (*entry)->stateInfo.IsCacheInvalid() ? "true" : "false",
        (*entry)->GetAddress());
    if (IsEqualVersion(entry, version)) {
        ss << " And need modify primary copy.";
        (void)needModifyPrimary.emplace(objectKey, std::make_pair(entry, version));
        if (entry->Get()->IsWriteBackMode()) {
            ss << " Would add to l2 queue.";
            std::future<Status> future;
            LOG_IF_ERROR(asyncSendManager_->Add(objectKey, entry, future),
                         FormatString("[Migrate Data] [%s] add to async queue failed", objectKey));
        }
    } else {
        // A valid newer local copy still needs master confirmation before it is returned as a successful migration.
        (void)successIds.emplace(objectKey);
    }
    LOG(INFO) << ss.str();
}

void WorkerOcServiceMigrateImpl::QueryMasterMetadataForGroup(
    const HostPort &masterAddr, const std::vector<std::string> &ids, QueryMetaMap &queryMetas,
    std::unordered_map<std::string, std::unordered_set<std::string>> &redirectIds,
    std::unordered_set<std::string> &tmpFailedIds, Status &lastRc)
{
    auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    if (workerMasterApi == nullptr) {
        std::stringstream ss;
        ss << "[Migrate Data] hash master get failed, Replace primary copy failed: " << masterAddr.ToString();
        LOG(ERROR) << ss.str();
        lastRc = Status(StatusCode::K_RUNTIME_ERROR, ss.str());
        tmpFailedIds.insert(ids.begin(), ids.end());
        return;
    }
    master::PureQueryMetaReqPb req;
    req.set_redirect(true);
    for (const auto &id : ids) {
        req.add_object_keys(id);
    }
    master::PureQueryMetaRspPb rsp;
    Status rc = PureQueryMetaRetry(workerMasterApi, req, rsp);
    if (rc.IsError()) {
        LOG(ERROR) << "[Migrate Data] Pure query meta failed: " << rc.ToString();
        tmpFailedIds.insert(ids.begin(), ids.end());
        lastRc = rc;
        return;
    }
    for (const auto &meta : rsp.query_metas()) {
        (void)queryMetas.emplace(meta.meta().object_key(), meta);
    }
    for (const auto &info : rsp.info()) {
        auto &objectList = redirectIds[info.redirect_meta_address()];
        objectList.insert(info.change_meta_ids().begin(), info.change_meta_ids().end());
    }
}

Status WorkerOcServiceMigrateImpl::QueryMasterMetadata(const std::unordered_set<std::string> &objectKeys,
                                                       QueryMetaMap &queryMetas,
                                                       std::unordered_set<std::string> &failedIds)
{
    std::vector<std::string> objectKeyList(objectKeys.begin(), objectKeys.end());
    CHECK_FAIL_RETURN_STATUS(metadataRouteResolver_ != nullptr, K_NOT_READY, "Metadata route resolver is unavailable");
    auto grouped = metadataRouteResolver_->GroupOwners(objectKeyList);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;
    Status lastRc;
    std::unordered_map<std::string, std::unordered_set<std::string>> redirectIds;
    std::unordered_set<std::string> tmpFailedIds;
    for (auto &item : objKeysGrpByMaster) {
        QueryMasterMetadataForGroup(item.first, item.second, queryMetas, redirectIds, tmpFailedIds, lastRc);
    }

    Status rc = PureQueryMetaToRedirectMaster(redirectIds, queryMetas, tmpFailedIds);
    lastRc = rc.IsError() ? rc : lastRc;

    size_t failedSize = tmpFailedIds.size();
    failedIds.insert(tmpFailedIds.begin(), tmpFailedIds.end());
    INJECT_POINT("WorkerOcServiceMigrateImpl.QueryMasterMetadata.notFound",
                 [&queryMetas]() -> Status {
                     queryMetas.clear();
                     return Status::OK();
                 });
    return failedSize == objectKeys.size() ? lastRc : Status::OK();
}

Status WorkerOcServiceMigrateImpl::FillObjectsLocked(
    const MigrateDataReqPb &req, LockedEntryMap &lockedEntries, const QueryMetaMap &metas,
    std::vector<RpcMessage> &payloads,
    std::unordered_set<std::string> &failedIds, ObjectInfoMap &needSendMasterIds,
    const std::unordered_map<std::string, std::shared_ptr<ShmUnit>> &units,
    std::unordered_set<std::string> &skippedIds)
{
    Status lastRc;
    const auto &infoList = req.objects();
    auto iter = infoList.begin();
    for (auto it = infoList.begin(); it != infoList.end(); ++it) {
        const auto &objectKey = it->object_key();
        auto lockedIt = lockedEntries.find(objectKey);
        if (lockedIt == lockedEntries.end()) {
            LOG(INFO) << FormatString("[Migrate Data] %s lock failed, would not be process this time.", objectKey);
            continue;
        }
        const auto &metaIt = metas.find(objectKey);
        if (metaIt == metas.end()) {
            LOG(INFO) << FormatString("[Migrate Data] %s has been deleted, not need to be process.", objectKey);
            // The object is gone from master. If BatchLock just inserted a placeholder here, it has no
            // metaTable entry (so TTL never targets it) and was never added to the eviction list, so it
            // would leak. Erase the placeholder now; entries that already existed keep their own lifecycle.
            auto &entry = lockedIt->second.first;
            if (entry != nullptr && entry->Get() != nullptr && IsNewCreatedObject(entry)) {
                (void)objectTable_->Erase(objectKey, *entry);
            }
            if (failedIds.find(objectKey) == failedIds.end()) {
                (void)skippedIds.emplace(objectKey);
            }
            continue;
        }
        const auto &unitIter = units.find(objectKey);
        std::shared_ptr<ShmUnit> unit = unitIter == units.end() ? nullptr : unitIter->second;
        Status status = FillOneObjectLocked(lockedIt->second.first, *it, metaIt->second, payloads, req.type(),
                                            needSendMasterIds, unit);
        if (IsNoSpace(status)) {
            std::transform(it, infoList.end(), std::inserter(failedIds, failedIds.end()),
                           [](const MigrateDataReqPb::ObjectInfoPb &info) { return info.object_key(); });
            lastRc = status;
            iter = it;
            break;
        }
        if (status.IsError()) {
            (void)failedIds.emplace(objectKey);
            lastRc = status;
        }
    }

    if (IsNoSpace(lastRc)) {
        for (auto it = iter; it != infoList.end(); ++it) {
            const auto &objectKey = it->object_key();
            auto lockedIt = lockedEntries.find(objectKey);
            if (lockedIt == lockedEntries.end()) {
                continue;
            }
            VLOG(1) << "[Migrate Data] " << objectKey << " Set cache invalid";
            lockedIt->second.first->Get()->stateInfo.SetCacheInvalid(true);
        }
    }
    return lastRc;
}

void WorkerOcServiceMigrateImpl::FillMetaToObjectEntries(LockedEntryMap &lockedEntries, const QueryMetaMap &metas,
                                                         std::unordered_set<std::string> &successIds,
                                                         std::unordered_set<std::string> &failedIds,
                                                         ObjectInfoMap &needReadDataIds,
                                                         std::unordered_set<std::string> &skippedIds)
{
    for (auto &[objectKey, it] : lockedEntries) {
        const auto &metaIt = metas.find(objectKey);
        if (metaIt == metas.end()) {
            LOG(INFO) << FormatString("[Migrate Data] %s has been deleted, not need to be process.", objectKey);
            // Same leak as FillObjectsLocked: a BatchLock-inserted placeholder has no master meta entry and
            // is not on the eviction list, so neither TTL nor evict can reclaim it. Erase it here.
            auto &entry = it.first;
            if (entry != nullptr && entry->Get() != nullptr && IsNewCreatedObject(entry)) {
                (void)objectTable_->Erase(objectKey, *entry);
            }
            if (failedIds.find(objectKey) == failedIds.end()) {
                (void)skippedIds.emplace(objectKey);
            }
            continue;
        }
        const auto &meta = metaIt->second;
        const auto &version = it.second;
        if (meta.meta().version() != version) {
            VLOG(1) << FormatString("[ObjectKey %s] Version %ld != %ld", objectKey, version, meta.meta().version());
            // Version mismatch means the object has been update, we will no need to update.
            (void)successIds.emplace(objectKey);
            continue;
        }
        // Fill metadata to object entry
        auto &entry = it.first;
        if ((*entry)->IsSpilled()) {
            LOG_IF_ERROR(WorkerOcSpill::Instance()->Delete(objectKey),
                         FormatString("[Migrate Data] Delete elder object %s failed", objectKey));
        }
        bool isNewCreate = IsNewCreatedObject(entry);
        SetObjectEntryAccordingToMeta(meta.meta(), GetMetadataSize(), *entry);
        (*entry)->stateInfo.SetPrimaryCopy(false);
        needReadDataIds.emplace(objectKey, std::make_pair(entry, isNewCreate));
    }
}

Status WorkerOcServiceMigrateImpl::AggregateAllocateHelper(const MigrateDataDirectReqPb &req,
                                                           const ObjectInfoMap &needReadDataIds,
                                                           std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
                                                           std::vector<uint32_t> &shmIndexMapping)
{
    // Aggregate allocation uses the same allocate-evict-retry semantics as ordinary object allocation. The target can
    // be above its high water mark while still containing cold, evictable objects.
    const bool includeLargeObjects = req.is_slot_migration();
    const size_t metaSz = GetMetadataSize();
    std::function<void(std::function<void(uint64_t, uint64_t, uint32_t)>, bool &)> traversalHelper =
        [&req, &needReadDataIds, &metaSz](const std::function<void(uint64_t, uint64_t, uint32_t)> &collector,
                                          bool &needAggregate) {
            needAggregate = req.is_slot_migration() || req.objects_size() > 1;
            for (int i = 0; i < req.objects_size(); i++) {
                const auto &object = req.objects(i);
                if (needReadDataIds.find(object.object_key()) == needReadDataIds.end()) {
                    continue;
                }
                collector(object.data_size(), object.data_size() + metaSz, i);
            }
        };
    const auto &firstObjectKey = req.objects().begin()->object_key();
    return AggregateAllocate(firstObjectKey, traversalHelper, evictionManager_, shmOwners, shmIndexMapping, true,
                             includeLargeObjects);
}

Status WorkerOcServiceMigrateImpl::FillDataToObjectEntries(DirectReadOutcome &outcome)
{
    // 1. Aggregate pre-allocated memory for objects.
    PerfPoint point(PerfKey::WORKER_SERVER_MIGRATE_DIRECT_ALLOCATE_AGGREGATE);
    std::vector<uint32_t> shmIndexMapping(outcome.req.objects_size(), std::numeric_limits<uint32_t>::max());
    std::vector<std::shared_ptr<ShmOwner>> shmOwners;
    Status rc = AggregateAllocateHelper(outcome.req, outcome.needReadDataIds, shmOwners, shmIndexMapping);
    if (rc.IsError()) {
        LOG(ERROR) << "[Migrate Data] Aggregate allocate memory failed: " << rc.ToString();
        if (outcome.req.is_slot_migration() && IsNoSpace(rc)) {
            return Status(StatusCode::K_NO_SPACE, "Slot migration aggregate allocate memory failed");
        }
        shmOwners.clear();
    }

    // 2. Start remote read tasks.
    point.RecordAndReset(PerfKey::WORKER_SERVER_MIGRATE_DIRECT_START_REMOTE_READ);
    std::vector<ReadTask> tasks;
    Status startRc = StartRemoteReadTasks(outcome, shmIndexMapping, shmOwners, tasks);
    if (tasks.empty()) {
        return startRc;
    }

    // 3. Wait tasks and finalize object entries.
    point.RecordAndReset(PerfKey::WORKER_SERVER_MIGRATE_DIRECT_WAIT_REMOTE_READ);
    Status waitRc = WaitRemoteReadTasks(tasks, outcome);
    return waitRc.IsError() ? waitRc : startRc;
}

std::shared_ptr<ShmOwner> WorkerOcServiceMigrateImpl::GetShmOwnerByIndex(
    int idx, const std::vector<uint32_t> &shmIndexMapping,
    const std::vector<std::shared_ptr<ShmOwner>> &shmOwners) const
{
    if (idx < 0 || static_cast<size_t>(idx) >= shmIndexMapping.size()) {
        return nullptr;
    }
    const auto ownerIdx = shmIndexMapping[idx];
    return ownerIdx < shmOwners.size() ? shmOwners[ownerIdx] : nullptr;
}

Status WorkerOcServiceMigrateImpl::ProcessRemoteReadForObject(const MigrateDataDirectReqPb::ObjectInfoPb &object,
                                                              ObjectInfoMap::const_iterator needReadIt,
                                                              std::shared_ptr<ShmUnit> shmUnit, size_t metaSize,
                                                              std::vector<ReadTask> &tasks,
                                                              std::unordered_set<std::string> &failedIds)
{
    PerfPoint point(PerfKey::WORKER_SERVER_MIGRATE_DIRECT_REMOTE_READ_ONE);
    const auto &objectKey = object.object_key();
    const auto dataSize = object.data_size();
    const uint64_t localObjectAddress = reinterpret_cast<uint64_t>(shmUnit->GetPointer());
    uint64_t localSegAddress;
    uint64_t localSegSize;
    GetSegmentInfoFromShmUnit(shmUnit, localObjectAddress, localSegAddress, localSegSize);

    std::vector<uint64_t> eventKeys;
    Status rc =
        UrmaRead(object.urma_info(), localSegAddress, localSegSize, localObjectAddress, dataSize, metaSize, eventKeys);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[Migrate Data] %s urma read failed: %s", objectKey, rc.ToString());
        failedIds.insert(objectKey);
        return rc;
    }

    tasks.push_back(ReadTask{ objectKey, dataSize, std::move(eventKeys), std::move(shmUnit), needReadIt->second.first,
                              needReadIt->second.second, {} });
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::StartRemoteReadTasks(DirectReadOutcome &outcome,
                                                        const std::vector<uint32_t> &shmIndexMapping,
                                                        const std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
                                                        std::vector<ReadTask> &tasks)
{
    const auto &req = outcome.req;
    const auto &needReadDataIds = outcome.needReadDataIds;
    auto &failedIds = outcome.failedIds;
    tasks.reserve(static_cast<size_t>(req.objects_size()));

    const auto metaSize = GetMetadataSize();
    Status lastRc;

    auto markRemainingAsFailed = [&req, &needReadDataIds, &failedIds](int fromIdx) {
        for (int i = fromIdx; i < req.objects_size(); ++i) {
            const auto &objectKey = req.objects(i).object_key();
            if (needReadDataIds.find(objectKey) != needReadDataIds.end()) {
                failedIds.insert(objectKey);
            }
        }
    };

    for (int i = 0; i < req.objects_size(); ++i) {
        const auto &object = req.objects(i);
        const auto &objectKey = object.object_key();
        const auto needReadIt = needReadDataIds.find(objectKey);
        if (needReadIt == needReadDataIds.end()) {
            continue;
        }

        const auto dataSize = object.data_size();
        auto shmUnit = std::make_shared<ShmUnit>();
        const auto shmOwner = GetShmOwnerByIndex(i, shmIndexMapping, shmOwners);
        if (shmOwner) {
            lastRc = DistributeMemoryForObject(objectKey, dataSize, metaSize, true, shmOwner, *shmUnit);
        } else {
            lastRc =
                AllocateMemoryForObject(objectKey, dataSize, metaSize, true, evictionManager_, *shmUnit,
                                        CacheType::MEMORY, true);
        }
        if (lastRc.IsError()) {
            LOG(ERROR) << FormatString("[Migrate Data] %s allocate memory failed: %s", objectKey, lastRc.ToString());
            markRemainingAsFailed(i);
            break;
        }
        evictionManager_->Add(objectKey);

        lastRc = ProcessRemoteReadForObject(object, needReadIt, shmUnit, metaSize, tasks, failedIds);
    }

    return lastRc;
}

Status WorkerOcServiceMigrateImpl::WaitRemoteReadTasks(std::vector<ReadTask> &tasks, DirectReadOutcome &outcome)
{
    const auto metaSize = GetMetadataSize();
    Status lastRc;
    for (auto &task : tasks) {
        auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        Status waitRc = WaitFastTransportEventWithFailure(task.eventKeys, remainingTime, errorHandler, &task.failure);
        if (waitRc.IsError()) {
            outcome.failedIds.insert(task.objectKey);
            RecordDirectReadUbFailure(task, outcome, waitRc);
            lastRc = waitRc;
            continue;
        }

        outcome.needSendMasterIds.emplace(task.objectKey, std::make_pair(task.entry, task.isNewCreate));
        outcome.migratedBytes += task.dataSize;

        task.shmUnit->id = ShmKey::Intern(GetStringUuid());
        if (metaSize > 0) {
            (void)memset_s(task.shmUnit->GetPointer(), metaSize, 0, metaSize);
        }
        (*task.entry)->SetShmUnit(task.shmUnit);
        if (task.entry->Get()->IsWriteBackMode()) {
            std::future<Status> future;
            LOG_IF_ERROR(asyncSendManager_->Add(task.objectKey, task.entry, future),
                         FormatString("[Migrate Data] [%s] add to async queue failed", task.objectKey));
        }
    }
    return lastRc;
}

void WorkerOcServiceMigrateImpl::RecordDirectReadUbFailure(const ReadTask &task, DirectReadOutcome &outcome,
                                                           const Status &status)
{
    ProviderUbFailureDetailPb detail;
    FillMigrationUbReadFailureDetail(status, outcome.req.worker_addr(), localAddr_, task.failure, detail);
    const bool hasError4 = task.failure.providerStatus == URMA_PORT_UNAVAILABLE_STATUS
                           || task.failure.cqeStatus == URMA_PORT_UNAVAILABLE_STATUS;
    if (!outcome.failureDetail.has_value() || hasError4) {
        outcome.failureDetail = std::move(detail);
    }
    if (ubAdmission_ == nullptr) {
        return;
    }
    HostPort operatorWorker;
    HostPort remoteWorker;
    if (operatorWorker.ParseString(localAddr_).IsError()
        || remoteWorker.ParseString(outcome.req.worker_addr()).IsError()) {
        return;
    }
    ReportLocalUbOperationFailure(ubAdmission_, operatorWorker, remoteWorker,
                                  UbOperationKind::MIGRATION_READ, status, task.failure.providerStatus,
                                  task.failure.cqeStatus);
}

Status WorkerOcServiceMigrateImpl::ReplacePrimaryImpl(const std::string &originAddr,
                                                      const ObjectInfoMap &needSendMasterIds, const MigrateType &type,
                                                      PrimarySwitchOutcome &outcome)
{
    auto objectKeys = CollectObjectInfoKeys(needSendMasterIds);
    std::vector<std::string> objectKeyList{ objectKeys.begin(), objectKeys.end() };
    CHECK_FAIL_RETURN_STATUS(metadataRouteResolver_ != nullptr, K_NOT_READY, "Metadata route resolver is unavailable");
    auto grouped = metadataRouteResolver_->GroupOwners(objectKeyList);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;
    Status lastRc;
    RedirectMap needRedirectIds;
    for (auto &item : objKeysGrpByMaster) {
        Status rc = ReplacePrimaryForMaster(originAddr, item.first, item.second, needSendMasterIds, type, outcome,
                                            needRedirectIds);
        if (rc.IsError()) {
            lastRc = std::move(rc);
        }
    }

    Status rc = ReplacePrimaryToRedirectMaster(originAddr, needRedirectIds, needSendMasterIds, type, outcome);
    if (rc.IsError()) {
        lastRc = rc;
    }

    // A key omitted from every master response is not safe to release at the source. Treat it as failed explicitly.
    for (const auto &objectKey : objectKeys) {
        if (outcome.confirmedIds.count(objectKey) == 0 && outcome.expiredIds.count(objectKey) == 0
            && outcome.failedIds.count(objectKey) == 0) {
            outcome.failedIds.emplace(objectKey);
        }
    }
    // A confirmed switch is authoritative if retries or redirects produced duplicate classifications.
    for (const auto &objectKey : outcome.confirmedIds) {
        outcome.expiredIds.erase(objectKey);
        outcome.failedIds.erase(objectKey);
    }
    for (const auto &objectKey : outcome.expiredIds) {
        outcome.failedIds.erase(objectKey);
    }
    return lastRc;
}

Status WorkerOcServiceMigrateImpl::ReplacePrimaryForMaster(
    const std::string &originAddr, const HostPort &masterAddr, const std::vector<std::string> &ids,
    const ObjectInfoMap &needSendMasterIds, const MigrateType &type, PrimarySwitchOutcome &outcome,
    RedirectMap &needRedirectIds)
{
    master::ReplacePrimaryReqPb req;
    req.set_redirect(true);
    req.set_origin_primary_addr(originAddr);
    req.set_new_primary_addr(localAddr_);
    req.set_remove_location(type == MigrateType::SPILL);
    AddReplacePrimaryObjectInfos(req, ids, needSendMasterIds);
    VLOG(1) << FormatString("[Migrate Data] Replace %ld objects primary location from %s to %s, master address: %s",
                            ids.size(), originAddr, localAddr_, masterAddr.ToString());
    auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    if (workerMasterApi == nullptr) {
        std::stringstream ss;
        ss << "[Migrate Data] hash master get failed, Replace primary copy failed: " << masterAddr.ToString();
        LOG(ERROR) << ss.str();
        outcome.failedIds.insert(ids.begin(), ids.end());
        return Status(StatusCode::K_RUNTIME_ERROR, ss.str());
    }
    master::ReplacePrimaryRspPb rsp;
    Status rc = ReplacePrimaryRetry(workerMasterApi, req, rsp);
    if (rc.IsError()) {
        outcome.failedIds.insert(ids.begin(), ids.end());
        return rc;
    }
    ProcessReplacePrimaryRsp(rsp, needSendMasterIds, outcome, needRedirectIds);
    return Status::OK();
}

uint64_t WorkerOcServiceMigrateImpl::CalcRemainBytes(const MigrateType &type)
{
    switch (type) {
        case MigrateType::SPILL:
        case MigrateType::REBALANCE_KEEP_LOCAL:
            return memory::Allocator::Instance()->GetMemoryAvailToHighWater();
        case MigrateType::SCALE_DOWN:
        default: {
            constexpr double remainThreshold = 0.8;
            uint64_t remainBytes = memory::Allocator::Instance()->GetTotalRealMemoryFree();
            if (WorkerOcSpill::Instance()->IsEnabled()) {
                remainBytes += WorkerOcSpill::Instance()->GetRemainActiveSpillSize();
            }
            return static_cast<uint64_t>(remainBytes * remainThreshold);
        }
    }
}

void WorkerOcServiceMigrateImpl::FillMigrateDataResponse(const MigrateDataReqPb &req,
                                                         const std::unordered_set<std::string> &successIds,
                                                         const std::unordered_set<std::string> &expiredIds,
                                                         const std::unordered_set<std::string> &failedIds, bool oom,
                                                         MigrateDataRspPb &rsp,
                                                         const std::unordered_set<std::string> &skipIds)
{
    for (const auto &id : successIds) {
        rsp.add_success_ids(id);
    }
    for (const auto &id : expiredIds) {
        // Legacy SPILL sources already release every success id, so preserve their wire contract during rolling
        // upgrades. Keep-local needs an explicit classification because confirmed ids are retained as replicas while
        // expired versions must be erased.
        if (req.type() == MigrateType::REBALANCE_KEEP_LOCAL) {
            rsp.add_expired_ids(id);
        } else {
            rsp.add_success_ids(id);
        }
    }
    for (const auto &id : failedIds) {
        rsp.add_fail_ids(id);
    }
    for (const auto &id : skipIds) {
        rsp.add_skipped_object_keys(id);
    }
    if (oom) {
        rsp.set_remain_bytes(0);
        rsp.set_disk_remain_bytes(0);
        rsp.set_limit_rate(0);
    } else {
        if (req.bytes_send() > 0) {
            rateController_->SlidingWindowUpdateRate(req.bytes_send());
        }
        rsp.set_remain_bytes(CalcRemainBytes(req.type()));
        rsp.set_available_ratio(memory::Allocator::Instance()->GetMemoryAvailableRatio());
        uint64_t diskRemainBytes = memory::Allocator::Instance()->GetTotalRealMemoryFree(memory::CacheType::DISK);
        constexpr double remainThreshold = 0.8;
        rsp.set_disk_remain_bytes(static_cast<uint64_t>(diskRemainBytes * remainThreshold));
        rsp.set_disk_available_ratio(memory::Allocator::Instance()->GetMemoryAvailableRatio(memory::CacheType::DISK));
        if (req.worker_addr().empty()) {
            rsp.set_limit_rate(rateController_->PeekAvailableRate(req.worker_addr()));
        } else {
            rsp.set_limit_rate(rateController_->CalculateNewRate(req.worker_addr()));
        }
    }
    rsp.set_scale_down_state(MigrateDataRspPb::NONE);
}

void WorkerOcServiceMigrateImpl::FillMigrateDataDirectResponse(const MigrateDataDirectReqPb &req,
                                                               const std::unordered_set<std::string> &failedIds,
                                                               bool oom, uint64_t migratedBytes,
                                                               MigrateDataDirectRspPb &rsp,
                                                               const std::unordered_set<std::string> &skipIds)
{
    for (const auto &id : failedIds) {
        rsp.add_failed_object_keys(id);
    }
    for (const auto &id : skipIds) {
        rsp.add_skipped_object_keys(id);
    }
    if (oom) {
        rsp.set_remain_bytes(0);
        rsp.set_limit_rate(0);
    } else {
        rsp.set_remain_bytes(CalcRemainBytes(MigrateType::SPILL));
        rateController_->SlidingWindowUpdateRate(migratedBytes);
        rsp.set_limit_rate(rateController_->CalculateNewRate(req.worker_addr()));
    }
}

Status WorkerOcServiceMigrateImpl::PureQueryMetaOnce(const std::shared_ptr<worker::WorkerMasterOCApi> &api,
                                                     master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp)
{
    return api->PureQueryMeta(req, rsp);
}

Status WorkerOcServiceMigrateImpl::PureQueryMetaRetry(const std::shared_ptr<worker::WorkerMasterOCApi> &api,
                                                      master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp)
{
    const int maxRetryCount = 3;
    int count = 0;
    Status status;
    do {
        count++;
        status = PureQueryMetaOnce(api, req, rsp);
        if (IsRpcError(status)) {
            continue;
        }
        if (MetaMovingDone(rsp)) {
            break;
        }
        rsp.Clear();
    } while (count <= maxRetryCount);
    return status;
}

Status WorkerOcServiceMigrateImpl::PureQueryMetaToRedirectMaster(
    const std::unordered_map<std::string, std::unordered_set<std::string>> &redirectIds, QueryMetaMap &queryMetas,
    std::unordered_set<std::string> &failedIds)
{
    Status lastRc;
    for (const auto &info : redirectIds) {
        const auto &addr = info.first;
        const auto &objects = info.second;

        // 1. Fill redirect request.
        master::PureQueryMetaReqPb req;
        master::PureQueryMetaRspPb rsp;
        req.set_redirect(false);
        for (const auto &objectKey : objects) {
            req.add_object_keys(objectKey);
        }

        // 2. Get redirect master api.
        HostPort masterAddr;
        Status rc = masterAddr.ParseString(addr);
        if (!rc.IsOk()) {
            lastRc = rc;
            LOG(WARNING) << "[Migrate Data] Get redirect master address failed: " << rc.ToString();
            failedIds.insert(objects.begin(), objects.end());
            continue;
        }
        auto masterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        if (masterApi == nullptr) {
            LOG(ERROR) << "[Migrate Data] Failed to get redirect WorkerMasterApi, masterAddr: " << addr;
            failedIds.insert(objects.begin(), objects.end());
            continue;
        }

        // 3. Send request to redirect master.
        rc = PureQueryMetaRetry(masterApi, req, rsp);
        if (rc.IsError()) {
            lastRc = rc;
            LOG(WARNING) << "remove meta failed: " << rc.ToString();
        }

        for (const auto &meta : rsp.query_metas()) {
            (void)queryMetas.emplace(meta.meta().object_key(), meta);
        }
    }
    return lastRc;
}

Status WorkerOcServiceMigrateImpl::FillOneObjectLocked(std::shared_ptr<SafeObjType> &entry,
                                                       const MigrateDataReqPb::ObjectInfoPb &info,
                                                       const master::QueryMetaInfoPb &meta,
                                                       std::vector<RpcMessage> &payloads, const MigrateType &type,
                                                       ObjectInfoMap &needSendMasterIds, std::shared_ptr<ShmUnit> &unit)
{
    const auto &objectKey = info.object_key();
    if (meta.meta().version() != info.version()) {
        VLOG(1) << FormatString("[ObjectKey %s] Version %ld != %ld", objectKey, info.version(), meta.meta().version());
        // Version mismatch means the object has been update, we will no need to update.
        return Status::OK();
    }
    if ((*entry)->IsSpilled()) {
        LOG_IF_ERROR(WorkerOcSpill::Instance()->Delete(objectKey),
                     FormatString("[Migrate Data] Delete elder object %s failed", objectKey));
    }
    bool isNewCreate = IsNewCreatedObject(entry);
    SetObjectEntryAccordingToMeta(meta.meta(), GetMetadataSize(), *entry);
    RETURN_IF_NOT_OK(SaveDataWithObjectLocked(entry, info, payloads, type, unit));
    if ((*entry)->IsMemoryCache() && (*entry)->GetShmUnit() == nullptr) {
        (*entry)->stateInfo.SetSpillState(true);
    }
    // Keep the staged object non-primary until master confirms ReplacePrimary. The object WLock remains held across
    // the RPC, so eviction cannot delete the pending copy before the local state is committed or rolled back.
    (*entry)->stateInfo.SetPrimaryCopy(false);
    needSendMasterIds.emplace(objectKey, std::make_pair(entry, isNewCreate));
    if ((*entry).Get()->IsWriteBackMode()) {
        std::future<Status> future;
        LOG_IF_ERROR(asyncSendManager_->Add(objectKey, entry, future),
                     FormatString("[Migrate Data] [%s] add to async queue failed", objectKey));
    }
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::SaveDataWithObjectLocked(std::shared_ptr<SafeObjType> &entry,
                                                            const MigrateDataReqPb::ObjectInfoPb &info,
                                                            std::vector<RpcMessage> &payloads, const MigrateType &type,
                                                            std::shared_ptr<ShmUnit> unit)
{
    const auto &objectKey = info.object_key();
    const auto &indexs = info.part_index();
    if (indexs.empty()) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("[Migrate Data] [%s]'s part index [%s] is large than payloads size [%ld]",
                                             objectKey, VectorToString(info.part_index()), payloads.size()));
    }

    std::vector<std::pair<const uint8_t *, uint64_t>> pairs;
    for (int i = 0; i < indexs.size(); ++i) {
        auto index = indexs[i];
        if (index >= payloads.size()) {
            RETURN_STATUS_LOG_ERROR(
                StatusCode::K_RUNTIME_ERROR,
                FormatString("[Migrate Data] [%s]'s part index [%s] is large than payloads size [%ld]", objectKey,
                             VectorToString(info.part_index()), payloads.size()));
        }
        pairs.emplace_back(std::make_pair<const uint8_t *, uint64_t>(
            static_cast<const uint8_t *>(payloads[index].Data()), payloads[index].Size()));
    }

    Status rc = Status(StatusCode::K_OUT_OF_MEMORY, "OOM");
    const bool memoryRebalance = (*entry)->IsMemoryCache()
                                 && (type == MigrateType::SPILL || type == MigrateType::REBALANCE_KEEP_LOCAL);
    if (unit != nullptr || memoryRebalance
        || IsResourceAvailable(type, (*entry)->modeInfo.GetCacheType(), info.data_size())) {
        rc = AllocateAndAssignData(objectKey, entry, pairs, info.data_size(), unit,
                                   memoryRebalance && evictionManager_ != nullptr);
        VLOG(1) << FormatString("[ObjectKey %s] Save data to memory, result: %s", objectKey, rc.ToString());
    }
    if (type == MigrateType::SCALE_DOWN && (*entry)->IsMemoryCache() && rc.IsError()
        && IsSpillAvaialble(info.data_size())) {
        rc = WorkerOcSpill::Instance()->Spill(objectKey, pairs, info.data_size());
        VLOG(1) << FormatString("[ObjectKey %s] Save data to spill dir, result: %s", objectKey, rc.ToString());
        LOG_IF(ERROR, rc.IsError()) << FormatString("[Migrate Data] Spill object [%s] failed: %s", objectKey,
                                                    rc.ToString());
    }
    return rc;
}

Status WorkerOcServiceMigrateImpl::BatchAllocateObjectGroupBySlot(
    const MigrateDataReqPb &req, std::unordered_map<std::string, std::shared_ptr<ShmUnit>> &units)
{
    for (const auto &info : req.objects()) {
        const auto &objectKey = info.object_key();
        auto shmUnit = std::make_shared<ShmUnit>();
        auto metaSize = GetMetadataSize();
        auto needSize = info.data_size() + metaSize;
        auto tenantId = TenantAuthManager::ExtractTenantId(objectKey);
        auto status = shmUnit->AllocateMemory(tenantId, needSize, false, ServiceType::OBJECT,
                                              static_cast<memory::CacheType>(info.cache_type()));
        if (IsNoSpace(status)) {
            LOG(ERROR) << FormatString("[Migrate Data] %s allocate memory failed, size: %ld", objectKey, needSize);
            return status;
        }
        shmUnit->id = ShmKey::Intern(GetStringUuid());
        units[objectKey] = shmUnit;
    }
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::AllocateAndAssignData(
    const std::string &objectKey, std::shared_ptr<SafeObjType> &entry,
    const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads, uint64_t size, std::shared_ptr<ShmUnit> unit,
    bool retryOnOOM)
{
    auto metaSize = GetMetadataSize();
    auto needSize = size + metaSize;
    std::shared_ptr<ShmUnit> shmUnit = unit;
    if (shmUnit == nullptr) {
        shmUnit = std::make_shared<ShmUnit>();
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            AllocateMemoryForObject(objectKey, size, metaSize, false, evictionManager_, *shmUnit,
                                    (*entry)->modeInfo.GetCacheType(), retryOnOOM),
            FormatString("[Migrate Data] %s allocate memory failed, size: %ld", objectKey, needSize));
        shmUnit->id = ShmKey::Intern(GetStringUuid());
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        shmUnit->MemoryCopy(payloads, memcpyThreadPool_, metaSize),
        FormatString("[Migrate Data] Memory copy failed, offset: %ld, size: %ld", metaSize, needSize));
    if (metaSize > 0) {
        (void)memset_s(shmUnit->GetPointer(), metaSize, 0, metaSize);
    }
    (*entry)->SetShmUnit(shmUnit);
    evictionManager_->Add(objectKey);
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::ReplacePrimaryOnce(const std::shared_ptr<worker::WorkerMasterOCApi> &api,
                                                      master::ReplacePrimaryReqPb &req,
                                                      master::ReplacePrimaryRspPb &rsp)
{
    return api->ReplacePrimary(req, rsp);
}

Status WorkerOcServiceMigrateImpl::ReplacePrimaryRetry(const std::shared_ptr<worker::WorkerMasterOCApi> &api,
                                                       master::ReplacePrimaryReqPb &req,
                                                       master::ReplacePrimaryRspPb &rsp)
{
    constexpr int maxRetryCount = 3;
    int count = 0;
    Status status;
    do {
        ++count;
        status = ReplacePrimaryOnce(api, req, rsp);
        if (!status.IsOk()) {
            continue;
        }
        if (MetaMovingDone(rsp)) {
            break;
        }
    } while (count <= maxRetryCount);

    if (IsRetryableRpcError(status) || IsNonRetryableRpcError(status)) {
        rsp.Clear();
        for (const auto &info : req.object_infos()) {
            rsp.add_failed_ids(info.object_key());
        }
    }
    return status;
}

Status WorkerOcServiceMigrateImpl::ReplacePrimaryToRedirectMaster(const std::string &originAddr,
                                                                  const RedirectMap &needRedirectIds,
                                                                  const ObjectInfoMap &needSendMasterIds,
                                                                  const MigrateType &type,
                                                                  PrimarySwitchOutcome &outcome)
{
    Status lastRc;
    for (const auto &item : needRedirectIds) {
        const auto &address = item.first;
        const auto &infos = item.second;

        // 1. Fill redirect request.
        VLOG(1) << FormatString("[Migrate Data] Redirect Replace %ld objects primary, meta address: %s", infos.size(),
                                address);
        master::ReplacePrimaryReqPb req;
        master::ReplacePrimaryRspPb rsp;
        req.set_origin_primary_addr(originAddr);
        req.set_new_primary_addr(localAddr_);
        req.set_remove_location(type == MigrateType::SPILL);
        req.set_redirect(false);
        for (const auto &info : infos) {
            auto newInfo = req.add_object_infos();
            newInfo->CopyFrom(info);
        }

        // 2. Get redirect master api.
        HostPort masterAddr;
        Status rc = masterAddr.ParseString(address);
        if (!rc.IsOk()) {
            lastRc = rc;
            LOG(WARNING) << "[Migrate Data] Get redirect master address failed: " << rc.ToString();
            std::transform(infos.begin(), infos.end(), std::inserter(outcome.failedIds, outcome.failedIds.end()),
                           [](const auto &info) { return info.object_key(); });
            continue;
        }

        auto masterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        if (masterApi == nullptr) {
            std::stringstream ss;
            ss << "[Migrate Data] Failed to get redirect WorkerMasterApi, masterAddr: " << address;
            LOG(ERROR) << ss.str();
            lastRc = Status(StatusCode::K_RUNTIME_ERROR, ss.str());
            std::transform(infos.begin(), infos.end(), std::inserter(outcome.failedIds, outcome.failedIds.end()),
                           [](const auto &info) { return info.object_key(); });
            continue;
        }

        // 3. Send request to redirect master.
        rc = ReplacePrimaryRetry(masterApi, req, rsp);
        if (rc.IsError()) {
            lastRc = rc;
            LOG(WARNING) << "remove meta failed: " << rc.ToString();
            std::transform(infos.begin(), infos.end(), std::inserter(outcome.failedIds, outcome.failedIds.end()),
                           [](const auto &info) { return info.object_key(); });
            continue;
        }
        RedirectMap needRedirectIds1;
        ProcessReplacePrimaryRsp(rsp, needSendMasterIds, outcome, needRedirectIds1);
        if (!needRedirectIds1.empty()) {
            LOG(WARNING) << "[Migrate Data] The redirect ids should not happen: " << needRedirectIds1.size();
        }
    }

    return lastRc;
}

void WorkerOcServiceMigrateImpl::ProcessReplacePrimaryRsp(master::ReplacePrimaryRspPb &rsp,
                                                          const ObjectInfoMap &needSendMasterIds,
                                                          PrimarySwitchOutcome &outcome,
                                                          RedirectMap &needRedirectIds)
{
    outcome.expiredIds.insert(rsp.expired_ids().begin(), rsp.expired_ids().end());
    outcome.confirmedIds.insert(rsp.success_ids().begin(), rsp.success_ids().end());
    outcome.failedIds.insert(rsp.failed_ids().begin(), rsp.failed_ids().end());

    // 2. Fill redirect infos.
    for (const auto &redirectInfo : rsp.info()) {
        const auto &addr = redirectInfo.redirect_meta_address();
        for (const auto &objectKey : redirectInfo.change_meta_ids()) {
            auto it = needSendMasterIds.find(objectKey);
            if (it == needSendMasterIds.end()) {
                LOG(WARNING) << FormatString("[Migrate Data] %s not found in needSendMasterIds, it should not happen!",
                                             objectKey);
                (void)outcome.failedIds.emplace(objectKey);
                continue;
            }
            auto info = needRedirectIds[addr].Add();
            info->set_object_key(objectKey);
            info->set_version((*it->second.first)->GetCreateTime());
        }
    }
}

void WorkerOcServiceMigrateImpl::FinalizePrimarySwitch(const ObjectInfoMap &needSendMasterIds,
                                                       const ObjectInfoMap &stagedObjectInfos,
                                                       const PrimarySwitchOutcome &outcome,
                                                       std::unordered_set<std::string> &successIds,
                                                       std::unordered_set<std::string> &failedIds)
{
    for (const auto &objectKey : outcome.confirmedIds) {
        auto iter = needSendMasterIds.find(objectKey);
        if (iter == needSendMasterIds.end()) {
            LOG(ERROR) << FormatString("[Migrate Data] Confirmed primary object %s is not in send list", objectKey);
            failedIds.emplace(objectKey);
            continue;
        }
        (*iter->second.first)->stateInfo.SetPrimaryCopy(true);
        successIds.emplace(objectKey);
    }

    // Expired means the source request is terminal and may release its stale copy, but data staged by this request is
    // not a committed primary and must be rolled back on the target.
    std::unordered_set<std::string> stagedExpiredIds;
    std::copy_if(outcome.expiredIds.begin(), outcome.expiredIds.end(),
                 std::inserter(stagedExpiredIds, stagedExpiredIds.end()),
                 [&stagedObjectInfos](const auto &objectKey) { return stagedObjectInfos.count(objectKey) > 0; });
    RollbackObjects(stagedExpiredIds, stagedObjectInfos);

    failedIds.insert(outcome.failedIds.begin(), outcome.failedIds.end());
    std::unordered_set<std::string> stagedFailedIds;
    std::copy_if(outcome.failedIds.begin(), outcome.failedIds.end(),
                 std::inserter(stagedFailedIds, stagedFailedIds.end()),
                 [&stagedObjectInfos](const auto &objectKey) { return stagedObjectInfos.count(objectKey) > 0; });
    RollbackObjects(stagedFailedIds, stagedObjectInfos);

    // Keep response sets disjoint even if an earlier phase classified a key before master confirmation.
    for (const auto &objectKey : successIds) {
        failedIds.erase(objectKey);
    }
}

void WorkerOcServiceMigrateImpl::ApplyConfirmedMigratedHeats(const MigrateDataReqPb &req,
                                                             const ObjectInfoMap &needSendMasterIds,
                                                             const ObjectInfoMap &stagedObjectInfos,
                                                             const PrimarySwitchOutcome &outcome)
{
    if (evictionManager_ == nullptr || outcome.confirmedIds.empty()) {
        return;
    }
    for (const auto &info : req.objects()) {
        const auto &objectKey = info.object_key();
        if (!info.has_heat() || outcome.confirmedIds.count(objectKey) == 0
            || needSendMasterIds.count(objectKey) == 0) {
            continue;
        }
        auto stagedIt = stagedObjectInfos.find(objectKey);
        // A newly created target object has only the synthetic initial heat and should restore the source value
        // exactly. An existing target replica may have observed local reads, so retain the higher of both values.
        const bool mergeExisting = stagedIt == stagedObjectInfos.end() || !stagedIt->second.second;
        auto rc = evictionManager_->ApplyMigratedHeat(objectKey, info.heat(), mergeExisting);
        LOG_IF(WARNING, rc.IsError()) << FormatString(
            "[Migrate Data] Ignore invalid or unavailable heat for confirmed object %s: %s", objectKey,
            rc.ToString());
    }
}

template <typename Container>
void WorkerOcServiceMigrateImpl::RollbackObjects(const Container &objectKeys, const ObjectInfoMap &objectInfos)
{
    for (const auto &objectKey : objectKeys) {
        VLOG(1) << "Rollback object: " << objectKey;
        auto it = objectInfos.find(objectKey);
        if (it == objectInfos.end()) {
            LOG(WARNING) << FormatString("[Migrate Data] %s is not found in send list, it should not happen in general",
                                         objectKey);
            continue;
        }
        bool needDel = it->second.second;
        auto &entry = it->second.first;
        if ((*entry)->IsSpilled() && (*entry)->GetShmUnit() == nullptr) {
            LOG_IF_ERROR(WorkerOcSpill::Instance()->Delete(objectKey),
                         FormatString("[Migrate Data] Rollback %s from disk failed", objectKey));
        } else {
            evictionManager_->Erase(objectKey);
        }
        if (needDel) {
            (void)objectTable_->Erase(objectKey, *entry);
        } else {
            (*entry)->stateInfo.SetSpillState(false);
            (*entry)->stateInfo.SetCacheInvalid(true);
            (*entry)->SetShmUnit(nullptr);
        }
    }
}

bool WorkerOcServiceMigrateImpl::IsEqualVersion(const std::shared_ptr<SafeObjType> &entry, uint64_t version)
{
    auto *entryImpl = entry->Get();
    return !entryImpl->stateInfo.IsCacheInvalid() && entryImpl->GetCreateTime() >= version;
}

bool WorkerOcServiceMigrateImpl::IsNewerVersion(const std::shared_ptr<SafeObjType> &entry, uint64_t version)
{
    auto *entryImpl = entry->Get();
    return (entryImpl->stateInfo.IsCacheInvalid() && entryImpl->GetCreateTime() > version)
           || (!entryImpl->stateInfo.IsCacheInvalid() && entryImpl->GetCreateTime() >= version);
}

bool WorkerOcServiceMigrateImpl::IsNewCreatedObject(std::shared_ptr<SafeObjType> &entry) const
{
    return (*entry)->GetCreateTime() == 0 && (*entry)->GetDataSize() == 0;
}

bool WorkerOcServiceMigrateImpl::IsMemoryAvailable(uint64_t size, MigrateType type) const
{
    INJECT_POINT("worker.migrate_service.memory_available", []() { return false; });
    INJECT_POINT("worker.migrate_service.memory_available1", []() {
        static int count = 0;
        constexpr int maxCount = 200;
        return count++ < maxCount;
    });
    uint64_t usedMemory = memory::Allocator::Instance()->GetTotalRealMemoryUsage();
    if (usedMemory > UINT64_MAX - size) {
        // overflow check
        return false;
    }
    uint64_t memOccupied = usedMemory + size;
    uint64_t freeMemory = memory::Allocator::Instance()->GetTotalRealMemoryFree();
    if (usedMemory > UINT64_MAX - freeMemory) {
        // overflow check
        return false;
    }
    switch (type) {
        case MigrateType::SPILL:
        case MigrateType::REBALANCE_KEEP_LOCAL:
            return memory::Allocator::Instance()->GetMemoryAvailToHighWater() > size;
        case MigrateType::SCALE_DOWN:
        default:
            return memOccupied <= (usedMemory + freeMemory) * MIGRATE_SCALE_DOWN_HIGH_WATER_FACTOR;
    }
}

bool WorkerOcServiceMigrateImpl::IsSpillAvaialble(uint64_t size) const
{
    INJECT_POINT("worker.migrate_service.spill_available", []() { return false; });
    INJECT_POINT("worker.migrate_service.spill_available1", []() {
        static int count = 0;
        constexpr int maxCount = 200;
        return count++ < maxCount;
    });
    auto handler = WorkerOcSpill::Instance();
    return handler->IsEnabled() && !handler->IsActiveSpillSizeExceedHWM(size);
}

bool WorkerOcServiceMigrateImpl::IsDiskAvailable(uint64_t size) const
{
    if (!memory::Allocator::Instance()->IsDiskAvailable()) {
        constexpr int freq = 10;
        LOG_EVERY_T(INFO, freq) << "[Migrate Data] Disk now is not available";
        return false;
    }
    auto realMemoryUsage =
        memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::OBJECT, memory::CacheType::DISK);
    uint64_t used = size < UINT64_MAX - realMemoryUsage ? realMemoryUsage + size : UINT64_MAX;
    uint64_t total = memory::Allocator::Instance()->GetMaxMemoryLimit(memory::CacheType::DISK);
    const double factor = 0.95;
    return used <= total * factor;
}

bool WorkerOcServiceMigrateImpl::IsResourceAvailable(const MigrateType &type, CacheType cacheType, uint64_t size) const
{
    return cacheType == CacheType::MEMORY ? IsMemoryAvailable(size, type) : IsDiskAvailable(size);
}

bool WorkerOcServiceMigrateImpl::IsNoSpace(const Status &status) const
{
    return status.GetCode() == StatusCode::K_NO_SPACE || status.GetCode() == StatusCode::K_OUT_OF_MEMORY;
}
}  // namespace object_cache
}  // namespace datasystem
