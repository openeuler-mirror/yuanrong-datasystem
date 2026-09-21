/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Migrate data.
 */
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/worker/object_cache/data_migrator/data_migrator.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <thread>

#include "datasystem/cluster/executor/topology_phase_callbacks.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/l2cache/slot_client/slot_internal_config.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/scale_down_node_selector.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/spill_node_selector.h"

DS_DECLARE_string(data_migrate_urma_transport_mode);
DS_DECLARE_bool(enable_transport_fallback);

namespace datasystem {
namespace object_cache {

namespace {
constexpr int K_MAX_PINNED_RETRY_ROUNDS = 4;
constexpr auto TOPOLOGY_MIGRATION_RETRY_INTERVAL = std::chrono::milliseconds(100);
constexpr int MIGRATION_RETRY_LOG_EVERY_N = 10;
}

void DataMigrator::Init()
{
    const uint32_t threadPoolSize = 4;
    threadPool_ = std::make_unique<ThreadPool>(0, threadPoolSize, "OcMigrateData");
}

Status DataMigrator::CheckSourceAdmission() const
{
    const auto role = type_ == MigrateType::SCALE_DOWN && !taskId_.empty()
                          ? DataPlaneAdmissionRole::TOPOLOGY_SCALE_IN_SOURCE
                          : DataPlaneAdmissionRole::ORDINARY_SOURCE;
    RETURN_IF_NOT_OK(endpointPolicy_.CheckDataPlaneAdmission(localAddress_, role));
    if (FLAGS_data_migrate_urma_transport_mode != "read") {
        RETURN_IF_NOT_OK(CheckUbAdmission(localAddress_, UbOperationKind::MIGRATION_WRITE));
    }
    return Status::OK();
}

Status DataMigrator::CheckTargetAdmission(const HostPort &target, DataPlaneAdmissionRole role) const
{
    RETURN_IF_NOT_OK(endpointPolicy_.CheckDataPlaneAdmission(target, role));
    const auto operation = FLAGS_data_migrate_urma_transport_mode == "read" ? UbOperationKind::MIGRATION_READ
                                                                           : UbOperationKind::MIGRATION_WRITE;
    return CheckUbAdmission(target, operation);
}

Status DataMigrator::CheckUbAdmission(const HostPort &worker, UbOperationKind operation) const
{
    if (!IsUrmaEnabled() || ubAdmission_ == nullptr) {
        return Status::OK();
    }
    auto status = ubAdmission_->CheckWriteTarget(worker, operation);
    const bool canUseTcpFallback = FLAGS_enable_transport_fallback && FLAGS_data_migrate_urma_transport_mode == "write";
    if (status.IsError() && canUseTcpFallback) {
        LOG_FIRST_AND_EVERY_N(WARNING, MIGRATION_RETRY_LOG_EVERY_N)
            << "[Migrate Data] UB admission denied for " << worker.ToString()
            << "; continue through the existing TCP fallback: " << status.ToString();
        return Status::OK();
    }
    return status;
}

std::shared_ptr<SelectionStrategy> DataMigrator::GetStrategyByType()
{
    switch (type_) {
        case MigrateType::SPILL:
        case MigrateType::REBALANCE_KEEP_LOCAL:
            return std::make_shared<SpillNodeSelector>(localAddress_);
        case MigrateType::SCALE_DOWN:
        default:
            return std::make_shared<ScaleDownNodeSelector>(membership_, localAddress_);
    }
}

Status DataMigrator::GetStandbyWorker(std::string &standbyWorker) const
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(membership_.GetSnapshot(snapshot));
    const cluster::Member *standby = nullptr;
    if (type_ == MigrateType::SCALE_DOWN && !taskId_.empty()) {
        RETURN_IF_NOT_OK(snapshot->FindNextActiveMember(localAddress_.ToString(), standby));
    } else {
        RETURN_IF_NOT_OK(snapshot->FindNextCommittedMember(localAddress_.ToString(), standby));
    }
    standbyWorker = standby->identity.address;
    return Status::OK();
}

uint64_t DataMigrator::CalculateTotalSize(const std::unordered_set<ImmutableString> &objectKeys,
                                          const std::unordered_map<std::string, uint64_t> &objectSizes)
{
    if (objectSizes.empty()) {
        return 0;
    }
    uint64_t totalSize = 0;
    for (const auto &key : objectKeys) {
        if (auto it = objectSizes.find(key); it != objectSizes.end()) {
            totalSize += it->second;
        }
    }
    return totalSize;
}

void DataMigrator::LogMigrateProgress(double elapsedSeconds, uint64_t processCount, uint64_t count)
{
    if (processCount < count) {
        LOG(INFO) << FormatString(
            "[Migrate Data Process] The task has been executed for %.2f seconds, %ld/%ld objects finished, "
            "still have %ld objects need to migrate data...",
            elapsedSeconds, processCount, count, (count - processCount));
    } else if (processCount == count) {
        LOG(INFO) << FormatString(
            "[Migrate Data Process] The task is complete(%ld objects) and takes for %.2f seconds.", count,
            elapsedSeconds);
    } else {
        LOG(WARNING) << FormatString(
            "[Migrate Data Process] The task has been executed for %.2f seconds, %ld/%ld objects finished, "
            "something wrong happen...",
            elapsedSeconds, processCount, count);
    }
}

std::shared_ptr<MigrateProgress> DataMigrator::CreateMigrateProgress(uint64_t objectCount)
{
    constexpr uint64_t intervalSeconds = 60;
    return std::make_shared<MigrateProgress>(objectCount, intervalSeconds, DataMigrator::LogMigrateProgress);
}

Status DataMigrator::Migrate(const std::vector<std::string> &objectKeys,
                             const std::unordered_map<std::string, uint64_t> &objectSizes)
{
    PerfPoint pointAll(PerfKey::WORKER_MIGRATE_E2E);
    if (objectKeys.empty()) {
        LOG(INFO) << "[Migrate Data] No object data need to be migrated, we have finish the job, task id: " << taskId_;
        return Status::OK();
    }
    RETURN_IF_NOT_OK(CheckSourceAdmission());
    progress_ = CreateMigrateProgress(objectKeys.size());
    failedKeys_.clear();
    skippedKeys_.clear();
    LOG(INFO) << FormatString(
        "[Migrate Data] Processing data migrate begin, migrate type: %d, object size: %zu, task id: %s",
        static_cast<int>(type_), objectKeys.size(), taskId_);

    PerfPoint point(PerfKey::WORKER_MIGRATE_TASK_SUBMIT);
    std::vector<std::future<MigrateDataHandler::MigrateResult>> futures;
    // During a ScaleIn drain the committed owner of the leaving Worker's own keys still resolves to itself, so the
    // redirect override (post-scale-in token owner) must decide the target; FindNextActiveMember below is only the
    // fallback for groups without a redirect override.
    auto grouped = type_ == MigrateType::SCALE_DOWN ? metadataRoute_.GroupMigrateTargets(objectKeys)
                                                    : metadataRoute_.GroupOwners(objectKeys);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;
    INJECT_POINT("DataMigrator.GetMasterAddr", [&objKeysGrpByMaster, &objectKeys]() {
        objKeysGrpByMaster.clear();
        (void)objKeysGrpByMaster.emplace(HostPort(), objectKeys);
        return Status::OK();
    });
    std::string standbyWorker;
    LOG_IF_ERROR(GetStandbyWorker(standbyWorker), "[Migrate Data] Failed to select standby worker");
    for (const auto &[addr, objectKeys] : objKeysGrpByMaster) {
        auto workerAddr = addr;
        if ((workerAddr.Empty() || workerAddr == localAddress_) && !standbyWorker.empty()) {
            LOG_IF_ERROR(workerAddr.ParseString(standbyWorker), "[Migrate Data] Parse worker address failed");
            INJECT_POINT_NO_RETURN("DataMigrator.AllowLocalWorker",
                                   [this, &workerAddr]() { workerAddr = localAddress_; });
        }
        futures.emplace_back(MigrateDataByNode(workerAddr, objectKeys, GetStrategyByType()));
    }

    point.RecordAndReset(PerfKey::WORKER_MIGRATE_TASK_EXECUTE);
    while (!futures.empty()) {
        std::vector<std::future<MigrateDataHandler::MigrateResult>> newFutures;
        RETURN_IF_NOT_OK(HandleMigrateDataResult(objectSizes, futures, newFutures));
        futures.swap(newFutures);
    }
    return Status::OK();
}

std::future<MigrateDataHandler::MigrateResult> DataMigrator::MigrateToSpecificNode(
    const std::vector<std::string> &objectKeys, const HostPort &targetAddr, std::shared_ptr<SelectionStrategy> strategy)
{
    if (targetAddr == localAddress_) {
        return ConstructFailedFuture(
            targetAddr.ToString(),
            Status(StatusCode::K_DUPLICATED,
                   FormatString("[Migrate Data] Target node %s is ourselves", targetAddr.ToString())),
            objectKeys, strategy);
    }

    std::shared_ptr<WorkerRemoteWorkerOCApi> remoteWorkerStub;
    Status rc = ConnectAndCreateRemoteApi(remoteWorkerStub, targetAddr);
    if (rc.IsError()) {
        return ConstructFailedFuture(targetAddr.ToString(), rc, objectKeys, strategy);
    }

    auto traceID = Trace::Instance().GetTraceID();
    if (traceID.empty()) {
        traceID = "migr;" + GetStringUuid();
    }
    return threadPool_->Submit([this, remoteWorkerStub, objectKeys, traceID, strategy]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        return MigrateDataByNodeImpl(remoteWorkerStub, objectKeys, strategy, true);
    });
}

std::future<MigrateDataHandler::MigrateResult> DataMigrator::MigrateToTargetNode(
    const std::vector<std::string> &objectKeys, const HostPort &targetAddr, std::shared_ptr<SelectionStrategy> strategy,
    TargetMigrationOptions options)
{
    if (!strategy) {
        strategy = GetStrategyByType();
    }

    auto traceID = Trace::Instance().GetTraceID();
    if (traceID.empty()) {
        traceID = "migr;" + GetStringUuid();
    }
    return threadPool_->Submit(
        [this, objectKeys, targetAddr, traceID, strategy, options = std::move(options)]() mutable {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);

            MigrateDataHandler::MigrateResult finalResult;
            finalResult.address = targetAddr.ToString();
            finalResult.strategy = strategy;

            std::shared_ptr<WorkerRemoteWorkerOCApi> remoteWorkerStub;
            Status rc = ConnectAndCreateRemoteApi(remoteWorkerStub, targetAddr);
            if (rc.IsError()) {
                LOG(ERROR) << "connect to remote worker " << finalResult.address << "failed: failed rc:"
                           << rc.ToString();
                finalResult.status = rc;
                finalResult.failedIds.insert(objectKeys.begin(), objectKeys.end());
                return finalResult;
            }

            std::vector<ImmutableString> needMigrateDataIds{ objectKeys.begin(), objectKeys.end() };
            MigrateDataHandler handler(type_, localAddress_.ToString(), needMigrateDataIds, objectTable_,
                                       remoteWorkerStub, strategy,
                                       options.cancellation == nullptr ? &stopping_ : options.cancellation, progress_,
                                       options.isRetry, options.slotId, std::move(options.objectHeats),
                                       std::move(options.rebalancePolicyFence));
            rc = ConfigureSendAdmission(handler, targetAddr.ToString());
            if (rc.IsError()) {
                finalResult.status = rc;
                finalResult.failedIds.insert(objectKeys.begin(), objectKeys.end());
                return finalResult;
            }
            auto result = handler.MigrateDataToRemote(options.isSlotMigration);
            ObserveUbHealthSummary(result);
            bool localOperator = false;
            (void)LearnStructuredUbFailure(result, localOperator);
            return result;
        });
}

Status DataMigrator::MigrateL2CacheBySlot(const std::vector<std::string> &objectKeys)
{
    constexpr int maxSameNodeRetryCount = 10;
    if (objectKeys.empty()) {
        LOG(INFO) << "[MigrateL2Cache] No L2 cache data need to migrate";
        return Status::OK();
    }

    progress_ = CreateMigrateProgress(objectKeys.size());
    failedKeys_.clear();
    skippedKeys_.clear();

    LOG(INFO) << FormatString("[MigrateL2Cache] Start migrating %zu L2 cache objects", objectKeys.size());

    auto objectsBySlot = GroupL2CacheObjectsBySlot(objectKeys);

    LOG(INFO) << FormatString("[MigrateL2Cache] Grouped into %zu slots", objectsBySlot.size());

    std::string standbyWorker;
    LOG_IF_ERROR(GetStandbyWorker(standbyWorker), "[MigrateL2Cache] Failed to select standby worker");

    std::vector<SlotMigrateFuture> futures;
    SlotRetryCounters sameNodeRetryCounts;
    SubmitL2CacheTasksBySlot(objectsBySlot, standbyWorker, futures);
    RETURN_IF_NOT_OK(ProcessL2CacheSlotFutures(futures, maxSameNodeRetryCount, sameNodeRetryCounts));

    LOG(INFO) << FormatString("[MigrateL2Cache] Finished");

    return Status::OK();
}

std::map<uint32_t, std::vector<std::string>> DataMigrator::GroupL2CacheObjectsBySlot(
    const std::vector<std::string> &objectKeys) const
{
    auto slotNum = DISTRIBUTED_DISK_SLOT_NUM;
    std::map<uint32_t, std::vector<std::string>> objectsBySlot;
    INJECT_POINT("TestGroupL2CacheObjectsBySlot", [&slotNum, &objectsBySlot] {
        slotNum = 1;
        return objectsBySlot;
    });
    for (const auto &objectKey : objectKeys) {
        uint32_t hash = MurmurHash3_32(objectKey);
        uint32_t slot = hash % slotNum;
        objectsBySlot[slot].push_back(objectKey);
    }
    return objectsBySlot;
}

void DataMigrator::SubmitL2CacheTasksBySlot(const std::map<uint32_t, std::vector<std::string>> &objectsBySlot,
                                            const std::string &standbyWorker, std::vector<SlotMigrateFuture> &futures)
{
    for (const auto &[slot, objs] : objectsBySlot) {
        auto grouped = metadataRoute_.GroupMigrateTargets(objs);
        AppendRouteFailures(grouped);
        for (const auto &[addr, keys] : grouped.groups) {
            HostPort currentTarget = addr;
            if ((currentTarget.Empty() || currentTarget == localAddress_) && !standbyWorker.empty()) {
                (void)currentTarget.ParseString(standbyWorker);
            }
            if (currentTarget.Empty() || currentTarget == localAddress_) {
                LOG(ERROR) << FormatString("[MigrateL2Cache] Slot %u has no available standby target, drop %zu objects",
                                           slot, keys.size());
                failedKeys_.insert(keys.begin(), keys.end());
                continue;
            }

            LOG(INFO) << FormatString("[MigrateL2Cache] Slot %u (%zu objects) -> %s", slot, keys.size(),
                                      currentTarget.ToString());
            futures.emplace_back(slot, MigrateToTargetNode(keys, currentTarget,
                                                           std::make_shared<ScaleDownNodeSelector>(membership_,
                                                                                                   localAddress_),
                                                           { .isRetry = false, .slotId = slot }));
        }
    }
}

bool DataMigrator::TrySubmitSameNodeRetryForL2Slot(uint32_t slot, const MigrateDataHandler::MigrateResult &result,
                                                   int maxSameNodeRetryCount, SlotRetryCounters &sameNodeRetryCounts,
                                                   std::vector<SlotMigrateFuture> &newFutures)
{
    const auto retryKey = SlotRetryCounters::key_type{ slot, result.address };
    const bool enteredSameNodeRetry = sameNodeRetryCounts[retryKey] > 0;
    if (result.successIds.empty() && !enteredSameNodeRetry) {
        return false;
    }

    int retryCount = ++sameNodeRetryCounts[retryKey];
    if (retryCount > maxSameNodeRetryCount) {
        LOG(WARNING) << FormatString(
            "[MigrateL2Cache] Slot %u same-node failedIds retry exceeded max(%d), stop retry on node %s and fail "
            "%zu keys",
            slot, maxSameNodeRetryCount, result.address.c_str(), result.failedIds.size());
        failedKeys_.insert(result.failedIds.begin(), result.failedIds.end());
        return true;
    }

    HostPort sameHost;
    Status rc = sameHost.ParseString(result.address);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[MigrateL2Cache] Parse node address failed for same-node retry: %s, status: %s",
                                   result.address, rc.ToString());
        return true;
    }

    VLOG(1) << FormatString(
        "[MigrateL2Cache] Slot %u retry failed ids on same node %s (%d/%d), success count: %zu, failed count: %zu",
        slot, result.address, retryCount, maxSameNodeRetryCount, result.successIds.size(), result.failedIds.size());
    newFutures.emplace_back(
        slot, MigrateToTargetNode(std::vector<std::string>{ result.failedIds.begin(), result.failedIds.end() },
                                  sameHost, result.strategy, { .isRetry = true, .slotId = slot }));
    return true;
}

void DataMigrator::TrySubmitRedirectRetryForL2Slot(uint32_t slot, MigrateDataHandler::MigrateResult &result,
                                                   SlotRetryCounters &sameNodeRetryCounts,
                                                   std::vector<SlotMigrateFuture> &newFutures)
{
    sameNodeRetryCounts[{ slot, result.address }] = 0;
    HostPort hostPort;
    Status rc = SelectRedirectTarget(result.address, 0, result.strategy, hostPort);
    if (rc.IsError()) {
        LOG(ERROR) << "[MigrateL2Cache] No admitted redirect target: " << rc;
        failedKeys_.insert(result.failedIds.begin(), result.failedIds.end());
        return;
    }

    LOG(INFO) << FormatString("[MigrateL2Cache] Slot %u retry with new node: %s", slot, hostPort.ToString());
    newFutures.emplace_back(
        slot, MigrateToTargetNode(std::vector<std::string>{ result.failedIds.begin(), result.failedIds.end() },
                                  hostPort, result.strategy, { .isRetry = false, .slotId = slot }));
}

Status DataMigrator::ProcessL2CacheSlotFutures(std::vector<SlotMigrateFuture> &futures, int maxSameNodeRetryCount,
                                               SlotRetryCounters &sameNodeRetryCounts)
{
    while (!futures.empty()) {
        std::vector<SlotMigrateFuture> newFutures;
        bool retryWaitCompleted = false;
        for (auto &fut : futures) {
            Status rc = HandleFailedResult();
            if (rc.IsError()) {
                LOG(ERROR) << "[Migrate Data]. Detail: " << rc.ToString();
                return rc;
            }
            uint32_t slot = fut.first;
            auto result = fut.second.get();
            ObserveUbHealthSummary(result);
            if (result.failedIds.empty()) {
                LOG(INFO) << MigrateDataHandler::ResultToString(result);
                continue;
            }

            LOG_FIRST_AND_EVERY_N(WARNING, MIGRATION_RETRY_LOG_EVERY_N)
                << MigrateDataHandler::ResultToString(result) << ", source=" << localAddress_.ToString()
                << ", task_id=" << taskId_ << ", slot=" << slot;
            VLOG(1) << FormatString(
                "[MigrateL2Cache] Slot %u migration to %s failed, status: %s, failed count: %zu", slot, result.address,
                result.status.ToString(), result.failedIds.size());
            bool localOperator = false;
            const bool structuredFailure = LearnStructuredUbFailure(result, localOperator);
            if ((structuredFailure && localOperator) || IsLocalMigrationOperatorUnavailable()) {
                failedKeys_.insert(result.failedIds.begin(), result.failedIds.end());
                return result.status.IsError() ? result.status
                                               : Status(K_URMA_ERROR, "Local migration UB operator is unavailable");
            }
            if (structuredFailure) {
                if (!retryWaitCompleted) {
                    RETURN_IF_NOT_OK(WaitBeforeRetry());
                    retryWaitCompleted = true;
                }
                TrySubmitRedirectRetryForL2Slot(slot, result, sameNodeRetryCounts, newFutures);
                continue;
            }
            if (!retryWaitCompleted) {
                RETURN_IF_NOT_OK(WaitBeforeRetry());
                retryWaitCompleted = true;
            }
            if (TrySubmitSameNodeRetryForL2Slot(slot, result, maxSameNodeRetryCount, sameNodeRetryCounts, newFutures)) {
                continue;
            }
            TrySubmitRedirectRetryForL2Slot(slot, result, sameNodeRetryCounts, newFutures);
        }
        futures.swap(newFutures);
    }
    return Status::OK();
}

std::future<MigrateDataHandler::MigrateResult> DataMigrator::ConstructFailedFuture(
    const std::string &workerAddr, const Status &status, const std::vector<std::string> &objectKeys,
    std::shared_ptr<SelectionStrategy> &strategy)
{
    MigrateDataHandler::MigrateResult result;
    result.address = workerAddr;
    result.status = status;
    result.failedIds.insert(objectKeys.begin(), objectKeys.end());
    result.strategy = std::move(strategy);
    std::promise<MigrateDataHandler::MigrateResult> p;
    p.set_value(result);
    return p.get_future();
}

MigrateDataHandler::MigrateResult DataMigrator::MigrateDataByNodeImpl(
    const std::shared_ptr<WorkerRemoteWorkerOCApi> &remoteWorkerStub, const std::vector<std::string> &objectKeys,
    const std::shared_ptr<SelectionStrategy> &strategy, bool isSlotMigration, uint32_t slotId)
{
    std::vector<ImmutableString> needMigrateDataIds{ objectKeys.begin(), objectKeys.end() };
    MigrateDataHandler handler(type_, localAddress_.ToString(), needMigrateDataIds, objectTable_, remoteWorkerStub,
                               strategy, &stopping_, progress_, false, slotId);
    auto admissionRc = ConfigureSendAdmission(handler, remoteWorkerStub->Address());
    if (admissionRc.IsError()) {
        MigrateDataHandler::MigrateResult result;
        result.address = remoteWorkerStub->Address();
        result.status = admissionRc;
        result.failedIds.insert(objectKeys.begin(), objectKeys.end());
        result.strategy = strategy;
        return result;
    }
    return handler.MigrateDataToRemote(isSlotMigration);
}

std::future<MigrateDataHandler::MigrateResult> DataMigrator::MigrateDataByNode(
    const HostPort &addr, const std::vector<std::string> &objectKeys, std::shared_ptr<SelectionStrategy> strategy)
{
    std::shared_ptr<WorkerRemoteWorkerOCApi> remoteWorkerStub;
    Status rc = ConnectAndCreateRemoteApi(remoteWorkerStub, addr);
    auto traceID = Trace::Instance().GetTraceID();
    if (traceID.empty()) {
        traceID = "migr;" + GetStringUuid();
    }
    return rc.IsOk() ? threadPool_->Submit([this, remoteWorkerStub, objectKeys, traceID, strategy]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        return MigrateDataByNodeImpl(remoteWorkerStub, objectKeys, strategy);
    })
                     : ConstructFailedFuture(addr.ToString().empty() ? localAddress_.ToString() : addr.ToString(), rc,
                                             objectKeys, strategy);
}

Status DataMigrator::ConnectAndCreateRemoteApi(std::shared_ptr<WorkerRemoteWorkerOCApi> &remoteWorkerStub,
                                               const HostPort &workerAddr)
{
    RETURN_IF_NOT_OK(CheckSourceAdmission());
    if (workerAddr == localAddress_) {
        return Status(StatusCode::K_NOT_FOUND, __LINE__, __FILE__,
                      FormatString("[Migrate Data] The node [%s] to be migrated is the current node [%s]",
                                   workerAddr.ToString(), localAddress_.ToString()));
    }

    RETURN_IF_NOT_OK(CheckTargetAdmission(workerAddr, DataPlaneAdmissionRole::NEW_MIGRATION_TARGET));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateRemoteWorkerApi(workerAddr.ToString(), localAddress_, akSkManager_,
                                                           remoteWorkerStub),
                                     "[Migrate Data] Create remote worker api failed.");
    return Status::OK();
}

Status DataMigrator::HandleFailedResult()
{
    if (type_ == MigrateType::SCALE_DOWN) {
        RETURN_OK_IF_TRUE(taskId_.empty());
    } else if (type_ == MigrateType::SPILL || type_ == MigrateType::REBALANCE_KEEP_LOCAL) {
        if (exitRequested_ != nullptr && exitRequested_->load(std::memory_order_relaxed)) {
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Local node is exiting, no need to execute migrate task"));
        }
    }
    return Status::OK();
}

Status DataMigrator::HandleMigrateDataResult(const std::unordered_map<std::string, uint64_t> &objectSizes,
                                             std::vector<std::future<MigrateDataHandler::MigrateResult>> &futures,
                                             std::vector<std::future<MigrateDataHandler::MigrateResult>> &newFutures)
{
    bool retryWaitCompleted = false;
    for (auto &fut : futures) {
        auto result = fut.get();
        ObserveUbHealthSummary(result);
        if (result.failedIds.empty()) {
            LOG(INFO) << MigrateDataHandler::ResultToString(result);
            continue;
        }
        LOG_FIRST_AND_EVERY_N(WARNING, MIGRATION_RETRY_LOG_EVERY_N)
            << MigrateDataHandler::ResultToString(result)
            << ", source=" << localAddress_.ToString()
            << ", task_id=" << taskId_;
        bool localOperator = false;
        if ((LearnStructuredUbFailure(result, localOperator) && localOperator)
            || IsLocalMigrationOperatorUnavailable()) {
            INJECT_POINT_NO_RETURN("DataMigrator.LocalOperatorUnavailable");
            failedKeys_.merge(std::move(result.failedIds));
            return result.status.IsError() ? result.status
                                           : Status(K_URMA_ERROR, "Local migration UB operator is unavailable");
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(HandleFailedResult(), "[Migrate Data]");
        skippedKeys_.merge(std::move(result.skipIds));
        result.retryCount++;
        if (maxRetryCount_ >= 0 && result.retryCount > maxRetryCount_) {
            LOG(ERROR) << "[Migrate Data] Migration failed after " << maxRetryCount_ << " retries";
            failedKeys_.merge(std::move(result.failedIds));
            continue;
        }
        if (!retryWaitCompleted) {
            auto retryStatus = WaitBeforeRetry();
            if (retryStatus.IsError()) {
                failedKeys_.merge(std::move(result.failedIds));
                return retryStatus;
            }
            retryWaitCompleted = true;
        }
        RedirectMigrateData(result, CalculateTotalSize(result.failedIds, objectSizes), newFutures);
    }
    return Status::OK();
}

Status DataMigrator::WaitBeforeRetry() const
{
    const bool bounded = deadline_ != std::chrono::steady_clock::time_point::max() || cancellation_ != nullptr;
    if (!bounded) {
        return Status::OK();
    }
    if (cancellation_ != nullptr && cancellation_->IsCancelled()) {
        RETURN_STATUS(K_NOT_READY, "topology data migration cancelled");
    }
    const auto now = std::chrono::steady_clock::now();
    CHECK_FAIL_RETURN_STATUS(now < deadline_, K_RPC_DEADLINE_EXCEEDED, "topology data migration deadline exceeded");
    const auto wake = std::min(deadline_, now + TOPOLOGY_MIGRATION_RETRY_INTERVAL);
    if (cancellation_ != nullptr) {
        (void)cancellation_->WaitUntil(wake);
    } else {
        std::this_thread::sleep_until(wake);
    }
    CHECK_FAIL_RETURN_STATUS(cancellation_ == nullptr || !cancellation_->IsCancelled(), K_NOT_READY,
                             "topology data migration cancelled");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline_, K_RPC_DEADLINE_EXCEEDED,
                             "topology data migration deadline exceeded");
    return Status::OK();
}

void DataMigrator::RedirectMigrateData(MigrateDataHandler::MigrateResult &result, uint64_t totalSize,
                                       std::vector<std::future<MigrateDataHandler::MigrateResult>> &newFutures)
{
    INJECT_POINT_NO_RETURN("DataMigrator.RedirectMigrationSubmitted");
    const auto &originAddr = result.address;
    const auto &needRetryIds = result.failedIds;
    auto &strategy = result.strategy;
    std::vector<std::string> objectKeys{ needRetryIds.begin(), needRetryIds.end() };
    if (RedirectByMigrateTargets(originAddr, totalSize, objectKeys, result.status, newFutures)) {
        return;
    }
    HostPort hostPort;
    auto rc = SelectRedirectTarget(originAddr, totalSize, strategy, hostPort);
    if (rc.IsError()) {
        newFutures.emplace_back(ConstructFailedFuture(hostPort.ToString(), rc, objectKeys, strategy));
        return;
    }
    newFutures.emplace_back(MigrateDataByNode(hostPort, objectKeys, strategy));
}

bool DataMigrator::RedirectByMigrateTargets(const std::string &originAddr, uint64_t totalSize,
                                            const std::vector<std::string> &objectKeys, const Status &lastFailure,
                                            std::vector<std::future<MigrateDataHandler::MigrateResult>> &newFutures)
{
    if (type_ != MigrateType::SCALE_DOWN || objectKeys.empty()) {
        return false;
    }
    auto grouped = metadataRoute_.GroupMigrateTargets(objectKeys);
    if (grouped.groups.empty()) {
        return false;
    }
    std::string standbyWorker;
    if (grouped.groups.count(localAddress_) > 0 && GetStandbyWorker(standbyWorker).IsError()) {
        return false;
    }
    std::vector<std::string> escapeKeys;
    for (const auto &[addr, keys] : grouped.groups) {
        DispatchPinnedRetryGroup(originAddr, lastFailure, standbyWorker, addr, keys, escapeKeys, newFutures);
    }
    std::vector<std::string> fallbackKeys;
    fallbackKeys.reserve(escapeKeys.size() + grouped.failures.size());
    fallbackKeys.insert(fallbackKeys.end(), escapeKeys.begin(), escapeKeys.end());
    for (const auto &failure : grouped.failures) {
        fallbackKeys.emplace_back(failure.first);
    }
    DispatchEscapeKeys(originAddr, totalSize, fallbackKeys, newFutures);
    return true;
}

void DataMigrator::DispatchPinnedRetryGroup(const std::string &originAddr, const Status &lastFailure,
                                            const std::string &standbyWorker, const HostPort &addr,
                                            const std::vector<std::string> &keys,
                                            std::vector<std::string> &escapeKeys,
                                            std::vector<std::future<MigrateDataHandler::MigrateResult>> &newFutures)
{
    HostPort target = addr;
    if (target == localAddress_) {
        (void)target.ParseString(standbyWorker);
    }
    // An unacceptable target (leaving, failed, or transport-isolated) can never accept this batch, so its keys
    // escape to the address-order chain instead of being pinned to the group's committed owner round after round.
    if (CheckTargetAdmission(target, DataPlaneAdmissionRole::NEW_MIGRATION_TARGET).IsError()) {
        escapeKeys.insert(escapeKeys.end(), keys.begin(), keys.end());
        (void)retryStrategies_.erase(target.ToString());
        (void)targetConsecutiveFailures_.erase(target.ToString());
        return;
    }
    // The selector persists per target across retry rounds; every revisit of the same failed target escalates
    // its stage, so a takeover target that reports low space cannot stall the drain forever.
    auto &targetStrategy = retryStrategies_[target.ToString()];
    if (!targetStrategy) {
        targetStrategy = GetStrategyByType();
    }
    targetStrategy->UpdateForRedirect(originAddr);
    // A probe that passes the resource gate but keeps failing to send never clears the stage ladder; the
    // per-target budget bounds such rounds and hands the keys to the address-order chain once exhausted.
    const int failedRounds = ++targetConsecutiveFailures_[target.ToString()];
    const bool exhaustedBudget = failedRounds > K_MAX_PINNED_RETRY_ROUNDS;
    if (target.ToString() == originAddr || exhaustedBudget) {
        auto selector = std::dynamic_pointer_cast<ScaleDownNodeSelector>(targetStrategy);
        LOG(WARNING) << FormatString(
            "[Migrate Data] Redirect retry re-dispatches %zu keys to %s, memory stage=%d, pinned rounds=%d, "
            "last failure: %s",
            keys.size(), target.ToString().c_str(),
            selector != nullptr ? static_cast<int>(selector->CurrentStage()) : -1, failedRounds,
            lastFailure.ToString().c_str());
    }
    if (exhaustedBudget) {
        targetConsecutiveFailures_[target.ToString()] = 0;
        escapeKeys.insert(escapeKeys.end(), keys.begin(), keys.end());
        return;
    }
    newFutures.emplace_back(MigrateDataByNode(target, keys, targetStrategy));
}

void DataMigrator::DispatchEscapeKeys(const std::string &originAddr, uint64_t totalSize,
                                      const std::vector<std::string> &fallbackKeys,
                                      std::vector<std::future<MigrateDataHandler::MigrateResult>> &newFutures)
{
    if (fallbackKeys.empty()) {
        return;
    }
    // A fresh selector keeps this one-shot address-order escape from inheriting the pinned selector's stage.
    auto fallbackStrategy = GetStrategyByType();
    HostPort hostPort;
    auto rc = SelectRedirectTarget(originAddr, totalSize, fallbackStrategy, hostPort);
    if (rc.IsError()) {
        newFutures.emplace_back(ConstructFailedFuture(hostPort.ToString(), rc, fallbackKeys, fallbackStrategy));
        return;
    }
    newFutures.emplace_back(MigrateDataByNode(hostPort, fallbackKeys, fallbackStrategy));
}

Status DataMigrator::SelectRedirectTarget(const std::string &originAddr, uint64_t totalSize,
                                          std::shared_ptr<SelectionStrategy> &strategy, HostPort &target) const
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(membership_.GetSnapshot(snapshot));
    std::string selectionOrigin = originAddr;
    Status lastRc(K_NOT_FOUND, "No admitted migration redirect target");
    strategy->UpdateForRedirect(originAddr);
    for (size_t attempt = 0; attempt < snapshot->Members().size(); ++attempt) {
        std::string nextWorker;
        lastRc = strategy->SelectNode(selectionOrigin, "", totalSize, nextWorker);
        if (lastRc.IsError()) {
            return lastRc;
        }
        RETURN_IF_NOT_OK(target.ParseString(nextWorker));
        lastRc = CheckTargetAdmission(target, DataPlaneAdmissionRole::REDIRECT_TARGET);
        if (lastRc.IsOk()) {
            return Status::OK();
        }
        LOG(WARNING) << "Skip unavailable migration redirect target " << nextWorker << ": " << lastRc;
        INJECT_POINT_NO_RETURN("DataMigrator.SelectRedirectTarget.skipped");
        strategy->UpdateForRedirect(nextWorker);
        selectionOrigin = std::move(nextWorker);
    }
    return lastRc;
}

void DataMigrator::ObserveUbHealthSummary(const MigrateDataHandler::MigrateResult &result) const
{
    if (!result.ubHealthSummary.has_value() || !ubHealthSummaryObserver_) {
        return;
    }
    try {
        ubHealthSummaryObserver_(*result.ubHealthSummary);
    } catch (const std::exception &error) {
        LOG(ERROR) << "Migration UB health summary observer threw: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Migration UB health summary observer threw";
    }
}

bool DataMigrator::LearnStructuredUbFailure(const MigrateDataHandler::MigrateResult &result, bool &localOperator)
{
    localOperator = false;
    if (ubAdmission_ == nullptr || !result.ubFailureDetail.has_value()) {
        return false;
    }
    HostPort operatorWorker;
    if (operatorWorker.ParseString(result.ubFailureDetail->operator_worker()).IsError()) {
        return false;
    }
    auto outcome = DecodeProviderUbFailureDetail(*result.ubFailureDetail, operatorWorker,
                                                 UbOperationKind::MIGRATION_WRITE, "migration_response");
    if (!outcome.has_value()) {
        return false;
    }
    ubAdmission_->ReportOutcome(*outcome);
    localOperator = outcome->peer == localAddress_;
    if (!localOperator) {
        INJECT_POINT_NO_RETURN("DataMigrator.RemoteOperatorUnavailable");
    }
    return true;
}

bool DataMigrator::IsLocalMigrationOperatorUnavailable() const
{
    const bool canUseTcpFallback = FLAGS_enable_transport_fallback && FLAGS_data_migrate_urma_transport_mode == "write";
    if (!IsUrmaEnabled() || ubAdmission_ == nullptr || canUseTcpFallback) {
        return false;
    }
    return ubAdmission_->CheckWriteTarget(localAddress_, UbOperationKind::MIGRATION_WRITE).IsError();
}

Status DataMigrator::ConfigureSendAdmission(MigrateDataHandler &handler, const std::string &targetAddress)
{
    HostPort target;
    RETURN_IF_NOT_OK(target.ParseString(targetAddress));
    handler.SetSendAdmission([this, target] {
        RETURN_IF_NOT_OK(CheckSourceAdmission());
        return CheckTargetAdmission(target, DataPlaneAdmissionRole::NEW_MIGRATION_TARGET);
    });
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
