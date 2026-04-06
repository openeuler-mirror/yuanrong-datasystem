/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Migrate data.
 */
#include "datasystem/worker/object_cache/data_migrator/data_migrator.h"

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/scale_down_node_selector.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/spill_node_selector.h"

DS_DECLARE_uint32(distributed_disk_slot_num);

namespace datasystem {
namespace object_cache {

void DataMigrator::Init()
{
    const uint32_t threadPoolSize = 4;
    threadPool_ = std::make_unique<ThreadPool>(0, threadPoolSize, "OcMigrateData");
}

std::shared_ptr<SelectionStrategy> DataMigrator::GetStrategyByType()
{
    switch (type_) {
        case MigrateType::SPILL:
            return std::make_shared<SpillNodeSelector>(etcdCM_, localAddress_);
        case MigrateType::SCALE_DOWN:
        default:
            return std::make_shared<ScaleDownNodeSelector>(etcdCM_, localAddress_);
    }
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
    progress_ = CreateMigrateProgress(objectKeys.size());
    failedKeys_.clear();
    skippedKeys_.clear();
    LOG(INFO) << FormatString(
        "[Migrate Data] Processing data migrate begin, migrate type: %d, object size: %zu, task id: %s",
        static_cast<int>(type_), objectKeys.size(), taskId_);

    PerfPoint point(PerfKey::WORKER_MIGRATE_TASK_SUBMIT);
    std::vector<std::future<MigrateDataHandler::MigrateResult>> futures;
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objectKeys);
    INJECT_POINT("DataMigrator.GetMasterAddr", [&objKeysGrpByMaster, &objectKeys]() {
        objKeysGrpByMaster.clear();
        MetaAddrInfo info;
        (void)objKeysGrpByMaster.emplace(info, objectKeys);
        return Status::OK();
    });
    std::string standbyWorker;
    (void)etcdCM_->GetStandbyWorkerByAddr(localAddress_.ToString(), standbyWorker);
    for (const auto &[addr, objectKeys] : objKeysGrpByMaster) {
        auto workerAddr = addr.GetAddress();
        if (workerAddr == localAddress_ && !standbyWorker.empty()) {
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
    return threadPool_->Submit([this, remoteWorkerStub, objectKeys, traceID, strategy]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        return MigrateDataByNodeImpl(remoteWorkerStub, objectKeys, strategy, true);
    });
}

std::future<MigrateDataHandler::MigrateResult> DataMigrator::MigrateToTargetNode(
    const std::vector<std::string> &objectKeys, const HostPort &targetAddr, std::shared_ptr<SelectionStrategy> strategy,
    bool isRetry, uint32_t slotId)
{
    if (!strategy) {
        strategy = GetStrategyByType();
    }

    auto traceID = Trace::Instance().GetTraceID();
    return threadPool_->Submit([this, objectKeys, targetAddr, traceID, strategy, isRetry, slotId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);

        MigrateDataHandler::MigrateResult finalResult;
        finalResult.address = targetAddr.ToString();
        finalResult.strategy = strategy;

        std::shared_ptr<WorkerRemoteWorkerOCApi> remoteWorkerStub;
        Status rc = ConnectAndCreateRemoteApi(remoteWorkerStub, targetAddr);
        if (rc.IsError()) {
            LOG(ERROR) << "connect to remote worker " << finalResult.address << "failed: failed rc:" << rc.ToString();
            finalResult.status = rc;
            finalResult.failedIds.insert(objectKeys.begin(), objectKeys.end());
            return finalResult;
        }

        std::vector<ImmutableString> needMigrateDataIds{ objectKeys.begin(), objectKeys.end() };
        MigrateDataHandler handler(type_, localAddress_.ToString(), needMigrateDataIds, objectTable_, remoteWorkerStub,
                                   strategy, nullptr, isRetry, slotId);
        auto result = handler.MigrateDataToRemote(true);
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
    (void)etcdCM_->GetStandbyWorkerByAddr(localAddress_.ToString(), standbyWorker);

    std::vector<SlotMigrateFuture> futures;
    std::unordered_map<uint32_t, int> sameNodeRetryCounts;
    SubmitL2CacheTasksBySlot(objectsBySlot, standbyWorker, futures, sameNodeRetryCounts);
    ProcessL2CacheSlotFutures(futures, maxSameNodeRetryCount, sameNodeRetryCounts);

    LOG(INFO) << FormatString("[MigrateL2Cache] Finished");

    return Status::OK();
}

std::map<uint32_t, std::vector<std::string>> DataMigrator::GroupL2CacheObjectsBySlot(
    const std::vector<std::string> &objectKeys) const
{
    auto slotNum = FLAGS_distributed_disk_slot_num;
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
                                            const std::string &standbyWorker, std::vector<SlotMigrateFuture> &futures,
                                            std::unordered_map<uint32_t, int> &sameNodeRetryCounts)
{
    for (const auto &[slot, objs] : objectsBySlot) {
        HostPort currentTarget;
        auto status = etcdCM_->GetMasterAddr(objs[0], currentTarget);
        if (status.IsError() || currentTarget == localAddress_) {
            status = currentTarget.ParseString(standbyWorker);
            if (status.IsError()) {
                LOG(ERROR) << "get target worker addr failed, status:" << status.ToString();
                continue;
            }
        }

        auto strategy = std::make_shared<ScaleDownNodeSelector>(etcdCM_, localAddress_);
        LOG(INFO) << FormatString("[MigrateL2Cache] Slot %u (%zu objects) -> %s", slot, objs.size(),
                                  currentTarget.ToString());
        futures.emplace_back(slot, MigrateToTargetNode(objs, currentTarget, strategy, false, slot));
        sameNodeRetryCounts[slot] = 0;
    }
}

bool DataMigrator::TrySubmitSameNodeRetryForL2Slot(uint32_t slot, const MigrateDataHandler::MigrateResult &result,
                                                   int maxSameNodeRetryCount,
                                                   std::unordered_map<uint32_t, int> &sameNodeRetryCounts,
                                                   std::vector<SlotMigrateFuture> &newFutures)
{
    const bool enteredSameNodeRetry = sameNodeRetryCounts[slot] > 0;
    if (result.successIds.empty() && !enteredSameNodeRetry) {
        return false;
    }

    int retryCount = ++sameNodeRetryCounts[slot];
    if (retryCount > maxSameNodeRetryCount) {
        LOG(WARNING) << FormatString("[MigrateL2Cache] Slot %u same-node failedIds retry exceeded max(%d), stop "
                                     "retry on node %s",
                                     slot, maxSameNodeRetryCount, result.address);
        return true;
    }

    HostPort sameHost;
    Status rc = sameHost.ParseString(result.address);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[MigrateL2Cache] Parse node address failed for same-node retry: %s, status: %s",
                                   result.address, rc.ToString());
        return true;
    }

    LOG(WARNING) << FormatString(
        "[MigrateL2Cache] Slot %u retry failed ids on same node %s (%d/%d), success count: %zu, failed count: %zu",
        slot, result.address, retryCount, maxSameNodeRetryCount, result.successIds.size(), result.failedIds.size());
    newFutures.emplace_back(
        slot, MigrateToTargetNode(std::vector<std::string>{ result.failedIds.begin(), result.failedIds.end() },
                                  sameHost, result.strategy, true, slot));
    return true;
}

void DataMigrator::TrySubmitRedirectRetryForL2Slot(uint32_t slot, MigrateDataHandler::MigrateResult &result,
                                                   std::unordered_map<uint32_t, int> &sameNodeRetryCounts,
                                                   std::vector<SlotMigrateFuture> &newFutures)
{
    sameNodeRetryCounts[slot] = 0;
    result.strategy->UpdateForRedirect(result.address);
    std::string nextTarget;
    Status rc = result.strategy->SelectNode(result.address, "", 0, nextTarget);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[MigrateL2Cache] No more available node");
        return;
    }

    HostPort hostPort;
    rc = hostPort.ParseString(nextTarget);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[MigrateL2Cache] Parse next target failed: %s, status: %s", nextTarget,
                                   rc.ToString());
        failedKeys_.insert(result.failedIds.begin(), result.failedIds.end());
        return;
    }

    LOG(INFO) << FormatString("[MigrateL2Cache] Slot %u retry with new node: %s", slot, nextTarget);
    newFutures.emplace_back(
        slot, MigrateToTargetNode(std::vector<std::string>{ result.failedIds.begin(), result.failedIds.end() },
                                  hostPort, result.strategy, false, slot));
}

void DataMigrator::ProcessL2CacheSlotFutures(std::vector<SlotMigrateFuture> &futures, int maxSameNodeRetryCount,
                                             std::unordered_map<uint32_t, int> &sameNodeRetryCounts)
{
    while (!futures.empty()) {
        std::vector<SlotMigrateFuture> newFutures;
        for (auto &fut : futures) {
            Status rc = HandleFailedResult();
            if (rc.IsError()) {
                LOG(ERROR) << "[Migrate Data]. Detail: " << rc.ToString();
                return;
            }
            uint32_t slot = fut.first;
            auto result = fut.second.get();
            LOG(INFO) << MigrateDataHandler::ResultToString(result);
            if (result.failedIds.empty()) {
                continue;
            }

            LOG(WARNING) << FormatString(
                "[MigrateL2Cache] Slot %u migration to %s failed, status: %s, failed count: %zu", slot, result.address,
                result.status.ToString(), result.failedIds.size());
            if (TrySubmitSameNodeRetryForL2Slot(slot, result, maxSameNodeRetryCount, sameNodeRetryCounts, newFutures)) {
                continue;
            }
            TrySubmitRedirectRetryForL2Slot(slot, result, sameNodeRetryCounts, newFutures);
        }
        futures.swap(newFutures);
    }
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
                               strategy, progress_, false, slotId);
    return handler.MigrateDataToRemote(isSlotMigration);
}

std::future<MigrateDataHandler::MigrateResult> DataMigrator::MigrateDataByNode(
    const HostPort &addr, const std::vector<std::string> &objectKeys, std::shared_ptr<SelectionStrategy> strategy)
{
    std::shared_ptr<WorkerRemoteWorkerOCApi> remoteWorkerStub;
    Status rc = ConnectAndCreateRemoteApi(remoteWorkerStub, addr);
    auto traceID = Trace::Instance().GetTraceID();
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
    if (workerAddr == localAddress_) {
        return Status(StatusCode::K_NOT_FOUND, __LINE__, __FILE__,
                      FormatString("[Migrate Data] The node [%s] to be migrated is the current node [%s]",
                                   workerAddr.ToString(), localAddress_.ToString()));
    }

    RETURN_IF_NOT_OK(etcdCM_->CheckConnection(workerAddr, true));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateRemoteWorkerApi(workerAddr.ToString(), akSkManager_, remoteWorkerStub),
                                     "[Migrate Data] Create remote worker api failed.");
    return Status::OK();
}

Status DataMigrator::HandleFailedResult()
{
    if (type_ == MigrateType::SCALE_DOWN) {
        RETURN_OK_IF_TRUE(taskId_.empty());
        if (etcdCM_->CheckVoluntaryTaskExpired(taskId_)) {
            RETURN_STATUS(
                K_RUNTIME_ERROR,
                FormatString(
                    "task id has expired, no need to excute voluntary scale down migrate data task, task id: %s",
                    taskId_));
        }
        if (etcdCM_->CheckVoluntaryScaleDown()) {
            RETURN_STATUS(
                K_RUNTIME_ERROR,
                FormatString("this node maybe failed or only one node left, no need to excute voluntary scale down "
                             "migrate data task, task id: %s",
                             taskId_));
        }
    } else if (type_ == MigrateType::SPILL) {
        if (etcdCM_->CheckLocalNodeIsExiting()) {
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Local node is exiting, no need to execute migrate task"));
        }
    }
    return Status::OK();
}

Status DataMigrator::HandleMigrateDataResult(const std::unordered_map<std::string, uint64_t> &objectSizes,
                                             std::vector<std::future<MigrateDataHandler::MigrateResult>> &futures,
                                             std::vector<std::future<MigrateDataHandler::MigrateResult>> &newFutures)
{
    for (auto &fut : futures) {
        auto result = fut.get();
        LOG(INFO) << MigrateDataHandler::ResultToString(result);
        if (!result.failedIds.empty()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(HandleFailedResult(), "[Migrate Data]");
            skippedKeys_.merge(std::move(result.skipIds));
            result.retryCount++;
            if (maxRetryCount_ >= 0 && result.retryCount > maxRetryCount_) {
                LOG(ERROR) << "[Migrate Data] Migration failed after " << maxRetryCount_ << " retries";
                failedKeys_.merge(std::move(result.failedIds));
                continue;
            }
            newFutures.emplace_back(RedirectMigrateData(result, CalculateTotalSize(result.failedIds, objectSizes)));
        }
    }
    return Status::OK();
}

std::future<MigrateDataHandler::MigrateResult> DataMigrator::RedirectMigrateData(
    MigrateDataHandler::MigrateResult &result, uint64_t totalSize)
{
    const auto &originAddr = result.address;
    const auto &needRetryIds = result.failedIds;
    auto &strategy = result.strategy;
    std::vector<std::string> objectKeys{ needRetryIds.begin(), needRetryIds.end() };
    std::string nextWorker;

    strategy->UpdateForRedirect(originAddr);

    auto rc = strategy->SelectNode(originAddr, "", totalSize, nextWorker);
    if (rc.IsError()) {
        return ConstructFailedFuture(nextWorker, rc, objectKeys, strategy);
    }

    HostPort hostPort;
    rc = hostPort.ParseString(nextWorker);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[Migrate Data] Failed to parse worker address [%s]: %s", originAddr, rc.ToString());
        return ConstructFailedFuture(nextWorker, rc, objectKeys, strategy);
    }
    return MigrateDataByNode(hostPort, objectKeys, strategy);
}

}  // namespace object_cache
}  // namespace datasystem
