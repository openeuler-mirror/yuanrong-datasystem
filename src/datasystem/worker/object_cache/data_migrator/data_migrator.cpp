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

#include "datasystem/worker/object_cache/data_migrator/strategy/scale_down_node_selector.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/spill_node_selector.h"

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
    const std::shared_ptr<SelectionStrategy> &strategy)
{
    std::vector<ImmutableString> needMigrateDataIds{ objectKeys.begin(), objectKeys.end() };
    MigrateDataHandler handler(type_, localAddress_.ToString(), needMigrateDataIds, objectTable_, remoteWorkerStub,
                               strategy, progress_);
    return handler.MigrateDataToRemote();
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