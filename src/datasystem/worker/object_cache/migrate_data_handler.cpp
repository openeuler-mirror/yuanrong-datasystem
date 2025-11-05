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
 * Description: Threead unsafe migrate data implemetation.
 */

#include "datasystem/worker/object_cache/migrate_data_handler.h"

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <sstream>
#include <vector>
#include <random>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"

DS_DECLARE_uint32(data_migrate_rate_limit_mb);
DS_DEFINE_validator(data_migrate_rate_limit_mb, [](const char *flagName, uint32_t value) {
    (void)flagName;
    return value > 0;
});

namespace datasystem {
namespace object_cache {
int ms2us = 1'000ul;
uint s2ms = 1'000ul;

static inline std::time_t Now()
{
    return GetSteadyClockTimeStampUs() / ms2us;
}

MigrateDataLimiter::MigrateDataLimiter(uint64_t rate) : rate_(rate), tokens_(rate)
{
    timestamp_ = Now();
}

void MigrateDataLimiter::WaitAllow(uint64_t requiredSize)
{
    std::unique_lock<std::mutex> l(mtx_);
    while (tokens_ < requiredSize) {
        Refill();
        if (tokens_ < requiredSize) {
            cond_.wait_for(l, std::chrono::milliseconds(WaitMilliseconds(requiredSize)));
        } else {
            break;
        }
    }
    tokens_ -= requiredSize;
}

void MigrateDataLimiter::Refill()
{
    auto now = Now();
    uint64_t elapsed = now - timestamp_;
    uint64_t newTokens;
    if (rate_ <= UINT64_MAX / (elapsed == 0 ? 1 : elapsed)) {
        newTokens = rate_ * elapsed;
    } else {
        newTokens = UINT64_MAX;
    }
    newTokens = newTokens / s2ms + 1;
    tokens_ = newTokens + tokens_ > tokens_ ? newTokens + tokens_ : UINT64_MAX;
    timestamp_ = now;
}

void MigrateDataLimiter::UpdateRate(uint64_t rate)
{
    std::unique_lock<std::mutex> l(mtx_);
    rate_ = rate;
}

std::time_t MigrateDataLimiter::WaitMilliseconds(uint64_t requiredSize)
{
    if (requiredSize <= tokens_) {
        return 0;
    }
    return (requiredSize - tokens_) * s2ms / rate_ + 1;
}

bool MigrateDataLimiter::IsRemoteBusyNode() const
{
    std::unique_lock<std::mutex> l(mtx_);
    return rate_ == 0;
}

void MigrateStrategy::CheckAndUpgradeStage(const std::string &currentWorker)
{
    if (visitedAddresses_.find(currentWorker) != visitedAddresses_.end()) {
        IncrementStage(currentStage_);
        visitedAddresses_.clear();
        visitedAddresses_.insert(currentWorker);
    } else {
        visitedAddresses_.insert(currentWorker);
    }

    if (visitedAddressesForDisk_.find(currentWorker) != visitedAddressesForDisk_.end()) {
        IncrementStage(currentDiskStage_);
        visitedAddressesForDisk_.clear();
        visitedAddressesForDisk_.insert(currentWorker);
    } else {
        visitedAddressesForDisk_.insert(currentWorker);
    }
}

bool MigrateStrategy::CheckCondition(const MigrateDataRspPb &rsp, const CacheType &type)
{
    bool isDataMigrationStarted = rsp.scale_down_state() == MigrateDataRspPb::DATA_MIGRATION_STARTED;
    bool needScaleDown = rsp.scale_down_state() == MigrateDataRspPb::NEED_SCALE_DOWN;
    double availableSpaceRatio = type == CacheType::MEMORY ? rsp.available_ratio() : rsp.disk_available_ratio();

    MigrationStrategyStage &stage = type == CacheType::MEMORY ? currentStage_ : currentDiskStage_;

    INJECT_POINT_NO_RETURN("MigrateStrategy.CheckCondition",
                           [&needScaleDown, &isDataMigrationStarted, &availableSpaceRatio, &stage](
                               int64_t newNeedScaleDown, int64_t newIsDataMigrationStarted,
                               int64_t newAvailableSpaceRatio, int64_t newStage) {
                               needScaleDown = (newNeedScaleDown != 0);
                               isDataMigrationStarted = (newIsDataMigrationStarted != 0);
                               availableSpaceRatio = static_cast<double>(newAvailableSpaceRatio);
                               stage = static_cast<MigrationStrategyStage>(newStage);
                           });
    INJECT_POINT_NO_RETURN("MigrateStrategy.CheckCondition1",
                           [this](int64_t newStage) { currentStage_ = static_cast<MigrationStrategyStage>(newStage); });
    bool res = EvaluateStageCondition(stage, needScaleDown, isDataMigrationStarted, availableSpaceRatio);
    if (res) {
        if (type == CacheType::MEMORY) {
            visitedAddresses_.clear();
        } else {
            visitedAddressesForDisk_.clear();
        }
    }
    return res;
}

bool MigrateStrategy::EvaluateStageCondition(MigrationStrategyStage stage, bool needScaleDown,
                                             bool isDataMigrationStarted, double availableSpaceRatio)
{
    const double firstStageThreshold = 50.0;
    const double secondStageThreshold = 20.0;
    switch (stage) {
        case MigrationStrategyStage::FIRST:
            return !needScaleDown && !isDataMigrationStarted && availableSpaceRatio > firstStageThreshold;
        case MigrationStrategyStage::SECOND:
            return !needScaleDown && !isDataMigrationStarted && availableSpaceRatio > secondStageThreshold;
        case MigrationStrategyStage::THIRD:
            return !needScaleDown && !isDataMigrationStarted;
        case MigrationStrategyStage::FINAL:
            return !isDataMigrationStarted;
        default:
            LOG(WARNING) << FormatString("Unknown migration stage: %d", static_cast<int>(stage));
            return false;
    }
}

void MigrateStrategy::IncrementStage(MigrationStrategyStage &stage)
{
    if (stage < MigrationStrategyStage::FINAL) {
        stage = static_cast<MigrationStrategyStage>(static_cast<int>(stage) + 1);
    }
}

MigrateDataHandler::MigrateDataHandler(const std::string &localAddr,
                                       const std::vector<ImmutableString> &needMigrateDataIds,
                                       std::shared_ptr<ObjectTable> objectTable,
                                       std::shared_ptr<WorkerRemoteWorkerOCApi> remoteApi,
                                       std::shared_ptr<MigrateProgress> progress,
                                       const MigrateStrategy migrateDataStrategy)
    : localAddr_(localAddr),
      needMigrateDataIds_(needMigrateDataIds.begin(), needMigrateDataIds.end()),
      objectTable_(std::move(objectTable)),
      remoteApi_(std::move(remoteApi)),
      maxBatchSize_(FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul),
      currBatchSize_(0),
      currBatchCount_(0),
      limiter_(FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul),
      progress_(std::move(progress)),
      migrateDataStrategy_(migrateDataStrategy)
{
}

void MigrateDataHandler::SplitByCacheType(std::vector<std::string> &memoryDataIds,
                                          std::vector<std::string> &diskDataIds)
{
    for (const auto &objectKey : needMigrateDataIds_) {
        std::shared_ptr<SafeObjType> entry;
        Status rc = objectTable_->Get(objectKey, entry);
        if (rc.IsError() || entry->RLock().IsError()) {
            (void)skipIds_.emplace(objectKey);
            continue;
        }
        if ((*entry)->IsMemoryCache()) {
            memoryDataIds.emplace_back(objectKey);
        } else {
            diskDataIds.emplace_back(objectKey);
        }
        entry->RUnlock();
    }
    LOG(INFO) << FormatString("[Migrate Data] Migrate %ld objects to: %s, memory(%ld) disk(%ld)",
                              needMigrateDataIds_.size(), remoteApi_->Address(), memoryDataIds.size(),
                              diskDataIds.size());
}

MigrateDataHandler::MigrateResult MigrateDataHandler::MigrateDataToRemote()
{
    INJECT_POINT_NO_RETURN("MigrateDataHandler.MigrateDataToRemote.DelayMigrate",
                           [](int sleepMs) { std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs)); });
    std::vector<std::string> memoryDataIds, diskDataIds;
    SplitByCacheType(memoryDataIds, diskDataIds);
    auto migrateFunc = [this](CacheType type, std::vector<std::string> &needMigrateDataIds) {
        if (needMigrateDataIds.empty()) {
            return Status::OK();
        }
        Status s = SpyOnRemoteRaminBytes(type);
        if (s.IsError()) {
            (void)failedIds_.insert(needMigrateDataIds.begin(), needMigrateDataIds.end());
            return s;
        }

        for (auto it = needMigrateDataIds.begin(); it != needMigrateDataIds.end(); ++it) {
            // If remote has no resources, we will abort.
            if (IsRemoteLackResources()) {
                LOG(WARNING) << FormatString("[Migrate Data] Remote node %s has no remain bytes: %ld",
                                             remoteApi_->Address(), maxBatchSize_);
                std::transform(datas_.begin(), datas_.end(), std::inserter(failedIds_, failedIds_.end()),
                               [](const std::unique_ptr<BaseData> &d) { return d->Id(); });
                (void)failedIds_.insert(it, needMigrateDataIds.end());
                return Status(StatusCode::K_NO_SPACE, "[Migrate Data] No remain bytes");
            }

            // We will send data in unlock zone.
            if (IsFull()) {
                SendDataToRemote();
            }
            std::shared_ptr<SafeObjType> entry;
            const auto &objectKey = *it;
            Status rc = objectTable_->Get(objectKey, entry);
            // Object has been deleted, just skip it.
            if (rc.IsError()) {
                (void)skipIds_.emplace(objectKey);
                continue;
            }
            // Object has been deleted, just skip it.
            rc = entry->RLock();
            if (rc.IsError()) {
                (void)skipIds_.emplace(objectKey);
                continue;
            }
            ObjectKV objectKV(objectKey, *entry);
            AddObjectDataLocked(objectKV);
            entry->RUnlock();
        }
        SendDataToRemote();
        return lastRc_;
    };
    lastRc_ = migrateFunc(CacheType::MEMORY, memoryDataIds);
    maxBatchSize_ = FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul;
    lastRc_ = lastRc_.IsError() ? lastRc_ : migrateFunc(CacheType::DISK, diskDataIds);
    return ConstructResult(lastRc_);
}

std::string MigrateDataHandler::ResultToString(const MigrateResult &result)
{
    std::stringstream ss;
    ss << "[Migrate Data] Migrate to remmote node [" << result.address << "] result: [\n"
       << "\treturn code: " << result.status.ToString() << "\n"
       << "\tsuccess objects count: " << result.successIds.size() << "\n"
       << "\tskip objects count: " << result.skipIds.size() << "\n"
       << "\tfail objects count: " << result.failedIds.size() << "\n"
       << "]";
    return ss.str();
}

Status MigrateDataHandler::SpyOnRemoteRaminBytes(CacheType type)
{
    MigrateDataReqPb req;
    MigrateDataRspPb rsp;
    Status s = MigrateDataToRemoteRetry(remoteApi_, req, {}, rsp);

    if (!migrateDataStrategy_.CheckCondition(rsp, type)) {
        RETURN_STATUS(StatusCode::K_NO_SPACE,
                      "[Migrate Data] migrateDataStrategy.CheckCondition failed due to insufficient space");
    }

    if (s.IsOk()) {
        RETURN_IF_NOT_OK(TryUpdateRate(rsp.limit_rate()));
        type == CacheType::MEMORY ? AdjustMaxBatchSize(rsp.remain_bytes())
                                  : AdjustMaxBatchSize(rsp.disk_remain_bytes());
    }

    if (s.IsError()) {
        LOG(WARNING) << FormatString("[Migrate Data] Spy on remote node %s remain bytes but meets error: %s",
                                     remoteApi_->Address(), s.ToString());
        if (s.GetCode() == StatusCode::K_NOT_READY) {
            RETURN_STATUS(StatusCode::K_NOT_READY, "[Migrate Data] Remote node cannot accept data");
        }
    }

    if (IsRemoteLackResources()) {
        LOG(WARNING) << FormatString("[Migrate Data] Remote node %s has no remain bytes: %ld", remoteApi_->Address(),
                                     maxBatchSize_);
        RETURN_STATUS(StatusCode::K_NO_SPACE, "[Migrate Data] No remain bytes");
    }

    LOG(INFO) << FormatString("[Migrate Data] Remote node %s remain bytes: %ld", remoteApi_->Address(), maxBatchSize_);
    return Status::OK();
}

void MigrateDataHandler::AdjustMaxBatchSize(uint64_t size)
{
    maxBatchSize_ = std::min<uint64_t>(maxBatchSize_, size);
}

bool MigrateDataHandler::IsRemoteLackResources() const
{
    constexpr uint64_t minRemianBytes = 1024ul * 1024ul;
    return maxBatchSize_ < minRemianBytes;
}

Status MigrateDataHandler::AddObjectDataLocked(const ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    const auto &entry = objectKV.GetObjEntry();
    std::unique_ptr<BaseData> data_;
    if (entry->IsSpilled() && entry->GetShmUnit() == nullptr) {
        std::vector<RpcMessage> data;
        Status rc = WorkerOcSpill::Instance()->Get(objectKey, data, entry->GetDataSize());
        if (rc.IsOk()) {
            datas_.emplace_back(std::make_unique<PayloadData>(objectKey, entry->GetCreateTime(), std::move(data),
                                                              entry->GetDataSize()));
        } else {
            (void)failedIds_.emplace(objectKey);
            return rc;
        }
    } else {
        datas_.emplace_back(std::make_unique<ShmData>(objectKey, entry->GetCreateTime(), entry->GetShmUnit(),
                                                      entry->GetDataSize(), entry->GetMetadataSize()));
    }
    currBatchSize_ += entry->GetDataSize();
    ++currBatchCount_;
    return Status::OK();
}

void MigrateDataHandler::SendDataToRemote()
{
    if (datas_.empty()) {
        Clear();
        return;
    }

    if (limiter_.IsRemoteBusyNode()) {
        LOG(WARNING) << FormatString("[Migrate Data] Remote node %s is busy", remoteApi_->Address());
        std::transform(datas_.begin(), datas_.end(), std::inserter(failedIds_, failedIds_.end()),
                       [](const std::unique_ptr<BaseData> &d) { return d->Id(); });
        lastRc_ = Status(StatusCode::K_NOT_READY, "[Migrate Data] Remote node is busy");
        Clear();
        return;
    }

    // 1. Construct request.
    MigrateDataReqPb req;
    req.set_worker_addr(localAddr_);
    req.set_bytes_send(currBatchSize_);
    std::vector<MemView> payloads;
    uint32_t currPartIndex = 0;
    for (const auto &data : datas_) {
        // If it is the shm data, we need to lock it first.
        Status s = data->LockData();
        if (s.IsError()) {
            LOG(ERROR) << FormatString("[Migrate Data] Lock object %s failed, it will not be sent!", data->Id());
            lastRc_ = s;
            (void)failedIds_.emplace(data->Id());
            continue;
        }

        auto *objInfo = req.add_objects();
        objInfo->set_object_key(data->Id());
        objInfo->set_version(data->Version());
        objInfo->set_data_size(data->Size());
        auto memViews = data->GetMemViews();
        for (uint32_t i = currPartIndex; i < currPartIndex + memViews.size(); ++i) {
            objInfo->add_part_index(i);
        }
        currPartIndex += memViews.size();
        (void)payloads.insert(payloads.end(), std::make_move_iterator(memViews.begin()),
                              std::make_move_iterator(memViews.end()));
    }

    // 2. Wait for limit ready and migrate data with retry.
    limiter_.WaitAllow(currBatchSize_);
    MigrateDataRspPb rsp;
    Status s = MigrateDataToRemoteRetry(remoteApi_, req, payloads, rsp);
    if (s.IsOk()) {
        AdjustMaxBatchSize(rsp.remain_bytes());
        (void)successIds_.insert(rsp.success_ids().begin(), rsp.success_ids().end());
        (void)failedIds_.insert(rsp.fail_ids().begin(), rsp.fail_ids().end());
        if (progress_ != nullptr) {
            progress_->Deal(rsp.success_ids_size());
        }
        LOG_IF(WARNING, !rsp.fail_ids().empty()) << FormatString(
            "[Migrate Data] Send %ld objects[%ld bytes] to %s and %ld objects [%s] failed", datas_.size(),
            currBatchSize_, remoteApi_->Address(), rsp.fail_ids_size(), VectorToString(rsp.fail_ids()));
    } else {
        LOG(ERROR) << FormatString("[Migrate Data] Send %ld objects[%ld bytes] data to %s failed, error message: %s",
                                   datas_.size(), currBatchSize_, remoteApi_->Address(), s.ToString());
        std::transform(datas_.begin(), datas_.end(), std::inserter(failedIds_, failedIds_.end()),
                       [](const std::unique_ptr<BaseData> &d) { return d->Id(); });
        lastRc_ = s;
    }
    TryUpdateRate(rsp.limit_rate());

    // 3. Clear finally.
    Clear();
}

Status MigrateDataHandler::MigrateDataToRemoteRetry(const std::shared_ptr<WorkerRemoteWorkerOCApi> &api,
                                                    MigrateDataReqPb &req, const std::vector<MemView> &payloads,
                                                    MigrateDataRspPb &rsp)
{
    const int maxRetryCount = 3;
    int count = 0;
    Status status;
    do {
        count++;
        status = api->MigrateData(req, payloads, rsp);
        if (!IsRpcError(status)) {
            break;
        }
        rsp.Clear();
    } while (count <= maxRetryCount);
    return status;
}

Status MigrateDataHandler::TryUpdateRate(uint64_t rate)
{
    const uint64_t minSleepMs = 100;
    const uint64_t maxSleepMs = 500;
    int busyNodeRetryCount = 5;
    MigrateDataReqPb req;
    MigrateDataRspPb rsp;
    while (rate == 0 && busyNodeRetryCount > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(RandomData().GetRandomUint64(minSleepMs, maxSleepMs)));
        if (!IsRpcError(remoteApi_->MigrateData(req, {}, rsp))) {
            rate = rsp.limit_rate();
        }
        --busyNodeRetryCount;
        rsp.Clear();
    }
    limiter_.UpdateRate(rate);
    CHECK_FAIL_RETURN_STATUS(
        rate != 0, K_NOT_READY,
        FormatString("Remote node %s can't provide banwidth to migrate data", remoteApi_->Address()));
    return Status::OK();
}

MigrateDataHandler::MigrateResult MigrateDataHandler::ConstructResult(Status status) const
{
    return { .address = remoteApi_->Address(),
             .status = status,
             .successIds = successIds_,
             .failedIds = failedIds_,
             .skipIds = skipIds_,
             .migrateDataStrategy = migrateDataStrategy_ };
}

void MigrateDataHandler::Clear()
{
    currBatchSize_ = 0;
    currBatchCount_ = 0;
    datas_.clear();
}

MigrateProgress::MigrateProgress(uint64_t count, uint64_t intervalSeconds,
                                 std::function<void(double, uint64_t, uint64_t)> callback)
    : count_(count), intervalSeconds_(intervalSeconds), callback_(callback)
{
    auto traceID = Trace::Instance().GetTraceID();
    thread_ = Thread([this, traceID]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        Process();
    });
}

MigrateProgress::~MigrateProgress()
{
    stopFlag_ = true;
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void MigrateProgress::Deal(uint64_t count)
{
    constexpr uint64_t div = 100;
    uint64_t originCount = processedCount_.fetch_add(count);
    if ((originCount + count) / div > originCount / div) {
        cv_.notify_all();
    }
}

void MigrateProgress::Process()
{
    while (!stopFlag_) {
        std::unique_lock<std::mutex> l(mutex_);
        (void)cv_.wait_for(l, std::chrono::seconds(intervalSeconds_));
        if (callback_ != nullptr) {
            callback_(timer_.ElapsedSecond(), processedCount_, count_);
        }
    }
}

void MigrateDataRateLimiter::SlidingWindowUpdateRate(const uint64_t &bytesReceived)
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    auto now = std::chrono::steady_clock::now();
    window.push_back({ now, bytesReceived });
    currentBandwidth += bytesReceived;

    while (!window.empty() && (window.front().timestamp - std::chrono::seconds(1)) < now) {
        currentBandwidth -= window.front().bytes;
        window.pop_front();
    }
}
}  // namespace object_cache
}  // namespace datasystem