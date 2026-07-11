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
 * Description: Migrate data handler implementation.
 */
#include "datasystem/worker/object_cache/data_migrator/handler/migrate_data_handler.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rdma/fast_transport_base.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/worker/object_cache/data_migrator/handler/async_resource_releaser.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"
#include "datasystem/worker/object_cache/data_migrator/transport/fast_migrate_transport.h"
#include "datasystem/worker/object_cache/data_migrator/transport/fast_migrate_transport2.h"
#include "datasystem/worker/object_cache/data_migrator/transport/tcp_migrate_transport.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"

DS_DECLARE_uint32(data_migrate_rate_limit_mb);
DS_DECLARE_string(data_migrate_urma_transport_mode);

namespace datasystem {
namespace object_cache {
MigrateDataHandler::MigrateDataHandler(MigrateType type, const std::string &localAddr,
                                       const std::vector<ImmutableString> &needMigrateDataIds,
                                       std::shared_ptr<ObjectTable> objectTable,
                                       std::shared_ptr<WorkerRemoteWorkerOCApi> remoteApi,
                                       std::shared_ptr<SelectionStrategy> strategy,
                                       std::atomic<bool> *stoppingPtr,
                                       std::shared_ptr<MigrateProgress> progress, bool isRetry, uint32_t slotId)
    : type_(type),
      localAddr_(localAddr),
      needMigrateDataIds_(needMigrateDataIds.begin(), needMigrateDataIds.end()),
      objectTable_(std::move(objectTable)),
      remoteApi_(std::move(remoteApi)),
      maxBatchSize_(FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul),
      currBatchSize_(0),
      currBatchCount_(0),
      limiter_(FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul),
      strategy_(std::move(strategy)),
      progress_(std::move(progress)),
      isRetry_(isRetry),
      slotId_(slotId),
      stoppingPtr_(stoppingPtr)
{
    if (ShouldUseFastTransport()) {
        if (FLAGS_data_migrate_urma_transport_mode == "read") {
            transport_ = std::make_shared<FastMigrateTransport>();
        } else {
            transport_ = std::make_shared<FastMigrateTransport2>();
        }
    } else {
        transport_ = std::make_shared<TcpMigrateTransport>();
    }
}

bool MigrateDataHandler::ShouldUseFastTransport() const
{
    return IsUrmaEnabled();
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

MigrateDataHandler::MigrateResult MigrateDataHandler::MigrateDataToRemote(bool isSlotMigration)
{
    PerfPoint point(PerfKey::WORKER_MIGRATE_TO_REMOTE);
    INJECT_POINT_NO_RETURN("MigrateDataHandler.MigrateDataToRemote.DelayMigrate",
                           [](int sleepMs) { std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs)); });
    std::vector<std::string> memoryDataIds;
    std::vector<std::string> diskDataIds;
    SplitByCacheType(memoryDataIds, diskDataIds);
    if (isSlotMigration) {
        std::vector<std::string> migrateIds(needMigrateDataIds_.begin(), needMigrateDataIds_.end());
        lastRc_ = MigrateDataByCacheType(CacheType::MEMORY, migrateIds, true);
        return ConstructResult(lastRc_);
    }
    lastRc_ = MigrateDataByCacheType(CacheType::MEMORY, memoryDataIds, false);
    maxBatchSize_ = FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul;
    lastRc_ = lastRc_.IsError() ? lastRc_ : MigrateDataByCacheType(CacheType::DISK, diskDataIds, false);
    return ConstructResult(lastRc_);
}

Status MigrateDataHandler::MigrateDataByCacheType(CacheType type, std::vector<std::string> &needMigrateDataIds,
                                                  bool isSlotMigration)
{
    RETURN_OK_IF_TRUE(needMigrateDataIds.empty());
    RETURN_IF_NOT_OK(PrepareRemoteMigration(type, needMigrateDataIds));

    for (auto it = needMigrateDataIds.begin(); it != needMigrateDataIds.end(); ++it) {
        if (IsRemoteLackResources()) {
            LOG(WARNING) << FormatString(
                "[Migrate Data] Remote node %s has no remain bytes, local node: %s, cache type: %d, "
                "max batch size: %ld",
                remoteApi_->Address(), localAddr_, static_cast<int>(type), maxBatchSize_);
            std::transform(datas_.begin(), datas_.end(), std::inserter(failedIds_, failedIds_.end()),
                           [](const std::unique_ptr<BaseDataUnit> &d) { return d->Id(); });
            (void)failedIds_.insert(it, needMigrateDataIds.end());
            return Status(StatusCode::K_NO_SPACE, "[Migrate Data] No remain bytes");
        }
        CollectObjectForMigration(*it, isSlotMigration);
    }
    SendDataToRemote(isSlotMigration);
    return lastRc_;
}

Status MigrateDataHandler::PrepareRemoteMigration(CacheType type, const std::vector<std::string> &needMigrateDataIds)
{
    Status s = SpyOnRemoteRemainBytes(type);
    if (s.IsError()) {
        (void)failedIds_.insert(needMigrateDataIds.begin(), needMigrateDataIds.end());
    }
    return s;
}

void MigrateDataHandler::CollectObjectForMigration(const std::string &objectKey, bool isSlotMigration)
{
    if (!isSlotMigration && IsFull()) {
        SendDataToRemote();
    }

    std::shared_ptr<SafeObjType> entry;
    Status rc = objectTable_->Get(objectKey, entry);
    if (rc.IsError()) {
        (void)skipIds_.emplace(objectKey);
        return;
    }

    rc = entry->RLock();
    if (rc.IsError()) {
        (void)skipIds_.emplace(objectKey);
        return;
    }

    ObjectKV objectKV(objectKey, *entry);
    rc = AddObjectDataLocked(objectKV);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[Migrate Data] Skip adding object %s into migrate batch, error: %s", objectKey,
                                     rc.ToString());
    }
    entry->RUnlock();
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

Status MigrateDataHandler::SpyOnRemoteRemainBytes(CacheType type)
{
    if (ShouldUseFastTransport()) {
        size_t availableMemory = 0;
        Status rc = NodeSelector::Instance().TryGetAvailableMemory(remoteApi_->Address(), availableMemory);
        if (rc.IsOk()) {
            maxBatchSize_ = availableMemory;
        } else if (rc.GetCode() == StatusCode::K_NOT_READY) {
            LOG(WARNING) << FormatString(
                "[Migrate Data] Remote node %s is not ready from resource snapshot, local node: %s, status: %s",
                remoteApi_->Address(), localAddr_, rc.ToString());
            return rc;
        } else {
            LOG(WARNING) << FormatString(
                "[Migrate Data] Failed to get remote node %s available memory from resource snapshot, local node: %s, "
                "status: %s, fallback to remote probe",
                remoteApi_->Address(), localAddr_, rc.ToString());
            RETURN_IF_NOT_OK(SpyOnRemoteRemainBytesByRpc(type));
        }
    } else {
        RETURN_IF_NOT_OK(SpyOnRemoteRemainBytesByRpc(type));
    }

    if (IsRemoteLackResources()) {
        LOG(WARNING) << FormatString(
            "[Migrate Data] Remote node %s has no remain bytes, local node: %s, cache type: %d, max batch size: %ld",
            remoteApi_->Address(), localAddr_, static_cast<int>(type), maxBatchSize_);
        RETURN_STATUS(StatusCode::K_NO_SPACE, "[Migrate Data] No remain bytes");
    }

    LOG(INFO) << FormatString(
        "[Migrate Data] Remote node %s remain bytes, local node: %s, cache type: %d, max batch size: %ld",
        remoteApi_->Address(), localAddr_, static_cast<int>(type), maxBatchSize_);
    return Status::OK();
}

Status MigrateDataHandler::SpyOnRemoteRemainBytesByRpc(CacheType type)
{
    MigrateDataReqPb req;
    req.set_type(type_);
    MigrateDataRspPb rsp;
    Status s = MigrateDataToRemoteRetry(remoteApi_, req, {}, rsp);
    if (s.IsError()) {
        LOG(WARNING) << FormatString(
            "[Migrate Data] Spy on remote node %s remain bytes but meets error, local node: %s, status: %s",
            remoteApi_->Address(), localAddr_, s.ToString());
        if (s.GetCode() == StatusCode::K_NOT_READY) {
            RETURN_STATUS(StatusCode::K_NOT_READY,
                          FormatString("[Migrate Data] Remote node %s cannot accept data", remoteApi_->Address()));
        }
        return s;
    }
    if (!strategy_->CheckCondition(rsp, type)) {
        LOG(WARNING) << FormatString(
            "[Migrate Data] Remote node %s has insufficient space, local node: %s, cache type: %d, remain bytes: %ld, "
            "disk remain bytes: %ld, available ratio: %.2f, disk available ratio: %.2f, scale down state: %d",
            remoteApi_->Address(), localAddr_, static_cast<int>(type), rsp.remain_bytes(), rsp.disk_remain_bytes(),
            rsp.available_ratio(), rsp.disk_available_ratio(), static_cast<int>(rsp.scale_down_state()));
        RETURN_STATUS(StatusCode::K_NO_SPACE,
                      FormatString("[Migrate Data] Remote node %s has insufficient space", remoteApi_->Address()));
    }
    RETURN_IF_NOT_OK(TryUpdateRate(rsp.limit_rate()));
    if (type == CacheType::MEMORY) {
        AdjustMaxBatchSize(rsp.remain_bytes());
    } else {
        AdjustMaxBatchSize(rsp.disk_remain_bytes());
    }
    return Status::OK();
}

void MigrateDataHandler::AdjustMaxBatchSize(uint64_t size)
{
    if (size == UINT64_MAX) {
        return;
    }
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
    if (entry->stateInfo.IsCacheInvalid() || entry->IsInvalid()) {
        (void)skipIds_.emplace(objectKey);
        return Status::OK();
    }

    auto shmUnit = entry->GetShmUnit();
    if (entry->IsSpilled() && shmUnit == nullptr) {
        std::vector<RpcMessage> data;
        Status rc = WorkerOcSpill::Instance()->Get(objectKey, data, entry->GetDataSize());
        if (rc.IsOk()) {
            datas_.emplace_back(std::make_unique<PayloadData>(objectKey, entry->GetCreateTime(), std::move(data),
                                                              entry->GetDataSize(), entry->modeInfo.GetCacheType()));
        } else {
            (void)failedIds_.emplace(objectKey);
            return rc;
        }
    } else {
        if (shmUnit == nullptr) {
            (void)failedIds_.emplace(objectKey);
            RETURN_STATUS_LOG_ERROR(K_NOT_FOUND,
                                    FormatString("[Migrate Data] Object %s has no shm unit when migrating", objectKey));
        }
        datas_.emplace_back(std::make_unique<ShmData>(objectKey, entry->GetCreateTime(), std::move(shmUnit),
                                                      entry->GetDataSize(), entry->GetMetadataSize(),
                                                      entry->modeInfo.GetCacheType()));
    }
    currBatchSize_ += entry->GetDataSize();
    ++currBatchCount_;
    return Status::OK();
}

void MigrateDataHandler::ReleaseResources(const std::unordered_set<ImmutableString> &successIds)
{
    if (type_ != MigrateType::SPILL) {
        return;
    }
    uint64_t releasedCount = 0;
    uint64_t releasedBytes = 0;
    for (const auto &data : datas_) {
        const auto &objectKey = data->Id();
        if (successIds.find(objectKey) == successIds.end()) {
            continue;
        }

        Status rc = AsyncResourceReleaser::Instance().Release(objectKey, data->Version());
        if (rc.IsError()) {
            AsyncResourceReleaser::Instance().AddTask(objectKey, data->Version());
            continue;
        }
        releasedCount++;
        releasedBytes += data->Size();
    }

    if (releasedCount > 0) {
        VLOG(1) << FormatString("[Migrate Data] Released %lu objects for spill type, total %lu bytes", releasedCount,
                                releasedBytes);
    }
}

void MigrateDataHandler::SendDataToRemote(bool isSlotMigration)
{
    PerfPoint pointAll(PerfKey::WORKER_SEND_DATA_TO_REMOTE);
    if (datas_.empty()) {
        Clear();
        return;
    }

    if (limiter_.IsRemoteBusyNode()) {
        VLOG(1) << FormatString("[Migrate Data] self-heal triggered for %s", remoteApi_->Address());
        Status heal = SelfHealBusyRate();
        if (heal.IsError()) {
            LOG(WARNING) << FormatString("[Migrate Data] Remote %s still busy after probe: %s",
                                         remoteApi_->Address(), heal.ToString());
            std::transform(datas_.begin(), datas_.end(), std::inserter(failedIds_, failedIds_.end()),
                           [](const std::unique_ptr<BaseDataUnit> &d) { return d->Id(); });
            lastRc_ = heal;
            Clear();
            return;
        }
    }
    limiter_.WaitAllow(currBatchSize_);

    MigrateTransport::Request req{ .type = type_,
                                   .api = remoteApi_,
                                   .datas = &datas_,
                                   .localAddr = localAddr_,
                                   .batchSize = currBatchSize_,
                                   .progress = progress_,
                                   .isSlotMigration = isSlotMigration,
                                   .isRetry = isRetry_,
                                   .slotId = slotId_ };
    MigrateTransport::Response rsp;
    PerfPoint point(PerfKey::WORKER_MIGRATE_TRANSPORT_SEND_DATA);
    Status s = transport_->MigrateDataToRemote(req, rsp);
    point.Record();
    if (s.IsOk()) {
        AdjustMaxBatchSize(rsp.remainBytes);
        successIds_.insert(rsp.successKeys.begin(), rsp.successKeys.end());
        failedIds_.insert(rsp.failedKeys.begin(), rsp.failedKeys.end());
        Status rc = TryUpdateRate(rsp.limitRate);
        if (rc.IsError()) {
            LOG(WARNING) << FormatString("[Migrate Data] Rate update failed for %s: %s",
                                         remoteApi_->Address(), rc.ToString());
        }
        ReleaseResources(rsp.successKeys);
    } else {
        LOG(ERROR) << FormatString("[Migrate Data] Send %ld objects[%ld bytes] data to %s failed, error message: %s",
                                   datas_.size(), currBatchSize_, remoteApi_->Address(), s.ToString());
        std::transform(datas_.begin(), datas_.end(), std::inserter(failedIds_, failedIds_.end()),
                       [](const std::unique_ptr<BaseDataUnit> &d) { return d->Id(); });
        lastRc_ = s;
    }

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

Status MigrateDataHandler::SelfHealBusyRate()
{
    if (selfHealAttempted_) {
        return lastHealStatus_;
    }
    selfHealAttempted_ = true;

    int probesMade = 0;
    auto deadline = static_cast<uint64_t>(GetSteadyClockTimeStampMs()) + BUSY_HEAL_BUDGET_MS;
    uint64_t rate = 0;
    Status lastErr;
    MigrateDataReqPb req;
    MigrateDataRspPb rsp;
    uint64_t sleepMs = BUSY_HEAL_INITIAL_SLEEP_MS;
    while (rate == 0 && probesMade < BUSY_HEAL_MAX_PROBES
           && static_cast<uint64_t>(GetSteadyClockTimeStampMs()) < deadline
           && (stoppingPtr_ == nullptr || !stoppingPtr_->load(std::memory_order_relaxed))) {
        INJECT_POINT_NO_RETURN("MigrateDataHandler.SelfHealBusyRate.probe");
        uint64_t actualSleep = RandomData().GetRandomUint64(
            sleepMs, std::min(sleepMs * BUSY_HEAL_BACKOFF_FACTOR, BUSY_HEAL_MAX_SLEEP_MS));
        for (uint64_t slept = 0;
             slept < actualSleep && (stoppingPtr_ == nullptr || !stoppingPtr_->load(std::memory_order_relaxed));
             slept += BUSY_HEAL_CANCEL_POLL_MS) {
            std::this_thread::sleep_for(std::chrono::milliseconds(BUSY_HEAL_CANCEL_POLL_MS));
        }
        if (stoppingPtr_ != nullptr && stoppingPtr_->load(std::memory_order_relaxed)) {
            break;
        }
        Status s = remoteApi_->MigrateDataProbe(req, rsp, 2000);
        if (s.IsOk()) {
            rate = rsp.limit_rate();
        } else {
            lastErr = s;
        }
        VLOG(1) << FormatString("[Migrate Data] busy re-probe for %s: attempt %d, rate=%lu, rc=%s",
                                remoteApi_->Address(), probesMade + 1, rate, s.ToString());
        ++probesMade;
        sleepMs = std::min(sleepMs * BUSY_HEAL_BACKOFF_FACTOR, BUSY_HEAL_MAX_SLEEP_MS);
        rsp.Clear();
    }
    limiter_.UpdateRate(rate);
    return BuildHealResult(rate, probesMade, lastErr);
}

Status MigrateDataHandler::BuildHealResult(uint64_t rate, int probesMade, const Status &lastErr)
{
    if (stoppingPtr_ != nullptr && stoppingPtr_->load(std::memory_order_relaxed)) {
        LOG(INFO) << FormatString("[Migrate Data] self-heal cancelled for %s during shutdown",
                                  remoteApi_->Address());
        lastHealStatus_ = Status(K_RUNTIME_ERROR, "Cancelled during shutdown");
        return lastHealStatus_;
    }
    if (rate == 0) {
        lastHealStatus_ = lastErr.IsError()
            ? lastErr
            : Status(K_NOT_READY,
                     FormatString("Remote node %s can't provide bandwidth after %d probes",
                                  remoteApi_->Address(), probesMade));
        return lastHealStatus_;
    }
    lastHealStatus_ = Status::OK();
    return lastHealStatus_;
}

Status MigrateDataHandler::TryUpdateRate(uint64_t rate)
{
    if (rate != 0) {
        limiter_.UpdateRate(rate);
        return Status::OK();
    }
    return SelfHealBusyRate();
}

MigrateDataHandler::MigrateResult MigrateDataHandler::ConstructResult(Status status) const
{
    return { .address = remoteApi_->Address(),
             .status = status,
             .successIds = successIds_,
             .failedIds = failedIds_,
             .skipIds = skipIds_,
             .strategy = strategy_ };
}

void MigrateDataHandler::Clear()
{
    currBatchSize_ = 0;
    currBatchCount_ = 0;
    datas_.clear();
}

}  // namespace object_cache
}  // namespace datasystem
