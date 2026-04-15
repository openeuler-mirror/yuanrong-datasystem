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
#include "datasystem/worker/object_cache/data_migrator/handler/async_resource_releaser.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"
#include "datasystem/worker/object_cache/data_migrator/transport/fast_migrate_transport.h"
#include "datasystem/worker/object_cache/data_migrator/transport/fast_migrate_transport2.h"
#include "datasystem/worker/object_cache/data_migrator/transport/tcp_migrate_transport.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"

DS_DECLARE_uint32(data_migrate_rate_limit_mb);

namespace datasystem {
namespace object_cache {
MigrateDataHandler::MigrateDataHandler(MigrateType type, const std::string &localAddr,
                                       const std::vector<ImmutableString> &needMigrateDataIds,
                                       std::shared_ptr<ObjectTable> objectTable,
                                       std::shared_ptr<WorkerRemoteWorkerOCApi> remoteApi,
                                       std::shared_ptr<SelectionStrategy> strategy,
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
      slotId_(slotId)
{
    if (ShouldUseFastTransport()) {
        transport_ = std::make_shared<FastMigrateTransport2>();
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
            LOG(WARNING) << FormatString("[Migrate Data] Remote node %s has no remain bytes: %ld",
                                         remoteApi_->Address(), maxBatchSize_);
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
    AddObjectDataLocked(objectKV);
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
        maxBatchSize_ = NodeSelector::Instance().GetAvailableMemory(remoteApi_->Address());
    } else {
        MigrateDataReqPb req;
        req.set_type(type_);
        MigrateDataRspPb rsp;
        Status s = MigrateDataToRemoteRetry(remoteApi_, req, {}, rsp);

        if (!strategy_->CheckCondition(rsp, type)) {
            RETURN_STATUS(StatusCode::K_NO_SPACE,
                          "[Migrate Data] migrateDataStrategy.CheckCondition failed due to insufficient space");
        }

        if (s.IsOk()) {
            RETURN_IF_NOT_OK(TryUpdateRate(rsp.limit_rate()));
            type == CacheType::MEMORY ? AdjustMaxBatchSize(rsp.remain_bytes())
                                      : AdjustMaxBatchSize(rsp.disk_remain_bytes());
            remoteDiskRemainSize_ = rsp.disk_remain_bytes();
            remoteMemoryRemainSize_ = rsp.remain_bytes();
        } else {
            LOG(WARNING) << FormatString("[Migrate Data] Spy on remote node %s remain bytes but meets error: %s",
                                         remoteApi_->Address(), s.ToString());
            if (s.GetCode() == StatusCode::K_NOT_READY) {
                RETURN_STATUS(StatusCode::K_NOT_READY, "[Migrate Data] Remote node cannot accept data");
            }
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
    std::unique_ptr<BaseDataUnit> data_;
    if (entry->IsSpilled() && entry->GetShmUnit() == nullptr) {
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
        datas_.emplace_back(std::make_unique<ShmData>(objectKey, entry->GetCreateTime(), entry->GetShmUnit(),
                                                      entry->GetDataSize(), entry->GetMetadataSize(),
                                                      entry->modeInfo.GetCacheType()));
    }
    currBatchSize_ += entry->GetDataSize();
    ++currBatchCount_;
    if (entry->modeInfo.GetCacheType() == CacheType::MEMORY) {
        currentMemorySize_ += entry->GetDataSize();
    } else {
        currentDiskSize_ += entry->GetDataSize();
    }
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

    if (!IsFastTransportEnabled()) {
        if (limiter_.IsRemoteBusyNode()) {
            LOG(WARNING) << FormatString("[Migrate Data] Remote node %s is busy", remoteApi_->Address());
            std::transform(datas_.begin(), datas_.end(), std::inserter(failedIds_, failedIds_.end()),
                           [](const std::unique_ptr<BaseDataUnit> &d) { return d->Id(); });
            lastRc_ = Status(StatusCode::K_NOT_READY, "[Migrate Data] Remote node is busy");
            Clear();
            return;
        }
        limiter_.WaitAllow(currBatchSize_);
    }

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
        TryUpdateRate(rsp.limitRate);
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

Status MigrateDataHandler::TryUpdateRate(uint64_t rate)
{
    RETURN_OK_IF_TRUE(IsFastTransportEnabled());
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
