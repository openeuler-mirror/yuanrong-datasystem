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

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
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
        if (lastRc_.IsError()) {
            INJECT_POINT_NO_RETURN("MigrateDataHandler.StopAfterTransportFailure");
            failedIds_.insert(it, needMigrateDataIds.end());
            return lastRc_;
        }
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
        if (lastRc_.IsError()) {
            return;
        }
    }

    std::shared_ptr<SafeObjType> entry;
    Status rc = objectTable_->Get(objectKey, entry);
    if (rc.IsError()) {
        (void)skipIds_.emplace(objectKey);
        return;
    }

    auto lockStartUs = GetSteadyClockTimeStampUs();
    rc = entry->RLock();
    auto lockWaitUs = GetSteadyClockTimeStampUs() - lockStartUs;
    constexpr int64_t slowLockThresholdUs = 100'000;
    constexpr uint32_t slowLockLogEveryN = 100;
    if (lockWaitUs >= slowLockThresholdUs) {
        LOG_FIRST_EVERY_N(INFO, slowLockLogEveryN)
            << "event=MIGRATE_OBJECT_LOCK_SLOW target=" << remoteApi_->Address()
            << " object_key_hash=" << std::hash<std::string>{}(objectKey) << " wait_ms=" << lockWaitUs / SECS_TO_MS
            << " status=" << rc.ToString();
    }
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
    ss << "[Migrate Data] Migrate to remmote node [" << result.address << "] result: ["
       << "return code: " << result.status.ToString() << ", "
       << "success objects count: " << result.successIds.size() << ", "
       << "skip objects count: " << result.skipIds.size() << ", "
       << "fail objects count: " << result.failedIds.size()
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
    if (!CheckSendAdmission()) {
        return;
    }

    Status rateRc = EnsureRateForBatch();
    if (rateRc.IsError()) {
        LOG(WARNING) << FormatString("[Migrate Data] Remote %s rate is not usable after probe: %s",
                                     remoteApi_->Address(), rateRc.ToString());
        std::transform(datas_.begin(), datas_.end(), std::inserter(failedIds_, failedIds_.end()),
                       [](const std::unique_ptr<BaseDataUnit> &d) { return d->Id(); });
        lastRc_ = rateRc;
        Clear();
        return;
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
    INJECT_POINT_NO_RETURN("MigrateDataHandler.BeforeTransportSend");
    INJECT_POINT_NO_RETURN("MigrateDataHandler.TransportBatchStarted");
    auto transportStartUs = GetSteadyClockTimeStampUs();
    Status s = transport_->MigrateDataToRemote(req, rsp);
    auto transportUs = GetSteadyClockTimeStampUs() - transportStartUs;
    constexpr int64_t slowTransportThresholdUs = 5'000'000;
    constexpr uint32_t slowTransportLogEveryN = 10;
    if (transportUs >= slowTransportThresholdUs) {
        LOG_FIRST_EVERY_N(INFO, slowTransportLogEveryN)
            << "event=MIGRATE_TRANSPORT_SLOW target=" << remoteApi_->Address() << " batch_count=" << datas_.size()
            << " batch_bytes=" << currBatchSize_ << " elapsed_ms=" << transportUs / SECS_TO_MS
            << " limit_rate=" << rsp.limitRate << " status=" << s.ToString();
    }
    point.Record();
    HandleMigrationTransportResponse(s, rsp);
    Clear();
}

Status MigrateDataHandler::EnsureRateForBatch()
{
    bool busy = limiter_.IsRemoteBusyNode();
    if (!busy && type_ != MigrateType::SCALE_DOWN) {
        return Status::OK();
    }
    uint64_t estimatedWaitMs = limiter_.EstimateWaitMilliseconds(currBatchSize_);
    uint64_t maxWaitMs = GetScaleDownMaxLimiterWaitMilliseconds(currBatchSize_);
    bool scaleDownWaitTooLong = type_ == MigrateType::SCALE_DOWN
                                && estimatedWaitMs > maxWaitMs;
    if (!busy && !scaleDownWaitTooLong) {
        return Status::OK();
    }
    LOG(INFO) << "event=MIGRATE_LIMITER_SLOW target=" << remoteApi_->Address() << " batch_count=" << datas_.size()
              << " batch_bytes=" << currBatchSize_ << " estimated_wait_ms=" << estimatedWaitMs
              << " wait_limit_ms=" << maxWaitMs << " action=probe";
    return SelfHealBusyRate(currBatchSize_);
}

bool MigrateDataHandler::CheckSendAdmission()
{
    if (!sendAdmission_) {
        return true;
    }
    auto status = sendAdmission_();
    if (status.IsOk()) {
        return true;
    }
    std::transform(datas_.begin(), datas_.end(), std::inserter(failedIds_, failedIds_.end()),
                   [](const std::unique_ptr<BaseDataUnit> &data) { return data->Id(); });
    lastRc_ = std::move(status);
    Clear();
    INJECT_POINT_NO_RETURN("MigrateDataHandler.SendAdmissionDenied");
    return false;
}

void MigrateDataHandler::HandleMigrationTransportResponse(const Status &status, MigrateTransport::Response &response)
{
    if (response.ubFailureDetail.has_value()) {
        ubFailureDetail_ = std::move(response.ubFailureDetail);
    }
    if (status.IsError()) {
        LOG(ERROR) << FormatString("[Migrate Data] Send %ld objects[%ld bytes] data to %s failed, error message: %s",
                                   datas_.size(), currBatchSize_, remoteApi_->Address(), status.ToString());
        std::transform(datas_.begin(), datas_.end(), std::inserter(failedIds_, failedIds_.end()),
                       [](const std::unique_ptr<BaseDataUnit> &data) { return data->Id(); });
        lastRc_ = status;
        return;
    }
    AdjustMaxBatchSize(response.remainBytes);
    if (response.remainBytes != UINT64_MAX) {
        lastRemainBytes_ = response.remainBytes;
    }
    successIds_.insert(response.successKeys.begin(), response.successKeys.end());
    failedIds_.insert(response.failedKeys.begin(), response.failedKeys.end());
    skipIds_.insert(response.skipKeys.begin(), response.skipKeys.end());
    Status rateStatus = TryUpdateRate(response.limitRate);
    LOG_IF_ERROR(rateStatus, FormatString("[Migrate Data] Rate update failed for %s", remoteApi_->Address()));
    ReleaseResources(response.successKeys);
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

Status MigrateDataHandler::SelfHealBusyRate(uint64_t requiredSize)
{
    if (selfHealAttempted_) {
        return lastHealStatus_;
    }
    selfHealAttempted_ = true;
    auto startMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    int probesMade = 0;
    auto deadline = startMs + BUSY_HEAL_BUDGET_MS;
    uint64_t rate = 0;
    uint64_t estimatedWaitMs = UINT64_MAX;
    bool recovered = false;
    Status lastErr;
    MigrateDataReqPb req;
    MigrateDataRspPb rsp;
    uint64_t sleepMs = BUSY_HEAL_INITIAL_SLEEP_MS;
    while (!recovered && probesMade < BUSY_HEAL_MAX_PROBES
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
        lastErr = remoteApi_->MigrateDataProbe(req, rsp, BUSY_HEAL_PROBE_TIMEOUT_MS);
        if (lastErr.IsOk()) {
            rate = rsp.limit_rate();
            limiter_.UpdateRate(rate);
            estimatedWaitMs = limiter_.EstimateWaitMilliseconds(requiredSize);
            recovered = IsRateRecovered(rate, estimatedWaitMs, requiredSize);
        }
        VLOG(1) << FormatString("[Migrate Data] busy re-probe for %s: attempt %d, rate=%lu, rc=%s",
                                remoteApi_->Address(), probesMade + 1, rate, lastErr.ToString());
        ++probesMade;
        sleepMs = std::min(sleepMs * BUSY_HEAL_BACKOFF_FACTOR, BUSY_HEAL_MAX_SLEEP_MS);
        rsp.Clear();
    }
    if (!recovered) {
        limiter_.UpdateRate(rate);
    }
    lastHealStatus_ = BuildHealResult(recovered, rate, probesMade, lastErr);
    LOG(INFO) << "event=MIGRATE_RATE_REFRESH target=" << remoteApi_->Address() << " attempts=" << probesMade
              << " new_rate_bps=" << rate << " estimated_wait_ms=" << estimatedWaitMs
              << " elapsed_ms=" << GetSteadyClockTimeStampMs() - startMs
              << " status=" << lastHealStatus_.ToString();
    return lastHealStatus_;
}

bool MigrateDataHandler::IsRateRecovered(uint64_t rate, uint64_t estimatedWaitMs, uint64_t requiredSize) const
{
    if (rate == 0) {
        return false;
    }
    return type_ != MigrateType::SCALE_DOWN
           || estimatedWaitMs <= GetScaleDownMaxLimiterWaitMilliseconds(requiredSize);
}

uint64_t MigrateDataHandler::GetScaleDownMaxLimiterWaitMilliseconds(uint64_t requiredSize) const
{
    uint64_t configuredRate = static_cast<uint64_t>(FLAGS_data_migrate_rate_limit_mb) * MB_TO_BYTES;
    if (configuredRate == 0) {
        return SCALE_DOWN_MIN_LIMITER_WAIT_MS;
    }
    uint64_t configuredWaitMs = requiredSize * SECS_TO_MS / configuredRate + 1;
    return std::max(SCALE_DOWN_MIN_LIMITER_WAIT_MS, configuredWaitMs);
}

Status MigrateDataHandler::BuildHealResult(bool recovered, uint64_t rate, int probesMade, const Status &lastErr)
{
    if (stoppingPtr_ != nullptr && stoppingPtr_->load(std::memory_order_relaxed)) {
        LOG(INFO) << FormatString("[Migrate Data] self-heal cancelled for %s during shutdown",
                                  remoteApi_->Address());
        return Status(K_RUNTIME_ERROR, "Cancelled during shutdown");
    }
    if (!recovered) {
        return lastErr.IsError()
                   ? lastErr
                   : Status(K_NOT_READY,
                            FormatString("Remote node %s can't provide usable bandwidth after %d probes, rate: %lu",
                                         remoteApi_->Address(), probesMade, rate));
    }
    return Status::OK();
}

Status MigrateDataHandler::TryUpdateRate(uint64_t rate)
{
    if (rate != 0) {
        selfHealAttempted_ = false;
        lastHealStatus_ = Status::OK();
        limiter_.UpdateRate(rate);
        if (type_ == MigrateType::SCALE_DOWN) {
            uint64_t estimatedWaitMs = limiter_.EstimateWaitMilliseconds(currBatchSize_);
            constexpr uint32_t lowRateLogEveryN = 10;
            if (estimatedWaitMs > GetScaleDownMaxLimiterWaitMilliseconds(currBatchSize_)) {
                LOG_FIRST_EVERY_N(INFO, lowRateLogEveryN)
                    << "event=MIGRATE_RATE_LOW target=" << remoteApi_->Address() << " batch_bytes=" << currBatchSize_
                    << " advertised_rate_bps=" << rate << " estimated_wait_ms=" << estimatedWaitMs;
            }
        }
        return Status::OK();
    }
    return SelfHealBusyRate(currBatchSize_);
}

MigrateDataHandler::MigrateResult MigrateDataHandler::ConstructResult(Status status) const
{
    return { .address = remoteApi_->Address(),
             .status = status,
             .successIds = successIds_,
             .failedIds = failedIds_,
             .skipIds = skipIds_,
             .strategy = strategy_,
             .ubFailureDetail = ubFailureDetail_,
             .targetRemainBytes = lastRemainBytes_ };
}

void MigrateDataHandler::Clear()
{
    currBatchSize_ = 0;
    currBatchCount_ = 0;
    datas_.clear();
}

}  // namespace object_cache
}  // namespace datasystem
