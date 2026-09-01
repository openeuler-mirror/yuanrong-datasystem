/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: BoundMode split out of ObjectClientImpl (lc=true data plane).
 */

#include "datasystem/client/object_cache/bound_mode.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/client/object_cache/worker_failover.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"

namespace datasystem {
namespace object_cache {

namespace {
// Only referenced from USE_URMA call paths; keep the definitions linkable in URMA-less builds.
struct UBGetBatch {
    std::vector<size_t> indices;
    uint64_t totalSize = 0;
};

Status WithProviderUbFailureDetail(Status status, const GetRspPb &rsp)
{
    if (status.IsOk() || !rsp.has_provider_ub_failure_detail()) {
        return status;
    }
    const auto &detail = rsp.provider_ub_failure_detail();
    if (detail.failure_side() != PROVIDER_LOCAL_UB_WRITE_FAILURE_SIDE || detail.failed_endpoint().empty()
        || detail.operator_worker().empty() || detail.status_code() != static_cast<int32_t>(status.GetCode())
        || detail.message() != status.GetMsg()) {
        return status;
    }
    std::string fields = "provider_ub_failure_detail: failed_endpoint=" + detail.failed_endpoint()
                         + ", operator_worker=" + detail.operator_worker()
                         + ", failure_side=" + detail.failure_side();
    if (detail.has_provider_status()) {
        fields += ", provider_status=" + std::to_string(detail.provider_status());
    }
    if (detail.has_cqe_status()) {
        fields += ", cqe_status=" + std::to_string(detail.cqe_status());
    }
    status.AppendMsg(fields);
    return status;
}

Status GetWorkerGetFailure(const GetRspPb &rsp, const HostPort &worker, const std::string &notFoundMessage)
{
    Status status(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    if (status.IsOk()) {
        return Status(K_NOT_FOUND, notFoundMessage);
    }
    return WithRpcDiag(WithProviderUbFailureDetail(std::move(status), rsp), "Get", worker);
}

const std::string K_SEPARATOR = "$";

#ifdef USE_URMA
AccessTransportKind MergeTransportKind(AccessTransportKind lhs, AccessTransportKind rhs)
{
    return static_cast<AccessTransportKind>(std::max(static_cast<uint8_t>(lhs), static_cast<uint8_t>(rhs)));
}
#endif

void MergeTransportKind(std::atomic<AccessTransportKind> &aggregatedTransport, AccessTransportKind kind)
{
    auto current = aggregatedTransport.load(std::memory_order_relaxed);
    // Transport priority only moves upward (SHM -> UB -> TCP), so failed CAS retries either
    // observe a newer higher-priority value and exit, or eventually publish this thread's value.
    while (static_cast<uint8_t>(kind) > static_cast<uint8_t>(current)
           && !aggregatedTransport.compare_exchange_weak(current, kind, std::memory_order_relaxed)) {
    }
}

[[maybe_unused]] std::vector<UBGetBatch> BuildUBGetBatches(const std::vector<ObjMetaInfo> &objMetas,
                                                           uint64_t ubMaxGetSize)
{
    std::vector<UBGetBatch> batches;
    UBGetBatch currentBatch;

    for (size_t i = 0; i < objMetas.size(); ++i) {
        uint64_t objSize = objMetas[i].objSize;

        if (objSize > ubMaxGetSize) {
            if (!currentBatch.indices.empty()) {
                batches.push_back(std::move(currentBatch));
                currentBatch = UBGetBatch{};
            }
            UBGetBatch tcpBatch;
            tcpBatch.indices.push_back(i);
            tcpBatch.totalSize = objSize;
            batches.push_back(std::move(tcpBatch));
            continue;
        }

        if (!currentBatch.indices.empty() && currentBatch.totalSize + objSize > ubMaxGetSize) {
            batches.push_back(std::move(currentBatch));
            currentBatch = UBGetBatch{};
        }

        currentBatch.indices.push_back(i);
        currentBatch.totalSize += objSize;
    }

    if (!currentBatch.indices.empty()) {
        batches.push_back(std::move(currentBatch));
    }
    return batches;
}
}  // namespace

struct PipelineAsyncResource {
    std::future<Status> rpcFuture;
    std::promise<AsyncResult> promise;
    PiplnRh2dParam piplnRh2dParam;
};

void ComputeDataSizes(const std::vector<StringView> &vals, std::vector<uint64_t> &sizes, uint64_t &sum)
{
    sizes.reserve(vals.size());
    for (const auto &val : vals) {
        sizes.emplace_back(val.size());
        sum += val.size();
    }
}

BoundMode::BoundMode(const Deps &deps)
    : workerApi_(deps.workerApi),
      mmapManager_(deps.mmapManager),
      memoryRefCount_(deps.memoryRefCount),
      globalRefCount_(deps.globalRefCount),
      globalRefMutex_(deps.globalRefMutex),
      transportLayer_(deps.transportLayer),
      routing_(deps.routing),
      memoryCopyThreadPool_(deps.memoryCopyThreadPool),
      asyncReleasePool_(deps.asyncReleasePool),
      asyncGetRPCPool_(deps.asyncGetRPCPool),
      asyncPipelineRH2DPool_(deps.asyncPipelineRH2DPool),
      simpleIdRe_(deps.simpleIdRe),
      failover_(deps.failover),
      shutdownMux_(deps.shutdownMux),
      currentNode_(deps.currentNode),
      requestTimeoutMs_(deps.requestTimeoutMs),
      tenantId_(deps.tenantId),
      token_(deps.token),
      enableLocalCache_(deps.enableLocalCache),
      enableClientDirectPipelineH2D_(deps.enableClientDirectPipelineH2D),
      parallismNum_(deps.parallismNum),
      getWorkerApi_(deps.getWorkerApi),
      getWorkerApiNode_(deps.getWorkerApiNode),
      host_(deps.host)
{
}

std::shared_ptr<ObjectBufferInfo> BoundMode::MakeUbPoolBufferInfo(const std::string &objectKey,
                                                                  uint64_t dataSize, const FullParam &param,
                                                                  uint32_t version, const ShmKey &shmId)
{
#ifdef USE_URMA
    std::shared_ptr<UrmaManager::BufferHandle> ubBufHandle;
    if (UrmaManager::Instance().GetMemoryBufferHandle(ubBufHandle, dataSize).IsOk()) {
        auto info = MakeObjectBufferInfo(objectKey, static_cast<uint8_t*>(ubBufHandle->GetPointer()),
                                         dataSize, 0, param, false, version, shmId);
        info->ubGetBufferHandle = ubBufHandle;
        return info;
    }
#endif
    return MakeObjectBufferInfo(objectKey, nullptr, dataSize, 0, param, false, version, shmId);
}

Status BoundMode::CreateShmBuffer(const std::string &objectKey, uint64_t dataSize, const FullParam &param,
                                  const std::shared_ptr<IClientWorkerApi> &workerApi,
                                  const LatencyTraceConfig &config, bool traceEnabled,
                                  std::shared_ptr<Buffer> &newBuffer)
{
    uint32_t version = 0;
    if (workerApi->ShmCreateable(dataSize) || IsUrmaEnabled()) {
        uint64_t metadataSize = 0;
        auto shmBuf = std::make_shared<ShmUnitInfo>();
        std::shared_ptr<UrmaRemoteAddrPb> urmaDataInfo = nullptr;
        Timer timer;
        if (traceEnabled) {
            Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_CREATE_RPC_START);
        }
        auto rc = workerApi->Create(objectKey, dataSize, version, metadataSize, shmBuf, urmaDataInfo, param.cacheType);
        if (traceEnabled) {
            Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_CREATE_RPC_END);
        }
        const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
        const double elapsedMs = static_cast<double>(elapsedUs) / US_PER_MS;
        SLOW_LOG_IF_OR_VLOG(INFO, config.rpcSlowerThanUs > 0 && elapsedUs >= config.rpcSlowerThanUs, 1,
                            FormatString("Finished creating object to worker, object_key: %s, path: %s, cost: %.3fms, "
                                         "rc: %s", objectKey,
                                         IsUrmaEnabled() && urmaDataInfo != nullptr ? "UB" : "SHM", elapsedMs,
                                         rc.ToString()));
        RETURN_IF_NOT_OK(rc);
        std::shared_ptr<ObjectBufferInfo> bufferInfo = nullptr;
        std::shared_ptr<client::IMmapTableEntry> mmapEntry = nullptr;
        if (!urmaDataInfo) {
            RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd("", shmBuf));
            mmapEntry = mmapManager_->GetMmapEntryByFd(shmBuf->fd);
            CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr, StatusCode::K_RUNTIME_ERROR, "Get mmap entry failed");
            bufferInfo =
                MakeObjectBufferInfo(objectKey, (uint8_t *)(shmBuf->pointer) + shmBuf->offset, dataSize, metadataSize,
                                     param, false, version, shmBuf->id, nullptr, std::move(mmapEntry));
        } else {
            bufferInfo = MakeUbPoolBufferInfo(objectKey, dataSize, param, version, shmBuf->id);
        }
        // Store URMA info for later use in SendBufferViaUb.
        bufferInfo->ubUrmaDataInfo = urmaDataInfo;
        memoryRefCount_->IncreaseRef(shmBuf->id);
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), host_.getSelf(), newBuffer));
    } else {
        auto bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, dataSize, 0, param, false, version);
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), host_.getSelf(), newBuffer));
    }
    return Status::OK();
}

Status BoundMode::ConstructMultiCreateParam(const std::vector<std::string> &objectKeyList,
                                            const std::vector<uint64_t> &dataSizeList,
                                            std::vector<std::shared_ptr<Buffer>> &bufferList,
                                            std::vector<MultiCreateParam> &multiCreateParamList,
                                            uint64_t &dataSizeSum)
{
    auto sz = objectKeyList.size();
    CHECK_FAIL_RETURN_STATUS(sz == dataSizeList.size(), K_INVALID,
                             "The length of objectKeyList and dataSizeList should be the same.");
    multiCreateParamList.reserve(sz);
    for (size_t i = 0; i < sz; i++) {
        auto &objectKey = objectKeyList[i];
        auto dataSize = dataSizeList[i];
        CHECK_FAIL_RETURN_STATUS(dataSize > 0, K_INVALID, "The dataSize value should be bigger than zero.");
        dataSizeSum += dataSize;
        multiCreateParamList.emplace_back(i, objectKey, dataSize);
    }
    bufferList.resize(sz);
    return Status::OK();
}

void BoundMode::BatchDecreaseRefCnt(const std::vector<std::pair<ShmKey, std::uint32_t>> &shmInfos)
{
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    auto decreaseRefCnt = [this](const std::vector<std::pair<ShmKey, std::uint32_t>> &shmInfos) {
        std::vector<ShmKey> decreaseShms;
        for (auto &info : shmInfos) {
            if (!host_.isBufferAlive(info.second)) {
                continue;
            }
            const auto &shmId = info.first;
            if (!memoryRefCount_->DecreaseRef(shmId)) {
                continue;
            }
            decreaseShms.emplace_back(shmId);
        }

        PerfPoint descPoint(PerfKey::CLIENT_BATCH_DECREASE_MEM_REF);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerApi_[ObjectClientImpl::LOCAL_WORKER]->DecreaseWorkerRef(decreaseShms),
                                         "DecreaseReferenceCnt failed.");
        return Status::OK();
    };

    Status rc = decreaseRefCnt(shmInfos);
    if (rc.IsError()) {
        LOG(WARNING) << "Decrease reference failed: " << rc.ToString();
    }
}

void BoundMode::DecreaseReferenceCnt(const ShmKey &shmId, bool isShm, uint32_t version)
{
    if (asyncReleasePool_ == nullptr || shmId.Empty()) {
        METRIC_INC(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL);
        return;
    }
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    int64_t apiRemainingUs = ApiDeadline::Instance().ApiRemainingUs();
    auto dispatchTime = std::chrono::steady_clock::now();
    bool async = true;
    INJECT_POINT("client.DecreaseReferenceCnt", [&async](bool value) { async = value; });
    if (async) {
        asyncReleasePool_->Execute([this, shmId, isShm, version, apiRemainingUs, dispatchTime] {
            ApiDeadline::Instance().Push();
            Raii deadlineRaii([]() { ApiDeadline::Instance().Pop(); });
            auto queueDelayUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::steady_clock::now() - dispatchTime)
                                    .count();
            int64_t actualRemainingUs = apiRemainingUs - queueDelayUs;
            if (actualRemainingUs > 0) {
                ApiDeadline::Instance().InitUs(actualRemainingUs);
            }
            LOG_IF_ERROR(DecreaseReferenceCntImpl(shmId, isShm, version), "DecreaseReferenceCntImpl failed");
        });
    } else {
        LOG_IF_ERROR(DecreaseReferenceCntImpl(shmId, isShm, version), "DecreaseReferenceCntImpl failed");
    }
}

Status BoundMode::DecreaseReferenceCntImpl(const ShmKey &shmId, bool isShm, uint32_t version)
{
    bool needDecreaseWorkerRef = memoryRefCount_->DecreaseRef(shmId);
    VLOG(1) << FormatString("Try decrease ref count for shmId %s on clientId %s, needDecreaseWorkerRef %d", shmId,
                            workerApi_[ObjectClientImpl::LOCAL_WORKER]->clientId_, needDecreaseWorkerRef);
    if (!needDecreaseWorkerRef) {
        METRIC_INC(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL);
        return Status::OK();
    }
    if (isShm && !host_.isBufferAlive(version)) {
        METRIC_INC(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL);
        return Status::OK();
    }
    RETURN_IF_NOT_OK(host_.checkConnection());
    PerfPoint descPoint(PerfKey::CLIENT_DECREASE_MEM_REF);
    auto checkFunc = host_.checkConnWhileShmModify;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        workerApi_[ObjectClientImpl::LOCAL_WORKER]->DecreaseShmRef(shmId, checkFunc, shutdownMux_),
        "DecreaseShmRef failed.");
    return Status::OK();
}

Status BoundMode::Seal(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                       const std::unordered_set<std::string> &nestedObjectKeys, bool isShm)
{
    RETURN_IF_NOT_OK(host_.isClientReady());
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    PerfPoint sealPoint(PerfKey::CLIENT_SEAL_OBJECT);
    RETURN_IF_NOT_OK(host_.checkConnection());
    RETURN_IF_NOT_OK(ObjectClientImpl::CheckValidObjectKeyVector(nestedObjectKeys, true));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::IsBatchSizeUnderLimit(nestedObjectKeys.size()), K_INVALID,
        FormatString("The nestedObjectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    const std::string &objectKey = bufferInfo->objectKey;
    if (nestedObjectKeys.find(objectKey) != nestedObjectKeys.end()) {
        RETURN_STATUS(K_UNKNOWN_ERROR, "Nested object references cannot be nested in a loop.");
    }
    VLOG(1) << "Begin to seal object, object_key: " << objectKey;
    if (bufferInfo->isRoutedWrite) {
        // Routed two-step buffer: seal via transport layer on the worker pinned at Create time.
        return host_.publishRoutedBuffer(bufferInfo, nestedObjectKeys, true);
    }
    PerfPoint rpcPoint(PerfKey::RPC_CLIENT_SEAL_OBJECT);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        workerApi_[ObjectClientImpl::LOCAL_WORKER]->Publish(bufferInfo, isShm, true, nestedObjectKeys),
        FormatString("Seal object %s", objectKey));
    rpcPoint.Record();
    VLOG(1) << "Finished sealing object, object_key: " << objectKey;
    sealPoint.Record();
    return Status::OK();
}

Status BoundMode::Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                          const std::unordered_set<std::string> &nestedObjectKeys, bool isShm)
{
    RETURN_IF_NOT_OK(host_.isClientReady());
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    auto config = GetClientLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    PerfPoint perfPoint(PerfKey::CLIENT_PUBLISH_OBJECT);
    RETURN_IF_NOT_OK(ObjectClientImpl::CheckValidObjectKeyVector(nestedObjectKeys, true));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::IsBatchSizeUnderLimit(nestedObjectKeys.size()), K_INVALID,
        FormatString("The nestedObjectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    const std::string &objectKey = bufferInfo->objectKey;
    const uint32_t ttlSecond = bufferInfo->ttlSecond;
    const int existence = bufferInfo->existence;
    VLOG(1) << "Begin to publish object, object_key: " << objectKey << " with ttlSecond = " << ttlSecond;

    bufferInfo->isSeal = false;
    if (bufferInfo->isRoutedWrite) {
        // Routed two-step buffer: the worker was pinned at Create time; seal via transport layer.
        return host_.publishRoutedBuffer(bufferInfo, nestedObjectKeys, false);
    }
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(getWorkerApi_(workerApi, raii));
    Timer timer;
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_PUBLISH_RPC_START);
    }
    auto rc = workerApi->Publish(bufferInfo, isShm, false, nestedObjectKeys, ttlSecond, existence);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_PUBLISH_RPC_END);
    }
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    const double elapsedMs = static_cast<double>(elapsedUs) / US_PER_MS;
    SLOW_LOG_IF_OR_VLOG(INFO, config.rpcSlowerThanUs > 0 && elapsedUs >= config.rpcSlowerThanUs, 1,
        FormatString("Finished publishing object to worker, object_key: %s, path: %s, cost: %.3fms, rc: %s",
                     objectKey, isShm ? "SHM" : (bufferInfo->ubUrmaDataInfo != nullptr ? "UB" : "TCP"),
                     elapsedMs, rc.ToString()));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, FormatString("Publish object %s", objectKey));
    return Status::OK();
}

Status BoundMode::SendBufferViaUb(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data,
                                  uint64_t length, bool traceEnabled)
{
    std::shared_ptr<IClientWorkerApi> api;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(getWorkerApi_(api, raii));
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    RETURN_RUNTIME_ERROR_IF_NULL(bufferInfo);
    return transportLayer_->RunClientLocalUbWrite(api->hostPort_, *bufferInfo, [&] {
        return api->SendBufferViaUb(bufferInfo, data, length, traceEnabled);
    });
}

Status BoundMode::SendBufferViaUbFromPool(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                                          const void *data, uint64_t length, bool traceEnabled)
{
    std::shared_ptr<IClientWorkerApi> api;
    std::unique_ptr<Raii> raii;
    RETURN_IF_NOT_OK(getWorkerApi_(api, raii));
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    RETURN_RUNTIME_ERROR_IF_NULL(bufferInfo);
    return transportLayer_->RunClientLocalUbWrite(
        api->hostPort_, *bufferInfo,
        [&] { return api->SendBufferViaUbFromPool(bufferInfo, data, length, traceEnabled); });
}

Status BoundMode::InvalidateBuffer(const std::string &objectKey)
{
    RETURN_IF_NOT_OK(host_.isClientReady());
    RETURN_IF_NOT_OK(ObjectClientImpl::CheckValidObjectKey(objectKey));
    RETURN_IF_NOT_OK(host_.checkConnection());
    RETURN_IF_NOT_OK(workerApi_[ObjectClientImpl::LOCAL_WORKER]->InvalidateBuffer(objectKey));
    return Status::OK();
}

Status BoundMode::TimedMmapLookupWithDeadline(const std::shared_ptr<ShmUnitInfo> &shmBuf, uint64_t size)
{
    RETURN_IF_NOT_OK(ApiDeadline::Instance().CheckApiDeadline());
    Timer mmapTimer;
    auto mmapRc = mmapManager_->LookupUnitsAndMmapFd("", shmBuf);
    int64_t mmapCostUs = mmapTimer.ElapsedMicroSecond();
    int64_t mmapRemainingUs = ApiDeadline::Instance().ApiRemainingUs();
    SLOW_LOG_IF_OR_VLOG(INFO, mmapCostUs >= TimeoutDuration::SLOW_PATH_LOG_THRESHOLD_US || mmapRc.IsError(), 1,
        FormatString("[Set] phase=mmap costUs=%lld remainingUs=%lld size=%zu rc=%s",
                     mmapCostUs, mmapRemainingUs, size, mmapRc.ToString()));
    return mmapRc;
}

Status BoundMode::TimedMemoryCopyWithDeadline(const std::shared_ptr<Buffer> &buffer, const uint8_t *data,
                                              uint64_t size, bool traceEnabled)
{
    RETURN_IF_NOT_OK(ApiDeadline::Instance().CheckApiDeadline());
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_MEMORY_COPY_START);
    }
    Timer copyTimer;
    // Copy user data into the shared memory buffer.
    // no need call WLatch, the other thread cannot change before publish.
    auto copyRc = buffer->MemoryCopy(data, size);
    int64_t copyCostUs = copyTimer.ElapsedMicroSecond();
    int64_t copyRemainingUs = ApiDeadline::Instance().ApiRemainingUs();
    SLOW_LOG_IF_OR_VLOG(INFO, copyCostUs >= TimeoutDuration::SLOW_PATH_LOG_THRESHOLD_US || copyRc.IsError(), 1,
        FormatString("[Set] phase=MemoryCopy costUs=%lld remainingUs=%lld size=%zu rc=%s",
                     copyCostUs, copyRemainingUs, size, copyRc.ToString()));
    RETURN_IF_NOT_OK(copyRc);
    return ApiDeadline::Instance().CheckApiDeadline();
}

Status BoundMode::ProcessShmPut(const std::string &objectKey, const uint8_t *data, uint64_t size,
                                const FullParam &param, const std::unordered_set<std::string> &nestedObjectKeys,
                                uint32_t ttlSecond, const std::shared_ptr<IClientWorkerApi> &workerApi,
                                int existence, SetFailureStage &failureStage, int32_t requestTimeoutMs)
{
    RETURN_IF_NOT_OK(CheckLocalUbSenderAdmission(workerApi));
    auto config = GetClientLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    // Create a buffer first.
    auto shmBuf = std::make_shared<ShmUnitInfo>();
    uint32_t version = 0;
    uint64_t metadataSize = 0;
    std::shared_ptr<UrmaRemoteAddrPb> urmaDataInfo = nullptr;  // For Create+MemoryCopy+Publish path with URMA
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_CREATE_RPC_START);
    }
    failureStage = SetFailureStage::CREATE;
    RETURN_IF_NOT_OK(workerApi->Create(objectKey, size, version, metadataSize, shmBuf, urmaDataInfo, param.cacheType,
                                       requestTimeoutMs));
    failureStage = SetFailureStage::TRANSFER;
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_CREATE_RPC_END);
    }
    std::shared_ptr<ObjectBufferInfo> objInfo = nullptr;
    std::shared_ptr<client::IMmapTableEntry> mmapEntry = nullptr;
    if (!urmaDataInfo) {
        RETURN_IF_NOT_OK(TimedMmapLookupWithDeadline(shmBuf, size));
        mmapEntry = mmapManager_->GetMmapEntryByFd(shmBuf->fd);
        CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr, StatusCode::K_RUNTIME_ERROR, "Get mmap entry failed");
        objInfo = MakeObjectBufferInfo(objectKey, (uint8_t *)(shmBuf->pointer) + shmBuf->offset, size, metadataSize,
                                       param, false, version, shmBuf->id, nullptr, std::move(mmapEntry));
    } else {
        objInfo = MakeUbPoolBufferInfo(objectKey, size, param, version, shmBuf->id);
    }
    // Store URMA info for later use in SendBufferViaUb
    objInfo->ubUrmaDataInfo = urmaDataInfo;
    std::shared_ptr<Buffer> buffer;

    memoryRefCount_->IncreaseRef(shmBuf->id);
    RETURN_IF_NOT_OK(Buffer::CreateBuffer(objInfo, host_.getSelf(), buffer));

    RETURN_IF_NOT_OK(TimedMemoryCopyWithDeadline(buffer, data, size, traceEnabled));
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_MEMORY_COPY_END);
    }

    // Start to send put request.
    // In this case buffer is local data, but rpc must be locked.:
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_PUBLISH_RPC_START);
    }
    // Skip the non-idempotent Publish RPC once Create+mmap+memcpy exhausted the budget. Create
    // allocates only a worker-local shm unit (reclaimed by client_dead_timeout GC), so skipping
    // here leaves no master metadata orphan; K_RPC_DEADLINE_EXCEEDED is non-retryable and non-evicting.
    RETURN_IF_NOT_OK(ApiDeadline::Instance().CheckApiDeadline());
    failureStage = SetFailureStage::PUBLISH;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerApi->Publish(objInfo, !urmaDataInfo || objInfo->ubDataSentByMemoryCopy,
                                                         false, nestedObjectKeys, ttlSecond, existence,
                                                         requestTimeoutMs),
                                     FormatString("Put object %s", objectKey));
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_PUBLISH_RPC_END);
    }
    if (!urmaDataInfo) {
        buffer->SetVisibility(true);
    }
    // Destruct buffer with async
    buffer.reset();
    return Status::OK();
}

Status BoundMode::CheckLocalUbSenderAdmission(const std::shared_ptr<IClientWorkerApi> &workerApi) const
{
    if (!IsUrmaEnabled() || workerApi->IsShmEnable()) {
        return Status::OK();
    }
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    return transportLayer_->CheckLocalUbSenderAdmission();
}

Status BoundMode::ProcessDirectSetWithoutTransport(
    const std::string &objectKey, const uint8_t *data, uint64_t size, const FullParam &param,
    const std::unordered_set<std::string> &nestedObjectKeys, uint32_t ttlSecond, int existence,
    const SetRouteContext &routeContext, SetFailureStage &failureStage, std::vector<HostPort> &excludedWorkers,
    int32_t requestTimeoutMs)
{
    if (IsUrmaEnabled()) {
        return ProcessShmPut(objectKey, data, size, param, nestedObjectKeys, ttlSecond, routeContext.directWorkerApi,
                             existence, failureStage, requestTimeoutMs);
    }
    auto info = MakeObjectBufferInfo(objectKey, const_cast<uint8_t *>(data), size, 0, param, false, 0);
    const bool traceEnabled = ShouldCollectLatencyTrace(GetClientLatencyTraceConfig());
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_PUBLISH_RPC_START);
    }
    failureStage = SetFailureStage::PUBLISH;
    auto rc = routeContext.directWorkerApi->Publish(info, false, false, nestedObjectKeys, ttlSecond, existence,
                                                    requestTimeoutMs);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_PUBLISH_RPC_END);
    }
    if (rc.IsError()) {
        (void)host_.handleSetRouteFailure(rc, failureStage, routeContext.worker, excludedWorkers);
        if (host_.isRoutingEvictionFailure(rc)) {
            (void)failover_->SwitchWorkerNode(currentNode_.load(), client::SwitchTriggerReason::WORKER_UNAVAILABLE);
        }
    }
    return rc;
}

Status BoundMode::CheckPipelineRH2DArgs(const std::vector<std::string> &objectKeys,
                                        const std::vector<Blob> &devBlob)
{
    // check args
    CHECK_FAIL_RETURN_STATUS(objectKeys.size() == devBlob.size(), K_INVALID,
                             "objectKeys size is not equal to devBlob size");
    CHECK_FAIL_RETURN_STATUS(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                             FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    RETURN_IF_NOT_OK(ObjectClientImpl::CheckValidObjectKeyVector(objectKeys));
    if (objectKeys.size() > 1) {
        std::unordered_set<std::string_view> uniqueKeys;
        uniqueKeys.reserve(objectKeys.size());
        for (size_t i = 0; i < objectKeys.size(); ++i) {
            const bool inserted = uniqueKeys.emplace(objectKeys[i]).second;
            CHECK_FAIL_RETURN_STATUS(inserted, K_INVALID,
                                     FormatString("The input parameter contains duplicate key at index %zu.", i));
        }
    }
    for (size_t i = 0; i < devBlob.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(devBlob[i].pointer != nullptr, K_INVALID,
                                 FormatString("device blob pointer is null, key index: %zu", i));
        CHECK_FAIL_RETURN_STATUS(devBlob[i].size > 0, K_INVALID,
                                 FormatString("device blob size is zero, key index: %zu", i));
    }
    RETURN_IF_NOT_OK(host_.isClientReady());
    return Status::OK();
}

Status BoundMode::CheckLocalPipelineRH2DArgs(std::shared_ptr<IClientWorkerApi> &workerApi)
{
    // client should be at same site with worker by shmem
    workerApi = workerApi_[ObjectClientImpl::LOCAL_WORKER];
    CHECK_FAIL_RETURN_STATUS(workerApi != nullptr, K_INVALID, "no local worker api");
    workerApi->IncreaseInvokeCount();
    CHECK_FAIL_RETURN_STATUS(workerApi->IsShmEnable(), K_NOT_SUPPORTED,
                             "not support pipeline rh2d: shared memory is not enabled");
    CHECK_FAIL_RETURN_STATUS(workerApi->WorkerSupportPiplnRH2D(), K_NOT_SUPPORTED, "worker don't enable pipeline rh2d");

    // check connection
    RETURN_IF_NOT_OK(host_.checkConnection());
    return Status::OK();
}

std::shared_future<AsyncResult> BoundMode::GetWithOsTransportPipeline(
    const std::vector<std::string> &objectKeys, const std::vector<Blob> &devBlob,
    std::vector<std::shared_ptr<Buffer>> &buffers, void *h2dStream)
{
    ApiDeadlineGuard deadlineGuard(requestTimeoutMs_);
    auto asyncResource = std::make_shared<PipelineAsyncResource>();
    std::shared_future<AsyncResult> future = asyncResource->promise.get_future().share();

#ifdef BUILD_PIPLN_H2D
    PerfPoint perfPoint(PerfKey::PIPLN_RH2D_CLIENT_SUBMIT);
    if (asyncPipelineRH2DPool_ == nullptr) {
        Status rc(K_RUNTIME_ERROR, "Pipeline RH2D task pool is not initialized");
        asyncResource->promise.set_value({ rc, objectKeys });
        return future;
    }

    Status rc = CheckPipelineRH2DArgs(objectKeys, devBlob);
    if (rc.IsError()) {
        asyncResource->promise.set_value({ rc, objectKeys });
        LOG(ERROR) << rc.GetMsg();
        return future;
    }

    const auto localWorkerApi =
        workerApi_.size() > ObjectClientImpl::LOCAL_WORKER ? workerApi_[ObjectClientImpl::LOCAL_WORKER] : nullptr;
    const bool hasLocalWorker = localWorkerApi != nullptr && localWorkerApi->IsShmEnable();
    if (!hasLocalWorker && enableClientDirectPipelineH2D_) {
        auto traceContext = Trace::Instance().GetContext();
        int64_t apiRemainingUs = ApiDeadline::Instance().ApiRemainingUs();
        if (apiRemainingUs <= 0) {
            Status rc(K_RPC_DEADLINE_EXCEEDED, "API deadline exceeded before client direct RH2D dispatch");
            asyncResource->promise.set_value({ rc, objectKeys });
            return future;
        }
        buffers.assign(objectKeys.size(), nullptr);
        auto keysCopy = objectKeys;
        auto blobCopy = devBlob;
        auto dispatchTime = std::chrono::steady_clock::now();
        asyncResource->rpcFuture = asyncPipelineRH2DPool_->Submit(
            [this, asyncResource, traceContext, apiRemainingUs, dispatchTime, keys = std::move(keysCopy),
             blobs = std::move(blobCopy), &buffers, h2dStream]() {
                TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
                ApiDeadline::Instance().Push();
                Raii deadlineRaii([]() { ApiDeadline::Instance().Pop(); });
                Status rc = InitTimeoutsFromDispatch(apiRemainingUs, dispatchTime);
                std::vector<std::string> failedKeys;
                if (rc.IsOk()) {
                    rc = host_.runClientDirectPipelineRH2D(keys, blobs, buffers, h2dStream, failedKeys);
                } else {
                    failedKeys = keys;
                }
                asyncResource->promise.set_value({ rc, std::move(failedKeys) });
                return rc;
            });
        perfPoint.Record();
        return future;
    }

    std::shared_ptr<IClientWorkerApi> workerApi;
    rc = CheckLocalPipelineRH2DArgs(workerApi);
    if (rc.IsError()) {
        if (workerApi) {
            workerApi->DecreaseInvokeCount();
        }
        asyncResource->promise.set_value({ rc, objectKeys });
        LOG(ERROR) << rc.GetMsg();
        return future;
    }

    // copy params
    std::vector<OsXprtPipln::DevShmInfo> devInfos;
    for (size_t i = 0; i < objectKeys.size(); i++) {
        devInfos.emplace_back(OsXprtPipln::DevShmInfo{ OsXprtPipln::TargetDeviceType::CUDA, (uint32_t)-1,
                                                       devBlob[i].pointer, static_cast<size_t>(devBlob[i].size),
                                                       h2dStream });
    }
    asyncResource->piplnRh2dParam =
        PiplnRh2dParam{ .requestTimeoutMs = requestTimeoutMs_,
                        .objectKeys = objectKeys,
                        .devInfos = std::move(devInfos),
                        .chunkManager = std::make_shared<H2DChunkManager>(true /* isClient */),
                        .version = 0 };

    auto traceContext = Trace::Instance().GetContext();
    int64_t apiRemainingUs = ApiDeadline::Instance().ApiRemainingUs();
    if (apiRemainingUs <= 0) {
        Status rc(K_RPC_DEADLINE_EXCEEDED,
                  FormatString("API deadline exceeded before PipelineRH2D dispatch, remaining %ld us.",
                               apiRemainingUs));
        asyncResource->promise.set_value({ rc, objectKeys });
        LOG(ERROR) << rc.GetMsg();
        return future;
    }
    auto dispatchTime = std::chrono::steady_clock::now();
    asyncResource->rpcFuture = asyncPipelineRH2DPool_->Submit(
        [this, asyncResource, traceContext, workerApi, apiRemainingUs, dispatchTime, &buffers]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
        ApiDeadline::Instance().Push();
        Raii deadlineRaii([]() { ApiDeadline::Instance().Pop(); });
        std::unique_ptr<Raii> raii = std::make_unique<Raii>([workerApi]() { workerApi->DecreaseInvokeCount(); });
        auto initRc = InitTimeoutsFromDispatch(apiRemainingUs, dispatchTime);
        if (initRc.IsError()) {
            asyncResource->promise.set_value({ initRc, asyncResource->piplnRh2dParam.objectKeys });
            LOG(ERROR) << initRc.GetMsg();
            return initRc;
        }

        // do RH2D
        GetRspPb getRsp;
        Status ret = workerApi->PipelineRH2D(asyncResource->piplnRh2dParam, getRsp);
        if (ret.IsError()) {
            asyncResource->promise.set_value({ ret, asyncResource->piplnRh2dParam.objectKeys });
            return ret;
        }
        return host_.postPipelineRH2D(asyncResource->promise, asyncResource->piplnRh2dParam, getRsp, buffers);
    });
    perfPoint.Record();
#else
    (void)devBlob;
    (void)h2dStream;
    asyncResource->promise.set_value({ Status(K_NOT_SUPPORTED, "not build with BUILD_PIPLN_H2D"), objectKeys });
    (void)buffers;
#endif
    return future;
}

void BoundMode::BuildClientDirectRH2DReadRequest(const std::vector<std::string> &objectKeys,
                                                 client::ObjectReadRequest &request,
                                                 std::vector<Status> &itemStatuses, int64_t subTimeoutMs,
                                                 bool queryL2Cache)
{
    if (std::atomic_load(&routing_) != nullptr) {
        host_.buildTransportReadRequest(objectKeys, request, itemStatuses, subTimeoutMs, queryL2Cache);
        return;
    }
    HostPort worker;
    if (!enableLocalCache_ || host_.getCurrentWorkerHostPort(worker).IsError()) {
        host_.buildTransportReadRequest(objectKeys, request, itemStatuses, subTimeoutMs, queryL2Cache);
        return;
    }
    std::fill(itemStatuses.begin(), itemStatuses.end(), Status::OK());
    for (size_t i = 0; i < objectKeys.size(); ++i) {
        request.items.push_back({ i, objectKeys[i], worker });
    }
}

Status BoundMode::RecoverWorkerAndRetryGet(const std::shared_ptr<IClientWorkerApi> &workerApi,
                                           GetParam &getParam, WorkerNode workerNode,
                                           const std::vector<std::string> &objectKeys,
                                           std::vector<std::shared_ptr<Buffer>> &buffers)
{
    auto recoveryReason = client::WorkerRecoveryReason::CLIENT_REMOVED;
    auto recoveryStatus = workerNode == ObjectClientImpl::LOCAL_WORKER
                              ? failover_->ProcessWorkerLost(recoveryReason)
                              : failover_->ProcessStandbyWorkerLost(workerNode, recoveryReason);
    if (recoveryStatus.IsError()) {
        return recoveryStatus;
    }
    buffers.assign(objectKeys.size(), nullptr);
    return GetBuffersFromWorker(workerApi, getParam, buffers);
}

Status BoundMode::GetFromLocalWorker(const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                                     std::vector<std::shared_ptr<Buffer>> &buffers, bool queryL2Cache,
                                     bool isRH2DSupported, int32_t requestTimeoutMs)
{
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::unique_ptr<Raii> raii;
    WorkerNode workerNode;
    RETURN_IF_NOT_OK(getWorkerApiNode_(workerApi, raii, workerNode));
    GetParam getParam{ .objectKeys = objectKeys,
                       .subTimeoutMs = subTimeoutMs,
                       .readParams = {},
                       .queryL2Cache = queryL2Cache,
                       .isRH2DSupported = isRH2DSupported,
                       .requestTimeoutMs = requestTimeoutMs };
    auto rc = GetBuffersFromWorker(workerApi, getParam, buffers);
    if (rc.GetCode() == K_CLIENT_WORKER_DISCONNECT) {
        rc = RecoverWorkerAndRetryGet(workerApi, getParam, workerNode, objectKeys, buffers);
    }
    host_.handleDirectGetFailure(workerApi, rc);
    return rc;
}

Status BoundMode::SetShmObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info,
                                     uint32_t version, std::shared_ptr<Buffer> &buffer)
{
    // Validator check ids in Get(objectKeys, subTimeoutMs, buffers)
    std::shared_ptr<client::IMmapTableEntry> mmapEntry;
    uint8_t *pointer;
    RETURN_IF_NOT_OK(MmapShmUnit(info.store_fd(), info.mmap_size(), info.offset(), mmapEntry, pointer));
    FullParam param;
    param.writeMode = WriteMode(info.write_mode());
    param.consistencyType = ConsistencyType(info.consistency_type());
    param.cacheType = CacheType(info.cache_type());
    auto bufferInfo =
        MakeObjectBufferInfo(objectKey, pointer, info.data_size(), info.metadata_size(), param, info.is_seal(), version,
                             ShmKey::Intern(info.shm_id()), nullptr, std::move(mmapEntry));

    // Update shared memory reference count.
    memoryRefCount_->IncreaseRef(ShmKey::Intern(info.shm_id()));
    return Buffer::CreateBuffer(std::move(bufferInfo), host_.getSelf(), buffer);
}

Status BoundMode::MmapShmUnit(int64_t fd, uint64_t mmapSize, ptrdiff_t offset,
                              std::shared_ptr<client::IMmapTableEntry> &mmapEntry, uint8_t *&pointer)
{
    auto shmBuf = std::make_shared<ShmUnitInfo>();
    shmBuf->fd = fd;
    shmBuf->mmapSize = mmapSize;
    shmBuf->offset = offset;
    PerfPoint mmapPoint(PerfKey::CLIENT_LOOK_UP_MMAP_FD);
    RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd("", shmBuf));
    mmapEntry = mmapManager_->GetMmapEntryByFd(shmBuf->fd);
    CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr, StatusCode::K_RUNTIME_ERROR, "Get mmap entry failed");
    mmapPoint.Record();
    pointer = static_cast<uint8_t *>(shmBuf->pointer) + shmBuf->offset;
    return Status::OK();
}

std::shared_ptr<ObjectBufferInfo> BoundMode::MakeObjectBufferInfo(
    const std::string &objectKey, uint8_t *pointer, uint64_t size, uint64_t metaSize, const FullParam &param,
    bool isSeal, uint32_t version, const ShmKey &shmId, const std::shared_ptr<RpcMessage> &payloadPointer,
    std::shared_ptr<client::IMmapTableEntry> mmapEntry, std::shared_ptr<RemoteH2DHostInfoPb> remoteHostInfo)
{
    auto bufferInfo = std::make_shared<ObjectBufferInfo>();
    bufferInfo->objectKey = objectKey;
    bufferInfo->shmId = shmId;
    bufferInfo->pointer = pointer;
    bufferInfo->dataSize = size;
    bufferInfo->metadataSize = metaSize;
    bufferInfo->ttlSecond = param.ttlSecond;
    bufferInfo->existence = static_cast<int>(param.existence);
    bufferInfo->objectMode.SetWriteMode(param.writeMode);
    bufferInfo->objectMode.SetConsistencyType(param.consistencyType);
    bufferInfo->objectMode.SetCacheType(param.cacheType);
    bufferInfo->isSeal = isSeal;
    bufferInfo->version = version;
    bufferInfo->payloadPointer = payloadPointer;
    bufferInfo->mmapEntry = std::move(mmapEntry);
    (void)remoteHostInfo;
#ifdef BUILD_HETERO
    bufferInfo->remoteHostInfo = std::move(remoteHostInfo);
#endif
    return bufferInfo;
}

#ifdef USE_URMA
// Used when UB buffer overflow is detected to prevent downstream code from
// accessing removed payload entries via dangling part_index values.
static void ClearUBPayloadPlaceholders(GetRspPb &rsp, std::vector<RpcMessage> &payloads,
                                       size_t origPayloadSize)
{
    payloads.resize(origPayloadSize);
    for (int k = 0; k < rsp.payload_info_size(); ++k) {
        auto *pi = rsp.mutable_payload_info(k);
        if (pi->part_index_size() > 0 && pi->part_index(0) >= origPayloadSize) {
            pi->clear_part_index();
        }
    }
}
#endif

Status BoundMode::GetBuffersFromWorker(std::shared_ptr<IClientWorkerApi> workerApi, GetParam &getParam,
                                       std::vector<std::shared_ptr<Buffer>> &buffers)
{
    PerfPoint totalPoint(PerfKey::CLIENT_GET_BUFFERS_FROM_WORKER);
    PerfPoint stagePoint(PerfKey::CLIENT_GET_BUFFERS_FROM_WORKER_RPC);
    const std::vector<std::string> &objectsNeedToGet = getParam.objectKeys;
    const std::vector<ReadParam> &readParams = getParam.readParams;
    CHECK_FAIL_RETURN_STATUS(buffers.size() == objectsNeedToGet.size(), K_INVALID, "buffers size does not match");
    bool shouldRecordTransport = false;
    AccessTransportKind actualTransportKind = AccessTransportKind::SHM;
    getParam.actualTransportKind = nullptr;
    auto config = GetClientLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);

#ifdef USE_URMA
    // Happy path: use pre-configured data size to skip GetObjMetaInfo RPC.
    constexpr int BASE_DECIMAL = 10;
    uint64_t configuredUbSize = 0;
    {
        const char *envUbGetSize = std::getenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES");
        if (envUbGetSize != nullptr && envUbGetSize[0] != '\0') {
            configuredUbSize = std::strtoull(envUbGetSize, nullptr, BASE_DECIMAL);
        }
    }
    if (configuredUbSize > 0) {
        getParam.ubTotalSize = configuredUbSize;
        getParam.ubMetaResolved = true;
        getParam.ubGetObjMetaElapsedMs = 0;
        getParam.actualTransportKind = &actualTransportKind;
    }

    // For UB mode, pre-fetch object sizes via GetObjMetaInfo and split into batches if needed.
    if (IsUrmaEnabled() && workerApi != nullptr && !workerApi->IsShmEnable()
        && !(getParam.isRH2DSupported && IsRemoteH2DEnabled()) && configuredUbSize == 0) {
        shouldRecordTransport = true;
        std::vector<ObjMetaInfo> objMetas;
        std::string tenantId = GetRequestContext()->tenantId.empty() ? tenantId_ : GetRequestContext()->tenantId;
        Timer metaTimer;
        Status metaRc = workerApi->GetObjMetaInfo(tenantId, objectsNeedToGet, objMetas);
        getParam.ubGetObjMetaElapsedMs = static_cast<int64_t>(metaTimer.ElapsedMilliSecond());
        getParam.ubMetaResolved = true;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metaRc, "GetObjMetaInfo failed before UB get");
        if (objMetas.size() != objectsNeedToGet.size()) {
            LOG(WARNING) << "GetObjMetaInfo size mismatch, expected " << objectsNeedToGet.size()
                         << " but got " << objMetas.size() << ", fallback to TCP/IP payload before get.";
            actualTransportKind = AccessTransportKind::TCP;
        } else {
            uint64_t ubMaxGetSize = UrmaManager::Instance().GetUBMaxGetDataSize();
            uint64_t totalSize = 0;
            for (const auto &meta : objMetas) {
                totalSize += meta.objSize;
            }
            if (totalSize <= ubMaxGetSize) {
                // common case: everything fits in one buffer.
                getParam.ubTotalSize = totalSize;
                getParam.actualTransportKind = &actualTransportKind;
            } else {
                // batch special case: total size exceeds buffer limit.
                Status batchRc = GetBuffersFromWorkerBatched(workerApi, getParam, buffers, objMetas, ubMaxGetSize,
                                                             &actualTransportKind);
                AccessTransportTracker::Record(actualTransportKind);
                return batchRc;
            }
        }
    }
#endif

    GetRspPb rsp;
    std::vector<RpcMessage> payloads;
    uint32_t version = 0;

    std::unordered_map<std::string, std::shared_ptr<ObjectBufferInfo>> ubBufferInfos;

#ifdef USE_URMA
    std::shared_ptr<UrmaManager::BufferHandle> ubHandle;
    uint8_t *ubPtr = nullptr;
    uint64_t ubSize = 0;
    UrmaRemoteAddrPb urmaInfo;

    if (getParam.ubTotalSize > 0 && getParam.ubMetaResolved) {
        uint64_t ubMaxGetSize = UrmaManager::Instance().GetUBMaxGetDataSize();
        if (getParam.ubTotalSize <= ubMaxGetSize) {
            Status ubRc = UrmaManager::Instance().GetMemoryBufferHandle(ubHandle, getParam.ubTotalSize);
            if (ubRc.IsOk() && ubHandle != nullptr) {
                ubRc = UrmaManager::Instance().GetMemoryBufferInfo(ubHandle, ubPtr, ubSize, urmaInfo);
            }
            if (ubRc.IsOk()) {
                getParam.ubPreAllocHandle = ubHandle.get();
            } else {
                LOG(WARNING) << "UB buffer allocation failed: " << ubRc.ToString() << ", fallback to TCP";
                ubHandle.reset();
                ubPtr = nullptr;
            }
        }
    }
#endif

    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_RPC_START);
    }
    Status getRc = workerApi->Get(getParam, version, rsp, payloads);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_RPC_END);
    }
    if (shouldRecordTransport) {
        AccessTransportTracker::Record(actualTransportKind);
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(getRc, "Get error");
    stagePoint.RecordAndReset(PerfKey::CLIENT_GET_BUFFERS_FROM_WORKER_PROCESS_RESPONSE);

#ifdef USE_URMA
    if (ubHandle != nullptr) {
        uint64_t ubReadOffset = 0;
        size_t origPayloadSize = payloads.size();
        for (int i = 0; i < rsp.payload_info_size(); ++i) {
            auto *pi = rsp.mutable_payload_info(i);
            if (pi->part_index_size() != 0) continue;

            uint64_t dataSize = static_cast<uint64_t>(pi->data_size());
            if (ubReadOffset > ubSize || dataSize > ubSize - ubReadOffset) {
                LOG(ERROR) << "UB payload overflow, object " << pi->object_key()
                           << ", size " << dataSize << ", consumed " << ubReadOffset
                           << ", buffer " << ubSize;
                ClearUBPayloadPlaceholders(rsp, payloads, origPayloadSize);
                ubHandle.reset();
                ubBufferInfos.clear();
                break;
            }
            payloads.emplace_back();
            pi->add_part_index(payloads.size() - 1);

            std::string mapKey = pi->object_key().empty()
                ? objectsNeedToGet[pi->object_index()]
                : pi->object_key();
            FullParam param;
            param.writeMode = WriteMode(pi->write_mode());
            param.consistencyType = ConsistencyType(pi->consistency_type());
            param.cacheType = CacheType(pi->cache_type());
            auto bufferInfo = MakeObjectBufferInfo(
                mapKey, ubPtr + ubReadOffset, dataSize, 0, param,
                pi->is_seal(), version, {}, nullptr, nullptr, nullptr);
            bufferInfo->ubGetBufferHandle = std::shared_ptr<void>(ubHandle, ubHandle.get());
            ubBufferInfos[mapKey] = std::move(bufferInfo);
            ubReadOffset += dataSize;
        }
    }
#endif

    std::vector<std::string> failedObjectKey;
    failedObjectKey.reserve(objectsNeedToGet.size());
    RETURN_IF_NOT_OK(ProcessGetResponse(objectsNeedToGet, readParams, rsp, version, payloads,
        buffers, failedObjectKey, ubBufferInfos));

    // Derive the real medium from the response — payload_info that hits ubBufferInfos is UB,
    // otherwise TCP; objects() with store_fd stays SHM — mirroring GetObjectBuffers' routing.
    AccessTransportKind responseKind = AccessTransportKind::SHM;
    for (const auto &pi : rsp.payload_info()) {
        const std::string &k = pi.object_key().empty() ? objectsNeedToGet[pi.object_index()] : pi.object_key();
        const AccessTransportKind itemKind = (ubBufferInfos.find(k) != ubBufferInfos.end())
            ? AccessTransportKind::UB : AccessTransportKind::TCP;
        responseKind = static_cast<AccessTransportKind>(std::max(static_cast<uint8_t>(responseKind),
                                                                 static_cast<uint8_t>(itemKind)));
    }
    AccessTransportTracker::Record(responseKind);

    if (objectsNeedToGet.size() > failedObjectKey.size()) {
        totalPoint.Record();
        return Status::OK();
    }

    totalPoint.Record();
    return GetWorkerGetFailure(rsp, workerApi->hostPort_, "Cannot get objects from worker");
}

#ifdef USE_URMA
Status BoundMode::GetBuffersFromWorkerBatched(std::shared_ptr<IClientWorkerApi> workerApi,
                                              const GetParam &getParam,
                                              std::vector<std::shared_ptr<Buffer>> &buffers,
                                              const std::vector<ObjMetaInfo> &objMetas, uint64_t ubMaxGetSize,
                                              AccessTransportKind *requestTransportKind)
{
    PerfPoint totalPoint(PerfKey::CLIENT_GET_BUFFERS_FROM_WORKER);
    const auto &objectKeys = getParam.objectKeys;
    const auto &readParams = getParam.readParams;

    auto batches = BuildUBGetBatches(objMetas, ubMaxGetSize);
    LOG(INFO) << "UB batch Get: " << objectKeys.size() << " objects split into " << batches.size() << " batches";

    size_t totalSuccessCount = 0;
    Status lastError;

    for (const auto &batch : batches) {
        if (batch.indices.size() == 1 && objMetas[batch.indices[0]].objSize > ubMaxGetSize) {
            const size_t idx = batch.indices[0];
            Status rc = GetOversizedBufferFromWorkerByChunks(workerApi, getParam, idx, objMetas[idx].objSize,
                                                             ubMaxGetSize, buffers[idx], requestTransportKind);
            if (rc.IsError()) {
                LOG(WARNING) << "Chunked Get failed for " << objectKeys[idx] << ": " << rc.ToString();
                lastError = rc;
                continue;
            }
            totalSuccessCount++;
            continue;
        }

        std::vector<std::string> subKeys;
        subKeys.reserve(batch.indices.size());
        for (size_t idx : batch.indices) {
            subKeys.push_back(objectKeys[idx]);
        }

        std::vector<ReadParam> subReadParams;
        if (!readParams.empty()) {
            subReadParams.reserve(batch.indices.size());
            for (size_t idx : batch.indices) {
                subReadParams.push_back(readParams[idx]);
            }
        }

        std::vector<std::shared_ptr<Buffer>> subBuffers(batch.indices.size());
        AccessTransportKind batchTransportKind = AccessTransportKind::SHM;

        GetParam subGetParam{ .objectKeys = subKeys,
                              .subTimeoutMs = getParam.subTimeoutMs,
                              .readParams = subReadParams,
                              .queryL2Cache = getParam.queryL2Cache,
                              .isRH2DSupported = getParam.isRH2DSupported,
                              .ubTotalSize = batch.totalSize,
                              .ubMetaResolved = true,
                              .ubGetObjMetaElapsedMs = getParam.ubGetObjMetaElapsedMs,
                              .actualTransportKind = &batchTransportKind,
                              .requestTimeoutMs = getParam.requestTimeoutMs };

        GetRspPb rsp;
        std::vector<RpcMessage> payloads;
        uint32_t version = 0;

        PerfPoint stagePoint(PerfKey::CLIENT_GET_BUFFERS_FROM_WORKER_RPC);
        Status rc = workerApi->Get(subGetParam, version, rsp, payloads);
        if (requestTransportKind != nullptr) {
            *requestTransportKind = MergeTransportKind(*requestTransportKind, batchTransportKind);
        }
        if (rc.IsError()) {
            LOG(WARNING) << "Batch Get failed for " << subKeys.size() << " objects: " << rc.ToString();
            lastError = rc;
            continue;
        }
        stagePoint.RecordAndReset(PerfKey::CLIENT_GET_BUFFERS_FROM_WORKER_PROCESS_RESPONSE);

        std::vector<std::string> failedObjectKey;
        failedObjectKey.reserve(subKeys.size());
        rc = ProcessGetResponse(subKeys, subReadParams, rsp, version, payloads, subBuffers, failedObjectKey);
        if (rc.IsError()) {
            LOG(WARNING) << "ProcessGetResponse failed in batch: " << rc.ToString();
            lastError = rc;
            continue;
        }

        for (size_t k = 0; k < batch.indices.size(); ++k) {
            buffers[batch.indices[k]] = std::move(subBuffers[k]);
        }
        const size_t batchSuccessCount = subKeys.size() - failedObjectKey.size();
        if (batchSuccessCount == 0) {
            lastError = GetWorkerGetFailure(rsp, workerApi->hostPort_, "Cannot get objects from worker");
        }
        totalSuccessCount += batchSuccessCount;
    }

    if (totalSuccessCount > 0) {
        totalPoint.Record();
        return Status::OK();
    }
    totalPoint.Record();
    return lastError.IsOk() ? Status(K_NOT_FOUND, "Cannot get objects from worker") : lastError;
}
#endif

#ifdef USE_URMA
Status BoundMode::GetOversizedBufferFromWorkerByChunks(std::shared_ptr<IClientWorkerApi> workerApi,
                                                       const GetParam &getParam, size_t objectIndex,
                                                       uint64_t objectSize, uint64_t ubMaxGetSize,
                                                       std::shared_ptr<Buffer> &buffer,
                                                       AccessTransportKind *requestTransportKind)
{
    CHECK_FAIL_RETURN_STATUS(ubMaxGetSize > 0, K_INVALID, "UB max get size is 0");
    const auto &objectKey = getParam.objectKeys[objectIndex];
    OffsetInfo offsetInfo;
    if (!getParam.readParams.empty()) {
        CHECK_FAIL_RETURN_STATUS(
            objectIndex < getParam.readParams.size(), K_INVALID,
            FormatString("Read parameter index %zu is out of range %zu", objectIndex, getParam.readParams.size()));
        offsetInfo = OffsetInfo(getParam.readParams[objectIndex].offset, getParam.readParams[objectIndex].size);
    } else {
        offsetInfo = OffsetInfo(0, objectSize);
    }
    offsetInfo.AdjustReadSize(objectSize);
    FullParam param;
    auto bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, offsetInfo.readSize, 0, param, false, 0);
    std::shared_ptr<Buffer> mergedBuffer;
    RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), host_.getSelf(), mergedBuffer));

    uint64_t copiedSize = 0;
    uint32_t firstVersion = 0;
    bool hasVersion = false;
    while (copiedSize < offsetInfo.readSize) {
        uint64_t chunkSize = std::min(ubMaxGetSize, offsetInfo.readSize - copiedSize);
        std::shared_ptr<Buffer> chunkBuffer;
        uint32_t chunkVersion = 0;
        RETURN_IF_NOT_OK(GetOversizedBufferChunk(workerApi, getParam, objectKey, offsetInfo.readOffset + copiedSize,
                                                 chunkSize, chunkBuffer, chunkVersion, requestTransportKind));
        if (!hasVersion) {
            firstVersion = chunkVersion;
            hasVersion = true;
        } else {
            CHECK_FAIL_RETURN_STATUS(firstVersion == chunkVersion, K_RUNTIME_ERROR,
                                     FormatString("Object %s version changed during chunked Get, first %u, current %u",
                                                  objectKey, firstVersion, chunkVersion));
        }
        uint64_t realChunkSize = 0;
        RETURN_IF_NOT_OK(CopyOversizedBufferChunk(objectKey, offsetInfo.readSize, copiedSize, chunkBuffer, mergedBuffer,
                                                  realChunkSize));
        copiedSize += realChunkSize;
    }
    ObjectClientImpl::GetBufferInfo(mergedBuffer)->version = firstVersion;
    buffer = std::move(mergedBuffer);
    return Status::OK();
}
#endif

#ifdef USE_URMA
Status BoundMode::GetOversizedBufferChunk(std::shared_ptr<IClientWorkerApi> workerApi, const GetParam &getParam,
                                          const std::string &objectKey, uint64_t offset, uint64_t chunkSize,
                                          std::shared_ptr<Buffer> &chunkBuffer, uint32_t &version,
                                          AccessTransportKind *requestTransportKind)
{
    ReadParam readParam{ objectKey, offset, chunkSize };
    std::vector<std::string> subKeys{ objectKey };
    std::vector<ReadParam> subReadParams{ readParam };
    AccessTransportKind chunkTransportKind = AccessTransportKind::SHM;
    GetParam subGetParam{ .objectKeys = subKeys,
                          .subTimeoutMs = getParam.subTimeoutMs,
                          .readParams = subReadParams,
                          .queryL2Cache = getParam.queryL2Cache,
                          .isRH2DSupported = getParam.isRH2DSupported,
                          .ubTotalSize = chunkSize,
                          .ubMetaResolved = true,
                          .ubGetObjMetaElapsedMs = getParam.ubGetObjMetaElapsedMs,
                          .actualTransportKind = &chunkTransportKind,
                          .requestTimeoutMs = getParam.requestTimeoutMs };
    GetRspPb rsp;
    std::vector<RpcMessage> payloads;
    RETURN_IF_NOT_OK(workerApi->Get(subGetParam, version, rsp, payloads));
    if (requestTransportKind != nullptr) {
        *requestTransportKind = MergeTransportKind(*requestTransportKind, chunkTransportKind);
    }

    std::vector<std::shared_ptr<Buffer>> chunkBuffers(1);
    std::vector<std::string> failedObjectKey;
    RETURN_IF_NOT_OK(ProcessGetResponse(subKeys, subReadParams, rsp, version, payloads, chunkBuffers,
                                        failedObjectKey));
    if (!failedObjectKey.empty() || chunkBuffers[0] == nullptr) {
        return GetWorkerGetFailure(
            rsp, workerApi->hostPort_,
            FormatString("Cannot get chunk of object %s, offset %zu, size %zu", objectKey, offset, chunkSize));
    }
    chunkBuffer = std::move(chunkBuffers[0]);
    return Status::OK();
}
#endif

#ifdef USE_URMA
Status BoundMode::CopyOversizedBufferChunk(const std::string &objectKey, uint64_t objectSize, uint64_t offset,
                                           const std::shared_ptr<Buffer> &chunkBuffer,
                                           std::shared_ptr<Buffer> &buffer, uint64_t &copiedSize)
{
    auto chunkBufferSize = chunkBuffer->GetSize();
    CHECK_FAIL_RETURN_STATUS(chunkBufferSize >= 0, K_RUNTIME_ERROR,
                             FormatString("Chunk size is negative for object %s", objectKey));
    uint64_t realChunkSize = static_cast<uint64_t>(chunkBufferSize);
    CHECK_FAIL_RETURN_STATUS(realChunkSize > 0, K_RUNTIME_ERROR,
                             FormatString("Chunk size is zero for object %s, offset %zu", objectKey, offset));
    CHECK_FAIL_RETURN_STATUS(realChunkSize <= objectSize - offset, K_RUNTIME_ERROR,
                             FormatString("Chunk size %zu overflows object %s remaining size %zu", realChunkSize,
                                          objectKey, objectSize - offset));
    RETURN_IF_NOT_OK(::datasystem::MemoryCopy(static_cast<uint8_t *>(buffer->MutableData()) + offset,
                                              objectSize - offset,
                                              static_cast<const uint8_t *>(chunkBuffer->ImmutableData()),
                                              realChunkSize, memoryCopyThreadPool_));
    copiedSize = realChunkSize;
    return Status::OK();
}
#endif

Status BoundMode::ProcessGetResponse(const std::vector<std::string> &objectKeys,
                                     const std::vector<ReadParam> &readParams, GetRspPb &rsp,
                                     uint32_t version, std::vector<RpcMessage> &payloads,
                                     std::vector<std::shared_ptr<Buffer>> &buffers,
                                     std::vector<std::string> &failedObjectKey,
                                     const std::unordered_map<std::string,
                                                std::shared_ptr<ObjectBufferInfo>> &ubBufferInfos)
{
    size_t shmCount = static_cast<size_t>(rsp.objects().size());
    size_t noShmCount = static_cast<size_t>(rsp.payload_info().size());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        SIZE_MAX - shmCount >= noShmCount, K_RUNTIME_ERROR,
        FormatString("Sum overflow, shmCount:%zu + noShmCount:%zu > UINT_MAX:%zu", shmCount, noShmCount, SIZE_MAX));
    size_t payloadSum = 0;
    if (noShmCount > 0) {
        for (auto &p : rsp.payload_info()) {
            payloadSum += p.part_index().size();
        }
    }
    CHECK_FAIL_RETURN_STATUS(shmCount + noShmCount == objectKeys.size() && payloadSum == payloads.size(),
                             K_UNKNOWN_ERROR, "The response count in GetRspPb does not match with objects count.");
    RETURN_IF_NOT_OK(GetObjectBuffers(objectKeys, rsp, version, readParams, payloads, buffers, failedObjectKey,
                                      ubBufferInfos));

    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    if (recvRc.IsError()) {
        LOG(WARNING) << "Get request may have failed, status:" << recvRc.ToString()
                     << " failed id:" << VectorToString(failedObjectKey);
    } else if (!failedObjectKey.empty()) {
        LOG(WARNING) << "Not all expected objects were obtained, failed id:" << VectorToString(failedObjectKey);
    }
    return Status::OK();
}

Status BoundMode::GetObjectBuffers(const std::vector<std::string> &objectsNeedToGet, const GetRspPb &rsp,
                                   uint32_t version, const std::vector<ReadParam> &readParams,
                                   std::vector<RpcMessage> &payloads,
                                   std::vector<std::shared_ptr<Buffer>> &buffers,
                                   std::vector<std::string> &failedObjectKey,
                                   const std::unordered_map<std::string,
                                              std::shared_ptr<ObjectBufferInfo>> &ubBufferInfos)
{
    size_t i = 0;
    size_t j = 0;
    size_t shmCount = static_cast<size_t>(rsp.objects().size());
    size_t noShmCount = static_cast<size_t>(rsp.payload_info().size());
    for (size_t index = 0; index < objectsNeedToGet.size(); index++) {
        const std::string &objectKey = objectsNeedToGet[index];
        Status status;
        std::shared_ptr<Buffer> &bufferPtr = buffers[i + j];
        bool isShm = false;
        bool isNoShm = false;
        if (i < shmCount) {
            isShm = rsp.objects(i).object_key().empty() ? index == rsp.objects(i).object_index()
                                                        : objectKey == rsp.objects(i).object_key();
        }
        if (j < noShmCount) {
            isNoShm = rsp.payload_info(j).object_key().empty() ? index == rsp.payload_info(j).object_index()
                                                               : objectKey == rsp.payload_info(j).object_key();
        }
        if (isShm) {
            const GetRspPb::ObjectInfoPb &info = rsp.objects(i);
            i++;
            if (info.store_fd() == -1) {
                failedObjectKey.emplace_back(objectKey);
                continue;
            }
            status = SetShmObjectBufferWithMetric(objectKey, info, version, readParams, index, bufferPtr);
        } else if (isNoShm) {
            status = SetNoShmObjectBufferWithMetric(objectKey, rsp.payload_info(j), version, payloads,
                                                    ubBufferInfos, bufferPtr);
            j++;
        } else {
            RETURN_STATUS(K_UNKNOWN_ERROR, "Object key does not match with GetRspPb");
        }

        if (status.IsError()) {
            failedObjectKey.emplace_back(objectKey);
            bufferPtr = nullptr;
            LOG(ERROR) << "Failed for " << objectKey << " : " << status.ToString();
        }
    }
    return Status::OK();
}

Status BoundMode::SetShmObjectBufferWithMetric(const std::string &objectKey,
                                               const GetRspPb::ObjectInfoPb &info, uint32_t version,
                                               const std::vector<ReadParam> &readParams, size_t index,
                                               std::shared_ptr<Buffer> &bufferPtr)
{
    // Special case for Remote H2D scenario.
    if (info.has_host_info()) {
        return SetRemoteHostObjectBuffer(objectKey, info, version, bufferPtr);
    }
    if (readParams.empty()) {
        METRIC_ADD(metrics::KvMetricId::CLIENT_GET_SHM_READ_TOTAL_BYTES,
                   static_cast<uint64_t>(info.data_size()));
        return SetShmObjectBuffer(objectKey, info, version, bufferPtr);
    }
    uint64_t dataSize = static_cast<uint64_t>(info.data_size());
    OffsetInfo offsetInfo(readParams[index].offset, readParams[index].size);
    offsetInfo.AdjustReadSize(dataSize);
    if (offsetInfo.readSize > 0) {
        METRIC_ADD(metrics::KvMetricId::CLIENT_GET_SHM_READ_TOTAL_BYTES, offsetInfo.readSize);
    }
    return SetOffsetReadObjectBuffer(objectKey, info, version, readParams[index].offset,
                                     readParams[index].size, bufferPtr);
}

Status BoundMode::SetNoShmObjectBufferWithMetric(const std::string &objectKey,
                                                 const GetRspPb::PayloadInfoPb &payloadInfo,
                                                 uint32_t version, std::vector<RpcMessage> &payloads,
                                                 const std::unordered_map<std::string,
                                                            std::shared_ptr<ObjectBufferInfo>> &ubBufferInfos,
                                                 std::shared_ptr<Buffer> &bufferPtr)
{
    uint64_t dataSize = static_cast<uint64_t>(payloadInfo.data_size());
    auto it = ubBufferInfos.find(objectKey);
    if (it != ubBufferInfos.end()) {
        METRIC_ADD(metrics::KvMetricId::CLIENT_GET_URMA_READ_TOTAL_BYTES, dataSize);
        return Buffer::CreateBuffer(it->second, host_.getSelf(), bufferPtr);
    }
    METRIC_ADD(metrics::KvMetricId::CLIENT_GET_TCP_READ_TOTAL_BYTES, dataSize);
    return SetNonShmObjectBuffer(objectKey, payloadInfo, version, payloads, bufferPtr);
}

Status BoundMode::SetRemoteHostObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info,
                                            uint32_t version, std::shared_ptr<Buffer> &buffer)
{
    FullParam param;
    param.writeMode = WriteMode(info.write_mode());
    param.consistencyType = ConsistencyType(info.consistency_type());
    param.cacheType = CacheType(info.cache_type());
    auto hostInfo = std::make_shared<RemoteH2DHostInfoPb>();
    *hostInfo = std::move(info.host_info());
    auto bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, info.data_size(), info.metadata_size(), param,
                                           info.is_seal(), version, {}, nullptr, nullptr, hostInfo);
    return Buffer::CreateBuffer(bufferInfo, host_.getSelf(), buffer);
}

Status BoundMode::SetNonShmObjectBuffer(const std::string &objectKey, const GetRspPb::PayloadInfoPb &payloadInfo,
                                        int version, std::vector<RpcMessage> &payloads,
                                        std::shared_ptr<Buffer> &bufferPtr)
{
    FullParam param;
    param.writeMode = WriteMode(payloadInfo.write_mode());
    param.consistencyType = ConsistencyType(payloadInfo.consistency_type());
    param.cacheType = CacheType(payloadInfo.cache_type());
    int payloadIndexSize = payloadInfo.part_index().size();
    if (payloadIndexSize == 1) {
        std::shared_ptr<RpcMessage> payloadSharedPtr =
            std::make_shared<RpcMessage>(std::move(payloads[payloadInfo.part_index(0)]));
        auto bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, payloadInfo.data_size(), 0, param,
                                               payloadInfo.is_seal(), version, {}, payloadSharedPtr, nullptr);
        return Buffer::CreateBuffer(std::move(bufferInfo), host_.getSelf(), bufferPtr);
    } else {
        std::vector<RpcMessage> objectPayloads;
        for (int i = 0; i < payloadIndexSize; i++) {
            auto partIndex = payloadInfo.part_index(i);
            if (partIndex >= payloads.size()) {
                RETURN_STATUS(K_UNKNOWN_ERROR,
                              "The response payload_index in GetRspPb exceeds the response payloads size.");
            }
            objectPayloads.emplace_back(std::move(payloads[partIndex]));
        }
        auto bufferInfo = MakeObjectBufferInfo(objectKey, nullptr, payloadInfo.data_size(), 0, param,
                                               payloadInfo.is_seal(), version, {}, nullptr, nullptr);
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), host_.getSelf(), bufferPtr));
        size_t offset = 0;
        for (const auto &part : objectPayloads) {
            const auto length = part.Size();
            const auto destSize = std::min(bufferPtr->GetSize() - offset, length);
            if (destSize < length) {
                RETURN_STATUS(
                    StatusCode::K_RUNTIME_ERROR,
                    FormatString(
                        "SetNonShmObjectBuffer failed because the MemoryCopy dst size: %zu smaller than src size: %zu",
                        destSize, length));
            }
            Status status =
                ::datasystem::MemoryCopy(static_cast<uint8_t *>(bufferPtr->MutableData()) + offset, destSize,
                                         static_cast<const uint8_t *>(part.Data()), length, memoryCopyThreadPool_);
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                status.IsOk(), K_RUNTIME_ERROR, FormatString("Copy data to buffer failed, err: %s", status.ToString()));
            offset += length;
        }
        return Status::OK();
    }
}

Status BoundMode::SetOffsetReadObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info,
                                            uint32_t version, uint64_t offset, uint64_t size,
                                            std::shared_ptr<Buffer> &buffer)
{
    uint64_t dataSize = static_cast<uint64_t>(info.data_size());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(offset < dataSize, K_RUNTIME_ERROR,
                                         FormatString("The read offset %zu out of range [0,%zu)", offset, dataSize));
    OffsetInfo offsetInfo(offset, size);
    offsetInfo.AdjustReadSize(dataSize);

    std::shared_ptr<client::IMmapTableEntry> mmapEntry;
    uint8_t *pointer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(MmapShmUnit(info.store_fd(), info.mmap_size(), info.offset(), mmapEntry, pointer),
                                     "MmapShmUnit failed for offset read.");
    FullParam param;
    param.writeMode = WriteMode(info.write_mode());
    param.consistencyType = ConsistencyType(info.consistency_type());
    param.cacheType = CacheType(info.cache_type());
    auto bufferInfo =
        MakeObjectBufferInfo(objectKey, pointer, info.data_size(), info.metadata_size(), param, info.is_seal(), version,
                             ShmKey::Intern(info.shm_id()), nullptr, std::move(mmapEntry));

    // Update shared memory reference count.
    std::shared_ptr<Buffer> tmpbuffer;
    {
        memoryRefCount_->IncreaseRef(ShmKey::Intern(info.shm_id()));
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), host_.getSelf(), tmpbuffer));
    }

    auto readBufferInfo = MakeObjectBufferInfo(objectKey, nullptr, offsetInfo.readSize, 0, param, info.is_seal(),
                                               version, {}, nullptr, nullptr);
    RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(readBufferInfo), host_.getSelf(), buffer));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        buffer->MemoryCopy(static_cast<uint8_t *>(tmpbuffer->MutableData()) + offset, offsetInfo.readSize),
        "Memory copy failed.");
    return Status::OK();
}

Status BoundMode::GIncreaseRef(const std::vector<std::string> &objectKeys,
                               std::vector<std::string> &failedObjectKeys, const std::string &remoteClientId)
{
    PerfPoint point(PerfKey::CLIENT_GINCREASE_REFERENCE);
    std::shared_lock<std::shared_timed_mutex> shutdownLck(shutdownMux_);
    RETURN_IF_NOT_OK(host_.isClientReady());
    RETURN_IF_NOT_OK(ObjectClientImpl::CheckValidObjectKeyVector(objectKeys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(failedObjectKeys.empty(), K_INVALID, "The failedObjectKeys not empty");
    RETURN_IF_NOT_OK(host_.checkConnection());

    if (!remoteClientId.empty()) {
        CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(simpleIdRe_, remoteClientId), K_INVALID,
                                 "The remoteClientId contains illegal char(s).");
        auto rc = workerApi_[ObjectClientImpl::LOCAL_WORKER]->GIncreaseWorkerRef(objectKeys,
                                                                                 failedObjectKeys, remoteClientId);
        VLOG(1) << "[Ref] Global ref count GIncreaseRef end" << VectorToString(objectKeys);
        if (!failedObjectKeys.empty()) {
            std::unordered_set<std::string> requestedObjectKeys;
            requestedObjectKeys.reserve(objectKeys.size());
            (void)requestedObjectKeys.insert(objectKeys.begin(), objectKeys.end());
            std::unordered_set<std::string> failedObjectKeySet;
            failedObjectKeySet.reserve(failedObjectKeys.size());
            (void)failedObjectKeySet.insert(failedObjectKeys.begin(), failedObjectKeys.end());
            return requestedObjectKeys.size() > failedObjectKeySet.size() ? Status::OK() : rc;
        }
        return rc;
    }

    std::map<std::string, GlobalRefInfo> accessorTable;  // Need sorted map to lock tbb data.
    std::shared_lock<std::shared_timed_mutex> lck(*globalRefMutex_);
    std::unordered_map<std::string, std::string> objWithTenantIdsToObjKey;
    AddTbbLockForGlobalRefIds(objectKeys, accessorTable, objWithTenantIdsToObjKey);

    std::vector<std::string> firstIncIds;
    VLOG(2) << "[Ref] RunTime GIncreaseRef object list: " << VectorToString(objectKeys);  // vlog level 2 means internal
    for (const auto &kv : accessorTable) {
        auto &accessor = *kv.second.second;
        int count = kv.second.first;
        TbbGlobalRefTable::value_type valuePair(kv.first, count);
        bool result = globalRefCount_->insert(accessor, valuePair);
        if (!result) {
            accessor->second += count;
        }
        if ((accessor->second - count) == 0) {
            firstIncIds.emplace_back(objWithTenantIdsToObjKey[kv.first]);
        }
    }

    RETURN_OK_IF_TRUE(firstIncIds.empty());

    VLOG(1) << "[Ref] Global ref count change from 0 to 1 list: " << VectorToString(firstIncIds);

    auto rc = workerApi_[ObjectClientImpl::LOCAL_WORKER]->GIncreaseWorkerRef(firstIncIds, failedObjectKeys);
    if (!failedObjectKeys.empty()) {
        GIncreaseRefRollback(failedObjectKeys, accessorTable);
    }

    // Return ok on partial success.
    return accessorTable.size() > failedObjectKeys.size() ? Status::OK() : rc;
}

std::string BoundMode::ConstructObjKeyWithTenantId(const std::string &objKey)
{
    std::string objKeyWithTenant = objKey;
    std::string tenantId;
    if (!token_.Empty()) {
        tenantId = "";
    } else if (GetRequestContext()->tenantId.empty()) {
        tenantId = tenantId_;
    } else {
        tenantId = GetRequestContext()->tenantId;
    }
    if (!tenantId.empty()) {
        objKeyWithTenant = GetRequestContext()->tenantId + K_SEPARATOR + objKey;
    }
    return objKeyWithTenant;
}

void BoundMode::GIncreaseRefRollback(const std::vector<std::string> &rollbackObjectKeys,
                                     std::map<std::string, GlobalRefInfo> &accessorTable)
{
    // Reset fail ref count.
    for (const auto &objectKey : rollbackObjectKeys) {
        auto objWithTenant = ConstructObjKeyWithTenantId(objectKey);
        auto it = accessorTable.find(objWithTenant);
        if (it == accessorTable.end()) {
            LOG(WARNING) << "Unknown object key " << objWithTenant;
            continue;
        }

        int count = it->second.first;
        auto &accessor = *it->second.second;
        accessor->second -= count;
        if (accessor->second <= 0) {
            (void)globalRefCount_->erase(accessor);
        }
    }

    LOG(WARNING) << "[Ref] failed GIncreaseRef objectKeys " << VectorToString(rollbackObjectKeys);
}

Status BoundMode::ReleaseGRefs(const std::string &remoteClientId)
{
    RETURN_IF_NOT_OK(host_.isClientReady());
    if (remoteClientId.empty()) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(simpleIdRe_, remoteClientId), K_INVALID,
                             "The remoteClientId contains illegal char(s).");
    RETURN_IF_NOT_OK(workerApi_[ObjectClientImpl::LOCAL_WORKER]->ReleaseGRefs(remoteClientId));
    return Status::OK();
}

Status BoundMode::GDecreaseRef(const std::vector<std::string> &objectKeys,
                               std::vector<std::string> &failedObjectKeys, const std::string &remoteClientId)
{
    PerfPoint point(PerfKey::CLIENT_GDECREASE_REFERENCE);
    RETURN_IF_NOT_OK(host_.isClientReady());
    for (auto &objectKey : objectKeys) {
        RETURN_IF_NOT_OK(ObjectClientImpl::CheckValidObjectKey(objectKey));
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(objectKeys.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(failedObjectKeys.empty(), K_RUNTIME_ERROR, "The failedObjectKeys not empty");
    RETURN_IF_NOT_OK(host_.checkConnection());

    if (!remoteClientId.empty()) {
        CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(simpleIdRe_, remoteClientId), K_INVALID,
                                 "The remoteClientId contains illegal char(s).");
        auto rc = workerApi_[ObjectClientImpl::LOCAL_WORKER]->GDecreaseWorkerRef(objectKeys,
                                                                                 failedObjectKeys, remoteClientId);
        VLOG(1) << "[Ref] Global ref count GDecreaseRef end " << VectorToString(objectKeys);
        return rc;
    }

    std::map<std::string, GlobalRefInfo> accessorTable;  // Need sorted map to lock tbb data.
    std::shared_lock<std::shared_timed_mutex> lck(*globalRefMutex_);
    std::unordered_map<std::string, std::string> objWithTenantIdsToObjKey;
    AddTbbLockForGlobalRefIds(objectKeys, accessorTable, objWithTenantIdsToObjKey);
    VLOG(2) << "[Ref] RunTime GDecreaseRef object list: " << VectorToString(objectKeys);  // vlog level 2 means internal

    std::vector<std::string> finishDecIds;
    for (const auto &kv : accessorTable) {
        auto &accessor = *kv.second.second;
        int count = kv.second.first;
        if (!(globalRefCount_->find(accessor, kv.first))) {
            LOG(WARNING) << FormatString("The objectKey id (%s) does not exist.", kv.first);
            continue;
        }
        // reference count change from n to 0 or negative.
        if (accessor->second > 0 && accessor->second <= count) {
            finishDecIds.emplace_back(objWithTenantIdsToObjKey[kv.first]);
        }

        if (accessor->second < count) {
            LOG(WARNING) << FormatString("GDecrease %s, dec num is %d, cur num is %d", kv.first, count,
                                         accessor->second);
        }
        accessor->second -= count;
    }

    RETURN_OK_IF_TRUE(finishDecIds.empty());

    VLOG(1) << "[Ref] Global ref count change from 1 to 0 list :" << VectorToString(finishDecIds);
    Status rc = workerApi_[ObjectClientImpl::LOCAL_WORKER]->GDecreaseWorkerRef(finishDecIds, failedObjectKeys);
    if (!failedObjectKeys.empty()) {
        GDecreaseRefRollback(failedObjectKeys, accessorTable);
    }

    RemoveZeroGlobalRefByRefTable(finishDecIds, accessorTable);

    // Return ok on partial success.
    return accessorTable.size() > failedObjectKeys.size() ? Status::OK() : rc;
}

void BoundMode::GDecreaseRefRollback(const std::vector<std::string> &rollbackObjectKeys,
                                     std::map<std::string, GlobalRefInfo> &accessorTable)
{
    // Reset fail ref count.
    for (const auto &objectKey : rollbackObjectKeys) {
        auto objWithTenant = ConstructObjKeyWithTenantId(objectKey);
        auto it = accessorTable.find(objWithTenant);
        if (it == accessorTable.end()) {
            LOG(WARNING) << "Unknown object key " << objWithTenant;
            continue;
        }

        int count = it->second.first;
        auto &accessor = *it->second.second;
        // if not exists in globalRefCount_
        if (accessor.empty()) {
            continue;
        }

        accessor->second += count;
    }

    LOG(WARNING) << "[Ref] failed GDecreaseRef objectKeys " << VectorToString(rollbackObjectKeys);
}

void BoundMode::RemoveZeroGlobalRefByRefTable(const std::vector<std::string> &checkIds,
                                              std::map<std::string, GlobalRefInfo> &accessorTable)
{
    for (const auto &objectKey : checkIds) {
        auto objWithTenant = ConstructObjKeyWithTenantId(objectKey);
        auto it = accessorTable.find(objWithTenant);
        if (it == accessorTable.end()) {
            LOG(WARNING) << "Unknown object key " << objWithTenant;
            continue;
        }
        auto &accessor = *(it->second.second);
        if (accessor->second <= 0) {
            (void)globalRefCount_->erase(accessor);
        }
    }
}

void BoundMode::AddTbbLockForGlobalRefIds(const std::vector<std::string> &objectKeys,
                                          std::map<std::string, GlobalRefInfo> &accessorTable,
                                          std::unordered_map<std::string, std::string> &objTenantIdsToObj)
{
    std::for_each(objectKeys.begin(), objectKeys.end(),
                  [this, &accessorTable, &objTenantIdsToObj](const std::string &objKey) {
                      auto objWithTenant = ConstructObjKeyWithTenantId(objKey);
                      auto it = accessorTable.find(objWithTenant);
                      if (it == accessorTable.end()) {
                          objTenantIdsToObj[objWithTenant] = objKey;
                          auto accessorPtr = std::make_shared<TbbGlobalRefTable::accessor>();
                          (void)accessorTable.emplace(objWithTenant, std::make_pair(1, std::move(accessorPtr)));
                      } else {
                          it->second.first++;
                      }
                  });
}

Status BoundMode::MutiCreateParallel(const bool skipCheckExistence, const FullParam &param,
                                     const uint32_t &version, std::vector<bool> &exists,
                                     std::vector<MultiCreateParam> &multiCreateParamList,
                                     std::vector<std::shared_ptr<Buffer>> &bufferList)
{
    const int sz = static_cast<int>(multiCreateParamList.size());
    auto multicreate = [&, this](size_t start, size_t end) {
        for (size_t i = start; i < end; i++) {
            RETURN_IF_NOT_OK(CreateBufferForMultiCreateParamAtIndex(i, skipCheckExistence, param, version, exists,
                                                                    multiCreateParamList, bufferList));
        }
        return Status::OK();
    };
    static const int parallelThreshold = 128;
    bool isParallel = multiCreateParamList.size() > parallelThreshold;
    if (!isParallel || parallismNum_ == 0) {
        return multicreate(0, sz);
    }
    static const int parallism = 4;
    return Parallel::ParallelFor<size_t>(0, multiCreateParamList.size(), multicreate, 0, parallism);
}

Status BoundMode::CreateBufferForMultiCreateParamAtIndex(size_t index, bool skipCheckExistence,
                                                         const FullParam &param, uint32_t version,
                                                         const std::vector<bool> &exists,
                                                         std::vector<MultiCreateParam> &multiCreateParamList,
                                                         std::vector<std::shared_ptr<Buffer>> &bufferList)
{
    Status injectRC = Status::OK();
    auto &createParam = multiCreateParamList[index];
    if (!skipCheckExistence && exists[createParam.index]) {
        auto bufferInfo = MakeObjectBufferInfo(createParam.objectKey, nullptr, 0, 0, param, false, 0);
        std::shared_ptr<Buffer> placeholder;
        RETURN_IF_NOT_OK(Buffer::CreateBuffer(bufferInfo, host_.getSelf(), placeholder));
        bufferList[createParam.index] = std::move(placeholder);
        return Status::OK();
    }
    auto &shmBuf = createParam.shmBuf;
    std::shared_ptr<ObjectBufferInfo> bufferInfo = nullptr;
#ifdef USE_URMA
    if (createParam.urmaDataInfo) {
        bufferInfo = MakeObjectBufferInfo(createParam.objectKey, nullptr, createParam.dataSize, 0, param, false,
                                          version, shmBuf->id);
        bufferInfo->ubUrmaDataInfo = createParam.urmaDataInfo;
    } else
#endif
    {
        PerfPoint mmapPoint(PerfKey::CLIENT_MULTI_CREATE_GET_MMAP);
        RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd("", shmBuf));
        auto mmapEntry = mmapManager_->GetMmapEntryByFd(shmBuf->fd);
        CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr, StatusCode::K_RUNTIME_ERROR, "Get mmap entry failed");
        mmapPoint.Record();

        bufferInfo = MakeObjectBufferInfo(createParam.objectKey, (uint8_t *)(shmBuf->pointer) + shmBuf->offset,
                                          createParam.dataSize, createParam.metadataSize, param, false, version,
                                          shmBuf->id, nullptr, std::move(mmapEntry));
    }
    PerfPoint refPoint(PerfKey::CLIENT_MEMORY_REF_ADD);
    memoryRefCount_->IncreaseRef(shmBuf->id);
    refPoint.Record();
    INJECT_POINT("ObjectClientImpl.MultiCreate.mmapFailed", [&bufferList, &injectRC](int failedIndex) {
        if (bufferList[failedIndex] != nullptr) {
            injectRC = Status(StatusCode::K_RUNTIME_ERROR, "Set runtime error");
        }
        return Status::OK();
    });
    RETURN_IF_NOT_OK(injectRC);
    PerfPoint point(PerfKey::CLIENT_MULTI_CREATE_BUFFER_CREATE);
    std::shared_ptr<Buffer> newBuffer;
    RETURN_IF_NOT_OK(Buffer::CreateBuffer(std::move(bufferInfo), host_.getSelf(), newBuffer));
    bufferList[createParam.index] = std::move(newBuffer);
    return Status::OK();
}

Status BoundMode::MemoryCopyParallel(bool isParallel, const std::vector<std::string> &keys,
                                     const std::vector<StringView> &vals, const FullParam &createParam,
                                     std::vector<std::shared_ptr<Buffer>> &bufferList,
                                     std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfoList,
                                     AccessTransportKind *requestTransportKind)
{
    const int sz = static_cast<int>(bufferList.size());
    INJECT_POINT("ObjectClientImpl.MemoryCopyParallel.slow");
    std::atomic<AccessTransportKind> aggregatedTransport(AccessTransportKind::SHM);
    auto memoryCopy = [&](int start, int end) {
        for (int i = start; i < end; i++) {
            auto &buffer = bufferList[i];
            if (buffer == nullptr) {
                bufferInfoList[i] =
                    MakeObjectBufferInfo(keys[i], reinterpret_cast<uint8_t *>(const_cast<char *>(vals[i].data())),
                                         vals[i].size(), 0, createParam, false, 0);
                continue;
            }
            RETURN_IF_NOT_OK(buffer->CheckDeprecated());
            CHECK_FAIL_RETURN_STATUS(!buffer->bufferInfo_->isSeal, K_OC_ALREADY_SEALED,
                                     "Client object is already sealed");
            AccessTransportKind actualTransportKind = AccessTransportKind::SHM;
            uint8_t transportKindValue = static_cast<uint8_t>(AccessTransportKind::SHM);
            RETURN_IF_NOT_OK(buffer->MemoryCopyWithTransport(
                vals[i].data(), vals[i].size(), requestTransportKind != nullptr ? &transportKindValue : nullptr));
            if (requestTransportKind != nullptr) {
                actualTransportKind = static_cast<AccessTransportKind>(transportKindValue);
                MergeTransportKind(aggregatedTransport, actualTransportKind);
            }
            bufferInfoList[i] = buffer->bufferInfo_;
        }
        return Status::OK();
    };
    Status rc;
    if (!isParallel || parallismNum_ == 0) {
        rc = memoryCopy(0, sz);
    } else {
        int workerNum = parallismNum_;
        size_t chunkSize = 4;
        if (sz <= parallismNum_) {
            workerNum = sz;
            chunkSize = 1;
        }
        rc = Parallel::ParallelFor<size_t>(0, bufferInfoList.size(), memoryCopy, chunkSize, workerNum);
    }
    if (rc.IsOk() && requestTransportKind != nullptr) {
        *requestTransportKind = aggregatedTransport.load(std::memory_order_relaxed);
    }
    return rc;
}

Status BoundMode::MemoryCopyParallelWithDeadline(bool isParallel, const std::vector<std::string> &keys,
                                                 const std::vector<StringView> &vals,
                                                 const FullParam &createParam,
                                                 std::vector<std::shared_ptr<Buffer>> &bufferList,
                                                 std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfoList,
                                                 uint64_t dataSizeSum, AccessTransportKind *requestTransportKind)
{
    RETURN_IF_NOT_OK(ApiDeadline::Instance().CheckApiDeadline());
    Timer memCopyTimer;
    auto memCopyRc =
        MemoryCopyParallel(isParallel, keys, vals, createParam, bufferList, bufferInfoList, requestTransportKind);
    int64_t memCopyCostUs = memCopyTimer.ElapsedMicroSecond();
    int64_t memCopyRemainingUs = ApiDeadline::Instance().ApiRemainingUs();
    SLOW_LOG_IF_OR_VLOG(
        INFO, memCopyCostUs >= TimeoutDuration::SLOW_PATH_LOG_THRESHOLD_US || memCopyRc.IsError(), 1,
        FormatString("[MSet] phase=MemoryCopyParallel costUs=%lld remainingUs=%lld size=%zu keys=%zu rc=%s",
                     memCopyCostUs, memCopyRemainingUs, dataSizeSum, keys.size(), memCopyRc.ToString()));
    RETURN_IF_NOT_OK(memCopyRc);
    return ApiDeadline::Instance().CheckApiDeadline();
}

Status BoundMode::MSetCreateCopyAndPublish(const std::vector<std::string> &keys,
                                           const std::vector<StringView> &vals,
                                           const std::vector<std::string> &deduplicateKeys,
                                           const std::vector<StringView> &deduplicateVals,
                                           const MSetParam &param,
                                           const std::shared_ptr<IClientWorkerApi> &workerApi,
                                           std::vector<std::string> &outFailedKeys, PerfPoint &point)
{
    LOG(INFO) << "Begin to multiput object." << VectorToString(keys);
    FullParam createParam;
    createParam.writeMode = param.writeMode;
    createParam.consistencyType = ConsistencyType::CAUSAL;
    createParam.cacheType = param.cacheType;
    const std::vector<std::string> &filteredKeys = deduplicateKeys.empty() ? keys : deduplicateKeys;
    const std::vector<StringView> &filteredValues = deduplicateVals.empty() ? vals : deduplicateVals;
    point.RecordAndReset(PerfKey::CLIENT_MSET_MULTICREATE);
    std::vector<uint64_t> dataSizeList;
    uint64_t dataSizeSum = 0;
    ComputeDataSizes(filteredValues, dataSizeList, dataSizeSum);
    std::vector<std::shared_ptr<Buffer>> bufferList;
    std::vector<bool> exist;
    RETURN_IF_NOT_OK(host_.multiCreate(filteredKeys, dataSizeList, createParam, true, bufferList, exist));
    std::vector<std::shared_ptr<ObjectBufferInfo>> bufferInfoList(bufferList.size());
    static const int minSizeThreshold = 500 * KB;
    static const int sizeThreshold = 4 * MB_TO_BYTES;
    static const int countThreshold = 32;
    bool isParallel =
        dataSizeSum > minSizeThreshold && (dataSizeSum >= sizeThreshold || filteredKeys.size() >= countThreshold);
    point.RecordAndReset(PerfKey::CLIENT_MSET_MEMCOPY);
    AccessTransportKind requestTransportKind = AccessTransportKind::SHM;
    RETURN_IF_NOT_OK(MemoryCopyParallelWithDeadline(isParallel, filteredKeys, filteredValues, createParam, bufferList,
                                                    bufferInfoList, dataSizeSum, &requestTransportKind));
    AccessTransportTracker::Record(requestTransportKind);
    point.RecordAndReset(PerfKey::CLIENT_MSET_MULTI_PUBLISH);
    MultiPublishRspPb rsp;
    PublishParam publishParam{
        .isReplica = false, .existence = param.existence, .ttlSecond = param.ttlSecond
    };
    RETURN_IF_NOT_OK(workerApi->MultiPublish(bufferInfoList, publishParam, rsp));
    point.RecordAndReset(PerfKey::CLIENT_MSET_POST_PROCESS);
    auto status = host_.handleShmRefCountAfterMultiPublish(bufferList, rsp);
    for (const auto &objKey : rsp.failed_object_keys()) {
        outFailedKeys.emplace_back(objKey);
    }
    if (filteredKeys.size() > outFailedKeys.size()) {
        return Status::OK();
    }
    return status;
}

}  // namespace object_cache
}  // namespace datasystem
