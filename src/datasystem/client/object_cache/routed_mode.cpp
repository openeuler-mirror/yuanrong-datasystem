/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/client/object_cache/routed_mode.h"

#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/client/object_cache/worker_failover.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/util/memory.h"

namespace datasystem {
namespace object_cache {


namespace {

constexpr size_t DRAINING_LOCATION_REFRESH_ATTEMPTS = 3;
constexpr int64_t DRAINING_LOCATION_REFRESH_INITIAL_BACKOFF_MS = 1;
constexpr int64_t STALE_LOCATION_REFRESH_INITIAL_BACKOFF_MS = 20;


constexpr int TRANSPORT_DIAG_LOG_RATE = 100;

enum class TransportReadRetryPolicy : uint8_t { NONE, DRAINING, STALE };

struct TransportReadRetryState {
    size_t outputIndex = 0;
    TransportReadRetryPolicy policy = TransportReadRetryPolicy::NONE;
    uint8_t drainingRetryCount = 0;
    uint8_t staleRetryCount = 0;
    int64_t drainingBackoffMs = DRAINING_LOCATION_REFRESH_INITIAL_BACKOFF_MS;
    int64_t staleBackoffMs = STALE_LOCATION_REFRESH_INITIAL_BACKOFF_MS;
};

struct TransportReadRoundResult {
    std::vector<std::shared_ptr<Buffer>> buffers;
    std::vector<Status> statuses;
};

TransportReadRetryPolicy ClassifyTransportReadRetry(const Status &status)
{
    if (client::IsWorkerDrainingForScaleIn(status)) {
        return TransportReadRetryPolicy::DRAINING;
    }
    if (client::IsTransportSnapshotStaleLocation(status)) {
        return TransportReadRetryPolicy::STALE;
    }
    return TransportReadRetryPolicy::NONE;
}

size_t TransportReadRetryLimit(TransportReadRetryPolicy policy)
{
    return policy == TransportReadRetryPolicy::DRAINING ? DRAINING_LOCATION_REFRESH_ATTEMPTS
                                                        : STALE_LOCATION_REFRESH_ATTEMPTS;
}

uint8_t TransportReadRetryCount(const TransportReadRetryState &state)
{
    return state.policy == TransportReadRetryPolicy::DRAINING ? state.drainingRetryCount : state.staleRetryCount;
}

uint8_t &TransportReadRetryCount(TransportReadRetryState &state)
{
    return state.policy == TransportReadRetryPolicy::DRAINING ? state.drainingRetryCount : state.staleRetryCount;
}

int64_t &TransportReadRetryBackoffMs(TransportReadRetryState &state)
{
    return state.policy == TransportReadRetryPolicy::DRAINING ? state.drainingBackoffMs : state.staleBackoffMs;
}

void UpdateTransportReadRetryState(const Status &status, TransportReadRetryState &state)
{
    state.policy = ClassifyTransportReadRetry(status);
}

void CollectInitialTransportReadRetryStates(const std::vector<Status> &itemStatuses,
                                            std::vector<TransportReadRetryState> &retryStates)
{
    for (size_t i = 0; i < itemStatuses.size(); ++i) {
        if (itemStatuses[i].IsOk()) {
            continue;
        }
        const auto policy = ClassifyTransportReadRetry(itemStatuses[i]);
        if (policy != TransportReadRetryPolicy::NONE) {
            TransportReadRetryState state;
            state.outputIndex = i;
            state.policy = policy;
            retryStates.emplace_back(std::move(state));
        }
    }
}

void CollectRetryTransportReadRound(const std::vector<size_t> &pendingStateIndexes,
                                    TransportReadRoundResult &roundResult,
                                    std::vector<std::shared_ptr<Buffer>> &buffers,
                                    std::vector<Status> &itemStatuses,
                                    std::vector<TransportReadRetryState> &retryStates)
{
    for (size_t i = 0; i < pendingStateIndexes.size(); ++i) {
        auto &state = retryStates[pendingStateIndexes[i]];
        itemStatuses[state.outputIndex] = roundResult.statuses[i];
        if (roundResult.statuses[i].IsOk()) {
            buffers[state.outputIndex] = std::move(roundResult.buffers[i]);
        }
        UpdateTransportReadRetryState(roundResult.statuses[i], state);
    }
}

bool CanRetryTransportRead(const TransportReadRetryState &state)
{
    return state.policy != TransportReadRetryPolicy::NONE &&
           TransportReadRetryCount(state) < TransportReadRetryLimit(state.policy);
}

std::vector<size_t> BuildNextTransportReadRetry(const std::vector<TransportReadRetryState> &states)
{
    TransportReadRetryPolicy policy = TransportReadRetryPolicy::NONE;
    uint8_t retryCount = std::numeric_limits<uint8_t>::max();
    std::vector<size_t> indexes;
    for (size_t i = 0; i < states.size(); ++i) {
        const auto &state = states[i];
        if (!CanRetryTransportRead(state)) {
            continue;
        }
        const auto stateRetryCount = TransportReadRetryCount(state);
        const bool higherPriority = state.policy == TransportReadRetryPolicy::DRAINING
                                    && policy != TransportReadRetryPolicy::DRAINING;
        if (policy == TransportReadRetryPolicy::NONE || higherPriority
            || (state.policy == policy && stateRetryCount < retryCount)) {
            policy = state.policy;
            retryCount = stateRetryCount;
            indexes.clear();
        }
        if (state.policy == policy && stateRetryCount == retryCount) {
            indexes.push_back(i);
        }
    }
    return indexes;
}

Status PrepareTransportReadRetry(const std::shared_ptr<client::Routing> &routing,
                                 const std::vector<size_t> &retryIndexes,
                                 std::vector<TransportReadRetryState> &states,
                                 client::DeadlineRetry &retry, bool &refreshRequested)
{
    auto &firstState = states[retryIndexes.front()];
    const bool draining = firstState.policy == TransportReadRetryPolicy::DRAINING;
    const auto retryCount = TransportReadRetryCount(firstState);
    const bool immediateStaleRetry = !draining && retryCount == 0;
    int64_t nextBackoffMs =
        client::SelectLocationRefreshBackoffMs(draining, retryCount, TransportReadRetryBackoffMs(firstState));
    LOG_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE)
        << "[TransportGet][Route] Retry " << (draining ? "draining" : "stale")
        << " locations, key count: " << retryIndexes.size() << ", retry count: "
        << (static_cast<int>(retryCount) + 1) << ", backoff ms: " << nextBackoffMs
        << ", remaining deadline us: " << ApiDeadline::Instance().ApiRemainingUs();
    if (!refreshRequested) {
        if (routing != nullptr) {
            routing->ForceRefresh();
        }
        refreshRequested = true;
    }
    RETURN_IF_NOT_OK(retry.Backoff(nextBackoffMs));
    for (auto index : retryIndexes) {
        ++TransportReadRetryCount(states[index]);
        if (!immediateStaleRetry) {
            TransportReadRetryBackoffMs(states[index]) = nextBackoffMs;
        }
    }
    return Status::OK();
}

void ApplyTransportReadRetryWaitFailure(const std::vector<size_t> &retryIndexes,
                                        const std::vector<TransportReadRetryState> &states,
                                        const Status &waitStatus, std::vector<Status> &itemStatuses)
{
    for (auto index : retryIndexes) {
        const auto outputIndex = states[index].outputIndex;
        Status deadlineStatus = waitStatus;
        deadlineStatus.AppendMsg(itemStatuses[outputIndex].GetMsg());
        itemStatuses[outputIndex] = std::move(deadlineStatus);
    }
}

void ApplyTransportReadRetryBudgetFailure(const std::vector<TransportReadRetryState> &states,
                                          std::vector<Status> &itemStatuses)
{
    const bool deadlineExpired = ApiDeadline::Instance().ApiRemainingUs() <= 0;
    for (const auto &state : states) {
        if (state.policy != TransportReadRetryPolicy::STALE
            || (!deadlineExpired && CanRetryTransportRead(state))) {
            continue;
        }
        const auto outputIndex = state.outputIndex;
        if (outputIndex >= itemStatuses.size()
            || !client::IsTransportSnapshotStaleLocation(itemStatuses[outputIndex])) {
            continue;
        }
        Status finalStatus = deadlineExpired
                                 ? Status(K_RPC_DEADLINE_EXCEEDED,
                                          "API deadline exceeded while waiting for transport snapshot refresh")
                                 : Status(K_RPC_UNAVAILABLE,
                                          "Transport snapshot refresh exhausted before a readable replica was found");
        finalStatus.AppendMsg(itemStatuses[outputIndex].GetMsg());
        itemStatuses[outputIndex] = std::move(finalStatus);
    }
}


class RoutingExistAdapter : public IExistRouting {
public:
    explicit RoutingExistAdapter(std::shared_ptr<client::Routing> routing) : routing_(std::move(routing)) {}
    ~RoutingExistAdapter() override = default;

    Status SelectWorkers(const std::vector<std::string> &keys, client::DataPlacementPolicy policy,
                         std::unordered_map<HostPort, std::vector<std::string>> &groups,
                         const std::vector<HostPort> &exclude) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(routing_);
        return routing_->SelectWorkers(keys, policy, groups, exclude);
    }

    void UpdateState(const HostPort &addr, StatusCode status) override
    {
        if (routing_ != nullptr) {
            routing_->UpdateState(addr, status);
        }
    }

private:
    std::shared_ptr<client::Routing> routing_;
};

class TransportLayerExistAdapter : public IExistTransport {
public:
    explicit TransportLayerExistAdapter(client::TransportLayer *transport) : transport_(transport) {}
    ~TransportLayerExistAdapter() override = default;

    Status Exist(const HostPort &workerAddr, const client::TransportExistRequest &input,
                 client::TransportExistResult &output) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(transport_);
        return transport_->Exist(workerAddr, input, output);
    }

private:
    client::TransportLayer *transport_;
};

}  // namespace

RoutedMode::RoutedMode(const Deps &deps, const HostServices &host)
    : transportLayer_(deps.transportLayer_),
      routing_(deps.routing_),
      requestTimeoutMs_(deps.requestTimeoutMs_),
      tenantId_(deps.tenantId_),
      dataPlacementPolicy_(deps.dataPlacementPolicy_),
      transportToken_(deps.transportToken_),
      memoryCopyThreadPool_(deps.memoryCopyThreadPool_),
      asyncGetRPCPool_(deps.asyncGetRPCPool_),
      memcpyParallelThreshold_(deps.memcpyParallelThreshold_),
      parallismNum_(deps.parallismNum_),
      setMemoryCopyThreadPool_(deps.setMemoryCopyThreadPool_),
      setMemcpyParallelThreshold_(deps.setMemcpyParallelThreshold_),
      boundMode_(deps.boundMode_),
      failover_(deps.failover_),
      host_(host)
{
}

void RoutedMode::HandleDirectGetFailure(const std::shared_ptr<IClientWorkerApi> &workerApi,
                                        const Status &status)
{
    if (!host_.shouldRefreshRoutingAfterFailure(status.GetCode())) {
        return;
    }
    auto routing = std::atomic_load(&routing_);
    if (routing != nullptr && routing->ForceRefresh()) {
        LOG(INFO) << "[Routing] Force hash ring refresh after direct Get failure, worker: "
                  << workerApi->hostPort_.ToString() << ", status: " << status.ToString();
    }
    if (status.GetCode() == K_RPC_PEER_DEAD) {
        (void)failover_->SubmitUnavailableWorkerSwitch(workerApi);
    }
}

Status RoutedMode::CreateRoutedBuffer(const std::string &objectKey, uint64_t dataSize,
                                      const FullParam &param, std::shared_ptr<Buffer> &buffer)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    SetRouteContext routeContext;
    RETURN_IF_NOT_OK(host_.selectSetRoute(objectKey, {}, routeContext));
    const auto requestContext = host_.buildTransportRequestContext(routeContext);
    client::TransportCreateParam createParam;
    createParam.requestContext = requestContext;
    createParam.cacheType = param.cacheType;
    createParam.consistencyType = param.consistencyType;
    createParam.writeMode = param.writeMode;
    createParam.subTimeoutMs = requestTimeoutMs_;
    std::shared_ptr<ObjectBuffer> objBuf;
    RETURN_IF_NOT_OK(transportLayer_->Create(routeContext.worker, objectKey, dataSize, createParam, objBuf));
    // Bridge: transfer the routed ObjectBufferInfo (populated by ShmTransporter::Create with
    // workerAddr/shmId/pointer/mmapEntry/sessionLockId/receiveBufferOwner) to a legacy Buffer.
    auto bufferInfo = ObjectBufferInternal::ExtractInfo(objBuf);
    bufferInfo->isRoutedWrite = true;  // marks a routed write buffer (not a Get'd read-only buffer)
    auto rc = Buffer::CreateBuffer(bufferInfo, host_.getSelf(), buffer);
    if (rc.IsError() && bufferInfo->receiveBufferOwner != nullptr) {
        // Buffer init failed (rare); no Buffer will release the worker allocation, so retire it here.
        bufferInfo->receiveBufferOwner->Release();
    }
    return rc;
}

Status RoutedMode::MultiCreateRouted(const std::vector<std::string> &objectKeyList,
                                     const std::vector<uint64_t> &dataSizeList, const FullParam &param,
                                     std::vector<std::shared_ptr<Buffer>> &bufferList,
                                     std::vector<bool> &exists)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    const auto sz = objectKeyList.size();
    bufferList.assign(sz, nullptr);
    // Routed MCreate does not pre-check existence; existence is enforced at MSet/Publish time
    // (consistent with the one-step routed MSet, which checks existence at transportLayer_->MSet).
    exists.assign(sz, false);
    auto routing = std::atomic_load(&routing_);
    RETURN_RUNTIME_ERROR_IF_NULL(routing);
    std::unordered_map<HostPort, std::vector<std::string>> groupedKeys;
    RETURN_IF_NOT_OK(routing->SelectWorkers(objectKeyList, dataPlacementPolicy_, groupedKeys));
    // Map each key back to its original position so results land in the caller's order.
    std::unordered_map<std::string, size_t> keyIndex;
    keyIndex.reserve(sz);
    for (size_t i = 0; i < sz; ++i) {
        keyIndex.emplace(objectKeyList[i], i);
    }
    CHECK_FAIL_RETURN_STATUS(keyIndex.size() == sz, K_INVALID,
                             "MultiCreate routed path does not support duplicate keys");
    // Retire worker allocations behind already-created routed Buffers when a later group fails.
    // IReceiveBufferOwner::Release is idempotent, so the Buffer destructor's later Release is a no-op.
    auto releaseAllocated = [&bufferList]() {
        for (auto &b : bufferList) {
            if (b != nullptr && b->bufferInfo_ != nullptr && b->bufferInfo_->receiveBufferOwner != nullptr) {
                b->bufferInfo_->receiveBufferOwner->Release();
            }
        }
    };
    for (auto &entry : groupedKeys) {
        std::vector<uint64_t> sizes;
        sizes.reserve(entry.second.size());
        for (const auto &key : entry.second) {
            sizes.emplace_back(dataSizeList[keyIndex[key]]);  // key from SelectWorkers(input) is in keyIndex
        }
        auto rc = ProcessRoutedMCreateGroup(entry.first, entry.second, sizes, param, keyIndex, bufferList);
        if (rc.IsError()) {
            releaseAllocated();
            bufferList.clear();
            return rc;
        }
    }
    return Status::OK();
}

Status RoutedMode::ProcessRoutedMCreateGroup(const HostPort &worker, const std::vector<std::string> &keys,
                                             const std::vector<uint64_t> &sizes, const FullParam &param,
                                             const std::unordered_map<std::string, size_t> &keyIndex,
                                             std::vector<std::shared_ptr<Buffer>> &bufferList)
{
    SetRouteContext routeContext;
    RETURN_IF_NOT_OK(host_.buildSetRouteContext(worker, routeContext));
    const auto requestContext = host_.buildTransportRequestContext(routeContext);
    client::TransportCreateParam createParam;
    createParam.requestContext = requestContext;
    createParam.cacheType = param.cacheType;
    createParam.consistencyType = param.consistencyType;
    createParam.writeMode = param.writeMode;
    createParam.subTimeoutMs = requestTimeoutMs_;
    std::vector<std::shared_ptr<ObjectBuffer>> objBufs;
    RETURN_IF_NOT_OK(transportLayer_->MCreate(worker, keys, sizes, createParam, objBufs));
    for (auto &objBuf : objBufs) {
        // Bridge: transfer the routed ObjectBufferInfo to a legacy Buffer at its original index.
        auto bufferInfo = ObjectBufferInternal::ExtractInfo(objBuf);
        bufferInfo->isRoutedWrite = true;  // marks a routed write buffer (not a Get'd read-only buffer)
        bufferInfo->ttlSecond = param.ttlSecond;          // carry Create-time ttl/existence to Publish
        bufferInfo->existence = static_cast<int>(param.existence);
        auto it = keyIndex.find(bufferInfo->objectKey);
        if (it == keyIndex.end()) {
            if (bufferInfo->receiveBufferOwner != nullptr) {
                bufferInfo->receiveBufferOwner->Release();
            }
            return Status(K_RUNTIME_ERROR, "Routed MCreate returned an unknown object key");
        }
        auto rc = Buffer::CreateBuffer(bufferInfo, host_.getSelf(), bufferList[it->second]);
        if (rc.IsError()) {
            // Buffer init failed (rare); no Buffer will own it, so retire the worker allocation here.
            if (bufferInfo->receiveBufferOwner != nullptr) {
                bufferInfo->receiveBufferOwner->Release();
            }
            return rc;
        }
    }
    return Status::OK();
}

Status RoutedMode::ProcessTransportPut(
    const std::string &objectKey, const uint8_t *data, uint64_t size, const FullParam &param,
    const std::unordered_set<std::string> &nestedObjectKeys, uint32_t ttlSecond, int existence,
    const SetRouteContext &routeContext, SetFailureStage &failureStage,
    client::TransportSetResult &transportResult, int32_t requestTimeoutMs, bool isSeal)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    const int32_t subTimeoutMs = requestTimeoutMs > 0 ? requestTimeoutMs : requestTimeoutMs_;
    const auto requestContext = host_.buildTransportRequestContext(routeContext);
    client::TransportCreateParam createParam;
    createParam.requestContext = requestContext;
    createParam.cacheType = param.cacheType;
    createParam.consistencyType = param.consistencyType;
    createParam.writeMode = param.writeMode;
    createParam.subTimeoutMs = subTimeoutMs;
    failureStage = SetFailureStage::CREATE;
    std::shared_ptr<ObjectBuffer> buffer;
    RETURN_IF_NOT_OK(transportLayer_->Create(routeContext.worker, objectKey, size, createParam, buffer));

    failureStage = SetFailureStage::TRANSFER;
    const bool traceEnabled = IsClientLatencyTraceActive();
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_MEMORY_COPY_START);
    auto *dst = static_cast<uint8_t *>(buffer->MutableData());
    Status copyRc = size == 0 || setMemoryCopyThreadPool_ == nullptr || size <= setMemcpyParallelThreshold_
                        ? HugeMemoryCopy(dst, buffer->GetSize(), data, size)
                        : ::datasystem::MemoryCopy(dst, buffer->GetSize(), data, size, setMemoryCopyThreadPool_,
                                                   setMemcpyParallelThreshold_);
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_MEMORY_COPY_END);
    if (copyRc.IsError()) {
        LOG_IF_ERROR(transportLayer_->Release(*buffer, requestContext),
                     "Release routed Set allocation after MemoryCopy failure failed");
        return copyRc;
    }
    client::TransportSetParam setParam;
    setParam.requestContext = requestContext;
    setParam.nestedKeys = nestedObjectKeys;
    setParam.ttlSecond = ttlSecond;
    setParam.existence = static_cast<ExistenceOpt>(existence);
    setParam.isSeal = isSeal;
    setParam.subTimeoutMs = subTimeoutMs;
    failureStage = SetFailureStage::PUBLISH;
    Status setRc = transportLayer_->Set(*buffer, setParam, transportResult);
    if (setRc.GetCode() == K_URMA_NEED_CONNECT) {
        // TransportLayer returns this only after same-worker UB reconnect failed, before Publish was sent.
        failureStage = SetFailureStage::TRANSFER;
    }
    return setRc;
}

void RoutedMode::BuildTransportReadRequest(const std::vector<std::string> &objectKeys,
                                           client::ObjectReadRequest &request,
                                           std::vector<Status> &itemStatuses, int64_t subTimeoutMs,
                                           bool queryL2Cache)
{
    auto context = std::make_shared<client::TransportReadContext>();
    context->requestContext.clientId = host_.getClientId();
    const auto token = std::atomic_load(&transportToken_);
    if (token != nullptr && !token->Empty()) {
        context->requestContext.token.assign(token->GetData(), token->GetSize());
    }
    const auto &requestTenantId = GetRequestContext()->tenantId;
    context->requestContext.tenantId = requestTenantId.empty() ? tenantId_ : requestTenantId;
    context->subTimeoutMs = subTimeoutMs;
    context->queryL2Cache = queryL2Cache;
    request.context = std::move(context);

    auto routing = std::atomic_load(&routing_);
    if (routing == nullptr) {
        std::fill(itemStatuses.begin(), itemStatuses.end(), Status(K_NOT_READY, "Object route is not ready"));
        LOG_EVERY_N(ERROR, TRANSPORT_DIAG_LOG_RATE)
            << "[TransportGet][Route] Route is not ready, key count: " << objectKeys.size();
        return;
    }
    std::unordered_map<HostPort, std::vector<std::string>> groupedKeys;
    Status routeStatus =
        routing->SelectWorkers(objectKeys, client::DataPlacementPolicy::PREFERRED_META_OWNER, groupedKeys);
    if (routeStatus.IsError()) {
        std::fill(itemStatuses.begin(), itemStatuses.end(), routeStatus);
        LOG(ERROR) << "[TransportGet][Route] Route selection failed, key count: " << objectKeys.size()
                   << ", status: " << routeStatus.ToString();
        return;
    }
    std::unordered_map<std::string, HostPort> metaOwners;
    metaOwners.reserve(objectKeys.size());
    for (const auto &group : groupedKeys) {
        for (const auto &key : group.second) {
            metaOwners.emplace(key, group.first);
        }
    }
    for (size_t i = 0; i < objectKeys.size(); ++i) {
        auto owner = metaOwners.find(objectKeys[i]);
        if (owner == metaOwners.end()) {
            itemStatuses[i] = Status(K_RUNTIME_ERROR, "Batch route result is incomplete");
            LOG_EVERY_N(ERROR, TRANSPORT_DIAG_LOG_RATE)
                << "[TransportGet][Route] Route result is incomplete, key: " << objectKeys[i]
                << ", request index: " << i << ", status: " << itemStatuses[i].ToString();
            continue;
        }
        itemStatuses[i] = Status::OK();
        request.items.push_back({ i, objectKeys[i], owner->second });
    }
    const size_t routed = request.items.size();
    const size_t failed = objectKeys.size() >= routed ? objectKeys.size() - routed : 0;
    VLOG(1) << "[TransportGet][Route] Route selection completed, key count: " << objectKeys.size()
            << ", routed: " << routed << ", failed: " << failed << ", meta owner count: " << groupedKeys.size();
}

Status RoutedMode::BuildTransportGetResponse(
    client::ObjectReadItemResult &item, GetRspPb &response,
    std::unordered_map<std::string, std::shared_ptr<ObjectBufferInfo>> &ubBufferInfos, uint64_t &payloadSize)
{
    auto &data = item.data;
    const uint64_t dataSize = data.externalOwner != nullptr
                                  ? data.externalSize
                                  : static_cast<uint64_t>(std::max<int64_t>(data.response.data_size(), 0));
    payloadSize = 0;
    auto *payloadInfo = response.add_payload_info();
    payloadInfo->set_object_key(item.objectKey);
    payloadInfo->set_object_index(0);
    payloadInfo->set_data_size(static_cast<int64_t>(dataSize));
    if (data.externalOwner != nullptr) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            data.response.data_size() >= 0
                && static_cast<uint64_t>(data.response.data_size()) == data.externalSize,
            K_RUNTIME_ERROR, "Invalid object data response");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(data.externalData != nullptr || dataSize == 0, K_RUNTIME_ERROR,
                                             "Invalid object data response");
        FullParam param;
        auto bufferInfo = boundMode_->MakeObjectBufferInfo(item.objectKey, const_cast<uint8_t *>(data.externalData),
                                                           dataSize, 0, param, false, 0);
        bufferInfo->ubGetBufferHandle = data.externalOwner;
        ubBufferInfos.emplace(item.objectKey, std::move(bufferInfo));
        payloadInfo->add_part_index(0);
        data.rpcPayloads.emplace_back();
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(data.externalData == nullptr && data.externalSize == 0, K_RUNTIME_ERROR,
                                         "Invalid object data response");
    for (size_t i = 0; i < data.rpcPayloads.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(payloadSize <= UINT64_MAX - data.rpcPayloads[i].Size(), K_RUNTIME_ERROR,
                                             "Invalid object data response");
        payloadSize += data.rpcPayloads[i].Size();
        payloadInfo->add_part_index(static_cast<uint32_t>(i));
    }
    LOG_IF(ERROR, payloadSize != dataSize)
        << "[TransportGet][Materialize] RPC payload size mismatch, key=" << item.objectKey
        << ", responseDataSize=" << data.response.data_size() << ", payloadSize=" << payloadSize
        << ", payloadCount=" << data.rpcPayloads.size()
        << ", dataSource=" << static_cast<int>(data.response.data_source());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(payloadSize == dataSize, K_RUNTIME_ERROR, "Invalid object data response");
    return Status::OK();
}

Status RoutedMode::MaterializeTransportItem(const std::string &objectKey, client::ObjectReadItemResult &item,
                                            std::shared_ptr<Buffer> &buffer)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(item.objectKey == objectKey, K_RUNTIME_ERROR,
                                         "Invalid object data response");
    auto &data = item.data;
    if (data.externalMeta.has_value()) {
        CHECK_FAIL_RETURN_STATUS(data.externalOwner != nullptr, K_RUNTIME_ERROR,
                                 "SHM object data owner is missing");
        CHECK_FAIL_RETURN_STATUS(data.externalData != nullptr || data.externalSize == 0, K_RUNTIME_ERROR,
                                 "SHM object data pointer is missing");
        const auto &meta = *data.externalMeta;
        FullParam param;
        param.writeMode = meta.mode.GetWriteMode();
        param.consistencyType = meta.mode.GetConsistencyType();
        param.cacheType = meta.mode.GetCacheType();
        // The routed owner checks the target session generation, so the legacy initial-Worker version is unused.
        auto bufferInfo = boundMode_->MakeObjectBufferInfo(item.objectKey, const_cast<uint8_t *>(data.externalData),
                                                           data.externalSize, meta.metadataSize, param, meta.isSeal, 0,
                                                           meta.shmId);
        bufferInfo->workerAddr = meta.workerAddr;
        bufferInfo->receiveBufferOwner = std::move(data.externalOwner);
        bufferInfo->sessionLockId = meta.lockId;
        bufferInfo->useSessionLockId = true;
        return Buffer::CreateBuffer(std::move(bufferInfo), host_.getSelf(), buffer);
    }

    GetRspPb response;
    std::unordered_map<std::string, std::shared_ptr<ObjectBufferInfo>> ubBufferInfos;
    uint64_t payloadSize = 0;
    RETURN_IF_NOT_OK(BuildTransportGetResponse(item, response, ubBufferInfos, payloadSize));
    const uint64_t dataSize = static_cast<uint64_t>(response.payload_info(0).data_size());
    VLOG(1) << "[TransportGet][Materialize] Materialize object, key: " << objectKey
            << ", transport: " << AccessTransportTracker::KindToName(data.kind) << ", data size: " << dataSize
            << ", payload size: " << payloadSize << ", payload count: " << data.rpcPayloads.size()
            << ", external size: " << data.externalSize
            << ", data source: " << static_cast<int>(data.response.data_source());
    std::vector<std::shared_ptr<Buffer>> itemBuffers(1);
    std::vector<std::string> failedKeys;
    RETURN_IF_NOT_OK(boundMode_->ProcessGetResponse({ item.objectKey }, {}, response, 0, data.rpcPayloads, itemBuffers,
                                                    failedKeys, ubBufferInfos));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(failedKeys.empty() && itemBuffers.front() != nullptr, K_NOT_FOUND,
                                         "Cannot get objects from worker");
    buffer = std::move(itemBuffers.front());
    return Status::OK();
}

Status RoutedMode::ApplyTransportReadResult(const std::vector<std::string> &objectKeys,
                                            const client::ObjectReadRequest &request,
                                            client::ObjectReadResult &result, const Status &transportStatus,
                                            std::vector<std::shared_ptr<Buffer>> &buffers,
                                            std::vector<Status> &itemStatuses, AccessTransportKind &actualKind)
{
    // a fully-failed Get still reflects the real transport instead of the SHM default from Reset().
    actualKind = static_cast<AccessTransportKind>(
        std::max(static_cast<uint8_t>(actualKind), static_cast<uint8_t>(result.actualKind)));
    std::vector<bool> returned(objectKeys.size(), false);
    for (auto &item : result.items) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(item.requestIndex < objectKeys.size(), K_RUNTIME_ERROR,
                                             "Invalid response while getting objects");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!returned[item.requestIndex], K_RUNTIME_ERROR,
                                             "Invalid response while getting objects");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(item.objectKey == objectKeys[item.requestIndex], K_RUNTIME_ERROR,
                                             "Invalid response while getting objects");
        returned[item.requestIndex] = true;
        itemStatuses[item.requestIndex] = item.status;
        if (item.status.IsOk()) {
            itemStatuses[item.requestIndex] =
                MaterializeTransportItem(item.objectKey, item, buffers[item.requestIndex]);
            if (itemStatuses[item.requestIndex].IsOk()) {
                actualKind = static_cast<AccessTransportKind>(std::max(
                    static_cast<uint8_t>(actualKind), static_cast<uint8_t>(item.data.kind)));
            }
        }
    }
    for (const auto &item : request.items) {
        if (!returned[item.requestIndex]) {
            itemStatuses[item.requestIndex] = transportStatus.IsError()
                                                  ? transportStatus
                                                  : Status(K_RUNTIME_ERROR, "Cannot get objects from worker");
            LOG_EVERY_N(ERROR, TRANSPORT_DIAG_LOG_RATE)
                << "[TransportGet][Result] Object result is missing, key: " << item.objectKey
                << ", request index: " << item.requestIndex
                << ", status: " << itemStatuses[item.requestIndex].ToString();
        }
    }
    if (VLOG_IS_ON(1)) {
        const auto succeeded = std::count_if(itemStatuses.begin(), itemStatuses.end(),
                                             [](const Status &status) { return status.IsOk(); });
        VLOG(1) << "[TransportGet][Result] Apply result completed, requested: " << objectKeys.size()
                << ", routed: " << request.items.size() << ", returned: "
                << std::count(returned.begin(), returned.end(), true) << ", succeeded: " << succeeded
                << ", failed: " << itemStatuses.size() - succeeded
                << ", actual transport: " << AccessTransportTracker::KindToName(actualKind);
    }
    return Status::OK();
}

Status RoutedMode::FinishTransportRead(const std::vector<Status> &itemStatuses,
                                       AccessTransportKind actualKind, const Status &transportStatus)
{
    const bool anyOk = std::any_of(itemStatuses.begin(), itemStatuses.end(),
                                   [](const Status &status) { return status.IsOk(); });
    // Record the transport whenever the transport layer actually carried data over a non-SHM
    // medium, or at least one item succeeded — covers the fully-failed Get case where
    // ApplyTransportReadResult still seeded actualKind from result.actualKind.
    if (anyOk || actualKind != AccessTransportKind::SHM) {
        AccessTransportTracker::Record(actualKind);
    }
    if (anyOk) {
        return Status::OK();
    }
    for (const auto &status : itemStatuses) {
        if (status.IsError()) {
            return status;
        }
    }
    return transportStatus.IsError() ? transportStatus : Status(K_RUNTIME_ERROR, "Failed to get objects");
}

Status RoutedMode::ReadTransportRound(const std::vector<std::string> &objectKeys, bool traceEnabled,
                                      int64_t subTimeoutMs, bool queryL2Cache,
                                      std::vector<std::shared_ptr<Buffer>> &buffers,
                                      std::vector<Status> &itemStatuses, AccessTransportKind &actualKind,
                                      Status &transportStatus)
{
    client::ObjectReadRequest request;
    request.traceEnabled = traceEnabled;
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_DIRECT_ROUTE_START);
    BuildTransportReadRequest(objectKeys, request, itemStatuses, subTimeoutMs, queryL2Cache);
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_DIRECT_ROUTE_END);
    client::ObjectReadResult result;
    transportStatus = request.items.empty() ? Status(K_NOT_READY, "No object route is available")
                                            : transportLayer_->Get(request, result);
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_DIRECT_MATERIALIZE_START);
    Status applyStatus =
        ApplyTransportReadResult(objectKeys, request, result, transportStatus, buffers, itemStatuses, actualKind);
    AddLatencyTickIfEnabled(traceEnabled, LatencyTickKey::CLIENT_DIRECT_MATERIALIZE_END);
    return applyStatus;
}

Status RoutedMode::GetFromTransportLayer(const std::vector<std::string> &objectKeys,
                                         std::vector<std::shared_ptr<Buffer>> &buffers, bool traceEnabled,
                                         int64_t subTimeoutMs, bool queryL2Cache)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(transportLayer_ != nullptr, K_NOT_READY, "Object service is not ready");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objectKeys.size() == buffers.size(), K_RUNTIME_ERROR,
                                         "Failed to prepare object Get request");
    std::vector<Status> itemStatuses(objectKeys.size(), Status(K_NOT_READY, "Object Get has not completed"));
    AccessTransportKind actualKind = AccessTransportKind::SHM;
    Status transportStatus(K_NOT_READY, "No object route is available");
    std::vector<TransportReadRetryState> retryStates;
    client::DeadlineRetry retry;
    bool refreshRequested = false;

    RETURN_IF_NOT_OK(ReadTransportRound(objectKeys, traceEnabled, subTimeoutMs, queryL2Cache, buffers, itemStatuses,
                                        actualKind, transportStatus));
    CollectInitialTransportReadRetryStates(itemStatuses, retryStates);

    while (ApiDeadline::Instance().ApiRemainingUs() > 0) {
        auto retryIndexes = BuildNextTransportReadRetry(retryStates);
        if (retryIndexes.empty()) {
            break;
        }
        auto routing = std::atomic_load(&routing_);
        Status waitStatus = PrepareTransportReadRetry(routing, retryIndexes, retryStates, retry, refreshRequested);
        if (waitStatus.IsError()) {
            transportStatus = waitStatus;
            ApplyTransportReadRetryWaitFailure(retryIndexes, retryStates, waitStatus, itemStatuses);
            break;
        }

        std::vector<std::string> retryKeys;
        retryKeys.reserve(retryIndexes.size());
        for (auto stateIndex : retryIndexes) {
            retryKeys.emplace_back(objectKeys[retryStates[stateIndex].outputIndex]);
        }
        TransportReadRoundResult roundResult;
        roundResult.buffers.resize(retryKeys.size());
        roundResult.statuses.resize(retryKeys.size(), Status(K_NOT_READY, "Object Get has not completed"));
        RETURN_IF_NOT_OK(ReadTransportRound(retryKeys, traceEnabled, subTimeoutMs, queryL2Cache, roundResult.buffers,
                                            roundResult.statuses, actualKind, transportStatus));
        CollectRetryTransportReadRound(retryIndexes, roundResult, buffers, itemStatuses, retryStates);
    }
    ApplyTransportReadRetryBudgetFailure(retryStates, itemStatuses);
    return FinishTransportRead(itemStatuses, actualKind, transportStatus);
}

Status RoutedMode::CheckMultiSetInputParamValidationNtx(const std::vector<std::string> &keys,
                                                        const std::vector<StringView> &vals,
                                                        std::vector<std::string> &outFailedKeys,
                                                        std::vector<std::string> &deduplicateKeys,
                                                        std::vector<StringView> &deduplicateVals)
{
    std::unordered_set<std::string_view> keySet;
    keySet.reserve(keys.size());
    CHECK_FAIL_RETURN_STATUS(!keys.empty(), K_INVALID, "The keys should not be empty.");
    CHECK_FAIL_RETURN_STATUS(keys.size() == vals.size(), K_INVALID, "The number of key and value is not the same.");
    RETURN_IF_NOT_OK(ObjectClientImpl::CheckValidObjectKey(*keys.begin()));
    for (size_t i = 0; i < keys.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(!keys[i].empty(), K_INVALID, "The key should not be empty.");
        CHECK_FAIL_RETURN_STATUS(vals[i].data() != nullptr, K_INVALID,
                                 FormatString("The value associated with key %s should not be empty.", keys[i]));
        auto [it, inserted] = keySet.emplace(keys[i]);
        (void)it;
        if (!inserted) {
            LOG(ERROR) << "The input parameter contains duplicate key " << keys[i];
            outFailedKeys.emplace_back(keys[i]);
        }
    }
    if (!outFailedKeys.empty()) {
        for (size_t i = 0; i < keys.size(); ++i) {
            if (keySet.find(keys[i]) == keySet.end()) {
                continue;
            }
            deduplicateKeys.emplace_back(keys[i]);
            deduplicateVals.emplace_back(vals[i]);
            keySet.erase(keys[i]);
        }
    }
    return Status::OK();
}

Status RoutedMode::MemoryCopyTransportMSetBuffers(
    const MSetRouteGroup &group, const std::vector<std::shared_ptr<ObjectBuffer>> &buffers, uint64_t dataSizeSum)
{
    CHECK_FAIL_RETURN_STATUS(group.values.size() == buffers.size(), K_RUNTIME_ERROR,
                             "MSet transport buffer count mismatch");
    RETURN_IF_NOT_OK(ApiDeadline::Instance().CheckApiDeadline());
    auto memoryCopy = [&](size_t start, size_t end) {
        for (size_t i = start; i < end; ++i) {
            RETURN_RUNTIME_ERROR_IF_NULL(buffers[i]);
            const auto &value = group.values[i];
            const int64_t bufferSize = buffers[i]->GetSize();
            auto *bufferData = static_cast<uint8_t *>(buffers[i]->MutableData());
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(value.data() != nullptr, K_INVALID, "Can't put null pointer.");
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(bufferSize >= 0 && bufferData != nullptr, K_INVALID,
                                                 "Buffer data is invalid.");
            const uint64_t dataSize = static_cast<uint64_t>(bufferSize);
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(value.size() > 0 && value.size() <= dataSize, K_INVALID,
                                                 "Data length must be in (0, buffer_size].");
            RETURN_IF_NOT_OK(::datasystem::MemoryCopy(
                bufferData, dataSize, reinterpret_cast<const uint8_t *>(value.data()), value.size(),
                memoryCopyThreadPool_, memcpyParallelThreshold_));
        }
        return Status::OK();
    };
    static constexpr uint64_t MIN_PARALLEL_SIZE = 500 * KB;
    static constexpr uint64_t PARALLEL_SIZE = 4 * MB_TO_BYTES;
    static constexpr size_t PARALLEL_COUNT = 32;
    const bool parallel = dataSizeSum > MIN_PARALLEL_SIZE
                          && (dataSizeSum >= PARALLEL_SIZE || buffers.size() >= PARALLEL_COUNT);
    Timer timer;
    Status rc = (!parallel || parallismNum_ == 0)
                    ? memoryCopy(0, buffers.size())
                    : Parallel::ParallelFor<size_t>(0, buffers.size(), memoryCopy, 4, parallismNum_);
    const int64_t elapsedUs = timer.ElapsedMicroSecond();
    SLOW_LOG_IF_OR_VLOG(
        INFO, elapsedUs >= TimeoutDuration::SLOW_PATH_LOG_THRESHOLD_US || rc.IsError(), 1,
        FormatString("[MSet] phase=TransportMemoryCopy costUs=%lld size=%zu keys=%zu rc=%s",
                     elapsedUs, dataSizeSum, group.keys.size(), rc.ToString()));
    RETURN_IF_NOT_OK(rc);
    return ApiDeadline::Instance().CheckApiDeadline();
}

Status RoutedMode::BuildMSetRouteGroups(const std::vector<std::string> &keys,
                                        const std::vector<StringView> &values,
                                        std::vector<MSetRouteGroup> &groups)
{
    auto routing = std::atomic_load(&routing_);
    RETURN_RUNTIME_ERROR_IF_NULL(routing);
    std::unordered_map<HostPort, std::vector<std::string>> groupedKeys;
    RETURN_IF_NOT_OK(routing->SelectWorkers(keys, dataPlacementPolicy_, groupedKeys,
                                            host_.mergeWriteTargetExclusions({})));
    std::unordered_map<std::string, size_t> valueIndexes;
    valueIndexes.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        valueIndexes.emplace(keys[i], i);
    }
    groups.reserve(groupedKeys.size());
    size_t groupedKeyCount = 0;
    for (auto &entry : groupedKeys) {
        MSetRouteGroup group;
        group.worker = entry.first;
        group.keys = std::move(entry.second);
        group.values.reserve(group.keys.size());
        for (const auto &key : group.keys) {
            auto iter = valueIndexes.find(key);
            CHECK_FAIL_RETURN_STATUS(iter != valueIndexes.end(), K_RUNTIME_ERROR, "MSet route contains unknown key");
            group.values.emplace_back(values[iter->second]);
        }
        groupedKeyCount += group.keys.size();
        groups.emplace_back(std::move(group));
    }
    CHECK_FAIL_RETURN_STATUS(groupedKeyCount == keys.size(), K_RUNTIME_ERROR, "MSet route result is incomplete");
    return Status::OK();
}

Status RoutedMode::ProcessTransportMSet(const MSetRouteGroup &group, const MSetParam &param,
                                        const SetRouteContext &routeContext,
                                        client::TransportMSetResult &result,
                                        SetFailureStage &failureStage, PerfPoint &point)
{
    RETURN_RUNTIME_ERROR_IF_NULL(transportLayer_);
    const auto requestContext = host_.buildTransportRequestContext(routeContext);
    client::TransportCreateParam createParam;
    createParam.requestContext = requestContext;
    createParam.cacheType = param.cacheType;
    createParam.consistencyType = ConsistencyType::CAUSAL;
    createParam.writeMode = param.writeMode;
    createParam.subTimeoutMs = requestTimeoutMs_;
    std::vector<uint64_t> sizes;
    uint64_t dataSizeSum = 0;
    ComputeDataSizes(group.values, sizes, dataSizeSum);
    point.RecordAndReset(PerfKey::CLIENT_MSET_MULTICREATE);
    failureStage = SetFailureStage::CREATE;
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    RETURN_IF_NOT_OK(transportLayer_->MCreate(routeContext.worker, group.keys, sizes, createParam, buffers));
    point.RecordAndReset(PerfKey::CLIENT_MSET_MEMCOPY);
    failureStage = SetFailureStage::TRANSFER;
    Status copyRc = MemoryCopyTransportMSetBuffers(group, buffers, dataSizeSum);
    if (copyRc.IsError()) {
        for (const auto &buffer : buffers) {
            LOG_IF_ERROR(transportLayer_->Release(*buffer, requestContext),
                         "Release routed MSet allocation after MemoryCopy failure failed");
        }
        return copyRc;
    }
    client::TransportSetParam setParam;
    setParam.requestContext = requestContext;
    setParam.ttlSecond = param.ttlSecond;
    setParam.existence = param.existence;
    setParam.subTimeoutMs = requestTimeoutMs_;
    point.RecordAndReset(PerfKey::CLIENT_MSET_MULTI_PUBLISH);
    failureStage = SetFailureStage::PUBLISH;
    Status rc = transportLayer_->MSet(buffers, setParam, result);
    if (rc.GetCode() == K_URMA_NEED_CONNECT) {
        failureStage = SetFailureStage::TRANSFER;
    }
    return rc;
}

Status RoutedMode::BuildMSetRetryRouteGroups(const MSetRouteGroup &group,
                                             const std::vector<HostPort> &excludedWorkers,
                                             std::vector<MSetRouteGroup> &groups)
{
    auto routing = std::atomic_load(&routing_);
    RETURN_RUNTIME_ERROR_IF_NULL(routing);
    std::unordered_map<HostPort, std::vector<std::string>> groupedKeys;
    RETURN_IF_NOT_OK(routing->SelectWorkers(group.keys, dataPlacementPolicy_, groupedKeys,
                                            host_.mergeWriteTargetExclusions(excludedWorkers)));
    std::unordered_map<std::string, size_t> valueIndexes;
    valueIndexes.reserve(group.keys.size());
    for (size_t i = 0; i < group.keys.size(); ++i) {
        valueIndexes.emplace(group.keys[i], i);
    }
    groups.reserve(groupedKeys.size());
    for (auto &entry : groupedKeys) {
        MSetRouteGroup retryGroup;
        retryGroup.worker = entry.first;
        retryGroup.keys = std::move(entry.second);
        retryGroup.values.reserve(retryGroup.keys.size());
        for (const auto &key : retryGroup.keys) {
            auto value = valueIndexes.find(key);
            CHECK_FAIL_RETURN_STATUS(value != valueIndexes.end(), K_RUNTIME_ERROR,
                                     "MSet retry route contains unknown key");
            retryGroup.values.emplace_back(group.values[value->second]);
        }
        groups.emplace_back(std::move(retryGroup));
    }
    return Status::OK();
}

Status RoutedMode::ExecuteTransportMSetRetryGroups(
    const std::vector<MSetRouteGroup> &groups, const MSetParam &param,
    const std::vector<HostPort> &excludedWorkers, size_t attempt,
    std::vector<std::string> &outFailedKeys, PerfPoint &point)
{
    const size_t failedBefore = outFailedKeys.size();
    size_t objectCount = 0;
    Status lastRc;
    for (const auto &retryGroup : groups) {
        objectCount += retryGroup.keys.size();
        Status rc = ExecuteTransportMSetGroupAttempt(retryGroup, param, excludedWorkers, attempt,
                                                     outFailedKeys, point);
        if (rc.IsError()) {
            lastRc = rc;
        }
    }
    if (outFailedKeys.size() - failedBefore < objectCount) {
        return Status::OK();
    }
    return lastRc.IsError() ? lastRc : Status(K_RUNTIME_ERROR, "All rerouted MSet objects failed");
}

Status RoutedMode::ExecuteTransportMSetGroupAttempt(
    const MSetRouteGroup &group, const MSetParam &param, std::vector<HostPort> excludedWorkers,
    size_t attempt, std::vector<std::string> &outFailedKeys, PerfPoint &point)
{
    Status rc = ApiDeadline::Instance().CheckApiDeadline();
    if (rc.IsError()) {
        outFailedKeys.insert(outFailedKeys.end(), group.keys.begin(), group.keys.end());
        return rc;
    }
    SetRouteContext routeContext;
    rc = host_.buildSetRouteContext(group.worker, routeContext);
    if (rc.IsError()) {
        outFailedKeys.insert(outFailedKeys.end(), group.keys.begin(), group.keys.end());
        return rc;
    }
    client::TransportMSetResult result;
    SetFailureStage failureStage = SetFailureStage::CREATE;
    rc = ProcessTransportMSet(group, param, routeContext, result, failureStage, point);
    if (rc.IsOk()) {
        outFailedKeys.insert(outFailedKeys.end(), result.failedKeys.begin(), result.failedKeys.end());
        return rc;
    }
    const bool safeWriteTargetReplay = result.writeTargetQuarantined
                                       && (!result.publishAttempted || result.publishDefinitelyNotSent);
    if (!host_.handleSetRouteFailure(rc, failureStage, routeContext.worker, excludedWorkers, safeWriteTargetReplay)
        || attempt + 1 >= SET_ROUTE_MAX_ATTEMPTS) {
        const auto &failedKeys = result.failedKeys.empty() ? group.keys : result.failedKeys;
        outFailedKeys.insert(outFailedKeys.end(), failedKeys.begin(), failedKeys.end());
        return rc;
    }
    if (std::find(excludedWorkers.begin(), excludedWorkers.end(), routeContext.worker) == excludedWorkers.end()) {
        excludedWorkers.emplace_back(routeContext.worker);
    }
    std::vector<MSetRouteGroup> retryGroups;
    rc = BuildMSetRetryRouteGroups(group, excludedWorkers, retryGroups);
    if (rc.IsError()) {
        outFailedKeys.insert(outFailedKeys.end(), group.keys.begin(), group.keys.end());
        return rc;
    }
    return ExecuteTransportMSetRetryGroups(retryGroups, param, excludedWorkers, attempt + 1, outFailedKeys, point);
}

Status RoutedMode::ExecuteTransportMSetGroup(const MSetRouteGroup &group, const MSetParam &param,
                                             std::vector<std::string> &outFailedKeys, PerfPoint &point)
{
    return ExecuteTransportMSetGroupAttempt(group, param, {}, 0, outFailedKeys, point);
}

Status RoutedMode::MSetThroughTransport(const std::vector<std::string> &keys,
                                        const std::vector<StringView> &values, const MSetParam &param,
                                        std::vector<std::string> &outFailedKeys, PerfPoint &point)
{
    std::vector<MSetRouteGroup> groups;
    RETURN_IF_NOT_OK(BuildMSetRouteGroups(keys, values, groups));
    const size_t failedBeforeMSet = outFailedKeys.size();
    Status lastRc;
    for (const auto &group : groups) {
        const size_t failedBeforeGroup = outFailedKeys.size();
        Status rc = ExecuteTransportMSetGroup(group, param, outFailedKeys, point);
        if (rc.IsError()) {
            lastRc = rc;
            if (outFailedKeys.size() == failedBeforeGroup) {
                outFailedKeys.insert(outFailedKeys.end(), group.keys.begin(), group.keys.end());
            }
        }
    }
    point.RecordAndReset(PerfKey::CLIENT_MSET_POST_PROCESS);
    if (outFailedKeys.size() - failedBeforeMSet < keys.size()) {
        return Status::OK();
    }
    return lastRc.IsError() ? lastRc : Status(K_RUNTIME_ERROR, "All objects set failed in worker");
}

Status RoutedMode::RunExist(std::shared_ptr<client::Routing> routing, std::shared_ptr<IClientWorkerApi> &workerApi,
    const std::vector<std::string> &keys, std::vector<bool> &exists, const bool queryL2Cache, const bool isLocal,
    const SensitiveValue &token)
{
    if (routing != nullptr && transportLayer_ != nullptr) {
        ExistHandlerRequest request{ keys, queryL2Cache, isLocal, requestTimeoutMs_, host_.getClientId(),
            GetRequestContext()->tenantId.empty() ? tenantId_ : GetRequestContext()->tenantId, token };
        // Stack-allocated adapters: avoids two make_shared control-block allocations per
        // Exist call. The handler holds non-owning aliased shared_ptr to them; Run is
        // synchronous so the adapters outlive the handler.
        RoutingExistAdapter existRouting(std::move(routing));
        TransportLayerExistAdapter existTransport(transportLayer_.get());
        ExistHandler flow(&existRouting, &existTransport, asyncGetRPCPool_);
        return flow.Run(request, exists);
    }
    CHECK_FAIL_RETURN_STATUS(workerApi != nullptr, K_RUNTIME_ERROR, "No available worker API for Exist");
    return workerApi->Exist(keys, exists, queryL2Cache, isLocal);
}

}  // namespace object_cache
}  // namespace datasystem
