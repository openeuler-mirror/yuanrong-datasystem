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

/** Description: Implements the UB object data transporter. */

#include "datasystem/client/transport/data_plane/ub_transporter.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <tuple>
#include <utility>
#include <vector>

#include "datasystem/client/transport/data_plane/tcp_transporter.h"
#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/client/transport/rpc/mset_request_builder.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/numa_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/object/object_buffer.h"

#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif

namespace datasystem {
namespace client {
namespace {
constexpr uint64_t UB_BATCH_SLICE_ALIGNMENT = 16;
constexpr size_t UB_BATCH_MAX_OBJECT_COUNT = 1024;

Status AlignUbBatchSlice(uint64_t size, uint64_t &alignedSize)
{
    constexpr uint64_t mask = UB_BATCH_SLICE_ALIGNMENT - 1;
    CHECK_FAIL_RETURN_STATUS(size <= std::numeric_limits<uint64_t>::max() - mask, K_INVALID,
                             "UB batch slice alignment overflows uint64");
    alignedSize = (size + mask) & ~mask;
    return Status::OK();
}

Status AddUbBatchSize(uint64_t lhs, uint64_t rhs, uint64_t &sum)
{
    CHECK_FAIL_RETURN_STATUS(lhs <= std::numeric_limits<uint64_t>::max() - rhs, K_INVALID,
                             "UB batch aggregate size overflows uint64");
    sum = lhs + rhs;
    return Status::OK();
}

#ifdef USE_URMA
class UbReceiveBufferOwner final : public IReceiveBufferOwner {
public:
    explicit UbReceiveBufferOwner(std::shared_ptr<UrmaManager::BufferHandle> handle) : handle_(std::move(handle))
    {
    }

private:
    std::shared_ptr<UrmaManager::BufferHandle> handle_;
};
#endif

class DefaultUbReceiveBufferProvider final : public IUbReceiveBufferProvider {
public:
    uint64_t MaxGetSize() const override
    {
#ifdef USE_URMA
        return UrmaManager::Instance().GetUBMaxGetDataSize();
#else
        return 0;
#endif
    }

    Status Allocate(uint64_t requiredSize, UbReceiveBuffer &buffer) override
    {
        buffer = UbReceiveBuffer{};
#ifdef USE_URMA
        RETURN_IF_NOT_OK(GetLocalTransportInstanceId(buffer.transportInstanceId));
        std::shared_ptr<UrmaManager::BufferHandle> handle;
        RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferHandle(handle, requiredSize));
        CHECK_FAIL_RETURN_STATUS(handle != nullptr, K_RUNTIME_ERROR, "UB receive buffer handle is null");
        RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferInfo(
            handle, buffer.data, buffer.size, buffer.remoteAddr));
        buffer.owner = std::make_shared<UbReceiveBufferOwner>(std::move(handle));
        return Status::OK();
#else
        (void)requiredSize;
        return Status(K_NOT_SUPPORTED, "USE_URMA not compiled");
#endif
    }
};

}  // namespace

std::shared_ptr<IUbReceiveBufferProvider> CreateDefaultUbReceiveBufferProvider()
{
    return std::make_shared<DefaultUbReceiveBufferProvider>();
}

UbTransporter::UbTransporter(std::shared_ptr<WorkerRpcClient> rpcClient, std::shared_ptr<UbConnection> conn,
                             std::shared_ptr<IUbReceiveBufferProvider> bufferProvider)
    : rpcClient_(std::move(rpcClient)), conn_(std::move(conn)), bufferProvider_(std::move(bufferProvider))
{
    if (bufferProvider_ == nullptr) {
        bufferProvider_ = CreateDefaultUbReceiveBufferProvider();
    }
}

Status UbTransporter::Get(const DataGetRequest &input, DataGetResult &output)
{
    std::shared_lock<std::shared_mutex> lock(lifecycleMutex_);
    return GetLocked(input, output);
}

Status UbTransporter::GetLocked(const DataGetRequest &input, DataGetResult &output)
{
    CHECK_FAIL_RETURN_STATUS(!input.objectKey.empty(), K_INVALID, "Object key is empty");
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    if (conn_ == nullptr || !conn_->IsAlive()) {
        return Status(K_URMA_NEED_CONNECT, "UB connection not alive");
    }
    uint64_t actualSize = input.expectedSize;
    Status rc = GetOnce(input, input.expectedSize, output, actualSize);
    if (rc.GetCode() != K_OC_REMOTE_GET_NOT_ENOUGH || actualSize == 0 || actualSize == input.expectedSize) {
        return rc;
    }
    return GetOnce(input, actualSize, output, actualSize);
}

Status UbTransporter::BatchGet(const DataGetBatchRequest &inputs, DataGetBatchResult &outputs)
{
    outputs.clear();
    std::shared_lock<std::shared_mutex> lock(lifecycleMutex_);
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    CHECK_FAIL_RETURN_STATUS(!inputs.empty(), K_INVALID, "Batch get request is empty");
    if (inputs.size() == 1) {
        DataGetItemResult item;
        Status status = GetLocked(inputs.front(), item.data);
        if (status.IsError() && static_cast<StatusCode>(item.data.response.error().error_code()) == K_OK) {
            return status;
        }
        item.status = status;
        outputs.emplace_back(std::move(item));
        return Status::OK();
    }
    if (conn_ == nullptr || !conn_->IsAlive()) {
        return Status(K_URMA_NEED_CONNECT, "UB connection not alive");
    }
    if (!conn_->SupportsPayloadOnlyClientBatchGet()) {
        TcpTransporter tcpTransporter(rpcClient_);
        return tcpTransporter.BatchGet(inputs, outputs);
    }

    std::vector<uint64_t> alignedSizes;
    alignedSizes.reserve(inputs.size());
    uint64_t totalSize = 0;
    for (const auto &input : inputs) {
        CHECK_FAIL_RETURN_STATUS(!input.objectKey.empty(), K_INVALID, "Object key is empty");
        uint64_t alignedSize = 0;
        RETURN_IF_NOT_OK(AlignUbBatchSlice(input.expectedSize, alignedSize));
        RETURN_IF_NOT_OK(AddUbBatchSize(totalSize, alignedSize, totalSize));
        alignedSizes.emplace_back(alignedSize);
    }

    const uint64_t maxGetSize = bufferProvider_->MaxGetSize();
    DataGetBatchResult pendingOutputs(inputs.size());
    std::vector<size_t> tcpFallbackIndexes;
    bool allocationPressureObserved = false;
    uint64_t allocationCeiling = maxGetSize;
    size_t begin = 0;
    while (begin < inputs.size()) {
        size_t end = begin;
        uint64_t chunkSize = 0;
        while (end < inputs.size() && end - begin < UB_BATCH_MAX_OBJECT_COUNT) {
            const uint64_t alignedSize = alignedSizes[end];
            if (alignedSize == 0 || alignedSize > maxGetSize) {
                if (end == begin) {
                    ++end;
                }
                break;
            }
            if (chunkSize > maxGetSize - alignedSize) {
                break;
            }
            chunkSize += alignedSize;
            ++end;
        }

        const bool preserveUnary = alignedSizes[begin] == 0 || alignedSizes[begin] > maxGetSize;
        if (end - begin == 1 && preserveUnary) {
            DataGetItemResult item;
            Status status = GetLocked(inputs[begin], item.data);
            if (status.IsError() && static_cast<StatusCode>(item.data.response.error().error_code()) == K_OK) {
                return status;
            }
            item.status = status;
            pendingOutputs[begin] = std::move(item);
        } else {
            RETURN_IF_NOT_OK(BatchGetAggregateAdaptive(inputs, alignedSizes, begin, end, chunkSize, pendingOutputs,
                                                       tcpFallbackIndexes, allocationPressureObserved,
                                                       allocationCeiling));
        }
        begin = end;
    }

    if (!tcpFallbackIndexes.empty()) {
        std::sort(tcpFallbackIndexes.begin(), tcpFallbackIndexes.end());
        DataGetBatchRequest fallbackInputs;
        fallbackInputs.reserve(tcpFallbackIndexes.size());
        for (size_t index : tcpFallbackIndexes) {
            fallbackInputs.emplace_back(inputs[index]);
        }

        METRIC_ADD(metrics::KvMetricId::CLIENT_DIRECT_BATCH_GET_TCP_FALLBACK_TOTAL,
                   tcpFallbackIndexes.size());
        TcpTransporter tcpTransporter(rpcClient_);
        DataGetBatchResult fallbackOutputs;
        Status fallbackStatus = tcpTransporter.BatchGet(fallbackInputs, fallbackOutputs);
        if (fallbackStatus.IsError()) {
            for (size_t index : tcpFallbackIndexes) {
                pendingOutputs[index] = DataGetItemResult{};
                pendingOutputs[index].status = fallbackStatus;
            }
        } else {
            CHECK_FAIL_RETURN_STATUS(fallbackOutputs.size() == tcpFallbackIndexes.size(), K_RUNTIME_ERROR,
                                     "TCP fallback result count does not match request count");
            for (size_t i = 0; i < tcpFallbackIndexes.size(); ++i) {
                pendingOutputs[tcpFallbackIndexes[i]] = std::move(fallbackOutputs[i]);
            }
        }
    }

    outputs = std::move(pendingOutputs);
    return Status::OK();
}

Status UbTransporter::BatchGetAggregateAdaptive(const DataGetBatchRequest &inputs,
                                                const std::vector<uint64_t> &alignedSizes, size_t begin, size_t end,
                                                uint64_t rangeSize, DataGetBatchResult &outputs,
                                                std::vector<size_t> &tcpFallbackIndexes,
                                                bool &allocationPressureObserved, uint64_t &allocationCeiling)
{
    bool allocationFailed = rangeSize > allocationCeiling;
    if (!allocationFailed) {
        DataGetBatchResult rangeOutputs;
        Status rc = BatchGetAggregateOnce(inputs, alignedSizes, begin, end, rangeSize, rangeOutputs, allocationFailed);
        if (allocationPressureObserved) {
            allocationCeiling = std::min(allocationCeiling, rangeSize);
        }
        if (rc.IsOk()) {
            CHECK_FAIL_RETURN_STATUS(rangeOutputs.size() == end - begin, K_RUNTIME_ERROR,
                                     "UB batch result count does not match request range");
            for (size_t i = 0; i < rangeOutputs.size(); ++i) {
                outputs[begin + i] = std::move(rangeOutputs[i]);
            }
            return Status::OK();
        }
        if (!allocationFailed) {
            return rc;
        }
        allocationPressureObserved = true;
        allocationCeiling = std::min(allocationCeiling, rangeSize);
    }
    if (end - begin == 1) {
        tcpFallbackIndexes.emplace_back(begin);
        return Status::OK();
    }

    using AggregateRange = std::tuple<uint64_t, size_t, size_t>;
    std::vector<AggregateRange> pendingRanges;
    auto enqueueChildren = [&alignedSizes, &pendingRanges](uint64_t rangeSize, size_t rangeBegin,
                                                          size_t rangeEnd) -> Status {
        size_t split = rangeBegin + 1;
        uint64_t bestLeftSize = alignedSizes[rangeBegin];
        uint64_t rightSize = rangeSize - bestLeftSize;
        uint64_t bestDifference =
            bestLeftSize > rightSize ? bestLeftSize - rightSize : rightSize - bestLeftSize;
        uint64_t cumulativeSize = bestLeftSize;
        for (size_t candidate = rangeBegin + 2; candidate < rangeEnd; ++candidate) {
            RETURN_IF_NOT_OK(AddUbBatchSize(cumulativeSize, alignedSizes[candidate - 1], cumulativeSize));
            rightSize = rangeSize - cumulativeSize;
            const uint64_t difference =
                cumulativeSize > rightSize ? cumulativeSize - rightSize : rightSize - cumulativeSize;
            if (difference < bestDifference) {
                bestDifference = difference;
                bestLeftSize = cumulativeSize;
                split = candidate;
            }
        }
        pendingRanges.emplace_back(bestLeftSize, rangeBegin, split);
        pendingRanges.emplace_back(rangeSize - bestLeftSize, split, rangeEnd);
        METRIC_INC(metrics::KvMetricId::CLIENT_DIRECT_BATCH_GET_UB_SPLIT_TOTAL);
        return Status::OK();
    };
    RETURN_IF_NOT_OK(enqueueChildren(rangeSize, begin, end));
    while (!pendingRanges.empty()) {
        auto largest = std::max_element(pendingRanges.begin(), pendingRanges.end(),
                                        [](const AggregateRange &lhs, const AggregateRange &rhs) {
                                            if (std::get<0>(lhs) != std::get<0>(rhs)) {
                                                return std::get<0>(lhs) < std::get<0>(rhs);
                                            }
                                            return std::get<1>(lhs) > std::get<1>(rhs);
                                        });
        const auto [rangeSize, rangeBegin, rangeEnd] = *largest;
        pendingRanges.erase(largest);

        allocationFailed = rangeSize > allocationCeiling;
        if (!allocationFailed) {
            DataGetBatchResult rangeOutputs;
            Status rc = BatchGetAggregateOnce(inputs, alignedSizes, rangeBegin, rangeEnd, rangeSize, rangeOutputs,
                                              allocationFailed);
            if (allocationPressureObserved) {
                allocationCeiling = std::min(allocationCeiling, rangeSize);
            }
            if (rc.IsOk()) {
                CHECK_FAIL_RETURN_STATUS(rangeOutputs.size() == rangeEnd - rangeBegin, K_RUNTIME_ERROR,
                                         "UB batch result count does not match request range");
                for (size_t i = 0; i < rangeOutputs.size(); ++i) {
                    outputs[rangeBegin + i] = std::move(rangeOutputs[i]);
                }
                continue;
            }
            if (!allocationFailed) {
                return rc;
            }
            allocationPressureObserved = true;
            allocationCeiling = std::min(allocationCeiling, rangeSize);
        }
        if (rangeEnd - rangeBegin == 1) {
            tcpFallbackIndexes.emplace_back(rangeBegin);
            continue;
        }

        RETURN_IF_NOT_OK(enqueueChildren(rangeSize, rangeBegin, rangeEnd));
    }
    return Status::OK();
}

Status UbTransporter::BatchGetAggregateOnce(const DataGetBatchRequest &inputs,
                                            const std::vector<uint64_t> &alignedSizes, size_t begin, size_t end,
                                            uint64_t aggregateSize, DataGetBatchResult &outputs,
                                            bool &allocationFailed)
{
    outputs.clear();
    allocationFailed = false;

    UbReceiveBuffer buffer;
    Status allocationStatus = bufferProvider_->Allocate(aggregateSize, buffer);
    if (allocationStatus.IsError()) {
        allocationFailed = true;
        return allocationStatus;
    }
    CHECK_FAIL_RETURN_STATUS(buffer.data != nullptr, K_RUNTIME_ERROR, "UB aggregate receive buffer is null");
    CHECK_FAIL_RETURN_STATUS(buffer.owner != nullptr, K_RUNTIME_ERROR, "UB aggregate receive buffer owner is null");
    CHECK_FAIL_RETURN_STATUS(buffer.size >= aggregateSize, K_RUNTIME_ERROR,
                             "UB aggregate receive buffer is smaller than requested");
    CHECK_FAIL_RETURN_STATUS(!buffer.transportInstanceId.empty(), K_RUNTIME_ERROR,
                             "UB aggregate receive buffer transport instance id is empty");

    BatchGetObjectRemoteReqPb request;
    std::vector<uint64_t> sliceOffsets;
    sliceOffsets.reserve(end - begin);
    uint64_t sliceOffset = 0;
    for (size_t i = begin; i < end; ++i) {
        auto *itemRequest = request.add_requests();
        itemRequest->set_object_key(inputs[i].objectKey);
        itemRequest->set_data_size(inputs[i].expectedSize);
        itemRequest->set_try_lock(true);
        itemRequest->set_read_offset(0);
        itemRequest->set_read_size(inputs[i].expectedSize);

        UrmaRemoteAddrPb itemRemoteAddr = buffer.remoteAddr;
        uint64_t itemRemoteOffset = 0;
        RETURN_IF_NOT_OK(AddUbBatchSize(buffer.remoteAddr.seg_data_offset(), sliceOffset, itemRemoteOffset));
        itemRemoteAddr.set_seg_data_offset(itemRemoteOffset);
        *itemRequest->mutable_urma_info() = std::move(itemRemoteAddr);
        sliceOffsets.emplace_back(sliceOffset);
        RETURN_IF_NOT_OK(AddUbBatchSize(sliceOffset, alignedSizes[i], sliceOffset));
    }
    request.set_urma_instance_id(buffer.transportInstanceId);

    BatchGetObjectRemoteRspPb response;
    std::vector<RpcMessage> payloads;
    if (end - begin > 1) {
        METRIC_INC(metrics::KvMetricId::CLIENT_DIRECT_BATCH_GET_RPC_TOTAL);
        METRIC_ADD(metrics::KvMetricId::CLIENT_DIRECT_BATCH_GET_OBJECT_TOTAL, end - begin);
    }
    RETURN_IF_NOT_OK(rpcClient_->InvokeBatchGetObject(request, response, payloads));
    CHECK_FAIL_RETURN_STATUS(response.responses_size() == static_cast<int>(end - begin), K_RUNTIME_ERROR,
                             "BatchGetObjectRemote response count does not match request count");

    size_t expectedPayloadCount = 0;
    for (size_t i = 0; i < end - begin; ++i) {
        const auto &itemResponse = response.responses(static_cast<int>(i));
        Status itemStatus(static_cast<StatusCode>(itemResponse.error().error_code()),
                          itemResponse.error().error_msg());
        if (!itemStatus.IsOk()) {
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(itemResponse.data_size() >= 0, K_RUNTIME_ERROR,
                                 "UB BatchGetObjectRemote returned a negative data size");
        if (itemResponse.data_source() == DataTransferSource::DATA_IN_PAYLOAD) {
            ++expectedPayloadCount;
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(itemResponse.data_source() == DataTransferSource::DATA_ALREADY_TRANSFERRED,
                                 K_RUNTIME_ERROR, "UB BatchGetObjectRemote returned an invalid data source");
        const uint64_t actualSize = static_cast<uint64_t>(itemResponse.data_size());
        CHECK_FAIL_RETURN_STATUS(actualSize <= inputs[begin + i].expectedSize, K_RUNTIME_ERROR,
                                 "UB batch response exceeds its receive slice");
    }
    CHECK_FAIL_RETURN_STATUS(payloads.size() == expectedPayloadCount, K_RUNTIME_ERROR,
                             "BatchGetObjectRemote payload count does not match payload responses");

    DataGetBatchResult pendingOutputs;
    pendingOutputs.reserve(end - begin);
    size_t payloadIndex = 0;
    for (size_t i = 0; i < end - begin; ++i) {
        const auto &itemResponse = response.responses(static_cast<int>(i));
        DataGetItemResult item;
        item.status = Status(static_cast<StatusCode>(itemResponse.error().error_code()),
                             itemResponse.error().error_msg());
        item.data.response = itemResponse;
        if (item.status.IsOk() && itemResponse.data_source() == DataTransferSource::DATA_IN_PAYLOAD) {
            item.data.kind = AccessTransportKind::TCP;
            item.data.rpcPayloads.emplace_back(std::move(payloads[payloadIndex++]));
        } else if (item.status.IsOk()) {
            const uint64_t actualSize = static_cast<uint64_t>(itemResponse.data_size());
            const uint64_t offset = sliceOffsets[i];
            CHECK_FAIL_RETURN_STATUS(offset <= buffer.size && actualSize <= buffer.size - offset, K_RUNTIME_ERROR,
                                     "UB batch response exceeds aggregate receive buffer");
            CHECK_FAIL_RETURN_STATUS(offset <= std::numeric_limits<size_t>::max(), K_RUNTIME_ERROR,
                                     "UB batch slice offset exceeds addressable memory");
            item.data.externalData = buffer.data + static_cast<size_t>(offset);
            item.data.externalSize = actualSize;
            item.data.externalOwner = buffer.owner;
            item.data.kind = AccessTransportKind::UB;
        }
        pendingOutputs.emplace_back(std::move(item));
    }

    outputs = std::move(pendingOutputs);
    return Status::OK();
}

Status UbTransporter::GetOnce(const DataGetRequest &input, uint64_t expectedSize, DataGetResult &output,
                              uint64_t &actualSize)
{
    output = DataGetResult{};
    GetObjectRemoteReqPb request;
    request.set_object_key(input.objectKey);
    request.set_data_size(expectedSize);
    request.set_try_lock(true);

    UbReceiveBuffer buffer;
    bool useUb = expectedSize > 0 && expectedSize <= bufferProvider_->MaxGetSize();
    if (useUb) {
        Status allocRc = bufferProvider_->Allocate(expectedSize, buffer);
        useUb = allocRc.IsOk() && buffer.data != nullptr && buffer.owner != nullptr && buffer.size >= expectedSize
                && !buffer.transportInstanceId.empty();
    }
    if (useUb) {
        request.set_read_offset(0);
        request.set_read_size(expectedSize);
        *request.mutable_urma_info() = buffer.remoteAddr;
        request.set_urma_instance_id(buffer.transportInstanceId);
    }

    Status rpcRc = rpcClient_->InvokeGetObject(request, output.response, output.rpcPayloads);
    actualSize = output.response.data_size() < 0 ? 0 : static_cast<uint64_t>(output.response.data_size());
    RETURN_IF_NOT_OK(rpcRc);
    Status responseStatus(static_cast<StatusCode>(output.response.error().error_code()),
                          output.response.error().error_msg());
    RETURN_IF_NOT_OK(responseStatus);
    if (!useUb || output.response.data_source() == DataTransferSource::DATA_IN_PAYLOAD) {
        CHECK_FAIL_RETURN_STATUS(output.response.data_source() == DataTransferSource::DATA_IN_PAYLOAD,
                                 K_RUNTIME_ERROR, "GetObjectRemote returned data outside the selected transport");
        output.kind = AccessTransportKind::TCP;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(output.response.data_source() == DataTransferSource::DATA_ALREADY_TRANSFERRED,
                             K_RUNTIME_ERROR, "UB GetObjectRemote returned an invalid data source");
    CHECK_FAIL_RETURN_STATUS(actualSize <= buffer.size, K_RUNTIME_ERROR, "UB response exceeds receive buffer");
    output.externalData = buffer.data;
    output.externalSize = actualSize;
    output.externalOwner = std::move(buffer.owner);
    output.kind = AccessTransportKind::UB;
    return Status::OK();
}

bool UbTransporter::IsAlive() const
{
    std::shared_lock<std::shared_mutex> lock(lifecycleMutex_);
    return rpcClient_ != nullptr && rpcClient_->IsAlive() && conn_ != nullptr && conn_->IsAlive();
}

void UbTransporter::CloseDataPlane()
{
    std::unique_lock<std::shared_mutex> lock(lifecycleMutex_);
    if (conn_ != nullptr) {
        conn_->Teardown();
    }
}

Status UbTransporter::Create(const HostPort &workerAddr, const std::string &key, uint64_t size,
                             const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer)
{
    (void)buffer;
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);

    CreateReqPb createReq;
    RETURN_IF_NOT_OK(BuildCreateRequest(key, size, param, createReq));

    CreateRspPb createRsp;
    uint32_t workerVersion = 0;
    RETURN_IF_NOT_OK(rpcClient_->InvokeCreate(param.subTimeoutMs, createReq, createRsp, workerVersion));

    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = key;
    info->dataSize = size;
    info->workerAddr = workerAddr;
    info->objectMode = ModeInfo(param.consistencyType, param.writeMode, param.cacheType);
    info->ubDataSentByMemoryCopy = false;

    if (createRsp.has_urma_info()) {
#ifdef USE_URMA
        auto urmaInfo = std::make_shared<UrmaRemoteAddrPb>(createRsp.urma_info());
        info->ubUrmaDataInfo = urmaInfo;

        std::shared_ptr<UrmaManager::BufferHandle> handle;
        RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferHandle(handle, size));
        info->pointer = static_cast<uint8_t *>(handle->GetPointer());
        info->ubGetBufferHandle = std::static_pointer_cast<void>(handle);
        // The local UB pool contains payload bytes only. The worker-side metadata offset is already encoded in
        // urmaInfo.seg_data_offset by FillRequestUrmaInfo.
        info->metadataSize = 0;
        info->shmId = ShmKey::Intern(createRsp.shm_id());
        info->version = workerVersion;

        return ObjectBufferInternal::Create(info, buffer);
#else
        static_cast<void>(buffer);
        return Status(K_NOT_SUPPORTED, "UB Create: USE_URMA not compiled");
#endif
    }

    // SHM-in-UB edge case: UB transport but worker returned no URMA info (deferred)
    return Status(K_NOT_SUPPORTED, "UB Create: worker returned no URMA info; SHM-in-UB not yet supported");
}

void UbTransporter::ReleaseMCreateAllocations(const MultiCreateRspPb &response,
                                              const TransportRequestContext &context)
{
    if (rpcClient_ == nullptr) {
        return;
    }
    for (const auto &item : response.results()) {
        if (item.shm_id().empty()) {
            continue;
        }
        Status rc = rpcClient_->InvokeDecreaseReference(context, ShmKey::Intern(item.shm_id()));
        if (rc.IsError()) {
            LOG(WARNING) << "Failed to release MCreate allocation after local setup failure: " << rc;
        }
    }
}

Status UbTransporter::BuildMCreateBuffer(const HostPort &workerAddr, const std::string &key, uint64_t size,
                                         const TransportCreateParam &param, const CreateRspPb &response,
                                         uint32_t workerVersion, std::shared_ptr<ObjectBuffer> &buffer)
{
#ifdef USE_URMA
    CHECK_FAIL_RETURN_STATUS(response.has_urma_info() && !response.shm_id().empty(), K_NOT_SUPPORTED,
                             "UB MCreate response has no URMA allocation");
    std::shared_ptr<UrmaManager::BufferHandle> handle;
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferHandle(handle, size));
    CHECK_FAIL_RETURN_STATUS(handle != nullptr && handle->GetPointer() != nullptr, K_RUNTIME_ERROR,
                             "UB MCreate buffer handle is invalid");
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = key;
    info->dataSize = size;
    info->metadataSize = 0;
    info->workerAddr = workerAddr;
    info->objectMode = ModeInfo(param.consistencyType, param.writeMode, param.cacheType);
    info->ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>(response.urma_info());
    info->ubDataSentByMemoryCopy = false;
    info->pointer = static_cast<uint8_t *>(handle->GetPointer());
    info->ubGetBufferHandle = std::static_pointer_cast<void>(handle);
    info->shmId = ShmKey::Intern(response.shm_id());
    info->version = workerVersion;
    return ObjectBufferInternal::Create(std::move(info), buffer);
#else
    (void)workerAddr;
    (void)key;
    (void)size;
    (void)param;
    (void)response;
    (void)workerVersion;
    (void)buffer;
    return Status(K_NOT_SUPPORTED, "UB MCreate: USE_URMA not compiled");
#endif
}

Status UbTransporter::BuildMCreateBuffers(const HostPort &workerAddr, const std::vector<std::string> &keys,
                                          const std::vector<uint64_t> &sizes, const TransportCreateParam &param,
                                          const MultiCreateRspPb &response, uint32_t workerVersion,
                                          std::vector<std::shared_ptr<ObjectBuffer>> &buffers)
{
    std::vector<std::shared_ptr<ObjectBuffer>> created;
    created.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        std::shared_ptr<ObjectBuffer> buffer;
        RETURN_IF_NOT_OK(BuildMCreateBuffer(workerAddr, keys[i], sizes[i], param,
                                            response.results(static_cast<int>(i)), workerVersion, buffer));
        created.emplace_back(std::move(buffer));
    }
    buffers = std::move(created);
    return Status::OK();
}

Status UbTransporter::MCreate(const HostPort &workerAddr, const std::vector<std::string> &keys,
                              const std::vector<uint64_t> &sizes, const TransportCreateParam &param,
                              std::vector<std::shared_ptr<ObjectBuffer>> &buffers)
{
    std::shared_lock<std::shared_mutex> lifecycleLock(lifecycleMutex_);
    if (rpcClient_ == nullptr || !rpcClient_->IsAlive()) {
        return Status(K_RPC_UNAVAILABLE, "UB MCreate: RPC client not alive");
    }
    MultiCreateReqPb request;
    RETURN_IF_NOT_OK(BuildMultiCreateRequest(keys, sizes, param, request));
    MultiCreateRspPb response;
    uint32_t workerVersion = 0;
    RETURN_IF_NOT_OK(rpcClient_->InvokeMultiCreate(param.subTimeoutMs, request, response, workerVersion));
    if (response.results_size() != static_cast<int>(keys.size())) {
        ReleaseMCreateAllocations(response, param.requestContext);
        return Status(K_RUNTIME_ERROR, "UB MCreate response count does not match request count");
    }

    Status rc = BuildMCreateBuffers(workerAddr, keys, sizes, param, response, workerVersion, buffers);
    if (rc.IsError()) {
        ReleaseMCreateAllocations(response, param.requestContext);
    }
    return rc;
}

Status UbTransporter::WritePayload(ObjectBufferInfo &info)
{
    std::vector<uint64_t> eventKeys;
    return SubmitPayload(info, true, eventKeys);
}

Status UbTransporter::WaitPayloadEvents(std::vector<uint64_t> &eventKeys)
{
    auto remainingTime = []() {
        return TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs());
    };
    auto preserveError = [](Status &rc) { return rc; };
    return WaitFastTransportEvent(eventKeys, remainingTime, preserveError);
}

size_t UbTransporter::GetMSetPipelineDepth()
{
    static constexpr size_t MSET_URMA_MAX_PIPELINE_DEPTH = 32;
    const auto lanePoolSize = static_cast<size_t>(FLAGS_urma_send_jetty_lane_pool_size);
    return std::max<size_t>(1, std::min(MSET_URMA_MAX_PIPELINE_DEPTH, lanePoolSize));
}

Status UbTransporter::SubmitPayload(ObjectBufferInfo &info, bool blocking, std::vector<uint64_t> &eventKeys)
{
#ifdef USE_URMA
    auto handle = std::static_pointer_cast<UrmaManager::BufferHandle>(info.ubGetBufferHandle);
    CHECK_FAIL_RETURN_STATUS(handle != nullptr, K_RUNTIME_ERROR, "UB Set: buffer handle is null");
    CHECK_FAIL_RETURN_STATUS(info.ubUrmaDataInfo != nullptr, K_RUNTIME_ERROR, "UB Set: remote address is null");
    CHECK_FAIL_RETURN_STATUS(info.pointer != nullptr, K_RUNTIME_ERROR, "UB Set: payload pointer is null");

    auto segment = UrmaManager::Instance().GetLocalSegmentInfo();
    const uint8_t srcChipId = NumaIdToChipId(handle->GetNumaId());
    const uint8_t dstChipId = info.ubUrmaDataInfo->has_chip_id()
                                  ? static_cast<uint8_t>(info.ubUrmaDataInfo->chip_id())
                                  : INVALID_CHIP_ID;
    return UrmaWritePayload(*(info.ubUrmaDataInfo), segment.first, segment.second,
                            reinterpret_cast<uint64_t>(info.pointer), 0, info.dataSize, info.metadataSize,
                            srcChipId, dstChipId, blocking, eventKeys);
#else
    (void)info;
    (void)blocking;
    (void)eventKeys;
    return Status(K_NOT_SUPPORTED, "UB Set: USE_URMA not compiled");
#endif
}

Status UbTransporter::WritePayloads(const std::vector<ObjectBufferInfo *> &infos, std::vector<Status> &statuses)
{
    statuses.assign(infos.size(), Status::OK());
    std::vector<std::vector<uint64_t>> eventKeys(infos.size());
    const size_t pipelineDepth = GetMSetPipelineDepth();
    size_t completedPayloads = 0;
    for (size_t begin = 0; begin < infos.size(); begin += pipelineDepth) {
        const size_t end = std::min(begin + pipelineDepth, infos.size());
        std::shared_lock<std::shared_mutex> lifecycleLock(lifecycleMutex_);
        if (conn_ == nullptr || !conn_->IsAlive()) {
            return Status(K_URMA_NEED_CONNECT,
                          FormatString("UB MSet: connection not alive before batch [%zu, %zu), completed=%zu/%zu",
                                       begin, end, completedPayloads, infos.size()));
        }
        for (size_t i = begin; i < end; ++i) {
            statuses[i] = SubmitPayload(*infos[i], false, eventKeys[i]);
        }
        for (size_t i = begin; i < end; ++i) {
            if (statuses[i].IsOk()) {
                statuses[i] = WaitPayloadEvents(eventKeys[i]);
            }
            if (statuses[i].IsOk()) {
                infos[i]->ubDataSentByMemoryCopy = true;
                METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_URMA_WRITE_TOTAL_BYTES, infos[i]->dataSize);
                ++completedPayloads;
            }
        }
    }
    return Status::OK();
}

Status UbTransporter::Set(ObjectBuffer &buffer, const TransportSetParam &param)
{
    // Keep the per-transporter lifecycle lock while the operation is in flight so CloseDataPlane cannot tear down
    // the UB connection between the liveness check and the write/publish sequence.
    std::shared_lock<std::shared_mutex> lifecycleLock(lifecycleMutex_);
    if (rpcClient_ == nullptr || !rpcClient_->IsAlive()) {
        return Status(K_RPC_UNAVAILABLE, "UB Set: RPC client not alive");
    }
    if (conn_ == nullptr || !conn_->IsAlive()) {
        return Status(K_URMA_NEED_CONNECT, "UB Set: UB connection not alive");
    }
    auto rpcClient = rpcClient_;

    ObjectBufferInfo &info = ObjectBufferInternal::GetMutableInfo(buffer);
    PublishReqPb pubReq;
    RETURN_IF_NOT_OK(BuildSetRequest(info, param, pubReq));
    // URMA write path: data already in pool buffer via user MemoryCopy.
    Status writeRc(K_URMA_ERROR, "URMA transport is unavailable");
    if (!info.ubDataSentByMemoryCopy && info.ubUrmaDataInfo != nullptr) {
        writeRc = WritePayload(info);
        if (writeRc.IsOk()) {
            info.ubDataSentByMemoryCopy = true;
            METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_URMA_WRITE_TOTAL_BYTES, info.dataSize);
        }
    }

    PublishRspPb rsp;
    uint32_t workerVersion = 0;

    if (info.ubDataSentByMemoryCopy) {
        // URMA write succeeded: no TCP payload
        std::vector<MemView> payloads;
        RETURN_IF_NOT_OK(rpcClient->InvokeSet(param.subTimeoutMs, pubReq, payloads, rsp, workerVersion));
    } else {
        // TCP fallback: send data as payload through RPC
        UrmaFallbackTcpLimiter::Ticket ticket;
        RETURN_IF_NOT_OK(UrmaFallbackTcpLimiter::TryAcquire(
            urmaFallbackTcpPendingBytes_, info.dataSize, writeRc,
            "client->worker", ticket));

        MemView payload(info.pointer + info.metadataSize, info.dataSize);
        std::vector<MemView> payloads{ payload };
        RETURN_IF_NOT_OK(rpcClient->InvokeSet(param.subTimeoutMs, pubReq, payloads, rsp, workerVersion));
    }

    const auto kind = info.ubDataSentByMemoryCopy ? AccessTransportKind::UB : AccessTransportKind::TCP;
    return SetTransportResponseStatus(rsp, kind, param.isSeal, param.isRetry);
}

void UbTransporter::ClassifyMSetPayload(
    const std::shared_ptr<ObjectBuffer> &buffer, const Status &writeRc,
    std::vector<std::shared_ptr<ObjectBuffer>> &publishBuffers, std::vector<bool> &tcpPayload,
    std::vector<UrmaFallbackTcpLimiter::Ticket> &fallbackTickets, uint64_t &fallbackBytes,
    TransportMSetResult &result)
{
    auto &info = ObjectBufferInternal::GetMutableInfo(*buffer);
    if (writeRc.IsOk()) {
        info.ubDataSentByMemoryCopy = true;
        METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_URMA_WRITE_TOTAL_BYTES, info.dataSize);
        publishBuffers.emplace_back(buffer);
        tcpPayload.emplace_back(false);
        return;
    }
    info.ubDataSentByMemoryCopy = false;
    UrmaFallbackTcpLimiter::Ticket ticket;
    Status acquireRc = UrmaFallbackTcpLimiter::TryAcquire(
        urmaFallbackTcpPendingBytes_, info.dataSize, writeRc, "client->worker", ticket);
    if (acquireRc.IsError()) {
        result.failedKeys.emplace_back(info.objectKey);
        result.lastRc = acquireRc;
        return;
    }
    fallbackBytes += info.dataSize;
    fallbackTickets.emplace_back(std::move(ticket));
    publishBuffers.emplace_back(buffer);
    tcpPayload.emplace_back(true);
}

Status UbTransporter::PrepareMSetPayloads(
    const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
    std::vector<std::shared_ptr<ObjectBuffer>> &publishBuffers, std::vector<bool> &tcpPayload,
    std::vector<UrmaFallbackTcpLimiter::Ticket> &fallbackTickets, uint64_t &fallbackBytes,
    TransportMSetResult &result)
{
    std::vector<ObjectBufferInfo *> pendingInfos;
    std::vector<size_t> pendingIndexes;
    for (size_t i = 0; i < buffers.size(); ++i) {
        auto &info = ObjectBufferInternal::GetMutableInfo(*buffers[i]);
        if (!info.ubDataSentByMemoryCopy) {
            pendingInfos.emplace_back(&info);
            pendingIndexes.emplace_back(i);
        }
    }
    std::vector<Status> writeStatuses;
    RETURN_IF_NOT_OK(WritePayloads(pendingInfos, writeStatuses));
    CHECK_FAIL_RETURN_STATUS(writeStatuses.size() == pendingInfos.size(), K_RUNTIME_ERROR,
                             "UB MSet write status count does not match pending payload count");

    publishBuffers.reserve(buffers.size());
    tcpPayload.reserve(buffers.size());
    fallbackTickets.reserve(buffers.size());
    size_t pending = 0;
    for (size_t i = 0; i < buffers.size(); ++i) {
        auto &info = ObjectBufferInternal::GetMutableInfo(*buffers[i]);
        const bool wasPending = pending < pendingIndexes.size() && pendingIndexes[pending] == i;
        if (info.ubDataSentByMemoryCopy) {
            publishBuffers.emplace_back(buffers[i]);
            tcpPayload.emplace_back(false);
            pending += static_cast<size_t>(wasPending);
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(wasPending, K_RUNTIME_ERROR,
                                 "UB MSet pending payload index mismatch");
        ClassifyMSetPayload(buffers[i], writeStatuses[pending++], publishBuffers, tcpPayload, fallbackTickets,
                            fallbackBytes, result);
    }
    CHECK_FAIL_RETURN_STATUS(pending == pendingIndexes.size(), K_RUNTIME_ERROR,
                             "UB MSet pending payloads were not fully classified");
    return Status::OK();
}

Status UbTransporter::PublishMSet(const std::shared_ptr<WorkerRpcClient> &rpcClient,
                                  const std::vector<std::shared_ptr<ObjectBuffer>> &publishBuffers,
                                  const std::vector<bool> &tcpPayload, const TransportSetParam &param,
                                  uint64_t fallbackBytes, TransportMSetResult &result)
{
    if (publishBuffers.empty()) {
        return result.lastRc.IsError() ? result.lastRc : Status(K_RUNTIME_ERROR, "All UB MSet payloads failed");
    }
    MultiPublishReqPb request;
    std::vector<MemView> payloads;
    RETURN_IF_NOT_OK(BuildMultiPublishRequest(publishBuffers, tcpPayload, param, request, payloads));
    MultiPublishRspPb response;
    uint32_t workerVersion = 0;
    Status invokeRc;
    {
        std::shared_lock<std::shared_mutex> lifecycleLock(lifecycleMutex_);
        if (rpcClient_ != rpcClient || !rpcClient_->IsAlive()) {
            return Status(K_RPC_UNAVAILABLE, "UB MSet: RPC client changed before publish");
        }
        result.publishAttempted = true;
        invokeRc = rpcClient->InvokeMultiSet(param.subTimeoutMs, request, payloads, response, workerVersion);
    }
    if (invokeRc.IsError()) {
        for (const auto &buffer : publishBuffers) {
            result.failedKeys.emplace_back(ObjectBufferInternal::GetInfo(*buffer).objectKey);
        }
        result.lastRc = invokeRc;
        return invokeRc;
    }
    const bool hasFallback = std::any_of(tcpPayload.begin(), tcpPayload.end(), [](bool value) { return value; });
    METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_TCP_WRITE_TOTAL_BYTES, fallbackBytes);
    const auto kind = hasFallback ? AccessTransportKind::TCP : AccessTransportKind::UB;
    TransportMSetResult publishedResult;
    Status publishRc = SetMSetResponseResult(response, publishBuffers.size(), kind, publishedResult);
    result.failedKeys.insert(result.failedKeys.end(), publishedResult.failedKeys.begin(),
                             publishedResult.failedKeys.end());
    if (publishRc.IsError() && publishedResult.failedKeys.empty()) {
        for (const auto &buffer : publishBuffers) {
            result.failedKeys.emplace_back(ObjectBufferInternal::GetInfo(*buffer).objectKey);
        }
    }
    // Keep the first failure so a local fallback rejection is not hidden by a later worker-side partial failure.
    if (result.lastRc.IsOk() && publishedResult.lastRc.IsError()) {
        result.lastRc = publishedResult.lastRc;
    }
    result.actualKind = publishedResult.actualKind;
    return publishRc;
}

Status UbTransporter::MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                           const TransportSetParam &param, TransportMSetResult &result)
{
    result.Clear();
    std::shared_ptr<WorkerRpcClient> rpcClient;
    std::vector<std::shared_ptr<ObjectBuffer>> publishBuffers;
    std::vector<bool> tcpPayload;
    std::vector<UrmaFallbackTcpLimiter::Ticket> fallbackTickets;
    uint64_t fallbackBytes = 0;
    {
        std::shared_lock<std::shared_mutex> lifecycleLock(lifecycleMutex_);
        if (rpcClient_ == nullptr || !rpcClient_->IsAlive()) {
            return Status(K_RPC_UNAVAILABLE, "UB MSet: RPC client not alive");
        }
        if (conn_ == nullptr || !conn_->IsAlive()) {
            return Status(K_URMA_NEED_CONNECT, "UB MSet: UB connection not alive");
        }
        rpcClient = rpcClient_;
    }
    RETURN_IF_NOT_OK(PrepareMSetPayloads(buffers, publishBuffers, tcpPayload, fallbackTickets, fallbackBytes,
                                         result));
    return PublishMSet(rpcClient, publishBuffers, tcpPayload, param, fallbackBytes, result);
}

Status UbTransporter::Release(const ShmKey &shmId, const TransportRequestContext &context)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    if (shmId.Empty()) {
        return Status::OK();
    }
    return rpcClient_->InvokeDecreaseReference(context, shmId);
}

}  // namespace client
}  // namespace datasystem
