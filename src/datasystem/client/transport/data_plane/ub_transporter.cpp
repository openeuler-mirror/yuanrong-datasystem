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

/** Description: Implements the UB object Get transporter. */

#include "datasystem/client/transport/data_plane/ub_transporter.h"

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <numeric>
#include <utility>

#include "datasystem/client/transport/rpc/get_request_builder.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/kv_client.h"

#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif

namespace datasystem {
namespace client {
namespace {

std::vector<GetBatchPlan> BuildBatchPlans(const std::vector<uint64_t> &objectSizes, uint64_t maxGetSize)
{
    std::vector<GetBatchPlan> batches;
    GetBatchPlan current;
    for (size_t i = 0; i < objectSizes.size(); ++i) {
        const uint64_t objectSize = objectSizes[i];
        if (objectSize == 0 || objectSize > maxGetSize) {
            if (!current.requestIndices.empty()) {
                batches.emplace_back(std::move(current));
                current = GetBatchPlan{};
            }
            batches.push_back(GetBatchPlan{ { i }, objectSize, false });
            continue;
        }
        if (!current.requestIndices.empty() && objectSize > maxGetSize - current.totalSize) {
            batches.emplace_back(std::move(current));
            current = GetBatchPlan{};
        }
        current.requestIndices.push_back(i);
        current.totalSize += objectSize;
    }
    if (!current.requestIndices.empty()) {
        batches.emplace_back(std::move(current));
    }
    return batches;
}

AccessTransportKind MergeTransportKind(AccessTransportKind lhs, AccessTransportKind rhs)
{
    return static_cast<AccessTransportKind>(
        std::max(static_cast<uint8_t>(lhs), static_cast<uint8_t>(rhs)));
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

    Status Allocate(uint64_t requiredSize, uint8_t *&data, uint64_t &size, UrmaRemoteAddrPb &remoteAddr,
                    std::shared_ptr<IReceiveBufferOwner> &owner) override
    {
        data = nullptr;
        size = 0;
        owner.reset();
#ifdef USE_URMA
        std::shared_ptr<UrmaManager::BufferHandle> handle;
        RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferHandle(handle, requiredSize));
        CHECK_FAIL_RETURN_STATUS(handle != nullptr, K_RUNTIME_ERROR, "UB receive buffer handle is null");
        RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferInfo(handle, data, size, remoteAddr));
        owner = std::make_shared<UbReceiveBufferOwner>(std::move(handle));
        return Status::OK();
#else
        (void)requiredSize;
        (void)remoteAddr;
        return Status(K_NOT_SUPPORTED, "USE_URMA not compiled");
#endif
    }
};

Status FillExternalPayloads(uint8_t *buffer, uint64_t bufferSize, TransportGetBatchResult &batch,
                            bool &hasTcpFallback)
{
    hasTcpFallback = false;
    uint64_t offset = 0;
    for (int i = 0; i < batch.response.payload_info_size(); ++i) {
        const auto &payloadInfo = batch.response.payload_info(i);
        if (payloadInfo.part_index_size() == 0) {
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(payloadInfo.data_size() >= 0, K_RUNTIME_ERROR,
                                                 "Invalid negative UB payload size");
            const uint64_t dataSize = static_cast<uint64_t>(payloadInfo.data_size());
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(offset <= bufferSize && dataSize <= bufferSize - offset,
                                                 K_RUNTIME_ERROR, "UB payload exceeds receive buffer");
            batch.externalPayloads.push_back(
                ExternalPayloadSpan{ static_cast<size_t>(i), buffer + offset, dataSize });
            offset += dataSize;
            continue;
        }
        hasTcpFallback = true;
        for (int j = 0; j < payloadInfo.part_index_size(); ++j) {
            const int32_t partIndex = payloadInfo.part_index(j);
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                partIndex >= 0 && static_cast<size_t>(partIndex) < batch.rpcPayloads.size(), K_RUNTIME_ERROR,
                "UB fallback payload index exceeds payload size");
        }
    }
    return Status::OK();
}

struct UrmaBufferCtx {
    const UrmaRemoteAddrPb *info = nullptr;
    uint64_t size = 0;
};

Status InvokeBatch(WorkerRpcClient &rpcClient, const TransportGetRequest &input, const GetBatchPlan &plan,
                   TransportGetBatchResult &batch, const UrmaBufferCtx &urma)
{
    batch.requestIndices = plan.requestIndices;
    GetReqPb request;
    RETURN_IF_NOT_OK(BuildGetRequest(input, plan.requestIndices, request));
    if (urma.info != nullptr) {
        *request.mutable_urma_info() = *urma.info;
        request.set_ub_buffer_size(urma.size);
    }
    RETURN_IF_NOT_OK(
        rpcClient.InvokeGet(input.subTimeoutMs, request, batch.response, batch.rpcPayloads, batch.workerVersion));
    return GetTransportResponseStatus(batch.response, AccessTransportKind::UB);
}

}  // namespace

UbTransporter::UbTransporter(std::shared_ptr<WorkerRpcClient> rpcClient, std::shared_ptr<UbConnection> conn,
                             std::shared_ptr<IUbReceiveBufferProvider> bufferProvider)
    : rpcClient_(std::move(rpcClient)), conn_(std::move(conn)), bufferProvider_(std::move(bufferProvider))
{
    if (bufferProvider_ == nullptr) {
        bufferProvider_ = std::make_shared<DefaultUbReceiveBufferProvider>();
    }
}

Status UbTransporter::Get(const TransportGetRequest &input, TransportGetResult &output)
{
    std::shared_lock<std::shared_mutex> lock(lifecycleMutex_);
    output.Clear();
    RETURN_IF_NOT_OK(ValidateGetRequest(input));
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    if (conn_ == nullptr || !conn_->IsAlive()) {
        return Status(K_URMA_NEED_CONNECT, "UB connection not alive");
    }
    std::vector<uint64_t> objectSizes;
    RETURN_IF_NOT_OK(rpcClient_->QueryObjectSizes(input.objectKeys, objectSizes));

    const uint64_t maxGetSize = bufferProvider_->MaxGetSize();
    CHECK_FAIL_RETURN_STATUS(maxGetSize > 0, K_NOT_SUPPORTED, "UB Get receive buffer is unavailable");
    std::vector<GetBatchPlan> plans;
    if (objectSizes.size() == input.objectKeys.size()) {
        plans = BuildBatchPlans(objectSizes, maxGetSize);
    } else {
        GetBatchPlan tcpPlan;
        tcpPlan.requestIndices.resize(input.objectKeys.size());
        std::iota(tcpPlan.requestIndices.begin(), tcpPlan.requestIndices.end(), 0);
        tcpPlan.useUb = false;
        plans.emplace_back(std::move(tcpPlan));
    }

    bool firstBatch = true;
    for (const auto &plan : plans) {
        TransportGetBatchResult batch;
        RETURN_IF_NOT_OK(ExecutePlan(input, plan, batch));
        output.actualKind = firstBatch ? batch.kind : MergeTransportKind(output.actualKind, batch.kind);
        firstBatch = false;
        output.batches.emplace_back(std::move(batch));
    }
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

Status UbTransporter::ExecutePlan(const TransportGetRequest &input, const GetBatchPlan &plan,
                                  TransportGetBatchResult &batch)
{
    bool useUb = plan.useUb;
    uint8_t *buffer = nullptr;
    uint64_t bufferSize = 0;
    UrmaRemoteAddrPb urmaInfo;
    std::shared_ptr<IReceiveBufferOwner> owner;
    if (useUb) {
        Status rc = bufferProvider_->Allocate(plan.totalSize, buffer, bufferSize, urmaInfo, owner);
        if (rc.IsError() || owner == nullptr || buffer == nullptr || bufferSize < plan.totalSize) {
            owner.reset();
            buffer = nullptr;
            bufferSize = 0;
            useUb = false;
        }
    }

    Status rc = useUb ? InvokeBatch(*rpcClient_, input, plan, batch, { &urmaInfo, bufferSize })
                      : InvokeBatch(*rpcClient_, input, plan, batch, {});
    RETURN_IF_NOT_OK(rc);

    batch.kind = useUb ? AccessTransportKind::UB : AccessTransportKind::TCP;
    if (useUb) {
        bool hasTcpFallback = false;
        RETURN_IF_NOT_OK(FillExternalPayloads(buffer, bufferSize, batch, hasTcpFallback));
        if (hasTcpFallback) {
            batch.kind = AccessTransportKind::TCP;
        }
        batch.externalOwner = std::move(owner);
    }
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
