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

#include <cstdint>
#include <cstring>
#include <mutex>
#include <utility>

#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/util/numa_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/object/object_buffer.h"

#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif

namespace datasystem {
namespace client {
namespace {
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

}  // namespace

UbTransporter::UbTransporter(std::shared_ptr<WorkerRpcClient> rpcClient, std::shared_ptr<UbConnection> conn,
                             std::shared_ptr<IUbReceiveBufferProvider> bufferProvider)
    : rpcClient_(std::move(rpcClient)), conn_(std::move(conn)), bufferProvider_(std::move(bufferProvider))
{
    if (bufferProvider_ == nullptr) {
        bufferProvider_ = std::make_shared<DefaultUbReceiveBufferProvider>();
    }
}

Status UbTransporter::Get(const DataGetRequest &input, DataGetResult &output)
{
    std::shared_lock<std::shared_mutex> lock(lifecycleMutex_);
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

Status UbTransporter::GetOnce(const DataGetRequest &input, uint64_t expectedSize, DataGetResult &output,
                              uint64_t &actualSize)
{
    output = DataGetResult{};
    GetObjectRemoteReqPb request;
    request.set_object_key(input.objectKey);
    request.set_data_size(expectedSize);
    request.set_try_lock(true);

    uint8_t *buffer = nullptr;
    uint64_t bufferSize = 0;
    std::shared_ptr<IReceiveBufferOwner> owner;
    UrmaRemoteAddrPb remoteAddr;
    bool useUb = expectedSize > 0 && expectedSize <= bufferProvider_->MaxGetSize();
    if (useUb) {
        Status allocRc = bufferProvider_->Allocate(expectedSize, buffer, bufferSize, remoteAddr, owner);
        useUb = allocRc.IsOk() && buffer != nullptr && owner != nullptr && bufferSize >= expectedSize;
    }
    if (useUb) {
        request.set_read_offset(0);
        request.set_read_size(expectedSize);
        *request.mutable_urma_info() = remoteAddr;
        std::string transportInstanceId;
        if (GetLocalTransportInstanceId(transportInstanceId).IsOk()) {
            request.set_urma_instance_id(transportInstanceId);
        }
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
    CHECK_FAIL_RETURN_STATUS(actualSize <= bufferSize, K_RUNTIME_ERROR, "UB response exceeds receive buffer");
    output.externalData = buffer;
    output.externalSize = actualSize;
    output.externalOwner = std::move(owner);
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
        return Status(K_NOT_SUPPORTED, "UB Create: USE_URMA not compiled");
#endif
    }

    // SHM-in-UB edge case: UB transport but worker returned no URMA info (deferred)
    return Status(K_NOT_SUPPORTED, "UB Create: worker returned no URMA info; SHM-in-UB not yet supported");
}

Status UbTransporter::WritePayload(ObjectBufferInfo &info)
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
    std::vector<uint64_t> eventKeys;
    return UrmaWritePayload(*(info.ubUrmaDataInfo), segment.first, segment.second,
                            reinterpret_cast<uint64_t>(info.pointer), 0, info.dataSize, info.metadataSize,
                            srcChipId, dstChipId, true, eventKeys);
#else
    (void)info;
    return Status(K_NOT_SUPPORTED, "UB Set: USE_URMA not compiled");
#endif
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
    RETURN_IF_NOT_OK(ValidateSetRequest(info, param));

    // URMA write path: data already in pool buffer via user MemoryCopy.
    Status writeRc(K_URMA_ERROR, "URMA transport is unavailable");
    if (!info.ubDataSentByMemoryCopy && info.ubUrmaDataInfo != nullptr) {
        writeRc = WritePayload(info);
        if (writeRc.IsOk()) {
            info.ubDataSentByMemoryCopy = true;
            METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_URMA_WRITE_TOTAL_BYTES, info.dataSize);
        }
    }

    PublishReqPb pubReq;
    RETURN_IF_NOT_OK(BuildSetRequest(info, param, pubReq));

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
