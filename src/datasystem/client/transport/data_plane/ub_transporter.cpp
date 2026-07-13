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

#include <cstdint>
#include <mutex>
#include <utility>

#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/status_helper.h"

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

}  // namespace client
}  // namespace datasystem
