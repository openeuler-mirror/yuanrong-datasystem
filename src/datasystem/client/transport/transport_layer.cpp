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

/** Description: Implements the client transport facade. */

#include "datasystem/client/transport/transport_layer.h"

#include <utility>

#include "datasystem/client/transport/common/deadline_retry.h"
#include "datasystem/client/transport/data_plane/data_plane_executor.h"
#include "datasystem/client/transport/metadata/object_metadata_client.h"
#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/client/transport/object_read/replica_reader.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

TransportLayer::TransportLayer(std::shared_ptr<Signature> signature, std::shared_ptr<ThreadPool> taskPool,
                               uint64_t fastTransportMemSize, BrpcChannelConfig channelConfig)
    : advisor_(std::make_shared<TransportAdvisor>())
{
    manager_ =
        std::make_shared<DataPlaneManager>(std::move(signature), fastTransportMemSize, std::move(channelConfig));
    auto retry = std::make_shared<DeadlineRetry>();
    auto metadata = std::make_shared<ObjectMetadataClient>(manager_, retry);
    auto executor = std::make_shared<DataPlaneExecutor>(manager_, advisor_);
    auto replicas = std::make_shared<ReplicaReader>(std::move(executor), std::move(retry));
    objectRead_ = std::make_unique<ObjectReadFlow>(std::move(metadata), std::move(replicas), std::move(taskPool));
}

TransportLayer::TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager,
                               std::shared_ptr<TransportAdvisor> advisor)
    : manager_(std::move(dataPlaneManager)), advisor_(std::move(advisor))
{
}

TransportLayer::~TransportLayer()
{
    Shutdown();
}

Status TransportLayer::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    return manager_->Init();
}

Status TransportLayer::Get(const ObjectReadRequest &input, ObjectReadResult &output)
{
    RETURN_RUNTIME_ERROR_IF_NULL(objectRead_);
    return objectRead_->Run(input, output);
}

Status TransportLayer::Create(const HostPort &workerAddr, const std::string &objectKey, uint64_t dataSize,
                              const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer)
{
    RETURN_IF_NOT_OK(ValidateCreateRequest(objectKey, dataSize, param));
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const TransportHint hint = advisor_->GetTransportHint(workerAddr);
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    Status rc = transporter->Create(workerAddr, objectKey, dataSize, param, buffer);
    if (rc.GetCode() != K_RPC_UNAVAILABLE) {
        return rc;
    }
    LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                 << " after Create failed: " << rc;
    manager_->Teardown(workerAddr);
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    rc = transporter->Create(workerAddr, objectKey, dataSize, param, buffer);
    if (rc.IsError()) {
        LOG(WARNING) << "Create still failed after rebuilding transport for worker " << workerAddr.ToString()
                     << ": " << rc;
    }
    return rc;
}

Status TransportLayer::Set(ObjectBuffer &buffer, const TransportSetParam &param)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const HostPort workerAddr = ObjectBufferInternal::GetInfo(buffer).workerAddr;
    const TransportHint hint = advisor_->GetTransportHint(workerAddr);
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    TransportSetParam retryParam = param;
    Status rc = transporter->Set(buffer, retryParam);
    if (rc.GetCode() == K_URMA_NEED_CONNECT) {
        LOG(WARNING) << "Rebuild UB data plane for worker " << workerAddr.ToString()
                     << " after Set failed: " << rc;
        manager_->ResetDataPlane(workerAddr);
    } else if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after Set failed: " << rc;
        manager_->Teardown(workerAddr);
    } else {
        return rc;
    }
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    retryParam.isRetry = true;
    rc = transporter->Set(buffer, retryParam);
    if (rc.IsError()) {
        LOG(WARNING) << "Set still failed after rebuilding transport for worker " << workerAddr.ToString()
                     << ": " << rc;
    }
    return rc;
}

void TransportLayer::Shutdown()
{
    objectRead_.reset();
    if (manager_ != nullptr) {
        manager_->Shutdown();
    }
}

}  // namespace client
}  // namespace datasystem
