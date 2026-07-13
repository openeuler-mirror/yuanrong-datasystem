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
#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/data_plane/data_plane_executor.h"
#include "datasystem/client/transport/metadata/object_metadata_client.h"
#include "datasystem/client/transport/object_read/object_read_flow.h"
#include "datasystem/client/transport/object_read/replica_reader.h"
#include "datasystem/client/transport/transport_advisor.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
TransportLayer::TransportLayer(std::shared_ptr<Signature> signature, std::shared_ptr<ThreadPool> taskPool,
                               uint64_t fastTransportMemSize, BrpcChannelConfig channelConfig)
{
    manager_ =
        std::make_shared<DataPlaneManager>(std::move(signature), fastTransportMemSize, std::move(channelConfig));
    auto retry = std::make_shared<DeadlineRetry>();
    auto metadata = std::make_shared<ObjectMetadataClient>(manager_, retry);
    auto executor = std::make_shared<DataPlaneExecutor>(manager_, std::make_shared<TransportAdvisor>());
    auto replicas = std::make_shared<ReplicaReader>(std::move(executor), std::move(retry));
    objectRead_ = std::make_unique<ObjectReadFlow>(std::move(metadata), std::move(replicas), std::move(taskPool));
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

void TransportLayer::Shutdown()
{
    objectRead_.reset();
    if (manager_ != nullptr) {
        manager_->Shutdown();
    }
}
}  // namespace client
}  // namespace datasystem
