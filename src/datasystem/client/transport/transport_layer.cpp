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

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

TransportLayer::TransportLayer(std::shared_ptr<ClientRequestAuth> auth, uint64_t fastTransportMemSize,
                               BrpcChannelConfig channelConfig)
    : dm_(std::make_shared<DataPlaneManager>(std::move(auth), fastTransportMemSize, std::move(channelConfig))),
      advisor_(std::make_shared<TransportAdvisor>())
{}

TransportLayer::TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager,
                               std::shared_ptr<TransportAdvisor> advisor)
    : dm_(std::move(dataPlaneManager)), advisor_(std::move(advisor))
{
}

TransportLayer::~TransportLayer()
{
    Shutdown();
}

Status TransportLayer::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(dm_);
    return dm_->Init();
}

Status TransportLayer::Get(const HostPort &workerAddr, const TransportGetRequest &input, TransportGetResult &output)
{
    TransportHint hint = advisor_->GetTransportHint(workerAddr);
    std::shared_ptr<IDataTransporter> tp;
    Status rc = dm_->GetOrCreate(workerAddr, hint, tp);
    RETURN_IF_NOT_OK(rc);
    rc = tp->Get(input, output);
    if (rc.GetCode() == K_URMA_NEED_CONNECT) {
        LOG(WARNING) << "Rebuild UB data plane for worker " << workerAddr.ToString() << " after Get failed: " << rc;
        dm_->ResetDataPlane(workerAddr);
    } else if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after Get failed: " << rc;
        dm_->Teardown(workerAddr);
    } else {
        return rc;
    }
    rc = dm_->GetOrCreate(workerAddr, hint, tp);
    RETURN_IF_NOT_OK(rc);
    rc = tp->Get(input, output);
    if (rc.IsError()) {
        LOG(WARNING) << "Get still failed after rebuilding transport for worker " << workerAddr.ToString() << ": "
                     << rc;
    }
    return rc;
}

void TransportLayer::Shutdown()
{
    if (dm_ != nullptr) {
        dm_->Shutdown();
    }
}
}  // namespace client
}  // namespace datasystem
