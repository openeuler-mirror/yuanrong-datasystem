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

/** Description: Implements endpoint data-plane execution and scoped connection rebuild. */

#include "datasystem/client/transport/data_plane/data_plane_executor.h"

#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

DataPlaneExecutor::DataPlaneExecutor(std::shared_ptr<DataPlaneManager> manager,
                                     std::shared_ptr<TransportAdvisor> advisor)
    : manager_(std::move(manager)), advisor_(std::move(advisor))
{
}

Status DataPlaneExecutor::Execute(const HostPort &workerAddr, const Operation &operation)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(operation), K_INVALID, "Data-plane operation is empty");
    const TransportHint hint = advisor_->GetTransportHint(workerAddr);
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    RETURN_RUNTIME_ERROR_IF_NULL(transporter);
    Status rc = operation(*transporter);
    if (rc.GetCode() == K_URMA_NEED_CONNECT) {
        VLOG(1) << "[TransportGet][Connection] Rebuild data plane, worker: " << workerAddr.ToString()
                << ", status: " << rc.ToString();
        manager_->ResetDataPlane(workerAddr);
    } else if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        VLOG(1) << "[TransportGet][Connection] Rebuild RPC connection, worker: " << workerAddr.ToString()
                << ", status: " << rc.ToString();
        manager_->Teardown(workerAddr);
    } else {
        return rc;
    }
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    RETURN_RUNTIME_ERROR_IF_NULL(transporter);
    return operation(*transporter);
}
}  // namespace client
}  // namespace datasystem
