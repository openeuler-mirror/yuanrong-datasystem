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

#include <cstddef>
#include <utility>

#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {
constexpr size_t INITIAL_ATTEMPT = 1;
constexpr size_t REBUILD_ATTEMPT = 2;
// Rebuild/teardown events are recoverable and can recur on every request during sustained instability;
// sample them like the other transport degradation WARNINGs. Terminal failures stay unsampled.
constexpr int TRANSPORT_DIAG_LOG_RATE = 100;

void LogDataPlaneOperation(const HostPort &workerAddr, AccessTransportKind kind, size_t attempt,
                           const Status &status)
{
    const bool rebuildRequired =
        status.GetCode() == K_URMA_NEED_CONNECT || status.GetCode() == K_RPC_UNAVAILABLE;
    if (status.IsError() && (!rebuildRequired || attempt >= REBUILD_ATTEMPT)) {
        LOG(ERROR) << "[TransportGet][DataPlane] Operation failed, worker: " << workerAddr.ToString()
                   << ", transport: " << AccessTransportTracker::KindToName(kind) << ", attempt: " << attempt
                   << ", status: " << status.ToString();
    } else if (!rebuildRequired) {
        VLOG(1) << "[TransportGet][DataPlane] Operation completed, worker: " << workerAddr.ToString()
                << ", transport: " << AccessTransportTracker::KindToName(kind) << ", attempt: " << attempt
                << ", status: " << status.ToString();
    }
}
}  // namespace

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
    Status connectionStatus = manager_->GetOrCreate(workerAddr, hint, transporter);
    if (connectionStatus.IsError()) {
        LOG(ERROR) << "[TransportGet][Connection] Get data plane failed, worker: " << workerAddr.ToString()
                   << ", attempt: " << INITIAL_ATTEMPT << ", status: " << connectionStatus.ToString();
        return connectionStatus;
    }
    RETURN_RUNTIME_ERROR_IF_NULL(transporter);
    VLOG(1) << "[TransportGet][DataPlane] Send operation, worker: " << workerAddr.ToString()
            << ", transport: " << AccessTransportTracker::KindToName(transporter->Kind())
            << ", attempt: " << INITIAL_ATTEMPT;
    Status rc = operation(*transporter);
    LogDataPlaneOperation(workerAddr, transporter->Kind(), INITIAL_ATTEMPT, rc);
    if (rc.GetCode() == K_URMA_NEED_CONNECT) {
        LOG_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE)
            << "[TransportGet][Connection] Rebuild data plane, worker: " << workerAddr.ToString()
            << ", transport: " << AccessTransportTracker::KindToName(transporter->Kind())
            << ", status: " << rc.ToString();
        manager_->ResetDataPlane(workerAddr);
    } else if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        LOG_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE)
            << "[TransportGet][Connection] Rebuild RPC connection, worker: " << workerAddr.ToString()
            << ", transport: " << AccessTransportTracker::KindToName(transporter->Kind())
            << ", status: " << rc.ToString();
        manager_->Teardown(workerAddr);
    } else {
        return rc;
    }
    connectionStatus = manager_->GetOrCreate(workerAddr, hint, transporter);
    if (connectionStatus.IsError()) {
        LOG(ERROR) << "[TransportGet][Connection] Rebuild data plane failed, worker: " << workerAddr.ToString()
                   << ", attempt: " << REBUILD_ATTEMPT << ", status: " << connectionStatus.ToString();
        return connectionStatus;
    }
    RETURN_RUNTIME_ERROR_IF_NULL(transporter);
    VLOG(1) << "[TransportGet][DataPlane] Send operation, worker: " << workerAddr.ToString()
            << ", transport: " << AccessTransportTracker::KindToName(transporter->Kind())
            << ", attempt: " << REBUILD_ATTEMPT;
    rc = operation(*transporter);
    LogDataPlaneOperation(workerAddr, transporter->Kind(), REBUILD_ATTEMPT, rc);
    return rc;
}
}  // namespace client
}  // namespace datasystem
