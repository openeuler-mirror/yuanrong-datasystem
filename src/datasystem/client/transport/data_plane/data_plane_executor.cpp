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
#include <optional>
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

bool DataPlaneExecutor::PrepareRetry(const HostPort &workerAddr, const std::shared_ptr<IDataTransporter> &transporter,
                                     const Status &rc, TransportHint hint, TransportHint &retryHint)
{
    retryHint = hint;
    auto logRebuild = [&](const char *what) {
        LOG_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE) << "[TransportGet][Connection] " << what << ", worker: "
                                                       << workerAddr.ToString() << ", transport: "
                                                       << AccessTransportTracker::KindToName(transporter->Kind())
                                                       << ", status: " << rc.ToString();
    };
    if (rc.GetCode() == K_URMA_NEED_CONNECT) {
        logRebuild("Rebuild data plane");
        manager_->ResetDataPlane(workerAddr);
    } else if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        logRebuild("Rebuild RPC connection");
        manager_->Teardown(workerAddr);
    } else if (rc.GetCode() == K_NOT_SUPPORTED && hint == TransportHint::SHM_CANDIDATE) {
        // SHM fd-passing endpoint unavailable (target Worker has SHM disabled). GetOrCreate re-arms
        // the entry with a TcpTransporter because the cached SHM transporter does not match the TCP
        // kind, so a same-host Get still succeeds instead of surfacing K_NOT_SUPPORTED.
        logRebuild("SHM fd-passing endpoint unavailable, fall back to TCP");
        retryHint = TransportHint::TCP_ONLY;
    } else {
        return false;
    }
    return true;
}

DataPlaneExecutor::AttemptResult DataPlaneExecutor::ExecuteAttempt(const HostPort &workerAddr,
                                                                   const Operation &operation, const AttemptPlan &plan,
                                                                   TransportPhaseLatencyRecorder *recorder)
{
    std::shared_ptr<IDataTransporter> transporter;
    const auto connectionBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                     : recorder->StartPhase();
    Status status = manager_->GetOrCreate(workerAddr, plan.hint, transporter, recorder);
    if (recorder != nullptr) {
        recorder->RecordPhase(plan.connectionPhase, connectionBegin, TransportLatencyThreshold::PROCESS);
    }
    if (status.IsError()) {
        const char *message = plan.attempt == INITIAL_ATTEMPT ? "Get data plane failed" : "Rebuild data plane failed";
        LOG(ERROR) << "[TransportGet][Connection] " << message << ", worker: " << workerAddr.ToString()
                   << ", attempt: " << plan.attempt << ", status: " << status.ToString();
        return { std::move(status), nullptr };
    }
    if (transporter == nullptr) {
        status = Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "The pointer [transporter] is null.");
        return { std::move(status), nullptr };
    }
    VLOG(1) << "[TransportGet][DataPlane] Send operation, worker: " << workerAddr.ToString()
            << ", transport: " << AccessTransportTracker::KindToName(transporter->Kind())
            << ", attempt: " << plan.attempt;
    const auto transferBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                   : recorder->StartPhase();
    status = operation(*transporter);
    if (recorder != nullptr) {
        recorder->RecordPhase(plan.transferPhase, transferBegin, TransportLatencyThreshold::RPC);
    }
    LogDataPlaneOperation(workerAddr, transporter->Kind(), plan.attempt, status);
    return { std::move(status), std::move(transporter) };
}

Status DataPlaneExecutor::Execute(const HostPort &workerAddr, const Operation &operation, bool traceEnabled)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(operation), K_INVALID, "Data-plane operation is empty");
    std::optional<TransportPhaseLatencyRecorder> recorder;
    TransportPhaseLatencyRecorder *phaseRecorder = nullptr;
    if (traceEnabled) {
        phaseRecorder = &recorder.emplace(workerAddr);
    }
    const TransportHint hint = advisor_->GetTransportHint(workerAddr);
    const AttemptPlan initialAttempt{ hint, INITIAL_ATTEMPT, "connection_acquire", "data_transfer" };
    AttemptResult result = ExecuteAttempt(workerAddr, operation, initialAttempt, phaseRecorder);
    if (result.transporter == nullptr) {
        return result.status;
    }
    TransportHint retryHint = hint;
    const auto prepareBegin = phaseRecorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                       : phaseRecorder->StartPhase();
    const bool shouldRetry = PrepareRetry(workerAddr, result.transporter, result.status, hint, retryHint);
    if (!shouldRetry) {
        return result.status;
    }
    if (phaseRecorder != nullptr) {
        phaseRecorder->RecordPhase("retry_prepare", prepareBegin, TransportLatencyThreshold::PROCESS);
    }
    const AttemptPlan retryAttempt{ retryHint, REBUILD_ATTEMPT, "connection_rebuild", "retry_data_transfer" };
    AttemptResult retryResult = ExecuteAttempt(workerAddr, operation, retryAttempt, phaseRecorder);
    return retryResult.status;
}
}  // namespace client
}  // namespace datasystem
