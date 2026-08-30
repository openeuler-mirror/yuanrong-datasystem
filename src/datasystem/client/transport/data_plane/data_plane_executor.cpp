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
#include <iterator>
#include <optional>
#include <utility>

#include "datasystem/client/transport/object_read/object_read_types.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {
constexpr size_t INITIAL_ATTEMPT = 1;
constexpr size_t REBUILD_ATTEMPT = 2;
// Rebuild/teardown events are recoverable and can recur on every request during sustained instability;
// sample them like the other transport degradation WARNINGs. Terminal failures stay unsampled.
constexpr int TRANSPORT_DIAG_LOG_RATE = 100;

bool IsShmFallbackError(const Status &status)
{
    return status.GetCode() == K_NOT_SUPPORTED || IsWorkerDrainingForScaleIn(status);
}

bool IsUbFallbackError(const Status &status)
{
    switch (status.GetCode()) {
        case K_NOT_SUPPORTED:
        case K_URMA_ERROR:
        case K_URMA_NEED_CONNECT:
        case K_URMA_CONNECT_FAILED:
        case K_URMA_WAIT_TIMEOUT:
        case K_URMA_TRY_AGAIN:
        case K_URMA_DATA_WORKER_UNAVAILABLE:
            return true;
        default:
            return false;
    }
}

const char *TransportHintName(TransportHint hint)
{
    switch (hint) {
        case TransportHint::SHM_CANDIDATE:
            return "SHM";
        case TransportHint::UB_CANDIDATE:
            return "UB";
        case TransportHint::TCP_ONLY:
            return "TCP";
        default:
            return "UNKNOWN";
    }
}

void LogDataPlaneOperation(const HostPort &workerAddr, TransportHint hint, size_t attempt, const Status &status,
                           bool terminalFailure)
{
    if (status.IsError() && terminalFailure) {
        LOG(ERROR) << "[TransportGet][DataPlane] Operation failed, worker: " << workerAddr.ToString()
                   << ", transport: " << TransportHintName(hint) << ", attempt: " << attempt
                   << ", status: " << status.ToString();
    } else {
        VLOG(1) << "[TransportGet][DataPlane] Operation attempt completed, worker: " << workerAddr.ToString()
                << ", transport: " << TransportHintName(hint) << ", attempt: " << attempt
                << ", status: " << status.ToString();
    }
}
}  // namespace

DataPlaneExecutor::DataPlaneExecutor(std::shared_ptr<DataPlaneManager> manager,
                                     std::shared_ptr<TransportAdvisor> advisor,
                                     DrainingFallbackHandler drainingFallbackHandler)
    : manager_(std::move(manager)),
      advisor_(std::move(advisor)),
      drainingFallbackHandler_(std::move(drainingFallbackHandler))
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
    } else if (rc.GetCode() == K_RPC_UNAVAILABLE || rc.GetCode() == K_RPC_NETWORK_BLIP) {
        // K_RPC_NETWORK_BLIP (ECONNRESET/ECONNABORTED/EHOSTUNREACH/ENETUNREACH) is a transient
        // transport hiccup, not a dead peer: rebuild the connection and retry once, same as the
        // legacy UNAVAILABLE path. A genuinely dead peer (K_RPC_PEER_DEAD) is handled below.
        logRebuild("Rebuild RPC connection");
        manager_->Teardown(workerAddr);
    } else if (IsNonRetryableRpcError(rc)) {
        // Dead peer (K_RPC_PEER_DEAD): the cached transporter's underlying brpc socket is failed
        // and (with health check off) will not auto-reconnect, so close/teardown it now to free the
        // stale entry. Return false so the caller fast-fails without retrying the same dead target —
        // tearing down here is resource cleanup, not a retry trigger.
        logRebuild("Tear down dead RPC peer");
        manager_->Teardown(workerAddr);
        return false;
    } else {
        return false;
    }
    return true;
}

DataPlaneExecutor::AttemptResult DataPlaneExecutor::ExecuteAttempt(const HostPort &workerAddr,
                                                                   const Operation &operation, const AttemptPlan &plan,
                                                                   uint64_t locationTopologyVersion,
                                                                   TransportPhaseLatencyRecorder *recorder)
{
    std::shared_ptr<IDataTransporter> transporter;
    const auto connectionBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                     : recorder->StartPhase();
    Status status = locationTopologyVersion == 0
                        ? manager_->GetOrCreate(workerAddr, plan.hint, transporter, recorder)
                        : manager_->GetOrCreateForDataLocation(workerAddr, plan.hint, locationTopologyVersion,
                                                               transporter, recorder);
    if (recorder != nullptr) {
        recorder->RecordPhase(plan.connectionPhase, connectionBegin, TransportLatencyThreshold::PROCESS);
    }
    if (status.IsError()) {
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
    return { std::move(status), std::move(transporter) };
}

Status DataPlaneExecutor::ExecuteFallbacks(const HostPort &workerAddr, const Operation &operation,
                                           const std::vector<TransportHint> &fallbackHints,
                                           uint64_t locationTopologyVersion,
                                           TransportPhaseLatencyRecorder *recorder)
{
    Status status(K_NOT_SUPPORTED, "No transport fallback is available");
    size_t attempt = REBUILD_ATTEMPT;
    for (auto iter = fallbackHints.begin(); iter != fallbackHints.end(); ++iter) {
        const auto hint = *iter;
        LOG_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE)
            << "[TransportGet][DataPlane] Fall back transport, worker: " << workerAddr.ToString()
            << ", transport: " << TransportHintName(hint);
        const bool ubFallback = hint == TransportHint::UB_CANDIDATE;
        const AttemptPlan plan{ hint, attempt,
                                ubFallback ? "ub_fallback_connection" : "tcp_fallback_connection",
                                ubFallback ? "ub_fallback_transfer" : "tcp_fallback_transfer" };
        AttemptResult result = ExecuteAttempt(workerAddr, operation, plan, locationTopologyVersion, recorder);
        status = std::move(result.status);
        if (status.IsOk()) {
            LogDataPlaneOperation(workerAddr, hint, attempt, status, false);
            return status;
        }
        const bool hasNext = ubFallback && IsUbFallbackError(status) && std::next(iter) != fallbackHints.end();
        LogDataPlaneOperation(workerAddr, hint, attempt, status, !hasNext);
        if (!hasNext) {
            return status;
        }
        ++attempt;
    }
    return status;
}

Status DataPlaneExecutor::Execute(const HostPort &workerAddr, const Operation &operation, bool traceEnabled)
{
    return ExecuteImpl(workerAddr, 0, operation, traceEnabled);
}

Status DataPlaneExecutor::ExecuteForDataLocation(const HostPort &workerAddr, uint64_t locationTopologyVersion,
                                                 const Operation &operation, bool traceEnabled)
{
    return ExecuteImpl(workerAddr, locationTopologyVersion, operation, traceEnabled);
}

Status DataPlaneExecutor::ExecuteImpl(const HostPort &workerAddr, uint64_t locationTopologyVersion,
                                      const Operation &operation, bool traceEnabled)
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
    AttemptResult result = ExecuteAttempt(workerAddr, operation, initialAttempt, locationTopologyVersion,
                                          phaseRecorder);
    if (hint == TransportHint::SHM_CANDIDATE && IsShmFallbackError(result.status)) {
        if (IsWorkerDrainingForScaleIn(result.status)) {
            manager_->MarkShmDraining(workerAddr);
            const bool shouldRefresh = advisor_->ObserveDrainingShmFailure(workerAddr);
            if (shouldRefresh && drainingFallbackHandler_ != nullptr) {
                drainingFallbackHandler_(workerAddr, result.status);
            }
        }
        LogDataPlaneOperation(workerAddr, hint, INITIAL_ATTEMPT, result.status, false);
        return ExecuteFallbacks(workerAddr, operation, advisor_->GetFallbackHints(hint), locationTopologyVersion,
                                phaseRecorder);
    }
    if (result.transporter == nullptr) {
        LogDataPlaneOperation(workerAddr, hint, INITIAL_ATTEMPT, result.status, true);
        return result.status;
    }
    TransportHint retryHint = hint;
    const auto prepareBegin = phaseRecorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                       : phaseRecorder->StartPhase();
    const bool shouldRetry = PrepareRetry(workerAddr, result.transporter, result.status, hint, retryHint);
    LogDataPlaneOperation(workerAddr, hint, INITIAL_ATTEMPT, result.status, !shouldRetry);
    if (!shouldRetry) {
        return result.status;
    }
    if (phaseRecorder != nullptr) {
        phaseRecorder->RecordPhase("retry_prepare", prepareBegin, TransportLatencyThreshold::PROCESS);
    }
    const AttemptPlan retryAttempt{ retryHint, REBUILD_ATTEMPT, "connection_rebuild", "retry_data_transfer" };
    AttemptResult retryResult = ExecuteAttempt(workerAddr, operation, retryAttempt, locationTopologyVersion,
                                               phaseRecorder);
    LogDataPlaneOperation(workerAddr, retryHint, REBUILD_ATTEMPT, retryResult.status, true);
    return retryResult.status;
}
}  // namespace client
}  // namespace datasystem
