/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Worker service admission.
 */
#include "datasystem/worker/runtime/worker_service_admission.h"

#include <chrono>
#include <sstream>

#include "datasystem/common/metrics/kv_metrics.h"

namespace datasystem::worker {
namespace {
bool IsAlwaysAllowed(WorkerAdmissionKind kind)
{
    return kind == WorkerAdmissionKind::RECOVERY_RPC || kind == WorkerAdmissionKind::CLEANUP_RPC
           || kind == WorkerAdmissionKind::DIAGNOSTIC_RPC;
}

bool IsTransitionAllowed(WorkerAdmissionKind kind)
{
    return IsAlwaysAllowed(kind) || kind == WorkerAdmissionKind::INTERNAL_JOINING_RPC;
}

bool IsOomAllowed(WorkerAdmissionKind kind)
{
    return kind == WorkerAdmissionKind::NORMAL_READ || kind == WorkerAdmissionKind::INTERNAL_JOINING_RPC
           || kind == WorkerAdmissionKind::RESOURCE_RECOVERY_RPC || IsAlwaysAllowed(kind);
}

metrics::KvMetricId RejectMetric(WorkerServiceMode mode)
{
    switch (mode) {
        case WorkerServiceMode::STARTING:
            return metrics::KvMetricId::WORKER_ADMISSION_REJECT_STARTING_TOTAL;
        case WorkerServiceMode::JOINING:
            return metrics::KvMetricId::WORKER_ADMISSION_REJECT_JOINING_TOTAL;
        case WorkerServiceMode::RUNNING:
            return metrics::KvMetricId::WORKER_ADMISSION_REJECT_RUNNING_TOTAL;
        case WorkerServiceMode::DRAINING:
            return metrics::KvMetricId::WORKER_ADMISSION_REJECT_DRAINING_TOTAL;
        case WorkerServiceMode::LOCAL_ISOLATED:
            return metrics::KvMetricId::WORKER_ADMISSION_REJECT_LOCAL_ISOLATED_TOTAL;
        case WorkerServiceMode::OUT_OF_MEMORY:
            return metrics::KvMetricId::WORKER_ADMISSION_REJECT_OUT_OF_MEMORY_TOTAL;
        case WorkerServiceMode::RECOVERING:
            return metrics::KvMetricId::WORKER_ADMISSION_REJECT_RECOVERING_TOTAL;
        case WorkerServiceMode::STOPPING:
            return metrics::KvMetricId::WORKER_ADMISSION_REJECT_STOPPING_TOTAL;
        default:
            return metrics::KvMetricId::WORKER_ADMISSION_REJECT_STARTING_TOTAL;
    }
}

Status Reject(const WorkerRuntimeStateSnapshot &snapshot, WorkerAdmissionKind kind, const std::string &operation,
              StatusCode code, const char *rejection)
{
    METRIC_INC(RejectMetric(snapshot.mode));
    std::ostringstream oss;
    oss << "Worker is not accepting " << operation << ", kind=" << ToString(kind)
        << ", mode=" << ToString(snapshot.mode) << ", reason=" << ToString(snapshot.reason)
        << ", phase=" << ToString(snapshot.recoveryPhase) << ", rejection=" << rejection;
    return Status(code, oss.str());
}

Status Evaluate(const WorkerRuntimeStateSnapshot &snapshot, WorkerAdmissionKind kind, const std::string &operation)
{
    switch (snapshot.mode) {
        case WorkerServiceMode::RUNNING:
            return Status::OK();
        case WorkerServiceMode::JOINING:
            if (kind == WorkerAdmissionKind::INTERNAL_JOINING_RPC || IsAlwaysAllowed(kind)) {
                return Status::OK();
            }
            break;
        case WorkerServiceMode::OUT_OF_MEMORY:
            if (IsOomAllowed(kind)) {
                return Status::OK();
            }
            return Reject(snapshot, kind, operation, K_OUT_OF_MEMORY, "MODE_NOT_SERVING");
        case WorkerServiceMode::DRAINING:
            if (kind == WorkerAdmissionKind::NORMAL_READ || kind == WorkerAdmissionKind::RESOURCE_RECOVERY_RPC
                || IsAlwaysAllowed(kind)) {
                return Status::OK();
            }
            break;
        case WorkerServiceMode::RECOVERING:
            if (IsAlwaysAllowed(kind) || kind == WorkerAdmissionKind::INTERNAL_JOINING_RPC
                || kind == WorkerAdmissionKind::RESOURCE_RECOVERY_RPC) {
                return Status::OK();
            }
            break;
        case WorkerServiceMode::STOPPING:
            if (kind == WorkerAdmissionKind::CLEANUP_RPC || kind == WorkerAdmissionKind::DIAGNOSTIC_RPC) {
                return Status::OK();
            }
            break;
        default:
            if (IsAlwaysAllowed(kind)) {
                return Status::OK();
            }
            break;
    }
    return Reject(snapshot, kind, operation, K_NOT_READY, "MODE_NOT_SERVING");
}

void RecordRejectLatency(const std::chrono::steady_clock::time_point &start, const Status &status)
{
    if (status.IsOk()) {
        return;
    }
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_ADMISSION_REJECT_LATENCY))
        .Observe(static_cast<uint64_t>(elapsed.count()));
}
}  // namespace

const char *ToString(WorkerAdmissionKind kind)
{
    switch (kind) {
        case WorkerAdmissionKind::NORMAL_READ:
            return "NORMAL_READ";
        case WorkerAdmissionKind::NORMAL_WRITE:
            return "NORMAL_WRITE";
        case WorkerAdmissionKind::MIGRATION_TARGET:
            return "MIGRATION_TARGET";
        case WorkerAdmissionKind::RECOVERY_RPC:
            return "RECOVERY_RPC";
        case WorkerAdmissionKind::CLEANUP_RPC:
            return "CLEANUP_RPC";
        case WorkerAdmissionKind::RESOURCE_RECOVERY_RPC:
            return "RESOURCE_RECOVERY_RPC";
        case WorkerAdmissionKind::INTERNAL_JOINING_RPC:
            return "INTERNAL_JOINING_RPC";
        case WorkerAdmissionKind::DIAGNOSTIC_RPC:
            return "DIAGNOSTIC_RPC";
        default:
            return "UNKNOWN";
    }
}

WorkerServiceAdmission::WorkerServiceAdmission(const WorkerRuntimeStateManager &runtimeState)
    : runtimeState_(runtimeState)
{
}

Status WorkerServiceAdmission::Check(WorkerAdmissionKind kind, const std::string &operation) const
{
    if (runtimeState_.IsFastRunningForAdmission()) {
        return Status::OK();
    }
    const auto start = std::chrono::steady_clock::now();
    const bool transitionWasPending = runtimeState_.IsTransitionPending();
    auto snapshot = runtimeState_.GetSnapshot();
    if ((transitionWasPending || runtimeState_.IsTransitionPending()) && !IsTransitionAllowed(kind)) {
        auto status = Reject(snapshot, kind, operation, K_NOT_READY, "TRANSITION_PENDING");
        RecordRejectLatency(start, status);
        return status;
    }
    auto status = Evaluate(snapshot, kind, operation);
    RecordRejectLatency(start, status);
    return status;
}

Status WorkerServiceAdmission::Check(const WorkerRuntimeStateSnapshot &snapshot, WorkerAdmissionKind kind,
                                     const std::string &operation) const
{
    const auto start = std::chrono::steady_clock::now();
    auto status = Evaluate(snapshot, kind, operation);
    RecordRejectLatency(start, status);
    return status;
}

Status WorkerServiceAdmission::CheckServing(const std::string &operation) const
{
    return Check(WorkerAdmissionKind::NORMAL_WRITE, operation);
}

bool WorkerServiceAdmission::IsServing() const
{
    return IsServingMode(runtimeState_.GetSnapshot().mode);
}
}  // namespace datasystem::worker
