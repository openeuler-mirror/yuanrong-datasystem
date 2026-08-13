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

/** Description: Implements fixed-location replica polling for object reads. */

#include "datasystem/client/transport/object_read/replica_reader.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <limits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {
constexpr size_t MAX_BATCH_OBJECT_COUNT = 1024;
constexpr uint64_t MAX_BATCH_EXPECTED_BYTES = 100ULL * 1024ULL * 1024ULL;
constexpr int TRANSPORT_DIAG_LOG_RATE = 100;

struct RefreshableLocationState {
    bool hasStaleLocation = false;
    Status staleLocationStatus;
    bool hasDrainingLocation = false;
    Status drainingLocationStatus;
};

struct ReadState {
    const master::ObjectLocationInfoPb *location = nullptr;
    ObjectReadItemResult *result = nullptr;
    std::shared_ptr<const TransportReadContext> context;
    size_t inputIndex = 0;
    size_t replicaIndex = 0;
    size_t round = 1;
    uint64_t expectedSize = 0;
    Status lastStatus = Status(K_NOT_FOUND, "Cannot get objects from worker");
    bool hasAttempt = false;
    bool completed = false;
    bool exhausted = false;
    RefreshableLocationState refreshableLocation;
};

void RecordRefreshableLocation(const Status &status, RefreshableLocationState &state)
{
    if (IsWorkerDrainingForScaleIn(status)) {
        state.hasDrainingLocation = true;
        state.drainingLocationStatus = status;
        return;
    }
    if (IsTransportSnapshotStaleLocation(status)) {
        state.hasStaleLocation = true;
        state.staleLocationStatus = status;
    }
}

bool HasRefreshableLocation(const RefreshableLocationState &state)
{
    return state.hasDrainingLocation || state.hasStaleLocation;
}

const Status &GetRefreshableLocationStatus(const RefreshableLocationState &state)
{
    return state.hasDrainingLocation ? state.drainingLocationStatus : state.staleLocationStatus;
}

struct ReadChunk {
    std::vector<size_t> stateIndexes;
    DataGetBatchRequest requests;
    uint64_t expectedBytes = 0;
    Status endpointStatus = Status(K_NOT_READY, "Endpoint read was not started");
    DataGetBatchResult results;
    bool attempted = false;
};

struct EndpointWork {
    explicit EndpointWork(HostPort workerAddress) : address(std::move(workerAddress))
    {
    }

    HostPort address;
    std::vector<ReadChunk> chunks;
};

Status BuildAggregateStatus(const std::vector<ReadState> &states)
{
    for (const auto &state : states) {
        if (state.result != nullptr && state.result->status.IsOk()) {
            return Status::OK();
        }
    }
    for (const auto &state : states) {
        if (state.result != nullptr && state.result->status.IsError()) {
            return state.result->status;
        }
        if (state.lastStatus.IsError()) {
            return state.lastStatus;
        }
    }
    return Status(K_NOT_FOUND, "Cannot get objects from worker");
}

void FinishUnresolvedWithDeadline(std::vector<ReadState> &states, const Status &deadlineStatus)
{
    for (auto &state : states) {
        if (state.completed) {
            continue;
        }
        const Status &status = state.hasAttempt && state.lastStatus.IsError() ? state.lastStatus : deadlineStatus;
        if (state.result != nullptr) {
            state.result->status = status;
        }
        state.completed = true;
    }
}

bool AllCompleted(const std::vector<ReadState> &states)
{
    for (const auto &state : states) {
        if (!state.completed) {
            return false;
        }
    }
    return true;
}

bool AllUnresolvedExhausted(const std::vector<ReadState> &states)
{
    bool hasUnresolved = false;
    for (const auto &state : states) {
        if (state.completed) {
            continue;
        }
        hasUnresolved = true;
        if (!state.exhausted) {
            return false;
        }
    }
    return hasUnresolved;
}

void AdvanceUnavailableReplica(ReadState &state, const Status &itemStatus)
{
    ++state.replicaIndex;
    const bool replicasExhausted = state.replicaIndex >= static_cast<size_t>(state.location->object_locations_size());
    state.completed = replicasExhausted;
    if (replicasExhausted) {
        state.result->status = HasRefreshableLocation(state.refreshableLocation)
                                   ? GetRefreshableLocationStatus(state.refreshableLocation)
                                   : itemStatus;
        return;
    }
    METRIC_INC(metrics::KvMetricId::CLIENT_DIRECT_BATCH_GET_REPLICA_RETRY_TOTAL);
}

void AdvanceRetryableReplica(ReadState &state, const Status &itemStatus)
{
    ++state.replicaIndex;
    const bool replicasExhausted = state.replicaIndex >= static_cast<size_t>(state.location->object_locations_size());
    if (!replicasExhausted) {
        METRIC_INC(metrics::KvMetricId::CLIENT_DIRECT_BATCH_GET_REPLICA_RETRY_TOTAL);
        return;
    }
    if (HasRefreshableLocation(state.refreshableLocation)) {
        state.result->status = GetRefreshableLocationStatus(state.refreshableLocation);
        state.completed = true;
        return;
    }
    if (itemStatus.GetCode() == K_WORKER_PULL_OBJECT_NOT_FOUND) {
        state.result->status = Status(K_WORKER_PULL_OBJECT_NOT_FOUND, "Object not found on any replica");
        state.completed = true;
        return;
    }
    state.exhausted = true;
}

// A replica whose UB read source is unavailable: either the worker reported it
// (K_URMA_DATA_WORKER_UNAVAILABLE) or the client's UB health cache denied it pre-read
// (K_URMA_READ_SOURCE_DENIED). Both fast-skip to the next replica.
bool IsReadSourceUnavailable(const Status &status)
{
    return status.GetCode() == K_URMA_DATA_WORKER_UNAVAILABLE
           || status.GetCode() == K_URMA_READ_SOURCE_DENIED;
}

bool IsLastUnavailableReplica(const Status &status, int replicaIndex, int replicaCount)
{
    return IsReadSourceUnavailable(status) && replicaIndex + 1 >= replicaCount;
}

void LogReplicaReadFailure(const master::ObjectLocationInfoPb &location, const HostPort &workerAddr, size_t round,
                           const Status &status, bool retryable)
{
    if (!retryable) {
        LOG(ERROR) << "[TransportGet][Data] Replica read failed without retry, key: " << location.object_key()
                   << ", worker: " << workerAddr.ToString() << ", round: " << round
                   << ", status: " << status.ToString();
        return;
    }
    LOG_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE)
        << "[TransportGet][Data] Replica read failed, try another replica, key: " << location.object_key()
        << ", worker: " << workerAddr.ToString() << ", round: " << round << ", status: " << status.ToString();
}
}  // namespace

ReplicaReader::ReplicaReader(std::shared_ptr<DataPlaneExecutor> executor, std::shared_ptr<DeadlineRetry> retry,
                             std::shared_ptr<ThreadPool> taskPool, ReadAdmissionCheck readAdmissionCheck,
                             ReadOutcomeReport readOutcomeReport)
    : executor_(std::move(executor)),
      retry_(std::move(retry)),
      taskPool_(std::move(taskPool)),
      readAdmissionCheck_(std::move(readAdmissionCheck)),
      readOutcomeReport_(std::move(readOutcomeReport))
{
}

bool ReplicaReader::IsRetryableLocationError(const Status &status) const
{
    if (IsTransportSnapshotStaleLocation(status) || IsWorkerDrainingForScaleIn(status)) {
        return true;
    }
    if (retry_->IsRetryableRpcError(status)) {
        return true;
    }
    switch (status.GetCode()) {
        case K_URMA_NEED_CONNECT:
        case K_URMA_CONNECT_FAILED:
        case K_URMA_DATA_WORKER_UNAVAILABLE:
        case K_URMA_READ_SOURCE_DENIED:
        case K_WORKER_PULL_OBJECT_NOT_FOUND:
        case K_NOT_FOUND:
        case K_OUT_OF_MEMORY:
            return true;
        default:
            return false;
    }
}

Status ReplicaReader::CheckDeadline() const
{
    return retry_->CheckDeadline();
}

Status ReplicaReader::Backoff(int64_t &backoffMs) const
{
    return retry_->Backoff(backoffMs);
}

Status ReplicaReader::ReadReplicaOnce(const ReplicaReadRequest &request, int replicaIndex, size_t round,
                                      const HostPort &workerAddr, bool traceEnabled)
{
    const auto &location = *request.location;
    auto &result = *request.result;
    VLOG(1) << "[TransportGet][Data] Read replica, key: " << location.object_key()
            << ", worker: " << workerAddr.ToString() << ", replica index: " << replicaIndex
            << ", replica count: " << location.object_locations_size() << ", expected size: " << location.object_size()
            << ", round: " << round << ", remaining deadline us: " << ApiDeadline::Instance().ApiRemainingUs();
    Status rc = readAdmissionCheck_ ? readAdmissionCheck_(workerAddr) : Status::OK();
    DataGetResult data;
    if (rc.IsOk()) {
        DataGetRequest dataRequest{ location.object_key(), location.object_size(), request.context };
        rc = executor_->Execute(workerAddr, [&dataRequest, &data](IDataTransporter &transporter) {
            return transporter.Get(dataRequest, data);
        }, traceEnabled);
        if (readOutcomeReport_) {
            readOutcomeReport_(workerAddr, data.response);
        }
    }
    if (rc.IsError()) {
        return rc;
    }
    result.objectKey = location.object_key();
    result.data = std::move(data);
    VLOG(1) << "[TransportGet][Data] Read succeeded, key: " << location.object_key()
            << ", worker: " << workerAddr.ToString()
            << ", transport: " << AccessTransportTracker::KindToName(result.data.kind)
            << ", expected size: " << location.object_size()
            << ", actual size: " << result.data.response.data_size()
            << ", payload count: " << result.data.rpcPayloads.size()
            << ", external size: " << result.data.externalSize << ", round: " << round;
    return Status::OK();
}

Status ReplicaReader::Read(const master::ObjectLocationInfoPb &location, ObjectReadItemResult &result,
                           std::shared_ptr<const TransportReadContext> context, bool traceEnabled)
{
    RETURN_RUNTIME_ERROR_IF_NULL(executor_);
    RETURN_RUNTIME_ERROR_IF_NULL(retry_);
    CHECK_FAIL_RETURN_STATUS(context != nullptr, K_INVALID, "Transport read context is missing");
    CHECK_FAIL_RETURN_STATUS(location.object_locations_size() > 0, K_NOT_FOUND, "Object was not found");
    int64_t backoffMs = 1;
    size_t round = 0;
    Status lastError(K_NOT_FOUND, "Cannot get objects from worker");
    const ReplicaReadRequest request{ &location, &result, std::move(context) };
    while (true) {
        ++round;
        RefreshableLocationState refreshableLocation;
        int notFoundReplicas = 0;
        for (int replicaIndex = 0; replicaIndex < location.object_locations_size(); ++replicaIndex) {
            Status deadlineStatus = retry_->CheckDeadline();
            if (deadlineStatus.IsError()) {
                LOG(ERROR) << "[TransportGet][Data] Replica read deadline exceeded, key: "
                           << location.object_key() << ", round: " << round
                           << ", status: " << deadlineStatus.ToString();
                return deadlineStatus;
            }
            HostPort workerAddr;
            RETURN_IF_NOT_OK(workerAddr.ParseString(location.object_locations(replicaIndex)));
            Status rc = ReadReplicaOnce(request, replicaIndex, round, workerAddr, traceEnabled);
            if (rc.IsOk()) {
                return Status::OK();
            }
            const bool retryable = IsRetryableLocationError(rc);
            LogReplicaReadFailure(location, workerAddr, round, rc, retryable);
            if (!retryable) {
                return rc;
            }
            lastError = rc;
            if (rc.GetCode() == K_WORKER_PULL_OBJECT_NOT_FOUND) {
                ++notFoundReplicas;
            }
            if (rc.GetCode() == K_RPC_UNAVAILABLE) {
                RecordRefreshableLocation(Status(K_NOT_READY, STALE_TRANSPORT_SNAPSHOT_MESSAGE), refreshableLocation);
            } else {
                RecordRefreshableLocation(rc, refreshableLocation);
            }
            if (IsLastUnavailableReplica(rc, replicaIndex, location.object_locations_size())) {
                return HasRefreshableLocation(refreshableLocation)
                           ? GetRefreshableLocationStatus(refreshableLocation)
                           : rc;
            }
            if (IsReadSourceUnavailable(rc)) {
                continue;
            }
        }
        if (notFoundReplicas == location.object_locations_size()) {
            LOG(WARNING) << "[TransportGet][Data] All replicas returned not-found in one round, fast-fail as deleted. "
                         << "key: " << location.object_key() << ", round: " << round;
            return Status(K_WORKER_PULL_OBJECT_NOT_FOUND, "Object not found on any replica");
        }
        if (HasRefreshableLocation(refreshableLocation)) {
            return GetRefreshableLocationStatus(refreshableLocation);
        }
        Status backoffRc = retry_->Backoff(backoffMs);
        if (backoffRc.IsError()) {
            backoffRc.AppendMsg(lastError.GetMsg());
            LOG(ERROR) << "[TransportGet][Data] Stop retrying replica read, key: " << location.object_key()
                       << ", round: " << round << ", status: " << backoffRc.ToString();
            return backoffRc;
        }
    }
}

Status ReplicaReader::ReadBatch(const ReplicaReadBatch &requests, bool traceEnabled)
{
    RETURN_RUNTIME_ERROR_IF_NULL(executor_);
    RETURN_RUNTIME_ERROR_IF_NULL(retry_);
    RETURN_RUNTIME_ERROR_IF_NULL(taskPool_);
    CHECK_FAIL_RETURN_STATUS(!requests.empty(), K_INVALID, "Replica read requests are empty");

    std::vector<ReadState> states;
    states.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto &request = requests[i];
        ReadState state;
        state.location = request.location;
        state.result = request.result;
        state.context = request.context;
        state.inputIndex = i;
        if (request.result == nullptr || request.location == nullptr) {
            state.lastStatus = Status(K_INVALID, "Replica read location or result is null");
            state.completed = true;
            if (request.result != nullptr) {
                request.result->status = state.lastStatus;
            }
        } else if (request.location->object_locations_size() == 0) {
            state.lastStatus = Status(K_NOT_FOUND, "Object was not found");
            state.completed = true;
            request.result->status = state.lastStatus;
        } else {
            state.expectedSize = request.location->object_size();
            request.result->objectKey = request.location->object_key();
            request.result->status = Status(K_NOT_READY, "Object data is not read");
        }
        states.emplace_back(std::move(state));
    }

    int64_t backoffMs = 1;
    while (!AllCompleted(states)) {
        Status deadlineStatus = CheckDeadline();
        if (deadlineStatus.IsError()) {
            FinishUnresolvedWithDeadline(states, deadlineStatus);
            break;
        }

        std::vector<EndpointWork> endpointWorks;
        endpointWorks.reserve(states.size());
        std::unordered_map<HostPort, size_t> endpointIndexes;
        endpointIndexes.reserve(states.size());
        for (auto &state : states) {
            if (state.completed || state.exhausted) {
                continue;
            }
            HostPort address;
            Status parseStatus = address.ParseString(state.location->object_locations(state.replicaIndex));
            if (parseStatus.IsError()) {
                state.lastStatus = parseStatus;
                state.result->status = parseStatus;
                state.completed = true;
                continue;
            }
            auto inserted = endpointIndexes.emplace(address, endpointWorks.size());
            if (inserted.second) {
                endpointWorks.emplace_back(address);
            }
            auto &chunks = endpointWorks[inserted.first->second].chunks;
            const bool exceedsByteCap = !chunks.empty() && !chunks.back().requests.empty()
                                        && (chunks.back().expectedBytes >= MAX_BATCH_EXPECTED_BYTES
                                            || state.expectedSize
                                                   > MAX_BATCH_EXPECTED_BYTES - chunks.back().expectedBytes);
            const bool needsNewChunk = chunks.empty() || chunks.back().stateIndexes.size() >= MAX_BATCH_OBJECT_COUNT
                                       || exceedsByteCap;
            if (needsNewChunk) {
                chunks.emplace_back();
            }
            chunks.back().stateIndexes.emplace_back(state.inputIndex);
            chunks.back().requests.push_back({ state.location->object_key(), state.expectedSize, state.context });
            if (state.expectedSize <= std::numeric_limits<uint64_t>::max() - chunks.back().expectedBytes) {
                chunks.back().expectedBytes += state.expectedSize;
            } else {
                chunks.back().expectedBytes = std::numeric_limits<uint64_t>::max();
            }
        }

        std::vector<std::future<void>> futures;
        futures.reserve(endpointWorks.size());
        const int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
        const auto dispatchTime = std::chrono::steady_clock::now();
        const auto traceContext = Trace::Instance().GetContext();
        for (auto &work : endpointWorks) {
            const Status admissionStatus = readAdmissionCheck_ ? readAdmissionCheck_(work.address) : Status::OK();
            auto *endpointWork = &work;
            futures.emplace_back(taskPool_->Submit([this, endpointWork, admissionStatus, remainingUs, dispatchTime,
                                                    traceContext, traceEnabled]() {
                TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
                Status dispatchStatus = InitTimeoutsFromDispatch(remainingUs, dispatchTime);
                bool outcomeReported = false;
                for (auto &chunk : endpointWork->chunks) {
                    if (admissionStatus.IsError()) {
                        chunk.attempted = true;
                        chunk.endpointStatus = admissionStatus;
                        continue;
                    }
                    if (dispatchStatus.IsError()) {
                        chunk.endpointStatus = dispatchStatus;
                        continue;
                    }
                    chunk.attempted = true;
                    if (chunk.requests.size() == 1) {
                        DataGetResult data;
                        Status unaryStatus = executor_->Execute(endpointWork->address,
                                                                [&chunk, &data](IDataTransporter &t) {
                            return t.Get(chunk.requests.front(), data);
                        }, traceEnabled);
                        chunk.results.resize(1);
                        chunk.results.front().status = unaryStatus;
                        // Preserve structured Provider failure detail even when the unary request failed.
                        // The batch-level outcome reporter consumes it before aggregate status handling.
                        chunk.results.front().data = std::move(data);
                        chunk.endpointStatus = unaryStatus.GetCode() == K_OC_REMOTE_GET_NOT_ENOUGH
                                                   ? Status::OK()
                                                   : std::move(unaryStatus);
                    } else {
                        chunk.endpointStatus = executor_->Execute(
                            endpointWork->address,
                            [&chunk](IDataTransporter &t) { return t.BatchGet(chunk.requests, chunk.results); },
                            traceEnabled);
                    }
                    if (!outcomeReported && readOutcomeReport_) {
                        for (const auto &result : chunk.results) {
                            if (result.data.response.has_provider_ub_failure_detail()) {
                                readOutcomeReport_(endpointWork->address, result.data.response);
                                outcomeReported = true;
                                break;
                            }
                        }
                    }
                }
            }));
        }
        for (auto &future : futures) {
            future.get();
        }

        bool dispatchExpired = false;
        for (auto &work : endpointWorks) {
            for (auto &chunk : work.chunks) {
                const bool validResults = chunk.endpointStatus.IsError()
                                          || chunk.results.size() == chunk.stateIndexes.size();
                for (size_t i = 0; i < chunk.stateIndexes.size(); ++i) {
                    auto &state = states[chunk.stateIndexes[i]];
                    if (!chunk.attempted) {
                        dispatchExpired = true;
                        continue;
                    }
                    state.hasAttempt = true;
                    Status itemStatus = chunk.endpointStatus;
                    DataGetResult *data = nullptr;
                    if (chunk.endpointStatus.IsOk()) {
                        if (!validResults) {
                            itemStatus = Status(K_RUNTIME_ERROR, "Batch Get response count does not match request");
                        } else {
                            itemStatus = chunk.results[i].status;
                            data = &chunk.results[i].data;
                        }
                    }
                    if (itemStatus.IsOk()) {
                        state.result->status = Status::OK();
                        state.result->data = std::move(*data);
                        state.completed = true;
                        continue;
                    }

                    state.lastStatus = itemStatus;
                    if (IsTransportSnapshotStaleLocation(itemStatus)
                        || IsWorkerDrainingForScaleIn(itemStatus)) {
                        RecordRefreshableLocation(itemStatus, state.refreshableLocation);
                        AdvanceRetryableReplica(state, itemStatus);
                        continue;
                    }
                    if (IsReadSourceUnavailable(itemStatus)) {
                        AdvanceUnavailableReplica(state, itemStatus);
                        continue;
                    }
                    if (itemStatus.GetCode() == K_OC_REMOTE_GET_NOT_ENOUGH && data != nullptr) {
                        const int64_t actualSize = data->response.data_size();
                        if (actualSize > 0 && static_cast<uint64_t>(actualSize) != state.expectedSize) {
                            state.expectedSize = static_cast<uint64_t>(actualSize);
                            continue;
                        }
                    }
                    if (!IsRetryableLocationError(itemStatus)
                        && itemStatus.GetCode() != K_OC_REMOTE_GET_NOT_ENOUGH) {
                        state.result->status = itemStatus;
                        state.completed = true;
                        continue;
                    }
                    AdvanceRetryableReplica(state, itemStatus);
                }
            }
        }

        if (dispatchExpired) {
            FinishUnresolvedWithDeadline(states, Status(K_RPC_DEADLINE_EXCEEDED,
                                                        "API deadline exceeded before data task dispatch"));
            break;
        }

        if (AllUnresolvedExhausted(states)) {
            Status backoffStatus = Backoff(backoffMs);
            if (backoffStatus.IsError()) {
                FinishUnresolvedWithDeadline(states, backoffStatus);
                break;
            }
            for (auto &state : states) {
                if (!state.completed) {
                    state.replicaIndex = 0;
                    state.exhausted = false;
                    ++state.round;
                }
            }
        }
    }
    return BuildAggregateStatus(states);
}
}  // namespace client
}  // namespace datasystem
