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

struct ReadState {
    const master::ObjectLocationInfoPb *location = nullptr;
    ObjectReadItemResult *result = nullptr;
    size_t inputIndex = 0;
    size_t replicaIndex = 0;
    size_t round = 1;
    uint64_t expectedSize = 0;
    Status lastStatus = Status(K_NOT_FOUND, "Cannot get objects from worker");
    bool hasAttempt = false;
    bool completed = false;
    bool exhausted = false;
};

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
}  // namespace

ReplicaReader::ReplicaReader(std::shared_ptr<DataPlaneExecutor> executor, std::shared_ptr<DeadlineRetry> retry,
                             std::shared_ptr<ThreadPool> taskPool)
    : executor_(std::move(executor)), retry_(std::move(retry)), taskPool_(std::move(taskPool))
{
}

bool ReplicaReader::IsRetryableLocationError(const Status &status) const
{
    if (retry_->IsRetryableRpcError(status)) {
        return true;
    }
    switch (status.GetCode()) {
        case K_URMA_NEED_CONNECT:
        case K_URMA_CONNECT_FAILED:
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

Status ReplicaReader::Read(const master::ObjectLocationInfoPb &location, ObjectReadItemResult &result)
{
    RETURN_RUNTIME_ERROR_IF_NULL(executor_);
    RETURN_RUNTIME_ERROR_IF_NULL(retry_);
    CHECK_FAIL_RETURN_STATUS(location.object_locations_size() > 0, K_NOT_FOUND, "Object was not found");
    int64_t backoffMs = 1;
    size_t round = 0;
    Status lastError(K_NOT_FOUND, "Cannot get objects from worker");
    while (true) {
        ++round;
        for (const auto &address : location.object_locations()) {
            RETURN_IF_NOT_OK(retry_->CheckDeadline());
            HostPort workerAddr;
            RETURN_IF_NOT_OK(workerAddr.ParseString(address));
            VLOG(1) << "[TransportGet][Data] Read replica, key: " << location.object_key()
                    << ", worker: " << workerAddr.ToString() << ", round: " << round;
            DataGetResult data;
            DataGetRequest request{ location.object_key(), location.object_size() };
            Status rc = executor_->Execute(workerAddr, [&request, &data](IDataTransporter &transporter) {
                return transporter.Get(request, data);
            });
            if (rc.IsOk()) {
                result.objectKey = location.object_key();
                result.data = std::move(data);
                VLOG(1) << "[TransportGet][Data] Read succeeded, key: " << location.object_key()
                        << ", worker: " << workerAddr.ToString() << ", round: " << round;
                return Status::OK();
            }
            const bool retryable = IsRetryableLocationError(rc);
            VLOG(1) << "[TransportGet][Data] Replica read failed, key: " << location.object_key()
                    << ", worker: " << workerAddr.ToString() << ", round: " << round
                    << ", retryable: " << retryable << ", status: " << rc.ToString();
            if (!retryable) {
                return rc;
            }
            lastError = rc;
        }
        Status backoffRc = retry_->Backoff(backoffMs);
        if (backoffRc.IsError()) {
            backoffRc.AppendMsg(lastError.GetMsg());
            return backoffRc;
        }
    }
}

Status ReplicaReader::ReadBatch(const ReplicaReadBatch &requests)
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
            chunks.back().requests.push_back({ state.location->object_key(), state.expectedSize });
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
            auto *endpointWork = &work;
            futures.emplace_back(taskPool_->Submit([this, endpointWork, remainingUs, dispatchTime, traceContext]() {
                TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
                Status dispatchStatus = InitTimeoutsFromDispatch(remainingUs, dispatchTime);
                for (auto &chunk : endpointWork->chunks) {
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
                        });
                        chunk.results.resize(1);
                        chunk.results.front().status = unaryStatus;
                        if (unaryStatus.IsOk() || unaryStatus.GetCode() == K_OC_REMOTE_GET_NOT_ENOUGH) {
                            chunk.results.front().data = std::move(data);
                        }
                        chunk.endpointStatus = unaryStatus.GetCode() == K_OC_REMOTE_GET_NOT_ENOUGH
                                                   ? Status::OK()
                                                   : std::move(unaryStatus);
                    } else {
                        chunk.endpointStatus = executor_->Execute(endpointWork->address, [&chunk](IDataTransporter &t) {
                            return t.BatchGet(chunk.requests, chunk.results);
                        });
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
                    ++state.replicaIndex;
                    if (state.replicaIndex >= static_cast<size_t>(state.location->object_locations_size())) {
                        state.exhausted = true;
                    } else {
                        METRIC_INC(metrics::KvMetricId::CLIENT_DIRECT_BATCH_GET_REPLICA_RETRY_TOTAL);
                    }
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
