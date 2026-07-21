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

#include "datasystem/common/coordinator/coordinator_service_proxy.h"

#include <chrono>
#include <memory>
#include <thread>
#include <utility>

#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/protos/coordinator.brpc.stub.pb.h"
#include "datasystem/protos/coordinator.stub.rpc.pb.h"

namespace datasystem {
namespace {
constexpr int MAX_CAS_RETRY_TIMES = 16;
constexpr uint32_t CAS_MAX_SLEEP_TIME_US = 200000;

Status CheckCoordinatorAddress(const HostPort &coordinatorAddr)
{
    CHECK_FAIL_RETURN_STATUS(!coordinatorAddr.Empty(), StatusCode::K_NOT_READY, "coordinator address is not set");
    return Status::OK();
}

Status CheckResponseHeader(const coordinator::ResponseHeader &header)
{
    CHECK_FAIL_RETURN_STATUS(header.is_leader(), StatusCode::K_NOT_READY,
                             "coordinator is not leader, leader address: " + header.leader_address());
    CHECK_FAIL_RETURN_STATUS(header.coordinator_id().size() == UUID_SIZE, StatusCode::K_INVALID,
                             "Coordinator response contains an invalid CoordinatorId");
    return Status::OK();
}

void FillKeyValueEntry(const coordinator::KeyValue &kv, KeyValueEntry &entry)
{
    entry.key = kv.key();
    entry.value = kv.value();
    entry.version = kv.version();
    entry.modRevision = kv.mod_revision();
}

void FillKeyValueEntries(const google::protobuf::RepeatedPtrField<coordinator::KeyValue> &pbKvs,
                         std::vector<KeyValueEntry> &kvs)
{
    kvs.clear();
    kvs.reserve(static_cast<size_t>(pbKvs.size()));
    for (const auto &pbKv : pbKvs) {
        KeyValueEntry entry;
        FillKeyValueEntry(pbKv, entry);
        kvs.emplace_back(std::move(entry));
    }
}

template <typename StubT>
Status GetCoordinatorStub(const HostPort &coordinatorAddr, std::shared_ptr<StubT> &stub)
{
    RETURN_IF_NOT_OK(CheckCoordinatorAddress(coordinatorAddr));
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(coordinatorAddr, StubType::TO_COORDINATOR_SVC, rpcStub));
    stub = std::dynamic_pointer_cast<StubT>(rpcStub);
    RETURN_RUNTIME_ERROR_IF_NULL(stub);
    return Status::OK();
}

Status IdentityChangedStatus()
{
    return Status(StatusCode::K_TRY_AGAIN, "CoordinatorId changed; retry request on the current Coordinator");
}
}  // namespace

Status CoordinatorServiceProxyBase::Init()
{
    if (!cachedLeader_.Empty()) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(coordinatorDiscovery_ != nullptr, StatusCode::K_INVALID, "Coordinator Discovery is null");

    std::vector<std::string> candidates;
    Status discoveryStatus;
    try {
        discoveryStatus = coordinatorDiscovery_->GetCoordinators(candidates);
    } catch (...) {
        return Status(StatusCode::K_RUNTIME_ERROR, "Coordinator Discovery threw an exception");
    }
    RETURN_IF_NOT_OK(discoveryStatus);
    CHECK_FAIL_RETURN_STATUS(!candidates.empty(), StatusCode::K_INVALID, "Coordinator Discovery returned no addresses");

    HostPort candidate;
    const auto parseStatus = candidate.ParseString(candidates.front());
    CHECK_FAIL_RETURN_STATUS(parseStatus.IsOk() && !candidate.Empty(), StatusCode::K_INVALID,
                             "Coordinator Discovery returned an invalid front address");
    cachedLeader_ = std::move(candidate);
    return Status::OK();
}

class CoordinatorServiceProxyBase::InFlightScope final {
public:
    InFlightScope(CoordinatorServiceProxyBase &owner, std::string startedCoordinatorId, int32_t timeoutMs)
        : owner_(&owner), startedCoordinatorId_(std::move(startedCoordinatorId)), timeoutMs_(timeoutMs)
    {
    }

    InFlightScope(const InFlightScope &) = delete;
    InFlightScope &operator=(const InFlightScope &) = delete;

    InFlightScope(InFlightScope &&other) noexcept
        : owner_(other.owner_),
          startedCoordinatorId_(std::move(other.startedCoordinatorId_)),
          timeoutMs_(other.timeoutMs_)
    {
        other.owner_ = nullptr;
    }

    ~InFlightScope()
    {
        if (owner_ != nullptr) {
            owner_->CompleteRpc(startedCoordinatorId_);
        }
    }

    Status Accept(const coordinator::ResponseHeader &header, std::string *coordinatorId)
    {
        return owner_->AcceptResponse(header, timeoutMs_, coordinatorId);
    }

    const std::string &StartedCoordinatorId() const
    {
        return startedCoordinatorId_;
    }

private:
    CoordinatorServiceProxyBase *owner_;
    std::string startedCoordinatorId_;
    int32_t timeoutMs_;
};

template <typename ReqT, typename RspT, typename CallT>
Status CoordinatorServiceProxyBase::CallRaw(RpcOptions &options, const ReqT &req, RspT &rsp, CallT call)
{
    RETURN_IF_NOT_OK(CheckCoordinatorAddress(cachedLeader_));
    if (GetTransport() == Transport::ZMQ) {
        std::shared_ptr<coordinator::CoordinatorService_Stub> stub;
        RETURN_IF_NOT_OK(GetCoordinatorStub(cachedLeader_, stub));
        return call(*stub, options, req, rsp);
    }
    std::shared_ptr<coordinator::CoordinatorService_BrpcGenericStub> stub;
    RETURN_IF_NOT_OK(GetCoordinatorStub(cachedLeader_, stub));
    return call(*stub, options, req, rsp);
}

CoordinatorServiceProxyBase::InFlightScope CoordinatorServiceProxyBase::BeginRpc(int32_t timeoutMs)
{
    std::string startedCoordinatorId;
    {
        std::lock_guard<std::mutex> lock(identityMutex_);
        startedCoordinatorId = currentCoordinatorId_;
        if (!startedCoordinatorId.empty()) {
            ++inFlightByCoordinatorId_[startedCoordinatorId];
        }
    }
    return InFlightScope(*this, std::move(startedCoordinatorId), timeoutMs);
}

void CoordinatorServiceProxyBase::CompleteRpc(const std::string &startedCoordinatorId)
{
    if (startedCoordinatorId.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(identityMutex_);
    auto iter = inFlightByCoordinatorId_.find(startedCoordinatorId);
    if (iter == inFlightByCoordinatorId_.end() || --iter->second != 0) {
        return;
    }
    inFlightByCoordinatorId_.erase(iter);
}

Status CoordinatorServiceProxyBase::AcceptResponse(const coordinator::ResponseHeader &header, int32_t timeoutMs,
                                                   std::string *coordinatorId)
{
    RETURN_IF_NOT_OK(CheckResponseHeader(header));
    const std::string &responseId = header.coordinator_id();
    {
        std::lock_guard<std::mutex> lock(identityMutex_);
        if (currentCoordinatorId_.empty()) {
            currentCoordinatorId_ = responseId;
        }
        if (currentCoordinatorId_ == responseId) {
            if (coordinatorId != nullptr) {
                *coordinatorId = responseId;
            }
            return Status::OK();
        }
        if (inFlightByCoordinatorId_.count(responseId) != 0) {
            return IdentityChangedStatus();
        }
    }
    RETURN_IF_NOT_OK(ConfirmResponseIdentity(responseId, timeoutMs));
    if (coordinatorId != nullptr) {
        *coordinatorId = responseId;
    }
    return Status::OK();
}

Status CoordinatorServiceProxyBase::ConfirmResponseIdentity(const std::string &responseId, int32_t timeoutMs)
{
    std::lock_guard<std::mutex> refreshLock(identityRefreshMutex_);
    {
        std::lock_guard<std::mutex> identityLock(identityMutex_);
        if (currentCoordinatorId_ == responseId) {
            return Status::OK();
        }
        if (inFlightByCoordinatorId_.count(responseId) != 0) {
            return IdentityChangedStatus();
        }
    }
    std::string probedId;
    RETURN_IF_NOT_OK(ProbeCoordinatorId(timeoutMs, probedId));
    RETURN_IF_NOT_OK(InstallProbedIdentity(probedId));
    return probedId == responseId ? Status::OK() : IdentityChangedStatus();
}

Status CoordinatorServiceProxyBase::ProbeCoordinatorId(int32_t timeoutMs, std::string &coordinatorId)
{
    coordinator::GetCoordinatorIdReqPb req;
    coordinator::GetCoordinatorIdRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, rsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.GetCoordinatorId(opts, request, response);
    }));
    RETURN_IF_NOT_OK(CheckResponseHeader(rsp.header()));
    coordinatorId = rsp.header().coordinator_id();
    return Status::OK();
}

Status CoordinatorServiceProxyBase::InstallProbedIdentity(const std::string &coordinatorId)
{
    std::lock_guard<std::mutex> lock(identityMutex_);
    if (currentCoordinatorId_ == coordinatorId) {
        return Status::OK();
    }
    if (inFlightByCoordinatorId_.count(coordinatorId) != 0) {
        return IdentityChangedStatus();
    }
    currentCoordinatorId_ = coordinatorId;
    return Status::OK();
}

Status CoordinatorServiceProxyBase::Put(const std::string &key, const std::string &value, int64_t ttlMs,
                                        int64_t expectedVersion, int64_t &version, int64_t &revision, int32_t timeoutMs,
                                        std::string *coordinatorId, const std::string &expectedCoordinatorId)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::PutReqPb req;
    req.set_key(key);
    req.set_value(value);
    req.set_ttl(ttlMs);
    req.set_expected_version(expectedVersion);
    req.set_expected_coordinator_id(expectedCoordinatorId);
    coordinator::PutRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, rsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.Put(opts, request, response);
    }));
    RETURN_IF_NOT_OK(inFlight.Accept(rsp.header(), coordinatorId));
    version = rsp.version();
    revision = rsp.revision();
    return Status::OK();
}

Status CoordinatorServiceProxyBase::Range(const std::string &key, const std::string &rangeEnd,
                                          std::vector<KeyValueEntry> &kvs, int64_t &revision, int32_t timeoutMs,
                                          std::string *coordinatorId)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::RangeReqPb req;
    req.set_key(key);
    req.set_range_end(rangeEnd);
    coordinator::RangeRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, rsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.Range(opts, request, response);
    }));
    RETURN_IF_NOT_OK(inFlight.Accept(rsp.header(), coordinatorId));
    FillKeyValueEntries(rsp.kvs(), kvs);
    revision = rsp.revision();
    return Status::OK();
}

Status CoordinatorServiceProxyBase::DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted,
                                                int64_t &revision, int32_t timeoutMs)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::DeleteRangeReqPb req;
    req.set_key(key);
    req.set_range_end(rangeEnd);
    req.set_expected_coordinator_id(inFlight.StartedCoordinatorId());
    coordinator::DeleteRangeRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, rsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.DeleteRange(opts, request, response);
    }));
    RETURN_IF_NOT_OK(inFlight.Accept(rsp.header(), nullptr));
    deleted = rsp.deleted();
    revision = rsp.revision();
    return Status::OK();
}

Status CoordinatorServiceProxyBase::WatchRange(const std::string &key, const std::string &rangeEnd,
                                               const std::string &watcherAddr, const std::string &registrationId,
                                               int64_t &watchId, std::vector<KeyValueEntry> &initialKvs,
                                               int32_t timeoutMs, std::string *coordinatorId)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::WatchRangeReqPb req;
    req.set_key(key);
    req.set_range_end(rangeEnd);
    req.set_watcher_addr(watcherAddr);
    req.set_registration_id(registrationId);
    coordinator::WatchRangeRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, rsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.WatchRange(opts, request, response);
    }));
    RETURN_IF_NOT_OK(inFlight.Accept(rsp.header(), coordinatorId));
    watchId = rsp.watch_id();
    FillKeyValueEntries(rsp.initial_kvs(), initialKvs);
    return Status::OK();
}

Status CoordinatorServiceProxyBase::CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds,
                                                const std::string &expectedCoordinatorId, int32_t timeoutMs)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::CancelWatchReqPb req;
    req.set_watcher_addr(watcherAddr);
    req.set_expected_coordinator_id(expectedCoordinatorId);
    for (const auto watchId : watchIds) {
        req.add_watch_ids(watchId);
    }
    coordinator::CancelWatchRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, rsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.CancelWatch(opts, request, response);
    }));
    return inFlight.Accept(rsp.header(), nullptr);
}

Status CoordinatorServiceProxyBase::KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs,
                                              int32_t timeoutMs, std::string *coordinatorId)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::KeepAliveReqPb req;
    req.set_key(key);
    coordinator::KeepAliveRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, rsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.KeepAlive(opts, request, response);
    }));
    RETURN_IF_NOT_OK(inFlight.Accept(rsp.header(), coordinatorId));
    ttlMs = rsp.ttl();
    remainingTtlMs = rsp.remaining_ttl();
    return Status::OK();
}

Status CoordinatorServiceProxyBase::GetCoordinatorId(std::string &coordinatorId, int32_t timeoutMs)
{
    auto inFlight = BeginRpc(timeoutMs);
    std::lock_guard<std::mutex> refreshLock(identityRefreshMutex_);
    std::string probedId;
    RETURN_IF_NOT_OK(ProbeCoordinatorId(timeoutMs, probedId));
    RETURN_IF_NOT_OK(InstallProbedIdentity(probedId));
    coordinatorId = std::move(probedId);
    return Status::OK();
}

Status CoordinatorServiceProxyBase::ReportTopologyRecoveryCandidate(
    const coordinator::ReportTopologyRecoveryCandidateReqPb &req,
    coordinator::ReportTopologyRecoveryCandidateRspPb &rsp, int32_t timeoutMs)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::ReportTopologyRecoveryCandidateRspPb localRsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, localRsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.ReportTopologyRecoveryCandidate(opts, request, response);
    }));
    RETURN_IF_NOT_OK(inFlight.Accept(localRsp.header(), nullptr));
    rsp = std::move(localRsp);
    return Status::OK();
}

Status CoordinatorServiceProxyBase::GetClusterRawSnapshot(const coordinator::GetClusterRawSnapshotReqPb &req,
                                                          coordinator::GetClusterRawSnapshotRspPb &rsp,
                                                          int32_t timeoutMs)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::GetClusterRawSnapshotRspPb localRsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, localRsp,
                             [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.GetClusterRawSnapshot(opts, request, response);
    }));
    RETURN_IF_NOT_OK(inFlight.Accept(localRsp.header(), nullptr));
    rsp = std::move(localRsp);
    return Status::OK();
}

void CoordinatorServiceProxyBase::GetObservedCoordinatorId(std::string &coordinatorId) const
{
    std::lock_guard<std::mutex> lock(identityMutex_);
    coordinatorId = currentCoordinatorId_;
}

Status CoordinatorServiceProxyBase::CAS(const std::string &key, const CasProcessFunc &processFunc, int64_t &version,
                                        int64_t &revision)
{
    CHECK_FAIL_RETURN_STATUS(processFunc != nullptr, StatusCode::K_INVALID, "CAS process function is null");
    Status lastErr;
    RandomData randomData;
    for (int retry = 0; retry < MAX_CAS_RETRY_TIMES; ++retry) {
        std::this_thread::sleep_for(std::chrono::microseconds(randomData.GetRandomUint32(0, CAS_MAX_SLEEP_TIME_US)));
        std::vector<KeyValueEntry> kvs;
        int64_t rangeRevision = 0;
        std::string rangeCoordinatorId;
        RETURN_IF_NOT_OK(Range(key, "", kvs, rangeRevision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, &rangeCoordinatorId));
        std::string oldValue;
        int64_t expectedVersion = COORDINATOR_KEY_NOT_EXISTS_VERSION;
        if (!kvs.empty()) {
            oldValue = kvs.front().value;
            expectedVersion = kvs.front().version;
        }
        std::unique_ptr<std::string> newValue;
        bool retryByCaller = true;
        Status status = processFunc(oldValue, newValue, retryByCaller);
        if (status.IsError()) {
            if (!retryByCaller) {
                return status;
            }
            lastErr = status;
            continue;
        }
        if (newValue == nullptr) {
            std::string currentCoordinatorId;
            auto identityStatus = GetCoordinatorId(currentCoordinatorId, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS);
            if (identityStatus.IsOk() && currentCoordinatorId == rangeCoordinatorId) {
                return Status::OK();
            }
            lastErr = identityStatus.IsError() ? identityStatus : IdentityChangedStatus();
            continue;
        }
        std::string putCoordinatorId;
        Status rc = Put(key, *newValue, 0, expectedVersion, version, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS,
                        &putCoordinatorId, rangeCoordinatorId);
        if (rc.IsOk() && putCoordinatorId != rangeCoordinatorId) {
            rc = IdentityChangedStatus();
        }
        if (rc.IsOk()) {
            return Status::OK();
        }
        if (rc.GetCode() != StatusCode::K_TRY_AGAIN && rc.GetCode() != StatusCode::K_INVALID
            && rc.GetCode() != StatusCode::K_NOT_FOUND) {
            return rc;
        }
        lastErr = rc;
    }
    return lastErr.IsError() ? lastErr : Status(StatusCode::K_TRY_AGAIN, "coordinator CAS exceeded retry limit");
}

}  // namespace datasystem
