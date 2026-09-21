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

#include "datasystem/common/log/logging.h"
#include "datasystem/common/rpc/bthread_utils.h"
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
constexpr uint64_t COORDINATOR_PROXY_RPC_STUB_CACHE_SIZE = 100;
constexpr uint32_t LEADER_CALLBACK_FAILURE_LOG_EVERY_N = 100;
constexpr auto COORDINATOR_ROUTE_RETRY_INTERVAL = std::chrono::milliseconds(50);

Status CheckCoordinatorAddress(const HostPort &coordinatorAddr)
{
    CHECK_FAIL_RETURN_STATUS(!coordinatorAddr.Empty(), StatusCode::K_NOT_READY, "coordinator address is not set");
    return Status::OK();
}

Status CheckResponseHeader(const coordinator::ResponseHeader &header, bool allowLeaderRecovering = false)
{
    const bool accepted = header.state() == coordinator::ResponseHeader::SERVING
                          || (allowLeaderRecovering && header.state() == coordinator::ResponseHeader::RECOVERING);
    CHECK_FAIL_RETURN_STATUS(accepted, StatusCode::K_NOT_READY,
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

CoordinatorLeaderRouter::RpcResult RouteResult(Status status, const coordinator::ResponseHeader &header)
{
    if (header.coordinator_id().size() != UUID_SIZE) {
        if (status.IsOk()) {
            CoordinatorLeaderRouter::RpcResponseHeader invalid;
            return { Status(K_INVALID, "Coordinator response contains an invalid CoordinatorId"), std::move(invalid) };
        }
        return { std::move(status), std::nullopt };
    }

    CoordinatorLeaderRouter::RpcResponseHeader route;
    route.leaderAddress = header.leader_address();
    route.coordinatorId = header.coordinator_id();
    route.leaderTerm = header.leader_term();
    if (header.state() == coordinator::ResponseHeader::RECOVERING) {
        route.state = CoordinatorLeaderRouter::RpcResponseHeader::State::RECOVERING;
    } else if (header.state() == coordinator::ResponseHeader::SERVING) {
        route.state = CoordinatorLeaderRouter::RpcResponseHeader::State::SERVING;
    } else if (header.state() == coordinator::ResponseHeader::NOT_LEADER) {
        route.state = CoordinatorLeaderRouter::RpcResponseHeader::State::NOT_LEADER;
    }
    return { std::move(status), std::move(route) };
}
}  // namespace

CoordinatorServiceProxyBase::~CoordinatorServiceProxyBase() = default;

Status CoordinatorServiceProxyBase::Init()
{
    if (router_ != nullptr) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(coordinatorDiscovery_ != nullptr, StatusCode::K_INVALID, "Coordinator Discovery is null");
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(COORDINATOR_PROXY_RPC_STUB_CACHE_SIZE));

    std::vector<std::string> candidates;
    Status discoveryStatus;
    try {
        discoveryStatus = coordinatorDiscovery_->GetCoordinators(candidates);
    } catch (...) {
        return Status(StatusCode::K_RUNTIME_ERROR, "Coordinator Discovery threw an exception");
    }
    RETURN_IF_NOT_OK(discoveryStatus);
    CHECK_FAIL_RETURN_STATUS(!candidates.empty(), StatusCode::K_INVALID, "Coordinator Discovery returned no addresses");

    std::vector<std::string> initialCandidates;
    for (const auto &value : candidates) {
        HostPort parsed;
        if (parsed.ParseString(value).IsOk() && !parsed.Empty()) {
            initialCandidates.emplace_back(parsed.ToString());
        }
    }
    CHECK_FAIL_RETURN_STATUS(!initialCandidates.empty(), StatusCode::K_INVALID,
                             "Coordinator Discovery returned no valid addresses");
    discoveryCache_ =
        std::make_unique<CoordinatorDiscoveryCache>(coordinatorDiscovery_, std::move(initialCandidates));
    auto *discoveryCache = discoveryCache_.get();
    router_ = std::make_unique<CoordinatorLeaderRouter>(CoordinatorLeaderRouter::Dependencies{
        .getCandidateSnapshot = [discoveryCache] { return discoveryCache->GetCandidateSnapshot(); },
        .refreshCandidates = [discoveryCache] { discoveryCache->RefreshAsync(); },
        .publishLeaderIdentity = [this](const CoordinatorLeaderIdentity &identity) { PublishLeaderIdentity(identity); },
        .now = [] { return std::chrono::steady_clock::now(); },
        .wait = [](std::chrono::milliseconds duration) { SleepCurrentFor(duration); },
    });
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

    Status Accept(const coordinator::ResponseHeader &header, std::string *coordinatorId,
                  bool allowLeaderRecovering = false)
    {
        return owner_->AcceptResponse(header, timeoutMs_, coordinatorId, allowLeaderRecovering);
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
Status CoordinatorServiceProxyBase::CallRawAt(const HostPort &address, RpcOptions &options, const ReqT &req, RspT &rsp,
                                              CallT call)
{
    RETURN_IF_NOT_OK(CheckCoordinatorAddress(address));
    auto reportCoordinatorFailure = [&address](Status rc) {
        if (rc.IsError()) {
            rc.AppendMsg("Failed to reach coordinator " + address.ToString()
                          + ". Check --coordinator_address config and whether the coordinator is running.");
        }
        return rc;
    };
    std::shared_ptr<coordinator::CoordinatorService_BrpcGenericStub> stub;
    RETURN_IF_NOT_OK(reportCoordinatorFailure(GetCoordinatorStub(address, stub)));
    return reportCoordinatorFailure(call(*stub, options, req, rsp));
}

template <typename ReqT, typename RspT, typename CallT>
Status CoordinatorServiceProxyBase::CallRaw(RpcOptions &options, const ReqT &req, RspT &rsp, CallT call,
                                            bool recoveryControl)
{
    CHECK_FAIL_RETURN_STATUS(router_ != nullptr, K_NOT_READY, "Coordinator leader router is not initialized");
    const auto timeout = std::chrono::milliseconds(options.GetTimeout());
    auto status = router_->Execute(
        [this, &req, &rsp, &call](const HostPort &address, std::chrono::milliseconds attemptTimeout) {
            rsp.Clear();
            RpcOptions attemptOptions;
            attemptOptions.SetTimeout(static_cast<int>(attemptTimeout.count()));
            auto status = CallRawAt(address, attemptOptions, req, rsp, call);
            return RouteResult(std::move(status), rsp.header());
        },
        std::chrono::steady_clock::now() + timeout, timeout, COORDINATOR_ROUTE_RETRY_INTERVAL, recoveryControl);
    if (status.IsError()) {
        status.AppendMsg("Coordinator request=" + req.GetTypeName());
    }
    return status;
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
                                                   std::string *coordinatorId, bool allowLeaderRecovering)
{
    RETURN_IF_NOT_OK(CheckResponseHeader(header, allowLeaderRecovering));
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
    RETURN_IF_NOT_OK(ConfirmResponseIdentity(responseId, timeoutMs, allowLeaderRecovering));
    if (coordinatorId != nullptr) {
        *coordinatorId = responseId;
    }
    return Status::OK();
}

Status CoordinatorServiceProxyBase::ConfirmResponseIdentity(const std::string &responseId, int32_t timeoutMs,
                                                            bool allowLeaderRecovering)
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
    RETURN_IF_NOT_OK(ProbeCoordinatorId(timeoutMs, probedId, allowLeaderRecovering));
    RETURN_IF_NOT_OK(InstallProbedIdentity(probedId));
    return probedId == responseId ? Status::OK() : IdentityChangedStatus();
}

Status CoordinatorServiceProxyBase::ProbeCoordinatorId(int32_t timeoutMs, std::string &coordinatorId,
                                                       bool allowLeaderRecovering)
{
    coordinator::GetCoordinatorIdReqPb req;
    coordinator::GetCoordinatorIdRspPb rsp;
    CHECK_FAIL_RETURN_STATUS(router_ != nullptr, K_NOT_READY, "Coordinator leader router is not initialized");
    const auto timeout = std::chrono::milliseconds(timeoutMs);
    RETURN_IF_NOT_OK(router_->Execute(
        [this, &req, &rsp](const HostPort &address, std::chrono::milliseconds attemptTimeout) {
            rsp.Clear();
            RpcOptions options;
            options.SetTimeout(static_cast<int>(attemptTimeout.count()));
            auto status =
                CallRawAt(address, options, req, rsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
                    return stub.GetCoordinatorId(opts, request, response);
                });
            return RouteResult(std::move(status), rsp.header());
        },
        std::chrono::steady_clock::now() + timeout, timeout, COORDINATOR_ROUTE_RETRY_INTERVAL,
        allowLeaderRecovering));
    RETURN_IF_NOT_OK(CheckResponseHeader(rsp.header(), allowLeaderRecovering));
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
                                        std::string *coordinatorId, const std::string &expectedCoordinatorId,
                                        int64_t expectedModRevision)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::PutReqPb req;
    req.set_key(key);
    req.set_value(value);
    req.set_ttl(ttlMs);
    req.set_expected_version(expectedVersion);
    req.set_expected_coordinator_id(expectedCoordinatorId);
    req.set_expected_mod_revision(expectedModRevision);
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

Status CoordinatorServiceProxyBase::RangeIfChanged(const std::string &key, int64_t knownModRevision,
                                                   const std::string &knownCoordinatorId,
                                                   std::vector<KeyValueEntry> &kvs, int64_t &revision, bool &unchanged,
                                                   int32_t timeoutMs, std::string *coordinatorId)
{
    CHECK_FAIL_RETURN_STATUS(knownModRevision > 0, K_INVALID, "known modification revision must be positive");
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::RangeReqPb req;
    req.set_key(key);
    req.set_known_mod_revision(knownCoordinatorId.empty() ? 0 : knownModRevision);
    coordinator::RangeRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, rsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.Range(opts, request, response);
    }));
    std::string responseCoordinatorId;
    RETURN_IF_NOT_OK(inFlight.Accept(rsp.header(), &responseCoordinatorId));
    if (rsp.unchanged() && !CanAcceptUnchangedRange(knownCoordinatorId, responseCoordinatorId)) {
        unchanged = false;
        return Range(key, "", kvs, revision, timeoutMs, coordinatorId);
    }
    if (coordinatorId != nullptr) {
        *coordinatorId = responseCoordinatorId;
    }
    FillKeyValueEntries(rsp.kvs(), kvs);
    revision = rsp.revision();
    unchanged = rsp.unchanged();
    return Status::OK();
}

bool CoordinatorServiceProxyBase::CanAcceptUnchangedRange(const std::string &knownCoordinatorId,
                                                          const std::string &responseCoordinatorId)
{
    return !knownCoordinatorId.empty() && knownCoordinatorId == responseCoordinatorId;
}

Status CoordinatorServiceProxyBase::DeleteRange(
    const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision, int32_t timeoutMs,
    int64_t expectedModRevision)
{
    return DeleteRangeInternal(key, rangeEnd, deleted, revision, timeoutMs, expectedModRevision, false);
}

Status CoordinatorServiceProxyBase::DeleteMembership(
    const std::string &key, int64_t &deleted, int64_t &revision, int32_t timeoutMs,
    const std::string &expectedCoordinatorId, int64_t expectedModRevision)
{
    return DeleteRangeInternal(key, "", deleted, revision, timeoutMs, expectedModRevision, true,
                               expectedCoordinatorId);
}

Status CoordinatorServiceProxyBase::DeleteRangeInternal(
    const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision, int32_t timeoutMs,
    int64_t expectedModRevision, bool recoveryControl, const std::string &expectedCoordinatorId)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::DeleteRangeReqPb req;
    req.set_key(key);
    req.set_range_end(rangeEnd);
    req.set_expected_coordinator_id(expectedCoordinatorId.empty() ? inFlight.StartedCoordinatorId()
                                                                  : expectedCoordinatorId);
    req.set_expected_mod_revision(expectedModRevision);
    coordinator::DeleteRangeRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, rsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.DeleteRange(opts, request, response);
    }, recoveryControl));
    RETURN_IF_NOT_OK(inFlight.Accept(rsp.header(), nullptr, recoveryControl));
    deleted = rsp.deleted();
    revision = rsp.revision();
    return Status::OK();
}

Status CoordinatorServiceProxyBase::WatchRange(const std::string &key, const std::string &rangeEnd,
                                               const std::string &watcherAddr, const std::string &registrationId,
                                               int64_t &watchId, std::vector<KeyValueEntry> &initialKvs,
                                               int32_t timeoutMs, std::string *coordinatorId, bool skipInitialKvs)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::WatchRangeReqPb req;
    req.set_key(key);
    req.set_range_end(rangeEnd);
    req.set_watcher_addr(watcherAddr);
    req.set_registration_id(registrationId);
    req.set_skip_initial_kvs(skipInitialKvs);
    coordinator::WatchRangeRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(
        options, req, rsp,
        [](auto &stub, auto &opts, const auto &request, auto &response) {
            return stub.WatchRange(opts, request, response);
        },
        false));
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
                                              int32_t timeoutMs, std::string *coordinatorId,
                                              const std::string &expectedCoordinatorId, int64_t expectedModRevision,
                                              const std::vector<std::string> &failedTargets)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::KeepAliveReqPb req;
    req.set_key(key);
    req.set_expected_coordinator_id(expectedCoordinatorId);
    req.set_expected_mod_revision(expectedModRevision);
    for (const auto &target : failedTargets) {
        req.add_failed_targets(target);
    }
    coordinator::KeepAliveRspPb rsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(
        options, req, rsp,
        [](auto &stub, auto &opts, const auto &request, auto &response) {
            return stub.KeepAlive(opts, request, response);
        },
        true));
    RETURN_IF_NOT_OK(inFlight.Accept(rsp.header(), coordinatorId, true));
    ttlMs = rsp.ttl();
    remainingTtlMs = rsp.remaining_ttl();
    return Status::OK();
}

Status CoordinatorServiceProxyBase::GetCoordinatorId(std::string &coordinatorId, int32_t timeoutMs)
{
    auto inFlight = BeginRpc(timeoutMs);
    std::lock_guard<std::mutex> refreshLock(identityRefreshMutex_);
    std::string probedId;
    RETURN_IF_NOT_OK(ProbeCoordinatorId(timeoutMs, probedId, false));
    RETURN_IF_NOT_OK(InstallProbedIdentity(probedId));
    coordinatorId = std::move(probedId);
    return Status::OK();
}

Status CoordinatorServiceProxyBase::ReportTopologyRecoveryCandidate(
    const coordinator::ReportTopologyRecoveryCandidateReqPb &req,
    coordinator::ReportTopologyRecoveryCandidateRspPb &rsp, int32_t timeoutMs)
{
    coordinator::ReportTopologyRecoveryCandidateRspPb localRsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(
        options, req, localRsp,
        [](auto &stub, auto &opts, const auto &request, auto &response) {
            return stub.ReportTopologyRecoveryCandidate(opts, request, response);
        },
        true));
    rsp = std::move(localRsp);
    return Status::OK();
}

Status CoordinatorServiceProxyBase::ReportWorkerLiveness(const coordinator::ReportWorkerLivenessReqPb &req,
                                                          coordinator::ReportWorkerLivenessRspPb &rsp,
                                                          int32_t timeoutMs)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::ReportWorkerLivenessRspPb localRsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, localRsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
        return stub.ReportWorkerLiveness(opts, request, response);
    }));
    RETURN_IF_NOT_OK(inFlight.Accept(localRsp.header(), nullptr));
    rsp = std::move(localRsp);
    return Status::OK();
}

Status CoordinatorServiceProxyBase::EnsureLeaderMembership(const coordinator::EnsureLeaderMembershipReqPb &req,
                                                           coordinator::EnsureLeaderMembershipRspPb &rsp,
                                                           int32_t timeoutMs)
{
    rsp.Clear();
    coordinator::EnsureLeaderMembershipRspPb localRsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    const auto status = CallRaw(
        options, req, localRsp,
        [](auto &stub, auto &opts, const auto &request, auto &response) {
            return stub.EnsureLeaderMembership(opts, request, response);
        },
        true);
    if (status.IsOk() && localRsp.result() == coordinator::EnsureLeaderMembershipRspPb::ACCEPTED) {
        RETURN_IF_NOT_OK(CheckResponseHeader(localRsp.header(), true));
        CHECK_FAIL_RETURN_STATUS(localRsp.header().coordinator_id() == req.coordinator_id()
                                     && localRsp.header().leader_term() == req.leader_term(),
                                 K_TRY_AGAIN, "accepted membership Ensure response identity does not match request");
        RETURN_IF_NOT_OK(InstallProbedIdentity(localRsp.header().coordinator_id()));
    }
    if (!localRsp.header().coordinator_id().empty()) {
        rsp = std::move(localRsp);
    }
    return status;
}

Status CoordinatorServiceProxyBase::GetRouter(CoordinatorLeaderRouter *&router)
{
    router = router_.get();
    CHECK_FAIL_RETURN_STATUS(router != nullptr, K_NOT_READY, "Coordinator Leader router is not initialized");
    return Status::OK();
}

Status CoordinatorServiceProxyBase::SetLeaderChangeHandler(
    std::function<void(const CoordinatorLeaderIdentity &)> handler)
{
    std::lock_guard<std::mutex> lock(leaderCallbackMutex_);
    leaderChangeHandler_ = std::move(handler);
    return Status::OK();
}

void CoordinatorServiceProxyBase::PublishLeaderIdentity(const CoordinatorLeaderIdentity &identity)
{
    std::lock_guard<std::mutex> lock(leaderCallbackMutex_);
    if (leaderChangeHandler_ == nullptr) {
        return;
    }
    try {
        leaderChangeHandler_(identity);
    } catch (const std::exception &exception) {
        LOG_EVERY_N(WARNING, LEADER_CALLBACK_FAILURE_LOG_EVERY_N)
            << "Coordinator Leader change callback failed: " << exception.what();
    } catch (...) {
        LOG_EVERY_N(WARNING, LEADER_CALLBACK_FAILURE_LOG_EVERY_N)
            << "Coordinator Leader change callback failed with an unknown exception";
    }
}

Status CoordinatorServiceProxyBase::GetClusterRawSnapshot(const coordinator::GetClusterRawSnapshotReqPb &req,
                                                          coordinator::GetClusterRawSnapshotRspPb &rsp,
                                                          int32_t timeoutMs)
{
    auto inFlight = BeginRpc(timeoutMs);
    coordinator::GetClusterRawSnapshotRspPb localRsp;
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(CallRaw(options, req, localRsp, [](auto &stub, auto &opts, const auto &request, auto &response) {
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
        if (rc.GetCode() != StatusCode::K_TRY_AGAIN && rc.GetCode() != StatusCode::K_DUPLICATED
            && rc.GetCode() != StatusCode::K_DATA_INCONSISTENCY && rc.GetCode() != StatusCode::K_NOT_FOUND) {
            return rc;
        }
        lastErr = rc;
    }
    return lastErr.IsError() ? lastErr : Status(StatusCode::K_TRY_AGAIN, "coordinator CAS exceeded retry limit");
}

}  // namespace datasystem
