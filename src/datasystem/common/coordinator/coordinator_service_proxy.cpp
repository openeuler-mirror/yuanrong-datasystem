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
#include <vector>

#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
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

Status CheckLeader(const coordinator::ResponseHeader &header)
{
    CHECK_FAIL_RETURN_STATUS(header.is_leader(), StatusCode::K_NOT_READY,
                             "coordinator is not leader, leader address: " + header.leader_address());
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

template <typename StubT>
Status PutImpl(const HostPort &coordinatorAddr, const std::string &key, const std::string &value, int64_t ttlMs,
               int64_t expectedVersion, int64_t &version, int64_t &revision, int32_t timeoutMs)
{
    std::shared_ptr<StubT> stub;
    RETURN_IF_NOT_OK(GetCoordinatorStub(coordinatorAddr, stub));

    coordinator::PutReqPb req;
    req.set_key(key);
    req.set_value(value);
    req.set_ttl(ttlMs);
    req.set_expected_version(expectedVersion);
    coordinator::PutRspPb rsp;
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(stub->Put(opts, req, rsp));
    RETURN_IF_NOT_OK(CheckLeader(rsp.header()));
    version = rsp.version();
    revision = rsp.revision();
    return Status::OK();
}

template <typename StubT>
Status RangeImpl(const HostPort &coordinatorAddr, const std::string &key, const std::string &rangeEnd,
                 std::vector<KeyValueEntry> &kvs, int64_t &revision, int32_t timeoutMs)
{
    std::shared_ptr<StubT> stub;
    RETURN_IF_NOT_OK(GetCoordinatorStub(coordinatorAddr, stub));

    coordinator::RangeReqPb req;
    req.set_key(key);
    req.set_range_end(rangeEnd);
    coordinator::RangeRspPb rsp;
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(stub->Range(opts, req, rsp));
    RETURN_IF_NOT_OK(CheckLeader(rsp.header()));
    FillKeyValueEntries(rsp.kvs(), kvs);
    revision = rsp.revision();
    return Status::OK();
}

template <typename StubT>
Status DeleteRangeImpl(const HostPort &coordinatorAddr, const std::string &key, const std::string &rangeEnd,
                       int64_t &deleted, int64_t &revision, int32_t timeoutMs)
{
    std::shared_ptr<StubT> stub;
    RETURN_IF_NOT_OK(GetCoordinatorStub(coordinatorAddr, stub));

    coordinator::DeleteRangeReqPb req;
    req.set_key(key);
    req.set_range_end(rangeEnd);
    coordinator::DeleteRangeRspPb rsp;
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(stub->DeleteRange(opts, req, rsp));
    RETURN_IF_NOT_OK(CheckLeader(rsp.header()));
    deleted = rsp.deleted();
    revision = rsp.revision();
    return Status::OK();
}

template <typename StubT>
Status WatchRangeImpl(const HostPort &coordinatorAddr, const std::string &key, const std::string &rangeEnd,
                      const std::string &watcherAddr, int64_t &watchId, std::vector<KeyValueEntry> &initialKvs,
                      int32_t timeoutMs)
{
    std::shared_ptr<StubT> stub;
    RETURN_IF_NOT_OK(GetCoordinatorStub(coordinatorAddr, stub));

    coordinator::WatchRangeReqPb req;
    req.set_key(key);
    req.set_range_end(rangeEnd);
    req.set_watcher_addr(watcherAddr);
    coordinator::WatchRangeRspPb rsp;
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(stub->WatchRange(opts, req, rsp));
    RETURN_IF_NOT_OK(CheckLeader(rsp.header()));
    watchId = rsp.watch_id();
    FillKeyValueEntries(rsp.initial_kvs(), initialKvs);
    return Status::OK();
}

template <typename StubT>
Status CancelWatchImpl(const HostPort &coordinatorAddr, const std::string &watcherAddr,
                       const std::vector<int64_t> &watchIds, int32_t timeoutMs)
{
    std::shared_ptr<StubT> stub;
    RETURN_IF_NOT_OK(GetCoordinatorStub(coordinatorAddr, stub));

    coordinator::CancelWatchReqPb req;
    req.set_watcher_addr(watcherAddr);
    for (const auto &watchId : watchIds) {
        req.add_watch_ids(watchId);
    }
    coordinator::CancelWatchRspPb rsp;
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(stub->CancelWatch(opts, req, rsp));
    RETURN_IF_NOT_OK(CheckLeader(rsp.header()));
    return Status::OK();
}

template <typename StubT>
Status KeepAliveImpl(const HostPort &coordinatorAddr, const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs,
                     int32_t timeoutMs)
{
    std::shared_ptr<StubT> stub;
    RETURN_IF_NOT_OK(GetCoordinatorStub(coordinatorAddr, stub));

    coordinator::KeepAliveReqPb req;
    req.set_key(key);
    coordinator::KeepAliveRspPb rsp;
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(stub->KeepAlive(opts, req, rsp));
    RETURN_IF_NOT_OK(CheckLeader(rsp.header()));
    ttlMs = rsp.ttl();
    remainingTtlMs = rsp.remaining_ttl();
    return Status::OK();
}
}  // namespace

Status CoordinatorServiceProxyZmqImpl::Put(const std::string &key, const std::string &value, int64_t ttlMs,
                                           int64_t expectedVersion, int64_t &version, int64_t &revision,
                                           int32_t timeoutMs)
{
    return PutImpl<coordinator::CoordinatorService_Stub>(coordinatorAddr_, key, value, ttlMs, expectedVersion, version,
                                                         revision, timeoutMs);
}

Status CoordinatorServiceProxyZmqImpl::Range(const std::string &key, const std::string &rangeEnd,
                                             std::vector<KeyValueEntry> &kvs, int64_t &revision, int32_t timeoutMs)
{
    return RangeImpl<coordinator::CoordinatorService_Stub>(coordinatorAddr_, key, rangeEnd, kvs, revision, timeoutMs);
}

Status CoordinatorServiceProxyZmqImpl::DeleteRange(const std::string &key, const std::string &rangeEnd,
                                                   int64_t &deleted, int64_t &revision, int32_t timeoutMs)
{
    return DeleteRangeImpl<coordinator::CoordinatorService_Stub>(coordinatorAddr_, key, rangeEnd, deleted, revision,
                                                                 timeoutMs);
}

Status CoordinatorServiceProxyZmqImpl::WatchRange(const std::string &key, const std::string &rangeEnd,
                                                  const std::string &watcherAddr, int64_t &watchId,
                                                  std::vector<KeyValueEntry> &initialKvs, int32_t timeoutMs)
{
    return WatchRangeImpl<coordinator::CoordinatorService_Stub>(coordinatorAddr_, key, rangeEnd, watcherAddr, watchId,
                                                                initialKvs, timeoutMs);
}

Status CoordinatorServiceProxyZmqImpl::CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds,
                                                   int32_t timeoutMs)
{
    return CancelWatchImpl<coordinator::CoordinatorService_Stub>(coordinatorAddr_, watcherAddr, watchIds, timeoutMs);
}

Status CoordinatorServiceProxyZmqImpl::KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs,
                                                 int32_t timeoutMs)
{
    return KeepAliveImpl<coordinator::CoordinatorService_Stub>(coordinatorAddr_, key, ttlMs, remainingTtlMs, timeoutMs);
}

Status CoordinatorServiceProxyBrpcImpl::Put(const std::string &key, const std::string &value, int64_t ttlMs,
                                            int64_t expectedVersion, int64_t &version, int64_t &revision,
                                            int32_t timeoutMs)
{
    return PutImpl<coordinator::CoordinatorService_BrpcGenericStub>(coordinatorAddr_, key, value, ttlMs,
                                                                    expectedVersion, version, revision, timeoutMs);
}

Status CoordinatorServiceProxyBrpcImpl::Range(const std::string &key, const std::string &rangeEnd,
                                              std::vector<KeyValueEntry> &kvs, int64_t &revision, int32_t timeoutMs)
{
    return RangeImpl<coordinator::CoordinatorService_BrpcGenericStub>(coordinatorAddr_, key, rangeEnd, kvs, revision,
                                                                      timeoutMs);
}

Status CoordinatorServiceProxyBrpcImpl::DeleteRange(const std::string &key, const std::string &rangeEnd,
                                                    int64_t &deleted, int64_t &revision, int32_t timeoutMs)
{
    return DeleteRangeImpl<coordinator::CoordinatorService_BrpcGenericStub>(coordinatorAddr_, key, rangeEnd, deleted,
                                                                            revision, timeoutMs);
}

Status CoordinatorServiceProxyBrpcImpl::WatchRange(const std::string &key, const std::string &rangeEnd,
                                                   const std::string &watcherAddr, int64_t &watchId,
                                                   std::vector<KeyValueEntry> &initialKvs, int32_t timeoutMs)
{
    return WatchRangeImpl<coordinator::CoordinatorService_BrpcGenericStub>(coordinatorAddr_, key, rangeEnd, watcherAddr,
                                                                           watchId, initialKvs, timeoutMs);
}

Status CoordinatorServiceProxyBrpcImpl::CancelWatch(const std::string &watcherAddr,
                                                    const std::vector<int64_t> &watchIds, int32_t timeoutMs)
{
    return CancelWatchImpl<coordinator::CoordinatorService_BrpcGenericStub>(coordinatorAddr_, watcherAddr, watchIds,
                                                                            timeoutMs);
}

Status CoordinatorServiceProxyBrpcImpl::KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs,
                                                  int32_t timeoutMs)
{
    return KeepAliveImpl<coordinator::CoordinatorService_BrpcGenericStub>(coordinatorAddr_, key, ttlMs, remainingTtlMs,
                                                                          timeoutMs);
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
        RETURN_IF_NOT_OK(Range(key, "", kvs, rangeRevision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS));

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
            return Status::OK();
        }

        Status rc = Put(key, *newValue, 0, expectedVersion, version, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS);
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
