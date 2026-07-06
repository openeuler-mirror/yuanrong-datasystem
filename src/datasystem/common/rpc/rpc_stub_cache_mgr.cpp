/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"

#include <mutex>

#include <brpc/channel.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/zmq/zmq_stub_conn.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/coordinator.stub.rpc.pb.h"
#include "datasystem/protos/master_object.stub.rpc.pb.h"
#include "datasystem/protos/master_stream.stub.rpc.pb.h"
#include "datasystem/protos/stream_posix.stub.rpc.pb.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"
#include "datasystem/protos/worker_stream.stub.rpc.pb.h"

// brpc stub headers
#include <brpc/socket.h>
#include <brpc/socket_map.h>
#include "datasystem/protos/coordinator.brpc.stub.pb.h"
#include "datasystem/protos/master_object.brpc.stub.pb.h"
#include "datasystem/protos/master_stream.brpc.stub.pb.h"
#include "datasystem/protos/stream_posix.brpc.stub.pb.h"
#include "datasystem/protos/worker_object.brpc.stub.pb.h"
#include "datasystem/protos/worker_stream.brpc.stub.pb.h"
// brpc headers above override LOG/VLOG/DLOG via butil/logging.h.
// Re-include log.h to restore datasystem's spdlog-based macros.
#include "datasystem/common/log/log.h"

DS_DEFINE_int32(oc_worker_worker_pool_size, 3, "Number of parallel connections between worker/worker. Default is 3.");
DS_DEFINE_int32(sc_worker_worker_pool_size, 3, "Number of parallel connections between worker/worker. Default is 3.");

namespace datasystem {
namespace {
constexpr int64_t SLOW_RPC_STUB_THRESHOLD_MS = 2;

const char *GetSlowPhase(int64_t lookupElapsedMs, int64_t accessElapsedMs, int64_t createElapsedMs)
{
    if (createElapsedMs >= lookupElapsedMs && createElapsedMs >= accessElapsedMs) {
        return "create";
    }
    if (accessElapsedMs >= lookupElapsedMs) {
        return "access";
    }
    return "lookup";
}

void LogStubGetEvent(const char *eventName, const HostPort &hostPort, StubType type, bool cacheHit,
                     int64_t lookupElapsedMs, int64_t getDataElapsedMs, int64_t accessElapsedMs,
                     int64_t createElapsedMs, int attempts, const Status *rc = nullptr)
{
    const auto totalElapsedMs = lookupElapsedMs + getDataElapsedMs + accessElapsedMs + createElapsedMs;
    const auto phase = GetSlowPhase(lookupElapsedMs, accessElapsedMs, createElapsedMs);
    if (rc == nullptr) {
        LOG_IF(INFO, totalElapsedMs > SLOW_RPC_STUB_THRESHOLD_MS) << FormatString(
            "[%s] dst=%s type=%d hit=%d phase=%s lookup=%ldms access=%ldms create=%ldms "
            "total=%ldms retry=%d trace=%s",
            eventName, hostPort.ToString(), static_cast<int>(type), cacheHit, phase, lookupElapsedMs, accessElapsedMs,
            createElapsedMs, totalElapsedMs, attempts, Trace::Instance().GetTraceID());
        return;
    }
    if (totalElapsedMs > SLOW_RPC_STUB_THRESHOLD_MS) {
        LOG(ERROR) << FormatString("[%s] dst=%s type=%d phase=%s total=%ldms retry=%d error=%s trace=%s", eventName,
                                   hostPort.ToString(), static_cast<int>(type), phase, totalElapsedMs, attempts,
                                   rc->ToString(), Trace::Instance().GetTraceID());
        return;
    }
    LOG(ERROR) << FormatString("[%s] dst=%s type=%d retry=%d error=%s trace=%s", eventName, hostPort.ToString(),
                               static_cast<int>(type), attempts, rc->ToString(), Trace::Instance().GetTraceID());
}
}  // namespace

Status RpcStubCacheMgr::Init(uint64_t maxStubCount, const HostPort &localAddress)
{
    LOG(INFO) << FormatString("Init RpcStubCacheMgr for %s, max cache num: %d", localAddress.ToString(), maxStubCount);
    std::lock_guard<std::mutex> lck(initMutex_);
    if (init_) {
        return Status::OK();
    }

    // Pre-warm ZmqStubConnMgr singleton to avoid initialization delay on first use
    (void)ZmqStubConnMgr::Instance();

    auto policy = std::make_unique<LruCountPolicy>();
    policy->SetCacheCount(maxStubCount);
    RETURN_IF_NOT_OK(LruForRpcStubCacheMgr::Builder()
                         .SetPolicy(std::move(policy))
                         .SetNumPartitions(stubPriorityNum_)
                         .Build(&lruCache_));
    localAddress_ = localAddress;
    InitCreators();
    init_ = true;
    return Status::OK();
}

Status RpcStubCacheMgr::CreateRpcStub(StubType type, const std::shared_ptr<RpcChannel> &channel,
                                      std::shared_ptr<RpcStubBase> &stub)
{
    switch (type) {
        case StubType::WORKER_WORKER_OC_SVC:
            stub = std::make_shared<WorkerWorkerOCService_Stub>(channel);
            break;
        case StubType::WORKER_MASTER_OC_SVC:
            stub = std::make_shared<master::MasterOCService_Stub>(channel, FLAGS_node_timeout_s * TO_MILLISECOND);
            break;
        case StubType::WORKER_WORKER_SC_SVC:
            stub = std::make_shared<WorkerWorkerSCService_Stub>(channel);
            break;
        case StubType::WORKER_MASTER_SC_SVC:
            stub = std::make_shared<master::MasterSCService_Stub>(channel);
            break;
        case StubType::MASTER_WORKER_OC_SVC:
            stub = std::make_shared<MasterWorkerOCService_Stub>(channel);
            break;
        case StubType::MASTER_WORKER_SC_SVC:
            stub = std::make_shared<MasterWorkerSCService_Stub>(channel);
            break;
        case StubType::MASTER_MASTER_OC_SVC:
            stub = std::make_shared<master::MasterOCService_Stub>(channel);
            break;
        case StubType::WORKER_WORKER_TRANS_SVC:
            stub = std::make_shared<WorkerWorkerTransportService_Stub>(channel);
            break;
        case StubType::TO_COORDINATOR_SVC:
            stub = std::make_shared<coordinator::CoordinatorService_Stub>(channel);
            break;
        case StubType::COORDINATOR_WORKER_SVC:
            stub = std::make_shared<coordinator::CoordinatorWatchService_Stub>(channel);
            break;
        default:
            RETURN_STATUS(K_RUNTIME_ERROR, "Unsupport type: " + std::to_string(static_cast<int>(type)));
    }
    return stub->GetInitStatus();
}

Status RpcStubCacheMgr::CreateBrpcStub(StubType type, const std::shared_ptr<brpc::Channel> &brpcChannel,
                                       std::shared_ptr<RpcStubBase> &stub)
{
    int32_t timeoutMs = FLAGS_node_timeout_s * TO_MILLISECOND;
    switch (type) {
        case StubType::WORKER_WORKER_OC_SVC:
            stub = std::make_shared<WorkerWorkerOCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::WORKER_MASTER_OC_SVC:
            stub = std::make_shared<master::MasterOCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::WORKER_WORKER_SC_SVC:
            stub = std::make_shared<WorkerWorkerSCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::WORKER_MASTER_SC_SVC:
            stub = std::make_shared<master::MasterSCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::MASTER_WORKER_OC_SVC:
            stub = std::make_shared<MasterWorkerOCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::MASTER_WORKER_SC_SVC:
            stub = std::make_shared<MasterWorkerSCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::MASTER_MASTER_OC_SVC:
            stub = std::make_shared<master::MasterOCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::WORKER_WORKER_TRANS_SVC:
            stub = std::make_shared<WorkerWorkerTransportService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::TO_COORDINATOR_SVC:
            stub = std::make_shared<coordinator::CoordinatorService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::COORDINATOR_WORKER_SVC:
            stub = std::make_shared<coordinator::CoordinatorWatchService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        default:
            RETURN_STATUS(K_RUNTIME_ERROR, "Unsupport type: " + std::to_string(static_cast<int>(type)));
    }
    return stub->GetInitStatus();
}

Status RpcStubCacheMgr::CreateRpcChannel(const HostPort &hostPort, const std::string &serviceName,
                                         std::shared_ptr<RpcChannel> &channel, size_t poolSize)
{
    CHECK_FAIL_RETURN_STATUS(channel == nullptr, K_RUNTIME_ERROR, "channel is not nullptr");
    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::CreateCredentials(WORKER_SERVER_NAME, cred));
    channel = std::make_shared<RpcChannel>(hostPort, cred);
    RETURN_RUNTIME_ERROR_IF_NULL(channel);
    if (!serviceName.empty()) {
        channel->SetServiceTcpDirect(serviceName);
    }
    if (poolSize > 0) {
        channel->SetServiceConnectPoolSize(serviceName, poolSize);
    }
    return Status::OK();
}

bool WaitForBrpcSocketAvailable(const HostPort &brpcAddr, int maxRetries, int intervalUs)
{
    butil::EndPoint ep;
    if (butil::str2endpoint(brpcAddr.Host().c_str(), brpcAddr.Port(), &ep) != 0) {
        return false;
    }
    for (int i = 0; i < maxRetries; ++i) {
        brpc::SocketId sid;
        if (brpc::SocketMapFind(brpc::SocketMapKey(ep), &sid) == 0) {
            brpc::SocketUniquePtr ptr;
            if (brpc::Socket::Address(sid, &ptr) == 0 && ptr->IsAvailable()) {
                return true;
            }
        }
        usleep(intervalUs);
    }
    return false;
}

Status RpcStubCacheMgr::CreateBrpcChannel(const HostPort &hostPort, std::shared_ptr<brpc::Channel> &brpcChannel)
{
    CHECK_FAIL_RETURN_STATUS(brpcChannel == nullptr, K_RUNTIME_ERROR, "brpc channel is not nullptr");
    HostPort brpcAddr(hostPort.Host(), hostPort.Port() + kBrpcPortOffset);
    BrpcChannelConfig cfg;
    cfg.endpoint = brpcAddr.ToString();
    cfg.connect_timeout_ms = FLAGS_node_timeout_s * TO_MILLISECOND;
    cfg.timeout_ms = FLAGS_node_timeout_s * TO_MILLISECOND;
    // These channels carry worker<->worker / worker<->master mesh traffic
    // (WORKER_WORKER_OC_SVC, WORKER_MASTER_OC_SVC, etc.). The circuit breaker
    // is designed for client->server fan-out where isolating a bad endpoint is
    // a net win. In a dense worker mesh, a healthy peer under momentary back-
    // pressure (eviction/rebalance) can trip the breaker and get isolated,
    // amplifying the failure. Disable it on internal mesh channels; client
    // paths (client_worker_*) keep the default-on breaker.
    cfg.enable_circuit_breaker = false;
    auto channel = BrpcChannelFactory::Create(cfg);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(channel != nullptr, K_RPC_UNAVAILABLE,
                                         FormatString("Failed to init brpc channel to %s", brpcAddr.ToString()));
    brpcChannel = std::move(channel);
    return Status::OK();
}

bool RpcStubCacheMgr::EnableOcWorkerWorkerDirectPort()
{
    INJECT_POINT("RpcStubCacheMgr.EnableOcWorkerWorkerDirectPort", []() { return true; });
    return FLAGS_oc_worker_worker_direct_port > 0;
}

bool RpcStubCacheMgr::EnableScWorkerWorkerDirectPort()
{
    return FLAGS_sc_worker_worker_direct_port > 0;
}

void RpcStubCacheMgr::InitCreators()
{
    if (FLAGS_use_brpc) {
        // brpc path: each creator makes a brpc::Channel and a _BrpcGenericStub
        auto makeBrpcCreator = [](StubType stubType) -> RpcStubCacheCreateFunc {
            return [stubType](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
                return BrpcCreatorTemplate(
                    [&hostPort](std::shared_ptr<brpc::Channel> &brpcChannel) {
                        return CreateBrpcChannel(hostPort, brpcChannel);
                    },
                    stubType, rpcStub);
            };
        };
        creators_.emplace(StubType::WORKER_WORKER_OC_SVC, makeBrpcCreator(StubType::WORKER_WORKER_OC_SVC));
        creators_.emplace(StubType::WORKER_MASTER_OC_SVC, makeBrpcCreator(StubType::WORKER_MASTER_OC_SVC));
        creators_.emplace(StubType::WORKER_WORKER_SC_SVC, makeBrpcCreator(StubType::WORKER_WORKER_SC_SVC));
        creators_.emplace(StubType::WORKER_MASTER_SC_SVC, makeBrpcCreator(StubType::WORKER_MASTER_SC_SVC));
        creators_.emplace(StubType::MASTER_WORKER_OC_SVC, makeBrpcCreator(StubType::MASTER_WORKER_OC_SVC));
        creators_.emplace(StubType::MASTER_WORKER_SC_SVC, makeBrpcCreator(StubType::MASTER_WORKER_SC_SVC));
        creators_.emplace(StubType::MASTER_MASTER_OC_SVC, makeBrpcCreator(StubType::MASTER_MASTER_OC_SVC));
        creators_.emplace(StubType::WORKER_WORKER_TRANS_SVC, makeBrpcCreator(StubType::WORKER_WORKER_TRANS_SVC));
        creators_.emplace(StubType::TO_COORDINATOR_SVC, makeBrpcCreator(StubType::TO_COORDINATOR_SVC));
        creators_.emplace(StubType::COORDINATOR_WORKER_SVC, makeBrpcCreator(StubType::COORDINATOR_WORKER_SVC));
        return;
    }

    // ZMQ path (original)
    creators_.emplace(
        StubType::WORKER_WORKER_OC_SVC, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
            return CreatorTemplate(
                [&hostPort](std::shared_ptr<RpcChannel> &channel) {
                    RETURN_IF_NOT_OK(CreateRpcChannel(
                        hostPort, EnableOcWorkerWorkerDirectPort() ? WorkerWorkerOCService_Stub::FullServiceName() : "",
                        channel, FLAGS_oc_worker_worker_pool_size));
                    return Status::OK();
                },
                StubType::WORKER_WORKER_OC_SVC, rpcStub);
        });
    creators_.emplace(
        StubType::WORKER_MASTER_OC_SVC, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
            return CreatorTemplate(
                [&hostPort](std::shared_ptr<RpcChannel> &channel) { return CreateRpcChannel(hostPort, "", channel); },
                StubType::WORKER_MASTER_OC_SVC, rpcStub);
        });
    creators_.emplace(
        StubType::WORKER_WORKER_SC_SVC, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
            return CreatorTemplate(
                [&hostPort](std::shared_ptr<RpcChannel> &channel) {
                    return CreateRpcChannel(
                        hostPort, EnableScWorkerWorkerDirectPort() ? WorkerWorkerSCService_Stub::FullServiceName() : "",
                        channel, FLAGS_sc_worker_worker_pool_size);
                },
                StubType::WORKER_WORKER_SC_SVC, rpcStub);
        });
    creators_.emplace(
        StubType::WORKER_MASTER_SC_SVC, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
            return CreatorTemplate(
                [&hostPort](std::shared_ptr<RpcChannel> &channel) { return CreateRpcChannel(hostPort, "", channel); },
                StubType::WORKER_MASTER_SC_SVC, rpcStub);
        });
    creators_.emplace(
        StubType::MASTER_WORKER_OC_SVC, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
            return CreatorTemplate(
                [&hostPort](std::shared_ptr<RpcChannel> &channel) { return CreateRpcChannel(hostPort, "", channel); },
                StubType::MASTER_WORKER_OC_SVC, rpcStub);
        });
    creators_.emplace(
        StubType::MASTER_WORKER_SC_SVC, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
            return CreatorTemplate(
                [&hostPort](std::shared_ptr<RpcChannel> &channel) { return CreateRpcChannel(hostPort, "", channel); },
                StubType::MASTER_WORKER_SC_SVC, rpcStub);
        });
    creators_.emplace(
        StubType::MASTER_MASTER_OC_SVC, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
            return CreatorTemplate(
                [&hostPort](std::shared_ptr<RpcChannel> &channel) { return CreateRpcChannel(hostPort, "", channel); },
                StubType::MASTER_MASTER_OC_SVC, rpcStub);
        });
    creators_.emplace(
        StubType::WORKER_WORKER_TRANS_SVC, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
            return CreatorTemplate(
                [&hostPort](std::shared_ptr<RpcChannel> &channel) { return CreateRpcChannel(hostPort, "", channel); },
                StubType::WORKER_WORKER_TRANS_SVC, rpcStub);
        });
    creators_.emplace(
        StubType::TO_COORDINATOR_SVC, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
            return CreatorTemplate(
                [&hostPort](std::shared_ptr<RpcChannel> &channel) { return CreateRpcChannel(hostPort, "", channel); },
                StubType::TO_COORDINATOR_SVC, rpcStub);
        });
    creators_.emplace(
        StubType::COORDINATOR_WORKER_SVC, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) {
            return CreatorTemplate(
                [&hostPort](std::shared_ptr<RpcChannel> &channel) { return CreateRpcChannel(hostPort, "", channel); },
                StubType::COORDINATOR_WORKER_SVC, rpcStub);
        });
}

void RpcStubCacheMgr::MaybeEvictStaleBrpcStub(const HostPort &hostPort, StubType type,
                                              std::shared_ptr<RpcStubBase> &rpcStub)
{
    HostPort brpcAddr(hostPort.Host(), hostPort.Port() + kBrpcPortOffset);
    // Single non-blocking check: IsAvailable() costs <1us. If the socket is
    // dead, evict the stub so the next access creates a fresh one. No retries —
    // blocking 3s on the hot path would stall all concurrent RPCs after worker
    // restart. If this check misses a transient state, the RPC will fail with
    // E112 and the error path will evict the stub then.
    //
    // F09 note: WaitForBrpcSocketAvailable(addr, maxRetries=1, intervalUs=0)
    // is the fast-path variant — single SocketMap::find + IsAvailable, no
    // sleeping. brpc SocketMap read lock is sharded, so under 4w QPS the
    // contention is bounded. If profiling shows this still hot, move the
    // check off the cache-hit path entirely and rely on E112 retry alone.
    if (!WaitForBrpcSocketAvailable(brpcAddr, 1, 0)) {
        LOG(WARNING) << FormatString("Stale brpc stub evicted for %s type=%d, reconnecting", hostPort.ToString(),
                                     static_cast<int>(type));
        (void)Remove(hostPort, type);
        rpcStub.reset();
    }
}

Status RpcStubCacheMgr::GetStub(const HostPort &hostPort, StubType type, std::shared_ptr<RpcStubBase> &rpcStub)
{
    Timer timer;
    int64_t lookupElapsedMs = 0;
    int64_t getDataElapsedMs = 0;
    int64_t accessElapsedMs = 0;
    int64_t createElapsedMs = 0;
    bool cacheHit = false;
    PerfPoint point(PerfKey::WORKER_RPC_STUB_CACHE_LOOKUP);
    std::shared_ptr<RpcStubCacheMgrObj> encapsulatedData = nullptr;
    if (lruCache_->Lookup(HashKeyForRpcStubCacheMgr(hostPort, type), &encapsulatedData).IsOk()) {
        lookupElapsedMs = static_cast<int64_t>(timer.ElapsedMilliSecondAndReset());
        cacheHit = true;
        rpcStub = encapsulatedData->GetData();
        getDataElapsedMs = static_cast<int64_t>(timer.ElapsedMilliSecondAndReset());
        if (rpcStub != nullptr) {
            // For brpc mode, verify the cached channel's socket is still alive.
            // After worker restart, the cached Channel may hold a dead Socket
            // while the cache entry still exists, causing E112 "Not connected".
            if (FLAGS_use_brpc) {
                MaybeEvictStaleBrpcStub(hostPort, type, rpcStub);
            }
            if (rpcStub != nullptr) {
                LogStubGetEvent("SLOW_RPC_STUB_GET", hostPort, type, cacheHit, lookupElapsedMs, getDataElapsedMs,
                                accessElapsedMs, createElapsedMs, 0);
                return Status::OK();
            }
        }
    } else {
        lookupElapsedMs = static_cast<int64_t>(timer.ElapsedMilliSecondAndReset());
    }
    point.RecordAndReset(PerfKey::WORKER_RPC_STUB_CACHE_FIND_CREATOR);
    auto creator = creators_.find(type);
    if (creator == creators_.end() || creator->second == nullptr) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Unsupported type: " + std::to_string(static_cast<int>(type)));
    }
    auto newEncapsulatedData = std::make_shared<RpcStubCacheMgrObj>(hostPort, type);
    Status rc;
    const int maxRetries = 5;
    const int retryIntervalMs = 100;
    int attempts = 0;
    {
        point.RecordAndReset(PerfKey::WORKER_RPC_STUB_CACHE_ACCESS);
        newEncapsulatedData->GetWriteLck();
        Raii raii([&newEncapsulatedData]() { newEncapsulatedData->ReleaseWriteLck(); });
        do {
            rc = lruCache_->Access(HashKeyForRpcStubCacheMgr(hostPort, type), newEncapsulatedData);
            if (rc.GetCode() == K_TRY_AGAIN) {
                attempts++;
                if (attempts < maxRetries) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(retryIntervalMs));
                } else {
                    RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Get error after retry: " + rc.GetMsg());
                }
            }
        } while (rc.GetCode() == K_TRY_AGAIN);
        accessElapsedMs = static_cast<int64_t>(timer.ElapsedMilliSecondAndReset());
        RETURN_IF_NOT_OK(rc);
        point.RecordAndReset(PerfKey::WORKER_RPC_STUB_CACHE_CONNECT);
        rc = creator->second(hostPort, rpcStub);
        createElapsedMs = static_cast<int64_t>(timer.ElapsedMilliSecond());
        point.Record();
        if (rc.IsError()) {
            LogStubGetEvent("RPC_STUB_GET_FAIL", hostPort, type, cacheHit, lookupElapsedMs, getDataElapsedMs,
                            accessElapsedMs, createElapsedMs, attempts, &rc);
            LOG_IF_ERROR(Remove(hostPort, type), "remove rpc stub failed");
            return rc;
        }
        newEncapsulatedData->SetDataWithoutLck(rpcStub);
    }
    // For brpc: wait for socket health check OUTSIDE the lock so we don't block
    // concurrent GetStub calls on the same LRU partition for up to 3s.
    // Channel::Init() is non-blocking -- health check runs periodically in a
    // background thread.  Waiting here gives the caller a ready-to-use channel
    // without serializing cache access behind the TCP handshake.
    if (FLAGS_use_brpc) {
        HostPort brpcAddr(hostPort.Host(), hostPort.Port() + kBrpcPortOffset);
        (void)WaitForBrpcSocketAvailable(brpcAddr);
    }
    LogStubGetEvent("SLOW_RPC_STUB_GET", hostPort, type, cacheHit, lookupElapsedMs, getDataElapsedMs, accessElapsedMs,
                    createElapsedMs, attempts);
    return Status::OK();
}

Status RpcStubCacheMgr::Remove(const HostPort &hostPort, StubType type)
{
    Timer timer;
    Status rc = lruCache_->Remove(HashKeyForRpcStubCacheMgr(hostPort, type));
    auto totalElapsedMs = static_cast<int64_t>(timer.ElapsedMilliSecond());
    LOG_IF(INFO,
           totalElapsedMs > SLOW_RPC_STUB_THRESHOLD_MS || (rc.IsError() && rc.GetCode() != StatusCode::K_NOT_FOUND))
        << FormatString("[SLOW_RPC_STUB_REMOVE] dst=%s type=%d total=%ldms error=%s trace=%s", hostPort.ToString(),
                        static_cast<int>(type), totalElapsedMs, rc.ToString(), Trace::Instance().GetTraceID());
    return rc;
}

namespace stub_priority {
StubPriority GetStubPriority(StubType type)
{
    switch (type) {
        case StubType::MASTER_WORKER_OC_SVC:
            return StubPriority::LOW;
        case StubType::WORKER_WORKER_TRANS_SVC:
        case StubType::WORKER_WORKER_OC_SVC:
        case StubType::WORKER_MASTER_OC_SVC:
        case StubType::WORKER_WORKER_SC_SVC:
        case StubType::WORKER_MASTER_SC_SVC:
        case StubType::MASTER_WORKER_SC_SVC:
        case StubType::MASTER_MASTER_OC_SVC:
        case StubType::TO_COORDINATOR_SVC:
        case StubType::COORDINATOR_WORKER_SVC:
            return StubPriority::HIGH;
#ifdef WITH_TESTS
        case StubType::TEST_TYPE_1:
            return StubPriority::LOW;
        case StubType::TEST_TYPE_2:
        case StubType::TEST_TYPE_3:
            return StubPriority::HIGH;
#endif
    }
    return StubPriority::INVALID;
}
}  // namespace stub_priority
}  // namespace datasystem
