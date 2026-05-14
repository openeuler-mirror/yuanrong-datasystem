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

#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"

#include <mutex>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/zmq/zmq_stub_conn.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.stub.rpc.pb.h"
#include "datasystem/protos/master_stream.stub.rpc.pb.h"
#include "datasystem/protos/stream_posix.stub.rpc.pb.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"
#include "datasystem/protos/worker_stream.stub.rpc.pb.h"

DS_DEFINE_int32(oc_worker_worker_pool_size, 3, "Number of parallel connections between worker/worker. Default is 3.");
DS_DEFINE_int32(sc_worker_worker_pool_size, 3, "Number of parallel connections between worker/worker. Default is 3.");

namespace datasystem {
Status RpcStubCacheMgr::Init(uint64_t maxStubCount, const HostPort &localAddress)
{
    LOG(INFO) << FormatString("Init RpcStubCacheMgr for %s, max cache num: %d", localAddress.ToString(), maxStubCount);
    std::lock_guard<std::mutex> lck(initMutex_);

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
}

Status RpcStubCacheMgr::GetStub(const HostPort &hostPort, StubType type, std::shared_ptr<RpcStubBase> &rpcStub)
{
    static constexpr int64_t SLOW_GET_STUB_THRESHOLD_MS = 10;
    auto getSlowPhase = [](int64_t lookupElapsedMs, int64_t accessElapsedMs, int64_t createElapsedMs) {
        if (createElapsedMs >= lookupElapsedMs && createElapsedMs >= accessElapsedMs) {
            return "create";
        }
        if (accessElapsedMs >= lookupElapsedMs) {
            return "access";
        }
        return "lookup";
    };
    Timer timer;
    int64_t lookupElapsedMs = 0;
    int64_t accessElapsedMs = 0;
    int64_t createElapsedMs = 0;
    bool cacheHit = false;
    PerfPoint point(PerfKey::WORKER_RPC_STUB_CACHE_LOOKUP);
    std::shared_ptr<RpcStubCacheMgrObj> encapsulatedData = nullptr;
    if (lruCache_->Lookup(HashKeyForRpcStubCacheMgr(hostPort, type), &encapsulatedData).IsOk()) {
        lookupElapsedMs = static_cast<int64_t>(timer.ElapsedMilliSecondAndReset());
        cacheHit = true;
        rpcStub = encapsulatedData->GetData();
        if (rpcStub != nullptr) {
            auto totalElapsedMs = lookupElapsedMs + static_cast<int64_t>(timer.ElapsedMilliSecond());
            LOG_IF(INFO, totalElapsedMs > SLOW_GET_STUB_THRESHOLD_MS)
                << FormatString("[SLOW_RPC_STUB_GET] dst=%s type=%d hit=%d phase=%s total=%ldms trace=%s",
                                hostPort.ToString(), static_cast<int>(type), cacheHit,
                                getSlowPhase(lookupElapsedMs, accessElapsedMs, createElapsedMs), totalElapsedMs,
                                Trace::Instance().GetTraceID());
            return Status::OK();
        }
    } else {
        lookupElapsedMs = static_cast<int64_t>(timer.ElapsedMilliSecondAndReset());
    }
    point.RecordAndReset(PerfKey::WORKER_RPC_STUB_CACHE_FIND_CREATOR);
    LOG(INFO) << "Start to create stub, destAddr: " << hostPort.ToString() << ", type: " << static_cast<int>(type);
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
            auto totalElapsedMs = lookupElapsedMs + accessElapsedMs + createElapsedMs;
            LOG(INFO) << FormatString(
                "[RPC_STUB_GET_FAIL] dst=%s type=%d phase=%s total=%ldms retry=%d status=%s trace=%s",
                hostPort.ToString(), static_cast<int>(type),
                getSlowPhase(lookupElapsedMs, accessElapsedMs, createElapsedMs), totalElapsedMs, attempts,
                rc.ToString(), Trace::Instance().GetTraceID());
            LOG(ERROR) << "create rpc stub failed, rc: " << rc.ToString();
            LOG_IF_ERROR(Remove(hostPort, type), "remove rpc stub failed");
            return rc;
        }
        newEncapsulatedData->SetDataWithoutLck(rpcStub);
    }
    auto totalElapsedMs = lookupElapsedMs + accessElapsedMs + createElapsedMs;
    LOG_IF(INFO, totalElapsedMs > SLOW_GET_STUB_THRESHOLD_MS)
        << FormatString("[SLOW_RPC_STUB_GET] dst=%s type=%d hit=%d phase=%s total=%ldms retry=%d trace=%s",
                        hostPort.ToString(), static_cast<int>(type), cacheHit,
                        getSlowPhase(lookupElapsedMs, accessElapsedMs, createElapsedMs), totalElapsedMs, attempts,
                        Trace::Instance().GetTraceID());
    return Status::OK();
}

Status RpcStubCacheMgr::Remove(const HostPort &hostPort, StubType type)
{
    return lruCache_->Remove(HashKeyForRpcStubCacheMgr(hostPort, type));
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
