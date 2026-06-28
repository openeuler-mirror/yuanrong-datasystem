/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

/**
 * Description: RPC Server.
 */
#include "datasystem/common/rpc/rpc_server.h"

#include <chrono>
#include <future>
#include <thread>

#include <brpc/server.h>
// brpc headers above override LOG/VLOG/DLOG via butil/logging.h.
// Re-include log.h to restore datasystem's spdlog-based macros.
#include "datasystem/common/log/log.h"

#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/zmq/zmq_server_impl.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace {
constexpr int BRPC_SERVER_JOIN_TIMEOUT_MS = 10000;
}  // namespace

RpcServer::RpcServer(Token key, const RpcCredential &cred) : useBrpc_(false)
{
    (void)key;
    auto passkey = ZmqServerImpl::Token();
    pimpl_ = std::make_unique<ZmqServerImpl>(passkey, cred);
}
RpcServer::~RpcServer() noexcept
{
    if (useBrpc_ && brpcServer_) {
        try {
            StopBrpcServer();
        } catch (const std::exception &e) {
            // std::thread constructor may throw std::system_error under
            // resource exhaustion. Since the destructor is noexcept, an
            // uncaught exception would call std::terminate. Release the
            // server to avoid UAF; log the error for diagnostics.
            LOG(ERROR) << "StopBrpcServer() threw in ~RpcServer: " << e.what()
                       << "; intentionally leaking brpcServer_";
            (void)brpcServer_.release();
        } catch (...) {
            LOG(ERROR) << "StopBrpcServer() threw unknown exception in ~RpcServer; "
                       << "intentionally leaking brpcServer_";
            (void)brpcServer_.release();
        }
    }
}

Status RpcServer::Init()
{
    return std::visit([](auto &pimpl) { return pimpl->Init(); }, pimpl_);
}

Status RpcServer::Run()
{
    return std::visit([](auto &pimpl) { return pimpl->Run(); }, pimpl_);
}

void RpcServer::Shutdown()
{
    if (useBrpc_) {
        StopBrpcServer();
    }
    std::visit([](auto &pimpl) { pimpl->Shutdown(); }, pimpl_);
}

Status RpcServer::Bind(const std::string &endpoint)
{
    return std::visit([&endpoint](auto &pimpl) { return pimpl->Bind(endpoint); }, pimpl_);
}

Status RpcServer::RegisterService(ZmqService *svc, const RpcServiceCfg &cfg)
{
    return std::get<std::unique_ptr<ZmqServerImpl>>(pimpl_)->RegisterService(ZmqServerImpl::Token(), svc, cfg);
}

Status RpcServer::InitAuthHandler()
{
    return std::visit([](auto &pimpl) { return pimpl->InitAuthHandler(); }, pimpl_);
}

Status RpcServer::AddBrpcService(google::protobuf::Service *service)
{
    CHECK_FAIL_RETURN_STATUS(service != nullptr, StatusCode::K_INVALID, "Service is nullptr");
    if (!brpcServer_) {
        brpcServer_ = std::make_unique<brpc::Server>();
    }
    if (brpcServer_->AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to add brpc service");
    }
    return Status::OK();
}

Status RpcServer::StartBrpcServer(const std::string &addr, int port)
{
    if (!brpcServer_) {
        brpcServer_ = std::make_unique<brpc::Server>();
    }
    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    // ST workers run worker + master in the same process, so a brpc handler on
    // worker can make a nested brpc call to itself (as master). With the default
    // num_threads (#cpu-cores, often small on test boxes), the small bthread
    // worker pool can be exhausted by concurrent nested RPCs -> Get RPCs queue
    // but never dispatch. Bump num_threads so handlers always find a free worker.
    options.num_threads = FLAGS_brpc_server_num_threads;
    butil::EndPoint ep;
    if (!addr.empty()) {
        butil::str2ip(addr.c_str(), &ep.ip);
    }
    ep.port = port;
    if (brpcServer_->Start(ep, &options) != 0) {
        // Start failed — destroy the server so AddBrpcService + Start can be
        // retried cleanly. Without this, the partially-initialized server would
        // reject duplicate service registrations on the next attempt.
        brpcServer_.reset();
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
            FormatString("Failed to start brpc server on %s:%d", addr.c_str(), port));
    }
    LOG(INFO) << "brpc server started on " << addr << ":" << port;
    return Status::OK();
}

void RpcServer::StopBrpcServer()
{
    if (brpcServer_) {
        // Stop() is async — marks server closing, sends ELOGOFF to clients,
        // rejects new requests. The closewait_ms parameter is "not used anymore"
        // in brpc 1.15 (see brpc/server.h:470), so pass 0 explicitly.
        brpcServer_->Stop(0);

        // brpc 1.15 has no Join(timeout) variant. Run Join() on a detached
        // thread; the calling thread waits bounded time. Capture the raw
        // pointer to avoid touching brpcServer_ from the detached thread
        // after the main thread potentially releases ownership (race-safe).
        brpc::Server* raw = brpcServer_.get();
        std::promise<void> joinDone;
        std::future<void> doneFuture = joinDone.get_future();
        std::thread([raw, p = std::move(joinDone)]() mutable {
            raw->Join();
            p.set_value();
        }).detach();

        // Bounded wait: 10s timeout. On timeout we DON'T crash — log error
        // and intentionally leak brpcServer_ to avoid UAF with the detached
        // thread. Process keeps running for graceful local cleanup (data
        // flush, log drain, other module shutdown); the detached Join
        // thread continues in background.
        if (doneFuture.wait_for(std::chrono::milliseconds(BRPC_SERVER_JOIN_TIMEOUT_MS))
            == std::future_status::timeout) {
            LOG(ERROR) << "brpc Server::Join() did not complete within "
                       << BRPC_SERVER_JOIN_TIMEOUT_MS
                       << "ms; likely OOM or stuck bthread. Intentionally "
                       << "leaking brpcServer_; detached Join thread continues "
                       << "in background. Manual investigation needed.";
            (void)brpcServer_.release();  // Give up ownership; raw pointer leaks
        } else {
            brpcServer_.reset();  // Join completed — safe to release normally
        }
    }
}

void RpcServer::Interrupt()
{
    std::visit([](auto &pimpl) { pimpl->Interrupt(); }, pimpl_);
}

bool RpcServer::IsInterrupted() const
{
    return std::visit([](auto &pimpl) { return pimpl->IsInterrupted(); }, pimpl_);
}

std::vector<std::string> RpcServer::GetListeningPorts() const
{
    return std::visit([](auto &pimpl) { return pimpl->GetListeningPorts(); }, pimpl_);
}

ThreadPool::ThreadPoolUsage RpcServer::GetRpcServicesUsage(const std::string &serviceName) const
{
    return std::visit([&serviceName](auto &pimpl) { return pimpl->GetRpcServicesUsage(serviceName); }, pimpl_);
}

ThreadPool::ThreadPoolUsage RpcServer::GetRpcServicesSnapshot(const std::string &serviceName) const
{
    return std::visit([&serviceName](auto &pimpl) { return pimpl->GetRpcServicesSnapshot(serviceName); }, pimpl_);
}

Status RpcServer::Builder::Init(std::unique_ptr<RpcServer> &server) const
{
    auto key = Token();
    server = std::make_unique<RpcServer>(key, cred_);
    // In brpc mode, also init ZMQ for client registration/UDS/SHM.
    // brpc handles RPC calls; ZMQ handles client management.
    RETURN_IF_NOT_OK(server->Init());
    if (RpcAuthKeyManager::Instance().HasAuthHandler()) {
        RETURN_IF_NOT_OK(server->InitAuthHandler());
    }
    // brpc owns the TCP port in brpc mode (kBrpcPortOffset=0); ZMQ must skip Bind.
    // Mirrors the !useBrpc_ guard on Run() in BuildAndStart. UDS/SHM use Init(), not this loop.
    if (!useBrpc_) {
        for (const auto &v : endPts_) {
            RETURN_IF_NOT_OK(server->Bind(v));
        }
    }
    if (useBrpc_) {
        server->useBrpc_ = true;
    }
    return Status::OK();
}

Status RpcServer::Builder::BuildAndStart(std::unique_ptr<RpcServer> &server) const
{
    try {
        // Register ZMQ services and start ZMQ server if any services were registered to ZMQ.
        // In brpc mode, all RPC services go to brpc and svcList_ may be empty.
        for (auto &ele : svcList_) {
            auto &svcEle = ele.second;
            auto func = [&server, &svcEle](auto *svc) {
                RETURN_RUNTIME_ERROR_IF_NULL(svc);
                RETURN_IF_NOT_OK(server->RegisterService(svc, svcEle));
                return Status::OK();
            };
            RETURN_IF_NOT_OK(std::visit([&func](auto *svc) { return func(svc); }, ele.first));
        }
        if (preStartCallback_) {
            RETURN_IF_NOT_OK(preStartCallback_());
        }
        // Start ZMQ server only in ZMQ mode. In brpc mode (useBrpc_=true), the port is
        // used exclusively by brpc (kBrpcPortOffset=0), so ZMQ must not bind the same port.
        if (!useBrpc_ && !svcList_.empty()) {
            RETURN_IF_NOT_OK(server->Run());
        }
        // IMPORTANT: brpc server start is NOT done here. BuildAndStart() is
        // called from CommonServer::Init() which runs before CreateAllServices().
        // Brpc services (adapters) are registered later via AddBrpcService(),
        // so starting brpc here would start with 0 services and fail.
        // The caller MUST invoke server->StartBrpcServer(brpcAddr_, brpcPort_)
        // after registering all brpc services (see WorkerOCServer::Init()).
    } catch (const std::bad_alloc &e) {
        RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY, e.what());
    }
    return Status::OK();
}
}  // namespace datasystem
