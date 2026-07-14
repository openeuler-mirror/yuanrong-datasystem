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

#include <brpc/server.h>
// brpc headers above override LOG/VLOG/DLOG via butil/logging.h.
// Re-include log.h to restore datasystem's spdlog-based macros.
#include "datasystem/common/log/log.h"

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/zmq/zmq_server_impl.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {

RpcServer::RpcServer(Token key, const RpcCredential &cred) : useBrpc_(false)
{
    (void)key;
    auto passkey = ZmqServerImpl::Token();
    pimpl_ = std::make_unique<ZmqServerImpl>(passkey, cred);
}
RpcServer::~RpcServer() noexcept
{
    if (useBrpc_) {
        StopBrpcServer();
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
    // Builtin HTTP services (/flags, /pprof, /vars) are off by default to match the
    // ZMQ security baseline; set FLAGS_brpc_enable_builtin_services=true to debug.
    options.has_builtin_services = FLAGS_brpc_enable_builtin_services;
    // ST workers run worker + master in the same process, so a brpc handler on
    // worker can make a nested brpc call to itself (as master). With the default
    // num_threads (#cpu-cores, often small on test boxes), the small bthread
    // worker pool can be exhausted by concurrent nested RPCs -> Get RPCs queue
    // but never dispatch. Bump num_threads so handlers always find a free worker.
    options.num_threads = FLAGS_brpc_server_num_threads;
    // Bound concurrent in-flight RPCs so a slow handler (e.g. Publish large object
    // synchronously calling master RPC) cannot queue unlimited bthreads -> OOM ->
    // systemd restart. The flag defaults to 128 (num_threads * 2); set it to 0 to
    // disable the limit (brpc treats 0 as unlimited). When exceeded, brpc returns
    // ELIMIT to the client immediately, which the caller can retry on another worker.
    options.max_concurrency = FLAGS_brpc_max_concurrency;
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
    std::lock_guard<std::mutex> lock(brpcStopMtx_);
    if (brpcServer_) {
        // Synchronous Stop+Join+reset. brpc::Server::~Server() also calls
        // Stop+Join, so doing it synchronously first makes the destructor's
        // calls no-ops (status=READY), avoiding a race with ~Server().
        brpcServer_->Stop(0);
        brpcServer_->Join();
        brpcServer_.reset();
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
