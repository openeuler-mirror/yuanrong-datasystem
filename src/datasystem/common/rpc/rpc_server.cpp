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
#include <brpc/protocol.h>  // for brpc::FLAGS_max_body_size (DECLARE_uint64)
#include <butil/logging.h>
// brpc only DEFINEs max_connection_pool_size in socket.cpp (inside namespace
// brpc, no header DECLARE), so declare it here to override the global cap.
namespace brpc {
DECLARE_int32(max_connection_pool_size);
}
// brpc headers above override LOG/VLOG/DLOG via butil/logging.h.
// Re-include log.h to restore datasystem's spdlog-based macros.
#include "datasystem/common/log/log.h"

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/rpc/brpc_expired_request_interceptor.h"
#include "datasystem/common/rpc/rpc_service_base.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {

RpcServer::RpcServer(Token key) : useBrpc_(false)
{
    (void)key;
}
RpcServer::~RpcServer() noexcept
{
    if (useBrpc_) {
        StopBrpcServer();
    }
}

Status RpcServer::Init()
{
    return Status::OK();
}

void RpcServer::Shutdown()
{
    if (useBrpc_) {
        StopBrpcServer();
    }
}

void RpcServer::Interrupt()
{
}

bool RpcServer::IsInterrupted() const
{
    return false;
}

Status RpcServer::RegisterService(RpcServiceBase *svc, const RpcServiceCfg &cfg)
{
    RETURN_RUNTIME_ERROR_IF_NULL(svc);
    RETURN_IF_NOT_OK(svc->Init(cfg));
    auto it = svcMap_.emplace(svc->ServiceName(), svc);
    if (!it.second) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Service already registered. Not replacing");
    }
    return Status::OK();
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

Status RpcServer::AddBrpcServices(const std::function<Status(brpc::Server &)> &registrar)
{
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(registrar), StatusCode::K_INVALID, "Registrar is empty");
    if (!brpcServer_) {
        brpcServer_ = std::make_unique<brpc::Server>();
    }
    return registrar(*brpcServer_);
}

Status RpcServer::StartBrpcServer(const std::string &addr, int port)
{
    if (!brpcServer_) {
        brpcServer_ = std::make_unique<brpc::Server>();
    }
    logging::LoggingSettings settings;
    settings.logging_dest = logging::LOG_TO_NONE;
    settings.log_file = "";
    settings.lock_log = logging::DONT_LOCK_LOG_FILE;
    settings.delete_old = logging::APPEND_TO_OLD_LOG_FILE;
    logging::InitLogging(settings);

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
    // Raise brpc's per-message body limit above the 64MB default so large
    // object payloads (e.g. a 300MB cross-node Get pull) are not rejected by
    // input_messenger with "too big data" -> connection close -> Host is down.
    // Fixed at 2GB: this brpc build exposes max_body_size only as the global
    // gflag (brpc::FLAGS_max_body_size, declared in brpc/protocol.h) — there is
    // no max_body_size field on ServerOptions — so set the gflag directly.
    // brpc's input_messenger reads FLAGS_max_body_size at socket-init time, and
    // BrpcChannelFactory::Create sets the same gflag on the client side. brpc
    // itself rejects any single RPC body at or above this limit, so objects
    // >= 2GB surface a brpc-level error rather than succeeding silently.
    constexpr uint64_t kBrpcMaxBodySize = 2ULL * 1024 * 1024 * 1024;  // 2GB
    if (brpc::FLAGS_max_body_size < kBrpcMaxBodySize) {
        brpc::FLAGS_max_body_size = kBrpcMaxBodySize;
    }
    // Override brpc's per-endpoint pooled-connection cap. Also set on client side.
    if (FLAGS_brpc_max_connection_pool_size > 0) {
        brpc::FLAGS_max_connection_pool_size = FLAGS_brpc_max_connection_pool_size;
    }
    // Install a process-static interceptor that drops requests whose client deadline already
    // elapsed while queued, before the handler runs (defense-in-depth against orphaned server
    // work under load). The interceptor is a no-op when FLAGS_brpc_drop_expired_request is false.
    static const ExpiredRequestInterceptor expiredRequestInterceptor;
    options.interceptor = &expiredRequestInterceptor;
    options.server_owns_interceptor = false;
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

ThreadPool::ThreadPoolUsage RpcServer::GetRpcServicesUsage(const std::string &serviceName) const
{
    auto it = svcMap_.find(serviceName);
    return it != svcMap_.end() ? it->second->GetThreadPoolUsage() : ThreadPool::ThreadPoolUsage();
}

ThreadPool::ThreadPoolUsage RpcServer::GetRpcServicesSnapshot(const std::string &serviceName) const
{
    auto it = svcMap_.find(serviceName);
    return it != svcMap_.end() ? it->second->GetThreadPoolSnapshot() : ThreadPool::ThreadPoolUsage();
}

Status RpcServer::Builder::Init(std::unique_ptr<RpcServer> &server) const
{
    auto key = Token();
    server = std::make_unique<RpcServer>(key);
    RETURN_IF_NOT_OK(server->Init());
    if (useBrpc_) {
        server->useBrpc_ = true;
    }
    return Status::OK();
}

Status RpcServer::Builder::BuildAndStart(std::unique_ptr<RpcServer> &server) const
{
    try {
        for (auto &ele : svcList_) {
            RETURN_IF_NOT_OK(server->RegisterService(ele.first, ele.second));
        }
        if (preStartCallback_) {
            RETURN_IF_NOT_OK(preStartCallback_());
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
