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

#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/zmq/zmq_server_impl.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
RpcServer::RpcServer(Token key, const RpcCredential &cred)
{
    (void)key;
    LOG(INFO) << "Start up server with ZMQ communication framework.";
    auto passkey = ZmqServerImpl::Token();
    pimpl_ = std::make_unique<ZmqServerImpl>(passkey, cred);
}
RpcServer::~RpcServer() noexcept = default;

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

Status RpcServer::Builder::Init(std::unique_ptr<RpcServer> &server) const
{
    auto key = Token();
    server = std::make_unique<RpcServer>(key, cred_);
    RETURN_IF_NOT_OK(server->Init());
    if (RpcAuthKeyManager::Instance().HasAuthHandler()) {
        RETURN_IF_NOT_OK(server->InitAuthHandler());
    }
    for (const auto &v : endPts_) {
        RETURN_IF_NOT_OK(server->Bind(v));
    }
    return Status::OK();
}

Status RpcServer::Builder::BuildAndStart(std::unique_ptr<RpcServer> &server) const
{
    try {
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
        RETURN_IF_NOT_OK(server->Run());
    } catch (const std::bad_alloc &e) {
        RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY, e.what());
    }
    return Status::OK();
}
}  // namespace datasystem
