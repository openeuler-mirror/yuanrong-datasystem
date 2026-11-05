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

#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"

#include <google/protobuf/descriptor.h>

#include "datasystem/common/rpc/brpc_auth.h"
#include "datasystem/protos/master_object.auth.pb.h"
#include "datasystem/protos/worker_object.auth.pb.h"

#ifdef BUILD_STREAM_CACHE
#include "datasystem/protos/master_stream.stub.rpc.pb.h"
#include "datasystem/protos/stream_posix.stub.rpc.pb.h"
#include "datasystem/protos/worker_stream.stub.rpc.pb.h"
#include "datasystem/protos/master_stream.brpc.stub.pb.h"
#include "datasystem/protos/stream_posix.brpc.stub.pb.h"
#include "datasystem/protos/worker_stream.brpc.stub.pb.h"
#include "datasystem/protos/master_stream.auth.pb.h"
#include "datasystem/protos/stream_posix.auth.pb.h"
#include "datasystem/protos/worker_stream.auth.pb.h"
#endif
// Restore datasystem logging macros after generated brpc headers.
#include "datasystem/common/log/log.h"

namespace datasystem {
Status RpcStubCacheMgr::CreateMeshMethodMap(std::shared_ptr<const RpcMethodMap> &methods)
{
    const auto servicePair = [](const google::protobuf::ServiceDescriptor *authenticated) {
        const auto &name = authenticated->full_name();
        constexpr size_t authSuffixLength = 4;
        const auto *plain = google::protobuf::DescriptorPool::generated_pool()->FindServiceByName(
            name.substr(0, name.size() - authSuffixLength));
        return RpcMethodMap::ServicePair{plain, authenticated};
    };
#ifdef BUILD_STREAM_CACHE
    return RpcMethodMap::Create({servicePair(WorkerWorkerOCServiceAuth::descriptor()),
                                servicePair(master::MasterOCServiceAuth::descriptor()),
                                servicePair(WorkerWorkerSCServiceAuth::descriptor()),
                                servicePair(master::MasterSCServiceAuth::descriptor()),
                                servicePair(MasterWorkerOCServiceAuth::descriptor()),
                                servicePair(MasterWorkerSCServiceAuth::descriptor()),
                                servicePair(WorkerWorkerTransportServiceAuth::descriptor()),
                                servicePair(ClientWorkerSCServiceAuth::descriptor())}, methods);
#else
    return RpcMethodMap::Create({servicePair(WorkerWorkerOCServiceAuth::descriptor()),
                                servicePair(master::MasterOCServiceAuth::descriptor()),
                                servicePair(MasterWorkerOCServiceAuth::descriptor()),
                                servicePair(WorkerWorkerTransportServiceAuth::descriptor())}, methods);
#endif
}

#ifdef BUILD_STREAM_CACHE
void RpcStubCacheMgr::InitStreamCreators(const RpcSigningBinding &binding, RpcStubCreators &creators)
{
    creators.emplace(StubType::WORKER_WORKER_SC_SVC, MakeBrpcCreator(StubType::WORKER_WORKER_SC_SVC, binding));
    creators.emplace(StubType::WORKER_MASTER_SC_SVC, MakeBrpcCreator(StubType::WORKER_MASTER_SC_SVC, binding));
    creators.emplace(StubType::MASTER_WORKER_SC_SVC, MakeBrpcCreator(StubType::MASTER_WORKER_SC_SVC, binding));
    creators.emplace(StubType::CLIENT_WORKER_SC_SVC, MakeBrpcCreator(StubType::CLIENT_WORKER_SC_SVC, binding));
}

Status RpcStubCacheMgr::CreateStreamBrpcStub(StubType type, const std::shared_ptr<brpc::Channel> &brpcChannel,
                                             int32_t timeoutMs, std::shared_ptr<RpcStubBase> &stub)
{
    switch (type) {
        case StubType::WORKER_WORKER_SC_SVC:
            stub = std::make_shared<WorkerWorkerSCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::WORKER_MASTER_SC_SVC:
            stub = std::make_shared<master::MasterSCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::MASTER_WORKER_SC_SVC:
            stub = std::make_shared<MasterWorkerSCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        case StubType::CLIENT_WORKER_SC_SVC:
            stub = std::make_shared<ClientWorkerSCService_BrpcGenericStub>(brpcChannel.get(), timeoutMs);
            break;
        default:
            RETURN_STATUS(K_RUNTIME_ERROR, "Unsupport type: " + std::to_string(static_cast<int>(type)));
    }
    return stub->GetInitStatus();
}
#else
void RpcStubCacheMgr::InitStreamCreators(const RpcSigningBinding &, RpcStubCreators &)
{
}

Status RpcStubCacheMgr::CreateStreamBrpcStub(StubType type, const std::shared_ptr<brpc::Channel> &,
                                             int32_t, std::shared_ptr<RpcStubBase> &)
{
    switch (type) {
        case StubType::WORKER_WORKER_SC_SVC:
        case StubType::WORKER_MASTER_SC_SVC:
        case StubType::MASTER_WORKER_SC_SVC:
        case StubType::CLIENT_WORKER_SC_SVC:
            RETURN_STATUS(K_NOT_SUPPORTED, "StreamCache is not compiled");
        default:
            RETURN_STATUS(K_RUNTIME_ERROR, "Unsupport type: " + std::to_string(static_cast<int>(type)));
    }
}
#endif
}  // namespace datasystem
