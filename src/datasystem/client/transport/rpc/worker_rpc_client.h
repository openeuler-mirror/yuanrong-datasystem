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

/** Description: Defines a reusable RPC client for one worker address. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_WORKER_RPC_CLIENT_H
#define DATASYSTEM_CLIENT_TRANSPORT_WORKER_RPC_CLIENT_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/client/transport/rpc/client_request_auth.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/protos/object_posix.brpc.stub.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/worker_object.brpc.stub.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

class WorkerRpcClient {
public:
    WorkerRpcClient(HostPort workerAddress, std::shared_ptr<ClientRequestAuth> auth,
                    BrpcChannelConfig channelConfig = {});
    virtual ~WorkerRpcClient();

    /** @brief Create the brpc channel and object-cache stub for this worker. */
    virtual Status Init();

    /** @brief Query object sizes through the cached control connection. */
    virtual Status QueryObjectSizes(const std::vector<std::string> &objectKeys,
                                    std::vector<uint64_t> &objectSizes);

    /** @brief Authenticate and invoke Get through the cached control connection. */
    virtual Status InvokeGet(int64_t subTimeoutMs, GetReqPb &request, GetRspPb &response,
                             std::vector<RpcMessage> &payloads, uint32_t &workerVersion);

    /** @brief Exchange URMA connection data over this worker's cached brpc channel. */
    virtual Status ExchangeUrmaConnectInfo(UrmaHandshakeRspPb &response);

    /** @return Whether this client still owns an initialized channel and stub. */
    virtual bool IsAlive() const;

    /** @return Logical worker address supplied by routing. */
    const HostPort &WorkerAddress() const
    {
        return workerAddress_;
    }

protected:
    virtual Status DoQueryObjectSizes(const RpcOptions &options, const GetObjMetaInfoReqPb &request,
                                      GetObjMetaInfoRspPb &response);

    virtual Status DoInvokeGet(const RpcOptions &options, const GetReqPb &request, GetRspPb &response,
                               std::vector<RpcMessage> &payloads);

    /** @brief Release the channel and stubs during object destruction. */
    virtual void Close();

private:
    std::shared_ptr<WorkerOCService_BrpcGenericStub> CreateControlStub(
        const std::shared_ptr<brpc::Channel> &channel, int32_t timeoutMs);

    std::shared_ptr<WorkerWorkerTransportService_BrpcGenericStub> CreateTransportStub(
        const std::shared_ptr<brpc::Channel> &channel, int32_t timeoutMs);

    HostPort workerAddress_;
    std::shared_ptr<ClientRequestAuth> auth_;
    BrpcChannelConfig channelConfig_;
    std::shared_ptr<brpc::Channel> channel_;
    std::shared_ptr<WorkerOCService_BrpcGenericStub> stub_;
    std::shared_ptr<WorkerWorkerTransportService_BrpcGenericStub> transportStub_;
    std::atomic<bool> alive_{ false };
    uint32_t connectionGeneration_ = 0;
    static std::atomic<uint32_t> nextConnectionGeneration_;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_WORKER_RPC_CLIENT_H
