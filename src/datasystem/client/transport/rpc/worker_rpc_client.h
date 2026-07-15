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
#include <vector>

#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/master_object.brpc.stub.pb.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/protos/object_posix.brpc.stub.pb.h"
#include "datasystem/protos/worker_object.brpc.stub.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

class WorkerRpcClient {
public:
    WorkerRpcClient(HostPort workerAddress, std::shared_ptr<Signature> signature,
                    BrpcChannelConfig channelConfig = {});
    virtual ~WorkerRpcClient();

    /** @brief Create the brpc channel and transport service stubs for this endpoint. */
    virtual Status Init();

    /** @brief Sign and invoke a direct object read on this worker. */
    virtual Status InvokeGetObject(GetObjectRemoteReqPb &request, GetObjectRemoteRspPb &response,
                                   std::vector<RpcMessage> &payloads);

    /** @brief Sign and query object locations and optional inline data from the master service at this endpoint. */
    virtual Status InvokeQueryAndGet(master::QueryAndGetReqPb &request, master::QueryAndGetRspPb &response,
                                     std::vector<RpcMessage> &payloads);

    /** @brief Sign and invoke Create through the cached control connection. */
    virtual Status InvokeCreate(int64_t subTimeoutMs, CreateReqPb &request, CreateRspPb &response,
                                uint32_t &workerVersion);

    /** @brief Sign and invoke Set (Publish) with optional TCP payload through the cached control connection. */
    virtual Status InvokeSet(int64_t subTimeoutMs, PublishReqPb &request,
                             const std::vector<MemView> &payloads, PublishRspPb &response,
                             uint32_t &workerVersion);

    /** @brief Sign and invoke MultiCreate through the cached control connection. */
    virtual Status InvokeMultiCreate(int64_t subTimeoutMs, MultiCreateReqPb &request,
                                     MultiCreateRspPb &response, uint32_t &workerVersion);

    /** @brief Sign and invoke MultiPublish with positional TCP payloads. */
    virtual Status InvokeMultiSet(int64_t subTimeoutMs, MultiPublishReqPb &request,
                                  const std::vector<MemView> &payloads, MultiPublishRspPb &response,
                                  uint32_t &workerVersion);

    /** @brief Release a worker-side allocation created for a routed Set transaction. */
    virtual Status InvokeDecreaseReference(const TransportRequestContext &context, const ShmKey &shmId);

    /** @brief Exchange URMA connection data over this worker's cached brpc channel. */
    virtual Status ExchangeUrmaConnectInfo(UrmaHandshakeRspPb &response);

    /** @brief Fetch the versioned routing hash ring through the cached worker channel. */
    virtual Status InvokeGetHashRing(uint64_t currentVersion, GetHashRingRspPb &response);

    /** @return Whether this client still owns an initialized channel and all required stubs. */
    virtual bool IsAlive() const;

    /** @return Logical worker address supplied by routing. */
    const HostPort &WorkerAddress() const
    {
        return workerAddress_;
    }

protected:
    virtual Status DoInvokeGetObject(const RpcOptions &options, const GetObjectRemoteReqPb &request,
                                     GetObjectRemoteRspPb &response, std::vector<RpcMessage> &payloads);

    virtual Status DoInvokeQueryAndGet(const RpcOptions &options, const master::QueryAndGetReqPb &request,
                                       master::QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads);

    virtual Status DoInvokeCreate(const RpcOptions &options, const CreateReqPb &request,
                                  CreateRspPb &response);

    virtual Status DoInvokeSet(const RpcOptions &options, const PublishReqPb &request,
                               PublishRspPb &response, const std::vector<MemView> &payloads);

    virtual Status DoInvokeMultiCreate(const RpcOptions &options, const MultiCreateReqPb &request,
                                       MultiCreateRspPb &response);

    virtual Status DoInvokeMultiSet(const RpcOptions &options, const MultiPublishReqPb &request,
                                    MultiPublishRspPb &response, const std::vector<MemView> &payloads);

    virtual Status DoInvokeDecreaseReference(const RpcOptions &options, const DecreaseReferenceRequest &request,
                                              DecreaseReferenceResponse &response);

    virtual Status DoInvokeGetHashRing(const RpcOptions &options, const GetHashRingReqPb &request,
                                       GetHashRingRspPb &response);

    /** @brief Release the channel and stubs during object destruction. */
    virtual void Close();

private:
    HostPort workerAddress_;
    std::shared_ptr<Signature> signature_;
    BrpcChannelConfig channelConfig_;
    std::shared_ptr<brpc::Channel> channel_;
    std::shared_ptr<WorkerOCService_BrpcGenericStub> controlStub_;
    std::shared_ptr<WorkerWorkerTransportService_BrpcGenericStub> transportStub_;
    std::shared_ptr<WorkerWorkerOCService_BrpcGenericStub> dataStub_;
    std::shared_ptr<master::MasterOCService_BrpcGenericStub> masterStub_;
    std::atomic<bool> alive_{ false };
    uint32_t connectionGeneration_ = 0;
    static std::atomic<uint32_t> nextConnectionGeneration_;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_WORKER_RPC_CLIENT_H
