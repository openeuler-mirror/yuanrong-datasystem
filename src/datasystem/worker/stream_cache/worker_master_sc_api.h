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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_WORKER_MASTER_SC_API_H
#define DATASYSTEM_WORKER_STREAM_CACHE_WORKER_MASTER_SC_API_H
#include <memory>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/master/stream_cache/master_sc_service_impl.h"
#include "datasystem/protos/master_stream.stub.rpc.pb.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/utils/optional.h"
#include "datasystem/worker/stream_cache/stream_producer.h"
#include "datasystem/worker/worker_master_api_manager_base.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
using MasterSCService_Stub = master::MasterSCService_Stub;
/**
 * @brief The WorkerMasterSCApi is an abstract class that defines the interface for interactions with the stream cache
 * master service.
 */
class WorkerMasterSCApi {
public:
    virtual ~WorkerMasterSCApi() = default;

    /**
     * @brief Initialize the WorkerMasterSCApi Object.
     * @return Status of the call.
     */
    virtual Status Init() = 0;

    /**
     * @brief Create producer service onto master request
     * @param[in] req The req protobuf.
     * @param[out] rsp The rsp protobuf.
     * @return Status of the call.
     */
    virtual Status CreateProducer(master::CreateProducerReqPb &req, master::CreateProducerRspPb &rsp) = 0;

    /**
     * @brief Close producers service onto master request.  List version.
     * @param[in] req The req protobuf.
     * @param[out] rsp The rsp protobuf.
     * @return Status of the call.
     */
    virtual Status CloseProducer(master::CloseProducerReqPb &req, master::CloseProducerRspPb &rsp) = 0;

    /**
     * @brief Subscribe a new consumer onto master request.
     * @param[in] req The req protobuf.
     * @param[out] rsp The rsp protobuf.
     * @return Status of the call.
     */
    virtual Status Subscribe(master::SubscribeReqPb &req, master::SubscribeRspPb &rsp) = 0;

    /**
     * @brief Close a consumer onto master request.
     * @param[in] req The req protobuf.
     * @param[out] rsp The rsp protobuf.
     * @return Status of the call.
     */
    virtual Status CloseConsumer(master::CloseConsumerReqPb &req, master::CloseConsumerRspPb &rsp) = 0;

    /**
     * @brief Delete stream on master request.
     * @param[in] req The req protobuf.
     * @param[out] rsp The rsp protobuf.
     * @return Status of the call.
     */
    virtual Status DeleteStream(master::DeleteStreamReqPb &req, master::DeleteStreamRspPb &rsp) = 0;

    /**
     * @brief Query each worker's producers in global scope for one stream.
     * @param[in] streamName The target stream.
     * @param[out] allWorkerProducers Vector of producers on every worker node.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status QueryGlobalProducersNum(master::QueryGlobalNumReqPb &req, master::QueryGlobalNumRsqPb &rsp) = 0;

    /**
     * @brief Query each worker's consumers in global scope for one stream.
     * @param[in] streamName The target stream.
     * @param[out] allWorkerProducers Vector of consumers on every worker node.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status QueryGlobalConsumersNum(master::QueryGlobalNumReqPb &req, master::QueryGlobalNumRsqPb &rsp) = 0;

    /**
     * @brief Get log prefix
     * @return The log prefix
     */
    virtual std::string LogPrefix() const = 0;

    /**
     * @brief Get master address.
     * @return Master address.
     */
    virtual std::string Address() const = 0;

    /**
     * @brief A factory method to instantiate the correct derived version of the api. Remote masters will use an
     * rpc-based api, whereas local masters can be optimized for in-process pointer based api.
     * @param[in] hostPort The host port of the target master
     * @param[in] localHostPort The local worker rpc service host port.
     * @param[in] akSkManager default to the RPC-based version.
     * @param[in] service The local pointer to the master SC service implementation. If null, the created api must
     * @return A base class pointer to the correct derived type of api.
     */
    static std::shared_ptr<WorkerMasterSCApi> CreateWorkerMasterSCApi(const HostPort &hostPort,
                                                                      const HostPort &localHostPort,
                                                                      std::shared_ptr<AkSkManager> akSkManager,
                                                                      master::MasterSCServiceImpl *service = nullptr);

protected:
    /**
     * @brief Construct WorkerMasterSCApi. Protected constructor enforces class instantiation through the factory method
     * CreateWorkerMasterSCApi.
     * @param[in] localWorkerAddress The local worker rpc service host port
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    explicit WorkerMasterSCApi(const HostPort &localWorkerAddress, std::shared_ptr<AkSkManager> akSkManager);

    HostPort localWorkerAddress_;  // The HostPort of the local worker node
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
};

/**
 * @brief WorkerRemoteMasterApi is the derived remote version of the api for sending and receiving master SC requests
 * where the master is on a different host. This class will use an RPC mechanism for communication to the remote
 * location.
 * Callers will access this class naturally through base class polymorphism.
 * See the parent interface for function argument documentation.
 */
class WorkerRemoteMasterSCApi : public WorkerMasterSCApi {
public:
    /**
     * @brief Constructor for the remote version of the api
     * @param[in] masterAddress The host port of the target master
     * @param[in] localHostPort The local worker rpc service host port
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    explicit WorkerRemoteMasterSCApi(const HostPort &masterAddress, const HostPort &localHostPort,
                                     std::shared_ptr<AkSkManager> akSkManager);
    ~WorkerRemoteMasterSCApi() override = default;
    Status Init() override;
    Status CreateProducer(master::CreateProducerReqPb &req, master::CreateProducerRspPb &rsp) override;
    Status CloseProducer(master::CloseProducerReqPb &req, master::CloseProducerRspPb &rsp) override;
    Status Subscribe(master::SubscribeReqPb &req, master::SubscribeRspPb &rsp) override;
    Status CloseConsumer(master::CloseConsumerReqPb &req, master::CloseConsumerRspPb &rsp) override;
    Status DeleteStream(master::DeleteStreamReqPb &req, master::DeleteStreamRspPb &rsp) override;
    Status QueryGlobalProducersNum(master::QueryGlobalNumReqPb &req, master::QueryGlobalNumRsqPb &rsp) override;
    Status QueryGlobalConsumersNum(master::QueryGlobalNumReqPb &req, master::QueryGlobalNumRsqPb &rsp) override;
    std::string LogPrefix() const override;

    std::string Address() const override
    {
        return masterAddress_.ToString();
    }

private:
    HostPort masterAddress_;                                       // The HostPort of the master node
    std::shared_ptr<MasterSCService_Stub> rpcSession_{ nullptr };  // Session to the master rpc service
};

/**
 * @brief WorkerLocalMasterSCApi is the derived local version of the api for sending and receiving master OC requests
 * where the master exists in the same process as the service. This class will directly reference the service through a
 * pointer and does not use any RPC mechanism for communication.
 * Callers will access this class naturally through base class polymorphism.
 * See the parent interface for function argument documentation.
 */
class WorkerLocalMasterSCApi : public WorkerMasterSCApi {
public:
    /**
     * @brief Constructor for the local version of the api
     * @param[in] service The pointer to the master SC service implementation
     * @param[in] localHostPort The local worker service host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    explicit WorkerLocalMasterSCApi(master::MasterSCServiceImpl *service, const HostPort &localHostPort,
                                    std::shared_ptr<AkSkManager> akSkManager);
    ~WorkerLocalMasterSCApi() override = default;
    Status Init() override;
    Status CreateProducer(master::CreateProducerReqPb &req, master::CreateProducerRspPb &rsp) override;
    Status CloseProducer(master::CloseProducerReqPb &req, master::CloseProducerRspPb &rsp) override;
    Status Subscribe(master::SubscribeReqPb &req, master::SubscribeRspPb &rsp) override;
    Status CloseConsumer(master::CloseConsumerReqPb &req, master::CloseConsumerRspPb &rsp) override;
    Status DeleteStream(master::DeleteStreamReqPb &req, master::DeleteStreamRspPb &rsp) override;
    Status QueryGlobalProducersNum(master::QueryGlobalNumReqPb &req, master::QueryGlobalNumRsqPb &rsp) override;
    Status QueryGlobalConsumersNum(master::QueryGlobalNumReqPb &req, master::QueryGlobalNumRsqPb &rsp) override;
    std::string LogPrefix() const override;

    std::string Address() const override
    {
        return localWorkerAddress_.ToString();
    }

private:
    master::MasterSCServiceImpl *masterSC_;
};

class WorkerMasterSCApiManager : public WorkerMasterApiManagerBase<WorkerMasterSCApi> {
public:
    WorkerMasterSCApiManager(HostPort &hostPort, std::shared_ptr<AkSkManager> manager,
                             master::MasterSCServiceImpl *masterSCService);
    virtual ~WorkerMasterSCApiManager() = default;

    /**
     * @brief Create a worker to Master api object for masterAddress
     * @param[in] masterAddress The remote master ip address
     * @return The WorkerMasterSCApi
     */
    std::shared_ptr<WorkerMasterSCApi> CreateWorkerMasterApi(const HostPort &masterAddress) override;

private:
    master::MasterSCServiceImpl *masterSCService_{ nullptr };
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_STREAM_CACHE_WORKER_MASTER_SC_API_H
