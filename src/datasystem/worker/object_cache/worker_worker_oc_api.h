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
 * Description: Defines the worker class to communicate with the worker service.
 */
#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_WORKER_OC_API_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_WORKER_OC_API_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_unary_client_impl.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"

namespace datasystem {
namespace object_cache {
class WorkerWorkerOCServiceImpl;

class WorkerWorkerOCApi {
public:
    virtual ~WorkerWorkerOCApi() = default;

    /**
     * @brief Initialize the WorkerMasterOCApi Object.
     * @return Status of the call.
     */
    virtual Status Init() = 0;

    /**
     * @brief Get object data from other worker.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @param[in] payload The rpc received payload.
     * @return Status of the call.
     */
    virtual Status GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                   std::vector<RpcMessage> &payload) = 0;

    virtual Status GetObjectRemote(
        std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> *clientApi) = 0;

    virtual Status BatchGetObjectRemote(
        std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> *clientApi) = 0;

    /**
     * @brief Migrate data by triggering remote get during voluntary scale down.
     * @param[in] req rpc request.
     * @param[out] rsp rpc response.
     * @return Status of the call.
     */
    virtual Status NotifyRemoteGet(NotifyRemoteGetReqPb &req, NotifyRemoteGetRspPb &rsp) = 0;
};

class WorkerLocalWorkerOCApi : public WorkerWorkerOCApi {
public:
    /**
     * @brief Construct WorkerLocalWorkerOCApi.
     * @param[in] hostPort The address of remote worker node.
     * @param[in] localHostPort The address of local worker node.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    WorkerLocalWorkerOCApi(WorkerWorkerOCServiceImpl *service, std::shared_ptr<AkSkManager> akSkManager);

    ~WorkerLocalWorkerOCApi() override = default;

    /**
     * @brief Initialize the WorkerMasterOCApi Object.
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Get object data from local worker.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @param[in] payload The rpc received payload.
     * @return Status of the call.
     */
    Status GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                           std::vector<RpcMessage> &payload) override;

    Status GetObjectRemote(
        std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> *clientApi) override
    {
        (void)clientApi;
        RETURN_STATUS(K_RUNTIME_ERROR, "Not supported in local version");
    }

    Status BatchGetObjectRemote(
        std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> *clientApi)
        override
    {
        (void)clientApi;
        RETURN_STATUS(K_RUNTIME_ERROR, "Not supported in local version");
    }

    Status NotifyRemoteGet(NotifyRemoteGetReqPb &req, NotifyRemoteGetRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        RETURN_STATUS(K_RUNTIME_ERROR, "Not supported in local version");
    }

private:
    WorkerWorkerOCServiceImpl *service_;
    std::shared_ptr<AkSkManager> akSkManager_;
};

class WorkerRemoteWorkerOCApi : public WorkerWorkerOCApi {
public:
    /**
     * @brief Construct WorkerRemoteWorkerOCApi.
     * @param[in] hostPort The address of remote worker node.
     * @param[in] localHostPort The address of local worker node.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    WorkerRemoteWorkerOCApi(HostPort hostPort, std::shared_ptr<AkSkManager> akSkManager);

    ~WorkerRemoteWorkerOCApi() override = default;

    /**
     * @brief Initialize the WorkerMasterOCApi Object.
     * @return Status of the call.
     */
    Status Init() override;

    std::string Address() const
    {
        return hostPort_.ToString();
    }

    /**
     * @brief Get the HostPort of the remote worker node.
     * @return HostPort The HostPort object of the remote worker node.
     */
    HostPort GetHostPort() const
    {
        return hostPort_;
    }

    /**
     * @brief Get object data from remote worker.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @param[in] payload The rpc received payload.
     * @return Status of the call.
     */
    Status GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                           std::vector<RpcMessage> &payload) override;

    Status GetObjectRemote(
        std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> *clientApi) override;

    Status GetObjectRemoteWrite(
        std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> &clientApi,
        GetObjectRemoteReqPb &req);

    Status BatchGetObjectRemote(
        std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> *clientApi)
        override;

    Status BatchGetObjectRemoteWrite(
        std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> &clientApi,
        BatchGetObjectRemoteReqPb &req);

    Status CheckEtcdStateAsyncWrite(CheckEtcdStateReqPb &req, int64_t &tag);

    Status CheckEtcdStateAsyncRead(int64_t tag, CheckEtcdStateRspPb &rsp);

    Status GetClusterStateAsyncWrite(GetClusterStateReqPb &req, int64_t &tag);

    Status GetClusterStateAsyncRead(int64_t tag, GetClusterStateRspPb &rsp);

    Status MigrateData(MigrateDataReqPb &req, const std::vector<MemView> &payloads, MigrateDataRspPb &rsp);

    Status MigrateDataDirect(MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp);

    Status NotifyRemoteGet(NotifyRemoteGetReqPb &req, NotifyRemoteGetRspPb &rsp) override;

private:
    // The HostPort of the remote worker node.
    HostPort hostPort_;
    // session to the worker rpc service.
    std::shared_ptr<WorkerWorkerOCService_Stub> rpcSession_{ nullptr };
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
};

/**
 * @brief Create a worker to worker api object to connect remote worker rpc server.
 * @param[in] endPoint The remote worker end point.
 * @param[in] akSkManager Used to do AK/SK authenticate.
 * @param[out] workerOcApi The WorkerRemoteWorkerOCApi ptr.
 * @return Status of the call.
 */
Status CreateRemoteWorkerApi(const std::string &endPoint, const std::shared_ptr<AkSkManager> &akSkManager,
                             std::shared_ptr<WorkerRemoteWorkerOCApi> &workerOcApi);
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_WORKER_OC_API_H
