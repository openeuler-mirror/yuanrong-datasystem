/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_WORKER_TRANSPORT_API_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_WORKER_TRANSPORT_API_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_unary_client_impl.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"

namespace datasystem {
namespace object_cache {
class WorkerWorkerTransportServiceImpl;

class WorkerWorkerTransportApi {
public:
    virtual ~WorkerWorkerTransportApi() = default;

    /**
     * @brief Initialize the WorkerWorkerTransportServiceImpl Object.
     * @return Status of the call.
     */
    virtual Status Init() = 0;

    /**
     * @brief Send local urma connect info.
     * @return Status of the call.
     */
    virtual Status ExchangeUrmaConnectInfo() = 0;

    /**
     * @brief Execute only once during concurrent execution of exchange.
     * @return Status of the call.
     */
    Status ExecOnceParrallelExchange();

private:
    std::mutex mtx_;
    std::condition_variable cv_;

    std::atomic<bool> isExecuting_{ false };
    std::atomic<bool> globalStopFlag_{ false };
};

class WorkerLocalWorkerTransApi : public WorkerWorkerTransportApi {
public:
    WorkerLocalWorkerTransApi(WorkerWorkerTransportServiceImpl *service);

    ~WorkerLocalWorkerTransApi() override = default;

    Status Init() override;

    Status ExchangeUrmaConnectInfo() override;

private:
    WorkerWorkerTransportServiceImpl *service_;
};

class WorkerRemoteWorkerTransApi : public WorkerWorkerTransportApi {
public:
    WorkerRemoteWorkerTransApi(HostPort hostPort);

    ~WorkerRemoteWorkerTransApi() override = default;

    Status Init() override;

    Status ExchangeUrmaConnectInfo() override;

private:
    HostPort hostPort_;

    std::shared_ptr<WorkerWorkerTransportService_Stub> rpcSession_{ nullptr };
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_WORKER_TRANSPORT_API_H
