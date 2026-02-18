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
 * Description: Defines the worker worker service processing main class.
 */
#ifndef DATASYSTEM_WORKER_TRANSPORT_WORKER_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_TRANSPORT_WORKER_SERVICE_IMPL_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/protos/worker_object.service.rpc.pb.h"
#include "datasystem/protos/meta_transport.pb.h"

namespace datasystem {
namespace object_cache {
class WorkerWorkerTransportServiceImpl : public WorkerWorkerTransportService {
public:
    /**
     * @brief Construct WorkerWorkerTransportServiceImpl.
     * @param[in] clientSvc The implementation of worker service.
     */
    WorkerWorkerTransportServiceImpl(std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> clientSvc);

    ~WorkerWorkerTransportServiceImpl() override;

    /**
     * @brief Initialize the WorkerWorkerTransportServiceImpl Object.
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Receive and set remote urma info.
     * @param[in] req Urma handshake request..
     * @param[in] rsp Urma handshake response.
     * @return Status of the call.
     */
    Status WorkerWorkerExchangeUrmaConnectInfo(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp) override;

private:
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> ocClientWorkerSvc_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_TRANSPORT_WORKER_SERVICE_IMPL_H
