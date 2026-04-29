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
#include "datasystem/worker/object_cache/worker_worker_transport_service_impl.h"

#include <cstdint>
#include <thread>

#include "datasystem/utils/status.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace object_cache {
WorkerWorkerTransportServiceImpl::WorkerWorkerTransportServiceImpl(
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> clientSvc)
    : ocClientWorkerSvc_(std::move(clientSvc))
{
}

WorkerWorkerTransportServiceImpl::~WorkerWorkerTransportServiceImpl()
{
    LOG(INFO) << "WorkerWorkerTransportServiceImpl exit";
}

Status WorkerWorkerTransportServiceImpl::Init()
{
    CHECK_FAIL_RETURN_STATUS(ocClientWorkerSvc_ != nullptr, StatusCode::K_NOT_READY,
                             "ClientWorkerService must be initialized before WorkerWorkerService construction");
    return WorkerWorkerTransportService::Init();
}

Status WorkerWorkerTransportServiceImpl::WorkerWorkerExchangeUrmaConnectInfo(const UrmaHandshakeReqPb &req,
                                                                             UrmaHandshakeRspPb &rsp)
{
    Timer timer;
    const std::string peerAddress =
        req.has_address() ? req.address().host() + ":" + std::to_string(req.address().port()) : "UNKNOWN";
    LOG(INFO) << "[URMA_NEED_CONNECT] WorkerWorkerExchangeUrmaConnectInfo start, peerAddress=" << peerAddress;
    auto rc = ExchangeJfr(req, rsp);
    LOG(INFO) << "[URMA_NEED_CONNECT] WorkerWorkerExchangeUrmaConnectInfo finish, elapsed ms: "
              << timer.ElapsedMilliSecond() << ", status=" << rc.ToString();
    return rc;
}
}  // namespace object_cache
}  // namespace datasystem
