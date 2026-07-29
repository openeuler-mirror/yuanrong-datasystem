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
#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
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
    RETURN_IF_NOT_OK(WorkerWorkerTransportService::Init());
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().GetRecoveryProbeSegmentInfo(recoveryProbeSegmentAddress_,
                                                                             recoveryProbeDataOffset_));
    }
#endif
    return Status::OK();
}

Status WorkerWorkerTransportServiceImpl::WorkerWorkerExchangeUrmaConnectInfo(const UrmaHandshakeReqPb &req,
                                                                             UrmaHandshakeRspPb &rsp)
{
    Timer timer;
    const std::string peerAddress =
        req.has_address() ? req.address().host() + ":" + std::to_string(req.address().port()) : "UNKNOWN";
    LOG(INFO) << "[URMA_NEED_CONNECT] WorkerWorkerExchangeUrmaConnectInfo start, peerAddress=" << peerAddress;
    auto rc = ExchangeJfr(req, rsp);
    if (rc.IsOk()) {
        rsp.set_supports_payload_only_client_batch_get(true);
#ifdef USE_URMA
        if (recoveryProbeSegmentAddress_ != 0 && rsp.has_hand_shake()) {
            auto *probeAddr = rsp.mutable_recovery_probe_addr();
            probeAddr->set_seg_va(recoveryProbeSegmentAddress_);
            probeAddr->set_seg_data_offset(recoveryProbeDataOffset_);
            probeAddr->mutable_request_address()->CopyFrom(rsp.hand_shake().address());
            probeAddr->set_client_id(rsp.hand_shake().client_id());
        }
#endif
    }
    LOG(INFO) << "[URMA_NEED_CONNECT] WorkerWorkerExchangeUrmaConnectInfo finish, elapsed ms: "
              << timer.ElapsedMilliSecond() << ", status=" << rc.ToString();
    return rc;
}
}  // namespace object_cache
}  // namespace datasystem
