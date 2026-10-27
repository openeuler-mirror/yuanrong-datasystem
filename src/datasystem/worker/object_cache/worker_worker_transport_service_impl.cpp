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
#include <optional>
#include <thread>

#include "datasystem/utils/status.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
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
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> clientSvc, HostPort localWorker,
    const cluster::MembershipEndpointView &membership)
    : ocClientWorkerSvc_(std::move(clientSvc)),
      localWorker_(std::move(localWorker)),
      membership_(membership)
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

UbHealthSummary WorkerWorkerTransportServiceImpl::EncodeProbeHealthSummary(
    const std::string &incarnation, ProviderUbRecoveryProbeRspPb &rsp) const
{
    auto summary = ocClientWorkerSvc_->BuildSelfUbHealthSummary();
    summary.incarnation = incarnation;
    EncodeUbHealthSummary(summary, *rsp.mutable_health_summary());
    return summary;
}

Status WorkerWorkerTransportServiceImpl::ProbeProviderUbRecovery(const ProviderUbRecoveryProbeReqPb &req,
                                                                 ProviderUbRecoveryProbeRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(req.has_hand_shake() && req.has_recovery_probe_addr(), K_INVALID,
                             "Provider UB recovery probe is missing Client URMA information");
    std::string workerIncarnation;
    RETURN_IF_NOT_OK(ResolveWorkerIncarnation(workerIncarnation));

    auto summary = EncodeProbeHealthSummary(workerIncarnation, rsp);
    CHECK_FAIL_RETURN_STATUS(req.expected_worker_incarnation().empty()
                                 || req.expected_worker_incarnation() == workerIncarnation,
                             K_NOT_READY, "Worker incarnation changed before Provider UB recovery probe");
#ifdef USE_URMA
    auto *admission = ocClientWorkerSvc_->GetUbAdmission();
    CHECK_FAIL_RETURN_STATUS(admission != nullptr, K_NOT_READY, "Provider UB admission is unavailable");
    std::optional<UbProbeToken> selfProbe;
    if (!summary.writable) {
        selfProbe = admission->TryBeginProbe(localWorker_, GetSteadyClockTimeStampMs());
        if (!selfProbe.has_value()) {
            return Status::OK();
        }
    }
    Status probeStatus = ImportRecoveryProbeHandshake(req.hand_shake());
    if (probeStatus.IsOk()) {
        UrmaHandshakeRspPb probeTarget;
        probeTarget.mutable_recovery_probe_addr()->CopyFrom(req.recovery_probe_addr());
        probeStatus = ProbeUbDataPlane(probeTarget);
    }
    if (selfProbe.has_value()) {
        (void)admission->CompleteProbe(*selfProbe, probeStatus, GetSteadyClockTimeStampMs(), false);
    }
    if (probeStatus.IsError()) {
        (void)EncodeProbeHealthSummary(workerIncarnation, rsp);
        return probeStatus;
    }
    rsp.set_probe_performed(true);
    std::string verifiedIncarnation;
    RETURN_IF_NOT_OK(ResolveWorkerIncarnation(verifiedIncarnation));
    auto verifiedSummary = EncodeProbeHealthSummary(verifiedIncarnation, rsp);
    const bool admissionEpochValid = selfProbe.has_value() ? verifiedSummary.epoch > summary.epoch
                                                            : verifiedSummary.epoch == summary.epoch;
    CHECK_FAIL_RETURN_STATUS(verifiedIncarnation == workerIncarnation && verifiedSummary.writable
                                 && admissionEpochValid,
                             K_NOT_READY, "Worker identity or UB admission changed during Provider recovery probe");
    INJECT_POINT_NO_RETURN("WorkerWorkerTransportService.ProbeProviderUbRecovery.success");
    return Status::OK();
#else
    if (!summary.writable) {
        return Status::OK();
    }
    return Status(K_NOT_SUPPORTED, "URMA Provider recovery probe is unavailable in this build");
#endif
}

Status WorkerWorkerTransportServiceImpl::QueryUbPortHealth(const QueryUbPortHealthReqPb &req,
                                                           QueryUbPortHealthRspPb &rsp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(ocClientWorkerSvc_);
    return ocClientWorkerSvc_->QuerySelfUbPortHealth(req, rsp);
}

Status WorkerWorkerTransportServiceImpl::ResolveWorkerIncarnation(std::string &incarnation) const
{
    cluster::MemberEndpoint endpoint;
    RETURN_IF_NOT_OK(membership_.ResolveByAddress(localWorker_.ToString(), endpoint));
    CHECK_FAIL_RETURN_STATUS(!endpoint.identity.id.empty(), K_NOT_READY,
                             "Worker topology incarnation is unavailable for Provider UB recovery probe");
    incarnation = endpoint.identity.id;
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
