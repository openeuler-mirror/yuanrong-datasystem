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

/** Description: Implements the UB data-plane connection. */

#include "datasystem/client/transport/data_plane/ub_connection.h"

#include <cstdint>
#include <utility>

#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/transport_phase_latency_recorder.h"
#include "datasystem/common/util/status_helper.h"

#ifdef USE_URMA
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rdma/urma_manager.h"
#include "datasystem/protos/meta_transport.pb.h"
#endif

namespace datasystem {
namespace client {

UbConnection::UbConnection(std::shared_ptr<WorkerRpcClient> rpcClient) : rpcClient_(std::move(rpcClient))
{
}

UbConnection::~UbConnection()
{
    Teardown();
}

Status UbConnection::Establish(const HostPort &workerAddr)
{
    return Establish(workerAddr, nullptr);
}

Status UbConnection::Establish(const HostPort &workerAddr, TransportPhaseLatencyRecorder *recorder)
{
    workerAddr_ = workerAddr;
    return EstablishUrma(recorder);
}

Status UbConnection::EstablishUrma(TransportPhaseLatencyRecorder *recorder)
{
    supportsPayloadOnlyClientBatchGet_.store(false, std::memory_order_release);
#ifdef USE_URMA
    if (!UrmaManager::IsUrmaEnabled()) {
        return Status(K_URMA_CONNECT_FAILED, "Urma not enabled");
    }
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    UrmaHandshakeRspPb response;
    const auto exchangeBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                   : recorder->StartPhase();
    Status status = rpcClient_->ExchangeUrmaConnectInfo(response);
    if (recorder != nullptr) {
        const uint64_t exchangeUs = recorder->ElapsedUs(exchangeBegin);
        recorder->RecordPhase("urma_connect_info_exchange", exchangeUs, TransportLatencyThreshold::RPC);
    }
    RETURN_IF_NOT_OK(status);
    const auto phaseBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                : recorder->StartPhase();
    status = FinalizeOutboundConnection(response);
    if (recorder != nullptr) {
        recorder->RecordPhase("urma_connection_finalize", phaseBegin, TransportLatencyThreshold::PROCESS);
    }
    RETURN_IF_NOT_OK(status);
    supportsPayloadOnlyClientBatchGet_.store(response.supports_payload_only_client_batch_get(),
                                             std::memory_order_release);
    urmaReady_.store(true, std::memory_order_release);
    return Status::OK();
#else
    (void) recorder;
    return Status(K_NOT_SUPPORTED, "USE_URMA not compiled");
#endif
}

bool UbConnection::IsAlive() const
{
    return urmaReady_.load(std::memory_order_acquire);
}

bool UbConnection::SupportsPayloadOnlyClientBatchGet() const
{
    return supportsPayloadOnlyClientBatchGet_.load(std::memory_order_acquire);
}

void UbConnection::Teardown()
{
    supportsPayloadOnlyClientBatchGet_.store(false, std::memory_order_release);
#ifdef USE_URMA
    if (urmaReady_.exchange(false, std::memory_order_acq_rel)) {
        (void)datasystem::RemoveRemoteFastTransportNode(workerAddr_);
    }
#else
    urmaReady_.store(false, std::memory_order_release);
#endif
}

}  // namespace client
}  // namespace datasystem
