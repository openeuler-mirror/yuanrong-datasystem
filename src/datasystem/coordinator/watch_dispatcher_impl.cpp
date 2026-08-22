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

#include "datasystem/coordinator/watch_dispatcher_impl.h"

#include <algorithm>

#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/coordinator.stub.rpc.pb.h"
#include "datasystem/protos/coordinator.brpc.stub.pb.h"

namespace datasystem {
namespace coordinator {
namespace {
constexpr int32_t WATCH_NOTIFY_RPC_TIMEOUT_MS = 3000;
constexpr int64_t PROBE_WATCH_ID = 1;

/**
 * @brief Calculate the watch callback RPC timeout from the node timeout.
 * @return Bounded callback RPC timeout in milliseconds.
 */
int32_t GetWatchNotifyRpcTimeoutMs()
{
    return std::min<int32_t>(WATCH_NOTIFY_RPC_TIMEOUT_MS, static_cast<int32_t>(FLAGS_node_timeout_s) * TO_MILLISECOND);
}

/**
 * @brief Convert an internal watch event type to its RPC representation.
 * @param[in] type Internal watch event type.
 * @param[out] pbType RPC watch event type.
 * @return Status of the conversion.
 */
Status ConvertEventType(WatchEvent::Type type, EventPb::EventType &pbType)
{
    switch (type) {
        case WatchEvent::Type::PUT:
            pbType = EventPb::PUT;
            return Status::OK();
        case WatchEvent::Type::DELETE:
            pbType = EventPb::DELETE;
            return Status::OK();
        case WatchEvent::Type::REWATCH:
            pbType = EventPb::RESET;
            return Status::OK();
        default:
            RETURN_STATUS(StatusCode::K_INVALID, "unknown watch event type");
    }
}

/**
 * @brief Serialize one committed key-value entry into a watch response.
 * @param[in] entry Committed store entry.
 * @param[out] kv RPC key-value message to populate.
 */
void FillEventKv(const KeyValueEntry &entry, KeyValue *kv)
{
    kv->set_key(entry.key);
    kv->set_value(entry.value);
    kv->set_version(entry.version);
    kv->set_mod_revision(entry.modRevision);
}
}  // namespace

WatchDispatcherImpl::~WatchDispatcherImpl()
{
    Stop();
}

Status WatchDispatcherImpl::DoNotify(int64_t watchId, const std::string &watcherAddr,
                                     std::vector<std::shared_ptr<WatchEvent>> &events)
{
    EventReqPb req;
    req.set_watch_id(watchId);
    req.set_coordinator_id(coordinatorId_);
    for (const auto &event : events) {
        CHECK_FAIL_RETURN_STATUS(event != nullptr, StatusCode::K_INVALID, "watch event is null");
        EventPb::EventType pbType = EventPb::EVENT_TYPE_UNSPECIFIED;
        RETURN_IF_NOT_OK(ConvertEventType(event->type, pbType));
        auto *pbEvent = req.add_events();
        pbEvent->set_type(pbType);
        if (pbType != EventPb::RESET) {
            FillEventKv(event->entry, pbEvent->mutable_kv());
        }
    }
    auto rc = SendEventRequest(watcherAddr, req, EventRequestKind::NOTIFICATION, GetWatchNotifyRpcTimeoutMs());
    if (rc.IsOk()) {
        METRIC_INC(metrics::KvMetricId::COORDINATOR_WATCH_NOTIFICATION_SENT_BATCHES_TOTAL);
        METRIC_ADD(metrics::KvMetricId::COORDINATOR_WATCH_NOTIFICATION_SENT_EVENTS_TOTAL, events.size());
        METRIC_ADD(metrics::KvMetricId::COORDINATOR_WATCH_NOTIFICATION_SENT_BYTES_TOTAL,
                   static_cast<uint64_t>(req.ByteSizeLong()));
    }
    return rc;
}

WorkerReachabilityProbeResult WatchDispatcherImpl::ProbeWorkerReachable(
    const std::string &watcherAddr, std::chrono::steady_clock::time_point absoluteDeadline)
{
    if (std::chrono::steady_clock::now() >= absoluteDeadline) {
        return { Status(K_RPC_DEADLINE_EXCEEDED, "Worker reachability probe deadline already expired"), false };
    }
    EventReqPb req;
    req.set_watch_id(PROBE_WATCH_ID);
    req.set_coordinator_id(coordinatorId_);
    // Intentionally fail Worker whole-batch validation: a dispatched application error proves process reachability
    // without acquiring/delivering the event handler or triggering rewatch.
    req.add_events()->set_type(EventPb::EVENT_TYPE_UNSPECIFIED);
    bool rpcDispatched = false;
    auto rc = SendEventRequest(watcherAddr, req, EventRequestKind::PROBE, GetWatchNotifyRpcTimeoutMs(), absoluteDeadline,
                               &rpcDispatched);
    return { std::move(rc), rpcDispatched };
}

Status WatchDispatcherImpl::SendEventRequest(const std::string &watcherAddr, const EventReqPb &req,
                                             EventRequestKind requestKind, int32_t timeoutMs,
                                             std::chrono::steady_clock::time_point absoluteDeadline,
                                             bool *rpcDispatched)
{
    if (rpcDispatched != nullptr) {
        *rpcDispatched = false;
    }
    HostPort watcherHostPort;
    RETURN_IF_NOT_OK(watcherHostPort.ParseString(watcherAddr));
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(
        watcherHostPort, StubType::COORDINATOR_WORKER_SVC, rpcStub, absoluteDeadline));
    RpcOptions opts;
    EventRspPb rsp;
    auto brpcStub = std::dynamic_pointer_cast<CoordinatorWatchService_BrpcGenericStub>(rpcStub);
    RETURN_RUNTIME_ERROR_IF_NULL(brpcStub);
    if (absoluteDeadline != std::chrono::steady_clock::time_point::max()) {
        const auto remainingMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     absoluteDeadline - std::chrono::steady_clock::now())
                                     .count();
        CHECK_FAIL_RETURN_STATUS(remainingMs > 0, K_RPC_DEADLINE_EXCEEDED,
                                 "Worker reachability probe deadline exceeded before dispatch");
        timeoutMs = static_cast<int32_t>(std::min<int64_t>(timeoutMs, remainingMs));
    }
    opts.SetTimeout(timeoutMs);
    if (rpcDispatched != nullptr) {
        *rpcDispatched = true;
    }
    const auto requestsMetric = requestKind == EventRequestKind::NOTIFICATION
                                    ? metrics::KvMetricId::COORDINATOR_WATCH_NOTIFICATION_INFLIGHT_REQUESTS
                                    : metrics::KvMetricId::COORDINATOR_WATCH_PROBE_INFLIGHT_REQUESTS;
    const auto bytesMetric = requestKind == EventRequestKind::NOTIFICATION
                                 ? metrics::KvMetricId::COORDINATOR_WATCH_NOTIFICATION_INFLIGHT_BYTES
                                 : metrics::KvMetricId::COORDINATOR_WATCH_PROBE_INFLIGHT_BYTES;
    auto inflightRequests = metrics::GetGauge(static_cast<uint16_t>(requestsMetric));
    auto inflightBytes = metrics::GetGauge(static_cast<uint16_t>(bytesMetric));
    const auto requestBytes = static_cast<int64_t>(req.ByteSizeLong());
    inflightRequests.Inc();
    inflightBytes.Inc(requestBytes);
    Raii inflightGuard([inflightRequests, inflightBytes, requestBytes]() {
        inflightRequests.Dec();
        inflightBytes.Dec(requestBytes);
    });
    auto rc = Status::OK();
    if (requestKind == EventRequestKind::NOTIFICATION) {
        METRIC_TIMER(metrics::KvMetricId::COORDINATOR_WATCH_RPC_LATENCY);
        rc = brpcStub->HandleEvent(opts, req, rsp);
        if (rc.IsError()) {
            METRIC_INC(metrics::KvMetricId::COORDINATOR_WATCH_RPC_FAILURE_TOTAL);
        }
    } else {
        rc = brpcStub->HandleEvent(opts, req, rsp);
    }
    return rc;
}
}  // namespace coordinator
}  // namespace datasystem
