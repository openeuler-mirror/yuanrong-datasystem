/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Zmq Monitor
 */

#include "datasystem/common/rpc/zmq/zmq_monitor.h"

#include <utility>
#include "datasystem/common/rpc/zmq/zmq_stub_conn.h"

DS_DEFINE_int32(zmq_monitor_interval_s, 3, "Default interval to report the same event.");
namespace datasystem {
ZmqMonitor::ZmqMonitor(const std::shared_ptr<ZmqContext> &ctx)
    : interrupt_(false), initialize_(false), ctx_(ctx), mapChanged_(false)
{
}

ZmqMonitor::~ZmqMonitor()
{
    Stop();
    // If the zmq context is still present, proceed to close the sockets
    auto ctx = ctx_.lock();
    if (ctx == nullptr) {
        return;
    }
    for (auto &ele : monitorMap_) {
        ctx->CloseSocket(ele.first);
    }
}

Status ZmqMonitor::AddZmqSocket(ZmqSocket &sock)
{
    auto sockHandle = (void *)sock;
    auto gatewayId = sock.GetWorkerId();
    const std::string bindAddr = FormatString("inproc://%s", gatewayId);
    int rc = zmq_socket_monitor(sockHandle, bindAddr.data(), ZMQ_EVENT_ALL);
    if (rc == -1) {
        return ZmqSocketRef::ZmqErrnoToStatus(
            errno, FormatString("Add monitor for ZMQ socket with routing id %s failed", bindAddr));
    }
    auto ctx = ctx_.lock();
    CHECK_FAIL_RETURN_STATUS(ctx != nullptr, K_SHUTTING_DOWN, "No context");
    auto monHandle = ctx->CreateZmqSocket(ZmqSocketType::PAIR);
    CHECK_FAIL_RETURN_STATUS(
        monHandle != nullptr, K_RUNTIME_ERROR,
        FormatString("Cannot acquire resources to initialize monitor socket for ZMQ socket with routing id %s",
                     sock.GetWorkerId()));
    ZmqSocketRef mon(monHandle);
    Status status = mon.Connect(bindAddr);
    if (status.IsError()) {
        mon.Close();
        return status;
    }
    {
        std::lock_guard<std::mutex> lock(mux_);
        mapChanged_ = true;
        monitorMap_.emplace(std::make_pair(monHandle, std::make_pair(gatewayId, false)));
        cv_.notify_all();
    }
    return Status::OK();
}

void ZmqMonitor::Stop()
{
    interrupt_ = true;
    cv_.notify_all();
    if (thrd_.joinable()) {
        thrd_.join();
    }
}

void ZmqMonitor::OnEventConnected(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    LOG(INFO) << FormatString("Gateway %s successfully connected to remote peer %s fd %d", gatewayId, addr, t.value_);
}

void ZmqMonitor::OnEventConnectDelayed(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    (void)t;
    VLOG(RPC_LOG_LEVEL) << FormatString("Gateway %s connect to remote peer %s is pending", gatewayId, addr);
}

void ZmqMonitor::OnEventConnectRetried(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    VLOG(RPC_LOG_LEVEL) << FormatString(
        "Gateway %s connect to remote peer %s is unsuccessful. Retry in %d milliseconds", gatewayId, addr, t.value_);
}

void ZmqMonitor::OnEventListening(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    LOG(INFO) << FormatString("Gateway %s successfully bind to %s. fd %d", gatewayId, addr, t.value_);
}

void ZmqMonitor::OnEventBindFailed(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    LOG(ERROR) << FormatString("Gateway %s bind to %s failed. Errno %d. %s", gatewayId, addr, t.value_,
                               zmq_strerror(t.value_));
}

void ZmqMonitor::OnEventAccepted(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    LOG(INFO) << FormatString("Gateway %s connection accepted. End point %s. fd %d", gatewayId, addr, t.value_);
}

void ZmqMonitor::OnEventAcceptFailed(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    LOG(WARNING) << FormatString("Gateway %s connection rejected. End point %s. Errno %d. %s", gatewayId, addr,
                                 t.value_, zmq_strerror(t.value_));
}

void ZmqMonitor::OnEventClosed(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    VLOG(RPC_LOG_LEVEL) << FormatString("Gateway %s socket closed. fd %d. %s", gatewayId, t.value_, addr);
}

void ZmqMonitor::OnEventCloseFailed(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    (void)addr;
    LOG(WARNING) << FormatString("Gateway %s socket close failed. Errno %d. %s %s", gatewayId, t.value_,
                                 zmq_strerror(t.value_), addr);
}

void ZmqMonitor::OnEventDisconnected(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    LOG(WARNING) << FormatString("Gateway %s socket disconnected. fd %d. %s", gatewayId, t.value_, addr);
}

void ZmqMonitor::OnEventHandshakeFailedNoDetail(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    LOG(WARNING) << FormatString("Gateway %s handshake failed. Errno %d. %s %s", gatewayId, t.value_,
                                 zmq_strerror(t.value_), addr);
}

void ZmqMonitor::OnEventHandshakeFailedProtocol(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    LOG(WARNING) << FormatString("Gateway %s handshake protocol failed. Protocol %d. %s", gatewayId, t.value_, addr);
}

void ZmqMonitor::OnEventHandshakeFailedAuth(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    LOG(WARNING) << FormatString("Gateway %s handshake auth failed. Status code returned by ZAP handler %d. %s",
                                 gatewayId, t.value_, addr);
}

void ZmqMonitor::OnEventHandshakeSucceeded(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    (void)t;
    LOG(INFO) << FormatString("Gateway %s handshake succeeded. %s", gatewayId, addr);
}

void ZmqMonitor::OnEventUnknown(const Event &t, const std::string &addr, const std::string &gatewayId)
{
    LOG(WARNING) << FormatString("Gateway %s unknown event type %d. value %d. %s", gatewayId, t.event_, t.value_, addr);
}

Status ZmqMonitor::CheckEvent(void *handle, Event &t, std::string &addr)
{
    ZmqMessage eventMsg;
    ZmqMessage addrMsg;
    ZmqSocketRef sock(handle);
    RETURN_IF_NOT_OK(sock.RecvMsg(eventMsg, ZmqRecvFlags::NONE));
    RETURN_IF_NOT_OK(sock.RecvMsg(addrMsg, ZmqRecvFlags::NONE));
    CHECK_FAIL_RETURN_STATUS(eventMsg.Size() >= sizeof(decltype(t.event_)) + sizeof(decltype(t.value_)), K_INVALID,
                             FormatString("The msg size %zu is not match with the expected content.", eventMsg.Size()));
    auto *p = static_cast<uint8_t *>(eventMsg.Data());
    t.event_ = *(reinterpret_cast<uint16_t *>(p));
    p += sizeof(t.event_);
    t.value_ = *(reinterpret_cast<int32_t *>(p));
    addr = addrMsg.ToString();
    return Status::OK();
}

void ZmqMonitor::ReportErrorEvent(const Event &t, const std::string &addr, const std::string &gatewayId, bool &inError)
{
    // If we are already in a bad state. No need to report
    if (inError) {
        return;
    }
    switch (t.event_) {
        case ZMQ_EVENT_BIND_FAILED:
            OnEventBindFailed(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_CONNECT_RETRIED:
            OnEventConnectRetried(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_ACCEPT_FAILED:
            OnEventAcceptFailed(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_DISCONNECTED:
            OnEventDisconnected(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL:
            OnEventHandshakeFailedNoDetail(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL:
            OnEventHandshakeFailedProtocol(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_HANDSHAKE_FAILED_AUTH:
            OnEventHandshakeFailedAuth(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_CLOSE_FAILED:
            OnEventCloseFailed(t, addr, gatewayId);
            break;
        default:
            OnEventUnknown(t, addr, gatewayId);
            break;
    }
    inError = true;
}

void ZmqMonitor::ReportEvent(const Event &t, const std::string &addr, const std::string &gatewayId, bool &inError)
{
    switch (t.event_) {
        case ZMQ_EVENT_CONNECTED:
            OnEventConnected(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_CONNECT_DELAYED:
            OnEventConnectDelayed(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_LISTENING:
            OnEventListening(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_ACCEPTED:
            OnEventAccepted(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_CLOSED:
            OnEventClosed(t, addr, gatewayId);
            break;
        case ZMQ_EVENT_HANDSHAKE_SUCCEEDED:
            OnEventHandshakeSucceeded(t, addr, gatewayId);
            break;
        default:
            // The rest are considered in error state
            ReportErrorEvent(t, addr, gatewayId, inError);
            return;
    }
    // The default (error) case has already returned. If we get here, we assume we are back in
    // good state.
    inError = false;
}

void ZmqMonitor::SyncMonitorMap()
{
    std::lock_guard<std::mutex> xlock(mux_);
    auto ctx = ctx_.lock();
    auto it = monitorMap_.begin();
    while (it != monitorMap_.end()) {
        it->second.second = false;  // default
        auto handle = it->first;
        if (pendingClose_.count(handle) > 0) {
            if (ctx) {
                ctx->CloseSocket(handle);
            }
            it = monitorMap_.erase(it);
            continue;
        }
        // Check if the monitoring socket is in a bad state
        if (errorState_.count(handle) > 0) {
            it->second.second = true;
        }
        ++it;
    }
    pendingClose_.clear();
    errorState_.clear();
    mapChanged_ = true;
}

bool ZmqMonitor::InitPollItems(std::unique_ptr<zmq_pollitem_t[]> &items, size_t &sz)
{
    std::unique_lock<std::mutex> lock(mux_);
    auto notEmpty =
        cv_.wait_for(lock, std::chrono::milliseconds(RPC_POLL_TIME), [this]() { return !monitorMap_.empty(); });
    sz = monitorMap_.size();
    if (!notEmpty) {
        return false;
    }
    if (mapChanged_ || items == nullptr) {
        items.reset();
        shadowCopy_.clear();
        items = std::make_unique<zmq_pollitem_t[]>(sz);
        int i = 0;
        for (auto &ele : monitorMap_) {
            items[i++] = { .socket = ele.first, .fd = 0, .events = ZMQ_POLLIN, .revents = 0 };
            shadowCopy_.emplace(ele.first, ele.second);
        }
        mapChanged_ = false;
    }
    return true;
}

Status ZmqMonitor::Run(std::unique_ptr<zmq_pollitem_t[]> &items)
{
    size_t sz = 0;
    CHECK_FAIL_RETURN_STATUS(!interrupt_ && ZmqStubConnMgr::LoggingOn(), K_SHUTTING_DOWN, "Shutting down");
    CHECK_FAIL_RETURN_STATUS(InitPollItems(items, sz), K_TRY_AGAIN, "Nothing to monitor");
    auto n = zmq_poll(items.get(), static_cast<int>(sz), RPC_POLL_TIME);
    // Check again for shutdown after a long wait
    CHECK_FAIL_RETURN_STATUS(!interrupt_ && ZmqStubConnMgr::LoggingOn(), K_SHUTTING_DOWN, "Shutting down");
    CHECK_FAIL_RETURN_STATUS(n > 0, K_TRY_AGAIN, "No event");
    for (size_t j = 0; j < sz; ++j) {
        if (!(static_cast<unsigned int>(items[j].revents) & ZMQ_POLLIN)) {
            continue;
        }
        Event t{};
        std::string addr;
        auto handle = items[j].socket;
        auto &gatewayId = shadowCopy_[handle].first;
        auto &inErrorState = shadowCopy_[handle].second;
        Status rc = CheckEvent(handle, t, addr);
        if (rc.IsError()) {
            LOG(ERROR) << rc.ToString();
            continue;
        }
        if (t.event_ == ZMQ_EVENT_MONITOR_STOPPED) {
            if (ZmqStubConnMgr::LoggingOn()) {
                VLOG(RPC_LOG_LEVEL) << FormatString("Monitor event stopped for gateway %s. %s", gatewayId, addr);
            }
            pendingClose_.insert(items[j].socket);
            continue;
        }
        auto now = std::chrono::steady_clock::now();
        auto key = std::make_pair(gatewayId, t.event_);
        auto it = lastReportEvent_.find(key);
        if (it != lastReportEvent_.end()) {
            auto interval = std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count();
            if (interval < FLAGS_zmq_monitor_interval_s) {
                continue;
            }
        }
        lastReportEvent_[key] = now;
        // One more check
        CHECK_FAIL_RETURN_STATUS(!interrupt_ && ZmqStubConnMgr::LoggingOn(), K_SHUTTING_DOWN, "Shutting down");
        ReportEvent(t, addr, gatewayId, inErrorState);
        if (inErrorState) {
            errorState_.insert(items[j].socket);
        }
    }
    return Status::OK();
}

Status ZmqMonitor::StartMonitoring()
{
    bool expected = false;
    if (initialize_.compare_exchange_strong(expected, true)) {
        RETURN_IF_EXCEPTION_OCCURS(thrd_ = Thread([this]() {
                                       TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                                       VLOG(RPC_KEY_LOG_LEVEL) << FormatString("ZMQ monitor thread starts");
                                       std::unique_ptr<zmq_pollitem_t[]> items;
                                       Status rc;
                                       do {
                                           rc = Run(items);
                                           SyncMonitorMap();
                                       } while (rc.GetCode() != K_SHUTTING_DOWN);
                                   }));
        thrd_.set_name("ZmqMonitor");
    }
    return Status::OK();
}
}  // namespace datasystem
