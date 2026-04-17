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
 * Description: Stub connection.
 */
#include "datasystem/common/rpc/zmq/zmq_stub_conn.h"

#include <poll.h>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/zmq/rpc_service_method.h"
#include "datasystem/common/rpc/zmq/zmq_context.h"
#include "datasystem/common/rpc/zmq/zmq_msg_decoder.h"
#include "datasystem/common/rpc/zmq/zmq_stub.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
bool ZmqStubConnMgr::logging_ = true;
std::once_flag ZmqStubConnMgr::init_;
static const int K_ONE_SECOND = 1000;
static const int K_HALF_A_SECOND = 500;

template <typename T>
Status GetSharedPtrFromThis(T *p, std::shared_ptr<T> &o)
{
    CHECK_FAIL_RETURN_STATUS(p != nullptr, K_NOT_FOUND, "Null pointer");
    try {
        o = p->shared_from_this();
    } catch (const std::bad_weak_ptr &e) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("Stale pointer. %s", e.what()));
    }
    return Status::OK();
}

ZmqFrontend::ZmqFrontend(const std::shared_ptr<ZmqContext> &ctx, std::shared_ptr<RpcChannel> channel,
                         ZmqMsgMgr *backendMgr)
    : ctx_(ctx),
      channel_(std::move(channel)),
      liveness_(K_LIVENESS),
      maxLiveness_(K_LIVENESS),
      heartbeatInterval_(K_ONE_SECOND),
      backendMgr_(backendMgr),
      interrupt_(false),
      efd_(ZMQ_NO_FILE_FD),
      ffd_(ZMQ_NO_FILE_FD),
      heartBeatID_(GetStringUuid().substr(0, ZMQ_SHORT_UUID))
{
}

ZmqFrontend::~ZmqFrontend()
{
    if (efd_ > 0) {
        RETRY_ON_EINTR(close(efd_));
        efd_ = ZMQ_NO_FILE_FD;
    }
    // Stop the async thread in case initialization fails.
    Stop();
    if (msgQue_) {
        msgQue_.reset();
    }
}

Status ZmqFrontend::Init()
{
    msgQue_ = std::make_unique<Queue<ConnMsgFrames>>(RPC_HWM);
    auto efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(efd > 0, K_RUNTIME_ERROR,
                                         "Unable to create event fd. Errno: " + std::to_string(errno));
    efd_ = static_cast<eventfd_t>(efd);
    RETURN_IF_EXCEPTION_OCCURS(async_ = Thread([this]() { LOG_IF_ERROR(WorkerEntry(), ""); }));
    async_.set_name("ZmqFrontWorker");
    initWp_.Wait();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(initialized_, K_RUNTIME_ERROR, "Frontend socket fails to initialize.");
    return Status::OK();
}

void ZmqFrontend::Stop()
{
    bool expected = false;
    if (interrupt_.compare_exchange_strong(expected, true)) {
        if (async_.joinable()) {
            async_.join();
        }
    }
}

Status ZmqFrontend::RouteToZmqSocket(const MetaPb &meta, ZmqMsgFrames &&p)
{
    Status rc;
    // The internal method ZMQ_PAYLOAD_PUT_METHOD needs socket support.
    // Technically ZMQ_PAYLOAD_HANDSHAKE_METHOD can also go through ZMQ
    // as long as we have V2MTP connection. But for now, let's raise an error.
    CHECK_FAIL_RETURN_STATUS(
        meta.method_index() != ZMQ_PAYLOAD_PUT_METHOD && meta.method_index() != ZMQ_PAYLOAD_HANDSHAKE_METHOD,
        K_RUNTIME_ERROR, "Parallel payload is not supported");
    RETURN_IF_NOT_OK(PushFrontProtobufToFrames(meta, p));
    VLOG(RPC_LOG_LEVEL) << FormatString(
        "(Gateway %s) Sending out (zmq) request for sender %s to %s for service %s method %d\n[meta]\n%s",
        GetGatewayId(), meta.client_id(), channel_->GetZmqEndPoint(), meta.svc_name(), meta.method_index(),
        LogHelper::IgnoreSensitive(meta));
    rc = frontend_->SendAllFrames(p, meta.method_index() < 0 ? ZmqSendFlags::DONTWAIT : ZmqSendFlags::NONE);
    return rc;
}

Status ZmqFrontend::RouteToUnixSocket(const std::shared_ptr<SockConnEntry> &connInfo, const MetaPb &meta,
                                      ZmqMsgFrames &&frames)
{
    // Hold a read lock here to block SockConnEntry::SetSvcFdInvalid
    ReadLock rlock(&connInfo->mux_);
    // We have a pool of socket connection plus the base zmq connection for sending out the request.
    // Note that for stream rpc, we can still make use of this pool because the server side logic will
    // do a sort and preserve the order.
    const auto method = meta.method_index();
    bool forceV2 = (method == ZMQ_PAYLOAD_PUT_METHOD || method == ZMQ_PAYLOAD_HANDSHAKE_METHOD);
    auto tcp = !connInfo->uds_;
    CHECK_FAIL_RETURN_STATUS(!forceV2 || tcp, K_NOT_READY, "ZMQ_PAYLOAD_PUT_METHOD is for tcp/ip only");
    std::shared_ptr<SockConnEntry::FdConn> fdConn;
    connInfo->GetNextFd(forceV2, fdConn);
    if (fdConn == nullptr || fdConn->fd_ == ZMQ_NO_FILE_FD) {
        return RouteToZmqSocket(meta, std::move(frames));
    }
    auto fd = fdConn->fd_;
    RETURN_IF_NOT_OK(PushFrontProtobufToFrames(meta, frames));
    // We use the same gateway id as the return address. The server side has the choice
    // to return the reply using this socket connection or route through zmq proxy
    RETURN_IF_NOT_OK(PushFrontStringToFrames(GetGatewayId(), frames));
    VLOG(RPC_LOG_LEVEL) << FormatString(
        "(Gateway %s) Sending out (socket) request for sender %s to %s for service %s method %d. fd %d. uds %s. tcp %s",
        GetGatewayId(), meta.client_id(), channel_->GetZmqEndPoint(), meta.svc_name(), meta.method_index(), fd,
        (fd > 0 && !tcp) ? "true" : "false", tcp ? "true" : "false");
    // It must match the server MTP protocol. V2 supports more customization like
    // writing into user's provided buffer. Currently, we won't support sending
    // payload using V2 due to performance concern.
    EventType type;
    if (forceV2 || (tcp && meta.payload_index() == ZMQ_INVALID_PAYLOAD_INX)) {
        type = V2MTP;
    } else {
        // Earlier version of local client is using V1. Switching
        // to different protocol will break rolling upgrade.
        type = V1MTP;
    }
    WriteLock lock(fdConn->outMux_.get());
    fdConn->outMsgQueue_->emplace_back(type, std::move(frames));
    RETURN_IF_NOT_OK(fdConn->outPoller_->SetPollOut(fdConn->outHandle_));
    return Status::OK();
}

static uint64_t CurrentTimeMs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

Status ZmqFrontend::BackendToFrontend()
{
    auto func = [this](SockConnEntry *cInfo, MetaPb &meta, ZmqMsgFrames &frames) {
        Status rc;
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(meta.trace_id());
        PerfPoint::RecordElapsed(PerfKey::ZMQ_STUB_BACK_TO_FRONT, GetLapTime(meta, "ZMQ_STUB_BACK_TO_FRONT"));
        // First take care of the easy one that must go through the zmq dealer socket.
        // Few internal methods (for uds/tcp handshake) must go through this code path.
        // Heartbeat method doesn't send out from here. Also, we can't support ZMQ_PAYLOAD_HANDSHAKE_METHOD
        // unless we have a V2MTP connection.
        const auto method = meta.method_index();
        CHECK_FAIL_RETURN_STATUS(cInfo != nullptr || method != ZMQ_PAYLOAD_HANDSHAKE_METHOD, K_NOT_READY,
                                 "V2MTP is needed for ZMQ_PAYLOAD_HANDSHAKE_METHOD");
        if (cInfo == nullptr || method == ZMQ_SOCKPATH_METHOD || method == ZMQ_TCP_DIRECT_METHOD
            || method == ZMQ_PAYLOAD_GET_METHOD || method == ZMQ_STREAM_WORKER_METHOD) {
            return RouteToZmqSocket(meta, std::move(frames));
        }
        // If the socket connection info is no longer valid, fall back to ZMQ
        std::shared_ptr<SockConnEntry> connInfo;
        rc = GetSharedPtrFromThis(cInfo, connInfo);
        if (rc.IsError() || connInfo->WaitForConnected(RPC_POLL_TIME).IsError()) {
            return RouteToZmqSocket(meta, std::move(frames));
        }
        // At this stage, we will use the socket pool.
        return RouteToUnixSocket(connInfo, meta, std::move(frames));
    };
    eventfd_t n;
    eventfd_read(efd_, &n);
    if (n > 1) {
        eventfd_write(efd_, n - 1);
    }
    Timer t;
    ConnMsgFrames que;
    RETURN_IF_NOT_OK(msgQue_->Poll(&que, RPC_POLL_TIME));
    frontendQueLastHandledTimeMs_ = CurrentTimeMs();
    auto cInfo = que.first;
    auto meta = que.second.first;
    auto &frames = que.second.second;
    Status rc = func(cInfo, meta, frames);
    if (rc.IsError()) {
        auto elapsed = t.ElapsedSecond();
        auto msg = FormatString("Message que %s service %s method %d, frontend gateway %s. Elapsed: [%.6lf]s",
                                meta.client_id(), meta.svc_name(), meta.method_index(), GetGatewayId(), elapsed);
        LOG(INFO) << FormatString("%s. %s", rc.ToString(), msg);
        RETURN_IF_NOT_OK(ZmqBaseStubConn::ReportErrorToClient(
            meta.client_id(), meta, K_RPC_UNAVAILABLE,
            FormatString("[RPC_SERVICE_UNAVAILABLE] The service is currently unavailable! %s", msg), backendMgr_));
        return rc;
    }
    return Status::OK();
}

Status ZmqFrontend::ZmqSocketToBackend()
{
    ZmqMsgFrames frames;
    RETURN_IF_NOT_OK(frontend_->GetAllFrames(frames, ZmqRecvFlags::DONTWAIT));
    ResetLiveness();
    std::string receiver = ZmqMessageToString(frames.front());
    frames.pop_front();
    // Next one is the original MetaPb.
    CHECK_FAIL_RETURN_STATUS(!frames.empty(), K_RUNTIME_ERROR, "Incomplete frame");
    ZmqMessage metaHdr = std::move(frames.front());
    frames.pop_front();
    MetaPb meta;
    RETURN_IF_NOT_OK(ParseFromZmqMessage(metaHdr, meta));
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(meta.trace_id());
    if (receiver != heartBeatID_) {
        INJECT_POINT("ZmqFrontend.CorruptMetaPb", [&meta]() {
            meta.Clear();
            return Status::OK();
        });
        CHECK_FAIL_RETURN_STATUS(meta.ticks_size() > 0, K_RUNTIME_ERROR,
                                 FormatString("Incomplete MetaPb for receiver %s:\n%s", receiver, meta.DebugString()));
        PerfPoint::RecordElapsed(PerfKey::ZMQ_NETWORK_TRANSFER_STUB_TCP,
                                 GetLapTime(meta, "ZMQ_NETWORK_TRANSFER (ZMQ)"));
        auto p = std::make_pair(meta, std::move(frames));
        VLOG(RPC_LOG_LEVEL) << FormatString("Receiving reply for sender %s gateway %s client %s service '%s' method %d",
                                            receiver, meta.gateway_id(), meta.client_id(), meta.svc_name(),
                                            meta.method_index());
        RETURN_IF_NOT_OK(backendMgr_->SendMsg(receiver, p, QUE_NO_TIMEOUT));
    }
    return Status::OK();
}

Status ZmqFrontend::SendHeartBeats()
{
    // One more check to ensure we can send out the message without any blocking
    unsigned int events = static_cast<unsigned int>(frontend_->Get(sockopt::ZmqEvents, 0));
    CHECK_FAIL_RETURN_STATUS(events & ZMQ_POLLOUT, K_RPC_UNAVAILABLE, "[TCP_NETWORK_UNREACHABLE] Network unreachable");
    MetaPb meta = CreateMetaData("", ZMQ_HEARTBEAT_METHOD, ZMQ_INVALID_PAYLOAD_INX, heartBeatID_);
    ZmqMsgFrames p;
    RETURN_IF_NOT_OK(PushFrontProtobufToFrames(meta, p));
    RETURN_IF_NOT_OK(frontend_->SendAllFrames(p, ZmqSendFlags::DONTWAIT));
    return Status::OK();
}

Status ZmqFrontend::HandleEvent(int timeout)
{
    bool gotEvent = false;
    static const int K_QUE = 1;
    static const int K_SIZE = 2;
    struct pollfd item[K_SIZE] = { { .fd = ffd_, .events = POLLIN, .revents = 0 },
                                   { .fd = static_cast<int>(efd_), .events = POLLIN, .revents = 0 } };
    auto n = poll(item, K_SIZE, timeout);
    if (n < 0) {
        LOG(ERROR) << FormatString("Error to poll for %s. Errno = %d", channel_->GetZmqEndPoint(), errno);
    }

    if (interrupt_) {
        RETURN_STATUS(K_SHUTTING_DOWN, "Shutting down");
    }

    // Note that logging this does not mean there is an error, it only indicates there can be a problem if
    // the frontend message queue was not handled for some time, while the data flow is considered rather busy.
    if (!msgQue_->Empty()) {
        const uint64_t LOG_THRESHOLD_TIME_MS = 10000;
        uint64_t elapsedTimeMs = CurrentTimeMs() - frontendQueLastHandledTimeMs_;
        VLOG_IF(RPC_KEY_LOG_LEVEL, elapsedTimeMs >= LOG_THRESHOLD_TIME_MS) << FormatString(
            "The frontend message queue has not been handled for %zu ms, with %zu entries in the queue.", elapsedTimeMs,
            msgQue_->Size());
    }

    // As stated in the ZMQ manual, we need to check ZMQ_EVENTS
    unsigned int events = static_cast<unsigned int>(frontend_->GetEvents());
    if (events & ZMQ_POLLIN) {
        gotEvent = true;
        RETURN_IF_NOT_OK(ZmqSocketToBackend());
    }

    if (static_cast<unsigned int>(item[K_QUE].revents) & POLLIN) {
        gotEvent = true;
        RETURN_IF_NOT_OK(BackendToFrontend());
    }
    return gotEvent ? Status::OK() : Status(StatusCode::K_NOT_FOUND, "Idle");
}

Status ZmqFrontend::WorkerEntry()
{
    VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Async thread for ZMQ end point %s starts up", channel_->GetZmqEndPoint());
    // Initialize frontend socket in the thread that will use it.
    std::unique_ptr<Raii> initializeGuard = std::make_unique<Raii>([this]() { initWp_.Set(); });
    RETURN_IF_NOT_OK(InitFrontend(ctx_, channel_, frontend_));
    frontendQueLastHandledTimeMs_ = CurrentTimeMs();
    Raii frontendCleanGuard([this]() {
        if (frontend_) {
            frontend_->Close();
            frontend_.reset();
        }
    });
    ffd_ = frontend_->GetEventFd();
    LOG(INFO) << FormatString("New gateway created %s", GetGatewayId());
    INJECT_POINT("ZmqFrontend.WorkerEntry.FailInit");
    initialized_ = true;
    initializeGuard.reset();
    Timer t;
    Status rc;
    int timeout = 0;
    while (!interrupt_) {
        rc = HandleEvent(timeout);
        if (rc.GetCode() == K_SHUTTING_DOWN) {
            break;
        }

        // If idle, wait for a full 100ms next round
        if (rc.GetCode() == K_NOT_FOUND) {
            timeout = RPC_POLL_TIME;
            // keep the connection alive by sending a heartbeat to the peer
            if (t.ElapsedMilliSecond() < heartbeatInterval_) {
                continue;
            }
        } else {
            timeout = 0;
            if (rc.GetCode() == K_TRY_AGAIN) {
                // Set liveness to 0 to trigger ZmqFrontend recreation
                VLOG(RPC_LOG_LEVEL) << "Received EAGAIN at HandleEvent, force ZmqFrontend recreation.";
                liveness_ = 0;
            } else {
                continue;
            }
        }

        // Unconditionally decrement the liveness. It will be
        // reset if we get any response from the peer
        if (liveness_ > 0) {
            auto val = liveness_.load();
            liveness_.compare_exchange_strong(val, val - 1);
        }
        (void)SendHeartBeats();
        t.Reset();

        // Create a new one if peer appears to be dead.
        if (liveness_ == 0) {
            std::shared_ptr<ZmqSocket> sock;
            Raii cleanGuard([&sock]() {
                if (sock) {
                    sock->Close();
                    sock.reset();
                }
            });
            rc = InitFrontend(ctx_, channel_, sock);
            // If create a new one is not successful, keep the old one and try again next time.
            if (rc.IsOk()) {
                std::swap(frontend_, sock);
                ResetLiveness();
                ffd_ = frontend_->GetEventFd();
                LOG(INFO) << FormatString("New gateway created %s", GetGatewayId());
                METRIC_INC(metrics::KvMetricId::ZMQ_GATEWAY_RECREATE_TOTAL);
            }
        }
    }
    VLOG_IF(RPC_KEY_LOG_LEVEL, ZmqStubConnMgr::LoggingOn())
        << FormatString("Async thread for ZMQ end point %s shutdown", channel_->GetZmqEndPoint());
    return Status::OK();
}

void ZmqFrontend::InterruptAll()
{
    if (ZmqStubConnMgr::LoggingOn()) {
        LOG(INFO) << "Zmq frontend interrupted to shut down";
    }
    Stop();
}

Status ZmqFrontend::SendMsg(SockConnEntry *cInfo, ZmqMetaMsgFrames &&que, int timeout)
{
    ConnMsgFrames ele = std::make_pair(cInfo, std::move(que));
    RETURN_IF_NOT_OK(msgQue_->Send(std::move(ele), timeout));
    eventfd_write(efd_, 1);
    return Status::OK();
}

bool ZmqFrontend::IsPeerAlive(uint32_t threshold)
{
    ReadLock rLock(&livenessMux_);
    return (maxLiveness_ - liveness_) <= threshold;
}

Status ZmqFrontend::InitFrontend(std::weak_ptr<ZmqContext> context, const std::shared_ptr<RpcChannel> &channel,
                                 std::shared_ptr<ZmqSocket> &out) const
{
    auto ctx = context.lock();
    CHECK_FAIL_RETURN_STATUS(ctx != nullptr, K_SHUTTING_DOWN, "No context");
    auto cred = channel->GetCredential();
    RpcOptions opts;
    opts.SetHWM(0);
    // We don't wait for the full 60s before we return K_TRY_AGAIN.
    opts.SetTimeout(STUB_FRONTEND_TIMEOUT);
    auto frontend = std::make_shared<ZmqSocket>(ctx, ZmqSocketType::DEALER);
    CHECK_FAIL_RETURN_STATUS(frontend->IsValid(), StatusCode::K_RPC_CANCELLED,
                             "Cannot acquire resources to initialize client");
    RETURN_IF_NOT_OK(frontend->UpdateOptions(opts));
    // Set up credential
    frontend->SetClientCredential(cred);
    // Initiate a connection to reduce the first connection cost.
    frontend->Set(sockopt::ZmqProbeRouter, true);
    RETURN_IF_NOT_OK(frontend->Connect(*channel));
    out = std::move(frontend);
    return Status::OK();
}

RpcCredential ZmqFrontend::GetCredential() const
{
    return channel_->GetCredential();
}

std::string ZmqFrontend::GetGatewayId() const
{
    return frontend_->GetWorkerId();
}

std::string ZmqFrontend::GetZmqEndPoint() const
{
    return channel_->GetZmqEndPoint();
}

void ZmqFrontend::UpdateLiveness(int32_t timeoutMs)
{
    if (timeoutMs <= 0) {
        return;
    }
    const int32_t minLiveness = 3;
    const int32_t minInterval = 500;     // 500ms.
    const int32_t maxInterval = 30'000;  // 30s.
    auto interval = std::max(minInterval, std::min(timeoutMs / (minLiveness + 1), maxInterval));
    auto realTimeoutMs = std::max<int32_t>(timeoutMs - interval, timeoutMs * 0.9);
    uint32_t newLiveness = std::max<int32_t>(realTimeoutMs / interval, minLiveness);
    WriteLock lock(&livenessMux_);
    if (newLiveness < maxLiveness_) {
        LOG(INFO) << "update liveness from " << maxLiveness_ << " to " << newLiveness << " heartbeatInterval from "
                  << heartbeatInterval_ << " to " << interval;
        maxLiveness_ = newLiveness;
        heartbeatInterval_ = interval;
        liveness_ = std::min<uint32_t>(newLiveness, liveness_);
    }
}

void ZmqFrontend::ResetLiveness()
{
    ReadLock rLock(&livenessMux_);
    liveness_ = maxLiveness_.load();
}

void ZmqStubConnMgrImpl::AutoReconnect(std::shared_ptr<SockConnEntry> &connInfo)
{
    // Look for all the stubs reference the same info. If found, drive a reconnection
    bool found = false;
    ReadLock rLock(&stubMux_);
    for (auto &e : stubInfos_) {
        auto &info = e.second;
        auto p = info->connInfo_.lock();
        if (p != nullptr && p.get() == connInfo.get()) {
            VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Try to reconnect to %s for service %s for stub id (0x%x)",
                                                    info->channel_->GetZmqEndPoint(), info->svcName_, info->stubId_);
            if (!found) {
                found = true;
                // We will drive a reconnection and bump up the retryCount to bypass the cache look up.
                connInfo->retryCount_++;
                connInfo->lastFailedAttempt = std::chrono::steady_clock::now();
            }
            sockConnHelper_->AddStubToConnList(info);
        }
    }
}

Status ZmqSockConnHelper::StartThreads()
{
    static constexpr int MAX_CONN_THREADS = 1;
    // UDS threads are spawn on demand to save resources.
    // Step 1. Lock the mutex to check the state.
    std::unique_lock<std::mutex> lock(connThreadsMux_);
    RETURN_OK_IF_TRUE(connState_ == CONN_THRD_RUNNING);
    // Step 2. Is the current state in progress ? If yes,
    // wait on the cv.
    auto waitForComplete = [this, &lock]() {
        auto &conn = ZmqStubConnMgr::Instance()->impl_;
        while (connState_ == CONN_THRD_INPROGESS) {
            connThreadsCv_.wait_for(lock, std::chrono::milliseconds(STUB_FRONTEND_TIMEOUT),
                                    [this]() { return connState_ == CONN_THRD_RUNNING; });
            CHECK_FAIL_RETURN_STATUS(!conn->interrupt_, K_SHUTTING_DOWN, "Shutting down");
        }
        return Status::OK();
    };
    RETURN_IF_NOT_OK(waitForComplete());
    RETURN_OK_IF_TRUE(connState_ == CONN_THRD_RUNNING);
    // Step 3. We're the first to start the uds threads.
    // Change the state to in progress and create a thread pool
    RETURN_IF_EXCEPTION_OCCURS(thrd_ = std::make_unique<ThreadPool>(MAX_CONN_THREADS, 0, "ZmqHandleConnect"));
    connState_ = CONN_THRD_INPROGESS;
    // Step 4. Start the thread(s) in the background.
    auto incAndNotify = [this]() {
        std::unique_lock<std::mutex> lock(connThreadsMux_);
        if (++numThreads_ == MAX_CONN_THREADS) {
            connState_ = CONN_THRD_RUNNING;
            connThreadsCv_.notify_all();
        }
        lock.unlock();
    };
    // One for auto re-connect.
    auto traceId = Trace::Instance().GetTraceID();
    thrd_->Execute([this, &incAndNotify, traceId = std::move(traceId)]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        incAndNotify();
        HandleConnectList();
    });
    LOG(INFO) << "Conn helper started";
    // Step 5. Let go of the lock and wait for the threads to post
    return waitForComplete();
}

Status ZmqSockConnHelper::GetEndPoint(const ReconnectInfo &cInfo, std::string &path, int timeout)
{
    auto connInfo = cInfo.first;
    auto info = cInfo.second;
    // This call is always run from the backend, driven by the stub.
    if (connInfo->retryCount_ == 0) {
        // First time, look it up in the cached map. If the return value is stale, we will
        // get error on connect and retry again with retryCount_ incremented. This time
        // we will drive a rpc call to get the true value.
        path = connInfo->endPoint_;
        RETURN_OK_IF_TRUE(!path.empty());
    }
    ZmqMsgQueRef mQue;
    const std::string suffix = ZmqStubConnMgrImpl::EncodeQueName(info);
    RETURN_IF_NOT_OK(connInfo->BackendMgr()->CreateMsgQ(mQue, RpcOptions(), suffix));
    ZmqMetaMsgFrames p;
    p.first = CreateMetaData(connInfo->svcName_, connInfo->uds_ ? ZMQ_SOCKPATH_METHOD : ZMQ_TCP_DIRECT_METHOD,
                             ZMQ_INVALID_PAYLOAD_INX, mQue.GetId());
    MetaPb rq;  // Not really used but we need to send one.
    RETURN_IF_NOT_OK(PushBackProtobufToFrames(rq, p.second));
    RETURN_IF_NOT_OK(mQue.SendMsg(p));
    // Wait for reply.
    ZmqMetaMsgFrames reply;
    Status rc = mQue.ReceiveMsg(reply, timeout);
    if (rc.GetCode() == K_TRY_AGAIN) {
        rc = Status(StatusCode::K_RPC_UNAVAILABLE,
                    FormatString("[RPC_RECV_TIMEOUT] Remote host %s is not available",
                                 info->channel_->GetZmqEndPoint()));
    }
    RETURN_IF_NOT_OK(rc);
    PerfPoint::RecordElapsed(PerfKey::ZMQ_STUB_FRONT_TO_BACK, GetLapTime(reply.first, "ZMQ_STUB_FRONT_TO_BACK"));
    ZmqMessage replyMsg;
    RETURN_IF_NOT_OK(AckRequest(reply.second, replyMsg));
    auto curPath = ZmqMessageToString(replyMsg);
    CHECK_FAIL_RETURN_STATUS(!curPath.empty(), K_NOT_FOUND,
                             FormatString("Channel %s service '%s' doesn't support uds/tcp direct",
                                          info->channel_->GetZmqEndPoint(), connInfo->svcName_));
    std::string zmqEndPoint = FormatString("%s%s", connInfo->uds_ ? "ipc://" : "tcp://",
                                           connInfo->uds_ ? FormatString("%s/%s", curPath, info->sockName_) : curPath);
    // Check again if someone has already updated the cache
    connInfo->endPoint_ = zmqEndPoint;
    path = std::move(zmqEndPoint);
    VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Channel %s service '%s' end point: %s", info->channel_->GetZmqEndPoint(),
                                            connInfo->svcName_, path);
    return Status::OK();
}

Status ZmqSockConnHelper::SetCacheConn(std::shared_ptr<SockConnEntry::FdConn> &fdConn, const ReconnectInfo &cInfo,
                                       UnixSockFd &sock)
{
    auto connInfo = cInfo.first;
    // For security reason, the uds path is not known until we initiate handshake with
    // the corresponding service using tcp/ip.
    // We want to shorten the timeout because this code is used for re-connection.
    // We don't want to wait too long.
    const int timeout = K_HALF_A_SECOND;
    std::string endPoint;
    RETURN_IF_NOT_OK(GetEndPoint(cInfo, endPoint, timeout));
    // If we have cached it, just drive a direct connect to see if it is still valid.
    RETURN_IF_NOT_OK(sock.Connect(endPoint));
    RETURN_IF_NOT_OK(sock.SetTimeout(STUB_FRONTEND_TIMEOUT));
    auto fd = sock.GetFd();
    RETURN_IF_NOT_OK(connInfo->AddEvent(fd, fdConn));
    return Status::OK();
}

Status ZmqSockConnHelper::StubConnect(const ReconnectInfo &cInfo, UnixSockFd &sock)
{
    INJECT_POINT("ZmqSockConnHelper.StubConnect");
    auto connInfo = cInfo.first;
    auto info = cInfo.second;
    for (auto &fdConn : connInfo->pools_) {
        WriteLock xlock(&connInfo->mux_);
        auto fd = fdConn->fd_;
        if (fd == ZMQ_NO_FILE_FD) {
            xlock.UnlockIfLocked();
            RETURN_IF_NOT_OK(SetCacheConn(fdConn, cInfo, sock));
            fd = sock.GetFd();
        }
        VLOG(RPC_KEY_LOG_LEVEL) << FormatString(
            "Stub (0x%x) service '%s' channel %s fd %d (%s) added to event loop. retryCount = %d", info->stubId_,
            connInfo->svcName_, info->channel_->GetZmqEndPoint(), fd, connInfo->uds_ ? "uds" : "tcp",
            connInfo->retryCount_);
        sock = UnixSockFd();
    }
    return Status::OK();
}

void ZmqSockConnHelper::HandleConnectListAfterCheck(std::shared_ptr<StubInfo> &info,
                                                    std::shared_ptr<SockConnEntry> &connInfo,
                                                    std::unique_lock<std::mutex> &lock, UnixSockFd &sock,
                                                    ReconnectInfo &cInfo)
{
    VLOG_IF(RPC_LOG_LEVEL, connInfo->retryCount_ == 0)
        << FormatString("Stub (0x%x) attempt to (%s) connect for service '%s' channel %s", info->stubId_,
                        info->uds_ ? "uds" : "tcp", info->svcName_, info->channel_->GetZmqEndPoint());
    // Synchronize with ZmqStub::CreateMsgQ. Stub wants to connect must wait until we are done.
    connInfo->wp_.Clear();
    connInfo->connInProgress_ = true;
    connInfo->connectRc_ = StubConnect(cInfo, sock);
    bool udsDisabled = (connInfo->connectRc_.GetCode() == K_NOT_FOUND);
    // If uds is not supported, just fall back to zmq.
    if (udsDisabled) {
        LOG(INFO) << FormatString("Channel %s service '%s' does not support uds/tcp, zmq is used for connection.",
                                  info->channel_->GetZmqEndPoint(), info->svcName_);
    }
    connInfo->connInProgress_ = false;
    if (connInfo->connectRc_.IsOk() || udsDisabled) {
        connInfo->wp_.Set();
    }
    if (udsDisabled) {
        return;
    }
    if (connInfo->connectRc_.IsError()) {
        connInfo->retryCount_++;
        connInfo->lastFailedAttempt = std::chrono::steady_clock::now();
        lock.lock();
        reconnectList_.push_back(info);
        return;
    }
}

void ZmqSockConnHelper::HandleConnectList()
{
    VLOG(RPC_KEY_LOG_LEVEL) << "HandleConnectList thread starts up";
    UnixSockFd sock;
    auto &conn = ZmqStubConnMgr::Instance()->impl_;
    while (!conn->interrupt_) {
        std::unique_lock<std::mutex> lock(reconnectMux_);
        reconnectCv_.wait_for(lock, std::chrono::milliseconds(RPC_POLL_TIME),
                              [this, &conn]() { return !reconnectList_.empty() || conn->interrupt_; });
        if (conn->interrupt_) {
            break;
        }
        if (reconnectList_.empty()) {
            continue;
        }
        std::weak_ptr<StubInfo> ptr = reconnectList_.front();
        reconnectList_.pop_front();
        // Check if this id is still valid in case the stub is already
        // out of scope.
        auto info = ptr.lock();
        if (info == nullptr) {
            continue;
        }
        ReconnectInfo cInfo = std::make_pair(info->connInfo_.lock(), info);
        auto connInfo = cInfo.first;
        if (connInfo == nullptr) {
            continue;
        }
        // If we have tried this already, wait a bit and give other a chance.
        if (connInfo->retryCount_ > 0) {
            auto now = std::chrono::steady_clock::now();
            int64_t ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - connInfo->lastFailedAttempt).count();
            if (ms < K_ONE_SECOND) {
                reconnectList_.push_back(ptr);
                continue;
            }
        }
        // Let go of the mutex because we can be blocked on connect.
        lock.unlock();
        HandleConnectListAfterCheck(info, connInfo, lock, sock, cInfo);
    }
    if (ZmqStubConnMgr::LoggingOn()) {
        VLOG(RPC_LOG_LEVEL) << "HandleConnectList thread shutdown";
    }
}

void ZmqSockConnHelper::AddStubToConnList(const std::shared_ptr<StubInfo> &info)
{
    // Add the stub to the list to be reconnected.
    std::unique_lock<std::mutex> lck(reconnectMux_);
    reconnectList_.emplace_back(info);
    reconnectCv_.notify_all();
    VLOG(RPC_LOG_LEVEL) << FormatString("Adding Stub (0x%x) service '%s' to connect list", info->stubId_,
                                        info->svcName_);
}

Status ZmqSockConnHelper::RegisterStub(const std::shared_ptr<StubInfo> &info, const ZmqConnKey &key,
                                       const std::shared_ptr<ZmqBaseStubConn> &conn,
                                       std::shared_ptr<SockConnEntry> &sockConn, bool upsert)
{
    // If we haven't started the uds threads, do it now.
    RETURN_IF_NOT_OK(StartThreads());
    WriteLock lock(&connMapMux_);
    ZmqConnKey connKey = key;
    // Change the key because socket connects at the service level.
    connKey.svcNameAsKey_ = true;
    std::shared_ptr<SockConnEntry> connEntry;
    auto it = connMap_.find(connKey);
    if (it != connMap_.end()) {
        auto sc = it->second.lock();
        // The row in this table can be stale, delete it if asked to do so.
        if (sc == nullptr || upsert) {
            VLOG(RPC_LOG_LEVEL) << FormatString("Erasing old key entry for service '%s' for channel %s", info->svcName_,
                                                info->channel_->GetZmqEndPoint());
            connMap_.erase(it);
        }
        connEntry = sc;
    }
    if (connEntry == nullptr) {
        VLOG(RPC_LOG_LEVEL) << FormatString("Adding new key entry for service '%s' for channel %s", info->svcName_,
                                            info->channel_->GetZmqEndPoint());
        connEntry = std::make_shared<SockConnEntry>(conn->BackendMgr());
        connEntry->svcName_ = info->svcName_;
        connEntry->uds_ = info->uds_;
        connMap_.emplace(connKey, connEntry);
    } else {
        // Make sure the backend match
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connEntry->BackendMgr() == conn->BackendMgr(), K_RUNTIME_ERROR,
                                             "Message queue doesn't match");
    }
    sockConn = connEntry;
    info->connInfo_ = connEntry;
    // We count the zmq connection as one of the parallel connection.
    // So we should subtract one from the parallelism
    auto numConn = std::max<size_t>(info->connPoolSz_ - 1, 1);
    auto cur = connEntry->pools_.size();
    for (size_t i = cur; i < numConn; ++i) {
        auto tempFdConn = std::make_shared<SockConnEntry::FdConn>();
        tempFdConn->outMux_ = std::make_unique<WriterPrefRWLock>();
        tempFdConn->fd_ = ZMQ_NO_FILE_FD;
        tempFdConn->inHandle_ = nullptr;
        tempFdConn->outHandle_ = nullptr;
        tempFdConn->decoder_ = nullptr;
        tempFdConn->encoder_ = nullptr;
        tempFdConn->inPoller_ = ZmqStubConnMgr::Instance()->GetPoller();
        tempFdConn->outPoller_ = ZmqStubConnMgr::Instance()->GetPoller();
        tempFdConn->outMsgQueue_ = std::make_unique<std::deque<std::pair<EventType, ZmqMsgFrames>>>();
        connEntry->pools_.push_back(tempFdConn);
    }
    connEntry->connInProgress_ = true;
    AddStubToConnList(info);
    return Status::OK();
}

ZmqSockConnHelper::ZmqSockConnHelper() : numThreads_(0), connState_(CONN_THRD_NONE)
{
}

ZmqSockConnHelper::~ZmqSockConnHelper()
{
    WriteLock lock(&connMapMux_);
    connMap_.clear();
}

void ZmqSockConnHelper::InterruptAll()
{
    reconnectCv_.notify_all();
    if (thrd_) {
        thrd_.reset();
    }
}

Status ZmqBaseStubConn::WaitForConnect(const std::shared_ptr<StubInfo> &info, int64_t timeout)
{
    CHECK_FAIL_RETURN_STATUS(!interrupt_, K_SHUTTING_DOWN, "Shutting down");
    RETURN_OK_IF_TRUE(!(info->uds_ || info->tcpDirect_));
    auto connInfo = info->connInfo_.lock();
    CHECK_FAIL_RETURN_STATUS(connInfo != nullptr, K_RUNTIME_ERROR,
                             FormatString("conn info of %s not found", info->svcName_));
    Timer t;
    Status rc;
    int64_t remaining = timeout;
    do {
        rc = connInfo->WaitForConnected(remaining);
        // Check for the case we fall back to zmq
        RETURN_OK_IF_TRUE(rc.GetCode() == K_NOT_FOUND);
        remaining = timeout - static_cast<int64_t>(t.ElapsedMilliSecond());
        INJECT_POINT("ZmqBaseStubConn.WaitForConnect");
    } while (rc.IsError() && remaining > 0);
    workerOperationTimeCost.Append("wait for connected", static_cast<uint64_t>(t.ElapsedMilliSecond()));
    return rc;
}

Status ZmqBaseStubConn::Init()
{
    return Status::OK();
}

void ZmqBaseStubConn::Shutdown()
{
    interrupt_ = true;
}

Status ZmqBaseStubConn::ReportErrorToClient(const std::string &qID, const MetaPb &meta, const StatusCode code,
                                            const std::string &msg, ZmqMsgMgr *backendMgr)
{
    ZmqMsgFrames reply;
    Status t = Status(code, msg);
    reply.push_back(StatusToZmqMessage(t));
    auto p = std::make_pair(meta, std::move(reply));
    RETURN_IF_NOT_OK(backendMgr->SendMsg(qID, p, QUE_NO_TIMEOUT));
    return Status::OK();
}

ZmqBaseStubConn::ZmqBaseStubConn(const ZmqConnKey &key, ZmqMsgMgr *backend)
    : interrupt_(false), key_(key), backendMgr_(backend)
{
}

ZmqStubConn::ZmqStubConn(const ZmqConnKey &key, ZmqMsgMgr *backend, std::shared_ptr<ZmqFrontend> frontend)
    : ZmqBaseStubConn(key, backend), frontend_(std::move(frontend))
{
}

ZmqStubConn::~ZmqStubConn()
{
    interrupt_ = true;
    frontend_->Stop();
}

void ZmqStubConn::Shutdown()
{
    Timer t;
    auto zmqEndPt = frontend_->GetZmqEndPoint();
    interrupt_ = true;
    frontend_->InterruptAll();
    ZmqBaseStubConn::Shutdown();
    if (ZmqStubConnMgr::LoggingOn()) {
        VLOG(RPC_LOG_LEVEL) << FormatString("Stub conn to %s shutdown. Elapsed: [%.6lf]s", zmqEndPt, t.ElapsedSecond());
    }
}

Status ZmqStubConn::Init()
{
    return ZmqBaseStubConn::Init();
}

bool ZmqStubConn::IsPeerAlive(uint32_t threshold)
{
    return frontend_->IsPeerAlive(threshold);
}

Status ZmqStubConn::CreateMsgQ(const std::shared_ptr<StubInfo> &info, ZmqMsgQueRef &mQue, const RpcOptions &opts)
{
    CHECK_FAIL_RETURN_STATUS(!interrupt_, K_SHUTTING_DOWN, "Shutting down");
    // Change the name of the queue, and so we can associate this
    // que with the outgoing method
    const std::string suffix = ZmqStubConnMgrImpl::EncodeQueName(info);
    RETURN_IF_NOT_OK(GetMsgQ(mQue, opts, suffix));
    return Status::OK();
}

void ZmqStubConn::CacheSession(bool cache)
{
    if (cache) {
        ZmqStubConnMgr::Instance()->IncConnRef(key_);
    } else {
        ZmqStubConnMgr::Instance()->DecConnRef(key_);
    }
}

void ChildAfterFork()
{
    ZmqStubConnMgr::Instance()->AfterFork();
}

void ZmqStubConnMgr::AfterFork()
{
    if (impl_ != nullptr) {
        (void)impl_.release();
    }
    impl_ = std::make_unique<ZmqStubConnMgrImpl>();
}

ZmqStubConnMgrImpl::ZmqStubConnMgrImpl()
    : interrupt_(false),
      initialize_(false),
      ctx_(std::make_shared<ZmqContext>()),
      stubId_(0),
      nextMgrId_(0),
      nextPollerId_(0),
      sockConnHelper_(std::make_unique<ZmqSockConnHelper>())
{
    const auto numBackends = std::max<int>(1, FLAGS_zmq_client_io_context);
    backends_.reserve(numBackends);
    for (auto i = 0; i < numBackends; ++i) {
        auto backend = std::make_shared<ZmqMsgMgr>(
            [this](const std::string &s, ZmqMetaMsgFrames &&p) { return this->Outbound(s, std::move(p)); });
        backends_.emplace_back(std::move(backend));
    }
    const auto numPollers = std::max<int>(1, FLAGS_zmq_client_io_context);
    pollers_.reserve(numPollers);
    for (auto i = 0; i < numPollers; ++i) {
        pollers_.emplace_back(std::make_unique<ZmqEpoll>());
    }
}

Status ZmqStubConnMgrImpl::StartAllThreads()
{
    bool expected = false;
    if (initialize_.compare_exchange_strong(expected, true)) {
        RETURN_IF_NOT_OK(ctx_->Init());
        for (auto &ele : backends_) {
            RETURN_IF_NOT_OK(ele->Init());
        }
        int i = 0;
        for (auto &ele : pollers_) {
            RETURN_IF_NOT_OK(ele->Init(FormatString("ZmqStubConnMgr poller %d", i++)));
        }

        unregStubThrd_ = std::make_unique<Thread>(&ZmqStubConnMgrImpl::UnregisterStub, this);
        unregStubThrd_->set_name("ZmqUnregisterStub");
    }
    return Status::OK();
}

ZmqStubConnMgrImpl::~ZmqStubConnMgrImpl()
{
    WriteLock lock(&connMux_);
    if (sockConnHelper_) {
        sockConnHelper_.reset();
    }
    connMap_.clear();
    backends_.clear();
    pollers_.clear();
}

void ZmqStubConnMgrImpl::Shutdown()
{
    // It can happen another stub can be calling GetConn when
    // the program terminates because the stub can attempt
    // to re-connect. So hold the lock while clear the map.
    WriteLock lock(&connMux_);
    interrupt_ = true;
    if (unregStubThrd_) {
        cvLock_.Set();
        unregStubThrd_->join();
    }
    if (sockConnHelper_) {
        sockConnHelper_->InterruptAll();
    }
    for (auto &ele : pollers_) {
        ele->Stop();
    }
    for (auto &ele : backends_) {
        ele->Stop();
    }
    for (const auto &p : connMap_) {
        auto conn = p.second.lock();
        if (conn == nullptr) {
            continue;
        }
        conn->Shutdown();
    }
    if (ctx_) {
        ctx_->Close(ZmqStubConnMgr::LoggingOn());
    }
}

ZmqStubConnMgr *ZmqStubConnMgr::Instance()
{
    static ZmqStubConnMgr instance(Token(), *(Logging::GetInstance()));
    return &instance;
}

ZmqStubConnMgr::ZmqStubConnMgr(Token t, Logging &inject) : log_(inject)
{
    (void)t;
    int rc = pthread_atfork(nullptr, nullptr, &ChildAfterFork);
    if (rc != 0) {
        LOG(WARNING) << "ZmqStubConnMgr: Constructor failure pthread_atfork.";
    }
    impl_ = std::make_unique<ZmqStubConnMgrImpl>();
}

ZmqStubConnMgr::~ZmqStubConnMgr()
{
    logging_ = false;
    std::lock_guard<std::shared_timed_mutex> lock(implMux_);
    if (impl_) {
        impl_->Shutdown();
        impl_.reset();
    }
}

std::shared_ptr<ZmqBaseStubConn> ZmqStubConnMgrImpl::GetExistingConn(const ZmqConnKey &key)
{
    std::shared_ptr<ZmqBaseStubConn> conn;
    auto it = connMap_.find(key);
    if (it != connMap_.end()) {
        auto sc = it->second.lock();
        if (sc) {
            conn = sc;
        } else {
            connMap_.erase(it);
        }
    }
    return conn;
}

Status ZmqStubConnMgrImpl::GetConn(ZmqStub *stub, std::shared_ptr<StubInfo> &handle,
                                   const std::shared_ptr<RpcChannel> &channel, int32_t timeoutMs,
                                   std::shared_ptr<ZmqBaseStubConn> &conn, std::shared_ptr<SockConnEntry> &sockConn)
{
    RETURN_IF_NOT_OK(RegisterStub(stub, handle, channel));
    const auto &info = handle;
    // Next we construct the key which controls if existing connection can be shared
    ZmqConnKey key = CreatePrimaryKey(info.get());
    WriteLock lock(&connMux_);
    conn = GetExistingConn(key);
    bool insert = (conn == nullptr);
    // Before we continue, ensure all threads are running if we haven't started them yet.
    RETURN_IF_NOT_OK(StartAllThreads());
    // If we create a new one, pick a ZmqMsgMgr and possibly a poller to link to the ZmqFrontend
    if (insert) {
        // They can be created independently, but we will simply
        // share with existing ones. Not only this avoids the cost of creating
        // and destroying objects but also control the number of threads spawned.
        auto backend = GetNextBackendMgr();
        auto frontend = std::make_shared<ZmqFrontend>(GetZmqContext(), info->channel_, backend);
        RETURN_IF_NOT_OK(frontend->Init());
        frontend->UpdateLiveness(timeoutMs);
        // Next we will create a ZmqStubConn to hold the pieces together which also
        // serves the purpose to keep the shared pointer reference count.
        conn = std::make_shared<ZmqStubConn>(key, backend, frontend);
        RETURN_IF_NOT_OK(conn->Init());
        std::weak_ptr<ZmqBaseStubConn> wp(conn);
        connMap_.insert(std::make_pair(key, wp));
        VLOG(RPC_LOG_LEVEL) << FormatString("Create conn end point for %s. Conn manager map size %d",
                                            key.channelEndPoint_, connMap_.size());
    } else {
        VLOG(RPC_LOG_LEVEL) << FormatString("Found existing conn end point for %s. Use count %d (including myself)",
                                            key.channelEndPoint_, conn.use_count());
    }
    // Attach a weak pointer to the stub info. The ZmqStubImpl class has saved the original shared pointer already.
    info->conn_ = conn;
    // If uds or tcp is required, register the stub to the sock helper class
    if (info->uds_ || info->tcpDirect_) {
        RETURN_IF_NOT_OK(sockConnHelper_->RegisterStub(info, key, conn, sockConn, insert));
    }
    return Status::OK();
}

void ZmqStubConnMgrImpl::IncConnRef(const ZmqConnKey &key)
{
    WriteLock lock(&connMux_);
    auto it = connMap_.find(key);
    if (it != connMap_.end()) {
        auto sc = it->second.lock();
        if (sc) {
            ref_.insert({ key, std::move(sc) });
            VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Cache connection for %s for service %s", key.channelEndPoint_,
                                                    key.svcName_);
        }
    }
}

void ZmqStubConnMgrImpl::DecConnRef(const ZmqConnKey &key)
{
    WriteLock lock(&connMux_);
    auto it = ref_.find(key);
    if (it != ref_.end()) {
        ref_.erase((it));
    }
}

Status ZmqStubConnMgrImpl::RegisterStub(ZmqStub *stub, std::shared_ptr<StubInfo> &handle,
                                        const std::shared_ptr<RpcChannel> &channel)
{
    std::shared_ptr<StubInfo> info;
    auto sockName = channel->GetServiceSockName(stub->FullServiceName());
    WriteLock lock(&stubMux_);
    auto id = stubId_.fetch_add(1);
    bool uds = !sockName.empty();
    info = std::make_shared<StubInfo>();
    info->svcName_ = stub->ServiceName();
    info->uds_ = uds;
    info->tcpDirect_ = channel->GetServiceTcpDirect(stub->FullServiceName());
    info->stubId_ = id;
    info->sockName_ = sockName;
    info->channel_ = channel;
    info->connPoolSz_ = channel->GetServiceConnectPoolSize(stub->FullServiceName());
    info->channelNo_ = stub->GetChannelNumber();
    // Adjust the channel number in some exceptional case. The plugin generator will set the channel
    // number to 1 if the service has some payload methods. But if we are going to create some
    // uds or tcp socket, we don't need to create a separate ZmqFrontend
    if (info->channelNo_ == 1 && (info->uds_ || info->tcpDirect_)) {
        info->channelNo_ = 0;
    }
    stubInfos_.emplace(id, info);
    VLOG(RPC_LOG_LEVEL) << FormatString(
        "Stub (0x%x) service '%s' channel %d register for channel end point %s. uds %s. direct tcp %s. pool size %zu",
        info->stubId_, info->svcName_, info->channelNo_, channel->GetZmqEndPoint(), info->uds_ ? "true" : "false",
        info->tcpDirect_ ? "true" : "false", info->connPoolSz_);
    handle = info;
    return Status::OK();
}

void ZmqStubConnMgrImpl::UnregisterStub()
{
    const int CHECK_UNREG_STUB_INTERVAL_MS = 100;
    while (!interrupt_) {
        std::set<int64_t> unregList{};
        ReadLock rlock(&stubMux_);
        for (auto &pair : stubInfos_) {
            auto &info = pair.second;
            if (info.use_count() == 1) {
                unregList.emplace(info->stubId_);
            }
        }
        rlock.UnlockIfLocked();
        if (unregList.empty()) {
            cvLock_.WaitFor(CHECK_UNREG_STUB_INTERVAL_MS);
            continue;
        }
        // Only acquire write lock when there is actually stub to cleanup
        WriteLock wlock(&stubMux_);
        for (auto &id : unregList) {
            auto it = stubInfos_.find(id);
            if (it != stubInfos_.end() && it->second.use_count() == 1) {
                auto &info = it->second;
                VLOG(RPC_LOG_LEVEL) << FormatString("Stub (0x%x) service '%s' unregister from channel %s",
                                                    info->stubId_, info->svcName_, info->channel_->GetZmqEndPoint());
                stubInfos_.erase(it);
            }
        }
    }
}

std::shared_ptr<ZmqContext> ZmqStubConnMgrImpl::GetZmqContext()
{
    return ctx_;
}

ZmqConnKey ZmqStubConnMgrImpl::CreatePrimaryKey(StubInfo *info)
{
    ZmqConnKey key;
    key.svcName_ = info->svcName_;
    key.channelEndPoint_ = info->channel_->GetZmqEndPoint();
    key.cred_ = info->channel_->GetCredential();
    key.channelNo_ = info->channelNo_;
    key.svcNameAsKey_ = false;  // see comment in operator==
    return key;
}

ZmqMsgMgr *ZmqStubConnMgrImpl::GetNextBackendMgr()
{
    auto mgrId = nextMgrId_.fetch_add(1);
    auto &backend = backends_.at(mgrId % backends_.size());
    return backend.get();
}

ZmqEpoll *ZmqStubConnMgrImpl::GetNextPoller()
{
    auto pollId = nextPollerId_.fetch_add(1);
    auto &poller = pollers_.at(pollId % pollers_.size());
    return poller.get();
}

std::string ZmqStubConnMgrImpl::EncodeQueName(const std::shared_ptr<StubInfo> &info)
{
    auto connInfo = info->connInfo_.lock();
    int64_t conn = 0;
    if (connInfo != nullptr) {
        conn = reinterpret_cast<intptr_t>(connInfo.get());
    }
    return FormatString(":%d:%d:%d", info->stubId_, reinterpret_cast<intptr_t>(info->conn_.lock()->Frontend()), conn);
}

Status ZmqStubConnMgrImpl::DecodeQueName(const std::string &queName, ZmqFrontend *&frontend, SockConnEntry *&connInfo,
                                         int64_t &stubId)
{
    frontend = nullptr;
    connInfo = nullptr;
    auto convert64Int = [](const std::string &str, intptr_t &out) -> Status {
        try {
            out = std::stol(str);
        } catch (std::invalid_argument const &ex) {
            RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                    FormatString("String to int64 failed for %s. %s\n", str, ex.what()));
        } catch (std::out_of_range const &ex) {
            RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                    FormatString("String to int64 failed at pos %d for %s. %s\n", str, ex.what()));
        }
        return Status::OK();
    };
    auto pos3 = queName.find_last_of(':');
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(pos3 != std::string::npos, K_RUNTIME_ERROR,
                                         FormatString("Unknown id format %s", queName));
    intptr_t conn = 0;
    RETURN_IF_NOT_OK_APPEND_MSG(convert64Int(queName.substr(pos3 + 1), conn), queName);
    if (conn != 0) {
        connInfo = reinterpret_cast<SockConnEntry *>(conn);
    }

    auto pos2 = queName.find_last_of(':', pos3 - 1);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(pos2 != std::string::npos, K_RUNTIME_ERROR,
                                         FormatString("Unknown id format %s", queName));
    int64_t addr = 0;
    RETURN_IF_NOT_OK_APPEND_MSG(convert64Int(queName.substr(pos2 + 1, pos3 - pos2 - 1), addr), queName);
    frontend = reinterpret_cast<ZmqFrontend *>(addr);

    auto pos1 = queName.find_last_of(':', pos2 - 1);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(pos1 != std::string::npos, K_RUNTIME_ERROR,
                                         FormatString("Unknown id format %s", queName));
    stubId = 0;
    RETURN_IF_NOT_OK_APPEND_MSG(convert64Int(queName.substr(pos1 + 1, pos2 - pos1 - 1), stubId), queName);
    return Status::OK();
}

Status ZmqStubConnMgrImpl::GetZmqFrontendFromPtr(int64_t stubId, ZmqFrontend *rawPtr,
                                                 std::shared_ptr<ZmqFrontend> &frontend)
{
    {
        ReadLock rlock(&stubMux_);
        auto it = stubInfos_.find(stubId);
        if (it != stubInfos_.end()) {
            return GetSharedPtrFromThis(rawPtr, frontend);
        }
    }
    // The stub is out of scope already. But the ZmqFrontend may still be there.
    // And it is a bit more work.
    {
        ReadLock rlock(&connMux_);
        for (const auto &p : connMap_) {
            auto conn = p.second.lock();
            if (conn == nullptr) {
                continue;
            }
            if (conn->Frontend() == rawPtr) {
                return GetSharedPtrFromThis(rawPtr, frontend);
            }
        }
    }
    RETURN_STATUS(K_NOT_FOUND, FormatString("Pointer 0x%x is out of scope", reinterpret_cast<intptr_t>(rawPtr)));
}

Status ZmqStubConnMgrImpl::Outbound(const std::string &sender, ZmqMetaMsgFrames &&p)
{
    ZmqFrontend *fePtr = nullptr;
    SockConnEntry *connInfo = nullptr;
    int64_t stubId = 0;
    std::shared_ptr<ZmqFrontend> frontend;
    MetaPb meta = p.first;
    PerfPoint::RecordElapsed(PerfKey::ZMQ_STUB_ROUTE_MSG, GetLapTime(meta, "ZMQ_STUB_ROUTE_MSG"));
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(meta.trace_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(DecodeQueName(sender, fePtr, connInfo, stubId),
                                     FormatString("Bad queue format %s", sender));
    // If the frontend is deallocated, not much we can do at this point.
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetZmqFrontendFromPtr(stubId, fePtr, frontend),
                                     FormatString("ZmqFrontend out of scope. Queue %s", sender));
    VLOG(RPC_LOG_LEVEL) << FormatString("Stub (0x%x) Routing from message que %s service %s method %d to frontend",
                                        stubId, sender, meta.svc_name(), meta.method_index());
    Timer t;
    Status rc = frontend->SendMsg(connInfo, std::move(p), STUB_FRONTEND_TIMEOUT);
    if (rc.IsError()) {
        // Failed to send out the request to server, so we reroute the error back to the client
        // instead of letting the client wait 60s and timeout.
        auto elapsed = t.ElapsedSecond();
        auto msg = FormatString("Message que %s service %s method %d. Elapsed: [%.6lf]s", meta.client_id(),
                                meta.svc_name(), meta.method_index(), elapsed);
        LOG(INFO) << FormatString("%s. %s", rc.ToString(), msg);
        return ZmqBaseStubConn::ReportErrorToClient(
            meta.client_id(), meta, K_RPC_UNAVAILABLE,
            FormatString("[RPC_SERVICE_UNAVAILABLE] The service is currently unavailable! %s", msg),
            frontend->GetBackendMgr());
    }
    return Status::OK();
}

bool ZmqConnKey::operator==(const ZmqConnKey &rhs) const
{
    // Different connection end point.
    if (channelEndPoint_ != rhs.channelEndPoint_) {
        return false;
    }
    // Cred must match
    if (!std::equal_to<RpcCredential>{}(this->cred_, rhs.cred_)) {
        return false;
    }
    // Channel number is an explicit request not sharing the connection
    if (channelNo_ != rhs.channelNo_) {
        return false;
    }
    // Last thing to check is svcName_.
    // For service, we have a choice not to consider it because zmq socket
    // can only connect to ZmqServerImpl, not individual ZmqService.
    // If we include it in the comparison in which case two stubs with the same end point
    // but different services will not share the same connection (and it improves
    // parallelism at the cost of extra ZmqFrontend). But sometimes some service
    // is rarely used and can share other stub's connection to save resources.
    if (svcNameAsKey_ && (svcName_ != rhs.svcName_)) {
        return false;
    }
    return true;
}

ZmqConnKey::ZmqConnKey() : channelNo_(0), svcNameAsKey_(false)
{
}

SockConnEntry::~SockConnEntry()
{
    WriteLock xlock(&mux_);
    for (auto &fdConn : pools_) {
        (void)fdConn->inPoller_->RemoveFd(fdConn->inHandle_);
        (void)fdConn->outPoller_->RemoveFd(fdConn->outHandle_);
        RETRY_ON_EINTR(close(fdConn->fd_));
    }
    pools_.clear();
}

Status SockConnEntry::SetSvcFdInvalid(int fd, std::shared_ptr<SockConnEntry::FdConn> &fdPtr)
{
    WriteLock xlock(&mux_);
    std::string svcName = svcName_;
    VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Service '%s' socket %d disconnect.", svcName, fd);
    endPoint_ = "";
    fdPtr->fd_ = ZMQ_NO_FILE_FD;
    fdPtr->decoder_.reset();
    wp_.Clear();
    Status rc1 = fdPtr->inPoller_->RemoveFd(fdPtr->inHandle_);
    Status rc2 = fdPtr->outPoller_->RemoveFd(fdPtr->outHandle_);
    RETRY_ON_EINTR(close(fd));
    WriteLock lock(fdPtr->outMux_.get());
    fdPtr->outMsgQueue_->clear();
    return rc1.IsOk() ? rc2 : rc1;
}

Status SockConnEntry::FillFdConn(int fd, std::shared_ptr<SockConnEntry::FdConn> &fdConn, ZmqCallBackFunc inFunc,
                                 ZmqCallBackFunc outFunc)
{
    WriteLock xlock(&mux_);
    fdConn->fd_ = fd;
    fdConn->decoder_ = std::make_unique<ZmqMsgDecoder>(fd);
    fdConn->encoder_ = std::make_unique<ZmqMsgEncoder>(fd);
    retryCount_ = 0;
    std::shared_ptr<SockConnEntry> connInfo;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetSharedPtrFromThis(this, connInfo),
                                     FormatString("Socket connection info for fd %d not found", fdConn->fd_));
    auto inPollEntry = ZmqCreatePollEntry(fdConn->fd_, { connInfo, fdConn }, inFunc, nullptr);
    auto outPollEntry = ZmqCreatePollEntry(fdConn->fd_, { connInfo, fdConn }, nullptr, outFunc);
    // add to the epoll list
    RETURN_IF_NOT_OK(fdConn->inPoller_->AddFd(fdConn->fd_, std::move(inPollEntry), fdConn->inHandle_));
    RETURN_IF_NOT_OK(fdConn->outPoller_->AddFd(fdConn->fd_, std::move(outPollEntry), fdConn->outHandle_));
    RETURN_IF_NOT_OK(fdConn->inPoller_->SetPollIn(fdConn->inHandle_));
    return Status::OK();
}

Status SockConnEntry::AddEvent(int fd, std::shared_ptr<SockConnEntry::FdConn> fdConn)
{
    // Create an epoll entry
    auto f = [](ZmqPollEntry *entry, uint32_t events) {
        // get connInfo from weak ptr
        auto connInfoSp = entry->hint_.front().lock();
        CHECK_FAIL_RETURN_STATUS(connInfoSp != nullptr, K_RUNTIME_ERROR, "connInfo is deallocated");
        auto connInfo = std::dynamic_pointer_cast<SockConnEntry>(connInfoSp);

        // get FdConn from weak ptr
        auto fdPtrSp = entry->hint_.back().lock();
        CHECK_FAIL_RETURN_STATUS(fdPtrSp != nullptr, K_RUNTIME_ERROR, "FdConn is deallocated");
        auto fdPtr = std::dynamic_pointer_cast<SockConnEntry::FdConn>(fdPtrSp);

        auto fd = entry->fd_;
        events &= ~EPOLLIN;
        auto inErrorState = events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP);
        Status rc;
        if (!inErrorState) {
            rc = connInfo->FrontendToBackend(fd, fdPtr);
            if (rc.GetCode() == K_RPC_CANCELLED) {
                inErrorState = true;
            }
        }
        if (inErrorState) {
            // Cache is no longer valid.
            rc = connInfo->SetSvcFdInvalid(fd, fdPtr);
            ZmqStubConnMgr::Instance()->impl_->AutoReconnect(connInfo);
        }
        return rc;
    };
    auto g = [](ZmqPollEntry *entry, uint32_t events) {
        // get connInfo from weak ptr
        auto connInfoSp = entry->hint_.front().lock();
        CHECK_FAIL_RETURN_STATUS(connInfoSp != nullptr, K_RUNTIME_ERROR, "connInfo is deallocated");
        auto connInfo = std::dynamic_pointer_cast<SockConnEntry>(connInfoSp);

        // get FdConn from weak ptr
        auto fdPtrSp = entry->hint_.back().lock();
        CHECK_FAIL_RETURN_STATUS(fdPtrSp != nullptr, K_RUNTIME_ERROR, "FdConn is deallocated");
        auto fdPtr = std::dynamic_pointer_cast<SockConnEntry::FdConn>(fdPtrSp);

        auto fd = entry->fd_;
        if (events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
            // Cache is no longer valid.
            Status rc = connInfo->SetSvcFdInvalid(fd, fdPtr);
            ZmqStubConnMgr::Instance()->impl_->AutoReconnect(connInfo);
            return rc;
        }
        return connInfo->BackendToFrontend(fd, fdPtr);
    };
    return FillFdConn(fd, fdConn, f, g);
}

Status SockConnEntry::FrontendToBackend(int fd, std::shared_ptr<SockConnEntry::FdConn> fdConn) const
{
    CHECK_FAIL_RETURN_STATUS(fdConn != nullptr, K_NOT_FOUND, FormatString("fd %d not found", fd));
    CHECK_FAIL_RETURN_STATUS(fdConn->fd_ == fd, K_RUNTIME_ERROR, FormatString("fd %d doesn't match FdConn", fd));
    auto &decoder = fdConn->decoder_;
    ZmqMsgFrames frames;
    Status rc;
    // Use V2 to receive frames. It is compatible with V1
    RETURN_IF_NOT_OK(decoder->ReceiveMsgFramesV2(frames));
    const size_t msgFrameMinSize = 2;
    CHECK_FAIL_RETURN_STATUS(frames.size() >= msgFrameMinSize, StatusCode::K_INVALID,
                             "Invalid msg: frames.size() = " + std::to_string(frames.size()));
    std::string receiver = ZmqMessageToString(frames.front());
    frames.pop_front();
    // Next one is the original MetaPb.
    ZmqMessage metaHdr = std::move(frames.front());
    frames.pop_front();
    MetaPb meta;
    RETURN_IF_NOT_OK(ParseFromZmqMessage(metaHdr, meta));
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(meta.trace_id());
    PerfPoint::RecordElapsed(PerfKey::ZMQ_NETWORK_TRANSFER_STUB_UDS, GetLapTime(meta, "ZMQ_NETWORK_TRANSFER (SOCKET)"));
    auto p = std::make_pair(meta, std::move(frames));
    VLOG(RPC_LOG_LEVEL) << FormatString("Receiving reply for sender %s gateway %s client %s service '%s' method %d",
                                        receiver, meta.gateway_id(), meta.client_id(), meta.svc_name(),
                                        meta.method_index());
    RETURN_IF_NOT_OK(backend_->SendMsg(receiver, p, QUE_NO_TIMEOUT));
    return Status::OK();
}

Status SockConnEntry::BackendToFrontend(int fd, std::shared_ptr<SockConnEntry::FdConn> fdConn)
{
    CHECK_FAIL_RETURN_STATUS(fdConn != nullptr, K_NOT_FOUND, FormatString("fd %d not found", fd));
    CHECK_FAIL_RETURN_STATUS(fdConn->fd_ == fd, K_RUNTIME_ERROR, FormatString("fd %d doesn't match FdConn", fd));
    EventType type;
    ZmqMsgFrames frames;
    {
        WriteLock lock(fdConn->outMux_.get());
        if (fdConn->outMsgQueue_->empty()) {
            return fdConn->outPoller_->UnsetPollOut(fdConn->outHandle_);
        }
        auto &ele = fdConn->outMsgQueue_->front();
        type = ele.first;
        frames = std::move(ele.second);
        fdConn->outMsgQueue_->pop_front();
    }
    return fdConn->encoder_->SendMsgFrames(type, frames);
}

Status SockConnEntry::WaitForConnected(int64_t timeout)
{
    // Check if background connection already completed. No need to wait for a long time.
    // If the connection is already completed, we will return from the WaitPost immediately.
    // Then we check for both the flag and the connection result because it may still in
    // progress.
    CHECK_FAIL_RETURN_STATUS(!pools_.empty(), K_NOT_FOUND, "Not in auto connection list");
    INJECT_POINT("SockConnEntry.WaitForConnected.SetTimeout", [&timeout](int32_t time) {
        timeout = time;
        return Status::OK();
    });

    auto success = wp_.WaitFor(timeout);
    // Timeout waiting to be connected, so return K_RPC_UNAVAILABLE to client
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(success, K_RPC_UNAVAILABLE,
                                         "[SOCK_CONN_WAIT_TIMEOUT] Timeout waiting for SockConnEntry wait");
    if (!connInProgress_) {
        return connectRc_;
    }
    RETURN_STATUS(K_RPC_UNAVAILABLE,
                  FormatString("[REMOTE_SERVICE_WAIT_TIMEOUT] Remote service is not available within allowable %d ms",
                               timeout));
}

void SockConnEntry::GetNextFd(bool forceV2mtp, std::shared_ptr<SockConnEntry::FdConn> &fdConn)
{
    // We count the one in the ZmqFrontend as part of the pool as long as it is tcp/ip
    // Return -1 to the caller in this case to inform the caller to use zmq socket
    const size_t poolSz = (uds_ || forceV2mtp) ? pools_.size() : pools_.size() + 1;
    auto i = idx_.fetch_add(1) % poolSz;
    if (i < pools_.size()) {
        fdConn = pools_[i];
    } else {
        fdConn = nullptr;
    }
}

SockConnEntry::SockConnEntry(ZmqMsgMgr *backend)
    : uds_(false), retryCount_(0), connInProgress_(false), backend_(backend), idx_(0)
{
}
}  // namespace datasystem

namespace std {
size_t hash<datasystem::ZmqConnKey>::operator()(const datasystem::ZmqConnKey &key) const
{
    auto h1 = hash<string>{}(key.channelEndPoint_);
    return h1;
}
}  // namespace std
