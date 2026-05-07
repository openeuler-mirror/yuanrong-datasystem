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
 * Description: Zmq Server Implementation.
 */
#include "datasystem/common/rpc/zmq/zmq_server_impl.h"

#include <chrono>
#include <sstream>
#include <utility>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/rpc/zmq/zmq_context.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/fd_manager.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/utils/status.h"

DS_DEFINE_int32(zmq_server_io_context, 5,
                "Optimize the performance of the customer. Default server 5. "
                "The higher the throughput, the higher the value, but should be in range [1, 32]");
DS_DECLARE_bool(enable_curve_zmq);

namespace {
static bool ValidateZmqServerIoCtxThreads(const char *flagname, int32_t value)
{
    (void)flagname;
    const int32_t minValue = 1;
    const int32_t maxValue = 32;
    if ((value < minValue) || (value > maxValue)) {
        LOG(ERROR) << "The server io context thread number must be between " << minValue << " and " << maxValue << ".";
        return false;
    }
    return true;
}
}  // namespace
DS_DEFINE_validator(zmq_server_io_context, &ValidateZmqServerIoCtxThreads);

namespace datasystem {

Status ZmqServerImpl::InitAuthHandler()
{
    VLOG(RPC_LOG_LEVEL) << "Register AuthHandler";
    return RpcAuthKeyManager::Instance().InitAuthHandler(ctx_);
}

void ZmqServerImpl::Shutdown()
{
    globalInterrupt_ = true;
    eventfd_write(sfd_, 1);
    // Stop the poller thread first.
    if (poller_) {
        poller_->Stop();
    }
    StopAllServices();
    if (ses_) {
        ses_->Stop();
    }
    if (thrd_) {
        LOG(WARNING) << "RPC Server shutdown in progress ...";
        try {
            auto status = thrd_->get();
            if (status.IsError()) {
                LOG(ERROR) << status.ToString();
            }
        } catch (const std::exception &e) {
            LOG(ERROR) << e.what();
        }
        thrd_.reset();
        VLOG(RPC_KEY_LOG_LEVEL) << "RPC Server shutdown successfully.";
    }
    // The followings are set up in the constructor.
    if (sfd_ > 0) {
        RETRY_ON_EINTR(close(sfd_));
        sfd_ = ZMQ_NO_FILE_FD;
    }
    // We must close all the ZMQ sockets. Otherwise, ZMQ context will hang on shutdown.
    // Same for the thread doing proxy work. It needs the context.
    if (frontend_) {
        frontend_->Close();
        frontend_.reset();
    }
    if (poller_) {
        poller_.reset();
    }
    if (ctx_) {
        ctx_->Close();
        ctx_.reset();
    }
    zmqfds_.clear();
}

void ZmqServerImpl::StopAllServices()
{
    for (auto &it : svcMap_) {
        it.second->Stop();
    }
    svcMap_.clear();
    if (RpcAuthKeyManager::Instance().HasAuthHandler()) {
        RpcAuthKeyManager::Instance().StopAuthHandler();
    }
}

void RecalculateMetaTimeout(MetaPb &meta, uint64_t lapTime)
{
    if (meta.svc_name().empty()) {
        return;
    }
    int64_t timeoutMs = meta.timeout();
    constexpr uint64_t NANO_TO_MILLI_UNIT = 1'000'000;
    int64_t lapTimeMs = static_cast<int64_t>(lapTime / NANO_TO_MILLI_UNIT);
    if (lapTimeMs > 0 && timeoutMs > lapTimeMs) {
        meta.set_timeout(timeoutMs - lapTimeMs);
    }
}

Status ParseMsgFrames(ZmqMsgFrames &frames, MetaPb &meta, int fd, const EventType type, ZmqCurveUserId &userId,
                      PerfKey transferPerfKey)
{
    // Next is the metadata.
    CHECK_FAIL_RETURN_STATUS(!frames.empty(), StatusCode::K_INVALID, "Expect a gateway id");
    // Get the client id which we can be sure it exists. It is an internal ZMQ thing.
    ZmqMessage gatewayId = std::move(frames.front());
    frames.pop_front();
    CHECK_FAIL_RETURN_STATUS(!frames.empty(), StatusCode::K_INVALID, "Expect a header");
    ZmqMessage metaHdr = std::move(frames.front());
    frames.pop_front();
    if (userId.checkUserId_) {
        RETURN_IF_NOT_OK(RpcAuthKeyManager::CopyCurveAuthKey(metaHdr.GetMetaProperty("User-Id"), userId.userId_));
    }
    if (ParseFromZmqMessage(metaHdr, meta).IsError() || meta.ticks_size() == 0) {
        // Since stub client will probe us (using an empty string), we will not put
        // that into log file and return a different rc (to avoid flooding the
        // log file).
        if (!metaHdr.Empty()) {
            LOG(WARNING) << "Unknown header when ParseMsgFrames: " << metaHdr;
            RETURN_STATUS(StatusCode::K_INVALID, "Unknown header when ParseMsgFrames");
        } else {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "Failed to parse zmq meta header when ParseMsgFrames");
        }
    }
    uint64_t lapTime = GetLapTime(meta, "ZMQ_NETWORK_TRANSFER (SERVER)");
    // This is the time spent in ZMQ and the underlying network (tcp/ip or unix socket) overhead.
    PerfPoint::RecordElapsed(transferPerfKey, lapTime);
    RecordTick(meta, TICK_SERVER_RECV);
    // Set up the return address.
    meta.set_gateway_id(ZmqMessageToString(gatewayId));
    meta.set_route_fd(fd);
    meta.set_event_type(type);
    RecalculateMetaTimeout(meta, lapTime);
    return Status::OK();
}

Status ZmqServerImpl::ServiceToClient(const MetaPb &meta, ZmqMsgFrames &&frames)
{
    RETURN_IF_NOT_OK(PushFrontProtobufToFrames(meta, frames));
    RETURN_IF_NOT_OK(PushFrontStringToFrames(meta.client_id(), frames));
    RETURN_IF_NOT_OK(PushFrontStringToFrames(meta.gateway_id(), frames));
    VLOG(RPC_LOG_LEVEL) << FormatString("Sending reply back to gateway %s for client %s service '%s' method %d",
                                        meta.gateway_id(), meta.client_id(), meta.svc_name(), meta.method_index());
    return frontend_->SendAllFrames(frames);
}

Status ZmqServerImpl::SendErrorToClient(const MetaPb &meta, const Status &status)
{
    ZmqMsgFrames reply;
    reply.push_back(StatusToZmqMessage(status));
    return ServiceToClient(meta, std::move(reply));
}

Status ZmqServerImpl::ClientToService(ZmqMsgFrames &&frames)
{
    MetaPb meta;
    ZmqCurveUserId userId = { .checkUserId_ = FLAGS_enable_curve_zmq };
    Status rc = ParseMsgFrames(frames, meta, ffd_, EventType::ZMQ, userId, PerfKey::ZMQ_NETWORK_TRANSFER_SERVER_TCP);
    if (rc.IsError()) {
        // Silently (no logging) ignore K_TRY_AGAIN because stub will probe us with an empty message
        // on first connection.
        RETURN_OK_IF_TRUE(rc.GetCode() == StatusCode::K_TRY_AGAIN);
        // Log the message for anything else, and move on.
        RETURN_STATUS_LOG_ERROR(StatusCode::K_OK, "Incompatible rpc request. Ignore");
    }
    // It is simple enough to deal with heartbeat here without routing to any service.
    if (meta.method_index() == ZMQ_HEARTBEAT_METHOD) {
        VLOG(RPC_LOG_LEVEL) << "Heartbeat";
        // Just send back anything. But we will send back the meta which
        // records the time when we receive it.
        ZmqMsgFrames reply;
        RETURN_IF_NOT_OK(PushBackProtobufToFrames(meta, reply));
        return ServiceToClient(meta, std::move(reply));
    }
    ZmqService *svc = nullptr;
    auto it = svcMap_.find(meta.svc_name());
    if (it == svcMap_.end()) {
        // Change the return code to K_NOT_FOUND if this is internal methods. One example
        // is the worker didn't register the sc service. But a sc client is doing
        // handshake for uds, and the stub client will drive endless retries. Sending
        // K_NOT_FOUND will stop the retry loop in the stub client.
        Status err = Status(meta.method_index() < 0 ? StatusCode::K_NOT_FOUND : StatusCode::K_RUNTIME_ERROR,
                            "Service not found");
        RETURN_IF_NOT_OK(SendErrorToClient(meta, err));
        return err;
    }
    if (userId.checkUserId_) {
        auto &svcMapping = RpcAuthKeyManager::Instance().GetSvcMapping();
        auto keyList = svcMapping.find(meta.svc_name());
        if (keyList == svcMapping.end() || keyList->second.find(userId.userId_.get()) == keyList->second.end()) {
            // Send a reply back for authentication failure at service level.
            std::string msg =
                FormatString("Trust list authentication failed. Access to service %s is not allowed.", meta.svc_name());
            Status temp = Status(K_INVALID, msg);
            LOG(ERROR) << temp.ToString();
            return SendErrorToClient(meta, temp);
        }
    }
    svc = it->second;
    // Continue to send to corresponding service.
    VLOG(RPC_LOG_LEVEL) << "Route to service '" << svc->ServiceName() << "'. Method " << meta.method_index()
                        << " for client " << meta.client_id() << " from gateway " << meta.gateway_id();
    return svc->ServiceRequest(std::move(meta), std::move(frames));
}

Status ZmqServerImpl::ProcessReply(ZmqService *svc)
{
    ZmqMetaMsgFrames p;
    RETURN_IF_NOT_OK(svc->ServiceReply(&p));
    ZmqMsgFrames &frames = p.second;
    MetaPb &meta = p.first;
    PerfPoint::RecordElapsed(PerfKey::ZMQ_SVC_TO_ROUTER, GetLapTime(meta, "ZMQ_SVC_TO_ROUTER"));
    RecordTick(meta, TICK_SERVER_SEND);
    RecordServerLatencyMetrics(meta);
    return ServiceToClient(meta, std::move(frames));
}

Status ZmqServerImpl::ZmqFrontendToSvc()
{
    Status rc;
    ZmqMsgFrames frames;
    if (frontend_->GetAllFrames(frames, ZmqRecvFlags::DONTWAIT).IsOk()) {
        rc = ClientToService(std::move(frames));
        if (rc.IsError()) {
            VLOG(RPC_LOG_LEVEL) << "Info only. rc from last rpc " << rc.ToString();
        }
    }
    return rc;
}

Status ZmqServerImpl::StartProxy()
{
    static const int K_FRONTEND = 0;
    static const int K_INTERRUPT = 1;
    static const int K_MIN_SZ = 2;
    auto items = std::make_unique<pollfd[]>(zmqfds_.size() + K_MIN_SZ);
    int idx = K_FRONTEND;
    items[idx++] = { .fd = ffd_, .events = POLLIN, .revents = 0 };
    items[idx++] = { .fd = sfd_, .events = POLLIN, .revents = 0 };
    for (auto &ele : zmqfds_) {
        items[idx++] = { .fd = ele.first, .events = POLLIN, .revents = 0 };
    }
    const auto K_SIZE = idx;
    Status rc;
    int timeout = 0;
    while (true) {
        auto n = poll(items.get(), K_SIZE, timeout);
        if (n < 0) {
            RETURN_STATUS_LOG_ERROR(StatusCode::K_UNKNOWN_ERROR, FormatString("Poll error. Errno = %d", errno));
        }

        if (IsInterrupted()) {
            VLOG(RPC_LOG_LEVEL) << "Front end shutdown";
            break;
        }

        // If we never got any event from previous iteration, wait for a full 100ms
        timeout = RPC_POLL_TIME;

        METRIC_TIMER(metrics::KvMetricId::ZMQ_SERVER_POLL_HANDLE_LATENCY);
        // revents from poll doesn't work correctly with ZMQ socket event fd.
        // We need to check the state
        unsigned int events = static_cast<unsigned int>(frontend_->GetEvents());
        if (events & ZMQ_POLLIN) {
            LOG_IF_ERROR(ZmqFrontendToSvc(), "Error in ZmqFrontendToSvc");
            timeout = 0;
        }

        if (items[K_INTERRUPT].revents & POLLIN) {
            break;
        }

        for (int i = K_MIN_SZ; i < K_SIZE; ++i) {
            if (items[i].revents & POLLIN) {
                auto fd = items[i].fd;
                auto *svc = zmqfds_[fd];
                LOG_IF_ERROR(ProcessReply(svc), FormatString("Error in routing for fd %d", fd));
                timeout = 0;
            }
        }
    }
    return Status::OK();
}

Status ZmqServerImpl::StartAllThreads()
{
    if (RpcAuthKeyManager::Instance().HasAuthHandler()) {
        RETURN_IF_NOT_OK(RpcAuthKeyManager::Instance().StartAuthHandler());
    } else if (authMechanism_ != RPC_AUTH_TYPE::NONE) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Cannot perform authentication without an auth handler!");
    }
    // Bring up the proxy. It will never return until shutdown. But underneath ZMQ function may throw.
    return StartProxy();
}

Status ZmqServerImpl::Run()
{
    CHECK_FAIL_RETURN_STATUS(!svcMap_.empty(), K_RUNTIME_ERROR, "No service is registered");
    CHECK_FAIL_RETURN_STATUS(!frontendEndPtList_.empty(), K_RUNTIME_ERROR, "No front end is defined");
    // We need a few more threads.
    auto numThreadsNeeded = 0;
    ++numThreadsNeeded;  // One for proxy.
    // Start a thread pool for these extra threads.
    RETURN_IF_EXCEPTION_OCCURS(thrdPool_ = std::make_unique<ThreadPool>(numThreadsNeeded, 0, "ZmqProxy"));
    auto func = [this] {
        if (!Thread::SetCurrentThreadNice(FLAGS_io_thread_nice)) {
            LOG(WARNING) << "Failed to set nice for ZmqServerImpl proxy thread, nice=" << FLAGS_io_thread_nice
                         << ", errno=" << errno;
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(StartAllThreads(), "ZmqServer StartAllThreads failed");
        return Status::OK();
    };
    thrd_ = std::make_unique<std::shared_future<Status>>(thrdPool_->Submit(func));
    // If all goes well, the thread will not return. Wait for 0.1 second and expect a time-out.
    auto rc = thrd_->wait_for(std::chrono::milliseconds(RPC_POLL_TIME));
    if (rc == std::future_status::ready) {
        return thrd_->get();
    }
    return Status::OK();
}

ZmqServerImpl::ZmqServerImpl(const Token key, const RpcCredential &cred)
    : ctx_(std::make_shared<ZmqContext>(FLAGS_zmq_server_io_context)),
      ffd_(ZMQ_NO_FILE_FD),
      sfd_(ZMQ_NO_FILE_FD),
      globalInterrupt_(false),
      cred_(cred),
      authMechanism_(cred_.GetAuthMechanism())
{
    (void)key;
}

Status ZmqServerImpl::Init()
{
    RETURN_IF_NOT_OK(ctx_->Init());
    frontend_ = std::make_shared<ZmqSocket>(ctx_, ZmqSocketType::ROUTER);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(frontend_->IsValid(), K_RUNTIME_ERROR,
                                         "Cannot acquire resources to initialize server");
    // Bump up the high watermark.
    RpcOptions opt;
    opt.SetHWM(0);
    RETURN_IF_NOT_OK(frontend_->UpdateOptions(opt));
    RETURN_IF_NOT_OK(frontend_->Set(sockopt::ZmqBacklog, ZMQ_SOCKET_BACKLOG));
    RETURN_IF_NOT_OK(frontend_->SetServerCredential(cred_));
    ffd_ = frontend_->GetEventFd();
    sfd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    CHECK_FAIL_RETURN_STATUS(sfd_ > 0, K_RUNTIME_ERROR,
                             FormatString("Event descriptor failed to created. Errno = %d", errno));
    poller_ = std::make_shared<ZmqEpoll>();
    RETURN_IF_NOT_OK(poller_->Init("ZmqServerImpl poller"));
    ses_ = std::make_shared<SockEventService>();
    RETURN_IF_NOT_OK(ses_->Init());
    return Status::OK();
}

Status ZmqServerImpl::Bind(const std::string &endpoint)
{
    const std::string tcpTransport = "tcp://";
    RETURN_IF_NOT_OK(frontend_->Bind(endpoint));
    std::string val = frontend_->Get(sockopt::ZmqLastEndPt);
    std::string tcpHostPort = ParseEndPt(endpoint, tcpTransport);
    if (!tcpHostPort.empty()) {
        auto pos = val.find_last_of(':');
        tcpListeningPorts_.push_back(val.substr(pos + 1));
    }
    frontendEndPtList_.push_back(val);
    return Status::OK();
}

Status ZmqServerImpl::RegisterService(Token key, ZmqService *svc, const RpcServiceCfg &svcCfg)
{
    (void)key;
    RETURN_RUNTIME_ERROR_IF_NULL(svc);
    RETURN_IF_NOT_OK(svc->Init(svcCfg, this));
    auto it = svcMap_.emplace(svc->ServiceName(), svc);
    if (!it.second) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Service already registered. Not replacing");
    }
    auto fd = svc->GetEventFd();
    zmqfds_.insert(std::make_pair(fd, svc));
    VLOG(RPC_LOG_LEVEL) << "Register " << *svc;
    return Status::OK();
}

ThreadPool::ThreadPoolUsage ZmqServerImpl::GetRpcServicesUsage(const std::string &serviceName)
{
    auto it = svcMap_.find(serviceName);
    if (it == svcMap_.end()) {
        ThreadPool::ThreadPoolUsage usage;
        return usage;
    }
    ZmqService *svc = it->second;
    return svc->thrdPool_->GetAndResetIntervalStats();
}

ThreadPool::ThreadPoolUsage ZmqServerImpl::GetRpcServicesSnapshot(const std::string &serviceName)
{
    auto it = svcMap_.find(serviceName);
    if (it == svcMap_.end()) {
        ThreadPool::ThreadPoolUsage usage;
        return usage;
    }
    ZmqService *svc = it->second;
    return svc->thrdPool_->GetThreadPoolUsage();
}

ZmqServerImpl::~ZmqServerImpl()
{
    Shutdown();
}

IOService::IOService(int id, ZmqEpoll *in, ZmqEpoll *out) : id_(id), inPoller_(in), outPoller_(out)
{
}

void IOService::Stop()
{
    WriteLock lock(&ioLock_);
    for (auto &item : outBoundHandles_) {
        auto &handle = item.second.first;
        (void)outPoller_->RemoveFd(handle);
    }
    outBoundHandles_.clear();
    for (auto &item : inBoundHandles_) {
        auto fd = item.first;
        auto &handle = item.second.first;
        auto fEvent = item.second.second;
        (void)inPoller_->RemoveFd(handle);
        RETRY_ON_EINTR(close(fd));
    }
    inBoundHandles_.clear();
}

IOService::~IOService()
{
    Stop();
}

Status IOService::Init()
{
    return Status::OK();
}

Status IOService::Send(int fd, ZmqMetaMsgFrames &p)
{
    ZmqEpollHandle handle;
    std::shared_ptr<FdEvent> ptr;
    {
        ReadLock lock(&ioLock_);
        auto it = outBoundHandles_.find(fd);
        RETURN_OK_IF_TRUE(it == outBoundHandles_.end());
        handle = it->second.first;
        ptr = it->second.second;
    }
    {
        WriteLock xlock(&ptr->outMux_);
        ptr->outMsgFrames_.push_back(std::move(p));
        RETURN_IF_NOT_OK(outPoller_->SetPollOut(handle));
    }
    return Status::OK();
}

Status IOService::ClientToService(ZmqPollEntry *pe, EventsVal events)
{
    auto ptrSp = pe->hint_.front().lock();
    CHECK_FAIL_RETURN_STATUS(ptrSp != nullptr, K_RUNTIME_ERROR, "FdEvent is deallocated");
    auto ptr = std::dynamic_pointer_cast<FdEvent>(ptrSp);
    INJECT_POINT("IOService.ClientToService.SetEvent", [&events]() {
        events |= EPOLLERR;
        return Status::OK();
    });
    if (events & (EPOLLERR | EPOLLHUP)) {
        auto fd = pe->fd_;
        VLOG(RPC_KEY_LOG_LEVEL) << "Socket " << fd << " disconnect.";
        return RemoveFd(fd);
    }

    ZmqMsgFrames frames;
    INJECT_POINT("IOService.ClientToService.wait", [](int32_t time) {
        std::this_thread::sleep_for(std::chrono::milliseconds(time));
        return Status::OK();
    });
    // We can use V2 protocol to receive. It is compatible with V1
    Status rc = ptr->decoder_->ReceiveMsgFramesV2(frames);
    if (rc.IsOk()) {
        VLOG(RPC_LOG_LEVEL) << "# of frames received " << frames.size();
        // MetaPb is embedded in the incoming socket connection. For now, pass a fake one.
        ZmqMetaMsgFrames p({ MetaPb(), std::move(frames) });
        EventType type =
            ptr->uds_ ? EventType::V1MTP : (ptr->decoder_->V2Client() ? EventType::V2MTP : EventType::V1MTP);
        // Add a new route the first time this fd has i/o
        RETURN_IF_NOT_OK(ptr->svc_->FrontendToBackend(ptr->fd_, type, p, ptr->addRoute_));
        // No need to do addRoute again.
        if (type == V2MTP) {
            ptr->addRoute_ = false;
        }
    } else if (rc.GetCode() == K_RPC_CANCELLED) {
        // For tcp/ip socket, we may not get EPOLLERR or any other events
        auto fd = pe->fd_;
        VLOG(RPC_KEY_LOG_LEVEL) << "Socket " << fd << " disconnect.";
        return RemoveFd(fd);
    } else {
        LOG(ERROR) << FormatString("Decoder error on fd %d. %s", pe->fd_, rc.ToString());
    }
    return Status::OK();
}

Status IOService::ServiceToClient(ZmqPollEntry *pe, EventsVal events)
{
    INJECT_POINT("IOService.ServiceToClient.SetEvent", [&events]() {
        events |= EPOLLERR;
        return Status::OK();
    });
    if (events & (EPOLLERR | EPOLLHUP)) {
        auto fd = pe->fd_;
        VLOG(RPC_KEY_LOG_LEVEL) << "Socket " << fd << " disconnect.";
        return RemoveFd(fd);
    }
    ZmqMetaMsgFrames p;
    auto ptrSp = pe->hint_.front().lock();
    CHECK_FAIL_RETURN_STATUS(ptrSp != nullptr, K_RUNTIME_ERROR, "FdEvent is deallocated");
    auto ptr = std::dynamic_pointer_cast<FdEvent>(ptrSp);
    {
        WriteLock xlock(&ptr->outMux_);
        if (ptr->outMsgFrames_.empty()) {
            return outPoller_->UnsetPollOut(pe->handle_);
        }
        INJECT_POINT("IOService.ServiceToClient.wait", [](int32_t time) {
            std::this_thread::sleep_for(std::chrono::milliseconds(time));
            return Status::OK();
        });
        p = std::move(ptr->outMsgFrames_.front());
        ptr->outMsgFrames_.pop_front();
    }
    MetaPb &meta = p.first;
    INJECT_POINT("IOService.CorruptMetaPb", [&meta]() {
        meta.Clear();
        return Status::OK();
    });
    CHECK_FAIL_RETURN_STATUS(meta.ticks_size() > 0, K_RUNTIME_ERROR,
                             FormatString("Incomplete MetaPb:\n%s", meta.DebugString()));
    PerfPoint::RecordElapsed(PerfKey::ZMQ_FRONTEND_TO_IOSVC, GetLapTime(meta, "ZMQ_FRONTEND_TO_IOSVC"));
    RecordTick(meta, TICK_SERVER_SEND);
    RecordServerLatencyMetrics(meta);
    ZmqMsgFrames &frames = p.second;
    TraceGuard traceGuard = SetTraceContextFromMeta(meta);
    int fd = meta.route_fd();
    // No need to prepend the gateway if it is direct connection
    RETURN_IF_NOT_OK(PushFrontProtobufToFrames(meta, frames));
    RETURN_IF_NOT_OK(PushFrontStringToFrames(meta.client_id(), frames));
    // We need to match the client protocol to be downward compatible
    Status rc = ptr->encoder_->SendMsgFrames(static_cast<EventType>(meta.event_type()), frames);
    INJECT_POINT("IOService.ServiceToClient.RPC_CANCELLED", [&rc]() {
        rc = Status(StatusCode::K_RPC_CANCELLED, "rpc cancelled");
        return Status::OK();
    });
    if (rc.GetCode() == K_RPC_CANCELLED) {
        // If the client is gone, treat it
        // as OK and move on.
        RemoveFd(fd);
        rc = Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    return Status::OK();
}

Status IOService::AddFd(int fd, ZmqService *svc, bool uds)
{
    auto hint = std::make_shared<FdEvent>(fd, svc);
    hint->uds_ = uds;
    auto inEle = ZmqCreatePollEntry(
        fd, { hint }, [this](ZmqPollEntry *pe, uint32_t events) { return ClientToService(pe, events); }, nullptr);
    auto outEle = ZmqCreatePollEntry(fd, { hint }, nullptr,
                                     [this](ZmqPollEntry *pe, uint32_t events) { return ServiceToClient(pe, events); });
    ZmqEpollHandle inHandle;
    RETURN_IF_NOT_OK(inPoller_->AddFd(fd, std::move(inEle), inHandle));
    ZmqEpollHandle outHandle;
    RETURN_IF_NOT_OK(outPoller_->AddFd(fd, std::move(outEle), outHandle));
    {
        WriteLock lock(&ioLock_);
        inBoundHandles_.emplace(fd, std::make_pair(inHandle, hint));
        outBoundHandles_.emplace(fd, std::make_pair(outHandle, hint));
    }
    RETURN_IF_NOT_OK(inPoller_->SetPollIn(inHandle));
    return Status::OK();
}

Status IOService::RemoveFd(int fd)
{
    std::shared_ptr<FdEvent> fEvent;
    {
        WriteLock lock(&ioLock_);
        auto it = outBoundHandles_.find(fd);
        if (it != outBoundHandles_.end()) {
            auto &handle = it->second.first;
            (void)outPoller_->RemoveFd(handle);
            outBoundHandles_.erase(it);
        }
        it = inBoundHandles_.find(fd);
        if (it != inBoundHandles_.end()) {
            auto &handle = it->second.first;
            (void)inPoller_->RemoveFd(handle);
            fEvent = it->second.second;
            inBoundHandles_.erase(it);
        }
    }
    if (fEvent) {
        fEvent->svc_->DeleteRoute(fd);
        INJECT_POINT("IOService.RemoveFd.wait", [](int32_t time) {
            std::this_thread::sleep_for(std::chrono::milliseconds(time));
            return Status::OK();
        });
    }
    RETRY_ON_EINTR(close(fd));
    return Status::OK();
}

Status SockEventService::Init()
{
    int numIoSvc = std::max<int>(1, FLAGS_zmq_server_io_context);
    io_ = std::make_unique<std::vector<std::unique_ptr<IOService>>>();
    io_->reserve(numIoSvc);
    pollers_ = std::make_unique<std::vector<std::unique_ptr<ZmqEpoll>>>();
    pollers_->reserve(numIoSvc);
    for (int i = 0; i < numIoSvc; ++i) {
        pollers_->emplace_back(std::make_unique<ZmqEpoll>());
        auto &poller = pollers_->back();
        RETURN_IF_NOT_OK(poller->Init(FormatString("IOService %d poller", i)));
    }
    for (int i = 0; i < numIoSvc; ++i) {
        io_->emplace_back(
            std::make_unique<IOService>(i, pollers_->at(i).get(), pollers_->at((i + 1) % numIoSvc).get()));
        auto &ioSvc = io_->back();
        RETURN_IF_NOT_OK(ioSvc->Init());
    }
    return Status::OK();
}

Status SockEventService::AddFd(ZmqService *svc, int fd, bool uds)
{
    int idx = 0;
    {
        WriteLock lock(&fdLock_);
        auto i = nextIoSvc_++;
        idx = static_cast<int>(static_cast<uint64_t>(i) % io_->size());
        fdIoSvc_[fd] = idx;
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(io_->at(idx)->AddFd(fd, svc, uds),
                                     FormatString("Service %s unable to add %d", svc->ServiceName(), fd));
    return Status::OK();
}

Status SockEventService::ServiceToClient(int fd, ZmqMetaMsgFrames &p)
{
    int idx = 0;
    {
        ReadLock lock(&fdLock_);
        idx = fdIoSvc_[fd];
    }
    auto &ioSvc = io_->at(idx);
    RETURN_IF_NOT_OK(ioSvc->Send(fd, p));
    return Status::OK();
}

SockEventService::SockEventService() : nextIoSvc_(0)
{
}

SockEventService::~SockEventService()
{
    io_.reset();
    pollers_.reset();
}

void SockEventService::Stop()
{
    if (io_) {
        for (auto &e : *io_) {
            e->Stop();
        }
    }
    if (pollers_) {
        for (auto &p : *pollers_) {
            p->Stop();
        }
    }
}
}  // namespace datasystem
