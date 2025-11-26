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
 * Description: Zmq Service.
 */
#include "datasystem/common/rpc/zmq/zmq_service.h"

#include <cstdint>
#include <utility>

#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/zmq/zmq_stub_conn.h"
#include "datasystem/common/rpc/zmq/zmq_server_impl.h"
#include "datasystem/common/util/fd_manager.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/thread_local.h"

DS_DECLARE_string(unix_domain_socket_dir);

namespace datasystem {
static const int MAX_EXCLUSIVE_CONNECTIONS_LIMIT = 128;
ZmqService::ZmqService()
    : proxy_(nullptr),
      outfd_(ZMQ_NO_FILE_FD),
      infd_(ZMQ_NO_FILE_FD),
      tcpfd_(ZMQ_NO_FILE_FD),
      exclListenFd_(ZMQ_NO_FILE_FD),
      nextWorker_(0),
      globalInterrupt_(false),
      streamSupport_(false),
      multiDestinations_(false),
      unlinkSocketPathOnExit_(false),
      tcpDirect_(false)
{
    // Two routing functions provided to the constructor of backendMgr depending on if uds/tcp
    // is enabled.
    routingFn_ = [this](const std::string &sender, ZmqMetaMsgFrames &&p) {
        MetaPb &meta = p.first;
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(meta.trace_id());
        VLOG(ZMQ_PERF_LOG_LEVEL) << FormatString("Routing reply from %s to reply queue for service %s", sender,
                                                 serviceName_);
        PerfPoint::RecordElapsed(PerfKey::ZMQ_BACKEND_TO_FRONTEND, GetLapTime(meta, "ZMQ_BACKEND_TO_FRONTEND"));
        RETURN_IF_NOT_OK(replyQueue_->Put(std::move(p)));
        eventfd_write(outfd_, 1);
        return Status::OK();
    };
    // When multiDestinations_ is true, it can go to different places, and need to call ServiceToClient
    // which can do some locking, etc. And it can take longer
    multiRoutingFn_ = [this](const std::string &sender, ZmqMetaMsgFrames &&p) {
        VLOG(ZMQ_PERF_LOG_LEVEL) << FormatString("Routing reply from %s to reply queue for service %s", sender,
                                                 serviceName_);
        RETURN_IF_NOT_OK(ServiceToClient(p));
        return Status::OK();
    };
}

ZmqService::~ZmqService()
{
    for (auto &e : fdHandles_) {
        if (e.second) {
            inBoundPoller_->RemoveFd(e.second);
        }
        RETRY_ON_EINTR(close(e.first));
    }
    fdHandles_.clear();
    if (payloadBank_ != nullptr) {
        payloadBank_.reset();
    }
    for (auto &agent : workAgents_) {
        try {
            agent->Stop();
            agent->CloseSocket();
        } catch  (const std::exception &e) {
            VLOG(ERROR) << "A work agent got an exception during Stop(): " << e.what();
        }
    }
    workAgentThreadPool_.reset();
}

Status ZmqService::CreateWorkerCBs()
{
    // Create a list of backend sockets. It doesn't have to match the number of threads.
    auto numRegularSockets = cfg_.numRegularSockets_;
    CHECK_FAIL_RETURN_STATUS(numRegularSockets > 0, StatusCode::K_INVALID, "Invalid number of backend sockets");
    for (int32_t i = 0; i < numRegularSockets; ++i) {
        auto itFlag = workerCBs_.emplace(i, std::make_shared<WorkerCB>(this, i));
        CHECK_FAIL_RETURN_STATUS(itFlag.second, StatusCode::K_RUNTIME_ERROR,
                                 "emplace workerCB fail for regular sockets");
        const auto &wb = itFlag.first->second;
        availWorkers_.push_back(wb);
    }

    auto numStreamSockets = cfg_.numStreamSockets_;
    if (streamSupport_) {
        VLOG(RPC_LOG_LEVEL) << FormatString("%s support stream rpc and the max num of stream sockets is %d",
                                            ServiceName(), numStreamSockets);
        CHECK_FAIL_RETURN_STATUS(numStreamSockets > 0, StatusCode::K_INVALID,
                                 "Invalid number of stream backend sockets");
        for (int32_t i = 0; i < numStreamSockets; ++i) {
            auto id = i + numRegularSockets;  // ID starts after the regular ones.
            auto itFlag = workerCBs_.emplace(id, std::make_shared<WorkerCB>(this, id));
            CHECK_FAIL_RETURN_STATUS(itFlag.second, StatusCode::K_RUNTIME_ERROR,
                                     "emplace workerCB fail for stream sockets");
            const auto &wb = itFlag.first->second;
            availStreamWorkers_.push_back(wb);
        }
    }
    return Status::OK();
}

Status ZmqService::AddListenFd(int fd)
{
    auto traceId = Trace::Instance().GetTraceID();
    auto pollEntry = ZmqCreatePollEntry(fd, {}, [this, traceId](ZmqPollEntry *pe, uint32_t events) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        if (events & (EPOLLERR | EPOLLHUP)) {
            // On shutdown.
            return Status::OK();
        }
        return ProcessAccept(pe->fd_);
    });
    ZmqEpollHandle handle;
    RETURN_IF_NOT_OK(inBoundPoller_->AddFd(fd, std::move(pollEntry), handle));
    fdHandles_.emplace(fd, handle);
    RETURN_IF_NOT_OK(inBoundPoller_->SetPollIn(handle));
    return Status::OK();
}

Status ZmqService::BindTcpIpPort(const std::vector<std::string> &frontendEndPtList)
{
    // Look for the interface the parent is binding to
    const std::string tcpTransport = "tcp://";
    std::string tcpHostPort;
    for (auto &e : frontendEndPtList) {
        auto res = ParseEndPt(e, tcpTransport);
        if (!res.empty()) {
            auto pos = res.find_last_of(':');
            tcpHostPort = FormatString("%s:%s", res.substr(0, pos), cfg_.tcpDirect_);
            break;
        }
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!tcpHostPort.empty(), K_UNKNOWN_ERROR,
                                         "Parent server not binding to any tcp/ip port");
    UnixSockFd sock;
    RETURN_IF_NOT_OK(sock.CreateTcpIpSocket());
    sockaddr_in addr{};
    RETURN_IF_NOT_OK(sock.SetUpTcpIpAddr(tcpHostPort, addr));
    RETURN_IF_NOT_OK(sock.Bind(addr));
    RETURN_IF_NOT_OK(sock.GetBindingHostPort(tcpHostPort_));
    tcpfd_ = sock.GetFd();
    RETURN_IF_NOT_OK(AddListenFd(tcpfd_));
    return Status::OK();
}

Status ZmqService::BindUnixPath()
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!cfg_.udsDirectory_.empty(), K_RUNTIME_ERROR,
                                         "unix_domain_socket_dir is empty");
    RETURN_IF_NOT_OK(Uri::NormalizePathWithUserHomeDir(cfg_.udsDirectory_, FLAGS_unix_domain_socket_dir,
                                                       GetStringUuid().substr(0, ZMQ_EIGHT)));
    const std::string &path = cfg_.udsDirectory_;
    Status rc = CreateDir(path, true, 0750);
    if (!rc.IsOk() && !FileExist(path)) {
        RETURN_STATUS(rc.GetCode(), rc.GetMsg());
    }
    std::string upLevelPath = cfg_.udsDirectory_;
    RETURN_IF_NOT_OK(Uri::DeleteAppendPath(upLevelPath));
    // Change permission of ~/datasystem/unix_domain_socket_dir to 0750.
    const mode_t permission = 0750;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(chmod(upLevelPath.c_str(), permission) == 0, K_RUNTIME_ERROR,
                                         FormatString("Chmod of %s error.", upLevelPath));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!cfg_.sockList_.empty(), K_RUNTIME_ERROR, "sockList is empty");
    for (auto &ele : cfg_.sockList_) {
        sockaddr_un addr{};
        auto sockPath = FormatString("%s/%s", path, ele.first);
        unlink(sockPath.data());
        UnixSockFd sockfd;
        RETURN_IF_NOT_OK(sockfd.CreateUnixSocket());
        RETURN_IF_NOT_OK(UnixSockFd::SetUpSockPath(sockPath, addr));
        RETURN_IF_NOT_OK(sockfd.Bind(addr, ele.second));
        RETURN_IF_NOT_OK(sockfd.SetNonBlocking());
        auto listenFd = sockfd.GetFd();
        RETURN_IF_NOT_OK(AddListenFd(listenFd));
        sockPath_.emplace_back(sockPath);
    }

    // create a exclusive connection listener fd.
    sockaddr_un addr{};
    auto exclSockPath = FormatString("%s/%s", path, "exclusiveConn");
    unlink(exclSockPath.data());
    UnixSockFd tempsockfd;
    RETURN_IF_NOT_OK(tempsockfd.CreateUnixSocket());
    RETURN_IF_NOT_OK(UnixSockFd::SetUpSockPath(exclSockPath, addr));
    RETURN_IF_NOT_OK(tempsockfd.Bind(addr, RPC_SOCK_MODE));
    RETURN_IF_NOT_OK(tempsockfd.SetNonBlocking());
    exclListenFd_ = tempsockfd.GetFd();
    RETURN_IF_NOT_OK(AddListenFd(exclListenFd_));
    sockPath_.emplace_back(exclSockPath);
    exclSockPath_ = exclSockPath;
    return Status::OK();
}

Status ZmqService::InitEventLoop()
{
    outfd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(outfd_ > 0, K_RUNTIME_ERROR,
                                         "Unable to create event fd. Errno: " + std::to_string(errno));
    fdHandles_.emplace(outfd_, nullptr);

    infd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(infd_ > 0, K_RUNTIME_ERROR,
                                         "Unable to create event fd. Errno: " + std::to_string(errno));
    fdHandles_.emplace(infd_, nullptr);
    VLOG(RPC_LOG_LEVEL) << FormatString("Adding event fd %d to event loop for service %s", infd_, serviceName_);

    // Add the input queue to epoll. The output queue will be monitored by ZmqServiceImpl,
    // if we route the reply through that queue.
    auto ele = ZmqCreatePollEntry(infd_, {}, [this](ZmqPollEntry *pe, uint32_t events) {
        (void)pe;
        (void)events;
        HandleRqFromProxy();
        return Status::OK();
    });
    ZmqEpollHandle handle = nullptr;
    RETURN_IF_NOT_OK(inBoundPoller_->AddFd(infd_, std::move(ele), handle));
    fdHandles_[infd_] = handle;
    RETURN_IF_NOT_OK(inBoundPoller_->SetPollIn(handle));

    return Status::OK();
}

Status ZmqService::ValidateAndAdjustCfg()
{
    bool hasPayloadMethods = false;
    for (auto &method : methodMap_) {
        if (method.second->ServerStreaming() || method.second->ClientStreaming()) {
            streamSupport_ = true;
        }
        if (method.second->HasPayloadRecvOption()) {
            methodsHavePayloadRecv_.insert(method.second->MethodIndex());
            hasPayloadMethods = true;
        }
        if (method.second->HasPayloadSendOption()) {
            hasPayloadMethods = true;
        }
    }

    if (hasPayloadMethods) {
        payloadBank_ = std::make_unique<ZmqPayloadBank>();
    }

    if (streamSupport_) {
        CHECK_FAIL_RETURN_STATUS(cfg_.numStreamSockets_ > 0, StatusCode::K_INVALID,
                                 "Invalid number of backend sockets");
    } else {
        cfg_.numStreamSockets_ = 0;
    }
    auto numSockets = cfg_.numRegularSockets_ + cfg_.numStreamSockets_;
    CHECK_FAIL_RETURN_STATUS(numSockets > 0, StatusCode::K_INVALID, "Invalid number of backend sockets");
    // Technically if the service contains stream rpc only, the numRegularSockets_ can be 0. But to make life
    // easier, we will adjust to be at least 1.
    cfg_.numRegularSockets_ = std::max<int>(1, cfg_.numRegularSockets_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(cfg_.hwm_ > 0, K_INVALID, "HWM must not be zero or negative");
    // Verify cfg_.tcpDirect_
    if (cfg_.tcpDirect_ != "*") {
        int port;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(StringToInt(cfg_.tcpDirect_, port), K_INVALID,
                                             FormatString("Invalid tcp/ip port %s", cfg_.tcpDirect_));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(port >= 0, K_INVALID,
                                             FormatString("Invalid tcp/ip port %s", cfg_.tcpDirect_));
        tcpDirect_ = (port > 0);
    } else {
        tcpDirect_ = true;
    }
    // If uds is enabled, a reply can be either routed back to ZmqServerImpl or directly sent back to client
    multiDestinations_ = cfg_.udsEnabled_ || tcpDirect_;
    return Status::OK();
}

Status ZmqService::CreateBackendMgr()
{
    auto unaryQueSize = cfg_.hwm_ / cfg_.numRegularSockets_;
    auto remainder = cfg_.hwm_ % cfg_.numRegularSockets_;
    if (unaryQueSize == 0 || remainder > 0) {
        unaryQueSize += 1;
    }
    VLOG(RPC_LOG_LEVEL) << FormatString("HWM of %s unary workerCB: %d", ServiceName(), unaryQueSize);
    // Another thing to calculate the size of each workerCB queue if we have stream support.
    // We need to calculate them separately.
    size_t streamQueSize = 0;
    if (streamSupport_) {
        streamQueSize = static_cast<size_t>(cfg_.hwm_ / cfg_.numStreamSockets_);
        remainder = cfg_.hwm_ % cfg_.numStreamSockets_;
        if (streamQueSize == 0 || remainder > 0) {
            streamQueSize += 1;
        }
        VLOG(RPC_LOG_LEVEL) << FormatString("HWM of %s stream workerCB: %zu", ServiceName(), streamQueSize);
    }

    // Create backend which is used for both unary and stream
    auto numSockets = cfg_.numRegularSockets_ + cfg_.numStreamSockets_;
    // We can just put the reply directly in the reply queue when there is no other place to go for the reply
    backendMgr_ = std::make_shared<ZmqServerMsgMgr>((multiDestinations_ ? multiRoutingFn_ : routingFn_),
                                                    std::min<int>(RPC_NUM_BACKEND, numSockets));
    RETURN_IF_NOT_OK(backendMgr_->Init());
    RETURN_IF_NOT_OK(CreateWorkerCBs());
    RETURN_IF_NOT_OK(ConnectToBackend(backendMgr_, unaryQueSize, streamQueSize));
    return Status::OK();
}

Status ZmqService::InitThreadPool()
{
    // Start a thread pool to handle incoming requests.
    auto numThreads = cfg_.numRegularSockets_ + cfg_.numStreamSockets_;
    // We need to account for the streaming threads. They can run forever until the server shutdown.
    const int minThread = 1 + cfg_.numStreamSockets_;
    const int initMaxThread = 8;
    auto minThreadNum = numThreads / minThread;
    minThreadNum = minThreadNum > minThread ? minThreadNum : minThread;
    minThreadNum = minThreadNum > initMaxThread ? initMaxThread : minThreadNum;
    VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Creating a thread pool with min %d max %d threads for %s", minThreadNum,
                                            numThreads, ServiceName());
    RETURN_IF_EXCEPTION_OCCURS(thrdPool_ = std::make_unique<ThreadPool>(minThreadNum, numThreads, ServiceName()));
    VLOG(RPC_KEY_LOG_LEVEL) << "All backend " << numThreads << " threads start up successfully for service "
                            << ServiceName();
    return Status::OK();
}

Status ZmqService::Init(RpcServiceCfg cfg, void *proxy)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    auto traceId = Trace::Instance().GetTraceID();
    proxy_ = proxy;
    cfg_ = std::move(cfg);
    RETURN_IF_NOT_OK(ValidateAndAdjustCfg());
    // Backend manager
    RETURN_IF_NOT_OK(CreateBackendMgr());
    // Two in/out queues for request that is route from/to ZmqServiceImpl.
    // We have an input queue (if routed from ZmqServerImpl) and each workerCB has its queue.
    // Because we may or may not be using the input queue, we will make it a bit small and
    // evenly distribute hwm_ among the workerCB.
    // The input queue can be smaller for the reasons mentioned above.
    rqQueue_ = std::make_unique<Queue<ZmqMetaMsgFrames>>(RPC_LIGHT_SERVICE_HWM);
    replyQueue_ = std::make_unique<Queue<ZmqMetaMsgFrames>>(cfg_.hwm_);

    // Use the one supplied by the proxy. Or we can create our own if this service is too congested. The
    // cost is only one extra thread.
    inBoundPoller_ = reinterpret_cast<ZmqServerImpl *>(proxy_)->GetPoller();
    RETURN_IF_NOT_OK(InitEventLoop());

    // Turn on per-service uds support.
    if (cfg_.udsEnabled_) {
        RETURN_IF_NOT_OK(BindUnixPath());
        unlinkSocketPathOnExit_ = true;
    } else {
        cfg_.udsDirectory_.clear();
    }

    if (tcpDirect_) {
        RETURN_IF_NOT_OK(BindTcpIpPort(reinterpret_cast<ZmqServerImpl *>(proxy_)->GetFrontendEndPtList()));
        LOG(INFO) << FormatString("Service %s successfully binds to %s. fd = %d", serviceName_, tcpHostPort_.ToString(),
                                  tcpfd_);
    }

    // Use the one supplied by the proxy. Or we can create our own if this service is too congested.
    io_ = reinterpret_cast<ZmqServerImpl *>(proxy_)->GetSockEventService();

    // Start the threads
    RETURN_IF_NOT_OK(InitThreadPool());

    // Start the HWM monitoring
    StartHWMTimer();

    return Status::OK();
}

void ZmqService::Stop()
{
    globalInterrupt_ = true;

    for (auto &it : workerCBs_) {
        it.second->CloseSocket();
    }
    thrdPool_.reset();

    if (backendMgr_) {
        backendMgr_->Stop();
    }

    if (unlinkSocketPathOnExit_) {
        for (auto &s : sockPath_) {
            unlink(s.data());
        }
        // Remove the directory if it is empty. If the directory is not empty, rmdir will
        // fail and therefore there is no need to check for error.
        rmdir(cfg_.udsDirectory_.data());
    }

    if (hwmCheck_) {
        TimerQueue::GetInstance()->Cancel(*hwmCheck_);
        hwmCheck_.reset();
    }
    VLOG(RPC_LOG_LEVEL) << "Service '" << this->ServiceName() << "' shutdown";
}

Status ZmqService::SendAll(ZmqMsgFramesRef &frames, const MetaPb &meta,
                           const std::function<Status(ZmqMetaMsgFramesRef)> &f)
{
    ZmqMetaMsgFrames p(std::make_pair(meta, std::move(frames)));
    return f(p);
}

Status ZmqService::SendStatus(ZmqMsgFramesRef &frames, const MetaPb &meta, const Status &rc,
                              const std::function<Status(ZmqMetaMsgFramesRef)> &f)
{
    ZmqMessage rcMsg = StatusToZmqMessage(rc);
    frames.push_front(std::move(rcMsg));
    return SendAll(frames, meta, f);
}

Status ZmqService::ConnectToBackend(std::shared_ptr<ZmqServerMsgMgr> &backend, size_t unaryHWM, size_t streamHWM)
{
    for (auto &wb : availWorkers_) {
        RETURN_IF_NOT_OK(wb->ConnectToBackend(backend, unaryHWM));
    }
    for (auto &wb : availStreamWorkers_) {
        RETURN_IF_NOT_OK(wb->ConnectToBackend(backend, streamHWM));
    }
    return Status::OK();
}

std::string ZmqService::GetWorkerIdStr(int32_t i) const
{
    auto myAddr = reinterpret_cast<intptr_t>(this);
    std::ostringstream os;
    const int WIDTH = 4;
    os << WORKER_PREFIX << std::to_string(myAddr) << ":" << std::setfill('0') << std::setw(WIDTH) << i;
    return os.str();
}

std::map<int32_t, std::string> ZmqService::GetAvailWorkerId() const
{
    std::map<int32_t, std::string> v;
    for (int32_t i = 0; i < cfg_.numRegularSockets_; ++i) {
        v.emplace(i, GetWorkerIdStr(i));
    }
    return v;
}

void ZmqService::UpdateStreamWorkerSeqNo(int32_t id, int64_t nextSeqNo)
{
    auto streamWorker = workerCBs_[id];
    auto &streamWA = streamWorker->GetStreamWA();
    auto state = streamWA.state_;
    if (state == WorkerCB::StreamState::END_OF_STREAM || nextSeqNo == std::numeric_limits<int64_t>::max()) {
        VLOG(RPC_LOG_LEVEL) << "Socket (" << StreamGetSvcName() << "," << id << ") is free to be recycled";
        streamWorker->GetStreamWA().Reset();
    } else if (state == WorkerCB::StreamState::IN_USE) {
        VLOG(RPC_LOG_LEVEL) << FormatString("Socket (%s,%d) will resume at %d the next restart", StreamGetSvcName(), id,
                                            nextSeqNo);
        streamWA.nextSeqNo_ = nextSeqNo;
    }
}

void ZmqService::MarkStreamWorkerAvailable(int32_t id)
{
    auto streamWorker = workerCBs_[id];
    std::unique_lock<std::mutex> lock(streamLock_);
    availStreamWorkers_.push_back(streamWorker);
    streamCv_.notify_all();
}

Status ZmqService::GetAvailStreamWorker(MetaPb meta, std::string &workerId)
{
    // This function is called by the internal method ZMQ_STREAM_WORKER_METHOD which
    // itself is driven by a ZMQ thread in the thread pool. Without blocking this thread,
    // we will wait for a 100ms and if no socket is available, we will return TRY_AGAIN.
    // This return code will be sent back to the stub class. The logic in the stub class
    // for GetStreamPeer will try again later.
    std::unique_lock<std::mutex> lock(streamLock_);
    auto rc = streamCv_.wait_for(lock, std::chrono::milliseconds(RPC_POLL_TIME),
                                 [this]() { return !availStreamWorkers_.empty(); });
    CHECK_FAIL_RETURN_STATUS(rc, StatusCode::K_TRY_AGAIN, "No available stream socket");
    auto streamWorker = std::move(availStreamWorkers_.front());
    availStreamWorkers_.pop_front();
    workerId = streamWorker->GetWorkerId();
    meta.set_worker_id(workerId);
    auto &streamWA = streamWorker->GetStreamWA();
    streamWA.Reset();
    streamWA.meta_ = meta;
    streamWA.state_ = WorkerCB::StreamState::CREATE;
    auto traceID = meta.trace_id();
    thrdPool_->Execute([=]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        auto status = streamWorker->StreamWorkerEntry();
        if (status.IsError()) {
            LOG(ERROR) << status.ToString();
        }
    });
    streamWA.cv_.notify_all();
    return Status::OK();
}

ZmqService::WorkerCB::WorkerCB(ZmqService *svc, int32_t id) : impl_(svc), workerId_(id)
{
}

ZmqService::WorkerCB::~WorkerCB() = default;

Status ZmqService::WorkerCB::ConnectToBackend(std::shared_ptr<ZmqServerMsgMgr> &mgr, int hwm)
{
    ZmqServerMsgQueRef mQue;
    RpcOptions opts;
    opts.SetTimeout(RPC_BACKEND_TIMEOUT);
    opts.SetHWM(hwm);
    // Change the id.
    std::string workerIdStr = impl_->GetWorkerIdStr(workerId_);
    RETURN_IF_NOT_OK(mgr->CreateMsgQWithName(mQue, workerIdStr, opts));
    worker_ = std::make_shared<ZmqServerMsgQueRef>(std::move(mQue));
    return Status::OK();
}

Status ZmqService::WorkerCB::ProcessStreamRpcRq(const MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg)
{
    auto *svc = impl_;
    MetaPb m;
    CHECK_FAIL_RETURN_STATUS(!inMsg.empty(), StatusCode::K_INVALID, "Invalid stream.");
    RETURN_IF_NOT_OK(ParseFromZmqMessage(inMsg.front(), m));
    inMsg.pop_front();
    // Sanity check
    auto methodIndex = m.method_index();
    auto methodIterator = svc->methodMap_.find(methodIndex);
    CHECK_FAIL_RETURN_STATUS(methodIterator != svc->methodMap_.end(), K_RUNTIME_ERROR, "Method not found");
    auto &methodObj = methodIterator->second;
    CHECK_FAIL_RETURN_STATUS(methodObj->ClientStreaming() || methodObj->ServerStreaming(), K_RUNTIME_ERROR,
                             "Not streaming method");
    m.set_gateway_id(meta.gateway_id());
    m.set_route_fd(meta.route_fd());
    m.set_event_type(meta.event_type());
    std::string workerId;
    PerfPoint point(PerfKey::ZMQ_GET_STREAM_WORKER);
    RETURN_IF_NOT_OK(svc->GetAvailStreamWorker(m, workerId));
    CHECK_FAIL_RETURN_STATUS(!workerId.empty(), StatusCode::K_RUNTIME_ERROR, "Invalid worker id");
    point.Record();
    VLOG(RPC_LOG_LEVEL) << FormatString("Stream worker %s serving service %s method %s.", workerId, svc->ServiceName(),
                                        methodObj->MethodName());
    RETURN_IF_NOT_OK(PushBackStringToFrames(workerId, replyMsg));
    return Status::OK();
}

Status ZmqService::WorkerCB::ProcessHandshakeRq(const MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg)
{
    VLOG(RPC_LOG_LEVEL) << "Server process payload handshake.";
    const int expectedSize = 3;
    CHECK_FAIL_RETURN_STATUS(inMsg.size() == expectedSize, StatusCode::K_INVALID, "Invalid handshake request.");
    HandshakePb rq;
    RETURN_IF_NOT_OK(ParseFromZmqMessage(inMsg.front(), rq));
    inMsg.pop_front();
    // Next one is the original MetaPb
    MetaPb m;
    RETURN_IF_NOT_OK(ParseFromZmqMessage(inMsg.front(), m));
    inMsg.pop_front();
    m.set_gateway_id(meta.gateway_id());
    m.set_route_fd(meta.route_fd());
    m.set_event_type(meta.event_type());
    // Create a bank entry
    HandshakeTokenPb reply;
    RETURN_IF_NOT_OK(impl_->payloadBank_->CreateAsyncPayloadEntry(rq, m, inMsg, reply));
    RETURN_IF_NOT_OK(PushBackProtobufToFrames(reply, replyMsg));
    return Status::OK();
}

Status ZmqService::WorkerCB::ProcessPayloadRq(const MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg)
{
    VLOG(RPC_LOG_LEVEL) << "Server process parallel payload send.";
    // Currently send no acks back
    (void)replyMsg;
    PayloadDirectGetRspPb rq;
    CHECK_FAIL_RETURN_STATUS(!inMsg.empty(), StatusCode::K_INVALID, "Invalid stream.");
    RETURN_IF_NOT_OK(ParseFromZmqMessage(inMsg.front(), rq));
    inMsg.pop_front();
    // imMsg should be empty.
    CHECK_FAIL_RETURN_STATUS(inMsg.empty(), StatusCode::K_INVALID, "Invalid stream.");
    auto idx = meta.payload_index();
    AsyncPayloadEntry *entry = nullptr;
    {
        ReadLock rlock(&impl_->payloadBank_->mux_);
        auto &bank = impl_->payloadBank_->bank_;
        auto it = bank.find(idx);
        CHECK_FAIL_RETURN_STATUS(it != bank.end(), K_NOT_FOUND, FormatString("payload index %d not found", idx));
        entry = std::get<ZmqPayloadBank::COL::K_ASYNC>(it->second).get();
    }
    // Now we decrement the pending count to see who is the last.
    auto lastMan = entry->numPending.fetch_sub(1);
    // We don't send back any ack but we need to decrement the pending count.
    // The last one complete the receive.
    if (lastMan == 1) {
        BankColumns ele = impl_->payloadBank_->GetAndErase(idx);
        auto &timer = std::get<ZmqPayloadBank::COL::K_TIMER>(ele);
        (void)TimerQueue::GetInstance()->Cancel(*timer);
        entry = std::get<ZmqPayloadBank::COL::K_ASYNC>(ele).get();
        ZmqMsgFrames origInMsg = std::move(std::get<ZmqPayloadBank::COL::K_MSG>(ele));
        MetaPb &origMetaPb = entry->meta;
        size_t bufSz = 0;
        RETURN_IF_NOT_OK(ZmqPayload::AddPayloadFrames(entry->recvBuf, origInMsg, bufSz, false));
        VLOG(RPC_LOG_LEVEL) << FormatString("Worker %s started for service '%s' Method %d serving %s", GetWorkerId(),
                                            origMetaPb.svc_name(), origMetaPb.method_index(), origMetaPb.client_id());
        RETURN_IF_NOT_OK(impl_->CallMethod(worker_, origMetaPb, std::move(origInMsg), 0));
    }
    return Status::OK();
}

Status ZmqService::WorkerCB::HandleInternalRq(int fd, const MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg)
{
    (void)fd;
    // FrontendToBackend has already filtered those it can handle plus the invalid ones.
    // We only have to deal the remaining.
    const int idx = meta.method_index();
    switch (idx) {
        case ZMQ_STREAM_WORKER_METHOD: {
            RETURN_IF_NOT_OK(ProcessStreamRpcRq(meta, inMsg, replyMsg));
            break;
        }
        case ZMQ_PAYLOAD_TICK_METHOD: {
            // The caller WorkerEntry has already updated the tick count,
            // so just send back everything.
            RETURN_IF_NOT_OK(PushBackProtobufToFrames(meta, replyMsg));
            for (auto &ele : inMsg) {
                replyMsg.push_back(std::move(ele));
            }
            break;
        }
        case ZMQ_PAYLOAD_HANDSHAKE_METHOD: {
            RETURN_OK_IF_TRUE(ProcessHandshakeRq(meta, inMsg, replyMsg));
            break;
        }
        case ZMQ_PAYLOAD_PUT_METHOD: {
            RETURN_IF_NOT_OK(ProcessPayloadRq(meta, inMsg, replyMsg));
            break;
        }
        default:
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Unexpected internal call");
    }
    return Status::OK();
}

Status ZmqService::WorkerCB::WorkerEntryImpl(MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg)
{
    int fd = meta.route_fd();
    const int idx = meta.method_index();
    if (idx >= 0) {
        auto remainingTime = reqTimeoutDuration.CalcRealRemainingTime();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
            FormatString("The remaining time of the RPC request is %lld, which has exceeded the "
                         "deadline and cannot be continued.",
                         remainingTime));
        // There is one more protobuf after, but we can't parse it (yet) and leave
        // it to the lower level to decode. Also note if the client side is streaming,
        // it will be handled by StreamWorkEntry.
        return impl_->CallMethod(worker_, meta, std::move(inMsg), 0);
    }
    // The rest are internal services with negative method indexes.
    RETURN_IF_NOT_OK(HandleInternalRq(fd, meta, inMsg, replyMsg));
    // skip the send for ProcessPayloadRq as of now
    RETURN_OK_IF_TRUE(replyMsg.empty());
    PerfPoint::RecordElapsed(PerfKey::ZMQ_INTERNAL_WORKLOAD, GetLapTime(meta, "ZMQ_INTERNAL_WORKLOAD"));
    RETURN_IF_NOT_OK(ZmqService::SendStatus(replyMsg, meta, Status::OK(),
                                            [this](ZmqMetaMsgFramesRef e) { return worker_->SendMsg(e); }));
    return Status::OK();
}

/**
 * Entry function for each worker thread in the pool.
 */
Status ZmqService::WorkerCB::WorkerEntry()
{
    ReadLock rlock(&inUse_);
    ZmqMetaMsgFrames inMsg;
    ZmqMsgFrames replyMsg;
    RETURN_IF_NOT_OK(worker_->ReceiveMsg(inMsg));
    MetaPb &meta = inMsg.first;
    // Check point.
    if (impl_->globalInterrupt_) {
        return Status::OK();
    }
    PerfPoint::RecordElapsed(PerfKey::ZMQ_FRONTEND_TO_WORKER, GetLapTime(meta, "ZMQ_FRONTEND_TO_WORKER"));
    VLOG(RPC_LOG_LEVEL) << FormatString("Worker %s started for service '%s' Method %d serving %s", GetWorkerId(),
                                        meta.svc_name(), meta.method_index(), meta.client_id());
    Status rc = WorkerEntryImpl(meta, inMsg.second, replyMsg);
    VLOG(RPC_LOG_LEVEL) << "Service '" << impl_->ServiceName() << "' Method " << meta.method_index() << " rc "
                        << rc.ToString();
    // The response will be sent by the calling method. We can just exit and ignore the rc above
    // except lower level code hit some error and didn't send anything back to the client.
    if (rc.IsError()) {
        replyMsg.clear();
        RETURN_IF_NOT_OK(
            ZmqService::SendStatus(replyMsg, meta, rc, [this](ZmqMetaMsgFramesRef e) { return worker_->SendMsg(e); }));
    }
    return Status::OK();
}

Status ZmqService::WorkerCB::WorkerEntryWithoutMsgQ(ZmqMetaMsgFrames &inMsg, ZmqMetaMsgFrames &outMsg)
{
    ReadLock rlock(&inUse_);
    MetaPb &meta = inMsg.first;
    // Check point.
    if (impl_->globalInterrupt_) {
        return Status::OK();
    }
    VLOG(RPC_LOG_LEVEL) << FormatString("Worker %s started for service '%s' Method %d serving %s", GetWorkerId(),
                                        meta.svc_name(), meta.method_index(), meta.client_id());
    ZmqMsgFrames replyMsg;
    Status rc;
    const int idx = meta.method_index();
    if (idx >= 0) {
        // There is one more protobuf after, but we can't parse it (yet) and leave
        // it to the lower level to decode. Also note if the client side is streaming,
        // it will be handled by StreamWorkEntry.
        rc = impl_->DirectCallMethod(meta, std::move(inMsg.second), 0, replyMsg);
    }
    VLOG(RPC_LOG_LEVEL) << "Service '" << impl_->ServiceName() << "' Method " << meta.method_index() << " rc "
                        << rc.ToString();
    outMsg.first = std::move(meta);
    outMsg.second = std::move(replyMsg);
    return Status::OK();
}

Status ZmqService::WorkerCB::StreamWorkerEntryImpl()
{
    Status rc;
    const std::string workerId = GetWorkerId();
    // Wait on the cv
    std::unique_lock<std::mutex> lock(streamWA_.mux_);
    bool hasData = streamWA_.cv_.wait_for(lock, std::chrono::milliseconds(RPC_POLL_TIME),
                                          [this]() { return worker_->PollRecv(0).IsOk(); });
    auto state = streamWA_.state_;
    CHECK_FAIL_RETURN_STATUS(state == StreamState::IN_USE || state == StreamState::CREATE, K_RPC_STREAM_END,
                             FormatString("Unexpected state. Current state is %d", static_cast<int>(state)));
    // Start a new stream rpc
    if (state == StreamState::CREATE) {
        streamWA_.nextSeqNo_ = 0;
        MetaPb &meta = streamWA_.meta_;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            meta.worker_id() == workerId, StatusCode::K_RUNTIME_ERROR,
            "Worker id doesn't match. Expect " + meta.worker_id() + " but get " + workerId);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            meta.svc_name() == impl_->ServiceName(), StatusCode::K_RUNTIME_ERROR,
            "Service name doesn't match. Expect " + meta.svc_name() + " but get " + impl_->ServiceName());
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!meta.client_id().empty(), StatusCode::K_RUNTIME_ERROR, "Empty client id");
        VLOG(RPC_LOG_LEVEL) << FormatString(
            "Stream worker %s started calling service %s Method %d for client %s at gateway %s", workerId,
            impl_->ServiceName(), meta.method_index(), meta.client_id(), meta.gateway_id());
        // We are going to call the virtual function unconditionally
        hasData = true;
        // Move the state to IN_USE
        streamWA_.state_ = StreamState::IN_USE;
    }
    // If idle, check if it has expired already.
    if (!hasData && state == StreamState::IN_USE) {
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - streamWA_.lastAccessTime_).count();
        bool expired = static_cast<uint64_t>(duration) > FLAGS_stream_idle_time_s;
        // If expired and nothing in the queue, mark ourselves available, and return the thread to the pool
        if (expired) {
            std::stringstream oss;
            oss << FormatString("Stream worker %s for %s expired after %d seconds", workerId, impl_->ServiceName(),
                                FLAGS_stream_idle_time_s);
            LOG(INFO) << oss.str();
            streamWA_.state_ = StreamState::END_OF_STREAM;
            impl_->UpdateStreamWorkerSeqNo(workerId_, 0);
            RETURN_STATUS(K_RPC_STREAM_END, oss.str());
        }
        return Status::OK();
    }
    // We are going to call the user virtual function. Update the last access time.
    streamWA_.lastAccessTime_ = std::chrono::steady_clock::now();
    // Now pass the control down.
    MetaPb &meta = streamWA_.meta_;
    rc = impl_->CallMethod(worker_, meta, ZmqMsgFrames(), streamWA_.nextSeqNo_);
    VLOG(RPC_LOG_LEVEL) << FormatString("[%s] Service '%s' Method %d rc %s. Current state %d", workerId,
                                        impl_->StreamGetSvcName(), meta.method_index(), rc.ToString(),
                                        static_cast<int>(streamWA_.state_));
    // The destructor of ServerWriterReader can change the state. Check if the stream has ended.
    CHECK_FAIL_RETURN_STATUS(streamWA_.state_ != StreamState::NONE, K_RPC_STREAM_END, "Stream ends");
    return Status::OK();
}

Status ZmqService::WorkerCB::StreamWorkerEntry()
{
    Status rc;
    ReadLock rlock(&inUse_);
    VLOG(RPC_LOG_LEVEL) << FormatString("Streaming thread %s starts", GetWorkerId());
    do {
        rc = StreamWorkerEntryImpl();
    } while (rc.GetCode() != K_RPC_STREAM_END && !impl_->globalInterrupt_);
    impl_->MarkStreamWorkerAvailable(workerId_);
    VLOG(RPC_LOG_LEVEL) << FormatString("Streaming thread %s exits", GetWorkerId());
    return Status::OK();
}

std::string ZmqService::WorkerCB::GetWorkerId() const
{
    return worker_->GetId();
}

Status ZmqService::ParseWorkerId(const std::string &workerId, ZmqService *&svc, int32_t &id)
{
    std::string prefix(WORKER_PREFIX);
    CHECK_FAIL_RETURN_STATUS(workerId.compare(0, prefix.size(), prefix) == 0, StatusCode::K_RUNTIME_ERROR,
                             "Parsing error on " + workerId);
    std::string rest = workerId.substr(prefix.length());
    auto pos = rest.find_last_of(':');
    CHECK_FAIL_RETURN_STATUS(pos != std::string::npos, StatusCode::K_RUNTIME_ERROR, "Parsing error on " + workerId);
    std::string suffix = rest.substr(pos + 1);
    rest.resize(pos);
    try {
        id = std::stoi(suffix);
        intptr_t addr = std::stol(rest);
        svc = reinterpret_cast<ZmqService *>(addr);
        CHECK_FAIL_RETURN_STATUS(svc != nullptr, StatusCode::K_RUNTIME_ERROR, "Parsing error on " + workerId);
    } catch (std::invalid_argument const &ex) {
        std::stringstream ss;
        ss << "std::invalid_argument::what(): " << ex.what() << '\n';
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR, ss.str());
    } catch (std::out_of_range const &ex) {
        std::stringstream ss;
        ss << "std::out_of_range::what(): " << ex.what() << '\n';
        ss << FormatString("Id LL: %s, Addr LongLong: %s", suffix, rest);
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR, ss.str());
    }
    return Status::OK();
}

Status ZmqService::ProcessPayloadGetRq(MetaPb &meta, ZmqMsgFrames &inMsg, ZmqMsgFrames &replyMsg)
{
    PayloadDirectGetReqPb rq;
    ZmqMessage msg = std::move(inMsg.front());
    inMsg.pop_front();
    RETURN_IF_NOT_OK(ParseFromZmqMessage(msg, rq));
    // Get the return code from the receiver. It may get OOM and in that case
    // we only need to clean up our parked payload
    Status rc(static_cast<StatusCode>(rq.error_code()), rq.error_msg());
    // We park the payload elsewhere, and we will use payload id to get it.
    BankColumns entry = payloadBank_->GetAndErase(rq.id());
    RETURN_OK_IF_TRUE(rc.IsError() && rc.GetCode() != K_NOT_READY);
    if (rc.IsOk()) {
        PayloadDirectGetRspPb reply;
        reply.set_addr(rq.addr());
        reply.set_sz(rq.sz());
        RETURN_IF_NOT_OK(PushFrontProtobufToFrames(reply, replyMsg));
        // Need to tag this special message for V2MTP
        replyMsg.front().SetType(ZmqMessage::ZmqMsgType::DECODER);
        // If it is coming from the ZMQ proxy, force to use the V2MTP
        if (meta.event_type() == EventType::ZMQ) {
            int fd = std::get<ZmqPayloadBank::COL::K_FD>(entry);
            meta.set_event_type(EventType::V2MTP);
            meta.set_route_fd(fd);
            VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Choosing fd %d for V2MTP for service %s client %s", fd,
                                                    meta.svc_name(), meta.client_id());
        }
    }
    size_t sz = 0;
    auto payload = std::move(std::get<ZmqPayloadBank::COL::K_MSG>(entry));
    while (!payload.empty()) {
        auto &ele = payload.front();
        sz += ele.Size();
        replyMsg.push_back(std::move(ele));
        payload.pop_front();
    }
    VLOG(RPC_LOG_LEVEL) << FormatString("Resume sending %zu payload for service %s to client %s gateway %s.", sz,
                                        meta.svc_name(), meta.client_id(), meta.gateway_id());
    return Status::OK();
}

Status ZmqService::ParkPayloadIfNeeded(ZmqMetaMsgFrames &p, ZmqMsgFrames &payload)
{
    MetaPb &meta = p.first;
    CHECK_FAIL_RETURN_STATUS(meta.event_type() != EventType::V1MTP, K_NOT_FOUND, "V1MTP is not supported");

    bool hasPayload = methodsHavePayloadRecv_.count(meta.method_index()) > 0;
    CHECK_FAIL_RETURN_STATUS(hasPayload, K_NOT_FOUND, "Not a payload recv method");

    // See if the payload frame is tagged
    int64_t sz = 0;
    auto it = p.second.begin();
    while (it != p.second.end()) {
        if (it->GetType() == ZmqMessage::ZmqMsgType::PAYLOAD_SZ) {
            RETURN_IF_NOT_OK(ZmqMessageToInt64(*it, sz));
            break;
        }
        ++it;
    }
    CHECK_FAIL_RETURN_STATUS(it != p.second.end(), K_NOT_FOUND, "Not tagged");

    // Because we always prepend the MetaPb for rpc, we want to make sure payload is not too small
    // compared to MetaPb. Anything too small is not worth the split.
    const size_t overhead = ZMQ_SHORT_UUID + meta.ByteSizeLong();
    const size_t limit = 1024 * 1024;
    CHECK_FAIL_RETURN_STATUS(sz + overhead > limit, K_NOT_FOUND, "Too small to split");

    // Here is the plan. We split the rpc into two rpc. First rpc consists
    // of the reply up to the iterator above. The second rpc is the remaining
    // payload. The reason is to allow the client to work piecewise. For example
    // the client can start allocate the memory earlier while waiting for the
    // payload. For the payload, we have two choices
    // (a) send after the first rpc is sent. This case doesn't need V2MTP but downlevel client
    //     will not understand this protocol. The safest bet is to rely on V2MTP.
    // (b) park at the server side until the client ack back. This choice is usually
    //     associated with the case the client wants the payload to write directly into
    //     shared memory. This case requires V2MTP.

    // If this is from a ZmqFrontend, we can still do it as long as this client has a
    // secondary V2MTP socket path.
    int fd = ZMQ_NO_FILE_FD;
    if (meta.event_type() == EventType::ZMQ) {
        ReadLock rlock(&routeMux_);
        auto routeIt = routes_.find(meta.gateway_id());
        CHECK_FAIL_RETURN_STATUS(routeIt != routes_.end(), K_NOT_FOUND, "No other routes");
        fd = *(routeIt->second.begin());
    } else {
        fd = meta.route_fd();
    }

    // At this point, we know the client can support V2MTP. We will use FLAGS_payload_nocopy_threshold
    // to decide we do (a) or (b)

    // Split the rpc into two
    payload.insert(payload.end(), std::make_move_iterator(std::next(it, 1)), std::make_move_iterator(p.second.end()));
    p.second.erase(std::next(it, 1), p.second.end());

    // If the size of the payload is too small, it is not worth to park it here.
    if (sz <= FLAGS_payload_nocopy_threshold) {
        it->SetType(ZmqMessage::ZmqMsgType::NONE);
        meta.set_payload_index(ZMQ_OFFLINE_PAYLOAD_INX);
    } else {
        // Park the payload and update the MetaPb
        meta.set_payload_index(payloadBank_->SavePayload(fd, std::move(payload), nullptr, nullptr));
        // We want to route this request to use ZMQ instead of using socket for two reasons
        // (a) the fd is already used to send large payload, we will be queued for a long time.
        // (b) improve the latency to give the client enough time to allocate the memory
        // Note that it is possible that we route the reply to another path even though
        // the request came from a socket because these paths are using the same gateway
        // of ZmqFrontend
        meta.set_event_type(EventType::ZMQ);
    }

    VLOG(RPC_LOG_LEVEL) << FormatString("Parking payload for service %s client %s payload index %d", meta.svc_name(),
                                        meta.client_id(), meta.payload_index());
    return Status::OK();
}

Status ZmqService::ServiceToClient(ZmqMetaMsgFrames &p)
{
    MetaPb &meta = p.first;
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(meta.trace_id());
    PerfPoint::RecordElapsed(PerfKey::ZMQ_BACKEND_TO_FRONTEND, GetLapTime(meta, "ZMQ_BACKEND_TO_FRONTEND"));
    // Before we put the replies onto the appropriate queue, we want to intercept the one with payload.
    // If we are going to send it in two steps (e.g. waiting for the client to allocate the buffer),
    // we want to park the payload or send separately. We need to make sure the client is using V2MTP.
    ZmqMsgFrames payload;
    ZmqMetaMsgFrames rpc2;
    Status rc = ParkPayloadIfNeeded(p, payload);
    bool offlineRpc = rc.IsOk() && meta.payload_index() == ZMQ_OFFLINE_PAYLOAD_INX;
    if (offlineRpc) {
        // If we need to send the payload separately, we need a copy of the MetaPb.
        MetaPb m = p.first;
        m.set_payload_index(ZMQ_EMBEDDED_PAYLOAD_INX);
        rpc2.first = std::move(m);
        rpc2.second = std::move(payload);
    }
    int fd = meta.route_fd();
    if (meta.event_type() == EventType::ZMQ) {
        RETURN_IF_NOT_OK(replyQueue_->Put(std::move(p)));
        eventfd_write(outfd_, 1);
    } else {
        RETURN_IF_NOT_OK(io_->ServiceToClient(fd, p));
    }
    if (offlineRpc) {
        // We will use the same route as the first part. If we pick a different route to send in
        // parallel, make sure the client is capable to receive the parts in different order
        if (rpc2.first.event_type() == EventType::ZMQ) {
            RETURN_IF_NOT_OK(replyQueue_->Put(std::move(rpc2)));
            eventfd_write(outfd_, 1);
        } else {
            RETURN_IF_NOT_OK(io_->ServiceToClient(fd, rpc2));
        }
    }
    return Status::OK();
}

Status ZmqService::BackendToFrontend(std::shared_ptr<ZmqServerMsgMgr> &mgr)
{
    ZmqMetaMsgFrames p;
    std::string workerId;
    RETURN_IF_NOT_OK(mgr->GetMsg(p, workerId, 0));
    RETURN_IF_NOT_OK(ServiceToClient(p));
    return Status::OK();
}

Status ZmqService::RouteToRegBackend(ZmqMetaMsgFrames &p)
{
    CHECK_FAIL_RETURN_STATUS(cfg_.numRegularSockets_ != 0, K_RUNTIME_ERROR,
                             "Get id failed, regular sockets as divisor is 0, route to reg back end failed");
    MetaPb &meta = p.first;
    auto id = nextWorker_.fetch_add(1) % cfg_.numRegularSockets_;
    auto worker = workerCBs_[id];
    auto workerId = worker->GetWorkerId();
    meta.set_worker_id(workerId);

    auto traceID = meta.trace_id();
    auto timeout = meta.timeout();
    auto dbName = meta.db_name();

    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
    VLOG(RPC_LOG_LEVEL) << "Receive message and timeout: " << std::to_string(timeout) << ", dbName:" << dbName;
    Timer timer;
    auto func = [=]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        if (timeout > 0) {
            int64_t elapsed = timer.ElapsedMilliSecond();
            reqTimeoutDuration.Init(timeout - elapsed);
            scTimeoutDuration.Init(timeout - elapsed);
        } else {
            reqTimeoutDuration.Init();
            scTimeoutDuration.Init();
        }
        g_MetaRocksDbName = dbName;
        LOG_IF_ERROR(worker->WorkerEntry(), "worker entry failed");
        g_MetaRocksDbName.clear();
        workerOperationTimeCost.Clear();
        masterOperationTimeCost.Clear();
    };
    CHECK_FAIL_RETURN_STATUS(thrdPool_ != nullptr, K_INVALID, "service is stop");
    thrdPool_->Execute(func);
    VLOG(RPC_LOG_LEVEL) << FormatString("Routing request %s to %s", meta.client_id(), workerId);
    RETURN_IF_NOT_OK(backendMgr_->SendMsg(workerId, p, QUE_NO_TIMEOUT));
    return Status::OK();
}

Status ZmqService::HandleInternalMethods(ZmqMetaMsgFrames &p)
{
    // Internal methods. They have negative method indexes. Most of the time, they are simple enough
    // without the need to route to WorkerEntry.
    MetaPb &meta = p.first;
    VLOG(RPC_LOG_LEVEL) << FormatString("Receive internal method %d for service %s for client %s", meta.method_index(),
                                        serviceName_, meta.client_id());
    ZmqMsgFrames reply;
    switch (meta.method_index()) {
        case ZMQ_SOCKPATH_METHOD: {
            // It is a special method sent by the stub to get the per-service socket path.
            RETURN_IF_NOT_OK(PushBackStringToFrames(cfg_.udsDirectory_, reply));
            break;
        }
        case ZMQ_TCP_DIRECT_METHOD: {
            // It is a special method sent by the stub to get the per-service tcp/ip hostport
            RETURN_IF_NOT_OK(PushBackStringToFrames(tcpHostPort_.ToString(), reply));
            break;
        }
        case ZMQ_HEARTBEAT_METHOD: {
            // Just send back anything. But we will send back the meta which
            // records the time when we receive it.
            VLOG(RPC_LOG_LEVEL) << "Heartbeat";
            RETURN_IF_NOT_OK(PushBackProtobufToFrames(meta, reply));
            break;
        }
        case ZMQ_PAYLOAD_GET_METHOD: {
            RETURN_IF_NOT_OK(ProcessPayloadGetRq(p.first, p.second, reply));
            break;
        }
        case ZMQ_STREAM_WORKER_METHOD:
        case ZMQ_PAYLOAD_TICK_METHOD:
        case ZMQ_PAYLOAD_HANDSHAKE_METHOD:
        case ZMQ_PAYLOAD_PUT_METHOD: {
            // These methods are not trivial to be handled here. Let another thread do the work.
            return RouteToRegBackend(p);
        }
        default:
            // Ignore anything we don't understand and don't reply.
            // Let the peer timeout.
            RETURN_STATUS_LOG_ERROR(K_INVALID, FormatString("Unknown internal request %d", meta.method_index()));
            break;
    }
    RETURN_IF_NOT_OK(ZmqService::SendStatus(reply, meta, Status::OK(),
                                            [this](ZmqMetaMsgFramesRef e) { return ServiceToClient(e); }));
    return Status::OK();
}

void ZmqService::AddRoute(MetaPb &meta, int fd)
{
    auto &gatewayId = meta.gateway_id();
    WriteLock xlock(&routeMux_);
    auto it = routes_.find(gatewayId);
    if (it == routes_.end()) {
        std::set<int> v{ fd };
        it = routes_.emplace(gatewayId, v).first;
    } else {
        it->second.insert(fd);
    }
    if (VLOG_IS_ON(RPC_LOG_LEVEL)) {
        std::stringstream oss;
        oss << "Gateway " << gatewayId << " routes :";
        for (auto e : it->second) {
            oss << " " << e;
        }
        VLOG(RPC_LOG_LEVEL) << oss.str();
    }
    fdToGateway_[fd] = gatewayId;
}

void ZmqService::DeleteRoute(int fd)
{
    WriteLock xlock(&routeMux_);
    auto it = fdToGateway_.find(fd);
    if (it == fdToGateway_.end()) {
        return;
    }
    auto gatewayId = it->second;
    fdToGateway_.erase(it);
    routes_.erase(gatewayId);
    VLOG(RPC_LOG_LEVEL) << FormatString("Delete gateway %s route %d", gatewayId, fd);
}

Status ZmqService::FrontendToBackend(int fd, const EventType type, ZmqMetaMsgFrames &p, bool addRoute)
{
    if (type == EventType::V2MTP || type == EventType::V1MTP) {
        MetaPb meta;
        ZmqCurveUserId userId;
        // A direct connection from stub needs to be parsed.
        Status rc = ParseMsgFrames(p.second, meta, fd, type, userId);
        if (rc.IsError()) {
            // Log the message for anything else, and move on.
            RETURN_STATUS_LOG_ERROR(StatusCode::K_OK, "Incompatible rpc request. Ignore");
        }
        p.first = std::move(meta);
        if (type == EventType::V2MTP && addRoute) {
            AddRoute(p.first, fd);
        }
    }
    MetaPb &meta = p.first;
    // Branch prediction. Let the most probably case in the first if-block.
    if (meta.worker_id().empty() && meta.method_index() >= 0) {
        return RouteToRegBackend(p);
    } else if (!meta.worker_id().empty()) {
        // If the target destination is known, it must be stream rpc.
        VLOG(RPC_LOG_LEVEL) << FormatString("Frontend got STREAM rpc from client %s, routing to stream worker %s",
                                            meta.client_id(), meta.worker_id());
        Status rc;
        const std::string workerId = meta.worker_id();
        do {
            rc = backendMgr_->SendMsg(workerId, p, RPC_POLL_TIME);
        } while (!globalInterrupt_ && rc.GetCode() == K_TRY_AGAIN);
        RETURN_IF_NOT_OK(rc);
        ZmqService *svc = nullptr;
        int32_t id = 0;
        LOG_IF_ERROR(ZmqService::ParseWorkerId(workerId, svc, id), "ParseWorkerId");
        workerCBs_[id]->WakeUpStreamWorker();
    } else {
        return HandleInternalMethods(p);
    }
    return Status::OK();
}

Status ZmqService::ProcessAccept(int listenFd)
{
    bool isTcp = (listenFd == tcpfd_);
    UnixSockFd listenSockFd(listenFd);
    UnixSockFd connectedSockFd;
    RETURN_IF_NOT_OK(listenSockFd.Accept(connectedSockFd));
    if (listenFd == exclListenFd_) {
        LOG(INFO) << FormatString("Spawn new work agent for exclusive connection, sock_fd: %s",
                                  connectedSockFd.GetFd());
        if (!workAgentThreadPool_) {
            RETURN_IF_EXCEPTION_OCCURS(workAgentThreadPool_ =
                                       std::make_unique<ThreadPool>(1, MAX_EXCLUSIVE_CONNECTIONS_LIMIT));
        }
        auto newAgent = std::make_unique<WorkAgent>(connectedSockFd, this, !isTcp);
        auto ptr = newAgent.get();
        workAgentThreadPool_->Execute([this, ptr] { ptr->Run(); });
        workAgents_.push_back(std::move(newAgent));
    } else {
        // Make it non-blocking
        RETURN_IF_NOT_OK(connectedSockFd.SetNonBlocking());
        if (isTcp) {
            RETURN_IF_NOT_OK(connectedSockFd.SetNoDelay());
        }
        // Assign it to the next io service
        RETURN_IF_NOT_OK(io_->AddFd(this, connectedSockFd.GetFd(), !isTcp));
        VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Spawn %s connection %d for service %s", isTcp ? "tcp" : "uds",
                                                connectedSockFd.GetFd(), serviceName_);
    }

    return Status::OK();
}

Status ZmqService::DirectExecInternalMethod(int fd, EventType type, ZmqMetaMsgFrames &inFrames,
                                            ZmqMetaMsgFrames &outFrames)
{
    if (type == EventType::V2MTP || type == EventType::V1MTP) {
        MetaPb meta;
        ZmqCurveUserId userId;
        // A direct connection from stub needs to be parsed.
        Status rc = ParseMsgFrames(inFrames.second, meta, fd, type, userId);
        if (rc.IsError()) {
            // Log the message for anything else, and move on.
            RETURN_STATUS_LOG_ERROR(StatusCode::K_OK, "Incompatible rpc request. Ignore");
        }
        inFrames.first = std::move(meta);
    }

    CHECK_FAIL_RETURN_STATUS(cfg_.numRegularSockets_ != 0, K_RUNTIME_ERROR,
                             "Get id failed, regular sockets as divisor is 0, route to reg back end failed");
    MetaPb &meta = inFrames.first;
    auto id = nextWorker_.fetch_add(1) % cfg_.numRegularSockets_;
    auto worker = workerCBs_[id];
    auto workerId = worker->GetWorkerId();
    meta.set_worker_id(workerId);

    auto traceID = meta.trace_id();
    auto timeout = meta.timeout();
    auto dbName = meta.db_name();

    Timer timer;
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
    if (timeout > 0) {
        int64_t elapsed = timer.ElapsedMilliSecond();
        reqTimeoutDuration.Init(timeout - elapsed);
        scTimeoutDuration.Init(timeout - elapsed);
    } else {
        reqTimeoutDuration.Init();
        scTimeoutDuration.Init();
    }
    g_MetaRocksDbName = dbName;
    LOG_IF_ERROR(worker->WorkerEntryWithoutMsgQ(inFrames, outFrames), "worker entry failed");
    g_MetaRocksDbName.clear();
    workerOperationTimeCost.Clear();
    masterOperationTimeCost.Clear();
    VLOG(RPC_LOG_LEVEL) << FormatString("Routing request %s to %s", meta.client_id(), workerId);

    return Status::OK();
}

Status ZmqService::HandleRqFromProxy()
{
    // It originates from ZmqServerImpl through zmq socket.
    eventfd_t v;
    eventfd_read(infd_, &v);
    while (v > 0) {
        ZmqMetaMsgFrames p;
        RETURN_IF_NOT_OK(rqQueue_->Take(&p));
        PerfPoint::RecordElapsed(PerfKey::ZMQ_ROUTER_TO_SVC, GetLapTime(p.first, "ZMQ_ROUTER_TO_SVC"));
        RETURN_IF_NOT_OK(FrontendToBackend(ZMQ_NO_FILE_FD, EventType::ZMQ, p, false));

        if (globalInterrupt_) {
            RETURN_STATUS(StatusCode::K_SHUTTING_DOWN, "Shutting down the socket.");
        }
        --v;
    }
    return Status::OK();
}

void ZmqService::StartHWMTimer()
{
    // Every 1s report the HWM
    const uint32_t K_ONE_SECOND = 1000;
    auto timerQIsRunning = TimerQueue::GetInstance()->Initialize();
    if (timerQIsRunning) {
        TimerQueue::TimerImpl impl;
        Status rc = TimerQueue::GetInstance()->AddTimer(
            K_ONE_SECOND, [this]() { this->CheckHWMRatio(); }, impl);
        if (rc.IsOk()) {
            hwmCheck_ = std::make_unique<TimerQueue::TimerImpl>(impl);
        } else {
            LOG(WARNING) << "Unable to add a timer. rc " << rc.ToString();
        }
    }
}

Status ZmqService::ServiceRequest(MetaPb &&meta, ZmqMsgFrames &&msgs)
{
    Timer timer;
    RETURN_IF_NOT_OK(rqQueue_->Put({ std::move(meta), std::move(msgs) }));
    auto timeSpent = timer.ElapsedMilliSecond();
    const int LOG_WARNING_FREQUENCY = 100;
    LOG_IF_EVERY_N(WARNING, timeSpent > RPC_POLL_TIME, LOG_WARNING_FREQUENCY)
        << FormatString("Service %s is congested. Proxy main thread congested for [%.3lf]ms. HWM setting is %d.",
                        ServiceName(), timeSpent, cfg_.hwm_);
    uint64_t eventFd = 1;
    eventfd_write(infd_, eventFd);
    return Status::OK();
}

Status ZmqService::ServiceReply(ZmqMetaMsgFrames *msg)
{
    RETURN_IF_NOT_OK(replyQueue_->Remove(msg));
    eventfd_t n;
    eventfd_read(outfd_, &n);
    if (n > 1) {
        eventfd_write(outfd_, n - 1);
    }
    return Status::OK();
}

void ZmqService::CheckHWMRatio()
{
    const float ONE_HUNDRED_PERCENT = 100.0;
    const float SIXTY_PERCENT_THRESHOLD = 60.0;
    const float EIGHTY_PERCENT_THRESHOLD = 80.0;
    const int LOG_QUEUE_FULL_FREQUENCY = 10;
    const int LOG_QUEUE_AT_EIGHTY_FREQUENCY = 100;
    const int LOG_QUEUE_AT_SIXTY_FREQUENCY = 1000;
    float ratio = backendMgr_->GetHWMRatio() * ONE_HUNDRED_PERCENT;
    std::string warningMsg = FormatString("The top HWM ratio of %s inbound queue is: [%.3lf]", ServiceName(), ratio);
    if (ratio >= ONE_HUNDRED_PERCENT) {
        LOG_EVERY_N(WARNING, LOG_QUEUE_FULL_FREQUENCY) << warningMsg << "%, the queue is full.";
    } else if (ratio >= EIGHTY_PERCENT_THRESHOLD) {
        LOG_EVERY_N(WARNING, LOG_QUEUE_AT_EIGHTY_FREQUENCY) << warningMsg << "%, the queue reaches 80% threshold.";
    } else if (ratio >= SIXTY_PERCENT_THRESHOLD) {
        LOG_EVERY_N(WARNING, LOG_QUEUE_AT_SIXTY_FREQUENCY) << warningMsg << "%, the queue reaches 60% threshold.";
    }
    // Reset the timer for another run
    if (!globalInterrupt_) {
        StartHWMTimer();
    }
}
}  // namespace datasystem
