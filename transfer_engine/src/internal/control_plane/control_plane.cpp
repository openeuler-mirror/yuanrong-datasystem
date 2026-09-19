#include "internal/control_plane/control_plane.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <system_error>
#include <utility>
#include <vector>

#include "internal/control_plane/control_plane_codec.h"
#include "internal/control_plane/socket_rpc_transport.h"
#include "internal/control_plane/transfer_control_dispatcher.h"
#include "internal/log/logging.h"
#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace {

constexpr size_t kMaxPendingRpcConnections = 4096;
constexpr int kServerSocketTimeoutSec = 30;
constexpr int K_LISTEN_SOCKET_BACKLOG = 128;

}  // namespace

Result SocketControlClient::ExchangeRootInfo(const std::string &host, uint16_t port, const ExchangeRootInfoRequest &req,
                                             ExchangeRootInfoResponse *rsp)
{
    TE_CHECK_PTR_OR_RETURN(rsp);
    std::vector<uint8_t> rspPayload;
    TE_RETURN_IF_ERROR(InvokeRpc(host, port, RpcMethod::kExchangeRootInfo, EncodeExchangeReq(req), &rspPayload));
    TE_CHECK_OR_RETURN(DecodeExchangeRsp(rspPayload, rsp), ErrorCode::kRuntimeError, "decode exchange response failed");
    return Result::OK();
}

Result SocketControlClient::QueryConnReady(const std::string &host, uint16_t port, const QueryConnReadyRequest &req,
                                           QueryConnReadyResponse *rsp)
{
    TE_CHECK_PTR_OR_RETURN(rsp);
    std::vector<uint8_t> rspPayload;
    TE_RETURN_IF_ERROR(InvokeRpc(host, port, RpcMethod::kQueryConnReady, EncodeQueryReq(req), &rspPayload));
    TE_CHECK_OR_RETURN(DecodeQueryRsp(rspPayload, rsp), ErrorCode::kRuntimeError, "decode query response failed");
    return Result::OK();
}

Result SocketControlClient::ReadTrigger(const std::string &host, uint16_t port, const ReadTriggerRequest &req,
                                        ReadTriggerResponse *rsp)
{
    TE_CHECK_PTR_OR_RETURN(rsp);
    std::vector<uint8_t> rspPayload;
    TE_RETURN_IF_ERROR(InvokeRpc(host, port, RpcMethod::kReadTrigger, EncodeReadReq(req), &rspPayload));
    TE_CHECK_OR_RETURN(DecodeReadRsp(rspPayload, rsp), ErrorCode::kRuntimeError, "decode read response failed");
    return Result::OK();
}

Result SocketControlClient::BatchReadTrigger(const std::string &host, uint16_t port, const BatchReadTriggerRequest &req,
                                             BatchReadTriggerResponse *rsp)
{
    TE_CHECK_PTR_OR_RETURN(rsp);
    std::vector<uint8_t> rspPayload;
    TE_RETURN_IF_ERROR(InvokeRpc(host, port, RpcMethod::kBatchReadTrigger, EncodeBatchReadReq(req), &rspPayload));
    TE_CHECK_OR_RETURN(DecodeBatchReadRsp(rspPayload, rsp),
                       ErrorCode::kRuntimeError, "decode batch read response failed");
    return Result::OK();
}

Result SocketControlClient::ReleaseReadLease(const std::string &host, uint16_t port,
                                             const ReleaseReadLeaseRequest &req, ReleaseReadLeaseResponse *rsp)
{
    TE_CHECK_PTR_OR_RETURN(rsp);
    std::vector<uint8_t> rspPayload;
    TE_RETURN_IF_ERROR(
        InvokeRpc(host, port, RpcMethod::kReleaseReadLease, EncodeReleaseReadLeaseReq(req), &rspPayload));
    TE_CHECK_OR_RETURN(DecodeReleaseReadLeaseRsp(rspPayload, rsp),
                       ErrorCode::kRuntimeError, "decode release read lease response failed");
    return Result::OK();
}

SocketControlServer::SocketControlServer() = default;

SocketControlServer::~SocketControlServer()
{
    Stop();
}

Result SocketControlServer::Bind(const std::string &host, uint16_t port, uint16_t *boundPort,
                                 ListenSocketFailureLogLevel failureLogLevel)
{
    TE_CHECK_PTR_OR_RETURN(boundPort);
    TE_CHECK_OR_RETURN(!running_, ErrorCode::kInvalid, "control server already running");
    TE_CHECK_OR_RETURN(listenFd_ < 0, ErrorCode::kInvalid, "control server already bound");

    int fd = -1;
    TE_RETURN_IF_ERROR(CreateListenSocket(host, port, K_LISTEN_SOCKET_BACKLOG, fd, failureLogLevel));
    uint16_t actualPort = 0;
    Result portRc = GetSocketLocalPort(fd, &actualPort);
    if (portRc.IsError()) {
        ::close(fd);
        return portRc;
    }
    listenFd_ = fd;
    boundHost_ = host;
    boundPort_ = actualPort;
    *boundPort = actualPort;
    return Result::OK();
}

Result SocketControlServer::Start(const std::string &host, uint16_t port, std::shared_ptr<ITransferControlService> service,
                                  int32_t workerThreads)
{
    TE_CHECK_OR_RETURN(!running_, ErrorCode::kInvalid, "control server already running");
    TE_CHECK_OR_RETURN(service != nullptr, ErrorCode::kInvalid, "service is null");
    TE_CHECK_OR_RETURN(workerThreads > 0, ErrorCode::kInvalid, "worker_threads should be positive");

    if (listenFd_ < 0) {
        uint16_t boundPort = 0;
        TE_RETURN_IF_ERROR(Bind(host, port, &boundPort));
    } else {
        TE_CHECK_OR_RETURN(host == boundHost_ && port == boundPort_, ErrorCode::kInvalid,
                           "control server endpoint differs from bound endpoint");
    }

    workerCount_ = workerThreads;
    service_ = std::move(service);
    running_ = true;
    try {
        workerThreads_.reserve(static_cast<size_t>(workerCount_));
        for (int32_t i = 0; i < workerCount_; ++i) {
            workerThreads_.emplace_back([this]() { WorkerLoop(); });
        }
        acceptThread_ = std::thread([this]() { AcceptLoop(); });
    } catch (const std::system_error &e) {
        TE_LOG_ERROR << "control server thread start failed, reason=" << e.what();
        Stop();
        return TE_MAKE_STATUS(ErrorCode::kRuntimeError, "control server thread start failed");
    }
    TE_LOG_INFO << "control server started"
              << ", host=" << host << ", port=" << boundPort_
              << ", worker_threads=" << workerThreads;
    return Result::OK();
}

void SocketControlServer::Stop()
{
    if (!running_ && listenFd_ < 0) {
        return;
    }
    running_ = false;

    if (listenFd_ >= 0) {
        ::shutdown(listenFd_, SHUT_RDWR);
        ::close(listenFd_);
        listenFd_ = -1;
    }
    boundHost_.clear();
    boundPort_ = 0;

    if (acceptThread_.joinable()) {
        acceptThread_.join();
    }
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        while (!clientFdQueue_.empty()) {
            ::close(clientFdQueue_.front());
            clientFdQueue_.pop_front();
        }
        for (const int clientFd : activeClientFds_) {
            (void)::shutdown(clientFd, SHUT_RDWR);
        }
    }
    queueCv_.notify_all();
    for (auto &worker : workerThreads_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workerThreads_.clear();
    workerCount_ = 0;
    service_.reset();
    TE_LOG_INFO << "control server stopped";
}

void SocketControlServer::AcceptLoop()
{
    while (running_) {
        sockaddr_storage addr;
        socklen_t addrLen = sizeof(addr);
        int clientFd = ::accept(listenFd_, reinterpret_cast<sockaddr *>(&addr), &addrLen);
        if (clientFd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (running_) {
                TE_LOG_WARNING << "accept failed while server running, errno=" << errno;
                continue;
            }
            break;
        }
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (clientFdQueue_.size() >= kMaxPendingRpcConnections) {
                TE_LOG_WARNING << "drop rpc connection due to full queue"
                             << ", pending=" << clientFdQueue_.size();
                ::close(clientFd);
                continue;
            }
            clientFdQueue_.push_back(clientFd);
        }
        queueCv_.notify_one();
    }
}

void SocketControlServer::WorkerLoop()
{
    for (;;) {
        int clientFd = -1;
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            queueCv_.wait(lock, [this]() { return !running_ || !clientFdQueue_.empty(); });
            if (clientFdQueue_.empty()) {
                if (!running_) {
                    break;
                }
                continue;
            }
            if (!running_) {
                while (!clientFdQueue_.empty()) {
                    ::close(clientFdQueue_.front());
                    clientFdQueue_.pop_front();
                }
                break;
            }
            clientFd = clientFdQueue_.front();
            clientFdQueue_.pop_front();
            activeClientFds_.insert(clientFd);
        }
        HandleClient(clientFd);
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            activeClientFds_.erase(clientFd);
            ::close(clientFd);
        }
    }
}

void SocketControlServer::HandleClient(int clientFd)
{
    (void)SetSocketTimeoutSec(clientFd, kServerSocketTimeoutSec);

    RpcMethod method;
    std::vector<uint8_t> reqPayload;
    Result recvRc = RecvFrame(clientFd, &method, &reqPayload, kServerSocketTimeoutSec * 1000);
    if (recvRc.IsError()) {
        TE_LOG_WARNING << "recv rpc frame failed, reason=" << recvRc.ToString();
        std::vector<uint8_t> err;
        (void)MakeServerErrorPayload(recvRc.GetMsg(), &err);
        (void)SendFrame(clientFd, RpcMethod::kReadTrigger, err);
        return;
    }

    RpcMethod rspMethod = method;
    std::vector<uint8_t> rspPayload;
    (void)DispatchControlRequest(service_, method, reqPayload, &rspMethod, &rspPayload);
    (void)SendFrame(clientFd, rspMethod, rspPayload);
}

}  // namespace datasystem
