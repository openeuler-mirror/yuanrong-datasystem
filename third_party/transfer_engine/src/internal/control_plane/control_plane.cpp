#include "internal/control_plane/control_plane.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "internal/control_plane/control_plane_codec.h"
#include "internal/control_plane/socket_rpc_transport.h"
#include "internal/control_plane/transfer_control_dispatcher.h"
#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace {

constexpr size_t kMaxPendingRpcConnections = 4096;
constexpr int kServerSocketTimeoutSec = 30;

}  // namespace

Status SocketControlClient::ExchangeRootInfo(const std::string &host, uint16_t port, const ExchangeRootInfoRequest &req,
                                             ExchangeRootInfoResponse *rsp)
{
    TE_CHECK_PTR_OR_RETURN(rsp);
    std::vector<uint8_t> rspPayload;
    TE_RETURN_IF_ERROR(InvokeRpc(host, port, RpcMethod::kExchangeRootInfo, EncodeExchangeReq(req), &rspPayload));
    TE_CHECK_OR_RETURN(DecodeExchangeRsp(rspPayload, rsp), StatusCode::kRuntimeError, "decode exchange response failed");
    return Status::OK();
}

Status SocketControlClient::QueryConnReady(const std::string &host, uint16_t port, const QueryConnReadyRequest &req,
                                           QueryConnReadyResponse *rsp)
{
    TE_CHECK_PTR_OR_RETURN(rsp);
    std::vector<uint8_t> rspPayload;
    TE_RETURN_IF_ERROR(InvokeRpc(host, port, RpcMethod::kQueryConnReady, EncodeQueryReq(req), &rspPayload));
    TE_CHECK_OR_RETURN(DecodeQueryRsp(rspPayload, rsp), StatusCode::kRuntimeError, "decode query response failed");
    return Status::OK();
}

Status SocketControlClient::ReadTrigger(const std::string &host, uint16_t port, const ReadTriggerRequest &req,
                                        ReadTriggerResponse *rsp)
{
    TE_CHECK_PTR_OR_RETURN(rsp);
    std::vector<uint8_t> rspPayload;
    TE_RETURN_IF_ERROR(InvokeRpc(host, port, RpcMethod::kReadTrigger, EncodeReadReq(req), &rspPayload));
    TE_CHECK_OR_RETURN(DecodeReadRsp(rspPayload, rsp), StatusCode::kRuntimeError, "decode read response failed");
    return Status::OK();
}

Status SocketControlClient::BatchReadTrigger(const std::string &host, uint16_t port, const BatchReadTriggerRequest &req,
                                             BatchReadTriggerResponse *rsp)
{
    TE_CHECK_PTR_OR_RETURN(rsp);
    std::vector<uint8_t> rspPayload;
    TE_RETURN_IF_ERROR(InvokeRpc(host, port, RpcMethod::kBatchReadTrigger, EncodeBatchReadReq(req), &rspPayload));
    TE_CHECK_OR_RETURN(DecodeBatchReadRsp(rspPayload, rsp),
                       StatusCode::kRuntimeError, "decode batch read response failed");
    return Status::OK();
}

SocketControlServer::SocketControlServer() = default;

SocketControlServer::~SocketControlServer()
{
    Stop();
}

Status SocketControlServer::Start(const std::string &host, uint16_t port, std::shared_ptr<ITransferControlService> service,
                                  int32_t workerThreads)
{
    TE_CHECK_OR_RETURN(!running_, StatusCode::kInvalid, "control server already running");
    TE_CHECK_OR_RETURN(service != nullptr, StatusCode::kInvalid, "service is null");
    TE_CHECK_OR_RETURN(workerThreads > 0, StatusCode::kInvalid, "worker_threads should be positive");

    int fd = -1;
    TE_RETURN_IF_ERROR(CreateListenSocket(host, port, 128, &fd));

    listenFd_ = fd;
    workerCount_ = workerThreads;
    service_ = std::move(service);
    running_ = true;
    workerThreads_.reserve(static_cast<size_t>(workerCount_));
    for (int32_t i = 0; i < workerCount_; ++i) {
        workerThreads_.emplace_back([this]() { WorkerLoop(); });
    }
    acceptThread_ = std::thread([this]() { AcceptLoop(); });
    LOG(INFO) << "control server started"
              << ", host=" << host << ", port=" << port
              << ", worker_threads=" << workerThreads;
    return Status::OK();
}

void SocketControlServer::Stop()
{
    if (!running_) {
        return;
    }
    running_ = false;

    if (listenFd_ >= 0) {
        ::shutdown(listenFd_, SHUT_RDWR);
        ::close(listenFd_);
        listenFd_ = -1;
    }

    if (acceptThread_.joinable()) {
        acceptThread_.join();
    }
    queueCv_.notify_all();
    for (auto &worker : workerThreads_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workerThreads_.clear();
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        while (!clientFdQueue_.empty()) {
            ::close(clientFdQueue_.front());
            clientFdQueue_.pop_front();
        }
    }
    workerCount_ = 0;
    service_.reset();
    LOG(INFO) << "control server stopped";
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
                LOG(WARNING) << "accept failed while server running, errno=" << errno;
                continue;
            }
            break;
        }
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (clientFdQueue_.size() >= kMaxPendingRpcConnections) {
                LOG(WARNING) << "drop rpc connection due to full queue"
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
            clientFd = clientFdQueue_.front();
            clientFdQueue_.pop_front();
        }
        HandleClient(clientFd);
    }
}

void SocketControlServer::HandleClient(int clientFd)
{
    (void)SetSocketTimeoutSec(clientFd, kServerSocketTimeoutSec);

    RpcMethod method;
    std::vector<uint8_t> reqPayload;
    Status recvRc = RecvFrame(clientFd, &method, &reqPayload);
    if (recvRc.IsError()) {
        LOG(WARNING) << "recv rpc frame failed, reason=" << recvRc.ToString();
        std::vector<uint8_t> err;
        (void)MakeServerErrorPayload(recvRc.GetMsg(), &err);
        (void)SendFrame(clientFd, RpcMethod::kReadTrigger, err);
        ::close(clientFd);
        return;
    }

    RpcMethod rspMethod = method;
    std::vector<uint8_t> rspPayload;
    (void)DispatchControlRequest(service_, method, reqPayload, &rspMethod, &rspPayload);
    (void)SendFrame(clientFd, rspMethod, rspPayload);
    ::close(clientFd);
}

}  // namespace datasystem
