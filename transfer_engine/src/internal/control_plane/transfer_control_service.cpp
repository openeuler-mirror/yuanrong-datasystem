#include "internal/control_plane/transfer_control_service.h"

#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <utility>

#include <glog/logging.h>

#include "internal/log/environment_dump.h"
#include "internal/runtime/acl_runtime_helper.h"
#include "internal/connection/connection_manager.h"
#include "internal/memory/registered_memory_table.h"
#include "datasystem/transfer_engine/data_plane_backend.h"
#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace {

constexpr int32_t kRpcOkCode = 0;
constexpr size_t kMaxOwnerInitQueueSize = 1024;

class TransferControlServiceImpl final : public ITransferControlService {
public:
    TransferControlServiceImpl(std::string localHost, uint16_t localPort, int32_t localDeviceId,
                               std::shared_ptr<ConnectionManager> connMgr,
                               std::shared_ptr<RegisteredMemoryTable> registeredMemory,
                               std::shared_ptr<IDataPlaneBackend> backend)
        : localHost_(std::move(localHost)),
          localPort_(localPort),
          localDeviceId_(localDeviceId),
          connMgr_(std::move(connMgr)),
          registeredMemory_(std::move(registeredMemory)),
          backend_(std::move(backend))
    {
        ownerInitThread_ = std::thread([this]() { OwnerInitLoop(); });
    }

    ~TransferControlServiceImpl() override
    {
        {
            std::lock_guard<std::mutex> lock(ownerInitQueueMutex_);
            ownerInitStop_ = true;
        }
        ownerInitQueueCv_.notify_all();
        if (ownerInitThread_.joinable()) {
            ownerInitThread_.join();
        }
    }

    Result ExchangeRootInfo(const ExchangeRootInfoRequest &req, ExchangeRootInfoResponse *rsp) override
    {
        internal::DumpProcessEnvironment("service_exchange_root_info");
        TE_CHECK_PTR_OR_RETURN(rsp);
        TE_CHECK_OR_RETURN(!req.requesterHost.empty(), ErrorCode::kInvalid, "requester_host is empty");
        TE_CHECK_OR_RETURN(req.requesterPort > 0, ErrorCode::kInvalid, "requester_port should be positive");
        TE_CHECK_OR_RETURN(req.requesterDeviceId >= 0, ErrorCode::kInvalid, "requester_device_id should be non-negative");
        TE_CHECK_OR_RETURN(!req.rootInfo.empty(), ErrorCode::kInvalid, "root_info is empty");

        rsp->code = kRpcOkCode;
        rsp->msg = "accepted";
        rsp->ownerDeviceId = localDeviceId_;

        OwnerInitTask task;
        task.spec.localHost = localHost_;
        task.spec.localPort = localPort_;
        task.spec.localDeviceId = localDeviceId_;
        task.spec.peerHost = req.requesterHost;
        task.spec.peerPort = static_cast<uint16_t>(req.requesterPort);
        task.spec.peerDeviceId = req.requesterDeviceId;
        task.key = ConnectionKey{ localDeviceId_, req.requesterHost, static_cast<uint16_t>(req.requesterPort),
                                  req.requesterDeviceId };
        task.rootInfo = req.rootInfo;
        {
            std::lock_guard<std::mutex> lock(ownerInitQueueMutex_);
            if (ownerInitQueue_.size() >= kMaxOwnerInitQueueSize) {
                rsp->code = static_cast<int32_t>(ErrorCode::kNotReady);
                rsp->msg = "owner init queue is full";
                LOG(WARNING) << "owner init queue full"
                             << ", requester=" << req.requesterHost << ":" << req.requesterPort
                             << ", requester_device_id=" << req.requesterDeviceId
                             << ", queue_size=" << ownerInitQueue_.size();
                return Result::OK();
            }
            ownerInitQueue_.push_back(std::move(task));
        }
        LOG(INFO) << "exchange root info accepted"
                  << ", requester=" << req.requesterHost << ":" << req.requesterPort
                  << ", requester_device_id=" << req.requesterDeviceId
                  << ", owner_device_id=" << localDeviceId_;
        ownerInitQueueCv_.notify_one();
        return Result::OK();
    }

    Result QueryConnReady(const QueryConnReadyRequest &req, QueryConnReadyResponse *rsp) override
    {
        TE_CHECK_PTR_OR_RETURN(rsp);
        TE_CHECK_OR_RETURN(!req.requesterHost.empty(), ErrorCode::kInvalid, "requester_host is empty");
        TE_CHECK_OR_RETURN(req.requesterPort > 0, ErrorCode::kInvalid, "requester_port should be positive");
        TE_CHECK_OR_RETURN(req.requesterDeviceId >= 0, ErrorCode::kInvalid, "requester_device_id should be non-negative");

        ConnectionKey key { localDeviceId_, req.requesterHost, static_cast<uint16_t>(req.requesterPort), req.requesterDeviceId };
        const auto state = connMgr_->GetState(key);

        rsp->code = kRpcOkCode;
        rsp->msg = "ok";
        rsp->ready = state.ownerSendReady && !state.stale;
        return Result::OK();
    }

    Result ReadTrigger(const ReadTriggerRequest &req, ReadTriggerResponse *rsp) override
    {
        internal::DumpProcessEnvironment("service_read_trigger");
        TE_CHECK_PTR_OR_RETURN(rsp);
        TE_CHECK_OR_RETURN(req.length > 0, ErrorCode::kInvalid, "length should be positive");
        TE_CHECK_OR_RETURN(req.remoteAddr > 0, ErrorCode::kInvalid, "remote_addr should be positive");
        int32_t registeredNpuId = -1;
        if (!registeredMemory_->FindDeviceIdByRange(req.remoteAddr, req.length, &registeredNpuId)) {
            rsp->code = static_cast<int32_t>(ErrorCode::kNotAuthorized);
            rsp->msg = "remote range is not registered";
            LOG(WARNING) << "read trigger rejected: remote range not registered"
                         << ", request_id=" << req.requestId
                         << ", requester=" << req.requesterHost << ":" << req.requesterPort
                         << ", remote_addr=0x" << std::hex << req.remoteAddr << std::dec
                         << ", length=" << req.length;
            return Result::OK();
        }
        if (backend_->RequiresAclRuntime()) {
            if (registeredNpuId != localDeviceId_) {
                rsp->code = static_cast<int32_t>(ErrorCode::kNotSupported);
                rsp->msg = "registered npu_id does not match owner initialized device_id";
                LOG(WARNING) << "read trigger rejected: registered npu mismatch"
                             << ", request_id=" << req.requestId
                             << ", registered_npu_id=" << registeredNpuId
                             << ", owner_device_id=" << localDeviceId_;
                return Result::OK();
            }
            TE_RETURN_IF_ERROR(internal::EnsureAclSetDeviceForCurrentThread(registeredNpuId));
        }

        ConnectionSpec spec;
        spec.localHost = localHost_;
        spec.localPort = localPort_;
        spec.localDeviceId = localDeviceId_;
        spec.peerHost = req.requesterHost;
        spec.peerPort = static_cast<uint16_t>(req.requesterPort);
        spec.peerDeviceId = req.requesterDeviceId;
        Result sendRc = backend_->PostSend(spec, req.remoteAddr, req.length);
        if (sendRc.IsError()) {
            rsp->code = static_cast<int32_t>(sendRc.GetCode());
            rsp->msg = sendRc.GetMsg();
            LOG(ERROR) << "read trigger post send failed"
                       << ", request_id=" << req.requestId
                       << ", requester=" << req.requesterHost << ":" << req.requesterPort
                       << ", reason=" << sendRc.ToString();
            return Result::OK();
        }

        rsp->code = kRpcOkCode;
        rsp->msg = "accepted";
        LOG(INFO) << "read trigger accepted"
                  << ", request_id=" << req.requestId
                  << ", requester=" << req.requesterHost << ":" << req.requesterPort
                  << ", requester_device_id=" << req.requesterDeviceId
                  << ", length=" << req.length;
        return Result::OK();
    }

    Result BatchReadTrigger(const BatchReadTriggerRequest &req, BatchReadTriggerResponse *rsp) override
    {
        internal::DumpProcessEnvironment("service_batch_read_trigger");
        TE_CHECK_PTR_OR_RETURN(rsp);
        TE_CHECK_OR_RETURN(!req.requesterHost.empty(), ErrorCode::kInvalid, "requester_host is empty");
        TE_CHECK_OR_RETURN(req.requesterPort > 0, ErrorCode::kInvalid, "requester_port should be positive");
        TE_CHECK_OR_RETURN(req.requesterDeviceId >= 0, ErrorCode::kInvalid, "requester_device_id should be non-negative");
        TE_CHECK_OR_RETURN(!req.items.empty(), ErrorCode::kInvalid, "batch items is empty");

        rsp->failedItemIndex = -1;
        for (size_t i = 0; i < req.items.size(); ++i) {
            const auto &item = req.items[i];
            ReadTriggerRequest singleReq;
            singleReq.requestId = item.requestId;
            singleReq.requesterHost = req.requesterHost;
            singleReq.requesterPort = req.requesterPort;
            singleReq.requesterDeviceId = req.requesterDeviceId;
            singleReq.ownerDeviceId = req.ownerDeviceId;
            singleReq.remoteAddr = item.remoteAddr;
            singleReq.length = item.length;

            ReadTriggerResponse singleRsp;
            Result singleRc = ReadTrigger(singleReq, &singleRsp);
            if (singleRc.IsError()) {
                rsp->code = static_cast<int32_t>(singleRc.GetCode());
                rsp->msg = singleRc.GetMsg();
                rsp->failedItemIndex = static_cast<int32_t>(i);
                return Result::OK();
            }
            if (singleRsp.code != kRpcOkCode) {
                rsp->code = singleRsp.code;
                rsp->msg = singleRsp.msg;
                rsp->failedItemIndex = static_cast<int32_t>(i);
                return Result::OK();
            }
        }

        rsp->code = kRpcOkCode;
        rsp->msg = "accepted";
        return Result::OK();
    }

private:
    struct OwnerInitTask {
        ConnectionSpec spec;
        ConnectionKey key;
        std::string rootInfo;
    };

    void OwnerInitLoop()
    {
        for (;;) {
            OwnerInitTask task;
            {
                std::unique_lock<std::mutex> lock(ownerInitQueueMutex_);
                ownerInitQueueCv_.wait(lock, [this]() { return ownerInitStop_ || !ownerInitQueue_.empty(); });
                if (ownerInitQueue_.empty()) {
                    if (ownerInitStop_) {
                        break;
                    }
                    continue;
                }
                task = std::move(ownerInitQueue_.front());
                ownerInitQueue_.pop_front();
            }

            Result rc = Result::OK();
            if (backend_->RequiresAclRuntime()) {
                rc = internal::EnsureAclSetDeviceForCurrentThread(localDeviceId_);
            }
            if (rc.IsOk()) {
                rc = backend_->InitSend(task.spec, task.rootInfo);
            }
            if (rc.IsOk()) {
                connMgr_->MarkOwnerSendReady(task.key);
                LOG(INFO) << "owner init done"
                          << ", peer=" << task.spec.peerHost << ":" << task.spec.peerPort
                          << ", peer_device_id=" << task.spec.peerDeviceId
                          << ", owner_device_id=" << localDeviceId_;
            } else {
                connMgr_->MarkStale(task.key);
                LOG(ERROR) << "owner init failed"
                           << ", peer=" << task.spec.peerHost << ":" << task.spec.peerPort
                           << ", peer_device_id=" << task.spec.peerDeviceId
                           << ", reason=" << rc.ToString();
            }
        }
    }

    std::string localHost_;
    uint16_t localPort_ = 0;
    int32_t localDeviceId_ = -1;
    std::shared_ptr<ConnectionManager> connMgr_;
    std::shared_ptr<RegisteredMemoryTable> registeredMemory_;
    std::shared_ptr<IDataPlaneBackend> backend_;

    std::mutex ownerInitQueueMutex_;
    std::condition_variable ownerInitQueueCv_;
    std::deque<OwnerInitTask> ownerInitQueue_;
    bool ownerInitStop_ = false;
    std::thread ownerInitThread_;
};

}  // namespace

std::shared_ptr<ITransferControlService> CreateTransferControlService(
    const std::string &localHost, uint16_t localPort, int32_t localDeviceId, std::shared_ptr<ConnectionManager> connMgr,
    std::shared_ptr<RegisteredMemoryTable> registeredMemory, std::shared_ptr<IDataPlaneBackend> backend)
{
    return std::make_shared<TransferControlServiceImpl>(localHost, localPort, localDeviceId, std::move(connMgr),
                                                         std::move(registeredMemory), std::move(backend));
}

}  // namespace datasystem
