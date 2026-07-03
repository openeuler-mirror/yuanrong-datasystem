#include "internal/control_plane/transfer_control_service.h"

#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>

#include "internal/log/environment_dump.h"
#include "internal/log/logging.h"
#include "internal/runtime/acl_runtime_helper.h"
#include "internal/connection/connection_manager.h"
#include "internal/memory/registered_memory_table.h"
#include "datasystem/transfer_engine/data_plane_backend.h"
#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace {

constexpr int32_t kRpcOkCode = 0;
constexpr size_t kMaxOwnerInitQueueSize = 1024;
constexpr uint64_t K_DEFAULT_READ_LEASE_TTL_MS = 30000;
constexpr uint64_t K_DECIMAL_BASE = 10;

std::string NormalizeBackendKind(const std::string &backendKind)
{
    return backendKind.empty() ? "p2p" : backendKind;
}

uint64_t GetReadLeaseTtlMs()
{
    const char *env = std::getenv("TRANSFER_ENGINE_HIXL_READ_LEASE_TTL_MS");
    if (env == nullptr || env[0] == '\0') {
        return K_DEFAULT_READ_LEASE_TTL_MS;
    }
    uint64_t value = 0;
    for (const char *p = env; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') {
            return K_DEFAULT_READ_LEASE_TTL_MS;
        }
        value = value * K_DECIMAL_BASE + static_cast<uint64_t>(*p - '0');
        if (value > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
            return K_DEFAULT_READ_LEASE_TTL_MS;
        }
    }
    return value == 0 ? K_DEFAULT_READ_LEASE_TTL_MS : value;
}

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

        FillAcceptedExchangeRsp(rsp);
        if (RejectBackendMismatch(req, rsp)) {
            return Result::OK();
        }
        if (backend_->SupportsReceiverDrivenRead()) {
            Result rootRc = backend_->CreateRootInfo(&rsp->requesterInitRootInfo);
            if (rootRc.IsError()) {
                rsp->code = static_cast<int32_t>(rootRc.GetCode());
                rsp->msg = rootRc.GetMsg();
                return Result::OK();
            }
        }
        return EnqueueOwnerInit(req, rsp);
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
        rsp->ownerMemGeneration = backend_->MemoryGeneration();
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
            TE_LOG_WARNING << "read trigger rejected: remote range not registered"
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
                TE_LOG_WARNING << "read trigger rejected: registered npu mismatch"
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
            TE_LOG_ERROR << "read trigger post send failed"
                       << ", request_id=" << req.requestId
                       << ", requester=" << req.requesterHost << ":" << req.requesterPort
                       << ", reason=" << sendRc.ToString();
            return Result::OK();
        }

        rsp->code = kRpcOkCode;
        rsp->msg = "accepted";
        TE_VLOG_1 << "read trigger accepted"
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
        rsp->readLeaseId = 0;
        rsp->ownerMemGeneration = backend_->MemoryGeneration();

        if (backend_->SupportsReceiverDrivenRead()) {
            return HandleReceiverDrivenBatchRead(req, rsp);
        }
        return HandlePostSendBatchRead(req, rsp);
    }

    Result ReleaseReadLease(const ReleaseReadLeaseRequest &req, ReleaseReadLeaseResponse *rsp) override
    {
        TE_CHECK_PTR_OR_RETURN(rsp);
        if (req.readLeaseId != 0) {
            registeredMemory_->ReleaseReadLease(req.readLeaseId);
        }
        rsp->code = kRpcOkCode;
        rsp->msg = "released";
        TE_VLOG_1 << "release read lease"
                  << ", requester=" << req.requesterHost << ":" << req.requesterPort
                  << ", requester_device_id=" << req.requesterDeviceId
                  << ", read_lease_id=" << req.readLeaseId;
        return Result::OK();
    }

private:
    struct OwnerInitTask {
        ConnectionSpec spec;
        ConnectionKey key;
        std::string rootInfo;
    };

    void FillAcceptedExchangeRsp(ExchangeRootInfoResponse *rsp) const
    {
        rsp->code = kRpcOkCode;
        rsp->msg = "accepted";
        rsp->ownerDeviceId = localDeviceId_;
        rsp->backendKind = backend_->BackendKind();
        rsp->hixlRoutePolicy = backend_->RoutePolicy();
        rsp->ownerMemGeneration = backend_->MemoryGeneration();
    }

    bool RejectBackendMismatch(const ExchangeRootInfoRequest &req, ExchangeRootInfoResponse *rsp) const
    {
        const std::string requesterBackendKind = NormalizeBackendKind(req.backendKind);
        if (requesterBackendKind != backend_->BackendKind()) {
            rsp->code = static_cast<int32_t>(ErrorCode::kNotSupported);
            rsp->msg = "backend kind mismatch, requester=" + requesterBackendKind +
                       ", owner=" + backend_->BackendKind();
            TE_LOG_WARNING << "exchange root info rejected: backend mismatch"
                         << ", requester_backend=" << requesterBackendKind
                         << ", owner_backend=" << backend_->BackendKind();
            return true;
        }
        if (backend_->BackendKind() == "hixl" && req.hixlRoutePolicy != backend_->RoutePolicy()) {
            rsp->code = static_cast<int32_t>(ErrorCode::kNotSupported);
            rsp->msg = "hixl route policy mismatch, requester=" + req.hixlRoutePolicy +
                       ", owner=" + backend_->RoutePolicy();
            TE_LOG_WARNING << "exchange root info rejected: hixl route mismatch"
                         << ", requester_route=" << req.hixlRoutePolicy
                         << ", owner_route=" << backend_->RoutePolicy();
            return true;
        }
        return false;
    }

    Result EnqueueOwnerInit(const ExchangeRootInfoRequest &req, ExchangeRootInfoResponse *rsp)
    {
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
                TE_LOG_WARNING << "owner init queue full"
                             << ", requester=" << req.requesterHost << ":" << req.requesterPort
                             << ", requester_device_id=" << req.requesterDeviceId
                             << ", queue_size=" << ownerInitQueue_.size();
                return Result::OK();
            }
            ownerInitQueue_.push_back(std::move(task));
        }
        TE_LOG_INFO << "exchange root info accepted"
                  << ", requester=" << req.requesterHost << ":" << req.requesterPort
                  << ", requester_device_id=" << req.requesterDeviceId
                  << ", owner_device_id=" << localDeviceId_
                  << ", backend=" << backend_->BackendKind()
                  << ", hixl_route_policy=" << backend_->RoutePolicy()
                  << ", owner_mem_generation=" << rsp->ownerMemGeneration;
        ownerInitQueueCv_.notify_one();
        return Result::OK();
    }

    Result HandleReceiverDrivenBatchRead(const BatchReadTriggerRequest &req, BatchReadTriggerResponse *rsp)
    {
        std::vector<TransferMemoryRegion> ranges;
        ranges.reserve(req.items.size());
        for (size_t i = 0; i < req.items.size(); ++i) {
            const auto &item = req.items[i];
            if (item.remoteAddr == 0 || item.length == 0) {
                rsp->code = static_cast<int32_t>(ErrorCode::kInvalid);
                rsp->msg = "invalid batch read item";
                rsp->failedItemIndex = static_cast<int32_t>(i);
                return Result::OK();
            }
            ranges.push_back(TransferMemoryRegion{ item.remoteAddr, item.length });
        }
        uint64_t leaseId = 0;
        Result leaseRc = registeredMemory_->AcquireReadLease(ranges, localDeviceId_, GetReadLeaseTtlMs(), &leaseId);
        if (leaseRc.IsError()) {
            rsp->code = static_cast<int32_t>(leaseRc.GetCode());
            rsp->msg = leaseRc.GetMsg();
            rsp->failedItemIndex = -1;
            return leaseRc;
        }
        rsp->code = kRpcOkCode;
        rsp->msg = "accepted";
        rsp->readLeaseId = leaseId;
        rsp->ownerMemGeneration = backend_->MemoryGeneration();
        TE_LOG_INFO << "hixl batch read lease accepted"
                  << ", requester=" << req.requesterHost << ":" << req.requesterPort
                  << ", requester_device_id=" << req.requesterDeviceId
                  << ", item_count=" << req.items.size()
                  << ", read_lease_id=" << leaseId
                  << ", owner_mem_generation=" << rsp->ownerMemGeneration;
        return Result::OK();
    }

    Result HandlePostSendBatchRead(const BatchReadTriggerRequest &req, BatchReadTriggerResponse *rsp)
    {
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
            if (singleRsp.code == kRpcOkCode) {
                continue;
            }
            rsp->code = singleRsp.code;
            rsp->msg = singleRsp.msg;
            rsp->failedItemIndex = static_cast<int32_t>(i);
            return Result::OK();
        }

        rsp->code = kRpcOkCode;
        rsp->msg = "accepted";
        return Result::OK();
    }

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
                TE_VLOG_1 << "owner init done"
                          << ", peer=" << task.spec.peerHost << ":" << task.spec.peerPort
                          << ", peer_device_id=" << task.spec.peerDeviceId
                          << ", owner_device_id=" << localDeviceId_;
            } else {
                connMgr_->MarkStale(task.key);
                TE_LOG_ERROR << "owner init failed"
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
