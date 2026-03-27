#include "datasystem/transfer_engine/transfer_engine.h"

#include <chrono>
#include <functional>
#include <thread>
#include <unordered_map>

#include <glog/logging.h>

#include "internal/connection/connection_manager.h"
#include "internal/control_plane/control_plane.h"
#include "internal/backend/mock_data_plane_backend.h"
#include "internal/control_plane/transfer_control_service.h"
#include "internal/log/logging.h"
#include "internal/log/environment_dump.h"
#include "internal/runtime/acl_runtime_helper.h"
#ifdef TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY
#include "internal/backend/ascend/p2p_transfer_backend.h"
#endif
#include "internal/memory/registered_memory_table.h"
#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace {

constexpr int32_t kRpcOkCode = 0;
constexpr int32_t kConnReadyRetryCount = 50;
constexpr int32_t kConnReadyRetryIntervalMs = 10;
constexpr uint64_t kDefaultRecvWaitTimeoutMs = 10000;
constexpr int32_t kDefaultRpcThreads = 8;

Result RpcCodeToStatus(int32_t code, const std::string &msg)
{
    if (code == kRpcOkCode) {
        return Result::OK();
    }
    switch (static_cast<ErrorCode>(code)) {
        case ErrorCode::kInvalid:
        case ErrorCode::kNotFound:
        case ErrorCode::kRuntimeError:
        case ErrorCode::kNotReady:
        case ErrorCode::kNotAuthorized:
        case ErrorCode::kNotSupported:
            return TE_MAKE_STATUS(static_cast<ErrorCode>(code), msg);
        default:
            return TE_MAKE_STATUS(ErrorCode::kRuntimeError, msg);
    }
}

Result ParseTargetHostname(const std::string &targetHostname, std::string *peerHost, uint16_t *peerPort)
{
    TE_CHECK_PTR_OR_RETURN(peerHost);
    TE_CHECK_PTR_OR_RETURN(peerPort);
    TE_CHECK_OR_RETURN(!targetHostname.empty(), ErrorCode::kInvalid, "targetHostname is empty");

    std::string host;
    std::string portStr;

    if (targetHostname.front() == '[') {
        const size_t close = targetHostname.find(']');
        TE_CHECK_OR_RETURN(close != std::string::npos, ErrorCode::kInvalid, "invalid targetHostname");
        TE_CHECK_OR_RETURN(close + 2 <= targetHostname.size() && targetHostname[close + 1] == ':',
                           ErrorCode::kInvalid, "targetHostname missing port");
        host = targetHostname.substr(1, close - 1);
        portStr = targetHostname.substr(close + 2);
    } else {
        const size_t sep = targetHostname.rfind(':');
        TE_CHECK_OR_RETURN(sep != std::string::npos, ErrorCode::kInvalid, "targetHostname missing host:port");
        host = targetHostname.substr(0, sep);
        portStr = targetHostname.substr(sep + 1);
    }

    TE_CHECK_OR_RETURN(!host.empty(), ErrorCode::kInvalid, "targetHostname host is empty");
    TE_CHECK_OR_RETURN(!portStr.empty(), ErrorCode::kInvalid, "targetHostname port is empty");
    uint64_t portValue = 0;
    for (char c : portStr) {
        TE_CHECK_OR_RETURN(c >= '0' && c <= '9', ErrorCode::kInvalid, "targetHostname port is invalid");
        portValue = portValue * 10 + static_cast<uint64_t>(c - '0');
        TE_CHECK_OR_RETURN(portValue <= 65535, ErrorCode::kInvalid, "targetHostname port is invalid");
    }
    TE_CHECK_OR_RETURN(portValue > 0, ErrorCode::kInvalid, "targetHostname port should be positive");

    *peerHost = host;
    *peerPort = static_cast<uint16_t>(portValue);
    return Result::OK();
}

Result ParseDeviceId(const std::string &deviceName, int32_t *deviceId)
{
    TE_CHECK_PTR_OR_RETURN(deviceId);
    constexpr char kDevicePrefix[] = "npu:";
    constexpr size_t kDevicePrefixLen = sizeof(kDevicePrefix) - 1;
    TE_CHECK_OR_RETURN(deviceName.size() > kDevicePrefixLen, ErrorCode::kInvalid, "device_name is invalid");
    TE_CHECK_OR_RETURN(deviceName.compare(0, kDevicePrefixLen, kDevicePrefix) == 0,
                       ErrorCode::kInvalid, "device_name should match npu:${device_id}");

    int64_t parsedDeviceId = 0;
    for (size_t i = kDevicePrefixLen; i < deviceName.size(); ++i) {
        const char c = deviceName[i];
        TE_CHECK_OR_RETURN(c >= '0' && c <= '9', ErrorCode::kInvalid,
                           "device_name should match npu:${device_id}");
        parsedDeviceId = parsedDeviceId * 10 + static_cast<int64_t>(c - '0');
        TE_CHECK_OR_RETURN(parsedDeviceId <= std::numeric_limits<int32_t>::max(), ErrorCode::kInvalid,
                           "device_id is out of range");
    }

    *deviceId = static_cast<int32_t>(parsedDeviceId);
    return Result::OK();
}

}  // namespace

class TransferEngineState {
public:
    std::unordered_map<std::string, int32_t> endpointOwnerDeviceCache;
};

TransferEngine::TransferEngine()
#ifdef TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY
    : TransferEngine(std::make_shared<P2PTransferBackend>())
#else
    : TransferEngine(std::make_shared<MockDataPlaneBackend>())
#endif
{
}

TransferEngine::TransferEngine(std::shared_ptr<IDataPlaneBackend> backend)
    : connMgr_(std::make_shared<ConnectionManager>()),
      registeredMemory_(std::make_shared<RegisteredMemoryTable>()),
      backend_(std::move(backend)),
      controlService_(nullptr),
      controlClient_(std::make_shared<SocketControlClient>()),
      controlServer_(std::make_shared<SocketControlServer>()),
      state_(std::make_unique<TransferEngineState>())
{
}

TransferEngine::~TransferEngine()
{
    (void)Finalize();
}

Result TransferEngine::Initialize(const std::string &localHostname, const std::string &protocol,
                                  const std::string &deviceName)
{
    internal::EnsureGlogInitialized();
    std::string localHost;
    uint16_t localPort = 0;
    int32_t deviceId = -1;
    TE_RETURN_IF_ERROR(ParseTargetHostname(localHostname, &localHost, &localPort));
    TE_RETURN_IF_ERROR(ParseDeviceId(deviceName, &deviceId));

    std::lock_guard<std::mutex> lock(apiMutex_);
    TE_CHECK_OR_RETURN(!initialized_, ErrorCode::kInvalid, "transfer engine already initialized");
    TE_CHECK_OR_RETURN(backend_ != nullptr, ErrorCode::kInvalid, "backend is null");
    LOG(INFO) << "transfer engine initialize start"
              << ", local_hostname=" << localHostname << ", protocol=" << protocol
              << ", device_name=" << deviceName << ", local_host=" << localHost << ", local_port=" << localPort
              << ", device_id=" << deviceId
              << ", rpc_threads=" << kDefaultRpcThreads;

    localHost_ = localHost;
    localPort_ = localPort;
    deviceId_ = deviceId;
    rpcThreads_ = kDefaultRpcThreads;
    controlService_ = CreateTransferControlService(localHost_, localPort_, deviceId_, connMgr_, registeredMemory_,
                                                   backend_);
    Result startRc = controlServer_->Start(localHost_, localPort_, controlService_, rpcThreads_);
    if (startRc.IsError()) {
        LOG(ERROR) << "control server start failed"
                   << ", local_host=" << localHost_ << ", local_port=" << localPort_
                   << ", device_id=" << deviceId_ << ", reason=" << startRc.ToString();
        return startRc;
    }

    initialized_ = true;
    LOG(INFO) << "transfer engine initialize success"
              << ", local_host=" << localHost_ << ", local_port=" << localPort_
              << ", device_id=" << deviceId_;
    return Result::OK();
}

int32_t TransferEngine::GetRpcPort()
{
    std::lock_guard<std::mutex> lock(apiMutex_);
    if (!initialized_) {
        return -1;
    }
    return static_cast<int32_t>(localPort_);
}

Result TransferEngine::RegisterMemory(uintptr_t bufferAddrRegisrterch, size_t length)
{
    return BatchRegisterMemory({bufferAddrRegisrterch}, {length});
}

Result TransferEngine::BatchRegisterMemory(const std::vector<uintptr_t> &bufferAddrs, const std::vector<size_t> &lengths)
{
    TE_CHECK_OR_RETURN(!bufferAddrs.empty(), ErrorCode::kInvalid, "bufferAddrs is empty");
    TE_CHECK_OR_RETURN(bufferAddrs.size() == lengths.size(), ErrorCode::kInvalid, "bufferAddrs/lengths size mismatch");

    std::lock_guard<std::mutex> lock(apiMutex_);
    TE_CHECK_OR_RETURN(initialized_, ErrorCode::kNotReady, "transfer engine not initialized");
    TE_CHECK_OR_RETURN(deviceId_ >= 0, ErrorCode::kNotReady, "device_id is invalid");

    std::vector<uint64_t> addedBaseAddrs;
    addedBaseAddrs.reserve(bufferAddrs.size());
    for (size_t i = 0; i < bufferAddrs.size(); ++i) {
        TE_CHECK_OR_RETURN(bufferAddrs[i] > 0, ErrorCode::kInvalid, "bufferAddr should be positive");
        TE_CHECK_OR_RETURN(lengths[i] > 0, ErrorCode::kInvalid, "length should be positive");
        RegisteredRegion region{ static_cast<uint64_t>(bufferAddrs[i]), static_cast<uint64_t>(lengths[i]), deviceId_ };
        if (!registeredMemory_->AddRegion(region)) {
            for (uint64_t addr : addedBaseAddrs) {
                (void)registeredMemory_->RemoveByBaseAddr(addr);
            }
            return Result(ErrorCode::kInvalid, "failed to add registered region");
        }
        addedBaseAddrs.push_back(region.baseAddr);
    }

    LOG(INFO) << "batch register memory success"
              << ", device_id=" << deviceId_ << ", count=" << bufferAddrs.size();
    return Result::OK();
}

Result TransferEngine::UnregisterMemory(uintptr_t bufferAddrRegisrterch)
{
    return BatchUnregisterMemory({bufferAddrRegisrterch});
}

Result TransferEngine::BatchUnregisterMemory(const std::vector<uintptr_t> &bufferAddrs)
{
    std::lock_guard<std::mutex> lock(apiMutex_);
    TE_CHECK_OR_RETURN(initialized_, ErrorCode::kNotReady, "transfer engine not initialized");
    TE_CHECK_OR_RETURN(!bufferAddrs.empty(), ErrorCode::kInvalid, "bufferAddrs is empty");
    for (size_t i = 0; i < bufferAddrs.size(); ++i) {
        TE_CHECK_OR_RETURN(bufferAddrs[i] > 0, ErrorCode::kInvalid, "bufferAddr should be positive");
        TE_CHECK_OR_RETURN(registeredMemory_->RemoveByBaseAddr(static_cast<uint64_t>(bufferAddrs[i])),
                           ErrorCode::kNotFound, "region is not registered");
    }
    LOG(INFO) << "batch unregister memory success, count=" << bufferAddrs.size();
    return Result::OK();
}

Result TransferEngine::TransferSyncRead(const std::string &targetHostname, uintptr_t buffer, uintptr_t peerBufferAddress,
                                        size_t length)
{
    return BatchTransferSyncRead(targetHostname, {buffer}, {peerBufferAddress}, {length});
}

Result TransferEngine::BatchTransferSyncRead(const std::string &targetHostname, const std::vector<uintptr_t> &buffers,
                                             const std::vector<uintptr_t> &peerBufferAddresses,
                                             const std::vector<size_t> &lengths)
{
    internal::DumpProcessEnvironment("batch_transfer_sync_read_begin");
    TE_CHECK_OR_RETURN(!buffers.empty(), ErrorCode::kInvalid, "buffers is empty");
    TE_CHECK_OR_RETURN(buffers.size() == peerBufferAddresses.size() && buffers.size() == lengths.size(),
                       ErrorCode::kInvalid, "buffers/peerBufferAddresses/lengths size mismatch");

    std::string peerHost;
    uint16_t peerPort = 0;
    TE_RETURN_IF_ERROR(ParseTargetHostname(targetHostname, &peerHost, &peerPort));
    for (size_t i = 0; i < buffers.size(); ++i) {
        TE_CHECK_OR_RETURN(buffers[i] > 0, ErrorCode::kInvalid, "buffer should be positive");
        TE_CHECK_OR_RETURN(peerBufferAddresses[i] > 0, ErrorCode::kInvalid, "peerBufferAddress should be positive");
        TE_CHECK_OR_RETURN(lengths[i] > 0, ErrorCode::kInvalid, "length should be positive");
    }

    std::string localHostSnapshot;
    uint16_t localPortSnapshot = 0;
    int32_t deviceIdSnapshot = -1;
    uint64_t requestIdStart = 0;
    {
        std::lock_guard<std::mutex> lock(apiMutex_);
        TE_CHECK_OR_RETURN(initialized_, ErrorCode::kNotReady, "transfer engine not initialized");
        TE_CHECK_OR_RETURN(!finalizing_, ErrorCode::kNotReady, "transfer engine is finalizing");
        localHostSnapshot = localHost_;
        localPortSnapshot = localPort_;
        deviceIdSnapshot = deviceId_;
        requestIdStart = nextRequestId_;
        nextRequestId_ += static_cast<uint64_t>(buffers.size());
        ++inFlightSyncReads_;
    }
    auto finishGuard = std::unique_ptr<void, std::function<void(void *)>>(
        reinterpret_cast<void *>(1), [this](void *) {
            std::lock_guard<std::mutex> lock(apiMutex_);
            if (inFlightSyncReads_ > 0) {
                --inFlightSyncReads_;
            }
            if (finalizing_ && inFlightSyncReads_ == 0) {
                apiCv_.notify_all();
            }
        });

    LOG(INFO) << "batch sync read begin"
              << ", item_count=" << buffers.size() << ", target_hostname=" << targetHostname
              << ", peer=" << peerHost << ":" << peerPort
              << ", request_id_start=" << requestIdStart;

    int32_t ownerDeviceId = -1;
    Result connRc = BuildConnectionIfNeeded(peerHost, peerPort, &ownerDeviceId);
    if (connRc.IsError()) {
        LOG(ERROR) << "batch sync read build connection failed, reason=" << connRc.ToString();
        return connRc;
    }

    ConnectionSpec spec;
    spec.localHost = localHostSnapshot;
    spec.localPort = localPortSnapshot;
    spec.localDeviceId = deviceIdSnapshot;
    spec.peerHost = peerHost;
    spec.peerPort = peerPort;
    spec.peerDeviceId = ownerDeviceId;

    for (size_t i = 0; i < buffers.size(); ++i) {
        Result postRecvRc = backend_->PostRecv(spec, static_cast<uint64_t>(buffers[i]), static_cast<uint64_t>(lengths[i]));
        if (postRecvRc.IsError()) {
            LOG(ERROR) << "batch sync read post recv failed, item_index=" << i
                       << ", reason=" << postRecvRc.ToString();
            backend_->AbortConnection(spec);
            ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
            connMgr_->MarkStale(key);
            return postRecvRc;
        }
    }

    BatchReadTriggerRequest req;
    req.requesterHost = localHostSnapshot;
    req.requesterPort = localPortSnapshot;
    req.requesterDeviceId = deviceIdSnapshot;
    req.ownerDeviceId = ownerDeviceId;
    req.items.reserve(buffers.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        BatchReadItem one;
        one.requestId = requestIdStart + static_cast<uint64_t>(i);
        one.remoteAddr = static_cast<uint64_t>(peerBufferAddresses[i]);
        one.length = static_cast<uint64_t>(lengths[i]);
        req.items.push_back(one);
    }

    BatchReadTriggerResponse rsp;
    Result triggerRc = controlClient_->BatchReadTrigger(peerHost, peerPort, req, &rsp);
    if (triggerRc.IsError()) {
        LOG(ERROR) << "batch sync read rpc failed, reason=" << triggerRc.ToString();
        backend_->AbortConnection(spec);
        ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
        connMgr_->MarkStale(key);
        return triggerRc;
    }
    Result rpcStatus = RpcCodeToStatus(rsp.code, rsp.msg);
    if (rpcStatus.IsError()) {
        LOG(ERROR) << "batch sync read rpc returned error, failed_item_index=" << rsp.failedItemIndex
                   << ", reason=" << rsp.msg;
        backend_->AbortConnection(spec);
        ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
        connMgr_->MarkStale(key);
        return rpcStatus;
    }

    for (size_t i = 0; i < buffers.size(); ++i) {
        Result waitRc = backend_->WaitRecv(spec, kDefaultRecvWaitTimeoutMs);
        if (waitRc.IsError()) {
            LOG(ERROR) << "batch sync read wait recv failed, item_index=" << i
                       << ", reason=" << waitRc.ToString();
            ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
            connMgr_->MarkStale(key);
            return waitRc;
        }
    }
    LOG(INFO) << "batch sync read success, item_count=" << buffers.size()
              << ", target_hostname=" << targetHostname;
    return Result::OK();
}

Result TransferEngine::Finalize()
{
    {
        std::unique_lock<std::mutex> lock(apiMutex_);
        if (!initialized_) {
            return Result::OK();
        }
        LOG(INFO) << "transfer engine finalize start"
                  << ", local_host=" << localHost_ << ", local_port=" << localPort_
                  << ", device_id=" << deviceId_ << ", inflight_sync_reads=" << inFlightSyncReads_;
        finalizing_ = true;
        apiCv_.wait(lock, [this]() { return inFlightSyncReads_ == 0; });
    }

    if (controlServer_ != nullptr) {
        controlServer_->Stop();
    }
    {
        std::lock_guard<std::mutex> lock(apiMutex_);
        {
            std::lock_guard<std::mutex> endpointLock(endpointCacheMutex_);
            state_->endpointOwnerDeviceCache.clear();
        }
        initialized_ = false;
        finalizing_ = false;
        localHost_.clear();
        localPort_ = 0;
        deviceId_ = -1;
        rpcThreads_ = 0;
        nextRequestId_ = 1;
        controlService_.reset();
    }
    LOG(INFO) << "transfer engine finalize success";
    google::FlushLogFiles(google::GLOG_INFO);
    google::FlushLogFiles(google::GLOG_WARNING);
    google::FlushLogFiles(google::GLOG_ERROR);
    return Result::OK();
}

Result TransferEngine::BuildConnectionIfNeeded(const std::string &peerHost, uint16_t peerPort, int32_t *ownerDeviceId)
{
    TE_CHECK_PTR_OR_RETURN(ownerDeviceId);
    const std::string endpoint = peerHost + ":" + std::to_string(peerPort);
    int32_t cachedOwnerDeviceId = -1;
    {
        std::lock_guard<std::mutex> lock(endpointCacheMutex_);
        const auto cacheIter = state_->endpointOwnerDeviceCache.find(endpoint);
        if (cacheIter != state_->endpointOwnerDeviceCache.end()) {
            cachedOwnerDeviceId = cacheIter->second;
        }
    }
    if (cachedOwnerDeviceId >= 0) {
        ConnectionKey key{ deviceId_, peerHost, peerPort, cachedOwnerDeviceId };
        if (connMgr_->HasReadyConnection(key)) {
            LOG(INFO) << "reuse cached connection"
                      << ", peer=" << peerHost << ":" << peerPort
                      << ", owner_device_id=" << cachedOwnerDeviceId;
            QueryConnReadyRequest queryReq;
            queryReq.requesterHost = localHost_;
            queryReq.requesterPort = localPort_;
            queryReq.requesterDeviceId = deviceId_;
            queryReq.ownerDeviceId = cachedOwnerDeviceId;

            QueryConnReadyResponse queryRsp;
            Result queryRc = controlClient_->QueryConnReady(peerHost, peerPort, queryReq, &queryRsp);
            if (queryRc.IsOk()) {
                Result rpcStatus = RpcCodeToStatus(queryRsp.code, queryRsp.msg);
                if (rpcStatus.IsOk() && queryRsp.ready) {
                    *ownerDeviceId = cachedOwnerDeviceId;
                    return Result::OK();
                }
            }
            LOG(WARNING) << "cached connection stale, rebuilding"
                         << ", peer=" << peerHost << ":" << peerPort
                         << ", owner_device_id=" << cachedOwnerDeviceId;
            connMgr_->MarkStale(key);
            std::lock_guard<std::mutex> lock(endpointCacheMutex_);
            state_->endpointOwnerDeviceCache.erase(endpoint);
        }
    }
    return BuildConnectionOnce(peerHost, peerPort, ownerDeviceId);
}

Result TransferEngine::BuildConnectionOnce(const std::string &peerHost, uint16_t peerPort, int32_t *ownerDeviceId)
{
    TE_CHECK_PTR_OR_RETURN(ownerDeviceId);
    std::lock_guard<std::mutex> lock(chainMutex_);
    LOG(INFO) << "build connection start"
              << ", peer=" << peerHost << ":" << peerPort << ", device_id=" << deviceId_;
    internal::DumpProcessEnvironment("build_connection_once_start");

    std::string rootInfo;
    Result createRootRc = backend_->CreateRootInfo(&rootInfo);
    if (createRootRc.IsError()) {
        LOG(ERROR) << "build connection create root info failed"
                   << ", peer=" << peerHost << ":" << peerPort << ", reason=" << createRootRc.ToString();
        return createRootRc;
    }

    ExchangeRootInfoRequest exchangeReq;
    exchangeReq.requesterHost = localHost_;
    exchangeReq.requesterPort = localPort_;
    exchangeReq.requesterDeviceId = deviceId_;
    exchangeReq.ownerDeviceId = -1;
    exchangeReq.rootInfo = rootInfo;

    ExchangeRootInfoResponse exchangeRsp;
    Result exchangeRc = controlClient_->ExchangeRootInfo(peerHost, peerPort, exchangeReq, &exchangeRsp);
    if (exchangeRc.IsError()) {
        LOG(ERROR) << "build connection exchange root info rpc failed"
                   << ", peer=" << peerHost << ":" << peerPort << ", reason=" << exchangeRc.ToString();
        return exchangeRc;
    }
    Result exchangeRpcStatus = RpcCodeToStatus(exchangeRsp.code, exchangeRsp.msg);
    if (exchangeRpcStatus.IsError()) {
        LOG(ERROR) << "build connection exchange root info rejected"
                   << ", peer=" << peerHost << ":" << peerPort << ", rsp_code=" << exchangeRsp.code
                   << ", rsp_msg=" << exchangeRsp.msg;
        return exchangeRpcStatus;
    }
    TE_CHECK_OR_RETURN(exchangeRsp.ownerDeviceId >= 0, ErrorCode::kRuntimeError, "owner_device_id is invalid");

    ConnectionSpec spec;
    spec.localHost = localHost_;
    spec.localPort = localPort_;
    spec.localDeviceId = deviceId_;
    spec.peerHost = peerHost;
    spec.peerPort = peerPort;
    spec.peerDeviceId = exchangeRsp.ownerDeviceId;
    Result initRecvRc = backend_->InitRecv(spec, rootInfo);
    if (initRecvRc.IsError()) {
        LOG(ERROR) << "build connection init recv failed"
                   << ", peer=" << peerHost << ":" << peerPort
                   << ", owner_device_id=" << exchangeRsp.ownerDeviceId << ", reason=" << initRecvRc.ToString();
        return initRecvRc;
    }

    ConnectionKey key{ deviceId_, peerHost, peerPort, exchangeRsp.ownerDeviceId };
    connMgr_->MarkRequesterRecvReady(key);

    QueryConnReadyRequest queryReq;
    queryReq.requesterHost = localHost_;
    queryReq.requesterPort = localPort_;
    queryReq.requesterDeviceId = deviceId_;
    queryReq.ownerDeviceId = exchangeRsp.ownerDeviceId;

    QueryConnReadyResponse queryRsp;
    for (int32_t i = 0; i < kConnReadyRetryCount; ++i) {
        TE_RETURN_IF_ERROR(controlClient_->QueryConnReady(peerHost, peerPort, queryReq, &queryRsp));
        TE_RETURN_IF_ERROR(RpcCodeToStatus(queryRsp.code, queryRsp.msg));
        if (queryRsp.ready) {
            connMgr_->MarkOwnerSendReady(key);
            *ownerDeviceId = exchangeRsp.ownerDeviceId;
            {
                std::lock_guard<std::mutex> lock(endpointCacheMutex_);
                state_->endpointOwnerDeviceCache[peerHost + ":" + std::to_string(peerPort)] = exchangeRsp.ownerDeviceId;
            }
            return Result::OK();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kConnReadyRetryIntervalMs));
    }

    connMgr_->MarkStale(key);
    LOG(WARNING) << "build connection timeout waiting owner ready"
                 << ", peer=" << peerHost << ":" << peerPort
                 << ", owner_device_id=" << exchangeRsp.ownerDeviceId
                 << ", retry_count=" << kConnReadyRetryCount;
    return TE_MAKE_STATUS(ErrorCode::kNotReady,
                          "connection is not ready, peer=" + peerHost + ":" + std::to_string(peerPort) +
                              ", device_id=" + std::to_string(deviceId_) +
                              ", owner_device_id=" + std::to_string(exchangeRsp.ownerDeviceId));
}

std::string TransferEngine::CreateRootInfo() const
{
    return "te_root_info_v0_1_0";
}

}  // namespace datasystem
