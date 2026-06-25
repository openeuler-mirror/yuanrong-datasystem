#include "datasystem/transfer_engine/transfer_engine.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <thread>
#include <unordered_map>
#include <utility>

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
#ifdef TRANSFER_ENGINE_ENABLE_HIXL
#include "internal/backend/ascend/hixl_d2d_backend.h"
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
constexpr int32_t K_MAX_HIXL_GENERATION_RETRY = 2;
constexpr uint64_t K_MAX_TCP_PORT = 65535;
constexpr uint64_t K_DECIMAL_BASE = 10;

std::string ToLowerAscii(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::string GetEnvString(const char *name)
{
    const char *value = std::getenv(name);
    return value == nullptr ? std::string() : std::string(value);
}

class ScopeExit final {
public:
    explicit ScopeExit(std::function<void()> fn) : fn_(std::move(fn))
    {
    }

    ~ScopeExit()
    {
        if (fn_) {
            fn_();
        }
    }

    ScopeExit(const ScopeExit &) = delete;
    ScopeExit &operator=(const ScopeExit &) = delete;

private:
    std::function<void()> fn_;
};

Result ResolveBackendKind(const std::string &protocol, std::string &backendKind)
{
    const std::string envBackend = ToLowerAscii(GetEnvString("TRANSFER_ENGINE_BACKEND"));
    if (!envBackend.empty()) {
        TE_CHECK_OR_RETURN(envBackend == "p2p" || envBackend == "hixl", ErrorCode::kInvalid,
                           "TRANSFER_ENGINE_BACKEND should be p2p or hixl");
        backendKind = envBackend;
        return Result::OK();
    }

    const std::string protocolLower = ToLowerAscii(protocol);
    if (protocolLower.empty() || protocolLower == "ascend" || protocolLower == "p2p") {
        backendKind = "p2p";
        return Result::OK();
    }
    if (protocolLower == "hixl" || protocolLower == "d2d_hixl" || protocolLower == "hccs_hixl") {
        backendKind = "hixl";
        return Result::OK();
    }
    return TE_MAKE_STATUS(ErrorCode::kInvalid, "unsupported transfer engine protocol: " + protocol);
}

Result CreateBackendByKind(const std::string &backendKind, std::shared_ptr<IDataPlaneBackend> &backend)
{
    if (backendKind == "p2p") {
#ifdef TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY
        backend = std::make_shared<P2PTransferBackend>();
#else
        backend = std::make_shared<MockDataPlaneBackend>();
#endif
        return Result::OK();
    }
    if (backendKind == "hixl") {
#ifdef TRANSFER_ENGINE_ENABLE_HIXL
        backend = std::make_shared<HixlD2DBackend>();
        return Result::OK();
#else
        return TE_MAKE_STATUS(ErrorCode::kNotSupported,
                              "hixl backend is not compiled; configure with -DTRANSFER_ENGINE_ENABLE_HIXL=ON");
#endif
    }
    return TE_MAKE_STATUS(ErrorCode::kInvalid, "unknown backend kind: " + backendKind);
}

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
        portValue = portValue * K_DECIMAL_BASE + static_cast<uint64_t>(c - '0');
        TE_CHECK_OR_RETURN(portValue <= K_MAX_TCP_PORT, ErrorCode::kInvalid, "targetHostname port is invalid");
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
    struct EndpointCacheEntry {
        int32_t ownerDeviceId = -1;
        uint64_t ownerMemGeneration = 0;
    };

    std::unordered_map<std::string, EndpointCacheEntry> endpointOwnerDeviceCache;
};

TransferEngine::TransferEngine()
    : connMgr_(std::make_shared<ConnectionManager>()),
      registeredMemory_(std::make_shared<RegisteredMemoryTable>()),
      backend_(nullptr),
      controlService_(nullptr),
      controlClient_(std::make_shared<SocketControlClient>()),
      controlServer_(std::make_shared<SocketControlServer>()),
      state_(std::make_unique<TransferEngineState>())
{
    backendInjected_ = false;
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
    backendInjected_ = true;
}

TransferEngine::~TransferEngine()
{
    (void)Finalize();
}

Result TransferEngine::Initialize(const std::string &localHostname, const std::string &protocol,
                                  const std::string &deviceName)
{
    internal::InitializeLogging();
    std::string localHost;
    uint16_t localPort = 0;
    int32_t deviceId = -1;
    TE_RETURN_IF_ERROR(ParseTargetHostname(localHostname, &localHost, &localPort));
    TE_RETURN_IF_ERROR(ParseDeviceId(deviceName, &deviceId));

    std::lock_guard<std::mutex> lock(apiMutex_);
    TE_CHECK_OR_RETURN(!initialized_, ErrorCode::kInvalid, "transfer engine already initialized");

    localHost_ = localHost;
    localPort_ = localPort;
    deviceId_ = deviceId;
    rpcThreads_ = kDefaultRpcThreads;
    TE_RETURN_IF_ERROR(InitializeBackendLocked(protocol));
    Result startRc = StartControlServerLocked();
    if (startRc.IsError()) {
        return startRc;
    }

    initialized_ = true;
    TE_LOG_INFO << "transfer engine initialize success"
              << ", local_host=" << localHost_ << ", local_port=" << localPort_
              << ", device_id=" << deviceId_;
    return Result::OK();
}

Result TransferEngine::InitializeBackendLocked(const std::string &protocol)
{
    std::string selectedBackendKind;
    TE_RETURN_IF_ERROR(ResolveBackendKind(protocol, selectedBackendKind));
    if (!backendInjected_) {
        std::shared_ptr<IDataPlaneBackend> selectedBackend;
        TE_RETURN_IF_ERROR(CreateBackendByKind(selectedBackendKind, selectedBackend));
        backend_ = std::move(selectedBackend);
    }
    TE_CHECK_OR_RETURN(backend_ != nullptr, ErrorCode::kInvalid, "backend is null");
    if (backendInjected_) {
        TE_CHECK_OR_RETURN(selectedBackendKind == backend_->BackendKind(), ErrorCode::kNotSupported,
                           "injected backend kind does not match requested protocol/backend");
    }

    TE_LOG_INFO << "transfer engine initialize start"
              << ", protocol=" << protocol
              << ", local_host=" << localHost_ << ", local_port=" << localPort_
              << ", device_id=" << deviceId_
              << ", backend=" << backend_->BackendKind()
              << ", backend_injected=" << backendInjected_
              << ", rpc_threads=" << kDefaultRpcThreads;

    Result backendInitRc = backend_->InitializeLocal(localHost_, localPort_, deviceId_);
    if (backendInitRc.IsError()) {
        TE_LOG_ERROR << "backend initialize failed"
                   << ", backend=" << backend_->BackendKind()
                   << ", local_host=" << localHost_ << ", local_port=" << localPort_
                   << ", device_id=" << deviceId_ << ", reason=" << backendInitRc.ToString();
        return backendInitRc;
    }
    return Result::OK();
}

Result TransferEngine::StartControlServerLocked()
{
    controlService_ = CreateTransferControlService(localHost_, localPort_, deviceId_, connMgr_, registeredMemory_,
                                                   backend_);
    Result startRc = controlServer_->Start(localHost_, localPort_, controlService_, rpcThreads_);
    if (startRc.IsError()) {
        TE_LOG_ERROR << "control server start failed"
                   << ", local_host=" << localHost_ << ", local_port=" << localPort_
                   << ", device_id=" << deviceId_ << ", reason=" << startRc.ToString();
        controlService_.reset();
        backend_->FinalizeLocal();
        return startRc;
    }
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
    TE_CHECK_OR_RETURN(!finalizing_, ErrorCode::kNotReady, "transfer engine is finalizing");
    TE_CHECK_OR_RETURN(deviceId_ >= 0, ErrorCode::kNotReady, "device_id is invalid");

    std::vector<RegisteredRegion> addedRegions;
    std::vector<RegisteredRegion> backendRegisteredRegions;
    addedRegions.reserve(bufferAddrs.size());
    backendRegisteredRegions.reserve(bufferAddrs.size());
    for (size_t i = 0; i < bufferAddrs.size(); ++i) {
        TE_CHECK_OR_RETURN(bufferAddrs[i] > 0, ErrorCode::kInvalid, "bufferAddr should be positive");
        TE_CHECK_OR_RETURN(lengths[i] > 0, ErrorCode::kInvalid, "length should be positive");
        RegisteredRegion region{ static_cast<uint64_t>(bufferAddrs[i]), static_cast<uint64_t>(lengths[i]), deviceId_ };
        RegisteredRegion existing;
        TE_CHECK_OR_RETURN(!registeredMemory_->FindRegionByBaseAddr(region.baseAddr, &existing),
                           ErrorCode::kInvalid, "region is already registered");
        Result backendRegRc = backend_->RegisterLocalMemory(region.baseAddr, region.length);
        if (backendRegRc.IsError()) {
            for (auto it = backendRegisteredRegions.rbegin(); it != backendRegisteredRegions.rend(); ++it) {
                (void)backend_->UnregisterLocalMemory(it->baseAddr, it->length);
            }
            for (const auto &added : addedRegions) {
                (void)registeredMemory_->RemoveByBaseAddr(added.baseAddr);
            }
            return backendRegRc;
        }
        backendRegisteredRegions.push_back(region);
        if (!registeredMemory_->AddRegion(region)) {
            for (auto it = backendRegisteredRegions.rbegin(); it != backendRegisteredRegions.rend(); ++it) {
                (void)backend_->UnregisterLocalMemory(it->baseAddr, it->length);
            }
            for (const auto &added : addedRegions) {
                (void)registeredMemory_->RemoveByBaseAddr(added.baseAddr);
            }
            return Result(ErrorCode::kInvalid, "failed to add registered region");
        }
        addedRegions.push_back(region);
    }

    TE_LOG_INFO << "batch register memory success"
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
    TE_CHECK_OR_RETURN(!finalizing_, ErrorCode::kNotReady, "transfer engine is finalizing");
    TE_CHECK_OR_RETURN(!bufferAddrs.empty(), ErrorCode::kInvalid, "bufferAddrs is empty");
    for (size_t i = 0; i < bufferAddrs.size(); ++i) {
        TE_CHECK_OR_RETURN(bufferAddrs[i] > 0, ErrorCode::kInvalid, "bufferAddr should be positive");
        RegisteredRegion region;
        TE_CHECK_OR_RETURN(registeredMemory_->FindRegionByBaseAddr(static_cast<uint64_t>(bufferAddrs[i]), &region),
                           ErrorCode::kNotFound, "region is not registered");
        auto removeRc = registeredMemory_->RemoveByBaseAddrIfNoActiveLease(region.baseAddr);
        if (removeRc == RegisteredMemoryTable::RemoveResult::K_BUSY) {
            return Result(ErrorCode::kNotReady, "region has active read lease");
        }
        TE_CHECK_OR_RETURN(removeRc == RegisteredMemoryTable::RemoveResult::K_REMOVED,
                           ErrorCode::kNotFound, "region is not registered");
        Result backendRc = backend_->UnregisterLocalMemory(region.baseAddr, region.length);
        if (backendRc.IsError()) {
            if (!registeredMemory_->AddRegion(region)) {
                TE_LOG_ERROR << "restore registered region failed after backend unregister failure"
                             << ", base_addr=0x" << std::hex << region.baseAddr << std::dec
                             << ", length=" << region.length
                             << ", backend_error=" << backendRc.ToString();
            }
            return backendRc;
        }
    }
    TE_LOG_INFO << "batch unregister memory success, count=" << bufferAddrs.size();
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
    ScopeExit finishGuard([this]() {
        std::lock_guard<std::mutex> lock(apiMutex_);
        if (inFlightSyncReads_ > 0) {
            --inFlightSyncReads_;
        }
        if (finalizing_ && inFlightSyncReads_ == 0) {
            apiCv_.notify_all();
        }
    });

    TE_VLOG_1 << "batch sync read begin"
              << ", item_count=" << buffers.size() << ", target_hostname=" << targetHostname
              << ", peer=" << peerHost << ":" << peerPort
              << ", request_id_start=" << requestIdStart
              << ", backend=" << backend_->BackendKind();

    if (backend_->SupportsReceiverDrivenRead()) {
        std::vector<TransferMemoryRegion> destRegions;
        destRegions.reserve(buffers.size());
        for (size_t i = 0; i < buffers.size(); ++i) {
            destRegions.push_back(TransferMemoryRegion{ static_cast<uint64_t>(buffers[i]),
                                                         static_cast<uint64_t>(lengths[i]) });
        }
        const uint64_t localMemGenerationBefore = backend_->MemoryGeneration();
        Result prepareRc = backend_->PrepareReadDestinations(destRegions);
        if (prepareRc.IsError()) {
            return prepareRc;
        }
        if (backend_->MemoryGeneration() != localMemGenerationBefore) {
            std::lock_guard<std::mutex> lock(endpointCacheMutex_);
            state_->endpointOwnerDeviceCache.clear();
        }

        for (int32_t attempt = 0; attempt < K_MAX_HIXL_GENERATION_RETRY; ++attempt) {
            int32_t ownerDeviceId = -1;
            uint64_t ownerMemGeneration = 0;
            Result connRc = BuildConnectionIfNeeded(peerHost, peerPort, &ownerDeviceId, &ownerMemGeneration);
            if (connRc.IsError()) {
                TE_LOG_ERROR << "batch sync read build hixl connection failed, reason=" << connRc.ToString();
                return connRc;
            }

            ConnectionSpec spec;
            spec.localHost = localHostSnapshot;
            spec.localPort = localPortSnapshot;
            spec.localDeviceId = deviceIdSnapshot;
            spec.peerHost = peerHost;
            spec.peerPort = peerPort;
            spec.peerDeviceId = ownerDeviceId;

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
                TE_LOG_ERROR << "batch sync read hixl trigger rpc failed, reason=" << triggerRc.ToString();
                backend_->AbortConnection(spec);
                ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
                connMgr_->MarkStale(key);
                return triggerRc;
            }
            Result rpcStatus = RpcCodeToStatus(rsp.code, rsp.msg);
            if (rpcStatus.IsError()) {
                TE_LOG_ERROR << "batch sync read hixl trigger rejected, failed_item_index=" << rsp.failedItemIndex
                           << ", reason=" << rsp.msg;
                backend_->AbortConnection(spec);
                ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
                connMgr_->MarkStale(key);
                return rpcStatus;
            }

            auto releaseLease = [this, &rsp, &peerHost, peerPort, &localHostSnapshot, localPortSnapshot,
                                 deviceIdSnapshot]() {
                if (rsp.readLeaseId == 0) {
                    return;
                }
                ReleaseReadLeaseRequest releaseReq;
                releaseReq.readLeaseId = rsp.readLeaseId;
                releaseReq.requesterHost = localHostSnapshot;
                releaseReq.requesterPort = localPortSnapshot;
                releaseReq.requesterDeviceId = deviceIdSnapshot;
                ReleaseReadLeaseResponse releaseRsp;
                Result releaseRc = controlClient_->ReleaseReadLease(peerHost, peerPort, releaseReq, &releaseRsp);
                if (releaseRc.IsError()) {
                    TE_LOG_WARNING << "release hixl read lease rpc failed"
                                 << ", read_lease_id=" << rsp.readLeaseId
                                 << ", reason=" << releaseRc.ToString();
                }
            };

            if (rsp.ownerMemGeneration != ownerMemGeneration) {
                releaseLease();
                backend_->AbortConnection(spec);
                ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
                connMgr_->MarkStale(key);
                {
                    std::lock_guard<std::mutex> lock(endpointCacheMutex_);
                    state_->endpointOwnerDeviceCache.erase(peerHost + ":" + std::to_string(peerPort));
                }
                TE_LOG_WARNING << "hixl owner memory generation changed during read authorization"
                             << ", cached_generation=" << ownerMemGeneration
                             << ", trigger_generation=" << rsp.ownerMemGeneration
                             << ", attempt=" << attempt;
                continue;
            }

            std::vector<TransferReadOp> ops;
            ops.reserve(buffers.size());
            for (size_t i = 0; i < buffers.size(); ++i) {
                ops.push_back(TransferReadOp{ static_cast<uint64_t>(buffers[i]),
                                              static_cast<uint64_t>(peerBufferAddresses[i]),
                                              static_cast<uint64_t>(lengths[i]) });
            }
            // Use the backend-configured HIXL transfer timeout.
            Result readRc = backend_->TransferSyncRead(spec, ops, 0);
            releaseLease();
            if (readRc.IsError()) {
                backend_->AbortConnection(spec);
                ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
                connMgr_->MarkStale(key);
                TE_LOG_ERROR << "hixl transfer sync read failed, reason=" << readRc.ToString();
                return readRc;
            }
            TE_LOG_INFO << "hixl batch sync read success"
                      << ", item_count=" << buffers.size()
                      << ", target_hostname=" << targetHostname
                      << ", owner_mem_generation=" << ownerMemGeneration;
            return Result::OK();
        }
        return TE_MAKE_STATUS(ErrorCode::kNotReady, "owner memory generation keeps changing during hixl read");
    }

    int32_t ownerDeviceId = -1;
    uint64_t ownerMemGeneration = 0;
    Result connRc = BuildConnectionIfNeeded(peerHost, peerPort, &ownerDeviceId, &ownerMemGeneration);
    if (connRc.IsError()) {
        TE_LOG_ERROR << "batch sync read build connection failed, reason=" << connRc.ToString();
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
            TE_LOG_ERROR << "batch sync read post recv failed, item_index=" << i
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
        TE_LOG_ERROR << "batch sync read rpc failed, reason=" << triggerRc.ToString();
        backend_->AbortConnection(spec);
        ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
        connMgr_->MarkStale(key);
        return triggerRc;
    }
    Result rpcStatus = RpcCodeToStatus(rsp.code, rsp.msg);
    if (rpcStatus.IsError()) {
        TE_LOG_ERROR << "batch sync read rpc returned error, failed_item_index=" << rsp.failedItemIndex
                   << ", reason=" << rsp.msg;
        backend_->AbortConnection(spec);
        ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
        connMgr_->MarkStale(key);
        return rpcStatus;
    }

    for (size_t i = 0; i < buffers.size(); ++i) {
        Result waitRc = backend_->WaitRecv(spec, kDefaultRecvWaitTimeoutMs);
        if (waitRc.IsError()) {
            TE_LOG_ERROR << "batch sync read wait recv failed, item_index=" << i
                       << ", reason=" << waitRc.ToString();
            ConnectionKey key{ deviceIdSnapshot, peerHost, peerPort, ownerDeviceId };
            connMgr_->MarkStale(key);
            return waitRc;
        }
    }
    TE_LOG_INFO << "batch sync read success, item_count=" << buffers.size()
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
        TE_CHECK_OR_RETURN(!finalizing_, ErrorCode::kNotReady, "transfer engine is finalizing");
        TE_LOG_INFO << "transfer engine finalize start"
                  << ", local_host=" << localHost_ << ", local_port=" << localPort_
                  << ", device_id=" << deviceId_ << ", inflight_sync_reads=" << inFlightSyncReads_;
        finalizing_ = true;
        apiCv_.wait(lock, [this]() { return inFlightSyncReads_ == 0; });
    }

    if (controlServer_ != nullptr) {
        controlServer_->Stop();
    }
    std::shared_ptr<ITransferControlService> controlServiceToStop;
    {
        std::lock_guard<std::mutex> lock(apiMutex_);
        controlServiceToStop = std::move(controlService_);
    }
    controlServiceToStop.reset();
    if (backend_ != nullptr) {
        backend_->FinalizeLocal();
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
    }
    TE_LOG_INFO << "transfer engine finalize success";
    internal::FlushLogs();
    return Result::OK();
}

Result TransferEngine::BuildConnectionIfNeeded(const std::string &peerHost, uint16_t peerPort, int32_t *ownerDeviceId,
                                               uint64_t *ownerMemGeneration)
{
    TE_CHECK_PTR_OR_RETURN(ownerDeviceId);
    TE_CHECK_PTR_OR_RETURN(ownerMemGeneration);
    const std::string endpoint = peerHost + ":" + std::to_string(peerPort);
    TransferEngineState::EndpointCacheEntry cached;
    {
        std::lock_guard<std::mutex> lock(endpointCacheMutex_);
        const auto cacheIter = state_->endpointOwnerDeviceCache.find(endpoint);
        if (cacheIter != state_->endpointOwnerDeviceCache.end()) {
            cached = cacheIter->second;
        }
    }
    if (cached.ownerDeviceId < 0) {
        return BuildConnectionOnce(peerHost, peerPort, ownerDeviceId, ownerMemGeneration);
    }

    bool reused = false;
    TE_RETURN_IF_ERROR(TryReuseCachedConnection(peerHost, peerPort, cached.ownerDeviceId, cached.ownerMemGeneration,
                                                ownerDeviceId, ownerMemGeneration, &reused));
    if (reused) {
        return Result::OK();
    }
    return BuildConnectionOnce(peerHost, peerPort, ownerDeviceId, ownerMemGeneration);
}

Result TransferEngine::TryReuseCachedConnection(const std::string &peerHost, uint16_t peerPort,
                                                int32_t cachedOwnerDeviceId, uint64_t cachedOwnerMemGeneration,
                                                int32_t *ownerDeviceId, uint64_t *ownerMemGeneration, bool *reused)
{
    TE_CHECK_PTR_OR_RETURN(reused);
    *reused = false;
    ConnectionKey key{ deviceId_, peerHost, peerPort, cachedOwnerDeviceId };
    if (!connMgr_->HasReadyConnection(key)) {
        return Result::OK();
    }
    TE_VLOG_1 << "reuse cached connection"
              << ", peer=" << peerHost << ":" << peerPort
              << ", owner_device_id=" << cachedOwnerDeviceId
              << ", owner_mem_generation=" << cachedOwnerMemGeneration;
    QueryConnReadyRequest queryReq;
    queryReq.requesterHost = localHost_;
    queryReq.requesterPort = localPort_;
    queryReq.requesterDeviceId = deviceId_;
    queryReq.ownerDeviceId = cachedOwnerDeviceId;

    QueryConnReadyResponse queryRsp;
    Result queryRc = controlClient_->QueryConnReady(peerHost, peerPort, queryReq, &queryRsp);
    if (queryRc.IsOk()) {
        Result rpcStatus = RpcCodeToStatus(queryRsp.code, queryRsp.msg);
        const bool generationMatches =
            !backend_->SupportsReceiverDrivenRead() || queryRsp.ownerMemGeneration == cachedOwnerMemGeneration;
        if (rpcStatus.IsOk() && queryRsp.ready && generationMatches) {
            *ownerDeviceId = cachedOwnerDeviceId;
            *ownerMemGeneration = queryRsp.ownerMemGeneration;
            *reused = true;
            return Result::OK();
        }
    }
    TE_LOG_WARNING << "cached connection stale, rebuilding"
                 << ", peer=" << peerHost << ":" << peerPort
                 << ", owner_device_id=" << cachedOwnerDeviceId
                 << ", cached_owner_mem_generation=" << cachedOwnerMemGeneration;
    connMgr_->MarkStale(key);
    std::lock_guard<std::mutex> lock(endpointCacheMutex_);
    state_->endpointOwnerDeviceCache.erase(peerHost + ":" + std::to_string(peerPort));
    return Result::OK();
}

Result TransferEngine::BuildConnectionOnce(const std::string &peerHost, uint16_t peerPort, int32_t *ownerDeviceId,
                                           uint64_t *ownerMemGeneration)
{
    TE_CHECK_PTR_OR_RETURN(ownerDeviceId);
    TE_CHECK_PTR_OR_RETURN(ownerMemGeneration);
    std::lock_guard<std::mutex> lock(chainMutex_);
    TE_LOG_INFO << "build connection start"
              << ", peer=" << peerHost << ":" << peerPort << ", device_id=" << deviceId_;
    internal::DumpProcessEnvironment("build_connection_once_start");

    std::string rootInfo;
    Result createRootRc = backend_->CreateRootInfo(&rootInfo);
    if (createRootRc.IsError()) {
        TE_LOG_ERROR << "build connection create root info failed"
                   << ", peer=" << peerHost << ":" << peerPort << ", reason=" << createRootRc.ToString();
        return createRootRc;
    }

    ExchangeRootInfoResponse exchangeRsp;
    TE_RETURN_IF_ERROR(ExchangeRootInfoForConnection(peerHost, peerPort, rootInfo, &exchangeRsp));
    TE_RETURN_IF_ERROR(InitRequesterRecvForConnection(peerHost, peerPort, rootInfo, exchangeRsp));
    TE_RETURN_IF_ERROR(WaitOwnerReadyAndCache(peerHost, peerPort, exchangeRsp.ownerDeviceId, ownerMemGeneration));
    *ownerDeviceId = exchangeRsp.ownerDeviceId;
    return Result::OK();
}

Result TransferEngine::ExchangeRootInfoForConnection(const std::string &peerHost, uint16_t peerPort,
                                                     const std::string &rootInfo,
                                                     ExchangeRootInfoResponse *exchangeRsp)
{
    TE_CHECK_PTR_OR_RETURN(exchangeRsp);
    ExchangeRootInfoRequest exchangeReq;
    exchangeReq.requesterHost = localHost_;
    exchangeReq.requesterPort = localPort_;
    exchangeReq.requesterDeviceId = deviceId_;
    exchangeReq.ownerDeviceId = -1;
    exchangeReq.rootInfo = rootInfo;
    exchangeReq.backendKind = backend_->BackendKind();
    exchangeReq.hixlRoutePolicy = backend_->RoutePolicy();

    Result exchangeRc = controlClient_->ExchangeRootInfo(peerHost, peerPort, exchangeReq, exchangeRsp);
    if (exchangeRc.IsError()) {
        TE_LOG_ERROR << "build connection exchange root info rpc failed"
                   << ", peer=" << peerHost << ":" << peerPort << ", reason=" << exchangeRc.ToString();
        return exchangeRc;
    }
    Result exchangeRpcStatus = RpcCodeToStatus(exchangeRsp->code, exchangeRsp->msg);
    if (exchangeRpcStatus.IsError()) {
        TE_LOG_ERROR << "build connection exchange root info rejected"
                   << ", peer=" << peerHost << ":" << peerPort << ", rsp_code=" << exchangeRsp->code
                   << ", rsp_msg=" << exchangeRsp->msg;
        return exchangeRpcStatus;
    }
    TE_CHECK_OR_RETURN(exchangeRsp->ownerDeviceId >= 0, ErrorCode::kRuntimeError, "owner_device_id is invalid");
    if (!exchangeRsp->backendKind.empty()) {
        TE_CHECK_OR_RETURN(exchangeRsp->backendKind == backend_->BackendKind(), ErrorCode::kNotSupported,
                           "owner backend kind mismatch");
    }
    if (backend_->SupportsReceiverDrivenRead()) {
        TE_CHECK_OR_RETURN(exchangeRsp->hixlRoutePolicy == backend_->RoutePolicy(), ErrorCode::kNotSupported,
                           "owner hixl route policy mismatch");
        TE_CHECK_OR_RETURN(!exchangeRsp->requesterInitRootInfo.empty(), ErrorCode::kInvalid,
                           "owner did not return requester init root info");
    }
    return Result::OK();
}

Result TransferEngine::InitRequesterRecvForConnection(const std::string &peerHost, uint16_t peerPort,
                                                      const std::string &rootInfo,
                                                      const ExchangeRootInfoResponse &exchangeRsp)
{
    ConnectionSpec spec;
    spec.localHost = localHost_;
    spec.localPort = localPort_;
    spec.localDeviceId = deviceId_;
    spec.peerHost = peerHost;
    spec.peerPort = peerPort;
    spec.peerDeviceId = exchangeRsp.ownerDeviceId;
    const std::string &requesterInitRootInfo =
        exchangeRsp.requesterInitRootInfo.empty() ? rootInfo : exchangeRsp.requesterInitRootInfo;
    Result initRecvRc = backend_->InitRecv(spec, requesterInitRootInfo);
    if (initRecvRc.IsError()) {
        TE_LOG_ERROR << "build connection init recv failed"
                   << ", peer=" << peerHost << ":" << peerPort
                   << ", owner_device_id=" << exchangeRsp.ownerDeviceId
                   << ", reason=" << initRecvRc.ToString();
        return initRecvRc;
    }

    ConnectionKey key{ deviceId_, peerHost, peerPort, exchangeRsp.ownerDeviceId };
    connMgr_->MarkRequesterRecvReady(key);
    return Result::OK();
}

Result TransferEngine::WaitOwnerReadyAndCache(const std::string &peerHost, uint16_t peerPort, int32_t ownerDeviceId,
                                              uint64_t *ownerMemGeneration)
{
    TE_CHECK_PTR_OR_RETURN(ownerMemGeneration);
    QueryConnReadyRequest queryReq;
    queryReq.requesterHost = localHost_;
    queryReq.requesterPort = localPort_;
    queryReq.requesterDeviceId = deviceId_;
    queryReq.ownerDeviceId = ownerDeviceId;

    ConnectionKey key{ deviceId_, peerHost, peerPort, ownerDeviceId };
    QueryConnReadyResponse queryRsp;
    for (int32_t i = 0; i < kConnReadyRetryCount; ++i) {
        TE_RETURN_IF_ERROR(controlClient_->QueryConnReady(peerHost, peerPort, queryReq, &queryRsp));
        TE_RETURN_IF_ERROR(RpcCodeToStatus(queryRsp.code, queryRsp.msg));
        if (queryRsp.ready) {
            connMgr_->MarkOwnerSendReady(key);
            *ownerMemGeneration = queryRsp.ownerMemGeneration;
            {
                std::lock_guard<std::mutex> lock(endpointCacheMutex_);
                state_->endpointOwnerDeviceCache[peerHost + ":" + std::to_string(peerPort)] =
                    TransferEngineState::EndpointCacheEntry{ ownerDeviceId, queryRsp.ownerMemGeneration };
            }
            return Result::OK();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kConnReadyRetryIntervalMs));
    }

    connMgr_->MarkStale(key);
    TE_LOG_WARNING << "build connection timeout waiting owner ready"
                 << ", peer=" << peerHost << ":" << peerPort
                 << ", owner_device_id=" << ownerDeviceId
                 << ", retry_count=" << kConnReadyRetryCount;
    return TE_MAKE_STATUS(ErrorCode::kNotReady,
                          "connection is not ready, peer=" + peerHost + ":" + std::to_string(peerPort) +
                              ", device_id=" + std::to_string(deviceId_) +
                              ", owner_device_id=" + std::to_string(ownerDeviceId));
}

std::string TransferEngine::CreateRootInfo() const
{
    return "te_root_info_v0_1_0";
}

}  // namespace datasystem
